package ingest

// This file implements the uploader: it merges committed local Parquet files
// into per-stream/hour objects and publishes them to the catalog. It owns no
// goroutine and no package state; the ingest service schedules RunOnce.

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// UploaderOptions configures an Uploader. Local, Objects, Catalog and
// PromotedFor are required; the thresholds must be positive.
type UploaderOptions struct {
	// Local is the tenant-scoped durable local-publication state.
	Local *LocalStore
	// Objects receives merged Parquet objects.
	Objects objstore.Store
	// Catalog lists the published files.
	Catalog catalog.Catalog
	// PromotedFor returns the promoted label columns of a stream. It is read
	// from several workers at once during a pass, so a custom function must be
	// safe for concurrent use.
	PromotedFor func(stream string) []string
	// MinBytes triggers an upload once a stream/hour group holds at least this
	// many pending bytes.
	MinBytes int64
	// MaxAge triggers an upload once the oldest pending file of a group was
	// published at least this long ago.
	MaxAge time.Duration
	// MergeMax caps the source bytes of one merged object. A single source
	// larger than MergeMax is still uploaded on its own.
	MergeMax int64
	// Concurrency bounds how many upload receipts — fresh or resumed, of one
	// tenant/stream/hour partition or of many — one pass may merge, build
	// sidecars and upload at the same time. Zero means
	// uploadDefaultConcurrency; values must stay within 1..uploadMaxConcurrency.
	// It bounds work, not memory: MergeMax caps the compressed source bytes
	// selected per receipt (one larger source still uploads alone), and the
	// resident memory of an in-flight receipt depends on its source fan-in,
	// the Parquet decoder's batches and the sidecar build, so peak memory
	// scales with this setting and must be measured at the configured
	// concurrency.
	Concurrency int
	// UploadMemory is the upload pipeline's memory admission budget in bytes.
	// Zero derives an automatic budget from the container and GOMEMLIMIT
	// limits; a positive value fixes it. It is a planning budget for admitted
	// merge footprints, not a hard RSS cap.
	UploadMemory int64
	// Index configures the search sidecar built for every published object.
	// The all-zero value means index.DefaultParams; any partially set value is
	// validated and rejected rather than silently normalized, so a caller
	// cannot publish with a half-configured index.
	Index index.Params
	// IndexMetrics observes sidecar builds. Nil is allowed: focused tests that
	// do not care about metrics skip observation, while the ingest service
	// always passes the shared handle registered at construction. No metrics
	// are defined in this package.
	IndexMetrics *index.Metrics
	// metrics receives the uploader's stage and job observations. It is owned
	// and registered by the ingest service; focused tests may leave it nil,
	// and every observation method is nil-safe.
	metrics *metrics
	// log receives the one-time budget resolution line and one bounded
	// admission line per admitted receipt. Nil is silent.
	log *slog.Logger
	// memoryLimits overrides the host budget readings in tests; nil reads the
	// real cgroup hierarchy and runtime GOMEMLIMIT.
	memoryLimits *uploadMemoryLimits
	// Now is the clock. Nil uses time.Now. It is read from several workers at
	// once during a pass, so a custom function must be safe for concurrent use.
	Now func() time.Time
}

// mergeFunc is the Parquet merge seam. Production uses storage.Merge; tests
// replace it to inject determinism at the merge boundary.
type mergeFunc func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error)

// Uploader turns committed local Parquet files into catalog-published objects.
// The ingest service calls RunOnce serially from one goroutine; RunOnce
// additionally holds a mutex for the whole pass, so an accidental second
// caller waits rather than interleaving reservations and cleanups. Inside one
// pass, independent receipts are driven concurrently by a bounded worker
// pool; the stage transitions of a single receipt always run sequentially on
// one worker.
type Uploader struct {
	mu sync.Mutex

	local        *LocalStore
	objects      objstore.Store
	catalog      catalog.Catalog
	promotedFor  func(string) []string
	merge        mergeFunc
	minBytes     int64
	maxAge       time.Duration
	mergeMax     int64
	concurrency  int
	indexParams  index.Params
	indexMetrics *index.Metrics
	metrics      *metrics
	log          *slog.Logger
	memory       *uploadMemoryAdmission
	memoryModel  uploadMemoryModel
	estimate     func(context.Context, *TenantStore, Upload) (uploadMemoryEstimate, error)
	now          func() time.Time
}

// NewUploader validates o and returns an Uploader. It starts no goroutines.
func NewUploader(o UploaderOptions) (*Uploader, error) {
	switch {
	case o.Local == nil:
		return nil, errors.New("ingest: uploader: Local is required")
	case o.Objects == nil:
		return nil, errors.New("ingest: uploader: Objects is required")
	case o.Catalog == nil:
		return nil, errors.New("ingest: uploader: Catalog is required")
	case o.PromotedFor == nil:
		return nil, errors.New("ingest: uploader: PromotedFor is required")
	case o.MinBytes <= 0:
		return nil, fmt.Errorf("ingest: uploader: MinBytes must be positive, got %d", o.MinBytes)
	case o.MaxAge <= 0:
		return nil, fmt.Errorf("ingest: uploader: MaxAge must be positive, got %s", o.MaxAge)
	case o.MergeMax <= 0:
		return nil, fmt.Errorf("ingest: uploader: MergeMax must be positive, got %d", o.MergeMax)
	case o.Concurrency < 0 || o.Concurrency > uploadMaxConcurrency:
		return nil, fmt.Errorf("ingest: uploader: Concurrency must be between 1 and %d when set, got %d",
			uploadMaxConcurrency, o.Concurrency)
	}
	indexParams, err := uploadIndexParams(o.Index)
	if err != nil {
		return nil, err
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	u := &Uploader{
		local:        o.Local,
		objects:      o.Objects,
		catalog:      o.Catalog,
		promotedFor:  o.PromotedFor,
		merge:        storage.Merge,
		minBytes:     o.MinBytes,
		maxAge:       o.MaxAge,
		mergeMax:     o.MergeMax,
		concurrency:  cmp.Or(o.Concurrency, uploadDefaultConcurrency),
		indexParams:  indexParams,
		indexMetrics: o.IndexMetrics,
		metrics:      o.metrics,
		log:          o.log,
		memoryModel:  newUploadMemoryModel(),
		now:          now,
	}
	u.estimate = u.estimateUploadMemory
	budget, err := resolveUploaderMemoryBudget(o.UploadMemory, o.memoryLimits)
	if err != nil {
		return nil, err
	}
	admission, err := newUploadMemoryAdmission(budget, uploadMemoryHooks{
		Warning: func(w uploadMemoryWarning) {
			if u.log == nil {
				return
			}
			u.log.Warn("upload memory: oversized estimate runs alone",
				"estimateBytes", w.Estimate, "weightBytes", w.Weight, "budgetBytes", w.Budget)
		},
		Oversized: func(uploadMemoryWarning) { u.metrics.oversizedJob() },
		Acquire:   func(lease uploadMemoryLease) { u.metrics.memoryAdmitted(lease) },
		Release:   func(lease uploadMemoryLease) { u.metrics.memoryReleased(lease) },
	})
	if err != nil {
		return nil, err
	}
	u.memory = admission
	if u.log != nil {
		u.log.Info("upload memory budget", "bytes", budget.Bytes, "source", budget.Source.String())
	}
	return u, nil
}

// resolveUploaderMemoryBudget resolves the configured budget, preferring
// injected limits for deterministic tests and reading the real host otherwise.
func resolveUploaderMemoryBudget(explicit int64, limits *uploadMemoryLimits) (uploadMemoryBudget, error) {
	if limits != nil {
		return resolveUploadMemoryBudget(explicit, *limits)
	}
	budget, err := resolveUploadMemoryBudget(explicit, hostUploadMemoryLimits())
	if err != nil {
		return uploadMemoryBudget{}, err
	}
	return budget, nil
}

// uploadIndexParams resolves an optional index.Params: the all-zero value
// means "use the package defaults" (so a focused Uploader without index
// settings still builds sidecars), while a partially set value must already be
// valid. It never fills in missing fields of a partial configuration.
func uploadIndexParams(p index.Params) (index.Params, error) {
	if p == (index.Params{}) {
		return index.DefaultParams, nil
	}
	if err := p.Validate(); err != nil {
		return index.Params{}, fmt.Errorf("ingest: uploader: index params: %w", err)
	}
	return p, nil
}

const (
	// uploadCleanupTimeout bounds the detached context used to abort a partial
	// merge once the pass context is canceled.
	uploadCleanupTimeout = 30 * time.Second
	// uploadDefaultConcurrency is the worker count a pass uses when
	// UploaderOptions.Concurrency is zero.
	uploadDefaultConcurrency = 4
	// uploadMaxConcurrency is the largest accepted UploaderOptions.Concurrency.
	uploadMaxConcurrency = 64
	// uploadOrphanGrace is how long a completed Put may wait before
	// Catalog.AddFiles. The compactor's orphan GC deletes objects without a
	// catalog row after one hour, so a Put older than this must not be
	// published under its old key.
	uploadOrphanGrace = 30 * time.Minute
	// uploadPutAttempts bounds the rotate-and-re-put loop when a Put itself
	// takes longer than the orphan grace.
	uploadPutAttempts = 3
)

// UploadResult summarizes one completed upload, fresh or resumed.
type UploadResult struct {
	Tenant tenant.ID
	Stream string
	Hour   time.Time
	// ID is the local receipt id and stays stable across retries and key
	// rotations.
	ID uuid.UUID
	// Key is the object key the catalog row now names. It can differ from the
	// receipt's initial key when a stale or unknown Put forced a rotation.
	Key string
	// Sources is the number of local files retired by the upload.
	Sources int
	Rows    int64
	Bytes   int64
	// Resumed reports that the receipt predated this RunOnce.
	Resumed bool
}

// UploadReport is the outcome of one Uploader.RunOnce. Completed holds the
// uploads that reached published and had their cleanup committed; Uploads and
// Resumed count them, and Bytes and Rows sum their object sizes (including
// Parquet footers) and row counts. The service turns these into metrics.
type UploadReport struct {
	Completed []UploadResult
	Uploads   int
	Resumed   int
	Bytes     int64
	Rows      int64
}

// RunOnce executes one upload pass in two barrier-separated phases. It first
// resumes every pending receipt concurrently, dispatching and reporting them in
// durable order, and only after all of them joined does it select and publish
// new work, so an existing receipt always wins over a fresh reservation. A
// failure in one receipt or group is reported with the others' errors and does
// not stop the pass.
//
// RunOnce never publishes a catalog row before the object Put completed, keeps
// the reservation id and source order stable across retries, and rotates the
// object key instead of publishing a Put the compactor's orphan GC may have
// collected or overwriting an object key that was already Put. The ingest
// service owns scheduling, metrics and the loop.
func (u *Uploader) RunOnce(ctx context.Context) (UploadReport, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	var report UploadReport
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("ingest: uploader: %w", err)
	}

	tenants, err := u.local.Tenants(ctx)
	if err != nil {
		return report, fmt.Errorf("ingest: uploader: list tenants: %w", err)
	}
	var errs []error
	// Phase 1: read every tenant's receipts, serially and before any new
	// reservation. A tenant whose store or receipt list cannot be read is
	// excluded from new work: without the list, its pending files may already
	// be reserved.
	var resume []uploadJob
	var ready []tenantUploader
	for _, id := range tenants {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("ingest: uploader: %w", err))
			break
		}
		ts, jobs, err := u.resumeJobsFor(ctx, id)
		if err != nil {
			errs = append(errs, fmt.Errorf("ingest: uploader: tenant %q: %w", id, err))
			continue
		}
		resume = append(resume, jobs...)
		ready = append(ready, tenantUploader{id: id, ts: ts})
	}
	// Phase 2: resume the collected receipts concurrently. The pool joins
	// before phase 3 looks at pending files, so a receipt this pass failed to
	// finish still reserves its sources below.
	u.collectJobs(ctx, u.runJobs(ctx, resume), &report, &errs)

	var fresh []uploadJob
	if err := ctx.Err(); err != nil {
		errs = append(errs, fmt.Errorf("ingest: uploader: %w", err))
	} else {
		// Phase 3: reserve every due group's chunks before dispatching any of
		// them, so the pass works from one fixed, disjoint job list.
		for _, t := range ready {
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("ingest: uploader: %w", err))
				break
			}
			jobs, err := u.reserveUploads(ctx, t.ts, t.id, &errs)
			if err != nil {
				errs = append(errs, fmt.Errorf("ingest: uploader: tenant %q: %w", t.id, err))
				continue
			}
			fresh = append(fresh, jobs...)
		}
		// Phase 4: publish the fresh reservations concurrently.
		u.collectJobs(ctx, u.runJobs(ctx, fresh), &report, &errs)
	}
	return report, errors.Join(errs...)
}

// tenantUploader is one tenant handle ready for new work after its receipts
// were collected and resumed.
type tenantUploader struct {
	id tenant.ID
	ts *TenantStore
}

// resumeJobsFor opens one tenant's store and turns its pending receipts into
// per-receipt resume jobs. The returned error text is the failure of opening
// the store or listing its uploads; on error the tenant must not start new
// work, because its pending files may already be reserved by unread receipts.
func (u *Uploader) resumeJobsFor(ctx context.Context, id tenant.ID) (*TenantStore, []uploadJob, error) {
	ts, err := u.local.Tenant(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("open tenant store: %w", err)
	}
	receipts, err := ts.Uploads(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list uploads: %w", err)
	}
	// The job source-byte metric needs each receipt's source sizes. Sizes live
	// in the same manifest the receipts came from, so this is one extra read
	// per tenant with pending work. A failed read only skips that observation:
	// metrics must never fail the receipt or the pass.
	var bytesByID map[uuid.UUID]int64
	if len(receipts) > 0 {
		if files, filesErr := ts.Files(ctx); filesErr == nil {
			bytesByID = make(map[uuid.UUID]int64, len(files))
			for _, f := range files {
				bytesByID[f.ID] = f.Bytes
			}
		}
	}
	return ts, newResumeJobs(ts, receipts, bytesByID), nil
}

// reserveUploads groups pending files by stream and UTC hour, applies the byte
// and age triggers, and reserves every due MergeMax-bounded chunk of each due
// group as its own receipt: a group aged past MaxAge drains fully, a group
// over MinBytes drains until its remainder is below MinBytes, and chunks take
// the group's files in durable order, so later passes keep merging from the
// same head. Sources already named by any receipt — including one this pass
// failed to finish — are skipped, so every reservation owns a disjoint source
// set, and the chunks of one group partition its remaining files by
// construction. Each chunk consumes at least one file, so the loop always
// reaches a not-due remainder.
func (u *Uploader) reserveUploads(ctx context.Context, ts *TenantStore, id tenant.ID, errs *[]error) ([]uploadJob, error) {
	pending, err := ts.PendingFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pending files: %w", err)
	}
	receipts, err := ts.Uploads(ctx)
	if err != nil {
		return nil, fmt.Errorf("list uploads: %w", err)
	}
	busy := make(map[uuid.UUID]bool)
	for _, up := range receipts {
		for _, src := range up.Sources {
			busy[src] = true
		}
	}
	groups := groupPendingFiles(pending, busy)
	keys := slices.SortedFunc(maps.Keys(groups), compareGroupKeys)

	now := u.now()
	var jobs []uploadJob
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			*errs = append(*errs, fmt.Errorf("ingest: uploader: %w", err))
			return jobs, nil
		}
		files := groups[key]
		for u.due(files, now) {
			chosen := u.selectSources(files)
			sources := make([]uuid.UUID, len(chosen))
			for i, f := range chosen {
				sources[i] = f.ID
			}
			up, err := ts.ReserveUpload(ctx, Upload{
				Tenant:  id,
				Stream:  key.stream,
				Hour:    key.hour,
				Sources: sources,
			})
			if err != nil {
				*errs = append(*errs, fmt.Errorf("reserve upload for %s %s: %w", key.stream, key.hour.Format(time.RFC3339), err))
				// Stop reserving this group: the failed chunk's sources stay
				// unreserved and retryable, and the failure likely affects the
				// whole manifest, not one chunk.
				break
			}
			sourceBytes, known := uploadLocalSourceBytes(chosen)
			jobs = append(jobs, uploadJob{
				ts: ts, up: up, sourceBytes: sourceBytes, sourceBytesKnown: known,
			})
			files = files[len(chosen):]
		}
	}
	return jobs, nil
}

// uploadLocalSourceBytes returns the exact compressed size of the files a
// fresh reservation selected, straight from their committed manifest
// metadata. chosen is never empty for a job, so known is false only for an
// empty selection.
func uploadLocalSourceBytes(chosen []LocalFile) (total int64, known bool) {
	if len(chosen) == 0 {
		return 0, false
	}
	for _, f := range chosen {
		total += f.Bytes
	}
	return total, true
}

// uploadReceiptSourceBytes sums a receipt's source sizes from the tenant's
// file metadata. known is false when a source is missing from the map, so the
// caller skips the sample instead of recording a misleading zero.
func uploadReceiptSourceBytes(sources []uuid.UUID, bytesByID map[uuid.UUID]int64) (total int64, known bool) {
	if len(sources) == 0 || len(bytesByID) == 0 {
		return 0, false
	}
	for _, id := range sources {
		n, ok := bytesByID[id]
		if !ok {
			return 0, false
		}
		total += n
	}
	return total, true
}

// processUpload drives one receipt to published and cleans it up. The catalog
// is consulted first on every receipt: a row (live or tombstoned) with the
// receipt's key and identical stable fields proves an earlier publication, so
// the upload finishes retirement without re-merging, re-Putting, re-adding or
// resurrecting compacted data. Only when no row exists does the uploader make
// the merged temp durable and Put the object under a key never used before.
func (u *Uploader) processUpload(ctx context.Context, ts *TenantStore, up Upload) (UploadResult, error) {
	original := up.Stage
	switch up.Stage {
	case UploadPublished:
		if err := ts.CompleteUpload(ctx, up.ID); err != nil {
			return UploadResult{}, fmt.Errorf("complete published upload: %w", err)
		}
		return uploadResult(up), nil
	case UploadReserved, UploadMerged, UploadStored:
	default:
		return UploadResult{}, fmt.Errorf("%w: unknown upload stage %q", ErrUploadStage, up.Stage)
	}

	found, err := u.catalogRowStatus(ctx, up)
	if err != nil {
		return UploadResult{}, err
	}
	if found {
		// A catalog row is only written after Put, so the receipt should
		// already be at least stored; raise the durable hint if a torn write
		// left it at merged, without touching the object.
		if original != UploadStored {
			if err := ts.MarkStored(ctx, up.ID); err != nil {
				return UploadResult{}, fmt.Errorf("raise matched upload to stored: %w", err)
			}
		}
		if err := ts.MarkPublished(ctx, up.ID); err != nil {
			return UploadResult{}, fmt.Errorf("mark published: %w", err)
		}
		if err := ts.CompleteUpload(ctx, up.ID); err != nil {
			return UploadResult{}, fmt.Errorf("complete upload: %w", err)
		}
		return uploadResult(up), nil
	}

	merged, err := u.ensureMerged(ctx, ts, up)
	if err != nil {
		return UploadResult{}, err
	}
	published, hasSidecar, err := u.putBeforeCatalog(ctx, ts, merged, original)
	if err != nil {
		return UploadResult{}, err
	}
	if err := u.publishUpload(ctx, ts, published, hasSidecar); err != nil {
		return UploadResult{}, err
	}
	return uploadResult(published), nil
}

// putBeforeCatalog uploads the merged temp and its search sidecar, records when
// the data Put completed, and reports whether a sidecar was published for the
// returned key. A resumed receipt always rotates to a fresh object key before
// its first Put: a recorded completion proves an earlier Put of the current
// key succeeded, and an unknown completion may hide one, so object keys are
// never written twice regardless of age. Only fresh reserved work, which has
// never been Put, uses the reservation key as-is.
//
// The data Put completion is captured immediately after Store.Put returns,
// before RecordPut, the sidecar build or any Close. The loop re-checks the age
// only after all of that work: a Put (or its receipt write) that outlives the
// grace rotates the key and rebuilds both objects under the new key, bounded
// by ctx and uploadPutAttempts. The sidecar is always built from the finalized
// temp for the current key, so no sidecar state has to survive a restart and a
// true HasSidecar flag can only be handed to the catalog after the object Put
// for that key succeeded.
func (u *Uploader) putBeforeCatalog(ctx context.Context, ts *TenantStore, up Upload, original UploadStage) (Upload, bool, error) {
	// Fresh reserved work has never been Put; its key comes from
	// ReserveUpload and is safe as-is. Every resumed merged/stored receipt may
	// already name a Put object, so it rotates even when its completion time
	// is fresh: rewriting a key that Catalog.AddFiles, a DuckDB metadata cache
	// or the compactor may still reference is forbidden.
	if original != UploadReserved {
		rotated, err := ts.RefreshUploadKey(ctx, up.ID)
		if err != nil {
			return Upload{}, false, fmt.Errorf("refresh upload key: %w", err)
		}
		up = rotated
	}
	for attempt := 1; attempt <= uploadPutAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Upload{}, false, fmt.Errorf("ingest: uploader: %w", err)
		}
		completed, err := u.putObject(ctx, ts, up)
		if err != nil {
			return Upload{}, false, err
		}
		if err := ts.RecordPut(ctx, up.ID, completed); err != nil {
			return Upload{}, false, fmt.Errorf("record put: %w", err)
		}
		up.Stage = UploadStored
		up.PutCompletedAt = completed
		// Build and Put the sidecar for the CURRENT key before the age
		// re-check: a slow build is exactly the work that can make the data
		// Put stale, and a rotation below must replace both objects.
		sidecar, err := u.buildSidecar(ctx, ts, up)
		if err != nil {
			return Upload{}, false, err
		}
		if err := u.putSidecar(ctx, up, sidecar); err != nil {
			return Upload{}, false, err
		}
		if !u.putExpired(completed, u.now()) {
			return up, sidecar.has, nil
		}
		if attempt == uploadPutAttempts {
			break
		}
		rotated, err := ts.RefreshUploadKey(ctx, up.ID)
		if err != nil {
			return Upload{}, false, fmt.Errorf("refresh stale upload key: %w", err)
		}
		up = rotated
	}
	return Upload{}, false, fmt.Errorf("upload %s stayed older than the orphan grace after %d puts", up.ID, uploadPutAttempts)
}

// sidecar is one sidecar build outcome. has is false when the file exceeded
// the gram cap, in which case the object is published without a sidecar; data
// is then nil.
type sidecar struct {
	data []byte
	has  bool
}

// buildSidecar encodes the search sidecar of up's finalized merge output. It
// opens and closes its own temp handle and reader, so the uploader keeps no
// borrowed state. index.ErrTooManyGrams is not an error: it means "publish
// without a sidecar". Every other build or read failure is returned and fails
// publication, leaving the receipt retryable; the next attempt rebuilds.
func (u *Uploader) buildSidecar(ctx context.Context, ts *TenantStore, up Upload) (_ sidecar, err error) {
	f, err := ts.OpenUploadTemp(ctx, up.ID)
	if err != nil {
		return sidecar{}, fmt.Errorf("open merge output for sidecar: %w", err)
	}
	defer func() {
		err = errors.Join(err, closeUploadFile(f))
	}()
	info, err := f.Stat()
	if err != nil {
		return sidecar{}, fmt.Errorf("stat merge output for sidecar: %w", err)
	}
	if info.Size() != up.Bytes {
		return sidecar{}, fmt.Errorf("merge output is %d bytes, receipt records %d", info.Size(), up.Bytes)
	}
	reader, err := storage.NewReader(f, info.Size())
	if err != nil {
		return sidecar{}, fmt.Errorf("read merge output for sidecar: %w", err)
	}
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	start := time.Now()
	data, err := storage.BuildSidecar(ctx, reader, u.indexParams)
	elapsed := time.Since(start)
	switch {
	case err == nil:
		if u.indexMetrics != nil {
			u.indexMetrics.ObserveBuild(index.SourceIngest, elapsed, len(data))
		}
		return sidecar{data: data, has: true}, nil
	case errors.Is(err, index.ErrTooManyGrams):
		if u.indexMetrics != nil {
			u.indexMetrics.ObserveSkip(index.SourceIngest, elapsed)
		}
		return sidecar{}, nil
	default:
		return sidecar{}, fmt.Errorf("build sidecar for %q: %w", up.Key, err)
	}
}

// putSidecar stores the encoded sidecar under objstore.SidecarKey(up.Key). It
// runs after the data Put and before Catalog.AddFiles; a failure leaves the
// catalog unflagged and the receipt retryable.
func (u *Uploader) putSidecar(ctx context.Context, up Upload, sc sidecar) error {
	if !sc.has {
		return nil
	}
	key := objstore.SidecarKey(up.Key)
	start := time.Now()
	err := u.objects.Put(ctx, key, bytes.NewReader(sc.data), int64(len(sc.data)))
	u.metrics.observePut(putKindSidecar, time.Since(start))
	if err != nil {
		return fmt.Errorf("put sidecar %q: %w", key, err)
	}
	return nil
}

// putExpired reports whether a completed Put is at least the orphan grace old.
func (u *Uploader) putExpired(completed, now time.Time) bool {
	return !completed.IsZero() && now.Sub(completed) >= uploadOrphanGrace
}

// addUploadResult records one completed upload in the pass report.
func addUploadResult(report *UploadReport, res UploadResult) {
	report.Completed = append(report.Completed, res)
	report.Uploads++
	if res.Resumed {
		report.Resumed++
	}
	report.Bytes += res.Bytes
	report.Rows += res.Rows
}

// ensureMerged returns up unchanged when its finalized temp already exists
// with the byte size the receipt records. A missing or stale temp — for
// example after a crash between finalize and RecordMerge — is rebuilt by
// re-merging the preserved sources under the same upload id.
func (u *Uploader) ensureMerged(ctx context.Context, ts *TenantStore, up Upload) (Upload, error) {
	if up.Stage != UploadReserved {
		size, err := u.tempSize(ctx, ts, up.ID)
		switch {
		case err == nil && size == up.Bytes:
			return up, nil
		case err == nil:
			// Keep going: a stale temp is replaced below.
		case !errors.Is(err, fs.ErrNotExist):
			return Upload{}, fmt.Errorf("check upload temp: %w", err)
		}
	}
	return u.mergeSources(ctx, ts, up)
}

// mergeSources merges up.Sources into the upload temp, finalizes it, records
// the stats and returns the receipt with those stats in memory. On any error
// the partial temp is aborted and every opened reader and file is released.
func (u *Uploader) mergeSources(ctx context.Context, ts *TenantStore, up Upload) (_ Upload, err error) {
	// The metric covers this whole phase: opening and validating the sources,
	// the merge itself and the finalize/record that makes the temp durable.
	start := time.Now()
	defer func() { u.metrics.observeMerge(time.Since(start)) }()
	if len(up.Sources) == 0 {
		return Upload{}, errors.New("upload has no sources")
	}
	tmp, err := ts.CreateUploadTemp(ctx, up.ID)
	if err != nil {
		return Upload{}, fmt.Errorf("create upload temp: %w", err)
	}
	// Abort any merge that does not end finalized so no partial output or
	// open handle leaks, including a finalize that failed before closing its
	// handle. Abort runs on a detached, bounded context because the pass may
	// already be canceled. An already-renamed temp cannot be aborted
	// (ErrUploadStage); that is proof the handle was closed and the finalized
	// file is durable, so the next pass re-merges it.
	finalized := false
	aborted := false
	abort := func() error {
		if finalized || aborted {
			return nil
		}
		aborted = true
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), uploadCleanupTimeout)
		defer cancel()
		abortErr := ts.AbortUploadTemp(abortCtx, tmp)
		if errors.Is(abortErr, ErrUploadStage) {
			return nil
		}
		return abortErr
	}
	defer func() {
		err = errors.Join(err, abort())
	}()

	readers := make([]*storage.Reader, 0, len(up.Sources))
	files := make([]*os.File, 0, len(up.Sources))
	defer func() {
		err = errors.Join(err, closeUploadSources(readers, files))
	}()
	for _, id := range up.Sources {
		f, err := ts.OpenFile(ctx, id)
		if err != nil {
			return Upload{}, fmt.Errorf("open source %s: %w", id, err)
		}
		files = append(files, f)
		info, err := f.Stat()
		if err != nil {
			return Upload{}, fmt.Errorf("stat source %s: %w", id, err)
		}
		r, err := storage.NewReaderWithOptions(f, info.Size(), storage.MergeReaderOptions())
		if err != nil {
			return Upload{}, fmt.Errorf("read source %s: %w", id, err)
		}
		readers = append(readers, r)
	}

	stats, err := u.merge(ctx, tmp, readers, u.promotedFor(up.Stream))
	if err != nil {
		return Upload{}, fmt.Errorf("merge sources: %w", err)
	}
	n := tmp.Bytes()
	if n <= 0 {
		return Upload{}, fmt.Errorf("merge produced %d bytes", n)
	}
	if err := ts.FinalizeUploadTemp(ctx, tmp); err != nil {
		return Upload{}, fmt.Errorf("finalize upload temp: %w", errors.Join(err, abort()))
	}
	finalized = true
	if err := ts.RecordMerge(ctx, up.ID, stats, n); err != nil {
		return Upload{}, fmt.Errorf("record merge: %w", err)
	}

	up.Stage = UploadMerged
	up.Rows = stats.Rows
	up.Bytes = n
	up.MinTS = stats.MinTS
	up.MaxTS = stats.MaxTS
	up.LabelValues = stats.LabelValues
	up.LabelsComplete = stats.LabelsComplete
	return up, nil
}

// putObject uploads the finalized temp under the receipt's object key and
// returns the clock reading taken as soon as Store.Put returned. That time is
// captured before the uploaded file is closed, so a slow close cannot make the
// object look fresher than its Put; close errors still fail the call. A
// catalog row is never written before this returns nil.
func (u *Uploader) putObject(ctx context.Context, ts *TenantStore, up Upload) (_ time.Time, err error) {
	f, err := ts.OpenUploadTemp(ctx, up.ID)
	if err != nil {
		return time.Time{}, fmt.Errorf("open upload temp: %w", err)
	}
	defer func() {
		err = errors.Join(err, closeUploadFile(f))
	}()
	info, err := f.Stat()
	if err != nil {
		return time.Time{}, fmt.Errorf("stat upload temp: %w", err)
	}
	if info.Size() != up.Bytes {
		return time.Time{}, fmt.Errorf("upload temp is %d bytes, receipt records %d", info.Size(), up.Bytes)
	}
	start := time.Now()
	err = u.objects.Put(ctx, up.Key, f, info.Size())
	u.metrics.observePut(putKindData, time.Since(start))
	if err != nil {
		return time.Time{}, fmt.Errorf("put object %q: %w", up.Key, err)
	}
	return u.now(), nil
}

// publishUpload lists up in the catalog and then retires its sources. When the
// catalog already holds the key, the existing row — live or tombstoned — must
// match the receipt exactly; that row is evidence of prior publication, so the
// upload finishes without re-adding or resurrecting data. HasSidecar comes
// from this call's successful sidecar Put for this key, never from a durable
// hint; the catalog flag is excluded from identity because backfill can set it
// later.
func (u *Uploader) publishUpload(ctx context.Context, ts *TenantStore, up Upload, hasSidecar bool) error {
	file := catalog.File{
		Tenant:         up.Tenant,
		Stream:         up.Stream,
		Key:            up.Key,
		MinTS:          up.MinTS,
		MaxTS:          up.MaxTS,
		Rows:           up.Rows,
		Bytes:          up.Bytes,
		LabelValues:    up.LabelValues,
		LabelsComplete: up.LabelsComplete,
		HasSidecar:     hasSidecar,
		CreatedAt:      up.CreatedAt,
	}
	err := u.catalog.AddFiles(ctx, file)
	switch {
	case err == nil:
	case errors.Is(err, catalog.ErrExists):
		found, err := u.catalogRowStatus(ctx, up)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: catalog reports %q exists but no matching row is listed", ErrUploadConflict, up.Key)
		}
	default:
		return fmt.Errorf("add catalog file %q: %w", up.Key, err)
	}
	if err := ts.MarkPublished(ctx, up.ID); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	if err := ts.CompleteUpload(ctx, up.ID); err != nil {
		return fmt.Errorf("complete upload: %w", err)
	}
	return nil
}

// catalogRowStatus looks the receipt's key up in the catalog, tombstones
// included. It returns true only for a row whose stable identity equals the
// receipt's; a different row under the key is a hard conflict and leaves the
// sources untouched.
func (u *Uploader) catalogRowStatus(ctx context.Context, up Upload) (bool, error) {
	// TODO(phase3): a Catalog.GetFile(tenant, key) lookup would replace this
	// O(number of tenant files) scan; the interface is deliberately unchanged
	// for now.
	// Filter by tenant only: a corrupted or foreign row may carry another
	// Stream for the same key, and the caller must see the conflict instead of
	// falling through to a duplicate AddFiles.
	for f, err := range u.catalog.ListFiles(ctx, catalog.Filter{
		Tenant:         up.Tenant,
		IncludeDeleted: true,
	}) {
		if err != nil {
			return false, fmt.Errorf("list catalog for %q: %w", up.Key, err)
		}
		if f.Key != up.Key {
			continue
		}
		if !uploadMatchesCatalog(up, f) {
			return false, fmt.Errorf("%w: catalog row %q conflicts with upload %s", ErrUploadConflict, up.Key, up.ID)
		}
		return true, nil
	}
	return false, nil
}

// sameCatalogTime compares two identity timestamps at the microsecond
// precision catalogs keep. New reservations are stamped canonically, but a
// version 1 manifest written by an older build may still carry a
// nanosecond-precision CreatedAt, which SQLite stores truncated.
func sameCatalogTime(a, b time.Time) bool {
	return a.UnixMicro() == b.UnixMicro()
}

// uploadMatchesCatalog compares the fields that catalog.File and Upload share.
// Timestamps that hold catalog identity are compared at canonical UTC
// microsecond precision, the representation every catalog keeps.
func uploadMatchesCatalog(up Upload, f catalog.File) bool {
	return f.Tenant == up.Tenant &&
		f.Stream == up.Stream &&
		f.Key == up.Key &&
		f.Rows == up.Rows &&
		f.Bytes == up.Bytes &&
		sameCatalogTime(f.MinTS, up.MinTS) &&
		sameCatalogTime(f.MaxTS, up.MaxTS) &&
		sameCatalogTime(f.CreatedAt, up.CreatedAt) &&
		maps.EqualFunc(f.LabelValues, up.LabelValues, slices.Equal)
}

// tempSize returns the finalized upload temp's size, or an error wrapping
// fs.ErrNotExist when it is absent.
func (u *Uploader) tempSize(ctx context.Context, ts *TenantStore, id uuid.UUID) (size int64, err error) {
	f, err := ts.OpenUploadTemp(ctx, id)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = errors.Join(err, closeUploadFile(f))
	}()
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat upload temp: %w", err)
	}
	return info.Size(), nil
}

// due reports whether a group reached the byte or publication-age trigger.
// Byte comparisons subtract instead of summing past the threshold, so a group
// with absurd sizes cannot overflow into "not due".
func (u *Uploader) due(files []LocalFile, now time.Time) bool {
	if len(files) == 0 {
		return false
	}
	var total int64
	for _, f := range files {
		// total is always below minBytes here; the difference can't overflow.
		if max(f.Bytes, 0) >= u.minBytes-total {
			return true
		}
		total += max(f.Bytes, 0)
	}
	var oldest time.Time
	for _, f := range files {
		if !f.PublishedAt.IsZero() && (oldest.IsZero() || f.PublishedAt.Before(oldest)) {
			oldest = f.PublishedAt
		}
	}
	return !oldest.IsZero() && !now.Before(oldest.Add(u.maxAge))
}

// selectSources takes whole files in durable order until adding the next one
// would exceed MergeMax. The first file is always taken, so a single oversized
// file still makes progress. The cap check subtracts to stay overflow-safe.
func (u *Uploader) selectSources(files []LocalFile) []LocalFile {
	chosen := make([]LocalFile, 0, len(files))
	var total int64
	for _, f := range files {
		size := max(f.Bytes, 0)
		if len(chosen) > 0 && size > u.mergeMax-total {
			break
		}
		chosen = append(chosen, f)
		total += size
	}
	return chosen
}

// uploadGroupKey identifies one merge partition: a stream and a UTC hour. The
// tenant is implied by the tenant store.
type uploadGroupKey struct {
	stream string
	hour   time.Time
}

// groupPendingFiles groups pending files by stream/hour, skipping files already
// owned by a pending receipt. PendingFiles is ordered by stream, hour and id,
// so each group keeps its durable source order.
func groupPendingFiles(files []LocalFile, busy map[uuid.UUID]bool) map[uploadGroupKey][]LocalFile {
	groups := make(map[uploadGroupKey][]LocalFile)
	for _, f := range files {
		if busy[f.ID] {
			continue
		}
		key := uploadGroupKey{stream: f.Stream, hour: f.Hour}
		groups[key] = append(groups[key], f)
	}
	return groups
}

// compareGroupKeys orders groups by stream, then hour, for deterministic
// passes and tests.
func compareGroupKeys(a, b uploadGroupKey) int {
	if c := cmp.Compare(a.stream, b.stream); c != 0 {
		return c
	}
	return a.hour.Compare(b.hour)
}

func uploadResult(up Upload) UploadResult {
	return UploadResult{
		Tenant:  up.Tenant,
		Stream:  up.Stream,
		Hour:    up.Hour,
		ID:      up.ID,
		Key:     up.Key,
		Sources: len(up.Sources),
		Rows:    up.Rows,
		Bytes:   up.Bytes,
	}
}

// closeUploadSources releases every reader and file, joining all errors.
func closeUploadSources(readers []*storage.Reader, files []*os.File) error {
	var errs []error
	for _, r := range slices.Backward(readers) {
		if err := r.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close source reader: %w", err))
		}
	}
	for _, f := range slices.Backward(files) {
		if err := closeUploadFile(f); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closeUploadFile closes f, treating a nil file as already closed.
func closeUploadFile(f *os.File) error {
	if f == nil {
		return nil
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close upload file: %w", err)
	}
	return nil
}
