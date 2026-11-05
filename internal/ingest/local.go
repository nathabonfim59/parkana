package ingest

// This file owns the tenant-scoped rooted local state under
// <data.dir>/parquet/: committed Parquet files, the atomic manifest commit
// machinery, crash recovery with quarantine, and the narrow upload-temp and
// receipt operations the uploader uses.
//
// Layout per tenant:
//
//	parquet/<tenant>/manifest.json                    committed snapshot
//	parquet/<tenant>/manifest.json.tmp                in-progress commit
//	parquet/<tenant>/<stream>/<yyyy>/<mm>/<dd>/<hh>/<uuidv7>.parquet
//	parquet/<tenant>/.uploads/<upload-id>.parquet     finalized merge output
//	parquet/<tenant>/.quarantine/                     unreferenced/stray files
//
// The dot-prefixed names cannot collide with sanitized stream names, which
// never begin with a dot.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// ErrLocalClosed reports a LocalStore use after Close.
var ErrLocalClosed = errors.New("ingest: local store closed")

const (
	filePerm = 0o640
	dirPerm  = 0o750
)

// syncFile is the narrow file seam the local store writes through; tests wrap
// it to inject write/sync/close failures.
type syncFile interface {
	io.Writer
	Sync() error
	Close() error
}

// localFileOps are the rooted filesystem seams. Tests replace members to
// inject failures at specific durability boundaries.
type localFileOps struct {
	create   func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error)
	rename   func(root *os.Root, old, new string) error
	remove   func(root *os.Root, name string) error
	mkdirAll func(root *os.Root, name string, perm os.FileMode) error
	syncDir  func(root *os.Root, name string) error
	now      func() time.Time
}

func defaultLocalOps() localFileOps {
	return localFileOps{
		create: func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			return root.OpenFile(name, flag, perm)
		},
		rename:   func(root *os.Root, old, new string) error { return root.Rename(old, new) },
		remove:   func(root *os.Root, name string) error { return root.Remove(name) },
		mkdirAll: func(root *os.Root, name string, perm os.FileMode) error { return root.MkdirAll(name, perm) },
		syncDir: func(root *os.Root, name string) error {
			dir, err := root.Open(name)
			if err != nil {
				return err
			}
			syncErr := dir.Sync()
			return errors.Join(syncErr, dir.Close())
		},
		now: time.Now,
	}
}

// ParquetWriter is the narrow seam over storage.Writer so tests can inject a
// fake while keeping the option to run real round trips.
type ParquetWriter interface {
	Write(arrow.RecordBatch) error
	Close() (storage.FileStats, error)
}

// WriterFactory builds a ParquetWriter writing to w. The production default
// wraps storage.NewWriter.
type WriterFactory func(w io.Writer, s *arrow.Schema, promoted []string) (ParquetWriter, error)

func defaultWriterFactory(w io.Writer, s *arrow.Schema, promoted []string) (ParquetWriter, error) {
	return storage.NewWriter(w, s, promoted)
}

// LocalStore owns the rooted parquet directory and the per-tenant state
// handles. All filesystem access stays beneath dataDir/parquet via os.Root.
type LocalStore struct {
	root *os.Root
	ops  localFileOps

	mu      sync.Mutex
	tenants map[tenant.ID]*TenantStore
	closed  bool
}

// OpenLocal opens or creates dataDir/parquet, then recovers every tenant
// directory found there: the committed manifest is loaded (a corrupt or
// unsupported manifest is an error, never silently reset), uncommitted
// temporaries and unreferenced Parquet files are quarantined, files missing
// while still live in the manifest are corruption, retired files already
// deleted on disk are pruned, and a reconciled snapshot is written back when
// anything changed. Unpublished upload receipts are never dropped.
func OpenLocal(ctx context.Context, dataDir string) (*LocalStore, error) {
	return openLocal(ctx, dataDir, defaultLocalOps())
}

func openLocal(ctx context.Context, dataDir string, ops localFileOps) (*LocalStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ingest: open local store: %w", err)
	}
	if err := objstore.MkdirAllDurable(dataDir, dirPerm); err != nil {
		return nil, fmt.Errorf("ingest: create data directory %s: %w", dataDir, err)
	}
	base, err := os.OpenRoot(dataDir)
	if err != nil {
		return nil, fmt.Errorf("ingest: open data directory %s: %w", dataDir, err)
	}
	defer base.Close()
	if err := base.MkdirAll("parquet", dirPerm); err != nil {
		return nil, fmt.Errorf("ingest: create parquet directory: %w", err)
	}
	// Persist the parquet directory entry itself before anything beneath it
	// may be reported durable.
	if err := ops.syncDir(base, "."); err != nil {
		return nil, fmt.Errorf("ingest: sync data directory: %w", err)
	}
	root, err := base.OpenRoot("parquet")
	if err != nil {
		return nil, fmt.Errorf("ingest: open parquet directory: %w", err)
	}
	// Persist tenant directory entries created by an earlier crashed or failed
	// parent sync before discovery acknowledges their contents.
	if err := ops.syncDir(root, "."); err != nil {
		root.Close()
		return nil, fmt.Errorf("ingest: sync parquet directory: %w", err)
	}
	s := &LocalStore{root: root, ops: ops, tenants: make(map[tenant.ID]*TenantStore)}
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		root.Close()
		return nil, fmt.Errorf("ingest: list tenants: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			s.closeRoots()
			return nil, fmt.Errorf("ingest: open local store: %w", err)
		}
		if !entry.IsDir() {
			s.closeRoots()
			return nil, fmt.Errorf("ingest: unexpected file %q in parquet root", entry.Name())
		}
		id := tenant.ID(entry.Name())
		if err := id.Validate(); err != nil {
			s.closeRoots()
			return nil, fmt.Errorf("ingest: discovered tenant: %w", err)
		}
		t, err := s.recoverTenant(ctx, id)
		if err != nil {
			s.closeRoots()
			return nil, err
		}
		s.tenants[id] = t
	}
	return s, nil
}

func (s *LocalStore) closeRoots() error {
	var errs []error
	for _, t := range s.tenants {
		errs = append(errs, t.closeTemps()...)
		if err := t.root.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.root.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// closeTemps releases every still-open upload temp's file handle, marking the
// temps aborted. Their uncommitted bytes are quarantined by the next
// recovery.
func (t *TenantStore) closeTemps() (errs []error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed.Store(true)
	for id, tmp := range t.temps {
		if tmp.file != nil {
			errs = append(errs, tmp.file.Close())
			tmp.file = nil
		}
		tmp.state = tempAborted
		delete(t.temps, id)
	}
	return errs
}

// Close releases every tenant root and the store root.
func (s *LocalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.closeRoots()
}

// Tenants returns the sorted ids of every tenant with a local directory.
func (s *LocalStore) Tenants(ctx context.Context) ([]tenant.ID, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ingest: tenants: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrLocalClosed
	}
	return slices.Sorted(maps.Keys(s.tenants)), nil
}

// Frontier returns the committed checkpoint of one tenant; the zero value
// means nothing published. A missing tenant directory is not an error.
func (s *LocalStore) Frontier(ctx context.Context, id tenant.ID) (Frontier, error) {
	t, err := s.Tenant(ctx, id)
	if err != nil {
		return Frontier{}, err
	}
	return t.Frontier(ctx)
}

// Tenant returns the state handle for id, creating and recovering its
// directory when needed.
func (s *LocalStore) Tenant(ctx context.Context, id tenant.ID) (*TenantStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ingest: tenant %s: %w", id, err)
	}
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("ingest: tenant: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrLocalClosed
	}
	if t := s.tenants[id]; t != nil {
		return t, nil
	}
	fresh := true
	if _, err := s.root.Stat(string(id)); err == nil {
		fresh = false
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("ingest: stat tenant %s: %w", id, err)
	}
	if fresh {
		if err := s.root.Mkdir(string(id), dirPerm); err != nil {
			return nil, fmt.Errorf("ingest: create tenant %s directory: %w", id, err)
		}
	}
	// Sync the tenant directory entry every time a handle is first cached: an
	// earlier attempt may have created the directory and then failed this
	// very sync before it could be cached.
	if err := s.ops.syncDir(s.root, "."); err != nil {
		return nil, fmt.Errorf("ingest: sync tenant %s parent: %w", id, err)
	}
	t, err := s.recoverTenant(ctx, id)
	if err != nil {
		return nil, err
	}
	s.tenants[id] = t
	return t, nil
}

// recoverTenant opens the tenant root, loads the manifest, reconciles it
// against the directory contents and persists a clean snapshot when the
// reconciliation changed anything.
func (s *LocalStore) recoverTenant(ctx context.Context, id tenant.ID) (_ *TenantStore, retErr error) {
	root, err := s.root.OpenRoot(string(id))
	if err != nil {
		return nil, fmt.Errorf("ingest: open tenant %s: %w", id, err)
	}
	defer func() {
		if retErr != nil {
			root.Close()
		}
	}()
	t := &TenantStore{store: s, id: id, root: root, ops: &s.ops, temps: make(map[uuid.UUID]*UploadTemp)}
	// A previous process may have renamed manifest.json and then crashed
	// before the tenant-directory fsync that makes the rename durable.
	// Confirm the opened tenant root before reading the manifest, so no
	// recovered frontier or rotation count is ever exposed from a rename the
	// disk may not have kept.
	if err := s.ops.syncDir(root, "."); err != nil {
		return nil, fmt.Errorf("ingest: sync tenant %s root: %w", id, err)
	}
	m, err := t.readManifest(ctx)
	if err != nil {
		return nil, err
	}
	t.state = m
	if err := t.reconcile(ctx); err != nil {
		return nil, err
	}
	return t, nil
}

// TenantStore is one tenant's rooted local state. Methods that report durable
// coverage (Frontier/Files/Uploads), mutate, or delete sources first complete
// any manifest commit whose tenant-directory fsync is still pending, so no
// call can act on coverage the disk may not have kept.
type TenantStore struct {
	store *LocalStore
	id    tenant.ID
	root  *os.Root
	ops   *localFileOps

	closed      atomic.Bool
	mu          sync.Mutex
	state       tenantManifest
	pendingSync bool // manifest renamed but the tenant-dir fsync failed
	temps       map[uuid.UUID]*UploadTemp
}

// ensureSyncedLocked finishes a pending manifest directory sync before the
// caller proceeds. The manifest was already renamed into place, so only the
// fsync is retried.
func (t *TenantStore) ensureSyncedLocked(ctx context.Context) error {
	if t.closed.Load() {
		return ErrLocalClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !t.pendingSync {
		return nil
	}
	if err := t.ops.syncDir(t.root, "."); err != nil {
		return fmt.Errorf("ingest: sync %s manifest dir: %w", t.id, err)
	}
	t.pendingSync = false
	return nil
}

// mutate applies fn to a clone of the manifest and commits it atomically when
// fn accepts the change. A fn returning errCovered reports an idempotent
// no-op: nothing is written and mutate returns nil.
func (t *TenantStore) mutate(ctx context.Context, fn func(*tenantManifest) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return err
	}
	next := cloneManifest(t.state)
	if err := fn(&next); err != nil {
		if errors.Is(err, errCovered) {
			return nil
		}
		return err
	}
	return t.commitLocked(next)
}

var errCovered = errors.New("ingest: generation already covered")

// commitLocked writes next as manifest.json.tmp, fsyncs it, renames it over
// manifest.json, then fsyncs the tenant directory. When the rename already
// happened but the directory fsync fails, the candidate state stays in memory
// with pendingSync set; every later operation retries the sync before
// reporting coverage or mutating further.
func (t *TenantStore) commitLocked(next tenantManifest) error {
	next.Seq = t.state.Seq + 1
	if err := validateManifest(next); err != nil {
		return err
	}
	data, err := encodeManifest(next)
	if err != nil {
		return err
	}
	file, err := t.ops.create(t.root, manifestTempName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("ingest: create %s manifest: %w", t.id, err)
	}
	if n, err := file.Write(data); err != nil || n != len(data) {
		file.Close()
		return fmt.Errorf("ingest: write %s manifest: %w", t.id, cmp.Or(err, io.ErrShortWrite))
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("ingest: sync %s manifest: %w", t.id, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("ingest: close %s manifest: %w", t.id, err)
	}
	if err := t.ops.rename(t.root, manifestTempName, manifestName); err != nil {
		return fmt.Errorf("ingest: rename %s manifest: %w", t.id, err)
	}
	t.state = next
	t.pendingSync = true
	if err := t.ops.syncDir(t.root, "."); err != nil {
		return fmt.Errorf("ingest: sync %s manifest dir: %w", t.id, err)
	}
	t.pendingSync = false
	return nil
}

func (t *TenantStore) readManifest(ctx context.Context) (tenantManifest, error) {
	if err := ctx.Err(); err != nil {
		return tenantManifest{}, err
	}
	data, err := t.root.ReadFile(manifestName)
	if errors.Is(err, fs.ErrNotExist) {
		return newManifest(t.id), nil
	}
	if err != nil {
		return tenantManifest{}, fmt.Errorf("ingest: read %s manifest: %w", t.id, err)
	}
	m, err := decodeManifest(data, t.id)
	if err != nil {
		return tenantManifest{}, err
	}
	sortFiles(m.Files)
	sortUploads(m.Uploads)
	return m, nil
}

// Frontier returns the last committed generation range; the zero value means
// nothing published yet.
func (t *TenantStore) Frontier(ctx context.Context) (Frontier, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return Frontier{}, err
	}
	return t.state.Frontier, nil
}

// KeyRotations returns the tenant's durable count of committed object-key
// rotations (see RefreshUploadKey). The count is cumulative across restart,
// reconciliation and upload cleanup, and is never reported while a manifest
// directory sync is still pending, so a returned value is always durable.
func (t *TenantStore) KeyRotations(ctx context.Context) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return 0, err
	}
	return t.state.KeyRotations, nil
}

// Files returns deep copies of every committed local file, retired or not,
// sorted by stream, hour, id.
func (t *TenantStore) Files(ctx context.Context) ([]LocalFile, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return nil, err
	}
	out := make([]LocalFile, len(t.state.Files))
	for i := range t.state.Files {
		out[i] = t.state.Files[i].Clone()
	}
	return out, nil
}

// PendingFiles returns the committed files that are not retired: the set the
// uploader may select from.
func (t *TenantStore) PendingFiles(ctx context.Context) ([]LocalFile, error) {
	files, err := t.Files(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(files, func(f LocalFile) bool { return f.Retired }), nil
}

// Uploads returns deep copies of every upload receipt, sorted by stream,
// hour, id.
func (t *TenantStore) Uploads(ctx context.Context) ([]Upload, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return nil, err
	}
	out := make([]Upload, len(t.state.Uploads))
	for i := range t.state.Uploads {
		out[i] = t.state.Uploads[i].Clone()
	}
	return out, nil
}

func findFile(files []LocalFile, id uuid.UUID) int {
	return slices.IndexFunc(files, func(f LocalFile) bool { return f.ID == id })
}

func findUpload(uploads []Upload, id uuid.UUID) int {
	return slices.IndexFunc(uploads, func(u Upload) bool { return u.ID == id })
}

// Publish commits an already synced and renamed set of part files and
// advances the tenant frontier atomically in one manifest commit. A
// generation fully covered by the committed frontier is a success no-op
// (after any pending sync completes); a gap or a partial overlap is an error.
// Every file must already exist at its derived path, fsynced and renamed.
// Publish stamps PublishedAt on all files with the commit time.
func (t *TenantStore) Publish(ctx context.Context, fr Frontier, files []LocalFile) error {
	if fr.First == 0 || fr.First > fr.Last {
		return fmt.Errorf("ingest: invalid generation frontier %+v", fr)
	}
	return t.mutate(ctx, func(m *tenantManifest) error {
		switch {
		case fr.Last <= m.Frontier.Last:
			return errCovered
		case fr.First > m.Frontier.Last+1:
			return fmt.Errorf("%w: generation starts at %d, committed frontier is %d", ErrGenerationGap, fr.First, m.Frontier.Last)
		case fr.First <= m.Frontier.Last:
			return fmt.Errorf("%w: generation %d-%d overlaps committed frontier %d", ErrGenerationOverlap, fr.First, fr.Last, m.Frontier.Last)
		}
		now := t.ops.now()
		committed := make([]LocalFile, 0, len(files))
		for i := range files {
			// Deep-clone caller input: the manifest must not retain the
			// caller's maps nor mutate its slice.
			f := files[i].Clone()
			f.PublishedAt = now
			if f.Tenant != t.id {
				return fmt.Errorf("ingest: publish file %s for %q under %q: %w", f.ID, f.Tenant, t.id, ErrTenantMismatch)
			}
			if err := f.validate(t.id); err != nil {
				return err
			}
			if findFile(m.Files, f.ID) >= 0 {
				return fmt.Errorf("ingest: publish file %s already listed", f.ID)
			}
			if _, err := t.root.Stat(f.RelPath()); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrFileMissing, f.RelPath(), err)
			}
			committed = append(committed, f)
		}
		m.Files = append(m.Files, committed...)
		sortFiles(m.Files)
		m.Frontier = fr
		return nil
	})
}

// ReserveUpload durably records a new upload reservation. The object key is
// derived as objstore.DataKey(tenant, stream, hour, u.ID); u.Key must be
// empty or equal to it. u.Tenant must be empty or this tenant. Every source
// must name a live, unretired file of the same stream and hour that no other
// active receipt already reserves. Reserving an existing id with an identical
// specification returns the stored receipt; a differing one is
// ErrUploadConflict.
func (t *TenantStore) ReserveUpload(ctx context.Context, u Upload) (Upload, error) {
	if u.Tenant != "" && u.Tenant != t.id {
		return Upload{}, fmt.Errorf("ingest: reserve upload %s for %q under %q: %w", u.ID, u.Tenant, t.id, ErrTenantMismatch)
	}
	if err := checkStreamName(u.Stream); err != nil {
		return Upload{}, err
	}
	if err := checkHour(u.Hour); err != nil {
		return Upload{}, err
	}
	if len(u.Sources) == 0 {
		return Upload{}, fmt.Errorf("ingest: upload needs at least one source")
	}
	if u.ID == uuid.Nil() {
		u.ID = uuid.NewV7()
	}
	key := objstore.DataKey(t.id, u.Stream, u.Hour, u.ID)
	if u.Key != "" && u.Key != key {
		return Upload{}, fmt.Errorf("%w: key %q, want %q", ErrUploadConflict, u.Key, key)
	}
	res := Upload{
		Tenant: t.id, ID: u.ID, Stream: u.Stream, Hour: u.Hour.UTC(), Key: key,
		Sources: slices.Clone(u.Sources), Stage: UploadReserved,
	}
	var stored Upload
	err := t.mutate(ctx, func(m *tenantManifest) error {
		if i := findUpload(m.Uploads, u.ID); i >= 0 {
			existing := m.Uploads[i]
			if sameUploadSpec(existing, res) {
				stored = existing.Clone()
				return errCovered
			}
			return fmt.Errorf("%w: upload %s already reserved with a different spec", ErrUploadConflict, u.ID)
		}
		for _, src := range u.Sources {
			i := findFile(m.Files, src)
			if i < 0 {
				return fmt.Errorf("%w: source %s is not a committed file", ErrUploadConflict, src)
			}
			f := m.Files[i]
			if f.Retired {
				return fmt.Errorf("%w: source %s is retired", ErrUploadConflict, src)
			}
			if f.Stream != res.Stream || !f.Hour.Equal(res.Hour) {
				return fmt.Errorf("%w: source %s belongs to another stream/hour", ErrUploadConflict, src)
			}
			for _, other := range m.Uploads {
				if slices.Contains(other.Sources, src) {
					return fmt.Errorf("%w: source %s already reserved by upload %s", ErrUploadConflict, src, other.ID)
				}
			}
		}
		// CreatedAt is catalog identity, compared exactly on resume, and the
		// catalog keeps integer microseconds (SQLite) or the exact time
		// (memory). Stamp it once at microsecond precision so the receipt and
		// its catalog row stay equal across a crash and a real catalog reopen.
		res.CreatedAt = t.ops.now().UTC().Truncate(time.Microsecond)
		m.Uploads = append(m.Uploads, res)
		sortUploads(m.Uploads)
		stored = res.Clone()
		return nil
	})
	if err != nil {
		return Upload{}, err
	}
	return stored, nil
}

// sameUploadSpec compares the reservation-defining fields; CreatedAt is
// stable state set by the first commit, not part of the spec.
func sameUploadSpec(a, b Upload) bool {
	a.Sources = slices.Clone(a.Sources)
	b.Sources = slices.Clone(b.Sources)
	slices.SortFunc(a.Sources, uuid.UUID.Compare)
	slices.SortFunc(b.Sources, uuid.UUID.Compare)
	return a.Tenant == b.Tenant && a.Stream == b.Stream && a.Hour.Equal(b.Hour) &&
		a.Key == b.Key && slices.Equal(a.Sources, b.Sources)
}

// RecordMerge durably records the finalized merge output and its stats for
// the catalog row. It requires a finalized upload temp (FinalizeUploadTemp)
// and raises a reserved receipt to merged; at merged or stored it refreshes
// the durable retry hint after a re-merge without downgrading the stage.
// bytes must equal the finalized temp's size.
//
// Every fresh merge result must account for exactly the physical rows of the
// manifest's source files: Rows + DeduplicatedRows must equal their total, so
// a dropped-row result without a matching deduplicated count is rejected
// instead of published. Only the receipt's Rows is persisted; the removed
// count is checked once, when the fresh result is recorded, because resumed
// receipts reuse the already-validated durable temp and receipts written
// before dedup existed carry Rows equal to the source total. This keeps legacy
// receipts resumable without inventing a removed count from a short output.
func (t *TenantStore) RecordMerge(ctx context.Context, id uuid.UUID, stats storage.FileStats, bytes int64) error {
	u := t.lookupUpload(id)
	if u == nil {
		return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
	}
	if u.Stage == UploadPublished {
		return fmt.Errorf("%w: %s already published", ErrUploadStage, id)
	}
	if stats.Rows <= 0 || bytes <= 0 || stats.MaxTS.Before(stats.MinTS) {
		return fmt.Errorf("ingest: record merge %s: invalid stats (rows=%d bytes=%d)", id, stats.Rows, bytes)
	}
	end := u.Hour.Add(time.Hour)
	if stats.MinTS.Before(u.Hour) || !stats.MaxTS.Before(end) {
		return fmt.Errorf("ingest: record merge %s: stats outside hour %s", id, u.Hour)
	}
	if st, err := t.root.Stat(uploadTempName(id)); err != nil {
		return fmt.Errorf("ingest: record merge %s: %w (re-merge first)", id, err)
	} else if st.Size() != bytes {
		return fmt.Errorf("ingest: record merge %s: %d bytes reported, temp has %d", id, bytes, st.Size())
	}
	return t.mutate(ctx, func(m *tenantManifest) error {
		i := findUpload(m.Uploads, id)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
		}
		mu := &m.Uploads[i]
		if mu.Stage == UploadPublished {
			return fmt.Errorf("%w: %s already published", ErrUploadStage, id)
		}
		if err := checkMergeRows(m.Files, mu.Sources, stats); err != nil {
			return fmt.Errorf("ingest: record merge %s: %w", id, err)
		}
		mu.Temp = uploadTempName(id)
		mu.Rows, mu.Bytes = stats.Rows, bytes
		mu.MinTS, mu.MaxTS = stats.MinTS, stats.MaxTS
		mu.LabelValues = cloneLabelValues(stats.LabelValues)
		mu.LabelsComplete = stats.LabelsComplete
		if mu.Stage == UploadReserved {
			mu.Stage = UploadMerged
		}
		return nil
	})
}

// checkMergeRows validates the exact row accounting of a fresh merge result
// against the physical Rows of its manifest source files. storage.Merge removes
// exact duplicates, so Rows + DeduplicatedRows must equal the source total; a
// short output with a missing or wrong removed count would otherwise silently
// discard log records. Both counts must be nonnegative, their sum and the
// source total must not overflow, and a nonempty input may never produce zero
// rows (dedup retains the first row of every equal-timestamp run).
func checkMergeRows(files []LocalFile, sources []uuid.UUID, stats storage.FileStats) error {
	if stats.Rows < 0 || stats.DeduplicatedRows < 0 {
		return fmt.Errorf("rows: merged %d plus %d deduplicated must be nonnegative", stats.Rows, stats.DeduplicatedRows)
	}
	if stats.Rows > math.MaxInt64-stats.DeduplicatedRows {
		return fmt.Errorf("rows: merged %d plus %d deduplicated overflows int64", stats.Rows, stats.DeduplicatedRows)
	}
	var want int64
	for _, src := range sources {
		i := findFile(files, src)
		if i < 0 {
			return fmt.Errorf("rows: source %s is not a committed file", src)
		}
		f := files[i]
		if f.Rows < 0 {
			return fmt.Errorf("rows: source %s has %d rows", src, f.Rows)
		}
		if want > math.MaxInt64-f.Rows {
			return fmt.Errorf("rows: source totals overflow int64")
		}
		want += f.Rows
	}
	if want > 0 && stats.Rows == 0 {
		return fmt.Errorf("rows: merged zero rows from %d source rows", want)
	}
	if accounted := stats.Rows + stats.DeduplicatedRows; accounted != want {
		return fmt.Errorf("rows: merged %d plus %d deduplicated = %d, want %d from sources",
			stats.Rows, stats.DeduplicatedRows, accounted, want)
	}
	return nil
}

// MarkStored records that Store.Put returned success. It is a durable retry
// hint only: orphan GC may have removed the object since, so the uploader must
// rotate to a fresh key and re-Put before Catalog.AddFiles. Idempotent at
// stored.
func (t *TenantStore) MarkStored(ctx context.Context, id uuid.UUID) error {
	return t.mutate(ctx, func(m *tenantManifest) error {
		i := findUpload(m.Uploads, id)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
		}
		switch m.Uploads[i].Stage {
		case UploadMerged:
			m.Uploads[i].Stage = UploadStored
			return nil
		case UploadStored:
			return errCovered
		default:
			return fmt.Errorf("%w: %s is %s", ErrUploadStage, id, m.Uploads[i].Stage)
		}
	})
}

// RecordPut records that Store.Put for the receipt's CURRENT key completed
// at completedAt, atomically advancing the receipt to stored. It is called
// once per key, after the key was either freshly reserved (never Put) or
// rotated, so the completion time always describes that key's only Put. The
// uploader uses it to bound how long the object may wait for the catalog and
// as proof that the key must not be written again. The stage update is
// idempotent at stored: a repeated call only restamps the same key's
// completion and never implies another Store.Put.
func (t *TenantStore) RecordPut(ctx context.Context, id uuid.UUID, completedAt time.Time) error {
	if completedAt.IsZero() {
		return fmt.Errorf("ingest: record put %s: zero completion time", id)
	}
	return t.mutate(ctx, func(m *tenantManifest) error {
		i := findUpload(m.Uploads, id)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
		}
		u := &m.Uploads[i]
		switch u.Stage {
		case UploadMerged, UploadStored:
			u.Stage = UploadStored
			u.PutCompletedAt = completedAt
			return nil
		default:
			return fmt.Errorf("%w: %s is %s", ErrUploadStage, id, u.Stage)
		}
	})
}

// RefreshUploadKey gives an unpublished merged or stored receipt a fresh
// canonical object key (new UUIDv7) before its next Put: object keys are
// immutable, so a receipt that may already name a stored object (recorded
// completion or unknown outcome) must never write the same data or sidecar key
// twice. The new key, cleared PutCompletedAt and the merged stage are committed
// atomically, while the receipt ID, durable merge output (temp), sources, stats
// and CreatedAt are retained. The same commit increments the tenant's
// cumulative KeyRotations counter, so the key change and the count can never
// disagree. The uploader then Puts the durable temp under the new key and
// calls RecordPut.
func (t *TenantStore) RefreshUploadKey(ctx context.Context, id uuid.UUID) (Upload, error) {
	var out Upload
	err := t.mutate(ctx, func(m *tenantManifest) error {
		i := findUpload(m.Uploads, id)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
		}
		u := &m.Uploads[i]
		switch u.Stage {
		case UploadMerged, UploadStored:
		default:
			return fmt.Errorf("%w: %s is %s", ErrUploadStage, id, u.Stage)
		}
		// Reject instead of wrapping: the counter is part of the durable
		// contract, so the commit must neither reset nor reuse a count.
		if m.KeyRotations == math.MaxUint64 {
			return fmt.Errorf("%w: %d", ErrKeyRotationOverflow, m.KeyRotations)
		}
		u.Key = objstore.NewDataKey(t.id, u.Stream, u.Hour)
		u.PutCompletedAt = time.Time{}
		u.Stage = UploadMerged
		m.KeyRotations++
		out = u.Clone()
		return nil
	})
	if err != nil {
		return Upload{}, err
	}
	return out, nil
}

// MarkPublished records that the catalog rows are committed and retires every
// source file in the same atomic manifest commit, so no watermark can race
// the retirement. Idempotent at published.
func (t *TenantStore) MarkPublished(ctx context.Context, id uuid.UUID) error {
	return t.mutate(ctx, func(m *tenantManifest) error {
		i := findUpload(m.Uploads, id)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrUploadNotFound, id)
		}
		switch m.Uploads[i].Stage {
		case UploadStored:
		case UploadPublished:
			return errCovered
		default:
			return fmt.Errorf("%w: %s is %s", ErrUploadStage, id, m.Uploads[i].Stage)
		}
		u := &m.Uploads[i]
		for _, src := range u.Sources {
			j := findFile(m.Files, src)
			if j < 0 {
				return fmt.Errorf("%w: published source %s missing", ErrFileMissing, src)
			}
			m.Files[j].Retired = true
		}
		u.Stage = UploadPublished
		return nil
	})
}

// CompleteUpload finishes a published upload's cleanup: it removes the
// retired source files and the upload temp from disk (each delete followed by
// a directory sync; already-absent files are fine), then drops the receipt
// and the retired source entries in one atomic commit. A receipt already gone
// is a success no-op.
func (t *TenantStore) CompleteUpload(ctx context.Context, id uuid.UUID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return err
	}
	i := findUpload(t.state.Uploads, id)
	if i < 0 {
		return nil
	}
	u := t.state.Uploads[i]
	if u.Stage != UploadPublished {
		return fmt.Errorf("%w: %s is %s", ErrUploadStage, id, u.Stage)
	}
	dirs := make(map[string]bool)
	if u.Temp != "" {
		if err := t.ops.remove(t.root, u.Temp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("ingest: remove upload %s temp: %w", id, err)
		}
		dirs[uploadsDir] = true
	}
	for _, src := range u.Sources {
		j := findFile(t.state.Files, src)
		if j < 0 {
			continue // entry pruned already; nothing on disk to remove
		}
		f := t.state.Files[j]
		if !f.Retired {
			return fmt.Errorf("%w: source %s of upload %s: %w", ErrManifestCorrupt, src, id, ErrNotRetired)
		}
		if err := t.ops.remove(t.root, f.RelPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("ingest: remove retired file %s: %w", f.RelPath(), err)
		}
		dirs[path.Dir(f.RelPath())] = true
	}
	for dir := range dirs {
		if err := t.ops.syncDir(t.root, dir); err != nil {
			return fmt.Errorf("ingest: sync %s after cleanup: %w", dir, err)
		}
	}
	next := cloneManifest(t.state)
	next.Uploads = slices.DeleteFunc(next.Uploads, func(other Upload) bool { return other.ID == id })
	next.Files = slices.DeleteFunc(next.Files, func(f LocalFile) bool {
		return f.Retired && slices.Contains(u.Sources, f.ID)
	})
	return t.commitLocked(next)
}

// OpenFile opens a committed local file for reading. The caller owns and
// closes the returned file (parquet.ReaderAtSeeker-compatible for
// storage.NewReader).
func (t *TenantStore) OpenFile(ctx context.Context, fileID uuid.UUID) (*os.File, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return nil, err
	}
	i := findFile(t.state.Files, fileID)
	if i < 0 {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, fileID)
	}
	f, err := t.root.Open(t.state.Files[i].RelPath())
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrFileMissing, t.state.Files[i].RelPath(), err)
	}
	return f, nil
}

// tempState is the UploadTemp lifecycle: open → synced → finalized, or
// aborted from any earlier state.
type tempState int

const (
	tempOpen tempState = iota
	tempSynced
	tempFinalized
	tempAborted
)

// UploadTemp is an open in-progress merge output. Write feeds it (for
// example as storage.Merge's destination) and counts the bytes. Exactly one
// of FinalizeUploadTemp or AbortUploadTemp finishes it; nothing leaks if
// Finalize fails.
type UploadTemp struct {
	t     *TenantStore
	id    uuid.UUID
	file  syncFile
	bytes int64
	state tempState
}

// Write implements io.Writer toward the temp file, counting bytes.
func (tmp *UploadTemp) Write(p []byte) (int, error) {
	if tmp == nil || tmp.state != tempOpen || tmp.file == nil {
		return 0, errors.New("ingest: write on closed upload temp")
	}
	n, err := tmp.file.Write(p)
	tmp.bytes += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

// Bytes returns the bytes written so far; after FinalizeUploadTemp it equals
// the finalized file size for RecordMerge.
func (tmp *UploadTemp) Bytes() int64 { return tmp.bytes }

// CreateUploadTemp opens the in-progress merge output of a reserved upload
// for (re)writing. The receipt must exist and must not be published. Any
// earlier partial temp is truncated. At most one temp per upload is open at a
// time.
func (t *TenantStore) CreateUploadTemp(ctx context.Context, uploadID uuid.UUID) (*UploadTemp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return nil, err
	}
	i := findUpload(t.state.Uploads, uploadID)
	if i < 0 {
		return nil, fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	if t.state.Uploads[i].Stage == UploadPublished {
		return nil, fmt.Errorf("%w: %s already published", ErrUploadStage, uploadID)
	}
	if t.temps[uploadID] != nil {
		return nil, fmt.Errorf("%w: upload %s temp already open", ErrUploadConflict, uploadID)
	}
	if err := t.ensureDir(uploadsDir); err != nil {
		return nil, err
	}
	file, err := t.ops.create(t.root, uploadTempTmpName(uploadID), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return nil, fmt.Errorf("ingest: create upload %s temp: %w", uploadID, err)
	}
	tmp := &UploadTemp{t: t, id: uploadID, file: file, state: tempOpen}
	t.temps[uploadID] = tmp
	return tmp, nil
}

// FinalizeUploadTemp syncs and closes tmp, renames it to the durable
// .uploads/<id>.parquet and syncs the directory. Retry behavior after a
// rename-success dir-sync error: a retry with the same handle re-attempts
// only the directory sync; a fully finalized temp is a no-op. If Sync or
// Close fails the file is closed and the temp is dead: retrying Finalize then
// reports a stable error and the caller must AbortUploadTemp and re-merge
// through a fresh CreateUploadTemp.
func (t *TenantStore) FinalizeUploadTemp(ctx context.Context, tmp *UploadTemp) error {
	if tmp == nil || tmp.t != t {
		return fmt.Errorf("ingest: upload temp for another tenant")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return err
	}
	switch tmp.state {
	case tempFinalized:
		return nil
	case tempAborted:
		return errors.New("ingest: upload temp already aborted")
	}
	tmpName, finalName := uploadTempTmpName(tmp.id), uploadTempName(tmp.id)
	if tmp.state == tempOpen {
		if tmp.file == nil {
			return fmt.Errorf("%w: upload %s temp closed after a failed sync; abort and recreate", ErrUploadStage, tmp.id)
		}
		syncErr := tmp.file.Sync()
		closeErr := tmp.file.Close()
		tmp.file = nil
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("ingest: sync upload %s temp: %w", tmp.id, err)
		}
		tmp.state = tempSynced
	}
	var err error
	if _, statErr := t.root.Stat(tmpName); statErr == nil {
		err = t.ops.rename(t.root, tmpName, finalName)
	} else if _, statErr := t.root.Stat(finalName); statErr != nil {
		err = fmt.Errorf("ingest: upload %s temp missing: %w", tmp.id, statErr)
	}
	if err == nil {
		err = t.ops.syncDir(t.root, uploadsDir)
	}
	if err != nil {
		return fmt.Errorf("ingest: finalize upload %s temp: %w", tmp.id, err)
	}
	tmp.state = tempFinalized
	delete(t.temps, tmp.id)
	return nil
}

// AbortUploadTemp closes an unfinished temp and quarantines its partial (or
// already renamed) output, so a failed or canceled merge never leaks files
// or handles. The owned handle is always closed and released even when the
// pending sync or the quarantine fails; those errors are joined into the
// result. A finalized temp cannot be aborted.
func (t *TenantStore) AbortUploadTemp(ctx context.Context, tmp *UploadTemp) error {
	if tmp == nil || tmp.t != t {
		return fmt.Errorf("ingest: upload temp for another tenant")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	syncErr := t.ensureSyncedLocked(ctx)
	switch tmp.state {
	case tempFinalized:
		return fmt.Errorf("%w: upload %s temp already finalized", ErrUploadStage, tmp.id)
	case tempAborted:
		return errors.New("ingest: upload temp already aborted")
	}
	errs := []error{syncErr}
	if tmp.file != nil {
		errs = append(errs, tmp.file.Close())
		tmp.file = nil
	}
	tmp.state = tempAborted
	delete(t.temps, tmp.id)
	// Discard whichever output the interrupted finalize reached. Skip the
	// quarantine when the pending sync failed: the manifest on disk may not
	// even cover this tenant dir yet, and the retry happens at recovery.
	if syncErr == nil {
		for _, name := range []string{uploadTempTmpName(tmp.id), uploadTempName(tmp.id)} {
			if err := t.quarantineLocked(name); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// OpenUploadTemp opens the finalized merge output of an upload for streaming
// to Store.Put. An upload that was never finalized (only a partial .tmp)
// reports not found: re-merge first.
func (t *TenantStore) OpenUploadTemp(ctx context.Context, uploadID uuid.UUID) (*os.File, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureSyncedLocked(ctx); err != nil {
		return nil, err
	}
	if findUpload(t.state.Uploads, uploadID) < 0 {
		return nil, fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	f, err := t.root.Open(uploadTempName(uploadID))
	if err != nil {
		return nil, fmt.Errorf("ingest: open upload %s temp: %w", uploadID, err)
	}
	return f, nil
}

func (t *TenantStore) lookupUpload(id uuid.UUID) *Upload {
	t.mu.Lock()
	defer t.mu.Unlock()
	i := findUpload(t.state.Uploads, id)
	if i < 0 {
		return nil
	}
	u := t.state.Uploads[i].Clone()
	return &u
}

// writeRecord writes one sorted part as a local Parquet file via nf, counts
// the bytes including the footer, fsyncs the file, renames it into place and
// fsyncs the partition directory. The returned LocalFile is not yet committed
// to the manifest; Publish does that atomically with the frontier advance.
// An empty record produces no file.
func (t *TenantStore) writeRecord(ctx context.Context, stream string, hour time.Time, rec arrow.RecordBatch, nf WriterFactory, promoted []string) (LocalFile, error) {
	if t.closed.Load() {
		return LocalFile{}, ErrLocalClosed
	}
	if err := ctx.Err(); err != nil {
		return LocalFile{}, err
	}
	if rec == nil || rec.NumRows() == 0 {
		return LocalFile{}, nil
	}
	if err := checkStreamName(stream); err != nil {
		return LocalFile{}, err
	}
	if err := checkHour(hour); err != nil {
		return LocalFile{}, err
	}
	dir := hourDirRelPath(stream, hour)
	if err := t.ensureDir(dir); err != nil {
		return LocalFile{}, err
	}
	id := uuid.NewV7()
	tmpName := dir + "/" + id.String() + parquetExt + tempSuffix
	finalName := dir + "/" + id.String() + parquetExt
	file, err := t.ops.create(t.root, tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return LocalFile{}, fmt.Errorf("ingest: create %s: %w", tmpName, err)
	}
	cw := &countWriter{w: file}
	writer, err := nf(cw, rec.Schema(), promoted)
	var stats storage.FileStats
	if err == nil {
		// Close always runs once the factory succeeded: Write failures must
		// not leak writer resources, and both errors are reported.
		werr := writer.Write(rec)
		var cerr error
		stats, cerr = writer.Close()
		err = errors.Join(werr, cerr)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err = errors.Join(err, closeErr); err != nil {
		t.ops.remove(t.root, tmpName)
		return LocalFile{}, fmt.Errorf("ingest: write %s: %w", tmpName, err)
	}
	if stats.Rows != rec.NumRows() {
		t.ops.remove(t.root, tmpName)
		return LocalFile{}, fmt.Errorf("ingest: %s wrote %d rows, want %d", tmpName, stats.Rows, rec.NumRows())
	}
	if err := ctx.Err(); err != nil {
		t.ops.remove(t.root, tmpName)
		return LocalFile{}, err
	}
	if err := t.ops.rename(t.root, tmpName, finalName); err != nil {
		t.ops.remove(t.root, tmpName)
		return LocalFile{}, fmt.Errorf("ingest: rename %s: %w", tmpName, err)
	}
	if err := t.ops.syncDir(t.root, dir); err != nil {
		return LocalFile{}, fmt.Errorf("ingest: sync %s: %w", dir, err)
	}
	return LocalFile{
		Tenant: t.id, ID: id, Stream: stream, Hour: hour.UTC(),
		Bytes: cw.n, Rows: stats.Rows, MinTS: stats.MinTS, MaxTS: stats.MaxTS,
		LabelValues: cloneLabelValues(stats.LabelValues), LabelsComplete: stats.LabelsComplete,
	}, nil
}

// countWriter counts every byte written, including the Parquet footer
// flushed by Writer.Close.
type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// ensureDir creates every missing level of rel beneath the tenant root and
// fsyncs each directory and its parent, so nested file durability never rests
// on an unsynced parent. It does no manifest work and needs no lock.
func (t *TenantStore) ensureDir(rel string) error {
	segs := strings.Split(rel, "/")
	for i := 1; i <= len(segs); i++ {
		prefix := strings.Join(segs[:i], "/")
		if err := t.root.Mkdir(prefix, dirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("ingest: create %s: %w", prefix, err)
		}
		parent := path.Dir(prefix)
		if err := t.ops.syncDir(t.root, parent); err != nil {
			return fmt.Errorf("ingest: sync %s: %w", parent, err)
		}
	}
	return nil
}

// quarantineLocked moves rel beneath .quarantine/, flattening the path, and
// syncs both directories. A missing rel is a no-op. The caller holds t.mu.
func (t *TenantStore) quarantineLocked(rel string) error {
	if _, err := t.root.Stat(rel); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := t.ensureDir(quarantineDir); err != nil {
		return err
	}
	name := strings.ReplaceAll(rel, "/", "__")
	dst := quarantineDir + "/" + name
	for i := 1; ; i++ {
		if _, err := t.root.Stat(dst); errors.Is(err, fs.ErrNotExist) {
			break
		} else if err != nil {
			return err
		}
		dst = fmt.Sprintf("%s/%s.%d", quarantineDir, name, i)
	}
	if err := t.ops.rename(t.root, rel, dst); err != nil {
		return fmt.Errorf("ingest: quarantine %s: %w", rel, err)
	}
	var errs []error
	if err := t.ops.syncDir(t.root, quarantineDir); err != nil {
		errs = append(errs, err)
	}
	if err := t.ops.syncDir(t.root, path.Dir(rel)); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// reconcile compares the manifest to the directory after a restart:
// uncommitted temporaries and unreferenced Parquet files are quarantined,
// manifest entries whose live file vanished are corruption, retired entries
// already deleted are pruned along with published receipts left without
// sources, and orphan upload temps are quarantined. Unpublished receipts are
// never dropped and no remote success is inferred from missing data.
func (t *TenantStore) reconcile(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	referenced := make(map[string]bool)
	for i := range t.state.Files {
		referenced[t.state.Files[i].RelPath()] = true
	}
	for _, u := range t.state.Uploads {
		if u.Temp != "" {
			referenced[u.Temp] = true
		}
	}
	referenced[manifestName] = true

	entries, err := fs.ReadDir(t.root.FS(), ".")
	if err != nil {
		return fmt.Errorf("ingest: list tenant %s: %w", t.id, err)
	}
	var onDisk []string
	var uploadTemps []string
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case entry.IsDir() && name == quarantineDir:
		case entry.IsDir() && name == uploadsDir:
			sub, err := fs.ReadDir(t.root.FS(), uploadsDir)
			if err != nil {
				return fmt.Errorf("ingest: list %s %s: %w", t.id, uploadsDir, err)
			}
			for _, se := range sub {
				if se.IsDir() {
					if err := t.quarantineLocked(uploadsDir + "/" + se.Name()); err != nil {
						return err
					}
					continue
				}
				uploadTemps = append(uploadTemps, uploadsDir+"/"+se.Name())
			}
		case entry.IsDir():
			if logs.SanitizeStream(name) != name {
				if err := t.quarantineLocked(name); err != nil {
					return err
				}
				continue
			}
			err := fs.WalkDir(t.root.FS(), name, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				onDisk = append(onDisk, p)
				return nil
			})
			if err != nil {
				return fmt.Errorf("ingest: scan %s %s: %w", t.id, name, err)
			}
		default:
			// A stray file at the tenant root (e.g. manifest.json.tmp).
			if !referenced[name] {
				if err := t.quarantineLocked(name); err != nil {
					return err
				}
			}
		}
	}
	for _, p := range uploadTemps {
		if !referenced[p] {
			if err := t.quarantineLocked(p); err != nil {
				return err
			}
		}
	}
	// Manifest file entries whose bytes vanished: a live file is corruption.
	// A retired file already deleted by an interrupted CompleteUpload stays
	// listed — the published receipt still references it, and only
	// CompleteUpload drops receipt and entries atomically.
	for i := range t.state.Files {
		f := &t.state.Files[i]
		if _, err := t.root.Stat(f.RelPath()); err == nil {
			referenced[f.RelPath()] = true
		} else if errors.Is(err, fs.ErrNotExist) {
			if !f.Retired {
				return fmt.Errorf("%w: %s", ErrFileMissing, f.RelPath())
			}
		} else {
			return fmt.Errorf("ingest: stat %s: %w", f.RelPath(), err)
		}
	}
	// Unreferenced stream files: uncommitted writes are quarantined, never
	// trusted as publication evidence.
	for _, p := range onDisk {
		if !referenced[p] {
			if err := t.quarantineLocked(p); err != nil {
				return err
			}
		}
	}
	return nil
}
