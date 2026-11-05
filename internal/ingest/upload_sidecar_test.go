package ingest

// Tests for A2a: the uploader builds a search sidecar from the finalized merge
// output, stores it under the current data key before cataloging the file, and
// never lets a catalog flag outlive proof of that Put. They also pin the retry
// matrix (data Put, build, sidecar Put and AddFiles failures), durable
// completeness carry-over, backfill-mutated HasSidecar identity, over-cap
// publication and shared metric registration.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadOpLog records object and catalog operations in order and observes the
// sidecar at AddFiles time.
type uploadOpLog struct {
	events       []string
	sidecarAtAdd []bool
}

type uploadLoggedStore struct {
	objstore.Store
	log *uploadOpLog
}

func (s *uploadLoggedStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		return err
	}
	s.log.events = append(s.log.events, "put "+key)
	return nil
}

type uploadLoggedCatalog struct {
	catalog.Catalog
	log     *uploadOpLog
	objects objstore.Store
}

func (c *uploadLoggedCatalog) AddFiles(ctx context.Context, files ...catalog.File) error {
	for _, f := range files {
		_, err := c.objects.Stat(ctx, objstore.SidecarKey(f.Key))
		c.log.sidecarAtAdd = append(c.log.sidecarAtAdd, err == nil)
		c.log.events = append(c.log.events, "addfiles "+f.Key)
	}
	return c.Catalog.AddFiles(ctx, files...)
}

// uploadFailSidecarPut stores every data object but refuses sidecar keys.
type uploadFailSidecarPut struct {
	objstore.Store
	err error
}

func (s *uploadFailSidecarPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if strings.HasSuffix(key, objstore.ExtSidecar) {
		return s.err
	}
	return s.Store.Put(ctx, key, r, size)
}

// uploadManualClock is a clock advanced explicitly by a test.
type uploadManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *uploadManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *uploadManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// uploadSlowSidecarStore advances a clock once, when the first sidecar Put
// completes. That models the sidecar build/Put work that can outlive the
// orphan grace after the data Put was stamped.
type uploadSlowSidecarStore struct {
	objstore.Store
	clock     *uploadManualClock
	advanceBy time.Duration
	advanced  bool
}

func (s *uploadSlowSidecarStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		return err
	}
	if !s.advanced && strings.HasSuffix(key, objstore.ExtSidecar) {
		s.advanced = true
		s.clock.Advance(s.advanceBy)
	}
	return nil
}

// uploadSidecarBytes reads and decodes the sidecar of key.
func uploadSidecarBytes(t *testing.T, f *uploadFixture, key string) []byte {
	t.Helper()
	rc, err := f.objects.Get(t.Context(), objstore.SidecarKey(key))
	if err != nil {
		t.Fatalf("get sidecar for %q: %v", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read sidecar for %q: %v", key, err)
	}
	return data
}

func TestUploaderPublishesSidecarBeforeCatalog(t *testing.T) {
	hour := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, nil)
			f.seed(tenant.Default, "checkout", hour, []uploadRow{
				{ts: hour.Add(time.Minute), line: "checkout failure code=payment"},
				{ts: hour.Add(2 * time.Minute), line: "cart abandoned by user"},
			})

			log := &uploadOpLog{}
			objects := &uploadLoggedStore{Store: f.objects, log: log}
			cat := &uploadLoggedCatalog{Catalog: f.cat, log: log, objects: f.objects}
			report, err := f.uploader(UploaderOptions{
				Objects: objects, Catalog: cat, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
			}).RunOnce(t.Context())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if report.Uploads != 1 {
				t.Fatalf("report = %+v, want one upload", report)
			}
			files := f.liveCatalogFiles(tenant.Default)
			if len(files) != 1 {
				t.Fatalf("catalog rows = %d, want 1", len(files))
			}
			file := files[0]
			if !file.HasSidecar {
				t.Fatal("catalog row HasSidecar = false after a successful sidecar Put")
			}
			if !file.LabelsComplete {
				t.Fatal("catalog row LabelsComplete = false, want the merge's complete summary")
			}

			// The recorded order is data Put, sidecar Put, then AddFiles, and
			// the sidecar object already existed when AddFiles was called.
			sidecarKey := objstore.SidecarKey(file.Key)
			if len(log.events) != 3 {
				t.Fatalf("events = %v, want data Put, sidecar Put, AddFiles", log.events)
			}
			if log.events[0] != "put "+file.Key || log.events[1] != "put "+sidecarKey || log.events[2] != "addfiles "+file.Key {
				t.Fatalf("events = %v, want ordered publication of %q and %q", log.events, file.Key, sidecarKey)
			}
			if len(log.sidecarAtAdd) != 1 || !log.sidecarAtAdd[0] {
				t.Fatalf("sidecarAtAdd = %v, want the sidecar present at AddFiles", log.sidecarAtAdd)
			}

			filter, err := index.Decode(uploadSidecarBytes(t, f, file.Key))
			if err != nil {
				t.Fatalf("Decode sidecar: %v", err)
			}
			for _, needle := range []string{"checkout", "failure", "payment", "abandoned"} {
				if !filter.MayContain(needle) {
					t.Fatalf("sidecar MayContain(%q) = false, want true", needle)
				}
			}
			if filter.MayContain("needle-absent-from-every-line") {
				t.Fatal("sidecar MayContain(absent needle) = true")
			}
			if got := f.receipts(tenant.Default); len(got) != 0 {
				t.Fatalf("receipts after publication = %+v, want none", got)
			}
		})
	}
}

func TestUploaderSidecarFailureMatrix(t *testing.T) {
	hour := time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC)

	t.Run("data put failure writes no sidecar", func(t *testing.T) {
		f := uploadNewFixture(t)
		source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "no data object"}})
		putErr := errors.New("data put refused")
		_, err := f.uploader(UploaderOptions{
			Objects:  &uploadFailBeforePut{Store: f.objects, err: putErr},
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context())
		if !errors.Is(err, putErr) {
			t.Fatalf("RunOnce error = %v, want %v", err, putErr)
		}
		receipt := f.onlyReceipt(tenant.Default)
		if receipt.Stage != UploadMerged || !receipt.PutCompletedAt.IsZero() {
			t.Fatalf("receipt = %+v, want merged without a Put completion", receipt)
		}
		if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(receipt.Key)); !errors.Is(err, objstore.ErrNotExist) {
			t.Fatalf("sidecar written without a data Put: %v", err)
		}
		if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
			t.Fatalf("catalog rows = %d, want none", len(got))
		}
		if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
			t.Fatalf("source lost on data Put failure: %v", err)
		}

		f.reopen()
		report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
		if err != nil {
			t.Fatalf("retry RunOnce: %v", err)
		}
		if report.Uploads != 1 || report.Resumed != 1 {
			t.Fatalf("retry = %+v, want one resumed upload", report)
		}
		file := f.liveCatalogFiles(tenant.Default)[0]
		if !file.HasSidecar {
			t.Fatal("retry published without HasSidecar")
		}
		uploadSidecarBytes(t, f, file.Key)
	})

	t.Run("sidecar put failure blocks publication", func(t *testing.T) {
		f := uploadNewFixture(t)
		source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "sidecar put"}})
		putErr := errors.New("sidecar put refused")
		seedPuts := &uploadKeyListPut{Store: f.objects}
		_, err := f.uploader(UploaderOptions{
			Objects:  &uploadFailSidecarPut{Store: seedPuts, err: putErr},
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context())
		if !errors.Is(err, putErr) {
			t.Fatalf("RunOnce error = %v, want %v", err, putErr)
		}
		receipt := f.onlyReceipt(tenant.Default)
		if receipt.Stage != UploadStored || receipt.PutCompletedAt.IsZero() {
			t.Fatalf("receipt = %+v, want stored with a data Put completion", receipt)
		}
		if _, err := f.objects.Stat(t.Context(), receipt.Key); err != nil {
			t.Fatalf("data object missing after sidecar-only failure: %v", err)
		}
		if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(receipt.Key)); !errors.Is(err, objstore.ErrNotExist) {
			t.Fatalf("sidecar exists despite the refused Put: %v", err)
		}
		if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
			t.Fatalf("catalog rows after sidecar Put failure = %d, want none", len(got))
		}
		if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
			t.Fatalf("source lost on sidecar Put failure: %v", err)
		}
		oldKey := receipt.Key
		oldBytes := uploadObjectBytes(t, f, oldKey)

		f.reopen()

		// The stored key already received its data Put, so the retry rotates
		// instead of overwriting it.
		retryPuts := &uploadKeyListPut{Store: f.objects}
		report, err := f.uploader(UploaderOptions{
			Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context())
		if err != nil {
			t.Fatalf("retry RunOnce: %v", err)
		}
		if report.Uploads != 1 || report.Resumed != 1 {
			t.Fatalf("retry = %+v, want one resumed upload", report)
		}
		file := f.liveCatalogFiles(tenant.Default)[0]
		if file.Key == oldKey {
			t.Fatalf("retry rewrote the stored key %q", oldKey)
		}
		if !file.HasSidecar {
			t.Fatal("retry published without HasSidecar")
		}
		if got := uploadObjectBytes(t, f, oldKey); !bytes.Equal(got, oldBytes) {
			t.Fatal("retry rewrote the old data object")
		}
		if !slices.Equal(retryPuts.keys, []string{file.Key, objstore.SidecarKey(file.Key)}) {
			t.Fatalf("retry Put keys = %v, want the new data/sidecar pair for %q", retryPuts.keys, file.Key)
		}
		if !slices.Equal(seedPuts.keys, []string{oldKey}) {
			t.Fatalf("seed Put keys = %v, want only the data key %q", seedPuts.keys, oldKey)
		}
		uploadSidecarBytes(t, f, file.Key)
	})

	t.Run("build read failure blocks publication until remerge", func(t *testing.T) {
		f := uploadNewFixture(t)
		source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "corrupt temp"}})

		broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
		if _, err := f.uploader(UploaderOptions{
			Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context()); err == nil {
			t.Fatal("RunOnce with failing catalog succeeded")
		}
		receipt := f.onlyReceipt(tenant.Default)
		oldBytes := uploadObjectBytes(t, f, receipt.Key)
		temp := f.tempPath(tenant.Default, receipt.ID.String())
		info, err := os.Stat(temp)
		if err != nil {
			t.Fatalf("stat temp: %v", err)
		}
		// Same size, unreadable Parquet: ensureMerged keeps the temp, so the
		// failure happens in the sidecar read/build step.
		garbage := append([]byte("not parquet"), make([]byte, info.Size()-11)...)
		if err := os.WriteFile(temp, garbage, 0o640); err != nil {
			t.Fatalf("corrupt temp: %v", err)
		}
		f.reopen()

		_, err = f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
		if err == nil || !strings.Contains(err.Error(), "sidecar") {
			t.Fatalf("RunOnce error = %v, want a sidecar read/build failure", err)
		}
		after := f.onlyReceipt(tenant.Default)
		if after.ID != receipt.ID || after.Stage != UploadStored {
			t.Fatalf("receipt = %+v, want the stored receipt retained", after)
		}
		if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
			t.Fatalf("catalog rows after build failure = %d, want none", len(got))
		}
		// The earlier attempt's sidecar may still sit under the old key. It
		// proves nothing and must never be reused: the retry rotates and
		// rebuilds both objects from the remerged temp.
		if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
			t.Fatalf("source lost on build failure: %v", err)
		}

		// Dropping the unreadable temp forces a remerge from the preserved
		// source; the retry then publishes a fresh data object and sidecar.
		if err := os.Remove(temp); err != nil {
			t.Fatalf("remove corrupt temp: %v", err)
		}
		report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
		if err != nil {
			t.Fatalf("remerge RunOnce: %v", err)
		}
		if report.Uploads != 1 {
			t.Fatalf("remerge report = %+v, want one upload", report)
		}
		file := f.liveCatalogFiles(tenant.Default)[0]
		if file.Key == receipt.Key {
			t.Fatalf("remerge rewrote the stored key %q", receipt.Key)
		}
		if got := uploadObjectBytes(t, f, receipt.Key); !bytes.Equal(got, oldBytes) {
			t.Fatal("remerge rewrote the old data object")
		}
		if !file.HasSidecar || !file.LabelsComplete {
			t.Fatalf("remerged row = %+v, want complete summary with sidecar", file)
		}
		if rows := f.readObject(tenant.Default, file.Key); len(rows) != 1 || rows[0].line != "corrupt temp" {
			t.Fatalf("remerged rows = %+v, want the source row once", rows)
		}
	})

	t.Run("successful sidecar then failed AddFiles", func(t *testing.T) {
		f := uploadNewFixture(t)
		source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "catalog down"}})
		catErr := errors.New("catalog unavailable")
		seedPuts := &uploadKeyListPut{Store: f.objects}
		if _, err := f.uploader(UploaderOptions{
			Objects: seedPuts, Catalog: &uploadFailingCatalog{Catalog: f.cat, err: catErr},
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context()); !errors.Is(err, catErr) {
			t.Fatalf("RunOnce error = %v, want %v", err, catErr)
		}
		receipt := f.onlyReceipt(tenant.Default)
		if receipt.Stage != UploadStored {
			t.Fatalf("receipt stage = %q, want stored", receipt.Stage)
		}
		if _, err := f.objects.Stat(t.Context(), receipt.Key); err != nil {
			t.Fatalf("data object missing: %v", err)
		}
		uploadSidecarBytes(t, f, receipt.Key)
		oldSidecarKey := objstore.SidecarKey(receipt.Key)
		if !slices.Equal(seedPuts.keys, []string{receipt.Key, oldSidecarKey}) {
			t.Fatalf("seed Put keys = %v, want the old data/sidecar pair", seedPuts.keys)
		}
		oldBytes := uploadObjectBytes(t, f, receipt.Key)
		if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
			t.Fatalf("catalog rows after failed AddFiles = %d, want none", len(got))
		}
		if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
			t.Fatalf("source lost on failed AddFiles: %v", err)
		}

		f.reopen()
		// Remove the sidecar the failed attempt left behind: only a retry that
		// actually Puts the current data+sidecar pair can publish a flag that
		// points at an object. The old key must never be rewritten, so it
		// stays absent.
		if err := f.objects.Delete(t.Context(), oldSidecarKey); err != nil {
			t.Fatalf("delete stale sidecar: %v", err)
		}
		log := &uploadOpLog{}
		report, err := f.uploader(UploaderOptions{
			Objects:  &uploadLoggedStore{Store: f.objects, log: log},
			Catalog:  &uploadLoggedCatalog{Catalog: f.cat, log: log, objects: f.objects},
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(t.Context())
		if err != nil {
			t.Fatalf("retry RunOnce: %v", err)
		}
		if report.Uploads != 1 || report.Resumed != 1 {
			t.Fatalf("retry = %+v, want one resumed upload", report)
		}
		newKey := report.Completed[0].Key
		if newKey == receipt.Key {
			t.Fatalf("retry rewrote the stored key %q", receipt.Key)
		}
		newSidecarKey := objstore.SidecarKey(newKey)
		wantEvents := []string{"put " + newKey, "put " + newSidecarKey, "addfiles " + newKey}
		if !slices.Equal(log.events, wantEvents) {
			t.Fatalf("retry events = %v, want exactly %v", log.events, wantEvents)
		}
		if len(log.sidecarAtAdd) != 1 || !log.sidecarAtAdd[0] {
			t.Fatalf("sidecarAtAdd = %v, want the rebuilt sidecar present at AddFiles", log.sidecarAtAdd)
		}
		file := f.liveCatalogFiles(tenant.Default)[0]
		if !file.HasSidecar || file.Key != newKey {
			t.Fatalf("row = %+v, want HasSidecar under the fresh key %q", file, newKey)
		}
		if got := uploadObjectBytes(t, f, receipt.Key); !bytes.Equal(got, oldBytes) {
			t.Fatal("retry rewrote the old data object")
		}
		if _, err := f.objects.Stat(t.Context(), oldSidecarKey); !errors.Is(err, objstore.ErrNotExist) {
			t.Fatalf("retry recreated the old sidecar %q: %v", oldSidecarKey, err)
		}
		uploadSidecarBytes(t, f, newKey)
	})
}

func TestUploaderRebuildsSidecarAfterSlowSidecarWorkRotatesKey(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "slow sidecar work"},
	})

	clock := &uploadManualClock{now: time.Now()}
	objects := &uploadSlowSidecarStore{
		Store: f.objects, clock: clock, advanceBy: uploadOrphanGrace + time.Minute,
	}
	report, err := f.uploader(UploaderOptions{
		Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20, Now: clock.Now,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("report = %+v, want one upload", report)
	}
	res := report.Completed[0]
	rows := f.liveCatalogFiles(tenant.Default)
	if len(rows) != 1 || rows[0].Key != res.Key {
		t.Fatalf("catalog = %+v, want one row under %q", rows, res.Key)
	}
	if !rows[0].HasSidecar {
		t.Fatal("published row lacks HasSidecar")
	}
	// The clock advanced only after the first sidecar Put; a completion
	// timestamp stamped after sidecar work would have looked fresh and left a
	// single key. Two distinct data keys prove the re-check ran after that
	// work with the pre-work completion.
	if !strings.HasSuffix(res.Key, objstore.ExtData) {
		t.Fatalf("result key = %q, want a data key", res.Key)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
	// The sidecar for the published (fresh) key exists and describes the row;
	// the stale key's sidecar is left for orphan GC.
	if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(res.Key)); err != nil {
		t.Fatalf("published-key sidecar missing: %v", err)
	}
	uploadSidecarBytes(t, f, res.Key)
	var staleSidecars int
	for k := range uploadSidecarKeys(t, f) {
		if k != objstore.SidecarKey(res.Key) {
			staleSidecars++
		}
	}
	if staleSidecars != 1 {
		t.Fatalf("stale sidecars = %d, want the one rotated attempt", staleSidecars)
	}
}

// uploadSidecarKeys lists every sidecar key in the fixture's object store.
func uploadSidecarKeys(t *testing.T, f *uploadFixture) map[string]bool {
	t.Helper()
	out := make(map[string]bool)
	for info, err := range f.objects.List(t.Context(), "") {
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}
		if strings.HasSuffix(info.Key, objstore.ExtSidecar) {
			out[info.Key] = true
		}
	}
	return out
}

func TestUploaderSidecarBuildMetrics(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	// Deterministic realistic corpus: repeated request templates with varying
	// ids/status/latency, 2000 rows (about 32 minutes of one stream).
	rows := make([]uploadRow, 0, 2000)
	for i := range 2000 {
		rows = append(rows, uploadRow{
			ts:   hour.Add(time.Duration(i) * time.Second),
			line: fmt.Sprintf("GET /api/v1/checkout/items id=%06d status=%d latency_ms=%d", i, []int{200, 200, 500}[i%3], i%97),
		})
	}
	f.seed(tenant.Default, "api", hour, rows)

	reg := prometheus.NewRegistry()
	im, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("index.RegisterMetrics: %v", err)
	}
	if _, err := f.uploader(UploaderOptions{
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 8 << 20, IndexMetrics: im,
	}).RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	file := f.liveCatalogFiles(tenant.Default)[0]
	if file.Rows != 2000 {
		t.Fatalf("catalog rows = %d, want 2000", file.Rows)
	}
	sidecar := uploadSidecarBytes(t, f, file.Key)
	if v, ok := gatherLabeled(t, reg, "parkana_index_built_total", "source", string(index.SourceIngest)); !ok || v != 1 {
		t.Fatalf("built counter = %v (present %v), want 1", v, ok)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_index_bytes_total", "source", string(index.SourceIngest)); !ok || v != float64(len(sidecar)) {
		t.Fatalf("bytes counter = %v (present %v), want %d", v, ok, len(sidecar))
	}
	count, sum := gatherHistogram(t, reg, "parkana_index_build_duration_seconds", "source", string(index.SourceIngest))
	if count != 1 || sum <= 0 {
		t.Fatalf("build duration histogram = count %d sum %v, want one positive observation", count, sum)
	}
	t.Logf("source=ingest sidecar build: %d rows, %d Parquet bytes, %d sidecar bytes, %.6fs (%.3f ms)",
		file.Rows, file.Bytes, len(sidecar), sum, sum*1000)
}

// gatherHistogram returns a histogram family's sample count and sum for the
// given label pair.
func gatherHistogram(t *testing.T, reg *prometheus.Registry, name, label, value string) (uint64, float64) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			match := false
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					match = true
				}
			}
			if match && m.GetHistogram() != nil {
				return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0, 0
}

func TestUploaderOverCapPublishesWithoutSidecar(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 4, 13, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "over the gram cap"}})

	reg := prometheus.NewRegistry()
	im, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("index.RegisterMetrics: %v", err)
	}
	report, err := f.uploader(UploaderOptions{
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Index:        index.Params{N: 4, FPRate: 0.01, MaxGrams: 1},
		IndexMetrics: im,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("report = %+v, want the over-cap file published", report)
	}
	rows := f.liveCatalogFiles(tenant.Default)
	if len(rows) != 1 {
		t.Fatalf("catalog rows = %d, want 1", len(rows))
	}
	file := rows[0]
	if file.HasSidecar {
		t.Fatal("over-cap file published with HasSidecar=true")
	}
	if !file.LabelsComplete {
		t.Fatal("over-cap publication lost the complete label summary")
	}
	if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(file.Key)); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("sidecar object exists for an over-cap file: %v", err)
	}
	if got := f.readObject(tenant.Default, file.Key); len(got) != 1 {
		t.Fatalf("over-cap object rows = %d, want 1", len(got))
	}
	if v, ok := gatherLabeled(t, reg, "parkana_index_skipped_too_many_grams_total", "source", string(index.SourceIngest)); !ok || v != 1 {
		t.Fatalf("skip counter = %v (present %v), want 1", v, ok)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_index_built_total", "source", string(index.SourceIngest)); ok && v != 0 {
		t.Fatalf("built counter = %v, want 0 for an over-cap build", v)
	}
}

func TestUploaderCompletenessSurvivesReopen(t *testing.T) {
	hour := time.Date(2026, 11, 4, 14, 0, 0, 0, time.UTC)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, nil)
			f.promote = func(string) []string { return []string{"level"} }
			f.seed(tenant.Default, "api", hour, []uploadRow{
				{ts: hour.Add(time.Minute), line: "complete", level: uploadText("info")},
			})

			broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
			if _, err := f.uploader(UploaderOptions{
				Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
			}).RunOnce(t.Context()); err == nil {
				t.Fatal("RunOnce with failing catalog succeeded")
			}
			receipt := f.onlyReceipt(tenant.Default)
			if !receipt.LabelsComplete {
				t.Fatal("merged receipt LabelsComplete = false, want the writer's complete summary")
			}

			f.reopen()
			reopened := f.onlyReceipt(tenant.Default)
			if !reopened.LabelsComplete {
				t.Fatal("reopened receipt lost LabelsComplete")
			}
			if !mapsEqual(reopened.LabelValues, receipt.LabelValues) {
				t.Fatalf("reopened label values = %v, want %v", reopened.LabelValues, receipt.LabelValues)
			}

			report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
			if err != nil {
				t.Fatalf("retry RunOnce: %v", err)
			}
			if report.Uploads != 1 {
				t.Fatalf("retry = %+v, want one upload", report)
			}
			file := f.liveCatalogFiles(tenant.Default)[0]
			if !file.LabelsComplete {
				t.Fatal("catalog row lost LabelsComplete")
			}
			if !file.HasSidecar {
				t.Fatal("catalog row HasSidecar = false")
			}
		})
	}
}

func TestUploaderNeverUpgradesLegacyIncompleteReceipt(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	f.promote = func(string) []string { return []string{"level"} }
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "legacy", level: uploadText("info")},
	})

	broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
	if _, err := f.uploader(UploaderOptions{
		Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context()); err == nil {
		t.Fatal("RunOnce with failing catalog succeeded")
	}
	receipt := f.onlyReceipt(tenant.Default)
	if err := f.tenantStore(tenant.Default).mutate(t.Context(), func(m *tenantManifest) error {
		i := findUpload(m.Uploads, receipt.ID)
		if i < 0 {
			return errors.New("receipt not in manifest")
		}
		// What a pre-A1 manifest decodes to: no completeness field.
		m.Uploads[i].LabelsComplete = false
		return nil
	}); err != nil {
		t.Fatalf("write legacy receipt: %v", err)
	}
	// The committed bytes must match an old manifest, not encode
	// labelsComplete:false (json/v2 omitempty would keep the false bool).
	onDisk, err := os.ReadFile(filepath.Join(f.dataDir, "parquet", string(tenant.Default), "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if bytes.Contains(onDisk, []byte("labelsComplete")) {
		t.Fatalf("legacy manifest bytes carry labelsComplete: %s", onDisk)
	}
	f.reopen()

	report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("report = %+v, want one upload", report)
	}
	file := f.liveCatalogFiles(tenant.Default)[0]
	if file.LabelsComplete {
		t.Fatal("legacy incomplete receipt was relabeled complete without recomputing")
	}
	if !file.HasSidecar {
		t.Fatal("legacy receipt published without its sidecar")
	}
	if !mapsEqual(file.LabelValues, receipt.LabelValues) {
		t.Fatalf("legacy summary values changed: %v, want %v", file.LabelValues, receipt.LabelValues)
	}
}

func TestUploaderCatalogFirstIgnoresBackfilledHasSidecar(t *testing.T) {
	hour := time.Date(2026, 11, 4, 16, 0, 0, 0, time.UTC)
	for _, kind := range uploadCatalogKinds() {
		for _, deleted := range []bool{false, true} {
			name := string(kind) + "/live"
			if deleted {
				name = string(kind) + "/tombstoned"
			}
			t.Run(name, func(t *testing.T) {
				f := uploadNewFixtureKind(t, kind, nil)
				f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "backfilled"}})

				broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
				if _, err := f.uploader(UploaderOptions{
					Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				}).RunOnce(t.Context()); err == nil {
					t.Fatal("RunOnce with failing catalog succeeded")
				}
				receipt := f.onlyReceipt(tenant.Default)

				// Crash after publication; backfill later sets the sidecar flag
				// (or leaves it false). The flag is mutable and must not make
				// the row a conflict.
				row := uploadCatalogFile(receipt)
				row.HasSidecar = true
				row.LabelsComplete = receipt.LabelsComplete
				if err := f.cat.AddFiles(t.Context(), row); err != nil {
					t.Fatalf("seed catalog row: %v", err)
				}
				if deleted {
					if err := f.cat.ReplaceFiles(t.Context(), tenant.Default, []string{row.Key}, nil); err != nil {
						t.Fatalf("tombstone row: %v", err)
					}
				}
				f.reopen()

				objects := &uploadCountingPut{Store: f.objects}
				counting := &uploadCountingCatalog{Catalog: f.cat}
				report, err := f.uploader(UploaderOptions{
					Objects: objects, Catalog: counting,
					MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				}).RunOnce(t.Context())
				if err != nil {
					t.Fatalf("catalog-first RunOnce: %v (want no conflict for mutable HasSidecar)", err)
				}
				if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
					t.Fatalf("resume = %+v, want receipt %s", report, receipt.ID)
				}
				if objects.calls != 0 || counting.calls != 0 {
					t.Fatalf("resume did %d Puts and %d AddFiles, want neither", objects.calls, counting.calls)
				}
				rows := f.allCatalogFiles(tenant.Default)
				if len(rows) != 1 || rows[0].Key != receipt.Key || rows[0].Deleted() != deleted || !rows[0].HasSidecar {
					t.Fatalf("catalog rows = %+v, want the untouched backfilled row", rows)
				}
				if got := f.receipts(tenant.Default); len(got) != 0 {
					t.Fatalf("receipts = %+v, want none", got)
				}
			})
		}
	}
}

func TestUploaderReopenAtReservedMergedStored(t *testing.T) {
	hour := time.Date(2026, 11, 4, 17, 0, 0, 0, time.UTC)

	t.Run("reserved", func(t *testing.T) {
		f := uploadNewFixture(t)
		f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "reserved reopen"}})
		u := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20})
		u.merge = func(context.Context, io.Writer, []*storage.Reader, []string) (storage.FileStats, error) {
			return storage.FileStats{}, errors.New("injected merge failure")
		}
		if _, err := u.RunOnce(t.Context()); err == nil {
			t.Fatal("RunOnce with failing merge succeeded")
		}
		if got := f.onlyReceipt(tenant.Default); got.Stage != UploadReserved {
			t.Fatalf("receipt stage = %q, want reserved", got.Stage)
		}
		f.reopen()
		report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
		if err != nil {
			t.Fatalf("retry RunOnce: %v", err)
		}
		if report.Uploads != 1 || report.Resumed != 1 {
			t.Fatalf("retry = %+v, want one resumed upload", report)
		}
		file := f.liveCatalogFiles(tenant.Default)[0]
		if !file.HasSidecar || !file.LabelsComplete {
			t.Fatalf("row = %+v, want HasSidecar and complete summary", file)
		}
		uploadSidecarBytes(t, f, file.Key)
	})

	t.Run("merged and stored", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			failPut  bool
			failCata bool
		}{
			{"merged", true, false},
			{"stored", false, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := uploadNewFixture(t)
				f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: tc.name + " reopen"}})
				opts := UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}
				if tc.failPut {
					opts.Objects = &uploadFailBeforePut{Store: f.objects, err: errors.New("put down")}
				}
				if tc.failCata {
					opts.Catalog = &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog down")}
				}
				if _, err := f.uploader(opts).RunOnce(t.Context()); err == nil {
					t.Fatal("seeding RunOnce succeeded unexpectedly")
				}
				wantStage := UploadMerged
				if tc.failCata {
					wantStage = UploadStored
				}
				if got := f.onlyReceipt(tenant.Default); got.Stage != wantStage {
					t.Fatalf("receipt stage = %q, want %q", got.Stage, wantStage)
				}
				f.reopen()
				report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
				if err != nil {
					t.Fatalf("retry RunOnce: %v", err)
				}
				if report.Uploads != 1 || report.Resumed != 1 {
					t.Fatalf("retry = %+v, want one resumed upload", report)
				}
				file := f.liveCatalogFiles(tenant.Default)[0]
				if !file.HasSidecar || !file.LabelsComplete {
					t.Fatalf("row = %+v, want HasSidecar and complete summary", file)
				}
				uploadSidecarBytes(t, f, file.Key)
			})
		}
	})
}

func TestServiceSharesIndexMetricsWithOtherRoles(t *testing.T) {
	deps, reg := testDeps(t, t.TempDir())
	deps.Config.Index = config.Index{N: 5, FPRate: 0.02, MaxGrams: 3 << 20}
	deps.Config.Ingest.UploadMinBytes = 1
	// Another role (for example the compactor) registers first.
	other, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("other role RegisterMetrics: %v", err)
	}
	other.ObserveBuild(index.SourceCompaction, time.Millisecond, 12)
	s := newTestService(t, deps, serviceOptions{})
	if s.opts.indexMetrics != other {
		t.Fatal("service did not reuse the shared index.Metrics handle")
	}

	// Drive the service's real uploader constructor, not a hand-built one, so
	// this test fails if production wiring drops the params or the handle.
	objects, ok := deps.Store.(*objstore.FS)
	if !ok {
		t.Fatalf("test store is %T, want *objstore.FS", deps.Store)
	}
	local, err := OpenLocal(t.Context(), deps.Config.DataDir)
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Errorf("local.Close: %v", err)
		}
	})
	f := &uploadFixture{
		t: t, dataDir: deps.Config.DataDir, local: local, objects: objects,
		cat: deps.Catalog, catKind: uploadCatalogMemory,
		promote: func(string) []string { return []string{"level"} },
		seq:     make(map[tenant.ID]uint64),
	}
	hour := time.Date(2026, 11, 4, 18, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "GET /api/checkout status=200 code=abc", level: uploadText("info")},
	})

	upl, err := s.opts.newUploader(local, objects, deps.Catalog)
	if err != nil {
		t.Fatalf("service newUploader: %v", err)
	}
	u, ok := upl.(*Uploader)
	if !ok {
		t.Fatalf("service uploader is %T, want *Uploader", upl)
	}
	if u.indexParams != deps.Config.Index.Params() {
		t.Fatalf("uploader index params = %+v, want the config's %+v", u.indexParams, deps.Config.Index.Params())
	}
	if u.indexMetrics != other {
		t.Fatal("service uploader did not receive the shared index.Metrics handle")
	}
	report, err := upl.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("upload RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("report = %+v, want one upload", report)
	}
	file := f.liveCatalogFiles(tenant.Default)[0]
	if !file.HasSidecar || !file.LabelsComplete {
		t.Fatalf("row = %+v, want HasSidecar and complete summary", file)
	}
	sidecar := uploadSidecarBytes(t, f, file.Key)
	if v, ok := gatherLabeled(t, reg, "parkana_index_built_total", "source", string(index.SourceCompaction)); !ok || v != 1 {
		t.Fatalf("compaction built counter = %v (present %v), want 1", v, ok)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_index_built_total", "source", string(index.SourceIngest)); !ok || v != 1 {
		t.Fatalf("ingest built counter = %v (present %v), want 1 from the real uploader", v, ok)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_index_bytes_total", "source", string(index.SourceIngest)); !ok || v != float64(len(sidecar)) {
		t.Fatalf("ingest sidecar bytes = %v (present %v), want %d", v, ok, len(sidecar))
	}
	count, sum := gatherHistogram(t, reg, "parkana_index_build_duration_seconds", "source", string(index.SourceIngest))
	if count != 1 || sum <= 0 {
		t.Fatalf("ingest build duration histogram = count %d sum %v, want one positive observation", count, sum)
	}
}
