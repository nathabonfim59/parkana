package ingest

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// --- helpers ---------------------------------------------------------------

var errInjected = errors.New("errInjected")

// testStore opens a LocalStore on a fresh dir, letting the test adjust ops.
func testStore(t *testing.T, edit func(*localFileOps)) (*LocalStore, string) {
	t.Helper()
	ops := defaultLocalOps()
	if edit != nil {
		edit(&ops)
	}
	dir := t.TempDir()
	s, err := openLocal(t.Context(), dir, ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func testTenantStore(t *testing.T, s *LocalStore) *TenantStore {
	t.Helper()
	ts, err := s.Tenant(t.Context(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// rowSpec is one row to build into a test record.
type rowSpec struct {
	us       int64
	line     string
	labels   logs.Labels
	meta     logs.Labels
	promoted map[string]string
}

// buildRecord assembles a schema.ForStream record sorted by _ts.
func buildRecord(t *testing.T, promoted []string, rows []rowSpec) arrow.RecordBatch {
	t.Helper()
	sc, err := schema.ForStream(promoted)
	if err != nil {
		t.Fatal(err)
	}
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), sc)
	defer rb.Release()
	fields := rb.Fields()
	tsb := fields[0].(*array.TimestampBuilder)
	lb := fields[1].(*array.StringBuilder)
	for _, r := range rows {
		tsb.Append(arrow.Timestamp(r.us))
		lb.Append(r.line)
		appendMap(fields[2].(*array.MapBuilder), r.labels)
		appendMap(fields[3].(*array.MapBuilder), r.meta)
		for i, name := range promoted {
			pb := fields[4+i].(*array.StringBuilder)
			if v, ok := r.promoted[name]; ok {
				pb.Append(v)
			} else {
				pb.AppendNull()
			}
		}
	}
	return rb.NewRecordBatch()
}

func writeTestFile(t *testing.T, ts *TenantStore, stream string, hour time.Time, promoted []string, rows []rowSpec) LocalFile {
	t.Helper()
	rec := buildRecord(t, promoted, rows)
	defer rec.Release()
	f, err := ts.writeRecord(t.Context(), stream, hour, rec, defaultWriterFactory, promoted)
	if err != nil {
		t.Fatal(err)
	}
	if f.ID == uuid.Nil() {
		t.Fatal("no file written")
	}
	return f
}

// readRows counts rows in a committed local file via the real Parquet reader.
func readRows(t *testing.T, ts *TenantStore, f LocalFile) (int64, []int64) {
	t.Helper()
	osf, err := ts.OpenFile(t.Context(), f.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer osf.Close()
	st, err := osf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != f.Bytes {
		t.Fatalf("file %s bytes: stat %d, manifest %d", f.RelPath(), st.Size(), f.Bytes)
	}
	rdr, err := storage.NewReader(osf, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	var rows int64
	var tsVals []int64
	for rec, err := range rdr.Records(t.Context()) {
		if err != nil {
			t.Fatal(err)
		}
		ts := rec.Column(0).(*array.Timestamp)
		for i := range rec.NumRows() {
			tsVals = append(tsVals, int64(ts.Value(int(i))))
		}
		rows += rec.NumRows()
	}
	return rows, tsVals
}

func us(hour time.Time, d time.Duration) int64 { return hour.UnixMicro() + d.Microseconds() }

// --- open/publish basics ---------------------------------------------------

func TestLocalEmptyOpen(t *testing.T) {
	s, _ := testStore(t, nil)
	ids, err := s.Tenants(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("tenants = %v", ids)
	}
	fr, err := s.Frontier(t.Context(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

func TestLocalPublishRoundTrip(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, []string{"level"}, []rowSpec{
		{us: us(testHour, 0), line: "a", labels: logs.New(logs.Label{Name: "job", Value: "x"}), promoted: map[string]string{"level": "info"}},
		{us: us(testHour, time.Second), line: "b", promoted: map[string]string{"level": "warn"}},
	})
	if f.Tenant != testTenant || f.Stream != "checkout" || f.Rows != 2 || f.Bytes <= 0 {
		t.Fatalf("file = %+v", f)
	}
	if f.LabelValues["level"][0] != "info" || f.LabelValues["level"][1] != "warn" {
		t.Fatalf("label values = %v", f.LabelValues)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 5}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{First: 1, Last: 5}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].PublishedAt.IsZero() {
		t.Fatalf("files = %+v", files)
	}
	rows, tsVals := readRows(t, ts, files[0])
	if rows != 2 || !slices.Equal(tsVals, []int64{us(testHour, 0), us(testHour, time.Second)}) {
		t.Fatalf("rows=%d ts=%v", rows, tsVals)
	}
}

func TestLocalPublishFrontierRules(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 5}, nil); err != nil {
		t.Fatal(err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 5}, nil); err != nil {
		t.Fatalf("covered republish = %v", err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 3, Last: 9}, nil); !errors.Is(err, ErrGenerationOverlap) {
		t.Fatalf("overlap = %v", err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 8, Last: 9}, nil); !errors.Is(err, ErrGenerationGap) {
		t.Fatalf("gap = %v", err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 6, Last: 9}, nil); err != nil {
		t.Fatalf("next generation = %v", err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 0, Last: 0}, nil); err == nil {
		t.Fatal("zero frontier accepted")
	}
}

func TestLocalPendingSyncRetry(t *testing.T) {
	armed := false
	s, _ := testStore(t, func(ops *localFileOps) {
		def := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if armed && name == "." {
				armed = false
				return errInjected
			}
			return def(root, name)
		}
	})
	ts := testTenantStore(t, s)
	armed = true // the tenant-dir fsync after the manifest rename fails once
	err := ts.Publish(t.Context(), Frontier{First: 1, Last: 5}, nil)
	if !errors.Is(err, errInjected) {
		t.Fatalf("publish = %v", err)
	}
	// The candidate state is preserved: a covered-generation retry must first
	// complete the pending tenant-directory sync, then report coverage.
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 5}, nil); err != nil {
		t.Fatalf("retry = %v", err)
	}
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{First: 1, Last: 5}) {
		t.Fatalf("frontier = %+v", fr)
	}
	// The manifest committed exactly once: seq 1, not 2.
	ts.mu.Lock()
	seq := ts.state.Seq
	ts.mu.Unlock()
	if seq != 1 {
		t.Fatalf("manifest seq = %d, want 1", seq)
	}
}

func TestLocalFileSyncFailure(t *testing.T) {
	s, _ := testStore(t, func(ops *localFileOps) {
		def := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			f, err := def(root, name, flag, perm)
			if err != nil {
				return nil, err
			}
			return &failSyncFile{syncFile: f}, nil
		}
	})
	ts := testTenantStore(t, s)
	rec := buildRecord(t, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	defer rec.Release()
	_, err := ts.writeRecord(t.Context(), "checkout", testHour, rec, defaultWriterFactory, nil)
	if !errors.Is(err, errInjected) {
		t.Fatalf("writeRecord = %v", err)
	}
	// The partial temp is removed and nothing is committed.
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
	entries, err := fs.ReadDir(ts.root.FS(), "checkout/2026/11/03/21")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Fatalf("leftover after failed sync: %s", e.Name())
	}
}

type failSyncFile struct{ syncFile }

func (f *failSyncFile) Sync() error { return errInjected }

func TestLocalManifestRenameFailure(t *testing.T) {
	s, dir := testStore(t, func(ops *localFileOps) {
		ops.rename = func(root *os.Root, old, new string) error {
			if new == manifestName {
				return errInjected
			}
			return root.Rename(old, new)
		}
	})
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); !errors.Is(err, errInjected) {
		t.Fatalf("publish = %v", err)
	}
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{}) {
		t.Fatalf("frontier advanced on failed commit: %+v", fr)
	}
	s.Close()
	// Reopen without the fault: the uncommitted manifest temp and the
	// renamed-but-unreferenced Parquet file are quarantined, frontier zero.
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	ts2 := testTenantStore(t, s2)
	fr, err = ts2.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{}) {
		t.Fatalf("recovered frontier = %+v", fr)
	}
	files, err := ts2.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("uncommitted file listed: %+v", files)
	}
	q, err := fs.ReadDir(ts2.root.FS(), quarantineDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 {
		t.Fatalf("quarantine holds %d entries, want manifest tmp + orphan parquet", len(q))
	}
}

// --- recovery --------------------------------------------------------------

func TestLocalQuarantineOnRecovery(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Plant garbage: an orphan committed-looking file, a temp, a stray root
	// file, an orphan upload temp and a non-sanitized stream dir.
	root, err := os.OpenRoot(filepath.Join(dir, "parquet", string(testTenant)))
	if err != nil {
		t.Fatal(err)
	}
	strays := []string{
		"checkout/2026/11/03/21/" + uuid.NewV7().String() + ".parquet",
		"checkout/2026/11/03/21/" + uuid.NewV7().String() + ".parquet.tmp",
		manifestTempName,
		uploadTempName(uuid.NewV7()),
		"Bad.Dir/junk",
	}
	for _, rel := range strays {
		if err := root.MkdirAll(filepath.Dir(rel), dirPerm); err != nil {
			t.Fatal(err)
		}
		if err := root.WriteFile(rel, []byte("x"), filePerm); err != nil {
			t.Fatal(err)
		}
	}
	root.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	ts2 := testTenantStore(t, s2)
	files, err := ts2.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ID != f.ID {
		t.Fatalf("files = %+v", files)
	}
	q, err := fs.ReadDir(ts2.root.FS(), quarantineDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != len(strays) {
		names := make([]string, len(q))
		for i, e := range q {
			names[i] = e.Name()
		}
		t.Fatalf("quarantined %v, want %d entries", names, len(strays))
	}
}

func TestLocalMissingLiveFile(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	root, err := os.OpenRoot(filepath.Join(dir, "parquet", string(testTenant)))
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Remove(f.RelPath()); err != nil {
		t.Fatal(err)
	}
	root.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err == nil {
		s2.Close()
		t.Fatal("missing live file accepted")
	}
	if !errors.Is(err, ErrFileMissing) {
		t.Fatalf("open = %v", err)
	}
}

// --- uploads ---------------------------------------------------------------

// driveToPublished takes a file through the whole upload lifecycle, leaving
// the receipt published and the source file retired on disk.
func driveToPublished(t *testing.T, ts *TenantStore, f LocalFile) Upload {
	t.Helper()
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if u.Key != objstore.DataKey(testTenant, f.Stream, f.Hour, u.ID) {
		t.Fatalf("key = %q", u.Key)
	}
	if u.CreatedAt.IsZero() {
		t.Fatal("no CreatedAt on reservation")
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("merged parquet bytes")
	if _, err := tmp.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	stats := storage.FileStats{Rows: f.Rows, MinTS: f.MinTS, MaxTS: f.MaxTS, LabelValues: f.LabelValues}
	if err := ts.RecordMerge(t.Context(), u.ID, stats, tmp.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := ts.MarkStored(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	if err := ts.MarkPublished(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	return u
}

func TestLocalUploadLifecycle(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u := driveToPublished(t, ts, f)
	if err := ts.CompleteUpload(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("files = %+v", files)
	}
	uploads, err := ts.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 0 {
		t.Fatalf("uploads = %+v", uploads)
	}
	for _, p := range []string{f.RelPath(), uploadTempName(u.ID)} {
		if _, err := ts.root.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s still exists", p)
		}
	}
	// Idempotent completion.
	if err := ts.CompleteUpload(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
}

func TestLocalRetiredMissingSurvivesRecovery(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u := driveToPublished(t, ts, f)
	// Crash mid-cleanup: the source file is deleted but the receipt stays.
	root, err := os.OpenRoot(filepath.Join(dir, "parquet", string(testTenant)))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{f.RelPath(), uploadTempName(u.ID)} {
		if err := root.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	root.Close()
	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	ts2 := testTenantStore(t, s2)
	uploads, err := ts2.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 1 || uploads[0].ID != u.ID || uploads[0].Stage != UploadPublished {
		t.Fatalf("uploads = %+v", uploads)
	}
	files, err := ts2.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The retired entry survives so the receipt's source identity is kept
	// until CompleteUpload drops both atomically.
	if len(files) != 1 || !files[0].Retired {
		t.Fatalf("files = %+v", files)
	}
	if err := ts2.CompleteUpload(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	files, _ = ts2.Files(t.Context())
	uploads, _ = ts2.Uploads(t.Context())
	if len(files) != 0 || len(uploads) != 0 {
		t.Fatalf("cleanup left %+v / %+v", files, uploads)
	}
}

func TestLocalReserveValidation(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f1 := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	f2 := writeTestFile(t, ts, "checkout", testHour.Add(time.Hour), nil, []rowSpec{{us: us(testHour.Add(time.Hour), 0), line: "b"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 2}, []LocalFile{f1, f2}); err != nil {
		t.Fatal(err)
	}
	uid := uuid.NewV7()
	base := Upload{Tenant: testTenant, ID: uid, Stream: "checkout", Hour: testHour, Sources: []uuid.UUID{f1.ID}}
	if _, err := ts.ReserveUpload(t.Context(), base); err != nil {
		t.Fatal(err)
	}
	// Same id, same spec → returns the stored receipt.
	again, err := ts.ReserveUpload(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	if again.Key != objstore.DataKey(testTenant, "checkout", testHour, uid) || again.CreatedAt.IsZero() {
		t.Fatalf("idempotent reserve = %+v", again)
	}
	// Same id, different spec → conflict.
	conflict := base
	conflict.Sources = []uuid.UUID{f1.ID, testID3}
	if _, err := ts.ReserveUpload(t.Context(), conflict); !errors.Is(err, ErrUploadConflict) {
		t.Fatalf("conflicting same-id reserve = %v", err)
	}
	// Cross-tenant value rejected.
	bad := base
	bad.ID = uuid.NewV7()
	bad.Tenant = tenant.ID("other")
	if _, err := ts.ReserveUpload(t.Context(), bad); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant reserve = %v", err)
	}
	// Unknown, cross-hour and already-reserved sources rejected.
	for name, mutate := range map[string]func(*Upload){
		"unknown source":  func(u *Upload) { u.Sources = []uuid.UUID{testID3} },
		"cross hour":      func(u *Upload) { u.Sources = []uuid.UUID{f2.ID} },
		"reserved source": func(u *Upload) { u.ID = uuid.NewV7() },
		"no sources":      func(u *Upload) { u.Sources = nil },
		"bad key":         func(u *Upload) { u.Key = "default/logs/checkout/x.parquet" },
		"bad stream":      func(u *Upload) { u.Stream = "Bad" },
		"bad hour":        func(u *Upload) { u.Hour = testHour.Add(time.Minute) },
	} {
		u := base
		u.ID = uuid.NewV7()
		mutate(&u)
		if name == "reserved source" {
			u.Sources = []uuid.UUID{f1.ID}
		}
		if _, err := ts.ReserveUpload(t.Context(), u); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	// Sources stay live and unretired until publication.
	files, _ := ts.Files(t.Context())
	for _, f := range files {
		if f.Retired {
			t.Fatalf("file %s retired before publish", f.ID)
		}
	}
}

func TestLocalReserveUploadCreatedAtMicrosecondAligned(t *testing.T) {
	// SQLite keeps CreatedAt as integer microseconds, so a reservation stamped
	// at nanosecond precision made uploadMatchesCatalog reject its own durable
	// row after a crash. The reservation must be truncated once, before it
	// becomes durable identity, and the manifest round trip must preserve it.
	clock := time.Date(2026, 11, 3, 23, 0, 0, 123456789, time.UTC)
	s, dir := testStore(t, func(ops *localFileOps) {
		ops.now = func() time.Time { return clock }
	})
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	want := clock.UTC().Truncate(time.Microsecond)
	if !u.CreatedAt.Equal(want) || !microAligned(u.CreatedAt) {
		t.Fatalf("CreatedAt = %s (%d ns), want microsecond-aligned %s", u.CreatedAt, u.CreatedAt.Nanosecond(), want)
	}
	if u.CreatedAt.Location() != time.UTC {
		t.Fatalf("CreatedAt location = %s, want UTC", u.CreatedAt.Location())
	}

	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	ups, err := testTenantStore(t, s2).Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(ups) != 1 || ups[0].ID != u.ID || !ups[0].CreatedAt.Equal(want) {
		t.Fatalf("reopened uploads = %+v, want id %s created %s", ups, u.ID, want)
	}
}

func TestLocalUploadStageEnforcement(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.MarkStored(t.Context(), u.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("stored before merged = %v", err)
	}
	if err := ts.MarkPublished(t.Context(), u.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("published before stored = %v", err)
	}
	if err := ts.CompleteUpload(t.Context(), u.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("complete before published = %v", err)
	}
	if err := ts.RecordMerge(t.Context(), u.ID, storage.FileStats{Rows: 1, MinTS: f.MinTS, MaxTS: f.MaxTS}, 10); err == nil {
		t.Fatal("record merge without finalized temp accepted")
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts.CreateUploadTemp(t.Context(), u.ID); !errors.Is(err, ErrUploadConflict) {
		t.Fatalf("double create = %v", err)
	}
	if _, err := tmp.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatalf("refinalize = %v", err)
	}
	stats := storage.FileStats{Rows: f.Rows, MinTS: f.MinTS, MaxTS: f.MaxTS}
	if err := ts.RecordMerge(t.Context(), u.ID, stats, tmp.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := ts.RecordMerge(t.Context(), u.ID, stats, tmp.Bytes()+1); err == nil {
		t.Fatal("byte mismatch accepted")
	}
	if err := ts.RecordMerge(t.Context(), testID3, stats, 1); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("unknown upload merge = %v", err)
	}
}

func TestLocalUploadTempAbort(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := ts.AbortUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write([]byte("x")); err == nil {
		t.Fatal("write on aborted temp accepted")
	}
	if _, err := ts.root.Stat(uploadTempTmpName(u.ID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("partial temp not quarantined")
	}
	q, err := fs.ReadDir(ts.root.FS(), quarantineDir)
	if err != nil || len(q) != 1 {
		t.Fatalf("quarantine = %v, %v", q, err)
	}
	// A new temp can be created after abort.
	tmp2, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.AbortUploadTemp(t.Context(), tmp2); err != nil {
		t.Fatal(err)
	}
}

func TestLocalUploadTempFinalizeDirSyncRetry(t *testing.T) {
	armed := false
	s, _ := testStore(t, func(ops *localFileOps) {
		def := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if armed && name == uploadsDir {
				armed = false
				return errInjected
			}
			return def(root, name)
		}
	})
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	tmp.Write([]byte("data"))
	armed = true
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); !errors.Is(err, errInjected) {
		t.Fatalf("finalize = %v", err)
	}
	// Rename already happened; retry only needs the directory sync.
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatalf("retry = %v", err)
	}
	of, err := ts.OpenUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	of.Close()
}

// --- isolation, cancellation, early hours ----------------------------------

func TestLocalTenantIsolation(t *testing.T) {
	s, _ := testStore(t, nil)
	other := tenant.ID("tenant_two")
	a := testTenantStore(t, s)
	fa := writeTestFile(t, a, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := a.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{fa}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Tenant(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	fr, err := b.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{}) {
		t.Fatalf("tenant %s sees frontier %+v", other, fr)
	}
	files, err := b.Files(t.Context())
	if err != nil || len(files) != 0 {
		t.Fatalf("tenant %s sees files %+v", other, files)
	}
	// A file owned by another tenant cannot be published under b.
	if err := b.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{fa}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant publish = %v", err)
	}
	ids, err := s.Tenants(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []tenant.ID{testTenant, other}) && !slices.Equal(ids, []tenant.ID{other, testTenant}) {
		t.Fatalf("tenants = %v", ids)
	}
}

func TestLocalEarlyHourRoundTrip(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	epoch := time.Time{} // 0001-01-01T00:00:00Z, a legal protobuf timestamp
	f := writeTestFile(t, ts, "s", epoch, nil, []rowSpec{{us: epoch.UnixMicro(), line: "first log ever"}})
	if f.RelPath() != "s/0001/01/01/00/"+f.ID.String()+".parquet" {
		t.Fatalf("path = %q", f.RelPath())
	}
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	rows, _ := readRows(t, ts, f)
	if rows != 1 {
		t.Fatalf("rows = %d", rows)
	}
}

func TestLocalContextCanceled(t *testing.T) {
	s, _ := testStore(t, nil)
	ctx := &cancelAfter{Context: t.Context(), at: 0}
	if _, err := s.Tenants(ctx); err == nil {
		t.Fatal("canceled Tenants accepted")
	}
	if _, err := s.Tenant(ctx, testTenant); err == nil {
		t.Fatal("canceled Tenant accepted")
	}
}

// --- fault-order durability, writer/close ownership, input cloning ---------

// TestLocalOpenSyncsParent proves the parquet dir entry and each new tenant
// dir entry are fsynced before use, including retries after a failed sync.
func TestLocalOpenSyncsParent(t *testing.T) {
	armed := true
	ops := defaultLocalOps()
	def := ops.syncDir
	var calls int
	ops.syncDir = func(root *os.Root, name string) error {
		calls++
		if armed {
			armed = false
			return errInjected
		}
		return def(root, name)
	}
	dir := t.TempDir()
	if _, err := openLocal(t.Context(), dir, ops); !errors.Is(err, errInjected) {
		t.Fatalf("open with failed parent sync = %v", err)
	}
	// Retry: the parquet dir exists now; the sync must still run.
	s, err := openLocal(t.Context(), dir, ops)
	if err != nil {
		t.Fatalf("reopen = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	// Tenant creation retry: first attempt fails the parent sync and is not
	// cached; the second must redo the sync even though the dir exists.
	armed = true
	if _, err := s.Tenant(t.Context(), testTenant); !errors.Is(err, errInjected) {
		t.Fatalf("tenant with failed parent sync = %v", err)
	}
	if _, err := s.Tenant(t.Context(), testTenant); err != nil {
		t.Fatalf("tenant retry = %v", err)
	}
	if calls < 4 {
		t.Fatalf("syncDir calls = %d, want parent syncs on every retry", calls)
	}
}

// TestLocalReopenSyncsParquetRoot injects a failed tenant-parent sync, then
// proves the parquet-root fsync runs on reopen before tenant discovery
// acknowledges the entry — and that its failure propagates.
func TestLocalReopenSyncsParquetRoot(t *testing.T) {
	var syncs, failOn int // failOn is the 1-based "." sync to fail; 0 disables
	ops := defaultLocalOps()
	def := ops.syncDir
	ops.syncDir = func(root *os.Root, name string) error {
		if name == "." {
			syncs++
			if syncs == failOn {
				return errInjected
			}
		}
		return def(root, name)
	}
	dir := t.TempDir()
	s, err := openLocal(t.Context(), dir, ops) // syncs 1 (data dir) + 2 (parquet root)
	if err != nil {
		t.Fatal(err)
	}
	// The tenant mkdir succeeds but its parent sync fails: the directory is
	// left durable-unsafe on disk and the handle is not cached.
	failOn = 3
	if _, err := s.Tenant(t.Context(), testTenant); !errors.Is(err, errInjected) {
		t.Fatalf("tenant = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen with the parquet-root sync failing (call 4 = data dir, 5 = root):
	// recovery must not acknowledge the unsynced tenant entry.
	failOn = 5
	syncs = 3
	if _, err := openLocal(t.Context(), dir, ops); !errors.Is(err, errInjected) {
		t.Fatalf("reopen = %v", err)
	}
	failOn = 0
	s2, err := openLocal(t.Context(), dir, ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	ids, err := s2.Tenants(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []tenant.ID{testTenant}) {
		t.Fatalf("tenants = %v", ids)
	}
	if syncs < 5 {
		t.Fatalf("parquet-root syncs during reopen = %d, want data-dir + root", syncs-3)
	}
}

// trackedFile counts Close calls on one wrapped file and can inject a
// distinct close error, for ownership assertions on a specific handle.
type trackedFile struct {
	syncFile
	closes *int
	err    *error
}

func (f *trackedFile) Close() error {
	(*f.closes)++
	return errors.Join(f.syncFile.Close(), *f.err)
}

// trackTemps wraps ops.create so every .uploads temp file's Close is counted
// and can return closeErr. Other files (data, manifest) are untouched.
func trackTemps(ops *localFileOps, closes *int, closeErr *error) {
	def := ops.create
	ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
		f, err := def(root, name, flag, perm)
		if err != nil || !strings.HasPrefix(name, uploadsDir+"/") || !strings.HasSuffix(name, ".tmp") {
			return f, err
		}
		return &trackedFile{syncFile: f, closes: closes, err: closeErr}, nil
	}
}

var errClose = errors.New("close failed")

// fakeWriter is a ParquetWriter test double counting Close calls.
type fakeWriter struct {
	closes   int
	writeErr error
	closeErr error
	stats    storage.FileStats
}

func (w *fakeWriter) Write(arrow.RecordBatch) error { return w.writeErr }
func (w *fakeWriter) Close() (storage.FileStats, error) {
	w.closes++
	return w.stats, w.closeErr
}

var errWrite = errors.New("write failed")

// TestWriteRecordClosesWriter proves the ParquetWriter is closed exactly once
// even when Write fails, and both failure causes are reported.
func TestWriteRecordClosesWriter(t *testing.T) {
	cases := []struct {
		name         string
		w            *fakeWriter
		wantWriteErr bool
		wantCloseErr bool
	}{
		{"write fails", &fakeWriter{writeErr: errWrite}, true, false},
		{"write and close fail", &fakeWriter{writeErr: errWrite, closeErr: errClose}, true, true},
		{"close fails", &fakeWriter{closeErr: errClose}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t, nil)
			ts := testTenantStore(t, s)
			rec := buildRecord(t, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
			_, err := ts.writeRecord(t.Context(), "checkout", testHour, rec,
				func(io.Writer, *arrow.Schema, []string) (ParquetWriter, error) { return tc.w, nil }, nil)
			rec.Release()
			if errors.Is(err, errWrite) != tc.wantWriteErr {
				t.Fatalf("writeRecord err = %v, wantWriteErr=%v", err, tc.wantWriteErr)
			}
			if errors.Is(err, errClose) != tc.wantCloseErr {
				t.Fatalf("writeRecord err = %v, wantCloseErr=%v", err, tc.wantCloseErr)
			}
			if tc.w.closes != 1 {
				t.Fatalf("writer Close calls = %d, want exactly 1", tc.w.closes)
			}
			s.Close()
		})
	}
}

// TestPublishInputIndependence proves Publish stamps clones, not caller data.
func TestPublishInputIndependence(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, []string{"level"}, []rowSpec{
		{us: us(testHour, 0), line: "a", promoted: map[string]string{"level": "info"}},
	})
	f.PublishedAt = time.Time{} // caller garbage; Publish must stamp its own
	input := []LocalFile{f}
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, input); err != nil {
		t.Fatal(err)
	}
	// Mutate caller state after Publish.
	input[0].LabelValues["level"][0] = "corrupted"
	input[0].Rows = -1
	input[0].Retired = true
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := files[0]
	if got.PublishedAt.IsZero() || got.Rows != f.Rows || got.Retired || got.LabelValues["level"][0] != "info" {
		t.Fatalf("manifest retains caller data: %+v", got)
	}
	if !input[0].PublishedAt.IsZero() {
		t.Fatal("caller slice was stamped")
	}
}

// TestAbortAlwaysCloses proves Abort releases the handle exactly once even
// when the ctx is canceled or a manifest dir sync is still pending and
// failing, and that close errors join into the result.
func TestAbortAlwaysCloses(t *testing.T) {
	t.Run("canceled ctx", func(t *testing.T) {
		var closes int
		closeErr := error(nil)
		s, _ := testStore(t, func(ops *localFileOps) {
			trackTemps(ops, &closes, &closeErr)
		})
		ts := testTenantStore(t, s)
		f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
		if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
			t.Fatal(err)
		}
		u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
		if err != nil {
			t.Fatal(err)
		}
		tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
		if err != nil {
			t.Fatal(err)
		}
		closeErr = errClose
		err = ts.AbortUploadTemp(&cancelAfter{Context: t.Context(), at: 0}, tmp)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abort missing ctx cause: %v", err)
		}
		if !errors.Is(err, errClose) {
			t.Fatalf("abort missing close cause: %v", err)
		}
		if closes != 1 {
			t.Fatalf("temp closes = %d, want exactly 1", closes)
		}
		if tmp.state != tempAborted {
			t.Fatalf("temp state = %v", tmp.state)
		}
	})

	t.Run("pending sync failure", func(t *testing.T) {
		var closes int
		closeErr := error(nil)
		armed := 0
		s, _ := testStore(t, func(ops *localFileOps) {
			trackTemps(ops, &closes, &closeErr)
			defs := ops.syncDir
			ops.syncDir = func(root *os.Root, name string) error {
				if armed > 0 && name == "." {
					armed--
					return errInjected
				}
				return defs(root, name)
			}
		})
		ts := testTenantStore(t, s)
		f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
		if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
			t.Fatal(err)
		}
		u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
		if err != nil {
			t.Fatal(err)
		}
		tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
		if err != nil {
			t.Fatal(err)
		}
		// Leave a manifest dir sync pending and failing, then abort.
		armed = 1
		if err := ts.Publish(t.Context(), Frontier{First: 2, Last: 2}, nil); !errors.Is(err, errInjected) {
			t.Fatalf("publish = %v", err)
		}
		armed = 1
		closeErr = errClose
		err = ts.AbortUploadTemp(t.Context(), tmp)
		if !errors.Is(err, errInjected) {
			t.Fatalf("abort missing sync cause: %v", err)
		}
		if !errors.Is(err, errClose) {
			t.Fatalf("abort missing close cause: %v", err)
		}
		if closes != 1 {
			t.Fatalf("temp closes = %d, want exactly 1", closes)
		}
		if tmp.state != tempAborted {
			t.Fatalf("temp state = %v", tmp.state)
		}
	})
}

// TestCloseReleasesActiveTemps proves Store.Close closes each open temp
// handle exactly once, joining its close error, and post-close operations
// report ErrLocalClosed.
func TestCloseReleasesActiveTemps(t *testing.T) {
	var closes int
	closeErr := error(nil)
	s, _ := testStore(t, func(ops *localFileOps) {
		trackTemps(ops, &closes, &closeErr)
	})
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	closeErr = errClose
	if err := s.Close(); !errors.Is(err, errClose) {
		t.Fatalf("close missing temp close cause: %v", err)
	}
	if closes != 1 {
		t.Fatalf("temp closes = %d, want exactly 1", closes)
	}
	if tmp.state != tempAborted {
		t.Fatalf("temp state = %v", tmp.state)
	}
	if _, err := s.Tenants(t.Context()); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close tenants = %v", err)
	}
	if _, err := s.Tenant(t.Context(), testTenant); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close tenant = %v", err)
	}
	if _, err := s.Frontier(t.Context(), testTenant); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close store frontier = %v", err)
	}
	if _, err := ts.Frontier(t.Context()); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close frontier = %v", err)
	}
	if _, err := ts.CreateUploadTemp(t.Context(), u.ID); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close create temp = %v", err)
	}
	if err := ts.Publish(t.Context(), Frontier{First: 2, Last: 2}, nil); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("post-close publish = %v", err)
	}
}

// TestLocalRecordPutAndRefresh covers the orphan-GC contract: RecordPut
// durably stamps stored+completion (again on re-Put) and RefreshUploadKey
// atomically rekeys an unpublished receipt while retaining its identity,
// durable temp, sources and stats.
func TestLocalRecordPutAndRefresh(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	// RecordPut requires a merge first and a nonzero completion.
	if err := ts.RecordPut(t.Context(), u.ID, time.Now()); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("put before merge = %v", err)
	}
	if err := ts.RecordPut(t.Context(), u.ID, time.Time{}); err == nil {
		t.Fatal("zero completion accepted")
	}
	if err := ts.RecordPut(t.Context(), testID3, time.Now()); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("put on unknown upload = %v", err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	tmp.Write([]byte("merged"))
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	stats := storage.FileStats{Rows: f.Rows, MinTS: f.MinTS, MaxTS: f.MaxTS}
	if err := ts.RecordMerge(t.Context(), u.ID, stats, tmp.Bytes()); err != nil {
		t.Fatal(err)
	}
	// First Put: stored + completion time.
	t1 := testHour.Add(2 * time.Hour)
	if err := ts.RecordPut(t.Context(), u.ID, t1); err != nil {
		t.Fatal(err)
	}
	got, err := ts.RefreshUploadKey(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Re-Put under the NEW key refreshes completion on the stored receipt.
	if err := ts.RecordPut(t.Context(), got.ID, t1.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	uploads, err := ts.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cur := uploads[0]
	if cur.Stage != UploadStored || !cur.PutCompletedAt.Equal(t1.Add(time.Minute)) {
		t.Fatalf("re-put state = %+v", cur)
	}
	// Refresh: merged again, fresh canonical key with a DIFFERENT uuidv7,
	// completion cleared, identity/temp/sources/stats/CreatedAt retained.
	ref, err := ts.RefreshUploadKey(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ref.ID != u.ID || ref.Stage != UploadMerged || !ref.PutCompletedAt.IsZero() {
		t.Fatalf("refresh = %+v", ref)
	}
	if ref.Key == cur.Key || ref.Key != objstore.LogPrefix(testTenant, u.Stream)+u.Hour.Format("2006/01/02/15/")+path.Base(ref.Key) {
		t.Fatalf("refresh key = %q, old %q", ref.Key, cur.Key)
	}
	if path.Dir(ref.Key) != path.Dir(u.Key) {
		t.Fatalf("refreshed key left the hour partition: %q", ref.Key)
	}
	keyID, err := uuid.Parse(strings.TrimSuffix(path.Base(ref.Key), objstore.ExtData))
	if err != nil || keyID[6]>>4 != 7 || keyID == u.ID {
		t.Fatalf("refreshed key uuid = %v, %v", keyID, err)
	}
	if ref.Temp != cur.Temp || ref.Rows != cur.Rows || !reflect.DeepEqual(ref.Sources, cur.Sources) || !ref.CreatedAt.Equal(cur.CreatedAt) {
		t.Fatalf("refresh lost receipt identity: %+v", ref)
	}
	// The refreshed key persists across restart even though its uuid is not
	// the receipt ID (validation is canonical, not id-derived).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	uploads, err = testTenantStore(t, s2).Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 1 || uploads[0].Key != ref.Key || uploads[0].Stage != UploadMerged {
		t.Fatalf("reopened uploads = %+v", uploads)
	}
	// Stage restrictions.
	ts2 := testTenantStore(t, s2)
	if _, err := ts2.RefreshUploadKey(t.Context(), u.ID); err != nil {
		t.Fatal("refresh on merged rejected")
	}
	if err := ts2.MarkPublished(t.Context(), u.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("published before stored = %v", err)
	}
	// Reserved and published receipts cannot refresh.
	f2 := writeTestFile(t, ts2, "checkout", testHour, nil, []rowSpec{{us: us(testHour, time.Second), line: "b"}})
	if err := ts2.Publish(t.Context(), Frontier{First: 2, Last: 2}, []LocalFile{f2}); err != nil {
		t.Fatal(err)
	}
	u2, err := ts2.ReserveUpload(t.Context(), Upload{Stream: f2.Stream, Hour: f2.Hour, Sources: []uuid.UUID{f2.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts2.RefreshUploadKey(t.Context(), u2.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("refresh reserved = %v", err)
	}
	u3 := driveToPublishedStage(t, ts2, u.ID)
	if _, err := ts2.RefreshUploadKey(t.Context(), u3.ID); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("refresh published = %v", err)
	}
	if err := ts2.RecordPut(t.Context(), u3.ID, time.Now()); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("put on published = %v", err)
	}
}

// driveToPublishedStage pushes an existing receipt to published.
func driveToPublishedStage(t *testing.T, ts *TenantStore, id uuid.UUID) Upload {
	t.Helper()
	if err := ts.RecordPut(t.Context(), id, testHour.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := ts.MarkPublished(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	uploads, err := ts.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	i := findUpload(uploads, id)
	if i < 0 {
		t.Fatalf("upload %s gone", id)
	}
	return uploads[i]
}

// TestFinalizeAfterSyncFailure proves retrying Finalize after a failed file
// sync reports a stable error instead of panicking, and Abort still works.
func TestFinalizeAfterSyncFailure(t *testing.T) {
	armed := false
	s, _ := testStore(t, func(ops *localFileOps) {
		def := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			f, err := def(root, name, flag, perm)
			if err != nil {
				return nil, err
			}
			return &gatedSyncFile{syncFile: f, armed: &armed}, nil
		}
	})
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	tmp.Write([]byte("data"))
	armed = true
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); !errors.Is(err, errInjected) {
		t.Fatalf("finalize = %v", err)
	}
	armed = false
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); !errors.Is(err, ErrUploadStage) {
		t.Fatalf("retry finalize = %v", err)
	}
	if err := ts.AbortUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
}
