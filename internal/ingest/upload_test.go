package ingest

// Tests for the uploader. They run against the real LocalStore, filesystem
// object store and a memory or SQLite catalog, using narrow delegating wrappers
// to inject Put and catalog failures. No interface is changed for tests.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadRow is one source row used to seed a local file.
type uploadRow struct {
	ts     time.Time
	line   string
	labels map[string]string
	meta   map[string]string
	level  *string
}

func uploadText(s string) *string { return &s }

// uploadReadRow is one row read back from a merged object.
type uploadReadRow struct {
	ts     time.Time
	line   string
	labels map[string]string
	meta   map[string]string
	level  *string
}

// uploadCatalogKind selects the catalog backend a fixture runs against.
type uploadCatalogKind string

const (
	uploadCatalogMemory uploadCatalogKind = "memory"
	uploadCatalogSQLite uploadCatalogKind = "sqlite"
)

// uploadCatalogKinds is the backend table for catalog-sensitive tests.
func uploadCatalogKinds() []uploadCatalogKind {
	return []uploadCatalogKind{uploadCatalogMemory, uploadCatalogSQLite}
}

// uploadFixture owns a local store, an object store and a catalog in temporary
// directories.
type uploadFixture struct {
	t       *testing.T
	dataDir string
	local   *LocalStore
	objects *objstore.FS
	cat     catalog.Catalog
	catKind uploadCatalogKind
	catPath string // SQLite database file, empty for memory
	promote func(string) []string
	seq     map[tenant.ID]uint64
}

func uploadNewFixture(t *testing.T) *uploadFixture {
	t.Helper()
	return uploadNewFixtureKind(t, uploadCatalogMemory, nil)
}

// uploadNewFixtureWithOps builds a fixture whose LocalStore filesystem seams
// were adjusted by mutate. Tests use it to inject durability failures.
func uploadNewFixtureWithOps(t *testing.T, mutate func(*localFileOps)) *uploadFixture {
	t.Helper()
	return uploadNewFixtureKind(t, uploadCatalogMemory, mutate)
}

// uploadNewFixtureKind builds a fixture backed by the given catalog backend.
// A SQLite catalog lives in its own temp file so reopening it can prove
// persistence.
func uploadNewFixtureKind(t *testing.T, kind uploadCatalogKind, mutate func(*localFileOps)) *uploadFixture {
	t.Helper()
	ctx := t.Context()
	dataDir := filepath.Join(t.TempDir(), "data")
	ops := defaultLocalOps()
	if mutate != nil {
		mutate(&ops)
	}
	local, err := openLocal(ctx, dataDir, ops)
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	objects, err := objstore.NewFS(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		local.Close()
		t.Fatalf("objstore.NewFS: %v", err)
	}
	cat, catPath := uploadNewCatalog(t, kind)
	f := &uploadFixture{
		t: t, dataDir: dataDir, local: local, objects: objects,
		cat: cat, catKind: kind, catPath: catPath,
		promote: func(string) []string { return nil },
		seq:     make(map[tenant.ID]uint64),
	}
	t.Cleanup(func() {
		if err := objects.Close(); err != nil {
			t.Errorf("objects.Close: %v", err)
		}
		if err := f.cat.Close(); err != nil {
			t.Errorf("catalog.Close: %v", err)
		}
		if f.local != nil {
			if err := f.local.Close(); err != nil {
				t.Errorf("local.Close: %v", err)
			}
		}
	})
	return f
}

// uploadNewCatalog opens one catalog backend, returning the instance and, for
// SQLite, the database path needed to reopen it.
func uploadNewCatalog(t *testing.T, kind uploadCatalogKind) (catalog.Catalog, string) {
	t.Helper()
	switch kind {
	case uploadCatalogMemory:
		return catalog.NewMemory(), ""
	case uploadCatalogSQLite:
		path := filepath.Join(t.TempDir(), "catalog.sqlite")
		cat, err := catalog.NewSQLite(path)
		if err != nil {
			t.Fatalf("catalog.NewSQLite: %v", err)
		}
		return cat, path
	default:
		t.Fatalf("unknown catalog kind %q", kind)
		return nil, ""
	}
}

// reopen closes and reopens the local store on the same directory, exercising
// crash recovery, and reopens the catalog the same way. The memory catalog
// stays live; a SQLite catalog is reopened on the same database file, so the
// reader must recover from real persisted rows.
func (f *uploadFixture) reopen() {
	f.t.Helper()
	if err := f.local.Close(); err != nil {
		f.t.Fatalf("close local: %v", err)
	}
	local, err := OpenLocal(f.t.Context(), f.dataDir)
	if err != nil {
		f.t.Fatalf("reopen local: %v", err)
	}
	f.local = local
	if f.catKind != uploadCatalogSQLite {
		return
	}
	if err := f.cat.Close(); err != nil {
		f.t.Fatalf("close catalog: %v", err)
	}
	cat, err := catalog.NewSQLite(f.catPath)
	if err != nil {
		f.t.Fatalf("reopen catalog: %v", err)
	}
	f.cat = cat
}

// tenantStore returns (creating on first use) one tenant's local state.
func (f *uploadFixture) tenantStore(id tenant.ID) *TenantStore {
	f.t.Helper()
	ts, err := f.local.Tenant(f.t.Context(), id)
	if err != nil {
		f.t.Fatalf("local.Tenant(%q): %v", id, err)
	}
	return ts
}

// seed writes and publishes one local Parquet file with rows for stream and
// hour. Rows must already be sorted by timestamp. It uses only storage.Writer
// and the approved LocalFile path, then the store's commit entry.
func (f *uploadFixture) seed(id tenant.ID, stream string, hour time.Time, rows []uploadRow) LocalFile {
	f.t.Helper()
	ctx := f.t.Context()
	ts := f.tenantStore(id)
	promoted := f.promote(stream)
	rec := uploadBuildRecord(f.t, promoted, rows)
	defer rec.Release()

	file := LocalFile{Tenant: id, ID: uuid.NewV7(), Stream: stream, Hour: hour.UTC()}
	abs := filepath.Join(f.dataDir, "parquet", string(id), filepath.FromSlash(file.RelPath()))
	if err := os.MkdirAll(filepath.Dir(abs), 0o750); err != nil {
		f.t.Fatalf("mkdir %s: %v", filepath.Dir(abs), err)
	}
	tmp := abs + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		f.t.Fatalf("create %s: %v", tmp, err)
	}
	keep := false
	defer func() {
		if !keep {
			out.Close()
			os.Remove(tmp)
		}
	}()
	w, err := storage.NewWriter(out, rec.Schema(), promoted)
	if err != nil {
		f.t.Fatalf("storage.NewWriter: %v", err)
	}
	if err := w.Write(rec); err != nil {
		f.t.Fatalf("writer.Write: %v", err)
	}
	stats, err := w.Close()
	if err != nil {
		f.t.Fatalf("writer.Close: %v", err)
	}
	if err := out.Sync(); err != nil {
		f.t.Fatalf("sync source: %v", err)
	}
	if err := out.Close(); err != nil {
		f.t.Fatalf("close source: %v", err)
	}
	if err := os.Rename(tmp, abs); err != nil {
		f.t.Fatalf("rename source: %v", err)
	}
	keep = true
	info, err := os.Stat(abs)
	if err != nil {
		f.t.Fatalf("stat source: %v", err)
	}
	file.Bytes = info.Size()
	file.Rows = stats.Rows
	file.MinTS = stats.MinTS
	file.MaxTS = stats.MaxTS
	file.LabelValues = stats.LabelValues

	f.seq[id]++
	if err := ts.Publish(ctx, Frontier{First: f.seq[id], Last: f.seq[id]}, []LocalFile{file}); err != nil {
		f.t.Fatalf("Publish(%s): %v", stream, err)
	}
	files, err := ts.Files(ctx)
	if err != nil {
		f.t.Fatalf("Files: %v", err)
	}
	for _, got := range files {
		if got.ID == file.ID {
			return got
		}
	}
	f.t.Fatalf("published file %s not listed", file.ID)
	return LocalFile{}
}

// uploader builds an Uploader over the fixture, filling required options and
// test defaults.
func (f *uploadFixture) uploader(o UploaderOptions) *Uploader {
	f.t.Helper()
	o.Local = f.local
	if o.Objects == nil {
		o.Objects = f.objects
	}
	if o.Catalog == nil {
		o.Catalog = f.cat
	}
	if o.PromotedFor == nil {
		o.PromotedFor = f.promote
	}
	if o.MinBytes == 0 {
		o.MinBytes = 1
	}
	if o.MaxAge == 0 {
		o.MaxAge = time.Hour
	}
	if o.MergeMax == 0 {
		o.MergeMax = 8 << 20
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	// Focused uploader tests must not depend on the host's auto budget: a small
	// CI cgroup limit could serialize the concurrent-merge rendezvous tests.
	// Integration tests that exercise admission pass an explicit smaller value.
	if o.UploadMemory == 0 {
		o.UploadMemory = 8 << 30
	}
	u, err := NewUploader(o)
	if err != nil {
		f.t.Fatalf("NewUploader: %v", err)
	}
	return u
}

// localPath returns the on-disk path of a committed local file.
func (f *uploadFixture) localPath(id tenant.ID, file LocalFile) string {
	return filepath.Join(f.dataDir, "parquet", string(id), file.RelPath())
}

// tempPath returns the on-disk path of an upload's finalized merge output.
func (f *uploadFixture) tempPath(id tenant.ID, uploadID string) string {
	return filepath.Join(f.dataDir, "parquet", string(id), ".uploads", uploadID+".parquet")
}

// receipts returns every durable upload receipt of the tenant.
func (f *uploadFixture) receipts(id tenant.ID) []Upload {
	f.t.Helper()
	ups, err := f.tenantStore(id).Uploads(f.t.Context())
	if err != nil {
		f.t.Fatalf("Uploads: %v", err)
	}
	return ups
}

// onlyReceipt asserts that exactly one receipt exists and returns it.
func (f *uploadFixture) onlyReceipt(id tenant.ID) Upload {
	f.t.Helper()
	ups := f.receipts(id)
	if len(ups) != 1 {
		f.t.Fatalf("got %d receipts, want 1: %+v", len(ups), ups)
	}
	return ups[0]
}

// liveCatalogFiles returns the tenant's live catalog rows.
func (f *uploadFixture) liveCatalogFiles(id tenant.ID) []catalog.File {
	f.t.Helper()
	var out []catalog.File
	for file, err := range f.cat.ListFiles(f.t.Context(), catalog.Filter{Tenant: id}) {
		if err != nil {
			f.t.Fatalf("ListFiles: %v", err)
		}
		out = append(out, file)
	}
	return out
}

// allCatalogFiles returns the tenant's catalog rows, tombstones included.
func (f *uploadFixture) allCatalogFiles(id tenant.ID) []catalog.File {
	f.t.Helper()
	var out []catalog.File
	for file, err := range f.cat.ListFiles(f.t.Context(), catalog.Filter{Tenant: id, IncludeDeleted: true}) {
		if err != nil {
			f.t.Fatalf("ListFiles: %v", err)
		}
		out = append(out, file)
	}
	return out
}

// catalogFile converts a receipt to the catalog row the uploader publishes.
func uploadCatalogFile(up Upload) catalog.File {
	return catalog.File{
		Tenant:      up.Tenant,
		Stream:      up.Stream,
		Key:         up.Key,
		MinTS:       up.MinTS,
		MaxTS:       up.MaxTS,
		Rows:        up.Rows,
		Bytes:       up.Bytes,
		LabelValues: up.LabelValues,
		CreatedAt:   up.CreatedAt,
	}
}

// readObject reads every row of a stored object back.
func (f *uploadFixture) readObject(id tenant.ID, key string) []uploadReadRow {
	f.t.Helper()
	ctx := f.t.Context()
	or, err := storage.NewObjectReader(ctx, f.objects, id, key)
	if err != nil {
		f.t.Fatalf("NewObjectReader(%s): %v", key, err)
	}
	reader, err := storage.NewReader(or, or.Size())
	if err != nil {
		f.t.Fatalf("NewReader(%s): %v", key, err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			f.t.Errorf("reader.Close: %v", err)
		}
	}()

	var out []uploadReadRow
	for rec, err := range reader.Records(ctx) {
		if err != nil {
			f.t.Fatalf("Records: %v", err)
		}
		var tsCol *array.Timestamp
		var lineCol *array.String
		var labelsCol, metaCol *array.Map
		var levelCol *array.String
		for i, field := range rec.Schema().Fields() {
			switch field.Name {
			case schema.ColTS:
				tsCol = rec.Column(i).(*array.Timestamp)
			case schema.ColLine:
				lineCol = rec.Column(i).(*array.String)
			case schema.ColLabels:
				labelsCol = rec.Column(i).(*array.Map)
			case schema.ColMeta:
				metaCol = rec.Column(i).(*array.Map)
			case "level":
				levelCol = rec.Column(i).(*array.String)
			}
		}
		if tsCol == nil || lineCol == nil || labelsCol == nil || metaCol == nil {
			f.t.Fatalf("merged object misses base columns: %s", rec.Schema())
		}
		for i := range int(rec.NumRows()) {
			row := uploadReadRow{
				ts:     schema.Time(int64(tsCol.Value(i))),
				line:   lineCol.Value(i),
				labels: uploadReadMap(labelsCol, i),
				meta:   uploadReadMap(metaCol, i),
			}
			if levelCol != nil && levelCol.IsValid(i) {
				row.level = uploadText(levelCol.Value(i))
			}
			out = append(out, row)
		}
	}
	return out
}

func uploadBuildRecord(t *testing.T, promoted []string, rows []uploadRow) arrow.RecordBatch {
	t.Helper()
	s, err := schema.ForStream(promoted)
	if err != nil {
		t.Fatalf("schema.ForStream: %v", err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	defer b.Release()
	tsB := b.Field(0).(*array.TimestampBuilder)
	lineB := b.Field(1).(*array.StringBuilder)
	labelsB := b.Field(2).(*array.MapBuilder)
	metaB := b.Field(3).(*array.MapBuilder)
	for _, r := range rows {
		tsB.Append(arrow.Timestamp(schema.TS(r.ts)))
		lineB.Append(r.line)
		uploadAppendMap(labelsB, r.labels)
		uploadAppendMap(metaB, r.meta)
		for i := 4; i < s.NumFields(); i++ {
			sb := b.Field(i).(*array.StringBuilder)
			if s.Field(i).Name != "level" || r.level == nil {
				sb.AppendNull()
				continue
			}
			sb.Append(*r.level)
		}
	}
	return b.NewRecordBatch()
}

func uploadAppendMap(mb *array.MapBuilder, m map[string]string) {
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		kb.Append(k)
		ib.Append(m[k])
	}
}

func uploadReadMap(mc *array.Map, row int) map[string]string {
	beg, end := mc.ValueOffsets(row)
	keys := mc.Keys().(*array.String)
	items := mc.Items().(*array.String)
	out := make(map[string]string, int(end-beg))
	for i := int(beg); i < int(end); i++ {
		out[keys.Value(i)] = items.Value(i)
	}
	return out
}

// uploadFailingCatalog fails AddFiles while delegating everything else.
type uploadFailingCatalog struct {
	catalog.Catalog
	err   error
	calls int
}

func (c *uploadFailingCatalog) AddFiles(context.Context, ...catalog.File) error {
	c.calls++
	return c.err
}

// uploadCountingCatalog counts AddFiles calls and delegates them, so a test
// can prove a resumed publication did not re-add its row.
type uploadCountingCatalog struct {
	catalog.Catalog
	calls int
}

func (c *uploadCountingCatalog) AddFiles(ctx context.Context, files ...catalog.File) error {
	c.calls++
	return c.Catalog.AddFiles(ctx, files...)
}

// uploadStoreThenFailPut stores the object and then reports an injected error,
// simulating a Put whose success the caller never observed.
type uploadStoreThenFailPut struct {
	objstore.Store
	err   error
	calls int
}

func (s *uploadStoreThenFailPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.calls++
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		return err
	}
	return s.err
}

// uploadFailBeforePut fails without writing.
type uploadFailBeforePut struct {
	objstore.Store
	err error
}

func (s *uploadFailBeforePut) Put(context.Context, string, io.Reader, int64) error {
	return s.err
}

// uploadStepClock advances once, at advanceAt, to simulate time passing
// inside one durable step.
type uploadStepClock struct {
	now       time.Time
	calls     int
	advanceAt int
	advanceBy time.Duration
	advanced  bool
}

func (c *uploadStepClock) Now() time.Time {
	c.calls++
	if !c.advanced && c.calls >= c.advanceAt {
		c.now = c.now.Add(c.advanceBy)
		c.advanced = true
	}
	return c.now
}

// uploadCreepClock advances on every call from startAt onward, so every
// post-Put freshness re-check sees an expired completion.
type uploadCreepClock struct {
	now       time.Time
	calls     int
	startAt   int
	advanceBy time.Duration
}

func (c *uploadCreepClock) Now() time.Time {
	c.calls++
	if c.calls >= c.startAt {
		c.now = c.now.Add(c.advanceBy)
	}
	return c.now
}

// uploadKeyListPut records Put keys and delegates.
type uploadKeyListPut struct {
	objstore.Store
	keys []string
}

func (s *uploadKeyListPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.keys = append(s.keys, key)
	return s.Store.Put(ctx, key, r, size)
}

// uploadSplitPutKeys splits recorded Put keys into data and sidecar keys,
// preserving order. It fails the test on any key that is neither.
func uploadSplitPutKeys(t *testing.T, keys []string) (data, sidecars []string) {
	t.Helper()
	for _, key := range keys {
		switch {
		case strings.HasSuffix(key, objstore.ExtData):
			data = append(data, key)
		case strings.HasSuffix(key, objstore.ExtSidecar):
			sidecars = append(sidecars, key)
		default:
			t.Fatalf("unexpected Put key %q", key)
		}
	}
	return data, sidecars
}

// uploadCountingPut counts Put calls and delegates them.
type uploadCountingPut struct {
	objstore.Store
	calls int
}

func (s *uploadCountingPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.calls++
	return s.Store.Put(ctx, key, r, size)
}

// uploadObjectBytes reads a stored object's raw bytes so a test can prove a
// key was never rewritten.
func uploadObjectBytes(t *testing.T, f *uploadFixture, key string) []byte {
	t.Helper()
	rc, err := f.objects.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get object %q: %v", key, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("close object %q: %v", key, err)
		}
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read object %q: %v", key, err)
	}
	return data
}

// uploadCheckingCatalog records whether each added row's data and sidecar
// objects already exist, and optionally arms a later failure once AddFiles
// committed.
type uploadCheckingCatalog struct {
	catalog.Catalog
	objects      objstore.Store
	afterAdd     func()
	calls        int
	dataAtAdd    []bool
	sidecarAtAdd []bool
}

func (c *uploadCheckingCatalog) AddFiles(ctx context.Context, files ...catalog.File) error {
	c.calls++
	for _, f := range files {
		_, dataErr := c.objects.Stat(ctx, f.Key)
		c.dataAtAdd = append(c.dataAtAdd, dataErr == nil)
		_, sidecarErr := c.objects.Stat(ctx, objstore.SidecarKey(f.Key))
		c.sidecarAtAdd = append(c.sidecarAtAdd, sidecarErr == nil)
	}
	if err := c.Catalog.AddFiles(ctx, files...); err != nil {
		return err
	}
	if c.afterAdd != nil {
		c.afterAdd()
	}
	return nil
}

// mergeOldWriterLayout is the injected pre-change writer used only for the
// first pass of the old-writer scenario: it emits the base columns (the labels
// map still carries every label, level included) and no promoted columns, so
// the resumed real merge with the new PromotedFor produces a different schema
// and different bytes without losing any label value.
func mergeOldWriterLayout(ctx context.Context, w io.Writer, inputs []*storage.Reader, _ []string) (storage.FileStats, error) {
	s, err := schema.ForStream(nil)
	if err != nil {
		return storage.FileStats{}, err
	}
	writer, err := storage.NewWriter(w, s, nil)
	if err != nil {
		return storage.FileStats{}, err
	}
	for _, input := range inputs {
		for rec, err := range input.Records(ctx) {
			if err != nil {
				_, _ = writer.Close()
				return storage.FileStats{}, err
			}
			base := array.NewRecordBatch(s, rec.Columns()[:4], rec.NumRows())
			err = writer.Write(base)
			base.Release()
			if err != nil {
				_, _ = writer.Close()
				return storage.FileStats{}, err
			}
		}
	}
	return writer.Close()
}

// uploadCancelPut cancels the pass context instead of writing the object.
type uploadCancelPut struct {
	objstore.Store
	cancel context.CancelFunc
}

func (s *uploadCancelPut) Put(context.Context, string, io.Reader, int64) error {
	s.cancel()
	return context.Canceled
}

// uploadCloseFailFile closes the real file and then reports an injected error,
// so FinalizeUploadTemp fails after the handle is closed.
type uploadCloseFailFile struct {
	syncFile
	err error
}

func (f uploadCloseFailFile) Close() error {
	return errors.Join(f.syncFile.Close(), f.err)
}

func TestNewUploaderValidation(t *testing.T) {
	f := uploadNewFixture(t)
	base := func() UploaderOptions {
		return UploaderOptions{
			Local: f.local, Objects: f.objects, Catalog: f.cat,
			PromotedFor: func(string) []string { return nil },
			MinBytes:    1, MaxAge: time.Hour, MergeMax: 1024,
		}
	}
	tests := []struct {
		name   string
		mutate func(*UploaderOptions)
		want   string
	}{
		{"local", func(o *UploaderOptions) { o.Local = nil }, "Local"},
		{"objects", func(o *UploaderOptions) { o.Objects = nil }, "Objects"},
		{"catalog", func(o *UploaderOptions) { o.Catalog = nil }, "Catalog"},
		{"promoted", func(o *UploaderOptions) { o.PromotedFor = nil }, "PromotedFor"},
		{"min bytes", func(o *UploaderOptions) { o.MinBytes = 0 }, "MinBytes"},
		{"max age", func(o *UploaderOptions) { o.MaxAge = 0 }, "MaxAge"},
		{"merge max", func(o *UploaderOptions) { o.MergeMax = 0 }, "MergeMax"},
		{"partial index params", func(o *UploaderOptions) { o.Index = index.Params{N: 4} }, "index params"},
		{"invalid index params", func(o *UploaderOptions) {
			o.Index = index.Params{N: 9, FPRate: 0.01, MaxGrams: 1 << 20}
		}, "index params"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := base()
			tt.mutate(&o)
			if _, err := NewUploader(o); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewUploader() error = %v, want mention of %s", err, tt.want)
			}
		})
	}
	u, err := NewUploader(base())
	if err != nil {
		t.Fatalf("NewUploader(valid): %v", err)
	}
	if u.indexParams != index.DefaultParams {
		t.Fatalf("default index params = %+v, want %+v", u.indexParams, index.DefaultParams)
	}
}

func TestUploaderByteTriggerMergesAndPublishes(t *testing.T) {
	f := uploadNewFixture(t)
	f.promote = func(string) []string { return []string{"level"} }
	hour := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	first := f.seed(tenant.Default, "checkout", hour, []uploadRow{
		{ts: hour.Add(10 * time.Minute), line: "first-10", level: uploadText("info")},
		{ts: hour.Add(30 * time.Minute), line: "first-30", level: uploadText("error")},
	})
	second := f.seed(tenant.Default, "checkout", hour, []uploadRow{
		{ts: hour.Add(5 * time.Minute), line: "second-05", level: uploadText("info")},
		{ts: hour.Add(40 * time.Minute), line: "second-40", level: uploadText("warn")},
	})
	total := first.Bytes + second.Bytes

	// One byte short of the trigger: nothing is uploaded.
	notDue := f.uploader(UploaderOptions{
		MinBytes: total + 1, MaxAge: 24 * time.Hour, MergeMax: 1 << 20, Now: time.Now,
	})
	if report, err := notDue.RunOnce(t.Context()); err != nil || report.Uploads != 0 {
		t.Fatalf("RunOnce below threshold = %+v, %v; want no upload", report, err)
	}
	if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows below threshold = %d, want 0", got)
	}

	u := f.uploader(UploaderOptions{MinBytes: total, MaxAge: 24 * time.Hour, MergeMax: 1 << 20, Now: time.Now})
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 0 || len(report.Completed) != 1 {
		t.Fatalf("report = %+v, want one fresh upload", report)
	}
	res := report.Completed[0]
	if res.Sources != 2 || res.Rows != 4 || res.Resumed {
		t.Fatalf("result = %+v, want 2 sources, 4 rows, fresh", res)
	}

	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 {
		t.Fatalf("catalog rows = %d, want 1", len(files))
	}
	file := files[0]
	info, err := f.objects.Stat(t.Context(), file.Key)
	if err != nil {
		t.Fatalf("object Stat: %v", err)
	}
	if file.Key != res.Key || file.Bytes != info.Size || res.Bytes != info.Size {
		t.Fatalf("bytes mismatch: row=%d object=%d result=%d", file.Bytes, info.Size, res.Bytes)
	}
	if file.Rows != 4 || res.Rows != 4 {
		t.Fatalf("rows mismatch: row=%d result=%d, want 4", file.Rows, res.Rows)
	}
	wantMin, wantMax := hour.Add(5*time.Minute), hour.Add(40*time.Minute)
	if !file.MinTS.Equal(wantMin) || !file.MaxTS.Equal(wantMax) {
		t.Fatalf("file range = %s..%s, want %s..%s", file.MinTS, file.MaxTS, wantMin, wantMax)
	}
	if got, want := file.LabelValues, map[string][]string{"level": {"error", "info", "warn"}}; !mapsEqual(got, want) {
		t.Fatalf("label values = %v, want %v", got, want)
	}

	rows := f.readObject(tenant.Default, file.Key)
	wantLines := []string{"second-05", "first-10", "first-30", "second-40"}
	if len(rows) != len(wantLines) {
		t.Fatalf("merged rows = %d, want %d", len(rows), len(wantLines))
	}
	for i, row := range rows {
		if row.line != wantLines[i] {
			t.Fatalf("row %d line = %q, want %q (merge not stable by timestamp)", i, row.line, wantLines[i])
		}
		if i > 0 && row.ts.Before(rows[i-1].ts) {
			t.Fatalf("row %d timestamp %s before previous", i, row.ts)
		}
	}
	if *rows[1].level != "info" || *rows[2].level != "error" {
		t.Fatalf("promoted level not preserved: %+v", rows)
	}

	// Publication retired and cleaned up both sources, the receipt and the
	// finalized temp; the local files are gone.
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after upload = %+v, want none", got)
	}
	if got := f.tenantStore(tenant.Default); len(mustPending(t, got)) != 0 {
		t.Fatalf("pending files after upload = %+v, want none", got)
	}
	for _, file := range []LocalFile{first, second} {
		if _, err := os.Stat(f.localPath(tenant.Default, file)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source %s still on disk: %v", file.ID, err)
		}
	}
	if report.Bytes != info.Size || report.Rows != 4 {
		t.Fatalf("report totals = %d bytes %d rows, want %d, 4", report.Bytes, report.Rows, info.Size)
	}
}

func TestUploaderAgeTrigger(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 11, 0, 0, 0, time.UTC)
	file := f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "aged"},
	})
	now := time.Now()

	fresh := f.uploader(UploaderOptions{
		MinBytes: file.Bytes + 1, MaxAge: time.Hour, MergeMax: 1 << 20, Now: func() time.Time { return now },
	})
	if report, err := fresh.RunOnce(t.Context()); err != nil || report.Uploads != 0 {
		t.Fatalf("fresh RunOnce = %+v, %v; want no upload", report, err)
	}

	aged := f.uploader(UploaderOptions{
		MinBytes: file.Bytes + 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return now.Add(time.Hour) }, // exactly at the boundary
	})
	report, err := aged.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("aged RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("aged report = %+v, want one upload", report)
	}
}

func TestUploaderMergeCapAndOversizedSingleton(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	var seeded []LocalFile
	for i := range 3 {
		seeded = append(seeded, f.seed(tenant.Default, "api", hour, []uploadRow{
			{ts: hour.Add(time.Duration(i) * time.Minute), line: "cap"},
		}))
	}
	slices.SortFunc(seeded, func(a, b LocalFile) int { return a.ID.Compare(b.ID) })
	capBytes := seeded[0].Bytes + seeded[1].Bytes

	u := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: capBytes})
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Every due chunk is reserved in one pass: the capped pair and the
	// remaining singleton, in durable source order.
	if report.Uploads != 2 || report.Completed[0].Sources != 2 || report.Completed[1].Sources != 1 {
		t.Fatalf("pass = %+v, want a 2-source chunk then the remaining source", report)
	}
	if pending := mustPending(t, f.tenantStore(tenant.Default)); len(pending) != 0 {
		t.Fatalf("pending files = %d, want 0", len(pending))
	}

	// A single file larger than MergeMax is uploaded alone instead of
	// stalling forever.
	big := f.seed(tenant.Default, "api", hour.Add(2*time.Hour), []uploadRow{
		{ts: hour.Add(2*time.Hour + time.Minute), line: "oversized"},
	})
	small := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: big.Bytes - 1})
	report, err = small.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("oversized RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].Sources != 1 || report.Completed[0].Rows != 1 {
		t.Fatalf("oversized pass = %+v, want one singleton upload", report)
	}
}

func TestUploaderIsolationByTenantStreamHour(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)
	other := tenant.ID("other")
	cases := []struct {
		id     tenant.ID
		stream string
		hour   time.Time
	}{
		{tenant.Default, "api", hour},
		{tenant.Default, "auth", hour},
		{tenant.Default, "api", hour.Add(time.Hour)},
		{other, "api", hour},
	}
	for _, c := range cases {
		f.seed(c.id, c.stream, c.hour, []uploadRow{{ts: c.hour.Add(time.Minute), line: c.stream}})
	}
	report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != len(cases) {
		t.Fatalf("uploads = %d, want %d: %+v", report.Uploads, len(cases), report.Completed)
	}
	for _, c := range cases {
		res, ok := uploadResultFor(report, c.id, c.stream, c.hour)
		if !ok {
			t.Fatalf("no completed upload for %q %s %s: %+v", c.id, c.stream, c.hour, report.Completed)
		}
		var matching []catalog.File
		for _, file := range f.liveCatalogFiles(c.id) {
			if file.Key == res.Key {
				matching = append(matching, file)
			}
		}
		if len(matching) != 1 {
			t.Fatalf("tenant %q rows with key %q = %d, want 1", c.id, res.Key, len(matching))
		}
		file := matching[0]
		if file.Stream != c.stream || !file.MinTS.Equal(c.hour.Add(time.Minute)) {
			t.Fatalf("row %+v mixed partitions, want %s %s", file, c.stream, c.hour)
		}
		prefix := objstore.LogPrefix(c.id, c.stream) + c.hour.UTC().Format("2006/01/02/15/")
		if file.Key != res.Key || !strings.HasPrefix(res.Key, prefix) || !strings.HasSuffix(res.Key, objstore.ExtData) {
			t.Fatalf("key = %q (result %q), want a canonical key under %q", file.Key, res.Key, prefix)
		}
	}
}

// uploadResultFor finds the completed upload of one partition.
func uploadResultFor(report UploadReport, id tenant.ID, stream string, hour time.Time) (UploadResult, bool) {
	for _, res := range report.Completed {
		if res.Tenant == id && res.Stream == stream && res.Hour.Equal(hour) {
			return res, true
		}
	}
	return UploadResult{}, false
}

func TestUploaderPutSuccessThenErrorRecoversAfterReopen(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "kept"}})

	putErr := errors.New("put acknowledgement lost")
	objects := &uploadStoreThenFailPut{Store: f.objects, err: putErr}
	u := f.uploader(UploaderOptions{Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20})
	report, err := u.RunOnce(t.Context())
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	if report.Uploads != 0 {
		t.Fatalf("report = %+v, want no completed upload", report)
	}
	if objects.calls != 1 {
		t.Fatalf("Put calls = %d, want 1", objects.calls)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
		t.Fatalf("catalog rows after unacknowledged Put = %d, want 0", len(got))
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadMerged {
		t.Fatalf("receipt stage = %q, want merged before RecordPut", receipt.Stage)
	}
	if !receipt.PutCompletedAt.IsZero() {
		t.Fatalf("PutCompletedAt = %s, want unknown", receipt.PutCompletedAt)
	}
	if receipt.Key == "" || len(receipt.Sources) != 1 || receipt.Sources[0] != source.ID {
		t.Fatalf("receipt = %+v, want stable key and source", receipt)
	}
	oldKey := receipt.Key
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source removed by unacknowledged Put: %v", err)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("finalized temp missing after unacknowledged Put: %v", err)
	}
	if _, err := f.objects.Stat(t.Context(), oldKey); err != nil {
		t.Fatalf("object not stored despite injected error: %v", err)
	}

	f.reopen()

	report, err = f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("retry report = %+v, want one resumed upload", report)
	}
	if got := report.Completed[0].ID; got != receipt.ID {
		t.Fatalf("retry id = %s, want stable %s", got, receipt.ID)
	}
	// An unknown Put time is treated as possibly completed: the key rotates
	// so a stale orphan can never be published.
	newKey := report.Completed[0].Key
	if newKey == oldKey {
		t.Fatalf("retry reused key %q despite unknown PutCompletedAt", oldKey)
	}
	if !strings.HasPrefix(newKey, objstore.LogPrefix(tenant.Default, "api")) {
		t.Fatalf("rotated key %q is not canonical for the tenant/stream", newKey)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != newKey {
		t.Fatalf("catalog after retry = %+v, want one row with key %q", files, newKey)
	}
	if files[0].Key == oldKey {
		t.Fatalf("catalog published the possibly-collected old key %q", oldKey)
	}
	if report.Completed[0].Rows != 1 {
		t.Fatalf("retry rows = %d, want 1", report.Completed[0].Rows)
	}
	if _, err := f.objects.Stat(t.Context(), oldKey); err != nil {
		t.Fatalf("retained old object was disturbed: %v", err)
	}
	if rows := f.readObject(tenant.Default, newKey); len(rows) != 1 || rows[0].line != "kept" {
		t.Fatalf("recovered object rows = %+v, want the single source row once", rows)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after recovery = %+v, want none", got)
	}
}

// uploadStoredAt drives a fresh upload to the stored stage with a failing
// catalog at clock, returning the durable receipt.
func uploadStoredAt(t *testing.T, f *uploadFixture, clock time.Time, rows []uploadRow) Upload {
	t.Helper()
	hour := clock.UTC().Truncate(time.Hour)
	f.seed(tenant.Default, "api", hour, rows)
	catErr := errors.New("catalog unavailable")
	broken := &uploadFailingCatalog{Catalog: f.cat, err: catErr}
	u := f.uploader(UploaderOptions{
		Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	})
	if _, err := u.RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("seed stored receipt: RunOnce error = %v, want %v", err, catErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored {
		t.Fatalf("seed stored receipt stage = %q, want stored", receipt.Stage)
	}
	return receipt
}

func TestUploaderStaleStoredReceiptRotatesAfterOrphanGC(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	clock := time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC)
	receipt := uploadStoredAt(t, f, clock, []uploadRow{
		{ts: clock.Truncate(time.Hour).Add(time.Minute), line: "one"},
		{ts: clock.Truncate(time.Hour).Add(2 * time.Minute), line: "two"},
	})
	if receipt.PutCompletedAt.IsZero() || !receipt.PutCompletedAt.Equal(clock) {
		t.Fatalf("PutCompletedAt = %s, want %s", receipt.PutCompletedAt, clock)
	}
	oldKey := receipt.Key
	if err := f.objects.Delete(ctx, oldKey); err != nil {
		t.Fatalf("simulate orphan GC: %v", err)
	}

	// The receipt and its old key survive the restart, but the old object is
	// gone; publishing it would be a permanent 404.
	clock = clock.Add(31 * time.Minute)
	f.reopen()
	reopened := f.onlyReceipt(tenant.Default)
	if reopened.ID != receipt.ID || reopened.Key != oldKey {
		t.Fatalf("reopened receipt = %+v, want id %s key %q", reopened, receipt.ID, oldKey)
	}

	report, err := f.uploader(UploaderOptions{
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("report = %+v, want one resumed upload", report)
	}
	res := report.Completed[0]
	if res.ID != receipt.ID {
		t.Fatalf("receipt id = %s, want stable %s", res.ID, receipt.ID)
	}
	if res.Key == oldKey {
		t.Fatalf("published the collected old key %q", oldKey)
	}
	if !strings.HasPrefix(res.Key, objstore.LogPrefix(tenant.Default, "api")) {
		t.Fatalf("rotated key %q is not canonical", res.Key)
	}
	if _, err := f.objects.Stat(ctx, oldKey); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("old key was recreated: %v", err)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != res.Key {
		t.Fatalf("catalog = %+v, want exactly the rotated key %q", files, res.Key)
	}
	if rows := f.readObject(tenant.Default, res.Key); len(rows) != 2 || rows[0].line != "one" || rows[1].line != "two" {
		t.Fatalf("rotated object rows = %+v, want both rows once", rows)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after publication = %+v, want none", got)
	}
}

func TestUploaderRefreshKeyPersistsAcrossReopen(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	clock := time.Date(2026, 11, 4, 2, 0, 0, 0, time.UTC)
	receipt := uploadStoredAt(t, f, clock, []uploadRow{{ts: clock.Truncate(time.Hour).Add(time.Minute), line: "rotated"}})
	oldKey := receipt.Key
	if err := f.objects.Delete(ctx, oldKey); err != nil {
		t.Fatalf("simulate orphan GC: %v", err)
	}
	clock = clock.Add(31 * time.Minute)
	f.reopen()

	putErr := errors.New("put refused")
	_, err := f.uploader(UploaderOptions{
		Objects:  &uploadFailBeforePut{Store: f.objects, err: putErr},
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	}).RunOnce(ctx)
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	rotated := f.onlyReceipt(tenant.Default)
	if rotated.ID != receipt.ID {
		t.Fatalf("receipt id = %s, want stable %s", rotated.ID, receipt.ID)
	}
	if rotated.Key == oldKey {
		t.Fatalf("key not rotated before the refused Put: %q", rotated.Key)
	}
	if rotated.Stage != UploadMerged || !rotated.PutCompletedAt.IsZero() {
		t.Fatalf("rotated receipt = %+v, want merged with unknown Put time", rotated)
	}
	newKey := rotated.Key

	f.reopen()
	persisted := f.onlyReceipt(tenant.Default)
	if persisted.Key != newKey || persisted.Stage != UploadMerged || !persisted.PutCompletedAt.IsZero() {
		t.Fatalf("after reopen receipt = %+v, want persisted key %q", persisted, newKey)
	}

	// The unknown Put time forces one more conservative rotation; publication
	// then succeeds under a fresh key, leaving both earlier keys untouched.
	report, err := f.uploader(UploaderOptions{
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("recovery RunOnce: %v", err)
	}
	res := report.Completed[0]
	if res.ID != receipt.ID {
		t.Fatalf("recovered receipt id = %s, want %s", res.ID, receipt.ID)
	}
	if res.Key == oldKey || res.Key == newKey {
		t.Fatalf("recovery reused an unproven key %q", res.Key)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != res.Key {
		t.Fatalf("catalog = %+v, want only the final key %q", files, res.Key)
	}
	for _, key := range []string{oldKey, newKey} {
		for _, file := range f.allCatalogFiles(tenant.Default) {
			if file.Key == key {
				t.Fatalf("unproven key %q reached the catalog", key)
			}
		}
		if key == oldKey {
			if _, err := f.objects.Stat(ctx, key); !errors.Is(err, objstore.ErrNotExist) {
				t.Fatalf("orphaned old key %q was recreated", key)
			}
		}
	}
	if rows := f.readObject(tenant.Default, res.Key); len(rows) != 1 || rows[0].line != "rotated" {
		t.Fatalf("final object rows = %+v, want the source row once", rows)
	}
}

// uploadMergeMode selects how the pre-crash object K was produced.
type uploadMergeMode string

const (
	// uploadMergeReal merges both passes with storage.Merge; only the
	// PromotedFor changes between them.
	uploadMergeReal uploadMergeMode = "real-merge"
	// uploadMergeOldWriter injects a pre-change writer for the first pass
	// only. It keeps every label (level included) in the labels map but writes
	// no promoted columns, so the resumed real merge with the changed
	// PromotedFor changes the schema and bytes.
	uploadMergeOldWriter uploadMergeMode = "injected-old-writer"
)

func TestUploaderCompletedPutRotatesAfterPurgedPublication(t *testing.T) {
	for _, kind := range uploadCatalogKinds() {
		for _, mode := range []uploadMergeMode{uploadMergeReal, uploadMergeOldWriter} {
			t.Run(string(kind)+"/"+string(mode), func(t *testing.T) {
				uploadCompletedPutScenario(t, kind, mode)
			})
		}
	}
}

// uploadCompletedPutScenario drives one catalog/writer combination through a
// stored receipt whose published key was purged before MarkPublished. The
// shared lifecycle assertions cover both writer modes so the regression body
// is not duplicated.
func uploadCompletedPutScenario(t *testing.T, kind uploadCatalogKind, mode uploadMergeMode) {
	t.Helper()
	// Crash after AddFiles committed but before MarkPublished: the receipt is
	// stored and its Put completion is fresh. The compactor may still replace
	// and purge that key, and the upload temp may be gone. Resuming must
	// remerge and Put a key never written before; rewriting the stored key
	// would poison the catalog and DuckDB's metadata cache.
	base := time.Date(2026, 11, 4, 5, 0, 0, 0, time.UTC)
	hour := base.Truncate(time.Hour)
	ctx := t.Context()
	clock := base
	failManifestRename := false
	f := uploadNewFixtureKind(t, kind, func(ops *localFileOps) {
		rename := ops.rename
		ops.rename = func(root *os.Root, old, new string) error {
			if failManifestRename && old == manifestTempName && new == manifestName {
				failManifestRename = false
				return errors.New("injected MarkPublished manifest failure")
			}
			return rename(root, old, new)
		}
	})
	f.promote = func(string) []string { return []string{"level"} }
	source := f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "one", level: uploadText("info"), labels: map[string]string{"level": "info"}},
		{ts: hour.Add(2 * time.Minute), line: "two", level: uploadText("error"), labels: map[string]string{"level": "error"}},
	})

	// The first pass publishes K with the pre-change PromotedFor (nil) and
	// adds its catalog row; MarkPublished's manifest commit then fails,
	// leaving a stored receipt with a fresh Put completion.
	seedPuts := &uploadKeyListPut{Store: f.objects}
	arming := &uploadCheckingCatalog{Catalog: f.cat, objects: f.objects}
	arming.afterAdd = func() { failManifestRename = true }
	first := f.uploader(UploaderOptions{
		Objects: seedPuts, Catalog: arming, PromotedFor: func(string) []string { return nil },
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	})
	if mode == uploadMergeOldWriter {
		first.merge = mergeOldWriterLayout
	}
	if _, err := first.RunOnce(ctx); err == nil || !strings.Contains(err.Error(), "injected MarkPublished manifest failure") {
		t.Fatalf("seed RunOnce error = %v, want the injected MarkPublished failure", err)
	}
	if arming.calls != 1 || len(arming.dataAtAdd) != 1 || !arming.dataAtAdd[0] || !arming.sidecarAtAdd[0] {
		t.Fatalf("seed AddFiles = %d calls dataAtAdd=%v sidecarAtAdd=%v, want one row with both objects present",
			arming.calls, arming.dataAtAdd, arming.sidecarAtAdd)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored {
		t.Fatalf("receipt stage = %q, want stored after failed MarkPublished", receipt.Stage)
	}
	if receipt.PutCompletedAt.IsZero() || !receipt.PutCompletedAt.Equal(clock) {
		t.Fatalf("PutCompletedAt = %s, want the fresh %s", receipt.PutCompletedAt, clock)
	}
	if clock.Sub(receipt.PutCompletedAt) >= uploadOrphanGrace {
		t.Fatalf("completion %s is not fresh at %s; the in-grace branch must be exercised", receipt.PutCompletedAt, clock)
	}
	oldKey := receipt.Key
	oldSidecarKey := objstore.SidecarKey(oldKey)
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, seedPuts.keys)
	if !slices.Equal(dataKeys, []string{oldKey}) || !slices.Equal(sidecarKeys, []string{oldSidecarKey}) {
		t.Fatalf("seed Put keys = %v, want exactly data %q and sidecar %q", seedPuts.keys, oldKey, oldSidecarKey)
	}
	oldBytes := uploadObjectBytes(t, f, oldKey)
	oldSidecarBytes := uploadObjectBytes(t, f, oldSidecarKey)
	oldRows := f.readObject(tenant.Default, oldKey)
	if len(oldRows) != 2 || oldRows[0].line != "one" || oldRows[1].line != "two" {
		t.Fatalf("old object rows = %+v, want the source rows", oldRows)
	}
	switch mode {
	case uploadMergeReal:
		// The real merge unions the source's promoted column even when
		// the caller promotes nothing, so K already carries level.
		if oldRows[0].level == nil || *oldRows[0].level != "info" ||
			oldRows[1].level == nil || *oldRows[1].level != "error" {
			t.Fatalf("old real-merge rows = %+v, want the source level column", oldRows)
		}
	case uploadMergeOldWriter:
		// The injected old writer drops the promoted column but keeps
		// the effective label value in the labels map.
		if oldRows[0].level != nil || oldRows[1].level != nil {
			t.Fatalf("old-writer rows = %+v, want no level column", oldRows)
		}
		if oldRows[0].labels["level"] != "info" || oldRows[1].labels["level"] != "error" {
			t.Fatalf("old-writer rows = %+v, want level preserved in labels", oldRows)
		}
	}

	// The compactor replaces K with a bigger row, then purges its
	// tombstone. The replacement stays live: the resumed upload adds
	// its rows again, which is the accepted at-least-once history.
	replacement := catalog.File{
		Tenant: tenant.Default, Stream: "api",
		Key:   objstore.NewDataKey(tenant.Default, "api", hour),
		MinTS: receipt.MinTS, MaxTS: receipt.MaxTS,
		Rows: receipt.Rows + 1, Bytes: receipt.Bytes,
		LabelValues:    maps.Clone(receipt.LabelValues),
		LabelsComplete: receipt.LabelsComplete,
		CreatedAt:      receipt.CreatedAt.Add(-time.Minute),
	}
	if err := f.cat.ReplaceFiles(ctx, tenant.Default, []string{oldKey}, []catalog.File{replacement}); err != nil {
		t.Fatalf("replace key: %v", err)
	}
	purged, err := f.cat.PurgeDeleted(ctx, tenant.Default, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("purge deleted: %v", err)
	}
	if !slices.Equal(purged, []string{oldKey}) {
		t.Fatalf("purged keys = %v, want %q", purged, oldKey)
	}
	if err := os.Remove(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("remove upload temp: %v", err)
	}
	f.reopen()
	reopened := f.onlyReceipt(tenant.Default)
	if reopened.ID != receipt.ID || reopened.Stage != UploadStored || reopened.Key != oldKey {
		t.Fatalf("reopened receipt = %+v, want stored %s under %q", reopened, receipt.ID, oldKey)
	}
	if !reopened.PutCompletedAt.Equal(clock) || clock.Sub(reopened.PutCompletedAt) >= uploadOrphanGrace {
		t.Fatalf("reopened completion = %s at %s, want the fresh %s", reopened.PutCompletedAt, clock, clock)
	}

	// Resume with the new PromotedFor and the real merge: no catalog
	// row and no temp remain, so the receipt must remerge.
	var mergedPromoted [][]string
	resumePuts := &uploadKeyListPut{Store: f.objects}
	checking := &uploadCheckingCatalog{Catalog: f.cat, objects: f.objects}
	resume := f.uploader(UploaderOptions{
		Objects: resumePuts, Catalog: checking,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		Now: func() time.Time { return clock },
	})
	resume.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		mergedPromoted = append(mergedPromoted, slices.Clone(promoted))
		return storage.Merge(ctx, w, inputs, promoted)
	}
	report, err := resume.RunOnce(ctx)
	if err != nil {
		t.Fatalf("resume RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("resume report = %+v, want the resumed receipt %s", report, receipt.ID)
	}
	if len(mergedPromoted) != 1 || !slices.Equal(mergedPromoted[0], []string{"level"}) {
		t.Fatalf("remerge PromotedFor = %v, want [level]", mergedPromoted)
	}
	newKey := report.Completed[0].Key
	newSidecarKey := objstore.SidecarKey(newKey)

	// Per-key Put counts: the old pair once, the new pair once. This
	// is the regression's core: the pre-fix uploader re-Put the stored
	// key because its completion was still inside the grace.
	allPuts := slices.Concat(seedPuts.keys, resumePuts.keys)
	dataKeys, _ = uploadSplitPutKeys(t, allPuts)
	if got := countUploadKey(dataKeys, oldKey); got != 1 {
		t.Fatalf("old data key %q Put %d times, want 1 (all puts %v)", oldKey, got, allPuts)
	}
	if newKey == oldKey {
		t.Fatalf("resume reused the stored key %q", oldKey)
	}
	if !strings.HasPrefix(newKey, objstore.LogPrefix(tenant.Default, "api")) || !strings.HasSuffix(newKey, objstore.ExtData) {
		t.Fatalf("resumed key %q is not canonical", newKey)
	}
	counts := uploadPutCounts(t, allPuts)
	wantKeys := []string{oldKey, oldSidecarKey, newKey, newSidecarKey}
	if len(counts) != len(wantKeys) {
		t.Fatalf("Put keys = %v, want exactly %v", allPuts, wantKeys)
	}
	for _, key := range wantKeys {
		if counts[key] != 1 {
			t.Fatalf("object key %q Put %d times, want 1 (all puts %v)", key, counts[key], allPuts)
		}
	}

	// The old objects are immutable, and the new pair existed before
	// AddFiles was called.
	if got := uploadObjectBytes(t, f, oldKey); !bytes.Equal(got, oldBytes) {
		t.Fatal("old data object was rewritten")
	}
	if got := uploadObjectBytes(t, f, oldSidecarKey); !bytes.Equal(got, oldSidecarBytes) {
		t.Fatal("old sidecar object was rewritten")
	}
	if checking.calls != 1 || len(checking.dataAtAdd) != 1 || !checking.dataAtAdd[0] || !checking.sidecarAtAdd[0] {
		t.Fatalf("resume AddFiles = %d calls dataAtAdd=%v sidecarAtAdd=%v, want the new pair present",
			checking.calls, checking.dataAtAdd, checking.sidecarAtAdd)
	}
	if _, err := f.objects.Stat(ctx, newKey); err != nil {
		t.Fatalf("new data object missing: %v", err)
	}
	if _, err := f.objects.Stat(ctx, newSidecarKey); err != nil {
		t.Fatalf("new sidecar object missing: %v", err)
	}

	// The remerge ran with the new PromotedFor and the new object
	// carries the promoted level column.
	rows := f.readObject(tenant.Default, newKey)
	if len(rows) != 2 || rows[0].line != "one" || rows[1].line != "two" {
		t.Fatalf("resumed rows = %+v, want the source rows once", rows)
	}
	if rows[0].level == nil || *rows[0].level != "info" || rows[1].level == nil || *rows[1].level != "error" {
		t.Fatalf("resumed rows = %+v, want the promoted level column", rows)
	}
	if mode == uploadMergeOldWriter {
		// The resumed real merge with the changed PromotedFor produces
		// different bytes, not just a different key.
		if bytes.Equal(oldBytes, uploadObjectBytes(t, f, newKey)) {
			t.Fatal("old-writer and resumed real-merge objects have identical bytes")
		}
	}

	// Catalog: the compacted replacement survives, the purged old key
	// stays gone, and the new row keeps the stable receipt identity.
	all := f.allCatalogFiles(tenant.Default)
	if len(all) != 2 {
		t.Fatalf("catalog rows = %+v, want the replacement and the new row", all)
	}
	var gotReplacement, gotNew bool
	for _, row := range all {
		switch row.Key {
		case oldKey:
			t.Fatalf("purged old key %q reappeared in the catalog", oldKey)
		case replacement.Key:
			gotReplacement = !row.Deleted() && row.Rows == replacement.Rows
		case newKey:
			gotNew = !row.Deleted() && row.Rows == 2 &&
				row.CreatedAt.Equal(receipt.CreatedAt) &&
				row.HasSidecar && row.LabelsComplete
		default:
			t.Fatalf("unexpected catalog row %+v", row)
		}
	}
	if !gotReplacement || !gotNew {
		t.Fatalf("catalog rows = %+v, want replacement %q preserved and new %q", all, replacement.Key, newKey)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after resume = %+v, want none", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source not retired: %v", err)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upload temp not cleaned up: %v", err)
	}
}

// countUploadKey counts exact key occurrences in a Put log.
func countUploadKey(keys []string, want string) int {
	var n int
	for _, key := range keys {
		if key == want {
			n++
		}
	}
	return n
}

// uploadPutCounts counts every Put key, sidecars included, and fails when any
// object key was written more than once.
func uploadPutCounts(t *testing.T, keys []string) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(keys))
	for _, key := range keys {
		counts[key]++
	}
	for key, n := range counts {
		if n != 1 {
			t.Fatalf("object key %q Put %d times, want at most 1 (all puts %v)", key, n, keys)
		}
	}
	return counts
}

func TestUploaderPutExpiresDuringRecordPut(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	base := time.Date(2026, 11, 4, 3, 0, 0, 0, time.UTC)
	hour := base.Truncate(time.Hour)
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "one"},
		{ts: hour.Add(2 * time.Minute), line: "two"},
	})

	// Call 1 is the due check, call 2 captures the Put completion, and call 3
	// is the pre-AddFiles freshness re-check. Advancing there models a durable
	// RecordPut (or any step) that outlived the orphan grace.
	clock := &uploadStepClock{now: base, advanceAt: 3, advanceBy: 31 * time.Minute}
	puts := &uploadKeyListPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: puts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20, Now: clock.Now,
	}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Each attempt Puts the data object and then its sidecar under the same
	// base key; a stale attempt is rotated before the next pair.
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, puts.keys)
	if len(dataKeys) != 2 || len(sidecarKeys) != 2 {
		t.Fatalf("Put calls = %v, want two data/sidecar pairs", puts.keys)
	}
	if dataKeys[0] == dataKeys[1] {
		t.Fatalf("re-Put reused key %q after the expiry re-check", dataKeys[0])
	}
	if sidecarKeys[0] != objstore.SidecarKey(dataKeys[0]) || sidecarKeys[1] != objstore.SidecarKey(dataKeys[1]) {
		t.Fatalf("sidecar keys = %v, want one per data key %v", sidecarKeys, dataKeys)
	}
	res := report.Completed[0]
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != dataKeys[1] || res.Key != dataKeys[1] {
		t.Fatalf("catalog=%+v result=%q, want only the rotated key %q", files, res.Key, dataKeys[1])
	}
	if !files[0].HasSidecar {
		t.Fatal("catalog row after a successful sidecar Put has HasSidecar=false")
	}
	if _, err := f.objects.Stat(ctx, sidecarKeys[1]); err != nil {
		t.Fatalf("current-key sidecar missing: %v", err)
	}
	if _, err := f.objects.Stat(ctx, sidecarKeys[0]); err != nil {
		t.Fatalf("stale-key sidecar should stay harmless until orphan GC: %v", err)
	}
	if _, err := f.objects.Stat(ctx, dataKeys[0]); err != nil {
		t.Fatalf("first object should stay harmless: %v", err)
	}
	if rows := f.readObject(tenant.Default, dataKeys[1]); len(rows) != 2 {
		t.Fatalf("final object rows = %d, want both rows once", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
}

func TestUploaderRepeatedExpiryStopsBeforeCatalog(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	base := time.Date(2026, 11, 4, 4, 0, 0, 0, time.UTC)
	hour := base.Truncate(time.Hour)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "slow"}})

	// Every post-Put re-check sees the completion 31 minutes old, so all
	// bounded attempts rotate and the pass must give up unpublished.
	clock := &uploadCreepClock{now: base, startAt: 3, advanceBy: 31 * time.Minute}
	puts := &uploadKeyListPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: puts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20, Now: clock.Now,
	}).RunOnce(ctx)
	if err == nil || !strings.Contains(err.Error(), "stayed older than the orphan grace") {
		t.Fatalf("RunOnce error = %v, want the bounded-expiry error", err)
	}
	if report.Uploads != 0 || len(report.Completed) != 0 {
		t.Fatalf("report = %+v, want no publication", report)
	}
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, puts.keys)
	if len(dataKeys) != uploadPutAttempts || len(sidecarKeys) != uploadPutAttempts {
		t.Fatalf("Put calls = %v, want %d data/sidecar pairs", puts.keys, uploadPutAttempts)
	}
	for i := 1; i < len(dataKeys); i++ {
		if dataKeys[i] == dataKeys[i-1] {
			t.Fatalf("attempt %d reused key %q", i, dataKeys[i])
		}
	}
	for i := range dataKeys {
		if sidecarKeys[i] != objstore.SidecarKey(dataKeys[i]) {
			t.Fatalf("sidecar %q does not belong to data key %q", sidecarKeys[i], dataKeys[i])
		}
	}
	if got := len(f.allCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows = %d, want none before a fresh Put", got)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored || receipt.PutCompletedAt.IsZero() {
		t.Fatalf("receipt = %+v, want stored with a recorded Put time", receipt)
	}
	if receipt.Key != dataKeys[len(dataKeys)-1] {
		t.Fatalf("receipt key = %q, want last attempted %q", receipt.Key, dataKeys[len(dataKeys)-1])
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source removed before publication: %v", err)
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 1 {
		t.Fatalf("pending files = %+v, want the source retained", got)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("merged temp removed before publication: %v", err)
	}
}

func TestUploaderCatalogFailurePreservesSourcesAndRetriesExactlyOnce(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 15, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "retry"}})

	catErr := errors.New("catalog unavailable")
	broken := &uploadFailingCatalog{Catalog: f.cat, err: catErr}
	seedPuts := &uploadKeyListPut{Store: f.objects}
	u := f.uploader(UploaderOptions{
		Objects: seedPuts, Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	})
	if _, err := u.RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, catErr)
	}
	if broken.calls != 1 {
		t.Fatalf("AddFiles calls = %d, want 1", broken.calls)
	}
	if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows after failure = %d, want 0", got)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored || receipt.PutCompletedAt.IsZero() {
		t.Fatalf("receipt = %+v, want stored with a fresh Put completion", receipt)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source removed by failed catalog: %v", err)
	}
	if _, err := f.objects.Stat(t.Context(), receipt.Key); err != nil {
		t.Fatalf("object missing after Put: %v", err)
	}
	oldKey := receipt.Key
	oldSidecarKey := objstore.SidecarKey(oldKey)
	oldBytes := uploadObjectBytes(t, f, oldKey)
	oldSidecarBytes := uploadObjectBytes(t, f, oldSidecarKey)

	f.reopen()

	// The stored key already received a completed Put, so the retry must
	// rotate both the data and sidecar keys instead of overwriting them.
	retryPuts := &uploadKeyListPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("retry = %+v, want one upload with id %s", report, receipt.ID)
	}
	newKey := report.Completed[0].Key
	if newKey == oldKey {
		t.Fatalf("retry rewrote the stored key %q", oldKey)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 {
		t.Fatalf("catalog rows after retry = %d, want exactly 1", len(files))
	}
	if files[0].Key != newKey || !files[0].CreatedAt.Equal(receipt.CreatedAt) {
		t.Fatalf("row identity changed: %+v vs receipt %+v", files[0], receipt)
	}
	if rows := f.readObject(tenant.Default, newKey); len(rows) != 1 || rows[0].line != "retry" {
		t.Fatalf("object rows after recovery = %+v, want the row exactly once", rows)
	}
	if got := uploadObjectBytes(t, f, oldKey); !bytes.Equal(got, oldBytes) {
		t.Fatal("retry rewrote the old data object")
	}
	if got := uploadObjectBytes(t, f, oldSidecarKey); !bytes.Equal(got, oldSidecarBytes) {
		t.Fatal("retry rewrote the old sidecar object")
	}
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, slices.Concat(seedPuts.keys, retryPuts.keys))
	if len(dataKeys) != 2 || len(sidecarKeys) != 2 {
		t.Fatalf("Put keys = %v, want one old and one new data/sidecar pair", slices.Concat(seedPuts.keys, retryPuts.keys))
	}
	if dataKeys[0] != oldKey || dataKeys[1] != newKey {
		t.Fatalf("data Put keys = %v, want old %q then new %q", dataKeys, oldKey, newKey)
	}
	if sidecarKeys[0] != oldSidecarKey || sidecarKeys[1] != objstore.SidecarKey(newKey) {
		t.Fatalf("sidecar Put keys = %v, want one per data key", sidecarKeys)
	}
	if !files[0].HasSidecar {
		t.Fatal("retried row has HasSidecar=false")
	}

	// A third pass has no work and adds no duplicate row.
	report, err = f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil || report.Uploads != 0 {
		t.Fatalf("idle pass = %+v, %v; want no work", report, err)
	}
	if got := len(f.allCatalogFiles(tenant.Default)); got != 1 {
		t.Fatalf("catalog rows after idle pass = %d, want 1", got)
	}
}

func TestUploaderResumeAfterCatalogAddBeforeMarkPublished(t *testing.T) {
	// The reservation is stamped at nanosecond precision. A catalog that keeps
	// only microseconds (SQLite) must still recognize its own row after the
	// crash, otherwise uploadMatchesCatalog rejects the real publication as
	// ErrUploadConflict. One fixed clock with nonzero sub-microsecond nanos
	// drives both backends so the comparison is exercised exactly.
	clock := time.Date(2026, 11, 3, 16, 0, 0, 123456789, time.UTC)
	hour := clock.UTC().Truncate(time.Hour)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, func(ops *localFileOps) {
				ops.now = func() time.Time { return clock }
			})
			ctx := t.Context()
			// A sub-microsecond row timestamp proves stats (and therefore the
			// catalog row's MinTS/MaxTS) never carry more precision than the
			// microsecond represented by Parquet and SQLite.
			rowTS := hour.Add(time.Minute + 789*time.Nanosecond)
			source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: rowTS, line: "published"}})
			if !microAligned(source.MinTS) || !microAligned(source.MaxTS) {
				t.Fatalf("source stats %s..%s are not microsecond-aligned", source.MinTS, source.MaxTS)
			}

			broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
			if _, err := f.uploader(UploaderOptions{
				Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx); err == nil {
				t.Fatal("RunOnce with failing catalog succeeded")
			}
			receipt := f.onlyReceipt(tenant.Default)
			if want := rowTS.Truncate(time.Microsecond); !receipt.MinTS.Equal(want) || !receipt.MaxTS.Equal(want) {
				t.Fatalf("receipt stats = %s..%s, want the truncated %s", receipt.MinTS, receipt.MaxTS, want)
			}
			if !microAligned(receipt.MinTS) || !microAligned(receipt.MaxTS) {
				t.Fatalf("receipt stats %s..%s are not microsecond-aligned", receipt.MinTS, receipt.MaxTS)
			}
			// Crash after the catalog row committed, before MarkPublished.
			if err := f.cat.AddFiles(ctx, uploadCatalogFile(receipt)); err != nil {
				t.Fatalf("seed catalog row: %v", err)
			}
			if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 1 {
				t.Fatalf("seeded catalog rows = %d, want 1", len(rows))
			}

			f.reopen()

			// Resume must recognize the row: no second Put, no second AddFiles,
			// same receipt and key, then retire and clean up the source.
			objects := &uploadCountingPut{Store: f.objects}
			counting := &uploadCountingCatalog{Catalog: f.cat}
			report, err := f.uploader(UploaderOptions{
				Catalog: counting, Objects: objects,
				MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx)
			if err != nil {
				t.Fatalf("resume RunOnce: %v (want the prior publication recognized, not ErrUploadConflict=%v)",
					err, errors.Is(err, ErrUploadConflict))
			}
			if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
				t.Fatalf("resume report = %+v, want resumed receipt %s", report, receipt.ID)
			}
			if res := report.Completed[0]; res.Key != receipt.Key {
				t.Fatalf("resume key = %q, want stable %q", res.Key, receipt.Key)
			}
			if objects.calls != 0 {
				t.Fatalf("Put calls during catalog-first resume = %d, want 0", objects.calls)
			}
			if counting.calls != 0 {
				t.Fatalf("AddFiles calls during catalog-first resume = %d, want 0", counting.calls)
			}
			got := f.allCatalogFiles(tenant.Default)
			if len(got) != 1 || got[0].Deleted() {
				t.Fatalf("catalog rows = %+v, want exactly the one live row", got)
			}
			if row := got[0]; row.Key != receipt.Key || row.Rows != 1 ||
				!row.CreatedAt.Equal(receipt.CreatedAt) || !row.MinTS.Equal(receipt.MinTS) || !row.MaxTS.Equal(receipt.MaxTS) {
				t.Fatalf("catalog row = %+v, want the seeded identity for receipt %+v", row, receipt)
			}
			if got := f.receipts(tenant.Default); len(got) != 0 {
				t.Fatalf("receipts = %+v, want none", got)
			}
			if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source not cleaned up: %v", err)
			}
			if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("upload temp not cleaned up: %v", err)
			}
			if rows := f.readObject(tenant.Default, receipt.Key); len(rows) != 1 || rows[0].line != "published" {
				t.Fatalf("published object rows = %+v, want the one source row exactly once", rows)
			}
		})
	}
}

func TestUploaderResumesLegacyNanosecondReceipt(t *testing.T) {
	// Version 1 manifests written before the reservation stamp was
	// canonicalized can hold a nanosecond CreatedAt. A catalog that keeps
	// microseconds (SQLite) still truncates its row, so identity comparison
	// must be canonical at micros or the upgrade leaves the receipt stuck.
	clock := time.Date(2026, 11, 3, 17, 0, 0, 123456789, time.UTC)
	hour := clock.UTC().Truncate(time.Hour)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, func(ops *localFileOps) {
				ops.now = func() time.Time { return clock }
			})
			ctx := t.Context()
			f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "legacy"}})

			broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
			if _, err := f.uploader(UploaderOptions{
				Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx); err == nil {
				t.Fatal("RunOnce with failing catalog succeeded")
			}
			receipt := f.onlyReceipt(tenant.Default)
			// Rewrite the durable receipt the way a pre-fix build left it.
			if err := f.tenantStore(tenant.Default).mutate(ctx, func(m *tenantManifest) error {
				i := findUpload(m.Uploads, receipt.ID)
				if i < 0 {
					return errors.New("receipt not in manifest")
				}
				m.Uploads[i].CreatedAt = clock
				return nil
			}); err != nil {
				t.Fatalf("write legacy manifest: %v", err)
			}
			legacy := f.onlyReceipt(tenant.Default)
			if legacy.CreatedAt.Nanosecond()%1000 == 0 {
				t.Fatalf("legacy CreatedAt = %s, want sub-microsecond nanos", legacy.CreatedAt)
			}
			// Crash after the pre-fix build added the catalog row, before
			// MarkPublished. SQLite keeps only the row's microseconds.
			if err := f.cat.AddFiles(ctx, uploadCatalogFile(legacy)); err != nil {
				t.Fatalf("seed catalog row: %v", err)
			}

			f.reopen()

			objects := &uploadCountingPut{Store: f.objects}
			counting := &uploadCountingCatalog{Catalog: f.cat}
			report, err := f.uploader(UploaderOptions{
				Catalog: counting, Objects: objects,
				MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx)
			if err != nil {
				t.Fatalf("resume legacy receipt: %v", err)
			}
			if report.Uploads != 1 || report.Resumed != 1 {
				t.Fatalf("resume report = %+v, want one resumed upload", report)
			}
			if res := report.Completed[0]; res.ID != legacy.ID || res.Key != legacy.Key {
				t.Fatalf("resume result = %+v, want stable id %s key %q", res, legacy.ID, legacy.Key)
			}
			if objects.calls != 0 || counting.calls != 0 {
				t.Fatalf("resume did %d Puts and %d AddFiles, want neither", objects.calls, counting.calls)
			}
			rows := f.allCatalogFiles(tenant.Default)
			if len(rows) != 1 || rows[0].Deleted() || rows[0].Key != legacy.Key {
				t.Fatalf("catalog rows = %+v, want the one live legacy row", rows)
			}
			if !sameCatalogTime(rows[0].CreatedAt, legacy.CreatedAt) {
				t.Fatalf("row CreatedAt = %s, want legacy receipt %s at catalog precision", rows[0].CreatedAt, legacy.CreatedAt)
			}
			if got := f.receipts(tenant.Default); len(got) != 0 {
				t.Fatalf("receipts = %+v, want none", got)
			}
			if rows := f.readObject(tenant.Default, legacy.Key); len(rows) != 1 || rows[0].line != "legacy" {
				t.Fatalf("published object rows = %+v, want the legacy row exactly once", rows)
			}
		})
	}
}

func TestUploadMatchesCatalogPrecision(t *testing.T) {
	base := time.Date(2026, 11, 3, 18, 0, 0, 123456789, time.UTC)
	up := Upload{
		Tenant: tenant.Default, Stream: "api", Key: "default/logs/api/x.parquet",
		MinTS: base, MaxTS: base.Add(time.Minute), Rows: 1, Bytes: 10,
		CreatedAt: base,
	}
	row := uploadCatalogFile(up)
	if !uploadMatchesCatalog(up, row) {
		t.Fatal("identical identity does not match")
	}
	tests := []struct {
		name   string
		mutate func(*catalog.File)
		want   bool
	}{
		{"micro-truncated row", func(f *catalog.File) {
			f.CreatedAt = f.CreatedAt.Truncate(time.Microsecond)
			f.MinTS = f.MinTS.Truncate(time.Microsecond)
			f.MaxTS = f.MaxTS.Truncate(time.Microsecond)
		}, true},
		{"different created microsecond", func(f *catalog.File) { f.CreatedAt = f.CreatedAt.Add(time.Microsecond) }, false},
		{"different min microsecond", func(f *catalog.File) { f.MinTS = f.MinTS.Add(time.Microsecond) }, false},
		{"different max microsecond", func(f *catalog.File) { f.MaxTS = f.MaxTS.Add(time.Microsecond) }, false},
		{"different rows", func(f *catalog.File) { f.Rows++ }, false},
		{"different bytes", func(f *catalog.File) { f.Bytes++ }, false},
		{"different stream", func(f *catalog.File) { f.Stream = "auth" }, false},
		{"different key", func(f *catalog.File) { f.Key = "default/logs/api/y.parquet" }, false},
		{"different labels", func(f *catalog.File) { f.LabelValues = map[string][]string{"level": {"info"}} }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := row.Clone()
			tt.mutate(&got)
			if uploadMatchesCatalog(up, got) != tt.want {
				t.Fatalf("uploadMatchesCatalog() = %v, want %v for %+v", !tt.want, tt.want, got)
			}
		})
	}
}

func TestUploaderPublishedPartialCleanupAfterReopen(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	hour := time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)
	removed := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "one"}})
	remaining := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "two"}})

	broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
	if _, err := f.uploader(UploaderOptions{Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(ctx); err == nil {
		t.Fatal("RunOnce with failing catalog succeeded")
	}
	receipt := f.onlyReceipt(tenant.Default)
	if len(receipt.Sources) != 2 {
		t.Fatalf("receipt sources = %v, want both seeded files", receipt.Sources)
	}
	if err := f.cat.AddFiles(ctx, uploadCatalogFile(receipt)); err != nil {
		t.Fatalf("seed catalog row: %v", err)
	}
	ts := f.tenantStore(tenant.Default)
	if err := ts.MarkPublished(ctx, receipt.ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	// Simulate a cleanup interrupted after one retired source was deleted.
	if err := os.Remove(f.localPath(tenant.Default, removed)); err != nil {
		t.Fatalf("remove one retired source: %v", err)
	}
	f.reopen()

	objects := &uploadCountingPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("cleanup RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("cleanup report = %+v, want resumed receipt %s", report, receipt.ID)
	}
	if objects.calls != 0 {
		t.Fatalf("Put calls during published cleanup = %d, want 0", objects.calls)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, remaining)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remaining retired source still on disk: %v", err)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upload temp still on disk: %v", err)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != receipt.Key || files[0].Rows != 2 {
		t.Fatalf("published row changed: %+v", files)
	}
	if rows := f.readObject(tenant.Default, receipt.Key); len(rows) != 2 {
		t.Fatalf("published object rows = %d, want both rows once", len(rows))
	}
}

func TestUploaderRemergesWhenTempIsMissing(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	hour := time.Date(2026, 11, 3, 18, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "one"},
		{ts: hour.Add(2 * time.Minute), line: "two"},
	})

	broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
	seedPuts := &uploadKeyListPut{Store: f.objects}
	if _, err := f.uploader(UploaderOptions{
		Objects: seedPuts, Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(ctx); err == nil {
		t.Fatal("RunOnce with failing catalog succeeded")
	}
	receipt := f.onlyReceipt(tenant.Default)
	oldKey := receipt.Key
	oldBytes := uploadObjectBytes(t, f, oldKey)
	if err := os.Remove(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("remove finalized temp: %v", err)
	}

	f.reopen()

	retryPuts := &uploadKeyListPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce after temp loss: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("remerge = %+v, want stable id %s", report, receipt.ID)
	}
	newKey := report.Completed[0].Key
	if newKey == oldKey {
		t.Fatalf("remerge rewrote the stored key %q", oldKey)
	}
	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 || files[0].Key != newKey || files[0].Rows != 2 {
		t.Fatalf("catalog after remerge = %+v, want 2 rows under %q", files, newKey)
	}
	if !files[0].HasSidecar || !files[0].LabelsComplete {
		t.Fatalf("remerged row = %+v, want a rebuilt sidecar and complete summary", files[0])
	}
	if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(newKey)); err != nil {
		t.Fatalf("sidecar missing after remerge: %v", err)
	}
	if got := uploadObjectBytes(t, f, oldKey); !bytes.Equal(got, oldBytes) {
		t.Fatal("remerge rewrote the old data object")
	}
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, slices.Concat(seedPuts.keys, retryPuts.keys))
	if len(dataKeys) != 2 || dataKeys[0] != oldKey || dataKeys[1] != newKey {
		t.Fatalf("data Put keys = %v, want old %q then new %q", dataKeys, oldKey, newKey)
	}
	if len(sidecarKeys) != 2 || sidecarKeys[0] != objstore.SidecarKey(oldKey) || sidecarKeys[1] != objstore.SidecarKey(newKey) {
		t.Fatalf("sidecar Put keys = %v, want one per data key", sidecarKeys)
	}
	rows := f.readObject(tenant.Default, newKey)
	if len(rows) != 2 || rows[0].line != "one" || rows[1].line != "two" {
		t.Fatalf("remerged object rows = %+v", rows)
	}
}

func TestUploaderTombstoneIsPublicationProof(t *testing.T) {
	// A tombstoned row is publication evidence only if its stable identity
	// still equals the receipt's, including CreatedAt across a catalog round
	// trip that keeps microseconds.
	clock := time.Date(2026, 11, 3, 19, 0, 0, 987654321, time.UTC)
	hour := clock.UTC().Truncate(time.Hour)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, func(ops *localFileOps) {
				ops.now = func() time.Time { return clock }
			})
			ctx := t.Context()
			source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "compacted"}})

			broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
			if _, err := f.uploader(UploaderOptions{
				Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx); err == nil {
				t.Fatal("RunOnce with failing catalog succeeded")
			}
			receipt := f.onlyReceipt(tenant.Default)
			file := uploadCatalogFile(receipt)
			if err := f.cat.AddFiles(ctx, file); err != nil {
				t.Fatalf("seed catalog row: %v", err)
			}
			if err := f.cat.ReplaceFiles(ctx, tenant.Default, []string{file.Key}, nil); err != nil {
				t.Fatalf("tombstone catalog row: %v", err)
			}
			if err := f.objects.Delete(ctx, receipt.Key); err != nil {
				t.Fatalf("delete object: %v", err)
			}

			f.reopen()

			// No Put, no AddFiles: the tombstone alone retires the sources.
			objects := &uploadCountingPut{Store: f.objects}
			report, err := f.uploader(UploaderOptions{
				Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx)
			if err != nil {
				t.Fatalf("RunOnce with tombstone: %v", err)
			}
			if report.Uploads != 1 || objects.calls != 0 || report.Completed[0].ID != receipt.ID {
				t.Fatalf("resume = %+v with %d Puts, want receipt %s and no Put", report, objects.calls, receipt.ID)
			}
			rows := f.allCatalogFiles(tenant.Default)
			if len(rows) != 1 || !rows[0].Deleted() {
				t.Fatalf("catalog rows = %+v, want the tombstone untouched", rows)
			}
			if got := f.receipts(tenant.Default); len(got) != 0 {
				t.Fatalf("receipts = %+v, want none", got)
			}
			if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source not retired: %v", err)
			}
			if _, err := f.objects.Stat(ctx, receipt.Key); !errors.Is(err, objstore.ErrNotExist) {
				t.Fatalf("object was resurrected: %v", err)
			}
		})
	}
}

func TestUploaderConflictLeavesSourcesUntouched(t *testing.T) {
	// A different row under the key is a hard conflict in every catalog, and
	// the identity check must not depend on the clock's sub-microsecond bits.
	clock := time.Date(2026, 11, 3, 20, 0, 0, 456789123, time.UTC)
	hour := clock.UTC().Truncate(time.Hour)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, func(ops *localFileOps) {
				ops.now = func() time.Time { return clock }
			})
			ctx := t.Context()
			source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "conflict"}})

			broken := &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")}
			if _, err := f.uploader(UploaderOptions{
				Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx); err == nil {
				t.Fatal("RunOnce with failing catalog succeeded")
			}
			receipt := f.onlyReceipt(tenant.Default)
			conflicting := uploadCatalogFile(receipt)
			conflicting.Stream = "auth" // a foreign row under the same key must still be found
			if err := f.cat.AddFiles(ctx, conflicting); err != nil {
				t.Fatalf("seed conflicting row: %v", err)
			}

			objects := &uploadCountingPut{Store: f.objects}
			_, err := f.uploader(UploaderOptions{
				Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
				Now: func() time.Time { return clock },
			}).RunOnce(ctx)
			if !errors.Is(err, ErrUploadConflict) {
				t.Fatalf("RunOnce error = %v, want ErrUploadConflict", err)
			}
			if objects.calls != 0 {
				t.Fatalf("Put calls with a conflict = %d, want 0", objects.calls)
			}
			rows := f.allCatalogFiles(tenant.Default)
			if len(rows) != 1 || rows[0].Stream != "auth" {
				t.Fatalf("conflicting row changed: %+v", rows)
			}
			got := f.onlyReceipt(tenant.Default)
			if got.Stage != UploadStored {
				t.Fatalf("receipt stage after conflict = %q, want stored", got.Stage)
			}
			if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
				t.Fatalf("source lost on conflict: %v", err)
			}
		})
	}
}

func TestUploaderMergeFailureAbortsTemp(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	hour := time.Date(2026, 11, 3, 21, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "corrupt"}})
	path := f.localPath(tenant.Default, source)
	if err := os.Truncate(path, 10); err != nil {
		t.Fatalf("truncate source: %v", err)
	}

	_, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(ctx)
	if err == nil {
		t.Fatal("RunOnce merged a corrupt source")
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadReserved {
		t.Fatalf("receipt stage = %q, want reserved", receipt.Stage)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
		t.Fatalf("catalog rows after merge failure = %d, want 0", len(got))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("source removed by failed merge: %v", err)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized temp left behind after merge failure: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(f.dataDir, "parquet", string(tenant.Default), ".uploads", "*.tmp"))
	if err != nil {
		t.Fatalf("glob temps: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("partial temps left behind: %v", temps)
	}
}

func TestUploaderFailedFinalizeClosesAndQuarantines(t *testing.T) {
	f := uploadNewFixtureWithOps(t, func(ops *localFileOps) {
		create := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			file, err := create(root, name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasPrefix(name, ".uploads/") && strings.HasSuffix(name, ".parquet.tmp") {
				return uploadCloseFailFile{syncFile: file, err: errors.New("injected close failure")}, nil
			}
			return file, nil
		}
	})
	hour := time.Date(2026, 11, 3, 21, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "finalize"}})

	_, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err == nil || !strings.Contains(err.Error(), "injected close failure") {
		t.Fatalf("RunOnce error = %v, want the injected close failure", err)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadReserved {
		t.Fatalf("receipt stage = %q, want reserved", receipt.Stage)
	}
	if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows after failed finalize = %d, want 0", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source lost on failed finalize: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(f.dataDir, "parquet", string(tenant.Default), ".uploads", "*"))
	if err != nil {
		t.Fatalf("glob uploads: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("failed finalize left upload artifacts: %v", temps)
	}
}

func TestUploaderFinalizeRenameSyncFailureIsQuarantined(t *testing.T) {
	f := uploadNewFixtureWithOps(t, func(ops *localFileOps) {
		syncDir := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if name == ".uploads" {
				return errors.New("injected directory sync failure")
			}
			return syncDir(root, name)
		}
	})
	hour := time.Date(2026, 11, 3, 22, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "renamed"}})

	_, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err == nil || !strings.Contains(err.Error(), "injected directory sync failure") {
		t.Fatalf("RunOnce error = %v, want the injected directory sync failure", err)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadReserved {
		t.Fatalf("receipt stage = %q, want reserved", receipt.Stage)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renamed temp survived the failed finalize: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(f.dataDir, "parquet", string(tenant.Default), ".uploads", "*"))
	if err != nil {
		t.Fatalf("glob temps: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("abort left upload artifacts: %v", temps)
	}
}

func TestUploaderCanceledMergeReleasesTemp(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 23, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "canceled-merge"}})

	u := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20})
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	u.merge = func(mergeCtx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		calls++
		if w == nil || len(inputs) != 1 {
			t.Errorf("merge seam got w=%v inputs=%d, want one open reader", w, len(inputs))
		}
		// Cancel the real pass context; cleanup must not ride on it.
		cancel()
		return storage.FileStats{}, mergeCtx.Err()
	}
	_, err := u.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("merge seam calls = %d, want 1", calls)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadReserved {
		t.Fatalf("receipt stage = %q, want reserved", receipt.Stage)
	}
	if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows after canceled merge = %d, want 0", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source lost on canceled merge: %v", err)
	}
	if _, err := os.Stat(f.tempPath(tenant.Default, receipt.ID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized temp left after canceled merge: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(f.dataDir, "parquet", string(tenant.Default), ".uploads", "*.tmp"))
	if err != nil {
		t.Fatalf("glob temps: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("partial temps left after canceled merge: %v", temps)
	}

	// Restoring the real merge recovers the same receipt.
	u.merge = storage.Merge
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("retry = %+v, want resumed receipt %s", report, receipt.ID)
	}
}

func TestUploaderCanceledPassKeepsDurableState(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 22, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "cancel"}})

	ctx, cancel := context.WithCancel(t.Context())
	objects := &uploadCancelPut{Store: f.objects, cancel: cancel}
	_, err := f.uploader(UploaderOptions{
		Objects: objects, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context.Canceled", err)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadMerged {
		t.Fatalf("receipt stage = %q, want merged", receipt.Stage)
	}
	if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
		t.Fatalf("catalog rows after cancellation = %d, want 0", got)
	}
	if _, err := f.objects.Stat(t.Context(), receipt.Key); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("object written after canceled Put: %v", err)
	}

	report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("retry = %+v, want resumed receipt %s", report, receipt.ID)
	}
}

func TestUploaderUnknownStageIsClassified(t *testing.T) {
	f := uploadNewFixture(t)
	u := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1024})
	_, err := u.processUpload(t.Context(), nil, Upload{Stage: UploadStage("bogus")})
	if !errors.Is(err, ErrUploadStage) {
		t.Fatalf("processUpload error = %v, want ErrUploadStage", err)
	}
}

func TestUploaderDueAndSelection(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	u := &Uploader{minBytes: 100, maxAge: time.Hour, mergeMax: 250}
	files := func(ages ...time.Duration) []LocalFile {
		out := make([]LocalFile, len(ages))
		for i, age := range ages {
			out[i] = LocalFile{Bytes: 10, PublishedAt: now.Add(-age)}
		}
		return out
	}
	tests := []struct {
		name  string
		files []LocalFile
		want  bool
	}{
		{"empty", nil, false},
		{"below both", files(time.Minute, 2*time.Minute), false},
		{"bytes boundary", func() []LocalFile {
			fs := files(time.Minute)
			fs[0].Bytes = 100
			return fs
		}(), true},
		{"age boundary", files(time.Hour), true},
		{"age below", files(time.Hour - time.Second), false},
		{"zero publication time", func() []LocalFile {
			fs := files(0)
			fs[0].PublishedAt = time.Time{}
			return fs
		}(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := u.due(tt.files, now); got != tt.want {
				t.Fatalf("due() = %v, want %v", got, tt.want)
			}
		})
	}

	sel := []LocalFile{{Bytes: 100}, {Bytes: 100}, {Bytes: 100}}
	if got := u.selectSources(sel); len(got) != 2 || got[0].Bytes != 100 || got[1].Bytes != 100 {
		t.Fatalf("selectSources(cap) = %+v, want two 100-byte files", got)
	}
	if got := u.selectSources(sel[2:]); len(got) != 1 {
		t.Fatalf("selectSources(oversize) = %+v, want the singleton", got)
	}
	if got := u.selectSources(sel[:2]); len(got) != 2 {
		t.Fatalf("selectSources(fit) = %+v, want both", got)
	}

	// Byte thresholds and caps must not wrap around int64.
	huge := []LocalFile{{Bytes: math.MaxInt64}, {Bytes: math.MaxInt64}}
	if !u.due(huge, now) {
		t.Fatal("due(MaxInt64 bytes) = false, want true")
	}
	if got := u.selectSources(huge); len(got) != 1 {
		t.Fatalf("selectSources(MaxInt64) = %+v, want the first file only", got)
	}
}

func TestUploaderGroupPendingFiles(t *testing.T) {
	hour := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	file := func(stream string, hour time.Time) LocalFile {
		return LocalFile{Tenant: tenant.Default, ID: uuid.NewV7(), Stream: stream, Hour: hour, Bytes: 1}
	}
	busy := file("api", hour)
	live := file("api", hour)
	otherHour := file("api", hour.Add(time.Hour))
	otherStream := file("auth", hour)

	groups := groupPendingFiles(
		[]LocalFile{live, busy, otherHour, otherStream},
		map[uuid.UUID]bool{busy.ID: true},
	)
	if len(groups) != 3 {
		t.Fatalf("groups = %d, want 3: %+v", len(groups), groups)
	}
	if got := groups[uploadGroupKey{stream: "api", hour: hour}]; len(got) != 1 || got[0].ID != live.ID {
		t.Fatalf("api/hour group = %+v, want only the live file", got)
	}
	if got := groups[uploadGroupKey{stream: "api", hour: hour.Add(time.Hour)}]; len(got) != 1 || got[0].ID != otherHour.ID {
		t.Fatalf("api/next-hour group = %+v", got)
	}
	if got := groups[uploadGroupKey{stream: "auth", hour: hour}]; len(got) != 1 || got[0].ID != otherStream.ID {
		t.Fatalf("auth hourly group = %+v", got)
	}

	keys := slices.SortedFunc(maps.Keys(groups), compareGroupKeys)
	if len(keys) != 3 || keys[0].stream != "api" || keys[2].stream != "auth" {
		t.Fatalf("group order = %+v, want api before auth", keys)
	}
}

// microAligned reports whether t carries no sub-microsecond component, the
// precision Parquet timestamps and the catalog's integer microseconds keep.
func microAligned(t time.Time) bool { return t.Nanosecond()%1000 == 0 }

// mapsEqual compares two label-value maps.
func mapsEqual(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !slices.Equal(av, bv) {
			return false
		}
	}
	return true
}

// mustPending returns the tenant's pending files.
func mustPending(t *testing.T, ts *TenantStore) []LocalFile {
	t.Helper()
	files, err := ts.PendingFiles(t.Context())
	if err != nil {
		t.Fatalf("PendingFiles: %v", err)
	}
	return files
}
