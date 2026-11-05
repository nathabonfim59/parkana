package ingest

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// --- helpers ---------------------------------------------------------------

// fakeGen satisfies SealedGeneration with prebuilt parts.
type fakeGen struct {
	id       tenant.ID
	first    uint64
	last     uint64
	parts    []Part
	partsErr error
}

func (g *fakeGen) Tenant() tenant.ID          { return g.id }
func (g *fakeGen) Frontier() (uint64, uint64) { return g.first, g.last }
func (g *fakeGen) Parts(ctx context.Context) ([]Part, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return g.parts, g.partsErr
}

func testFlusher(t *testing.T, s *LocalStore, reclaim ReclaimFunc) *Flusher {
	t.Helper()
	f, err := NewFlusher(FlushOptions{
		Store:       s,
		Reclaim:     reclaim,
		PromotedFor: func(stream string) []string { return []string{"level"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// genWith builds a fake generation whose parts hold real records; the caller
// releases them via t.Cleanup.
func genWith(t *testing.T, id tenant.ID, first, last uint64, parts []Part) *fakeGen {
	t.Helper()
	for _, p := range parts {
		p := p
		t.Cleanup(p.Record.Release)
	}
	return &fakeGen{id: id, first: first, last: last, parts: parts}
}

func partFor(t *testing.T, stream string, hour time.Time, rows []rowSpec) Part {
	t.Helper()
	return Part{Stream: stream, Hour: hour, Record: buildRecord(t, []string{"level"}, rows)}
}

// recordingReclaim captures calls.
type reclaimLog struct {
	calls []wal.Position
	err   error
}

func (r *reclaimLog) reclaim(ctx context.Context, id tenant.ID, pos wal.Position) error {
	r.calls = append(r.calls, pos)
	return r.err
}

// --- flush ------------------------------------------------------------------

func TestFlushPublishesGeneration(t *testing.T) {
	s, _ := testStore(t, nil)
	rl := &reclaimLog{}
	fl := testFlusher(t, s, rl.reclaim)
	hour2 := testHour.Add(time.Hour)
	gen := genWith(t, testTenant, 1, 4, []Part{
		partFor(t, "checkout", testHour, []rowSpec{
			{us: us(testHour, 0), line: "a", promoted: map[string]string{"level": "info"}},
			{us: us(testHour, time.Second), line: "b", promoted: map[string]string{"level": "warn"}},
		}),
		partFor(t, "checkout", hour2, []rowSpec{
			{us: us(hour2, 0), line: "c"},
		}),
		partFor(t, "api", testHour, []rowSpec{
			{us: us(testHour, time.Minute), line: "d"},
		}),
	})
	if err := fl.Flush(t.Context(), gen); err != nil {
		t.Fatal(err)
	}
	ts := testTenantStore(t, s)
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{First: 1, Last: 4}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("files = %+v", files)
	}
	// Sorted by stream, hour, id; each within its UTC hour.
	want := []struct {
		stream string
		hour   time.Time
		rows   int64
	}{
		{"api", testHour, 1},
		{"checkout", testHour, 2},
		{"checkout", hour2, 1},
	}
	for i, f := range files {
		if f.Stream != want[i].stream || !f.Hour.Equal(want[i].hour) || f.Rows != want[i].rows {
			t.Fatalf("file %d = %+v, want %+v", i, f, want[i])
		}
		if f.MinTS.Before(f.Hour) || !f.MaxTS.Before(f.Hour.Add(time.Hour)) {
			t.Fatalf("file %s stats outside hour", f.ID)
		}
		if f.Bytes <= 0 || f.PublishedAt.IsZero() {
			t.Fatalf("file %s missing publication metadata", f.ID)
		}
	}
	rows, tsVals := readRows(t, ts, files[1])
	if rows != 2 || tsVals[0] != us(testHour, 0) {
		t.Fatalf("roundtrip rows=%d ts=%v", rows, tsVals)
	}
	if len(rl.calls) != 1 || rl.calls[0] != (wal.Position{Tenant: testTenant, Seq: 4}) {
		t.Fatalf("reclaim calls = %+v", rl.calls)
	}
}

func TestFlushCoveredIsNoop(t *testing.T) {
	s, _ := testStore(t, nil)
	fl := testFlusher(t, s, nil)
	mk := func() *fakeGen {
		return genWith(t, testTenant, 1, 3, []Part{
			partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
		})
	}
	if err := fl.Flush(t.Context(), mk()); err != nil {
		t.Fatal(err)
	}
	// A second publication of the covered generation must not write files or
	// duplicate rows.
	dup := mk()
	dup.partsErr = errors.New("Parts must not be called for a covered generation")
	if err := fl.Flush(t.Context(), dup); err != nil {
		t.Fatalf("covered flush = %v", err)
	}
	ts := testTenantStore(t, s)
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %+v", files)
	}
}

func TestFlushGapOverlapRejected(t *testing.T) {
	s, _ := testStore(t, nil)
	fl := testFlusher(t, s, nil)
	g1 := genWith(t, testTenant, 1, 3, []Part{
		partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
	})
	if err := fl.Flush(t.Context(), g1); err != nil {
		t.Fatal(err)
	}
	gap := genWith(t, testTenant, 6, 9, nil)
	if err := fl.Flush(t.Context(), gap); !errors.Is(err, ErrGenerationGap) {
		t.Fatalf("gap = %v", err)
	}
	over := genWith(t, testTenant, 3, 9, nil)
	if err := fl.Flush(t.Context(), over); !errors.Is(err, ErrGenerationOverlap) {
		t.Fatalf("overlap = %v", err)
	}
	bad := genWith(t, testTenant, 5, 2, nil)
	if err := fl.Flush(t.Context(), bad); err == nil {
		t.Fatal("inverted frontier accepted")
	}
	// No files were written for the rejected generations.
	ts := testTenantStore(t, s)
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %+v", files)
	}
}

func TestFlushFileSyncFailureRetries(t *testing.T) {
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
	fl := testFlusher(t, s, nil)
	mk := func() *fakeGen {
		return genWith(t, testTenant, 1, 2, []Part{
			partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
		})
	}
	armed = true
	if err := fl.Flush(t.Context(), mk()); !errors.Is(err, errInjected) {
		t.Fatalf("flush = %v", err)
	}
	armed = false
	if err := fl.Flush(t.Context(), mk()); err != nil {
		t.Fatalf("retry = %v", err)
	}
	ts := testTenantStore(t, s)
	fr, _ := ts.Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 2}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, _ := ts.Files(t.Context())
	if len(files) != 1 {
		t.Fatalf("files = %+v", files)
	}
}

// gatedSyncFile fails Sync while armed.
type gatedSyncFile struct {
	syncFile
	armed *bool
}

func (f *gatedSyncFile) Sync() error {
	if *f.armed {
		return errInjected
	}
	return f.syncFile.Sync()
}

func TestFlushReclaimFailureKeepsPublication(t *testing.T) {
	s, _ := testStore(t, nil)
	rl := &reclaimLog{err: errInjected}
	fl := testFlusher(t, s, rl.reclaim)
	gen := genWith(t, testTenant, 1, 2, []Part{
		partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
	})
	if err := fl.Flush(t.Context(), gen); !errors.Is(err, errInjected) {
		t.Fatalf("flush = %v", err)
	}
	// Publication is durable despite the failed reclaim.
	ts := testTenantStore(t, s)
	fr, _ := ts.Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 2}) {
		t.Fatalf("frontier = %+v", fr)
	}
	// A covered retry still surfaces the failed reclaim at the committed
	// frontier rather than reporting success.
	gen2 := genWith(t, testTenant, 1, 2, []Part{
		partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
	})
	gen2.partsErr = errors.New("must not materialize")
	if err := fl.Flush(t.Context(), gen2); !errors.Is(err, errInjected) {
		t.Fatalf("covered retry = %v", err)
	}
	files, _ := ts.Files(t.Context())
	if len(files) != 1 {
		t.Fatalf("files = %+v", files)
	}
	// Once reclaim works, the covered retry returns nil.
	rl.err = nil
	if err := fl.Flush(t.Context(), gen2); err != nil {
		t.Fatalf("retry = %v", err)
	}
	if len(rl.calls) != 3 || rl.calls[2].Seq != 2 {
		t.Fatalf("reclaim calls = %+v", rl.calls)
	}
	// ReclaimAll also covers it on restart.
	if err := fl.ReclaimAll(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFlushEmptyGeneration(t *testing.T) {
	s, _ := testStore(t, nil)
	rl := &reclaimLog{}
	fl := testFlusher(t, s, rl.reclaim)
	gen := &fakeGen{id: testTenant, first: 1, last: 9}
	if err := fl.Flush(t.Context(), gen); err != nil {
		t.Fatal(err)
	}
	fr, _ := testTenantStore(t, s).Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 9}) {
		t.Fatalf("frontier = %+v", fr)
	}
	if len(rl.calls) != 1 {
		t.Fatalf("reclaim calls = %+v", rl.calls)
	}
}

func TestFlushWriterFactoryFailure(t *testing.T) {
	s, _ := testStore(t, nil)
	fl, err := NewFlusher(FlushOptions{
		Store: s,
		NewWriter: func(w io.Writer, sc *arrow.Schema, promoted []string) (ParquetWriter, error) {
			return nil, errInjected
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gen := genWith(t, testTenant, 1, 1, []Part{
		partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
	})
	if err := fl.Flush(t.Context(), gen); !errors.Is(err, errInjected) {
		t.Fatalf("flush = %v", err)
	}
	fr, _ := testTenantStore(t, s).Frontier(t.Context())
	if fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

func TestFlushPartsError(t *testing.T) {
	s, _ := testStore(t, nil)
	fl := testFlusher(t, s, nil)
	gen := &fakeGen{id: testTenant, first: 1, last: 1, partsErr: errInjected}
	if err := fl.Flush(t.Context(), gen); !errors.Is(err, errInjected) {
		t.Fatalf("flush = %v", err)
	}
}

func TestFlushCancellation(t *testing.T) {
	s, _ := testStore(t, nil)
	fl := testFlusher(t, s, nil)
	gen := genWith(t, testTenant, 1, 2, []Part{
		partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
	})
	ctx := &cancelAfter{Context: t.Context(), at: 3}
	if err := fl.Flush(ctx, gen); err == nil {
		t.Fatal("canceled flush succeeded")
	}
	fr, _ := testTenantStore(t, s).Frontier(t.Context())
	if fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

// TestFlushRealGeneration drives a real memtable generation through flush.
func TestFlushRealGeneration(t *testing.T) {
	s, _ := testStore(t, nil)
	rl := &reclaimLog{}
	fl := testFlusher(t, s, rl.reclaim)
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	m, err := NewMemtable(MemtableOptions{
		Allocator:    ca,
		StreamLabels: []string{"service_name"},
		PromotedFor:  func(string) []string { return []string{"level"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	req := &logs.PushRequest{Streams: []logs.Stream{
		{Labels: logs.New(
			logs.Label{Name: "level", Value: "info"},
			logs.Label{Name: "service_name", Value: "Checkout"},
		), Entries: []logs.Entry{
			{Timestamp: testHour.Add(3 * time.Second), Line: "l2"},
			{Timestamp: testHour.Add(time.Second), Line: "l1"},
			{Timestamp: testHour.Add(time.Hour + time.Second), Line: "next hour"},
		}},
		{Labels: logs.New(
			logs.Label{Name: "service_name", Value: "api"},
		), Entries: []logs.Entry{
			{Timestamp: testHour.Add(2 * time.Second), Line: "api line", Metadata: logs.New(logs.Label{Name: "trace", Value: "t1"})},
		}},
	}}
	if err := m.Append(t.Context(), testTenant, 1, 100, req); err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(t.Context(), testTenant); err != nil {
		t.Fatal(err)
	}
	gens, err := m.TakeSealed(t.Context(), testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 {
		t.Fatalf("gens = %d", len(gens))
	}
	gen := gens[0]
	defer gen.Release()
	if err := fl.Flush(t.Context(), gen); err != nil {
		t.Fatal(err)
	}
	ts := testTenantStore(t, s)
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("files = %+v", files)
	}
	var total int64
	for _, f := range files {
		n, _ := readRows(t, ts, f)
		total += n
		if f.MinTS.Before(f.Hour) || !f.MaxTS.Before(f.Hour.Add(time.Hour)) {
			t.Fatalf("file %s outside hour", f.ID)
		}
	}
	if total != 4 {
		t.Fatalf("rows = %d, want 4", total)
	}
	// The "Checkout" stream sanitized to "checkout"; check a checkout file's
	// rows are _ts-sorted via the reader output.
	for _, f := range files {
		if f.Stream != "checkout" || !f.Hour.Equal(testHour) {
			continue
		}
		_, tsVals := readRows(t, ts, f)
		if !slices.IsSorted(tsVals) {
			t.Fatalf("unsorted file rows: %v", tsVals)
		}
		if f.LabelValues["level"][0] != "info" {
			t.Fatalf("label values = %v", f.LabelValues)
		}
	}
	if len(rl.calls) != 1 || rl.calls[0] != (wal.Position{Tenant: testTenant, Seq: 1}) {
		t.Fatalf("reclaim = %+v", rl.calls)
	}
	// Flush borrowed the part records; the generation still owns them until
	// released.
	gen.Release()
	ca.AssertSize(t, 0)
}

// --- restart boundaries ------------------------------------------------------

// TestFlushRestartExactOnce publishes gen1, "crashes" (reopens the store),
// republishes gen1 (covered) and gen2, and verifies the exact file set.
func TestFlushRestartExactOnce(t *testing.T) {
	s, dir := testStore(t, nil)
	fl := testFlusher(t, s, nil)
	mkGen := func(first, last uint64, line string) *fakeGen {
		return genWith(t, testTenant, first, last, []Part{
			partFor(t, "checkout", testHour, []rowSpec{{us: us(testHour, 0), line: line}}),
		})
	}
	if err := fl.Flush(t.Context(), mkGen(1, 5, "a")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	fl2 := testFlusher(t, s2, nil)
	// Republish the covered generation: idempotent, no second file.
	if err := fl2.Flush(t.Context(), mkGen(1, 5, "a")); err != nil {
		t.Fatal(err)
	}
	if err := fl2.Flush(t.Context(), mkGen(6, 9, "b")); err != nil {
		t.Fatal(err)
	}
	ts := testTenantStore(t, s2)
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
	var ids []uuid.UUID
	for _, f := range files {
		ids = append(ids, f.ID)
	}
	slices.SortFunc(ids, uuid.UUID.Compare)
	if ids[0] == ids[1] {
		t.Fatal("duplicate file id")
	}
	fr, _ := ts.Frontier(t.Context())
	if fr != (Frontier{First: 6, Last: 9}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

// TestFlushPartialPublicationFault injects a write failure on the second
// part's file: the frontier and reclaim cannot advance past a partially
// written generation, and the orphan is quarantined on reopen.
func TestFlushPartialPublicationFault(t *testing.T) {
	failSecond := true
	var creates int
	s, dir := testStore(t, func(ops *localFileOps) {
		def := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			creates++
			if failSecond && creates == 2 && strings.HasSuffix(name, tempSuffix) {
				return nil, errInjected
			}
			return def(root, name, flag, perm)
		}
	})
	rl := &reclaimLog{}
	// The injected fault expects the first serial part to be renamed before
	// the second part's creation fails, so pin this fixture to serial
	// publication.
	fl, err := NewFlusher(FlushOptions{
		Store:       s,
		Reclaim:     rl.reclaim,
		PromotedFor: func(string) []string { return []string{"level"} },
		Concurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hour2 := testHour.Add(time.Hour)
	mk := func() *fakeGen {
		return genWith(t, testTenant, 1, 3, []Part{
			partFor(t, "a_stream", testHour, []rowSpec{{us: us(testHour, 0), line: "a"}}),
			partFor(t, "b_stream", hour2, []rowSpec{{us: us(hour2, 0), line: "b"}}),
		})
	}
	if err := fl.Flush(t.Context(), mk()); !errors.Is(err, errInjected) {
		t.Fatalf("flush = %v", err)
	}
	ts := testTenantStore(t, s)
	fr, _ := ts.Frontier(t.Context())
	if fr != (Frontier{}) {
		t.Fatalf("frontier crossed partial publication: %+v", fr)
	}
	if len(rl.calls) != 0 {
		t.Fatalf("reclaim ran on partial publication: %+v", rl.calls)
	}
	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	ts2 := testTenantStore(t, s2)
	files, _ := ts2.Files(t.Context())
	if len(files) != 0 {
		t.Fatalf("uncommitted part listed: %+v", files)
	}
	q, err := fs.ReadDir(ts2.root.FS(), quarantineDir)
	if err != nil || len(q) != 1 {
		t.Fatalf("quarantine = %v, %v", q, err)
	}
	// Retry publishes the whole generation cleanly.
	fl2 := testFlusher(t, s2, rl.reclaim)
	if err := fl2.Flush(t.Context(), mk()); err != nil {
		t.Fatal(err)
	}
	fr, _ = ts2.Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 3}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, _ = ts2.Files(t.Context())
	if len(files) != 2 {
		t.Fatalf("files = %+v", files)
	}
	if len(rl.calls) != 1 || rl.calls[0].Seq != 3 {
		t.Fatalf("reclaim = %+v", rl.calls)
	}
}
