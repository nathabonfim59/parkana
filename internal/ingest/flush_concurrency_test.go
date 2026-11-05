package ingest

// Concurrency tests for the flush half of local publication. They prove that
// independent parts of one generation write in parallel within a bound, that
// no failure or cancellation can publish a partial generation, that every
// worker is joined before Flush returns (the parts borrow generation-owned
// Arrow memory), and that retries and sync failures keep exact file/row
// counts. No sleeps are used to observe overlap: writers meet at explicit
// gates.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var (
	errFatal     = errors.New("errFatal")
	errTransient = errors.New("errTransient")
)

// --- helpers ---------------------------------------------------------------

// testConcFlusher builds a flusher with an explicit part concurrency.
func testConcFlusher(t *testing.T, s *LocalStore, concurrency int, reclaim ReclaimFunc, nf WriterFactory) *Flusher {
	t.Helper()
	f, err := NewFlusher(FlushOptions{
		Store:       s,
		NewWriter:   nf,
		Reclaim:     reclaim,
		PromotedFor: func(string) []string { return []string{"level"} },
		Concurrency: concurrency,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// concParts builds one part per stream, each in its own hour with two rows.
func concParts(t *testing.T, streams []string) []Part {
	t.Helper()
	parts := make([]Part, len(streams))
	for i, stream := range streams {
		hour := testHour.Add(time.Duration(i) * time.Hour)
		parts[i] = partFor(t, stream, hour, []rowSpec{
			{us: us(hour, 0), line: stream + "-a", promoted: map[string]string{"level": "info"}},
			{us: us(hour, time.Second), line: stream + "-b"},
		})
	}
	return parts
}

// writeGate blocks every writer inside Write until the gate is opened and
// tracks the peak number of concurrent writers.
type writeGate struct {
	limit  int
	active atomic.Int32
	peak   atomic.Int32
	full   chan struct{}
	open   chan struct{}

	fullOnce sync.Once
	openOnce sync.Once
}

func newWriteGate(limit int) *writeGate {
	return &writeGate{limit: limit, full: make(chan struct{}), open: make(chan struct{})}
}

// enter counts this writer as active, closes full once limit writers overlap,
// and blocks until the gate is opened.
func (g *writeGate) enter() {
	active := g.active.Add(1)
	for {
		cur := g.peak.Load()
		if active <= cur || g.peak.CompareAndSwap(cur, active) {
			break
		}
	}
	if int(active) >= g.limit {
		g.fullOnce.Do(func() { close(g.full) })
	}
	<-g.open
	g.active.Add(-1)
}

// release lets every blocked writer proceed.
func (g *writeGate) release() {
	g.openOnce.Do(func() { close(g.open) })
}

// waitFull blocks until limit writers were inside Write at the same time.
func (g *writeGate) waitFull(t *testing.T) {
	t.Helper()
	select {
	case <-g.full:
	case <-time.After(10 * time.Second):
		t.Fatal("writers did not overlap")
	}
}

// gatedWriter blocks in Write on a writeGate, then delegates to the real
// Parquet writer.
type gatedWriter struct {
	inner ParquetWriter
	gate  *writeGate
}

func (w *gatedWriter) Write(rec arrow.RecordBatch) error {
	w.gate.enter()
	return w.inner.Write(rec)
}

func (w *gatedWriter) Close() (storage.FileStats, error) { return w.inner.Close() }

// gateFactory wraps the production writer with a shared gate.
func gateFactory(g *writeGate) WriterFactory {
	return func(w io.Writer, sc *arrow.Schema, promoted []string) (ParquetWriter, error) {
		inner, err := defaultWriterFactory(w, sc, promoted)
		if err != nil {
			return nil, err
		}
		return &gatedWriter{inner: inner, gate: g}, nil
	}
}

// hookWriter is a ParquetWriter whose Write/Close are test closures.
type hookWriter struct {
	write func(arrow.RecordBatch) error
	close func() (storage.FileStats, error)
}

func (w *hookWriter) Write(rec arrow.RecordBatch) error { return w.write(rec) }

func (w *hookWriter) Close() (storage.FileStats, error) { return w.close() }

// rowLine returns the line column of row 0 of a part record.
func rowLine(rec arrow.RecordBatch) string {
	return rec.Column(1).(*array.String).Value(0)
}

// fileShapes renders the manifest's file set in a comparable, id-agnostic
// form (uuid order within a stream/hour may legitimately differ).
func fileShapes(t *testing.T, ts *TenantStore) []string {
	t.Helper()
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = fmt.Sprintf("%s|%s|%d|%d|%d|%v",
			f.Stream, f.Hour.Format(time.RFC3339), f.Rows,
			f.MinTS.UnixMicro(), f.MaxTS.UnixMicro(), f.LabelValues)
	}
	slices.Sort(out)
	return out
}

// --- option validation -----------------------------------------------------

func TestNewFlusherConcurrencyValidation(t *testing.T) {
	s, _ := testStore(t, nil)
	tests := []struct {
		name    string
		in      int
		want    int
		wantErr bool
	}{
		{name: "zero defaults", in: 0, want: defaultFlushConcurrency},
		{name: "one", in: 1, want: 1},
		{name: "max", in: maxFlushConcurrency, want: maxFlushConcurrency},
		{name: "negative", in: -1, wantErr: true},
		{name: "above max", in: maxFlushConcurrency + 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFlusher(FlushOptions{Store: s, Concurrency: tt.in})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Concurrency %d accepted", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if f.concurrency != tt.want {
				t.Fatalf("concurrency = %d, want %d", f.concurrency, tt.want)
			}
		})
	}
}

// --- overlap and bounds ----------------------------------------------------

// TestFlushConcurrencyOverlapsParts proves independent parts are written at
// the same time: two writers must be inside Write before either is released.
func TestFlushConcurrencyOverlapsParts(t *testing.T) {
	s, _ := testStore(t, nil)
	gate := newWriteGate(2)
	t.Cleanup(gate.release)
	fl := testConcFlusher(t, s, 4, nil, gateFactory(gate))
	gen := genWith(t, testTenant, 1, 4, concParts(t, []string{"part0", "part1", "part2", "part3"}))

	done := make(chan error, 1)
	go func() { done <- fl.Flush(t.Context(), gen) }()
	gate.waitFull(t)
	gate.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flush did not finish after writers were released")
	}
	if got := gate.peak.Load(); got < 2 || got > 4 {
		t.Fatalf("peak concurrent writers = %d, want 2..4", got)
	}
	files, err := testTenantStore(t, s).Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 4 {
		t.Fatalf("files = %d, want 4", len(files))
	}
}

// TestFlushConcurrencyBoundsWriters proves the configured limit is the peak:
// with concurrency two, exactly two writers overlap and a third never starts
// before one finishes.
func TestFlushConcurrencyBoundsWriters(t *testing.T) {
	s, _ := testStore(t, nil)
	gate := newWriteGate(2)
	t.Cleanup(gate.release)
	fl := testConcFlusher(t, s, 2, nil, gateFactory(gate))
	gen := genWith(t, testTenant, 1, 8, concParts(t, []string{
		"part0", "part1", "part2", "part3", "part4", "part5", "part6", "part7",
	}))

	done := make(chan error, 1)
	go func() { done <- fl.Flush(t.Context(), gen) }()
	gate.waitFull(t)
	if got := gate.peak.Load(); got != 2 {
		t.Fatalf("peak while blocked = %d, want 2", got)
	}
	gate.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("flush did not finish after writers were released")
	}
	if got := gate.peak.Load(); got != 2 {
		t.Fatalf("peak concurrent writers = %d, want exactly 2", got)
	}
	files, err := testTenantStore(t, s).Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 8 {
		t.Fatalf("files = %d, want 8", len(files))
	}
}

// TestFlushConcurrencyEmptyGenerationPreservesFrontier pins that zero parts
// still advance the committed frontier through the parallel path.
func TestFlushConcurrencyEmptyGenerationPreservesFrontier(t *testing.T) {
	s, _ := testStore(t, nil)
	rl := &reclaimLog{}
	fl := testConcFlusher(t, s, 4, rl.reclaim, nil)
	gen := &fakeGen{id: testTenant, first: 1, last: 9}
	if err := fl.Flush(t.Context(), gen); err != nil {
		t.Fatal(err)
	}
	fr, err := testTenantStore(t, s).Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if fr != (Frontier{First: 1, Last: 9}) {
		t.Fatalf("frontier = %+v", fr)
	}
	if len(rl.calls) != 1 || rl.calls[0].Seq != 9 {
		t.Fatalf("reclaim calls = %+v", rl.calls)
	}
}

// --- failure and cancellation ----------------------------------------------

// TestFlushConcurrencyFailedPartBlocksFrontier injects a create failure for
// one part under concurrency and proves sibling writers were already
// overlapping when it failed: two writers must be inside Write before the
// failure is released. The generation must not publish or reclaim, and a
// reopen must list no files.
func TestFlushConcurrencyFailedPartBlocksFrontier(t *testing.T) {
	gate := newWriteGate(2)
	t.Cleanup(gate.release)
	s, dir := testStore(t, func(ops *localFileOps) {
		def := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			if strings.HasPrefix(name, "boom/") && strings.HasSuffix(name, tempSuffix) {
				// Fail only after two sibling writers overlap, so the test
				// observes parallel work at the failure point.
				<-gate.full
				return nil, errFatal
			}
			return def(root, name, flag, perm)
		}
	})
	rl := &reclaimLog{}
	fl := testConcFlusher(t, s, 4, rl.reclaim, gateFactory(gate))
	gen := genWith(t, testTenant, 1, 4, concParts(t, []string{"alpha", "boom", "gamma", "delta"}))
	done := make(chan error, 1)
	go func() { done <- fl.Flush(t.Context(), gen) }()
	gate.waitFull(t)
	gate.release()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("failed flush did not return")
	}
	if !errors.Is(err, errFatal) {
		t.Fatalf("flush = %v", err)
	}
	ts := testTenantStore(t, s)
	if fr, _ := ts.Frontier(t.Context()); fr != (Frontier{}) {
		t.Fatalf("frontier crossed failed generation: %+v", fr)
	}
	if len(rl.calls) != 0 {
		t.Fatalf("reclaim ran on failed generation: %+v", rl.calls)
	}
	if files, _ := ts.Files(t.Context()); len(files) != 0 {
		t.Fatalf("failed generation published files: %+v", files)
	}
	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	if files, _ := testTenantStore(t, s2).Files(t.Context()); len(files) != 0 {
		t.Fatalf("reopen listed uncommitted files: %+v", files)
	}
}

// TestFlushConcurrencyPreservesConcurrentErrors proves a fatal part error is
// not masked by an error that arrives later through cancellation: both
// writers are held at a barrier until both are inside Write, then the fatal
// part returns first and the sibling returns its transient error after that,
// so scheduling cannot drop either one.
func TestFlushConcurrencyPreservesConcurrentErrors(t *testing.T) {
	s, _ := testStore(t, nil)
	var both sync.WaitGroup
	both.Add(2)
	fatalDone := make(chan struct{})
	nf := func(io.Writer, *arrow.Schema, []string) (ParquetWriter, error) {
		return &hookWriter{
			write: func(rec arrow.RecordBatch) error {
				both.Done()
				both.Wait()
				if rowLine(rec) == "boom-line" {
					close(fatalDone)
					return errFatal
				}
				<-fatalDone
				return errTransient
			},
			close: func() (storage.FileStats, error) { return storage.FileStats{}, nil },
		}, nil
	}
	fl := testConcFlusher(t, s, 2, nil, nf)
	gen := genWith(t, testTenant, 1, 2, []Part{
		partFor(t, "fatal", testHour, []rowSpec{{us: us(testHour, 0), line: "boom-line"}}),
		partFor(t, "slow", testHour.Add(time.Hour), []rowSpec{{us: us(testHour.Add(time.Hour), 0), line: "slow-line"}}),
	})
	err := fl.Flush(t.Context(), gen)
	if !errors.Is(err, errFatal) || !errors.Is(err, errTransient) {
		t.Fatalf("flush = %v, want both errFatal and errTransient", err)
	}
	if fr, _ := testTenantStore(t, s).Frontier(t.Context()); fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

// TestFlushConcurrencyCancellationJoinsWriters cancels while workers are
// inside Write and proves Flush returns an error only after every started
// writer has exited, with no publication.
func TestFlushConcurrencyCancellationJoinsWriters(t *testing.T) {
	s, _ := testStore(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{}, 8)
	var exited atomic.Int32
	nf := func(io.Writer, *arrow.Schema, []string) (ParquetWriter, error) {
		return &hookWriter{
			write: func(arrow.RecordBatch) error {
				defer exited.Add(1)
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			},
			close: func() (storage.FileStats, error) { return storage.FileStats{}, nil },
		}, nil
	}
	fl := testConcFlusher(t, s, 3, nil, nf)
	gen := genWith(t, testTenant, 1, 6, concParts(t, []string{
		"part0", "part1", "part2", "part3", "part4", "part5",
	}))
	done := make(chan error, 1)
	go func() { done <- fl.Flush(ctx, gen) }()
	for range 3 {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("writers did not start")
		}
	}
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("canceled flush did not return")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("flush = %v, want context.Canceled", err)
	}
	// Flush joined the workers, so every writer that started has exited.
	if got := exited.Load(); got != 3 {
		t.Fatalf("writers exited = %d, want 3 (Flush returned before join)", got)
	}
	if fr, _ := testTenantStore(t, s).Frontier(t.Context()); fr != (Frontier{}) {
		t.Fatalf("canceled flush advanced frontier: %+v", fr)
	}
}

// TestFlushConcurrencyPreexistingCancellation pins that a context already
// canceled before scheduling starts returns an error instead of publishing.
func TestFlushConcurrencyPreexistingCancellation(t *testing.T) {
	s, _ := testStore(t, nil)
	fl := testConcFlusher(t, s, 4, nil, nil)
	gen := genWith(t, testTenant, 1, 2, concParts(t, []string{"part0", "part1"}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := fl.Flush(ctx, gen); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush = %v, want context.Canceled", err)
	}
	if fr, _ := testTenantStore(t, s).Frontier(t.Context()); fr != (Frontier{}) {
		t.Fatalf("frontier = %+v", fr)
	}
}

// --- sync failures and retries ---------------------------------------------

// armedSyncFile fails Sync while armed and counts each injected failsync;
// parallel workers read the flag atomically.
type armedSyncFile struct {
	syncFile
	armed *atomic.Bool
	hits  *atomic.Int32
}

func (f *armedSyncFile) Sync() error {
	if f.armed.Load() {
		f.hits.Add(1)
		return errFatal
	}
	return f.syncFile.Sync()
}

// TestFlushConcurrencyFileSyncFailureRetries fails every Parquet temp sync
// during a first parallel attempt: nothing publishes, and a clean retry
// commits exact counts with the orphans quarantined on reopen.
func TestFlushConcurrencyFileSyncFailureRetries(t *testing.T) {
	var armed atomic.Bool
	var hits atomic.Int32
	s, dir := testStore(t, func(ops *localFileOps) {
		def := ops.create
		ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
			f, err := def(root, name, flag, perm)
			if err != nil {
				return nil, err
			}
			if !strings.HasSuffix(name, parquetExt+tempSuffix) {
				return f, nil
			}
			return &armedSyncFile{syncFile: f, armed: &armed, hits: &hits}, nil
		}
	})
	rl := &reclaimLog{}
	fl := testConcFlusher(t, s, 3, rl.reclaim, nil)
	mk := func() *fakeGen {
		return genWith(t, testTenant, 1, 4, concParts(t, []string{"part0", "part1", "part2", "part3"}))
	}
	armed.Store(true)
	if err := fl.Flush(t.Context(), mk()); !errors.Is(err, errFatal) {
		t.Fatalf("flush = %v", err)
	}
	if hits.Load() == 0 {
		t.Fatal("injected Parquet temp sync failure never ran")
	}
	ts := testTenantStore(t, s)
	if fr, _ := ts.Frontier(t.Context()); fr != (Frontier{}) {
		t.Fatalf("frontier crossed failed sync: %+v", fr)
	}
	if files, _ := ts.Files(t.Context()); len(files) != 0 {
		t.Fatalf("failed sync published files: %+v", files)
	}
	if len(rl.calls) != 0 {
		t.Fatalf("reclaim ran on failed sync: %+v", rl.calls)
	}
	armed.Store(false)
	if err := fl.Flush(t.Context(), mk()); err != nil {
		t.Fatalf("retry = %v", err)
	}
	fr, _ := ts.Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 4}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, _ := ts.Files(t.Context())
	if len(files) != 4 {
		t.Fatalf("files = %d, want 4", len(files))
	}
	var rows int64
	for _, f := range files {
		n, _ := readRows(t, ts, f)
		rows += n
	}
	if rows != 8 {
		t.Fatalf("rows = %d, want 8", rows)
	}
	if len(rl.calls) != 1 || rl.calls[0].Seq != 4 {
		t.Fatalf("reclaim calls = %+v", rl.calls)
	}
	// Reopen: only the committed files survive; uncommitted retry orphans are
	// quarantined, not listed.
	s.Close()
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	files, _ = testTenantStore(t, s2).Files(t.Context())
	if len(files) != 4 {
		t.Fatalf("reopen files = %d, want 4", len(files))
	}
}

// TestFlushConcurrencyDirSyncFailureRetries fails the partition directory
// fsync after one part's rename: the generation stays unpublished, and the
// retry publishes exact counts.
func TestFlushConcurrencyDirSyncFailureRetries(t *testing.T) {
	var armed atomic.Bool
	var hits atomic.Int32
	var renamed atomic.Bool
	target := hourDirRelPath("part0", testHour)
	s, _ := testStore(t, func(ops *localFileOps) {
		defSync := ops.syncDir
		defRename := ops.rename
		ops.rename = func(root *os.Root, old, new string) error {
			if err := defRename(root, old, new); err != nil {
				return err
			}
			if path.Dir(new) == target {
				renamed.Store(true)
			}
			return nil
		}
		ops.syncDir = func(root *os.Root, name string) error {
			if name == target && renamed.Load() && armed.CompareAndSwap(true, false) {
				hits.Add(1)
				return errFatal
			}
			return defSync(root, name)
		}
	})
	rl := &reclaimLog{}
	fl := testConcFlusher(t, s, 3, rl.reclaim, nil)
	mk := func() *fakeGen {
		return genWith(t, testTenant, 1, 4, concParts(t, []string{"part0", "part1", "part2", "part3"}))
	}
	armed.Store(true)
	if err := fl.Flush(t.Context(), mk()); !errors.Is(err, errFatal) {
		t.Fatalf("flush = %v", err)
	}
	if hits.Load() == 0 {
		t.Fatal("injected post-rename partition dir sync never ran")
	}
	fr, _ := testTenantStore(t, s).Frontier(t.Context())
	if fr != (Frontier{}) {
		t.Fatalf("frontier crossed failed dir sync: %+v", fr)
	}
	if len(rl.calls) != 0 {
		t.Fatalf("reclaim ran on failed dir sync: %+v", rl.calls)
	}
	if err := fl.Flush(t.Context(), mk()); err != nil {
		t.Fatalf("retry = %v", err)
	}
	ts := testTenantStore(t, s)
	fr, _ = ts.Frontier(t.Context())
	if fr != (Frontier{First: 1, Last: 4}) {
		t.Fatalf("frontier = %+v", fr)
	}
	files, _ := ts.Files(t.Context())
	if len(files) != 4 {
		t.Fatalf("files = %d, want 4", len(files))
	}
	var rows int64
	for _, f := range files {
		n, _ := readRows(t, ts, f)
		rows += n
	}
	if rows != 8 {
		t.Fatalf("rows = %d, want 8", rows)
	}
}

// --- ordering --------------------------------------------------------------

// TestFlushConcurrencyEqualsSerial pins that concurrency one and a parallel
// flush produce the same id-agnostic file set.
func TestFlushConcurrencyEqualsSerial(t *testing.T) {
	streams := []string{"alpha", "beta", "gamma", "delta"}
	serial, _ := testStore(t, nil)
	parallel, _ := testStore(t, nil)
	if err := testConcFlusher(t, serial, 1, nil, nil).Flush(t.Context(),
		genWith(t, testTenant, 1, 4, concParts(t, streams))); err != nil {
		t.Fatal(err)
	}
	if err := testConcFlusher(t, parallel, 4, nil, nil).Flush(t.Context(),
		genWith(t, testTenant, 1, 4, concParts(t, streams))); err != nil {
		t.Fatal(err)
	}
	serialShapes := fileShapes(t, testTenantStore(t, serial))
	parallelShapes := fileShapes(t, testTenantStore(t, parallel))
	if !slices.Equal(serialShapes, parallelShapes) {
		t.Fatalf("serial files = %v\nparallel files = %v", serialShapes, parallelShapes)
	}
	for _, s := range []*LocalStore{serial, parallel} {
		if fr, _ := testTenantStore(t, s).Frontier(t.Context()); fr != (Frontier{First: 1, Last: 4}) {
			t.Fatalf("frontier = %+v", fr)
		}
	}
}

// TestFlushConcurrencyTenantFrontiers covers independent tenants and ordered
// same-tenant frontiers: each tenant advances its own checkpoint in sequence,
// and gaps or overlaps are still rejected after parallel publication.
func TestFlushConcurrencyTenantFrontiers(t *testing.T) {
	s, _ := testStore(t, nil)
	other := tenant.ID("other")
	rl := &reclaimLog{}
	fl := testConcFlusher(t, s, 3, rl.reclaim, nil)

	flush := func(id tenant.ID, first, last uint64, streams []string) {
		t.Helper()
		if err := fl.Flush(t.Context(), genWith(t, id, first, last, concParts(t, streams))); err != nil {
			t.Fatalf("flush %s %d-%d: %v", id, first, last, err)
		}
	}
	flush(testTenant, 1, 2, []string{"alpha", "beta"})
	flush(other, 1, 3, []string{"one", "two", "three"})
	flush(testTenant, 3, 4, []string{"gamma", "delta"})
	flush(other, 4, 6, []string{"four", "five", "six"})

	checkTenant := func(id tenant.ID, want Frontier, wantFiles int, wantSeqs []uint64) {
		t.Helper()
		ts := testTenantStoreFor(t, s, id)
		fr, err := ts.Frontier(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if fr != want {
			t.Fatalf("%s frontier = %+v, want %+v", id, fr, want)
		}
		files, err := ts.Files(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != wantFiles {
			t.Fatalf("%s files = %d, want %d", id, len(files), wantFiles)
		}
		for _, f := range files {
			if f.Tenant != id {
				t.Fatalf("%s file has tenant %s", id, f.Tenant)
			}
		}
		var got []uint64
		for _, call := range rl.calls {
			if call.Tenant == id {
				got = append(got, call.Seq)
			}
		}
		if !slices.Equal(got, wantSeqs) {
			t.Fatalf("%s reclaim seqs = %v, want %v", id, got, wantSeqs)
		}
	}
	checkTenant(testTenant, Frontier{First: 3, Last: 4}, 4, []uint64{2, 4})
	checkTenant(other, Frontier{First: 4, Last: 6}, 6, []uint64{3, 6})

	// Same-tenant ordering survives parallel part writes: a gap and an
	// overlap are still rejected and leave the frontier untouched.
	gap := genWith(t, testTenant, 6, 7, nil)
	if err := fl.Flush(t.Context(), gap); !errors.Is(err, ErrGenerationGap) {
		t.Fatalf("gap = %v", err)
	}
	overlap := genWith(t, testTenant, 4, 5, nil)
	if err := fl.Flush(t.Context(), overlap); !errors.Is(err, ErrGenerationOverlap) {
		t.Fatalf("overlap = %v", err)
	}
	ts := testTenantStoreFor(t, s, testTenant)
	if fr, _ := ts.Frontier(t.Context()); fr != (Frontier{First: 3, Last: 4}) {
		t.Fatalf("frontier moved after rejected generations: %+v", fr)
	}
}

// testTenantStoreFor returns the tenant store of id.
func testTenantStoreFor(t *testing.T, s *LocalStore, id tenant.ID) *TenantStore {
	t.Helper()
	ts, err := s.Tenant(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}
