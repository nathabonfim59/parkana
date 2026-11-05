package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// --- test configuration ---------------------------------------------------

func testIngest() config.Ingest {
	return config.Ingest{
		MaxLineSize:      256 * config.KiB,
		MaxRequestSize:   16 * config.MiB,
		MemtableMaxBytes: 256 * config.MiB,
		WALMaxBytes:      128 * config.MiB,
		// Long intervals keep the workers from racing the tests; tests drive
		// flushPass/uploader passes directly or shorten the interval.
		FlushInterval:  time.Hour,
		UploadInterval: time.Hour,
		UploadMinBytes: 128 * config.MiB,
		UploadMaxAge:   time.Hour,
		UploadMergeMax: 2 * config.GiB,
		Promotions:     []config.StreamPromotion{{Stream: "*", Columns: []string{"level"}}},
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testDeps returns a fully wired Deps: a real server on a fresh registry, a
// filesystem object store and the memory catalog. dataDir may be shared
// between services to simulate a restart over the same durable state.
func testDeps(t *testing.T, dataDir string) (app.Deps, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	log := testLogger()
	srv := server.New(server.Options{Logger: log, Registry: reg})
	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	deps := app.Deps{
		Config: config.Config{
			DataDir:      dataDir,
			StreamLabels: []string{"service_name", "job"},
			Ingest:       testIngest(),
		},
		Logger:  log,
		Store:   store,
		Catalog: catalog.NewMemory(),
		HTTP:    srv,
		Metrics: reg,
	}
	return deps, reg
}

func newTestService(t *testing.T, deps app.Deps, opts serviceOptions) *service {
	t.Helper()
	s, err := newService(deps, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// startRun launches s.Run and returns its cancel and result channel.
func startRun(t *testing.T, s *service) (context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	return cancel, done
}

func waitReady(t *testing.T, s *service) {
	t.Helper()
	eventually(t, "service ready", func() bool { return s.ready(t.Context()) == nil })
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func pushReq(lines ...string) *logs.PushRequest {
	st := logs.Stream{
		Labels: logs.New(
			logs.Label{Name: "job", Value: "api"},
			logs.Label{Name: "level", Value: "info"},
		),
	}
	for i, l := range lines {
		st.Entries = append(st.Entries, logs.Entry{
			Timestamp: time.Unix(1600000000, int64(i)*1000).UTC(),
			Line:      l,
		})
	}
	return &logs.PushRequest{Streams: []logs.Stream{st}}
}

// pushHTTP sends one JSON push through the real middleware stack.
func pushHTTP(t *testing.T, deps app.Deps, req *logs.PushRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := pushcodec.MarshalJSON(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/loki/api/v1/push", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(rec, r)
	return rec
}

// --- fakes ----------------------------------------------------------------

// fakeWAL scripts walAPI behavior; the zero value appends ascending sequences
// with instantly durable receipts.
type fakeWAL struct {
	mu       sync.Mutex
	seq      uint64
	tenants  []tenant.ID
	closed   bool
	calls    []string
	reclaims []wal.Position

	enqueueFn func(context.Context, tenant.ID, wal.Payload) (enqueueResult, error)
	writtenFn func(context.Context, uint64) error
	waitFn    func(context.Context, uint64) error
	syncFn    func(context.Context) error
	replayFn  func(context.Context, tenant.ID, wal.Position) iter.Seq2[wal.Record, error]
	reclaimFn func(context.Context, tenant.ID, wal.Position) error
	closeFn   func(context.Context) error
	tenantsFn func(context.Context) ([]tenant.ID, error)
}

// result builds an enqueueResult with the next sequence. A nil written defers
// to writtenFn (or stays nil when writtenFn is nil, so the service falls back
// to wait); a nil wait defers to waitFn (or resolves instantly when waitFn is
// nil).
func (f *fakeWAL) result(id tenant.ID, wait func(context.Context) error) enqueueResult {
	f.mu.Lock()
	f.seq++
	seq := f.seq
	f.mu.Unlock()
	var written func(context.Context) error
	if f.writtenFn != nil {
		written = func(ctx context.Context) error { return f.writtenFn(ctx, seq) }
	}
	if wait == nil {
		wait = func(ctx context.Context) error {
			if f.waitFn != nil {
				return f.waitFn(ctx, seq)
			}
			return nil
		}
	}
	return enqueueResult{pos: wal.Position{Tenant: id, Seq: seq}, written: written, wait: wait}
}

func (f *fakeWAL) Enqueue(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "enqueue")
	f.mu.Unlock()
	if f.enqueueFn != nil {
		return f.enqueueFn(ctx, id, p)
	}
	return f.result(id, nil), nil
}

func (f *fakeWAL) Replay(ctx context.Context, id tenant.ID, pos wal.Position) iter.Seq2[wal.Record, error] {
	if f.replayFn != nil {
		return f.replayFn(ctx, id, pos)
	}
	return func(func(wal.Record, error) bool) {}
}

func (f *fakeWAL) Reclaim(ctx context.Context, id tenant.ID, pos wal.Position) error {
	f.mu.Lock()
	f.calls = append(f.calls, "reclaim")
	f.reclaims = append(f.reclaims, pos)
	f.mu.Unlock()
	if f.reclaimFn != nil {
		return f.reclaimFn(ctx, id, pos)
	}
	return nil
}

func (f *fakeWAL) Sync(ctx context.Context) error {
	f.mu.Lock()
	f.calls = append(f.calls, "sync")
	f.mu.Unlock()
	if f.syncFn != nil {
		return f.syncFn(ctx)
	}
	return nil
}

func (f *fakeWAL) Tenants(ctx context.Context) ([]tenant.ID, error) {
	if f.tenantsFn != nil {
		return f.tenantsFn(ctx)
	}
	return f.tenants, nil
}

func (f *fakeWAL) Close(ctx context.Context) error {
	f.mu.Lock()
	f.calls = append(f.calls, "close")
	f.closed = true
	f.mu.Unlock()
	if f.closeFn != nil {
		return f.closeFn(ctx)
	}
	return nil
}

func (f *fakeWAL) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// fakeFlusher scripts generation publication.
type fakeFlusher struct {
	mu     sync.Mutex
	sealed [][2]uint64 // attempted frontiers in order
	errFn  func(SealedGeneration) error
}

func (f *fakeFlusher) Flush(ctx context.Context, gen SealedGeneration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	first, last := gen.Frontier()
	f.sealed = append(f.sealed, [2]uint64{first, last})
	if f.errFn != nil {
		return f.errFn(gen)
	}
	return nil
}

func (f *fakeFlusher) flushed() [][2]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sealed)
}

// setErr installs errFn under the lock so a flush worker running concurrently
// with the test observes the swap safely.
func (f *fakeFlusher) setErr(fn func(SealedGeneration) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errFn = fn
}

// manifestFlusher advances the tenant's durable manifest frontier through the
// real LocalStore without writing Parquet files.
type manifestFlusher struct {
	ls    *LocalStore
	errFn func(SealedGeneration) error
}

func (f *manifestFlusher) Flush(ctx context.Context, gen SealedGeneration) error {
	if f.errFn != nil {
		if err := f.errFn(gen); err != nil {
			return err
		}
	}
	ts, err := f.ls.Tenant(ctx, gen.Tenant())
	if err != nil {
		return err
	}
	first, last := gen.Frontier()
	return ts.Publish(ctx, Frontier{First: first, Last: last}, nil)
}

// fakeUploader scripts uploaderAPI.
type fakeUploader struct {
	runOnce func(context.Context) (UploadReport, error)
}

func (f *fakeUploader) RunOnce(ctx context.Context) (UploadReport, error) {
	if f.runOnce != nil {
		return f.runOnce(ctx)
	}
	return UploadReport{}, nil
}

// --- helpers ---------------------------------------------------------------

func fakeWALOpt(f *fakeWAL) serviceOptions {
	return serviceOptions{openWAL: func(context.Context, string, wal.Config) (walAPI, error) { return f, nil }}
}

// localFiles returns the committed local files of a tenant.
func localFiles(t *testing.T, ls *LocalStore, id tenant.ID) []LocalFile {
	t.Helper()
	ts, err := ls.Tenant(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// countLocalRows reads the tenant's committed local Parquet files and sums
// their rows.
func countLocalRows(t *testing.T, dataDir string, files []LocalFile) int64 {
	t.Helper()
	var rows int64
	for _, f := range files {
		path := filepath.Join(dataDir, "parquet", string(f.Tenant), f.RelPath())
		fh, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer fh.Close()
		st, err := fh.Stat()
		if err != nil {
			t.Fatal(err)
		}
		r, err := storage.NewReader(fh, st.Size())
		if err != nil {
			t.Fatal(err)
		}
		for rec, err := range r.Records(t.Context()) {
			if err != nil {
				t.Fatal(err)
			}
			rows += rec.NumRows()
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return rows
}

// --- lifecycle tests --------------------------------------------------------

// Pushes and readiness fail before Run opens the gate.
func TestServiceUnavailableBeforeRun(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	s := newTestService(t, deps, fakeWALOpt(&fakeWAL{}))
	if err := s.ready(t.Context()); err == nil {
		t.Fatal("expected unready before Run")
	}
	rec := pushHTTP(t, deps, pushReq("a"))
	if rec.Code != 503 {
		t.Fatalf("got %d, want 503; body %q", rec.Code, rec.Body)
	}
}

// A push answers 204 only after the receipt's durability wait resolves.
func TestServicePushWaitsForSync(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	release := make(chan struct{})
	fw.waitFn = func(ctx context.Context, seq uint64) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	waitReady(t, s)

	resp := make(chan *httptest.ResponseRecorder, 1)
	go func() { resp <- pushHTTP(t, deps, pushReq("x")) }()
	select {
	case r := <-resp:
		t.Fatalf("push answered %d before fsync", r.Code)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if r := <-resp; r.Code != 204 {
		t.Fatalf("got %d, want 204; body %q", r.Code, r.Body)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Replay gates acceptance: pushes get 503 and /ready fails while it runs.
func TestServiceUnavailableDuringReplay(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{tenants: []tenant.ID{tenant.Default}}
	entered := make(chan struct{})
	release := make(chan struct{})
	fw.replayFn = func(ctx context.Context, id tenant.ID, pos wal.Position) iter.Seq2[wal.Record, error] {
		return func(func(wal.Record, error) bool) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	<-entered
	if err := s.ready(t.Context()); err == nil {
		t.Fatal("expected unready during replay")
	}
	if rec := pushHTTP(t, deps, pushReq("a")); rec.Code != 503 {
		t.Fatalf("got %d during replay, want 503", rec.Code)
	}
	close(release)
	waitReady(t, s)
	if rec := pushHTTP(t, deps, pushReq("a")); rec.Code != 204 {
		t.Fatalf("got %d after replay, want 204; body %q", rec.Code, rec.Body)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// A rejected WAL enqueue is fatal: acceptance stops and Run returns the error.
func TestServiceEnqueueFailureFatal(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	boom := errors.New("disk gone")
	fw.enqueueFn = func(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
		return enqueueResult{}, boom
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if rec := pushHTTP(t, deps, pushReq("a")); rec.Code != 500 {
		t.Fatalf("got %d, want 500; body %q", rec.Code, rec.Body)
	}
	if err := s.ready(t.Context()); err == nil {
		t.Fatal("expected unready after enqueue failure")
	}
	if rec := pushHTTP(t, deps, pushReq("a")); rec.Code != 503 {
		t.Fatalf("got %d after failure, want 503", rec.Code)
	}
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("Run: %v, want boom", err)
	}
}

// A WAL sync failure is fatal immediately, not at the next flush tick.
func TestServiceSyncFailureFatal(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	boom := errors.New("fsync failed")
	fw.waitFn = func(ctx context.Context, seq uint64) error { return boom }
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if rec := pushHTTP(t, deps, pushReq("a")); rec.Code != 500 {
		t.Fatalf("got %d, want 500; body %q", rec.Code, rec.Body)
	}
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("Run: %v, want boom", err)
	}
}

// An in-flight push completes its admission before the WAL closes; a push
// that arrives after the gate closes is rejected.
func TestServiceGateDrainsInflight(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	enqueued := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fw.enqueueFn = func(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
		once.Do(func() { close(enqueued) })
		<-release
		return fw.result(id, nil), nil
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	waitReady(t, s)

	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("x")) }()
	<-enqueued
	cancel()
	// The drain must wait for the in-flight admission, not race wal.Close.
	select {
	case <-done:
		t.Fatal("Run returned while an admission was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("in-flight append: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := fw.callList()
	if slices.Index(calls, "enqueue") > slices.Index(calls, "close") {
		t.Fatalf("close before enqueue finished: %v", calls)
	}
	if err := s.append(t.Context(), tenant.Default, pushReq("y")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("post-close append: %v, want ErrUnavailable", err)
	}
}

// A push whose fsync wait outlives the drain deadline is unblocked with
// ErrUnavailable; its tracked durability survives the client.
func TestServiceDrainUnblocksReceiptWait(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	fw.waitFn = func(ctx context.Context, seq uint64) error {
		<-ctx.Done()
		return ctx.Err()
	}
	opts := fakeWALOpt(fw)
	opts.shutdownTimeout = 100 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("x")) }()
	eventually(t, "receipt tracked", func() bool {
		ts := s.state(tenant.Default)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return len(ts.pending) == 1
	})
	cancel()
	if err := <-appendDone; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("append: %v, want ErrUnavailable", err)
	}
	<-done
}

// --- flush ordering and durability -----------------------------------------

// A sealed generation is never published before its WAL frontier is durable.
func TestServiceFlushWaitsForDurability(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	release := make(chan struct{})
	fw.waitFn = func(ctx context.Context, seq uint64) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ff := &fakeFlusher{}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)

	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("x")) }()
	eventually(t, "receipt tracked", func() bool {
		ts := s.state(tenant.Default)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return len(ts.pending) == 1
	})
	passDone := make(chan error, 1)
	go func() { passDone <- s.flushPass(t.Context()) }()
	time.Sleep(20 * time.Millisecond)
	if n := len(ff.flushed()); n != 0 {
		t.Fatalf("flushed %d generations before durability", n)
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := <-passDone; err != nil {
		t.Fatalf("flushPass: %v", err)
	}
	if got := ff.flushed(); len(got) != 1 || got[0] != [2]uint64{1, 1} {
		t.Fatalf("flushed %v, want [1,1]", got)
	}
	cancel()
	<-done
}

// A failed generation blocks every later one from publishing.
func TestServiceFailedGenBlocksLater(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	ff := &fakeFlusher{}
	failFirst := true
	ff.errFn = func(SealedGeneration) error {
		if failFirst {
			return errors.New("temporary publish failure")
		}
		return nil
	}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	ctx := t.Context()
	for _, line := range []string{"a", "b"} {
		if err := s.append(ctx, tenant.Default, pushReq(line)); err != nil {
			t.Fatal(err)
		}
		if err := s.mem.Seal(ctx, tenant.Default); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("flushPass: %v", err)
	}
	if got := ff.flushed(); len(got) != 1 {
		t.Fatalf("flushed %v, want exactly the first generation attempted", got)
	}
	if n := len(s.taken[tenant.Default]); n != 2 {
		t.Fatalf("taken holds %d generations, want 2 retained", n)
	}
	failFirst = false
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("flushPass retry: %v", err)
	}
	if got := ff.flushed(); len(got) != 3 || got[1] != [2]uint64{1, 1} || got[2] != [2]uint64{2, 2} {
		t.Fatalf("flush order %v, want attempts on [1,1],[1,1],[2,2]", got)
	}
	cancel()
	<-done
}

// Reclaim runs at the committed manifest frontier even on passes that publish
// nothing, so a reclaim lost to a crash is retried.
func TestServiceReclaimAtManifestFrontier(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	var ls *LocalStore
	mf := &manifestFlusher{}
	opts := fakeWALOpt(fw)
	opts.openLocal = func(ctx context.Context, dir string) (*LocalStore, error) {
		var err error
		ls, err = OpenLocal(ctx, dir)
		return ls, err
	}
	opts.newFlusher = func(*LocalStore) (genFlusher, error) {
		mf.ls = ls
		return mf, nil
	}
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	ctx := t.Context()
	if err := s.append(ctx, tenant.Default, pushReq("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("flushPass: %v", err)
	}
	fr, err := ls.Frontier(ctx, tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Last != 1 {
		t.Fatalf("manifest frontier %d, want 1", fr.Last)
	}
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("second flushPass: %v", err)
	}
	fw.mu.Lock()
	reclaims := slices.Clone(fw.reclaims)
	fw.mu.Unlock()
	if len(reclaims) < 2 {
		t.Fatalf("reclaims %v, want at least two passes at the frontier", reclaims)
	}
	for _, p := range reclaims {
		if p != (wal.Position{Tenant: tenant.Default, Seq: 1}) {
			t.Fatalf("reclaim at %v, want seq 1", p)
		}
	}
	cancel()
	<-done
}

// A corrupt or invalid-frontier reclaim is fatal; ordinary IO retries.
func TestServiceReclaimFatalVsRetry(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	var ls *LocalStore
	mf := &manifestFlusher{}
	opts := fakeWALOpt(fw)
	opts.openLocal = func(ctx context.Context, dir string) (*LocalStore, error) {
		var err error
		ls, err = OpenLocal(ctx, dir)
		return ls, err
	}
	opts.newFlusher = func(*LocalStore) (genFlusher, error) {
		mf.ls = ls
		return mf, nil
	}
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	ctx := t.Context()
	if err := s.append(ctx, tenant.Default, pushReq("a")); err != nil {
		t.Fatal(err)
	}
	ioErr := errors.New("remove failed")
	fw.reclaimFn = func(context.Context, tenant.ID, wal.Position) error { return ioErr }
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("transient reclaim must not fail the pass: %v", err)
	}
	fw.reclaimFn = func(context.Context, tenant.ID, wal.Position) error {
		return wal.ErrInvalidFrontier
	}
	if err := s.flushPass(ctx); !errors.Is(err, wal.ErrInvalidFrontier) {
		t.Fatalf("invalid-frontier reclaim: %v, want fatal", err)
	}
	cancel()
	<-done
}

// A fatal uploader error stops the service; transient ones are retried.
func TestServiceUploadFatalVsRetry(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	fu := &fakeUploader{}
	calls := make(chan struct{}, 16)
	var corrupt atomic.Bool
	fu.runOnce = func(ctx context.Context) (UploadReport, error) {
		calls <- struct{}{}
		if corrupt.Load() {
			return UploadReport{}, fmt.Errorf("ingest: %w", ErrManifestCorrupt)
		}
		return UploadReport{}, errors.New("temporary object store outage")
	}
	opts := fakeWALOpt(fw)
	opts.newUploader = func(*LocalStore, objstore.Store, catalog.Catalog) (uploaderAPI, error) {
		return fu, nil
	}
	deps.Config.Ingest.UploadInterval = 10 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	for range 3 {
		<-calls
	}
	corrupt.Store(true)
	if err := <-done; !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("Run: %v, want ErrManifestCorrupt", err)
	}
	cancel()
}

// --- real-component end-to-end ----------------------------------------------

// Graceful shutdown seals, flushes and durably publishes a tiny low-rate push
// in both sync modes: the rows are readable Parquet afterwards. The interval
// case exercises the real walAdapter.Written + WAL.Sync shutdown coupling with
// a timer that could never fire inside the test.
func TestServiceDrainPublishesTinyPush(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode string
	}{
		{name: "always", mode: config.WALSyncModeAlways},
		{name: "interval", mode: config.WALSyncModeInterval},
	} {
		t.Run(tt.name, func(t *testing.T) {
			deps, _ := testDeps(t, t.TempDir())
			deps.Config.Ingest.WALSync = tt.mode
			deps.Config.Ingest.WALSyncInterval = time.Hour
			dataDir := deps.Config.DataDir
			s := newTestService(t, deps, serviceOptions{})
			cancel, done := startRun(t, s)
			waitReady(t, s)
			if err := s.append(t.Context(), tenant.Default, pushReq("one", "two", "three")); err != nil {
				t.Fatal(err)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
			// The service owns and closes its LocalStore; reopen it for
			// assertions.
			ls, err := OpenLocal(t.Context(), dataDir)
			if err != nil {
				t.Fatal(err)
			}
			defer ls.Close()
			files := localFiles(t, ls, tenant.Default)
			if len(files) == 0 {
				t.Fatal("no local Parquet files after drain")
			}
			if rows := countLocalRows(t, dataDir, files); rows != 3 {
				t.Fatalf("rows %d, want 3", rows)
			}
		})
	}
}

// A publish failure leaves the WAL intact: run 2 replays and republishes the
// record as queryable Parquet.
func TestServiceReplayedPushBecomesQueryable(t *testing.T) {
	dataDir := t.TempDir()
	deps, _ := testDeps(t, dataDir)
	failing := errors.New("publish outage")
	opts := serviceOptions{
		newFlusher: func(*LocalStore) (genFlusher, error) {
			return &fakeFlusher{errFn: func(SealedGeneration) error { return failing }}, nil
		},
	}
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("persisted")); err != nil {
		t.Fatal(err)
	}
	if err := s.flushPass(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel()
	// The strict final flush must surface the publication failure while the
	// WAL keeps the records.
	if err := <-done; !errors.Is(err, failing) {
		t.Fatalf("Run 1: %v, want the publication error", err)
	}

	// A fresh service over the same data dir replays the retained WAL.
	deps2, _ := testDeps(t, dataDir)
	s2 := newTestService(t, deps2, serviceOptions{})
	cancel, done = startRun(t, s2)
	waitReady(t, s2)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	ls, err := OpenLocal(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	files := localFiles(t, ls, tenant.Default)
	if rows := countLocalRows(t, dataDir, files); rows != 1 {
		t.Fatalf("rows %d after replay, want 1", rows)
	}
}

// The periodic flush publishes below-threshold data without an explicit seal.
func TestServicePeriodicFlushSeals(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	var ls *LocalStore
	opts := serviceOptions{
		openLocal: func(ctx context.Context, dir string) (*LocalStore, error) {
			var err error
			ls, err = OpenLocal(ctx, dir)
			return ls, err
		},
	}
	deps.Config.Ingest.FlushInterval = 20 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("trickle")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "periodic flush publication", func() bool {
		fr, err := ls.Frontier(t.Context(), tenant.Default)
		return err == nil && fr.Last >= 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The service owns and closes its LocalStore; reopen it for assertions.
	ls2, err := OpenLocal(t.Context(), deps.Config.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ls2.Close()
	files := localFiles(t, ls2, tenant.Default)
	if rows := countLocalRows(t, deps.Config.DataDir, files); rows != 1 {
		t.Fatalf("rows %d, want 1", rows)
	}
}

// An empty push still advances the manifest frontier through publication.
func TestServiceEmptyPushAdvancesFrontier(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	s := newTestService(t, deps, serviceOptions{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, &logs.PushRequest{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The service owns and closes its LocalStore; reopen it for assertions.
	ls, err := OpenLocal(t.Context(), deps.Config.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	fr, err := ls.Frontier(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Last != 1 {
		t.Fatalf("manifest frontier %d, want 1 after empty push", fr.Last)
	}
}

// Repeated start/stop cycles join every goroutine and return all Arrow
// memory to the checked allocator.
func TestServiceRepeatedStartStopNoLeak(t *testing.T) {
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	for range 3 {
		deps, _ := testDeps(t, t.TempDir())
		s := newTestService(t, deps, serviceOptions{allocator: ca})
		cancel, done := startRun(t, s)
		waitReady(t, s)
		if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
			t.Fatal(err)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	if got := ca.CurrentAlloc(); got != 0 {
		t.Fatalf("allocator holds %d bytes after cycles", got)
	}
}

// A request full of tiny entries still fits the derived WAL bound — the
// amplification is covered by walMaxRecordBytes, not guessed.
func TestServiceTinyEntryAmplification(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.MaxRequestSize = config.MiB
	entries := make([]logs.Entry, 4096)
	for i := range entries {
		entries[i] = logs.Entry{Timestamp: time.Unix(1600000000, 1).UTC(), Line: "x"}
	}
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "job", Value: "api"}),
		Entries: entries,
	}}}
	s := newTestService(t, deps, serviceOptions{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, req); err != nil {
		t.Fatalf("append: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// --- construction -----------------------------------------------------------

func TestWalMaxRecordBytes(t *testing.T) {
	bound, err := walMaxRecordBytes(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if bound != 64*(16<<20)+8<<10 {
		t.Fatalf("bound %d", bound)
	}
	if _, err := walMaxRecordBytes(math.MaxUint32); err == nil {
		t.Fatal("expected rejection of an unsupportable request size")
	}
	if _, err := walMaxRecordBytes(0); err == nil {
		t.Fatal("expected rejection of a nonpositive request size")
	}
}

// A second factory on the same server fails cleanly instead of panicking.
func TestServiceNewDuplicateRoute(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	newTestService(t, deps, serviceOptions{})
	if _, err := newService(deps, serviceOptions{}); err == nil {
		t.Fatal("expected duplicate route error")
	}
}

// Invalid ingest configuration is rejected even without config.Load.
func TestServiceNewValidatesConfig(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.FlushInterval = 0
	if _, err := newService(deps, serviceOptions{}); err == nil {
		t.Fatal("expected validation error for zero flush interval")
	}
	deps, _ = testDeps(t, t.TempDir())
	deps.Config.Ingest.MaxRequestSize = config.ByteSize(math.MaxUint32)
	if _, err := newService(deps, serviceOptions{}); err == nil {
		t.Fatal("expected rejection of unsupported request size")
	}
}

// New returns the public factory's service and registers the push route.
func TestServiceFactoryRegistersPush(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	svc, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil {
		t.Fatal("nil service")
	}
	if !deps.HTTP.Registered("POST /loki/api/v1/push") {
		t.Fatal("push route not registered")
	}
}

// A canceled client's receipt stays tracked: its data still publishes.
func TestServiceCanceledClientReceiptTracked(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	release := make(chan struct{})
	fw.waitFn = func(ctx context.Context, seq uint64) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ff := &fakeFlusher{}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	pushCtx, cancelPush := context.WithCancel(t.Context())
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(pushCtx, tenant.Default, pushReq("x")) }()
	eventually(t, "receipt tracked", func() bool {
		ts := s.state(tenant.Default)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return len(ts.pending) == 1
	})
	cancelPush()
	if err := <-appendDone; err == nil {
		t.Fatal("expected canceled append to return an error")
	}
	close(release)
	if err := s.flushPass(t.Context()); err != nil {
		t.Fatalf("flushPass: %v", err)
	}
	if got := ff.flushed(); len(got) != 1 || got[0] != [2]uint64{1, 1} {
		t.Fatalf("flushed %v, want the canceled client's generation [1,1]", got)
	}
	cancel()
	<-done
}

// Replay publishes auto-sealed generations during iteration, bounding memory;
// readiness stays false until replay finishes.
func TestServiceReplayPublishesSealed(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALMaxBytes = 1 // seal after every record
	fw := &fakeWAL{tenants: []tenant.ID{tenant.Default}}
	ff := &fakeFlusher{}
	seen := make(chan struct{}, 8)
	ff.errFn = func(SealedGeneration) error {
		seen <- struct{}{}
		return nil
	}
	proceed := make(chan struct{})
	var pOnce sync.Once
	proceedFn := func() { pOnce.Do(func() { close(proceed) }) }
	t.Cleanup(proceedFn)
	fw.replayFn = func(ctx context.Context, id tenant.ID, pos wal.Position) iter.Seq2[wal.Record, error] {
		return func(yield func(wal.Record, error) bool) {
			rec := wal.Record{Position: wal.Position{Tenant: id, Seq: 1}, Request: *pushReq("r1")}
			if !yield(rec, nil) {
				return
			}
			select {
			case <-proceed:
			case <-ctx.Done():
			}
			rec.Position.Seq = 2
			yield(rec, nil)
		}
	}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	select {
	case <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("no generation published during replay")
	}
	if err := s.ready(t.Context()); err == nil {
		t.Fatal("expected unready during replay")
	}
	proceedFn()
	waitReady(t, s)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// A worker blocked past the deadline keeps owning resources: nothing is
// released until it actually exits.
func TestServiceReaperReleasesAfterStraggler(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	fw := &fakeWAL{}
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	var once, uOnce sync.Once
	unblockFn := func() { uOnce.Do(func() { close(unblock) }) }
	t.Cleanup(unblockFn)
	ff := &fakeFlusher{errFn: func(SealedGeneration) error {
		once.Do(func() { close(blocked) })
		<-unblock
		return nil
	}}
	opts := fakeWALOpt(fw)
	opts.allocator = ca
	opts.shutdownTimeout = 100 * time.Millisecond
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	deps.Config.Ingest.FlushInterval = 10 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatal(err)
	}
	<-blocked
	cancel()
	<-done // Run must stay bounded by the deadline
	if got := ca.CurrentAlloc(); got == 0 {
		t.Fatal("resources released while the flush worker was still blocked")
	}
	unblockFn()
	eventually(t, "straggler cleanup", func() bool { return ca.CurrentAlloc() == 0 })
}

// Fatal sentinels win inside joined errors that also carry context errors.
func TestServiceIsFatalJoined(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	s := newTestService(t, deps, serviceOptions{})
	if !s.isFatal(errors.Join(ErrManifestCorrupt, context.Canceled)) {
		t.Fatal("joined manifest corruption must be fatal")
	}
	if s.isFatal(context.Canceled) || s.isFatal(context.DeadlineExceeded) {
		t.Fatal("pure context errors are not fatal")
	}
	if s.isFatal(errors.New("temporary io")) {
		t.Fatal("ordinary io errors are not fatal")
	}
}

// A WAL closed underneath an accepting service fails readiness, not just 503.
func TestServiceClosedWhileAcceptingFailsReady(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	fw.enqueueFn = func(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
		return enqueueResult{}, wal.ErrClosed
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("append: %v, want ErrUnavailable", err)
	}
	if err := s.ready(t.Context()); err == nil {
		t.Fatal("expected unready after unexpected wal close")
	}
	if err := <-done; !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("Run: %v, want wal.ErrClosed", err)
	}
}

// Startup cleanup errors join into the returned error.
func TestServiceStartupCleanupErrorsJoin(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	closeErr := errors.New("wal close failed")
	openErr := errors.New("open local failed")
	fw := &fakeWAL{closeFn: func(context.Context) error { return closeErr }}
	opts := fakeWALOpt(fw)
	opts.openLocal = func(context.Context, string) (*LocalStore, error) { return nil, openErr }
	s := newTestService(t, deps, opts)
	err := s.Run(t.Context())
	if !errors.Is(err, openErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Run: %v, want both open and close errors", err)
	}
}

// A live wait that reports context.Canceled is a durability failure whose
// cause tree still carries the cancel leaf alongside errDurability.
func TestServiceWatcherCanceledWaitIsDurability(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	fw.waitFn = func(ctx context.Context, seq uint64) error { return context.Canceled }
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err == nil {
		t.Fatal("expected append to report the failed wait")
	}
	err := <-done
	if !errors.Is(err, errDurability) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v, want both errDurability and context.Canceled", err)
	}
}

// Once Enqueue accepts, the receipt is registered even when memtable
// admission fails: the wait outcome is observed, not stranded.
func TestServicePostEnqueueAdmissionFailure(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	var observed []uint64
	var obsMu sync.Mutex
	fw.waitFn = func(ctx context.Context, seq uint64) error {
		obsMu.Lock()
		observed = append(observed, seq)
		obsMu.Unlock()
		return nil
	}
	var s *service
	fw.enqueueFn = func(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
		s.mem.Close() // force the post-enqueue admission to fail
		return fw.result(id, nil), nil
	}
	s = newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err == nil {
		t.Fatal("expected admission failure")
	}
	eventually(t, "receipt observed and settled", func() bool {
		ts := s.state(tenant.Default)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		obsMu.Lock()
		defer obsMu.Unlock()
		return len(ts.pending) == 0 && ts.durable == 1 && len(observed) == 1 && observed[0] == 1
	})
	if err := <-done; err == nil {
		t.Fatal("expected Run to report the admission failure")
	}
}

// A push still inside Prepare when shutdown starts is held by the gate: the
// WAL never closes under it and its data is flushed, not lost.
func TestServicePrepareGateAcrossShutdown(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	ff := &fakeFlusher{}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	orig := s.mem.promotedFor
	entered := make(chan struct{})
	release := make(chan struct{})
	var eOnce, rOnce sync.Once
	releasePrep := func() { rOnce.Do(func() { close(release) }) }
	t.Cleanup(releasePrep)
	s.mem.promotedFor = func(name string) []string {
		eOnce.Do(func() { close(entered) })
		<-release
		return orig(name)
	}
	cancel, done := startRun(t, s)
	waitReady(t, s)
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("x")) }()
	<-entered
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while a push was still preparing")
	case <-time.After(50 * time.Millisecond):
	}
	if slices.Contains(fw.callList(), "close") {
		t.Fatal("wal closed while a push was still preparing")
	}
	releasePrep()
	if err := <-appendDone; err != nil {
		t.Fatalf("in-flight append: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := ff.flushed(); len(got) != 1 || got[0] != [2]uint64{1, 1} {
		t.Fatalf("flushed %v, want the preparing push's generation [1,1]", got)
	}
}

// A strict final flush that expires mid-publish reports the context error.
func TestServiceStrictFlushReturnsDeadline(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	opts := fakeWALOpt(&fakeWAL{})
	opts.shutdownTimeout = 80 * time.Millisecond
	opts.newFlusher = func(*LocalStore) (genFlusher, error) {
		return genFlusherFunc(func(ctx context.Context, gen SealedGeneration) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	}
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v, want context.DeadlineExceeded", err)
	}
}

// A final flush stuck in a Flush that ignores ctx — like a sync blocked in
// the kernel — must not hold Run past its shutdown deadline: Run reports the
// deadline, the memtable and local store stay owned by the stuck pass, and a
// completion-joined reaper frees them only once the pass actually exits.
func TestServiceRunStuckFinalFlushBounded(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	opts := fakeWALOpt(&fakeWAL{})
	opts.shutdownTimeout = 80 * time.Millisecond
	opts.newFlusher = func(*LocalStore) (genFlusher, error) {
		return genFlusherFunc(func(context.Context, SealedGeneration) error {
			<-release
			return nil
		}), nil
	}
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run: %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run still blocked on the final flush past its deadline")
	}
	// The stuck pass still owns the local store and the memtable.
	if _, err := s.local.Tenant(t.Context(), tenant.Default); errors.Is(err, ErrLocalClosed) {
		t.Fatal("local store closed underneath the stuck final flush")
	}
	if err := s.mem.SealAll(t.Context()); errors.Is(err, ErrClosed) {
		t.Fatal("memtable closed underneath the stuck final flush")
	}
	close(release)
	// The reaper frees both only after the pass completes.
	eventually(t, "local store closed by the reaper", func() bool {
		_, err := s.local.Tenant(t.Context(), tenant.Default)
		return errors.Is(err, ErrLocalClosed)
	})
	eventually(t, "memtable closed by the reaper", func() bool {
		return errors.Is(s.mem.SealAll(t.Context()), ErrClosed)
	})
}

// Run restores each tenant's cumulative rotation total from its committed
// manifest at startup, labeled per tenant; ready implies the restore ran.
func TestServiceRotationsStartupRestore(t *testing.T) {
	dataDir := t.TempDir()
	pre, err := OpenLocal(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	tsDef, err := pre.Tenant(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	u := refreshableUpload(t, tsDef, "checkout")
	for range 2 {
		if _, err := tsDef.RefreshUploadKey(t.Context(), u.ID); err != nil {
			t.Fatal(err)
		}
	}
	other := tenant.ID("team-b")
	tsOther, err := pre.Tenant(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	uo := refreshableUpload(t, tsOther, "payments")
	if _, err := tsOther.RefreshUploadKey(t.Context(), uo.ID); err != nil {
		t.Fatal(err)
	}
	if err := pre.Close(); err != nil {
		t.Fatal(err)
	}

	deps, reg := testDeps(t, dataDir)
	opts := fakeWALOpt(&fakeWAL{})
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); !ok || v != 2 {
		t.Fatalf("default rotations %v (found=%v), want 2", v, ok)
	}
	if v, ok := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "team-b"); !ok || v != 1 {
		t.Fatalf("team-b rotations %v (found=%v), want 1", v, ok)
	}
}

// Every uploader pass refreshes the rotation totals — a pass that fails after
// its rotation committed included — and an unchanged total never double-counts.
func TestServiceRotationsRefreshAfterPasses(t *testing.T) {
	dataDir := t.TempDir()
	// One refreshable receipt for the default tenant, prepared before Run.
	pre, err := OpenLocal(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	tsPre, err := pre.Tenant(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	u := refreshableUpload(t, tsPre, "checkout")
	rotID := u.ID
	if err := pre.Close(); err != nil {
		t.Fatal(err)
	}

	deps, reg := testDeps(t, dataDir)
	var ls *LocalStore
	opts := fakeWALOpt(&fakeWAL{})
	opts.openLocal = func(ctx context.Context, dir string) (*LocalStore, error) {
		var err error
		ls, err = OpenLocal(ctx, dir)
		return ls, err
	}
	fu := &fakeUploader{}
	opts.newUploader = func(*LocalStore, objstore.Store, catalog.Catalog) (uploaderAPI, error) {
		return fu, nil
	}
	var pass atomic.Int64
	fu.runOnce = func(ctx context.Context) (UploadReport, error) {
		rotate := func() error {
			ts, err := ls.Tenant(ctx, tenant.Default)
			if err != nil {
				return err
			}
			_, err = ts.RefreshUploadKey(ctx, rotID)
			return err
		}
		switch pass.Add(1) {
		case 1, 3:
			// The rotation commits, then the pass fails before any Put or
			// catalog update completes.
			if err := rotate(); err != nil {
				return UploadReport{}, err
			}
			return UploadReport{}, errors.New("transient put failure")
		default:
			return UploadReport{}, nil
		}
	}
	deps.Config.Ingest.UploadInterval = 10 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	// After the third pass: two failed rotations landed, a clean pass added
	// nothing on top, and both pass outcomes were counted. Later clean ticks
	// may already have fired, so only the pass count is a lower bound.
	eventually(t, "rotation totals after three passes", func() bool {
		rot, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default")
		errs, _ := gatherLabeled(t, reg, "parkana_ingest_upload_total", "result", "error")
		ok, _ := gatherLabeled(t, reg, "parkana_ingest_upload_total", "result", "ok")
		return rot == 2 && errs == 2 && ok >= 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

type genFlusherFunc func(context.Context, SealedGeneration) error

func (f genFlusherFunc) Flush(ctx context.Context, gen SealedGeneration) error {
	return f(ctx, gen)
}

// A joined fatal sentinel plus context.Canceled still fails the service.
func TestServiceJoinedFatalWithContext(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fu := &fakeUploader{runOnce: func(context.Context) (UploadReport, error) {
		return UploadReport{}, errors.Join(ErrManifestCorrupt, context.Canceled)
	}}
	opts := fakeWALOpt(&fakeWAL{})
	opts.newUploader = func(*LocalStore, objstore.Store, catalog.Catalog) (uploaderAPI, error) {
		return fu, nil
	}
	deps.Config.Ingest.UploadInterval = 10 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	err := <-done
	if !errors.Is(err, ErrManifestCorrupt) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v, want ErrManifestCorrupt and context.Canceled", err)
	}
	cancel()
}

// A flusher that ends the pass context and returns a joined fatal must not be
// suppressed by the ctx check: the sentinel reaches the caller.
func TestServiceFlushFatalAfterCtxCancel(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	ff := &fakeFlusher{}
	opts := fakeWALOpt(&fakeWAL{})
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatal(err)
	}
	ctx, cancelPass := context.WithCancel(t.Context())
	defer cancelPass()
	ff.setErr(func(SealedGeneration) error {
		cancelPass()
		return errors.Join(ErrManifestCorrupt, context.Canceled)
	})
	if err := s.flushPass(ctx); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("flushPass: %v, want ErrManifestCorrupt", err)
	}
	cancel()
	<-done
}

// The same joined fatal through the periodic worker keeps Run's cause. The
// flusher arms once — the first Flush cancels the worker's own context and
// returns a joined fatal, every later call publishes normally — so a periodic
// pass that swallowed the error cannot be rescued by the shutdown drain's
// retry: Run would report no error at all.
func TestServiceFlushFatalAfterCtxCancelWorker(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	ff := &fakeFlusher{}
	opts := fakeWALOpt(&fakeWAL{})
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	deps.Config.Ingest.FlushInterval = 10 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)
	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatal(err)
	}
	var armed atomic.Bool
	ff.setErr(func(SealedGeneration) error {
		if !armed.CompareAndSwap(false, true) {
			return nil // the shutdown drain retry publishes normally
		}
		cancel()
		return errors.Join(ErrManifestCorrupt, context.Canceled)
	})
	if err := <-done; !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("Run: %v, want ErrManifestCorrupt", err)
	}
}

// Closed-memtable misuse surfaces from a flush pass instead of retrying
// forever: ErrClosed is fatal, so the worker stops the service. (With an
// already-canceled context SealAll reports the cancellation itself, which is
// not fatal and may end the pass cleanly.)
func TestServiceFlushPassFatalSealClosed(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	s := newTestService(t, deps, fakeWALOpt(&fakeWAL{}))
	s.mem.Close()
	if err := s.flushPass(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("flushPass: %v, want ErrClosed", err)
	}
}
