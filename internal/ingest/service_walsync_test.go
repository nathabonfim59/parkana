package ingest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// waitEntered waits for a test hook to report that its blocking wait began.
func waitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the blocked receipt")
	}
}

// closeOnce returns a func that closes ch at most once.
func closeOnce(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

// TestServiceWALConfigWiring proves the configured mode and interval reach the
// WAL opener, with an empty mode keeping the documented always default.
func TestServiceWALConfigWiring(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mode     string
		interval time.Duration
		wantMode wal.SyncMode
	}{
		{name: "empty is always", interval: 250 * time.Millisecond, wantMode: wal.SyncAlways},
		{name: "always", mode: config.WALSyncModeAlways, interval: 2 * time.Second, wantMode: wal.SyncAlways},
		{name: "interval", mode: config.WALSyncModeInterval, interval: 750 * time.Millisecond, wantMode: wal.SyncInterval},
	} {
		t.Run(tt.name, func(t *testing.T) {
			deps, _ := testDeps(t, t.TempDir())
			deps.Config.Ingest.WALSync = tt.mode
			deps.Config.Ingest.WALSyncInterval = tt.interval
			fw := &fakeWAL{}
			var got wal.Config
			opts := serviceOptions{openWAL: func(_ context.Context, _ string, c wal.Config) (walAPI, error) {
				got = c
				return fw, nil
			}}
			s := newTestService(t, deps, opts)
			cancel, done := startRun(t, s)
			waitReady(t, s)
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got.SyncMode != tt.wantMode || got.SyncInterval != tt.interval {
				t.Fatalf("wal.Config = mode %v interval %s, want mode %v interval %s",
					got.SyncMode, got.SyncInterval, tt.wantMode, tt.interval)
			}
		})
	}
}

// TestServiceNewRejectsBadWALSync proves hand-built invalid settings fail at
// construction rather than opening a WAL with an unusable policy.
func TestServiceNewRejectsBadWALSync(t *testing.T) {
	for _, tt := range []struct {
		name string
		mut  func(*config.Ingest)
	}{
		{"invalid mode", func(c *config.Ingest) { c.WALSync = "sometimes" }},
		{"negative interval", func(c *config.Ingest) { c.WALSyncInterval = -time.Second }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			deps, _ := testDeps(t, t.TempDir())
			tt.mut(&deps.Config.Ingest)
			if _, err := newService(deps, serviceOptions{}); err == nil {
				t.Fatal("expected construction error")
			}
		})
	}
}

// TestServiceAlwaysIgnoresWritten proves SyncAlways still acknowledges only
// after the durable receipt, even when the WAL reports the write earlier.
func TestServiceAlwaysIgnoresWritten(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	release := make(chan struct{})
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	fw.waitFn = func(ctx context.Context, _ uint64) error {
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

	resp := make(chan int, 1)
	go func() { resp <- pushHTTP(t, deps, pushReq("x")).Code }()
	select {
	case code := <-resp:
		t.Fatalf("push answered %d before fsync", code)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if code := <-resp; code != 204 {
		t.Fatalf("got %d, want 204", code)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceIntervalAcksAfterWritten proves interval mode answers 204 once
// the WAL write and memtable admission complete, before any fsync.
func TestServiceIntervalAcksAfterWritten(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	writtenEntered := make(chan struct{})
	writtenRelease := make(chan struct{})
	var enteredOnce sync.Once
	fw.writtenFn = func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(writtenEntered) })
		select {
		case <-writtenRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	waitRelease := make(chan struct{})
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-waitRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	waitReady(t, s)

	resp := make(chan int, 1)
	go func() { resp <- pushHTTP(t, deps, pushReq("x")).Code }()
	waitEntered(t, writtenEntered)
	select {
	case code := <-resp:
		t.Fatalf("push answered %d before the WAL write", code)
	case <-time.After(50 * time.Millisecond):
	}
	close(writtenRelease)
	if code := <-resp; code != 204 {
		t.Fatalf("got %d, want 204 before fsync", code)
	}
	close(waitRelease)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceIntervalAdmissionFailureNoAck proves an interval push whose
// memtable admission fails is never acknowledged, even when the WAL write
// receipt already completed.
func TestServiceIntervalAdmissionFailureNoAck(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
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
		t.Fatal("append succeeded despite a failed memtable admission")
	}
	if err := <-done; err == nil {
		t.Fatal("expected Run to report the admission failure")
	}
}

// TestServiceIntervalFallsBackToWait proves a fake WAL with no separate write
// receipt keeps its old meaning: its wait is the only completion to observe.
func TestServiceIntervalFallsBackToWait(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	release := make(chan struct{})
	fw.waitFn = func(ctx context.Context, _ uint64) error {
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

	resp := make(chan int, 1)
	go func() { resp <- pushHTTP(t, deps, pushReq("x")).Code }()
	select {
	case code := <-resp:
		t.Fatalf("push answered %d without any completion", code)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if code := <-resp; code != 204 {
		t.Fatalf("got %d, want 204", code)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceIntervalCrossTenantNoHeadOfLine proves a tenant whose durable
// receipt blocks the global watcher cannot delay another tenant's HTTP write
// acknowledgment.
func TestServiceIntervalCrossTenantNoHeadOfLine(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	aWait := make(chan struct{})
	aEntered := make(chan struct{})
	var aOnce sync.Once
	bWait := make(chan struct{})
	fw.enqueueFn = func(ctx context.Context, id tenant.ID, _ wal.Payload) (enqueueResult, error) {
		if id == tenant.ID("aaa") {
			return fw.result(id, func(ctx context.Context) error {
				aOnce.Do(func() { close(aEntered) })
				select {
				case <-aWait:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}), nil
		}
		return fw.result(id, func(ctx context.Context) error {
			select {
			case <-bWait:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), nil
	}
	ff := &fakeFlusher{}
	opts := fakeWALOpt(fw)
	// The fake assigns sequences globally; a per-tenant manifest flusher would
	// reject tenant b's first generation for starting at 2. This test proves
	// acknowledgment ordering, not publication ordering.
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.ID("aaa"), pushReq("a")); err != nil {
		t.Fatalf("tenant a append: %v", err)
	}
	waitEntered(t, aEntered)

	bDone := make(chan error, 1)
	go func() { bDone <- s.append(t.Context(), tenant.ID("bbb"), pushReq("b")) }()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("tenant b append: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tenant b acknowledgment blocked behind tenant a's durable receipt")
	}

	close(aWait)
	close(bWait)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceIntervalClientCancelKeepsDurable proves a client cancellation
// returns immediately while the receipt watcher still records the outcome.
func TestServiceIntervalClientCancelKeepsDurable(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	writtenEntered := make(chan struct{})
	writtenExited := make(chan struct{})
	durableRelease := make(chan struct{})
	var enteredOnce, exitedOnce sync.Once
	fw.writtenFn = func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(writtenEntered) })
		<-ctx.Done()
		exitedOnce.Do(func() { close(writtenExited) })
		return ctx.Err()
	}
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-durableRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	pushCtx, cancelPush := context.WithCancel(t.Context())
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(pushCtx, tenant.Default, pushReq("canceled")) }()
	waitEntered(t, writtenEntered)
	cancelPush()
	if err := <-appendDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("append: %v, want context.Canceled", err)
	}
	// The client canceled the Written wait, so that goroutine must exit; the
	// admitted WAL receipt stays with the durable watcher.
	waitEntered(t, writtenExited)
	ts := s.state(tenant.Default)
	ts.mu.Lock()
	pending, durable := len(ts.pending), ts.durable
	ts.mu.Unlock()
	if pending != 1 || durable != 0 {
		t.Fatalf("after client cancel pending=%d durable=%d, want the watcher still pending", pending, durable)
	}
	close(durableRelease)
	eventually(t, "durable bookkeeping after client cancel", func() bool {
		ts := s.state(tenant.Default)
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return ts.durable == 1 && len(ts.pending) == 0
	})
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published a canceled client's push: %+v", entries)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceIntervalDrainDeadlineUnblocksWritten proves an interval push
// blocked in the WAL write wait is released by the drain deadline, not by an
// fsync.
func TestServiceIntervalDrainDeadlineUnblocksWritten(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	writtenEntered := make(chan struct{})
	writtenExited := make(chan struct{})
	waitRelease := make(chan struct{})
	var enteredOnce, exitedOnce sync.Once
	fw.writtenFn = func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(writtenEntered) })
		<-ctx.Done()
		exitedOnce.Do(func() { close(writtenExited) })
		return ctx.Err()
	}
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-waitRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	opts := fakeWALOpt(fw)
	opts.shutdownTimeout = 100 * time.Millisecond
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("x")) }()
	waitEntered(t, writtenEntered)
	cancel()
	select {
	case err := <-appendDone:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("append: %v, want ErrUnavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append still waiting after the drain deadline")
	}
	// The append's deferred cancel releases the Written waiter even though the
	// admitted WAL work and its durable receipt are independent of it.
	waitEntered(t, writtenExited)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run still blocked after the drain deadline")
	}
	close(waitRelease)
}

// TestServiceIntervalWriteFailureFailsReady proves a WAL write failure never
// acknowledges interval data and poisons readiness.
func TestServiceIntervalWriteFailureFailsReady(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	boom := errors.New("write failed")
	fw.writtenFn = func(context.Context, uint64) error { return boom }
	fw.waitFn = func(context.Context, uint64) error { return boom }
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)

	if rec := pushHTTP(t, deps, pushReq("x")); rec.Code != 500 {
		t.Fatalf("got %d, want 500; body %q", rec.Code, rec.Body)
	}
	eventually(t, "readiness after a WAL write failure", func() bool {
		return s.ready(t.Context()) != nil
	})
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("Run: %v, want boom", err)
	}
}

// TestServiceIntervalLaterSyncFailurePoisons proves interval data is
// acknowledged before fsync, and a later failed sync still stops readiness.
func TestServiceIntervalLaterSyncFailurePoisons(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	boom := errors.New("timer sync failed")
	failRelease := make(chan struct{})
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-failRelease:
			return boom
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)

	if rec := pushHTTP(t, deps, pushReq("x")); rec.Code != 204 {
		t.Fatalf("got %d, want 204 before fsync; body %q", rec.Code, rec.Body)
	}
	close(failRelease)
	eventually(t, "readiness after failed interval sync", func() bool {
		return s.ready(t.Context()) != nil
	})
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("Run: %v, want boom", err)
	}
}

// TestTailIntervalPublishesAfterWritten proves live tail follows the interval
// acknowledgment point: entries appear after the WAL write plus admission,
// without waiting for fsync, and only once.
func TestTailIntervalPublishesAfterWritten(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	writtenEntered := make(chan struct{})
	writtenRelease := make(chan struct{})
	var enteredOnce sync.Once
	fw.writtenFn = func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(writtenEntered) })
		select {
		case <-writtenRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	durableRelease := make(chan struct{})
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-durableRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := newTestService(t, deps, fakeWALOpt(fw))
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("interval")) }()
	waitEntered(t, writtenEntered)
	if got := tailPublished(t, s); got != 0 {
		t.Fatalf("published entries = %v before the WAL write, want 0", got)
	}
	close(writtenRelease)
	if err := <-appendDone; err != nil {
		t.Fatalf("append: %v", err)
	}
	entries, dropped, ok := tailPop(t, sub, time.Second)
	if !ok || dropped != 0 || len(entries) != 1 || entries[0].Line != "interval" {
		t.Fatalf("tail after interval ack = %+v, dropped %d, ok %t", entries, dropped, ok)
	}
	if got := tailPublished(t, s); got != 1 {
		t.Fatalf("published entries = %v, want 1", got)
	}
	close(durableRelease)
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("durable completion published a second time: %+v", entries)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestTailIntervalWriteFailureNotPublished proves a failed WAL write never
// reaches live tail even though interval acknowledgment is the write receipt.
func TestTailIntervalWriteFailureNotPublished(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	boom := errors.New("write failed")
	fw.writtenFn = func(context.Context, uint64) error { return boom }
	fw.waitFn = func(context.Context, uint64) error { return boom }
	s := newTestService(t, deps, fakeWALOpt(fw))
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	defer cancel()
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.Default, pushReq("lost")); err == nil {
		t.Fatal("append succeeded despite a WAL write failure")
	}
	if got := tailPublished(t, s); got != 0 {
		t.Fatalf("published entries = %v after a failed write, want 0", got)
	}
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published after a failed write: %+v", entries)
	}
	<-done
}

// TestServiceIntervalFlushWaitsForDurable proves an interval-acknowledged push
// is still not published until its WAL receipt is fsync-durable.
func TestServiceIntervalFlushWaitsForDurable(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	release := make(chan struct{})
	fw.waitFn = func(ctx context.Context, _ uint64) error {
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

	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	passDone := make(chan error, 1)
	go func() { passDone <- s.flushPass(t.Context()) }()
	time.Sleep(20 * time.Millisecond)
	if got := ff.flushed(); len(got) != 0 {
		t.Fatalf("flushed %d generations before fsync", len(got))
	}
	close(release)
	if err := <-passDone; err != nil {
		t.Fatalf("flushPass: %v", err)
	}
	if got := ff.flushed(); len(got) != 1 || got[0] != [2]uint64{1, 1} {
		t.Fatalf("flushed %v, want [1,1]", got)
	}
	cancel()
	<-done
}

// TestServiceIntervalShutdownSyncBarrier proves shutdown forces the WAL sync
// before joining the durable watcher, so a one-hour interval cannot stall the
// drain, while the final flush and reclaim still run against the open WAL.
func TestServiceIntervalShutdownSyncBarrier(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	waitBlocked := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		enteredOnce.Do(func() { close(waitBlocked) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fw.syncFn = func(context.Context) error {
		closeOnce(release)()
		return nil
	}
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
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitEntered(t, waitBlocked)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := fw.callList()
	if !slices.Contains(calls, "sync") {
		t.Fatalf("calls %v, want a force sync", calls)
	}
	if slices.Index(calls, "sync") > slices.Index(calls, "close") {
		t.Fatalf("calls %v, want sync before close", calls)
	}
	fw.mu.Lock()
	reclaims := slices.Clone(fw.reclaims)
	fw.mu.Unlock()
	if len(reclaims) == 0 || reclaims[0] != (wal.Position{Tenant: tenant.Default, Seq: 1}) {
		t.Fatalf("reclaims %v, want the final manifest frontier", reclaims)
	}
}

// TestServiceIntervalSyncErrorReported proves a failed shutdown force sync is
// returned and skips the final publication of unknown-durability data.
func TestServiceIntervalSyncErrorReported(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.WALSync = config.WALSyncModeInterval
	deps.Config.Ingest.WALSyncInterval = time.Hour
	fw := &fakeWAL{}
	fw.writtenFn = func(context.Context, uint64) error { return nil }
	boom := errors.New("force sync failed")
	syncCalled := make(chan struct{})
	fw.syncFn = func(context.Context) error {
		closeOnce(syncCalled)()
		return boom
	}
	// Mirror a real poisoned barrier: the durable receipt stays pending until
	// the force sync runs, then fails with the same sticky error.
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-syncCalled:
			return boom
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ff := &fakeFlusher{}
	opts := fakeWALOpt(fw)
	opts.newFlusher = func(*LocalStore) (genFlusher, error) { return ff, nil }
	s := newTestService(t, deps, opts)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.Default, pushReq("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	ts := s.state(tenant.Default)
	ts.mu.Lock()
	pending, durable := len(ts.pending), ts.durable
	ts.mu.Unlock()
	if pending != 1 || durable != 0 {
		t.Fatalf("before force sync pending=%d durable=%d, want the receipt still pending", pending, durable)
	}
	cancel()
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("Run: %v, want boom", err)
	}
	if got := ff.flushed(); len(got) != 0 {
		t.Fatalf("published %v after a failed force sync", got)
	}
}
