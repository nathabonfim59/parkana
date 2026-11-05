package wal

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

type syncGate struct {
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newSyncGate() *syncGate {
	return &syncGate{started: make(chan struct{}), release: make(chan struct{})}
}

func (g *syncGate) unblock() {
	g.releaseOnce.Do(func() { close(g.release) })
}

type groupHooks struct {
	syncCalls   atomic.Int64
	failSyncAt  int64
	writeErr    error
	syncErr     error
	closeErr    error
	syncDirErr  error
	failWrites  atomic.Bool
	writeFails  atomic.Int64
	failClose   atomic.Bool
	failSyncDir atomic.Bool
	gates       map[int64]*syncGate
	dirGate     *syncGate
	eventsMu    sync.Mutex
	events      []string
}

// fileOps returns fully hooked file operations built on the defaults, so a
// test can install them before open (and add its own seams such as a manual
// ticker factory).
func (h *groupHooks) fileOps() fileOps {
	ops := defaultFileOps()
	ops.create = func(root *os.Root, name string) (segmentFile, error) {
		file, err := defaultFileOps().create(root, name)
		if err != nil {
			return nil, err
		}
		return &hookedFile{segmentFile: file, hooks: h}, nil
	}
	ops.open = func(root *os.Root, name string) (segmentFile, error) {
		file, err := defaultFileOps().open(root, name)
		if err != nil {
			return nil, err
		}
		return &hookedFile{segmentFile: file, hooks: h}, nil
	}
	ops.remove = func(root *os.Root, name string) error {
		h.record("remove " + name)
		return root.Remove(name)
	}
	ops.syncDir = func(root *os.Root) error {
		h.record("syncdir")
		if gate := h.dirGate; gate != nil {
			gate.startOnce.Do(func() { close(gate.started) })
			<-gate.release
		}
		if h.failSyncDir.CompareAndSwap(true, false) {
			return h.syncDirErr
		}
		return syncRoot(root)
	}
	return ops
}

// install replaces w's file operations with the hooked set. It must run
// before any tenant worker starts.
func (h *groupHooks) install(w *WAL) {
	w.ops = h.fileOps()
}

func (h *groupHooks) record(event string) {
	h.eventsMu.Lock()
	h.events = append(h.events, event)
	h.eventsMu.Unlock()
}

func (h *groupHooks) eventLog() []string {
	h.eventsMu.Lock()
	defer h.eventsMu.Unlock()
	return slices.Clone(h.events)
}

func (h *groupHooks) unblockAll() {
	for _, gate := range h.gates {
		gate.unblock()
	}
	if h.dirGate != nil {
		h.dirGate.unblock()
	}
}

type hookedFile struct {
	segmentFile
	hooks  *groupHooks
	writes atomic.Int64
}

func (f *hookedFile) Write(data []byte) (int, error) {
	// Count every write, not only the ones issued while failure injection is
	// armed: a segment header written before a test arms failWrites must still
	// consume the first-write exemption, so the next record write fails.
	if f.writes.Add(1) > 1 && f.hooks.failWrites.Load() {
		f.hooks.writeFails.Add(1)
		return 0, f.hooks.writeErr
	}
	return f.segmentFile.Write(data)
}

func (f *hookedFile) Sync() error {
	call := f.hooks.syncCalls.Add(1)
	if gate := f.hooks.gates[call]; gate != nil {
		gate.startOnce.Do(func() { close(gate.started) })
		<-gate.release
	}
	if call == f.hooks.failSyncAt {
		return f.hooks.syncErr
	}
	return f.segmentFile.Sync()
}

func (f *hookedFile) Close() error {
	err := f.segmentFile.Close()
	if f.hooks.failClose.Load() {
		return f.hooks.closeErr
	}
	return err
}

func TestGroupCommitNoEarlyAckAndSharedSync(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate(), 2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default
	log := pauseTenant(w, id)
	receipts := enqueueBatch(t, w, id, 8)
	startTenant(w, log)
	awaitSignal(t, hooks.gates[1].started)

	results := waitAllAsync(receipts)
	assertNoResults(t, results)
	hooks.gates[1].unblock()
	awaitSignal(t, hooks.gates[2].started)
	assertNoResults(t, results)
	hooks.gates[2].unblock()
	for i, result := range results {
		if err := awaitResult(t, result); err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	if got := hooks.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls = %d, want header sync + one batch sync", got)
	}
	for i, receipt := range receipts {
		if receipt.Position.Seq != uint64(i+1) {
			t.Errorf("receipt %d sequence = %d", i, receipt.Position.Seq)
		}
	}
}

func TestGroupCommitWaitCancellationStillReplays(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate(), 2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	dir := t.TempDir()
	w := openHookedWAL(t, dir, Config{}, hooks)
	id := tenant.Default
	log := pauseTenant(w, id)
	receipt := enqueueBatch(t, w, id, 1)[0]
	startTenant(w, log)
	awaitSignal(t, hooks.gates[1].started)
	hooks.gates[1].unblock()
	awaitSignal(t, hooks.gates[2].started)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := receipt.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error = %v, want context.Canceled", err)
	}
	hooks.gates[2].unblock()
	if err := receipt.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(context.Background())
	if got := internalReplayPositions(t, w, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("restart positions = %v, want [1]", got)
	}
}

func TestGroupCommitWriteAndSyncErrors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		failWrites bool
		failSyncAt int64
		wantReplay []uint64
	}{
		{name: "write", failWrites: true, wantReplay: nil},
		{name: "sync", failSyncAt: 2, wantReplay: []uint64{1, 2, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			injected := errors.New("injected " + tt.name + " failure")
			hooks := &groupHooks{writeErr: injected, syncErr: injected, failSyncAt: tt.failSyncAt}
			hooks.failWrites.Store(tt.failWrites)
			dir := t.TempDir()
			w := openHookedWAL(t, dir, Config{}, hooks)
			id := tenant.Default
			log := pauseTenant(w, id)
			receipts := enqueueBatch(t, w, id, 3)
			startTenant(w, log)
			for i, receipt := range receipts {
				if err := receipt.Wait(t.Context()); !errors.Is(err, injected) {
					t.Fatalf("waiter %d error = %v, want injected failure", i, err)
				}
			}
			payload := encodeInternal(t, id)
			if _, err := w.Enqueue(t.Context(), id, payload); !errors.Is(err, injected) {
				t.Fatalf("enqueue after poison error = %v, want injected failure", err)
			}
			if err := w.Close(context.Background()); err != nil {
				t.Fatal(err)
			}

			w, err := Open(t.Context(), dir, Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(context.Background())
			if got := internalReplayPositions(t, w, id, Position{}); !slices.Equal(got, tt.wantReplay) {
				t.Fatalf("restart positions = %v, want %v", got, tt.wantReplay)
			}
			receipt, err := w.Enqueue(t.Context(), id, encodeInternal(t, id))
			if err != nil {
				t.Fatal(err)
			}
			if err := receipt.Wait(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantNext := uint64(len(tt.wantReplay) + 1)
			if receipt.Position.Seq != wantNext {
				t.Fatalf("sequence after restart = %d, want %d", receipt.Position.Seq, wantNext)
			}
		})
	}
}

func TestGroupCommitCloseDrainsQueuedWork(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate(), 2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default
	log := pauseTenant(w, id)
	receipts := enqueueBatch(t, w, id, 5)
	startTenant(w, log)
	awaitSignal(t, hooks.gates[1].started)
	closeResult := make(chan error, 1)
	go func() { closeResult <- w.Close(context.Background()) }()
	awaitClosing(t, w)
	if _, err := w.Enqueue(t.Context(), id, encodeInternal(t, id)); !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue during close error = %v, want ErrClosed", err)
	}
	hooks.gates[1].unblock()
	awaitSignal(t, hooks.gates[2].started)
	hooks.gates[2].unblock()
	if err := awaitResult(t, closeResult); err != nil {
		t.Fatal(err)
	}
	for i, receipt := range receipts {
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatalf("drained waiter %d: %v", i, err)
		}
	}
}

func TestGroupCommitCloseContextAndConcurrentEnqueue(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate()}}
		t.Cleanup(hooks.unblockAll)
		w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
		log := pauseTenant(w, tenant.Default)
		receipt := enqueueBatch(t, w, tenant.Default, 1)[0]
		startTenant(w, log)
		awaitSignal(t, hooks.gates[1].started)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := w.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close error = %v, want context.Canceled", err)
		}
		hooks.unblockAll()
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent enqueue", func(t *testing.T) {
		w, err := Open(t.Context(), t.TempDir(), Config{})
		if err != nil {
			t.Fatal(err)
		}
		payload := encodeInternal(t, tenant.Default)
		var acknowledged atomic.Int64
		var wg sync.WaitGroup
		for range 40 {
			wg.Go(func() {
				receipt, err := w.Enqueue(context.Background(), tenant.Default, payload)
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("concurrent enqueue: %v", err)
					return
				}
				if err := receipt.Wait(context.Background()); err != nil {
					t.Errorf("concurrent wait: %v", err)
					return
				}
				acknowledged.Add(1)
			})
		}
		if err := w.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if err := firstReplayErr(w, tenant.Default); !errors.Is(err, ErrClosed) {
			t.Fatalf("replay on closed WAL error = %v, want ErrClosed", err)
		}
		t.Logf("acknowledged %d of 40 concurrent enqueues before close", acknowledged.Load())
	})
}

func TestWALCloseReportsSegmentCloseError(t *testing.T) {
	injected := errors.New("injected close failure")
	hooks := &groupHooks{closeErr: injected}
	w, err := Open(t.Context(), t.TempDir(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	hooks.install(w)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	id := tenant.Default
	log := pauseTenant(w, id)
	receipt := enqueueBatch(t, w, id, 1)[0]
	startTenant(w, log)
	if err := receipt.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	hooks.failClose.Store(true)
	if err := w.Close(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Close error = %v, want injected close failure", err)
	}
}

func TestGroupCommitSyncErrorDuringConcurrentClose(t *testing.T) {
	injected := errors.New("injected sync failure")
	hooks := &groupHooks{
		syncErr:    injected,
		failSyncAt: 2,
		gates:      map[int64]*syncGate{2: newSyncGate()},
	}
	t.Cleanup(hooks.unblockAll)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default
	log := pauseTenant(w, id)
	receipts := enqueueBatch(t, w, id, 4)
	startTenant(w, log)
	awaitSignal(t, hooks.gates[2].started)

	closeResult := make(chan error, 1)
	go func() { closeResult <- w.Close(context.Background()) }()
	awaitClosing(t, w)
	hooks.gates[2].unblock()

	for i, receipt := range receipts {
		if err := receipt.Wait(t.Context()); !errors.Is(err, injected) {
			t.Fatalf("waiter %d error = %v, want injected failure", i, err)
		}
	}
	if err := awaitResult(t, closeResult); err != nil {
		t.Fatalf("Close after sync failure: %v", err)
	}
}

func TestReplayRejectsQueuedAndInFlightAppends(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate(), 2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default

	log := pauseTenant(w, id)
	receipt := enqueueBatch(t, w, id, 1)[0]
	if err := firstReplayErr(w, id); err == nil {
		t.Fatal("replay with a queued append succeeded")
	}

	startTenant(w, log)
	awaitSignal(t, hooks.gates[1].started)
	hooks.gates[1].unblock()
	awaitSignal(t, hooks.gates[2].started)
	if err := firstReplayErr(w, id); err == nil {
		t.Fatal("replay during in-flight batch succeeded")
	}
	second, err := w.Enqueue(t.Context(), id, encodeInternal(t, id))
	if err != nil {
		t.Fatalf("enqueue during in-flight batch: %v", err)
	}
	log.mu.Lock()
	log.replaying = true
	log.mu.Unlock()
	if _, err := w.Enqueue(t.Context(), id, encodeInternal(t, id)); err == nil {
		t.Fatal("enqueue during replay succeeded")
	}
	log.mu.Lock()
	log.replaying = false
	log.mu.Unlock()

	hooks.gates[2].unblock()
	for i, r := range []Receipt{receipt, second} {
		if err := r.Wait(t.Context()); err != nil {
			t.Fatalf("receipt %d: %v", i, err)
		}
	}
	if got := internalReplayPositions(t, w, id, Position{}); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("replay positions = %v, want [1 2]", got)
	}
}

func TestReclaimSyncsEachRemovalAndRetriesAfterFailure(t *testing.T) {
	injected := errors.New("injected dir sync failure")
	hooks := &groupHooks{syncDirErr: injected}
	hooks.failSyncDir.Store(true)
	dir := t.TempDir()
	cfg := Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220}
	w := openHookedWAL(t, dir, cfg, hooks)
	id := tenant.ID("reclaim")
	for range 4 {
		appendInternal(t, w, id)
	}

	// Records 1-3 sit in their own sealed segments; record 4 is active. The
	// first directory sync fails, so the first removal stays unpersisted.
	frontier := Position{Tenant: id, Seq: 3}
	if err := w.Reclaim(t.Context(), id, frontier); !errors.Is(err, injected) {
		t.Fatalf("reclaim error = %v, want injected failure", err)
	}
	if got := len(segmentPaths(t, dir, id)); got != 3 {
		t.Fatalf("segments after failed reclaim = %d, want 3", got)
	}

	// The retry first flushes the pending deletion, then reclaims the rest.
	if err := w.Reclaim(t.Context(), id, frontier); err != nil {
		t.Fatalf("reclaim retry: %v", err)
	}
	wantEvents := []string{
		"remove " + segmentName(1), "syncdir",
		"syncdir",
		"remove " + segmentName(2), "syncdir",
		"remove " + segmentName(3), "syncdir",
	}
	if got := hooks.eventLog(); !slices.Equal(got, wantEvents) {
		t.Fatalf("reclaim event order = %v, want %v", got, wantEvents)
	}
	if got := len(segmentPaths(t, dir, id)); got != 1 {
		t.Fatalf("segments after retry = %d, want 1", got)
	}
}

func TestReclaimFlushesPendingSyncOnZeroRemovalRetry(t *testing.T) {
	injected := errors.New("injected dir sync failure")
	hooks := &groupHooks{syncDirErr: injected}
	dir := t.TempDir()
	cfg := Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220}
	w := openHookedWAL(t, dir, cfg, hooks)
	id := tenant.ID("reclaim")
	for range 2 {
		appendInternal(t, w, id)
	}

	// The only covered segment is removed but its directory sync fails.
	hooks.failSyncDir.Store(true)
	frontier := Position{Tenant: id, Seq: 1}
	if err := w.Reclaim(t.Context(), id, frontier); !errors.Is(err, injected) {
		t.Fatalf("reclaim error = %v, want injected failure", err)
	}
	// The retry has nothing left to remove but must still flush the pending
	// deletion before returning.
	if err := w.Reclaim(t.Context(), id, frontier); err != nil {
		t.Fatalf("zero-removal retry: %v", err)
	}
	// Once drained, another no-op reclaim does not re-sync.
	if err := w.Reclaim(t.Context(), id, frontier); err != nil {
		t.Fatalf("clean retry: %v", err)
	}
	wantEvents := []string{"remove " + segmentName(1), "syncdir", "syncdir"}
	if got := hooks.eventLog(); !slices.Equal(got, wantEvents) {
		t.Fatalf("reclaim events = %v, want %v", got, wantEvents)
	}
	if got := len(segmentPaths(t, dir, id)); got != 1 {
		t.Fatalf("segments after retry = %d, want 1", got)
	}
}

func TestReclaimRejectsConcurrentReclaimAndReplay(t *testing.T) {
	hooks := &groupHooks{dirGate: newSyncGate()}
	t.Cleanup(hooks.unblockAll)
	dir := t.TempDir()
	cfg := Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220}
	w := openHookedWAL(t, dir, cfg, hooks)
	id := tenant.ID("reclaim")
	for range 3 {
		appendInternal(t, w, id)
	}

	// Records 1-2 sit in their own sealed segments; record 3 is active. The
	// first directory sync is gated, so the reclaim holds the tenant's reclaim
	// slot mid-prefix.
	frontier := Position{Tenant: id, Seq: 2}
	reclaimResult := make(chan error, 1)
	go func() { reclaimResult <- w.Reclaim(context.Background(), id, frontier) }()
	awaitSignal(t, hooks.dirGate.started)

	if err := w.Reclaim(t.Context(), id, frontier); err == nil {
		t.Fatal("concurrent reclaim succeeded")
	}
	if err := firstReplayErr(w, id); err == nil {
		t.Fatal("replay during reclaim succeeded")
	}

	// Enqueue stays independent of reclaim: the append is written and synced
	// while the gated directory fsync is still blocked.
	if pos := appendInternal(t, w, id); pos.Seq != 4 {
		t.Fatalf("append during reclaim sequence = %d, want 4", pos.Seq)
	}

	hooks.dirGate.unblock()
	if err := awaitResult(t, reclaimResult); err != nil {
		t.Fatalf("gated reclaim: %v", err)
	}
	if got := internalReplayPositions(t, w, id, frontier); !slices.Equal(got, []uint64{3, 4}) {
		t.Fatalf("replay after reclaim = %v, want [3 4]", got)
	}
}

func TestOnSyncObservesSegmentSyncs(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var mu sync.Mutex
		var errs []error
		var durations []time.Duration
		cfg := Config{OnSync: func(d time.Duration, err error) {
			mu.Lock()
			durations = append(durations, d)
			errs = append(errs, err)
			mu.Unlock()
		}}
		w := openHookedWAL(t, t.TempDir(), cfg, &groupHooks{})
		receipt := enqueueBatch(t, w, tenant.Default, 1)[0]
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		// Segment header initialization plus the batch commit sync.
		if len(errs) != 2 {
			t.Fatalf("OnSync calls = %d, want 2", len(errs))
		}
		for i, err := range errs {
			if err != nil {
				t.Fatalf("OnSync call %d error = %v, want nil", i, err)
			}
			if durations[i] <= 0 {
				t.Fatalf("OnSync call %d duration = %v, want positive", i, durations[i])
			}
		}
	})

	t.Run("error", func(t *testing.T) {
		injected := errors.New("injected sync failure")
		var mu sync.Mutex
		var errs []error
		hooks := &groupHooks{syncErr: injected, failSyncAt: 2}
		cfg := Config{OnSync: func(_ time.Duration, err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}}
		w := openHookedWAL(t, t.TempDir(), cfg, hooks)
		receipt := enqueueBatch(t, w, tenant.Default, 1)[0]
		if err := receipt.Wait(t.Context()); !errors.Is(err, injected) {
			t.Fatalf("wait error = %v, want injected failure", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(errs) != 2 || errs[0] != nil || !errors.Is(errs[1], injected) {
			t.Fatalf("OnSync errors = %v, want [nil injected]", errs)
		}
	})
}

func openHookedWAL(t *testing.T, dir string, cfg Config, hooks *groupHooks) *WAL {
	t.Helper()
	w, err := Open(t.Context(), dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hooks.install(w)
	t.Cleanup(func() {
		hooks.unblockAll()
		if err := w.Close(context.Background()); err != nil {
			t.Errorf("close hooked WAL: %v", err)
		}
	})
	return w
}

func pauseTenant(w *WAL, id tenant.ID) *tenantLog {
	log := newTenantLog(id, nil, 1)
	w.mu.Lock()
	w.logs[id] = log
	w.mu.Unlock()
	return log
}

func startTenant(w *WAL, log *tenantLog) {
	w.wg.Go(func() { log.run(w) })
}

func enqueueBatch(t *testing.T, w *WAL, id tenant.ID, count int) []Receipt {
	t.Helper()
	payload := encodeInternal(t, id)
	receipts := make([]Receipt, count)
	for i := range count {
		receipt, err := w.Enqueue(t.Context(), id, payload)
		if err != nil {
			t.Fatal(err)
		}
		receipts[i] = receipt
	}
	return receipts
}

func encodeInternal(t *testing.T, id tenant.ID) Payload {
	t.Helper()
	payload, err := Encode(tenant.With(t.Context(), id), logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "job", Value: "group"}),
		Entries: []logs.Entry{{Timestamp: time.Unix(1, 2), Line: "group commit"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func waitAllAsync(receipts []Receipt) []chan error {
	results := make([]chan error, len(receipts))
	for i, receipt := range receipts {
		result := make(chan error, 1)
		go func() { result <- receipt.Wait(context.Background()) }()
		results[i] = result
	}
	return results
}

func assertNoResults(t *testing.T, results []chan error) {
	t.Helper()
	for i, result := range results {
		select {
		case err := <-result:
			t.Fatalf("waiter %d returned before sync release: %v", i, err)
		default:
		}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for controlled file operation")
	}
}

func awaitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for WAL result")
		return nil
	}
}

func internalReplayPositions(t *testing.T, w *WAL, id tenant.ID, after Position) []uint64 {
	t.Helper()
	var positions []uint64
	for record, err := range w.Replay(t.Context(), id, after) {
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, record.Position.Seq)
	}
	return positions
}

func firstReplayErr(w *WAL, id tenant.ID) error {
	for _, err := range w.Replay(context.Background(), id, Position{}) {
		return err
	}
	return nil
}

func awaitClosing(t *testing.T, w *WAL) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		closing := w.closing
		w.mu.Unlock()
		if closing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for WAL close to begin")
		}
		time.Sleep(time.Millisecond)
	}
}
