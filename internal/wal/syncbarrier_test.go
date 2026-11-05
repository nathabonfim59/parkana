package wal

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// syncAsync runs WAL.Sync in the background and returns its result.
func syncAsync(w *WAL, ctx context.Context) chan error {
	result := make(chan error, 1)
	go func() { result <- w.Sync(ctx) }()
	return result
}

// awaitQueueBarrier waits until log's queue holds count requests whose last
// one is a barrier, so a test can prove the barrier was admitted before the
// worker starts. It observes only in-memory state under the tenant lock.
func awaitQueueBarrier(t *testing.T, log *tenantLog, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		log.mu.Lock()
		ok := len(log.queue) == count && log.queue[count-1].barrier
		log.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			log.mu.Lock()
			got := len(log.queue)
			log.mu.Unlock()
			t.Fatalf("queue holds %d requests, want %d ending in a barrier", got, count)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSyncBarrierCleanByDefault proves Sync on a tenant-less WAL is a no-op,
// and that an always-mode tenant with nothing dirty passes a clean barrier
// without issuing a redundant fsync.
func TestSyncBarrierCleanByDefault(t *testing.T) {
	t.Run("zero tenants", func(t *testing.T) {
		w := openHookedWAL(t, t.TempDir(), Config{}, &groupHooks{})
		if err := w.Sync(t.Context()); err != nil {
			t.Fatalf("Sync with no tenants: %v", err)
		}
	})

	t.Run("clean always mode", func(t *testing.T) {
		hooks := &groupHooks{}
		w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
		id := tenant.Default
		receipt := enqueueLong(t, w, id)
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		before := hooks.syncCalls.Load() // header + batch sync
		if err := w.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if after := hooks.syncCalls.Load(); after != before {
			t.Fatalf("clean barrier sync calls = %d, want unchanged %d", after, before)
		}
	})
}

// TestSyncBarrierForcesIdlePending proves Sync fsyncs a written-but-unsynced
// interval-mode record with no timer tick, completes its durable receipt and
// reports the fsync to OnSync.
func TestSyncBarrierForcesIdlePending(t *testing.T) {
	var mu sync.Mutex
	var syncErrs []error
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	cfg := Config{OnSync: func(_ time.Duration, err error) {
		mu.Lock()
		syncErrs = append(syncErrs, err)
		mu.Unlock()
	}}
	w := openIntervalWAL(t, t.TempDir(), cfg, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	waiting := waitAllAsync([]Receipt{receipt})[0]
	assertNoResults(t, []chan error{waiting})

	if err := w.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, waiting); err != nil {
		t.Fatalf("Sync did not make the pending record durable: %v", err)
	}
	if got := hooks.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls = %d, want header + barrier", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(syncErrs, []error{nil, nil}) {
		t.Fatalf("OnSync errors = %v, want two successful syncs", syncErrs)
	}
}

// TestSyncBarrierBehindBusyBatch proves a barrier queued behind an in-flight
// batch runs after it, in queue order, and covers its records.
func TestSyncBarrierBehindBusyBatch(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	awaitSignal(t, hooks.gates[1].started) // worker holds the header sync

	result := syncAsync(w, context.Background())
	hooks.gates[1].unblock()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := receipt.Wait(t.Context()); err != nil {
		t.Fatalf("barrier behind busy batch did not cover it: %v", err)
	}
	if got := hooks.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls = %d, want header + barrier", got)
	}
}

// TestSyncBarrierTwoTenantsBlockedIndependence proves every tenant barrier is
// enqueued before any is waited for: while tenant a's barrier fsync is held,
// tenant b's barrier runs to completion and makes its records durable.
// Determinism: sync 1 is a's header; b's worker is held in its header sync
// (call 2), so sync 3 is necessarily a's barrier and sync 4 b's.
func TestSyncBarrierTwoTenantsBlockedIndependence(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate(), 3: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	a, b := tenant.ID("aaa"), tenant.ID("bbb")

	ra := enqueueLong(t, w, a) // sync 1: a header
	if err := ra.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	rb := enqueueLong(t, w, b)
	awaitSignal(t, hooks.gates[2].started) // b's worker holds its header sync

	result := syncAsync(w, context.Background())
	awaitSignal(t, hooks.gates[3].started) // sync 3: a's barrier, held
	hooks.gates[2].unblock()               // b proceeds: batch, then its barrier

	bDurable := waitAllAsync([]Receipt{rb})[0]
	if err := awaitResult(t, bDurable); err != nil {
		t.Fatalf("tenant b barrier blocked behind tenant a: %v", err)
	}
	aDurable := waitAllAsync([]Receipt{ra})[0]
	assertNoResults(t, []chan error{aDurable})

	hooks.gates[3].unblock()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, aDurable); err != nil {
		t.Fatal(err)
	}
	if got := hooks.syncCalls.Load(); got != 4 {
		t.Fatalf("sync calls = %d, want a/b headers + a/b barriers", got)
	}
}

// TestSyncBarrierFailureIsSticky proves a failed barrier fsync poisons the
// tenant like a timer failure: the barrier and pending receipts fail, later
// enqueues and later Sync calls are rejected with the same error, and nothing
// is stranded.
func TestSyncBarrierFailureIsSticky(t *testing.T) {
	injected := errors.New("injected barrier sync failure")
	hooks := &groupHooks{syncErr: injected, failSyncAt: 2}
	dir := t.TempDir()
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("Sync error = %v, want injected failure", err)
	}
	if err := receipt.Wait(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("pending receipt after barrier failure = %v, want injected failure", err)
	}
	if _, err := w.Enqueue(t.Context(), id, encodeLong(t, id)); !errors.Is(err, injected) {
		t.Fatalf("enqueue after barrier poison = %v, want injected failure", err)
	}
	if err := w.Sync(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("Sync after barrier poison = %v, want injected failure", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after barrier poison = %v, want [1]", got)
	}
}

// TestSyncBarrierCancellation proves cancellation only stops the caller's
// wait: an already-canceled ctx returns before any barrier is enqueued, and a
// canceled in-flight wait still leaves the admitted barrier to complete.
func TestSyncBarrierCancellation(t *testing.T) {
	t.Run("already canceled zero tenants", func(t *testing.T) {
		w := openHookedWAL(t, t.TempDir(), Config{}, &groupHooks{})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := w.Sync(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Sync with canceled ctx = %v, want context.Canceled", err)
		}
	})

	t.Run("already canceled existing tenant", func(t *testing.T) {
		hooks := &groupHooks{}
		rec := &tickerRecorder{}
		w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
		id := tenant.Default
		receipt := enqueueLong(t, w, id)
		if err := receipt.Written(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := w.Sync(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Sync with canceled ctx = %v, want context.Canceled", err)
		}
		if got := hooks.syncCalls.Load(); got != 1 {
			t.Fatalf("canceled Sync issued syncs: %d, want header only", got)
		}
		// The tenant is untouched: the normal timer still owns durability.
		rec.get(0).fire()
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("canceled wait completes barrier", func(t *testing.T) {
		hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
		t.Cleanup(hooks.unblockAll)
		rec := &tickerRecorder{}
		w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
		id := tenant.Default
		receipt := enqueueLong(t, w, id)
		if err := receipt.Written(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := syncAsync(w, ctx)
		awaitSignal(t, hooks.gates[2].started) // barrier fsync held
		cancel()
		if err := awaitResult(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Sync = %v, want context.Canceled", err)
		}
		hooks.gates[2].unblock()
		// The admitted barrier still finishes: no tick is ever fired.
		if err := receipt.Wait(context.Background()); err != nil {
			t.Fatalf("admitted barrier did not complete after cancel: %v", err)
		}
	})
}

// TestSyncBarrierAdmittedBeforeOnly proves the barrier covers exactly the
// records admitted before it: a record enqueued while the barrier fsync is
// held stays pending until its own sync point.
func TestSyncBarrierAdmittedBeforeOnly(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	first := enqueueLong(t, w, id)
	if err := first.Written(t.Context()); err != nil {
		t.Fatal(err)
	}

	result := syncAsync(w, context.Background())
	awaitSignal(t, hooks.gates[2].started) // barrier fsync held
	second := enqueueLong(t, w, id)        // admitted after the barrier
	hooks.gates[2].unblock()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := second.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	secondDurable := waitAllAsync([]Receipt{second})[0]
	assertNoResults(t, []chan error{secondDurable})
	rec.get(0).fire()
	if err := awaitResult(t, secondDurable); err != nil {
		t.Fatal(err)
	}
}

// TestSyncBarrierQueuedBehindRecordDrains proves a barrier detached together
// with a successful batch is still processed: with the worker paused, a
// record and then a Sync barrier are admitted (observed under the tenant
// lock), so the worker's first snapshot cuts [record | barrier]. The batch
// succeeds and the barrier must run before the loop continues — Sync returns
// and the record turns durable with no timer tick.
func TestSyncBarrierQueuedBehindRecordDrains(t *testing.T) {
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	log := pauseTenant(w, id)
	receipt := enqueueLong(t, w, id)
	result := syncAsync(w, context.Background())
	awaitQueueBarrier(t, log, 2) // [record, barrier], worker still paused
	startTenant(w, log)

	if err := awaitResult(t, result); err != nil {
		t.Fatalf("Sync with barrier behind a successful batch: %v", err)
	}
	if err := receipt.Wait(context.Background()); err != nil {
		t.Fatalf("record behind drained barrier not durable: %v", err)
	}
	// Durability came from the barrier fsync alone: the manual ticker is
	// never fired in this test, and interval batches never fsync.
	if got := hooks.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls = %d, want header + barrier", got)
	}
}

// TestSyncBarrierExcludesReplay proves a head barrier detached from the queue
// keeps the tenant's in-flight exclusion: replay is rejected while the
// barrier fsync runs, exactly as it is during a batch, and works again once
// the barrier completes.
func TestSyncBarrierExcludesReplay(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}

	result := syncAsync(w, context.Background())
	awaitSignal(t, hooks.gates[2].started) // worker holds the barrier fsync
	if err := firstReplayErr(w, id); err == nil {
		t.Fatal("replay admitted while a barrier sync was in flight")
	}
	hooks.gates[2].unblock()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if got := internalReplayPositions(t, w, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after barrier = %v, want [1]", got)
	}
}

// TestWritingAtomicWithHeadBarrierDequeue proves the in-flight exclusion is
// set in the same critical section as the dequeue: replaying immediately
// after a head barrier leaves the queue must be rejected. The afterDequeue
// seam runs between the snapshot and the barrier work, exactly where a
// non-atomic writing assignment would leave a gap.
func TestWritingAtomicWithHeadBarrierDequeue(t *testing.T) {
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	log := pauseTenant(w, id)
	result := syncAsync(w, context.Background()) // barrier into the empty queue
	awaitQueueBarrier(t, log, 1)
	admitted := make(chan bool, 1)
	log.afterDequeue = func() {
		if err := log.beginReplay(); err != nil {
			admitted <- false
			return
		}
		log.endReplay()
		admitted <- true
	}
	startTenant(w, log)
	if <-admitted {
		t.Fatal("replay admitted after a head barrier left the queue with writing unset")
	}
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := firstReplayErr(w, id); err != nil {
		t.Fatalf("replay after completed barrier: %v", err)
	}
}

// TestIntervalRotationFlushSurvivesLaterWriteFailure proves receipts made
// durable by a completed rotation seal are not lost when a later write in the
// same batch fails: the first record's Wait completes nil from the seal, the
// failing record never acknowledges, and nothing strands.
func TestIntervalRotationFlushSurvivesLaterWriteFailure(t *testing.T) {
	injected := errors.New("injected post-rotation write failure")
	hooks := &groupHooks{writeErr: injected, gates: map[int64]*syncGate{3: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	dir := t.TempDir()
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{SegmentMaxBytes: 300}, hooks, rec)
	id := tenant.Default

	first := enqueueLong(t, w, id) // sync 1: first segment header
	if err := first.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstDurable := waitAllAsync([]Receipt{first})[0]
	assertNoResults(t, []chan error{firstDurable})

	second := enqueueLong(t, w, id) // forces rotation: sync 2 seals (covers first), sync 3 gates the new segment header
	awaitSignal(t, hooks.gates[3].started)
	hooks.failWrites.Store(true) // the next write on the new segment fails
	hooks.gates[3].unblock()

	if err := awaitResult(t, firstDurable); err != nil {
		t.Fatalf("rotation-durable receipt stranded by later write failure: %v", err)
	}
	if err := second.Written(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("failing record Written = %v, want injected failure", err)
	}
	if err := second.Wait(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("failing record Wait = %v, want injected failure", err)
	}
	if got := hooks.writeFails.Load(); got != 1 {
		t.Fatalf("injected write failures = %d, want exactly the failing record once", got)
	}
	if _, err := w.Enqueue(t.Context(), id, encodeLong(t, id)); !errors.Is(err, injected) {
		t.Fatalf("enqueue after poison = %v, want injected failure", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{SegmentMaxBytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after rotation failure = %v, want [1]", got)
	}
}

// TestSyncBarrierRacesPoisonNoStrand proves a barrier queued behind a batch
// whose write then fails is swept by the poison with an outcome, so Sync
// returns instead of stranding a waiter.
func TestSyncBarrierRacesPoisonNoStrand(t *testing.T) {
	injected := errors.New("injected batch write failure")
	hooks := &groupHooks{writeErr: injected}
	hooks.failWrites.Store(true)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default
	log := pauseTenant(w, id)
	if _, err := w.Enqueue(t.Context(), id, encodeInternal(t, id)); err != nil {
		t.Fatal(err)
	}
	result := syncAsync(w, context.Background())
	awaitQueueBarrier(t, log, 2) // barrier provably admitted before the worker starts
	startTenant(w, log)          // the batch write fails and poisons
	if err := awaitResult(t, result); !errors.Is(err, injected) {
		t.Fatalf("Sync racing poison = %v, want injected failure", err)
	}
	if _, err := w.Enqueue(t.Context(), id, encodeInternal(t, id)); !errors.Is(err, injected) {
		t.Fatalf("enqueue after poison = %v, want injected failure", err)
	}
}

// TestSyncBarrierRacesCloseDrainsBarrier proves a barrier admitted before
// Close begins is drained and synced by the exiting worker — both Sync and
// Close return cleanly and the record replays — while Sync after Close is
// rejected with ErrClosed.
func TestSyncBarrierRacesCloseDrainsBarrier(t *testing.T) {
	dir := t.TempDir()
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}

	result := syncAsync(w, context.Background())
	awaitSignal(t, hooks.gates[2].started) // barrier fsync held mid-close window
	closing := make(chan error, 1)
	go func() { closing <- w.Close(context.Background()) }()
	awaitClosing(t, w) // closing is set before the held fsync is released
	hooks.gates[2].unblock()
	if err := awaitResult(t, result); err != nil {
		t.Fatalf("Sync racing close: %v", err)
	}
	if err := awaitResult(t, closing); err != nil {
		t.Fatalf("Close with drained barrier: %v", err)
	}
	if err := w.Sync(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after close = %v, want ErrClosed", err)
	}

	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after barrier drain = %v, want [1]", got)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 2 {
		t.Fatalf("append after barrier drain sequence = %d, want 2", pos.Seq)
	}
}
