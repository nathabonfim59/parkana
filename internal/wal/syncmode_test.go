package wal

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// manualTicker is a test ticker whose ticks the test fires by hand, so timer
// behavior is asserted on events instead of sleeps.
type manualTicker struct {
	c    chan time.Time
	stop chan struct{}
	once sync.Once
}

func newManualTicker() *manualTicker {
	return &manualTicker{c: make(chan time.Time, 1), stop: make(chan struct{})}
}

func (t *manualTicker) C() <-chan time.Time { return t.c }

func (t *manualTicker) Stop() { t.once.Do(func() { close(t.stop) }) }

// fire queues one tick. A tick that was never consumed is dropped, matching
// time.Ticker's buffered drop.
func (t *manualTicker) fire() {
	select {
	case t.c <- time.Now():
	default:
	}
}

// tickerRecorder hands out manual tickers in worker start order, so a test
// that awaits one tenant's first write receipt before enqueuing for the next
// can address each tenant's ticker deterministically.
type tickerRecorder struct {
	mu      sync.Mutex
	tickers []*manualTicker
}

func (r *tickerRecorder) factory(d time.Duration) ticker {
	_ = d // the period is irrelevant: ticks are fired by the test
	t := newManualTicker()
	r.mu.Lock()
	r.tickers = append(r.tickers, t)
	r.mu.Unlock()
	return t
}

func (r *tickerRecorder) get(i int) *manualTicker {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tickers[i]
}

// openIntervalWAL opens a SyncInterval-mode WAL with hooked file operations
// and manual tickers.
func openIntervalWAL(t *testing.T, dir string, cfg Config, hooks *groupHooks, rec *tickerRecorder) *WAL {
	t.Helper()
	ops := hooks.fileOps()
	ops.newTicker = rec.factory
	cfg.SyncMode = SyncInterval
	w, err := open(t.Context(), dir, cfg, ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		hooks.unblockAll()
		if err := w.Close(context.Background()); err != nil {
			t.Errorf("close interval WAL: %v", err)
		}
	})
	return w
}

// writeAsync awaits a receipt's write completion in the background.
func writeAsync(receipt Receipt) chan error {
	result := make(chan error, 1)
	go func() { result <- receipt.Written(context.Background()) }()
	return result
}

// encodeLong encodes a one-entry payload long enough that a second record
// forces rotation at SegmentMaxBytes 300.
func encodeLong(t *testing.T, id tenant.ID) Payload {
	t.Helper()
	payload, err := Encode(tenant.With(t.Context(), id), logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "job", Value: "syncmode"}),
		Entries: []logs.Entry{{Line: strings.Repeat("x", 200)}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// enqueueLong encodes and enqueues one long record and returns its receipt.
func enqueueLong(t *testing.T, w *WAL, id tenant.ID) Receipt {
	t.Helper()
	receipt, err := w.Enqueue(t.Context(), id, encodeLong(t, id))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func assertPending(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("receipt completed before its durability point: %v", err)
	default:
	}
}

func awaitCompleted(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for receipt completion")
		return nil
	}
}

func TestSyncModeDefaultsAndParsing(t *testing.T) {
	def, err := Config{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if def.SyncMode != SyncAlways {
		t.Fatalf("zero config sync mode = %v, want always", def.SyncMode)
	}
	interval, err := Config{SyncMode: SyncInterval}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if interval.SyncInterval != defaultSyncInterval {
		t.Fatalf("interval default = %v, want %v", interval.SyncInterval, defaultSyncInterval)
	}
	if _, err := (Config{SyncInterval: -time.Second}).withDefaults(); err == nil {
		t.Fatal("negative sync interval accepted")
	}
	if _, err := (Config{SyncMode: SyncMode(9)}).withDefaults(); err == nil {
		t.Fatal("unknown sync mode accepted")
	}
	for _, tt := range []struct {
		value string
		want  SyncMode
		fails bool
	}{
		{value: "always", want: SyncAlways},
		{value: "interval", want: SyncInterval},
		{value: "", fails: true},
		{value: "sometimes", fails: true},
	} {
		got, err := ParseSyncMode(tt.value)
		if tt.fails {
			if err == nil {
				t.Fatalf("ParseSyncMode(%q) accepted", tt.value)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Fatalf("ParseSyncMode(%q) = %v, %v; want %v", tt.value, got, err, tt.want)
		}
		if got.String() != tt.value {
			t.Fatalf("SyncMode.String() = %q, want %q", got.String(), tt.value)
		}
	}
}

// TestSyncAlwaysBlockedFsyncBlocksWaitNotWritten proves the always-mode
// durable receipt still follows the fsync, while the write receipt is already
// complete while the fsync is blocked.
func TestSyncAlwaysBlockedFsyncBlocksWaitNotWritten(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	w := openHookedWAL(t, t.TempDir(), Config{}, hooks)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	awaitSignal(t, hooks.gates[2].started)

	waiting := waitAllAsync([]Receipt{receipt})[0]
	assertNoResults(t, []chan error{waiting})
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatalf("Written while fsync blocked: %v", err)
	}
	hooks.gates[2].unblock()
	if err := awaitResult(t, waiting); err != nil {
		t.Fatal(err)
	}
}

// TestSyncIntervalWrittenBeforeSyncAndWaitAfterTick proves the interval-mode
// write receipt completes without any fsync of the record, and the durable
// receipt completes only when the timer's sync finishes.
func TestSyncIntervalWrittenBeforeSyncAndWaitAfterTick(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	var mu sync.Mutex
	var syncErrs []error
	cfg := Config{OnSync: func(_ time.Duration, err error) {
		mu.Lock()
		syncErrs = append(syncErrs, err)
		mu.Unlock()
	}}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), cfg, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := hooks.syncCalls.Load(); got != 1 {
		t.Fatalf("sync calls after write = %d, want header only", got)
	}
	waiting := waitAllAsync([]Receipt{receipt})[0]
	assertNoResults(t, []chan error{waiting})

	rec.get(0).fire()
	awaitSignal(t, hooks.gates[2].started)
	assertNoResults(t, []chan error{waiting})
	hooks.gates[2].unblock()
	if err := awaitResult(t, waiting); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(syncErrs, []error{nil, nil}) {
		t.Fatalf("OnSync errors = %v, want two successful syncs", syncErrs)
	}
}

// TestSyncIntervalBoundarySyncsBusyTenant proves a tick that arrives while a
// batch is writing makes the sync due at the batch boundary, before the next
// queued batch is written: a continuously busy tenant cannot starve its
// durability timer.
func TestSyncIntervalBoundarySyncsBusyTenant(t *testing.T) {
	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate(), 2: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default

	first := enqueueLong(t, w, id)
	awaitSignal(t, hooks.gates[1].started)
	rec.get(0).fire() // arrives while the worker sits in the header sync
	second := enqueueLong(t, w, id)
	writing := writeAsync(second)
	hooks.gates[1].unblock()

	awaitSignal(t, hooks.gates[2].started)
	assertPending(t, writing) // boundary sync runs before the next batch
	if err := first.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	waiting := waitAllAsync([]Receipt{first})[0]
	assertPending(t, waiting)
	hooks.gates[2].unblock()
	if err := awaitResult(t, waiting); err != nil {
		t.Fatal(err)
	}

	if err := awaitCompleted(t, writing); err != nil {
		t.Fatal(err)
	}
	rec.get(0).fire()
	if err := second.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := hooks.syncCalls.Load(); got != 3 {
		t.Fatalf("sync calls = %d, want header + boundary + tick", got)
	}
}

// TestSyncIntervalRotationSealSyncsPending proves a rotation's seal fsync is a
// durability point: earlier pending receipts complete without any timer tick,
// while the post-rotation record still waits for one.
func TestSyncIntervalRotationSealSyncsPending(t *testing.T) {
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{SegmentMaxBytes: 300}, hooks, rec)
	id := tenant.Default

	first := enqueueLong(t, w, id)
	if err := first.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := enqueueLong(t, w, id) // forces rotation
	if err := second.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(t.Context()); err != nil {
		t.Fatalf("rotation did not make the first record durable: %v", err)
	}
	waiting := waitAllAsync([]Receipt{second})[0]
	assertNoResults(t, []chan error{waiting})

	rec.get(0).fire()
	if err := awaitResult(t, waiting); err != nil {
		t.Fatal(err)
	}
	if got := hooks.syncCalls.Load(); got != 4 {
		t.Fatalf("sync calls = %d, want header + seal + header + tick", got)
	}
}

// TestSyncIntervalCloseSyncsDirty proves an orderly close forces the pending
// sync: the durable receipt completes at close and the record replays after
// reopen.
func TestSyncIntervalCloseSyncsDirty(t *testing.T) {
	dir := t.TempDir()
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	waiting := waitAllAsync([]Receipt{receipt})[0]
	assertNoResults(t, []chan error{waiting})

	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, waiting); err != nil {
		t.Fatalf("close did not complete the durable receipt: %v", err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after close sync = %v, want [1]", got)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 2 {
		t.Fatalf("append after reopen sequence = %d, want 2", pos.Seq)
	}
}

// TestSyncIntervalCloseSyncFailureFailsWaitersAndClose proves a failed close
// sync never acknowledges durability and surfaces through Close. The written
// record itself stays readable in this simulation: a failed sync is unknown
// durability, not a truncation.
func TestSyncIntervalCloseSyncFailureFailsWaitersAndClose(t *testing.T) {
	injected := errors.New("injected close sync failure")
	hooks := &groupHooks{syncErr: injected, failSyncAt: 2}
	dir := t.TempDir()
	rec := &tickerRecorder{}
	ops := hooks.fileOps()
	ops.newTicker = rec.factory
	w, err := open(t.Context(), dir, Config{SyncMode: SyncInterval}, ops)
	if err != nil {
		t.Fatal(err)
	}
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Close error = %v, want injected failure", err)
	}
	if err := receipt.Wait(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Wait after failed close sync = %v, want injected failure", err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after failed close sync = %v, want [1]", got)
	}
}

// TestSyncIntervalSyncFailureIsSticky proves a timer sync failure poisons the
// tenant: every pending receipt fails, later enqueues are rejected with the
// same error, and the data written before the failure still replays.
func TestSyncIntervalSyncFailureIsSticky(t *testing.T) {
	injected := errors.New("injected tick sync failure")
	hooks := &groupHooks{syncErr: injected, failSyncAt: 2}
	dir := t.TempDir()
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	// Two batches, each written and parked as pending before the tick that
	// would sync them: the poison must fail both durable receipts.
	receipts := []Receipt{enqueueLong(t, w, id)}
	if err := receipts[0].Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	receipts = append(receipts, enqueueLong(t, w, id))
	if err := receipts[1].Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	rec.get(0).fire()
	for i, receipt := range receipts {
		if err := receipt.Wait(t.Context()); !errors.Is(err, injected) {
			t.Fatalf("pending receipt %d after failed tick sync = %v, want injected failure", i, err)
		}
	}
	if _, err := w.Enqueue(t.Context(), id, encodeLong(t, id)); !errors.Is(err, injected) {
		t.Fatalf("enqueue after poison = %v, want injected failure", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("replay after poison = %v, want [1 2]", got)
	}
}

// TestSyncIntervalWriteFailureNeverAcknowledges proves a failed write fails
// both the write and the durable receipt, and poisons the tenant.
func TestSyncIntervalWriteFailureNeverAcknowledges(t *testing.T) {
	injected := errors.New("injected interval write failure")
	hooks := &groupHooks{writeErr: injected}
	hooks.failWrites.Store(true)
	dir := t.TempDir()
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("Written after write failure = %v, want injected failure", err)
	}
	if err := receipt.Wait(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("Wait after write failure = %v, want injected failure", err)
	}
	if _, err := w.Enqueue(t.Context(), id, encodeLong(t, id)); !errors.Is(err, injected) {
		t.Fatalf("enqueue after write failure = %v, want injected failure", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); got != nil {
		t.Fatalf("replay after write failure = %v, want none", got)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 1 {
		t.Fatalf("append after write failure sequence = %d, want 1", pos.Seq)
	}
}

// TestSyncIntervalTenantsIndependent proves tickers, syncs, and poison are
// per tenant: while one tenant's timer sync is blocked or failed, the other
// keeps writing and syncing, and a poisoned tenant never stops its neighbor.
func TestSyncIntervalTenantsIndependent(t *testing.T) {
	injected := errors.New("injected tenant a sync failure")
	hooks := &groupHooks{syncErr: injected, failSyncAt: 5, gates: map[int64]*syncGate{3: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	a, b := tenant.ID("aaa"), tenant.ID("bbb")

	ra1 := enqueueLong(t, w, a)
	if err := ra1.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	rb1 := enqueueLong(t, w, b)
	if err := rb1.Written(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Tenant a's timer sync blocks mid-call. Tenant b keeps admitting,
	// writing and syncing to completion while it is held.
	rec.get(0).fire() // sync 3: tenant a tick
	awaitSignal(t, hooks.gates[3].started)
	aWaiting := waitAllAsync([]Receipt{ra1})[0]
	assertNoResults(t, []chan error{aWaiting})
	rb2 := enqueueLong(t, w, b)
	if err := rb2.Written(t.Context()); err != nil {
		t.Fatalf("tenant b write while tenant a sync blocked: %v", err)
	}
	rec.get(1).fire() // sync 4: tenant b tick
	if err := rb1.Wait(t.Context()); err != nil {
		t.Fatalf("tenant b durable while tenant a sync blocked: %v", err)
	}
	if err := rb2.Wait(t.Context()); err != nil {
		t.Fatalf("tenant b durable while tenant a sync blocked: %v", err)
	}
	hooks.gates[3].unblock()
	if err := awaitResult(t, aWaiting); err != nil {
		t.Fatal(err)
	}

	ra2 := enqueueLong(t, w, a)
	if err := ra2.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	rec.get(0).fire() // sync 5: tenant a tick fails
	if err := ra2.Wait(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("tenant a wait after failed sync = %v, want injected failure", err)
	}
	if _, err := w.Enqueue(t.Context(), a, encodeLong(t, a)); !errors.Is(err, injected) {
		t.Fatalf("tenant a enqueue after poison = %v, want injected failure", err)
	}

	rb3 := enqueueLong(t, w, b)
	if err := rb3.Written(t.Context()); err != nil {
		t.Fatalf("tenant b write after tenant a poison: %v", err)
	}
	rec.get(1).fire() // sync 6: tenant b tick
	if err := rb3.Wait(t.Context()); err != nil {
		t.Fatalf("tenant b wait after tenant a poison: %v", err)
	}
}

// TestSyncIntervalCanceledWaitStillDurable proves canceling a Wait only stops
// the waiting: the admitted record still syncs on the timer and replays.
func TestSyncIntervalCanceledWaitStillDurable(t *testing.T) {
	dir := t.TempDir()
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	if err := receipt.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := receipt.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v, want context.Canceled", err)
	}
	rec.get(0).fire()
	if err := receipt.Wait(context.Background()); err != nil {
		t.Fatal(err)
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
		t.Fatalf("replay after canceled wait = %v, want [1]", got)
	}
}

// TestSyncIntervalLossBoundIsDataSinceLastSync simulates the machine-crash
// loss bound of interval mode: bytes written after the last completed sync
// are truncated away, and recovery replays exactly the synced prefix. The
// first WAL is abandoned without Close, as a crashed process would be.
func TestSyncIntervalLossBoundIsDataSinceLastSync(t *testing.T) {
	dir := t.TempDir()
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	ops := hooks.fileOps()
	ops.newTicker = rec.factory
	w, err := open(t.Context(), dir, Config{SyncMode: SyncInterval}, ops)
	if err != nil {
		t.Fatal(err)
	}
	// The abandoned WAL is only closed after every crash-loss assertion (and
	// the recovery WAL's own close) has run, so the simulation never gets a
	// clean close first, yet the worker and root cannot leak.
	t.Cleanup(func() {
		if err := w.Close(context.Background()); err != nil {
			t.Logf("close abandoned crash-simulation WAL: %v", err)
		}
	})
	id := tenant.Default

	first := enqueueLong(t, w, id)
	if err := first.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	rec.get(0).fire()
	if err := first.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	segment := newestSegment(t, dir, id)
	info, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	durableBytes := info.Size()

	second := enqueueLong(t, w, id)
	if err := second.Written(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= durableBytes {
		t.Fatalf("unsynced write did not extend the segment: %d, want more than %d", after.Size(), durableBytes)
	}
	// Simulate loss of everything written since the last successful sync.
	if err := os.Truncate(segment, durableBytes); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	if got := internalReplayPositions(t, w2, id, Position{}); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("replay after simulated loss = %v, want [1]", got)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 2 {
		t.Fatalf("append after simulated loss sequence = %d, want 2 (lost seq reused)", pos.Seq)
	}
}

// TestReceiptWrittenZeroAndCancellation proves both waits reject a zero
// receipt, and that canceling Written only stops the waiting — the worker is
// held in the segment header sync, so the write cannot have completed yet.
func TestReceiptWrittenZeroAndCancellation(t *testing.T) {
	var zero Receipt
	if err := zero.Written(t.Context()); err == nil {
		t.Fatal("Written on zero receipt succeeded")
	}
	if err := zero.Wait(t.Context()); err == nil {
		t.Fatal("Wait on zero receipt succeeded")
	}

	hooks := &groupHooks{gates: map[int64]*syncGate{1: newSyncGate()}}
	t.Cleanup(hooks.unblockAll)
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, t.TempDir(), Config{}, hooks, rec)
	id := tenant.Default
	receipt := enqueueLong(t, w, id)
	awaitSignal(t, hooks.gates[1].started)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := receipt.Written(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Written = %v, want context.Canceled", err)
	}
	hooks.gates[1].unblock()
	if err := receipt.Written(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.get(0).fire()
	if err := receipt.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestCloseDrainsWorkAdmittedAfterIdleSnapshot arranges the admission/close
// race deterministically: the worker has just snapshotted an empty queue when
// a record is admitted and close begins. The drain contract requires the
// worker to write, sync and complete that record's receipts anyway, and
// recovery must replay it exactly.
func TestCloseDrainsWorkAdmittedAfterIdleSnapshot(t *testing.T) {
	dir := t.TempDir()
	w := openHookedWAL(t, dir, Config{}, &groupHooks{})
	id := tenant.Default
	log := pauseTenant(w, id)
	payload := encodeInternal(t, id)
	probed := make(chan Receipt, 1)
	log.idleProbe = func() {
		log.idleProbe = nil // one shot: only this first idle pass races
		receipt, err := w.Enqueue(context.Background(), id, payload)
		if err != nil {
			t.Errorf("probe enqueue: %v", err)
			return
		}
		probed <- receipt
		log.beginClose()
	}
	startTenant(w, log)
	var receipt Receipt
	select {
	case receipt = <-probed:
	case <-time.After(5 * time.Second):
		t.Fatal("idle probe never ran")
	}
	if err := awaitCompleted(t, writeAsync(receipt)); err != nil {
		t.Fatalf("write receipt of admitted work: %v", err)
	}
	if err := awaitResult(t, waitAllAsync([]Receipt{receipt})[0]); err != nil {
		t.Fatalf("durable receipt of admitted work: %v", err)
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
		t.Fatalf("replay after close drain = %v, want [1]", got)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 2 {
		t.Fatalf("append after close drain sequence = %d, want 2", pos.Seq)
	}
}

// TestSyncIntervalReplayReclaimFrontiers re-runs the replay and reclaim
// frontier contract in interval mode: rotation seals and one final tick make
// every record durable, reclaim drops sealed covered segments, and frontier
// validation errors stay exact.
func TestSyncIntervalReplayReclaimFrontiers(t *testing.T) {
	dir := t.TempDir()
	hooks := &groupHooks{}
	rec := &tickerRecorder{}
	w := openIntervalWAL(t, dir, Config{SegmentMaxBytes: 300}, hooks, rec)
	id := tenant.ID("frontier")
	receipts := []Receipt{
		enqueueLong(t, w, id),
		enqueueLong(t, w, id),
		enqueueLong(t, w, id),
	}
	for i, receipt := range receipts {
		if err := receipt.Written(t.Context()); err != nil {
			t.Fatalf("write receipt %d: %v", i, err)
		}
	}
	rec.get(0).fire()
	for i, receipt := range receipts {
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatalf("durable receipt %d: %v", i, err)
		}
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(t.Context(), dir, Config{SegmentMaxBytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(context.Background())
	frontier := Position{Tenant: id, Seq: 2}
	if err := w2.Reclaim(t.Context(), id, frontier); err != nil {
		t.Fatal(err)
	}
	if got := internalReplayPositions(t, w2, id, frontier); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("replay after reclaim = %v, want [3]", got)
	}
	if err := firstReplayErr(w2, id); !errors.Is(err, ErrInvalidFrontier) {
		t.Fatalf("replay without frontier error = %v, want ErrInvalidFrontier", err)
	}
	if err := w2.Reclaim(t.Context(), id, Position{Tenant: id, Seq: 4}); !errors.Is(err, ErrInvalidFrontier) {
		t.Fatalf("future reclaim error = %v, want ErrInvalidFrontier", err)
	}
	if pos := appendInternal(t, w2, id); pos.Seq != 4 {
		t.Fatalf("append after reclaim sequence = %d, want 4", pos.Seq)
	}
}
