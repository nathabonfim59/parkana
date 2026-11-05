package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// receiptWaiter carries a receipt's two outcomes. Both channels are closed
// only by the tenant worker goroutine, so the error fields and the completion
// flags need no lock. completeWrite is idempotent so a failure path can never
// rewrite an outcome that already completed — a batch whose frames were
// written keeps its nil write receipt even when a later sync fails.
type receiptWaiter struct {
	written    chan struct{}
	writeDone  bool
	werr       error
	done       chan struct{}
	durableOne bool
	err        error
}

// completeWrite completes the write outcome exactly once.
func (w *receiptWaiter) completeWrite(err error) {
	if w.writeDone {
		return
	}
	w.writeDone = true
	w.werr = err
	close(w.written)
}

// completeDurable completes the durability outcome exactly once.
func (w *receiptWaiter) completeDurable(err error) {
	if w.durableOne {
		return
	}
	w.durableOne = true
	w.err = err
	close(w.done)
}

// Written waits until the record's frames have been written to the segment
// file. It returns nil as soon as the whole batch's writes succeeded — before
// any fsync — and an error when the write failed, in which case the record is
// never acknowledged on either channel. Like Wait, cancellation after
// admission only stops waiting; the record may still become durable.
func (r Receipt) Written(ctx context.Context) error {
	if r.waiter == nil {
		return fmt.Errorf("wal: write wait on zero receipt")
	}
	select {
	case <-r.waiter.written:
		if r.waiter.werr != nil {
			return fmt.Errorf("wal: written %s sequence %d: %w", r.Position.Tenant, r.Position.Seq, r.waiter.werr)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wal: written %s sequence %d: %w", r.Position.Tenant, r.Position.Seq, ctx.Err())
	}
}

// Wait waits until a file sync covering the record has completed. In
// SyncInterval mode that is a later timer, rotation or close sync, so Wait
// keeps its durable meaning in both modes. Cancellation after admission only
// stops waiting; the record may still become durable and replay after restart.
func (r Receipt) Wait(ctx context.Context) error {
	if r.waiter == nil {
		return fmt.Errorf("wal: wait on zero receipt")
	}
	select {
	case <-r.waiter.done:
		if r.waiter.err != nil {
			return fmt.Errorf("wal: wait %s sequence %d: %w", r.Position.Tenant, r.Position.Seq, r.waiter.err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wal: wait %s sequence %d: %w", r.Position.Tenant, r.Position.Seq, ctx.Err())
	}
}

type appendRequest struct {
	position Position
	payload  Payload
	waiter   *receiptWaiter
	// barrier marks a force-sync marker enqueued by WAL.Sync: it carries no
	// payload and consumes no sequence; the worker completes it once a sync
	// covers everything admitted before it.
	barrier bool
}

type tenantLog struct {
	id   tenant.ID
	root *os.Root

	mu sync.Mutex
	// wake admits the worker's idle select; sends never block and coalesce.
	wake chan struct{}
	// queue holds admitted, not yet written requests.
	queue []*appendRequest
	next  uint64
	// written is the highest sequence whose frames reached the segment file;
	// durable is the highest sequence a completed file sync covered. In
	// SyncAlways mode they advance together; in SyncInterval mode written runs
	// ahead between timer, rotation and close syncs.
	written uint64
	durable uint64
	// pending holds written requests in sequence order whose durability
	// receipts have not closed yet: until the batch sync in SyncAlways mode,
	// until the next covering sync in SyncInterval mode.
	pending    []*appendRequest
	segments   []segmentInfo
	closing    bool
	poisoned   error
	replaying  bool
	reclaiming bool
	writing    bool
	// dirSyncPending records a successful segment removal whose directory sync
	// has not completed. The next Reclaim must finish it before removing more.
	dirSyncPending bool

	done     chan struct{}
	active   segmentFile
	closeErr error
	// idleProbe is a test seam invoked on the worker goroutine after it has
	// snapshotted an empty queue and before it decides to exit on closing, so
	// a test can arrange admission and close inside that window. It must not
	// block on the worker's progress. It is nil in production.
	idleProbe func()
	// afterDequeue is a test seam invoked on the worker goroutine right after
	// the queue snapshot unlocked work (batch or barrier), so a test can
	// observe the exact post-dequeue state. It must not block on the worker's
	// progress. It is nil in production.
	afterDequeue func()
}

func newTenantLog(id tenant.ID, root *os.Root, next uint64) *tenantLog {
	return &tenantLog{
		id:   id,
		root: root,
		next: next,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
}

func (l *tenantLog) enqueue(ctx context.Context, payload Payload) (Receipt, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Receipt{}, fmt.Errorf("wal: enqueue %s: %w", l.id, err)
	}
	if l.closing {
		return Receipt{}, fmt.Errorf("wal: enqueue %s: %w", l.id, ErrClosed)
	}
	if l.poisoned != nil {
		return Receipt{}, fmt.Errorf("wal: enqueue %s after failure: %w", l.id, l.poisoned)
	}
	if l.replaying {
		return Receipt{}, fmt.Errorf("wal: enqueue %s during replay", l.id)
	}
	if l.next == 0 || l.next == ^uint64(0) {
		return Receipt{}, fmt.Errorf("wal: enqueue %s: sequence space exhausted", l.id)
	}
	position := Position{Tenant: l.id, Seq: l.next}
	waiter := &receiptWaiter{written: make(chan struct{}), done: make(chan struct{})}
	l.queue = append(l.queue, &appendRequest{position: position, payload: payload, waiter: waiter})
	l.next++
	l.signalLocked()
	return Receipt{Position: position, waiter: waiter}, nil
}

// signalLocked wakes an idle worker; l.mu must be held.
func (l *tenantLog) signalLocked() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// enqueueBarrier admits a force-sync marker at the queue tail. Rejections
// mirror enqueue — a closing, poisoned or replaying tenant never receives a
// barrier whose worker could not complete it, so no barrier is stranded.
func (l *tenantLog) enqueueBarrier() (*appendRequest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return nil, fmt.Errorf("wal: sync %s: %w", l.id, ErrClosed)
	}
	if l.poisoned != nil {
		return nil, fmt.Errorf("wal: sync %s after failure: %w", l.id, l.poisoned)
	}
	if l.replaying {
		return nil, fmt.Errorf("wal: sync %s during replay", l.id)
	}
	request := &appendRequest{
		position: Position{Tenant: l.id, Seq: l.next},
		waiter:   &receiptWaiter{written: make(chan struct{}), done: make(chan struct{})},
		barrier:  true,
	}
	l.queue = append(l.queue, request)
	l.signalLocked()
	return request, nil
}

func (l *tenantLog) beginClose() {
	l.mu.Lock()
	l.closing = true
	l.signalLocked()
	l.mu.Unlock()
}

func (l *tenantLog) beginReplay() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return fmt.Errorf("wal: replay %s: %w", l.id, ErrClosed)
	}
	if l.poisoned != nil {
		return fmt.Errorf("wal: replay %s after failure: %w", l.id, l.poisoned)
	}
	if l.replaying {
		return fmt.Errorf("wal: replay %s already active", l.id)
	}
	if l.reclaiming {
		return fmt.Errorf("wal: replay %s during reclaim", l.id)
	}
	if len(l.queue) != 0 || l.writing {
		return fmt.Errorf("wal: replay %s while appends are queued or in flight", l.id)
	}
	l.replaying = true
	return nil
}

func (l *tenantLog) endReplay() {
	l.mu.Lock()
	l.replaying = false
	l.mu.Unlock()
}

func (l *tenantLog) beginReclaim() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return fmt.Errorf("wal: reclaim %s: %w", l.id, ErrClosed)
	}
	if l.poisoned != nil {
		return fmt.Errorf("wal: reclaim %s after failure: %w", l.id, l.poisoned)
	}
	if l.replaying {
		return fmt.Errorf("wal: reclaim %s during replay", l.id)
	}
	if l.reclaiming {
		return fmt.Errorf("wal: reclaim %s already active", l.id)
	}
	l.reclaiming = true
	return nil
}

func (l *tenantLog) endReclaim() {
	l.mu.Lock()
	l.reclaiming = false
	l.mu.Unlock()
}

// run is the tenant worker: it drains admitted batches, and in SyncInterval
// mode additionally syncs written-but-unsynced data when the timer fires —
// including while batches keep arriving, via the boundary check after each
// batch — and once more at close. Any write or sync failure is sticky and
// stops the worker.
func (l *tenantLog) run(w *WAL) {
	defer close(l.done)
	defer func() {
		if l.active != nil {
			l.closeErr = errors.Join(l.closeErr, l.active.Close())
			l.active = nil
		}
	}()
	var ticks <-chan time.Time
	if w.cfg.SyncMode == SyncInterval {
		t := w.ops.newTicker(w.cfg.SyncInterval)
		defer t.Stop()
		ticks = t.C()
	}
	for {
		// Snapshot queue and closing in one critical section: an enqueue that
		// admits work after an empty-queue snapshot always sees closing false
		// and leaves a wake token, so the next iteration drains it even when
		// close has since begun. Exiting on closing is safe only when the
		// same snapshot proved the queue empty. A queued barrier is work like
		// any other, so Close drains it too; the batch is cut at the first
		// barrier, which then runs alone in sequence order.
		l.mu.Lock()
		var batch []*appendRequest
		var barrier *appendRequest
		if i := slices.IndexFunc(l.queue, func(r *appendRequest) bool { return r.barrier }); i >= 0 {
			batch = l.queue[:i]
			barrier = l.queue[i]
			l.queue = l.queue[i+1:]
		} else {
			batch = l.queue
			l.queue = nil
		}
		closing := l.closing
		dirty := l.written > l.durable
		if len(batch) > 0 || barrier != nil {
			// In-flight exclusion is set in the same critical section as the
			// dequeue: from the moment work leaves the queue until it finishes
			// (or poisons), beginReplay must see writing true with no gap
			// between the batch and a detached barrier.
			l.writing = true
		}
		l.mu.Unlock()

		if probe := l.afterDequeue; probe != nil && (len(batch) > 0 || barrier != nil) {
			probe()
		}

		// release accumulates the receipts a completed sync has made durable
		// while this selected work ran. On success they are completed only
		// after writing is cleared; on failure poison first marks the tenant
		// failed and clears writing under one lock, then they are completed
		// nil. Either way a completed receipt implies finalized worker
		// housekeeping, and the release set is disjoint from the failing
		// batch, queue and pending because a completed sync detached it.
		var release []*appendRequest
		if len(batch) > 0 {
			flushed, err := w.writeBatch(l, batch)
			release = flushed
			if err != nil {
				// Poison marks the tenant failed and clears the in-flight
				// exclusion in one critical section, so replay cannot enter
				// between them. The release set was detached by a completed
				// sync and is known durable, so complete it nil after the
				// poison sweeps the batch, queue and pending.
				l.poison(err, batch, barrierBatch(barrier))
				completeDurableAll(release)
				return
			}
			// A tick that arrived while the batch was writing makes the sync
			// due at this boundary — before the next batch — so a continuously
			// busy tenant cannot starve its durability timer behind a
			// non-empty queue. The sync covers the batch just written.
			if ticks != nil {
				select {
				case <-ticks:
					more, err := w.syncDirtyData(l)
					release = append(release, more...)
					if err != nil {
						l.poison(err, barrierBatch(barrier))
						completeDurableAll(release)
						return
					}
				default:
				}
			}
			if barrier == nil {
				l.mu.Lock()
				l.writing = false
				l.mu.Unlock()
				completeDurableAll(release)
				continue
			}
			// A barrier was detached together with the batch: run it now,
			// exactly as the next iteration would, so it is never dropped.
			// writing stays true across the batch→barrier transition, and the
			// release set waits with it.
		}

		if barrier != nil {
			// Force-sync barrier, worker-serialized with the tenant's writes
			// (writing was set atomically with its dequeue). Everything
			// admitted before it in queue order is on the file; syncDirtyData
			// is a clean no-op when nothing is dirty, so a SyncAlways steady
			// state issues no redundant fsync.
			more, err := w.syncDirtyData(l)
			release = append(release, more...)
			if err != nil {
				l.poison(err, barrierBatch(barrier))
				completeDurableAll(release)
				return
			}
			// Finalize the tenant bookkeeping before completing any waiters,
			// so a caller returning from Sync observes writing already clear.
			l.mu.Lock()
			l.writing = false
			l.mu.Unlock()
			completeDurableAll(release)
			barrier.waiter.completeWrite(nil)
			barrier.waiter.completeDurable(nil)
			continue
		}

		if probe := l.idleProbe; probe != nil {
			probe()
		}

		if closing {
			// Orderly close forces dirty data out before the active file
			// closes; a failure fails the pending receipts and is reported
			// by Close.
			if dirty {
				w.syncAtClose(l)
			}
			return
		}
		select {
		case <-l.wake:
		case <-ticks:
			release, err := w.syncDirtyData(l)
			if err != nil {
				l.poison(err)
				return
			}
			completeDurableAll(release)
		}
	}
}

// barrierBatch wraps a barrier detached from the queue so poison completes
// its waiter; nil stays nil for the failure paths that hold no barrier.
func barrierBatch(barrier *appendRequest) []*appendRequest {
	if barrier == nil {
		return nil
	}
	return []*appendRequest{barrier}
}

type segmentFile interface {
	io.Writer
	Sync() error
	Close() error
	Seek(offset int64, whence int) (int64, error)
}

type fileOps struct {
	create  func(root *os.Root, name string) (segmentFile, error)
	open    func(root *os.Root, name string) (segmentFile, error)
	remove  func(root *os.Root, name string) error
	syncDir func(root *os.Root) error
	// newTicker builds the SyncInterval-mode durability timer of a tenant
	// worker. Tests replace it with a manually fired ticker; production uses
	// a real clock. Zero is invalid: build fileOps with defaultFileOps.
	newTicker newTickerFunc
}

func defaultFileOps() fileOps {
	return fileOps{
		create: func(root *os.Root, name string) (segmentFile, error) {
			return root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		},
		open: func(root *os.Root, name string) (segmentFile, error) {
			return root.OpenFile(name, os.O_RDWR, 0)
		},
		remove:    func(root *os.Root, name string) error { return root.Remove(name) },
		syncDir:   syncRoot,
		newTicker: defaultNewTicker,
	}
}

// syncFile Syncs a segment file and reports the observed latency and result to
// Config.OnSync. It must be called outside tenant locks.
func (w *WAL) syncFile(file segmentFile) (err error) {
	if w.cfg.OnSync == nil {
		return file.Sync()
	}
	start := time.Now()
	err = file.Sync()
	w.cfg.OnSync(time.Since(start), err)
	return err
}

// TODO(phase3): coalesce the per-frame writes below into one batch buffer and
// a single Write call; benchmarks will check the WAL path against the 31 MB/s
// ingest target.
//
// writeBatch writes the batch's frames and returns the receipts a completed
// sync has already made durable (rotation seals within the batch, plus the
// batch sync itself in SyncAlways mode). It does not complete them: the caller
// releases them after clearing the in-flight exclusion, so a completed
// receipt always implies finalized worker housekeeping. Write receipts still
// complete here, immediately after the whole batch's frames are in the file.
func (w *WAL) writeBatch(l *tenantLog, batch []*appendRequest) ([]*appendRequest, error) {
	var release []*appendRequest
	for _, request := range batch {
		if err := w.ensureTenantRoot(l); err != nil {
			return release, err
		}
		if l.active == nil {
			if err := w.openActive(l, request.position.Seq); err != nil {
				return release, err
			}
		}
		rotate, err := w.shouldRotate(l, request)
		if err != nil {
			return release, err
		}
		if rotate {
			flushed, err := w.rotate(l, request.position.Seq)
			release = append(release, flushed...)
			if err != nil {
				return release, err
			}
			if err := w.openActive(l, request.position.Seq); err != nil {
				return release, err
			}
		}
		frame, err := marshalFrame(frameRecord, request.position.Seq, request.payload.data)
		if err != nil {
			return release, err
		}
		if err := writeAll(l.active, frame); err != nil {
			return release, fmt.Errorf("wal: write %s sequence %d: %w", l.id, request.position.Seq, err)
		}
		l.mu.Lock()
		active := &l.segments[len(l.segments)-1]
		active.size += int64(len(frame))
		active.last = request.position.Seq
		active.next = request.position.Seq + 1
		active.records++
		l.written = request.position.Seq
		l.mu.Unlock()
	}
	if l.active == nil {
		return release, fmt.Errorf("wal: empty batch for %s", l.id)
	}
	// The whole batch's frames are in the file now: a write receipt is never
	// issued for a partially written batch.
	for _, request := range batch {
		request.waiter.completeWrite(nil)
	}
	// Park the batch as pending so a completed sync can release its durable
	// receipts. In SyncAlways mode the batch sync that follows covers all of
	// it; in SyncInterval mode durability waits for the timer, a rotation
	// seal or close — a rotation inside this batch may have synced part of it
	// already, which the high-water snapshot below releases.
	high := l.durable
	if w.cfg.SyncMode == SyncAlways {
		if err := w.syncFile(l.active); err != nil {
			return release, fmt.Errorf("wal: sync %s: %w", l.id, err)
		}
		high = batch[len(batch)-1].position.Seq
	}
	l.mu.Lock()
	l.pending = append(l.pending, batch...)
	l.mu.Unlock()
	return append(release, l.advanceDurable(high)...), nil
}

func (w *WAL) ensureTenantRoot(l *tenantLog) error {
	l.mu.Lock()
	root := l.root
	l.mu.Unlock()
	if root != nil {
		return nil
	}
	if err := w.root.Mkdir(string(l.id), 0o750); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("wal: create tenant %s directory: %w", l.id, err)
	}
	if err := syncRoot(w.root); err != nil {
		return fmt.Errorf("wal: sync tenant %s parent: %w", l.id, err)
	}
	root, err := w.root.OpenRoot(string(l.id))
	if err != nil {
		return fmt.Errorf("wal: open tenant %s root: %w", l.id, err)
	}
	l.mu.Lock()
	l.root = root
	l.mu.Unlock()
	return nil
}

func (w *WAL) shouldRotate(l *tenantLog, request *appendRequest) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	active := l.segments[len(l.segments)-1]
	if active.next != request.position.Seq {
		return false, fmt.Errorf("wal: %s active next %d, append sequence %d", l.id, active.next, request.position.Seq)
	}
	return active.records > 0 && active.size+int64(frameHeaderSize+len(request.payload.data)) > w.cfg.SegmentMaxBytes, nil
}

func (w *WAL) openActive(l *tenantLog, seq uint64) error {
	l.mu.Lock()
	root := l.root
	segments := len(l.segments)
	var active segmentInfo
	if segments > 0 {
		active = l.segments[segments-1]
	}
	l.mu.Unlock()
	if root == nil {
		return fmt.Errorf("wal: open %s active segment without root", l.id)
	}
	if segments > 0 && active.sealed && active.next != seq {
		return fmt.Errorf("wal: %s sealed next %d, append sequence %d", l.id, active.next, seq)
	}
	if segments == 0 || active.sealed {
		file, err := w.ops.create(root, segmentName(seq))
		if err != nil {
			return fmt.Errorf("wal: create %s segment %d: %w", l.id, seq, err)
		}
		header, err := marshalSegmentHeader(l.id, seq)
		if err == nil {
			err = writeAll(file, header)
		}
		if err == nil {
			err = w.syncFile(file)
		}
		if err == nil {
			err = syncRoot(root)
		}
		if err != nil {
			file.Close()
			return fmt.Errorf("wal: initialize %s segment %d: %w", l.id, seq, err)
		}
		l.active = file
		l.mu.Lock()
		l.segments = append(l.segments, segmentInfo{
			first: seq,
			last:  seq - 1,
			next:  seq,
			size:  segmentHeaderSize(len(l.id)),
		})
		l.mu.Unlock()
		return nil
	}
	file, err := w.ops.open(root, segmentName(active.first))
	if err != nil {
		return fmt.Errorf("wal: open %s active segment %d: %w", l.id, active.first, err)
	}
	if _, err := file.Seek(active.size, io.SeekStart); err != nil {
		file.Close()
		return fmt.Errorf("wal: seek %s active segment %d: %w", l.id, active.first, err)
	}
	l.active = file
	return nil
}

// rotate seals the active segment and returns the receipts the seal sync made
// durable — only ones already marked written (earlier batches); the current
// batch parks at its end. The caller completes the returned receipts after
// clearing the in-flight exclusion.
func (w *WAL) rotate(l *tenantLog, next uint64) ([]*appendRequest, error) {
	seal, err := marshalFrame(frameSeal, next, nil)
	if err != nil {
		return nil, err
	}
	if err := writeAll(l.active, seal); err != nil {
		return nil, fmt.Errorf("wal: write %s segment seal: %w", l.id, err)
	}
	if err := w.syncFile(l.active); err != nil {
		return nil, fmt.Errorf("wal: sync %s segment seal: %w", l.id, err)
	}
	// The seal sync made every frame written so far durable — rotation is a
	// durability point in both modes.
	l.mu.Lock()
	high := l.written
	l.mu.Unlock()
	flushed := l.advanceDurable(high)
	if err := l.active.Close(); err != nil {
		// The seal sync already made flushed durable; they still complete.
		return flushed, fmt.Errorf("wal: close %s sealed segment: %w", l.id, err)
	}
	l.active = nil
	l.mu.Lock()
	active := &l.segments[len(l.segments)-1]
	active.sealed = true
	active.size += int64(len(seal))
	l.mu.Unlock()
	return flushed, nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
