package wal

import (
	"errors"
	"fmt"
	"time"
)

// defaultSyncInterval is the timer period used in SyncInterval mode when
// Config.SyncInterval is zero.
const defaultSyncInterval = time.Second

// SyncMode selects when a tenant worker fsyncs appended records.
type SyncMode int

const (
	// SyncAlways fsyncs every batch before completing its receipts. It is the
	// zero value, so a zero Config keeps the phase 3 durability behavior:
	// Receipt.Wait returns only after the record's bytes are on stable storage.
	SyncAlways SyncMode = iota
	// SyncInterval fsyncs on a timer: Receipt.Written completes once the batch
	// is written, Receipt.Wait completes only when a later sync — the timer,
	// a rotation seal or an orderly close — covers the record. A machine crash
	// may lose everything written since the last completed sync.
	SyncInterval
)

// String returns the flag spelling of the mode.
func (m SyncMode) String() string {
	switch m {
	case SyncAlways:
		return "always"
	case SyncInterval:
		return "interval"
	default:
		return "unknown"
	}
}

// ParseSyncMode converts a -wal.sync flag value into a SyncMode.
func ParseSyncMode(value string) (SyncMode, error) {
	switch value {
	case "always":
		return SyncAlways, nil
	case "interval":
		return SyncInterval, nil
	default:
		return 0, fmt.Errorf("wal: invalid sync mode %q: want always or interval", value)
	}
}

// ticker is the seam between the tenant worker loop and a repeating clock.
// Production uses time.Ticker; tests fire ticks by hand.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

// timeTicker adapts *time.Ticker to the ticker interface.
type timeTicker struct{ *time.Ticker }

func (t timeTicker) C() <-chan time.Time { return t.Ticker.C }

// newTickerFunc builds a ticker for a tenant worker. It is replaced in tests.
type newTickerFunc func(time.Duration) ticker

// defaultNewTicker builds a real repeating clock.
func defaultNewTicker(d time.Duration) ticker { return timeTicker{time.NewTicker(d)} }

// syncDirtyData fsyncs the active segment when records are written but not yet
// known durable and returns the receipts the completed sync made durable. It
// runs on the tenant worker goroutine for timer, rotation and close syncs; a
// sync here makes every frame written before it started durable, and syncFile
// reports the outcome to Config.OnSync. A nil active file means nothing was
// ever written and the call is a no-op. The caller completes the returned
// receipts only after its own housekeeping is finalized, so receipt
// completion always implies the worker holds no in-flight exclusion.
func (w *WAL) syncDirtyData(l *tenantLog) ([]*appendRequest, error) {
	l.mu.Lock()
	high := l.written
	dirty := high > l.durable
	l.mu.Unlock()
	if !dirty || l.active == nil {
		return nil, nil
	}
	if err := w.syncFile(l.active); err != nil {
		return nil, fmt.Errorf("wal: sync %s: %w", l.id, err)
	}
	return l.advanceDurable(high), nil
}

// completeDurableAll completes a release set's durability receipts with nil.
func completeDurableAll(requests []*appendRequest) {
	for _, request := range requests {
		request.waiter.completeDurable(nil)
	}
}

// advanceDurable records high as durable — never regressing — and detaches
// the pending receipts it now covers. The caller completes them outside the
// tenant lock.
func (l *tenantLog) advanceDurable(high uint64) []*appendRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	if high > l.durable {
		l.durable = high
	}
	var ready []*appendRequest
	for len(l.pending) > 0 && l.pending[0].position.Seq <= l.durable {
		ready = append(ready, l.pending[0])
		l.pending = l.pending[1:]
	}
	return ready
}

// syncAtClose is the worker's final durability pass: any data left dirty by
// SyncInterval mode is synced before the active file closes. A failure
// completes the pending receipts with the error and is reported through
// WAL.Close; the file still closes so the process can shut down.
func (w *WAL) syncAtClose(l *tenantLog) {
	release, err := w.syncDirtyData(l)
	if err != nil {
		l.mu.Lock()
		l.closeErr = errors.Join(l.closeErr, fmt.Errorf("wal: close tenant %s sync: %w", l.id, err))
		pending := l.pending
		l.pending = nil
		l.mu.Unlock()
		for _, request := range pending {
			request.waiter.completeDurable(err)
		}
		return
	}
	completeDurableAll(release)
}

// poison stops the tenant worker after a write or sync failure and makes the
// error sticky: later enqueues and replays are rejected with it. Requests
// whose frames never completed writing — the failing batch before its write
// receipts, and everything still queued — fail on both channels; requests
// already written keep their nil write receipt and fail durability, which is
// unknown after a failed sync. Completion helpers are idempotent, so the
// failing batch can be passed wholesale whether the failure happened at a
// write or a later sync. Any barrier already detached from the queue by the
// worker must be passed in one of the groups so its waiter is never stranded.
//
// The poison and the in-flight exclusion clear happen under one lock: a
// concurrent beginReplay must observe either writing set or the sticky error,
// never a window where both are clear. Receipts a completed sync detached as
// known durable are not in these groups; the caller completes them nil after
// the poison has swept the failing work.
func (l *tenantLog) poison(err error, groups ...[]*appendRequest) {
	l.mu.Lock()
	l.poisoned = err
	l.writing = false
	queued := l.queue
	l.queue = nil
	pending := l.pending
	l.pending = nil
	l.mu.Unlock()
	for _, group := range append(groups, queued, pending) {
		for _, request := range group {
			request.waiter.completeWrite(err)
			request.waiter.completeDurable(err)
		}
	}
}
