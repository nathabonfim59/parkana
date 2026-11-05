package tail

import (
	"container/heap"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// clock is the per-service time source. Tests replace it to drive delay
// ordering deterministically; there is deliberately no global clock.
type clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// queuedEntry is one already-pipelined entry waiting for its delay deadline.
// labels are the stream labels of the public response (meta/parsed flattened
// or categorized away), so grouping and drop descriptors use the same set.
type queuedEntry struct {
	seq    uint64
	ts     time.Time
	labels logs.Labels
	entry  plan.Entry
}

// droppedEntry is the timestamp and stream labels of one locally dropped
// entry: enough to emit a real Loki dropped_entries descriptor.
type droppedEntry struct {
	ts     time.Time
	labels logs.Labels
}

// delayQueue is one session's bounded, timestamp-ordered delay buffer. It is
// fed by the ingester readers and drained by the WebSocket writer. Overflow
// drops the newest entries with known descriptors; it never blocks a reader
// and never holds more than budget entries.
type delayQueue struct {
	budget int
	m      *tailMetrics
	tenant tenant.ID

	mu           sync.Mutex
	h            entryHeap
	seq          uint64
	pendingDrops []droppedEntry
	// descriptorOverflow counts local drops after the descriptor buffer hit
	// its cap. They cannot be described, so the writer closes the stream
	// explicitly with this count instead of silently hiding the loss.
	descriptorOverflow uint64
	closed             bool
	notify             chan struct{}
}

func newDelayQueue(budget int, m *tailMetrics, id tenant.ID) *delayQueue {
	return &delayQueue{
		budget: max(budget, 1),
		m:      m,
		tenant: id,
		notify: make(chan struct{}, 1),
	}
}

// push enqueues entries when they fit and records real descriptors for what
// it cannot keep. It returns the number of dropped entries.
func (q *delayQueue) push(entries []queuedEntry) int {
	if len(entries) == 0 {
		return 0
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return 0
	}
	room := q.budget - len(q.h)
	keep := 0
	if room > 0 {
		keep = min(room, len(entries))
	}
	for _, e := range entries[:keep] {
		q.seq++
		e.seq = q.seq
		heap.Push(&q.h, e)
	}
	dropped := 0
	for _, e := range entries[keep:] {
		dropped++
		if len(q.pendingDrops) < maxDroppedDescriptors {
			q.pendingDrops = append(q.pendingDrops, droppedEntry{ts: e.ts, labels: e.labels})
		} else {
			// The descriptor budget is full: the writer must end the stream
			// with an explicit gap instead of pretending this loss is
			// described.
			q.descriptorOverflow++
		}
	}
	if dropped > 0 {
		q.m.dropped(q.tenant, uint64(dropped))
	}
	q.mu.Unlock()
	q.signal()
	return dropped
}

// take returns entries whose delay deadline has passed, in timestamp order,
// up to limit entries and maxBytes of line data, plus every pending drop
// descriptor. overflow is the number of additionally dropped entries that
// could not be described because the descriptor cap was reached. nextWait is
// the next interesting time: the earliest deadline, capped by the configured
// flush interval so a sparse stream is re-evaluated promptly.
func (q *delayQueue) take(now time.Time, delay time.Duration, limit, maxBytes int, flush time.Duration) (due []queuedEntry, drops []droppedEntry, overflow uint64, nextWait time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	bytes := 0
	for q.h.Len() > 0 && (limit <= 0 || len(due) < limit) {
		next := q.h[0]
		if next.ts.Add(delay).After(now) {
			break
		}
		// Always take at least one entry, even if its line alone exceeds
		// maxBytes: otherwise a huge line could stall the stream forever.
		if len(due) > 0 && bytes+len(next.entry.Line) > maxBytes {
			break
		}
		heap.Pop(&q.h)
		due = append(due, next)
		bytes += len(next.entry.Line)
	}
	if len(q.pendingDrops) > 0 {
		drops = q.pendingDrops
		q.pendingDrops = nil
	}
	overflow = q.descriptorOverflow
	q.descriptorOverflow = 0
	switch {
	case overflow > 0 || len(drops) > 0:
		nextWait = 0
	case q.h.Len() == 0:
		nextWait = flush
	default:
		nextWait = q.h[0].ts.Add(delay).Sub(now)
		if nextWait < 0 {
			nextWait = 0
		} else {
			nextWait = min(nextWait, flush)
		}
	}
	return due, drops, overflow, nextWait
}

// close drops the queue and wakes the writer. Pushes after close are ignored.
func (q *delayQueue) close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.h = nil
	q.pendingDrops = nil
	q.mu.Unlock()
	q.signal()
}

func (q *delayQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// entryHeap orders by timestamp, then insertion order so entries with equal
// timestamps stay in the order their ingester delivered them.
type entryHeap []queuedEntry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	if h[i].ts.Equal(h[j].ts) {
		return h[i].seq < h[j].seq
	}
	return h[i].ts.Before(h[j].ts)
}

func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) { *h = append(*h, x.(queuedEntry)) }

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}
