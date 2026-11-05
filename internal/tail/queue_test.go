package tail

import (
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func testQueue(t *testing.T, budget int) *delayQueue {
	t.Helper()
	m, err := newTailMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return newDelayQueue(budget, m, tenant.Default)
}

func qentry(ts time.Time, line string) queuedEntry {
	return queuedEntry{
		ts:     ts,
		labels: logs.New(logs.Label{Name: "job", Value: "api"}),
		entry:  plan.Entry{TS: ts, Line: line},
	}
}

func TestQueueOrdersByTimestampAcrossIngesters(t *testing.T) {
	q := testQueue(t, 16)
	base := time.Unix(100, 0).UTC()
	// Delivered out of order, as two ingesters would.
	q.push([]queuedEntry{
		qentry(base.Add(100*time.Millisecond), "third"),
		qentry(base.Add(-100*time.Millisecond), "first"),
		qentry(base.Add(50*time.Millisecond), "second"),
	})
	due, drops, overflow, _ := q.take(base.Add(time.Second), 0, 100, 1<<20, time.Second)
	if len(drops) != 0 || overflow != 0 {
		t.Fatalf("drops/overflow = %d/%d", len(drops), overflow)
	}
	var lines []string
	for _, e := range due {
		lines = append(lines, e.entry.Line)
	}
	if want := []string{"first", "second", "third"}; !slices.Equal(lines, want) {
		t.Fatalf("order = %v, want %v", lines, want)
	}
}

func TestQueueDelayHoldsEntriesUntilDue(t *testing.T) {
	q := testQueue(t, 16)
	base := time.Unix(100, 0).UTC()
	q.push([]queuedEntry{qentry(base.Add(500*time.Millisecond), "later")})

	due, _, _, wait := q.take(base, 2*time.Second, 100, 1<<20, 100*time.Millisecond)
	if len(due) != 0 {
		t.Fatalf("entry released before delay: %v", due)
	}
	if want := 100 * time.Millisecond; wait != want {
		t.Fatalf("next wait = %v, want flush cap %v", wait, want)
	}

	due, _, _, _ = q.take(base.Add(3*time.Second), 2*time.Second, 100, 1<<20, 100*time.Millisecond)
	if len(due) != 1 || due[0].entry.Line != "later" {
		t.Fatalf("due entries = %+v", due)
	}
}

func TestQueueDelayZeroFlushesImmediately(t *testing.T) {
	q := testQueue(t, 16)
	base := time.Unix(100, 0).UTC()
	q.push([]queuedEntry{qentry(base, "now")})
	due, _, _, wait := q.take(base, 0, 100, 1<<20, 100*time.Millisecond)
	if len(due) != 1 || due[0].entry.Line != "now" {
		t.Fatalf("due = %+v", due)
	}
	if wait != 100*time.Millisecond {
		t.Fatalf("wait = %v, want flush interval when empty", wait)
	}
}

func TestQueueLimitLeavesRestForNextFrame(t *testing.T) {
	q := testQueue(t, 16)
	base := time.Unix(100, 0).UTC()
	var in []queuedEntry
	for i := range 5 {
		in = append(in, qentry(base.Add(time.Duration(i)*time.Millisecond), string(rune('a'+i))))
	}
	q.push(in)
	first, _, _, _ := q.take(base.Add(time.Second), 0, 3, 1<<20, time.Second)
	if len(first) != 3 {
		t.Fatalf("first frame = %d, want limit 3", len(first))
	}
	second, _, _, _ := q.take(base.Add(time.Second), 0, 3, 1<<20, time.Second)
	if len(second) != 2 {
		t.Fatalf("second frame = %d, want remaining 2", len(second))
	}
}

func TestQueueBoundsAndDescribesLocalDrops(t *testing.T) {
	q := testQueue(t, 2)
	base := time.Unix(100, 0).UTC()
	var in []queuedEntry
	for i := range 4 {
		in = append(in, qentry(base.Add(time.Duration(i)*time.Millisecond), string(rune('a'+i))))
	}
	if dropped := q.push(in); dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	due, drops, overflow, _ := q.take(base.Add(time.Second), 0, 100, 1<<20, time.Second)
	if len(due) != 2 || due[0].entry.Line != "a" || due[1].entry.Line != "b" {
		t.Fatalf("kept = %+v", due)
	}
	if len(drops) != 2 || overflow != 0 {
		t.Fatalf("descriptors = %d, overflow = %d; want 2/0", len(drops), overflow)
	}
	if drops[0].labels.String() != `{job="api"}` {
		t.Fatalf("descriptor labels = %s", drops[0].labels)
	}
}

func TestQueueDescriptorOverflowIsExplicit(t *testing.T) {
	q := testQueue(t, 1)
	base := time.Unix(100, 0).UTC()
	// Fill the descriptor budget, then overflow it.
	for range maxDroppedDescriptors + 5 {
		q.push([]queuedEntry{qentry(base.Add(time.Hour), "kept")})
	}
	q.push([]queuedEntry{qentry(base.Add(time.Hour), "dropped-1"), qentry(base.Add(time.Hour), "dropped-2")})
	_, drops, overflow, _ := q.take(base, 0, 100, 1<<20, time.Second)
	if len(drops) != maxDroppedDescriptors {
		t.Fatalf("descriptors = %d, want cap %d", len(drops), maxDroppedDescriptors)
	}
	if overflow == 0 {
		t.Fatal("descriptor overflow was silently hidden")
	}
}

func TestQueuePushAfterCloseIsIgnored(t *testing.T) {
	q := testQueue(t, 4)
	q.close()
	if dropped := q.push([]queuedEntry{qentry(time.Unix(1, 0).UTC(), "after")}); dropped != 0 {
		t.Fatalf("push after close dropped %d", dropped)
	}
	due, drops, overflow, _ := q.take(time.Unix(2, 0).UTC(), 0, 10, 1<<20, time.Second)
	if len(due) != 0 || len(drops) != 0 || overflow != 0 {
		t.Fatalf("closed queue returned %v/%v/%d", due, drops, overflow)
	}
}
