package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// --- helpers ---------------------------------------------------------------

func testMemtable(t *testing.T, o ingest.MemtableOptions) (*ingest.Memtable, *memory.CheckedAllocator) {
	t.Helper()
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	o.Allocator = ca
	if o.StreamLabels == nil {
		o.StreamLabels = []string{"service_name", "job"}
	}
	m, err := ingest.NewMemtable(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return m, ca
}

func levelPromotion(string) []string { return []string{"level"} }

func ent(us int64, line string) logs.Entry {
	return logs.Entry{Timestamp: time.UnixMicro(us).UTC(), Line: line}
}

func entMeta(us int64, line string, meta ...logs.Label) logs.Entry {
	return logs.Entry{Timestamp: time.UnixMicro(us).UTC(), Line: line, Metadata: meta}
}

func stream(name string, ents ...logs.Entry) logs.Stream {
	return logs.Stream{Labels: logs.New(logs.Label{Name: "job", Value: name}), Entries: ents}
}

func push(streams ...logs.Stream) *logs.PushRequest {
	return &logs.PushRequest{Streams: streams}
}

// row is a decoded record-batch row for assertions.
type row struct {
	us       int64
	line     string
	labels   map[string]string
	meta     map[string]string
	promoted []any // nil for SQL null
}

func readMap(a arrow.Array, i int) map[string]string {
	m := a.(*array.Map)
	from, to := m.ValueOffsets(i)
	keys := m.Keys().(*array.String)
	items := m.Items().(*array.String)
	out := make(map[string]string, to-from)
	for j := from; j < to; j++ {
		out[keys.Value(int(j))] = items.Value(int(j))
	}
	return out
}

// rowsOf decodes a generation part into rows (columns 4+ are promoted).
func rowsOf(rec arrow.RecordBatch) []row {
	n := int(rec.NumRows())
	ts := rec.Column(0).(*array.Timestamp)
	lines := rec.Column(1).(*array.String)
	var out []row
	for i := 0; i < n; i++ {
		r := row{
			us:     int64(ts.Value(i)),
			line:   lines.Value(i),
			labels: readMap(rec.Column(2), i),
			meta:   readMap(rec.Column(3), i),
		}
		for c := 4; c < int(rec.NumCols()); c++ {
			col := rec.Column(c).(*array.String)
			if col.IsNull(i) {
				r.promoted = append(r.promoted, nil)
			} else {
				r.promoted = append(r.promoted, col.Value(i))
			}
		}
		out = append(out, r)
	}
	return out
}

// sealTake seals the tenant's generation and returns it.
func sealTake(t *testing.T, m *ingest.Memtable, id tenant.ID) *ingest.Generation {
	t.Helper()
	if err := m.Seal(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	gens, err := m.TakeSealed(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 {
		t.Fatalf("sealed generations = %d, want 1", len(gens))
	}
	t.Cleanup(gens[0].Release)
	return gens[0]
}

// --- tests -----------------------------------------------------------------

func TestMemtableTenantStreamIsolation(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	req := push(stream("api", ent(1000, "a1"), ent(2000, "a2")))
	for _, id := range []tenant.ID{"t1", "t2"} {
		if err := m.Append(t.Context(), id, 1, 10, req); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []tenant.ID{"t1", "t2"} {
		g := sealTake(t, m, id)
		if g.Tenant() != id {
			t.Fatalf("tenant = %s, want %s", g.Tenant(), id)
		}
		if f, l := g.Frontier(); f != 1 || l != 1 {
			t.Fatalf("frontier = %d-%d, want 1-1", f, l)
		}
		parts, err := g.Parts(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != 1 || parts[0].Stream != "api" {
			t.Fatalf("parts = %+v", parts)
		}
		rows := rowsOf(parts[0].Record)
		if len(rows) != 2 || rows[0].line != "a1" || rows[1].line != "a2" {
			t.Fatalf("rows = %+v", rows)
		}
	}
	// The other tenant sees nothing.
	gens, err := m.TakeSealed(t.Context(), "t3")
	if err != nil || len(gens) != 0 {
		t.Fatalf("t3 gens = %v, %v", gens, err)
	}
}

func TestMemtablePromotionAndMaps(t *testing.T) {
	promoted := func(string) []string { return []string{"level", "namespace"} }
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: promoted})
	s := logs.Stream{
		Labels: logs.New(
			logs.Label{Name: "job", Value: "api"},
			logs.Label{Name: "level", Value: "info"},
			logs.Label{Name: "pod", Value: "p1"},
		),
		Entries: []logs.Entry{
			entMeta(1000, "with meta", logs.Label{Name: "trace_id", Value: "t1"}),
			ent(2000, "plain"),
		},
	}
	if err := m.Append(t.Context(), "t1", 1, 10, push(s)); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rec := parts[0].Record
	if rec.Schema().Field(4).Name != "level" || rec.Schema().Field(5).Name != "namespace" {
		t.Fatalf("schema = %s", rec.Schema())
	}
	rows := rowsOf(rec)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v", rows)
	}
	r0 := rows[0]
	if r0.promoted[0] != "info" || r0.promoted[1] != nil {
		t.Fatalf("promoted = %v", r0.promoted)
	}
	if want := map[string]string{"job": "api", "pod": "p1"}; !reflect.DeepEqual(r0.labels, want) {
		t.Fatalf("labels = %v, want %v", r0.labels, want)
	}
	if want := map[string]string{"trace_id": "t1"}; !reflect.DeepEqual(r0.meta, want) {
		t.Fatalf("meta = %v, want %v", r0.meta, want)
	}
	if len(rows[1].meta) != 0 {
		t.Fatalf("meta = %v", rows[1].meta)
	}
}

func TestMemtableStreamFallbackAndSanitize(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	// No service_name/job label → "default"; odd service_name → sanitized.
	s1 := logs.Stream{Labels: logs.New(logs.Label{Name: "env", Value: "prod"}), Entries: []logs.Entry{ent(1, "x")}}
	s2 := logs.Stream{Labels: logs.New(logs.Label{Name: "service_name", Value: "My App!"}), Entries: []logs.Entry{ent(2, "y")}}
	if err := m.Append(t.Context(), "t1", 1, 10, push(s1, s2)); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, p := range parts {
		names[p.Stream] = true
	}
	if !names["default"] || !names["my_app_"] {
		t.Fatalf("streams = %v", names)
	}
}

func TestMemtableSortAndHours(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	const h = int64(time.Hour / time.Microsecond)
	// Out-of-order across three hours, one pre-epoch, equal timestamps for
	// stability.
	s := stream("api",
		ent(3*h+5, "late"),
		ent(1, "first"),
		ent(2*h+1, "mid1"),
		ent(-1, "pre-epoch"),
		ent(2*h+1, "mid2"),
		ent(7, "second"),
	)
	if err := m.Append(t.Context(), "t1", 1, 10, push(s)); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got []row
	var hours []time.Time
	for _, p := range parts {
		hours = append(hours, p.Hour)
		got = append(got, rowsOf(p.Record)...)
	}
	wantLines := []string{"pre-epoch", "first", "second", "mid1", "mid2", "late"}
	wantUS := []int64{-1, 1, 7, 2*h + 1, 2*h + 1, 3*h + 5}
	var lines []string
	var us []int64
	for _, r := range got {
		lines = append(lines, r.line)
		us = append(us, r.us)
	}
	if !reflect.DeepEqual(lines, wantLines) || !reflect.DeepEqual(us, wantUS) {
		t.Fatalf("sorted = %v %v", lines, us)
	}
	// Parts split exactly at hour boundaries: -1 (hour -1), hour 0, hour 2, hour 3.
	wantHours := []time.Time{time.UnixMicro(-h).UTC(), time.UnixMicro(0).UTC(), time.UnixMicro(2 * h).UTC(), time.UnixMicro(3 * h).UTC()}
	if !reflect.DeepEqual(hours, wantHours) {
		t.Fatalf("hours = %v, want %v", hours, wantHours)
	}
}

func TestMemtableThresholds(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion, MemtableMaxBytes: 1, WALMaxBytes: 1e9})
	if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
		t.Fatal(err)
	}
	gens, _ := m.TakeSealed(t.Context(), "t1")
	if len(gens) != 1 {
		t.Fatalf("mem threshold did not seal, gens = %d", len(gens))
	}
	gens[0].Release()

	m2, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion, WALMaxBytes: 5})
	if err := m2.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
		t.Fatal(err)
	}
	gens, _ = m2.TakeSealed(t.Context(), "t1")
	if len(gens) != 1 {
		t.Fatalf("wal threshold did not seal, gens = %d", len(gens))
	}
	gens[0].Release()
}

func TestMemtableEmptyPushFrontier(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	if err := m.Append(t.Context(), "t1", 7, 10, &logs.PushRequest{}); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	if f, l := g.Frontier(); f != 7 || l != 7 {
		t.Fatalf("frontier = %d-%d, want 7-7", f, l)
	}
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 0 {
		t.Fatalf("parts = %v", parts)
	}
	// The sequence continues after the empty generation.
	if err := m.Append(t.Context(), "t1", 8, 10, push(stream("api", ent(1, "x")))); err != nil {
		t.Fatal(err)
	}
	g2 := sealTake(t, m, "t1")
	if f, l := g2.Frontier(); f != 8 || l != 8 {
		t.Fatalf("frontier = %d-%d, want 8-8", f, l)
	}
}

func TestMemtableSequenceContiguity(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	req := push(stream("api", ent(1, "x")))
	// First sequence may resume above 1 after a checkpoint.
	if err := m.Append(t.Context(), "t1", 5, 10, req); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(t.Context(), "t1", 7, 10, req); !errors.Is(err, ingest.ErrSequence) {
		t.Fatalf("gap: err = %v, want ErrSequence", err)
	}
	if err := m.Append(t.Context(), "t1", 5, 10, req); !errors.Is(err, ingest.ErrSequence) {
		t.Fatalf("replay: err = %v, want ErrSequence", err)
	}
	if err := m.Append(t.Context(), "t1", 6, 10, req); err != nil {
		t.Fatalf("next: %v", err)
	}
	// Contiguity is per tenant.
	if err := m.Append(t.Context(), "t2", 42, 10, req); err != nil {
		t.Fatal(err)
	}
}

func TestMemtableNoPartialAdmission(t *testing.T) {
	promoted := func(name string) []string {
		if name == "bad" {
			return []string{"labels"} // reserved: schema.ForStream fails
		}
		return []string{"level"}
	}
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: promoted})
	good := stream("api", ent(1, "good"))
	bad := stream("bad", ent(2, "bad"))
	req := push(good, bad)
	if err := m.Append(t.Context(), "t1", 1, 10, req); err == nil {
		t.Fatal("expected promotion failure")
	}
	// The rejected request consumed no sequence and admitted no stream.
	if err := m.Append(t.Context(), "t1", 1, 10, push(good)); err != nil {
		t.Fatalf("re-append same seq: %v", err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0].Stream != "api" {
		t.Fatalf("parts = %+v", parts)
	}
}

// gatedAllocator blocks once inside a tracked allocation while armed, so a
// test can prove work proceeds while Generation.Parts is stuck materializing.
type gatedAllocator struct {
	memory.Allocator
	armed   atomic.Bool
	entered atomic.Bool
	blocked chan struct{}
	gate    chan struct{}
}

func newGatedAllocator() *gatedAllocator {
	return &gatedAllocator{
		Allocator: memory.NewGoAllocator(),
		blocked:   make(chan struct{}),
		gate:      make(chan struct{}),
	}
}

func (a *gatedAllocator) Allocate(n int) []byte {
	if a.armed.Load() && a.entered.CompareAndSwap(false, true) {
		close(a.blocked)
		<-a.gate
	}
	return a.Allocator.Allocate(n)
}

func TestMemtableProgressDuringMaterialization(t *testing.T) {
	gated := newGatedAllocator()
	ca := memory.NewCheckedAllocator(gated)
	// Registered first so it runs last (cleanups are LIFO), after the gate
	// release, generation Release and Close.
	t.Cleanup(func() { ca.AssertSize(t, 0) })
	m, err := ingest.NewMemtable(ingest.MemtableOptions{
		Allocator:    ca,
		StreamLabels: []string{"job"},
		PromotedFor:  levelPromotion,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	req := push(stream("api", ent(2, "b"), ent(1, "a")))
	if err := m.Append(t.Context(), "t1", 1, 10, req); err != nil {
		t.Fatal(err)
	}
	g1 := sealTake(t, m, "t1")
	// Runs before sealTake's Release cleanup (LIFO), so a failure below
	// cannot leave Parts holding g.mu and deadlock on Release.
	var gateOnce sync.Once
	t.Cleanup(func() { gateOnce.Do(func() { close(gated.gate) }) })

	// Materialization blocks on its first tracked allocation, inside the
	// sort. An Append must complete while it is stuck there, proving no
	// tenant lock is held during materialization.
	gated.armed.Store(true)
	var wg sync.WaitGroup
	var partsErr error
	var parts []ingest.Part
	wg.Go(func() {
		parts, partsErr = g1.Parts(t.Context())
	})
	select {
	case <-gated.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("Parts never reached a tracked allocation")
	}
	done := make(chan error, 1)
	go func() {
		done <- m.Append(t.Context(), "t1", 2, 10, push(stream("api", ent(3, "c"))))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Append blocked behind a materializing generation")
	}
	gateOnce.Do(func() { close(gated.gate) })
	wg.Wait()
	if partsErr != nil {
		t.Fatal(partsErr)
	}
	if len(parts) != 1 || int(parts[0].Record.NumRows()) != 2 {
		t.Fatalf("parts = %+v", parts)
	}
	g2 := sealTake(t, m, "t1")
	if f, l := g2.Frontier(); f != 2 || l != 2 {
		t.Fatalf("frontier = %d-%d, want 2-2", f, l)
	}
}

// canceledCtx fails Err() immediately.
func canceledCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

func TestGenerationPartsCancelRetry(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(2, "b"), ent(1, "a")))); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	// Cancellation leaves the detached builders usable for a retry.
	if _, err := g.Parts(canceledCtx(t)); err == nil {
		t.Fatal("Parts succeeded with a canceled context")
	}
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || int(parts[0].Record.NumRows()) != 2 {
		t.Fatalf("parts = %+v", parts)
	}
	// Cached: a second call returns the same parts.
	again, err := g.Parts(t.Context())
	if err != nil || len(again) != 1 {
		t.Fatalf("cached parts: %v %v", again, err)
	}
}

func TestMemtableAllocatorReturnsToZero(t *testing.T) {
	// Success path: seal → Parts → Release.
	func() {
		m, ca := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
		for seq := uint64(1); seq <= 10; seq++ {
			req := push(stream("api", ent(int64(seq), fmt.Sprintf("l%d", seq))))
			if err := m.Append(t.Context(), "t1", seq, 10, req); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Seal(t.Context(), "t1"); err != nil {
			t.Fatal(err)
		}
		gens, _ := m.TakeSealed(t.Context(), "t1")
		if len(gens) != 1 {
			t.Fatalf("gens = %d", len(gens))
		}
		if _, err := gens[0].Parts(t.Context()); err != nil {
			t.Fatal(err)
		}
		gens[0].Release()
		m.Close()
		ca.AssertSize(t, 0)
	}()
	// Close with an active (unsealed) generation.
	func() {
		m, ca := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
		if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
			t.Fatal(err)
		}
		m.Close()
		ca.AssertSize(t, 0)
	}()
	// Close with an untaken, unmaterialized sealed generation.
	func() {
		m, ca := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
		if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
			t.Fatal(err)
		}
		if err := m.Seal(t.Context(), "t1"); err != nil {
			t.Fatal(err)
		}
		m.Close()
		ca.AssertSize(t, 0)
	}()
	// Failed materialization, retry, then Release.
	func() {
		m, ca := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
		if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
			t.Fatal(err)
		}
		if err := m.Seal(t.Context(), "t1"); err != nil {
			t.Fatal(err)
		}
		gens, _ := m.TakeSealed(t.Context(), "t1")
		if _, err := gens[0].Parts(canceledCtx(t)); err == nil {
			t.Fatal("expected canceled Parts")
		}
		gens[0].Release()
		m.Close()
		ca.AssertSize(t, 0)
	}()
}

func TestMemtableOptionsValidation(t *testing.T) {
	if _, err := ingest.NewMemtable(ingest.MemtableOptions{}); err == nil {
		t.Error("nil PromotedFor accepted")
	}
	if _, err := ingest.NewMemtable(ingest.MemtableOptions{PromotedFor: levelPromotion, WALMaxBytes: -1}); err == nil {
		t.Error("negative limit accepted")
	}
}

func TestPrepareAppendPreparedRoundTrip(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	req := push(stream("api", ent(1000, "x")))
	p, err := m.Prepare(t.Context(), "t1", req)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the request after Prepare must not corrupt the prepared push.
	req.Streams[0].Entries[0].Metadata = logs.New(logs.Label{Name: "mut", Value: "1"})
	req.Streams[0].Labels[0].Value = "corrupted"
	if err := m.AppendPrepared(t.Context(), "t1", 1, 10, p); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rows := rowsOf(parts[0].Record)
	if len(rows) != 1 || len(rows[0].meta) != 0 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestAppendPreparedMismatch(t *testing.T) {
	m1, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	m2, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	req := push(stream("api", ent(1, "x")))
	p, err := m1.Prepare(t.Context(), "t1", req)
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.AppendPrepared(t.Context(), "t2", 1, 10, p); !errors.Is(err, ingest.ErrPreparedMismatch) {
		t.Fatalf("wrong tenant: err = %v, want ErrPreparedMismatch", err)
	}
	if err := m2.AppendPrepared(t.Context(), "t1", 1, 10, p); !errors.Is(err, ingest.ErrPreparedMismatch) {
		t.Fatalf("wrong table: err = %v, want ErrPreparedMismatch", err)
	}
	if err := m1.AppendPrepared(t.Context(), "t1", 1, 10, p); err != nil {
		t.Fatalf("right pair rejected: %v", err)
	}
}

func TestGenerationReleaseIdempotent(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	if err := m.Append(t.Context(), "t1", 1, 10, push(stream("api", ent(1, "x")))); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	g.Release()
	g.Release()
	if _, err := g.Parts(t.Context()); err == nil {
		t.Error("Parts after Release succeeded")
	}
}

func TestPrepareRejects(t *testing.T) {
	badLabels := logs.Labels{{Name: "b", Value: "1"}, {Name: "a", Value: "2"}} // unsorted
	badMeta := logs.Labels{{Name: "x"}, {Name: "x"}}
	cases := []struct {
		name string
		id   tenant.ID
		req  *logs.PushRequest
	}{
		{"nil request", "t1", nil},
		{"invalid tenant", "bad tenant!", push(stream("api", ent(1, "x")))},
		{"unsorted labels", "t1", push(logs.Stream{Labels: badLabels, Entries: []logs.Entry{ent(1, "x")}})},
		{"invalid label name", "t1", push(logs.Stream{Labels: logs.Labels{{Name: "bad-name", Value: "v"}}, Entries: []logs.Entry{ent(1, "x")}})},
		{"invalid metadata", "t1", push(logs.Stream{Labels: logs.New(logs.Label{Name: "job", Value: "api"}),
			Entries: []logs.Entry{{Timestamp: time.UnixMicro(1).UTC(), Line: "x", Metadata: badMeta}}})},
	}
	for _, c := range cases {
		m, ca := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
		if _, err := m.Prepare(t.Context(), c.id, c.req); err == nil {
			t.Errorf("%s: Prepare succeeded", c.name)
			continue
		}
		// Rejection allocated no Arrow data and consumed no sequence.
		ca.AssertSize(t, 0)
		if c.id == "t1" {
			if err := m.Append(t.Context(), c.id, 1, 10, push(stream("api", ent(1, "x")))); err != nil {
				t.Errorf("%s: seq after rejection: %v", c.name, err)
			}
		}
	}
}

func TestAppendPreparedSequenceBounds(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	p, err := m.Prepare(t.Context(), "t1", push(stream("api", ent(1, "x"))))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AppendPrepared(t.Context(), "t1", 0, 10, p); err == nil {
		t.Error("seq 0 accepted")
	}
	if err := m.AppendPrepared(t.Context(), "t1", math.MaxUint64, 10, p); err == nil {
		t.Error("wrapping seq accepted")
	}
	// Neither rejection consumed the sequence.
	if err := m.AppendPrepared(t.Context(), "t1", 1, 10, p); err != nil {
		t.Fatal(err)
	}
}

func TestMemtableNanosecondTruncation(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	mk := func(ns int64) logs.Entry {
		return logs.Entry{Timestamp: time.Unix(0, ns).UTC(), Line: fmt.Sprintf("%d", ns)}
	}
	req := push(stream("api", mk(1500), mk(1499), mk(-1500), mk(-2500), mk(1)))
	if err := m.Append(t.Context(), "t1", 1, 10, req); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	parts, err := g.Parts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for _, p := range parts {
		ts := p.Record.Column(0).(*array.Timestamp)
		for i := range int(ts.Len()) {
			got = append(got, int64(ts.Value(i)))
		}
	}
	// UnixMicro floors nanoseconds (toward -inf); sorted ascending afterwards.
	want := []int64{-3, -2, 0, 1, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("truncated _ts = %v, want %v", got, want)
	}
}

func TestGenerationPartsPreCanceled(t *testing.T) {
	m, _ := testMemtable(t, ingest.MemtableOptions{PromotedFor: levelPromotion})
	// Empty (frontier-only) generation: a canceled context still fails.
	if err := m.Append(t.Context(), "t1", 1, 10, &logs.PushRequest{}); err != nil {
		t.Fatal(err)
	}
	g := sealTake(t, m, "t1")
	if _, err := g.Parts(canceledCtx(t)); err == nil {
		t.Error("canceled Parts on empty generation succeeded")
	}
	parts, err := g.Parts(t.Context())
	if err != nil || len(parts) != 0 {
		t.Fatalf("parts = %v, %v", parts, err)
	}
	// Cached generation: pre-canceled context still fails before the cache.
	if _, err := g.Parts(canceledCtx(t)); err == nil {
		t.Error("canceled Parts on a cached generation succeeded")
	}
}
