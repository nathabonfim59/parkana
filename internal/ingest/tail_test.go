package ingest

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"iter"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// --- helpers ---------------------------------------------------------------

// newTestTailHub builds a hub with its own registry.
func newTestTailHub(t *testing.T, maxEntries int) *tailHub {
	t.Helper()
	m, err := newTailMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	h, err := newTailHub(maxEntries, m)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// tailPop waits up to timeout for the subscription's next event.
func tailPop(t *testing.T, sub *tailSubscription, timeout time.Duration) ([]wire.Entry, uint64, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	return sub.pop(ctx)
}

// tailService starts a service with the given live-tail buffer and fake WAL.
func tailService(t *testing.T, buffer int, fw *fakeWAL) (app.Deps, *service) {
	t.Helper()
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.LogQL.Tail.BufferEntries = buffer
	s := newTestService(t, deps, fakeWALOpt(fw))
	return deps, s
}

// stopTailService cancels Run and waits for it, returning Run's error.
func stopTailService(t *testing.T, cancel context.CancelFunc, done <-chan error) error {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("service did not stop")
		return nil
	}
}

// tailMessage decodes one NDJSON record.
func tailMessage(t *testing.T, br *bufio.Reader) wire.Message {
	t.Helper()
	line, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read tail record: %v", err)
	}
	var m wire.Message
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("decode tail record %q: %v", line, err)
	}
	return m
}

// tailSubscriberCount reports the hub's live subscribers for a tenant.
func tailSubscriberCount(h *tailHub, id tenant.ID) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs[id])
}

// tailMetric reads a collector's current value through a throwaway registry,
// so assertions see the metric even after a shutdown clears queues.
func tailMetric(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register metric: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if v := metric.GetCounter(); v != nil {
				return v.GetValue()
			}
			if v := metric.GetGauge(); v != nil {
				return v.GetValue()
			}
		}
	}
	t.Fatalf("metric %v has no value", c)
	return 0
}

// tailPublished returns the entries published to tenant's subscribers.
func tailPublished(t *testing.T, s *service) float64 {
	t.Helper()
	return tailMetric(t, s.tail.m.published.WithLabelValues(string(tenant.Default)))
}

// --- hub --------------------------------------------------------------------

func TestTailHubTenantIsolation(t *testing.T) {
	h := newTestTailHub(t, 16)
	defer h.close()
	a := h.subscribe(tenant.Default)
	b := h.subscribe(tenant.ID("other"))

	h.publish(tenant.Default, pushReq("only-default"))
	entries, _, ok := tailPop(t, a, time.Second)
	if !ok || len(entries) != 1 || entries[0].Line != "only-default" {
		t.Fatalf("default subscriber got %+v, ok=%t", entries, ok)
	}
	if entries, _, ok := tailPop(t, b, 50*time.Millisecond); ok {
		t.Fatalf("other tenant received %+v", entries)
	}

	h.publish(tenant.ID("other"), pushReq("only-other"))
	if entries, _, ok := tailPop(t, a, 50*time.Millisecond); ok {
		t.Fatalf("default tenant received %+v", entries)
	}
	entries, _, ok = tailPop(t, b, time.Second)
	if !ok || len(entries) != 1 || entries[0].Line != "only-other" {
		t.Fatalf("other subscriber got %+v, ok=%t", entries, ok)
	}
}

func TestTailHubEvictsSlowSubscriberWithoutBlocking(t *testing.T) {
	for _, tc := range []struct {
		name  string
		buf   int
		first []string
		next  []string
		want  uint64
	}{
		{"second push overflows", 2, []string{"a", "b"}, []string{"c"}, 3},
		{"one push larger than the buffer", 2, nil, []string{"a", "b", "c"}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestTailHub(t, tc.buf)
			defer h.close()
			sub := h.subscribe(tenant.Default)
			if len(tc.first) > 0 {
				h.publish(tenant.Default, pushReq(tc.first...))
			}

			start := time.Now()
			h.publish(tenant.Default, pushReq(tc.next...))
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("publish stalled for %v on a slow subscriber", elapsed)
			}
			if got := tailSubscriberCount(h, tenant.Default); got != 0 {
				t.Fatalf("subscribers = %d, want evicted", got)
			}

			entries, dropped, ok := tailPop(t, sub, time.Second)
			if !ok || len(entries) != 0 || dropped != tc.want {
				t.Fatalf("eviction event = entries %d, dropped %d, ok %t; want dropped %d", len(entries), dropped, ok, tc.want)
			}
			if _, _, ok := tailPop(t, sub, 50*time.Millisecond); ok {
				t.Fatal("evicted subscription was not drained")
			}

			// A subsequent push is unaffected by the eviction.
			next := h.subscribe(tenant.Default)
			h.publish(tenant.Default, pushReq("after"))
			entries, _, ok = tailPop(t, next, time.Second)
			if !ok || len(entries) != 1 || entries[0].Line != "after" {
				t.Fatalf("push after eviction = %+v, ok=%t", entries, ok)
			}
		})
	}
}

func TestTailHubClonesRequestAndNormalizesTimestamps(t *testing.T) {
	h := newTestTailHub(t, 16)
	defer h.close()
	sub := h.subscribe(tenant.Default)
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(logs.Label{Name: "job", Value: "api"}),
		Entries: []logs.Entry{{
			Timestamp: time.Unix(1, 123456789).UTC(),
			Line:      "original",
			Metadata:  logs.New(logs.Label{Name: "trace_id", Value: "abc"}),
		}},
	}}}
	h.publish(tenant.Default, req)

	// Mutating the request after publish must not reach the subscriber.
	req.Streams[0].Labels[0].Value = "mutated"
	req.Streams[0].Entries[0].Line = "mutated"
	req.Streams[0].Entries[0].Metadata[0].Value = "mutated"

	entries, _, ok := tailPop(t, sub, time.Second)
	if !ok || len(entries) != 1 {
		t.Fatalf("entries = %+v, ok=%t", entries, ok)
	}
	e := entries[0]
	if e.Line != "original" || len(e.Labels) != 1 || e.Labels[0].Value != "api" {
		t.Fatalf("publish did not clone the request: %+v", e)
	}
	if len(e.Metadata) != 1 || e.Metadata[0].Value != "abc" {
		t.Fatalf("metadata was not cloned: %+v", e.Metadata)
	}
	if want := strconv.FormatInt(time.Unix(1, 123456789).UnixMicro()*1000, 10); e.Timestamp != want {
		t.Fatalf("timestamp = %q, want microsecond-aligned %q", e.Timestamp, want)
	}
}

func TestTailHubCloseWakesSubscribers(t *testing.T) {
	h := newTestTailHub(t, 16)
	sub := h.subscribe(tenant.Default)
	h.publish(tenant.Default, pushReq("queued"))
	h.close()

	if entries, dropped, ok := tailPop(t, sub, time.Second); ok {
		t.Fatalf("closed hub returned entries %+v / dropped %d", entries, dropped)
	}
	if got := tailSubscriberCount(h, tenant.Default); got != 0 {
		t.Fatalf("subscribers after close = %d", got)
	}
	h.close() // idempotent
	h.publish(tenant.Default, pushReq("ignored"))
}

func TestTailHubConcurrentLifecycle(t *testing.T) {
	h := newTestTailHub(t, 8)
	defer h.close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 50 {
				sub := h.subscribe(tenant.Default)
				h.publish(tenant.Default, pushReq("x"))
				ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
				sub.pop(ctx)
				cancel()
				h.unsubscribe(sub)
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			sub := h.subscribe(tenant.ID("other"))
			h.publish(tenant.ID("other"), pushReq("y"))
			h.unsubscribe(sub)
		}
	})
	wg.Wait()
}

// --- durability hook --------------------------------------------------------

func TestTailPublishesOnlyAfterDurability(t *testing.T) {
	release := make(chan struct{})
	waiting := make(chan struct{}, 1)
	fw := &fakeWAL{}
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case waiting <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	_, s := tailService(t, 16, fw)
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(t.Context(), tenant.Default, pushReq("durable")) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("fake WAL durability wait was never entered")
	}

	// The WAL receipt is still pending: nothing may be visible, and the
	// persistent published counter must stay at zero.
	if got := tailPublished(t, s); got != 0 {
		t.Fatalf("published entries = %v before WAL fsync, want 0", got)
	}
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published before WAL fsync: %+v", entries)
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, dropped, ok := tailPop(t, sub, time.Second)
	if !ok || dropped != 0 || len(entries) != 1 {
		t.Fatalf("entries = %+v, dropped = %d, ok = %t", entries, dropped, ok)
	}
	if got := tailPublished(t, s); got != 1 {
		t.Fatalf("published entries = %v after fsync, want 1", got)
	}
	if entries[0].Line != "durable" || entries[0].Timestamp != "1600000000000000000" {
		t.Fatalf("entry = %+v", entries[0])
	}
	if len(entries[0].Labels) != 2 || entries[0].Labels[0].Name != "job" || entries[0].Labels[1].Name != "level" {
		t.Fatalf("labels = %+v, want sorted job,level", entries[0].Labels)
	}
	if _, _, ok := tailPop(t, sub, 50*time.Millisecond); ok {
		t.Fatal("one push published more than once")
	}

	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailDoesNotPublishFailedSync(t *testing.T) {
	fw := &fakeWAL{waitFn: func(context.Context, uint64) error {
		return errors.New("simulated fsync failure")
	}}
	_, s := tailService(t, 16, fw)
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.Default, pushReq("lost")); err == nil {
		t.Fatal("append succeeded despite a failed fsync")
	}
	if got := tailPublished(t, s); got != 0 {
		t.Fatalf("published entries = %v after failed fsync, want 0", got)
	}
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published after failed fsync: %+v", entries)
	}
	if err := stopTailService(t, cancel, done); err == nil {
		t.Fatal("Run reported success after a durability failure")
	}
}

func TestTailDoesNotPublishFailedAdmission(t *testing.T) {
	fw := &fakeWAL{}
	var calls int
	fw.enqueueFn = func(_ context.Context, id tenant.ID, _ wal.Payload) (enqueueResult, error) {
		calls++
		if calls == 1 {
			return fw.result(id, nil), nil
		}
		// Skip a sequence after the first admission, so the memtable
		// rejects the second push after the WAL accepted it.
		return enqueueResult{
			pos:  wal.Position{Tenant: id, Seq: 3},
			wait: func(context.Context) error { return nil },
		}, nil
	}
	_, s := tailService(t, 16, fw)
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	if err := s.append(t.Context(), tenant.Default, pushReq("first")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if entries, _, ok := tailPop(t, sub, time.Second); !ok || len(entries) != 1 || entries[0].Line != "first" {
		t.Fatalf("first push = %+v, ok=%t", entries, ok)
	}
	if got := tailPublished(t, s); got != 1 {
		t.Fatalf("published entries after first push = %v, want 1", got)
	}

	if err := s.append(t.Context(), tenant.Default, pushReq("rejected")); err == nil {
		t.Fatal("append succeeded despite a rejected memtable admission")
	}
	if got := tailPublished(t, s); got != 1 {
		t.Fatalf("published entries after rejected admission = %v, want 1", got)
	}
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published after failed admission: %+v", entries)
	}
	if err := stopTailService(t, cancel, done); err == nil {
		t.Fatal("Run reported success after a fatal admission failure")
	}
}

func TestTailCanceledClientNotPublished(t *testing.T) {
	release := make(chan struct{})
	enqueued := make(chan struct{}, 1)
	fw := &fakeWAL{}
	fw.waitFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fw.enqueueFn = func(_ context.Context, id tenant.ID, _ wal.Payload) (enqueueResult, error) {
		er := fw.result(id, nil)
		enqueued <- struct{}{}
		return er, nil
	}
	_, s := tailService(t, 16, fw)
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	ctx, cancelAppend := context.WithCancel(t.Context())
	appendDone := make(chan error, 1)
	go func() { appendDone <- s.append(ctx, tenant.Default, pushReq("canceled")) }()
	<-enqueued
	cancelAppend()
	if err := <-appendDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("append error = %v, want context.Canceled", err)
	}
	// The WAL record becomes durable after the client is gone; it must not
	// appear in tail because cancellation was observed first.
	close(release)
	time.Sleep(100 * time.Millisecond)
	if entries, _, ok := tailPop(t, sub, 100*time.Millisecond); ok {
		t.Fatalf("published a push whose client was canceled first: %+v", entries)
	}
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailReplayDoesNotPublish(t *testing.T) {
	fw := &fakeWAL{
		tenants: []tenant.ID{tenant.Default},
		replayFn: func(_ context.Context, id tenant.ID, _ wal.Position) iter.Seq2[wal.Record, error] {
			return func(yield func(wal.Record, error) bool) {
				yield(wal.Record{
					Position: wal.Position{Tenant: id, Seq: 1},
					Request:  *pushReq("from-wal"),
				}, nil)
			}
		},
	}
	_, s := tailService(t, 16, fw)
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	if entries, _, ok := tailPop(t, sub, 150*time.Millisecond); ok {
		t.Fatalf("replay re-tailed old rows: %+v", entries)
	}
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailReadyWhileReplayingThenStreamsNewPushes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fw := &fakeWAL{
		tenants: []tenant.ID{tenant.Default},
		replayFn: func(context.Context, tenant.ID, wal.Position) iter.Seq2[wal.Record, error] {
			return func(func(wal.Record, error) bool) {
				once.Do(func() { close(started) })
				<-release
			}
		},
	}
	deps, s := tailService(t, 16, fw)
	srv := httptest.NewServer(deps.HTTP.Handler())
	defer srv.Close()
	cancel, done := startRun(t, s)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("replay did not start")
	}

	// The internal stream opens during replay: ready means subscribed, not
	// accepting. Pushes are unavailable until replay finishes.
	ctx, cancelHTTP := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if ready := tailMessage(t, br); ready.Type != wire.TypeReady || ready.Tenant != string(tenant.Default) {
		t.Fatalf("ready during replay = %+v", ready)
	}
	if err := s.append(ctx, tenant.Default, pushReq("during-replay")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("push during replay = %v, want ErrUnavailable", err)
	}

	close(release)
	waitReady(t, s)
	if err := s.append(ctx, tenant.Default, pushReq("after-replay")); err != nil {
		t.Fatalf("append after replay: %v", err)
	}
	batch := tailMessage(t, br)
	if batch.Type != wire.TypeBatch || len(batch.Entries) != 1 || batch.Entries[0].Line != "after-replay" {
		t.Fatalf("first batch after replay = %+v", batch)
	}
	cancelHTTP()
	resp.Body.Close()
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailHandlerStopsOnServiceShutdown(t *testing.T) {
	deps, s := tailService(t, 16, &fakeWAL{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	srv := httptest.NewServer(deps.HTTP.Handler())
	defer srv.Close()

	ctx, cancelHTTP := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if ready := tailMessage(t, br); ready.Type != wire.TypeReady {
		t.Fatalf("first record = %+v", ready)
	}

	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("handler kept streaming after service shutdown")
	}
}

func TestTailHubClosedOnShutdown(t *testing.T) {
	_, s := tailService(t, 16, &fakeWAL{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	sub := s.tail.subscribe(tenant.Default)

	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, _, ok := tailPop(t, sub, time.Second); ok {
		t.Fatal("subscription survived service shutdown")
	}
}

func TestTailSlowSubscriberDoesNotBlockPush(t *testing.T) {
	_, s := tailService(t, 2, &fakeWAL{})
	sub := s.tail.subscribe(tenant.Default)
	cancel, done := startRun(t, s)
	waitReady(t, s)

	// Fill the buffer without reading it, then push more: the subscriber is
	// evicted and the push still completes promptly.
	if err := s.append(t.Context(), tenant.Default, pushReq("a", "b")); err != nil {
		t.Fatalf("filling append: %v", err)
	}
	start := time.Now()
	if err := s.append(t.Context(), tenant.Default, pushReq("c")); err != nil {
		t.Fatalf("overflow append: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("overflowing push stalled for %v", elapsed)
	}
	if _, dropped, ok := tailPop(t, sub, time.Second); !ok || dropped != 3 {
		t.Fatalf("dropped = %d, ok = %t; want 3", dropped, ok)
	}
	labels := []string{string(tenant.Default)}
	if got := tailPublished(t, s); got != 2 {
		t.Fatalf("published entries = %v, want 2 (the overflowing push was dropped)", got)
	}
	if got := tailMetric(t, s.tail.m.droppedEntries.WithLabelValues(labels...)); got != 3 {
		t.Fatalf("dropped entries metric = %v, want 3", got)
	}
	if got := tailMetric(t, s.tail.m.evictions.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("evictions metric = %v, want 1", got)
	}
	if got := tailMetric(t, s.tail.m.subscribers.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("subscribers metric = %v, want 0 after eviction", got)
	}
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// --- HTTP handler -----------------------------------------------------------

func TestTailHandlerStreamsReadyAndBatches(t *testing.T) {
	deps, s := tailService(t, 16, &fakeWAL{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	srv := httptest.NewServer(deps.HTTP.Handler())
	defer srv.Close()

	ctx, cancelHTTP := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != wire.ContentType {
		t.Fatalf("Content-Type = %q, want %q", got, wire.ContentType)
	}
	br := bufio.NewReader(resp.Body)

	ready := tailMessage(t, br)
	if ready.Type != wire.TypeReady || ready.Tenant != string(tenant.Default) || ready.BufferEntries != 16 {
		t.Fatalf("ready = %+v", ready)
	}
	if err := s.append(ctx, tenant.Default, pushReq("live")); err != nil {
		t.Fatalf("append: %v", err)
	}
	batch := tailMessage(t, br)
	if batch.Type != wire.TypeBatch || len(batch.Entries) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	if e := batch.Entries[0]; e.Line != "live" || e.Timestamp != "1600000000000000000" || len(e.Labels) != 2 {
		t.Fatalf("entry = %+v", e)
	}

	cancelHTTP()
	resp.Body.Close()
	eventually(t, "tail subscriber removed", func() bool {
		return tailSubscriberCount(s.tail, tenant.Default) == 0
	})
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailHandlerReportsDropped(t *testing.T) {
	deps, s := tailService(t, 2, &fakeWAL{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	srv := httptest.NewServer(deps.HTTP.Handler())
	defer srv.Close()

	ctx, cancelHTTP := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if ready := tailMessage(t, br); ready.Type != wire.TypeReady {
		t.Fatalf("first record = %+v", ready)
	}

	// The handler is not reading yet (it blocked in pop before this push was
	// queued), and three entries exceed the buffer: the subscriber is evicted
	// and the handler reports the loss, then ends.
	if err := s.append(ctx, tenant.Default, pushReq("a", "b", "c")); err != nil {
		t.Fatalf("append: %v", err)
	}
	dropped := tailMessage(t, br)
	if dropped.Type != wire.TypeDropped || dropped.Dropped != 3 || dropped.Reason == "" {
		t.Fatalf("dropped record = %+v", dropped)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("stream continued after a dropped record")
	}

	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestTailHandlerStopsOnHubClose(t *testing.T) {
	deps, s := tailService(t, 16, &fakeWAL{})
	srv := httptest.NewServer(deps.HTTP.Handler())
	defer srv.Close()

	ctx, cancelHTTP := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if ready := tailMessage(t, br); ready.Type != wire.TypeReady {
		t.Fatalf("first record = %+v", ready)
	}

	s.tail.close()
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("handler kept streaming after the hub closed")
	}
	if got := tailSubscriberCount(s.tail, tenant.Default); got != 0 {
		t.Fatalf("subscribers = %d after hub close", got)
	}
}

// smallWriteBufferListener shrinks the server-side socket buffer so a client
// that stops reading blocks the handler's write quickly.
type smallWriteBufferListener struct{ net.Listener }

func (l smallWriteBufferListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(4096)
	}
	return c, nil
}

// TestTailSlowClientWriteDeadline proves a client that stops reading cannot pin
// a handler past its write deadline: the handler returns even though its next
// pop would never run and its response body is still unread.
func TestTailSlowClientWriteDeadlineReleasesHandler(t *testing.T) {
	deps, s := tailService(t, 16, &fakeWAL{})
	s.tailWriteTimeout = 200 * time.Millisecond
	cancel, done := startRun(t, s)
	waitReady(t, s)

	handlerDone := make(chan struct{})
	var once sync.Once
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deps.HTTP.Handler().ServeHTTP(w, r)
		once.Do(func() { close(handlerDone) })
	})
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = smallWriteBufferListener{srv.Listener}
	srv.Start()
	defer srv.Close()

	dialer := &net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		})
	}}
	tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	ctx, cancelHTTP := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancelHTTP()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+strings.TrimPrefix(wire.Pattern, "GET "), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if ready := tailMessage(t, br); ready.Type != wire.TypeReady {
		t.Fatalf("first record = %+v", ready)
	}

	// The client stops reading here. A 4 MiB line cannot fit in the 4 KiB
	// socket buffers, so the handler blocks in Write until its deadline.
	if err := s.append(ctx, tenant.Default, pushReq(strings.Repeat("x", 4<<20))); err != nil {
		t.Fatalf("append: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler stayed blocked on a client that stopped reading")
	}
	if err := stopTailService(t, cancel, done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
