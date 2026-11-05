package tail

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestTailInvalidRequestsNeverUpgrade(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	for _, tc := range []struct {
		name   string
		values url.Values
		status int
		want   string
	}{
		{"missing query", url.Values{}, 400, "missing query parameter"},
		{"duplicate query", url.Values{"query": {"a", "b"}}, 400, "at most once"},
		{"zero limit", url.Values{"query": {"a"}, "limit": {"0"}}, 400, "limit must be a positive value"},
		{"limit over max entries", url.Values{"query": {"a"}, "limit": {"6000"}}, 400, "max entries limit per query exceeded"},
		{"negative delay", url.Values{"query": {"a"}, "delay_for": {"-1"}}, 400, "delay_for must be a positive value"},
		{"large delay", url.Values{"query": {"a"}, "delay_for": {"6"}}, 400, "delay_for can't be greater than 5"},
		{"bad start", url.Values{"query": {"a"}, "start": {"nope"}}, 400, "invalid start"},
		{"metric query", url.Values{"query": {`sum(rate({job="api"}[1m]))`}}, 400, "not a log query"},
		{"bad logql", url.Values{"query": {`{job=`}}, 400, "bad query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(h.srv.URL + "/loki/api/v1/tail?" + tc.values.Encode())
			if err != nil {
				t.Fatalf("GET tail: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d (body %q)", resp.StatusCode, tc.status, body)
			}
			if !strings.Contains(string(body), tc.want) {
				t.Fatalf("body = %q, want %q", body, tc.want)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
				t.Fatalf("content type = %q, want text/plain", ct)
			}
		})
	}
	select {
	case id := <-f.tenants:
		t.Fatalf("invalid request subscribed to the ingester as %q", id)
	default:
	}
}

func TestTailWithoutIngestersReturns503(t *testing.T) {
	h := newTailHarness(t, nil, nil)
	resp, err := http.Get(h.srv.URL + `/loki/api/v1/tail?query=%7Bjob%3D%22api%22%7D`)
	if err != nil {
		t.Fatalf("GET tail: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "no ingester URLs") {
		t.Fatalf("body = %q", body)
	}
}

func TestTailQueryOnlyDefaultsAndBatchLimit(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, func(s *service) { s.bufferEntries = 200 })
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	internalQuery := f.lastQuery(t)
	for _, name := range []string{"query", "limit", "delay_for", "start"} {
		if internalQuery.Has(name) {
			t.Fatalf("internal subscription leaked %q: %v", name, internalQuery)
		}
	}

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	entries := make([]wire.Entry, 150)
	for i := range entries {
		entries[i] = wireEntry(base.Add(time.Duration(i)*time.Millisecond), "line-"+strconv.Itoa(i), map[string]string{"job": "api"}, nil)
	}
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: entries})

	total := 0
	frames := 0
	for total < 150 {
		m, err := readTail(t, conn, 5*time.Second)
		if err != nil {
			t.Fatalf("read frame %d: %v", frames, err)
		}
		n := len(m.entries(t))
		if n > defaultTailLimit {
			t.Fatalf("frame %d carried %d entries, default limit is %d", frames, n, defaultTailLimit)
		}
		total += n
		frames++
	}
	if frames < 2 {
		t.Fatalf("150 entries arrived in %d frame(s); limit did not split the response", frames)
	}

	// The connection is not closed by the limit: a later row still arrives.
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(base.Add(time.Second), "after-limit", map[string]string{"job": "api"}, nil),
	}})
	got := readEntries(t, conn, 1, 5*time.Second)
	if len(got) != 1 || got[0].line != "after-limit" {
		t.Fatalf("post-limit entry = %+v", got)
	}
}

func TestTailForwardsTenantAndIsolatesSubscriptions(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	t1 := tenant.ID("tenant-one")
	t2 := tenant.ID("tenant-two")
	c1 := h.dial(t, `{job="api"}`, http.Header{tenant.Header: {string(t1)}})
	c2 := h.dial(t, `{job="api"}`, http.Header{tenant.Header: {string(t2)}})
	f.waitSubscribe(t, t1)
	f.waitSubscribe(t, t2)

	base := time.Now().UTC().Truncate(time.Microsecond)
	f.send(t, t1, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(base, "only-one", map[string]string{"job": "api"}, nil),
	}})
	got := readEntries(t, c1, 1, 5*time.Second)
	if got[0].line != "only-one" {
		t.Fatalf("tenant one got %+v", got)
	}

	f.send(t, t2, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(base, "only-two", map[string]string{"job": "api"}, nil),
	}})
	got = readEntries(t, c2, 1, 5*time.Second)
	if got[0].line != "only-two" {
		t.Fatalf("tenant two got %+v", got)
	}

	// A timed-out read closes the client connection in coder/websocket, so
	// the negative cross-tenant assertion comes last.
	if _, err := readTail(t, c1, 150*time.Millisecond); err == nil {
		t.Fatal("tenant one received tenant two's row")
	}
}

func TestTailRunsSharedPipelineAndPreservesMetadata(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Microsecond)
	entry := func(line string, meta map[string]string) wire.Entry {
		return wireEntry(base, line, map[string]string{"job": "api", "level": "info"}, meta)
	}

	t.Run("flat merges metadata and parsed labels", func(t *testing.T) {
		f := newFakeIngester(t, fakeIngesterOptions{})
		h := newTailHarness(t, []string{f.srv.URL}, nil)
		conn := h.dial(t, `{job="api"} |= "boom" | json`, nil)
		f.waitSubscribe(t, tenant.Default)
		f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
			entry("hello", nil),
			entry(`{"level":"error","msg":"boom"}`, map[string]string{"trace_id": "abc"}),
		}})
		got := readEntries(t, conn, 1, 5*time.Second)
		if len(got) != 1 || got[0].line != `{"level":"error","msg":"boom"}` {
			t.Fatalf("pipeline result = %+v", got)
		}
		labels := got[0].labels
		if labels["job"] != "api" || labels["level"] != "info" || labels["level_extracted"] != "error" || labels["msg"] != "boom" || labels["trace_id"] != "abc" {
			t.Fatalf("flat labels = %v", labels)
		}
	})

	t.Run("categorize-labels keeps categories per entry", func(t *testing.T) {
		f := newFakeIngester(t, fakeIngesterOptions{})
		h := newTailHarness(t, []string{f.srv.URL}, nil)
		conn := h.dial(t, `{job="api"} | json`, http.Header{"X-Loki-Response-Encoding-Flags": {"categorize-labels"}})
		f.waitSubscribe(t, tenant.Default)
		f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
			entry(`{"level":"error","msg":"boom"}`, map[string]string{"trace_id": "abc"}),
		}})
		got := readEntries(t, conn, 1, 5*time.Second)
		if len(got) != 1 {
			t.Fatalf("entries = %+v", got)
		}
		if len(got[0].labels) != 2 || got[0].labels["job"] != "api" || got[0].labels["level"] != "info" {
			t.Fatalf("stream labels = %v, want the base stream labels", got[0].labels)
		}
		if got[0].metadata["trace_id"] != "abc" || got[0].parsed["level_extracted"] != "error" || got[0].parsed["msg"] != "boom" {
			t.Fatalf("categories = %v / %v", got[0].metadata, got[0].parsed)
		}
	})
}

func TestTailStartFiltersIncomingWithoutBackfill(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	start := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Microsecond)
	conn := h.dialValues(t, url.Values{
		"query": {`{job="api"}`},
		"start": {strconv.FormatInt(start.UnixNano(), 10)},
	}, nil)
	f.waitSubscribe(t, tenant.Default)
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(start.Add(-time.Second), "before-start", map[string]string{"job": "api"}, nil),
		wireEntry(start, "at-start", map[string]string{"job": "api"}, nil),
		wireEntry(start.Add(time.Second), "after-start", map[string]string{"job": "api"}, nil),
	}})
	got := readEntries(t, conn, 2, 5*time.Second)
	if len(got) != 2 || got[0].line != "at-start" || got[1].line != "after-start" {
		t.Fatalf("start filter = %+v", got)
	}
}

func TestTailDelayOrdersAcrossIngestersWithFakeClock(t *testing.T) {
	clock := newFakeClock(time.Unix(1000, 0).UTC())
	first := newFakeIngester(t, fakeIngesterOptions{})
	second := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{first.srv.URL, second.srv.URL}, func(s *service) {
		s.clock = clock
		s.flushInterval = 100 * time.Millisecond
	})
	conn := h.dialValues(t, url.Values{"query": {`{job="api"}`}, "delay_for": {"1"}}, nil)
	first.waitSubscribe(t, tenant.Default)
	second.waitSubscribe(t, tenant.Default)

	base := clock.Now()
	e := func(offset time.Duration, line string) wire.Entry {
		return wireEntry(base.Add(offset), line, map[string]string{"job": "api"}, nil)
	}
	// Two ingesters deliver interleaved batches whose push order is not
	// timestamp order; the delay buffer must merge them by timestamp.
	first.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		e(-700*time.Millisecond, "first"),
		e(-300*time.Millisecond, "third"),
		e(100*time.Millisecond, "fifth"),
	}})
	second.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		e(-500*time.Millisecond, "second"),
		e(-100*time.Millisecond, "fourth"),
	}})
	waitQueued(t, h.svc, 5)
	clock.Advance(1500 * time.Millisecond)

	got := readEntries(t, conn, 5, 5*time.Second)
	var lines []string
	for _, entry := range got {
		lines = append(lines, entry.line)
	}
	if want := []string{"first", "second", "third", "fourth", "fifth"}; !slices.Equal(lines, want) {
		t.Fatalf("delay order = %v, want timestamp order %v", lines, want)
	}
}

func TestTailLocalOverflowEmitsRealDroppedDescriptors(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, func(s *service) {
		s.bufferEntries = 2
	})
	conn := h.dialValues(t, url.Values{"query": {`{job="api"}`}, "delay_for": {"4"}}, nil)
	f.waitSubscribe(t, tenant.Default)

	base := time.Now().UTC().Truncate(time.Microsecond)
	entries := make([]wire.Entry, 4)
	for i := range entries {
		entries[i] = wireEntry(base.Add(time.Duration(i)*time.Millisecond), "line-"+strconv.Itoa(i), map[string]string{"job": "api"}, nil)
	}
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: entries})

	m, err := readTail(t, conn, 5*time.Second)
	if err != nil {
		t.Fatalf("read drops: %v", err)
	}
	if len(m.Streams) != 0 {
		t.Fatalf("held entries were written before their delay: %+v", m.Streams)
	}
	if len(m.Dropped) != 2 {
		t.Fatalf("dropped = %+v, want 2 descriptors", m.Dropped)
	}
	if m.Dropped[0].Labels["job"] != "api" || m.Dropped[0].Timestamp == "" {
		t.Fatalf("descriptor = %+v", m.Dropped[0])
	}
	eventually(t, "drop metric", func() bool {
		return metricValue(t, h.reg, "parkana_tail_dropped_entries_total") == 2
	})
}

func TestTailMultipleIngestersKeepDuplicates(t *testing.T) {
	f1 := newFakeIngester(t, fakeIngesterOptions{})
	f2 := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f1.srv.URL, f2.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f1.waitSubscribe(t, tenant.Default)
	f2.waitSubscribe(t, tenant.Default)

	e := wireEntry(time.Now().UTC().Truncate(time.Microsecond), "duplicate", map[string]string{"job": "api"}, nil)
	f1.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{e}})
	f2.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{e}})

	got := readEntries(t, conn, 2, 5*time.Second)
	if len(got) != 2 || got[0].line != "duplicate" || got[1].line != "duplicate" {
		t.Fatalf("duplicates = %+v, want both retained", got)
	}
}

func TestTailUpstreamFailuresAreExplicit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   fakeIngesterOptions
		after  []byte
		reason string
	}{
		{name: "http error", opts: fakeIngesterOptions{status: http.StatusInternalServerError}, reason: "returned HTTP 500"},
		{name: "wrong ready tenant", opts: fakeIngesterOptions{ready: func(tenant.ID) []byte {
			return mustJSON(wire.Message{Type: wire.TypeReady, Tenant: "someone-else"})
		}}, reason: "does not match request tenant"},
		{name: "wrong ready type", opts: fakeIngesterOptions{ready: func(tenant.ID) []byte {
			return mustJSON(wire.Message{Type: wire.TypeBatch})
		}}, reason: "first record"},
		{name: "malformed ready", opts: fakeIngesterOptions{rawFirst: []byte(`{"type":`)}, reason: "malformed ready record"},
		{name: "oversized record", opts: fakeIngesterOptions{rawFirst: make([]byte, maxRecordBytes+1)}, reason: "token too long"},
		{name: "eof after ready", opts: fakeIngesterOptions{eofAfterReady: true}, reason: "closed the tail stream"},
		{name: "type dropped", after: mustJSON(wire.Message{Type: wire.TypeDropped, Dropped: 3, Reason: "slow subscriber"}), reason: "dropped 3 entries"},
		{name: "unknown type", after: []byte(`{"type":"nonsense"}`), reason: "unknown record type"},
		{name: "malformed record", after: []byte(`{bad`), reason: "malformed record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeIngester(t, tc.opts)
			h := newTailHarness(t, []string{f.srv.URL}, nil)
			conn := h.dial(t, `{job="api"}`, nil)
			f.waitSubscribe(t, tenant.Default)
			if tc.after != nil {
				f.sendRaw(t, tenant.Default, tc.after)
			}
			err := readClose(t, conn, 5*time.Second)
			code, reason := closeInfo(err)
			if code != websocket.StatusInternalError {
				t.Fatalf("close = %d %q, want %d", code, reason, websocket.StatusInternalError)
			}
			if !strings.Contains(reason, tc.reason) {
				t.Fatalf("close reason = %q, want %q", reason, tc.reason)
			}
			if strings.Contains(reason, "?query=") || strings.Contains(reason, "@") {
				t.Fatalf("close reason leaked request details: %q", reason)
			}
			if id := f.waitHangup(t); id != tenant.Default {
				t.Fatalf("upstream hangup tenant = %q", id)
			}
			waitNoSessions(t, h)
			waitNoSubscribers(t, h)
			eventually(t, "upstream error metric", func() bool {
				return metricValue(t, h.reg, "parkana_tail_upstream_errors_total") == 1
			})
		})
	}
}

func TestTailFailedPeerTearsDownSurvivingIngester(t *testing.T) {
	good := newFakeIngester(t, fakeIngesterOptions{})
	// The bad peer fails only after the good one has subscribed; otherwise
	// the session can tear down before the good peer is ever dialed.
	subscribed := make(chan struct{})
	bad := newFakeIngester(t, fakeIngesterOptions{status: http.StatusInternalServerError, failAfter: subscribed})
	h := newTailHarness(t, []string{good.srv.URL, bad.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	good.waitSubscribe(t, tenant.Default)
	close(subscribed)

	err := readClose(t, conn, 5*time.Second)
	code, reason := closeInfo(err)
	if code != websocket.StatusInternalError || !strings.Contains(reason, "returned HTTP 500") {
		t.Fatalf("close = %d %q", code, reason)
	}
	if id := good.waitHangup(t); id != tenant.Default {
		t.Fatalf("surviving ingester hangup tenant = %q", id)
	}
	if id := bad.waitHangup(t); id != tenant.Default {
		t.Fatalf("failed ingester hangup tenant = %q", id)
	}
	waitNoSessions(t, h)
	waitNoSubscribers(t, h)
}

func TestTailStalledReadinessTimesOutBeforeAnyData(t *testing.T) {
	ready := newFakeIngester(t, fakeIngesterOptions{})
	stalled := newFakeIngester(t, fakeIngesterOptions{noReady: true})
	h := newTailHarness(t, []string{ready.srv.URL, stalled.srv.URL}, func(s *service) {
		s.readyTimeout = 150 * time.Millisecond
	})
	conn := h.dial(t, `{job="api"}`, nil)
	ready.waitSubscribe(t, tenant.Default)
	ready.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(time.Now().UTC(), "must-not-be-written", map[string]string{"job": "api"}, nil),
	}})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dataMessages := 0
	var err error
	for {
		_, _, err = conn.Read(ctx)
		if err != nil {
			break
		}
		dataMessages++
	}
	code, reason := closeInfo(err)
	if code != websocket.StatusInternalError || !strings.Contains(reason, "timed out waiting") {
		t.Fatalf("close = %d %q", code, reason)
	}
	if dataMessages != 0 {
		t.Fatalf("wrote %d messages before all ingesters were ready", dataMessages)
	}
}

func TestTailPingsKeepIdleConnectionAlive(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, func(s *service) {
		s.pingPeriod = 20 * time.Millisecond
		s.pingTimeout = 100 * time.Millisecond
	})
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	msgs := make(chan decodedTail, 4)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, data, err := conn.Read(ctx)
			cancel()
			if err != nil {
				errs <- err
				return
			}
			var m decodedTail
			if err := json.Unmarshal(data, &m); err != nil {
				errs <- err
				return
			}
			msgs <- m
		}
	})
	// Several ping periods with no data: without ping/pong the server would
	// have closed the connection before the entry below.
	time.Sleep(120 * time.Millisecond)
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(time.Now().UTC().Truncate(time.Microsecond), "after-pings", map[string]string{"job": "api"}, nil),
	}})
	select {
	case m := <-msgs:
		entries := m.entries(t)
		if len(entries) != 1 || entries[0].line != "after-pings" {
			t.Fatalf("entry after pings = %+v", entries)
		}
	case err := <-errs:
		t.Fatalf("read before entry: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for entry after pings")
	}
	_ = conn.CloseNow()
	wg.Wait()
}

func TestTailClientDisconnectStopsUpstream(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("client close: %v", err)
	}
	if id := f.waitHangup(t); id != tenant.Default {
		t.Fatalf("upstream hangup tenant = %q", id)
	}
	eventually(t, "session removal", func() bool {
		h.svc.mu.Lock()
		defer h.svc.mu.Unlock()
		return len(h.svc.sessions) == 0
	})
}

func TestTailDescriptorOverflowClosesExplicitly(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	// Force the queue's descriptor budget past its cap. This is the same
	// state a >1000-entry undescribed overflow produces; it must end the
	// stream explicitly rather than send a partial dropped_entries list.
	h.svc.mu.Lock()
	for sess := range h.svc.sessions {
		sess.queue.mu.Lock()
		sess.queue.descriptorOverflow = 7
		sess.queue.mu.Unlock()
		sess.queue.signal()
	}
	h.svc.mu.Unlock()

	err := readClose(t, conn, 5*time.Second)
	code, reason := closeInfo(err)
	if code != websocket.StatusInternalError {
		t.Fatalf("close = %d %q, want 1011", code, reason)
	}
	if !strings.Contains(reason, "7 entries without descriptors") {
		t.Fatalf("reason = %q, want the undescribed count", reason)
	}
}

func TestTailClientCancelDuringStalledReadiness(t *testing.T) {
	stalled := newFakeIngester(t, fakeIngesterOptions{noReady: true})
	h := newTailHarness(t, []string{stalled.srv.URL}, func(s *service) {
		s.readyTimeout = time.Minute // only the client cancel may end this
	})
	conn := h.dial(t, `{job="api"}`, nil)
	stalled.waitSubscribe(t, tenant.Default)

	if err := conn.Close(websocket.StatusNormalClosure, "cancel"); err != nil {
		t.Fatalf("client close: %v", err)
	}
	eventually(t, "upstream cancellation", func() bool {
		select {
		case <-stalled.hangups:
			return true
		default:
			return false
		}
	})
	eventually(t, "session removal", func() bool {
		h.svc.mu.Lock()
		defer h.svc.mu.Unlock()
		return len(h.svc.sessions) == 0
	})
}

func TestTailOversizedEntryClosesExplicitly(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	ts := time.Now().Add(-time.Second).UTC()
	h.svc.mu.Lock()
	for sess := range h.svc.sessions {
		sess.queue.push([]queuedEntry{{
			ts:     ts,
			labels: logs.New(logs.Label{Name: "job", Value: "api"}),
			entry:  plan.Entry{TS: ts, Line: strings.Repeat(`"`, maxFrameBytes)},
		}})
	}
	h.svc.mu.Unlock()

	err := readClose(t, conn, 5*time.Second)
	code, reason := closeInfo(err)
	if code != websocket.StatusInternalError || !strings.Contains(reason, "maximum WebSocket frame size") {
		t.Fatalf("close = %d %q, want explicit frame-size error", code, reason)
	}
}

// recordingSocket is a socket whose writes are counted, so writeLoop's
// error precedence can be tested without a network.
type recordingSocket struct {
	mu     sync.Mutex
	writes int
}

func (r *recordingSocket) Write(context.Context, websocket.MessageType, []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes++
	return nil
}
func (r *recordingSocket) Ping(context.Context) error                    { return nil }
func (r *recordingSocket) Close(websocket.StatusCode, string) error      { return nil }
func (r *recordingSocket) CloseNow() error                               { return nil }
func (r *recordingSocket) CloseRead(ctx context.Context) context.Context { return ctx }

func (r *recordingSocket) writeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writes
}

func TestWriteLoopChecksFailureBeforeBusyQueue(t *testing.T) {
	m, err := newTailMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	q := newDelayQueue(500, m, tenant.Default)
	base := time.Now().Add(-time.Hour).UTC()
	var entries []queuedEntry
	for i := range 500 {
		entries = append(entries, qentry(base.Add(time.Duration(i)*time.Millisecond), "busy"))
	}
	q.push(entries)

	sock := &recordingSocket{}
	sess := &session{
		svc: &service{
			clock:         realClock{},
			flushInterval: time.Millisecond,
			writeTimeout:  time.Second,
			pingPeriod:    time.Hour,
			pingTimeout:   time.Second,
			m:             m,
			log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		conn:   sock,
		tenant: tenant.Default,
		req:    tailRequest{limit: 100},
		queue:  q,
		errs:   make(chan error, 1),
	}
	sess.errs <- errors.New("ingester dropped 9 entries; the live tail is incomplete")

	res := sess.writeLoop(t.Context())
	if got := sock.writeCount(); got != 0 {
		t.Fatalf("wrote %d frames before surfacing the pending upstream gap", got)
	}
	if res.code != websocket.StatusInternalError || !strings.Contains(res.reason, "dropped 9 entries") {
		t.Fatalf("result = %d %q", res.code, res.reason)
	}
}

func TestTailSessionClosesBoundedQueueOnExit(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dialValues(t, url.Values{"query": {`{job="api"}`}, "delay_for": {"4"}}, nil)
	f.waitSubscribe(t, tenant.Default)

	var queue *delayQueue
	eventually(t, "session registration", func() bool {
		h.svc.mu.Lock()
		defer h.svc.mu.Unlock()
		for sess := range h.svc.sessions {
			queue = sess.queue
			return true
		}
		return false
	})

	base := time.Now().UTC().Truncate(time.Microsecond)
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(base, "held-1", map[string]string{"job": "api"}, nil),
		wireEntry(base.Add(time.Millisecond), "held-2", map[string]string{"job": "api"}, nil),
		wireEntry(base.Add(2*time.Millisecond), "held-3", map[string]string{"job": "api"}, nil),
	}})
	waitQueued(t, h.svc, 3)

	if err := conn.CloseNow(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	waitNoSessions(t, h)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if !queue.closed {
		t.Fatal("session exit did not close its delay queue")
	}
	if len(queue.h) != 0 || queue.pendingDrops != nil {
		t.Fatalf("queue retained %d entries / %d descriptors after exit", len(queue.h), len(queue.pendingDrops))
	}
}

func TestTailServiceShutdownClosesActiveSessions(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)

	// Read the close frame while the service shuts down: coder's Close waits
	// for the peer's close handshake, and a client that is not reading would
	// only delay it.
	closeErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			_, _, err := conn.Read(ctx)
			if err != nil {
				closeErr <- err
				return
			}
		}
	}()
	h.stop(t)
	err := <-closeErr
	code, reason := closeInfo(err)
	if code != websocket.StatusGoingAway || !strings.Contains(reason, "shutting down") {
		t.Fatalf("close = %d %q, want 1001 shutdown", code, reason)
	}
	if id := f.waitHangup(t); id != tenant.Default {
		t.Fatalf("upstream hangup tenant = %q", id)
	}

	resp, err := http.Get(h.srv.URL + `/loki/api/v1/tail?query=%7Bjob%3D%22api%22%7D`)
	if err != nil {
		t.Fatalf("post-shutdown request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("post-shutdown status = %d, want 503", resp.StatusCode)
	}
}

func TestTailMetricsTrackSubscribersAndEntries(t *testing.T) {
	f := newFakeIngester(t, fakeIngesterOptions{})
	h := newTailHarness(t, []string{f.srv.URL}, nil)
	conn := h.dial(t, `{job="api"}`, nil)
	f.waitSubscribe(t, tenant.Default)
	f.send(t, tenant.Default, wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{
		wireEntry(time.Now().UTC(), "metered", map[string]string{"job": "api"}, nil),
	}})
	readEntries(t, conn, 1, 5*time.Second)
	eventually(t, "subscriber gauge", func() bool {
		return metricValue(t, h.reg, "parkana_tail_subscribers") == 1
	})
	// The client can read the entry before the counter is incremented.
	eventually(t, "entries metric", func() bool {
		v, _ := gatherMetric(t, h.reg, "parkana_tail_entries_total")
		return v == 1
	})
	_ = conn.CloseNow()
	eventually(t, "subscriber gauge decrement", func() bool {
		return metricValue(t, h.reg, "parkana_tail_subscribers") == 0
	})
}

func TestParseIngestersRejectsUnsafeURLs(t *testing.T) {
	for _, raw := range []string{
		"ftp://example.com",
		"http://",
		"http://user:pass@example.com",
		"http://example.com/?token=secret",
		"http://example.com/#frag",
	} {
		_, err := parseIngesters([]string{raw})
		if err == nil {
			t.Fatalf("parseIngesters(%q) accepted an unsafe URL", raw)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "pass") {
			t.Fatalf("error leaked credentials/query: %v", err)
		}
	}
	if got, err := parseIngesters([]string{"https://ingester.example/prefix"}); err != nil || len(got) != 1 || got[0].display != "https://ingester.example/prefix" {
		t.Fatalf("valid URL = %+v, %v", got, err)
	}
}
