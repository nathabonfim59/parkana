package tail_test

// Live-tail latency acceptance: a real ingester and querier on one HTTP server,
// 24 single-line pushes paced at 4/s, and one already-ready WebSocket reader.
// Every push is acknowledged only after its WAL fsync, so the HTTP completion
// time is the conservative durability bound. The reader records the monotonic
// frame-arrival time as soon as Read returns. The test reports the signed
// acknowledgement-to-frame delta (negative means the frame overtook the HTTP
// response), the explicitly named residual max(0, delta) and the conservative
// push-start-to-frame latency. The 100ms production flush interval is left at
// its default, so buffering is exposed rather than tuned away.

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/schema"
)

const (
	latencyPushes   = 24
	latencyInterval = 250 * time.Millisecond // 4 pushes per second
)

// latencyPush is one acknowledged push and the row identity its frame must
// carry back.
type latencyPush struct {
	id    int
	start time.Time
	ack   time.Time
	ts    int64
	line  string
}

// latencyArrival is the first frame entry seen for one pushed id.
type latencyArrival struct {
	at     time.Time
	ts     string
	line   string
	labels map[string]string
}

func TestLiveTailLatency(t *testing.T) {
	stack := startTailStack(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dialCtx, stack.wsBase+"/loki/api/v1/tail?query="+url.QueryEscape(`{job="api"}`)+"&delay_for=0", nil)
	dialCancel()
	if err != nil {
		t.Fatalf("dial tail: %v", err)
	}
	defer conn.CloseNow()
	// Do not push until both the querier subscriber and the ingester
	// subscriber exist; a push before that would race the subscription.
	waitMetricAtLeast(t, stack.reg, "parkana_tail_subscribers", 1)
	waitMetricAtLeast(t, stack.reg, "parkana_ingest_tail_subscribers", 1)

	readCtx, stopRead := context.WithCancel(ctx)
	readDone := make(chan struct{})
	defer func() {
		stopRead()
		<-readDone
	}()
	var (
		mu       sync.Mutex
		arrivals = make(map[int]latencyArrival, latencyPushes)
		readErr  = make(chan error, 1)
	)
	reportReadErr := func(err error) {
		select {
		case readErr <- err:
		default:
		}
	}
	go func() {
		defer close(readDone)
		for {
			messageType, data, err := conn.Read(readCtx)
			at := time.Now()
			if err != nil {
				if readCtx.Err() == nil {
					reportReadErr(err)
				}
				return
			}
			entries, err := decodeLatencyFrame(messageType, data)
			if err != nil {
				reportReadErr(err)
				return
			}
			mu.Lock()
			for _, entry := range entries {
				entry.at = at
				if err := recordLatencyArrival(arrivals, entry); err != nil {
					mu.Unlock()
					reportReadErr(err)
					return
				}
			}
			mu.Unlock()
		}
	}()

	client := &http.Client{Timeout: 10 * time.Second}
	pushes := make([]latencyPush, 0, latencyPushes)
	for i := range latencyPushes {
		start := time.Now()
		ts := start.UnixNano()
		line := fmt.Sprintf("latency-id=%d", i)
		body := fmt.Sprintf(`{"streams":[{"stream":{"job":"api"},"values":[["%d",%q]]}]}`, ts, line)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, stack.base+"/loki/api/v1/push", strings.NewReader(body))
		if err != nil {
			t.Fatalf("push %d request: %v", i, err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
		// Reading and closing the body completes the HTTP exchange; the
		// acknowledgement is only valid once the whole response is consumed.
		_, copyErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		ack := time.Now()
		if copyErr != nil || closeErr != nil {
			t.Fatalf("push %d response: read=%v close=%v", i, copyErr, closeErr)
		}
		if resp.StatusCode/100 != 2 {
			t.Fatalf("push %d status = %d", i, resp.StatusCode)
		}
		pushes = append(pushes, latencyPush{id: i, start: start, ack: ack, ts: ts, line: line})
		if i < latencyPushes-1 {
			if wait := time.Until(start.Add(latencyInterval)); wait > 0 {
				time.Sleep(wait)
			}
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		got := len(arrivals)
		mu.Unlock()
		if got == latencyPushes {
			break
		}
		select {
		case err := <-readErr:
			t.Fatalf("tail reader: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d frames, got %d", latencyPushes, got)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Give two default flush intervals to expose a duplicate or malformed
	// trailing frame, then require the reader to have stayed healthy.
	time.Sleep(2 * config.DefaultTailFlushInterval)
	select {
	case err := <-readErr:
		t.Fatalf("tail reader: %v", err)
	default:
	}

	mu.Lock()
	gotArrivals := maps.Clone(arrivals)
	mu.Unlock()
	if len(gotArrivals) != latencyPushes {
		t.Fatalf("arrivals = %d, want exactly %d unique ids", len(gotArrivals), latencyPushes)
	}

	var residuals, requestLatencies, signedDeltas []time.Duration
	framesBeforeAck := 0
	for _, push := range pushes {
		arrival, ok := gotArrivals[push.id]
		if !ok {
			t.Fatalf("push %d never arrived", push.id)
		}
		if arrival.line != push.line {
			t.Errorf("push %d line = %q, want %q", push.id, arrival.line, push.line)
		}
		if want := map[string]string{"job": "api"}; !maps.Equal(arrival.labels, want) {
			t.Errorf("push %d labels = %v, want %v", push.id, arrival.labels, want)
		}
		if want := strconv.FormatInt(schema.TS(time.Unix(0, push.ts))*1000, 10); arrival.ts != want {
			t.Errorf("push %d timestamp = %q, want microsecond-grid %q", push.id, arrival.ts, want)
		}
		signed := arrival.at.Sub(push.ack)
		if signed < 0 {
			framesBeforeAck++
		}
		signedDeltas = append(signedDeltas, signed)
		residuals = append(residuals, max(signed, 0))
		requestLatencies = append(requestLatencies, arrival.at.Sub(push.start))
	}

	for i, residual := range residuals {
		if residual >= time.Second {
			t.Errorf("push %d ack-to-frame residual = %s, want < 1s", pushes[i].id, residual)
		}
	}
	// No arbitrary bound is placed on the conservative push-start latency: it
	// includes the request, the WAL fsync and the response, so it is recorded
	// rather than asserted beyond the residual durability bound.
	t.Logf("tail latency: n=%d ack-to-frame signed=%s p50=%s max=%s frames-before-ack=%d; push-start-to-frame p50=%s max=%s",
		latencyPushes, describeDurations(signedDeltas), p50(residuals), slices.Max(residuals),
		framesBeforeAck, p50(requestLatencies), slices.Max(requestLatencies))

	assertNoTailLoss(t, stack)
}

// decodeLatencyFrame validates one Loki tail frame and returns its entries.
func decodeLatencyFrame(messageType websocket.MessageType, data []byte) ([]latencyArrival, error) {
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("tail frame message type = %v, want text", messageType)
	}
	var msg struct {
		Streams []struct {
			Stream map[string]string  `json:"stream"`
			Values [][]jsontext.Value `json:"values"`
		} `json:"streams"`
		DroppedEntries jsontext.Value `json:"dropped_entries"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("decode tail frame: %w", err)
	}
	if msg.DroppedEntries.Kind() != jsontext.KindInvalid && msg.DroppedEntries.Kind() != jsontext.KindNull {
		return nil, fmt.Errorf("tail frame reports dropped entries: %s", msg.DroppedEntries)
	}
	var entries []latencyArrival
	for _, stream := range msg.Streams {
		if stream.Stream == nil {
			return nil, fmt.Errorf("tail stream is missing labels")
		}
		for i, tuple := range stream.Values {
			if len(tuple) != 2 {
				return nil, fmt.Errorf("tail value %d: want [timestamp, line], got %d elements", i, len(tuple))
			}
			if tuple[0].Kind() != jsontext.KindString || tuple[1].Kind() != jsontext.KindString {
				return nil, fmt.Errorf("tail value %d: timestamp and line must be strings", i)
			}
			var ts, line string
			if err := json.Unmarshal(tuple[0], &ts); err != nil {
				return nil, fmt.Errorf("tail value %d timestamp: %w", i, err)
			}
			if err := json.Unmarshal(tuple[1], &line); err != nil {
				return nil, fmt.Errorf("tail value %d line: %w", i, err)
			}
			entries = append(entries, latencyArrival{ts: ts, line: line, labels: maps.Clone(stream.Stream)})
		}
	}
	return entries, nil
}

// parseLatencyLine extracts the pushed id from a latency marker line.
func parseLatencyLine(line string) (int, bool) {
	const prefix = "latency-id="
	if !strings.HasPrefix(line, prefix) {
		return 0, false
	}
	id, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
	if err != nil || id < 0 || id >= latencyPushes {
		return 0, false
	}
	return id, true
}

// recordLatencyArrival records the first frame entry for a pushed id. A marker
// line without the expected id, or a repeated id, ends the reader: the test
// must never silently accept a duplicate or an unrelated row.
func recordLatencyArrival(arrivals map[int]latencyArrival, entry latencyArrival) error {
	id, ok := parseLatencyLine(entry.line)
	if !ok {
		return fmt.Errorf("unexpected tail line %q", entry.line)
	}
	if _, exists := arrivals[id]; exists {
		return fmt.Errorf("duplicate tail arrival for id %d", id)
	}
	arrivals[id] = entry
	return nil
}

// TestDecodeLatencyFrame pins the wire validation the reader relies on.
func TestDecodeLatencyFrame(t *testing.T) {
	valid := `{"streams":[{"stream":{"job":"api"},"values":[["1700000000000000000","latency-id=0"]]}]}`
	entries, err := decodeLatencyFrame(websocket.MessageText, []byte(valid))
	if err != nil {
		t.Fatalf("decodeLatencyFrame: %v", err)
	}
	if len(entries) != 1 || entries[0].line != "latency-id=0" || entries[0].labels["job"] != "api" {
		t.Fatalf("entries = %+v", entries)
	}
	tests := []struct {
		name    string
		typ     websocket.MessageType
		body    string
		wantErr string
	}{
		{"non-text frame", websocket.MessageBinary, valid, "want text"},
		{"malformed json", websocket.MessageText, `{`, "decode tail frame"},
		{"missing stream labels", websocket.MessageText, `{"streams":[{"values":[]}]}`, "missing labels"},
		{"tuple too long", websocket.MessageText, `{"streams":[{"stream":{},"values":[["1","a","b"]]}]}`, "want [timestamp, line]"},
		{"non-string timestamp", websocket.MessageText, `{"streams":[{"stream":{},"values":[[1,"a"]]}]}`, "must be strings"},
		{"dropped entries", websocket.MessageText, `{"streams":[],"dropped_entries":[{}]}`, "dropped entries"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeLatencyFrame(tc.typ, []byte(tc.body)); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestRecordLatencyArrival pins duplicate and unexpected-marker rejection.
func TestRecordLatencyArrival(t *testing.T) {
	arrivals := make(map[int]latencyArrival)
	if err := recordLatencyArrival(arrivals, latencyArrival{line: "latency-id=0"}); err != nil {
		t.Fatalf("recordLatencyArrival: %v", err)
	}
	if err := recordLatencyArrival(arrivals, latencyArrival{line: "latency-id=0"}); err == nil ||
		!strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate error = %v", err)
	}
	for _, line := range []string{"other-line", "latency-id=", "latency-id=99", "latency-id=-1"} {
		if err := recordLatencyArrival(arrivals, latencyArrival{line: line}); err == nil {
			t.Errorf("recordLatencyArrival accepted %q", line)
		}
	}
	if len(arrivals) != 1 {
		t.Fatalf("arrivals = %d, want only the first id", len(arrivals))
	}
}

// assertNoTailLoss requires the ingester and querier drop counters to stay zero.
func assertNoTailLoss(t *testing.T, stack *tailStack) {
	t.Helper()
	families, err := stack.reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "parkana_ingest_tail_dropped_entries_total", "parkana_ingest_tail_evictions_total", "parkana_tail_dropped_entries_total":
			for _, metric := range family.GetMetric() {
				if value := metric.GetCounter().GetValue(); value != 0 {
					t.Errorf("%s = %v, want no tail loss", family.GetName(), value)
				}
			}
		}
	}
}

// p50 returns the lower median of the sorted durations.
func p50(values []time.Duration) time.Duration {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[(len(sorted)-1)/2]
}

// describeDurations renders the minimum and maximum of the signed deltas.
func describeDurations(values []time.Duration) string {
	if len(values) == 0 {
		return "none"
	}
	return fmt.Sprintf("min=%s max=%s", slices.Min(values), slices.Max(values))
}
