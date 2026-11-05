package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// streamBodies records the exact emitted tuples per label set, in arrival
// order, so a second run can prove payload determinism including batching. It
// stores the decoded (timestamp, line) entries rather than raw JSON bytes: the
// push codec serializes label maps in unspecified order, which is not part of
// the emitted tuple determinism this benchmark promises.
type streamBodies struct {
	mu      sync.Mutex
	byLabel map[string][][]logs.Entry
}

func newStreamBodies() *streamBodies {
	return &streamBodies{byLabel: make(map[string][][]logs.Entry)}
}

func (s *streamBodies) add(labels logs.Labels, entries []logs.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := labels.String()
	s.byLabel[key] = append(s.byLabel[key], slices.Clone(entries))
}

func (s *streamBodies) snapshot() map[string][][]logs.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][][]logs.Entry, len(s.byLabel))
	for key, batches := range s.byLabel {
		copied := make([][]logs.Entry, len(batches))
		for i, entries := range batches {
			copied[i] = slices.Clone(entries)
		}
		out[key] = copied
	}
	return out
}

// testIngestConfig returns a small, fast, fully deterministic ingest run.
func testIngestConfig(url string) IngestConfig {
	return IngestConfig{
		URL:          url,
		Tenant:       "bench",
		Seed:         42,
		Start:        testStart(),
		Step:         100 * time.Microsecond,
		TargetBytes:  200_000,
		BatchLines:   137,
		Concurrency:  4,
		Timeout:      2 * time.Second,
		Retries:      2,
		RetryBackoff: time.Millisecond,
	}
}

// orderChecker asserts that entries of one label set arrive in strictly
// increasing timestamp order, which is Loki's out-of-order ingest rule.
type orderChecker struct {
	mu         sync.Mutex
	last       map[string]int64
	violations []string
}

func newOrderChecker() *orderChecker {
	return &orderChecker{last: make(map[string]int64)}
}

func (o *orderChecker) check(labels logs.Labels, entries []logs.Entry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := labels.String()
	for _, entry := range entries {
		ts := entry.Timestamp.UnixNano()
		if prev, ok := o.last[key]; ok && ts <= prev {
			o.violations = append(o.violations, key)
			return
		}
		o.last[key] = ts
	}
}

// snapshot returns a copy of the violations recorded so far under the checker's
// mutex, so tests can assert after unlocking.
func (o *orderChecker) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.violations)
}

// concurrencyGate holds the first want concurrent requests until all of them
// have arrived, then permanently releases every later request. The wait is
// bounded by the request context, so a client that cannot reach want concurrent
// requests fails instead of hanging the test.
type concurrencyGate struct {
	want    int
	mu      sync.Mutex
	arrived int
	open    chan struct{}
	release func()
}

func newConcurrencyGate(want int) *concurrencyGate {
	g := &concurrencyGate{want: want, open: make(chan struct{})}
	g.release = sync.OnceFunc(func() { close(g.open) })
	return g
}

func (g *concurrencyGate) wait(ctx context.Context) {
	g.mu.Lock()
	g.arrived++
	if g.arrived == g.want {
		g.release()
	}
	g.mu.Unlock()
	select {
	case <-g.open:
	case <-ctx.Done():
	}
}

// TestRunIngestDefaultSixteenSenders proves the default configuration can
// actually issue 16 simultaneous pushes: with the default batch size and no
// instance flag, the server must admit 16 concurrent requests before any of
// them may finish. It also proves no full label set ever has two simultaneous
// requests. Existing default Concurrency is 8 and the corpus exposes four label
// sets, so this fails before the parallelism change.
func TestRunIngestDefaultSixteenSenders(t *testing.T) {
	gate := newConcurrencyGate(16)
	var (
		mu              sync.Mutex
		labels          = make(map[string]bool)
		services        = make(map[string]bool)
		labelIn         = make(map[string]int)
		labelMax        = make(map[string]int)
		requestsByLabel = make(map[string]int)
		decodedLines    int64
		decodedBytes    int64
		order           = newOrderChecker()
		inflight        atomic.Int64
		peak            atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			seen := peak.Load()
			if cur <= seen || peak.CompareAndSwap(seen, cur) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		keys := make([]string, 0, len(req.Streams))
		mu.Lock()
		for _, stream := range req.Streams {
			key := stream.Labels.String()
			labels[key] = true
			keys = append(keys, key)
			labelIn[key]++
			labelMax[key] = max(labelMax[key], labelIn[key])
			requestsByLabel[key]++
			for _, entry := range stream.Entries {
				decodedLines++
				decodedBytes += int64(len(entry.Line))
			}
			if service, ok := stream.Labels.Get(defaultStreamLabel); ok {
				services[service] = true
			}
		}
		mu.Unlock()
		for _, stream := range req.Streams {
			order.check(stream.Labels, stream.Entries)
		}
		defer func() {
			mu.Lock()
			for _, key := range keys {
				labelIn[key]--
			}
			mu.Unlock()
		}()
		gate.wait(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.TargetBytes = 8_000_000
	cfg.Concurrency = 16
	cfg.BatchLines = 1250
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	report, err := RunIngest(ctx, cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	if got := peak.Load(); got != 16 {
		t.Fatalf("server admitted peak %d concurrent requests, want 16", got)
	}
	if par := report.Parallelism; par != (ParallelismStats{
		LabelSets: 16, ActiveSenders: 16, ConcurrencyLimit: 16, PeakInFlightPushes: 16,
	}) {
		t.Fatalf("parallelism = %+v, want all 16", par)
	}
	mu.Lock()
	gotLabels, gotServices := len(labels), len(services)
	gotLabelMax := maps.Clone(labelMax)
	gotRequests := maps.Clone(requestsByLabel)
	gotDecodedLines, gotDecodedBytes := decodedLines, decodedBytes
	mu.Unlock()
	violations := order.snapshot()

	for key, got := range gotLabelMax {
		if got != 1 {
			t.Errorf("label set %s had %d simultaneous requests, want 1", key, got)
		}
	}
	if len(gotRequests) != 16 {
		t.Fatalf("server served %d distinct label sets, want 16", len(gotRequests))
	}
	for key, requests := range gotRequests {
		if requests < 2 {
			t.Errorf("label set %s made %d requests, want at least 2 batches", key, requests)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("out-of-order entries across pushes for %d label sets, first %s",
			len(violations), violations[0])
	}
	if gotLabels != 16 {
		t.Fatalf("server saw %d distinct label sets, want 16", gotLabels)
	}
	if gotServices != 4 {
		t.Fatalf("server saw %d distinct service names, want 4", gotServices)
	}
	if gotDecodedLines != report.AckLines || gotDecodedBytes != report.AckRawLineBytes ||
		gotDecodedLines != report.Corpus.Lines || gotDecodedBytes != report.Corpus.RawLineBytes {
		t.Fatalf("acknowledged totals: server (%d lines, %d bytes), report (%d, %d), corpus (%d, %d)",
			gotDecodedLines, gotDecodedBytes, report.AckLines, report.AckRawLineBytes,
			report.Corpus.Lines, report.Corpus.RawLineBytes)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded || report.Delivery.Failed != 0 {
		t.Fatalf("request accounting: %+v", report.Delivery)
	}
}

// TestRunIngestParallelismCeiling repeats the barrier proof at concurrency 2:
// two requests may overlap, a third never does, and the reported peak equals the
// server-admitted peak on every attempt.
func TestRunIngestParallelismCeiling(t *testing.T) {
	gate := newConcurrencyGate(2)
	var (
		mu       sync.Mutex
		labels   = make(map[string]bool)
		labelIn  = make(map[string]int)
		labelMax = make(map[string]int)
		inflight atomic.Int64
		peak     atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			seen := peak.Load()
			if cur <= seen || peak.CompareAndSwap(seen, cur) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		keys := make([]string, 0, len(req.Streams))
		mu.Lock()
		for _, stream := range req.Streams {
			key := stream.Labels.String()
			labels[key] = true
			keys = append(keys, key)
			labelIn[key]++
			labelMax[key] = max(labelMax[key], labelIn[key])
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			for _, key := range keys {
				labelIn[key]--
			}
			mu.Unlock()
		}()
		gate.wait(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.Concurrency = 2
	cfg.BatchLines = 5000
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	report, err := RunIngest(ctx, cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("server admitted peak %d concurrent requests, want 2", got)
	}
	if par := report.Parallelism; par != (ParallelismStats{
		LabelSets: 16, ActiveSenders: 16, ConcurrencyLimit: 2, PeakInFlightPushes: 2,
	}) {
		t.Fatalf("parallelism = %+v, want 16 senders capped at 2", par)
	}
	mu.Lock()
	gotLabels := len(labels)
	gotLabelMax := maps.Clone(labelMax)
	mu.Unlock()
	for key, got := range gotLabelMax {
		if got != 1 {
			t.Errorf("label set %s had %d simultaneous requests, want 1", key, got)
		}
	}
	if gotLabels != 16 {
		t.Fatalf("server saw %d distinct label sets, want 16", gotLabels)
	}
}

// TestRunIngestTinyCorpusActivatesOneSender proves active_senders is measured:
// a corpus that fits one slot reports 16 configured label sets but only one
// sender that entered client.Do.
func TestRunIngestTinyCorpusActivatesOneSender(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.TargetBytes = tailMinBytes
	cfg.Concurrency = 16
	cfg.BatchLines = 5000
	report, err := RunIngest(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	par := report.Parallelism
	if par.LabelSets != 16 {
		t.Fatalf("label sets = %d, want 16", par.LabelSets)
	}
	if par.ActiveSenders != 1 || par.PeakInFlightPushes != 1 {
		t.Fatalf("tiny corpus parallelism = %+v, want one active sender at peak 1", par)
	}
	if b := report.Batching; b.PushRequests != 1 || b.AttemptedEntries != 1 ||
		b.MinEntriesPerRequest != 1 || b.MaxEntriesPerRequest != 1 || b.MeanEntriesPerRequest != 1 {
		t.Fatalf("tiny corpus batching = %+v, want one single-entry final request", b)
	}
}

// TestRunIngestEmittedTuplesIndependentOfConcurrency compares the emitted
// (label set, timestamp, line) tuples of two runs with the same corpus but a
// different push concurrency. Batching must not depend on network timing.
func TestRunIngestEmittedTuplesIndependentOfConcurrency(t *testing.T) {
	run := func(concurrency int) map[string][][]logs.Entry {
		bodies := newStreamBodies()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read push body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			req, err := pushcodec.DecodeJSON(r.Context(), body)
			if err != nil {
				t.Errorf("decode push body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, stream := range req.Streams {
				bodies.add(stream.Labels, stream.Entries)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()
		cfg := testIngestConfig(srv.URL)
		cfg.Concurrency = concurrency
		cfg.BatchLines = 17
		if _, err := RunIngest(t.Context(), cfg); err != nil {
			t.Fatalf("RunIngest at concurrency %d: %v", concurrency, err)
		}
		return bodies.snapshot()
	}
	two := run(2)
	sixteen := run(16)
	if len(two) != 16 || len(sixteen) != 16 {
		t.Fatalf("label sets: concurrency 2 = %d, 16 = %d, want 16", len(two), len(sixteen))
	}
	if !reflect.DeepEqual(two, sixteen) {
		t.Fatal("emitted tuples depend on push concurrency")
	}
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// errorTransport fails every request with a fixed error and counts calls.
type errorTransport struct {
	err   error
	calls atomic.Int64
}

func (t *errorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	_ = r.Body.Close()
	return nil, t.err
}

// gatedResponseBody blocks Read until readGate and Close until closeGate,
// signaling each stage, so a test can observe the meter while the response body
// is still being drained and closed. The stage signals are constructor-bound
// sync.OnceFunc values, so a test can release the body from t.Cleanup before it
// launches the attempt and still close the gates exactly once on every path.
type gatedResponseBody struct {
	readStarted  chan struct{}
	readGate     <-chan struct{}
	closeStarted chan struct{}
	closeGate    <-chan struct{}
	readSignal   func()
	closeSignal  func()
}

func newGatedResponseBody(readGate, closeGate <-chan struct{}) *gatedResponseBody {
	b := &gatedResponseBody{
		readStarted:  make(chan struct{}),
		readGate:     readGate,
		closeStarted: make(chan struct{}),
		closeGate:    closeGate,
	}
	b.readSignal = sync.OnceFunc(func() { close(b.readStarted) })
	b.closeSignal = sync.OnceFunc(func() { close(b.closeStarted) })
	return b
}

func (b *gatedResponseBody) Read([]byte) (int, error) {
	b.readSignal()
	<-b.readGate
	return 0, io.EOF
}

func (b *gatedResponseBody) Close() error {
	b.closeSignal()
	<-b.closeGate
	return nil
}

// gatedThenOKTransport returns body on the first request and an immediately
// drained 204 on every later request.
type gatedThenOKTransport struct {
	calls atomic.Int64
	body  *gatedResponseBody
}

func (t *gatedThenOKTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_ = r.Body.Close()
	var body io.ReadCloser = io.NopCloser(strings.NewReader(""))
	if t.calls.Add(1) == 1 {
		body = t.body
	}
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Status:     "204 No Content",
		Header:     make(http.Header),
		Body:       body,
		Request:    r,
	}, nil
}

// waitSignal waits for a test stage or fails when ctx bounds the wait.
func waitSignal(t *testing.T, ctx context.Context, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, ctx.Err())
	}
}

// TestSenderAttemptCanceledWhileSemaphoreBusyStaysInactive proves waiting on
// the concurrency semaphore is not a push: no Do call, no active sender, no
// in-flight request, no peak.
func TestSenderAttemptCanceledWhileSemaphoreBusyStaysInactive(t *testing.T) {
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		_ = r.Body.Close()
		return nil, errors.New("attempt reached the transport")
	})}
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // occupy the only slot
	meter := &parallelismMeter{}
	s := &sender{client: client, url: "http://example.invalid/push", meter: meter, sem: sem}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	status, err := s.attempt(ctx, []byte("{}"), 1)
	if status != 0 {
		t.Fatalf("status = %d, want 0", status)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("transport was called %d times from a canceled semaphore wait", got)
	}
	if meter.active.Load() != 0 || meter.peak.Load() != 0 || meter.inFlight.Load() != 0 {
		t.Fatalf("canceled semaphore waiter was counted: active=%d peak=%d inFlight=%d",
			meter.active.Load(), meter.peak.Load(), meter.inFlight.Load())
	}
	if s.stats.pushRequests != 0 || s.stats.pushEntries != 0 {
		t.Fatalf("canceled semaphore waiter contributed a size sample: requests=%d entries=%d",
			s.stats.pushRequests, s.stats.pushEntries)
	}
}

// TestSenderAttemptDoErrorPreservesCauseAndDrainsMeter proves a transport
// error keeps its original cause and leaves the in-flight gauge at zero.
func TestSenderAttemptDoErrorPreservesCauseAndDrainsMeter(t *testing.T) {
	sentinel := errors.New("transport boom")
	transport := &errorTransport{err: sentinel}
	meter := &parallelismMeter{}
	s := &sender{
		client: &http.Client{Transport: transport},
		url:    "http://example.invalid/push",
		meter:  meter,
		sem:    make(chan struct{}, 1),
	}
	status, err := s.attempt(t.Context(), []byte("{}"), 1)
	if status != 0 {
		t.Fatalf("status = %d, want 0", status)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want the transport cause", err)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("transport calls = %d, want 1", got)
	}
	if meter.active.Load() != 1 || meter.peak.Load() != 1 || meter.inFlight.Load() != 0 {
		t.Fatalf("meter after Do error: active=%d peak=%d inFlight=%d, want 1/1/0",
			meter.active.Load(), meter.peak.Load(), meter.inFlight.Load())
	}
	if s.stats.pushRequests != 1 || s.stats.pushEntries != 1 || s.stats.minPush != 1 || s.stats.maxPush != 1 {
		t.Fatalf("Do error size sample = requests=%d entries=%d min=%d max=%d, want 1/1/1/1",
			s.stats.pushRequests, s.stats.pushEntries, s.stats.minPush, s.stats.maxPush)
	}
}

// TestSenderAttemptCountsActiveOnceAndDrainsThroughBodyClose proves the meter
// stays at one until the response body is drained AND closed, then returns to
// zero, and that a retry attempt from the same sender keeps active_senders at
// one.
func TestSenderAttemptCountsActiveOnceAndDrainsThroughBodyClose(t *testing.T) {
	readGate := make(chan struct{})
	closeGate := make(chan struct{})
	readRelease := sync.OnceFunc(func() { close(readGate) })
	closeRelease := sync.OnceFunc(func() { close(closeGate) })
	// Register the releases before the attempt starts, so an assertion or
	// timeout failure cannot leave the attempt goroutine blocked forever.
	t.Cleanup(readRelease)
	t.Cleanup(closeRelease)
	body := newGatedResponseBody(readGate, closeGate)
	transport := &gatedThenOKTransport{body: body}
	meter := &parallelismMeter{}
	s := &sender{
		client: &http.Client{Transport: transport},
		url:    "http://example.invalid/push",
		meter:  meter,
		sem:    make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	type attemptResult struct {
		status int
		err    error
	}
	done := make(chan attemptResult, 1)
	go func() {
		status, err := s.attempt(ctx, []byte("{}"), 1)
		done <- attemptResult{status: status, err: err}
	}()

	waitSignal(t, ctx, body.readStarted, "response body read")
	if got := meter.inFlight.Load(); got != 1 {
		t.Fatalf("in-flight while draining body = %d, want 1", got)
	}
	if meter.active.Load() != 1 || meter.peak.Load() != 1 {
		t.Fatalf("meter while draining body: active=%d peak=%d, want 1/1",
			meter.active.Load(), meter.peak.Load())
	}
	readRelease()
	waitSignal(t, ctx, body.closeStarted, "response body close")
	// The leave is deferred until after Close, so Close still observes one
	// in-flight request. A leave right after Do would read zero here.
	if got := meter.inFlight.Load(); got != 1 {
		t.Fatalf("in-flight while closing body = %d, want 1 (leave must wait for Close)", got)
	}
	closeRelease()
	select {
	case result := <-done:
		if result.err != nil || result.status != http.StatusNoContent {
			t.Fatalf("first attempt = (%d, %v), want (204, nil)", result.status, result.err)
		}
	case <-ctx.Done():
		t.Fatalf("first attempt did not finish: %v", ctx.Err())
	}
	if got := meter.inFlight.Load(); got != 0 {
		t.Fatalf("in-flight after drain/close = %d, want 0 (gauge leaked)", got)
	}
	if meter.active.Load() != 1 || meter.peak.Load() != 1 {
		t.Fatalf("meter after first attempt: active=%d peak=%d, want 1/1",
			meter.active.Load(), meter.peak.Load())
	}

	// A diagnostic retry goes through attempt again on the same sender. It is
	// still one active sender and must not leak the gauge.
	status, err := s.attempt(ctx, []byte("{}"), 1)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("retry attempt = (%d, %v), want (204, nil)", status, err)
	}
	if meter.active.Load() != 1 {
		t.Fatalf("active senders after retry = %d, want 1", meter.active.Load())
	}
	if meter.peak.Load() != 1 || meter.inFlight.Load() != 0 {
		t.Fatalf("meter after retry: peak=%d inFlight=%d, want 1/0",
			meter.peak.Load(), meter.inFlight.Load())
	}
}

// TestRunIngestPerLabelSetRequestTarget is the regression for per-label-set
// request sizing. With a request target of 17 entries, every full label set must
// send exactly 17 entries in every nonfinal request and 1..17 in its final
// request. The old behaviour split one global 17-record generator batch across
// all label sets, so requests carried only about one entry.
func TestRunIngestPerLabelSetRequestTarget(t *testing.T) {
	for _, instances := range []int{1, 4} {
		t.Run(fmt.Sprintf("instances=%d", instances), func(t *testing.T) {
			assertPerLabelSetRequestTarget(t, instances)
		})
	}
}

func assertPerLabelSetRequestTarget(t *testing.T, instances int) {
	t.Helper()
	const requestTarget = 17
	var (
		mu       sync.Mutex
		counts   = make(map[string][]int)
		total    int64
		totalRaw int64
		order    = newOrderChecker()
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		for _, stream := range req.Streams {
			key := stream.Labels.String()
			counts[key] = append(counts[key], len(stream.Entries))
			for _, entry := range stream.Entries {
				total++
				totalRaw += int64(len(entry.Line))
			}
			order.check(stream.Labels, stream.Entries)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.TargetBytes = 200_000
	cfg.BatchLines = requestTarget
	cfg.Concurrency = 4
	cfg.InstancesPerService = instances
	cfg.Retries = 0
	cfg.RetryBackoff = 0
	report, err := RunIngest(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	mu.Lock()
	gotCounts := make(map[string][]int, len(counts))
	for key, values := range counts {
		gotCounts[key] = slices.Clone(values)
	}
	gotTotal, gotTotalRaw := total, totalRaw
	mu.Unlock()
	wantLabels := len(defaultStreamNames()) * instances
	if len(gotCounts) != wantLabels {
		t.Fatalf("server saw %d label sets, want %d", len(gotCounts), wantLabels)
	}
	for key, values := range gotCounts {
		if len(values) < 2 {
			t.Fatalf("label set %s made %d requests, want multiple requests", key, len(values))
		}
		for i, got := range values[:len(values)-1] {
			if got != requestTarget {
				t.Fatalf("label set %s nonfinal request %d carried %d entries, want %d; counts=%v",
					key, i, got, requestTarget, values)
			}
		}
		if last := values[len(values)-1]; last < 1 || last > requestTarget {
			t.Fatalf("label set %s final request carried %d entries, want 1..%d; counts=%v",
				key, last, requestTarget, values)
		}
	}
	if violations := order.snapshot(); len(violations) > 0 {
		t.Fatalf("out-of-order entries for %d label sets, first %s", len(violations), violations[0])
	}
	if gotTotal != report.AckLines || gotTotalRaw != report.AckRawLineBytes ||
		gotTotal != report.Corpus.Lines || gotTotalRaw != report.Corpus.RawLineBytes {
		t.Fatalf("acknowledged totals: server (%d lines, %d bytes), report (%d, %d), corpus (%d, %d)",
			gotTotal, gotTotalRaw, report.AckLines, report.AckRawLineBytes,
			report.Corpus.Lines, report.Corpus.RawLineBytes)
	}
}

// TestRunIngestSmallRequestTargetAdmitsAllSenders is the round-robin guard:
// with 16 label sets and a 17-entry request target, a single 5000-record
// generator chunk holds many full jobs per slot. The batcher must give every
// slot one job per round, so all 16 senders still reach client.Do and pass the
// 16-way barrier instead of stalling behind one slot's queued jobs.
func TestRunIngestSmallRequestTargetAdmitsAllSenders(t *testing.T) {
	gate := newConcurrencyGate(16)
	var (
		mu       sync.Mutex
		labels   = make(map[string]bool)
		inflight atomic.Int64
		peak     atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			seen := peak.Load()
			if cur <= seen || peak.CompareAndSwap(seen, cur) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		for _, stream := range req.Streams {
			labels[stream.Labels.String()] = true
		}
		mu.Unlock()
		gate.wait(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.TargetBytes = 200_000
	cfg.BatchLines = 17
	cfg.Concurrency = 16
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	report, err := RunIngest(ctx, cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	if got := peak.Load(); got != 16 {
		t.Fatalf("server admitted peak %d, want all 16 senders with a 17-entry target", got)
	}
	if par := report.Parallelism; par.LabelSets != 16 || par.ActiveSenders != 16 || par.PeakInFlightPushes != 16 {
		t.Fatalf("parallelism = %+v, want all 16 active", par)
	}
	mu.Lock()
	gotLabels := len(labels)
	mu.Unlock()
	if gotLabels != 16 {
		t.Fatalf("server saw %d label sets, want 16", gotLabels)
	}
}

// TestRequestBatcherEmitsExactFullRequestsAndRemainder checks the batcher's
// grouping and ownership directly: full requests carry exactly the target, the
// final remainder is kept, and two interleaved slots never borrow from or
// overwrite each other's jobs.
func TestRequestBatcherEmitsExactFullRequestsAndRemainder(t *testing.T) {
	jobs := []chan *pushJob{make(chan *pushJob, 8), make(chan *pushJob, 8)}
	batcher := newRequestBatcher(jobs, 4)
	ctx := t.Context()
	firstBatch := &Batch{Streams: [][]logs.Entry{
		{{Timestamp: testStart(), Line: "a0"}, {Timestamp: testStart(), Line: "a1"}},
		{{Timestamp: testStart(), Line: "b0"}, {Timestamp: testStart(), Line: "b1"}},
	}}
	if !batcher.addBatch(ctx, firstBatch) {
		t.Fatal("addBatch failed")
	}
	if got := batcher.pending[0]; len(got) != 2 {
		t.Fatalf("slot 0 pending = %d, want 2 below the target", len(got))
	}

	// Four more per slot fill both pending buffers; round-robin drains one full
	// job per ready slot per round, leaving two entries in each remainder.
	secondBatch := &Batch{Streams: [][]logs.Entry{
		{{Line: "a2"}, {Line: "a3"}, {Line: "a4"}, {Line: "a5"}},
		{{Line: "b2"}, {Line: "b3"}, {Line: "b4"}, {Line: "b5"}},
	}}
	if !batcher.addBatch(ctx, secondBatch) {
		t.Fatal("second addBatch failed")
	}
	jobA := <-jobs[0]
	jobB := <-jobs[1]
	jobALines := lineTexts(jobA.entries)
	jobBLines := lineTexts(jobB.entries)
	if !slices.Equal(jobALines, []string{"a0", "a1", "a2", "a3"}) || jobA.lines != 4 {
		t.Fatalf("slot 0 job = %v lines=%d", jobALines, jobA.lines)
	}
	if !slices.Equal(jobBLines, []string{"b0", "b1", "b2", "b3"}) || jobB.lines != 4 {
		t.Fatalf("slot 1 job = %v lines=%d", jobBLines, jobB.lines)
	}
	if cap(jobA.entries) != len(jobA.entries) || cap(jobB.entries) != len(jobB.entries) {
		t.Fatalf("job capacity not clipped: a=%d/%d b=%d/%d",
			cap(jobA.entries), len(jobA.entries), cap(jobB.entries), len(jobB.entries))
	}
	if got := batcher.pending[0]; len(got) != 2 {
		t.Fatalf("slot 0 pending after full emit = %d, want 2", len(got))
	}
	if got := batcher.pending[1]; len(got) != 2 {
		t.Fatalf("slot 1 pending after full emit = %d, want 2", len(got))
	}

	// Mutating the original input entry structs after they were copied must not
	// change the retained jobs or the pending remainders: the batcher owns its
	// entries. The next batch also appends into the reused pending backing
	// array and emits the remaining full request, so this exercises append
	// reuse rather than only the first allocation.
	for slot := range secondBatch.Streams {
		for i := range secondBatch.Streams[slot] {
			secondBatch.Streams[slot][i].Line = "MUTATED"
		}
	}
	thirdBatch := &Batch{Streams: [][]logs.Entry{
		{{Line: "a6"}, {Line: "a7"}},
		{{Line: "b6"}, {Line: "b7"}},
	}}
	if !batcher.addBatch(ctx, thirdBatch) {
		t.Fatal("third addBatch failed")
	}
	jobA2 := <-jobs[0]
	jobB2 := <-jobs[1]
	if got := lineTexts(jobA2.entries); !slices.Equal(got, []string{"a4", "a5", "a6", "a7"}) || jobA2.lines != 4 {
		t.Fatalf("slot 0 second job = %v lines=%d", got, jobA2.lines)
	}
	if got := lineTexts(jobB2.entries); !slices.Equal(got, []string{"b4", "b5", "b6", "b7"}) || jobB2.lines != 4 {
		t.Fatalf("slot 1 second job = %v lines=%d", got, jobB2.lines)
	}
	if cap(jobA2.entries) != len(jobA2.entries) || cap(jobB2.entries) != len(jobB2.entries) {
		t.Fatalf("second job capacity not clipped: a=%d/%d b=%d/%d",
			cap(jobA2.entries), len(jobA2.entries), cap(jobB2.entries), len(jobB2.entries))
	}
	if got := lineTexts(jobA.entries); !slices.Equal(got, jobALines) {
		t.Fatalf("retained job A changed to %v, want %v", got, jobALines)
	}
	if got := lineTexts(jobB.entries); !slices.Equal(got, jobBLines) {
		t.Fatalf("retained job B changed to %v, want %v", got, jobBLines)
	}
	if got := batcher.pending[0]; len(got) != 0 {
		t.Fatalf("slot 0 pending after second full emit = %d, want 0", len(got))
	}
	if got := batcher.pending[1]; len(got) != 0 {
		t.Fatalf("slot 1 pending after second full emit = %d, want 0", len(got))
	}

	// Only the final remainder is allowed to be smaller than the target.
	if !batcher.addBatch(ctx, &Batch{Streams: [][]logs.Entry{{{Line: "a8"}}, {{Line: "b8"}}}}) {
		t.Fatal("final addBatch failed")
	}
	if !batcher.flush(ctx) {
		t.Fatal("flush failed")
	}
	if got := <-jobs[0]; !slices.Equal(lineTexts(got.entries), []string{"a8"}) || got.lines != 1 {
		t.Fatalf("slot 0 remainder = %v lines=%d", lineTexts(got.entries), got.lines)
	}
	if got := <-jobs[1]; !slices.Equal(lineTexts(got.entries), []string{"b8"}) || got.lines != 1 {
		t.Fatalf("slot 1 remainder = %v lines=%d", lineTexts(got.entries), got.lines)
	}
}

// TestRequestBatcherCanceledFullChannelStops proves a full capacity-one job
// channel plus a canceled context makes the batcher stop and return false
// instead of buffering an unbounded number of jobs; the unqueued data is not
// reported as acknowledged by RunIngest.
func TestRequestBatcherCanceledFullChannelStops(t *testing.T) {
	jobs := []chan *pushJob{make(chan *pushJob, 1)}
	batcher := newRequestBatcher(jobs, 2)
	if !batcher.addBatch(t.Context(), &Batch{Streams: [][]logs.Entry{{{Line: "a0"}, {Line: "a1"}}}}) {
		t.Fatal("first full job did not fit the capacity-one channel")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if batcher.addBatch(ctx, &Batch{Streams: [][]logs.Entry{{{Line: "a2"}, {Line: "a3"}}}}) {
		t.Fatal("batcher accepted a second job into a full channel after cancellation")
	}
	if got := len(jobs[0]); got != 1 {
		t.Fatalf("job channel holds %d jobs after cancel, want the original 1", got)
	}
}

// lineTexts returns the line payloads of entries for test assertions.
func lineTexts(entries []logs.Entry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.Line
	}
	return out
}

func TestRunIngestSuccess(t *testing.T) {
	var (
		mu          sync.Mutex
		requests    int
		decoded     int64
		decodedRaw  int64
		maxEntries  int
		tenantSeen  []string
		order       = newOrderChecker()
		inflight    atomic.Int64
		maxInflight atomic.Int64
		bodies      = newStreamBodies()
	)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			seen := maxInflight.Load()
			if cur <= seen || maxInflight.CompareAndSwap(seen, cur) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(req.Streams) != 1 {
			t.Errorf("request carries %d streams, want 1 (one label per request)", len(req.Streams))
		}
		for _, stream := range req.Streams {
			if len(stream.Entries) == 0 {
				t.Error("request carries an empty stream")
			}
			var raw int64
			for _, entry := range stream.Entries {
				raw += int64(len(entry.Line))
			}
			order.check(stream.Labels, stream.Entries)
			bodies.add(stream.Labels, stream.Entries)
			mu.Lock()
			requests++
			decoded += int64(len(stream.Entries))
			decodedRaw += raw
			maxEntries = max(maxEntries, len(stream.Entries))
			tenantSeen = append(tenantSeen, r.Header.Get(tenant.Header))
			mu.Unlock()
		}
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	report, err := RunIngest(t.Context(), testIngestConfig(srv.URL))
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	if report.Corpus.RawLineBytes != report.Corpus.TargetRawLineBytes {
		t.Fatalf("corpus bytes = %d, target %d", report.Corpus.RawLineBytes, report.Corpus.TargetRawLineBytes)
	}
	if report.AckRawLineBytes != report.Corpus.RawLineBytes {
		t.Fatalf("acked bytes = %d, generated %d", report.AckRawLineBytes, report.Corpus.RawLineBytes)
	}
	if report.AckLines != report.Corpus.Lines {
		t.Fatalf("acked lines = %d, generated %d", report.AckLines, report.Corpus.Lines)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded || report.Delivery.Failed != 0 {
		t.Fatalf("request accounting: %+v", report.Delivery)
	}
	if report.Delivery.Retried != 0 || report.Delivery.Ambiguous != 0 {
		t.Fatalf("clean run retried/ambiguous: %+v", report.Delivery)
	}
	if par := report.Parallelism; par.LabelSets != 16 ||
		par.ConcurrencyLimit != testIngestConfig(srv.URL).Concurrency {
		t.Fatalf("parallelism config = %+v, want 16 label sets at concurrency %d",
			par, testIngestConfig(srv.URL).Concurrency)
	}
	if par := report.Parallelism; par.ActiveSenders != par.LabelSets {
		t.Fatalf("active senders = %d of %d label sets; every slot got a batch", par.ActiveSenders, par.LabelSets)
	}
	if peak := report.Parallelism.PeakInFlightPushes; peak < 1 || peak > report.Parallelism.ConcurrencyLimit {
		t.Fatalf("peak in-flight pushes = %d, want 1..%d", peak, report.Parallelism.ConcurrencyLimit)
	}
	if report.Delivery.Batches == 0 {
		t.Fatal("no batches reported")
	}
	if report.Corpus.Tenant != "bench" {
		t.Fatalf("manifest tenant = %q", report.Corpus.Tenant)
	}
	if report.Generation.RawLineBytes != report.Corpus.RawLineBytes ||
		report.Generation.Lines != report.Corpus.Lines {
		t.Fatalf("generation accounting: %+v", report.Generation)
	}
	if report.Generation.Seconds <= 0 || report.Generation.MBps <= 0 {
		t.Fatalf("generation throughput missing: %+v", report.Generation)
	}
	if report.AckMBps <= 0 || report.AckLinesPerSec <= 0 || report.ElapsedSeconds <= 0 {
		t.Fatalf("run throughput missing: %+v", report)
	}
	wantMBps := float64(report.AckRawLineBytes) / 1e6 / report.ElapsedSeconds
	if diff := report.AckMBps - wantMBps; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("ack MB/s = %v, want %v (elapsed includes generation)", report.AckMBps, wantMBps)
	}
	if report.Generation.PercentOfElapsed < 0 || report.Generation.PercentOfElapsed > 100.000001 {
		t.Fatalf("generation share = %v%%, must be within one elapsed run", report.Generation.PercentOfElapsed)
	}

	mu.Lock()
	gotRequests, gotDecoded, gotRaw, gotMax := requests, decoded, decodedRaw, maxEntries
	gotTenants := append([]string(nil), tenantSeen...)
	mu.Unlock()
	if gotDecoded != report.AckLines || gotRaw != report.AckRawLineBytes {
		t.Fatalf("server decoded (%d lines, %d bytes), report acked (%d, %d)",
			gotDecoded, gotRaw, report.AckLines, report.AckRawLineBytes)
	}
	if gotMax > testIngestConfig(srv.URL).BatchLines {
		t.Fatalf("one request carried %d entries, batch limit %d", gotMax, 137)
	}
	if gotRequests == 0 {
		t.Fatal("server saw no requests")
	}
	for _, got := range gotTenants {
		if got != "bench" {
			t.Fatalf("X-Scope-OrgID = %q, want bench", got)
		}
	}
	if violations := order.snapshot(); len(violations) > 0 {
		t.Fatalf("out-of-order entries for %d label sets, first %s", len(violations), violations[0])
	}
	if got := maxInflight.Load(); got > 4 {
		t.Fatalf("in-flight requests = %d, concurrency limit 4", got)
	} else if got < 2 {
		t.Fatalf("in-flight requests never overlapped (max %d); concurrency was not exercised", got)
	}

	// The same configuration must produce the same corpus identity and the
	// same request bodies even though the network timing differs.
	secondBodies := newStreamBodies()
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read second push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode second push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, stream := range req.Streams {
			secondBodies.add(stream.Labels, stream.Entries)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv2.Close()
	second, err := RunIngest(t.Context(), testIngestConfig(srv2.URL))
	if err != nil {
		t.Fatalf("second RunIngest: %v", err)
	}
	if second.Corpus.ContentHash != report.Corpus.ContentHash || second.Corpus.Hash != report.Corpus.Hash {
		t.Fatalf("corpus identity changed between runs:\n%+v\n%+v", report.Corpus, second.Corpus)
	}
	if second.Corpus.Lines != report.Corpus.Lines || second.AckRawLineBytes != report.AckRawLineBytes {
		t.Fatal("corpus totals changed between runs")
	}
	if !reflect.DeepEqual(bodies.snapshot(), secondBodies.snapshot()) {
		t.Fatal("push payloads changed between runs with identical corpus settings")
	}
}

func TestRunIngestRetries429Clean(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) <= 3 {
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.Retries = 5
	report, err := RunIngest(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean {
		t.Fatalf("429 retries should end clean: %+v", report.Delivery)
	}
	if report.Delivery.Retried == 0 || report.Delivery.Failed == 0 {
		t.Fatalf("retries not reported: %+v", report.Delivery)
	}
	if report.Delivery.Ambiguous != 0 {
		t.Fatalf("429 must be a definite rejection, ambiguous = %d", report.Delivery.Ambiguous)
	}
	if report.AckRawLineBytes != report.Corpus.RawLineBytes {
		t.Fatalf("acked bytes = %d, generated %d", report.AckRawLineBytes, report.Corpus.RawLineBytes)
	}
}

// assertAmbiguousRun checks the invariants every ambiguous run must satisfy:
// the run fails with an ambiguity error, delivery is not clean, the diagnostic
// retry is counted, request accounting balances and the whole corpus still
// arrived through definite 204 deliveries.
func assertAmbiguousRun(t *testing.T, report *Report, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("RunIngest succeeded despite an ambiguous attempt")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("error does not explain ambiguity: %v", err)
	}
	if report == nil {
		t.Fatal("no report for an ambiguous run")
	}
	if report.Delivery.Clean {
		t.Fatalf("ambiguous run marked clean: %+v", report.Delivery)
	}
	if report.Delivery.Retried == 0 {
		t.Fatal("diagnostic retry was not counted")
	}
	if report.Delivery.Error == "" {
		t.Fatal("delivery error is empty")
	}
	if report.Delivery.Requests != report.Delivery.Succeeded+report.Delivery.Failed {
		t.Fatalf("request accounting broke: %+v", report.Delivery)
	}
	// The diagnostic retry still delivered every record; the run is invalid
	// only because delivery is ambiguous.
	if report.AckRawLineBytes != report.Corpus.RawLineBytes {
		t.Fatalf("acked bytes = %d, generated %d", report.AckRawLineBytes, report.Corpus.RawLineBytes)
	}
	if report.AckLines != report.Corpus.Lines {
		t.Fatalf("acked lines = %d, generated %d", report.AckLines, report.Corpus.Lines)
	}
}

// onceTimeoutTransport injects one deterministic timeout error, then delegates
// to base. The error wraps context.DeadlineExceeded, so retry classification
// sees the same timeout semantics as a real client deadline without racing a
// server sleep against a scheduler-dependent deadline.
type onceTimeoutTransport struct {
	base     http.RoundTripper
	attempts *atomic.Int64
	injected atomic.Bool
}

func (t *onceTimeoutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.attempts.Add(1)
	if t.injected.CompareAndSwap(false, true) {
		// RoundTripper must close the body even when it returns an error.
		_ = r.Body.Close()
		return nil, fmt.Errorf("parkbench test: push deadline exceeded: %w", context.DeadlineExceeded)
	}
	return t.base.RoundTrip(r)
}

func TestRunIngestAmbiguousFailures(t *testing.T) {
	t.Run("5xx", func(t *testing.T) {
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		cfg := testIngestConfig(srv.URL)
		cfg.Retries = 2
		report, err := RunIngest(t.Context(), cfg)
		assertAmbiguousRun(t, report, err)
		if report.Delivery.Ambiguous != 1 {
			t.Fatalf("ambiguous attempts = %d, want 1", report.Delivery.Ambiguous)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		// The transport fails exactly one attempt with a timeout error before
		// it reaches the server, then delegates every other attempt to the
		// real server. That keeps the single ambiguous attempt and its
		// diagnostic retry deterministic under any scheduler.
		var (
			mu       sync.Mutex
			served   int64
			ackLines int64
			ackBytes int64
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read push body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			req, err := pushcodec.DecodeJSON(r.Context(), body)
			if err != nil {
				t.Errorf("decode push body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			served++
			for _, stream := range req.Streams {
				for _, entry := range stream.Entries {
					ackLines++
					ackBytes += int64(len(entry.Line))
				}
			}
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		var attempts atomic.Int64
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			t.Fatal("http.DefaultTransport is not an *http.Transport")
		}
		transport := base.Clone()
		t.Cleanup(transport.CloseIdleConnections)
		cfg := testIngestConfig(srv.URL)
		cfg.Retries = 2
		// The injected error is the only timeout: no per-attempt deadline can
		// turn an unrelated real request ambiguous under scheduling pressure.
		cfg.Timeout = 0
		cfg.Client = &http.Client{Transport: &onceTimeoutTransport{
			base:     transport,
			attempts: &attempts,
		}}
		report, err := RunIngest(t.Context(), cfg)
		assertAmbiguousRun(t, report, err)

		delivery := report.Delivery
		if delivery.Ambiguous != 1 || delivery.Failed != 1 || delivery.Retried != 1 {
			t.Fatalf("injected timeout accounting: ambiguous=%d failed=%d retried=%d, want 1/1/1: %+v",
				delivery.Ambiguous, delivery.Failed, delivery.Retried, delivery)
		}
		if got := attempts.Load(); got != delivery.Requests {
			t.Fatalf("transport attempts = %d, reported requests = %d", got, delivery.Requests)
		}
		mu.Lock()
		gotServed, gotLines, gotBytes := served, ackLines, ackBytes
		mu.Unlock()
		if delivery.Succeeded != gotServed {
			t.Fatalf("succeeded attempts = %d, server issued %d definite 204s", delivery.Succeeded, gotServed)
		}
		if report.AckLines != gotLines || report.AckRawLineBytes != gotBytes {
			t.Fatalf("report acked (%d lines, %d bytes), server delivered (%d, %d)",
				report.AckLines, report.AckRawLineBytes, gotLines, gotBytes)
		}
	})
}

func TestRunIngest4xxFailsWithoutRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.Retries = 3
	report, err := RunIngest(t.Context(), cfg)
	if err == nil {
		t.Fatal("RunIngest accepted a 400 response")
	}
	if report == nil {
		t.Fatal("no report for a failed run")
	}
	if report.Delivery.Retried != 0 {
		t.Fatalf("4xx was retried %d times", report.Delivery.Retried)
	}
	if report.Delivery.Ambiguous != 0 {
		t.Fatalf("4xx counted as ambiguous: %+v", report.Delivery)
	}
	if report.Delivery.Failed == 0 {
		t.Fatalf("no failed attempts reported: %+v", report.Delivery)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded+report.Delivery.Failed {
		t.Fatalf("request accounting broke: %+v", report.Delivery)
	}
	if report.AckLines != 0 || report.AckRawLineBytes != 0 {
		t.Fatalf("acked data despite 400: %+v", report.Delivery)
	}
	if report.Delivery.Clean {
		t.Fatal("failed run marked clean")
	}
}

func TestRunIngestDoesNotFollowRedirects(t *testing.T) {
	var sinkHits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/sink" {
			sinkHits.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Redirect(w, r, "/sink", http.StatusFound)
	}))
	defer srv.Close()

	callerPolicyUsed := atomic.Bool{}
	caller := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		callerPolicyUsed.Store(true)
		return nil
	}}
	cfg := testIngestConfig(srv.URL + "/push")
	cfg.Client = caller
	cfg.Retries = 2
	report, err := RunIngest(t.Context(), cfg)
	if err == nil {
		t.Fatal("RunIngest followed a redirect and accepted the destination")
	}
	if report == nil {
		t.Fatal("no report for a redirected run")
	}
	if report.AckLines != 0 || report.AckRawLineBytes != 0 {
		t.Fatalf("redirected POST counted as acknowledged: %+v", report.Delivery)
	}
	if report.Delivery.Succeeded != 0 || report.Delivery.Ambiguous != 0 {
		t.Fatalf("redirect accounting: %+v", report.Delivery)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded+report.Delivery.Failed {
		t.Fatalf("request accounting broke: %+v", report.Delivery)
	}
	if got := sinkHits.Load(); got != 0 {
		t.Fatalf("redirect destination received %d requests", got)
	}
	if caller.CheckRedirect == nil {
		t.Fatal("caller-owned client was mutated")
	}
	if callerPolicyUsed.Load() {
		t.Fatal("caller redirect policy was used for a measured push")
	}
}

// TestRunIngestBatchingStatsMatchAttempts proves the batching JSON counts every
// real Do attempt including a retry: push_requests, attempted_entries and
// min/max/mean all equal what the server decoded, while acked totals stay exact.
func TestRunIngestBatchingStatsMatchAttempts(t *testing.T) {
	var (
		mu             sync.Mutex
		attemptEntries []int
		ackedLines     int64
		ackedBytes     int64
		calls          int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		calls++
		first := calls == 1
		if !first {
			for _, stream := range req.Streams {
				for _, entry := range stream.Entries {
					ackedLines++
					ackedBytes += int64(len(entry.Line))
				}
			}
		}
		for _, stream := range req.Streams {
			attemptEntries = append(attemptEntries, len(stream.Entries))
		}
		mu.Unlock()
		if first {
			http.Error(w, "busy", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.InstancesPerService = 1
	cfg.TargetBytes = 200_000
	cfg.BatchLines = 50
	cfg.Retries = 1
	cfg.RetryBackoff = 0
	report, err := RunIngest(t.Context(), cfg)
	if err != nil {
		t.Fatalf("RunIngest: %v", err)
	}
	if !report.Delivery.Clean || report.Delivery.Retried != 1 || report.Delivery.Failed != 1 {
		t.Fatalf("429 retry accounting: %+v", report.Delivery)
	}
	mu.Lock()
	gotEntries := slices.Clone(attemptEntries)
	gotAckedLines, gotAckedBytes := ackedLines, ackedBytes
	mu.Unlock()
	b := report.Batching
	if b.EntriesPerRequest != 50 {
		t.Fatalf("entries_per_request = %d, want 50", b.EntriesPerRequest)
	}
	if b.PushRequests != int64(len(gotEntries)) || b.PushRequests != report.Delivery.Requests {
		t.Fatalf("push_requests = %d, server attempts %d, delivery requests %d",
			b.PushRequests, len(gotEntries), report.Delivery.Requests)
	}
	var sum int64
	minEntries, maxEntries := gotEntries[0], gotEntries[0]
	for _, entries := range gotEntries {
		sum += int64(entries)
		minEntries = min(minEntries, entries)
		maxEntries = max(maxEntries, entries)
	}
	if b.AttemptedEntries != sum {
		t.Fatalf("attempted_entries = %d, server sum %d", b.AttemptedEntries, sum)
	}
	if b.MinEntriesPerRequest != int64(minEntries) || b.MaxEntriesPerRequest != int64(maxEntries) {
		t.Fatalf("entries per request min/max = %d/%d, server %d/%d",
			b.MinEntriesPerRequest, b.MaxEntriesPerRequest, minEntries, maxEntries)
	}
	if wantMean := float64(sum) / float64(len(gotEntries)); b.MeanEntriesPerRequest != wantMean {
		t.Fatalf("mean entries = %v, want %v", b.MeanEntriesPerRequest, wantMean)
	}
	if gotAckedLines != report.AckLines || gotAckedBytes != report.AckRawLineBytes ||
		gotAckedLines != report.Corpus.Lines || gotAckedBytes != report.Corpus.RawLineBytes {
		t.Fatalf("acknowledged totals: server (%d, %d), report (%d, %d), corpus (%d, %d)",
			gotAckedLines, gotAckedBytes, report.AckLines, report.AckRawLineBytes,
			report.Corpus.Lines, report.Corpus.RawLineBytes)
	}
}

// TestRunIngestCancellationDropsPendingBuffers proves a cancel while partial
// per-label buffers are still pending leaves the run invalid and never counts
// those entries as acknowledged.
func TestRunIngestCancellationDropsPendingBuffers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		cancel()
		<-r.Context().Done()
	}))
	defer srv.Close()

	cfg := testIngestConfig(srv.URL)
	cfg.InstancesPerService = 1
	cfg.TargetBytes = 200_000
	cfg.BatchLines = 50 // several full requests plus partial remainders
	cfg.Timeout = 0
	cfg.Retries = 0
	report, err := RunIngest(ctx, cfg)
	if err == nil {
		t.Fatal("RunIngest accepted a canceled run with pending buffers")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if report == nil {
		t.Fatal("no report for a canceled run")
	}
	if report.Delivery.Clean || !report.Delivery.Cancelled {
		t.Fatalf("canceled delivery flags: %+v", report.Delivery)
	}
	if report.AckLines != 0 || report.AckRawLineBytes != 0 {
		t.Fatalf("pending entries counted as acknowledged: %+v", report.Delivery)
	}
	if report.AckLines >= report.Corpus.Lines {
		t.Fatalf("canceled run acked %d of %d lines", report.AckLines, report.Corpus.Lines)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded+report.Delivery.Failed {
		t.Fatalf("request accounting broke on cancellation: %+v", report.Delivery)
	}
}

func TestRunIngestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		cancel()
		<-r.Context().Done()
	}))
	defer srv.Close()

	report, err := RunIngest(ctx, testIngestConfig(srv.URL))
	if err == nil {
		t.Fatal("RunIngest ignored cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if report == nil {
		t.Fatal("no report for a canceled run")
	}
	if report.Delivery.Clean || !report.Delivery.Cancelled {
		t.Fatalf("canceled delivery flags: %+v", report.Delivery)
	}
	if report.Delivery.Requests != report.Delivery.Succeeded+report.Delivery.Failed {
		t.Fatalf("request accounting broke on cancellation: %+v", report.Delivery)
	}
	par := report.Parallelism
	if par.LabelSets != 16 || par.ConcurrencyLimit != testIngestConfig(srv.URL).Concurrency {
		t.Fatalf("canceled run parallelism config = %+v", par)
	}
	if par.ActiveSenders < 1 || par.ActiveSenders > par.LabelSets {
		t.Fatalf("canceled run active senders = %d of %d", par.ActiveSenders, par.LabelSets)
	}
	if par.PeakInFlightPushes < 1 || par.PeakInFlightPushes > par.ConcurrencyLimit {
		t.Fatalf("canceled run peak = %d at concurrency %d (gauge leaked?)", par.PeakInFlightPushes, par.ConcurrencyLimit)
	}
	if report.AckRawLineBytes == report.Corpus.RawLineBytes && report.Corpus.RawLineBytes > 0 {
		// A tiny corpus can finish before the first handler cancels. That is
		// not a failure of the cancellation path, but it must still be marked
		// canceled and not clean.
		t.Logf("corpus finished before cancellation; acked %d bytes", report.AckRawLineBytes)
	}
}

// TestRunIngestInstanceValidationBeforeRequests proves corpus/label errors are
// rejected before the first HTTP request, not after some batches were pushed.
func TestRunIngestInstanceValidationBeforeRequests(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	valid := testIngestConfig(srv.URL)
	tests := []struct {
		name   string
		mutate func(*IngestConfig)
	}{
		{"negative instances", func(c *IngestConfig) { c.InstancesPerService = -1 }},
		{"instance stream label", func(c *IngestConfig) { c.StreamLabel = instanceLabel }},
		{"instance extra label", func(c *IngestConfig) {
			c.Labels = logs.Labels{{Name: instanceLabel, Value: "0"}}
		}},
		{"product above bound", func(c *IngestConfig) { c.InstancesPerService = maxLabelSets }},
		{"count above bound", func(c *IngestConfig) { c.InstancesPerService = math.MaxInt }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			cfg.Labels = slices.Clone(valid.Labels)
			tc.mutate(&cfg)
			report, err := RunIngest(t.Context(), cfg)
			if err == nil {
				t.Fatalf("RunIngest accepted invalid config %+v", cfg)
			}
			if report != nil {
				t.Fatalf("invalid config returned a report: %+v", report)
			}
			if got := hits.Load(); got != 0 {
				t.Fatalf("invalid config issued %d push requests before failing", got)
			}
		})
	}
}

func TestRunIngestValidation(t *testing.T) {
	valid := testIngestConfig("http://127.0.0.1:1/loki/api/v1/push")
	tests := []struct {
		name string
		cfg  IngestConfig
	}{
		{"missing url", func() IngestConfig { c := valid; c.URL = ""; return c }()},
		{"bad scheme", func() IngestConfig { c := valid; c.URL = "ftp://example.com/push"; return c }()},
		{"missing host", func() IngestConfig { c := valid; c.URL = "http:///push"; return c }()},
		{"bad tenant", func() IngestConfig { c := valid; c.Tenant = "bad tenant"; return c }()},
		{"zero batch", func() IngestConfig { c := valid; c.BatchLines = 0; return c }()},
		{"zero concurrency", func() IngestConfig { c := valid; c.Concurrency = 0; return c }()},
		{"negative timeout", func() IngestConfig { c := valid; c.Timeout = -time.Second; return c }()},
		{"negative retries", func() IngestConfig { c := valid; c.Retries = -1; return c }()},
		{"negative backoff", func() IngestConfig { c := valid; c.RetryBackoff = -1; return c }()},
		{"negative instances", func() IngestConfig { c := valid; c.InstancesPerService = -1; return c }()},
		{"instance stream label", func() IngestConfig { c := valid; c.StreamLabel = instanceLabel; return c }()},
		{"instance extra label", func() IngestConfig {
			c := valid
			c.Labels = logs.Labels{{Name: instanceLabel, Value: "0"}}
			return c
		}()},
		{"too many label sets", func() IngestConfig { c := valid; c.InstancesPerService = maxLabelSets; return c }()},
		{"absurd instance count", func() IngestConfig { c := valid; c.InstancesPerService = math.MaxInt; return c }()},
		{"sub-microsecond start", func() IngestConfig { c := valid; c.Start = testStart().Add(1); return c }()},
		{"zero target", func() IngestConfig { c := valid; c.TargetBytes = 0; return c }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report, err := RunIngest(t.Context(), tc.cfg)
			if err == nil {
				t.Fatalf("RunIngest accepted invalid config %+v", tc.cfg)
			}
			if report != nil {
				t.Fatalf("invalid config returned a report: %+v", report)
			}
		})
	}
}
