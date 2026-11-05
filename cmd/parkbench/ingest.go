package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// IngestConfig is one parkbench ingest run.
type IngestConfig struct {
	// URL is the Loki-compatible push endpoint.
	URL string
	// Tenant, when set, is sent as X-Scope-OrgID.
	Tenant tenant.ID
	// Seed, Start, Step and TargetBytes configure the corpus. See CorpusConfig.
	Seed        uint64
	Start       time.Time
	Step        time.Duration
	TargetBytes int64
	// Streams, StreamLabel and Labels configure the stream label sets.
	Streams     []string
	StreamLabel string
	Labels      logs.Labels
	// InstancesPerService is the number of deterministic instance label values
	// per stream and therefore the number of sequential senders per stream.
	// Zero uses defaultInstancesPerService. See CorpusConfig.
	InstancesPerService int
	// BatchLines is the maximum number of entries in one HTTP push for one
	// full label set (one sender). Every nonfinal request of a label set has
	// exactly this many entries; only that label set's final request may be
	// smaller (1..BatchLines). It does not bound a global generator batch:
	// generation uses a fixed internal chunk size so request sizing never
	// changes corpus content or grouping.
	BatchLines int
	// Concurrency bounds in-flight HTTP requests. Ordering within each label
	// set is preserved by one sender per label set regardless of this value.
	Concurrency int
	// Timeout is the per-attempt HTTP deadline; zero disables it and only the
	// caller context bounds the request.
	Timeout time.Duration
	// Retries is the number of diagnostic retries after a retryable failure.
	// A retried transport/timeout/5xx attempt stays ambiguous: the run still
	// reports failure because the server may have applied it.
	Retries int
	// RetryBackoff is the delay before the first retry; it doubles per retry.
	// Zero retries immediately.
	RetryBackoff time.Duration
	// Client sends requests; nil uses a client with no global timeout (the
	// per-attempt context carries the deadline). RunIngest uses a shallow copy
	// with redirects disabled, so a 3xx is measured as a failure instead of
	// being followed (possibly converting POST to GET) and the caller's
	// client is never mutated.
	Client *http.Client
}

// generationChunkLines bounds how many records Corpus.NextBatch renders per
// generation iteration. It is a bounded generation chunk independent of
// BatchLines: request sizing does not change corpus grouping or line content.
// The per-label-set pending buffers still scale with BatchLines; see
// requestBatcher.
const generationChunkLines = 5000

// Report is the JSON artifact of one ingest run. Field names are stable and
// snake_case so a later query runner can consume the file without importing
// this command.
type Report struct {
	Corpus CorpusInfo `json:"corpus"`
	// ElapsedSeconds measures first setup to last acknowledgment. It includes
	// generation time, so the acknowledged throughput below is honest.
	ElapsedSeconds  float64          `json:"elapsed_seconds"`
	AckRawLineBytes int64            `json:"ack_raw_line_bytes"`
	AckLines        int64            `json:"ack_lines"`
	AckMBps         float64          `json:"ack_mb_s"`
	AckLinesPerSec  float64          `json:"ack_lines_s"`
	Generation      GenerationStats  `json:"generation"`
	Delivery        DeliveryStats    `json:"delivery"`
	Parallelism     ParallelismStats `json:"parallelism"`
	Batching        BatchingStats    `json:"batching"`
}

// BatchingStats reports the measured HTTP request sizes after per-label-set
// batching. PushRequests and AttemptedEntries count only attempts that reached
// client.Do, including retries; attempts canceled while waiting for the
// concurrency semaphore are in DeliveryStats.Requests but not here, so the mean
// is the actual request size and not deflated by queued cancellations.
type BatchingStats struct {
	// EntriesPerRequest is the configured per-full-label-set request target:
	// every nonfinal push of a label set carries exactly this many entries.
	EntriesPerRequest int `json:"entries_per_request"`
	// PushRequests is the number of HTTP attempts that entered client.Do,
	// including retries.
	PushRequests int64 `json:"push_requests"`
	// AttemptedEntries is the sum of job entries over PushRequests attempts,
	// including retried attempts.
	AttemptedEntries int64 `json:"attempted_entries"`
	// MinEntriesPerRequest, MaxEntriesPerRequest and MeanEntriesPerRequest
	// describe those attempts. They are zero when no attempt entered Do.
	MinEntriesPerRequest  int64   `json:"min_entries_per_request"`
	MaxEntriesPerRequest  int64   `json:"max_entries_per_request"`
	MeanEntriesPerRequest float64 `json:"mean_entries_per_request"`
}

// ParallelismStats reports how many senders the corpus configured and how much
// push parallelism the run actually achieved. It is measured, not inferred from
// configuration: ActiveSenders counts the sender slots that entered at least
// one client.Do (a canceled sender still waiting for the concurrency semaphore
// is not active), and PeakInFlightPushes is sampled from just before a request
// is sent until after its response body is drained and closed. Requests in
// DeliveryStats still counts attempts including retries.
type ParallelismStats struct {
	// LabelSets is the number of full (stream, instance) label sets, one
	// sequential sender each.
	LabelSets int `json:"label_sets"`
	// ActiveSenders is the number of those senders that entered at least one
	// client.Do.
	ActiveSenders int `json:"active_senders"`
	// ConcurrencyLimit is the configured maximum number of in-flight HTTP
	// requests.
	ConcurrencyLimit int `json:"concurrency_limit"`
	// PeakInFlightPushes is the highest number of requests simultaneously
	// between client.Do entry and response drain/close. It can never exceed
	// min(LabelSets, ConcurrencyLimit).
	PeakInFlightPushes int `json:"peak_in_flight_pushes"`
}

// GenerationStats separates corpus rendering cost from network time. Seconds
// is the wall time the generator goroutine spent rendering records; it
// overlaps pushes and is part of ElapsedSeconds.
type GenerationStats struct {
	Seconds          float64 `json:"seconds"`
	RawLineBytes     int64   `json:"raw_line_bytes"`
	Lines            int64   `json:"lines"`
	MBps             float64 `json:"mb_s"`
	PercentOfElapsed float64 `json:"percent_of_elapsed"`
}

// DeliveryStats accounts for every push attempt. Requests counts attempts
// (including retries), Succeeded counts 204 responses, Failed counts failed
// attempts. Ambiguous counts attempts whose outcome is unknown (transport
// error, timeout or 5xx): retrying them is diagnostic only and the run is
// never clean. Retried 429 attempts are definite rejections and can still
// deliver a clean run when every record is finally acknowledged.
type DeliveryStats struct {
	// Batches counts generator iterations (Corpus.NextBatch calls), not HTTP
	// requests. Request groups are counted by Batching.PushRequests and
	// Delivery.Requests.
	Batches      int64  `json:"batches"`
	Requests     int64  `json:"requests"`
	Succeeded    int64  `json:"succeeded"`
	Failed       int64  `json:"failed"`
	Retried      int64  `json:"retried"`
	Ambiguous    int64  `json:"ambiguous"`
	Lines        int64  `json:"lines"`
	RawLineBytes int64  `json:"raw_line_bytes"`
	Clean        bool   `json:"clean"`
	Cancelled    bool   `json:"cancelled"`
	Error        string `json:"error,omitempty"`
}

// pushJob is one per-request slice of generated entries waiting for its label
// set's sender.
type pushJob struct {
	entries  []logs.Entry
	lines    int64
	rawBytes int64
}

// requestBatcher accumulates generated entries per full label set and emits one
// push job per full request target. Incoming generator chunks are appended to
// each slot's pending buffer first; full jobs are then drained round-robin, one
// job per ready slot per round, so a large chunk cannot enqueue many jobs for
// one slot before other slots get their turn (the capacity-one job channels
// would otherwise stall the others behind the generator). Pending slices are
// allocated lazily as entries arrive: no upfront labelsets*target allocation
// and no all-corpus buffering. A job takes an isolated subslice of the pending
// buffer (cap clipped) and the slot is re-sliced past it, so the sender never
// sees memory the generator may append to.
type requestBatcher struct {
	jobs       []chan *pushJob
	pending    [][]logs.Entry
	maxEntries int
}

func newRequestBatcher(jobs []chan *pushJob, maxEntries int) *requestBatcher {
	return &requestBatcher{
		jobs:       jobs,
		pending:    make([][]logs.Entry, len(jobs)),
		maxEntries: maxEntries,
	}
}

// addBatch appends one generator chunk to every slot, then drains full jobs
// round-robin. It returns false when ctx stops a bounded send.
func (b *requestBatcher) addBatch(ctx context.Context, batch *Batch) bool {
	for slot, entries := range batch.Streams {
		if len(entries) > 0 {
			b.pending[slot] = append(b.pending[slot], entries...)
		}
	}
	return b.drain(ctx)
}

// drain emits at most one full job per ready slot per round, repeating until no
// slot has a full request left.
func (b *requestBatcher) drain(ctx context.Context) bool {
	for {
		sent := false
		for slot := range b.pending {
			if len(b.pending[slot]) < b.maxEntries {
				continue
			}
			if !b.emit(ctx, slot, b.maxEntries) {
				return false
			}
			sent = true
		}
		if !sent {
			return true
		}
	}
}

// flush emits every non-empty remainder. It runs after the corpus is complete.
func (b *requestBatcher) flush(ctx context.Context) bool {
	for slot := range b.pending {
		if !b.emit(ctx, slot, len(b.pending[slot])) {
			return false
		}
	}
	return true
}

// emit hands the first n entries of one slot to its sender and keeps the rest.
// entries[:n:n] clips only the JOB's capacity, so nothing can append into the
// job's bytes through that slice. The producer's pending slice is re-sliced to
// start AFTER the job region, which is what keeps later appends away from the
// job; the sender reads only the clipped job region.
func (b *requestBatcher) emit(ctx context.Context, slot, n int) bool {
	if n == 0 {
		return true
	}
	pending := b.pending[slot]
	entries := pending[:n:n]
	var raw int64
	for _, entry := range entries {
		raw += int64(len(entry.Line))
	}
	job := &pushJob{entries: entries, lines: int64(n), rawBytes: raw}
	b.pending[slot] = pending[n:]
	select {
	case b.jobs[slot] <- job:
		return true
	case <-ctx.Done():
		return false
	}
}

// senderStats is owned by one sender goroutine and read after it stops.
type senderStats struct {
	requests     int64
	succeeded    int64
	failed       int64
	retried      int64
	ambiguous    int64
	lines        int64
	rawBytes     int64
	pushRequests int64
	pushEntries  int64
	minPush      int64
	maxPush      int64
	terminalErr  error
}

// parallelismMeter tracks the real push concurrency of one run. It is owned by
// RunIngest and shared with its senders; there is no package-level state. A
// sender enters only immediately before client.Do and leaves after the response
// body is drained and closed, on every success, error and cancellation path, so
// the gauge cannot leak and requests still queued on the semaphore are not
// counted.
type parallelismMeter struct {
	active   atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

// enter records one request entering the measured interval and raises the
// peak if it is a new maximum.
func (m *parallelismMeter) enter() {
	n := m.inFlight.Add(1)
	for {
		peak := m.peak.Load()
		if n <= peak || m.peak.CompareAndSwap(peak, n) {
			return
		}
	}
}

// leave records one request leaving the measured interval.
func (m *parallelismMeter) leave() { m.inFlight.Add(-1) }

// sender pushes the jobs of exactly one full label set, sequentially, so Loki
// never sees out-of-order entries within a label set even though different
// label sets push concurrently.
type sender struct {
	client       *http.Client
	url          string
	tenant       tenant.ID
	timeout      time.Duration
	retries      int
	retryBackoff time.Duration
	labels       logs.Labels
	meter        *parallelismMeter
	// active is owned by this sender's goroutine: it marks that the sender
	// already counted itself once it entered client.Do.
	active bool
	stats  senderStats
	sem    chan struct{}
}

// RunIngest generates the corpus described by cfg, pushes it and returns a
// report plus an error when the measured run is invalid. Delivery is invalid
// when any record was not acknowledged, or when any attempt was ambiguous
// (transport error, timeout or 5xx) even if a diagnostic retry later succeeded:
// the server may have applied the ambiguous request twice.
func RunIngest(ctx context.Context, cfg IngestConfig) (*Report, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	corpus, err := NewCorpus(CorpusConfig{
		Seed:                cfg.Seed,
		Start:               cfg.Start,
		Step:                cfg.Step,
		TargetBytes:         cfg.TargetBytes,
		Streams:             cfg.Streams,
		StreamLabel:         cfg.StreamLabel,
		Labels:              cfg.Labels,
		InstancesPerService: cfg.InstancesPerService,
	})
	if err != nil {
		return nil, err
	}

	startWall := time.Now()
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	streamLabels := corpus.StreamLabels()
	jobs := make([]chan *pushJob, len(streamLabels))
	senders := make([]*sender, len(streamLabels))
	sem := make(chan struct{}, cfg.Concurrency)
	meter := &parallelismMeter{}
	var wg sync.WaitGroup
	for i := range jobs {
		jobs[i] = make(chan *pushJob, 1)
		senders[i] = &sender{
			client:       cfg.Client,
			url:          cfg.URL,
			tenant:       cfg.Tenant,
			timeout:      cfg.Timeout,
			retries:      cfg.Retries,
			retryBackoff: cfg.RetryBackoff,
			labels:       streamLabels[i],
			meter:        meter,
			sem:          sem,
		}
		wg.Go(func() { senders[i].run(runCtx, jobs[i], cancel) })
	}

	batcher := newRequestBatcher(jobs, cfg.BatchLines)

	var (
		batches  int64
		genLines int64
		genBytes int64
		genDur   time.Duration
		genErr   error
	)
	for {
		if cause := context.Cause(runCtx); cause != nil {
			genErr = cause
			break
		}
		t := time.Now()
		batch, err := corpus.NextBatch(generationChunkLines)
		genDur += time.Since(t)
		if err != nil {
			genErr = fmt.Errorf("parkbench: generate: %w", err)
			cancel(genErr)
			break
		}
		batches++
		genLines += batch.Lines
		genBytes += batch.RawBytes
		if !dispatchBatch(runCtx, batcher, batch) {
			genErr = context.Cause(runCtx)
			break
		}
		if batch.Done {
			break
		}
	}
	// Only a complete corpus gets its final partial requests. On cancellation
	// or a generation error the pending entries stay unacknowledged, so the
	// report stays invalid instead of pretending they were delivered.
	if genErr == nil && !batcher.flush(runCtx) {
		genErr = context.Cause(runCtx)
	}
	for _, ch := range jobs {
		close(ch)
	}
	wg.Wait()
	elapsed := time.Since(startWall)

	var delivery DeliveryStats
	batching := BatchingStats{EntriesPerRequest: cfg.BatchLines}
	var minPushEntries int64
	var errs []error
	for _, s := range senders {
		delivery.Requests += s.stats.requests
		delivery.Succeeded += s.stats.succeeded
		delivery.Failed += s.stats.failed
		delivery.Retried += s.stats.retried
		delivery.Ambiguous += s.stats.ambiguous
		delivery.Lines += s.stats.lines
		delivery.RawLineBytes += s.stats.rawBytes
		// Only senders with a real Do contribute a size sample; an inactive
		// slot must not force the minimum to zero. Canceled-before-Do attempts
		// and Do errors/retries contribute exactly when they entered Do.
		if s.stats.pushRequests > 0 {
			batching.PushRequests += s.stats.pushRequests
			batching.AttemptedEntries += s.stats.pushEntries
			if minPushEntries == 0 || s.stats.minPush < minPushEntries {
				minPushEntries = s.stats.minPush
			}
			batching.MaxEntriesPerRequest = max(batching.MaxEntriesPerRequest, s.stats.maxPush)
		}
		if s.stats.terminalErr != nil {
			errs = append(errs, fmt.Errorf("parkbench: sender %s: %w", s.labels.String(), s.stats.terminalErr))
		}
	}
	if batching.PushRequests > 0 {
		batching.MinEntriesPerRequest = minPushEntries
		batching.MeanEntriesPerRequest = float64(batching.AttemptedEntries) / float64(batching.PushRequests)
	}
	if genErr != nil && !errors.Is(genErr, context.Canceled) {
		errs = append(errs, genErr)
	}
	if ctx.Err() != nil {
		errs = append(errs, fmt.Errorf("parkbench: canceled: %w", context.Cause(ctx)))
	}
	if delivery.Lines != genLines {
		errs = append(errs, fmt.Errorf("parkbench: delivery incomplete: acknowledged %d of %d lines", delivery.Lines, genLines))
	}
	if delivery.RawLineBytes != genBytes {
		errs = append(errs, fmt.Errorf("parkbench: delivery incomplete: acknowledged %d of %d raw line bytes", delivery.RawLineBytes, genBytes))
	}
	if delivery.Ambiguous > 0 {
		errs = append(errs, fmt.Errorf("parkbench: delivery ambiguous after %d transport/timeout/5xx failure(s); measured run is invalid", delivery.Ambiguous))
	}
	delivery.Batches = batches
	delivery.Clean = len(errs) == 0
	delivery.Cancelled = ctx.Err() != nil
	runErr := errors.Join(errs...)
	if runErr != nil {
		delivery.Error = runErr.Error()
	}

	corpusInfo := corpus.Manifest()
	corpusInfo.Tenant = string(cfg.Tenant)
	report := &Report{
		Corpus:          corpusInfo,
		ElapsedSeconds:  elapsed.Seconds(),
		AckRawLineBytes: delivery.RawLineBytes,
		AckLines:        delivery.Lines,
		Generation: GenerationStats{
			Seconds:      genDur.Seconds(),
			RawLineBytes: genBytes,
			Lines:        genLines,
		},
		Delivery: delivery,
		Parallelism: ParallelismStats{
			LabelSets:          len(streamLabels),
			ActiveSenders:      int(meter.active.Load()),
			ConcurrencyLimit:   cfg.Concurrency,
			PeakInFlightPushes: int(meter.peak.Load()),
		},
		Batching: batching,
	}
	if elapsed > 0 {
		report.AckMBps = float64(delivery.RawLineBytes) / 1e6 / elapsed.Seconds()
		report.AckLinesPerSec = float64(delivery.Lines) / elapsed.Seconds()
	}
	if genDur > 0 {
		report.Generation.MBps = float64(genBytes) / 1e6 / genDur.Seconds()
	}
	if elapsed > 0 {
		report.Generation.PercentOfElapsed = genDur.Seconds() / elapsed.Seconds() * 100
	}
	return report, runErr
}

// normalize fills defaults and validates the invocation.
func (c *IngestConfig) normalize() error {
	if c.URL == "" {
		return fmt.Errorf("parkbench: no -url given")
	}
	u, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("parkbench: -url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("parkbench: -url must be http or https, got %q", c.URL)
	}
	if u.Host == "" {
		return fmt.Errorf("parkbench: -url has no host: %q", c.URL)
	}
	if c.Tenant != "" {
		if err := c.Tenant.Validate(); err != nil {
			return fmt.Errorf("parkbench: -tenant: %w", err)
		}
	}
	if c.BatchLines <= 0 {
		return fmt.Errorf("parkbench: -batch-lines must be positive, got %d", c.BatchLines)
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("parkbench: -concurrency must be positive, got %d", c.Concurrency)
	}
	if c.InstancesPerService < 0 {
		return fmt.Errorf("parkbench: -instances-per-service must not be negative, got %d", c.InstancesPerService)
	}
	if c.InstancesPerService == 0 {
		c.InstancesPerService = defaultInstancesPerService
	}
	if c.Timeout < 0 {
		return fmt.Errorf("parkbench: -timeout must not be negative, got %v", c.Timeout)
	}
	if c.Retries < 0 {
		return fmt.Errorf("parkbench: -retries must not be negative, got %d", c.Retries)
	}
	if c.RetryBackoff < 0 {
		return fmt.Errorf("parkbench: -retry-backoff must not be negative, got %v", c.RetryBackoff)
	}
	if c.Client == nil {
		c.Client = &http.Client{}
	}
	// Copy the client and override redirect handling: a followed redirect can
	// turn the POST into a GET and a destination 204 would falsely count the
	// batch as acknowledged. ErrUseLastResponse returns the 3xx response
	// unchanged, which the sender reports as a definite failure.
	client := *c.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.Client = &client
	return nil
}

// dispatchBatch feeds one generator chunk to the per-label-set request
// batcher. Job channels have capacity one, so a slow sender applies
// backpressure and memory stays bounded by each label set's pending request
// target plus one queued job.
func dispatchBatch(ctx context.Context, batcher *requestBatcher, batch *Batch) bool {
	return batcher.addBatch(ctx, batch)
}

// run drains one stream's jobs in order and stops when the channel closes.
func (s *sender) run(ctx context.Context, jobs <-chan *pushJob, cancel context.CancelCauseFunc) {
	for job := range jobs {
		if context.Cause(ctx) != nil {
			continue
		}
		if err := s.push(ctx, job); err != nil {
			s.stats.terminalErr = err
			cancel(err)
			continue
		}
	}
}

// push sends one job, with bounded diagnostic retries. It returns an error for
// a definitive failure or cancellation. Ambiguous attempts are counted even
// when a later retry succeeds: the sender continues so the report covers the
// whole corpus, and RunIngest invalidates the run at the end.
func (s *sender) push(ctx context.Context, job *pushJob) error {
	body, err := pushcodec.MarshalJSON(ctx, &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  s.labels,
		Entries: job.entries,
	}}})
	if err != nil {
		return fmt.Errorf("encode %d lines: %w", job.lines, err)
	}
	for attempt := 0; ; attempt++ {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		s.stats.requests++
		status, attemptErr := s.attempt(ctx, body, int(job.lines))
		if attemptErr == nil && status == http.StatusNoContent {
			s.stats.succeeded++
			s.stats.lines += job.lines
			s.stats.rawBytes += job.rawBytes
			// An earlier ambiguous attempt is already counted in
			// s.stats.ambiguous; RunIngest invalidates the run at the end
			// while still letting the remaining data arrive so the report
			// describes the whole corpus.
			return nil
		}
		// Every attempt that is not a success is a failure, including one
		// interrupted by cancellation, so Requests == Succeeded + Failed.
		s.stats.failed++
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}

		// 429 is a definite rejection. A transport error, timeout or 5xx may
		// have been applied by the server, so it is ambiguous even when a
		// diagnostic retry later succeeds. Any other 4xx definitively failed.
		switch {
		case status == http.StatusTooManyRequests:
		case status == 0 || status >= 500 && status <= 599:
			s.stats.ambiguous++
		}
		if attempt >= s.retries || !retryable(status, attemptErr) {
			return fmt.Errorf("push %d lines: %w", job.lines, describeFailure(status, attemptErr))
		}
		s.stats.retried++
		if err := sleep(ctx, retryDelay(s.retryBackoff, attempt)); err != nil {
			return err
		}
	}
}

// attempt performs one HTTP request under the concurrency semaphore. entries is
// the number of log entries in the job body; it is counted only when the
// request really enters client.Do.
func (s *sender) attempt(ctx context.Context, body []byte, entries int) (int, error) {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	}
	attemptCtx := ctx
	if s.timeout > 0 {
		var cancel context.CancelFunc
		attemptCtx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.tenant != "" {
		req.Header.Set(tenant.Header, string(s.tenant))
	}
	// Count the sender and the request at the real Do entry. Everything before
	// this point (waiting for the semaphore, encoding) is not a push.
	if !s.active {
		s.active = true
		s.meter.active.Add(1)
	}
	s.stats.pushRequests++
	s.stats.pushEntries += int64(entries)
	if s.stats.minPush == 0 || int64(entries) < s.stats.minPush {
		s.stats.minPush = int64(entries)
	}
	s.stats.maxPush = max(s.stats.maxPush, int64(entries))
	s.meter.enter()
	defer s.meter.leave()
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(snippet))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// retryable reports whether another attempt is useful. Transport errors and
// 5xx may have been applied, so they are retried only diagnostically and stay
// ambiguous; 429 is a definite rejection. Any other HTTP status is final.
func retryable(status int, err error) bool {
	switch {
	case status == http.StatusTooManyRequests:
		return true
	case status >= 500 && status <= 599:
		return true
	case status != 0:
		return false
	default:
		return err != nil
	}
}

// describeFailure renders a push failure for the report.
func describeFailure(status int, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected status %d", status)
}

// retryDelay returns the exponential backoff before retry attempt n, capped at
// maxRetryDelay. It saturates before shifting so a large base cannot overflow.
func retryDelay(base time.Duration, attempt int) time.Duration {
	const maxRetryDelay = 5 * time.Second
	if base <= 0 {
		return 0
	}
	if base >= maxRetryDelay {
		return maxRetryDelay
	}
	shift := min(attempt, 6)
	if base > maxRetryDelay>>shift {
		return maxRetryDelay
	}
	return base << shift
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}
