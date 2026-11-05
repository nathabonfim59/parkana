// Query benchmark: the parkbench query subcommand, its JSON artifact and the
// HTTP execution of one backend measurement.
//
// # One corpus identity, two backends
//
// The command consumes the JSON report that `parkbench ingest --output` wrote
// (the corpus manifest) and refuses anything else: an unsupported corpus
// version, an incomplete or ambiguous delivery, inconsistent totals or a
// marker schedule that disagrees with the manifest counts are all rejected
// before the first backend request. The same manifest therefore drives the
// Parkana and the Loki run, and both artifacts echo its Hash and ContentHash so
// a caller can prove they measured the same corpus.
//
// # Query shape and interval semantics
//
// Every measurement is one instant metric query:
//
//	sum(count_over_time({stream_label=~"..."} |= "literal" [range]))
//
// Loki evaluates a range aggregation at the instant time t over the window
// (t-range, t] on exact nanosecond timestamps; Parkana implements the same rule
// over microsecond storage (internal/logql/range.go). Choosing
//
//	t = through - 1µs, range = through - from
//
// makes that window, over stored records, exactly the half-open manifest range
// [from, through). LogQL range literals accept only whole y/w/d/h/m/s/ms units
// (internal/logql/parser_log.go), so the range is the window length rounded up
// to a whole millisecond: the measured window starts at the manifest bound and
// may end less than 1 ms later. Expected occurrence counts are recomputed from
// the manifest schedule for the rounded window with
// CorpusInfo.ExpectedMarkerCount, so they stay exact.
//
// # Measurement discipline
//
// Queries run sequentially against an otherwise idle stack, so global metric
// deltas are attributable to the one request. Each class/window records
// explicit warmups, N timed observations, failures and timeouts. The first
// query this process issues is reported separately as a first observation:
// without controlling the backend restart and its page cache it is never
// called "cold". Parkana counters are scraped before and after every timed
// sample; a missing metric is unavailable (null), never zero, and a reset or a
// non-finite counter invalidates the measurement. Loki bytes come only from
// data.stats.summary.totalBytesProcessed and are logical bytes, not physical
// I/O.
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// queryReportVersion is the JSON artifact layout version. It is independent of
// corpusVersion: the query artifact wraps a corpus manifest, it does not change
// its layout.
const queryReportVersion = 1

// Backend names accepted by -backend.
const (
	backendParkana = "parkana"
	backendLoki    = "loki"
)

// Needle classes, named exactly like the manifest markers they query.
const (
	queryClassFrequent   = "frequent"
	queryClassRare       = "rare"
	queryClassAbsent     = "absent"
	queryClassShort4Byte = "short_4byte"
)

// Windows measured for every class.
const (
	queryWindowShort = "short"
	queryWindowLong  = "long"
)

// Body bounds. A count query returns a tiny vector; a metrics scrape is a few
// hundred kilobytes at most. Anything larger is a broken backend, not a
// measurement, and must not be buffered.
const (
	maxQueryBodyBytes   int64 = 8 << 20
	maxMetricsBodyBytes int64 = 4 << 20
	// maxManifestBytes bounds the ingest report read from disk.
	maxManifestBytes int64 = 8 << 20
)

// queryClassNames returns the needle classes in report order.
func queryClassNames() []string {
	return []string{queryClassFrequent, queryClassRare, queryClassAbsent, queryClassShort4Byte}
}

// queryWindowNames returns the window names in report order.
func queryWindowNames() []string { return []string{queryWindowShort, queryWindowLong} }

// QueryConfig is one parkbench query run. Zero values are invalid unless
// documented otherwise; RunQueries normalizes and validates it.
type QueryConfig struct {
	// Backend is backendParkana or backendLoki.
	Backend string
	// URL is the backend base URL (the instant query endpoint is derived) or a
	// full endpoint already ending in /loki/api/v1/query.
	URL string
	// MetricsURL is the Parkana metrics endpoint. It defaults to the backend
	// base URL plus /metrics for the parkana backend and must be empty for Loki.
	MetricsURL string
	// Tenant is sent as X-Scope-OrgID. Empty means "use the manifest tenant";
	// when both are set they must agree.
	Tenant tenant.ID
	// Repetitions is the number of timed observations per class and window.
	Repetitions int
	// Warmups is the number of untimed requests before the timed observations.
	Warmups int
	// Timeout bounds one HTTP request; zero disables it.
	Timeout time.Duration
	// Classes and Windows select what to measure. Empty means all.
	Classes []string
	Windows []string
	// Client sends requests; nil uses a client with no global timeout (the
	// per-request context carries the deadline). RunQueries disables redirects
	// on a shallow copy so a 3xx is reported instead of followed.
	Client *http.Client
	// Progress, when set, receives one line per finished series.
	Progress io.Writer
}

// QueryReport is the JSON artifact of one query run. Field names are stable
// snake_case so a later reporting step can read the file without importing this
// package.
type QueryReport struct {
	Version        int       `json:"version"`
	Backend        string    `json:"backend"`
	StartedAt      time.Time `json:"started_at"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	// Valid is false when any query failed, a count did not match the manifest,
	// a metric was missing where it is required, or a counter reset.
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors"`
	Notes  []string `json:"notes"`
	// Corpus echoes the whole ingest manifest corpus identity, so the artifact
	// identifies which corpus it measured.
	Corpus        CorpusInfo     `json:"corpus"`
	QueryEndpoint string         `json:"query_endpoint"`
	Tenant        string         `json:"tenant,omitempty"`
	Selector      string         `json:"selector"`
	Config        QueryRunConfig `json:"config"`
	// FirstObservation is the first query this process issued. Page-cache state
	// is not controlled, so it is a first observation only.
	FirstObservation *FirstObservation `json:"first_observation"`
	Series           []QuerySeries     `json:"series"`
}

// QueryRunConfig records the measurement settings that affect interpretation.
type QueryRunConfig struct {
	Repetitions    int      `json:"repetitions"`
	Warmups        int      `json:"warmups"`
	TimeoutSeconds float64  `json:"timeout_seconds"`
	Classes        []string `json:"classes"`
	Windows        []string `json:"windows"`
	MetricsURL     string   `json:"metrics_url,omitempty"`
}

// FirstObservation is the first query this process issued. The benchmark does
// not restart the backend or drop its caches, so page-cache state is not
// controlled and this is a first observation only.
type FirstObservation struct {
	Class             string  `json:"class"`
	Window            string  `json:"window"`
	Query             string  `json:"query"`
	ExpectedCount     int64   `json:"expected_count"`
	LatencyMS         float64 `json:"latency_ms"`
	StatusCode        int     `json:"status_code"`
	ObservedCount     *int64  `json:"observed_count"`
	Error             string  `json:"error,omitempty"`
	Timeout           bool    `json:"timeout,omitempty"`
	Canceled          bool    `json:"canceled,omitempty"`
	ControlledRestart bool    `json:"controlled_restart"`
	Note              string  `json:"note"`
}

// QuerySeries is one needle class measured over one window: its exact query,
// expected count, timed samples and per-query byte/diagnostic counters.
type QuerySeries struct {
	Class   string `json:"class"`
	Literal string `json:"literal"`
	Window  string `json:"window"`
	// From and Through are the measured half-open record interval.
	From    time.Time `json:"from"`
	Through time.Time `json:"through"`
	// EvalTime is the instant query time; it equals Through-1µs.
	EvalTime time.Time `json:"eval_time"`
	// RangeMS is the LogQL range literal length in milliseconds.
	RangeMS int64  `json:"range_ms"`
	Query   string `json:"query"`

	ExpectedCount    int64  `json:"expected_count"`
	ObservedCount    *int64 `json:"observed_count"`
	CountsConsistent bool   `json:"counts_consistent"`

	WarmupLatenciesMS []float64     `json:"warmup_latencies_ms"`
	Samples           []QuerySample `json:"samples"`

	P50MS       *float64 `json:"p50_ms"`
	P99MS       *float64 `json:"p99_ms"`
	SampleCount int      `json:"sample_count"`
	Failures    int      `json:"failures"`
	Timeouts    int      `json:"timeouts"`
	Valid       bool     `json:"valid"`
	Error       string   `json:"error,omitempty"`

	// Per-query Parkana counters. A counter is reported only when every timed
	// sample that reported it agreed; null means either that no sample reported
	// it or that the reporting samples varied (then metrics_consistent is false
	// and the per-sample values in samples are authoritative). It is never
	// treated as zero.
	FilesScanned             *int64   `json:"files_scanned"`
	FilesPrunedIndex         *int64   `json:"files_pruned_index"`
	FilesPrunedLabels        *int64   `json:"files_pruned_labels"`
	BytesScanned             *int64   `json:"bytes_scanned"`
	CacheFetchedBytes        *int64   `json:"cache_fetched_bytes"`
	CacheDownloadsSuccess    *int64   `json:"cache_downloads_success"`
	CacheDownloadsError      *int64   `json:"cache_downloads_error"`
	SidecarCacheFetchedBytes *int64   `json:"sidecar_cache_fetched_bytes"`
	MetricsConsistent        bool     `json:"metrics_consistent"`
	MetricsMissing           []string `json:"metrics_missing,omitempty"`

	// LokiTotalBytesProcessed is the backend's logical bytes for the query
	// (data.stats.summary.totalBytesProcessed). It is not a physical I/O or
	// compressed-file-bytes measurement; it is null when the backend omitted it
	// or when repetitions disagreed (then stats_consistent is false and the
	// per-sample values are authoritative).
	LokiTotalBytesProcessed *int64 `json:"stats_total_bytes_processed"`
	LokiBytesConsistent     bool   `json:"stats_consistent"`
}

// QuerySample is one timed observation.
type QuerySample struct {
	Index      int     `json:"index"`
	LatencyMS  float64 `json:"latency_ms"`
	StatusCode int     `json:"status_code"`
	// ObservedCount is null when the response never produced a count.
	ObservedCount *int64 `json:"observed_count"`
	Error         string `json:"error,omitempty"`
	Timeout       bool   `json:"timeout,omitempty"`
	Canceled      bool   `json:"canceled,omitempty"`
	// Metrics is the Parkana counter delta for exactly this request; null for
	// the Loki backend or when the scrape failed.
	Metrics                  *SampleMetrics `json:"metrics"`
	MetricsError             string         `json:"metrics_error,omitempty"`
	StatsError               string         `json:"stats_error,omitempty"`
	StatsTotalBytesProcessed *int64         `json:"stats_total_bytes_processed"`
}

// SampleMetrics is one attributable Parkana counter delta. Every field is null
// when the series was absent from a scrape: a missing counter is unavailable,
// never zero.
type SampleMetrics struct {
	FilesScanned             *int64   `json:"files_scanned"`
	FilesPrunedIndex         *int64   `json:"files_pruned_index"`
	FilesPrunedLabels        *int64   `json:"files_pruned_labels"`
	BytesScanned             *int64   `json:"bytes_scanned"`
	CacheFetchedBytes        *int64   `json:"cache_fetched_bytes"`
	CacheDownloadsSuccess    *int64   `json:"cache_downloads_success"`
	CacheDownloadsError      *int64   `json:"cache_downloads_error"`
	SidecarCacheFetchedBytes *int64   `json:"sidecar_cache_fetched_bytes"`
	MissingSeries            []string `json:"missing_series,omitempty"`
	ResetSeries              []string `json:"reset_series,omitempty"`
}

// queryDefinition is one class/window measurement before it runs: the exact
// query text, window and manifest-derived expected count. RunQueries and the
// tests both build measurements through queryDefinitions, so a test can never
// agree with a different query text than the command sends.
type queryDefinition struct {
	Class    string
	Literal  string
	Window   string
	Windows  queryWindow
	Query    string
	Expected int64
}

// queryWindow is one measured interval plus its instant evaluation time.
type queryWindow struct {
	Name     string
	From     time.Time
	Through  time.Time
	RangeMS  int64
	EvalTime time.Time
}

// rangeDuration returns the LogQL range length.
func (w queryWindow) rangeDuration() time.Duration {
	return time.Duration(w.RangeMS) * time.Millisecond
}

// queryPlan is the validated, normalized form of one invocation.
type queryPlan struct {
	backend    string
	queryURL   *url.URL
	metricsURL *url.URL
	tenant     tenant.ID
	selector   string
	windows    map[string]queryWindow
	classes    []string
	windowList []string
	timeout    time.Duration
}

// RunQueries validates cfg against manifest, runs every configured class and
// window sequentially and returns the report plus an error when the run is not
// a valid measurement. The report is always returned when it could be built, so
// a failed run still leaves evidence.
func RunQueries(ctx context.Context, cfg QueryConfig, manifest Report) (*QueryReport, error) {
	plan, err := newQueryPlan(&cfg, &manifest)
	if err != nil {
		return nil, err
	}
	defs, err := queryDefinitions(plan, manifest)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	report := &QueryReport{
		Version:       queryReportVersion,
		Backend:       plan.backend,
		StartedAt:     started.UTC(),
		Corpus:        manifest.Corpus,
		QueryEndpoint: plan.queryURL.String(),
		Tenant:        string(plan.tenant),
		Selector:      plan.selector,
		Config: QueryRunConfig{
			Repetitions:    cfg.Repetitions,
			Warmups:        cfg.Warmups,
			TimeoutSeconds: cfg.Timeout.Seconds(),
			Classes:        slices.Clone(plan.classes),
			Windows:        slices.Clone(plan.windowList),
		},
		Series: make([]QuerySeries, 0, len(defs)),
	}
	if plan.metricsURL != nil {
		report.Config.MetricsURL = plan.metricsURL.String()
	}
	report.Notes = append(report.Notes,
		"the first query is reported separately as a first observation; page-cache state was not controlled, so it is not a cache-state measurement",
		"windows start at the manifest bound and end at most 1ms later because LogQL range literals accept whole milliseconds only; expected counts are recomputed for the rounded window",
	)
	if plan.backend == backendParkana {
		report.Notes = append(report.Notes,
			"required Parkana series in both the before and after scrape: parkana_query_files_total{result=scanned}, parkana_query_files_total{result=pruned_index}, parkana_query_files_total{result=pruned_labels} and parkana_query_bytes_scanned_total; a missing one is null and invalidates the measurement",
			"optional Parkana diagnostics (null when absent): parkana_querier_cache_fetched_bytes_total, parkana_querier_cache_downloads_total{result=success|error} and parkana_querier_sidecar_cache_fetched_bytes_total",
			"bytes_scanned is catalog compressed candidate file bytes submitted to a scan attempt, including retries; it is not physical I/O",
			"cache_fetched_bytes counts serialized object bytes copied into the remote object cache (partial copies included, FS direct hits excluded); sidecar_cache_fetched_bytes counts serialized sidecar bytes read into the decoded in-memory sidecar cache, including FS reads and partial failures; neither is network or disk traffic",
		)
	} else {
		report.Notes = append(report.Notes,
			"required Loki byte stat: data.stats.summary.totalBytesProcessed; a missing value is null and invalidates the byte-bearing measurement",
			"stats_total_bytes_processed is the backend's logical bytes processed (data.stats.summary.totalBytesProcessed, which includes structured metadata); it is not comparable with compressed candidate bytes or network fetch bytes",
		)
	}

	runner := &queryRunner{cfg: cfg, plan: plan, client: cfg.Client, progress: cfg.Progress}
	var errs []error
	for _, def := range defs {
		series := QuerySeries{
			Class:             def.Class,
			Literal:           def.Literal,
			Window:            def.Window,
			From:              def.Windows.From,
			Through:           def.Windows.Through,
			EvalTime:          def.Windows.EvalTime,
			RangeMS:           def.Windows.RangeMS,
			Query:             def.Query,
			ExpectedCount:     def.Expected,
			Valid:             true,
			WarmupLatenciesMS: []float64{},
			Samples:           []QuerySample{},
		}
		runner.runSeries(ctx, &series)
		if !series.Valid {
			errs = append(errs, fmt.Errorf("parkbench query: %s/%s: %s", series.Class, series.Window, series.Error))
		}
		report.Series = append(report.Series, series)
		runner.reportProgress(&report.Series[len(report.Series)-1])
		if ctx.Err() != nil {
			errs = append(errs, fmt.Errorf("parkbench query: canceled: %w", context.Cause(ctx)))
			break
		}
	}
	report.ElapsedSeconds = time.Since(started).Seconds()
	report.FirstObservation = runner.first
	report.Valid = len(errs) == 0
	report.Errors = errorStrings(errs)
	return report, errors.Join(errs...)
}

// errorStrings renders errors for the JSON artifact.
func errorStrings(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			out = append(out, err.Error())
		}
	}
	return out
}

// runSeries executes the warmups and timed observations of one class/window and
// fills its artifacts. It never returns an error: every failure is recorded on
// the series so the report keeps the evidence.
func (r *queryRunner) runSeries(ctx context.Context, series *QuerySeries) {
	latencies := make([]float64, 0, r.cfg.Repetitions)
	// Finalize even after a failure: percentiles summarize the validated
	// samples collected before it, while the series stays invalid and visible.
	defer func() { series.finish(latencies) }()
	for i := range r.cfg.Warmups {
		if err := ctx.Err(); err != nil {
			series.fail(fmt.Errorf("canceled before warmup %d: %w", i, context.Cause(ctx)))
			return
		}
		out := r.execute(ctx, series)
		if out.err != nil {
			countWarmupFailure(series, out)
			series.fail(fmt.Errorf("warmup %d: %w", i, out.err))
			return
		}
		if out.statsErr != nil {
			series.fail(fmt.Errorf("warmup %d: %w", i, out.statsErr))
		}
		if out.observed == nil {
			series.Failures++
			series.fail(fmt.Errorf("warmup %d: response has no count", i))
			return
		}
		series.observe(*out.observed)
		if err := series.checkCount(*out.observed); err != nil {
			series.Failures++
			series.fail(fmt.Errorf("warmup %d: %w", i, err))
			return
		}
		series.WarmupLatenciesMS = append(series.WarmupLatenciesMS, milliseconds(out.latency))
	}

	for i := range r.cfg.Repetitions {
		if err := ctx.Err(); err != nil {
			series.fail(fmt.Errorf("canceled before sample %d: %w", i, context.Cause(ctx)))
			return
		}
		sample := QuerySample{Index: i}
		var before metricScrape
		scraped := false
		if r.plan.metricsURL != nil {
			var err error
			before, err = r.scrapeMetrics(ctx)
			if err != nil {
				sample.MetricsError = fmt.Sprintf("scrape before sample: %v", err)
			} else {
				scraped = true
			}
		}
		out := r.execute(ctx, series)
		sample.LatencyMS = milliseconds(out.latency)
		sample.StatusCode = out.statusCode
		sample.ObservedCount = out.observed
		sample.Timeout = out.timeout
		sample.Canceled = out.canceled
		sample.StatsTotalBytesProcessed = out.statsBytes
		switch {
		case out.statsErr != nil:
			sample.StatsError = out.statsErr.Error()
		case out.err == nil && r.plan.backend == backendLoki && out.statsBytes == nil:
			sample.StatsError = "response has no data.stats.summary.totalBytesProcessed; logical bytes are unavailable, not zero"
		}
		if scraped {
			after, err := r.scrapeMetrics(ctx)
			switch {
			case err != nil:
				sample.MetricsError = fmt.Sprintf("scrape after sample: %v", err)
			default:
				metrics, diffErr := diffScrapes(before, after)
				sample.Metrics = metrics
				if diffErr != nil {
					sample.MetricsError = diffErr.Error()
				}
			}
		}
		// A missing/invalid counter or byte stat invalidates the series even
		// when the query latency and count are fine.
		if sample.MetricsError != "" {
			series.fail(fmt.Errorf("sample %d metrics: %s", i, sample.MetricsError))
		}
		if sample.StatsError != "" {
			series.fail(fmt.Errorf("sample %d stats: %s", i, sample.StatsError))
		}
		if out.err != nil {
			sample.Error = out.err.Error()
			if out.timeout {
				series.Timeouts++
			} else {
				series.Failures++
			}
			series.Samples = append(series.Samples, sample)
			series.fail(out.err)
			return
		}
		if out.observed == nil {
			sample.Error = "response has no count"
			series.Failures++
			series.Samples = append(series.Samples, sample)
			series.fail(errors.New(sample.Error))
			return
		}
		series.observe(*out.observed)
		if err := series.checkCount(*out.observed); err != nil {
			sample.Error = err.Error()
			series.Failures++
			series.Samples = append(series.Samples, sample)
			series.fail(err)
			return
		}
		series.Samples = append(series.Samples, sample)
		latencies = append(latencies, sample.LatencyMS)
	}
}

// countWarmupFailure accounts for a failed warmup request in the series
// failure/timeout totals.
func countWarmupFailure(series *QuerySeries, out *queryOutcome) {
	if out.timeout {
		series.Timeouts++
		return
	}
	series.Failures++
}

// fail marks a series invalid and appends the reason. Every failure is kept so
// a missing counter and a query error are both visible.
func (s *QuerySeries) fail(err error) {
	s.Valid = false
	if err == nil {
		return
	}
	message := err.Error()
	if message == "" {
		return
	}
	if s.Error == "" {
		s.Error = message
		return
	}
	for part := range strings.SplitSeq(s.Error, "; ") {
		if part == message {
			return
		}
	}
	s.Error += "; " + message
}

// observe records one count observation.
func (s *QuerySeries) observe(count int64) {
	if s.ObservedCount == nil {
		s.ObservedCount = new(count)
		s.CountsConsistent = true
		return
	}
	if *s.ObservedCount != count {
		s.CountsConsistent = false
	}
}

// checkCount rejects a response that does not match the manifest. No latency is
// accepted into a series before its count matched.
func (s *QuerySeries) checkCount(count int64) error {
	if count != s.ExpectedCount {
		s.CountsConsistent = false
		return fmt.Errorf("expected %d occurrences, backend reported %d", s.ExpectedCount, count)
	}
	return nil
}

// finish computes the nearest-rank percentiles and the per-query counter
// summary. latencies holds only samples that produced a validated count.
func (s *QuerySeries) finish(latencies []float64) {
	s.SampleCount = len(latencies)
	slices.Sort(latencies)
	if p, ok := nearestRankPercentile(latencies, 50); ok {
		s.P50MS = new(p)
	}
	if p, ok := nearestRankPercentile(latencies, 99); ok {
		s.P99MS = new(p)
	}
	s.summarizeMetrics()
}

// metricField is one per-query counter: how to read it from a sample and where
// its summary lives on the series.
type metricField struct {
	pick func(*SampleMetrics) *int64
	set  func(*QuerySeries, *int64)
}

// metricFields returns the counters in report order. It builds a fresh slice so
// no package-level mutable state is shared.
func metricFields() []metricField {
	return []metricField{
		{pick: func(m *SampleMetrics) *int64 { return m.FilesScanned }, set: func(s *QuerySeries, v *int64) { s.FilesScanned = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.FilesPrunedIndex }, set: func(s *QuerySeries, v *int64) { s.FilesPrunedIndex = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.FilesPrunedLabels }, set: func(s *QuerySeries, v *int64) { s.FilesPrunedLabels = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.BytesScanned }, set: func(s *QuerySeries, v *int64) { s.BytesScanned = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.CacheFetchedBytes }, set: func(s *QuerySeries, v *int64) { s.CacheFetchedBytes = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.CacheDownloadsSuccess }, set: func(s *QuerySeries, v *int64) { s.CacheDownloadsSuccess = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.CacheDownloadsError }, set: func(s *QuerySeries, v *int64) { s.CacheDownloadsError = v }},
		{pick: func(m *SampleMetrics) *int64 { return m.SidecarCacheFetchedBytes }, set: func(s *QuerySeries, v *int64) { s.SidecarCacheFetchedBytes = v }},
	}
}

// summarizeField returns the value every reporting sample agrees on, whether
// any sample reported it, and whether those samples disagreed.
func summarizeField(samples []QuerySample, pick func(*SampleMetrics) *int64) (agreed *int64, seen, varies bool) {
	for i := range samples {
		metrics := samples[i].Metrics
		if metrics == nil {
			continue
		}
		value := pick(metrics)
		if value == nil {
			continue
		}
		if !seen {
			agreed = value
			seen = true
			continue
		}
		if *agreed != *value {
			varies = true
		}
	}
	if varies {
		agreed = nil
	}
	return agreed, seen, varies
}

// summarizeStats returns the logical byte statistic every reporting sample
// agrees on.
func summarizeStats(samples []QuerySample) (agreed *int64, seen, varies bool) {
	for i := range samples {
		value := samples[i].StatsTotalBytesProcessed
		if value == nil {
			continue
		}
		if !seen {
			agreed = value
			seen = true
			continue
		}
		if *agreed != *value {
			varies = true
		}
	}
	if varies {
		agreed = nil
	}
	return agreed, seen, varies
}

// summarizeMetrics summarizes the per-query counters and byte statistic. A
// summary field is reported only when every sample that reported it agreed;
// otherwise that field is withheld (null) and the per-sample values remain in
// the artifact. Cache warming can legitimately change transfer diagnostics
// between identical queries, so a variation is never treated as a query
// failure.
func (s *QuerySeries) summarizeMetrics() {
	reported := false
	consistent := true
	for _, field := range metricFields() {
		value, seen, varies := summarizeField(s.Samples, field.pick)
		field.set(s, value)
		reported = reported || seen
		if varies {
			consistent = false
		}
	}
	if reported {
		s.MetricsConsistent = consistent
	}
	for i := range s.Samples {
		metrics := s.Samples[i].Metrics
		if metrics == nil {
			continue
		}
		for _, name := range metrics.MissingSeries {
			if !slices.Contains(s.MetricsMissing, name) {
				s.MetricsMissing = append(s.MetricsMissing, name)
			}
		}
	}
	stats, seen, varies := summarizeStats(s.Samples)
	s.LokiTotalBytesProcessed = stats
	if seen {
		s.LokiBytesConsistent = !varies
	}
}

// nearestRankPercentile returns the p-th percentile using the nearest-rank
// method: rank = ceil(p/100 * n), index = rank-1. sorted must be ascending. An
// empty sample yields ok=false, not zero.
func nearestRankPercentile(sorted []float64, p float64) (value float64, ok bool) {
	n := len(sorted)
	if n == 0 {
		return 0, false
	}
	if p <= 0 {
		return sorted[0], true
	}
	if p >= 100 {
		return sorted[n-1], true
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	return sorted[min(max(rank-1, 0), n-1)], true
}

// milliseconds renders a duration as fractional milliseconds.
func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// queryRunner owns the per-run HTTP state.
type queryRunner struct {
	cfg      QueryConfig
	plan     *queryPlan
	client   *http.Client
	progress io.Writer
	ranAny   bool
	first    *FirstObservation
}

// reportProgress writes one line per finished series to the progress writer.
func (r *queryRunner) reportProgress(series *QuerySeries) {
	if r.progress == nil {
		return
	}
	state := "ok"
	if !series.Valid {
		state = "FAILED: " + series.Error
	}
	fmt.Fprintf(r.progress, "parkbench: %s/%s expected=%d observed=%s p50=%s p99=%s %s\n",
		series.Class, series.Window, series.ExpectedCount, formatInt64(series.ObservedCount),
		formatFloat(series.P50MS), formatFloat(series.P99MS), state)
}

// formatInt64 renders a nullable count for progress output.
func formatInt64(v *int64) string {
	if v == nil {
		return "unavailable"
	}
	return strconv.FormatInt(*v, 10)
}

// formatFloat renders a nullable latency for progress output.
func formatFloat(v *float64) string {
	if v == nil {
		return "n/a"
	}
	return strconv.FormatFloat(*v, 'f', 2, 64) + "ms"
}

// queryOutcome is one HTTP query attempt.
type queryOutcome struct {
	observed   *int64
	statsBytes *int64
	statsErr   error
	statusCode int
	latency    time.Duration
	timeout    bool
	canceled   bool
	err        error
}

// execute runs one query and records the first observation of the process. The
// first observation is the actual first HTTP request: no hidden validation
// request precedes it, so its class and window are the series it belongs to.
func (r *queryRunner) execute(ctx context.Context, series *QuerySeries) *queryOutcome {
	out := r.doQuery(ctx, series.Query, series.EvalTime)
	if !r.ranAny {
		r.ranAny = true
		r.first = &FirstObservation{
			Class:             series.Class,
			Window:            series.Window,
			Query:             series.Query,
			ExpectedCount:     series.ExpectedCount,
			LatencyMS:         milliseconds(out.latency),
			StatusCode:        out.statusCode,
			ObservedCount:     out.observed,
			Timeout:           out.timeout,
			Canceled:          out.canceled,
			ControlledRestart: false,
			Note:              "first HTTP query issued by this process; the backend was not restarted and its page cache was not dropped, so page-cache state was not controlled",
		}
		if out.err != nil {
			r.first.Error = out.err.Error()
		}
	}
	return out
}

// doQuery issues one instant query and decodes the count.
func (r *queryRunner) doQuery(ctx context.Context, query string, at time.Time) *queryOutcome {
	out := &queryOutcome{}
	start := time.Now()
	defer func() { out.latency = time.Since(start) }()

	requestCtx := ctx
	if r.plan.timeout > 0 {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithTimeout(ctx, r.plan.timeout)
		defer cancel()
	}
	endpoint := r.plan.queryURL.Clone()
	values := url.Values{}
	values.Set("query", query)
	values.Set("time", formatAPITime(at))
	endpoint.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		out.err = fmt.Errorf("build request: %w", err)
		return out
	}
	req.Header.Set("Accept", "application/json")
	if r.plan.tenant != "" {
		req.Header.Set(tenant.Header, string(r.plan.tenant))
	}
	resp, err := r.client.Do(req)
	if err != nil {
		out.err = fmt.Errorf("request: %w", err)
		out.timeout, out.canceled = classifyRequestError(ctx, requestCtx, err)
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	out.statusCode = resp.StatusCode
	body, err := readBoundedBody(resp.Body, maxQueryBodyBytes)
	if err != nil {
		// A 200 with flushed headers can stall in the body until the deadline:
		// that is a timeout, not a generic read failure.
		out.err = fmt.Errorf("read response: %w", err)
		out.timeout, out.canceled = classifyRequestError(ctx, requestCtx, err)
		return out
	}
	if resp.StatusCode != http.StatusOK {
		out.err = fmt.Errorf("backend returned %s: %s", resp.Status, bodySnippet(body))
		return out
	}
	decoded, err := decodeQueryResponse(body)
	if err != nil {
		out.err = err
		return out
	}
	out.observed = decoded.count
	out.statsBytes = decoded.statsBytes
	out.statsErr = decoded.statsErr
	return out
}

// classifyRequestError separates a per-request deadline from caller
// cancellation for any failure on the request path, including a body read that
// stalled after the response headers arrived.
func classifyRequestError(parent, request context.Context, err error) (timeout, canceled bool) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(request.Err(), context.DeadlineExceeded) {
		return true, false
	}
	if parent.Err() != nil {
		return false, true
	}
	return false, false
}

// apiQueryResponse is the part of Loki's instant query response this command
// reads. A vector result is a list of samples and a scalar result is one
// [timestamp, value] pair, so the raw result is decoded by resultType. Sample
// values stay as raw JSON literals so they can be parsed exactly.
type apiQueryResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string         `json:"resultType"`
		Result     jsontext.Value `json:"result"`
		Stats      struct {
			Summary struct {
				TotalBytesProcessed *int64 `json:"totalBytesProcessed"`
			} `json:"summary"`
		} `json:"stats"`
	} `json:"data"`
}

// decodeQueryResponse extracts the summed count and the optional logical byte
// stat. Every sample value is parsed from its raw JSON literal as an exact
// decimal rational, so no value is rounded through float64 before the
// integrality and policy-bound checks. A response without a metric resultType,
// without a result member or with a null result is an error: silently treating
// a malformed success as zero would fake a passing absent-needle measurement.
// An empty list is a valid empty vector and means zero.
func decodeQueryResponse(body []byte) (apiQueryValues, error) {
	var resp apiQueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return apiQueryValues{}, fmt.Errorf("decode response: %w", err)
	}
	if resp.Status != "success" {
		if resp.Error != "" {
			return apiQueryValues{}, fmt.Errorf("backend error %s: %s", cmp.Or(resp.ErrorType, "unknown"), resp.Error)
		}
		return apiQueryValues{}, fmt.Errorf("backend status %q", resp.Status)
	}
	raw := bytes.TrimSpace(resp.Data.Result)
	if len(raw) == 0 {
		return apiQueryValues{}, fmt.Errorf("response has no result member")
	}
	if string(raw) == "null" {
		return apiQueryValues{}, fmt.Errorf("response result is null, want a vector list or []")
	}
	var sum int64
	add := func(sample []jsontext.Value) error {
		exact, err := sampleValue(sample)
		if err != nil {
			return err
		}
		// Both values are non-negative and the running sum stays within the
		// policy bound, so the subtraction cannot overflow.
		if exact > maxExactInteger-sum {
			return fmt.Errorf("summed count exceeds the exactly representable integer range 2^53")
		}
		sum += exact
		return nil
	}
	switch resp.Data.ResultType {
	case "vector":
		var samples []struct {
			Value []jsontext.Value `json:"value"`
		}
		if err := json.Unmarshal(raw, &samples); err != nil {
			return apiQueryValues{}, fmt.Errorf("decode vector result: %w", err)
		}
		for _, sample := range samples {
			if err := add(sample.Value); err != nil {
				return apiQueryValues{}, err
			}
		}
	case "scalar":
		var pair []jsontext.Value
		if err := json.Unmarshal(raw, &pair); err != nil {
			return apiQueryValues{}, fmt.Errorf("decode scalar result: %w", err)
		}
		if err := add(pair); err != nil {
			return apiQueryValues{}, err
		}
	default:
		return apiQueryValues{}, fmt.Errorf("unexpected resultType %q for a count query", resp.Data.ResultType)
	}
	count := sum
	values := apiQueryValues{count: &count, statsBytes: resp.Data.Stats.Summary.TotalBytesProcessed}
	if values.statsBytes != nil && *values.statsBytes < 0 {
		values.statsErr = fmt.Errorf("data.stats.summary.totalBytesProcessed is negative (%d); logical bytes are unavailable", *values.statsBytes)
		values.statsBytes = nil
	}
	return values, nil
}

// apiQueryValues is the decoded content of one success response.
type apiQueryValues struct {
	count      *int64
	statsBytes *int64
	statsErr   error
}

// sampleValue parses the count of one [timestamp, value] pair.
func sampleValue(sample []jsontext.Value) (int64, error) {
	if len(sample) != 2 {
		return 0, fmt.Errorf("sample has %d values, want 2", len(sample))
	}
	return parseExactCountValue(sample[1])
}

// parseExactCountValue parses one raw JSON number or one quoted numeric string
// as an exact decimal rational. A fraction, a negative value or a value above
// the 2^53 policy bound is rejected; nothing is rounded through float64 first,
// so "9007199254740993.0", "1.00000000000000001" and a numeric 2^53+1 are all
// rejected instead of collapsing to an accepted integer.
func parseExactCountValue(raw jsontext.Value) (int64, error) {
	literal := strings.TrimSpace(string(raw))
	if literal == "" {
		return 0, errors.New("sample value is empty")
	}
	if literal[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, fmt.Errorf("sample value %q: %w", literal, err)
		}
		literal = strings.TrimSpace(text)
	}
	literal = strings.TrimPrefix(literal, "+")
	if !isDecimalNumericLiteral(literal) {
		return 0, fmt.Errorf("sample value %q is not a number", literal)
	}
	value, ok := new(big.Rat).SetString(literal)
	if !ok {
		return 0, fmt.Errorf("sample value %q is not a number", literal)
	}
	if value.Sign() < 0 {
		return 0, fmt.Errorf("sample value %s is negative", value)
	}
	if !value.IsInt() {
		return 0, fmt.Errorf("sample value %s is not an integer", value)
	}
	if value.Num().Cmp(big.NewInt(maxExactInteger)) > 0 {
		return 0, fmt.Errorf("sample value %s exceeds the exactly representable integer range 2^53", value)
	}
	return value.Num().Int64(), nil
}

// maxExactInteger is the largest integer this command accepts as a count: it is
// the largest integer a float64 represents exactly, and the documented
// conservative policy bound. Counts are parsed exactly before this check.
const maxExactInteger int64 = 1 << 53

// isDecimalNumericLiteral reports whether s contains only decimal numeric
// characters and at least one digit. big.Rat would also accept fraction and
// hexadecimal forms; a count or counter literal must be decimal.
func isDecimalNumericLiteral(s string) bool {
	digits := 0
	for i := range len(s) {
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			digits++
		case c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-':
		default:
			return false
		}
	}
	return digits > 0
}

// isDecimalInteger reports whether s is an optionally signed run of decimal
// digits with no decimal point or exponent.
func isDecimalInteger(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// readBoundedBody reads at most max bytes and fails when the body is larger.
func readBoundedBody(r io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("response body exceeds %d bytes", max)
	}
	return body, nil
}

// bodySnippet renders a short error-body preview.
func bodySnippet(body []byte) string {
	const limit = 256
	if len(body) > limit {
		body = body[:limit]
	}
	return strings.TrimSpace(string(body))
}

// apiTimeLayout is the fixed microsecond layout of an instant query time.
const apiTimeLayout = "2006-01-02T15:04:05.000000Z"

// formatAPITime renders an instant query time as fixed microsecond RFC3339 UTC.
func formatAPITime(t time.Time) string {
	return t.UTC().Format(apiTimeLayout)
}

// queryDefinitions builds the exact class/window measurements in report order.
func queryDefinitions(plan *queryPlan, manifest Report) ([]queryDefinition, error) {
	defs := make([]queryDefinition, 0, len(plan.classes)*len(plan.windowList))
	for _, class := range plan.classes {
		marker, ok := findMarker(manifest.Corpus, class)
		if !ok {
			return nil, fmt.Errorf("parkbench query: manifest has no %q marker", class)
		}
		for _, name := range plan.windowList {
			window := plan.windows[name]
			expected, err := manifest.Corpus.ExpectedMarkerCount(class, window.From, window.Through)
			if err != nil {
				return nil, fmt.Errorf("parkbench query: %s/%s: %w", class, name, err)
			}
			defs = append(defs, queryDefinition{
				Class:    class,
				Literal:  marker.Literal,
				Window:   name,
				Windows:  window,
				Query:    buildCountQuery(plan.selector, marker.Literal, window.rangeDuration()),
				Expected: expected,
			})
		}
	}
	return defs, nil
}

// buildCountQuery renders the instant count query. The literal is escaped as a
// LogQL string and compared byte-wise by |=, exactly like the pipeline does.
func buildCountQuery(selector, literal string, rng time.Duration) string {
	return "sum(count_over_time(" + selector + " |= " + strconv.Quote(literal) + " [" + formatLogQLRange(rng) + "]))"
}

// buildSelector renders the selector that covers every manifest stream. The
// stream names are regex-escaped and then LogQL-quoted.
func buildSelector(manifest CorpusInfo) (string, error) {
	if manifest.StreamLabel == "" {
		return "", errors.New("parkbench query: manifest has no stream label")
	}
	if err := (logs.Labels{{Name: manifest.StreamLabel}}).Validate(); err != nil {
		return "", fmt.Errorf("parkbench query: manifest stream label: %w", err)
	}
	if len(manifest.Streams) == 0 {
		return "", errors.New("parkbench query: manifest has no streams")
	}
	names := make([]string, 0, len(manifest.Streams))
	for _, stream := range manifest.Streams {
		if stream.Name == "" {
			return "", errors.New("parkbench query: manifest has an empty stream name")
		}
		names = append(names, regexp.QuoteMeta(stream.Name))
	}
	return "{" + manifest.StreamLabel + "=~" + strconv.Quote(strings.Join(names, "|")) + "}", nil
}

// formatLogQLRange renders a whole-millisecond duration in the units
// model.ParseDuration accepts, in descending order and without zero
// components. LogQL range literals take no fractional units, so this is the
// only exact representation of a measured window.
func formatLogQLRange(d time.Duration) string {
	ms := d.Milliseconds()
	if ms <= 0 {
		return "0ms"
	}
	var b strings.Builder
	for _, unit := range []struct {
		size int64
		name string
	}{
		{3600000, "h"},
		{60000, "m"},
		{1000, "s"},
		{1, "ms"},
	} {
		if part := ms / unit.size; part > 0 {
			b.WriteString(strconv.FormatInt(part, 10))
			b.WriteString(unit.name)
			ms -= part * unit.size
		}
	}
	return b.String()
}

// newQueryWindows derives the short and long windows from the manifest alone.
func newQueryWindows(manifest CorpusInfo) (map[string]queryWindow, error) {
	shortFrom, shortThrough, err := shortBounds(manifest)
	if err != nil {
		return nil, err
	}
	long, err := newQueryWindow(queryWindowLong, manifest.Start, manifest.Through)
	if err != nil {
		return nil, fmt.Errorf("parkbench query: long window: %w", err)
	}
	short, err := newQueryWindow(queryWindowShort, shortFrom, shortThrough)
	if err != nil {
		return nil, fmt.Errorf("parkbench query: short window: %w", err)
	}
	return map[string]queryWindow{
		queryWindowShort: short,
		queryWindowLong:  long,
	}, nil
}

// newQueryWindow builds one measured window from its half-open manifest bounds.
// The range is the window length rounded up to a whole millisecond and the
// instant query time is Through-1µs, which makes Loki's (t-range, t] window
// cover exactly the intended microsecond timestamps.
func newQueryWindow(name string, from, through time.Time) (queryWindow, error) {
	from = from.UTC()
	through = through.UTC()
	if !through.After(from) {
		return queryWindow{}, fmt.Errorf("bounds are empty: [%v, %v)", from, through)
	}
	span := through.Sub(from)
	// A span longer than an int64 duration already saturated Sub; rounding it
	// up would overflow. Reject it before any arithmetic.
	if span <= 0 || span >= math.MaxInt64-time.Millisecond {
		return queryWindow{}, fmt.Errorf("bounds [%v, %v) span too much for a range literal", from, through)
	}
	rng := roundUpMilli(span)
	end := from.Add(rng)
	if end.Before(through) || end.After(maxLokiTime()) {
		return queryWindow{}, fmt.Errorf("rounded window [%v, %v) exceeds the int64 nanosecond range", from, end)
	}
	return queryWindow{
		Name:     name,
		From:     from,
		Through:  end,
		RangeMS:  rng.Milliseconds(),
		EvalTime: end.Add(-time.Microsecond),
	}, nil
}

// roundUpMilli rounds d up to a whole millisecond.
func roundUpMilli(d time.Duration) time.Duration {
	const ms = time.Millisecond
	if d <= 0 {
		return 0
	}
	if d%ms == 0 {
		return d
	}
	return d + (ms - d%ms)
}

// shortBounds returns the common short interval: the union from the first rare
// range to the first four-byte-needle range, so every class is measured over
// the same small window. A manifest too small to contain either marker falls
// back to its first marker block, which keeps the short window deterministic and
// non-empty while expected counts are still exact.
func shortBounds(manifest CorpusInfo) (from, through time.Time, err error) {
	var starts, ends []time.Time
	for _, name := range []string{queryClassRare, queryClassShort4Byte} {
		marker, ok := findMarker(manifest, name)
		if !ok || len(marker.Ranges) == 0 {
			continue
		}
		starts = append(starts, marker.Ranges[0].From)
		ends = append(ends, marker.Ranges[0].Through)
	}
	if len(starts) == 0 {
		step := time.Duration(manifest.StepMicros) * time.Microsecond
		records := min(manifest.Lines, markerBlockSize)
		from = manifest.Start
		through = manifest.Start.Add(time.Duration(records) * step)
	} else {
		from = slices.MinFunc(starts, time.Time.Compare)
		through = slices.MaxFunc(ends, time.Time.Compare)
	}
	if from.Before(manifest.Start) {
		from = manifest.Start
	}
	if through.After(manifest.Through) {
		through = manifest.Through
	}
	if !through.After(from) {
		return time.Time{}, time.Time{}, fmt.Errorf("the manifest has no non-empty short window; measure only the long window")
	}
	return from, through, nil
}

// findMarker returns the manifest marker with the given name.
func findMarker(manifest CorpusInfo, name string) (MarkerInfo, bool) {
	for _, marker := range manifest.Markers {
		if marker.Name == name {
			return marker, true
		}
	}
	return MarkerInfo{}, false
}

// newQueryPlan validates the invocation and the manifest, normalizes defaults
// and derives the endpoints. All of this happens before any backend request.
func newQueryPlan(cfg *QueryConfig, manifest *Report) (*queryPlan, error) {
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	if err := cfg.normalize(manifest); err != nil {
		return nil, err
	}
	queryURL, err := queryEndpoint(cfg.URL)
	if err != nil {
		return nil, err
	}
	plan := &queryPlan{
		backend:    cfg.Backend,
		queryURL:   queryURL,
		tenant:     cfg.Tenant,
		timeout:    cfg.Timeout,
		classes:    slices.Clone(cfg.Classes),
		windowList: slices.Clone(cfg.Windows),
	}
	if strings.TrimSpace(cfg.MetricsURL) != "" {
		metricsURL, err := metricsEndpoint(cfg.MetricsURL)
		if err != nil {
			return nil, err
		}
		plan.metricsURL = metricsURL
	} else if cfg.Backend == backendParkana {
		plan.metricsURL = deriveMetricsURL(queryURL)
	}
	selector, err := buildSelector(manifest.Corpus)
	if err != nil {
		return nil, err
	}
	plan.selector = selector
	windows, err := newQueryWindows(manifest.Corpus)
	if err != nil {
		return nil, err
	}
	plan.windows = windows
	return plan, nil
}

// normalize fills defaults and validates the flag values.
func (c *QueryConfig) normalize(manifest *Report) error {
	switch c.Backend {
	case backendParkana, backendLoki:
	case "":
		return errors.New("parkbench query: -backend is required (parkana or loki)")
	default:
		return fmt.Errorf("parkbench query: -backend must be %q or %q, got %q", backendParkana, backendLoki, c.Backend)
	}
	if strings.TrimSpace(c.URL) == "" {
		return errors.New("parkbench query: -url is required")
	}
	if c.Repetitions < 1 {
		return fmt.Errorf("parkbench query: -repetitions must be at least 1, got %d", c.Repetitions)
	}
	if c.Warmups < 0 {
		return fmt.Errorf("parkbench query: -warmups must not be negative, got %d", c.Warmups)
	}
	if c.Timeout < 0 {
		return fmt.Errorf("parkbench query: -timeout must not be negative, got %v", c.Timeout)
	}
	if len(c.Classes) == 0 {
		c.Classes = queryClassNames()
	}
	classes, err := normalizeNames("class", c.Classes, queryClassNames())
	if err != nil {
		return err
	}
	c.Classes = classes
	if len(c.Windows) == 0 {
		c.Windows = queryWindowNames()
	}
	windows, err := normalizeNames("window", c.Windows, queryWindowNames())
	if err != nil {
		return err
	}
	c.Windows = windows
	if c.Backend == backendLoki && strings.TrimSpace(c.MetricsURL) != "" {
		return errors.New("parkbench query: -metrics-url is only valid with -backend parkana")
	}
	// Tenant identity. An ingest manifest with no tenant was pushed without a
	// header, so the backend's default tenant owns it; an explicit -tenant may
	// confirm that effective tenant but never query a different one. Resolving
	// to the default does not add a header: absence stays absence, matching the
	// ingest command.
	manifestTenant := tenant.ID(manifest.Corpus.Tenant)
	effectiveTenant := manifestTenant
	if effectiveTenant == "" {
		effectiveTenant = tenant.Default
	}
	if err := effectiveTenant.Validate(); err != nil {
		return fmt.Errorf("parkbench query: manifest effective tenant: %w", err)
	}
	if c.Tenant == "" {
		c.Tenant = manifestTenant
	} else if c.Tenant != effectiveTenant {
		return fmt.Errorf("parkbench query: -tenant %q does not match the manifest's effective tenant %q", c.Tenant, effectiveTenant)
	}
	if c.Tenant != "" {
		if err := c.Tenant.Validate(); err != nil {
			return fmt.Errorf("parkbench query: -tenant: %w", err)
		}
	}
	if c.Client == nil {
		c.Client = &http.Client{}
	}
	// A followed redirect can change the request (and hide the backend that
	// actually answered); report it as the failure it is.
	client := *c.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.Client = &client
	return nil
}

// normalizeNames trims, de-duplicates and validates a comma-separated selection
// against the allowed names, preserving the order it first appears in.
func normalizeNames(kind string, values, allowed []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, value := range values {
		for name := range strings.SplitSeq(value, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			if !slices.Contains(allowed, name) {
				return nil, fmt.Errorf("parkbench query: unknown %s %q (want %s)", kind, name, strings.Join(allowed, ", "))
			}
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("parkbench query: no %s selected", kind)
	}
	return out, nil
}

// queryEndpoint validates a backend URL and returns the instant query endpoint.
// A base URL gets /loki/api/v1/query appended; a full endpoint is kept and any
// query string is dropped.
func queryEndpoint(raw string) (*url.URL, error) {
	u, err := parseHTTPURL(raw, "-url")
	if err != nil {
		return nil, err
	}
	u = u.Clone()
	path := strings.TrimSuffix(u.Path, "/")
	if !strings.HasSuffix(path, "/loki/api/v1/query") {
		path += "/loki/api/v1/query"
	}
	u.Path = path
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return u, nil
}

// metricsEndpoint validates an explicit metrics URL.
func metricsEndpoint(raw string) (*url.URL, error) {
	u, err := parseHTTPURL(raw, "-metrics-url")
	if err != nil {
		return nil, err
	}
	u = u.Clone()
	u.Fragment = ""
	return u, nil
}

// deriveMetricsURL returns the default metrics endpoint: the backend base URL
// plus /metrics. A full query endpoint is reduced to its base first.
func deriveMetricsURL(endpoint *url.URL) *url.URL {
	u := endpoint.Clone()
	u.Path = strings.TrimSuffix(u.Path, "/loki/api/v1/query")
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	u.Path += "metrics"
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	return u
}

// parseHTTPURL parses and validates an absolute http(s) URL.
func parseHTTPURL(raw, flagName string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parkbench query: %s: %w", flagName, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("parkbench query: %s must be http or https, got %q", flagName, raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("parkbench query: %s has no host: %q", flagName, raw)
	}
	return u, nil
}

// validateManifest rejects any ingest report that is not a complete, clean,
// unambiguous corpus identity. Every check runs before the first query.
func validateManifest(report *Report) error {
	if report == nil {
		return errors.New("parkbench query: no manifest")
	}
	m := &report.Corpus
	if m.Version != corpusVersion {
		return fmt.Errorf("parkbench query: unsupported manifest version %d (want %d)", m.Version, corpusVersion)
	}
	if m.Hash == "" || m.ContentHash == "" {
		return errors.New("parkbench query: manifest is missing its corpus hash or content hash")
	}
	if !report.Delivery.Clean || report.Delivery.Error != "" || report.Delivery.Ambiguous > 0 || report.Delivery.Cancelled {
		return fmt.Errorf("parkbench query: ingest report is not a clean delivery (clean=%t ambiguous=%d cancelled=%t error=%q)",
			report.Delivery.Clean, report.Delivery.Ambiguous, report.Delivery.Cancelled, report.Delivery.Error)
	}
	if m.StepMicros <= 0 || m.StepMicros > math.MaxInt64/int64(time.Microsecond) {
		return fmt.Errorf("parkbench query: manifest step %d microseconds is out of range", m.StepMicros)
	}
	step := time.Duration(m.StepMicros) * time.Microsecond
	if m.Start.IsZero() {
		return errors.New("parkbench query: manifest has no corpus start")
	}
	if m.Lines <= 0 || m.Lines > math.MaxInt64/int64(step) {
		return fmt.Errorf("parkbench query: manifest line count %d is out of range for step %v", m.Lines, step)
	}
	wantThrough := m.Start.Add(time.Duration(m.Lines) * step)
	if !m.Through.Equal(wantThrough) {
		return fmt.Errorf("parkbench query: manifest through %v does not match start+lines*step (%v)", m.Through, wantThrough)
	}
	// Every range arithmetic below converts these bounds to int64 nanoseconds;
	// a manifest outside Loki's timestamp range must fail here, not in
	// ExpectedMarkerCount after the run started. Microsecond alignment is
	// required because the reported window and evaluation time are µs-exact.
	if m.Start.Nanosecond()%1000 != 0 {
		return fmt.Errorf("parkbench query: manifest start %v is not microsecond-aligned", m.Start)
	}
	if m.Through.Nanosecond()%1000 != 0 {
		return fmt.Errorf("parkbench query: manifest through %v is not microsecond-aligned", m.Through)
	}
	if m.Start.Before(minLokiTime()) || m.Through.After(maxLokiTime()) {
		return fmt.Errorf("parkbench query: manifest interval %v..%v is outside the int64 nanosecond range", m.Start, m.Through)
	}
	if m.RawLineBytes <= 0 {
		return fmt.Errorf("parkbench query: manifest raw line bytes %d is not positive", m.RawLineBytes)
	}
	if m.TargetRawLineBytes != m.RawLineBytes {
		return fmt.Errorf("parkbench query: manifest is incomplete: achieved %d of %d target raw line bytes", m.RawLineBytes, m.TargetRawLineBytes)
	}
	if report.AckLines != m.Lines || report.AckRawLineBytes != m.RawLineBytes {
		return fmt.Errorf("parkbench query: acknowledged totals (%d lines, %d bytes) differ from the corpus (%d lines, %d bytes)",
			report.AckLines, report.AckRawLineBytes, m.Lines, m.RawLineBytes)
	}
	if m.StreamLabel == "" {
		return errors.New("parkbench query: manifest has no stream label")
	}
	if err := (logs.Labels{{Name: m.StreamLabel}}).Validate(); err != nil {
		return fmt.Errorf("parkbench query: manifest stream label: %w", err)
	}
	if len(m.Streams) == 0 {
		return errors.New("parkbench query: manifest has no streams")
	}
	if m.InstancesPerService <= 0 {
		return fmt.Errorf("parkbench query: manifest instances_per_service %d is not positive", m.InstancesPerService)
	}
	if m.InstancesPerService > maxLabelSets {
		return fmt.Errorf("parkbench query: manifest instances_per_service %d exceeds the %d label set bound", m.InstancesPerService, maxLabelSets)
	}
	if len(m.Streams) > maxLabelSets/m.InstancesPerService {
		return fmt.Errorf("parkbench query: manifest %d streams times %d instances exceeds the %d label set bound",
			len(m.Streams), m.InstancesPerService, maxLabelSets)
	}
	if m.StreamLabel == instanceLabel {
		return fmt.Errorf("parkbench query: manifest stream label must not be the reserved instance label %q", instanceLabel)
	}
	seenStreams := make(map[string]bool, len(m.Streams))
	var streamLines, streamBytes int64
	for _, stream := range m.Streams {
		if stream.Name == "" {
			return errors.New("parkbench query: manifest has an empty stream name")
		}
		if seenStreams[stream.Name] {
			return fmt.Errorf("parkbench query: manifest has duplicate stream %q", stream.Name)
		}
		seenStreams[stream.Name] = true
		if _, ok := stream.Labels[instanceLabel]; ok {
			return fmt.Errorf("parkbench query: stream %q carries the reserved instance label %q", stream.Name, instanceLabel)
		}
		if stream.Lines < 0 || stream.RawLineBytes < 0 {
			return fmt.Errorf("parkbench query: stream %q has negative totals", stream.Name)
		}
		if stream.Lines > math.MaxInt64-streamLines || stream.RawLineBytes > math.MaxInt64-streamBytes {
			return fmt.Errorf("parkbench query: stream %q totals overflow", stream.Name)
		}
		streamLines += stream.Lines
		streamBytes += stream.RawLineBytes
	}
	if streamLines != m.Lines || streamBytes != m.RawLineBytes {
		return fmt.Errorf("parkbench query: stream totals (%d lines, %d bytes) differ from the corpus (%d lines, %d bytes)",
			streamLines, streamBytes, m.Lines, m.RawLineBytes)
	}
	if err := validateMarkers(m); err != nil {
		return err
	}
	return nil
}

// validateMarkers checks that exactly the four needle classes this command
// queries are present, with literals of the expected length and a schedule and
// ranges that agree with the corpus. Unknown markers, overlapping blocks or
// ranges, and counts that exceed the record count are rejected so the expected
// counts cannot be trusted for the wrong reason.
func validateMarkers(m *CorpusInfo) error {
	allowed := queryClassNames()
	seen := make(map[string]bool, len(m.Markers))
	for _, marker := range m.Markers {
		if marker.Name == "" {
			return errors.New("parkbench query: manifest has a marker without a name")
		}
		if !slices.Contains(allowed, marker.Name) {
			return fmt.Errorf("parkbench query: manifest has unknown marker %q", marker.Name)
		}
		if seen[marker.Name] {
			return fmt.Errorf("parkbench query: manifest has duplicate marker %q", marker.Name)
		}
		seen[marker.Name] = true
		if marker.Literal == "" {
			return fmt.Errorf("parkbench query: marker %q has an empty literal", marker.Name)
		}
		if marker.Bytes != len(marker.Literal) {
			return fmt.Errorf("parkbench query: marker %q records %d bytes for a %d-byte literal", marker.Name, marker.Bytes, len(marker.Literal))
		}
		if marker.Count < 0 || marker.Count > m.Lines {
			return fmt.Errorf("parkbench query: marker %q count %d is outside the %d-record corpus", marker.Name, marker.Count, m.Lines)
		}
		if marker.Schedule.EveryRecords < 0 || marker.Schedule.EveryRecords > math.MaxInt64-m.Lines {
			return fmt.Errorf("parkbench query: marker %q has an invalid schedule period %d", marker.Name, marker.Schedule.EveryRecords)
		}
		previousThrough := int64(-1)
		for _, block := range marker.Schedule.Blocks {
			if block.From < 0 || block.Through <= block.From {
				return fmt.Errorf("parkbench query: marker %q has an invalid schedule block [%d, %d)", marker.Name, block.From, block.Through)
			}
			if block.Through > m.Lines {
				return fmt.Errorf("parkbench query: marker %q schedule block [%d, %d) exceeds the %d-record corpus", marker.Name, block.From, block.Through, m.Lines)
			}
			if previousThrough > block.From {
				return fmt.Errorf("parkbench query: marker %q has overlapping or unordered schedule blocks", marker.Name)
			}
			previousThrough = block.Through
		}
		if err := validateMarkerRanges(m, marker); err != nil {
			return err
		}
	}
	for _, name := range allowed {
		if !seen[name] {
			return fmt.Errorf("parkbench query: manifest has no %q marker", name)
		}
	}
	rare, _ := findMarker(*m, queryClassRare)
	if rare.Bytes < 8 {
		return fmt.Errorf("parkbench query: rare marker %q is only %d bytes; a rare needle needs at least 8", rare.Literal, rare.Bytes)
	}
	absent, _ := findMarker(*m, queryClassAbsent)
	if absent.Bytes < 8 {
		return fmt.Errorf("parkbench query: absent marker %q is only %d bytes; an absent needle needs at least 8", absent.Literal, absent.Bytes)
	}
	if absent.Count != 0 {
		return fmt.Errorf("parkbench query: absent marker %q occurs %d times; it must be absent", absent.Literal, absent.Count)
	}
	short, _ := findMarker(*m, queryClassShort4Byte)
	if short.Bytes != 4 {
		return fmt.Errorf("parkbench query: short_4byte marker %q is %d bytes, want 4", short.Literal, short.Bytes)
	}
	// The schedule must reproduce the recorded count over the whole corpus and
	// every range must match the schedule inside its own interval; together
	// with the non-overlapping ranges this proves the ranges account for every
	// occurrence the count claims.
	for _, marker := range m.Markers {
		expected, err := m.ExpectedMarkerCount(marker.Name, m.Start, m.Through)
		if err != nil {
			return err
		}
		if expected != marker.Count {
			return fmt.Errorf("parkbench query: marker %q schedule yields %d occurrences over the corpus, but the manifest says %d", marker.Name, expected, marker.Count)
		}
		var ranged int64
		for _, r := range marker.Ranges {
			inRange, err := m.ExpectedMarkerCount(marker.Name, r.From, r.Through)
			if err != nil {
				return err
			}
			if inRange != r.Count {
				return fmt.Errorf("parkbench query: marker %q range %v..%v records %d occurrences, but its schedule has %d", marker.Name, r.From, r.Through, r.Count, inRange)
			}
			if r.Count > math.MaxInt64-ranged {
				return fmt.Errorf("parkbench query: marker %q range counts overflow", marker.Name)
			}
			ranged += r.Count
		}
		if marker.Count > 0 && len(marker.Ranges) == 0 {
			return fmt.Errorf("parkbench query: marker %q has %d occurrences but no ranges", marker.Name, marker.Count)
		}
		if ranged != marker.Count {
			return fmt.Errorf("parkbench query: marker %q ranges account for %d of %d occurrences", marker.Name, ranged, marker.Count)
		}
	}
	return nil
}

// validateMarkerRanges checks that ranges are microsecond-aligned, inside the
// corpus and strictly ordered without overlap.
func validateMarkerRanges(m *CorpusInfo, marker MarkerInfo) error {
	var previousThrough time.Time
	for i, r := range marker.Ranges {
		if !r.Through.After(r.From) {
			return fmt.Errorf("parkbench query: marker %q range %d is empty: [%v, %v)", marker.Name, i, r.From, r.Through)
		}
		if r.From.Nanosecond()%1000 != 0 || r.Through.Nanosecond()%1000 != 0 {
			return fmt.Errorf("parkbench query: marker %q range %d is not microsecond-aligned", marker.Name, i)
		}
		if r.From.Before(m.Start) || r.Through.After(m.Through) {
			return fmt.Errorf("parkbench query: marker %q range %d [%v, %v) is outside the corpus", marker.Name, i, r.From, r.Through)
		}
		if i > 0 && r.From.Before(previousThrough) {
			return fmt.Errorf("parkbench query: marker %q ranges overlap or are unordered", marker.Name)
		}
		previousThrough = r.Through
	}
	return nil
}

// runQueryCommand parses the query flags, loads the manifest, runs the
// measurement and writes the JSON and Markdown artifacts.
func runQueryCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("parkbench query", flag.ContinueOnError)
	fs.SetOutput(stderr)
	backend := fs.String("backend", "", "backend to measure: parkana or loki")
	backendURL := fs.String("url", "", "backend base URL, or a full instant-query endpoint ending in /loki/api/v1/query")
	metricsURL := fs.String("metrics-url", "", "Parkana metrics endpoint (default: -url plus /metrics; parkana only)")
	manifestPath := fs.String("manifest", "", "ingest report JSON written by \"parkbench ingest --output\" (the corpus identity)")
	tenantFlag := fs.String("tenant", "", "tenant id sent as X-Scope-OrgID (default: the manifest corpus tenant)")
	repetitions := fs.Int("repetitions", 5, "timed observations per class and window")
	warmups := fs.Int("warmups", 2, "untimed warmup requests per class and window")
	timeout := fs.Duration("timeout", 60*time.Second, "per-request HTTP deadline (0 disables)")
	classesFlag := fs.String("classes", strings.Join(queryClassNames(), ","), "comma-separated needle classes: "+strings.Join(queryClassNames(), ","))
	windowsFlag := fs.String("windows", strings.Join(queryWindowNames(), ","), "comma-separated windows: "+strings.Join(queryWindowNames(), ","))
	output := fs.String("output", "", "write the JSON report to this file instead of stdout")
	markdown := fs.String("markdown", "", "write a Markdown summary to this file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("parkbench query: unexpected arguments %q", fs.Args())
	}
	if *manifestPath == "" {
		return errors.New("parkbench query: -manifest is required")
	}
	if err := validateArtifactPaths(*manifestPath, *output, *markdown); err != nil {
		return err
	}
	manifest, err := loadIngestManifest(*manifestPath)
	if err != nil {
		return err
	}
	cfg := QueryConfig{
		Backend:     *backend,
		URL:         *backendURL,
		MetricsURL:  *metricsURL,
		Tenant:      tenant.ID(*tenantFlag),
		Repetitions: *repetitions,
		Warmups:     *warmups,
		Timeout:     *timeout,
		Classes:     splitNames(*classesFlag),
		Windows:     splitNames(*windowsFlag),
		Progress:    stderr,
	}
	report, runErr := RunQueries(ctx, cfg, *manifest)
	if report == nil {
		return runErr
	}
	encoded, err := marshalQueryReport(report)
	if err != nil {
		return errors.Join(runErr, err)
	}
	var writeErrs []error
	if *output != "" {
		if err := os.WriteFile(*output, encoded, 0o644); err != nil {
			writeErrs = append(writeErrs, fmt.Errorf("parkbench query: write %s: %w", *output, err))
		} else {
			fmt.Fprintf(stdout, "parkbench: %d series, %d observations, valid=%t in %.2fs; report: %s\n",
				len(report.Series), report.observationCount(), report.Valid, report.ElapsedSeconds, *output)
		}
	} else if _, err := stdout.Write(encoded); err != nil {
		writeErrs = append(writeErrs, err)
	}
	if *markdown != "" {
		var buf bytes.Buffer
		if err := WriteMarkdown(&buf, report); err != nil {
			writeErrs = append(writeErrs, err)
		} else if err := os.WriteFile(*markdown, buf.Bytes(), 0o644); err != nil {
			writeErrs = append(writeErrs, fmt.Errorf("parkbench query: write %s: %w", *markdown, err))
		} else if *output != "" {
			fmt.Fprintf(stdout, "parkbench: markdown: %s\n", *markdown)
		}
	}
	return errors.Join(runErr, errors.Join(writeErrs...))
}

// observationCount counts the timed observations recorded in the report.
func (r *QueryReport) observationCount() int {
	total := 0
	for _, series := range r.Series {
		total += len(series.Samples)
	}
	return total
}

// splitNames splits a comma-separated flag value; an empty value stays empty so
// normalize treats it as "all".
func splitNames(value string) []string {
	var out []string
	for part := range strings.SplitSeq(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// validateArtifactPaths rejects artifact paths that would overwrite each other
// or the required manifest. It runs before the manifest is read or any request
// is made, so a typo cannot destroy the corpus identity or hide the JSON.
func validateArtifactPaths(manifestPath, output, markdown string) error {
	pairs := []struct {
		first, second, description string
	}{
		{output, markdown, "-output and -markdown"},
		{output, manifestPath, "-output and -manifest"},
		{markdown, manifestPath, "-markdown and -manifest"},
	}
	for _, pair := range pairs {
		if pair.first == "" || pair.second == "" {
			continue
		}
		if sameFilePath(pair.first, pair.second) {
			return fmt.Errorf("parkbench query: %s must not name the same file (%s)", pair.description, pair.first)
		}
	}
	return nil
}

// sameFilePath reports whether two paths name the same file. It resolves
// symlinks in existing components and, for a file that does not exist yet,
// resolves its parent directory, so two aliases of the same destination compare
// equal before either artifact is created. os.SameFile covers existing hard
// links.
func sameFilePath(a, b string) bool {
	canonicalA, errA := canonicalFilePath(a)
	canonicalB, errB := canonicalFilePath(b)
	if errA == nil && errB == nil && canonicalA == canonicalB {
		return true
	}
	infoA, errA := os.Stat(a)
	infoB, errB := os.Stat(b)
	return errA == nil && errB == nil && os.SameFile(infoA, infoB)
}

// canonicalFilePath resolves the parts of path that exist. A nonexistent file
// whose parent directory exists resolves through that directory, which is the
// case that matters for a new artifact: a writable new path always has an
// existing parent.
func canonicalFilePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, nil
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		// The parent does not exist either; the best available identity is the
		// cleaned absolute path, and the eventual write will fail clearly.
		return filepath.Clean(abs), nil
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

// loadIngestManifest reads and validates one ingest report.
func loadIngestManifest(path string) (*Report, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parkbench query: open manifest %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	body, err := readBoundedBody(file, maxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("parkbench query: read manifest %s: %w", path, err)
	}
	var report Report
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, fmt.Errorf("parkbench query: decode manifest %s: %w", path, err)
	}
	if err := validateManifest(&report); err != nil {
		return nil, err
	}
	return &report, nil
}
