// Command lokicompare runs a corpus of Loki read API requests against two
// servers and compares their normalized responses.
//
// It exists so Parkana's Loki-compatible surface can be checked against real
// Loki with the same queries, parameters and time bounds. The command imports
// no storage, query or DuckDB code; it only speaks HTTP and JSON.
//
// Exit codes are part of its contract: 0 means every corpus entry compared
// equal, 1 means at least one semantic difference, and 2 means the invocation
// was invalid or a request/response failed (transport, unexpected HTTP status
// or JSON decoding). Failures are never reported as equal empty results.
//
// See testdata/example-corpus.json for a corpus example.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	exitEqual     = 0
	exitDifferent = 1
	exitFailure   = 2

	// maxBodyBytes bounds one response so a misbehaving server cannot exhaust
	// memory before decoding rejects it.
	maxBodyBytes = 64 << 20
	// maxReportedDiffs caps the per-entry difference lines printed.
	maxReportedDiffs = 10
)

const usageText = `lokicompare compares Loki read API responses from two servers.

Usage:
  lokicompare -left URL -right URL -corpus PATH -start TIME -end TIME [flags]

The corpus is a JSON array of request definitions:
  [{"name":"logs-basic","type":"range","query":"{app=\"api\"}",
    "params":{"limit":"100","direction":"backward"}}]

Fields:
  name    unique, human-readable entry name used in reports
  type    range, instant, labels, label_values, series, stats, volume,
          volume_range
  query   LogQL query or stream selector (type-dependent; stats/volume* and
          range/instant require it)
  params  query parameters; each value is a string or an array of strings for
          repeated parameters such as match[]
  lokiQuirk
          optional, one of: scalar-timestamp (instant entries only). Opts into
          one documented Loki 3.7.9 deviation; see below
  expect  optional exact assertions (see below): stats and series check the
          RIGHT server only, status asserts both servers return the same
          expected HTTP error

params rules:
  - label_values consumes params.label as the path label name; it is not sent
    as a query parameter. The name is path-escaped.
  - For labels and label_values, a non-empty query field is sent as the query
    parameter, the scope selector Grafana uses. For series, it is sent as the
    first match[] parameter.
  - range and instant use params.start/end/time when supplied, otherwise the
    -start/-end flags. instant uses -end as its evaluation time.
  - Every request uses GET with URL-encoded query parameters.

Comparison:
  - streams, entries and series are order-insensitive; duplicate entries are
    preserved.
  - labels and label_values compare as sets; series compares label-set sets.
  - metric values compare as floats with -float-abs/-float-rel tolerance.
    NaN equals NaN, infinities equal themselves, and neither equals a finite
    value.
  - data.stats attached to regular query responses is ignored as
    implementation specific.
  - index/stats compares all four counts (streams, entries, bytes, chunks) as
    informational LEFT/RIGHT estimates and never fails on them; the documented
    non-negative integer shape is still required, and expect.stats asserts
    exact RIGHT values.
  - index/volume and index/volume_range compare result shape and metric label
    sets; sample values are informational. Volume responses must include a
    data.stats object. A bare {} selector documents grouping-label
    differences as informational, but volume_range still compares exact
    timestamp grids: the unique union across all series plus each shared
    metric key.
  - index/volume sample timestamps are request processing times and are never
    compared across servers. They must be numeric Unix seconds inside each
    request's wall-clock span +/-60s; historical -start/-end bounds do not
    define that window.
  - volume_range follows Loki's frontend rule: the result is a vector while
    every series has one bucket and a matrix once any series has more than one
    (independent of the request bucket count). Both sides must have the same
    shape. Every sample must be numeric Unix seconds on one of the exact
    bucket ends derived from the request's start, end and step (millisecond
    rounding, step alignment, one-millisecond SplitGap before non-final
    boundaries). Historical timestamps are compared exactly; the instant
    volume wall-clock rule does not apply.
  - volume_range timestamps compare exactly even with -timestamp-precision=us.
  - timestamps compare exactly by default; -timestamp-precision=us floors
    both sides to the stored microsecond grid explicitly (including negative
    Unix-nanosecond times).

Exact expectations (optional expect field):
  stats:        {"stats":{"streams":N,"entries":N,"bytes":N}}
  volume:       {"series":[{"metric":{...},"value":1200}]}
  volume_range: {"series":[{"metric":{...},"values":[[1700000000,1200]]}]}
  status:       {"status":400}
  - The stats and series forms are valid only on stats, volume and
    volume_range entries. stats takes at least one of streams, entries and
    bytes; series takes complete metric label maps and exactly one of value
    (instant) or values (range). An empty series array asserts that the
    RIGHT result has no series.
  - status is a JSON integer in [400, 599], valid on every entry type and
    exclusive with stats and series. Both servers must answer that exact
    status with a non-empty UTF-8 text/plain body (charset and other media
    parameters are accepted); the message text is never compared. A wrong
    status or an empty, non-text or binary body is a semantic difference
    (exit 1); transport, read and size failures still exit 2. Loki answers a
    log-selector instant query with HTTP 400 (see the
    instant-log-selector-rejected corpus entry).
  - Values may be JSON numbers or decimal strings and compare exactly, without
    -float-abs/-float-rel. 1200 equals 1200.0 but not 1201. Fractions, hex and
    other non-decimal spellings are invalid. stats counts are non-negative
    integers.
  - bytes is the sum of len(line) for matched rows; labels, structured
    metadata, compression and Parquet overhead are excluded.
  - Expected and RIGHT series sets must match exactly. Missing or extra series
    and value mismatches exit 1. Invalid, null, misplaced or malformed
    expectations exit 2 before any request.
  - Instant volume expectations carry no timestamp. Range expectation
    timestamps are numeric Unix seconds and compare exactly. The default e2e
    corpus should use expect.stats and instant volume expect.series over the
    full uploaded range; it cannot carry static volume_range timestamps for
    data that starts at generation time.
  - lokiQuirk is a flat entry field, not part of expect. The only supported
    value is "scalar-timestamp", and it is valid only on instant entries
    without expect. Both servers must return a scalar result or the invocation
    fails (exit 2). Scalar values keep the -float-abs/-float-rel tolerances.
    LEFT may carry a numeric JSON timestamp outside the int64 nanosecond range
    (Loki 3.7.9 calls model.TimeFromUnix on an already-millisecond
    LegacySample.TimestampMs in pkg/querier/queryrange/prometheus.go and
    overflows it again in the protobuf round trip); that timestamp is
    reported in a note and never compared. RIGHT and every unopt-in entry
    stay strict, and RIGHT must be the request evaluation instant truncated to
    milliseconds. Null, non-string, unknown or misplaced quirk values fail
    before any request.
Policy details: docs/logql.md#stats-and-volume

Example corpus: cmd/lokicompare/testdata/example-corpus.json

Exit codes:
  0  every corpus entry compared equal
  1  at least one semantic difference
  2  invalid invocation or a transport, unexpected HTTP status or JSON
     decode failure

Flags:
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// config is the validated command invocation.
type config struct {
	left, right string
	corpus      string
	bounds      timeBounds
	tenant      tenant.ID
	timeout     time.Duration
	opts        compareOptions
	precision   timestampPrecision
}

// run executes one comparison pass and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, err, printed := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return exitEqual
	}
	if err != nil {
		if !printed {
			fmt.Fprintf(stderr, "lokicompare: %v\n", err)
		}
		return exitFailure
	}
	entries, err := loadCorpus(cfg.corpus)
	if err != nil {
		fmt.Fprintf(stderr, "lokicompare: %v\n", err)
		return exitFailure
	}

	client := &http.Client{}
	equal, different, failed := 0, 0, 0
	for _, entry := range entries {
		endpoint, result, err := compareEntry(ctx, client, cfg, entry)
		if err != nil {
			failed++
			fmt.Fprintf(stderr, "FAIL %s: %v\n", entryLabel(entry), err)
			continue
		}
		if len(result.diffs) == 0 {
			equal++
			fmt.Fprintf(stdout, "ok   %s\n", entry.Name)
			printNotes(stdout, entry.Name, result.notes)
			continue
		}
		different++
		fmt.Fprintf(stdout, "DIFF %s (%s)\n", entry.Name, endpoint)
		for i, diff := range result.diffs {
			if i >= maxReportedDiffs {
				fmt.Fprintf(stdout, "  ... and %d more difference(s)\n", len(result.diffs)-maxReportedDiffs)
				break
			}
			fmt.Fprintf(stdout, "  %s\n", diff)
		}
		printNotes(stdout, entry.Name, result.notes)
	}
	fmt.Fprintf(stdout, "%d entries: %d equal, %d different, %d failed\n",
		len(entries), equal, different, failed)
	switch {
	case failed > 0:
		return exitFailure
	case different > 0:
		return exitDifferent
	default:
		return exitEqual
	}
}

// entryLabel identifies an entry in failure reports, including a bounded
// version of its query so a failure is actionable without opening the corpus.
func entryLabel(entry corpusEntry) string {
	if entry.Query == "" {
		return entry.Name
	}
	return fmt.Sprintf("%s query=%q", entry.Name, short(entry.Query))
}

// printNotes reports informational observations without affecting the exit
// code. The list is bounded like difference output.
func printNotes(stdout io.Writer, name string, notes []string) {
	for i, note := range notes {
		if i >= maxReportedDiffs {
			fmt.Fprintf(stdout, "  ... and %d more informational note(s)\n", len(notes)-maxReportedDiffs)
			break
		}
		fmt.Fprintf(stdout, "  info %s: %s\n", name, note)
	}
}

// compareEntry fetches one corpus entry from both servers and compares the
// normalized results. The returned endpoint is used in difference reports.
func compareEntry(ctx context.Context, client *http.Client, cfg config, entry corpusEntry) (string, comparison, error) {
	spec, err := buildSpec(entry, cfg.bounds, cfg.tenant)
	if err != nil {
		return "", comparison{}, err
	}
	endpoint := spec.endpoint()

	if entry.Expect != nil && entry.Expect.status != nil {
		result, err := compareExpectedStatus(ctx, client, cfg, spec, entry.Expect)
		if err != nil {
			return endpoint, comparison{}, err
		}
		return endpoint, result, nil
	}

	leftWindow := requestWindow{start: time.Now()}
	leftBody, err := fetch(ctx, client, cfg.left, spec, cfg.timeout)
	leftWindow.end = time.Now()
	if err != nil {
		return endpoint, comparison{}, fmt.Errorf("left: %w", err)
	}
	rightWindow := requestWindow{start: time.Now()}
	rightBody, err := fetch(ctx, client, cfg.right, spec, cfg.timeout)
	rightWindow.end = time.Now()
	if err != nil {
		return endpoint, comparison{}, fmt.Errorf("right: %w", err)
	}

	result, err := compareBodies(entry, spec, leftBody, rightBody, cfg, requestWindows{left: leftWindow, right: rightWindow})
	if err != nil {
		return endpoint, comparison{}, fmt.Errorf("%s: %w", endpoint, err)
	}
	return endpoint, result, nil
}

// compareBodies decodes and compares two successful response bodies.
func compareBodies(entry corpusEntry, spec requestSpec, leftBody, rightBody []byte, cfg config, windows requestWindows) (comparison, error) {
	switch entry.Type {
	case queryStats:
		left, err := decodeStatsResponse(leftBody)
		if err != nil {
			return comparison{}, fmt.Errorf("left: %w", err)
		}
		right, err := decodeStatsResponse(rightBody)
		if err != nil {
			return comparison{}, fmt.Errorf("right: %w", err)
		}
		result := compareStats(left, right)
		result.diffs = append(result.diffs, checkStatsExpectation(entry.Expect, right)...)
		return result, nil
	case queryLabels, queryLabelValues:
		left, err := decodeLabelsResponse(leftBody)
		if err != nil {
			return comparison{}, fmt.Errorf("left: %w", err)
		}
		right, err := decodeLabelsResponse(rightBody)
		if err != nil {
			return comparison{}, fmt.Errorf("right: %w", err)
		}
		return comparison{diffs: diffLabelSets(left, right)}, nil
	case querySeries:
		left, err := decodeSeriesResponse(leftBody)
		if err != nil {
			return comparison{}, fmt.Errorf("left: %w", err)
		}
		right, err := decodeSeriesResponse(rightBody)
		if err != nil {
			return comparison{}, fmt.Errorf("right: %w", err)
		}
		return comparison{diffs: diffSeriesSets(left, right)}, nil
	case queryVolume, queryVolumeRange:
		left, err := decodeQueryResponse(leftBody, cfg.precision)
		if err != nil {
			return comparison{}, fmt.Errorf("left: %w", err)
		}
		right, err := decodeQueryResponse(rightBody, cfg.precision)
		if err != nil {
			return comparison{}, fmt.Errorf("right: %w", err)
		}
		var grid []int64
		if entry.Type == queryVolumeRange {
			grid, err = volumeRangeGridFromSpec(spec)
			if err != nil {
				return comparison{}, err
			}
		}
		result, err := compareVolumeResults(entry.Type, left, right, entry.Query, grid, windows)
		if err != nil {
			return comparison{}, err
		}
		result.diffs = append(result.diffs, checkVolumeExpectation(entry, right)...)
		return result, nil
	default:
		mode := decodeMode{allowScalarTimestampOverflow: entry.LokiQuirk == lokiQuirkScalarTimestamp}
		left, err := decodeQueryResponseMode(leftBody, cfg.precision, mode)
		if err != nil {
			return comparison{}, fmt.Errorf("left: %w", err)
		}
		right, err := decodeQueryResponse(rightBody, cfg.precision)
		if err != nil {
			return comparison{}, fmt.Errorf("right: %w", err)
		}
		if entry.LokiQuirk == lokiQuirkScalarTimestamp {
			eval, err := parseTime(spec.query.Get("time"))
			if err != nil {
				return comparison{}, fmt.Errorf("lokiQuirk instant time: %w", err)
			}
			return compareScalarTimestampQuirk(left, right, eval.UnixNano(), cfg.precision, cfg.opts)
		}
		return comparison{diffs: diffQueryResults(left, right, cfg.opts)}, nil
	}
}

// compareExpectedStatus fetches an expect.status entry from both servers and
// checks the expected error. Both requests are always attempted; transport,
// read and size failures win over status or body mismatches.
func compareExpectedStatus(ctx context.Context, client *http.Client, cfg config, spec requestSpec, exp *expectation) (comparison, error) {
	left, leftErr := fetchRaw(ctx, client, cfg.left, spec, cfg.timeout)
	right, rightErr := fetchRaw(ctx, client, cfg.right, spec, cfg.timeout)
	if leftErr != nil {
		return comparison{}, fmt.Errorf("left: %w", leftErr)
	}
	if rightErr != nil {
		return comparison{}, fmt.Errorf("right: %w", rightErr)
	}
	return comparison{diffs: checkStatusExpectation(exp, left, right)}, nil
}

// rawResponse keeps the status line, content type and body of one response,
// including HTTP errors.
type rawResponse struct {
	status      int
	statusLine  string
	contentType string
	body        []byte
}

// fetch performs one request against one server and returns the body of a 2xx
// response. HTTP error statuses and oversized bodies are failures.
func fetch(ctx context.Context, client *http.Client, base string, spec requestSpec, timeout time.Duration) ([]byte, error) {
	resp, err := fetchRaw(ctx, client, base, spec, timeout)
	if err != nil {
		return nil, err
	}
	if resp.status < 200 || resp.status >= 300 {
		return nil, fmt.Errorf("%s: HTTP %s: %s", spec.endpoint(), resp.statusLine, bodySnippet(resp.body))
	}
	return resp.body, nil
}

// fetchRaw performs one request and returns the response for any status.
// Only transport, read and size failures are errors.
func fetchRaw(ctx context.Context, client *http.Client, base string, spec requestSpec, timeout time.Duration) (rawResponse, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, spec.method, spec.url(base), nil)
	if err != nil {
		return rawResponse{}, fmt.Errorf("%s: %w", spec.endpoint(), err)
	}
	maps.Copy(req.Header, spec.headers)
	resp, err := client.Do(req)
	if err != nil {
		return rawResponse{}, fmt.Errorf("%s: %w", spec.endpoint(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return rawResponse{}, fmt.Errorf("%s: read response: %w", spec.endpoint(), err)
	}
	if len(body) > maxBodyBytes {
		return rawResponse{}, fmt.Errorf("%s: response exceeds %d bytes", spec.endpoint(), maxBodyBytes)
	}
	return rawResponse{
		status:      resp.StatusCode,
		statusLine:  resp.Status,
		contentType: resp.Header.Get("Content-Type"),
		body:        body,
	}, nil
}

// bodySnippet bounds error bodies included in failure messages.
func bodySnippet(body []byte) string {
	const limit = 300
	s := string(body)
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	if s == "" {
		return "(empty body)"
	}
	return s
}

// parseFlags parses and validates the invocation. printed reports whether the
// flag package already wrote the error (and usage) to stderr.
func parseFlags(args []string, stderr io.Writer) (cfg config, err error, printed bool) {
	fs := flag.NewFlagSet("lokicompare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		left      = fs.String("left", "", "base URL of the left server, e.g. http://localhost:3100")
		right     = fs.String("right", "", "base URL of the right server, e.g. http://localhost:3200")
		corpus    = fs.String("corpus", "", "path to the JSON corpus file")
		start     = fs.String("start", "", "default range start: RFC3339 or integer Unix nanoseconds")
		end       = fs.String("end", "", "default range end: RFC3339 or integer Unix nanoseconds")
		tenantStr = fs.String("tenant", "", "tenant id sent as X-Scope-OrgID (empty: no header)")
		timeout   = fs.Duration("timeout", 30*time.Second, "per-request HTTP timeout (0 disables it)")
		floatAbs  = fs.Float64("float-abs", 1e-9, "absolute float comparison tolerance")
		floatRel  = fs.Float64("float-rel", 1e-9, "relative float comparison tolerance")
		precision = fs.String("timestamp-precision", string(precisionExact), "timestamp comparison: exact or us (floor to the stored microsecond grid)")
	)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usageText)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return config{}, flag.ErrHelp, true
		}
		return config{}, err, true
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected arguments %q", fs.Args()), false
	}

	if *left == "" || *right == "" || *corpus == "" || *start == "" || *end == "" {
		return config{}, errors.New("-left, -right, -corpus, -start and -end are required"), false
	}
	leftBase, err := normalizeBase(*left)
	if err != nil {
		return config{}, fmt.Errorf("-left: %w", err), false
	}
	rightBase, err := normalizeBase(*right)
	if err != nil {
		return config{}, fmt.Errorf("-right: %w", err), false
	}
	startTime, err := parseTime(*start)
	if err != nil {
		return config{}, fmt.Errorf("-start: %w", err), false
	}
	endTime, err := parseTime(*end)
	if err != nil {
		return config{}, fmt.Errorf("-end: %w", err), false
	}
	if startTime.After(endTime) {
		return config{}, fmt.Errorf("-start %s is after -end %s", *start, *end), false
	}
	id := tenant.ID(*tenantStr)
	if id != "" {
		if err := id.Validate(); err != nil {
			return config{}, fmt.Errorf("-tenant: %w", err), false
		}
	}
	if *timeout < 0 {
		return config{}, fmt.Errorf("-timeout must not be negative, got %v", *timeout), false
	}
	if math.IsNaN(*floatAbs) || math.IsInf(*floatAbs, 0) || *floatAbs < 0 {
		return config{}, fmt.Errorf("-float-abs must be a finite non-negative tolerance, got %v", *floatAbs), false
	}
	if math.IsNaN(*floatRel) || math.IsInf(*floatRel, 0) || *floatRel < 0 {
		return config{}, fmt.Errorf("-float-rel must be a finite non-negative tolerance, got %v", *floatRel), false
	}
	precisionValue := timestampPrecision(*precision)
	if !precisionValue.valid() {
		return config{}, fmt.Errorf("-timestamp-precision must be exact or us, got %q", *precision), false
	}
	return config{
		left:      leftBase,
		right:     rightBase,
		corpus:    *corpus,
		bounds:    timeBounds{start: startTime, end: endTime},
		tenant:    id,
		timeout:   *timeout,
		opts:      compareOptions{floatAbs: *floatAbs, floatRel: *floatRel},
		precision: precisionValue,
	}, nil, false
}
