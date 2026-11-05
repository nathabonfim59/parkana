package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// apiPrefix is the Loki-compatible read API prefix on a server base URL.
	apiPrefix = "/loki/api/v1"
	// encodingFlagsHeader asks Loki and Parkana for categorized entry labels.
	encodingFlagsHeader = "X-Loki-Response-Encoding-Flags"
	categorizeLabels    = "categorize-labels"
)

// requestSpec is one corpus request before it is bound to a server base URL.
type requestSpec struct {
	method  string
	path    string
	query   url.Values
	headers http.Header
}

// endpoint renders the request for reports, without query parameters.
func (s requestSpec) endpoint() string { return s.method + " " + s.path }

// url binds the request to a server base URL.
func (s requestSpec) url(base string) string {
	if len(s.query) == 0 {
		return base + s.path
	}
	return base + s.path + "?" + s.query.Encode()
}

// timeBounds are the CLI's default time bounds.
type timeBounds struct {
	start time.Time
	end   time.Time
}

// buildSpec turns a corpus entry into a request. Bounds and query text come
// from the entry's params where present, otherwise from the CLI defaults.
func buildSpec(e corpusEntry, bounds timeBounds, id tenant.ID) (requestSpec, error) {
	spec := requestSpec{method: http.MethodGet, headers: http.Header{}}
	spec.headers.Set("Accept", "application/json")
	if id != "" {
		spec.headers.Set(tenant.Header, string(id))
	}

	values := make(url.Values, len(e.Params))
	for k, vs := range e.Params {
		values[k] = slices.Clone(vs)
	}
	if err := canonicalizeTimeParams(values); err != nil {
		return requestSpec{}, err
	}

	switch e.Type {
	case queryRange, queryInstant:
		spec.path = apiPrefix + "/query_range"
		if e.Type == queryInstant {
			spec.path = apiPrefix + "/query"
		}
		values.Set("query", e.Query)
		spec.headers.Set(encodingFlagsHeader, categorizeLabels)
	case queryLabels:
		spec.path = apiPrefix + "/labels"
		setScope(values, e.Query)
	case queryLabelValues:
		label := values.Get("label")
		values.Del("label")
		spec.path = apiPrefix + "/label/" + url.PathEscape(label) + "/values"
		setScope(values, e.Query)
	case querySeries:
		spec.path = apiPrefix + "/series"
		appendMatch(values, e.Query)
	case queryStats:
		spec.path = apiPrefix + "/index/stats"
		values.Set("query", e.Query)
	case queryVolume:
		spec.path = apiPrefix + "/index/volume"
		values.Set("query", e.Query)
	case queryVolumeRange:
		spec.path = apiPrefix + "/index/volume_range"
		values.Set("query", e.Query)
	default:
		return requestSpec{}, fmt.Errorf("unsupported type %q", e.Type)
	}

	if e.Type == queryInstant {
		if values.Get("time") == "" {
			values.Set("time", formatTime(bounds.end))
		}
	} else {
		if values.Get("start") == "" {
			values.Set("start", formatTime(bounds.start))
		}
		if values.Get("end") == "" {
			values.Set("end", formatTime(bounds.end))
		}
		start, err := parseTime(values.Get("start"))
		if err != nil {
			return requestSpec{}, fmt.Errorf("param start: %w", err)
		}
		end, err := parseTime(values.Get("end"))
		if err != nil {
			return requestSpec{}, fmt.Errorf("param end: %w", err)
		}
		if start.After(end) {
			return requestSpec{}, fmt.Errorf("start %s is after end %s", values.Get("start"), values.Get("end"))
		}
	}

	spec.query = values
	return spec, nil
}

// canonicalizeTimeParams rewrites start/end/time parameters to RFC3339Nano so
// both sides receive exactly the same textual bounds.
func canonicalizeTimeParams(values url.Values) error {
	for _, key := range []string{"start", "end", "time"} {
		for i, raw := range values[key] {
			t, err := parseTime(raw)
			if err != nil {
				return fmt.Errorf("param %s: %w", key, err)
			}
			values[key][i] = formatTime(t)
		}
	}
	return nil
}

// setScope adds a stream selector as the query parameter, which is how Loki's
// labels and label_values endpoints scope their metadata responses.
func setScope(values url.Values, query string) {
	if strings.TrimSpace(query) != "" {
		values.Set("query", query)
	}
}

// appendMatch prepends the entry query as a match[] selector, keeping any
// match[] values the entry's params already supplied. Only series uses it.
func appendMatch(values url.Values, query string) {
	if strings.TrimSpace(query) == "" {
		return
	}
	values["match[]"] = append([]string{query}, values["match[]"]...)
}

// parseTime accepts RFC3339 (optionally with nanoseconds) or integer Unix
// nanoseconds.
func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty timestamp")
	}
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, ns).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp %q: want RFC3339 or integer Unix nanoseconds", s)
	}
	return t.UTC(), nil
}

// formatTime renders a timestamp the way both Loki and Parkana accept.
func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// maxVolumeRangeBuckets bounds the request-derived volume_range grid so a tiny
// step over a huge range cannot exhaust memory before the response is compared.
const maxVolumeRangeBuckets = 1 << 15

// volumeRangeGridFromSpec derives the exact volume_range bucket ends from the
// request's start, end and step parameters.
func volumeRangeGridFromSpec(spec requestSpec) ([]int64, error) {
	start, err := parseTime(spec.query.Get("start"))
	if err != nil {
		return nil, fmt.Errorf("volume_range start: %w", err)
	}
	end, err := parseTime(spec.query.Get("end"))
	if err != nil {
		return nil, fmt.Errorf("volume_range end: %w", err)
	}
	step, err := parseStep(spec.query.Get("step"))
	if err != nil {
		return nil, fmt.Errorf("volume_range step: %w", err)
	}
	if spec.query.Get("step") == "" {
		step = defaultVolumeRangeStep(end.Sub(start))
	}
	return volumeRangeGrid(start, end, step)
}

// defaultVolumeRangeStep mirrors loghttp's default step for range and volume
// range requests: floor(span seconds / 250), at least one second.
func defaultVolumeRangeStep(span time.Duration) time.Duration {
	seconds := max(int(math.Floor(span.Seconds()/250)), 1)
	return time.Duration(seconds) * time.Second
}

// parseStep accepts Loki's step parameter forms: a decimal number of seconds
// ("300") or a Prometheus model duration ("5m", "1d", "1w", "1y"). The step of a
// successful response must be positive and representable; zero, negative,
// non-finite and overflowing values are rejected.
func parseStep(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	if seconds, err := strconv.ParseFloat(s, 64); err == nil {
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			return 0, fmt.Errorf("%q is not a finite duration", raw)
		}
		ns := seconds * float64(time.Second)
		if ns > float64(math.MaxInt64) || ns < float64(math.MinInt64) {
			return 0, fmt.Errorf("%q overflows a duration", raw)
		}
		step := time.Duration(ns)
		if step <= 0 {
			return 0, fmt.Errorf("%q must be a positive duration", raw)
		}
		return step, nil
	}
	step, err := parseModelDuration(s)
	if err != nil {
		return 0, err
	}
	if step <= 0 {
		return 0, fmt.Errorf("%q must be a positive duration", raw)
	}
	return step, nil
}

// modelDurationUnit returns the Prometheus model duration unit: a year is 365
// days and a week is 7 days. Positions enforce biggest-to-smallest order. A
// pure lookup keeps the test-visible grammar immutable.
func modelDurationUnit(unit string) (position int, nanos uint64, ok bool) {
	switch unit {
	case "ms":
		return 7, uint64(time.Millisecond), true
	case "s":
		return 6, uint64(time.Second), true
	case "m":
		return 5, uint64(time.Minute), true
	case "h":
		return 4, uint64(time.Hour), true
	case "d":
		return 3, uint64(24 * time.Hour), true
	case "w":
		return 2, uint64(7 * 24 * time.Hour), true
	case "y":
		return 1, uint64(365 * 24 * time.Hour), true
	default:
		return 0, 0, false
	}
}

// parseModelDuration parses Prometheus model durations, whose d/w/y units Go's
// time.ParseDuration does not support. It follows the pinned
// prometheus/common/model.ParseDuration rules: unsigned integers, units in
// biggest-to-smallest order, and an explicit int64 overflow check.
func parseModelDuration(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	if s == "" {
		return 0, errors.New("empty duration")
	}
	var total uint64
	lastPosition := 0
	for rest := s; rest != ""; {
		digits := 0
		for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
			digits++
		}
		if digits == 0 {
			return 0, fmt.Errorf("duration %q must start with an integer", s)
		}
		value, err := strconv.ParseUint(rest[:digits], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("duration %q: %w", s, err)
		}
		rest = rest[digits:]

		letters := 0
		for letters < len(rest) && (rest[letters] < '0' || rest[letters] > '9') {
			letters++
		}
		if letters == 0 {
			return 0, fmt.Errorf("duration %q is missing a unit", s)
		}
		position, nanos, ok := modelDurationUnit(rest[:letters])
		if !ok {
			return 0, fmt.Errorf("duration %q: unknown unit %q", s, rest[:letters])
		}
		rest = rest[letters:]
		if position <= lastPosition {
			return 0, fmt.Errorf("duration %q units must be ordered from biggest to smallest", s)
		}
		lastPosition = position
		if value > (1<<63)/nanos {
			return 0, fmt.Errorf("duration %q is out of range", s)
		}
		total += value * nanos
		if total > 1<<63-1 {
			return 0, fmt.Errorf("duration %q is out of range", s)
		}
	}
	return time.Duration(total), nil
}

// volumeRangeGrid returns the bucket-end timestamps Loki's volume frontend can
// emit for one request. The frontend rounds the bounds to milliseconds, aligns
// the first bucket down to the step, inserts a one-millisecond SplitGap before
// every non-final boundary, and caps the final boundary at the rounded-through
// bound (pkg/util/time.go ForInterval, pkg/util/conv.go RoundToMilliseconds,
// pkg/querier/queryrange/volume.go). The first bucket can be shorter than the
// step because the aligned start may fall inside the request's first interval.
// Timestamps are returned as exact Unix nanoseconds on the millisecond grid. A
// zero step makes one bucket ending at the rounded-through bound; the step
// itself is truncated to whole milliseconds, matching the volume request's
// Step field.
func volumeRangeGrid(start, end time.Time, step time.Duration) ([]int64, error) {
	from := start.UnixNano() / int64(time.Millisecond) * int64(time.Millisecond)
	through := end.UnixNano() / int64(time.Millisecond) * int64(time.Millisecond)
	if end.Nanosecond()%int(time.Millisecond) != 0 {
		through += int64(time.Millisecond)
	}
	if step <= 0 {
		return []int64{through}, nil
	}
	// The volume request carries Step as whole milliseconds
	// (codec.go Setting Step: req.Step.Milliseconds()), so a sub-millisecond
	// step becomes zero and produces one bucket rather than the metric-engine
	// sub-millisecond rejection.
	stepMillis := step.Milliseconds()
	if stepMillis <= 0 {
		return []int64{through}, nil
	}
	interval := stepMillis * int64(time.Millisecond)
	first := from - from%interval
	var grid []int64
	for at := first; at < through; {
		if len(grid) >= maxVolumeRangeBuckets {
			return nil, fmt.Errorf("%s..%s with step %s needs more than %d buckets",
				formatTime(start), formatTime(end), step, maxVolumeRangeBuckets)
		}
		nextEnd := at + interval
		// The final bucket is capped at through; the wrap check also covers a
		// step large enough to overflow the addition.
		final := nextEnd >= through || nextEnd < at
		if final {
			grid = append(grid, through)
			break
		}
		grid = append(grid, nextEnd-int64(time.Millisecond))
		at = nextEnd
	}
	return grid, nil
}

// normalizeBase validates a -left/-right server base URL. A path prefix is
// kept; a query or fragment is rejected because requests are appended to it.
func normalizeBase(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", errors.New("missing URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%q: missing host", raw)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("%q: query and fragment are not allowed", raw)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u.String(), nil
}
