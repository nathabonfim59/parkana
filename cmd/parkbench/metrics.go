// Parkana metric scraping for the query benchmark.
//
// Only the fixed-name, fixed-label counters below are parsed; every other
// exposition line is ignored, so unrelated metrics and future additions never
// break a measurement. Values are parsed as exact int64 counters: a fractional,
// negative, non-finite or out-of-range value is an error, and deltas are int64
// subtractions, so a counter above 2^53 never rounds into a fabricated delta.
//
// A required series missing from either the before or the after scrape is
// unavailable (null), never zero, and invalidates the sample: reporting zero
// would silently claim that no file was scanned or pruned. Optional cache
// diagnostics are reported as null when absent.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
)

// parkanaMetricSpec identifies one fixed counter series the report reads.
// required marks the series whose absence makes a Parkana measurement invalid.
type parkanaMetricSpec struct {
	name     string
	result   string
	field    string
	required bool
}

// parkanaSpecs returns the only series the report parses. The three
// files_total results and bytes_scanned are required for a measured Parkana
// run; the object/sidecar cache counters are optional diagnostics.
func parkanaSpecs() []parkanaMetricSpec {
	return []parkanaMetricSpec{
		{name: "parkana_query_files_total", result: "scanned", field: "files_scanned", required: true},
		{name: "parkana_query_files_total", result: "pruned_index", field: "files_pruned_index", required: true},
		{name: "parkana_query_files_total", result: "pruned_labels", field: "files_pruned_labels", required: true},
		{name: "parkana_query_bytes_scanned_total", field: "bytes_scanned", required: true},
		{name: "parkana_querier_cache_fetched_bytes_total", field: "cache_fetched_bytes"},
		{name: "parkana_querier_cache_downloads_total", result: "success", field: "cache_downloads_success"},
		{name: "parkana_querier_cache_downloads_total", result: "error", field: "cache_downloads_error"},
		{name: "parkana_querier_sidecar_cache_fetched_bytes_total", field: "sidecar_cache_fetched_bytes"},
	}
}

// knownMetricFamily reports whether one metric family is parsed.
func knownMetricFamily(name string) bool {
	for _, spec := range parkanaSpecs() {
		if spec.name == name {
			return true
		}
	}
	return false
}

// metricScrape is one parsed metrics endpoint response: the value of every
// known series, keyed by family and result label.
type metricScrape struct {
	values map[string]int64
}

// value returns the value of one known series.
func (s metricScrape) value(name, result string) (int64, bool) {
	value, ok := s.values[metricKey(name, result)]
	return value, ok
}

// metricKey identifies one series.
func metricKey(name, result string) string { return name + "\x00" + result }

// seriesLabel renders a series for error messages.
func seriesLabel(name, result string) string {
	if result == "" {
		return name
	}
	return name + `{result="` + result + `"}`
}

// scrapeMetrics fetches and parses the metrics endpoint.
func (r *queryRunner) scrapeMetrics(ctx context.Context) (metricScrape, error) {
	requestCtx := ctx
	if r.plan.timeout > 0 {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithTimeout(ctx, r.plan.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, r.plan.metricsURL.String(), nil)
	if err != nil {
		return metricScrape{}, fmt.Errorf("metrics request: %w", err)
	}
	req.Header.Set("Accept", "text/plain")
	resp, err := r.client.Do(req)
	if err != nil {
		return metricScrape{}, fmt.Errorf("metrics request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readBoundedBody(resp.Body, maxMetricsBodyBytes)
	if err != nil {
		return metricScrape{}, fmt.Errorf("metrics response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return metricScrape{}, fmt.Errorf("metrics endpoint returned %s: %s", resp.Status, bodySnippet(body))
	}
	return parseMetricsText(body)
}

// diffScrapes turns two scrapes into the delta of one query. A required series
// missing from either scrape, a counter that went backwards or a non-integral
// value yields an error as well as the null fields; the caller reports both.
func diffScrapes(before, after metricScrape) (*SampleMetrics, error) {
	metrics := &SampleMetrics{}
	var errs []error
	for _, spec := range parkanaSpecs() {
		label := seriesLabel(spec.name, spec.result)
		previous, beforeOK := before.value(spec.name, spec.result)
		current, afterOK := after.value(spec.name, spec.result)
		switch {
		case !beforeOK || !afterOK:
			metrics.MissingSeries = append(metrics.MissingSeries, label)
			if spec.required {
				errs = append(errs, fmt.Errorf("required metric series %s is missing from a scrape", label))
			}
			continue
		case current < previous:
			metrics.ResetSeries = append(metrics.ResetSeries, label)
			errs = append(errs, fmt.Errorf("counter %s went backwards: %d -> %d", label, previous, current))
			continue
		}
		setMetricField(metrics, spec.field, current-previous)
	}
	return metrics, errors.Join(errs...)
}

// setMetricField stores a delta in its report field.
func setMetricField(metrics *SampleMetrics, field string, value int64) {
	switch field {
	case "files_scanned":
		metrics.FilesScanned = new(value)
	case "files_pruned_index":
		metrics.FilesPrunedIndex = new(value)
	case "files_pruned_labels":
		metrics.FilesPrunedLabels = new(value)
	case "bytes_scanned":
		metrics.BytesScanned = new(value)
	case "cache_fetched_bytes":
		metrics.CacheFetchedBytes = new(value)
	case "cache_downloads_success":
		metrics.CacheDownloadsSuccess = new(value)
	case "cache_downloads_error":
		metrics.CacheDownloadsError = new(value)
	case "sidecar_cache_fetched_bytes":
		metrics.SidecarCacheFetchedBytes = new(value)
	}
}

// parseMetricsText parses a Prometheus text exposition response. Only the
// known families are parsed strictly; HELP, TYPE, comments and every unrelated
// metric are ignored.
func parseMetricsText(body []byte) (metricScrape, error) {
	scrape := metricScrape{values: make(map[string]int64, len(parkanaSpecs()))}
	for line := range strings.SplitSeq(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := metricFamilyOf(line)
		if !knownMetricFamily(name) {
			continue
		}
		labels, value, err := parsePrometheusSample(line)
		if err != nil {
			return metricScrape{}, fmt.Errorf("parse metrics line %q: %w", line, err)
		}
		key := metricKey(name, labels["result"])
		if _, duplicate := scrape.values[key]; duplicate {
			return metricScrape{}, fmt.Errorf("duplicate metric series %s", seriesLabel(name, labels["result"]))
		}
		scrape.values[key] = value
	}
	return scrape, nil
}

// metricFamilyOf returns the metric name at the start of one exposition line.
func metricFamilyOf(line string) string {
	end := strings.IndexAny(line, "{ \t")
	if end < 0 {
		return line
	}
	return line[:end]
}

// parsePrometheusSample parses one sample line into its labels and exact
// integer value. The optional trailing timestamp is ignored.
func parsePrometheusSample(line string) (map[string]string, int64, error) {
	labels := map[string]string{}
	rest := line
	if start := strings.IndexByte(line, '{'); start >= 0 {
		end := strings.LastIndexByte(line, '}')
		if end < start {
			return nil, 0, errors.New("unterminated label set")
		}
		parsed, err := parsePrometheusLabels(line[start+1 : end])
		if err != nil {
			return nil, 0, err
		}
		labels = parsed
		rest = line[end+1:]
	} else if space := strings.IndexAny(line, " \t"); space >= 0 {
		rest = line[space:]
	} else {
		return nil, 0, errors.New("sample has no value")
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return nil, 0, errors.New("sample has no value")
	}
	value, err := parseCounterValue(fields[0])
	if err != nil {
		return nil, 0, err
	}
	return labels, value, nil
}

// parseCounterValue parses one counter value exactly. Integer literals use
// strconv.ParseInt and every other decimal/exponent form goes through an exact
// rational, so no value is rounded through float64 before its integrality and
// int64-range checks. Prometheus scientific notation such as 1.234e+06 is
// accepted; fractional, negative and non-finite values are rejected.
func parseCounterValue(raw string) (int64, error) {
	literal := strings.TrimSpace(raw)
	switch literal {
	case "NaN", "nan", "+Inf", "-Inf", "Inf", "inf", "+inf", "-inf":
		return 0, fmt.Errorf("value %q is not finite", raw)
	}
	if isDecimalInteger(literal) {
		value, err := strconv.ParseInt(literal, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value %q is outside the int64 range", raw)
		}
		if value < 0 {
			return 0, fmt.Errorf("counter value %q is negative", raw)
		}
		return value, nil
	}
	if !isDecimalNumericLiteral(strings.TrimPrefix(literal, "+")) {
		return 0, fmt.Errorf("value %q is not a number", raw)
	}
	value, ok := new(big.Rat).SetString(strings.TrimPrefix(literal, "+"))
	if !ok {
		return 0, fmt.Errorf("value %q is not a number", raw)
	}
	if value.Sign() < 0 {
		return 0, fmt.Errorf("counter value %q is negative", raw)
	}
	if !value.IsInt() {
		return 0, fmt.Errorf("counter value %q is not an integer", raw)
	}
	if !value.Num().IsInt64() {
		return 0, fmt.Errorf("value %q is outside the int64 range", raw)
	}
	return value.Num().Int64(), nil
}

// parsePrometheusLabels parses the inside of a Prometheus label set, including
// the exposition escapes for backslash, quote and newline.
func parsePrometheusLabels(raw string) (map[string]string, error) {
	labels := make(map[string]string)
	for {
		raw = strings.TrimLeft(raw, " \t,")
		if raw == "" {
			return labels, nil
		}
		eq := strings.IndexByte(raw, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("label %q has no name or '='", raw)
		}
		name := strings.TrimSpace(raw[:eq])
		value, rest, err := parsePrometheusValue(raw[eq+1:])
		if err != nil {
			return nil, fmt.Errorf("label %s: %w", name, err)
		}
		labels[name] = value
		raw = rest
	}
}

// parsePrometheusValue parses one quoted label value and returns the remainder
// of the label set.
func parsePrometheusValue(raw string) (value, rest string, err error) {
	if raw == "" || raw[0] != '"' {
		return "", "", errors.New("unquoted value")
	}
	var b strings.Builder
	for i := 1; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			if i+1 >= len(raw) {
				return "", "", errors.New("dangling escape")
			}
			i++
			switch raw[i] {
			case 'n':
				b.WriteByte('\n')
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte(raw[i])
			}
		case '"':
			return b.String(), raw[i+1:], nil
		default:
			b.WriteByte(raw[i])
		}
	}
	return "", "", errors.New("unterminated value")
}
