package main

import (
	"cmp"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
)

// timestampPrecision controls optional timestamp normalization before
// comparison. Exact preserves every nanosecond; Microsecond truncates.
type timestampPrecision string

const (
	precisionExact       timestampPrecision = "exact"
	precisionMicrosecond timestampPrecision = "us"
)

func (p timestampPrecision) valid() bool {
	return p == precisionExact || p == precisionMicrosecond
}

// normalizeTimestamp applies the requested comparison precision. With the
// default exact policy the value is returned unchanged. Microsecond precision
// floors to the stored grid, including negative Unix-nanosecond times.
func (p timestampPrecision) normalizeTimestamp(ns int64) int64 {
	if p == precisionMicrosecond {
		remainder := ns % 1000
		if remainder < 0 {
			remainder += 1000
		}
		return ns - remainder
	}
	return ns
}

// normResult is a decoded query result shape ready for comparison.
type normResult struct {
	resultType string
	// stats is the raw data.stats member. Index volume requires a JSON object;
	// regular query stats remain ignored.
	stats   jsontext.Value
	streams []normStream
	series  []normSeries
	vector  []normVector
	scalar  *normPoint
	text    *normPoint
}

type normStream struct {
	labels  map[string]string
	entries []normEntry
}

type normEntry struct {
	ts         int64
	line       string
	structured map[string]string
	parsed     map[string]string
}

type normSeries struct {
	labels map[string]string
	points []normPoint
}

type normVector struct {
	labels map[string]string
	point  normPoint
}

type normPoint struct {
	// ts is the timestamp after the requested precision normalization.
	ts int64
	// rawTS is the timestamp exactly as decoded, before normalization. Volume
	// range grids and RIGHT expectations compare this value.
	rawTS int64
	// numeric records whether the timestamp arrived as a JSON number (decimal
	// Unix seconds) rather than a string. Index volume requires numeric
	// timestamps.
	numeric bool
	// tsOverflow marks a scalar timestamp accepted only under a lokiQuirk:
	// a JSON number outside the representable int64 nanosecond range. Such a
	// timestamp is never compared, but tsLexeme keeps its exact spelling for
	// the diagnostic note.
	tsOverflow bool
	tsLexeme   string
	value      string
}

// queryEnvelope is the Loki JSON envelope shared by query, query_range,
// labels, label_values and series. Stats is retained only so index volume can
// validate that the documented object is present; regular query stats stay
// implementation-specific and are otherwise ignored.
type queryEnvelope struct {
	Status string `json:"status"`
	Data   *struct {
		ResultType string         `json:"resultType"`
		Result     jsontext.Value `json:"result"`
		Stats      jsontext.Value `json:"stats"`
	} `json:"data"`
}

type streamJSON struct {
	Stream *map[string]string  `json:"stream"`
	Values *[][]jsontext.Value `json:"values"`
}

type matrixJSON struct {
	Metric *map[string]string  `json:"metric"`
	Values *[][]jsontext.Value `json:"values"`
}

type vectorJSON struct {
	Metric *map[string]string `json:"metric"`
	Value  []jsontext.Value   `json:"value"`
}

// decodeMode controls opt-in tolerance for decoded responses. The zero value is
// the strict default; a lokiQuirk entry sets exactly the tolerance it names.
type decodeMode struct {
	// allowScalarTimestampOverflow accepts a LEFT scalar timestamp as a JSON
	// number outside the int64 nanosecond range (lokiQuirk
	// scalar-timestamp). Values are still decoded and compared.
	allowScalarTimestampOverflow bool
}

// decodeQueryResponse decodes a query or query_range response. The response's
// data.stats member is deliberately not decoded: it is implementation-specific
// and is excluded from comparison.
func decodeQueryResponse(body []byte, precision timestampPrecision) (*normResult, error) {
	return decodeQueryResponseMode(body, precision, decodeMode{})
}

// decodeQueryResponseMode is decodeQueryResponse with an explicit tolerance for
// documented Loki quirks. RIGHT responses and entries without a quirk always use
// the strict zero mode.
func decodeQueryResponseMode(body []byte, precision timestampPrecision, mode decodeMode) (*normResult, error) {
	var env queryEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode query response: %w", err)
	}
	if err := checkStatus(env.Status); err != nil {
		return nil, err
	}
	if env.Data == nil {
		return nil, errors.New("query response has no data")
	}
	if env.Data.ResultType == "" {
		return nil, errors.New("query response has no data.resultType")
	}
	if env.Data.Result.Kind() != jsontext.KindBeginArray {
		return nil, errors.New("query response has no data.result array")
	}
	res := &normResult{resultType: env.Data.ResultType, stats: env.Data.Stats}
	switch env.Data.ResultType {
	case "streams":
		streams, err := decodeStreams(env.Data.Result, precision)
		if err != nil {
			return nil, err
		}
		res.streams = streams
	case "matrix":
		series, err := decodeMatrix(env.Data.Result, precision)
		if err != nil {
			return nil, err
		}
		res.series = series
	case "vector":
		vector, err := decodeVector(env.Data.Result, precision)
		if err != nil {
			return nil, err
		}
		res.vector = vector
	case "scalar", "string":
		point, err := decodePointArray(env.Data.Result, precision, env.Data.ResultType == "scalar", mode)
		if err != nil {
			return nil, err
		}
		if env.Data.ResultType == "scalar" {
			res.scalar = &point
		} else {
			res.text = &point
		}
	default:
		return nil, fmt.Errorf("unsupported resultType %q", env.Data.ResultType)
	}
	return res, nil
}

func decodeStreams(raw jsontext.Value, precision timestampPrecision) ([]normStream, error) {
	var items []streamJSON
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("decode streams result: %w", err)
	}
	streams := make([]normStream, 0, len(items))
	for i, item := range items {
		if item.Stream == nil {
			return nil, fmt.Errorf("stream %d is missing its stream labels", i)
		}
		if item.Values == nil {
			return nil, fmt.Errorf("stream %d is missing values", i)
		}
		stream := normStream{labels: maps.Clone(*item.Stream)}
		for j, tuple := range *item.Values {
			entry, err := parseLogEntry(tuple, precision)
			if err != nil {
				return nil, fmt.Errorf("stream %d value %d: %w", i, j, err)
			}
			stream.entries = append(stream.entries, entry)
		}
		slices.SortFunc(stream.entries, compareEntries)
		streams = append(streams, stream)
	}
	slices.SortFunc(streams, func(a, b normStream) int {
		return strings.Compare(canonicalLabels(a.labels), canonicalLabels(b.labels))
	})
	return streams, nil
}

// parseLogEntry decodes Loki's value tuple: [ns, line] or, with the
// categorize-labels encoding flag, [ns, line, metadata]. The metadata object
// holds structuredMetadata and parsed sub-objects; a flat string-valued object
// is accepted as structured metadata, matching Loki's own Entry decoder.
func parseLogEntry(tuple []jsontext.Value, precision timestampPrecision) (normEntry, error) {
	if len(tuple) < 2 || len(tuple) > 3 {
		return normEntry{}, fmt.Errorf("entry tuple has %d elements, want 2 or 3", len(tuple))
	}
	if tuple[0].Kind() != jsontext.KindString {
		return normEntry{}, errors.New("entry timestamp must be a string")
	}
	var tsString string
	if err := json.Unmarshal(tuple[0], &tsString); err != nil {
		return normEntry{}, err
	}
	ts, err := parseTimestampString(tsString)
	if err != nil {
		return normEntry{}, err
	}
	if tuple[1].Kind() != jsontext.KindString {
		return normEntry{}, errors.New("entry line must be a string")
	}
	var line string
	if err := json.Unmarshal(tuple[1], &line); err != nil {
		return normEntry{}, err
	}
	entry := normEntry{ts: precision.normalizeTimestamp(ts), line: line}
	if len(tuple) == 3 {
		entry.structured, entry.parsed, err = parseEntryMetadata(tuple[2])
		if err != nil {
			return normEntry{}, err
		}
	}
	return entry, nil
}

// parseEntryMetadata decodes the third tuple element written by
// pkg/util/marshal.encodeStream when categorize-labels is requested.
func parseEntryMetadata(raw jsontext.Value) (structured, parsed map[string]string, err error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return nil, nil, fmt.Errorf("entry metadata must be an object, got %s", raw.Kind())
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("decode entry metadata: %w", err)
	}
	for key, value := range fields {
		switch {
		case key == "structuredMetadata" || key == "parsed":
			if value.Kind() != jsontext.KindBeginObject {
				return nil, nil, fmt.Errorf("entry metadata %q must be an object", key)
			}
			labels, err := parseLabelObject(value)
			if err != nil {
				return nil, nil, fmt.Errorf("entry metadata %q: %w", key, err)
			}
			if key == "structuredMetadata" {
				structured = labels
			} else {
				parsed = labels
			}
		case value.Kind() == jsontext.KindString || value.Kind() == jsontext.KindNumber:
			if structured == nil {
				structured = make(map[string]string)
			}
			label, err := labelValue(value)
			if err != nil {
				return nil, nil, fmt.Errorf("entry metadata %q: %w", key, err)
			}
			structured[key] = label
		default:
			return nil, nil, fmt.Errorf("entry metadata %q must be a string, number or object", key)
		}
	}
	return structured, parsed, nil
}

// parseLabelObject decodes a label object whose values are strings or numbers.
func parseLabelObject(raw jsontext.Value) (map[string]string, error) {
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	labels := make(map[string]string, len(fields))
	for key, value := range fields {
		label, err := labelValue(value)
		if err != nil {
			return nil, fmt.Errorf("label %q: %w", key, err)
		}
		labels[key] = label
	}
	return labels, nil
}

// labelValue reads a label value encoded as a string or number.
func labelValue(raw jsontext.Value) (string, error) {
	switch raw.Kind() {
	case jsontext.KindString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	case jsontext.KindNumber:
		return strings.TrimSpace(string(raw)), nil
	default:
		return "", fmt.Errorf("value must be a string or number, got %s", raw.Kind())
	}
}

func decodeMatrix(raw jsontext.Value, precision timestampPrecision) ([]normSeries, error) {
	var items []matrixJSON
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("decode matrix result: %w", err)
	}
	series := make([]normSeries, 0, len(items))
	for i, item := range items {
		if item.Metric == nil {
			return nil, fmt.Errorf("matrix series %d is missing its metric object", i)
		}
		if item.Values == nil {
			return nil, fmt.Errorf("matrix series %d is missing its values array", i)
		}
		s := normSeries{labels: maps.Clone(*item.Metric)}
		for j, pair := range *item.Values {
			if len(pair) != 2 {
				return nil, fmt.Errorf("matrix series %d value %d: want [timestamp, value], got %d elements", i, j, len(pair))
			}
			point, err := parsePoint(pair[0], pair[1], precision, true, decodeMode{})
			if err != nil {
				return nil, fmt.Errorf("matrix series %d value %d: %w", i, j, err)
			}
			s.points = append(s.points, point)
		}
		slices.SortFunc(s.points, comparePoints)
		series = append(series, s)
	}
	slices.SortFunc(series, func(a, b normSeries) int {
		return strings.Compare(canonicalLabels(a.labels), canonicalLabels(b.labels))
	})
	return series, nil
}

func decodeVector(raw jsontext.Value, precision timestampPrecision) ([]normVector, error) {
	var items []vectorJSON
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("decode vector result: %w", err)
	}
	vector := make([]normVector, 0, len(items))
	for i, item := range items {
		if item.Metric == nil {
			return nil, fmt.Errorf("vector sample %d is missing its metric object", i)
		}
		if len(item.Value) != 2 {
			return nil, fmt.Errorf("vector sample %d: want [timestamp, value], got %d elements", i, len(item.Value))
		}
		point, err := parsePoint(item.Value[0], item.Value[1], precision, true, decodeMode{})
		if err != nil {
			return nil, fmt.Errorf("vector sample %d: %w", i, err)
		}
		vector = append(vector, normVector{labels: maps.Clone(*item.Metric), point: point})
	}
	slices.SortFunc(vector, func(a, b normVector) int {
		return cmp.Or(
			strings.Compare(canonicalLabels(a.labels), canonicalLabels(b.labels)),
			comparePoints(a.point, b.point),
		)
	})
	return vector, nil
}

func decodePointArray(raw jsontext.Value, precision timestampPrecision, numeric bool, mode decodeMode) (normPoint, error) {
	var pair []jsontext.Value
	if err := json.Unmarshal(raw, &pair); err != nil {
		return normPoint{}, fmt.Errorf("decode point result: %w", err)
	}
	if len(pair) != 2 {
		return normPoint{}, fmt.Errorf("point result must be [timestamp, value], got %d elements", len(pair))
	}
	return parsePoint(pair[0], pair[1], precision, numeric, mode)
}

// checkStatus rejects Loki error envelopes instead of comparing them as data.
func checkStatus(status string) error {
	switch status {
	case "success":
		return nil
	case "":
		return errors.New("response has no status")
	default:
		return fmt.Errorf("response status is %q", status)
	}
}

// parseTimestamp reads a metric timestamp: an integer nanosecond string, a
// fractional-second string, or a JSON number of seconds. The boolean reports
// whether the value was a JSON number.
func parseTimestamp(v jsontext.Value) (int64, bool, error) {
	switch v.Kind() {
	case jsontext.KindString:
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return 0, false, err
		}
		ns, err := parseTimestampString(s)
		return ns, false, err
	case jsontext.KindNumber:
		ns, err := parseDecimalSeconds(strings.TrimSpace(string(v)))
		return ns, true, err
	default:
		return 0, false, fmt.Errorf("want a timestamp string or number, got %s", v.Kind())
	}
}

// parseTimestampString reads the integer Unix-nanosecond strings Loki uses for
// log entry timestamps.
func parseTimestampString(s string) (int64, error) {
	ns, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("timestamp %q is not integer Unix nanoseconds", s)
	}
	return ns, nil
}

// parseDecimalSeconds reads a JSON numeric timestamp, which Loki encodes as
// decimal seconds. The lexeme is parsed exactly, not through float64, so
// nanosecond precision survives.
func parseDecimalSeconds(s string) (int64, error) {
	seconds, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0, fmt.Errorf("timestamp %q is not a decimal number of seconds", s)
	}
	seconds.Mul(seconds, big.NewRat(1_000_000_000, 1))
	ns, ok := ratToInt64(seconds)
	if !ok {
		return 0, fmt.Errorf("timestamp %q is out of range", s)
	}
	return ns, nil
}

// ratToInt64 converts a rational number of nanoseconds to int64, rounding half
// away from zero. Timestamps are parsed as exact decimals rather than float64
// so nanosecond precision survives JSON numeric timestamps.
func ratToInt64(r *big.Rat) (int64, bool) {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(r.Num(), r.Denom(), remainder)
	if remainder.Sign() != 0 {
		twiceRemainder := new(big.Int).Abs(remainder)
		twiceRemainder.Lsh(twiceRemainder, 1)
		if twiceRemainder.Cmp(r.Denom()) >= 0 {
			if r.Sign() < 0 {
				quotient.Sub(quotient, big.NewInt(1))
			} else {
				quotient.Add(quotient, big.NewInt(1))
			}
		}
	}
	if !quotient.IsInt64() {
		return 0, false
	}
	return quotient.Int64(), true
}

func parsePoint(tsRaw, valueRaw jsontext.Value, precision timestampPrecision, numeric bool, mode decodeMode) (normPoint, error) {
	ts, tsNumeric, overflow, err := parseTimestampMode(tsRaw, numeric && mode.allowScalarTimestampOverflow)
	if err != nil {
		return normPoint{}, err
	}
	value, err := sampleValue(valueRaw)
	if err != nil {
		return normPoint{}, err
	}
	if numeric {
		if _, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err != nil {
			return normPoint{}, fmt.Errorf("sample value %q is not numeric", value)
		}
	}
	if overflow {
		return normPoint{numeric: tsNumeric, tsOverflow: true, tsLexeme: strings.TrimSpace(string(tsRaw)), value: value}, nil
	}
	return normPoint{ts: precision.normalizeTimestamp(ts), rawTS: ts, numeric: tsNumeric, value: value}, nil
}

// parseTimestampMode reads a metric timestamp. With allowOverflow set, a JSON
// number outside the int64 nanosecond range is accepted as an overflow marker
// instead of an error; every other input follows parseTimestamp exactly.
func parseTimestampMode(v jsontext.Value, allowOverflow bool) (ns int64, numeric, overflow bool, err error) {
	if allowOverflow && v.Kind() == jsontext.KindNumber {
		lexeme := strings.TrimSpace(string(v))
		if !validDecimalNumber(lexeme) {
			return 0, true, false, fmt.Errorf("timestamp %q is not a decimal number of seconds", lexeme)
		}
		parsed, parseErr := parseDecimalSeconds(lexeme)
		if parseErr != nil {
			return 0, true, true, nil
		}
		return parsed, true, false, nil
	}
	ns, numeric, err = parseTimestamp(v)
	return ns, numeric, false, err
}

// sampleValue reads a metric sample value, which Loki encodes as a string
// (including "NaN" and "+Inf") but some implementations send as a number.
func sampleValue(v jsontext.Value) (string, error) {
	switch v.Kind() {
	case jsontext.KindString:
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return "", err
		}
		return s, nil
	case jsontext.KindNumber:
		return strings.TrimSpace(string(v)), nil
	default:
		return "", fmt.Errorf("sample value must be a string or number, got %s", v.Kind())
	}
}

// statsResult is a validated index/stats response. streams is the only field
// compared numerically; entries, bytes and chunks are reported as estimates.
type statsResult struct {
	streams int64
	entries int64
	bytes   int64
	chunks  int64
}

// statsEnvelope is the documented Loki index/stats response shape.
type statsEnvelope struct {
	Status  string `json:"status,omitempty"`
	Streams *int64 `json:"streams"`
	Entries *int64 `json:"entries"`
	Chunks  *int64 `json:"chunks"`
	Bytes   *int64 `json:"bytes"`
}

// decodeStatsResponse decodes index/stats. Every documented field is required
// and must be a non-negative integer so a malformed or partial response cannot
// compare equal.
func decodeStatsResponse(body []byte) (statsResult, error) {
	var env statsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return statsResult{}, fmt.Errorf("decode stats response: %w", err)
	}
	if env.Status != "" && env.Status != "success" {
		return statsResult{}, fmt.Errorf("response status is %q", env.Status)
	}
	fields := []struct {
		name  string
		value *int64
	}{
		{"streams", env.Streams},
		{"chunks", env.Chunks},
		{"entries", env.Entries},
		{"bytes", env.Bytes},
	}
	for _, field := range fields {
		if field.value == nil {
			return statsResult{}, fmt.Errorf("stats response is missing %s", field.name)
		}
		if *field.value < 0 {
			return statsResult{}, fmt.Errorf("stats response %s must not be negative, got %d", field.name, *field.value)
		}
	}
	return statsResult{
		streams: *env.Streams,
		entries: *env.Entries,
		bytes:   *env.Bytes,
		chunks:  *env.Chunks,
	}, nil
}

// decodeLabelsResponse decodes labels and label_values, returning the label
// set sorted with duplicates removed.
func decodeLabelsResponse(body []byte) ([]string, error) {
	var env struct {
		Status string         `json:"status"`
		Data   jsontext.Value `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode labels response: %w", err)
	}
	if err := checkStatus(env.Status); err != nil {
		return nil, err
	}
	if env.Data.Kind() != jsontext.KindBeginArray {
		return nil, errors.New("labels response is missing a data array")
	}
	var labels []string
	if err := json.Unmarshal(env.Data, &labels); err != nil {
		return nil, fmt.Errorf("decode labels data: %w", err)
	}
	return sortedSet(labels), nil
}

// decodeSeriesResponse decodes series, returning label sets sorted by canonical
// form with duplicate label sets removed.
func decodeSeriesResponse(body []byte) ([]map[string]string, error) {
	var env struct {
		Status string         `json:"status"`
		Data   jsontext.Value `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode series response: %w", err)
	}
	if err := checkStatus(env.Status); err != nil {
		return nil, err
	}
	if env.Data.Kind() != jsontext.KindBeginArray {
		return nil, errors.New("series response is missing a data array")
	}
	var items []map[string]string
	if err := json.Unmarshal(env.Data, &items); err != nil {
		return nil, fmt.Errorf("decode series data: %w", err)
	}
	labels := make([]map[string]string, 0, len(items))
	for i, item := range items {
		if item == nil {
			return nil, fmt.Errorf("series data %d is null", i)
		}
		labels = append(labels, maps.Clone(item))
	}
	slices.SortFunc(labels, func(a, b map[string]string) int {
		return strings.Compare(canonicalLabels(a), canonicalLabels(b))
	})
	return slices.CompactFunc(labels, func(a, b map[string]string) bool {
		return canonicalLabels(a) == canonicalLabels(b)
	}), nil
}

func sortedSet(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return slices.Compact(out)
}

func compareEntries(a, b normEntry) int {
	return cmp.Or(cmp.Compare(a.ts, b.ts), strings.Compare(a.line, b.line),
		strings.Compare(entryMetadataKey(a), entryMetadataKey(b)))
}

func comparePoints(a, b normPoint) int {
	return cmp.Or(cmp.Compare(a.ts, b.ts), strings.Compare(a.value, b.value))
}

// canonicalLabels renders a label map unambiguously for sorting and grouping.
func canonicalLabels(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%d:%s%d:%s", len(k), k, len(labels[k]), labels[k])
	}
	return b.String()
}

// formatLabels renders labels for human-readable difference reports.
func formatLabels(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	if len(keys) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

func entryMetadataKey(e normEntry) string {
	return canonicalLabels(e.structured) + "|" + canonicalLabels(e.parsed)
}

// entryKey identifies an entry for multiset comparison, preserving duplicate
// entries and the full line. Diagnostics use entryDisplay, which truncates.
func entryKey(e normEntry) string {
	return fmt.Sprintf("ts=%d line=%q structured=%s parsed=%s", e.ts, e.line,
		formatLabels(e.structured), formatLabels(e.parsed))
}

// entryDisplay renders an entry for a difference report, truncating only the
// rendered line, never the comparison identity.
func entryDisplay(e normEntry) string {
	return fmt.Sprintf("ts=%d line=%q structured=%s parsed=%s", e.ts, short(e.line),
		formatLabels(e.structured), formatLabels(e.parsed))
}

func short(s string) string {
	const limit = 160
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
