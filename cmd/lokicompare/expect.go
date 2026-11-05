package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"mime"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// expectation holds optional exact assertions for the comparison. Numeric
// stats and series expectations never consult the LEFT response and never
// use the float tolerances: values compare exactly. A status expectation
// instead asserts that both servers return the same expected HTTP error.
type expectation struct {
	stats     *statsExpectation
	series    []seriesExpectation
	seriesSet bool
	status    *int64
}

// statsExpectation asserts exact index/stats counts. A field is present only
// when the corpus author asked for it.
type statsExpectation struct {
	streams *int64
	entries *int64
	bytes   *int64
}

// seriesExpectation asserts one volume series. Instant entries set value;
// volume_range entries set points.
type seriesExpectation struct {
	metric map[string]string
	value  *big.Rat
	points []expectedPoint
}

// expectedPoint is one exact volume_range sample: numeric Unix seconds and a
// sample value.
type expectedPoint struct {
	ts    int64
	value *big.Rat
}

// parseExpectation validates the optional expect object. Unknown, null and
// empty definitions are rejected here, before any request is made.
func parseExpectation(raw jsontext.Value) (*expectation, error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return nil, fmt.Errorf("must be an object, got %s", raw.Kind())
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	exp := &expectation{}
	for key, value := range fields {
		switch key {
		case "stats":
			if value.Kind() == jsontext.KindNull {
				return nil, errors.New("stats must not be null")
			}
			stats, err := parseStatsExpectation(value)
			if err != nil {
				return nil, err
			}
			exp.stats = stats
		case "series":
			if value.Kind() == jsontext.KindNull {
				return nil, errors.New("series must not be null")
			}
			series, err := parseSeriesExpectations(value)
			if err != nil {
				return nil, err
			}
			exp.series = series
			exp.seriesSet = true
		case "status":
			if value.Kind() == jsontext.KindNull {
				return nil, errors.New("status must not be null")
			}
			status, err := parseStatusExpectation(value)
			if err != nil {
				return nil, err
			}
			exp.status = &status
		default:
			return nil, fmt.Errorf("unknown field %q", key)
		}
	}
	if exp.status != nil && (exp.stats != nil || exp.seriesSet) {
		return nil, errors.New("status must not be combined with stats or series")
	}
	if exp.stats == nil && !exp.seriesSet && exp.status == nil {
		return nil, errors.New("must define stats, a series array or status")
	}
	return exp, nil
}

// parseStatusExpectation reads the expected HTTP error status: a JSON integer
// in [400, 599]. Unlike stats counts, decimal strings are not accepted.
func parseStatusExpectation(raw jsontext.Value) (int64, error) {
	if raw.Kind() != jsontext.KindNumber {
		return 0, fmt.Errorf("status must be a JSON integer, got %s", raw.Kind())
	}
	lexeme := strings.TrimSpace(string(raw))
	if !validDecimalNumber(lexeme) {
		return 0, fmt.Errorf("status %q is not a decimal number", lexeme)
	}
	number, ok := new(big.Rat).SetString(lexeme)
	if !ok || !number.IsInt() || !number.Num().IsInt64() {
		return 0, fmt.Errorf("status %q must be an integer", lexeme)
	}
	code := number.Num().Int64()
	if code < 400 || code > 599 {
		return 0, fmt.Errorf("status %d is outside [400, 599]", code)
	}
	return code, nil
}

// parseStatsExpectation reads a non-empty subset of streams, entries and bytes.
func parseStatsExpectation(raw jsontext.Value) (*statsExpectation, error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return nil, fmt.Errorf("stats must be an object, got %s", raw.Kind())
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("stats: decode: %w", err)
	}
	stats := &statsExpectation{}
	for key, value := range fields {
		var target **int64
		switch key {
		case "streams":
			target = &stats.streams
		case "entries":
			target = &stats.entries
		case "bytes":
			target = &stats.bytes
		default:
			return nil, fmt.Errorf("stats: unknown field %q", key)
		}
		count, err := parseNonNegativeInteger(value)
		if err != nil {
			return nil, fmt.Errorf("stats.%s: %w", key, err)
		}
		*target = &count
	}
	if stats.streams == nil && stats.entries == nil && stats.bytes == nil {
		return nil, errors.New("stats: needs at least one of streams, entries, bytes")
	}
	return stats, nil
}

// parseSeriesExpectations reads the volume series assertions and rejects
// duplicate metric label sets.
func parseSeriesExpectations(raw jsontext.Value) ([]seriesExpectation, error) {
	if raw.Kind() != jsontext.KindBeginArray {
		return nil, fmt.Errorf("series must be an array, got %s", raw.Kind())
	}
	var items []jsontext.Value
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("series: decode: %w", err)
	}
	series := make([]seriesExpectation, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for i, item := range items {
		entry, err := parseSeriesExpectation(item)
		if err != nil {
			return nil, fmt.Errorf("series[%d]: %w", i, err)
		}
		key := canonicalLabels(entry.metric)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("series[%d]: duplicate metric %s", i, formatLabels(entry.metric))
		}
		seen[key] = struct{}{}
		series = append(series, entry)
	}
	return series, nil
}

// parseSeriesExpectation reads one series object. Exactly one of value and
// values must be present; the entry type decides which one is legal.
func parseSeriesExpectation(raw jsontext.Value) (seriesExpectation, error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return seriesExpectation{}, fmt.Errorf("must be an object, got %s", raw.Kind())
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return seriesExpectation{}, fmt.Errorf("decode: %w", err)
	}
	entry := seriesExpectation{}
	hasValue, hasValues := false, false
	for key, value := range fields {
		switch key {
		case "metric":
			if value.Kind() != jsontext.KindBeginObject {
				return seriesExpectation{}, fmt.Errorf("metric must be an object, got %s", value.Kind())
			}
			metric, err := parseLabelObject(value)
			if err != nil {
				return seriesExpectation{}, fmt.Errorf("metric: %w", err)
			}
			entry.metric = metric
		case "value":
			if hasValues {
				return seriesExpectation{}, errors.New("must not set both value and values")
			}
			hasValue = true
			number, err := parseExactNumber(value)
			if err != nil {
				return seriesExpectation{}, fmt.Errorf("value: %w", err)
			}
			entry.value = number
		case "values":
			if hasValue {
				return seriesExpectation{}, errors.New("must not set both value and values")
			}
			hasValues = true
			points, err := parseExpectedPoints(value)
			if err != nil {
				return seriesExpectation{}, fmt.Errorf("values: %w", err)
			}
			entry.points = points
		default:
			return seriesExpectation{}, fmt.Errorf("unknown field %q", key)
		}
	}
	if entry.metric == nil {
		return seriesExpectation{}, errors.New("missing metric")
	}
	if !hasValue && !hasValues {
		return seriesExpectation{}, errors.New("needs value (instant) or values (range)")
	}
	return entry, nil
}

// parseExpectedPoints reads exact [numeric-seconds, value] tuples.
func parseExpectedPoints(raw jsontext.Value) ([]expectedPoint, error) {
	if raw.Kind() != jsontext.KindBeginArray {
		return nil, fmt.Errorf("must be an array, got %s", raw.Kind())
	}
	var tuples [][]jsontext.Value
	if err := json.Unmarshal(raw, &tuples); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(tuples) == 0 {
		return nil, errors.New("must not be empty")
	}
	points := make([]expectedPoint, 0, len(tuples))
	seen := make(map[int64]struct{}, len(tuples))
	for i, tuple := range tuples {
		if len(tuple) != 2 {
			return nil, fmt.Errorf("sample %d must be [numeric seconds, value], got %d elements", i, len(tuple))
		}
		if tuple[0].Kind() != jsontext.KindNumber {
			return nil, fmt.Errorf("sample %d timestamp must be numeric Unix seconds, got %s", i, tuple[0].Kind())
		}
		ts, err := parseDecimalSeconds(strings.TrimSpace(string(tuple[0])))
		if err != nil {
			return nil, fmt.Errorf("sample %d: %w", i, err)
		}
		if _, ok := seen[ts]; ok {
			return nil, fmt.Errorf("sample %d: duplicate timestamp %s", i, timeString(ts))
		}
		seen[ts] = struct{}{}
		value, err := parseExactNumber(tuple[1])
		if err != nil {
			return nil, fmt.Errorf("sample %d value: %w", i, err)
		}
		points = append(points, expectedPoint{ts: ts, value: value})
	}
	return points, nil
}

// parseNonNegativeInteger accepts a JSON integer or a decimal string holding
// one. Non-integer and negative values are rejected.
func parseNonNegativeInteger(raw jsontext.Value) (int64, error) {
	lexeme, err := numericLexeme(raw)
	if err != nil {
		return 0, err
	}
	if !validDecimalNumber(lexeme) {
		return 0, fmt.Errorf("%q is not a decimal number", lexeme)
	}
	number, ok := new(big.Rat).SetString(lexeme)
	if !ok {
		return 0, fmt.Errorf("%q is not a number", lexeme)
	}
	if number.Sign() < 0 {
		return 0, errors.New("must not be negative")
	}
	if !number.IsInt() {
		return 0, errors.New("must be an integer")
	}
	if !number.Num().IsInt64() {
		return 0, errors.New("is out of range")
	}
	return number.Num().Int64(), nil
}

// parseExactNumber accepts a JSON number or a non-empty decimal string and
// parses it as an exact rational. NaN and infinities are rejected because
// expectations must be concrete values.
func parseExactNumber(raw jsontext.Value) (*big.Rat, error) {
	lexeme, err := numericLexeme(raw)
	if err != nil {
		return nil, err
	}
	if !validDecimalNumber(lexeme) {
		return nil, fmt.Errorf("%q is not a decimal number", lexeme)
	}
	number, ok := new(big.Rat).SetString(lexeme)
	if !ok {
		return nil, fmt.Errorf("%q is not a finite number", lexeme)
	}
	return number, nil
}

// numericLexeme reads a JSON number or string lexeme. Null, booleans, objects
// and arrays are rejected.
func numericLexeme(raw jsontext.Value) (string, error) {
	switch raw.Kind() {
	case jsontext.KindNumber:
		return strings.TrimSpace(string(raw)), nil
	case jsontext.KindString:
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return "", err
		}
		if text == "" {
			return "", errors.New("must not be empty")
		}
		return text, nil
	default:
		return "", fmt.Errorf("must be a number or decimal string, got %s", raw.Kind())
	}
}

// validDecimalNumber reports whether s is a JSON-style decimal number: an
// optional minus, an integer part, an optional fraction and an optional
// exponent. This rejects the other spellings big.Rat accepts, such as "1/1",
// "0x1" and underscores, before any request runs.
func validDecimalNumber(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[i] == '-' {
		i++
		if i == len(s) {
			return false
		}
	}
	switch {
	case s[i] == '0':
		i++
	case s[i] >= '1' && s[i] <= '9':
		for i < len(s) && isDecimalDigit(s[i]) {
			i++
		}
	default:
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		if i == len(s) || !isDecimalDigit(s[i]) {
			return false
		}
		for i < len(s) && isDecimalDigit(s[i]) {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if i == len(s) || !isDecimalDigit(s[i]) {
			return false
		}
		for i < len(s) && isDecimalDigit(s[i]) {
			i++
		}
	}
	return i == len(s)
}

func isDecimalDigit(c byte) bool { return c >= '0' && c <= '9' }

// validateExpectation checks that the expectation matches the corpus entry's
// endpoint before any request runs.
func validateExpectation(kind queryType, exp *expectation) error {
	if exp == nil {
		return nil
	}
	// A status expectation applies to every endpoint type.
	if exp.status != nil {
		return nil
	}
	switch kind {
	case queryStats:
		if exp.stats == nil {
			return errors.New("stats entry expect requires stats")
		}
		if exp.seriesSet {
			return errors.New("stats entry expect must not define series")
		}
	case queryVolume:
		if exp.stats != nil {
			return errors.New("volume entry expect must not define stats")
		}
		if !exp.seriesSet {
			return errors.New("volume entry expect requires series")
		}
		for i, series := range exp.series {
			if series.value == nil {
				return fmt.Errorf("series[%d]: volume entry expects value, not values", i)
			}
		}
	case queryVolumeRange:
		if exp.stats != nil {
			return errors.New("volume_range entry expect must not define stats")
		}
		if !exp.seriesSet {
			return errors.New("volume_range entry expect requires series")
		}
		for i, series := range exp.series {
			if len(series.points) == 0 {
				return fmt.Errorf("series[%d]: volume_range entry expects values, not value", i)
			}
		}
	default:
		return fmt.Errorf("expect is only valid for stats, volume and volume_range entries, not %s", kind)
	}
	return nil
}

// checkStatusExpectation asserts the expected HTTP error status on both
// responses. Each side must return exactly the expected code with a non-empty
// text/plain UTF-8 body; the message wording is implementation specific and
// is never compared.
func checkStatusExpectation(exp *expectation, left, right rawResponse) []string {
	var diffs []string
	want := int(*exp.status)
	for _, side := range []struct {
		name string
		resp rawResponse
	}{{"left", left}, {"right", right}} {
		if side.resp.status != want {
			diffs = append(diffs, fmt.Sprintf("expect status %d: %s HTTP %d", want, side.name, side.resp.status))
			continue
		}
		if err := checkErrorBody(side.resp); err != nil {
			diffs = append(diffs, fmt.Sprintf("expect status %d: %s body: %v", want, side.name, err))
		}
	}
	return diffs
}

// checkErrorBody validates an expected-error response body: a non-empty UTF-8
// text/plain message. Content-Type parameters such as charset are accepted;
// a missing, unparseable or non-plain media type is not. Control characters
// other than tab, carriage return and line feed mark binary content.
func checkErrorBody(resp rawResponse) error {
	mediaType, _, err := mime.ParseMediaType(resp.contentType)
	if err != nil {
		return fmt.Errorf("Content-Type %q is not a valid media type", resp.contentType)
	}
	if mediaType != "text/plain" {
		return fmt.Errorf("Content-Type %q is not text/plain", resp.contentType)
	}
	if !utf8.Valid(resp.body) {
		return errors.New("is not valid UTF-8")
	}
	if strings.TrimSpace(string(resp.body)) == "" {
		return errors.New("is empty")
	}
	for _, r := range string(resp.body) {
		if unicode.IsControl(r) && r != '\t' && r != '\r' && r != '\n' {
			return errors.New("contains control characters")
		}
	}
	return nil
}

// checkStatsExpectation asserts the requested exact RIGHT stats counts.
func checkStatsExpectation(exp *expectation, right statsResult) []string {
	if exp == nil || exp.stats == nil {
		return nil
	}
	var diffs []string
	check := func(name string, want *int64, got int64) {
		if want != nil && *want != got {
			diffs = append(diffs, fmt.Sprintf("expect stats %s: expected=%d right=%d", name, *want, got))
		}
	}
	check("streams", exp.stats.streams, right.streams)
	check("entries", exp.stats.entries, right.entries)
	check("bytes", exp.stats.bytes, right.bytes)
	return diffs
}

// checkVolumeExpectation applies the RIGHT-only volume expectations. It never
// consults the LEFT response or the float tolerances. A volume_range response
// may be a one-bucket vector or a multi-bucket matrix, so both flow through the
// same exact series comparison.
func checkVolumeExpectation(entry corpusEntry, right *normResult) []string {
	if entry.Expect == nil {
		return nil
	}
	if entry.Type == queryVolume {
		return checkVolumeVectorExpectation(entry.Expect.series, right.vector)
	}
	return checkVolumeMatrixExpectation(entry.Expect.series, volumeRangeSeries(right))
}

// checkVolumeVectorExpectation requires the expected and RIGHT series sets to
// match exactly and compares each value as an exact rational.
func checkVolumeVectorExpectation(expected []seriesExpectation, right []normVector) []string {
	var diffs []string
	rightByKey := make(map[string]normVector, len(right))
	for _, sample := range right {
		rightByKey[canonicalLabels(sample.labels)] = sample
	}
	expectedKeys := make(map[string]struct{}, len(expected))
	for _, want := range expected {
		key := canonicalLabels(want.metric)
		expectedKeys[key] = struct{}{}
		got, ok := rightByKey[key]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("expect series %s missing on right", formatLabels(want.metric)))
			continue
		}
		if !exactValueMatches(got.point.value, want.value) {
			diffs = append(diffs, fmt.Sprintf("expect series %s value: expected=%s right=%q",
				formatLabels(want.metric), want.value.RatString(), got.point.value))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(rightByKey)) {
		if _, ok := expectedKeys[key]; !ok {
			diffs = append(diffs, fmt.Sprintf("expect: right series %s was not expected", formatLabels(rightByKey[key].labels)))
		}
	}
	return diffs
}

// checkVolumeMatrixExpectation requires exact series membership and exact
// timestamp/value tuples on the RIGHT response.
func checkVolumeMatrixExpectation(expected []seriesExpectation, right []normSeries) []string {
	var diffs []string
	rightByKey := make(map[string]normSeries, len(right))
	for _, series := range right {
		rightByKey[canonicalLabels(series.labels)] = series
	}
	expectedKeys := make(map[string]struct{}, len(expected))
	for _, want := range expected {
		key := canonicalLabels(want.metric)
		expectedKeys[key] = struct{}{}
		got, ok := rightByKey[key]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("expect series %s missing on right", formatLabels(want.metric)))
			continue
		}
		diffs = append(diffs, compareExpectedPoints(want, got)...)
	}
	for _, key := range slices.Sorted(maps.Keys(rightByKey)) {
		if _, ok := expectedKeys[key]; !ok {
			diffs = append(diffs, fmt.Sprintf("expect: right series %s was not expected", formatLabels(rightByKey[key].labels)))
		}
	}
	return diffs
}

// compareExpectedPoints compares one expected series with the RIGHT series,
// reporting missing, extra and mismatched exact tuples.
func compareExpectedPoints(want seriesExpectation, got normSeries) []string {
	var diffs []string
	gotByTS := make(map[int64]string, len(got.points))
	for _, point := range got.points {
		gotByTS[point.rawTS] = point.value
	}
	wantTS := make(map[int64]struct{}, len(want.points))
	for _, point := range want.points {
		wantTS[point.ts] = struct{}{}
		gotValue, ok := gotByTS[point.ts]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("expect series %s timestamp %s missing on right",
				formatLabels(want.metric), timeString(point.ts)))
			continue
		}
		if !exactValueMatches(gotValue, point.value) {
			diffs = append(diffs, fmt.Sprintf("expect series %s timestamp %s value: expected=%s right=%q",
				formatLabels(want.metric), timeString(point.ts), point.value.RatString(), gotValue))
		}
	}
	for _, ts := range slices.Sorted(maps.Keys(gotByTS)) {
		if _, ok := wantTS[ts]; !ok {
			diffs = append(diffs, fmt.Sprintf("expect series %s: right timestamp %s was not expected",
				formatLabels(want.metric), timeString(ts)))
		}
	}
	return diffs
}

// exactValueMatches compares a decoded sample value with an expected rational
// exactly. Decimal spellings such as 1200 and 1200.0 are equal; 1200 and 1201
// are not, regardless of float tolerances.
func exactValueMatches(got string, want *big.Rat) bool {
	if want == nil {
		return false
	}
	number, ok := new(big.Rat).SetString(strings.TrimSpace(got))
	return ok && number.Cmp(want) == 0
}
