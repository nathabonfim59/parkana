package main

import (
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// comparison is the outcome of comparing one corpus entry. Notes are
// informational: they are printed but never change the exit code.
type comparison struct {
	diffs []string
	notes []string
}

// requestWindow is the wall-clock span of one HTTP request.
type requestWindow struct {
	start time.Time
	end   time.Time
}

// requestWindows holds each side's wall-clock span.
type requestWindows struct {
	left  requestWindow
	right requestWindow
}

// volumeSkew is the documented margin around a request's wall-clock span in
// which an instant volume processing timestamp is plausible.
const volumeSkew = 60 * time.Second

// timeString renders an exact nanosecond timestamp for reports.
func timeString(ns int64) string {
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
}

// compareOptions configures numeric comparison. Non-finite floats always
// compare by exact value class: NaN equals NaN, +Inf equals +Inf, and neither
// equals a finite number.
type compareOptions struct {
	floatAbs float64
	floatRel float64
}

// diffQueryResults compares two decoded query results and returns readable
// differences. Reordered results compare equal; duplicate entries are
// preserved by comparing entry multisets.
func diffQueryResults(left, right *normResult, opts compareOptions) []string {
	if left.resultType != right.resultType {
		return []string{fmt.Sprintf("resultType: left=%q right=%q", left.resultType, right.resultType)}
	}
	switch left.resultType {
	case "streams":
		return diffStreams(left.streams, right.streams, opts)
	case "matrix":
		return diffPointGroups(groupMatrix(left.series), groupMatrix(right.series), opts)
	case "vector":
		return diffPointGroups(groupVector(left.vector), groupVector(right.vector), opts)
	case "scalar":
		return diffPoints("scalar result", *left.scalar, *right.scalar, opts)
	case "string":
		return diffStringPoints("string result", *left.text, *right.text)
	default:
		return []string{fmt.Sprintf("unsupported resultType %q", left.resultType)}
	}
}

// scalarRequestTimestamp converts an instant evaluation time in nanoseconds to
// the millisecond timestamp a scalar result carries. Go integer division
// truncates toward zero, matching Loki's model.Time millisecond conversion;
// pre-epoch instants must not be floored.
func scalarRequestTimestamp(ns int64, precision timestampPrecision) int64 {
	return precision.normalizeTimestamp(ns / int64(time.Millisecond) * int64(time.Millisecond))
}

// compareScalarTimestampQuirk applies the lokiQuirk scalar-timestamp policy to
// a pair of instant responses. Both bodies must be scalar results; any other
// shape is misuse of the opt-in and fails the invocation (exit 2) rather than
// being compared. Values keep the normal float tolerances. The RIGHT timestamp
// must be the request instant truncated to Loki's millisecond grid; the LEFT
// timestamp is validated as a numeric JSON timestamp by decoding and is never
// compared, because it is the upstream unit-overflow artifact.
func compareScalarTimestampQuirk(left, right *normResult, evalNS int64, precision timestampPrecision, opts compareOptions) (comparison, error) {
	if left.resultType != "scalar" || right.resultType != "scalar" {
		return comparison{}, fmt.Errorf("lokiQuirk %s: resultType left=%q right=%q; both must be scalar",
			lokiQuirkScalarTimestamp, left.resultType, right.resultType)
	}
	if left.scalar == nil || right.scalar == nil {
		return comparison{}, fmt.Errorf("lokiQuirk %s: scalar result is missing its point", lokiQuirkScalarTimestamp)
	}
	if !left.scalar.numeric {
		return comparison{}, fmt.Errorf("lokiQuirk %s: left scalar timestamp must be a numeric JSON timestamp", lokiQuirkScalarTimestamp)
	}
	result := comparison{}
	if !valuesEqual(left.scalar.value, right.scalar.value, opts) {
		result.diffs = append(result.diffs, fmt.Sprintf(
			"scalar result value: left=%q right=%q (absolute tolerance %g, relative tolerance %g)",
			left.scalar.value, right.scalar.value, opts.floatAbs, opts.floatRel))
	}
	// Loki stores scalar and vector timestamps in milliseconds; trust the RIGHT
	// server only at that grid and only for this request's evaluation instant.
	want := scalarRequestTimestamp(evalNS, precision)
	if right.scalar.ts != want {
		result.diffs = append(result.diffs, fmt.Sprintf(
			"lokiQuirk %s: right scalar timestamp=%s want request instant %s truncated to milliseconds",
			lokiQuirkScalarTimestamp, timeString(right.scalar.ts), timeString(want)))
	}
	leftTS := "<outside int64 nanoseconds>"
	if left.scalar.tsOverflow {
		leftTS = left.scalar.tsLexeme
	} else {
		leftTS = timeString(left.scalar.ts)
	}
	result.notes = append(result.notes, fmt.Sprintf(
		"lokiQuirk %s: left scalar timestamp %s is not compared; Loki 3.7.9 calls model.TimeFromUnix on an already-millisecond LegacySample.TimestampMs (pkg/querier/queryrange/prometheus.go marshalScalar) and the protobuf scalar round trip overflows it again; RIGHT and unopt-in entries stay strict",
		lokiQuirkScalarTimestamp, leftTS))
	return result, nil
}

type streamGroup struct {
	labels  map[string]string
	entries []normEntry
}

func groupStreams(streams []normStream) map[string]*streamGroup {
	groups := make(map[string]*streamGroup, len(streams))
	for _, stream := range streams {
		key := canonicalLabels(stream.labels)
		group, ok := groups[key]
		if !ok {
			group = &streamGroup{labels: stream.labels}
			groups[key] = group
		}
		group.entries = append(group.entries, stream.entries...)
	}
	return groups
}

func diffStreams(left, right []normStream, _ compareOptions) []string {
	lg, rg := groupStreams(left), groupStreams(right)
	var diffs []string
	for _, key := range slices.Sorted(maps.Keys(lg)) {
		group := lg[key]
		other, ok := rg[key]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("stream %s missing on right (left has %d entries)",
				formatLabels(group.labels), len(group.entries)))
			continue
		}
		diffs = append(diffs, diffEntryMultisets(formatLabels(group.labels), group.entries, other.entries)...)
	}
	for _, key := range slices.Sorted(maps.Keys(rg)) {
		if _, ok := lg[key]; ok {
			continue
		}
		group := rg[key]
		diffs = append(diffs, fmt.Sprintf("stream %s missing on left (right has %d entries)",
			formatLabels(group.labels), len(group.entries)))
	}
	return diffs
}

func diffEntryMultisets(stream string, left, right []normEntry) []string {
	leftCounts := make(map[string]int, len(left))
	leftDisplay := make(map[string]string, len(left))
	for _, e := range left {
		key := entryKey(e)
		leftCounts[key]++
		leftDisplay[key] = entryDisplay(e)
	}
	rightCounts := make(map[string]int, len(right))
	rightDisplay := make(map[string]string, len(right))
	for _, e := range right {
		key := entryKey(e)
		rightCounts[key]++
		rightDisplay[key] = entryDisplay(e)
	}
	display := func(key string) string {
		if text, ok := leftDisplay[key]; ok {
			return text
		}
		return rightDisplay[key]
	}
	var diffs []string
	for _, key := range slices.Sorted(maps.Keys(leftCounts)) {
		if lc, rc := leftCounts[key], rightCounts[key]; lc != rc {
			diffs = append(diffs, fmt.Sprintf("stream %s entry %s: left=%d right=%d", stream, display(key), lc, rc))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(rightCounts)) {
		if _, ok := leftCounts[key]; ok {
			continue
		}
		diffs = append(diffs, fmt.Sprintf("stream %s entry %s: left=0 right=%d", stream, display(key), rightCounts[key]))
	}
	return diffs
}

type pointGroup struct {
	labels map[string]string
	points map[int64][]normPoint
}

func newPointGroup(labels map[string]string) *pointGroup {
	return &pointGroup{labels: labels, points: make(map[int64][]normPoint)}
}

func (g *pointGroup) add(point normPoint) { g.points[point.ts] = append(g.points[point.ts], point) }

func groupMatrix(series []normSeries) map[string]*pointGroup {
	groups := make(map[string]*pointGroup, len(series))
	for _, item := range series {
		key := canonicalLabels(item.labels)
		group, ok := groups[key]
		if !ok {
			group = newPointGroup(item.labels)
			groups[key] = group
		}
		for _, point := range item.points {
			group.add(point)
		}
	}
	return groups
}

func groupVector(vector []normVector) map[string]*pointGroup {
	groups := make(map[string]*pointGroup, len(vector))
	for _, sample := range vector {
		key := canonicalLabels(sample.labels)
		group, ok := groups[key]
		if !ok {
			group = newPointGroup(sample.labels)
			groups[key] = group
		}
		group.add(sample.point)
	}
	return groups
}

func diffPointGroups(left, right map[string]*pointGroup, opts compareOptions) []string {
	var diffs []string
	for _, key := range slices.Sorted(maps.Keys(left)) {
		group := left[key]
		other, ok := right[key]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("series %s missing on right", formatLabels(group.labels)))
			continue
		}
		diffs = append(diffs, diffPointMaps(formatLabels(group.labels), group.points, other.points, opts)...)
	}
	for _, key := range slices.Sorted(maps.Keys(right)) {
		if _, ok := left[key]; ok {
			continue
		}
		group := right[key]
		diffs = append(diffs, fmt.Sprintf("series %s missing on left", formatLabels(group.labels)))
	}
	return diffs
}

func diffPointMaps(series string, left, right map[int64][]normPoint, opts compareOptions) []string {
	var diffs []string
	allTimestamps := make(map[int64]struct{}, len(left)+len(right))
	for ts := range left {
		allTimestamps[ts] = struct{}{}
	}
	for ts := range right {
		allTimestamps[ts] = struct{}{}
	}
	for _, ts := range slices.Sorted(maps.Keys(allTimestamps)) {
		lp, rp := left[ts], right[ts]
		if len(lp) != len(rp) {
			diffs = append(diffs, fmt.Sprintf("series %s timestamp %d: sample count left=%d right=%d",
				series, ts, len(lp), len(rp)))
			continue
		}
		for i := range lp {
			diffs = append(diffs, diffPoints(fmt.Sprintf("series %s timestamp %d sample %d", series, ts, i), lp[i], rp[i], opts)...)
		}
	}
	return diffs
}

func diffPoints(what string, left, right normPoint, opts compareOptions) []string {
	if left.ts != right.ts {
		return []string{fmt.Sprintf("%s timestamp: left=%d right=%d", what, left.ts, right.ts)}
	}
	if valuesEqual(left.value, right.value, opts) {
		return nil
	}
	return []string{fmt.Sprintf("%s value: left=%q right=%q (absolute tolerance %g, relative tolerance %g)",
		what, left.value, right.value, opts.floatAbs, opts.floatRel)}
}

// diffStringPoints compares Prometheus-style string result payloads exactly;
// they are not numeric samples and never use float tolerance.
func diffStringPoints(what string, left, right normPoint) []string {
	if left.ts != right.ts {
		return []string{fmt.Sprintf("%s timestamp: left=%d right=%d", what, left.ts, right.ts)}
	}
	if left.value == right.value {
		return nil
	}
	return []string{fmt.Sprintf("%s value: left=%q right=%q", what, left.value, right.value)}
}

// valuesEqual compares numeric metric sample values. Decoding validates that
// numeric samples parse as floats, so invalid strings are never equal, even to
// themselves.
func valuesEqual(left, right string, opts compareOptions) bool {
	lf, lerr := strconv.ParseFloat(strings.TrimSpace(left), 64)
	rf, rerr := strconv.ParseFloat(strings.TrimSpace(right), 64)
	if lerr != nil || rerr != nil {
		return false
	}
	if math.IsNaN(lf) || math.IsNaN(rf) {
		return math.IsNaN(lf) && math.IsNaN(rf)
	}
	if math.IsInf(lf, 0) || math.IsInf(rf, 0) {
		return lf == rf
	}
	diff := math.Abs(lf - rf)
	return diff <= opts.floatAbs+opts.floatRel*max(math.Abs(lf), math.Abs(rf))
}

// compareStats applies the index/stats policy. Every count (streams, entries,
// bytes and chunks) is a storage estimate that differs between Loki's chunk
// metadata and Parkana's Parquet files, so all four are reported side by side as
// informational notes and none of them fails the comparison. The documented
// shape is still decoded strictly, and expect.stats asserts exact RIGHT values.
func compareStats(left, right statsResult) comparison {
	return comparison{notes: []string{fmt.Sprintf(
		"stats counts (informational; exact RIGHT values come from expect.stats): streams left=%d right=%d, entries left=%d right=%d, bytes left=%d right=%d, chunks left=%d right=%d",
		left.streams, right.streams, left.entries, right.entries, left.bytes, right.bytes, left.chunks, right.chunks)}}
}

// compareVolumeResults applies the index volume policy. Instant volume reports
// processing-time timestamps, so only shape and grouping are compared;
// volume_range compares exact timestamp grids while values stay informational.
// grid is the request-derived set of allowed volume_range bucket ends.
func compareVolumeResults(kind queryType, left, right *normResult, query string, grid []int64, windows requestWindows) (comparison, error) {
	if kind == queryVolume {
		return compareInstantVolume(left, right, query, windows)
	}
	return compareRangeVolume(left, right, query, grid)
}

// compareInstantVolume compares index/volume vectors. Sample timestamps are
// request processing times and are never compared across the two requests.
func compareInstantVolume(left, right *normResult, query string, windows requestWindows) (comparison, error) {
	if err := validateVolumeVector("left", left, windows.left); err != nil {
		return comparison{}, err
	}
	if err := validateVolumeVector("right", right, windows.right); err != nil {
		return comparison{}, err
	}
	result := comparison{}
	bare := isBareSelector(query)
	lg, rg := vectorByKey(left.vector), vectorByKey(right.vector)
	for _, key := range slices.Sorted(maps.Keys(lg)) {
		sample := lg[key]
		other, ok := rg[key]
		if !ok {
			appendGroupingDifference(&result, bare, fmt.Sprintf("series %s missing on right", formatLabels(sample.labels)))
			result.notes = append(result.notes, fmt.Sprintf("series %s value left=%s right=<missing> (informational)",
				formatLabels(sample.labels), strconv.Quote(sample.point.value)))
			continue
		}
		result.notes = append(result.notes, fmt.Sprintf("series %s value left=%s right=%s (informational)",
			formatLabels(sample.labels), strconv.Quote(sample.point.value), strconv.Quote(other.point.value)))
	}
	for _, key := range slices.Sorted(maps.Keys(rg)) {
		if _, ok := lg[key]; !ok {
			appendGroupingDifference(&result, bare, fmt.Sprintf("series %s missing on left", formatLabels(rg[key].labels)))
			result.notes = append(result.notes, fmt.Sprintf("series %s value left=<missing> right=%s (informational)",
				formatLabels(rg[key].labels), strconv.Quote(rg[key].point.value)))
		}
	}
	return result, nil
}

// volumeRangeSeries returns a volume_range result as series regardless of the
// response's resultType: a vector carries one sample per series (the common
// single-bucket shape), a matrix carries multiple buckets.
func volumeRangeSeries(result *normResult) []normSeries {
	if result.resultType != "vector" {
		return result.series
	}
	series := make([]normSeries, 0, len(result.vector))
	for _, sample := range result.vector {
		series = append(series, normSeries{labels: sample.labels, points: []normPoint{sample.point}})
	}
	return series
}

// compareRangeVolume compares index/volume_range results. Loki's frontend emits
// a vector while every metric series has exactly one bucket and a matrix as soon
// as any series has more than one, independent of the number of request buckets
// (pkg/querier/queryrange/volume.go). Both sides must have the same shape, and
// every sample must sit on an exact request-derived bucket end. The global grid
// is compared as a SET of exact timestamps: a collapsed Loki series may repeat
// the same grid tick once per full Parkana series, which is not a difference.
func compareRangeVolume(left, right *normResult, query string, grid []int64) (comparison, error) {
	leftShape, err := validateVolumeRange("left", left, grid)
	if err != nil {
		return comparison{}, err
	}
	rightShape, err := validateVolumeRange("right", right, grid)
	if err != nil {
		return comparison{}, err
	}
	result := comparison{}
	sameShape := leftShape == rightShape
	if !sameShape {
		result.diffs = append(result.diffs, fmt.Sprintf("volume_range resultType: left=%q right=%q", leftShape, rightShape))
	}
	bare := isBareSelector(query)
	leftSeries, rightSeries := volumeRangeSeries(left), volumeRangeSeries(right)
	lg, rg := matrixByKey(leftSeries), matrixByKey(rightSeries)

	if sameShape {
		leftTS, rightTS := globalTimestamps(leftSeries), globalTimestamps(rightSeries)
		leftOnly := timestampDifference(leftTS, rightTS)
		rightOnly := timestampDifference(rightTS, leftTS)
		if len(leftOnly) > 0 || len(rightOnly) > 0 {
			result.diffs = append(result.diffs, fmt.Sprintf(
				"volume_range global timestamps: missing on right=%s missing on left=%s",
				formatTimestamps(leftOnly), formatTimestamps(rightOnly)))
		}
	}

	for _, key := range slices.Sorted(maps.Keys(lg)) {
		series := lg[key]
		other, ok := rg[key]
		if !ok {
			appendGroupingDifference(&result, bare, fmt.Sprintf("series %s missing on right", formatLabels(series.labels)))
			result.notes = append(result.notes, seriesValuesNote(series.labels, series.points, true, nil, false))
			continue
		}
		if sameShape {
			result.diffs = append(result.diffs, diffSeriesTimestamps(formatLabels(series.labels), series.points, other.points)...)
		}
		result.notes = append(result.notes, seriesValuesNote(series.labels, series.points, true, other.points, true))
	}
	for _, key := range slices.Sorted(maps.Keys(rg)) {
		if _, ok := lg[key]; !ok {
			appendGroupingDifference(&result, bare, fmt.Sprintf("series %s missing on left", formatLabels(rg[key].labels)))
			result.notes = append(result.notes, seriesValuesNote(rg[key].labels, nil, false, rg[key].points, true))
		}
	}
	return result, nil
}

// appendGroupingDifference records an unmatched metric label set. A bare {}
// selector lets Loki collapse labels, so the difference is informational; any
// other selector treats a missing series as a semantic difference.
func appendGroupingDifference(result *comparison, bare bool, message string) {
	if bare {
		result.notes = append(result.notes, message+" (informational grouping difference)")
		return
	}
	result.diffs = append(result.diffs, message)
}

// isBareSelector reports whether the query is a bare {} selector, ignoring
// whitespace.
func isBareSelector(query string) bool {
	return strings.Join(strings.Fields(query), "") == "{}"
}

// validateVolumeVector validates the index/volume shape and each sample's
// processing timestamp. The allowed window is the request's own wall-clock span
// plus the documented skew margin, never the historical query bounds.
func validateVolumeVector(side string, result *normResult, window requestWindow) error {
	if result.resultType != "vector" {
		return fmt.Errorf("%s: volume resultType is %q, want vector", side, result.resultType)
	}
	if err := validateVolumeStats(side, result); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(result.vector))
	low := window.start.Add(-volumeSkew).UnixNano()
	high := window.end.Add(volumeSkew).UnixNano()
	for i, sample := range result.vector {
		if !sample.point.numeric {
			return fmt.Errorf("%s: volume sample %d timestamp must be numeric Unix seconds, got a string", side, i)
		}
		key := canonicalLabels(sample.labels)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s: volume response repeats series %s; want one sample per series", side, formatLabels(sample.labels))
		}
		seen[key] = struct{}{}
		if sample.point.rawTS < low || sample.point.rawTS > high {
			return fmt.Errorf("%s: volume sample timestamp %s is outside the request wall-clock window %s..%s (+/-%s)",
				side, timeString(sample.point.rawTS), timeString(low), timeString(high), volumeSkew)
		}
	}
	return nil
}

// validateVolumeRange validates one index/volume_range result against the
// request's exact bucket grid and returns its result shape. Loki emits a vector
// while every metric series has one bucket and a matrix once any series has more
// than one (pkg/querier/queryrange/volume.go); an empty result stays a vector.
// Duplicate metric keys, non-numeric timestamps, off-grid timestamps, repeated
// timestamps and a matrix with no multi-sample series are malformed responses.
func validateVolumeRange(side string, result *normResult, grid []int64) (string, error) {
	if err := validateVolumeStats(side, result); err != nil {
		return "", err
	}
	allowed := make(map[int64]struct{}, len(grid))
	for _, ts := range grid {
		allowed[ts] = struct{}{}
	}
	checkTimestamp := func(series string, point normPoint) error {
		if !point.numeric {
			return fmt.Errorf("%s: volume_range %s timestamp must be numeric Unix seconds, got a string", side, series)
		}
		if _, ok := allowed[point.rawTS]; !ok {
			return fmt.Errorf("%s: volume_range %s timestamp %s is not a bucket end for the request (allowed: %s)",
				side, series, timeString(point.rawTS), formatTimestamps(grid))
		}
		return nil
	}

	switch result.resultType {
	case "vector":
		seenSeries := make(map[string]struct{}, len(result.vector))
		for _, sample := range result.vector {
			key := canonicalLabels(sample.labels)
			if _, ok := seenSeries[key]; ok {
				return "", fmt.Errorf("%s: volume_range response repeats series %s", side, formatLabels(sample.labels))
			}
			seenSeries[key] = struct{}{}
			if err := checkTimestamp(fmt.Sprintf("series %s", formatLabels(sample.labels)), sample.point); err != nil {
				return "", err
			}
		}
		return "vector", nil
	case "matrix":
		if len(result.series) == 0 {
			return "", fmt.Errorf("%s: volume_range matrix is empty; Loki emits an empty vector when no series has a sample", side)
		}
		seenSeries := make(map[string]struct{}, len(result.series))
		multiSample := false
		for i, series := range result.series {
			key := canonicalLabels(series.labels)
			if _, ok := seenSeries[key]; ok {
				return "", fmt.Errorf("%s: volume_range response repeats series %s", side, formatLabels(series.labels))
			}
			seenSeries[key] = struct{}{}
			if len(series.points) > 1 {
				multiSample = true
			}
			seenTS := make(map[int64]struct{}, len(series.points))
			for j, point := range series.points {
				if _, ok := seenTS[point.rawTS]; ok {
					return "", fmt.Errorf("%s: volume_range series %s repeats timestamp %s",
						side, formatLabels(series.labels), timeString(point.rawTS))
				}
				seenTS[point.rawTS] = struct{}{}
				if err := checkTimestamp(fmt.Sprintf("series %d %s point %d", i, formatLabels(series.labels), j), point); err != nil {
					return "", err
				}
			}
		}
		if len(result.series) > 0 && !multiSample {
			return "", fmt.Errorf("%s: volume_range matrix has no series with more than one sample; Loki emits a vector for one sample per series", side)
		}
		return "matrix", nil
	default:
		return "", fmt.Errorf("%s: volume_range resultType is %q, want vector or matrix", side, result.resultType)
	}
}

// validateVolumeStats requires the documented data.stats object. Regular query
// stats remain ignored; only index volume promises this member.
func validateVolumeStats(side string, result *normResult) error {
	switch result.stats.Kind() {
	case jsontext.KindBeginObject:
		return nil
	case jsontext.KindInvalid:
		return fmt.Errorf("%s: volume response has no data.stats object", side)
	default:
		return fmt.Errorf("%s: volume data.stats must be an object, got %s", side, result.stats.Kind())
	}
}

// vectorByKey indexes a vector by its metric label set. Duplicate keys are
// rejected earlier by validateVolumeVector.
func vectorByKey(vector []normVector) map[string]normVector {
	samples := make(map[string]normVector, len(vector))
	for _, sample := range vector {
		samples[canonicalLabels(sample.labels)] = sample
	}
	return samples
}

// matrixByKey indexes matrix series by their metric label set.
func matrixByKey(series []normSeries) map[string]normSeries {
	seriesByKey := make(map[string]normSeries, len(series))
	for _, item := range series {
		seriesByKey[canonicalLabels(item.labels)] = item
	}
	return seriesByKey
}

// globalTimestamps returns the unique, sorted timestamps across every series.
func globalTimestamps(series []normSeries) []int64 {
	set := make(map[int64]struct{})
	for _, item := range series {
		for _, point := range item.points {
			set[point.rawTS] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// seriesTimestamps returns one series' exact timestamps in sorted order.
func seriesTimestamps(points []normPoint) []int64 {
	timestamps := make([]int64, 0, len(points))
	for _, point := range points {
		timestamps = append(timestamps, point.rawTS)
	}
	slices.Sort(timestamps)
	return timestamps
}

// timestampDifference returns the values in left that are absent from right.
func timestampDifference(left, right []int64) []int64 {
	set := make(map[int64]struct{}, len(right))
	for _, ts := range right {
		set[ts] = struct{}{}
	}
	var difference []int64
	for _, ts := range left {
		if _, ok := set[ts]; !ok {
			difference = append(difference, ts)
		}
	}
	return difference
}

// diffSeriesTimestamps compares one common series' exact timestamp grid.
func diffSeriesTimestamps(name string, left, right []normPoint) []string {
	leftTS, rightTS := seriesTimestamps(left), seriesTimestamps(right)
	if slices.Equal(leftTS, rightTS) {
		return nil
	}
	return []string{fmt.Sprintf("series %s timestamps: missing on right=%s missing on left=%s",
		name, formatTimestamps(timestampDifference(leftTS, rightTS)), formatTimestamps(timestampDifference(rightTS, leftTS)))}
}

// seriesValuesNote reports both sides' sample values for one metric label set.
// Values are rendered as exact strings, never through float64, so large
// integers that differ after 2^53 remain visible. A missing side is explicit.
func seriesValuesNote(labels map[string]string, left []normPoint, leftPresent bool, right []normPoint, rightPresent bool) string {
	return fmt.Sprintf("series %s values left=%s right=%s (informational)",
		formatLabels(labels), formatPointValues(left, leftPresent), formatPointValues(right, rightPresent))
}

// formatPointValues renders a bounded list of exact timestamp/value tuples.
func formatPointValues(points []normPoint, present bool) string {
	if !present {
		return "<missing>"
	}
	const limit = 10
	var b strings.Builder
	b.WriteByte('[')
	for i, point := range points {
		if i == limit {
			fmt.Fprintf(&b, ", ... (%d more)", len(points)-limit)
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%s", timeString(point.rawTS), strconv.Quote(point.value))
	}
	b.WriteByte(']')
	return b.String()
}

// formatTimestamps renders exact timestamps for reports, bounding the list so a
// shifted grid cannot flood the output.
func formatTimestamps(timestamps []int64) string {
	const limit = 10
	var b strings.Builder
	b.WriteByte('[')
	for i, ts := range timestamps {
		if i == limit {
			fmt.Fprintf(&b, ", ... (%d more)", len(timestamps)-limit)
			break
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(timeString(ts))
	}
	b.WriteByte(']')
	return b.String()
}

// diffLabelSets compares labels and label_values results as sets.
func diffLabelSets(left, right []string) []string {
	return diffSets("label", left, right)
}

func diffSeriesSets(left, right []map[string]string) []string {
	lg := make(map[string]map[string]string, len(left))
	for _, labels := range left {
		lg[canonicalLabels(labels)] = labels
	}
	rg := make(map[string]map[string]string, len(right))
	for _, labels := range right {
		rg[canonicalLabels(labels)] = labels
	}
	var diffs []string
	for _, key := range slices.Sorted(maps.Keys(lg)) {
		if _, ok := rg[key]; !ok {
			diffs = append(diffs, fmt.Sprintf("series %s missing on right", formatLabels(lg[key])))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(rg)) {
		if _, ok := lg[key]; !ok {
			diffs = append(diffs, fmt.Sprintf("series %s missing on left", formatLabels(rg[key])))
		}
	}
	return diffs
}

func diffSets(what string, left, right []string) []string {
	leftSet := make(map[string]struct{}, len(left))
	for _, value := range left {
		leftSet[value] = struct{}{}
	}
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	var diffs []string
	for _, value := range slices.Sorted(maps.Keys(leftSet)) {
		if _, ok := rightSet[value]; !ok {
			diffs = append(diffs, fmt.Sprintf("%s %q missing on right", what, value))
		}
	}
	for _, value := range slices.Sorted(maps.Keys(rightSet)) {
		if _, ok := leftSet[value]; !ok {
			diffs = append(diffs, fmt.Sprintf("%s %q missing on left", what, value))
		}
	}
	return diffs
}
