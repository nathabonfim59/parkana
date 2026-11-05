package main

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// maxBodyBytes bounds one response so a misbehaving server cannot exhaust
	// memory before decoding rejects it.
	maxBodyBytes = 64 << 20
)

// canonicalEntry is one complete log entry. Timestamps stay exact
// nanoseconds; labels and both JSON metadata categories are compared
// literally. Only ordering is normalized, never content.
type canonicalEntry struct {
	ts         int64
	line       string
	structured map[string]string
	parsed     map[string]string
}

// canonicalStream is one label set and its entries in response order. Entry
// order is preserved so direction=forward/backward is verified.
type canonicalStream struct {
	labels  map[string]string
	entries []canonicalEntry
}

// canonicalResult is a decoded /query_range (streams) or /series result.
type canonicalResult struct {
	kind    string
	streams []canonicalStream
	series  []map[string]string
	entries int
}

// fetchResult performs one query and decodes its successful response.
func fetchResult(ctx context.Context, client *http.Client, base string, q queryCase, id tenant.ID, timeout time.Duration) (canonicalResult, error) {
	reqCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := q.request(reqCtx, base, id)
	if err != nil {
		return canonicalResult{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return canonicalResult{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return canonicalResult{}, fmt.Errorf("read response: %w", err)
	}
	if len(body) > maxBodyBytes {
		return canonicalResult{}, fmt.Errorf("response exceeds %d bytes", maxBodyBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return canonicalResult{}, fmt.Errorf("HTTP %s: %s", resp.Status, bodySnippet(body))
	}
	switch q.kind {
	case queryRange:
		return decodeRangeResponse(body)
	case querySeries:
		return decodeSeriesResponse(body)
	default:
		return canonicalResult{}, fmt.Errorf("query %q has unknown kind %q", q.name, q.kind)
	}
}

// queryEnvelope is the Loki JSON envelope shared by query_range and series.
type queryEnvelope struct {
	Status string `json:"status"`
	Data   *struct {
		ResultType string         `json:"resultType"`
		Result     jsontext.Value `json:"result"`
	} `json:"data"`
}

// decodeRangeResponse decodes a categorized /query_range response.
func decodeRangeResponse(body []byte) (canonicalResult, error) {
	var env queryEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return canonicalResult{}, fmt.Errorf("decode query response: %w", err)
	}
	if env.Status != "success" {
		return canonicalResult{}, fmt.Errorf("response status is %q", env.Status)
	}
	if env.Data == nil {
		return canonicalResult{}, errors.New("query response has no data")
	}
	if env.Data.ResultType != "streams" {
		return canonicalResult{}, fmt.Errorf("resultType is %q, want streams", env.Data.ResultType)
	}
	if env.Data.Result.Kind() != jsontext.KindBeginArray {
		return canonicalResult{}, errors.New("query response has no data.result array")
	}
	var items []streamJSON
	if err := json.Unmarshal(env.Data.Result, &items); err != nil {
		return canonicalResult{}, fmt.Errorf("decode streams result: %w", err)
	}
	result := canonicalResult{kind: "streams", streams: make([]canonicalStream, 0, len(items))}
	for i, item := range items {
		if item.Stream == nil {
			return canonicalResult{}, fmt.Errorf("stream %d is missing its stream labels", i)
		}
		if item.Values == nil {
			return canonicalResult{}, fmt.Errorf("stream %d is missing values", i)
		}
		stream := canonicalStream{labels: maps.Clone(*item.Stream), entries: make([]canonicalEntry, 0, len(*item.Values))}
		for j, tuple := range *item.Values {
			entry, err := parseEntryTuple(tuple)
			if err != nil {
				return canonicalResult{}, fmt.Errorf("stream %d value %d: %w", i, j, err)
			}
			stream.entries = append(stream.entries, entry)
		}
		result.entries += len(stream.entries)
		result.streams = append(result.streams, stream)
	}
	slices.SortFunc(result.streams, func(a, b canonicalStream) int {
		return strings.Compare(canonicalLabels(a.labels), canonicalLabels(b.labels))
	})
	return result, nil
}

type streamJSON struct {
	Stream *map[string]string  `json:"stream"`
	Values *[][]jsontext.Value `json:"values"`
}

// parseEntryTuple decodes [ns, line, categorized-metadata]. The third element
// is required: both servers are asked for categorize-labels, and comparing
// entries without metadata would hide exactly the differences this tool exists
// to find.
func parseEntryTuple(tuple []jsontext.Value) (canonicalEntry, error) {
	if len(tuple) != 3 {
		return canonicalEntry{}, fmt.Errorf("entry tuple has %d elements, want 3 (categorized)", len(tuple))
	}
	if tuple[0].Kind() != jsontext.KindString {
		return canonicalEntry{}, errors.New("entry timestamp must be a string")
	}
	var raw string
	if err := json.Unmarshal(tuple[0], &raw); err != nil {
		return canonicalEntry{}, err
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return canonicalEntry{}, fmt.Errorf("entry timestamp %q is not integer Unix nanoseconds", raw)
	}
	if tuple[1].Kind() != jsontext.KindString {
		return canonicalEntry{}, errors.New("entry line must be a string")
	}
	var line string
	if err := json.Unmarshal(tuple[1], &line); err != nil {
		return canonicalEntry{}, err
	}
	structured, parsed, err := parseEntryMetadata(tuple[2])
	if err != nil {
		return canonicalEntry{}, err
	}
	return canonicalEntry{ts: ts, line: line, structured: structured, parsed: parsed}, nil
}

// parseEntryMetadata decodes the documented JSON label categorization
// envelope. Only structuredMetadata and parsed are accepted; an unknown member
// is an error, not data silently moved into structured metadata.
func parseEntryMetadata(raw jsontext.Value) (structured, parsed map[string]string, err error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return nil, nil, fmt.Errorf("entry metadata must be an object, got %s", raw.Kind())
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("decode entry metadata: %w", err)
	}
	for key, value := range fields {
		switch key {
		case "structuredMetadata":
			labels, err := parseLabelObject(key, value)
			if err != nil {
				return nil, nil, err
			}
			structured = labels
		case "parsed":
			labels, err := parseLabelObject(key, value)
			if err != nil {
				return nil, nil, err
			}
			parsed = labels
		default:
			return nil, nil, fmt.Errorf("entry metadata has unexpected member %q", key)
		}
	}
	return structured, parsed, nil
}

func parseLabelObject(name string, raw jsontext.Value) (map[string]string, error) {
	if raw.Kind() != jsontext.KindBeginObject {
		return nil, fmt.Errorf("entry metadata %q must be an object", name)
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode entry metadata %q: %w", name, err)
	}
	labels := make(map[string]string, len(fields))
	for key, value := range fields {
		if value.Kind() != jsontext.KindString {
			// Loki's encoder writes structured metadata and parsed labels as
			// map[string]string (pkg/util/marshal), so a JSON number is a
			// conformance violation. Converting it to a string would hide a
			// real mismatch.
			return nil, fmt.Errorf("entry metadata %q label %q must be a string, got %s", name, key, value.Kind())
		}
		var s string
		if err := json.Unmarshal(value, &s); err != nil {
			return nil, fmt.Errorf("entry metadata %q label %q: %w", name, key, err)
		}
		labels[key] = s
	}
	return labels, nil
}

// decodeSeriesResponse decodes /series. Duplicate label sets are preserved so
// a server that repeats them cannot compare equal to one that does not.
func decodeSeriesResponse(body []byte) (canonicalResult, error) {
	var env struct {
		Status string         `json:"status"`
		Data   jsontext.Value `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return canonicalResult{}, fmt.Errorf("decode series response: %w", err)
	}
	if env.Status != "success" {
		return canonicalResult{}, fmt.Errorf("response status is %q", env.Status)
	}
	if env.Data.Kind() != jsontext.KindBeginArray {
		return canonicalResult{}, errors.New("series response is missing a data array")
	}
	var items []map[string]string
	if err := json.Unmarshal(env.Data, &items); err != nil {
		return canonicalResult{}, fmt.Errorf("decode series data: %w", err)
	}
	series := make([]map[string]string, 0, len(items))
	for i, item := range items {
		if item == nil {
			return canonicalResult{}, fmt.Errorf("series data %d is null", i)
		}
		series = append(series, maps.Clone(item))
	}
	slices.SortFunc(series, func(a, b map[string]string) int {
		return strings.Compare(canonicalLabels(a), canonicalLabels(b))
	})
	return canonicalResult{kind: "series", series: series}, nil
}

// diffResults compares two decoded results. Ordering across streams is
// normalized; entries within a stream are compared in the response order so
// direction semantics are checked.
func diffResults(left, right canonicalResult) []string {
	if left.kind != right.kind {
		return []string{fmt.Sprintf("result kind: left=%q right=%q", left.kind, right.kind)}
	}
	if left.kind == "series" {
		return diffSeries(left.series, right.series)
	}
	var diffs []string
	diffs = append(diffs, duplicateStreamLabelDiffs("left", left.streams)...)
	diffs = append(diffs, duplicateStreamLabelDiffs("right", right.streams)...)
	if left.entries != right.entries {
		diffs = append(diffs, fmt.Sprintf("entry count: left=%d right=%d", left.entries, right.entries))
	}
	leftByLabels := streamsByLabels(left.streams)
	rightByLabels := streamsByLabels(right.streams)
	for _, key := range unionKeys(leftByLabels, rightByLabels) {
		l, lok := leftByLabels[key]
		r, rok := rightByLabels[key]
		switch {
		case !lok:
			diffs = append(diffs, fmt.Sprintf("stream only on right: %s", formatLabels(r.labels)))
			continue
		case !rok:
			diffs = append(diffs, fmt.Sprintf("stream only on left: %s", formatLabels(l.labels)))
			continue
		}
		diffs = append(diffs, diffStreamEntries(l, r)...)
	}
	return diffs
}

// duplicateStreamLabelDiffs rejects repeated stream label sets. Loki groups
// entries by their complete label set, so a duplicate set is a response shape
// violation; silently collapsing it in a map would let [A,A,B] compare equal
// to [A,B,B].
func duplicateStreamLabelDiffs(side string, streams []canonicalStream) []string {
	counts := make(map[string]int, len(streams))
	labelsByKey := make(map[string]map[string]string, len(streams))
	for _, stream := range streams {
		key := canonicalLabels(stream.labels)
		counts[key]++
		labelsByKey[key] = stream.labels
	}
	var diffs []string
	for _, key := range slices.Sorted(maps.Keys(counts)) {
		if counts[key] > 1 {
			diffs = append(diffs, fmt.Sprintf("%s response repeats stream label set %s %d times",
				side, formatLabels(labelsByKey[key]), counts[key]))
		}
	}
	return diffs
}

func streamsByLabels(streams []canonicalStream) map[string]canonicalStream {
	out := make(map[string]canonicalStream, len(streams))
	for _, s := range streams {
		out[canonicalLabels(s.labels)] = s
	}
	return out
}

func unionKeys[V any](a, b map[string]V) []string {
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	return slices.Sorted(maps.Keys(keys))
}

func diffStreamEntries(left, right canonicalStream) []string {
	var diffs []string
	if len(left.entries) != len(right.entries) {
		diffs = append(diffs, fmt.Sprintf("stream %s entry count: left=%d right=%d",
			formatLabels(left.labels), len(left.entries), len(right.entries)))
	}
	for i := 0; i < min(len(left.entries), len(right.entries)); i++ {
		l, r := left.entries[i], right.entries[i]
		if entryKey(l) == entryKey(r) {
			continue
		}
		diffs = append(diffs, fmt.Sprintf("stream %s entry %d: left {%s} right {%s}",
			formatLabels(left.labels), i, entryDisplay(l), entryDisplay(r)))
	}
	return diffs
}

func diffSeries(left, right []map[string]string) []string {
	leftCounts := make(map[string]int, len(left))
	leftLabels := make(map[string]map[string]string, len(left))
	for _, labels := range left {
		key := canonicalLabels(labels)
		leftCounts[key]++
		leftLabels[key] = labels
	}
	rightCounts := make(map[string]int, len(right))
	rightLabels := make(map[string]map[string]string, len(right))
	for _, labels := range right {
		key := canonicalLabels(labels)
		rightCounts[key]++
		rightLabels[key] = labels
	}
	var diffs []string
	for _, key := range unionKeys(leftCounts, rightCounts) {
		want, got := leftCounts[key], rightCounts[key]
		if want == got {
			continue
		}
		labels := leftLabels[key]
		if labels == nil {
			labels = rightLabels[key]
		}
		diffs = append(diffs, fmt.Sprintf("series %s multiplicity: left=%d right=%d",
			formatLabels(labels), want, got))
	}
	return diffs
}

// directionViolation checks the requested ordering within one stream.
func directionViolation(s canonicalStream, direction string) string {
	for i := 1; i < len(s.entries); i++ {
		prev, cur := s.entries[i-1].ts, s.entries[i].ts
		if direction == "backward" && cur > prev {
			return fmt.Sprintf("stream %s is not backward: entry %d ts %s is newer than entry %d ts %s",
				formatLabels(s.labels), i, timeString(cur), i-1, timeString(prev))
		}
		if direction == "forward" && cur < prev {
			return fmt.Sprintf("stream %s is not forward: entry %d ts %s is older than entry %d ts %s",
				formatLabels(s.labels), i, timeString(cur), i-1, timeString(prev))
		}
	}
	return ""
}

// itemCount is the comparable result cardinality: entries for streams, label
// sets for series.
func (r canonicalResult) itemCount() int {
	if r.kind == "series" {
		return len(r.series)
	}
	return r.entries
}

// checkQueryExpectation applies the structural assertions derived from the
// fixture plan. These are independent of the other side: both-empty is never a
// meaningful pass when the fixture says data must exist.
func checkQueryExpectation(q queryCase, left, right canonicalResult) []string {
	var diffs []string
	if q.expectEntries != nil {
		want := *q.expectEntries
		if left.entries != want {
			diffs = append(diffs, fmt.Sprintf("left entry count is %d, fixture requires %d", left.entries, want))
		}
		if right.entries != want {
			diffs = append(diffs, fmt.Sprintf("right entry count is %d, fixture requires %d", right.entries, want))
		}
	}
	if q.requireMatch && left.itemCount() == 0 && right.itemCount() == 0 {
		diffs = append(diffs, "query matched nothing on either side; the fixture and filter disagree")
	}
	if q.expectTimestamps != nil {
		want := slices.Clone(q.expectTimestamps)
		slices.Sort(want)
		diffs = append(diffs, checkTimestamps("left", left, want)...)
		diffs = append(diffs, checkTimestamps("right", right, want)...)
	}
	return diffs
}

func checkTimestamps(side string, result canonicalResult, want []int64) []string {
	got := make([]int64, 0, result.entries)
	for _, s := range result.streams {
		for _, e := range s.entries {
			got = append(got, e.ts)
		}
	}
	slices.Sort(got)
	if slices.Equal(got, want) {
		return nil
	}
	format := func(ts []int64) string {
		parts := make([]string, len(ts))
		for i, t := range ts {
			parts[i] = timeString(t)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return []string{fmt.Sprintf("%s entry timestamps are %s, fixture requires %s", side, format(got), format(want))}
}

// timeString renders an exact nanosecond timestamp for reports.
func timeString(ns int64) string {
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
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

// entryKey is the complete entry identity used for equality.
func entryKey(e canonicalEntry) string {
	return fmt.Sprintf("ts=%d line=%q structured=%s parsed=%s", e.ts, e.line,
		canonicalLabels(e.structured), canonicalLabels(e.parsed))
}

// entryDisplay renders an entry for a difference report, truncating only the
// rendered line and never its comparison identity.
func entryDisplay(e canonicalEntry) string {
	return fmt.Sprintf("ts=%s line=%q structured=%s parsed=%s", timeString(e.ts), short(e.line),
		formatLabels(e.structured), formatLabels(e.parsed))
}

func short(s string) string {
	const limit = 160
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
