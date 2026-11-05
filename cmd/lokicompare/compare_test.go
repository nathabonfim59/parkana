package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// volumeRangeTestStartSec and volumeRangeTestEndSec are the request bounds
// behind the volume_range unit fixtures. With a one-minute step their grid is
// four aligned bucket ends. They are constants so the test package holds no
// mutable shared state.
const (
	volumeRangeTestStartSec int64 = 1700000000
	volumeRangeTestEndSec   int64 = 1700000180
)

// testVolumeRangeGrid returns the exact bucket-end grid for the shared
// volume_range request.
func testVolumeRangeGrid(t *testing.T) []int64 {
	t.Helper()
	grid, err := volumeRangeGrid(
		time.Unix(volumeRangeTestStartSec, 0).UTC(),
		time.Unix(volumeRangeTestEndSec, 0).UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("volumeRangeGrid: %v", err)
	}
	return grid
}

// secondsOfNS renders a millisecond-grid timestamp as exact decimal seconds.
func secondsOfNS(ns int64) string {
	return fmt.Sprintf("%d.%03d", ns/1e9, (ns%1e9)/1e6)
}

func testCompareOptions() compareOptions {
	return compareOptions{floatAbs: 1e-9, floatRel: 1e-9}
}

func mustDecodeQuery(t *testing.T, body string, precision timestampPrecision) *normResult {
	t.Helper()
	res, err := decodeQueryResponse([]byte(body), precision)
	if err != nil {
		t.Fatalf("decodeQueryResponse: %v", err)
	}
	return res
}

// mustDecodeQueryMode decodes with an explicit quirk tolerance.
func mustDecodeQueryMode(t *testing.T, body string, mode decodeMode) *normResult {
	t.Helper()
	res, err := decodeQueryResponseMode([]byte(body), precisionExact, mode)
	if err != nil {
		t.Fatalf("decodeQueryResponseMode: %v", err)
	}
	return res
}

func diffQuery(t *testing.T, left, right string, opts compareOptions) []string {
	t.Helper()
	return diffQueryResults(
		mustDecodeQuery(t, left, precisionExact),
		mustDecodeQuery(t, right, precisionExact),
		opts,
	)
}

const streamsLeft = `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a"],["1700000000000000001","b"]]},
 {"stream":{"app":"web"},"values":[["1700000000000000002","c"]]}
],"stats":{"summary":{"bytesProcessedPerSecond":1}}}}`

const streamsRight = `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"web"},"values":[["1700000000000000002","c"]]},
 {"stream":{"app":"api"},"values":[["1700000000000000001","b"],["1700000000000000000","a"]]}
]}}`

func TestDiffStreamsReorderedEqual(t *testing.T) {
	if diffs := diffQuery(t, streamsLeft, streamsRight, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("diffs = %v, want none", diffs)
	}
}

func TestDiffStreamsDuplicateMultiplicity(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a"],["1700000000000000000","a"]]}
]}}`
	right := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a"]]}
]}}`
	diffs := diffQuery(t, left, right, testCompareOptions())
	if len(diffs) != 1 {
		t.Fatalf("diffs = %v, want one duplicate-multiplicity difference", diffs)
	}
	if !strings.Contains(diffs[0], "left=2 right=1") {
		t.Errorf("diff = %q, want duplicate counts", diffs[0])
	}
}

func TestDiffStreamsMissing(t *testing.T) {
	diffs := diffQuery(t, streamsLeft, `{"status":"success","data":{"resultType":"streams","result":[]}}`, testCompareOptions())
	if len(diffs) == 0 {
		t.Fatal("want differences for missing streams")
	}
	joined := strings.Join(diffs, "\n")
	if !strings.Contains(joined, "missing on right") {
		t.Errorf("diffs = %q, want missing stream", joined)
	}
}

func TestDiffStreamsTupleMetadataShapes(t *testing.T) {
	plain := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a"]]}
]}}`
	categorized := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a",{"structuredMetadata":{"trace_id":"t1"},"parsed":{"level":"info"}}]]}
]}}`
	emptyMetadata := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a",{}]]}
]}}`
	if diffs := diffQuery(t, plain, emptyMetadata, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("plain tuple and empty categorized tuple differ: %v", diffs)
	}
	if diffs := diffQuery(t, plain, categorized, testCompareOptions()); len(diffs) == 0 {
		t.Fatal("tuple metadata difference not detected")
	}
}

func TestDiffMetricFloatTolerance(t *testing.T) {
	makeVector := func(value string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":[1700000000.5,"` + value + `"]}
]}}`
	}
	opts := testCompareOptions()
	if diffs := diffQuery(t, makeVector("1"), makeVector("1.0000000005"), opts); len(diffs) != 0 {
		t.Fatalf("values within tolerance differ: %v", diffs)
	}
	diffs := diffQuery(t, makeVector("1"), makeVector("1.1"), opts)
	if len(diffs) != 1 || !strings.Contains(diffs[0], "value") {
		t.Fatalf("diffs = %v, want a value difference", diffs)
	}
}

func TestDiffMetricNonFinite(t *testing.T) {
	makeVector := func(value string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":[1700000000.5,"` + value + `"]}
]}}`
	}
	opts := testCompareOptions()
	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		t.Run(value, func(t *testing.T) {
			if diffs := diffQuery(t, makeVector(value), makeVector(value), opts); len(diffs) != 0 {
				t.Fatalf("%s differs from itself: %v", value, diffs)
			}
		})
	}
	if diffs := diffQuery(t, makeVector("NaN"), makeVector("1"), opts); len(diffs) == 0 {
		t.Fatal("NaN compared equal to a finite value")
	}
	if diffs := diffQuery(t, makeVector("+Inf"), makeVector("-Inf"), opts); len(diffs) == 0 {
		t.Fatal("+Inf compared equal to -Inf")
	}
}

func TestDiffVectorReordered(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":[1700000000.5,"1"]},
 {"metric":{"level":"error"},"value":[1700000000.5,"2"]}
]}}`
	right := `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"error"},"value":[1700000000.5,"2"]},
 {"metric":{"level":"info"},"value":[1700000000.5,"1"]}
]}}`
	if diffs := diffQuery(t, left, right, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("reordered vector differs: %v", diffs)
	}
}

func TestDiffMatrixReordered(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"matrix","result":[
 {"metric":{"level":"info"},"values":[[1700000000,"1"],[1700000060,"2"]]},
 {"metric":{"level":"error"},"values":[[1700000000,"3"]]}
]}}`
	right := `{"status":"success","data":{"resultType":"matrix","result":[
 {"metric":{"level":"error"},"values":[[1700000000,"3"]]},
 {"metric":{"level":"info"},"values":[[1700000060,"2"],[1700000000,"1"]]}
]}}`
	if diffs := diffQuery(t, left, right, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("reordered matrix differs: %v", diffs)
	}
	missing := `{"status":"success","data":{"resultType":"matrix","result":[
 {"metric":{"level":"info"},"values":[[1700000000,"1"]]}
]}}`
	if diffs := diffQuery(t, left, missing, testCompareOptions()); len(diffs) == 0 {
		t.Fatal("missing matrix point compared equal")
	}
}

func TestDiffScalar(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"scalar","result":[1700000000.5,"42"]}}`
	right := `{"status":"success","data":{"resultType":"scalar","result":[1700000000.5,"42"]}}`
	if diffs := diffQuery(t, left, right, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("equal scalars differ: %v", diffs)
	}
	changed := `{"status":"success","data":{"resultType":"scalar","result":[1700000000.6,"42"]}}`
	if diffs := diffQuery(t, left, changed, testCompareOptions()); len(diffs) == 0 {
		t.Fatal("scalar timestamp difference was not detected")
	}
}

func TestDiffStringResultUsesExactString(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"string","result":[1700000000,"abc"]}}`
	same := `{"status":"success","data":{"resultType":"string","result":[1700000000,"abc"]}}`
	changed := `{"status":"success","data":{"resultType":"string","result":[1700000000,"abd"]}}`
	if diffs := diffQuery(t, left, same, testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("equal string results differ: %v", diffs)
	}
	diffs := diffQuery(t, left, changed, testCompareOptions())
	if len(diffs) == 0 || !strings.Contains(diffs[0], "value") {
		t.Fatalf("diffs = %v, want an exact string difference", diffs)
	}
	// A string result is not a numeric sample: "1" and "1.0" must differ even
	// though numeric comparison would treat them as equal.
	numericA := `{"status":"success","data":{"resultType":"string","result":[1700000000,"1"]}}`
	numericB := `{"status":"success","data":{"resultType":"string","result":[1700000000,"1.0"]}}`
	if diffs := diffQuery(t, numericA, numericB, testCompareOptions()); len(diffs) == 0 {
		t.Fatal("string result used numeric tolerance instead of exact string comparison")
	}
}

func TestDiffTimestampPrecision(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000001","a"]]}
]}}`
	right := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000002","a"]]}
]}}`
	if diffs := diffQuery(t, left, right, testCompareOptions()); len(diffs) == 0 {
		t.Fatal("exact timestamp comparison ignored a nanosecond difference")
	}
	exactLeft := mustDecodeQuery(t, left, precisionExact)
	exactRight := mustDecodeQuery(t, right, precisionExact)
	microLeft := mustDecodeQuery(t, left, precisionMicrosecond)
	microRight := mustDecodeQuery(t, right, precisionMicrosecond)
	opts := testCompareOptions()
	if diffs := diffQueryResults(exactLeft, exactRight, opts); len(diffs) == 0 {
		t.Fatal("exact precision ignored a nanosecond difference")
	}
	if diffs := diffQueryResults(microLeft, microRight, opts); len(diffs) != 0 {
		t.Fatalf("microsecond precision kept a sub-microsecond difference: %v", diffs)
	}
}

func TestDiffNumericTimestampPrecision(t *testing.T) {
	makeVector := func(ts string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[` + ts + `,"1"]}]}}`
	}
	leftBody := makeVector("1700000000.123456789")
	rightBody := makeVector("1700000000.123456790")
	exactLeft := mustDecodeQuery(t, leftBody, precisionExact)
	exactRight := mustDecodeQuery(t, rightBody, precisionExact)
	opts := testCompareOptions()
	diffs := diffQueryResults(exactLeft, exactRight, opts)
	if len(diffs) == 0 || !strings.Contains(strings.Join(diffs, "\n"), "timestamp") {
		t.Fatalf("diffs = %v, want an exact numeric timestamp difference", diffs)
	}
	microLeft := mustDecodeQuery(t, leftBody, precisionMicrosecond)
	microRight := mustDecodeQuery(t, rightBody, precisionMicrosecond)
	if diffs := diffQueryResults(microLeft, microRight, opts); len(diffs) != 0 {
		t.Fatalf("microsecond precision kept a sub-microsecond numeric timestamp difference: %v", diffs)
	}
}

func TestParseDecimalSecondsExact(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{input: "2", want: 2000000000},
		{input: "1700000000.123456789", want: 1700000000123456789},
		{input: "1.5e9", want: 1500000000000000000},
		{input: "-1.5", want: -1500000000},
		{input: "nope", wantErr: true},
		{input: "1e30", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseDecimalSeconds(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseDecimalSeconds(%q) = %d, want error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDecimalSeconds(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseDecimalSeconds(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseTimestampStringRequiresIntegerNanos(t *testing.T) {
	if got, err := parseTimestampString("1700000000123456789"); err != nil || got != 1700000000123456789 {
		t.Fatalf("parseTimestampString = (%d, %v), want the integer nanoseconds", got, err)
	}
	for _, input := range []string{"1700000000.5", "1e9", "1/2", ""} {
		if _, err := parseTimestampString(input); err == nil {
			t.Errorf("parseTimestampString(%q) succeeded, want an error", input)
		}
	}
}

func TestDiffEntriesFullLineIdentity(t *testing.T) {
	prefix := strings.Repeat("x", 200)
	makeStream := func(suffix string) string {
		return `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","` + prefix + suffix + `"]]}
]}}`
	}
	diffs := diffQuery(t, makeStream("A"), makeStream("B"), testCompareOptions())
	if len(diffs) == 0 {
		t.Fatal("lines differing after the diagnostic truncation point compared equal")
	}
}

func TestDiffCategorizedTupleMetadata(t *testing.T) {
	body := func(trace string) string {
		return `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[
  ["1700000000000000000","line",{"structuredMetadata":{"trace_id":"` + trace + `"},"parsed":{"level":"info"}}]
 ]}
]}}`
	}
	left := mustDecodeQuery(t, body("t1"), precisionExact)
	right := mustDecodeQuery(t, body("t2"), precisionExact)
	diffs := diffQueryResults(left, right, testCompareOptions())
	if len(diffs) == 0 {
		t.Fatal("categorized tuple metadata difference not detected")
	}
	if !strings.Contains(strings.Join(diffs, "\n"), "trace_id") {
		t.Errorf("diffs = %v, want metadata detail", diffs)
	}
	if diffs := diffQueryResults(left, mustDecodeQuery(t, body("t1"), precisionExact), testCompareOptions()); len(diffs) != 0 {
		t.Fatalf("identical tuple metadata differs: %v", diffs)
	}
}

func TestDecodeRejectsMissingRequiredData(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		decode  func([]byte) error
		wantErr string
	}{
		{
			name: "labels missing data",
			body: `{"status":"success"}`,
			decode: func(b []byte) error {
				_, err := decodeLabelsResponse(b)
				return err
			},
			wantErr: "data",
		},
		{
			name: "labels null data",
			body: `{"status":"success","data":null}`,
			decode: func(b []byte) error {
				_, err := decodeLabelsResponse(b)
				return err
			},
			wantErr: "data",
		},
		{
			name: "series missing data",
			body: `{"status":"success"}`,
			decode: func(b []byte) error {
				_, err := decodeSeriesResponse(b)
				return err
			},
			wantErr: "data",
		},
		{
			name: "stats empty object",
			body: `{}`,
			decode: func(b []byte) error {
				_, err := decodeStatsResponse(b)
				return err
			},
			wantErr: "streams",
		},
		{
			name: "query missing result",
			body: `{"status":"success","data":{"resultType":"streams"}}`,
			decode: func(b []byte) error {
				_, err := decodeQueryResponse(b, precisionExact)
				return err
			},
			wantErr: "result",
		},
		{
			name: "query null result",
			body: `{"status":"success","data":{"resultType":"streams","result":null}}`,
			decode: func(b []byte) error {
				_, err := decodeQueryResponse(b, precisionExact)
				return err
			},
			wantErr: "result",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.decode([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestVolumeInstantProcessingTimestampsIgnored(t *testing.T) {
	volume := func(ts string) string {
		return `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + ts + `,"1000"]}
]}}`
	}
	now := time.Now()
	windows := requestWindows{
		left:  requestWindow{start: now.Add(-time.Second), end: now},
		right: requestWindow{start: now.Add(-time.Second), end: now},
	}
	left := mustDecodeQuery(t, volume(secondsAt(now.Add(-500*time.Millisecond))), precisionExact)
	right := mustDecodeQuery(t, volume(secondsAt(now.Add(-400*time.Millisecond))), precisionExact)
	result, err := compareVolumeResults(queryVolume, left, right, `{service_name="api"}`, nil, windows)
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 0 {
		t.Fatalf("instant processing timestamps affected comparison: %v", result.diffs)
	}
}

func TestVolumeRangeTimestampsExactUnderMicrosecondPrecision(t *testing.T) {
	volumeRange := func(ts string) string {
		return `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + ts + `,"1000"]}
]}}`
	}
	// Both timestamps are acceptable bucket ends for this comparator-level
	// fixture; they differ by one nanosecond and share a microsecond grid cell,
	// so -timestamp-precision=us must still compare the raw timestamps exactly.
	left := mustDecodeQuery(t, volumeRange("1700000000.123456789"), precisionMicrosecond)
	right := mustDecodeQuery(t, volumeRange("1700000000.123456790"), precisionMicrosecond)
	grid := []int64{1700000000123456789, 1700000000123456790}
	result, err := compareVolumeResults(queryVolumeRange, left, right, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) == 0 {
		t.Fatal("volume_range ignored a timestamp difference under -timestamp-precision=us")
	}
	if !strings.Contains(strings.Join(result.diffs, "\n"), "timestamp") {
		t.Fatalf("diffs = %v, want a timestamp difference", result.diffs)
	}
}

func TestTimestampMicrosecondFloor(t *testing.T) {
	tests := []struct {
		ns   int64
		want int64
	}{
		{ns: -1, want: -1000},
		{ns: 1, want: 0},
		{ns: -1000, want: -1000},
		{ns: -1000001, want: -1001000},
		{ns: 1000001, want: 1000000},
	}
	for _, tc := range tests {
		if got := precisionMicrosecond.normalizeTimestamp(tc.ns); got != tc.want {
			t.Errorf("normalizeTimestamp(%d) = %d, want %d", tc.ns, got, tc.want)
		}
	}
	if got := precisionExact.normalizeTimestamp(-1); got != -1 {
		t.Errorf("exact normalizeTimestamp(-1) = %d, want -1", got)
	}
}

func TestDiffResultTypeMismatch(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	right := `{"status":"success","data":{"resultType":"matrix","result":[]}}`
	diffs := diffQuery(t, left, right, testCompareOptions())
	if len(diffs) != 1 || !strings.Contains(diffs[0], "resultType") {
		t.Fatalf("diffs = %v, want a resultType difference", diffs)
	}
}

func TestDecodeQueryErrors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"malformed JSON", `{`, "decode query response"},
		{"status error", `{"status":"error","data":{}}`, "status"},
		{"missing data", `{"status":"success"}`, "no data"},
		{"missing resultType", `{"status":"success","data":{}}`, "resultType"},
		{"missing result", `{"status":"success","data":{"resultType":"streams"}}`, "result"},
		{"null result", `{"status":"success","data":{"resultType":"streams","result":null}}`, "result"},
		{"unsupported resultType", `{"status":"success","data":{"resultType":"weird","result":[]}}`, "unsupported resultType"},
		{"missing stream labels", `{"status":"success","data":{"resultType":"streams","result":[{"values":[]}]}}`, "stream labels"},
		{"missing values", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{}}]}}`, "missing values"},
		{"entries alias rejected", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"entries":[{"timestamp":"1","line":"a"}]}]}}`, "missing values"},
		{"tuple too long", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a",{},{}]]}]}}`, "tuple"},
		{"non-string line", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1",2]]}]}}`, "line"},
		{"bad metadata", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","a",[]]]}]}}`, "metadata"},
		{"bad log timestamp", `{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["nope","a"]]}]}}`, "timestamp"},
		{"missing vector value", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{}}]}}`, "[timestamp, value]"},
		{"overlong vector value", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"1",2]}]}}`, "[timestamp, value]"},
		{"overlong matrix value", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000000,"1",2]]}]}}`, "[timestamp, value]"},
		{"overlong scalar", `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"1",2]}}`, "[timestamp, value]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeQueryResponse([]byte(tc.body), precisionExact)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestLabelsSets(t *testing.T) {
	left := `{"status":"success","data":["job","app","job"]}`
	right := `{"status":"success","data":["app","job"]}`
	leftLabels, err := decodeLabelsResponse([]byte(left))
	if err != nil {
		t.Fatal(err)
	}
	rightLabels, err := decodeLabelsResponse([]byte(right))
	if err != nil {
		t.Fatal(err)
	}
	if diffs := diffLabelSets(leftLabels, rightLabels); len(diffs) != 0 {
		t.Fatalf("set-equal labels differ: %v", diffs)
	}
	missing, err := decodeLabelsResponse([]byte(`{"status":"success","data":["app"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if diffs := diffLabelSets(leftLabels, missing); len(diffs) == 0 {
		t.Fatal("missing label not detected")
	}
}

func TestSeriesSets(t *testing.T) {
	left := `{"status":"success","data":[{"job":"a"},{"job":"b"},{"job":"a"}]}`
	right := `{"status":"success","data":[{"job":"b"},{"job":"a"}]}`
	leftSeries, err := decodeSeriesResponse([]byte(left))
	if err != nil {
		t.Fatal(err)
	}
	rightSeries, err := decodeSeriesResponse([]byte(right))
	if err != nil {
		t.Fatal(err)
	}
	if diffs := diffSeriesSets(leftSeries, rightSeries); len(diffs) != 0 {
		t.Fatalf("set-equal series differ: %v", diffs)
	}
	missing, err := decodeSeriesResponse([]byte(`{"status":"success","data":[{"job":"a"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	diffs := diffSeriesSets(leftSeries, missing)
	if len(diffs) != 1 || !strings.Contains(diffs[0], "job=\"b\"") {
		t.Fatalf("diffs = %v, want missing job=b", diffs)
	}
}

func TestStatsPolicy(t *testing.T) {
	left, err := decodeStatsResponse([]byte(`{"streams":2,"chunks":1,"entries":10,"bytes":100}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := decodeStatsResponse([]byte(`{"streams":2,"chunks":9,"entries":11,"bytes":999}`))
	if err != nil {
		t.Fatal(err)
	}
	result := compareStats(left, right)
	if len(result.diffs) != 0 {
		t.Fatalf("estimate counts affected stats comparison: %v", result.diffs)
	}
	if len(result.notes) != 1 || !strings.Contains(result.notes[0], "entries left=10 right=11") ||
		!strings.Contains(result.notes[0], "bytes left=100 right=999") ||
		!strings.Contains(result.notes[0], "chunks left=1 right=9") ||
		!strings.Contains(result.notes[0], "streams left=2 right=2") {
		t.Fatalf("notes = %v, want both sides of every count", result.notes)
	}
	changed, err := decodeStatsResponse([]byte(`{"streams":3,"chunks":1,"entries":10,"bytes":100}`))
	if err != nil {
		t.Fatal(err)
	}
	// streams is now informational too: Loki reports 0 for its own store while
	// Parkana counts distinct label sets. Only the exact expect.stats assertion
	// can require a concrete RIGHT value.
	result = compareStats(left, changed)
	if len(result.diffs) != 0 {
		t.Fatalf("streams estimate affected stats comparison: %v", result.diffs)
	}
	if len(result.notes) != 1 || !strings.Contains(result.notes[0], "streams left=2 right=3") {
		t.Fatalf("notes = %v, want the streams counts", result.notes)
	}
}

func TestVolumeValuesInformational(t *testing.T) {
	now := time.Now()
	volume := func(value string) string {
		return `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + secondsAt(now) + `,"` + value + `"]}
]}}`
	}
	windows := requestWindows{
		left:  requestWindow{start: now.Add(-time.Second), end: now},
		right: requestWindow{start: now.Add(-time.Second), end: now},
	}
	left := mustDecodeQuery(t, volume("1000"), precisionExact)
	right := mustDecodeQuery(t, volume("2000"), precisionExact)
	result, err := compareVolumeResults(queryVolume, left, right, `{service_name="api"}`, nil, windows)
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 0 {
		t.Fatalf("volume value difference failed the comparison: %v", result.diffs)
	}
	if len(result.notes) != 1 || !strings.Contains(result.notes[0], "value") {
		t.Fatalf("notes = %v, want an informational value note", result.notes)
	}
}
