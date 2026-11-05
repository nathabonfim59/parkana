package main

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func decodeSingleEntry(t *testing.T, body string) normEntry {
	t.Helper()
	res := mustDecodeQuery(t, body, precisionExact)
	if len(res.streams) != 1 || len(res.streams[0].entries) != 1 {
		t.Fatalf("got %d streams / %d entries, want one each", len(res.streams), len(res.streams[0].entries))
	}
	return res.streams[0].entries[0]
}

func TestDecodeTupleMetadataShapes(t *testing.T) {
	stream := func(metadata string) string {
		return `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[["1700000000000000000","line",` + metadata + `]]}]}}`
	}
	tests := []struct {
		name       string
		metadata   string
		structured map[string]string
		parsed     map[string]string
	}{
		{
			name:       "structured only",
			metadata:   `{"structuredMetadata":{"trace_id":"t1"}}`,
			structured: map[string]string{"trace_id": "t1"},
		},
		{
			name:     "parsed only",
			metadata: `{"parsed":{"level":"info"}}`,
			parsed:   map[string]string{"level": "info"},
		},
		{
			name:       "both",
			metadata:   `{"structuredMetadata":{"trace_id":"t1"},"parsed":{"level":"info"}}`,
			structured: map[string]string{"trace_id": "t1"},
			parsed:     map[string]string{"level": "info"},
		},
		{
			name:       "flat metadata alias",
			metadata:   `{"trace_id":"t1","attempt":3}`,
			structured: map[string]string{"trace_id": "t1", "attempt": "3"},
		},
		{
			name:       "numeric nested label",
			metadata:   `{"structuredMetadata":{"attempt":3}}`,
			structured: map[string]string{"attempt": "3"},
		},
		{
			name:     "empty object",
			metadata: `{}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := decodeSingleEntry(t, stream(tc.metadata))
			if !maps.Equal(entry.structured, tc.structured) {
				t.Errorf("structured = %v, want %v", entry.structured, tc.structured)
			}
			if !maps.Equal(entry.parsed, tc.parsed) {
				t.Errorf("parsed = %v, want %v", entry.parsed, tc.parsed)
			}
		})
	}
}

func TestDecodeMalformedMetadata(t *testing.T) {
	stream := func(metadata string) string {
		return `{"status":"success","data":{"resultType":"streams","result":[{"stream":{"app":"api"},"values":[["1700000000000000000","line",` + metadata + `]]}]}}`
	}
	tests := []struct {
		name     string
		metadata string
		wantErr  string
	}{
		{"structured not object", `{"structuredMetadata":"nope"}`, "must be an object"},
		{"parsed not object", `{"parsed":[]}`, "must be an object"},
		{"nested label boolean", `{"structuredMetadata":{"ok":true}}`, "string or number"},
		{"flat boolean", `{"ok":true}`, "string, number or object"},
		{"unknown nested object", `{"other":{"a":"b"}}`, "string, number or object"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeQueryResponse([]byte(stream(tc.metadata)), precisionExact)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestDecodeMetricTimestampAndValueKinds(t *testing.T) {
	matrix := `{"status":"success","data":{"resultType":"matrix","result":[
 {"metric":{"level":"info"},"values":[[1700000000.123456789,3]]}
]}}`
	res := mustDecodeQuery(t, matrix, precisionExact)
	if got, want := res.series[0].points[0].ts, int64(1700000000123456789); got != want {
		t.Errorf("matrix numeric timestamp = %d, want %d", got, want)
	}
	if got, want := res.series[0].points[0].value, "3"; got != want {
		t.Errorf("matrix numeric value = %q, want %q", got, want)
	}

	vector := `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":["1700000000000000000","1"]}
]}}`
	res = mustDecodeQuery(t, vector, precisionExact)
	if got, want := res.vector[0].point.ts, int64(1700000000000000000); got != want {
		t.Errorf("vector string timestamp = %d, want %d", got, want)
	}

	scalar := `{"status":"success","data":{"resultType":"scalar","result":[1700000000.5,2.5]}}`
	res = mustDecodeQuery(t, scalar, precisionExact)
	if got, want := res.scalar.ts, int64(1700000000500000000); got != want {
		t.Errorf("scalar timestamp = %d, want %d", got, want)
	}
	if got, want := res.scalar.value, "2.5"; got != want {
		t.Errorf("scalar value = %q, want %q", got, want)
	}

	text := `{"status":"success","data":{"resultType":"string","result":[1700000000,"hello"]}}`
	res = mustDecodeQuery(t, text, precisionExact)
	if got, want := res.text.value, "hello"; got != want {
		t.Errorf("string result value = %q, want %q", got, want)
	}
}

func TestDecodeRejectsMalformedSampleKinds(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "matrix null timestamp",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[null,"1"]]}]}}`,
			wantErr: "timestamp",
		},
		{
			name:    "matrix boolean value",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000000,true]]}]}}`,
			wantErr: "sample value",
		},
		{
			name:    "scalar too short",
			body:    `{"status":"success","data":{"resultType":"scalar","result":[1700000000]}}`,
			wantErr: "point result",
		},
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

func TestDecodeMatrixAndVectorRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "matrix missing metric",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"values":[]}]}}`,
			wantErr: "missing its metric",
		},
		{
			name:    "matrix null metric",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"metric":null,"values":[]}]}}`,
			wantErr: "missing its metric",
		},
		{
			name:    "matrix missing values",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{}}]}}`,
			wantErr: "missing its values",
		},
		{
			name:    "matrix null values",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":null}]}}`,
			wantErr: "missing its values",
		},
		{
			name:    "vector missing metric",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1700000000,"1"]}]}}`,
			wantErr: "missing its metric",
		},
		{
			name:    "vector null metric",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"metric":null,"value":[1700000000,"1"]}]}}`,
			wantErr: "missing its metric",
		},
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

func TestDecodeExplicitEmptyMetricAndValues(t *testing.T) {
	matrix := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[]}]}}`
	if res := mustDecodeQuery(t, matrix, precisionExact); len(res.series) != 1 || len(res.series[0].points) != 0 {
		t.Fatalf("explicit empty matrix = %+v, want one empty series", res.series)
	}
	vector := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	if res := mustDecodeQuery(t, vector, precisionExact); len(res.vector) != 0 {
		t.Fatalf("explicit empty vector = %+v, want no samples", res.vector)
	}
	streams := `{"status":"success","data":{"resultType":"streams","result":[]}}`
	if res := mustDecodeQuery(t, streams, precisionExact); len(res.streams) != 0 {
		t.Fatalf("explicit empty streams = %+v, want no streams", res.streams)
	}
}

func TestDecodeRejectsInvalidNumericSamples(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "matrix string",
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1700000000,"abc"]]}]}}`,
		},
		{
			name: "vector string",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"abc"]}]}}`,
		},
		{
			name: "scalar string",
			body: `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"abc"]}}`,
		},
		{
			name: "empty string",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,""]}]}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeQueryResponse([]byte(tc.body), precisionExact)
			if err == nil || !strings.Contains(err.Error(), "not numeric") {
				t.Fatalf("error = %v, want a numeric validation error", err)
			}
		})
	}
}

func TestDecodeAcceptsNonFiniteNumericSamples(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf", "Inf", "-1.5e-3"} {
		t.Run(value, func(t *testing.T) {
			body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"` + value + `"]}]}}`
			if _, err := decodeQueryResponse([]byte(body), precisionExact); err != nil {
				t.Fatalf("decodeQueryResponse(%q): %v", value, err)
			}
		})
	}
}

func TestDecodeStatsRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"missing streams", `{"chunks":1,"entries":1,"bytes":1}`, "streams"},
		{"missing chunks", `{"streams":1,"entries":1,"bytes":1}`, "chunks"},
		{"missing entries", `{"streams":1,"chunks":1,"bytes":1}`, "entries"},
		{"missing bytes", `{"streams":1,"chunks":1,"entries":1}`, "bytes"},
		{"status error", `{"status":"error","streams":1,"chunks":1,"entries":1,"bytes":1}`, "status"},
		{"wrong field type", `{"streams":"many","chunks":1,"entries":1,"bytes":1}`, "decode stats"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeStatsResponse([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestDecodeLabelsAndSeriesMalformed(t *testing.T) {
	if _, err := decodeLabelsResponse([]byte(`{"status":"success","data":{"a":"b"}}`)); err == nil {
		t.Fatal("labels accepted a non-array data field")
	}
	if _, err := decodeLabelsResponse([]byte(`{"status":"success","data":[1,2]}`)); err == nil {
		t.Fatal("labels accepted non-string values")
	}
	if _, err := decodeSeriesResponse([]byte(`{"status":"success","data":{"a":"b"}}`)); err == nil {
		t.Fatal("series accepted a non-array data field")
	}
	if _, err := decodeSeriesResponse([]byte(`{"status":"success","data":[{"a":1}]}`)); err == nil {
		t.Fatal("series accepted a non-string label value")
	}
	if _, err := decodeSeriesResponse([]byte(`{"status":"success","data":[null]}`)); err == nil {
		t.Fatal("series accepted a null label set")
	}
}

func TestDiffStreamsExtraOnRight(t *testing.T) {
	left := `{"status":"success","data":{"resultType":"streams","result":[]}}`
	right := `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"app":"api"},"values":[["1700000000000000000","a"]]}
]}}`
	diffs := diffQuery(t, left, right, testCompareOptions())
	if len(diffs) == 0 || !strings.Contains(diffs[0], "missing on left") {
		t.Fatalf("diffs = %v, want a stream missing on left", diffs)
	}
}

func TestValuesEqualRejectsInvalidNumbers(t *testing.T) {
	opts := testCompareOptions()
	if valuesEqual("abc", "abc", opts) {
		t.Fatal("two invalid numeric samples compared equal")
	}
	if !valuesEqual("1", "1.0", opts) {
		t.Fatal("valid equivalent numeric samples differ")
	}
}

func TestVolumeShapeAndGrouping(t *testing.T) {
	vector := func(body string) *normResult {
		return mustDecodeQuery(t, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[`+body+`]}}`, precisionExact)
	}
	matrix := func(body string) *normResult {
		return mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[`+body+`]}}`, precisionExact)
	}
	now := time.Now()
	windows := requestWindows{
		left:  requestWindow{start: now.Add(-time.Second), end: now},
		right: requestWindow{start: now.Add(-time.Second), end: now},
	}
	nonBare := `{service_name="api"}`
	left := vector(`{"metric":{"service_name":"api"},"value":[` + secondsAt(now) + `,"1000"]}`)
	right := vector(`{"metric":{"service_name":"web"},"value":[` + secondsAt(now) + `,"1000"]}`)
	result, err := compareVolumeResults(queryVolume, left, right, nonBare, nil, windows)
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 2 {
		t.Fatalf("diffs = %v, want both missing directions", result.diffs)
	}
	if !slices.ContainsFunc(result.diffs, func(d string) bool { return strings.Contains(d, "missing on right") }) ||
		!slices.ContainsFunc(result.diffs, func(d string) bool { return strings.Contains(d, "missing on left") }) {
		t.Fatalf("diffs = %v, want missing series on both sides", result.diffs)
	}

	emptyMatrix := matrix(``)
	if _, err := compareVolumeResults(queryVolume, emptyMatrix, emptyMatrix, nonBare, nil, windows); err == nil ||
		!strings.Contains(err.Error(), "want vector") {
		t.Fatalf("error = %v, want a non-vector volume shape error", err)
	}
	scalarResult := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"scalar","stats":{},"result":[1700000000.123,"1"]}}`, precisionExact)
	if _, err := compareVolumeResults(queryVolumeRange, scalarResult, scalarResult, nonBare, nil, requestWindows{}); err == nil ||
		!strings.Contains(err.Error(), "want vector or matrix") {
		t.Fatalf("error = %v, want a non-volume_range shape error", err)
	}

	duplicate := vector(`{"metric":{"service_name":"api"},"value":[` + secondsAt(now) + `,"1000"]},` +
		`{"metric":{"service_name":"api"},"value":[` + secondsAt(now.Add(time.Second)) + `,"2000"]}`)
	if _, err := compareVolumeResults(queryVolume, duplicate, duplicate, nonBare, nil, windows); err == nil ||
		!strings.Contains(err.Error(), "repeats series") {
		t.Fatalf("error = %v, want a duplicate series error", err)
	}

	noStats := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`, precisionExact)
	if _, err := compareVolumeResults(queryVolume, noStats, noStats, nonBare, nil, windows); err == nil ||
		!strings.Contains(err.Error(), "data.stats") {
		t.Fatalf("error = %v, want a missing data.stats error", err)
	}

	empty := vector(``)
	if result, err := compareVolumeResults(queryVolume, empty, empty, nonBare, nil, windows); err != nil || len(result.diffs) != 0 {
		t.Fatalf("empty volume result = (%v, %v), want valid and equal", result.diffs, err)
	}
	if _, err := compareVolumeResults(queryVolumeRange, emptyMatrix, emptyMatrix, nonBare, nil, requestWindows{}); err == nil ||
		!strings.Contains(err.Error(), "matrix is empty") {
		t.Fatalf("error = %v, want an empty matrix error; Loki emits an empty vector", err)
	}
}

func TestVolumeRangeBareGroupingUsesUniqueTimestampSet(t *testing.T) {
	// Loki can collapse a bare {} selector to one empty-label series while
	// Parkana retains every full label set. The same grid repeated per series
	// must stay equal; a different grid tick must fail.
	grid := testVolumeRangeGrid(t)
	left := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{},"values":[[`+secondsOfNS(grid[0])+`,"10"],[`+secondsOfNS(grid[2])+`,"20"]]}
]}}`, precisionExact)
	right := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"4"],[`+secondsOfNS(grid[2])+`,"8"]]},
 {"metric":{"service_name":"web"},"values":[[`+secondsOfNS(grid[0])+`,"6"],[`+secondsOfNS(grid[2])+`,"12"]]}
]}}`, precisionExact)
	result, err := compareVolumeResults(queryVolumeRange, left, right, "{}", grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 0 {
		t.Fatalf("bare {} grouping differences failed: %v", result.diffs)
	}
	if len(result.notes) == 0 {
		t.Fatal("bare {} grouping differences were not reported as informational notes")
	}

	shifted := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[1])+`,"4"],[`+secondsOfNS(grid[2])+`,"8"]]},
 {"metric":{"service_name":"web"},"values":[[`+secondsOfNS(grid[0])+`,"6"],[`+secondsOfNS(grid[1])+`,"12"]]}
]}}`, precisionExact)
	result, err = compareVolumeResults(queryVolumeRange, left, shifted, "{}", grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) == 0 {
		t.Fatal("a shifted volume_range global tick compared equal")
	}

	nonBare := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"4"],[`+secondsOfNS(grid[2])+`,"8"]]}
]}}`, precisionExact)
	result, err = compareVolumeResults(queryVolumeRange, left, nonBare, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) == 0 {
		t.Fatal("non-bare metric label differences compared equal")
	}

	duplicateTS := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"4"],[`+secondsOfNS(grid[0])+`,"8"]]}
]}}`, precisionExact)
	if _, err := compareVolumeResults(queryVolumeRange, duplicateTS, duplicateTS, "{}", grid, requestWindows{}); err == nil ||
		!strings.Contains(err.Error(), "repeats timestamp") {
		t.Fatalf("error = %v, want a duplicate timestamp error", err)
	}
}

func TestVolumeRangeSharedSeriesTimestamps(t *testing.T) {
	grid := testVolumeRangeGrid(t)
	left := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"4"],[`+secondsOfNS(grid[1])+`,"8"]]}
]}}`, precisionExact)
	right := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"4"],[`+secondsOfNS(grid[2])+`,"8"]]}
]}}`, precisionExact)
	result, err := compareVolumeResults(queryVolumeRange, left, right, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	joined := strings.Join(result.diffs, "\n")
	if !strings.Contains(joined, "series {service_name=\"api\"} timestamps") {
		t.Fatalf("diffs = %v, want the shared series timestamp difference", result.diffs)
	}
}
