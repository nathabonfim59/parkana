package lokiapi

import (
	"encoding/json/v2"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// emptyLokiStats is the stats object from Loki v3.7.9's marshal test fixture
// (pkg/util/marshal/marshal_test.go, emptyStats). It pins every field and its
// JSON type, including the omitted zero recvWaitTime.
const emptyLokiStats = `{
	"index": {
		"bloomFilterTime": 0,
		"chunkRefsLookupTime": 0,
		"postFilterChunks": 0,
		"totalChunks": 0,
		"totalStreams": 0,
		"usedBloomFilters": false,
		"shardsDuration": 0
	},
	"ingester": {
		"store": {
			"chunksDownloadTime": 0,
			"congestionControlLatency": 0,
			"totalChunksRef": 0,
			"totalChunksDownloaded": 0,
			"chunkRefsFetchTime": 0,
			"queryReferencedStructuredMetadata": false,
			"queryUsedV2Engine": false,
			"pipelineWrapperFilteredLines": 0,
			"chunk": {
				"compressedBytes": 0,
				"decompressedBytes": 0,
				"decompressedLines": 0,
				"decompressedStructuredMetadataBytes": 0,
				"headChunkBytes": 0,
				"headChunkLines": 0,
				"headChunkStructuredMetadataBytes": 0,
				"postFilterLines": 0,
				"totalDuplicates": 0
			},
			"dataobj": {
				"pageBatches": 0,
				"pagesDownloaded": 0,
				"pagesDownloadedBytes": 0,
				"pagesScanned": 0,
				"postFilterRows": 0,
				"postPredicateRows": 0,
				"postPredicateDecompressedBytes": 0,
				"postPredicateStructuredMetadataBytes": 0,
				"prePredicateDecompressedRows": 0,
				"prePredicateDecompressedBytes": 0,
				"prePredicateDecompressedStructuredMetadataBytes": 0,
				"totalPageDownloadTime": 0,
				"totalRowsAvailable": 0
			}
		},
		"totalBatches": 0,
		"totalChunksMatched": 0,
		"totalLinesSent": 0,
		"totalReached": 0
	},
	"querier": {
		"querierExecTime": 0,
		"store": {
			"chunksDownloadTime": 0,
			"congestionControlLatency": 0,
			"totalChunksRef": 0,
			"totalChunksDownloaded": 0,
			"chunkRefsFetchTime": 0,
			"queryReferencedStructuredMetadata": false,
			"queryUsedV2Engine": false,
			"pipelineWrapperFilteredLines": 0,
			"chunk": {
				"compressedBytes": 0,
				"decompressedBytes": 0,
				"decompressedLines": 0,
				"decompressedStructuredMetadataBytes": 0,
				"headChunkBytes": 0,
				"headChunkLines": 0,
				"headChunkStructuredMetadataBytes": 0,
				"postFilterLines": 0,
				"totalDuplicates": 0
			},
			"dataobj": {
				"pageBatches": 0,
				"pagesDownloaded": 0,
				"pagesDownloadedBytes": 0,
				"pagesScanned": 0,
				"postFilterRows": 0,
				"postPredicateRows": 0,
				"postPredicateDecompressedBytes": 0,
				"postPredicateStructuredMetadataBytes": 0,
				"prePredicateDecompressedRows": 0,
				"prePredicateDecompressedBytes": 0,
				"prePredicateDecompressedStructuredMetadataBytes": 0,
				"totalPageDownloadTime": 0,
				"totalRowsAvailable": 0
			}
		}
	},
	"cache": {
		"chunk": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"index": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"statsResult": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"seriesResult": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"labelResult": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"volumeResult": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"instantMetricResult": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		},
		"result": {
			"entriesFound": 0,
			"entriesRequested": 0,
			"entriesStored": 0,
			"bytesReceived": 0,
			"bytesSent": 0,
			"requests": 0,
			"downloadTime": 0,
			"queryLengthServed": 0
		}
	},
	"summary": {
		"bytesProcessedPerSecond": 0,
		"execTime": 0,
		"linesProcessedPerSecond": 0,
		"queueTime": 0,
		"shards": 0,
		"splits": 0,
		"subqueries": 0,
		"totalBytesProcessed": 0,
		"totalEntriesReturned": 0,
		"totalLinesProcessed": 0,
		"totalStructuredMetadataBytesProcessed": 0,
		"totalPostFilterLines": 0
	}
}`

// lokiCategorizedStreams is the expected response from Loki v3.7.9's
// queryTestWithEncodingFlags fixture, without the warnings field (Parkana has
// no warnings yet).
const lokiCategorizedStreams = `{
	"status": "success",
	"data": {
		"resultType": "streams",
		"encodingFlags": ["categorize-labels"],
		"result": [
			{
				"stream": {"test": "test"},
				"values": [
					["123456789012345", "super line", {}],
					["123456789012346", "super line with labels", {
						"structuredMetadata": {"foo": "a", "bar": "b"}
					}],
					["123456789012347", "super line with labels msg=text", {
						"structuredMetadata": {"foo": "a", "bar": "b"},
						"parsed": {"msg": "text"}
					}]
				]
			}
		],
		"stats": ` + emptyLokiStats + `
	}
}`

func decodeJSON(t *testing.T, b []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode JSON %s: %v", b, err)
	}
	return v
}

func encodeDecoded(t *testing.T, res plan.Result, opts ResponseOptions) any {
	t.Helper()
	b, err := EncodeQueryResult(res, opts)
	if err != nil {
		t.Fatalf("EncodeQueryResult() error = %v", err)
	}
	return decodeJSON(t, b)
}

func dataOf(t *testing.T, decoded any) map[string]any {
	t.Helper()
	root, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("response root is %T, want object", decoded)
	}
	data, ok := root["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is %T, want object", root["data"])
	}
	return data
}

func TestEncodeQueryResultCategorizedMatchesLokiFixture(t *testing.T) {
	res := plan.Result{
		Type: plan.TypeStreams,
		Streams: []plan.Stream{{
			Labels: logs.New(logs.Label{Name: "test", Value: "test"}),
			Entries: []plan.Entry{
				{TS: time.Unix(0, 123456789012345), Line: "super line"},
				{
					TS:   time.Unix(0, 123456789012346),
					Line: "super line with labels",
					Meta: logs.New(
						logs.Label{Name: "foo", Value: "a"},
						logs.Label{Name: "bar", Value: "b"},
					),
				},
				{
					TS:   time.Unix(0, 123456789012347),
					Line: "super line with labels msg=text",
					Meta: logs.New(
						logs.Label{Name: "foo", Value: "a"},
						logs.Label{Name: "bar", Value: "b"},
					),
					Parsed: logs.New(logs.Label{Name: "msg", Value: "text"}),
				},
			},
		}},
	}

	got := encodeDecoded(t, res, ResponseOptions{
		EncodingFlags: FlagCategorizeLabels,
		Direction:     plan.Forward,
	})
	want := decodeJSON(t, []byte(lokiCategorizedStreams))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("response mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestEncodeQueryResultUncategorizedMergesAndRegroups(t *testing.T) {
	base := time.Unix(1700000000, 0)
	res := plan.Result{
		Type: plan.TypeStreams,
		Streams: []plan.Stream{
			{
				Labels: logs.New(
					logs.Label{Name: "app", Value: "api"},
					logs.Label{Name: "level", Value: "info"},
				),
				Entries: []plan.Entry{
					{
						TS:     base.Add(3 * time.Second),
						Line:   "a3",
						Meta:   logs.New(logs.Label{Name: "trace", Value: "t1"}),
						Parsed: logs.New(logs.Label{Name: "msg", Value: "m1"}),
					},
					{TS: base.Add(time.Second), Line: "a1"},
				},
			},
			{
				Labels: logs.New(
					logs.Label{Name: "app", Value: "api"},
					logs.Label{Name: "level", Value: "warn"},
				),
				Entries: []plan.Entry{
					{
						TS:   base.Add(2 * time.Second),
						Line: "b2",
						Meta: logs.New(logs.Label{Name: "level", Value: "debug"}),
					},
					{
						TS:     base.Add(2 * time.Second),
						Line:   "b2b",
						Parsed: logs.New(logs.Label{Name: "level", Value: "error"}),
					},
				},
			},
			{
				// Same labels as the first stream: its entries must join the
				// first stream's group.
				Labels: logs.New(
					logs.Label{Name: "app", Value: "api"},
					logs.Label{Name: "level", Value: "info"},
				),
				Entries: []plan.Entry{
					{TS: base.Add(1500 * time.Millisecond), Line: "c15"},
					{TS: base.Add(time.Second), Line: "c1tie"},
				},
			},
		},
	}

	before := cloneResult(res)
	for _, tc := range []struct {
		name        string
		dir         plan.Direction
		infoEntries [][]any
	}{
		{
			name: "backward",
			dir:  plan.Backward,
			infoEntries: [][]any{
				{nsString(base.Add(1500 * time.Millisecond)), "c15"},
				{nsString(base.Add(time.Second)), "a1"},
				{nsString(base.Add(time.Second)), "c1tie"},
			},
		},
		{
			name: "forward",
			dir:  plan.Forward,
			infoEntries: [][]any{
				{nsString(base.Add(time.Second)), "a1"},
				{nsString(base.Add(time.Second)), "c1tie"},
				{nsString(base.Add(1500 * time.Millisecond)), "c15"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dataOf(t, encodeDecoded(t, res, ResponseOptions{Direction: tc.dir}))
			if got["resultType"] != "streams" {
				t.Fatalf("resultType = %v, want streams", got["resultType"])
			}
			if _, ok := got["encodingFlags"]; ok {
				t.Fatalf("encodingFlags present without request header: %v", got["encodingFlags"])
			}
			streams := resultStreams(t, got)

			want := map[string][][]any{
				`{app="api", level="debug"}`: {{nsString(base.Add(2 * time.Second)), "b2"}},
				`{app="api", level="error"}`: {{nsString(base.Add(2 * time.Second)), "b2b"}},
				`{app="api", level="info"}`:  tc.infoEntries,
				`{app="api", level="info", msg="m1", trace="t1"}`: {
					{nsString(base.Add(3 * time.Second)), "a3"},
				},
			}
			if len(streams) != len(want) {
				t.Fatalf("got %d streams, want %d: %#v", len(streams), len(want), streams)
			}
			lastKey := ""
			for _, s := range streams {
				key := streamKey(t, s)
				if lastKey != "" && key < lastKey {
					t.Errorf("streams out of order: %q before %q", lastKey, key)
				}
				lastKey = key
				wantValues, ok := want[key]
				if !ok {
					t.Errorf("unexpected merged stream %s", key)
					continue
				}
				if !reflect.DeepEqual(toAnySlice(wantValues), s["values"]) {
					t.Errorf("stream %s values = %#v, want %#v", key, s["values"], toAnySlice(wantValues))
				}
			}
			if !reflect.DeepEqual(res, before) {
				t.Errorf("encoder mutated the caller's plan.Result")
			}
		})
	}
}

func TestEncodeQueryResultCategorizedShapes(t *testing.T) {
	base := time.Unix(1700000000, 0)
	res := plan.Result{
		Type: plan.TypeStreams,
		Streams: []plan.Stream{
			{
				Labels: logs.New(
					logs.Label{Name: "level", Value: "stream"},
					logs.Label{Name: "team", Value: "core"},
				),
				Entries: []plan.Entry{
					{
						TS:     base.Add(4 * time.Second),
						Line:   "both",
						Meta:   logs.New(logs.Label{Name: "level", Value: "meta"}, logs.Label{Name: "trace", Value: "t"}),
						Parsed: logs.New(logs.Label{Name: "level", Value: "parsed"}, logs.Label{Name: "msg", Value: "m"}),
					},
					{TS: base.Add(2 * time.Second), Line: "meta-only", Meta: logs.New(logs.Label{Name: "trace", Value: "t"})},
					{TS: base.Add(3 * time.Second), Line: "parsed-only", Parsed: logs.New(logs.Label{Name: "msg", Value: "m"})},
					{TS: base.Add(time.Second), Line: "empty"},
				},
			},
			{Labels: nil, Entries: nil},
		},
	}

	got := dataOf(t, encodeDecoded(t, res, ResponseOptions{
		EncodingFlags: FlagCategorizeLabels,
		Direction:     plan.Forward,
	}))
	streams := resultStreams(t, got)
	if len(streams) != 2 {
		t.Fatalf("got %d streams, want 2: %#v", len(streams), streams)
	}

	first := streamByKey(t, streams, `{level="stream", team="core"}`)
	wantValues := []any{
		[]any{nsString(base.Add(time.Second)), "empty", map[string]any{}},
		[]any{nsString(base.Add(2 * time.Second)), "meta-only", map[string]any{
			"structuredMetadata": map[string]any{"trace": "t"},
		}},
		[]any{nsString(base.Add(3 * time.Second)), "parsed-only", map[string]any{
			"parsed": map[string]any{"msg": "m"},
		}},
		[]any{nsString(base.Add(4 * time.Second)), "both", map[string]any{
			"structuredMetadata": map[string]any{"level": "meta", "trace": "t"},
			"parsed":             map[string]any{"level": "parsed", "msg": "m"},
		}},
	}
	if !reflect.DeepEqual(first["values"], wantValues) {
		t.Errorf("categorized values = %#v, want %#v", first["values"], wantValues)
	}
	// Categorized mode keeps the stream label even when an entry has a
	// structured metadata or parsed label of the same name (frozen plan
	// contract: stream labels stay stream labels, no regrouping).
	if !reflect.DeepEqual(first["stream"], map[string]any{"level": "stream", "team": "core"}) {
		t.Errorf("categorized stream labels = %#v", first["stream"])
	}

	empty := streamByKey(t, streams, "{}")
	if values, ok := empty["values"].([]any); !ok || len(values) != 0 {
		t.Errorf("empty stream values = %#v, want []", empty["values"])
	}
}

func TestMergeLabelsPrecedence(t *testing.T) {
	stream := logs.New(logs.Label{Name: "a", Value: "stream"}, logs.Label{Name: "b", Value: "stream"})
	meta := logs.New(logs.Label{Name: "b", Value: "meta"}, logs.Label{Name: "c", Value: "meta"})
	parsed := logs.New(logs.Label{Name: "c", Value: "parsed"}, logs.Label{Name: "d", Value: "parsed"})
	got := mergeLabels(stream, meta, parsed)
	want := logs.New(
		logs.Label{Name: "a", Value: "stream"},
		logs.Label{Name: "b", Value: "meta"},
		logs.Label{Name: "c", Value: "parsed"},
		logs.Label{Name: "d", Value: "parsed"},
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mergeLabels() = %s, want %s", got, want)
	}
}

func TestEncodeQueryResultMatrixVectorScalar(t *testing.T) {
	ts := time.UnixMilli(1568404331324)
	metric := logs.New(logs.Label{Name: "filename", Value: "/var/hostlog/apport.log"}, logs.Label{Name: "job", Value: "varlogs"})

	t.Run("matrix", func(t *testing.T) {
		res := plan.Result{Type: plan.TypeMatrix, Matrix: []plan.Series{
			{Metric: metric, Points: []plan.Point{{T: ts, V: 0.013333333333333334}}},
			{Metric: logs.New(logs.Label{Name: "job", Value: "varlogs"}), Points: []plan.Point{
				{T: ts, V: 3.45},
				{T: time.UnixMilli(1568404331339), V: 4.45},
			}},
		}}
		got := dataOf(t, encodeDecoded(t, res, ResponseOptions{}))
		want := map[string]any{
			"resultType": "matrix",
			"result": []any{
				map[string]any{
					"metric": map[string]any{"filename": "/var/hostlog/apport.log", "job": "varlogs"},
					"values": []any{[]any{1568404331.324, "0.013333333333333334"}},
				},
				map[string]any{
					"metric": map[string]any{"job": "varlogs"},
					"values": []any{
						[]any{1568404331.324, "3.45"},
						[]any{1568404331.339, "4.45"},
					},
				},
			},
			"stats": decodeJSON(t, []byte(emptyLokiStats)),
		}
		if !reflect.DeepEqual(got["result"], want["result"]) {
			t.Errorf("matrix result = %#v, want %#v", got["result"], want["result"])
		}
		if !reflect.DeepEqual(got["stats"], want["stats"]) {
			t.Errorf("matrix stats = %#v, want Loki empty stats", got["stats"])
		}
	})

	t.Run("vector", func(t *testing.T) {
		res := plan.Result{Type: plan.TypeVector, Vector: []plan.Sample{
			{Metric: metric, Point: plan.Point{T: ts, V: 0.013333333333333334}},
			{Metric: logs.New(logs.Label{Name: "job", Value: "varlogs"}), Point: plan.Point{T: ts, V: 3.45}},
		}}
		got := dataOf(t, encodeDecoded(t, res, ResponseOptions{}))
		want := []any{
			map[string]any{
				"metric": map[string]any{"filename": "/var/hostlog/apport.log", "job": "varlogs"},
				"value":  []any{1568404331.324, "0.013333333333333334"},
			},
			map[string]any{
				"metric": map[string]any{"job": "varlogs"},
				"value":  []any{1568404331.324, "3.45"},
			},
		}
		if !reflect.DeepEqual(got["result"], want) {
			t.Errorf("vector result = %#v, want %#v", got["result"], want)
		}
		if got["resultType"] != "vector" {
			t.Errorf("resultType = %v, want vector", got["resultType"])
		}
	})

	t.Run("scalar", func(t *testing.T) {
		res := plan.Result{Type: plan.TypeScalar, Scalar: plan.Point{T: time.UnixMilli(1000), V: 1}}
		got := dataOf(t, encodeDecoded(t, res, ResponseOptions{}))
		if !reflect.DeepEqual(got["result"], []any{float64(1), "1"}) {
			t.Errorf("scalar result = %#v, want [1, \"1\"]", got["result"])
		}
	})
}

func TestEncodeQueryResultNonFiniteValues(t *testing.T) {
	ts := time.UnixMilli(1568404331324)
	res := plan.Result{Type: plan.TypeMatrix, Matrix: []plan.Series{{
		Metric: logs.New(logs.Label{Name: "job", Value: "api"}),
		Points: []plan.Point{
			{T: ts, V: math.NaN()},
			{T: ts, V: math.Inf(1)},
			{T: ts, V: math.Inf(-1)},
		},
	}}}
	got := dataOf(t, encodeDecoded(t, res, ResponseOptions{}))
	series := got["result"].([]any)[0].(map[string]any)
	want := []any{
		[]any{1568404331.324, "NaN"},
		[]any{1568404331.324, "+Inf"},
		[]any{1568404331.324, "-Inf"},
	}
	if !reflect.DeepEqual(series["values"], want) {
		t.Errorf("non-finite values = %#v, want %#v", series["values"], want)
	}
}

func TestSampleTimestampFormatting(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Time
		want string
	}{
		{"whole seconds", time.Unix(1700000000, 0), "1700000000"},
		{"milliseconds", time.UnixMilli(1568404331324), "1568404331.324"},
		{"microseconds", time.Unix(1700000000, 123456000), "1700000000.123456"},
		{"nanoseconds", time.Unix(0, 123456789), "0.123456789"},
		{"negative nanosecond", time.Unix(0, -1), "-0.000000001"},
		{"negative half", time.Unix(-1, 500000000), "-0.5"},
		{"negative fraction", time.Unix(-2, 750000000), "-1.25"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(sampleTimestamp(tc.in))
			if err != nil {
				t.Fatalf("marshal sampleTimestamp: %v", err)
			}
			if got := string(b); got != tc.want {
				t.Errorf("sample timestamp = %s, want %s", got, tc.want)
			}
		})
	}

	if got := formatStreamTime(time.Unix(0, 123456789012345)); got != "123456789012345" {
		t.Errorf("formatStreamTime = %s", got)
	}
}

func TestSampleValueFormatting(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{1, "1"},
		{3.45, "3.45"},
		{0.013333333333333334, "0.013333333333333334"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"},
	} {
		if got := formatSampleValue(tc.in); got != tc.want {
			t.Errorf("formatSampleValue(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEncodingFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want []string
	}{
		{"absent", "", nil},
		{"single", "categorize-labels", []string{"categorize-labels"}},
		{"multiple sorted", "categorize-labels,zeta", []string{"categorize-labels", "zeta"}},
		{"deduplicated", "categorize-labels,categorize-labels", []string{"categorize-labels"}},
		{"unknown", "something-else", []string{"something-else"}},
		{"whitespace is not trimmed", " categorize-labels", []string{" categorize-labels"}},
		{"empty entries collapse", ",", []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseEncodingFlags(tc.raw); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseEncodingFlags(%q) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}

	res := plan.Result{Type: plan.TypeStreams, Streams: []plan.Stream{{
		Labels:  logs.New(logs.Label{Name: "app", Value: "api"}),
		Entries: []plan.Entry{{TS: time.Unix(0, 1), Line: "line"}},
	}}}

	// Known plus unknown flags: both are echoed, and categorization applies.
	got := dataOf(t, encodeDecoded(t, res, ResponseOptions{EncodingFlags: "categorize-labels,something-else"}))
	if want := []any{"categorize-labels", "something-else"}; !reflect.DeepEqual(got["encodingFlags"], want) {
		t.Errorf("encodingFlags = %#v, want %#v", got["encodingFlags"], want)
	}
	values := got["result"].([]any)[0].(map[string]any)["values"].([]any)
	if entry := values[0].([]any); len(entry) != 3 {
		t.Errorf("categorized entry = %#v, want 3 elements", entry)
	}

	// An unknown flag alone is echoed but does not change the encoding.
	got = dataOf(t, encodeDecoded(t, res, ResponseOptions{EncodingFlags: "something-else"}))
	if want := []any{"something-else"}; !reflect.DeepEqual(got["encodingFlags"], want) {
		t.Errorf("encodingFlags = %#v, want %#v", got["encodingFlags"], want)
	}
	values = got["result"].([]any)[0].(map[string]any)["values"].([]any)
	if entry := values[0].([]any); len(entry) != 2 {
		t.Errorf("uncategorized entry = %#v, want 2 elements", entry)
	}

	// Leading whitespace is not trimmed, so the flag is unknown and the
	// response stays uncategorized, matching Loki 3.7.9.
	got = dataOf(t, encodeDecoded(t, res, ResponseOptions{EncodingFlags: " categorize-labels"}))
	if want := []any{" categorize-labels"}; !reflect.DeepEqual(got["encodingFlags"], want) {
		t.Errorf("encodingFlags = %#v, want %#v", got["encodingFlags"], want)
	}
	values = got["result"].([]any)[0].(map[string]any)["values"].([]any)
	if entry := values[0].([]any); len(entry) != 2 {
		t.Errorf("whitespace-prefixed flag categorized the response: %#v", entry)
	}
}

func TestEncodeQueryResultEmptyPayloads(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  plan.Result
	}{
		{"streams", plan.Result{Type: plan.TypeStreams}},
		{"matrix", plan.Result{Type: plan.TypeMatrix}},
		{"vector", plan.Result{Type: plan.TypeVector}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dataOf(t, encodeDecoded(t, tc.res, ResponseOptions{}))
			if !reflect.DeepEqual(got["result"], []any{}) {
				t.Errorf("empty result = %#v, want []", got["result"])
			}
			if _, ok := got["stats"].(map[string]any); !ok {
				t.Errorf("stats = %#v, want object", got["stats"])
			}
		})
	}

	// Categorized mode keeps a stream object even when it has no entries, and
	// still encodes empty label and values objects as {} and [].
	got := dataOf(t, encodeDecoded(t, plan.Result{
		Type:    plan.TypeStreams,
		Streams: []plan.Stream{{Labels: nil}},
	}, ResponseOptions{EncodingFlags: FlagCategorizeLabels}))
	stream := got["result"].([]any)[0].(map[string]any)
	if !reflect.DeepEqual(stream["stream"], map[string]any{}) {
		t.Errorf("empty stream labels = %#v, want {}", stream["stream"])
	}
	if !reflect.DeepEqual(stream["values"], []any{}) {
		t.Errorf("empty stream values = %#v, want []", stream["values"])
	}
}

func TestEncodeQueryResultInvalidType(t *testing.T) {
	_, err := EncodeQueryResult(plan.Result{Type: plan.ResultType("bogus")}, ResponseOptions{})
	if err == nil || !strings.Contains(err.Error(), "unknown query result type") {
		t.Fatalf("EncodeQueryResult() error = %v, want unknown result type", err)
	}
}

func TestWriteQueryResultFailureBeforeHeaders(t *testing.T) {
	res := plan.Result{Type: plan.TypeStreams, Streams: []plan.Stream{{
		Labels: logs.New(logs.Label{Name: "app", Value: "api"}),
		Entries: []plan.Entry{{
			TS:   time.Unix(0, 1),
			Line: string([]byte{0xff, 0xfe}),
		}},
	}}}
	if _, err := EncodeQueryResult(res, ResponseOptions{}); err == nil {
		t.Fatal("EncodeQueryResult() accepted invalid UTF-8; test needs a real marshal failure")
	}

	rec := httptest.NewRecorder()
	WriteQueryResult(rec, res, ResponseOptions{})
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Errorf("Content-Type = %q, want plain text error", got)
	}
	if got := rec.Header().Get(EncodingFlagsHeader); got != "" {
		t.Errorf("unexpected response encoding flag header %q", got)
	}
	if strings.Contains(rec.Body.String(), "\"status\":\"success\"") {
		t.Errorf("wrote a partial JSON response after an encoding failure: %s", rec.Body.String())
	}
}

func TestWriteQueryResultSuccess(t *testing.T) {
	res := plan.Result{Type: plan.TypeStreams, Streams: []plan.Stream{{
		Labels:  logs.New(logs.Label{Name: "app", Value: "api"}),
		Entries: []plan.Entry{{TS: time.Unix(0, 1), Line: "line"}},
	}}}
	rec := httptest.NewRecorder()
	WriteQueryResult(rec, res, ResponseOptions{EncodingFlags: FlagCategorizeLabels})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := rec.Header().Get(EncodingFlagsHeader); got != "" {
		t.Errorf("Loki 3.7.9 does not set a response encoding-flag header, got %q", got)
	}
	decoded := decodeJSON(t, rec.Body.Bytes())
	if decoded.(map[string]any)["status"] != "success" {
		t.Errorf("status field = %v", decoded.(map[string]any)["status"])
	}
}

func TestEncodeQueryResultStats(t *testing.T) {
	got := dataOf(t, encodeDecoded(t, plan.Result{Type: plan.TypeStreams}, ResponseOptions{
		Stats: Stats{
			ExecTime:                              2 * time.Second,
			QueueTime:                             500 * time.Millisecond,
			TotalBytesProcessed:                   100,
			TotalLinesProcessed:                   10,
			TotalPostFilterLines:                  7,
			TotalStructuredMetadataBytesProcessed: 3,
			TotalEntriesReturned:                  5,
		},
	}))
	stats, ok := got["stats"].(map[string]any)
	if !ok {
		t.Fatalf("stats = %T, want object", got["stats"])
	}
	summary := stats["summary"].(map[string]any)
	want := map[string]any{
		"bytesProcessedPerSecond":               float64(50),
		"linesProcessedPerSecond":               float64(5),
		"totalBytesProcessed":                   float64(100),
		"totalLinesProcessed":                   float64(10),
		"execTime":                              float64(2),
		"queueTime":                             float64(0.5),
		"subqueries":                            float64(0),
		"totalEntriesReturned":                  float64(5),
		"splits":                                float64(0),
		"shards":                                float64(0),
		"totalPostFilterLines":                  float64(7),
		"totalStructuredMetadataBytesProcessed": float64(3),
	}
	if !reflect.DeepEqual(summary, want) {
		t.Errorf("summary = %#v, want %#v", summary, want)
	}

	// Unmeasured objects keep Loki's shape, with zero counters.
	if v := stats["cache"].(map[string]any)["chunk"].(map[string]any)["entriesFound"]; v != float64(0) {
		t.Errorf("cache.chunk.entriesFound = %v, want 0", v)
	}
	ingester := stats["ingester"].(map[string]any)
	if _, ok := ingester["recvWaitTime"]; ok {
		t.Errorf("recvWaitTime present with zero value; Loki omits it")
	}
	if v := ingester["totalReached"]; v != float64(0) {
		t.Errorf("ingester.totalReached = %v, want 0", v)
	}
}

func resultStreams(t *testing.T, data map[string]any) []map[string]any {
	t.Helper()
	raw, ok := data["result"].([]any)
	if !ok {
		t.Fatalf("result = %T, want array", data["result"])
	}
	streams := make([]map[string]any, 0, len(raw))
	for _, s := range raw {
		m, ok := s.(map[string]any)
		if !ok {
			t.Fatalf("stream = %T, want object", s)
		}
		streams = append(streams, m)
	}
	return streams
}

func streamByKey(t *testing.T, streams []map[string]any, want string) map[string]any {
	t.Helper()
	for _, s := range streams {
		if streamKey(t, s) == want {
			return s
		}
	}
	t.Fatalf("stream %s not found in %#v", want, streams)
	return nil
}

func streamKey(t *testing.T, stream map[string]any) string {
	t.Helper()
	labels, ok := stream["stream"].(map[string]any)
	if !ok {
		t.Fatalf("stream labels = %T, want object", stream["stream"])
	}
	m := make(map[string]string, len(labels))
	for k, v := range labels {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("stream label %q = %T, want string", k, v)
		}
		m[k] = s
	}
	return logs.FromMap(m).String()
}

func nsString(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

func toAnySlice(in [][]any) []any {
	out := make([]any, 0, len(in))
	for _, v := range in {
		out = append(out, v)
	}
	return out
}

func cloneResult(r plan.Result) plan.Result {
	c := r
	c.Streams = slices.Clone(r.Streams)
	for i := range c.Streams {
		c.Streams[i].Labels = slices.Clone(r.Streams[i].Labels)
		c.Streams[i].Entries = slices.Clone(r.Streams[i].Entries)
		for j := range c.Streams[i].Entries {
			c.Streams[i].Entries[j].Meta = slices.Clone(r.Streams[i].Entries[j].Meta)
			c.Streams[i].Entries[j].Parsed = slices.Clone(r.Streams[i].Entries[j].Parsed)
		}
	}
	c.Matrix = slices.Clone(r.Matrix)
	for i := range c.Matrix {
		c.Matrix[i].Metric = slices.Clone(r.Matrix[i].Metric)
		c.Matrix[i].Points = slices.Clone(r.Matrix[i].Points)
	}
	c.Vector = slices.Clone(r.Vector)
	for i := range c.Vector {
		c.Vector[i].Metric = slices.Clone(r.Vector[i].Metric)
	}
	return c
}
