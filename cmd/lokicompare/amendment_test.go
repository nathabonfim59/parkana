package main

// Tests for the M4 amendment: informational stats counts, the volume_range
// vector/matrix rule, and the explicit lokiQuirk scalar-timestamp opt-in.

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	// quirkStart and quirkEnd are the recorded final-M4 request bounds: a
	// historical range whose end is not millisecond aligned.
	quirkStart = "1793790688179590303"
	quirkEnd   = "1793790833543340518"
)

func TestVolumeRangeGrid(t *testing.T) {
	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		step  time.Duration
		want  []int64
	}{
		{
			name:  "recorded single five-minute bucket",
			start: time.Unix(0, 1793790688179590303),
			end:   time.Unix(0, 1793790833543340518),
			step:  5 * time.Minute,
			// RoundToMilliseconds rounds the through bound up to .544; the
			// whole request fits in the final bucket.
			want: []int64{1793790833544000000},
		},
		{
			name:  "three one-minute buckets",
			start: time.Unix(volumeRangeTestStartSec, 0).UTC(),
			end:   time.Unix(volumeRangeTestEndSec, 0).UTC(),
			step:  time.Minute,
			want: []int64{
				1700000039999000000, // boundary minus the one-millisecond SplitGap
				1700000099999000000,
				1700000159999000000,
				1700000180000000000, // final bucket uses the rounded-through end
			},
		},
		{
			name:  "zero step makes one bucket",
			start: time.Unix(volumeRangeTestStartSec, 0).UTC(),
			end:   time.Unix(volumeRangeTestEndSec, 0).UTC(),
			step:  0,
			want:  []int64{1700000180000000000},
		},
		{
			name:  "pre-epoch bounds",
			start: time.Unix(-90, 0).UTC(),
			end:   time.Unix(-30, 0).UTC(),
			step:  time.Minute,
			want:  []int64{-30000000000},
		},
		{
			// The request spans only two seconds but crosses a five-minute
			// boundary: the aligned first bucket ends at 11:14:59.999 and the
			// final bucket at 11:15:01, so a step larger than the span still
			// produces two buckets.
			name:  "short range crossing a step boundary",
			start: time.Date(2026, 11, 4, 11, 14, 59, 0, time.UTC),
			end:   time.Date(2026, 11, 4, 11, 15, 1, 0, time.UTC),
			step:  5 * time.Minute,
			want:  []int64{1793790899999000000, 1793790901000000000},
		},
		{
			name:  "step larger than span",
			start: time.Unix(1700000000, 0).UTC(),
			end:   time.Unix(1700000001, 0).UTC(),
			step:  time.Hour,
			want:  []int64{1700000001000000000},
		},
		{
			// Loki's ForInterval emits no bucket when a step-aligned start is
			// not before end; the default corpus deliberately excludes this
			// point-range case.
			name:  "aligned point range has no bucket",
			start: time.Unix(1700000040, 0).UTC(),
			end:   time.Unix(1700000040, 0).UTC(),
			step:  time.Minute,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := volumeRangeGrid(tc.start, tc.end, tc.step)
			if err != nil {
				t.Fatalf("volumeRangeGrid: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("grid = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestVolumeRangeCrossBoundaryMatrixAccepted(t *testing.T) {
	start := time.Date(2026, 11, 4, 11, 14, 59, 0, time.UTC)
	end := time.Date(2026, 11, 4, 11, 15, 1, 0, time.UTC)
	grid, err := volumeRangeGrid(start, end, 5*time.Minute)
	if err != nil {
		t.Fatalf("volumeRangeGrid: %v", err)
	}
	if len(grid) != 2 {
		t.Fatalf("grid = %v, want two buckets", grid)
	}
	body := `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[1793790899.999,"10"],[1793790901,"20"]]}
]}}`
	result := mustDecodeQuery(t, body, precisionExact)
	comparison, err := compareVolumeResults(queryVolumeRange, result, result, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(comparison.diffs) != 0 {
		t.Fatalf("cross-boundary grid rejected: %v", comparison.diffs)
	}
}

func TestVolumeRangeGridRejectsTooManyBuckets(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(24 * time.Hour)
	if _, err := volumeRangeGrid(start, end, time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), "more than") {
		t.Fatalf("error = %v, want a bucket bound error", err)
	}
}

func TestScalarRequestTimestampTruncatesTowardZero(t *testing.T) {
	tests := []struct {
		ns   int64
		want int64
	}{
		{ns: 1793790833543340518, want: 1793790833543000000},
		{ns: 1500000, want: 1000000},
		// Truncation toward zero, not floor: a sub-millisecond pre-epoch
		// instant maps to 0, not -1ms.
		{ns: -1, want: 0},
		{ns: -1500000, want: -1000000},
		{ns: -1000001, want: -1000000},
	}
	for _, tc := range tests {
		if got := scalarRequestTimestamp(tc.ns, precisionExact); got != tc.want {
			t.Errorf("scalarRequestTimestamp(%d) = %d, want %d", tc.ns, got, tc.want)
		}
		if got := scalarRequestTimestamp(tc.ns, precisionMicrosecond); got != tc.want {
			t.Errorf("scalarRequestTimestamp(%d, us) = %d, want %d", tc.ns, got, tc.want)
		}
	}
}

// runQuirkCase starts one response server per side and runs the CLI with the
// recorded bounds.
func runQuirkCase(t *testing.T, corpus string, leftBody, rightBody string) (int, string, string, int, int) {
	t.Helper()
	leftServer, leftRec := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, 200, leftBody)
	})
	rightServer, rightRec := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, 200, rightBody)
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), []string{
		"-left", leftServer.URL, "-right", rightServer.URL, "-corpus", corpus,
		"-start", quirkStart, "-end", quirkEnd,
	}, &stdout, &stderr)
	return code, stdout.String(), stderr.String(), len(leftRec.all()), len(rightRec.all())
}

func TestParseStep(t *testing.T) {
	tests := []struct {
		raw   string
		want  time.Duration
		error bool
	}{
		{raw: "", want: 0},
		{raw: "5m", want: 5 * time.Minute},
		{raw: "300", want: 5 * time.Minute},
		{raw: "0.0015", want: 1500 * time.Microsecond},
		{raw: "1y", want: 365 * 24 * time.Hour},
		{raw: "2w", want: 14 * 24 * time.Hour},
		{raw: "1d", want: 24 * time.Hour},
		{raw: "1h30m", want: 90 * time.Minute},
		{raw: "1ms", want: time.Millisecond},
		{raw: "0", error: true},
		{raw: "-5m", error: true},
		{raw: "0s", error: true},
		{raw: "1.5m", error: true},
		{raw: "1m1h", error: true},
		{raw: "5foo", error: true},
		{raw: "NaN", error: true},
		{raw: "Inf", error: true},
		{raw: "1e30", error: true},
		{raw: "999999999999999999999y", error: true},
		{raw: "300000y", error: true},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := parseStep(tc.raw)
			if tc.error {
				if err == nil {
					t.Fatalf("parseStep(%q) = %v, want an error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStep(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("parseStep(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestVolumeRangeGridTruncatesStepToMilliseconds(t *testing.T) {
	start := time.Date(2026, 11, 4, 11, 11, 28, 0, time.UTC)
	end := start.Add(5 * time.Millisecond)
	tests := []struct {
		name string
		step time.Duration
		want []int64
	}{
		{
			// 1.5ms is 1ms in the request's Step field. The one-millisecond
			// SplitGap makes each non-final bucket end on the previous aligned
			// boundary, and the final bucket is capped at the rounded end.
			name: "1500us behaves as 1ms",
			step: 1500 * time.Microsecond,
			want: []int64{
				start.UnixNano(),
				start.UnixNano() + 1000000,
				start.UnixNano() + 2000000,
				start.UnixNano() + 3000000,
				start.UnixNano() + 5000000,
			},
		},
		{
			// A positive sub-millisecond step truncates to zero, which the
			// volume request treats as a single bucket.
			name: "sub-millisecond step makes one bucket",
			step: 500 * time.Microsecond,
			want: []int64{start.UnixNano() + 5000000},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := volumeRangeGrid(start, end, tc.step)
			if err != nil {
				t.Fatalf("volumeRangeGrid: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("grid = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScalarQuirkNoteKeepsLeftLexeme(t *testing.T) {
	left := mustDecodeQueryMode(t, scalarEnvelope(`[4456658393173,"7"]`), decodeMode{allowScalarTimestampOverflow: true})
	right := mustDecodeQuery(t, scalarEnvelope(`[1793790833.543,"7"]`), precisionExact)
	result, err := compareScalarTimestampQuirk(left, right, 1793790833543340518, precisionExact, testCompareOptions())
	if err != nil {
		t.Fatalf("compareScalarTimestampQuirk: %v", err)
	}
	if len(result.diffs) != 0 {
		t.Fatalf("diffs = %v, want none", result.diffs)
	}
	note := strings.Join(result.notes, "\n")
	if !strings.Contains(note, "4456658393173") {
		t.Fatalf("notes = %q, want the exact LEFT timestamp lexeme", note)
	}
}

func TestRunRightStreamsExpectationStaysExact(t *testing.T) {
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, 200, `{"streams":0,"entries":0,"bytes":0,"chunks":0}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, 200, `{"streams":2,"entries":10,"bytes":100,"chunks":1}`)
	})
	corpus := writeRawCorpus(t, `[{"name":"exact-streams","type":"stats","query":"{service_name=\"api\"}","expect":{"stats":{"streams":1}}}]`)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), append(baseArgs(left.URL, right.URL, corpus), "-float-abs=1e9", "-float-rel=1e9"), &stdout, &stderr)
	if code != exitDifferent || !strings.Contains(stdout.String(), "expect stats streams") {
		t.Fatalf("exit = %d stdout = %s stderr = %s, want an exact RIGHT streams mismatch", code, stdout.String(), stderr.String())
	}
}

func TestVolumeRangeVectorExpectations(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	g0, g1 := secondsOfNS(grid[0]), secondsOfNS(grid[1])
	tests := []struct {
		name   string
		expect string
		series string
		code   int
		want   string
	}{
		{
			name:   "empty expectation rejects nonempty vector",
			expect: `[]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g0 + `,"10"]}]`,
			code:   exitDifferent,
			want:   "was not expected",
		},
		{
			name:   "matching one-bucket values",
			expect: `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200]]}]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g0 + `,"1200"]}]`,
			code:   exitEqual,
		},
		{
			name:   "vector value mismatch",
			expect: `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200]]}]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g0 + `,"1201"]}]`,
			code:   exitDifferent,
			want:   "expected=1200",
		},
		{
			name:   "vector timestamp mismatch",
			expect: `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200]]}]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g1 + `,"1200"]}]`,
			code:   exitDifferent,
			want:   "missing on right",
		},
		{
			name:   "missing expected series",
			expect: `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200]]},{"metric":{"service_name":"web"},"values":[[` + g0 + `,600]]}]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g0 + `,"1200"]}]`,
			code:   exitDifferent,
			want:   "missing on right",
		},
		{
			name:   "extra right series",
			expect: `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200]]}]`,
			series: `[{"metric":{"service_name":"api"},"value":[` + g0 + `,"1200"]},{"metric":{"service_name":"web"},"value":[` + g0 + `,"600"]}]`,
			code:   exitDifferent,
			want:   "was not expected",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corpus := writeRawCorpus(t, fmt.Sprintf(
				`[{"name":"volume-range","type":"volume_range","query":"{service_name=\"api\"}","expect":{"series":%s}}]`,
				tc.expect))
			body := `{"status":"success","data":{"resultType":"vector","stats":{},"result":` + tc.series + `}}`
			respond := func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, body)
			}
			code, stdout, stderr, _, _ := runWithServers(t, corpus, respond, respond)
			if code != tc.code {
				t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, tc.code, stdout.String(), stderr.String())
			}
			if tc.want != "" && !strings.Contains(stdout.String(), tc.want) {
				t.Fatalf("stdout = %q, want containing %q", stdout.String(), tc.want)
			}
		})
	}
}

func scalarEnvelope(point string) string {
	return `{"status":"success","data":{"resultType":"scalar","result":` + point + `}}`
}

func TestScalarQuirkRecordedOverflowAccepted(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7","lokiQuirk":"scalar-timestamp"}]`)
	code, stdout, stderr, _, _ := runQuirkCase(t, corpus,
		scalarEnvelope(`[4456658393173,"7"]`), scalarEnvelope(`[1793790833.543,"7"]`))
	if code != exitEqual {
		t.Fatalf("exit = %d, want 0\n%s\n%s", code, stdout, stderr)
	}
	if !strings.Contains(strings.ToLower(stdout), "quirk") {
		t.Fatalf("stdout = %q, want the explicit quirk note", stdout)
	}
}

func TestScalarQuirkValueMismatchIsDifference(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7","lokiQuirk":"scalar-timestamp"}]`)
	code, stdout, stderr, _, _ := runQuirkCase(t, corpus,
		scalarEnvelope(`[4456658393173,"7"]`), scalarEnvelope(`[1793790833.543,"8"]`))
	if code != exitDifferent {
		t.Fatalf("exit = %d, want %d\n%s\n%s", code, exitDifferent, stdout, stderr)
	}
}

func TestScalarQuirkRightMustMatchRequestInstant(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7","lokiQuirk":"scalar-timestamp"}]`)
	code, stdout, stderr, _, _ := runQuirkCase(t, corpus,
		scalarEnvelope(`[4456658393173,"7"]`), scalarEnvelope(`[1793790833.500,"7"]`))
	if code != exitDifferent {
		t.Fatalf("exit = %d, want %d\n%s\n%s", code, exitDifferent, stdout, stderr)
	}
	if !strings.Contains(stdout, "want request instant") {
		t.Fatalf("stdout = %q, want a request-instant difference", stdout)
	}
}

func TestScalarQuirkRejectsStringTimestamp(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7","lokiQuirk":"scalar-timestamp"}]`)
	code, _, stderr, _, _ := runQuirkCase(t, corpus,
		scalarEnvelope(`["4456658393173","7"]`), scalarEnvelope(`[1793790833.543,"7"]`))
	if code != exitFailure || !strings.Contains(stderr, "numeric JSON timestamp") {
		t.Fatalf("exit = %d stderr = %q, want a numeric-timestamp failure", code, stderr)
	}
}

func TestScalarQuirkWrongResultShapeIsFailure(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7","lokiQuirk":"scalar-timestamp"}]`)
	vector := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1793790833.543,"7"]}]}}`
	code, _, stderr, _, _ := runQuirkCase(t, corpus, vector, scalarEnvelope(`[1793790833.543,"7"]`))
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d\n%s", code, exitFailure, stderr)
	}
}

func TestScalarQuirkDefaultStaysStrict(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"scalar","type":"instant","query":"7"}]`)
	code, _, stderr, _, _ := runQuirkCase(t, corpus,
		scalarEnvelope(`[4456658393173,"7"]`), scalarEnvelope(`[1793790833.543,"7"]`))
	if code != exitFailure || !strings.Contains(stderr, "out of range") {
		t.Fatalf("exit = %d stderr = %q, want a strict out-of-range failure", code, stderr)
	}
}

func TestScalarQuirkRejectedBeforeHTTP(t *testing.T) {
	tests := []struct {
		name  string
		entry string
		want  string
	}{
		{"unknown enum", `{"name":"s","type":"instant","query":"7","lokiQuirk":"all-timestamps"}`, "not supported"},
		{"null", `{"name":"s","type":"instant","query":"7","lokiQuirk":null}`, "must be a string"},
		{"non-string", `{"name":"s","type":"instant","query":"7","lokiQuirk":1}`, "must be a string"},
		{"range entry", `{"name":"s","type":"range","query":"{}","lokiQuirk":"scalar-timestamp"}`, "only valid on instant"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corpus := writeRawCorpus(t, "["+tc.entry+"]")
			code, _, stderr, leftReqs, rightReqs := runQuirkCase(t, corpus,
				scalarEnvelope(`[1793790833.543,"7"]`), scalarEnvelope(`[1793790833.543,"7"]`))
			if code != exitFailure || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit = %d stderr = %q, want failure containing %q", code, stderr, tc.want)
			}
			if leftReqs+rightReqs != 0 {
				t.Fatalf("invalid quirk sent %d+%d HTTP requests", leftReqs, rightReqs)
			}
		})
	}
}

func TestLokiQuirkMustNotCombineWithExpect(t *testing.T) {
	var entry corpusEntry
	body := `{"name":"s","type":"instant","query":"7","lokiQuirk":"scalar-timestamp","expect":{"status":400}}`
	if err := json.Unmarshal([]byte(body), &entry); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if err := entry.validate(0); err == nil || !strings.Contains(err.Error(), "must not be combined with expect") {
		t.Fatalf("validate error = %v, want an expect conflict", err)
	}
}

func TestVolumeRangeVectorSingleBucketAccepted(t *testing.T) {
	grid := []int64{1793790833544000000}
	body := `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[1793790833.544,"184445"]}
]}}`
	left := mustDecodeQuery(t, body, precisionExact)
	right := mustDecodeQuery(t, body, precisionExact)
	result, err := compareVolumeResults(queryVolumeRange, left, right, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 0 {
		t.Fatalf("matching one-bucket vectors differ: %v", result.diffs)
	}
}

func TestVolumeRangeShapeMismatchIsDifference(t *testing.T) {
	grid := testVolumeRangeGrid(t)
	vector := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+secondsOfNS(grid[0])+`,"10"]}
]}}`, precisionExact)
	matrix := mustDecodeQuery(t, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"10"],[`+secondsOfNS(grid[2])+`,"20"]]}
]}}`, precisionExact)
	result, err := compareVolumeResults(queryVolumeRange, vector, matrix, `{service_name="api"}`, grid, requestWindows{})
	if err != nil {
		t.Fatalf("compareVolumeResults: %v", err)
	}
	if len(result.diffs) != 1 || !strings.Contains(result.diffs[0], "resultType") {
		t.Fatalf("diffs = %v, want one resultType difference", result.diffs)
	}
}

func TestVolumeRangeRejectsOffGridTimestamp(t *testing.T) {
	grid := testVolumeRangeGrid(t)
	body := `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[1700000000.000,"10"]}
]}}`
	result := mustDecodeQuery(t, body, precisionExact)
	if _, err := compareVolumeResults(queryVolumeRange, result, result, `{service_name="api"}`, grid, requestWindows{}); err == nil ||
		!strings.Contains(err.Error(), "not a bucket end") {
		t.Fatalf("error = %v, want an off-grid timestamp error", err)
	}
}

func TestVolumeRangeMatrixNeedsMultiSample(t *testing.T) {
	grid := testVolumeRangeGrid(t)
	body := `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[` + secondsOfNS(grid[0]) + `,"10"]]}
]}}`
	result := mustDecodeQuery(t, body, precisionExact)
	if _, err := compareVolumeResults(queryVolumeRange, result, result, `{service_name="api"}`, grid, requestWindows{}); err == nil ||
		!strings.Contains(err.Error(), "more than one sample") {
		t.Fatalf("error = %v, want a single-sample matrix error", err)
	}
}
