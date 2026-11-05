package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordedRequest struct {
	method     string
	requestURI string
	path       string
	query      url.Values
	header     http.Header
}

type recorder struct {
	mu   sync.Mutex
	reqs []recordedRequest
}

func (r *recorder) add(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, recordedRequest{
		method:     req.Method,
		requestURI: req.RequestURI,
		path:       req.URL.Path,
		query:      req.URL.Query(),
		header:     req.Header.Clone(),
	})
}

func (r *recorder) all() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reqs)
}

func newRecorderServer(t *testing.T, handler func(http.ResponseWriter, *http.Request)) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func writeResponse(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// nowSeconds renders the current wall-clock time as decimal Unix seconds with
// nanosecond precision, the shape index volume processing timestamps use.
func nowSeconds() string {
	ns := time.Now().UnixNano()
	return fmt.Sprintf("%d.%09d", ns/1e9, ns%1e9)
}

// secondsAt renders a time as decimal Unix seconds.
func secondsAt(t time.Time) string {
	ns := t.UnixNano()
	return fmt.Sprintf("%d.%09d", ns/1e9, ns%1e9)
}

const (
	testStart = "2026-11-04T10:00:00Z"
	testEnd   = "2026-11-04T11:00:00Z"
)

func baseArgs(left, right, corpus string) []string {
	return []string{
		"-left", left, "-right", right, "-corpus", corpus,
		"-start", testStart, "-end", testEnd,
	}
}

// responseFor returns semantically equal responses in different order/shape
// for the left (variant 0) and right (variant 1) servers.
func responseFor(path string, variant int, grid []int64) string {
	switch path {
	case "/loki/api/v1/query_range":
		if variant == 0 {
			return `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"job":"corpus"},"values":[["1700000000000000000","a"],["1700000000000000001","b"]]},
 {"stream":{"job":"web"},"values":[["1700000000000000002","c"]]}
],"stats":{"summary":{"bytesProcessedPerSecond":99}}}}`
		}
		return `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"job":"web"},"values":[["1700000000000000002","c"]]},
 {"stream":{"job":"corpus"},"values":[["1700000000000000001","b"],["1700000000000000000","a"]]}
]}}`
	case "/loki/api/v1/query":
		if variant == 0 {
			return `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":[1700000000.5,"1"]},
 {"metric":{"level":"error"},"value":[1700000000.5,"2"]}
],"stats":{"summary":{"totalBytesProcessed":10}}}}`
		}
		return `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"error"},"value":[1700000000.5,"2"]},
 {"metric":{"level":"info"},"value":[1700000000.5,"1"]}
]}}`
	case "/loki/api/v1/labels":
		if variant == 0 {
			return `{"status":"success","data":["job","level","job"]}`
		}
		return `{"status":"success","data":["level","job"]}`
	case "/loki/api/v1/label/service_name/values":
		if variant == 0 {
			return `{"status":"success","data":["api","web"]}`
		}
		return `{"status":"success","data":["web","api"]}`
	case "/loki/api/v1/series":
		if variant == 0 {
			return `{"status":"success","data":[{"job":"corpus"},{"job":"web"}]}`
		}
		return `{"status":"success","data":[{"job":"web"},{"job":"corpus"},{"job":"web"}]}`
	case "/loki/api/v1/index/stats":
		if variant == 0 {
			return `{"streams":2,"chunks":1,"entries":10,"bytes":100}`
		}
		return `{"streams":2,"chunks":9,"entries":10,"bytes":999}`
	case "/loki/api/v1/index/volume":
		if variant == 0 {
			return `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + nowSeconds() + `,"1000"]}
]}}`
		}
		return `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + nowSeconds() + `,"1000.0000000005"]}
]}}`
	case "/loki/api/v1/index/volume_range":
		if variant == 0 {
			return `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[` + secondsOfNS(grid[0]) + `,"1000"],[` + secondsOfNS(grid[1]) + `,"2000"]]}
]}}`
		}
		return `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[` + secondsOfNS(grid[1]) + `,"2000"],[` + secondsOfNS(grid[0]) + `,"1000"]]}
]}}`
	default:
		return `{"status":"success","data":[]}`
	}
}

// baseVolumeRangeGrid is the exact volume_range bucket grid for the shared CLI
// test bounds and Loki's default step.
func baseVolumeRangeGrid(t *testing.T) []int64 {
	t.Helper()
	start, err := parseTime(testStart)
	if err != nil {
		t.Fatal(err)
	}
	end, err := parseTime(testEnd)
	if err != nil {
		t.Fatal(err)
	}
	grid, err := volumeRangeGrid(start, end, defaultVolumeRangeStep(end.Sub(start)))
	if err != nil {
		t.Fatalf("volumeRangeGrid: %v", err)
	}
	return grid
}

func equalCorpus(t *testing.T) []corpusEntry {
	t.Helper()
	return []corpusEntry{
		{Name: "range", Type: queryRange, Query: `{job="corpus"}`},
		{Name: "instant", Type: queryInstant, Query: `sum(count_over_time({job="corpus"}[5m]))`},
		{Name: "labels", Type: queryLabels, Query: `{job="corpus"}`},
		{Name: "label-values", Type: queryLabelValues, Query: `{job="corpus"}`, Params: Params{"label": {"service_name"}}},
		{Name: "series", Type: querySeries, Query: `{job="corpus"}`},
		{Name: "stats", Type: queryStats, Query: `{job="corpus"}`},
		{Name: "volume", Type: queryVolume, Query: `{job="corpus"}`},
		{Name: "volume-range", Type: queryVolumeRange, Query: `{job="corpus"}`},
	}
}

func TestRunEqualAcrossTypes(t *testing.T) {
	corpus := writeCorpusFile(t, equalCorpus(t))
	left, leftRec := newRecorderServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, http.StatusOK, responseFor(r.URL.Path, 0, baseVolumeRangeGrid(t)))
	})
	right, rightRec := newRecorderServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, http.StatusOK, responseFor(r.URL.Path, 1, baseVolumeRangeGrid(t)))
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), append(baseArgs(left.URL, right.URL, corpus), "-tenant", "team-a"), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitEqual, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "8 entries: 8 equal, 0 different, 0 failed") {
		t.Errorf("stdout = %q, want equal summary", stdout.String())
	}

	leftReqs, rightReqs := leftRec.all(), rightRec.all()
	if len(leftReqs) != 8 || len(rightReqs) != 8 {
		t.Fatalf("request counts left=%d right=%d, want 8 each", len(leftReqs), len(rightReqs))
	}
	wantStart := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	wantEnd := time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	for i := range leftReqs {
		l, r := leftReqs[i], rightReqs[i]
		if l.requestURI != r.requestURI {
			t.Errorf("request %d URI differs: left=%q right=%q", i, l.requestURI, r.requestURI)
		}
		if got := l.header.Get("X-Scope-OrgID"); got != "team-a" {
			t.Errorf("request %d tenant header = %q, want team-a", i, got)
		}
		if l.method != http.MethodGet {
			t.Errorf("request %d method = %q, want GET", i, l.method)
		}
		switch l.path {
		case "/loki/api/v1/query_range":
			if got := l.query.Get("start"); got != wantStart {
				t.Errorf("range start = %q, want %q", got, wantStart)
			}
			if got := l.query.Get("end"); got != wantEnd {
				t.Errorf("range end = %q, want %q", got, wantEnd)
			}
			if got := l.header.Get(encodingFlagsHeader); got != categorizeLabels {
				t.Errorf("range encoding header = %q, want %q", got, categorizeLabels)
			}
		case "/loki/api/v1/query":
			if got := l.query.Get("time"); got != wantEnd {
				t.Errorf("instant time = %q, want %q", got, wantEnd)
			}
			if got := l.header.Get(encodingFlagsHeader); got != categorizeLabels {
				t.Errorf("instant encoding header = %q, want %q", got, categorizeLabels)
			}
		case "/loki/api/v1/labels", "/loki/api/v1/label/service_name/values":
			if got := l.query.Get("query"); got != `{job="corpus"}` {
				t.Errorf("%s scope query = %q, want the selector", l.path, got)
			}
			if got := l.query["match[]"]; len(got) != 0 {
				t.Errorf("%s match[] = %v, want none", l.path, got)
			}
			if got := l.query.Get("start"); got != wantStart {
				t.Errorf("%s start = %q, want %q", l.path, got, wantStart)
			}
			if got := l.query.Get("end"); got != wantEnd {
				t.Errorf("%s end = %q, want %q", l.path, got, wantEnd)
			}
		case "/loki/api/v1/series":
			if got := l.query.Get("query"); got != "" {
				t.Errorf("series query = %q, want match[] only", got)
			}
			if got := l.query["match[]"]; !slices.Equal(got, []string{`{job="corpus"}`}) {
				t.Errorf("series match[] = %v, want selector", got)
			}
			if got := l.query.Get("start"); got != wantStart {
				t.Errorf("series start = %q, want %q", got, wantStart)
			}
			if got := l.query.Get("end"); got != wantEnd {
				t.Errorf("series end = %q, want %q", got, wantEnd)
			}
		default:
			if got := l.query.Get("start"); got != wantStart {
				t.Errorf("%s start = %q, want %q", l.path, got, wantStart)
			}
			if got := l.query.Get("end"); got != wantEnd {
				t.Errorf("%s end = %q, want %q", l.path, got, wantEnd)
			}
		}
	}
}

func TestRunDifference(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{
		{Name: "range-diff", Type: queryRange, Query: `{job="corpus"}`},
	})
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"job":"corpus"},"values":[["1700000000000000000","left"]]}
]}}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[
 {"stream":{"job":"corpus"},"values":[["1700000000000000000","right"]]}
]}}`)
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitDifferent {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitDifferent, stdout.String(), stderr.String())
	}
	report := stdout.String()
	for _, want := range []string{"DIFF range-diff", "/loki/api/v1/query_range", "1 entries: 0 equal, 1 different, 0 failed"} {
		if !strings.Contains(report, want) {
			t.Errorf("report %q does not contain %q", report, want)
		}
	}
}

func TestRunHTTPFailure(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "range-broken", Type: queryRange, Query: `{}`}})
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusInternalServerError, "boom")
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailure, stdout.String(), stderr.String())
	}
	for _, want := range []string{"FAIL range-broken", "/loki/api/v1/query_range", "HTTP 500", "boom"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr %q does not contain %q", stderr.String(), want)
		}
	}
}

func TestRunBothServersFail(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "range-both-broken", Type: queryRange, Query: `{}`}})
	newBroken := func(t *testing.T) *httptest.Server {
		srv, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusBadGateway, "upstream down")
		})
		return srv
	}
	left, right := newBroken(t), newBroken(t)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d; identical failures must not compare equal\nstderr:\n%s", code, exitFailure, stderr.String())
	}
}

func TestRunBothServersReturnErrorEnvelope(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "labels-error", Type: queryLabels}})
	newErrorServer := func(t *testing.T) *httptest.Server {
		srv, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"error","error":"boom"}`)
		})
		return srv
	}
	left, right := newErrorServer(t), newErrorServer(t)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d; identical error envelopes must not compare equal\nstdout:\n%s\nstderr:\n%s",
			code, exitFailure, stdout.String(), stderr.String())
	}
}

func TestRunMalformedJSON(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "labels-bad", Type: queryLabels}})
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":[]}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":`)
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
	}
	if !strings.Contains(stderr.String(), "decode") {
		t.Errorf("stderr = %q, want a decode failure", stderr.String())
	}
}

func TestRunTimeout(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "labels-slow", Type: queryLabels}})
	left, _ := newRecorderServer(t, func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":[]}`)
	})
	var stdout, stderr bytes.Buffer
	args := append(baseArgs(left.URL, right.URL, corpus), "-timeout=50ms")
	code := run(t.Context(), args, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailure, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "FAIL labels-slow") {
		t.Errorf("stderr = %q, want the corpus entry name", stderr.String())
	}
}

func TestRunCanceledContext(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "range-canceled", Type: queryRange, Query: `{}`}})
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var stdout, stderr bytes.Buffer
	code := run(ctx, baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
	}
}

func TestRunLabelValuesEscapesPath(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{
		{Name: "label-values", Type: queryLabelValues, Params: Params{"label": {"app/zone"}}},
	})
	left, leftRec := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":[]}`)
	})
	right, rightRec := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":[]}`)
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitEqual, stderr.String())
	}
	for _, rec := range []*recorder{leftRec, rightRec} {
		reqs := rec.all()
		if len(reqs) != 1 {
			t.Fatalf("request count = %d, want 1", len(reqs))
		}
		if !strings.Contains(reqs[0].requestURI, "/loki/api/v1/label/app%2Fzone/values") {
			t.Errorf("request URI = %q, want escaped label path", reqs[0].requestURI)
		}
		if reqs[0].path != "/loki/api/v1/label/app/zone/values" {
			t.Errorf("decoded path = %q", reqs[0].path)
		}
		if _, ok := reqs[0].query["label"]; ok {
			t.Error("label_values sent the label as a query parameter")
		}
	}
}

func TestRunVolumeInstantIgnoresProcessingTimestamp(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "volume", Type: queryVolume, Query: `{job="corpus"}`}})
	newVolumeServer := func(t *testing.T) *httptest.Server {
		srv, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1000"]}
]}}`)
		})
		return srv
	}
	left, right := newVolumeServer(t), newVolumeServer(t)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d; volume processing timestamps must not differ\nstdout:\n%s\nstderr:\n%s",
			code, exitEqual, stdout.String(), stderr.String())
	}
}

func TestRunMalformedResultShapes(t *testing.T) {
	tests := []struct {
		name  string
		kind  queryType
		query string
		bad   string
	}{
		{
			name:  "matrix missing metric",
			kind:  queryRange,
			query: `sum(rate({job="corpus"}[1m]))`,
			bad:   `{"status":"success","data":{"resultType":"matrix","result":[{"values":[]}]}}`,
		},
		{
			name:  "matrix missing values",
			kind:  queryRange,
			query: `sum(rate({job="corpus"}[1m]))`,
			bad:   `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{}}]}}`,
		},
		{
			name:  "vector missing metric",
			kind:  queryInstant,
			query: `sum(rate({job="corpus"}[1m]))`,
			bad:   `{"status":"success","data":{"resultType":"vector","result":[{"value":[1700000000,"1"]}]}}`,
		},
		{
			name:  "nonnumeric sample",
			kind:  queryInstant,
			query: `sum(rate({job="corpus"}[1m]))`,
			bad:   `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"abc"]}]}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corpus := writeCorpusFile(t, []corpusEntry{{Name: tc.name, Type: tc.kind, Query: tc.query}})
			left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, tc.bad)
			})
			right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, tc.bad)
			})
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d; malformed payloads must never compare equal\nstdout:\n%s\nstderr:\n%s",
					code, exitFailure, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "FAIL") {
				t.Errorf("stderr = %q, want a decode failure", stderr.String())
			}
		})
	}
}

func TestRunCanonicalEmptyResults(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{
		{Name: "empty-range", Type: queryRange, Query: `{job="corpus"}`},
		{Name: "empty-instant", Type: queryInstant, Query: `sum(count_over_time({job="corpus"}[5m]))`},
	})
	newServer := func(t *testing.T) *httptest.Server {
		srv, _ := newRecorderServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/loki/api/v1/query" {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
				return
			}
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
		})
		return srv
	}
	left, right := newServer(t), newServer(t)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d; explicit empty results are valid\nstdout:\n%s\nstderr:\n%s",
			code, exitEqual, stdout.String(), stderr.String())
	}
}

func TestRunNonFiniteSamplesRemainValid(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{
		{Name: "nan", Type: queryInstant, Query: `sum(rate({job="corpus"}[1m]))`},
	})
	newServer := func(t *testing.T) *httptest.Server {
		srv, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"level":"info"},"value":[1700000000,"NaN"]}
]}}`)
		})
		return srv
	}
	left, right := newServer(t), newServer(t)
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d; NaN is a valid numeric sample\nstdout:\n%s\nstderr:\n%s",
			code, exitEqual, stdout.String(), stderr.String())
	}
}

func TestRunCorpusOverridesBounds(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{
		{
			Name: "range-override", Type: queryRange, Query: `{}`,
			Params: Params{"start": {"1700000000000000000"}, "end": {"2023-11-14T22:13:21Z"}},
		},
	})
	left, leftRec := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitEqual, stderr.String())
	}
	req := leftRec.all()[0]
	if got, want := req.query.Get("start"), "2023-11-14T22:13:20Z"; got != want {
		t.Errorf("start = %q, want %q", got, want)
	}
	if got, want := req.query.Get("end"), "2023-11-14T22:13:21Z"; got != want {
		t.Errorf("end = %q, want %q", got, want)
	}
}

func TestRunHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), []string{"-h"}, &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("run -h exit = %d, want %d", code, exitEqual)
	}
	help := stderr.String()
	for _, want := range []string{
		"lokicompare compares", "label_values consumes params.label", "Exit codes",
		"-left", "-right", "-corpus", "-start", "-end", "-tenant", "-timeout",
		"range, instant, labels", "volume_range", "expect", "wall-clock",
		"bare {}", "docs/logql.md#stats-and-volume",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("help does not contain %q", want)
		}
	}
}

func TestRunInvalidInvocation(t *testing.T) {
	good := writeCorpusFile(t, []corpusEntry{{Name: "labels", Type: queryLabels}})
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing required flags",
			args:    []string{"-left", "http://example.com"},
			wantErr: "required",
		},
		{
			name:    "bad left URL",
			args:    []string{"-left", "ftp://example.com", "-right", "http://example.com", "-corpus", good, "-start", testStart, "-end", testEnd},
			wantErr: "-left",
		},
		{
			name:    "start after end",
			args:    []string{"-left", "http://example.com", "-right", "http://example.com", "-corpus", good, "-start", testEnd, "-end", testStart},
			wantErr: "after",
		},
		{
			name:    "bad tenant",
			args:    []string{"-left", "http://example.com", "-right", "http://example.com", "-corpus", good, "-start", testStart, "-end", testEnd, "-tenant", "not/valid"},
			wantErr: "tenant",
		},
		{
			name:    "bad precision",
			args:    []string{"-left", "http://example.com", "-right", "http://example.com", "-corpus", good, "-start", testStart, "-end", testEnd, "-timestamp-precision", "ms"},
			wantErr: "timestamp-precision",
		},
		{
			name:    "negative float tolerance",
			args:    []string{"-left", "http://example.com", "-right", "http://example.com", "-corpus", good, "-start", testStart, "-end", testEnd, "-float-abs", "-1"},
			wantErr: "float-abs",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tc.args, &stdout, &stderr)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d", code, exitFailure)
			}
			if tc.wantErr != "" && !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want containing %q", stderr.String(), tc.wantErr)
			}
		})
	}
}

func TestRunUnknownCorpusType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corpus.json")
	if err := os.WriteFile(path, []byte(`[{"name":"x","type":"nope"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), []string{
		"-left", "http://example.com", "-right", "http://example.com",
		"-corpus", path, "-start", testStart, "-end", testEnd,
	}, &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("run exit = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "unknown type") {
		t.Errorf("stderr = %q, want unknown type", stderr.String())
	}
}

// writeRawCorpus writes a corpus body verbatim so tests can exercise fields
// that the struct-based helper cannot express, such as malformed expectations.
func writeRawCorpus(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corpus.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// runWithServers runs the CLI against two response functions and returns the
// exit code, output buffers, and request counts.
func runWithServers(t *testing.T, corpus string, left, right http.HandlerFunc, extra ...string) (int, *bytes.Buffer, *bytes.Buffer, int, int) {
	t.Helper()
	leftSrv, leftRec := newRecorderServer(t, left)
	rightSrv, rightRec := newRecorderServer(t, right)
	var stdout, stderr bytes.Buffer
	args := append(baseArgs(leftSrv.URL, rightSrv.URL, corpus), extra...)
	code := run(t.Context(), args, &stdout, &stderr)
	return code, &stdout, &stderr, len(leftRec.all()), len(rightRec.all())
}

func TestRunStatsEstimatesInformational(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "stats", Type: queryStats, Query: `{job="corpus"}`}})
	left := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"streams":2,"chunks":1,"entries":10,"bytes":100}`)
	}
	right := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"streams":2,"chunks":9,"entries":11,"bytes":999}`)
	}
	code, stdout, stderr, _, _ := runWithServers(t, corpus, left, right)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitEqual, stdout.String(), stderr.String())
	}
	report := stdout.String()
	for _, want := range []string{"entries left=10 right=11", "bytes left=100 right=999", "chunks left=1 right=9"} {
		if !strings.Contains(report, want) {
			t.Errorf("stdout %q does not report %q", report, want)
		}
	}
}

func TestRunStatsStreamsInformational(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "stats", Type: queryStats, Query: `{job="corpus"}`}})
	left := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"streams":2,"chunks":1,"entries":10,"bytes":100}`)
	}
	right := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"streams":3,"chunks":1,"entries":10,"bytes":100}`)
	}
	code, stdout, stderr, _, _ := runWithServers(t, corpus, left, right)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitEqual, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "streams left=2 right=3") {
		t.Errorf("stdout = %q, want the streams counts in the informational note", stdout.String())
	}
}

func TestRunVolumeInstantTimestampValidation(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "volume", Type: queryVolume, Query: `{job="corpus"}`}})
	valid := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1000"]}
]}}`)
	}
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "stale",
			body: `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + secondsAt(time.Now().Add(-2*time.Minute)) + `,"1000"]}
]}}`,
			wantErr: "wall-clock window",
		},
		{
			name: "future",
			body: `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + secondsAt(time.Now().Add(2*time.Minute)) + `,"1000"]}
]}}`,
			wantErr: "wall-clock window",
		},
		{
			name: "string timestamp",
			body: `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":["1700000000","1000"]}
]}}`,
			wantErr: "numeric Unix seconds",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := func(w http.ResponseWriter, _ *http.Request) { writeResponse(w, http.StatusOK, tc.body) }
			code, _, stderr, _, _ := runWithServers(t, corpus, valid, bad)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want containing %q", stderr.String(), tc.wantErr)
			}
		})
	}
}

func TestRunVolumeInstantWindowAnchoredToRequestTime(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"volume","type":"volume","query":"{job=\"corpus\"}",
 "params":{"start":"2023-11-14T22:13:20Z","end":"2023-11-14T22:13:21Z"}}]`)
	historical := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	tests := []struct {
		name string
		ts   string
		code int
	}{
		{name: "request time", ts: nowSeconds(), code: exitEqual},
		{name: "historical query bound", ts: secondsAt(historical), code: exitFailure},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			respond := func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+tc.ts+`,"1000"]}
]}}`)
			}
			code, stdout, stderr, _, _ := runWithServers(t, corpus, respond, respond)
			if code != tc.code {
				t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, tc.code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunVolumeRangeBareGroupingInformational(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	g0, g1, g2 := secondsOfNS(grid[0]), secondsOfNS(grid[1]), secondsOfNS(grid[2])
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "volume-range", Type: queryVolumeRange, Query: `{}`}})
	left := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{},"values":[[`+g0+`,"10"],[`+g1+`,"20"]]}
]}}`)
	}
	right := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+g0+`,"4"],[`+g1+`,"8"]]},
 {"metric":{"service_name":"web"},"values":[[`+g0+`,"6"],[`+g1+`,"12"]]}
]}}`)
	}
	code, stdout, stderr, _, _ := runWithServers(t, corpus, left, right)
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d for bare {} grouping\nstdout:\n%s\nstderr:\n%s",
			code, exitEqual, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "informational grouping difference") {
		t.Errorf("stdout = %q, want an informational grouping note", stdout.String())
	}

	shifted := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+g1+`,"4"],[`+g2+`,"8"]]},
 {"metric":{"service_name":"web"},"values":[[`+g0+`,"6"],[`+g2+`,"12"]]}
]}}`)
	}
	code, stdout, _, _, _ = runWithServers(t, corpus, left, shifted)
	if code != exitDifferent {
		t.Fatalf("run exit = %d, want %d for a shifted global tick\nstdout:\n%s", code, exitDifferent, stdout.String())
	}
}

func TestRunVolumeRangeTimestampsExactWithMicrosecondFlag(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "volume-range", Type: queryVolumeRange, Query: `{job="corpus"}`}})
	respond := func(ts string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+ts+`,"1000"]}
]}}`)
		}
	}
	code, stdout, _, _, _ := runWithServers(t, corpus, respond(secondsOfNS(grid[0])), respond(secondsOfNS(grid[1])), "-timestamp-precision=us")
	if code != exitDifferent {
		t.Fatalf("run exit = %d, want %d under -timestamp-precision=us\nstdout:\n%s", code, exitDifferent, stdout.String())
	}
	code, _, stderr, _, _ := runWithServers(t, corpus, respond(secondsOfNS(grid[0])), respond(secondsOfNS(grid[0])), "-timestamp-precision=us")
	if code != exitEqual {
		t.Fatalf("run exit = %d, want %d for an identical grid\nstderr:\n%s", code, exitEqual, stderr.String())
	}
}

func TestRunVolumeShapeFailures(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	g0 := secondsOfNS(grid[0])
	corpusVolume := writeCorpusFile(t, []corpusEntry{{Name: "volume", Type: queryVolume, Query: `{job="corpus"}`}})
	corpusRange := writeCorpusFile(t, []corpusEntry{{Name: "volume-range", Type: queryVolumeRange, Query: `{job="corpus"}`}})
	corpusStats := writeCorpusFile(t, []corpusEntry{{Name: "stats", Type: queryStats, Query: `{job="corpus"}`}})
	tests := []struct {
		name    string
		corpus  string
		body    string
		wantErr string
	}{
		{
			name:   "volume missing stats",
			corpus: corpusVolume,
			body: `{"status":"success","data":{"resultType":"vector","result":[
 {"metric":{"service_name":"api"},"value":[` + nowSeconds() + `,"1000"]}
]}}`,
			wantErr: "data.stats",
		},
		{
			name:    "volume wrong result type",
			corpus:  corpusVolume,
			body:    `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[]}}`,
			wantErr: "want vector",
		},
		{
			name:   "volume duplicate series",
			corpus: corpusVolume,
			body: `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[` + nowSeconds() + `,"1"]},
 {"metric":{"service_name":"api"},"value":[` + nowSeconds() + `,"2"]}
]}}`,
			wantErr: "repeats series",
		},
		{
			name:    "volume_range wrong result type",
			corpus:  corpusRange,
			body:    `{"status":"success","data":{"resultType":"scalar","stats":{},"result":[1700000000.123,"1"]}}`,
			wantErr: "want vector or matrix",
		},
		{
			name:   "volume_range duplicate timestamp",
			corpus: corpusRange,
			body: `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[` + g0 + `,"1"],[` + g0 + `,"2"]]}
]}}`,
			wantErr: "repeats timestamp",
		},
		{
			name:    "stats missing field",
			corpus:  corpusStats,
			body:    `{"streams":1,"chunks":1,"entries":1}`,
			wantErr: "bytes",
		},
		{
			name:    "stats negative field",
			corpus:  corpusStats,
			body:    `{"streams":1,"chunks":1,"entries":-1,"bytes":1}`,
			wantErr: "negative",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := func(w http.ResponseWriter, _ *http.Request) { writeResponse(w, http.StatusOK, tc.body) }
			code, _, stderr, _, _ := runWithServers(t, tc.corpus, bad, bad)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d; identical malformed shapes must never pass\nstderr:\n%s",
					code, exitFailure, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want containing %q", stderr.String(), tc.wantErr)
			}
		})
	}
}

func TestRunVolumeNonBareGroupingFails(t *testing.T) {
	corpus := writeCorpusFile(t, []corpusEntry{{Name: "volume", Type: queryVolume, Query: `{service_name="api"}`}})
	left := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1000"]}
]}}`)
	}
	right := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"web"},"value":[`+nowSeconds()+`,"1000"]}
]}}`)
	}
	code, stdout, _, _, _ := runWithServers(t, corpus, left, right)
	if code != exitDifferent {
		t.Fatalf("run exit = %d, want %d for non-bare grouping\nstdout:\n%s", code, exitDifferent, stdout.String())
	}
}

func TestRunExpectations(t *testing.T) {
	t.Run("stats exact passes", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"stats","type":"stats","query":"{job=\"corpus\"}",
 "expect":{"stats":{"streams":2,"entries":10,"bytes":100}}}]`)
		respond := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"streams":2,"chunks":1,"entries":10,"bytes":100}`)
		}
		code, _, stderr, _, _ := runWithServers(t, corpus, respond, respond)
		if code != exitEqual {
			t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitEqual, stderr.String())
		}
	})
	t.Run("stats exact failure ignores left and float flags", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"stats","type":"stats","query":"{job=\"corpus\"}",
 "expect":{"stats":{"streams":2,"entries":10,"bytes":100}}}]`)
		left := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"streams":2,"chunks":1,"entries":10,"bytes":100}`)
		}
		right := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"streams":2,"chunks":1,"entries":10,"bytes":101}`)
		}
		code, stdout, _, _, _ := runWithServers(t, corpus, left, right, "-float-abs=1e9", "-float-rel=1e9")
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d; LEFT agreement or float tolerance hid a RIGHT mismatch\nstdout:\n%s",
				code, exitDifferent, stdout.String())
		}
		if !strings.Contains(stdout.String(), "expect stats bytes: expected=100 right=101") {
			t.Errorf("stdout = %q, want the exact bytes expectation failure", stdout.String())
		}
	})
	t.Run("instant volume exact rational", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"volume","type":"volume","query":"{job=\"corpus\"}",
 "expect":{"series":[{"metric":{"service_name":"api"},"value":1200}]}}]`)
		respond := func(value string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"`+value+`"]}
]}}`)
			}
		}
		code, _, stderr, _, _ := runWithServers(t, corpus, respond("1200"), respond("1200.0"))
		if code != exitEqual {
			t.Fatalf("run exit = %d, want %d; 1200 and 1200.0 are exactly equal\nstderr:\n%s", code, exitEqual, stderr.String())
		}
		code, stdout, _, _, _ := runWithServers(t, corpus, respond("1200"), respond("1201"), "-float-abs=1e9", "-float-rel=1e9")
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d; float flags hid an exact expectation mismatch\nstdout:\n%s",
				code, exitDifferent, stdout.String())
		}
	})
	t.Run("instant volume membership", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"volume","type":"volume","query":"{job=\"corpus\"}",
 "expect":{"series":[{"metric":{"service_name":"api"},"value":"1200"}]}}]`)
		extra := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1200"]},
 {"metric":{"service_name":"web"},"value":[`+nowSeconds()+`,"1"]}
]}}`)
		}
		code, stdout, _, _, _ := runWithServers(t, corpus, extra, extra)
		if code != exitDifferent || !strings.Contains(stdout.String(), "was not expected") {
			t.Fatalf("run exit = %d, want %d with an extra RIGHT series\nstdout:\n%s", code, exitDifferent, stdout.String())
		}
		missing := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[]}}`)
		}
		code, stdout, _, _, _ = runWithServers(t, corpus, missing, missing)
		if code != exitDifferent || !strings.Contains(stdout.String(), "missing on right") {
			t.Fatalf("run exit = %d, want %d with a missing expected series\nstdout:\n%s", code, exitDifferent, stdout.String())
		}
	})
	t.Run("volume_range exact tuples", func(t *testing.T) {
		grid := baseVolumeRangeGrid(t)
		g0, g1, g2 := secondsOfNS(grid[0]), secondsOfNS(grid[1]), secondsOfNS(grid[2])
		corpus := writeRawCorpus(t, fmt.Sprintf(`[{"name":"volume-range","type":"volume_range","query":"{job=\"corpus\"}",
 "expect":{"series":[{"metric":{"service_name":"api"},"values":[[%s,1200],[%s,2400]]}]}}]`, g0, g1))
		respond := func(body string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":`+body+`}}`)
			}
		}
		exact := `[{"metric":{"service_name":"api"},"values":[[` + g1 + `,2400],[` + g0 + `,1200]]}]`
		code, _, stderr, _, _ := runWithServers(t, corpus, respond(exact), respond(exact))
		if code != exitEqual {
			t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitEqual, stderr.String())
		}
		shifted := `[{"metric":{"service_name":"api"},"values":[[` + g2 + `,1200],[` + g1 + `,2400]]}]`
		code, stdout, _, _, _ := runWithServers(t, corpus, respond(exact), respond(shifted))
		if code != exitDifferent || !strings.Contains(stdout.String(), "missing on right") {
			t.Fatalf("run exit = %d, want %d for a shifted expected timestamp\nstdout:\n%s", code, exitDifferent, stdout.String())
		}
		changed := `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1201],[` + g1 + `,2400]]}]`
		code, stdout, _, _, _ = runWithServers(t, corpus, respond(exact), respond(changed), "-float-abs=1e9", "-float-rel=1e9")
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d; float flags hid an exact range expectation mismatch\nstdout:\n%s",
				code, exitDifferent, stdout.String())
		}
	})
}

func TestRunInvalidExpectationsMakeNoRequests(t *testing.T) {
	tests := []struct {
		name    string
		corpus  string
		wantErr string
	}{
		{
			name:    "null expect",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":null}]`,
			wantErr: "must not be null",
		},
		{
			name:    "empty expect",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{}}]`,
			wantErr: "must define stats",
		},
		{
			name:    "unknown expect field",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"counts":{}}}]`,
			wantErr: "unknown field",
		},
		{
			name:    "expect on range",
			corpus:  `[{"name":"x","type":"range","query":"{}","expect":{"stats":{"streams":1}}}]`,
			wantErr: "only valid for stats",
		},
		{
			name:    "stats with series",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":{"streams":1},"series":[{"metric":{},"value":1}]}}]`,
			wantErr: "must not define series",
		},
		{
			name:    "volume with stats",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"stats":{"streams":1}}}]`,
			wantErr: "must not define stats",
		},
		{
			name:    "volume series both forms",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":[{"metric":{},"value":1,"values":[[1,1]]}]}}]`,
			wantErr: "both value and values",
		},
		{
			name:    "volume series neither form",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":[{"metric":{}}]}}]`,
			wantErr: "needs value",
		},
		{
			name:    "volume_range with value",
			corpus:  `[{"name":"x","type":"volume_range","query":"{}","expect":{"series":[{"metric":{},"value":1}]}}]`,
			wantErr: "expects values",
		},
		{
			name:    "negative stats",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":{"streams":-1}}}]`,
			wantErr: "negative",
		},
		{
			name:    "fractional stats",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":{"streams":1.5}}}]`,
			wantErr: "integer",
		},
		{
			name:    "string stats",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":{"streams":"many"}}}]`,
			wantErr: "not a decimal number",
		},
		{
			name:    "volume metric missing",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":[{"value":1}]}}]`,
			wantErr: "missing metric",
		},
		{
			name:    "volume string timestamp",
			corpus:  `[{"name":"x","type":"volume_range","query":"{}","expect":{"series":[{"metric":{},"values":[["1700000000",1]]}]}}]`,
			wantErr: "numeric Unix seconds",
		},
		{
			name:    "duplicate expected metric",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":[{"metric":{"a":"b"},"value":1},{"metric":{"a":"b"},"value":2}]}}]`,
			wantErr: "duplicate metric",
		},
		{
			name:    "duplicate expected timestamp",
			corpus:  `[{"name":"x","type":"volume_range","query":"{}","expect":{"series":[{"metric":{},"values":[[1700000000,1],[1700000000,2]]}]}}]`,
			wantErr: "duplicate timestamp",
		},
		{
			name:    "overlong expected tuple",
			corpus:  `[{"name":"x","type":"volume_range","query":"{}","expect":{"series":[{"metric":{},"values":[[1700000000,1,2]]}]}}]`,
			wantErr: "[numeric seconds, value]",
		},
		{
			name:    "null stats",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":null}}]`,
			wantErr: "stats must not be null",
		},
		{
			name:    "stats with empty series",
			corpus:  `[{"name":"x","type":"stats","query":"{}","expect":{"stats":{"streams":1},"series":[]}}]`,
			wantErr: "must not define series",
		},
		{
			name:    "volume series null",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":null}}]`,
			wantErr: "series must not be null",
		},
		{
			name:    "unknown series field",
			corpus:  `[{"name":"x","type":"volume","query":"{}","expect":{"series":[{"metric":{},"value":1,"extra":2}]}}]`,
			wantErr: "unknown field",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corpus := writeRawCorpus(t, tc.corpus)
			respond := func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"streams":1,"chunks":1,"entries":1,"bytes":1}`)
			}
			code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus, respond, respond)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want containing %q", stderr.String(), tc.wantErr)
			}
			if leftReqs != 0 || rightReqs != 0 {
				t.Errorf("invalid expectation made requests: left=%d right=%d, want 0", leftReqs, rightReqs)
			}
		})
	}
}

func TestRunLegacyCorpusWithoutExpect(t *testing.T) {
	corpus := writeCorpusFile(t, equalCorpus(t))
	left, _ := newRecorderServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, http.StatusOK, responseFor(r.URL.Path, 0, baseVolumeRangeGrid(t)))
	})
	right, _ := newRecorderServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeResponse(w, http.StatusOK, responseFor(r.URL.Path, 1, baseVolumeRangeGrid(t)))
	})
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("legacy corpus exit = %d, want %d\nstdout:\n%s\nstderr:\n%s",
			code, exitEqual, stdout.String(), stderr.String())
	}
}

func TestRunEmptySeriesExpectation(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	for _, kind := range []queryType{queryVolume, queryVolumeRange} {
		t.Run(string(kind), func(t *testing.T) {
			corpus := writeRawCorpus(t, `[{"name":"empty","type":"`+string(kind)+`","query":"{}","expect":{"series":[]}}]`)
			// Loki's empty volume and volume_range results are both vectors.
			shape := "vector"
			empty := func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"`+shape+`","stats":{},"result":[]}}`)
			}
			code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus, empty, empty)
			if code != exitEqual || leftReqs != 1 || rightReqs != 1 {
				t.Fatalf("empty-series assertion: code=%d requests=%d/%d stderr=%s",
					code, leftReqs, rightReqs, stderr.String())
			}

			nonEmpty := func(w http.ResponseWriter, _ *http.Request) {
				if kind == queryVolume {
					writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1"]}
]}}`)
					return
				}
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":[
 {"metric":{"service_name":"api"},"values":[[`+secondsOfNS(grid[0])+`,"1"],[`+secondsOfNS(grid[1])+`,"2"]]}
]}}`)
			}
			code, stdout, _, _, _ := runWithServers(t, corpus, nonEmpty, nonEmpty)
			if code != exitDifferent || !strings.Contains(stdout.String(), "was not expected") {
				t.Fatalf("nonempty RIGHT for empty expectation: code=%d stdout=%s", code, stdout.String())
			}
		})
	}
}

func TestRunNonDecimalExpectationStringsMakeNoRequests(t *testing.T) {
	numbers := []string{"1/1", "0x1", "1_000", "NaN", "+1", " 1", "1.", ".5", "01", "1e", "1e+", "Inf", "-Inf"}
	for _, kind := range []queryType{queryStats, queryVolume, queryVolumeRange} {
		for _, number := range numbers {
			t.Run(string(kind)+"/"+number, func(t *testing.T) {
				var expect, body string
				switch kind {
				case queryStats:
					expect = `{"stats":{"streams":"` + number + `"}}`
					body = `{"streams":1,"entries":1,"chunks":1,"bytes":1}`
				case queryVolume:
					expect = `{"series":[{"metric":{},"value":"` + number + `"}]}`
					body = `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{},"value":[` + nowSeconds() + `,"1"]}
]}}`
				case queryVolumeRange:
					expect = `{"series":[{"metric":{},"values":[[1700000000,"` + number + `"]]}]}`
					body = `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{},"value":[1700000000,"1"]}
]}}`
				}
				corpus := writeRawCorpus(t, `[{"name":"decimal","type":"`+string(kind)+`","query":"{}","expect":`+expect+`}]`)
				respond := func(w http.ResponseWriter, _ *http.Request) {
					writeResponse(w, http.StatusOK, body)
				}
				code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus, respond, respond)
				if code != exitFailure || leftReqs != 0 || rightReqs != 0 {
					t.Fatalf("nondecimal expectation: code=%d requests=%d/%d stderr=%s",
						code, leftReqs, rightReqs, stderr.String())
				}
			})
		}
	}
}

func TestRunVolumeInformationalValuesAlwaysReported(t *testing.T) {
	t.Run("instant unmatched groups", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"bare","type":"volume","query":"{}"}]`)
		respond := func(metric, value string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":`+metric+`,"value":[`+nowSeconds()+`,"`+value+`"]}
]}}`)
			}
		}
		code, stdout, stderr, _, _ := runWithServers(t, corpus, respond(`{}`, "11111"), respond(`{"service_name":"api"}`, "22222"))
		if code != exitEqual {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		for _, want := range []string{"11111", "22222", "<missing>"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("missing informational value %q: %s", want, stdout.String())
			}
		}
	})
	t.Run("range unmatched groups", func(t *testing.T) {
		grid := baseVolumeRangeGrid(t)
		corpus := writeRawCorpus(t, `[{"name":"bare","type":"volume_range","query":"{}"}]`)
		respond := func(metric, value string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":`+metric+`,"value":[`+secondsOfNS(grid[0])+`,"`+value+`"]}
]}}`)
			}
		}
		code, stdout, stderr, _, _ := runWithServers(t, corpus, respond(`{}`, "11111"), respond(`{"service_name":"api"}`, "22222"))
		if code != exitEqual {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		for _, want := range []string{"11111", "22222", "<missing>"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("missing informational value %q: %s", want, stdout.String())
			}
		}
	})
	t.Run("large integers stay distinct", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"volume","type":"volume","query":"{service_name=\"api\"}"}]`)
		respond := func(value string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"`+value+`"]}
]}}`)
			}
		}
		// 2^53 and 2^53+1 collapse to the same float64; the report must keep
		// both exact integer spellings.
		code, stdout, stderr, _, _ := runWithServers(t, corpus, respond("9007199254740992"), respond("9007199254740993"))
		if code != exitEqual {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		for _, want := range []string{"9007199254740992", "9007199254740993"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("missing exact large value %q: %s", want, stdout.String())
			}
		}
	})
	t.Run("equal estimates side by side", func(t *testing.T) {
		corpus := writeRawCorpus(t, `[{"name":"volume","type":"volume","query":"{service_name=\"api\"}"}]`)
		respond := func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","stats":{},"result":[
 {"metric":{"service_name":"api"},"value":[`+nowSeconds()+`,"1000"]}
]}}`)
		}
		code, stdout, stderr, _, _ := runWithServers(t, corpus, respond, respond)
		if code != exitEqual {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), `left="1000" right="1000"`) {
			t.Errorf("equal estimates were not reported side by side: %s", stdout.String())
		}
	})
}

// writeErrorResponse writes a Loki-style plain-text error body, as produced by
// http.Error.
func writeErrorResponse(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func TestRunExpectedStatus(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"instant-log","type":"instant","query":"{service_name=\"api\"}",
 "expect":{"status":400}}]`)
	textError := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			writeErrorResponse(w, status, body)
		}
	}
	validSuccess := func(w http.ResponseWriter, _ *http.Request) {
		writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}

	t.Run("different message wording passes", func(t *testing.T) {
		left := textError(http.StatusBadRequest, "log queries are not allowed as instant queries")
		right := textError(http.StatusBadRequest, "unsupported syntax\r\nfor instant query\tdetail")
		code, stdout, stderr, leftReqs, rightReqs := runWithServers(t, corpus, left, right)
		if code != exitEqual {
			t.Fatalf("run exit = %d, want %d; message wording must not be compared\nstdout:\n%s\nstderr:\n%s",
				code, exitEqual, stdout.String(), stderr.String())
		}
		if leftReqs != 1 || rightReqs != 1 {
			t.Errorf("request counts left=%d right=%d, want 1 each", leftReqs, rightReqs)
		}
	})
	t.Run("left wrong status", func(t *testing.T) {
		left := textError(http.StatusInternalServerError, "boom")
		right := textError(http.StatusBadRequest, "unsupported syntax for instant query")
		code, stdout, stderr, leftReqs, rightReqs := runWithServers(t, corpus, left, right)
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitDifferent, stdout.String(), stderr.String())
		}
		if !strings.Contains(stdout.String(), "left HTTP 500") {
			t.Errorf("stdout = %q, want the left status difference", stdout.String())
		}
		if leftReqs != 1 || rightReqs != 1 {
			t.Errorf("request counts left=%d right=%d, want 1 each even on mismatch", leftReqs, rightReqs)
		}
	})
	t.Run("right wrong status", func(t *testing.T) {
		left := textError(http.StatusBadRequest, "unsupported syntax for instant query")
		right := textError(http.StatusUnprocessableEntity, "boom")
		code, stdout, _, _, _ := runWithServers(t, corpus, left, right)
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d\nstdout:\n%s", code, exitDifferent, stdout.String())
		}
		if !strings.Contains(stdout.String(), "right HTTP 422") {
			t.Errorf("stdout = %q, want the right status difference", stdout.String())
		}
	})
	t.Run("success on left is a difference", func(t *testing.T) {
		right := textError(http.StatusBadRequest, "unsupported syntax for instant query")
		code, stdout, stderr, _, rightReqs := runWithServers(t, corpus, validSuccess, right)
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d; a valid 200 success body must not count\nstdout:\n%s\nstderr:\n%s",
				code, exitDifferent, stdout.String(), stderr.String())
		}
		if !strings.Contains(stdout.String(), "left HTTP 200") {
			t.Errorf("stdout = %q, want the left status difference", stdout.String())
		}
		if rightReqs != 1 {
			t.Errorf("right requests = %d, want 1; right must be fetched even when left mismatches", rightReqs)
		}
	})
	t.Run("success on right is a difference", func(t *testing.T) {
		left := textError(http.StatusBadRequest, "unsupported syntax for instant query")
		code, stdout, _, _, _ := runWithServers(t, corpus, left, validSuccess)
		if code != exitDifferent {
			t.Fatalf("run exit = %d, want %d\nstdout:\n%s", code, exitDifferent, stdout.String())
		}
		if !strings.Contains(stdout.String(), "right HTTP 200") {
			t.Errorf("stdout = %q, want the right status difference", stdout.String())
		}
	})
	t.Run("other expected status", func(t *testing.T) {
		corpus422 := writeRawCorpus(t, `[{"name":"teapot","type":"instant","query":"{service_name=\"api\"}",
 "expect":{"status":422}}]`)
		respond := textError(http.StatusUnprocessableEntity, "unprocessable")
		code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus422, respond, respond)
		if code != exitEqual {
			t.Fatalf("run exit = %d, want %d for expected 422\nstderr:\n%s", code, exitEqual, stderr.String())
		}
		if leftReqs != 1 || rightReqs != 1 {
			t.Errorf("request counts left=%d right=%d, want 1 each", leftReqs, rightReqs)
		}
	})
	t.Run("integer-valued spellings and boundaries", func(t *testing.T) {
		for _, tc := range []struct {
			spelling string
			status   int
		}{
			{spelling: "400.0", status: http.StatusBadRequest},
			{spelling: "4e2", status: http.StatusBadRequest},
			{spelling: "599", status: 599},
		} {
			corpus := writeRawCorpus(t, `[{"name":"x","type":"instant","query":"{a=\"b\"}",
 "expect":{"status":`+tc.spelling+`}}]`)
			respond := textError(tc.status, "rejected")
			code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus, respond, respond)
			if code != exitEqual {
				t.Fatalf("expect status %s: run exit = %d, want %d\nstderr:\n%s",
					tc.spelling, code, exitEqual, stderr.String())
			}
			if leftReqs != 1 || rightReqs != 1 {
				t.Errorf("expect status %s: request counts left=%d right=%d, want 1 each",
					tc.spelling, leftReqs, rightReqs)
			}
		}
	})
	t.Run("media type and parameters accepted", func(t *testing.T) {
		for _, contentType := range []string{
			"text/plain",
			"Text/Plain; charset=\"utf-8\"",
			"text/plain; charset=UTF-8; boundary=x",
		} {
			respond := func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", contentType)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("rejected"))
			}
			code, _, stderr, _, _ := runWithServers(t, corpus, respond, respond)
			if code != exitEqual {
				t.Fatalf("Content-Type %q: run exit = %d, want %d\nstderr:\n%s",
					contentType, code, exitEqual, stderr.String())
			}
		}
	})
	t.Run("endpoint and name in report", func(t *testing.T) {
		left := textError(http.StatusInternalServerError, "boom")
		right := textError(http.StatusBadRequest, "unsupported syntax for instant query")
		_, stdout, _, _, _ := runWithServers(t, corpus, left, right)
		for _, want := range []string{"DIFF instant-log", "/loki/api/v1/query"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("stdout %q does not contain %q", stdout.String(), want)
			}
		}
	})
}

func TestRunExpectedStatusInvalidBodies(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"instant-log","type":"instant","query":"{service_name=\"api\"}",
 "expect":{"status":400}}]`)
	valid := func(w http.ResponseWriter, _ *http.Request) {
		writeErrorResponse(w, http.StatusBadRequest, "unsupported syntax for instant query")
	}
	tests := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{name: "empty body", contentType: "text/plain; charset=utf-8", body: nil},
		{name: "whitespace body", contentType: "text/plain", body: []byte("  \n\t ")},
		{name: "JSON content type", contentType: "application/json", body: []byte(`{"status":"error"}`)},
		{name: "HTML content type", contentType: "text/html; charset=utf-8", body: []byte("<html>bad</html>")},
		{name: "invalid UTF-8", contentType: "text/plain", body: []byte{0xff, 0xfe, 'x'}},
		{name: "NUL byte body", contentType: "text/plain", body: []byte{'b', 'a', 'd', 0x00}},
		{name: "control byte body", contentType: "text/plain", body: []byte{'b', 'a', 'd', 0x07}},
		{name: "empty content type", contentType: "", body: []byte("bad")},
		{name: "unparseable content type", contentType: ";;;", body: []byte("bad")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := func(w http.ResponseWriter, _ *http.Request) {
				if tc.contentType != "" {
					w.Header().Set("Content-Type", tc.contentType)
				} else {
					w.Header()["Content-Type"] = nil
				}
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(tc.body)
			}
			for _, side := range []string{"left", "right"} {
				t.Run(side, func(t *testing.T) {
					left, right := valid, valid
					if side == "left" {
						left = bad
					} else {
						right = bad
					}
					code, stdout, stderr, leftReqs, rightReqs := runWithServers(t, corpus, left, right)
					if code != exitDifferent {
						t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s",
							code, exitDifferent, stdout.String(), stderr.String())
					}
					if !strings.Contains(stdout.String(), side+" body") {
						t.Errorf("stdout = %q, want a %s body difference", stdout.String(), side)
					}
					if leftReqs != 1 || rightReqs != 1 {
						t.Errorf("request counts left=%d right=%d, want 1 each", leftReqs, rightReqs)
					}
				})
			}
		})
	}
}

func TestRunExpectedStatusAllTypes(t *testing.T) {
	params := map[queryType]string{
		queryRange:       `"query":"{job=\"a\"}"`,
		queryInstant:     `"query":"{job=\"a\"}"`,
		queryLabels:      `"query":"{job=\"a\"}"`,
		queryLabelValues: `"query":"{job=\"a\"}","params":{"label":"service_name"}`,
		querySeries:      `"query":"{job=\"a\"}"`,
		queryStats:       `"query":"{job=\"a\"}"`,
		queryVolume:      `"query":"{job=\"a\"}"`,
		queryVolumeRange: `"query":"{job=\"a\"}"`,
	}
	for _, kind := range queryTypeList() {
		t.Run(string(kind), func(t *testing.T) {
			corpus := writeRawCorpus(t, `[{"name":"x","type":"`+string(kind)+`",`+
				params[kind]+`,"expect":{"status":400}}]`)
			respond := func(w http.ResponseWriter, _ *http.Request) {
				writeErrorResponse(w, http.StatusBadRequest, "rejected")
			}
			code, stdout, stderr, leftReqs, rightReqs := runWithServers(t, corpus, respond, respond)
			if code != exitEqual {
				t.Fatalf("run exit = %d, want %d; expect.status is valid on every type\nstdout:\n%s\nstderr:\n%s",
					code, exitEqual, stdout.String(), stderr.String())
			}
			if leftReqs != 1 || rightReqs != 1 {
				t.Errorf("request counts left=%d right=%d, want 1 each", leftReqs, rightReqs)
			}
		})
	}
}

func TestRunExpectedStatusTransportFailures(t *testing.T) {
	corpus := writeRawCorpus(t, `[{"name":"instant-log","type":"instant","query":"{service_name=\"api\"}",
 "expect":{"status":400}}]`)
	valid := func(w http.ResponseWriter, _ *http.Request) {
		writeErrorResponse(w, http.StatusBadRequest, "unsupported syntax for instant query")
	}

	t.Run("left unreachable still queries right", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		dead.Close()
		right, rightRec := newRecorderServer(t, valid)
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), baseArgs(dead.URL, right.URL, corpus), &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailure, stdout.String(), stderr.String())
		}
		if got := len(rightRec.all()); got != 1 {
			t.Errorf("right requests = %d, want 1; right must be attempted even when left fails", got)
		}
	})
	t.Run("right unreachable is a failure", func(t *testing.T) {
		left, leftRec := newRecorderServer(t, valid)
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		dead.Close()
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), baseArgs(left.URL, dead.URL, corpus), &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFailure, stdout.String(), stderr.String())
		}
		if got := len(leftRec.all()); got != 1 {
			t.Errorf("left requests = %d, want 1", got)
		}
	})
	t.Run("left status mismatch right transport failure exits 2", func(t *testing.T) {
		wrong := func(w http.ResponseWriter, _ *http.Request) {
			writeErrorResponse(w, http.StatusInternalServerError, "boom")
		}
		left, leftRec := newRecorderServer(t, wrong)
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		dead.Close()
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), baseArgs(left.URL, dead.URL, corpus), &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d; transport failure wins over a status mismatch\nstdout:\n%s\nstderr:\n%s",
				code, exitFailure, stdout.String(), stderr.String())
		}
		if got := len(leftRec.all()); got != 1 {
			t.Errorf("left requests = %d, want 1", got)
		}
	})
	t.Run("left timeout still queries right", func(t *testing.T) {
		left, _ := newRecorderServer(t, func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})
		right, rightRec := newRecorderServer(t, valid)
		var stdout, stderr bytes.Buffer
		args := append(baseArgs(left.URL, right.URL, corpus), "-timeout=50ms")
		code := run(t.Context(), args, &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
		}
		if got := len(rightRec.all()); got != 1 {
			t.Errorf("right requests = %d, want 1", got)
		}
	})
	t.Run("truncated left body still queries right", func(t *testing.T) {
		left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Skip("response writer cannot hijack")
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = buf.WriteString("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 100\r\n\r\nshort")
			_ = buf.Flush()
			_ = conn.Close()
		})
		right, rightRec := newRecorderServer(t, valid)
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d for a truncated body\nstderr:\n%s", code, exitFailure, stderr.String())
		}
		if got := len(rightRec.all()); got != 1 {
			t.Errorf("right requests = %d, want 1", got)
		}
	})
	t.Run("oversized left body still queries right", func(t *testing.T) {
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		left, _ := newRecorderServer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusBadRequest)
			for range maxBodyBytes/(1<<20) + 1 {
				_, _ = w.Write(chunk)
			}
		})
		right, rightRec := newRecorderServer(t, valid)
		var stdout, stderr bytes.Buffer
		code := run(t.Context(), baseArgs(left.URL, right.URL, corpus), &stdout, &stderr)
		if code != exitFailure {
			t.Fatalf("run exit = %d, want %d for an oversized body\nstderr:\n%s", code, exitFailure, stderr.String())
		}
		if got := len(rightRec.all()); got != 1 {
			t.Errorf("right requests = %d, want 1", got)
		}
	})
}

func TestRunInvalidStatusExpectationsMakeNoRequests(t *testing.T) {
	tests := []struct {
		name    string
		expect  string
		wantErr string
	}{
		{name: "null", expect: `{"status":null}`, wantErr: "must not be null"},
		{name: "string", expect: `{"status":"400"}`, wantErr: "JSON integer"},
		{name: "bool", expect: `{"status":true}`, wantErr: "JSON integer"},
		{name: "object", expect: `{"status":{}}`, wantErr: "JSON integer"},
		{name: "array", expect: `{"status":[400]}`, wantErr: "JSON integer"},
		{name: "fraction", expect: `{"status":400.5}`, wantErr: "integer"},
		{name: "below range", expect: `{"status":399}`, wantErr: "[400, 599]"},
		{name: "above range", expect: `{"status":600}`, wantErr: "[400, 599]"},
		{name: "success status", expect: `{"status":200}`, wantErr: "[400, 599]"},
		{name: "with stats", expect: `{"status":400,"stats":{"streams":1}}`, wantErr: "must not be combined"},
		{name: "with empty series", expect: `{"status":400,"series":[]}`, wantErr: "must not be combined"},
		{name: "with series", expect: `{"status":400,"series":[{"metric":{},"value":1}]}`, wantErr: "must not be combined"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corpus := writeRawCorpus(t, `[{"name":"x","type":"stats","query":"{}","expect":`+tc.expect+`}]`)
			respond := func(w http.ResponseWriter, _ *http.Request) {
				writeErrorResponse(w, http.StatusBadRequest, "bad")
			}
			code, _, stderr, leftReqs, rightReqs := runWithServers(t, corpus, respond, respond)
			if code != exitFailure {
				t.Fatalf("run exit = %d, want %d\nstderr:\n%s", code, exitFailure, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Errorf("stderr = %q, want containing %q", stderr.String(), tc.wantErr)
			}
			if leftReqs != 0 || rightReqs != 0 {
				t.Errorf("invalid status expectation made requests: left=%d right=%d, want 0", leftReqs, rightReqs)
			}
		})
	}
}

func TestRunExactRangeExpectationMembership(t *testing.T) {
	grid := baseVolumeRangeGrid(t)
	g0, g1, g2 := secondsOfNS(grid[0]), secondsOfNS(grid[1]), secondsOfNS(grid[2])
	corpus := writeRawCorpus(t, fmt.Sprintf(`[{"name":"volume-range","type":"volume_range","query":"{job=\"corpus\"}",
 "expect":{"series":[{"metric":{"service_name":"api"},"values":[[%s,1200],[%s,2400]]}]}}]`, g0, g1))
	respond := func(result string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"matrix","stats":{},"result":`+result+`}}`)
		}
	}
	exact := `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200],[` + g1 + `,2400]]}]`
	tests := []struct {
		name    string
		shape   string
		result  string
		wantErr string
	}{
		{
			name:    "missing expected series",
			shape:   "vector",
			result:  `[]`,
			wantErr: "missing on right",
		},
		{
			name:    "extra right series",
			shape:   "matrix",
			result:  `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200],[` + g1 + `,2400]]},{"metric":{"service_name":"web"},"values":[[` + g0 + `,1],[` + g1 + `,2]]}]`,
			wantErr: "was not expected",
		},
		{
			// A one-bucket volume_range result is a vector, not a matrix.
			name:    "missing expected tick",
			shape:   "vector",
			result:  `[{"metric":{"service_name":"api"},"value":[` + g0 + `,1200]}]`,
			wantErr: "missing on right",
		},
		{
			name:    "extra right tick",
			shape:   "matrix",
			result:  `[{"metric":{"service_name":"api"},"values":[[` + g0 + `,1200],[` + g1 + `,2400],[` + g2 + `,3600]]}]`,
			wantErr: "was not expected",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			right := func(w http.ResponseWriter, _ *http.Request) {
				writeResponse(w, http.StatusOK, `{"status":"success","data":{"resultType":"`+tc.shape+`","stats":{},"result":`+tc.result+`}}`)
			}
			code, stdout, stderr, _, _ := runWithServers(t, corpus, respond(exact), right)
			if code != exitDifferent || !strings.Contains(stdout.String(), tc.wantErr) {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
}
