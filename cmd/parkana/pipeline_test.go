package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestPipelineConfigDefaultsMatchRegistry guards the duplicated defaults:
// config cannot import pipeline (pipeline imports ingest, which imports
// config), so the two constants must stay equal.
func TestPipelineConfigDefaultsMatchRegistry(t *testing.T) {
	if config.DefaultPipelinePollInterval != pipeline.DefaultPollInterval {
		t.Errorf("config poll default %s != pipeline default %s",
			config.DefaultPipelinePollInterval, pipeline.DefaultPollInterval)
	}
	if config.DefaultPipelineReloadTimeout != pipeline.DefaultReloadTimeout {
		t.Errorf("config reload default %s != pipeline default %s",
			config.DefaultPipelineReloadTimeout, pipeline.DefaultReloadTimeout)
	}
}

// TestPipelineCompileOptions checks that the wiring feeds the configured
// routing keys and line bound into every compile, so a kept transform cannot
// produce a line the push handler would reject.
func TestPipelineCompileOptions(t *testing.T) {
	cfg := assemblyConfig(t)
	cfg.Ingest.MaxLineSize = 1234
	cfg.StreamLabels = []string{"service_name", "job"}
	pipes, err := newPipelineWiring(cfg, slog.New(slog.DiscardHandler), catalog.NewMemory(), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	opts := pipes.compileOptions()
	if opts.LineBytes != 1234 {
		t.Errorf("LineBytes = %d, want 1234", opts.LineBytes)
	}
	if got := strings.Join(opts.StreamLabels, ","); got != "service_name,job" {
		t.Errorf("StreamLabels = %q", got)
	}
}

// syncLog is a concurrent-safe writer for run's log output.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// waitFor polls until the log contains want, returning the full text. It only
// reports the first line that carries the match.
func (l *syncLog) waitFor(t *testing.T, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		text := l.String()
		if i := strings.Index(text, want); i >= 0 {
			end := strings.IndexByte(text[i:], '\n')
			if end < 0 {
				end = len(text) - i
			}
			return text[i : i+end]
		}
		if time.Now().After(deadline) {
			t.Fatalf("log lacks %q within %s:\n%s", want, timeout, text)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// liveRun is one run() invocation serving HTTP on an ephemeral port.
type liveRun struct {
	url  string
	logs *syncLog

	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
	stopErr  error
}

// startLive starts the real binary wiring in a goroutine and waits until the
// server accepts requests (and, when present, its readiness checks pass). The
// cancel/join cleanup is installed before any startup log is awaited, so a
// startup failure still stops and joins the process.
func startLive(t *testing.T, args ...string) *liveRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	logs := &syncLog{}
	done := make(chan error, 1)
	lr := &liveRun{logs: logs, cancel: cancel, done: done}
	t.Cleanup(func() { lr.stop(t) })
	full := append([]string{"-http.addr=127.0.0.1:0", "-data.dir=" + t.TempDir()}, args...)
	go func() { done <- run(ctx, full, func(string) string { return "" }, logs) }()
	line := logs.waitFor(t, "http server listening", 30*time.Second)
	addr, ok := strings.CutPrefix(line[strings.Index(line, "http server listening"):], "http server listening")
	if !ok {
		t.Fatalf("unparsable listening line %q", line)
	}
	_, rest, ok := strings.Cut(addr, "addr=")
	if !ok {
		t.Fatalf("listening line has no addr: %q", line)
	}
	host, _, _ := strings.Cut(rest, " ")
	lr.url = "http://" + host
	lr.waitReady(t)
	return lr
}

func (l *liveRun) stop(t *testing.T) error {
	t.Helper()
	l.stopOnce.Do(func() {
		l.cancel()
		select {
		case l.stopErr = <-l.done:
		case <-time.After(90 * time.Second):
			l.stopErr = errors.New("run did not stop within 90s")
		}
	})
	if l.stopErr != nil {
		t.Errorf("run returned %v\n%s", l.stopErr, l.logs.String())
	}
	return l.stopErr
}

func (l *liveRun) waitReady(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.url+"/ready", nil)
		if err == nil {
			resp, rerr := client.Do(req)
			if rerr == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					cancel()
					return
				}
			}
		}
		cancel()
		if time.Now().After(deadline) {
			t.Fatalf("server never became ready\n%s", l.logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// request performs one HTTP request with an optional JSON body and returns the
// status and response bytes.
func request(t *testing.T, method, rawurl string, body any) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	reqCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, rawurl, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawurl, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, b
}

// lokiResponse is the subset of the Loki query response these tests read.
type lokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string  `json:"stream"`
			Values [][]jsontext.Value `json:"values"`
			Value  []jsontext.Value   `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func decodeLoki(t *testing.T, body []byte) lokiResponse {
	t.Helper()
	var r lokiResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decode query response: %v\n%s", err, body)
	}
	if r.Status != "success" {
		t.Fatalf("query status %q:\n%s", r.Status, body)
	}
	return r
}

// streamLines returns every returned log line, in response order.
func streamLines(t *testing.T, r lokiResponse) []string {
	t.Helper()
	var out []string
	for _, res := range r.Data.Result {
		for _, v := range res.Values {
			if len(v) < 2 {
				t.Fatalf("entry has %d fields: %s", len(v), v)
			}
			var line string
			if err := json.Unmarshal(v[1], &line); err != nil {
				t.Fatalf("decode line: %v", err)
			}
			out = append(out, line)
		}
	}
	return out
}

// streamLabels returns the merged label maps of the streams in a log result.
func streamLabels(r lokiResponse) []map[string]string {
	out := make([]map[string]string, 0, len(r.Data.Result))
	for _, res := range r.Data.Result {
		out = append(out, res.Stream)
	}
	return out
}

func queryRange(t *testing.T, lr *liveRun, expr string, start, end time.Time) lokiResponse {
	t.Helper()
	v := url.Values{}
	v.Set("query", expr)
	v.Set("start", start.UTC().Format(time.RFC3339Nano))
	v.Set("end", end.UTC().Format(time.RFC3339Nano))
	code, body := request(t, http.MethodGet, lr.url+"/loki/api/v1/query_range?"+v.Encode(), nil)
	if code != http.StatusOK {
		t.Fatalf("query_range %q: %d\n%s", expr, code, body)
	}
	return decodeLoki(t, body)
}

// instantQuery runs one instant metric query and returns its scalar value.
func instantQuery(t *testing.T, lr *liveRun, expr string, at time.Time) float64 {
	t.Helper()
	v := url.Values{}
	v.Set("query", expr)
	v.Set("time", at.UTC().Format(time.RFC3339Nano))
	code, body := request(t, http.MethodGet, lr.url+"/loki/api/v1/query?"+v.Encode(), nil)
	if code != http.StatusOK {
		t.Fatalf("query %q: %d\n%s", expr, code, body)
	}
	r := decodeLoki(t, body)
	if len(r.Data.Result) != 1 || len(r.Data.Result[0].Value) != 2 {
		t.Fatalf("query %q returned %d series:\n%s", expr, len(r.Data.Result), body)
	}
	var raw string
	if err := json.Unmarshal(r.Data.Result[0].Value[1], &raw); err != nil {
		t.Fatalf("decode value: %v", err)
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse value %q: %v", raw, err)
	}
	return f
}

func pushRequest(t *testing.T, lr *liveRun, stream map[string]string, ts time.Time, line string) (int, []byte) {
	t.Helper()
	body := map[string]any{"streams": []any{map[string]any{
		"stream": stream,
		"values": [][2]string{{strconv.FormatInt(ts.UnixNano(), 10), line}},
	}}}
	return request(t, http.MethodPost, lr.url+"/loki/api/v1/push", body)
}

func pushLine(t *testing.T, lr *liveRun, stream map[string]string, ts time.Time, line string) {
	t.Helper()
	code, resp := pushRequest(t, lr, stream, ts, line)
	if code != http.StatusNoContent {
		t.Fatalf("push: %d\n%s", code, resp)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if f() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pipelineDoc is a create/replace request body.
func pipelineDoc(name, stage, selector string, version int64, steps ...pipeline.Step) map[string]any {
	doc := map[string]any{
		"name":     name,
		"stage":    stage,
		"selector": selector,
		"steps":    steps,
	}
	if version != 0 {
		doc["version"] = version
	}
	return doc
}

func TestPipelineRoleStartup(t *testing.T) {
	fast := []string{"-pipeline.poll-interval=50ms", "-pipeline.reload-timeout=2s"}
	for _, tc := range []struct {
		role     string
		wantAPI  int
		services []string
	}{
		{"ingester", http.StatusOK, []string{"ingester"}},
		{"querier", http.StatusOK, []string{"querier"}},
		{"all", http.StatusOK, []string{"ingester", "querier", "compactor"}},
		{"compactor", http.StatusNotFound, []string{"compactor"}},
	} {
		t.Run(tc.role, func(t *testing.T) {
			args := append([]string{"-roles=" + tc.role}, fast...)
			lr := startLive(t, args...)
			code, body := request(t, http.MethodGet, lr.url+"/api/v1/pipelines", nil)
			if code != tc.wantAPI {
				t.Fatalf("pipeline API on %s = %d, want %d:\n%s", tc.role, code, tc.wantAPI, body)
			}
			if tc.wantAPI == http.StatusNotFound {
				// A compactor-only process must not register pipeline metrics.
				_, metrics := request(t, http.MethodGet, lr.url+"/metrics", nil)
				if bytes.Contains(metrics, []byte("parkana_pipeline_")) {
					t.Fatalf("compactor-only process registered pipeline metrics:\n%s", metrics)
				}
			}
			if err := lr.stop(t); err != nil {
				t.Fatalf("stop %s: %v", tc.role, err)
			}
			for _, svc := range tc.services {
				if !strings.Contains(lr.logs.String(), `msg="service stopped" service=`+svc) {
					t.Errorf("clean stop not logged for %s:\n%s", svc, lr.logs.String())
				}
			}
		})
	}
}

// TestPipelineEndToEnd exercises the wired process: the API, an ingest
// pipeline before durable append, a read pipeline over real uploaded Parquet,
// read filter/aggregate semantics, stored metadata routes and dry-run.
func TestPipelineEndToEnd(t *testing.T) {
	lr := startLive(t,
		"-roles=all",
		"-pipeline.poll-interval=50ms",
		"-pipeline.reload-timeout=2s",
		"-ingest.flush-interval=20ms",
		"-ingest.upload-interval=20ms",
		"-ingest.upload-max-age=1ms",
		"-ingest.upload-min=1B",
		"-ingest.memtable-max=64KiB",
		"-ingest.wal-max=64KiB",
		"-querier.timeout=30s",
	)

	// Create the ingest pipeline through the real route behind tenant
	// middleware.
	code, body := request(t, http.MethodPost, lr.url+"/api/v1/pipelines", pipelineDoc(
		"ingest-extract", "ingest", "checkout", 0,
		pipeline.Step{Type: pipeline.StepParseJSON},
		pipeline.Step{Type: pipeline.StepRename, From: "msg", To: "message"},
	))
	if code != http.StatusCreated {
		t.Fatalf("create ingest pipeline: %d\n%s", code, body)
	}
	// The remaining CRUD verbs are mounted exactly once as well.
	if code, body := request(t, http.MethodGet, lr.url+"/api/v1/pipelines", nil); code != http.StatusOK || !bytes.Contains(body, []byte(`"ingest-extract"`)) {
		t.Fatalf("list pipelines: %d\n%s", code, body)
	}
	if code, body := request(t, http.MethodGet, lr.url+"/api/v1/pipelines/ingest-extract", nil); code != http.StatusOK {
		t.Fatalf("get pipeline: %d\n%s", code, body)
	}
	if code, body := request(t, http.MethodPut, lr.url+"/api/v1/pipelines/ingest-extract", pipelineDoc(
		"ingest-extract", "ingest", "checkout", 1,
		pipeline.Step{Type: pipeline.StepParseJSON},
		pipeline.Step{Type: pipeline.StepRename, From: "msg", To: "message"},
	)); code != http.StatusOK {
		t.Fatalf("replace pipeline: %d\n%s", code, body)
	}
	// A second create for the same name is a version conflict, proving one
	// live registry write path (not a duplicate route stack).
	if code, _ := request(t, http.MethodPost, lr.url+"/api/v1/pipelines", pipelineDoc(
		"ingest-extract", "ingest", "checkout", 0,
		pipeline.Step{Type: pipeline.StepParseJSON},
	)); code != http.StatusConflict {
		t.Fatalf("duplicate create = %d, want 409", code)
	}

	ts := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	line := `{"msg":"hello","user":{"id":7}}`
	pushLine(t, lr, map[string]string{"service_name": "checkout"}, ts, line)

	start, end := ts.Add(-2*time.Minute), ts.Add(2*time.Minute)
	var lines []string
	waitFor(t, 20*time.Second, "pushed row to become queryable", func() bool {
		lines = streamLines(t, queryRange(t, lr, `{service_name="checkout"}`, start, end))
		return len(lines) == 1
	})
	if lines[0] != line {
		t.Fatalf("ingest pipeline changed the line to %q, want %q (only metadata was configured)", lines[0], line)
	}
	// The ingest pipeline ran before storage: flattened and renamed metadata
	// is visible on the stored row (merged into labels in uncategorized
	// responses).
	r := queryRange(t, lr, `{service_name="checkout"}`, start, end)
	labels := streamLabels(r)
	if labels[0]["user_id"] != "7" || labels[0]["message"] != "hello" {
		t.Fatalf("ingest pipelines were not applied before storage: labels=%v", labels[0])
	}

	// Read pipeline: rewrite the line at query time and add a metadata field,
	// so the metadata-route checks can prove read output never becomes a
	// stored label.
	code, body = request(t, http.MethodPost, lr.url+"/api/v1/pipelines", pipelineDoc(
		"read-redact", "read", "checkout", 0,
		pipeline.Step{Type: pipeline.StepRedact, Pattern: "hello", Replace: new("MASKED")},
		pipeline.Step{
			Type:    pipeline.StepRegexExtract,
			Pattern: `(?P<read_marker>MASKED)`,
			When:    `line contains "MASKED"`,
		},
	))
	if code != http.StatusCreated {
		t.Fatalf("create read pipeline: %d\n%s", code, body)
	}
	waitFor(t, 10*time.Second, "read pipeline to become visible", func() bool {
		return len(streamLines(t, queryRange(t, lr, `{service_name="checkout"} |= "MASKED"`, start, end))) == 1
	})
	if got := streamLines(t, queryRange(t, lr, `{service_name="checkout"} |= "hello"`, start, end)); len(got) != 0 {
		t.Fatalf("rewritten-away needle still matched: %v", got)
	}
	readLabels := streamLabels(queryRange(t, lr, `{service_name="checkout"} |= "MASKED"`, start, end))
	if len(readLabels) != 1 {
		t.Fatalf("read-marked row disappeared: %d streams", len(readLabels))
	}
	if readLabels[0]["read_marker"] != "MASKED" {
		t.Errorf("read pipeline metadata read_marker = %q, want MASKED", readLabels[0]["read_marker"])
	}

	// Metric queries see the transformed line; pipeline_error is ordinary
	// metadata and does not break count/bytes queries.
	if got := instantQuery(t, lr, `sum(count_over_time({service_name="checkout"}[10m]))`, ts.Add(time.Minute)); got != 1 {
		t.Errorf("count_over_time = %v, want 1", got)
	}
	masked := strings.ReplaceAll(line, "hello", "MASKED")
	if got := instantQuery(t, lr, `sum(bytes_over_time({service_name="checkout"}[10m]))`, ts.Add(time.Minute)); got != float64(len(masked)) {
		t.Errorf("bytes_over_time = %v, want %d (transformed line bytes)", got, len(masked))
	}

	// The default keep policy is wired through the real push handler: a
	// malformed parse still answers 204 and stores the entry with the
	// pipeline_error metadata markers.
	bad := `{"msg":"not-json"`
	badTS := ts.Add(2 * time.Second)
	if code, body := pushRequest(t, lr, map[string]string{"service_name": "checkout"}, badTS, bad); code != http.StatusNoContent {
		t.Fatalf("malformed line with default keep = %d, want 204:\n%s", code, body)
	}
	waitFor(t, 20*time.Second, "kept malformed row", func() bool {
		return len(streamLines(t, queryRange(t, lr, `{service_name="checkout"} |= "not-json"`, badTS, badTS.Add(time.Microsecond)))) == 1
	})
	badLabels := streamLabels(queryRange(t, lr, `{service_name="checkout"} |= "not-json"`, badTS, badTS.Add(time.Microsecond)))
	if len(badLabels) != 1 {
		t.Fatalf("kept malformed row disappeared: %d streams", len(badLabels))
	}
	if badLabels[0]["pipeline_error"] != string(pipeline.StepParseJSON) {
		t.Errorf("pipeline_error = %q, want %q", badLabels[0]["pipeline_error"], pipeline.StepParseJSON)
	}
	if badLabels[0]["pipeline_error_details"] == "" {
		t.Error("pipeline_error_details was not set")
	}
	// Stored error metadata stays ordinary metadata: dashboards can still
	// count the stream and split clean from errored rows.
	countAt := badTS.Add(time.Minute)
	if got := instantQuery(t, lr, `sum(count_over_time({service_name="checkout"}[10m]))`, countAt); got != 2 {
		t.Errorf("count_over_time with a kept error row = %v, want 2", got)
	}
	if got := instantQuery(t, lr, `sum(count_over_time({service_name="checkout"} | pipeline_error="" [10m]))`, countAt); got != 1 {
		t.Errorf("clean-row count_over_time = %v, want 1", got)
	}
	if got := instantQuery(t, lr, `sum(count_over_time({service_name="checkout"} | pipeline_error!="" [10m]))`, countAt); got != 1 {
		t.Errorf("error-row count_over_time = %v, want 1", got)
	}

	// Metadata routes still report stored labels, never read-pipeline output.
	v := url.Values{}
	v.Set("start", start.UTC().Format(time.RFC3339Nano))
	v.Set("end", end.UTC().Format(time.RFC3339Nano))
	code, body = request(t, http.MethodGet, lr.url+"/loki/api/v1/labels?"+v.Encode(), nil)
	if code != http.StatusOK {
		t.Fatalf("labels: %d\n%s", code, body)
	}
	var labelResp struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(body, &labelResp); err != nil {
		t.Fatalf("decode labels: %v", err)
	}
	if !strings.Contains(strings.Join(labelResp.Data, ","), "service_name") {
		t.Errorf("labels lost the stored stream label: %v", labelResp.Data)
	}
	for _, name := range labelResp.Data {
		if name == "read_marker" || strings.Contains(name, "MASKED") || strings.Contains(name, "message") {
			t.Errorf("read-pipeline metadata leaked into labels: %q", name)
		}
	}

	// Dry-run validates and runs a candidate without persisting it.
	code, body = request(t, http.MethodPost, lr.url+"/api/v1/pipelines/dry-run", map[string]any{
		"pipeline": pipelineDoc("unsaved", "ingest", "checkout", 0,
			pipeline.Step{Type: pipeline.StepParseLogfmt}),
		"entries": []any{map[string]any{
			"line":   "user.id=alice http-status=200",
			"labels": map[string]string{"service_name": "checkout"},
		}},
	})
	if code != http.StatusOK {
		t.Fatalf("dry-run: %d\n%s", code, body)
	}
	var dry struct {
		Entries []struct {
			Meta map[string]string `json:"meta"`
		} `json:"entries"`
		Dropped int `json:"dropped"`
	}
	if err := json.Unmarshal(body, &dry); err != nil {
		t.Fatalf("decode dry-run: %v", err)
	}
	if dry.Dropped != 0 || len(dry.Entries) != 1 || dry.Entries[0].Meta["user_id"] != "alice" {
		t.Fatalf("dry-run output = %+v", dry)
	}
	if code, _ := request(t, http.MethodGet, lr.url+"/api/v1/pipelines/unsaved", nil); code != http.StatusNotFound {
		t.Fatalf("dry-run persisted its candidate (GET = %d)", code)
	}
	// Delete the live read pipeline and observe it disappear from GET.
	if code, body := request(t, http.MethodDelete, lr.url+"/api/v1/pipelines/read-redact", map[string]any{"version": 1}); code != http.StatusNoContent {
		t.Fatalf("delete read pipeline: %d\n%s", code, body)
	}
	if code, _ := request(t, http.MethodGet, lr.url+"/api/v1/pipelines/read-redact", nil); code != http.StatusNotFound {
		t.Fatalf("deleted pipeline still present (GET = %d)", code)
	}

	// Both role registries export their own bounded-cardinality series.
	_, metrics := request(t, http.MethodGet, lr.url+"/metrics", nil)
	for _, want := range []string{
		`parkana_pipeline_active_definitions{role="ingester"}`,
		`parkana_pipeline_active_definitions{role="querier"}`,
	} {
		if n := bytes.Count(metrics, []byte(want)); n != 1 {
			t.Errorf("%s appears %d times, want exactly 1", want, n)
		}
	}
}

// TestPipelineConvergesAcrossIndependentCatalogs writes pipelines through a
// second catalog connection while the process runs, proving the roles reload
// independently through the catalog and that a delete becomes visible.
func TestPipelineConvergesAcrossIndependentCatalogs(t *testing.T) {
	dir := t.TempDir()
	dsn := dir + "/catalog.db"
	lr := startLive(t,
		"-roles=all",
		"-catalog.dsn="+dsn,
		"-pipeline.poll-interval=50ms",
		"-pipeline.reload-timeout=2s",
		"-ingest.flush-interval=20ms",
		"-ingest.upload-interval=20ms",
		"-ingest.upload-max-age=1ms",
		"-ingest.upload-min=1B",
	)
	second, err := catalog.NewSQLite(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })

	ts := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	line := "value=hello"
	pushLine(t, lr, map[string]string{"service_name": "checkout"}, ts, line)
	start, end := ts.Add(-2*time.Minute), ts.Add(2*time.Minute)
	waitFor(t, 20*time.Second, "baseline row", func() bool {
		return len(streamLines(t, queryRange(t, lr, `{service_name="checkout"}`, start, end))) == 1
	})

	spec, err := json.Marshal(pipeline.Spec{Steps: []pipeline.Step{
		{Type: pipeline.StepRedact, Pattern: "hello", Replace: new("CONVERGED")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	def, err := second.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   tenant.Default,
		Name:     "read-converge",
		Stage:    catalog.PipelineStageRead,
		Selector: "checkout",
		Spec:     spec,
	}, 0)
	if err != nil {
		t.Fatalf("second catalog put: %v", err)
	}
	waitFor(t, 10*time.Second, "read registry to reload from the independent catalog", func() bool {
		return len(streamLines(t, queryRange(t, lr, `{service_name="checkout"} |= "CONVERGED"`, start, end))) == 1
	})

	// A delete through the same independent catalog also converges.
	if _, err := second.DeletePipeline(t.Context(), tenant.Default, "read-converge", def.Version); err != nil {
		t.Fatalf("second catalog delete: %v", err)
	}
	waitFor(t, 10*time.Second, "read registry to drop the deleted pipeline", func() bool {
		return len(streamLines(t, queryRange(t, lr, `{service_name="checkout"} |= "CONVERGED"`, start, end))) == 0
	})

	// An ingest-stage change converges too, even though the ingester cached an
	// empty definition set on its first push: retry pushes until one is
	// stored through the reloaded snapshot, then check the stored metadata.
	ingestSpec, err := json.Marshal(pipeline.Spec{Steps: []pipeline.Step{{Type: pipeline.StepParseLogfmt}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   tenant.Default,
		Name:     "ingest-converge",
		Stage:    catalog.PipelineStageIngest,
		Selector: "checkout",
		Spec:     ingestSpec,
	}, 0); err != nil {
		t.Fatalf("second catalog ingest put: %v", err)
	}
	var firstPush, lastPush time.Time
	waitFor(t, 15*time.Second, "ingest registry to reload and store extracted metadata", func() bool {
		if now := time.Now(); now.Sub(lastPush) >= 100*time.Millisecond {
			lastPush = now.UTC().Truncate(time.Microsecond)
			if firstPush.IsZero() {
				firstPush = lastPush
			}
			pushLine(t, lr, map[string]string{"service_name": "checkout"}, lastPush, "user=alice status=200")
		}
		if firstPush.IsZero() {
			return false
		}
		// Query every hot attempt, not just the newest one: upload latency may
		// exceed the push cadence, so an earlier transformed row may be the
		// first visible proof.
		for _, got := range streamLabels(queryRange(t, lr, `{service_name="checkout"}`, firstPush, lastPush.Add(time.Microsecond))) {
			if got["user"] == "alice" {
				return true
			}
		}
		return false
	})
}

// TestPipelineAssemblyFailureReleasesRegistry proves a role constructor
// failure closes the registry and unregisters its metrics instead of leaking
// them to a process that will never run the role.
func TestPipelineAssemblyFailureReleasesRegistry(t *testing.T) {
	cfg := assemblyConfig(t)
	reg := prometheus.NewRegistry()
	log := slog.New(slog.DiscardHandler)
	cat := catalog.NewMemory()
	pipes, err := newPipelineWiring(cfg, log, cat, reg)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Options{Addr: "127.0.0.1:0", Logger: log, Registry: reg})
	// Make it already registered so the ingester constructor fails.
	srv.Handle(lokiapi.Push, http.NotFoundHandler())
	factory, err := pipes.ingesterRole()
	if err != nil {
		t.Fatal(err)
	}
	deps := app.Deps{Config: cfg, Logger: log, Store: mustFS(t), Catalog: cat, HTTP: srv, Metrics: reg}
	if _, err := factory(deps); err == nil {
		t.Fatal("ingester construction succeeded despite a conflicting route")
	}
	// The failed assembly must have released the role's metrics: registering a
	// fresh registry for the same role on the same registerer has to succeed.
	// (A fresh registry has no metric children, so Gather cannot prove this.)
	if _, err := pipes.newRegistry(pipeline.RoleIngester); err != nil {
		t.Fatalf("failed assembly left ingester metrics registered: %v", err)
	}
	if err := pipes.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := pipes.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// probeService records that its Run was called and with what context.
type probeService struct {
	mu     sync.Mutex
	called bool
	ctxErr error
	err    error
}

func (p *probeService) Run(ctx context.Context) error {
	p.mu.Lock()
	p.called = true
	p.ctxErr = ctx.Err()
	p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	<-ctx.Done()
	return nil
}

func (p *probeService) state() (called bool, ctxErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.called, p.ctxErr
}

// TestCombineWithRegistryReleasesPartiallyBuiltRole proves app.Combine's
// missing rollback is covered: a later factory failure releases services that
// were already constructed (with a done context) and closes the registry.
func TestCombineWithRegistryReleasesPartiallyBuiltRole(t *testing.T) {
	cfg := assemblyConfig(t)
	reg := prometheus.NewRegistry()
	log := slog.New(slog.DiscardHandler)
	pipes, err := newPipelineWiring(cfg, log, catalog.NewMemory(), reg)
	if err != nil {
		t.Fatal(err)
	}
	preg, err := pipes.newRegistry(pipeline.RoleQuerier)
	if err != nil {
		t.Fatal(err)
	}
	probe := &probeService{}
	built := func(app.Deps) (app.Service, error) { return probe, nil }
	failing := func(app.Deps) (app.Service, error) { return nil, errors.New("tail assembly failed") }
	deps := app.Deps{Config: cfg, Logger: log, Store: mustFS(t), Catalog: pipes.cat, Metrics: reg}
	if _, err := combineWithRegistry(preg, built, failing)(deps); err == nil {
		t.Fatal("assembly succeeded despite the failing factory")
	}
	called, ctxErr := probe.state()
	if !called || !errors.Is(ctxErr, context.Canceled) {
		t.Fatalf("previously built service was not released: called=%t ctxErr=%v", called, ctxErr)
	}
	// A fresh registry has no metric children, so Gather cannot prove the
	// failed role unregistered its collectors. The closed registry and a
	// successful same-role re-registration do.
	if _, err := preg.Snapshot(t.Context(), tenant.Default); !errors.Is(err, pipeline.ErrRegistryClosed) {
		t.Fatalf("failed assembly did not close the registry: %v", err)
	}
	if _, err := pipes.newRegistry(pipeline.RoleQuerier); err != nil {
		t.Fatalf("failed assembly left querier metrics registered: %v", err)
	}
	if err := pipes.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestCombineWithRegistryKeepsRealError proves a real service failure is
// reported even though its siblings stop with context cancellation. The
// failing service returns a joined error, matching the case errors.Is alone
// would mishandle.
func TestCombineWithRegistryKeepsRealError(t *testing.T) {
	cfg := assemblyConfig(t)
	reg := prometheus.NewRegistry()
	log := slog.New(slog.DiscardHandler)
	pipes, err := newPipelineWiring(cfg, log, catalog.NewMemory(), reg)
	if err != nil {
		t.Fatal(err)
	}
	preg, err := pipes.newRegistry(pipeline.RoleQuerier)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("query worker failed")
	failing := func(app.Deps) (app.Service, error) {
		return &probeService{err: errors.Join(sentinel, context.Canceled)}, nil
	}
	waiting := func(app.Deps) (app.Service, error) { return &probeService{}, nil }
	deps := app.Deps{Config: cfg, Logger: log, Store: mustFS(t), Catalog: pipes.cat, Metrics: reg}
	svc, err := combineWithRegistry(preg, failing, waiting)(deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = svc.Run(ctx)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run error = %v, want the sentinel service failure", err)
	}
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatal("role shutdown hung instead of canceling and joining its siblings")
	}
}

// TestRunRejectsBadCatalogConfig covers a startup catalog failure before any
// role is built.
func TestRunRejectsBadCatalogConfig(t *testing.T) {
	dir := t.TempDir()
	var stderr bytes.Buffer
	err := run(t.Context(), []string{
		"-http.addr=127.0.0.1:0",
		"-data.dir=" + dir,
		"-catalog.driver=sqlite",
		"-catalog.dsn=" + dir, // a directory is not a database file
	}, func(string) string { return "" }, &stderr)
	if err == nil {
		t.Fatal("run accepted a directory as the catalog DSN")
	}
}

// assemblyConfig returns a valid Config for constructor-level tests.
func assemblyConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load([]string{"-data.dir=" + t.TempDir(), "-http.addr=127.0.0.1:0"},
		func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func mustFS(t *testing.T) objstore.Store {
	t.Helper()
	fs, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return fs
}
