package pipelineapi_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/pipelineapi"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// entryJSON is the client's reading of one dry-run entry.
type entryJSON struct {
	Timestamp *time.Time        `json:"timestamp"`
	Line      string            `json:"line"`
	Labels    map[string]string `json:"labels"`
	Meta      map[string]string `json:"meta"`
}

type dryRunResult struct {
	Entries []entryJSON `json:"entries"`
	Dropped int         `json:"dropped"`
}

// dryRunDoc builds a request body: pipeline + entries.
func dryRunDoc(pipeline, entries string) string {
	return `{"pipeline":` + pipeline + `,"entries":[` + entries + `]}`
}

func TestDryRunTransform(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	// rename moves a metadata field; promote copies metadata into a label.
	doc := dryRunDoc(
		`{"name":"cand","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"},{"type":"promote","from":"b","to":"level"}]}`,
		`{"timestamp":"2026-11-05T10:00:00Z","line":"hello","labels":{"service_name":"svc"},"meta":{"a":"info","keep":"1"}}`,
	)
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", doc)
	if code != http.StatusOK {
		t.Fatalf("dry-run = %d %q, want 200", code, body)
	}
	res := decodeJSON[dryRunResult](t, body)
	if res.Dropped != 0 || len(res.Entries) != 1 {
		t.Fatalf("result = %+v, want one kept entry", res)
	}
	e := res.Entries[0]
	if e.Line != "hello" {
		t.Errorf("line = %q, want unchanged", e.Line)
	}
	if e.Meta["a"] != "" || e.Meta["b"] != "info" || e.Meta["keep"] != "1" {
		t.Errorf("meta = %v, want a renamed to b and keep preserved", e.Meta)
	}
	if e.Labels["service_name"] != "svc" || e.Labels["level"] != "info" {
		t.Errorf("labels = %v, want original label plus promoted level=info", e.Labels)
	}
	if e.Timestamp == nil || !e.Timestamp.Equal(time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("timestamp = %v, want echoed input", e.Timestamp)
	}
	for name := range e.Meta {
		if strings.HasPrefix(name, "__") {
			t.Errorf("meta has reserved __ name %q", name)
		}
	}
}

func TestDryRunSelector(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	// The pipeline selects dataset "other" only. The svc entry's dataset is
	// "svc" (service_name routes); it passes through untouched. The entry
	// routed into "other" is transformed.
	doc := dryRunDoc(
		`{"name":"cand","stage":"ingest","selector":"other","steps":[{"type":"rename","from":"a","to":"b"}]}`,
		`{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}`+
			`,{"line":"y","labels":{"service_name":"other"},"meta":{"a":"2"}}`,
	)
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", doc)
	if code != http.StatusOK {
		t.Fatalf("dry-run = %d %q, want 200", code, body)
	}
	res := decodeJSON[dryRunResult](t, body)
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(res.Entries))
	}
	if res.Entries[0].Meta["a"] != "1" || res.Entries[0].Meta["b"] != "" {
		t.Errorf("non-matching entry transformed: %v", res.Entries[0].Meta)
	}
	if res.Entries[1].Meta["b"] != "2" || res.Entries[1].Meta["a"] != "" {
		t.Errorf("matching entry not transformed: %v", res.Entries[1].Meta)
	}
}

func TestDryRunKeepDropReject(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	post := func(doc string) (int, string) {
		return do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", doc)
	}

	// Default keep: a parse failure keeps the entry, marked with
	// pipeline_error / pipeline_error_details metadata.
	code, body := post(dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"parse_json"}]}`,
		`{"line":"not json"}`,
	))
	if code != http.StatusOK {
		t.Fatalf("keep dry-run = %d %q, want 200", code, body)
	}
	res := decodeJSON[dryRunResult](t, body)
	if res.Dropped != 0 || len(res.Entries) != 1 {
		t.Fatalf("keep result = %+v, want one kept entry", res)
	}
	em := res.Entries[0].Meta
	if em["pipeline_error"] != "parse_json" || em["pipeline_error_details"] == "" {
		t.Fatalf("error metadata = %v, want pipeline_error=parse_json and details", em)
	}
	for name := range em {
		if strings.HasPrefix(name, "__") {
			t.Fatalf("error metadata wrote reserved name %q", name)
		}
	}
	if res.Entries[0].Line != "not json" {
		t.Fatal("keep policy changed the line")
	}

	// Explicit drop removes the entry.
	code, body = post(dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"parse_json","on_error":"drop"}]}`,
		`{"line":"not json"},{"line":"{\"a\":1}"}`,
	))
	if code != http.StatusOK {
		t.Fatalf("drop dry-run = %d %q", code, body)
	}
	res = decodeJSON[dryRunResult](t, body)
	if res.Dropped != 1 || len(res.Entries) != 1 || res.Entries[0].Line != `{"a":1}` {
		t.Fatalf("drop result = %+v, want dropped=1 and the good entry", res)
	}
	if res.Entries[0].Meta["a"] != "1" {
		t.Fatalf("parse_json did not extract: %v", res.Entries[0].Meta)
	}

	// drop_if with a true condition drops every entry.
	code, body = post(dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"drop_if","expr":"line == \"gone\""}]}`,
		`{"line":"gone"},{"line":"here"}`,
	))
	res = decodeJSON[dryRunResult](t, body)
	if code != http.StatusOK || res.Dropped != 1 || len(res.Entries) != 1 || res.Entries[0].Line != "here" {
		t.Fatalf("drop_if result = %d %+v, want dropped=1 kept 'here'", code, res)
	}

	// Explicit reject fails the request, mirroring the ingest adapter's
	// ErrRejected -> 400.
	code, body = post(dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"parse_json","on_error":"reject"}]}`,
		`{"line":"not json"}`,
	))
	if code != http.StatusBadRequest || !strings.Contains(body, "rejected") {
		t.Fatalf("reject dry-run = %d %q, want 400 naming the rejection", code, body)
	}
}

func TestDryRunRequestValidation(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{MaxDryRunEntries: 2})
	pipe := `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`
	before, err := cat.PipelineChangeToken(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, doc string
	}{
		{"not an object", `[]`},
		{"unknown member", `{"pipeline":` + pipe + `,"bogus":1}`},
		{"unknown pipeline member", `{"pipeline":{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":1}}`},
		{"compile error", dryRunDoc(`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"melt"}]}`, `{"line":"x"}`)},
		{"no steps", dryRunDoc(`{"name":"p","stage":"ingest","selector":"*","steps":[]}`, `{"line":"x"}`)},
		{"too many entries", dryRunDoc(pipe, `{"line":"1"},{"line":"2"},{"line":"3"}`)},
		{"invalid label name", dryRunDoc(pipe, `{"line":"x","labels":{"bad name":"v"}}`)},
		{"invalid meta name", dryRunDoc(pipe, `{"line":"x","meta":{"bad name":"v"}}`)},
		{"bad timestamp", dryRunDoc(pipe, `{"timestamp":"soon","line":"x"}`)},
		{"unknown entry member", dryRunDoc(pipe, `{"line":"x","extra":1}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", tc.doc); code != http.StatusBadRequest {
				t.Fatalf("dry-run = %d %q, want 400", code, body)
			}
		})
	}
	// Boundary: exactly the entry limit succeeds.
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "",
		dryRunDoc(pipe, `{"line":"1"},{"line":"2"}`))
	if code != http.StatusOK {
		t.Fatalf("limit-boundary dry-run = %d %q, want 200", code, body)
	}
	// Nothing was persisted: no rows, token unchanged.
	defs, err := cat.ListPipelines(t.Context(), tenant.Default)
	if err != nil || len(defs) != 0 {
		t.Fatalf("dry-run wrote pipelines: %v %v", defs, err)
	}
	if _, err := cat.GetPipeline(t.Context(), tenant.Default, "p"); err == nil {
		t.Fatal("dry-run persisted its candidate")
	}
	after, err := cat.PipelineChangeToken(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("change token advanced %d -> %d during dry-run", before, after)
	}
}

func TestDryRunEdgeCases(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	pipe := `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`
	// No entries: an empty transformed list, zero dropped.
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "",
		`{"pipeline":`+pipe+`,"entries":[]}`)
	if code != http.StatusOK || body != `{"entries":[],"dropped":0}` {
		t.Fatalf("empty dry-run = %d %q, want empty result", code, body)
	}
	// Missing labels/meta decode as empty sets and are echoed as {}.
	code, body = do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "",
		dryRunDoc(pipe, `{"line":"x"}`))
	res := decodeJSON[dryRunResult](t, body)
	if code != http.StatusOK || len(res.Entries) != 1 {
		t.Fatalf("minimal entry dry-run = %d %q", code, body)
	}
	e := res.Entries[0]
	if e.Labels == nil || e.Meta == nil || e.Timestamp != nil {
		t.Fatalf("minimal entry = %+v, want empty maps and no timestamp", e)
	}
	if !strings.Contains(body, `"labels":{}`) || !strings.Contains(body, `"meta":{}`) {
		t.Fatalf("output = %s, want explicit {} maps", body)
	}
}
