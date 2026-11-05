package pipelineapi_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json/v2"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/pipelineapi"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// viewJSON is the client's reading of a pipeline view.
type viewJSON struct {
	Name      string           `json:"name"`
	Stage     string           `json:"stage"`
	Selector  string           `json:"selector"`
	Steps     []map[string]any `json:"steps"`
	Version   int64            `json:"version"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

const defJSON = `{"name":"pipe-a","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`

// newTestServer registers the API on a real server (real tenant middleware)
// and serves it through httptest.
func newTestServer(t *testing.T, multiTenant bool, o pipelineapi.Options) (*httptest.Server, *catalog.Memory) {
	t.Helper()
	cat := catalog.NewMemory()
	srv := server.New(server.Options{
		Logger:      slog.New(slog.DiscardHandler),
		Registry:    prometheus.NewRegistry(),
		MultiTenant: multiTenant,
	})
	o.Catalog = cat
	if len(o.CompileOptions.StreamLabels) == 0 {
		o.CompileOptions.StreamLabels = []string{"service_name", "job"}
	}
	if err := pipelineapi.Register(srv, o); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, cat
}

// do issues one request; tenantHdr, when non-empty, sets X-Scope-OrgID.
func do(t *testing.T, ts *httptest.Server, method, path, tenantHdr, body string) (int, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if tenantHdr != "" {
		req.Header.Set(tenant.Header, tenantHdr)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func decodeJSON[T any](t *testing.T, body string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	return v
}

func getView(t *testing.T, ts *httptest.Server, org, name string) (int, viewJSON) {
	t.Helper()
	code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines/"+name, org, "")
	if code != http.StatusOK {
		return code, viewJSON{}
	}
	return code, decodeJSON[viewJSON](t, body)
}

func TestRegister(t *testing.T) {
	srv := server.New(server.Options{
		Logger:   slog.New(slog.DiscardHandler),
		Registry: prometheus.NewRegistry(),
	})
	cat := catalog.NewMemory()
	o := pipelineapi.Options{
		Catalog:        cat,
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{"service_name"}},
	}
	if err := pipelineapi.Register(srv, o); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, p := range []string{
		pipelineapi.List, pipelineapi.Create, pipelineapi.Get,
		pipelineapi.Replace, pipelineapi.Delete, pipelineapi.DryRun,
	} {
		if !srv.Registered(p) {
			t.Errorf("route %q not registered", p)
		}
	}
	if err := pipelineapi.Register(srv, o); err == nil {
		t.Error("second Register succeeded, want already-registered error")
	}
}

// TestRegisterPrecheckAtomic: a registrar with one colliding pattern gets no
// route at all.
func TestRegisterPrecheckAtomic(t *testing.T) {
	srv := server.New(server.Options{
		Logger:   slog.New(slog.DiscardHandler),
		Registry: prometheus.NewRegistry(),
	})
	srv.Handle(pipelineapi.Get, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	err := pipelineapi.Register(srv, pipelineapi.Options{
		Catalog:        catalog.NewMemory(),
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{"service_name"}},
	})
	if err == nil {
		t.Fatal("Register succeeded over a taken route")
	}
	for _, p := range []string{pipelineapi.List, pipelineapi.Create, pipelineapi.Replace, pipelineapi.Delete, pipelineapi.DryRun} {
		if srv.Registered(p) {
			t.Errorf("route %q registered despite failed Register", p)
		}
	}
}

func TestRegisterBadOptions(t *testing.T) {
	ok := pipeline.CompileOptions{StreamLabels: []string{"service_name"}}
	for _, tc := range []struct {
		name string
		o    pipelineapi.Options
	}{
		{"no catalog", pipelineapi.Options{CompileOptions: ok}},
		{"no stream labels", pipelineapi.Options{Catalog: catalog.NewMemory()}},
		{"bad stream label name", pipelineapi.Options{
			Catalog: catalog.NewMemory(),
			CompileOptions: pipeline.CompileOptions{
				StreamLabels: []string{"bad label"},
			},
		}},
		{"negative body limit", pipelineapi.Options{
			Catalog: catalog.NewMemory(), MaxBodyBytes: -1, CompileOptions: ok,
		}},
		{"negative entry limit", pipelineapi.Options{
			Catalog: catalog.NewMemory(), MaxDryRunEntries: -1, CompileOptions: ok,
		}},
		{"negative line bytes", pipelineapi.Options{
			Catalog:        catalog.NewMemory(),
			CompileOptions: pipeline.CompileOptions{StreamLabels: ok.StreamLabels, LineBytes: -1},
		}},
		{"negative meta bytes", pipelineapi.Options{
			Catalog:        catalog.NewMemory(),
			CompileOptions: pipeline.CompileOptions{StreamLabels: ok.StreamLabels, MetaBytes: -1},
		}},
		{"negative meta value bytes", pipelineapi.Options{
			Catalog:        catalog.NewMemory(),
			CompileOptions: pipeline.CompileOptions{StreamLabels: ok.StreamLabels, MetaValueBytes: -1},
		}},
		{"negative label value bytes", pipelineapi.Options{
			Catalog:        catalog.NewMemory(),
			CompileOptions: pipeline.CompileOptions{StreamLabels: ok.StreamLabels, LabelValueBytes: -1},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := server.New(server.Options{
				Logger:   slog.New(slog.DiscardHandler),
				Registry: prometheus.NewRegistry(),
			})
			if err := pipelineapi.Register(srv, tc.o); err == nil {
				t.Fatal("Register succeeded, want error")
			}
			for _, p := range []string{
				pipelineapi.List, pipelineapi.Create, pipelineapi.Get,
				pipelineapi.Replace, pipelineapi.Delete, pipelineapi.DryRun,
			} {
				if srv.Registered(p) {
					t.Fatalf("route %q registered despite failed Register", p)
				}
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{})

	// Empty list.
	code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines", "", "")
	if code != http.StatusOK || body != `{"pipelines":[]}` {
		t.Fatalf("GET list = %d %q, want 200 empty list", code, body)
	}

	// Create.
	code, body = do(t, ts, http.MethodPost, "/api/v1/pipelines", "", defJSON)
	if code != http.StatusCreated {
		t.Fatalf("POST create = %d %q, want 201", code, body)
	}
	if strings.Contains(body, "tenant") {
		t.Errorf("view exposes tenant: %q", body)
	}
	createBody := body
	v := decodeJSON[viewJSON](t, body)
	if v.Name != "pipe-a" || v.Stage != "ingest" || v.Selector != "*" || v.Version != 1 {
		t.Fatalf("create view = %+v, want name pipe-a stage ingest selector * version 1", v)
	}
	if len(v.Steps) != 1 || v.Steps[0]["type"] != "rename" || v.Steps[0]["from"] != "a" || v.Steps[0]["to"] != "b" {
		t.Fatalf("create steps = %v, want one rename step", v.Steps)
	}
	if v.CreatedAt.IsZero() || v.UpdatedAt.IsZero() {
		t.Fatalf("create timestamps zero: %+v", v)
	}

	// List and get show the same row.
	code, body = do(t, ts, http.MethodGet, "/api/v1/pipelines", "", "")
	listed := decodeJSON[struct {
		Pipelines []viewJSON `json:"pipelines"`
	}](t, body)
	if code != http.StatusOK || len(listed.Pipelines) != 1 || listed.Pipelines[0].Version != 1 {
		t.Fatalf("GET list after create = %d %q, want one version-1 pipeline", code, body)
	}
	if code, gbody := do(t, ts, http.MethodGet, "/api/v1/pipelines/pipe-a", "", ""); code != http.StatusOK || gbody != createBody {
		t.Fatalf("GET pipe-a = %d %q, want the create body %q", code, gbody, createBody)
	}

	// Duplicate create conflicts.
	if code, body = do(t, ts, http.MethodPost, "/api/v1/pipelines", "", defJSON); code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d %q, want 409", code, body)
	}

	// Replace at the live version.
	putDoc := `{"name":"pipe-a","stage":"ingest","selector":"*","steps":[{"type":"drop_if","expr":"line == \"x\""}],"version":1}`
	code, body = do(t, ts, http.MethodPut, "/api/v1/pipelines/pipe-a", "", putDoc)
	if code != http.StatusOK {
		t.Fatalf("PUT = %d %q, want 200", code, body)
	}
	v2 := decodeJSON[viewJSON](t, body)
	if v2.Version != 2 || len(v2.Steps) != 1 || v2.Steps[0]["type"] != "drop_if" {
		t.Fatalf("replace view = %+v, want version 2 drop_if", v2)
	}
	if !v2.CreatedAt.Equal(v.CreatedAt) {
		t.Fatalf("replace changed created_at: %v -> %v", v.CreatedAt, v2.CreatedAt)
	}
	if v2.UpdatedAt.Before(v.CreatedAt) {
		t.Fatalf("replace updated_at %v predates created_at %v", v2.UpdatedAt, v.CreatedAt)
	}

	// Stale version and missing rows.
	if code, _ = do(t, ts, http.MethodPut, "/api/v1/pipelines/pipe-a", "", putDoc); code != http.StatusConflict {
		t.Fatalf("stale PUT = %d, want 409", code)
	}
	putMissing := `{"name":"nope","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":3}`
	if code, _ = do(t, ts, http.MethodPut, "/api/v1/pipelines/nope", "", putMissing); code != http.StatusNotFound {
		t.Fatalf("PUT missing = %d, want 404", code)
	}

	// Delete needs the live version.
	if code, _ = do(t, ts, http.MethodDelete, "/api/v1/pipelines/pipe-a", "", `{"version":1}`); code != http.StatusConflict {
		t.Fatalf("stale DELETE = %d, want 409", code)
	}
	if code, body = do(t, ts, http.MethodDelete, "/api/v1/pipelines/pipe-a", "", `{"version":2}`); code != http.StatusNoContent || body != "" {
		t.Fatalf("DELETE = %d %q, want 204 empty", code, body)
	}
	if code, _ = getView(t, ts, "", "pipe-a"); code != http.StatusNotFound {
		t.Fatalf("GET deleted = %d, want 404", code)
	}
	if code, _ = do(t, ts, http.MethodDelete, "/api/v1/pipelines/pipe-a", "", `{"version":3}`); code != http.StatusNotFound {
		t.Fatalf("DELETE tombstoned = %d, want 404", code)
	}
	// PUT over a tombstone with a nonzero version is a 404 too.
	if code, _ = do(t, ts, http.MethodPut, "/api/v1/pipelines/pipe-a", "",
		`{"name":"pipe-a","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":3}`); code != http.StatusNotFound {
		t.Fatalf("PUT tombstoned = %d, want 404", code)
	}

	// Recreation continues above the tombstone version (3), never resets.
	code, body = do(t, ts, http.MethodPost, "/api/v1/pipelines", "", defJSON)
	if code != http.StatusCreated {
		t.Fatalf("recreate POST = %d %q, want 201", code, body)
	}
	if v3 := decodeJSON[viewJSON](t, body); v3.Version != 4 {
		t.Fatalf("recreated version = %d, want 4 (above tombstone 3)", v3.Version)
	}

	// The catalog holds a steps-only spec; name/stage/selector are columns.
	stored, err := cat.GetPipeline(t.Context(), tenant.Default, "pipe-a")
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	specDoc := decodeJSON[map[string]any](t, string(stored.Spec))
	if len(specDoc) != 1 || specDoc["steps"] == nil {
		t.Fatalf("stored spec = %s, want only a steps member", stored.Spec)
	}
	if stored.Tenant != tenant.Default || stored.Name != "pipe-a" ||
		stored.Stage != catalog.PipelineStageIngest || stored.Selector != "*" {
		t.Fatalf("stored columns = %+v", stored)
	}
}

func TestValidation(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	post := func(doc string) (int, string) {
		return do(t, ts, http.MethodPost, "/api/v1/pipelines", "", doc)
	}
	steps := func(s string) string {
		return `{"name":"p","stage":"ingest","selector":"*","steps":[` + s + `]}`
	}
	for _, tc := range []struct {
		name, doc string
	}{
		{"empty object", `{}`},
		{"empty name", `{"name":"","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"reserved name ..", `{"name":"..","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"bad name bytes", `{"name":"a/b","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"bad stage", `{"name":"p","stage":"cron","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"unsanitized selector", `{"name":"p","stage":"ingest","selector":"My App","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"empty selector", `{"name":"p","stage":"ingest","selector":"","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"no steps", `{"name":"p","stage":"ingest","selector":"*","steps":[]}`},
		{"null steps", `{"name":"p","stage":"ingest","selector":"*","steps":null}`},
		{"nonzero version", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":2}`},
		{"unknown step type", steps(`{"type":"melt"}`)},
		{"wrong member for type", steps(`{"type":"rename","from":"a","to":"b","pattern":"x"}`)},
		{"missing type", steps(`{"from":"a","to":"b"}`)},
		{"bad on_error", steps(`{"type":"rename","from":"a","to":"b","on_error":"explode"}`)},
		{"bad expr", steps(`{"type":"drop_if","expr":"((("}`)},
		{"bad regex", steps(`{"type":"redact","pattern":"("}`)},
		{"read stage promote", `{"name":"p","stage":"read","selector":"*","steps":[{"type":"promote","from":"m","to":"lvl"}]}`},
		{"read stage set_stream", `{"name":"p","stage":"read","selector":"*","steps":[{"type":"set_stream","value":"other"}]}`},
		{"promote reserved name", steps(`{"type":"promote","from":"m","to":"__x"}`)},
		{"too many steps", steps(strings.Repeat(`{"type":"rename","from":"a","to":"b"},`, 32) + `{"type":"rename","from":"a","to":"b"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code, body := post(tc.doc); code != http.StatusBadRequest {
				t.Fatalf("POST = %d %q, want 400", code, body)
			}
		})
	}
}

func TestMalformedJSON(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	good := `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`
	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"truncated", "POST", "/api/v1/pipelines", `{"name":`},
		{"not json", "POST", "/api/v1/pipelines", `nope`},
		{"json array", "POST", "/api/v1/pipelines", `[]`},
		{"trailing comma", "POST", "/api/v1/pipelines", `{"name":"p",}`},
		{"trailing data", "POST", "/api/v1/pipelines", good + ` {}`},
		{"duplicate member", "POST", "/api/v1/pipelines", `{"name":"p","name":"q","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`},
		{"unknown top member", "POST", "/api/v1/pipelines", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"created_at":"2024-01-01T00:00:00Z"}`},
		{"unknown step member", "POST", "/api/v1/pipelines", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b","x":1}]}`},
		{"null step member", "POST", "/api/v1/pipelines", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b","pattern":null}]}`},
		{"version wrong type", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":"1"}`},
		{"version fractional", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":1.5}`},
		{"delete unknown member", "DELETE", "/api/v1/pipelines/p", `{"version":1,"x":2}`},
		{"delete no body", "DELETE", "/api/v1/pipelines/p", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := do(t, ts, tc.method, tc.path, "", tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d %q, want 400", tc.method, tc.path, code, body)
			}
		})
	}
}

func TestWriteSemantics(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	def := `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`
	if code, _ := do(t, ts, http.MethodPost, "/api/v1/pipelines", "", def); code != http.StatusCreated {
		t.Fatalf("create = %d", code)
	}
	for _, tc := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"put name mismatch", "PUT", "/api/v1/pipelines/p", `{"name":"other","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":1}`, 400},
		{"put version missing", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`, 400},
		{"put version zero", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":0}`, 400},
		{"put version negative", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":-2}`, 400},
		{"put compile error", "PUT", "/api/v1/pipelines/p", `{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"melt"}],"version":1}`, 400},
		{"delete version missing", "DELETE", "/api/v1/pipelines/p", `{}`, 400},
		{"delete version zero", "DELETE", "/api/v1/pipelines/p", `{"version":0}`, 400},
		{"delete version negative", "DELETE", "/api/v1/pipelines/p", `{"version":-1}`, 400},
		{"delete ok", "DELETE", "/api/v1/pipelines/p", `{"version":1}`, 204},
		{"delete again", "DELETE", "/api/v1/pipelines/p", `{"version":2}`, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := do(t, ts, tc.method, tc.path, "", tc.body)
			if code != tc.want {
				t.Fatalf("%s %s = %d %q, want %d", tc.method, tc.path, code, body, tc.want)
			}
		})
	}
}

func TestCompileBeforeWrite(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{})
	before, err := cat.PipelineChangeToken(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	// A read-stage pipeline may not promote labels: compile must reject it
	// before any catalog write.
	doc := `{"name":"bad","stage":"read","selector":"*","steps":[{"type":"promote","from":"m","to":"lvl"}]}`
	if code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines", "", doc); code != http.StatusBadRequest {
		t.Fatalf("POST invalid = %d %q, want 400", code, body)
	}
	if _, err := cat.GetPipeline(t.Context(), tenant.Default, "bad"); err == nil {
		t.Fatal("rejected definition was persisted")
	}
	after, err := cat.PipelineChangeToken(t.Context(), tenant.Default)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("change token advanced %d -> %d on a rejected write", before, after)
	}
}

func TestBodyLimit(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{MaxBodyBytes: 64})
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines", "", defJSON)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("POST over limit = %d %q, want 413", code, body)
	}
	if !strings.HasPrefix(body, "pipelineapi:") {
		t.Fatalf("413 body = %q, want pipelineapi plain text", body)
	}
	dryDoc := `{"pipeline":` + defJSON + `,"entries":[{"line":"x"}]}`
	if code, _ := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", dryDoc); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("dry-run over limit = %d, want 413", code)
	}
}

// TestBrokenStoredSpec: a catalog row the API did not write can hold spec
// bytes that fail to decode; the read endpoints answer the fixed internal
// error, not the decode detail. Options.Logger is unset here: the record is
// discarded without a panic.
func TestBrokenStoredSpec(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{})
	if _, err := cat.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant: tenant.Default, Name: "broken", Stage: catalog.PipelineStageIngest,
		Selector: "*", Spec: []byte(`{"oops":1}`),
	}, 0); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/pipelines/broken", "/api/v1/pipelines"} {
		code, body := do(t, ts, http.MethodGet, path, "", "")
		if code != http.StatusInternalServerError || body != "pipelineapi: internal error\n" {
			t.Fatalf("GET %s = %d %q, want 500 fixed body", path, code, body)
		}
	}
}

func TestTenant(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{})
	// Create under acme.
	if code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines", "acme", defJSON); code != http.StatusCreated {
		t.Fatalf("acme POST = %d %q", code, body)
	}
	stored, err := cat.GetPipeline(t.Context(), "acme", "pipe-a")
	if err != nil || stored.Tenant != "acme" {
		t.Fatalf("stored tenant = %+v, err %v", stored.Tenant, err)
	}
	// The default tenant sees nothing.
	if code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines", "", ""); code != http.StatusOK || body != `{"pipelines":[]}` {
		t.Fatalf("default list = %d %q, want empty", code, body)
	}
	if code, _ := getView(t, ts, "", "pipe-a"); code != http.StatusNotFound {
		t.Fatalf("default GET acme's pipeline = %d, want 404", code)
	}
	// ...and vice versa after a default-tenant create.
	if code, _ := do(t, ts, http.MethodPost, "/api/v1/pipelines", "", defJSON); code != http.StatusCreated {
		t.Fatal("default POST failed")
	}
	code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines", "acme", "")
	if code != http.StatusOK || !strings.Contains(body, `"pipe-a"`) {
		t.Fatalf("acme list = %d %q, want its one pipeline", code, body)
	}
	if code, _ := do(t, ts, http.MethodDelete, "/api/v1/pipelines/pipe-a", "acme", `{"version":1}`); code != http.StatusNoContent {
		t.Fatal("acme DELETE failed")
	}
	// Bad tenant ids are rejected by the middleware.
	if code, _ := do(t, ts, http.MethodGet, "/api/v1/pipelines", "bad tenant!", ""); code != http.StatusUnauthorized {
		t.Fatalf("bad tenant = %d, want 401", code)
	}
}

func TestTenantSingle(t *testing.T) {
	ts, _ := newTestServer(t, false, pipelineapi.Options{})
	if code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines", "acme", ""); code != http.StatusUnauthorized {
		t.Fatalf("foreign tenant = %d %q, want 401", code, body)
	}
	if code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines", "default", ""); code != http.StatusOK || body != `{"pipelines":[]}` {
		t.Fatalf("default tenant = %d %q, want 200 empty", code, body)
	}
}

func TestConcurrentWrites(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	// doQuiet is do without t.Helper calls: it runs inside goroutines.
	doQuiet := func(method, path, body string) int {
		req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
		if err != nil {
			return -1
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return -1
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	run := func(n int, f func() int) map[int]int {
		codes := make(chan int, n)
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() { codes <- f() })
		}
		wg.Wait()
		close(codes)
		out := map[int]int{}
		for c := range codes {
			out[c]++
		}
		return out
	}
	// Concurrent creates: exactly one winner.
	got := run(8, func() int {
		return doQuiet(http.MethodPost, "/api/v1/pipelines", defJSON)
	})
	if got[http.StatusCreated] != 1 || got[http.StatusConflict] != 7 {
		t.Fatalf("concurrent creates = %v, want one 201 and seven 409", got)
	}
	// Concurrent replaces at the same version: exactly one winner.
	putDoc := `{"name":"pipe-a","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":1}`
	got = run(8, func() int {
		return doQuiet(http.MethodPut, "/api/v1/pipelines/pipe-a", putDoc)
	})
	if got[http.StatusOK] != 1 || got[http.StatusConflict] != 7 {
		t.Fatalf("concurrent replaces = %v, want one 200 and seven 409", got)
	}
	// Concurrent deletes at the live version: exactly one 204.
	got = run(8, func() int {
		return doQuiet(http.MethodDelete, "/api/v1/pipelines/pipe-a", `{"version":2}`)
	})
	if got[http.StatusNoContent] != 1 || got[http.StatusNotFound]+got[http.StatusConflict] != 7 {
		t.Fatalf("concurrent deletes = %v, want one 204 and seven 404/409", got)
	}
}

// TestCompileOptionsCloned: Register owns its copy of StreamLabels; mutating
// the caller's slice afterwards must not change routing or compilation.
func TestCompileOptionsCloned(t *testing.T) {
	streamLabels := []string{"service_name"}
	ts, _ := newTestServer(t, true, pipelineapi.Options{
		CompileOptions: pipeline.CompileOptions{StreamLabels: streamLabels},
	})
	streamLabels[0] = "job" // caller mutation after Register
	doc := dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"svc","steps":[{"type":"rename","from":"a","to":"b"}]}`,
		`{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}`,
	)
	code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", doc)
	if code != http.StatusOK {
		t.Fatalf("dry-run = %d %q", code, body)
	}
	res := decodeJSON[dryRunResult](t, body)
	if len(res.Entries) != 1 || res.Entries[0].Meta["b"] != "1" {
		t.Fatalf("entries = %+v, want the selector to match via service_name", res.Entries)
	}
}

// TestObserveExecutionCleared: administration and dry-run execution emit no
// role metrics — a caller-supplied observer is dropped at Register.
func TestObserveExecutionCleared(t *testing.T) {
	var calls atomic.Int64
	ts, _ := newTestServer(t, true, pipelineapi.Options{
		CompileOptions: pipeline.CompileOptions{
			StreamLabels: []string{"service_name"},
			ObserveExecution: func(pipeline.Stage, pipeline.Outcome, error, time.Duration) {
				calls.Add(1)
			},
		},
	})
	doc := dryRunDoc(
		`{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]}`,
		`{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}`,
	)
	if code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", doc); code != http.StatusOK {
		t.Fatalf("dry-run = %d %q", code, body)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("ObserveExecution called %d times during dry-run, want 0", got)
	}
}

// TestRedactEmptyReplace: an explicit empty replacement round-trips through
// the API: POST persists it in the Spec, GET reports it, and the stored
// definition deletes matches rather than substituting the "***" default.
func TestRedactEmptyReplace(t *testing.T) {
	ts, cat := newTestServer(t, true, pipelineapi.Options{})
	doc := `{"name":"rx","stage":"ingest","selector":"*","steps":[{"type":"redact","pattern":"x","replace":""}]}`
	if code, body := do(t, ts, http.MethodPost, "/api/v1/pipelines", "", doc); code != http.StatusCreated {
		t.Fatalf("create = %d %q", code, body)
	}
	stored, err := cat.GetPipeline(t.Context(), tenant.Default, "rx")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stored.Spec), `"replace":""`) {
		t.Fatalf("stored spec = %s, want empty replace preserved", stored.Spec)
	}
	code, body := do(t, ts, http.MethodGet, "/api/v1/pipelines/rx", "", "")
	if code != http.StatusOK {
		t.Fatalf("GET = %d", code)
	}
	if !strings.Contains(body, `"replace":""`) {
		t.Fatalf("view = %s, want empty replace reported", body)
	}
	// Re-issuing the reported definition (view minus catalog-owned fields)
	// deletes the matches instead of writing the "***" default.
	v := decodeJSON[viewJSON](t, body)
	defB, err := json.Marshal(map[string]any{
		"name": v.Name, "stage": v.Stage, "selector": v.Selector, "steps": v.Steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	dr := `{"pipeline":` + string(defB) + `,"entries":[{"line":"axbx"}]}`
	code, dbody := do(t, ts, http.MethodPost, "/api/v1/pipelines/dry-run", "", dr)
	if code != http.StatusOK {
		t.Fatalf("dry-run = %d %q", code, dbody)
	}
	res := decodeJSON[dryRunResult](t, dbody)
	if len(res.Entries) != 1 || res.Entries[0].Line != "ab" {
		t.Fatalf("dry-run entries = %+v, want line %q deleted to %q", res.Entries, "axbx", "ab")
	}
}

func TestRoutingEdges(t *testing.T) {
	ts, _ := newTestServer(t, true, pipelineapi.Options{})
	// GET/PUT/DELETE treat "dry-run" as an ordinary pipeline name.
	if code, _ := getView(t, ts, "", "dry-run"); code != http.StatusNotFound {
		t.Fatalf("GET dry-run name = %d, want 404", code)
	}
	if code, _ := do(t, ts, http.MethodDelete, "/api/v1/pipelines/dry-run", "", `{"version":1}`); code != http.StatusNotFound {
		t.Fatalf("DELETE dry-run name = %d, want 404", code)
	}
	// A method no pattern serves gets 405.
	if code, _ := do(t, ts, http.MethodPatch, "/api/v1/pipelines", "", `{}`); code != http.StatusMethodNotAllowed {
		t.Fatalf("PATCH list = %d, want 405", code)
	}
	if code, _ := do(t, ts, http.MethodPost, "/api/v1/pipelines/pipe-a", "", defJSON); code != http.StatusMethodNotAllowed {
		t.Fatalf("POST {name} = %d, want 405", code)
	}
}
