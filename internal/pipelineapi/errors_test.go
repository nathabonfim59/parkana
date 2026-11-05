package pipelineapi_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/pipelineapi"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// errStore mimics a real catalog failure: file paths and SQL text that must
// never reach the client.
var errStore = errors.New(`sqlite: open /srv/parkana/catalog.db: SELECT "version","spec" FROM pipelines WHERE "tenant"='default': disk I/O error`)

// injectCatalog wraps a real catalog and either fails one pipeline
// operation with err or corrupts what it returns, forwarding everything
// else. The underlying catalog still enforces the real contract.
type injectCatalog struct {
	catalog.Catalog
	op  string
	err error
}

func (c injectCatalog) ListPipelines(ctx context.Context, id tenant.ID) ([]catalog.PipelineDefinition, error) {
	if c.op == "list" {
		return nil, c.err
	}
	return c.Catalog.ListPipelines(ctx, id)
}

func (c injectCatalog) GetPipeline(ctx context.Context, id tenant.ID, name string) (catalog.PipelineDefinition, error) {
	if c.op == "get" {
		return catalog.PipelineDefinition{}, c.err
	}
	return c.Catalog.GetPipeline(ctx, id, name)
}

func (c injectCatalog) PutPipeline(ctx context.Context, d catalog.PipelineDefinition, expected int64) (catalog.PipelineDefinition, error) {
	if c.op == "put" {
		return catalog.PipelineDefinition{}, c.err
	}
	d, err := c.Catalog.PutPipeline(ctx, d, expected)
	if err == nil && c.op == "put-corrupt" {
		// The stored view cannot be decoded back: what the API got from
		// the catalog no longer parses as a spec.
		d.Spec = []byte(`{"oops":1}`)
	}
	return d, err
}

func (c injectCatalog) DeletePipeline(ctx context.Context, id tenant.ID, name string, expected int64) (catalog.PipelineDefinition, error) {
	if c.op == "delete" {
		return catalog.PipelineDefinition{}, c.err
	}
	return c.Catalog.DeletePipeline(ctx, id, name, expected)
}

// errServer registers the API over cat on a real server (real tenant
// middleware), with the package logger writing to the returned buffer.
func errServer(t *testing.T, cat catalog.Catalog) (*httptest.Server, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	srv := server.New(server.Options{
		Logger:      slog.New(slog.DiscardHandler),
		Registry:    prometheus.NewRegistry(),
		MultiTenant: true,
	})
	o := pipelineapi.Options{
		Catalog:        cat,
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{"service_name", "job"}},
		Logger:         slog.New(slog.NewTextHandler(&buf, nil)),
	}
	if err := pipelineapi.Register(srv, o); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, &buf
}

// seed writes a live pipeline directly through the real catalog.
func seed(t *testing.T, cat catalog.Catalog, name, spec string) catalog.PipelineDefinition {
	t.Helper()
	d, err := cat.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant: tenant.Default, Name: name, Stage: catalog.PipelineStageIngest,
		Selector: "*", Spec: []byte(spec),
	}, 0)
	if err != nil {
		t.Fatalf("seed %q: %v", name, err)
	}
	return d
}

// TestInternalErrors: every internal failure answers the same fixed plain
// text and reports the real detail — file paths, SQL, stored bytes — to the
// configured logger with the request's method, path and tenant.
func TestInternalErrors(t *testing.T) {
	const fixed = "pipelineapi: internal error\n"
	putDoc := `{"name":"pipe-a","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}],"version":1}`
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T) catalog.Catalog
		method string
		path   string
		org    string
		body   string
		// fragments required in the captured log, never in the body
		wantLog []string
	}{
		{
			name: "list catalog failure",
			setup: func(t *testing.T) catalog.Catalog {
				return injectCatalog{Catalog: catalog.NewMemory(), op: "list", err: errStore}
			},
			method: http.MethodGet, path: "/api/v1/pipelines", org: "acme",
			wantLog: []string{"/srv/parkana/catalog.db", "SELECT", "method=GET", "path=/api/v1/pipelines", "tenant=acme"},
		},
		{
			name: "get catalog failure",
			setup: func(t *testing.T) catalog.Catalog {
				return injectCatalog{Catalog: catalog.NewMemory(), op: "get", err: errStore}
			},
			method: http.MethodGet, path: "/api/v1/pipelines/pipe-a",
			wantLog: []string{"/srv/parkana/catalog.db", "SELECT", "method=GET", "path=/api/v1/pipelines/pipe-a", "tenant=default"},
		},
		{
			name: "create catalog failure",
			setup: func(t *testing.T) catalog.Catalog {
				return injectCatalog{Catalog: catalog.NewMemory(), op: "put", err: errStore}
			},
			method: http.MethodPost, path: "/api/v1/pipelines", body: defJSON,
			wantLog: []string{"/srv/parkana/catalog.db", "SELECT", "method=POST", "path=/api/v1/pipelines", "tenant=default"},
		},
		{
			name: "replace catalog failure",
			setup: func(t *testing.T) catalog.Catalog {
				cat := catalog.NewMemory()
				seed(t, cat, "pipe-a", `{"steps":[{"type":"rename","from":"a","to":"b"}]}`)
				return injectCatalog{Catalog: cat, op: "put", err: errStore}
			},
			method: http.MethodPut, path: "/api/v1/pipelines/pipe-a", body: putDoc,
			wantLog: []string{"/srv/parkana/catalog.db", "SELECT", "method=PUT", "path=/api/v1/pipelines/pipe-a", "tenant=default"},
		},
		{
			name: "delete catalog failure",
			setup: func(t *testing.T) catalog.Catalog {
				cat := catalog.NewMemory()
				seed(t, cat, "pipe-a", `{"steps":[{"type":"rename","from":"a","to":"b"}]}`)
				return injectCatalog{Catalog: cat, op: "delete", err: errStore}
			},
			method: http.MethodDelete, path: "/api/v1/pipelines/pipe-a", body: `{"version":1}`,
			wantLog: []string{"/srv/parkana/catalog.db", "SELECT", "method=DELETE", "path=/api/v1/pipelines/pipe-a", "tenant=default"},
		},
		{
			name: "list undecodable stored spec",
			setup: func(t *testing.T) catalog.Catalog {
				cat := catalog.NewMemory()
				seed(t, cat, "bad", `{"oops":1}`)
				return cat
			},
			method: http.MethodGet, path: "/api/v1/pipelines",
			wantLog: []string{`stored spec`, "bad", "method=GET", "path=/api/v1/pipelines", "tenant=default"},
		},
		{
			name: "get undecodable stored spec",
			setup: func(t *testing.T) catalog.Catalog {
				cat := catalog.NewMemory()
				seed(t, cat, "bad", `{"oops":1}`)
				return cat
			},
			method: http.MethodGet, path: "/api/v1/pipelines/bad",
			wantLog: []string{`stored spec`, "bad", "method=GET", "path=/api/v1/pipelines/bad", "tenant=default"},
		},
		{
			name: "replace undecodable stored view",
			setup: func(t *testing.T) catalog.Catalog {
				cat := catalog.NewMemory()
				seed(t, cat, "pipe-a", `{"steps":[{"type":"rename","from":"a","to":"b"}]}`)
				return injectCatalog{Catalog: cat, op: "put-corrupt"}
			},
			method: http.MethodPut, path: "/api/v1/pipelines/pipe-a", body: putDoc,
			wantLog: []string{`stored spec`, "method=PUT", "path=/api/v1/pipelines/pipe-a", "tenant=default"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, buf := errServer(t, tc.setup(t))
			code, body := do(t, ts, tc.method, tc.path, tc.org, tc.body)
			if code != http.StatusInternalServerError {
				t.Fatalf("%s %s = %d %q, want 500", tc.method, tc.path, code, body)
			}
			if body != fixed {
				t.Fatalf("body = %q, want the fixed %q (no internal detail)", body, fixed)
			}
			for _, frag := range []string{"parkana/catalog.db", "SELECT"} {
				if strings.Contains(body, frag) {
					t.Fatalf("body %q leaks internal detail %q", body, frag)
				}
			}
			for _, frag := range tc.wantLog {
				if !strings.Contains(buf.String(), frag) {
					t.Errorf("log %q missing %q", buf.String(), frag)
				}
			}
		})
	}
}

// TestCatalogClientErrorsKeepDetail: errors.Is-matched catalog failures are
// client errors even when a real backend wraps them in storage text: 400
// and 404 keep their detail and are not rewritten to the fixed internal
// body or logged as internal errors.
func TestCatalogClientErrorsKeepDetail(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		method   string
		path     string
		body     string
		wantCode int
		frag     string
	}{
		{"invalid pipeline", fmt.Errorf("catalog: sqlite: %w: stage %q", catalog.ErrInvalidPipeline, "cron"),
			http.MethodPost, "/api/v1/pipelines", defJSON, http.StatusBadRequest, "cron"},
		{"not found", fmt.Errorf("catalog: sqlite: get pipeline %q: %w", "pipe-a", catalog.ErrNotFound),
			http.MethodGet, "/api/v1/pipelines/pipe-a", "", http.StatusNotFound, "pipe-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := "get"
			if tc.method == http.MethodPost {
				op = "put"
			}
			ts, buf := errServer(t, injectCatalog{
				Catalog: catalog.NewMemory(), op: op, err: tc.err,
			})
			code, body := do(t, ts, tc.method, tc.path, "", tc.body)
			if code != tc.wantCode {
				t.Fatalf("%s %s = %d %q, want %d", tc.method, tc.path, code, body, tc.wantCode)
			}
			if !strings.Contains(body, tc.frag) || !strings.Contains(body, "sqlite") {
				t.Fatalf("%d body = %q, want the catalog detail preserved", tc.wantCode, body)
			}
			if strings.Contains(buf.String(), "internal error") {
				t.Fatalf("log records an internal error for a client fault: %q", buf.String())
			}
		})
	}
}
