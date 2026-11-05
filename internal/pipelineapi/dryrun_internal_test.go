package pipelineapi

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/pipeline"
)

// cancelAfterCtx wraps a real context.WithCancel and cancels it after n
// Err calls, so Done and Err report a genuine cancellation at a chosen
// point in the handler. Handlers are single-threaded per request, so no
// synchronization is needed.
type cancelAfterCtx struct {
	context.Context
	cancel context.CancelFunc
	n      int64
	calls  int64
}

func newCancelAfterCtx(parent context.Context, n int64) *cancelAfterCtx {
	ctx, cancel := context.WithCancel(parent)
	return &cancelAfterCtx{Context: ctx, cancel: cancel, n: n}
}

func (c *cancelAfterCtx) Err() error {
	c.calls++
	if c.calls > c.n {
		c.cancel()
	}
	return c.Context.Err()
}

func testAPI(t *testing.T, log *slog.Logger) *api {
	t.Helper()
	return &api{o: Options{
		Catalog:          catalog.NewMemory(),
		CompileOptions:   pipeline.CompileOptions{StreamLabels: []string{"service_name"}},
		MaxBodyBytes:     1 << 20,
		MaxDryRunEntries: 100,
		Logger:           log,
	}}
}

func dryRunReq(t *testing.T, doc string, n int64) *httptest.ResponseRecorder {
	t.Helper()
	return dryRunReqLog(t, doc, n, slog.New(slog.DiscardHandler))
}

func dryRunReqLog(t *testing.T, doc string, n int64, log *slog.Logger) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/dry-run", strings.NewReader(doc))
	req = req.WithContext(newCancelAfterCtx(t.Context(), n))
	testAPI(t, log).dryRun(rec, req)
	return rec
}

// TestDryRunContextCancel: a canceled request must never execute or write a
// success response, at entry, between validation and Apply (including
// selector misses and empty entry lists).
func TestDryRunContextCancel(t *testing.T) {
	matching := `{"pipeline":{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]},"entries":[{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}]}`
	missing := `{"pipeline":{"name":"p","stage":"ingest","selector":"other","steps":[{"type":"rename","from":"a","to":"b"}]},"entries":[{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}]}`
	empty := `{"pipeline":{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]},"entries":[]}`
	for _, tc := range []struct {
		name        string
		doc         string
		cancelAfter int64
	}{
		{"at entry", matching, 0},
		{"in validation loop", matching, 1},
		{"in execution loop", matching, 2},
		{"in execution loop on selector miss", missing, 2},
		{"empty entries before write", empty, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := dryRunReq(t, tc.doc, tc.cancelAfter)
			if rec.Body.Len() != 0 {
				t.Fatalf("wrote %q despite cancellation", rec.Body)
			}
		})
	}
	// Control: no cancellation writes the normal success response.
	rec := dryRunReq(t, matching, 99)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"entries"`) {
		t.Fatalf("uncanceled dry-run = %d %q, want 200 entries", rec.Code, rec.Body)
	}
}

// TestDryRunApplyInternal: a fault surfacing inside a real compiled
// pipeline's Apply — here a real context.Canceled reaching Apply's own
// context checks — is an internal error: the fixed plain-text body goes out
// and the real cause lands in the configured logger. The counting context
// is a genuine context.WithCancel wrapper: once canceled, Err keeps
// reporting context.Canceled and Done stays closed.
func TestDryRunApplyInternal(t *testing.T) {
	matching := `{"pipeline":{"name":"p","stage":"ingest","selector":"*","steps":[{"type":"rename","from":"a","to":"b"}]},"entries":[{"line":"x","labels":{"service_name":"svc"},"meta":{"a":"1"}}]}`
	// Err() call count for one matching entry on a one-step pipeline:
	// 1 handler entry check, 2 entry validation, 3 execution loop, then
	// Apply's own checks — 4 at entry, 5 per step. n=3 makes the cancel
	// fire at call 4 (Apply's entry check), n=4 at call 5 (its per-step
	// check): the REAL Apply returns a wrapped context.Canceled.
	for _, tc := range []struct {
		name string
		n    int64
	}{
		{"cancel inside apply entry check", 3},
		{"cancel inside apply step check", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			rec := dryRunReqLog(t, matching, tc.n,
				slog.New(slog.NewTextHandler(&buf, nil)))
			if rec.Code != http.StatusInternalServerError ||
				rec.Body.String() != "pipelineapi: internal error\n" {
				t.Fatalf("Apply internal error = %d %q, want 500 fixed body", rec.Code, rec.Body)
			}
			out := buf.String()
			for _, frag := range []string{"pipelineapi: internal error", "context canceled", "method=POST", "path=/api/v1/pipelines/dry-run", "tenant=default"} {
				if !strings.Contains(out, frag) {
					t.Errorf("log %q missing %q", out, frag)
				}
			}
		})
	}
}
