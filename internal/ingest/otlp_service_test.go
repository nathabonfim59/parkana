package ingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"

	"github.com/prometheus/client_golang/prometheus"
)

// otlpServiceBody builds a one-record OTLP protobuf export request body.
func otlpServiceBody(t *testing.T, line string) []byte {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.Body().SetStr(line)
	rec.SetTimestamp(pcommon.Timestamp(1600000000500000001))
	b, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// otlpHTTP posts body to POST /v1/logs through the real middleware stack;
// org, when set, is the X-Scope-OrgID tenant.
func otlpHTTP(t *testing.T, deps app.Deps, body []byte, org string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/x-protobuf")
	if org != "" {
		r.Header.Set(tenant.Header, org)
	}
	rec := httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(rec, r)
	return rec
}

// enqueues counts the WAL admissions the fake recorded.
func enqueues(fw *fakeWAL) int {
	n := 0
	for _, c := range fw.callList() {
		if c == "enqueue" {
			n++
		}
	}
	return n
}

// TestOTLPRouteRegistered: the ingester registers POST /v1/logs on its own
// append path — 503 from the closed admission gate before Run, 204 with one
// WAL admission once accepting, 400 for a malformed body without admission.
func TestOTLPRouteRegistered(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	fw := &fakeWAL{}
	s := newTestService(t, deps, fakeWALOpt(fw))
	if !deps.HTTP.Registered(lokiapi.OTLPLogs) {
		t.Fatal("OTLP route not registered")
	}
	if rec := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d before Run, want 503; body %q", rec.Code, rec.Body)
	}
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if rec := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), ""); rec.Code != http.StatusNoContent {
		t.Fatalf("got %d, want 204; body %q", rec.Code, rec.Body)
	}
	if n := enqueues(fw); n != 1 {
		t.Fatalf("enqueues = %d, want 1", n)
	}
	if rec := otlpHTTP(t, deps, []byte{0xff, 0xff}, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d for a malformed body, want 400; body %q", rec.Code, rec.Body)
	}
	if n := enqueues(fw); n != 1 {
		t.Fatalf("enqueues = %d after a rejected push, want still 1", n)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestOTLPRouteDuplicateRejected: an already-registered OTLP route stops the
// ingester from constructing, and so does a whole duplicate ingester.
func TestOTLPRouteDuplicateRejected(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.HTTP.Handle(lokiapi.OTLPLogs, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	_, err := newService(deps, serviceOptions{})
	if err == nil || !strings.Contains(err.Error(), lokiapi.OTLPLogs) || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("err = %v, want the OTLP route rejected as already registered", err)
	}

	deps2, _ := testDeps(t, t.TempDir())
	if _, err := newService(deps2, serviceOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := newService(deps2, serviceOptions{}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("err = %v, want a duplicate ingester rejected", err)
	}
}

// multiTenantDeps is testDeps with the server accepting X-Scope-OrgID tenants,
// so the tenant middleware resolves the header into the request context.
func multiTenantDeps(t *testing.T, dataDir string) app.Deps {
	t.Helper()
	reg := prometheus.NewRegistry()
	log := testLogger()
	srv := server.New(server.Options{Logger: log, Registry: reg, MultiTenant: true})
	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return app.Deps{
		Config: config.Config{
			DataDir:      dataDir,
			StreamLabels: []string{"service_name", "job"},
			Ingest:       testIngest(),
		},
		Logger:  log,
		Store:   store,
		Catalog: catalog.NewMemory(),
		HTTP:    srv,
		Metrics: reg,
	}
}

// TestOTLPTransformHookPropagated: the configured Transform hook reaches the
// OTLP route through the service wiring: it runs exactly once on the request
// tenant, its rewrite is admitted, and its all-drop outcome skips WAL
// admission entirely.
func TestOTLPTransformHookPropagated(t *testing.T) {
	t.Run("rewrite admitted with tenant", func(t *testing.T) {
		deps := multiTenantDeps(t, t.TempDir())
		fw := &fakeWAL{}
		hits := 0
		var seen []tenant.ID
		var gotID tenant.ID
		var gotPayload wal.Payload
		fw.enqueueFn = func(_ context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
			gotID, gotPayload = id, p
			return fw.result(id, nil), nil
		}
		opts := fakeWALOpt(fw)
		opts.transform = func(_ context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
			hits++
			seen = append(seen, id)
			return &logs.PushRequest{Streams: []logs.Stream{{
				Labels:  req.Streams[0].Labels,
				Entries: []logs.Entry{{Timestamp: req.Streams[0].Entries[0].Timestamp, Line: "HOOKED"}},
			}}}, nil
		}
		s := newTestService(t, deps, opts)
		cancel, done := startRun(t, s)
		waitReady(t, s)
		if rec := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), "acme"); rec.Code != http.StatusNoContent {
			t.Fatalf("got %d, want 204; body %q", rec.Code, rec.Body)
		}
		if hits != 1 {
			t.Fatalf("transform hits = %d, want 1", hits)
		}
		if len(seen) != 1 || seen[0] != tenant.ID("acme") {
			t.Fatalf("transform tenants = %v, want [acme]", seen)
		}
		// The admitted record is the transformed request for the request's
		// tenant: compare it with an independent encode of what the hook
		// returned (OTLP service.name=api decodes to the service_name label).
		want := &logs.PushRequest{Streams: []logs.Stream{{
			Labels:  logs.New(logs.Label{Name: "service_name", Value: "api"}),
			Entries: []logs.Entry{{Timestamp: time.Unix(0, 1600000000500000001).UTC(), Line: "HOOKED"}},
		}}}
		wantPayload, err := wal.Encode(tenant.With(t.Context(), tenant.ID("acme")), *want)
		if err != nil {
			t.Fatal(err)
		}
		if gotID != tenant.ID("acme") {
			t.Fatalf("enqueue tenant = %q, want acme", gotID)
		}
		if !reflect.DeepEqual(gotPayload, wantPayload) {
			t.Fatalf("enqueued payload is not the transformed request (sizes %d vs %d)",
				gotPayload.Size(), wantPayload.Size())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
	t.Run("drop all skips admission", func(t *testing.T) {
		deps, _ := testDeps(t, t.TempDir())
		fw := &fakeWAL{}
		opts := fakeWALOpt(fw)
		opts.transform = func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
			return nil, nil
		}
		s := newTestService(t, deps, opts)
		cancel, done := startRun(t, s)
		waitReady(t, s)
		if rec := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), ""); rec.Code != http.StatusNoContent {
			t.Fatalf("got %d, want 204; body %q", rec.Code, rec.Body)
		}
		if n := enqueues(fw); n != 0 {
			t.Fatalf("enqueues = %d, want 0 for a dropped push", n)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
}
