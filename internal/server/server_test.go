package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func start(t *testing.T) (*server.Server, string) {
	t.Helper()
	srv := server.New(server.Options{Logger: slog.New(slog.DiscardHandler), Registry: prometheus.NewRegistry()})
	srv.Handle("GET /boom", http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	srv.Handle("GET /whoami", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, string(tenant.From(r.Context())))
	}))
	lokiapi.RegisterBuildInfo(srv)
	lokiapi.RegisterUnimplemented(srv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	base := "http://" + ln.Addr().String()
	// Wait until /ready flips.
	for range 100 {
		if code, _ := get(t, base+"/ready", nil); code == http.StatusOK {
			return srv, base
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server never became ready")
	return nil, ""
}

func get(t *testing.T, url string, header http.Header) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestServer(t *testing.T) {
	_, base := start(t)

	tests := []struct {
		name     string
		path     string
		header   http.Header
		wantCode int
		wantBody string
	}{
		{"buildinfo", "/loki/api/v1/status/buildinfo", nil, 200, `"version":"3.0.0"`},
		{"unimplemented", "/loki/api/v1/query_range?query={}", nil, 501, "not implemented yet"},
		{"default tenant", "/whoami", nil, 200, "default"},
		{"explicit default tenant", "/whoami", http.Header{tenant.Header: {"default"}}, 200, "default"},
		{"foreign tenant", "/whoami", http.Header{tenant.Header: {"acme"}}, 401, "multi-tenancy is disabled"},
		{"panic recovered", "/boom", nil, 500, "internal error"},
		{"unknown route", "/nope", nil, 404, ""},
		{"metrics", "/metrics", nil, 200, `parkana_http_requests_total{code="200",route="GET /whoami"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := get(t, base+tt.path, tt.header)
			if code != tt.wantCode || !strings.Contains(body, tt.wantBody) {
				t.Fatalf("GET %s = %d %q, want %d containing %q", tt.path, code, body, tt.wantCode, tt.wantBody)
			}
		})
	}
}

func TestReadyCheck(t *testing.T) {
	srv, base := start(t)
	srv.AddReadyCheck("wal", func(context.Context) error { return io.ErrUnexpectedEOF })
	if code, body := get(t, base+"/ready", nil); code != http.StatusServiceUnavailable || !strings.Contains(body, "wal") {
		t.Fatalf("/ready = %d %q, want 503 naming the check", code, body)
	}
}
