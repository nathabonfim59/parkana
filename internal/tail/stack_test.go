package tail_test

// Shared fixture for the live-tail end-to-end and latency tests: the real
// ingester factory (WAL, memtable, push handler, private NDJSON tail endpoint)
// and the real querier factory (public WebSocket handler) running as two roles
// on one HTTP server, with the configured default querier flush interval.

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tail"
)

// tailStack is a running ingester+querier pair with its HTTP server.
type tailStack struct {
	// base is the http:// base URL.
	base string
	// wsBase is the ws:// base URL for the live-tail endpoint.
	wsBase string
	// reg is the Prometheus registry used by both roles.
	reg *prometheus.Registry
}

// startTailStack starts the real roles on a loopback listener and waits until
// the server is ready. Every service is stopped in t.Cleanup.
func startTailStack(t *testing.T) *tailStack {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := prometheus.NewRegistry()
	srv := server.New(server.Options{Logger: log, Registry: reg})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()

	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	deps := app.Deps{
		Config: config.Config{
			DataDir:      t.TempDir(),
			StreamLabels: []string{"service_name", "job"},
			Ingest: config.Ingest{
				MaxLineSize:      256 * config.KiB,
				MaxRequestSize:   16 * config.MiB,
				MemtableMaxBytes: 256 * config.MiB,
				WALMaxBytes:      128 * config.MiB,
				FlushInterval:    time.Hour,
				UploadInterval:   time.Hour,
				UploadMinBytes:   128 * config.MiB,
				UploadMaxAge:     time.Hour,
				UploadMergeMax:   2 * config.GiB,
			},
			LogQL: config.LogQL{
				MaxEntries: 5000,
				Tail: config.Tail{
					IngesterURLs:  []string{base},
					BufferEntries: 64,
					// FlushInterval stays zero so the service applies
					// config.DefaultTailFlushInterval (100ms), the production
					// default the latency test must exercise.
				},
			},
		},
		Logger:  log,
		Store:   store,
		Catalog: catalog.NewMemory(),
		HTTP:    srv,
		Metrics: reg,
	}

	ingester, err := ingest.New(deps)
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	querier, err := tail.NewQuerier(deps)
	if err != nil {
		t.Fatalf("tail.NewQuerier: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Go(func() { errs <- ingester.Run(ctx) })
	wg.Go(func() { errs <- querier.Run(ctx) })
	wg.Go(func() { errs <- srv.Serve(ctx, ln) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("service error: %v", err)
			}
		}
	})

	waitReady(t, base)
	return &tailStack{base: base, wsBase: "ws://" + ln.Addr().String(), reg: reg}
}

func waitReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/ready")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server never became ready")
}

// waitMetricAtLeast waits until a counter or gauge with the given name
// reaches want.
func waitMetricAtLeast(t *testing.T, reg *prometheus.Registry, name string, want float64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		families, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather metrics: %v", err)
		}
		for _, f := range families {
			if f.GetName() != name {
				continue
			}
			var total float64
			for _, m := range f.GetMetric() {
				if g := m.GetGauge(); g != nil {
					total += g.GetValue()
				}
				if c := m.GetCounter(); c != nil {
					total += c.GetValue()
				}
			}
			if total >= want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("metric %s never reached %v", name, want)
}
