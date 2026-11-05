package query

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// engineCall is what recordingEngine observed for one Query call.
type engineCall struct {
	params      plan.Params
	src         plan.Source
	hasDeadline bool
	deadline    time.Time
}

// recordingEngine is a plan.Engine test double: it records every call and
// either returns a canned result or runs a test hook.
type recordingEngine struct {
	mu     sync.Mutex
	calls  []engineCall
	result plan.Result
	err    error
	query  func(context.Context, plan.Source, plan.Params) (plan.Result, error)
}

func (f *recordingEngine) Query(ctx context.Context, src plan.Source, p plan.Params) (plan.Result, error) {
	deadline, ok := ctx.Deadline()
	f.mu.Lock()
	f.calls = append(f.calls, engineCall{params: p, src: src, hasDeadline: ok, deadline: deadline})
	hook := f.query
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, src, p)
	}
	return f.result, f.err
}

func (f *recordingEngine) ParseSelector(string) ([]plan.Matcher, error) {
	return nil, errors.New("recording engine: ParseSelector is not used by the read handlers")
}

func (f *recordingEngine) call(t *testing.T, i int) engineCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if i >= len(f.calls) {
		t.Fatalf("engine call %d missing; got %d calls", i, len(f.calls))
	}
	return f.calls[i]
}

func (f *recordingEngine) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// newTestDeps builds the shared dependencies a querier needs, over a fresh
// temporary FS object store and SQLite catalog. mutate adjusts the config
// before the service is built.
func newTestDeps(t *testing.T, mutate func(*config.Config)) app.Deps {
	t.Helper()
	dir := t.TempDir()
	store, err := objstore.NewFS(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.NewSQLite(filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cat.Close(); err != nil {
			t.Errorf("catalog close: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	logger := slog.New(slog.DiscardHandler)
	reg := prometheus.NewRegistry()
	srv := server.New(server.Options{Logger: logger, Registry: reg})
	cfg := config.Config{
		DataDir:      dir,
		Storage:      config.Storage{Backend: "fs"},
		StreamLabels: []string{"service_name", "job"},
		Querier: config.Querier{
			CacheSize:        config.GiB,
			SidecarCacheSize: 64 * config.MiB,
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return app.Deps{Config: cfg, Logger: logger, Store: store, Catalog: cat, HTTP: srv, Metrics: reg}
}

// newTestService builds the service and shuts it down when the test ends.
func newTestService(t *testing.T, deps app.Deps, engine plan.Engine) *service {
	t.Helper()
	s, err := newService(t.Context(), deps, engine)
	if err != nil {
		t.Fatalf("newService: %v", err)
	}
	t.Cleanup(func() {
		if err := s.shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return s
}

func TestServiceConstructorValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*app.Deps)
	}{
		{"nil logger", func(d *app.Deps) { d.Logger = nil }},
		{"nil http", func(d *app.Deps) { d.HTTP = nil }},
		{"nil metrics", func(d *app.Deps) { d.Metrics = nil }},
		{"nil store", func(d *app.Deps) { d.Store = nil }},
		{"nil catalog", func(d *app.Deps) { d.Catalog = nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := newTestDeps(t, nil)
			tc.mutate(&deps)
			if s, err := newService(t.Context(), deps, &recordingEngine{}); err == nil {
				s.shutdown()
				t.Fatal("newService succeeded, want an error")
			}
		})
	}
	t.Run("nil engine", func(t *testing.T) {
		deps := newTestDeps(t, nil)
		if s, err := newService(t.Context(), deps, nil); err == nil {
			s.shutdown()
			t.Fatal("newService succeeded, want an error")
		}
	})
}

func TestServiceCanceledInitContext(t *testing.T) {
	deps := newTestDeps(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s, err := newService(ctx, deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService returned a service for a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newService error = %v, want context.Canceled", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
}

// TestServiceDuplicateRoutePrecheck checks all read routes, metadata included,
// before any resource is built: a duplicate leaves no partial registration and
// no registered metrics behind.
func TestServiceDuplicateRoutePrecheck(t *testing.T) {
	deps := newTestDeps(t, nil)
	deps.HTTP.Handle(lokiapi.IndexVolume, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if s, err := newService(t.Context(), deps, &recordingEngine{}); err == nil {
		s.shutdown()
		t.Fatal("newService succeeded despite a duplicate route")
	} else if !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("newService error = %v, want an already-registered error", err)
	}
	for _, pattern := range []string{lokiapi.Query, lokiapi.QueryPost, lokiapi.QueryRange, lokiapi.QueryRangePost} {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered by a failed construction", pattern)
		}
	}
	// The cache registers metrics during construction; a failure before that
	// must leave the registry reusable.
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("metrics leaked from failed construction: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatalf("close cache: %v", err)
	}
}

// TestCloseCacheUnregistersMetrics pins the constructor-failure cleanup:
// objectCache.Close keeps its collectors registered, so closeCache must drop
// them explicitly.
func TestCloseCacheUnregistersMetrics(t *testing.T) {
	deps := newTestDeps(t, nil)
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatal(err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	again, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics were not unregistered: %v", err)
	}
	if err := closeCache(again); err != nil {
		t.Fatal(err)
	}
}

// cancelRegisterer cancels a construction context the first time a collector
// is registered through it. It turns the cache's metric registration into a
// deterministic late-constructor failure without a production test hook.
type cancelRegisterer struct {
	prometheus.Registerer
	cancel context.CancelFunc
}

func (r *cancelRegisterer) Register(c prometheus.Collector) error {
	if err := r.Registerer.Register(c); err != nil {
		return err
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	return nil
}

// TestServiceLateCanceledConstructionClosesCache is the real newService
// regression for late canceled construction cleanup: the creating context is
// canceled while the cache registers its metrics, after the cache exists.
// newService must return nil, report context.Canceled, register no routes and
// unregister the cache's collectors. If the late failure path skips
// closeCache, the final cache construction below fails on duplicate metric
// registration. It covers this cancellation path only, not an OpenDB driver
// failure, which the synchronous opener cannot be made to report.
func TestServiceLateCanceledConstructionClosesCache(t *testing.T) {
	deps := newTestDeps(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deps.Metrics = &cancelRegisterer{Registerer: deps.Metrics, cancel: cancel}

	s, err := newService(ctx, deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService returned a service after a canceled construction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newService error = %v, want context.Canceled", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
}

// collectorName returns the first metric family name a collector describes, so
// a registerer can pick out one collector deterministically.
func collectorName(c prometheus.Collector) string {
	ch := make(chan *prometheus.Desc, 16)
	go func() {
		c.Describe(ch)
		close(ch)
	}()
	for d := range ch {
		return d.String()
	}
	return ""
}

// cancelOnMetric cancels the construction context when a collector whose
// description mentions name registers through it. Unlike cancelRegisterer it
// lets earlier constructors finish, so the cancellation provably lands after
// the named collectors registered — the deterministic late failure for each
// metric-registering constructor in turn.
type cancelOnMetric struct {
	prometheus.Registerer
	name   string
	cancel context.CancelFunc
}

func (r *cancelOnMetric) Register(c prometheus.Collector) error {
	if err := r.Registerer.Register(c); err != nil {
		return err
	}
	if r.cancel != nil && strings.Contains(collectorName(c), r.name) {
		r.cancel()
		r.cancel = nil
	}
	return nil
}

// TestServiceLateCanceledAfterSidecarMetrics proves the rollback reaches the
// sidecar cache branch: the creating context is canceled while the SIDECAR
// collectors register — after the object cache finished — and newService must
// still return nil, register no routes and leave every collector unregistered.
func TestServiceLateCanceledAfterSidecarMetrics(t *testing.T) {
	deps := newTestDeps(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deps.Metrics = &cancelOnMetric{Registerer: deps.Metrics, name: "parkana_querier_sidecar_cache_requests_total", cancel: cancel}

	s, err := newService(ctx, deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService returned a service after a canceled construction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newService error = %v, want context.Canceled", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	// Every collector group must be gone: all three construct again.
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics leaked from the failed constructor: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
	scan, err := newScanMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("scan metrics registered by the failed constructor: %v", err)
	}
	scan.unregister()
}

// TestServiceLateCanceledAfterScanMetrics is the same proof for the last
// metric group: cancellation while the scan counters register rolls back the
// object cache, the sidecar cache and the scan metrics together.
func TestServiceLateCanceledAfterScanMetrics(t *testing.T) {
	deps := newTestDeps(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deps.Metrics = &cancelOnMetric{Registerer: deps.Metrics, name: "parkana_query_files_total", cancel: cancel}

	s, err := newService(ctx, deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService returned a service after a canceled construction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newService error = %v, want context.Canceled", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics leaked from the failed constructor: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
	scan, err := newScanMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("scan metrics registered by the failed constructor: %v", err)
	}
	scan.unregister()
}

// TestServiceMetricCollisionRollsBack pre-registers a
// parkana_query_bytes_scanned_total collector — the SECOND scan collector, so
// the files counter registers first and must be rolled back with it — and the
// scan metrics constructor collides after both caches were built: newService
// must fail, register no routes and close both caches with their collectors
// unregistered, while the squatter stays untouched.
func TestServiceMetricCollisionRollsBack(t *testing.T) {
	deps := newTestDeps(t, nil)
	squatter := prometheus.NewCounter(prometheus.CounterOpts{Name: "parkana_query_bytes_scanned_total", Help: "already here"})
	if err := deps.Metrics.Register(squatter); err != nil {
		t.Fatal(err)
	}
	s, err := newService(t.Context(), deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService succeeded despite a metric collision")
	}
	if errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "register") {
		t.Fatalf("newService error = %v, want a registration failure", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	// The files counter registered before the collided bytes counter and must
	// be rolled back with it: its pre-initialized series would stay visible
	// if it leaked. The squatter keeps its own name.
	gather, ok := deps.Metrics.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("deps.Metrics (%T) cannot be gathered", deps.Metrics)
	}
	if seriesPresent(t, gather, "parkana_query_files_total", map[string]string{"result": "scanned"}) {
		t.Error("the files counter registered before the collision was not rolled back")
	}
	if !metricPresent(t, gather, "parkana_query_bytes_scanned_total") {
		t.Error("the pre-registered squatter was unregistered by the rollback")
	}
	// Both earlier caches must have unregistered cleanly.
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics leaked from the failed constructor: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestServiceSidecarMetricCollisionRollsBack pre-registers a
// parkana_querier_sidecar_cache_bytes collector — a NON-first sidecar
// collector, so the earlier sidecar collectors register and must be rolled
// back when the collision hits — making the sidecar cache construction fail
// after the object cache was built. newService must fail, register no routes,
// close the object cache and leave the squatter alone.
func TestServiceSidecarMetricCollisionRollsBack(t *testing.T) {
	deps := newTestDeps(t, nil)
	squatter := prometheus.NewGauge(prometheus.GaugeOpts{Name: "parkana_querier_sidecar_cache_bytes", Help: "already here"})
	if err := deps.Metrics.Register(squatter); err != nil {
		t.Fatal(err)
	}
	s, err := newService(t.Context(), deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService succeeded despite a metric collision")
	}
	if errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "register") {
		t.Fatalf("newService error = %v, want a registration failure", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	gather, ok := deps.Metrics.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("deps.Metrics (%T) cannot be gathered", deps.Metrics)
	}
	// The sidecar collectors that registered before the collided one must be
	// rolled back with it: their pre-initialized series would stay visible if
	// they leaked. The squatter keeps its own name.
	if seriesPresent(t, gather, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "hit"}) {
		t.Error("the sidecar collectors registered before the collision were not rolled back")
	}
	if !metricPresent(t, gather, "parkana_querier_sidecar_cache_bytes") {
		t.Error("the pre-registered squatter was unregistered by the rollback")
	}
	// The object cache closed before the failure and unregistered cleanly.
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
}

// TestServiceShutdownClosesSidecarCacheAndScanMetrics proves normal shutdown
// closes the sidecar cache (new Gets refuse) and unregisters the scan metrics,
// so a fresh service could be built on the same registry.
func TestServiceShutdownClosesSidecarCacheAndScanMetrics(t *testing.T) {
	deps := newTestDeps(t, nil)
	svc := newTestService(t, deps, &recordingEngine{result: plan.Result{Type: plan.TypeStreams}})
	if err := svc.shutdown(); err != nil {
		t.Fatal(err)
	}
	key := objstore.DataKey(tenant.Default, "api", plantest.T0, uuid.NewV7())
	if _, err := svc.sidecars.Get(t.Context(), tenant.Default, key); !errors.Is(err, errSidecarCacheClosed) {
		t.Errorf("sidecar Get after shutdown = %v, want errSidecarCacheClosed", err)
	}
	scan, err := newScanMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("scan metrics still registered after shutdown: %v", err)
	}
	scan.unregister()
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics still registered after shutdown: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestServiceRegistersAllReadRoutes is the real constructor check: New must
// install all 12 read patterns, query and metadata alike.
func TestServiceRegistersAllReadRoutes(t *testing.T) {
	deps := newTestDeps(t, nil)
	newTestService(t, deps, &recordingEngine{})
	for _, pattern := range allReadPatterns() {
		if !deps.HTTP.Registered(pattern) {
			t.Errorf("route %q not registered", pattern)
		}
	}
}

// TestMetadataRegisterThroughReadRoute proves the metadata handlers register
// through wrappingRegistrar: all eight routes are installed, requests reach the
// reader, and after shutdown the same admission gate answers 503 instead of
// entering the handler. New has already registered the real metadata handlers
// on deps.HTTP, so this narrow fake uses an independent server: a fresh
// registrar with svc.readRoute and the same middleware stack.
func TestMetadataRegisterThroughReadRoute(t *testing.T) {
	deps := newTestDeps(t, nil)
	engine := &recordingEngine{}
	svc := newTestService(t, deps, engine)
	fakeSrv := newIndependentServer(t, deps)
	reader := &fakeMetaReader{names: []string{"app", "env"}}
	h := &metadataHTTP{reader: reader, engine: engine, timeout: time.Second, now: time.Now}
	h.register(wrappingRegistrar{Registrar: fakeSrv, wrap: svc.readRoute})

	want := []string{
		lokiapi.Labels, lokiapi.LabelsPost, lokiapi.LabelValues,
		lokiapi.Series, lokiapi.SeriesPost, lokiapi.IndexStats,
		lokiapi.IndexVolume, lokiapi.IndexVolumeRng,
	}
	for _, pattern := range want {
		if !fakeSrv.Registered(pattern) {
			t.Errorf("metadata route %q not registered", pattern)
		}
	}

	w := httptest.NewRecorder()
	fakeSrv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("labels status = %d, body = %s", w.Code, w.Body)
	}
	if len(reader.gotNames) != 1 {
		t.Fatalf("reader calls = %d, want 1", len(reader.gotNames))
	}

	if err := svc.shutdown(); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	fakeSrv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("labels after shutdown = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	if len(reader.gotNames) != 1 {
		t.Errorf("reader called after shutdown: %d calls", len(reader.gotNames))
	}
}

// newIndependentServer returns a server with the same middleware as the
// shared one but its own routes and metrics, so a test can register fake
// handlers without colliding with the real ones New installed on deps.HTTP.
func newIndependentServer(t *testing.T, deps app.Deps) *server.Server {
	t.Helper()
	return server.New(server.Options{Logger: deps.Logger, Registry: prometheus.NewRegistry()})
}

// blockingMetaReader blocks LabelNames until its context ends, then until the
// test releases it, and signals both steps. The embedded fake answers the
// endpoints this test does not use.
type blockingMetaReader struct {
	fakeMetaReader
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (r *blockingMetaReader) LabelNames(ctx context.Context, _ metaQuery) ([]string, error) {
	close(r.started)
	<-ctx.Done()
	close(r.canceled)
	<-r.release
	return nil, ctx.Err()
}

// TestMetadataReadSharesShutdown proves a metadata read registered through
// wrappingRegistrar uses the same lifetime as query reads: shutdown cancels
// it and waits for it before closing the owned resources.
func TestMetadataReadSharesShutdown(t *testing.T) {
	deps := newTestDeps(t, nil)
	svc := newTestService(t, deps, &recordingEngine{})
	fakeSrv := newIndependentServer(t, deps)
	reader := &blockingMetaReader{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
	h := &metadataHTTP{reader: reader, engine: &recordingEngine{}, timeout: time.Minute, now: time.Now}
	h.register(wrappingRegistrar{Registrar: fakeSrv, wrap: svc.readRoute})

	runCtx, cancelRun := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(runCtx) }()

	readDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		fakeSrv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil))
		readDone <- w
	}()
	<-reader.started

	cancelRun()
	<-reader.canceled
	select {
	case err := <-runErr:
		t.Fatalf("Run returned before the metadata read finished: %v", err)
	default:
	}

	close(reader.release)
	w := <-readDone
	if w.Code != statusClientClosedRequest {
		t.Errorf("canceled metadata read = %d, want %d; body = %s", w.Code, statusClientClosedRequest, w.Body)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestServiceServesBeforeRun proves a built service answers requests without
// Run having started.
func TestServiceServesBeforeRun(t *testing.T) {
	deps := newTestDeps(t, nil)
	engine := &recordingEngine{result: plan.Result{Type: plan.TypeStreams}}
	newTestService(t, deps, engine)
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query=%7Bservice_name%3D%22api%22%7D", nil)
	w := httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}
	if got := engine.call(t, 0).params.Tenant; got != tenant.Default {
		t.Errorf("tenant = %q, want %q", got, tenant.Default)
	}
}

// TestServiceShutdownCancelsAndWaitsReads proves shutdown waits for an active
// read after canceling it, then closes only the resources it owns.
func TestServiceShutdownCancelsAndWaitsReads(t *testing.T) {
	deps := newTestDeps(t, nil)
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	engine := &recordingEngine{query: func(ctx context.Context, _ plan.Source, _ plan.Params) (plan.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return plan.Result{}, ctx.Err()
	}}
	svc := newTestService(t, deps, engine)
	runCtx, cancelRun := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(runCtx) }()

	readDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?query=%7Bservice_name%3D%22api%22%7D", nil)
		w := httptest.NewRecorder()
		deps.HTTP.Handler().ServeHTTP(w, r)
		readDone <- w
	}()
	<-started

	cancelRun()
	<-canceled
	select {
	case err := <-runErr:
		t.Fatalf("Run returned before the read finished: %v", err)
	default:
	}

	close(release)
	w := <-readDone
	if w.Code != statusClientClosedRequest {
		t.Errorf("canceled read status = %d, want %d; body = %s", w.Code, statusClientClosedRequest, w.Body)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if err := svc.db.Ping(); err == nil {
		t.Error("DuckDB handle still open after shutdown")
	}
	key := objstore.DataKey(tenant.Default, "api", time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC), uuid.NewV7())
	if _, _, err := svc.cache.Acquire(t.Context(), tenant.Default, key); !errors.Is(err, errCacheClosed) {
		t.Errorf("cache Acquire after shutdown = %v, want errCacheClosed", err)
	}

	// The shared store and catalog outlive the role.
	if err := deps.Store.Put(t.Context(), key, strings.NewReader("alive"), 5); err != nil {
		t.Fatalf("shared store unusable after shutdown: %v", err)
	}
	rc, err := deps.Store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("shared store unusable after shutdown: %v", err)
	}
	body, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || string(body) != "alive" {
		t.Fatalf("shared store content = %q, %v", body, err)
	}
	if _, err := deps.Catalog.Streams(t.Context(), tenant.Default); err != nil {
		t.Fatalf("shared catalog unusable after shutdown: %v", err)
	}
}

// TestServiceShutdownConcurrent proves concurrent shutdown callers share one
// close sequence and all observe the same error. Run under -race to catch
// unsynchronized cleanup (the cache metric registry included).
func TestServiceShutdownConcurrent(t *testing.T) {
	deps := newTestDeps(t, nil)
	svc := newTestService(t, deps, &recordingEngine{result: plan.Result{Type: plan.TypeStreams}})

	const callers = 8
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			errs <- svc.shutdown()
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}
}

// TestServiceShutdownRejectsNewReads proves the production New admission
// gate covers every read route, not only the query handlers: after shutdown,
// one request for each of the 12 registered patterns must answer 503 before
// parsing, and no engine query may run.
func TestServiceShutdownRejectsNewReads(t *testing.T) {
	deps := newTestDeps(t, nil)
	engine := &recordingEngine{result: plan.Result{Type: plan.TypeStreams}}
	svc := newTestService(t, deps, engine)
	if err := svc.shutdown(); err != nil {
		t.Fatal(err)
	}
	for _, pattern := range allReadPatterns() {
		method, target, ok := strings.Cut(pattern, " ")
		if !ok {
			t.Fatalf("route pattern %q has no method", pattern)
		}
		target = strings.ReplaceAll(target, "{name}", "level")
		w := httptest.NewRecorder()
		deps.HTTP.Handler().ServeHTTP(w, httptest.NewRequest(method, target, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want %d", pattern, w.Code, http.StatusServiceUnavailable)
		}
	}
	if n := engine.callCount(); n != 0 {
		t.Errorf("engine called %d times after shutdown", n)
	}
}

// TestServiceRealFSSource exercises the whole stack the handlers depend on:
// the FS object cache hands DuckDB a real Parquet file listed by the catalog,
// and the engine collects rows through the plan.Source interface.
func TestServiceRealFSSource(t *testing.T) {
	deps := newTestDeps(t, nil)
	file := plantest.Dataset()[0]
	plantest.Load(t, deps.Store, deps.Catalog, []plantest.File{file})

	var selected []plan.Row
	engine := &recordingEngine{query: func(ctx context.Context, src plan.Source, p plan.Params) (plan.Result, error) {
		sel := plan.Selection{
			Tenant:    p.Tenant,
			Matchers:  []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "api")},
			From:      p.Start,
			Through:   p.End,
			Direction: p.Direction,
			Limit:     p.Limit,
		}
		for row, err := range src.Select(ctx, sel) {
			if err != nil {
				return plan.Result{}, err
			}
			selected = append(selected, row)
		}
		if len(selected) == 0 {
			return plan.Result{Type: plan.TypeStreams}, nil
		}
		entries := make([]plan.Entry, len(selected))
		for i, row := range selected {
			entries[i] = plan.Entry{TS: row.TS, Line: row.Line, Meta: row.Meta}
		}
		return plan.Result{
			Type:    plan.TypeStreams,
			Streams: []plan.Stream{{Labels: selected[0].Labels, Entries: entries}},
		}, nil
	}}
	newTestService(t, deps, engine)

	start := plantest.T0.Add(-time.Hour).Format(time.RFC3339)
	end := plantest.T0.Add(2 * time.Hour).Format(time.RFC3339)
	target := "/loki/api/v1/query_range?query=%7Bservice_name%3D%22api%22%7D&start=" + start + "&end=" + end + "&step=60&limit=1000"
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r = r.WithContext(tenant.With(r.Context(), tenant.Default))
	w := httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}

	want := 0
	for _, row := range file.Rows {
		if !row.TS.Before(plantest.T0.Add(-time.Hour)) && row.TS.Before(plantest.T0.Add(2*time.Hour)) {
			want++
		}
	}
	if want == 0 {
		t.Fatal("fixture has no rows in the query range")
	}
	if len(selected) != want {
		t.Fatalf("source selected %d rows, want %d", len(selected), want)
	}
	var body queryResponseJSON
	if err := unmarshalJSON(t, w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if got := body.Data.Stats.Summary.TotalEntriesReturned; got != int64(want) {
		t.Errorf("totalEntriesReturned = %d, want %d", got, want)
	}
}

// TestNewMetadataOverFSSource exercises the real New wiring end to end: a
// labels request and an index stats request go through the production
// metadata handlers, the real logql.ParseSelector, the wrappingRegistrar
// admission stack and the FS-backed Source over a loaded fixture. It needs the
// SQL metadata reader, so it runs once that implementation is relayed.
func TestNewMetadataOverFSSource(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) {
		c.Querier.Timeout = 30 * time.Second
	})
	plantest.Load(t, deps.Store, deps.Catalog, plantest.Dataset())
	newTestService(t, deps, logql.New(deps.Config.LogQL))

	start := plantest.T0.Add(-time.Hour).Format(time.RFC3339)
	end := plantest.T0.Add(2 * time.Hour).Format(time.RFC3339)
	rangeParams := func() url.Values {
		return url.Values{"start": {start}, "end": {end}}
	}

	w := httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels?"+rangeParams().Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("labels status = %d, body = %s", w.Code, w.Body)
	}
	var labels labelsResponse
	if err := unmarshalJSON(t, w.Body.Bytes(), &labels); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(labels.Data, "service_name") {
		t.Errorf("labels = %v, want service_name", labels.Data)
	}

	params := rangeParams()
	params.Set("query", `{service_name="api"}`)
	w = httptest.NewRecorder()
	deps.HTTP.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/index/stats?"+params.Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("stats status = %d, body = %s", w.Code, w.Body)
	}
	var stats indexStatsResponse
	if err := unmarshalJSON(t, w.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Entries == 0 || stats.Streams == 0 || stats.Chunks == 0 || stats.Bytes == 0 {
		t.Errorf("stats = %+v, want non-zero measured fields", stats)
	}
}

// wrappedSource marks a source produced by Options.WrapSource.
type wrappedSource struct{ plan.Source }

// TestServiceWrapSource proves the LogQL routes read the wrapped source while
// the service keeps the catalog-backed one for metadata.
func TestServiceWrapSource(t *testing.T) {
	deps := newTestDeps(t, nil)
	engine := &recordingEngine{result: plan.Result{Type: plan.TypeStreams}}
	var inner plan.Source
	s, err := newService(t.Context(), deps, engine, Options{WrapSource: func(src plan.Source) plan.Source {
		inner = src
		return wrappedSource{src}
	}})
	if err != nil {
		t.Fatalf("newService: %v", err)
	}
	t.Cleanup(func() {
		if err := s.shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	if inner != plan.Source(s.src) {
		t.Fatalf("WrapSource got %T, want the catalog-backed source", inner)
	}
	for _, path := range []string{
		"/loki/api/v1/query?query=%7Bservice_name%3D%22api%22%7D",
		"/loki/api/v1/query_range?query=%7Bservice_name%3D%22api%22%7D",
	} {
		w := httptest.NewRecorder()
		deps.HTTP.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, body = %s", path, w.Code, w.Body)
		}
	}
	for i := range 2 {
		if _, ok := engine.call(t, i).src.(wrappedSource); !ok {
			t.Errorf("call %d read %T, want the wrapped source", i, engine.call(t, i).src)
		}
	}
}
