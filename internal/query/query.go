package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// allReadPatterns returns every read route the querier owns: the Loki query
// routes registered here, the metadata routes registered by metadataHTTP and
// the SQL endpoint. newService wires all of them through wrappingRegistrar, so
// they share one admission and lifetime stack, and checks every pattern before
// construction registers anything: a conflicting route never leaves a
// half-registered read API. A function returning a fresh literal keeps the
// package free of mutable global state.
func allReadPatterns() []string {
	return []string{
		lokiapi.Query,
		lokiapi.QueryPost,
		lokiapi.QueryRange,
		lokiapi.QueryRangePost,
		lokiapi.Labels,
		lokiapi.LabelsPost,
		lokiapi.LabelValues,
		lokiapi.Series,
		lokiapi.SeriesPost,
		lokiapi.IndexStats,
		lokiapi.IndexVolume,
		lokiapi.IndexVolumeRng,
		sqlAPIPattern,
	}
}

// service is the querier's read API. It owns the DuckDB handle, the object
// cache, the decoded sidecar cache and the scan metrics; the shared object
// store and catalog belong to the process and are never closed here. One
// service answers requests from the moment it is built until Run's context
// ends.
type service struct {
	db       *sql.DB
	cache    *objectCache
	sidecars *sidecarCache
	metrics  *scanMetrics
	sql      *sqlAPI
	src      *source
	// logSrc is what the LogQL engine reads: src, or Options.WrapSource(src).
	// It is set before any route is registered.
	logSrc  plan.Source
	engine  plan.Engine
	log     *slog.Logger
	timeout time.Duration

	// base is canceled at shutdown so in-flight reads stop even though the
	// HTTP server's request contexts are detached from the run context.
	base   context.Context
	cancel context.CancelFunc

	// mu guards closed. Every read route adds to inflight only while closed
	// is false, and shutdown sets closed before waiting, so the WaitGroup
	// never sees an Add race with a Wait. Requests that arrive before Run
	// work as well.
	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup

	// shutdownOnce runs the whole close sequence exactly once: concurrent
	// callers wait for it and all observe the same cleanup error. newService
	// initializes it with sync.OnceValue over close.
	shutdownOnce func() error
}

var _ app.Service = (*service)(nil)

// New builds the querier role: it registers the Loki read endpoints on
// deps.HTTP and evaluates queries with engine over the catalog's files. The
// returned service owns only the resources it created (the DuckDB handle, the
// object cache, the sidecar cache and the scan metrics); deps.Store and
// deps.Catalog are shared.
func New(deps app.Deps, engine plan.Engine) (app.Service, error) {
	return newService(context.Background(), deps, engine)
}

// Options are the querier's pluggable hooks. The zero value is New.
type Options struct {
	// WrapSource, when set, wraps the catalog-backed source the LogQL engine
	// reads (query and query_range), e.g. to apply read pipelines (PLAN.md
	// §6). The wrapper must keep plan.Source semantics for what it passes
	// through and must not change stream labels: the metadata routes
	// (labels, series, index stats and volume) read the unwrapped source.
	WrapSource func(plan.Source) plan.Source
}

// NewWith is New with o's hooks.
func NewWith(deps app.Deps, engine plan.Engine, o Options) (app.Service, error) {
	return newService(context.Background(), deps, engine, o)
}

// newService is New with a caller-supplied construction context, so tests can
// exercise a canceled or expired startup without waiting. Every route check
// happens before any resource is acquired; every later failure closes what was
// already acquired and unregisters its metrics, joining cleanup errors with
// the construction error. The same rollback covers a creator canceled while
// any of the three metric-registering constructors (object cache, sidecar
// cache, scan metrics) is between registration and its own late-cancel check.
func newService(ctx context.Context, deps app.Deps, engine plan.Engine, opts ...Options) (*service, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("query: init: %w", err)
	}
	switch {
	case deps.Logger == nil:
		return nil, errors.New("query: Deps.Logger is required")
	case deps.HTTP == nil:
		return nil, errors.New("query: Deps.HTTP is required")
	case deps.Metrics == nil:
		return nil, errors.New("query: Deps.Metrics is required")
	case deps.Store == nil:
		return nil, errors.New("query: Deps.Store is required")
	case deps.Catalog == nil:
		return nil, errors.New("query: Deps.Catalog is required")
	case engine == nil:
		return nil, errors.New("query: plan.Engine is required")
	}
	if err := checkRoutes(deps.HTTP, allReadPatterns()); err != nil {
		return nil, err
	}

	cache, err := newObjectCache(cacheOptions{
		store:    deps.Store,
		dir:      deps.Config.Querier.CachePath(deps.Config.DataDir),
		budget:   int64(deps.Config.Querier.CacheSize),
		local:    deps.Config.Storage.Backend == "fs",
		logger:   deps.Logger,
		register: deps.Metrics,
	})
	if err != nil {
		return nil, err
	}
	// Building the cache registers its metrics; a creator canceled during
	// that window must not leave the cache open or its collectors behind.
	// This is the late canceled-construction cleanup branch; it shares the
	// same rollback as every other constructor failure.
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(fmt.Errorf("query: init: %w", err), closeCache(cache))
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{
		store:    deps.Store,
		budget:   int64(deps.Config.Querier.SidecarCacheSize),
		logger:   deps.Logger,
		register: deps.Metrics,
	})
	if err != nil {
		return nil, errors.Join(err, closeCache(cache))
	}
	// The sidecar cache registers its metrics too; the same late-cancel
	// rollback applies, with the sidecar cache torn down as well.
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(fmt.Errorf("query: init: %w", err), closeCache(cache), sidecars.Close())
	}
	metrics, err := newScanMetrics(deps.Metrics)
	if err != nil {
		return nil, errors.Join(err, closeCache(cache), sidecars.Close())
	}
	// And once more for the scan metrics' own registration window.
	if err := ctx.Err(); err != nil {
		metrics.unregister()
		return nil, errors.Join(fmt.Errorf("query: init: %w", err), closeCache(cache), sidecars.Close())
	}
	db, err := OpenDB(ctx)
	if err != nil {
		metrics.unregister()
		return nil, errors.Join(fmt.Errorf("query: init: %w", err), closeCache(cache), sidecars.Close())
	}
	exec, err := newSQLExecFromConfig(deps.Config.SQL, deps.Catalog, cache, deps.Logger)
	if err != nil {
		metrics.unregister()
		return nil, errors.Join(err, db.Close(), closeCache(cache), sidecars.Close())
	}
	sqlapi, err := newSQLAPI(deps.Config.SQL, exec, deps.Metrics, deps.Logger)
	if err != nil {
		metrics.unregister()
		return nil, errors.Join(err, db.Close(), closeCache(cache), sidecars.Close())
	}
	// The SQL metrics register too; a creator canceled during that window must
	// not leave them, the DuckDB handle or either cache behind.
	if err := ctx.Err(); err != nil {
		sqlapi.close()
		metrics.unregister()
		return nil, errors.Join(fmt.Errorf("query: init: %w", err), db.Close(), closeCache(cache), sidecars.Close())
	}
	base, cancel := context.WithCancel(context.Background())
	s := &service{
		db:       db,
		cache:    cache,
		sidecars: sidecars,
		metrics:  metrics,
		sql:      sqlapi,
		src: newSourceWithOptions(db, deps.Catalog, cache, deps.Config.StreamLabels, sourceOptions{
			sidecars: sidecars,
			metrics:  metrics,
			logger:   deps.Logger,
		}),
		engine:  engine,
		log:     deps.Logger,
		timeout: deps.Config.Querier.Timeout,
		base:    base,
		cancel:  cancel,
	}
	s.shutdownOnce = sync.OnceValue(s.close)
	s.logSrc = s.src
	for _, o := range opts {
		if o.WrapSource != nil {
			s.logSrc = o.WrapSource(s.logSrc)
		}
	}
	if err := s.register(deps.HTTP); err != nil {
		cancel()
		return nil, errors.Join(err, s.closeOwned())
	}
	meta := &metadataHTTP{
		reader:  newMetadataReader(s.src),
		engine:  engine,
		timeout: s.timeout,
		now:     time.Now,
	}
	meta.register(wrappingRegistrar{Registrar: deps.HTTP, wrap: s.readRoute})
	return s, nil
}

// Run serves reads until ctx is done, then stops admitting them, cancels the
// ones in flight and waits for them before closing the DuckDB handle, the
// object cache, the sidecar cache and the scan metrics. Requests that started
// before Run keep working: only shutdown closes the service.
func (s *service) Run(ctx context.Context) error {
	<-ctx.Done()
	return s.shutdown()
}

// shutdown closes the service exactly once. Repeated and concurrent callers
// wait for the first close to finish and all observe its cleanup error, so no
// once-only state (the cache's metric registry included) is touched twice.
func (s *service) shutdown() error {
	return s.shutdownOnce()
}

// close is the shutdown body, run under shutdownOnce.
func (s *service) close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.inflight.Wait()
	return s.closeOwned()
}

// closeOwned closes the resources the service owns and unregisters their
// metrics. objectCache.Close deliberately leaves its collectors registered
// (leases may still be released after Close), so construction and shutdown
// clean them up here; sidecarCache.Close unregisters its own collectors, and
// the scan metrics are unregistered last, after every owner stopped counting.
func (s *service) closeOwned() error {
	err := errors.Join(s.db.Close(), closeCache(s.cache), s.sidecars.Close())
	s.metrics.unregister()
	s.sql.close()
	return err
}

// closeCache closes cache and unregisters its metrics, preserving both errors.
func closeCache(cache *objectCache) error {
	err := cache.Close()
	if cache.metrics != nil {
		cache.metrics.unregister()
	}
	return err
}

// begin reserves one in-flight read. It returns false once shutdown has
// started; the returned release must be called exactly once when the read
// finishes.
func (s *service) begin() (release func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	s.inflight.Add(1)
	return s.inflight.Done, true
}

// readRoute admits one read request and serves h with a context tied to the
// request, the service lifetime and the configured query deadline. Every read
// route, query and metadata alike, is registered through this wrapper, so
// shutdown cancels and waits for all of them.
func (s *service) readRoute(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done, ok := s.begin()
		if !ok {
			lokiapi.Error(w, http.StatusServiceUnavailable, "parkana: querier is shutting down")
			return
		}
		defer done()
		ctx, cancel := s.requestContext(r)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requestContext joins the request's own context (tenant, client
// cancellation) with the service lifetime and, when configured, the query
// deadline. Canceling any of the three cancels the query.
func (s *service) requestContext(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(s.base, cancel)
	if s.timeout > 0 {
		ctx, cancelTimeout := context.WithTimeout(ctx, s.timeout)
		return ctx, func() {
			cancelTimeout()
			stop()
			cancel()
		}
	}
	return ctx, func() {
		stop()
		cancel()
	}
}

// wrappingRegistrar wraps every handler it registers with wrap. The service
// registers its query handlers through it, and the metadata handler's
// register method is handed the same wrapper when it is wired in, so query and
// metadata reads share one admission and lifetime stack.
type wrappingRegistrar struct {
	lokiapi.Registrar
	wrap func(http.Handler) http.Handler
}

func (r wrappingRegistrar) Handle(pattern string, h http.Handler) {
	r.Registrar.Handle(pattern, r.wrap(h))
}

// register installs the query handlers through the shared read wrapper.
// newService registers the metadata handlers the same way, with its
// metadataHTTP built over newMetadataReader(s.src).
func (s *service) register(r lokiapi.Registrar) error {
	routes := []struct {
		pattern string
		handler http.Handler
	}{
		{lokiapi.Query, http.HandlerFunc(s.handleQuery)},
		{lokiapi.QueryPost, http.HandlerFunc(s.handleQuery)},
		{lokiapi.QueryRange, http.HandlerFunc(s.handleQueryRange)},
		{lokiapi.QueryRangePost, http.HandlerFunc(s.handleQueryRange)},
		{sqlAPIPattern, s.sql},
	}
	for _, rt := range routes {
		if r.Registered(rt.pattern) {
			return fmt.Errorf("query: route %q already registered", rt.pattern)
		}
	}
	reg := wrappingRegistrar{Registrar: r, wrap: s.readRoute}
	for _, rt := range routes {
		reg.Handle(rt.pattern, rt.handler)
	}
	return nil
}

// checkRoutes returns an error when any pattern is already registered.
func checkRoutes(r lokiapi.Registrar, patterns []string) error {
	for _, pattern := range patterns {
		if r.Registered(pattern) {
			return fmt.Errorf("query: route %q already registered", pattern)
		}
	}
	return nil
}
