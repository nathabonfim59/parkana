// Command parkana runs the Parkana log store. One binary hosts every role;
// -roles picks which run in this process.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/compactor"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/pipelineapi"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/query"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tail"
	"github.com/nathabonfim59/parkana/internal/version"
)

// roleFactories builds the roles that need no pipeline registry. A nil
// factory means the role isn't implemented yet; its endpoints answer 501.
var roleFactories = map[config.Role]app.Factory{
	config.RoleCompactor: compactor.New,
	config.RoleScheduler: nil,
}

// pipelineWiring owns the process-wide pipeline assembly: the shared
// administration API and one registry per pipeline-using role. It is not a
// store of role state; the registries are independent and talk to each other
// only through the catalog.
type pipelineWiring struct {
	cfg       config.Config
	cat       catalog.Catalog
	log       *slog.Logger
	metrics   prometheus.Registerer
	lineBytes int

	mu   sync.Mutex
	regs []*pipeline.Registry
}

func newPipelineWiring(cfg config.Config, log *slog.Logger, cat catalog.Catalog, metrics prometheus.Registerer) (*pipelineWiring, error) {
	lineBytes, err := checkedLineBytes(cfg.Ingest.MaxLineSize)
	if err != nil {
		return nil, err
	}
	return &pipelineWiring{cfg: cfg, cat: cat, log: log, metrics: metrics, lineBytes: lineBytes}, nil
}

// checkedLineBytes converts the configured line limit to the compiler's int
// limit without a silent narrowing conversion.
func checkedLineBytes(size config.ByteSize) (int, error) {
	n := int(size)
	if config.ByteSize(n) != size {
		return 0, fmt.Errorf("pipeline: -ingest.max-line-size %d does not fit in int", int64(size))
	}
	return n, nil
}

// registerAPI installs the pipeline administration routes once on the process
// server. Callers call it when at least one pipeline-using role is enabled,
// before any role is constructed.
func (w *pipelineWiring) registerAPI(r lokiapi.Registrar) error {
	return pipelineapi.Register(r, pipelineapi.Options{
		Catalog:        w.cat,
		CompileOptions: w.compileOptions(),
		Logger:         w.log,
	})
}

// compileOptions are the limits every compile in this process uses. The
// routing keys select and write datasets exactly as the ingester does, and
// the line bound mirrors -ingest.max-line-size so a default-keep transform
// can never produce a line the push handler's revalidation would reject.
func (w *pipelineWiring) compileOptions() pipeline.CompileOptions {
	return pipeline.CompileOptions{
		StreamLabels: w.cfg.StreamLabels,
		LineBytes:    w.lineBytes,
	}
}

// roleFactory returns the factory for one role, building its registry now so
// registry construction failures abort startup. Compactor and scheduler need
// no registry and never serve the pipeline API.
func (w *pipelineWiring) roleFactory(role config.Role) (app.Factory, error) {
	switch role {
	case config.RoleIngester:
		return w.ingesterRole()
	case config.RoleQuerier:
		return w.querierRole()
	default:
		return roleFactories[role], nil
	}
}

// ingesterRole builds the ingester with its own registry and the ingest
// transform over it.
func (w *pipelineWiring) ingesterRole() (app.Factory, error) {
	reg, err := w.newRegistry(pipeline.RoleIngester)
	if err != nil {
		return nil, err
	}
	transform, err := pipeline.NewTransform(reg, w.cfg.StreamLabels)
	if err != nil {
		return nil, errors.Join(err, reg.Close())
	}
	return combineWithRegistry(reg, ingest.NewWith(ingest.Options{Transform: transform})), nil
}

// querierRole builds the querier with its own registry: WrapEngine pins one
// snapshot per query, and WrapSource pins one snapshot per direct source
// operation. Only the LogQL routes see the wrapped source.
func (w *pipelineWiring) querierRole() (app.Factory, error) {
	reg, err := w.newRegistry(pipeline.RoleQuerier)
	if err != nil {
		return nil, err
	}
	engine, err := pipeline.WrapEngine(logql.New(w.cfg.LogQL), reg)
	if err != nil {
		return nil, errors.Join(err, reg.Close())
	}
	read := func(deps app.Deps) (app.Service, error) {
		// WrapSource cannot report an error, so the constructor inputs must be
		// valid before query.NewWith accepts the hook. The registry and the
		// engine wrapper already validated them; this makes the startup
		// contract explicit instead of relying on a later fallback.
		if err := validateReadSourceInputs(reg, w.cfg.StreamLabels); err != nil {
			return nil, err
		}
		return query.NewWith(deps, engine, query.Options{
			WrapSource: func(raw plan.Source) plan.Source {
				wrapped, err := pipeline.NewReadSource(raw, reg, w.cfg.StreamLabels)
				if err != nil {
					// Defensive only: inputs were validated above. Never answer
					// raw rows, which would silently skip configured
					// transforms; fail the read instead.
					return errorSource{err: fmt.Errorf("query: read pipelines unavailable: %w", err)}
				}
				return wrapped
			},
		})
	}
	return combineWithRegistry(reg, read, tail.NewQuerier), nil
}

// validateReadSourceInputs checks the NewReadSource arguments that the
// WrapSource hook cannot report. A successfully built registry and engine
// wrapper already guarantee them; this is the explicit assembly guard.
func validateReadSourceInputs(reg *pipeline.Registry, streamLabels []string) error {
	if reg == nil {
		return errors.New("query: read pipelines: nil registry")
	}
	if len(streamLabels) == 0 {
		return errors.New("query: read pipelines: no stream labels configured")
	}
	return nil
}

// newRegistry builds and tracks one role's registry. NewRegistry leaves
// nothing registered when it fails; after success the registry is in w.regs
// so close releases it even when a later role factory fails and its service
// never runs.
func (w *pipelineWiring) newRegistry(role string) (*pipeline.Registry, error) {
	reg, err := pipeline.NewRegistry(w.cat, pipeline.RegistryOptions{
		CompileOptions: w.compileOptions(),
		PollInterval:   w.cfg.Pipeline.PollInterval,
		ReloadTimeout:  w.cfg.Pipeline.ReloadTimeout,
		Role:           role,
		Logger:         w.log,
		Metrics:        w.metrics,
	})
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.regs = append(w.regs, reg)
	w.mu.Unlock()
	return reg, nil
}

// close releases every registry this wiring created. Close is idempotent, so
// registries a role already closed in Run are untouched; this covers a
// registry whose role was built but never ran because a later factory failed.
func (w *pipelineWiring) close() error {
	w.mu.Lock()
	regs := w.regs
	w.regs = nil
	w.mu.Unlock()
	var errs []error
	for _, reg := range regs {
		errs = append(errs, reg.Close())
	}
	return errors.Join(errs...)
}

// combineWithRegistry turns a role's factories plus its pipeline registry
// into one app.Service. Services are constructed first; if any construction
// fails, the services built before it and the registry are released, because
// app.Combine does not clean up previously built services on a later factory
// error. When the role runs, app.Combine starts the registry and every service
// under one context, cancels the others when one fails, joins them, and the
// wrapper closes the registry exactly once after all of them returned.
func combineWithRegistry(reg *pipeline.Registry, factories ...app.Factory) app.Factory {
	return func(deps app.Deps) (app.Service, error) {
		services := make([]app.Service, 0, len(factories))
		for _, f := range factories {
			svc, err := f(deps)
			if err != nil {
				return nil, errors.Join(err, releaseServices(services), reg.Close())
			}
			services = append(services, svc)
		}
		combined := make([]app.Factory, 0, len(services)+1)
		combined = append(combined, fixedService(app.ServiceFunc(reg.Run)))
		for _, svc := range services {
			combined = append(combined, fixedService(svc))
		}
		run, err := app.Combine(combined...)(app.Deps{})
		if err != nil {
			return nil, errors.Join(err, reg.Close())
		}
		return &registryRole{registry: reg, run: run}, nil
	}
}

// fixedService adapts an already-built service back to a factory.
func fixedService(svc app.Service) app.Factory {
	return func(app.Deps) (app.Service, error) { return svc, nil }
}

// releaseServices releases services that were constructed for a role that
// never ran. Every role's Run treats a done context as a prompt, clean stop,
// so this closes resources opened during construction (the querier's DuckDB
// handle and caches) the same way a normal shutdown would.
func releaseServices(services []app.Service) error {
	if len(services) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, svc := range services {
		wg.Go(func() {
			if err := svc.Run(ctx); err != nil && !isCancellation(err) {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// registryRole runs one role's services together with its pipeline registry.
type registryRole struct {
	registry *pipeline.Registry
	run      app.Service
}

func (r *registryRole) Run(ctx context.Context) error {
	defer r.registry.Close()
	return r.run.Run(ctx)
}

// errorSource fails every read with one construction error. It is the
// fail-closed fallback for a read wrapper that could not validate its
// options, so a broken setup never serves untransformed rows.
type errorSource struct{ err error }

func (s errorSource) Select(context.Context, plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) { yield(plan.Row{}, s.err) }
}

func (s errorSource) Aggregate(context.Context, plan.Selection, plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) { yield(plan.Bucket{}, s.err) }
}

// isCancellation reports whether err is nothing but context cancellation.
// It mirrors app's rule so this role wrapper does not report a normal
// shutdown as a failure of the joined role.
func isCancellation(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled {
		return true
	}
	switch u := err.(type) {
	case interface{ Unwrap() []error }:
		errs := u.Unwrap()
		if len(errs) == 0 {
			return false
		}
		for _, e := range errs {
			if !isCancellation(e) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isCancellation(u.Unwrap())
	}
	return false
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "parkana:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) error {
	cfg, err := config.Load(args, getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	log := newLogger(cfg, stderr)
	info := version.Get()
	log.Info("starting parkana", "version", info.Version, "revision", info.Revision, "roles", cfg.Roles)

	store, err := app.OpenStore(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	cat, err := app.OpenCatalog(cfg)
	if err != nil {
		return err
	}
	defer cat.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	srv := server.New(server.Options{Addr: cfg.HTTPAddr, Logger: log, Registry: reg, MultiTenant: cfg.MultiTenant})
	lokiapi.RegisterBuildInfo(srv)

	pipes, err := newPipelineWiring(cfg, log, cat, reg)
	if err != nil {
		return err
	}
	defer func() { _ = pipes.close() }()
	if cfg.Has(config.RoleIngester) || cfg.Has(config.RoleQuerier) {
		if err := pipes.registerAPI(srv); err != nil {
			return fmt.Errorf("pipeline api: %w", err)
		}
	}

	deps := app.Deps{Config: cfg, Logger: log, Store: store, Catalog: cat, HTTP: srv, Metrics: reg}
	services := map[string]app.Service{"http": srv}
	assembled := make([]app.Service, 0, len(cfg.Roles))
	for _, role := range cfg.Roles {
		factory, err := pipes.roleFactory(role)
		if err != nil {
			return errors.Join(fmt.Errorf("role %s: %w", role, err), releaseServices(assembled))
		}
		if factory == nil {
			log.Warn("role not implemented yet; skipping", "role", role)
			continue
		}
		svc, err := factory(deps)
		if err != nil {
			// app.Combine (and every factory contract) leaves earlier services
			// running only in the caller's hands; this process is aborting, so
			// release what was already constructed for the roles that will
			// never start.
			return errors.Join(fmt.Errorf("role %s: %w", role, err), releaseServices(assembled))
		}
		assembled = append(assembled, svc)
		services[string(role)] = svc
	}
	lokiapi.RegisterUnimplemented(srv)

	return app.Run(ctx, log, services)
}

func newLogger(cfg config.Config, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
