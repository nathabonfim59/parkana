// Package app wires roles together: it opens the shared dependencies and
// runs services until the context ends.
//
// Roles talk to each other only through Deps.Store, Deps.Catalog and HTTP,
// never through shared in-process state, so a role can move to another
// process (or node) by changing -roles.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/server"
)

// ErrNotImplemented marks configuration options whose backend doesn't exist yet.
var ErrNotImplemented = errors.New("not implemented yet")

// Deps are the shared dependencies a role is built from.
type Deps struct {
	Config  config.Config
	Logger  *slog.Logger
	Store   objstore.Store
	Catalog catalog.Catalog
	// HTTP is the shared server; roles register routes on it in their Factory.
	HTTP    *server.Server
	Metrics prometheus.Registerer
}

// Service is a long-running part of the process. Run blocks until ctx is
// done or the service fails. On ctx.Done it must finish in-flight work it is
// responsible for (e.g. the ingester seals and flushes) and return nil.
type Service interface {
	Run(ctx context.Context) error
}

// ServiceFunc adapts a function to Service.
type ServiceFunc func(ctx context.Context) error

func (f ServiceFunc) Run(ctx context.Context) error { return f(ctx) }

// Factory builds a role's service from Deps and registers its HTTP routes.
type Factory func(Deps) (Service, error)

// Combine builds a role from several factories, e.g. the querier's read API
// and its live tail. The combined service runs them all and stops when ctx is
// done or one fails (the others are canceled; their errors are joined).
func Combine(factories ...Factory) Factory {
	return func(deps Deps) (Service, error) {
		services := make([]Service, 0, len(factories))
		for _, f := range factories {
			svc, err := f(deps)
			if err != nil {
				return nil, err
			}
			services = append(services, svc)
		}
		return ServiceFunc(func(ctx context.Context) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
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
						cancel()
					}
				})
			}
			wg.Wait()
			return errors.Join(errs...)
		}), nil
	}
}

// OpenStore opens the configured object store.
func OpenStore(cfg config.Config) (objstore.Store, error) {
	switch cfg.Storage.Backend {
	case "fs":
		return objstore.NewFS(cfg.Storage.FSDir)
	case "s3":
		s3, err := objstore.NewS3(objstore.S3Options{
			Endpoint:     cfg.Storage.S3.Endpoint,
			Bucket:       cfg.Storage.S3.Bucket,
			Region:       cfg.Storage.S3.Region,
			AccessKey:    cfg.Storage.S3.AccessKey,
			SecretKey:    cfg.Storage.S3.SecretKey,
			Insecure:     cfg.Storage.S3.Insecure,
			CreateBucket: cfg.Storage.S3.CreateBucket,
		})
		if err != nil {
			return nil, fmt.Errorf("storage backend s3: %w", err)
		}
		return s3, nil
	}
	return nil, fmt.Errorf("unknown storage backend %q", cfg.Storage.Backend)
}

// OpenCatalog opens the configured catalog.
func OpenCatalog(cfg config.Config) (catalog.Catalog, error) {
	switch cfg.Catalog.Driver {
	case "memory":
		return catalog.NewMemory(), nil
	case "sqlite":
		cat, err := catalog.NewSQLite(cfg.Catalog.DSN)
		if err != nil {
			return nil, fmt.Errorf("catalog driver sqlite: %w", err)
		}
		return cat, nil
	}
	return nil, fmt.Errorf("unknown catalog driver %q", cfg.Catalog.Driver)
}

// Run runs every service until ctx is done or one of them fails; a failure
// cancels the others. It returns the joined errors of the services that
// failed, or nil on a clean shutdown.
func Run(ctx context.Context, log *slog.Logger, services map[string]Service) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for name, svc := range services {
		wg.Go(func() {
			log.Info("service starting", "service", name)
			err := svc.Run(ctx)
			if err != nil && !isCancellation(err) {
				err = fmt.Errorf("%s: %w", name, err)
				log.Error("service failed", "service", name, "err", err)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				cancel(err)
				return
			}
			log.Info("service stopped", "service", name)
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// isCancellation reports whether err is nothing but context cancellation: every
// leaf of its error tree (through Unwrap() error and Unwrap() []error) is
// context.Canceled. errors.Is is not enough: a joined error holding a real
// failure next to context.Canceled would pass it and turn a fatal error into a
// clean exit.
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
