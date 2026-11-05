package compactor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// maxMergeInputs bounds the files, and so the open storage.Readers, of one
// merge, no matter how many candidates a group holds.
const maxMergeInputs = 32

// service is the compactor role's app.Service.
type service struct {
	cfg   config.Compactor
	log   *slog.Logger
	store objstore.Store
	cat   catalog.Catalog

	// params tune the sidecar builds for merge output and backfill. They are
	// validated in New and never defaulted there: a hand-built config with a
	// zero Index section is rejected rather than silently normalized.
	params index.Params
	// idx holds the shared parkana_index_* collectors, reused across roles
	// when one registry serves several services.
	idx *index.Metrics
	// skips remembers keys of files found over the gram cap so the backfill
	// pass does not re-read them every cycle. Bounded and per-service; see
	// skipSet.
	skips *skipSet
	// merge is the Parquet merge seam. Production uses storage.Merge; focused
	// tests replace it to inject malformed row accounting at the boundary.
	merge mergeFunc

	cycles       prometheus.Counter
	cycleErrors  prometheus.Counter
	mergeErrors  prometheus.Counter
	filesMerged  prometheus.Counter
	bytesMerged  prometheus.Counter
	cycleSeconds prometheus.Histogram
}

// New builds the compactor role from deps. It validates the compactor config,
// the sidecar index params and the shared deps, registers parkana_compactor_*
// metrics on deps.Metrics and the shared parkana_index_* collectors, and
// returns an error for invalid index params rather than substituting
// defaults: config.Load always fills them, so a zero or half-set Index section
// reaching New is a wiring bug. deps.Store and deps.Catalog stay owned by the
// caller and are never closed here.
func New(deps app.Deps) (app.Service, error) {
	cfg := deps.Config.Compactor
	params := deps.Config.Index.Params()
	switch {
	case cfg.Interval <= 0:
		return nil, fmt.Errorf("compactor: interval must be positive, got %s", cfg.Interval)
	case cfg.TargetSize <= 0:
		return nil, fmt.Errorf("compactor: target size must be positive, got %s", cfg.TargetSize.String())
	case cfg.Retention < 0 || cfg.DeleteDelay < 0:
		return nil, errors.New("compactor: retention and delete delay must not be negative")
	case deps.Store == nil:
		return nil, errors.New("compactor: nil object store")
	case deps.Catalog == nil:
		return nil, errors.New("compactor: nil catalog")
	case deps.Logger == nil:
		return nil, errors.New("compactor: nil logger")
	case deps.Metrics == nil:
		return nil, errors.New("compactor: nil metrics registry")
	}
	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("compactor: sidecar index params: %w", err)
	}
	idx, err := index.RegisterMetrics(deps.Metrics)
	if err != nil {
		return nil, err
	}

	s := &service{
		cfg:    cfg,
		log:    deps.Logger,
		store:  deps.Store,
		cat:    deps.Catalog,
		params: params,
		idx:    idx,
		skips:  newSkipSet(backfillSkipCapacity),
		merge:  storage.Merge,
	}
	if s.cycles, err = registerMetric(deps.Metrics, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_compactor_cycles_total", Help: "Compaction cycles started.",
	})); err != nil {
		return nil, err
	}
	if s.cycleErrors, err = registerMetric(deps.Metrics, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_compactor_cycle_errors_total", Help: "Compaction cycles that returned an error.",
	})); err != nil {
		return nil, err
	}
	if s.mergeErrors, err = registerMetric(deps.Metrics, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_compactor_merge_errors_total", Help: "File merges that failed.",
	})); err != nil {
		return nil, err
	}
	if s.filesMerged, err = registerMetric(deps.Metrics, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_compactor_files_merged_total", Help: "Input files replaced by a merged object.",
	})); err != nil {
		return nil, err
	}
	if s.bytesMerged, err = registerMetric(deps.Metrics, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_compactor_bytes_merged_total", Help: "Input object bytes replaced by merges.",
	})); err != nil {
		return nil, err
	}
	if s.cycleSeconds, err = registerMetric(deps.Metrics, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "parkana_compactor_cycle_duration_seconds",
		Help:    "Compaction cycle latency.",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 8),
	})); err != nil {
		return nil, err
	}
	return s, nil
}

// registerMetric registers c on r, reusing the already-registered collector
// when another compactor instance registered it first.
func registerMetric[C prometheus.Collector](r prometheus.Registerer, c C) (C, error) {
	if err := r.Register(c); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := are.ExistingCollector.(C); ok {
				return existing, nil
			}
		}
		var zero C
		return zero, fmt.Errorf("compactor: register metrics: %w", err)
	}
	return c, nil
}

// Run compacts once at startup and then every cfg.Interval until ctx ends. A
// failed cycle is logged, counted and retried on the next tick; it never
// takes down the other roles. An in-flight merge is abandoned on
// cancellation: it has no output to seal and its sources stay intact, so Run
// returns nil promptly.
func (s *service) Run(ctx context.Context) error {
	tick := time.NewTicker(s.cfg.Interval)
	defer tick.Stop()
	for {
		s.runCycle(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// tenants reports the tenants to compact. Phase 1 is single-tenant, so only
// the default tenant is scanned; multi-tenant deployments will enumerate
// tenants from the catalog or config here.
func tenants() []tenant.ID {
	return []tenant.ID{tenant.Default}
}

// runCycle runs one compaction cycle, observing latency and counting
// failures. Cancellation is not an error.
func (s *service) runCycle(ctx context.Context) {
	start := time.Now()
	s.cycles.Inc()
	switch err := s.cycle(ctx); {
	case err == nil:
	case ctx.Err() != nil:
		s.log.Debug("compaction cycle canceled", "err", err)
	default:
		s.cycleErrors.Inc()
		s.log.Error("compaction cycle failed", "err", err)
	}
	s.cycleSeconds.Observe(time.Since(start).Seconds())
}

// cycle runs each tenant's cleanup pass, then scans its streams once and
// merges what it can, then backfills sidecars for the files no merge covered.
// Errors are collected per tenant, stream and file so one bad group cannot
// starve the rest. The backfill pass only needs the file listing, not the
// stream listing, so it still runs when Streams failed and merges were
// skipped: its per-file isolation keeps cleanup and merge errors from
// compounding with its own.
func (s *service) cycle(ctx context.Context) error {
	var errs []error
	for _, t := range tenants() {
		// Retention, delete-delay purge and orphan GC run for the tenant
		// even when it has no streams or listing them fails: unreferenced
		// objects can exist where no live catalog row does.
		st, err := cleanup(ctx, s.store, s.cat, t, time.Now(), s.cfg.Retention, s.cfg.DeleteDelay)
		if st != (cleanupStats{}) {
			s.log.Info("compactor cleanup",
				"tenant", t, "marked", st.marked, "purged", st.purged, "orphaned", st.orphaned)
		}
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			errs = append(errs, fmt.Errorf("tenant %s cleanup: %w", t, err))
		}
		streams, err := s.cat.Streams(ctx, t)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			errs = append(errs, fmt.Errorf("tenant %s: %w", t, err))
		} else {
			for _, stream := range streams {
				if err := s.compactStream(ctx, t, stream); err != nil {
					if ctx.Err() != nil {
						return err
					}
					errs = append(errs, fmt.Errorf("stream %s: %w", stream, err))
				}
			}
		}
		if s.cfg.SidecarBackfill {
			if err := s.backfillTenant(ctx, t); err != nil {
				if ctx.Err() != nil {
					return err
				}
				errs = append(errs, fmt.Errorf("tenant %s backfill: %w", t, err))
			}
		}
	}
	return errors.Join(errs...)
}

// compactStream lists the stream's live files, groups them by the UTC hour in
// their object key and merges each group that has a batch fitting the target.
// Files with malformed or cross-tenant keys are skipped, never merged.
func (s *service) compactStream(ctx context.Context, t tenant.ID, stream string) error {
	groups := make(map[string][]catalog.File)
	var hours []string
	for f, err := range s.cat.ListFiles(ctx, catalog.Filter{Tenant: t, Stream: stream}) {
		if err != nil {
			return fmt.Errorf("list files: %w", err)
		}
		if f.Bytes >= int64(s.cfg.TargetSize) {
			continue
		}
		hour, err := keyHour(t, stream, f.Key)
		if err != nil {
			s.log.Warn("compactor skipping file with malformed key",
				"tenant", t, "stream", stream, "key", f.Key, "err", err)
			continue
		}
		if _, ok := groups[hour]; !ok {
			hours = append(hours, hour)
		}
		groups[hour] = append(groups[hour], f)
	}
	slices.Sort(hours)

	var errs []error
	for _, hour := range hours {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.mergeHour(ctx, t, stream, groups[hour]); err != nil {
			if ctx.Err() != nil {
				return err
			}
			s.mergeErrors.Inc()
			errs = append(errs, fmt.Errorf("hour %s: %w", hour, err))
		}
	}
	return errors.Join(errs...)
}

// keyHour returns the UTC hour partition of key, which must look like
// {tenant}/logs/{stream}/yyyy/mm/dd/hh/{name}.parquet. Anything else is
// malformed: such files are skipped rather than merged or touched.
func keyHour(t tenant.ID, stream, key string) (string, error) {
	if err := objstore.ValidateKey(key); err != nil {
		return "", err
	}
	rest, ok := strings.CutPrefix(key, objstore.LogPrefix(t, stream))
	if !ok {
		return "", fmt.Errorf("key %q is outside %q", key, objstore.LogPrefix(t, stream))
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 5 {
		return "", fmt.Errorf("key %q: want yyyy/mm/dd/hh/name%s after the stream prefix", key, objstore.ExtData)
	}
	for i, n := range []int{4, 2, 2, 2} {
		if len(parts[i]) != n || !isDigits(parts[i]) {
			return "", fmt.Errorf("key %q: bad date segment %q", key, parts[i])
		}
	}
	if stem, ok := strings.CutSuffix(parts[4], objstore.ExtData); !ok || stem == "" {
		return "", fmt.Errorf("key %q: name does not end with %s", key, objstore.ExtData)
	}
	hour := strings.Join(parts[:4], "/")
	if _, err := time.Parse("2006/01/02/15", hour); err != nil {
		return "", fmt.Errorf("key %q: %q is not a valid UTC hour", key, hour)
	}
	return hour, nil
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
