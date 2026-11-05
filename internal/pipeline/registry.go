package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Defaults for the zero fields of RegistryOptions.
const (
	// DefaultPollInterval is how often Run checks the change token of every
	// tenant it has seen.
	DefaultPollInterval = time.Second
	// DefaultReloadTimeout bounds one tenant's poll reload, including its
	// catalog requests. It does not bound a first-use load, which runs in the
	// caller's context and returns that operation's error.
	DefaultReloadTimeout = 5 * time.Second
)

// ErrRegistryClosed is returned by Snapshot after Close. An operation that
// already holds a snapshot keeps using it; only new lookups fail.
var ErrRegistryClosed = errors.New("pipeline: registry is closed")

// RegistryOptions configures a Registry.
type RegistryOptions struct {
	// CompileOptions are used for every definition the registry compiles.
	// StreamLabels is required; its bounds should mirror the ingester's
	// configured limits. The registry sets ObserveExecution itself, so any
	// value here is replaced by its metrics observer.
	CompileOptions CompileOptions
	// PollInterval is how often Run polls seen tenants. Zero uses
	// DefaultPollInterval.
	PollInterval time.Duration
	// ReloadTimeout bounds one tenant's poll reload: its change-token read and
	// its list plus compile. Each tick starts one independent worker per seen
	// tenant and does not wait for earlier ticks, so a slow tenant never delays
	// another tenant's reload or a request that already holds a snapshot; Run
	// joins every worker before it returns. Zero uses DefaultReloadTimeout.
	// First-use loads run in the caller's context instead.
	ReloadTimeout time.Duration
	// Role labels this registry's metrics and must be RoleIngester or
	// RoleQuerier.
	Role string
	// Logger receives reload failures and successful changes. Nil discards
	// them.
	Logger *slog.Logger
	// Metrics is the process registerer this registry's parkana_pipeline_*
	// collectors are registered on. It is required. Roles in one process pass
	// the same registerer; each role registers its own collectors with role as
	// a constant label, and Close releases only this registry's collectors.
	Metrics prometheus.Registerer
}

// Registry is the catalog-backed SnapshotProvider for one role. It keeps one
// immutable snapshot per seen tenant, loads a tenant synchronously on first
// use, and reloads seen tenants in Run when their catalog change token moves.
// A request holds the Snapshot it received for its whole execution; a reload
// swaps the tenant's snapshot atomically and never mutates a live one.
// Registries are safe for concurrent use by many requests.
type Registry struct {
	cat           catalog.Catalog
	compileOpts   CompileOptions
	pollInterval  time.Duration
	reloadTimeout time.Duration
	log           *slog.Logger
	metrics       *Metrics

	mu     sync.RWMutex
	states map[tenant.ID]*tenantState
	closed bool
	done   chan struct{}
	// ctx is canceled by Close: in-flight first loads and poll reloads stop
	// and never publish a snapshot after Close.
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	// pollWG tracks every tenant reload worker spawned by Run, so Run joins
	// them all before it returns. Workers are not joined per tick: a slow
	// tenant must not delay the next tick's reloads for other tenants.
	pollWG sync.WaitGroup
}

// NewRegistry builds a registry over cat. It registers the role's metrics on
// opts.Metrics; on error nothing is left registered by this call, and every
// later failure leaves nothing to Close. The caller must Close the registry
// when the role stops, including when role assembly fails before Run.
func NewRegistry(cat catalog.Catalog, opts RegistryOptions) (*Registry, error) {
	if cat == nil {
		return nil, errors.New("pipeline: registry requires a catalog")
	}
	if !validRole(opts.Role) {
		return nil, fmt.Errorf("pipeline: registry role %q must be %q or %q", opts.Role, RoleIngester, RoleQuerier)
	}
	if opts.PollInterval < 0 {
		return nil, fmt.Errorf("pipeline: registry PollInterval %s must not be negative", opts.PollInterval)
	}
	if opts.ReloadTimeout < 0 {
		return nil, fmt.Errorf("pipeline: registry ReloadTimeout %s must not be negative", opts.ReloadTimeout)
	}
	if opts.Metrics == nil {
		return nil, errors.New("pipeline: registry requires a metrics registerer")
	}
	if err := validateRegistryCompileOptions(opts.CompileOptions); err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m, err := RegisterMetrics(opts.Metrics, opts.Role)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	copts := opts.CompileOptions
	copts.StreamLabels = append([]string(nil), copts.StreamLabels...)
	r := &Registry{
		cat:           cat,
		compileOpts:   copts,
		pollInterval:  cmp.Or(opts.PollInterval, DefaultPollInterval),
		reloadTimeout: cmp.Or(opts.ReloadTimeout, DefaultReloadTimeout),
		log:           log,
		metrics:       m,
		states:        make(map[tenant.ID]*tenantState),
		done:          make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}
	copts.ObserveExecution = m.ExecutionObserver()
	r.compileOpts = copts
	return r, nil
}

// validateRegistryCompileOptions rejects the option mistakes the constructor
// can see without compiling anything; Compile performs the full validation.
// Routing labels keep their configured priority order, which is not
// necessarily sorted, so each name is validated on its own.
func validateRegistryCompileOptions(o CompileOptions) error {
	if len(o.StreamLabels) == 0 {
		return errors.New("pipeline: registry CompileOptions.StreamLabels is required")
	}
	seen := make(map[string]struct{}, len(o.StreamLabels))
	for _, name := range o.StreamLabels {
		if err := (logs.Labels{{Name: name}}).Validate(); err != nil {
			return fmt.Errorf("pipeline: registry CompileOptions.StreamLabels: %w", err)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("pipeline: registry CompileOptions.StreamLabels: duplicate label %q", name)
		}
		seen[name] = struct{}{}
	}
	for _, b := range []struct {
		name string
		v    int
	}{
		{"LineBytes", o.LineBytes},
		{"MetaBytes", o.MetaBytes},
		{"MetaValueBytes", o.MetaValueBytes},
		{"LabelValueBytes", o.LabelValueBytes},
	} {
		if b.v < 0 {
			return fmt.Errorf("pipeline: registry CompileOptions.%s must not be negative", b.name)
		}
	}
	return nil
}

// Snapshot implements SnapshotProvider. The first call for a tenant loads its
// definitions synchronously and fails with the load error rather than serving
// an empty snapshot; later calls return the current immutable snapshot without
// catalog I/O. A changed catalog version becomes visible when Run reloads it.
// A canceled context stops a first-use load (including a wait for the tenant's
// reload lock) and is returned with errors.Is(context.Canceled) intact.
func (r *Registry) Snapshot(ctx context.Context, id tenant.ID) (Snapshot, error) {
	if ctx == nil {
		return nil, errors.New("pipeline: registry: nil context")
	}
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("pipeline: registry: tenant: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: registry: %w", err)
	}
	st, err := r.stateFor(id)
	if err != nil {
		return nil, err
	}
	if err := r.ensure(ctx, id, st); err != nil {
		return nil, fmt.Errorf("pipeline: registry: load tenant %q: %w", id, err)
	}
	// The provider may ignore cancellation; never report a snapshot for a
	// canceled operation, including one already cached.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: registry: %w", err)
	}
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, ErrRegistryClosed
	}
	view := st.view.Load()
	if view == nil {
		// ensure never returns success without a view.
		return nil, fmt.Errorf("pipeline: registry: tenant %q has no snapshot", id)
	}
	return view.snap, nil
}

// stateFor returns the tenant's state, creating it (and so marking the tenant
// seen by Run) on first use.
func (r *Registry) stateFor(id tenant.ID) (*tenantState, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, ErrRegistryClosed
	}
	st := r.states[id]
	r.mu.RUnlock()
	if st != nil {
		return st, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRegistryClosed
	}
	if st = r.states[id]; st == nil {
		st = newTenantState()
		r.states[id] = st
	}
	return st, nil
}

// ensure performs the tenant's first load exactly once. Concurrent first uses
// share it; a waiter that is canceled returns before the load finishes, and a
// failed load is retried by the next Snapshot.
func (r *Registry) ensure(ctx context.Context, id tenant.ID, st *tenantState) error {
	for {
		if st.view.Load() != nil {
			return nil
		}
		st.initMu.Lock()
		if st.view.Load() != nil {
			st.initMu.Unlock()
			return nil
		}
		if st.loading {
			done := st.initDone
			st.initMu.Unlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("wait for initial load: %w", ctx.Err())
			case <-r.ctx.Done():
				return ErrRegistryClosed
			case <-done:
			}
			if st.view.Load() != nil {
				return nil
			}
			st.initMu.Lock()
			err := st.initErr
			st.initMu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		st.loading = true
		done := make(chan struct{})
		st.initDone = done
		st.initErr = nil
		st.initMu.Unlock()

		// The load context also watches registry shutdown, so a catalog call
		// that ignores cancellation cannot pin this load after Close.
		loadCtx, stop := r.loadContext(ctx)
		var err error
		if err = st.reload.acquire(loadCtx); err != nil {
			if r.ctx.Err() != nil {
				err = ErrRegistryClosed
			} else {
				err = fmt.Errorf("wait for tenant reload lock: %w", err)
			}
		} else {
			err = r.reloadLocked(loadCtx, id, st)
			st.reload.release()
		}
		stop()

		st.initMu.Lock()
		st.loading = false
		st.initErr = err
		close(done)
		st.initMu.Unlock()
		if err != nil {
			return err
		}
		if st.view.Load() != nil {
			return nil
		}
		return fmt.Errorf("initial load produced no snapshot")
	}
}

// Run polls every seen tenant until ctx is done or Close is called, and then
// waits for every spawned reload worker and returns nil (a clean shutdown).
// Tenants seen after Run starts are picked up on the next tick. Only tenants
// that had at least one Snapshot call are polled.
func (r *Registry) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("pipeline: registry: nil context")
	}
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.pollWG.Wait()
			return nil
		case <-r.done:
			r.pollWG.Wait()
			return nil
		case <-ticker.C:
			r.poll(ctx)
		}
	}
}

// poll starts one worker per seen tenant without waiting for them. Each worker
// reloads at most one tenant, is bounded by reloadTimeout, and skips the tick
// if that tenant's previous reload is still in flight (TryLock). Slow tenants
// therefore never delay later ticks for other tenants; Run joins the workers
// on shutdown.
func (r *Registry) poll(ctx context.Context) {
	type entry struct {
		id tenant.ID
		st *tenantState
	}
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return
	}
	entries := make([]entry, 0, len(r.states))
	for id, st := range r.states {
		entries = append(entries, entry{id: id, st: st})
	}
	r.mu.RUnlock()

	for _, e := range entries {
		r.pollWG.Go(func() {
			if !e.st.reload.tryAcquire() {
				return
			}
			defer e.st.reload.release()
			lctx, cancel := context.WithTimeout(ctx, r.reloadTimeout)
			defer cancel()
			// Failures are observed and logged inside reloadLocked; the next
			// tick retries the same changed token.
			_ = r.reloadLocked(lctx, e.id, e.st)
		})
	}
}

// reloadLocked loads tenant id if its change token moved, then atomically
// swaps in a complete new snapshot. The caller holds the tenant's reload lock.
// Any error keeps the previous good snapshot and the previous stored token, so
// the same pending change is retried on the next poll. It observes reload
// metrics and logs every attempt that saw a changed token (or a token read
// failure); an unchanged token is a no-op.
func (r *Registry) reloadLocked(ctx context.Context, id tenant.ID, st *tenantState) error {
	start := time.Now()
	// A close or a canceled caller must not start a catalog round trip.
	ctx, stop := r.loadContext(ctx)
	defer stop()
	if err := ctx.Err(); err != nil {
		if r.ctx.Err() != nil {
			return ErrRegistryClosed
		}
		// Registry close is not a reload failure to retry: Run stops anyway.
		return err
	}
	// Read the token BEFORE the list. A mutation that lands between the two
	// makes the stored token older than the returned definitions: the next
	// poll sees a newer token and reloads redundantly. The reverse order could
	// stamp a later token on an earlier list and never reload that change.
	token, err := r.cat.PipelineChangeToken(ctx, id)
	if err != nil {
		if r.ctx.Err() != nil {
			return ErrRegistryClosed
		}
		err = fmt.Errorf("read change token: %w", err)
		r.observeReload(id, start, err)
		return err
	}
	old := st.view.Load()
	if old != nil && old.token == token {
		// An unchanged token is only a no-op when the caller still wants the
		// answer. A catalog that ignores cancellation may have returned after
		// the caller gave up.
		if err := ctx.Err(); err != nil {
			if r.ctx.Err() != nil {
				return ErrRegistryClosed
			}
			return err
		}
		return nil
	}
	defs, err := r.cat.ListPipelines(ctx, id)
	if err != nil {
		if r.ctx.Err() != nil {
			return ErrRegistryClosed
		}
		err = fmt.Errorf("list definitions: %w", err)
		r.observeReload(id, start, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		if r.ctx.Err() != nil {
			return ErrRegistryClosed
		}
		return err
	}
	snap, err := r.compileDefinitions(ctx, defs)
	if err != nil {
		if r.ctx.Err() != nil {
			return ErrRegistryClosed
		}
		r.observeReload(id, start, err)
		return err
	}
	view := &snapshotView{token: token, definitions: snap.count(), snap: snap}
	// Publish under the registry lock so Close and publication are ordered: a
	// load that loses the race is discarded instead of becoming visible after
	// shutdown.
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrRegistryClosed
	}
	if r.ctx.Err() != nil {
		r.mu.RUnlock()
		return ErrRegistryClosed
	}
	// Final caller check after the last compile, before publication.
	if err := ctx.Err(); err != nil {
		r.mu.RUnlock()
		return err
	}
	st.view.Store(view)
	r.mu.RUnlock()
	var delta int64 = int64(view.definitions)
	if old != nil {
		delta -= int64(old.definitions)
	}
	// The gauge sums concurrent tenant deltas itself; never Set a computed
	// total, which a stale publication could overwrite.
	r.metrics.AddActiveDefinitions(int(delta))
	r.observeReload(id, start, nil)
	r.log.Debug("pipeline registry reloaded", "tenant", id, "definitions", view.definitions)
	return nil
}

// loadContext derives a load context that is also canceled by Close.
func (r *Registry) loadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	lctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	if r.ctx.Err() != nil {
		cancel()
	}
	return lctx, func() {
		stop()
		cancel()
	}
}

// observeReload records one reload attempt and logs failures. Loads aborted by
// Close are not reload failures and are not observed.
func (r *Registry) observeReload(id tenant.ID, start time.Time, err error) {
	if r.ctx.Err() != nil {
		return
	}
	r.metrics.ObserveReload(time.Since(start), err)
	if err != nil {
		r.log.Warn("pipeline registry reload failed; keeping previous snapshot", "tenant", id, "err", err)
	}
}

// compileDefinitions builds a complete snapshot from catalog rows. Name, stage
// and selector come from the authoritative columns; only the steps come from
// the stored spec. One bad definition fails the whole build, so a caller never
// sees a partially applied catalog change.
func (r *Registry) compileDefinitions(ctx context.Context, defs []catalog.PipelineDefinition) (*registrySnapshot, error) {
	snap := &registrySnapshot{byStage: make(map[Stage][]*Compiled)}
	seen := make(map[string]struct{}, len(defs))
	for _, row := range defs {
		// A large catalog change must remain interruptible and must not publish
		// after Close even if every definition is valid.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("compile definitions: %w", err)
		}
		if row.Deleted() {
			continue
		}
		spec, err := DecodeSpec(row.Spec)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", row.Name, err)
		}
		c, err := Compile(Definition{
			Name:     row.Name,
			Stage:    Stage(row.Stage),
			Selector: row.Selector,
			Steps:    spec.Steps,
		}, r.compileOpts)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", row.Name, err)
		}
		if _, dup := seen[c.name]; dup {
			return nil, fmt.Errorf("pipeline %q: duplicate definition in catalog", c.name)
		}
		seen[c.name] = struct{}{}
		snap.byStage[c.stage] = append(snap.byStage[c.stage], c)
	}
	for stage := range snap.byStage {
		slices.SortStableFunc(snap.byStage[stage], func(a, b *Compiled) int {
			return cmp.Compare(a.name, b.name)
		})
	}
	return snap, nil
}

// Close stops Run and releases this registry's metrics registration. It is
// idempotent and safe before Run, after a failed role assembly, and while
// requests hold snapshots (they keep the snapshot they already have).
func (r *Registry) Close() error {
	r.closeOnce.Do(func() {
		// Cancel in-flight first loads and poll reloads before taking the lock:
		// a load blocked in the catalog returns, sees closed, and discards its
		// snapshot instead of publishing it after shutdown.
		r.cancel()
		r.mu.Lock()
		r.closed = true
		close(r.done)
		r.mu.Unlock()
		_ = r.metrics.Close()
	})
	return nil
}

// tenantState is the registry's per-tenant state: the current immutable
// snapshot, the first-load single-flight gate, and the reload lock.
type tenantState struct {
	reload *tenantLock
	view   atomic.Pointer[snapshotView]

	initMu   sync.Mutex
	loading  bool
	initDone chan struct{}
	initErr  error
}

// newTenantState returns an initialized state with a free reload lock.
func newTenantState() *tenantState {
	return &tenantState{reload: newTenantLock()}
}

// snapshotView pairs one immutable snapshot with the catalog change token it
// was built from. token 0 is the catalog's "no writes yet" value; because the
// view exists, the first-use load still reads the list once.
type snapshotView struct {
	token       int64
	definitions int
	snap        *registrySnapshot
}

// registrySnapshot is the immutable per-tenant Snapshot the registry hands
// out. Its maps and slices are never modified after construction.
type registrySnapshot struct {
	byStage map[Stage][]*Compiled
}

// For returns a fresh slice of the stage's pipelines matching dataset, in name
// order (selector "*" matches every dataset). Callers may keep or mutate it.
func (s *registrySnapshot) For(stage Stage, dataset string) []*Compiled {
	return Match(s.byStage[stage], stage, dataset)
}

// Has reports whether any pipeline of stage exists for the tenant.
func (s *registrySnapshot) Has(stage Stage) bool { return len(s.byStage[stage]) > 0 }

// count returns the number of compiled definitions in the snapshot.
func (s *registrySnapshot) count() int {
	n := 0
	for _, defs := range s.byStage {
		n += len(defs)
	}
	return n
}

// tenantLock is a context-aware binary lock: acquire waits until the lock is
// free or ctx is done, tryAcquire never waits. Unlike sync.Mutex, a canceled
// request waiting for a long reload returns promptly.
type tenantLock struct {
	ch chan struct{}
}

// newTenantLock returns a free lock.
func newTenantLock() *tenantLock {
	l := &tenantLock{ch: make(chan struct{}, 1)}
	l.ch <- struct{}{}
	return l
}

// acquire takes the lock, or returns ctx's error while waiting.
func (l *tenantLock) acquire(ctx context.Context) error {
	select {
	case <-l.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryAcquire takes the lock if it is free.
func (l *tenantLock) tryAcquire() bool {
	select {
	case <-l.ch:
		return true
	default:
		return false
	}
}

// release frees the lock.
func (l *tenantLock) release() { l.ch <- struct{}{} }
