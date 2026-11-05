package pipeline

import (
	"context"
	"errors"
	"sync"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// readCtxKey is the private context key WrapEngine installs. Only this package
// can store or find it, so no other context value can be mistaken for the
// per-query holder and the holder can never leak into another engine.
type readCtxKey struct{}

// readSnapshot is one query's snapshot holder: the first Source operation of
// the query loads the tenant's snapshot, and every later Source call of the
// same query reuses that snapshot — or that load error. It is safe for
// concurrent use; the once caches both results, exactly as the query-pinning
// contract requires (a provider change mid-query is invisible to the query).
type readSnapshot struct {
	tenant   tenant.ID
	provider SnapshotProvider

	once sync.Once
	snap Snapshot
	err  error
}

// load returns the holder's snapshot, loading it on first use.
func (h *readSnapshot) load(ctx context.Context) (Snapshot, error) {
	h.once.Do(func() {
		h.snap, h.err = loadSnapshot(ctx, h.provider, h.tenant)
	})
	return h.snap, h.err
}

// WrapEngine returns an engine that forwards every method to inner unchanged —
// ParseSelector included — except Query, which runs inner with a context
// carrying a fresh lazy snapshot holder for p.Tenant. A read source built by
// NewReadSource reads that holder, so all Source calls of one query (a binary
// metric query selects twice) share one immutable snapshot while scalar and
// parse-only queries never load the provider at all. Each Query call gets its
// own holder, so separate queries observe provider updates.
//
// Used only by cmd wiring next to query.NewWith's WrapSource hook; there is no
// package-level state.
func WrapEngine(inner plan.Engine, provider SnapshotProvider) (plan.Engine, error) {
	if inner == nil {
		return nil, errors.New("pipeline: WrapEngine requires a plan.Engine")
	}
	if provider == nil {
		return nil, errors.New("pipeline: WrapEngine requires a SnapshotProvider")
	}
	return engineWrapper{Engine: inner, provider: provider}, nil
}

// engineWrapper is the plan.Engine WrapEngine returns. The embedded interface
// forwards every current and future method; Query is overridden only to
// install the holder.
type engineWrapper struct {
	plan.Engine
	provider SnapshotProvider
}

var _ plan.Engine = engineWrapper{}

// Query implements plan.Engine: the wrapped engine runs with a context that
// carries a fresh per-query snapshot holder for p.Tenant. Nothing else changes:
// src, p, results and errors pass through untouched, and the holder loads only
// if a wrapped Source actually runs.
func (e engineWrapper) Query(ctx context.Context, src plan.Source, p plan.Params) (plan.Result, error) {
	h := &readSnapshot{tenant: p.Tenant, provider: e.provider}
	return e.Engine.Query(context.WithValue(ctx, readCtxKey{}, h), src, p)
}
