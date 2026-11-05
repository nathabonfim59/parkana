package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// sameTokenCancelingCatalog returns the underlying token unchanged but cancels
// the caller's context while doing so, simulating a catalog that finishes as
// the caller gives up.
type sameTokenCancelingCatalog struct {
	catalog.Catalog
	lists  atomic.Int64
	cancel context.CancelFunc
}

func (c *sameTokenCancelingCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	token, err := c.Catalog.PipelineChangeToken(ctx, t)
	if err == nil && c.cancel != nil {
		c.cancel()
	}
	return token, err
}

func (c *sameTokenCancelingCatalog) ListPipelines(ctx context.Context, t tenant.ID) ([]catalog.PipelineDefinition, error) {
	c.lists.Add(1)
	return c.Catalog.ListPipelines(ctx, t)
}

// TestReloadLockedUnchangedTokenObservesCancellation covers the reload path
// where a tenant already has a snapshot and the catalog returns the SAME token
// after the caller canceled: the reload must report context.Canceled, must not
// list definitions, and must not publish.
func TestReloadLockedUnchangedTokenObservesCancellation(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	id := tenant.ID("tenant-a")
	if _, err := base.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   id,
		Name:     "a",
		Stage:    catalog.PipelineStageIngest,
		Selector: "*",
		Spec:     []byte(`{"steps":[{"type":"drop_if","expr":"false"}]}`),
	}, 0); err != nil {
		t.Fatalf("PutPipeline: %v", err)
	}
	cat := &sameTokenCancelingCatalog{Catalog: base}
	reg, err := NewRegistry(cat, RegistryOptions{
		CompileOptions: CompileOptions{StreamLabels: []string{"service"}},
		Role:           RoleIngester,
		Metrics:        prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { reg.Close() })

	if _, err := reg.Snapshot(t.Context(), id); err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("first load made %d ListPipelines calls, want 1", got)
	}
	st, err := reg.stateFor(id)
	if err != nil {
		t.Fatalf("stateFor: %v", err)
	}
	before := st.view.Load()
	if before == nil {
		t.Fatal("first load published no snapshot")
	}

	// The token is unchanged, so this is the no-op branch; canceling inside
	// the token call makes it anything but a successful no-op.
	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cat.cancel = cancel
	if !st.reload.tryAcquire() {
		t.Fatal("tenant reload lock unexpectedly held")
	}
	err = reg.reloadLocked(runCtx, id, st)
	st.reload.release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unchanged-token reload with a canceled caller = %v, want context.Canceled", err)
	}
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("canceled unchanged-token reload made %d ListPipelines calls, want no new list", got)
	}
	if got := st.view.Load(); got != before {
		t.Fatal("canceled unchanged-token reload published a new snapshot")
	}
}
