package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestPipelineCountersIgnoreClockResolution pins the catalog clock so every
// mutation has the same timestamp, then checks versions and the change token
// still advance by exactly one: neither depends on wall-clock resolution.
func TestPipelineCountersIgnoreClockResolution(t *testing.T) {
	fixed := time.Date(2026, 11, 5, 12, 0, 0, 123456789, time.UTC)
	require := func(t *testing.T, c Catalog) {
		t.Helper()
		ctx := t.Context()
		def := func(spec string) PipelineDefinition {
			return PipelineDefinition{
				Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
				Selector: "api", Spec: []byte(spec),
			}
		}
		created, err := c.PutPipeline(ctx, def(`{"v":1}`), 0)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		updated, err := c.PutPipeline(ctx, def(`{"v":2}`), 1)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		deleted, err := c.DeletePipeline(ctx, tenant.Default, "p", 2)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		recreated, err := c.PutPipeline(ctx, def(`{"v":3}`), 0)
		if err != nil {
			t.Fatalf("recreate: %v", err)
		}
		for i, got := range []PipelineDefinition{created, updated, deleted, recreated} {
			if want := int64(i + 1); got.Version != want {
				t.Fatalf("step %d version = %d, want %d", i, got.Version, want)
			}
		}
		for _, got := range []PipelineDefinition{created, updated, deleted, recreated} {
			if want := pipelineMicros(fixed); !got.UpdatedAt.Equal(want) || !got.CreatedAt.Equal(want) {
				t.Fatalf("timestamps = %v / %v, want pinned %v", got.CreatedAt, got.UpdatedAt, want)
			}
		}
		token, err := c.PipelineChangeToken(ctx, tenant.Default)
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		if token != 4 {
			t.Fatalf("token = %d, want 4 after four mutations", token)
		}
	}

	t.Run("memory", func(t *testing.T) {
		c := NewMemory()
		c.now = func() time.Time { return fixed }
		require(t, c)
	})
	t.Run("sqlite", func(t *testing.T) {
		c, err := NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
		if err != nil {
			t.Fatalf("NewSQLite: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		c.now = func() time.Time { return fixed }
		require(t, c)
	})
}

// firstCheckContext wraps a real cancelable context and closes first the first
// time Err is observed. A test holds a Memory lock, waits for first (so the
// method's entry check saw a live context), then cancels and unlocks: the
// method's post-lock recheck sees a genuine cancellation with Done closed.
type firstCheckContext struct {
	context.Context
	first chan struct{}
	once  sync.Once
}

// newFirstCheckContext returns the wrapper and the underlying cancel func.
func newFirstCheckContext(parent context.Context) (*firstCheckContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	return &firstCheckContext{Context: ctx, first: make(chan struct{})}, cancel
}

func (c *firstCheckContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.first) })
	return err
}

// canceledWhileHeld runs call with a first-check context while holding m.mu,
// cancels it after the entry check, then releases the lock. call must perform
// its entry ctx check before taking the lock.
func canceledWhileHeld(t *testing.T, m *Memory, call func(context.Context) error) error {
	t.Helper()
	ctx, cancel := newFirstCheckContext(t.Context())
	defer cancel()
	m.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- call(ctx) }()
	<-ctx.first
	cancel()
	m.mu.Unlock()
	return <-done
}

// TestMemoryPipelineCanceledAfterEntryCheck is the regression for a context
// canceled while a method waits for the Memory lock: the entry check passes,
// the wait returns, and the method must recheck before touching state.
func TestMemoryPipelineCanceledAfterEntryCheck(t *testing.T) {
	def := func(spec string) PipelineDefinition {
		return PipelineDefinition{
			Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
			Selector: "api", Spec: []byte(spec),
		}
	}

	t.Run("put", func(t *testing.T) {
		m := NewMemory()
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			_, err := m.PutPipeline(ctx, def(`{"v":1}`), 0)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PutPipeline = %v, want context.Canceled", err)
		}
		if rows := m.pipelines[tenant.Default]; len(rows) != 0 {
			t.Fatalf("canceled PutPipeline wrote %d rows", len(rows))
		}
		if token, err := m.PipelineChangeToken(t.Context(), tenant.Default); err != nil || token != 0 {
			t.Fatalf("token = %d, %v, want 0, nil", token, err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		m := NewMemory()
		if _, err := m.PutPipeline(t.Context(), def(`{"v":1}`), 0); err != nil {
			t.Fatalf("seed: %v", err)
		}
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			_, err := m.DeletePipeline(ctx, tenant.Default, "p", 1)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DeletePipeline = %v, want context.Canceled", err)
		}
		rec := m.pipelines[tenant.Default]["p"]
		if rec.def.Deleted() || rec.def.Version != 1 || string(rec.def.Spec) != `{"v":1}` || rec.token != 1 {
			t.Fatalf("canceled DeletePipeline changed state: %+v token %d", rec.def, rec.token)
		}
		if token, err := m.PipelineChangeToken(t.Context(), tenant.Default); err != nil || token != 1 {
			t.Fatalf("token = %d, %v, want 1, nil", token, err)
		}
	})

	t.Run("list with rows", func(t *testing.T) {
		m := NewMemory()
		for i, name := range []string{"a", "b", "c"} {
			d := def(`{"v":` + string(rune('1'+i)) + `}`)
			d.Name = name
			if _, err := m.PutPipeline(t.Context(), d, 0); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}
		var list []PipelineDefinition
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			var err error
			list, err = m.ListPipelines(ctx, tenant.Default)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ListPipelines = %v, want context.Canceled", err)
		}
		if list != nil {
			t.Fatalf("ListPipelines returned %d rows while canceled", len(list))
		}
	})

	t.Run("list empty tenant", func(t *testing.T) {
		m := NewMemory()
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			_, err := m.ListPipelines(ctx, tenant.Default)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ListPipelines empty tenant = %v, want context.Canceled", err)
		}
	})

	t.Run("get", func(t *testing.T) {
		m := NewMemory()
		if _, err := m.PutPipeline(t.Context(), def(`{"v":1}`), 0); err != nil {
			t.Fatalf("seed: %v", err)
		}
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			_, err := m.GetPipeline(ctx, tenant.Default, "p")
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetPipeline = %v, want context.Canceled", err)
		}
	})

	t.Run("change token", func(t *testing.T) {
		m := NewMemory()
		err := canceledWhileHeld(t, m, func(ctx context.Context) error {
			_, err := m.PipelineChangeToken(ctx, tenant.Default)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PipelineChangeToken = %v, want context.Canceled", err)
		}
	})
}
