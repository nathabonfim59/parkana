package catalog_test

import (
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func openPipelineCatalog(t *testing.T, path string) *catalog.SQLite {
	t.Helper()
	c, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite(%s): %v", path, err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func pipelineDef(tenantID tenant.ID, name string, stage catalog.PipelineStage, selector, spec string) catalog.PipelineDefinition {
	return catalog.PipelineDefinition{
		Tenant: tenantID, Name: name, Stage: stage, Selector: selector, Spec: []byte(spec),
	}
}

func pipelineToken(t *testing.T, c catalog.Catalog, tid tenant.ID) int64 {
	t.Helper()
	token, err := c.PipelineChangeToken(t.Context(), tid)
	if err != nil {
		t.Fatalf("PipelineChangeToken: %v", err)
	}
	return token
}

func pipelineNames(t *testing.T, c catalog.Catalog, tid tenant.ID) []string {
	t.Helper()
	list, err := c.ListPipelines(t.Context(), tid)
	if err != nil {
		t.Fatalf("ListPipelines: %v", err)
	}
	names := make([]string, len(list))
	for i, d := range list {
		names[i] = d.Name
	}
	return names
}

func TestSQLitePipelinesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	c1 := openPipelineCatalog(t, path)
	ctx := t.Context()

	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[1]}`), 0); err != nil {
		t.Fatalf("create p: %v", err)
	}
	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageRead, "*", `{"steps":[2]}`), 1); err != nil {
		t.Fatalf("update p: %v", err)
	}
	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "q", catalog.PipelineStageIngest, "web", `{}`), 0); err != nil {
		t.Fatalf("create q: %v", err)
	}
	if _, err := c1.DeletePipeline(ctx, tenant.Default, "q", 1); err != nil {
		t.Fatalf("delete q: %v", err)
	}
	if got := pipelineToken(t, c1, tenant.Default); got != 4 {
		t.Fatalf("token before reopen = %d, want 4", got)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2 := openPipelineCatalog(t, path)
	if got := pipelineToken(t, c2, tenant.Default); got != 4 {
		t.Fatalf("token after reopen = %d, want 4", got)
	}
	if got := pipelineNames(t, c2, tenant.Default); !slices.Equal(got, []string{"p"}) {
		t.Fatalf("live after reopen = %v, want [p]", got)
	}
	p, err := c2.GetPipeline(ctx, tenant.Default, "p")
	if err != nil {
		t.Fatalf("get p: %v", err)
	}
	if p.Version != 2 || p.Stage != catalog.PipelineStageRead || p.Selector != "*" || string(p.Spec) != `{"steps":[2]}` {
		t.Fatalf("p after reopen = %+v", p)
	}
	if _, err := c2.GetPipeline(ctx, tenant.Default, "q"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("tombstone after reopen = %v, want ErrNotFound", err)
	}
	// Recreation continues above the persisted tombstone version.
	q, err := c2.PutPipeline(ctx, pipelineDef(tenant.Default, "q", catalog.PipelineStageIngest, "web", `{"steps":[3]}`), 0)
	if err != nil {
		t.Fatalf("recreate q: %v", err)
	}
	if q.Version != 3 {
		t.Fatalf("recreated q version = %d, want 3", q.Version)
	}
	if got := pipelineToken(t, c2, tenant.Default); got != 5 {
		t.Fatalf("token after recreate = %d, want 5", got)
	}
}

func TestSQLitePipelinesTwoHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	c1 := openPipelineCatalog(t, path)
	c2 := openPipelineCatalog(t, path)
	ctx := t.Context()

	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`), 0); err != nil {
		t.Fatalf("c1 create: %v", err)
	}
	if got, err := c2.GetPipeline(ctx, tenant.Default, "p"); err != nil || got.Version != 1 {
		t.Fatalf("c2 get after c1 create = %+v, %v", got, err)
	}
	if got := pipelineToken(t, c2, tenant.Default); got != 1 {
		t.Fatalf("c2 token = %d, want 1", got)
	}

	if _, err := c2.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageRead, "*", `{"v":2}`), 1); err != nil {
		t.Fatalf("c2 update: %v", err)
	}
	// c1 has the stale version from before c2 wrote: the conflict must be
	// reported as ErrVersionConflict, not a raw SQLite busy error.
	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":3}`), 1); !errors.Is(err, catalog.ErrVersionConflict) {
		t.Fatalf("c1 stale update = %v, want ErrVersionConflict", err)
	}
	if _, err := c1.DeletePipeline(ctx, tenant.Default, "p", 1); !errors.Is(err, catalog.ErrVersionConflict) {
		t.Fatalf("c1 stale delete = %v, want ErrVersionConflict", err)
	}
	if got := pipelineToken(t, c1, tenant.Default); got != 2 {
		t.Fatalf("token after conflicts = %d, want 2", got)
	}

	if _, err := c2.DeletePipeline(ctx, tenant.Default, "p", 2); err != nil {
		t.Fatalf("c2 delete: %v", err)
	}
	if _, err := c1.GetPipeline(ctx, tenant.Default, "p"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("c1 get after c2 delete = %v, want ErrNotFound", err)
	}
	if got := pipelineToken(t, c1, tenant.Default); got != 3 {
		t.Fatalf("c1 token after delete = %d, want 3", got)
	}

	recreated, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":4}`), 0)
	if err != nil {
		t.Fatalf("c1 recreate: %v", err)
	}
	if recreated.Version != 4 {
		t.Fatalf("recreated version = %d, want 4", recreated.Version)
	}
	if got, err := c2.GetPipeline(ctx, tenant.Default, "p"); err != nil || got.Version != 4 {
		t.Fatalf("c2 get after recreate = %+v, %v", got, err)
	}
	if got := pipelineToken(t, c2, tenant.Default); got != 4 {
		t.Fatalf("c2 token after recreate = %d, want 4", got)
	}
}

func TestSQLitePipelinesTwoHandlesConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	c1 := openPipelineCatalog(t, path)
	c2 := openPipelineCatalog(t, path)

	t.Run("different names both succeed", func(t *testing.T) {
		start := make(chan struct{})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, c := range []*catalog.SQLite{c1, c2} {
			wg.Go(func() {
				<-start
				name := []string{"a", "b"}[i]
				_, errs[i] = c.PutPipeline(t.Context(),
					pipelineDef(tenant.Default, name, catalog.PipelineStageIngest, "api", `{}`), 0)
			})
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("writer %d: %v, want success", i, err)
			}
		}
		if got := pipelineNames(t, c1, tenant.Default); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("names = %v, want [a b]", got)
		}
		if got := pipelineToken(t, c1, tenant.Default); got != 2 {
			t.Fatalf("token = %d, want 2 after two distinct writes", got)
		}
	})

	t.Run("same name exactly one winner", func(t *testing.T) {
		start := make(chan struct{})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, c := range []*catalog.SQLite{c1, c2} {
			wg.Go(func() {
				<-start
				_, errs[i] = c.PutPipeline(t.Context(),
					pipelineDef(tenant.Default, "race", catalog.PipelineStageIngest, "api", `{}`), 0)
			})
		}
		close(start)
		wg.Wait()

		successes := 0
		for i, err := range errs {
			switch {
			case err == nil:
				successes++
			case !errors.Is(err, catalog.ErrVersionConflict):
				t.Fatalf("writer %d: %v, want nil or ErrVersionConflict", i, err)
			}
		}
		if successes != 1 {
			t.Fatalf("%d concurrent creates succeeded, want exactly 1", successes)
		}
		if got := pipelineToken(t, c1, tenant.Default); got != 3 {
			t.Fatalf("token = %d, want 3 (two distinct writes plus one race winner)", got)
		}
	})
}
