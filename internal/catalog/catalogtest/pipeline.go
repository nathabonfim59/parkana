package catalogtest

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// RunPipelines exercises the pipeline half of catalog.Catalog on catalogs from
// newCatalog, which must return an empty catalog on each call. RunPipelines
// closes them. Run calls it, so an implementation that passes Run need not call
// it again.
func RunPipelines(t *testing.T, newCatalog func(t *testing.T) catalog.Catalog) {
	t.Helper()
	open := func(t *testing.T) catalog.Catalog {
		c := newCatalog(t)
		t.Cleanup(func() {
			if err := c.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
		return c
	}
	def := func(tid tenant.ID, name string, stage catalog.PipelineStage, selector, spec string) catalog.PipelineDefinition {
		return catalog.PipelineDefinition{
			Tenant: tid, Name: name, Stage: stage, Selector: selector, Spec: []byte(spec),
		}
	}
	mustPut := func(t *testing.T, c catalog.Catalog, d catalog.PipelineDefinition, expected int64) catalog.PipelineDefinition {
		t.Helper()
		got, err := c.PutPipeline(t.Context(), d, expected)
		if err != nil {
			t.Fatalf("PutPipeline(%q): %v", d.Name, err)
		}
		return got
	}
	mustGet := func(t *testing.T, c catalog.Catalog, tid tenant.ID, name string) catalog.PipelineDefinition {
		t.Helper()
		got, err := c.GetPipeline(t.Context(), tid, name)
		if err != nil {
			t.Fatalf("GetPipeline(%q): %v", name, err)
		}
		return got
	}
	token := func(t *testing.T, c catalog.Catalog, tid tenant.ID) int64 {
		t.Helper()
		got, err := c.PipelineChangeToken(t.Context(), tid)
		if err != nil {
			t.Fatalf("PipelineChangeToken(%q): %v", tid, err)
		}
		return got
	}
	wantErr := func(t *testing.T, err, want error) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Fatalf("error = %v, want %v", err, want)
		}
	}
	names := func(t *testing.T, c catalog.Catalog, tid tenant.ID) []string {
		t.Helper()
		list, err := c.ListPipelines(t.Context(), tid)
		if err != nil {
			t.Fatalf("ListPipelines(%q): %v", tid, err)
		}
		out := make([]string, len(list))
		for i, d := range list {
			out[i] = d.Name
		}
		return out
	}

	t.Run("CreateGetList", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()

		bSpec := []byte("{\n  \"steps\": []\n}")
		b := mustPut(t, c, def(tenant.Default, "b", catalog.PipelineStageIngest, "api", string(bSpec)), 0)
		a := mustPut(t, c, def(tenant.Default, "a", catalog.PipelineStageRead, "*", `{"steps":[{"type":"redact"}]}`), 0)

		for _, got := range []catalog.PipelineDefinition{a, b} {
			if got.Version != 1 || got.Deleted() {
				t.Fatalf("%q = version %d deleted %v, want version 1 live", got.Name, got.Version, got.Deleted())
			}
			if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() || !got.CreatedAt.Equal(got.UpdatedAt) {
				t.Fatalf("%q timestamps = %v / %v", got.Name, got.CreatedAt, got.UpdatedAt)
			}
			if got.CreatedAt.Location() != time.UTC || got.CreatedAt.Nanosecond()%int(time.Microsecond) != 0 {
				t.Fatalf("%q CreatedAt = %v, want UTC microseconds", got.Name, got.CreatedAt)
			}
		}

		if got := names(t, c, tenant.Default); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("ListPipelines = %v, want [a b]", got)
		}
		got := mustGet(t, c, tenant.Default, "b")
		if got.Stage != catalog.PipelineStageIngest || got.Selector != "api" || !bytes.Equal(got.Spec, bSpec) {
			t.Fatalf("GetPipeline(b) = %+v", got)
		}

		acme := mustPut(t, c, def("acme", "c", catalog.PipelineStageIngest, "web", `{"steps":[]}`), 0)
		if acme.Version != 1 {
			t.Fatalf("acme version = %d, want 1", acme.Version)
		}
		if got := names(t, c, tenant.Default); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("default list changed by acme write: %v", got)
		}
		if got := names(t, c, "acme"); !slices.Equal(got, []string{"c"}) {
			t.Fatalf("acme list = %v", got)
		}
		if _, err := c.GetPipeline(ctx, tenant.Default, "c"); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("cross-tenant GetPipeline = %v, want ErrNotFound", err)
		}
		if got := names(t, c, "other"); len(got) != 0 {
			t.Fatalf("unknown tenant list = %v", got)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("default token = %d, want 2", got)
		}
		if got := token(t, c, "acme"); got != 1 {
			t.Fatalf("acme token = %d, want 1", got)
		}
		if got := token(t, c, "other"); got != 0 {
			t.Fatalf("unknown tenant token = %d, want 0", got)
		}
	})

	t.Run("UpdateAndStaleAtomic", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		created := mustPut(t, c, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[1]}`), 0)
		if got := token(t, c, tenant.Default); got != 1 {
			t.Fatalf("token after create = %d, want 1", got)
		}

		text := def(tenant.Default, "p", catalog.PipelineStageRead, "*", `{"steps":[2]}`)
		updated, err := c.PutPipeline(ctx, text, 1)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if updated.Version != 2 || updated.Stage != catalog.PipelineStageRead || updated.Selector != "*" ||
			string(updated.Spec) != `{"steps":[2]}` || !updated.CreatedAt.Equal(created.CreatedAt) ||
			updated.UpdatedAt.Before(created.UpdatedAt) {
			t.Fatalf("update = %+v (created %+v)", updated, created)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("token after update = %d, want 2", got)
		}

		_, err = c.PutPipeline(ctx, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[3]}`), 1)
		wantErr(t, err, catalog.ErrVersionConflict)
		got := mustGet(t, c, tenant.Default, "p")
		if got.Version != 2 || got.Stage != catalog.PipelineStageRead || string(got.Spec) != `{"steps":[2]}` {
			t.Fatalf("stale update changed state: %+v", got)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("stale update advanced token to %d", got)
		}

		_, err = c.PutPipeline(ctx, def(tenant.Default, "missing", catalog.PipelineStageIngest, "api", `{}`), 1)
		wantErr(t, err, catalog.ErrNotFound)
		_, err = c.PutPipeline(ctx, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{}`), 0)
		wantErr(t, err, catalog.ErrVersionConflict)
		_, err = c.PutPipeline(ctx, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{}`), 99)
		wantErr(t, err, catalog.ErrVersionConflict)
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("failed writes advanced token to %d", got)
		}
	})

	t.Run("DeleteRecreate", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		created := mustPut(t, c, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[]}`), 0)

		tomb, err := c.DeletePipeline(ctx, tenant.Default, "p", 1)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		if tomb.Version != 2 || !tomb.Deleted() || tomb.DeletedAt.IsZero() || !tomb.DeletedAt.Equal(tomb.UpdatedAt) {
			t.Fatalf("tombstone = %+v", tomb)
		}
		if _, err := c.GetPipeline(ctx, tenant.Default, "p"); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("GetPipeline after delete = %v, want ErrNotFound", err)
		}
		if got := names(t, c, tenant.Default); len(got) != 0 {
			t.Fatalf("list after delete = %v", got)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("token after delete = %d, want 2", got)
		}

		_, err = c.DeletePipeline(ctx, tenant.Default, "p", 1)
		wantErr(t, err, catalog.ErrNotFound)
		_, err = c.DeletePipeline(ctx, tenant.Default, "p", 2)
		wantErr(t, err, catalog.ErrNotFound) // tombstoned, not live
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("failed deletes advanced token to %d", got)
		}

		// Create expects zero even over a tombstone; the version continues
		// above the tombstone so recreation never resets it.
		_, err = c.PutPipeline(ctx, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{}`), 2)
		wantErr(t, err, catalog.ErrNotFound) // no live row
		recreated := mustPut(t, c, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[9]}`), 0)
		if recreated.Version != 3 || recreated.Deleted() || recreated.CreatedAt.IsZero() ||
			// Equality is fine: recreation can land in the same microsecond.
			recreated.CreatedAt.Before(created.CreatedAt) || string(recreated.Spec) != `{"steps":[9]}` {
			t.Fatalf("recreated = %+v (original %+v)", recreated, created)
		}
		if got := token(t, c, tenant.Default); got != 3 {
			t.Fatalf("token after recreate = %d, want 3", got)
		}
		if got := names(t, c, tenant.Default); !slices.Equal(got, []string{"p"}) {
			t.Fatalf("list after recreate = %v", got)
		}

		_, err = c.DeletePipeline(ctx, tenant.Default, "p", 2)
		wantErr(t, err, catalog.ErrVersionConflict) // live version is 3

		// Tombstones are per tenant: the same name is independent elsewhere.
		if _, err := c.DeletePipeline(ctx, "acme", "p", 1); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("delete in other tenant = %v, want ErrNotFound", err)
		}
	})

	t.Run("DeepCopyAndOpaqueBytes", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		original := []byte(`{ "steps" : [ {"type" : "parse_json"} ] }`)
		want := slices.Clone(original)
		in := def(tenant.Default, "p", catalog.PipelineStageIngest, "api", "")
		in.Spec = original
		created := mustPut(t, c, in, 0)

		in.Spec[0] = 'X'
		got := mustGet(t, c, tenant.Default, "p")
		if !bytes.Equal(got.Spec, want) {
			t.Fatalf("caller mutation leaked into catalog: %q", got.Spec)
		}
		got.Spec[1] = 'Y'
		again := mustGet(t, c, tenant.Default, "p")
		if !bytes.Equal(again.Spec, want) {
			t.Fatalf("returned mutation leaked into catalog: %q", again.Spec)
		}
		created.Spec[2] = 'Z'
		list, err := c.ListPipelines(ctx, tenant.Default)
		if err != nil || len(list) != 1 || !bytes.Equal(list[0].Spec, want) {
			t.Fatalf("ListPipelines = %+v, %v", list, err)
		}
		list[0].Spec[0] = 'W'
		if got := mustGet(t, c, tenant.Default, "p"); !bytes.Equal(got.Spec, want) {
			t.Fatalf("list result mutation leaked into catalog: %q", got.Spec)
		}

		// nil and empty specs are normalised; bytes stay opaque.
		mustPut(t, c, catalog.PipelineDefinition{
			Tenant: tenant.Default, Name: "empty", Stage: catalog.PipelineStageRead,
			Selector: "api", Spec: []byte{},
		}, 0)
		if got := mustGet(t, c, tenant.Default, "empty"); got.Spec == nil || len(got.Spec) != 0 {
			t.Fatalf("empty spec = %#v, want non-nil empty", got.Spec)
		}
	})

	t.Run("CatalogOwnedFieldsIgnoreInput", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		old := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)

		in := def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`)
		in.Version = 99
		in.CreatedAt = old
		in.UpdatedAt = old
		in.DeletedAt = old
		created, err := c.PutPipeline(ctx, in, 0)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if created.Version != 1 || created.Deleted() || created.CreatedAt.IsZero() ||
			created.CreatedAt.Equal(old) || created.UpdatedAt.Equal(old) {
			t.Fatalf("create used caller-owned fields: %+v", created)
		}

		in = def(tenant.Default, "p", catalog.PipelineStageRead, "*", `{"v":2}`)
		in.Version = 77
		in.CreatedAt = old.Add(time.Hour)
		in.UpdatedAt = old
		in.DeletedAt = old
		updated, err := c.PutPipeline(ctx, in, 1)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if updated.Version != 2 || updated.Deleted() || !updated.CreatedAt.Equal(created.CreatedAt) ||
			updated.UpdatedAt.Equal(old) {
			t.Fatalf("update used caller-owned fields: %+v (created %+v)", updated, created)
		}
	})

	t.Run("CanceledContexts", func(t *testing.T) {
		c := open(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		d := def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{}`)

		if _, err := c.GetPipeline(ctx, tenant.Default, "p"); !errors.Is(err, context.Canceled) {
			t.Fatalf("GetPipeline canceled = %v", err)
		}
		if _, err := c.ListPipelines(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
			t.Fatalf("ListPipelines canceled = %v", err)
		}
		if _, err := c.PutPipeline(ctx, d, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("PutPipeline canceled = %v", err)
		}
		if _, err := c.DeletePipeline(ctx, tenant.Default, "p", 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("DeletePipeline canceled = %v", err)
		}
		if _, err := c.PipelineChangeToken(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
			t.Fatalf("PipelineChangeToken canceled = %v", err)
		}

		if got := names(t, c, tenant.Default); len(got) != 0 {
			t.Fatalf("canceled PutPipeline wrote: %v", got)
		}
		if got := token(t, c, tenant.Default); got != 0 {
			t.Fatalf("canceled PutPipeline advanced token to %d", got)
		}
	})

	t.Run("InvalidDefinitions", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		valid := def(tenant.Default, "valid", catalog.PipelineStageIngest, "api", `{}`)
		for _, tc := range []struct {
			name   string
			mutate func(*catalog.PipelineDefinition)
			want   error
		}{
			{"empty tenant", func(d *catalog.PipelineDefinition) { d.Tenant = "" }, tenant.ErrInvalid},
			{"bad tenant", func(d *catalog.PipelineDefinition) { d.Tenant = "not valid" }, tenant.ErrInvalid},
			{"empty name", func(d *catalog.PipelineDefinition) { d.Name = "" }, catalog.ErrInvalidPipeline},
			{"name space", func(d *catalog.PipelineDefinition) { d.Name = "a b" }, catalog.ErrInvalidPipeline},
			{"name slash", func(d *catalog.PipelineDefinition) { d.Name = "a/b" }, catalog.ErrInvalidPipeline},
			{"name dot", func(d *catalog.PipelineDefinition) { d.Name = "." }, catalog.ErrInvalidPipeline},
			{"name long", func(d *catalog.PipelineDefinition) { d.Name = strings.Repeat("a", 129) }, catalog.ErrInvalidPipeline},
			{"name unicode", func(d *catalog.PipelineDefinition) { d.Name = "naïve" }, catalog.ErrInvalidPipeline},
			{"empty stage", func(d *catalog.PipelineDefinition) { d.Stage = "" }, catalog.ErrInvalidPipeline},
			{"bad stage", func(d *catalog.PipelineDefinition) { d.Stage = "both" }, catalog.ErrInvalidPipeline},
			{"empty selector", func(d *catalog.PipelineDefinition) { d.Selector = "" }, catalog.ErrInvalidPipeline},
			{"unsanitized selector", func(d *catalog.PipelineDefinition) { d.Selector = "API" }, catalog.ErrInvalidPipeline},
			{"selector space", func(d *catalog.PipelineDefinition) { d.Selector = "a b" }, catalog.ErrInvalidPipeline},
			{"selector long", func(d *catalog.PipelineDefinition) { d.Selector = strings.Repeat("a", 129) }, catalog.ErrInvalidPipeline},
		} {
			d := valid
			tc.mutate(&d)
			if _, err := c.PutPipeline(ctx, d, 0); !errors.Is(err, tc.want) {
				t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
			}
		}
		if got := names(t, c, tenant.Default); len(got) != 0 {
			t.Fatalf("invalid definitions were stored: %v", got)
		}
		if got := token(t, c, tenant.Default); got != 0 {
			t.Fatalf("invalid definitions advanced token to %d", got)
		}

		// "*", lowercase sanitized names and a name at the limit are valid.
		for i, d := range []catalog.PipelineDefinition{
			def(tenant.Default, "Wild", catalog.PipelineStageIngest, "*", `{}`),
			def(tenant.Default, strings.Repeat("a", 128), catalog.PipelineStageIngest, "api_v1-blue", `{}`),
		} {
			if _, err := c.PutPipeline(ctx, d, 0); err != nil {
				t.Fatalf("valid definition %d: %v", i, err)
			}
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("token = %d after two valid creates, want 2", got)
		}
	})

	t.Run("TokenAdvancesAcrossNames", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		mustPut(t, c, def(tenant.Default, "a", catalog.PipelineStageIngest, "api", `{}`), 0)
		if got := token(t, c, tenant.Default); got != 1 {
			t.Fatalf("token = %d, want 1", got)
		}
		mustPut(t, c, def(tenant.Default, "b", catalog.PipelineStageIngest, "api", `{}`), 0)
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("token = %d, want 2", got)
		}
		mustPut(t, c, def(tenant.Default, "a", catalog.PipelineStageIngest, "api", `{"v":2}`), 1)
		if got := token(t, c, tenant.Default); got != 3 {
			t.Fatalf("token = %d, want 3", got)
		}
		if _, err := c.DeletePipeline(ctx, tenant.Default, "b", 1); err != nil {
			t.Fatalf("delete b: %v", err)
		}
		if got := token(t, c, tenant.Default); got != 4 {
			t.Fatalf("token = %d, want 4", got)
		}
		recreated := mustPut(t, c, def(tenant.Default, "b", catalog.PipelineStageRead, "web", `{}`), 0)
		if recreated.Version != 3 {
			t.Fatalf("recreated b version = %d, want 3", recreated.Version)
		}
		if got := token(t, c, tenant.Default); got != 5 {
			t.Fatalf("token = %d, want 5", got)
		}
		if _, err := c.DeletePipeline(ctx, tenant.Default, "a", 2); err != nil {
			t.Fatalf("delete a: %v", err)
		}
		if got := token(t, c, tenant.Default); got != 6 {
			t.Fatalf("token = %d, want 6", got)
		}
		// Stale and missing writes never advance the token.
		_, err := c.PutPipeline(ctx, def(tenant.Default, "a", catalog.PipelineStageIngest, "api", `{}`), 2)
		wantErr(t, err, catalog.ErrNotFound) // a is tombstoned
		_, err = c.DeletePipeline(ctx, tenant.Default, "a", 3)
		wantErr(t, err, catalog.ErrNotFound)
		if got := token(t, c, tenant.Default); got != 6 {
			t.Fatalf("failed writes advanced token to %d", got)
		}
	})

	t.Run("TenantIsolation", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		mustPut(t, c, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"tenant":"default"}`), 0)
		mustPut(t, c, def("acme", "p", catalog.PipelineStageIngest, "api", `{"tenant":"acme"}`), 0)
		if got := token(t, c, tenant.Default); got != 1 {
			t.Fatalf("default token = %d, want 1", got)
		}
		if got := token(t, c, "acme"); got != 1 {
			t.Fatalf("acme token = %d, want 1", got)
		}

		updated, err := c.PutPipeline(ctx, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":2}`), 1)
		if err != nil {
			t.Fatalf("update default: %v", err)
		}
		if updated.Version != 2 {
			t.Fatalf("default version = %d, want 2", updated.Version)
		}
		if got := mustGet(t, c, "acme", "p"); got.Version != 1 || string(got.Spec) != `{"tenant":"acme"}` {
			t.Fatalf("acme changed by default update: %+v", got)
		}

		if _, err := c.DeletePipeline(ctx, "acme", "p", 1); err != nil {
			t.Fatalf("delete acme: %v", err)
		}
		if got := mustGet(t, c, tenant.Default, "p"); got.Version != 2 || got.Deleted() {
			t.Fatalf("default changed by acme delete: %+v", got)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("default token = %d after acme delete, want 2", got)
		}
		if got := token(t, c, "acme"); got != 2 {
			t.Fatalf("acme token = %d after delete, want 2", got)
		}
	})

	t.Run("ConcurrentCreateOneWinner", func(t *testing.T) {
		c := open(t)
		const writers = 8
		d := def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"steps":[]}`)
		start := make(chan struct{})
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Go(func() {
				<-start
				_, errs[i] = c.PutPipeline(t.Context(), d, 0)
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
		if got := mustGet(t, c, tenant.Default, "p"); got.Version != 1 {
			t.Fatalf("winner version = %d, want 1", got.Version)
		}
		if got := token(t, c, tenant.Default); got != 1 {
			t.Fatalf("token = %d after concurrent create, want 1", got)
		}
	})

	t.Run("ConcurrentUpdateOneWinner", func(t *testing.T) {
		c := open(t)
		mustPut(t, c, def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`), 0)
		const writers = 8
		start := make(chan struct{})
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Go(func() {
				<-start
				d := def(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":2}`)
				_, errs[i] = c.PutPipeline(t.Context(), d, 1)
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
			t.Fatalf("%d concurrent updates succeeded, want exactly 1", successes)
		}
		if got := mustGet(t, c, tenant.Default, "p"); got.Version != 2 {
			t.Fatalf("final version = %d, want 2", got.Version)
		}
		if got := token(t, c, tenant.Default); got != 2 {
			t.Fatalf("token = %d after concurrent update, want 2", got)
		}
	})
}
