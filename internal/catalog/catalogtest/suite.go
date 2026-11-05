// Package catalogtest is the conformance suite every catalog.Catalog
// implementation must pass.
package catalogtest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Run exercises catalogs from newCatalog, which must return an empty catalog
// on each call. Run closes them.
func Run(t *testing.T, newCatalog func(t *testing.T) catalog.Catalog) {
	t0 := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	file := func(key, stream string, minMin, maxMin int, lv map[string][]string) catalog.File {
		return catalog.File{
			Tenant: tenant.Default, Stream: stream, Key: key,
			MinTS: t0.Add(time.Duration(minMin) * time.Minute), MaxTS: t0.Add(time.Duration(maxMin) * time.Minute),
			Rows: 10, Bytes: 1000, LabelValues: lv,
		}
	}
	open := func(t *testing.T) catalog.Catalog {
		c := newCatalog(t)
		t.Cleanup(func() { c.Close() })
		return c
	}
	keys := func(t *testing.T, c catalog.Catalog, f catalog.Filter) []string {
		t.Helper()
		var out []string
		for file, err := range c.ListFiles(t.Context(), f) {
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, file.Key)
		}
		return out
	}

	complete := func(f catalog.File) catalog.File {
		f.LabelsComplete = true
		return f
	}

	t.Run("AddAndList", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		if err := c.AddFiles(ctx,
			complete(file("b", "api", 0, 10, map[string][]string{"level": {"error", "info"}})),
			complete(file("a", "api", 0, 5, map[string][]string{"level": {"info"}})),
			file("c", "web", 20, 30, nil),
			// Incomplete summary (cataloged before phase 3): never pruned by label.
			file("d", "api", 40, 50, map[string][]string{"level": {"info"}}),
		); err != nil {
			t.Fatal(err)
		}
		all := catalog.Filter{Tenant: tenant.Default}
		if got := keys(t, c, all); !slices.Equal(got, []string{"a", "b", "c", "d"}) {
			t.Fatalf("ListFiles = %v (want MinTS, Key order)", got)
		}
		for _, tc := range []struct {
			name string
			flt  catalog.Filter
			want []string
		}{
			{"stream", catalog.Filter{Tenant: tenant.Default, Stream: "web"}, []string{"c"}},
			{"time overlap", catalog.Filter{Tenant: tenant.Default, From: t0.Add(6 * time.Minute), Through: t0.Add(20 * time.Minute)}, []string{"b", "c"}},
			{"inclusive bounds", catalog.Filter{Tenant: tenant.Default, Through: t0}, []string{"a", "b"}},
			{"label match", catalog.Filter{Tenant: tenant.Default, Match: map[string]string{"level": "error"}}, []string{"b", "c", "d"}},
			{"label match empty value", catalog.Filter{Tenant: tenant.Default, Match: map[string]string{"level": ""}}, []string{"c", "d"}},
			{"other tenant", catalog.Filter{Tenant: "acme"}, nil},
		} {
			if got := keys(t, c, tc.flt); !slices.Equal(got, tc.want) {
				t.Errorf("%s: ListFiles = %v, want %v", tc.name, got, tc.want)
			}
		}
		got, err := c.Streams(ctx, tenant.Default)
		if err != nil || !slices.Equal(got, []string{"api", "web"}) {
			t.Fatalf("Streams = %v, %v", got, err)
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		c := open(t)
		want := file("k", "api", 1, 2, map[string][]string{"level": {"info", "warn"}, "pod": {"p1"}})
		want.HasSidecar = true
		want.LabelsComplete = true
		want.MinTS = want.MinTS.Add(123 * time.Microsecond)
		if err := c.AddFiles(t.Context(), want); err != nil {
			t.Fatal(err)
		}
		for got, err := range c.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default}) {
			if err != nil {
				t.Fatal(err)
			}
			if got.Key != want.Key || got.Stream != want.Stream || !got.MinTS.Equal(want.MinTS) || !got.MaxTS.Equal(want.MaxTS) ||
				got.Rows != want.Rows || got.Bytes != want.Bytes || !got.HasSidecar || !got.LabelsComplete || got.CreatedAt.IsZero() || got.Deleted() ||
				!slices.Equal(got.LabelValues["level"], want.LabelValues["level"]) || !slices.Equal(got.LabelValues["pod"], want.LabelValues["pod"]) {
				t.Fatalf("round trip:\n got %+v\nwant %+v", got, want)
			}
		}
	})

	t.Run("SetSidecar", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		if err := c.AddFiles(ctx, complete(file("a", "api", 0, 1, map[string][]string{"level": {"info"}})), file("gone", "api", 0, 1, nil)); err != nil {
			t.Fatal(err)
		}
		if err := c.ReplaceFiles(ctx, tenant.Default, []string{"gone"}, nil); err != nil {
			t.Fatal(err)
		}
		for range 2 { // idempotent
			if err := c.SetSidecar(ctx, tenant.Default, "a"); err != nil {
				t.Fatalf("SetSidecar: %v", err)
			}
		}
		for _, k := range []string{"gone", "missing"} {
			if err := c.SetSidecar(ctx, tenant.Default, k); !errors.Is(err, catalog.ErrNotFound) {
				t.Errorf("SetSidecar(%s) = %v, want ErrNotFound", k, err)
			}
		}
		if err := c.SetSidecar(ctx, "acme", "a"); !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("SetSidecar in another tenant = %v, want ErrNotFound", err)
		}
		for got, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
			if err != nil {
				t.Fatal(err)
			}
			if !got.HasSidecar || !got.LabelsComplete || !slices.Equal(got.LabelValues["level"], []string{"info"}) {
				t.Fatalf("after SetSidecar: %+v", got)
			}
		}
	})

	t.Run("DuplicateIsAtomic", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		if err := c.AddFiles(ctx, file("a", "api", 0, 1, nil)); err != nil {
			t.Fatal(err)
		}
		err := c.AddFiles(ctx, file("new", "api", 0, 1, nil), file("a", "api", 0, 1, nil))
		if !errors.Is(err, catalog.ErrExists) {
			t.Fatalf("AddFiles duplicate: %v, want ErrExists", err)
		}
		if got := keys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a"}) {
			t.Fatalf("partial add: %v", got)
		}
	})

	t.Run("ReplaceAndPurge", func(t *testing.T) {
		c := open(t)
		ctx := t.Context()
		if err := c.AddFiles(ctx, file("s1", "api", 0, 1, nil), file("s2", "api", 1, 2, nil)); err != nil {
			t.Fatal(err)
		}
		if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1", "missing"}, nil); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("ReplaceFiles missing: %v, want ErrNotFound", err)
		}
		if got := keys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"s1", "s2"}) {
			t.Fatalf("failed replace changed state: %v", got)
		}
		if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1", "s2"}, []catalog.File{file("m", "api", 0, 2, nil)}); err != nil {
			t.Fatal(err)
		}
		if got := keys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"m"}) {
			t.Fatalf("after replace = %v", got)
		}
		if got := keys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"m", "s1", "s2"}) {
			t.Fatalf("with deleted = %v", got)
		}
		if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1"}, nil); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("replacing a deleted file: %v, want ErrNotFound", err)
		}
		purged, err := c.PurgeDeleted(ctx, tenant.Default, time.Now().Add(-time.Hour))
		if err != nil || len(purged) != 0 {
			t.Fatalf("early purge = %v, %v", purged, err)
		}
		purged, err = c.PurgeDeleted(ctx, tenant.Default, time.Now().Add(time.Hour))
		if err != nil || !slices.Equal(purged, []string{"s1", "s2"}) {
			t.Fatalf("purge = %v, %v", purged, err)
		}
		if got := keys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"m"}) {
			t.Fatalf("after purge = %v", got)
		}
	})

	RunPipelines(t, newCatalog)
}
