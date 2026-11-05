package plantest_test

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestMemSource(t *testing.T) {
	plantest.RunSource(t, func(t *testing.T, files []plantest.File) plan.Source { return plantest.Mem(files) })
}

func TestLoad(t *testing.T) {
	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewMemory()
	files := plantest.Dataset()
	plantest.Load(t, store, cat, files)

	var rows int64
	n := 0
	for f, err := range cat.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Stat(t.Context(), f.Key); err != nil {
			t.Fatalf("listed object missing: %v", err)
		}
		rows += f.Rows
		n++
	}
	var want int64
	for _, f := range files {
		if f.Tenant == tenant.Default {
			want += int64(len(f.Rows))
		}
	}
	if n != 7 || rows != want {
		t.Fatalf("catalog lists %d files with %d rows, want 7 files with %d rows", n, rows, want)
	}
}
