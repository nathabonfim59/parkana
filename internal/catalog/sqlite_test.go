package catalog_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// testStart is a fixed base time for deterministic file bounds.
var testStart = time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)

func newTestCatalog(t *testing.T) *catalog.SQLite {
	t.Helper()
	c, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func testFile(key, stream string, minOff, maxOff time.Duration) catalog.File {
	return catalog.File{
		Tenant: tenant.Default,
		Stream: stream,
		Key:    key,
		MinTS:  testStart.Add(minOff),
		MaxTS:  testStart.Add(maxOff),
		Rows:   2,
		Bytes:  64,
	}
}

func collectKeys(c catalog.Catalog, ctx context.Context, flt catalog.Filter) ([]string, error) {
	var keys []string
	for file, err := range c.ListFiles(ctx, flt) {
		if err != nil {
			return nil, err
		}
		keys = append(keys, file.Key)
	}
	return keys, nil
}

func listKeys(t *testing.T, c catalog.Catalog, flt catalog.Filter) []string {
	t.Helper()
	keys, err := collectKeys(c, t.Context(), flt)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	return keys
}

func TestSQLite(t *testing.T) {
	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		return newTestCatalog(t)
	})
}

func TestSQLiteCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "catalog.db")
	c, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	defer c.Close()

	if err := c.AddFiles(t.Context(), testFile("k", "api", 0, time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.IsDir() || info.Size() == 0 {
		t.Fatalf("database file = %+v", info)
	}
}

func TestSQLiteLiteralMemoryFilename(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	// ":memory:" is a literal file name here, not SQLite's in-memory database;
	// the read pool must see the same file the write pool migrated.
	c, err := catalog.NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	if err := c.AddFiles(t.Context(), testFile("k", "api", 0, time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"k"}) {
		t.Fatalf("ListFiles = %v, want [k]", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ":memory:")); err != nil {
		t.Fatalf("literal :memory: file was not created: %v", err)
	}
}

func TestSQLiteFilenameWithDSNCharacters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cat?alog#1.db")
	c, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.AddFiles(t.Context(), testFile("k", "api", 0, time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"k"}) {
		t.Fatalf("ListFiles = %v, want [k]", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file with ? and # was not created: %v", err)
	}
}

func TestSQLiteEmptyPath(t *testing.T) {
	if _, err := catalog.NewSQLite(""); err == nil {
		t.Fatal("NewSQLite(\"\") succeeded, want error")
	}
}

func TestSQLiteReopenPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	c1, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}

	want := testFile("a", "api", 0, 10*time.Minute)
	want.LabelValues = map[string][]string{"level": {"info", "warn"}, "pod": {"p1"}}
	want.HasSidecar = true
	want.Rows = 7
	want.Bytes = 4096
	want.CreatedAt = testStart.Add(time.Hour)
	if err := c1.AddFiles(t.Context(), want, testFile("b", "api", time.Minute, 2*time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	// b is replaced, so its deletion must survive the reopen too.
	if err := c1.ReplaceFiles(t.Context(), tenant.Default, []string{"b"},
		[]catalog.File{testFile("c", "web", 2*time.Minute, 3*time.Minute)}); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	var deletedAt time.Time
	for f := range c1.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}) {
		if f.Key == "b" {
			deletedAt = f.DeletedAt
		}
	}
	if deletedAt.IsZero() {
		t.Fatal("b has no DeletedAt before close")
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite reopen: %v", err)
	}
	t.Cleanup(func() { c2.Close() })

	if got := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("live after reopen = %v, want [a c]", got)
	}
	all := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true})
	if !slices.Equal(all, []string{"a", "b", "c"}) {
		t.Fatalf("all after reopen = %v, want [a b c]", all)
	}

	var got catalog.File
	for f := range c2.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default}) {
		if f.Key == "a" {
			got = f
		}
	}
	// b is deleted, so its persistence assertion needs IncludeDeleted.
	foundB := false
	for f := range c2.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}) {
		if f.Key == "b" {
			foundB = true
			if !f.Deleted() || !f.DeletedAt.Equal(deletedAt) {
				t.Fatalf("b.DeletedAt after reopen = %v, want %v", f.DeletedAt, deletedAt)
			}
		}
	}
	if !foundB {
		t.Fatal("b missing after reopen")
	}
	if got.Tenant != want.Tenant || got.Stream != want.Stream ||
		!got.MinTS.Equal(want.MinTS) || !got.MaxTS.Equal(want.MaxTS) ||
		got.Rows != want.Rows || got.Bytes != want.Bytes || !got.HasSidecar ||
		!got.CreatedAt.Equal(want.CreatedAt) || got.Deleted() ||
		!slices.Equal(got.LabelValues["level"], want.LabelValues["level"]) ||
		!slices.Equal(got.LabelValues["pod"], want.LabelValues["pod"]) {
		t.Fatalf("round trip after reopen:\n got %+v\nwant %+v", got, want)
	}
	if got, err := c2.Streams(t.Context(), tenant.Default); err != nil || !slices.Equal(got, []string{"api", "web"}) {
		t.Fatalf("Streams after reopen = %v, %v", got, err)
	}
}

func TestSQLiteMigrationsReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	for i := range 2 {
		c, err := catalog.NewSQLite(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if err := c.AddFiles(t.Context(), testFile(fmt.Sprintf("k%d", i), "api", 0, time.Minute)); err != nil {
			t.Fatalf("AddFiles %d: %v", i, err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}

	rows, err := db.Query(`SELECT "version" FROM schema_migrations ORDER BY "version"`)
	if err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("schema_migrations: %v", err)
	}
	if !slices.Equal(versions, []int{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("applied migrations = %v, want [1 2 3 4 5 6]", versions)
	}

	var files int
	if err := db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&files); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if files != 2 {
		t.Fatalf("files after two reopens = %d, want 2", files)
	}
}

func TestSQLiteTenantIsolation(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()

	local := testFile("same", "api", 0, time.Minute)
	remote := testFile("same", "web", 0, time.Minute)
	remote.Tenant = "acme"
	if err := c.AddFiles(ctx, local, remote); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"same"}) {
		t.Fatalf("default tenant = %v", got)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: "acme"}); !slices.Equal(got, []string{"same"}) {
		t.Fatalf("acme tenant = %v", got)
	}
	if got, err := c.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"api"}) {
		t.Fatalf("default streams = %v, %v", got, err)
	}
	if got, err := c.Streams(ctx, "acme"); err != nil || !slices.Equal(got, []string{"web"}) {
		t.Fatalf("acme streams = %v, %v", got, err)
	}

	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"same"}, nil); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); len(got) != 0 {
		t.Fatalf("default live after replace = %v", got)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: "acme"}); !slices.Equal(got, []string{"same"}) {
		t.Fatalf("acme changed by default replace: %v", got)
	}

	purged, err := c.PurgeDeleted(ctx, tenant.Default, time.Now().Add(time.Hour))
	if err != nil || !slices.Equal(purged, []string{"same"}) {
		t.Fatalf("purge default = %v, %v", purged, err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: "acme"}); !slices.Equal(got, []string{"same"}) {
		t.Fatalf("acme changed by default purge: %v", got)
	}

	bad := testFile("bad", "api", 0, time.Minute)
	bad.Tenant = "not valid"
	if err := c.AddFiles(ctx, bad); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("invalid tenant: %v, want tenant.ErrInvalid", err)
	}
}

func TestSQLiteAddFilesAtomic(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	if err := c.AddFiles(ctx, testFile("a", "api", 0, time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	live := func() []string { return listKeys(t, c, catalog.Filter{Tenant: tenant.Default}) }

	if err := c.AddFiles(ctx, testFile("new", "api", time.Minute, 2*time.Minute), testFile("a", "api", 0, time.Minute)); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("duplicate existing: %v, want ErrExists", err)
	}
	if got := live(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("partial add of existing duplicate: %v", got)
	}

	if err := c.AddFiles(ctx, testFile("x", "api", 0, time.Minute), testFile("x", "api", 0, time.Minute)); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("duplicate in batch: %v, want ErrExists", err)
	}
	if got := live(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("partial add of in-batch duplicate: %v", got)
	}

	bad := testFile("y", "api", 0, time.Minute)
	bad.Tenant = ""
	if err := c.AddFiles(ctx, testFile("z", "api", 0, time.Minute), bad); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("invalid tenant in batch: %v, want tenant.ErrInvalid", err)
	}
	if got := live(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("partial add of invalid tenant: %v", got)
	}

	if err := c.AddFiles(ctx); err != nil {
		t.Fatalf("empty AddFiles: %v", err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, nil, nil); err != nil {
		t.Fatalf("empty ReplaceFiles: %v", err)
	}
}

func TestSQLiteReplaceFilesAtomic(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	if err := c.AddFiles(ctx,
		testFile("s1", "api", 0, time.Minute),
		testFile("s2", "api", time.Minute, 2*time.Minute),
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	live := func() []string { return listKeys(t, c, catalog.Filter{Tenant: tenant.Default}) }

	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1", "missing"}, nil); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("missing remove: %v, want ErrNotFound", err)
	}
	if got := live(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("state after missing remove: %v", got)
	}

	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1"}, []catalog.File{testFile("s2", "api", 0, time.Minute)}); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("add existing: %v, want ErrExists", err)
	}
	if got := live(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("state after conflicting add: %v", got)
	}

	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1"}, []catalog.File{testFile("s1", "api", 0, time.Minute)}); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("add removed key: %v, want ErrExists", err)
	}
	if got := live(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("state after re-adding removed key: %v", got)
	}

	dupes := []catalog.File{testFile("n", "api", 0, time.Minute), testFile("n", "api", 0, time.Minute)}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1"}, dupes); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("duplicate add: %v, want ErrExists", err)
	}
	if got := live(); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Fatalf("state after duplicate add: %v", got)
	}

	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1", "s1"}, []catalog.File{testFile("n", "api", 0, 2*time.Minute)}); err != nil {
		t.Fatalf("ReplaceFiles success with duplicate remove: %v", err)
	}
	if got := live(); !slices.Equal(got, []string{"n", "s2"}) {
		t.Fatalf("after replace = %v, want [n s2]", got)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"s1"}, nil); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("replace deleted: %v, want ErrNotFound", err)
	}
	if err := c.AddFiles(ctx, testFile("s1", "api", 0, time.Minute)); !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("add deleted key: %v, want ErrExists", err)
	}
}

func TestSQLiteConcurrentUse(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	const (
		writers  = 8
		perWrite = 5
		readers  = 8
	)
	var wg sync.WaitGroup
	writeErrs := make([]error, writers)
	for i := range writers {
		wg.Go(func() {
			for j := range perWrite {
				f := testFile(fmt.Sprintf("w%d-%d", i, j), "api", time.Duration(j)*time.Millisecond, time.Duration(j+1)*time.Millisecond)
				if err := c.AddFiles(ctx, f); err != nil {
					writeErrs[i] = err
					return
				}
			}
		})
	}
	readErrs := make([]error, readers)
	for i := range readers {
		wg.Go(func() {
			for range 5 {
				keys, err := collectKeys(c, ctx, catalog.Filter{Tenant: tenant.Default})
				if err != nil {
					readErrs[i] = err
					return
				}
				_ = keys
				if _, err := c.Streams(ctx, tenant.Default); err != nil {
					readErrs[i] = err
					return
				}
			}
		})
	}
	wg.Wait()
	for i, err := range writeErrs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	for i, err := range readErrs {
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
	}

	keys := listKeys(t, c, catalog.Filter{Tenant: tenant.Default})
	if len(keys) != writers*perWrite {
		t.Fatalf("listed %d files, want %d", len(keys), writers*perWrite)
	}
}

func TestSQLiteCancellation(t *testing.T) {
	c := newTestCatalog(t)
	if err := c.AddFiles(t.Context(), testFile("a", "api", 0, time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.AddFiles(ctx, testFile("b", "api", 0, time.Minute)); !errors.Is(err, context.Canceled) {
		t.Fatalf("AddFiles canceled: %v", err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"a"}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReplaceFiles canceled: %v", err)
	}
	if _, err := c.Streams(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
		t.Fatalf("Streams canceled: %v", err)
	}
	if _, err := c.PurgeDeleted(ctx, tenant.Default, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("PurgeDeleted canceled: %v", err)
	}
	var listErr error
	for _, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
		listErr = err
	}
	if !errors.Is(listErr, context.Canceled) {
		t.Fatalf("ListFiles canceled: %v", listErr)
	}

	// Cancellation mid-iteration stops the remaining rows and surfaces the
	// context error.
	if err := c.AddFiles(t.Context(),
		testFile("c", "api", time.Minute, 2*time.Minute),
		testFile("d", "api", 2*time.Minute, 3*time.Minute),
	); err != nil {
		t.Fatalf("seed more: %v", err)
	}
	midCtx, midCancel := context.WithCancel(t.Context())
	defer midCancel()
	yielded := 0
	var midErr error
	for _, err := range c.ListFiles(midCtx, catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			midErr = err
			break
		}
		yielded++
		if yielded == 1 {
			midCancel()
		}
	}
	if !errors.Is(midErr, context.Canceled) {
		t.Fatalf("mid-iteration error = %v, want context.Canceled", midErr)
	}
	if yielded != 1 {
		t.Fatalf("mid-iteration yielded %d files, want 1", yielded)
	}
}

func TestSQLiteEarlyBreakReentrant(t *testing.T) {
	c := newTestCatalog(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := c.AddFiles(ctx,
		testFile("a", "api", 0, time.Minute),
		testFile("b", "api", time.Minute, 2*time.Minute),
		testFile("c", "api", 2*time.Minute, 3*time.Minute),
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	found := 0
	for f, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		found++
		// While this iterator holds an open read cursor, other catalog
		// methods must not deadlock.
		if _, err := c.Streams(ctx, tenant.Default); err != nil {
			t.Fatalf("reentrant Streams: %v", err)
		}
		if err := c.AddFiles(ctx, testFile("during", f.Stream, 3*time.Minute, 4*time.Minute)); err != nil {
			t.Fatalf("reentrant AddFiles: %v", err)
		}
		nested := 0
		for range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
			nested++
		}
		if nested == 0 {
			t.Fatal("nested ListFiles yielded nothing")
		}
		break
	}
	if found != 1 {
		t.Fatalf("yielded %d files before break, want 1", found)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a", "b", "c", "during"}) {
		t.Fatalf("after early break = %v", got)
	}
}

func TestSQLiteTimestampPrecision(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	f := testFile("k", "api", 0, 0)
	f.MinTS = testStart.Add(1500 * time.Nanosecond)
	f.MaxTS = testStart.Add(2500 * time.Nanosecond)
	f.CreatedAt = testStart.Add(3500 * time.Nanosecond)
	if err := c.AddFiles(ctx, f); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default})
	if len(got) != 1 {
		t.Fatalf("got %d files", len(got))
	}
	if want := testStart.Add(time.Microsecond); !got[0].MinTS.Equal(want) {
		t.Fatalf("MinTS = %v, want %v (floor to µs)", got[0].MinTS, want)
	}
	if want := testStart.Add(2 * time.Microsecond); !got[0].MaxTS.Equal(want) {
		t.Fatalf("MaxTS = %v, want %v (floor to µs)", got[0].MaxTS, want)
	}
	if want := testStart.Add(3 * time.Microsecond); !got[0].CreatedAt.Equal(want) {
		t.Fatalf("CreatedAt = %v, want %v (floor to µs)", got[0].CreatedAt, want)
	}
}

func TestSQLiteFilterTimeBoundaries(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	// m covers the single microsecond instant testStart; n covers the next one.
	m := testFile("m", "api", 0, 0)
	n := testFile("n", "api", time.Microsecond, time.Microsecond)
	if err := c.AddFiles(ctx, m, n); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	for _, tc := range []struct {
		name string
		flt  catalog.Filter
		want []string
	}{
		{"from exact", catalog.Filter{Tenant: tenant.Default, From: testStart}, []string{"m", "n"}},
		{"from sub-µs after m", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(time.Nanosecond)}, []string{"n"}},
		{"from sub-µs below n", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(time.Microsecond - time.Nanosecond)}, []string{"n"}},
		{"through exact", catalog.Filter{Tenant: tenant.Default, Through: testStart}, []string{"m"}},
		{"through sub-µs below m", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(-time.Nanosecond)}, nil},
		{"through sub-µs above m", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(999 * time.Nanosecond)}, []string{"m"}},
		{"through sub-µs below n", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(time.Microsecond - time.Nanosecond)}, []string{"m"}},
		{"through exact n", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(time.Microsecond)}, []string{"m", "n"}},
		{"window", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(500 * time.Nanosecond), Through: testStart.Add(time.Microsecond + 500*time.Nanosecond)}, []string{"n"}},
	} {
		if got := listKeys(t, c, tc.flt); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSQLitePreEpochTimestamps(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	// -500ns rounds toward negative infinity to -1µs.
	pre := time.Date(1969, 12, 31, 23, 59, 59, 999_999_500, time.UTC)
	stored := time.Date(1969, 12, 31, 23, 59, 59, 999_999_000, time.UTC)
	f := testFile("pre", "api", 0, 0)
	f.MinTS = pre
	f.MaxTS = pre
	if err := c.AddFiles(ctx, f); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default})
	if len(got) != 1 || !got[0].MinTS.Equal(stored) || !got[0].MaxTS.Equal(stored) {
		t.Fatalf("pre-epoch round trip = %+v, want %v", got, stored)
	}

	for _, tc := range []struct {
		name string
		flt  catalog.Filter
		want []string
	}{
		{"from before stored", catalog.Filter{Tenant: tenant.Default, From: stored.Add(-time.Nanosecond)}, []string{"pre"}},
		{"from stored", catalog.Filter{Tenant: tenant.Default, From: stored}, []string{"pre"}},
		{"from original", catalog.Filter{Tenant: tenant.Default, From: pre}, nil},
		{"through stored", catalog.Filter{Tenant: tenant.Default, Through: stored}, []string{"pre"}},
		{"through original", catalog.Filter{Tenant: tenant.Default, Through: pre}, []string{"pre"}},
		{"through before stored", catalog.Filter{Tenant: tenant.Default, Through: stored.Add(-time.Nanosecond)}, nil},
		{"through epoch", catalog.Filter{Tenant: tenant.Default, Through: time.Unix(0, 0)}, []string{"pre"}},
	} {
		if keys := listKeys(t, c, tc.flt); !slices.Equal(keys, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, keys, tc.want)
		}
	}
}

func TestSQLiteOrdering(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	files := []catalog.File{
		testFile("c", "api", 2*time.Minute, 3*time.Minute),
		testFile("b", "api", 0, time.Minute),
		testFile("a", "api", 0, time.Minute),
		testFile("d", "api", time.Minute, 2*time.Minute),
	}
	if err := c.AddFiles(ctx, files...); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a", "b", "d", "c"}) {
		t.Fatalf("order = %v, want MinTS then Key", got)
	}
}

func TestSQLitePurgeBoundaries(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	if err := c.AddFiles(ctx,
		testFile("gone", "api", 0, time.Minute),
		testFile("live", "api", time.Minute, 2*time.Minute),
	); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"gone"}, nil); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	deletedAt := time.Time{}
	for _, f := range listFiles(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}) {
		if f.Key == "gone" {
			deletedAt = f.DeletedAt
		}
	}
	if deletedAt.IsZero() {
		t.Fatal("gone has no DeletedAt")
	}

	for _, tc := range []struct {
		name   string
		cutoff time.Time
		want   []string
	}{
		{"zero cutoff", time.Time{}, nil},
		{"well before", deletedAt.Add(-time.Hour), nil},
		{"cutoff equal (strict)", deletedAt, nil},
		{"cutoff just below", deletedAt.Add(-time.Nanosecond), nil},
		{"cutoff just after", deletedAt.Add(time.Nanosecond), []string{"gone"}},
	} {
		purged, err := c.PurgeDeleted(ctx, tenant.Default, tc.cutoff)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !slices.Equal(purged, tc.want) {
			t.Fatalf("%s: purged %v, want %v", tc.name, purged, tc.want)
		}
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("live after purge = %v", got)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("all after purge = %v", got)
	}
}

func TestSQLiteAddFilesPreservesDeletedAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	c1, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}

	live := testFile("live", "api", 0, time.Minute)
	deleted := testFile("deleted", "api", time.Minute, 2*time.Minute)
	deleted.DeletedAt = testStart
	epoch := testFile("epoch", "api", 2*time.Minute, 3*time.Minute)
	epoch.DeletedAt = time.Unix(0, 0)
	if err := c1.AddFiles(t.Context(), live, deleted, epoch); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	// A zero DeletedAt stays live; nonzero values, including the Unix epoch,
	// are preserved as given.
	if got := listKeys(t, c1, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("live files = %v, want [live]", got)
	}
	all := listFiles(t, c1, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true})
	if len(all) != 3 {
		t.Fatalf("all files = %+v", all)
	}
	wantDeleted := map[string]time.Time{
		"live":    {},
		"deleted": testStart,
		"epoch":   time.Unix(0, 0).UTC(),
	}
	for _, f := range all {
		want, ok := wantDeleted[f.Key]
		if !ok {
			t.Fatalf("unexpected file %q", f.Key)
		}
		if !f.DeletedAt.Equal(want) || f.Deleted() != !want.IsZero() {
			t.Fatalf("%s DeletedAt = %v (deleted %v), want %v", f.Key, f.DeletedAt, f.Deleted(), want)
		}
	}
	if got, err := c1.Streams(t.Context(), tenant.Default); err != nil || !slices.Equal(got, []string{"api"}) {
		t.Fatalf("Streams = %v, %v", got, err)
	}

	// Both the deleted-at and epoch values must survive a reopen.
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c2, err := catalog.NewSQLite(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { c2.Close() })
	all = listFiles(t, c2, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true})
	if len(all) != 3 {
		t.Fatalf("all files after reopen = %+v", all)
	}
	for _, f := range all {
		if want := wantDeleted[f.Key]; !f.DeletedAt.Equal(want) {
			t.Fatalf("%s DeletedAt after reopen = %v, want %v", f.Key, f.DeletedAt, want)
		}
	}

	// Purge is strict: epoch is not before epoch, and testStart is not before
	// itself. Epoch is purged first because every later cutoff is also after it.
	purged, err := c2.PurgeDeleted(t.Context(), tenant.Default, time.Unix(0, 0))
	if err != nil || len(purged) != 0 {
		t.Fatalf("purge at epoch = %v, %v", purged, err)
	}
	purged, err = c2.PurgeDeleted(t.Context(), tenant.Default, time.Unix(0, 0).Add(-time.Nanosecond))
	if err != nil || len(purged) != 0 {
		t.Fatalf("purge before epoch = %v, %v", purged, err)
	}
	purged, err = c2.PurgeDeleted(t.Context(), tenant.Default, time.Unix(0, 0).Add(time.Microsecond))
	if err != nil || !slices.Equal(purged, []string{"epoch"}) {
		t.Fatalf("purge after epoch = %v, %v", purged, err)
	}
	purged, err = c2.PurgeDeleted(t.Context(), tenant.Default, testStart)
	if err != nil || len(purged) != 0 {
		t.Fatalf("purge at deleted-at = %v, %v", purged, err)
	}
	purged, err = c2.PurgeDeleted(t.Context(), tenant.Default, testStart.Add(-time.Nanosecond))
	if err != nil || len(purged) != 0 {
		t.Fatalf("purge before deleted-at = %v, %v", purged, err)
	}
	purged, err = c2.PurgeDeleted(t.Context(), tenant.Default, testStart.Add(time.Nanosecond))
	if err != nil || !slices.Equal(purged, []string{"deleted"}) {
		t.Fatalf("purge after deleted-at = %v, %v", purged, err)
	}
	if got := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("live after purges = %v", got)
	}
}

func TestSQLiteLabelMatch(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	a := testFile("a", "api", 0, time.Minute)
	a.LabelValues = map[string][]string{"level": {"error", "info"}, "pod": {"p1"}}
	b := testFile("b", "api", time.Minute, 2*time.Minute)
	b.LabelValues = map[string][]string{"level": {"info"}}
	cc := testFile("c", "api", 2*time.Minute, 3*time.Minute)
	d := testFile("d", "api", 3*time.Minute, 4*time.Minute)
	d.LabelValues = map[string][]string{"level": {"warn"}}
	for _, f := range []*catalog.File{&a, &b, &cc, &d} {
		f.LabelsComplete = true
	}
	if err := c.AddFiles(ctx, a, b, cc, d); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	for _, tc := range []struct {
		name  string
		match map[string]string
		want  []string
	}{
		{"value", map[string]string{"level": "error"}, []string{"a", "c"}},
		{"shared value", map[string]string{"level": "info"}, []string{"a", "b", "c"}},
		{"missing label kept", map[string]string{"pod": "p1"}, []string{"a", "b", "c", "d"}},
		{"two labels", map[string]string{"level": "info", "pod": "p2"}, []string{"b", "c"}},
		{"absent value", map[string]string{"level": "debug"}, []string{"c"}},
	} {
		flt := catalog.Filter{Tenant: tenant.Default, Match: tc.match}
		if got := listKeys(t, c, flt); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSQLiteStreams(t *testing.T) {
	c := newTestCatalog(t)
	ctx := t.Context()
	if err := c.AddFiles(ctx,
		testFile("a", "api", 0, time.Minute),
		testFile("b", "api", time.Minute, 2*time.Minute),
		testFile("c", "web", 2*time.Minute, 3*time.Minute),
		testFile("d", "billing", 3*time.Minute, 4*time.Minute),
	); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"d"}, nil); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	if got, err := c.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"api", "web"}) {
		t.Fatalf("Streams = %v, %v", got, err)
	}
	if got, err := c.Streams(ctx, "acme"); err != nil || len(got) != 0 {
		t.Fatalf("Streams empty tenant = %v, %v", got, err)
	}
}

func TestSQLiteCloseIdempotent(t *testing.T) {
	c, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func listFiles(t *testing.T, c catalog.Catalog, flt catalog.Filter) []catalog.File {
	t.Helper()
	var files []catalog.File
	for f, err := range c.ListFiles(t.Context(), flt) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		files = append(files, f)
	}
	return files
}
