package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// hookCatalog counts catalog listings and can run a hook after the first
// listing has been materialized but before its files are consumed, so the
// first attempt sees exactly the pre-hook catalog snapshot and later attempts
// see the mutation. It is the compaction/retry race injector.
type hookCatalog struct {
	catalog.Catalog

	mu    sync.Mutex
	calls int
	hook  func()
}

// setHook arms the first-listing hook. It must be called before the first
// query.
func (c *hookCatalog) setHook(hook func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hook = hook
}

// listings returns how many ListFiles calls were made.
func (c *hookCatalog) listings() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *hookCatalog) ListFiles(ctx context.Context, f catalog.Filter) iter.Seq2[catalog.File, error] {
	c.mu.Lock()
	c.calls++
	first := c.calls == 1
	hook := c.hook
	c.mu.Unlock()
	if !first || hook == nil {
		return c.Catalog.ListFiles(ctx, f)
	}
	var files []catalog.File
	for file, err := range c.Catalog.ListFiles(ctx, f) {
		if err != nil {
			return func(yield func(catalog.File, error) bool) { yield(catalog.File{}, err) }
		}
		files = append(files, file)
	}
	hook()
	return func(yield func(catalog.File, error) bool) {
		for _, file := range files {
			if !yield(file, nil) {
				return
			}
		}
	}
}

// countingResolver wraps the real object cache: it counts successful
// acquisitions and their releases, tracks per-key leases, and can run a hook
// after the first successful acquisition (the acquire-to-open race injector).
type countingResolver struct {
	fileResolver

	mu       sync.Mutex
	acquires int
	releases int
	live     map[string]int
	hook     func()
}

func (r *countingResolver) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	path, release, err := r.fileResolver.Acquire(ctx, t, key)
	if err != nil {
		return "", nil, err
	}
	r.mu.Lock()
	r.acquires++
	if r.live == nil {
		r.live = make(map[string]int)
	}
	r.live[key]++
	first := r.acquires == 1
	hook := r.hook
	r.mu.Unlock()
	if first && hook != nil {
		hook()
	}
	return path, func() {
		r.mu.Lock()
		r.releases++
		r.live[key]--
		r.mu.Unlock()
		release()
	}, nil
}

// counts returns the successful acquisition and release totals.
func (r *countingResolver) counts() (acquires, releases int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acquires, r.releases
}

// acquired reports whether key was ever acquired successfully. live keeps a
// zero entry for every key seen, so this is the acquisition history the
// pruning tests check: a pruned file must never appear here.
func (r *countingResolver) acquired(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.live[key]
	return ok
}

// liveLeases returns how many leases are still outstanding.
func (r *countingResolver) liveLeases() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, v := range r.live {
		n += v
	}
	return n
}

// resolverFunc adapts a function to fileResolver for failure-injection tests.
type resolverFunc func(ctx context.Context, t tenant.ID, key string) (string, func(), error)

func (f resolverFunc) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	return f(ctx, t, key)
}

// retryFiles is the small two-file fixture the retry tests mutate: an old file
// whose object disappears and a newer one that survives.
func retryFiles() []plantest.File {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s", "env": "prod"})}
	}
	return []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"env"}, Rows: []plan.Row{
			rw(plantest.T0, "old 1"),
			rw(plantest.T0.Add(time.Second), "old 2"),
		}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
			rw(plantest.T0.Add(100*time.Second), "new 1"),
			rw(plantest.T0.Add(101*time.Second), "new 2"),
		}},
	}
}

// replacementFile holds the rows a compactor writes in place of a merged file.
func replacementFile() plantest.File {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s", "env": "prod"})}
	}
	return plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"env"}, Rows: []plan.Row{
		rw(plantest.T0.Add(200*time.Second), "replacement 1"),
		rw(plantest.T0.Add(201*time.Second), "replacement 2"),
	}}
}

func retrySelection() plan.Selection {
	return plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "s"),
		},
		From:      plantest.T0.Add(-time.Hour),
		Through:   plantest.T0.Add(time.Hour),
		Direction: plan.Forward,
	}
}

// collectCatalogFiles lists tenant.Default's catalog files.
func collectCatalogFiles(t *testing.T, cat catalog.Catalog) []catalog.File {
	t.Helper()
	var files []catalog.File
	for file, err := range cat.ListFiles(t.Context(), catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	return files
}

// storeFixture writes f's Parquet bytes under a fresh object key and returns
// that key and the catalog row describing it, without listing it. Callers add
// the row through plantest.Load or the catalog's ReplaceFiles.
func storeFixture(t *testing.T, store objstore.Store, f plantest.File) (string, catalog.File) {
	t.Helper()
	data, st, err := plantest.Encode(f)
	if err != nil {
		t.Fatalf("encode %s/%s: %v", f.Tenant, f.Stream, err)
	}
	key := objstore.DataKey(f.Tenant, f.Stream, st.MinTS, uuid.NewV7())
	if err := store.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return key, catalog.File{
		Tenant: f.Tenant, Stream: f.Stream, Key: key,
		MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
		LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
	}
}

// TestSourceRetryReplacement checks the compaction race: the catalog lists a
// file whose object is already replaced between listing and acquisition. The
// retry must take a fresh listing and yield the replacement rows exactly once,
// never the old ones and never duplicates.
func TestSourceRetryReplacement(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			files := retryFiles()
			env := newBackendEnv(t, kind, files)
			if len(env.files) != len(files) {
				t.Fatalf("listed %d files, want %d", len(env.files), len(files))
			}
			oldKey := env.files[0].Key
			_, replCatalog := storeFixture(t, env.store, replacementFile())
			env.cat.setHook(func() {
				if err := env.store.Delete(t.Context(), oldKey); err != nil {
					t.Fatalf("delete replaced object: %v", err)
				}
				if err := env.cat.Catalog.ReplaceFiles(t.Context(), tenant.Default, []string{oldKey}, []catalog.File{replCatalog}); err != nil {
					t.Fatalf("replace catalog row: %v", err)
				}
			})

			got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
			if err != nil {
				t.Fatalf("Select after replacement: %v", err)
			}
			if n := env.cat.listings(); n != 2 {
				t.Errorf("catalog listings = %d, want 2 (one stale listing plus one fresh)", n)
			}
			wantRows, err := collectRows(plantest.Mem([]plantest.File{files[1], replacementFile()}).Select(t.Context(), retrySelection()))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(wantRows) {
				t.Fatalf("got %d rows, want %d (old rows must not survive)", len(got), len(wantRows))
			}
			gotKeys := make([]string, len(got))
			for i, r := range got {
				gotKeys[i] = rowKey(r)
			}
			wantKeys := make([]string, len(wantRows))
			for i, r := range wantRows {
				wantKeys[i] = rowKey(r)
			}
			slices.Sort(gotKeys)
			slices.Sort(wantKeys)
			if !slices.Equal(gotKeys, wantKeys) {
				t.Errorf("rows after replacement differ:\n got  %v\n want %v", gotKeys, wantKeys)
			}
			if leaks := env.res.liveLeases(); leaks != 0 {
				t.Errorf("%d leases still held after a successful retry", leaks)
			}
		})
	}
}

// TestSourceRetryPersistentMissing checks that an object which never comes
// back is listed exactly twice and reported, with no rows and no held lease.
func TestSourceRetryPersistentMissing(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, retryFiles())
			missingKey := env.files[0].Key
			env.cat.setHook(func() {
				if err := env.store.Delete(t.Context(), missingKey); err != nil {
					t.Fatalf("delete object: %v", err)
				}
			})

			got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
			if err == nil {
				t.Fatal("Select succeeded with a persistently missing object")
			}
			if len(got) != 0 {
				t.Errorf("%d rows yielded before the failure, want 0", len(got))
			}
			if !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("error = %v, want fs.ErrNotExist", err)
			}
			if !strings.Contains(err.Error(), missingKey) {
				t.Errorf("error %v does not name the missing object key %s", err, missingKey)
			}
			if n := env.cat.listings(); n != 2 {
				t.Errorf("catalog listings = %d, want exactly 2 (one retry)", n)
			}
			if leaks := env.res.liveLeases(); leaks != 0 {
				t.Errorf("%d leases still held after the failed retry", leaks)
			}
		})
	}
}

// TestSourceMissingFileDuckDBError pins the DuckDB error text produced when an
// acquired FS object disappears before DuckDB opens it, and proves the retry
// classifier recognizes it. The first attempt's acquisition succeeds; only the
// DuckDB open fails, so the classified message is the driver's own.
func TestSourceMissingFileDuckDBError(t *testing.T) {
	env := newBackendEnv(t, backendFS, retryFiles())
	gone := env.files[0].Key
	env.res.hook = func() {
		if err := os.Remove(filepath.Join(env.dir, filepath.FromSlash(gone))); err != nil {
			t.Fatalf("remove acquired object: %v", err)
		}
	}

	sel := retrySelection()
	_, err := env.src.attemptRows(t.Context(), sel, func(groups []fileGroup) selectQuery {
		return selectSQL(sel, groups)
	})
	if err == nil {
		t.Fatal("attemptRows succeeded after the object disappeared before open")
	}
	t.Logf("DuckDB missing-file error: %v", err)
	if !errors.Is(err, errObjectDisappeared) {
		t.Errorf("attemptRows error not classified as a disappearance: %v", err)
	}
	if !hasMissingNeedle(err.Error()) {
		t.Errorf("error %v carries no known missing-file phrase (%q or %q)", err, duckDBMissingPatternNeedle, duckDBMissingErrnoNeedle)
	}
}

// TestSourceRetryMissingDuringOpen is the end-to-end acquire-to-open race: the
// object exists when leased, disappears before DuckDB reads it, and stays
// gone. The first attempt must fail after acquisition (classified from
// DuckDB's error), the retry must list afresh and fail at acquisition, and the
// reported error must remain fs.ErrNotExist with the object key.
func TestSourceRetryMissingDuringOpen(t *testing.T) {
	env := newBackendEnv(t, backendFS, retryFiles())
	gone := env.files[0].Key
	env.res.hook = func() {
		if err := os.Remove(filepath.Join(env.dir, filepath.FromSlash(gone))); err != nil {
			t.Fatalf("remove acquired object: %v", err)
		}
	}

	got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
	if err == nil {
		t.Fatal("Select succeeded after a file vanished before open")
	}
	if len(got) != 0 {
		t.Errorf("%d rows yielded before the failure, want 0", len(got))
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error = %v, want fs.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), gone) {
		t.Errorf("error %v does not name the missing object key %s", err, gone)
	}
	if n := env.cat.listings(); n != 2 {
		t.Errorf("catalog listings = %d, want exactly 2 (the retry proves the DuckDB error was classified)", n)
	}
	if acquires, releases := env.res.counts(); acquires != releases {
		t.Errorf("acquired %d leases but released %d", acquires, releases)
	}
}

// TestSourceRetryCorruptFileNoRetry checks that a corrupt Parquet object fails
// on the first attempt: only missing objects are retried, never bad data.
func TestSourceRetryCorruptFileNoRetry(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, retryFiles())
			corrupt := []byte("this is not a parquet file")
			if err := env.store.Put(t.Context(), env.files[0].Key, bytes.NewReader(corrupt), int64(len(corrupt))); err != nil {
				t.Fatalf("corrupt object: %v", err)
			}

			got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
			if err == nil {
				t.Fatal("Select succeeded on a corrupt Parquet object")
			}
			if len(got) != 0 {
				t.Errorf("%d rows yielded from the corrupt attempt, want 0", len(got))
			}
			if errors.Is(err, fs.ErrNotExist) {
				t.Errorf("corrupt-file error misclassified as missing: %v", err)
			}
			if n := env.cat.listings(); n != 1 {
				t.Errorf("catalog listings = %d, want exactly 1 (corrupt data must not retry)", n)
			}
			if leaks := env.res.liveLeases(); leaks != 0 {
				t.Errorf("%d leases still held after the corrupt-file failure", leaks)
			}
		})
	}
}

// TestSourceRetryCanceledNoRetry checks that a context canceled during the
// first attempt ends iteration as context.Canceled without a second listing.
func TestSourceRetryCanceledNoRetry(t *testing.T) {
	env := newBackendEnv(t, backendFS, retryFiles())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.cat.setHook(cancel)

	got, err := collectRows(env.src.Select(ctx, retrySelection()))
	if err == nil {
		t.Fatal("Select on a canceled context succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if len(got) != 0 {
		t.Errorf("%d rows yielded after cancellation, want 0", len(got))
	}
	if n := env.cat.listings(); n != 1 {
		t.Errorf("catalog listings = %d, want exactly 1 (cancellation must not retry)", n)
	}
}

// TestSourceRetryEarlyBreakReleasesLeases checks that stopping iteration after
// the first row closes the rows and returns every lease, on both backends, and
// leaves no cache references or in-flight downloads behind.
func TestSourceRetryEarlyBreakReleasesLeases(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, retryFiles())
			seen := 0
			for _, err := range env.src.Select(t.Context(), retrySelection()) {
				if err != nil {
					t.Fatalf("Select: %v", err)
				}
				seen++
				break
			}
			if seen != 1 {
				t.Fatalf("yielded %d rows before the break, want 1", seen)
			}
			acquires, releases := env.res.counts()
			if acquires == 0 {
				t.Fatal("no object was acquired; the lease assertion is vacuous")
			}
			if acquires != releases {
				t.Errorf("acquired %d leases but released %d after the early break", acquires, releases)
			}
			if leaks := env.res.liveLeases(); leaks != 0 {
				t.Errorf("%d leases still held after the early break", leaks)
			}
			env.cache.mu.Lock()
			refs := 0
			for _, e := range env.cache.entries {
				refs += e.refs
			}
			inflight := len(env.cache.inflight)
			env.cache.mu.Unlock()
			if refs != 0 {
				t.Errorf("cache holds %d references after the early break", refs)
			}
			if inflight != 0 {
				t.Errorf("%d cache downloads still in flight after the early break", inflight)
			}
		})
	}
}

// TestSourceRetryRejectsJoinedMissingFailure regresses the classifier's most
// dangerous temptation: an errors.Join carrying fs.ErrNotExist plus a genuine
// failure is not a disappearance, so it must not retry, and both failures must
// survive in the reported error.
func TestSourceRetryRejectsJoinedMissingFailure(t *testing.T) {
	boom := errors.New("disk on fire")
	res := resolverFunc(func(context.Context, tenant.ID, string) (string, func(), error) {
		return "", nil, errors.Join(fs.ErrNotExist, boom)
	})
	base := catalog.NewMemory()
	if err := base.AddFiles(t.Context(), catalog.File{
		Tenant: tenant.Default, Stream: "s",
		Key:   "default/logs/s/00000000-0000-0000-0000-000000000000.parquet",
		MinTS: plantest.T0, MaxTS: plantest.T0.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	cat := &hookCatalog{Catalog: base}
	src := newSource(nil, cat, res, defaultStreamLabels) // acquisition fails before DuckDB is touched

	_, err := collectRows(src.Select(t.Context(), retrySelection()))
	if err == nil {
		t.Fatal("Select succeeded with a joined missing+failure acquire error")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error %v lost the genuine failure %v", err, boom)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error %v lost fs.ErrNotExist", err)
	}
	if n := cat.listings(); n != 1 {
		t.Errorf("catalog listings = %d, want exactly 1 (joined failures must not retry)", n)
	}
}

// TestDisappearedObjectClassifier pins the classifier's narrow rules directly.
func TestDisappearedObjectClassifier(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "present.parquet")
	if err := os.WriteFile(present, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "absent.parquet")
	files := []catalog.File{{Key: "default/logs/s/a.parquet"}, {Key: "default/logs/s/b.parquet"}}
	paths := []string{present, absent}
	ctx := t.Context()
	parserMissing := func(p string) error {
		return &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: fmt.Sprintf("IO Error: No files found that match the pattern %q", p)}
	}
	boom := errors.New("boom")

	for _, tc := range []struct {
		name    string
		ctx     context.Context
		err     error
		wantOK  bool
		wantKey string
	}{
		{name: "single-acquire-missing", ctx: ctx, err: fmt.Errorf("query: acquire %s: %w", files[1].Key, fs.ErrNotExist), wantOK: true, wantKey: files[1].Key},
		{name: "single-duckdb-missing", ctx: ctx, err: parserMissing(absent), wantOK: true, wantKey: files[1].Key},
		{name: "join-single-acquire-missing", ctx: ctx, err: errors.Join(fmt.Errorf("query: acquire %s: %w", files[1].Key, fs.ErrNotExist), nil), wantOK: true, wantKey: files[1].Key},
		{name: "join-single-duckdb-missing", ctx: ctx, err: errors.Join(parserMissing(absent), nil), wantOK: true, wantKey: files[1].Key},
		{name: "joined-missing-and-boom", ctx: ctx, err: errors.Join(fs.ErrNotExist, boom)},
		{name: "joined-duckdb-text-and-boom", ctx: ctx, err: errors.Join(parserMissing(absent), boom)},
		{name: "nested-single-join-plus-boom", ctx: ctx, err: errors.Join(errors.Join(fs.ErrNotExist, nil), boom)},
		{name: "missing-text-but-path-present", ctx: ctx, err: parserMissing(present)},
		{name: "corrupt-parquet", ctx: ctx, err: &duckdb.Error{Type: duckdb.ErrorTypeInvalid, Msg: `Invalid Input Error: No magic bytes found at the end of file "` + absent + `"`}},
		{name: "generic-io", ctx: ctx, err: &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: Cannot open file " + absent + ": Permission denied"}},
		{name: "canceled-context", ctx: canceledContext(t), err: fmt.Errorf("query: acquire: %w", fs.ErrNotExist)},
		{name: "wrapped-canceled-error", ctx: ctx, err: fmt.Errorf("query: acquire: %w", context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := disappearedObject(tc.ctx, tc.err, files, paths)
			if ok != tc.wantOK || key != tc.wantKey {
				t.Errorf("disappearedObject = (%q, %v), want (%q, %v)", key, ok, tc.wantKey, tc.wantOK)
			}
		})
	}

	// Injected resolvers may hand DuckDB relative paths; the classifier must
	// match and stat them exactly as acquired, never absolutize or skip them.
	t.Run("relative-acquired-path", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		relPresentAbs := filepath.Join(dir, "present.parquet")
		if err := os.WriteFile(relPresentAbs, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		relAbsentAbs := filepath.Join(dir, "absent.parquet")
		relPresent, err := filepath.Rel(cwd, relPresentAbs)
		if err != nil {
			t.Fatal(err)
		}
		relAbsent, err := filepath.Rel(cwd, relAbsentAbs)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.IsAbs(relPresent) || filepath.IsAbs(relAbsent) {
			t.Fatalf("test paths are not relative: %q, %q", relPresent, relAbsent)
		}
		relPaths := []string{relPresent, relAbsent}
		if key, ok := disappearedObject(ctx, parserMissing(relAbsent), files, relPaths); !ok || key != files[1].Key {
			t.Errorf("relative missing path = (%q, %v), want (%q, true)", key, ok, files[1].Key)
		}
		if _, ok := disappearedObject(ctx, parserMissing(relPresent), files, relPaths); ok {
			t.Error("relative present path classified as missing")
		}
	})
}

// failingCatalog yields a fixed ListFiles error.
type failingCatalog struct {
	catalog.Catalog // nil: only ListFiles is exercised

	err   error
	calls int
}

func (c *failingCatalog) ListFiles(context.Context, catalog.Filter) iter.Seq2[catalog.File, error] {
	c.calls++
	return func(yield func(catalog.File, error) bool) {
		yield(catalog.File{}, c.err)
	}
}

// TestSourceRetryNoRetryOnCatalogError proves a missing-object error from the
// catalog listing is not a disappearance: it must fail immediately, without
// acquiring anything or listing again.
func TestSourceRetryNoRetryOnCatalogError(t *testing.T) {
	catErr := fmt.Errorf("catalog: list files: %w", fs.ErrNotExist)
	cat := &failingCatalog{err: catErr}
	acquired := false
	res := resolverFunc(func(context.Context, tenant.ID, string) (string, func(), error) {
		acquired = true
		return "", nil, errors.New("acquisition must not run after a catalog failure")
	})
	src := newSource(nil, cat, res, defaultStreamLabels) // listing fails before DuckDB is touched

	_, err := collectRows(src.Select(t.Context(), retrySelection()))
	if err == nil {
		t.Fatal("Select succeeded with a failing catalog")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error = %v, want fs.ErrNotExist preserved", err)
	}
	if cat.calls != 1 {
		t.Errorf("catalog listings = %d, want exactly 1 (list failures must not retry)", cat.calls)
	}
	if acquired {
		t.Error("resolver acquired an object after the catalog listing failed")
	}
}

// canceledContext returns an already-canceled context.
func canceledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}
