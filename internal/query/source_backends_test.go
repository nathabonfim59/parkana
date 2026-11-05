package query

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// backendKind is one real object-store backend combination.
type backendKind int

const (
	backendFS backendKind = iota
	backendS3
)

func (k backendKind) String() string {
	switch k {
	case backendFS:
		return "fs"
	case backendS3:
		return "s3"
	}
	return fmt.Sprintf("backendKind(%d)", int(k))
}

// backendEnv is a real backend combination for the conformance and retry
// tests: an object store, the real object cache as the Source's resolver, a
// SQLite catalog and DuckDB. No resolver fake stands in for the cache, and
// the Source runs the production pruning path: a real decoded sidecar cache
// and scan metrics on their own registry, so the needle fixtures of the
// conformance suite exercise sidecar pruning, not just full scans.
type backendEnv struct {
	kind     backendKind
	store    objstore.Store
	cat      *hookCatalog
	cache    *objectCache
	sidecars *sidecarCache
	scanReg  *prometheus.Registry
	res      *countingResolver
	src      *source
	db       *sql.DB
	dir      string // FS root (fs backend only): object key to local path
	files    []catalog.File
}

// newBackendEnv loads files the way the ingester stores them and returns a
// Source reading them through the real object cache, the real sidecar cache
// and a SQLite catalog, with scan metrics on env.scanReg.
func newBackendEnv(t *testing.T, kind backendKind, files []plantest.File) *backendEnv {
	t.Helper()
	db := openTestDB(t)
	base, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { base.Close() })
	cat := &hookCatalog{Catalog: base}

	var store objstore.Store
	var cache *objectCache
	dir := ""
	switch kind {
	case backendFS:
		dir = t.TempDir()
		fsStore, err := objstore.NewFS(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fsStore.Close() })
		store = fsStore
		cache, err = newObjectCache(cacheOptions{store: store, local: true})
		if err != nil {
			t.Fatal(err)
		}
	case backendS3:
		store = newFakeS3(t)
		cache, err = newObjectCache(cacheOptions{
			store:  store,
			dir:    t.TempDir(),
			budget: 1 << 30,
		})
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown backend %d", kind)
	}
	t.Cleanup(func() { cache.Close() })

	reg := prometheus.NewRegistry()
	metrics, err := newScanMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: store, budget: 64 << 20, register: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sidecars.Close(); err != nil {
			t.Errorf("sidecar cache close: %v", err)
		}
	})

	plantest.Load(t, store, cat, files)
	res := &countingResolver{fileResolver: cache}
	return &backendEnv{
		kind: kind, store: store, cat: cat, cache: cache, sidecars: sidecars,
		scanReg: reg, res: res, db: db,
		src: newSourceWithOptions(db, cat, res, defaultStreamLabels, sourceOptions{
			sidecars: sidecars,
			metrics:  metrics,
		}),
		dir:   dir,
		files: collectCatalogFiles(t, base),
	}
}

const (
	fakeS3Bucket    = "parkana-query-test"
	fakeS3AccessKey = "test-access-key"
	fakeS3SecretKey = "test-secret-key"
)

// newFakeS3Server starts an in-process S3 fake.
func newFakeS3Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	return srv
}

// newFakeS3 starts an in-process S3 fake with the test bucket and returns a
// real objstore.S3 for it.
func newFakeS3(t *testing.T) *objstore.S3 {
	t.Helper()
	srv := newFakeS3Server(t)
	endpoint := strings.TrimPrefix(srv.URL, "http://")
	admin, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(fakeS3AccessKey, fakeS3SecretKey, ""),
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.MakeBucket(t.Context(), fakeS3Bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
	s, err := objstore.NewS3(objstore.S3Options{
		Endpoint:  endpoint,
		Bucket:    fakeS3Bucket,
		Region:    "us-east-1",
		AccessKey: fakeS3AccessKey,
		SecretKey: fakeS3SecretKey,
		Insecure:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestSourceBackends runs the complete frozen conformance suite — Select and
// Aggregate subtrees included — over both real backends, loaded through
// plantest.Dataset/Load: FS in place and S3 through the real object cache,
// each with a SQLite catalog. The Source carries the real decoded sidecar
// cache, so the suite's needle fixtures exercise production index pruning and
// the label-equality pruning runs on the dataset's complete summaries; the
// equality with MemSource proves neither ever changes results.
// TestSourcePrunesBySidecar and TestSourcePrunesByLabels prove the pruning
// actually fires on this wiring.
func TestSourceBackends(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			plantest.RunSource(t, func(t *testing.T, files []plantest.File) plan.Source {
				return newBackendEnv(t, kind, files).src
			})
		})
	}
}

// TestSourceBackendMergedMapFallback regresses compaction against both real
// backends: storage.Merge keeps map-only rows with a NULL promoted column
// beside a column that the file's stats list, so a level=warn query must still
// find them through the labels-map fallback. The merged bytes and its actual
// stats are stored like a compactor's replacement, then read through the real
// cache. The catalog row carries no LabelsComplete, so this is the legacy
// pre-phase-3 shape: conservative, never pruned by label.
func TestSourceBackendMergedMapFallback(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	colFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(base, "column row", map[string]string{"service_name": "merged", "level": "info"}),
	}}
	mapFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Rows: []plan.Row{
		rw(base.Add(time.Second), "map row", map[string]string{"service_name": "merged", "level": "warn"}),
	}}

	var readers []*storage.Reader
	for _, f := range []plantest.File{colFile, mapFile} {
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		readers = append(readers, r)
	}
	var merged bytes.Buffer
	st, err := storage.Merge(t.Context(), &merged, readers, []string{"level"})
	if err != nil {
		t.Fatal(err)
	}

	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, nil)
			key := objstore.DataKey(tenant.Default, "merged", st.MinTS, uuid.NewV7())
			if err := env.store.Put(t.Context(), key, bytes.NewReader(merged.Bytes()), int64(merged.Len())); err != nil {
				t.Fatalf("put merged object: %v", err)
			}
			if err := env.cat.AddFiles(t.Context(), catalog.File{
				Tenant: tenant.Default, Stream: "merged", Key: key,
				MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(merged.Len()),
				LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
			}); err != nil {
				t.Fatalf("list merged file: %v", err)
			}

			ref := plantest.Mem([]plantest.File{colFile, mapFile})
			eq := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, name, value) }
			for _, tc := range []struct {
				name     string
				matchers []plan.Matcher
			}{
				{"map-fallback-value", []plan.Matcher{eq("level", "warn")}},
				{"column-value", []plan.Matcher{eq("level", "info")}},
				{"all-rows", []plan.Matcher{eq("service_name", "merged")}},
			} {
				sel := plan.Selection{
					Tenant: tenant.Default, Matchers: tc.matchers,
					From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
				}
				t.Run(tc.name, func(t *testing.T) { assertSelectEqual(t, env.src, ref, sel) })
			}
		})
	}
}

// TestSourceBackendMergedMapFallbackCompleteSummary is the decision-10/13
// regression on real backends with a complete summary: the merged file is
// cataloged with LabelsComplete, its summary lists both the column value and
// the map-fallback value, and a level=warn query must keep the file through
// label pruning and still find the map-only rows. The SQLite/backend twin of
// TestSelectMergedMapFallback: value queries must scan the file
// (pruned_labels 0), while a value absent from the complete summary
// (level=error matches no row) prunes it exactly as the summary proves.
func TestSourceBackendMergedMapFallbackCompleteSummary(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	colFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(base, "column row", map[string]string{"service_name": "merged", "level": "info"}),
	}}
	mapFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Rows: []plan.Row{
		rw(base.Add(time.Second), "map row", map[string]string{"service_name": "merged", "level": "warn"}),
	}}

	var readers []*storage.Reader
	for _, f := range []plantest.File{colFile, mapFile} {
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		readers = append(readers, r)
	}
	var merged bytes.Buffer
	st, err := storage.Merge(t.Context(), &merged, readers, []string{"level"})
	if err != nil {
		t.Fatal(err)
	}
	if got := st.LabelValues["level"]; !slices.Equal(got, []string{"info", "warn"}) {
		t.Fatalf("merged LabelValues[level] = %v, want the complete [info warn]", got)
	}
	if !st.LabelsComplete {
		t.Fatal("merged LabelsComplete = false, want true")
	}

	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, nil)
			key := objstore.DataKey(tenant.Default, "merged", st.MinTS, uuid.NewV7())
			if err := env.store.Put(t.Context(), key, bytes.NewReader(merged.Bytes()), int64(merged.Len())); err != nil {
				t.Fatalf("put merged object: %v", err)
			}
			cf := catalog.File{
				Tenant: tenant.Default, Stream: "merged", Key: key,
				MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(merged.Len()),
				LabelValues: st.LabelValues, LabelsComplete: st.LabelsComplete,
				CreatedAt: time.Now().Truncate(time.Microsecond),
			}
			if err := env.cat.AddFiles(t.Context(), cf); err != nil {
				t.Fatalf("list merged file: %v", err)
			}

			ref := plantest.Mem([]plantest.File{colFile, mapFile})
			eq := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, name, value) }
			for _, tc := range []struct {
				name       string
				matchers   []plan.Matcher
				wantPruned float64
			}{
				{"map-fallback-value", []plan.Matcher{eq("level", "warn")}, 0},
				{"column-value", []plan.Matcher{eq("level", "info")}, 0},
				// level=error matches no row and the complete summary says so:
				// pruning the file is exactly the safe case.
				{"absent-value", []plan.Matcher{eq("level", "error")}, 1},
				{"all-rows", []plan.Matcher{eq("service_name", "merged")}, 0},
			} {
				sel := plan.Selection{
					Tenant: tenant.Default, Matchers: tc.matchers,
					From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
				}
				t.Run(tc.name, func(t *testing.T) {
					before := scanCountsNow(t, env)
					acquiresBefore, _ := env.res.counts()
					assertSelectEqual(t, env.src, ref, sel)
					d := scanCountsNow(t, env).delta(before)
					if d.prunedLabels != tc.wantPruned {
						t.Fatalf("pruned_labels delta = %v, want %v", d.prunedLabels, tc.wantPruned)
					}
					acquiresAfter, _ := env.res.counts()
					wantAcquires := 1 - int(tc.wantPruned)
					if got := acquiresAfter - acquiresBefore; got != wantAcquires {
						t.Fatalf("acquisitions delta = %d, want %d", got, wantAcquires)
					}
				})
			}
		})
	}
}
