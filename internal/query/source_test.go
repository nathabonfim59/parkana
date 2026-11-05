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

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// defaultStreamLabels is the configured priority the tests run with, the
// same default as config.Load.
var defaultStreamLabels = []string{"service_name", "job"}

// fixtureResolver resolves object keys to their path in an objstore.FS
// directory: no copies and no remote reads, keys checked for tenant
// ownership, missing files surfacing fs.ErrNotExist.
type fixtureResolver struct{ dir string }

func (r fixtureResolver) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if !strings.HasPrefix(key, string(t)+"/") {
		return "", nil, fmt.Errorf("fixture resolver: key %q is not owned by tenant %q", key, t)
	}
	p := filepath.Join(r.dir, filepath.FromSlash(key))
	if _, err := os.Stat(p); err != nil {
		return "", nil, fmt.Errorf("fixture resolver: %s: %w", key, err)
	}
	return p, func() {}, nil
}

// recordCatalog records every ListFiles filter for pruning assertions,
// delegating to the wrapped catalog (SQLite in these tests).
type recordCatalog struct {
	catalog.Catalog
	mu      sync.Mutex
	filters []catalog.Filter
}

func (c *recordCatalog) ListFiles(ctx context.Context, f catalog.Filter) iter.Seq2[catalog.File, error] {
	c.mu.Lock()
	c.filters = append(c.filters, f)
	c.mu.Unlock()
	return c.Catalog.ListFiles(ctx, f)
}

func (c *recordCatalog) lastFilter() catalog.Filter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.filters) == 0 {
		return catalog.Filter{}
	}
	return c.filters[len(c.filters)-1]
}

// sourceEnv is a Source over an objstore.FS directory, a SQLite catalog and
// a fixture resolver, loaded with files the way the ingester stores them.
type sourceEnv struct {
	source *source
	cat    *recordCatalog
	dir    string
	files  []catalog.File
}

func openSourceEnv(t *testing.T, files []plantest.File, streamLabels []string) *sourceEnv {
	t.Helper()
	dir := t.TempDir()
	store, err := objstore.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	base, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { base.Close() })
	rc := &recordCatalog{Catalog: base}

	var listed []catalog.File
	for _, f := range files {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatalf("encode %s/%s: %v", f.Tenant, f.Stream, err)
		}
		key := objstore.DataKey(f.Tenant, f.Stream, st.MinTS, uuid.NewV7())
		if err := store.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		cf := catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}
		if err := base.AddFiles(t.Context(), cf); err != nil {
			t.Fatalf("catalog add %s: %v", key, err)
		}
		listed = append(listed, cf)
	}
	return &sourceEnv{source: newSource(openTestDB(t), rc, fixtureResolver{dir}, streamLabels), cat: rc, dir: dir, files: listed}
}

// path returns the local path of one listed object.
func (e *sourceEnv) path(key string) string {
	return filepath.Join(e.dir, filepath.FromSlash(key))
}

func collectRows(seq iter.Seq2[plan.Row, error]) ([]plan.Row, error) {
	var out []plan.Row
	for r, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, nil
}

func rowKey(r plan.Row) string {
	return fmt.Sprintf("%d|%s|%s|%q", r.TS.UnixMicro(), r.Labels, r.Meta, r.Line)
}

// assertSelectEqual runs sel through src and the MemSource reference and
// compares the results as multisets, checking Direction order the way the
// conformance suite does.
func assertSelectEqual(t *testing.T, src plan.Source, ref *plan.MemSource, sel plan.Selection) {
	t.Helper()
	got, err := collectRows(src.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want, err := collectRows(ref.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("reference Select: %v", err)
	}
	for i := 1; i < len(got); i++ {
		c := got[i-1].TS.Compare(got[i].TS)
		if sel.Direction == plan.Forward && c > 0 || sel.Direction == plan.Backward && c < 0 {
			t.Fatalf("row %d out of %s order: %s then %s", i, sel.Direction, got[i-1].TS, got[i].TS)
		}
	}
	for _, r := range got {
		if r.TS.Location() != time.UTC || r.TS.Nanosecond()%1000 != 0 {
			t.Fatalf("row TS %s is not UTC with µs precision", r.TS)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d\nfirst got:  %s\nfirst want: %s", len(got), len(want), firstRow(got), firstRow(want))
	}
	g := make([]string, len(got))
	for i, r := range got {
		g[i] = rowKey(r)
	}
	w := make([]string, len(want))
	for i, r := range want {
		w[i] = rowKey(r)
	}
	slices.Sort(g)
	slices.Sort(w)
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("row %d differs:\n got  %s\n want %s", i, g[i], w[i])
		}
	}
}

func firstRow(rs []plan.Row) string {
	if len(rs) == 0 {
		return "(none)"
	}
	return rowKey(rs[0])
}

// TestSourceConformance runs the complete frozen plantest suite — Select and
// Aggregate subtrees included — over the real Source with the fixture
// resolver and a SQLite catalog. The suite itself is never edited.
func TestSourceConformance(t *testing.T) {
	plantest.RunSource(t, func(t *testing.T, files []plantest.File) plan.Source {
		return openSourceEnv(t, files, defaultStreamLabels).source
	})
}

// TestSourceTenantSelect keeps a Select-side check of the frozen suite's
// Tenant subtree: other tenants' rows stay invisible, and unknown tenants
// see nothing at all.
func TestSourceTenantSelect(t *testing.T) {
	files := plantest.Dataset()
	env := openSourceEnv(t, files, defaultStreamLabels)
	ref := plantest.Mem(files)
	eq := plan.MustMatcher(plan.MatchEqual, "service_name", "api")
	for _, tc := range []tenant.ID{plantest.OtherTenant, "nobody"} {
		sel := plan.Selection{
			Tenant: tc, Matchers: []plan.Matcher{eq},
			From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(2 * time.Hour), Direction: plan.Forward,
		}
		assertSelectEqual(t, env.source, ref, sel)
	}
}

// TestSourceCatalogPruning pins the catalog filters Select derives: tenant
// and time overlap always; the stream only when the configured priority lets
// the matchers prove the dataset (a job equality alone proves nothing while
// service_name has priority). The listing filter itself never carries Match:
// label-equality pruning runs in the querier through catalog.Filter.Matches
// (see listObjects), so one catalog pass suffices and the pruned_labels
// counter records the method's exact rejections. Files with complete
// summaries can be dropped there without losing rows — see
// TestSelectMergedMapFallback and TestSourcePrunesByLabels. Every case also
// checks the rows against the MemSource reference: pruning must never change
// results.
func TestSourceCatalogPruning(t *testing.T) {
	files := plantest.Dataset()
	env := openSourceEnv(t, files, defaultStreamLabels)
	ref := plantest.Mem(files)
	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	re := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, n, v) }
	from, through := plantest.T0.Add(-time.Hour), plantest.T0.Add(2*time.Hour)

	for _, tc := range []struct {
		name     string
		matchers []plan.Matcher
		stream   string
	}{
		{name: "stream-equality", matchers: []plan.Matcher{eq("service_name", "api")},
			stream: "api"},
		{name: "stream-and-promoted", matchers: []plan.Matcher{eq("service_name", "api"), eq("level", "error")},
			stream: "api"},
		{name: "job-alone-undetermined", matchers: []plan.Matcher{eq("job", "batch")},
			stream: ""},
		{name: "empty-stream-label-falls-through", matchers: []plan.Matcher{eq("service_name", ""), eq("job", "batch")},
			stream: "batch"},
		{name: "regexp-is-not-proof", matchers: []plan.Matcher{re("service_name", "api|web")},
			stream: ""},
		{name: "stream-sanitized", matchers: []plan.Matcher{eq("service_name", "API")},
			stream: "api"},
		{name: "empty-equality-never-prunes", matchers: []plan.Matcher{eq("service_name", "api"), eq("level", "")},
			stream: "api"},
		{name: "unpromoted-label", matchers: []plan.Matcher{eq("env", "prod")},
			stream: ""},
		{name: "contradictory-equalities", matchers: []plan.Matcher{eq("service_name", "api"), eq("level", "info"), eq("level", "error")},
			stream: "api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sel := plan.Selection{Tenant: tenant.Default, Matchers: tc.matchers, From: from, Through: through, Direction: plan.Forward}
			assertSelectEqual(t, env.source, ref, sel)
			f := env.cat.lastFilter()
			if f.Tenant != tenant.Default || !f.From.Equal(from) || !f.Through.Equal(through) {
				t.Errorf("filter tenant/time = %s %s..%s, want %s %s..%s", f.Tenant, f.From, f.Through, tenant.Default, from, through)
			}
			if f.Stream != tc.stream {
				t.Errorf("filter stream = %q, want %q", f.Stream, tc.stream)
			}
			if f.Match != nil {
				t.Errorf("filter match = %v, want none (Match pruning runs in the querier, not the catalog filter)", f.Match)
			}
		})
	}
}

// TestSelectRetryBoundary proves the attempt boundary Select documents: by
// the first yielded row, DuckDB's blocking sort has read every input file.
// Deleting the object of a file whose rows sort later, after the first row,
// leaves the iteration complete and correct; an object missing before the
// attempt fails it with the object key and fs.ErrNotExist, before any row
// is yielded. B1d builds its exactly-one-retry-after-fresh-listing on this
// boundary.
func TestSelectRetryBoundary(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	apiLabels := func(level string) map[string]string {
		return map[string]string{"service_name": "api", "level": level}
	}
	oldF := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(plantest.T0, "old 1", apiLabels("info")),
		rw(plantest.T0.Add(time.Second), "old 2", apiLabels("warn")),
	}}
	// newF stores its level in the labels map: a second promotion group, so
	// the boundary holds across a UNION ALL, not just one read_parquet.
	newF := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{
		rw(plantest.T0.Add(100*time.Second), "new 1", apiLabels("info")),
		rw(plantest.T0.Add(101*time.Second), "new 2", apiLabels("error")),
	}}
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "api"),
		},
		From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(3 * time.Hour), Direction: plan.Forward,
	}

	t.Run("delete-later-file-mid-iteration", func(t *testing.T) {
		env := openSourceEnv(t, []plantest.File{oldF, newF}, defaultStreamLabels)
		ref := plantest.Mem([]plantest.File{oldF, newF})
		var got []plan.Row
		seen := false
		for r, err := range env.source.Select(t.Context(), sel) {
			if err != nil {
				t.Fatalf("iteration failed after the first row: %v", err)
			}
			got = append(got, r)
			if !seen {
				seen = true
				// The newest file's object disappears mid-iteration; its
				// rows sort after everything yielded so far.
				if err := os.Remove(env.path(env.files[1].Key)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(got) != 4 {
			t.Fatalf("got %d rows after mid-iteration deletion, want all 4", len(got))
		}
		want, err := collectRows(ref.Select(t.Context(), sel))
		if err != nil {
			t.Fatal(err)
		}
		if len(want) != len(got) {
			t.Fatalf("reference has %d rows, got %d", len(want), len(got))
		}
		for i := range got {
			if rowKey(got[i]) != rowKey(want[i]) {
				t.Fatalf("row %d = %s, want %s", i, rowKey(got[i]), rowKey(want[i]))
			}
		}
	})

	t.Run("missing-file-fails-before-first-row", func(t *testing.T) {
		env := openSourceEnv(t, []plantest.File{oldF, newF}, defaultStreamLabels)
		if err := os.Remove(env.path(env.files[1].Key)); err != nil {
			t.Fatal(err)
		}
		got, err := collectRows(env.source.Select(t.Context(), sel))
		if err == nil {
			t.Fatal("Select with a missing object succeeded")
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("error = %v, want fs.ErrNotExist", err)
		}
		if !strings.Contains(err.Error(), env.files[1].Key) {
			t.Errorf("error %v does not name the missing object key %s", err, env.files[1].Key)
		}
		if len(got) != 0 {
			t.Errorf("%d rows yielded before the failure, want 0", len(got))
		}
	})
}

// TestSelectNanosecondBounds checks nanosecond selection bounds against
// whole-microsecond stored timestamps on both sides of both boundaries,
// including pre-epoch times, always against the MemSource reference.
func TestSelectNanosecondBounds(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api", "env": "prod"})}
	}
	rows := []plan.Row{
		rw(time.UnixMicro(-2).UTC(), "pre two"),
		rw(time.UnixMicro(-1).UTC(), "pre one"),
		rw(time.UnixMicro(0).UTC(), "epoch"),
		rw(time.UnixMicro(1).UTC(), "post one"),
		rw(plantest.T0, "t0"),
		rw(plantest.T0.Add(60*time.Second), "boundary exact"),
		rw(plantest.T0.Add(60*time.Second+time.Microsecond), "boundary plus one"),
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"env"}, Rows: rows}
	env := openSourceEnv(t, []plantest.File{f}, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{f})
	b := plantest.T0.Add(60 * time.Second)
	nano := 500 * time.Nanosecond
	for _, tc := range []struct {
		name          string
		from, through time.Time
	}{
		{"between-boundary-rows", b.Add(nano), b.Add(time.Microsecond + nano)},
		{"around-first-boundary", b.Add(-nano), b.Add(nano)},
		{"from-exact-through-ns", b, b.Add(time.Microsecond + nano)},
		{"pre-epoch-window", time.UnixMicro(-1).Add(-nano), time.UnixMicro(0).Add(nano)},
		{"pre-epoch-exact", time.UnixMicro(-2), time.UnixMicro(1)},
	} {
		for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
			sel := plan.Selection{
				Tenant: tenant.Default,
				Matchers: []plan.Matcher{
					plan.MustMatcher(plan.MatchEqual, "service_name", "api"),
				},
				From: tc.from, Through: tc.through, Direction: dir,
			}
			t.Run(fmt.Sprintf("%s/%s", tc.name, dir), func(t *testing.T) {
				assertSelectEqual(t, env.source, ref, sel)
			})
		}
	}
}

// TestSelectCanceledMidIteration checks that a context canceled after the
// first row ends the iteration with context.Canceled, matching what the
// suite's Canceled case requires of a pre-canceled context.
func TestSelectCanceledMidIteration(t *testing.T) {
	rows := make([]plan.Row, 0, 5000)
	for i := range 5000 { // more rows than one driver fetch chunk
		rows = append(rows, plan.Row{
			TS: plantest.T0.Add(time.Duration(i) * time.Millisecond), Line: fmt.Sprintf("bulk %d", i),
			Labels: logs.FromMap(map[string]string{"service_name": "api"}),
		})
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: rows}
	env := openSourceEnv(t, []plantest.File{f}, defaultStreamLabels)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "api"),
		},
		From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
	}
	var got int
	var gotErr error
	for _, err := range env.source.Select(ctx, sel) {
		if err != nil {
			gotErr = err
			break
		}
		got++
		if got == 1 {
			cancel()
		}
	}
	if gotErr == nil {
		t.Fatal("iteration completed without an error after mid-iteration cancel")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", gotErr)
	}
	if got > len(rows) {
		t.Errorf("yielded %d rows, more than exist", got)
	}
}
