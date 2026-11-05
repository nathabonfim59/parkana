package pipeline_test

import (
	"cmp"
	"context"
	"log/slog"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/query"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// equivalenceDeps builds the querier's shared dependencies over a fresh FS
// object store and SQLite catalog, the way the role is built in production.
func equivalenceDeps(t *testing.T) app.Deps {
	t.Helper()
	dir := t.TempDir()
	store, err := objstore.NewFS(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.NewSQLite(filepath.Join(dir, "catalog.db"))
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cat.Close(); err != nil {
			t.Errorf("catalog close: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	logger := slog.New(slog.DiscardHandler)
	reg := prometheus.NewRegistry()
	return app.Deps{
		Config: config.Config{
			DataDir:      dir,
			Storage:      config.Storage{Backend: "fs"},
			StreamLabels: readStreamLabels,
			Querier: config.Querier{
				CacheSize:        config.GiB,
				SidecarCacheSize: 64 * config.MiB,
			},
		},
		Logger:  logger,
		Store:   store,
		Catalog: cat,
		HTTP:    server.New(server.Options{Logger: logger, Registry: reg}),
		Metrics: reg,
	}
}

// equivalenceSnapshot compiles the read pipelines the equivalence test uses.
func equivalenceSnapshot(t *testing.T) pipeline.Snapshot {
	t.Helper()
	return readSnapshot(t,
		readCompile(t, "a-rewrite", "*", pipeline.Step{Type: pipeline.StepRedact, Pattern: "timeout", Replace: new("rewritten-needle")}),
		readCompile(t, "b-rename", "*", pipeline.Step{Type: pipeline.StepRename, From: "trace_id", To: "trace"}),
		readCompile(t, "c-web-drop", "web", pipeline.Step{Type: pipeline.StepDropIf, Expr: `line contains "ERROR"`}),
	)
}

// equivalenceRules is the same pipeline written by hand for the independent
// reference. It never calls the pipeline package's evaluator.
func equivalenceRules() []refRule {
	timeout := regexp.MustCompile("timeout")
	rules := []refRule{
		{
			name: "a-rewrite", selector: "*",
			apply: func(r plan.Row) (plan.Row, bool) {
				r.Line = timeout.ReplaceAllString(r.Line, "rewritten-needle")
				return r, true
			},
		},
		{
			name: "b-rename", selector: "*",
			apply: func(r plan.Row) (plan.Row, bool) {
				v, ok := r.Meta.Get("trace_id")
				if !ok {
					return r, true
				}
				m := r.Meta.Map()
				delete(m, "trace_id")
				m["trace"] = v
				r.Meta = logs.FromMap(m)
				return r, true
			},
		},
		{
			name: "c-web-drop", selector: "web",
			apply: func(r plan.Row) (plan.Row, bool) {
				return r, !strings.Contains(r.Line, "ERROR")
			},
		},
	}
	slices.SortFunc(rules, func(a, b refRule) int { return cmp.Compare(a.name, b.name) })
	return rules
}

// equivalenceDuckDB stores the fixture dataset and returns the raw DuckDB
// source plus the wrapped source query.NewWith installed through WrapSource.
// The service runs for the test's lifetime and shuts down on cleanup.
func equivalenceDuckDB(t *testing.T, deps app.Deps, files []plantest.File, provider pipeline.SnapshotProvider) (plan.Source, plan.Source) {
	t.Helper()
	plantest.Load(t, deps.Store, deps.Catalog, files)

	var raw, wrapped plan.Source
	var wrapErr error
	svc, err := query.NewWith(deps, logql.New(config.LogQL{MaxEntries: 5000, MaxSeries: 500}), query.Options{
		WrapSource: func(src plan.Source) plan.Source {
			raw = src
			wrapped, wrapErr = pipeline.NewReadSource(src, provider, readStreamLabels)
			return wrapped
		},
	})
	if err != nil {
		t.Fatalf("query.NewWith: %v", err)
	}
	if wrapErr != nil {
		t.Fatalf("NewReadSource: %v", wrapErr)
	}
	if raw == nil || wrapped == nil {
		t.Fatal("WrapSource did not run")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("query service Run: %v", err)
		}
	})
	return raw, wrapped
}

// readCompareRowOrder compares rows position by position, preserving the
// actual result order. plan.Source leaves only the order among rows with equal
// timestamps unspecified, so a run of equal-TS rows is compared as a multiset;
// every distinct-TS position must match exactly.
func readCompareRowOrder(t *testing.T, name string, got, want []plan.Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s row counts: got %d, want %d", name, len(got), len(want))
	}
	for i := 0; i < len(got); {
		ts := got[i].TS
		if !want[i].TS.Equal(ts) {
			t.Fatalf("%s row %d TS = %s, want %s", name, i, got[i].TS, want[i].TS)
		}
		j := i + 1
		for ; j < len(got) && got[j].TS.Equal(ts); j++ {
			if !want[j].TS.Equal(ts) {
				t.Fatalf("%s equal-TS run at %d ends at different positions", name, i)
			}
		}
		gotKeys := make([]string, 0, j-i)
		for _, r := range got[i:j] {
			gotKeys = append(gotKeys, readRowKey(r))
		}
		wantKeys := make([]string, 0, j-i)
		for _, r := range want[i:j] {
			wantKeys = append(wantKeys, readRowKey(r))
		}
		slices.Sort(gotKeys)
		slices.Sort(wantKeys)
		if !slices.Equal(gotKeys, wantKeys) {
			t.Fatalf("%s equal-TS run at %d:\n got %v\nwant %v", name, i, gotKeys, wantKeys)
		}
		i = j
	}
}

// readRowKeys renders rows in order.
func readRowKeys(rows []plan.Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = readRowKey(r)
	}
	return out
}

// TestReadSourceMemDuckDBEquivalence stores the plantest fixture the way the
// ingester does and checks the wrapped Mem and DuckDB sources against each
// other and against the independent eager reference.
func TestReadSourceMemDuckDBEquivalence(t *testing.T) {
	files := plantest.Dataset()
	deps := equivalenceDeps(t)
	snap := equivalenceSnapshot(t)
	provider := &readProvider{fallback: snap}

	// Raw MemSource for the reference: unwrapped input, so a wrapper bug
	// cannot define the expected answer.
	memRaw := plantest.Mem(files)
	memWrapped, err := pipeline.NewReadSource(memRaw, provider, readStreamLabels)
	if err != nil {
		t.Fatal(err)
	}
	_, dbWrapped := equivalenceDuckDB(t, deps, files, provider)
	rules := equivalenceRules()

	wide := func(ms ...plan.Matcher) plan.Selection {
		return plan.Selection{
			Tenant:    tenant.Default,
			Matchers:  ms,
			From:      plantest.T0.Add(-time.Hour),
			Through:   plantest.T0.Add(2 * time.Hour),
			Direction: plan.Forward,
		}
	}
	assertAll := func(t *testing.T, name string, sel plan.Selection) {
		t.Helper()
		ref := refSelect(t, t.Context(), memRaw, rules, readStreamLabels, sel)
		mem := readCollectRows(t, memWrapped.Select(t.Context(), sel))
		db := readCollectRows(t, dbWrapped.Select(t.Context(), sel))
		readCompareRowOrder(t, name+"/mem vs reference", mem, ref)
		readCompareRowOrder(t, name+"/duckdb vs reference", db, ref)
		readCompareRowOrder(t, name+"/mem vs duckdb", mem, db)
	}
	assertAgg := func(t *testing.T, name string, sel plan.Selection, agg plan.BucketAgg) {
		t.Helper()
		ref := refAggregate(t, t.Context(), memRaw, rules, readStreamLabels, sel, agg)
		mem := readCollectBuckets(t, memWrapped.Aggregate(t.Context(), sel, agg))
		db := readCollectBuckets(t, dbWrapped.Aggregate(t.Context(), sel, agg))
		if !slices.Equal(readBucketKeys(mem), readBucketKeys(ref)) {
			t.Fatalf("%s/mem vs reference buckets:\n got %v\nwant %v", name, readBucketKeys(mem), readBucketKeys(ref))
		}
		if !slices.Equal(readBucketKeys(db), readBucketKeys(ref)) {
			t.Fatalf("%s/duckdb vs reference buckets:\n got %v\nwant %v", name, readBucketKeys(db), readBucketKeys(ref))
		}
		if !slices.Equal(readBucketKeys(mem), readBucketKeys(db)) {
			t.Fatalf("%s/mem vs duckdb buckets:\n mem %v\n db %v", name, readBucketKeys(mem), readBucketKeys(db))
		}
	}

	t.Run("rewritten-in needle survives sidecar pruning", func(t *testing.T) {
		sel := wide(readEq("service_name", "api"))
		sel.LineFilters = []plan.LineFilter{readContains("rewritten-needle")}
		got := readCollectRows(t, dbWrapped.Select(t.Context(), sel))
		if len(got) == 0 {
			t.Fatal("no rows: the raw scan pruned the sidecar before the rewrite")
		}
		for _, r := range got {
			if !strings.Contains(r.Line, "rewritten-needle") {
				t.Fatalf("row did not survive the rewrite: %q", r.Line)
			}
		}
		assertAll(t, "rewritten-in", sel)
	})

	t.Run("rewritten-away needle disappears", func(t *testing.T) {
		sel := wide(readEq("service_name", "api"))
		sel.LineFilters = []plan.LineFilter{readContains("timeout")}
		if got := readCollectRows(t, dbWrapped.Select(t.Context(), sel)); len(got) != 0 {
			t.Fatalf("rows = %d, want none after the needle was rewritten away", len(got))
		}
		assertAll(t, "rewritten-away", sel)
	})

	t.Run("all filter operators", func(t *testing.T) {
		for _, f := range []plan.LineFilter{
			plan.MustLineFilter(plan.LineContains, "error"),
			plan.MustLineFilter(plan.LineNotContains, "error"),
			plan.MustLineFilter(plan.LineRegexp, `(?i)error`),
			plan.MustLineFilter(plan.LineNotRegexp, `(?i)error`),
		} {
			sel := wide(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web"))
			sel.LineFilters = []plan.LineFilter{f}
			assertAll(t, "filter "+f.String(), sel)
		}
	})

	t.Run("transformed drop", func(t *testing.T) {
		sel := wide(readEq("service_name", "web"))
		sel.LineFilters = []plan.LineFilter{readContains("ERROR")}
		if got := readCollectRows(t, dbWrapped.Select(t.Context(), sel)); len(got) != 0 {
			t.Fatalf("rows = %d, want none after drop_if removed every ERROR line", len(got))
		}
		assertAll(t, "transformed-drop", sel)
	})

	t.Run("mixed tenants", func(t *testing.T) {
		sel := wide(readEq("service_name", "api"))
		sel.Tenant = plantest.OtherTenant
		assertAll(t, "other-tenant", sel)
	})

	t.Run("direction and unique timestamps", func(t *testing.T) {
		// caseLabels rows are one second apart, so their order is stable.
		sel := wide(readEq("Level", "High"))
		for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
			sel.Direction = dir
			sel.Limit = 3
			ref := refSelect(t, t.Context(), memRaw, rules, readStreamLabels, sel)
			mem := readCollectRows(t, memWrapped.Select(t.Context(), sel))
			db := readCollectRows(t, dbWrapped.Select(t.Context(), sel))
			memKeys, dbKeys, refKeys := readRowKeys(mem), readRowKeys(db), readRowKeys(ref)
			if !slices.Equal(memKeys, refKeys) || !slices.Equal(dbKeys, refKeys) {
				t.Fatalf("%s limit rows:\n  mem %v\n   db %v\n  ref %v", dir, memKeys, dbKeys, refKeys)
			}
		}
	})

	t.Run("inclusive from exclusive through", func(t *testing.T) {
		sel := wide(readEq("service_name", "api"))
		// The fixture also has ordinary API traffic at this timestamp.
		// Isolate its boundary exact / boundary plus one pair.
		sel.LineFilters = []plan.LineFilter{readContains("boundary")}
		sel.From = plantest.T0.Add(60 * time.Second)
		sel.Through = plantest.T0.Add(60*time.Second + time.Microsecond)
		got := readCollectRows(t, dbWrapped.Select(t.Context(), sel))
		if len(got) != 1 || got[0].Line != "boundary exact" {
			t.Fatalf("rows = %v, want exactly the boundary row", got)
		}
		assertAll(t, "boundaries", sel)
	})

	t.Run("aggregate include meta and negative buckets", func(t *testing.T) {
		sel := wide(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web|batch"))
		for _, agg := range []plan.BucketAgg{
			{Width: time.Minute, Origin: plantest.T0},
			{Width: 7 * time.Second, Origin: plantest.T0.Add(3 * time.Hour)},
			{Width: time.Minute, Origin: plantest.T0, IncludeMeta: true},
			{Width: 7 * time.Second, Origin: plantest.T0.Add(-time.Minute), IncludeMeta: true},
		} {
			assertAgg(t, "aggregate", sel, agg)
		}
	})

	t.Run("duplicate raw rows count once", func(t *testing.T) {
		// The dups file re-delivers identical rows; the raw source removes
		// them once and the wrapper must not deduplicate rewritten rows.
		sel := wide(readEq("service_name", "api"))
		sel.From = plantest.T0
		sel.Through = plantest.T0.Add(5 * time.Second)
		agg := plan.BucketAgg{Width: time.Hour, Origin: plantest.T0, IncludeMeta: true}
		assertAgg(t, "duplicates", sel, agg)
	})
}
