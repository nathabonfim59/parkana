package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// scanCounts is a snapshot of the scan metrics on one registry.
type scanCounts struct {
	scanned, prunedIndex, prunedLabels, bytes float64
}

// scanCountsNow reads the scan metrics off env's registry.
func scanCountsNow(t *testing.T, env *backendEnv) scanCounts {
	t.Helper()
	return scanCounts{
		scanned:      metricValue(t, env.scanReg, "parkana_query_files_total", map[string]string{"result": "scanned"}),
		prunedIndex:  metricValue(t, env.scanReg, "parkana_query_files_total", map[string]string{"result": "pruned_index"}),
		prunedLabels: metricValue(t, env.scanReg, "parkana_query_files_total", map[string]string{"result": "pruned_labels"}),
		bytes:        metricFloat(t, env.scanReg, "parkana_query_bytes_scanned_total"),
	}
}

// delta returns the increase from older to c.
func (c scanCounts) delta(older scanCounts) scanCounts {
	return scanCounts{
		scanned:      c.scanned - older.scanned,
		prunedIndex:  c.prunedIndex - older.prunedIndex,
		prunedLabels: c.prunedLabels - older.prunedLabels,
		bytes:        c.bytes - older.bytes,
	}
}

// sidecarMisses returns how many sidecar cache lookups missed so far.
func sidecarMisses(t *testing.T, env *backendEnv) float64 {
	t.Helper()
	return metricValue(t, env.scanReg, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "miss"})
}

// storeIndexFixture encodes f, stores its Parquet object and — when
// withSidecar — a search sidecar built from its lines, and returns the catalog
// row describing it without listing it. The caller adds the row through
// AddFiles or ReplaceFiles, so tests control LabelsComplete (complete) and
// HasSidecar independently.
func storeIndexFixture(t *testing.T, store objstore.Store, f plantest.File, complete, withSidecar bool) catalog.File {
	t.Helper()
	data, st, err := plantest.Encode(f)
	if err != nil {
		t.Fatalf("encode %s/%s: %v", f.Tenant, f.Stream, err)
	}
	key := objstore.DataKey(f.Tenant, f.Stream, st.MinTS, uuid.NewV7())
	if err := store.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	if withSidecar {
		b, err := index.NewBuilder(index.DefaultParams)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range f.Rows {
			b.Add(r.Line)
		}
		idx, err := b.Encode()
		if err != nil {
			t.Fatalf("sidecar %s: %v", key, err)
		}
		if err := store.Put(t.Context(), objstore.SidecarKey(key), bytes.NewReader(idx), int64(len(idx))); err != nil {
			t.Fatalf("put sidecar %s: %v", key, err)
		}
	}
	return catalog.File{
		Tenant: f.Tenant, Stream: f.Stream, Key: key,
		MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
		LabelValues: st.LabelValues, LabelsComplete: complete,
		HasSidecar: withSidecar, CreatedAt: time.Now().Truncate(time.Microsecond),
	}
}

// assertRowsEqual compares two row slices as multisets of row keys.
func assertRowsEqual(t *testing.T, got, want []plan.Row) {
	t.Helper()
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

// assertBucketsEqual compares two bucket slices as multisets.
func assertBucketsEqual(t *testing.T, got, want []plan.Bucket) {
	t.Helper()
	key := func(b plan.Bucket) string {
		return fmt.Sprintf("%d|%s|%s|%d|%d", b.Start.UnixMicro(), b.Labels, b.Meta, b.Count, b.Bytes)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d buckets, want %d", len(got), len(want))
	}
	g := make([]string, len(got))
	for i, b := range got {
		g[i] = key(b)
	}
	w := make([]string, len(want))
	for i, b := range want {
		w[i] = key(b)
	}
	slices.Sort(g)
	slices.Sort(w)
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("bucket %d differs:\n got  %s\n want %s", i, g[i], w[i])
		}
	}
}

// TestSourcePrunesBySidecar proves index pruning end to end over both real
// backends: a |= needle absent from a file's sidecar drops the file before any
// Parquet acquisition, a file without a sidecar stays scannable, and the scan
// metrics record the split exactly. A control Source without the sidecar cache
// runs the same selection, returns the same rows and acquires every file: the
// acquisition and metric assertions bind pruning, not fixture accidents.
func TestSourcePrunesBySidecar(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			files := plantest.Dataset()
			env := newBackendEnv(t, kind, files)
			var dupsBytes float64
			for _, f := range env.files {
				if f.Tenant == tenant.Default && f.Stream == "api" && !f.HasSidecar {
					dupsBytes = float64(f.Bytes)
				}
			}
			if dupsBytes == 0 {
				t.Fatal("fixture has no sidecar-less api file")
			}
			sel := plan.Selection{
				Tenant:      tenant.Default,
				Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "api")},
				LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "no-such-needle-zq")},
				From:        plantest.T0.Add(-time.Hour),
				Through:     plantest.T0.Add(2 * time.Hour),
				Direction:   plan.Forward,
			}
			before := scanCountsNow(t, env)
			got, err := collectRows(env.src.Select(t.Context(), sel))
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("got %d rows for a needle no line contains, want 0", len(got))
			}
			after := scanCountsNow(t, env)
			if d := after.delta(before); d.prunedIndex != 4 || d.scanned != 1 || d.prunedLabels != 0 {
				t.Fatalf("metric delta = scanned %v pruned_index %v pruned_labels %v, want scanned 1 pruned_index 4 pruned_labels 0", d.scanned, d.prunedIndex, d.prunedLabels)
			} else if d.bytes != dupsBytes {
				t.Fatalf("bytes scanned delta = %v, want the dups file's %v catalog bytes", d.bytes, dupsBytes)
			}
			for _, f := range env.files {
				if f.Tenant == tenant.Default && f.Stream == "api" && f.HasSidecar && env.res.acquired(f.Key) {
					t.Errorf("pruned object %s was acquired", f.Key)
				}
			}
			if acquires, _ := env.res.counts(); acquires != 1 {
				t.Fatalf("acquired %d objects, want 1 (only the sidecar-less dups file)", acquires)
			}

			// Control: the same selection over a Source without the sidecar
			// cache acquires every api file and returns the same (zero) rows.
			plainRes := &countingResolver{fileResolver: env.cache}
			plain := newSource(env.db, env.cat, plainRes, defaultStreamLabels)
			controlRows, err := collectRows(plain.Select(t.Context(), sel))
			if err != nil {
				t.Fatalf("control Select: %v", err)
			}
			assertRowsEqual(t, controlRows, got)
			if acquires, _ := plainRes.counts(); acquires != 5 {
				t.Fatalf("control without pruning acquired %d objects, want all 5 api files", acquires)
			}
		})
	}
}

// TestSourcePrunesBySidecarCases walks the pruning decisions over the frozen
// dataset: absent needles prune every sidecar file, a needle in one file
// prunes the rest, ANDed needles drop a file when any of them is definitely
// absent, present needles and too-short needles keep everything, and
// non-contains operators never consult a sidecar at all. Rows are always
// compared with MemSource.
func TestSourcePrunesBySidecarCases(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			files := plantest.Dataset()
			env := newBackendEnv(t, kind, files)
			ref := plantest.Mem(files)
			eq := plan.MustMatcher(plan.MatchEqual, "service_name", "api")
			contains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }
			notContains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineNotContains, v) }
			lre := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineRegexp, v) }

			for _, tc := range []struct {
				name        string
				filters     []plan.LineFilter
				wantPruned  float64
				wantScanned float64
				wantMisses  float64 // new sidecar fetches this case performs
			}{
				{name: "absent-needle-prunes-all", filters: []plan.LineFilter{contains("no-such-needle-zq")}, wantPruned: 4, wantScanned: 1, wantMisses: 4},
				{name: "absent-again-served-from-cache", filters: []plan.LineFilter{contains("no-such-needle-zq")}, wantPruned: 4, wantScanned: 1, wantMisses: 0},
				{name: "one-file-needle", filters: []plan.LineFilter{contains("boundary exact")}, wantPruned: 3, wantScanned: 2, wantMisses: 0},
				{name: "and-split-any-absence-drops", filters: []plan.LineFilter{contains("boundary"), contains("bulk 12")}, wantPruned: 4, wantScanned: 1, wantMisses: 0},
				{name: "present-needle-keeps", filters: []plan.LineFilter{contains("POST /api/orders")}, wantPruned: 0, wantScanned: 5, wantMisses: 0},
				{name: "short-needle-never-prunes", filters: []plan.LineFilter{contains("GE")}, wantPruned: 0, wantScanned: 5, wantMisses: 0},
				{name: "regexp-never-consults-sidecar", filters: []plan.LineFilter{lre("(?i)error")}, wantPruned: 0, wantScanned: 5, wantMisses: 0},
				{name: "not-contains-never-consults-sidecar", filters: []plan.LineFilter{notContains("error")}, wantPruned: 0, wantScanned: 5, wantMisses: 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					sel := plan.Selection{
						Tenant: tenant.Default, Matchers: []plan.Matcher{eq}, LineFilters: tc.filters,
						From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(2 * time.Hour), Direction: plan.Forward,
					}
					before := scanCountsNow(t, env)
					misses := sidecarMisses(t, env)
					assertSelectEqual(t, env.src, ref, sel)
					d := scanCountsNow(t, env).delta(before)
					if d.prunedIndex != tc.wantPruned || d.scanned != tc.wantScanned {
						t.Fatalf("metric delta = scanned %v pruned_index %v, want scanned %v pruned_index %v",
							d.scanned, d.prunedIndex, tc.wantScanned, tc.wantPruned)
					}
					if d.prunedLabels != 0 {
						t.Fatalf("pruned_labels delta = %v, want 0 (no equality matcher on a promoted label)", d.prunedLabels)
					}
					if gotMisses := sidecarMisses(t, env) - misses; gotMisses != tc.wantMisses {
						t.Fatalf("sidecar cache misses delta = %v, want %v", gotMisses, tc.wantMisses)
					}
				})
			}
			// The first case fetched all four sidecars exactly once.
			if misses := sidecarMisses(t, env); misses != 4 {
				t.Fatalf("total sidecar misses = %v, want 4 (each sidecar fetched once, then cached)", misses)
			}
		})
	}
}

// TestSourceSidecarFailureScans checks the fail-open rule: a sidecar that is
// corrupt, or flagged but missing from the store, keeps its file scannable and
// counts its error in the sidecar cache metrics; results stay MemSource-equal.
func TestSourceSidecarFailureScans(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			files := plantest.Dataset()
			env := newBackendEnv(t, kind, files)
			ref := plantest.Mem(files)
			var corruptKey, missingKey string
			for _, f := range env.files {
				if f.Tenant != tenant.Default || f.Stream != "api" || !f.HasSidecar {
					continue
				}
				switch {
				case corruptKey == "":
					corruptKey = f.Key
					junk := []byte("not a sidecar at all")
					if err := env.store.Put(t.Context(), objstore.SidecarKey(f.Key), bytes.NewReader(junk), int64(len(junk))); err != nil {
						t.Fatalf("corrupt sidecar: %v", err)
					}
				case missingKey == "":
					missingKey = f.Key
					if err := env.store.Delete(t.Context(), objstore.SidecarKey(f.Key)); err != nil {
						t.Fatalf("delete sidecar: %v", err)
					}
				}
			}
			if corruptKey == "" || missingKey == "" {
				t.Fatal("fixture does not carry two sidecar-backed files")
			}
			sel := plan.Selection{
				Tenant:      tenant.Default,
				Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "api")},
				LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "no-such-needle-zq")},
				From:        plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(2 * time.Hour), Direction: plan.Forward,
			}
			before := scanCountsNow(t, env)
			assertSelectEqual(t, env.src, ref, sel)
			d := scanCountsNow(t, env).delta(before)
			// Five api files: two kept through sidecar failures, dups without a
			// sidecar, and the remaining two pruned.
			if d.scanned != 3 || d.prunedIndex != 2 {
				t.Fatalf("metric delta = scanned %v pruned_index %v, want scanned 3 pruned_index 2", d.scanned, d.prunedIndex)
			}
			if got := metricValue(t, env.scanReg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "decode_error"}); got != 1 {
				t.Fatalf("decode_error fetches = %v, want 1", got)
			}
			if got := metricValue(t, env.scanReg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}); got != 1 {
				t.Fatalf("fetch_error fetches = %v, want 1", got)
			}
			if !env.res.acquired(corruptKey) || !env.res.acquired(missingKey) {
				t.Error("a file with an unreadable sidecar was not scanned")
			}
		})
	}
}

// TestSourcePruneTenantIsolation proves each tenant's pruning consults only
// that tenant's own sidecar objects: a needle present in the other tenant's
// lines does not keep this tenant's file, and vice versa.
func TestSourcePruneTenantIsolation(t *testing.T) {
	const needle = "ZEBRA-marker"
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	mk := func(t tenant.ID, line string) plantest.File {
		return plantest.File{Tenant: t, Stream: "api", Rows: []plan.Row{
			rw(plantest.T0, line, map[string]string{"service_name": "api"}),
		}}
	}
	files := []plantest.File{mk(tenant.Default, "plain line"), mk(plantest.OtherTenant, "has "+needle+" inside")}
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, files)
			for _, tc := range []struct {
				tenant      tenant.ID
				wantPruned  float64
				wantScanned float64
			}{
				{tenant: tenant.Default, wantPruned: 1, wantScanned: 0},
				{tenant: plantest.OtherTenant, wantPruned: 0, wantScanned: 1},
			} {
				sel := plan.Selection{
					Tenant:      tc.tenant,
					Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "api")},
					LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, needle)},
					From:        plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
				}
				before := scanCountsNow(t, env)
				assertSelectEqual(t, env.src, plantest.Mem(files), sel)
				d := scanCountsNow(t, env).delta(before)
				if d.prunedIndex != tc.wantPruned || d.scanned != tc.wantScanned {
					t.Fatalf("%s: metric delta = scanned %v pruned_index %v, want scanned %v pruned_index %v",
						tc.tenant, d.scanned, d.prunedIndex, tc.wantScanned, tc.wantPruned)
				}
			}
		})
	}
}

// TestSourcePrunesByLabels pins label-equality pruning on real backends with
// a SQLite catalog: only complete summaries that list the constrained label
// can drop a file, legacy summaries and files without the promoted column
// stay conservative, empty equalities prune exactly like value equalities,
// and results are always MemSource-equal. The control rows run the same
// selection with the level matcher as an anchored regexp — row-equivalent but
// never label-prunable — proving the pruned_labels assertions bind the Match
// mechanism rather than the fixture.
func TestSourcePrunesByLabels(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	infoRow := func(ts time.Time) plan.Row {
		return rw(ts, "info line", map[string]string{"service_name": "api", "level": "info"})
	}
	noLevelRow := func(ts time.Time) plan.Row {
		return rw(ts, "no level line", map[string]string{"service_name": "api"})
	}
	// completeInfo: complete summary ["info"]; every row has level=info.
	completeInfo := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		infoRow(plantest.T0), infoRow(plantest.T0.Add(time.Second)), infoRow(plantest.T0.Add(2 * time.Second)),
	}}
	// legacyWarn: same shape but cataloged before phase 3 (LabelsComplete
	// false), so its summary must never prune.
	legacyWarn := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(plantest.T0.Add(time.Minute), "warn line", map[string]string{"service_name": "api", "level": "warn"}),
	}}
	// mapOnly: level lives in the labels map only; no entry in any summary.
	mapOnly := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{
		infoRow(plantest.T0.Add(2 * time.Minute)),
	}}
	// completeMixed: complete summary ["", "info"]; some rows lack level.
	completeMixed := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		infoRow(plantest.T0.Add(3 * time.Minute)), noLevelRow(plantest.T0.Add(3*time.Minute + time.Second)),
	}}
	all := []plantest.File{completeInfo, legacyWarn, mapOnly, completeMixed}

	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, nil)
			cf := storeIndexFixture(t, env.store, completeInfo, true, true)
			lf := storeIndexFixture(t, env.store, legacyWarn, false, true)
			mf := storeIndexFixture(t, env.store, mapOnly, true, true)
			xf := storeIndexFixture(t, env.store, completeMixed, true, true)
			if err := env.cat.AddFiles(t.Context(), cf, lf, mf, xf); err != nil {
				t.Fatal(err)
			}
			ref := plantest.Mem(all)
			stream := plan.MustMatcher(plan.MatchEqual, "service_name", "api")
			eqLevel := func(v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, "level", v) }
			reLevel := func(v string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, "level", v) }

			for _, tc := range []struct {
				name        string
				matcher     plan.Matcher
				wantPruned  float64
				wantScanned float64
			}{
				{name: "value-absent-prunes-complete-only", matcher: eqLevel("debug"), wantPruned: 2, wantScanned: 2},
				{name: "value-present-prunes-nothing", matcher: eqLevel("info"), wantPruned: 0, wantScanned: 4},
				{name: "warn-found-through-legacy-summary", matcher: eqLevel("warn"), wantPruned: 2, wantScanned: 2},
				{name: "empty-equality-prunes-complete-without-value", matcher: eqLevel(""), wantPruned: 1, wantScanned: 3},
			} {
				t.Run(tc.name, func(t *testing.T) {
					sel := plan.Selection{
						Tenant: tenant.Default, Matchers: []plan.Matcher{stream, tc.matcher},
						From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
					}
					before := scanCountsNow(t, env)
					assertSelectEqual(t, env.src, ref, sel)
					d := scanCountsNow(t, env).delta(before)
					if d.prunedLabels != tc.wantPruned || d.scanned != tc.wantScanned {
						t.Fatalf("metric delta = scanned %v pruned_labels %v, want scanned %v pruned_labels %v",
							d.scanned, d.prunedLabels, tc.wantScanned, tc.wantPruned)
					}
					if d.prunedIndex != 0 {
						t.Fatalf("pruned_index delta = %v, want 0 (no line filters)", d.prunedIndex)
					}
				})
			}

			// Control: the same value as an anchored regexp is row-equivalent
			// but never prunes by label, so every file is scanned.
			for _, v := range []string{"debug", "info", "warn", ""} {
				sel := plan.Selection{
					Tenant: tenant.Default, Matchers: []plan.Matcher{stream, reLevel(v)},
					From: plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
				}
				before := scanCountsNow(t, env)
				assertSelectEqual(t, env.src, ref, sel)
				d := scanCountsNow(t, env).delta(before)
				if d.prunedLabels != 0 || d.scanned != 4 {
					t.Fatalf("control(%q): metric delta = scanned %v pruned_labels %v, want scanned 4 pruned_labels 0", v, d.scanned, d.prunedLabels)
				}
			}
		})
	}
}

// TestSourcePruneReplacementRetry is the compaction race with pruning in the
// loop: attempt 1 prunes file A by its sidecar and then fails to acquire the
// other input B, whose object compaction deleted; by attempt 2 compaction has
// replaced both rows with R, and R must be evaluated against R's own sidecar —
// never inheriting A's or B's decision. The replacement-scans variant proves a
// needle-bearing replacement is scanned after A was pruned; the
// replacement-prunes variant proves a needle-free replacement is itself pruned
// with no SQL at all. Select and Aggregate each run the race in their own
// environment, so EACH operation independently experiences the failed first
// attempt and the fresh-listing retry; both real backends and the SQLite
// catalog are exercised.
func TestSourcePruneReplacementRetry(t *testing.T) {
	const needle = "WALRUS-marker"
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s"})}
	}
	aFile := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		rw(plantest.T0, "a noise one"),
		rw(plantest.T0.Add(time.Second), "a noise two"),
	}}
	bFile := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		rw(plantest.T0.Add(time.Minute), "b has "+needle),
	}}
	replacementFile := func(hasNeedle bool) plantest.File {
		line := "replacement quiet"
		if hasNeedle {
			line = "replacement has " + needle
		}
		return plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
			rw(plantest.T0.Add(2*time.Minute), line),
			rw(plantest.T0.Add(2*time.Minute+time.Second), "replacement tail"),
		}}
	}
	sel := plan.Selection{
		Tenant:      tenant.Default,
		Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")},
		LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, needle)},
		From:        plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
	}
	agg := plan.BucketAgg{Width: time.Minute, Origin: plantest.T0}

	// newReplacementRaceEnv builds a fresh environment with the race armed:
	// the first listing returns A and B, then compaction deletes B's object
	// and replaces both rows with R before the first acquisition.
	newReplacementRaceEnv := func(t *testing.T, kind backendKind, hasNeedle bool) (*backendEnv, catalog.File, catalog.File, catalog.File) {
		env := newBackendEnv(t, kind, nil)
		a := storeIndexFixture(t, env.store, aFile, true, true)
		b := storeIndexFixture(t, env.store, bFile, true, true)
		if err := env.cat.AddFiles(t.Context(), a, b); err != nil {
			t.Fatal(err)
		}
		r := storeIndexFixture(t, env.store, replacementFile(hasNeedle), true, true)
		env.cat.setHook(func() {
			// Compaction deletes the unpruned input's object (a cold acquire
			// fails) and swaps the catalog rows; A's objects may still exist
			// during the deletion grace — the cache is keyed by data key, so
			// they cannot leak into attempt 2.
			if err := env.store.Delete(t.Context(), b.Key); err != nil {
				t.Errorf("delete replaced object: %v", err)
			}
			if err := env.cat.Catalog.ReplaceFiles(t.Context(), tenant.Default, []string{a.Key, b.Key}, []catalog.File{r}); err != nil {
				t.Errorf("replace catalog rows: %v", err)
			}
		})
		return env, a, b, r
	}

	// operations are the two public paths over the shared attempt machinery.
	operations := []struct {
		name string
		run  func(t *testing.T, env *backendEnv, ref *plan.MemSource)
	}{
		{"select", func(t *testing.T, env *backendEnv, ref *plan.MemSource) {
			got, err := collectRows(env.src.Select(t.Context(), sel))
			if err != nil {
				t.Fatalf("Select after replacement: %v", err)
			}
			want, err := collectRows(ref.Select(t.Context(), sel))
			if err != nil {
				t.Fatal(err)
			}
			assertRowsEqual(t, got, want)
		}},
		{"aggregate", func(t *testing.T, env *backendEnv, ref *plan.MemSource) {
			buckets, err := collectBuckets(env.src.Aggregate(t.Context(), sel, agg))
			if err != nil {
				t.Fatalf("Aggregate after replacement: %v", err)
			}
			want, err := collectBuckets(ref.Aggregate(t.Context(), sel, agg))
			if err != nil {
				t.Fatal(err)
			}
			assertBucketsEqual(t, buckets, want)
		}},
	}

	for _, kind := range []backendKind{backendFS, backendS3} {
		for _, variant := range []struct {
			name      string
			hasNeedle bool
		}{
			{"replacement-prunes", false},
			{"replacement-scans", true},
		} {
			for _, op := range operations {
				t.Run(kind.String()+"/"+variant.name+"/"+op.name, func(t *testing.T) {
					env, a, b, r := newReplacementRaceEnv(t, kind, variant.hasNeedle)
					wantFiles := []plantest.File{replacementFile(variant.hasNeedle)}
					if !variant.hasNeedle {
						wantFiles = nil
					}
					ref := plantest.Mem(wantFiles)

					before := scanCountsNow(t, env)
					op.run(t, env, ref)

					// The race itself: one stale listing, one fresh retry.
					if n := env.cat.listings(); n != 2 {
						t.Fatalf("catalog listings = %d, want exactly 2 (stale listing plus fresh retry)", n)
					}
					if env.res.acquired(a.Key) {
						t.Error("pruned object A was acquired")
					}
					if env.res.acquired(b.Key) {
						t.Error("deleted object B was acquired")
					}
					// Inheritance control: if attempt 2 had reused A's pruning
					// decision instead of fetching R's own sidecar, the scans
					// variant would acquire nothing and return no rows; if it
					// had reused B's keep decision, the prunes variant would
					// scan R. The acquisition and metric assertions below fail
					// in exactly those cases.
					if acq := env.res.acquired(r.Key); acq != variant.hasNeedle {
						t.Fatalf("replacement acquired = %v, want %v (attempt 2 decides by the replacement's own sidecar)", acq, variant.hasNeedle)
					}
					d := scanCountsNow(t, env).delta(before)
					wantScanned, wantPruned, wantBytes := 0.0, 2.0, 0.0
					if variant.hasNeedle {
						wantScanned, wantPruned, wantBytes = 1, 1, float64(r.Bytes)
					}
					if d.scanned != wantScanned || d.prunedIndex != wantPruned || d.bytes != wantBytes {
						t.Fatalf("metric delta = scanned %v pruned_index %v bytes %v, want scanned %v pruned_index %v bytes %v",
							d.scanned, d.prunedIndex, d.bytes, wantScanned, wantPruned, wantBytes)
					}
					// Attempt 1 fetched A's and B's sidecars, attempt 2 fetched
					// R's: three misses prove R was evaluated, not inherited.
					if misses := sidecarMisses(t, env); misses != 3 {
						t.Fatalf("sidecar misses = %v, want 3 (A, B on attempt 1; the replacement on attempt 2)", misses)
					}
					if leaks := env.res.liveLeases(); leaks != 0 {
						t.Errorf("%d leases still held after the retry", leaks)
					}
				})
			}
		}
	}
}

// TestSourceScanAccounting pins the scan counters' attempt boundary: bytes and
// files count the moment a statement is submitted after successful
// acquisitions — a corrupt-Parquet SQL failure still counts, an acquisition
// failure before any SQL counts nothing, and a disappearance during open
// counts attempt 1's submission while the retried attempt's failed acquisition
// adds nothing.
func TestSourceScanAccounting(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s"})}
	}
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0, "old 1"), rw(plantest.T0.Add(time.Second), "old 2")}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0.Add(time.Minute), "new 1"), rw(plantest.T0.Add(time.Minute+time.Second), "new 2")}},
	}
	corrupt := []byte("this is not a parquet file")

	t.Run("failed-acquisition-before-sql-counts-nothing", func(t *testing.T) {
		for _, kind := range []backendKind{backendFS, backendS3} {
			t.Run(kind.String(), func(t *testing.T) {
				env := newBackendEnv(t, kind, files)
				gone := env.files[0].Key
				env.cat.setHook(func() {
					if err := env.store.Delete(t.Context(), gone); err != nil {
						t.Errorf("delete object: %v", err)
					}
				})
				before := scanCountsNow(t, env)
				got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
				if err == nil {
					t.Fatal("Select succeeded with a persistently missing object")
				}
				if !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("error = %v, want fs.ErrNotExist", err)
				}
				if len(got) != 0 {
					t.Fatalf("%d rows yielded before the failure, want 0", len(got))
				}
				if d := scanCountsNow(t, env).delta(before); d.scanned != 0 || d.bytes != 0 {
					t.Fatalf("metric delta = scanned %v bytes %v, want 0 and 0 (nothing was submitted)", d.scanned, d.bytes)
				}
			})
		}
	})

	t.Run("failed-sql-submission-counts", func(t *testing.T) {
		for _, kind := range []backendKind{backendFS, backendS3} {
			t.Run(kind.String(), func(t *testing.T) {
				env := newBackendEnv(t, kind, files)
				if err := env.store.Put(t.Context(), env.files[0].Key, bytes.NewReader(corrupt), int64(len(corrupt))); err != nil {
					t.Fatalf("corrupt object: %v", err)
				}
				var wantBytes float64
				for _, f := range env.files {
					wantBytes += float64(f.Bytes)
				}
				before := scanCountsNow(t, env)
				got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
				if err == nil {
					t.Fatal("Select succeeded on a corrupt Parquet object")
				}
				if errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("corrupt-file error misclassified as missing: %v", err)
				}
				if len(got) != 0 {
					t.Fatalf("%d rows yielded from the corrupt attempt, want 0", len(got))
				}
				d := scanCountsNow(t, env).delta(before)
				if d.scanned != 2 || d.bytes != wantBytes {
					t.Fatalf("metric delta = scanned %v bytes %v, want scanned 2 bytes %v (submitted once, before the SQL failed)", d.scanned, d.bytes, wantBytes)
				}
				if n := env.cat.listings(); n != 1 {
					t.Fatalf("catalog listings = %d, want exactly 1 (corrupt data must not retry)", n)
				}
			})
		}
	})

	t.Run("open-disappearance-counts-first-attempt-only", func(t *testing.T) {
		env := newBackendEnv(t, backendFS, files)
		gone := env.files[0].Key
		env.res.hook = func() {
			if err := os.Remove(filepath.Join(env.dir, filepath.FromSlash(gone))); err != nil {
				t.Fatalf("remove acquired object: %v", err)
			}
		}
		var wantBytes float64
		for _, f := range env.files {
			wantBytes += float64(f.Bytes)
		}
		before := scanCountsNow(t, env)
		got, err := collectRows(env.src.Select(t.Context(), retrySelection()))
		if err == nil {
			t.Fatal("Select succeeded after a file vanished before open")
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("error = %v, want fs.ErrNotExist", err)
		}
		if len(got) != 0 {
			t.Fatalf("%d rows yielded before the failure, want 0", len(got))
		}
		d := scanCountsNow(t, env).delta(before)
		if d.scanned != 2 || d.bytes != wantBytes {
			t.Fatalf("metric delta = scanned %v bytes %v, want scanned 2 bytes %v (attempt 1 submitted; the retry died at acquisition)", d.scanned, d.bytes, wantBytes)
		}
		if n := env.cat.listings(); n != 2 {
			t.Fatalf("catalog listings = %d, want exactly 2", n)
		}
	})
}

// TestSourcePruneCanceled checks that a context canceled while the pruning
// walk is about to start terminates the request through the pruning loop's own
// cancellation check: no sidecar is fetched, no file is dropped, and the
// Select fails with context.Canceled after exactly one listing.
func TestSourcePruneCanceled(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s"})}
	}
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0, "one"), rw(plantest.T0.Add(time.Second), "two")}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0.Add(time.Minute), "three")}},
	}
	env := newBackendEnv(t, backendFS, files)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.cat.setHook(cancel) // barrier: canceled while the listing is consumed

	sel := plan.Selection{
		Tenant:      tenant.Default,
		Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")},
		LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "never-matches-anywhere")},
		From:        plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour), Direction: plan.Forward,
	}
	before := scanCountsNow(t, env)
	got, err := collectRows(env.src.Select(ctx, sel))
	if err == nil {
		t.Fatal("Select succeeded on a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if len(got) != 0 {
		t.Fatalf("%d rows yielded after cancellation, want 0", len(got))
	}
	if d := scanCountsNow(t, env).delta(before); d.prunedIndex != 0 || d.scanned != 0 {
		t.Fatalf("metric delta = scanned %v pruned_index %v, want 0 and 0 (cancellation is not a pruning decision)", d.scanned, d.prunedIndex)
	}
	if misses := sidecarMisses(t, env); misses != 0 {
		t.Fatalf("sidecar misses = %v, want 0 (no sidecar consulted after cancellation)", misses)
	}
	if n := env.cat.listings(); n != 1 {
		t.Fatalf("catalog listings = %d, want exactly 1", n)
	}
	if leaks := env.res.liveLeases(); leaks != 0 {
		t.Errorf("%d leases still held after cancellation", leaks)
	}
}

// TestSourcePruneAggregateAllPruned proves the all-pruned path returns cleanly
// without SQL for Aggregate too: no error, no buckets, no acquisitions.
func TestSourcePruneAggregateAllPruned(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "s"})}
	}
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0, "one")}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{rw(plantest.T0.Add(time.Second), "two")}},
	}
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, files)
			sel := plan.Selection{
				Tenant:      tenant.Default,
				Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")},
				LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "no-such-needle-zq")},
				From:        plantest.T0.Add(-time.Hour), Through: plantest.T0.Add(time.Hour),
			}
			before := scanCountsNow(t, env)
			buckets, err := collectBuckets(env.src.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: plantest.T0}))
			if err != nil {
				t.Fatalf("Aggregate with every file pruned: %v", err)
			}
			if len(buckets) != 0 {
				t.Fatalf("got %d buckets for a needle no line contains, want 0", len(buckets))
			}
			d := scanCountsNow(t, env).delta(before)
			if d.scanned != 0 || d.prunedIndex != 2 || d.bytes != 0 {
				t.Fatalf("metric delta = scanned %v pruned_index %v bytes %v, want scanned 0 pruned_index 2 bytes 0", d.scanned, d.prunedIndex, d.bytes)
			}
			if acquires, _ := env.res.counts(); acquires != 0 {
				t.Fatalf("acquired %d objects, want 0 (everything pruned before acquisition)", acquires)
			}
		})
	}
}
