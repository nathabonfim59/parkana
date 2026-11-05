// Stored-row identity for the querier's plan.Source: at-least-once delivery
// can store the same row twice, and the Source contract counts rows with the
// same TS, complete canonical labels, canonical metadata and line once, before
// Select's order/limit and before Aggregate's bucketing. These tests drive the
// public entrypoints against the frozen MemSource oracle and also pin the
// surviving row of every identity dimension.
package query

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// dedupFixtures returns the same logical rows in two physical layouts: file1
// promotes level, file2 keeps everything in the labels map. A row appears in
// both files, a row appears twice inside one file, the same metadata is stored
// in two physical key orders across files, and nil/empty metadata both denote
// the empty set. Rows that differ in exactly one identity field survive:
// timestamp (the survivor other ts row has a same-line/labels/meta counterpart
// at another TS), line, complete labels and metadata. The logical files carry
// sorted Labels and Meta, the shape MemSource's identity compares.
func dedupFixtures() (physical, logical []plantest.File) {
	base := plantest.T0
	labels := logs.FromMap(map[string]string{"service_name": "dedup", "env": "prod", "level": "info"})
	otherLabels := logs.FromMap(map[string]string{"service_name": "dedup", "env": "dev", "level": "info"})
	meta := logs.FromMap(map[string]string{"region": "r1", "trace_id": "t1"})
	metaOtherOrder := logs.Labels{
		{Name: "trace_id", Value: "t1"},
		{Name: "region", Value: "r1"},
	}
	otherMeta := logs.FromMap(map[string]string{"region": "r1", "trace_id": "t2"})
	row := func(ts time.Time, line string, ls, meta logs.Labels) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: ls, Meta: meta}
	}

	physical = []plantest.File{
		{Tenant: tenant.Default, Stream: "dedup", Promoted: []string{"level"}, Rows: []plan.Row{
			row(base, "cross layout duplicate", labels, meta),
			row(base.Add(time.Second), "within file duplicate", labels, nil),
			row(base.Add(time.Second), "within file duplicate", labels, nil),
			row(base.Add(2*time.Second), "metadata key order duplicate", labels, metaOtherOrder),
			row(base.Add(3*time.Second), "nil empty meta duplicate", labels, nil),
			row(base.Add(4*time.Second), "survivor other ts", labels, meta),
			row(base.Add(5*time.Second), "survivor line a", labels, meta),
			row(base.Add(5*time.Second), "survivor line b", labels, meta),
			row(base.Add(6*time.Second), "survivor labels", labels, meta),
			row(base.Add(7*time.Second), "survivor meta", labels, meta),
		}},
		{Tenant: tenant.Default, Stream: "dedup", Rows: []plan.Row{
			row(base, "cross layout duplicate", labels, meta),
			row(base.Add(2*time.Second), "metadata key order duplicate", labels, meta),
			row(base.Add(3*time.Second), "nil empty meta duplicate", labels, logs.Labels{}),
			row(base.Add(6*time.Second), "survivor labels", otherLabels, meta),
			row(base.Add(7*time.Second), "survivor meta", labels, otherMeta),
			row(base.Add(8*time.Second), "survivor other ts", labels, meta),
		}},
	}
	logical = []plantest.File{
		{Tenant: tenant.Default, Stream: "dedup", Promoted: []string{"level"}, Rows: []plan.Row{
			row(base, "cross layout duplicate", labels, meta),
			row(base.Add(time.Second), "within file duplicate", labels, nil),
			row(base.Add(time.Second), "within file duplicate", labels, nil),
			row(base.Add(2*time.Second), "metadata key order duplicate", labels, meta),
			row(base.Add(3*time.Second), "nil empty meta duplicate", labels, nil),
			row(base.Add(4*time.Second), "survivor other ts", labels, meta),
			row(base.Add(5*time.Second), "survivor line a", labels, meta),
			row(base.Add(5*time.Second), "survivor line b", labels, meta),
			row(base.Add(6*time.Second), "survivor labels", labels, meta),
			row(base.Add(7*time.Second), "survivor meta", labels, meta),
		}},
		{Tenant: tenant.Default, Stream: "dedup", Rows: []plan.Row{
			row(base, "cross layout duplicate", labels, meta),
			row(base.Add(2*time.Second), "metadata key order duplicate", labels, meta),
			row(base.Add(3*time.Second), "nil empty meta duplicate", labels, logs.Labels{}),
			row(base.Add(6*time.Second), "survivor labels", otherLabels, meta),
			row(base.Add(7*time.Second), "survivor meta", labels, otherMeta),
			row(base.Add(8*time.Second), "survivor other ts", labels, meta),
		}},
	}
	return physical, logical
}

// dedupSelection reads every fixture row.
func dedupSelection() plan.Selection {
	return plan.Selection{
		Tenant:   tenant.Default,
		Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "dedup")},
		From:     plantest.T0.Add(-time.Hour),
		Through:  plantest.T0.Add(time.Hour),
	}
}

// dedupLineCounts is the distinct-row answer over dedupSelection: one per
// collapsed duplicate and the surviving pairs in line, labels, metadata and
// timestamp (the two "survivor other ts" rows share line, labels and metadata).
func dedupLineCounts() map[string]int {
	return map[string]int{
		"cross layout duplicate":       1,
		"within file duplicate":        1,
		"metadata key order duplicate": 1,
		"nil empty meta duplicate":     1,
		"survivor other ts":            2,
		"survivor line a":              1,
		"survivor line b":              1,
		"survivor labels":              2,
		"survivor meta":                2,
	}
}

func dedupDistinctRows() int {
	total := 0
	for _, n := range dedupLineCounts() {
		total += n
	}
	return total
}

func dedupDistinctBytes() int64 {
	var total int64
	for line, n := range dedupLineCounts() {
		total += int64(len(line) * n)
	}
	return total
}

// TestSelectDedup drives the public Select: the MemSource oracle and the
// explicit per-line counts agree only if every exact stored copy collapses
// while the four identity dimensions keep their survivors.
func TestSelectDedup(t *testing.T) {
	physical, logical := dedupFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	ref := plantest.Mem(logical)
	sel := dedupSelection()

	got, err := collectRows(env.source.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if want := dedupDistinctRows(); len(got) != want {
		t.Fatalf("Select returned %d rows, want %d distinct stored rows", len(got), want)
	}
	counts := make(map[string]int)
	for _, r := range got {
		counts[r.Line]++
	}
	for line, want := range dedupLineCounts() {
		if got := counts[line]; got != want {
			t.Errorf("line %q appears %d times, want %d (all other lines: %v)", line, got, want, counts)
		}
	}
	assertSelectEqual(t, env.source, ref, sel)
}

// TestSelectDedupLimit runs finite limits in both directions. A duplicate may
// not consume a limit slot: the two copies of the first row would fill a
// forward limit of 2 before dedup, and the four trailing copies would make a
// backward result longer than the reference.
func TestSelectDedupLimit(t *testing.T) {
	physical, logical := dedupFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	ref := plantest.Mem(logical)
	for _, limit := range []int{1, 2, 3, 5, 12, 15} {
		for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
			sel := dedupSelection()
			sel.Direction = dir
			sel.Limit = limit
			t.Run(fmt.Sprintf("%s/%d", dir, limit), func(t *testing.T) {
				assertSelectLimitEqual(t, env.source, ref, sel)
			})
		}
	}
}

// assertSelectLimitEqual compares a limited Select with the oracle the way the
// frozen conformance suite does: same timestamp sequence, and every returned
// row present in the unlimited reference without a duplicate. Rows with equal
// TS have an unspecified order, so their exact contents may differ at the cut;
// a duplicate consuming a slot still changes the timestamp sequence or length.
func assertSelectLimitEqual(t *testing.T, src plan.Source, ref *plan.MemSource, sel plan.Selection) {
	t.Helper()
	got, err := collectRows(src.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want, err := collectRows(ref.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("reference Select: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].TS.Equal(want[i].TS) {
			t.Fatalf("row %d TS = %s, want %s", i, got[i].TS, want[i].TS)
		}
	}
	all := sel
	all.Limit = 0
	full, err := collectRows(ref.Select(t.Context(), all))
	if err != nil {
		t.Fatal(err)
	}
	avail := make(map[string]int)
	for _, r := range full {
		avail[rowKey(r)]++
	}
	for _, r := range got {
		k := rowKey(r)
		if avail[k] == 0 {
			t.Fatalf("unexpected or duplicate row %s", k)
		}
		avail[k]--
	}
}

// TestAggregateDedup drives the public Aggregate with and without metadata
// grouping, over several bucket widths: counts and bytes follow the distinct
// row set, and each survivor stays in the bucket its exact start names.
func TestAggregateDedup(t *testing.T) {
	physical, logical := dedupFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	ref := plantest.Mem(logical)
	sel := dedupSelection()
	base := plantest.T0

	bucketKey := func(b plan.Bucket) string {
		return fmt.Sprintf("%d.%09d|%s|%s|%d|%d", b.Start.Unix(), b.Start.Nanosecond(),
			b.Labels.String(), b.Meta.String(), b.Count, b.Bytes)
	}
	for _, tc := range []struct {
		name string
		agg  plan.BucketAgg
	}{
		{"minute", plan.BucketAgg{Width: time.Minute, Origin: base}},
		{"minute-meta", plan.BucketAgg{Width: time.Minute, Origin: base, IncludeMeta: true}},
		{"second", plan.BucketAgg{Width: time.Second, Origin: base}},
		{"second-meta", plan.BucketAgg{Width: time.Second, Origin: base, IncludeMeta: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collectBuckets(env.source.Aggregate(t.Context(), sel, tc.agg))
			if err != nil {
				t.Fatalf("Aggregate: %v", err)
			}
			want, err := collectBuckets(ref.Aggregate(t.Context(), sel, tc.agg))
			if err != nil {
				t.Fatalf("reference Aggregate: %v", err)
			}
			var gotCount, wantCount int64
			for _, b := range got {
				gotCount += b.Count
			}
			for _, b := range want {
				wantCount += b.Count
			}
			if gotCount != wantCount {
				t.Fatalf("Aggregate counted %d rows, want %d distinct stored rows", gotCount, wantCount)
			}
			if wantCount != int64(dedupDistinctRows()) {
				t.Fatalf("reference counted %d rows, fixture expects %d", wantCount, dedupDistinctRows())
			}
			g := make([]string, len(got))
			for i, b := range got {
				g[i] = bucketKey(b)
			}
			w := make([]string, len(want))
			for i, b := range want {
				w[i] = bucketKey(b)
			}
			slices.Sort(g)
			slices.Sort(w)
			if !slices.Equal(g, w) {
				t.Fatalf("buckets differ:\n got  %v\n want %v", g, w)
			}
		})
	}
}
