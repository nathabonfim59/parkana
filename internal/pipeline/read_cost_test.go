package pipeline_test

import (
	"context"
	"iter"
	"runtime"
	"runtime/debug"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// costCall records one Select that reached the raw source: the limit and
// line-filter count it was asked for, and how many rows the raw source
// yielded back. rawRows counts rows pulled from the raw iterator, not
// physical storage reads.
type costCall struct {
	limit   int
	filters int
	rawRows int
}

// costSource wraps the raw DuckDB source for TestReadSourceFixtureCost. Each
// Select appends one per-operation record before forwarding; Aggregate
// forwards unchanged.
type costSource struct {
	raw   plan.Source
	calls []costCall
}

var _ plan.Source = (*costSource)(nil)

// Select implements plan.Source.
func (s *costSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	s.calls = append(s.calls, costCall{limit: sel.Limit, filters: len(sel.LineFilters)})
	i := len(s.calls) - 1
	return func(yield func(plan.Row, error) bool) {
		for r, err := range s.raw.Select(ctx, sel) {
			if err != nil {
				yield(plan.Row{}, err)
				return
			}
			s.calls[i].rawRows++
			if !yield(r, nil) {
				return
			}
		}
	}
}

// Aggregate implements plan.Source.
func (s *costSource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return s.raw.Aggregate(ctx, sel, agg)
}

// costScan is a snapshot of the querier's candidate-input counters: catalog
// files submitted to DuckDB or pruned before it by search sidecars or label
// summaries, and the compressed candidate bytes submitted. These count
// candidates entering read attempts, not physical reads.
type costScan struct {
	scanned, prunedIndex, prunedLabels, bytes float64
}

// sub returns the per-query delta of two counter snapshots.
func (s costScan) sub(o costScan) costScan {
	return costScan{
		scanned:      s.scanned - o.scanned,
		prunedIndex:  s.prunedIndex - o.prunedIndex,
		prunedLabels: s.prunedLabels - o.prunedLabels,
		bytes:        s.bytes - o.bytes,
	}
}

// costGather snapshots the querier scan counters registered on g.
func costGather(t *testing.T, g prometheus.Gatherer) costScan {
	t.Helper()
	return costScan{
		scanned:      sampleValue(t, g, "parkana_query_files_total", map[string]string{"result": "scanned"}),
		prunedIndex:  sampleValue(t, g, "parkana_query_files_total", map[string]string{"result": "pruned_index"}),
		prunedLabels: sampleValue(t, g, "parkana_query_files_total", map[string]string{"result": "pruned_labels"}),
		bytes:        sampleValue(t, g, "parkana_query_bytes_scanned_total", nil),
	}
}

// costRaceEnabled reports whether the test binary was built with the race
// detector, read from the binary's own build settings so the measurement log
// can say which mode produced it.
func costRaceEnabled() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, s := range info.Settings {
		if s.Key == "-race" {
			return s.Value != "false"
		}
	}
	return false
}

// TestReadSourceFixtureCost measures the cost of one unmatched read pipeline
// on the real DuckDB source over the plantest fixture. The pipeline's
// selector fits no fixture dataset, so its output is identical to having no
// read pipelines at all; but Snapshot.Has(StageRead) still sends the wrapper
// down the conservative transformed path, which strips the line filter and
// the limit from the raw Select. Two warmups per variant, then seven timed
// runs alternating which variant goes first; the log reports per-run and
// median durations, the rows the raw source yielded, the limit/filters the
// raw Select was asked for, and the candidate file/byte counter deltas. The
// assertions cover output equivalence, the recorded request shape and that
// the candidate counters moved, never a timing threshold.
func TestReadSourceFixtureCost(t *testing.T) {
	files := plantest.Dataset()
	deps := equivalenceDeps(t)
	raw, _ := equivalenceDuckDB(t, deps, files, &readProvider{})

	emptySnap := readSnapshot(t)
	unmatchedSnap := readSnapshot(t, readCompile(t, "unmatched", "elsewhere",
		pipeline.Step{Type: pipeline.StepDropIf, Expr: "false"}))

	recEmpty := &costSource{raw: raw}
	recUnmatched := &costSource{raw: raw}
	emptySrc, err := pipeline.NewReadSource(recEmpty, &readProvider{fallback: emptySnap}, readStreamLabels)
	if err != nil {
		t.Fatal(err)
	}
	unmatchedSrc, err := pipeline.NewReadSource(recUnmatched, &readProvider{fallback: unmatchedSnap}, readStreamLabels)
	if err != nil {
		t.Fatal(err)
	}

	sel := plan.Selection{
		Tenant:      tenant.Default,
		Matchers:    []plan.Matcher{readEq("service_name", "api")},
		LineFilters: []plan.LineFilter{readContains("bulk")},
		From:        plantest.T0.Add(-time.Microsecond),
		Through:     plantest.T0.Add(time.Hour),
		Direction:   plan.Forward,
		Limit:       100,
	}
	gatherer, ok := deps.Metrics.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("deps.Metrics (%T) does not implement prometheus.Gatherer", deps.Metrics)
	}

	// Measurement environment and the fixture's scale for the selection: the
	// total rows the transformed raw Select covers (no filters, no limit)
	// and the rows the line filter would match, both from the frozen
	// MemSource over the same dataset.
	rawSel := sel
	rawSel.LineFilters = nil
	rawSel.Limit = 0
	mem := plantest.Mem(files)
	rawTotal := len(readCollectRows(t, mem.Select(t.Context(), rawSel)))
	matchSel := sel
	matchSel.Limit = 0
	matchTotal := len(readCollectRows(t, mem.Select(t.Context(), matchSel)))
	t.Logf("env go=%s numcpu=%d gomaxprocs=%d race=%v fixture_raw_rows=%d fixture_matched_rows=%d",
		runtime.Version(), runtime.NumCPU(), runtime.GOMAXPROCS(0), costRaceEnabled(), rawTotal, matchTotal)

	type variant struct {
		name string
		rec  *costSource
		src  plan.Source
	}
	variants := []variant{
		{"no-read-defs", recEmpty, emptySrc},
		{"unmatched-read-pipeline", recUnmatched, unmatchedSrc},
	}

	// baseline is the first measured result; every later run is compared
	// against it, outside the timed region. The counters must move on every
	// query: an all-zero delta means the metric names are wrong, not that the
	// scan cost nothing.
	var baseline []plan.Row
	measure := func(v variant) (time.Duration, costCall, costScan) {
		before := costGather(t, gatherer)
		base := len(v.rec.calls)
		start := time.Now()
		rows := readCollectRows(t, v.src.Select(t.Context(), sel))
		dur := time.Since(start)
		if len(v.rec.calls) != base+1 {
			t.Fatalf("%s: raw Select calls = %d, want exactly 1 per query", v.name, len(v.rec.calls)-base)
		}
		delta := costGather(t, gatherer).sub(before)
		if len(rows) != 100 {
			t.Fatalf("%s: rows = %d, want 100", v.name, len(rows))
		}
		if baseline == nil {
			baseline = rows
		} else {
			readCompareRowOrder(t, v.name+" vs baseline", rows, baseline)
		}
		if delta.scanned <= 0 || delta.bytes <= 0 {
			t.Fatalf("%s: candidate counters scanned=%v bytes=%v, want positive deltas", v.name, delta.scanned, delta.bytes)
		}
		return dur, v.rec.calls[base], delta
	}

	for range 2 {
		for _, v := range variants {
			measure(v)
		}
	}

	const runs = 7
	durs := make(map[string][]time.Duration, len(variants))
	for i := range runs {
		order := variants
		if i%2 == 1 {
			order = []variant{variants[1], variants[0]}
		}
		for _, v := range order {
			dur, call, delta := measure(v)
			durs[v.name] = append(durs[v.name], dur)
			t.Logf("run %d %-24s %9s raw_rows=%d req_limit=%d req_filters=%d candidate_files{scanned=%v pruned_index=%v pruned_labels=%v} candidate_bytes=%v",
				i, v.name, dur, call.rawRows, call.limit, call.filters,
				delta.scanned, delta.prunedIndex, delta.prunedLabels, delta.bytes)
		}
	}
	for _, v := range variants {
		ds := slices.Clone(durs[v.name])
		slices.Sort(ds)
		last := v.rec.calls[len(v.rec.calls)-1]
		t.Logf("SUMMARY %-24s median=%s runs=%v raw_rows=%d req_limit=%d req_filters=%d",
			v.name, ds[len(ds)/2], durs[v.name], last.rawRows, last.limit, last.filters)
	}

	// The mechanism under measurement: the empty snapshot delegates the exact
	// selection to the raw source, while the unmatched pipeline strips the
	// filter and the limit so the raw source yields rows until the wrapper's
	// own limit stops it.
	if c := recEmpty.calls[len(recEmpty.calls)-1]; c.limit != sel.Limit || c.filters != len(sel.LineFilters) {
		t.Fatalf("no-defs raw request = limit %d, %d filters; want the delegated %d and %d",
			c.limit, c.filters, sel.Limit, len(sel.LineFilters))
	}
	if c := recUnmatched.calls[len(recUnmatched.calls)-1]; c.limit != 0 || c.filters != 0 {
		t.Fatalf("unmatched raw request = limit %d, %d filters; want the stripped 0 and 0", c.limit, c.filters)
	}
	if more, less := recUnmatched.calls[len(recUnmatched.calls)-1].rawRows, recEmpty.calls[len(recEmpty.calls)-1].rawRows; more <= less {
		t.Fatalf("raw_rows unmatched=%d no-defs=%d: the transformed path did not pull more raw rows", more, less)
	}
}
