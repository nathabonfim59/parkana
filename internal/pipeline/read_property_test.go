package pipeline_test

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// readStreamLabels is the routing priority every read-wrapper test compiles
// and wraps with, the config default.
var readStreamLabels = []string{"service_name", "job"}

// readT0 is the base instant of the hand-built datasets.
var readT0 = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

// readCompile compiles one read-stage definition with the test routing labels.
func readCompile(t *testing.T, name, selector string, steps ...pipeline.Step) *pipeline.Compiled {
	t.Helper()
	c, err := pipeline.Compile(pipeline.Definition{Name: name, Stage: pipeline.StageRead, Selector: selector, Steps: steps}, pipeline.CompileOptions{StreamLabels: readStreamLabels})
	if err != nil {
		t.Fatalf("compile %q: %v", name, err)
	}
	return c
}

// readSnapshot builds a fixed snapshot, failing on the first error.
func readSnapshot(t *testing.T, defs ...*pipeline.Compiled) pipeline.Snapshot {
	t.Helper()
	s, err := pipeline.NewFixedSnapshot(defs...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// readRow builds one stored row at readT0+d.
func readRow(d time.Duration, line string, labels, meta map[string]string) plan.Row {
	return plan.Row{TS: readT0.Add(d), Line: line, Labels: logs.FromMap(labels), Meta: logs.FromMap(meta)}
}

// readAPILabels builds an api (or custom) service label set.
func readAPILabels(service string) map[string]string {
	return map[string]string{"service_name": service, "env": "prod"}
}

// readEq is plan.MustMatcher(plan.MatchEqual, name, value).
func readEq(name, value string) plan.Matcher {
	return plan.MustMatcher(plan.MatchEqual, name, value)
}

// readContains is plan.MustLineFilter(plan.LineContains, v).
func readContains(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }

// readCollectRows drains a Select sequence.
func readCollectRows(t *testing.T, seq iter.Seq2[plan.Row, error]) []plan.Row {
	t.Helper()
	var out []plan.Row
	for r, err := range seq {
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// readCollectBuckets drains an Aggregate sequence.
func readCollectBuckets(t *testing.T, seq iter.Seq2[plan.Bucket, error]) []plan.Bucket {
	t.Helper()
	var out []plan.Bucket
	for b, err := range seq {
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// readRowKey renders a row for comparison.
func readRowKey(r plan.Row) string {
	return fmt.Sprintf("%d|%s|%s|%q", r.TS.UnixMicro(), r.Labels, r.Meta, r.Line)
}

// readBucketKey renders a bucket for comparison.
func readBucketKey(b plan.Bucket) string {
	return fmt.Sprintf("%d|%s|%s|%d/%d", b.Start.UnixMicro(), b.Labels, b.Meta, b.Count, b.Bytes)
}

// readBucketKeys renders buckets in their output order: bucket order is part
// of the plan.Source contract, so comparisons must not sort it away.
func readBucketKeys(bs []plan.Bucket) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = readBucketKey(b)
	}
	return out
}

// readProvider returns a distinct immutable snapshot per tenant.
// Unconfigured tenants get the fallback, or the empty snapshot, like the
// registry.
type readProvider struct {
	mu       sync.Mutex
	snaps    map[tenant.ID]pipeline.Snapshot
	fallback pipeline.Snapshot
}

func (p *readProvider) Snapshot(ctx context.Context, id tenant.ID) (pipeline.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := p.snaps[id]
	if snap == nil {
		snap = p.fallback
	}
	if snap == nil {
		snap, _ = pipeline.NewFixedSnapshot()
	}
	return snap, nil
}

// refRule is one hand-written read pipeline in the eager reference. It shares
// no code with internal/pipeline: a shared wrapper bug cannot make the
// comparison pass by construction.
type refRule struct {
	name     string
	selector string
	apply    func(plan.Row) (plan.Row, bool)
}

// refApplyRow runs rules in name order against the row's ORIGINAL dataset,
// exactly as Compiled matching does. A dropped row returns false.
func refApplyRow(rules []refRule, streamLabels []string, r plan.Row) (plan.Row, bool) {
	dataset := r.Labels.StreamName(streamLabels)
	for _, rule := range rules {
		if rule.selector != "*" && rule.selector != dataset {
			continue
		}
		next, keep := rule.apply(r)
		if !keep {
			return plan.Row{}, false
		}
		r = next
	}
	return r, true
}

// refLineMatches applies the selection's line filters to a transformed line.
func refLineMatches(filters []plan.LineFilter, line string) bool {
	for _, f := range filters {
		if !f.Matches(line) {
			return false
		}
	}
	return true
}

// refSelect is the eager reference Select: read raw rows without filters or
// limit, transform, filter, then apply the limit in raw order. raw must be the
// unwrapped source.
func refSelect(t *testing.T, ctx context.Context, raw plan.Source, rules []refRule, streamLabels []string, sel plan.Selection) []plan.Row {
	t.Helper()
	rawSel := sel
	rawSel.LineFilters = nil
	rawSel.Limit = 0
	var out []plan.Row
	for r, err := range raw.Select(ctx, rawSel) {
		if err != nil {
			t.Fatalf("reference raw select: %v", err)
		}
		r, keep := refApplyRow(rules, streamLabels, r)
		if !keep || !refLineMatches(sel.LineFilters, r.Line) {
			continue
		}
		out = append(out, r)
		if sel.Limit > 0 && len(out) >= sel.Limit {
			break
		}
	}
	return out
}

// refAggregate is the eager reference Aggregate: the same transform/filter
// pipeline, then grouping by BucketAgg.Start, labels and optional metadata.
// Distinct raw rows stay distinct even when a rewrite makes them equal.
func refAggregate(t *testing.T, ctx context.Context, raw plan.Source, rules []refRule, streamLabels []string, sel plan.Selection, agg plan.BucketAgg) []plan.Bucket {
	t.Helper()
	rawSel := sel
	rawSel.LineFilters = nil
	rawSel.Limit = 0
	type key struct {
		start        int64
		labels, meta string
	}
	sums := make(map[key]*plan.Bucket)
	for r, err := range raw.Select(ctx, rawSel) {
		if err != nil {
			t.Fatalf("reference raw select: %v", err)
		}
		r, keep := refApplyRow(rules, streamLabels, r)
		if !keep || !refLineMatches(sel.LineFilters, r.Line) {
			continue
		}
		start := agg.Start(r.TS)
		k := key{start: start.UnixMicro(), labels: r.Labels.String()}
		if agg.IncludeMeta {
			k.meta = r.Meta.String()
		}
		b := sums[k]
		if b == nil {
			b = &plan.Bucket{Start: start, Labels: slices.Clone(r.Labels)}
			if agg.IncludeMeta {
				b.Meta = slices.Clone(r.Meta)
			}
			sums[k] = b
		}
		b.Count++
		b.Bytes += int64(len(r.Line))
	}
	if len(sums) == 0 {
		return nil
	}
	// Independent ordering oracle: one marker row per already-computed group,
	// run through the frozen plan.MemSource Aggregate. Each marker is a unique
	// group and contributes no raw row to the counts, so this borrows only the
	// contract's output order, never its transformed-row handling.
	markers := plan.NewMemSource()
	markerRows := make([]plan.Row, 0, len(sums))
	var first, last time.Time
	for _, b := range sums {
		markerRows = append(markerRows, plan.Row{TS: b.Start, Labels: b.Labels, Meta: b.Meta})
		if first.IsZero() || b.Start.Before(first) {
			first = b.Start
		}
		if last.IsZero() || b.Start.After(last) {
			last = b.Start
		}
	}
	markers.Add(tenant.Default, markerRows...)
	orderSel := plan.Selection{
		Tenant:    tenant.Default,
		Matchers:  []plan.Matcher{plan.MustMatcher(plan.MatchRegexp, "service_name", ".*")},
		From:      first.Add(-agg.Width),
		Through:   last.Add(2 * agg.Width),
		Direction: plan.Forward,
	}
	out := make([]plan.Bucket, 0, len(sums))
	for b, err := range markers.Aggregate(ctx, orderSel, agg) {
		if err != nil {
			t.Fatalf("reference marker aggregate: %v", err)
		}
		k := key{start: b.Start.UnixMicro(), labels: b.Labels.String()}
		if agg.IncludeMeta {
			k.meta = b.Meta.String()
		}
		got, ok := sums[k]
		if !ok {
			t.Fatalf("reference marker bucket %v has no independently computed group", b)
		}
		out = append(out, *got)
	}
	if len(out) != len(sums) {
		t.Fatalf("reference marker buckets = %d, want %d groups", len(out), len(sums))
	}
	return out
}

// propertyRules returns the hand-written reference pipelines.
func propertyRules() []refRule {
	digits := regexp.MustCompile(`[0-9]+`)
	rules := []refRule{
		{
			name: "a-api-redact", selector: "api",
			apply: func(r plan.Row) (plan.Row, bool) {
				r.Line = digits.ReplaceAllString(r.Line, "N")
				return r, true
			},
		},
		{
			name: "a-web-redact", selector: "web",
			apply: func(r plan.Row) (plan.Row, bool) {
				r.Line = strings.ReplaceAll(r.Line, "secret", "hidden")
				return r, true
			},
		},
		{
			name: "b-api-rename", selector: "api",
			apply: func(r plan.Row) (plan.Row, bool) {
				v, ok := r.Meta.Get("trace")
				if !ok {
					return r, true
				}
				m := r.Meta.Map()
				delete(m, "trace")
				m["run"] = v
				r.Meta = logs.FromMap(m)
				return r, true
			},
		},
		{
			name: "b-web-drop", selector: "web",
			apply: func(r plan.Row) (plan.Row, bool) {
				return r, !strings.Contains(r.Line, "hide")
			},
		},
		{
			name: "c-api-drop", selector: "api",
			apply: func(r plan.Row) (plan.Row, bool) {
				return r, !strings.Contains(r.Line, "dropme")
			},
		},
	}
	slices.SortFunc(rules, func(a, b refRule) int { return cmp.Compare(a.name, b.name) })
	return rules
}

// propertySnapshot compiles the matching read pipelines that implement the
// same semantics as propertyRules.
func propertySnapshot(t *testing.T) pipeline.Snapshot {
	t.Helper()
	return readSnapshot(t,
		readCompile(t, "a-api-redact", "api", pipeline.Step{Type: pipeline.StepRedact, Pattern: `[0-9]+`, Replace: new("N")}),
		readCompile(t, "a-web-redact", "web", pipeline.Step{Type: pipeline.StepRedact, Pattern: "secret", Replace: new("hidden")}),
		readCompile(t, "b-api-rename", "api", pipeline.Step{Type: pipeline.StepRename, From: "trace", To: "run"}),
		readCompile(t, "b-web-drop", "web", pipeline.Step{Type: pipeline.StepDropIf, Expr: `line contains "hide"`}),
		readCompile(t, "c-api-drop", "api", pipeline.Step{Type: pipeline.StepDropIf, Expr: `line contains "dropme"`}),
	)
}

func propertyRows(rng *rand.Rand, base time.Time, n int, lines []string) []plan.Row {
	streams := []string{"api", "web", "other"}
	rows := make([]plan.Row, 0, n)
	for i := range n {
		stream := streams[rng.IntN(len(streams))]
		labels := map[string]string{"service_name": stream, "env": "prod"}
		if rng.IntN(4) == 0 {
			labels["job"] = "batch"
		}
		var meta map[string]string
		if rng.IntN(3) != 0 {
			meta = map[string]string{"trace": fmt.Sprintf("t%d", rng.IntN(4))}
		}
		rows = append(rows, plan.Row{
			TS:     base.Add(time.Duration(i) * time.Millisecond),
			Line:   lines[rng.IntN(len(lines))],
			Labels: logs.FromMap(labels),
			Meta:   logs.FromMap(meta),
		})
	}
	return rows
}

// TestReadSourcePropertyReference compares the wrapper with an eager
// hand-written transform/filter/group implementation over randomized datasets,
// selections and aggregates. It is deterministic: the PCG seed is fixed and
// logged for reproduction.
func TestReadSourcePropertyReference(t *testing.T) {
	const (
		seed1 = 0x9e3779b97f4a7c15
		seed2 = 0xbf58476d1ce4e5b9
		cases = 100
	)
	lines := []string{
		"req 12 ok",
		"dropme 34",
		"needle 99",
		"web secret hide",
		"café 7",
		"plain",
		"hide 5",
		"ERROR 42",
		"one token here",
	}
	rng := rand.New(rand.NewPCG(seed1, seed2))
	rules := propertyRules()
	provider := &readProvider{snaps: map[tenant.ID]pipeline.Snapshot{tenant.Default: propertySnapshot(t)}}

	var rewrittenInHits, aggregateBuckets, otherTenantHits int
	for tc := range cases {
		otherRows := propertyRows(rng, readT0, 1+rng.IntN(4), lines)
		defaultRows := propertyRows(rng, readT0, 1+rng.IntN(12), lines)
		// Re-deliver some exact rows: the raw source counts them once and the
		// wrapper must not deduplicate again after a rewrite.
		for range rng.IntN(3) {
			defaultRows = append(defaultRows, defaultRows[rng.IntN(len(defaultRows))])
		}
		raw := plan.NewMemSource()
		raw.Add(tenant.Default, defaultRows...)
		raw.Add(tenant.ID("other"), otherRows...)

		wrapped, err := pipeline.NewReadSource(raw, provider, readStreamLabels)
		if err != nil {
			t.Fatal(err)
		}

		tenantChoice := tenant.Default
		if rng.IntN(10) < 3 {
			tenantChoice = tenant.ID("other")
		}
		matchers := [][]plan.Matcher{
			{readEq("service_name", "api")},
			{readEq("service_name", "web")},
			{readEq("service_name", "other")},
			{plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web")},
			{readEq("job", "batch")},
			{plan.MustMatcher(plan.MatchRegexp, "service_name", ".+")},
		}[rng.IntN(6)]
		filterChoices := []plan.LineFilter{
			readContains("N"),
			readContains("needle"),
			readContains("dropme"),
			plan.MustLineFilter(plan.LineNotContains, "dropme"),
			plan.MustLineFilter(plan.LineRegexp, `[0-9]+`),
			plan.MustLineFilter(plan.LineNotRegexp, "hidden"),
			readContains("café"),
		}
		var filters []plan.LineFilter
		// Bias a third of the cases to the rewritten-in needle, so the
		// transformed-line filter path is always exercised.
		if rng.IntN(3) == 0 {
			filters = append(filters, readContains("N"))
		}
		for range rng.IntN(3) {
			filters = append(filters, filterChoices[rng.IntN(len(filterChoices))])
		}
		fromK := rng.IntN(14)
		throughK := fromK + rng.IntN(15-fromK)
		sel := plan.Selection{
			Tenant:      tenantChoice,
			Matchers:    matchers,
			LineFilters: filters,
			From:        readT0.Add(time.Duration(fromK) * time.Millisecond),
			Through:     readT0.Add(time.Duration(throughK) * time.Millisecond),
			Direction:   []plan.Direction{plan.Forward, plan.Backward}[rng.IntN(2)],
			Limit:       rng.IntN(6),
		}
		// Every third case is a guaranteed rewritten-in scenario: the api
		// redact step creates the needle the filter then matches. This keeps
		// the transformed-line filter/aggregate blocks covered.
		if tc%3 == 0 {
			sel.Tenant = tenant.Default
			sel.Matchers = []plan.Matcher{readEq("service_name", "api")}
			sel.LineFilters = []plan.LineFilter{readContains("N")}
			sel.From = readT0.Add(-time.Second)
			sel.Through = readT0.Add(time.Second)
		}

		// Guarantee a nonempty query for a tenant with no read definitions.
		// Its data must stay unchanged even when another tenant has rules.
		if tc%10 == 1 {
			raw.Add(tenant.ID("other"), readRow(0, "req 12 ok", readAPILabels("api"), map[string]string{"trace": "other-trace"}))
			sel.Tenant = tenant.ID("other")
			sel.Matchers = []plan.Matcher{readEq("service_name", "api")}
			sel.LineFilters = nil
			sel.From = readT0.Add(-time.Second)
			sel.Through = readT0.Add(time.Second)
			sel.Limit = 0
		}

		got := readCollectRows(t, wrapped.Select(t.Context(), sel))
		if sel.Tenant == tenant.ID("other") && len(got) > 0 {
			otherTenantHits++
		}
		// The reference chooses rules from its own tenant configuration,
		// independently of the snapshot provider used by the wrapper.
		tenantRules := rules
		if sel.Tenant != tenant.Default {
			tenantRules = nil
		}
		want := refSelect(t, t.Context(), raw, tenantRules, readStreamLabels, sel)
		gotKeys := make([]string, len(got))
		for i, r := range got {
			gotKeys[i] = readRowKey(r)
		}
		wantKeys := make([]string, len(want))
		for i, r := range want {
			wantKeys[i] = readRowKey(r)
		}
		if !slices.Equal(gotKeys, wantKeys) {
			t.Fatalf("case %d sel=%+v:\n got %v\nwant %v", tc, sel, gotKeys, wantKeys)
		}
		if len(sel.LineFilters) > 0 && len(got) > 0 {
			for _, f := range sel.LineFilters {
				if f.String() == readContains("N").String() {
					rewrittenInHits++
				}
			}
		}

		aggs := []plan.BucketAgg{
			{Width: time.Millisecond, Origin: readT0.Add(-2 * time.Second)},
			{Width: 7 * time.Millisecond, Origin: readT0.Add(time.Millisecond)},
			{Width: time.Second, Origin: readT0.Add(1500 * time.Millisecond)},
		}
		agg := aggs[rng.IntN(len(aggs))]
		agg.IncludeMeta = rng.IntN(2) == 0
		gotBuckets := readCollectBuckets(t, wrapped.Aggregate(t.Context(), sel, agg))
		wantBuckets := refAggregate(t, t.Context(), raw, tenantRules, readStreamLabels, sel, agg)
		aggregateBuckets += len(gotBuckets)
		if !slices.Equal(readBucketKeys(gotBuckets), readBucketKeys(wantBuckets)) {
			t.Fatalf("case %d sel=%+v agg=%+v:\n got %v\nwant %v", tc, sel, agg, readBucketKeys(gotBuckets), readBucketKeys(wantBuckets))
		}
	}
	if rewrittenInHits == 0 {
		t.Fatal("no property case exercised the rewritten-in filter path")
	}
	if otherTenantHits == 0 {
		t.Fatal("no property case returned rows for the tenant without read definitions")
	}
	if aggregateBuckets == 0 {
		t.Fatal("no property case produced an aggregate bucket")
	}
	t.Logf("property seed=%#x/%#x cases=%d rewritten-in-hits=%d aggregate-buckets=%d other-tenant-hits=%d", uint64(seed1), uint64(seed2), cases, rewrittenInHits, aggregateBuckets, otherTenantHits)
}
