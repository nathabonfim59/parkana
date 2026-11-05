// Metric query evaluation. Engine.Query hands every non-log expression here;
// this file owns the AST dispatch, the count/bytes range aggregations, the
// result shape (matrix for range queries, vector for instant queries) and the
// top-level metric limits and error rules.
//
// Everything that produces rows runs through the same compiled pipeline as log
// queries (CompiledLogQuery.process), so parsers, label filters and the
// builder error state behave identically in both. A surviving __error__ in a
// window is a bad query (pkg/logql/evaluator.go RangeVectorEvaluator.Next
// checks the metric for the error label after the whole pipeline), while a
// label filter or a drop that removes it drops or clears the row first.
//
// This slice implements the four count/rate/bytes range aggregations
// (count_over_time, rate, bytes_over_time, bytes_rate). Every other range
// function and unwrap is evaluated in range_funcs.go (A8b), vector aggregations
// in vector.go (A8a), and scalar and binary expressions in binary.go (A9).
package logql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// rangeOptions controls how a range aggregation is evaluated. The zero value
// selects the exact row-by-row plan.Source.Select path, which the tests use as
// the reference; Engine.Query opts into the single-call Aggregate fast path
// with aggregate=true.
//
// The fast path is exact for the four count/rate/bytes functions because
// BucketAgg.IncludeMeta makes the Source group by the rows' structured
// metadata as well as their stream labels: Loki keeps structured metadata in a
// metric series' identity (pkg/logql/log/metrics_extraction.go adds it before
// the pipeline, pkg/logql/log/labels.go includes it in LabelsResult), so
// aggregating stream labels alone would merge series that Loki keeps apart.
// Per-entry *parsed* labels cannot be reconstructed from buckets, so a
// pipeline with a parser still takes the Select path.
type rangeOptions struct {
	aggregate bool
	// group keeps only these labels from each Aggregate bucket, the enclosing
	// vector aggregation's by-grouping. nil keeps every label.
	group []string
	// child marks a recursive evaluation: an enclosing expression (A8's
	// vector aggregation) owns MaxSeries over its final response, so this
	// range must not reject a large intermediate result. Loki's
	// JoinSampleVector limit applies to the assembled top-level vector only.
	child bool
}

// evalMetric evaluates one metric expression. A8 owns range functions other
// than count/rate/bytes and vector aggregations; A9 (binary.go) owns scalar
// and binary expressions.
func (e *Engine) evalMetric(ctx context.Context, src plan.Source, expr Expr, p plan.Params) (plan.Result, error) {
	switch a := expr.(type) {
	case *RangeAgg:
		return e.evalRangeAgg(ctx, src, a, p, rangeOptions{aggregate: true})
	case *VectorAgg:
		return e.evalVectorAgg(ctx, src, a, p)
	case *VectorLiteral:
		return e.evalVectorLiteral(ctx, a, p)
	case *NumberLit:
		return e.evalScalar(ctx, a, p)
	case *BinaryExpr:
		return e.evalBinary(ctx, src, a, p)
	case *deferredExpr:
		return plan.Result{}, fmt.Errorf("logql: %w: %s is not supported", plan.ErrUnsupported, a.what)
	default:
		return plan.Result{}, fmt.Errorf("logql: %w: unexpected metric expression %T", plan.ErrUnsupported, expr)
	}
}

// countBytesFunc reports whether fn is one of the four range aggregations A7
// evaluates. The rest are valid LogQL (and parse) but belong to A8.
func countBytesFunc(fn RangeFunc) bool {
	switch fn {
	case CountOverTime, Rate, BytesOverTime, BytesRate:
		return true
	}
	return false
}

// evalRangeAgg evaluates a count/rate/bytes range aggregation over p and
// shapes the top-level result. A8's vector aggregation calls evalRangeSeries
// instead, because the series limit belongs to the final response.
func (e *Engine) evalRangeAgg(ctx context.Context, src plan.Source, a *RangeAgg, p plan.Params, opts rangeOptions) (plan.Result, error) {
	series, err := e.evalRangeSeries(ctx, src, a, p, opts)
	if err != nil {
		return plan.Result{}, err
	}
	if opts.child {
		return metricResult(series, p.Instant()), nil
	}
	return e.finishMetric(series, p)
}

// evalRangeSeries is the recursive seam A8 uses: it evaluates the aggregation
// and returns every series without applying MaxSeries or shaping the result.
// An enclosing vector aggregation applies the limit to its own final response
// (Loki's JoinSampleVector), so a large raw-metadata child that a by()
// projection would collapse must not fail here.
//
// It tags every returned series with the range grid (nominal evaluation index
// plus Loki's millisecond output timestamp), so neither A7's bucket path nor
// A8b's range functions deal with timestamps themselves.
func (e *Engine) evalRangeSeries(ctx context.Context, src plan.Source, a *RangeAgg, p plan.Params, opts rangeOptions) ([]*metricSeries, error) {
	series, err := e.evalRangeSeriesRaw(ctx, src, a, p, opts)
	if err != nil {
		return nil, err
	}
	return tagRangeSeries(series, p, a.Range.Offset)
}

// evalRangeSeriesRaw is evalRangeSeries without output-timestamp tagging.
func (e *Engine) evalRangeSeriesRaw(ctx context.Context, src plan.Source, a *RangeAgg, p plan.Params, opts rangeOptions) ([]*metricSeries, error) {
	// A8b owns unwrap and every range function outside the count/rate/bytes
	// family; they need the extracted values, so they take their own exact
	// path. A7 keeps the bucket fast path below.
	if a.Range.Unwrap != nil || !countBytesFunc(a.Func) {
		return e.evalAdditionalRangeSeries(ctx, src, a, p, opts)
	}
	// Validated by the parser; the four count/rate/bytes functions reject any
	// grouping, so a query that reaches here with one is a programming error
	// in the caller, not something to approximate.
	if a.Grouping != nil {
		return nil, fmt.Errorf("logql: %w: grouping is not valid for %s", plan.ErrBadQuery, a.Func)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: query: %w", err)
	}
	if src == nil {
		return nil, errors.New("logql: nil source")
	}
	if a.Range.Range < 0 {
		return nil, fmt.Errorf("logql: %w: negative range %s", plan.ErrBadQuery, a.Range.Range)
	}
	// Loki's window is (t-range, t]. With range 0 it is empty for every t:
	// range_vector.go loads rangeStart == rangeEnd and skips every sample with
	// Timestamp <= start, so no series exists and rate never divides by zero.
	// Return no series without asking the Source for rows.
	if a.Range.Range == 0 {
		return nil, nil
	}
	times, err := metricEvalTimes(p)
	if err != nil {
		return nil, err
	}
	windows := metricWindows(times, a.Range.Offset, a.Range.Range)
	grid, single, err := newMetricGrid(windows, a.Range.Range, p.Step)
	if err != nil {
		return nil, err
	}

	if opts.aggregate {
		proj, filters, ok, err := compileAggregatePipeline(a.Range)
		if err != nil {
			return nil, err
		}
		if ok {
			return e.evalAggregateRange(ctx, src, p, a, windows, grid, single, proj, filters, opts.group)
		}
	}

	return e.evalSelectRange(ctx, src, p, a, windows, grid, opts.group)
}

// evalSelectRange is the exact path: one Select over the union of every
// window, the shared pipeline per row, then microsecond bucket accumulation.
// The metric identity is the row's final label set including structured
// metadata and parsed labels, in the precedence the shared entry model gives
// them (parsed over metadata over stream labels). group is the enclosing sum's
// injected by-grouping, applied with the same GroupedLabels rules as the
// Aggregate bucket projection and only after the full error check.
func (e *Engine) evalSelectRange(ctx context.Context, src plan.Source, p plan.Params, a *RangeAgg, windows []metricWindow, grid metricGrid, group []string) ([]*metricSeries, error) {
	c, err := compileLogQuery(a.Range.Pipeline)
	if err != nil {
		return nil, err
	}
	sel := rangeSelection(c, p, windows)
	// No Select call may start after cancellation.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: select: %w", err)
	}
	var set metricSet
	for row, err := range src.Select(ctx, sel) {
		if err != nil {
			return nil, fmt.Errorf("logql: select: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: select: %w", err)
		}
		entry, keep, err := c.process(ctx, p.Tenant, row)
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		if !windowContains(windows, row.TS) {
			continue
		}
		// Checked after the whole pipeline, so a filter or drop could have
		// removed the row or cleared the state first.
		if entry.err != "" {
			return nil, metricPipelineErr(entry, metricBuilderIdentity(entry, nil, ""))
		}
		// A parent sum can inject its by-grouping into this exact count/rate/
		// bytes extractor; apply it only after the full error check, like the
		// Aggregate bucket projection.
		ms := set.get(metricProjectedIdentity(entry, group))
		ms.add(gridIndex(grid, row.TS), 1, int64(len(entry.line)))
	}
	// A Source may cancel the request right after its final yield; that is not
	// a successful query.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: select: %w", err)
	}
	for _, ms := range set.series {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: select: %w", err)
		}
		ms.windowPoints(windows, a.Func, a.Range.Range)
	}
	return set.series, nil
}

// rangeSelection is the storage request covering every evaluation window: the
// compiled query's matchers and pushed-down line filters, From the first
// window's start, Through the last window's end. The Select path uses it, and
// A8's unwrap and remaining range functions can materialize the same single
// scan with it.
func rangeSelection(c *CompiledLogQuery, p plan.Params, windows []metricWindow) plan.Selection {
	return plan.Selection{
		Tenant:      p.Tenant,
		Matchers:    slices.Clone(c.matchers),
		LineFilters: slices.Clone(c.pushdown),
		From:        windows[0].from,
		Through:     windows[len(windows)-1].through,
		// Direction only orders Select rows; bucketing ignores it, but Forward
		// keeps behavior deterministic for Sources that document tie order.
		Direction: plan.Forward,
	}
}

// gridIndex returns the grid bucket holding ts.
func gridIndex(grid metricGrid, ts time.Time) int64 {
	return int64(ts.Sub(grid.origin) / grid.width)
}

// evalAggregateRange answers the same windows from plan.Source.Aggregate. With
// single it issues exactly one Aggregate call over the union of every window;
// otherwise (a range wider than maxBucketsPerWindow buckets) it issues one
// exact call per window. BucketAgg.IncludeMeta makes each bucket carry its
// stream labels and structured metadata, which the shared entry helpers
// project with the label-only pipeline and the enclosing grouping, then merge
// before the per-window sums.
func (e *Engine) evalAggregateRange(ctx context.Context, src plan.Source, p plan.Params, a *RangeAgg, windows []metricWindow, grid metricGrid, single bool, proj bucketProjection, filters []plan.LineFilter, group []string) ([]*metricSeries, error) {
	base := plan.Selection{
		Tenant:      p.Tenant,
		Matchers:    slices.Clone(a.Range.Pipeline.Selector.Matchers),
		LineFilters: slices.Clone(filters),
		// Aggregate ignores Direction and Limit.
		Direction: plan.Backward,
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: aggregate: %w", err)
	}
	var set metricSet
	if single {
		sel := base
		sel.From = grid.origin
		sel.Through = windows[len(windows)-1].through
		agg := plan.BucketAgg{Width: grid.width, Origin: grid.origin, IncludeMeta: true}
		for b, err := range src.Aggregate(ctx, sel, agg) {
			if err != nil {
				return nil, fmt.Errorf("logql: aggregate: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: aggregate: %w", err)
			}
			idx := gridIndex(grid, b.Start)
			// Buckets in the gap between a large step and a short range feed
			// no evaluation; skip them before projecting or checking state so
			// an error label outside every window cannot fail the query.
			if !windowCoversBucket(windows, idx) {
				continue
			}
			identity, err := proj.apply(ctx, b, group)
			if err != nil {
				return nil, err
			}
			set.get(identity).add(idx, b.Count, b.Bytes)
		}
		// The Source may cancel the request after its final yield; the finished
		// work is not a success path.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: aggregate: %w", err)
		}
		for _, ms := range set.series {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: aggregate: %w", err)
			}
			ms.windowPoints(windows, a.Func, a.Range.Range)
		}
		return set.series, nil
	}

	for _, w := range windows {
		// No Aggregate call may start after cancellation.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: aggregate: %w", err)
		}
		sel := base
		sel.From, sel.Through = w.from, w.through
		agg := plan.BucketAgg{Width: w.through.Sub(w.from), Origin: w.from, IncludeMeta: true}
		totals := make(map[string]metricBucket, 8)
		labels := make(map[string]logs.Labels, 8)
		for b, err := range src.Aggregate(ctx, sel, agg) {
			if err != nil {
				return nil, fmt.Errorf("logql: aggregate: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: aggregate: %w", err)
			}
			identity, err := proj.apply(ctx, b, group)
			if err != nil {
				return nil, err
			}
			key := identity.String()
			t := totals[key]
			t.count += b.Count
			t.bytes += b.Bytes
			totals[key] = t
			labels[key] = identity
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: aggregate: %w", err)
		}
		for key, t := range totals {
			ms := set.get(labels[key])
			ms.points = append(ms.points, plan.Point{T: w.at, V: rangeValue(a.Func, t.count, t.bytes, a.Range.Range)})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: aggregate: %w", err)
	}
	return set.series, nil
}

// finishMetric enforces the series limit and shapes the result. The limit is
// checked over the assembled response, as Loki checks the series created by
// JoinSampleVector, so a query that only exceeds it in later steps still
// fails. Series with no point are dropped first: a row in the gap between a
// large step and a short range never appears in any window, and Loki would
// never create that series at all.
func (e *Engine) finishMetric(series []*metricSeries, p plan.Params) (plan.Result, error) {
	series = slices.DeleteFunc(series, func(s *metricSeries) bool { return len(s.points) == 0 })
	if e.cfg.MaxSeries > 0 && len(series) > e.cfg.MaxSeries {
		return plan.Result{}, fmt.Errorf("logql: %w: query returned %d series, exceeding the limit of %d; reduce cardinality with a by() aggregation", plan.ErrLimit, len(series), e.cfg.MaxSeries)
	}
	return metricResult(series, p.Instant()), nil
}

// metricResult shapes series into Loki's instant vector or range matrix.
// Series without a point are omitted (empty windows never invent a sample),
// and series are ordered by label set for deterministic output.
func metricResult(series []*metricSeries, instant bool) plan.Result {
	slices.SortFunc(series, func(a, b *metricSeries) int {
		return compareLabels(a.labels, b.labels)
	})
	if instant {
		vec := make([]plan.Sample, 0, len(series))
		for _, s := range series {
			if len(s.points) == 0 {
				continue
			}
			vec = append(vec, plan.Sample{Metric: s.labels, Point: metricPoint(s, 0)})
		}
		return plan.Result{Type: plan.TypeVector, Vector: vec}
	}
	matrix := make([]plan.Series, 0, len(series))
	for _, s := range series {
		if len(s.points) == 0 {
			continue
		}
		points := make([]plan.Point, len(s.points))
		for i := range s.points {
			points[i] = metricPoint(s, i)
		}
		matrix = append(matrix, plan.Series{Metric: s.labels, Points: points})
	}
	return plan.Result{Type: plan.TypeMatrix, Matrix: matrix}
}

// identityLabels merges an entry's label categories into the identity a
// metric series carries: stream labels first, then structured metadata, then
// labels the pipeline extracted. The categories are already maintained with
// Loki's precedence by the shared entry helpers - addEntryMetadata renames a
// metadata label colliding with the base stream labels to name+"_extracted"
// and removes the shadowed stream label, and a parser's setParsed does the
// same for parsed labels - so a later category simply wins here.
func identityLabels(labels, meta, parsed logs.Labels) logs.Labels {
	if len(meta) == 0 && len(parsed) == 0 {
		return slices.Clone(labels)
	}
	m := make(map[string]string, len(labels)+len(meta)+len(parsed))
	for _, l := range labels {
		m[l.Name] = l.Value
	}
	for _, l := range meta {
		m[l.Name] = l.Value
	}
	for _, l := range parsed {
		m[l.Name] = l.Value
	}
	return logs.FromMap(m)
}

// metricPipelineErr is the client error for a row or bucket whose pipeline
// failed and kept its error state, Loki's PipelineError
// (pkg/logqlmodel/error.go) mapped onto plan.ErrBadQuery so the HTTP layer
// answers 400. identity is the series' final labels.
func metricPipelineErr(e *logEntry, identity logs.Labels) error {
	return fmt.Errorf("logql: %w: pipeline error %q for series %s; add a label filter such as | %s=\"\" to skip this error", plan.ErrBadQuery, e.err, identity.String(), errorLabel)
}

// metricBuilderIdentity derives a metric sample's label set exactly as Loki's
// LabelsBuilder.GroupedLabels does for an extracting pipeline. unwrapLabel is
// the | unwrap value label the extractor removes from its grouping; "" is a
// line extractor (count/rate/bytes) or the absent extractor.
//
// The cases, in GroupedLabels order (pkg/logql/log/labels.go):
//
//  1. A live __error__ wins over grouping and returns LabelsResult, which
//     appends __error__ and __error_details__.
//  2. noLabels (Grouping.Singleton, an explicit by()) yields an empty
//     identity before any unchanged or group-list path.
//  3. An unchanged builder returns the untouched base labels, never the
//     retained error details; grouped unchanged entries group the base labels.
//  4. A modified builder with grouping returns the by/without projection and
//     never appends the builder error state: withResult/withoutResult build
//     their buffer from label categories only. A by() request for
//     __error_details__ therefore still omits retained details.
//  5. A modified builder without grouping returns the effective labels with
//     the retained __error_details__ appended as a parsed label.
//
// The range aggregation's own grouping and the enclosing vector aggregation's
// injected grouping are both represented by g. A nil grouping makes the unwrap
// extractor append its value label to the without list (the implicit "remove
// the value label" rule); an explicit by() is noLabels and never reaches that
// rule. Parent projections that must not participate in the extractor identity
// (a pure optimization) are applied separately with metricProjectedIdentity.
func metricBuilderIdentity(e *logEntry, g *Grouping, unwrapLabel string) logs.Labels {
	if e.err != "" {
		identity := identityLabels(e.labels, e.meta, e.parsed)
		identity = upsertLabel(identity, errorLabel, e.err)
		if e.errDetails != "" {
			identity = upsertLabel(identity, errorDetailsLabel, e.errDetails)
		}
		return identity
	}
	groups, without, noLabels := metricGrouping(g, unwrapLabel)
	// GroupedLabels checks noLabels before the unchanged and group-list paths:
	// an explicit by() (Grouping.Singleton) always yields an empty identity.
	if noLabels {
		return nil
	}
	if !e.builderMutated {
		if len(groups) == 0 && !without {
			return slices.Clone(e.base)
		}
		return baseGroupLabels(e.base, groups, without)
	}
	if without {
		return withoutGroupLabels(identityLabels(e.labels, e.meta, e.parsed), groups)
	}
	if len(groups) > 0 {
		return withGroupLabels(e, groups)
	}
	identity := identityLabels(e.labels, e.meta, e.parsed)
	if e.errDetails != "" {
		identity = upsertLabel(identity, errorDetailsLabel, e.errDetails)
	}
	return identity
}

// metricGrouping converts an aggregation grouping into the extractor's
// effective group list and noLabels flag, mirroring RangeAggregationExpr's
// extractor: noLabels is Grouping.Singleton(), true exactly for an explicit
// by() (empty group list, not without). The noLabels check happens before the
// group lists in GroupedLabels, so an explicit by() yields an empty identity.
// A nil grouping (and without) makes the unwrap extractor append its value
// label to the without list, the implicit "remove the value label" rule.
func metricGrouping(g *Grouping, unwrapLabel string) (groups []string, without, noLabels bool) {
	if g != nil {
		groups = slices.Clone(g.Names)
		without = g.Without
		noLabels = !g.Without && len(g.Names) == 0
	}
	if !noLabels && unwrapLabel != "" && (len(groups) == 0 || without) {
		without = true
		groups = append(groups, unwrapLabel)
	}
	return groups, without, noLabels
}

// metricProjectedIdentity applies the enclosing vector aggregation's injected
// by-grouping to a bucket entry, on top of its exact ungrouped identity. A nil
// group keeps that identity; an empty group projects to no labels; a non-empty
// group follows GroupedLabels itself: an unchanged builder groups the base
// labels (toBaseGroup, keeping an empty value), while a modified builder uses
// withResult and never promotes the builder error-details state (even when
// named). A real label with that name remains eligible unless Del removed it.
func metricProjectedIdentity(e *logEntry, group []string) logs.Labels {
	switch {
	case group == nil:
		return metricBuilderIdentity(e, nil, "")
	case len(group) == 0:
		return nil
	case !e.builderMutated:
		return baseGroupLabels(e.base, group, false)
	default:
		return withGroupLabels(e, group)
	}
}

// baseGroupLabels is LabelsBuilder.toBaseGroup: the unchanged fast path's
// by/without over the base labels. Empty values are kept, unlike withResult.
func baseGroupLabels(base logs.Labels, groups []string, without bool) logs.Labels {
	kept := make(logs.Labels, 0, len(base))
	if without {
		for _, l := range base {
			if !slices.Contains(groups, l.Name) {
				kept = append(kept, l)
			}
		}
		return kept
	}
	for _, l := range base {
		if slices.Contains(groups, l.Name) {
			kept = append(kept, l)
		}
	}
	return kept
}

// withGroupLabels is LabelsBuilder.withResult: only the requested names, each
// resolved through the entry's categories. An empty stream-label value counts
// as missing, the builder error state is not a label category, and Loki checks
// the Del list before additions, so a name deleted and later re-added stays
// hidden from a by grouping.
func withGroupLabels(e *logEntry, groups []string) logs.Labels {
	kept := make(logs.Labels, 0, len(groups))
	for _, name := range groups {
		if _, deleted := e.builderDeleted[name]; deleted {
			continue
		}
		if v, _, ok := e.get(name); ok {
			kept = append(kept, logs.Label{Name: name, Value: v})
		}
	}
	return logs.New(kept...)
}

// withoutGroupLabels is LabelsBuilder.withoutResult: every effective label
// except the group names. Empty values are kept.
func withoutGroupLabels(ls logs.Labels, groups []string) logs.Labels {
	kept := make(logs.Labels, 0, len(ls))
	for _, l := range ls {
		if !slices.Contains(groups, l.Name) {
			kept = append(kept, l)
		}
	}
	return kept
}

// bucketProjection is a pipeline of compiled label-only drop/keep stages
// applied to Aggregate buckets. It reuses A6's compiled stages, so drop/keep
// semantics are identical to the Select path, and builds the bucket entry
// through the same addEntryMetadata helper as a stored row.
type bucketProjection struct {
	stages []pipelineStage
}

// apply returns one bucket's metric identity after the projection and the
// enclosing vector aggregation's injected grouping. A6's drop/keep stages
// cancel through ctx and never reject a bucket; the bucket's stream labels and
// metadata are cloned because Buckets may share backing arrays between yields.
func (p bucketProjection) apply(ctx context.Context, b plan.Bucket, group []string) (logs.Labels, error) {
	e := &logEntry{
		labels: slices.Clone(b.Labels),
		// base is only read (baseHas) and must stay the immutable stored label
		// set; aliasing the bucket's slice avoids a second clone per bucket.
		base: b.Labels,
	}
	addEntryMetadata(e, b.Meta)
	for _, st := range p.stages {
		if _, err := st.process(ctx, e); err != nil {
			return nil, err
		}
	}
	// A metadata label literally named __error__ feeds the builder error state
	// (addEntryMetadata), so even a parser-free pipeline must fail unless a
	// drop cleared it. Loki checks the metric after the whole pipeline, and its
	// HasErr path ignores any grouping.
	if e.err != "" {
		return nil, metricPipelineErr(e, metricBuilderIdentity(e, nil, ""))
	}
	return metricProjectedIdentity(e, group), nil
}

// compileAggregatePipeline compiles a range pipeline for the Aggregate fast
// path. It succeeds only when every stage is a line filter plan.Selection can
// express, or a label-only drop/keep: such a pipeline cannot reject a row or
// change the line, so count and bytes totals are unaffected and only the final
// label set changes. Parsers, label filters, formatting and unwrap force the
// Select path, whose rows carry the parsed labels Aggregate cannot see.
func compileAggregatePipeline(r *LogRange) (bucketProjection, []plan.LineFilter, bool, error) {
	var (
		proj    bucketProjection
		filters []plan.LineFilter
	)
	for _, st := range r.Pipeline.Stages {
		switch st := st.(type) {
		case *LineFilter:
			m, err := compileLineFilter(st)
			if err != nil {
				return bucketProjection{}, nil, false, err
			}
			fs, ok := pushableLineFilters(m)
			if !ok {
				return bucketProjection{}, nil, false, nil
			}
			filters = append(filters, fs...)
		case *Drop, *Keep:
			ps, err := compileStage(st)
			if err != nil {
				return bucketProjection{}, nil, false, err
			}
			proj.stages = append(proj.stages, ps)
		default:
			return bucketProjection{}, nil, false, nil
		}
	}
	return proj, filters, true, nil
}

// compareLabels orders label sets lexicographically by their canonical form.
func compareLabels(a, b logs.Labels) int {
	return strings.Compare(a.String(), b.String())
}
