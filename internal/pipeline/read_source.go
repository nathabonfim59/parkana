package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// NewReadSource wraps raw so the tenant's read pipelines (PLAN.md §6) run over
// every row a query returns, through query's WrapSource hook. The wrapper keeps
// plan.Source semantics: with no read pipelines for the tenant it delegates the
// exact original operation and arguments; with any read pipeline it takes the
// conservative tenant-wide transformed path — the raw Select keeps the
// tenant, time range, label matchers and direction but loses every line filter
// and the limit (so sidecars can never prune a row a rewrite would match), the
// matching pipelines run per row in name order against the row's original
// dataset, then the original line filters and limit apply to the transformed
// rows. Aggregate never delegates on that path: it groups transformed rows
// itself, counting each surviving raw row once and never deduplicating
// rewritten rows that collide.
//
// streamLabels are the configured routing labels (config.StreamLabels), the
// same priority order storage uses; they are validated and cloned.
func NewReadSource(raw plan.Source, provider SnapshotProvider, streamLabels []string) (plan.Source, error) {
	if raw == nil {
		return nil, errors.New("pipeline: NewReadSource requires a raw plan.Source")
	}
	if provider == nil {
		return nil, errors.New("pipeline: NewReadSource requires a SnapshotProvider")
	}
	if len(streamLabels) == 0 {
		return nil, errors.New("pipeline: NewReadSource requires at least one stream label")
	}
	labels := slices.Clone(streamLabels)
	seen := make(map[string]struct{}, len(labels))
	for _, name := range labels {
		if err := validLabelNameErr(name); err != nil {
			return nil, fmt.Errorf("pipeline: stream label: %w", err)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("pipeline: duplicate stream label %q", name)
		}
		seen[name] = struct{}{}
	}
	return &readSource{raw: raw, provider: provider, streamLabels: labels}, nil
}

// readSource is the plan.Source wrapper NewReadSource returns. It is immutable
// after construction and safe for concurrent use.
type readSource struct {
	raw          plan.Source
	provider     SnapshotProvider
	streamLabels []string
}

var _ plan.Source = (*readSource)(nil)

// loadSnapshot loads id's snapshot with the wrapper's standard checks: a
// canceled context is an error before the provider runs, a provider failure and
// a nil snapshot are errors, and a context canceled during the load is an error
// too, so a canceled query can never continue on a good snapshot.
func loadSnapshot(ctx context.Context, provider SnapshotProvider, id tenant.ID) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: read: %w", err)
	}
	snap, err := provider.Snapshot(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("pipeline: read: snapshot tenant %q: %w", id, err)
	}
	if snap == nil {
		return nil, errors.New("pipeline: read: SnapshotProvider returned a nil snapshot")
	}
	// The provider may ignore cancellation; never report success for a canceled
	// query, including the no-read-pipeline pass-through.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: read: %w", err)
	}
	return snap, nil
}

// snapshotFor resolves the snapshot for one Select or Aggregate iteration,
// exactly once per operation. When WrapEngine installed a per-query holder in
// ctx, the holder's lazily loaded snapshot is used, pinning one snapshot for
// the whole query; a holder pinned to a different tenant is an execution error
// rather than a silent second snapshot load, so one query can never observe
// two tenants' snapshots. Without a holder the provider is loaded directly.
func (s *readSource) snapshotFor(ctx context.Context, id tenant.ID) (Snapshot, error) {
	h, _ := ctx.Value(readCtxKey{}).(*readSnapshot)
	if h == nil {
		return loadSnapshot(ctx, s.provider, id)
	}
	if h.tenant != id {
		return nil, fmt.Errorf("pipeline: read: query snapshot is pinned to tenant %q, cannot read tenant %q", h.tenant, id)
	}
	snap, err := h.load(ctx)
	if err != nil {
		return nil, err
	}
	// The load may be a cached result from an earlier Source call of the same
	// query; this caller still checks its own context before proceeding.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pipeline: read: %w", err)
	}
	return snap, nil
}

// Select implements plan.Source.
func (s *readSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Row{}, err)
			return
		}
		snap, err := s.snapshotFor(ctx, sel.Tenant)
		if err != nil {
			yield(plan.Row{}, err)
			return
		}
		if !snap.Has(StageRead) {
			// No read pipelines anywhere for the tenant: the exact original
			// operation and arguments, unmodified.
			for r, err := range s.raw.Select(ctx, sel) {
				if err != nil {
					yield(plan.Row{}, err)
					return
				}
				if !yield(r, nil) {
					return
				}
			}
			return
		}
		s.rawSelect(ctx, snap, sel, yield)
	}
}

// rawSelect is Select's transformed path: raw rows without line filters or
// limit, pipelines then filters then limit over the transformed rows. yield is
// the caller's; raw is the wrapped source.
func (s *readSource) rawSelect(ctx context.Context, snap Snapshot, sel plan.Selection, yield func(plan.Row, error) bool) {
	// The conservative tenant-wide scan: keep tenant, time range, matchers and
	// direction, drop every line filter and the limit. Filters must see the
	// transformed line, and dropped rows must not consume the limit.
	raw := sel
	raw.LineFilters = nil
	raw.Limit = 0
	matches := make(map[string][]*Compiled)
	emitted := 0
	for r, err := range s.raw.Select(ctx, raw) {
		if err != nil {
			yield(plan.Row{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(plan.Row{}, fmt.Errorf("pipeline: read: %w", err))
			return
		}
		out, keep, err := s.transformRow(ctx, snap, matches, r)
		if err != nil {
			yield(plan.Row{}, err)
			return
		}
		// A pipeline can only observe cancellation at a step boundary, so a
		// transform may return successfully after ctx was canceled (including
		// for a row whose dataset matches no pipeline). Never yield that row.
		if err := ctx.Err(); err != nil {
			yield(plan.Row{}, fmt.Errorf("pipeline: read: %w", err))
			return
		}
		if !keep || !lineMatches(sel.LineFilters, out.Line) {
			continue
		}
		if !yield(out, nil) {
			return
		}
		emitted++
		if sel.Limit > 0 && emitted >= sel.Limit {
			return
		}
	}
	// A canceled context must not report success, including when the scan
	// matched no rows at all.
	if err := ctx.Err(); err != nil {
		yield(plan.Row{}, fmt.Errorf("pipeline: read: %w", err))
	}
}

// Aggregate implements plan.Source.
func (s *readSource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if err := agg.Validate(); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		snap, err := s.snapshotFor(ctx, sel.Tenant)
		if err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if !snap.Has(StageRead) {
			for b, err := range s.raw.Aggregate(ctx, sel, agg) {
				if err != nil {
					yield(plan.Bucket{}, err)
					return
				}
				if !yield(b, nil) {
					return
				}
			}
			return
		}
		s.rawAggregate(ctx, snap, sel, agg, yield)
	}
}

// rawAggregate is Aggregate's transformed path. It never calls raw Aggregate
// (that would pre-group untransformed rows); it scans raw rows without line
// filters or limit, transforms and filters them, and groups by
// BucketAgg.Start(row.TS), the immutable stream labels and, when asked, the
// transformed metadata. Count increments once per surviving raw row — the raw
// source already removed stored duplicates, and rows a rewrite makes equal
// stay distinct — and Bytes sums the transformed line's byte length.
func (s *readSource) rawAggregate(ctx context.Context, snap Snapshot, sel plan.Selection, agg plan.BucketAgg, yield func(plan.Bucket, error) bool) {
	raw := sel
	raw.LineFilters = nil
	raw.Limit = 0
	type key struct {
		start        int64
		labels, meta string
	}
	sums := make(map[key]*bucketAccum)
	matches := make(map[string][]*Compiled)
	for r, err := range s.raw.Select(ctx, raw) {
		if err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(plan.Bucket{}, fmt.Errorf("pipeline: read: %w", err))
			return
		}
		out, keep, err := s.transformRow(ctx, snap, matches, r)
		if err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		// Check after the transform and before committing the row to a bucket:
		// a canceled query must not count (or later yield) a row a pipeline
		// finished transforming after cancellation, even when no pipeline
		// matched the row's dataset.
		if err := ctx.Err(); err != nil {
			yield(plan.Bucket{}, fmt.Errorf("pipeline: read: %w", err))
			return
		}
		if !keep || !lineMatches(sel.LineFilters, out.Line) {
			continue
		}
		start := agg.Start(out.TS)
		k := key{start: start.UnixMicro(), labels: out.Labels.String()}
		if agg.IncludeMeta {
			k.meta = out.Meta.String()
		}
		b := sums[k]
		if b == nil {
			// The raw source may reuse row label/meta backing arrays between
			// yields, and even untouched rows are retained here: the buckets
			// must own their slices.
			b = &bucketAccum{Start: start, Labels: slices.Clone(out.Labels)}
			if agg.IncludeMeta {
				b.Meta = slices.Clone(out.Meta)
			}
			sums[k] = b
		}
		b.Count++
		b.Bytes += int64(len(out.Line))
	}
	if err := ctx.Err(); err != nil {
		yield(plan.Bucket{}, fmt.Errorf("pipeline: read: %w", err))
		return
	}
	out := make([]plan.Bucket, 0, len(sums))
	for _, b := range sums {
		out = append(out, b.Bucket)
	}
	// The order is plan.MemSource's, part of the Source contract: direct
	// Labels.String / Meta.String comparisons put the empty metadata set "{}"
	// after every non-empty set.
	slices.SortFunc(out, func(a, b plan.Bucket) int {
		return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Labels.String(), b.Labels.String()),
			cmp.Compare(a.Meta.String(), b.Meta.String()))
	})
	for _, b := range out {
		if err := ctx.Err(); err != nil {
			yield(plan.Bucket{}, fmt.Errorf("pipeline: read: %w", err))
			return
		}
		if !yield(b, nil) {
			return
		}
	}
	if err := ctx.Err(); err != nil {
		yield(plan.Bucket{}, fmt.Errorf("pipeline: read: %w", err))
	}
}

// bucketAccum is one aggregate bucket under construction. plan.Bucket is
// embedded so the accumulated value converts directly.
type bucketAccum struct {
	plan.Bucket
}

// transformRow runs r through the read pipelines matching its original dataset
// and reports the row to keep. Rows whose dataset matches no pipeline pass
// through untouched (Apply is not run, so their slices still alias the raw
// row). matched caches snap.For per dataset for the lifetime of one iteration;
// a Snapshot is immutable, so the cached slices stay valid.
func (s *readSource) transformRow(ctx context.Context, snap Snapshot, matched map[string][]*Compiled, r plan.Row) (plan.Row, bool, error) {
	dataset := r.Labels.StreamName(s.streamLabels)
	pipelines, ok := matched[dataset]
	if !ok {
		pipelines = snap.For(StageRead, dataset)
		matched[dataset] = pipelines
	}
	if len(pipelines) == 0 {
		return r, true, nil
	}
	out, drop, err := applyRead(ctx, pipelines, dataset, Entry{
		TS:     r.TS,
		Line:   r.Line,
		Labels: r.Labels,
		Meta:   r.Meta,
	})
	if err != nil {
		return plan.Row{}, false, err
	}
	if drop {
		return plan.Row{}, false, nil
	}
	return plan.Row{TS: out.TS, Labels: out.Labels, Line: out.Line, Meta: out.Meta}, true, nil
}

// applyRead runs one row's entry through the matched pipelines in order. A
// drop removes the row; a stop keeps the error-marked entry and halts the
// remaining pipelines (core Apply already restored the pre-step state and
// wrote pipeline_error/pipeline_error_details). An explicit on_error reject
// becomes a query execution error; every other error (cancellation, an
// internal fault) propagates with errors.Is intact.
func applyRead(ctx context.Context, matched []*Compiled, dataset string, in Entry) (Entry, bool, error) {
	for _, c := range matched {
		next, outcome, err := c.Apply(ctx, dataset, in)
		if err != nil {
			if rej, ok := errors.AsType[*RejectError](err); ok {
				return Entry{}, false, fmt.Errorf("pipeline: read pipeline %q rejected the query: %w", c.Name(), rej)
			}
			return Entry{}, false, fmt.Errorf("pipeline: read: apply %q: %w", c.Name(), err)
		}
		in = next
		switch outcome {
		case OutcomeDrop:
			return Entry{}, true, nil
		case OutcomeStop:
			return in, false, nil
		}
	}
	return in, false, nil
}

// lineMatches reports whether line passes every filter.
func lineMatches(filters []plan.LineFilter, line string) bool {
	for _, f := range filters {
		if !f.Matches(line) {
			return false
		}
	}
	return true
}
