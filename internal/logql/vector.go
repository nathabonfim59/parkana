// Vector aggregation evaluation: sum, avg, min, max, count, stddev and stdvar
// with by/without grouping, plus topk, bottomk, sort and sort_desc.
//
// A vector aggregation evaluates its child at every step and then combines the
// samples present at that step. The child is either a range aggregation (the
// common case) or another vector aggregation; both are evaluated through the
// recursive series seam, which returns every child series with its own points
// and applies no limits, so the enclosing expression owns Loki's final
// response limit (JoinSampleVector). Each step is aggregated independently:
// groups that have no sample at a timestamp produce no point, never a zero.
//
// The grouping and the arithmetic mirror Loki 3.7.9's VectorAggEvaluator
// (pkg/logql/evaluator.go) and its heaps (pkg/logql/vector.go):
//
//   - by (...) keeps only the named labels that exist on the sample; a missing
//     label stays missing.
//   - without (...) removes the named labels and __name__.
//   - sum/avg/min/max/count/stddev/stdvar replace the series identity with the
//     group labels; topk/bottomk/sort/sort_desc keep each selected sample's
//     full labels.
//   - avg and stddev/stdvar use Loki's incremental mean (Welford); the variance
//     is the population variance (divided by the number of samples).
//   - max/min let a NaN keep whatever value the group already had, and only
//     replace a value that is itself NaN.
//   - topk/bottomk bound k by the input length before converting it to an int,
//     so a parameter that rounds to 2^63 cannot overflow or allocate.
//
// sort and sort_desc order the instant vector by value. Loki skips the usual
// label sort for any expression containing them (evaluator.go Sortable), so
// evalVectorAgg reapplies that order after the shared finishMetric shaping.
package logql

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// metricNameLabel is Prometheus/Loki's metric name label; `without` deletes it
// from the result identity (VectorAggEvaluator lb.Del(model.MetricNameLabel)).
const metricNameLabel = "__name__"

// evalVectorAgg evaluates a vector aggregation and shapes the final response.
// It is the top-level entry point from evalMetric; the series limit and the
// instant/range shaping happen once, here, over the assembled result.
func (e *Engine) evalVectorAgg(ctx context.Context, src plan.Source, a *VectorAgg, p plan.Params) (plan.Result, error) {
	series, err := e.evalVectorSeries(ctx, src, a, p)
	if err != nil {
		return plan.Result{}, err
	}
	res, err := e.finishMetric(series, p)
	if err != nil {
		return plan.Result{}, err
	}
	// Loki reports a sort/sort_desc instant result in value order instead of
	// label order; the range matrix is label-sorted like every other metric.
	if p.Instant() && (a.Func == Sort || a.Func == SortDesc) {
		slices.SortStableFunc(res.Vector, func(x, y plan.Sample) int {
			return compareVectorValues(x.Point.V, y.Point.V, a.Func == SortDesc)
		})
	}
	return res, nil
}

// evalVectorSeries is the recursive seam for vector aggregations: it returns
// every output series with its points, without enforcing MaxSeries or shaping
// the response. An enclosing expression applies the limit to its final result,
// and a nested vector aggregation consumes these series directly.
func (e *Engine) evalVectorSeries(ctx context.Context, src plan.Source, a *VectorAgg, p plan.Params) ([]*metricSeries, error) {
	es, err := e.evalVectorSeriesGrid(ctx, src, a, p)
	if err != nil {
		return nil, err
	}
	return es.series, nil
}

// evalVectorSeriesGrid is evalVectorSeries with the evaluation grid its parent
// needs: a vector aggregation inherits its child's step sequence, so a nested
// vector(NUMBER) keeps its own millisecond spacing and count.
func (e *Engine) evalVectorSeriesGrid(ctx context.Context, src plan.Source, a *VectorAgg, p plan.Params) (*evalSeries, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: vector aggregation: %w", err)
	}
	if src == nil {
		return nil, errors.New("logql: nil source")
	}
	// The request grid is still validated for every family: negative steps and
	// spans beyond the point bound are client errors even when the child is a
	// vector literal with its own grid.
	if _, err := metricEvalTimes(p); err != nil {
		return nil, err
	}
	if a.Func == TopK || a.Func == BottomK {
		// The parser rejects a missing or non-positive parameter; guard anyway
		// so a hand-built AST cannot index an empty slice. Loki evaluates the
		// child first and then returns an empty vector for k < 1; a dropped
		// parameter is a programming error, not a valid query.
		if len(a.Params) != 1 {
			return nil, fmt.Errorf("logql: %w: %s requires one parameter", plan.ErrBadQuery, a.Func)
		}
	}
	grid, inputs, err := e.vectorInputsGrid(ctx, src, a, p)
	if err != nil {
		return nil, err
	}
	series, err := vectorApply(ctx, a, inputs, grid.times)
	if err != nil {
		return nil, err
	}
	if err := tagSeries(series, grid); err != nil {
		return nil, err
	}
	return &evalSeries{grid: grid, series: series}, nil
}

// vectorInput is one child series: its full identity and the child value at the
// evaluation steps where it has a sample. A sparse map keeps absent steps
// absent, so no aggregation invents a zero sample.
type vectorInput struct {
	labels logs.Labels
	values map[int]float64
}

// vectorInputsGrid evaluates the child expression and returns the grid its
// steps belong to plus those steps indexed for aggregation. A direct range
// child can push a sum's grouping into the count/rate/bytes Aggregate fast
// path, or into sum_over_time's Select extractor via rangeOptions.group
// (canInjectVectorGrouping); every other shape keeps the child's own grid (so
// a nested vector(NUMBER) can have more or fewer steps than the request grid).
func (e *Engine) vectorInputsGrid(ctx context.Context, src plan.Source, a *VectorAgg, p plan.Params) (*evalGrid, []vectorInput, error) {
	switch child := a.Left.(type) {
	case *RangeAgg:
		times, err := metricEvalTimes(p)
		if err != nil {
			return nil, nil, err
		}
		opts := rangeOptions{aggregate: true, child: true}
		if group, ok := injectVectorGrouping(a, child); ok {
			opts.group = group
		}
		series, err := e.evalRangeSeries(ctx, src, child, p, opts)
		if err != nil {
			return nil, nil, err
		}
		inputs, err := indexVectorPoints(times, series)
		if err != nil {
			return nil, nil, err
		}
		return rangeGrid(times, child.Range.Offset), inputs, nil
	case *VectorAgg, *VectorLiteral, *BinaryExpr:
		es, err := e.evalVectorSeriesExpr(ctx, src, a.Left, p)
		if err != nil {
			return nil, nil, err
		}
		inputs, err := indexVectorPoints(es.grid.times, es.series)
		if err != nil {
			return nil, nil, err
		}
		return es.grid, inputs, nil
	case *NumberLit:
		// Loki's vector aggregation evaluator has no step evaluator for a
		// bare literal child; only vector-typed operands are consumable.
		return nil, nil, fmt.Errorf("logql: %w: aggregation over a scalar expression is not supported", plan.ErrUnsupported)
	case *deferredExpr:
		return nil, nil, fmt.Errorf("logql: %w: %s is not supported", plan.ErrUnsupported, child.what)
	default:
		return nil, nil, fmt.Errorf("logql: %w: unexpected vector aggregation argument %T", plan.ErrUnsupported, a.Left)
	}
}

// injectVectorGrouping reports the label projection an enclosing sum can push
// into a direct range child. Loki does this in VectorAggregationExpr.Extractors
// (pkg/logql/syntax/ast.go): only sum pushes down, only into the linear
// count/rate/bytes and sum_over_time range functions
// (canInjectVectorGrouping), and only when the range itself has no grouping.
// The projection is purely an optimization - a nested sum regroups the same
// labels afterwards - so it must not change the result.
//
// A `by` grouping and no grouping are expressible as a keep-list; `without`
// needs a drop-list that rangeOptions.group cannot represent, so it is left to
// the enclosing vector aggregation. That is equivalent, just less selective.
func injectVectorGrouping(parent *VectorAgg, child *RangeAgg) ([]string, bool) {
	if parent.Func != Sum || child.Grouping != nil || !canInjectVectorGrouping(child.Func) {
		return nil, false
	}
	g := parent.Grouping
	if g == nil {
		// sum(...) is by () - one group with no labels.
		return []string{}, true
	}
	if g.Without {
		return nil, false
	}
	return slices.Clone(g.Names), true
}

// canInjectVectorGrouping mirrors Loki's canInjectVectorGrouping: the linear
// range operations whose extractor identity a sum may replace.
func canInjectVectorGrouping(fn RangeFunc) bool {
	switch fn {
	case BytesOverTime, BytesRate, SumOverTime, Rate, CountOverTime:
		return true
	}
	return false
}

// indexVectorPoints turns child series into per-step inputs. A tagged series
// carries its step indices directly, so children with a different grid (a
// nested vector literal's millisecond grid) never join by timestamp. An
// untagged hand-built series falls back to matching its nominal time against
// the supplied grid; a mismatch is an engine bug, never an approximation.
func indexVectorPoints(times []time.Time, series []*metricSeries) ([]vectorInput, error) {
	inputs := make([]vectorInput, 0, len(series))
	for _, s := range series {
		if len(s.points) == 0 {
			continue
		}
		values := make(map[int]float64, len(s.points))
		if len(s.steps) == len(s.points) {
			for i, pt := range s.points {
				step := s.steps[i]
				if step < 0 || step >= len(times) {
					return nil, fmt.Errorf("logql: vector aggregation: sample step %d is outside the %d-step grid", step, len(times))
				}
				values[step] = pt.V
			}
		} else {
			for _, pt := range s.points {
				i, ok := slices.BinarySearchFunc(times, pt.T, func(x, target time.Time) int {
					return x.Compare(target)
				})
				if !ok {
					return nil, fmt.Errorf("logql: vector aggregation: sample at %s is not an evaluation time", pt.T)
				}
				values[i] = pt.V
			}
		}
		inputs = append(inputs, vectorInput{labels: s.labels, values: values})
	}
	return inputs, nil
}

// vectorApply dispatches on the aggregation. It is pure apart from ctx so the
// arithmetic and ordering can be tested directly with non-finite values that a
// log source cannot produce.
func vectorApply(ctx context.Context, a *VectorAgg, inputs []vectorInput, times []time.Time) ([]*metricSeries, error) {
	switch a.Func {
	case Sum, Avg, Min, Max, Count, Stddev, Stdvar:
		return vectorAggregate(ctx, a, inputs, times)
	case TopK, BottomK:
		return vectorTopK(ctx, a, inputs, times)
	case Sort, SortDesc:
		return vectorSort(ctx, a, inputs, times)
	default:
		return nil, fmt.Errorf("logql: %w: unknown vector aggregation %d", plan.ErrUnsupported, a.Func)
	}
}

// vectorGrouping returns the group identity of one input series. A nil or empty
// by() grouping is the singleton group with no labels. Loki builds the same
// labels by hashing/keeping the listed names, and a without grouping also
// deletes __name__. Missing names are absent from the result; no empty value is
// invented.
func vectorGrouping(ls logs.Labels, g *Grouping) logs.Labels {
	if g == nil || !g.Without {
		if g == nil || len(g.Names) == 0 {
			return nil
		}
		kept := make(logs.Labels, 0, min(len(g.Names), len(ls)))
		for _, l := range ls {
			if slices.Contains(g.Names, l.Name) {
				kept = append(kept, l)
			}
		}
		return logs.New(kept...)
	}
	kept := make(logs.Labels, 0, len(ls))
	for _, l := range ls {
		if l.Name == metricNameLabel || slices.Contains(g.Names, l.Name) {
			continue
		}
		kept = append(kept, l)
	}
	return logs.New(kept...)
}

// vectorGroupAcc accumulates one by/without group at one step. The field use
// mirrors Loki's groupedAggregation: value holds sum/min/max/stdvar's running
// value, mean is the incremental mean for avg and Welford's stddev/stdvar, and
// count is the group's sample count.
type vectorGroupAcc struct {
	labels      logs.Labels
	value, mean float64
	count       int64
}

// add folds one sample into the group's running state.
func (g *vectorGroupAcc) add(fn VectorFunc, f float64) {
	switch fn {
	case Sum:
		g.value += f
	case Avg:
		// Incremental mean, as Loki's group.mean update.
		g.count++
		g.mean += (f - g.mean) / float64(g.count)
	case Max:
		if g.value < f || math.IsNaN(g.value) {
			g.value = f
		}
	case Min:
		if g.value > f || math.IsNaN(g.value) {
			g.value = f
		}
	case Count:
		g.count++
	case Stddev, Stdvar:
		// Welford: delta is measured against the old mean, then M2 accumulates
		// delta*(f-newMean).
		g.count++
		delta := f - g.mean
		g.mean += delta / float64(g.count)
		g.value += delta * (f - g.mean)
	}
}

// init seeds a group with its first sample. Loki initializes stddev/stdvar's
// running M2 to zero; every other aggregation starts from the sample value.
func (g *vectorGroupAcc) init(fn VectorFunc, labels logs.Labels, f float64) {
	g.labels, g.mean, g.count = labels, f, 1
	if fn != Stddev && fn != Stdvar {
		g.value = f
	}
}

// result converts the running state to the sample value. Variance is the
// population variance (M2/n), matching VectorAggEvaluator's final division.
func (g *vectorGroupAcc) result(fn VectorFunc) float64 {
	switch fn {
	case Avg:
		return g.mean
	case Count:
		return float64(g.count)
	case Stddev:
		return math.Sqrt(g.value / float64(g.count))
	case Stdvar:
		return g.value / float64(g.count)
	default:
		return g.value
	}
}

// vectorAggregate evaluates the aggregations that replace the input identity
// with the grouped identity.
func vectorAggregate(ctx context.Context, a *VectorAgg, inputs []vectorInput, times []time.Time) ([]*metricSeries, error) {
	groups := make([]logs.Labels, len(inputs))
	keys := make([]string, len(inputs))
	for i, in := range inputs {
		groups[i] = vectorGrouping(in.labels, a.Grouping)
		keys[i] = groups[i].String()
	}
	set := &metricSet{}
	keyed := make(map[string]int, len(inputs))
	accs := make([]vectorGroupAcc, 0, len(inputs))
	for step, at := range times {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: vector aggregation: %w", err)
		}
		accs = accs[:0]
		clear(keyed)
		for i := range inputs {
			f, ok := inputs[i].values[step]
			if !ok {
				continue
			}
			idx, ok := keyed[keys[i]]
			if !ok {
				keyed[keys[i]] = len(accs)
				accs = append(accs, vectorGroupAcc{})
				accs[len(accs)-1].init(a.Func, groups[i], f)
				continue
			}
			accs[idx].add(a.Func, f)
		}
		for i := range accs {
			// Recheck between groups so a canceled request stops even when one
			// step has many groups.
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: vector aggregation: %w", err)
			}
			acc := &accs[i]
			ms := set.get(acc.labels)
			ms.points = append(ms.points, plan.Point{T: at, V: acc.result(a.Func)})
		}
	}
	return set.series, nil
}

// vectorSample is one full-identity sample in a topk/bottomk/sort heap.
type vectorSample struct {
	value  float64
	labels logs.Labels
}

// vectorValueHeap is Loki's vectorByValueHeap/vectorByReverseValueHeap: a NaN
// compares as the smallest value regardless of direction, so it always sits at
// the heap root when present. descending picks the reverse heap.
type vectorValueHeap struct {
	samples    []vectorSample
	descending bool
}

func (h *vectorValueHeap) Len() int { return len(h.samples) }

func (h *vectorValueHeap) Less(i, j int) bool {
	if math.IsNaN(h.samples[i].value) {
		return true
	}
	if h.descending {
		return h.samples[i].value > h.samples[j].value
	}
	return h.samples[i].value < h.samples[j].value
}

func (h *vectorValueHeap) Swap(i, j int) { h.samples[i], h.samples[j] = h.samples[j], h.samples[i] }

func (h *vectorValueHeap) Push(x any) { h.samples = append(h.samples, x.(vectorSample)) }

func (h *vectorValueHeap) Pop() any {
	old := h.samples
	n := len(old)
	v := old[n-1]
	h.samples = old[:n-1]
	return v
}

// vectorTopK evaluates topk and bottomk. Each group keeps its k highest (or
// lowest) samples by full identity; the instant response is label-sorted by
// finishMetric because Loki's Sortable only recognizes sort/sort_desc.
func vectorTopK(ctx context.Context, a *VectorAgg, inputs []vectorInput, times []time.Time) ([]*metricSeries, error) {
	if len(a.Params) != 1 {
		return nil, fmt.Errorf("logql: %w: %s requires one parameter", plan.ErrBadQuery, a.Func)
	}
	k := a.Params[0]
	if math.IsNaN(k) || k < 1 {
		return nil, nil
	}
	groups := make([]logs.Labels, len(inputs))
	keys := make([]string, len(inputs))
	for i, in := range inputs {
		groups[i] = vectorGrouping(in.labels, a.Grouping)
		keys[i] = groups[i].String()
	}
	set := &metricSet{}
	parts := make(map[string][]vectorSample, len(inputs))
	order := make([]string, 0, len(inputs))
	for step, at := range times {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: vector aggregation: %w", err)
		}
		clear(parts)
		order = order[:0]
		for i := range inputs {
			f, ok := inputs[i].values[step]
			if !ok {
				continue
			}
			if _, ok := parts[keys[i]]; !ok {
				order = append(order, keys[i])
			}
			parts[keys[i]] = append(parts[keys[i]], vectorSample{value: f, labels: inputs[i].labels})
		}
		for _, key := range order {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: vector aggregation: %w", err)
			}
			samples := vectorTopKPartition(a.Func, k, parts[key])
			for _, s := range samples {
				ms := set.get(s.labels)
				ms.points = append(ms.points, plan.Point{T: at, V: s.value})
			}
		}
	}
	return set.series, nil
}

// vectorTopKPartition keeps k samples from one partition, reproducing Loki's
// heap conditions exactly: a new sample replaces the root only when the root is
// smaller (topk) or larger (bottomk), or when the root is NaN.
func vectorTopKPartition(fn VectorFunc, k float64, samples []vectorSample) []vectorSample {
	size := topKSize(k, len(samples))
	if size == 0 {
		return nil
	}
	h := &vectorValueHeap{
		descending: fn == BottomK,
		samples:    make([]vectorSample, 0, size),
	}
	for _, s := range samples {
		root := math.NaN()
		if h.Len() == size {
			root = h.samples[0].value
		}
		if fn == BottomK {
			if h.Len() < size || root > s.value || math.IsNaN(root) {
				if h.Len() == size {
					heap.Pop(h)
				}
				heap.Push(h, s)
			}
			continue
		}
		if h.Len() < size || root < s.value || math.IsNaN(root) {
			if h.Len() == size {
				heap.Pop(h)
			}
			heap.Push(h, s)
		}
	}
	// Loki sorts the reversed heap: descending for topk, ascending for bottomk;
	// NaN sorts last in both orders.
	slices.SortFunc(h.samples, func(x, y vectorSample) int {
		return compareVectorValues(x.value, y.value, fn == TopK)
	})
	return h.samples
}

// topKSize bounds k by the input size before any conversion to int, so a huge
// (or 2^63-rounded) parameter can never overflow or allocate more than the
// input.
func topKSize(k float64, n int) int {
	if math.IsNaN(k) || k < 1 {
		return 0
	}
	if k >= float64(n) {
		return n
	}
	return int(k)
}

// vectorSort evaluates sort and sort_desc. Grouping can only be empty by/without
// (the parser rejects a non-empty grouping); each partition emits all its
// samples in value order, keeping the full labels.
func vectorSort(ctx context.Context, a *VectorAgg, inputs []vectorInput, times []time.Time) ([]*metricSeries, error) {
	groups := make([]logs.Labels, len(inputs))
	keys := make([]string, len(inputs))
	for i, in := range inputs {
		groups[i] = vectorGrouping(in.labels, a.Grouping)
		keys[i] = groups[i].String()
	}
	set := &metricSet{}
	parts := make(map[string][]vectorSample, len(inputs))
	order := make([]string, 0, len(inputs))
	for step, at := range times {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: vector aggregation: %w", err)
		}
		clear(parts)
		order = order[:0]
		for i := range inputs {
			f, ok := inputs[i].values[step]
			if !ok {
				continue
			}
			if _, ok := parts[keys[i]]; !ok {
				order = append(order, keys[i])
			}
			parts[keys[i]] = append(parts[keys[i]], vectorSample{value: f, labels: inputs[i].labels})
		}
		for _, key := range order {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("logql: vector aggregation: %w", err)
			}
			samples := parts[key]
			slices.SortFunc(samples, func(x, y vectorSample) int {
				return compareVectorValues(x.value, y.value, a.Func == SortDesc)
			})
			for _, s := range samples {
				ms := set.get(s.labels)
				ms.points = append(ms.points, plan.Point{T: at, V: s.value})
			}
		}
	}
	return set.series, nil
}

// compareVectorValues orders values for output, with NaN last in both
// directions, matching sort.Reverse of Loki's heaps.
func compareVectorValues(x, y float64, descending bool) int {
	xNaN, yNaN := math.IsNaN(x), math.IsNaN(y)
	switch {
	case xNaN && yNaN:
		return 0
	case xNaN:
		return 1
	case yNaN:
		return -1
	case descending:
		return cmp.Compare(y, x)
	default:
		return cmp.Compare(x, y)
	}
}
