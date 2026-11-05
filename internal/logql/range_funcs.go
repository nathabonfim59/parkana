// Additional range aggregations: unwrap and the sum, avg, min, max, stddev,
// stdvar, first, last, quantile and absent families.
//
// These functions need every extracted value, so unlike A7's count/rate/bytes
// they have no bucket fast path: one Select covers the union of the evaluation
// windows, each row runs through the shared compiled pipeline and unwrap
// extraction (unwrap.go), and each series keeps its samples in ascending
// timestamp order. Two advancing window indices select each window's span in
// linear total time, but every window's function then reads (and quantile
// sorts) its own samples, so the arithmetic is O(window samples) per
// evaluation, not globally linear.
//
// The function arithmetic is the batch path of Loki's range_vector.go
// (aggregator): sum, an Inf-aware running mean, NaN-aware max/min, Welford's
// population variance, Loki's quantile interpolation (sorted ascending, rank =
// q*(n-1), linear between the two neighbouring values), first and last by
// ascending timestamp, and rate over unwrapped values as sum/range-seconds.
// Empty windows produce no point; absent_over_time is the exception, emitting
// 1 for every window with no surviving sample.
package logql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// metricSample is one unwrapped entry of a series. Samples arrive in Select's
// Forward order, so a series' slice is timestamp ascending.
type metricSample struct {
	ts    time.Time
	value float64
}

// sampleSeries accumulates one child series' samples before the per-window
// functions run.
type sampleSeries struct {
	labels  logs.Labels
	samples []metricSample
}

// sampleSet groups accumulating series by the extractor's exact identity.
type sampleSet struct {
	index  map[string]*sampleSeries
	series []*sampleSeries
}

// get returns the series for key, creating it with labels on first use. The
// key is the extractor identity; labels may be a parent projection of it.
func (s *sampleSet) get(key, labels logs.Labels) *sampleSeries {
	k := key.String()
	if s.index == nil {
		s.index = make(map[string]*sampleSeries)
	}
	ss, ok := s.index[k]
	if !ok {
		ss = &sampleSeries{labels: labels}
		s.index[k] = ss
		s.series = append(s.series, ss)
	}
	return ss
}

// points evaluates the range function over every series and window. Series
// without a sample in any window are omitted; labels were already projected
// by the caller when the enclosing vector aggregation injected a grouping.
func (s *sampleSet) points(ctx context.Context, fn RangeFunc, params []float64, windows []metricWindow, rng time.Duration) ([]*metricSeries, error) {
	out := make([]*metricSeries, 0, len(s.series))
	for _, ss := range s.series {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: range aggregation: %w", err)
		}
		points, err := samplePoints(ctx, ss.samples, windows, fn, params, rng)
		if err != nil {
			return nil, err
		}
		if len(points) == 0 {
			continue
		}
		out = append(out, &metricSeries{labels: ss.labels, points: points})
	}
	return out, nil
}

// samplePoints evaluates fn over each window's samples. Samples and windows are
// both ordered and never move backwards, so two indices select every window's
// span in linear total time even when windows overlap heavily; fn then reads
// (and, for quantile, sorts) that span once per window.
func samplePoints(ctx context.Context, samples []metricSample, windows []metricWindow, fn RangeFunc, params []float64, rng time.Duration) ([]plan.Point, error) {
	points := make([]plan.Point, 0, len(windows))
	lo, hi := 0, 0
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: range aggregation: %w", err)
		}
		for lo < len(samples) && samples[lo].ts.Before(w.from) {
			lo++
		}
		if hi < lo {
			hi = lo
		}
		for hi < len(samples) && samples[hi].ts.Before(w.through) {
			hi++
		}
		if lo == hi {
			continue
		}
		points = append(points, plan.Point{T: w.at, V: rangeFuncValue(fn, samples[lo:hi], params, rng)})
	}
	return points, nil
}

// evalAdditionalRangeSeries is the A8b branch of evalRangeSeries: every
// aggregation with unwrap and every range function outside A7's count/rate/
// bytes family. It returns unrestricted child series; the caller owns the
// final shaping and series limit.
func (e *Engine) evalAdditionalRangeSeries(ctx context.Context, src plan.Source, a *RangeAgg, p plan.Params, opts rangeOptions) ([]*metricSeries, error) {
	if err := validateAdditionalRange(a); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: range aggregation: %w", err)
	}
	if src == nil {
		return nil, errors.New("logql: nil source")
	}
	if a.Range.Range < 0 {
		return nil, fmt.Errorf("logql: %w: negative range %s", plan.ErrBadQuery, a.Range.Range)
	}
	times, err := metricEvalTimes(p)
	if err != nil {
		return nil, err
	}
	windows := metricWindows(times, a.Range.Offset, a.Range.Range)
	u := a.Range.Unwrap
	c, err := compileLogQuery(a.Range.Pipeline)
	if err != nil {
		return nil, err
	}
	var post labelFilterer
	if u != nil {
		if post, err = compileUnwrapPostFilters(u.PostFilters); err != nil {
			return nil, err
		}
	}
	absent := a.Func == AbsentOverTime
	// A zero range has an empty window at every evaluation, so no stored row
	// can match. Like A7's zero-range count, do not open the Source: every
	// other function has no series and absent_over_time emits its 1s.
	if a.Range.Range == 0 {
		if !absent {
			return nil, nil
		}
		return absentRangeSeries(ctx, c, windows, nil, opts.group)
	}
	// The Select below covers the union of every window, so its span must fit
	// a time.Duration exactly. newMetricGrid owns A7's guard for the bucket
	// path (range.go); reusing it here validates the same union before the
	// Source sees it.
	if _, _, err := newMetricGrid(windows, a.Range.Range, p.Step); err != nil {
		return nil, err
	}
	// No Source call may start after cancellation.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: range aggregation: %w", err)
	}

	sel := rangeSelection(c, p, windows)
	var (
		set     sampleSet
		present []time.Time
	)
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
		// A row in the gap between a large step's windows belongs to no
		// evaluation: it neither creates a series nor trips a pipeline error.
		if !windowContains(windows, row.TS) {
			continue
		}
		value, ok := unwrapRow(entry, u, post)
		if !ok {
			continue
		}
		if entry.err != "" {
			return nil, metricPipelineErr(entry, metricBuilderIdentity(entry, a.Grouping, unwrapLabel(u)))
		}
		if absent {
			present = append(present, row.TS)
			continue
		}
		// The series key is the exact extractor identity, so children a parent
		// projection would collapse stay separate for the parent to aggregate.
		identity := metricBuilderIdentity(entry, a.Grouping, unwrapLabel(u))
		labels := identity
		if opts.group != nil {
			if canInjectUnwrapGrouping(a) {
				// A real sum injection replaces the extractor's grouping
				// (Loki's RangeAggregationExpr.extractor override): the
				// requested names, including the unwrap value field, join the
				// identity itself, and that grouped identity is also the
				// collection key.
				var injected *Grouping
				if len(opts.group) > 0 {
					injected = &Grouping{Names: slices.Clone(opts.group)}
				} else {
					injected = &Grouping{}
				}
				identity = metricBuilderIdentity(entry, injected, unwrapLabel(u))
				labels = identity
			} else {
				// Not an actual inject-eligible function: the seam is a pure
				// projection over the exact child identity.
				labels = metricProjectedIdentity(entry, opts.group)
			}
		}
		ms := set.get(identity, labels)
		ms.samples = append(ms.samples, metricSample{ts: row.TS, value: value})
	}
	// A Source may cancel the request right after its final yield; that is not
	// a successful query.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("logql: select: %w", err)
	}
	if absent {
		return absentRangeSeries(ctx, c, windows, present, opts.group)
	}
	return set.points(ctx, a.Func, a.Params, windows, a.Range.Range)
}

// absentRangeSeries builds absent_over_time's single result series: value 1 at
// every evaluation whose window holds no surviving sample. Loki derives the
// label set from the selector's equality matchers only (evaluator.go
// absentLabels). The enclosing vector aggregation's injected grouping is a
// pure projection of that label set.
func absentRangeSeries(ctx context.Context, c *CompiledLogQuery, windows []metricWindow, present []time.Time, group []string) ([]*metricSeries, error) {
	points, err := absentPoints(ctx, windows, present)
	if err != nil {
		return nil, err
	}
	if len(points) == 0 {
		return nil, nil
	}
	labels := absentRangeLabels(c.matchers)
	if group != nil {
		if len(group) == 0 {
			labels = nil
		} else {
			labels = projectGroup(labels, group)
		}
	}
	return []*metricSeries{{labels: labels, points: points}}, nil
}

// absentPoints reports whether each window holds a present timestamp.
// present is timestamp ascending, so the same advancing indices as
// samplePoints decide each window.
func absentPoints(ctx context.Context, windows []metricWindow, present []time.Time) ([]plan.Point, error) {
	points := make([]plan.Point, 0, len(windows))
	lo, hi := 0, 0
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("logql: range aggregation: %w", err)
		}
		for lo < len(present) && present[lo].Before(w.from) {
			lo++
		}
		if hi < lo {
			hi = lo
		}
		for hi < len(present) && present[hi].Before(w.through) {
			hi++
		}
		if lo == hi {
			points = append(points, plan.Point{T: w.at, V: 1})
		}
	}
	return points, nil
}

// canInjectUnwrapGrouping reports whether an enclosing sum injects its
// by-grouping into this unwrap child. Loki's canInjectVectorGrouping
// (pkg/logql/syntax/ast.go) lists rate and sum_over_time among the linear range
// operations, and the injection only happens when the range has no grouping of
// its own (VectorAggregationExpr.Extractors).
func canInjectUnwrapGrouping(a *RangeAgg) bool {
	if a.Grouping != nil {
		return false
	}
	switch a.Func {
	case Rate, SumOverTime:
		return true
	}
	return false
}

// validateAdditionalRange enforces the parser's function, parameter, unwrap
// and grouping rules for the additional families. The parser already rejects
// these combinations, so a failure here is a mis-built AST, reported as a bad
// query rather than approximated.
func validateAdditionalRange(a *RangeAgg) error {
	if a == nil || a.Range == nil {
		return fmt.Errorf("logql: %w: range aggregation without a range", plan.ErrBadQuery)
	}
	switch u := a.Range.Unwrap; {
	case u == nil && a.Func != AbsentOverTime:
		return fmt.Errorf("logql: %w: invalid aggregation %s without unwrap", plan.ErrBadQuery, a.Func)
	case u != nil && !unwrapAllowedRange(a.Func):
		return fmt.Errorf("logql: %w: invalid aggregation %s with unwrap", plan.ErrBadQuery, a.Func)
	}
	if a.Func == QuantileOverTime {
		if len(a.Params) != 1 {
			return fmt.Errorf("logql: %w: parameter required for operation %s", plan.ErrBadQuery, a.Func)
		}
	} else if len(a.Params) != 0 {
		return fmt.Errorf("logql: %w: parameter not supported for operation %s", plan.ErrBadQuery, a.Func)
	}
	if a.Grouping != nil && !groupingAllowedRange(a.Func) {
		return fmt.Errorf("logql: %w: grouping is not valid for %s", plan.ErrBadQuery, a.Func)
	}
	return nil
}

// unwrapLabel returns the unwrap field name, "" for a plain line extractor.
func unwrapLabel(u *Unwrap) string {
	if u == nil {
		return ""
	}
	return u.Label
}

// rangeFuncValue computes one window's value from its samples. The bucket
// families (count_over_time, bytes_over_time, bytes_rate) never reach here;
// A7 evaluates them.
func rangeFuncValue(fn RangeFunc, samples []metricSample, params []float64, rng time.Duration) float64 {
	switch fn {
	case SumOverTime:
		return sumSamples(samples)
	case AvgOverTime:
		return avgSamples(samples)
	case MaxOverTime:
		return maxSamples(samples)
	case MinOverTime:
		return minSamples(samples)
	case StddevOverTime:
		return stddevSamples(samples)
	case StdvarOverTime:
		return stdvarSamples(samples)
	case FirstOverTime:
		return firstSample(samples)
	case LastOverTime:
		return lastSample(samples)
	case QuantileOverTime:
		return quantileSamples(params[0], samples)
	case Rate:
		// Unwrapped rate is the sum of the extracted values over the LogQL
		// range in seconds (range_vector.go rateLogs, computeValues).
		return sumSamples(samples) / rng.Seconds()
	}
	return math.NaN() // unreachable: validateAdditionalRange rejects the rest
}

// sumSamples is Loki's sumOverTime.
func sumSamples(samples []metricSample) float64 {
	var sum float64
	for _, s := range samples {
		sum += s.value
	}
	return sum
}

// avgSamples is Loki's avgOverTime: a running mean that keeps an infinite mean
// stable instead of turning it into a NaN.
func avgSamples(samples []metricSample) float64 {
	var mean, count float64
	for _, s := range samples {
		count++
		if math.IsInf(mean, 0) {
			if math.IsInf(s.value, 0) && (mean > 0) == (s.value > 0) {
				continue
			}
			if !math.IsInf(s.value, 0) && !math.IsNaN(s.value) {
				continue
			}
		}
		mean += s.value/count - mean/count
	}
	return mean
}

// maxSamples is Loki's maxOverTime; a NaN current maximum is replaced.
func maxSamples(samples []metricSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	m := samples[0].value
	for _, s := range samples {
		if s.value > m || math.IsNaN(m) {
			m = s.value
		}
	}
	return m
}

// minSamples is Loki's minOverTime.
func minSamples(samples []metricSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	m := samples[0].value
	for _, s := range samples {
		if s.value < m || math.IsNaN(m) {
			m = s.value
		}
	}
	return m
}

// stdvarSamples is Loki's stdvarOverTime: Welford's online algorithm, divided
// by the population count.
func stdvarSamples(samples []metricSample) float64 {
	aux, count, _ := welfordSamples(samples)
	return aux / count
}

// stddevSamples is stdvarOverTime's square root.
func stddevSamples(samples []metricSample) float64 {
	aux, count, _ := welfordSamples(samples)
	return math.Sqrt(aux / count)
}

// welfordSamples returns the sum of squared deviations, the count and the mean.
func welfordSamples(samples []metricSample) (aux, count, mean float64) {
	for _, s := range samples {
		count++
		delta := s.value - mean
		mean += delta / count
		aux += delta * (s.value - mean)
	}
	return aux, count, mean
}

// firstSample is Loki's first: the earliest timestamp's value. Ties share a
// timestamp and follow Select's unspecified order for equal timestamps.
func firstSample(samples []metricSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	return samples[0].value
}

// lastSample is Loki's last: the latest timestamp's value, with the same
// equal-timestamp caveat as firstSample.
func lastSample(samples []metricSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	return samples[len(samples)-1].value
}

// quantileSamples is Loki's Quantile over the window's values: values sorted
// ascending, rank = q*(n-1), linear interpolation between the two neighbouring
// ranks. q < 0 is -Inf, q > 1 is +Inf; an empty set is NaN.
func quantileSamples(q float64, samples []metricSample) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(1)
	}
	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.value
	}
	slices.Sort(values)
	n := float64(len(values))
	rank := q * (n - 1)
	lower := max(0, math.Floor(rank))
	upper := min(n-1, lower+1)
	weight := rank - math.Floor(rank)
	return values[int(lower)]*(1-weight) + values[int(upper)]*weight
}
