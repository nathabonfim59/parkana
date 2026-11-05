// Metric output timestamps. Evaluation keeps full-precision nominal times for
// window bounds, Source selections and recursive indexing; every emitted
// sample carries a separate millisecond timestamp computed once by the family
// that produced it.
//
// Loki 3.7.9's evaluator works in milliseconds at three different points and
// the formulas are not interchangeable:
//
//   - A range aggregation converts its data timestamp and offset separately:
//     ts = current/1e6 + offset/1e6 (pkg/logql/range_vector.go 45-52, 104,
//     195 and 589), where current = eval_ns - offset_ns. That is two
//     truncations toward zero, so a sub-millisecond offset and a negative
//     timestamp do not collapse to trunc(eval/1e6).
//   - A direct root scalar or vector literal range iterates the request's exact
//     times and reports ts.UnixNano()/1e6 (pkg/logql/engine.go 695-755,
//     PopulateMatrixFromScalar). A direct root vector literal instant query is
//     Start.UnixMilli() (engine.go 712-731), which floors negative times
//     instead of truncating toward zero.
//   - A nested vector(NUMBER) is a VectorIterator, not a root: it takes integer
//     millisecond start, end and step, forcing a zero step to 1ms
//     (pkg/logql/evaluator.go 389-394 and 1216-1238). Its step count can
//     therefore differ from the request's nominal grid.
//
// An evalGrid keeps the nominal times (what parents index by, never rounded)
// next to the emitted times. metricSeries carries parallel steps/outs slices so
// no parent ever joins children by rounded timestamps.
package logql

import (
	"fmt"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// evalGrid is one evaluation's step sequence: times are the full-precision
// nominal times used for indexing and Source windows; outs are the millisecond
// timestamps emitted for each step.
type evalGrid struct {
	times []time.Time
	outs  []time.Time
}

// count is the number of evaluation steps.
func (g *evalGrid) count() int { return len(g.times) }

// head truncates a grid to its first n steps. The slices share backing arrays;
// callers only read them.
func (g *evalGrid) head(n int) *evalGrid {
	n = min(max(n, 0), g.count())
	return &evalGrid{times: g.times[:n], outs: g.outs[:n]}
}

// evalSeries is a vector expression's evaluated series plus the grid their
// points belong to.
type evalSeries struct {
	grid   *evalGrid
	series []*metricSeries
}

// nominalGrid builds the exact request grid with Loki's UnixNano()/1e6
// truncation. It is the direct-root scalar and vector-literal-range grid.
func nominalGrid(times []time.Time) *evalGrid {
	outs := make([]time.Time, len(times))
	for i, t := range times {
		outs[i] = truncMilli(t)
	}
	return &evalGrid{times: times, outs: outs}
}

// rangeGrid builds a range aggregation's grid. Bounds and nominal times stay
// untouched; only outs use the range formula.
func rangeGrid(times []time.Time, offset time.Duration) *evalGrid {
	outs := make([]time.Time, len(times))
	for i, t := range times {
		outs[i] = rangeOutputTime(t, offset)
	}
	return &evalGrid{times: times, outs: outs}
}

// truncMilli is Go's UnixNano()/1e6: integer division truncates toward zero,
// unlike time.Time.UnixMilli, which floors.
func truncMilli(t time.Time) time.Time {
	return time.UnixMilli(t.UnixNano() / int64(time.Millisecond)).UTC()
}

// rangeOutputTime is Loki's range iterator timestamp: the data time and the
// offset are each truncated to milliseconds and then added.
func rangeOutputTime(t time.Time, offset time.Duration) time.Time {
	o := offset.Nanoseconds()
	data := t.Add(-offset)
	return time.UnixMilli(data.UnixNano()/int64(time.Millisecond) + o/int64(time.Millisecond)).UTC()
}

// maxNestedVectorSteps bounds a nested vector(NUMBER)'s expanded grid. A
// nominal-valid request has at most maxMetricSteps+1 points, and the nested
// grid can expand by just under 2x: with a step just below 2ms the integer
// millisecond step is 1ms, so 11000 intervals can span 22001 milliseconds.
// The bound is therefore stated in points, 2*(maxMetricSteps+1), and never
// rejects a query the nominal guard accepted.
const maxNestedVectorSteps = 2 * (maxMetricSteps + 1)

// vectorIterGrid is a nested vector(NUMBER)'s VectorIterator grid:
// start/end/step are integer milliseconds, a zero step becomes 1ms, and the
// step count is derived from that grid rather than the request's exact times.
func vectorIterGrid(p plan.Params) (*evalGrid, error) {
	startMs := p.Start.UnixMilli()
	endMs := p.End.UnixMilli()
	if endMs < startMs {
		return &evalGrid{}, nil
	}
	stepMs := p.Step.Milliseconds()
	if stepMs < 1 {
		stepMs = 1
	}
	// Both ends are time.Time UnixMilli values, so the span fits an int64; the
	// explicit negative check above also catches an overflowing subtraction.
	span := endMs - startMs
	if span < 0 {
		return &evalGrid{}, nil
	}
	if span/stepMs >= int64(maxNestedVectorSteps) {
		return nil, fmt.Errorf("logql: %w: vector literal needs more than %d evaluation points; increase the step", plan.ErrLimit, maxNestedVectorSteps)
	}
	count := int(span/stepMs) + 1
	times := make([]time.Time, count)
	for i := range times {
		times[i] = time.UnixMilli(startMs + int64(i)*stepMs).UTC()
	}
	return &evalGrid{times: times, outs: slices.Clone(times)}, nil
}

// metricPoint returns point i with its emitted timestamp when the series was
// tagged, or its nominal time for a hand-built series.
func metricPoint(s *metricSeries, i int) plan.Point {
	pt := s.points[i]
	if len(s.outs) == len(s.points) {
		pt.T = s.outs[i]
	}
	return pt
}

// tagRangeSeries tags every series a range aggregation returned, including the
// A8b families, with its range grid. It is the single place range output
// timestamps are applied, so the range functions themselves never need to know
// about milliseconds.
func tagRangeSeries(series []*metricSeries, p plan.Params, offset time.Duration) ([]*metricSeries, error) {
	if len(series) == 0 {
		return series, nil
	}
	times, err := metricEvalTimes(p)
	if err != nil {
		return nil, err
	}
	if err := tagSeries(series, rangeGrid(times, offset)); err != nil {
		return nil, err
	}
	return series, nil
}

// tagSeries fills each point's step index and output timestamp from grid. The
// lookup uses the point's nominal time, before any rounding, so a sub-millisecond
// grid can hold several steps with the same emitted timestamp.
func tagSeries(series []*metricSeries, grid *evalGrid) error {
	for _, s := range series {
		if len(s.points) == 0 {
			continue
		}
		s.steps = make([]int, len(s.points))
		s.outs = make([]time.Time, len(s.points))
		for i := range s.points {
			idx, ok := slices.BinarySearchFunc(grid.times, s.points[i].T, func(a, b time.Time) int {
				return a.Compare(b)
			})
			if !ok || idx < 0 || idx >= grid.count() {
				return fmt.Errorf("logql: metric sample at %s is not an evaluation time", s.points[i].T)
			}
			s.steps[i] = idx
			s.outs[i] = grid.outs[idx]
		}
	}
	return nil
}
