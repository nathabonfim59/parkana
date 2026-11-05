// Range metric evaluation for count_over_time, rate, bytes_over_time and
// bytes_rate: Loki's range windows, the microsecond bucket grid, and the
// per-window totals shared by the Aggregate fast path and the Select fallback.
//
// Loki evaluates a range aggregation at every step time t, over the window
// (t-offset-range, t-offset] (pkg/logql/range_vector.go, load: a sample at or
// before rangeStart is skipped), and reports the point at the original
// evaluation time t. Stored timestamps are microseconds, so a window becomes
// the half-open Selection range
//
//	[floorMicro(t-offset-range)+1µs, floorMicro(t-offset)+1µs)
//
// which is exactly the set of microsecond timestamps in the Loki window. A
// window with no rows yields no point: Loki only emits series present in the
// window map, so empty windows never invent zero-valued samples.
//
// The common case of whole-microsecond steps uses one Aggregate call for the
// whole query: with Width = gcd(range, step) every window starts on the same
// bucket grid (Origin is the first window's start), so a window's total is the
// sum of range/Width consecutive buckets. A range wider than
// maxBucketsPerWindow buckets falls back to one exact Aggregate per window.
// Sub-microsecond steps cannot share a whole-microsecond grid (successive
// windows advance by floor or ceil of the step), so they use the exact 1µs
// grid, which is exact but often exceeds the bucket bound and falls back.
package logql

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

const (
	// maxMetricSteps mirrors Loki's point bound (pkg/loghttp/query.go:510
	// rejects (End-Start)/Step > 11000), so a request with exactly 11001
	// evaluation points is accepted, as in Loki. It also keeps evaluation-time
	// arithmetic and memory bounded.
	maxMetricSteps = 11000

	// maxBucketsPerWindow bounds the buckets one optimized Aggregate call has
	// to look up per evaluation. Beyond it the engine issues one exact
	// Aggregate per window instead.
	maxBucketsPerWindow = 1000
)

// metricEvalTimes returns the evaluation timestamps of p: a single point for
// an instant query, otherwise Start, Start+Step, ... <= End.
func metricEvalTimes(p plan.Params) ([]time.Time, error) {
	if p.Instant() {
		return []time.Time{p.Start}, nil
	}
	if p.Step <= 0 {
		return nil, fmt.Errorf("logql: %w: metric range query needs a positive step", plan.ErrBadQuery)
	}
	// time.Time.Sub saturates at the time.Duration limits, which would
	// silently collapse a wider request to a bogus span and lose evaluations
	// (a 400-year span would look like 292 years). Reject the client request
	// before any arithmetic or Source call instead.
	if p.End.After(p.Start.Add(math.MaxInt64)) {
		return nil, fmt.Errorf("logql: %w: query span %v..%v is wider than a time.Duration; narrow the range", plan.ErrBadQuery, p.Start, p.End)
	}
	// Guard before the +1 so a degenerate span/step cannot overflow the count.
	intervals := p.End.Sub(p.Start) / p.Step
	if intervals > maxMetricSteps {
		return nil, fmt.Errorf("logql: %w: %d evaluation points exceed the limit of %d; increase the step", plan.ErrLimit, intervals+1, maxMetricSteps+1)
	}
	times := make([]time.Time, intervals+1)
	for i := range times {
		// (i*Step) <= intervals*Step <= End-Start, so the multiplication cannot
		// overflow time.Duration for any request that passed the bound above.
		times[i] = p.Start.Add(time.Duration(i) * p.Step)
	}
	return times, nil
}

// metricWindow is one evaluation's exact half-open microsecond window.
type metricWindow struct {
	// at is the evaluation timestamp reported in the result (the request time,
	// not the offset-shifted data time).
	at time.Time
	// from is the first microsecond strictly after at-offset-range; through is
	// the first microsecond strictly after at-offset. from == through for a
	// zero range.
	from, through time.Time
	// first is the index of the window's first bucket in a metricGrid;
	// buckets is how many consecutive buckets the window covers.
	first, buckets int64
}

// metricWindows computes the window for every evaluation time. Windows are
// returned in evaluation order; with a positive step both from and through are
// non-decreasing.
func metricWindows(times []time.Time, offset, rng time.Duration) []metricWindow {
	windows := make([]metricWindow, len(times))
	for i, t := range times {
		data := t.Add(-offset)
		windows[i] = metricWindow{
			at:      t,
			from:    floorMicro(data.Add(-rng)).Add(time.Microsecond),
			through: floorMicro(data).Add(time.Microsecond),
		}
	}
	return windows
}

// floorMicro truncates t down to the stored microsecond grid. Absolute times
// are always positive since year 1, so Truncate floors for negative Unix times
// too.
func floorMicro(t time.Time) time.Time {
	return t.Truncate(time.Microsecond).UTC()
}

// metricGrid is the shared microsecond bucket grid of one range query.
type metricGrid struct {
	width  time.Duration // positive whole number of µs
	origin time.Time     // start of bucket 0, on the µs grid
}

// newMetricGrid derives the bucket grid for windows. It sets each window's
// first and buckets fields, and reports whether one Aggregate call can cover
// every window (buckets per window within maxBucketsPerWindow).
//
// A positive step (a range query) shares Width = gcd(range, step): successive
// windows start exactly step/Width buckets apart, so one Aggregate can cover
// them all. Only an instant query (Step 0) uses the whole range as its width,
// and a sub-microsecond step uses the exact 1µs grid because the rounded window
// starts then advance by floor or ceil of the step. There is no approximate
// fallback: a window that does not start on the grid is a programming error.
func newMetricGrid(windows []metricWindow, rng, step time.Duration) (metricGrid, bool, error) {
	rangeUS := int64(rng / time.Microsecond)
	widthUS := rangeUS // instant query: Step 0, one exact window
	if step > 0 {
		if step%time.Microsecond == 0 {
			widthUS = gcd(rangeUS, int64(step/time.Microsecond))
		} else {
			// Sub-microsecond steps advance the floored window start by floor
			// or ceil of the step, so only 1µs buckets are exact.
			widthUS = 1
		}
	}
	if widthUS <= 0 {
		return metricGrid{}, false, fmt.Errorf("logql: range %s has no whole-microsecond buckets", rng)
	}
	// Every bucket index below is a time.Duration subtraction and the Source's
	// BucketAgg.Start does the same, so the union of the windows must fit in a
	// time.Duration exactly. A short range plus a large offset or lookback can
	// span more than that even when the request span itself does not; reject
	// it here rather than let Sub saturate the bucket arithmetic.
	last := windows[len(windows)-1].through
	if last.After(windows[0].from.Add(math.MaxInt64)) {
		return metricGrid{}, false, fmt.Errorf("logql: %w: evaluation span %v..%v (range plus offset) exceeds the maximum %s", plan.ErrLimit, windows[0].from, last, time.Duration(math.MaxInt64))
	}
	grid := metricGrid{
		width:  time.Duration(widthUS) * time.Microsecond,
		origin: windows[0].from,
	}
	buckets := rangeUS / widthUS
	for i := range windows {
		d := windows[i].from.Sub(grid.origin)
		if d%grid.width != 0 {
			return metricGrid{}, false, fmt.Errorf("logql: window %s is not aligned to the %s grid", windows[i].from, grid.width)
		}
		windows[i].first = int64(d / grid.width)
		windows[i].buckets = buckets
	}
	return grid, buckets <= maxBucketsPerWindow, nil
}

// gcd returns the greatest common divisor of a and b, both positive.
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// metricBucket is one window bucket's totals for one series.
type metricBucket struct {
	index, count, bytes int64
}

// metricSeries accumulates one result series. The fast path fills buckets;
// the per-window fallback appends points directly. points holds full-precision
// nominal times (never rounded); steps and outs are parallel slices filled by
// tagSeries with the evaluation index and the millisecond timestamp Loki emits
// for that step.
type metricSeries struct {
	labels  logs.Labels
	buckets map[int64]metricBucket
	points  []plan.Point
	steps   []int
	outs    []time.Time
}

func (s *metricSeries) add(idx, count, bytes int64) {
	if s.buckets == nil {
		s.buckets = make(map[int64]metricBucket, 8)
	}
	b := s.buckets[idx]
	b.index = idx
	b.count += count
	b.bytes += bytes
	s.buckets[idx] = b
}

// metricSet groups series by their final label set, merging source label sets
// that project onto the same labels before any per-window aggregation.
type metricSet struct {
	index  map[string]*metricSeries
	series []*metricSeries
}

func (s *metricSet) get(labels logs.Labels) *metricSeries {
	key := labels.String()
	if s.index == nil {
		s.index = make(map[string]*metricSeries)
	}
	ms, ok := s.index[key]
	if !ok {
		ms = &metricSeries{labels: labels}
		s.index[key] = ms
		s.series = append(s.series, ms)
	}
	return ms
}

// windowPoints converts a series' buckets into one point per window that holds
// at least one row. Bucket indices and window ranges are non-decreasing, so
// two pointers keep the total work linear in the number of present buckets.
func (s *metricSeries) windowPoints(windows []metricWindow, fn RangeFunc, rng time.Duration) {
	if s.buckets == nil {
		return
	}
	keys := slices.Sorted(maps.Keys(s.buckets))
	lo, hi := 0, 0
	var count, bytes int64
	for _, w := range windows {
		end := w.first + w.buckets
		for lo < hi && keys[lo] < w.first {
			count -= s.buckets[keys[lo]].count
			bytes -= s.buckets[keys[lo]].bytes
			lo++
		}
		for lo < len(keys) && keys[lo] < w.first {
			lo++
		}
		if hi < lo {
			hi = lo
		}
		for hi < len(keys) && keys[hi] < end {
			count += s.buckets[keys[hi]].count
			bytes += s.buckets[keys[hi]].bytes
			hi++
		}
		if hi > lo {
			s.points = append(s.points, plan.Point{T: w.at, V: rangeValue(fn, count, bytes, rng)})
		}
	}
}

// rangeValue converts a window's totals to the function's sample value. Loki's
// rate family divides by the LogQL range in seconds (range_vector.go
// rateLogs/rateLogBytes), never by the number of buckets or the window length.
func rangeValue(fn RangeFunc, count, bytes int64, rng time.Duration) float64 {
	switch fn {
	case Rate:
		return float64(count) / rng.Seconds()
	case BytesRate:
		return float64(bytes) / rng.Seconds()
	case BytesOverTime:
		return float64(bytes)
	default: // CountOverTime
		return float64(count)
	}
}

// windowContains reports whether ts falls in at least one evaluation window.
// Windows are ordered by from and their through values are non-decreasing, so
// the last window starting at or before ts is the one that matters. Rows in
// the gaps between a large step and a short range belong to no window and must
// not make a metric query fail on a pipeline error.
func windowContains(windows []metricWindow, ts time.Time) bool {
	i := sort.Search(len(windows), func(i int) bool { return windows[i].from.After(ts) }) - 1
	if i < 0 {
		return false
	}
	return ts.Before(windows[i].through)
}

// windowCoversBucket reports whether bucket index idx feeds at least one
// evaluated window. A step wider than the range leaves buckets in the gaps
// between windows; the union Aggregate returns them, but no point uses them,
// so their counts and any state they carry (for example metadata named
// __error__) must not affect the query.
func windowCoversBucket(windows []metricWindow, idx int64) bool {
	i := sort.Search(len(windows), func(i int) bool { return windows[i].first > idx }) - 1
	return i >= 0 && idx < windows[i].first+windows[i].buckets
}

// projectGroup keeps only the labels named in group, the effective grouping of
// an enclosing vector aggregation. A nil group keeps every label (used only by
// the exact Select path, where per-entry labels are already applied).
func projectGroup(ls logs.Labels, group []string) logs.Labels {
	if group == nil {
		return ls
	}
	if len(group) == 0 {
		return nil
	}
	kept := make(logs.Labels, 0, len(group))
	for _, name := range group {
		if v, ok := ls.Get(name); ok {
			kept = append(kept, logs.Label{Name: name, Value: v})
		}
	}
	return logs.New(kept...)
}
