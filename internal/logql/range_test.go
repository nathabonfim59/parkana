package logql

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

var (
	rangeBase  = time.Date(2024, 1, 2, 3, 4, 0, 0, time.UTC)
	rangeBase2 = time.Date(2024, 1, 2, 3, 5, 0, 0, time.UTC)
)

func mustEvalTimes(t *testing.T, p plan.Params) []time.Time {
	t.Helper()
	times, err := metricEvalTimes(p)
	if err != nil {
		t.Fatalf("metricEvalTimes: %v", err)
	}
	return times
}

func TestMetricEvalTimes(t *testing.T) {
	instant := plan.Params{Start: rangeBase, End: rangeBase}
	got := mustEvalTimes(t, instant)
	if !slices.Equal(got, []time.Time{rangeBase}) {
		t.Fatalf("instant times = %v, want [%v]", got, rangeBase)
	}

	// A step that does not divide the span stops at the last point <= End.
	p := plan.Params{Start: rangeBase, End: rangeBase.Add(time.Second), Step: 300 * time.Millisecond}
	want := []time.Time{
		rangeBase,
		rangeBase.Add(300 * time.Millisecond),
		rangeBase.Add(600 * time.Millisecond),
		rangeBase.Add(900 * time.Millisecond),
	}
	if got := mustEvalTimes(t, p); !slices.Equal(got, want) {
		t.Fatalf("times = %v, want %v", got, want)
	}
}

func TestMetricEvalTimesStepBound(t *testing.T) {
	// Loki rejects (End-Start)/Step > 11000, so exactly 11001 points pass.
	ok := plan.Params{Start: rangeBase, End: rangeBase.Add(11000 * time.Millisecond), Step: time.Millisecond}
	times := mustEvalTimes(t, ok)
	if len(times) != 11001 {
		t.Fatalf("points = %d, want 11001", len(times))
	}
	if !times[11000].Equal(rangeBase.Add(11000 * time.Millisecond)) {
		t.Fatalf("last point = %v", times[11000])
	}

	over := ok
	over.End = rangeBase.Add(11001 * time.Millisecond)
	if _, err := metricEvalTimes(over); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("err = %v, want ErrLimit", err)
	}

	// Extreme spans are rejected as a client error before any arithmetic.
	huge := plan.Params{
		Start: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		Step:  time.Nanosecond,
	}
	if _, err := metricEvalTimes(huge); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("huge span err = %v, want ErrBadQuery", err)
	}
}

func TestMetricWindows(t *testing.T) {
	aligned := metricWindows([]time.Time{rangeBase}, 0, time.Minute)[0]
	if want := rangeBase.Add(-time.Minute + time.Microsecond); !aligned.from.Equal(want) {
		t.Fatalf("from = %v, want %v", aligned.from, want)
	}
	if want := rangeBase.Add(time.Microsecond); !aligned.through.Equal(want) {
		t.Fatalf("through = %v, want %v", aligned.through, want)
	}

	// Sub-microsecond evaluation times floor onto the stored grid; the window
	// still covers exactly the Loki range.
	sub := metricWindows([]time.Time{rangeBase.Add(500 * time.Nanosecond)}, 0, time.Millisecond)[0]
	if want := rangeBase.Add(-time.Millisecond + time.Microsecond); !sub.from.Equal(want) {
		t.Fatalf("sub from = %v, want %v", sub.from, want)
	}
	if want := rangeBase.Add(time.Microsecond); !sub.through.Equal(want) {
		t.Fatalf("sub through = %v, want %v", sub.through, want)
	}

	// Offset moves the data window but never the reported evaluation time.
	off := metricWindows([]time.Time{rangeBase}, time.Hour, time.Minute)[0]
	if !off.at.Equal(rangeBase) {
		t.Fatalf("at = %v, want the original evaluation time", off.at)
	}
	if want := rangeBase.Add(-time.Hour - time.Minute + time.Microsecond); !off.from.Equal(want) {
		t.Fatalf("offset from = %v, want %v", off.from, want)
	}
	if want := rangeBase.Add(-time.Hour + time.Microsecond); !off.through.Equal(want) {
		t.Fatalf("offset through = %v, want %v", off.through, want)
	}

	// Negative offset looks forward from the evaluation time.
	neg := metricWindows([]time.Time{rangeBase}, -time.Hour, time.Minute)[0]
	if want := rangeBase.Add(time.Hour - time.Minute + time.Microsecond); !neg.from.Equal(want) {
		t.Fatalf("negative offset from = %v, want %v", neg.from, want)
	}

	zero := metricWindows([]time.Time{rangeBase}, 0, 0)[0]
	if !zero.from.Equal(zero.through) {
		t.Fatalf("zero range window = [%v, %v), want empty", zero.from, zero.through)
	}

	negUnix := time.Unix(-100, 0).UTC()
	nw := metricWindows([]time.Time{negUnix}, 0, time.Minute)[0]
	if want := negUnix.Add(-time.Minute + time.Microsecond); !nw.from.Equal(want) {
		t.Fatalf("negative unix from = %v, want %v", nw.from, want)
	}
	if want := negUnix.Add(time.Microsecond); !nw.through.Equal(want) {
		t.Fatalf("negative unix through = %v, want %v", nw.through, want)
	}
}

func TestWindowContains(t *testing.T) {
	times := []time.Time{rangeBase, rangeBase.Add(time.Minute), rangeBase.Add(2 * time.Minute)}
	windows := metricWindows(times, 0, time.Minute)
	cases := []struct {
		name string
		ts   time.Time
		want bool
	}{
		{"before first window", rangeBase.Add(-time.Minute), false},
		{"first microsecond", rangeBase.Add(-time.Minute + time.Microsecond), true},
		{"first eval", rangeBase, true},
		{"shared edge is in the earlier window", rangeBase.Add(time.Minute), true},
		{"last eval", rangeBase.Add(2 * time.Minute), true},
		{"after last window", rangeBase.Add(2*time.Minute + time.Microsecond), false},
		{"after last window far", rangeBase.Add(10 * time.Minute), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := windowContains(windows, tc.ts); got != tc.want {
				t.Fatalf("windowContains(%v) = %v, want %v", tc.ts, got, tc.want)
			}
		})
	}

	// A step larger than the range leaves gaps that belong to no window.
	gapTimes := []time.Time{rangeBase, rangeBase.Add(time.Hour)}
	gapWindows := metricWindows(gapTimes, 0, time.Minute)
	if windowContains(gapWindows, rangeBase.Add(30*time.Minute)) {
		t.Fatalf("ts in the gap between windows reported contained")
	}
}

func TestNewMetricGridGCD(t *testing.T) {
	cases := []struct {
		name    string
		times   []time.Time
		rng     time.Duration
		step    time.Duration
		width   time.Duration
		buckets int64
		first   []int64
		single  bool
	}{
		{
			name:    "equal range and step",
			times:   []time.Time{rangeBase, rangeBase2},
			rng:     time.Minute,
			step:    time.Minute,
			width:   time.Minute,
			buckets: 1,
			first:   []int64{0, 1},
			single:  true,
		},
		{
			name:    "range divisible by step",
			times:   []time.Time{rangeBase, rangeBase.Add(15 * time.Second)},
			rng:     time.Minute,
			step:    15 * time.Second,
			width:   15 * time.Second,
			buckets: 4,
			first:   []int64{0, 1},
			single:  true,
		},
		{
			name:    "nonaligned range and step",
			times:   []time.Time{rangeBase, rangeBase.Add(40 * time.Second), rangeBase.Add(80 * time.Second)},
			rng:     90 * time.Second,
			step:    40 * time.Second,
			width:   10 * time.Second,
			buckets: 9,
			first:   []int64{0, 4, 8},
			single:  true,
		},
		{
			name:    "sub-microsecond step uses 1us buckets",
			times:   []time.Time{rangeBase, rangeBase.Add(1500 * time.Nanosecond), rangeBase.Add(3000 * time.Nanosecond)},
			rng:     time.Millisecond,
			step:    1500 * time.Nanosecond,
			width:   time.Microsecond,
			buckets: 1000,
			first:   []int64{0, 1, 3},
			single:  true,
		},
		{
			name:    "exactly the bucket bound stays single",
			times:   []time.Time{rangeBase, rangeBase.Add(2 * time.Microsecond)},
			rng:     2 * time.Millisecond,
			step:    2 * time.Microsecond,
			width:   2 * time.Microsecond,
			buckets: 1000,
			first:   []int64{0, 1},
			single:  true,
		},
		{
			name:    "over the bucket bound falls back",
			times:   []time.Time{rangeBase, rangeBase.Add(2500 * time.Nanosecond)},
			rng:     2 * time.Millisecond,
			step:    2500 * time.Nanosecond,
			width:   time.Microsecond,
			buckets: 2000,
			first:   []int64{0, 2},
			single:  false,
		},
		{
			name:    "instant uses the whole range",
			times:   []time.Time{rangeBase},
			rng:     time.Minute,
			step:    0,
			width:   time.Minute,
			buckets: 1,
			first:   []int64{0},
			single:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			windows := metricWindows(tc.times, 0, tc.rng)
			grid, single, err := newMetricGrid(windows, tc.rng, tc.step)
			if err != nil {
				t.Fatalf("newMetricGrid: %v", err)
			}
			if grid.width != tc.width {
				t.Fatalf("width = %s, want %s", grid.width, tc.width)
			}
			if grid.origin != windows[0].from {
				t.Fatalf("origin = %v, want first window start %v", grid.origin, windows[0].from)
			}
			if single != tc.single {
				t.Fatalf("single = %v, want %v", single, tc.single)
			}
			for i := range windows {
				if windows[i].buckets != tc.buckets {
					t.Fatalf("window %d buckets = %d, want %d", i, windows[i].buckets, tc.buckets)
				}
				if windows[i].first != tc.first[i] {
					t.Fatalf("window %d first = %d, want %d", i, windows[i].first, tc.first[i])
				}
			}
		})
	}
}

func TestGCD(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{60, 60, 60},
		{60, 15, 15},
		{90, 40, 10},
		{1000, 1, 1},
		{7, 13, 1},
	}
	for _, tc := range cases {
		if got := gcd(tc.a, tc.b); got != tc.want {
			t.Fatalf("gcd(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestWindowPointsTwoPointer(t *testing.T) {
	times := []time.Time{rangeBase, rangeBase.Add(time.Minute), rangeBase.Add(2 * time.Minute)}
	windows := metricWindows(times, 0, time.Minute)
	grid, _, err := newMetricGrid(windows, time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("newMetricGrid: %v", err)
	}
	_ = grid

	s := &metricSeries{labels: lab("app", "api")}
	s.add(0, 1, 5)
	s.add(1, 2, 10)
	s.add(2, 2, 20)
	s.windowPoints(windows, CountOverTime, time.Minute)
	want := []plan.Point{
		{T: rangeBase, V: 1},
		{T: rangeBase.Add(time.Minute), V: 2},
		{T: rangeBase.Add(2 * time.Minute), V: 2},
	}
	if !pointsEqual(s.points, want) {
		t.Fatalf("count points = %v, want %v", s.points, want)
	}

	b := &metricSeries{labels: lab("app", "api")}
	b.add(0, 1, 5)
	b.add(1, 2, 10)
	b.add(2, 2, 20)
	b.windowPoints(windows, BytesOverTime, time.Minute)
	wantBytes := []plan.Point{
		{T: rangeBase, V: 5},
		{T: rangeBase.Add(time.Minute), V: 10},
		{T: rangeBase.Add(2 * time.Minute), V: 20},
	}
	if !pointsEqual(b.points, wantBytes) {
		t.Fatalf("bytes points = %v, want %v", b.points, wantBytes)
	}

	// A window with no bucket contributes no point at all.
	sparse := &metricSeries{labels: lab("app", "api")}
	sparse.add(1, 1, 1)
	sparse.windowPoints(windows, CountOverTime, time.Minute)
	if len(sparse.points) != 1 || !sparse.points[0].T.Equal(rangeBase.Add(time.Minute)) {
		t.Fatalf("sparse points = %v, want one point at the middle window", sparse.points)
	}

	// A step larger than the range skips every bucket in the gap.
	gapTimes := []time.Time{rangeBase, rangeBase.Add(2 * time.Minute)}
	gapWindows := metricWindows(gapTimes, 0, time.Minute)
	if _, _, err := newMetricGrid(gapWindows, time.Minute, 2*time.Minute); err != nil {
		t.Fatalf("newMetricGrid: %v", err)
	}
	gapped := &metricSeries{labels: lab("app", "api")}
	gapped.add(0, 1, 1)
	gapped.add(1, 99, 99)
	gapped.add(2, 3, 3)
	gapped.windowPoints(gapWindows, CountOverTime, time.Minute)
	wantGap := []plan.Point{{T: rangeBase, V: 1}, {T: rangeBase.Add(2 * time.Minute), V: 3}}
	if !pointsEqual(gapped.points, wantGap) {
		t.Fatalf("gap points = %v, want %v", gapped.points, wantGap)
	}

	// Overlapping windows include the same bucket in several points.
	overTimes := []time.Time{rangeBase, rangeBase.Add(time.Minute), rangeBase.Add(2 * time.Minute)}
	overWindows := metricWindows(overTimes, 0, 2*time.Minute)
	if _, _, err := newMetricGrid(overWindows, 2*time.Minute, time.Minute); err != nil {
		t.Fatalf("newMetricGrid: %v", err)
	}
	over := &metricSeries{labels: lab("app", "api")}
	over.add(0, 1, 1)
	over.add(1, 10, 10)
	over.add(2, 100, 100)
	over.windowPoints(overWindows, CountOverTime, 2*time.Minute)
	wantOver := []plan.Point{
		{T: rangeBase, V: 11},
		{T: rangeBase.Add(time.Minute), V: 110},
		{T: rangeBase.Add(2 * time.Minute), V: 100},
	}
	if !pointsEqual(over.points, wantOver) {
		t.Fatalf("overlap points = %v, want %v", over.points, wantOver)
	}
}

func TestRangeValue(t *testing.T) {
	rng := 90 * time.Second
	if got := rangeValue(CountOverTime, 3, 30, rng); got != 3 {
		t.Fatalf("count = %v", got)
	}
	if got := rangeValue(BytesOverTime, 3, 30, rng); got != 30 {
		t.Fatalf("bytes = %v", got)
	}
	if got := rangeValue(Rate, 3, 30, rng); got != 3.0/90.0 {
		t.Fatalf("rate = %v", got)
	}
	if got := rangeValue(BytesRate, 3, 30, rng); got != 30.0/90.0 {
		t.Fatalf("bytes_rate = %v", got)
	}
}

func TestProjectGroup(t *testing.T) {
	ls := lab("app", "api", "level", "info", "pod", "a")
	if got := projectGroup(ls, nil); !slices.Equal(got, ls) {
		t.Fatalf("nil group = %v, want unchanged", got)
	}
	if got := projectGroup(ls, []string{}); len(got) != 0 {
		t.Fatalf("empty group = %v, want no labels", got)
	}
	got := projectGroup(ls, []string{"level", "absent"})
	if want := lab("level", "info"); !slices.Equal(got, want) {
		t.Fatalf("projected = %v, want %v", got, want)
	}
}

// lab builds a sorted label set from name/value pairs.
func lab(pairs ...string) logs.Labels {
	ls := make(logs.Labels, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		ls = append(ls, logs.Label{Name: pairs[i], Value: pairs[i+1]})
	}
	return logs.New(ls...)
}

func pointsEqual(got, want []plan.Point) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !got[i].T.Equal(want[i].T) || got[i].V != want[i].V {
			return false
		}
	}
	return true
}

func TestMetricEvalTimesRejectsNegativeStep(t *testing.T) {
	p := plan.Params{Start: rangeBase, End: rangeBase.Add(time.Minute), Step: -time.Second}
	if _, err := metricEvalTimes(p); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
}

func TestNewMetricGridErrors(t *testing.T) {
	// A zero range has no whole-microsecond bucket.
	windows := metricWindows([]time.Time{rangeBase}, 0, 0)
	if _, _, err := newMetricGrid(windows, 0, 0); err == nil {
		t.Fatalf("zero range grid accepted")
	}

	// An off-grid window start is a programming error, never approximated.
	off := []metricWindow{
		{at: rangeBase, from: rangeBase},
		{at: rangeBase.Add(2 * time.Microsecond), from: rangeBase.Add(time.Microsecond)},
	}
	if _, _, err := newMetricGrid(off, 2*time.Microsecond, 2*time.Microsecond); err == nil {
		t.Fatalf("misaligned window accepted")
	}
}
