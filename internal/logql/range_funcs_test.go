package logql

import (
	"context"
	"errors"
	"iter"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// sampleRowAt builds a row whose JSON line holds the unwrap field v.
func sampleRowAt(ts time.Time, value string, labels map[string]string) plan.Row {
	return metricRowAt(ts, `{"v":"`+value+`"}`, labels, nil)
}

func TestAdditionalRangeFunctions(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "5", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "1", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(30*time.Second), "9", map[string]string{"app": "api"}),
	}
	cases := []struct {
		name  string
		query string
		want  float64
	}{
		{"sum", `sum_over_time({app="api"} | json | unwrap v [1m])`, 15},
		{"avg", `avg_over_time({app="api"} | json | unwrap v [1m])`, 5},
		{"min", `min_over_time({app="api"} | json | unwrap v [1m])`, 1},
		{"max", `max_over_time({app="api"} | json | unwrap v [1m])`, 9},
		{"stdvar", `stdvar_over_time({app="api"} | json | unwrap v [1m])`, 32.0 / 3},
		{"stddev", `stddev_over_time({app="api"} | json | unwrap v [1m])`, math.Sqrt(32.0 / 3)},
		{"first", `first_over_time({app="api"} | json | unwrap v [1m])`, 5},
		{"last", `last_over_time({app="api"} | json | unwrap v [1m])`, 9},
		{"rate unwrap sums values", `rate({app="api"} | json | unwrap v [1m])`, 15.0 / 60},
		{"quantile median", `quantile_over_time(0.5, {app="api"} | json | unwrap v [1m])`, 5},
		{"quantile minimum", `quantile_over_time(0, {app="api"} | json | unwrap v [1m])`, 1},
		{"quantile maximum", `quantile_over_time(1, {app="api"} | json | unwrap v [1m])`, 9},
		{"quantile interpolates", `quantile_over_time(0.25, {app="api"} | json | unwrap v [1m])`, 3},
		{"quantile over one is unbounded above", `quantile_over_time(2, {app="api"} | json | unwrap v [1m])`, math.Inf(1)},
	}
	e := testEngine()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(rows...)
			res := mustQuery(t, e, src, instantParams(tc.query, rangeBase.Add(time.Minute)))
			if got := resultPoints(t, res); len(got) != 1 {
				t.Fatalf("series = %v, want one", got)
			}
			points := resultPoints(t, res)[lab("app", "api").String()]
			if len(points) != 1 {
				t.Fatalf("points = %v, want one", points)
			}
			got := points[0].V
			if tc.want != got && !(math.IsInf(tc.want, 0) && math.IsInf(got, 0) && (tc.want > 0) == (got > 0)) {
				t.Fatalf("%s = %v, want %v", tc.name, got, tc.want)
			}
			if src.selectCalls != 1 || src.aggregateCalls != 0 {
				t.Fatalf("calls = %d select / %d aggregate, want one select", src.selectCalls, src.aggregateCalls)
			}
		})
	}
}

func TestAdditionalRangeNonFinite(t *testing.T) {
	rows := func(values ...string) []plan.Row {
		out := make([]plan.Row, len(values))
		for i, v := range values {
			out[i] = sampleRowAt(rangeBase.Add(time.Duration(i+1)*10*time.Second), v, map[string]string{"app": "api"})
		}
		return out
	}
	e := testEngine()
	cases := []struct {
		name  string
		query string
		want  float64
	}{
		{"sum propagates Inf", `sum_over_time({app="api"} | json | unwrap v [1m])`, math.Inf(1)},
		{"avg keeps an Inf mean", `avg_over_time({app="api"} | json | unwrap v [1m])`, math.Inf(1)},
		{"max replaces a NaN start", `max_over_time({app="api"} | json | unwrap v [1m])`, 2},
		{"min replaces a NaN start", `min_over_time({app="api"} | json | unwrap v [1m])`, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var srcRows []plan.Row
			switch {
			case strings.Contains(tc.name, "NaN"):
				srcRows = rows("NaN", "2")
			default:
				srcRows = rows("Inf", "1", "2")
			}
			res := mustQuery(t, e, newMetricSource(srcRows...), instantParams(tc.query, rangeBase.Add(time.Minute)))
			points := resultPoints(t, res)[lab("app", "api").String()]
			if len(points) != 1 || points[0].V != tc.want {
				t.Fatalf("points = %v, want %v", points, tc.want)
			}
		})
	}
}

func TestQuantileSamples(t *testing.T) {
	samples := func(values ...float64) []metricSample {
		out := make([]metricSample, len(values))
		for i, v := range values {
			out[i] = metricSample{ts: rangeBase.Add(time.Duration(i) * time.Second), value: v}
		}
		return out
	}
	if got := quantileSamples(0.5, nil); !math.IsNaN(got) {
		t.Fatalf("empty quantile = %v, want NaN", got)
	}
	if got := quantileSamples(-1, samples(1, 2)); !math.IsInf(got, -1) {
		t.Fatalf("q<0 = %v, want -Inf", got)
	}
	if got := quantileSamples(2, samples(1, 2)); !math.IsInf(got, 1) {
		t.Fatalf("q>1 = %v, want +Inf", got)
	}
	if got := quantileSamples(0.5, samples(4)); got != 4 {
		t.Fatalf("single point = %v, want 4", got)
	}
	if got := quantileSamples(0.5, samples(3, 1, 2)); got != 2 {
		t.Fatalf("median = %v, want 2", got)
	}
	if got := quantileSamples(0.75, samples(1, 2, 3, 4)); got != 3.25 {
		t.Fatalf("interpolated = %v, want 3.25", got)
	}
	if got := quantileSamples(0.5, samples(math.NaN(), 5)); !math.IsNaN(got) {
		t.Fatalf("NaN quantile = %v, want NaN", got)
	}
	// Population variance of a single sample is zero.
	if got := stdvarSamples(samples(7)); got != 0 {
		t.Fatalf("single-sample stdvar = %v, want 0", got)
	}
}

func TestAdditionalRangeEmptyWindowsAndGaps(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(30*time.Second), "1", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(2*time.Minute+30*time.Second), "2", map[string]string{"app": "api"}),
	}
	e := testEngine()
	q := `sum_over_time({app="api"} | json | unwrap v [1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(3*time.Minute), time.Minute)
	res := mustQuery(t, e, newMetricSource(rows...), p)
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: rangeBase.Add(time.Minute), V: 1},
			{T: rangeBase.Add(3 * time.Minute), V: 2},
		},
	})

	// A row that falls between two windows of a large step creates no series.
	gap := mustQuery(t, e, newMetricSource(rows[0]), rangeParams(q, rangeBase, rangeBase.Add(10*time.Minute), 10*time.Minute))
	if got := resultPoints(t, gap); len(got) != 0 {
		t.Fatalf("gap row produced series %v", got)
	}
}

func TestAdditionalRangeFirstLastOrder(t *testing.T) {
	// Rows arrive unsorted from the caller; Select's Forward order makes first
	// and last follow ascending timestamps. Equal timestamps have no ordering
	// guarantee, so this only pins distinct ones.
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(30*time.Second), "3", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api"}),
	}
	e := testEngine()
	for _, tc := range []struct {
		q    string
		want float64
	}{
		{`first_over_time({app="api"} | json | unwrap v [1m])`, 1},
		{`last_over_time({app="api"} | json | unwrap v [1m])`, 3},
	} {
		res := mustQuery(t, e, newMetricSource(rows...), instantParams(tc.q, rangeBase.Add(time.Minute)))
		points := resultPoints(t, res)[lab("app", "api").String()]
		if len(points) != 1 || points[0].V != tc.want {
			t.Fatalf("%s = %v, want %v", tc.q, points, tc.want)
		}
	}
}

func TestAdditionalRangeGrouping(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api", "level": "info"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api", "level": "info"}),
		sampleRowAt(rangeBase.Add(30*time.Second), "4", map[string]string{"app": "api", "level": "warn"}),
	}
	at := rangeBase.Add(time.Minute)
	e := testEngine()
	// max_over_time is one of the functions the parser allows grouping on.
	cases := []struct {
		name  string
		query string
		want  map[string][]plan.Point
	}{
		{
			"no grouping keeps every non-value label",
			`max_over_time({app="api"} | json | unwrap v [1m])`,
			map[string][]plan.Point{
				lab("app", "api", "level", "info").String(): {{T: at, V: 2}},
				lab("app", "api", "level", "warn").String(): {{T: at, V: 4}},
			},
		},
		{
			"by keeps requested names only",
			`max_over_time({app="api"} | json | unwrap v [1m]) by (level)`,
			map[string][]plan.Point{
				lab("level", "info").String(): {{T: at, V: 2}},
				lab("level", "warn").String(): {{T: at, V: 4}},
			},
		},
		{
			"without removes names and the value label",
			`max_over_time({app="api"} | json | unwrap v [1m]) without (level)`,
			map[string][]plan.Point{lab("app", "api").String(): {{T: at, V: 4}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertResult(t, mustQuery(t, e, newMetricSource(rows...), instantParams(tc.query, at)), tc.want)
		})
	}

	// by () is Grouping.Singleton(): noLabels wins and every row collapses
	// into one empty-label series, so max over the three values is 4.
	byEmpty := mustQuery(t, e, newMetricSource(rows...), instantParams(`max_over_time({app="api"} | json | unwrap v [1m]) by ()`, at))
	assertResult(t, byEmpty, map[string][]plan.Point{
		lab().String(): {{T: at, V: 4}},
	})

	// sum_over_time does not allow grouping in LogQL; the parser rejects it.
	if _, err := Parse(`sum_over_time({app="api"} | unwrap v [1m]) by (level)`); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery for grouping", err)
	}
}

func TestAdditionalRangeParentGroupingSeam(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "2", map[string]string{"app": "api", "level": "info", "pod": "a"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "4", map[string]string{"app": "api", "level": "info", "pod": "b"}),
	}
	e := testEngine()
	p := rangeParams(`avg_over_time({app="api"} | json | unwrap v [1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	a := parseRangeAgg(t, `avg_over_time({app="api"} | json | unwrap v [1m])`)

	// The child projects the parent's by-grouping onto every returned series
	// but does not merge them: the parent owns the aggregation (merging before
	// avg would be wrong).
	series, err := e.evalRangeSeries(t.Context(), newMetricSource(rows...), a, p, rangeOptions{aggregate: true, child: true, group: []string{"level"}})
	if err != nil {
		t.Fatalf("evalRangeSeries: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("projected series = %d, want 2 (parent aggregates)", len(series))
	}
	values := make(map[float64]bool, len(series))
	for _, s := range series {
		if want := lab("level", "info"); !slices.Equal(s.labels, want) {
			t.Fatalf("projected labels = %v, want %v", s.labels, want)
		}
		if len(s.points) != 1 {
			t.Fatalf("points = %v, want one", s.points)
		}
		values[s.points[0].V] = true
	}
	if !values[2] || !values[4] {
		t.Fatalf("child values = %v, want the per-series averages 2 and 4", values)
	}

	// An empty parent grouping drops every label.
	series, err = e.evalRangeSeries(t.Context(), newMetricSource(rows...), a, p, rangeOptions{aggregate: true, child: true, group: []string{}})
	if err != nil {
		t.Fatalf("evalRangeSeries empty group: %v", err)
	}
	for _, s := range series {
		if len(s.labels) != 0 {
			t.Fatalf("empty parent grouping kept %v", s.labels)
		}
	}
}

func TestAdditionalRangeMaxSeries(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api", "env": "a"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api", "env": "b"}),
	}
	e := New(config.LogQL{MaxEntries: 100, MaxSeries: 1})
	p := rangeParams(`sum_over_time({app="api"} | json | unwrap v [1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	if _, err := e.Query(t.Context(), newMetricSource(rows...), p); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("err = %v, want ErrLimit for the assembled response", err)
	}
	a := parseRangeAgg(t, `sum_over_time({app="api"} | json | unwrap v [1m])`)
	series, err := e.evalRangeSeries(t.Context(), newMetricSource(rows...), a, p, rangeOptions{aggregate: true, child: true})
	if err != nil {
		t.Fatalf("child evaluation: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("child series = %d, want 2 (limit belongs to the parent)", len(series))
	}
}

func TestAdditionalRangeSelectionAndWindowBoundaries(t *testing.T) {
	e := testEngine()
	p := rangeParams(`sum_over_time({app="api"} | json | unwrap v [1m])`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute)
	src := newMetricSource(sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}))
	mustQuery(t, e, src, p)
	if src.selectCalls != 1 || src.aggregateCalls != 0 {
		t.Fatalf("calls = %d select / %d aggregate, want exactly one select", src.selectCalls, src.aggregateCalls)
	}
	sel := src.selections[0]
	if want := rangeBase.Add(-time.Minute + time.Microsecond); !sel.From.Equal(want) {
		t.Fatalf("From = %v, want %v", sel.From, want)
	}
	if want := rangeBase.Add(2*time.Minute + time.Microsecond); !sel.Through.Equal(want) {
		t.Fatalf("Through = %v, want %v", sel.Through, want)
	}
	if sel.Direction != plan.Forward || sel.Limit != 0 {
		t.Fatalf("selection = %+v, want Forward with no limit", sel)
	}
}

func TestAdditionalRangeOffsetAndUnusualTimes(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(-40*time.Second), "2", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(-10*time.Second), "5", map[string]string{"app": "api"}),
	}
	e := testEngine()
	at := rangeBase
	cases := []struct {
		name  string
		query string
		want  float64
	}{
		{"positive offset shifts the window back", `sum_over_time({app="api"} | json | unwrap v [1m] offset 30s)`, 2},
		{"negative offset looks forward", `sum_over_time({app="api"} | json | unwrap v [1m] offset -30s)`, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := mustQuery(t, e, newMetricSource(rows...), instantParams(tc.query, at))
			points := resultPoints(t, res)[lab("app", "api").String()]
			if len(points) != 1 || points[0].V != tc.want {
				t.Fatalf("points = %v, want %v", points, tc.want)
			}
		})
	}

	// Sub-microsecond evaluation times floor onto the stored grid: the first
	// window still ends at the last microsecond before the evaluation and the
	// second begins one microsecond later.
	start := rangeBase.Add(500 * time.Nanosecond)
	subRows := []plan.Row{
		sampleRowAt(rangeBase.Add(-30*time.Second), "2", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(30*time.Second), "3", map[string]string{"app": "api"}),
	}
	// The frontend aligns the sub-microsecond request onto the minute grid:
	// 03:04:00.0000005..03:05:00.0000005 becomes 03:04..03:06.
	sub := mustQuery(t, e, newMetricSource(subRows...), rangeParams(
		`sum_over_time({app="api"} | json | unwrap v [1m])`, start, start.Add(time.Minute), time.Minute))
	assertResult(t, sub, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: rangeBase, V: 2},
			{T: rangeBase.Add(time.Minute), V: 3},
		},
	})

	// Negative Unix timestamps floor correctly too.
	neg := time.Unix(-100, 0).UTC()
	negRows := []plan.Row{
		sampleRowAt(neg.Add(-30*time.Second), "2", map[string]string{"app": "api"}),
		sampleRowAt(neg.Add(-10*time.Second), "5", map[string]string{"app": "api"}),
	}
	negRes := mustQuery(t, e, newMetricSource(negRows...), instantParams(`sum_over_time({app="api"} | json | unwrap v [1m])`, neg))
	assertResult(t, negRes, map[string][]plan.Point{
		lab("app", "api").String(): {{T: neg, V: 7}},
	})
}

func TestAdditionalRangeCancellationAndSourceErrors(t *testing.T) {
	e := testEngine()
	p := instantParams(`sum_over_time({app="api"} | json | unwrap v [1m])`, rangeBase.Add(time.Minute))

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	cancelSrc := newMetricSource(sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}))
	if _, err := e.Query(canceled, cancelSrc, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if cancelSrc.selectCalls != 0 {
		t.Fatalf("canceled query opened the Source %d times", cancelSrc.selectCalls)
	}

	mid, midCancel := context.WithCancel(t.Context())
	midSrc := newMetricSource(sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}))
	midSrc.onSelect = func(int) { midCancel() }
	if _, err := e.Query(mid, midSrc, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-iteration err = %v, want context.Canceled", err)
	}

	boom := errors.New("boom")
	failing := newMetricSource(
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api"}),
	)
	failing.err, failing.selectErrAfter = boom, 1
	if _, err := e.Query(t.Context(), failing, p); !errors.Is(err, boom) {
		t.Fatalf("source err = %v, want boom", err)
	}
}

func TestAdditionalRangeAliasedRowsAndImmutability(t *testing.T) {
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api", "level": "info"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api", "level": "info"}),
		sampleRowAt(rangeBase.Add(30*time.Second), "3", map[string]string{"app": "api", "level": "info"}),
	}
	before := make([]logs.Labels, len(rows))
	for i, r := range rows {
		before[i] = slices.Clone(r.Labels)
	}
	src := newMetricSource(rows...)
	src.mutate = true // the Source reuses its label buffers between yields
	res := mustQuery(t, testEngine(), src, instantParams(
		`sum_over_time({app="api"} | json | unwrap v [1m])`, rangeBase.Add(time.Minute)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "level", "info").String(): {{T: rangeBase.Add(time.Minute), V: 6}},
	})
	for i, r := range rows {
		if !slices.Equal(r.Labels, before[i]) {
			t.Fatalf("row %d labels mutated: %v, want %v", i, r.Labels, before[i])
		}
	}
}

func TestAbsentOverTime(t *testing.T) {
	e := testEngine()
	plain := `absent_over_time({app="api"}[1m])`

	// No data at all: one sample is 1.
	empty := newMetricSource()
	res := mustQuery(t, e, empty, instantParams(plain, rangeBase))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase, V: 1}},
	})
	if empty.selectCalls != 1 {
		t.Fatalf("plain absent Select calls = %d, want 1", empty.selectCalls)
	}

	// Present data: no sample at all.
	present := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), "a", map[string]string{"app": "api"}, nil))
	if got := resultPoints(t, mustQuery(t, e, present, instantParams(plain, rangeBase))); len(got) != 0 {
		t.Fatalf("present window produced %v", got)
	}

	// Step gaps: only empty windows get a 1.
	rangeRes := mustQuery(t, e, newMetricSource(metricRowAt(rangeBase.Add(30*time.Second), "a", map[string]string{"app": "api"}, nil)),
		rangeParams(plain, rangeBase, rangeBase.Add(3*time.Minute), time.Minute))
	assertResult(t, rangeRes, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: rangeBase, V: 1},
			{T: rangeBase.Add(2 * time.Minute), V: 1},
			{T: rangeBase.Add(3 * time.Minute), V: 1},
		},
	})

	// Labels come from the selector's equality matchers only; a non-equality
	// matcher's label is removed.
	labels := mustQuery(t, e, newMetricSource(), instantParams(
		`absent_over_time({app="api", env=~"prod", tier="web"}[1m])`, rangeBase))
	assertResult(t, labels, map[string][]plan.Point{
		lab("app", "api", "tier", "web").String(): {{T: rangeBase, V: 1}},
	})

	// The pipeline still filters: a row the line filter rejects leaves the
	// window absent.
	filtered := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), "ok", map[string]string{"app": "api"}, nil))
	if got := resultPoints(t, mustQuery(t, e, filtered, instantParams(`absent_over_time({app="api"} |= "err" [1m])`, rangeBase))); len(got) == 0 {
		t.Fatalf("filtered row left the window present")
	}
}

func TestAbsentOverTimeUnwrap(t *testing.T) {
	e := testEngine()
	// A row without the unwrap field contributes no sample, so the window is
	// absent.
	src := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), `{"other":"1"}`, map[string]string{"app": "api"}, nil))
	res := mustQuery(t, e, src, instantParams(`absent_over_time({app="api"} | json | unwrap v [1m])`, rangeBase))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase, V: 1}},
	})

	// A present and convertible value makes the window present.
	ok := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), `{"v":"3"}`, map[string]string{"app": "api"}, nil))
	if got := resultPoints(t, mustQuery(t, e, ok, instantParams(`absent_over_time({app="api"} | json | unwrap v [1m])`, rangeBase))); len(got) != 0 {
		t.Fatalf("present unwrapped row produced %v", got)
	}

	// A failed conversion is a metric error, unless a post filter clears it;
	// then the row is dropped and the window is absent.
	bad := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), `{"v":"oops"}`, map[string]string{"app": "api"}, nil))
	if _, err := e.Query(t.Context(), bad, instantParams(`absent_over_time({app="api"} | json | unwrap v [1m])`, rangeBase)); !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), sampleExtractionErr) {
		t.Fatalf("err = %v, want SampleExtractionErr", err)
	}
	cleared := mustQuery(t, e, newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), `{"v":"oops"}`, map[string]string{"app": "api"}, nil)),
		instantParams(`absent_over_time({app="api"} | json | unwrap v | __error__="" [1m])`, rangeBase))
	assertResult(t, cleared, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase, V: 1}},
	})
}

func TestAbsentOverTimeZeroRange(t *testing.T) {
	// A zero range window is empty for every evaluation, so absent_over_time
	// returns 1 at each step and needs no rows.
	src := newMetricSource(metricRowAt(rangeBase.Add(-10*time.Second), "a", map[string]string{"app": "api"}, nil))
	res := mustQuery(t, testEngine(), src, rangeParams(`absent_over_time({app="api"}[0s])`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: rangeBase, V: 1},
			{T: rangeBase.Add(time.Minute), V: 1},
			{T: rangeBase.Add(2 * time.Minute), V: 1},
		},
	})
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("zero-range absent opened the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}
}

func TestAdditionalRangeRateCounterStillUnsupported(t *testing.T) {
	src := newMetricSource(sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api"}))
	_, err := testEngine().Query(t.Context(), src, instantParams(`rate_counter({app="api"} | json | unwrap v [1m])`, rangeBase.Add(time.Minute)))
	if !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("rate_counter touched the Source")
	}
}

func TestPlainRateKeepsBucketFastPath(t *testing.T) {
	// Plain rate stays in A7's count/bytes family: one Aggregate, no Select.
	src := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil))
	res := mustQuery(t, testEngine(), src, instantParams(`rate({app="api"}[1m])`, rangeBase.Add(time.Minute)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 1.0 / 60}},
	})
	if src.aggregateCalls != 1 || src.selectCalls != 0 {
		t.Fatalf("plain rate calls = %d aggregate / %d select, want one aggregate", src.aggregateCalls, src.selectCalls)
	}
}

func TestValidateAdditionalRange(t *testing.T) {
	rangeOf := func(u *Unwrap) *LogRange { return &LogRange{Pipeline: &LogQuery{}, Range: time.Minute, Unwrap: u} }
	cases := []struct {
		name string
		agg  *RangeAgg
		want error
	}{
		{"nil aggregation", nil, plan.ErrBadQuery},
		{"nil range", &RangeAgg{Func: SumOverTime}, plan.ErrBadQuery},
		{"sum needs unwrap", &RangeAgg{Func: SumOverTime, Range: rangeOf(nil)}, plan.ErrBadQuery},
		{"count rejects unwrap", &RangeAgg{Func: CountOverTime, Range: rangeOf(&Unwrap{Label: "v"})}, plan.ErrBadQuery},
		{"quantile needs a parameter", &RangeAgg{Func: QuantileOverTime, Range: rangeOf(&Unwrap{Label: "v"})}, plan.ErrBadQuery},
		{"only quantile takes a parameter", &RangeAgg{Func: SumOverTime, Params: []float64{1}, Range: rangeOf(&Unwrap{Label: "v"})}, plan.ErrBadQuery},
		{"grouping must be allowed", &RangeAgg{Func: SumOverTime, Grouping: &Grouping{Names: []string{"a"}}, Range: rangeOf(&Unwrap{Label: "v"})}, plan.ErrBadQuery},
		{"absent rejects grouping", &RangeAgg{Func: AbsentOverTime, Grouping: &Grouping{}, Range: rangeOf(nil)}, plan.ErrBadQuery},
		{"valid sum", &RangeAgg{Func: SumOverTime, Range: rangeOf(&Unwrap{Label: "v"})}, nil},
		{"valid grouped max", &RangeAgg{Func: MaxOverTime, Grouping: &Grouping{Names: []string{"a"}}, Range: rangeOf(&Unwrap{Label: "v"})}, nil},
		{"valid absent", &RangeAgg{Func: AbsentOverTime, Range: rangeOf(nil)}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAdditionalRange(tc.agg)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("validateAdditionalRange = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("validateAdditionalRange = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestAdditionalRangeDuplicateFinalSeries(t *testing.T) {
	// Grouping merges every source identity with the same by-labels into one
	// series; the response never carries duplicate label sets.
	rows := []plan.Row{
		sampleRowAt(rangeBase.Add(10*time.Second), "1", map[string]string{"app": "api", "level": "info", "pod": "a"}),
		sampleRowAt(rangeBase.Add(20*time.Second), "2", map[string]string{"app": "api", "level": "info", "pod": "b"}),
	}
	res := mustQuery(t, testEngine(), newMetricSource(rows...), instantParams(
		`max_over_time({app="api"} | json | unwrap v [1m]) by (level)`, rangeBase.Add(time.Minute)))
	if len(res.Vector) != 1 {
		t.Fatalf("series = %v, want one merged series", resultPoints(t, res))
	}
	if want := lab("level", "info"); !slices.Equal(res.Vector[0].Metric, want) {
		t.Fatalf("labels = %v, want %v", res.Vector[0].Metric, want)
	}
	if res.Vector[0].Point.V != 2 {
		t.Fatalf("value = %v, want 2", res.Vector[0].Point.V)
	}
}

// cancelAfterSource cancels the request immediately after the wrapped Source
// finishes yielding, before the engine's CPU finishing work. The engine must
// still fail with context.Canceled and must not call the Source again.
type cancelAfterSource struct {
	*metricSource
	cancel context.CancelFunc
}

func (s cancelAfterSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		for row, err := range s.metricSource.Select(ctx, sel) {
			if !yield(row, err) {
				return
			}
		}
		s.cancel()
	}
}

func (s cancelAfterSource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		for b, err := range s.metricSource.Aggregate(ctx, sel, agg) {
			if !yield(b, err) {
				return
			}
		}
		s.cancel()
	}
}

func TestMetricCancellationAfterSourceCompletion(t *testing.T) {
	row := metricRowAt(rangeBase, `{"n":3}`, map[string]string{"app": "api"}, nil)
	for _, q := range []string{
		`sum_over_time({app="api"} | json | unwrap n [1m])`,
		`max_over_time({app="api"} | json | unwrap n [1m])`,
		`count_over_time({app="api"}[1m])`,
		`bytes_over_time({app="api"}[1m])`,
	} {
		t.Run(q, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			src := cancelAfterSource{newMetricSource(row), cancel}
			if _, err := testEngine().Query(ctx, src, instantParams(q, rangeBase.Add(time.Second))); !errors.Is(err, context.Canceled) {
				t.Fatalf("completed after Source cancellation: %v", err)
			}
		})
	}
}

func TestMetricAggregateFallbackStopsAfterCancellation(t *testing.T) {
	// A range wider than the bucket bound takes one exact Aggregate per
	// window. Cancellation after the first window must be observed before the
	// second Source call, not at the end of the query.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := cancelAfterSource{newMetricSource(metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil)), cancel}
	q := `count_over_time({app="api"}[2ms])`
	p := rangeParams(q, rangeBase, rangeBase.Add(2500*time.Nanosecond), 2500*time.Nanosecond)
	// Lower-level exact grid: Engine.Query normalizes this sub-ms step first.
	if _, err := testEngine().evalRangeAgg(ctx, src, parseRangeAgg(t, q), p, rangeOptions{aggregate: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if src.aggregateCalls != 1 {
		t.Fatalf("Aggregate calls = %d, want 1 (no Source call after cancel)", src.aggregateCalls)
	}
}

func TestMetricSelectRetainsErrorDetailsAfterDropError(t *testing.T) {
	// A parser error sets __error__ and __error_details__; drop __error__
	// clears only the error. GroupedLabels then takes the modified ungrouped
	// path (LabelsResult), which appends the retained details as a parsed
	// label, so the metric identity keeps __error_details__.
	row := metricRowAt(rangeBase, `{"x":1,"broken":}`, map[string]string{"app": "api"}, nil)
	e := testEngine()
	compiled, err := e.CompileLogQuery(`{app="api"} | json | drop __error__`)
	if err != nil {
		t.Fatal(err)
	}
	_, entry, keep, err := compiled.Process(t.Context(), instantParams("", rangeBase).Tenant, row)
	if err != nil || !keep {
		t.Fatalf("Process keep=%v err=%v", keep, err)
	}
	details, ok := entry.Parsed.Get("__error_details__")
	if !ok || details == "" {
		t.Fatalf("fixture lacks retained parser details: %v", entry.Parsed)
	}

	src := newMetricSource(row)
	src.mutate = true // the Source reuses label buffers between yields
	res := mustQuery(t, e, src, instantParams(`count_over_time({app="api"} | json | drop __error__[1m])`, rangeBase.Add(time.Second)))
	metric := res.Vector[0].Metric
	if got, ok := metric.Get("__error_details__"); !ok || got != details {
		t.Fatalf("metric lost retained details: %v; want %q", metric, details)
	}
	if got, ok := metric.Get("x"); !ok || got != "1" {
		t.Fatalf("metric lost the parsed label: %v", metric)
	}
}

func TestMetricAggregateRetainsErrorDetailsAfterDropError(t *testing.T) {
	// Only a drop stage, so the Aggregate fast path handles the pipeline.
	// Structured metadata feeds the builder error state and also adds an
	// ordinary label (foo): the builder is modified, so the ungrouped identity
	// keeps the retained details while dropping only __error__.
	row := plan.Row{
		TS:     rangeBase,
		Line:   "a",
		Labels: logs.FromMap(map[string]string{"app": "api", "level": "info"}),
		Meta:   logs.FromMap(map[string]string{errorLabel: "Err", errorDetailsLabel: "detail", "foo": "bar"}),
	}
	e := testEngine()
	src := newMetricSource(row)
	q := `count_over_time({app="api"} | drop __error__[1m])`
	res := mustQuery(t, e, src, instantParams(q, rangeBase.Add(time.Second)))
	if src.aggregateCalls != 1 || src.selectCalls != 0 {
		t.Fatalf("calls = %d aggregate / %d select, want the Aggregate path", src.aggregateCalls, src.selectCalls)
	}
	metric := res.Vector[0].Metric
	if got, ok := metric.Get(errorDetailsLabel); !ok || got != "detail" {
		t.Fatalf("metric lost retained details: %v", metric)
	}
	if got, ok := metric.Get("foo"); !ok || got != "bar" {
		t.Fatalf("metric lost the added metadata label: %v", metric)
	}
}

func TestMetricAggregateUnchangedFastPathOmitsErrorState(t *testing.T) {
	// Error names in structured metadata feed only the builder state: without
	// any ordinary add or delete, GroupedLabels returns the untouched base
	// labels, so neither the cleared error nor the retained details appear.
	row := plan.Row{
		TS:     rangeBase,
		Line:   "a",
		Labels: logs.FromMap(map[string]string{"app": "api"}),
		Meta:   logs.FromMap(map[string]string{errorLabel: "Err", errorDetailsLabel: "detail"}),
	}
	res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(
		`count_over_time({app="api"} | drop __error__[1m])`, rangeBase.Add(time.Second)))
	if got := res.Vector[0].Metric; !slices.Equal(got, lab("app", "api")) {
		t.Fatalf("unchanged bucket identity = %v, want the base labels only", got)
	}
}

func TestMetricIdentityOmitsDroppedErrorDetails(t *testing.T) {
	row := metricRowAt(rangeBase, `{"x":1,"broken":}`, map[string]string{"app": "api"}, nil)
	res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(
		`count_over_time({app="api"} | json | drop __error__, __error_details__[1m])`, rangeBase.Add(time.Second)))
	metric := res.Vector[0].Metric
	if _, ok := metric.Get(errorDetailsLabel); ok {
		t.Fatalf("explicitly dropped details survive: %v", metric)
	}
	if got, ok := metric.Get("x"); !ok || got != "1" {
		t.Fatalf("metric lost the parsed label: %v", metric)
	}
}

func TestMetricIdentityRetainsDetailsAfterMetadataAddThenDrop(t *testing.T) {
	// An ordinary metadata label is added and then dropped, while __error__
	// is cleared and __error_details__ remains. The add/delete history is not
	// erased by the later delete, so the modified ungrouped path still appends
	// the retained details.
	row := plan.Row{
		TS:     rangeBase,
		Line:   "a",
		Labels: logs.FromMap(map[string]string{"app": "api"}),
		Meta:   logs.FromMap(map[string]string{errorLabel: "Err", errorDetailsLabel: "detail", "foo": "bar"}),
	}
	src := newMetricSource(row)
	res := mustQuery(t, testEngine(), src, instantParams(
		`count_over_time({app="api"} | drop __error__, foo [1m])`, rangeBase.Add(time.Second)))
	if src.aggregateCalls != 1 {
		t.Fatalf("Aggregate calls = %d, want the bucket path", src.aggregateCalls)
	}
	metric := res.Vector[0].Metric
	if got, ok := metric.Get(errorDetailsLabel); !ok || got != "detail" {
		t.Fatalf("metric lost retained details after metadata add+drop: %v", metric)
	}
	if _, ok := metric.Get("foo"); ok {
		t.Fatalf("dropped metadata label survives: %v", metric)
	}
}

func TestMetricIdentityUnchangedFastPathOmitsDetails(t *testing.T) {
	// The parser fails before extracting anything and drop __error__ clears
	// the error: no addition or deletion happened, so GroupedLabels returns
	// the untouched base labels and must not append the retained details.
	row := metricRowAt(rangeBase, `{"broken":}`, map[string]string{"app": "api"}, nil)
	res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(
		`count_over_time({app="api"} | json | drop __error__[1m])`, rangeBase.Add(time.Second)))
	metric := res.Vector[0].Metric
	if !slices.Equal(metric, lab("app", "api")) {
		t.Fatalf("unchanged identity = %v, want the base labels only", metric)
	}
	if _, ok := metric.Get(errorDetailsLabel); ok {
		t.Fatalf("unchanged fast path appended details: %v", metric)
	}
}

func TestMetricIdentityGroupedOmitsDetails(t *testing.T) {
	// Grouped paths (by/without) do not append the error state, even though
	// the ungrouped identity keeps the retained details.
	row := metricRowAt(rangeBase, `{"n":3,"broken":}`, map[string]string{"app": "api", "level": "info"}, nil)
	at := rangeBase.Add(time.Second)
	e := testEngine()
	byRes := mustQuery(t, e, newMetricSource(row), instantParams(
		`max_over_time({app="api"} | json | drop __error__ | unwrap n [1m]) by (level)`, at))
	if got := byRes.Vector[0].Metric; !slices.Equal(got, lab("level", "info")) {
		t.Fatalf("by identity = %v, want {level=info} without details", got)
	}
	withoutRes := mustQuery(t, e, newMetricSource(row), instantParams(
		`max_over_time({app="api"} | json | drop __error__ | unwrap n [1m]) without (level)`, at))
	if got := withoutRes.Vector[0].Metric; !slices.Equal(got, lab("app", "api")) {
		t.Fatalf("without identity = %v, want {app=api} without details", got)
	}

	// The ungrouped unwrap identity also omits details: the extractor expresses
	// its implicit value-label removal as a without grouping.
	raw := mustQuery(t, e, newMetricSource(row), instantParams(
		`sum_over_time({app="api"} | json | drop __error__ | unwrap n [1m])`, at))
	if _, ok := raw.Vector[0].Metric.Get(errorDetailsLabel); ok {
		t.Fatalf("ungrouped unwrap promoted builder details: %v", raw.Vector[0].Metric)
	}
	if want := lab("app", "api", "level", "info"); !slices.Equal(raw.Vector[0].Metric, want) {
		t.Fatalf("ungrouped unwrap identity = %v, want %v", raw.Vector[0].Metric, want)
	}
	if got := raw.Vector[0].Point.V; got != 3 {
		t.Fatalf("value = %v, want 3", got)
	}
}

func TestMetricIdentityRangeByEmptySingleton(t *testing.T) {
	// Explicit by() is Grouping.Singleton(): noLabels wins, so every row lands
	// in one empty-label series (the unwrap value label is not preserved).
	row := metricRowAt(rangeBase, `{"n":3,"broken":}`, map[string]string{"app": "api", "level": "info"}, nil)
	res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(
		`max_over_time({app="api"} | json | drop __error__ | unwrap n [1m]) by ()`, rangeBase.Add(time.Second)))
	if len(res.Vector) != 1 || len(res.Vector[0].Metric) != 0 {
		t.Fatalf("by () identity = %v, want one empty-label series", res.Vector)
	}
	if res.Vector[0].Point.V != 3 {
		t.Fatalf("value = %v, want 3", res.Vector[0].Point.V)
	}
}

func TestAdditionalRangeInjectedProjectionDetails(t *testing.T) {
	// The parent's injected by-grouping is an optimization applied after the
	// exact child identity: a non-empty projection uses withResult and never
	// promotes the builder error state, even when __error_details__ is named;
	// an empty projection yields no labels; nil leaves the exact child labels.
	row := metricRowAt(rangeBase, `{"n":3,"broken":}`, map[string]string{"app": "api", "level": "info"}, nil)
	q := `sum_over_time({app="api"} | json | drop __error__ | unwrap n [1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(time.Second), time.Second)
	a := parseRangeAgg(t, q)
	e := testEngine()

	raw, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true, child: true})
	if err != nil {
		t.Fatalf("raw child: %v", err)
	}
	if len(raw) != 1 {
		t.Fatalf("raw child series = %d, want 1", len(raw))
	}
	// The unwrap extractor's implicit value-label removal is a without
	// grouping, so even the raw child omits the retained builder details.
	if want := lab("app", "api", "level", "info"); !slices.Equal(raw[0].labels, want) {
		t.Fatalf("raw child labels = %v, want %v without details", raw[0].labels, want)
	}

	by, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true, child: true, group: []string{"level", errorDetailsLabel}})
	if err != nil {
		t.Fatalf("projected child: %v", err)
	}
	if len(by) != 1 || !slices.Equal(by[0].labels, lab("level", "info")) {
		t.Fatalf("injected by labels = %v, want {level=info} without builder details", by)
	}

	empty, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true, child: true, group: []string{}})
	if err != nil {
		t.Fatalf("empty-projection child: %v", err)
	}
	if len(empty) != 1 || len(empty[0].labels) != 0 {
		t.Fatalf("empty projection labels = %v, want none", empty)
	}
}

func TestMetricBuilderIdentityUnchanged(t *testing.T) {
	// An untouched builder returns currentResult for no grouping and
	// toBaseGroup for a grouping: empty base values survive the by path,
	// unlike withResult, and no builder error state can be appended.
	base := lab("app", "api", "empty", "", "latency", "5", "level", "info")
	e := &logEntry{labels: slices.Clone(base), base: base}
	cases := []struct {
		name   string
		group  *Grouping
		unwrap string
		want   logs.Labels
	}{
		{"line extractor no grouping", nil, "", base},
		{"unwrap no grouping removes the value label", nil, "latency", lab("app", "api", "empty", "", "level", "info")},
		{"by keeps the requested base label", &Grouping{Names: []string{"level"}}, "latency", lab("level", "info")},
		{"by keeps an empty base value", &Grouping{Names: []string{"empty"}}, "latency", lab("empty", "")},
		{"without removes names and the value label", &Grouping{Without: true, Names: []string{"level"}}, "latency", lab("app", "api", "empty", "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := metricBuilderIdentity(e, tc.group, tc.unwrap)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("metricBuilderIdentity = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMetricBuilderIdentityErrorPath(t *testing.T) {
	// A live error ignores any grouping and appends __error__ and
	// __error_details__ last, replacing a parsed label that happens to carry
	// the same name, exactly like LabelsResult/entryParsed.
	e := &logEntry{
		labels:         lab("app", "api"),
		parsed:         lab(errorDetailsLabel, "parsed", "x", "1"),
		err:            "Err",
		errDetails:     "detail",
		builderMutated: true,
	}
	want := lab(errorLabel, "Err", errorDetailsLabel, "detail", "app", "api", "x", "1")
	for _, g := range []*Grouping{nil, {Names: []string{"app"}}, {Without: true, Names: []string{"x"}}} {
		if got := metricBuilderIdentity(e, g, ""); !slices.Equal(got, want) {
			t.Fatalf("identity with %v = %v, want %v", g, got, want)
		}
	}
}

func TestAdditionalRangeCPUCancellationGuards(t *testing.T) {
	// The CPU-only phases observe cancellation too, not just the Source loop.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	windows := metricWindows([]time.Time{rangeBase}, 0, time.Minute)
	if _, err := samplePoints(ctx, nil, windows, SumOverTime, nil, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("samplePoints err = %v, want context.Canceled", err)
	}
	if _, err := absentPoints(ctx, windows, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("absentPoints err = %v, want context.Canceled", err)
	}
	var set sampleSet
	set.get(lab("app", "api"), lab("app", "api")).samples = []metricSample{{ts: rangeBase, value: 1}}
	if _, err := set.points(ctx, SumOverTime, nil, windows, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("points err = %v, want context.Canceled", err)
	}
	c, err := testEngine().CompileLogQuery(`{app="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := absentRangeSeries(ctx, c, windows, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("absentRangeSeries err = %v, want context.Canceled", err)
	}
}

func TestMetricBuilderIdentityDeleteThenReadd(t *testing.T) {
	// Loki's del list is checked before additions: by() hides a name that was
	// deleted and then re-added, while the raw LabelsResult and without() keep
	// the re-added parsed label (without only removes its named groups). get()
	// itself is untouched and still resolves the addition.
	e := &logEntry{
		labels:         lab("app", "api"),
		parsed:         lab("n", "3", "x", "restored"),
		builderMutated: true,
		builderDeleted: map[string]struct{}{"x": {}},
	}
	cases := []struct {
		name   string
		group  *Grouping
		unwrap string
		want   logs.Labels
	}{
		{"by drops the deleted name", &Grouping{Names: []string{"x"}}, "n", nil},
		{"without removes only its groups", &Grouping{Without: true, Names: []string{"x"}}, "n", lab("app", "api")},
		{"raw unwrap keeps the re-added label", nil, "n", lab("app", "api", "x", "restored")},
		{"line extractor raw keeps the re-added label", nil, "", lab("app", "api", "n", "3", "x", "restored")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := metricBuilderIdentity(e, tc.group, tc.unwrap); !slices.Equal(got, tc.want) {
				t.Fatalf("metricBuilderIdentity = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMetricDeleteThenReaddGrouping(t *testing.T) {
	row := metricRowAt(rangeBase, `{"x":"old","n":"3"}`, map[string]string{"app": "api"}, nil)
	at := rangeBase.Add(time.Second)
	e := testEngine()
	base := `{app="api"} | json | drop x | label_format x="restored" | unwrap n [1m]`

	// by(x): the deleted name is hidden even though label_format re-added it.
	by := mustQuery(t, e, newMetricSource(row), instantParams(`max_over_time(`+base+`) by (x)`, at))
	if len(by.Vector) != 1 || len(by.Vector[0].Metric) != 0 {
		t.Fatalf("by identity = %v, want empty group labels", by.Vector)
	}
	// Raw identity keeps the re-added parsed label, with the value label gone.
	raw := mustQuery(t, e, newMetricSource(row), instantParams(`max_over_time(`+base+`)`, at))
	if want := lab("app", "api", "x", "restored"); !slices.Equal(raw.Vector[0].Metric, want) {
		t.Fatalf("raw identity = %v, want %v", raw.Vector[0].Metric, want)
	}
	// without(x) removes x; the unwrap value label is removed as well.
	without := mustQuery(t, e, newMetricSource(row), instantParams(`max_over_time(`+base+`) without (x)`, at))
	if want := lab("app", "api"); !slices.Equal(without.Vector[0].Metric, want) {
		t.Fatalf("without identity = %v, want %v", without.Vector[0].Metric, want)
	}
}

func TestVectorSumInjectedUnchangedEmptyGroup(t *testing.T) {
	// An unchanged builder grouped by an empty-valued base label keeps that
	// label, and the Select and Aggregate child paths must agree because the
	// injected parent sum grouping goes through the same GroupedLabels branch.
	row := metricRowAt(rangeBase, "a", map[string]string{"app": "api", "empty": ""}, nil)
	at := rangeBase.Add(time.Second)
	e := testEngine()
	want := map[string][]plan.Point{lab("empty", "").String(): {{T: at, V: 1}}}
	fast := mustQuery(t, e, newMetricSource(row), instantParams(`sum by (empty) (count_over_time({app="api"}[1m]))`, at))
	assertResult(t, fast, want)
	slowSrc := newMetricSource(row)
	slow := mustQuery(t, e, slowSrc, instantParams(`sum by (empty) (count_over_time({app="api"} | line_format "a" [1m]))`, at))
	if slowSrc.selectCalls != 1 || slowSrc.aggregateCalls != 0 {
		t.Fatalf("line_format child calls = %d select / %d aggregate, want the Select path", slowSrc.selectCalls, slowSrc.aggregateCalls)
	}
	assertResult(t, slow, want)
	assertSameResult(t, slow, fast)
}

func TestVectorSumInjectedProjectionDeleteThenReadd(t *testing.T) {
	// The Select child path applies the injected by-grouping after the full
	// error check and hides the deleted name, matching the Aggregate path.
	row := metricRowAt(rangeBase, `{"x":"old"}`, map[string]string{"app": "api"}, nil)
	at := rangeBase.Add(time.Second)
	res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(
		`sum by (x) (count_over_time({app="api"} | json | drop x | label_format x="restored" [1m]))`, at))
	if len(res.Vector) != 1 || len(res.Vector[0].Metric) != 0 {
		t.Fatalf("sum by(x) = %v, want one empty-label series", res.Vector)
	}
	if res.Vector[0].Point.V != 1 {
		t.Fatalf("value = %v, want 1", res.Vector[0].Point.V)
	}
}

func TestVectorSumInjectedUnwrapValueGrouping(t *testing.T) {
	// canInjectVectorGrouping includes rate and sum_over_time: for an unwrap
	// child with no range grouping the parent's by() is injected into the
	// extractor identity itself, so the requested value label is a real group
	// (and the collection key must carry it too).
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), `{"n":"1"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"n":"2"}`, map[string]string{"app": "api"}, nil),
	}
	at := rangeBase.Add(time.Minute)
	e := testEngine()
	rate := mustQuery(t, e, newMetricSource(rows...), instantParams(
		`sum by (n) (rate({app="api"} | json | unwrap n [1m]))`, at))
	assertResult(t, rate, map[string][]plan.Point{
		lab("n", "1").String(): {{T: at, V: 1.0 / 60}},
		lab("n", "2").String(): {{T: at, V: 2.0 / 60}},
	})
	total := mustQuery(t, e, newMetricSource(rows...), instantParams(
		`sum(rate({app="api"} | json | unwrap n [1m]))`, at))
	assertResult(t, total, map[string][]plan.Point{
		lab().String(): {{T: at, V: 3.0 / 60}},
	})
	sum := mustQuery(t, e, newMetricSource(rows...), instantParams(
		`sum by (n) (sum_over_time({app="api"} | json | unwrap n [1m]))`, at))
	assertResult(t, sum, map[string][]plan.Point{
		lab("n", "1").String(): {{T: at, V: 1}},
		lab("n", "2").String(): {{T: at, V: 2}},
	})
}

func TestInjectGroupingEligibility(t *testing.T) {
	// Pinned canInjectVectorGrouping: only the linear range operations, and
	// only when the range has no grouping. The unwrap path shares that list for
	// rate and sum_over_time.
	for _, fn := range []RangeFunc{BytesOverTime, BytesRate, SumOverTime, Rate, CountOverTime} {
		if !canInjectVectorGrouping(fn) {
			t.Fatalf("canInjectVectorGrouping(%s) = false, want true", fn)
		}
	}
	for _, fn := range []RangeFunc{AvgOverTime, MaxOverTime, QuantileOverTime, AbsentOverTime, FirstOverTime, LastOverTime} {
		if canInjectVectorGrouping(fn) {
			t.Fatalf("canInjectVectorGrouping(%s) = true, want false", fn)
		}
	}
	for _, tc := range []struct {
		name  string
		agg   *RangeAgg
		where bool
	}{
		{"rate", &RangeAgg{Func: Rate}, true},
		{"sum over time", &RangeAgg{Func: SumOverTime}, true},
		{"avg is not linear", &RangeAgg{Func: AvgOverTime}, false},
		{"count is A7's", &RangeAgg{Func: CountOverTime}, false},
		{"range grouping blocks injection", &RangeAgg{Func: Rate, Grouping: &Grouping{Names: []string{"x"}}}, false},
	} {
		if got := canInjectUnwrapGrouping(tc.agg); got != tc.where {
			t.Fatalf("canInjectUnwrapGrouping(%s) = %v, want %v", tc.name, got, tc.where)
		}
	}
}

func TestMetricBucketProjectionDetailsFollowsGrouping(t *testing.T) {
	// Bucket projection applies the same GroupedLabels rule: a raw bucket
	// identity keeps retained details, a parent by-grouping does not.
	row := plan.Row{
		TS:     rangeBase,
		Line:   "a",
		Labels: logs.FromMap(map[string]string{"app": "api", "level": "info"}),
		Meta:   logs.FromMap(map[string]string{errorLabel: "Err", errorDetailsLabel: "detail", "foo": "bar"}),
	}
	e := testEngine()
	p := rangeParams(`count_over_time({app="api"} | drop __error__[1m])`, rangeBase, rangeBase.Add(time.Second), time.Second)
	a := parseRangeAgg(t, `count_over_time({app="api"} | drop __error__[1m])`)

	raw, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true})
	if err != nil {
		t.Fatalf("raw bucket evaluation: %v", err)
	}
	if len(raw) != 1 {
		t.Fatalf("raw series = %d, want 1", len(raw))
	}
	if got, ok := raw[0].labels.Get(errorDetailsLabel); !ok || got != "detail" {
		t.Fatalf("raw bucket lost retained details: %v", raw[0].labels)
	}
	if got, ok := raw[0].labels.Get("foo"); !ok || got != "bar" {
		t.Fatalf("raw bucket lost the added metadata label: %v", raw[0].labels)
	}

	grouped, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true, group: []string{"level"}})
	if err != nil {
		t.Fatalf("grouped bucket evaluation: %v", err)
	}
	if len(grouped) != 1 || !slices.Equal(grouped[0].labels, lab("level", "info")) {
		t.Fatalf("grouped bucket identity = %v, want {level=info} without details", grouped)
	}

	// An empty injected projection is applied after the exact identity.
	empty, err := e.evalRangeSeries(t.Context(), newMetricSource(row), a, p, rangeOptions{aggregate: true, group: []string{}})
	if err != nil {
		t.Fatalf("empty-projection bucket evaluation: %v", err)
	}
	if len(empty) != 1 || len(empty[0].labels) != 0 {
		t.Fatalf("empty projection identity = %v, want none", empty)
	}
}
