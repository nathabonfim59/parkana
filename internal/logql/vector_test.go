package logql

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

func parseVectorAgg(t *testing.T, q string) *VectorAgg {
	t.Helper()
	expr, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	a, ok := expr.(*VectorAgg)
	if !ok {
		t.Fatalf("Parse(%q) = %T, want *VectorAgg", q, expr)
	}
	return a
}

func vectorInputOf(pairs []string, values map[int]float64) vectorInput {
	return vectorInput{labels: lab(pairs...), values: values}
}

// vectorSteps builds a one-step or two-step time grid for pure aggregation
// tests.
func vectorSteps(n int) []time.Time {
	times := make([]time.Time, n)
	for i := range times {
		times[i] = rangeBase.Add(time.Duration(i) * time.Minute)
	}
	return times
}

// vectorAggRows is the shared dataset for the Engine.Query aggregation tests:
// three leaf series whose count_over_time values at vectorBase are 1, 2 and 1.
func vectorAggRows() []plan.Row {
	at := rangeBase.Add(10 * time.Second)
	return []plan.Row{
		metricRowAt(at, "a", map[string]string{"app": "api", "level": "info", "pod": "a"}, nil),
		metricRowAt(at, "b", map[string]string{"app": "api", "level": "info", "pod": "b"}, nil),
		metricRowAt(at.Add(10*time.Second), "c", map[string]string{"app": "api", "level": "info", "pod": "b"}, nil),
		metricRowAt(at.Add(20*time.Second), "d", map[string]string{"app": "api", "level": "error", "pod": "a"}, nil),
	}
}

const vectorAggAt = 50 * time.Second

func TestVectorGrouping(t *testing.T) {
	ls := lab("__name__", "requests", "app", "api", "level", "info", "pod", "a")
	cases := []struct {
		name string
		g    *Grouping
		want logs.Labels
	}{
		{"no grouping is a singleton", nil, nil},
		{"empty by is a singleton", &Grouping{}, nil},
		{"by keeps only present names", &Grouping{Names: []string{"level"}}, lab("level", "info")},
		{"by missing names keeps nothing", &Grouping{Names: []string{"absent"}}, nil},
		{"by keeps the metric name when named", &Grouping{Names: []string{"__name__"}}, lab("__name__", "requests")},
		{"without removes names and the metric name", &Grouping{Without: true, Names: []string{"pod"}}, lab("app", "api", "level", "info")},
		{"empty without removes only the metric name", &Grouping{Without: true}, lab("app", "api", "level", "info", "pod", "a")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := vectorGrouping(ls, tc.g)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("grouping = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInjectVectorGrouping(t *testing.T) {
	child := func(q string) *RangeAgg { return parseRangeAgg(t, q) }
	cases := []struct {
		name   string
		parent string
		child  string
		group  []string
		ok     bool
	}{
		{
			name:   "sum pushes a by grouping into count",
			parent: `sum by (level) (count_over_time({app="api"}[1m]))`,
			child:  `count_over_time({app="api"}[1m])`,
			group:  []string{"level"},
			ok:     true,
		},
		{
			name:   "sum with no grouping pushes an empty keep-list",
			parent: `sum(count_over_time({app="api"}[1m]))`,
			child:  `count_over_time({app="api"}[1m])`,
			group:  []string{},
			ok:     true,
		},
		{
			name:   "sum pushes into rate and bytes too",
			parent: `sum by (level) (rate({app="api"}[1m]))`,
			child:  `rate({app="api"}[1m])`,
			group:  []string{"level"},
			ok:     true,
		},
		{
			name:   "sum does not push a without grouping",
			parent: `sum without (pod) (count_over_time({app="api"}[1m]))`,
			child:  `count_over_time({app="api"}[1m])`,
			ok:     false,
		},
		{
			name:   "non-sum parents never push",
			parent: `avg by (level) (count_over_time({app="api"}[1m]))`,
			child:  `count_over_time({app="api"}[1m])`,
			ok:     false,
		},
		{
			name:   "a handled range function is not injectable",
			parent: `sum by (level) (bytes_over_time({app="api"}[1m]))`,
			child:  `bytes_over_time({app="api"}[1m])`,
			group:  []string{"level"},
			ok:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := parseVectorAgg(t, tc.parent)
			got, ok := injectVectorGrouping(parent, child(tc.child))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if !slices.Equal(got, tc.group) {
				t.Fatalf("group = %v, want %v", got, tc.group)
			}
		})
	}
}

func TestVectorAggregations(t *testing.T) {
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	cases := []struct {
		name string
		q    string
		want map[string][]plan.Point
	}{
		{
			name: "sum by",
			q:    `sum by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 3}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "sum without keeps the unlisted labels",
			q:    `sum without (pod) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "info").String():  {{T: at, V: 3}},
				lab("app", "api", "level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "sum without empty keeps every identity",
			q:    `sum without () (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "info", "pod", "a").String():  {{T: at, V: 1}},
				lab("app", "api", "level", "info", "pod", "b").String():  {{T: at, V: 2}},
				lab("app", "api", "level", "error", "pod", "a").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "sum by over rate",
			q:    `sum by (level) (rate({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 3.0 / 60}},
				lab("level", "error").String(): {{T: at, V: 1.0 / 60}},
			},
		},
		{
			name: "sum by over bytes",
			q:    `sum by (level) (bytes_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 3}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "sum with no grouping is a singleton",
			q:    `sum(count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab().String(): {{T: at, V: 4}},
			},
		},
		{
			name: "avg by",
			q:    `avg by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 1.5}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "min by",
			q:    `min by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 1}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "max by",
			q:    `max by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 2}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "count counts series not rows",
			q:    `count by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 2}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "global count is the series count",
			q:    `count(count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab().String(): {{T: at, V: 3}},
			},
		},
		{
			name: "stdvar is the population variance",
			q:    `stdvar by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 0.25}},
				lab("level", "error").String(): {{T: at, V: 0}},
			},
		},
		{
			name: "stddev is the population standard deviation",
			q:    `stddev by (level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 0.5}},
				lab("level", "error").String(): {{T: at, V: 0}},
			},
		},
		{
			name: "a missing grouped label stays absent",
			q:    `sum by (level, detected_level) (count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 3}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(vectorAggRows()...)
			res := mustQuery(t, e, src, instantParams(tc.q, at))
			if res.Type != plan.TypeVector {
				t.Fatalf("type = %q, want vector", res.Type)
			}
			assertResult(t, res, tc.want)
			// All four range functions keep the one-Aggregate fast path; the
			// sum projection must not force a row scan.
			if src.aggregateCalls != 1 || src.selectCalls != 0 {
				t.Fatalf("calls = %d aggregate / %d select, want one aggregate", src.aggregateCalls, src.selectCalls)
			}
			if !src.aggs[0].IncludeMeta {
				t.Fatalf("metadata-aware Aggregate expected (IncludeMeta)")
			}
		})
	}
}

func TestVectorAggregateMetadataIdentity(t *testing.T) {
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace_id": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace_id": "1"}),
		metricRowAt(rangeBase.Add(30*time.Second), "c", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace_id": "2"}),
	}
	p := instantParams(`avg by (level) (count_over_time({app="api"}[1m]))`, at)

	// The two metadata-distinct children stay apart through the child
	// aggregation; avg sees values 2 and 1. A parent that merged them first
	// would return 3.
	src := newMetricSource(rows...)
	res := mustQuery(t, e, src, p)
	assertResult(t, res, map[string][]plan.Point{
		lab("level", "info").String(): {{T: at, V: 1.5}},
	})
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
	}

	// A sum by the same grouping collapses the metadata children by summing.
	sum := mustQuery(t, e, newMetricSource(rows...), instantParams(`sum by (level) (count_over_time({app="api"}[1m]))`, at))
	assertResult(t, sum, map[string][]plan.Point{
		lab("level", "info").String(): {{T: at, V: 3}},
	})
}

func TestVectorAggregateSparseSteps(t *testing.T) {
	e := testEngine()
	start := rangeBase.Add(30 * time.Second)
	end := rangeBase.Add(90 * time.Second)
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(20*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(60*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
	}
	src := newMetricSource(rows...)
	p := rangeParams(`sum by (pod) (count_over_time({app="api"}[1m]))`, start, end, time.Minute)
	res := mustQuery(t, e, src, p)
	if res.Type != plan.TypeMatrix {
		t.Fatalf("type = %q, want matrix", res.Type)
	}
	// The frontend aligns 03:04:30..03:05:30 to 03:04..03:06 on the minute
	// grid. No zero sample is invented for the step where a group has no rows.
	assertResult(t, res, map[string][]plan.Point{
		lab("pod", "a").String(): {{T: rangeBase.Add(time.Minute), V: 1}},
		lab("pod", "b").String(): {{T: rangeBase.Add(time.Minute), V: 1}},
	})
}

func TestVectorNested(t *testing.T) {
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	cases := []struct {
		name string
		q    string
		want map[string][]plan.Point
	}{
		{
			name: "nested sums regroup",
			q:    `sum by (level) (sum by (level, pod) (count_over_time({app="api"}[1m])))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 3}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "max over avg",
			q:    `max by (level) (avg by (level, pod) (count_over_time({app="api"}[1m])))`,
			want: map[string][]plan.Point{
				lab("level", "info").String():  {{T: at, V: 2}},
				lab("level", "error").String(): {{T: at, V: 1}},
			},
		},
		{
			name: "count over count",
			q:    `count(count(count_over_time({app="api"}[1m])))`,
			want: map[string][]plan.Point{
				lab().String(): {{T: at, V: 1}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(vectorAggRows()...)
			res := mustQuery(t, e, src, instantParams(tc.q, at))
			assertResult(t, res, tc.want)
			if src.aggregateCalls != 1 {
				t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
			}
		})
	}
}

func TestVectorTopKBottomK(t *testing.T) {
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "b1", map[string]string{"app": "api", "level": "info", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "b2", map[string]string{"app": "api", "level": "info", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "c1", map[string]string{"app": "api", "level": "error", "pod": "c"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "c2", map[string]string{"app": "api", "level": "error", "pod": "c"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "c3", map[string]string{"app": "api", "level": "error", "pod": "c"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "d1", map[string]string{"app": "api", "level": "error", "pod": "d"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "d2", map[string]string{"app": "api", "level": "error", "pod": "d"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "d3", map[string]string{"app": "api", "level": "error", "pod": "d"}, nil),
		metricRowAt(rangeBase.Add(40*time.Second), "d4", map[string]string{"app": "api", "level": "error", "pod": "d"}, nil),
	}
	cases := []struct {
		name string
		q    string
		want map[string][]plan.Point
	}{
		{
			name: "topk keeps the full selected identities",
			q:    `topk(2, count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "error", "pod", "c").String(): {{T: at, V: 3}},
				lab("app", "api", "level", "error", "pod", "d").String(): {{T: at, V: 4}},
			},
		},
		{
			name: "bottomk",
			q:    `bottomk(2, count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "info", "pod", "a").String(): {{T: at, V: 1}},
				lab("app", "api", "level", "info", "pod", "b").String(): {{T: at, V: 2}},
			},
		},
		{
			name: "topk partitions by grouping and keeps full labels",
			q:    `topk(1, count_over_time({app="api"}[1m])) by (level)`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "info", "pod", "b").String():  {{T: at, V: 2}},
				lab("app", "api", "level", "error", "pod", "d").String(): {{T: at, V: 4}},
			},
		},
		{
			name: "huge k keeps every input without allocating by k",
			q:    `topk(9223372036854775807, count_over_time({app="api"}[1m]))`,
			want: map[string][]plan.Point{
				lab("app", "api", "level", "info", "pod", "a").String():  {{T: at, V: 1}},
				lab("app", "api", "level", "info", "pod", "b").String():  {{T: at, V: 2}},
				lab("app", "api", "level", "error", "pod", "c").String(): {{T: at, V: 3}},
				lab("app", "api", "level", "error", "pod", "d").String(): {{T: at, V: 4}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(rows...)
			res := mustQuery(t, e, src, instantParams(tc.q, at))
			if res.Type != plan.TypeVector {
				t.Fatalf("type = %q, want vector", res.Type)
			}
			assertResult(t, res, tc.want)
			if src.aggregateCalls != 1 {
				t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
			}
		})
	}
}

func TestVectorTopKTies(t *testing.T) {
	// Both series have the same value; Loki keeps whichever the heap saw first,
	// so only membership and the count are guaranteed.
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
	)
	res := mustQuery(t, testEngine(), src, instantParams(`topk(1, count_over_time({app="api"}[1m]))`, rangeBase.Add(30*time.Second)))
	if len(res.Vector) != 1 {
		t.Fatalf("vector = %v, want exactly one tie winner", res.Vector)
	}
	got := res.Vector[0].Metric.String()
	if got != lab("app", "api", "pod", "a").String() && got != lab("app", "api", "pod", "b").String() {
		t.Fatalf("tie winner = %s, want one of the two input series", got)
	}
}

func TestVectorTopKParameterRejection(t *testing.T) {
	for _, q := range []string{
		`topk(0, count_over_time({app="api"}[1m]))`,
		`bottomk(0, count_over_time({app="api"}[1m]))`,
		`topk(count_over_time({app="api"}[1m]))`,
	} {
		t.Run(q, func(t *testing.T) {
			src := newMetricSource(metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil))
			_, err := testEngine().Query(t.Context(), src, instantParams(q, rangeBase.Add(time.Minute)))
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("err = %v, want ErrBadQuery", err)
			}
			if src.aggregateCalls != 0 || src.selectCalls != 0 {
				t.Fatalf("parse rejection touched the source: %d/%d", src.aggregateCalls, src.selectCalls)
			}
		})
	}
}

func TestVectorSortOrder(t *testing.T) {
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "b1", map[string]string{"app": "api", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "b2", map[string]string{"app": "api", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(40*time.Second), "b3", map[string]string{"app": "api", "pod": "b"}, nil),
	}
	for _, tc := range []struct {
		name string
		q    string
		want []float64
		lab  []string
	}{
		{
			name: "sort is ascending by value",
			q:    `sort(count_over_time({app="api"}[1m]))`,
			want: []float64{1, 3},
			lab:  []string{"a", "b"},
		},
		{
			name: "sort_desc is descending by value",
			q:    `sort_desc(count_over_time({app="api"}[1m]))`,
			want: []float64{3, 1},
			lab:  []string{"b", "a"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := mustQuery(t, e, newMetricSource(rows...), instantParams(tc.q, at))
			if res.Type != plan.TypeVector {
				t.Fatalf("type = %q, want vector", res.Type)
			}
			gotValues := make([]float64, len(res.Vector))
			gotPods := make([]string, len(res.Vector))
			for i, s := range res.Vector {
				gotValues[i] = s.Point.V
				gotPods[i], _ = s.Metric.Get("pod")
			}
			if !slices.Equal(gotValues, tc.want) {
				t.Fatalf("values = %v, want %v", gotValues, tc.want)
			}
			if !slices.Equal(gotPods, tc.lab) {
				t.Fatalf("pods = %v, want %v", gotPods, tc.lab)
			}
		})
	}

	// A range sort keeps the label-sorted matrix shape.
	start := rangeBase.Add(30 * time.Second)
	end := rangeBase.Add(90 * time.Second)
	// Aligned to the minute grid, all rows fall in the 03:05 window.
	res := mustQuery(t, e, newMetricSource(rows...), rangeParams(`sort(count_over_time({app="api"}[1m]))`, start, end, time.Minute))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "pod", "a").String(): {{T: rangeBase.Add(time.Minute), V: 1}},
		lab("app", "api", "pod", "b").String(): {{T: rangeBase.Add(time.Minute), V: 3}},
	})
}

func TestVectorMaxSeriesAcrossRange(t *testing.T) {
	// One series per step: every step stays under the limit, but the assembled
	// response has three, which must still fail.
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(-10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(50*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(110*time.Second), "c", map[string]string{"app": "api", "pod": "c"}, nil),
	}
	p := rangeParams(`sum by (pod) (count_over_time({app="api"}[1m]))`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute)
	strict := New(config.LogQL{MaxEntries: 100, MaxSeries: 2})
	if _, err := strict.Query(t.Context(), newMetricSource(rows...), p); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("err = %v, want ErrLimit", err)
	}
	// The recursive path must not reject the intermediate result early: with a
	// limit of three the same query succeeds.
	fits := New(config.LogQL{MaxEntries: 100, MaxSeries: 3})
	res := mustQuery(t, fits, newMetricSource(rows...), p)
	if got := len(resultPoints(t, res)); got != 3 {
		t.Fatalf("series = %d, want 3", got)
	}
}

func TestVectorInstantAndMatrixShapes(t *testing.T) {
	e := testEngine()
	start := rangeBase.Add(30 * time.Second)
	end := rangeBase.Add(90 * time.Second)
	q := `sum by (level) (count_over_time({app="api"}[1m]))`
	instant := mustQuery(t, e, newMetricSource(vectorAggRows()...), instantParams(q, start))
	if instant.Type != plan.TypeVector {
		t.Fatalf("instant type = %q, want vector", instant.Type)
	}
	matrix := mustQuery(t, e, newMetricSource(vectorAggRows()...), rangeParams(q, start, end, time.Minute))
	if matrix.Type != plan.TypeMatrix {
		t.Fatalf("range type = %q, want matrix", matrix.Type)
	}
	if len(matrix.Matrix) == 0 {
		t.Fatalf("matrix is empty")
	}
}

func TestVectorCancellationAndSourceError(t *testing.T) {
	e := testEngine()
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
	}
	p := instantParams(`sum by (pod) (count_over_time({app="api"}[1m]))`, rangeBase.Add(30*time.Second))

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	src := newMetricSource(rows...)
	if _, err := e.Query(canceled, src, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("canceled request opened the source: %d/%d", src.aggregateCalls, src.selectCalls)
	}

	boom := errors.New("boom")
	bad := newMetricSource(rows...)
	bad.err = boom
	bad.aggErrAfter = 1
	if _, err := e.Query(t.Context(), bad, p); !errors.Is(err, boom) {
		t.Fatalf("source error = %v, want boom", err)
	}
}

func TestVectorAggregateContextCheck(t *testing.T) {
	// The per-step loop refuses to build a result from a canceled context even
	// when the child series were already materialized.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	a := &VectorAgg{Func: Sum}
	inputs := []vectorInput{vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1})}
	if _, err := vectorApply(canceled, a, inputs, vectorSteps(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestVectorAliasedInputs(t *testing.T) {
	// A Source may reuse its label backing arrays between yields; the vector
	// result must own its labels and values.
	e := testEngine()
	at := rangeBase.Add(vectorAggAt)
	src := newMetricSource(vectorAggRows()...)
	src.mutate = true
	res := mustQuery(t, e, src, instantParams(`sum by (level) (count_over_time({app="api"}[1m]))`, at))
	assertResult(t, res, map[string][]plan.Point{
		lab("level", "info").String():  {{T: at, V: 3}},
		lab("level", "error").String(): {{T: at, V: 1}},
	})
}

func TestVectorUnsupportedChildren(t *testing.T) {
	cases := []string{
		`sum(1)`,
		`sum(sum(1))`,
		`sum(label_replace(count_over_time({app="api"}[1m]), "dst", "repl", "src", ".*"))`,
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			src := newMetricSource(metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil))
			_, err := testEngine().Query(t.Context(), src, instantParams(q, rangeBase.Add(time.Minute)))
			if !errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
			if src.aggregateCalls != 0 || src.selectCalls != 0 {
				t.Fatalf("unsupported expression touched the source: %d/%d", src.aggregateCalls, src.selectCalls)
			}
		})
	}
}

// TestVectorPopulationVarianceMath pins the arithmetic against Loki's
// VectorAggEvaluator: incremental mean/Welford for avg/stddev/stdvar, direct
// sum, NaN max/min rules, and population (not sample) variance.
func TestVectorPopulationVarianceMath(t *testing.T) {
	one := vectorSteps(1)
	cases := []struct {
		name string
		fn   VectorFunc
		in   []float64
		want float64
		nan  bool
	}{
		{"sum", Sum, []float64{1, 2, 3}, 6, false},
		{"sum overflow to +Inf", Sum, []float64{math.MaxFloat64, math.MaxFloat64}, math.Inf(1), false},
		{"sum Inf minus Inf is NaN", Sum, []float64{math.Inf(1), math.Inf(-1)}, 0, true},
		{"avg", Avg, []float64{1, 2}, 1.5, false},
		{"avg NaN", Avg, []float64{math.NaN(), 5}, 0, true},
		{"avg MaxFloat64 stays finite", Avg, []float64{math.MaxFloat64, math.MaxFloat64}, math.MaxFloat64, false},
		{"avg opposite extremes overflow to -Inf", Avg, []float64{math.MaxFloat64, -math.MaxFloat64}, math.Inf(-1), false},
		{"min skips NaN", Min, []float64{5, math.NaN(), 3}, 3, false},
		{"min replaces a NaN first value", Min, []float64{math.NaN(), 5}, 5, false},
		{"max skips NaN", Max, []float64{5, math.NaN()}, 5, false},
		{"max replaces a NaN first value", Max, []float64{math.NaN(), 5}, 5, false},
		{"max Inf", Max, []float64{1, math.Inf(1)}, math.Inf(1), false},
		{"count", Count, []float64{1, 2, 3, 4}, 4, false},
		{"stdvar population", Stdvar, []float64{1, 2, 3}, 2.0 / 3.0, false},
		{"stddev population", Stddev, []float64{1, 2, 3}, math.Sqrt(2.0 / 3.0), false},
		{"stdvar of one sample is zero", Stdvar, []float64{math.Inf(1)}, 0, false},
		{"stdvar of equal Inf values is NaN", Stdvar, []float64{math.Inf(1), math.Inf(1)}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := make([]vectorInput, len(tc.in))
			for i, v := range tc.in {
				inputs[i] = vectorInputOf([]string{"k", "v"}, map[int]float64{0: v})
			}
			a := &VectorAgg{Func: tc.fn}
			series, err := vectorApply(t.Context(), a, inputs, one)
			if err != nil {
				t.Fatalf("vectorApply: %v", err)
			}
			got := resultPoints1(t, series)
			if tc.nan {
				if !math.IsNaN(got) {
					t.Fatalf("value = %v, want NaN", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("value = %v, want %v", got, tc.want)
			}
		})
	}
}

func resultPoints1(t *testing.T, series []*metricSeries) float64 {
	t.Helper()
	if len(series) != 1 {
		t.Fatalf("series = %v, want one", series)
	}
	if len(series[0].points) != 1 {
		t.Fatalf("points = %v, want one", series[0].points)
	}
	return series[0].points[0].V
}

// TestVectorNaNOrdering pins the heap behaviour from Loki's vector.go: NaNs sit
// at the heap root and therefore can be evicted, and sort output puts NaNs last.
func TestVectorNaNOrdering(t *testing.T) {
	steps := vectorSteps(1)
	inputs := []vectorInput{
		vectorInputOf([]string{"pod", "nan"}, map[int]float64{0: math.NaN()}),
		vectorInputOf([]string{"pod", "five"}, map[int]float64{0: 5}),
		vectorInputOf([]string{"pod", "one"}, map[int]float64{0: 1}),
	}

	top := mustVectorValues(t, &VectorAgg{Func: TopK, Params: []float64{2}}, inputs, steps)
	if want := []float64{5, 1}; !slices.Equal(top, want) {
		t.Fatalf("topk(2) = %v, want %v (the NaN is evicted)", top, want)
	}
	bottom := mustVectorValues(t, &VectorAgg{Func: BottomK, Params: []float64{2}}, inputs, steps)
	if want := []float64{1, 5}; !slices.Equal(bottom, want) {
		t.Fatalf("bottomk(2) = %v, want %v", bottom, want)
	}
	all := mustVectorValues(t, &VectorAgg{Func: TopK, Params: []float64{3}}, inputs, steps)
	if len(all) != 3 || !math.IsNaN(all[2]) {
		t.Fatalf("topk(3) = %v, want all inputs with NaN last", all)
	}

	sorted := mustVectorValues(t, &VectorAgg{Func: Sort}, inputs, steps)
	if len(sorted) != 3 || sorted[0] != 1 || sorted[1] != 5 || !math.IsNaN(sorted[2]) {
		t.Fatalf("sort = %v, want [1 5 NaN]", sorted)
	}
	sortedDesc := mustVectorValues(t, &VectorAgg{Func: SortDesc}, inputs, steps)
	if len(sortedDesc) != 3 || sortedDesc[0] != 5 || sortedDesc[1] != 1 || !math.IsNaN(sortedDesc[2]) {
		t.Fatalf("sort_desc = %v, want [5 1 NaN]", sortedDesc)
	}

	if got := topKSize(9223372036854775807, 3); got != 3 {
		t.Fatalf("topKSize(huge, 3) = %d, want 3", got)
	}
	if got := topKSize(math.NaN(), 3); got != 0 {
		t.Fatalf("topKSize(NaN, 3) = %d, want 0", got)
	}
}

// mustVectorValues returns the output vector values in series order for a
// pure one-step evaluation.
func mustVectorValues(t *testing.T, a *VectorAgg, inputs []vectorInput, times []time.Time) []float64 {
	t.Helper()
	series, err := vectorApply(t.Context(), a, inputs, times)
	if err != nil {
		t.Fatalf("vectorApply: %v", err)
	}
	var out []float64
	for _, s := range series {
		for _, p := range s.points {
			out = append(out, p.V)
		}
	}
	return out
}

// cancelAfterContext returns nil from Err a fixed number of times and then
// reports cancellation, so a test can stop a loop after its first check.
type cancelAfterContext struct {
	context.Context
	remaining int
	canceled  bool
}

func (c *cancelAfterContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	if c.remaining <= 0 {
		c.canceled = true
		return context.Canceled
	}
	c.remaining--
	return nil
}

func TestVectorStepAndGroupContextChecks(t *testing.T) {
	steps := vectorSteps(1)
	inputs := []vectorInput{vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1})}
	for _, a := range []*VectorAgg{
		{Func: Sum},
		{Func: TopK, Params: []float64{1}},
		{Func: Sort},
	} {
		// The first Err check is the step loop's; the second is the group loop's.
		ctx := &cancelAfterContext{Context: t.Context(), remaining: 1}
		if _, err := vectorApply(ctx, a, inputs, steps); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s err = %v, want context.Canceled", a.Func, err)
		}
	}
}

func TestVectorSeriesGuards(t *testing.T) {
	p := instantParams(`sum(count_over_time({app="api"}[1m]))`, rangeBase.Add(time.Minute))
	if _, err := testEngine().evalVectorSeries(t.Context(), nil, &VectorAgg{Func: Sum}, p); err == nil {
		t.Fatalf("nil source accepted")
	}

	src := newMetricSource(metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil))
	if _, err := testEngine().evalVectorSeries(t.Context(), src, &VectorAgg{Func: TopK}, p); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("missing topk parameter err = %v, want ErrBadQuery", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("invalid parameter touched the source")
	}

	if _, err := vectorApply(t.Context(), &VectorAgg{Func: VectorFunc(99)}, nil, nil); !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("unknown function err = %v, want ErrUnsupported", err)
	}
	if _, err := vectorApply(t.Context(), &VectorAgg{Func: TopK}, nil, nil); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("vectorApply topk without params err = %v, want ErrBadQuery", err)
	}
	if res, err := vectorApply(t.Context(), &VectorAgg{Func: TopK, Params: []float64{0}},
		[]vectorInput{vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1})}, vectorSteps(1)); err != nil || len(res) != 0 {
		t.Fatalf("k=0 = %v, %v; want empty result", res, err)
	}
	if res, err := vectorApply(t.Context(), &VectorAgg{Func: BottomK, Params: []float64{math.NaN()}},
		[]vectorInput{vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1})}, vectorSteps(1)); err != nil || len(res) != 0 {
		t.Fatalf("NaN k = %v, %v; want empty result", res, err)
	}

	// The recursive helper only takes metric children; a hand-built log range
	// is rejected without touching the source.
	bad := &VectorAgg{Func: Sum, Left: &LogRange{}}
	if _, err := testEngine().evalVectorSeries(t.Context(), src, bad, p); !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("log-range child err = %v, want ErrUnsupported", err)
	}

	// A child point off the evaluation grid is an engine bug, not a new series.
	series := []*metricSeries{{
		labels: lab("k", "v"),
		points: []plan.Point{{T: rangeBase.Add(time.Second), V: 1}},
	}}
	if _, err := indexVectorPoints([]time.Time{rangeBase}, series); err == nil {
		t.Fatalf("off-grid sample accepted")
	}
	// A point-less child contributes nothing.
	if got, err := indexVectorPoints([]time.Time{rangeBase}, []*metricSeries{{labels: lab("k", "v")}}); err != nil || len(got) != 0 {
		t.Fatalf("point-less series = %v, %v; want no input", got, err)
	}

	// A negative step is rejected before the source is opened.
	badRange := p
	badRange.Step = -time.Second
	if _, err := testEngine().evalVectorSeries(t.Context(), src, &VectorAgg{Func: Sum}, badRange); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("negative step err = %v, want ErrBadQuery", err)
	}
}

func TestVectorHeapLess(t *testing.T) {
	nan := math.NaN()
	asc := &vectorValueHeap{samples: []vectorSample{{value: nan}, {value: 1}, {value: 2}}}
	if !asc.Less(0, 1) {
		t.Fatalf("NaN must compare smallest in the ascending heap")
	}
	if !asc.Less(1, 2) || asc.Less(2, 1) {
		t.Fatalf("ascending heap order is wrong")
	}
	desc := &vectorValueHeap{descending: true, samples: []vectorSample{{value: 2}, {value: 1}}}
	if !desc.Less(0, 1) || desc.Less(1, 0) {
		t.Fatalf("descending heap order is wrong")
	}
}

func TestCompareVectorValues(t *testing.T) {
	nan := math.NaN()
	cases := []struct {
		x, y       float64
		descending bool
		want       int
	}{
		{1, 2, false, -1},
		{2, 1, false, 1},
		{1, 2, true, 1},
		{2, 1, true, -1},
		{nan, nan, false, 0},
		{nan, 1, false, 1},
		{1, nan, false, -1},
		{nan, 1, true, 1},
		{1, nan, true, -1},
	}
	for _, tc := range cases {
		if got := compareVectorValues(tc.x, tc.y, tc.descending); got != tc.want {
			t.Fatalf("compareVectorValues(%v, %v, %v) = %d, want %d", tc.x, tc.y, tc.descending, got, tc.want)
		}
	}
}

// evalVectorSelectReference computes the same vector aggregation from the
// exact row-by-row Select path, so tests can prove the one-Aggregate fast path
// agrees with evaluating the shared pipeline over every row.
func evalVectorSelectReference(t *testing.T, e *Engine, src plan.Source, a *VectorAgg, p plan.Params) plan.Result {
	t.Helper()
	child, ok := a.Left.(*RangeAgg)
	if !ok {
		t.Fatalf("reference helper needs a direct range child, got %T", a.Left)
	}
	times, err := metricEvalTimes(p)
	if err != nil {
		t.Fatalf("metricEvalTimes: %v", err)
	}
	series, err := e.evalRangeSeries(t.Context(), src, child, p, rangeOptions{aggregate: false, child: true})
	if err != nil {
		t.Fatalf("evalRangeSeries(select): %v", err)
	}
	inputs, err := indexVectorPoints(times, series)
	if err != nil {
		t.Fatalf("indexVectorPoints: %v", err)
	}
	out, err := vectorApply(t.Context(), a, inputs, times)
	if err != nil {
		t.Fatalf("vectorApply: %v", err)
	}
	res, err := e.finishMetric(out, p)
	if err != nil {
		t.Fatalf("finishMetric: %v", err)
	}
	return res
}

func TestVectorGrafanaHistogram(t *testing.T) {
	// The captured Grafana 13.3.0 / Loki datasource request for logs volume is
	//   sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))
	// with a 1m step. It must use exactly one Aggregate call (the drop/keep
	// projection keeps the fast path), group metadata-origin detected_level,
	// keep a missing detected_level absent, and agree with the Select path.
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"service_name": "api", "level": "info"},
			map[string]string{"trace_id": "1", "detected_level": "error"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"service_name": "api", "level": "info"}, nil),
		metricRowAt(rangeBase.Add(25*time.Second), "c", map[string]string{"service_name": "api", "level": "warn", "detected_level": "info"}, nil),
	}
	q := `sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))`
	p := rangeParams(q, rangeBase, rangeBase.Add(time.Minute), time.Minute)

	e := testEngine()
	fast := newMetricSource(rows...)
	res := mustQuery(t, e, fast, p)
	if fast.aggregateCalls != 1 || fast.selectCalls != 0 {
		t.Fatalf("calls = %d aggregate / %d select, want exactly one Aggregate", fast.aggregateCalls, fast.selectCalls)
	}
	if !fast.aggs[0].IncludeMeta {
		t.Fatalf("metadata-aware Aggregate expected (IncludeMeta)")
	}
	// The aligned 03:04..03:05 range has one non-empty step: every row falls
	// in the 03:05 window.
	want := map[string][]plan.Point{
		lab("level", "info", "detected_level", "error").String(): {{T: rangeBase.Add(time.Minute), V: 1}},
		lab("level", "info").String():                            {{T: rangeBase.Add(time.Minute), V: 1}},
		lab("level", "warn", "detected_level", "info").String():  {{T: rangeBase.Add(time.Minute), V: 1}},
	}
	assertResult(t, res, want)

	refSrc := newMetricSource(rows...)
	ref := evalVectorSelectReference(t, e, refSrc, parseVectorAgg(t, q), p)
	if refSrc.selectCalls != 1 || refSrc.aggregateCalls != 0 {
		t.Fatalf("reference calls = %d select / %d aggregate, want one Select", refSrc.selectCalls, refSrc.aggregateCalls)
	}
	assertSameResult(t, res, ref)
}
