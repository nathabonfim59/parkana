package logql

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// binaryTestRows is the shared two-row dataset: one stream whose
// count_over_time over [1m] at rangeBase+30s is 2.
func binaryTestRows() []plan.Row {
	at := rangeBase.Add(10 * time.Second)
	return []plan.Row{
		metricRowAt(at, "a", map[string]string{"app": "api", "level": "info"}, nil),
		metricRowAt(at.Add(10*time.Second), "b", map[string]string{"app": "api", "level": "info"}, nil),
	}
}

const binaryAt = 30 * time.Second

func binaryInstant(q string) plan.Params { return instantParams(q, rangeBase.Add(binaryAt)) }

// assertScalar checks an instant scalar result's value and timestamp.
func assertScalar(t *testing.T, res plan.Result, at time.Time, want float64, nan bool) {
	t.Helper()
	if res.Type != plan.TypeScalar {
		t.Fatalf("type = %q, want scalar", res.Type)
	}
	if !res.Scalar.T.Equal(at) {
		t.Fatalf("scalar T = %s, want %s", res.Scalar.T, at)
	}
	if nan {
		if !math.IsNaN(res.Scalar.V) {
			t.Fatalf("scalar V = %v, want NaN", res.Scalar.V)
		}
		return
	}
	if res.Scalar.V != want {
		t.Fatalf("scalar V = %v, want %v", res.Scalar.V, want)
	}
}

// assertOneVectorValue checks a one-series vector result's value and keeps its
// label set untouched.
func assertOneVectorValue(t *testing.T, res plan.Result, at time.Time, key string, want float64, nan bool) {
	t.Helper()
	if res.Type != plan.TypeVector {
		t.Fatalf("type = %q, want vector", res.Type)
	}
	if len(res.Vector) != 1 {
		t.Fatalf("vector = %v, want exactly one sample", res.Vector)
	}
	sample := res.Vector[0]
	if got := sample.Metric.String(); got != key {
		t.Fatalf("labels = %s, want %s", got, key)
	}
	if !sample.Point.T.Equal(at) {
		t.Fatalf("T = %s, want %s", sample.Point.T, at)
	}
	if nan {
		if !math.IsNaN(sample.Point.V) {
			t.Fatalf("V = %v, want NaN", sample.Point.V)
		}
		return
	}
	if sample.Point.V != want {
		t.Fatalf("V = %v, want %v", sample.Point.V, want)
	}
}

// TestHealthVectorArithmetic is Grafana's datasource health query. It must
// parse, round trip, and answer from the evaluation grid alone: one
// empty-label series, value 2, an instant vector and a range matrix, with no
// Source call.
func TestHealthVectorArithmetic(t *testing.T) {
	const health = `vector(1)+vector(1)`
	expr := parseAndCheck(t, health, `vector(1) + vector(1)`)
	if _, ok := expr.(*BinaryExpr); !ok {
		t.Fatalf("Parse(%q) = %T, want *BinaryExpr", health, expr)
	}

	src := newMetricSource()
	res := mustQuery(t, testEngine(), src, instantParams(health, rangeBase))
	if res.Type != plan.TypeVector {
		t.Fatalf("instant type = %q, want vector", res.Type)
	}
	if len(res.Vector) != 1 {
		t.Fatalf("instant vector = %v, want one sample", res.Vector)
	}
	sample := res.Vector[0]
	if len(sample.Metric) != 0 {
		t.Fatalf("labels = %s, want no labels", sample.Metric)
	}
	if !sample.Point.T.Equal(rangeBase) || sample.Point.V != 2 {
		t.Fatalf("sample = %+v, want value 2 at %s", sample.Point, rangeBase)
	}

	start := rangeBase
	end := start.Add(time.Minute)
	res = mustQuery(t, testEngine(), src, rangeParams(health, start, end, time.Minute))
	if res.Type != plan.TypeMatrix {
		t.Fatalf("range type = %q, want matrix", res.Type)
	}
	if len(res.Matrix) != 1 || len(res.Matrix[0].Metric) != 0 {
		t.Fatalf("range matrix = %+v, want one unlabelled series", res.Matrix)
	}
	want := []plan.Point{{T: start, V: 2}, {T: end, V: 2}}
	if !pointsEqual(res.Matrix[0].Points, want) {
		t.Fatalf("range points = %v, want %v", res.Matrix[0].Points, want)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("health query touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}
}

func TestVectorLiteralShapes(t *testing.T) {
	src := newMetricSource()
	res := mustQuery(t, testEngine(), src, binaryInstant(`vector(1.5)`))
	assertOneVectorValue(t, res, rangeBase.Add(binaryAt), `{}`, 1.5, false)

	// A vector literal needs no Source at all, so a nil Source is valid.
	res, err := testEngine().Query(t.Context(), nil, binaryInstant(`vector(1)`))
	if err != nil {
		t.Fatalf("vector(1) with nil source: %v", err)
	}
	assertOneVectorValue(t, res, rangeBase.Add(binaryAt), `{}`, 1, false)

	start := rangeBase
	end := start.Add(time.Minute)
	res = mustQuery(t, testEngine(), src, rangeParams(`vector(2)`, start, end, time.Minute))
	if res.Type != plan.TypeMatrix || len(res.Matrix) != 1 {
		t.Fatalf("range vector(2) = %+v, want one-series matrix", res)
	}
	want := []plan.Point{{T: start, V: 2}, {T: end, V: 2}}
	if !pointsEqual(res.Matrix[0].Points, want) {
		t.Fatalf("points = %v, want %v", res.Matrix[0].Points, want)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("vector literal touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}
}

func TestScalarExpressions(t *testing.T) {
	at := rangeBase.Add(binaryAt)
	cases := []struct {
		q    string
		want float64
		nan  bool
	}{
		{`1`, 1, false},
		{`1 + 2 * 3`, 7, false},
		{`(1 + 2) * 3`, 9, false},
		{`2 ^ 3 ^ 2`, 512, false},
		{`-2 ^ 2`, 4, false}, // the sign belongs to the literal (syntax.y:464-468)
		{`2 ^ -3`, 0.125, false},
		{`10 % 3`, 1, false},
		{`10 / 4`, 2.5, false},
		{`1 / 0`, 0, true}, // Loki's MergeBinOp maps zero divisors to NaN
		{`10 % 0`, 0, true},
		{`1 < 2`, 1, false},
		{`1 == 2`, 0, false},
		{`1 + bool 2`, 3, false}, // bool only changes comparisons
	}
	src := newMetricSource()
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), src, instantParams(tc.q, at))
			assertScalar(t, res, at, tc.want, tc.nan)
		})
	}

	// Non-finite results are IEEE arithmetic: only division and modulo by
	// zero are special-cased to NaN.
	inf := mustQuery(t, testEngine(), src, instantParams(`1e308 * 10`, at))
	if inf.Type != plan.TypeScalar || !math.IsInf(inf.Scalar.V, 1) {
		t.Fatalf("1e308*10 = %+v, want +Inf scalar", inf)
	}

	// A range scalar query is one label-less matrix series on the step grid,
	// Loki's PopulateMatrixFromScalar.
	start := rangeBase
	end := start.Add(time.Minute)
	res := mustQuery(t, testEngine(), src, rangeParams(`1 + 1`, start, end, time.Minute))
	if res.Type != plan.TypeMatrix || len(res.Matrix) != 1 {
		t.Fatalf("range scalar = %+v, want one-series matrix", res)
	}
	if len(res.Matrix[0].Metric) != 0 {
		t.Fatalf("labels = %s, want none", res.Matrix[0].Metric)
	}
	want := []plan.Point{{T: start, V: 2}, {T: end, V: 2}}
	if !pointsEqual(res.Matrix[0].Points, want) {
		t.Fatalf("points = %v, want %v", res.Matrix[0].Points, want)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("scalar query touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}
}

func TestScalarVectorArithmeticBothOrders(t *testing.T) {
	key := lab("app", "api", "level", "info").String()
	at := rangeBase.Add(binaryAt)
	cases := []struct {
		q    string
		want float64
		nan  bool
	}{
		{`count_over_time({app="api"}[1m]) + 1`, 3, false},
		{`1 + count_over_time({app="api"}[1m])`, 3, false},
		{`count_over_time({app="api"}[1m]) - 1`, 1, false},
		{`10 - count_over_time({app="api"}[1m])`, 8, false},
		{`count_over_time({app="api"}[1m]) * 3`, 6, false},
		{`3 * count_over_time({app="api"}[1m])`, 6, false},
		{`count_over_time({app="api"}[1m]) / 2`, 1, false},
		{`4 / count_over_time({app="api"}[1m])`, 2, false},
		{`count_over_time({app="api"}[1m]) % 2`, 0, false},
		{`5 % count_over_time({app="api"}[1m])`, 1, false},
		{`count_over_time({app="api"}[1m]) ^ 2`, 4, false},
		{`2 ^ count_over_time({app="api"}[1m])`, 4, false},
		{`count_over_time({app="api"}[1m]) / 0`, 0, true},
		{`0 / count_over_time({app="api"}[1m])`, 0, false},
		{`count_over_time({app="api"}[1m]) % 0`, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			src := newMetricSource(binaryTestRows()...)
			res := mustQuery(t, testEngine(), src, binaryInstant(tc.q))
			assertOneVectorValue(t, res, at, key, tc.want, tc.nan)
		})
	}

	// A vector literal on either side has no labels to carry through.
	src := newMetricSource()
	for _, tc := range []struct {
		q    string
		want float64
		nan  bool
	}{
		{`vector(2) / 0`, 0, true},
		{`0 / vector(2)`, 0, false},
	} {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), src, binaryInstant(tc.q))
			assertOneVectorValue(t, res, at, `{}`, tc.want, tc.nan)
		})
	}
}

func TestScalarVectorComparisons(t *testing.T) {
	key := lab("app", "api", "level", "info").String()
	at := rangeBase.Add(binaryAt)
	cases := []struct {
		q    string
		want float64
		nan  bool
		none bool
	}{
		// A filtering comparison keeps the vector-side value, not 1.
		{`count_over_time({app="api"}[1m]) > 1`, 2, false, false},
		{`count_over_time({app="api"}[1m]) > 2`, 0, false, true},
		{`count_over_time({app="api"}[1m]) >= 2`, 2, false, false},
		{`1 < count_over_time({app="api"}[1m])`, 2, false, false},
		{`2 < count_over_time({app="api"}[1m])`, 0, false, true},
		{`count_over_time({app="api"}[1m]) == 2`, 2, false, false},
		{`count_over_time({app="api"}[1m]) != 2`, 0, false, true},
		// bool emits 0/1 for every matched sample.
		{`count_over_time({app="api"}[1m]) > bool 1`, 1, false, false},
		{`count_over_time({app="api"}[1m]) > bool 5`, 0, false, false},
		{`1 < bool count_over_time({app="api"}[1m])`, 1, false, false},
		{`count_over_time({app="api"}[1m]) < bool 1`, 0, false, false},
		// NaN comparisons are all false except !=, which keeps the
		// vector-side value.
		{`count_over_time({app="api"}[1m]) / 0 > 1`, 0, false, true},
		{`count_over_time({app="api"}[1m]) / 0 == count_over_time({app="api"}[1m]) / 0`, 0, false, true},
		{`count_over_time({app="api"}[1m]) / 0 != count_over_time({app="api"}[1m]) / 0`, 0, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			src := newMetricSource(binaryTestRows()...)
			res := mustQuery(t, testEngine(), src, binaryInstant(tc.q))
			if tc.none {
				if res.Type != plan.TypeVector || len(res.Vector) != 0 {
					t.Fatalf("result = %+v, want an empty vector", res)
				}
				return
			}
			assertOneVectorValue(t, res, at, key, tc.want, tc.nan)
		})
	}
}

// TestBinaryTypeIsSyntactic pins that a vector-typed comparison stays a vector
// even when its runtime result is empty.
func TestBinaryTypeIsSyntactic(t *testing.T) {
	at := rangeBase.Add(binaryAt)
	src := newMetricSource()
	cases := []struct {
		q      string
		typ    plan.ResultType
		values []float64
	}{
		{`1 == 2`, plan.TypeScalar, nil},
		{`vector(1) == 2`, plan.TypeVector, nil},
		{`vector(1) == bool 2`, plan.TypeVector, []float64{0}},
		{`sum(vector(1) == 2)`, plan.TypeVector, nil},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), src, instantParams(tc.q, at))
			if res.Type != tc.typ {
				t.Fatalf("type = %q, want %q", res.Type, tc.typ)
			}
			if tc.typ == plan.TypeScalar {
				if res.Scalar.V != 0 {
					t.Fatalf("scalar = %v, want 0", res.Scalar.V)
				}
				return
			}
			if len(res.Vector) != len(tc.values) {
				t.Fatalf("vector = %v, want %d samples", res.Vector, len(tc.values))
			}
			for i, want := range tc.values {
				if res.Vector[i].Point.V != want {
					t.Fatalf("sample %d = %v, want %v", i, res.Vector[i].Point.V, want)
				}
			}
		})
	}
}

// TestVectorVectorDefaultMatching covers default one-to-one matching on the
// complete label set: identical labels pair up, unmatched samples disappear,
// comparisons filter (or emit 0/1 with bool), and result labels are the left
// side's, including __name__.
func TestVectorVectorDefaultMatching(t *testing.T) {
	rows := append(binaryTestRows(),
		metricRowAt(rangeBase.Add(20*time.Second), "w", map[string]string{"app": "web"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "n", map[string]string{"app": "api", "level": "info", "__name__": "requests"}, nil),
	)
	at := rangeBase.Add(binaryAt)
	apiKey := lab("app", "api", "level", "info").String()
	webKey := lab("app", "web").String()
	namedKey := lab("__name__", "requests", "app", "api", "level", "info").String()
	cases := []struct {
		name string
		q    string
		want map[string]float64
	}{
		{
			name: "add matches identical label sets",
			q:    `count_over_time({app="api"}[1m]) + count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 4, namedKey: 2},
		},
		{
			name: "subtract",
			q:    `count_over_time({app="api"}[1m]) - count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 0, namedKey: 0},
		},
		{
			name: "divide",
			q:    `count_over_time({app="api"}[1m]) / count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 1, namedKey: 1},
		},
		{
			name: "modulo",
			q:    `count_over_time({app="api"}[1m]) % count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 0, namedKey: 0},
		},
		{
			name: "power",
			q:    `count_over_time({app="api"}[1m]) ^ count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 4, namedKey: 1},
		},
		{
			name: "unmatched streams disappear",
			q:    `count_over_time({app="api"}[1m]) + count_over_time({app="web"}[1m])`,
			want: map[string]float64{},
		},
		{
			name: "web matches itself",
			q:    `count_over_time({app="web"}[1m]) + count_over_time({app="web"}[1m])`,
			want: map[string]float64{webKey: 2},
		},
		{
			name: "filtering comparison keeps the left value",
			q:    `count_over_time({app="api"}[1m]) >= count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 2, namedKey: 1},
		},
		{
			name: "filtering comparison drops false matches",
			q:    `count_over_time({app="api"}[1m]) > count_over_time({app="api"}[1m])`,
			want: map[string]float64{},
		},
		{
			name: "bool comparison emits zero for matched samples",
			q:    `count_over_time({app="api"}[1m]) > bool count_over_time({app="api"}[1m])`,
			want: map[string]float64{apiKey: 0, namedKey: 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(rows...)
			res := mustQuery(t, testEngine(), src, instantParams(tc.q, at))
			if res.Type != plan.TypeVector {
				t.Fatalf("type = %q, want vector", res.Type)
			}
			if len(res.Vector) != len(tc.want) {
				t.Fatalf("vector = %v, want %d samples", res.Vector, len(tc.want))
			}
			for _, sample := range res.Vector {
				want, ok := tc.want[sample.Metric.String()]
				if !ok {
					t.Fatalf("unexpected series %s", sample.Metric)
				}
				if sample.Point.V != want {
					t.Fatalf("series %s = %v, want %v", sample.Metric, sample.Point.V, want)
				}
			}
		})
	}
}

// TestBinaryRecursiveChildren drives binary expressions through the vector
// aggregation recursion, including the final-only series limit.
func TestBinaryRecursiveChildren(t *testing.T) {
	at := rangeBase.Add(binaryAt)
	cases := []struct {
		name string
		q    string
		want map[string][]plan.Point
	}{
		{
			name: "sum over arithmetic",
			q:    `sum(count_over_time({app="api"}[1m]) + 1)`,
			want: map[string][]plan.Point{`{}`: {{T: at, V: 3}}},
		},
		{
			name: "grouping applies to the binary result",
			q:    `sum by (level) (count_over_time({app="api"}[1m]) + 1)`,
			want: map[string][]plan.Point{lab("level", "info").String(): {{T: at, V: 3}}},
		},
		{
			name: "nested arithmetic",
			q:    `sum((count_over_time({app="api"}[1m]) + 1) * 2)`,
			want: map[string][]plan.Point{`{}`: {{T: at, V: 6}}},
		},
		{
			name: "sum over vector literals",
			q:    `sum(vector(1) + vector(1))`,
			want: map[string][]plan.Point{`{}`: {{T: at, V: 2}}},
		},
		{
			name: "sum over a vector literal",
			q:    `sum(vector(1))`,
			want: map[string][]plan.Point{`{}`: {{T: at, V: 1}}},
		},
		{
			name: "sum over a filtering comparison",
			q:    `sum(count_over_time({app="api"}[1m]) > 1)`,
			want: map[string][]plan.Point{`{}`: {{T: at, V: 2}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMetricSource(binaryTestRows()...)
			res := mustQuery(t, testEngine(), src, instantParams(tc.q, at))
			assertResult(t, res, tc.want)
		})
	}
}

func TestBinaryScalarChildUnsupported(t *testing.T) {
	for _, q := range []string{
		`sum(1)`,
		`sum(1 + 2)`,
		`sum((1 + 2) * 3)`,
		`sum(sum(1))`,
	} {
		t.Run(q, func(t *testing.T) {
			src := newMetricSource(binaryTestRows()...)
			_, err := testEngine().Query(t.Context(), src, binaryInstant(q))
			if !errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
			if src.selectCalls != 0 || src.aggregateCalls != 0 {
				t.Fatalf("scalar child touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
			}
		})
	}
}

func TestBinaryFinalSeriesLimit(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "c", map[string]string{"app": "api", "pod": "c"}, nil),
	}
	e := New(config.LogQL{MaxEntries: 100, MaxSeries: 1})
	at := rangeBase.Add(binaryAt)

	// Three intermediate series collapse to one final group: the limit applies
	// to the response, not to the recursive child.
	res := mustQuery(t, e, newMetricSource(rows...), instantParams(`sum by () (count_over_time({app="api"}[1m]) + 1)`, at))
	assertResult(t, res, map[string][]plan.Point{`{}`: {{T: at, V: 6}}})

	if _, err := e.Query(t.Context(), newMetricSource(rows...), instantParams(`count_over_time({app="api"}[1m]) + 1`, at)); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("three-series binary err = %v, want ErrLimit", err)
	}
}

// TestBinarySortOrder preserves Loki's sort ordering: an instant expression
// containing sort/sort_desc skips the label sort for the whole response.
func TestBinarySortOrder(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a1", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "a2", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "a3", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "b1", map[string]string{"app": "api", "pod": "b"}, nil),
		metricRowAt(rangeBase.Add(10*time.Second), "c1", map[string]string{"app": "api", "pod": "c"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "c2", map[string]string{"app": "api", "pod": "c"}, nil),
	}
	at := rangeBase.Add(50 * time.Second)
	cases := []struct {
		name string
		q    string
		want []float64
	}{
		{
			name: "sort then add keeps value order",
			q:    `sort(count_over_time({app="api"}[1m])) + 1`,
			want: []float64{2, 3, 4},
		},
		{
			name: "scalar left preserves the sorted right",
			q:    `1 + sort(count_over_time({app="api"}[1m]))`,
			want: []float64{2, 3, 4},
		},
		{
			name: "sort_desc then add keeps reverse value order",
			q:    `sort_desc(count_over_time({app="api"}[1m])) + 1`,
			want: []float64{4, 3, 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := mustQuery(t, testEngine(), newMetricSource(rows...), instantParams(tc.q, at))
			if res.Type != plan.TypeVector || len(res.Vector) != len(tc.want) {
				t.Fatalf("result = %+v, want %d vector samples", res, len(tc.want))
			}
			for i, want := range tc.want {
				if res.Vector[i].Point.V != want {
					t.Fatalf("sample %d = %v, want %v (order %v)", i, res.Vector[i].Point.V, want, res.Vector)
				}
			}
		})
	}
}

func TestBinarySparseRangeSteps(t *testing.T) {
	// The row is in the first window only. The second step has no sample, so
	// the series keeps its single point and no zero is invented.
	rows := []plan.Row{metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil)}
	start := rangeBase.Add(30 * time.Second)
	end := start.Add(time.Minute)
	res := mustQuery(t, testEngine(), newMetricSource(rows...), rangeParams(`count_over_time({app="api"}[1m]) + 1`, start, end, time.Minute))
	// The frontend aligns the raw 03:04:30..03:05:30 request to the minute
	// grid: 03:04..03:06. The row at 03:04:10 falls in the 03:05 window.
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 2}},
	})

	// Per-step vector/vector matching is sparse: each series pairs only where
	// both sides have a sample at that step.
	rows = []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "pod": "a"}, nil),
		metricRowAt(rangeBase.Add(70*time.Second), "b", map[string]string{"app": "api", "pod": "b"}, nil),
	}
	res = mustQuery(t, testEngine(), newMetricSource(rows...),
		rangeParams(`count_over_time({app="api"}[1m]) + count_over_time({app="api"}[1m])`, start, end, time.Minute))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "pod", "a").String(): {{T: rangeBase.Add(time.Minute), V: 2}},
		lab("app", "api", "pod", "b").String(): {{T: rangeBase.Add(2 * time.Minute), V: 2}},
	})
}

// TestVectorVectorAbsentLabelDoesNotMatchEmpty pins that a missing label and a
// label with an empty value are different identities, as Loki's label-set hash
// treats them.
func TestVectorVectorAbsentLabelDoesNotMatchEmpty(t *testing.T) {
	at := rangeBase.Add(binaryAt)
	rows := binaryTestRows()

	res := mustQuery(t, testEngine(), newMetricSource(rows...),
		binaryInstant(`count_over_time({app="api"}[1m]) + count_over_time({app="api"} | label_format x="" [1m])`))
	if res.Type != plan.TypeVector || len(res.Vector) != 0 {
		t.Fatalf("absent vs empty = %+v, want no match", res)
	}

	res = mustQuery(t, testEngine(), newMetricSource(rows...),
		binaryInstant(`count_over_time({app="api"} | label_format x="" [1m]) + count_over_time({app="api"} | label_format x="" [1m])`))
	assertOneVectorValue(t, res, at, lab("app", "api", "level", "info", "x", "").String(), 4, false)
}

func TestBinaryAliasedSourceRows(t *testing.T) {
	src := newMetricSource(binaryTestRows()...)
	src.mutate = true
	at := rangeBase.Add(binaryAt)
	res := mustQuery(t, testEngine(), src, binaryInstant(`count_over_time({app="api"}[1m]) + 1`))
	assertOneVectorValue(t, res, at, lab("app", "api", "level", "info").String(), 3, false)
}

func TestBinaryCancellationAndSourceError(t *testing.T) {
	// A canceled request never opens the Source, even for a vector literal.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	src := newMetricSource(binaryTestRows()...)
	if _, err := testEngine().Query(canceled, src, binaryInstant(`vector(1) + vector(1)`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled vector err = %v, want context.Canceled", err)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("canceled request touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}

	// Cancellation during the child's Source iteration surfaces before the
	// binary step loop.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src = newMetricSource(binaryTestRows()...)
	src.onAggregate = func(int) { cancel() }
	if _, err := testEngine().Query(ctx, src, binaryInstant(`count_over_time({app="api"}[1m]) + 1`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-query cancel err = %v, want context.Canceled", err)
	}

	// A Source error propagates through the recursive child unchanged. The
	// or-filter cannot be pushed down, so this query takes the Select path.
	boom := errors.New("boom")
	src = newMetricSource(binaryTestRows()...)
	src.err = boom
	src.selectErrAfter = 1
	_, err := testEngine().Query(t.Context(), src, binaryInstant(`count_over_time({app="api"} |= "a" or "zzz" [1m]) + 1`))
	if !errors.Is(err, boom) {
		t.Fatalf("source error = %v, want boom", err)
	}
}

func TestBinaryDuplicateCardinality(t *testing.T) {
	b := &BinaryExpr{Op: OpAdd}
	at := rangeBase
	dup := []vectorInput{
		vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1}),
		vectorInputOf([]string{"k", "v"}, map[int]float64{0: 2}),
	}
	err := binaryVectorStep(t.Context(), b, 0, at, dup, dup, &metricSet{})
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), "duplicate series") {
		t.Fatalf("duplicate right err = %v, want duplicate-series ErrBadQuery", err)
	}
	lhs := []vectorInput{
		vectorInputOf([]string{"k", "v"}, map[int]float64{0: 1}),
		vectorInputOf([]string{"k", "v"}, map[int]float64{0: 2}),
	}
	rhs := []vectorInput{vectorInputOf([]string{"k", "v"}, map[int]float64{0: 3})}
	err = binaryVectorStep(t.Context(), b, 0, at, lhs, rhs, &metricSet{})
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), "multiple matches") {
		t.Fatalf("multiple lhs err = %v, want multiple-matches ErrBadQuery", err)
	}
}

func TestBinaryInvalidASTDoesNotPanic(t *testing.T) {
	e := testEngine()
	ctx := t.Context()
	p := binaryInstant(`1`)

	if _, err := exprKind(&BinaryExpr{}); err == nil {
		t.Fatal("exprKind(empty binary) succeeded")
	}
	if _, err := e.evalBinary(ctx, nil, &BinaryExpr{}, p); err == nil {
		t.Fatal("evalBinary(empty binary) succeeded")
	}
	if _, err := e.evalBinarySeriesGrid(ctx, nil, &BinaryExpr{}, p); err == nil {
		t.Fatal("evalBinarySeriesGrid(empty binary) succeeded")
	}
	if _, err := e.evalVectorLiteral(ctx, nil, p); err == nil {
		t.Fatal("evalVectorLiteral(nil) succeeded")
	}
	if _, err := e.evalVectorSeriesExpr(ctx, nil, &LogRange{}, p); !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("log range child err = %v, want ErrUnsupported", err)
	}
	if _, err := scalarValue(&BinaryExpr{LHS: &VectorLiteral{V: 1}}); err == nil {
		t.Fatal("scalarValue(vector) succeeded")
	}
	if _, _, err := binOpValue(OpAnd, 1, 2); !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("set operator err = %v, want ErrUnsupported", err)
	}
	// A scalar binary child of a vector aggregation is rejected, not
	// evaluated.
	src := newMetricSource()
	a := &VectorAgg{Func: Sum, Left: &BinaryExpr{Op: OpAdd, LHS: &NumberLit{V: 1}, RHS: &NumberLit{V: 2}}}
	if _, err := e.evalVectorSeries(ctx, src, a, p); !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("scalar binary child err = %v, want ErrUnsupported", err)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("scalar binary child touched the Source: %d select / %d aggregate", src.selectCalls, src.aggregateCalls)
	}
}
