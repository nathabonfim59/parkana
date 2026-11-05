package logql

import (
	"errors"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// metricTimeRows is one row that every 1m window in these tests contains.
func metricTimeRows() []plan.Row {
	return []plan.Row{metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil)}
}

// evalExact runs a parsed metric expression through the evaluator without the
// frontend grid normalization Engine.Query applies. It keeps the lower-level
// exact-grid tests able to exercise sub-millisecond steps and raw starts.
func evalExact(t *testing.T, e *Engine, src plan.Source, q string, p plan.Params) plan.Result {
	t.Helper()
	expr, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	res, err := e.evalMetric(t.Context(), src, expr, p)
	if err != nil {
		t.Fatalf("evalMetric(%q): %v", q, err)
	}
	return res
}

// pointsAt builds a one-series expectation from explicit timestamps.
func pointsAt(times []time.Time, values ...float64) map[string][]plan.Point {
	points := make([]plan.Point, len(times))
	for i := range times {
		points[i] = plan.Point{T: times[i], V: values[i]}
	}
	return map[string][]plan.Point{lab("app", "api").String(): points}
}

// TestMetricOutputRangeOffsets pins Loki's range timestamp formula:
// trunc((t-offset)/1ms) + trunc(offset/1ms), with the data time and the offset
// truncated separately. The request time carries a non-whole-millisecond
// fraction, and both whole and sub-millisecond offsets are covered in both
// directions.
func TestMetricOutputRangeOffsets(t *testing.T) {
	start := rangeBase.Add(123456789 * time.Nanosecond)
	cases := []struct {
		name   string
		offset string
		want   time.Time
	}{
		{"no offset", "", rangeBase.Add(123 * time.Millisecond)},
		{"whole positive offset", " offset 1h", rangeBase.Add(123 * time.Millisecond)},
		{"sub-ms positive offset", " offset 1h500us", rangeBase.Add(122 * time.Millisecond)},
		{"whole negative offset", " offset -1h", rangeBase.Add(123 * time.Millisecond)},
		{"sub-ms negative offset", " offset -1h500us", rangeBase.Add(123 * time.Millisecond)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// One row in each offset's data window: the window is shifted by
			// the offset, so the rows sit at -1h, 0 and +1h.
			src := newMetricSource(
				metricRowAt(rangeBase.Add(-time.Hour), "a", map[string]string{"app": "api"}, nil),
				metricRowAt(rangeBase, "b", map[string]string{"app": "api"}, nil),
				metricRowAt(rangeBase.Add(time.Hour), "c", map[string]string{"app": "api"}, nil),
			)
			q := `count_over_time({app="api"}[1m]` + tc.offset + `)`
			res := mustQuery(t, testEngine(), src, instantParams(q, start))
			assertResult(t, res, pointsAt([]time.Time{tc.want}, 1))
		})
	}
}

// TestMetricOutputSubMillisecondFractionNoDoubleRounding uses a request
// fraction above 0.5ms: truncating the request first and then applying the
// offset would lose a millisecond, while Loki truncates the data time once.
func TestMetricOutputSubMillisecondFractionNoDoubleRounding(t *testing.T) {
	start := rangeBase.Add(123756789 * time.Nanosecond)
	src := newMetricSource(metricTimeRows()...)
	res := mustQuery(t, testEngine(), src, instantParams(`count_over_time({app="api"}[1m] offset 500us)`, start))
	// Loki truncates the data time once: 123.256789ms -> 123ms. Rounding the
	// request first would subtract 0.5ms from 123ms and emit 122ms.
	assertResult(t, res, pointsAt([]time.Time{rangeBase.Add(123 * time.Millisecond)}, 1))
}

// TestMetricOutputRangeGrid applies the formula at every step of a range query
// whose start is not a whole millisecond.
func TestMetricOutputRangeGrid(t *testing.T) {
	start := rangeBase.Add(123456789 * time.Nanosecond)
	end := start.Add(3 * time.Millisecond)
	src := newMetricSource(metricTimeRows()...)
	q := `count_over_time({app="api"}[1m] offset 500us)`
	res := evalExact(t, testEngine(), src, q, rangeParams(q, start, end, 1500*time.Microsecond))
	assertResult(t, res, pointsAt([]time.Time{
		rangeBase.Add(122 * time.Millisecond), // 123.456789 - 0.5 = 122.956789
		rangeBase.Add(124 * time.Millisecond), // 124.956789
		rangeBase.Add(125 * time.Millisecond), // 126.456789 - 0.5 = 125.956789
	}, 1, 1, 1))
}

// TestMetricOutputSelectionPrecision proves output rounding never reaches the
// Source: the selection still uses the exact microsecond window bounds.
func TestMetricOutputSelectionPrecision(t *testing.T) {
	start := rangeBase.Add(123456789 * time.Nanosecond)
	src := newMetricSource(metricTimeRows()...)
	// The or-filter cannot be pushed down, so this takes the Select path and
	// records the Selection.
	q := `count_over_time({app="api"} |= "a" or "z" [1m] offset 500us)`
	evalExact(t, testEngine(), src, q, instantParams(q, start))
	if len(src.selections) != 1 {
		t.Fatalf("selections = %d, want 1", len(src.selections))
	}
	sel := src.selections[0]
	wantFrom := rangeBase.Add(-time.Minute + 122957*time.Microsecond)
	wantThrough := rangeBase.Add(122957 * time.Microsecond)
	if !sel.From.Equal(wantFrom) || !sel.Through.Equal(wantThrough) {
		t.Fatalf("selection = [%s, %s), want [%s, %s)", sel.From, sel.Through, wantFrom, wantThrough)
	}

	// The union of a range query's windows keeps the same precision.
	src = newMetricSource(metricTimeRows()...)
	end := start.Add(3 * time.Millisecond)
	evalExact(t, testEngine(), src, q, rangeParams(q, start, end, 1500*time.Microsecond))
	if len(src.selections) != 1 {
		t.Fatalf("range selections = %d, want 1", len(src.selections))
	}
	sel = src.selections[0]
	wantThrough = rangeBase.Add(3*time.Millisecond + 122957*time.Microsecond)
	if !sel.From.Equal(wantFrom) || !sel.Through.Equal(wantThrough) {
		t.Fatalf("range selection = [%s, %s), want [%s, %s)", sel.From, sel.Through, wantFrom, wantThrough)
	}
}

// TestMetricOutputScalarAndVectorRootsNegativeUnix pins the family difference
// for negative Unix times: scalar roots and direct vector ranges truncate
// toward zero, while a direct vector instant uses UnixMilli (floor).
func TestMetricOutputScalarAndVectorRootsNegativeUnix(t *testing.T) {
	at := time.Unix(0, -500000).UTC() // -0.5ms
	epoch := time.UnixMilli(0).UTC()
	before := time.UnixMilli(-1).UTC()

	res := mustQuery(t, testEngine(), newMetricSource(), instantParams(`1`, at))
	if res.Type != plan.TypeScalar || !res.Scalar.T.Equal(epoch) {
		t.Fatalf("scalar root = %+v, want T %s", res.Scalar, epoch)
	}
	res = mustQuery(t, testEngine(), newMetricSource(), instantParams(`vector(1)`, at))
	if res.Type != plan.TypeVector || len(res.Vector) != 1 || !res.Vector[0].T.Equal(before) {
		t.Fatalf("vector root instant = %+v, want T %s", res, before)
	}

	start := time.Unix(0, -500000).UTC()
	end := time.Unix(0, 500000).UTC()
	step := 500 * time.Microsecond
	// Direct roots iterate the exact times and truncate each one. These use
	// the lower-level evaluator because Engine.Query now normalizes a
	// positive range step to whole milliseconds.
	res = evalExact(t, testEngine(), newMetricSource(), `1 + 1`, rangeParams(`1 + 1`, start, end, step))
	want := []plan.Point{{T: epoch, V: 2}, {T: epoch, V: 2}, {T: epoch, V: 2}}
	if res.Type != plan.TypeMatrix || len(res.Matrix) != 1 || !pointsEqual(res.Matrix[0].Points, want) {
		t.Fatalf("scalar range = %+v, want %v", res, want)
	}
	res = evalExact(t, testEngine(), newMetricSource(), `vector(1)`, rangeParams(`vector(1)`, start, end, step))
	if len(res.Matrix) != 1 || !pointsEqual(res.Matrix[0].Points, []plan.Point{{T: epoch, V: 1}, {T: epoch, V: 1}, {T: epoch, V: 1}}) {
		t.Fatalf("direct vector range = %+v, want exact-grid truncation", res)
	}
	// A nested vector literal uses the VectorIterator's integer-ms grid:
	// start floor -1ms, end floor 0ms, 1ms step -> two points.
	res = evalExact(t, testEngine(), newMetricSource(), "vector(1) + vector(1)", rangeParams("vector(1) + vector(1)", start, end, step))
	if len(res.Matrix) != 1 || !pointsEqual(res.Matrix[0].Points, []plan.Point{{T: before, V: 2}, {T: epoch, V: 2}}) {
		t.Fatalf("nested vector range = %+v, want [-1ms, 0ms]", res)
	}
	res = evalExact(t, testEngine(), newMetricSource(), "sum(vector(1))", rangeParams("sum(vector(1))", start, end, step))
	if len(res.Matrix) != 1 || !pointsEqual(res.Matrix[0].Points, []plan.Point{{T: before, V: 1}, {T: epoch, V: 1}}) {
		t.Fatalf("sum(vector) range = %+v, want [-1ms, 0ms]", res)
	}
}

// TestMetricOutputNestedVectorGridCounts pins the nested VectorIterator grid
// and its step count against the direct root's exact grid: 1500us expands
// (1ms spacing), 1us contracts (one millisecond).
func TestMetricOutputNestedVectorGridCounts(t *testing.T) {
	start := rangeBase
	end := start.Add(10 * time.Millisecond)
	step := 1500 * time.Microsecond

	direct := evalExact(t, testEngine(), newMetricSource(), `vector(1)`, rangeParams(`vector(1)`, start, end, step))
	if len(direct.Matrix) != 1 || len(direct.Matrix[0].Points) != 7 {
		t.Fatalf("direct vector points = %d, want 7 nominal steps", len(direct.Matrix[0].Points))
	}
	wantDirect := []time.Time{
		rangeBase, rangeBase.Add(time.Millisecond), rangeBase.Add(3 * time.Millisecond),
		rangeBase.Add(4 * time.Millisecond), rangeBase.Add(6 * time.Millisecond),
		rangeBase.Add(7 * time.Millisecond), rangeBase.Add(9 * time.Millisecond),
	}
	for i, at := range wantDirect {
		if !direct.Matrix[0].Points[i].T.Equal(at) {
			t.Fatalf("direct point %d = %s, want %s", i, direct.Matrix[0].Points[i].T, at)
		}
	}

	nested := evalExact(t, testEngine(), newMetricSource(), "vector(1) + vector(1)", rangeParams("vector(1) + vector(1)", start, end, step))
	if len(nested.Matrix) != 1 || len(nested.Matrix[0].Points) != 11 {
		t.Fatalf("nested vector points = %d, want 11 one-ms steps", len(nested.Matrix[0].Points))
	}
	for i, point := range nested.Matrix[0].Points {
		want := rangeBase.Add(time.Duration(i) * time.Millisecond)
		if !point.T.Equal(want) || point.V != 2 {
			t.Fatalf("nested point %d = %+v, want V2 at %s", i, point, want)
		}
	}
	agg := evalExact(t, testEngine(), newMetricSource(), "sum(vector(1))", rangeParams("sum(vector(1))", start, end, step))
	if len(agg.Matrix) != 1 || len(agg.Matrix[0].Points) != 11 {
		t.Fatalf("sum(vector) points = %d, want the child's 11", len(agg.Matrix[0].Points))
	}

	// 1us steps: six nominal points but only one millisecond in the nested
	// VectorIterator grid.
	end = start.Add(5 * time.Microsecond)
	step = time.Microsecond
	direct = evalExact(t, testEngine(), newMetricSource(), `vector(1)`, rangeParams(`vector(1)`, start, end, step))
	if len(direct.Matrix) != 1 || len(direct.Matrix[0].Points) != 6 {
		t.Fatalf("1us direct vector points = %d, want 6", len(direct.Matrix[0].Points))
	}
	nested = evalExact(t, testEngine(), newMetricSource(), "vector(1) + vector(1)", rangeParams("vector(1) + vector(1)", start, end, step))
	if len(nested.Matrix) != 1 || len(nested.Matrix[0].Points) != 1 {
		t.Fatalf("1us nested vector points = %d, want 1", len(nested.Matrix[0].Points))
	}
	agg = evalExact(t, testEngine(), newMetricSource(), "sum(vector(1))", rangeParams("sum(vector(1))", start, end, step))
	if len(agg.Matrix) != 1 || len(agg.Matrix[0].Points) != 1 {
		t.Fatalf("1us sum(vector) points = %d, want 1", len(agg.Matrix[0].Points))
	}
}

// TestMetricOutputBinaryGrids pins index-wise synchronization when a binary's
// two children have different step counts, and the lhs timestamp rule.
func TestMetricOutputBinaryGrids(t *testing.T) {
	start := rangeBase
	end := start.Add(10 * time.Millisecond)
	step := 1500 * time.Microsecond

	// Range lhs (7 nominal steps) with nested vector rhs (11): the common
	// prefix is 7 and the output timestamps are the lhs range timestamps. The
	// sum strips the stream labels so the two legs' empty identities match.
	q := `sum(count_over_time({app="api"}[1m])) + vector(1)`
	res := evalExact(t, testEngine(), newMetricSource(metricTimeRows()...), q, rangeParams(q, start, end, step))
	if len(res.Matrix) != 1 || len(res.Matrix[0].Points) != 7 {
		t.Fatalf("range+vector points = %d, want 7", len(res.Matrix[0].Points))
	}
	wantRangeTimes := []time.Time{
		rangeBase, rangeBase.Add(time.Millisecond), rangeBase.Add(3 * time.Millisecond),
		rangeBase.Add(4 * time.Millisecond), rangeBase.Add(6 * time.Millisecond),
		rangeBase.Add(7 * time.Millisecond), rangeBase.Add(9 * time.Millisecond),
	}
	for i, point := range res.Matrix[0].Points {
		if !point.T.Equal(wantRangeTimes[i]) || point.V != 2 {
			t.Fatalf("range+vector point %d = %+v, want V2 at %s", i, point, wantRangeTimes[i])
		}
	}

	// Vector lhs (11 one-ms steps) with range rhs (7): 7 outputs on the
	// vector iterator's timestamps.
	q = `vector(1) + sum(count_over_time({app="api"}[1m]))`
	res = evalExact(t, testEngine(), newMetricSource(metricTimeRows()...), q, rangeParams(q, start, end, step))
	if len(res.Matrix) != 1 || len(res.Matrix[0].Points) != 7 {
		t.Fatalf("vector+range points = %d, want 7", len(res.Matrix[0].Points))
	}
	for i, point := range res.Matrix[0].Points {
		want := start.Add(time.Duration(i) * time.Millisecond)
		if !point.T.Equal(want) || point.V != 2 {
			t.Fatalf("vector+range point %d = %+v, want V2 at %s", i, point, want)
		}
	}

	// A sub-ms range offset survives both operand orders and a nested sum.
	at := rangeBase.Add(123456789 * time.Nanosecond)
	want := rangeBase.Add(122 * time.Millisecond)
	cases := []struct {
		q   string
		key string
	}{
		{`count_over_time({app="api"}[1m] offset 500us) + 1`, lab("app", "api").String()},
		{`1 + count_over_time({app="api"}[1m] offset 500us)`, lab("app", "api").String()},
		{`count_over_time({app="api"}[1m] offset 500us) + count_over_time({app="api"}[1m] offset 500us)`, lab("app", "api").String()},
		{`sum(count_over_time({app="api"}[1m] offset 500us) + 1)`, `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), newMetricSource(metricTimeRows()...), instantParams(tc.q, at))
			assertResult(t, res, map[string][]plan.Point{tc.key: {{T: want, V: 2}}})
		})
	}
}

// TestMetricOutputNegativeRangeOffsets pins the signed-remainder range formula
// around the Unix epoch: the data time and offset are truncated separately.
func TestMetricOutputNegativeRangeOffsets(t *testing.T) {
	at := time.Unix(0, -500000).UTC()
	row := metricRowAt(at.Add(-time.Second), "x", map[string]string{"app": "api"}, nil)
	cases := []struct {
		q    string
		want time.Time
	}{
		{`count_over_time({app="api"}[1m])`, time.Unix(0, 0).UTC()},
		{`count_over_time({app="api"}[1m] offset 500us)`, time.Unix(0, -1000000).UTC()},
		{`count_over_time({app="api"}[1m] offset -500us)`, time.Unix(0, 0).UTC()},
		{`count_over_time({app="api"}[1m] offset 1ms)`, time.Unix(0, 0).UTC()},
		{`count_over_time({app="api"}[1m] offset -1ms)`, time.Unix(0, -1000000).UTC()},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(tc.q, at))
			assertResult(t, res, map[string][]plan.Point{
				lab("app", "api").String(): {{T: tc.want, V: 1}},
			})
		})
	}
}

// TestMetricOutputBinaryDifferentOffsetsKeepsLHS pins the lhs timestamp rule
// when a vector/vector operation's legs carry different offsets.
func TestMetricOutputBinaryDifferentOffsetsKeepsLHS(t *testing.T) {
	at := time.Unix(0, -500000).UTC()
	row := metricRowAt(at.Add(-time.Second), "x", map[string]string{"app": "api"}, nil)
	cases := []struct {
		q    string
		want time.Time
	}{
		{
			q:    `count_over_time({app="api"}[1m] offset 1ms) + count_over_time({app="api"}[1m] offset -1ms)`,
			want: time.Unix(0, 0).UTC(),
		},
		{
			q:    `count_over_time({app="api"}[1m] offset -1ms) + count_over_time({app="api"}[1m] offset 1ms)`,
			want: time.Unix(0, -1000000).UTC(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.q, func(t *testing.T) {
			res := mustQuery(t, testEngine(), newMetricSource(row), instantParams(tc.q, at))
			assertResult(t, res, map[string][]plan.Point{
				lab("app", "api").String(): {{T: tc.want, V: 2}},
			})
		})
	}
}

// TestNestedVectorGridBoundaryRegression is the source-derived boundary from
// the A9 review: a nominal-valid 11001-point request whose nested
// VectorIterator grid is exactly 22001 points must not be rejected.
func TestNestedVectorGridBoundaryRegression(t *testing.T) {
	start := rangeBase.Add(999999 * time.Nanosecond)
	step := 1999999 * time.Nanosecond
	p := rangeParams("sum(vector(1))", start, start.Add(time.Duration(maxMetricSteps)*step), step)
	nominal, err := metricEvalTimes(p)
	if err != nil {
		t.Fatalf("nominal grid rejected: %v", err)
	}
	if len(nominal) != maxMetricSteps+1 {
		t.Fatalf("nominal count = %d, want %d", len(nominal), maxMetricSteps+1)
	}
	grid, err := vectorIterGrid(p)
	if err != nil {
		t.Fatalf("valid nominal limit rejected: %v", err)
	}
	if grid.count() != 22001 {
		t.Fatalf("nested count = %d, want 22001", grid.count())
	}
}

// TestVectorIterGridGuard pins the expanded-grid bound without rejecting the
// nominal query limit.
func TestVectorIterGridGuard(t *testing.T) {
	start := rangeBase
	step := time.Millisecond

	// count == maxNestedVectorSteps is allowed.
	allowed := plan.Params{Start: start, End: start.Add(time.Duration(maxNestedVectorSteps-1) * step), Step: step}
	grid, err := vectorIterGrid(allowed)
	if err != nil || grid.count() != maxNestedVectorSteps {
		t.Fatalf("boundary grid = %d, %v; want %d points", grid.count(), err, maxNestedVectorSteps)
	}

	// One more point is over the bound.
	over := plan.Params{Start: start, End: start.Add(time.Duration(maxNestedVectorSteps) * step), Step: step}
	if _, err := vectorIterGrid(over); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("over-bound err = %v, want ErrLimit", err)
	}

	// Instant and inverted spans stay bounded and do not panic.
	grid, err = vectorIterGrid(plan.Params{Start: start, End: start})
	if err != nil || grid.count() != 1 {
		t.Fatalf("instant grid = %d, %v; want 1 point", grid.count(), err)
	}
	grid, err = vectorIterGrid(plan.Params{Start: start.Add(time.Second), End: start, Step: step})
	if err != nil || grid.count() != 0 {
		t.Fatalf("inverted grid = %d, %v; want 0 points", grid.count(), err)
	}
}
