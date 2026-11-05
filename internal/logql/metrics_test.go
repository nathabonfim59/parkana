package logql

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// metricSource wraps plan.MemSource and records how the engine reached it: the
// number of Select and Aggregate calls, the exact selections and bucket
// parameters, and, optionally, the aliasing behavior of an executor that
// decodes into reused buffers. It can inject errors after N yields or cancel
// the request in the middle of iteration.
type metricSource struct {
	mem *plan.MemSource

	selectCalls, aggregateCalls int
	selections                  []plan.Selection
	aggs                        []plan.BucketAgg

	mutate bool

	err                          error
	selectErrAfter, aggErrAfter  int
	onSelect, onAggregate        func(i int)
	selectYields, aggregateYield int
}

func newMetricSource(rows ...plan.Row) *metricSource {
	mem := plan.NewMemSource()
	mem.Add(tenant.Default, rows...)
	return &metricSource{mem: mem}
}

func (s *metricSource) addTenant(t tenant.ID, rows ...plan.Row) { s.mem.Add(t, rows...) }

func (s *metricSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		s.selectCalls++
		s.selections = append(s.selections, sel)
		var scratchLabels, scratchMeta logs.Labels
		i := 0
		for row, err := range s.mem.Select(ctx, sel) {
			if err != nil {
				yield(plan.Row{}, err)
				return
			}
			if s.onSelect != nil {
				s.onSelect(i)
			}
			if s.selectErrAfter > 0 && i == s.selectErrAfter {
				yield(plan.Row{}, s.err)
				return
			}
			if s.mutate {
				scratchLabels = append(scratchLabels[:0], row.Labels...)
				row.Labels = scratchLabels
				scratchMeta = append(scratchMeta[:0], row.Meta...)
				row.Meta = scratchMeta
			}
			s.selectYields++
			if !yield(row, nil) {
				return
			}
			i++
		}
	}
}

func (s *metricSource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		s.aggregateCalls++
		s.aggs = append(s.aggs, agg)
		var scratchLabels, scratchMeta logs.Labels
		i := 0
		for bucket, err := range s.mem.Aggregate(ctx, sel, agg) {
			if err != nil {
				yield(plan.Bucket{}, err)
				return
			}
			if s.onAggregate != nil {
				s.onAggregate(i)
			}
			if s.aggErrAfter > 0 && i == s.aggErrAfter {
				yield(plan.Bucket{}, s.err)
				return
			}
			if s.mutate {
				scratchLabels = append(scratchLabels[:0], bucket.Labels...)
				bucket.Labels = scratchLabels
				scratchMeta = append(scratchMeta[:0], bucket.Meta...)
				bucket.Meta = scratchMeta
			}
			s.aggregateYield++
			if !yield(bucket, nil) {
				return
			}
			i++
		}
	}
}

func metricRowAt(ts time.Time, line string, labels, meta map[string]string) plan.Row {
	return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(labels), Meta: logs.FromMap(meta)}
}

func rangeParams(q string, start, end time.Time, step time.Duration) plan.Params {
	return plan.Params{
		Tenant:    tenant.Default,
		Query:     q,
		Start:     start,
		End:       end,
		Step:      step,
		Limit:     100,
		Direction: plan.Backward,
	}
}

func instantParams(q string, at time.Time) plan.Params {
	return plan.Params{Tenant: tenant.Default, Query: q, Start: at, End: at, Direction: plan.Backward}
}

func parseRangeAgg(t *testing.T, q string) *RangeAgg {
	t.Helper()
	expr, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	a, ok := expr.(*RangeAgg)
	if !ok {
		t.Fatalf("Parse(%q) = %T, want *RangeAgg", q, expr)
	}
	return a
}

func evalRangeWith(t *testing.T, e *Engine, src plan.Source, q string, p plan.Params, opts rangeOptions) plan.Result {
	t.Helper()
	res, err := e.evalRangeAgg(t.Context(), src, parseRangeAgg(t, q), p, opts)
	if err != nil {
		t.Fatalf("evalRangeAgg(%q): %v", q, err)
	}
	return res
}

// resultPoints flattens a matrix or vector into label-set -> points. A
// duplicate label set is a result bug (Loki never returns two series with the
// same identity), so it fails the test instead of silently overwriting.
func resultPoints(t *testing.T, res plan.Result) map[string][]plan.Point {
	t.Helper()
	out := make(map[string][]plan.Point, len(res.Matrix)+len(res.Vector))
	add := func(key string, points []plan.Point) {
		if _, dup := out[key]; dup {
			t.Fatalf("duplicate series %s in %s result", key, res.Type)
		}
		out[key] = points
	}
	switch res.Type {
	case plan.TypeMatrix:
		for _, s := range res.Matrix {
			add(s.Metric.String(), s.Points)
		}
	case plan.TypeVector:
		for _, s := range res.Vector {
			add(s.Metric.String(), []plan.Point{s.Point})
		}
	default:
		t.Fatalf("result type = %q, want matrix or vector", res.Type)
	}
	return out
}

func assertResult(t *testing.T, res plan.Result, want map[string][]plan.Point) {
	t.Helper()
	got := resultPoints(t, res)
	if len(got) != len(want) {
		t.Fatalf("result has %d series, want %d: got %v", len(got), len(want), got)
	}
	for key, wantPoints := range want {
		gotPoints, ok := got[key]
		if !ok {
			t.Fatalf("missing series %s; got %v", key, maps.Keys(got))
		}
		if !pointsEqual(gotPoints, wantPoints) {
			t.Fatalf("series %s = %v, want %v", key, gotPoints, wantPoints)
		}
	}
}

func assertSameResult(t *testing.T, got, want plan.Result) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("type = %q, want %q", got.Type, want.Type)
	}
	assertResult(t, got, resultPoints(t, want))
}

func TestQueryMetricEdgesAndShapes(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(-time.Minute), "edge", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "be", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(time.Minute), "c", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(time.Minute+time.Microsecond), "dd", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(2*time.Minute), "e", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	label := lab("app", "api").String()
	t1, t2, t3 := rangeBase, rangeBase.Add(time.Minute), rangeBase.Add(2*time.Minute)

	for _, tc := range []struct {
		name string
		q    string
		want []plan.Point
	}{
		{"count", `count_over_time({app="api"}[1m])`, []plan.Point{{T: t1, V: 1}, {T: t2, V: 2}, {T: t3, V: 2}}},
		{"bytes", `bytes_over_time({app="api"}[1m])`, []plan.Point{{T: t1, V: 1}, {T: t2, V: 3}, {T: t3, V: 3}}},
		{"rate", `rate({app="api"}[1m])`, []plan.Point{{T: t1, V: 1.0 / 60}, {T: t2, V: 2.0 / 60}, {T: t3, V: 2.0 / 60}}},
		{"bytes_rate", `bytes_rate({app="api"}[1m])`, []plan.Point{{T: t1, V: 1.0 / 60}, {T: t2, V: 3.0 / 60}, {T: t3, V: 3.0 / 60}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := rangeParams(tc.q, t1, t3, time.Minute)
			res := mustQuery(t, e, src, p)
			if res.Type != plan.TypeMatrix {
				t.Fatalf("type = %q, want matrix", res.Type)
			}
			assertResult(t, res, map[string][]plan.Point{label: tc.want})
			if src.aggregateCalls != 1 || src.selectCalls != 0 {
				t.Fatalf("calls = %d aggregate / %d select, want exactly one aggregate", src.aggregateCalls, src.selectCalls)
			}
			if !src.aggs[0].IncludeMeta {
				t.Fatalf("metadata-aware Aggregate expected (IncludeMeta)")
			}
			src.aggregateCalls = 0
		})
	}

	// Instant query: one exact window at the request time.
	src.aggregateCalls = 0
	instant := mustQuery(t, e, src, instantParams(`count_over_time({app="api"}[1m])`, rangeBase.Add(30*time.Second)))
	if instant.Type != plan.TypeVector {
		t.Fatalf("instant type = %q, want vector", instant.Type)
	}
	assertResult(t, instant, map[string][]plan.Point{
		label: {{T: rangeBase.Add(30 * time.Second), V: 2}},
	})
	if src.aggregateCalls != 1 {
		t.Fatalf("instant aggregate calls = %d, want 1", src.aggregateCalls)
	}
}

func TestQueryMetricNonalignedOverlap(t *testing.T) {
	// Rows chosen to pin both edges of every overlapping window; see the
	// hand-computed counts below.
	src := newMetricSource(
		metricRowAt(rangeBase.Add(-90*time.Second), "y", map[string]string{"app": "api"}, nil),                  // left of w1, excluded
		metricRowAt(rangeBase.Add(-90*time.Second+time.Microsecond), "x", map[string]string{"app": "api"}, nil), // w1 left edge
		metricRowAt(rangeBase.Add(-89*time.Second), "x", map[string]string{"app": "api"}, nil),                  // w1
		metricRowAt(rangeBase, "x", map[string]string{"app": "api"}, nil),                                       // w1 right edge, w2
		metricRowAt(rangeBase.Add(time.Second), "x", map[string]string{"app": "api"}, nil),                      // w2, w3
		metricRowAt(rangeBase.Add(40*time.Second), "x", map[string]string{"app": "api"}, nil),                   // w2 right edge, w3
		metricRowAt(rangeBase.Add(41*time.Second), "x", map[string]string{"app": "api"}, nil),                   // w3
		metricRowAt(rangeBase.Add(80*time.Second), "x", map[string]string{"app": "api"}, nil),                   // w3 right edge
	)
	e := testEngine()
	label := lab("app", "api").String()
	t1 := rangeBase
	t2 := rangeBase.Add(40 * time.Second)
	t3 := rangeBase.Add(80 * time.Second)
	want := map[string][]plan.Point{
		label: {{T: t1, V: 3}, {T: t2, V: 3}, {T: t3, V: 5}},
	}

	count := mustQuery(t, e, src, rangeParams(`count_over_time({app="api"}[90s])`, t1, t3, 40*time.Second))
	assertResult(t, count, want)
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
	}
	if got := src.aggs[0].Width; got != 10*time.Second {
		t.Fatalf("bucket width = %s, want gcd(90s,40s)=10s", got)
	}

	bytes := mustQuery(t, e, src, rangeParams(`bytes_over_time({app="api"}[90s])`, t1, t3, 40*time.Second))
	assertResult(t, bytes, want)

	rateWant := map[string][]plan.Point{
		label: {{T: t1, V: 3.0 / 90}, {T: t2, V: 3.0 / 90}, {T: t3, V: 5.0 / 90}},
	}
	rate := mustQuery(t, e, src, rangeParams(`rate({app="api"}[90s])`, t1, t3, 40*time.Second))
	assertResult(t, rate, rateWant)

	// The identical query through Select must agree.
	ref := evalRangeWith(t, e, src, `count_over_time({app="api"}[90s])`, rangeParams(`count_over_time({app="api"}[90s])`, t1, t3, 40*time.Second), rangeOptions{})
	assertSameResult(t, count, ref)
}

func TestQueryMetricCallCounts(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()

	// Hundreds of steps, still one call.
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(600*time.Second), time.Second)
	mustQuery(t, e, src, p)
	if src.aggregateCalls != 1 || src.selectCalls != 0 {
		t.Fatalf("calls = %d aggregate / %d select, want exactly one", src.aggregateCalls, src.selectCalls)
	}
	if want := rangeBase.Add(-time.Minute + time.Microsecond); !src.aggs[0].Origin.Equal(want) {
		t.Fatalf("grid origin = %v, want %v", src.aggs[0].Origin, want)
	}

	// A pipeline that needs rows must never call Aggregate.
	src2 := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "x", map[string]string{"app": "api"}, nil),
	)
	q := `count_over_time({app="api"} | level =~ ".*" [1m])`
	mustQuery(t, e, src2, rangeParams(q, rangeBase, rangeBase.Add(time.Minute), time.Minute))
	if src2.aggregateCalls != 0 || src2.selectCalls != 1 {
		t.Fatalf("pipeline calls = %d aggregate / %d select, want one select", src2.aggregateCalls, src2.selectCalls)
	}
}

func TestQueryMetricFallbackMatchesSelect(t *testing.T) {
	// 2ms range with a 2.5µs step needs 2000 one-microsecond buckets per
	// window, above the bound, so the engine issues one exact Aggregate per
	// window; the Select reference must agree.
	src := newMetricSource(
		metricRowAt(rangeBase, "aa", map[string]string{"app": "api"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(time.Microsecond), "b", map[string]string{"app": "api"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(2*time.Microsecond), "ccc", map[string]string{"app": "api"}, map[string]string{"trace": "2"}),
		metricRowAt(rangeBase.Add(5*time.Microsecond), "d", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	start := rangeBase
	end := rangeBase.Add(5 * time.Microsecond)
	step := 2500 * time.Nanosecond
	q := `count_over_time({app="api"}[2ms])`
	fast := evalRangeWith(t, e, src, q, rangeParams(q, start, end, step), rangeOptions{aggregate: true})
	if src.aggregateCalls != 3 || src.selectCalls != 0 {
		t.Fatalf("calls = %d aggregate / %d select, want 3 aggregates", src.aggregateCalls, src.selectCalls)
	}
	ref := evalRangeWith(t, e, src, q, rangeParams(q, start, end, step), rangeOptions{})
	assertSameResult(t, fast, ref)

	// Bytes and rate too.
	for _, fq := range []string{`bytes_over_time({app="api"}[2ms])`, `rate({app="api"}[2ms])`, `bytes_rate({app="api"}[2ms])`} {
		t.Run(fq, func(t *testing.T) {
			src.aggregateCalls = 0
			fast := evalRangeWith(t, e, src, fq, rangeParams(fq, start, end, step), rangeOptions{aggregate: true})
			if src.aggregateCalls != 3 {
				t.Fatalf("aggregate calls = %d, want 3", src.aggregateCalls)
			}
			ref := evalRangeWith(t, e, src, fq, rangeParams(fq, start, end, step), rangeOptions{})
			assertSameResult(t, fast, ref)
		})
	}
}

func TestQueryMetricBucketBoundary(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(2*time.Microsecond), "b", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()

	// Exactly 1000 buckets per window: still one call.
	q := `count_over_time({app="api"}[2ms])`
	p := rangeParams(q, rangeBase, rangeBase.Add(4*time.Microsecond), 2*time.Microsecond)
	fast := evalRangeWith(t, e, src, q, p, rangeOptions{aggregate: true})
	if src.aggregateCalls != 1 {
		t.Fatalf("1000-bucket query aggregate calls = %d, want 1", src.aggregateCalls)
	}
	assertSameResult(t, fast, evalRangeWith(t, e, src, q, p, rangeOptions{}))

	// 2000 buckets per window: per-window fallback, same result.
	src.aggregateCalls = 0
	p = rangeParams(q, rangeBase, rangeBase.Add(5*time.Microsecond), 2500*time.Nanosecond)
	fallback := evalRangeWith(t, e, src, q, p, rangeOptions{aggregate: true})
	if src.aggregateCalls != 3 {
		t.Fatalf("2000-bucket query aggregate calls = %d, want 3", src.aggregateCalls)
	}
	assertSameResult(t, fallback, evalRangeWith(t, e, src, q, p, rangeOptions{}))
}

func TestQueryMetricZeroRange(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(time.Minute), "b", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	for _, fn := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		t.Run(fn, func(t *testing.T) {
			q := fn + `({app="api"}[0s])`
			res := mustQuery(t, e, src, rangeParams(q, rangeBase, rangeBase.Add(2*time.Minute), time.Minute))
			if res.Type != plan.TypeMatrix || len(res.Matrix) != 0 {
				t.Fatalf("zero range result = %+v, want empty matrix", res)
			}
			inst := mustQuery(t, e, src, instantParams(q, rangeBase))
			if inst.Type != plan.TypeVector || len(inst.Vector) != 0 {
				t.Fatalf("zero range instant = %+v, want empty vector", inst)
			}
			if src.aggregateCalls != 0 || src.selectCalls != 0 {
				t.Fatalf("zero range opened the Source: %d aggregate / %d select", src.aggregateCalls, src.selectCalls)
			}
		})
	}
}

func TestQueryMetricEmptyAndSparseWindows(t *testing.T) {
	e := testEngine()
	empty := newMetricSource()
	res := mustQuery(t, e, empty, rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute))
	if len(resultPoints(t, res)) != 0 {
		t.Fatalf("empty source result = %+v, want no series", res)
	}

	// Data only in the last window: one point, no invented zeros.
	sparse := newMetricSource(metricRowAt(rangeBase.Add(2*time.Minute), "a", map[string]string{"app": "api"}, nil))
	res = mustQuery(t, e, sparse, rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(2 * time.Minute), V: 1}},
	})
}

func TestQueryMetricNegativeUnix(t *testing.T) {
	base := time.Unix(-100, 0).UTC()
	src := newMetricSource(
		metricRowAt(base.Add(-30*time.Second), "a", map[string]string{"app": "api"}, nil),
		metricRowAt(base.Add(-10*time.Second), "b", map[string]string{"app": "api"}, nil),
		metricRowAt(base, "c", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	res := mustQuery(t, e, src, rangeParams(`count_over_time({app="api"}[20s])`, base, base.Add(10*time.Second), 10*time.Second))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: base, V: 2},
			{T: base.Add(10 * time.Second), V: 1},
		},
	})
}

func TestQueryMetricOffset(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(-time.Hour-45*time.Second), "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(-time.Hour), "b", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(-time.Hour+10*time.Second), "c", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	start := rangeBase.Add(-30 * time.Second)
	end := rangeBase.Add(30 * time.Second)
	q := `count_over_time({app="api"}[1m] offset 1h)`
	res := mustQuery(t, e, src, rangeParams(q, start, end, 30*time.Second))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: start, V: 1},
			{T: rangeBase, V: 2},
			{T: end, V: 2},
		},
	})

	// Negative offset looks forward, and the output timestamp is still the
	// original evaluation time.
	neg := newMetricSource(
		metricRowAt(rangeBase.Add(time.Hour-10*time.Second), "a", map[string]string{"app": "api"}, nil),
	)
	nq := `count_over_time({app="api"}[1m] offset -1h)`
	nres := mustQuery(t, e, neg, instantParams(nq, rangeBase))
	assertResult(t, nres, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase, V: 1}},
	})
}

func TestQueryMetricSubMicrosecond(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase, "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(time.Microsecond), "b", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(2*time.Microsecond), "c", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(4*time.Microsecond), "d", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	start := rangeBase
	end := rangeBase.Add(4500 * time.Nanosecond)
	q := `count_over_time({app="api"}[1ms])`
	// A lower-level exact-grid test: Engine.Query normalizes metric steps to
	// whole milliseconds, so this bypasses that normalization.
	res := evalRangeWith(t, e, src, q, rangeParams(q, start, end, 1500*time.Nanosecond), rangeOptions{aggregate: true})
	// Loki emits millisecond timestamps: all four sub-millisecond steps
	// truncate to the same millisecond even though the values are distinct.
	want := map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: start, V: 1},
			{T: start, V: 2},
			{T: start, V: 3},
			{T: start, V: 4},
		},
	}
	assertResult(t, res, want)
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1 (1000 one-us buckets)", src.aggregateCalls)
	}
	assertSameResult(t, res, evalRangeWith(t, e, src, q, rangeParams(q, start, end, 1500*time.Nanosecond), rangeOptions{}))
}

func TestQueryMetricMetadataIdentity(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "2"}),
	)
	e := testEngine()
	q := `count_over_time({app="api"}[1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	res := mustQuery(t, e, src, p)
	want := map[string][]plan.Point{
		lab("app", "api", "level", "info", "trace", "1").String(): {
			{T: rangeBase.Add(30 * time.Second), V: 1},
		},
		lab("app", "api", "level", "info", "trace", "2").String(): {
			{T: rangeBase.Add(30 * time.Second), V: 1},
		},
	}
	assertResult(t, res, want)
	if !src.aggs[0].IncludeMeta {
		t.Fatalf("aggregate must include metadata")
	}
	assertSameResult(t, res, evalRangeWith(t, e, src, q, p, rangeOptions{}))

	// Parsed labels enter the identity on the Select path and force it.
	parsed := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), `{"level":"info"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"level":"warn"}`, map[string]string{"app": "api"}, nil),
	)
	pq := `count_over_time({app="api"} | json [1m])`
	pres := mustQuery(t, e, parsed, rangeParams(pq, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second))
	if parsed.aggregateCalls != 0 || parsed.selectCalls != 1 {
		t.Fatalf("parser query calls = %d aggregate / %d select, want one select", parsed.aggregateCalls, parsed.selectCalls)
	}
	if len(resultPoints(t, pres)) != 2 {
		t.Fatalf("parsed labels did not split series: %v", resultPoints(t, pres))
	}
}

func TestQueryMetricPipelineError(t *testing.T) {
	bad := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "not json", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	_, err := e.Query(t.Context(), bad, rangeParams(`count_over_time({app="api"} | json [1m])`, rangeBase, rangeBase.Add(time.Minute), time.Minute))
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("parser error = %v, want ErrBadQuery", err)
	}

	// __error__="" drops the failed row, so no error survives.
	ok := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "not json", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"x":"1"}`, map[string]string{"app": "api"}, nil),
	)
	res := mustQuery(t, e, ok, rangeParams(`count_over_time({app="api"} | json | __error__="" [1m])`, rangeBase, rangeBase.Add(time.Minute), time.Minute))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "x", "1").String(): {{T: rangeBase.Add(time.Minute), V: 1}},
	})

	// A failed row that belongs to no evaluation window is never evaluated.
	gap := newMetricSource(
		metricRowAt(rangeBase.Add(30*time.Second), "not json", map[string]string{"app": "api"}, nil),
	)
	gres := mustQuery(t, e, gap, rangeParams(`count_over_time({app="api"} | json [1m])`, rangeBase, rangeBase.Add(2*time.Minute), 2*time.Minute))
	if len(resultPoints(t, gres)) != 0 {
		t.Fatalf("gap error produced series: %+v", gres)
	}
}

func TestQueryMetricDropErrorFastPath(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "bb", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "2"}),
	)
	e := testEngine()
	q := `count_over_time({app="api"} | drop __error__[1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	res := mustQuery(t, e, src, p)
	if src.aggregateCalls != 1 || src.selectCalls != 0 {
		t.Fatalf("drop __error__ calls = %d aggregate / %d select, want one aggregate", src.aggregateCalls, src.selectCalls)
	}
	// drop __error__ is a no-op on rows without parser state: same as the
	// plain query through Select.
	plain := `count_over_time({app="api"}[1m])`
	assertSameResult(t, res, evalRangeWith(t, e, src, plain, rangeParams(plain, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second), rangeOptions{}))
}

func TestQueryMetricLabelProjection(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "bb", map[string]string{"app": "api", "level": "warn"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(30*time.Second), "ccc", map[string]string{"app": "web", "level": "info"}, nil),
	}
	e := testEngine()
	p := func(q string) plan.Params {
		return rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	}

	keep := newMetricSource(rows...)
	kres := mustQuery(t, e, keep, p(`count_over_time({app=~"api|web"} | keep level [1m])`))
	if keep.aggregateCalls != 1 {
		t.Fatalf("keep aggregate calls = %d, want 1", keep.aggregateCalls)
	}
	assertResult(t, kres, map[string][]plan.Point{
		lab("level", "info").String(): {{T: rangeBase.Add(30 * time.Second), V: 2}},
		lab("level", "warn").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})

	drop := newMetricSource(rows...)
	dres := mustQuery(t, e, drop, p(`count_over_time({app=~"api|web"} | drop level [1m])`))
	if drop.aggregateCalls != 1 {
		t.Fatalf("drop aggregate calls = %d, want 1", drop.aggregateCalls)
	}
	// trace=1 rows from api merge; web has no metadata.
	assertResult(t, dres, map[string][]plan.Point{
		lab("app", "api", "trace", "1").String(): {{T: rangeBase.Add(30 * time.Second), V: 2}},
		lab("app", "web").String():               {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestQueryMetricGrafanaHistogramPath(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"service_name": "api", "level": "info", "detected_level": "error"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"service_name": "api", "level": "info"}, nil),
	)
	e := testEngine()
	q := `count_over_time({service_name="api"} | drop __error__[1m])`
	res := mustQuery(t, e, src, rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second))
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
	}
	// The stream without detected_level keeps it absent; nothing invents it.
	want := map[string][]plan.Point{
		lab("service_name", "api", "level", "info", "detected_level", "error", "trace", "1").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
		lab("service_name", "api", "level", "info").String():                                          {{T: rangeBase.Add(30 * time.Second), V: 1}},
	}
	assertResult(t, res, want)
}

func TestQueryMetricBytesVsRunes(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "héllo", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	res := mustQuery(t, e, src, instantParams(`bytes_over_time({app="api"}[1m])`, rangeBase.Add(30*time.Second)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(30 * time.Second), V: 6}},
	})
}

func TestQueryMetricMaxSeries(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "k": "1"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "k": "2"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "c", map[string]string{"app": "api", "k": "3"}, nil),
	}
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)

	strict := New(config.LogQL{MaxEntries: 100, MaxSeries: 2})
	src := newMetricSource(rows...)
	if _, err := strict.Query(t.Context(), src, p); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("err = %v, want ErrLimit", err)
	}

	fits := New(config.LogQL{MaxEntries: 100, MaxSeries: 3})
	res := mustQuery(t, fits, newMetricSource(rows...), p)
	if len(resultPoints(t, res)) != 3 {
		t.Fatalf("series = %d, want 3", len(resultPoints(t, res)))
	}
}

func TestQueryMetricTenantIsolation(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil),
	)
	other := tenant.ID("other")
	src.addTenant(other, metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api"}, nil))
	if err := other.Validate(); err != nil {
		t.Skipf("tenant.ID validation: %v", err)
	}
	res := mustQuery(t, testEngine(), src, rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestQueryMetricCancellation(t *testing.T) {
	e := testEngine()
	rows := []plan.Row{metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil)}
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	src := newMetricSource(rows...)
	if _, err := e.Query(canceled, src, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("canceled request opened the Source: %d/%d", src.aggregateCalls, src.selectCalls)
	}

	// Cancellation while iterating buckets stops the query.
	midCtx, midCancel := context.WithCancel(t.Context())
	mid := newMetricSource(rows...)
	mid.onAggregate = func(int) { midCancel() }
	if _, err := e.Query(midCtx, mid, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-iteration err = %v, want context.Canceled", err)
	}
	if mid.aggregateCalls != 1 {
		t.Fatalf("mid-iteration aggregate calls = %d, want 1", mid.aggregateCalls)
	}

	// Same for the Select path.
	selCtx, selCancel := context.WithCancel(t.Context())
	sel := newMetricSource(rows...)
	sel.onSelect = func(int) { selCancel() }
	if _, err := e.Query(selCtx, sel, rangeParams(`count_over_time({app="api"} | level =~ ".*" [1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatalf("select cancellation err = %v, want context.Canceled", err)
	}
	if sel.selectCalls != 1 {
		t.Fatalf("select calls = %d, want 1", sel.selectCalls)
	}
}

func TestQueryMetricSourceErrors(t *testing.T) {
	sentinel := errors.New("storage exploded")
	e := testEngine()

	agg := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(70*time.Second), "b", map[string]string{"app": "api"}, nil),
	)
	agg.err = sentinel
	agg.aggErrAfter = 1
	aggParams := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(2*time.Minute), time.Minute)
	if _, err := e.Query(t.Context(), agg, aggParams); !errors.Is(err, sentinel) {
		t.Fatalf("aggregate error = %v, want wrapped sentinel", err)
	}

	sel := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api"}, nil),
	)
	sel.err = sentinel
	sel.selectErrAfter = 1
	if _, err := e.Query(t.Context(), sel, rangeParams(`count_over_time({app="api"} | level =~ ".*" [1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)); !errors.Is(err, sentinel) {
		t.Fatalf("select error = %v, want wrapped sentinel", err)
	}
}

func TestQueryMetricAliasedBuffers(t *testing.T) {
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "level": "warn"}, map[string]string{"trace": "2"}),
		metricRowAt(rangeBase.Add(10*time.Second), "cc", map[string]string{"app": "web"}, nil),
	)
	src.mutate = true
	e := testEngine()
	p := rangeParams(`count_over_time({app=~"api|web"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	res := mustQuery(t, e, src, p)
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "level", "info", "trace", "1").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
		lab("app", "api", "level", "warn", "trace", "2").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
		lab("app", "web").String():                                {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestQueryMetricDoesNotMutateRows(t *testing.T) {
	labels := logs.FromMap(map[string]string{"app": "api"})
	meta := logs.FromMap(map[string]string{"trace": "1"})
	row := plan.Row{TS: rangeBase.Add(10 * time.Second), Line: "x", Labels: labels, Meta: meta}
	src := newMetricSource(row)
	res := mustQuery(t, testEngine(), src, rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second))
	if !slices.Equal(labels, logs.FromMap(map[string]string{"app": "api"})) {
		t.Fatalf("source labels mutated: %v", labels)
	}
	if !slices.Equal(meta, logs.FromMap(map[string]string{"trace": "1"})) {
		t.Fatalf("source metadata mutated: %v", meta)
	}
	if len(resultPoints(t, res)) != 1 {
		t.Fatalf("result = %+v", res)
	}
}

func TestQueryMetricUnsupported(t *testing.T) {
	e := testEngine()
	// absent_over_time, unwrap, vector aggregations and A9 scalars are
	// implemented; the deferred families and scalar aggregation children are
	// still rejected.
	for _, q := range []string{
		`sum(1)`,
		`count_over_time({app="api"}[1m]) and count_over_time({app="api"}[1m])`,
		`label_replace(count_over_time({app="api"}[1m]), "dst", "repl", "src", ".*")`,
	} {
		t.Run(q, func(t *testing.T) {
			src := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, nil))
			_, err := e.Query(t.Context(), src, rangeParams(q, rangeBase, rangeBase.Add(time.Minute), time.Minute))
			if !errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
			if src.aggregateCalls != 0 || src.selectCalls != 0 {
				t.Fatalf("unsupported query touched the Source: %d aggregate / %d select", src.aggregateCalls, src.selectCalls)
			}
		})
	}
}

func TestQueryMetricNonPushableOrFilter(t *testing.T) {
	// A positive or-group cannot be pushed into plan.Selection (which ANDs),
	// so the query must fall back to Select and still agree with it.
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "error here", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "warn here", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(25*time.Second), "nothing", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	q := `count_over_time({app="api"} |= "error" or "warn" [1m])`
	p := instantParams(q, rangeBase.Add(30*time.Second))
	res := mustQuery(t, e, src, p)
	if src.aggregateCalls != 0 || src.selectCalls != 1 {
		t.Fatalf("or-filter calls = %d aggregate / %d select, want one select", src.aggregateCalls, src.selectCalls)
	}
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(30 * time.Second), V: 2}},
	})
}

func TestQueryMetricStepOverflow(t *testing.T) {
	// A span wider than time.Duration is an explicit client error.
	src := newMetricSource()
	huge := plan.Params{
		Tenant: tenant.Default,
		Query:  `count_over_time({app="api"}[1m])`,
		Start:  time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC),
		End:    time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		Step:   time.Nanosecond,
	}
	if _, err := testEngine().Query(t.Context(), src, huge); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("huge span err = %v, want ErrBadQuery", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("huge-span query opened the Source")
	}

	// A span within time.Duration but with too many evaluation points is a
	// limit error, also before the Source.
	many := huge
	many.Start = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	many.End = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	many.Step = time.Millisecond
	if _, err := testEngine().Query(t.Context(), src, many); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("many points err = %v, want ErrLimit", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("over-limit query opened the Source")
	}
}

func TestQueryMetricProjectionMatchesSelect(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "bb", map[string]string{"app": "api", "level": "warn"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(30*time.Second), "ccc", map[string]string{"app": "web", "level": "info"}, nil),
	}
	queries := []string{
		`count_over_time({app=~"api|web"} | drop level [1m])`,
		`count_over_time({app=~"api|web"} | keep level [1m])`,
		`count_over_time({app=~"api|web"} | drop trace [1m])`,
		`count_over_time({app=~"api|web"} | drop trace=~"1" [1m])`,
		`count_over_time({app=~"api|web"} | keep level, trace [1m])`,
		`bytes_over_time({app=~"api|web"} | drop level [1m])`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			fastSrc := newMetricSource(rows...)
			refSrc := newMetricSource(rows...)
			p := rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
			fast := mustQuery(t, testEngine(), fastSrc, p)
			if fastSrc.aggregateCalls != 1 || fastSrc.selectCalls != 0 {
				t.Fatalf("projection calls = %d aggregate / %d select, want one aggregate", fastSrc.aggregateCalls, fastSrc.selectCalls)
			}
			ref := evalRangeWith(t, testEngine(), refSrc, q, p, rangeOptions{})
			if refSrc.selectCalls != 1 {
				t.Fatalf("reference Select calls = %d, want 1", refSrc.selectCalls)
			}
			assertSameResult(t, fast, ref)
		})
	}
}

func TestQueryMetricDropErrorClearsPipelineState(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "not json", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"x":"1"}`, map[string]string{"app": "api"}, nil),
	}
	e := testEngine()
	p := func(q string) plan.Params {
		return rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	}

	// drop __error__ keeps the failed row and clears the state, so the metric
	// query does not fail: the failed row has no parsed labels and the good
	// row carries x=1, so two series exist.
	src := newMetricSource(rows...)
	res := mustQuery(t, e, src, p(`count_over_time({app="api"} | json | drop __error__[1m])`))
	if src.aggregateCalls != 0 || src.selectCalls != 1 {
		t.Fatalf("drop __error__ after a parser must use Select: %d/%d", src.aggregateCalls, src.selectCalls)
	}
	points := resultPoints(t, res)
	if len(points) != 2 {
		t.Fatalf("series = %v, want the failed row and the parsed row separately", points)
	}
	if got := points[lab("app", "api").String()]; len(got) != 1 || got[0].V != 1 {
		t.Fatalf("failed-row series = %v, want count 1", got)
	}

	// __error__="" instead discards the failed row.
	filtered := newMetricSource(rows...)
	fres := mustQuery(t, e, filtered, p(`count_over_time({app="api"} | json | __error__="" [1m])`))
	assertResult(t, fres, map[string][]plan.Point{
		lab("app", "api", "x", "1").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestQueryMetricGapRowsCreateNoSeries(t *testing.T) {
	// A step larger than the range leaves the row in no window. It must not
	// create a series (nor trip the series limit).
	row := metricRowAt(rangeBase.Add(30*time.Second), "a", map[string]string{"app": "api"}, nil)
	e := testEngine()
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(2*time.Minute), 2*time.Minute)
	res := mustQuery(t, e, newMetricSource(row), p)
	if got := resultPoints(t, res); len(got) != 0 {
		t.Fatalf("gap row produced series %v", got)
	}

	strict := New(config.LogQL{MaxEntries: 100, MaxSeries: 1})
	if _, err := strict.Query(t.Context(), newMetricSource(row), p); err != nil {
		t.Fatalf("gap row tripped the series limit: %v", err)
	}
}

func TestQueryMetricLineFormatBytes(t *testing.T) {
	// bytes_over_time counts the line after the whole pipeline, so
	// line_format changes the byte total. It also keeps the query on the
	// Select path because line_format can change what a filter sees.
	src := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), "long line", map[string]string{"app": "api"}, nil))
	e := testEngine()
	q := `bytes_over_time({app="api"} | line_format "xx" [1m])`
	res := mustQuery(t, e, src, instantParams(q, rangeBase.Add(30*time.Second)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(30 * time.Second), V: 2}},
	})
	if src.aggregateCalls != 0 || src.selectCalls != 1 {
		t.Fatalf("line_format calls = %d aggregate / %d select, want one select", src.aggregateCalls, src.selectCalls)
	}
}

func TestQueryMetricMetadataBaseCollision(t *testing.T) {
	// Both the metadata and the parsed field collide with the base stream
	// label level: metadata becomes level_extracted, then the parser replaces
	// that value, while the stream label survives. The identity must reflect
	// the shared entry categories exactly.
	src := newMetricSource(
		plan.Row{
			TS:     rangeBase.Add(10 * time.Second),
			Line:   `{"level":"parsed"}`,
			Labels: logs.FromMap(map[string]string{"app": "api", "level": "stream"}),
			Meta:   logs.FromMap(map[string]string{"level": "meta"}),
		},
	)
	e := testEngine()
	res := mustQuery(t, e, src, instantParams(`count_over_time({app="api"} | json [1m])`, rangeBase.Add(30*time.Second)))
	want := logs.FromMap(map[string]string{"app": "api", "level": "stream", "level_extracted": "parsed"})
	assertResult(t, res, map[string][]plan.Point{
		want.String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestEvalRangeSeriesChildSkipsSeriesLimit(t *testing.T) {
	// A8's enclosing vector aggregation owns MaxSeries: a child with more raw
	// metadata series than the limit must still evaluate, so the parent can
	// project it down to few series.
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api"}, map[string]string{"trace": "2"}),
		metricRowAt(rangeBase.Add(30*time.Second), "c", map[string]string{"app": "api"}, map[string]string{"trace": "3"}),
	}
	e := New(config.LogQL{MaxEntries: 100, MaxSeries: 1})
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	a := parseRangeAgg(t, `count_over_time({app="api"}[1m])`)

	series, err := e.evalRangeSeries(t.Context(), newMetricSource(rows...), a, p, rangeOptions{aggregate: true, child: true})
	if err != nil {
		t.Fatalf("child evaluation failed: %v", err)
	}
	if len(series) != 3 {
		t.Fatalf("child series = %d, want 3 (limit belongs to the parent)", len(series))
	}

	// The same evaluation at the top level enforces the limit.
	if _, err := e.Query(t.Context(), newMetricSource(rows...), p); !errors.Is(err, plan.ErrLimit) {
		t.Fatalf("top-level err = %v, want ErrLimit", err)
	}
}

func TestQueryMetricSubMicrosecondStartAndOffset(t *testing.T) {
	start := rangeBase.Add(500 * time.Nanosecond)
	src := newMetricSource(
		metricRowAt(rangeBase.Add(-time.Minute), "a", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase, "b", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), "c", map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	q := `count_over_time({app="api"}[1m] offset 1500ns)`
	p := rangeParams(q, start, start.Add(time.Minute), time.Minute)
	// A lower-level exact-grid test (Engine.Query would align the raw start).
	res := evalRangeWith(t, e, src, q, p, rangeOptions{aggregate: true})
	// Range timestamps truncate the data time and the offset separately
	// (range_vector.go At): (start-offset)/1ms + offset/1ms. With a 1500ns
	// offset the data time falls one millisecond before the request time.
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {
			{T: rangeBase.Add(-time.Millisecond), V: 1},
			{T: rangeBase.Add(time.Minute - time.Millisecond), V: 2},
		},
	})
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
	}
	assertSameResult(t, res, evalRangeWith(t, e, src, q, p, rangeOptions{}))
}

func TestEvalRangeSeriesGroupingSeam(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "1"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"app": "api", "level": "info"}, map[string]string{"trace": "2"}),
		metricRowAt(rangeBase.Add(25*time.Second), "c", map[string]string{"app": "api", "level": "warn"}, map[string]string{"trace": "3"}),
	}
	e := testEngine()
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	a := parseRangeAgg(t, `count_over_time({app="api"}[1m])`)
	series, err := e.evalRangeSeries(t.Context(), newMetricSource(rows...), a, p, rangeOptions{aggregate: true, child: true, group: []string{"level"}})
	if err != nil {
		t.Fatalf("evalRangeSeries: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("grouped series = %d, want 2", len(series))
	}
	got := make(map[string]float64, len(series))
	for _, s := range series {
		got[s.labels.String()] = s.points[0].V
	}
	want := map[string]float64{
		lab("level", "info").String(): 2,
		lab("level", "warn").String(): 1,
	}
	if len(got) != len(want) {
		t.Fatalf("grouped = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("grouped[%s] = %v, want %v (all %v)", k, got[k], v, got)
		}
	}
}

func TestEvalRangeSeriesGrafanaHistogramSeam(t *testing.T) {
	// The captured Grafana form is
	//   sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))
	// with step 1m. detected_level can be structured metadata, which the
	// metadata-aware buckets keep, so the A8 grouping seam must see it.
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), "a", map[string]string{"service_name": "api", "level": "info"},
			map[string]string{"trace": "1", "detected_level": "error"}),
		metricRowAt(rangeBase.Add(20*time.Second), "b", map[string]string{"service_name": "api", "level": "info"}, nil),
	}
	e := testEngine()
	q := `count_over_time({service_name="api"} | drop __error__[1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(time.Minute), time.Minute)
	a := parseRangeAgg(t, q)
	src := newMetricSource(rows...)
	series, err := e.evalRangeSeries(t.Context(), src, a, p, rangeOptions{aggregate: true, child: true, group: []string{"level", "detected_level"}})
	if err != nil {
		t.Fatalf("evalRangeSeries: %v", err)
	}
	if src.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", src.aggregateCalls)
	}
	got := make(map[string]float64, len(series))
	for _, s := range series {
		if len(s.points) != 1 || !s.points[0].T.Equal(rangeBase.Add(time.Minute)) {
			t.Fatalf("series %s points = %v, want one point at the step", s.labels, s.points)
		}
		got[s.labels.String()] = s.points[0].V
	}
	want := map[string]float64{
		lab("level", "info", "detected_level", "error").String(): 1,
		lab("level", "info").String():                            1,
	}
	if len(got) != len(want) {
		t.Fatalf("grouped series = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("grouped[%s] = %v, want %v (all %v)", k, got[k], v, got)
		}
	}
}

func TestQueryMetricMetadataCollisionMatchesSelect(t *testing.T) {
	rows := []plan.Row{
		plan.Row{
			TS:     rangeBase.Add(10 * time.Second),
			Line:   "a",
			Labels: logs.FromMap(map[string]string{"app": "api", "level": "stream"}),
			Meta:   logs.FromMap(map[string]string{"level": "meta"}),
		},
	}
	e := testEngine()
	q := `count_over_time({app="api"}[1m])`
	p := rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	fastSrc := newMetricSource(rows...)
	fast := mustQuery(t, e, fastSrc, p)
	if fastSrc.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", fastSrc.aggregateCalls)
	}
	assertResult(t, fast, map[string][]plan.Point{
		lab("app", "api", "level", "stream", "level_extracted", "meta").String(): {
			{T: rangeBase.Add(30 * time.Second), V: 1},
		},
	})
	assertSameResult(t, fast, evalRangeWith(t, e, newMetricSource(rows...), q, p, rangeOptions{}))
}

func TestQueryMetricMetadataErrorState(t *testing.T) {
	// Structured metadata can itself carry the builder error label. Loki's
	// sample extractor still fails the metric, and only an explicit drop
	// clears the state while keeping the row.
	rows := []plan.Row{
		plan.Row{
			TS:     rangeBase.Add(10 * time.Second),
			Line:   "a",
			Labels: logs.FromMap(map[string]string{"app": "api"}),
			Meta:   logs.FromMap(map[string]string{"__error__": "metadata failure"}),
		},
	}
	e := testEngine()
	p := func(q string) plan.Params {
		return rangeParams(q, rangeBase, rangeBase.Add(30*time.Second), 30*time.Second)
	}
	if _, err := e.Query(t.Context(), newMetricSource(rows...), p(`count_over_time({app="api"}[1m])`)); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("metadata error = %v, want ErrBadQuery", err)
	}

	src := newMetricSource(rows...)
	res := mustQuery(t, e, src, p(`count_over_time({app="api"} | drop __error__[1m])`))
	if src.aggregateCalls != 1 {
		t.Fatalf("drop __error__ aggregate calls = %d, want 1", src.aggregateCalls)
	}
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(30 * time.Second), V: 1}},
	})
}

func TestQueryMetricMetadataErrorInGap(t *testing.T) {
	// count[1s] with a 2s step evaluates (base-1s,base], (base+1s,base+2s]
	// and (base+3s,base+4s]. The metadata __error__ row at base+500ms is in a
	// gap: the union Aggregate sees it, but no evaluation covers it, so it
	// must not fail the query. Select already ignores it; Aggregate must too.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []plan.Row{
		metricRowAt(base, "ok", map[string]string{"app": "api"}, nil),
		metricRowAt(base.Add(500*time.Millisecond), "gap", map[string]string{"app": "api"}, map[string]string{"__error__": "gap_only"}),
	}
	p := rangeParams(`count_over_time({app="api"}[1s])`, base, base.Add(4*time.Second), 2*time.Second)
	e := testEngine()

	refSrc := newMetricSource(rows...)
	ref := evalRangeWith(t, e, refSrc, `count_over_time({app="api"}[1s])`, p, rangeOptions{})
	if refSrc.selectCalls != 1 {
		t.Fatalf("reference Select calls = %d, want 1", refSrc.selectCalls)
	}
	want := map[string][]plan.Point{lab("app", "api").String(): {{T: base, V: 1}}}
	assertResult(t, ref, want)

	fastSrc := newMetricSource(rows...)
	fast := mustQuery(t, e, fastSrc, p)
	if fastSrc.aggregateCalls != 1 {
		t.Fatalf("aggregate calls = %d, want 1", fastSrc.aggregateCalls)
	}
	assertSameResult(t, fast, ref)
}

func TestMetricEvalTimesRejectsSpanBeyondDuration(t *testing.T) {
	const year = 365 * 24 * time.Hour
	base := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	p := plan.Params{
		Start: base,
		End:   base.AddDate(400, 0, 0),
		Step:  200 * year,
	}
	_, err := metricEvalTimes(p)
	if err == nil {
		t.Fatalf("accepted a span wider than time.Duration, silently losing evaluations")
	}
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
}

func TestQueryMetricUnionSpanOverflowRejected(t *testing.T) {
	// A 250-year request with a 100-year range spans 350 years for the
	// Aggregate grid, beyond time.Duration. Reject it before any saturated
	// bucket arithmetic or Source call.
	const year = 365 * 24 * time.Hour
	base := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	src := newMetricSource()
	p := rangeParams(`count_over_time({app="api"}[100y])`, base, base.Add(250*year), year)
	_, err := testEngine().Query(t.Context(), src, p)
	if !errors.Is(err, plan.ErrLimit) && !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want an explicit client error", err)
	}
	if src.aggregateCalls != 0 || src.selectCalls != 0 {
		t.Fatalf("oversized span opened the Source: %d aggregate / %d select", src.aggregateCalls, src.selectCalls)
	}
}

func TestNewMetricGridUsesStepForOneEvaluation(t *testing.T) {
	// Start == End with a positive Step is a range query with one evaluation;
	// its width is gcd(range, step), not the whole range (that is reserved for
	// the Step 0 instant query).
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	windows := metricWindows([]time.Time{base}, 0, time.Minute)
	grid, single, err := newMetricGrid(windows, time.Minute, 15*time.Second)
	if err != nil {
		t.Fatalf("newMetricGrid: %v", err)
	}
	if grid.width != 15*time.Second {
		t.Fatalf("width = %s, want gcd(1m,15s)=15s", grid.width)
	}
	if !single || windows[0].buckets != 4 {
		t.Fatalf("single = %v buckets = %d, want true and 4", single, windows[0].buckets)
	}

	src := newMetricSource(metricRowAt(base, "a", map[string]string{"app": "api"}, nil))
	p := rangeParams(`count_over_time({app="api"}[1m])`, base, base, 15*time.Second)
	res := mustQuery(t, testEngine(), src, p)
	if len(src.aggs) != 1 || src.aggs[0].Width != 15*time.Second {
		t.Fatalf("aggregate width = %v, want 15s", src.aggs[0].Width)
	}
	assertResult(t, res, map[string][]plan.Point{lab("app", "api").String(): {{T: base, V: 1}}})
}
