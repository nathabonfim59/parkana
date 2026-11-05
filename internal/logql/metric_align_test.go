package logql

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// TestNormalizeMetricParams pins Loki's frontend grid: step truncated to whole
// milliseconds, start rounded toward zero, end rounded away from zero on the
// signed remainder grid.
func TestNormalizeMetricParams(t *testing.T) {
	// Positive, non-whole-millisecond start and end with a 1.5ms step.
	p := plan.Params{
		Start: time.Unix(100, 1234000).UTC(),
		End:   time.Unix(100, 4567000).UTC(),
		Step:  1500 * time.Microsecond,
	}
	got, err := normalizeMetricParams(p)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if got.Step != time.Millisecond {
		t.Fatalf("step = %s, want 1ms", got.Step)
	}
	if want := time.Unix(100, 1000000).UTC(); !got.Start.Equal(want) {
		t.Fatalf("start = %s, want %s", got.Start, want)
	}
	if want := time.Unix(100, 5000000).UTC(); !got.End.Equal(want) {
		t.Fatalf("end = %s, want %s", got.End, want)
	}

	// Negative timestamps use Go's signed remainder: start is start-start%step
	// (toward zero) and end is end+(step-end%step) when the remainder is
	// non-zero, so -70.25s..-10.25s becomes -60s..+60s on a one-minute grid.
	p = plan.Params{
		Start: time.Unix(-71, 750000000).UTC(),
		End:   time.Unix(-11, 750000000).UTC(),
		Step:  time.Minute,
	}
	got, err = normalizeMetricParams(p)
	if err != nil {
		t.Fatalf("negative normalize: %v", err)
	}
	if want := time.Unix(-60, 0).UTC(); !got.Start.Equal(want) {
		t.Fatalf("negative start = %s, want %s", got.Start, want)
	}
	if want := time.Unix(60, 0).UTC(); !got.End.Equal(want) {
		t.Fatalf("negative end = %s, want %s", got.End, want)
	}

	// Instants and already-aligned ranges are untouched.
	instant := plan.Params{Start: time.Unix(1, 234000000).UTC(), End: time.Unix(1, 234000000).UTC()}
	got, err = normalizeMetricParams(instant)
	if err != nil || !got.Start.Equal(instant.Start) || got.Step != 0 {
		t.Fatalf("instant changed: %+v, %v", got, err)
	}
	aligned := plan.Params{Start: time.Unix(120, 0).UTC(), End: time.Unix(180, 0).UTC(), Step: time.Minute}
	got, err = normalizeMetricParams(aligned)
	if err != nil || !got.Start.Equal(aligned.Start) || !got.End.Equal(aligned.End) {
		t.Fatalf("aligned range changed: %+v, %v", got, err)
	}
}

func TestNormalizeMetricParamsRejects(t *testing.T) {
	cases := []struct {
		name string
		p    plan.Params
	}{
		{
			name: "sub-millisecond positive step",
			p:    plan.Params{Start: time.Unix(0, 0), End: time.Unix(1, 0), Step: time.Microsecond},
		},
		{
			name: "start outside nanosecond range",
			p:    plan.Params{Start: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(9999, 1, 1, 0, 1, 0, 0, time.UTC), Step: time.Minute},
		},
		{
			name: "aligned end overflows",
			p:    plan.Params{Start: time.Unix(0, math.MaxInt64-int64(time.Hour)), End: time.Unix(0, math.MaxInt64-1000), Step: time.Millisecond},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeMetricParams(tc.p); !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("err = %v, want ErrBadQuery", err)
			}
		})
	}
}

// TestNormalizedHistogramSingleAggregate pins the Grafana histogram target:
// after normalization the whole aligned range is one Aggregate call.
func TestNormalizedHistogramSingleAggregate(t *testing.T) {
	base := time.Unix(1793785500, 0).UTC()
	src := newMetricSource(
		metricRowAt(base.Add(-time.Second), "before", map[string]string{"app": "api", "level": "info"}, nil),
		metricRowAt(base.Add(59*time.Second), "after", map[string]string{"app": "api", "level": "info"}, nil),
	)
	q := `sum by (level, detected_level) (count_over_time({app="api"} | drop __error__[1m]))`
	p := rangeParams(q, base.Add(10*time.Second+123456789*time.Nanosecond), base.Add(50*time.Second+987654321*time.Nanosecond), time.Minute)
	res := mustQuery(t, testEngine(), src, p)
	if src.aggregateCalls != 1 || src.selectCalls != 0 {
		t.Fatalf("calls = %d Aggregate / %d Select, want exactly one Aggregate", src.aggregateCalls, src.selectCalls)
	}
	assertResult(t, res, map[string][]plan.Point{
		lab("level", "info").String(): {{T: base, V: 1}, {T: base.Add(time.Minute), V: 1}},
	})
}

// TestNormalizeMetricParamsBeforeSource proves the rejection happens before the
// evaluator opens the Source.
func TestNormalizeMetricParamsBeforeSource(t *testing.T) {
	src := newMetricSource()
	p := rangeParams(`count_over_time({app="api"}[1m])`, rangeBase, rangeBase.Add(time.Millisecond), time.Microsecond)
	if _, err := testEngine().Query(t.Context(), src, p); !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
	if src.selectCalls != 0 || src.aggregateCalls != 0 {
		t.Fatalf("Source called on invalid step: %d/%d", src.selectCalls, src.aggregateCalls)
	}
}
