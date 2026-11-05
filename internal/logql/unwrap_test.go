package logql

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

func TestConvertUnwrapValue(t *testing.T) {
	cases := []struct {
		name    string
		fn      UnwrapFunc
		raw     string
		want    float64
		wantErr bool
	}{
		{"label integer", UnwrapLabel, "42", 42, false},
		{"label decimal", UnwrapLabel, "-3.5", -3.5, false},
		{"label unparsable", UnwrapLabel, "abc", 0, true},
		{"duration hours", UnwrapDuration, "1h30m", 5400, false},
		{"duration milliseconds", UnwrapDuration, "1500ms", 1.5, false},
		{"duration_seconds is seconds too", UnwrapDurationSeconds, "2m", 120, false},
		// time.ParseDuration has no day unit, unlike a LogQL model duration
		// literal on the right-hand side of a label filter.
		{"duration Go units only", UnwrapDuration, "1d", 0, true},
		{"bytes SI", UnwrapBytes, "1MB", 1e6, false},
		{"bytes IEC", UnwrapBytes, "1MiB", 1 << 20, false},
		{"bytes bare", UnwrapBytes, "512", 512, false},
		{"bytes fractional truncates", UnwrapBytes, "1.5KiB", 1536, false},
		{"bytes unknown unit", UnwrapBytes, "12wat", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertUnwrapValue(tc.fn, tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("convertUnwrapValue(%d, %q) = %v, want error", tc.fn, tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("convertUnwrapValue(%d, %q): %v", tc.fn, tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("convertUnwrapValue(%d, %q) = %v, want %v", tc.fn, tc.raw, got, tc.want)
			}
		})
	}
}

func TestUnwrapGroupLabels(t *testing.T) {
	e := &logEntry{
		labels:         logs.FromMap(map[string]string{"app": "api", "empty": "", "level": "info", "latency": "9"}),
		meta:           logs.FromMap(map[string]string{"trace": "1"}),
		parsed:         logs.FromMap(map[string]string{"p": "2"}),
		builderMutated: true,
	}
	every := lab("app", "api", "empty", "", "latency", "9", "level", "info", "p", "2", "trace", "1")
	withoutValue := lab("app", "api", "empty", "", "level", "info", "p", "2", "trace", "1")
	cases := []struct {
		name   string
		group  *Grouping
		unwrap string
		want   logs.Labels
	}{
		{"no grouping removes the value label", nil, "latency", withoutValue},
		// Explicit by() is Grouping.Singleton(): noLabels wins before the
		// group lists, so the extractor identity is empty.
		{"explicit by empty is the noLabels singleton", &Grouping{}, "latency", nil},
		{"without removes names and the value label", &Grouping{Without: true, Names: []string{"level"}}, "latency", lab("app", "api", "empty", "", "p", "2", "trace", "1")},
		{"by keeps only requested names", &Grouping{Names: []string{"level", "trace", "absent"}}, "latency", lab("level", "info", "trace", "1")},
		// withResult resolves a name through the base labels, where an empty
		// value counts as missing; metadata and parsed labels keep "".
		{"by drops an empty stream value", &Grouping{Names: []string{"empty"}}, "latency", nil},
		{"by keeps a requested value label", &Grouping{Names: []string{"latency"}}, "latency", lab("latency", "9")},
		{"unknown unwrap label is harmless", nil, "missing", every},
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

func TestUnwrapMissingFieldDropsWithoutError(t *testing.T) {
	// A row with a parser error but no unwrap field produces no sample and no
	// metric error (Loki checks the field before letting a surviving error
	// fail the query).
	src := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), "not json", map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"other":"1"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(30*time.Second), `{"latency":"4"}`, map[string]string{"app": "api"}, nil),
	)
	e := testEngine()
	q := `sum_over_time({app="api"} | json | unwrap latency [1m])`
	res := mustQuery(t, e, src, instantParams(q, rangeBase.Add(time.Minute)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 4}},
	})
	if src.selectCalls != 1 || src.aggregateCalls != 0 {
		t.Fatalf("calls = %d select / %d aggregate, want one select", src.selectCalls, src.aggregateCalls)
	}
}

func TestUnwrapFieldLookupUsesAllCategories(t *testing.T) {
	// A metadata field with the unwrap name wins over the parsed collision:
	// the JSON parser renames its extraction to latency_extracted
	// (pkg/logql/log/parser.go:342-343 checks BaseHas and the structured
	// metadata category), so the value comes from metadata and that label
	// stays in the identity while the unwrap label is removed.
	src := newMetricSource(
		plan.Row{
			TS:     rangeBase.Add(10 * time.Second),
			Line:   `{"latency":"5"}`,
			Labels: logs.FromMap(map[string]string{"app": "api"}),
			Meta:   logs.FromMap(map[string]string{"latency": "7", "trace": "1"}),
		},
	)
	res := mustQuery(t, testEngine(), src, instantParams(`sum_over_time({app="api"} | json | unwrap latency [1m])`, rangeBase.Add(time.Minute)))
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api", "latency_extracted", "5", "trace", "1").String(): {{T: rangeBase.Add(time.Minute), V: 7}},
	})
}

func TestUnwrapConversionErrorFailsQuery(t *testing.T) {
	src := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), `{"latency":"oops"}`, map[string]string{"app": "api"}, nil))
	_, err := testEngine().Query(t.Context(), src, instantParams(`sum_over_time({app="api"} | json | unwrap latency [1m])`, rangeBase.Add(time.Minute)))
	if err == nil {
		t.Fatal("conversion failure did not fail the query")
	}
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), sampleExtractionErr) {
		t.Fatalf("err = %v, want ErrBadQuery with %s", err, sampleExtractionErr)
	}
}

func TestUnwrapPostFilters(t *testing.T) {
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), `{"latency":"5"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"latency":"oops"}`, map[string]string{"app": "api"}, nil),
	}
	p := func(q string) plan.Params {
		return instantParams(q, rangeBase.Add(time.Minute))
	}
	e := testEngine()

	// __error__="" after unwrap drops the failed conversion before the
	// metric error check sees it.
	assertResult(t, mustQuery(t, e, newMetricSource(rows...), p(`sum_over_time({app="api"} | json | unwrap latency | __error__="" [1m])`)),
		map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 5}}})
	// __error__!="" keeps only the error row, whose state then fails the query.
	_, err := e.Query(t.Context(), newMetricSource(rows...), p(`sum_over_time({app="api"} | json | unwrap latency | __error__!="" [1m])`))
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), sampleExtractionErr) {
		t.Fatalf("err = %v, want the surviving SampleExtractionErr", err)
	}
	// A typed post filter compares the *label value*, not the converted
	// sample: 2 > 3 is false and the row is dropped. (A failed conversion
	// stays in the window, because an unparsable filter value keeps the row.)
	valid := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), `{"latency":"5"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"latency":"2"}`, map[string]string{"app": "api"}, nil),
	}
	typed := mustQuery(t, e, newMetricSource(valid...), p(`sum_over_time({app="api"} | json | unwrap latency | latency > 3 [1m])`))
	assertResult(t, typed, map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 5}}})

	// Filters run in order: the first clears the error, the second drops by
	// value.
	valid = append(valid, metricRowAt(rangeBase.Add(30*time.Second), `{"latency":"oops"}`, map[string]string{"app": "api"}, nil))
	assertResult(t, mustQuery(t, e, newMetricSource(valid...), p(`sum_over_time({app="api"} | json | unwrap latency | __error__="" | latency > 3 [1m])`)),
		map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 5}}})
}

func TestUnwrapConversionOverwritesEarlierParserError(t *testing.T) {
	// A typed label filter records LabelFilterErr; a successful conversion
	// leaves it in place, while a failed conversion replaces it with
	// SampleExtractionErr (LabelsBuilder.SetErr is unconditional).
	e := testEngine()
	ok := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), `{"n":"x","latency":"2"}`, map[string]string{"app": "api"}, nil))
	_, err := e.Query(t.Context(), ok, instantParams(`sum_over_time({app="api"} | json | n > 1 | unwrap latency [1m])`, rangeBase.Add(time.Minute)))
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), labelFilterErr) {
		t.Fatalf("err = %v, want the earlier LabelFilterErr", err)
	}
	if strings.Contains(err.Error(), sampleExtractionErr) {
		t.Fatalf("successful conversion replaced the earlier error: %v", err)
	}

	bad := newMetricSource(metricRowAt(rangeBase.Add(10*time.Second), `{"n":"x","latency":"oops"}`, map[string]string{"app": "api"}, nil))
	_, err = e.Query(t.Context(), bad, instantParams(`sum_over_time({app="api"} | json | n > 1 | unwrap latency [1m])`, rangeBase.Add(time.Minute)))
	if !errors.Is(err, plan.ErrBadQuery) || !strings.Contains(err.Error(), sampleExtractionErr) {
		t.Fatalf("err = %v, want the conversion to overwrite with %s", err, sampleExtractionErr)
	}
}

func TestUnwrapDurationAndBytesThroughQuery(t *testing.T) {
	e := testEngine()
	durations := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), `{"t":"1h"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"t":"2h"}`, map[string]string{"app": "api"}, nil),
	)
	// duration and duration_seconds both report seconds.
	assertResult(t, mustQuery(t, e, durations, instantParams(`avg_over_time({app="api"} | json | unwrap duration(t) [1m])`, rangeBase.Add(time.Minute))),
		map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 5400}}})
	assertResult(t, mustQuery(t, e, newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), `{"t":"1h"}`, map[string]string{"app": "api"}, nil),
	), instantParams(`sum_over_time({app="api"} | json | unwrap duration_seconds(t) [1m])`, rangeBase.Add(time.Minute))),
		map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 3600}}})

	sizes := newMetricSource(
		metricRowAt(rangeBase.Add(10*time.Second), `{"sz":"1MB"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), "éé", map[string]string{"app": "api"}, nil), // not parsed: no sz
		metricRowAt(rangeBase.Add(30*time.Second), `{"sz":"1KiB"}`, map[string]string{"app": "api"}, nil),
	)
	assertResult(t, mustQuery(t, e, sizes, instantParams(`sum_over_time({app="api"} | json | unwrap bytes(sz) [1m])`, rangeBase.Add(time.Minute))),
		map[string][]plan.Point{lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 1e6 + 1024}}})
}

func TestUnwrapFilterDurationUsesModelRHS(t *testing.T) {
	// The filter's right-hand side is a LogQL model duration (1d = 24h), while
	// the field value is parsed with time.ParseDuration (25h works, 1d would
	// not).
	rows := []plan.Row{
		metricRowAt(rangeBase.Add(10*time.Second), `{"x":"25h"}`, map[string]string{"app": "api"}, nil),
		metricRowAt(rangeBase.Add(20*time.Second), `{"x":"23h"}`, map[string]string{"app": "api"}, nil),
	}
	src := newMetricSource(rows...)
	res := mustQuery(t, testEngine(), src, instantParams(`sum_over_time({app="api"} | json | x > 1d | unwrap duration(x) [1m])`, rangeBase.Add(time.Minute)))
	// x is the unwrap field, so the surviving row's value is 25h in seconds.
	assertResult(t, res, map[string][]plan.Point{
		lab("app", "api").String(): {{T: rangeBase.Add(time.Minute), V: 25 * 3600}},
	})
}
