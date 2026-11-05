package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fakeParseSelector is the tests' stand-in for plan.Engine.ParseSelector: it
// accepts comma-separated equality matchers inside braces and rejects
// everything else, including the bare "{}" and the empty string, with
// plan.ErrBadQuery the way the real engine does.
func fakeParseSelector(s string) ([]plan.Matcher, error) {
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("fake selector %q: %w", s, plan.ErrBadQuery)
	}
	body := strings.TrimSpace(s[1 : len(s)-1])
	if body == "" {
		return nil, fmt.Errorf("fake selector %q: %w: empty selector", s, plan.ErrBadQuery)
	}
	var ms []plan.Matcher
	for _, part := range strings.Split(body, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || len(v) < 2 || !strings.HasPrefix(v, `"`) || !strings.HasSuffix(v, `"`) {
			return nil, fmt.Errorf("fake selector %q: %w: bad matcher %q", s, plan.ErrBadQuery, part)
		}
		m, err := plan.NewMatcher(plan.MatchEqual, strings.TrimSpace(k), v[1:len(v)-1])
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, nil
}

// fakeEngine records every selector handed to ParseSelector and parses it
// with fakeParseSelector. Query is never called by the metadata handlers.
type fakeEngine struct {
	parsed *[]string
}

func (e *fakeEngine) Query(context.Context, plan.Source, plan.Params) (plan.Result, error) {
	return plan.Result{}, fmt.Errorf("fake engine: %w", plan.ErrUnsupported)
}

func (e *fakeEngine) ParseSelector(s string) ([]plan.Matcher, error) {
	*e.parsed = append(*e.parsed, s)
	return fakeParseSelector(s)
}

// metaValuesQuery records one LabelValues call.
type metaValuesQuery struct {
	q    metaQuery
	name string
}

// fakeMetaReader answers the reader interface from canned fields and records
// every call. With hang set, each call waits for ctx to end and returns its
// error, which exercises the deadline and client-cancellation paths.
type fakeMetaReader struct {
	names  []string
	values []string
	series []logs.Labels
	stats  metaStats
	volume []metaVolumeEntry
	err    error
	hang   bool

	gotNames  []metaQuery
	gotValues []metaValuesQuery
	gotSeries []metaQuery
	gotStats  []metaQuery
	gotVolume []metaVolumeRequest
}

func (f *fakeMetaReader) LabelNames(ctx context.Context, q metaQuery) ([]string, error) {
	f.gotNames = append(f.gotNames, q)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.names, f.err
}

func (f *fakeMetaReader) LabelValues(ctx context.Context, q metaQuery, name string) ([]string, error) {
	f.gotValues = append(f.gotValues, metaValuesQuery{q: q, name: name})
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.values, f.err
}

func (f *fakeMetaReader) Series(ctx context.Context, q metaQuery) ([]logs.Labels, error) {
	f.gotSeries = append(f.gotSeries, q)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.series, f.err
}

func (f *fakeMetaReader) Stats(ctx context.Context, q metaQuery) (metaStats, error) {
	f.gotStats = append(f.gotStats, q)
	if f.hang {
		<-ctx.Done()
		return metaStats{}, ctx.Err()
	}
	return f.stats, f.err
}

func (f *fakeMetaReader) Volume(ctx context.Context, req metaVolumeRequest) ([]metaVolumeEntry, error) {
	f.gotVolume = append(f.gotVolume, req)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.volume, f.err
}

// matchersEqual compares matcher groups structurally, ignoring the compiled
// regexp that plan.Matcher carries.
func matchersEqual(a, b []plan.Matcher) bool {
	return slices.EqualFunc(a, b, func(x, y plan.Matcher) bool {
		return x.Type == y.Type && x.Name == y.Name && x.Value == y.Value
	})
}

func metaQueryEqual(a, b metaQuery) bool {
	if a.Tenant != b.Tenant || !a.Start.Equal(b.Start) || !a.End.Equal(b.End) || len(a.Groups) != len(b.Groups) {
		return false
	}
	for i := range a.Groups {
		if !matchersEqual(a.Groups[i], b.Groups[i]) {
			return false
		}
	}
	return true
}

func TestIsEmptySelector(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{in: "{}", want: true},
		{in: "{ }", want: true},
		{in: "{  }", want: true},
		{in: `{app="api"}`, want: false},
		{in: "", want: false},
		{in: "{{}}", want: false},
		{in: `{ }{}`, want: false},
	} {
		if got := isEmptySelector(tc.in); got != tc.want {
			t.Errorf("isEmptySelector(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSortedUniqueStrings(t *testing.T) {
	got := sortedUniqueStrings([]string{`{b="2"}`, `{a="1"}`, `{b="2"}`, `{a="1"}`})
	want := []string{`{a="1"}`, `{b="2"}`}
	if !slices.Equal(got, want) {
		t.Errorf("sortedUniqueStrings = %v, want %v", got, want)
	}
}

func TestParseMetaQuerySelector(t *testing.T) {
	t.Run("empty query selects everything without parsing", func(t *testing.T) {
		var parsed []string
		groups, err := parseMetaQuerySelector((&fakeEngine{parsed: &parsed}).ParseSelector, "", true)
		if err != nil || groups != nil || len(parsed) != 0 {
			t.Errorf("got groups=%v err=%v parsed=%v, want nil,nil,none", groups, err, parsed)
		}
	})
	t.Run("selector parses into one group", func(t *testing.T) {
		var parsed []string
		groups, err := parseMetaQuerySelector((&fakeEngine{parsed: &parsed}).ParseSelector, `{app="api"}`, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "api")}
		if len(groups) != 1 || !matchersEqual(groups[0], want) || len(parsed) != 1 {
			t.Errorf("got groups=%v parsed=%v", groups, parsed)
		}
	})
	t.Run("bare empty selector allowed when tolerated", func(t *testing.T) {
		for _, q := range []string{"{}", "{ }"} {
			var parsed []string
			groups, err := parseMetaQuerySelector((&fakeEngine{parsed: &parsed}).ParseSelector, q, true)
			if err != nil || groups != nil || len(parsed) != 0 {
				t.Errorf("query %q: got groups=%v err=%v parsed=%v, want nil,nil,none", q, groups, err, parsed)
			}
		}
	})
	t.Run("bare empty selector rejected when not tolerated", func(t *testing.T) {
		var parsed []string
		_, err := parseMetaQuerySelector((&fakeEngine{parsed: &parsed}).ParseSelector, "{}", false)
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("err = %v, want plan.ErrBadQuery", err)
		}
	})
}

func TestParseMetaMatchSelectors(t *testing.T) {
	parse := (&fakeEngine{parsed: new([]string)}).ParseSelector
	t.Run("no selectors selects everything", func(t *testing.T) {
		groups, err := parseMetaMatchSelectors(parse, nil)
		if err != nil || groups != nil {
			t.Errorf("got groups=%v err=%v, want nil,nil", groups, err)
		}
	})
	t.Run("lone empty selector is treated as absent", func(t *testing.T) {
		for _, m := range []string{"{}", "{  }"} {
			var parsed []string
			groups, err := parseMetaMatchSelectors((&fakeEngine{parsed: &parsed}).ParseSelector, []string{m})
			if err != nil || groups != nil || len(parsed) != 0 {
				t.Errorf("match %q: got groups=%v err=%v parsed=%v", m, groups, err, parsed)
			}
		}
	})
	t.Run("duplicate selectors are parsed once", func(t *testing.T) {
		var parsed []string
		groups, err := parseMetaMatchSelectors((&fakeEngine{parsed: &parsed}).ParseSelector,
			[]string{`{a="1"}`, `{a="1"}`})
		if err != nil || len(groups) != 1 || len(parsed) != 1 {
			t.Errorf("got groups=%v err=%v parsed=%v", groups, err, parsed)
		}
	})
	t.Run("multiple selectors stay separate groups", func(t *testing.T) {
		var parsed []string
		groups, err := parseMetaMatchSelectors((&fakeEngine{parsed: &parsed}).ParseSelector,
			[]string{`{a="1"}`, `{b="2"}`})
		if err != nil || len(groups) != 2 || len(parsed) != 2 {
			t.Errorf("got groups=%v err=%v parsed=%v", groups, err, parsed)
		}
	})
	t.Run("empty selector among others reaches the parser and fails", func(t *testing.T) {
		var parsed []string
		_, err := parseMetaMatchSelectors((&fakeEngine{parsed: &parsed}).ParseSelector,
			[]string{`{a="1"}`, "{}"})
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("err = %v, want plan.ErrBadQuery", err)
		}
	})
}

func TestRoundToMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		name               string
		start, end         time.Time
		wantStart, wantEnd time.Time
	}{
		{
			name:      "sub-millisecond start floors and end ceilings",
			start:     time.Unix(1793793600, 123456),
			end:       time.Unix(1793793630, 999999),
			wantStart: time.Unix(1793793600, 0),
			wantEnd:   time.Unix(1793793630, 1000000),
		},
		{
			name:      "millisecond-aligned bounds are unchanged",
			start:     time.Unix(1793793600, 500000000),
			end:       time.Unix(1793793660, 0),
			wantStart: time.Unix(1793793600, 500000000),
			wantEnd:   time.Unix(1793793660, 0),
		},
		{
			name:      "nanosecond precision end widens one millisecond",
			start:     time.Unix(1793793600, 0),
			end:       time.Unix(1793793600, 1),
			wantStart: time.Unix(1793793600, 0),
			wantEnd:   time.Unix(1793793600, 1000000),
		},
		// The pre-epoch cases pin the kept compatibility behavior, not true
		// floor/ceiling: UnixNano division truncates toward zero, so the
		// start moves up and the end's +1ms can overshoot, exactly as Loki's
		// util.RoundToMilliseconds does. Any change here needs the lead's
		// approval first.
		{
			name:      "pre-epoch start truncates toward zero, not down",
			start:     time.Unix(-2, 500123456),
			end:       time.Unix(-2, 500123456),
			wantStart: time.UnixMilli(-1499),
			wantEnd:   time.UnixMilli(-1498),
		},
		{
			name:      "pre-epoch millisecond-aligned end is unchanged",
			start:     time.Unix(-2, 500000000),
			end:       time.Unix(-2, 900000000),
			wantStart: time.Unix(-2, 500000000),
			wantEnd:   time.Unix(-2, 900000000),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotStart, gotEnd := roundToMilliseconds(tc.start, tc.end)
			if !gotStart.Equal(tc.wantStart) || !gotEnd.Equal(tc.wantEnd) {
				t.Errorf("roundToMilliseconds = (%s, %s), want (%s, %s)",
					gotStart, gotEnd, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

// TestMetaQueryGroupIsolation documents that a metadata request without
// selectors never invents matchers: the reader sees tenant and time only.
func TestMetaQueryGroupIsolation(t *testing.T) {
	q := metaQuery{Tenant: tenant.Default, Start: testNow.Add(-time.Hour), End: testNow}
	if q.Groups != nil {
		t.Errorf("zero-value metaQuery.Groups = %v, want nil", q.Groups)
	}
	if err := q.Tenant.Validate(); err != nil {
		t.Errorf("tenant: %v", err)
	}
}
