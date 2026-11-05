package plan_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestMatcher(t *testing.T) {
	tests := []struct {
		typ   plan.MatchType
		value string
		in    string
		want  bool
	}{
		{plan.MatchEqual, "api", "api", true},
		{plan.MatchEqual, "", "", true},
		{plan.MatchNotEqual, "api", "", true},
		{plan.MatchRegexp, "ap", "api", false}, // anchored
		{plan.MatchRegexp, "api|web", "web", true},
		{plan.MatchRegexp, ".*", "", true},
		{plan.MatchRegexp, ".+", "", false},
		{plan.MatchRegexp, "(?i)PROD", "prod", true},
		{plan.MatchNotRegexp, "prod|dev", "", true},
		{plan.MatchNotRegexp, "prod|dev", "dev", false},
	}
	for _, tt := range tests {
		m := plan.MustMatcher(tt.typ, "l", tt.value)
		if got := m.Matches(tt.in); got != tt.want {
			t.Errorf("%s matches %q = %v, want %v", m, tt.in, got, tt.want)
		}
	}
	if _, err := plan.NewMatcher(plan.MatchRegexp, "l", "("); !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("bad regexp: err = %v, want ErrBadQuery", err)
	}
	if _, err := plan.NewMatcher(plan.MatchEqual, "", "x"); !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("empty name: err = %v, want ErrBadQuery", err)
	}
	if !plan.MustMatcher(plan.MatchEqual, "foo", "").MatchesLabels(logs.FromMap(map[string]string{"a": "b"})) {
		t.Error(`{foo=""} must match a row without foo`)
	}
}

func TestLineFilter(t *testing.T) {
	tests := []struct {
		typ   plan.LineFilterType
		value string
		line  string
		want  bool
	}{
		{plan.LineContains, "err", "an error", true},
		{plan.LineContains, "ERR", "an error", false},
		{plan.LineContains, "%", "100% done", true},
		{plan.LineContains, "_", "100% done", false},
		{plan.LineNotContains, "err", "an error", false},
		{plan.LineRegexp, "err", "an error", true}, // unanchored
		{plan.LineRegexp, "^$", "", true},
		{plan.LineNotRegexp, "^GET", "GET /", false},
	}
	for _, tt := range tests {
		f := plan.MustLineFilter(tt.typ, tt.value)
		if got := f.Matches(tt.line); got != tt.want {
			t.Errorf("%s on %q = %v, want %v", f, tt.line, got, tt.want)
		}
	}
}

func TestBucketStart(t *testing.T) {
	o := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	b := plan.BucketAgg{Width: time.Minute, Origin: o.Add(time.Microsecond)}
	tests := []struct{ ts, want time.Time }{
		{o.Add(time.Microsecond), o.Add(time.Microsecond)},
		{o, o.Add(-time.Minute + time.Microsecond)}, // negative bucket
		{o.Add(time.Minute), o.Add(time.Microsecond)},
		{o.Add(time.Minute + time.Microsecond), o.Add(time.Minute + time.Microsecond)},
		{o.Add(-2*time.Minute + time.Microsecond), o.Add(-2*time.Minute + time.Microsecond)},
	}
	for _, tt := range tests {
		if got := b.Start(tt.ts); !got.Equal(tt.want) {
			t.Errorf("Start(%s) = %s, want %s", tt.ts, got, tt.want)
		}
	}
}

// TestMemSource pins the reference semantics by hand, so the conformance
// suite isn't only comparing MemSource with itself.
func TestMemSource(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	m := plan.NewMemSource()
	lbl := func(kv ...string) logs.Labels {
		mm := map[string]string{}
		for i := 0; i < len(kv); i += 2 {
			mm[kv[i]] = kv[i+1]
		}
		return logs.FromMap(mm)
	}
	m.Add(tenant.Default,
		plan.Row{TS: t0, Labels: lbl("app", "a", "level", "info"), Line: "one"},
		plan.Row{TS: t0.Add(time.Second + 999*time.Nanosecond), Labels: lbl("app", "a"), Line: "two error"},
		plan.Row{TS: t0.Add(2 * time.Second), Labels: lbl("app", "b"), Line: "three error"},
	)
	m.Add("other", plan.Row{TS: t0, Labels: lbl("app", "a"), Line: "hidden"})

	sel := plan.Selection{
		Tenant:   tenant.Default,
		Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "a")},
		From:     t0, Through: t0.Add(time.Hour), Direction: plan.Backward,
	}
	var lines []string
	for r, err := range m.Select(t.Context(), sel) {
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, r.Line)
		if r.Line == "two error" && r.TS.Nanosecond() != 0 {
			t.Errorf("TS not truncated to µs: %s", r.TS)
		}
	}
	if len(lines) != 2 || lines[0] != "two error" || lines[1] != "one" {
		t.Fatalf("backward select = %q", lines)
	}

	sel.Through = t0.Add(time.Second) // exclusive: drops "two error"
	sel.Matchers = append(sel.Matchers, plan.MustMatcher(plan.MatchEqual, "level", ""))
	n := 0
	for _, err := range m.Select(t.Context(), sel) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 0 {
		t.Fatalf("level=\"\" before t0+1s: %d rows, want 0", n)
	}

	sel = plan.Selection{
		Tenant:      tenant.Default,
		Matchers:    []plan.Matcher{plan.MustMatcher(plan.MatchRegexp, "app", ".+")},
		LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "error")},
		From:        t0, Through: t0.Add(time.Hour),
	}
	var buckets []plan.Bucket
	for b, err := range m.Aggregate(t.Context(), sel, plan.BucketAgg{Width: 2 * time.Second, Origin: t0}) {
		if err != nil {
			t.Fatal(err)
		}
		buckets = append(buckets, b)
	}
	if len(buckets) != 2 ||
		!buckets[0].Start.Equal(t0) || buckets[0].Count != 1 || buckets[0].Bytes != 9 ||
		!buckets[1].Start.Equal(t0.Add(2*time.Second)) || buckets[1].Labels.String() != `{app="b"}` || buckets[1].Bytes != 11 {
		t.Fatalf("buckets = %+v", buckets)
	}
}

func TestMemSourceAggregateIncludeMeta(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	m := plan.NewMemSource()
	app := logs.FromMap(map[string]string{"app": "a"})
	m.Add(tenant.Default,
		plan.Row{TS: t0, Labels: app, Meta: logs.FromMap(map[string]string{"trace_id": "1"}), Line: "x"},
		plan.Row{TS: t0.Add(time.Second), Labels: app, Meta: logs.FromMap(map[string]string{"trace_id": "2"}), Line: "yy"},
		plan.Row{TS: t0.Add(2 * time.Second), Labels: app, Line: "zzz"},
	)
	sel := plan.Selection{Tenant: tenant.Default, Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "a")},
		From: t0, Through: t0.Add(time.Hour)}
	count := func(agg plan.BucketAgg) []plan.Bucket {
		var out []plan.Bucket
		for b, err := range m.Aggregate(t.Context(), sel, agg) {
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, b)
		}
		return out
	}
	if bs := count(plan.BucketAgg{Width: time.Minute, Origin: t0}); len(bs) != 1 || bs[0].Count != 3 || bs[0].Meta != nil {
		t.Fatalf("without IncludeMeta: %+v", bs)
	}
	bs := count(plan.BucketAgg{Width: time.Minute, Origin: t0, IncludeMeta: true})
	if len(bs) != 3 {
		t.Fatalf("with IncludeMeta: %d buckets, want 3 (two trace_ids and no metadata): %+v", len(bs), bs)
	}
	// Sorted by Meta.String(): `{trace_id="1"}` < `{trace_id="2"}` < `{}`.
	if bs[0].Meta.String() != `{trace_id="1"}` || bs[0].Bytes != 1 || bs[1].Bytes != 2 || bs[2].Meta.String() != "{}" || bs[2].Bytes != 3 {
		t.Fatalf("with IncludeMeta: %+v", bs)
	}
}

// TestMemSourceDeduplicates pins the at-least-once rule: an exact copy of a
// row (a retried push) counts once in Select and Aggregate; a copy with
// different metadata is a different row.
func TestMemSourceDeduplicates(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	app := logs.FromMap(map[string]string{"app": "a"})
	r := plan.Row{TS: t0, Labels: app, Meta: logs.FromMap(map[string]string{"trace_id": "1"}), Line: "hello"}
	other := r
	other.Meta = logs.FromMap(map[string]string{"trace_id": "2"})
	m := plan.NewMemSource()
	m.Add(tenant.Default, r, r, other, r)
	sel := plan.Selection{Tenant: tenant.Default, Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "a")},
		From: t0, Through: t0.Add(time.Hour), Limit: 10}
	n := 0
	for _, err := range m.Select(t.Context(), sel) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("Select returned %d rows, want 2 (one per distinct row)", n)
	}
	for b, err := range m.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
		if err != nil {
			t.Fatal(err)
		}
		if b.Count != 2 || b.Bytes != 10 {
			t.Fatalf("bucket = %+v, want Count 2, Bytes 10", b)
		}
	}
}
