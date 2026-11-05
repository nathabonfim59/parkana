package logql

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// The HTTP frontend always sends a positive step for range queries (sol-b's
// query_range floors range/250s at 1s and rejects <=0); Step==0 is the
// /query instant path, which the engine rejects for log selectors. Log
// evaluation ignores Step otherwise.
const testStep = time.Nanosecond

func testEngine() *Engine { return New(config.LogQL{MaxEntries: 100, MaxSeries: 50}) }

func testParams(q string, start, end time.Time) plan.Params {
	return plan.Params{
		Tenant:    tenant.Default,
		Query:     q,
		Start:     start,
		End:       end,
		Step:      testStep,
		Limit:     100,
		Direction: plan.Backward,
	}
}

// coverParams is testParams for a range that must include a row stamped
// exactly at last: log queries select [start, end), so the end is last+1ns.
func coverParams(q string, start, last time.Time) plan.Params {
	return testParams(q, start, last.Add(time.Nanosecond))
}

func logRow(ts time.Time, line string, pairs ...logs.Label) plan.Row {
	return plan.Row{TS: ts, Line: line, Labels: logs.New(pairs...)}
}

func appRow(ts time.Time, line string) plan.Row {
	return logRow(ts, line, logs.Label{Name: "app", Value: "api"})
}

func shardRow(ts time.Time, line, shard string) plan.Row {
	return logRow(ts, line,
		logs.Label{Name: "app", Value: "api"},
		logs.Label{Name: "shard", Value: shard},
	)
}

// resultLines returns every entry line in result order.
func resultLines(res plan.Result) []string {
	var out []string
	for _, s := range res.Streams {
		for _, e := range s.Entries {
			out = append(out, e.Line)
		}
	}
	return out
}

// sortedResultLines returns the entry lines sorted, for order-insensitive
// assertions.
func sortedResultLines(res plan.Result) []string {
	out := resultLines(res)
	slices.Sort(out)
	return out
}

func checkLines(t *testing.T, got, want []string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("lines = %q, want %q", got, want)
	}
}

func mustQuery(t *testing.T, e *Engine, src plan.Source, p plan.Params) plan.Result {
	t.Helper()
	res, err := e.Query(t.Context(), src, p)
	if err != nil {
		t.Fatalf("Query(%q): %v", p.Query, err)
	}
	return res
}

// TestQueryLogsMicrosecondBounds pins Loki's [start, end) log range onto the
// microsecond storage grid: From and the exclusive Through both round up.
func TestQueryLogsMicrosecondBounds(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		appRow(base, "t0"),
		appRow(base.Add(time.Microsecond), "t1"),
		appRow(base.Add(2*time.Microsecond), "t2"),
	)
	e := testEngine()
	tests := []struct {
		name       string
		start, end time.Time
		want       []string
	}{
		{"aligned end is exclusive", base, base.Add(time.Microsecond), []string{"t0"}},
		{"empty point range", base, base, nil},
		{"start between grid points", base.Add(500 * time.Nanosecond), base.Add(time.Microsecond + 500*time.Nanosecond), []string{"t1"}},
		{"end just below next grid point", base, base.Add(time.Microsecond - time.Nanosecond), []string{"t0"}},
		{"start just after grid point", base.Add(time.Nanosecond), base.Add(2*time.Microsecond - time.Nanosecond), []string{"t1"}},
		{"sub-microsecond window", base.Add(-time.Nanosecond), base.Add(time.Nanosecond), []string{"t0"}},
		{"end on the last row excludes it", base, base.Add(2 * time.Microsecond), []string{"t0", "t1"}},
		{"end just past the last row", base, base.Add(2*time.Microsecond + time.Nanosecond), []string{"t0", "t1", "t2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testParams(`{app="api"}`, tt.start, tt.end)
			p.Direction = plan.Forward
			res := mustQuery(t, e, src, p)
			checkLines(t, resultLines(res), tt.want)
		})
	}
}

func TestQueryLogsDirection(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		appRow(base, "t0"),
		appRow(base.Add(time.Second), "t1"),
		appRow(base.Add(2*time.Second), "t2"),
	)
	e := testEngine()
	tests := []struct {
		dir  plan.Direction
		want []string
	}{
		{plan.Forward, []string{"t0", "t1", "t2"}},
		{plan.Backward, []string{"t2", "t1", "t0"}},
	}
	for _, tt := range tests {
		p := coverParams(`{app="api"}`, base, base.Add(2*time.Second))
		p.Direction = tt.dir
		res := mustQuery(t, e, src, p)
		if got := resultLines(res); !slices.Equal(got, tt.want) {
			t.Fatalf("direction %s lines = %q, want %q", tt.dir, got, tt.want)
		}
	}
}

func TestQueryLogsInterval(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	// Two genuinely different streams, interleaved in time: sampling is
	// global across streams, not per stream.
	src.Add(tenant.Default,
		shardRow(base, "s1-t0", "s1"),
		shardRow(base.Add(30*time.Second), "s1-t30", "s1"),
		shardRow(base.Add(60*time.Second), "s2-t60", "s2"),
		shardRow(base.Add(90*time.Second), "s1-t90", "s1"),
		shardRow(base.Add(120*time.Second), "s2-t120", "s2"),
	)
	e := testEngine()
	for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
		p := coverParams(`{app="api"}`, base, base.Add(120*time.Second))
		p.Direction = dir
		p.Interval = 60 * time.Second
		res := mustQuery(t, e, src, p)
		if len(res.Streams) != 2 {
			t.Fatalf("direction %s: streams = %d, want 2 distinct label sets", dir, len(res.Streams))
		}
		// Equality at the boundary passes in both directions; the skipped
		// +30s row never becomes the sampling reference, even though the
		// +60s survivor belongs to the other stream.
		checkLines(t, sortedResultLines(res), []string{"s1-t0", "s2-t60", "s2-t120"})
	}
}

func TestQueryLogsIntervalUsesPriorEmittedRow(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		appRow(base, "keep-t0"),
		appRow(base.Add(60*time.Second), "skip-t60"),
		appRow(base.Add(90*time.Second), "keep-t90"),
	)
	p := coverParams(`{app="api"} != "skip"`, base, base.Add(90*time.Second))
	p.Direction = plan.Forward
	p.Interval = 60 * time.Second
	res := mustQuery(t, testEngine(), src, p)
	// If the rejected +60s row had updated the interval clock, +90s would be
	// skipped; the prior *emitted* row is t0, so +90s passes.
	checkLines(t, resultLines(res), []string{"keep-t0", "keep-t90"})
}

func TestQueryLogsIntervalPre1970(t *testing.T) {
	base := time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		appRow(base, "t0"),
		appRow(base.Add(30*time.Second), "t30"),
		appRow(base.Add(60*time.Second), "t60"),
	)
	e := testEngine()
	for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
		p := coverParams(`{app="api"}`, base, base.Add(60*time.Second))
		p.Direction = dir
		p.Interval = 60 * time.Second
		res := mustQuery(t, e, src, p)
		// A first-emitted flag, not Loki's lastEntry.Unix() < 0 sentinel:
		// pre-epoch rows still sample, so t30 is skipped.
		checkLines(t, sortedResultLines(res), []string{"t0", "t60"})
	}
}

func TestQueryLogsIntervalZeroEmitsAll(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	var want []string
	for i := range 5 {
		ts := base.Add(time.Duration(i) * time.Second)
		line := "t" + string(rune('0'+i))
		want = append(want, line)
		src.Add(tenant.Default, appRow(ts, line))
	}
	p := coverParams(`{app="api"}`, base, base.Add(4*time.Second))
	p.Direction = plan.Forward
	res := mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), want)
}

func TestQueryLogsGlobalLimit(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		appRow(base, "oldest"),
		logRow(base.Add(time.Second), "second", logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "shard", Value: "b"}),
		appRow(base.Add(2*time.Second), "third"),
		logRow(base.Add(3*time.Second), "fourth", logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "shard", Value: "b"}),
		appRow(base.Add(4*time.Second), "newest"),
	)
	p := coverParams(`{app="api"}`, base, base.Add(4*time.Second))
	p.Limit = 2
	res := mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), []string{"fourth", "newest"})
}

// TestQueryLogsLimitZeroIsUnlimited documents the engine side of the limit
// contract: the HTTP frontend rejects an explicit zero and supplies Loki's
// default 100, so an engine request with Limit 0 means "no client limit".
func TestQueryLogsLimitZeroIsUnlimited(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	for i := range 5 {
		src.Add(tenant.Default, appRow(base.Add(time.Duration(i)*time.Second), "line"))
	}
	p := coverParams(`{app="api"}`, base, base.Add(4*time.Second))
	p.Limit = 0
	res := mustQuery(t, testEngine(), src, p)
	if got := len(resultLines(res)); got != 5 {
		t.Fatalf("Limit 0 returned %d rows, want all 5", got)
	}
}

func TestQueryLogsMaxEntries(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(base, "line"))
	e := New(config.LogQL{MaxEntries: 3, MaxSeries: 50})
	tests := []struct {
		limit int
		want  error
	}{
		{0, nil},
		{3, nil},
		{4, plan.ErrLimit},
	}
	for _, tt := range tests {
		p := testParams(`{app="api"}`, base, base.Add(time.Second))
		p.Limit = tt.limit
		_, err := e.Query(t.Context(), src, p)
		if !errors.Is(err, tt.want) {
			t.Fatalf("limit %d: err = %v, want errors.Is(_, %v)", tt.limit, err, tt.want)
		}
	}
}

func TestQueryLogsInvalidRequest(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	e := testEngine()
	tests := []struct {
		name   string
		mutate func(*plan.Params)
	}{
		{"invalid direction", func(p *plan.Params) { p.Direction = plan.Direction(99) }},
		{"end before start", func(p *plan.Params) { p.Start, p.End = p.End, p.Start }},
		{"negative limit", func(p *plan.Params) { p.Limit = -1 }},
		{"negative interval", func(p *plan.Params) { p.Interval = -time.Second }},
		{"empty query", func(p *plan.Params) { p.Query = "" }},
		{"invalid tenant", func(p *plan.Params) { p.Tenant = tenant.ID("bad tenant") }},
		{"negative step", func(p *plan.Params) { p.Step = -time.Second }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testParams(`{app="api"}`, base, base.Add(time.Second))
			tt.mutate(&p)
			_, err := e.Query(t.Context(), src, p)
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("err = %v, want ErrBadQuery", err)
			}
		})
	}
}

func TestQueryLogsNoMatch(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	res := mustQuery(t, testEngine(), plan.NewMemSource(), testParams(`{app="api"}`, base, base.Add(time.Second)))
	if res.Type != plan.TypeStreams {
		t.Fatalf("Type = %q, want %q", res.Type, plan.TypeStreams)
	}
	if len(res.Streams) != 0 {
		t.Fatalf("Streams = %v, want empty", res.Streams)
	}
}

// TestQueryLogsInstantRejected pins Loki's public instant-query behavior:
// pkg/querier/http.go:122-128 rejects a non-SampleExpr (a log selector) with
// ErrUnsupportedSyntaxForInstantQuery before executing anything. Params.Instant
// (Step==0) is the engine's only distinction, so the Source must not be read.
func TestQueryLogsInstantRejected(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{rows: []plan.Row{appRow(base, "line")}}
	p := testParams(`{app="api"}`, base, base)
	p.Step = 0
	_, err := testEngine().Query(t.Context(), src, p)
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
	if src.selectCalls != 0 {
		t.Fatalf("Source.Select called %d times for an instant log query", src.selectCalls)
	}
}

// TestQueryLogsPointRangeWithStep: a point range (Start==End) with a positive
// step is a range request, not an instant query: it stays valid and, like
// Loki's [start, end) log selection, returns nothing.
func TestQueryLogsPointRangeWithStep(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(base, "hit"), appRow(base.Add(time.Microsecond), "after"))
	p := testParams(`{app="api"}`, base, base)
	res := mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), nil)
}

func TestQueryLogsTenantIsolation(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(base, "default-row"))
	src.Add(tenant.ID("other"), appRow(base, "other-row"))
	p := testParams(`{app="api"}`, base, base.Add(time.Second))
	res := mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), []string{"default-row"})

	p.Tenant = tenant.ID("other")
	res = mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), []string{"other-row"})
}

func TestQueryLogsGroupingAndEntryCategories(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		plan.Row{TS: base, Line: "a1", Labels: logs.New(logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "env", Value: "prod"}), Meta: logs.New(logs.Label{Name: "trace", Value: "1"})},
		plan.Row{TS: base.Add(time.Second), Line: "a2", Labels: logs.New(logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "env", Value: "prod"}), Meta: logs.New(logs.Label{Name: "trace", Value: "2"})},
		plan.Row{TS: base.Add(2 * time.Second), Line: "b1", Labels: logs.New(logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "env", Value: "dev"})},
	)
	res := mustQuery(t, testEngine(), src, coverParams(`{app="api"}`, base, base.Add(2*time.Second)))
	if len(res.Streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(res.Streams))
	}
	// Streams sort by final label set: dev before prod.
	if got := res.Streams[0].Labels.String(); got != `{app="api", env="dev"}` {
		t.Fatalf("first stream labels = %s", got)
	}
	prod := res.Streams[1]
	if got := prod.Labels.String(); got != `{app="api", env="prod"}` {
		t.Fatalf("prod labels = %s", got)
	}
	// Meta stays per entry and out of the stream labels.
	if len(prod.Entries) != 2 {
		t.Fatalf("prod entries = %d, want 2", len(prod.Entries))
	}
	if got, _ := prod.Entries[0].Meta.Get("trace"); got != "2" {
		t.Fatalf("newest entry meta trace = %q, want 2", got)
	}
	if _, ok := prod.Labels.Get("trace"); ok {
		t.Fatalf("stream labels leaked per-entry meta: %s", prod.Labels)
	}
	if prod.Entries[0].Parsed != nil {
		t.Fatalf("Parsed = %v, want nil without a parser", prod.Entries[0].Parsed)
	}
}

func TestQueryLogsLineFilterForms(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"err", "fail", "ok", "err debug", "fail err"}
	src := plan.NewMemSource()
	for i, line := range lines {
		src.Add(tenant.Default, appRow(base.Add(time.Duration(i)*time.Second), line))
	}
	e := testEngine()
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"contains", `{app="api"} |= "err"`, []string{"err", "err debug", "fail err"}},
		{"contains or", `{app="api"} |= "err" or "fail"`, []string{"err", "fail", "err debug", "fail err"}},
		{"not contains", `{app="api"} != "err"`, []string{"fail", "ok"}},
		{"negative or is AND", `{app="api"} != "err" or "fail"`, []string{"ok"}},
		{"regexp", `{app="api"} |~ "err|fail"`, []string{"err", "fail", "err debug", "fail err"}},
		{"not regexp", `{app="api"} !~ "err|fail"`, []string{"ok"}},
		{"two stages", `{app="api"} |= "err" != "debug"`, []string{"err", "fail err"}},
		{"anchored regexp", `{app="api"} |~ "^err$"`, []string{"err"}},
		{"empty contains is true", `{app="api"} |= ""`, lines},
		{"empty not contains is false", `{app="api"} != ""`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mustQuery(t, e, src, coverParams(tt.query, base, base.Add(4*time.Second)))
			checkLines(t, sortedResultLines(res), tt.want)
		})
	}
}

// TestQueryLogsLineRegexpNewlineSemantics pins Loki's observable line-regexp
// behavior, which is not plain Go regexp: the optimizer turns .+ (including
// captures) into len(line)>0, so a newline-only line passes, while a.b still
// does not cross the newline and explicit (?s:)/(?-s:) overrides are honored.
func TestQueryLogsLineRegexpNewlineSemantics(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"", "\n", "a\nb", "a b", "x", "ERR"}
	src := plan.NewMemSource()
	for i, line := range lines {
		src.Add(tenant.Default, appRow(base.Add(time.Duration(i)*time.Second), line))
	}
	e := testEngine()
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"plus is non-empty", `{app="api"} |~ ".+"`, []string{"\n", "a\nb", "a b", "x", "ERR"}},
		{"captured plus is non-empty", `{app="api"} |~ "(.+)"`, []string{"\n", "a\nb", "a b", "x", "ERR"}},
		{"dot excludes newline", `{app="api"} |~ "a.b"`, []string{"a b"}},
		{"explicit dotall", `{app="api"} |~ "(?s:a.b)"`, []string{"a\nb", "a b"}},
		{"explicit no dotall", `{app="api"} |~ "(?-s:a.b)"`, []string{"a b"}},
		{"negated plus", `{app="api"} !~ ".+"`, []string{""}},
		{"alternate with plus", `{app="api"} |~ "a|.+"`, []string{"\n", "a\nb", "a b", "x", "ERR"}},
		{"star is true", `{app="api"} |~ ".*"`, lines},
		{"negated dot excludes newline", `{app="api"} !~ "a.b"`, []string{"", "\n", "a\nb", "x", "ERR"}},
		{"case insensitive", `{app="api"} |~ "(?i)err"`, []string{"ERR"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mustQuery(t, e, src, coverParams(tt.query, base, base.Add(5*time.Second)))
			checkLines(t, sortedResultLines(res), tt.want)
		})
	}
}

func TestQueryLogsUnsupportedStages(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// A6 supplies formatting, drop and keep. The recognized unpack and
	// decolorize forms remain explicitly unsupported without reading.
	queries := []string{
		`{app="api"} | unpack`,
		`{app="api"} | decolorize`,
	}
	e := testEngine()
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			src := &fakeSource{}
			_, err := e.Query(t.Context(), src, testParams(q, base, base.Add(time.Second)))
			if !errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
			if src.selectCalls != 0 {
				t.Fatalf("Source.Select called %d times for an unsupported stage", src.selectCalls)
			}
		})
	}
}

func TestQueryLogsMetricUnsupported(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// A7 evaluates count_over_time/rate/bytes_over_time/bytes_rate, A8 its
	// vector aggregations and remaining range functions, and A9 scalars and
	// binary expressions. The deferred families must still fail explicitly
	// without reading.
	queries := []string{
		`sum(1)`,
		`count_over_time({app="api"}[5m]) and count_over_time({app="api"}[5m])`,
		`label_replace(count_over_time({app="api"}[5m]), "dst", "repl", "src", ".*")`,
	}
	e := testEngine()
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			src := &fakeSource{}
			p := testParams(q, base, base.Add(time.Minute))
			p.Step = time.Minute
			_, err := e.Query(t.Context(), src, p)
			if !errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported", err)
			}
			if src.selectCalls != 0 {
				t.Fatalf("Source.Select called %d times for a metric query", src.selectCalls)
			}
		})
	}
}

func TestQueryLogsBareRangeRejected(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{}
	_, err := testEngine().Query(t.Context(), src, testParams(`{app="api"}[5m]`, base, base.Add(time.Minute)))
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
	if src.selectCalls != 0 {
		t.Fatalf("Source.Select called for a bare range")
	}
}

func TestQueryLogsSourceErrorAfterRows(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	sentinel := errors.New("storage exploded")
	src := &fakeSource{
		rows:     []plan.Row{appRow(base, "one"), appRow(base.Add(time.Second), "two"), appRow(base.Add(2*time.Second), "three")},
		errAfter: 2,
		err:      sentinel,
	}
	_, err := testEngine().Query(t.Context(), src, testParams(`{app="api"}`, base, base.Add(2*time.Second)))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want errors.Is(_, sentinel)", err)
	}
}

func TestQueryLogsCancellationDuringIteration(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx, cancel := context.WithCancel(t.Context())
	src := &fakeSource{
		rows: []plan.Row{appRow(base, "one"), appRow(base.Add(time.Second), "two")},
		onYield: func(i int) {
			if i == 1 {
				cancel()
			}
		},
	}
	_, err := testEngine().Query(ctx, src, testParams(`{app="api"}`, base, base.Add(time.Second)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestQueryLogsCancellationBeforeIteration(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	src := &fakeSource{rows: []plan.Row{appRow(base, "one")}}
	_, err := testEngine().Query(ctx, src, testParams(`{app="api"}`, base, base.Add(time.Second)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// The canceled path must not open the Source at all.
	if src.selectCalls != 0 {
		t.Fatalf("Source.Select called %d times for a canceled query", src.selectCalls)
	}
}

func TestQueryLogsIteratorEarlyStop(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{rows: []plan.Row{
		appRow(base, "one"),
		appRow(base.Add(time.Second), "two"),
		appRow(base.Add(2*time.Second), "three"),
	}}
	p := testParams(`{app="api"}`, base, base.Add(2*time.Second))
	p.Direction = plan.Forward
	p.Limit = 1
	res := mustQuery(t, testEngine(), src, p)
	if got := resultLines(res); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("lines = %q, want [one]", got)
	}
	if !src.stoppedEarly {
		t.Fatalf("consumer did not stop the Source iterator after the limit")
	}
	if src.yields != 1 {
		t.Fatalf("Source yielded %d rows, want 1", src.yields)
	}
}

// TestQueryLogsTieOrderIsUnspecified runs the same equal-timestamp rows in two
// different Source orders. The result must be valid for both: two rows at the
// tied timestamp, with no assumption about which ones a stable MemSource would
// pick.
func TestQueryLogsTieOrderIsUnspecified(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	orders := [][]plan.Row{
		{appRow(base, "a"), appRow(base, "b"), appRow(base, "c")},
		{appRow(base, "c"), appRow(base, "a"), appRow(base, "b")},
	}
	for _, rows := range orders {
		src := &fakeSource{rows: rows}
		p := testParams(`{app="api"}`, base, base)
		p.Limit = 2
		res := mustQuery(t, testEngine(), src, p)
		lines := resultLines(res)
		if len(lines) != 2 {
			t.Fatalf("lines = %q, want 2", lines)
		}
		for _, s := range res.Streams {
			for _, e := range s.Entries {
				if !e.TS.Equal(base) {
					t.Fatalf("entry TS = %s, want tied %s", e.TS, base)
				}
			}
		}
	}
}

func TestQueryLogsDoesNotMutateSourceRows(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	row := plan.Row{
		TS:     base,
		Line:   "line",
		Labels: logs.New(logs.Label{Name: "app", Value: "api"}),
		Meta:   logs.New(logs.Label{Name: "trace", Value: "1"}),
	}
	beforeLabels := slices.Clone(row.Labels)
	beforeMeta := slices.Clone(row.Meta)
	src := plan.NewMemSource()
	src.Add(tenant.Default, row)
	res := mustQuery(t, testEngine(), src, coverParams(`{app="api"}`, base, base))
	if !slices.Equal(row.Labels, beforeLabels) || !slices.Equal(row.Meta, beforeMeta) {
		t.Fatalf("source row changed: labels %v meta %v", row.Labels, row.Meta)
	}
	// Mutating the result must not reach back into the source either.
	res.Streams[0].Labels[0].Value = "changed"
	if row.Labels[0].Value != "api" {
		t.Fatalf("result labels alias the source row: %v", row.Labels)
	}
}

// TestQueryLogsOrDropsTrueFilter pins Loki's newOrFilter identity rule
// (filter.go:309-322): a TrueFilter operand is dropped, not absorbing. So
// foo|.* matches only foo, and the surface |= "" or "foo" follows the same
// ChainOrFilter rule. A negative foo|.* therefore matches everything that does
// not contain foo.
func TestQueryLogsOrDropsTrueFilter(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"foo", "other", "É", "é"}
	src := plan.NewMemSource()
	for i, line := range lines {
		src.Add(tenant.Default, appRow(base.Add(time.Duration(i)*time.Second), line))
	}
	e := testEngine()
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"regexp true right", `{app="api"} |~ "foo|.*"`, []string{"foo"}},
		{"regexp true left", `{app="api"} |~ ".*|foo"`, []string{"foo"}},
		{"surface empty right", `{app="api"} |= "foo" or ""`, []string{"foo"}},
		{"surface empty left", `{app="api"} |= "" or "foo"`, []string{"foo"}},
		{"negative regexp true", `{app="api"} !~ "foo|.*"`, []string{"other", "É", "é"}},
		{"negative surface empty", `{app="api"} != "" or "foo"`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mustQuery(t, e, src, coverParams(tt.query, base, base.Add(3*time.Second)))
			checkLines(t, sortedResultLines(res), tt.want)
		})
	}
}

func TestQueryLogsInvalidLineRegexp(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{}
	_, err := testEngine().Query(t.Context(), src, testParams(`{app="api"} |~ "["`, base, base.Add(time.Second)))
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("err = %v, want ErrBadQuery", err)
	}
	if src.selectCalls != 0 {
		t.Fatalf("Source.Select called for an invalid line regexp")
	}
}

// TestQueryLogsLimitCountsOnlyEmittedRows: a Go-side filter (positive OR, which
// plan.Selection cannot express) rejects rows, and only the emitted ones count
// against the limit.
func TestQueryLogsLimitCountsOnlyEmittedRows(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// Rejected rows precede the cap: skip, err, skip, fail, err. Only err and
	// fail are emitted, and only those two consume the limit.
	lines := []string{"skip", "err", "skip", "fail", "err"}
	src := plan.NewMemSource()
	for i, line := range lines {
		src.Add(tenant.Default, appRow(base.Add(time.Duration(i)*time.Second), line))
	}
	p := testParams(`{app="api"} |= "err" or "fail"`, base, base.Add(4*time.Second))
	p.Direction = plan.Forward
	p.Limit = 2
	res := mustQuery(t, testEngine(), src, p)
	checkLines(t, resultLines(res), []string{"err", "fail"})
}

// TestQueryLogsFixtureLabels uses the plantest dataset: promoted labels are
// ordinary stream labels, and "Level" is a different label from "level".
func TestQueryLogsFixtureLabels(t *testing.T) {
	src := plantest.Mem(plantest.Dataset())
	p := testParams(`{Level="High"}`, plantest.T0, plantest.T0.Add(700*time.Second))
	p.Limit = 0
	res := mustQuery(t, testEngine(), src, p)
	var rows int
	for _, s := range res.Streams {
		level, ok := s.Labels.Get("Level")
		if !ok || level != "High" {
			t.Fatalf("stream labels = %s, want Level=High", s.Labels)
		}
		rows += len(s.Entries)
	}
	if rows != 20 {
		t.Fatalf("rows = %d, want 20 (the caseLabels file only)", rows)
	}
}
