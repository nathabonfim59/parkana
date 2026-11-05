package logql

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// mustStages parses query and compiles its non-line-filter stages. A4 compiles
// line filters itself; these tests exercise the A5 compiler directly.
func mustStages(t *testing.T, query string) []pipelineStage {
	t.Helper()
	expr, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q): %v", query, err)
	}
	lq, ok := expr.(*LogQuery)
	if !ok {
		t.Fatalf("Parse(%q) = %T, want *LogQuery", query, expr)
	}
	stages := make([]pipelineStage, 0, len(lq.Stages))
	for _, st := range lq.Stages {
		if _, ok := st.(*LineFilter); ok {
			continue
		}
		ps, err := compileStage(st)
		if err != nil {
			t.Fatalf("compileStage(%s): %v", st, err)
		}
		stages = append(stages, ps)
	}
	return stages
}

// newTestEntry builds the mutable entry Process would build from a source row.
func newTestEntry(line string, labels, meta logs.Labels) *logEntry {
	return &logEntry{
		line:   line,
		labels: slices.Clone(labels),
		base:   slices.Clone(labels),
		meta:   slices.Clone(meta),
	}
}

func mustRun(t *testing.T, stages []pipelineStage, e *logEntry) bool {
	t.Helper()
	for _, st := range stages {
		keep, err := st.process(t.Context(), e)
		if err != nil {
			t.Fatalf("process: %v", err)
		}
		if !keep {
			return false
		}
	}
	return true
}

// labelPairs builds a sorted label set from name/value pairs.
func labelPairs(kv ...string) logs.Labels {
	ls := make([]logs.Label, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		ls = append(ls, logs.Label{Name: kv[i], Value: kv[i+1]})
	}
	return logs.New(ls...)
}

// parsedExceptErrors returns the parsed labels without the error state, which
// tests assert separately.
func parsedExceptErrors(e *logEntry) map[string]string {
	m := e.parsed.Map()
	delete(m, errorLabel)
	delete(m, errorDetailsLabel)
	return m
}

func assertParsed(t *testing.T, e *logEntry, want map[string]string) {
	t.Helper()
	if got := parsedExceptErrors(e); !maps.Equal(got, want) {
		t.Fatalf("parsed labels = %v, want %v", got, want)
	}
}

func assertErr(t *testing.T, e *logEntry, wantKind string) {
	t.Helper()
	if got := e.errValue(); got != wantKind {
		t.Fatalf("__error__ = %q, want %q (details %q)", got, wantKind, e.errDetailsValue())
	}
}

func TestLogEntryGetPrecedence(t *testing.T) {
	e := newTestEntry("line", labelPairs("s", "v"), labelPairs("a", "meta"))
	if v, cat, ok := e.get("a"); !ok || v != "meta" || cat != metaLabels {
		t.Fatalf("get(a) = %q, %v, %v; want meta", v, cat, ok)
	}
	// A parsed write takes the name away from the metadata category.
	e.setParsed("a", "parsed")
	if v, cat, ok := e.get("a"); !ok || v != "parsed" || cat != parsedLabels {
		t.Fatalf("get(a) = %q, %v, %v; want parsed", v, cat, ok)
	}
	// Del removes every category and leaves the name missing, like
	// LabelsBuilder.Del (the base labels are tombstoned too).
	e.deleteLabel("a")
	if _, _, ok := e.get("a"); ok {
		t.Fatal("get(a) after delete = found, want missing")
	}
	// A stream label is visible when nothing shadows it.
	e2 := newTestEntry("line", labelPairs("a", "stream"), nil)
	if v, cat, ok := e2.get("a"); !ok || v != "stream" || cat != streamLabels {
		t.Fatalf("get(a) = %q, %v, %v; want stream", v, cat, ok)
	}
}

func TestLogEntryGetEmptyStreamLabelIsMissing(t *testing.T) {
	e := newTestEntry("line", labelPairs("a", ""), nil)
	if _, _, ok := e.get("a"); ok {
		t.Fatal("empty stream label reported present, want missing")
	}
	e.setParsed("a", "")
	if v, cat, ok := e.get("a"); !ok || v != "" || cat != parsedLabels {
		t.Fatalf("get(a) = %q, %v, %v; want empty parsed label", v, cat, ok)
	}
}

func TestLogEntryGetIgnoresErrorState(t *testing.T) {
	e := newTestEntry("line", nil, nil)
	e.setParseError(jsonParserErr, nil)
	if _, _, ok := e.get(errorLabel); ok {
		t.Fatal("get(__error__) = found, want missing (Loki keeps it out of Get)")
	}
	if got := e.errValue(); got != jsonParserErr {
		t.Fatalf("errValue() = %q, want %q", got, jsonParserErr)
	}
}

func TestLogEntryParserKeyUsesBaseAfterDelete(t *testing.T) {
	e := newTestEntry("line", labelPairs("x", "s"), nil)
	e.deleteLabel("x")
	if got := e.parserKey("x"); got != "x"+duplicateSuffix {
		t.Fatalf("parserKey(x) after delete = %q, want %q", got, "x"+duplicateSuffix)
	}
}

func TestLogEntrySetParsedRemovesOtherCategories(t *testing.T) {
	e := newTestEntry("line", labelPairs("x", "stream", "y", "keep"), labelPairs("x", "meta"))
	e.setParsed("x", "parsed")
	if _, ok := e.labels.Get("x"); ok {
		t.Fatal("stream label x still present after parsed write")
	}
	if _, ok := e.meta.Get("x"); ok {
		t.Fatal("meta label x still present after parsed write")
	}
	if !e.alreadyExtracted("x") {
		t.Fatal("setParsed did not record the extracted name")
	}
	if v, ok := e.labels.Get("y"); !ok || v != "keep" {
		t.Fatal("unrelated stream label lost")
	}
}

func TestLogEntryDeleteKeepsExtractedRecord(t *testing.T) {
	e := newTestEntry("line", nil, nil)
	e.setParsed("x", "v")
	e.deleteLabel("x")
	if !e.alreadyExtracted("x") {
		t.Fatal("delete cleared the extracted record; LabelsBuilder.Del does not")
	}
}

func TestLogEntryParserErrors(t *testing.T) {
	e := newTestEntry("line", nil, nil)
	e.setParseError(jsonParserErr, context.Canceled)
	if got, want := e.errDetailsValue(), context.Canceled.Error(); got != want {
		t.Fatalf("details = %q, want %q", got, want)
	}
	// A second parser failure replaces the kind, like addErrLabel.
	e.setParseError(logfmtParserErr, nil)
	assertErr(t, e, logfmtParserErr)
	if got, want := e.errDetailsValue(), context.Canceled.Error(); got != want {
		t.Fatalf("details after nil detail = %q, want previous %q", got, want)
	}
	// Typed filters must not overwrite an existing error.
	e.setErrIfAbsent(labelFilterErr, "ignored")
	assertErr(t, e, logfmtParserErr)
	if got, want := e.errDetailsValue(), context.Canceled.Error(); got != want {
		t.Fatalf("details after setErrIfAbsent = %q, want %q", got, want)
	}
	// With no error yet it sets both.
	e2 := newTestEntry("line", nil, nil)
	e2.setErrIfAbsent(labelFilterErr, "bad number")
	assertErr(t, e2, labelFilterErr)
	if got, want := e2.errDetailsValue(), "bad number"; got != want {
		t.Fatalf("details = %q, want %q", got, want)
	}
}

func TestPipelineContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, query := range []string{
		`{app="api"} | json`,
		`{app="api"} | logfmt`,
		`{app="api"} | regexp "(?P<x>.*)"`,
		`{app="api"} | pattern "<x>"`,
		`{app="api"} | x > 1`,
	} {
		t.Run(query, func(t *testing.T) {
			stages := mustStages(t, query)
			for _, st := range stages {
				_, err := st.process(ctx, newTestEntry("{}", nil, nil))
				if err == nil {
					t.Fatalf("%T: canceled context returned no error", st)
				}
			}
		})
	}
}

func TestEngineQueryJSONPipelineCategories(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		plan.Row{
			TS:     base,
			Line:   `{"level":"error","code":500,"msg":"boom"}`,
			Labels: labelPairs("app", "api"),
			Meta:   labelPairs("trace", "t1"),
		},
		plan.Row{
			TS:     base.Add(time.Second),
			Line:   `{"level":"info","code":200,"msg":"ok"}`,
			Labels: labelPairs("app", "api"),
		},
	)
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | json | level="error"`, base, base.Add(time.Minute)))
	if len(res.Streams) != 1 || len(res.Streams[0].Entries) != 1 {
		t.Fatalf("streams = %+v, want one stream with one entry", res.Streams)
	}
	s := res.Streams[0]
	if got := s.Labels.String(); got != `{app="api"}` {
		t.Fatalf("stream labels = %s, want only app (parsed fields stay per entry)", got)
	}
	e := s.Entries[0]
	if e.Line != `{"level":"error","code":500,"msg":"boom"}` {
		t.Fatalf("line = %q; parser stages must not rewrite the line", e.Line)
	}
	parsed := e.Parsed.Map()
	if parsed["level"] != "error" || parsed["code"] != "500" || parsed["msg"] != "boom" {
		t.Fatalf("parsed = %v", parsed)
	}
	if got := e.Meta.Map()["trace"]; got != "t1" {
		t.Fatalf("meta trace = %q, want t1", got)
	}
	if _, ok := e.Meta.Map()["level"]; ok {
		t.Fatal("json extracted a label into Meta; it belongs to Parsed")
	}
}

func TestEngineQueryNamedPathAndTypedFilter(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		logRow(base, `{"status":500,"path":"/a"}`, logs.Label{Name: "app", Value: "api"}),
		logRow(base.Add(time.Second), `{"status":200,"path":"/b"}`, logs.Label{Name: "app", Value: "api"}),
		logRow(base.Add(2*time.Second), `{"status":"bad","path":"/c"}`, logs.Label{Name: "app", Value: "api"}),
	)
	res := mustQuery(t, testEngine(), src, testParams(
		`{app="api"} | json code="status" | code >= 500`, base, base.Add(time.Minute)))
	lines := resultLines(res)
	if len(lines) != 2 {
		t.Fatalf("lines = %v, want the numeric 500 row and the conversion-error row", lines)
	}
	if lines[0] != `{"status":"bad","path":"/c"}` || lines[1] != `{"status":500,"path":"/a"}` {
		t.Fatalf("lines = %v, want backward order with the errored row kept", lines)
	}
	var sawErr bool
	for _, s := range res.Streams {
		for _, e := range s.Entries {
			if _, ok := e.Parsed.Get(errorLabel); ok {
				sawErr = true
			}
		}
	}
	if !sawErr {
		t.Fatal("conversion-error row has no __error__ label")
	}
}

func TestEngineQueryParserForms(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name  string
		line  string
		query string
		want  map[string]string
	}{
		{
			name:  "logfmt",
			line:  `level=error code=500 msg="boom now"`,
			query: `{app="api"} | logfmt | code > 400`,
			want:  map[string]string{"level": "error", "code": "500", "msg": "boom now"},
		},
		{
			name:  "regexp",
			line:  `level=error code=500`,
			query: `{app="api"} | regexp "level=(?P<lvl>\\w+) code=(?P<code>\\d+)" | code >= 500`,
			want:  map[string]string{"lvl": "error", "code": "500"},
		},
		{
			name:  "pattern",
			line:  `level=error code=500`,
			query: `{app="api"} | pattern "level=<lvl> code=<code>"`,
			want:  map[string]string{"lvl": "error", "code": "500"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := plan.NewMemSource()
			src.Add(tenant.Default, appRow(base, tt.line))
			res := mustQuery(t, testEngine(), src, testParams(tt.query, base, base.Add(time.Minute)))
			lines := resultLines(res)
			if len(lines) != 1 || lines[0] != tt.line {
				t.Fatalf("lines = %v, want the original line once", lines)
			}
			got := res.Streams[0].Entries[0].Parsed.Map()
			for k, want := range tt.want {
				if got[k] != want {
					t.Fatalf("parsed[%q] = %q, want %q (all %v)", k, got[k], want, got)
				}
			}
		})
	}
}

func TestEngineQueryParserErrorsAreFilterable(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(base, `not json`))

	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | json`, base, base.Add(time.Minute)))
	if lines := resultLines(res); len(lines) != 1 {
		t.Fatalf("parser failure dropped the row: lines = %v", lines)
	}
	parsed := res.Streams[0].Entries[0].Parsed.Map()
	if parsed[errorLabel] != jsonParserErr {
		t.Fatalf("__error__ = %q, want %q", parsed[errorLabel], jsonParserErr)
	}

	res = mustQuery(t, testEngine(), src, testParams(`{app="api"} | json | __error__ != ""`, base, base.Add(time.Minute)))
	if lines := resultLines(res); len(lines) != 1 {
		t.Fatalf("error filter dropped an errored row: lines = %v", lines)
	}
	res = mustQuery(t, testEngine(), src, testParams(`{app="api"} | json | __error__ = ""`, base, base.Add(time.Minute)))
	if lines := resultLines(res); len(lines) != 0 {
		t.Fatalf("empty-error filter kept an errored row: lines = %v", lines)
	}
}

func TestEngineQueryPipelineNeverPushesLimit(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{rows: []plan.Row{
		appRow(base, `{"level":"info"}`),
		appRow(base.Add(time.Second), `{"level":"error"}`),
		appRow(base.Add(2*time.Second), `{"level":"error"}`),
	}}
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | json | level="error"`, base, base.Add(time.Minute)))
	if got := len(resultLines(res)); got != 2 {
		t.Fatalf("lines = %d, want both errored rows despite limit 100", got)
	}
	sel := lastSelection(t, src)
	if sel.Limit != 0 {
		t.Fatalf("Selection.Limit = %d, want 0: a pipeline can drop rows", sel.Limit)
	}
	if len(sel.LineFilters) != 0 {
		t.Fatalf("Selection.LineFilters = %v, want none", sel.LineFilters)
	}
}

func TestEngineQueryLineFilterBeforeParserIsPushedDown(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{rows: []plan.Row{appRow(base, `{"level":"error"}`)}}
	mustQuery(t, testEngine(), src, testParams(`{app="api"} |= "error" | json | level="error"`, base, base.Add(time.Minute)))
	sel := lastSelection(t, src)
	if len(sel.LineFilters) != 1 || sel.LineFilters[0].Value != "error" {
		t.Fatalf("LineFilters = %v, want the pre-parser contains filter", sel.LineFilters)
	}
	if sel.Limit != 0 {
		t.Fatalf("Selection.Limit = %d, want 0", sel.Limit)
	}
}

func TestEngineQueryMutatingSourceKeepsParsedRows(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{
		mutate: true,
		rows: []plan.Row{
			appRow(base, `{"level":"error","n":1}`),
			appRow(base.Add(time.Second), `{"level":"error","n":2}`),
		},
	}
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | json`, base, base.Add(time.Minute)))
	if got := len(resultLines(res)); got != 2 {
		t.Fatalf("lines = %d, want 2", got)
	}
	wantN := map[string]string{
		"{\"level\":\"error\",\"n\":1}": "1",
		"{\"level\":\"error\",\"n\":2}": "2",
	}
	for _, s := range res.Streams {
		for _, e := range s.Entries {
			parsed := e.Parsed.Map()
			if parsed["level"] != "error" {
				t.Fatalf("parsed level = %q, want error (aliased source row leaked)", parsed["level"])
			}
			if got, want := parsed["n"], wantN[e.Line]; got != want {
				t.Fatalf("line %q parsed n = %q, want %q (aliased row)", e.Line, got, want)
			}
		}
	}
}

func TestCompiledLogQueryProcessTail(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	c, err := testEngine().CompileLogQuery(`{app="api"} | json | level="error"`)
	if err != nil {
		t.Fatal(err)
	}
	row := plan.Row{
		TS:     base,
		Line:   `{"level":"error","n":1}`,
		Labels: labelPairs("app", "api"),
		Meta:   labelPairs("trace", "t1"),
	}
	before := slices.Clone(row.Labels)
	labels, entry, keep, err := c.Process(t.Context(), tenant.Default, row)
	if err != nil || !keep {
		t.Fatalf("Process = keep %v, err %v; want kept", keep, err)
	}
	if got := labels.String(); got != `{app="api"}` {
		t.Fatalf("labels = %s", got)
	}
	if entry.Parsed.Map()["level"] != "error" || entry.Line != row.Line || entry.Meta.Map()["trace"] != "t1" {
		t.Fatalf("entry = %+v", entry)
	}
	if !slices.Equal(row.Labels, before) {
		t.Fatalf("Process mutated source Labels: %v -> %v", before, row.Labels)
	}

	// The pipeline filter rejects an info row.
	_, _, keep, err = c.Process(t.Context(), tenant.Default, plan.Row{
		TS: base, Line: `{"level":"info"}`, Labels: labelPairs("app", "api"),
	})
	if err != nil || keep {
		t.Fatalf("Process info row = keep %v, err %v; want dropped", keep, err)
	}

	// The selector rejects another app without running the pipeline.
	_, _, keep, err = c.Process(t.Context(), tenant.Default, plan.Row{
		TS: base, Line: `{"level":"error"}`, Labels: labelPairs("app", "other"),
	})
	if err != nil || keep {
		t.Fatalf("Process other-app row = keep %v, err %v; want dropped", keep, err)
	}
}

func TestCompiledLogQueryParserStagesConcurrentProcess(t *testing.T) {
	c, err := testEngine().CompileLogQuery(`{app="api"} | json code="status" | code >= 500`)
	if err != nil {
		t.Fatal(err)
	}
	rows := []plan.Row{
		{Line: `{"status":500}`, Labels: labelPairs("app", "api")},
		{Line: `{"status":200}`, Labels: labelPairs("app", "api")},
		{Line: `{"status":"bad"}`, Labels: labelPairs("app", "api")},
		{Line: `not json`, Labels: labelPairs("app", "api")},
	}
	wantKeep := []bool{true, false, true, false}
	var wg sync.WaitGroup
	errs := make(chan string, len(rows)*8)
	for range 8 {
		for i, row := range rows {
			wg.Go(func() {
				labels, entry, keep, err := c.Process(t.Context(), tenant.Default, row)
				if err != nil {
					errs <- err.Error()
					return
				}
				if keep != wantKeep[i] {
					errs <- fmt.Sprintf("row %d: keep = %v, want %v", i, keep, wantKeep[i])
					return
				}
				if keep && labels.String() != `{app="api"}` {
					errs <- fmt.Sprintf("row %d: labels = %s", i, labels)
					return
				}
				if keep && i == 0 && entry.Parsed.Map()["code"] != "500" {
					errs <- fmt.Sprintf("row %d: parsed = %v", i, entry.Parsed)
				}
			})
		}
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Error(msg)
	}
}

func TestEntryParsedAppendsErrorState(t *testing.T) {
	parsed := labelPairs("a", "1", errorLabel, "user", errorDetailsLabel, "old")
	got := entryParsed(parsed, jsonParserErr, "boom")
	want := labelPairs("a", "1", errorLabel, jsonParserErr, errorDetailsLabel, "boom")
	if !slices.Equal(got, want) {
		t.Fatalf("entryParsed = %v, want %v", got, want)
	}
	// Without state the parsed slice is returned as-is.
	if got := entryParsed(parsed, "", ""); len(got) != len(parsed) || got[0] != parsed[0] {
		t.Fatalf("entryParsed without state changed the labels: %v", got)
	}
}

func TestEngineQueryParsedErrorFieldVsState(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// The parsed field named __error__ is the numeric 5; the malformed x member
	// also sets the builder state to JSONParserErr.
	const line = `{"__error__":5,"x":}`
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(base, line))
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | json`, base, base.Add(time.Minute)))
	parsed := res.Streams[0].Entries[0].Parsed.Map()
	// The state is appended last, so it owns the duplicate name in the output.
	if parsed[errorLabel] != jsonParserErr {
		t.Fatalf("output __error__ = %q, want the state %q", parsed[errorLabel], jsonParserErr)
	}
	if parsed[errorDetailsLabel] == "" {
		t.Fatal("output lost the error details")
	}

	// Get must expose the parsed numeric 5, not the unparsable state string:
	// >10 drops, >1 keeps. If the filter read the state both would keep the row
	// with a conversion error.
	res = mustQuery(t, testEngine(), src, testParams(
		`{app="api"} | json | __error__ > 10`, base, base.Add(time.Minute)))
	if lines := resultLines(res); len(lines) != 0 {
		t.Fatalf("__error__ > 10 kept %d rows; the filter read the state instead of the parsed 5", len(lines))
	}
	res = mustQuery(t, testEngine(), src, testParams(
		`{app="api"} | json | __error__ > 1`, base, base.Add(time.Minute)))
	if lines := resultLines(res); len(lines) != 1 {
		t.Fatalf("__error__ > 1 kept %d rows, want 1", len(lines))
	}
}
