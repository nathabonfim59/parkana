package logql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// formatTS is on the microsecond grid, so direct Process calls and MemSource
// agree on the timestamp templates see.
var formatTS = time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

// formatRow is the row the formatting tests run over unless a test needs
// different labels.
func formatRow(line string) plan.Row {
	return logRow(formatTS, line,
		logs.Label{Name: "app", Value: "api"},
		logs.Label{Name: "level", Value: "warn"},
	)
}

// lineQuery builds a log query with one line_format stage whose template is
// quoted exactly as a LogQL string literal.
func lineQuery(tmpl string) string {
	return `{app="api"} | line_format ` + strconv.Quote(tmpl)
}

// mustFormatQuery compiles a formatting query with the shared test engine.
func mustFormatQuery(t *testing.T, q string) *CompiledLogQuery {
	t.Helper()
	c, err := testEngine().CompileLogQuery(q)
	if err != nil {
		t.Fatalf("CompileLogQuery(%q): %v", q, err)
	}
	return c
}

// runProcess applies a compiled query to one row through the frozen Process API.
func runProcess(t *testing.T, c *CompiledLogQuery, r plan.Row) (logs.Labels, plan.Entry, bool) {
	t.Helper()
	labels, entry, keep, err := c.Process(t.Context(), tenant.Default, r)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	return labels, entry, keep
}

// formattedLine runs one line_format template over the standard row.
func formattedLine(t *testing.T, tmpl, line string) string {
	t.Helper()
	_, entry, keep := runProcess(t, mustFormatQuery(t, lineQuery(tmpl)), formatRow(line))
	if !keep {
		t.Fatalf("line_format %q dropped the row", tmpl)
	}
	return entry.Line
}

// labelQuery builds a label_format stage from pre-quoted assignment text.
func labelQuery(assignments string) string {
	return `{app="api"} | label_format ` + assignments
}

// TestLineFormatHelpers exercises every helper the allowlist claims, including
// the argument order difference between Loki's uppercase strings helpers
// (subject first) and the sprig helpers (operand first).
func TestLineFormatHelpers(t *testing.T) {
	tests := []struct {
		name, tmpl, want string
	}{
		{"line function", `{{ __line__ }}`, "Hello World 42"},
		{"timestamp unix nanos", `{{ __timestamp__.UnixNano }}`, fmt.Sprint(formatTS.UnixNano())},
		{"timestamp renders like Loki", `{{ __timestamp__ }}`, time.Unix(0, formatTS.UnixNano()).String()},
		{"ToLower", `{{ ToLower "ABC" }}`, "abc"},
		{"ToLower pipeline", `{{ .app | ToLower }}`, "api"},
		{"ToUpper", `{{ "abc" | ToUpper }}`, "ABC"},
		{"TrimSpace", `{{ TrimSpace "  x  " }}`, "x"},
		{"Trim", `{{ Trim "xxhixx" "x" }}`, "hi"},
		{"TrimLeft", `{{ TrimLeft "xxhi" "x" }}`, "hi"},
		{"TrimRight", `{{ TrimRight "hixx" "x" }}`, "hi"},
		{"TrimPrefix subject first", `{{ TrimPrefix __line__ "Hello" }}`, " World 42"},
		{"TrimSuffix subject first", `{{ TrimSuffix __line__ "42" }}`, "Hello World "},
		{"Replace", `{{ Replace "hello" "l" "L" -1 }}`, "heLLo"},
		{"lower sprig", `{{ lower "ABC" }}`, "abc"},
		{"upper sprig", `{{ upper "abc" }}`, "ABC"},
		{"contains operand first", `{{ if contains "World" __line__ }}yes{{ else }}no{{ end }}`, "yes"},
		{"hasPrefix operand first", `{{ if hasPrefix "Hello" __line__ }}yes{{ else }}no{{ end }}`, "yes"},
		{"hasSuffix operand first", `{{ if hasSuffix "42" __line__ }}yes{{ else }}no{{ end }}`, "yes"},
		{"trim trims spaces", `{{ trim "  x  " }}`, "x"},
		{"trimPrefix operand first", `{{ trimPrefix "Hello" __line__ }}`, " World 42"},
		{"trimSuffix operand first", `{{ trimSuffix "42" __line__ }}`, "Hello World "},
		{"replace sprig", `{{ replace "l" "L" "hello" }}`, "heLLo"},
		{"default missing", `{{ default "fallback" .missing }}`, "fallback"},
		{"default present", `{{ default "fallback" .app }}`, "api"},
		{"default pipeline", `{{ .missing | default "fallback" }}`, "fallback"},
		{"regexReplaceAll", `{{ regexReplaceAll "a(x*)b" "-ab-axxb-" "${1}W" }}`, "-W-xxW-"},
		{"regexReplaceAllLiteral", `{{ regexReplaceAllLiteral "a(x*)b" "-ab-axxb-" "${1}W" }}`, "-${1}W-${1}W-"},
		{"count", `{{ count "o" "foo" }}`, "2"},
		{"urlencode", `{{ urlencode "a b&c" }}`, "a+b%26c"},
		{"urldecode", `{{ urldecode "a+b%26c" }}`, "a b&c"},
		{"bytes decimal unit", `{{ "1MB" | bytes }}`, "1e+06"},
		{"bytes binary unit", `{{ bytes "1KiB" }}`, "1024"},
		{"bytes bare number", `{{ bytes "1024" }}`, "1024"},
		{"bytes comma number", `{{ bytes "1,024" }}`, "1024"},
		{"duration", `{{ duration "1h30m" }}`, "5400"},
		{"duration_seconds", `{{ duration_seconds "1.5s" }}`, "1.5"},
		{"printf builtin", `{{ printf "%s-%s" .app __line__ }}`, "api-Hello World 42"},
		{"eq builtin", `{{ if and .app (eq .app "api") }}ok{{ end }}`, "ok"},
		{"defined template", `{{ define "x" }}{{ .app }}{{ end }}{{ template "x" . }}`, "api"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formattedLine(t, tt.tmpl, "Hello World 42"); got != tt.want {
				t.Fatalf("line_format %q = %q, want %q", tt.tmpl, got, tt.want)
			}
		})
	}
}

// TestLineFormatMissingField pins both text/template paths: the simple-key
// fast path returns an empty line, the general path uses missingkey=zero.
func TestLineFormatMissingField(t *testing.T) {
	if got := formattedLine(t, `{{.missing}}`, "orig"); got != "" {
		t.Fatalf("simple missing field = %q, want empty", got)
	}
	if got := formattedLine(t, `x{{.missing}}y`, "orig"); got != "xy" {
		t.Fatalf("general missing field = %q, want %q", got, "xy")
	}

	c := mustFormatQuery(t, lineQuery(`{{.missing}}`))
	stage, ok := c.stages[0].(*lineFormatStage)
	if !ok {
		t.Fatalf("stage = %T, want *lineFormatStage", c.stages[0])
	}
	if stage.simpleKey != "missing" {
		t.Fatalf("simpleKey = %q, want %q", stage.simpleKey, "missing")
	}
	c = mustFormatQuery(t, lineQuery(`x{{.missing}}y`))
	stage = c.stages[0].(*lineFormatStage)
	if stage.simpleKey != "" {
		t.Fatalf("simpleKey = %q, want none for a general template", stage.simpleKey)
	}
}

// TestLineFormatSimpleKeyCopiesVerbatim proves the fast path copies a label
// value without re-parsing it as a template.
func TestLineFormatSimpleKeyCopiesVerbatim(t *testing.T) {
	row := logRow(formatTS, "orig",
		logs.Label{Name: "app", Value: "api"},
		logs.Label{Name: "tpl", Value: "{{.app}}"},
	)
	_, entry, keep := runProcess(t, mustFormatQuery(t, lineQuery(`{{.tpl}}`)), row)
	if !keep {
		t.Fatal("row dropped")
	}
	if entry.Line != "{{.app}}" {
		t.Fatalf("line = %q, want the literal label value", entry.Line)
	}
}

// TestLineFormatSeesCurrentLineAndTimestamp covers chained formatting: the
// second stage must see the first stage's output line and the row timestamp.
func TestLineFormatSeesCurrentLineAndTimestamp(t *testing.T) {
	q := `{app="api"} | line_format "L={{.app}}" | line_format ` +
		strconv.Quote(`{{__line__}}@{{__timestamp__.UnixNano}}`)
	_, entry, keep := runProcess(t, mustFormatQuery(t, q), formatRow("orig"))
	if !keep {
		t.Fatal("row dropped")
	}
	want := "L=api@" + fmt.Sprint(formatTS.UnixNano())
	if entry.Line != want {
		t.Fatalf("line = %q, want %q", entry.Line, want)
	}
}

// TestLineFormatRuntimeError pins Loki's failure behavior: the original line is
// kept, TemplateFormatErr is recorded, and the row survives so later stages run.
func TestLineFormatRuntimeError(t *testing.T) {
	q := `{app="api"} | line_format "{{.app.nope}}" | label_format later="{{.app}}"`
	labels, entry, keep := runProcess(t, mustFormatQuery(t, q), formatRow("orig"))
	if !keep {
		t.Fatal("template failure dropped the row")
	}
	if entry.Line != "orig" {
		t.Fatalf("line = %q, want the original line", entry.Line)
	}
	if got, _ := entry.Parsed.Get(errorLabel); got != templateFormatErr {
		t.Fatalf("__error__ = %q, want %q", got, templateFormatErr)
	}
	if details, _ := entry.Parsed.Get(errorDetailsLabel); details == "" {
		t.Fatal("__error_details__ is empty, want the template error text")
	}
	// The entry's stream labels are unchanged; the later label template ran.
	if got, _ := labels.Get("app"); got != "api" {
		t.Fatalf("app = %q, want api", got)
	}
	if got, _ := entry.Parsed.Get("later"); got != "api" {
		t.Fatalf("later = %q, want api", got)
	}
}

// TestLineFormatBarrierBlocksPushdown proves a line filter after line_format is
// not pushed into the Selection: the stored line does not match, but the
// formatted line does, so the row must survive. A pushed filter would drop it
// before the pipeline ever runs.
func TestLineFormatBarrierBlocksPushdown(t *testing.T) {
	src := plan.NewMemSource()
	src.Add(tenant.Default, appRow(formatTS, "original"))
	e := testEngine()

	q := `{app="api"} | line_format "{{.app}}" |= "api"`
	c := mustFormatQuery(t, q)
	if len(c.pushdown) != 0 {
		t.Fatalf("pushdown = %v, want none after line_format", c.pushdown)
	}
	res := mustQuery(t, e, src, testParams(q, formatTS, formatTS.Add(time.Second)))
	checkLines(t, resultLines(res), []string{"api"})

	// A filter before the barrier is still pushed down: that one sees the
	// stored line and failing it legitimately excludes the row.
	q = `{app="api"} |= "original" | line_format "{{.app}}"`
	c = mustFormatQuery(t, q)
	if len(c.pushdown) != 1 {
		t.Fatalf("pushdown = %v, want the pre-barrier filter", c.pushdown)
	}
	if c.exact {
		t.Fatal("exact = true after line_format, want false so no Source limit")
	}
}

// TestLabelFormatRenameCategories checks a rename moves the value into the
// parsed category, removes the mutable copy, and leaves the immutable base
// bookkeeping alone.
func TestLabelFormatRenameCategories(t *testing.T) {
	e := newTestEntry("line", labelPairs("app", "api", "level", "warn"), labelPairs("trace", "t1"))
	stages := mustStages(t, labelQuery(`lvl=level, tr=trace, absent=nope`))
	if !mustRun(t, stages, e) {
		t.Fatal("label_format dropped the row")
	}
	if _, ok := e.labels.Get("level"); ok {
		t.Fatal("level still in stream labels after rename")
	}
	if _, ok := e.meta.Get("trace"); ok {
		t.Fatal("trace still in metadata after rename")
	}
	if v, ok := e.parsed.Get("lvl"); !ok || v != "warn" {
		t.Fatalf("parsed lvl = %q/%v, want warn", v, ok)
	}
	if v, ok := e.parsed.Get("tr"); !ok || v != "t1" {
		t.Fatalf("parsed tr = %q/%v, want t1", v, ok)
	}
	if !e.baseHas("level") {
		t.Fatal("immutable base lost level, breaking BaseHas bookkeeping")
	}
	if _, ok := e.parsed.Get("absent"); ok {
		t.Fatal("a missing rename source must not create the destination")
	}
}

// TestLabelFormatSameNameRename pins Loki's Set(Parsed)+Del order: renaming a
// label to itself removes it.
func TestLabelFormatSameNameRename(t *testing.T) {
	e := newTestEntry("line", labelPairs("app", "api", "level", "warn"), nil)
	mustRun(t, mustStages(t, labelQuery(`level=level`)), e)
	if _, ok := e.labels.Get("level"); ok {
		t.Fatal("level still in stream labels")
	}
	if _, ok := e.parsed.Get("level"); ok {
		t.Fatal("level still in parsed labels; same-name rename must delete it")
	}
}

// TestLabelFormatTemplateAndEmptyValue covers template assignments, including
// the distinction between an empty template and a rename.
func TestLabelFormatTemplateAndEmptyValue(t *testing.T) {
	q := labelQuery(fmt.Sprintf(`msg=%s, gone=%s, copy=app`,
		strconv.Quote(`{{.app}}/{{__line__}}`), strconv.Quote("")))
	_, entry, keep := runProcess(t, mustFormatQuery(t, q), formatRow("orig"))
	if !keep {
		t.Fatal("row dropped")
	}
	if v, ok := entry.Parsed.Get("msg"); !ok || v != "api/orig" {
		t.Fatalf("msg = %q/%v, want api/orig", v, ok)
	}
	if v, ok := entry.Parsed.Get("gone"); !ok || v != "" {
		t.Fatalf("gone = %q/%v, want a present empty label", v, ok)
	}
	if v, ok := entry.Parsed.Get("copy"); !ok || v != "api" {
		t.Fatalf("copy = %q/%v, want api", v, ok)
	}
}

// TestLabelFormatLazySnapshot pins Loki's per-stage label-map snapshot: renames
// before the first template are visible, later writes are not refreshed.
func TestLabelFormatLazySnapshot(t *testing.T) {
	// Rename first: the snapshot only sees the post-rename state.
	e := newTestEntry("line", labelPairs("x", "old", "y", "orig"), nil)
	mustRun(t, mustStages(t, labelQuery(`a=y, got=`+strconv.Quote(`{{.a}}`))), e)
	if v, ok := e.parsed.Get("a"); !ok || v != "orig" {
		t.Fatalf("a = %q/%v, want orig", v, ok)
	}
	if v, ok := e.parsed.Get("got"); !ok || v != "orig" {
		t.Fatalf("got = %q/%v, want orig: the rename happened before the snapshot", v, ok)
	}

	// Template first, rename after: the snapshot keeps the pre-rename value.
	e = newTestEntry("line", labelPairs("x", "old", "y", "orig"), nil)
	mustRun(t, mustStages(t, labelQuery(
		`first=`+strconv.Quote(`{{.y}}`)+`, a=y, second=`+strconv.Quote(`{{.y}}`))), e)
	for _, name := range []string{"first", "second"} {
		if v, ok := e.parsed.Get(name); !ok || v != "orig" {
			t.Fatalf("%s = %q/%v, want orig from the cached snapshot", name, v, ok)
		}
	}
	if v, ok := e.parsed.Get("a"); !ok || v != "orig" {
		t.Fatalf("parsed a = %q/%v, want orig", v, ok)
	}
}

// TestLabelFormatTemplateError pins the failure path: the destination is left
// untouched, TemplateFormatErr is set, and later assignments still run.
func TestLabelFormatTemplateError(t *testing.T) {
	q := labelQuery(`bad=` + strconv.Quote(`{{.app | regexReplaceAll "[" "x"}}`) +
		`, good=` + strconv.Quote(`{{.app}}`))
	c := mustFormatQuery(t, q)
	_, entry, keep := runProcess(t, c, formatRow("orig"))
	if !keep {
		t.Fatal("template failure dropped the row")
	}
	if _, ok := entry.Parsed.Get("bad"); ok {
		t.Fatal("failed template wrote its destination")
	}
	if got, _ := entry.Parsed.Get(errorLabel); got != templateFormatErr {
		t.Fatalf("__error__ = %q, want %q", got, templateFormatErr)
	}
	if details, _ := entry.Parsed.Get(errorDetailsLabel); details == "" {
		t.Fatal("__error_details__ is empty, want the regexp error")
	}
	if v, ok := entry.Parsed.Get("good"); !ok || v != "api" {
		t.Fatalf("good = %q/%v, want api: later assignments must still run", v, ok)
	}
}

// TestLabelFormatValidation pins the compile-time rejections: duplicate
// destinations and __error__.
func TestLabelFormatValidation(t *testing.T) {
	tests := []struct {
		name, q string
		want    error
	}{
		{"duplicate rename", labelQuery(`a=x, a=y`), plan.ErrBadQuery},
		{"duplicate template", labelQuery(`a=` + strconv.Quote(`{{.x}}`) + `, a=y`), plan.ErrBadQuery},
		{"format error label", labelQuery(`__error__=` + strconv.Quote(`{{.x}}`)), plan.ErrBadQuery},
		{"rename error label", labelQuery(`__error__=x`), plan.ErrBadQuery},
		{"unknown helper", lineQuery(`{{ now }}`), plan.ErrUnsupported},
		{"deferred helper", lineQuery(`{{ .app | alignLeft 5 }}`), plan.ErrUnsupported},
		{"unsupported division", lineQuery(`{{ div 1 2 }}`), plan.ErrUnsupported},
		{"syntax error", lineQuery(`{{`), plan.ErrBadQuery},
		{"label syntax error", labelQuery(`a=` + strconv.Quote(`{{`)), plan.ErrBadQuery},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{}
			_, err := testEngine().Query(t.Context(), src, testParams(tt.q, formatTS, formatTS.Add(time.Second)))
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if src.selectCalls != 0 {
				t.Fatalf("Source.Select called %d times for a rejected query", src.selectCalls)
			}
		})
	}
}

// TestLabelFormatErrorStateInMap pins IntoMap ordering: the builder error state
// is written after the label categories, so it wins, while a parsed label with
// the same name stands on its own when no error is set.
func TestLabelFormatErrorStateInMap(t *testing.T) {
	e := newTestEntry("line", labelPairs("app", "api"), nil)
	e.setParsed(errorLabel, "parsed-error")
	e.setErr("ParserErr")
	e.setErrorDetails("boom")
	stages := mustStages(t, labelQuery(
		`seen=`+strconv.Quote(`{{.__error__}}`)+`, det=`+strconv.Quote(`{{.__error_details__}}`)))
	mustRun(t, stages, e)
	if v, ok := e.parsed.Get("seen"); !ok || v != "ParserErr" {
		t.Fatalf("seen = %q/%v, want the builder error state", v, ok)
	}
	if v, ok := e.parsed.Get("det"); !ok || v != "boom" {
		t.Fatalf("det = %q/%v, want the builder error details", v, ok)
	}

	// Without error state the parsed field named __error__ is an ordinary label.
	e = newTestEntry("line", labelPairs("app", "api"), nil)
	e.setParsed(errorLabel, "parsed-error")
	mustRun(t, mustStages(t, labelQuery(`seen=`+strconv.Quote(`{{.__error__}}`))), e)
	if v, ok := e.parsed.Get("seen"); !ok || v != "parsed-error" {
		t.Fatalf("seen = %q/%v, want the parsed field", v, ok)
	}
}

// TestLabelFormatSeesFormattedLineAndTimestamp covers the second formatter's
// inputs: the line a previous line_format produced and the row timestamp.
func TestLabelFormatSeesFormattedLineAndTimestamp(t *testing.T) {
	q := `{app="api"} | line_format "L={{.app}}" | label_format ` +
		`seen=` + strconv.Quote(`{{__line__}}`) + `, at=` + strconv.Quote(`{{__timestamp__.UnixNano}}`)
	_, entry, keep := runProcess(t, mustFormatQuery(t, q), formatRow("orig"))
	if !keep {
		t.Fatal("row dropped")
	}
	if v, _ := entry.Parsed.Get("seen"); v != "L=api" {
		t.Fatalf("seen = %q, want L=api", v)
	}
	if v, _ := entry.Parsed.Get("at"); v != fmt.Sprint(formatTS.UnixNano()) {
		t.Fatalf("at = %q, want %d", v, formatTS.UnixNano())
	}
}

// TestFormatProcessDoesNotMutateRow proves Process works on clones: formatting
// stages rename and delete labels but the source-owned slices stay intact.
func TestFormatProcessDoesNotMutateRow(t *testing.T) {
	labels := labelPairs("app", "api", "level", "warn")
	meta := labelPairs("trace", "t1")
	wantLabels := slices.Clone(labels)
	wantMeta := slices.Clone(meta)
	row := plan.Row{TS: formatTS, Line: "orig", Labels: labels, Meta: meta}
	q := `{app="api"} | line_format "{{.trace}}" | label_format lvl=level, m="{{.app}}"`
	runProcess(t, mustFormatQuery(t, q), row)
	if !slices.Equal(labels, wantLabels) {
		t.Fatalf("row labels mutated: %v, want %v", labels, wantLabels)
	}
	if !slices.Equal(meta, wantMeta) {
		t.Fatalf("row meta mutated: %v, want %v", meta, wantMeta)
	}
}

// TestLabelFormatEmptyMapRefill pins Loki's lazy `if len(m) == 0` snapshot:
// when the entry has no labels at all the map stays empty, so a later template
// re-populates it and sees an assignment made by an earlier template.
func TestLabelFormatEmptyMapRefill(t *testing.T) {
	e := newTestEntry("line", nil, nil)
	stages := mustStages(t, labelQuery(
		`a=`+strconv.Quote("first")+`, b=`+strconv.Quote(`{{.a}}`)))
	if !mustRun(t, stages, e) {
		t.Fatal("label_format dropped the row")
	}
	if v, ok := e.parsed.Get("a"); !ok || v != "first" {
		t.Fatalf("a = %q/%v, want first", v, ok)
	}
	if v, ok := e.parsed.Get("b"); !ok || v != "first" {
		t.Fatalf("b = %q/%v, want first: an empty snapshot must be refilled", v, ok)
	}
}

// TestLabelFormatErrorKeepsExistingDestination proves a failed template
// assignment leaves a destination that already had a value untouched.
func TestLabelFormatErrorKeepsExistingDestination(t *testing.T) {
	e := newTestEntry("line", labelPairs("app", "api"), nil)
	e.setParsed("msg", "keep me")
	stages := mustStages(t, labelQuery(
		`msg=`+strconv.Quote(`{{.app | regexReplaceAll "[" "x"}}`)))
	if !mustRun(t, stages, e) {
		t.Fatal("template failure dropped the row")
	}
	if v, ok := e.parsed.Get("msg"); !ok || v != "keep me" {
		t.Fatalf("msg = %q/%v, want the prior value", v, ok)
	}
	if got := e.errValue(); got != templateFormatErr {
		t.Fatalf("__error__ = %q, want %q", got, templateFormatErr)
	}
}

// TestFormatStageCancellationBothKinds calls each stage's process directly with
// a canceled context: the public Process stops before the pipeline, so this is
// the only path that reaches the stage-level checks.
func TestFormatStageCancellationBothKinds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, q := range []string{
		`{app="api"} | line_format "{{.app}}"`,
		`{app="api"} | label_format a=` + strconv.Quote(`{{.app}}`),
	} {
		stages := mustStages(t, q)
		e := newTestEntry("line", labelPairs("app", "api"), nil)
		_, err := stages[0].process(ctx, e)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: err = %v, want context.Canceled", q, err)
		}
		if e.line != "line" {
			t.Fatalf("%s: line = %q, want the original line", q, e.line)
		}
	}
}

// TestFormatConcurrentProcess runs one compiled query from many goroutines; the
// race detector must see no shared mutable template or binding state. Lines and
// timestamps vary so a leaked __line__/__timestamp__ binding would show up.
func TestFormatConcurrentProcess(t *testing.T) {
	q := `{app="api"} | line_format "{{.app}}-{{__line__}}@{{__timestamp__.UnixNano}}" | label_format ` +
		`up=` + strconv.Quote(`{{.app | upper}}`)
	c := mustFormatQuery(t, q)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 50 {
				ts := formatTS.Add(time.Duration(g*1000+i) * time.Microsecond)
				line := fmt.Sprintf("g%d-line%d", g, i)
				row := logRow(ts, line, logs.Label{Name: "app", Value: "api"})
				_, entry, keep, err := c.Process(t.Context(), tenant.Default, row)
				if err != nil {
					t.Errorf("Process: %v", err)
					return
				}
				if !keep {
					t.Errorf("row %q dropped", line)
					return
				}
				want := fmt.Sprintf("api-%s@%d", line, ts.UnixNano())
				if entry.Line != want {
					t.Errorf("line = %q, want %q", entry.Line, want)
					return
				}
				if v, _ := entry.Parsed.Get("up"); v != "API" {
					t.Errorf("up = %q, want API", v)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestFormatProcessCancellation pins that a canceled context is reported and
// no formatting runs.
func TestFormatProcessCancellation(t *testing.T) {
	c := mustFormatQuery(t, `{app="api"} | line_format "{{.app}}"`)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, entry, _, err := c.Process(ctx, tenant.Default, formatRow("orig"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if entry.Line != "" {
		t.Fatalf("line = %q, want no entry", entry.Line)
	}
}

// TestFormatDefinesAndNesting proves the re-bound template carries every
// associated define tree, including nested template invocations.
func TestFormatDefinesAndNesting(t *testing.T) {
	tmpl := `{{ define "a" }}{{ .app }}{{ end }}` +
		`{{ define "b" }}[{{ template "a" . }}]{{ end }}` +
		`{{ template "b" . }}`
	if got := formattedLine(t, tmpl, "orig"); got != "[api]" {
		t.Fatalf("line = %q, want %q", got, "[api]")
	}
}

// TestFormatReservedFunctionsShadowLabels pins Loki's precedence: __line__ and
// __timestamp__ are functions, while a field reference still reads a label of
// that name.
func TestFormatReservedFunctionsShadowLabels(t *testing.T) {
	row := logRow(formatTS, "real line",
		logs.Label{Name: "app", Value: "api"},
		logs.Label{Name: "__line__", Value: "label line"},
	)
	_, entry, keep := runProcess(t, mustFormatQuery(t, lineQuery(`{{ __line__ }}`)), row)
	if !keep || entry.Line != "real line" {
		t.Fatalf("__line__ = %q (keep %v), want the real line", entry.Line, keep)
	}
	_, entry, keep = runProcess(t, mustFormatQuery(t, lineQuery(`{{ .__line__ }}`)), row)
	if !keep || entry.Line != "label line" {
		t.Fatalf(".__line__ = %q (keep %v), want the label", entry.Line, keep)
	}
}

// TestFormatDeferredHelperDoesNotMaskSyntaxError pins whole-template
// validation: an unknown function must not short-circuit parsing before a later
// syntax error is discovered. Only a syntax-valid template with a deferred
// helper is Unsupported.
func TestFormatDeferredHelperDoesNotMaskSyntaxError(t *testing.T) {
	syntaxErrors := []struct{ name, tmpl string }{
		{"unclosed action after helper", `{{ now }}{{`},
		{"unclosed action at EOF", `{{ now }`},
		{"misplaced end after helper", `{{ now }}{{ end }}`},
		{"unclosed if after helper", `{{ now }}{{ if .app }}`},
	}
	for _, tt := range syntaxErrors {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testEngine().CompileLogQuery(lineQuery(tt.tmpl))
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("CompileLogQuery(%q) err = %v, want ErrBadQuery", tt.tmpl, err)
			}
			if errors.Is(err, plan.ErrUnsupported) {
				t.Fatalf("CompileLogQuery(%q) err = %v, must not be ErrUnsupported", tt.tmpl, err)
			}
		})
	}

	t.Run("label_format", func(t *testing.T) {
		q := labelQuery(`a=` + strconv.Quote(`{{ now }}{{`))
		_, err := testEngine().CompileLogQuery(q)
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Fatalf("err = %v, want ErrBadQuery", err)
		}
	})

	// A well-formed deferred helper is still an explicit Unsupported.
	for _, tmpl := range []string{`{{ now }}`, `{{ now }}{{ .app }}`} {
		_, err := testEngine().CompileLogQuery(lineQuery(tmpl))
		if !errors.Is(err, plan.ErrUnsupported) {
			t.Fatalf("CompileLogQuery(%q) err = %v, want ErrUnsupported", tmpl, err)
		}
	}
}

// TestFormatSyntaxErrorIsNotUnsupported keeps the error classes apart: only a
// genuine undefined helper identifier is Unsupported.
func TestFormatSyntaxErrorIsNotUnsupported(t *testing.T) {
	for _, tmpl := range []string{`{{ ToLower .app `, `{{ if }}`, `{{ end }}`} {
		c := testEngine()
		_, err := c.CompileLogQuery(lineQuery(tmpl))
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Fatalf("CompileLogQuery(%q) err = %v, want ErrBadQuery", tmpl, err)
		}
		if errors.Is(err, plan.ErrUnsupported) {
			t.Fatalf("CompileLogQuery(%q) err = %v, must not be ErrUnsupported", tmpl, err)
		}
	}
}

// TestIsEmptyValue pins the sprig emptiness rules the default helper relies on.
func TestIsEmptyValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{"nil", nil, true},
		{"empty string", "", true},
		{"string", "x", false},
		{"false", false, true},
		{"true", true, false},
		{"zero int", 0, true},
		{"int", 7, false},
		{"zero uint", uint(0), true},
		{"uint", uint(7), false},
		{"zero float", 0.0, true},
		{"float", 1.5, false},
		{"empty slice", []string{}, true},
		{"slice", []string{"x"}, false},
		{"empty map", map[string]string{}, true},
		{"map", map[string]string{"a": "b"}, false},
		{"nil pointer", (*int)(nil), true},
		{"struct", struct{}{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isEmptyValue(tt.in); got != tt.want {
				t.Fatalf("isEmptyValue(%#v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
	if got := dfault("fallback", ""); got != "fallback" {
		t.Fatalf("dfault with empty given = %#v, want fallback", got)
	}
	if got := dfault("fallback"); got != "fallback" {
		t.Fatalf("dfault with no given = %#v, want fallback", got)
	}
}

// TestFormatHelperExecutionErrors pins that bad helper input is a runtime
// TemplateFormatErr, not a compile error: the row is retained and the original
// line or destination survives.
func TestFormatHelperExecutionErrors(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
	}{
		{"regexReplaceAll", `{{ regexReplaceAll "[" "x" "y" }}`},
		{"regexReplaceAllLiteral", `{{ regexReplaceAllLiteral "[" "x" "y" }}`},
		{"count", `{{ count "[" "x" }}`},
		{"duration", `{{ duration "soon" }}`},
		{"bytes", `{{ bytes "lots" }}`},
		{"field on string", `{{ .app.nope }}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, entry, keep := runProcess(t, mustFormatQuery(t, lineQuery(tt.tmpl)), formatRow("orig"))
			if !keep {
				t.Fatal("helper failure dropped the row")
			}
			if entry.Line != "orig" {
				t.Fatalf("line = %q, want the original line", entry.Line)
			}
			if got, _ := entry.Parsed.Get(errorLabel); got != templateFormatErr {
				t.Fatalf("__error__ = %q, want %q", got, templateFormatErr)
			}
		})
	}
}

// TestLabelFormatKeepsLine pins that label formatting never rewrites the line.
func TestLabelFormatKeepsLine(t *testing.T) {
	_, entry, keep := runProcess(t, mustFormatQuery(t, labelQuery(`a=`+strconv.Quote(`{{.app}}`))), formatRow("orig"))
	if !keep {
		t.Fatal("row dropped")
	}
	if entry.Line != "orig" {
		t.Fatalf("line = %q, want orig", entry.Line)
	}
}

// TestFormatRegroupsStreams proves grouping uses the post-pipeline stream
// labels: renaming the only differing stream label merges the streams while the
// per-entry parsed values stay distinct.
func TestFormatRegroupsStreams(t *testing.T) {
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		shardRow(formatTS, "a", "one"),
		shardRow(formatTS.Add(time.Microsecond), "b", "two"),
	)
	q := `{app="api"} | label_format s=shard`
	res := mustQuery(t, testEngine(), src, testParams(q, formatTS, formatTS.Add(time.Second)))
	if len(res.Streams) != 1 {
		t.Fatalf("streams = %d, want 1 after the rename", len(res.Streams))
	}
	s := res.Streams[0]
	if _, ok := s.Labels.Get("shard"); ok {
		t.Fatalf("stream labels %v still hold shard after the rename", s.Labels)
	}
	if len(s.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(s.Entries))
	}
	got := make(map[string]string, 2)
	for _, entry := range s.Entries {
		v, _ := entry.Parsed.Get("s")
		got[entry.Line] = v
	}
	want := map[string]string{"a": "one", "b": "two"}
	if len(got) != len(want) || got["a"] != want["a"] || got["b"] != want["b"] {
		t.Fatalf("parsed shard values = %v, want %v", got, want)
	}
}

// TestLabelFormatRequiredHelperErrorsAreCompileTime pins that a bad helper is
// reported even for label_format and never reaches the Source.
func TestLabelFormatRequiredHelperErrorsAreCompileTime(t *testing.T) {
	src := &fakeSource{}
	q := labelQuery(`a=` + strconv.Quote(`{{ .app | toDate "2006" }}`))
	_, err := testEngine().Query(t.Context(), src, testParams(q, formatTS, formatTS.Add(time.Second)))
	if !errors.Is(err, plan.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
	if src.selectCalls != 0 {
		t.Fatalf("Source.Select called %d times", src.selectCalls)
	}
}
