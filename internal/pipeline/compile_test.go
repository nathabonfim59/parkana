package pipeline_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
	. "github.com/nathabonfim59/parkana/internal/pipeline"
)

// goodDef is a definition that compiles; every rejection test mutates one
// thing about it.
func goodDef() Definition {
	return Definition{
		Name:     "app.json",
		Stage:    StageIngest,
		Selector: "app",
		Steps:    []Step{{Type: StepParseJSON}},
	}
}

func goodOpts() CompileOptions {
	return CompileOptions{StreamLabels: []string{"stream", "org"}}
}

// reject asserts Compile fails and the error mentions want.
func reject(t *testing.T, def Definition, opts CompileOptions, want string) {
	t.Helper()
	_, err := Compile(def, opts)
	if err == nil {
		t.Fatalf("Compile(%s) succeeded, want error mentioning %q", def.Name, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Compile(%s) error %q does not mention %q", def.Name, err, want)
	}
}

func TestDefinitionSyntax(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Definition)
		want string
	}{
		{"empty name", func(d *Definition) { d.Name = "" }, "invalid name"},
		{"dot name", func(d *Definition) { d.Name = "." }, `invalid name "."`},
		{"dotdot name", func(d *Definition) { d.Name = ".." }, `invalid name ".."`},
		{"long name", func(d *Definition) { d.Name = strings.Repeat("a", 129) }, "invalid name"},
		{"bad name char", func(d *Definition) { d.Name = "a b" }, "invalid name"},
		{"bad stage", func(d *Definition) { d.Stage = "both" }, "invalid stage"},
		{"unsanitized selector", func(d *Definition) { d.Selector = "Foo" }, "invalid selector"},
		{"empty selector", func(d *Definition) { d.Selector = "" }, "invalid selector"},
		{"slash selector", func(d *Definition) { d.Selector = "a/b" }, "invalid selector"},
		{"no steps", func(d *Definition) { d.Steps = nil }, "no steps"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := goodDef()
			tc.mut(&d)
			reject(t, d, goodOpts(), tc.want)
		})
	}

	tooMany := goodDef()
	tooMany.Steps = make([]Step, MaxSteps+1)
	reject(t, tooMany, goodOpts(), "maximum")

	// The catalog-exact name rule accepts dots and case.
	for _, name := range []string{"a", "A.b-c_d", strings.Repeat("n", 128)} {
		d := goodDef()
		d.Name = name
		if _, err := Compile(d, goodOpts()); err != nil {
			t.Errorf("name %q: %v", name, err)
		}
	}
	// Wildcard selector and sanitized selectors are valid.
	for _, sel := range []string{"*", "app", "a_b-c0"} {
		d := goodDef()
		d.Selector = sel
		if _, err := Compile(d, goodOpts()); err != nil {
			t.Errorf("selector %q: %v", sel, err)
		}
	}
}

func TestCompileOptionsValidation(t *testing.T) {
	reject(t, goodDef(), CompileOptions{}, "StreamLabels is required")
	reject(t, goodDef(), CompileOptions{StreamLabels: []string{"bad name"}}, "StreamLabels")
	for _, o := range []CompileOptions{
		{StreamLabels: []string{"stream"}, LineBytes: -1},
		{StreamLabels: []string{"stream"}, MetaBytes: 0, MetaValueBytes: -1},
		{StreamLabels: []string{"stream"}, LabelValueBytes: -2},
	} {
		// resolve() turns zeros into defaults; only negatives fail.
		reject(t, goodDef(), o, "must be positive")
	}
	// Zero bounds resolve to defaults and compile.
	if _, err := Compile(goodDef(), CompileOptions{StreamLabels: []string{"stream"}}); err != nil {
		t.Fatal(err)
	}
}

func TestStepFieldValidation(t *testing.T) {
	tests := []struct {
		name string
		step Step
		want string
	}{
		{"unknown type", Step{Type: "explode"}, "unknown step type"},
		{"pattern on rename", Step{Type: StepRename, From: "a", To: "b", Pattern: "x"}, "does not take pattern"},
		{"expr on parse", Step{Type: StepParseJSON, Expr: "true"}, "does not take expr"},
		{"target on drop_if", Step{Type: StepDropIf, Expr: "true", Target: "x"}, "does not take target"},
		{"value on rename", Step{Type: StepRename, From: "a", To: "b", Value: `"x"`}, "does not take value"},
		{"to on parse", Step{Type: StepParseJSON, To: "x"}, "does not take to"},
		{"bad on_error", Step{Type: StepParseJSON, OnError: "explode"}, "invalid on_error"},
		{"drop_if without expr", Step{Type: StepDropIf}, "expr is required"},
		{"rename without to", Step{Type: StepRename, From: "a"}, "invalid name"}, // To="" fails the name rule
		{"rename to dunder", Step{Type: StepRename, From: "a", To: "__x"}, "reserved"},
		{"rename to invalid", Step{Type: StepRename, From: "a", To: "a.b"}, "invalid name"},
		{"rename from invalid", Step{Type: StepRename, From: "a.b", To: "c"}, "invalid name"},
		{"parse from invalid", Step{Type: StepParseJSON, From: "a.b"}, "invalid name"},
		{"redact without pattern", Step{Type: StepRedact}, "pattern is required"},
		{"redact invalid pattern", Step{Type: StepRedact, Pattern: "("}, "invalid pattern"},
		{"redact long pattern", Step{Type: StepRedact, Pattern: strings.Repeat("a", MaxPatternBytes+1)}, "pattern exceeds"},
		{"redact long replace", Step{Type: StepRedact, Pattern: "a", Replace: new(strings.Repeat("b", MaxReplaceBytes+1))}, "replacement exceeds"},
		{"redact target dunder", Step{Type: StepRedact, Pattern: "a", Target: "__x"}, "reserved"},
		{"extract no named group", Step{Type: StepRegexExtract, Pattern: `(a+)`}, "no named capture group"},
		{"extract invalid pattern", Step{Type: StepRegexExtract, Pattern: "(?P<x>"}, "invalid pattern"},
		{"promote without source", Step{Type: StepPromote, To: "svc"}, "takes from or value"},
		{"promote both sources", Step{Type: StepPromote, To: "svc", From: "a", Value: `"b"`}, "not both"},
		{"promote without to", Step{Type: StepPromote, From: "a"}, "invalid name"},
		{"promote reserved to", Step{Type: StepPromote, From: "a", To: "_ts"}, "reserved column"},
		{"promote case-reserved to", Step{Type: StepPromote, From: "a", To: "LINE"}, "reserved column"},
		{"promote invalid to", Step{Type: StepPromote, From: "a", To: "a b"}, "invalid name"},
		{"set_stream without source", Step{Type: StepSetStream}, "takes from or value"},
		{"set_stream both", Step{Type: StepSetStream, From: "a", Value: `"b"`}, "not both"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := goodDef()
			d.Steps = []Step{tc.step}
			reject(t, d, goodOpts(), tc.want)
		})
	}
}

// TestRegexCaptureNamesAreSanitizedNames: Go accepts leading-digit and
// __-prefixed capture names; they are extraction names, not explicit user
// destinations, so Compile accepts and execExtract sanitizes/prefixes them.
func TestRegexCaptureNamesAreSanitizedNames(t *testing.T) {
	for _, pattern := range []string{
		`(?P<1lvl>\w+)`,
		`(?P<__err>err)`,
		`(?P<lvl>\w+) (?P<lvl2>\w+)`,
	} {
		d := goodDef()
		d.Steps = []Step{{Type: StepRegexExtract, Pattern: pattern}}
		if _, err := Compile(d, goodOpts()); err != nil {
			t.Errorf("Compile(pattern %q): %v", pattern, err)
		}
	}
}

func TestPromoteCaseCollision(t *testing.T) {
	d := goodDef()
	d.Steps = []Step{
		{Type: StepPromote, From: "a", To: "App"},
		{Type: StepPromote, From: "b", To: "app"},
	}
	reject(t, d, goodOpts(), "differs only in case")
}

func TestReadStageRules(t *testing.T) {
	d := goodDef()
	d.Stage = StageRead
	d.Steps = []Step{
		{Type: StepParseJSON},
		{Type: StepParseLogfmt},
		{Type: StepRegexExtract, Pattern: `(?P<x>a)`},
		{Type: StepRename, From: "a", To: "b"},
		{Type: StepRedact, Pattern: "a"},
		{Type: StepDropIf, Expr: "line == \"\""},
	}
	if _, err := Compile(d, goodOpts()); err != nil {
		t.Fatalf("read stage should accept metadata/line steps: %v", err)
	}
	// rename/redact may target names equal to routing labels: rename is
	// metadata-only and never mutates Row.Labels.
	relabeled := goodDef()
	relabeled.Stage = StageRead
	relabeled.Steps = []Step{
		{Type: StepRename, From: "a", To: "stream"},
		{Type: StepRedact, Pattern: "a", Target: "org"},
	}
	if _, err := Compile(relabeled, goodOpts()); err != nil {
		t.Fatalf("routing-name metadata targets are legal: %v", err)
	}
	for _, tc := range []struct {
		name string
		step Step
		want string
	}{
		{"promote", Step{Type: StepPromote, From: "a", To: "x"}, "read stage"},
		{"set_stream", Step{Type: StepSetStream, Value: `"x"`}, "read stage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := goodDef()
			r.Stage = StageRead
			r.Steps = []Step{tc.step}
			reject(t, r, goodOpts(), tc.want)
		})
	}
}

// TestBoundedSubset exercises every construct the expression subset rejects.
func TestBoundedSubset(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{`let x = line; len(x) > 0`, "unsupported expression construct"},
		{`line[0] == "a"`, "literal member name"},
		{`line[0:2] == "a"`, "unsupported expression construct"},
		{`meta.x?.y == "a"`, "unsupported expression construct"},
		{`{"a": 1}.a == 1`, "member access is only supported on labels and meta"},
		{`1..5 | filter(# > 2)`, "not allowed"}, // range pipes into a builtin
		{`line + line != ""`, "concatenation may build"},
		{`lower(line) + lower(line) == ""`, "concatenation may build"},
		{`meta.a + line == ""`, "concatenation may build"},
		{`meta.x == 1`, "cannot compare"},
		{`len(labels) > 0`, "literal member access"},
		{`labels in ["a"]`, "literal member access"},
		{`get(labels, "x") == ""`, "function \"get\" is not allowed"},
		{`replace(line, "a", "bb") == ""`, "function \"replace\" is not allowed"},
		{`now() != ""`, "not allowed"},
		{`split(line, ",")[0] == ""`, "member access is only supported on labels and meta"},
		{`foo == "x"`, "unknown name"},
		{`line matches meta.p`, "literal pattern"},
		{`line matches "(?P<x>"`, "invalid matches pattern"},
		{`line != nil`, "unsupported expression construct"},
		{`meta.x == "a" ? "y" : 1`, "different types"},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			d := goodDef()
			d.Steps = []Step{{Type: StepDropIf, Expr: tc.expr}}
			reject(t, d, goodOpts(), tc.want)
		})
	}
	// A matches literal longer than MaxPatternBytes.
	d := goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: `line matches "` + strings.Repeat("a", MaxPatternBytes+1) + `"`}}
	reject(t, d, goodOpts(), "pattern exceeds")

	// An expression longer than MaxExprBytes fails the source cap.
	long := strings.Repeat("x", MaxExprBytes+1)
	d = goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.` + long + ` == ""`}}
	reject(t, d, goodOpts(), "exceeds")

	d = goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: `line == "` + long + `"`}}
	reject(t, d, goodOpts(), "exceeds")

	// Kind enforcement.
	d = goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.x`}}
	reject(t, d, goodOpts(), "must be boolean")
	d = goodDef()
	d.Steps = []Step{{Type: StepSetStream, Value: `len(line)`}}
	reject(t, d, goodOpts(), "must be string")
	d = goodDef()
	d.Steps = []Step{{Type: StepSetStream, Value: `1`}}
	reject(t, d, goodOpts(), "must be string")
	d = goodDef()
	d.Steps = []Step{{Type: StepParseJSON, When: `meta.x`}}
	reject(t, d, goodOpts(), "must be boolean")
}

// TestSubsetAccepts pins accepted expressions, including every allowlisted
// builtin and operator.
func TestSubsetAccepts(t *testing.T) {
	exprs := []string{
		`line == ""`,
		`line != "" && len(line) > 0`,
		`lower(line) == "x" || upper(line) == "Y"`,
		`trim(line) != "" `,
		`trimPrefix(line, "a") != ""`,
		`trimSuffix(line, "a") != ""`,
		`indexOf(line, "a") >= 0`,
		`line contains "err"`,
		`line startsWith "a"`,
		`line endsWith "z"`,
		`line matches "^a+$"`,
		`labels.app == "web"`,
		`labels["app"] == "web"`,
		`meta.level == "debug"`,
		`meta.level in ["debug", "info"]`,
		`stream == "default"`,
		`int(meta.status) >= 500`,
		`float(meta.latency) < 0.5`,
		`string(len(line)) != ""`,
		`meta.a + meta.b == "xy"`,
		`(meta.a == "1" ? meta.b : "none") == "x"`,
		`!false`,
		`-(int(meta.n)) < 0`,
		`1 + 2 * 3 == 7`,
		`1.5 / 2 > 0.7`,
		`7 % 2 == 1`,
	}
	for _, e := range exprs {
		t.Run(e, func(t *testing.T) {
			d := goodDef()
			d.Steps = []Step{{Type: StepDropIf, Expr: e}}
			if _, err := Compile(d, goodOpts()); err != nil {
				t.Fatalf("accepted expression rejected: %v", err)
			}
		})
	}
	// The same analyzer powers when and value.
	d := goodDef()
	d.Steps = []Step{
		{Type: StepParseJSON, When: `line startsWith "{"`},
		{Type: StepSetStream, Value: `lower(meta.env) + "-" + labels.zone`},
	}
	if _, err := Compile(d, goodOpts()); err != nil {
		t.Fatal(err)
	}
}

// TestJSONWhenGatesExecution: a when decoded from JSON actually gates the
// step, proving the common member is not just accepted but used: false skips
// the step, true runs it.
func TestJSONWhenGatesExecution(t *testing.T) {
	compileWhen := func(t *testing.T, when string) *Compiled {
		t.Helper()
		doc := []byte(`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"redact","pattern":"secret","when":` + strconv.Quote(when) + `}]}`)
		d, err := DecodeDefinition(doc)
		if err != nil {
			t.Fatalf("DecodeDefinition: %v", err)
		}
		c, err := Compile(d, goodOpts())
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return c
	}
	tests := []struct {
		name string
		when string
		line string
		want string
	}{
		{"false skips", `line == "never"`, "secret secret", "secret secret"},
		{"true applies", `line contains "secret"`, "secret secret", "*** ***"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := compileWhen(t, tc.when)
			out, outcome, err := c.Apply(t.Context(), "app", Entry{
				Line: tc.line,
				Meta: logs.New(logs.Label{Name: "env", Value: "prod"}),
			})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if outcome != OutcomeKeep {
				t.Fatalf("outcome = %v, want keep", outcome)
			}
			if out.Line != tc.want {
				t.Errorf("line = %q, want %q", out.Line, tc.want)
			}
			if got, _ := out.Meta.Get(ErrField); got != "" {
				t.Errorf("pipeline_error = %q", got)
			}
		})
	}
}

// TestCustomBoundsChangeAnalysis proves the static size cap honors
// CompileOptions, not just the defaults.
func TestCustomBoundsChangeAnalysis(t *testing.T) {
	opts := goodOpts()
	opts.LineBytes = 64 // tiny line budget
	opts.MetaValueBytes = 16
	d := goodDef()
	// 16+16 <= 64: fine at defaults, still fine here.
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.a + meta.b == "x"`}}
	if _, err := Compile(d, opts); err != nil {
		t.Fatal(err)
	}
	// line (64) + meta (16) > 64.
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.a + line == "x"`}}
	reject(t, d, opts, "concatenation may build")
	// With LineBytes=2 and MetaValueBytes=1, three metas exceed the cap but
	// two do not.
	tiny := goodOpts()
	tiny.LineBytes = 2
	tiny.MetaValueBytes = 1
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.a + meta.b == "x"`}}
	if _, err := Compile(d, tiny); err != nil {
		t.Fatalf("2-byte concat rejected: %v", err)
	}
	d.Steps = []Step{{Type: StepDropIf, Expr: `meta.a + meta.b + meta.c == "x"`}}
	reject(t, d, tiny, "concatenation may build")
}

// TestMaxNodesBound verifies the AST node cap with an expression well under
// the source-size cap: 1500 "+1" terms are ~3000 nodes in ~3000 bytes.
func TestMaxNodesBound(t *testing.T) {
	var b strings.Builder
	b.WriteString("1")
	for range 1500 {
		b.WriteString("+1")
	}
	b.WriteString("==0")
	if b.Len() > MaxExprBytes {
		t.Fatalf("test expression is %d bytes, want <= %d", b.Len(), MaxExprBytes)
	}
	d := goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: b.String()}}
	_, err := Compile(d, goodOpts())
	if err == nil {
		t.Fatal("node bomb compiled")
	}
	if !strings.Contains(err.Error(), "complex") && !strings.Contains(err.Error(), "nodes") {
		t.Fatalf("error is not the node budget: %v", err)
	}
}

// TestStaticSizeSaturates reaches the static size analysis with nested
// lower/upper inside the source and node caps: without saturating
// arithmetic the worst-case size overflows negative and slips past the
// concatenation cap.
func TestStaticSizeSaturates(t *testing.T) {
	expr := strings.Repeat("lower(", 570) + "line" + strings.Repeat(")", 570) + ` + "x" == "y"`
	if len(expr) > MaxExprBytes {
		t.Fatalf("test expression is %d bytes, want <= %d", len(expr), MaxExprBytes)
	}
	d := goodDef()
	d.Steps = []Step{{Type: StepDropIf, Expr: expr}}
	reject(t, d, goodOpts(), "concatenation may build")
}
