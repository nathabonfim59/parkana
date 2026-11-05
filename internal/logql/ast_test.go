package logql

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

func TestStageStrings(t *testing.T) {
	pos := Position{Offset: 9, Line: 1, Col: 10}
	m := plan.MustMatcher(plan.MatchRegexp, "bar", "x.*")
	tests := []struct {
		name string
		node Node
		want string
	}{
		{"line filter", &LineFilter{Pos: pos, Op: plan.LineContains, Values: []string{"err"}},
			`|= "err"`},
		{"line filter or-chain", &LineFilter{Pos: pos, Op: plan.LineRegexp, Values: []string{"err", "fail", "panic"}},
			`|~ "err" or "fail" or "panic"`},
		{"json", &JSONParser{Pos: pos}, `json`},
		{"json extractions", &JSONParser{Pos: pos, Extractions: []NamedExtraction{
			{Name: "status", Path: "response.status"}, {Name: "user", Path: "user.name"},
		}}, `json status="response.status", user="user.name"`},
		// json takes no flags in Loki's grammar; logfmt is the only flagged
		// parser.
		{"logfmt flags", &LogfmtParser{Pos: pos, Strict: true, KeepEmpty: true}, `logfmt --strict --keep-empty`},
		{"logfmt", &LogfmtParser{Pos: pos}, `logfmt`},
		{"logfmt flags", &LogfmtParser{Pos: pos, Strict: true, KeepEmpty: true}, `logfmt --strict --keep-empty`},
		{"logfmt extractions", &LogfmtParser{Pos: pos, Extractions: []NamedExtraction{{Name: "a", Path: "b"}}},
			`logfmt a="b"`},
		{"regexp", &RegexpParser{Pos: pos, Pattern: `(?P<method>\w+)`}, `regexp "(?P<method>\\w+)"`},
		{"pattern", &PatternParser{Pos: pos, Pattern: "<method> <uri>"}, `pattern "<method> <uri>"`},
		{"line_format", &LineFormat{Pos: pos, Template: "{{.level}}: {{__line__}}"}, `line_format "{{.level}}: {{__line__}}"`},
		{"label_format", &LabelFormat{Pos: pos, Replacements: []LabelReplacement{
			{Dest: "level", Src: "lvl"}, {Dest: "line", IsTemplate: true, Template: "{{.a}} {{.b}}"},
		}}, `label_format level=lvl, line="{{.a}} {{.b}}"`},
		// An explicit variant flag: dest="" is a template assignment, not a
		// rename with an empty source.
		{"label_format empty template", &LabelFormat{Pos: pos, Replacements: []LabelReplacement{
			{Dest: "gone", IsTemplate: true},
		}}, `label_format gone=""`},
		{"drop names", &Drop{Pos: pos, Targets: []DropTarget{
			{Name: "__error__"}, {Name: "tmp"},
		}}, `drop __error__, tmp`},
		{"drop mixed targets", &Drop{Pos: pos, Targets: []DropTarget{
			{Name: "foo"}, {Matcher: &m}, {Name: "__error__"},
		}}, `drop foo, bar=~"x.*", __error__`},
		{"keep names", &Keep{Pos: pos, Targets: []DropTarget{{Name: "app"}}}, `keep app`},
		{"unwrap plain", &Unwrap{Pos: pos, Label: "latency"}, `unwrap latency`},
		{"unwrap duration", &Unwrap{Pos: pos, Func: UnwrapDuration, Label: "latency"}, `unwrap duration(latency)`},
		{"unwrap duration_seconds", &Unwrap{Pos: pos, Func: UnwrapDurationSeconds, Label: "t"}, `unwrap duration_seconds(t)`},
		{"label filter leaf string", mustLeaf(t, "env", OpLabelEq, FilterValue{Kind: StringValue, Str: "prod"}), `env = "prod"`},
		{"label filter leaf duration", mustLeaf(t, "latency", OpLabelGt, FilterValue{Kind: DurationValue, Dur: 500 * time.Millisecond}), `latency > 500ms`},
		{"label filter leaf bytes", mustLeaf(t, "size", OpLabelGte, FilterValue{Kind: BytesValue, Bytes: 1024}), `size >= 1KiB`},
		{"label filter leaf number", mustLeaf(t, "count", OpLabelLt, FilterValue{Kind: NumberValue, Num: 0.99}), `count < 0.99`},
		{"label filter and", &LabelFilterBinary{Pos: pos, Op: OpFilterAnd,
			LHS: mustLeaf(t, "a", OpLabelEq, FilterValue{Kind: StringValue, Str: "1"}),
			RHS: mustLeaf(t, "b", OpLabelEq, FilterValue{Kind: StringValue, Str: "2"}),
		}, `a = "1" and b = "2"`},
		{"label filter comma", &LabelFilterBinary{Pos: pos, Op: OpComma,
			LHS: mustLeaf(t, "a", OpLabelEq, FilterValue{Kind: StringValue, Str: "1"}),
			RHS: mustLeaf(t, "b", OpLabelEq, FilterValue{Kind: StringValue, Str: "2"}),
		}, `a = "1", b = "2"`},
		// or is the loosest label-filter operator (syntax.y labelFilter), so
		// an or child of an and parent keeps parentheses.
		{"label filter parens", &LabelFilterBinary{Pos: pos, Op: OpFilterAnd,
			LHS: mustLeaf(t, "a", OpLabelEq, FilterValue{Kind: StringValue, Str: "1"}),
			RHS: &LabelFilterBinary{Op: OpFilterOr,
				LHS: mustLeaf(t, "b", OpLabelEq, FilterValue{Kind: StringValue, Str: "2"}),
				RHS: mustLeaf(t, "c", OpLabelEq, FilterValue{Kind: StringValue, Str: "3"}),
			},
		}, `a = "1" and (b = "2" or c = "3")`},
	}
	for _, tt := range tests {
		if got := tt.node.String(); got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func mustLeaf(t *testing.T, label string, op LabelOp, v FilterValue) *LabelFilterLeaf {
	t.Helper()
	f, err := NewLabelFilterLeaf(Position{}, label, op, v)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestLogQueryString(t *testing.T) {
	sel, err := ParseSelector(`{app="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	pos := Position{Line: 1}
	// Line-filter stages sit next to each other (Loki has no pipe between
	// them); every other stage joins with a pipe.
	q := &LogQuery{Pos: pos, Selector: *sel, Stages: []Stage{
		&LineFilter{Pos: pos, Op: plan.LineContains, Values: []string{"err", "fail"}},
		&LineFilter{Pos: pos, Op: plan.LineNotRegexp, Values: []string{"debug"}},
		&JSONParser{Pos: pos},
		&Drop{Pos: pos, Targets: []DropTarget{{Name: "__error__"}}},
		mustLeaf(t, "code", OpLabelGte, FilterValue{Kind: NumberValue, Num: 400}),
	}}
	want := `{app="api"} |= "err" or "fail" !~ "debug" | json | drop __error__ | code >= 400`
	if got := q.String(); got != want {
		t.Errorf("LogQuery.String() =\n %q\nwant\n %q", got, want)
	}
}

func TestMetricStrings(t *testing.T) {
	sel, err := ParseSelector(`{app="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	base := &LogQuery{Selector: *sel}
	pos := Position{Line: 1}
	tests := []struct {
		name string
		node Node
		want string
	}{
		{"log range", &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute},
			`{app="api"} [5m]`},
		{"log range offset", &LogRange{Pos: pos, Pipeline: base, Range: time.Hour, Offset: 24 * time.Hour},
			`{app="api"} [1h] offset 1d`},
		{"range agg", &RangeAgg{Pos: pos, Func: CountOverTime, Range: &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute}},
			`count_over_time({app="api"} [5m])`},
		// Range grouping renders after the closing parenthesis (syntax.y
		// rangeAggregationExpr), vector grouping before it.
		{"range agg grouped", &RangeAgg{Pos: pos, Func: CountOverTime,
			Grouping: &Grouping{Pos: pos, Names: []string{"env"}},
			Range:    &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute}},
			`count_over_time({app="api"} [5m]) by (env)`},
		{"quantile", &RangeAgg{Pos: pos, Func: QuantileOverTime, Params: []float64{0.99},
			Range: &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute}},
			`quantile_over_time(0.99, {app="api"} [5m])`},
		{"vector agg", &VectorAgg{Pos: pos, Func: Sum,
			Grouping: &Grouping{Pos: pos, Names: []string{"level", "detected_level"}},
			Left: &RangeAgg{Pos: pos, Func: CountOverTime,
				Range: &LogRange{Pos: pos, Pipeline: base, Range: time.Minute}}},
			`sum by (level, detected_level) (count_over_time({app="api"} [1m]))`},
		{"vector agg without", &VectorAgg{Pos: pos, Func: Avg,
			Grouping: &Grouping{Pos: pos, Without: true, Names: []string{"env"}},
			Left: &RangeAgg{Pos: pos, Func: Rate,
				Range: &LogRange{Pos: pos, Pipeline: base, Range: time.Minute}}},
			`avg without (env) (rate({app="api"} [1m]))`},
		{"topk", &VectorAgg{Pos: pos, Func: TopK, Params: []float64{3},
			Left: &RangeAgg{Pos: pos, Func: CountOverTime,
				Range: &LogRange{Pos: pos, Pipeline: base, Range: time.Minute}}},
			`topk(3, count_over_time({app="api"} [1m]))`},
		{"scalar", &NumberLit{Pos: pos, V: 0.99}, `0.99`},
		{"scalar large", &NumberLit{Pos: pos, V: 1e6}, `1e+06`},
		{"binary", &BinaryExpr{Pos: pos, Op: OpGt,
			LHS: &RangeAgg{Pos: pos, Func: Rate, Range: &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute}},
			RHS: &NumberLit{Pos: pos, V: 10}},
			`rate({app="api"} [5m]) > 10`},
		{"binary bool", &BinaryExpr{Pos: pos, Op: OpEq, Bool: true,
			LHS: &NumberLit{Pos: pos, V: 1}, RHS: &NumberLit{Pos: pos, V: 2}},
			`1 == bool 2`},
		{"binary parens", &BinaryExpr{Pos: pos, Op: OpMul,
			LHS: &BinaryExpr{Op: OpAdd, LHS: &NumberLit{V: 1}, RHS: &NumberLit{V: 2}},
			RHS: &NumberLit{V: 3}},
			`(1 + 2) * 3`},
		{"binary right assoc", &BinaryExpr{Pos: pos, Op: OpPow,
			LHS: &NumberLit{V: 2},
			RHS: &BinaryExpr{Op: OpPow, LHS: &NumberLit{V: 3}, RHS: &NumberLit{V: 4}}},
			`2 ^ 3 ^ 4`},
		{"binary left assoc parens", &BinaryExpr{Pos: pos, Op: OpPow,
			LHS: &BinaryExpr{Op: OpPow, LHS: &NumberLit{V: 2}, RHS: &NumberLit{V: 3}},
			RHS: &NumberLit{V: 4}},
			`(2 ^ 3) ^ 4`},
		{"vector matching", &BinaryExpr{Pos: pos, Op: OpAdd,
			LHS: &NumberLit{V: 1}, RHS: &NumberLit{V: 2},
			Matching: &VectorMatching{Pos: pos, On: true, Card: MatchManyToOne, Names: []string{"a"}, Include: []string{"b"}}},
			`1 + on (a) group_left (b) 2`},
		// Unwrap renders between the pipeline and the range, with its post
		// filters behind further pipes (syntax.y unwrapExpr).
		{"log range unwrap", &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute,
			Unwrap: &Unwrap{Pos: pos, Label: "latency"}},
			`{app="api"} | unwrap latency [5m]`},
		{"log range unwrap conversion + post filter", &LogRange{Pos: pos, Pipeline: base, Range: 5 * time.Minute,
			Unwrap: &Unwrap{Pos: pos, Func: UnwrapDurationSeconds, Label: "t",
				PostFilters: []LabelFilterExpr{
					mustLeaf(t, "ok", OpLabelEq, FilterValue{Kind: StringValue, Str: "y"}),
					mustLeaf(t, "n", OpLabelGt, FilterValue{Kind: NumberValue, Num: 3}),
				}}},
			`{app="api"} | unwrap duration_seconds(t) | ok = "y" | n > 3 [5m]`},
	}
	for _, tt := range tests {
		if got := tt.node.String(); got != tt.want {
			t.Errorf("%s =\n %q\nwant\n %q", tt.name, got, tt.want)
		}
	}
}

// TestGrafanaLogsVolumeSeed pins the AST shape of Grafana's logs-volume
// query (grafana-loki-datasource v13.2.5 src/datasource.ts:240-266), relayed
// by the lead: A2's grammar must parse exactly this string into exactly this
// tree, and the tree must render back to the same string.
func TestGrafanaLogsVolumeSeed(t *testing.T) {
	sel, err := ParseSelector(`{service_name="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	q := &VectorAgg{
		Func:     Sum,
		Grouping: &Grouping{Names: []string{"level", "detected_level"}},
		Left: &RangeAgg{
			Func: CountOverTime,
			Range: &LogRange{
				Pipeline: &LogQuery{
					Selector: *sel,
					Stages:   []Stage{&Drop{Targets: []DropTarget{{Name: "__error__"}}}},
				},
				Range: time.Minute,
			},
		},
	}
	want := `sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__ [1m]))`
	if got := q.String(); got != want {
		t.Errorf("seed =\n %q\nwant\n %q", got, want)
	}
	if drop, ok := q.Left.(*RangeAgg).Range.Pipeline.Stages[0].(*Drop); !ok || drop.Targets[0].Name != "__error__" {
		t.Error("seed pipeline must carry a name-form Drop stage")
	}
}

// TestEnumSpellings checks every value of every enum against an explicit
// table, so a missing switch case (like the OpLte one the review caught)
// fails loudly instead of falling through to a default.
func TestEnumSpellings(t *testing.T) {
	rangeWant := []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate",
		"sum_over_time", "avg_over_time", "max_over_time", "min_over_time",
		"stddev_over_time", "stdvar_over_time", "first_over_time", "last_over_time",
		"quantile_over_time", "absent_over_time"}
	for f, want := range rangeWant {
		if got := RangeFunc(f).String(); got != want {
			t.Errorf("RangeFunc(%d) = %q, want %q", f, got, want)
		}
		if got, ok := ParseRangeFunc(want); !ok || got != RangeFunc(f) {
			t.Errorf("ParseRangeFunc(%q) = %v, %v", want, got, ok)
		}
	}
	vectorWant := []string{"sum", "avg", "min", "max", "count", "stddev", "stdvar",
		"topk", "bottomk", "sort", "sort_desc"}
	for f, want := range vectorWant {
		if got := VectorFunc(f).String(); got != want {
			t.Errorf("VectorFunc(%d) = %q, want %q", f, got, want)
		}
		if got, ok := ParseVectorFunc(want); !ok || got != VectorFunc(f) {
			t.Errorf("ParseVectorFunc(%q) = %v, %v", want, got, ok)
		}
	}
	unwrapWant := []string{"", "duration", "bytes", "duration_seconds"}
	for f, want := range unwrapWant {
		if got := UnwrapFunc(f).String(); got != want {
			t.Errorf("UnwrapFunc(%d) = %q, want %q", f, got, want)
		}
	}
	binWant := []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">", ">=", "<", "<=", "and", "or", "unless"}
	for op, want := range binWant {
		if got := BinOp(op).String(); got != want {
			t.Errorf("BinOp(%d) = %q, want %q", op, got, want)
		}
	}
	labelWant := []string{"=", "!=", "=~", "!~", ">", ">=", "<", "<="}
	for op, want := range labelWant {
		if got := LabelOp(op).String(); got != want {
			t.Errorf("LabelOp(%d) = %q, want %q", op, got, want)
		}
	}
	boolWant := []string{"or", "and", ","}
	for op, want := range boolWant {
		if got := BoolOp(op).String(); got != want {
			t.Errorf("BoolOp(%d) = %q, want %q", op, got, want)
		}
	}
	cardWant := []string{"", "group_left", "group_right"}
	for c, want := range cardWant {
		if got := MatchCard(c).String(); got != want {
			t.Errorf("MatchCard(%d) = %q, want %q", c, got, want)
		}
	}
	// Functions in Loki but outside the supported set must not parse.
	for _, name := range []string{"rate_counter", "approx_topk", "label_replace", "unpack"} {
		if _, ok := ParseRangeFunc(name); ok {
			t.Errorf("ParseRangeFunc(%q) succeeded, want unsupported", name)
		}
		if _, ok := ParseVectorFunc(name); ok {
			t.Errorf("ParseVectorFunc(%q) succeeded, want unsupported", name)
		}
	}
}

// TestBinOpPrecedence pins PromQL's table, matching the %left/%right order
// of syntax.y:89-95: or < and/unless < comparisons < +- < */% < ^ (right).
func TestBinOpPrecedence(t *testing.T) {
	if !(OpOr.precedence() < OpAnd.precedence() &&
		OpAnd.precedence() == OpUnless.precedence() &&
		OpAnd.precedence() < OpEq.precedence() &&
		OpEq.precedence() == OpLte.precedence() &&
		OpEq.precedence() < OpAdd.precedence() &&
		OpAdd.precedence() < OpMul.precedence() &&
		OpMul.precedence() < OpPow.precedence()) {
		t.Error("scalar precedence order must be or < and/unless < cmp < +- < */% < ^")
	}
	if !OpPow.rightAssociative() || OpAdd.rightAssociative() {
		t.Error("only ^ is right-associative")
	}
	if !OpGte.IsComparison() || OpAnd.IsComparison() {
		t.Error("IsComparison must cover exactly ==, !=, >, >=, <, <=")
	}
}

// TestFilterBoolOpPrecedence pins label-filter precedence from syntax.y's
// labelFilter rule: or is loosest; and, comma and juxtaposition all mean AND
// and bind tighter.
func TestFilterBoolOpPrecedence(t *testing.T) {
	if !(OpFilterOr.precedence() < OpFilterAnd.precedence() &&
		OpFilterAnd.precedence() == OpComma.precedence()) {
		t.Error("label-filter precedence must be or < and == comma")
	}
}

func TestNewLabelFilterLeafValidation(t *testing.T) {
	str := FilterValue{Kind: StringValue, Str: "x"}
	num := FilterValue{Kind: NumberValue, Num: 1}
	dur := FilterValue{Kind: DurationValue, Dur: time.Second}
	byts := FilterValue{Kind: BytesValue, Bytes: 100}
	tests := []struct {
		op    LabelOp
		v     FilterValue
		valid bool
	}{
		{OpLabelEq, str, true},
		{OpLabelNeq, str, true},
		{OpLabelRe, str, true},
		{OpLabelNre, str, true},
		// = and != accept every value kind (a label reads back as a string).
		{OpLabelEq, num, true},
		{OpLabelEq, dur, true},
		{OpLabelNeq, byts, true},
		// Ordering comparisons need a numeric, duration or bytes value.
		{OpLabelGt, num, true},
		{OpLabelGte, dur, true},
		{OpLabelLt, byts, true},
		{OpLabelLte, num, true},
		// =~ and !~ are string-only; ordering never takes strings.
		{OpLabelRe, num, false},
		{OpLabelNre, dur, false},
		{OpLabelGt, str, false},
		{OpLabelLte, str, false},
	}
	for _, tt := range tests {
		_, err := NewLabelFilterLeaf(Position{}, "l", tt.op, tt.v)
		if tt.valid && err != nil {
			t.Errorf("NewLabelFilterLeaf(l %s %s): %v", tt.op, tt.v, err)
		}
		if !tt.valid && !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("NewLabelFilterLeaf(l %s %s) err = %v, want ErrBadQuery", tt.op, tt.v, err)
		}
	}
	if _, err := NewLabelFilterLeaf(Position{}, "", OpLabelEq, str); !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("empty label: err = %v, want ErrBadQuery", err)
	}
}

func TestNewRegexpParserValidation(t *testing.T) {
	// At least one named capture group is required; unnamed groups beside a
	// named one are fine (Loki parser.go skips empty SubexpNames and rejects
	// only an empty name index). Capture names need only be nonempty UTF-8
	// here, like Loki's model.UTF8Validation.IsValidLabelName, so 1bad is
	// accepted at the syntax stage; A5 sanitizes names before storing.
	for _, good := range []string{
		`(?P<method>\w+) (?P<path>.+)`,
		`(?P<x>a)(b)`,
		`(\w+)(?P<method>\w+)`,
		`(?P<1bad>x)`,
		`(?P<_ok>\d+)(?P<1bad>x)`,
	} {
		if _, err := NewRegexpParser(Position{}, good); err != nil {
			t.Errorf("pattern %q: %v, want valid", good, err)
		}
	}
	for _, bad := range []string{
		`.+`,                   // no capture group at all
		`(\w+)`,                // only unnamed capture groups
		`(?P<a>\d+)(?P<a>\d+)`, // duplicate name
		`(?P<x>a)(b)(?P<x>c)`,  // duplicate name across a mixed pattern
		`(?P<bad-name>\w+)`,    // Go rejects the spelling at compile time
		`(`,                    // invalid regexp
		``,
	} {
		if _, err := NewRegexpParser(Position{}, bad); !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("pattern %q: err = %v, want ErrBadQuery", bad, err)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		n    uint64
		want string
	}{
		{0, "0B"},
		{1, "1B"},
		{999, "999B"},
		{1000, "1kB"},   // SI: kB is 1000 (humanize.ParseBytes)
		{1024, "1KiB"},  // IEC: KiB is 1024
		{1500, "1500B"}, // no exact unit
		{1536, "1536B"},
		{1 << 20, "1MiB"},
		{5 * 1 << 30, "5GiB"},
		{uint64(1.5 * 1e6), "1500kB"},
		{1e15, "1PB"},
		{1 << 63, "8192PiB"}, // above int64, still formatted exactly
		{math.MaxUint64, "18446744073709551615B"},
	}
	for _, tt := range tests {
		if got := formatBytes(tt.n); got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
	// Every canonical form must lex back to the same byte count.
	for _, n := range []uint64{0, 1, 999, 1000, 1024, 1500, 1 << 20, 5 * 1 << 30, 1e15, 1 << 63, math.MaxUint64} {
		got, err := lexAll(formatBytes(n))
		if err != nil || len(got) != 2 || got[0].Kind != tokBytes || got[0].Bytes != n {
			t.Errorf("formatBytes(%d) = %q does not lex back: %v", n, formatBytes(n), err)
		}
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		v    float64
		want string
	}{
		{0, "0"}, {42, "42"}, {0.99, "0.99"}, {1e6, "1e+06"}, {1.5, "1.5"}, {-3, "-3"},
	}
	for _, tt := range tests {
		if got := formatNumber(tt.v); got != tt.want {
			t.Errorf("formatNumber(%v) = %q, want %q", tt.v, got, tt.want)
		}
	}
}
