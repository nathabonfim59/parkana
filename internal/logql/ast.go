// Typed AST for LogQL. A1 defined the node shapes for the whole phase-2
// grammar — pipelines, range and vector aggregation, offsets and scalar
// operations — so the evaluator packages (A4+) and A2's expression parser
// never invented competing representations. A1 implemented parsing for
// stream selectors (selector.go); A2's parser (parser.go, parser_log.go,
// parser_expr.go) builds every other node, and the deferred-family
// placeholders at the bottom of this file never escape Parse.
//
// The compiler stages are: lexer (lexer.go) → tokens → parser (selector.go
// for bare selectors, parser*.go for full expressions) → this AST →
// evaluator (logql.go and friends, plan.Selection pushdown plus a Go
// pipeline).
//
// All nodes render canonical LogQL from String(); parsing a String() output
// again yields an equivalent tree. Canonical forms normalize spelling but
// never meaning: raw strings are re-quoted, and durations render as integer
// LogQL unit components (90m renders 1h30m). Literal binary expressions
// stay lossless (Loki folds 1/0 to a NaN literal at parse time; Parkana
// defers folding to evaluation so every canonical form still reparses).
//
// Grammar facts pinned against Loki 3.7.9 (lead-saved
// /tmp/sol-a/loki-3.7.9/pkg_logql_syntax_syntax.y): scalar operator
// precedence or < and/unless < comparisons < +- < */% < ^ (right, lines
// 89-95); range aggregation grouping renders after the closing parenthesis
// while vector grouping renders before it (lines 169-186); label filters
// bind or loosest with and/comma tighter (labelFilter rule); drop/keep take
// a comma list of label names and string matchers and select labels, never
// rows (dropLabelsExpr, pkg_logql_log_drop_labels.go); unwrap conversions
// are exactly duration, duration_seconds and bytes (convOp); only logfmt
// takes parser flags (json has no --strict).

package logql

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// Node is a LogQL syntax node. String returns the canonical LogQL spelling.
type Node interface {
	String() string
}

// Expr is a complete expression: a log query, a range or vector aggregation,
// a scalar subexpression, or a binary operation over those.
type Expr interface {
	Node
	exprNode()
}

// Stage is one pipeline stage of a log query.
type Stage interface {
	Node
	stageNode()
}

// LabelFilterExpr is the boolean expression of one | label-filter stage.
type LabelFilterExpr interface {
	Node
	labelFilterExpr()
}

// LogQuery is a selector and its pipeline: {app="api"} |= "err" | json.
// Consecutive line-filter stages sit next to each other without a pipe,
// like Loki's lineFilters rule; every other stage is separated by |.
type LogQuery struct {
	Pos      Position
	Selector Selector
	Stages   []Stage
}

func (*LogQuery) exprNode() {}

func (q *LogQuery) String() string {
	var b strings.Builder
	b.WriteString(q.Selector.String())
	for _, st := range q.Stages {
		if _, ok := st.(*LineFilter); ok {
			b.WriteString(" ")
		} else {
			b.WriteString(" | ")
		}
		b.WriteString(st.String())
	}
	return b.String()
}

// LineFilter is a line-filter stage with its or-alternatives:
// |= "err" or "fail". plan.Selection ANDs line filters, so or-groups are
// evaluated in Go (A4).
type LineFilter struct {
	Pos    Position
	Op     plan.LineFilterType // |=, !=, |~ or !~
	Values []string            // the value and its or-alternatives; at least one
}

func (*LineFilter) stageNode() {}

func (f *LineFilter) String() string {
	var b strings.Builder
	b.WriteString(f.Op.String())
	for i, v := range f.Values {
		if i > 0 {
			b.WriteString(" or")
		}
		b.WriteString(" ")
		b.WriteString(strconv.Quote(v))
	}
	return b.String()
}

// NamedExtraction is one named parameter of a parser stage: name="path".
type NamedExtraction struct {
	Name string
	Path string
}

func (e NamedExtraction) String() string {
	return e.Name + "=" + strconv.Quote(e.Path)
}

// JSONParser is the | json stage: | json or | json status="response.status".
// Unlike logfmt, json takes no flags in Loki's grammar.
type JSONParser struct {
	Pos         Position
	Extractions []NamedExtraction
}

func (*JSONParser) stageNode() {}

func (p *JSONParser) String() string {
	var b strings.Builder
	b.WriteString("json")
	for i, e := range p.Extractions {
		b.WriteString(" ")
		b.WriteString(e.String())
		if i+1 < len(p.Extractions) {
			b.WriteString(",")
		}
	}
	return b.String()
}

// LogfmtParser is the | logfmt stage: | logfmt, | logfmt --strict
// --keep-empty, | logfmt a="b". Only logfmt takes parser flags.
type LogfmtParser struct {
	Pos         Position
	Strict      bool
	KeepEmpty   bool
	Extractions []NamedExtraction
}

func (*LogfmtParser) stageNode() {}

func (p *LogfmtParser) String() string {
	var b strings.Builder
	b.WriteString("logfmt")
	if p.Strict {
		b.WriteString(" --strict")
	}
	if p.KeepEmpty {
		b.WriteString(" --keep-empty")
	}
	for i, e := range p.Extractions {
		b.WriteString(" ")
		b.WriteString(e.String())
		if i+1 < len(p.Extractions) {
			b.WriteString(",")
		}
	}
	return b.String()
}

// RegexpParser is the | regexp stage. At least one capture group must be
// named, and names must be unique. Unnamed groups are allowed beside named
// ones, as in Loki; NewRegexpParser enforces it.
type RegexpParser struct {
	Pos     Position
	Pattern string
}

func (*RegexpParser) stageNode() {}

func (p *RegexpParser) String() string {
	return "regexp " + strconv.Quote(p.Pattern)
}

// NewRegexpParser validates a | regexp pattern: it must compile as RE2 and
// contain at least one named capture group, with each name used once.
// Unnamed groups are fine as long as one group is named (Loki parser.go
// skips empty SubexpNames and rejects only an empty name index). Capture
// names only need to be nonempty valid UTF-8 here, like Loki's
// model.UTF8Validation.IsValidLabelName; Go's regexp compiler already
// restricts their spelling, and A5's pipeline sanitizes them before they
// become stored labels.
func NewRegexpParser(pos Position, pattern string) (*RegexpParser, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, badQuery(pos, "invalid regexp pattern %q: %v", pattern, err)
	}
	seen := make(map[string]bool)
	for i, name := range re.SubexpNames() {
		if i == 0 || name == "" {
			continue // the whole match, or an unnamed group: allowed
		}
		if seen[name] {
			return nil, badQuery(pos, "regexp pattern %q repeats capture name %q", pattern, name)
		}
		seen[name] = true
	}
	if len(seen) == 0 {
		return nil, badQuery(pos, "regexp pattern %q must contain at least one named capture group", pattern)
	}
	return &RegexpParser{Pos: pos, Pattern: pattern}, nil
}

// PatternParser is the | pattern stage; the pattern's captures are its
// identifiers and <_>.
type PatternParser struct {
	Pos     Position
	Pattern string
}

func (*PatternParser) stageNode() {}

func (p *PatternParser) String() string {
	return "pattern " + strconv.Quote(p.Pattern)
}

// LineFormat is the | line_format stage; Template is a Go template.
type LineFormat struct {
	Pos      Position
	Template string
}

func (*LineFormat) stageNode() {}

func (f *LineFormat) String() string {
	return "line_format " + strconv.Quote(f.Template)
}

// LabelReplacement is one replacement of a | label_format stage: a rename
// dest=src (IsTemplate false) or a template assignment dest="{{ .src }}"
// (IsTemplate true; the template may be empty). The flag exists because
// Template != "" alone cannot tell a rename from dest="".
type LabelReplacement struct {
	Dest       string
	Src        string // rename source label, when !IsTemplate
	Template   string // template text, when IsTemplate
	IsTemplate bool
}

func (r LabelReplacement) String() string {
	if r.IsTemplate {
		return r.Dest + "=" + strconv.Quote(r.Template)
	}
	return r.Dest + "=" + r.Src
}

// LabelFormat is the | label_format stage.
type LabelFormat struct {
	Pos          Position
	Replacements []LabelReplacement
}

func (*LabelFormat) stageNode() {}

func (f *LabelFormat) String() string {
	parts := make([]string, len(f.Replacements))
	for i, r := range f.Replacements {
		parts[i] = r.String()
	}
	return "label_format " + strings.Join(parts, ", ")
}

// DropTarget is one target of a drop or keep stage: either a label name
// (Name, Matcher nil) or a string matcher (Matcher). Targets select labels,
// never rows — the stage always keeps the row (Loki's DropLabels.Process);
// dropping __error__ additionally clears the parser-error state.
type DropTarget struct {
	Name    string
	Matcher *plan.Matcher
}

func (t DropTarget) String() string {
	if t.Matcher != nil {
		return t.Matcher.String()
	}
	return t.Name
}

// Drop is the | drop stage: | drop foo, bar=~"x.*", __error__. It drops the
// targeted labels wherever they appear; a target no row has is harmless.
// Grafana's logs-volume queries append | drop __error__.
type Drop struct {
	Pos     Position
	Targets []DropTarget
}

func (*Drop) stageNode() {}

func (d *Drop) String() string {
	return dropKeepString("drop", d.Targets)
}

// Keep is the | keep stage: | keep foo, bar=~"x.*". It keeps only the
// targeted labels.
type Keep struct {
	Pos     Position
	Targets []DropTarget
}

func (*Keep) stageNode() {}

func (k *Keep) String() string {
	return dropKeepString("keep", k.Targets)
}

func dropKeepString(name string, targets []DropTarget) string {
	parts := make([]string, len(targets))
	for i, t := range targets {
		parts[i] = t.String()
	}
	return name + " " + strings.Join(parts, ", ")
}

// UnwrapFunc converts the unwrapped label value of an Unwrap stage. Loki's
// convOp defines exactly these three conversions.
type UnwrapFunc int

// Unwrap conversions. UnwrapLabel is a plain label with no conversion.
const (
	UnwrapLabel UnwrapFunc = iota
	UnwrapDuration
	UnwrapBytes
	UnwrapDurationSeconds
)

func (f UnwrapFunc) String() string {
	switch f {
	case UnwrapDuration:
		return "duration"
	case UnwrapBytes:
		return "bytes"
	case UnwrapDurationSeconds:
		return "duration_seconds"
	}
	return ""
}

// Unwrap is the | unwrap stage, which ends a metric pipeline:
// | unwrap latency, | unwrap duration(latency). PostFilters are the label
// filters Loki allows after an unwrap (unwrapExpr: unwrapExpr PIPE
// labelFilter), applied to the unwrapped samples.
type Unwrap struct {
	Pos         Position
	Func        UnwrapFunc
	Label       string
	PostFilters []LabelFilterExpr
}

func (*Unwrap) stageNode() {}

func (u *Unwrap) String() string {
	var b strings.Builder
	if u.Func == UnwrapLabel {
		b.WriteString("unwrap " + u.Label)
	} else {
		b.WriteString("unwrap " + u.Func.String() + "(" + u.Label + ")")
	}
	for _, f := range u.PostFilters {
		b.WriteString(" | ")
		b.WriteString(f.String())
	}
	return b.String()
}

// BoolOp connects label-filter expressions. Precedence follows Loki's
// labelFilter rule (syntax.y:302-311): or binds loosest; and, the comma
// operator and plain juxtaposition all mean AND and bind tighter.
type BoolOp int

// Label-filter boolean operators, loosest to tightest.
const (
	OpFilterOr  BoolOp = iota // or
	OpFilterAnd               // and (comma and juxtaposition parse to this tier)
	OpComma                   // ,
)

func (op BoolOp) String() string {
	switch op {
	case OpFilterOr:
		return "or"
	case OpFilterAnd:
		return "and"
	}
	return ","
}

// precedence returns the binding power of op in label-filter expressions:
// or 1, and and comma 2.
func (op BoolOp) precedence() int {
	if op == OpFilterOr {
		return 1
	}
	return 2
}

// LabelFilterBinary is a boolean combination of label-filter expressions:
// env="prod" and (code=~"5.." or code=~"4..").
type LabelFilterBinary struct {
	Pos Position
	Op  BoolOp
	LHS LabelFilterExpr
	RHS LabelFilterExpr
}

func (*LabelFilterBinary) stageNode()       {}
func (*LabelFilterBinary) labelFilterExpr() {}

func (n *LabelFilterBinary) String() string {
	sep := " " + n.Op.String() + " "
	if n.Op == OpComma {
		sep = ", "
	}
	return renderFilterChild(n.LHS, n.Op) + sep + renderFilterChild(n.RHS, n.Op)
}

// renderFilterChild parenthesizes a binary child that binds looser than its
// parent, so the rendered form reparses to the same tree.
func renderFilterChild(n LabelFilterExpr, parent BoolOp) string {
	s := n.String()
	if child, ok := n.(*LabelFilterBinary); ok && child.Op.precedence() < parent.precedence() {
		return "(" + s + ")"
	}
	return s
}

// LabelOp is a label-filter comparison operator.
type LabelOp int

// Label comparison operators.
const (
	OpLabelEq  LabelOp = iota // =
	OpLabelNeq                // !=
	OpLabelRe                 // =~
	OpLabelNre                // !~
	OpLabelGt                 // >
	OpLabelGte                // >=
	OpLabelLt                 // <
	OpLabelLte                // <=
)

func (op LabelOp) String() string {
	switch op {
	case OpLabelEq:
		return "="
	case OpLabelNeq:
		return "!="
	case OpLabelRe:
		return "=~"
	case OpLabelNre:
		return "!~"
	case OpLabelGt:
		return ">"
	case OpLabelGte:
		return ">="
	case OpLabelLt:
		return "<"
	case OpLabelLte:
		return "<="
	}
	return "LabelOp(" + strconv.Itoa(int(op)) + ")"
}

// ValueType classifies the right-hand side of a label comparison.
type ValueType int

// Label-filter value kinds.
const (
	StringValue ValueType = iota
	NumberValue
	DurationValue
	BytesValue
)

// FilterValue is the typed right-hand side of a label comparison. String
// comparisons use Str; >, >=, < and <= compare against Num, Dur or Bytes.
type FilterValue struct {
	Kind  ValueType
	Str   string        // StringValue
	Num   float64       // NumberValue
	Dur   time.Duration // DurationValue
	Bytes uint64        // BytesValue, matching Loki's uint64 byte tokens
}

func (v FilterValue) String() string {
	switch v.Kind {
	case NumberValue:
		return formatNumber(v.Num)
	case DurationValue:
		return formatDuration(v.Dur)
	case BytesValue:
		return formatBytes(v.Bytes)
	}
	return strconv.Quote(v.Str)
}

// formatDuration renders d with LogQL's duration unit vocabulary
// (y w d h m s ms, largest component first), falling back to
// time.Duration's own form for negative values and sub-millisecond
// precision (only the time.ParseDuration units ns/us/µs produce those).
// It is not byte-identical to model.Duration's formatting, which emits
// years and weeks only when they divide exactly.
func formatDuration(d time.Duration) string {
	if d < 0 || d%time.Millisecond != 0 {
		return d.String()
	}
	if d == 0 {
		return "0s"
	}
	units := [...]struct {
		name string
		d    time.Duration
	}{
		{"y", 365 * 24 * time.Hour},
		{"w", 7 * 24 * time.Hour},
		{"d", 24 * time.Hour},
		{"h", time.Hour},
		{"m", time.Minute},
		{"s", time.Second},
		{"ms", time.Millisecond},
	}
	var b strings.Builder
	for _, u := range units {
		if n := int64(d / u.d); n > 0 {
			b.WriteString(strconv.FormatInt(n, 10))
			b.WriteString(u.name)
			d -= time.Duration(n) * u.d
		}
	}
	return b.String()
}

// formatBytes renders a byte count as the largest exact unit that
// humanize.ParseBytes (Loki's bytes lexing) reads back to the same value:
// SI units are 1000-based, IEC units 1024-based, tried in decreasing size,
// and anything without an exact unit is a plain byte count. Values above
// int64 are valid; the rune gate rejects e-forms, so they render in full.
func formatBytes(n uint64) string {
	units := [...]struct {
		name string
		mult uint64
	}{
		{"PB", 1e15}, {"PiB", 1 << 50},
		{"TB", 1e12}, {"TiB", 1 << 40},
		{"GB", 1e9}, {"GiB", 1 << 30},
		{"MB", 1e6}, {"MiB", 1 << 20},
		{"kB", 1000}, {"KiB", 1 << 10},
	}
	for _, u := range units {
		if n >= u.mult && n%u.mult == 0 {
			return strconv.FormatUint(n/u.mult, 10) + u.name
		}
	}
	return strconv.FormatUint(n, 10) + "B"
}

// LabelFilterLeaf is one label comparison: env="prod", latency > 500ms,
// size > 1KB. LabelFilterLeaf and LabelFilterBinary double as pipeline
// stages, so a label-filter stage needs no wrapper node.
type LabelFilterLeaf struct {
	Pos   Position
	Label string
	Op    LabelOp
	Value FilterValue
}

func (*LabelFilterLeaf) stageNode()       {}
func (*LabelFilterLeaf) labelFilterExpr() {}

func (f *LabelFilterLeaf) String() string {
	return f.Label + " " + f.Op.String() + " " + f.Value.String()
}

// NewLabelFilterLeaf validates the operator/value pairing: =~ and !~ take
// string values only; = and != accept every kind (Loki's numeric label
// filters allow =, !=, == with literals); >, >=, < and <= take number,
// duration or byte values.
func NewLabelFilterLeaf(pos Position, label string, op LabelOp, v FilterValue) (*LabelFilterLeaf, error) {
	if label == "" {
		return nil, badQuery(pos, "label filter with empty label name")
	}
	switch {
	case op == OpLabelRe || op == OpLabelNre:
		if v.Kind != StringValue {
			return nil, badQuery(pos, "label filter %s %s needs a string value", label, op)
		}
	case op == OpLabelGt || op == OpLabelGte || op == OpLabelLt || op == OpLabelLte:
		if v.Kind == StringValue {
			return nil, badQuery(pos, "label filter %s %s needs a number, duration or bytes value", label, op)
		}
	}
	return &LabelFilterLeaf{Pos: pos, Label: label, Op: op, Value: v}, nil
}

// LogRange is a pipeline closed by a range selector:
// {app="api"} |= "err" [5m] offset 1h, with the unwrap stage that turns it
// into a metric pipeline. Range aggregations wrap one. Grafana's logs-volume
// queries put stages before the range, as in
// count_over_time({service_name="api"} | drop __error__ [1m]); the grammar
// also accepts stages after the range, which canonicalize before it.
type LogRange struct {
	Pos      Position
	Pipeline *LogQuery
	Range    time.Duration
	Offset   time.Duration
	Unwrap   *Unwrap // nil for count/rate-style aggregations
}

// LogRange carries exprNode so the parser can hold it in operand position
// while type checks keep it out of every expression a query can actually
// evaluate: a bare range is rejected at the root and in binary operands,
// and only range aggregations take one as their argument.
func (*LogRange) exprNode() {}

func (r *LogRange) String() string {
	var b strings.Builder
	b.WriteString(r.Pipeline.String())
	if r.Unwrap != nil {
		b.WriteString(" | ")
		b.WriteString(r.Unwrap.String())
	}
	b.WriteString(" [")
	b.WriteString(formatDuration(r.Range))
	b.WriteString("]")
	if r.Offset != 0 {
		b.WriteString(" offset ")
		b.WriteString(formatDuration(r.Offset))
	}
	return b.String()
}

// RangeFunc is a range aggregation function.
type RangeFunc int

// Range aggregation functions.
const (
	CountOverTime RangeFunc = iota
	Rate
	BytesOverTime
	BytesRate
	SumOverTime
	AvgOverTime
	MaxOverTime
	MinOverTime
	StddevOverTime
	StdvarOverTime
	FirstOverTime
	LastOverTime
	QuantileOverTime
	AbsentOverTime
)

func (f RangeFunc) String() string {
	switch f {
	case CountOverTime:
		return "count_over_time"
	case Rate:
		return "rate"
	case BytesOverTime:
		return "bytes_over_time"
	case BytesRate:
		return "bytes_rate"
	case SumOverTime:
		return "sum_over_time"
	case AvgOverTime:
		return "avg_over_time"
	case MaxOverTime:
		return "max_over_time"
	case MinOverTime:
		return "min_over_time"
	case StddevOverTime:
		return "stddev_over_time"
	case StdvarOverTime:
		return "stdvar_over_time"
	case FirstOverTime:
		return "first_over_time"
	case LastOverTime:
		return "last_over_time"
	case QuantileOverTime:
		return "quantile_over_time"
	case AbsentOverTime:
		return "absent_over_time"
	}
	return "RangeFunc(" + strconv.Itoa(int(f)) + ")"
}

// ParseRangeFunc looks up a range aggregation function by name.
func ParseRangeFunc(s string) (RangeFunc, bool) {
	for f := CountOverTime; f <= AbsentOverTime; f++ {
		if f.String() == s {
			return f, true
		}
	}
	return 0, false
}

// VectorFunc is a vector aggregation function.
type VectorFunc int

// Vector aggregation functions.
const (
	Sum VectorFunc = iota
	Avg
	Min
	Max
	Count
	Stddev
	Stdvar
	TopK
	BottomK
	Sort
	SortDesc
)

func (f VectorFunc) String() string {
	switch f {
	case Sum:
		return "sum"
	case Avg:
		return "avg"
	case Min:
		return "min"
	case Max:
		return "max"
	case Count:
		return "count"
	case Stddev:
		return "stddev"
	case Stdvar:
		return "stdvar"
	case TopK:
		return "topk"
	case BottomK:
		return "bottomk"
	case Sort:
		return "sort"
	case SortDesc:
		return "sort_desc"
	}
	return "VectorFunc(" + strconv.Itoa(int(f)) + ")"
}

// ParseVectorFunc looks up a vector aggregation function by name.
func ParseVectorFunc(s string) (VectorFunc, bool) {
	for f := Sum; f <= SortDesc; f++ {
		if f.String() == s {
			return f, true
		}
	}
	return 0, false
}

// Grouping is a by/without clause: by (a, b) or without (a). A nil
// *Grouping means no grouping.
type Grouping struct {
	Pos     Position
	Without bool // without (…) when true, by (…) when false
	Names   []string
}

func (g *Grouping) String() string {
	if g == nil {
		return ""
	}
	how := "by"
	if g.Without {
		how = "without"
	}
	return how + " (" + strings.Join(g.Names, ", ") + ")"
}

// RangeAgg is a range aggregation over a log range:
// count_over_time({…} |= "err" [5m] offset 1h), with optional grouping
// rendering after the closing parenthesis,
// count_over_time({…}[5m]) by (env). Params holds quantile_over_time's φ.
type RangeAgg struct {
	Pos      Position
	Func     RangeFunc
	Grouping *Grouping
	Params   []float64
	Range    *LogRange
}

func (*RangeAgg) exprNode() {}

func (a *RangeAgg) String() string {
	var b strings.Builder
	b.WriteString(a.Func.String())
	b.WriteString("(")
	for i, p := range a.Params {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(formatNumber(p))
	}
	if len(a.Params) > 0 {
		b.WriteString(", ")
	}
	b.WriteString(a.Range.String())
	b.WriteString(")")
	if g := a.Grouping.String(); g != "" {
		b.WriteString(" ")
		b.WriteString(g)
	}
	return b.String()
}

// VectorAgg is a vector aggregation over another expression:
// sum by (level) (count_over_time({…}[1m])), topk(3, …). Params holds
// topk/bottomk's k. Grouping renders before the parentheses, though the
// grammar also accepts it after. Grafana's logs-volume query is exactly
// this shape over a pipeline-carrying LogRange; see the LogRange doc.
type VectorAgg struct {
	Pos      Position
	Func     VectorFunc
	Grouping *Grouping
	Params   []float64
	Left     Expr
}

func (*VectorAgg) exprNode() {}

func (a *VectorAgg) String() string {
	var b strings.Builder
	b.WriteString(a.Func.String())
	if g := a.Grouping.String(); g != "" {
		b.WriteString(" ")
		b.WriteString(g)
		b.WriteString(" (")
	} else {
		b.WriteString("(")
	}
	for i, p := range a.Params {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(formatNumber(p))
	}
	if len(a.Params) > 0 {
		b.WriteString(", ")
	}
	b.WriteString(a.Left.String())
	b.WriteString(")")
	return b.String()
}

// BinOp is a scalar or vector binary operator.
type BinOp int

// Binary operators.
const (
	OpAdd    BinOp = iota // +
	OpSub                 // -
	OpMul                 // *
	OpDiv                 // /
	OpMod                 // %
	OpPow                 // ^
	OpEq                  // ==
	OpNeq                 // !=
	OpGt                  // >
	OpGte                 // >=
	OpLt                  // <
	OpLte                 // <=
	OpAnd                 // and
	OpOr                  // or
	OpUnless              // unless
)

func (op BinOp) String() string {
	switch op {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpMod:
		return "%"
	case OpPow:
		return "^"
	case OpEq:
		return "=="
	case OpNeq:
		return "!="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	case OpUnless:
		return "unless"
	}
	return "BinOp(" + strconv.Itoa(int(op)) + ")"
}

// IsComparison reports whether op is one of ==, !=, >, >=, <, <=.
func (op BinOp) IsComparison() bool { return op >= OpEq && op <= OpLte }

// precedence returns the binding power of op in scalar and vector
// expressions, PromQL's table (syntax.y:89-95): or 1, and/unless 2,
// comparisons 3, + and - 4, * / % 5 and ^ 6.
func (op BinOp) precedence() int {
	switch op {
	case OpOr:
		return 1
	case OpAnd, OpUnless:
		return 2
	case OpEq, OpNeq, OpGt, OpGte, OpLt, OpLte:
		return 3
	case OpAdd, OpSub:
		return 4
	case OpMul, OpDiv, OpMod:
		return 5
	}
	return 6 // ^, right-associative
}

// rightAssociative reports whether op groups right to left; only ^ does.
func (op BinOp) rightAssociative() bool { return op == OpPow }

// formatNumber renders a scalar with its shortest exact spelling.
func formatNumber(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// MatchCard is the cardinality of a vector/vector match.
type MatchCard int

// Vector matching cardinalities.
const (
	MatchOneToOne  MatchCard = iota
	MatchManyToOne           // group_left
	MatchOneToMany           // group_right
)

func (c MatchCard) String() string {
	switch c {
	case MatchManyToOne:
		return "group_left"
	case MatchOneToMany:
		return "group_right"
	}
	return ""
}

// VectorMatching is the on/ignoring/group_left/group_right modifier of a
// vector/vector binary operation. Phase 2 parses these but evaluates them as
// plan.ErrUnsupported (deferred families); they exist in the AST so A2 can
// tell a valid deferred query from a malformed one.
type VectorMatching struct {
	Pos     Position
	On      bool // on (…) when true, ignoring (…) when false
	Card    MatchCard
	Names   []string
	Include []string // the group_left/group_right argument labels
}

func (m *VectorMatching) String() string {
	keyword := "ignoring"
	if m.On {
		keyword = "on"
	}
	s := keyword + " (" + strings.Join(m.Names, ", ") + ")"
	if m.Card != MatchOneToOne {
		s += " " + m.Card.String()
		if len(m.Include) > 0 {
			s += " (" + strings.Join(m.Include, ", ") + ")"
		}
	}
	return s
}

// BinaryExpr is a binary operation: 2 * 1, rate({…}[5m]) > 10, vector op
// vector. Bool makes a comparison yield 0/1 instead of filtering.
type BinaryExpr struct {
	Pos      Position
	Op       BinOp
	LHS      Expr
	RHS      Expr
	Bool     bool
	Matching *VectorMatching // nil without on/ignoring/group_*
}

func (*BinaryExpr) exprNode() {}

func (e *BinaryExpr) String() string {
	var b strings.Builder
	b.WriteString(renderBinaryChild(e.LHS, e.Op, false))
	b.WriteString(" ")
	b.WriteString(e.Op.String())
	if e.Bool {
		b.WriteString(" bool")
	}
	if e.Matching != nil {
		b.WriteString(" ")
		b.WriteString(e.Matching.String())
	}
	b.WriteString(" ")
	b.WriteString(renderBinaryChild(e.RHS, e.Op, true))
	return b.String()
}

// renderBinaryChild parenthesizes a binary child when it binds looser than
// its parent, or equally tight on the wrong side for the associativity, so
// the rendered form reparses to the same tree.
func renderBinaryChild(n Expr, parent BinOp, right bool) string {
	s := n.String()
	child, ok := n.(*BinaryExpr)
	if !ok {
		return s
	}
	cp, pp := child.Op.precedence(), parent.precedence()
	needParens := cp < pp
	if cp == pp && (parent.rightAssociative() != right || child.Op != parent) {
		needParens = true
	}
	if needParens {
		return "(" + s + ")"
	}
	return s
}

// NumberLit is a scalar literal. Loki's NUMBER token is what its scanner
// reads as an int or float — Inf and NaN are identifiers there and never
// number literals, so they do not parse here either. Derived values may be
// non-finite at evaluation (1/0), but no input spelling produces them.
type NumberLit struct {
	Pos Position
	V   float64
}

func (*NumberLit) exprNode() {}

func (l *NumberLit) String() string {
	return formatNumber(l.V)
}

// VectorLiteral is vector(scalar): one sample with no labels at every
// evaluation step. Loki's grammar takes a bare NUMBER here, not an arbitrary
// scalar expression (syntax.y:470-472), so V is the literal's value and no
// sign is part of the production.
type VectorLiteral struct {
	Pos Position
	V   float64
}

func (*VectorLiteral) exprNode() {}

// String renders vector(NUMBER) in canonical LogQL.
func (v *VectorLiteral) String() string {
	return "vector(" + formatNumber(v.V) + ")"
}

// deferredExpr, deferredStage and deferredFilter stand in for valid LogQL
// families Parkana defers: label_replace(), variants(),
// rate_counter, approx_topk, | unpack, | decolorize, ip() filters and the
// |> and !> pattern line filters. The parser fully validates a deferred
// construct's tokens, types, regexes and arguments before building one, and
// Parse never returns a tree containing them — it reports
// plan.ErrUnsupported for the query instead. They exist only so parsing can
// continue validating the rest of the query the way Loki's grammar accepts
// those families.
type deferredExpr struct {
	pos  Position
	what string
}

func (*deferredExpr) exprNode() {}

func (e *deferredExpr) String() string { return e.what }

type deferredStage struct {
	pos  Position
	what string
}

func (*deferredStage) stageNode() {}

func (s *deferredStage) String() string { return s.what }

type deferredFilter struct {
	pos  Position
	what string
}

func (*deferredFilter) stageNode()       {}
func (*deferredFilter) labelFilterExpr() {}

func (f *deferredFilter) String() string { return f.what }
