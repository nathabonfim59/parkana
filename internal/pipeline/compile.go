package pipeline

import (
	"cmp"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// Compile-time and execution limits. Every bound has a regression test that
// exceeds exactly it. MaxNodes and VMMemoryBudget are defense in depth for
// the bounded expression subset (which already rejects the constructs that
// could allocate unboundedly), not the primary safety mechanism.
const (
	// MaxSteps caps the steps of one definition.
	MaxSteps = 32
	// MaxExprBytes caps one expression source (when, drop_if's expr,
	// promote/set_stream's value).
	MaxExprBytes = 4096
	// MaxPatternBytes caps a regex pattern, both in steps and as the literal
	// right side of a matches operator.
	MaxPatternBytes = 1024
	// MaxReplaceBytes caps redact's replacement template.
	MaxReplaceBytes = 256
	// MaxNodes caps the AST size of one expression (expr.MaxNodes).
	MaxNodes = 1000
	// VMMemoryBudget caps the VM's array/map/range allocations per run. The
	// subset forbids those constructs, so this only guards regressions.
	VMMemoryBudget = 100_000
	// MaxMatchIndexes caps the total match-index ints FindAllSubmatchIndex
	// may allocate for one redact before the output-size precheck runs
	// (2 ints per group per match). Exceeding it is a data error.
	MaxMatchIndexes = 1 << 20
	// MaxErrorDetails caps pipeline_error_details.
	MaxErrorDetails = 256
	// maxStreamName is the dataset-name length bound (logs.SanitizeStream).
	maxStreamName = 128
	// sizeSaturation is where static worst-case sizes clamp. Saturating
	// arithmetic compares before it adds or multiplies, so nesting
	// lower/upper or deep concatenation can never overflow int64 and turn
	// negative (a negative size would slip past the cap comparison).
	sizeSaturation = 1 << 62
	// lowerUpperFactor conservatively bounds how much lower/upper can widen
	// a string with Go's simple case mapping (e.g. ȿ, 2 bytes, uppercases
	// to Ȿ, 3 bytes); landed values are still commit-checked.
	lowerUpperFactor = 3
	// numericStringSize bounds string(int)/string(float) results.
	numericStringSize = 32
)

// satClamp clamps one configured input-bound charge (bounds are positive).
func satClamp(n int64) int64 { return min(n, sizeSaturation) }

// satAdd adds two worst-case sizes, both non-negative and at most
// sizeSaturation, comparing before adding so the sum cannot overflow.
func satAdd(a, b int64) int64 {
	if a >= sizeSaturation || b >= sizeSaturation || a > sizeSaturation-b {
		return sizeSaturation
	}
	return a + b
}

// satMul multiplies a worst-case size by a factor ≥ 1, comparing before
// multiplying so the product cannot overflow.
func satMul(a, f int64) int64 {
	if f < 1 {
		return 0
	}
	if a >= sizeSaturation || a > sizeSaturation/f {
		return sizeSaturation
	}
	return a * f
}

// Defaults for the zero fields of CompileOptions. LineBytes matches the
// ingester's default line limit; the registry wires the configured
// -ingest.max-line-size through so a default-keep transform can never
// produce a line the frozen push handler's revalidation would reject.
const (
	DefaultLineBytes        = 256 << 10
	DefaultMetaBytes        = 256 << 10
	DefaultMetaValueBytes   = 16 << 10
	DefaultValueBytesLabels = 2048
)

// CompileOptions configures Compile. StreamLabels is required; the byte
// bounds are optional and default to the constants above. The registry that
// compiles definitions should copy the bounds from the ingester's configured
// limits, so a default-keep transform can never produce a line or metadata
// blob the frozen push handler's revalidation would reject.
type CompileOptions struct {
	// StreamLabels are the stream-routing label keys, in priority order
	// (config.StreamLabels). The first is set_stream's target; DatasetOf
	// resolves datasets with the same order. Required non-empty.
	StreamLabels []string
	// LineBytes bounds the line before and after every step. Zero uses
	// DefaultLineBytes.
	LineBytes int
	// MetaBytes bounds the total metadata (names and values) of an entry.
	// Zero uses DefaultMetaBytes.
	MetaBytes int
	// MetaValueBytes bounds one metadata value. Zero uses
	// DefaultMetaValueBytes.
	MetaValueBytes int
	// LabelValueBytes bounds one label value. Upstream pushes validate label
	// names but not value lengths, so pipelines enforce this themselves.
	// Zero uses DefaultValueBytesLabels.
	LabelValueBytes int
	// ObserveExecution, when set, is called exactly once per Apply
	// invocation with the pipeline's stage, the outcome (check err first:
	// on internal or canceled calls it carries OutcomeKeep with a non-nil
	// err), the error (nil, a *RejectError, or an internal/cancel error) and
	// the elapsed time. It must be safe for concurrent use; registries
	// supply a Prometheus collector-backed observer. Dry-run leaves it nil.
	ObserveExecution func(stage Stage, outcome Outcome, err error, elapsed time.Duration)
}

func (o CompileOptions) resolve() CompileOptions {
	o.StreamLabels = slices.Clone(o.StreamLabels)
	o.LineBytes = cmp.Or(o.LineBytes, DefaultLineBytes)
	o.MetaBytes = cmp.Or(o.MetaBytes, DefaultMetaBytes)
	o.MetaValueBytes = cmp.Or(o.MetaValueBytes, DefaultMetaValueBytes)
	o.LabelValueBytes = cmp.Or(o.LabelValueBytes, DefaultValueBytesLabels)
	return o
}

func (o CompileOptions) validate() error {
	if len(o.StreamLabels) == 0 {
		return fmt.Errorf("pipeline: CompileOptions.StreamLabels is required")
	}
	for _, k := range o.StreamLabels {
		if err := validLabelNameErr(k); err != nil {
			return fmt.Errorf("pipeline: CompileOptions.StreamLabels: %w", err)
		}
	}
	for _, b := range []struct {
		name string
		v    int
	}{
		{"LineBytes", o.LineBytes},
		{"MetaBytes", o.MetaBytes},
		{"MetaValueBytes", o.MetaValueBytes},
		{"LabelValueBytes", o.LabelValueBytes},
	} {
		if b.v <= 0 {
			return fmt.Errorf("pipeline: CompileOptions.%s must be positive", b.name)
		}
	}
	return nil
}

// validLabelNameErr checks one label or metadata name with the logs label
// name rule, [a-zA-Z_][a-zA-Z0-9_]*.
func validLabelNameErr(name string) error {
	if err := (logs.Labels{{Name: name}}).Validate(); err != nil {
		return fmt.Errorf("invalid name: %w", err)
	}
	return nil
}

// envType returns the typed, method-free expression environment: a map, so
// expr exposes no methods, with lowercase variables. labels and meta may
// only be used through literal member access (enforced by the analyzer);
// stream is the entry's original dataset. A fresh map per call; nothing
// mutates it.
func envType() map[string]any {
	return map[string]any{
		"line":   "",
		"labels": map[string]string{},
		"meta":   map[string]string{},
		"stream": "",
	}
}

// Compiled is an immutable, concurrency-safe pipeline. Build it with Compile;
// zero values are not usable. All exported methods are safe for simultaneous
// use by multiple goroutines.
type Compiled struct {
	name     string
	stage    Stage
	selector string
	steps    []compiledStep
	opts     CompileOptions
	// envLabels/envMeta are the literal member names the definition's
	// expressions reference; Apply seeds absent ones with "" so expressions
	// observe missing labels and metadata as empty strings.
	envLabels map[string]struct{}
	envMeta   map[string]struct{}
}

// Name returns the definition's name.
func (c *Compiled) Name() string { return c.name }

// Stage returns the definition's stage.
func (c *Compiled) Stage() Stage { return c.stage }

// Selector returns the definition's dataset selector ("*" or an exact name).
func (c *Compiled) Selector() string { return c.selector }

// DatasetOf returns the sanitized dataset of l over the routing keys the
// pipeline was compiled with. Adapters call it once on the original labels
// and pass the result to every Apply of the request or query.
func (c *Compiled) DatasetOf(l logs.Labels) string { return l.StreamName(c.opts.StreamLabels) }

// compiledStep is one compiled step: the spec plus its compiled artifacts.
type compiledStep struct {
	spec  Step
	onErr OnError
	// when is nil when the step is unconditional.
	when *vm.Program
	// dropIf is drop_if's compiled boolean expression.
	dropIf *vm.Program
	// value is promote's/set_stream's compiled string expression, nil when
	// the step uses a literal From field instead.
	value *vm.Program
	// re is regex_extract's or redact's compiled pattern.
	re *regexp.Regexp
	// fromMeta is the metadata source field of the parse steps; "" means the
	// line.
	fromMeta string
	// replace is redact's replacement (default applied).
	replace string
	// targetMeta is redact's metadata target; "" means the line.
	targetMeta string
}

// Compile validates def and compiles it into an immutable pipeline. All
// schema violations, malformed expressions and read-stage violations are
// compile errors and fail definition validation (PUT and dry-run answer 400
// with the error text).
func Compile(def Definition, opts CompileOptions) (*Compiled, error) {
	if err := def.ValidateSyntax(); err != nil {
		return nil, err
	}
	opts = opts.resolve()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	a := &analyzer{opts: opts, labels: map[string]struct{}{}, metas: map[string]struct{}{}}
	c := &Compiled{
		name:      def.Name,
		stage:     def.Stage,
		selector:  def.Selector,
		opts:      opts,
		envLabels: a.labels,
		envMeta:   a.metas,
	}
	promoted := map[string]struct{}{}
	for i, s := range def.Steps {
		cs, err := compileStep(s, def.Stage, opts, a, promoted)
		if err != nil {
			return nil, fmt.Errorf("pipeline: step %d (%s): %w", i+1, s.Type, err)
		}
		c.steps = append(c.steps, *cs)
	}
	return c, nil
}

// compileStep compiles one step against the shared analyzer (so referenced
// keys are collected across the whole definition) and the running set of
// promote targets (for case-collision checks).
func compileStep(s Step, stage Stage, opts CompileOptions, a *analyzer, promoted map[string]struct{}) (*compiledStep, error) {
	if unknown := s.unknownFields(); len(unknown) > 0 {
		return nil, fmt.Errorf("%s does not take %s", s.Type, strings.Join(unknown, ", "))
	}
	cs := &compiledStep{spec: s, onErr: cmp.Or(s.OnError, OnErrorKeep)}
	if !cs.onErr.Valid() {
		return nil, fmt.Errorf("invalid on_error %q (want keep, drop or reject)", s.OnError)
	}
	if s.When != "" {
		prog, err := a.compileExpr(s.When, kBool)
		if err != nil {
			return nil, fmt.Errorf("when: %w", err)
		}
		cs.when = prog
	}
	if err := validSourceField(s.From, s.Type); err != nil {
		return nil, err
	}
	cs.fromMeta = sourceMeta(s.From)

	switch s.Type {
	case StepParseJSON, StepParseLogfmt:
		// Parsing happens at execution time.
	case StepRegexExtract:
		re, err := compilePattern(s.Pattern)
		if err != nil {
			return nil, err
		}
		names := 0
		for _, name := range re.SubexpNames() {
			if name != "" {
				names++
			}
		}
		if names == 0 {
			return nil, fmt.Errorf("pattern has no named capture group (use (?P<name>...))")
		}
		cs.re = re
	case StepRename:
		if err := validLabelNameErr(s.From); err != nil {
			return nil, fmt.Errorf("from %q: %w", s.From, err)
		}
		if err := metaNameErr("rename destination", s.To); err != nil {
			return nil, err
		}
	case StepRedact:
		re, err := compilePattern(s.Pattern)
		if err != nil {
			return nil, err
		}
		cs.re = re
		cs.replace = "***"
		if s.Replace != nil {
			cs.replace = *s.Replace
		}
		if len(cs.replace) > MaxReplaceBytes {
			return nil, fmt.Errorf("replacement exceeds %d bytes", MaxReplaceBytes)
		}
		if s.Target != "" && s.Target != fromLine {
			if err := metaNameErr("redact target", s.Target); err != nil {
				return nil, err
			}
			cs.targetMeta = s.Target
		}
	case StepDropIf:
		if s.Expr == "" {
			return nil, fmt.Errorf("expr is required")
		}
		prog, err := a.compileExpr(s.Expr, kBool)
		if err != nil {
			return nil, fmt.Errorf("expr: %w", err)
		}
		cs.dropIf = prog
	case StepPromote:
		if stage == StageRead {
			return nil, fmt.Errorf("promote is not allowed at the read stage")
		}
		if err := validLabelNameErr(s.To); err != nil {
			return nil, fmt.Errorf("to %q: %w", s.To, err)
		}
		if schema.IsReserved(s.To) {
			return nil, fmt.Errorf("to %q is a reserved column name", s.To)
		}
		for other := range promoted {
			if schema.Collide(other, s.To) {
				return nil, fmt.Errorf("to %q differs only in case from %q", s.To, other)
			}
		}
		promoted[s.To] = struct{}{}
		if err := compileValueSource(&s, a, cs, "promote"); err != nil {
			return nil, err
		}
	case StepSetStream:
		if stage == StageRead {
			return nil, fmt.Errorf("set_stream is not allowed at the read stage")
		}
		if err := compileValueSource(&s, a, cs, "set_stream"); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown step type %q", s.Type)
	}
	return cs, nil
}

// compileValueSource compiles the exactly-one-of From/Value source of
// promote/set_stream. From is a literal metadata field name ("line" is an
// ordinary metadata field here, not the log line: only the parse steps and
// redact treat it specially).
func compileValueSource(s *Step, a *analyzer, cs *compiledStep, what string) error {
	switch {
	case s.From != "" && s.Value != "":
		return fmt.Errorf("%s takes either from or value, not both", what)
	case s.From != "":
		return validLabelNameErr(s.From)
	case s.Value != "":
		prog, err := a.compileExpr(s.Value, kString)
		if err != nil {
			return fmt.Errorf("value: %w", err)
		}
		cs.value = prog
		return nil
	default:
		return fmt.Errorf("%s takes from or value", what)
	}
}

// compilePattern compiles a step regex pattern: required and capped.
func compilePattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("pattern is required")
	}
	if len(pattern) > MaxPatternBytes {
		return nil, fmt.Errorf("pattern exceeds %d bytes", MaxPatternBytes)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}
	return re, nil
}

// validSourceField checks the From field of the parse steps: the line
// (default) or a metadata field name. rename, promote and set_stream give
// From their own meaning and validate it separately.
func validSourceField(from string, typ StepType) error {
	switch typ {
	case StepParseJSON, StepParseLogfmt, StepRegexExtract:
		if from == "" || from == fromLine {
			return nil
		}
		return validLabelNameErr(from)
	case StepRename, StepPromote, StepSetStream, StepRedact:
		return nil // no From; own rules apply
	default:
		if from != "" {
			return fmt.Errorf("from is not a field of %s", typ)
		}
		return nil
	}
}

// sourceMeta maps a From value to the metadata field it reads; "" is the line.
func sourceMeta(from string) string {
	if from == "" || from == fromLine {
		return ""
	}
	return from
}

// stepFields returns, per step type, the members that carry meaning besides
// the common when/on_error. Compile rejects members a type does not use, so
// typos cannot silently no-op.
func stepFields(t StepType) []string {
	switch t {
	case StepParseJSON, StepParseLogfmt:
		return []string{"from"}
	case StepRegexExtract:
		return []string{"pattern", "from"}
	case StepRename:
		return []string{"from", "to"}
	case StepRedact:
		return []string{"pattern", "replace", "target"}
	case StepDropIf:
		return []string{"expr"}
	case StepPromote:
		return []string{"from", "to", "value"}
	case StepSetStream:
		return []string{"from", "value"}
	default:
		return nil
	}
}

// unknownFields returns the members of s that its type does not use: both
// non-empty fields and members merely present in the JSON (explicitly empty
// or null). UnmarshalJSON records a present when like any other member, so
// the presence check allows the common members for every type too.
func (s Step) unknownFields() []string {
	allowed := slices.Concat(stepFields(s.Type), []string{"when", "on_error"})
	var unknown []string
	for _, f := range []struct {
		name string
		set  bool
	}{
		{"from", s.From != ""},
		{"to", s.To != ""},
		{"pattern", s.Pattern != ""},
		{"replace", s.Replace != nil},
		{"target", s.Target != ""},
		{"expr", s.Expr != ""},
		{"value", s.Value != ""},
	} {
		if f.set && !slices.Contains(allowed, f.name) {
			unknown = append(unknown, f.name)
		}
	}
	for _, name := range s.present {
		if !slices.Contains(allowed, name) && !slices.Contains(unknown, name) {
			unknown = append(unknown, name)
		}
	}
	return unknown
}

// kind is the static type of an expression node in the bounded subset.
type kind uint8

const (
	kString kind = iota
	kBool
	kInt
	kFloat
)

func (k kind) String() string {
	switch k {
	case kString:
		return "string"
	case kBool:
		return "boolean"
	case kInt:
		return "int"
	case kFloat:
		return "float"
	}
	return "kind"
}

func numeric(k kind) bool { return k == kInt || k == kFloat }

// analyzer validates the bounded expression subset and collects the literal
// labels/meta member names an expression references. One analyzer spans a
// whole definition so Apply can seed every referenced key at once.
type analyzer struct {
	opts   CompileOptions
	labels map[string]struct{}
	metas  map[string]struct{}
}

// compileExpr analyzes src against the bounded subset and compiles it to a
// program whose result kind is want. Parsing happens twice (once here, once
// inside expr.Compile); sources are capped at MaxExprBytes, so the cost is
// negligible, and the second parse applies the checker against the typed env.
func (a *analyzer) compileExpr(src string, want kind) (*vm.Program, error) {
	if len(src) > MaxExprBytes {
		return nil, fmt.Errorf("expression exceeds %d bytes", MaxExprBytes)
	}
	tree, err := parser.Parse(src)
	if err != nil {
		return nil, fmt.Errorf("invalid expression: %w", err)
	}
	got, _, err := a.infer(tree.Node)
	if err != nil {
		return nil, err
	}
	if got != want {
		return nil, fmt.Errorf("expression must be %s, got %s", want, got)
	}
	opts := []expr.Option{
		expr.Env(envType()),
		expr.MaxNodes(MaxNodes),
		expr.Optimize(false),
		expr.DisableAllBuiltins(),
	}
	// The deterministic, pure builtin allowlist: every function's output is
	// bounded by its input. replace is deliberately absent (empty needles
	// allocate quadratically; bounded replacement is redact's job), as are
	// all collection, time and randomness builtins. Operators (matches,
	// contains, startsWith, endsWith, in) are opcodes, not builtins.
	for _, name := range []string{
		"len", "lower", "upper", "trim", "trimPrefix", "trimSuffix",
		"indexOf", "int", "float", "string",
	} {
		opts = append(opts, expr.EnableBuiltin(name))
	}
	if want == kBool {
		opts = append(opts, expr.AsBool(), expr.WarnOnAny())
	} else {
		opts = append(opts, expr.AsKind(reflect.String), expr.WarnOnAny())
	}
	return expr.Compile(src, opts...)
}

// infer type-checks one node against the subset and returns its kind and the
// conservative worst-case byte size of string results. Anything outside the
// subset is an error; there is no fallback typing.
func (a *analyzer) infer(n ast.Node) (kind, int64, error) {
	switch n := n.(type) {
	case *ast.StringNode:
		return kString, int64(len(n.Value)), nil
	case *ast.IntegerNode:
		return kInt, 0, nil
	case *ast.FloatNode:
		return kFloat, 0, nil
	case *ast.BoolNode:
		return kBool, 0, nil
	case *ast.IdentifierNode:
		switch n.Value {
		case "line":
			return kString, satClamp(int64(a.opts.LineBytes)), nil
		case "stream":
			return kString, maxStreamName, nil
		case "labels", "meta":
			return 0, 0, fmt.Errorf("%s may only be used with a literal member access, like %s.name", n.Value, n.Value)
		default:
			return 0, 0, fmt.Errorf("unknown name %q (want line, labels, meta or stream)", n.Value)
		}
	case *ast.MemberNode:
		return a.inferMember(n)
	case *ast.UnaryNode:
		k, _, err := a.infer(n.Node)
		if err != nil {
			return 0, 0, err
		}
		switch n.Operator {
		case "!":
			if k != kBool {
				return 0, 0, fmt.Errorf("! needs a boolean operand")
			}
			return kBool, 0, nil
		case "-", "+":
			if !numeric(k) {
				return 0, 0, fmt.Errorf("%s needs a numeric operand", n.Operator)
			}
			return k, 0, nil
		default:
			return 0, 0, fmt.Errorf("unsupported unary operator %q", n.Operator)
		}
	case *ast.BinaryNode:
		return a.inferBinary(n)
	case *ast.ConditionalNode:
		if !n.Ternary {
			return 0, 0, fmt.Errorf("if/else is not supported; use the ternary operator")
		}
		ck, _, err := a.infer(n.Cond)
		if err != nil {
			return 0, 0, err
		}
		if ck != kBool {
			return 0, 0, fmt.Errorf("ternary condition must be boolean")
		}
		k1, s1, err := a.infer(n.Exp1)
		if err != nil {
			return 0, 0, err
		}
		k2, s2, err := a.infer(n.Exp2)
		if err != nil {
			return 0, 0, err
		}
		if k1 == kString && k2 == kString {
			return kString, max(s1, s2), nil
		}
		if numeric(k1) && numeric(k2) {
			if k1 == kFloat || k2 == kFloat {
				return kFloat, 0, nil
			}
			return kInt, 0, nil
		}
		if k1 == kBool && k2 == kBool {
			return kBool, 0, nil
		}
		return 0, 0, fmt.Errorf("ternary branches have different types (%s and %s)", k1, k2)
	case *ast.BuiltinNode:
		return a.inferBuiltin(n)
	default:
		return 0, 0, fmt.Errorf("unsupported expression construct %s", nodeKind(n))
	}
}

// nodeKind names a node for error messages.
func nodeKind(n ast.Node) string {
	s := strings.TrimSpace(fmt.Sprintf("%T", n))
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return s[i+1:]
	}
	return s
}

// inferMember handles the only member access in the subset: a literal
// property of labels or meta.
func (a *analyzer) inferMember(n *ast.MemberNode) (kind, int64, error) {
	if n.Optional {
		return 0, 0, fmt.Errorf("optional member access (?.) is not supported")
	}
	if n.Method {
		return 0, 0, fmt.Errorf("method calls are not available")
	}
	id, ok := n.Node.(*ast.IdentifierNode)
	if !ok {
		return 0, 0, fmt.Errorf("member access is only supported on labels and meta")
	}
	prop, ok := n.Property.(*ast.StringNode)
	if !ok {
		return 0, 0, fmt.Errorf("%s must be accessed with a literal member name", id.Value)
	}
	key := prop.Value
	switch id.Value {
	case "labels":
		if err := validLabelNameErr(key); err != nil {
			return 0, 0, fmt.Errorf("labels.%s: %w", key, err)
		}
		a.labels[key] = struct{}{}
		return kString, satClamp(int64(a.opts.LabelValueBytes)), nil
	case "meta":
		if err := validLabelNameErr(key); err != nil {
			return 0, 0, fmt.Errorf("meta.%s: %w", key, err)
		}
		a.metas[key] = struct{}{}
		return kString, satClamp(int64(a.opts.MetaValueBytes)), nil
	default:
		return 0, 0, fmt.Errorf("member access is only supported on labels and meta, not %q", id.Value)
	}
}

// inferBinary type-checks the binary operators of the subset. For string
// concatenation it enforces the static expansion cap: the sum of the
// operands' worst-case sizes must not exceed LineBytes, which bounds every
// string an expression can build (lower/upper add at most a constant factor
// on top and are commit-checked where the value lands).
func (a *analyzer) inferBinary(n *ast.BinaryNode) (kind, int64, error) {
	kl, sl, err := a.infer(n.Left)
	if err != nil {
		return 0, 0, err
	}
	// `in` is the one operator whose right side must NOT be inferred: the
	// list of literals is validated as a whole by checkInList.
	if n.Operator == "in" {
		if err := a.checkInList(n.Right); err != nil {
			return 0, 0, err
		}
		return kBool, 0, nil
	}
	kr, sr, err := a.infer(n.Right)
	if err != nil {
		return 0, 0, err
	}
	switch n.Operator {
	case "&&", "||", "and", "or":
		if kl != kBool || kr != kBool {
			return 0, 0, fmt.Errorf("%s needs boolean operands", n.Operator)
		}
		return kBool, 0, nil
	case "==", "!=":
		if !kindsCompatible(kl, kr) {
			return 0, 0, fmt.Errorf("cannot compare %s with %s", kl, kr)
		}
		return kBool, 0, nil
	case "<", "<=", ">", ">=":
		if !((numeric(kl) && numeric(kr)) || (kl == kString && kr == kString)) {
			return 0, 0, fmt.Errorf("%s needs two numbers or two strings", n.Operator)
		}
		return kBool, 0, nil
	case "+":
		if kl == kString && kr == kString {
			size := satAdd(sl, sr)
			if size > satClamp(int64(a.opts.LineBytes)) {
				return 0, 0, fmt.Errorf("concatenation may build %d bytes, exceeding the %d-byte limit", size, a.opts.LineBytes)
			}
			return kString, size, nil
		}
		if numeric(kl) && numeric(kr) {
			if kl == kFloat || kr == kFloat {
				return kFloat, 0, nil
			}
			return kInt, 0, nil
		}
		return 0, 0, fmt.Errorf("+ needs two strings or two numbers")
	case "-", "*", "/", "%":
		if !numeric(kl) || !numeric(kr) {
			return 0, 0, fmt.Errorf("%s needs numeric operands", n.Operator)
		}
		if kl == kFloat || kr == kFloat {
			return kFloat, 0, nil
		}
		return kInt, 0, nil
	case "contains", "startsWith", "endsWith":
		if kl != kString || kr != kString {
			return 0, 0, fmt.Errorf("%s needs string operands", n.Operator)
		}
		return kBool, 0, nil
	case "matches":
		if kl != kString || kr != kString {
			return 0, 0, fmt.Errorf("matches needs string operands")
		}
		lit, ok := n.Right.(*ast.StringNode)
		if !ok {
			return 0, 0, fmt.Errorf("matches needs a literal pattern on the right")
		}
		if len(lit.Value) > MaxPatternBytes {
			return 0, 0, fmt.Errorf("matches pattern exceeds %d bytes", MaxPatternBytes)
		}
		// Compile the pattern here: with the optimizer off, an invalid
		// literal must still fail at definition compile time, not at run.
		if _, err := regexp.Compile(lit.Value); err != nil {
			return 0, 0, fmt.Errorf("invalid matches pattern: %w", err)
		}
		return kBool, 0, nil
	case "in":
		return kBool, 0, nil // handled before the right side is inferred
	default:
		return 0, 0, fmt.Errorf("unsupported operator %q", n.Operator)
	}
}

// kindsCompatible reports whether == / != may compare the two kinds.
func kindsCompatible(a, b kind) bool {
	if a == b {
		return true
	}
	return numeric(a) && numeric(b)
}

// checkInList verifies that the right side of `in` is a list of scalar
// literals, the only collection the subset allows.
func (a *analyzer) checkInList(n ast.Node) error {
	arr, ok := n.(*ast.ArrayNode)
	if !ok {
		return fmt.Errorf("in needs a list of literals")
	}
	for _, el := range arr.Nodes {
		switch el.(type) {
		case *ast.StringNode, *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode:
		default:
			return fmt.Errorf("in needs a list of literals")
		}
	}
	return nil
}

// inferBuiltin type-checks the allowlisted builtins. Every one is pure,
// deterministic and output-bounded by its input.
func (a *analyzer) inferBuiltin(n *ast.BuiltinNode) (kind, int64, error) {
	args := func(want ...kind) (int64, error) {
		if len(n.Arguments) != len(want) {
			return 0, fmt.Errorf("%s takes %d argument(s)", n.Name, len(want))
		}
		var size int64
		for i, w := range want {
			k, s, err := a.infer(n.Arguments[i])
			if err != nil {
				return 0, err
			}
			if k != w {
				return 0, fmt.Errorf("%s: argument %d must be %s", n.Name, i+1, w)
			}
			size = max(size, s)
		}
		return size, nil
	}
	switch n.Name {
	case "len":
		if _, err := args(kString); err != nil {
			return 0, 0, err
		}
		return kInt, 0, nil
	case "lower", "upper":
		size, err := args(kString)
		if err != nil {
			return 0, 0, err
		}
		return kString, satMul(size, lowerUpperFactor), nil
	case "trim":
		size, err := args(kString)
		if err != nil {
			return 0, 0, err
		}
		return kString, size, nil
	case "trimPrefix", "trimSuffix":
		// The result is at most the trimmed input; args returns the max of
		// the two operand sizes, a valid upper bound.
		size, err := args(kString, kString)
		if err != nil {
			return 0, 0, err
		}
		return kString, size, nil
	case "indexOf":
		if _, err := args(kString, kString); err != nil {
			return 0, 0, err
		}
		return kInt, 0, nil
	case "int", "float":
		if len(n.Arguments) != 1 {
			return 0, 0, fmt.Errorf("%s takes one argument", n.Name)
		}
		k, _, err := a.infer(n.Arguments[0])
		if err != nil {
			return 0, 0, err
		}
		if !numeric(k) && k != kString {
			return 0, 0, fmt.Errorf("%s takes one numeric or string argument", n.Name)
		}
		if n.Name == "int" {
			return kInt, 0, nil
		}
		return kFloat, 0, nil
	case "string":
		if len(n.Arguments) != 1 {
			return 0, 0, fmt.Errorf("string takes one argument")
		}
		k, s, err := a.infer(n.Arguments[0])
		if err != nil {
			return 0, 0, err
		}
		switch k {
		case kString:
			return kString, s, nil
		case kInt, kFloat:
			return kString, numericStringSize, nil
		default:
			return 0, 0, fmt.Errorf("string needs a string or number")
		}
	default:
		return 0, 0, fmt.Errorf("function %q is not allowed", n.Name)
	}
}
