// Label filters: the | label-filter stage and its boolean combinations.
//
// String comparisons follow Loki's NewStringLabelFilter: a missing label reads
// as "", __error__ reads the parser error state, and =~ / !~ use the
// pipeline-label regexp path (syntax.Perl without DotNL plus Loki's label
// optimizer), never the stream-matcher DotNL semantics. Numeric, duration and
// byte comparisons mirror NumericLabelFilter, DurationLabelFilter and
// BytesLabelFilter: a missing label drops the row without an error, an
// unparsable present value keeps the row and records LabelFilterErr only when
// no earlier error is set.
//
// BinaryLabelFilter evaluates both sides of an AND even when the left side is
// already false, because the right side's conversion error is observable. OR
// short-circuits when the left side keeps the row.
package logql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// labelFilterer reports whether a row survives.
type labelFilterer interface {
	filter(e *logEntry) bool
}

// labelValueMatcher decides whether a label value passes a string matcher.
type labelValueMatcher interface {
	matchValue(v string) bool
}

// compileLabelValueRegexp compiles the pipeline-label regexp path: Loki's
// parseRegexpFilter(match, isLabel=true) through the shared optimizer in
// select.go. Literals become whole-value equality, .* matches everything, .+
// is a non-empty test, a literal+.* concatenation becomes a substring test and
// the unsimplified fallback is anchored ^(?:...)$ with syntax.Perl (no DotNL).
func compileLabelValueRegexp(pattern string) (labelValueMatcher, error) {
	m, err := lokiRegexp(pattern, true)
	if err != nil {
		return nil, err
	}
	return lineValueMatcher{m: m}, nil
}

type lineValueMatcher struct{ m lineMatcher }

func (m lineValueMatcher) matchValue(v string) bool { return m.m.match(v) }

type labelFilterStage struct {
	f labelFilterer
}

func (s labelFilterStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	return s.f.filter(e), nil
}

// compileLabelFilterLeaf compiles one comparison.
func compileLabelFilterLeaf(l *LabelFilterLeaf) (pipelineStage, error) {
	f, err := newLabelFilter(l)
	if err != nil {
		return nil, err
	}
	return labelFilterStage{f: f}, nil
}

// compileLabelFilterBinary compiles an and/or combination.
func compileLabelFilterBinary(b *LabelFilterBinary) (pipelineStage, error) {
	f, err := compileLabelFilterExpr(b)
	if err != nil {
		return nil, err
	}
	return labelFilterStage{f: f}, nil
}

func compileLabelFilterExpr(expr LabelFilterExpr) (labelFilterer, error) {
	switch e := expr.(type) {
	case *LabelFilterLeaf:
		return newLabelFilter(e)
	case *LabelFilterBinary:
		lhs, err := compileLabelFilterExpr(e.LHS)
		if err != nil {
			return nil, err
		}
		rhs, err := compileLabelFilterExpr(e.RHS)
		if err != nil {
			return nil, err
		}
		return binaryLabelFilter{and: e.Op != OpFilterOr, lhs: lhs, rhs: rhs}, nil
	default:
		return nil, fmt.Errorf("logql: %w: label filter %q is not supported", plan.ErrUnsupported, expr.String())
	}
}

// binaryLabelFilter is BinaryLabelFilter.Process.
type binaryLabelFilter struct {
	and      bool
	lhs, rhs labelFilterer
}

func (f binaryLabelFilter) filter(e *logEntry) bool {
	l := f.lhs.filter(e)
	if !f.and && l {
		return true
	}
	r := f.rhs.filter(e)
	if !f.and {
		return l || r
	}
	return l && r
}

func newLabelFilter(l *LabelFilterLeaf) (labelFilterer, error) {
	switch l.Op {
	case OpLabelEq, OpLabelNeq:
		switch l.Value.Kind {
		case StringValue:
			return newStringLabelFilter(l.Label, l.Op, l.Value.Str, false)
		case NumberValue:
			return numericLabelFilter{name: l.Label, op: l.Op, value: l.Value.Num}, nil
		case DurationValue:
			return durationLabelFilter{name: l.Label, op: l.Op, value: l.Value.Dur}, nil
		case BytesValue:
			return bytesLabelFilter{name: l.Label, op: l.Op, value: l.Value.Bytes}, nil
		}
	case OpLabelRe, OpLabelNre:
		if l.Value.Kind != StringValue {
			return nil, fmt.Errorf("logql: %w: label filter %s %s needs a string value", plan.ErrBadQuery, l.Label, l.Op)
		}
		return newStringLabelFilter(l.Label, l.Op, l.Value.Str, true)
	case OpLabelGt, OpLabelGte, OpLabelLt, OpLabelLte:
		switch l.Value.Kind {
		case NumberValue:
			return numericLabelFilter{name: l.Label, op: l.Op, value: l.Value.Num}, nil
		case DurationValue:
			return durationLabelFilter{name: l.Label, op: l.Op, value: l.Value.Dur}, nil
		case BytesValue:
			return bytesLabelFilter{name: l.Label, op: l.Op, value: l.Value.Bytes}, nil
		}
	}
	return nil, fmt.Errorf("logql: %w: label filter %s is not supported", plan.ErrBadQuery, l.String())
}

// newStringLabelFilter builds the string comparison. Regexp comparisons use
// Loki's pipeline-label optimizer: =~ compiles the optimized matcher, !~
// negates it (so !~ ".*" matches nothing, exactly like NewNotFilter(True)).
func newStringLabelFilter(name string, op LabelOp, value string, regex bool) (labelFilterer, error) {
	var m labelValueMatcher
	if regex {
		re, err := compileLabelValueRegexp(value)
		if err != nil {
			return nil, fmt.Errorf("logql: %w: label filter %s %s %q: %w", plan.ErrBadQuery, name, op, value, err)
		}
		m = re
	} else {
		m = equalValueMatcher{value: value}
	}
	if op == OpLabelNeq || op == OpLabelNre {
		m = notValueMatcher{inner: m}
	}
	return stringLabelFilter{name: name, matcher: m}, nil
}

type stringLabelFilter struct {
	name    string
	matcher labelValueMatcher
}

// filter uses Loki's labelValue: __error__ reads the error state, every other
// missing label reads "".
func (f stringLabelFilter) filter(e *logEntry) bool {
	v := ""
	if isErrorLabel(f.name) {
		v = e.errValue()
	} else if got, _, ok := e.get(f.name); ok {
		v = got
	}
	return f.matcher.matchValue(v)
}

type equalValueMatcher struct{ value string }

func (m equalValueMatcher) matchValue(v string) bool { return v == m.value }

type notValueMatcher struct{ inner labelValueMatcher }

func (m notValueMatcher) matchValue(v string) bool { return !m.inner.matchValue(v) }

// numericLabelFilter is NumericLabelFilter.
type numericLabelFilter struct {
	name  string
	op    LabelOp
	value float64
}

func (f numericLabelFilter) filter(e *logEntry) bool {
	s, _, ok := e.get(f.name)
	if !ok {
		return false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		e.setErrIfAbsent(labelFilterErr, err.Error())
		return true
	}
	switch f.op {
	case OpLabelEq:
		return v == f.value
	case OpLabelNeq:
		return v != f.value
	case OpLabelGt:
		return v > f.value
	case OpLabelGte:
		return v >= f.value
	case OpLabelLt:
		return v < f.value
	case OpLabelLte:
		return v <= f.value
	}
	return false
}

// durationLabelFilter is DurationLabelFilter. The field conversion is
// time.ParseDuration, unlike a duration literal on the right-hand side, so a
// field value "1d" is an error even though 1d is a valid literal.
type durationLabelFilter struct {
	name  string
	op    LabelOp
	value time.Duration
}

func (f durationLabelFilter) filter(e *logEntry) bool {
	s, _, ok := e.get(f.name)
	if !ok {
		return false
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		e.setErrIfAbsent(labelFilterErr, err.Error())
		return true
	}
	switch f.op {
	case OpLabelEq:
		return v == f.value
	case OpLabelNeq:
		return v != f.value
	case OpLabelGt:
		return v > f.value
	case OpLabelGte:
		return v >= f.value
	case OpLabelLt:
		return v < f.value
	case OpLabelLte:
		return v <= f.value
	}
	return false
}

// bytesLabelFilter is BytesLabelFilter.
type bytesLabelFilter struct {
	name  string
	op    LabelOp
	value uint64
}

func (f bytesLabelFilter) filter(e *logEntry) bool {
	s, _, ok := e.get(f.name)
	if !ok {
		return false
	}
	v, err := parseBytesField(s)
	if err != nil {
		e.setErrIfAbsent(labelFilterErr, err.Error())
		return true
	}
	switch f.op {
	case OpLabelEq:
		return v == f.value
	case OpLabelNeq:
		return v != f.value
	case OpLabelGt:
		return v > f.value
	case OpLabelGte:
		return v >= f.value
	case OpLabelLt:
		return v < f.value
	case OpLabelLte:
		return v <= f.value
	}
	return false
}

// bytesUnitMultiplier is go-humanize v1.0.1's size table as a switch, so the
// package holds no mutable lookup state. Keys are the lowercased unit.
func bytesUnitMultiplier(unit string) (uint64, bool) {
	switch unit {
	case "b", "":
		return 1, true
	case "kib", "ki":
		return 1 << 10, true
	case "kb", "k":
		return 1000, true
	case "mib", "mi":
		return 1 << 20, true
	case "mb", "m":
		return 1000 * 1000, true
	case "gib", "gi":
		return 1 << 30, true
	case "gb", "g":
		return 1000 * 1000 * 1000, true
	case "tib", "ti":
		return 1 << 40, true
	case "tb", "t":
		return 1000 * 1000 * 1000 * 1000, true
	case "pib", "pi":
		return 1 << 50, true
	case "pb", "p":
		return 1000 * 1000 * 1000 * 1000 * 1000, true
	case "eib", "ei":
		return 1 << 60, true
	case "eb", "e":
		return 1000 * 1000 * 1000 * 1000 * 1000 * 1000, true
	}
	return 0, false
}

// parseBytesField is a port of go-humanize ParseBytes: leading digits, dots
// and commas form the number (commas are removed), the rest is a
// case-insensitive unit, and a bare number means bytes. Fractional values
// truncate toward zero after scaling, and values at or above 2^64 fail.
func parseBytesField(s string) (uint64, error) {
	lastDigit := 0
	hasComma := false
	for _, r := range s {
		if !(unicode.IsDigit(r) || r == '.' || r == ',') {
			break
		}
		if r == ',' {
			hasComma = true
		}
		lastDigit++
	}
	num := s[:lastDigit]
	if hasComma {
		num = strings.ReplaceAll(num, ",", "")
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}
	extra := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	m, ok := bytesUnitMultiplier(extra)
	if !ok {
		return 0, fmt.Errorf("unhandled size name: %v", extra)
	}
	f *= float64(m)
	if f >= math.MaxUint64 {
		return 0, fmt.Errorf("too large: %v", s)
	}
	return uint64(f), nil
}
