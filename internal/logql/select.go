// Source selection for stored log queries: turning a compiled query and a
// request into a plan.Selection, and the Loki-compatible direction, grouping
// and interval rules applied to what the Source yields.
//
// Pushdown (PLAN.md decision 4): stream matchers always go down, and so do
// line filters that run before any stage can change the line. plan.Selection
// ANDs its line filters, so Loki's positive or-groups stay in Go; a negative
// or-group is an AND of negations and does go down. A Source limit is pushed
// only when every selected row is emitted unchanged: a pre-limit in front of a
// filter, pipeline or interval would underfill the result.
package logql

import (
	"context"
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// ceilMicro returns the first microsecond-grid instant at or after t, the
// inclusive Selection.From that matches Loki's inclusive request start. Stored
// timestamps lie on the microsecond grid, so an unaligned start must round up:
// truncating would include a row before the requested start.
func ceilMicro(t time.Time) time.Time {
	trunc := t.Truncate(time.Microsecond)
	if trunc.Equal(t) {
		return trunc.UTC()
	}
	return trunc.Add(time.Microsecond).UTC()
}

// throughMicro returns the exclusive Selection.Through for a log query's
// request end. Loki selects log entries in [start, end): a line stamped
// exactly at end is not returned (verified against Loki 3.7.9 with lines on
// both endpoints). Stored timestamps are truncated to the microsecond, so an
// unaligned end rounds up: a stored row t < end on the nanosecond scale is
// kept. A row whose original nanosecond timestamp lay in [end, ceil(end))
// was truncated below end and is kept too; that sub-microsecond case cannot
// be decided after truncation.
func throughMicro(t time.Time) time.Time {
	return ceilMicro(t)
}

// selection builds the storage request for p. It never sets a Source limit
// unless the query can emit every selected row unchanged.
func (c *CompiledLogQuery) selection(p plan.Params) plan.Selection {
	sel := plan.Selection{
		Tenant:      p.Tenant,
		Matchers:    slices.Clone(c.matchers),
		LineFilters: slices.Clone(c.pushdown),
		From:        ceilMicro(p.Start),
		Through:     throughMicro(p.End),
		Direction:   p.Direction,
	}
	if c.exact && p.Interval == 0 && p.Limit > 0 {
		sel.Limit = p.Limit
	}
	return sel
}

// intervalPass reports whether ts may be emitted after prev, the previous
// emitted row. Loki samples the merged iterator globally, so the interval is
// measured against the last emitted row of any stream, and equality at the
// boundary passes in both directions (engine.go readStreams).
func intervalPass(prev, ts time.Time, interval time.Duration, dir plan.Direction) bool {
	if dir == plan.Forward {
		return !ts.Before(prev.Add(interval))
	}
	return !ts.After(prev.Add(-interval))
}

// streamSet accumulates entries under their final stream labels while keeping
// the Source's order inside each stream. Loki groups after the pipeline, so
// the key is the labels Process returns, not the stored row labels.
type streamSet struct {
	index   map[string]int
	streams []plan.Stream
}

func (s *streamSet) add(labels logs.Labels, e plan.Entry) {
	key := labels.String()
	i, ok := s.index[key]
	if !ok {
		if s.index == nil {
			s.index = make(map[string]int)
		}
		i = len(s.streams)
		s.index[key] = i
		s.streams = append(s.streams, plan.Stream{Labels: labels})
	}
	s.streams[i].Entries = append(s.streams[i].Entries, e)
}

// result returns the streams sorted by label set, as Loki's readStreams does.
func (s *streamSet) result() []plan.Stream {
	slices.SortFunc(s.streams, func(a, b plan.Stream) int {
		return strings.Compare(a.Labels.String(), b.Labels.String())
	})
	return s.streams
}

// lineMatchKind is one node of a compiled Loki line-filter predicate.
type lineMatchKind int

const (
	lineMatchTrue     lineMatchKind = iota // every line passes (.*, empty match)
	lineMatchExists                        // len(line) > 0 (.+)
	lineMatchContains                      // byte substring, optionally case-folded
	lineMatchEqual                         // whole-line equality (label regexp literals)
	lineMatchRegexp                        // RE2 regexp, Loki's unsimplified fallback
	lineMatchNot                           // !subs[0]
	lineMatchAnd                           // subs[0] && subs[1] && ...
	lineMatchOr                            // subs[0] || subs[1] || ...
)

// lineMatcher is a compiled Loki regexp/filter predicate, shared by line
// filters (isLabel=false) and A5's pipeline label filters (isLabel=true). It
// mirrors Loki's MatcherFilterer tree (pkg/logql/log/filter.go) closely enough
// to preserve the observable semantics: notably .+ is a non-empty test
// (existsFilter.Filter, filter.go:138-142), so a newline-only value passes it
// even though Go's regexp does not let '.' match a newline. Empty line
// contains values become trueFilter (newContainsFilter, filter.go:556-567),
// while a label literal uses equalFilter's whole-value test
// (filter.go:394-428); both follow parseRegexpFilter (filter.go:645-668).
type lineMatcher struct {
	kind   lineMatchKind
	substr string // lineMatchContains, lineMatchEqual
	fold   bool   // case-folded, as Loki's containsLower
	re     *regexp.Regexp
	subs   []lineMatcher
}

func (m lineMatcher) match(line string) bool {
	switch m.kind {
	case lineMatchTrue:
		return true
	case lineMatchExists:
		return len(line) > 0
	case lineMatchContains:
		if m.fold {
			return containsLower(line, m.substr)
		}
		return strings.Contains(line, m.substr)
	case lineMatchEqual:
		if len(m.substr) != len(line) {
			return false
		}
		if m.fold {
			return containsLower(line, m.substr)
		}
		return line == m.substr
	case lineMatchRegexp:
		return m.re.MatchString(line)
	case lineMatchNot:
		return !m.subs[0].match(line)
	case lineMatchAnd:
		for _, sub := range m.subs {
			if !sub.match(line) {
				return false
			}
		}
		return true
	case lineMatchOr:
		for _, sub := range m.subs {
			if sub.match(line) {
				return true
			}
		}
		return false
	}
	return false
}

func trueMatcher() lineMatcher { return lineMatcher{kind: lineMatchTrue} }

func existsMatcher() lineMatcher { return lineMatcher{kind: lineMatchExists} }

// containsMatcher mirrors newContainsFilter: an empty needle matches every
// line (Loki returns TrueFilter for it), and a case-insensitive needle is
// lowered once at construction, as newContainsFilter does.
func containsMatcher(substr string, fold bool) lineMatcher {
	if substr == "" {
		return trueMatcher()
	}
	if fold {
		substr = strings.ToLower(substr)
	}
	return lineMatcher{kind: lineMatchContains, substr: substr, fold: fold}
}

// equalMatcher mirrors newEqualFilter (filter.go:423-428): a whole-value
// comparison with no empty special case, so an empty literal matches only the
// empty value. It is what isLabel regexp literals simplify to.
func equalMatcher(substr string, fold bool) lineMatcher {
	if fold {
		substr = strings.ToLower(substr)
	}
	return lineMatcher{kind: lineMatchEqual, substr: substr, fold: fold}
}

func notMatcher(m lineMatcher) lineMatcher {
	return lineMatcher{kind: lineMatchNot, subs: []lineMatcher{m}}
}

func orMatchers(ms ...lineMatcher) lineMatcher {
	// Loki's newOrFilter (filter.go:309-322) treats TrueFilter as an identity
	// and *drops* it, not as an absorbing element: foo|.* is foo, and a
	// surface |= "" or "foo" is foo. ChainOrMatcherFilterer/ChainOrFilter
	// build both the optimizer's and the surface's or-groups this way.
	kept := make([]lineMatcher, 0, len(ms))
	for _, m := range ms {
		if m.kind == lineMatchTrue {
			continue
		}
		kept = append(kept, m)
	}
	switch len(kept) {
	case 0:
		return trueMatcher()
	case 1:
		return kept[0]
	}
	return lineMatcher{kind: lineMatchOr, subs: kept}
}

func andMatchers(ms ...lineMatcher) lineMatcher {
	// NewAndFilter (filter.go:194-208) and NewAndFilters also treat true as
	// the identity and skip it.
	kept := make([]lineMatcher, 0, len(ms))
	for _, m := range ms {
		if m.kind == lineMatchTrue {
			continue
		}
		kept = append(kept, m)
	}
	switch len(kept) {
	case 0:
		return trueMatcher()
	case 1:
		return kept[0]
	}
	return lineMatcher{kind: lineMatchAnd, subs: kept}
}

// containsLower is an exact port of Loki's case-insensitive substring test
// (log/filter.go containsLower, lines 446-535). substr must already be
// lowercased, as newContainsFilter/newEqualFilter guarantee. The ASCII fast
// path is only a prefilter: once a byte is non-ASCII the Unicode path decodes
// runes and compares unicode.ToLower(lineRune) with the already-lowercase
// pattern rune, so (?i)é matches É. Invalid UTF-8 bytes fall back to a raw
// byte comparison exactly as the pinned code does; do not replace this with
// strings.EqualFold or a plain ToLower comparison.
func containsLower(line, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(line) {
		return false
	}
	firstByte := substr[0]
	maxIndex := len(line) - len(substr)
	for i := 0; i <= maxIndex; i++ {
		c := line[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != firstByte {
			continue
		}
		matched := true
		linePos, substrPos := i, 0
		for linePos < len(line) && substrPos < len(substr) {
			c := line[linePos]
			s := substr[substrPos]
			if c < utf8.RuneSelf && s < utf8.RuneSelf {
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				if c != s {
					matched = false
					break
				}
				linePos++
				substrPos++
				continue
			}
			lr, lineSize := utf8.DecodeRuneInString(line[linePos:])
			if lr == utf8.RuneError && lineSize == 1 {
				// Invalid UTF-8 in the line: compare the raw byte.
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				if c != s {
					matched = false
					break
				}
				linePos++
				substrPos++
				continue
			}
			mr, substrSize := utf8.DecodeRuneInString(substr[substrPos:])
			if mr == utf8.RuneError && substrSize == 1 {
				// Invalid UTF-8 in the pattern; substr is lowercased valid
				// UTF-8 in practice, but keep the pinned failure mode.
				matched = false
				break
			}
			if unicode.ToLower(lr) != mr {
				matched = false
				break
			}
			linePos += lineSize
			substrPos += substrSize
		}
		if matched && substrPos == len(substr) {
			return true
		}
	}
	return false
}

// compileLineFilter compiles one line-filter stage. Positive operators with
// or-alternatives are a union (Loki's Or chain); negative ones are the
// intersection of the negated alternatives, because !(a or b) == !a and !b
// (syntax/ast.go newOrLineFilterExpr, lines 473-495).
func compileLineFilter(f *LineFilter) (lineMatcher, error) {
	ms := make([]lineMatcher, 0, len(f.Values))
	for _, v := range f.Values {
		var m lineMatcher
		switch f.Op {
		case plan.LineContains:
			m = containsMatcher(v, false)
		case plan.LineRegexp:
			var err error
			m, err = lokiRegexp(v, false)
			if err != nil {
				return lineMatcher{}, err
			}
		case plan.LineNotContains:
			m = notMatcher(containsMatcher(v, false))
		case plan.LineNotRegexp:
			sub, err := lokiRegexp(v, false)
			if err != nil {
				return lineMatcher{}, err
			}
			m = notMatcher(sub)
		default:
			return lineMatcher{}, fmt.Errorf("logql: unknown line filter operator %d: %w", f.Op, plan.ErrUnsupported)
		}
		ms = append(ms, m)
	}
	if f.Op == plan.LineContains || f.Op == plan.LineRegexp {
		return orMatchers(ms...), nil
	}
	return andMatchers(ms...), nil
}

// lokiRegexp compiles a regexp the way Loki's parseRegexpFilter does
// (filter.go:645-668): try the default simplifier, then fall back to the
// original RE2 language. isLabel selects label-regexp semantics: literals
// simplify to whole-value equality and the fallback is anchored ^(?:...)$;
// line filters pass false, where literals simplify to contains and the
// fallback is unanchored. The simplifier is observable: .+ becomes
// len(value)>0, so a newline-only value passes .+ even though Go's regexp does
// not match a newline with '.'. A5's pipeline label filters call this with
// isLabel=true on the raw pattern; no selector DotNL helper is involved.
func lokiRegexp(pattern string, isLabel bool) (lineMatcher, error) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return lineMatcher{}, fmt.Errorf("logql: regexp %q: %w: %w", pattern, plan.ErrBadQuery, err)
	}
	re = re.Simplify()
	if m, ok := simplifyRegexp(re, isLabel); ok {
		return m, nil
	}
	// Loki calls util.AllNonGreedy here; greediness changes which match is
	// reported, never whether a value matches, so the simplified tree's
	// language is what this predicate needs.
	rendered := re.String()
	if isLabel {
		rendered = "^(?:" + rendered + ")$"
	}
	compiled, err := regexp.Compile(rendered)
	if err != nil {
		return lineMatcher{}, fmt.Errorf("logql: regexp %q: %w: %w", pattern, plan.ErrBadQuery, err)
	}
	return lineMatcher{kind: lineMatchRegexp, re: compiled}, nil
}

// clearCapture mirrors util.ClearCapture (pkg/util/regex.go:29-35): a capture
// node is replaced by its only child. Loki clears captures on the children of
// alternations and concatenations before simplifying them, which changes what
// the simplifier sees (for example (foo).* becomes a literal+star concat) and
// must be preserved.
func clearCapture(re *syntax.Regexp) {
	if re.Op == syntax.OpCapture {
		*re = *re.Sub[0]
	}
}

func clearCaptures(subs []*syntax.Regexp) {
	for _, sub := range subs {
		clearCapture(sub)
	}
}

// simplifyRegexp ports Loki's default RegexSimplifier (pkg/logql/log/filter.go
// Simplify, lines 696-722). isLabel propagates through alternation and capture
// but deliberately not into concat, which always uses contains (pinned
// simplifyConcat, lines 749-806). The second result reports whether the
// pattern could be simplified; when false the caller uses the RE2 fallback.
func simplifyRegexp(re *syntax.Regexp, isLabel bool) (lineMatcher, bool) {
	switch re.Op {
	case syntax.OpAlternate:
		return simplifyAlternate(re, isLabel)
	case syntax.OpConcat:
		// simplifyConcat has no isLabel parameter in Loki: concatenation
		// literals always become contains filters, even for labels.
		return simplifyConcat(re, nil, false)
	case syntax.OpCapture:
		clearCapture(re)
		return simplifyRegexp(re, isLabel)
	case syntax.OpLiteral:
		if isLabel {
			return equalMatcher(string(re.Rune), re.Flags&syntax.FoldCase != 0), true
		}
		return containsMatcher(string(re.Rune), re.Flags&syntax.FoldCase != 0), true
	case syntax.OpStar:
		if re.Sub[0].Op == syntax.OpAnyCharNotNL {
			return trueMatcher(), true
		}
	case syntax.OpPlus:
		if len(re.Sub) == 1 && re.Sub[0].Op == syntax.OpAnyCharNotNL {
			return existsMatcher(), true
		}
	case syntax.OpEmptyMatch:
		return trueMatcher(), true
	}
	return lineMatcher{}, false
}

// simplifyAlternate ports simplifyAlternate (filter.go:724-742): each branch is
// simplified with the same isLabel and combined with Loki's or identity rule.
func simplifyAlternate(re *syntax.Regexp, isLabel bool) (lineMatcher, bool) {
	clearCaptures(re.Sub)
	m, ok := simplifyRegexp(re.Sub[0], isLabel)
	if !ok {
		return lineMatcher{}, false
	}
	for _, sub := range re.Sub[1:] {
		subMatcher, ok := simplifyRegexp(sub, isLabel)
		if !ok {
			return lineMatcher{}, false
		}
		m = orMatchers(m, subMatcher)
	}
	return m, true
}

// simplifyConcat ports simplifyConcat (filter.go:749-806): only literal, .*
// and alternate concatenations simplify; base is the literal an enclosing
// alternate concatenation contributed. It intentionally has no isLabel
// parameter: concat literals are always contains filters.
func simplifyConcat(re *syntax.Regexp, base []byte, baseFold bool) (lineMatcher, bool) {
	clearCaptures(re.Sub)
	subs := make([]*syntax.Regexp, 0, len(re.Sub))
	for _, sub := range re.Sub {
		if sub.Op != syntax.OpEmptyMatch {
			subs = append(subs, sub)
		}
	}
	if len(subs) > 3 {
		return lineMatcher{}, false
	}
	var (
		curr        lineMatcher
		haveCurr    bool
		literals    int
		literal     = slices.Clone(base)
		haveLit     = base != nil
		literalFold = baseFold
	)
	for _, sub := range subs {
		if sub.Op == syntax.OpLiteral {
			if literals != 0 {
				return lineMatcher{}, false
			}
			literals++
			literal = append(literal, []byte(string(sub.Rune))...)
			haveLit = true
			literalFold = sub.Flags&syntax.FoldCase != 0
			continue
		}
		if sub.Op == syntax.OpAlternate && haveLit {
			m, ok := simplifyConcatAlternate(sub, literal, literalFold, curr, haveCurr)
			if !ok {
				return lineMatcher{}, false
			}
			curr, haveCurr = m, true
			continue
		}
		if sub.Op == syntax.OpStar && sub.Sub[0].Op == syntax.OpAnyCharNotNL {
			continue
		}
		return lineMatcher{}, false
	}
	if haveCurr {
		return curr, true
	}
	if haveLit {
		return containsMatcher(string(literal), literalFold), true
	}
	return lineMatcher{}, false
}

// simplifyConcatAlternate ports simplifyConcatAlternate (filter.go:812-851).
func simplifyConcatAlternate(re *syntax.Regexp, literal []byte, fold bool, curr lineMatcher, haveCurr bool) (lineMatcher, bool) {
	for _, alt := range re.Sub {
		if !fold && alt.Flags&syntax.FoldCase != 0 {
			return lineMatcher{}, false
		}
		var m lineMatcher
		switch alt.Op {
		case syntax.OpEmptyMatch:
			m = containsMatcher(string(literal), fold)
		case syntax.OpLiteral:
			joined := append(slices.Clone(literal), []byte(string(alt.Rune))...)
			m = containsMatcher(string(joined), fold)
		case syntax.OpConcat:
			var ok bool
			if m, ok = simplifyConcat(alt, literal, fold); !ok {
				return lineMatcher{}, false
			}
		case syntax.OpStar:
			if alt.Sub[0].Op != syntax.OpAnyCharNotNL {
				return lineMatcher{}, false
			}
			m = containsMatcher(string(literal), fold)
		default:
			return lineMatcher{}, false
		}
		if haveCurr {
			curr = orMatchers(curr, m)
		} else {
			curr, haveCurr = m, true
		}
	}
	if !haveCurr {
		return lineMatcher{}, false
	}
	return curr, true
}

// pushableLineFilters returns plan line filters whose conjunction is
// language-equivalent to m, or ok=false when plan.Selection cannot express it
// (it only ANDs, and has no "line is non-empty" filter). The result is used in
// addition to the Go-side predicate, so it must never be stronger than m.
func pushableLineFilters(m lineMatcher) ([]plan.LineFilter, bool) {
	switch m.kind {
	case lineMatchTrue:
		return nil, true
	case lineMatchContains:
		if m.fold {
			return nil, false // plan.LineContains is case-sensitive
		}
		f, err := plan.NewLineFilter(plan.LineContains, m.substr)
		if err != nil {
			return nil, false
		}
		return []plan.LineFilter{f}, true
	case lineMatchRegexp:
		f, err := plan.NewLineFilter(plan.LineRegexp, m.re.String())
		if err != nil {
			return nil, false
		}
		return []plan.LineFilter{f}, true
	case lineMatchNot:
		sub := m.subs[0]
		switch sub.kind {
		case lineMatchContains:
			if sub.fold {
				return nil, false
			}
			f, err := plan.NewLineFilter(plan.LineNotContains, sub.substr)
			if err != nil {
				return nil, false
			}
			return []plan.LineFilter{f}, true
		case lineMatchRegexp:
			f, err := plan.NewLineFilter(plan.LineNotRegexp, sub.re.String())
			if err != nil {
				return nil, false
			}
			return []plan.LineFilter{f}, true
		case lineMatchOr:
			// !(a or b) == !a and !b.
			var out []plan.LineFilter
			for _, s := range sub.subs {
				fs, ok := pushableLineFilters(notMatcher(s))
				if !ok {
					return nil, false
				}
				out = append(out, fs...)
			}
			return out, true
		}
		return nil, false
	case lineMatchAnd:
		var out []plan.LineFilter
		for _, sub := range m.subs {
			fs, ok := pushableLineFilters(sub)
			if !ok {
				return nil, false
			}
			out = append(out, fs...)
		}
		return out, true
	}
	return nil, false
}

// lineFilterStage applies one compiled line-filter stage to the line. Parsers
// and formatting do not exist until A5/A6, so the line is immutable today;
// A6's line_format must stop pushdown after it, which compileLogQuery already
// tracks.
type lineFilterStage struct {
	m lineMatcher
}

func (s lineFilterStage) process(_ context.Context, e *logEntry) (bool, error) {
	return s.m.match(e.line), nil
}
