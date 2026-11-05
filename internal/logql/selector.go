// The stream selector grammar, the part A1 parses, following Loki's
// selector/matchers rules: a matcher must follow '{' and every ','.
//
//	selector := '{' matcher { ',' matcher } '}'
//	matcher  := label op string
//	op       := '=' | '!=' | '=~' | '!~'
//
// Strings are double-quoted (Go escapes, as PromQL) or backtick raw. The
// full expression grammar (pipelines, aggregations, scalars) is A2's and
// extends this parser; selectors stay here because ParseSelector and the
// metadata endpoints use them alone.

package logql

import (
	"strings"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// Selector is a stream selector: {app="api", env=~"prod|staging"}. Matchers
// are plan.Matchers, the frozen query contract: regexps are validated and
// compiled, and a missing label has the value "".
type Selector struct {
	Pos      Position
	Matchers []plan.Matcher
}

func (s Selector) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, m := range s.Matchers {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(m.String())
	}
	b.WriteByte('}')
	return b.String()
}

// ParseSelector parses a bare stream selector, as the /series match[]
// parameter and label endpoints use, rejecting any trailing input. It errors
// wrapping plan.ErrBadQuery for invalid selectors, including selectors whose
// matchers all match the empty string (they would select every stream; Loki
// rejects them too).
func ParseSelector(q string) (*Selector, error) {
	p, err := newParser(q)
	if err != nil {
		return nil, err
	}
	sel, err := parseSelector(p)
	if err != nil {
		return nil, err
	}
	if t := p.cur(); t.Kind != tokEOF {
		return nil, badQuery(t.Pos, "unexpected %s after selector; ParseSelector takes a bare selector", t)
	}
	return sel, nil
}

// parseSelector parses the selector grammar; A2's expression parser calls it
// after a '{'. Matchers are separated by exactly one comma, and a matcher
// must follow every comma (no trailing comma). The all-match-empty check
// runs here so every entry point gets it.
func parseSelector(p *parser) (*Selector, error) {
	open := p.next()
	if open.Kind != tokLBrace {
		return nil, badQuery(open.Pos, "expected '{' to start a selector, found %s", open)
	}
	sel := &Selector{Pos: open.Pos}
	expectMatcher := true
	for {
		t := p.cur()
		if t.Kind == tokEOF {
			return nil, badQuery(t.Pos, "unterminated selector, missing '}'")
		}
		if !expectMatcher && t.Kind == tokRBrace {
			p.next()
			if matchersAllMatchEmpty(sel.Matchers) {
				return nil, badQuery(sel.Pos, `selector needs at least one matcher that does not match "" (a missing label matches "")`)
			}
			return sel, nil
		}
		if expectMatcher && t.Kind == tokComma {
			return nil, badQuery(t.Pos, "expected label name, found ','")
		}
		m, err := parseMatcher(p) // reports expected-label-name errors itself
		if err != nil {
			return nil, err
		}
		sel.Matchers = append(sel.Matchers, m)
		expectMatcher = false
		switch t := p.cur(); t.Kind {
		case tokRBrace, tokEOF: // handled at the top of the loop
		case tokComma:
			p.next()
			expectMatcher = true
		default:
			return nil, badQuery(t.Pos, "expected ',' or '}' between matchers, found %s", t)
		}
	}
}

// parseMatcher parses label op string, building a validated plan.Matcher.
// Label names reject Loki's unconditional keywords case-insensitively
// (parser.go reservedWord): they lex as keyword tokens in Loki, so
// {and="x"} and {json="x"} are syntax errors there, while function names
// (sum, rate, ip, ...) stay valid identifiers without a parenthesis.
func parseMatcher(p *parser) (plan.Matcher, error) {
	name := p.next()
	if name.Kind != tokIdent {
		return plan.Matcher{}, badQuery(name.Pos, "expected label name, found %s", name)
	}
	if reservedWord(name.Text) {
		return plan.Matcher{}, badQuery(name.Pos, "%s is a reserved word and cannot be a label name", name.Text)
	}
	op := p.next()
	var typ plan.MatchType
	switch op.Kind {
	case tokAssign:
		typ = plan.MatchEqual
	case tokNeq:
		typ = plan.MatchNotEqual
	case tokRe:
		typ = plan.MatchRegexp
	case tokNre:
		typ = plan.MatchNotRegexp
	default:
		return plan.Matcher{}, badQuery(op.Pos, "expected matcher operator (=, !=, =~, !~), found %s", op)
	}
	val := p.next()
	if val.Kind != tokString {
		return plan.Matcher{}, badQuery(val.Pos, "expected quoted string label value, found %s", val)
	}
	value := labelMatcherValue(typ, val.Str)
	m, err := plan.NewMatcher(typ, name.Text, value)
	if err != nil {
		return plan.Matcher{}, badQuery(op.Pos, "invalid matcher: %v", err)
	}
	return m, nil
}

// matchersAllMatchEmpty reports whether every matcher accepts the empty
// string, so the selector would match every possible label set.
func matchersAllMatchEmpty(ms []plan.Matcher) bool {
	for _, m := range ms {
		if !m.MatchesEmpty() {
			return false
		}
	}
	return true
}
