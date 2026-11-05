package logs

import (
	"fmt"
	"slices"
	"strconv"
)

// ParseLabels parses a Loki stream selector like `{a="b", c="d"}` into a
// sorted label set. Only the `=` matcher is supported: selectors with match
// operators (`!=`, `=~`, `!~`), bare label names or a trailing comma are
// rejected. Whitespace is allowed around tokens; values may be double-quoted
// strings with Go escapes or backquoted raw strings, as in LogQL.
func ParseLabels(s string) (Labels, error) {
	p := &selectorParser{s: s}
	ls, err := p.parse()
	if err != nil {
		return nil, err
	}
	return ls, nil
}

type selectorParser struct {
	s string
	i int
}

func (p *selectorParser) parse() (Labels, error) {
	p.ws()
	if !p.eat('{') {
		return nil, p.err("expected '{'")
	}
	var pairs []Label
	p.ws()
	if p.eat('}') {
		return p.finish(pairs)
	}
	for {
		p.ws()
		name, err := p.name()
		if err != nil {
			return nil, err
		}
		p.ws()
		if !p.eat('=') {
			return nil, p.err("expected '=' after label name " + strconv.Quote(name) +
				" (match operators !=, =~ and !~ are not supported)")
		}
		p.ws()
		value, err := p.quoted()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, Label{Name: name, Value: value})
		p.ws()
		if p.eat(',') {
			continue
		}
		if p.eat('}') {
			return p.finish(pairs)
		}
		return nil, p.err("expected ',' or '}'")
	}
}

func (p *selectorParser) finish(pairs []Label) (Labels, error) {
	p.ws()
	if p.i != len(p.s) {
		return nil, p.err("unexpected text after '}'")
	}
	names := make([]string, len(pairs))
	for i, l := range pairs {
		names[i] = l.Name
	}
	slices.Sort(names)
	for i := 1; i < len(names); i++ {
		if names[i-1] == names[i] {
			return nil, fmt.Errorf("logs: duplicate label %q", names[i])
		}
	}
	return New(pairs...), nil
}

func (p *selectorParser) ws() {
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *selectorParser) eat(c byte) bool {
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *selectorParser) err(msg string) error {
	return fmt.Errorf("logs: %s at offset %d in %q", msg, p.i, p.s)
}

func (p *selectorParser) name() (string, error) {
	start := p.i
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && p.i > start:
		default:
			goto done
		}
		p.i++
	}
done:
	n := p.s[start:p.i]
	if !validName(n) {
		return "", p.err("expected a label name")
	}
	return n, nil
}

// quoted scans a label value starting at p.i. Double-quoted values may
// contain escapes; backquoted values are raw, as in Go and LogQL.
func (p *selectorParser) quoted() (string, error) {
	if p.i >= len(p.s) {
		return "", p.err("expected a quoted label value")
	}
	start := p.i
	switch q := p.s[p.i]; q {
	case '`':
		p.i++
		for p.i < len(p.s) && p.s[p.i] != '`' {
			p.i++
		}
		if p.i >= len(p.s) {
			return "", p.err("unterminated raw string")
		}
		p.i++
	case '"':
		p.i++
		closed := false
		for p.i < len(p.s) {
			c := p.s[p.i]
			p.i++
			if c == '\\' {
				if p.i >= len(p.s) {
					return "", p.err("unterminated escape in label value")
				}
				p.i++
			} else if c == '"' {
				closed = true
				break
			}
		}
		if !closed {
			return "", p.err("unterminated label value")
		}
	default:
		return "", p.err("expected a quoted label value")
	}
	v, err := strconv.Unquote(p.s[start:p.i])
	if err != nil {
		return "", fmt.Errorf("logs: invalid label value %s: %v", p.s[start:p.i], err)
	}
	return v, nil
}
