// Parser core shared by the selector parser (selector.go, A1) and the full
// expression parser (A2): the token stream, the parse-error constructors and
// shared keyword classification. The binary-operator precedence table lives
// on BinOp in ast.go and the label-filter one on BoolOp.
//
// Compile-stage boundary: lexAll turns a query string into tokens (errors
// wrap plan.ErrBadQuery), the parser turns tokens into the AST of ast.go,
// and evaluation (A4+) walks the AST into a plan.Selection plus a Go
// pipeline.
//
// Error contract (plan.md "Query path", Loki-compatible): malformed queries
// wrap plan.ErrBadQuery; queries whose syntax is valid LogQL but belong to a
// family Parkana does not evaluate wrap plan.ErrUnsupported. Syntax and type
// validation always run first: a deferred construct with a malformed payload
// is ErrBadQuery, never ErrUnsupported. Keyword and function names match
// Loki's lexer case-insensitively; only the logfmt parser flags
// (--strict, --keep-empty) are case-sensitive.
//
// The parser is recursive descent over the pre-lexed token list, so the
// paren-shape lookahead in parenIsLog and the adjacency checks for signed
// offsets and parser flags are plain token comparisons. Recursion is bounded
// by maxExprDepth to keep adversarial input from exhausting the stack.

package logql

import (
	"fmt"
	"strings"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// maxExprDepth bounds parser recursion (parentheses, nested aggregations,
// binary and label-filter expressions) and, through the depth checks in the
// precedence loops, the AST depth of left-deep chains built iteratively.
// Loki's goyacc parser has no explicit limit; this one is a guard against
// stack exhaustion, set far above anything real queries or Grafana
// generate.
const maxExprDepth = 500

// maxInputSize matches Loki's query size limit (pinned parser.go line 42,
// checked with >= in ParseExprWithoutValidation): inputs at or above it are
// rejected before lexing, so an oversized quoted matcher never parses.
const maxInputSize = 131072

// badQuery returns a syntax error at pos wrapping plan.ErrBadQuery, the
// sentinel handlers map to Loki-style 400s.
func badQuery(pos Position, format string, args ...any) error {
	return fmt.Errorf("logql: parse error at %s: %s: %w", pos, fmt.Sprintf(format, args...), plan.ErrBadQuery)
}

// unsupportedQuery returns an error at pos wrapping plan.ErrUnsupported for
// a syntactically valid LogQL family Parkana does not evaluate. It is only
// returned after the whole query passed syntax and type validation.
func unsupportedQuery(pos Position, format string, args ...any) error {
	return fmt.Errorf("logql: parse error at %s: %s: %w", pos, fmt.Sprintf(format, args...), plan.ErrUnsupported)
}

// Parse parses a complete LogQL expression: a log query, a range or vector
// aggregation, a scalar expression, or a binary operation over those. It
// consumes the whole input and returns errors wrapping plan.ErrBadQuery for
// invalid grammar (including trailing tokens) and plan.ErrUnsupported for
// valid LogQL Parkana defers (vector/vector binary operations with
// on/ignoring/group_* matching, vector(), label_replace(), variants(),
// rate_counter, approx_topk, | unpack, | decolorize, ip() filters and the
// |> and !> pattern line filters).
func Parse(q string) (Expr, error) {
	p, err := newParser(q)
	if err != nil {
		return nil, err
	}
	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if t := p.cur(); t.Kind != tokEOF {
		return nil, badQuery(t.Pos, "unexpected %s after expression", t)
	}
	// A bare log range is not an expression in Loki's grammar: ranges only
	// exist inside range aggregation functions (syntax.y: root expr has no
	// logRangeExpr production).
	if r, ok := e.(*LogRange); ok {
		return nil, badQuery(r.Pos, "a range selector needs a range aggregation function")
	}
	if err := p.firstDeferred(); err != nil {
		return nil, err
	}
	return e, nil
}

// parser walks the token list produced by lexAll.
type parser struct {
	toks []token
	i    int
	// depth counts active recursive-descent frames; bounded by maxExprDepth.
	depth int
	// deferred collects plan.ErrUnsupported errors for valid deferred
	// families, reported only after the whole parse validated.
	deferred []error
}

// newParser lexes src and returns a parser positioned at its first token.
// It rejects inputs at or above maxInputSize first, as Loki does.
func newParser(src string) (*parser, error) {
	if len(src) >= maxInputSize {
		return nil, badQuery(Position{Line: 1, Col: 1}, "input size too long (%d > %d)", len(src), maxInputSize)
	}
	toks, err := lexAll(src)
	if err != nil {
		return nil, err
	}
	return &parser{toks: toks}, nil
}

// cur returns the current token; at the end it stays at tokEOF.
func (p *parser) cur() token {
	return p.toks[min(p.i, len(p.toks)-1)]
}

// next returns the current token and advances; it never moves past tokEOF.
func (p *parser) next() token {
	t := p.cur()
	if t.Kind != tokEOF {
		p.i++
	}
	return t
}

// peek returns the token n positions ahead of the cursor without moving it.
func (p *parser) peek(n int) token {
	return p.toks[min(p.i+n, len(p.toks)-1)]
}

// expect consumes the current token if it has kind k, or returns a syntax
// error naming what it wanted and found.
func (p *parser) expect(k tokenKind, context string) (token, error) {
	t := p.cur()
	if t.Kind != k {
		return token{}, badQuery(t.Pos, "expected %s in %s, found %s", k, context, t)
	}
	return p.next(), nil
}

// enter increments the recursion depth, refusing queries nested deeper than
// maxExprDepth.
func (p *parser) enter(pos Position) error {
	p.depth++
	if p.depth > maxExprDepth {
		return badQuery(pos, "expression nested deeper than %d levels", maxExprDepth)
	}
	return nil
}

func (p *parser) leave() { p.depth-- }

// deferUnsupported records a plan.ErrUnsupported error for a fully parsed
// deferred construct. The first recorded one wins.
func (p *parser) deferUnsupported(pos Position, format string, args ...any) {
	p.deferred = append(p.deferred, unsupportedQuery(pos, format, args...))
}

// exprDepth returns the AST depth of e (a leaf is 1), iteratively, bailing
// out at maxExprDepth+1. The precedence-climbing loops build left-deep
// chains iteratively — 1+1+1+… never grows the recursion counter — so they
// check the accumulated depth here instead; the bail keeps each check
// linear-bounded even near the input-size limit.
func exprDepth(e Expr) int {
	maxd := 0
	type item struct {
		n Expr
		d int
	}
	stack := []item{{e, 1}}
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if it.d > maxd {
			maxd = it.d
			if maxd > maxExprDepth {
				return maxd
			}
		}
		if b, ok := it.n.(*BinaryExpr); ok {
			stack = append(stack, item{b.LHS, it.d + 1}, item{b.RHS, it.d + 1})
		}
	}
	return maxd
}

// filterDepth is exprDepth for label-filter expressions.
func filterDepth(f LabelFilterExpr) int {
	maxd := 0
	type item struct {
		n LabelFilterExpr
		d int
	}
	stack := []item{{f, 1}}
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if it.d > maxd {
			maxd = it.d
			if maxd > maxExprDepth {
				return maxd
			}
		}
		if b, ok := it.n.(*LabelFilterBinary); ok {
			stack = append(stack, item{b.LHS, it.d + 1}, item{b.RHS, it.d + 1})
		}
	}
	return maxd
}

// firstDeferred reports the first deferred-family error, if any.
func (p *parser) firstDeferred() error {
	if len(p.deferred) == 0 {
		return nil
	}
	return p.deferred[0]
}

// adjacent reports whether token b starts exactly where token a ends, with
// no whitespace or comment between them. Loki's scanner glues some token
// pairs together this way (negative durations after '-', the --strict
// parser flags); the token stream re-derives the same decision.
func adjacent(a, b token) bool {
	return a.Pos.Offset+len(a.Text) == b.Pos.Offset
}

// reservedWord reports whether name, compared case-insensitively as Loki's
// lexer does, is one of the unconditional keywords: the word entries of
// Loki's tokens map (pkg_logql_syntax_lex.go lines 17-87). Those can never
// be identifiers — {and="x"} is a syntax error in Loki — unlike function
// names (sum, rate, bytes, ip, label_replace, ...), which are identifiers
// unless followed by '(' or a by/without grouping parenthesis.
func reservedWord(name string) bool {
	switch strings.ToLower(name) {
	case "and", "or", "unless", "by", "without", "bool", "offset",
		"on", "ignoring", "group_left", "group_right",
		"unwrap", "json", "regexp", "logfmt", "unpack", "pattern",
		"label_format", "line_format", "decolorize",
		"drop", "keep", "variants", "of":
		return true
	}
	return false
}

// isLabelName reports whether t can serve as a label (or extraction)
// name: a non-reserved identifier. Original spelling is preserved.
func isLabelName(t token) bool {
	return t.Kind == tokIdent && !reservedWord(t.Text)
}

// looksLikeFunction reports whether an identifier token at the cursor would
// lex as a Loki function token: followed by '(', or by a by/without
// grouping followed by '(' (lex.go isFunction). pos is the index of the
// identifier in p.toks.
func (p *parser) looksLikeFunction(pos int) bool {
	after := p.toks[min(pos+1, len(p.toks)-1)]
	if after.Kind == tokLParen {
		return true
	}
	if after.Kind == tokIdent {
		switch strings.ToLower(after.Text) {
		case "by", "without":
			return p.toks[min(pos+2, len(p.toks)-1)].Kind == tokLParen
		}
	}
	return false
}

// parenIsLog reports whether the '(' at the cursor opens a log query group
// rather than a metric expression: some chain of '(' leads to a '{'.
// Metric expressions cannot start with '{', so this is unambiguous.
func (p *parser) parenIsLog() bool {
	j := p.i
	for p.toks[j].Kind == tokLParen && j < len(p.toks)-1 {
		j++
	}
	return p.toks[j].Kind == tokLBrace
}

// expectLabelName consumes an identifier usable as a label name, rejecting
// Loki's unconditional keywords case-insensitively (they lex as keyword
// tokens in Loki, so {json="x"} is a syntax error there).
func (p *parser) expectLabelName(context string) (token, error) {
	t := p.cur()
	if t.Kind != tokIdent {
		return token{}, badQuery(t.Pos, "expected label name in %s, found %s", context, t)
	}
	if reservedWord(t.Text) {
		return token{}, badQuery(t.Pos, "%s is a reserved word and cannot be a label name in %s", t.Text, context)
	}
	return p.next(), nil
}

// identIs reports whether token t is the case-insensitive keyword word
// (which must not be a reserved word context; callers use this for
// context-dependent keywords).
func identIs(t token, word string) bool {
	return t.Kind == tokIdent && strings.EqualFold(t.Text, word)
}
