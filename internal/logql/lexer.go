// Lexer for LogQL: positions, tokens and the scanner. Shared by the selector
// parser (this milestone) and the full expression parser (A2). The grammar is
// documented in selector.go and, later, parser.go; this file owns only the
// lexical rules: whitespace and # comments, identifiers, numbers, durations,
// byte sizes, quoted and raw strings, and operators.
//
// Duration and byte-size tokens follow Loki 3.7.9's lexer (lead-saved
// /tmp/sol-a/loki-3.7.9/pkg_logql_syntax_lex.go): a number tries a duration
// suffix first (model.ParseDuration units y w d h m s ms, then
// time.ParseDuration units ns us µs with decimals, repeats and any order),
// then a byte-size suffix (humanize.ParseBytes: SI kb=1000, IEC kib=1024,
// fractions truncate, e-forms rejected), and only then lexes as a plain
// number, leaving the letters to later tokens.

package logql

import (
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Position is a location in a query string. Line and Col are 1-based; Col
// counts bytes. Offset is the byte offset from the start of the query.
type Position struct {
	Offset int
	Line   int
	Col    int
}

func (p Position) String() string {
	return strconv.Itoa(p.Line) + ":" + strconv.Itoa(p.Col)
}

// tokenKind names a lexical token.
type tokenKind uint8

// Token kinds. Matcher operators (=, !=, =~, !~) and line-filter operators
// (|=, !=, |~, !~) share tokens; the parser decides by context.
const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokDuration
	tokBytes
	tokString

	tokComma
	tokLBrace
	tokRBrace
	tokLBracket
	tokRBracket
	tokLParen
	tokRParen

	// Matcher and scalar comparison operators.
	tokAssign // =
	tokNeq    // !=
	tokEq     // ==
	tokRe     // =~
	tokNre    // !~
	tokGt     // >
	tokGte    // >=
	tokLt     // <
	tokLte    // <=

	// Arithmetic operators.
	tokPlus
	tokMinus
	tokStar
	tokSlash
	tokPercent
	tokCaret

	// Pipeline operators.
	tokPipe        // |
	tokPipeEq      // |=
	tokPipeRe      // |~
	tokPipePattern // |> (Loki pattern line filter, deferred in phase 2)
	tokNotPattern  // !> (Loki negative pattern line filter, deferred)
)

func (k tokenKind) String() string {
	switch k {
	case tokEOF:
		return "end of query"
	case tokIdent:
		return "identifier"
	case tokNumber:
		return "number"
	case tokDuration:
		return "duration"
	case tokBytes:
		return "bytes"
	case tokString:
		return "string"
	case tokComma:
		return "','"
	case tokLBrace:
		return "'{'"
	case tokRBrace:
		return "'}'"
	case tokLBracket:
		return "'['"
	case tokRBracket:
		return "']'"
	case tokLParen:
		return "'('"
	case tokRParen:
		return "')'"
	case tokAssign:
		return "'='"
	case tokNeq:
		return "'!='"
	case tokEq:
		return "'=='"
	case tokRe:
		return "'=~'"
	case tokNre:
		return "'!~'"
	case tokGt:
		return "'>'"
	case tokGte:
		return "'>='"
	case tokLt:
		return "'<'"
	case tokLte:
		return "'<='"
	case tokPlus:
		return "'+'"
	case tokMinus:
		return "'-'"
	case tokStar:
		return "'*'"
	case tokSlash:
		return "'/'"
	case tokPercent:
		return "'%'"
	case tokCaret:
		return "'^'"
	case tokPipe:
		return "'|'"
	case tokPipeEq:
		return "'|='"
	case tokPipeRe:
		return "'|~'"
	case tokPipePattern:
		return "'|>'"
	case tokNotPattern:
		return "'!>'"
	}
	return "token(" + strconv.Itoa(int(k)) + ")"
}

// token is one lexical token. The value fields are set for their kind only:
// Str for tokString, Num for tokNumber, Dur for tokDuration and Bytes for
// tokBytes. Text always holds the raw source text.
type token struct {
	Kind  tokenKind
	Pos   Position
	Text  string
	Str   string
	Num   float64
	Dur   time.Duration
	Bytes uint64
}

// String renders a token for error messages.
func (t token) String() string {
	if t.Kind == tokEOF {
		return "end of query"
	}
	return strconv.Quote(t.Text)
}

// scanner splits a query string into tokens.
type scanner struct {
	src  string
	off  int
	line int
	col  int
}

func newScanner(src string) *scanner {
	return &scanner{src: src, line: 1, col: 1}
}

func (s *scanner) pos() Position {
	return Position{Offset: s.off, Line: s.line, Col: s.col}
}

// advance moves past the next n bytes, counting lines.
func (s *scanner) advance(n int) {
	for range n {
		if s.off >= len(s.src) {
			return
		}
		if s.src[s.off] == '\n' {
			s.line++
			s.col = 1
		} else {
			s.col++
		}
		s.off++
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }

// scan returns the next token. It returns a tokEOF token at the end of the
// query and an error (wrapping plan.ErrBadQuery, see parser.go) for malformed
// input; scanning stops at the first error.
func (s *scanner) scan() (token, error) {
	s.skipSpace()
	start := s.pos()
	if s.off >= len(s.src) {
		return token{Kind: tokEOF, Pos: start}, nil
	}
	c := s.src[s.off]
	switch {
	case c == '"' || c == '`':
		return s.scanString(start)
	case isDigit(c) || c == '.' && s.off+1 < len(s.src) && isDigit(s.src[s.off+1]):
		return s.scanNumber(start)
	case isIdentStart(c):
		return s.scanIdent(start), nil
	}
	return s.scanOperator(start)
}

func (s *scanner) skipSpace() {
	for s.off < len(s.src) {
		switch s.src[s.off] {
		case ' ', '\t', '\n', '\r':
			s.advance(1)
		case '#':
			// Comment to end of line, as Loki's Lex skips them; the newline
			// itself is consumed as whitespace on the next pass. Comments
			// live between tokens only: strings are scanned as tokens, so a
			// # inside quotes is data.
			for s.off < len(s.src) && s.src[s.off] != '\n' {
				s.advance(1)
			}
		default:
			return
		}
	}
}

func (s *scanner) scanIdent(start Position) token {
	for s.off < len(s.src) && isIdentPart(s.src[s.off]) {
		s.advance(1)
	}
	return token{Kind: tokIdent, Pos: start, Text: s.src[start.Offset:s.off]}
}

// scanString scans a double-quoted string, with Go escapes as in PromQL, or a
// backtick raw string, which may span lines. The decoded value lands in Str.
// Like Loki's lexer, it rejects invalid UTF-8 inside strings.
func (s *scanner) scanString(start Position) (token, error) {
	quote := s.src[s.off]
	s.advance(1)
	if quote == '`' {
		end := strings.IndexByte(s.src[s.off:], '`')
		if end < 0 {
			return token{}, badQuery(start, "unterminated raw string")
		}
		raw := s.src[start.Offset : s.off+end+1]
		s.advance(end + 1)
		return unquoteString(start, raw)
	}
	for s.off < len(s.src) {
		switch s.src[s.off] {
		case '"':
			raw := s.src[start.Offset : s.off+1]
			s.advance(1)
			return unquoteString(start, raw)
		case '\n':
			return token{}, badQuery(start, "unterminated string")
		case '\\':
			s.advance(2) // the escaped character is consumed on the next pass
		default:
			s.advance(1)
		}
	}
	return token{}, badQuery(start, "unterminated string")
}

func unquoteString(start Position, raw string) (token, error) {
	if !utf8.ValidString(raw) {
		return token{}, badQuery(start, "invalid UTF-8 rune in string")
	}
	str, err := strconv.Unquote(raw)
	if err != nil {
		return token{}, badQuery(start, "invalid string escape: %v", err)
	}
	return token{Kind: tokString, Pos: start, Text: raw, Str: str}, nil
}

// scanNumber scans a number, a duration or a byte size. See the package
// comment in this file for the suffix order.
func (s *scanner) scanNumber(start Position) (token, error) {
	numStart := s.off
	s.scanDigits()
	if s.off < len(s.src) && s.src[s.off] == '.' {
		// A trailing decimal point is valid after digits (1., 1.e2, the
		// duration 1.s), matching Go's scanner; a leading one needs a digit
		// after it (.5), which the dispatch already checked.
		if s.off > numStart || s.off+1 < len(s.src) && isDigit(s.src[s.off+1]) {
			s.advance(1)
			s.scanDigits()
		}
	}
	// Exponent: only when e/E starts a valid exponent; 1eb is left to the
	// bytes suffix (which rejects it), not read as an exponent.
	if s.off < len(s.src) && (s.src[s.off] == 'e' || s.src[s.off] == 'E') {
		j := s.off + 1
		if j < len(s.src) && (s.src[j] == '+' || s.src[j] == '-') {
			j++
		}
		if j < len(s.src) && isDigit(s.src[j]) {
			s.advance(j - s.off)
			s.scanDigits()
		}
	}
	text := s.src[numStart:s.off]
	num, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return token{}, badQuery(start, "invalid number %q", text)
	}
	rest := s.src[s.off:]
	if n := durationSuffixLen(rest); n > 0 {
		if d, ok := parseLogQLDuration(text + rest[:n]); ok {
			s.advance(n)
			return token{Kind: tokDuration, Pos: start, Text: s.src[start.Offset:s.off], Dur: d}, nil
		}
	}
	if n := bytesSuffixLen(rest); n > 0 {
		if b, ok := parseBytesValue(text + rest[:n]); ok {
			s.advance(n)
			return token{Kind: tokBytes, Pos: start, Text: s.src[start.Offset:s.off], Bytes: b}, nil
		}
	}
	return token{Kind: tokNumber, Pos: start, Text: text, Num: num}, nil
}

func (s *scanner) scanDigits() {
	for s.off < len(s.src) && isDigit(s.src[s.off]) {
		s.advance(1)
	}
}

// durationSuffixLen returns the length of the run of duration-token bytes at
// the start of src: digits, dots, the lowercase unit letters n u s m h d w y
// and U+00B5 (µ), mirroring Loki's isDurationRune scan. Lowercase m is a
// duration unit here, so 1mb never reaches the bytes scan.
func durationSuffixLen(src string) int {
	n := 0
	for n < len(src) {
		c := src[n]
		switch {
		case isDigit(c) || c == '.':
			n++
		case c == 'n' || c == 'u' || c == 's' || c == 'm' || c == 'h' || c == 'd' || c == 'w' || c == 'y':
			n++
		case c == 0xc2 && n+1 < len(src) && src[n+1] == 0xb5: // µ
			n += 2
		default:
			return n
		}
	}
	return n
}

// bytesSuffixLen returns the length of the run of byte-size-token bytes at
// the start of src: digits, dots and exactly the unit runes of Loki's
// isBytesSizeRune gate, B i k K M G T P (lowercase b/m/g/t/p and uppercase I
// are not in the gate and end the candidate). The gate is lexical;
// humanize.ParseBytes is case-insensitive only after it.
func bytesSuffixLen(src string) int {
	n := 0
	for n < len(src) {
		c := src[n]
		switch {
		case isDigit(c) || c == '.':
			n++
		case strings.IndexByte("BikKMGTP", c) >= 0:
			n++
		default:
			return n
		}
	}
	return n
}

// parseLogQLDuration parses the union Loki's parseDuration accepts:
// model.ParseDuration first (integer components, units y w d h m s ms, in
// that order, each at most once), then time.ParseDuration (ns us µs ms s m h,
// decimals, repeats, any order). A failed parse is not an error: the caller
// falls back to a plain number token.
func parseLogQLDuration(s string) (time.Duration, bool) {
	if d, ok := parseModelDuration(s); ok {
		return d, true
	}
	d, err := time.ParseDuration(s)
	return d, err == nil
}

// parseModelDuration parses Prometheus model.ParseDuration's language with
// checked 64-bit arithmetic: integer components with units in decreasing
// size order y w d h m s ms, each unit at most once, at least one component.
func parseModelDuration(s string) (time.Duration, bool) {
	units := [...]struct {
		name string
		ns   int64
	}{
		{"y", int64(365 * 24 * time.Hour)},
		{"w", int64(7 * 24 * time.Hour)},
		{"d", int64(24 * time.Hour)},
		{"h", int64(time.Hour)},
		{"m", int64(time.Minute)},
		{"s", int64(time.Second)},
		{"ms", int64(time.Millisecond)},
	}
	if s == "" {
		return 0, false
	}
	var total int64
	i, nextUnit := 0, 0
	for i < len(s) {
		digits := i
		for i < len(s) && isDigit(s[i]) {
			i++
		}
		if i == digits {
			return 0, false
		}
		n, err := strconv.ParseInt(s[digits:i], 10, 64)
		if err != nil {
			return 0, false // more digits than int64: the fallback fails too
		}
		unit := -1
		for _, k := range [...]int{6, 0, 1, 2, 3, 4, 5} { // ms before m
			if k < nextUnit {
				continue
			}
			if strings.HasPrefix(s[i:], units[k].name) {
				unit = k
				i += len(units[k].name)
				break
			}
		}
		if unit < 0 {
			return 0, false
		}
		nextUnit = unit + 1
		if n > 0 {
			ns := units[unit].ns
			if n > math.MaxInt64/ns {
				return 0, false
			}
			term := n * ns
			if total > math.MaxInt64-term {
				return 0, false
			}
			total += term
		}
	}
	return time.Duration(total), true
}

// parseBytesValue parses a number plus byte-size unit with
// humanize.ParseBytes semantics, as gated by Loki's lexer: SI units
// (k kB M MB ...) are 1000-based, IEC units (ki kib Mi MiB ...) are
// 1024-based, B is 1, and e-forms never arrive (the rune gate rejects E).
// Whole numbers multiply exactly in uint64 with an overflow check, up to
// MaxUint64; fractions truncate, and anything at or above 2^64 fails.
func parseBytesValue(s string) (uint64, bool) {
	i := 0
	for i < len(s) && (isDigit(s[i]) || s[i] == '.') {
		i++
	}
	num, unit := s[:i], strings.ToLower(s[i:])
	var mult uint64
	switch unit {
	case "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "ki", "kib":
		mult = 1 << 10
	case "m", "mb":
		mult = 1000 * 1000
	case "mi", "mib":
		mult = 1 << 20
	case "g", "gb":
		mult = 1000 * 1000 * 1000
	case "gi", "gib":
		mult = 1 << 30
	case "t", "tb":
		mult = 1000 * 1000 * 1000 * 1000
	case "ti", "tib":
		mult = 1 << 40
	case "p", "pb":
		mult = 1000 * 1000 * 1000 * 1000 * 1000
	case "pi", "pib":
		mult = 1 << 50
	default:
		return 0, false
	}
	if !strings.Contains(num, ".") {
		if v, err := strconv.ParseUint(num, 10, 64); err == nil {
			hi, lo := bits.Mul64(v, mult)
			if hi == 0 {
				return lo, true
			}
			return 0, false // would overflow uint64
		}
		// Too many digits for uint64: fall through to the float path, as
		// humanize.ParseBytes does.
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, false
	}
	f *= float64(mult)
	if f < 0 || f >= math.MaxUint64 {
		return 0, false
	}
	return uint64(f), true // truncates, like humanize
}

// scanOperator scans punctuation and operators, longest match first.
func (s *scanner) scanOperator(start Position) (token, error) {
	mk := func(kind tokenKind, n int) (token, error) {
		s.advance(n)
		return token{Kind: kind, Pos: start, Text: s.src[start.Offset:s.off]}, nil
	}
	rest := s.src[s.off:]
	switch {
	case strings.HasPrefix(rest, "|="):
		return mk(tokPipeEq, 2)
	case strings.HasPrefix(rest, "|~"):
		return mk(tokPipeRe, 2)
	case strings.HasPrefix(rest, "|>"):
		return mk(tokPipePattern, 2)
	case strings.HasPrefix(rest, "!="):
		return mk(tokNeq, 2)
	case strings.HasPrefix(rest, "!>"):
		return mk(tokNotPattern, 2)
	case strings.HasPrefix(rest, "!~"):
		return mk(tokNre, 2)
	case strings.HasPrefix(rest, "=~"):
		return mk(tokRe, 2)
	case strings.HasPrefix(rest, "=="):
		return mk(tokEq, 2)
	case strings.HasPrefix(rest, ">="):
		return mk(tokGte, 2)
	case strings.HasPrefix(rest, "<="):
		return mk(tokLte, 2)
	}
	single := map[byte]tokenKind{
		'|': tokPipe, ',': tokComma,
		'{': tokLBrace, '}': tokRBrace,
		'[': tokLBracket, ']': tokRBracket,
		'(': tokLParen, ')': tokRParen,
		'=': tokAssign,
		'>': tokGt, '<': tokLt,
		'+': tokPlus, '-': tokMinus,
		'*': tokStar, '/': tokSlash,
		'%': tokPercent, '^': tokCaret,
	}
	if kind, ok := single[s.src[s.off]]; ok {
		return mk(kind, 1)
	}
	r, _ := utf8.DecodeRuneInString(s.src[s.off:])
	return token{}, badQuery(start, "invalid character %q", r)
}

// lexAll splits a query into tokens, ending with a tokEOF token.
func lexAll(src string) ([]token, error) {
	sc := newScanner(src)
	var toks []token
	for {
		t, err := sc.scan()
		if err != nil {
			return nil, err
		}
		toks = append(toks, t)
		if t.Kind == tokEOF {
			return toks, nil
		}
	}
}
