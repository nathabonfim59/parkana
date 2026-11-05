package plugin

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// MacroValues are the values substituted for supported Grafana SQL macros.
// Times are rendered as UTC TIMESTAMP literals; Interval is rendered as a
// DuckDB INTERVAL literal in whole milliseconds.
type MacroValues struct {
	From     time.Time
	To       time.Time
	Interval time.Duration
}

// Macro names. The parenthesized Grafana function form is accepted too.
const (
	macroTimeFrom = "$__timeFrom"
	macroTimeTo   = "$__timeTo"
	macroInterval = "$__interval"
)

// expandMacros replaces $__timeFrom, $__timeTo and $__interval (and their
// function forms, such as $__timeFrom()) with DuckDB literals. It only
// substitutes outside SQL string literals, quoted identifiers, dollar-quoted
// strings and comments, so macro-looking text stays data when it is data.
// Unknown $__ names are left untouched.
func expandMacros(sql string, values MacroValues) string {
	var b strings.Builder
	b.Grow(len(sql))

	for i := 0; i < len(sql); {
		switch {
		case strings.HasPrefix(sql[i:], "--"):
			i = copyLineComment(&b, sql, i)
		case strings.HasPrefix(sql[i:], "/*"):
			i = copyBlockComment(&b, sql, i)
		case sql[i] == '\'':
			i = copySingleQuoted(&b, sql, i)
		case sql[i] == '"':
			i = copyDoubleQuoted(&b, sql, i)
		case sql[i] == '$':
			// A '$' that continues an identifier can open nothing:
			// DuckDB's ident_cont includes '$', so foo$tag$ is one
			// identifier, not a dollar-quoted string, and foo$__timeTo is
			// not a macro.
			if !identifierBoundaryBefore(sql, i) {
				b.WriteByte(sql[i])
				i++
				continue
			}
			// A valid dollar-quote delimiter wins over macro names: for
			// example $__interval$ is a quoted string, not a macro.
			if _, ok := dollarDelimiter(sql[i:]); ok {
				i = copyDollarQuoted(&b, sql, i)
				continue
			}
			if consumed, replacement, ok := macroAt(sql[i:], values); ok {
				b.WriteString(replacement)
				i += consumed
				continue
			}
			b.WriteByte(sql[i])
			i++
		default:
			b.WriteByte(sql[i])
			i++
		}
	}
	return b.String()
}

// macroAt matches one supported macro at the start of s. It reports the
// consumed byte count and its replacement. A macro name must be followed by
// the end of input, by "()", or by a byte that cannot continue an identifier,
// so $__timeFromX is not treated as $__timeFrom followed by X.
func macroAt(s string, values MacroValues) (int, string, bool) {
	switch {
	case macroPrefix(s, "$__timeFrom"):
		return macroMatch(s, len(macroTimeFrom), timeLiteral(values.From))
	case macroPrefix(s, "$__timeTo"):
		return macroMatch(s, len(macroTimeTo), timeLiteral(values.To))
	case macroPrefix(s, "$__interval"):
		return macroMatch(s, len(macroInterval), intervalLiteral(values.Interval))
	}
	return 0, "", false
}

// macroPrefix reports whether s begins with name.
func macroPrefix(s, name string) bool {
	return strings.HasPrefix(s, name)
}

// macroMatch applies the identifier-boundary rule to a macro already known to
// start with name of length n.
func macroMatch(s string, n int, replacement string) (int, string, bool) {
	rest := s[n:]
	if strings.HasPrefix(rest, "()") {
		return n + 2, replacement, true
	}
	if rest != "" {
		r, _ := utf8.DecodeRuneInString(rest)
		if isIdentContRune(r) {
			return 0, "", false
		}
	}
	return n, replacement, true
}

// identifierBoundaryBefore reports whether the byte at offset i starts a
// standalone token: it must not be preceded by a character that can continue
// a DuckDB identifier.
func identifierBoundaryBefore(s string, i int) bool {
	if i == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:i])
	return !isIdentContRune(r)
}

// isIdentContRune reports whether r can continue a DuckDB identifier:
// ASCII [A-Za-z0-9_$] or any non-ASCII rune that DuckDB does not normalize
// to a space before tokenizing (so an emoji is an identifier character even
// though it is not a Unicode letter).
func isIdentContRune(r rune) bool {
	if r < 0x80 {
		c := byte(r)
		return c == '$' || isDolqContByte(c)
	}
	return !isNormalizedSpaceRune(r)
}

// isNormalizedSpaceRune reports whether r is one of the Unicode spaces that
// DuckDB's Parser::StripUnicodeSpaces (src/parser/parser.cpp) replaces with
// an ASCII space before tokenizing, outside quoted text and comments. Such
// runes are token boundaries, not identifier characters.
func isNormalizedSpaceRune(r rune) bool {
	return r == '\u00A0' || r == '\u202F' || r == '\u205F' || r == '\u2060' ||
		r == '\u3000' || r == '\uFEFF' || (r >= '\u2000' && r <= '\u200B')
}

// isDolqStartRune reports whether r can start a dollar-quote tag: an ASCII
// letter, underscore, or any non-ASCII rune that is not a normalized space.
func isDolqStartRune(r rune) bool {
	if r < 0x80 {
		return isDolqStartByte(byte(r))
	}
	return !isNormalizedSpaceRune(r)
}

// isDolqContRune reports whether r can continue a dollar-quote tag: the
// start runes plus ASCII digits.
func isDolqContRune(r rune) bool {
	if r < 0x80 {
		return isDolqContByte(byte(r))
	}
	return !isNormalizedSpaceRune(r)
}

// isDolqContByte reports whether c can continue a dollar-quote tag: the
// start bytes plus digits, but not '$', which closes the delimiter.
func isDolqContByte(c byte) bool {
	return isDolqStartByte(c) || (c >= '0' && c <= '9')
}

// isDolqStartByte reports whether c can start a dollar-quote tag or an
// identifier: an ASCII letter or underscore.
func isDolqStartByte(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z')
}

// timeLiteral renders t as a UTC DuckDB TIMESTAMP literal. Storage keeps
// whole microseconds, so a sub-microsecond bound is rounded up exactly like
// the executor's ceilMicro: truncating would move the bound below a stored
// row and drop it.
func timeLiteral(t time.Time) string {
	t = ceilMicroTime(t)
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02 15:04:05.000000") + "'"
}

// ceilMicroTime rounds t up to the next whole microsecond. time.Time's
// Nanosecond is always in [0, 1s), so this also crosses seconds correctly for
// pre-epoch instants.
func ceilMicroTime(t time.Time) time.Time {
	if remainder := t.Nanosecond() % 1000; remainder != 0 {
		return t.Add(time.Duration(1000-remainder) * time.Nanosecond)
	}
	return t
}

// intervalLiteral renders d as a DuckDB INTERVAL literal. The API works in
// whole milliseconds; a sub-millisecond interval rounds up to 1ms so the
// literal never means "zero".
func intervalLiteral(d time.Duration) string {
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return "INTERVAL '" + strconv.FormatInt(d.Milliseconds(), 10) + " milliseconds'"
}

// copyLineComment copies "--" through the end of the line. DuckDB's newline
// is [\n\r], so a carriage return ends the comment just like a line feed.
func copyLineComment(b *strings.Builder, s string, i int) int {
	j := i + 2
	for j < len(s) && s[j] != '\n' && s[j] != '\r' {
		j++
	}
	b.WriteString(s[i:j])
	return j
}

// copyBlockComment copies a block comment, honoring nesting.
func copyBlockComment(b *strings.Builder, s string, i int) int {
	depth := 0
	j := i
	for j < len(s) {
		switch {
		case strings.HasPrefix(s[j:], "/*"):
			depth++
			j += 2
		case strings.HasPrefix(s[j:], "*/"):
			depth--
			j += 2
			if depth == 0 {
				b.WriteString(s[i:j])
				return j
			}
		default:
			j++
		}
	}
	b.WriteString(s[i:])
	return len(s)
}

// copySingleQuoted copies a single-quoted SQL string, where a doubled quote
// escapes a quote. Escape-string syntax (E'...') additionally treats a
// backslash as escaping the following byte.
func copySingleQuoted(b *strings.Builder, s string, i int) int {
	escape := i > 0 && (s[i-1] == 'E' || s[i-1] == 'e') &&
		identifierBoundaryBefore(s, i-1)
	j := i + 1
	for j < len(s) {
		if escape && s[j] == '\\' && j+1 < len(s) {
			j += 2
			continue
		}
		if s[j] == '\'' {
			if j+1 < len(s) && s[j+1] == '\'' {
				j += 2
				continue
			}
			j++
			break
		}
		j++
	}
	b.WriteString(s[i:j])
	return j
}

// copyDoubleQuoted copies a double-quoted identifier, where "" escapes a
// quote.
func copyDoubleQuoted(b *strings.Builder, s string, i int) int {
	j := i + 1
	for j < len(s) {
		if s[j] == '"' {
			if j+1 < len(s) && s[j+1] == '"' {
				j += 2
				continue
			}
			j++
			break
		}
		j++
	}
	b.WriteString(s[i:j])
	return j
}

// copyDollarQuoted copies a dollar-quoted string ($tag$...$tag$ or $$...$$).
// A lone '$' that does not start a delimited string is copied as one byte.
func copyDollarQuoted(b *strings.Builder, s string, i int) int {
	delim, ok := dollarDelimiter(s[i:])
	if !ok {
		b.WriteByte(s[i])
		return i + 1
	}
	start := i + len(delim)
	if end := strings.Index(s[start:], delim); end >= 0 {
		end += start + len(delim)
		b.WriteString(s[i:end])
		return end
	}
	b.WriteString(s[i:])
	return len(s)
}

// dollarDelimiter returns the dollar-quote delimiter at the start of s. A
// tag begins with an ASCII letter, underscore or a non-ASCII rune that is
// not a normalized space (never a digit); digits are allowed after the
// first character. The empty tag ("$$") is valid.
func dollarDelimiter(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}
	first := true
	for i := 1; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == '$' {
			return s[:i+size], true
		}
		ok := isDolqContRune(r)
		if first {
			ok = isDolqStartRune(r)
			first = false
		}
		if !ok {
			return "", false
		}
		i += size
	}
	return "", false
}
