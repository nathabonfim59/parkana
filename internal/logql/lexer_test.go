package logql

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// describe renders a token compactly for table comparisons.
func describe(t token) string {
	switch t.Kind {
	case tokIdent:
		return "ident(" + t.Text + ")"
	case tokNumber:
		return fmt.Sprintf("num(%v)", t.Num)
	case tokDuration:
		return "dur(" + t.Dur.String() + ")"
	case tokBytes:
		return fmt.Sprintf("bytes(%d)", t.Bytes)
	case tokString:
		return fmt.Sprintf("str(%q)", t.Str)
	case tokEOF:
		return "eof"
	}
	return "'" + t.Text + "'"
}

// lexToks lexes src, requiring success, and renders all tokens.
func lexToks(t *testing.T, src string) string {
	t.Helper()
	toks, err := lexAll(src)
	if err != nil {
		t.Fatalf("lexAll(%q): %v", src, err)
	}
	out := ""
	for i, tok := range toks {
		if i > 0 {
			out += " "
		}
		out += describe(tok)
	}
	return out
}

func TestLexerTokens(t *testing.T) {
	tests := []struct{ src, want string }{
		// A full query exercises most token kinds in context.
		{`{app="api"} |= "err" or "fail"`,
			`'{' ident(app) '=' str("api") '}' '|=' str("err") ident(or) str("fail") eof`},
		{`count_over_time({app="api"}[5m]) by (env)`,
			`ident(count_over_time) '(' '{' ident(app) '=' str("api") '}' '[' dur(5m0s) ']' ')' ident(by) '(' ident(env) ')' eof`},
		// Strings: quoted with Go escapes, raw in backticks.
		{`""`, `str("") eof`},
		{"\"tab\\there\"", `str("tab\there") eof`},
		{"\"back\\\\slash\"", `str("back\\slash") eof`},
		{"\"uni héllo ✓\"", `str("uni héllo ✓") eof`},
		{"`raw string`", `str("raw string") eof`},
		{"`has \"quotes\" and\nnewline`", `str("has \"quotes\" and\nnewline") eof`},
		// Numbers.
		{`0`, `num(0) eof`},
		{`42`, `num(42) eof`},
		{`1.5`, `num(1.5) eof`},
		{`.5`, `num(0.5) eof`},
		{`1e3`, `num(1000) eof`},
		{`1E-2`, `num(0.01) eof`},
		{`2.5e2`, `num(250) eof`},
		{`1e`, `num(1) ident(e) eof`}, // no exponent, no unit
		{`5x`, `num(5) ident(x) eof`},
		// Trailing decimal point, as Go's scanner accepts Floats: 1. and
		// exponents after it; a lone dot is still invalid.
		{`1.`, `num(1) eof`},
		{`1.e2`, `num(100) eof`},
		{`1.e-2`, `num(0.01) eof`},
		{`1.s`, `dur(1s) eof`}, // 1. + s, a duration via the Go fallback
		{`1.m`, `dur(1m0s) eof`},
		// Comments run from # to the end of line, outside strings.
		{"# whole line comment\n{a=\"1\"}", `'{' ident(a) '=' str("1") '}' eof`},
		{"1 # trailing comment", `num(1) eof`}, // comment may end at EOF
		{"1 # c\n+ 2", `num(1) '+' num(2) eof`},
		// Durations: model-style composites (y w d h m s ms, in order,
		// integers) plus time.ParseDuration forms (ns us µs, decimals, any
		// order) as Loki's parseDuration fallback does.
		{`500ms`, `dur(500ms) eof`},
		{`30s`, `dur(30s) eof`},
		{`5m`, `dur(5m0s) eof`},
		{`1h30m`, `dur(1h30m0s) eof`},
		{`1.5h`, `dur(1h30m0s) eof`},
		{`1d`, `dur(24h0m0s) eof`},
		{`7d`, `dur(168h0m0s) eof`},
		{`1w`, `dur(168h0m0s) eof`},
		{`1y`, `dur(8760h0m0s) eof`},
		{`1m500ms`, `dur(1m0.5s) eof`},
		{`1ns`, `dur(1ns) eof`},
		{`1us`, `dur(1µs) eof`},
		{"1µs", `dur(1µs) eof`},
		{`1m1h`, `dur(1h1m0s) eof`},                   // only valid via the fallback
		{`90s1m`, `dur(2m30s) eof`},                   // order-free fallback form
		{`1h1h`, `dur(2h0m0s) eof`},                   // repeated units via the fallback
		{`1h2`, `num(1) ident(h2) eof`},               // trailing number without a unit breaks the candidate, as in Loki
		{`1000000000000y`, `num(1e+12) ident(y) eof`}, // overflow: no duration token
		// Byte sizes: humanize.ParseBytes semantics behind Loki's exact
		// isBytesSizeRune gate (B i k K M G T P only): SI kB=1000, IEC
		// KiB=1024. Lowercase b/m/g/t/p and uppercase I are not in the gate,
		// so they end a candidate early.
		{`1KB`, `bytes(1000) eof`},
		{`1kb`, `bytes(1000) ident(b) eof`}, // b is not in the rune gate
		{`1KiB`, `bytes(1024) eof`},
		{`1.5MB`, `bytes(1500000) eof`},
		{`10GB`, `bytes(10000000000) eof`},
		{`1B`, `bytes(1) eof`},
		{`1b`, `num(1) ident(b) eof`},         // lowercase b is not in the rune gate
		{`1KIB`, `bytes(1000) ident(IB) eof`}, // uppercase I is not in the rune gate
		{`2TiB`, `bytes(2199023255552) eof`},
		{`1k`, `bytes(1000) eof`},
		{`1M`, `bytes(1000000) eof`},                                            // uppercase M: not a duration rune
		{`1g`, `num(1) ident(g) eof`},                                           // lowercase g is not in the rune gate
		{`1mi`, `dur(1m0s) ident(i) eof`},                                       // duration wins: lowercase m scans as minutes first
		{`1eb`, `num(1) ident(eb) eof`},                                         // e is not in the rune gate
		{`9223372036854775808B`, `bytes(9223372036854775808) eof`},              // MaxInt64+1 fits uint64
		{`18446744073709551615B`, `bytes(18446744073709551615) eof`},            // MaxUint64
		{`18446744073709551616KB`, `num(1.8446744073709552e+19) ident(KB) eof`}, // overflow: plain number
		// Operators.
		{`!= !~ =~ == = > >= < <= + - * / % ^ | |= |~ , ( ) { } [ ]`,
			`'!=' '!~' '=~' '==' '=' '>' '>=' '<' '<=' '+' '-' '*' '/' '%' '^' '|' '|=' '|~' ',' '(' ')' '{' '}' '[' ']' eof`},
	}
	for _, tt := range tests {
		if got := lexToks(t, tt.src); got != tt.want {
			t.Errorf("lexAll(%q) =\n %s\nwant\n %s", tt.src, got, tt.want)
		}
	}
}

func TestLexerPositions(t *testing.T) {
	toks, err := lexAll("{app=\"a\"}\n  |= \"b\"")
	if err != nil {
		t.Fatal(err)
	}
	wantPos := map[int]Position{
		0: {Offset: 0, Line: 1, Col: 1},  // {
		1: {Offset: 1, Line: 1, Col: 2},  // app
		4: {Offset: 8, Line: 1, Col: 9},  // }
		5: {Offset: 12, Line: 2, Col: 3}, // |=
	}
	for i, p := range wantPos {
		if toks[i].Pos != p {
			t.Errorf("token %d (%s) pos = %v, want %v", i, describe(toks[i]), toks[i].Pos, p)
		}
	}
	last := toks[len(toks)-1]
	if last.Kind != tokEOF || last.Pos.Line != 2 {
		t.Errorf("eof token = %s at %v, want line 2", describe(last), last.Pos)
	}
}

func TestLexerErrors(t *testing.T) {
	tests := []string{
		`@`,
		`!`,                // bare ! is not an operator
		`{a="unterminated`, // quoted string runs to the end
		"{a=\"newline\ninside\"}",
		"{a=`unterminated raw}",
		`"bad escape \q"`,
		"{a=\"\xff\"}", // invalid UTF-8 in a quoted string is rejected by Unquote
	}
	for _, src := range tests {
		_, err := lexAll(src)
		if err == nil {
			t.Errorf("lexAll(%q) succeeded, want error", src)
			continue
		}
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("lexAll(%q) err = %v, want ErrBadQuery", src, err)
		}
	}
}

func TestParseLogQLDuration(t *testing.T) {
	// The union Loki's parseDuration accepts: model.ParseDuration first,
	// then time.ParseDuration.
	valid := []struct {
		s    string
		want string // time.Duration string
	}{
		{"1y", "8760h0m0s"},
		{"1w2d", "216h0m0s"},
		{"1d12h", "36h0m0s"},
		{"2h45m30s500ms", "2h45m30.5s"},
		{"0s", "0s"},
		{"1.5h", "1h30m0s"},  // fallback: decimal
		{"100ns", "100ns"},   // fallback: ns unit
		{"5us", "5µs"},       // fallback: us unit
		{"1h30m", "1h30m0s"}, // both paths
		{"30m1h", "1h30m0s"}, // fallback: any order
	}
	for _, tt := range valid {
		d, ok := parseLogQLDuration(tt.s)
		if !ok || d.String() != tt.want {
			t.Errorf("parseLogQLDuration(%q) = %v, %v; want %s", tt.s, d, ok, tt.want)
		}
	}
	// The Go fallback accepts repeats and any order, so those are valid too.
	moreValid := []struct {
		s    string
		want string
	}{
		{"1s1s", "2s"},
		{"1ms1m", "1m0.001s"},
	}
	for _, tt := range moreValid {
		if d, ok := parseLogQLDuration(tt.s); !ok || d.String() != tt.want {
			t.Errorf("parseLogQLDuration(%q) = %v, %v; want %s", tt.s, d, ok, tt.want)
		}
	}
	invalid := []string{"", "1", "1x", "1.5d", "1d1w", "1e3s"}
	for _, s := range invalid {
		if _, ok := parseLogQLDuration(s); ok {
			t.Errorf("parseLogQLDuration(%q) succeeded, want failure", s)
		}
	}
}

func TestParseBytesValue(t *testing.T) {
	tests := []struct {
		s    string
		want uint64
		ok   bool
	}{
		{"1b", 1, true},
		{"1kB", 1000, true},
		{"1KB", 1000, true},
		{"1KiB", 1024, true},
		{"1ki", 1024, true},
		{"1.5MB", 1500000, true},
		{"2GiB", 2 << 30, true},
		{"1.5KiB", 1536, true},                          // fraction truncates exactly here
		{"1.9b", 1, true},                               // fraction truncates
		{"9223372036854775807b", (1 << 63) - 1, true},   // MaxInt64 exactly
		{"9223372036854775808b", 1 << 63, true},         // MaxInt64+1 fits uint64
		{"18446744073709551615b", math.MaxUint64, true}, // MaxUint64
		{"18446744073709551616b", 0, false},             // 2^64: float path rejects
		{"9223372036854775808KB", 0, false},             // 2^63 * 1000 overflows uint64
		{"1e3", 0, false},                               // no unit
		{"1eb", 0, false},                               // e-forms rejected like Loki's lexer
		{"1zb", 0, false},                               // unknown unit
		{"1.5.2", 0, false},                             // invalid number
	}
	for _, tt := range tests {
		got, ok := parseBytesValue(tt.s)
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("parseBytesValue(%q) = %d, %v; want %d, %v", tt.s, got, ok, tt.want, tt.ok)
		}
	}
}
