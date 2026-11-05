package plugin

import (
	"strings"
	"testing"
	"time"
)

func TestExpandMacros(t *testing.T) {
	from := time.Date(2026, 11, 5, 12, 34, 56, 123456789, time.UTC)
	to := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	values := MacroValues{From: from, To: to, Interval: 15 * time.Second}

	fromLiteral := "TIMESTAMP '2026-11-05 12:34:56.123457'"
	toLiteral := "TIMESTAMP '2026-11-05 13:00:00.000000'"
	intervalLiteral := "INTERVAL '15000 milliseconds'"

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "bare macros",
			sql:  "SELECT * FROM logs WHERE _ts >= $__timeFrom AND _ts < $__timeTo GROUP BY time_bucket($__interval, _ts)",
			want: "SELECT * FROM logs WHERE _ts >= " + fromLiteral + " AND _ts < " + toLiteral + " GROUP BY time_bucket(" + intervalLiteral + ", _ts)",
		},
		{
			name: "function forms",
			sql:  "SELECT * WHERE _ts >= $__timeFrom() AND _ts < $__timeTo() AND _ts > now() - $__interval()",
			want: "SELECT * WHERE _ts >= " + fromLiteral + " AND _ts < " + toLiteral + " AND _ts > now() - " + intervalLiteral,
		},
		{
			name: "single quoted string",
			sql:  "SELECT '$__timeFrom', $__timeTo",
			want: "SELECT '$__timeFrom', " + toLiteral,
		},
		{
			name: "doubled quote inside string",
			sql:  "SELECT 'it''s $__timeFrom' AS x, $__interval",
			want: "SELECT 'it''s $__timeFrom' AS x, " + intervalLiteral,
		},
		{
			name: "escape string",
			sql:  `SELECT E'it\'s $__timeFrom' AS x, $__timeTo`,
			want: `SELECT E'it\'s $__timeFrom' AS x, ` + toLiteral,
		},
		{
			name: "double quoted identifier",
			sql:  `SELECT "$__interval", $__timeFrom`,
			want: `SELECT "$__interval", ` + fromLiteral,
		},
		{
			name: "escaped double quoted identifier",
			sql:  `SELECT "a""$__timeTo" AS x, $__interval`,
			want: `SELECT "a""$__timeTo" AS x, ` + intervalLiteral,
		},
		{
			name: "line comment",
			sql:  "SELECT 1 -- $__timeFrom\n, $__timeTo",
			want: "SELECT 1 -- $__timeFrom\n, " + toLiteral,
		},
		{
			name: "line comment ends at CR",
			sql:  "SELECT 1 -- $__timeFrom\r, $__timeTo",
			want: "SELECT 1 -- $__timeFrom\r, " + toLiteral,
		},
		{
			name: "line comment ends at CRLF",
			sql:  "SELECT 1 -- $__timeFrom\r\n, $__timeTo",
			want: "SELECT 1 -- $__timeFrom\r\n, " + toLiteral,
		},
		{
			name: "nested block comment",
			sql:  "/* $__timeFrom /* $__interval */ $__timeTo */ SELECT $__timeFrom",
			want: "/* $__timeFrom /* $__interval */ $__timeTo */ SELECT " + fromLiteral,
		},
		{
			name: "dollar quoted strings",
			sql:  "SELECT $$ $__timeFrom $$, $tag$ $__timeTo $tag$, $__interval",
			want: "SELECT $$ $__timeFrom $$, $tag$ $__timeTo $tag$, " + intervalLiteral,
		},
		{
			name: "identifier boundary",
			sql:  "SELECT $__timeFromX, $__interval_ms, $__timeTo2, $__timeFrom",
			want: "SELECT $__timeFromX, $__interval_ms, $__timeTo2, " + fromLiteral,
		},
		{
			name: "unknown macro",
			sql:  "SELECT $__unknown, $__timeTo",
			want: "SELECT $__unknown, " + toLiteral,
		},
		{
			name: "dollar quote wins over macro name",
			sql:  "SELECT $__interval$not a macro $__timeTo$__interval$, $__timeTo",
			want: "SELECT $__interval$not a macro $__timeTo$__interval$, " + toLiteral,
		},
		{
			name: "empty dollar quote before macro",
			sql:  "SELECT $$__timeTo$$, $__timeTo",
			want: "SELECT $$__timeTo$$, " + toLiteral,
		},
		{
			name: "unicode dollar quote tag",
			sql:  "SELECT $tagé$ $__timeFrom $tagé$, $__timeFrom",
			want: "SELECT $tagé$ $__timeFrom $tagé$, " + fromLiteral,
		},
		{
			name: "non-ASCII only dollar quote tag",
			sql:  "SELECT $é$ $__timeFrom $é$, $__timeTo",
			want: "SELECT $é$ $__timeFrom $é$, " + toLiteral,
		},
		{
			name: "dollar quote tag with digit",
			sql:  "SELECT $d1$ $__timeFrom $d1$, $__timeTo",
			want: "SELECT $d1$ $__timeFrom $d1$, " + toLiteral,
		},
		{
			name: "digit-first dollar tag is not a delimiter",
			sql:  "SELECT $1$ $__timeTo $1$, $__timeTo",
			want: "SELECT $1$ " + toLiteral + " $1$, " + toLiteral,
		},
		{
			name: "identifier embedded tag then macro",
			sql:  "SELECT 1 AS foo$tag$, $__timeTo",
			want: "SELECT 1 AS foo$tag$, " + toLiteral,
		},
		{
			name: "emoji identifier before macro",
			sql:  "SELECT 😀$__interval",
			want: "SELECT 😀$__interval",
		},
		{
			name: "unicode space boundaries expand",
			sql:  "SELECT\u00A0$__timeFrom,\u200B$__timeTo,\uFEFF$__interval",
			want: "SELECT\u00A0" + fromLiteral + ",\u200B" + toLiteral + ",\uFEFF" + intervalLiteral,
		},
		{
			name: "ideographic space boundary",
			sql:  "SELECT\u3000$__timeTo",
			want: "SELECT\u3000" + toLiteral,
		},
		{
			name: "unicode space after macro name expands",
			sql:  "SELECT $__timeTo\u00A0AS t",
			want: "SELECT " + toLiteral + "\u00A0AS t",
		},
		{
			name: "unicode spaces inside quotes preserved",
			sql:  "SELECT '\u00A0$__timeTo', $$ \u200B$__timeFrom $$, $__timeTo",
			want: "SELECT '\u00A0$__timeTo', $$ \u200B$__timeFrom $$, " + toLiteral,
		},
		{
			name: "normalized space in dollar tag",
			sql:  "SELECT $a\u00A0$ $__timeTo",
			want: "SELECT $a\u00A0$ " + toLiteral,
		},
		{
			name: "no escape string after non-ASCII identifier",
			sql:  `SELECT éE'\'$__timeTo'`,
			want: `SELECT éE'\'` + toLiteral + `'`,
		},
		{
			name: "macro-like identifier suffix",
			sql:  "SELECT foo$__timeTo, 9$__timeFrom, _bar$__interval",
			want: "SELECT foo$__timeTo, 9$__timeFrom, _bar$__interval",
		},
		{
			name: "unicode identifier boundaries",
			sql:  "SELECT $__timeFromé, é$__timeTo, $__intervalΩ",
			want: "SELECT $__timeFromé, é$__timeTo, $__intervalΩ",
		},
		{
			name: "punctuation boundary still expands",
			sql:  "SELECT ($__timeFrom), $__timeTo;",
			want: "SELECT (" + fromLiteral + "), " + toLiteral + ";",
		},
		{
			name: "nothing to replace",
			sql:  "SELECT 1",
			want: "SELECT 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := expandMacros(tt.sql, values); got != tt.want {
				t.Fatalf("expandMacros() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTimeLiteralCeilsToMicrosecond(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "sub-microsecond rounds up",
			in:   time.Date(2026, 11, 5, 12, 34, 56, 123456789, time.UTC),
			want: "TIMESTAMP '2026-11-05 12:34:56.123457'",
		},
		{
			name: "exact microsecond unchanged",
			in:   time.Date(2026, 11, 5, 12, 34, 56, 123456000, time.UTC),
			want: "TIMESTAMP '2026-11-05 12:34:56.123456'",
		},
		{
			name: "one nanosecond rounds up",
			in:   time.Date(2026, 11, 5, 12, 34, 56, 1, time.UTC),
			want: "TIMESTAMP '2026-11-05 12:34:56.000001'",
		},
		{
			name: "pre-epoch rounds across the second",
			in:   time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
			want: "TIMESTAMP '1970-01-01 00:00:00.000000'",
		},
		{
			name: "pre-epoch sub-microsecond",
			in:   time.Date(1969, 12, 31, 23, 59, 59, 1, time.UTC),
			want: "TIMESTAMP '1969-12-31 23:59:59.000001'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timeLiteral(tt.in); got != tt.want {
				t.Fatalf("timeLiteral(%s) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestExpandMacrosIntervalMinimum(t *testing.T) {
	values := MacroValues{From: time.Unix(0, 0), To: time.Unix(1, 0)}
	for _, interval := range []time.Duration{0, time.Nanosecond, time.Millisecond - 1} {
		got := expandMacros("SELECT $__interval", MacroValues{
			From: values.From, To: values.To, Interval: interval,
		})
		if want := "SELECT INTERVAL '1 milliseconds'"; got != want {
			t.Fatalf("interval %s: got %q, want %q", interval, got, want)
		}
	}
}

func TestExpandMacrosPreservesTextAroundDollarQuotes(t *testing.T) {
	sql := "SELECT '$__timeFrom' AS a, $$ $__timeTo $$ AS b, $__timeFrom AS c -- $__interval"
	got := expandMacros(sql, MacroValues{
		From: time.Unix(0, 0), To: time.Unix(1, 0), Interval: time.Second,
	})
	if strings.Count(got, "TIMESTAMP '1970-01-01 00:00:00.000000'") != 1 {
		t.Fatalf("got %q, want exactly one expanded from macro", got)
	}
	if !strings.Contains(got, "'$__timeFrom'") || !strings.Contains(got, "$$ $__timeTo $$") {
		t.Fatalf("quoted macros were changed: %q", got)
	}
}
