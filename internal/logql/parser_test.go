package logql

import (
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// parseAndCheck parses q and asserts the canonical String form plus the
// parse → String → parse round trip: parsing the canonical form again must
// succeed and render identically (textual stability), which every supported
// family guarantees.
func parseAndCheck(t *testing.T, q, want string) Expr {
	t.Helper()
	e, err := Parse(q)
	if err != nil {
		t.Errorf("Parse(%q): %v", q, err)
		return nil
	}
	if got := e.String(); got != want {
		t.Errorf("Parse(%q).String() = %q, want %q", q, got, want)
		return e
	}
	e2, err := Parse(e.String())
	if err != nil {
		t.Fatalf("reparse of %q -> %q: %v", q, e.String(), err)
	}
	if again := e2.String(); again != e.String() {
		t.Errorf("round trip unstable: %q -> %q -> %q", q, e.String(), again)
	}
	return e
}

// TestParseValidLogQueries covers every supported pipeline family with one
// canonical round trip each.
func TestParseValidLogQueries(t *testing.T) {
	tests := []struct{ q, want string }{
		// Bare log queries and selectors as function-name labels.
		{`{app="api"}`, `{app="api"}`},
		{`{sum="x", rate="y", ip="z"}`, `{sum="x", rate="y", ip="z"}`},
		// Line filters: all four operators, or-alternatives (surface
		// syntax), juxtaposed runs, and a line filter after a piped stage.
		{`{app="api"} |= "err"`, `{app="api"} |= "err"`},
		{`{app="api"} |= "err" or "fail" or "panic"`, `{app="api"} |= "err" or "fail" or "panic"`},
		{`{app="api"} != "err" or "fail"`, `{app="api"} != "err" or "fail"`},
		{`{app="api"} |~ "e.*" or "f.*"`, `{app="api"} |~ "e.*" or "f.*"`},
		{`{app="api"} !~ "debug"`, `{app="api"} !~ "debug"`},
		{`{app="api"} |= "a" != "b" |~ "c" !~ "d"`, `{app="api"} |= "a" != "b" |~ "c" !~ "d"`},
		{`{app="api"} |= "a" or "b" != "c"`, `{app="api"} |= "a" or "b" != "c"`},
		{`{app="api"} | json |= "x"`, `{app="api"} | json |= "x"`},
		// Parsers: json (no flags), logfmt (exact flags), named and bare
		// extractions, regexp (named captures, unnamed allowed), pattern.
		{`{app="api"} | json`, `{app="api"} | json`},
		{`{app="api"} | json status="response.status"`, `{app="api"} | json status="response.status"`},
		{`{app="api"} | json foo, bar="baz.buz"`, `{app="api"} | json foo="foo", bar="baz.buz"`},
		{`{app="api"} | json a="b", c="d"`, `{app="api"} | json a="b", c="d"`},
		{`{app="api"} | logfmt`, `{app="api"} | logfmt`},
		{`{app="api"} | logfmt --strict`, `{app="api"} | logfmt --strict`},
		{`{app="api"} | logfmt --strict --keep-empty`, `{app="api"} | logfmt --strict --keep-empty`},
		// Repeated flags are allowed, like Loki's parserFlags+ rule.
		{`{app="api"} | logfmt --strict --strict`, `{app="api"} | logfmt --strict`},
		{`{app="api"} | logfmt --keep-empty a="b", c="d"`, `{app="api"} | logfmt --keep-empty a="b", c="d"`},
		{`{app="api"} | regexp "(?P<method>\\w+)>"`, `{app="api"} | regexp "(?P<method>\\w+)>"`},
		{`{app="api"} | regexp "(?P<x>a)(b)"`, `{app="api"} | regexp "(?P<x>a)(b)"`},
		{`{app="api"} | pattern "<method> <uri>"`, `{app="api"} | pattern "<method> <uri>"`},
		// Formatting and label mutation, including the empty template.
		{`{app="api"} | line_format "{{.level}}: {{__line__}}"`, `{app="api"} | line_format "{{.level}}: {{__line__}}"`},
		{`{app="api"} | label_format level=lvl`, `{app="api"} | label_format level=lvl`},
		{`{app="api"} | label_format a=b, c="{{.d}}"`, `{app="api"} | label_format a=b, c="{{.d}}"`},
		{`{app="api"} | label_format gone=""`, `{app="api"} | label_format gone=""`},
		// Drop and keep: label names and string matchers, targets select
		// labels, never rows.
		// Dot-newline regexps are canonicalized for direct label matchers:
		// x.* renders with the scope dotNLMatcherValue emits.
		{`{app="api"} | drop foo, bar=~"x.*", __error__`, `{app="api"} | drop foo, bar=~"(?s:x.*)", __error__`},
		{`{app="api"} | keep foo, bar!~"x.*"`, `{app="api"} | keep foo, bar!~"(?s:x.*)"`},
		{`{app="api"} | drop __error__`, `{app="api"} | drop __error__`},
		// Label filters: every operator and value kind, == canonicalized
		// to =, signs, boolean combinations with and/comma/juxtaposition
		// and parentheses.
		{`{app="api"} | env="prod"`, `{app="api"} | env = "prod"`},
		{`{app="api"} | env!="prod"`, `{app="api"} | env != "prod"`},
		{`{app="api"} | env=~"prod|stg"`, `{app="api"} | env =~ "prod|stg"`},
		{`{app="api"} | env!~"prod"`, `{app="api"} | env !~ "prod"`},
		{`{app="api"} | code >= 400`, `{app="api"} | code >= 400`},
		{`{app="api"} | code = 400`, `{app="api"} | code = 400`},
		{`{app="api"} | code == 400`, `{app="api"} | code = 400`},
		{`{app="api"} | code != 400`, `{app="api"} | code != 400`},
		{`{app="api"} | code < 400`, `{app="api"} | code < 400`},
		{`{app="api"} | code <= 400`, `{app="api"} | code <= 400`},
		{`{app="api"} | code > -1`, `{app="api"} | code > -1`},
		{`{app="api"} | code > +1`, `{app="api"} | code > 1`},
		{`{app="api"} | latency > 500ms`, `{app="api"} | latency > 500ms`},
		{`{app="api"} | latency > 1.5h`, `{app="api"} | latency > 1h30m`},
		{`{app="api"} | latency > 100ns`, `{app="api"} | latency > 100ns`},
		{`{app="api"} | size >= 1KiB`, `{app="api"} | size >= 1KiB`},
		{`{app="api"} | size >= 2048KiB`, `{app="api"} | size >= 2MiB`},
		{`{app="api"} | count < 0.99`, `{app="api"} | count < 0.99`},
		// Negative glued durations (Loki's lexer binds '-' to the unit);
		// numbers still take any spacing, bytes never take a sign.
		{`{app="api"} | latency > -1s`, `{app="api"} | latency > -1s`},
		{`{app="api"} | latency == -1.5s`, `{app="api"} | latency = -1.5s`},
		{`{app="api"} | code > -1`, `{app="api"} | code > -1`},
		{`{app="api"} | code > - 1`, `{app="api"} | code > -1`},
		{`{app="api"} | a="1" and b="2"`, `{app="api"} | a = "1" and b = "2"`},
		{`{app="api"} | a="1", b="2"`, `{app="api"} | a = "1", b = "2"`},
		{`{app="api"} | a="1" b="2"`, `{app="api"} | a = "1" and b = "2"`},
		{`{app="api"} | a="1" or b="2" and c="3"`, `{app="api"} | a = "1" or b = "2" and c = "3"`},
		{`{app="api"} | (a="1" or b="2") and c="3"`, `{app="api"} | (a = "1" or b = "2") and c = "3"`},
		{`{app="api"} | (a="1") (b="2")`, `{app="api"} | a = "1" and b = "2"`},
		// A line-filter run after a label-filter stage (no pipe).
		{`{app="api"} | foo="bar" != "baz"`, `{app="api"} | foo = "bar" != "baz"`},
		// A whole pipeline mixing families, Grafana logs-volume shape.
		{`{app="api"} |= "err" != "warn" | json | line_format "{{.x}}" | drop __error__ | code >= 400`,
			`{app="api"} |= "err" != "warn" | json | line_format "{{.x}}" | drop __error__ | code >= 400`},
		// Comments between tokens.
		{"{app=\"api\"} # trailing\n|= \"err\" # after", `{app="api"} |= "err"`},
		// Parentheses around a log query are grammatical grouping only.
		{`({app="api"} |= "err")`, `{app="api"} |= "err"`},
		{`(({app="api"}))`, `{app="api"}`},
		// |~ patterns are not compiled at parse time (Loki compiles line
		// filter regexes at evaluation, unlike selector matchers).
		{`{app="api"} |~ "("`, `{app="api"} |~ "("`},
		// An empty line_format template is a valid string.
		{`{app="api"} | line_format ""`, `{app="api"} | line_format ""`},
	}
	for _, tt := range tests {
		parseAndCheck(t, tt.q, tt.want)
	}
}

// TestParseValidMetricQueries covers range aggregations, unwrap forms,
// vector aggregations and scalar expressions.
func TestParseValidMetricQueries(t *testing.T) {
	tests := []struct{ q, want string }{
		// Plain range aggregations.
		{`count_over_time({app="api"} [5m])`, `count_over_time({app="api"} [5m])`},
		{`count_over_time({app="api"}[90m])`, `count_over_time({app="api"} [1h30m])`},
		{`rate({app="api"} [5m])`, `rate({app="api"} [5m])`},
		{`bytes_over_time({app="api"} [1m])`, `bytes_over_time({app="api"} [1m])`},
		{`bytes_rate({app="api"} [1m])`, `bytes_rate({app="api"} [1m])`},
		{`absent_over_time({app="api"} [1m])`, `absent_over_time({app="api"} [1m])`},
		// Grouping after the closing parenthesis, allowed functions only.
		{`max_over_time({app="api"} | unwrap x [1m]) by (env)`, `max_over_time({app="api"} | unwrap x [1m]) by (env)`},
		{`min_over_time({app="api"} | unwrap x [1m]) without (env)`, `min_over_time({app="api"} | unwrap x [1m]) without (env)`},
		{`stddev_over_time({app="api"} | unwrap x [1m]) by ()`, `stddev_over_time({app="api"} | unwrap x [1m]) by ()`},
		// Unwrap conversions and post filters.
		{`sum_over_time({app="api"} | unwrap latency [5m])`, `sum_over_time({app="api"} | unwrap latency [5m])`},
		{`sum_over_time({app="api"} | unwrap duration(latency) [5m])`, `sum_over_time({app="api"} | unwrap duration(latency) [5m])`},
		{`sum_over_time({app="api"} | unwrap bytes(sz) [5m])`, `sum_over_time({app="api"} | unwrap bytes(sz) [5m])`},
		{`sum_over_time({app="api"} | unwrap duration_seconds(t) [5m])`, `sum_over_time({app="api"} | unwrap duration_seconds(t) [5m])`},
		{`sum_over_time({app="api"} | unwrap latency | foo="bar" [5m])`, `sum_over_time({app="api"} | unwrap latency | foo = "bar" [5m])`},
		// A conversion keyword without a parenthesis is a plain label
		// name, like Loki's function-token disambiguation.
		{`sum_over_time({app="api"} | unwrap duration [5m])`, `sum_over_time({app="api"} | unwrap duration [5m])`},
		{`avg_over_time({app="api"} | json | unwrap x [1m])`, `avg_over_time({app="api"} | json | unwrap x [1m])`},
		{`quantile_over_time(0.5, {app="api"} | unwrap x [5m])`, `quantile_over_time(0.5, {app="api"} | unwrap x [5m])`},
		{`quantile_over_time(.5, {app="api"} | unwrap x [5m])`, `quantile_over_time(0.5, {app="api"} | unwrap x [5m])`},
		{`first_over_time({app="api"} | unwrap x [1m])`, `first_over_time({app="api"} | unwrap x [1m])`},
		{`last_over_time({app="api"} | unwrap x [1m])`, `last_over_time({app="api"} | unwrap x [1m])`},
		// Vector aggregations: grouping before and after, parameters,
		// nesting, Grafana's logs-volume shape.
		{`sum(count_over_time({app="api"} [1m]))`, `sum(count_over_time({app="api"} [1m]))`},
		{`sum by (level) (count_over_time({app="api"} [1m]))`, `sum by (level) (count_over_time({app="api"} [1m]))`},
		{`sum(count_over_time({app="api"} [1m])) by (level)`, `sum by (level) (count_over_time({app="api"} [1m]))`},
		{`sum by () (count_over_time({app="api"} [1m]))`, `sum by () (count_over_time({app="api"} [1m]))`},
		{`avg(count_over_time({app="api"} [1m]))`, `avg(count_over_time({app="api"} [1m]))`},
		{`min(count_over_time({app="api"} [1m]))`, `min(count_over_time({app="api"} [1m]))`},
		{`max(count_over_time({app="api"} [1m]))`, `max(count_over_time({app="api"} [1m]))`},
		{`count(count_over_time({app="api"} [1m]))`, `count(count_over_time({app="api"} [1m]))`},
		{`stddev(count_over_time({app="api"} [1m]))`, `stddev(count_over_time({app="api"} [1m]))`},
		{`stdvar(count_over_time({app="api"} [1m]))`, `stdvar(count_over_time({app="api"} [1m]))`},
		{`topk(3, count_over_time({app="api"} [1m]))`, `topk(3, count_over_time({app="api"} [1m]))`},
		{`bottomk(1, count_over_time({app="api"} [1m]))`, `bottomk(1, count_over_time({app="api"} [1m]))`},
		{`sort(count_over_time({app="api"} [1m]))`, `sort(count_over_time({app="api"} [1m]))`},
		{`sort_desc(count_over_time({app="api"} [1m]))`, `sort_desc(count_over_time({app="api"} [1m]))`},
		// sort/sort_desc keep the empty groupings (validateSortGrouping
		// rejects only non-empty ones; see TestLeadA2AdditionalValidation).
		{`sort by () (count_over_time({app="api"} [1m]))`, `sort by () (count_over_time({app="api"} [1m]))`},
		{`sort_desc(count_over_time({app="api"} [1m])) without ()`, `sort_desc without () (count_over_time({app="api"} [1m]))`},
		{`sum(max(count_over_time({app="api"} [1m])))`, `sum(max(count_over_time({app="api"} [1m])))`},
		{`sum by (a, b) (avg by (c) (count_over_time({app="api"} [1m])))`, `sum by (a, b) (avg by (c) (count_over_time({app="api"} [1m])))`},
		{`sum(count_over_time({app="api"} [1m]) + 1)`, `sum(count_over_time({app="api"} [1m]) + 1)`},
		{`sum by (a) (rate({app="api"} [5m]) + 1)`, `sum by (a) (rate({app="api"} [5m]) + 1)`},
		{`rate({app="api"} [5m] | unwrap x)`, `rate({app="api"} | unwrap x [5m])`},
		{`sum by (level, detected_level) (count_over_time({app="api"} | drop __error__ [1m]))`,
			`sum by (level, detected_level) (count_over_time({app="api"} | drop __error__ [1m]))`},
		// Scalar expressions: literals with signs, arithmetic precedence,
		// right-associative ^, comparisons, bool, and lossless constants
		// (Loki folds 1/0 to NaN at parse time; Parkana keeps the tree so
		// every canonical form still reparses).
		{`1`, `1`},
		{`1 + 2`, `1 + 2`},
		{`1 + 2 * 3`, `1 + 2 * 3`},
		{`2 * 3 + 1`, `2 * 3 + 1`},
		{`(1 + 2) * 3`, `(1 + 2) * 3`},
		{`2 - 3 - 4`, `2 - 3 - 4`},
		{`2 - (3 - 4)`, `2 - (3 - 4)`},
		{`2 ^ 3 ^ 2`, `2 ^ 3 ^ 2`},
		{`(2 ^ 3) ^ 2`, `(2 ^ 3) ^ 2`},
		{`-2 ^ 2`, `-2 ^ 2`},
		{`2 ^ -3`, `2 ^ -3`},
		{`1 - -2`, `1 - -2`},
		{`- 2 + 3`, `-2 + 3`},
		{`10 % 3`, `10 % 3`},
		{`10 / 4`, `10 / 4`},
		{`1 / 0`, `1 / 0`},
		{`1.5e3 + 2`, `1500 + 2`},
		{`.5 + 1`, `0.5 + 1`},
		{`1. + 2`, `1 + 2`},
		{`(1 < 2) + 3`, `(1 < 2) + 3`},
		{`1 + bool 2`, `1 + bool 2`},
		// Scalar/vector operations.
		{`count_over_time({app="api"} [1m]) > 10`, `count_over_time({app="api"} [1m]) > 10`},
		{`count_over_time({app="api"} [1m]) >= 10 * 2`, `count_over_time({app="api"} [1m]) >= 10 * 2`},
		{`count_over_time({app="api"} [1m]) > bool 10`, `count_over_time({app="api"} [1m]) > bool 10`},
		{`10 < count_over_time({app="api"} [1m])`, `10 < count_over_time({app="api"} [1m])`},
		{`2 * count_over_time({app="api"} [1m]) + 1`, `2 * count_over_time({app="api"} [1m]) + 1`},
		{`topk(3, count_over_time({app="api"} [1m])) > 5`, `topk(3, count_over_time({app="api"} [1m])) > 5`},
		{`sum(count_over_time({app="api"} [1m])) / 2 / 2`, `sum(count_over_time({app="api"} [1m])) / 2 / 2`},
	}
	for _, tt := range tests {
		parseAndCheck(t, tt.q, tt.want)
	}
}

// TestKeywordCaseInsensitivity pins Loki's lexer: keywords and functions
// match case-insensitively, logfmt flags do not, and label spelling keeps
// its original case.
func TestKeywordCaseInsensitivity(t *testing.T) {
	tests := []struct{ q, want string }{
		{`SUM(1)`, `sum(1)`},
		{`Sum(1)`, `sum(1)`},
		{`sum BY (a) (1)`, `sum by (a) (1)`},
		{`sum(1) WITHOUT ()`, `sum without () (1)`},
		{`COUNT_OVER_TIME({app="api"} [5m])`, `count_over_time({app="api"} [5m])`},
		{`RATE({app="api"} [5m])`, `rate({app="api"} [5m])`},
		{`sum_over_time({app="api"} | UNWRAP duration(latency) [5m])`, `sum_over_time({app="api"} | unwrap duration(latency) [5m])`},
		{`{app="api"} | JSON`, `{app="api"} | json`},
		{`{app="api"} | LOGFMT --strict`, `{app="api"} | logfmt --strict`},
		{`{app="api"} | Line_Format "{{.x}}"`, `{app="api"} | line_format "{{.x}}"`},
		{`{app="api"} | Drop foo`, `{app="api"} | drop foo`},
		{`count_over_time({app="api"} [5m]) > BOOL 10`, `count_over_time({app="api"} [5m]) > bool 10`},
		{`topk(2, rate({APP="api"} [5m]))`, `topk(2, rate({APP="api"} [5m]))`},
		{`SORT_DESC(1)`, `sort_desc(1)`},
		{`LAST_OVER_TIME({app="api"} | unwrap x [1m])`, `last_over_time({app="api"} | unwrap x [1m])`},
	}
	for _, tt := range tests {
		parseAndCheck(t, tt.q, tt.want)
	}
	// The flags are case-sensitive: --STRICT is not a flag.
	if _, err := Parse(`{app="api"} | logfmt --STRICT`); err == nil {
		t.Error(`Parse({app="api"} | logfmt --STRICT) succeeded, want error`)
	}
}

// TestLogRangePlacementMatrix walks every parenthesized/pipeline/unwrap/
// offset arrangement of Loki's logRangeExpr rule (syntax.y lines 128-153)
// and checks they all merge to the same canonical tree. Unwrap forms need
// an unwrap aggregation (sum_over_time), plain forms use count_over_time.
func TestLogRangePlacementMatrix(t *testing.T) {
	tests := []struct{ fn, q, want string }{
		// selector RANGE [offset]
		{`count_over_time`, `{app="api"} [5m]`, `{app="api"} [5m]`},
		{`count_over_time`, `{app="api"} [5m] offset 1h`, `{app="api"} [5m] offset 1h`},
		// ( selector ) RANGE [offset]
		{`count_over_time`, `({app="api"}) [5m]`, `{app="api"} [5m]`},
		{`count_over_time`, `({app="api"}) [5m] offset 1h`, `{app="api"} [5m] offset 1h`},
		// selector RANGE unwrap / offset unwrap
		{`sum_over_time`, `{app="api"} [5m] | unwrap latency`, `{app="api"} | unwrap latency [5m]`},
		{`sum_over_time`, `{app="api"} [5m] offset 1h | unwrap latency`, `{app="api"} | unwrap latency [5m] offset 1h`},
		// ( selector ) RANGE unwrap / offset unwrap
		{`sum_over_time`, `({app="api"}) [5m] | unwrap latency`, `{app="api"} | unwrap latency [5m]`},
		{`sum_over_time`, `({app="api"}) [5m] offset 1h | unwrap latency`, `{app="api"} | unwrap latency [5m] offset 1h`},
		// but never a post-range pipeline after parentheses (production
		// lines 149-152 need a bare selector; see TestLeadA2AdditionalValidation)
		// selector unwrap RANGE [offset]
		{`sum_over_time`, `{app="api"} | unwrap latency [5m]`, `{app="api"} | unwrap latency [5m]`},
		{`sum_over_time`, `{app="api"} | unwrap latency [5m] offset 1h`, `{app="api"} | unwrap latency [5m] offset 1h`},
		// ( selector unwrap ) RANGE [offset]
		{`sum_over_time`, `({app="api"} | unwrap latency) [5m]`, `{app="api"} | unwrap latency [5m]`},
		{`sum_over_time`, `({app="api"} | unwrap latency) [5m] offset 1h`, `{app="api"} | unwrap latency [5m] offset 1h`},
		// selector pipeline RANGE [offset]
		{`count_over_time`, `{app="api"} | json [5m]`, `{app="api"} | json [5m]`},
		{`count_over_time`, `{app="api"} | json [5m] offset 1h`, `{app="api"} | json [5m] offset 1h`},
		// ... with the pipeline's own argument forms and a bare line filter
		// or label filter as the only stage.
		{`count_over_time`, `{app="api"} | json a="b" [5m]`, `{app="api"} | json a="b" [5m]`},
		{`count_over_time`, `{app="api"} |= "x" [5m]`, `{app="api"} |= "x" [5m]`},
		{`count_over_time`, `{app="api"} | foo="bar" [5m]`, `{app="api"} | foo = "bar" [5m]`},
		// ( selector pipeline ) RANGE [offset]
		{`count_over_time`, `({app="api"} | json) [5m]`, `{app="api"} | json [5m]`},
		{`count_over_time`, `({app="api"} | json) [5m] offset 1h`, `{app="api"} | json [5m] offset 1h`},
		// selector pipeline unwrap RANGE [offset]
		{`sum_over_time`, `{app="api"} | json | unwrap latency [5m]`, `{app="api"} | json | unwrap latency [5m]`},
		{`sum_over_time`, `{app="api"} | json | unwrap latency [5m] offset 1h`, `{app="api"} | json | unwrap latency [5m] offset 1h`},
		// ( selector pipeline unwrap ) RANGE [offset]
		{`sum_over_time`, `({app="api"} | json | unwrap latency) [5m]`, `{app="api"} | json | unwrap latency [5m]`},
		{`sum_over_time`, `({app="api"} | json | unwrap latency) [5m] offset 1h`, `{app="api"} | json | unwrap latency [5m] offset 1h`},
		// selector RANGE pipeline [offset pipeline unwrap]
		{`count_over_time`, `{app="api"} [5m] | json`, `{app="api"} | json [5m]`},
		{`count_over_time`, `{app="api"} [5m] offset 1h | json`, `{app="api"} | json [5m] offset 1h`},
		{`sum_over_time`, `{app="api"} [5m] | json | unwrap latency`, `{app="api"} | json | unwrap latency [5m]`},
		{`sum_over_time`, `{app="api"} [5m] offset 1h | json | unwrap latency`, `{app="api"} | json | unwrap latency [5m] offset 1h`},
		// ( logRangeExpr ), recursively
		{`count_over_time`, `({app="api"} [5m])`, `{app="api"} [5m]`},
		{`count_over_time`, `(({app="api"} | json [5m] offset 1h))`, `{app="api"} | json [5m] offset 1h`},
	}
	for _, tt := range tests {
		// A bare range is not a root expression; wrap it for parsing.
		parseAndCheck(t, tt.fn+`(`+tt.q+`)`, tt.fn+`(`+tt.want+`)`)
	}
}

// TestOffsetForms pins offset grammar: any general duration token
// (model units, decimals, sub-millisecond, negative with the minus glued to
// the number as Loki's scanner does), never a spaced or plus-signed form.
func TestOffsetForms(t *testing.T) {
	valid := []struct{ q, want string }{
		{`count_over_time({app="api"} [5m] offset 30m)`, `count_over_time({app="api"} [5m] offset 30m)`},
		{`count_over_time({app="api"} [5m] offset -30m)`, `count_over_time({app="api"} [5m] offset -30m0s)`},
		{`count_over_time({app="api"} [5m] offset 1.5h)`, `count_over_time({app="api"} [5m] offset 1h30m)`},
		{`count_over_time({app="api"} [5m] offset 500ms)`, `count_over_time({app="api"} [5m] offset 500ms)`},
		{`count_over_time({app="api"} [5m] offset 1500us)`, `count_over_time({app="api"} [5m] offset 1.5ms)`},
		{`count_over_time({app="api"} [5m] offset 0s)`, `count_over_time({app="api"} [5m])`},
	}
	for _, tt := range valid {
		parseAndCheck(t, tt.q, tt.want)
	}
	invalid := []string{
		`count_over_time({app="api"} [5m] offset - 30m)`, // spaced minus
		`count_over_time({app="api"} [5m] offset -30 m)`, // unit split from number
		`count_over_time({app="api"} [5m] offset +30m)`,  // no plus form
		`count_over_time({app="api"} [5m] offset 5)`,     // plain number
		`count_over_time({app="api"} [5m] offset "5m")`,  // string
		`count_over_time({app="api"} [5m] offset)`,       // missing duration
		`{app="api"} offset 5m`,                          // log queries have no offset
		`count_over_time({app="api"} [5m]) offset 1h`,    // aggregation has no offset
	}
	for _, q := range invalid {
		if _, err := Parse(q); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", q)
		}
	}
}

// TestRangeDurationGrammar pins that range brackets accept
// model.ParseDuration forms only, unlike general duration tokens.
func TestRangeDurationGrammar(t *testing.T) {
	valid := []string{"5m", "1h30m", "500ms", "0s", "1y", "2w", "3d", "10s", "1m1s"}
	for _, d := range valid {
		parseAndCheck(t, `count_over_time({app="api"} [`+d+`])`, `count_over_time({app="api"} [`+d+`])`)
	}
	// A bare 0 is model.ParseDuration's unitless special case and
	// canonicalizes to 0s.
	parseAndCheck(t, `count_over_time({app="api"} [0])`, `count_over_time({app="api"} [0s])`)
	invalid := []string{
		"1.5s",  // fractional: model durations are integers
		"500ns", // ns is not a model unit
		"100us", // us is not a model unit
		"-5m",   // no sign in model durations
		"30s5m", // wrong unit order for model
		"5m5m",  // repeated unit
		"",      // empty brackets
		"m",     // no number
		"5",     // no unit
		"00",    // only the exact "0" is unitless-valid
		" 5m",   // whitespace inside the brackets
		"5m ",   // whitespace before the closing bracket
	}
	for _, d := range invalid {
		q := `count_over_time({app="api"} [` + d + `])`
		if _, err := Parse(q); err == nil {
			t.Errorf("Parse(%q) succeeded, want error (model-only range durations)", q)
		}
	}
}

// TestLineFilterOrSurface pins the A4 handoff: or-chains keep the surface
// values and operator; the evaluator (not the parser) applies Loki's
// newOrLineFilterExpr semantics where negative chains are intersections.
func TestLineFilterOrSurface(t *testing.T) {
	e, err := Parse(`{app="api"} != "a" or "b" or "c"`)
	if err != nil {
		t.Fatal(err)
	}
	q := e.(*LogQuery)
	lf := q.Stages[0].(*LineFilter)
	if lf.Op != plan.LineNotContains || len(lf.Values) != 3 || lf.Values[0] != "a" || lf.Values[2] != "c" {
		t.Errorf("surface or-chain lost: op=%v values=%v", lf.Op, lf.Values)
	}
}

// TestParsePrecedenceTrees checks the trees behind a few renderings where
// text alone cannot prove associativity or unary attachment.
func TestParsePrecedenceTrees(t *testing.T) {
	// -2 ^ 2 is (-2)^2: the sign belongs to the literal, Loki's literalExpr.
	e, err := Parse(`-2 ^ 2`)
	if err != nil {
		t.Fatal(err)
	}
	b := e.(*BinaryExpr)
	if b.Op != OpPow {
		t.Fatalf("root op = %v, want ^", b.Op)
	}
	if lhs, ok := b.LHS.(*NumberLit); !ok || lhs.V != -2 {
		t.Errorf("-2 ^ 2 LHS = %#v, want literal -2", b.LHS)
	}
	// Comparisons associate left: 1 < 2 < 3 is (1 < 2) < 3.
	e, err = Parse(`1 < 2 < 3`)
	if err != nil {
		t.Fatal(err)
	}
	b = e.(*BinaryExpr)
	if _, ok := b.LHS.(*BinaryExpr); !ok {
		t.Errorf("1 < 2 < 3 LHS = %#v, want a binary comparison (left-associative)", b.LHS)
	}
	// + binds looser than comparison in the render check: (1 < 2) + 3.
	e, _ = Parse(`(1 < 2) + 3`)
	b = e.(*BinaryExpr)
	if b.Op != OpAdd {
		t.Fatalf("root op = %v, want +", b.Op)
	}
	if _, ok := b.LHS.(*BinaryExpr); !ok {
		t.Errorf("(1 < 2) + 3 LHS = %#v, want the comparison", b.LHS)
	}
	// Label filters: or is loosest, juxtaposition/and/comma tie.
	e, err = Parse(`{app="api"} | a="1" or b="2" and c="3"`)
	if err != nil {
		t.Fatal(err)
	}
	root := e.(*LogQuery).Stages[0].(*LabelFilterBinary)
	if root.Op != OpFilterOr {
		t.Fatalf("filter root op = %v, want or", root.Op)
	}
	if rhs, ok := root.RHS.(*LabelFilterBinary); !ok || rhs.Op != OpFilterAnd {
		t.Errorf("or RHS = %#v, want an and node (and binds tighter)", root.RHS)
	}
}

// TestParseNestingBound checks the recursion guard on deeply nested
// parentheses, aggregations and label filters: generous nesting parses,
// adversarial depth fails as a syntax error, never a stack overflow.
func TestParseNestingBound(t *testing.T) {
	deep := strings.Repeat("(", 200) + `1` + strings.Repeat(")", 200)
	if _, err := Parse(deep); err != nil {
		t.Errorf("200 nested parens: %v", err)
	}
	tooDeep := strings.Repeat("(", 800) + `1` + strings.Repeat(")", 800)
	if _, err := Parse(tooDeep); err == nil {
		t.Error("800 nested parens parsed, want nesting error")
	}
	tooDeepAgg := strings.Repeat("sum(", 600) + `1` + strings.Repeat(")", 600)
	if _, err := Parse(tooDeepAgg); err == nil {
		t.Error("600 nested aggregations parsed, want nesting error")
	}
	deepFilter := `{app="api"} | ` + strings.Repeat("(", 800) + `a="1"` + strings.Repeat(")", 800)
	if _, err := Parse(deepFilter); err == nil {
		t.Error("800 nested label-filter parens parsed, want nesting error")
	}
	deepLog := strings.Repeat("(", 800) + `{app="api"} [5m]` + strings.Repeat(")", 800)
	if _, err := Parse(`count_over_time(` + deepLog + `)`); err == nil {
		t.Error("800 nested log-range parens parsed, want nesting error")
	}
}
