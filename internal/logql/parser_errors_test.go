package logql

import (
	"errors"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// wantBad asserts err is a syntax error: ErrBadQuery, never ErrUnsupported.
func wantBad(t *testing.T, q string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("Parse(%q) succeeded, want ErrBadQuery", q)
		return
	}
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("Parse(%q) err = %v, want ErrBadQuery", q, err)
	}
	if errors.Is(err, plan.ErrUnsupported) {
		t.Errorf("Parse(%q) err = %v, must not claim ErrUnsupported", q, err)
	}
}

// TestParseInvalid covers malformed supported syntax: every family's
// broken forms classify plan.ErrBadQuery.
func TestParseInvalid(t *testing.T) {
	tests := []string{
		// Empty and trailing garbage.
		``,
		`   `,
		`{a="1"} junk`,
		`sum(1) 2`,
		`1 2`,
		`{a="1"} {b="2"}`,
		// Line filters.
		`{a="1"} |=`,
		`{a="1"} |= 5`,
		`{a="1"} |= "x" or`,
		`{a="1"} |= "x" or 5`,
		`{a="1"} |= "x" or != "y"`, // or-alternatives are strings only
		`{a="1"} |`,
		`{a="1"} | 5`,
		`{a="1"} |= "x" foo="y"`, // label filters need a pipe
		// Parsers.
		`{a="1"} | json --strict`,             // json takes no flags
		`{a="1"} | json 5`,                    // extraction is name or name="path"
		`{a="1"} | json a=b`,                  // path must be quoted
		`{a="1"} | json a="b" c="d"`,          // missing comma
		`{a="1"} | json a="b",`,               // trailing comma
		`{a="1"} | json and="b"`,              // reserved extraction name
		`{a="1"} | logfmt --STRICT`,           // flags are case-sensitive
		`{a="1"} | logfmt --bogus`,            // unknown flag
		`{a="1"} | logfmt -strict`,            // single dash is not a flag
		`{a="1"} | logfmt a="b" --strict`,     // flags precede extractions
		`{a="1"} | regexp`,                    // missing pattern
		`{a="1"} | regexp "no-group"`,         // needs a named capture
		`{a="1"} | regexp "(a)(b)"`,           // no named capture at all
		`{a="1"} | regexp "(?P<x>a)(?P<x>b)"`, // duplicate names
		`{a="1"} | regexp "("`,                // invalid RE2
		`{a="1"} | regexp 5`,
		`{a="1"} | pattern`,
		`{a="1"} | pattern 5`,
		// Formatting.
		`{a="1"} | line_format`,
		`{a="1"} | line_format 5`,
		`{a="1"} | label_format`,
		`{a="1"} | label_format foo`,
		`{a="1"} | label_format foo=5`,
		`{a="1"} | label_format foo=bar,`,
		`{a="1"} | label_format and=foo`,
		`{a="1"} | label_format foo=json`,
		// Drop/keep.
		`{a="1"} | drop`,
		`{a="1"} | drop a,`,
		`{a="1"} | drop json`,   // reserved target name
		`{a="1"} | drop a=~"("`, // invalid matcher regexp
		`{a="1"} | drop a > "x"`,
		`{a="1"} | keep`,
		// Label filters.
		`{a="1"} | foo`,
		`{a="1"} | foo bar`,
		`{a="1"} | foo=`,
		`{a="1"} | foo == "bar"`,      // == rejects strings
		`{a="1"} | foo > "bar"`,       // > rejects strings
		`{a="1"} | foo =~ 5`,          // =~ needs a string
		`{a="1"} | foo = ip("x") and`, // dangling and
		`{a="1"} | (a="b"`,
		`{a="1"} | (a="b"))`,
		`{a="1"} | a="b" and`,
		`{a="1"} | a="b" or`,
		`{a="1"} | a="b",`,
		`{a="1"} | and="b"`,        // reserved label name
		`{a="b"} | latency > - 1s`, // spaced minus is not a glued duration
		`{a="b"} | latency > +1s`,  // no plus form for durations
		`{a="b"} | size > -1KB`,    // bytes never take a sign
		// Unwrap.
		`{a="1"} | unwrap`,
		`{a="1"} | unwrap json`,
		`{a="1"} | unwrap "x"`,
		`{a="1"} | unwrap x`,                            // no range: unwrap belongs to ranges
		`sum_over_time({a="1"} | unwrap x | json [5m])`, // only label filters follow unwrap
		`sum_over_time({a="1"} | unwrap x | unwrap y [5m])`,
		// Ranges and placement.
		`count_over_time({a="1"})`, // missing range
		`count_over_time(5)`,
		`count_over_time({a="1"} [5m] [3m])`,
		`count_over_time(({a="1"} [5m]) [3m])`,
		`count_over_time((({a="1"} | json))[5m])`,        // only one paren layer takes a range
		`count_over_time({a="1"} [5m`,                    // unclosed bracket
		`count_over_time({a="1"} [5m] | json offset 1h)`, // offset follows the range, not the pipeline
		`{a="1"} [5m]`,                                   // bare range at the root
		`({a="1"} [5m])`,                                 // parenthesized bare range at the root
		`{a="1"} (| json)`,                               // pipelines do not continue a parenthesized query
		// Range aggregation arity and validation.
		`rate(0.5, {a="1"} [5m])`,                 // parameter not supported
		`quantile_over_time({a="1"} [5m])`,        // parameter required
		`quantile_over_time("0.5", {a="1"} [5m])`, // parameter is a number
		`count_over_time by (a) ({a="1"} [5m])`,   // range aggs take grouping after only
		`count_over_time({a="1"} [5m]) by (a)`,    // count does not group
		`rate({a="1"} [5m]) by (a)`,
		`bytes_over_time({a="1"} [5m]) by (a)`,
		`sum_over_time({a="1"} [5m])`,              // sum_over_time needs unwrap
		`avg_over_time({a="1"} [5m])`,              // avg_over_time needs unwrap
		`count_over_time({a="1"} [5m] | unwrap x)`, // count rejects unwrap
		`bytes_over_time({a="1"} [5m] | unwrap x)`,
		`max_over_time({a="1"} [5m]) by (a,)`, // trailing comma in grouping
		`max_over_time({a="1"} [5m] | unwrap x) by (and)`,
		// Vector aggregation arity and validation.
		`topk(count_over_time({a="1"} [5m]))`,      // parameter required
		`topk(0, count_over_time({a="1"} [5m]))`,   // k must be > 0
		`topk(1.5, count_over_time({a="1"} [5m]))`, // k must be an integer
		`topk(-1, count_over_time({a="1"} [5m]))`,
		`topk(1e2, count_over_time({a="1"} [5m]))`,
		`sum(1, count_over_time({a="1"} [5m]))`, // sum takes no parameter
		`sum by (a) by (b) (1)`,                 // grouping twice
		`sum(1) by (a) by (b)`,
		`sum by (a,) (1)`,   // trailing comma
		`sum by (and) (1)`,  // reserved grouping label
		`sum({a="1"})`,      // log query argument
		`sum({a="1"} [5m])`, // log range argument
		`sum by (a`,         // unclosed grouping
		`sum(`,
		`sum()`, // empty argument
		// Scalar expressions.
		`foo`,
		`foo + 1`,
		`foo(1)`,
		`duration(5) + 1`, // conversion functions only exist in unwrap
		`bytes(5)`,
		`1 +`,
		`+`,
		`1 + +`, // sign needs a number
		`()`,
		`(1`,
		`(1))`,
		`1..5`,
		`1 and 2`, // literal legs on set operations
		`rate({a="1"} [5m]) and 2`,
		`(1+2) and count_over_time({a="1"} [1m])`, // folded-literal leg
		`count_over_time({a="1"} [1m]) or (2*3)`,
		`1 + group_left (a) 2`, // group_* needs on/ignoring
		`count_over_time({a="1"} [5m]) and bool on(x) count_over_time({a="1"} [5m]) and group_left`, // group_* without on
		// Deferred families with malformed payloads: syntax errors, not
		// unsupported. Trailing tokens after a deferred construct also stay
		// syntax errors: validation precedes unsupported classification.
		`vector(1) junk`,
		`{a="1"} |>`, // pattern line filter needs a string
		`{a="1"} |> 5`,
		"{a=\"1\"} |= ip(",
		`{a="1"} | foo = ip("x") or garbage`,
		`{a="1"} | foo =~ ip("x")`, // ip needs = or !=
		`{a="1"} | unpack extra`,
		`vector()`,
		`vector(a)`,
		`vector(-1)`,                           // the grammar takes a bare NUMBER
		`vector(+1)`,                           // no signed literal either
		`vector(1+1)`,                          // not an arbitrary scalar expression
		`vector(1, 2)`,                         // one argument
		`label_replace(1, "a", "b", "c", "(")`, // invalid regex
		`label_replace(1, "a", "b", "c")`,      // wrong arity
		`label_replace({a="1"}, "a", "b", "c", "d")`, // non-metric argument
		`variants(rate({a="1"} [5m]))`,               // missing of
		`variants(rate({a="1"} [5m])) of (5)`,
		`rate_counter({a="1"} [5m])`, // rate_counter needs unwrap
		`rate_counter(0.5, {a="1"} [5m] | unwrap x)`,
		`rate_counter(garbage)`,
		`approx_topk(rate({a="1"} [5m]))`,           // parameter required
		`approx_topk(2, rate({a="1"} [5m])) by (a)`, // grouping not allowed
		`approx_topk(a, 1)`,                         // parameter is a number
		`approx_topk(1.5, 1)`,                       // integer parameter
		`approx_topk(0, 1)`,                         // must be greater than 0
		// Truncated deferred shapes and remaining argument errors.
		`vector(1`,
		`vector 1`, // not a call without '('
		`variants(rate({a="1"} [5m])`,
		`variants(rate({a="1"} [5m]), garbage)`,
		`count_over_time((1+2))`,            // metric parens are not a log range
		`count_over_time(({a="1"} | json))`, // parenthesized unit without a range
		`sum_over_time({a="1"} [5m] | unwrap)`,
		`sum_over_time({a="1"} [5m] | unwrap duration())`,
		`sum_over_time({a="1"} [5m] | unwrap x | garbage="y" or)`,
		`{a="1"} | foo = ip("x"`,
		`{a="1"} | drop a=`,
		`sum by a (1)`,   // grouping needs parentheses
		`sum(1) by (a,)`, // trailing comma in after-grouping
		`1 + on(a,) 2`,   // trailing comma in matching labels
	}
	for _, q := range tests {
		_, err := Parse(q)
		wantBad(t, q, err)
	}
}

// TestParseUnsupported covers valid LogQL families Parkana defers: they
// parse completely (tokens, types, regexes, arguments) and classify
// plan.ErrUnsupported.
func TestParseUnsupported(t *testing.T) {
	tests := []string{
		// Vector matching modifiers on any binary operation.
		`count_over_time({a="1"} [5m]) + on(app) count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) + ignoring(app) count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) and on(app) count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) or ignoring() count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) * on(a, b) group_left count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) / on(a) group_left(c) count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) / ignoring(a) group_right(c, d) count_over_time({a="1"} [5m])`,
		`count_over_time({a="1"} [5m]) / on(a) group_left() count_over_time({a="1"} [5m])`,
		`1 + on(a) 2`, // grammar allows the modifier on scalar legs too
		`1 + bool on(a) 2`,
		// Logical/set vector operations stay deferred even without a
		// modifier; vector/vector arithmetic and comparisons are supported.
		`count_over_time({a="1"} [5m]) and count_over_time({a="1"} [5m])`,
		`rate({a="1"} [5m]) or rate({a="1"} [5m])`,
		`rate({a="1"} [5m]) unless rate({a="1"} [5m])`,
		`vector(1) and vector(1)`,
		`vector(1) or vector(1)`,
		`vector(1) unless vector(1)`,
		// Deferred functions.
		`label_replace(rate({a="1"} [5m]), "dst", "x-$1", "src", "re.*")`,
		`variants(rate({a="1"} [5m])) of ({a="1"} [1m])`,
		`variants(rate({a="1"} [5m]), sum(rate({a="1"} [5m]))) of ({a="1"} [1m])`,
		`rate_counter({a="1"} [5m] | unwrap x)`,
		`rate_counter({a="1"} [5m] offset 1h | unwrap x)`,
		`approx_topk(2, rate({a="1"} [5m]))`,
		// Deferred pipeline stages and filters.
		`{a="1"} | unpack`,
		`{a="1"} | json | unpack`,
		`{a="1"} | decolorize`,
		`{a="1"} |> "<method> <uri>"`,
		`{a="1"} !> "<method>"`,
		`{a="1"} |~ "x" or ip("10.0.0.1")`,
		`{a="1"} |= ip("10.0.0.1")`,
		`{a="1"} | ip != ip("10.0.0.1") and level="info"`,
	}
	for _, q := range tests {
		e, err := Parse(q)
		if err == nil {
			t.Errorf("Parse(%q) = %v, want ErrUnsupported", q, e)
			continue
		}
		if !errors.Is(err, plan.ErrUnsupported) {
			t.Errorf("Parse(%q) err = %v, want ErrUnsupported", q, err)
		}
		if errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("Parse(%q) err = %v, must not claim ErrBadQuery", q, err)
		}
	}
	// ip as a plain label name is a normal string matcher: only the ip(...)
	// function form defers.
	parseAndCheck(t, `{a="1"} | ip="10.0.0.1"`, `{a="1"} | ip = "10.0.0.1"`)
}

// TestParseLogVsMetricTypes pins the type boundary: log queries and bare
// log ranges are never binary operands, and log queries are never
// aggregation arguments.
func TestParseLogVsMetricTypes(t *testing.T) {
	tests := []string{
		`{a="1"} + 1`,
		`1 + {a="1"}`,
		`({a="1"} | json) * 2`,
		`{a="1"} [5m] + 1`,
		`({a="1"} [5m]) - 1`,
		`count_over_time({a="1"} [5m]) and {a="1"}`,
		`sum({a="1"} | json)`,
		`avg(rate({a="1"} [5m]) + {a="1"})`,
	}
	for _, q := range tests {
		_, err := Parse(q)
		wantBad(t, q, err)
	}
}

// TestParseErrorPositions pins source positions in error messages.
func TestParseErrorPositions(t *testing.T) {
	tests := []struct {
		q    string
		want string // position substring, like "at 1:12"
	}{
		{`{a="1"} |= `, "at 1:12"},        // end of query after |=
		{`sum(1) junk`, "at 1:8"},         // trailing identifier
		{`{a="1"} {b`, "at 1:9"},          // trailing selector
		{`{a="1"} | json a=b`, "at 1:18"}, // unquoted path
		{`count_over_time(5)`, "at 1:17"}, // non-selector argument
		{`sum(topk(0, 1))`, "at 1:10"},    // parameter anchored at its token
		{`{a="1"} | foo == "x"`, "at 1:15"},
		{"{a=\"1\"}\n| json a=b", "at 2:10"}, // line two
	}
	for _, tt := range tests {
		_, err := Parse(tt.q)
		if err == nil {
			t.Errorf("Parse(%q) succeeded, want error at %s", tt.q, tt.want)
			continue
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("Parse(%q) err = %v, want position %s", tt.q, err, tt.want)
		}
	}
}

// TestLeadA2AdditionalValidation: lead review round 1. sort/sort_desc take
// no non-empty grouping (pinned parser.go validateSortGrouping, lines
// 253-258); ip() label filters take = or != only; and the grammar has no
// production mixing a pre-range pipeline or unwrap (or a parenthesized
// selector) with post-range stages or unwrap — syntax.y lines 128-153 allow
// post-range content only directly after a bare selector (lines 149-152).
func TestLeadA2AdditionalValidation(t *testing.T) {
	for _, q := range []string{
		`sort by (app) (count_over_time({a="b"}[1m]))`,
		`sort_desc(count_over_time({a="b"}[1m])) without(app)`,
		`{a="b"} | host == ip("10.0.0.1")`,
		`count_over_time({a="b"} | json [1m] | logfmt)`,
		`sum_over_time({a="b"} | unwrap x [1m] | json)`,
		`sum_over_time({a="b"} | json [1m] | unwrap x)`,
		`count_over_time(({a="b"})[1m] | json)`,
		`sum_over_time(({a="b"} | json)[1m] | unwrap x)`,
		`count_over_time({a="b"}[ 1m])`,
		`count_over_time({a="b"}[1m ])`,
		"count_over_time({a=\"b\"}[# comment\n1m])",
	} {
		t.Run(q, func(t *testing.T) {
			_, err := Parse(q)
			wantBad(t, q, err)
		})
	}
}

// TestLeadA2NegativeDurationsAndZeroRange: Loki's lexer glues '-' to
// durations in any context (lex.go lines 177-189), so label filters accept
// negative durations, and model.ParseDuration accepts a bare "0"
// (prometheus/common v0.67.5 model/time.go lines 205-212), so [0] is a
// valid range.
func TestLeadA2NegativeDurationsAndZeroRange(t *testing.T) {
	for _, q := range []string{
		`{a="b"} | latency > -1s`,
		`{a="b"} | latency == -1.5s`,
		`count_over_time({a="b"}[0])`,
	} {
		t.Run(q, func(t *testing.T) {
			e, err := Parse(q)
			if err != nil {
				t.Fatalf("valid pinned query rejected: %v", err)
			}
			if _, err := Parse(e.String()); err != nil {
				t.Fatalf("canonical form %s failed: %v", e.String(), err)
			}
		})
	}
}

// TestLeadA2LeftDeepBounds: the depth guard must cover left-deep chains
// built iteratively by the precedence loops, not only right recursion.
func TestLeadA2LeftDeepBounds(t *testing.T) {
	for _, q := range []string{
		strings.Repeat("1+", 800) + "1",
		`{a="b"} | ` + strings.Repeat(`x="y" and `, 800) + `x="y"`,
	} {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("left-deep AST over 800 nodes escaped depth guard: %q…", q[:32])
		}
	}
}

// TestLeadA2InputLimit: Loki rejects inputs at maxInputSize 131072 bytes
// (pinned parser.go lines 42, 86-88), including oversized quoted matchers.
func TestLeadA2InputLimit(t *testing.T) {
	_, err := Parse(`{a="` + strings.Repeat("x", 131072) + `"}`)
	if !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("oversize query should fail: %v", err)
	}
	if _, err := ParseSelector(`{a="` + strings.Repeat("x", 131072) + `"}`); !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("oversize selector should fail: %v", err)
	}
	if _, err := Parse(`{a="` + strings.Repeat("x", 131040) + `"}`); err != nil {
		t.Errorf("query under the limit should parse: %v", err)
	}
}

// TestLeadA2WrappedCompleteRanges: a parenthesis around a COMPLETE
// logRangeExpr (syntax.y line 153) wraps a range built by the bare-selector
// productions 149-152, so post-range stages, offset and unwrap all stay
// valid inside it; the paren placement restriction applies only when the
// range attaches after the closing parenthesis (lines 131-136, 143-148).
func TestLeadA2WrappedCompleteRanges(t *testing.T) {
	for _, q := range []string{
		`count_over_time(({a="b"}[1m] | json))`,
		`count_over_time((({a="b"}[1m] | json)))`,
		`sum_over_time(({a="b"}[1m] | json | unwrap x))`,
		`count_over_time(({a="b"}[1m] offset 1h | json))`,
	} {
		t.Run(q, func(t *testing.T) {
			e, err := Parse(q)
			if err != nil {
				t.Fatalf("parenthesized COMPLETE logRangeExpr rejected: %v", err)
			}
			if _, err := Parse(e.String()); err != nil {
				t.Fatalf("canonical form rejected: %v", err)
			}
		})
	}
}

// TestLeadA2GrafanaCorpus: the pinned Grafana corpus query shapes must all
// parse and round trip.
func TestLeadA2GrafanaCorpus(t *testing.T) {
	for _, tc := range []struct{ name, q string }{
		{"logs-base-api-range", `{service_name="api"}`},
		{"logs-base-web-line-filter", `{service_name="web"} |= "stream=web"`},
		{"logs-job-only-legacy", `{job="legacy"}`},
		{"logs-corpus-json-parser-filter", `{service_name="corpus", format="json"} | json status="response.status" | status >= 400`},
		{"logs-corpus-logfmt-line-filter", `{service_name="corpus", format="logfmt"} |= "status=5"`},
		{"logs-corpus-plain-regexp", `{service_name="corpus", format="plain"} |~ "ERROR pod-[ab] status=5[0-9][0-9]"`},
		{"count-instant-by-level", `sum by (level) (count_over_time({service_name="corpus"}[5m]))`},
		{"grafana-histogram-api", `sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__ [1m]))`},
		{"grafana-histogram-corpus-json", `sum by (level, detected_level) (count_over_time({service_name="corpus", format="json"} | json | drop __error__ [1m]))`},
		{"grafana-histogram-corpus-logfmt", `sum by (level, detected_level) (count_over_time({service_name="corpus", format="logfmt"} | logfmt | drop __error__ [1m]))`},
		{"grafana-labels-scoped", `{service_name="corpus"}`},
		{"grafana-label-values-service-name", `{job="corpus"}`},
		{"grafana-label-values-format-scoped", `{service_name="corpus", level="error"}`},
		{"grafana-series-corpus", `{service_name="corpus"}`},
		{"series-job-only-legacy", `{job="legacy"}`},
		{"series-multiple-selectors", `{service_name="api"}`},
		{"series-multiple-selectors-match-web", `{service_name="web"}`},
		{"series-multiple-selectors-match-legacy", `{job="legacy"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := Parse(tc.q)
			if err != nil {
				t.Fatalf("corpus query rejected: %v", err)
			}
			if _, err := Parse(e.String()); err != nil {
				t.Fatalf("canonical corpus query rejected: %v", err)
			}
		})
	}
}
