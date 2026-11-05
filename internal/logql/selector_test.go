package logql

import (
	"errors"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// sameMatchers compares matchers by type, name and value (the compiled regexp
// is derived from those).
func sameMatchers(a, b []plan.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func TestParseSelectorValid(t *testing.T) {
	tests := []struct {
		q    string
		want []plan.Matcher
	}{
		{`{app="api"}`, []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "api")}},
		{`{app="api", env=~"prod|staging"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
			plan.MustMatcher(plan.MatchRegexp, "env", "prod|staging"),
		}},
		// Negated matchers are valid only beside one that rejects "" (see
		// the all-match-empty rejections in TestParseSelectorInvalid).
		{`{app="api", env!="prod"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
			plan.MustMatcher(plan.MatchNotEqual, "env", "prod"),
		}},
		{`{app="api", path!~"/api/v[0-9]+"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
			plan.MustMatcher(plan.MatchNotRegexp, "path", "/api/v[0-9]+"),
		}},
		// Direct label matchers canonicalize with Loki's syntax.DotNL: the
		// value carries the scope to every plan.Source.
		{`{foo=~".+"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchRegexp, "foo", "(?s:.+)"),
		}},
		{`{foo=~"a.(?-s:b.)"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchRegexp, "foo", "(?-s:a(?s:.)b.)"),
		}},
		// Whitespace and newlines between tokens.
		{"  {  app = \"api\" ,\n env =\"prod\" } ", []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
			plan.MustMatcher(plan.MatchEqual, "env", "prod"),
		}},
		// A comment may sit between a comma and the next matcher; a matcher
		// must still follow the comma (see the invalid table for the rest).
		{"{app=\"api\", # between matchers\n env=\"prod\"}", []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
			plan.MustMatcher(plan.MatchEqual, "env", "prod"),
		}},
		// Raw string value.
		{"{app=`my app`}", []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "app", "my app")}},
		// Escapes: \" \\ \n \t, unicode.
		{`{msg="he said \"hi\""}`, []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "msg", `he said "hi"`)}},
		{`{msg="line\n1\tend"}`, []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "msg", "line\n1\tend")}},
		{`{msg="back\\slash"}`, []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "msg", `back\slash`)}},
		{`{msg="héllo ✓"}`, []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "msg", "héllo ✓")}},
		// A matcher that matches "" is fine when another does not.
		{`{app="", env="prod"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", ""),
			plan.MustMatcher(plan.MatchEqual, "env", "prod"),
		}},
		// Duplicate label names are separate ANDed matchers.
		{`{a="1", a="2"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "a", "1"),
			plan.MustMatcher(plan.MatchEqual, "a", "2"),
		}},
		// Underscores and reserved-looking names.
		{`{__name__="x", _p="y"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "__name__", "x"),
			plan.MustMatcher(plan.MatchEqual, "_p", "y"),
		}},
		// Comments from # to end of line, outside strings, anywhere between
		// tokens: before, within and after the selector.
		{"# lead comment\n{app=\"api\"}", []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
		}},
		{"{app=\"api\"} # trailing comment", []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "app", "api"),
		}},
		// A # inside a quoted value is data, not a comment.
		{`{msg="a # not a comment"}`, []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "msg", "a # not a comment"),
		}},
	}
	for _, tt := range tests {
		sel, err := ParseSelector(tt.q)
		if err != nil {
			t.Errorf("ParseSelector(%q): %v", tt.q, err)
			continue
		}
		if !sameMatchers(sel.Matchers, tt.want) {
			t.Errorf("ParseSelector(%q) matchers = %v, want %v", tt.q, sel.Matchers, tt.want)
		}
		if want := strings.IndexByte(tt.q, '{'); sel.Pos.Offset != want {
			t.Errorf("ParseSelector(%q).Pos.Offset = %d, want %d", tt.q, sel.Pos.Offset, want)
		}
	}
}

func TestParseSelectorInvalid(t *testing.T) {
	tests := []string{
		``,                       // empty query
		`app="api"`,              // missing braces
		`{}`,                     // no matchers
		`{,}`,                    // leading comma
		`{a}`,                    // missing operator
		`{a=}`,                   // missing value
		`{a=1}`,                  // value must be a string
		"{a='x'}",                // no single-quoted strings
		`{a="x"`,                 // missing closing brace
		`{a=="x"}`,               // == is not a matcher operator
		`{a=~"x"},`,              // trailing comma outside the selector
		`{"a"="x"}`,              // label names are identifiers, not strings
		`{1a="x"}`,               // invalid label name
		`{a-b="x"}`,              // invalid label name
		`{a="x" b="y"}`,          // missing comma between matchers
		`{a="x",,b="y"}`,         // doubled comma
		`{app="api",}`,           // trailing comma: a matcher must follow every comma
		"{a=\"1\", # comment\n}", // comment after a comma needs a matcher after it too
		`{a="x",,}`,              // doubled comma before close
		`{a=~"("}`,               // invalid regexp
		`{a=""}`,                 // matches empty
		`{a=~".*"}`,              // matches empty
		`{a=~"x|"}`,              // alternation with an empty branch
		`{app!="api"}`,           // alone: a missing app label reads "" != "api"
		`{a!~"x+"}`,              // "" fails ^x+$, so !~ matches ""
		`{a="", b=~""}`,          // all match empty
		`{a="x"} b`,              // trailing garbage
		`{a="x"} |="err"`,        // trailing pipeline
		`{a="x"}}`,               // trailing brace
		`{a="x"}{b="y"}`,         // trailing selector
		"{a=\"unterminated}",     // string runs to the end
		"{a=`unterminated}",      // raw string runs to the end
		`{a="x" @}`,              // invalid character
		"{\xff}",                 // malformed UTF-8 label position
	}
	for _, q := range tests {
		_, err := ParseSelector(q)
		if err == nil {
			t.Errorf("ParseSelector(%q) succeeded, want error", q)
			continue
		}
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("ParseSelector(%q) err = %v, want ErrBadQuery", q, err)
		}
		if errors.Is(err, plan.ErrUnsupported) {
			t.Errorf("ParseSelector(%q) err = %v, must not be ErrUnsupported", q, err)
		}
	}
}

func TestParseSelectorAnchoredSemantics(t *testing.T) {
	// One matcher that rejects "" is enough; regexps stay anchored RE2.
	sel, err := ParseSelector(`{a=~".*", code=~"5.."}`)
	if err != nil {
		t.Fatal(err)
	}
	if matchersAllMatchEmpty(sel.Matchers) {
		t.Error(`code=~"5.." must not match the empty string (anchored RE2)`)
	}
	if !sel.Matchers[1].Matches("500") || !sel.Matchers[1].Matches("50x") || sel.Matchers[1].Matches("5001") || sel.Matchers[1].Matches("x500") {
		t.Error("matcher semantics must be anchored: 500 and 50x match 5.., 5001 and x500 do not")
	}
}

func TestParseSelectorRoundTrip(t *testing.T) {
	queries := []string{
		`{app="api"}`,
		`{app="api", env=~"prod|staging"}`,
		`{app="api", path!~"/api/v[0-9]+"}`,
		`{foo=~".+"}`,
		`{foo=~"a.b"}`,
		`{foo=~"(?s:(?s:.+))"}`,
		`{foo=~"a.(?-s:b.)"}`,
		`{foo=~"(a).*|(b)."}`,
		`{foo!~"x.*", job="api"}`,
		`{app="", env="prod"}`,
		`{a="1", a="2"}`,
		"{app=`raw value`}",
		`{msg="he said \"hi\""}`,
		`{msg="héllo ✓"}`,
	}
	for _, q := range queries {
		sel, err := ParseSelector(q)
		if err != nil {
			t.Fatalf("ParseSelector(%q): %v", q, err)
		}
		first := sel.String()
		sel2, err := ParseSelector(first)
		if err != nil {
			t.Fatalf("reparse of %q -> %q: %v", q, first, err)
		}
		if second := sel2.String(); second != first {
			t.Errorf("round trip %q: %q != %q", q, second, first)
		}
		if !sameMatchers(sel.Matchers, sel2.Matchers) {
			t.Errorf("round trip %q: matchers differ: %v vs %v", q, sel.Matchers, sel2.Matchers)
		}
	}
}

func TestParseSelectorErrorPosition(t *testing.T) {
	_, err := ParseSelector(`{app="api" {`)
	if err == nil || !strings.Contains(err.Error(), "at 1:12") {
		t.Errorf("err = %v, want a position at 1:12", err)
	}
	_, err = ParseSelector(`{a-"x"}`)
	if err == nil || !strings.Contains(err.Error(), "at 1:3") {
		t.Errorf("err = %v, want the matcher operator position at 1:3", err)
	}
}

func TestEngineParseSelector(t *testing.T) {
	e := New(config.LogQL{})
	ms, err := e.ParseSelector(`{app="api", env=~"prod"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !sameMatchers(ms, []plan.Matcher{
		plan.MustMatcher(plan.MatchEqual, "app", "api"),
		plan.MustMatcher(plan.MatchRegexp, "env", "prod"),
	}) {
		t.Errorf("Engine.ParseSelector matchers = %v", ms)
	}
	if _, err := e.ParseSelector(`{}`); !errors.Is(err, plan.ErrBadQuery) {
		t.Errorf("Engine.ParseSelector({}) err = %v, want ErrBadQuery", err)
	}
}

// TestSelectorReservedWords pins Loki's lexer behavior for words used as
// label names (pkg_logql_syntax_lex.go lines 226-254): the unconditional
// tokens map keywords can never be identifiers — {and="x"} is a syntax
// error — while functionTokens names (sum, rate, bytes, vector, and the
// dual-map ip and label_replace) fall back to identifiers when not followed
// by '(' or a by/without grouping, keeping their original spelling.
func TestSelectorReservedWords(t *testing.T) {
	valid := []string{
		// Function names are labels when no parenthesis follows.
		`{sum="x", rate="y"}`,
		`{bytes="x", duration_seconds="y"}`,
		`{vector="x", duration="y"}`,
		// ip and label_replace live in both maps: identifier without '('.
		`{ip="10.0.0.1", label_replace="x"}`,
		// Mixed-case spelling is preserved, and case does not make a
		// function name reserved.
		`{Sum="x", BYE="z"}`,
		// Non-keyword identifiers that merely contain keywords.
		`{android="yes", offset_x="1"}`,
	}
	for _, q := range valid {
		sel, err := ParseSelector(q)
		if err != nil {
			t.Errorf("ParseSelector(%q): %v", q, err)
			continue
		}
		if _, err := Parse(q + ` |= "err"`); err != nil {
			t.Errorf("Parse(%q |= \"err\"): %v", q, err)
		}
		if sel.Matchers[0].Name == "" {
			t.Errorf("ParseSelector(%q) lost the label spelling", q)
		}
	}
	reserved := []string{
		// Unconditional keywords, in any case.
		`{and="x"}`, `{AND="x"}`, `{And="x"}`,
		`{or="x"}`, `{unless="x"}`,
		`{by="x"}`, `{BY="x"}`, `{without="x"}`,
		`{bool="x"}`, `{offset="x"}`,
		`{on="x"}`, `{ignoring="x"}`,
		`{group_left="x"}`, `{group_right="x"}`,
		`{unwrap="x"}`,
		`{json="x"}`, `{JSON="x"}`, `{logfmt="x"}`, `{regexp="x"}`,
		`{pattern="x"}`, `{unpack="x"}`,
		`{line_format="x"}`, `{label_format="x"}`, `{decolorize="x"}`,
		`{drop="x"}`, `{DROP="x"}`, `{keep="x"}`,
		`{variants="x"}`, `{of="x"}`,
	}
	for _, q := range reserved {
		if _, err := ParseSelector(q); !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("ParseSelector(%q) err = %v, want ErrBadQuery (reserved word)", q, err)
		}
		if _, err := Parse(q + ` |= "err"`); !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("Parse(%q |= \"err\") err = %v, want ErrBadQuery (reserved word)", q, err)
		}
	}
}
