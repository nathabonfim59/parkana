package logql

import (
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// dotNLFixture is the MemSource the acceptance tests select from: labels whose
// values contain newlines next to ordinary values, a missing label and a row
// in another tenant.
func dotNLFixture() (*plan.MemSource, time.Time) {
	t0 := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	labels := func(kv ...string) logs.Labels {
		m := make(map[string]string, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return logs.FromMap(m)
	}
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		plan.Row{TS: t0.Add(0 * time.Second), Labels: labels("foo", "\n", "job", "api"), Line: "nl only"},
		plan.Row{TS: t0.Add(1 * time.Second), Labels: labels("foo", "a\nb", "job", "api"), Line: "nl middle"},
		plan.Row{TS: t0.Add(2 * time.Second), Labels: labels("foo", "a\nb\n", "job", "api"), Line: "nl plus"},
		plan.Row{TS: t0.Add(3 * time.Second), Labels: labels("foo", "axb", "job", "api"), Line: "ascii"},
		plan.Row{TS: t0.Add(4 * time.Second), Labels: labels("job", "api"), Line: "no foo"},
		plan.Row{TS: t0.Add(5 * time.Second), Labels: labels("foo", "a\nb.", "job", "api"), Line: "override dot"},
		plan.Row{TS: t0.Add(6 * time.Second), Labels: labels("foo", "x", "job", "api"), Line: "one char"},
		plan.Row{TS: t0.Add(7 * time.Second), Labels: labels("eq", "a\nb", "job", "api"), Line: "equality"},
		plan.Row{TS: t0.Add(8 * time.Second), Labels: labels("foo", "xa\nb", "job", "api"), Line: "prefixed"},
		plan.Row{TS: t0.Add(9 * time.Second), Labels: labels("eq", "a.b", "job", "api"), Line: "eq literal"},
	)
	src.Add("other",
		plan.Row{TS: t0.Add(0 * time.Second), Labels: labels("foo", "\n", "job", "api"), Line: "other nl only"},
	)
	return src, t0
}

// selectLines runs a query through Engine.ParseSelector and plan.MemSource,
// the same path /series and the label endpoints use.
func selectLines(t *testing.T, tenantID tenant.ID, t0 time.Time, q string) []string {
	t.Helper()
	src, _ := dotNLFixture()
	matchers, err := New(config.LogQL{}).ParseSelector(q)
	if err != nil {
		t.Fatalf("ParseSelector(%q): %v", q, err)
	}
	sel := plan.Selection{
		Tenant:    tenantID,
		Matchers:  matchers,
		From:      t0,
		Through:   t0.Add(time.Hour),
		Direction: plan.Forward,
	}
	var lines []string
	for r, err := range src.Select(t.Context(), sel) {
		if err != nil {
			t.Fatalf("Select(%q): %v", q, err)
		}
		lines = append(lines, r.Line)
	}
	return lines
}

// TestDotNLMatching is the acceptance regression: Loki/Prometheus label
// regexps use syntax.DotNL, so . matches a newline inside a label value. It
// fails without the scoped matcher value: the rows with newline-only or
// newline-containing label values would be dropped (positive matchers) or
// wrongly selected (negative matchers).
func TestDotNLMatching(t *testing.T) {
	_, t0 := dotNLFixture()
	tests := []struct {
		name string
		q    string
		want []string
	}{
		{
			name: "dot matches newline-only value",
			q:    `{foo=~".+"}`,
			want: []string{"nl only", "nl middle", "nl plus", "ascii", "override dot", "one char", "prefixed"},
		},
		{
			name: "anchored dot matches newline in the middle",
			q:    `{foo=~"a.b", job="api"}`,
			want: []string{"nl middle", "ascii"},
		},
		{
			name: "negated matcher rejects a newline value",
			q:    `{foo!~"a.b", job="api"}`,
			want: []string{"nl only", "nl plus", "no foo", "override dot", "one char", "equality", "prefixed", "eq literal"},
		},
		{
			name: "explicit (?-s:...) disables dot-newline for its group",
			q:    `{foo=~"a.(?-s:b.)", job="api"}`,
			want: []string{"override dot"},
		},
		{
			name: "explicit (?s:...) enables dot-newline",
			q:    `{foo=~"(?s:.)", job="api"}`,
			want: []string{"nl only", "one char"},
		},
		{
			name: "explicit (?-s:.) still excludes newline",
			q:    `{foo=~"(?-s:.)", job="api"}`,
			want: []string{"one char"},
		},
		{
			name: "equality keeps byte semantics",
			q:    `{eq="a\nb"}`,
			want: []string{"equality"},
		},
		{
			name: "equality with regex metacharacters stays literal",
			q:    `{eq="a.b"}`,
			want: []string{"eq literal"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectLines(t, tenant.Default, t0, tt.q); !slices.Equal(got, tt.want) {
				t.Errorf("select %q = %q, want %q", tt.q, got, tt.want)
			}
		})
	}
}

// TestDotNLMatchingOtherTenant pins that the same semantics apply per tenant
// and that rows never leak across tenants.
func TestDotNLMatchingOtherTenant(t *testing.T) {
	_, t0 := dotNLFixture()
	got := selectLines(t, "other", t0, `{foo=~".+"}`)
	if want := []string{"other nl only"}; !slices.Equal(got, want) {
		t.Errorf(`select {foo=~".+"} for tenant other = %q, want %q`, got, want)
	}
}

// TestDropKeepMatcherDotNL covers the direct matcher use in the drop/keep
// stages (drop_labels.go and keep_labels.go call Matcher.Matches directly,
// without a plan.Selection). The A6 runtime is not needed to tell whether the
// matcher accepts a newline.
func TestDropKeepMatcherDotNL(t *testing.T) {
	tests := []struct {
		name  string
		q     string
		value string
		want  bool
	}{
		{name: "drop regexp accepts a newline", q: `{job="api"} | drop foo, bar=~"x.*"`, value: "x\ny", want: true},
		{name: "drop regexp is anchored", q: `{job="api"} | drop foo, bar=~"x.*"`, value: "ax\ny", want: false},
		{name: "keep negated regexp rejects a newline", q: `{job="api"} | keep foo, bar!~"x.*"`, value: "x\ny", want: false},
		{name: "keep negated regexp accepts a non-match", q: `{job="api"} | keep foo, bar!~"x.*"`, value: "y", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.q)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.q, err)
			}
			var target DropTarget
			switch stage := q.(*LogQuery).Stages[0].(type) {
			case *Drop:
				target = stage.Targets[1]
			case *Keep:
				target = stage.Targets[1]
			default:
				t.Fatalf("stage 0 is %T, want *Drop or *Keep", stage)
			}
			if target.Matcher == nil {
				t.Fatal("target has no matcher")
			}
			if got := target.Matcher.Matches(tt.value); got != tt.want {
				t.Errorf("%s matcher %s with value %q = %v, want %v", tt.q, target, tt.value, got, tt.want)
			}
		})
	}
}

// TestDropKeepMatcherValueIsScoped pins that the drop/keep matcher value, the
// only representation a SQL Source sees, carries the dot-newline scope.
func TestDropKeepMatcherValueIsScoped(t *testing.T) {
	for _, tt := range []struct {
		q    string
		want string
	}{
		{`{job="api"} | drop bar=~"x.*"`, `(?s:x.*)`},
		{`{job="api"} | keep bar!~"x.*"`, `(?s:x.*)`},
	} {
		q, err := Parse(tt.q)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tt.q, err)
		}
		var target DropTarget
		switch stage := q.(*LogQuery).Stages[0].(type) {
		case *Drop:
			target = stage.Targets[0]
		case *Keep:
			target = stage.Targets[0]
		default:
			t.Fatalf("%q: stage 0 is %T, want *Drop or *Keep", tt.q, stage)
		}
		if target.Matcher == nil {
			t.Fatalf("%q: target has no matcher", tt.q)
		}
		if got := target.Matcher.Value; got != tt.want {
			t.Errorf("%q matcher value = %q, want %q", tt.q, got, tt.want)
		}
	}
}

// TestPipelineStringRegexpsStayRaw pins the pinned Loki runtime distinction:
// pipeline label filters and line filters parse their regexps without DotNL,
// so their AST values must not be rewritten by the direct-matcher helper.
func TestPipelineStringRegexpsStayRaw(t *testing.T) {
	q, err := Parse(`{app="api"} |~ "x.*" | bar=~"y.*"`)
	if err != nil {
		t.Fatal(err)
	}
	stages := q.(*LogQuery).Stages
	line, ok := stages[0].(*LineFilter)
	if !ok {
		t.Fatalf("stage 0 is %T, want *LineFilter", stages[0])
	}
	if got := line.Values[0]; got != "x.*" {
		t.Errorf("line filter regexp = %q, want the raw %q", got, "x.*")
	}
	leaf, ok := stages[1].(*LabelFilterLeaf)
	if !ok {
		t.Fatalf("stage 1 is %T, want *LabelFilterLeaf", stages[1])
	}
	if got := leaf.Value.Str; got != "y.*" {
		t.Errorf("label filter regexp = %q, want the raw %q", got, "y.*")
	}
}

// TestDotNLMatcherValue pins the canonical spelling and its stability. Every
// canonical value must canonicalize to itself, otherwise Parse -> String ->
// Parse would grow nested (?s:) groups.
func TestDotNLMatcherValue(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"dot", ".", "(?s:.)"},
		{"dot plus", ".+", "(?s:.+)"},
		{"dot in concat", "a.b", "(?s:a.b)"},
		{"dot star", "x.*", "(?s:x.*)"},
		{"no dot is unchanged", "prod|staging", "prod|staging"},
		{"already scoped", "(?s:.+)", "(?s:.+)"},
		{"explicit dot off", "(?-s:a.b)", "(?-s:a.b)"},
		{"mixed override", "a.(?-s:b.)", "(?-s:a(?s:.)b.)"},
		{"redundant nesting collapses", "(?s:(?s:.+))", "(?s:.+)"},
		{"no single outer scope", "(?s:foo)|(?:bar)", "foo|bar"},
		{"captures and alternation", "(a).*|(b).", "(?s:(a).*|(b).)"},
		{"named capture", "(?P<x>.+)", "(?s:(?P<x>.+))"},
		{"char class normalizes", "[.]", `\.`},
		{"newline escape", `foo\nbar`, `foo\nbar`},
		{"invalid pattern returns raw", "(", "("},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dotNLMatcherValue(tt.in)
			if got != tt.want {
				t.Fatalf("dotNLMatcherValue(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if again := dotNLMatcherValue(got); again != got {
				t.Errorf("canonical form %q is not stable: re-canonicalized to %q", got, again)
			}
			if tt.in == "(" {
				return
			}
			if _, err := plan.NewMatcher(plan.MatchRegexp, "a", got); err != nil {
				t.Errorf("canonical value %q does not build a matcher: %v", got, err)
			}
		})
	}
}

// TestLabelMatcherValueChecksType pins that only regexp matchers are scoped:
// equality values are data and must stay byte-exact.
func TestLabelMatcherValueChecksType(t *testing.T) {
	for _, typ := range []plan.MatchType{plan.MatchEqual, plan.MatchNotEqual} {
		if got := labelMatcherValue(typ, "a.b\n"); got != "a.b\n" {
			t.Errorf("labelMatcherValue(%s, %q) = %q, want it unchanged", typ, "a.b\n", got)
		}
	}
	for _, typ := range []plan.MatchType{plan.MatchRegexp, plan.MatchNotRegexp} {
		if got := labelMatcherValue(typ, ".+"); got != "(?s:.+)" {
			t.Errorf("labelMatcherValue(%s, %q) = %q, want %q", typ, ".+", got, "(?s:.+)")
		}
	}
}

// TestMatcherValueCarriesDotNLForSQL checks the value alone, compiled with Go's
// default flags, the way a SQL Source renders it (plan.NewMatcher on the other
// side of the contract wraps it in ^(?:...)$ without any flags of its own).
func TestMatcherValueCarriesDotNLForSQL(t *testing.T) {
	tests := []struct {
		q     string
		want  string
		input string
		match bool
	}{
		{`{foo=~"a.b"}`, "(?s:a.b)", "a\nb", true},
		{`{foo=~"a.b"}`, "(?s:a.b)", "axb", true},
		{`{foo=~"a.b"}`, "(?s:a.b)", "xa\nb", false},
		{`{foo=~"a.(?-s:b.)"}`, "(?-s:a(?s:.)b.)", "a\nb.", true},
		{`{foo=~"a.(?-s:b.)"}`, "(?-s:a(?s:.)b.)", "a\nb\n", false},
	}
	for _, tt := range tests {
		sel, err := ParseSelector(tt.q)
		if err != nil {
			t.Fatalf("ParseSelector(%q): %v", tt.q, err)
		}
		m := sel.Matchers[0]
		if m.Value != tt.want {
			t.Errorf("%q matcher value = %q, want %q", tt.q, m.Value, tt.want)
		}
		// Compiling the value without DotNL proves the scope travelled with it.
		re, err := regexp.Compile(`^(?:` + m.Value + `)$`)
		if err != nil {
			t.Fatalf("value %q does not compile: %v", m.Value, err)
		}
		if got := re.MatchString(tt.input); got != tt.match {
			t.Errorf("value %q on %q = %v, want %v", m.Value, tt.input, got, tt.match)
		}
	}
}

// TestSelectorStringDotNLRoundTrip pins the canonical query spellings and that
// they are fixed points. pattern is what dotNLMatcherValue makes of the raw
// value; String must emit it and reparsing must reproduce it.
func TestSelectorStringDotNLRoundTrip(t *testing.T) {
	tests := []struct{ q, want string }{
		{`{foo=~".+"}`, `{foo=~"(?s:.+)"}`},
		{`{foo=~"a.b"}`, `{foo=~"(?s:a.b)"}`},
		{`{foo=~"(?s:.+)"}`, `{foo=~"(?s:.+)"}`},
		{`{foo=~"(?-s:a.b)"}`, `{foo=~"(?-s:a.b)"}`},
		{`{foo=~"a.(?-s:b.)"}`, `{foo=~"(?-s:a(?s:.)b.)"}`},
		{`{foo=~"(a).*|(b)."}`, `{foo=~"(?s:(a).*|(b).)"}`},
		{`{foo=~"(?s:(?s:.+))"}`, `{foo=~"(?s:.+)"}`},
		{`{foo!~"x.*", job="api"}`, `{foo!~"(?s:x.*)", job="api"}`},
	}
	for _, tt := range tests {
		sel, err := ParseSelector(tt.q)
		if err != nil {
			t.Fatalf("ParseSelector(%q): %v", tt.q, err)
		}
		canonical := sel.String()
		if canonical != tt.want {
			t.Errorf("ParseSelector(%q).String() = %q, want %q", tt.q, canonical, tt.want)
		}
		again, err := ParseSelector(canonical)
		if err != nil {
			t.Fatalf("reparse %q: %v", canonical, err)
		}
		if got := again.String(); got != canonical {
			t.Errorf("round trip unstable: %q -> %q -> %q", tt.q, canonical, got)
		}
		if !sameMatchers(sel.Matchers, again.Matchers) {
			t.Errorf("round trip %q changed matchers: %v vs %v", tt.q, sel.Matchers, again.Matchers)
		}
	}
}

// TestFullParseDropKeepDotNLRoundTrip covers the full-expression path: the
// drop/keep matcher values and their String spellings must be stable too.
func TestFullParseDropKeepDotNLRoundTrip(t *testing.T) {
	const q = `{app="api"} | drop foo, bar=~"x.*", baz!~"q." | keep qux`
	const want = `{app="api"} | drop foo, bar=~"(?s:x.*)", baz!~"(?s:q.)" | keep qux`
	e, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	canonical := e.String()
	if canonical != want {
		t.Errorf("Parse(%q).String() = %q, want %q", q, canonical, want)
	}
	again, err := Parse(canonical)
	if err != nil {
		t.Fatalf("reparse %q: %v", canonical, err)
	}
	if got := again.String(); got != canonical {
		t.Errorf("round trip unstable: %q -> %q -> %q", q, canonical, got)
	}
}

// TestMatcherValueTransport rebuilds every matcher from only its Type, Name and
// Value, the representation a SQL Source receives, then selects newline rows
// from plan.MemSource with the rebuilt matchers. If the dot-newline scope lived
// only in the in-process compiled regexp, this would lose it.
func TestMatcherValueTransport(t *testing.T) {
	_, t0 := dotNLFixture()
	tests := []struct {
		name string
		q    string
		want []string
	}{
		{
			name: "scoped positive regexp",
			q:    `{foo=~".+"}`,
			want: []string{"nl only", "nl middle", "nl plus", "ascii", "override dot", "one char", "prefixed"},
		},
		{
			name: "scoped negative regexp",
			q:    `{foo!~"a.b", job="api"}`,
			want: []string{"nl only", "nl plus", "no foo", "override dot", "one char", "equality", "prefixed", "eq literal"},
		},
		{
			name: "inline override",
			q:    `{foo=~"a.(?-s:b.)", job="api"}`,
			want: []string{"override dot"},
		},
		{
			name: "equality is not rewritten",
			q:    `{eq="a.b"}`,
			want: []string{"eq literal"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := New(config.LogQL{}).ParseSelector(tt.q)
			if err != nil {
				t.Fatalf("ParseSelector(%q): %v", tt.q, err)
			}
			rebuilt := make([]plan.Matcher, len(parsed))
			for i, m := range parsed {
				rebuilt[i], err = plan.NewMatcher(m.Type, m.Name, m.Value)
				if err != nil {
					t.Fatalf("rebuild %s%s%q: %v", m.Name, m.Type, m.Value, err)
				}
				if rebuilt[i].Value != m.Value {
					t.Errorf("rebuild changed value %q to %q", m.Value, rebuilt[i].Value)
				}
			}
			src, _ := dotNLFixture()
			sel := plan.Selection{
				Tenant:    tenant.Default,
				Matchers:  rebuilt,
				From:      t0,
				Through:   t0.Add(time.Hour),
				Direction: plan.Forward,
			}
			var lines []string
			for r, err := range src.Select(t.Context(), sel) {
				if err != nil {
					t.Fatalf("Select(%q): %v", tt.q, err)
				}
				lines = append(lines, r.Line)
			}
			if !slices.Equal(lines, tt.want) {
				t.Errorf("rebuilt select %q = %q, want %q", tt.q, lines, tt.want)
			}
		})
	}
}
