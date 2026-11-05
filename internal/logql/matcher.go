// Direct label matcher regexps follow Loki/Prometheus's model/labels regexp
// flags: pinned Loki 3.7.9 parses them with syntax.Perl|syntax.DotNL, so an
// unescaped . also matches a newline inside a label value. plan.NewMatcher
// compiles with Go's default flags, so the parser canonicalizes the pattern
// with the same DotNL default before building the matcher. The canonical
// spelling is what a plan.Matcher carries to SQL Sources; the compiled RE2 is
// only the in-process reference.

package logql

import (
	"regexp/syntax"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// dotNLMatcherValue returns the canonical spelling of a direct label matcher
// regexp, parsing it with syntax.DotNL as Loki does. syntax.Regexp.String
// emits the flags that are not implied by the root: an enabled (?s:...) around
// a dot that must match a newline and an explicit (?-s:...) where the query
// turns it back off, collapsing redundant nesting. Parsing that spelling
// again yields the same bytes, so canonical query Strings are stable.
//
// On a parse error the pattern is returned unchanged: plan.NewMatcher reports
// the same invalid-regexp error (wrapping plan.ErrBadQuery) as before.
func dotNLMatcherValue(pattern string) string {
	re, err := syntax.Parse(pattern, syntax.Perl|syntax.DotNL)
	if err != nil {
		return pattern
	}
	return re.String()
}

// labelMatcherValue returns the value a direct label matcher is built with:
// regexp matchers get Loki's dot-newline semantics, while = and != stay
// byte-exact.
func labelMatcherValue(typ plan.MatchType, value string) string {
	switch typ {
	case plan.MatchRegexp, plan.MatchNotRegexp:
		return dotNLMatcherValue(value)
	}
	return value
}
