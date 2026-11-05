package pipeline

import (
	"regexp"
	"testing"
)

// TestExpandLenMatchesGo is a differential table: expandLen must be exactly
// len(re.ExpandString(...)) for the same match, including Unicode reference
// names (unknown refs expand to nothing), malformed braces, $$ and lone $.
func TestExpandLenMatchesGo(t *testing.T) {
	const src = `a1 b22 c333 user=bob x=1 z é`
	cases := []struct {
		pattern  string
		template string
	}{
		{`(\w+)=(\w+)`, "$1=***"},
		{`user=(\w+)`, "${1}-hidden"},
		{`user=(?P<name>\w+)`, "$name-hidden"},
		{`a`, "$$"},
		{`z`, "$"},
		{`b`, "$nope"},
		{`c`, "${nope}"},
		{`(\d+)`, "$1$1$1"},
		{`x`, "$0"},
		{`(?P<a>a)(?P<b>b)?`, "$a$b"},
		{`é`, "$é"},   // Unicode name: unknown ref, expands empty
		{`é`, "$éx"},  // Unicode name run
		{`é`, "${é}"}, // braced Unicode name
		{`a`, "${1"},  // malformed brace: literal $
		{`a`, "${}x"}, // empty braced name
		{`a`, "$"},
		{`a`, "$$1"},
		{`a`, "x$"},
		{`a`, "$1x"},
		{`a`, "${1}x"},
		{`b22`, "$0$0"},
		{`user=(?P<n>\w+)`, "$n$n$n"},
		{`z`, "$1e3"},
		{`a`, "$é€"},
	}
	for _, tc := range cases {
		t.Run(tc.pattern+" ~ "+tc.template, func(t *testing.T) {
			re := regexp.MustCompile(tc.pattern)
			m := re.FindStringSubmatchIndex(src)
			if m == nil {
				t.Fatalf("pattern does not match src")
			}
			want := len(re.ExpandString(nil, tc.template, src, m))
			got := int(expandLen(re, tc.template, src, m))
			if got != want {
				t.Fatalf("expandLen = %d, len(ExpandString) = %d", got, want)
			}
		})
	}
}
