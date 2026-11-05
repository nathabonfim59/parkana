package logql

import (
	"errors"
	"testing"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// FuzzParseSelector checks two invariants on arbitrary input: parsing never
// panics (the seeds include nesting and malformed UTF-8), and any successful
// parse round-trips through its canonical String form. Selector-level errors
// are always syntax errors (plan.ErrBadQuery), never plan.ErrUnsupported.
// Run a short bounded fuzz on Boat:
//
//	go test ./internal/logql/ -run '^$' -fuzz '^FuzzParseSelector$' -fuzztime 30s
func FuzzParseSelector(f *testing.F) {
	seeds := []string{
		`{app="api"}`,
		`{app="api", env=~"prod|staging", code!="500"}`,
		`{a="", b="x"}`,
		`{a="1", a="2", a="3"}`,
		"{app=`raw string`}",
		`{msg="he said \"hi\"\n"}`,
		`{}`,
		`{`,
		`{a=}`,
		`{a=~"("}`,
		`{a="1"} |= "x"`,
		`{a="1"} {b="2"} {c="3"}`,
		"{a=\"\xff\"}",
		"{\xff=\xff}",
		"{a=`\xff`}",
		`{a="¹²³"}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		sel, err := ParseSelector(q)
		if err != nil {
			if errors.Is(err, plan.ErrUnsupported) {
				t.Errorf("ParseSelector(%q) = %v, selector errors are ErrBadQuery", q, err)
			}
			return
		}
		canonical := sel.String()
		sel2, err := ParseSelector(canonical)
		if err != nil {
			t.Fatalf("canonical form %q of %q does not reparse: %v", canonical, q, err)
		}
		if again := sel2.String(); again != canonical {
			t.Errorf("round trip unstable: %q -> %q -> %q", q, canonical, again)
		}
	})
}
