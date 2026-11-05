package logql

import (
	"errors"
	"testing"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// FuzzParse checks three invariants on arbitrary input: parsing never
// panics (the seeds include deep nesting, malformed UTF-8 and truncated
// queries), a successful parse round-trips through its canonical String
// form, and every error classifies as either a syntax error
// (plan.ErrBadQuery) or a deferred family (plan.ErrUnsupported). Run a
// short bounded fuzz on Boat:
//
//	go test ./internal/logql/ -run '^$' -fuzz '^FuzzParse$' -fuzztime 30s
func FuzzParse(f *testing.F) {
	seeds := []string{
		// Supported families.
		`{app="api"}`,
		`{app="api"} |= "err" or "fail" != "warn" |~ "e.*" !~ "debug"`,
		`{app="api"} | json status="response.status", foo`,
		`{app="api"} | logfmt --strict --keep-empty a="b", c="d"`,
		`{app="api"} | regexp "(?P<x>a)(b)" | pattern "<m> <u>"`,
		`{app="api"} | line_format "{{.level}}" | label_format a=b, c="{{.d}}"`,
		`{app="api"} | drop foo, bar=~"x.*", __error__ | keep foo`,
		`{app="api"} | (a="1" or b="2") and c > 500ms, d >= 1KiB, e == 0.5`,
		`sum by (a) (topk(3, count_over_time({app="api"} | drop __error__ [5m]) ))`,
		`quantile_over_time(0.5, {app="api"} | unwrap duration(latency) | ok="y" [5m]) offset 1h`,
		`sum_over_time(({app="api"} | json | unwrap x)[5m] offset -30m)`,
		`2 ^ -3 * (1 + 2) / 0 % 5 > bool 10`,
		`count_over_time({app="api"} [5m]) > bool on(a) group_left(b) count_over_time({app="api"} [5m])`,
		`rate_counter({app="api"} [5m] | unwrap x)`,
		`variants(rate({a="1"} [5m])) of ({a="1"} [1m])`,
		`label_replace(rate({a="1"} [5m]), "d", "r", "s", "re.*")`,
		`{app="api"} |> "<m>"`,
		`{app="api"} | unpack | decolorize`,
		// Malformed and truncated forms.
		`{`,
		`{a=}`,
		`{a="1",}`,
		`{a="1"} |`,
		`{a="1"} |= or`,
		`{a="1"} | json --strict`,
		`{a="1"} | logfmt --`,
		`{a="1"} | regexp`,
		`{a="1"} | label_format a=`,
		`{a="1"} | unwrap`,
		`sum(`,
		`sum by (`,
		`topk(`,
		`count_over_time({a="1"}`,
		`count_over_time(({a="1"}`,
		`quantile_over_time(`,
		`1 +`,
		`1 + on(`,
		`vector(`,
		`label_replace(`,
		`variants(`,
		`{a="1"} [`,
		`{a="1"} [5m] offset`,
		`{a="1"} [5m] offset -`,
		`{a="1"} [1.5s]`,
		`{a="1"} [500ns]`,
		// Nesting pressure and malformed UTF-8.
		`((((((((((1))))))))))`,
		`sum(sum(sum(sum(1))))`,
		`{a="1"} | ((((((((((a="1"))))))))))`,
		`((((((((((({a="1"} | json [5m]))))))))))`,
		"{a=\"\xff\"}",
		"{\xff=\xff}",
		"{a=`\xff`}",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		e, err := Parse(q)
		if err != nil {
			if !errors.Is(err, plan.ErrBadQuery) && !errors.Is(err, plan.ErrUnsupported) {
				t.Errorf("Parse(%q) err = %v, want ErrBadQuery or ErrUnsupported", q, err)
			}
			return
		}
		canonical := e.String()
		e2, err := Parse(canonical)
		if err != nil {
			t.Fatalf("canonical form %q of %q does not reparse: %v", canonical, q, err)
		}
		if again := e2.String(); again != canonical {
			t.Errorf("round trip unstable: %q -> %q -> %q", q, canonical, again)
		}
	})
}
