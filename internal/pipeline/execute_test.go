package pipeline_test

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/logs"
	. "github.com/nathabonfim59/parkana/internal/pipeline"
)

// apply runs one entry and fails the test on an unexpected error.
func apply(t *testing.T, c *Compiled, in Entry) (Entry, Outcome) {
	t.Helper()
	out, oc, err := c.Apply(t.Context(), "app", in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return out, oc
}

// compileRegexp compiles a pattern for comparing against Go's own
// ReplaceAllString.
func compileRegexp(t *testing.T, pattern string) *regexp.Regexp {
	t.Helper()
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return re
}

func TestExecParseJSON(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON}},
	})
	tests := []struct {
		name string
		line string
		want map[string]string
		stop bool // default keep fires
	}{
		{"scalars", `{"a":"x","i":42,"t":true,"n":null}`, map[string]string{"a": "x", "i": "42", "t": "true", "n": "null"}, false},
		{"nested flatten", `{"o":{"z":1,"a":2}}`, map[string]string{"o_z": "1", "o_a": "2"}, false},
		{"sanitized names", `{"user.id":7,"http-status":"200","@timestamp":"t"}`, map[string]string{"user_id": "7", "http_status": "200", "_timestamp": "t"}, false},
		{"sanitized collision later wins", `{"user.id":1,"user_id":2}`, map[string]string{"user_id": "2"}, false},
		{"reserved prefix rewritten", `{"__error__":"x"}`, map[string]string{"extracted__error__": "x"}, false},
		{"protected collision interleaving", `{"__x":"first","extracted__x":"middle","..x":"last"}`, map[string]string{"extracted__x": "last"}, false},
		{"overwrite", `{"a":"1"}`, map[string]string{"a": "1"}, false},
		{"array top", `[1]`, nil, true},
		{"scalar top", `42`, nil, true},
		{"null top", `null`, nil, true},
		{"trailing", `{"a":1} x`, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, oc := apply(t, c, Entry{Line: tc.line, Meta: logs.New(logs.Label{Name: "pre", Value: "yes"})})
			if tc.stop {
				if oc != OutcomeStop {
					t.Fatalf("outcome = %v, want stop", oc)
				}
				if got, _ := out.Meta.Get(ErrField); got != "parse_json" {
					t.Errorf("pipeline_error = %q", got)
				}
				if got, _ := out.Meta.Get(ErrDetailsField); got == "" {
					t.Errorf("empty details")
				}
				// Pre-step state restored: pre-existing metadata intact.
				if got, _ := out.Meta.Get("pre"); got != "yes" {
					t.Errorf("pre = %q, want restored", got)
				}
				return
			}
			if oc != OutcomeKeep {
				t.Fatalf("outcome = %v", oc)
			}
			for k, v := range tc.want {
				if got, _ := out.Meta.Get(k); got != v {
					t.Errorf("meta[%q] = %q, want %q", k, got, v)
				}
			}
		})
	}

	// from a metadata field.
	cf := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, From: "raw"}},
	})
	out, oc := apply(t, cf, Entry{Line: "notjson", Meta: logs.New(logs.Label{Name: "raw", Value: `{"a":1}`})})
	if oc != OutcomeKeep || out.Line != "notjson" {
		t.Fatalf("from-meta: %v %q", oc, out.Line)
	}
	if got, _ := out.Meta.Get("a"); got != "1" {
		t.Errorf("a = %q", got)
	}
	// missing source field is a data error
	_, oc = apply(t, cf, Entry{Line: "x"})
	if oc != OutcomeStop {
		t.Errorf("missing source: outcome = %v, want stop", oc)
	}

	// dynamic reserved key: rewritten with the safe prefix, never written __.
	cdunder := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON}},
	})
	out2, oc2 := apply(t, cdunder, Entry{Line: `{"ok":"1","__proto__":"x"}`})
	if oc2 != OutcomeKeep {
		t.Fatalf("dunder key: outcome = %v", oc2)
	}
	if got, _ := out2.Meta.Get("ok"); got != "1" {
		t.Errorf("ok = %q", got)
	}
	if got, _ := out2.Meta.Get("extracted__proto__"); got != "x" {
		t.Errorf("extracted__proto__ = %q", got)
	}
	if _, ok := out2.Meta.Get("__proto__"); ok {
		t.Errorf("reserved __proto__ was written")
	}

	// dotted names are sanitized, not rejected.
	out3, oc3 := apply(t, cdunder, Entry{Line: `{"a.b":1}`})
	if oc3 != OutcomeKeep {
		t.Fatalf("dotted name: outcome = %v", oc3)
	}
	if got, _ := out3.Meta.Get("a_b"); got != "1" {
		t.Errorf("a_b = %q", got)
	}

	// metadata budget: tiny MetaBytes, everything rolled back.
	small := CompileOptions{StreamLabels: []string{"stream"}, MetaBytes: 16}
	cb, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON}},
	}, small)
	if err != nil {
		t.Fatal(err)
	}
	out, oc = applyOpts(t, cb, Entry{Line: `{"aaaaaaaa":"1111111111"}`})
	if oc != OutcomeStop {
		t.Fatalf("budget: outcome = %v", oc)
	}
	if len(out.Meta) != 2 { // pipeline_error + details only
		t.Errorf("meta after rollback = %v", out.Meta)
	}
}

// applyOpts is apply for pipelines compiled with non-default bounds.
func applyOpts(t *testing.T, c *Compiled, in Entry) (Entry, Outcome) {
	t.Helper()
	return apply(t, c, in)
}

func TestExecParseLogfmt(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseLogfmt}},
	})
	tests := []struct {
		name string
		line string
		want map[string]string
		stop bool
	}{
		{"pairs", "a=1 b=two", map[string]string{"a": "1", "b": "two"}, false},
		{"quoted", `a="x y"`, map[string]string{"a": "x y"}, false},
		{"crlf", "a=1\r\nb=2", map[string]string{"a": "1", "b": "2"}, false},
		{"duplicate later wins", "a=1 a=2", map[string]string{"a": "2"}, false},
		{"empty", "", map[string]string{}, false},
		{"bare word", "novalue", nil, true},
		{"quote run-on", `a="x"b=2`, nil, true},
		{"dotted key sanitizes", "a.b=1", map[string]string{"a_b": "1"}, false},
		{"sanitized collision later wins", "a.b=1 a_b=2", map[string]string{"a_b": "2"}, false},
		{"leading digit", "1a=x", map[string]string{"_1a": "x"}, false},
		{"reserved prefix rewritten", "__x=1", map[string]string{"extracted__x": "1"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, oc := apply(t, c, Entry{Line: tc.line})
			if tc.stop {
				if oc != OutcomeStop {
					t.Fatalf("outcome = %v, want stop", oc)
				}
				return
			}
			if oc != OutcomeKeep {
				t.Fatalf("outcome = %v", oc)
			}
			for k, v := range tc.want {
				if got, _ := out.Meta.Get(k); got != v {
					t.Errorf("meta[%q] = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestExecRegexExtract(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRegexExtract, Pattern: `^(?P<lvl>\w+) (?P<rest>.*)$`}},
	})
	out, oc := apply(t, c, Entry{Line: "info the message"})
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	if got, _ := out.Meta.Get("lvl"); got != "info" {
		t.Errorf("lvl = %q", got)
	}
	// Only the first match extracts.
	cf := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRegexExtract, Pattern: `(?P<lvl>\w+)`}},
	})
	out, _ = apply(t, cf, Entry{Line: "first second"})
	if got, _ := out.Meta.Get("lvl"); got != "first" {
		t.Errorf("lvl = %q, want first match", got)
	}
	// No match is a data error.
	_, oc = apply(t, c, Entry{Line: ""})
	if oc != OutcomeStop {
		t.Errorf("no match: outcome = %v", oc)
	}
	// Partial groups merge; unmatched groups skipped.
	cp := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRegexExtract, Pattern: `^(?P<a>x)?(?P<b>y)?y`}},
	})
	out, oc = apply(t, cp, Entry{Line: "y"})
	if oc != OutcomeKeep {
		t.Fatalf("partial: %v", oc)
	}
	if _, ok := out.Meta.Get("a"); ok {
		t.Errorf("unmatched group a merged")
	}

	// Capture names are sanitized: a leading digit gets '_' and a reserved
	// __ prefix is rewritten; neither is a compile error.
	cs := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRegexExtract, Pattern: `^(?P<1lvl>\w+) (?P<__rest>.*)$`}},
	})
	out, oc = apply(t, cs, Entry{Line: "info the message"})
	if oc != OutcomeKeep {
		t.Fatalf("sanitized captures: %v", oc)
	}
	if got, _ := out.Meta.Get("_1lvl"); got != "info" {
		t.Errorf("_1lvl = %q", got)
	}
	if got, _ := out.Meta.Get("extracted__rest"); got != "the message" {
		t.Errorf("extracted__rest = %q", got)
	}
	if _, ok := out.Meta.Get("__rest"); ok {
		t.Errorf("reserved __rest was written")
	}

	// Duplicate capture names keep capture order: the later group wins.
	cd := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRegexExtract, Pattern: `(?P<x>a)(?P<x>b)`}},
	})
	out, oc = apply(t, cd, Entry{Line: "ab"})
	if oc != OutcomeKeep {
		t.Fatalf("duplicate captures: %v", oc)
	}
	if got, _ := out.Meta.Get("x"); got != "b" {
		t.Errorf("x = %q, want later capture b", got)
	}
}

func TestExecRename(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRename, From: "lvl", To: "level"}},
	})
	out, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "lvl", Value: "info"}, logs.Label{Name: "keep", Value: "1"})})
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	if got, _ := out.Meta.Get("level"); got != "info" {
		t.Errorf("level = %q", got)
	}
	if _, ok := out.Meta.Get("lvl"); ok {
		t.Errorf("lvl still present")
	}
	if got, _ := out.Meta.Get("keep"); got != "1" {
		t.Errorf("keep = %q", got)
	}
	// missing source: no-op
	_, oc = apply(t, c, Entry{Line: "x"})
	if oc != OutcomeKeep {
		t.Errorf("missing source: %v", oc)
	}
}

// TestRenameSeedRestore: deleting an expression-referenced key must leave
// the seed (empty string) for later expressions.
func TestRenameSeedRestore(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepRename, From: "lvl", To: "level"},
			{Type: StepDropIf, Expr: `meta.lvl == ""`}, // true only via the restored seed
		},
	})
	_, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "lvl", Value: "info"})})
	if oc != OutcomeDrop {
		t.Fatalf("outcome = %v, want drop (seed restored to \"\")", oc)
	}
	// And the restored seed must not leak into the output.
	c2 := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepRename, From: "lvl", To: "level"},
			{Type: StepDropIf, Expr: `meta.lvl != ""`, When: `1 == 2`}, // never drops
		},
	})
	out, oc := apply(t, c2, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "lvl", Value: "info"})})
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	if _, ok := out.Meta.Get("lvl"); ok {
		t.Errorf("seed leaked into output meta: %v", out.Meta)
	}
}

func TestExecRedact(t *testing.T) {
	base := Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `secret-\d+`}},
	}
	c := mustCompile(t, base)
	out, oc := apply(t, c, Entry{Line: "a secret-123 b secret-9 c"})
	if oc != OutcomeKeep || out.Line != "a *** b *** c" {
		t.Fatalf("default replace: %q %v", out.Line, oc)
	}

	// Explicit empty replacement deletes matches.
	del := base
	del.Steps = []Step{{Type: StepRedact, Pattern: `secret-\d+`, Replace: new("")}}
	cd := mustCompile(t, del)
	out, oc = apply(t, cd, Entry{Line: "a secret-123 b"})
	if oc != OutcomeKeep || out.Line != "a  b" {
		t.Fatalf("empty replace deletes: %q %v", out.Line, oc)
	}

	// Group references.
	grp := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `user=(\w+)`, Replace: new("user=$1-hidden")}},
	})
	out, _ = apply(t, grp, Entry{Line: "u user=bob w"})
	if out.Line != "u user=bob-hidden w" {
		t.Fatalf("group ref: %q", out.Line)
	}

	// Metadata target; absent target is a no-op; no match is a no-op.
	metaR := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `tok-\d+`, Target: "auth"}},
	})
	out, oc = apply(t, metaR, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "auth", Value: "tok-42"})})
	if oc != OutcomeKeep {
		t.Fatalf("meta redact: %v", oc)
	}
	if got, _ := out.Meta.Get("auth"); got != "***" {
		t.Errorf("auth = %q", got)
	}
	if out, oc = apply(t, metaR, Entry{Line: "x"}); oc != OutcomeKeep || len(out.Meta) != 0 {
		t.Errorf("absent target: %v %v", out.Meta, oc)
	}
	if out, oc = apply(t, metaR, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "auth", Value: "clean"})}); oc != OutcomeKeep {
		t.Errorf("no match: %v", oc)
	} else if got, _ := out.Meta.Get("auth"); got != "clean" {
		t.Errorf("no-match target changed: %q", got)
	}

	// Empty-match patterns are legal and bounded: `s*` matches everywhere,
	// default replace expands the line.
	emp := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `z*`, Replace: new("_")}},
	})
	out, oc = apply(t, emp, Entry{Line: "ab"})
	if oc != OutcomeKeep || out.Line != "_a_b_" {
		// Go: ReplaceAllString("ab", "z*", "_") == "_a_b_"
		t.Fatalf("empty-match pattern: %q %v", out.Line, oc)
	}

	// Expansion bomb with small bounds fails before materializing and
	// restores the line.
	bombOpts := CompileOptions{StreamLabels: []string{"stream"}, LineBytes: 32}
	bomb, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `.`, Replace: new(strings.Repeat("x", 64))}},
	}, bombOpts)
	if err != nil {
		t.Fatal(err)
	}
	out, oc = applyOpts(t, bomb, Entry{Line: "0123456789"})
	if oc != OutcomeStop {
		t.Fatalf("bomb outcome = %v", oc)
	}
	if out.Line != "0123456789" {
		t.Errorf("line not restored: %q", out.Line)
	}
	if got, _ := out.Meta.Get(ErrField); got != "redact" {
		t.Errorf("pipeline_error = %q", got)
	}
}

// TestRedactSizerMatchesGo pins the exact precompute against Go's own
// ReplaceAllString, including tricky templates.
func TestRedactSizerMatchesGo(t *testing.T) {
	const src = `a1 b22 c333 user=bob x=1 z`
	reTpl := []struct {
		pattern string
		tpl     string
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
	}
	for _, tc := range reTpl {
		t.Run(tc.pattern+" "+tc.tpl, func(t *testing.T) {
			c, err := Compile(Definition{
				Name: "p", Stage: StageIngest, Selector: "*",
				Steps: []Step{{Type: StepRedact, Pattern: tc.pattern, Replace: new(tc.tpl)}},
			}, CompileOptions{StreamLabels: []string{"stream"}, LineBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			out, oc := apply(t, c, Entry{Line: src})
			if oc != OutcomeKeep {
				t.Fatalf("outcome = %v (precompute/actual divergence?)", oc)
			}
			if want := compileRegexp(t, tc.pattern).ReplaceAllString(src, tc.tpl); out.Line != want {
				t.Fatalf("line = %q, want %q", out.Line, want)
			}
		})
	}
}

// TestRedactMatchBudget: hundreds of matches must fail the index budget
// cheaply, before any size math or materialization.
func TestRedactMatchBudget(t *testing.T) {
	opts := CompileOptions{StreamLabels: []string{"stream"}, LineBytes: 8 << 20}
	c, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		// Two groups: 6 ints per match, so the budget allows ~174k matches.
		Steps: []Step{{Type: StepRedact, Pattern: `(a)(b)`, Replace: new("z")}},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	// 700k matches of "ab" exceed MaxMatchIndexes (would allocate ~16 MB of
	// index slice before the old unbounded FindAll... now capped first).
	big := strings.Repeat("ab", 700_000/2)
	_, oc := applyOpts(t, c, Entry{Line: big})
	if oc != OutcomeStop {
		t.Fatalf("outcome = %v, want stop (match budget)", oc)
	}
	// Within the budget it works.
	small := strings.Repeat("ab", 1000)
	out, oc := applyOpts(t, c, Entry{Line: small})
	if oc != OutcomeKeep || out.Line != strings.Repeat("z", 1000) {
		t.Fatalf("within budget: %v %q...", oc, out.Line[:8])
	}
}

func TestExecDropIfWhen(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepDropIf, Expr: `int(meta.status) >= 500`, When: `meta.status != ""`},
		},
	})
	if _, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "status", Value: "503"})}); oc != OutcomeDrop {
		t.Errorf("drop 503: %v", oc)
	}
	if _, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "status", Value: "200"})}); oc != OutcomeKeep {
		t.Errorf("keep 200: %v", oc)
	}
	if _, oc := apply(t, c, Entry{Line: "x"}); oc != OutcomeKeep {
		t.Errorf("when false skips: %v", oc)
	}
	// Runtime conversion error: default keep marks and stops.
	out, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "status", Value: "abc"})})
	if oc != OutcomeStop {
		t.Fatalf("bad int: %v", oc)
	}
	if got, _ := out.Meta.Get(ErrField); got != "drop_if" {
		t.Errorf("pipeline_error = %q", got)
	}
	// when itself failing uses the same policy.
	cw := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, When: `int(meta.bad) > 0`}},
	})
	if _, oc := apply(t, cw, Entry{Line: `{}`, Meta: logs.New(logs.Label{Name: "bad", Value: "x"})}); oc != OutcomeStop {
		t.Errorf("when error: %v", oc)
	}
}

func TestExecOnErrorPolicies(t *testing.T) {
	mk := func(policy OnError) *Compiled {
		return mustCompile(t, Definition{
			Name: "p", Stage: StageIngest, Selector: "*",
			Steps: []Step{
				{Type: StepParseJSON, OnError: policy},
				{Type: StepRename, From: "a", To: "b"}, // must not run after an error
			},
		})
	}
	in := Entry{Line: "notjson", Meta: logs.New(logs.Label{Name: "a", Value: "1"})}

	out, oc := apply(t, mk(OnErrorKeep), in)
	if oc != OutcomeStop {
		t.Fatalf("keep: %v", oc)
	}
	if _, ok := out.Meta.Get("b"); ok {
		t.Errorf("later step ran after keep-stop")
	}
	if got, _ := out.Meta.Get("a"); got != "1" {
		t.Errorf("input metadata not restored: %v", out.Meta)
	}

	if _, oc := apply(t, mk(OnErrorDrop), in); oc != OutcomeDrop {
		t.Fatalf("drop: %v", oc)
	}

	c := mk(OnErrorReject)
	_, oc, err := c.Apply(t.Context(), "app", in)
	if oc != OutcomeKeep || err == nil {
		t.Fatalf("reject: %v %v", oc, err)
	}
	re, ok := errors.AsType[*RejectError](err)
	if !ok {
		t.Fatalf("error type %T", err)
	}
	if re.Pipeline != "p" || re.Step != "parse_json" || re.Details == "" {
		t.Errorf("RejectError = %+v", *re)
	}
}

// TestKeepSkipsRemainingSteps proves the keep policy stops the chain inside
// one pipeline: step 3 runs only when step 2 succeeds.
func TestKeepSkipsRemainingSteps(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepParseLogfmt, OnError: OnErrorDrop},
			{Type: StepParseJSON, OnError: OnErrorKeep}, // fails on logfmt lines
			{Type: StepRedact, Pattern: "never"},        // must not run
		},
	})
	_, oc := apply(t, c, Entry{Line: "a=1"})
	if oc != OutcomeStop {
		t.Fatalf("outcome = %v", oc)
	}
}

func TestExecPromote(t *testing.T) {
	mk := func(step Step) *Compiled {
		return mustCompile(t, Definition{
			Name: "p", Stage: StageIngest, Selector: "*",
			Steps: []Step{step},
		})
	}
	// Literal source.
	out, oc := apply(t, mk(Step{Type: StepPromote, From: "svc", To: "service"}), Entry{
		Line: "x", Meta: logs.New(logs.Label{Name: "svc", Value: "api"}), Labels: logs.New(logs.Label{Name: "stream", Value: "app"}),
	})
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	if got, _ := out.Labels.Get("service"); got != "api" {
		t.Errorf("service = %q", got)
	}
	// Expression source.
	out, oc = apply(t, mk(Step{Type: StepPromote, To: "service", Value: `upper(meta.svc)`}), Entry{
		Line: "x", Meta: logs.New(logs.Label{Name: "svc", Value: "api"}),
	})
	if svc, _ := out.Labels.Get("service"); oc != OutcomeKeep || svc != "API" {
		t.Errorf("expr promote: %v %q", oc, out.Labels)
	}
	// Missing source is a no-op; empty value copies "".
	out, oc = apply(t, mk(Step{Type: StepPromote, From: "svc", To: "service"}), Entry{
		Line: "x", Labels: logs.New(logs.Label{Name: "service", Value: "old"}),
	})
	if oc != OutcomeKeep {
		t.Fatalf("missing source: %v", oc)
	}
	if got, _ := out.Labels.Get("service"); got != "old" {
		t.Errorf("missing source changed labels: %q", got)
	}
	out, oc = apply(t, mk(Step{Type: StepPromote, From: "svc", To: "service"}), Entry{
		Line: "x", Meta: logs.New(logs.Label{Name: "svc", Value: ""}), Labels: logs.New(logs.Label{Name: "service", Value: "old"}),
	})
	if oc != OutcomeKeep {
		t.Fatalf("empty promote: %v", oc)
	}
	if got, ok := out.Labels.Get("service"); !ok || got != "" {
		t.Errorf("empty promote = %q,%v (label must exist with \"\")", got, ok)
	}
	// Value bound.
	large := CompileOptions{StreamLabels: []string{"stream"}, LabelValueBytes: 4}
	cb, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepPromote, From: "svc", To: "service"}},
	}, large)
	if err != nil {
		t.Fatal(err)
	}
	if _, oc := applyOpts(t, cb, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "svc", Value: "abcdefgh"})}); oc != OutcomeStop {
		t.Errorf("value bound: %v", oc)
	}
}

// TestPromoteLiveCaseCollision: promoting "app" onto a live "App" label is a
// data error, not a silent second label.
func TestPromoteLiveCaseCollision(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepPromote, From: "svc", To: "app"}},
	})
	_, oc := apply(t, c, Entry{
		Line:   "x",
		Meta:   logs.New(logs.Label{Name: "svc", Value: "v"}),
		Labels: logs.New(logs.Label{Name: "App", Value: "live"}),
	})
	if oc != OutcomeStop {
		t.Fatalf("outcome = %v, want stop (case collision with live label)", oc)
	}
}

func TestExecSetStream(t *testing.T) {
	mk := func(step Step) *Compiled {
		return mustCompile(t, Definition{
			Name: "p", Stage: StageIngest, Selector: "*",
			Steps: []Step{step},
		})
	}
	base := Entry{Line: "x", Labels: logs.New(logs.Label{Name: "stream", Value: "app"}, logs.Label{Name: "org", Value: "acme"})}

	out, oc := apply(t, mk(Step{Type: StepSetStream, Value: `"Pay/Logs"`}), base)
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	if got, _ := out.Labels.Get("stream"); got != "pay_logs" {
		t.Errorf("stream = %q, want pay_logs", got)
	}
	if got, _ := out.Labels.Get("org"); got != "acme" {
		t.Errorf("org = %q (untouched)", got)
	}

	// Empty value routes to default, never removing the label: org cannot
	// take over even though it is the second routing key.
	out, oc = apply(t, mk(Step{Type: StepSetStream, Value: `""`}), base)
	if oc != OutcomeKeep {
		t.Fatalf("empty: %v", oc)
	}
	if got, _ := out.Labels.Get("stream"); got != logs.DefaultStream {
		t.Errorf("stream = %q, want %q", got, logs.DefaultStream)
	}
	// With stream removed entirely by a prior empty promote... still default:
	// DatasetOf of the ORIGINAL labels is what callers use; the label itself
	// is rewritten to default.
	out, oc = apply(t, mk(Step{Type: StepSetStream, From: "route"}), base)
	if oc != OutcomeKeep {
		t.Fatalf("missing source no-op: %v", oc)
	}
	if got, _ := out.Labels.Get("stream"); got != "app" {
		t.Errorf("no-op changed stream: %q", got)
	}
}

// TestDatasetParameter: expressions see the ORIGINAL dataset even after
// set_stream rewrote the routing label.
func TestDatasetParameter(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepSetStream, Value: `"elsewhere"`},
			{Type: StepDropIf, Expr: `stream != "app"`, OnError: OnErrorDrop},
		},
	})
	// If stream were re-derived after set_stream, this would drop.
	if _, oc := apply(t, c, Entry{Line: "x", Labels: logs.New(logs.Label{Name: "stream", Value: "app"})}); oc != OutcomeKeep {
		t.Fatalf("outcome = %v, want keep (original dataset)", oc)
	}
	if got := c.DatasetOf(logs.New(logs.Label{Name: "stream", Value: "App"})); got != "app" {
		t.Errorf("DatasetOf = %q, want app (sanitized)", got)
	}
	// The second routing key takes over when the first is absent.
	if got := c.DatasetOf(logs.New(logs.Label{Name: "org", Value: "acme"})); got != "acme" {
		t.Errorf("DatasetOf second key = %q", got)
	}
	if got := c.DatasetOf(logs.New(logs.Label{Name: "other", Value: "x"})); got != logs.DefaultStream {
		t.Errorf("DatasetOf fallback = %q", got)
	}
}

func TestExecReadStage(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "r", Stage: StageRead, Selector: "*",
		Steps: []Step{
			{Type: StepParseJSON},
			{Type: StepRedact, Pattern: `secret-\d+`},
			{Type: StepDropIf, Expr: `meta.debug == "true"`},
		},
	})
	labels := logs.New(logs.Label{Name: "stream", Value: "app"}, logs.Label{Name: "job", Value: "j"})
	out, oc := apply(t, c, Entry{Line: `{"debug":"true"}`, Labels: labels})
	if oc != OutcomeDrop {
		t.Fatalf("read drop: %v", oc)
	}
	out, oc = apply(t, c, Entry{Line: `{"msg":"plain secret-1"}`, Labels: labels})
	if oc != OutcomeKeep {
		t.Fatalf("read keep: %v", oc)
	}
	if out.Line != `{"msg":"plain ***"}` {
		t.Errorf("read redact: %q", out.Line)
	}
	// Labels preserved exactly.
	if out.Labels.String() != labels.String() || len(out.Labels) != len(labels) {
		t.Errorf("read labels changed: %v", out.Labels)
	}
	// Default keep never fails the query.
	out, oc = apply(t, c, Entry{Line: "not-json", Labels: labels})
	if oc != OutcomeStop {
		t.Fatalf("read keep: %v", oc)
	}
	if got, _ := out.Meta.Get(ErrField); got != "parse_json" {
		t.Errorf("read marker = %q", got)
	}
	// A redact-only read pipeline works on non-JSON lines.
	redactOnly := mustCompile(t, Definition{
		Name: "r2", Stage: StageRead, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: `secret-\d+`}},
	})
	out, oc = apply(t, redactOnly, Entry{Line: "plain secret-1", Labels: labels})
	if oc != OutcomeKeep || out.Line != "plain ***" {
		t.Fatalf("read redact-only: %q %v", out.Line, oc)
	}
	// Explicit read reject surfaces.
	cr := mustCompile(t, Definition{
		Name: "r", Stage: StageRead, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, OnError: OnErrorReject}},
	})
	_, _, err := cr.Apply(t.Context(), "app", Entry{Line: "bad", Labels: labels})
	if _, ok := errors.AsType[*RejectError](err); !ok {
		t.Errorf("read reject: %v", err)
	}
}

// TestImmutability: Apply never touches the input, its arrays or its
// strings — while the steps really do fire: rename moves a, redact rewrites
// the line (so the JSON still parses), promote reads the renamed field.
func TestImmutability(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepParseJSON},
			{Type: StepRename, From: "a", To: "b"},
			{Type: StepRedact, Pattern: `val\w+`},
			{Type: StepPromote, From: "b", To: "promoted"},
			{Type: StepSetStream, Value: `"z"`},
		},
	})
	in := Entry{
		Line:   `{"a":"value"}`,
		Labels: logs.New(logs.Label{Name: "stream", Value: "app"}),
		Meta:   logs.New(logs.Label{Name: "m", Value: "1"}),
	}
	lineCopy := in.Line
	labelsCopy := slices.Clone(in.Labels)
	metaCopy := slices.Clone(in.Meta)
	out, oc := apply(t, c, in)
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v", oc)
	}
	// The steps had their effects: redacted line, promoted label, rerouted.
	if out.Line != `{"a":"***"}` {
		t.Errorf("line = %q, want redacted", out.Line)
	}
	if got, _ := out.Labels.Get("promoted"); got != "value" {
		t.Errorf("promoted = %q, want value (renamed field)", got)
	}
	if got, _ := out.Labels.Get("stream"); got != "z" {
		t.Errorf("stream = %q", got)
	}
	// The input is untouched.
	if in.Line != lineCopy || !slices.Equal(in.Labels, labelsCopy) || !slices.Equal(in.Meta, metaCopy) {
		t.Fatalf("input mutated:\n line=%q\n labels=%v\n meta=%v", in.Line, in.Labels, in.Meta)
	}
	// Output shares no mutable state.
	if len(out.Labels) > 0 && len(in.Labels) > 0 && &out.Labels[0] == &in.Labels[0] {
		t.Errorf("output labels share backing array")
	}
	if len(out.Meta) > 0 && len(in.Meta) > 0 && &out.Meta[0] == &in.Meta[0] {
		t.Errorf("output meta share backing array")
	}
	// Dropping everything still does not mutate.
	drop := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepDropIf, Expr: "true"}},
	})
	if _, oc := apply(t, drop, in); oc != OutcomeDrop {
		t.Fatalf("drop: %v", oc)
	}
	if in.Line != lineCopy || !slices.Equal(in.Labels, labelsCopy) || !slices.Equal(in.Meta, metaCopy) {
		t.Fatalf("input mutated by drop")
	}
}

// TestInputBounds: oversized input is a data error of the first step's
// policy, never amplified. Every case uses a step that succeeds on any
// well-bounded input (drop_if false), so only the input bound itself can
// fire, and the marker details name the exceeded bound.
func TestInputBounds(t *testing.T) {
	opts := CompileOptions{StreamLabels: []string{"stream"}, LineBytes: 16, MetaBytes: 32, MetaValueBytes: 8, LabelValueBytes: 8}
	c, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepDropIf, Expr: "false"}},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	bound := func(name string, in Entry, wantDetails string) {
		t.Helper()
		out, oc := apply(t, c, in)
		if oc != OutcomeStop {
			t.Fatalf("%s: outcome = %v, want stop", name, oc)
		}
		if got, _ := out.Meta.Get(ErrField); got != "drop_if" {
			t.Errorf("%s: marker = %q", name, got)
		}
		details, _ := out.Meta.Get(ErrDetailsField)
		if !strings.Contains(details, wantDetails) {
			t.Errorf("%s: details = %q, want mention of %q", name, details, wantDetails)
		}
	}
	bound("oversized line", Entry{Line: strings.Repeat("a", 17)}, "line exceeds")
	bound("oversized metadata value", Entry{
		Line: "ok",
		Meta: logs.New(logs.Label{Name: "ab", Value: "123456789"}), // name 2 + value 9: value alone breaks MetaValueBytes
	}, "metadata value")
	bound("oversized metadata total", Entry{
		Line: "ok",
		// Each value fits MetaValueBytes; the total (6x(2+5)=42) breaks MetaBytes.
		Meta: logs.New(
			logs.Label{Name: "aa", Value: "12345"},
			logs.Label{Name: "bb", Value: "12345"},
			logs.Label{Name: "cc", Value: "12345"},
			logs.Label{Name: "dd", Value: "12345"},
			logs.Label{Name: "ee", Value: "12345"},
			logs.Label{Name: "ff", Value: "12345"},
		),
	}, "metadata exceeds")
	bound("oversized label value", Entry{
		Line:   "ok",
		Labels: logs.New(logs.Label{Name: "l", Value: "123456789"}),
	}, "label value")
	// A well-bounded entry passes through the same step untouched.
	out, oc := apply(t, c, Entry{Line: "ok", Meta: logs.New(logs.Label{Name: "ab", Value: "1"})})
	if oc != OutcomeKeep || out.Line != "ok" {
		t.Fatalf("bounded entry: %v %q", oc, out.Line)
	}
}

// TestCanceledContextWins: a canceled context answers with an error even
// for an oversized entry, never keep/drop.
func TestCanceledContextWins(t *testing.T) {
	opts := CompileOptions{StreamLabels: []string{"stream"}, LineBytes: 8}
	c, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepDropIf, Expr: "true"}},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, oc, err := c.Apply(ctx, "app", Entry{Line: strings.Repeat("a", 100)})
	if oc == OutcomeDrop || oc == OutcomeStop || err == nil {
		t.Fatalf("canceled: outcome=%v err=%v", oc, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// stepCancelCtx wraps a real cancellable context and cancels it on the
// (n+1)th Err observation, so a test can cancel inside a parse step while
// Done/Err stay real.
type stepCancelCtx struct {
	context.Context
	cancel context.CancelFunc
	n      int
}

func newStepCancelCtx(t *testing.T, n int) *stepCancelCtx {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &stepCancelCtx{Context: ctx, cancel: cancel, n: n}
}

func (c *stepCancelCtx) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	if c.n <= 0 {
		c.cancel()
		return c.Context.Err()
	}
	c.n--
	return nil
}

// TestCancelInsideParseIsNotADataError: a ctx cancellation that lands between
// JSON members must surface as an operation error, never run on_error
// (here drop) and never answer keep/drop for the entry.
func TestCancelInsideParseIsNotADataError(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, OnError: OnErrorDrop}},
	})
	// Apply checks at entry and per step; the parse checks once before the
	// object and once per member, so four ticks cancel on member "b".
	ctx := newStepCancelCtx(t, 4)
	_, oc, err := c.Apply(ctx, "app", Entry{Line: `{"a":1,"b":2,"c":3}`})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v, want keep with an error", oc)
	}
}

// TestCancelAfterParseBeforeCommitIsNotADataError: a cancellation that lands
// after the parser succeeds but before execParse commits must still be an
// operation error and skip on_error.
func TestCancelAfterParseBeforeCommitIsNotADataError(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, OnError: OnErrorDrop}},
	})
	// Checks: Apply entry, Apply step, parse pre, three members, the loop's
	// end-object check and the parse's post-success check are the first
	// eight; the ninth is execParse's post-parse check.
	ctx := newStepCancelCtx(t, 8)
	_, oc, err := c.Apply(ctx, "app", Entry{Line: `{"a":1,"b":2,"c":3}`})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if oc != OutcomeKeep {
		t.Fatalf("outcome = %v, want keep with an error", oc)
	}
}

// TestMarkerUTF8Boundary: truncated details never split a rune.
func TestMarkerUTF8Boundary(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepDropIf, Expr: `int(meta.n) > 0`}},
	})
	// int() of a long multi-byte string fails with a message carrying the
	// value; the marker is truncated at 256 bytes.
	out, oc := apply(t, c, Entry{Line: "x", Meta: logs.New(logs.Label{Name: "n", Value: strings.Repeat("é", 200)})})
	if oc != OutcomeStop {
		t.Fatalf("outcome = %v", oc)
	}
	details, _ := out.Meta.Get(ErrDetailsField)
	if len(details) > MaxErrorDetails {
		t.Errorf("details %d bytes", len(details))
	}
	if !utf8.ValidString(details) {
		t.Errorf("details split a rune: %q", details)
	}
	// A literal replacement character is valid and must survive.
	if !utf8.ValidString("a�b") {
		t.Errorf("ValidString rejects literal U+FFFD")
	}
}

// TestMarkerExemptFromBounds: the keep markers are written even when the
// metadata budget is already exhausted.
func TestMarkerExemptFromBounds(t *testing.T) {
	opts := CompileOptions{StreamLabels: []string{"stream"}, MetaBytes: 8}
	c, err := Compile(Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON}},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	out, oc := applyOpts(t, c, Entry{Line: "notjson", Meta: logs.New(logs.Label{Name: "ab", Value: "12"})})
	if oc != OutcomeStop {
		t.Fatalf("outcome = %v", oc)
	}
	if _, ok := out.Meta.Get(ErrDetailsField); !ok {
		t.Errorf("marker suppressed by budget")
	}
}

// TestConcurrentApply: one Compiled, many goroutines; run with -race.
func TestConcurrentApply(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepParseJSON},
			{Type: StepRedact, Pattern: `tok-\d+`},
			{Type: StepPromote, From: "svc", To: "service"},
		},
	})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				out, oc, err := c.Apply(t.Context(), "app", Entry{
					Line:   `{"svc":"api` + strconv.Itoa(g*1000+i) + `"}`,
					Labels: logs.New(logs.Label{Name: "stream", Value: "app"}),
				})
				if err != nil || oc != OutcomeKeep {
					t.Errorf("apply %d/%d: %v %v", g, i, oc, err)
					return
				}
				if _, ok := out.Labels.Get("service"); !ok {
					t.Errorf("apply %d/%d: no promotion", g, i)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestSnapshotDupAcrossStages: names are tenant-unique even across stages.
func TestSnapshotDupAcrossStages(t *testing.T) {
	a := mustCompile(t, Definition{Name: "x", Stage: StageIngest, Selector: "*", Steps: []Step{{Type: StepParseJSON}}})
	b := mustCompile(t, Definition{Name: "x", Stage: StageRead, Selector: "*", Steps: []Step{{Type: StepDropIf, Expr: "false"}}})
	if _, err := NewFixedSnapshot(a, b); err == nil {
		t.Fatal("duplicate name across stages accepted")
	}
}

func TestSnapshotHas(t *testing.T) {
	i := mustCompile(t, Definition{Name: "i", Stage: StageIngest, Selector: "app", Steps: []Step{{Type: StepParseJSON}}})
	snap, err := NewFixedSnapshot(i)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Has(StageIngest) {
		t.Errorf("Has(ingest) = false")
	}
	if snap.Has(StageRead) {
		t.Errorf("Has(read) = true (exact pass-through case)")
	}
}

// TestObserveExecution: the observer fires exactly once per Apply, for every
// result class, with the stage and the raw outcome/error pair.
func TestObserveExecution(t *testing.T) {
	type call struct {
		stage    Stage
		outcome  Outcome
		reject   bool
		otherErr bool
	}
	var mu sync.Mutex
	var calls []call
	observe := func(stage Stage, outcome Outcome, err error, elapsed time.Duration) {
		_, reject := errors.AsType[*RejectError](err)
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, call{
			stage:    stage,
			outcome:  outcome,
			reject:   reject,
			otherErr: err != nil && !reject,
		})
		if elapsed < 0 {
			t.Errorf("negative elapsed")
		}
	}
	opts := CompileOptions{StreamLabels: []string{"stream"}, ObserveExecution: observe}
	compile := func(steps ...Step) *Compiled {
		c, err := Compile(Definition{Name: "p", Stage: StageIngest, Selector: "*", Steps: steps}, opts)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	_, _, _ = compile(Step{Type: StepParseJSON}).Apply(t.Context(), "app", Entry{Line: `{}`})
	_, _, _ = compile(Step{Type: StepParseJSON}).Apply(t.Context(), "app", Entry{Line: "bad"})
	_, _, _ = compile(Step{Type: StepDropIf, Expr: "true"}).Apply(t.Context(), "app", Entry{Line: "x"})
	_, _, _ = compile(Step{Type: StepParseJSON, OnError: OnErrorReject}).Apply(t.Context(), "app", Entry{Line: "bad"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _ = compile(Step{Type: StepParseJSON}).Apply(ctx, "app", Entry{Line: "x"})

	mu.Lock()
	defer mu.Unlock()
	want := []call{
		{StageIngest, OutcomeKeep, false, false},
		{StageIngest, OutcomeStop, false, false},
		{StageIngest, OutcomeDrop, false, false},
		{StageIngest, OutcomeKeep, true, false},
		{StageIngest, OutcomeKeep, false, true}, // cancellation
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %+v, want %+v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Errorf("call %d = %+v, want %+v", i, calls[i], want[i])
		}
	}
}

// TestExtractSanitizesAndFlattens is the regression for normal-world keys:
// dotted, hyphenated and @-prefixed JSON/logfmt keys are sanitized instead of
// rejected, and nested JSON objects flatten to underscore-joined leaf names.
func TestExtractSanitizesAndFlattens(t *testing.T) {
	cj := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseJSON}},
	})
	out, oc := apply(t, cj, Entry{Line: `{"user.id":7,"http-status":"200","@timestamp":"t","nested":{"id":9}}`})
	if oc != OutcomeKeep {
		t.Fatalf("json: outcome = %v, want keep", oc)
	}
	for k, want := range map[string]string{
		"user_id": "7", "http_status": "200", "_timestamp": "t", "nested_id": "9",
	} {
		if got, _ := out.Meta.Get(k); got != want {
			t.Errorf("json meta[%q] = %q, want %q (meta %v)", k, got, want, out.Meta)
		}
	}

	cl := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepParseLogfmt}},
	})
	out, oc = apply(t, cl, Entry{Line: `user.id=7 http-status=200 @timestamp=t`})
	if oc != OutcomeKeep {
		t.Fatalf("logfmt: outcome = %v, want keep", oc)
	}
	for k, want := range map[string]string{
		"user_id": "7", "http_status": "200", "_timestamp": "t",
	} {
		if got, _ := out.Meta.Get(k); got != want {
			t.Errorf("logfmt meta[%q] = %q, want %q (meta %v)", k, got, want, out.Meta)
		}
	}
}

// TestSanitizedExtractChain: sanitized names are ordinary extracted metadata
// for later steps, at both the ingest and read stages.
func TestSanitizedExtractChain(t *testing.T) {
	c := mustCompile(t, Definition{
		Name: "p", Stage: StageIngest, Selector: "*",
		Steps: []Step{
			{Type: StepParseJSON},
			{Type: StepPromote, From: "user_id", To: "uid", When: `meta.user_id != ""`},
		},
	})
	out, oc := apply(t, c, Entry{Line: `{"user":{"id":7},"http-status":200}`})
	if oc != OutcomeKeep {
		t.Fatalf("ingest: %v", oc)
	}
	if got, _ := out.Meta.Get("user_id"); got != "7" {
		t.Errorf("meta user_id = %q", got)
	}
	if got, _ := out.Meta.Get("http_status"); got != "200" {
		t.Errorf("meta http_status = %q", got)
	}
	if got, _ := out.Labels.Get("uid"); got != "7" {
		t.Errorf("promoted uid = %q", got)
	}

	// Read stage: parse_json from transformed metadata into sanitized names.
	cr := mustCompile(t, Definition{
		Name: "r", Stage: StageRead, Selector: "*",
		Steps: []Step{{Type: StepParseJSON, From: "raw"}},
	})
	out, oc = apply(t, cr, Entry{Line: "not json", Meta: logs.New(logs.Label{Name: "raw", Value: `{"@timestamp":"t","1n":2}`})})
	if oc != OutcomeKeep {
		t.Fatalf("read: %v", oc)
	}
	if got, _ := out.Meta.Get("_timestamp"); got != "t" {
		t.Errorf("read _timestamp = %q", got)
	}
	if got, _ := out.Meta.Get("_1n"); got != "2" {
		t.Errorf("read _1n = %q", got)
	}
}

// TestExtractErrorPolicies: genuine remaining extraction errors (here an empty
// JSON member name) still flow through on_error atomically.
func TestExtractErrorPolicies(t *testing.T) {
	def := func(policy OnError) Definition {
		return Definition{
			Name: "p", Stage: StageIngest, Selector: "*",
			Steps: []Step{{Type: StepParseJSON, OnError: policy}},
		}
	}
	// keep: pre-step metadata restored, markers written, stop.
	out, oc := apply(t, mustCompile(t, def(OnErrorKeep)), Entry{Line: `{"ok":1,"":2}`, Meta: logs.New(logs.Label{Name: "pre", Value: "yes"})})
	if oc != OutcomeStop {
		t.Fatalf("keep: outcome = %v", oc)
	}
	if got, _ := out.Meta.Get("pre"); got != "yes" {
		t.Errorf("keep: pre = %q", got)
	}
	if _, ok := out.Meta.Get("ok"); ok {
		t.Errorf("keep: ok merged despite atomic rollback")
	}
	if got, _ := out.Meta.Get(ErrField); got != "parse_json" {
		t.Errorf("keep: marker = %q", got)
	}
	// drop.
	if _, oc = apply(t, mustCompile(t, def(OnErrorDrop)), Entry{Line: `{"":2}`}); oc != OutcomeDrop {
		t.Fatalf("drop: outcome = %v", oc)
	}
	// reject: Apply returns *RejectError.
	c := mustCompile(t, def(OnErrorReject))
	_, _, err := c.Apply(t.Context(), "app", Entry{Line: `{"":2}`})
	if err == nil {
		t.Fatal("reject: want error")
	}
	if rej, ok := errors.AsType[*RejectError](err); !ok || rej.Step != "parse_json" {
		t.Fatalf("reject: err = %v", err)
	}
}
