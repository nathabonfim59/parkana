package pipeline_test

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
	. "github.com/nathabonfim59/parkana/internal/pipeline"
)

func mustCompile(t *testing.T, def Definition) *Compiled {
	t.Helper()
	c, err := Compile(def, CompileOptions{StreamLabels: []string{"stream", "org"}})
	if err != nil {
		t.Fatalf("Compile(%s): %v", def.Name, err)
	}
	return c
}

func TestSmokeEndToEnd(t *testing.T) {
	c := mustCompile(t, Definition{
		Name:     "app-json",
		Stage:    StageIngest,
		Selector: "app",
		Steps: []Step{
			{Type: StepParseJSON},
			{Type: StepRename, From: "lvl", To: "level"},
			{Type: StepPromote, From: "level", To: "level"},
			{Type: StepRedact, Pattern: `token=\w+`, Replace: new("token=***")},
			{Type: StepDropIf, Expr: `meta.level == "debug"`, When: `line != ""`},
		},
	})

	in := Entry{
		Line:   `{"lvl":"info","svc":"api","nested":{"b":2,"a":1},"n":1e3}`,
		Labels: logs.New(logs.Label{Name: "stream", Value: "app"}),
	}
	out, outcome, err := c.Apply(t.Context(), "app", in)
	if err != nil || outcome != OutcomeKeep {
		t.Fatalf("Apply: outcome=%v err=%v", outcome, err)
	}
	if got, _ := out.Meta.Get("level"); got != "info" {
		t.Errorf("level meta = %q, want info", got)
	}
	if got, _ := out.Labels.Get("level"); got != "info" {
		t.Errorf("level label = %q, want info", got)
	}
	if got, _ := out.Meta.Get("nested_b"); got != "2" {
		t.Errorf("nested_b = %q, want 2", got)
	}
	if got, _ := out.Meta.Get("nested_a"); got != "1" {
		t.Errorf("nested_a = %q, want 1", got)
	}
	if got, _ := out.Meta.Get("n"); got != "1e3" {
		t.Errorf("n = %q, want exact spelling 1e3", got)
	}
	if out.Line != in.Line {
		t.Errorf("line = %q", out.Line)
	}
	// Redaction runs on a line-shaped entry in the same pipeline.
	lineIn := Entry{Line: "msg token=secret tail", Labels: in.Labels}
	lineOut, oc, err := c.Apply(t.Context(), "app", lineIn)
	if err != nil || oc != OutcomeStop {
		t.Fatalf("redact pass: outcome=%v err=%v", oc, err)
	}
	// parse_json fails on a non-JSON line (default keep), but the line
	// survives untouched with the marker; redact never ran (steps skipped).
	if lineOut.Line != "msg token=secret tail" {
		t.Errorf("line = %q, want restored original", lineOut.Line)
	}
	if got, _ := lineOut.Meta.Get(ErrField); got != "parse_json" {
		t.Errorf("pipeline_error = %q", got)
	}
	// Seeded keys referenced by when (meta.lvl) must not materialize.
	if _, ok := out.Meta.Get("lvl"); ok {
		t.Errorf("lvl survived rename and leaked")
	}
	if len(in.Meta) != 0 {
		t.Errorf("input metadata mutated: %v", in.Meta)
	}

	// drop_if fires when lvl present and level==debug.
	in2 := in
	in2.Line = `{"lvl":"debug"}`
	if _, outcome, err := c.Apply(t.Context(), "app", in2); err != nil || outcome != OutcomeDrop {
		t.Fatalf("drop: outcome=%v err=%v", outcome, err)
	}

	// Malformed JSON: default keep restores, marks and stops.
	in3 := in
	in3.Line = "not json"
	out3, outcome3, err := c.Apply(t.Context(), "app", in3)
	if err != nil || outcome3 != OutcomeStop {
		t.Fatalf("keep: outcome=%v err=%v", outcome3, err)
	}
	if got, _ := out3.Meta.Get(ErrField); got != "parse_json" {
		t.Errorf("pipeline_error = %q", got)
	}
	if out3.Line != "not json" {
		t.Errorf("line not restored: %q", out3.Line)
	}

	// set_stream always writes the sanitized value to the first routing label.
	cs := mustCompile(t, Definition{
		Name: "route", Stage: StageIngest, Selector: "*",
		Steps: []Step{{Type: StepSetStream, Value: `lower(labels.tenant) + "-logs"`}},
	})
	out4, _, err := cs.Apply(t.Context(), "app", Entry{
		Line:   "hi",
		Labels: logs.New(logs.Label{Name: "stream", Value: "app"}, logs.Label{Name: "tenant", Value: "Pay"}),
	})
	if err != nil {
		t.Fatalf("set_stream: %v", err)
	}
	if got, _ := out4.Labels.Get("stream"); got != "pay-logs" {
		t.Errorf("stream label = %q, want pay-logs", got)
	}
}

func TestSmokeSnapshotMatch(t *testing.T) {
	a := mustCompile(t, Definition{Name: "a", Stage: StageIngest, Selector: "app", Steps: []Step{{Type: StepParseLogfmt}}})
	b := mustCompile(t, Definition{Name: "b", Stage: StageIngest, Selector: "*", Steps: []Step{{Type: StepParseJSON}}})
	r := mustCompile(t, Definition{Name: "r", Stage: StageRead, Selector: "*", Steps: []Step{{Type: StepDropIf, Expr: `line == ""`}}})
	snap, err := NewFixedSnapshot(a, b, r)
	if err != nil {
		t.Fatal(err)
	}
	got := snap.For(StageIngest, "app")
	if len(got) != 2 || got[0].Name() != "a" || got[1].Name() != "b" {
		t.Errorf("ingest/app = %v", got)
	}
	got = snap.For(StageIngest, "other")
	if len(got) != 1 || got[0].Name() != "b" {
		t.Errorf("ingest/other = %v", got)
	}
	if len(snap.For(StageRead, "app")) != 1 {
		t.Errorf("read/app")
	}
	// Cloned: mutating the result does not affect later calls.
	got[0] = nil
	if snap.For(StageIngest, "app")[0] == nil {
		t.Errorf("snapshot result not cloned")
	}
}
