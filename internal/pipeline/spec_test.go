package pipeline_test

import (
	"encoding/json/v2"
	"strings"
	"testing"

	. "github.com/nathabonfim59/parkana/internal/pipeline"
)

func TestDecodeSpec(t *testing.T) {
	s, err := DecodeSpec([]byte(`{"steps":[{"type":"parse_logfmt"},{"type":"redact","pattern":"a","replace":""}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Steps) != 2 || s.Steps[0].Type != StepParseLogfmt {
		t.Fatalf("steps = %+v", s.Steps)
	}
	if s.Steps[1].Replace == nil || *s.Steps[1].Replace != "" {
		t.Errorf("explicit empty replace lost: %+v", s.Steps[1])
	}
	if _, err := DecodeSpec([]byte(`{"steps":[]}`)); err != nil {
		t.Errorf("empty steps decode fine; Compile rejects them: %v", err)
	}
	// Unknown step member.
	if _, err := DecodeSpec([]byte(`{"steps":[{"type":"parse_json","colour":"red"}]}`)); err == nil {
		t.Errorf("unknown member accepted")
	}
	// Trailing data.
	if _, err := DecodeSpec([]byte(`{"steps":[]} x`)); err == nil {
		t.Errorf("trailing data accepted")
	}
}

// TestDecodeDefinitionPresence: members that are present but empty are
// rejected for step types that do not use them; present-and-null members are
// decode errors outright.
func TestDecodeDefinitionPresence(t *testing.T) {
	present := []string{
		`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"drop_if","expr":"true","target":""}]}`,
		`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"parse_json","value":""}]}`,
	}
	for _, doc := range present {
		d, err := DecodeDefinition([]byte(doc))
		if err != nil {
			t.Fatalf("decode %s: %v", doc, err)
		}
		if _, err := Compile(d, CompileOptions{StreamLabels: []string{"stream"}}); err == nil {
			t.Errorf("accepted %s", doc)
		} else if !strings.Contains(err.Error(), "does not take") {
			t.Errorf("wrong error for %s: %v", doc, err)
		}
	}
	// Present-and-empty from on a parse step is the documented default.
	if d, err := DecodeDefinition([]byte(`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"parse_json","from":""}]}`)); err != nil {
		t.Fatalf("decode from-empty: %v", err)
	} else if _, err := Compile(d, CompileOptions{StreamLabels: []string{"stream"}}); err != nil {
		t.Errorf("from-empty rejected: %v", err)
	}
	nulls := []string{
		`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"drop_if","expr":"true","pattern":null}]}`,
		`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"parse_json","from":null}]}`,
		`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"redact","pattern":"a","replace":null}]}`,
	}
	for _, doc := range nulls {
		if _, err := DecodeDefinition([]byte(doc)); err == nil {
			t.Errorf("null member accepted: %s", doc)
		}
	}
	// Missing type.
	if _, err := DecodeDefinition([]byte(`{"name":"n","stage":"ingest","selector":"*","steps":[{"pattern":"a"}]}`)); err == nil {
		t.Errorf("missing type accepted")
	}
}

// TestJSONWhenCompiles: when is a member every step type accepts, so a
// definition carrying it must compile after decoding from JSON for all eight
// step types. Built in Go the field is invisible to the presence check; this
// regression covers the JSON/API path.
func TestJSONWhenCompiles(t *testing.T) {
	steps := []struct {
		name string
		step string
	}{
		{"parse_json", `{"type":"parse_json","when":"line != \"\""}`},
		{"parse_logfmt", `{"type":"parse_logfmt","when":"line != \"\""}`},
		{"regex_extract", `{"type":"regex_extract","pattern":"(?P<x>a)","when":"line contains \"MASKED\""}`},
		{"rename", `{"type":"rename","from":"a","to":"b","when":"line != \"\""}`},
		{"redact", `{"type":"redact","pattern":"a","when":"line != \"\""}`},
		{"drop_if", `{"type":"drop_if","expr":"false","when":"line != \"\""}`},
		{"promote", `{"type":"promote","from":"a","to":"b","when":"line != \"\""}`},
		{"set_stream", `{"type":"set_stream","value":"\"x\"","when":"line != \"\""}`},
	}
	for _, tc := range steps {
		t.Run(tc.name, func(t *testing.T) {
			doc := []byte(`{"name":"n","stage":"ingest","selector":"*","steps":[` + tc.step + `]}`)
			d, err := DecodeDefinition(doc)
			if err != nil {
				t.Fatalf("DecodeDefinition: %v", err)
			}
			if _, err := Compile(d, CompileOptions{StreamLabels: []string{"stream"}}); err != nil {
				t.Fatalf("Compile with common when: %v", err)
			}
		})
	}
}

// TestJSONWhenRoundTripCompiles: a definition decoded from JSON keeps its
// common when through both document round-trips (the API Definition and the
// catalog Spec) and still compiles.
func TestJSONWhenRoundTripCompiles(t *testing.T) {
	doc := []byte(`{"name":"n","stage":"ingest","selector":"*","steps":[{"type":"parse_json","when":"line != \"\""},{"type":"redact","pattern":"a","when":"line contains \"a\"","on_error":"drop"}]}`)
	d, err := DecodeDefinition(doc)
	if err != nil {
		t.Fatalf("DecodeDefinition: %v", err)
	}
	// Assert the conditions survive, not just Compile: a marshaler that
	// dropped when or on_error would still compile.
	check := func(label string, d Definition) {
		t.Helper()
		want := []struct {
			when  string
			onErr OnError
		}{
			{`line != ""`, ""},
			{`line contains "a"`, OnErrorDrop},
		}
		for i, w := range want {
			if d.Steps[i].When != w.when {
				t.Fatalf("%s: step %d when = %q, want %q", label, i+1, d.Steps[i].When, w.when)
			}
			if d.Steps[i].OnError != w.onErr {
				t.Fatalf("%s: step %d on_error = %q, want %q", label, i+1, d.Steps[i].OnError, w.onErr)
			}
		}
		if _, err := Compile(d, CompileOptions{StreamLabels: []string{"stream"}}); err != nil {
			t.Fatalf("%s: Compile: %v", label, err)
		}
	}
	check("decoded", d)

	defJSON, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	fromDef, err := DecodeDefinition(defJSON)
	if err != nil {
		t.Fatalf("decode definition round-trip: %v", err)
	}
	check("definition round-trip", fromDef)

	specJSON, err := json.Marshal(d.Spec())
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	spec, err := DecodeSpec(specJSON)
	if err != nil {
		t.Fatalf("decode spec round-trip: %v", err)
	}
	fromSpec := Definition{Name: d.Name, Stage: d.Stage, Selector: d.Selector, Steps: spec.Steps}
	check("spec round-trip", fromSpec)
}

// TestJSONWhenRejectsInvalid: allowing the common members does not weaken
// type checking. A non-boolean or null when fails, and a wrong-for-the-type
// member is still rejected after decoding.
func TestJSONWhenRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		step string
		want string
	}{
		{"nonbool when", `{"type":"parse_json","when":"line"}`, "must be boolean"},
		{"nonbool call when", `{"type":"parse_json","when":"int(meta.n)"}`, "must be boolean"},
		{"unknown name in when", `{"type":"parse_json","when":"nope"}`, "unknown name"},
		{"null when", `{"type":"parse_json","when":null}`, "must be a string, got null"},
		{"number when", `{"type":"parse_json","when":42}`, "when must be a string"},
		{"expr on parse", `{"type":"parse_json","when":"true","expr":"true"}`, "does not take expr"},
		{"pattern on rename", `{"type":"rename","from":"a","to":"b","when":"true","pattern":"x"}`, "does not take pattern"},
		{"target on drop_if", `{"type":"drop_if","expr":"true","when":"true","target":""}`, "does not take target"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := []byte(`{"name":"n","stage":"ingest","selector":"*","steps":[` + tc.step + `]}`)
			d, err := DecodeDefinition(doc)
			if err == nil {
				_, err = Compile(d, CompileOptions{StreamLabels: []string{"stream"}})
			}
			if err == nil {
				t.Fatalf("accepted %s", tc.step)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestStepUnmarshalResetsReceiver: reusing a Step cannot leak previous
// fields or presence.
func TestStepUnmarshalResetsReceiver(t *testing.T) {
	s := Step{Type: StepRename, From: "old", To: "older", Replace: new("stale")}
	if err := json.Unmarshal([]byte(`{"type":"drop_if","expr":"true"}`), &s); err != nil {
		t.Fatal(err)
	}
	if s.From != "" || s.To != "" || s.Replace != nil {
		t.Errorf("stale fields leaked: %+v", s)
	}
	if s.Type != StepDropIf || s.Expr != "true" {
		t.Errorf("bad decode: %+v", s)
	}
}

// TestSpecRoundTrip: the steps-only document round-trips through the
// catalog shape, with replace omitted, defaulted and explicitly empty.
func TestSpecRoundTrip(t *testing.T) {
	empty := ""
	d := Definition{
		Name:     "n",
		Stage:    StageIngest,
		Selector: "*",
		Steps: []Step{
			{Type: StepRedact, Pattern: "a"},
			{Type: StepRedact, Pattern: "b", Replace: new("")},
			{Type: StepRedact, Pattern: "c", Replace: new("x")},
			{Type: StepDropIf, Expr: "false", OnError: OnErrorDrop, When: "true"},
		},
	}
	b, err := json.Marshal(d.Spec())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"steps":[{"type":"redact","pattern":"a"},{"type":"redact","pattern":"b","replace":""},{"type":"redact","pattern":"c","replace":"x"},{"type":"drop_if","expr":"false","when":"true","on_error":"drop"}]}`
	if string(b) != want {
		t.Fatalf("spec = %s, want %s", b, want)
	}
	s, err := DecodeSpec(b)
	if err != nil {
		t.Fatal(err)
	}
	if s.Steps[0].Replace != nil || s.Steps[1].Replace == nil || *s.Steps[1].Replace != empty {
		t.Errorf("replace round-trip: %+v", s.Steps)
	}
}
