// Package pipeline compiles and executes log pipelines: ordered steps that
// parse, rewrite, promote or drop log entries at ingest (PLAN.md §6, phase 4)
// and at read time.
//
// A Definition is the administrator-facing JSON document. Compile turns it
// into an immutable, concurrency-safe Compiled that Apply runs over entries.
// The JSON stored in the catalog is the steps-only Spec; name, stage and
// selector live in catalog columns and the registry restores them, so a Spec
// can never override authoritative fields. This package does not import the
// catalog.
package pipeline

import (
	"fmt"
	"strings"

	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// Stage selects when a pipeline runs.
type Stage string

// The two pipeline stages.
const (
	// StageIngest runs in the ingester before the WAL.
	StageIngest Stage = "ingest"
	// StageRead runs in the querier on every row a query returns.
	StageRead Stage = "read"
)

// Valid reports whether s is a known stage.
func (s Stage) Valid() bool { return s == StageIngest || s == StageRead }

// OnError is a step's policy for data-dependent errors (parse failures,
// regex no-match, expression runtime errors, exceeded bounds). Keep restores
// the pre-step entry, records pipeline_error/pipeline_error_details metadata
// and skips the entry's remaining steps; Drop removes the entry; Reject
// fails the whole ingest request (or query) explicitly.
type OnError string

const (
	OnErrorKeep   OnError = "keep"
	OnErrorDrop   OnError = "drop"
	OnErrorReject OnError = "reject"
)

// Valid reports whether p is a known policy.
func (p OnError) Valid() bool {
	return p == OnErrorKeep || p == OnErrorDrop || p == OnErrorReject
}

// StepType discriminates the eight pipeline steps.
type StepType string

// The eight step types.
const (
	StepParseJSON    StepType = "parse_json"
	StepParseLogfmt  StepType = "parse_logfmt"
	StepRegexExtract StepType = "regex_extract"
	StepRename       StepType = "rename"
	StepRedact       StepType = "redact"
	StepDropIf       StepType = "drop_if"
	StepPromote      StepType = "promote"
	StepSetStream    StepType = "set_stream"
)

// fromLine marks a parse/redact source or target of the log line itself
// instead of a metadata field.
const fromLine = "line"

// Step is one pipeline step. The JSON object is a tagged union: Type selects
// the step and which fields are meaningful; Compile rejects members that the
// type does not use — including members that are present but empty or null,
// which UnmarshalJSON records — and the JSON decoder rejects unknown members
// outright. Every step accepts When (a boolean expression over the pre-step
// state; false skips the step) and OnError (default keep).
type Step struct {
	Type StepType `json:"type"`
	// From is the source: the line (default, "line") or a metadata field
	// name for the parse steps, the moved field for rename, and the
	// metadata literal for promote/set_stream (where "line" is an ordinary
	// metadata field name, not the log line).
	From string `json:"from,omitempty"`
	// To is rename's destination metadata field or promote's target label.
	To string `json:"to,omitempty"`
	// Pattern is regex_extract's or redact's Go RE2 pattern.
	Pattern string `json:"pattern,omitempty"`
	// Replace is redact's replacement (Go regexp syntax). nil means the
	// default "***"; a pointer to an empty string deletes matches.
	// omitzero (not omitempty): encoding/json/v2 omits a pointer to an
	// empty string under omitempty, and an explicit empty replacement must
	// survive the round-trip.
	Replace *string `json:"replace,omitzero"`
	// Target is redact's destination: the line (default, "line") or a
	// metadata field.
	Target string `json:"target,omitempty"`
	// Expr is drop_if's required boolean expression.
	Expr string `json:"expr,omitempty"`
	// Value is promote's or set_stream's string expression (exclusive with From).
	Value string `json:"value,omitempty"`
	// When is the optional boolean condition guarding the step.
	When string `json:"when,omitempty"`
	// OnError is the step's data-error policy; empty means keep.
	OnError OnError `json:"on_error,omitempty"`

	// present records which typed members appeared in the JSON object
	// (including explicit empty or null), so Compile can reject them for
	// step types that do not use them. It is empty for steps built in Go.
	present []string
}

// UnmarshalJSON decodes one step object, remembering every member that was
// present. Unknown members are rejected here; wrong-for-the-type members are
// rejected by Compile, which knows Type's rules. The receiver is reset
// first, so reusing a Step cannot leak previous values or presence.
func (s *Step) UnmarshalJSON(b []byte) error {
	*s = Step{}
	var raw struct {
		Type    StepType       `json:"type"`
		From    jsontext.Value `json:"from"`
		To      jsontext.Value `json:"to"`
		Pattern jsontext.Value `json:"pattern"`
		Replace jsontext.Value `json:"replace"`
		Target  jsontext.Value `json:"target"`
		Expr    jsontext.Value `json:"expr"`
		Value   jsontext.Value `json:"value"`
		When    jsontext.Value `json:"when"`
		OnError jsontext.Value `json:"on_error"`
	}
	if err := decodeDoc(b, &raw); err != nil {
		return err
	}
	str := func(name string, v jsontext.Value, dst *string) error {
		if len(v) == 0 {
			return nil
		}
		s.present = append(s.present, name)
		if v.Kind() == jsontext.KindNull {
			return fmt.Errorf("%s must be a string, got null", name)
		}
		var sv string
		if err := json.Unmarshal(v, &sv); err != nil {
			return fmt.Errorf("%s must be a string", name)
		}
		*dst = sv
		return nil
	}
	if raw.Type == "" {
		return fmt.Errorf("type is required")
	}
	s.Type = raw.Type
	for _, f := range []struct {
		name string
		v    jsontext.Value
		dst  *string
	}{
		{"from", raw.From, &s.From},
		{"to", raw.To, &s.To},
		{"pattern", raw.Pattern, &s.Pattern},
		{"target", raw.Target, &s.Target},
		{"expr", raw.Expr, &s.Expr},
		{"value", raw.Value, &s.Value},
		{"when", raw.When, &s.When},
	} {
		if err := str(f.name, f.v, f.dst); err != nil {
			return err
		}
	}
	// replace is a *string: present-and-null is invalid, present records it.
	if len(raw.Replace) > 0 {
		s.present = append(s.present, "replace")
		if raw.Replace.Kind() == jsontext.KindNull {
			return fmt.Errorf("replace must be a string, got null")
		}
		var rv string
		if err := json.Unmarshal(raw.Replace, &rv); err != nil {
			return fmt.Errorf("replace must be a string")
		}
		s.Replace = &rv
	}
	if len(raw.OnError) > 0 {
		if raw.OnError.Kind() == jsontext.KindNull {
			return fmt.Errorf("on_error must be a string, got null")
		}
		var ov string
		if err := json.Unmarshal(raw.OnError, &ov); err != nil {
			return fmt.Errorf("on_error must be a string")
		}
		s.OnError = OnError(ov)
	}
	return nil
}

// Spec is the steps-only JSON document the catalog persists:
// {"steps":[...]}.
type Spec struct {
	Steps []Step `json:"steps"`
}

// Definition is the full API document: everything a pipeline is, plus its
// steps. Name, stage and selector are authoritative in the catalog row; the
// registry rebuilds a Definition from the row plus the stored Spec.
type Definition struct {
	Name     string `json:"name"`
	Stage    Stage  `json:"stage"`
	Selector string `json:"selector"`
	Steps    []Step `json:"steps"`
}

// Spec returns the steps-only document of d.
func (d Definition) Spec() Spec { return Spec{Steps: d.Steps} }

// ValidateSyntax checks the parts of a Definition that need no compilation:
// name, stage, selector and step count. Compile performs the full validation;
// this is the cheap first pass for handlers.
func (d Definition) ValidateSyntax() error {
	if !validName(d.Name) {
		return fmt.Errorf("pipeline: invalid name %q (want 1-128 bytes of [A-Za-z0-9._-], not \".\" or \"..\")", d.Name)
	}
	if !d.Stage.Valid() {
		return fmt.Errorf("pipeline: invalid stage %q", d.Stage)
	}
	if !validSelector(d.Selector) {
		return fmt.Errorf("pipeline: invalid selector %q (want \"*\" or an exact dataset name)", d.Selector)
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("pipeline: no steps")
	}
	if len(d.Steps) > MaxSteps {
		return fmt.Errorf("pipeline: %d steps exceeds the maximum of %d", len(d.Steps), MaxSteps)
	}
	return nil
}

// validName reports whether n is a legal pipeline name: 1-128 ASCII bytes of
// letters, digits, '.', '_' or '-', excluding the names "." and ".." (the
// catalog's rule).
func validName(n string) bool {
	if len(n) == 0 || len(n) > 128 || n == "." || n == ".." {
		return false
	}
	for _, c := range []byte(n) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// validSelector reports whether s is "*" or an exact sanitized dataset name
// (a fixed point of logs.SanitizeStream).
func validSelector(s string) bool {
	if s == "*" {
		return true
	}
	return s != "" && s == logs.SanitizeStream(s)
}

// DecodeSpec decodes a steps-only catalog document ({"steps":[...]}),
// rejecting unknown members and trailing data.
func DecodeSpec(b []byte) (Spec, error) {
	var s Spec
	if err := decodeDoc(b, &s); err != nil {
		return Spec{}, fmt.Errorf("pipeline: invalid spec: %w", err)
	}
	return s, nil
}

// DecodeDefinition decodes a full API definition document, rejecting unknown
// members and trailing data. Field-level validation happens in Compile.
func DecodeDefinition(b []byte) (Definition, error) {
	var d Definition
	if err := decodeDoc(b, &d); err != nil {
		return Definition{}, fmt.Errorf("pipeline: invalid definition: %w", err)
	}
	return d, nil
}

// decodeDoc decodes one JSON document into v, rejecting unknown members and
// trailing data.
func decodeDoc(b []byte, v any) error {
	return json.Unmarshal(b, v, json.RejectUnknownMembers(true))
}

// metaNameErr validates one metadata field name using the logs label-name
// rule, and rejects '__'-prefixed names, which pipelines must never write
// (they collide with Parkana internals and break LogQL metric queries).
func metaNameErr(kind, name string) error {
	if err := validLabelNameErr(name); err != nil {
		return fmt.Errorf("pipeline: %s %q: %w", kind, name, err)
	}
	if strings.HasPrefix(name, "__") {
		return fmt.Errorf("pipeline: %s %q starts with __, which is reserved", kind, name)
	}
	return nil
}
