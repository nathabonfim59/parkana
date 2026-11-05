package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"strings"
)

// queryType is the corpus entry type, which selects both the Loki endpoint and
// the response comparison policy.
type queryType string

const (
	queryRange       queryType = "range"
	queryInstant     queryType = "instant"
	queryLabels      queryType = "labels"
	queryLabelValues queryType = "label_values"
	querySeries      queryType = "series"
	queryStats       queryType = "stats"
	queryVolume      queryType = "volume"
	queryVolumeRange queryType = "volume_range"
)

// queryTypeList lists every accepted corpus entry type, in help order. It
// returns a fresh slice so callers cannot mutate package state.
func queryTypeList() []queryType {
	return []queryType{
		queryRange, queryInstant, queryLabels, queryLabelValues,
		querySeries, queryStats, queryVolume, queryVolumeRange,
	}
}

func (t queryType) valid() bool {
	switch t {
	case queryRange, queryInstant, queryLabels, queryLabelValues,
		querySeries, queryStats, queryVolume, queryVolumeRange:
		return true
	default:
		return false
	}
}

// requiresQuery reports whether the corpus entry's query field must be set.
func (t queryType) requiresQuery() bool {
	switch t {
	case queryRange, queryInstant, queryStats, queryVolume, queryVolumeRange:
		return true
	default:
		return false
	}
}

// lokiQuirk is an explicit, per-entry opt-in to a known Loki response
// deviation that Parkana deliberately does not reproduce. The zero value means
// no quirk: the default comparison stays strict.
type lokiQuirk string

const (
	// lokiQuirkScalarTimestamp accepts a LEFT scalar timestamp as a numeric
	// JSON timestamp outside the int64 nanosecond range. Loki 3.7.9 encodes
	// scalar timestamps with a millisecond-as-second unit error and then
	// overflows them through the protobuf round trip. Values still compare and
	// RIGHT (and every unopt-in entry) stays strict.
	lokiQuirkScalarTimestamp lokiQuirk = "scalar-timestamp"
)

// valid reports whether q is a supported quirk. The empty string is not a
// quirk, it is an absent field.
func (q lokiQuirk) valid() bool {
	return q == lokiQuirkScalarTimestamp
}

// corpusEntry is one request in the comparison corpus. Expect is optional and
// carries exact assertions evaluated against the RIGHT response only. LokiQuirk
// is optional and opts into one documented Loki deviation.
type corpusEntry struct {
	Name      string       `json:"name"`
	Type      queryType    `json:"type"`
	Query     string       `json:"query,omitempty"`
	Params    Params       `json:"params,omitempty"`
	LokiQuirk lokiQuirk    `json:"lokiQuirk,omitempty"`
	Expect    *expectation `json:"-"`
}

// UnmarshalJSON decodes one corpus entry and distinguishes an absent expect
// field from an explicit null. Unknown expectation members are rejected by the
// expectation parser before any request runs. lokiQuirk must be a JSON string
// naming a supported quirk; null and unknown values are rejected here, before
// any HTTP request.
func (e *corpusEntry) UnmarshalJSON(data []byte) error {
	type entryJSON struct {
		Name      string         `json:"name"`
		Type      queryType      `json:"type"`
		Query     string         `json:"query,omitempty"`
		Params    Params         `json:"params,omitempty"`
		LokiQuirk jsontext.Value `json:"lokiQuirk"`
		Expect    jsontext.Value `json:"expect"`
	}
	var raw entryJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.Name, e.Type, e.Query, e.Params, e.Expect = raw.Name, raw.Type, raw.Query, raw.Params, nil
	switch raw.LokiQuirk.Kind() {
	case jsontext.KindInvalid:
		e.LokiQuirk = ""
	case jsontext.KindString:
		var name string
		if err := json.Unmarshal(raw.LokiQuirk, &name); err != nil {
			return fmt.Errorf("lokiQuirk: %w", err)
		}
		q := lokiQuirk(name)
		if !q.valid() {
			return fmt.Errorf("lokiQuirk %q is not supported (want %q)", name, lokiQuirkScalarTimestamp)
		}
		e.LokiQuirk = q
	default:
		return fmt.Errorf("lokiQuirk must be a string, got %s", raw.LokiQuirk.Kind())
	}
	switch raw.Expect.Kind() {
	case jsontext.KindInvalid:
		return nil
	case jsontext.KindNull:
		return errors.New("expect must not be null")
	default:
		exp, err := parseExpectation(raw.Expect)
		if err != nil {
			return fmt.Errorf("expect: %w", err)
		}
		e.Expect = exp
		return nil
	}
}

// Params holds query parameters for a corpus entry. A JSON value may be a
// string or an array of strings; arrays preserve repeated parameters such as
// match[].
type Params map[string][]string

// UnmarshalJSON accepts both {"k":"v"} and {"k":["v","w"]}.
func (p *Params) UnmarshalJSON(data []byte) error {
	var raw map[string]jsontext.Value
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	params := make(Params, len(raw))
	for k, v := range raw {
		switch v.Kind() {
		case jsontext.KindString:
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return fmt.Errorf("param %q: %w", k, err)
			}
			params[k] = []string{s}
		case jsontext.KindBeginArray:
			var values []string
			if err := json.Unmarshal(v, &values); err != nil {
				return fmt.Errorf("param %q: want a string or an array of strings: %w", k, err)
			}
			params[k] = values
		default:
			return fmt.Errorf("param %q: want a string or an array of strings", k)
		}
	}
	*p = params
	return nil
}

// validate checks the schema-level rules that do not depend on CLI flags.
func (e *corpusEntry) validate(index int) error {
	if strings.TrimSpace(e.Name) == "" {
		return fmt.Errorf("entry %d: missing name", index)
	}
	if !e.Type.valid() {
		return fmt.Errorf("entry %d (%q): unknown type %q; want one of %v", index, e.Name, e.Type, queryTypeList())
	}
	if e.Type.requiresQuery() && strings.TrimSpace(e.Query) == "" {
		return fmt.Errorf("entry %d (%q): type %s requires query", index, e.Name, e.Type)
	}
	if _, ok := e.Params["query"]; ok {
		return fmt.Errorf("entry %d (%q): put the query in the query field, not params.query", index, e.Name)
	}
	if e.Type == queryLabelValues {
		labels := e.Params["label"]
		if len(labels) != 1 || strings.TrimSpace(labels[0]) == "" {
			return fmt.Errorf("entry %d (%q): label_values requires params.label with one non-empty label name", index, e.Name)
		}
	}
	if err := validateExpectation(e.Type, e.Expect); err != nil {
		return fmt.Errorf("entry %d (%q): expect: %w", index, e.Name, err)
	}
	if e.LokiQuirk != "" {
		if e.Type != queryInstant {
			return fmt.Errorf("entry %d (%q): lokiQuirk is only valid on instant entries, not %s", index, e.Name, e.Type)
		}
		if e.Expect != nil {
			return fmt.Errorf("entry %d (%q): lokiQuirk must not be combined with expect", index, e.Name)
		}
	}
	return nil
}

// loadCorpus reads and validates a corpus JSON file.
func loadCorpus(path string) ([]corpusEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read corpus: %w", err)
	}
	var entries []corpusEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse corpus %s: %w", path, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("corpus %s is empty", path)
	}
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		if err := entries[i].validate(i); err != nil {
			return nil, fmt.Errorf("corpus %s: %w", path, err)
		}
		if _, ok := seen[entries[i].Name]; ok {
			return nil, fmt.Errorf("corpus %s: duplicate entry name %q", path, entries[i].Name)
		}
		seen[entries[i].Name] = struct{}{}
	}
	return entries, nil
}
