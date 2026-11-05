package main

import (
	"encoding/json/v2"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLoadExampleCorpus(t *testing.T) {
	entries, err := loadCorpus("testdata/example-corpus.json")
	if err != nil {
		t.Fatalf("loadCorpus: %v", err)
	}
	var types []queryType
	for _, entry := range entries {
		types = append(types, entry.Type)
	}
	if len(types) != len(queryTypeList()) {
		t.Fatalf("example corpus has %d entries, want one per type (%d)", len(types), len(queryTypeList()))
	}
	for _, want := range queryTypeList() {
		if !slices.Contains(types, want) {
			t.Errorf("example corpus is missing type %s", want)
		}
	}
}

func TestLoadGrafanaCorpus(t *testing.T) {
	entries, err := loadCorpus("testdata/grafana-corpus.json")
	if err != nil {
		t.Fatalf("loadCorpus: %v", err)
	}
	if len(entries) != featureCorpusEntries {
		t.Fatalf("grafana corpus has %d entries, want %d", len(entries), featureCorpusEntries)
	}
	var instant corpusEntry
	found := false
	for _, e := range entries {
		if e.Name == "instant-log-selector-rejected" {
			instant, found = e, true
		}
	}
	if !found {
		t.Fatal("grafana corpus is missing instant-log-selector-rejected")
	}
	if instant.Type != queryInstant || instant.Query != `{service_name="api"}` {
		t.Fatalf("instant-log-selector-rejected = %+v, want an instant {service_name=\"api\"} entry", instant)
	}
	if instant.Expect == nil || instant.Expect.status == nil || *instant.Expect.status != 400 {
		t.Fatalf("instant-log-selector-rejected expect = %+v, want status 400", instant.Expect)
	}
}

// TestGrafanaCorpusPinsCapturedForms asserts the exact query expressions and
// parameters the Grafana browser capture produced, on both the corpus entry
// and the request built from it.
func TestGrafanaCorpusPinsCapturedForms(t *testing.T) {
	entries, err := loadCorpus("testdata/grafana-corpus.json")
	if err != nil {
		t.Fatalf("loadCorpus: %v", err)
	}
	byName := make(map[string]corpusEntry, len(entries))
	for _, e := range entries {
		byName[e.Name] = e
	}
	tests := []struct {
		name  string
		query string
		step  string
	}{
		{"logs-base-api-range", `{service_name="api"}`, "5s"},
		{"grafana-histogram-api", `sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))`, "1m"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := byName[tc.name]
			if !ok {
				t.Fatalf("grafana corpus is missing %s", tc.name)
			}
			if entry.Query != tc.query {
				t.Fatalf("query = %q, want %q", entry.Query, tc.query)
			}
			for _, tc := range []struct{ param, want string }{
				{"step", tc.step},
				{"direction", "backward"},
				{"limit", "1000"},
			} {
				if got := entry.Params[tc.param]; !slices.Equal(got, []string{tc.want}) {
					t.Fatalf("params %s = %v, want [%s]", tc.param, got, tc.want)
				}
			}
			spec, err := buildSpec(entry, timeBounds{}, "")
			if err != nil {
				t.Fatalf("buildSpec: %v", err)
			}
			if spec.path != apiPrefix+"/query_range" {
				t.Errorf("path = %q, want %s/query_range", spec.path, apiPrefix)
			}
			for _, tc := range []struct{ param, want string }{
				{"query", tc.query},
				{"step", tc.step},
				{"direction", "backward"},
				{"limit", "1000"},
			} {
				if got := spec.query.Get(tc.param); got != tc.want {
					t.Errorf("wire %s = %q, want %q", tc.param, got, tc.want)
				}
			}
		})
	}
}

func TestQueryTypeListIsNotSharedState(t *testing.T) {
	first := queryTypeList()
	first[0] = "mutated"
	if !queryRange.valid() {
		t.Fatal("mutating a returned type list changed validation")
	}
	second := queryTypeList()
	if second[0] != queryRange {
		t.Fatalf("queryTypeList()[0] = %q, want %q", second[0], queryRange)
	}
}

func TestParamsUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Params
		wantErr string
	}{
		{
			name:  "string and array",
			input: `{"a":"b","c":["d","e"]}`,
			want:  Params{"a": {"b"}, "c": {"d", "e"}},
		},
		{
			name:  "empty object",
			input: `{}`,
			want:  Params{},
		},
		{
			name:    "number rejected",
			input:   `{"a":1}`,
			wantErr: "string or an array",
		},
		{
			name:    "mixed array rejected",
			input:   `{"a":["b",1]}`,
			wantErr: "array of strings",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got Params
			err := got.UnmarshalJSON([]byte(tc.input))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("UnmarshalJSON error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalJSON: %v", err)
			}
			if !slices.Equal(slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(tc.want))) {
				t.Fatalf("params keys = %v, want %v", got, tc.want)
			}
			for key, want := range tc.want {
				if !slices.Equal(got[key], want) {
					t.Errorf("params[%q] = %v, want %v", key, got[key], want)
				}
			}
		})
	}
}

func TestCorpusValidation(t *testing.T) {
	tests := []struct {
		name    string
		entries []corpusEntry
		wantErr string
	}{
		{
			name:    "missing name",
			entries: []corpusEntry{{Type: queryLabels}},
			wantErr: "missing name",
		},
		{
			name:    "unknown type",
			entries: []corpusEntry{{Name: "x", Type: "nope"}},
			wantErr: "unknown type",
		},
		{
			name:    "range needs query",
			entries: []corpusEntry{{Name: "x", Type: queryRange}},
			wantErr: "requires query",
		},
		{
			name:    "stats needs query",
			entries: []corpusEntry{{Name: "x", Type: queryStats}},
			wantErr: "requires query",
		},
		{
			name:    "params query rejected",
			entries: []corpusEntry{{Name: "x", Type: queryRange, Query: "{}", Params: Params{"query": {"{}"}}}},
			wantErr: "query field",
		},
		{
			name:    "params query rejected for labels",
			entries: []corpusEntry{{Name: "x", Type: queryLabels, Query: "{}", Params: Params{"query": {"{}"}}}},
			wantErr: "query field",
		},
		{
			name:    "label_values label missing",
			entries: []corpusEntry{{Name: "x", Type: queryLabelValues}},
			wantErr: "params.label",
		},
		{
			name:    "label_values repeated label",
			entries: []corpusEntry{{Name: "x", Type: queryLabelValues, Params: Params{"label": {"a", "b"}}}},
			wantErr: "params.label",
		},
		{
			name:    "labels query optional",
			entries: []corpusEntry{{Name: "x", Type: queryLabels}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCorpusFile(t, tc.entries)
			_, err := loadCorpus(path)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("loadCorpus: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("loadCorpus error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadCorpusErrors(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		if _, err := loadCorpus(filepath.Join(t.TempDir(), "missing.json")); err == nil {
			t.Fatal("loadCorpus succeeded for a missing file")
		}
	})
	t.Run("malformed JSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.json")
		if err := os.WriteFile(path, []byte(`[`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadCorpus(path); err == nil {
			t.Fatal("loadCorpus succeeded for malformed JSON")
		}
	})
	t.Run("empty array", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.json")
		if err := os.WriteFile(path, []byte(`[]`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadCorpus(path); err == nil {
			t.Fatal("loadCorpus succeeded for an empty corpus")
		}
	})
	t.Run("duplicate name", func(t *testing.T) {
		path := writeCorpusFile(t, []corpusEntry{
			{Name: "same", Type: queryLabels},
			{Name: "same", Type: queryLabels},
		})
		if _, err := loadCorpus(path); err == nil || !strings.Contains(err.Error(), "duplicate entry name") {
			t.Fatalf("loadCorpus error = %v, want duplicate name", err)
		}
	})
}

func writeCorpusFile(t *testing.T, entries []corpusEntry) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corpus.json")
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
