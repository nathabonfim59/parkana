package main

import (
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

func testBounds() timeBounds {
	return timeBounds{
		start: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC),
		end:   time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC),
	}
}

func TestBuildSpec(t *testing.T) {
	tests := []struct {
		name       string
		entry      corpusEntry
		tenant     tenant.ID
		wantPath   string
		wantQuery  map[string][]string
		wantHeader map[string]string
	}{
		{
			name: "range",
			entry: corpusEntry{
				Name: "r", Type: queryRange, Query: `{app="api"} |= "x"`,
				Params: Params{"limit": {"100"}, "direction": {"backward"}},
			},
			wantPath: "/loki/api/v1/query_range",
			wantQuery: map[string][]string{
				"query":     {`{app="api"} |= "x"`},
				"limit":     {"100"},
				"direction": {"backward"},
				"start":     {"2026-11-04T10:00:00Z"},
				"end":       {"2026-11-04T11:00:00Z"},
			},
			wantHeader: map[string]string{encodingFlagsHeader: categorizeLabels},
		},
		{
			name: "instant uses end as time",
			entry: corpusEntry{
				Name: "i", Type: queryInstant, Query: `sum(count_over_time({app="api"}[5m]))`,
			},
			wantPath: "/loki/api/v1/query",
			wantQuery: map[string][]string{
				"query": {`sum(count_over_time({app="api"}[5m]))`},
				"time":  {"2026-11-04T11:00:00Z"},
			},
			wantHeader: map[string]string{encodingFlagsHeader: categorizeLabels},
		},
		{
			name: "instant time override",
			entry: corpusEntry{
				Name: "i", Type: queryInstant, Query: "1",
				Params: Params{"time": {"1700000000123456789"}},
			},
			wantPath: "/loki/api/v1/query",
			wantQuery: map[string][]string{
				"query": {"1"},
				"time":  {"2023-11-14T22:13:20.123456789Z"},
			},
			wantHeader: map[string]string{encodingFlagsHeader: categorizeLabels},
		},
		{
			name: "labels with selector",
			entry: corpusEntry{
				Name: "l", Type: queryLabels, Query: `{app="api"}`,
			},
			wantPath: "/loki/api/v1/labels",
			wantQuery: map[string][]string{
				"query": {`{app="api"}`},
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "label values escapes path and consumes label",
			entry: corpusEntry{
				Name: "lv", Type: queryLabelValues, Query: `{app="api"}`,
				Params: Params{"label": {"app/zone"}},
			},
			wantPath: "/loki/api/v1/label/app%2Fzone/values",
			wantQuery: map[string][]string{
				"query": {`{app="api"}`},
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "series keeps multiple match selectors",
			entry: corpusEntry{
				Name: "s", Type: querySeries, Query: `{app="api"}`,
				Params: Params{"match[]": {`{app="web"}`}},
			},
			wantPath: "/loki/api/v1/series",
			wantQuery: map[string][]string{
				"match[]": {`{app="api"}`, `{app="web"}`},
				"start":   {"2026-11-04T10:00:00Z"},
				"end":     {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "stats",
			entry: corpusEntry{
				Name: "st", Type: queryStats, Query: `{app="api"}`,
			},
			wantPath: "/loki/api/v1/index/stats",
			wantQuery: map[string][]string{
				"query": {`{app="api"}`},
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "volume",
			entry: corpusEntry{
				Name: "v", Type: queryVolume, Query: `{app="api"}`,
				Params: Params{"limit": {"10"}},
			},
			wantPath: "/loki/api/v1/index/volume",
			wantQuery: map[string][]string{
				"query": {`{app="api"}`},
				"limit": {"10"},
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "volume range",
			entry: corpusEntry{
				Name: "vr", Type: queryVolumeRange, Query: `{app="api"}`,
				Params: Params{"step": {"60"}},
			},
			wantPath: "/loki/api/v1/index/volume_range",
			wantQuery: map[string][]string{
				"query": {`{app="api"}`},
				"step":  {"60"},
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
		},
		{
			name: "params timestamps override defaults",
			entry: corpusEntry{
				Name: "r", Type: queryRange, Query: "{}",
				Params: Params{
					"start": {"1700000000000000000"},
					"end":   {"2023-11-14T22:13:21Z"},
				},
			},
			wantPath: "/loki/api/v1/query_range",
			wantQuery: map[string][]string{
				"query": {"{}"},
				"start": {"2023-11-14T22:13:20Z"},
				"end":   {"2023-11-14T22:13:21Z"},
			},
			wantHeader: map[string]string{encodingFlagsHeader: categorizeLabels},
		},
		{
			name: "tenant header",
			entry: corpusEntry{
				Name: "l", Type: queryLabels,
			},
			tenant:   "team-a",
			wantPath: "/loki/api/v1/labels",
			wantQuery: map[string][]string{
				"start": {"2026-11-04T10:00:00Z"},
				"end":   {"2026-11-04T11:00:00Z"},
			},
			wantHeader: map[string]string{tenant.Header: "team-a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := buildSpec(tc.entry, testBounds(), tc.tenant)
			if err != nil {
				t.Fatalf("buildSpec: %v", err)
			}
			if spec.path != tc.wantPath {
				t.Errorf("path = %q, want %q", spec.path, tc.wantPath)
			}
			for key, want := range tc.wantQuery {
				if got := spec.query[key]; !slices.Equal(got, want) {
					t.Errorf("query[%q] = %v, want %v", key, got, want)
				}
			}
			for key, want := range tc.wantHeader {
				if got := spec.headers.Get(key); got != want {
					t.Errorf("header %q = %q, want %q", key, got, want)
				}
			}
			if spec.headers.Get("Accept") != "application/json" {
				t.Errorf("Accept = %q", spec.headers.Get("Accept"))
			}
			if got := spec.headers.Get(encodingFlagsHeader); got != "" && tc.wantHeader[encodingFlagsHeader] == "" {
				t.Errorf("unexpected encoding flags header %q", got)
			}

			rawURL := spec.url("http://example.com/prefix")
			parsed, err := url.Parse(rawURL)
			if err != nil {
				t.Fatalf("url.Parse(%q): %v", rawURL, err)
			}
			if parsed.Path != "/prefix"+decodedPath(tc.wantPath) {
				t.Errorf("URL path = %q, want prefix plus %q", parsed.Path, tc.wantPath)
			}
			for key, want := range tc.wantQuery {
				if got := parsed.Query()[key]; !slices.Equal(got, want) {
					t.Errorf("URL query[%q] = %v, want %v", key, got, want)
				}
			}
		})
	}
}

// decodedPath returns the decoded path shape used by url.URL.Path.
func decodedPath(path string) string {
	decoded, err := url.PathUnescape(path)
	if err != nil {
		return path
	}
	return decoded
}

func TestBuildSpecErrors(t *testing.T) {
	tests := []struct {
		name    string
		entry   corpusEntry
		wantErr string
	}{
		{
			name:    "invalid start",
			entry:   corpusEntry{Name: "x", Type: queryRange, Query: "{}", Params: Params{"start": {"yesterday"}}},
			wantErr: "param start",
		},
		{
			name:    "start after end",
			entry:   corpusEntry{Name: "x", Type: queryRange, Query: "{}", Params: Params{"start": {"2026-11-04T12:00:00Z"}, "end": {"2026-11-04T11:00:00Z"}}},
			wantErr: "after end",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildSpec(tc.entry, testBounds(), "")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("buildSpec error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestBuildSpecScopedLabelsUseQueryParameter(t *testing.T) {
	for _, kind := range []queryType{queryLabels, queryLabelValues} {
		t.Run(string(kind), func(t *testing.T) {
			entry := corpusEntry{Name: "scope", Type: kind, Query: `{app="api"}`}
			if kind == queryLabelValues {
				entry.Params = Params{"label": {"app"}}
			}
			spec, err := buildSpec(entry, testBounds(), "")
			if err != nil {
				t.Fatalf("buildSpec: %v", err)
			}
			if got := spec.query.Get("query"); got != `{app="api"}` {
				t.Errorf("query = %q, want the scope selector", got)
			}
			if got := spec.query["match[]"]; len(got) != 0 {
				t.Errorf("match[] = %v, want none (only series uses match[])", got)
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Time
		wantErr bool
	}{
		{input: "1700000000123456789", want: time.Unix(0, 1700000000123456789)},
		{input: "2026-11-04T10:00:00Z", want: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)},
		{input: "2026-11-04T10:00:00.123456789Z", want: time.Date(2026, 11, 4, 10, 0, 0, 123456789, time.UTC)},
		{input: "", wantErr: true},
		{input: "not-a-time", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseTime(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseTime(%q) succeeded", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTime(%q): %v", tc.input, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseTime(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeBase(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{input: "http://localhost:3100", want: "http://localhost:3100"},
		{input: "https://loki.example.com/", want: "https://loki.example.com"},
		{input: "https://example.com/loki/", want: "https://example.com/loki"},
		{input: "ftp://example.com", wantErr: true},
		{input: "example.com", wantErr: true},
		{input: "http://example.com/?x=1", wantErr: true},
		{input: "http://example.com/#frag", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeBase(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalizeBase(%q) succeeded", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeBase(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("normalizeBase(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
