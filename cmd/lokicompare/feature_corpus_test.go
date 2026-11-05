package main

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/cmd/pushgen/gen"
	logqlconfig "github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// featureCorpusEntries is the total entry count of the checked-in Grafana
// corpus: the 18 historical entries plus the A12 feature matrix.
const featureCorpusEntries = 112

// The pushed fixture totals below are literal expectations. Tests recompute
// them from the public generator and the documented pushgen commands, so a
// change to the generator or to the three approved datasets fails here before
// it can silently invalidate the corpus expectations.
const (
	fixtureBasePerStreamEntries = 10000
	fixtureBasePerStreamBytes   = 184445
	fixtureBaseEntries          = 20000
	fixtureBaseBytes            = 368890
	fixtureLegacyEntries        = 1000
	fixtureLegacyBytes          = 19890
	fixtureCorpusEntries        = 1200
	fixtureCorpusBytes          = 164711
	fixtureCorpusJSONEntries    = 423
	fixtureCorpusLogfmtEntries  = 384
	fixtureCorpusPlainEntries   = 393
	fixtureCorpusStreams        = 18
	fixtureCorpusErrorEntries   = 339
	fixtureCorpusInfoEntries    = 448
	fixtureCorpusWarnEntries    = 413
	fixtureAllEntries           = 22200
	fixtureAllBytes             = 553491
	fixtureNonAPIWebEntries     = 2200
	fixtureNonAPIWebBytes       = 184601
	fixtureNonAPIWebStreams     = 19
	fixtureNonemptyEntries      = 21200
	fixtureNonemptyBytes        = 533601
	fixtureNonemptyStreams      = 20
)

// fixtureTotals holds the source-derived byte and entry totals of the three
// approved pushgen datasets.
type fixtureTotals struct {
	baseAPIEntries int64
	baseAPIBytes   int64
	baseWebEntries int64
	baseWebBytes   int64
	legacyEntries  int64
	legacyBytes    int64
	corpusEntries  int64
	corpusBytes    int64
	// byFormatLevel sums corpus line bytes by "format/level".
	byFormatLevel map[string]int64
	// byStream sums corpus line bytes by "format/level/pod".
	byStream map[string]int64
	// formatEntries counts corpus records per line format.
	formatEntries map[string]int64
	// levelEntries counts corpus records per level.
	levelEntries map[string]int64
}

// loadFixtureTotals recomputes every literal total from the fixture
// definition. Base and legacy lines are rebuilt from the documented pushgen
// templates; corpus lines come from the public gen.NewCorpusRecord API.
func loadFixtureTotals(t *testing.T) fixtureTotals {
	t.Helper()
	f := fixtureTotals{
		byFormatLevel: make(map[string]int64),
		byStream:      make(map[string]int64),
		formatEntries: make(map[string]int64),
		levelEntries:  make(map[string]int64),
	}
	for i := range 2 * fixtureBasePerStreamEntries {
		name := "api"
		if i%2 == 1 {
			name = "web"
		}
		line := fmt.Sprintf("id=%d stream=%s", i, name)
		if name == "api" {
			f.baseAPIEntries++
			f.baseAPIBytes += int64(len(line))
			continue
		}
		f.baseWebEntries++
		f.baseWebBytes += int64(len(line))
	}
	for i := range fixtureLegacyEntries {
		f.legacyEntries++
		f.legacyBytes += int64(len(fmt.Sprintf("id=%d stream=legacy", i)))
	}
	for i := range fixtureCorpusEntries {
		record := gen.NewCorpusRecord(i, "corpus")
		line, err := record.Line()
		if err != nil {
			t.Fatalf("corpus record %d: %v", i, err)
		}
		f.corpusEntries++
		f.formatEntries[record.Format]++
		f.levelEntries[record.Level]++
		f.corpusBytes += int64(len(line))
		f.byFormatLevel[record.Format+"/"+record.Level] += int64(len(line))
		f.byStream[record.Format+"/"+record.Level+"/"+record.Pod] += int64(len(line))
	}
	return f
}

// corpusEntriesByName loads the checked-in corpus and indexes it by entry name.
func corpusEntriesByName(t *testing.T) ([]corpusEntry, map[string]corpusEntry) {
	t.Helper()
	entries, err := loadCorpus("testdata/grafana-corpus.json")
	if err != nil {
		t.Fatalf("loadCorpus: %v", err)
	}
	if len(entries) != featureCorpusEntries {
		t.Fatalf("grafana corpus has %d entries, want %d", len(entries), featureCorpusEntries)
	}
	byName := make(map[string]corpusEntry, len(entries))
	for _, entry := range entries {
		if _, ok := byName[entry.Name]; ok {
			t.Fatalf("duplicate entry name %q", entry.Name)
		}
		byName[entry.Name] = entry
	}
	return entries, byName
}

// TestFeatureCorpusBuildsEveryEntry loads the corpus and builds a request for
// every entry with the CLI's default bounds, checking the endpoint each type
// reaches.
func TestFeatureCorpusBuildsEveryEntry(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	bounds := timeBounds{
		start: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC),
		end:   time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC),
	}
	wantPaths := map[queryType]string{
		queryRange:       apiPrefix + "/query_range",
		queryInstant:     apiPrefix + "/query",
		queryLabels:      apiPrefix + "/labels",
		querySeries:      apiPrefix + "/series",
		queryStats:       apiPrefix + "/index/stats",
		queryVolume:      apiPrefix + "/index/volume",
		queryVolumeRange: apiPrefix + "/index/volume_range",
	}
	for _, entry := range entries {
		t.Run(entry.Name, func(t *testing.T) {
			spec, err := buildSpec(entry, bounds, "default")
			if err != nil {
				t.Fatalf("buildSpec: %v", err)
			}
			if entry.Type == queryLabelValues {
				label := entry.Params["label"][0]
				if want := apiPrefix + "/label/" + label + "/values"; spec.path != want {
					t.Fatalf("path = %q, want %q", spec.path, want)
				}
			} else if want := wantPaths[entry.Type]; spec.path != want {
				t.Fatalf("path = %q, want %q", spec.path, want)
			}
			if entry.Type.requiresQuery() {
				if got := spec.query.Get("query"); got != entry.Query {
					t.Fatalf("wire query = %q, want %q", got, entry.Query)
				}
			}
			if entry.Type == queryInstant {
				if got := spec.query.Get("time"); got != formatTime(bounds.end) {
					t.Fatalf("wire time = %q, want %q", got, formatTime(bounds.end))
				}
			}
		})
	}
}

// TestFeatureCorpusPreservesHistoricalEntries pins the 18 historical entries
// that predate the feature expansion. Only logs-base-api-range and
// grafana-histogram-api are actual captured requests; the rest were authored
// from Loki/plugin source evidence. The test pins their names, order, types,
// queries, params and status expectation, and the two captured forms pinned by
// TestGrafanaCorpusPinsCapturedForms remain a subset of this table.
func TestFeatureCorpusPreservesHistoricalEntries(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	if len(entries) < 18 {
		t.Fatalf("corpus has %d entries, want at least the 18 historical ones", len(entries))
	}
	want := []corpusEntry{
		{Name: "logs-base-api-range", Type: queryRange, Query: `{service_name="api"}`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5s"}}},
		{Name: "logs-base-web-line-filter", Type: queryRange, Query: `{service_name="web"} |= "stream=web"`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5000ms"}}},
		{Name: "logs-job-only-legacy", Type: queryRange, Query: `{job="legacy"}`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5000ms"}}},
		{Name: "logs-corpus-json-parser-filter", Type: queryRange, Query: `{service_name="corpus", format="json"} | json status="response.status" | status >= 400`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5000ms"}}},
		{Name: "logs-corpus-logfmt-line-filter", Type: queryRange, Query: `{service_name="corpus", format="logfmt"} |= "status=5"`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5000ms"}}},
		{Name: "logs-corpus-plain-regexp", Type: queryRange, Query: `{service_name="corpus", format="plain"} |~ "ERROR pod-[ab] status=5[0-9][0-9]"`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"5000ms"}}},
		{Name: "count-instant-by-level", Type: queryInstant, Query: `sum by (level) (count_over_time({service_name="corpus"}[5m]))`, Params: Params{"direction": {"backward"}, "limit": {"1000"}}},
		{Name: "grafana-histogram-api", Type: queryRange, Query: `sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"1m"}}},
		{Name: "grafana-histogram-corpus-json", Type: queryRange, Query: `sum by (level, detected_level) (count_over_time({service_name="corpus", format="json"} | json | drop __error__ [1m]))`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"60000ms"}}},
		{Name: "grafana-histogram-corpus-logfmt", Type: queryRange, Query: `sum by (level, detected_level) (count_over_time({service_name="corpus", format="logfmt"} | logfmt | drop __error__ [1m]))`, Params: Params{"direction": {"backward"}, "limit": {"1000"}, "step": {"60000ms"}}},
		{Name: "grafana-labels-scoped", Type: queryLabels, Query: `{service_name="corpus"}`, Params: nil},
		{Name: "grafana-labels-unscoped", Type: queryLabels, Query: "", Params: nil},
		{Name: "grafana-label-values-service-name", Type: queryLabelValues, Query: `{job="corpus"}`, Params: Params{"label": {"service_name"}}},
		{Name: "grafana-label-values-format-scoped", Type: queryLabelValues, Query: `{service_name="corpus", level="error"}`, Params: Params{"label": {"format"}}},
		{Name: "grafana-series-corpus", Type: querySeries, Query: `{service_name="corpus"}`, Params: nil},
		{Name: "series-job-only-legacy", Type: querySeries, Query: `{job="legacy"}`, Params: nil},
		{Name: "series-multiple-selectors", Type: querySeries, Query: `{service_name="api"}`, Params: Params{"match[]": {`{service_name="web"}`, `{job="legacy"}`}}},
		{Name: "instant-log-selector-rejected", Type: queryInstant, Query: `{service_name="api"}`, Params: nil},
	}
	if len(want) != 18 {
		t.Fatalf("preserved entry table has %d rows, want 18", len(want))
	}
	for i, wantEntry := range want {
		got := entries[i]
		if got.Name != wantEntry.Name {
			t.Fatalf("entry %d name = %q, want %q (historical order changed)", i, got.Name, wantEntry.Name)
		}
		if got.Type != wantEntry.Type || got.Query != wantEntry.Query {
			t.Errorf("entry %q = type %s query %q, want type %s query %q",
				got.Name, got.Type, got.Query, wantEntry.Type, wantEntry.Query)
		}
		if !maps.EqualFunc(got.Params, wantEntry.Params, slices.Equal) {
			t.Errorf("entry %q params = %v, want %v", got.Name, got.Params, wantEntry.Params)
		}
	}
	if got := entries[17].Expect; got == nil || got.status == nil || *got.status != 400 {
		t.Fatalf("instant-log-selector-rejected expect = %+v, want status 400", got)
	}
	if got := entries[17].Expect; got != nil && (got.stats != nil || got.seriesSet) {
		t.Fatalf("instant-log-selector-rejected expect = %+v, want status only", got)
	}
}

// TestFeatureCorpusFamilyCoverage asserts that each approved feature family is
// represented by at least one query, so a future edit cannot silently drop a
// family while the corpus still loads.
func TestFeatureCorpusFamilyCoverage(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	required := map[string]string{
		"selector not equal":       `service_name!="api"`,
		"selector regexp":          `=~`,
		"selector not regexp":      `!~`,
		"line contains":            `|= "timed out"`,
		"line not contains":        `!= "timed out"`,
		"line regexp":              `|~ "status=`,
		"line not regexp":          `!~ "status=`,
		"line or alternatives":     ` or "invalid token"`,
		"json flat":                `| json | latency >`,
		"json named path":          `status="response.status"`,
		"logfmt permissive":        `| logfmt | level = "warn"`,
		"logfmt strict":            `| logfmt --strict`,
		"logfmt keep-empty":        `| logfmt --keep-empty`,
		"logfmt strict keep-empty": `| logfmt --strict --keep-empty`,
		"regexp captures":          `| regexp "(`,
		"pattern captures":         `| pattern "<`,
		"string filter":            `level="error" or level="warn"`,
		"number filter":            `latency > 100.5`,
		"duration filter":          `duration > 1s`,
		"bytes filter":             `size >= 1MiB`,
		"boolean filter":           `) and status >= 400`,
		"string regexp filter":     `msg =~ "cache .*"`,
		"line_format":              `line_format "{{`,
		"label_format":             `label_format `,
		"drop names":               `| drop __error__`,
		"drop matcher":             `| drop level="info"`,
		"keep names":               `| keep level, pod`,
		"keep matcher":             `| keep level, pod="pod-a"`,
		"unwrap numeric":           `| unwrap latency`,
		"unwrap duration":          `| unwrap duration(`,
		"unwrap duration_seconds":  `| unwrap duration_seconds(`,
		"unwrap bytes":             `| unwrap bytes(`,
		"unwrap captured":          `| unwrap lat`,
		"count_over_time":          `count_over_time(`,
		"rate":                     `rate(`,
		"bytes_over_time":          `bytes_over_time(`,
		"bytes_rate":               `bytes_rate(`,
		"sum_over_time":            `sum_over_time(`,
		"avg_over_time":            `avg_over_time(`,
		"min_over_time":            `min_over_time(`,
		"max_over_time":            `max_over_time(`,
		"stddev_over_time":         `stddev_over_time(`,
		"stdvar_over_time":         `stdvar_over_time(`,
		"first_over_time":          `first_over_time(`,
		"last_over_time":           `last_over_time(`,
		"quantile_over_time":       `quantile_over_time(`,
		"absent_over_time":         `absent_over_time(`,
		"offset positive":          `offset 10s`,
		"offset negative":          `offset -10s`,
		"sum by":                   `sum by (`,
		"avg by":                   `avg by (`,
		"min by":                   `min by (`,
		"max by":                   `max by (`,
		"count by":                 `count by (`,
		"stddev by":                `stddev by (`,
		"stdvar by":                `stdvar by (`,
		"without":                  ` without (`,
		"topk":                     `topk(`,
		"bottomk":                  `bottomk(`,
		"sort":                     `sort(`,
		"sort_desc":                `sort_desc(`,
		"scalar precedence":        `1 + 2 * 3`,
		"scalar parentheses":       `(1 + 2) * 3`,
		"scalar power":             `2 ^ 3 ^ 2`,
		"bool comparison":          `bool`,
		"scalar-vector left":       `2 * count_over_time(`,
		"scalar-vector right":      `/ 2`,
		"scalar-vector compare":    `10 < count_over_time(`,
		"vector-vector arithmetic": ` + sum by (`,
		"health vector":            `vector(1) + vector(1)`,
	}
	for family, fragment := range required {
		if !slices.ContainsFunc(entries, func(entry corpusEntry) bool {
			return strings.Contains(entry.Query, fragment)
		}) {
			t.Errorf("family %q: no corpus query contains %q", family, fragment)
		}
	}
}

// TestFeatureCorpusHealthVector pins the two constant health probes: an
// instant and a one-second-step range query, both independent of generation
// timestamps.
func TestFeatureCorpusHealthVector(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	found := make(map[queryType]corpusEntry)
	for _, entry := range entries {
		if entry.Query == `vector(1) + vector(1)` {
			found[entry.Type] = entry
		}
	}
	for _, kind := range []queryType{queryInstant, queryRange} {
		entry, ok := found[kind]
		if !ok {
			t.Fatalf("missing %s health vector query", kind)
		}
		if want := "health-vector-binary-" + string(kind); entry.Name != want {
			t.Errorf("%s health entry name = %q, want %q", kind, entry.Name, want)
		}
	}
	if got := found[queryRange].Params["step"]; !slices.Equal(got, []string{"1s"}) {
		t.Errorf("health range step = %v, want [1s]", got)
	}
	if len(found) != 2 {
		t.Fatalf("health vector entries = %d, want exactly 2", len(found))
	}
}

// TestFeatureCorpusCardinalitySensitiveQueries pins the shapes that must not
// regress into over-limit results: tie-sensitive rank aggregations stay instant
// over a generous window (per-step topk over partial [10m] windows can select a
// different label set when level counts tie), the without() aggregation removes
// the high-cardinality structured metadata so it returns nine series rather
// than one per trace/request id, and the two comparison entries group by level
// before comparing so they stay bounded and produce nontrivial results.
func TestFeatureCorpusCardinalitySensitiveQueries(t *testing.T) {
	_, byName := corpusEntriesByName(t)
	for _, name := range []string{"agg-topk", "agg-bottomk"} {
		entry, ok := byName[name]
		if !ok {
			t.Fatalf("missing %q", name)
		}
		if entry.Type != queryInstant {
			t.Errorf("%q type = %s, want instant (no per-step topk)", name, entry.Type)
		}
		if !strings.Contains(entry.Query, "[6h]") {
			t.Errorf("%q query = %q, want the full-corpus [6h] window", name, entry.Query)
		}
	}
	entry, ok := byName["agg-without-pod"]
	if !ok {
		t.Fatal("missing agg-without-pod")
	}
	want := `sum without (pod, request_id, trace_id) (count_over_time({service_name="corpus"}[10m]))`
	if entry.Query != want {
		t.Errorf("agg-without-pod query = %q, want %q", entry.Query, want)
	}
	for _, tc := range []struct{ name, query string }{
		{"binary-filter-compare", `sum by (level) (count_over_time({service_name="corpus"}[10m])) >= 400`},
		{"binary-bool-compare", `sum by (level) (count_over_time({service_name="corpus"}[10m])) > bool 400`},
	} {
		entry, ok := byName[tc.name]
		if !ok {
			t.Fatalf("missing %q", tc.name)
		}
		if entry.Query != tc.query {
			t.Errorf("%s query = %q, want %q", tc.name, entry.Query, tc.query)
		}
	}
}

// TestFeatureCorpusFixtureTotals proves the literal pins in this file and in
// the corpus still match the generator and the documented pushgen lines.
func TestFeatureCorpusFixtureTotals(t *testing.T) {
	f := loadFixtureTotals(t)
	tests := []struct {
		name string
		got  int64
		want int64
	}{
		{"base api entries", f.baseAPIEntries, fixtureBasePerStreamEntries},
		{"base api bytes", f.baseAPIBytes, fixtureBasePerStreamBytes},
		{"base web entries", f.baseWebEntries, fixtureBasePerStreamEntries},
		{"base web bytes", f.baseWebBytes, fixtureBasePerStreamBytes},
		{"base entries", f.baseAPIEntries + f.baseWebEntries, fixtureBaseEntries},
		{"base bytes", f.baseAPIBytes + f.baseWebBytes, fixtureBaseBytes},
		{"legacy entries", f.legacyEntries, fixtureLegacyEntries},
		{"legacy bytes", f.legacyBytes, fixtureLegacyBytes},
		{"corpus entries", f.corpusEntries, fixtureCorpusEntries},
		{"corpus bytes", f.corpusBytes, fixtureCorpusBytes},
		{"corpus json entries", f.formatEntries["json"], fixtureCorpusJSONEntries},
		{"corpus logfmt entries", f.formatEntries["logfmt"], fixtureCorpusLogfmtEntries},
		{"corpus plain entries", f.formatEntries["plain"], fixtureCorpusPlainEntries},
		{"corpus streams", int64(len(f.byStream)), fixtureCorpusStreams},
		{"corpus error entries", f.levelEntries["error"], fixtureCorpusErrorEntries},
		{"corpus info entries", f.levelEntries["info"], fixtureCorpusInfoEntries},
		{"corpus warn entries", f.levelEntries["warn"], fixtureCorpusWarnEntries},
		{"all entries", f.baseAPIEntries + f.baseWebEntries + f.legacyEntries + f.corpusEntries, fixtureAllEntries},
		{"all bytes", f.baseAPIBytes + f.baseWebBytes + f.legacyBytes + f.corpusBytes, fixtureAllBytes},
		{"service_name !~ api|web entries", f.corpusEntries + f.legacyEntries, fixtureNonAPIWebEntries},
		{"service_name !~ api|web bytes", f.corpusBytes + f.legacyBytes, fixtureNonAPIWebBytes},
		{"nonempty-service_name entries", f.baseAPIEntries + f.baseWebEntries + f.corpusEntries, fixtureNonemptyEntries},
		{"nonempty-service_name bytes", f.baseAPIBytes + f.baseWebBytes + f.corpusBytes, fixtureNonemptyBytes},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// TestFeatureCorpusStatsExpectations checks each index/stats entry carries the
// exact RIGHT streams/entries/bytes over the full uploaded range.
func TestFeatureCorpusStatsExpectations(t *testing.T) {
	entries, byName := corpusEntriesByName(t)
	f := loadFixtureTotals(t)
	type pinnedStats struct {
		query string
		stats statsExpectation
	}
	want := map[string]pinnedStats{
		"stats-service-api": {query: `{service_name="api"}`, stats: statsExpectation{
			streams: new(int64(1)), entries: new(f.baseAPIEntries), bytes: new(f.baseAPIBytes)}},
		"stats-job-frontend": {query: `{job="frontend"}`, stats: statsExpectation{
			streams: new(int64(2)), entries: new(f.baseAPIEntries + f.baseWebEntries), bytes: new(f.baseAPIBytes + f.baseWebBytes)}},
		"stats-job-legacy": {query: `{job="legacy"}`, stats: statsExpectation{
			streams: new(int64(1)), entries: new(f.legacyEntries), bytes: new(f.legacyBytes)}},
		"stats-job-corpus": {query: `{job="corpus"}`, stats: statsExpectation{
			streams: new(int64(18)), entries: new(f.corpusEntries), bytes: new(f.corpusBytes)}},
		// The negative selector needs the positive env="dev" matcher and
		// matches the legacy stream whose service_name is missing.
		"stats-not-regexp-missing-env": {query: `{env="dev", service_name!~"api|web"}`, stats: statsExpectation{
			streams: new(int64(fixtureNonAPIWebStreams)), entries: new(f.corpusEntries + f.legacyEntries), bytes: new(f.corpusBytes + f.legacyBytes)}},
		"stats-env-dev": {query: `{env="dev"}`, stats: statsExpectation{
			streams: new(int64(21)), entries: new(f.baseAPIEntries + f.baseWebEntries + f.legacyEntries + f.corpusEntries), bytes: new(f.baseAPIBytes + f.baseWebBytes + f.legacyBytes + f.corpusBytes)}},
		"stats-service-regexp-nonempty": {query: `{service_name=~".+"}`, stats: statsExpectation{
			streams: new(int64(fixtureNonemptyStreams)), entries: new(f.baseAPIEntries + f.baseWebEntries + f.corpusEntries), bytes: new(f.baseAPIBytes + f.baseWebBytes + f.corpusBytes)}},
	}
	seen := 0
	for _, entry := range entries {
		if entry.Type != queryStats {
			continue
		}
		seen++
		wantStats, ok := want[entry.Name]
		if !ok {
			t.Errorf("stats entry %q has no pinned expectation", entry.Name)
			continue
		}
		if entry.Query != wantStats.query {
			t.Errorf("stats entry %q query = %q, want %q", entry.Name, entry.Query, wantStats.query)
		}
		if entry.Expect == nil || entry.Expect.stats == nil {
			t.Errorf("stats entry %q has no expect.stats", entry.Name)
			continue
		}
		got := entry.Expect.stats
		if !equalInt64Ptr(got.streams, wantStats.stats.streams) ||
			!equalInt64Ptr(got.entries, wantStats.stats.entries) ||
			!equalInt64Ptr(got.bytes, wantStats.stats.bytes) {
			t.Errorf("stats entry %q = streams %s entries %s bytes %s, want streams %s entries %s bytes %s",
				entry.Name, int64PtrString(got.streams), int64PtrString(got.entries), int64PtrString(got.bytes),
				int64PtrString(wantStats.stats.streams), int64PtrString(wantStats.stats.entries), int64PtrString(wantStats.stats.bytes))
		}
		assertFullRangeParams(t, byName[entry.Name])
	}
	if seen != len(want) {
		t.Fatalf("stats entries = %d, want %d", seen, len(want))
	}
}

// TestFeatureCorpusVolumeExpectations checks the exact instant volume
// groupings and that volume_range carries no static timestamp expectations.
func TestFeatureCorpusVolumeExpectations(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	f := loadFixtureTotals(t)
	want := map[string]map[string]int64{
		"volume-api-service": {
			canonicalLabels(map[string]string{"service_name": "api"}): f.baseAPIBytes,
		},
		"volume-job-frontend": {
			canonicalLabels(map[string]string{"service_name": "api"}): f.baseAPIBytes,
			canonicalLabels(map[string]string{"service_name": "web"}): f.baseWebBytes,
		},
		"volume-corpus-format-level": {},
	}
	for key, value := range f.byFormatLevel {
		format, level, ok := strings.Cut(key, "/")
		if !ok {
			t.Fatalf("corpus format/level key %q", key)
		}
		want["volume-corpus-format-level"][canonicalLabels(map[string]string{"format": format, "level": level})] = value
	}

	instants := 0
	ranges := 0
	for _, entry := range entries {
		switch entry.Type {
		case queryVolume:
			instants++
			assertFullRangeParams(t, entry)
			if entry.Name == "volume-bare-match-all" {
				if entry.Expect != nil {
					t.Errorf("bare {} volume must stay informational, got expect %+v", entry.Expect)
				}
				continue
			}
			wantSeries, ok := want[entry.Name]
			if !ok {
				t.Errorf("volume entry %q has no pinned expectation", entry.Name)
				continue
			}
			if entry.Expect == nil || !entry.Expect.seriesSet {
				t.Errorf("volume entry %q has no expect.series", entry.Name)
				continue
			}
			got := entry.Expect.series
			if len(got) != len(wantSeries) {
				t.Errorf("volume entry %q has %d series, want %d", entry.Name, len(got), len(wantSeries))
			}
			for _, series := range got {
				key := canonicalLabels(series.metric)
				value, ok := wantSeries[key]
				if !ok {
					t.Errorf("volume entry %q unexpected series %s", entry.Name, formatLabels(series.metric))
					continue
				}
				if series.value == nil || series.value.Cmp(big.NewRat(value, 1)) != 0 {
					t.Errorf("volume entry %q series %s value = %v, want %d",
						entry.Name, formatLabels(series.metric), series.value, value)
				}
			}
			assertVolumeTargetLabels(t, entry, got)
		case queryVolumeRange:
			ranges++
			assertFullRangeParams(t, entry)
			if entry.Expect != nil {
				t.Errorf("volume_range entry %q must not carry static expectations", entry.Name)
			}
			if got := entry.Params["step"]; !slices.Equal(got, []string{"5m"}) {
				t.Errorf("volume_range %q step = %v, want [5m]", entry.Name, got)
			}
		}
	}
	if instants != 4 || ranges != 1 {
		t.Fatalf("volume entries = %d instant / %d range, want 4 / 1", instants, ranges)
	}
}

// TestFeatureCorpusMalformedStatuses pins the representative queries that both
// servers reject with a Loki-style 400.
func TestFeatureCorpusMalformedStatuses(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	want := map[string]string{
		"instant-log-selector-rejected":     `{service_name="api"}`,
		"invalid-selector-empty-value":      `{service_name=}`,
		"invalid-grammar-unclosed-selector": `{service_name="api"`,
		"invalid-aggregation-topk-string":   `topk("3", count_over_time({service_name="api"}[5m]))`,
	}
	seen := 0
	for _, entry := range entries {
		if entry.Expect == nil || entry.Expect.status == nil {
			continue
		}
		seen++
		wantQuery, ok := want[entry.Name]
		if !ok {
			t.Errorf("unexpected status expectation on %q", entry.Name)
			continue
		}
		if *entry.Expect.status != 400 {
			t.Errorf("%q expect status = %d, want 400", entry.Name, *entry.Expect.status)
		}
		if entry.Query != wantQuery {
			t.Errorf("%q query = %q, want %q", entry.Name, entry.Query, wantQuery)
		}
		if entry.Expect.stats != nil || entry.Expect.seriesSet {
			t.Errorf("%q status expectation must be exclusive with stats/series", entry.Name)
		}
	}
	if seen != len(want) {
		t.Fatalf("status entries = %d, want %d", seen, len(want))
	}
}

// TestFeatureCorpusInvalidQueriesAreBadQuery runs the four mutual-invalid
// status fixtures through the real engine and requires plan.ErrBadQuery, not
// plan.ErrUnsupported: the instant log-selector rejection, the invalid
// selector, the invalid grammar and the invalid aggregation argument.
func TestFeatureCorpusInvalidQueriesAreBadQuery(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	src, start := corpusMemSource(t)
	engine := logql.New(logqlconfig.LogQL{MaxEntries: 5000, MaxSeries: 500})
	evalAt := start.Add(2 * time.Second)
	checked := 0
	for _, entry := range entries {
		if entry.Expect == nil || entry.Expect.status == nil {
			continue
		}
		checked++
		params := plan.Params{
			Tenant:    tenant.Default,
			Query:     entry.Query,
			Start:     start.Add(-time.Minute),
			End:       start.Add(10 * time.Minute),
			Step:      5 * time.Second,
			Limit:     1000,
			Direction: plan.Backward,
		}
		if entry.Type == queryInstant {
			params.Start, params.End, params.Step = evalAt, evalAt, 0
		}
		_, err := engine.Query(t.Context(), src, params)
		if err == nil {
			t.Errorf("%q: Query succeeded, want ErrBadQuery", entry.Name)
			continue
		}
		if !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("%q: Query error = %v, want ErrBadQuery", entry.Name, err)
		}
		if errors.Is(err, plan.ErrUnsupported) {
			t.Errorf("%q: Query error = %v, must not be ErrUnsupported", entry.Name, err)
		}
	}
	if checked != 4 {
		t.Fatalf("checked %d invalid entries, want 4", checked)
	}
}

// TestFeatureCorpusEngineExecutes parses, compiles and runs every positive
// range/instant entry through the real LogQL engine over a plan.MemSource
// loaded with all 1200 generator records (labels and structured metadata)
// under the default 500-series limit. It is the persistent guard against
// invalid literal syntax, misnamed functions and cardinality regressions that
// the request builder alone cannot see.
func TestFeatureCorpusEngineExecutes(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	src, start := corpusMemSource(t)
	engine := logql.New(logqlconfig.LogQL{MaxEntries: 5000, MaxSeries: 500})
	evalAt := start.Add(2 * time.Second)
	queryStart, queryEnd := start.Add(-time.Minute), start.Add(10*time.Minute)
	// Scalar instants return plan.TypeScalar, unlike metric instants.
	scalars := map[string]float64{
		"scalar-precedence":   7,
		"scalar-parentheses":  9,
		"scalar-power":        512,
		"scalar-compare-bool": 1,
	}
	// The two level-grouped comparisons keep bounded three-series results and
	// nontrivial true/false outcomes: filter keeps info(448)/warn(413), bool
	// keeps all three levels with 1/1/0.
	vectors := map[string]map[string]float64{
		"binary-filter-compare": {
			canonicalLabels(map[string]string{"level": "info"}): 448,
			canonicalLabels(map[string]string{"level": "warn"}): 413,
		},
		"binary-bool-compare": {
			canonicalLabels(map[string]string{"level": "error"}): 0,
			canonicalLabels(map[string]string{"level": "info"}):  1,
			canonicalLabels(map[string]string{"level": "warn"}):  1,
		},
		"agg-stddev": outerVarianceByFormat(t, false),
		"agg-stdvar": outerVarianceByFormat(t, true),
	}
	// Only the generator-derived variance fixtures need float tolerance; the
	// binary comparison fixtures are literal integers and stay exact.
	tolerantVectors := map[string]struct{}{"agg-stddev": {}, "agg-stdvar": {}}
	executed, seenScalars, seenVectors := 0, 0, 0
	for _, entry := range entries {
		if entry.Type != queryRange && entry.Type != queryInstant {
			continue
		}
		if entry.Expect != nil && entry.Expect.status != nil {
			continue
		}
		t.Run(entry.Name, func(t *testing.T) {
			executed++
			params := plan.Params{
				Tenant:    tenant.Default,
				Query:     entry.Query,
				Start:     queryStart,
				End:       queryEnd,
				Limit:     1000,
				Direction: plan.Backward,
			}
			if entry.Type == queryInstant {
				params.Start, params.End = evalAt, evalAt
			}
			if entry.Type == queryRange {
				step := 5 * time.Second
				if raw := entry.Params["step"]; len(raw) == 1 {
					parsed, err := time.ParseDuration(raw[0])
					if err != nil {
						t.Fatalf("step %q: %v", raw[0], err)
					}
					step = parsed
				}
				params.Step = step
			}

			expr, err := logql.Parse(entry.Query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			_, isLog := expr.(*logql.LogQuery)
			if isLog {
				if _, err := engine.CompileLogQuery(entry.Query); err != nil {
					t.Fatalf("CompileLogQuery: %v", err)
				}
			}
			result, err := engine.Query(t.Context(), src, params)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if want, ok := scalars[entry.Name]; ok {
				seenScalars++
				if result.Type != plan.TypeScalar {
					t.Fatalf("result type = %s, want scalar", result.Type)
				}
				if result.Scalar.V != want {
					t.Fatalf("scalar value = %v, want %v", result.Scalar.V, want)
				}
				return
			}
			if want, ok := vectors[entry.Name]; ok {
				seenVectors++
				if result.Type != plan.TypeVector {
					t.Fatalf("result type = %s, want vector", result.Type)
				}
				if len(result.Vector) != len(want) {
					t.Fatalf("vector series = %d, want %d", len(result.Vector), len(want))
				}
				got := make(map[string]float64, len(result.Vector))
				for _, sample := range result.Vector {
					labels := labelMap(sample.Metric)
					key := canonicalLabels(labels)
					if _, exists := got[key]; exists {
						t.Fatalf("duplicate vector series %s", formatLabels(labels))
					}
					got[key] = sample.V
				}
				if _, tolerant := tolerantVectors[entry.Name]; tolerant {
					if !valuesClose(got, want) {
						t.Fatalf("vector result = %v, want %v", got, want)
					}
				} else if !maps.Equal(got, want) {
					t.Fatalf("vector result = %v, want %v", got, want)
				}
				return
			}
			want := plan.TypeVector
			switch {
			case isLog:
				want = plan.TypeStreams
			case entry.Type == queryRange:
				want = plan.TypeMatrix
			}
			if result.Type != want {
				t.Errorf("result type = %s, want %s", result.Type, want)
			}
		})
	}
	if executed != 89 {
		t.Fatalf("executed %d positive entries, want 89", executed)
	}
	if seenScalars != len(scalars) {
		t.Fatalf("checked %d scalar instants, want %d", seenScalars, len(scalars))
	}
	if seenVectors != len(vectors) {
		t.Fatalf("checked %d exact vectors, want %d", seenVectors, len(vectors))
	}
}

// labelMap turns sorted labels into the map form canonicalLabels expects.
func labelMap(labels logs.Labels) map[string]string {
	out := make(map[string]string, len(labels))
	for _, label := range labels {
		out[label.Name] = label.Value
	}
	return out
}

// TestFeatureCorpusQueriesParse compiles every positive LogQL expression with
// the real parser. A grammar regression (a bad string escape, an invented
// aggregation function) fails here.
func TestFeatureCorpusQueriesParse(t *testing.T) {
	entries, _ := corpusEntriesByName(t)
	for _, entry := range entries {
		if entry.Type != queryRange && entry.Type != queryInstant {
			continue
		}
		if entry.Expect != nil && entry.Expect.status != nil {
			continue
		}
		t.Run(entry.Name, func(t *testing.T) {
			if _, err := logql.Parse(entry.Query); err != nil {
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

// corpusMemSource loads the 1200-record corpus dataset the way pushgen sends
// it: base stream labels plus per-record format/level/pod and structured
// trace_id/request_id metadata, one millisecond apart.
func corpusMemSource(t *testing.T) (*plan.MemSource, time.Time) {
	t.Helper()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := logs.Labels{
		{Name: "env", Value: "dev"},
		{Name: "job", Value: "corpus"},
		{Name: "service_name", Value: "corpus"},
	}
	rows := make([]plan.Row, 0, fixtureCorpusEntries)
	for i := range fixtureCorpusEntries {
		record := gen.NewCorpusRecord(i, "corpus")
		line, err := record.Line()
		if err != nil {
			t.Fatalf("corpus record %d: %v", i, err)
		}
		rows = append(rows, plan.Row{
			TS:     start.Add(time.Duration(i) * time.Millisecond),
			Line:   line,
			Labels: logs.New(append(slices.Clone(base), record.Labels()...)...),
			Meta:   record.Metadata(),
		})
	}
	src := plan.NewMemSource()
	src.Add(tenant.Default, rows...)
	return src, start
}

// probe is one Parkana-only unsupported-feature smoke probe. It is consumed by
// the e2e harness, not by lokicompare's corpus loader.
type probe struct {
	Name           string            `json:"name"`
	Query          string            `json:"query"`
	Endpoint       string            `json:"endpoint"`
	Params         map[string]string `json:"params"`
	ExpectedStatus int64             `json:"expectedStatus"`
	BodyContains   string            `json:"bodyContains"`
}

// TestUnsupportedProbes validates the separate Parkana-only probe file: stable
// schema, unique names, valid deferred families, 400/unsupported contract and
// no vector() probe (A9 requires vector() to work).
func TestUnsupportedProbes(t *testing.T) {
	data, err := os.ReadFile("testdata/unsupported-probes.json")
	if err != nil {
		t.Fatalf("read probes: %v", err)
	}
	var probes []probe
	if err := json.Unmarshal(data, &probes); err != nil {
		t.Fatalf("parse probes: %v", err)
	}
	if len(probes) != 10 {
		t.Fatalf("probes = %d, want 10", len(probes))
	}
	seen := make(map[string]struct{}, len(probes))
	byName := make(map[string]probe, len(probes))
	for _, p := range probes {
		if _, ok := seen[p.Name]; ok {
			t.Errorf("duplicate probe name %q", p.Name)
		}
		seen[p.Name] = struct{}{}
		byName[p.Name] = p
		if strings.TrimSpace(p.Query) == "" {
			t.Errorf("probe %q: empty query", p.Name)
		}
		switch p.Endpoint {
		case "/loki/api/v1/query", "/loki/api/v1/query_range":
		default:
			t.Errorf("probe %q: endpoint %q, want /loki/api/v1/query or /query_range", p.Name, p.Endpoint)
		}
		if p.ExpectedStatus != 400 {
			t.Errorf("probe %q: expectedStatus = %d, want 400", p.Name, p.ExpectedStatus)
		}
		if p.BodyContains != "unsupported" {
			t.Errorf("probe %q: bodyContains = %q, want unsupported", p.Name, p.BodyContains)
		}
		if strings.Contains(p.Query, "vector(") {
			t.Errorf("probe %q must not probe vector(): A9 requires it to work", p.Name)
		}
		if p.Endpoint == "/loki/api/v1/query_range" && strings.TrimSpace(p.Params["step"]) == "" {
			t.Errorf("probe %q: query_range probe needs params.step", p.Name)
		}
		_, parseErr := logql.Parse(p.Query)
		switch {
		case parseErr == nil:
			t.Errorf("probe %q parsed, want ErrUnsupported", p.Name)
		case errors.Is(parseErr, plan.ErrBadQuery):
			t.Errorf("probe %q parse error = %v, want ErrUnsupported not ErrBadQuery", p.Name, parseErr)
		case !errors.Is(parseErr, plan.ErrUnsupported):
			t.Errorf("probe %q parse error = %v, want ErrUnsupported", p.Name, parseErr)
		}
	}
	required := map[string]string{
		"modifier on":            `on(`,
		"modifier ignoring":      `ignoring(`,
		"modifier group_left":    `group_left`,
		"modifier group_right":   `group_right`,
		"set or":                 ` or `,
		"set and":                ` and `,
		"set unless":             ` unless `,
		"stage unpack":           `| unpack`,
		"stage decolorize":       `| decolorize`,
		"function label_replace": `label_replace(`,
	}
	queries := make([]string, 0, len(probes))
	for _, p := range probes {
		queries = append(queries, p.Query)
	}
	for family, fragment := range required {
		if !slices.ContainsFunc(queries, func(query string) bool {
			return strings.Contains(query, fragment)
		}) {
			t.Errorf("probe family %q: no query contains %q", family, fragment)
		}
	}
	for _, name := range []string{"modifier-group-left", "modifier-group-right"} {
		p, ok := byName[name]
		if !ok {
			t.Fatalf("missing probe %q", name)
		}
		if !strings.Contains(p.Query, "on(") && !strings.Contains(p.Query, "ignoring(") {
			t.Errorf("probe %q: group_left/group_right needs a preceding on()/ignoring()", name)
		}
	}
}

func assertFullRangeParams(t *testing.T, entry corpusEntry) {
	t.Helper()
	for _, key := range []string{"start", "end", "time"} {
		if got, ok := entry.Params[key]; ok {
			t.Errorf("entry %q overrides %s = %v; exact counts need the CLI's full range", entry.Name, key, got)
		}
	}
}

func assertVolumeTargetLabels(t *testing.T, entry corpusEntry, series []seriesExpectation) {
	t.Helper()
	raw := entry.Params["targetLabels"]
	if len(raw) != 1 || strings.TrimSpace(raw[0]) == "" {
		t.Fatalf("volume entry %q needs one non-empty params.targetLabels", entry.Name)
	}
	targets := strings.Split(raw[0], ",")
	metricKeys := make(map[string]struct{})
	for _, s := range series {
		for key := range s.metric {
			metricKeys[key] = struct{}{}
		}
	}
	if len(targets) != len(metricKeys) {
		t.Errorf("volume entry %q targetLabels %v, expected metric keys %v", entry.Name, targets, slices.Sorted(maps.Keys(metricKeys)))
	}
	for _, target := range targets {
		if _, ok := metricKeys[target]; !ok {
			t.Errorf("volume entry %q targetLabel %q missing from expected metric keys", entry.Name, target)
		}
	}
}

// outerVarianceByFormat returns the expected vector for
// stddev/stdvar by (format) (sum by (format, level) (count_over_time(...)))
// over the full 1200-record fixture. Each format has three level sums, so the
// outer aggregation is the population variance (or standard deviation) of
// three metadata-collapsed counts.
func outerVarianceByFormat(t *testing.T, variance bool) map[string]float64 {
	t.Helper()
	counts := make(map[string]map[string]float64)
	for i := range fixtureCorpusEntries {
		record := gen.NewCorpusRecord(i, "corpus")
		if counts[record.Format] == nil {
			counts[record.Format] = make(map[string]float64)
		}
		counts[record.Format][record.Level]++
	}
	out := make(map[string]float64, len(counts))
	for format, byLevel := range counts {
		if len(byLevel) != 3 {
			t.Fatalf("format %q has %d levels, want 3", format, len(byLevel))
		}
		var sum float64
		for _, n := range byLevel {
			sum += n
		}
		mean := sum / float64(len(byLevel))
		var m2 float64
		for _, n := range byLevel {
			delta := n - mean
			m2 += delta * delta
		}
		value := m2 / float64(len(byLevel))
		if !variance {
			value = math.Sqrt(value)
		}
		out[canonicalLabels(map[string]string{"format": format})] = value
	}
	return out
}

// valuesClose compares two vector expectation maps with the comparator's float
// tolerance. The variance fixtures are computed from the generator rather than
// being literal integers, so last-bit differences in the Welford accumulator are
// not meaningful. Non-finite actual values never match a finite expectation.
func valuesClose(got, want map[string]float64) bool {
	if len(got) != len(want) {
		return false
	}
	for key, expected := range want {
		actual, ok := got[key]
		if !ok {
			return false
		}
		if math.IsNaN(actual) || math.IsInf(actual, 0) {
			return false
		}
		if math.Abs(actual-expected) > 1e-9*max(1, math.Abs(expected)) {
			return false
		}
	}
	return true
}

func TestValuesCloseRejectsNonFinite(t *testing.T) {
	key := canonicalLabels(map[string]string{"format": "json"})
	finite := map[string]float64{key: 1}
	for _, actual := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if valuesClose(map[string]float64{key: actual}, finite) {
			t.Errorf("valuesClose accepted %v for a finite expectation", actual)
		}
	}
	if !valuesClose(map[string]float64{key: 1 + 1e-12}, finite) {
		t.Error("valuesClose rejected a within-tolerance value")
	}
	if valuesClose(map[string]float64{key: 2}, finite) {
		t.Error("valuesClose accepted a value outside the tolerance")
	}
}

// TestFeatureCorpusBoundedOuterVariance pins the bounded-child form that keeps
// Loki's frontend-inner series limit and Parkana's final-series limit from
// diverging, and proves the expected variance is nonzero.
func TestFeatureCorpusBoundedOuterVariance(t *testing.T) {
	_, byName := corpusEntriesByName(t)
	want := map[string]string{
		"agg-stddev": `stddev by (format) (sum by (format, level) (count_over_time({service_name="corpus"}[10m])))`,
		"agg-stdvar": `stdvar by (format) (sum by (format, level) (count_over_time({service_name="corpus"}[10m])))`,
	}
	for name, query := range want {
		entry, ok := byName[name]
		if !ok {
			t.Fatalf("missing %q", name)
		}
		if entry.Type != queryInstant {
			t.Errorf("%q type = %s, want instant", name, entry.Type)
		}
		if entry.Query != query {
			t.Errorf("%q query = %q, want %q", name, entry.Query, query)
		}
	}
	for format, value := range outerVarianceByFormat(t, true) {
		if value == 0 {
			t.Errorf("expected variance for %s is zero; the fixture lost its nontrivial counts", format)
		}
	}
}

// TestFeatureCorpusScalarTimestampQuirks pins the explicit opt-in: exactly the
// four scalar instant entries carry it and nothing else does.
func TestFeatureCorpusScalarTimestampQuirks(t *testing.T) {
	entries, byName := corpusEntriesByName(t)
	want := []string{"scalar-precedence", "scalar-parentheses", "scalar-power", "scalar-compare-bool"}
	for _, name := range want {
		entry, ok := byName[name]
		if !ok {
			t.Fatalf("missing %q", name)
		}
		if entry.LokiQuirk != lokiQuirkScalarTimestamp {
			t.Errorf("%q lokiQuirk = %q, want %q", name, entry.LokiQuirk, lokiQuirkScalarTimestamp)
		}
		if entry.Type != queryInstant {
			t.Errorf("%q type = %s, want instant", name, entry.Type)
		}
	}
	seen := 0
	for _, entry := range entries {
		if entry.LokiQuirk == "" {
			continue
		}
		seen++
		if !slices.Contains(want, entry.Name) {
			t.Errorf("unexpected lokiQuirk on %q", entry.Name)
		}
	}
	if seen != len(want) {
		t.Fatalf("lokiQuirk entries = %d, want %d", seen, len(want))
	}
}

func equalInt64Ptr(got, want *int64) bool {
	return got != nil && want != nil && *got == *want
}

func int64PtrString(v *int64) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", *v)
}
