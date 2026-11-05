package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// The synthetic test manifest uses these marker periods and blocks; addTestRows
// applies exactly the same predicates, so the engine and the manifest agree.
const (
	testFrequentEvery = 4
	testRareEvery     = 8
	testShortEvery    = 16
	testManifestLines = 4000
	testManifestStep  = 100 // microseconds
	testLineBytes     = 10

	// The rare and four-byte blocks are offset from the corpus start, like the
	// production generator's block layout; the second rare block makes the
	// short window narrower than the long one for the rare class.
	testRareFrom        = 100
	testRareThrough     = 800
	testRareLateFrom    = 3000
	testRareLateThrough = 3600
	testShortFrom       = 1600
	testShortThrough    = 2400
)

// testQueryStart is microsecond-aligned and in the past.
func testQueryStart() time.Time {
	return time.Date(2026, 11, 1, 10, 0, 0, 123000000, time.UTC)
}

// buildTestManifest builds a small valid ingest report whose schedule, ranges
// and counts agree, for tests that need a manifest without generating bytes.
func buildTestManifest(t *testing.T) Report {
	t.Helper()
	start := testQueryStart()
	step := time.Duration(testManifestStep) * time.Microsecond
	half := int64(testManifestLines / 2)
	manifest := CorpusInfo{
		Version:             corpusVersion,
		Hash:                "config-hash",
		ContentHash:         "content-hash",
		Seed:                7,
		Start:               start,
		StepMicros:          testManifestStep,
		TargetRawLineBytes:  testManifestLines * testLineBytes,
		RawLineBytes:        testManifestLines * testLineBytes,
		Lines:               testManifestLines,
		Through:             start.Add(time.Duration(testManifestLines) * step),
		StreamLabel:         "service_name",
		InstancesPerService: 2,
		Streams: []StreamInfo{
			{Name: "api", Labels: map[string]string{"service_name": "api"}, Lines: half, RawLineBytes: half * testLineBytes},
			{Name: "web", Labels: map[string]string{"service_name": "web"}, Lines: half, RawLineBytes: half * testLineBytes},
		},
		Markers: []MarkerInfo{
			testMarker(t, queryClassFrequent, frequentMarker, testFrequentEvery, RecordRange{From: 0, Through: testManifestLines - 1}),
			testMarker(t, queryClassRare, rareMarker, testRareEvery,
				RecordRange{From: testRareFrom, Through: testRareThrough},
				RecordRange{From: testRareLateFrom, Through: testRareLateThrough}),
			testMarker(t, queryClassAbsent, absentMarker, 0),
			testMarker(t, queryClassShort4Byte, shortNeedle, testShortEvery, RecordRange{From: testShortFrom, Through: testShortThrough}),
		},
	}
	report := Report{
		Corpus:          manifest,
		AckRawLineBytes: manifest.RawLineBytes,
		AckLines:        manifest.Lines,
		Delivery:        DeliveryStats{Clean: true},
	}
	if err := validateManifest(&report); err != nil {
		t.Fatalf("test manifest is invalid: %v", err)
	}
	return report
}

// testMarker builds one marker with one schedule block and one range per block,
// mirroring the production generator's per-block ranges.
func testMarker(t *testing.T, name, literal string, every int64, blocks ...RecordRange) MarkerInfo {
	t.Helper()
	marker := MarkerInfo{
		Name:               name,
		Literal:            literal,
		Bytes:              len(literal),
		IndexEligibleGrams: eligibleGrams(len(literal)),
		Ranges:             []MarkerRange{},
		Schedule:           MarkerSchedule{Blocks: []RecordRange{}},
	}
	if every <= 0 || len(blocks) == 0 {
		return marker
	}
	marker.Schedule = MarkerSchedule{EveryRecords: every, Blocks: blocks}
	step := time.Duration(testManifestStep) * time.Microsecond
	for _, block := range blocks {
		count := multiplesIn(block.From, block.Through, every)
		if count == 0 {
			continue
		}
		first := ((block.From + every - 1) / every) * every
		last := ((block.Through - 1) / every) * every
		marker.Ranges = append(marker.Ranges, MarkerRange{
			From:    testQueryStart().Add(time.Duration(first) * step),
			Through: testQueryStart().Add(time.Duration(last+1) * step),
			Count:   count,
		})
		marker.Count += count
	}
	return marker
}

// addTestRows loads a MemSource with exactly the records the test manifest
// describes.
func addTestRows(t *testing.T, manifest CorpusInfo) *plan.MemSource {
	t.Helper()
	source := plan.NewMemSource()
	step := time.Duration(manifest.StepMicros) * time.Microsecond
	labels := make([]logs.Labels, len(manifest.Streams))
	for i, stream := range manifest.Streams {
		labels[i] = logs.FromMap(stream.Labels)
	}
	for i := range manifest.Lines {
		var markers []string
		if i%testFrequentEvery == 0 {
			markers = append(markers, frequentMarker)
		}
		if i >= testRareFrom && i < testRareThrough || i >= testRareLateFrom && i < testRareLateThrough {
			if i%testRareEvery == 0 {
				markers = append(markers, rareMarker)
			}
		}
		if i >= testShortFrom && i < testShortThrough && i%testShortEvery == 0 {
			markers = append(markers, shortNeedle)
		}
		line := fmt.Sprintf("record %d", i)
		if len(markers) > 0 {
			line += " " + strings.Join(markers, " ")
		}
		source.Add(tenant.Default, plan.Row{
			TS:     manifest.Start.Add(time.Duration(i) * step),
			Labels: labels[i%int64(len(labels))],
			Line:   line,
		})
	}
	return source
}

// engineCount runs one query through the real LogQL engine and returns the
// integer count it produced.
func engineCount(t *testing.T, source plan.Source, query string, at time.Time) int64 {
	t.Helper()
	engine := logql.New(config.LogQL{MaxEntries: 5000, MaxSeries: 500})
	result, err := engine.Query(t.Context(), source, plan.Params{
		Tenant: tenant.Default,
		Query:  query,
		Start:  at,
		End:    at,
	})
	if err != nil {
		t.Fatalf("query %q at %v: %v", query, at, err)
	}
	var sum float64
	switch result.Type {
	case plan.TypeVector:
		for _, sample := range result.Vector {
			sum += sample.V
		}
	case plan.TypeScalar:
		sum = result.Scalar.V
	default:
		t.Fatalf("query %q returned %q, want a vector", query, result.Type)
	}
	return int64(math.Round(sum))
}

func TestNearestRankPercentile(t *testing.T) {
	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
		ok     bool
	}{
		{"empty", nil, 50, 0, false},
		{"single p50", []float64{3}, 50, 3, true},
		{"single p99", []float64{3}, 99, 3, true},
		{"two p50 is the lower", []float64{1, 2}, 50, 1, true},
		{"two p99 is the upper", []float64{1, 2}, 99, 2, true},
		{"four p50", []float64{1, 2, 3, 4}, 50, 2, true},
		{"four p99", []float64{1, 2, 3, 4}, 99, 4, true},
		{"hundred p99", rangeFloats(100), 99, 98, true},
		{"hundred p50", rangeFloats(100), 50, 49, true},
		{"p0", []float64{5, 6}, 0, 5, true},
		{"p100", []float64{5, 6}, 100, 6, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := nearestRankPercentile(tc.sorted, tc.p)
			if ok != tc.ok {
				t.Fatalf("ok = %t, want %t", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("p%v of %v = %v, want %v", tc.p, tc.sorted, got, tc.want)
			}
		})
	}
}

// rangeFloats returns 0..n-1.
func rangeFloats(n int) []float64 {
	out := make([]float64, n)
	for i := range n {
		out[i] = float64(i)
	}
	return out
}

func TestFormatLogQLRange(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "0ms"},
		{-time.Second, "0ms"},
		{time.Millisecond, "1ms"},
		{999 * time.Millisecond, "999ms"},
		{time.Second, "1s"},
		{1500 * time.Millisecond, "1s500ms"},
		{time.Minute, "1m"},
		{time.Hour + 2*time.Minute + 3*time.Second + 4*time.Millisecond, "1h2m3s4ms"},
		{25 * time.Hour, "25h"},
		{19661 * time.Millisecond, "19s661ms"},
	}
	for _, tc := range tests {
		if got := formatLogQLRange(tc.d); got != tc.want {
			t.Fatalf("formatLogQLRange(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestFormatAPITimeMicroseconds(t *testing.T) {
	at := time.Date(2026, 11, 1, 10, 0, 0, 123456000, time.UTC)
	got := formatAPITime(at)
	if want := "2026-11-01T10:00:00.123456Z"; got != want {
		t.Fatalf("formatAPITime = %q, want %q", got, want)
	}
	parsed, err := time.Parse(time.RFC3339Nano, got)
	if err != nil {
		t.Fatalf("parse formatted time: %v", err)
	}
	if !parsed.Equal(at) {
		t.Fatalf("round trip = %v, want %v", parsed, at)
	}
}

func TestQueryWindowsFromManifest(t *testing.T) {
	manifest := buildTestManifest(t).Corpus
	windows, err := newQueryWindows(manifest)
	if err != nil {
		t.Fatalf("newQueryWindows: %v", err)
	}
	long := windows[queryWindowLong]
	if !long.From.Equal(manifest.Start) {
		t.Fatalf("long from = %v, want the corpus start %v", long.From, manifest.Start)
	}
	if !long.Through.Equal(manifest.Through) {
		t.Fatalf("long through = %v, want %v (already ms-aligned difference)", long.Through, manifest.Through)
	}
	short := windows[queryWindowShort]
	rare, _ := findMarker(manifest, queryClassRare)
	shortMarker, _ := findMarker(manifest, queryClassShort4Byte)
	if len(rare.Ranges) == 0 || len(shortMarker.Ranges) == 0 {
		t.Fatal("test manifest has no rare/short ranges")
	}
	if !short.From.Equal(rare.Ranges[0].From) {
		t.Fatalf("short from = %v, want the first rare hit %v", short.From, rare.Ranges[0].From)
	}
	if short.Through.Before(shortMarker.Ranges[0].Through) {
		t.Fatalf("short through = %v, want at least the first short range end %v", short.Through, shortMarker.Ranges[0].Through)
	}
	for name, window := range windows {
		if window.RangeMS <= 0 || time.Duration(window.RangeMS)*time.Millisecond%time.Millisecond != 0 {
			t.Fatalf("%s window range %d is not whole milliseconds", name, window.RangeMS)
		}
		if want := time.Duration(window.RangeMS)*time.Millisecond - time.Microsecond; window.EvalTime.Sub(window.From) != want {
			t.Fatalf("%s eval time %v is not through-1µs", name, window.EvalTime)
		}
		if window.EvalTime.Nanosecond()%1000 != 0 {
			t.Fatalf("%s eval time %v is not microsecond-aligned", name, window.EvalTime)
		}
		if window.EvalTime.Before(window.From) || !window.EvalTime.Before(window.Through) {
			t.Fatalf("%s eval time %v is outside [%v, %v)", name, window.EvalTime, window.From, window.Through)
		}
	}

	// A manifest too small to contain marker ranges falls back to its first
	// marker block and stays non-empty.
	tiny := manifest
	for i := range tiny.Markers {
		tiny.Markers[i].Ranges = []MarkerRange{}
		tiny.Markers[i].Count = 0
	}
	tinyFrom, tinyThrough, err := shortBounds(tiny)
	if err != nil {
		t.Fatalf("shortBounds(tiny): %v", err)
	}
	if !tinyThrough.After(tinyFrom) {
		t.Fatalf("tiny short window is empty: [%v, %v)", tinyFrom, tinyThrough)
	}

	// An empty corpus end yields no short window.
	empty := manifest
	empty.Through = empty.Start
	if _, _, err := shortBounds(empty); err == nil {
		t.Fatal("shortBounds accepted an empty corpus")
	}
}

func TestQueryWindowsRoundOutwardToWholeMilliseconds(t *testing.T) {
	from := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	through := from.Add(19661*time.Millisecond + 137*time.Microsecond)
	window, err := newQueryWindow("short", from, through)
	if err != nil {
		t.Fatalf("newQueryWindow: %v", err)
	}
	if !window.From.Equal(from) {
		t.Fatalf("from moved to %v, want %v", window.From, from)
	}
	if window.Through.Before(through) {
		t.Fatalf("through %v does not cover %v", window.Through, through)
	}
	if window.Through.Sub(through) >= time.Millisecond {
		t.Fatalf("through %v extends by %v, want less than 1ms", window.Through, window.Through.Sub(through))
	}
	if window.RangeMS*int64(time.Millisecond) != int64(window.Through.Sub(window.From)) {
		t.Fatalf("range %dms does not match the window", window.RangeMS)
	}
	if _, err := newQueryWindow("bad", from, from); err == nil {
		t.Fatal("newQueryWindow accepted empty bounds")
	}
}

// TestQueryWindowEndpointInclusivity proves the (t-range, t] mapping on exact
// microsecond boundaries through the real engine: the window [from, through)
// must include from and through-1µs and exclude both outside neighbours.
func TestQueryWindowEndpointInclusivity(t *testing.T) {
	from := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	through := from.Add(10 * time.Millisecond)
	window, err := newQueryWindow("long", from, through)
	if err != nil {
		t.Fatalf("newQueryWindow: %v", err)
	}
	source := plan.NewMemSource()
	labels := logs.FromMap(map[string]string{"service_name": "api"})
	for _, ts := range []time.Time{
		from.Add(-time.Microsecond),
		from,
		from.Add(5 * time.Millisecond),
		through.Add(-time.Microsecond),
		through,
	} {
		source.Add(tenant.Default, plan.Row{TS: ts, Labels: labels, Line: "needle"})
	}
	selector, err := buildSelector(CorpusInfo{StreamLabel: "service_name", Streams: []StreamInfo{{Name: "api"}}})
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	query := buildCountQuery(selector, "needle", window.rangeDuration())
	if got := engineCount(t, source, query, window.EvalTime); got != 3 {
		t.Fatalf("count = %d, want 3 ([from, through) includes from and through-1µs only)", got)
	}
}

// TestQueryDefinitionsThroughEngine computes every configured count through the
// real LogQL engine over rows that match the manifest, for both windows and all
// four classes.
func TestQueryDefinitionsThroughEngine(t *testing.T) {
	manifest := buildTestManifest(t)
	source := addTestRows(t, manifest.Corpus)
	cfg := QueryConfig{
		Backend:     backendParkana,
		URL:         "http://example.invalid:3100",
		Repetitions: 3,
		Warmups:     1,
	}
	queryPlan, err := newQueryPlan(&cfg, &manifest)
	if err != nil {
		t.Fatalf("newQueryPlan: %v", err)
	}
	defs, err := queryDefinitions(queryPlan, manifest)
	if err != nil {
		t.Fatalf("queryDefinitions: %v", err)
	}
	if len(defs) != len(queryClassNames())*len(queryWindowNames()) {
		t.Fatalf("definitions = %d, want %d", len(defs), len(queryClassNames())*len(queryWindowNames()))
	}
	seen := map[string]int64{}
	for _, def := range defs {
		key := def.Class + "/" + def.Window
		seen[key] = def.Expected
		got := engineCount(t, source, def.Query, def.Windows.EvalTime)
		if got != def.Expected {
			t.Fatalf("%s engine count = %d, manifest expects %d (query %q)", key, got, def.Expected, def.Query)
		}
		if def.Class == queryClassAbsent && got != 0 {
			t.Fatalf("%s absent needle matched %d rows", key, got)
		}
	}
	for _, class := range queryClassNames() {
		for _, window := range queryWindowNames() {
			if _, ok := seen[class+"/"+window]; !ok {
				t.Fatalf("missing definition %s/%s", class, window)
			}
		}
	}
	// The short window ends inside the corpus, so the frequent and rare counts
	// must be strictly smaller there; the four-byte block is fully inside it.
	for _, class := range []string{queryClassFrequent, queryClassRare} {
		if seen[class+"/"+queryWindowShort] >= seen[class+"/"+queryWindowLong] {
			t.Fatalf("%s short count %d is not smaller than the long count %d",
				class, seen[class+"/"+queryWindowShort], seen[class+"/"+queryWindowLong])
		}
	}
	if seen[queryClassShort4Byte+"/"+queryWindowShort] == 0 {
		t.Fatal("the short window contains no four-byte needle occurrences")
	}
	// The short window must contain the first rare and the first short hit.
	rare, _ := findMarker(manifest.Corpus, queryClassRare)
	short, _ := findMarker(manifest.Corpus, queryClassShort4Byte)
	shortWindow := queryPlan.windows[queryWindowShort]
	for _, hit := range []time.Time{rare.Ranges[0].From, short.Ranges[0].From} {
		if hit.Before(shortWindow.From) || !hit.Before(shortWindow.Through) {
			t.Fatalf("short window [%v, %v) does not contain %v", shortWindow.From, shortWindow.Through, hit)
		}
	}
}

// TestQueryRealCorpusManifest generates a real corpus with the production
// generator and proves that the query definitions and the count helper agree
// with the actual emitted lines. The corpus is below the first marker block, so
// rare and short counts are zero: small manifests must still be exact.
func TestQueryRealCorpusManifest(t *testing.T) {
	corpus, err := NewCorpus(CorpusConfig{
		Seed:        42,
		Start:       testQueryStart(),
		Step:        100 * time.Microsecond,
		TargetBytes: 200_000,
		Streams:     defaultStreamNames(),
		StreamLabel: defaultStreamLabel,
	})
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	source := plan.NewMemSource()
	labels := corpus.StreamLabels()
	for {
		batch, err := corpus.NextBatch(500)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		for i, entries := range batch.Streams {
			for _, entry := range entries {
				source.Add(tenant.Default, plan.Row{TS: entry.Timestamp, Labels: labels[i], Line: entry.Line})
			}
		}
		if batch.Done {
			break
		}
	}
	generated := corpus.Manifest()
	if got := len(corpus.StreamLabels()); got != 16 {
		t.Fatalf("real corpus exposes %d full label sets, want 16", got)
	}
	if generated.InstancesPerService != defaultInstancesPerService {
		t.Fatalf("real corpus instances_per_service = %d, want %d", generated.InstancesPerService, defaultInstancesPerService)
	}
	if got := len(generated.Streams); got != 4 {
		t.Fatalf("real corpus manifest services = %d, want 4 aggregates", got)
	}
	manifest := Report{
		Corpus:          generated,
		AckLines:        generated.Lines,
		AckRawLineBytes: generated.RawLineBytes,
		Delivery:        DeliveryStats{Clean: true},
	}
	if err := validateManifest(&manifest); err != nil {
		t.Fatalf("validateManifest(real corpus): %v", err)
	}
	qcfg := QueryConfig{Backend: backendParkana, URL: "http://example.invalid", Repetitions: 1}
	queryPlan, err := newQueryPlan(&qcfg, &manifest)
	if err != nil {
		t.Fatalf("newQueryPlan: %v", err)
	}
	defs, err := queryDefinitions(queryPlan, manifest)
	if err != nil {
		t.Fatalf("queryDefinitions: %v", err)
	}
	var frequentLong int64
	for _, def := range defs {
		got := engineCount(t, source, def.Query, def.Windows.EvalTime)
		if got != def.Expected {
			t.Fatalf("%s/%s engine count = %d, manifest expects %d", def.Class, def.Window, got, def.Expected)
		}
		if def.Class == queryClassFrequent && def.Window == queryWindowLong {
			frequentLong = def.Expected
		}
		if def.Class == queryClassAbsent && def.Expected != 0 {
			t.Fatalf("absent needle expects %d occurrences", def.Expected)
		}
	}
	if frequentLong == 0 {
		t.Fatal("the long window found no frequent marker occurrences")
	}
}

func TestBuildSelectorEscapesDynamicNames(t *testing.T) {
	selector, err := buildSelector(CorpusInfo{
		StreamLabel: "service_name",
		Streams:     []StreamInfo{{Name: "a.b"}, {Name: "api"}},
	})
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	if want := `{service_name=~"a\\.b|api"}`; selector != want {
		t.Fatalf("selector = %s, want %s", selector, want)
	}
	if _, err := buildSelector(CorpusInfo{StreamLabel: "bad label", Streams: []StreamInfo{{Name: "api"}}}); err == nil {
		t.Fatal("buildSelector accepted an invalid label name")
	}
	if _, err := buildSelector(CorpusInfo{Streams: []StreamInfo{{Name: "api"}}}); err == nil {
		t.Fatal("buildSelector accepted a missing stream label")
	}
	if _, err := buildSelector(CorpusInfo{StreamLabel: "service_name"}); err == nil {
		t.Fatal("buildSelector accepted a manifest without streams")
	}
}

func TestValidateManifest(t *testing.T) {
	base := buildTestManifest(t)
	clone := func() Report {
		var copy Report
		body, err := json.Marshal(base)
		if err != nil {
			t.Fatalf("marshal manifest: %v", err)
		}
		if err := json.Unmarshal(body, &copy); err != nil {
			t.Fatalf("unmarshal manifest: %v", err)
		}
		return copy
	}
	marker := func(report *Report, name string) *MarkerInfo {
		for i := range report.Corpus.Markers {
			if report.Corpus.Markers[i].Name == name {
				return &report.Corpus.Markers[i]
			}
		}
		t.Fatalf("missing marker %q", name)
		return nil
	}
	tests := []struct {
		name    string
		mutate  func(*Report)
		wantErr string
	}{
		{"valid", func(*Report) {}, ""},
		{"unsupported version", func(r *Report) { r.Corpus.Version = corpusVersion + 1 }, "unsupported manifest version"},
		{"old corpus version", func(r *Report) { r.Corpus.Version = 1 }, "unsupported manifest version"},
		{"missing hash", func(r *Report) { r.Corpus.Hash = "" }, "missing its corpus hash"},
		{"dirty delivery", func(r *Report) { r.Delivery.Clean = false }, "clean delivery"},
		{"ambiguous delivery", func(r *Report) { r.Delivery.Ambiguous = 1 }, "clean delivery"},
		{"cancelled delivery", func(r *Report) { r.Delivery.Cancelled = true }, "clean delivery"},
		{"delivery error", func(r *Report) { r.Delivery.Error = "boom" }, "clean delivery"},
		{"ack lines", func(r *Report) { r.AckLines-- }, "acknowledged totals"},
		{"ack bytes", func(r *Report) { r.AckRawLineBytes++ }, "acknowledged totals"},
		{"incomplete corpus", func(r *Report) { r.Corpus.RawLineBytes -= testLineBytes }, "incomplete"},
		{"zero step", func(r *Report) { r.Corpus.StepMicros = 0 }, "step"},
		{"through mismatch", func(r *Report) { r.Corpus.Through = r.Corpus.Through.Add(time.Microsecond) }, "does not match start+lines*step"},
		{"negative lines", func(r *Report) { r.Corpus.Lines = -1 }, "line count"},
		{"missing stream label", func(r *Report) { r.Corpus.StreamLabel = "" }, "no stream label"},
		{"invalid stream label", func(r *Report) { r.Corpus.StreamLabel = "bad label" }, "stream label"},
		{"no streams", func(r *Report) { r.Corpus.Streams = nil }, "no streams"},
		{"missing instances", func(r *Report) { r.Corpus.InstancesPerService = 0 }, "instances_per_service"},
		{"negative instances", func(r *Report) { r.Corpus.InstancesPerService = -1 }, "instances_per_service"},
		{"instances above bound", func(r *Report) { r.Corpus.InstancesPerService = maxLabelSets + 1 }, "exceeds the"},
		{"label set product above bound", func(r *Report) { r.Corpus.InstancesPerService = maxLabelSets }, "exceeds the"},
		{"stream label is instance", func(r *Report) { r.Corpus.StreamLabel = instanceLabel }, "reserved instance label"},
		{"stream labels carry instance", func(r *Report) {
			r.Corpus.Streams[0].Labels[instanceLabel] = "0"
		}, "carries the reserved instance label"},
		{"duplicate stream", func(r *Report) { r.Corpus.Streams[1].Name = r.Corpus.Streams[0].Name }, "duplicate stream"},
		{"stream totals", func(r *Report) { r.Corpus.Streams[0].Lines++ }, "stream totals"},
		{"missing marker", func(r *Report) { r.Corpus.Markers = r.Corpus.Markers[:3] }, `has no "short_4byte" marker`},
		{"duplicate marker", func(r *Report) { r.Corpus.Markers[1].Name = r.Corpus.Markers[0].Name }, "duplicate marker"},
		{"empty literal", func(r *Report) { marker(r, queryClassRare).Literal = "" }, "empty literal"},
		{"literal bytes", func(r *Report) { marker(r, queryClassRare).Bytes = 2 }, "records 2 bytes"},
		{"short literal length", func(r *Report) {
			m := marker(r, queryClassShort4Byte)
			m.Literal = "12345"
			m.Bytes = 5
		}, "want 4"},
		{"absent count", func(r *Report) {
			m := marker(r, queryClassAbsent)
			m.Count = 1
			m.Ranges = []MarkerRange{{From: r.Corpus.Start, Through: r.Corpus.Start.Add(time.Millisecond), Count: 1}}
		}, "must be absent"},
		{"schedule count", func(r *Report) { marker(r, queryClassFrequent).Schedule.EveryRecords = 5 }, "schedule yields"},
		{"range count", func(r *Report) { marker(r, queryClassFrequent).Ranges[0].Count-- }, "but its schedule has"},
		{"negative range count", func(r *Report) { marker(r, queryClassRare).Ranges[0].Count = -1 }, "but its schedule has"},
		{"empty range", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Ranges[0].Through = m.Ranges[0].From
		}, "is empty"},
		{"unordered ranges", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Ranges = append(m.Ranges, MarkerRange{From: m.Ranges[0].From.Add(-time.Millisecond), Through: m.Ranges[0].Through, Count: 0})
		}, "overlap or are unordered"},
		{"overlapping ranges", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Ranges = append(m.Ranges, MarkerRange{From: m.Ranges[0].From, Through: m.Ranges[0].Through, Count: m.Ranges[0].Count})
		}, "overlap or are unordered"},
		{"range outside corpus", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Ranges[0].From = r.Corpus.Start.Add(-time.Millisecond)
		}, "outside the corpus"},
		{"non-microsecond range", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Ranges[0].From = m.Ranges[0].From.Add(500 * time.Nanosecond)
		}, "microsecond-aligned"},
		{"non-microsecond start", func(r *Report) {
			r.Corpus.Start = r.Corpus.Start.Add(500 * time.Nanosecond)
			r.Corpus.Through = r.Corpus.Through.Add(500 * time.Nanosecond)
		}, "microsecond-aligned"},
		{"unknown marker", func(r *Report) {
			r.Corpus.Markers = append(r.Corpus.Markers, MarkerInfo{Name: "bogus", Literal: "bogus1", Bytes: 6, Count: 0})
		}, "unknown marker"},
		{"duplicate marker", func(r *Report) { r.Corpus.Markers[1].Name = r.Corpus.Markers[0].Name }, "duplicate marker"},
		{"count past corpus", func(r *Report) { marker(r, queryClassRare).Count = r.Corpus.Lines + 1 }, "outside the"},
		{"block past corpus", func(r *Report) { marker(r, queryClassRare).Schedule.Blocks[0].Through = r.Corpus.Lines + 10 }, "exceeds the"},
		{"bad block", func(r *Report) { marker(r, queryClassRare).Schedule.Blocks[0].Through = 0 }, "invalid schedule block"},
		{"overlapping blocks", func(r *Report) {
			m := marker(r, queryClassRare)
			m.Schedule.Blocks = append(m.Schedule.Blocks, RecordRange{From: m.Schedule.Blocks[0].Through - 10, Through: m.Schedule.Blocks[0].Through + 10})
		}, "overlapping or unordered"},
		{"schedule period overflow", func(r *Report) {
			marker(r, queryClassRare).Schedule.EveryRecords = math.MaxInt64
		}, "invalid schedule period"},
		{"out of range interval", func(r *Report) {
			// Move the corpus start so start+lines*step exceeds Loki's
			// int64-nanosecond range while the totals stay consistent and the
			// start stays microsecond-aligned.
			r.Corpus.Start = maxLokiTime().Truncate(time.Microsecond).Add(-10 * time.Millisecond)
			r.Corpus.Through = r.Corpus.Start.Add(time.Duration(r.Corpus.Lines) * time.Duration(r.Corpus.StepMicros) * time.Microsecond)
		}, "nanosecond range"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := clone()
			tc.mutate(&report)
			err := validateManifest(&report)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateManifest: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateManifest accepted the mutation, want %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestDecodeQueryResponse(t *testing.T) {
	const maxExact = "9007199254740992"      // 2^53
	const aboveMaxExact = "9007199254740993" // 2^53+1, not representable
	tests := []struct {
		name         string
		body         string
		want         *int64
		wantBytes    *int64
		wantStatsErr string
		wantErr      string
	}{
		{
			name: "vector string value",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1.5,"7"]}]}}`,
			want: int64Ptr(7),
		},
		{
			name:      "vector numeric value and stats",
			body:      `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,3]}],"stats":{"summary":{"totalBytesProcessed":12345}}}}`,
			want:      int64Ptr(3),
			wantBytes: int64Ptr(12345),
		},
		{
			name: "empty vector is zero",
			body: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
			want: int64Ptr(0),
		},
		{
			name:    "null vector result",
			body:    `{"status":"success","data":{"resultType":"vector","result":null}}`,
			wantErr: "result is null",
		},
		{
			name: "multiple samples sum",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"2"]},{"value":[1,"3"]}]}}`,
			want: int64Ptr(5),
		},
		{
			name: "scalar",
			body: `{"status":"success","data":{"resultType":"scalar","result":[1,"4"]}}`,
			want: int64Ptr(4),
		},
		{
			name:    "missing result member",
			body:    `{"status":"success","data":{"resultType":"vector"}}`,
			wantErr: "no result member",
		},
		{
			name:    "missing resultType",
			body:    `{"status":"success","data":{"result":[]}}`,
			wantErr: "unexpected resultType",
		},
		{
			name:    "streams result",
			body:    `{"status":"success","data":{"resultType":"streams","result":[]}}`,
			wantErr: "unexpected resultType",
		},
		{
			name:    "backend error",
			body:    `{"status":"error","errorType":"bad_data","error":"parse error"}`,
			wantErr: "parse error",
		},
		{
			name:    "unexpected status",
			body:    `{"status":"weird"}`,
			wantErr: `backend status "weird"`,
		},
		{
			name:    "malformed",
			body:    `{"status":`,
			wantErr: "decode response",
		},
		{
			name:    "negative value",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"-1"]}]}}`,
			wantErr: "negative",
		},
		{
			name:    "negative numeric value",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,-2]}]}}`,
			wantErr: "negative",
		},
		{
			name:    "fractional value",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1.5"]}]}}`,
			wantErr: "not an integer",
		},
		{
			name:    "fractional bogus tolerance",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"2.0000001"]}]}}`,
			wantErr: "not an integer",
		},
		{
			name: "exactly 2^53 is accepted",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + maxExact + `"]}]}}`,
			want: int64Ptr(1 << 53),
		},
		{
			name: "decimal string at 2^53 is accepted",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + maxExact + `.0"]}]}}`,
			want: int64Ptr(1 << 53),
		},
		{
			name:    "decimal string above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + aboveMaxExact + `.0"]}]}}`,
			wantErr: "2^53",
		},
		{
			name:    "numeric json above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,` + aboveMaxExact + `]}]}}`,
			wantErr: "2^53",
		},
		{
			name:    "numeric exponent above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,1e19]}]}}`,
			wantErr: "2^53",
		},
		{
			name:    "tiny fraction string",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1.00000000000000001"]}]}}`,
			wantErr: "not an integer",
		},
		{
			name:    "exponent string above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"9.007199254740993e15"]}]}}`,
			wantErr: "2^53",
		},
		{
			name: "scientific notation string is exact",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1e3"]}]}}`,
			want: int64Ptr(1000),
		},
		{
			name: "scientific notation number is exact",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,1e3]}]}}`,
			want: int64Ptr(1000),
		},
		{
			name: "zero with a decimal point",
			body: `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"0.0"]}]}}`,
			want: int64Ptr(0),
		},
		{
			name:    "vector sum above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + maxExact + `"]},{"value":[1,"1"]}]}}`,
			wantErr: "2^53",
		},
		{
			name:    "integer string above 2^53",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + aboveMaxExact + `"]}]}}`,
			wantErr: "2^53",
		},
		{
			name:    "fraction notation is not a number",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"4/2"]}]}}`,
			wantErr: "not a number",
		},
		{
			name:    "hexadecimal is not a number",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"0x10"]}]}}`,
			wantErr: "not a number",
		},
		{
			name:    "wrong value arity",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1]}]}}`,
			wantErr: "want 2",
		},
		{
			name:         "negative stats invalidate bytes only",
			body:         `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"3"]}],"stats":{"summary":{"totalBytesProcessed":-5}}}}`,
			want:         int64Ptr(3),
			wantBytes:    nil,
			wantStatsErr: "negative",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := decodeQueryResponse([]byte(tc.body))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("decode succeeded, want %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tc.want == nil || decoded.count == nil || *decoded.count != *tc.want {
				t.Fatalf("count = %v, want %v", decoded.count, tc.want)
			}
			if (tc.wantBytes == nil) != (decoded.statsBytes == nil) {
				t.Fatalf("stats = %v, want %v", decoded.statsBytes, tc.wantBytes)
			}
			if decoded.statsBytes != nil && *decoded.statsBytes != *tc.wantBytes {
				t.Fatalf("stats = %d, want %d", *decoded.statsBytes, *tc.wantBytes)
			}
			if (tc.wantStatsErr == "") != (decoded.statsErr == nil) {
				t.Fatalf("stats error = %v, want %q", decoded.statsErr, tc.wantStatsErr)
			}
			if decoded.statsErr != nil && !strings.Contains(decoded.statsErr.Error(), tc.wantStatsErr) {
				t.Fatalf("stats error = %q, want it to contain %q", decoded.statsErr, tc.wantStatsErr)
			}
		})
	}
}

func TestParseMetricsText(t *testing.T) {
	t.Run("full exposition", func(t *testing.T) {
		body := `# HELP parkana_query_files_total Files.
# TYPE parkana_query_files_total counter
parkana_query_files_total{result="scanned"} 12
parkana_query_files_total{result="pruned_index"} 3
parkana_query_files_total{result="pruned_labels"} 1
parkana_query_bytes_scanned_total 4096
parkana_querier_cache_fetched_bytes_total 2048
parkana_querier_cache_downloads_total{result="success"} 5
parkana_querier_cache_downloads_total{result="error"} 0
parkana_querier_sidecar_cache_fetched_bytes_total 512
other_metric{kind="x",b="y"} 99
`
		scrape, err := parseMetricsText([]byte(body))
		if err != nil {
			t.Fatalf("parseMetricsText: %v", err)
		}
		for _, tc := range []struct {
			name, result string
			want         int64
		}{
			{"parkana_query_files_total", "scanned", 12},
			{"parkana_query_files_total", "pruned_index", 3},
			{"parkana_query_files_total", "pruned_labels", 1},
			{"parkana_query_bytes_scanned_total", "", 4096},
			{"parkana_querier_cache_fetched_bytes_total", "", 2048},
			{"parkana_querier_cache_downloads_total", "success", 5},
			{"parkana_querier_cache_downloads_total", "error", 0},
			{"parkana_querier_sidecar_cache_fetched_bytes_total", "", 512},
		} {
			got, ok := scrape.value(tc.name, tc.result)
			if !ok || got != tc.want {
				t.Fatalf("%s{%q} = %v (%t), want %v", tc.name, tc.result, got, ok, tc.want)
			}
		}
		if _, ok := scrape.value("other_metric", ""); ok {
			t.Fatal("an unrelated metric was parsed")
		}
	})

	t.Run("unknown families are ignored even when malformed", func(t *testing.T) {
		scrape, err := parseMetricsText([]byte("other_metric{broken\n"))
		if err != nil {
			t.Fatalf("parseMetricsText: %v", err)
		}
		if len(scrape.values) != 0 {
			t.Fatalf("parsed %d values from an unrelated metric", len(scrape.values))
		}
	})

	t.Run("escaped result label", func(t *testing.T) {
		scrape, err := parseMetricsText([]byte(`parkana_query_files_total{result="a\"b\nc"} 1` + "\n"))
		if err != nil {
			t.Fatalf("parseMetricsText: %v", err)
		}
		if _, ok := scrape.value("parkana_query_files_total", "a\"b\nc"); !ok {
			t.Fatalf("escaped label value not parsed: %v", scrape.values)
		}
	})

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"nan", "parkana_query_bytes_scanned_total NaN\n", "not finite"},
		{"infinite", "parkana_query_bytes_scanned_total +Inf\n", "not finite"},
		{"negative", "parkana_query_bytes_scanned_total -1\n", "negative"},
		{"duplicate", "parkana_query_files_total{result=\"scanned\"} 1\nparkana_query_files_total{result=\"scanned\"} 2\n", "duplicate"},
		{"bad value", "parkana_query_bytes_scanned_total lots\n", "value"},
		{"no value", "parkana_query_bytes_scanned_total\n", "no value"},
		{"unterminated value", "parkana_query_files_total{result=\"scanned} 1\n", "unterminated"},
		{"unquoted value", `parkana_query_files_total{result=scanned} 1` + "\n", "unquoted"},
		{"no equals", `parkana_query_files_total{result} 1` + "\n", "no name"},
		{"unterminated label set", `parkana_query_files_total{result="scanned" 1` + "\n", "unterminated label set"},
		{"fractional counter", "parkana_query_bytes_scanned_total 1.5\n", "not an integer"},
		{"exponent above int64", "parkana_query_bytes_scanned_total 1e19\n", "outside the int64"},
		{"tiny exponent fraction", "parkana_query_bytes_scanned_total 1e-3\n", "not an integer"},
		{"integer above int64", "parkana_query_bytes_scanned_total 99999999999999999999\n", "outside the int64"},
	}
	t.Run("exact large counter", func(t *testing.T) {
		// Integer literals are parsed with ParseInt, so values beyond 2^53 keep
		// exact precision and a one-byte delta stays one byte.
		scrape, err := parseMetricsText([]byte("parkana_query_bytes_scanned_total 9007199254740993\n"))
		if err != nil {
			t.Fatalf("parseMetricsText: %v", err)
		}
		if got, _ := scrape.value("parkana_query_bytes_scanned_total", ""); got != 1<<53+1 {
			t.Fatalf("large counter = %d, want %d", got, 1<<53+1)
		}
	})
	t.Run("exact scientific counters", func(t *testing.T) {
		for _, tc := range []struct {
			literal string
			want    int64
		}{
			{"1.234e+06", 1234000},
			{"9.007199254740993e15", 9007199254740993},
			{"9007199254740993e0", 9007199254740993},
			{"2.5e2", 250},
		} {
			got, err := parseCounterValue(tc.literal)
			if err != nil {
				t.Fatalf("parseCounterValue(%q): %v", tc.literal, err)
			}
			if got != tc.want {
				t.Fatalf("parseCounterValue(%q) = %d, want %d", tc.literal, got, tc.want)
			}
		}
		for _, bad := range []string{"4/2", "0x10", "1_000"} {
			if _, err := parseCounterValue(bad); err == nil {
				t.Fatalf("parseCounterValue(%q) accepted a non-decimal literal", bad)
			}
		}
	})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseMetricsText([]byte(tc.body)); err == nil {
				t.Fatalf("parseMetricsText accepted %q", tc.body)
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestDiffScrapes(t *testing.T) {
	body := func(scanned, prunedIndex, prunedLabels, bytesScanned string) string {
		return "parkana_query_files_total{result=\"scanned\"} " + scanned + "\n" +
			"parkana_query_files_total{result=\"pruned_index\"} " + prunedIndex + "\n" +
			"parkana_query_files_total{result=\"pruned_labels\"} " + prunedLabels + "\n" +
			"parkana_query_bytes_scanned_total " + bytesScanned + "\n"
	}
	parse := func(t *testing.T, body string) metricScrape {
		t.Helper()
		scrape, err := parseMetricsText([]byte(body))
		if err != nil {
			t.Fatalf("parseMetricsText: %v", err)
		}
		return scrape
	}
	t.Run("deltas", func(t *testing.T) {
		before := parse(t, body("1", "2", "3", "100")+"parkana_querier_cache_fetched_bytes_total 5\n")
		after := parse(t, body("2", "5", "7", "600")+"parkana_querier_cache_fetched_bytes_total 9\n")
		metrics, err := diffScrapes(before, after)
		if err != nil {
			t.Fatalf("diffScrapes: %v", err)
		}
		want := map[string]int64{
			"scanned": 1, "pruned_index": 3, "pruned_labels": 4, "bytes": 500, "cache": 4,
		}
		if metrics.FilesScanned == nil || *metrics.FilesScanned != want["scanned"] {
			t.Fatalf("files_scanned = %v", metrics.FilesScanned)
		}
		if metrics.FilesPrunedIndex == nil || *metrics.FilesPrunedIndex != want["pruned_index"] {
			t.Fatalf("pruned_index = %v", metrics.FilesPrunedIndex)
		}
		if metrics.FilesPrunedLabels == nil || *metrics.FilesPrunedLabels != want["pruned_labels"] {
			t.Fatalf("pruned_labels = %v", metrics.FilesPrunedLabels)
		}
		if metrics.BytesScanned == nil || *metrics.BytesScanned != want["bytes"] {
			t.Fatalf("bytes_scanned = %v", metrics.BytesScanned)
		}
		if metrics.CacheFetchedBytes == nil || *metrics.CacheFetchedBytes != want["cache"] {
			t.Fatalf("cache_fetched = %v", metrics.CacheFetchedBytes)
		}
		if metrics.SidecarCacheFetchedBytes != nil {
			t.Fatalf("missing optional counter = %v, want null", metrics.SidecarCacheFetchedBytes)
		}
		if len(metrics.MissingSeries) != 3 {
			t.Fatalf("missing series = %v, want the three optional counters", metrics.MissingSeries)
		}
		for _, want := range []string{
			`parkana_querier_cache_downloads_total{result="success"}`,
			`parkana_querier_cache_downloads_total{result="error"}`,
			"parkana_querier_sidecar_cache_fetched_bytes_total",
		} {
			if !slices.Contains(metrics.MissingSeries, want) {
				t.Fatalf("missing series %v does not list %s", metrics.MissingSeries, want)
			}
		}
	})
	t.Run("required missing before", func(t *testing.T) {
		before := parse(t, "parkana_query_files_total{result=\"scanned\"} 1\n"+
			"parkana_query_files_total{result=\"pruned_index\"} 2\n"+
			"parkana_query_bytes_scanned_total 100\n")
		after := parse(t, body("2", "4", "6", "200"))
		metrics, err := diffScrapes(before, after)
		if err == nil || !strings.Contains(err.Error(), "pruned_labels") {
			t.Fatalf("error = %v, want a missing required series", err)
		}
		if metrics.FilesPrunedLabels != nil {
			t.Fatalf("missing required counter = %v, want null", metrics.FilesPrunedLabels)
		}
		if !slices.Contains(metrics.MissingSeries, `parkana_query_files_total{result="pruned_labels"}`) {
			t.Fatalf("missing series = %v", metrics.MissingSeries)
		}
	})
	t.Run("required missing after", func(t *testing.T) {
		before := parse(t, body("1", "2", "3", "100"))
		after := parse(t, "parkana_query_files_total{result=\"scanned\"} 2\n"+
			"parkana_query_files_total{result=\"pruned_index\"} 4\n"+
			"parkana_query_files_total{result=\"pruned_labels\"} 6\n")
		_, err := diffScrapes(before, after)
		if err == nil || !strings.Contains(err.Error(), "bytes_scanned_total") {
			t.Fatalf("error = %v, want a missing required series", err)
		}
	})
	t.Run("reset", func(t *testing.T) {
		before := parse(t, body("5", "2", "3", "500"))
		after := parse(t, body("1", "2", "3", "100"))
		metrics, err := diffScrapes(before, after)
		if err == nil || !strings.Contains(err.Error(), "went backwards") {
			t.Fatalf("error = %v, want a reset", err)
		}
		if metrics.FilesScanned != nil || metrics.BytesScanned != nil {
			t.Fatal("a reset counter produced a delta")
		}
		if !slices.Contains(metrics.ResetSeries, `parkana_query_files_total{result="scanned"}`) {
			t.Fatalf("reset series = %v", metrics.ResetSeries)
		}
	})
	t.Run("exact large delta", func(t *testing.T) {
		// Both scrapes hold counters above 2^53; int64 parsing keeps the
		// one-byte increment exact instead of rounding it to zero.
		before := parse(t, body("1", "2", "3", "9007199254740993"))
		after := parse(t, body("1", "2", "3", "9007199254740994"))
		metrics, err := diffScrapes(before, after)
		if err != nil {
			t.Fatalf("diffScrapes: %v", err)
		}
		if metrics.BytesScanned == nil || *metrics.BytesScanned != 1 {
			t.Fatalf("bytes_scanned delta = %v, want exactly 1", metrics.BytesScanned)
		}
	})
}

func int64Ptr(v int64) *int64 { return new(v) }

func TestReadBoundedBody(t *testing.T) {
	body, err := readBoundedBody(strings.NewReader("abcd"), 4)
	if err != nil || string(body) != "abcd" {
		t.Fatalf("readBoundedBody = %q, %v", body, err)
	}
	if _, err := readBoundedBody(strings.NewReader("abcde"), 4); err == nil {
		t.Fatal("readBoundedBody accepted an oversized body")
	}
}

func TestQueryEndpointDerivation(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantQuery  string
		wantMetric string
	}{
		{"base", "http://127.0.0.1:3100", "http://127.0.0.1:3100/loki/api/v1/query", "http://127.0.0.1:3100/metrics"},
		{"trailing slash", "http://127.0.0.1:3100/", "http://127.0.0.1:3100/loki/api/v1/query", "http://127.0.0.1:3100/metrics"},
		{"prefix", "http://host/tenant/a", "http://host/tenant/a/loki/api/v1/query", "http://host/tenant/a/metrics"},
		{"full endpoint", "https://host/loki/api/v1/query?keep=no", "https://host/loki/api/v1/query", "https://host/metrics"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, err := queryEndpoint(tc.url)
			if err != nil {
				t.Fatalf("queryEndpoint: %v", err)
			}
			if endpoint.String() != tc.wantQuery {
				t.Fatalf("query endpoint = %s, want %s", endpoint, tc.wantQuery)
			}
			if metrics := deriveMetricsURL(endpoint); metrics.String() != tc.wantMetric {
				t.Fatalf("metrics endpoint = %s, want %s", metrics, tc.wantMetric)
			}
		})
	}
	for _, bad := range []string{"", "ftp://host", "http://", "not a url"} {
		if _, err := queryEndpoint(bad); err == nil {
			t.Fatalf("queryEndpoint(%q) accepted an invalid URL", bad)
		}
	}
	if _, err := metricsEndpoint("ftp://host/metrics"); err == nil {
		t.Fatal("metricsEndpoint accepted a non-HTTP URL")
	}
}

// fakeQueryBackend answers exactly the expected queries and serves a mutable
// metrics exposition.
type fakeQueryBackend struct {
	t        *testing.T
	counts   map[string]int64
	stats    *int64
	statsFn  func(*fakeQueryBackend) *int64
	queries  []queryRecord
	scrapes  int
	override http.HandlerFunc

	mu             sync.Mutex
	scanned        int64
	prunedIndex    int64
	prunedLabels   int64
	bytesScanned   int64
	cacheFetched   int64
	cacheSuccess   int64
	sidecarFetched int64
	metricBody     func(*fakeQueryBackend) string
}

// queryRecord is one captured instant query.
type queryRecord struct {
	query  string
	at     time.Time
	tenant string
}

func (b *fakeQueryBackend) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /loki/api/v1/query", b.query)
	mux.HandleFunc("GET /metrics", b.metrics)
	return mux
}

func (b *fakeQueryBackend) query(w http.ResponseWriter, r *http.Request) {
	if b.override != nil {
		b.override(w, r)
		return
	}
	query := r.URL.Query().Get("query")
	count, ok := b.counts[query]
	if !ok {
		b.t.Errorf("unexpected query %q", query)
		http.Error(w, "unexpected query", http.StatusBadRequest)
		return
	}
	at, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("time"))
	if err != nil {
		b.t.Errorf("instant time parameter: %v", err)
	}
	b.mu.Lock()
	b.queries = append(b.queries, queryRecord{query: query, at: at, tenant: r.Header.Get(tenant.Header)})
	b.scanned++
	b.prunedIndex += 2
	b.prunedLabels += 3
	b.bytesScanned += 1000
	b.cacheFetched += 10
	b.cacheSuccess++
	b.sidecarFetched += 5
	stats := b.stats
	if b.statsFn != nil {
		stats = b.statsFn(b)
	}
	b.mu.Unlock()
	data := map[string]any{
		"resultType": "vector",
		"result": []any{
			map[string]any{"metric": map[string]any{}, "value": []any{float64(at.Unix()), fmt.Sprintf("%d", count)}},
		},
	}
	if stats != nil {
		data["stats"] = map[string]any{"summary": map[string]any{"totalBytesProcessed": *stats}}
	}
	writeJSONResponse(b.t, w, map[string]any{"status": "success", "data": data})
}

func (b *fakeQueryBackend) metrics(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.scrapes++
	body := b.defaultMetricBody()
	if b.metricBody != nil {
		body = b.metricBody(b)
	}
	b.mu.Unlock()
	_, _ = io.WriteString(w, body)
}

// records returns a copy of the captured queries.
func (b *fakeQueryBackend) records() []queryRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]queryRecord(nil), b.queries...)
}

// scrapeCount returns the number of metrics scrapes served.
func (b *fakeQueryBackend) scrapeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.scrapes
}

// defaultMetricBody renders the fixed exposition with all required series and
// the optional cache counters.
func (b *fakeQueryBackend) defaultMetricBody() string {
	return fmt.Sprintf(`# HELP parkana_query_files_total Files per query.
# TYPE parkana_query_files_total counter
parkana_query_files_total{result="scanned"} %d
parkana_query_files_total{result="pruned_index"} %d
parkana_query_files_total{result="pruned_labels"} %d
# TYPE parkana_query_bytes_scanned_total counter
parkana_query_bytes_scanned_total %d
parkana_querier_cache_fetched_bytes_total %d
parkana_querier_cache_downloads_total{result="success"} %d
parkana_querier_cache_downloads_total{result="error"} 0
parkana_querier_sidecar_cache_fetched_bytes_total %d
parkana_unrelated_metric{kind="other"} 42
`, b.scanned, b.prunedIndex, b.prunedLabels, b.bytesScanned, b.cacheFetched, b.cacheSuccess, b.sidecarFetched)
}

// writeJSONResponse writes one JSON body.
func writeJSONResponse(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Errorf("marshal response: %v", err)
		http.Error(w, "encode", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// expectedQueryCounts maps every query the command must send to its manifest
// count.
func expectedQueryCounts(t *testing.T, manifest Report, cfg QueryConfig) map[string]int64 {
	t.Helper()
	queryPlan, err := newQueryPlan(&cfg, &manifest)
	if err != nil {
		t.Fatalf("newQueryPlan: %v", err)
	}
	defs, err := queryDefinitions(queryPlan, manifest)
	if err != nil {
		t.Fatalf("queryDefinitions: %v", err)
	}
	counts := make(map[string]int64, len(defs))
	for _, def := range defs {
		counts[def.Query] = def.Expected
	}
	return counts
}

// defaultQueryConfig is the config the CLI tests run with.
func defaultQueryConfig(backend, url string) QueryConfig {
	return QueryConfig{
		Backend:     backend,
		URL:         url,
		Repetitions: 2,
		Warmups:     1,
		Timeout:     5 * time.Second,
	}
}

func TestQueryParkanaEndToEnd(t *testing.T) {
	manifest := buildTestManifest(t)
	manifest.Corpus.Tenant = "acme"
	metrics := int64(999)
	backend := &fakeQueryBackend{t: t, stats: &metrics}
	cfg := defaultQueryConfig(backendParkana, "http://placeholder")
	cfg.Tenant = "acme"
	backend.counts = expectedQueryCounts(t, manifest, QueryConfig{Backend: backendParkana, URL: "http://placeholder", Repetitions: cfg.Repetitions, Warmups: cfg.Warmups})
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	manifestPath := writeManifestFile(t, dir, manifest)
	outputPath := filepath.Join(dir, "parkana.json")
	markdownPath := filepath.Join(dir, "parkana.md")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", manifestPath,
		"--repetitions", "2",
		"--warmups", "1",
		"--output", outputPath,
		"--markdown", markdownPath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("query command: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), outputPath) {
		t.Fatalf("summary does not mention the report path: %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), markdownPath) {
		t.Fatalf("summary does not mention the markdown path: %q", stdout.String())
	}
	body, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if !report.Valid || len(report.Errors) != 0 {
		t.Fatalf("report invalid: %+v", report.Errors)
	}
	if report.Backend != backendParkana || report.Tenant != "acme" {
		t.Fatalf("report backend/tenant = %q/%q", report.Backend, report.Tenant)
	}
	if len(report.Series) != 8 {
		t.Fatalf("series = %d, want 8", len(report.Series))
	}
	if report.FirstObservation == nil {
		t.Fatal("report has no first observation")
	}
	if report.FirstObservation.Class != queryClassFrequent || report.FirstObservation.Window != queryWindowShort {
		t.Fatalf("first observation = %s/%s, want the first configured series", report.FirstObservation.Class, report.FirstObservation.Window)
	}
	if report.FirstObservation.ControlledRestart {
		t.Fatal("first observation claims a controlled restart")
	}
	for _, series := range report.Series {
		if series.SampleCount != 2 || series.Failures != 0 || series.Timeouts != 0 {
			t.Fatalf("%s/%s samples = %d failures = %d timeouts = %d", series.Class, series.Window, series.SampleCount, series.Failures, series.Timeouts)
		}
		if series.P50MS == nil || series.P99MS == nil {
			t.Fatalf("%s/%s has no percentile", series.Class, series.Window)
		}
		if series.ObservedCount == nil || *series.ObservedCount != series.ExpectedCount {
			t.Fatalf("%s/%s observed = %v, expected %d", series.Class, series.Window, series.ObservedCount, series.ExpectedCount)
		}
		if !series.CountsConsistent {
			t.Fatalf("%s/%s counts inconsistent", series.Class, series.Window)
		}
		if series.FilesScanned == nil || *series.FilesScanned != 1 {
			t.Fatalf("%s/%s files_scanned = %v, want 1", series.Class, series.Window, series.FilesScanned)
		}
		if series.FilesPrunedIndex == nil || *series.FilesPrunedIndex != 2 {
			t.Fatalf("%s/%s files_pruned_index = %v, want 2", series.Class, series.Window, series.FilesPrunedIndex)
		}
		if series.FilesPrunedLabels == nil || *series.FilesPrunedLabels != 3 {
			t.Fatalf("%s/%s files_pruned_labels = %v, want 3", series.Class, series.Window, series.FilesPrunedLabels)
		}
		if series.BytesScanned == nil || *series.BytesScanned != 1000 {
			t.Fatalf("%s/%s bytes_scanned = %v, want 1000", series.Class, series.Window, series.BytesScanned)
		}
		if series.CacheFetchedBytes == nil || *series.CacheFetchedBytes != 10 {
			t.Fatalf("%s/%s cache_fetched_bytes = %v, want 10", series.Class, series.Window, series.CacheFetchedBytes)
		}
		if series.CacheDownloadsError == nil || *series.CacheDownloadsError != 0 {
			t.Fatalf("%s/%s cache_downloads_error = %v, want a reported 0", series.Class, series.Window, series.CacheDownloadsError)
		}
		if series.SidecarCacheFetchedBytes == nil || *series.SidecarCacheFetchedBytes != 5 {
			t.Fatalf("%s/%s sidecar bytes = %v, want 5", series.Class, series.Window, series.SidecarCacheFetchedBytes)
		}
		if series.LokiTotalBytesProcessed == nil || *series.LokiTotalBytesProcessed != 999 {
			t.Fatalf("%s/%s stats bytes = %v, want 999", series.Class, series.Window, series.LokiTotalBytesProcessed)
		}
		if !series.MetricsConsistent {
			t.Fatalf("%s/%s metrics inconsistent", series.Class, series.Window)
		}
	}
	records := backend.records()
	if len(records) != len(report.Series)*(report.Config.Warmups+report.Config.Repetitions) {
		t.Fatalf("queries = %d, want warmups+repetitions per series", len(records))
	}
	for _, record := range records {
		if record.tenant != "acme" {
			t.Fatalf("query tenant header = %q, want acme", record.tenant)
		}
	}
	if strings.Contains(string(body), `"cold"`) {
		t.Fatal("report still uses the cold field name")
	}
	if !strings.Contains(string(body), `"first_observation"`) {
		t.Fatal("report has no first_observation field")
	}

	markdown, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatalf("read markdown: %v", err)
	}
	text := string(markdown)
	for _, want := range []string{
		"## parkana query benchmark",
		"valid measurement",
		"first observation",
		"Parkana scan and prune counters",
		"candidate bytes",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("markdown is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(strings.ToLower(text), "cold") {
		t.Fatalf("markdown calls the first request cold:\n%s", text)
	}
}

func TestQueryLokiEndToEnd(t *testing.T) {
	manifest := buildTestManifest(t)
	stats := int64(4242)
	backend := &fakeQueryBackend{t: t, stats: &stats}
	backend.counts = expectedQueryCounts(t, manifest, QueryConfig{Backend: backendLoki, URL: "http://placeholder", Repetitions: 2, Warmups: 1})
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	manifestPath := writeManifestFile(t, dir, manifest)
	outputPath := filepath.Join(dir, "loki.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", manifestPath,
		"--repetitions", "2",
		"--warmups", "1",
		"--output", outputPath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("query command: %v\nstderr: %s", err, stderr.String())
	}
	body, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if !report.Valid {
		t.Fatalf("loki report invalid: %+v", report.Errors)
	}
	if backend.scrapeCount() != 0 {
		t.Fatalf("loki backend scraped metrics %d times", backend.scrapeCount())
	}
	for _, series := range report.Series {
		if series.LokiTotalBytesProcessed == nil || *series.LokiTotalBytesProcessed != 4242 {
			t.Fatalf("%s/%s stats bytes = %v, want 4242", series.Class, series.Window, series.LokiTotalBytesProcessed)
		}
		if !series.LokiBytesConsistent {
			t.Fatalf("%s/%s stats inconsistent", series.Class, series.Window)
		}
		if series.FilesScanned != nil || series.BytesScanned != nil {
			t.Fatalf("%s/%s has parkana counters on a loki run", series.Class, series.Window)
		}
	}
}

func TestQueryLokiMissingStatsInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	cfg := QueryConfig{
		Backend: backendLoki, URL: "http://placeholder", Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "loki.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a loki run without totalBytesProcessed was accepted")
	}
	if !strings.Contains(err.Error(), "totalBytesProcessed") {
		t.Fatalf("error = %q, want a totalBytesProcessed failure", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if series.LokiTotalBytesProcessed != nil {
		t.Fatalf("missing stats reported as %d", *series.LokiTotalBytesProcessed)
	}
	if series.P50MS == nil {
		t.Fatal("a valid latency was discarded because the byte stat was missing")
	}
	if series.Samples[0].StatsError == "" {
		t.Fatal("missing stats were not recorded on the sample")
	}
	if report.Valid {
		t.Fatal("a byte-bearing run without totalBytesProcessed is valid")
	}
	if series.Valid {
		t.Fatal("the affected series is still valid")
	}
	if !strings.Contains(series.Error, "totalBytesProcessed") {
		t.Fatalf("series error = %q, want the missing stats reason", series.Error)
	}
}

func TestQueryCountMismatchInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	cfg := QueryConfig{
		Backend: backendParkana, URL: "http://placeholder", Repetitions: 2, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	// Break exactly one count: the single configured query.
	for query := range backend.counts {
		backend.counts[query]++
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "2",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a count mismatch was accepted")
	}
	if !strings.Contains(err.Error(), "expected") {
		t.Fatalf("error = %q, want an expected-count mismatch", err)
	}
	body, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Valid {
		t.Fatal("count mismatch report is valid")
	}
	if len(report.Series) != 1 || report.Series[0].Valid {
		t.Fatalf("series = %+v, want one invalid series", report.Series)
	}
	if report.Series[0].Failures != 1 {
		t.Fatalf("count mismatch failures = %d, want 1", report.Series[0].Failures)
	}
	if report.Series[0].P50MS != nil || report.Series[0].SampleCount != 0 {
		t.Fatalf("mismatched count produced a latency sample: %+v", report.Series[0])
	}
	if report.Series[0].CountsConsistent {
		t.Fatal("mismatched count reported as consistent")
	}
}

func TestQueryHTTPErrorInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassRare,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("error = %v, want an HTTP 500 failure", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Valid || report.Series[0].Valid {
		t.Fatal("a 500 response left the run valid")
	}
	if report.Series[0].Failures != 1 || report.Series[0].Timeouts != 0 {
		t.Fatalf("failure totals = %d failures / %d timeouts, want 1/0", report.Series[0].Failures, report.Series[0].Timeouts)
	}
}

func TestQueryTimeoutRecorded(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--timeout", "50ms",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a timed-out query was accepted")
	}
	body, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Valid || len(report.Series) != 1 {
		t.Fatalf("timeout report = valid %t, series %d", report.Valid, len(report.Series))
	}
	series := report.Series[0]
	if series.Timeouts != 1 || len(series.Samples) != 1 || !series.Samples[0].Timeout {
		t.Fatalf("timeout not recorded: %+v", series)
	}
	if series.P50MS != nil {
		t.Fatal("timed-out sample entered the percentile")
	}
	if report.FirstObservation == nil || !report.FirstObservation.Timeout {
		t.Fatalf("first observation = %+v, want the timed-out query", report.FirstObservation)
	}
}

func TestQueryCanceledStopsBeforeRequests(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		t.Error("a canceled run issued a query")
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cfg := defaultQueryConfig(backendParkana, server.URL)
	report, err := RunQueries(ctx, cfg, manifest)
	if err == nil {
		t.Fatal("a canceled run succeeded")
	}
	if !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("error = %q, want a cancellation", err)
	}
	if report == nil {
		t.Fatal("canceled run returned no report")
	}
	if report.Valid {
		t.Fatal("canceled run reported valid")
	}
	if report.FirstObservation != nil {
		t.Fatal("canceled run recorded a first observation without a request")
	}
}

func TestQueryRequiredMetricsMissingInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	cfg := QueryConfig{
		Backend: backendParkana, URL: "http://placeholder", Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	// pruned_labels is never reported; the other required series stay present.
	backend.metricBody = func(b *fakeQueryBackend) string {
		return fmt.Sprintf(`parkana_query_files_total{result="scanned"} %d
parkana_query_files_total{result="pruned_index"} %d
parkana_query_bytes_scanned_total %d
`, b.scanned, b.prunedIndex, b.bytesScanned)
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "pruned_labels") {
		t.Fatalf("error = %v, want a missing pruned_labels failure", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if series.FilesPrunedLabels != nil {
		t.Fatalf("missing counter reported as %v", *series.FilesPrunedLabels)
	}
	if series.FilesScanned == nil || *series.FilesScanned != 1 {
		t.Fatalf("present counter = %v, want the real delta", series.FilesScanned)
	}
	if !slices.Contains(series.MetricsMissing, `parkana_query_files_total{result="pruned_labels"}`) {
		t.Fatalf("metrics_missing = %v", series.MetricsMissing)
	}
	if report.Valid {
		t.Fatal("a run with a missing required counter is valid")
	}
	if series.Valid {
		t.Fatal("the affected series is still valid")
	}
	if !strings.Contains(series.Error, "pruned_labels") {
		t.Fatalf("series error = %q, want the missing counter reason", series.Error)
	}
}

func TestQueryMetricsResetInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	cfg := QueryConfig{
		Backend: backendParkana, URL: "http://placeholder", Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	// Every scrape reports a smaller counter than the previous one, which is a
	// reset: no delta may be derived from it.
	backend.metricBody = func(b *fakeQueryBackend) string {
		value := int64(100000 - b.scrapes*1000)
		return fmt.Sprintf(`parkana_query_files_total{result="scanned"} %d
parkana_query_files_total{result="pruned_index"} %d
parkana_query_files_total{result="pruned_labels"} %d
parkana_query_bytes_scanned_total %d
`, b.scanned, b.prunedIndex, b.prunedLabels, value)
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", filepath.Join(dir, "report.json"),
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "went backwards") {
		t.Fatalf("error = %v, want a counter reset failure", err)
	}
	body, readErr := os.ReadFile(filepath.Join(dir, "report.json"))
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Series[0].Valid {
		t.Fatal("a reset counter left the series valid")
	}
	if !strings.Contains(report.Series[0].Error, "went backwards") {
		t.Fatalf("series error = %q", report.Series[0].Error)
	}
}

func TestQueryMetricsScrapeFailureInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	cfg := QueryConfig{
		Backend: backendParkana, URL: "http://placeholder", Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /loki/api/v1/query", backend.query)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no metrics", http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "metrics endpoint returned") {
		t.Fatalf("error = %v, want a metrics scrape failure", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Series[0].Samples[0].Metrics != nil {
		t.Fatal("a failed scrape produced metric deltas")
	}
	if report.Series[0].Samples[0].MetricsError == "" {
		t.Fatal("a failed scrape was not recorded")
	}
	if report.Series[0].Valid {
		t.Fatal("a failed scrape left the series valid")
	}
}

func TestQueryFlagValidation(t *testing.T) {
	manifest := buildTestManifest(t)
	manifestPath := writeManifestFile(t, t.TempDir(), manifest)
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		t.Error("an invalid invocation issued a request")
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing manifest", []string{"query", "--backend", "loki", "--url", server.URL}, "manifest"},
		{"missing backend", []string{"query", "--url", server.URL, "--manifest", manifestPath}, "-backend is required"},
		{"unknown backend", []string{"query", "--backend", "es", "--url", server.URL, "--manifest", manifestPath}, "-backend must be"},
		{"missing url", []string{"query", "--backend", "loki", "--manifest", manifestPath}, "-url is required"},
		{"bad scheme", []string{"query", "--backend", "loki", "--url", "ftp://host", "--manifest", manifestPath}, "http or https"},
		{"no host", []string{"query", "--backend", "loki", "--url", "http://", "--manifest", manifestPath}, "no host"},
		{"zero repetitions", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--repetitions", "0"}, "-repetitions"},
		{"negative warmups", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--warmups", "-1"}, "-warmups"},
		{"negative timeout", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--timeout", "-1s"}, "-timeout"},
		{"unknown class", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--classes", "bogus"}, "unknown class"},
		{"unknown window", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--windows", "medium"}, "unknown window"},
		{"metrics url on loki", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--metrics-url", server.URL + "/metrics"}, "-metrics-url is only valid"},
		{"extra arguments", []string{"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "extra"}, "unexpected arguments"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(t.Context(), tc.args, io.Discard, io.Discard)
			if err == nil {
				t.Fatalf("args %v were accepted", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
	if len(backend.records()) != 0 {
		t.Fatalf("invalid invocations sent %d queries", len(backend.records()))
	}

	tenantManifest := buildTestManifest(t)
	tenantManifest.Corpus.Tenant = "acme"
	tenantPath := writeManifestFile(t, t.TempDir(), tenantManifest)
	err := run(t.Context(), []string{
		"query", "--backend", "loki", "--url", server.URL, "--manifest", tenantPath, "--tenant", "other",
	}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match the manifest's effective tenant") {
		t.Fatalf("tenant mismatch error = %v", err)
	}
	// An empty manifest tenant means the default tenant: naming a different one
	// is rejected, while explicitly confirming the default is allowed and sends
	// that header.
	err = run(t.Context(), []string{
		"query", "--backend", "loki", "--url", server.URL, "--manifest", manifestPath, "--tenant", "acme",
	}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match the manifest's effective tenant") {
		t.Fatalf("default-tenant mismatch error = %v", err)
	}
	if len(backend.records()) != 0 {
		t.Fatalf("a tenant mismatch sent %d queries", len(backend.records()))
	}

	defaultBackend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	defaultCfg := QueryConfig{
		Backend: backendLoki, URL: "http://placeholder", Tenant: tenant.Default, Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	defaultBackend.counts = expectedQueryCounts(t, manifest, defaultCfg)
	defaultServer := httptest.NewServer(defaultBackend.handler())
	t.Cleanup(defaultServer.Close)
	err = run(t.Context(), []string{
		"query", "--backend", "loki", "--url", defaultServer.URL, "--manifest", manifestPath,
		"--tenant", string(tenant.Default), "--repetitions", "1", "--warmups", "0",
		"--classes", queryClassFrequent, "--windows", queryWindowLong,
	}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("explicit default tenant run: %v", err)
	}
	records := defaultBackend.records()
	if len(records) != 1 || records[0].tenant != string(tenant.Default) {
		t.Fatalf("explicit default tenant header = %q, want %q", records[0].tenant, tenant.Default)
	}
}

func TestQueryRejectsDirtyManifestBeforeIO(t *testing.T) {
	manifest := buildTestManifest(t)
	manifest.Delivery.Clean = false
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		t.Error("a dirty manifest reached the backend")
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, t.TempDir(), manifest),
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "clean delivery") {
		t.Fatalf("error = %v, want a dirty-delivery rejection", err)
	}
}

func TestQueryRejectsUnsupportedManifestVersion(t *testing.T) {
	// Version 1 is the pre-rotation corpus and version 2 has no instance
	// labels; a query report built from either would misrepresent the current
	// corpus identity.
	tests := []struct {
		name    string
		version int
	}{
		{"old version 1", 1},
		{"old version 2", 2},
		{"future version", corpusVersion + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifest := buildTestManifest(t)
			manifest.Corpus.Version = tc.version
			backend := &fakeQueryBackend{t: t}
			server := httptest.NewServer(backend.handler())
			t.Cleanup(server.Close)

			var stdout, stderr bytes.Buffer
			err := run(t.Context(), []string{
				"query",
				"--backend", backendLoki,
				"--url", server.URL,
				"--manifest", writeManifestFile(t, t.TempDir(), manifest),
			}, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "unsupported manifest version") {
				t.Fatalf("error = %v, want an unsupported-version rejection", err)
			}
			if got := len(backend.records()); got != 0 {
				t.Fatalf("rejected manifest issued %d backend queries", got)
			}
			if got := backend.scrapeCount(); got != 0 {
				t.Fatalf("rejected manifest issued %d backend metric scrapes", got)
			}
		})
	}
}

func TestQueryManifestFileDecodeError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := loadIngestManifest(path); err == nil || !strings.Contains(err.Error(), "decode manifest") {
		t.Fatalf("error = %v, want a decode failure", err)
	}
	if _, err := loadIngestManifest(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("a missing manifest was accepted")
	}
}

func TestQueryNullVectorResultInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":null}}`)
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassAbsent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "result is null") {
		t.Fatalf("error = %v, want a null-result rejection", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if report.Valid || series.Valid {
		t.Fatal("a null vector result passed as a valid absent measurement")
	}
	if series.ObservedCount != nil {
		t.Fatalf("null result produced observed_count %d", *series.ObservedCount)
	}
	if !strings.Contains(series.Error, "result is null") {
		t.Fatalf("series error = %q", series.Error)
	}
}

func TestQueryNegativeStatsInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(-5)}
	cfg := QueryConfig{
		Backend: backendLoki, URL: "http://placeholder", Repetitions: 1, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("error = %v, want a negative byte stat rejection", err)
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if series.LokiTotalBytesProcessed != nil {
		t.Fatalf("negative bytes reported as %d", *series.LokiTotalBytesProcessed)
	}
	if series.P50MS == nil {
		t.Fatal("a valid count latency was discarded because the byte stat was negative")
	}
	if series.Valid {
		t.Fatal("a negative byte stat left the series valid")
	}
	if !strings.Contains(series.Error, "negative") {
		t.Fatalf("series error = %q", series.Error)
	}
}

// stallingHandler flushes a 200 response header and then blocks until the
// client gives up or the test releases it.
func stallingHandler(release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
}

// TestQueryBodyStallIsTimeout proves a 200 whose headers arrive but whose body
// stalls is classified as a timeout, not a generic read failure.
func TestQueryBodyStallIsTimeout(t *testing.T) {
	manifest := buildTestManifest(t)
	release := make(chan struct{})
	server := httptest.NewServer(stallingHandler(release))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "0",
		"--timeout", "50ms",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a stalled body was accepted")
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if series.Timeouts != 1 || len(series.Samples) != 1 || !series.Samples[0].Timeout {
		t.Fatalf("body stall not recorded as a timeout: %+v", series)
	}
	if !report.FirstObservation.Timeout {
		t.Fatal("the first observation did not preserve the timeout flag")
	}
	if series.Valid || report.Valid {
		t.Fatal("a timed-out request is valid")
	}
}

// TestQueryWarmupFailuresCounted proves warmup errors and timeouts reach the
// series totals even though warmups are not timed samples.
func TestQueryWarmupFailuresCounted(t *testing.T) {
	manifest := buildTestManifest(t)
	release := make(chan struct{})
	server := httptest.NewServer(stallingHandler(release))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "1",
		"--warmups", "1",
		"--timeout", "50ms",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a timed-out warmup was accepted")
	}
	var report QueryReport
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if series.Timeouts != 1 || series.Failures != 0 {
		t.Fatalf("warmup timeout totals = %d timeouts / %d failures, want 1/0", series.Timeouts, series.Failures)
	}
	if len(series.Samples) != 0 || series.P50MS != nil {
		t.Fatalf("a failed warmup produced timed samples: %+v", series)
	}
	if series.Valid {
		t.Fatal("a failed warmup left the series valid")
	}

	// The same accounting for a non-timeout warmup error.
	errorBackend := &fakeQueryBackend{t: t}
	errorBackend.override = func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}
	errorServer := httptest.NewServer(errorBackend.handler())
	t.Cleanup(errorServer.Close)
	errorPath := filepath.Join(t.TempDir(), "report.json")
	stdout.Reset()
	stderr.Reset()
	err = run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", errorServer.URL,
		"--manifest", writeManifestFile(t, t.TempDir(), manifest),
		"--repetitions", "1",
		"--warmups", "1",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", errorPath,
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("a failed warmup was accepted")
	}
	body, readErr = os.ReadFile(errorPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series = report.Series[0]
	if series.Failures != 1 || series.Timeouts != 0 {
		t.Fatalf("warmup error totals = %d timeouts / %d failures, want 0/1", series.Timeouts, series.Failures)
	}
}

// TestQueryVaryingCountersNotInvalid proves that per-sample counter variation
// (cache warming, a compaction changing the file set) withholds the summary but
// never invalidates a valid latency/count measurement.
func TestQueryVaryingCountersNotInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t, stats: int64Ptr(1)}
	cfg := QueryConfig{
		Backend: backendParkana, URL: "http://placeholder", Repetitions: 2, Warmups: 0,
		Classes: []string{queryClassFrequent}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	backend.metricBody = func(b *fakeQueryBackend) string {
		// Square the scrape count so each delta differs from the previous one,
		// for both a required series and an optional cache counter.
		varying := int64(b.scrapes) * int64(b.scrapes)
		return fmt.Sprintf(`parkana_query_files_total{result="scanned"} %d
parkana_query_files_total{result="pruned_index"} %d
parkana_query_files_total{result="pruned_labels"} %d
parkana_query_bytes_scanned_total %d
parkana_querier_cache_fetched_bytes_total %d
`, varying, b.prunedIndex, b.prunedLabels, b.bytesScanned, varying)
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	outputPath := filepath.Join(dir, "report.json")
	markdownPath := filepath.Join(dir, "report.md")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendParkana,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "2",
		"--warmups", "0",
		"--classes", queryClassFrequent,
		"--windows", queryWindowLong,
		"--output", outputPath,
		"--markdown", markdownPath,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("varying diagnostics invalidated the run: %v\nstderr: %s", err, stderr.String())
	}
	body, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if !report.Valid || !series.Valid {
		t.Fatalf("varying counters invalidated a valid run: %q", series.Error)
	}
	if series.MetricsConsistent {
		t.Fatal("varying counters reported as consistent")
	}
	if series.FilesScanned != nil || series.CacheFetchedBytes != nil {
		t.Fatalf("varying summaries kept: scanned=%v cache=%v", series.FilesScanned, series.CacheFetchedBytes)
	}
	if series.FilesPrunedIndex == nil || *series.FilesPrunedIndex != 2 {
		t.Fatalf("consistent counter summary = %v, want 2", series.FilesPrunedIndex)
	}
	if len(series.Samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(series.Samples))
	}
	first := series.Samples[0].Metrics.FilesScanned
	second := series.Samples[1].Metrics.FilesScanned
	if first == nil || second == nil || *first == *second {
		t.Fatalf("per-sample values = %v / %v, want the differing originals", first, second)
	}
	markdown, readErr := os.ReadFile(markdownPath)
	if readErr != nil {
		t.Fatalf("read markdown: %v", readErr)
	}
	if !strings.Contains(string(markdown), "varies") {
		t.Fatalf("markdown does not expose the variation:\n%s", markdown)
	}
	if !strings.Contains(string(markdown), "valid measurement") {
		t.Fatalf("markdown does not report the run as valid:\n%s", markdown)
	}
}

// TestQueryVaryingStatsNotInvalid covers Loki's logical byte statistic.
func TestQueryVaryingStatsNotInvalid(t *testing.T) {
	manifest := buildTestManifest(t)
	backend := &fakeQueryBackend{t: t}
	backend.statsFn = func(b *fakeQueryBackend) *int64 { return new(int64(len(b.queries))) }
	cfg := QueryConfig{
		Backend: backendLoki, URL: "http://placeholder", Repetitions: 2, Warmups: 0,
		Classes: []string{queryClassRare}, Windows: []string{queryWindowLong},
	}
	backend.counts = expectedQueryCounts(t, manifest, cfg)
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"query",
		"--backend", backendLoki,
		"--url", server.URL,
		"--manifest", writeManifestFile(t, dir, manifest),
		"--repetitions", "2",
		"--warmups", "0",
		"--classes", queryClassRare,
		"--windows", queryWindowLong,
		"--output", filepath.Join(dir, "report.json"),
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("varying stats invalidated the run: %v", err)
	}
	body, readErr := os.ReadFile(filepath.Join(dir, "report.json"))
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	var report QueryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	series := report.Series[0]
	if !report.Valid || !series.Valid || series.P50MS == nil {
		t.Fatalf("varying stats invalidated the latency: %+v", series)
	}
	if series.LokiTotalBytesProcessed != nil || series.LokiBytesConsistent {
		t.Fatalf("varying stats summaries = %v consistent=%t, want null/false", series.LokiTotalBytesProcessed, series.LokiBytesConsistent)
	}
}

func TestQueryArtifactPathConflicts(t *testing.T) {
	manifest := buildTestManifest(t)
	dir := t.TempDir()
	manifestPath := writeManifestFile(t, dir, manifest)
	original, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	realDir := filepath.Join(dir, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	aliasDir := filepath.Join(dir, "alias")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatalf("symlink alias: %v", err)
	}
	backend := &fakeQueryBackend{t: t}
	backend.override = func(w http.ResponseWriter, r *http.Request) {
		t.Error("a conflicting artifact path reached the backend")
	}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	tests := []struct {
		name     string
		args     []string
		artifact string
	}{
		{"output equals markdown", []string{"--output", filepath.Join(dir, "out.md"), "--markdown", filepath.Join(dir, "out.md")}, filepath.Join(dir, "out.md")},
		{"relative alias", []string{"--output", filepath.Join(dir, "out.json"), "--markdown", filepath.Join(dir, ".", "out.json")}, filepath.Join(dir, "out.json")},
		{"output overwrites manifest", []string{"--output", manifestPath}, manifestPath},
		{"markdown overwrites manifest", []string{"--markdown", manifestPath}, manifestPath},
		{"nonexistent alias under a symlinked parent", []string{"--output", filepath.Join(realDir, "new.json"), "--markdown", filepath.Join(aliasDir, "new.json")}, filepath.Join(realDir, "new.json")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"query", "--backend", backendLoki, "--url", server.URL, "--manifest", manifestPath}
			err := run(t.Context(), append(args, tc.args...), io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "must not name the same file") {
				t.Fatalf("error = %v, want a conflicting artifact rejection", err)
			}
			if tc.artifact != manifestPath {
				if _, statErr := os.Stat(tc.artifact); !os.IsNotExist(statErr) {
					t.Fatalf("the rejected conflict created %s", tc.artifact)
				}
			}
		})
	}
	current, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest after conflicts: %v", err)
	}
	if !bytes.Equal(original, current) {
		t.Fatal("a conflicting artifact path modified the manifest")
	}
	if len(backend.records()) != 0 {
		t.Fatalf("a conflicting artifact path sent %d queries", len(backend.records()))
	}
}

func TestSameFilePathAliases(t *testing.T) {
	dir := t.TempDir()
	realDir := filepath.Join(dir, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	aliasDir := filepath.Join(dir, "alias")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	// Nonexistent files through a symlinked parent are the same destination.
	if !sameFilePath(filepath.Join(realDir, "new.json"), filepath.Join(aliasDir, "new.json")) {
		t.Fatal("nonexistent files behind a symlinked parent did not compare equal")
	}
	// Different new files are not the same destination.
	if sameFilePath(filepath.Join(realDir, "a.json"), filepath.Join(realDir, "b.json")) {
		t.Fatal("two distinct new files compared equal")
	}
	// Existing hard links are the same file.
	first := filepath.Join(realDir, "first.json")
	if err := os.WriteFile(first, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	second := filepath.Join(realDir, "second.json")
	if err := os.Link(first, second); err != nil {
		t.Fatalf("hard link: %v", err)
	}
	if !sameFilePath(first, second) {
		t.Fatal("hard links did not compare equal")
	}
	// An existing file and its symlink alias are the same file.
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(first, link); err != nil {
		t.Fatalf("symlink file: %v", err)
	}
	if !sameFilePath(first, link) {
		t.Fatal("file and symlink did not compare equal")
	}
}

func TestQueryWindowRoundedBoundsBeyondRange(t *testing.T) {
	// Both manifest bounds fit, but rounding the end up to a whole millisecond
	// would leave the int64 nanosecond range.
	through := time.Unix(0, math.MaxInt64).UTC().Truncate(time.Microsecond)
	from := through.Add(-1500 * time.Microsecond)
	if _, err := newQueryWindow("long", from, through); err == nil || !strings.Contains(err.Error(), "int64 nanosecond range") {
		t.Fatalf("newQueryWindow error = %v, want a rounded-bound rejection", err)
	}
}

// writeManifestFile writes one ingest report as JSON.
func writeManifestFile(t *testing.T, dir string, manifest Report) string {
	t.Helper()
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("manifest-%d.json", time.Now().UnixNano()))
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}
