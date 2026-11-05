package main

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"
	"time"
)

// testQueryReport builds a small report for rendering tests.
func testQueryReport(backend string) *QueryReport {
	latency := 12.5
	p99 := 99.5
	files := int64(3)
	zeros := int64(0)
	bytesRead := int64(4096)
	observed := int64(2)
	lokiBytes := int64(8192)
	report := &QueryReport{
		Version:        queryReportVersion,
		Backend:        backend,
		StartedAt:      time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
		ElapsedSeconds: 1.25,
		Valid:          true,
		Errors:         []string{},
		Notes:          []string{"a note with a | pipe and a\nnewline"},
		Corpus: CorpusInfo{
			Hash:         "hash",
			ContentHash:  "content",
			Start:        time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC),
			StepMicros:   100,
			RawLineBytes: 4000,
			Lines:        400,
			Through:      time.Date(2026, 11, 1, 10, 0, 0, 40*int(time.Millisecond), time.UTC),
		},
		QueryEndpoint: "http://127.0.0.1:3100/loki/api/v1/query",
		Tenant:        "acme",
		Selector:      `{service_name=~"api|web"}`,
		Config: QueryRunConfig{
			Repetitions:    2,
			Warmups:        1,
			TimeoutSeconds: 60,
			Classes:        queryClassNames(),
			Windows:        queryWindowNames(),
		},
		FirstObservation: &FirstObservation{
			Class:         queryClassRare,
			Window:        queryWindowShort,
			Query:         `sum(count_over_time({service_name=~"api"} |= "x" [1s]))`,
			ExpectedCount: 2,
			LatencyMS:     3.5,
			StatusCode:    200,
			ObservedCount: &observed,
			Note:          "page-cache state was not controlled",
		},
		Series: []QuerySeries{{
			Class:                   queryClassRare,
			Literal:                 rareMarker,
			Window:                  queryWindowShort,
			From:                    time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC),
			Through:                 time.Date(2026, 11, 1, 10, 0, 0, 20*int(time.Millisecond), time.UTC),
			EvalTime:                time.Date(2026, 11, 1, 10, 0, 0, 19999000, time.UTC),
			RangeMS:                 20,
			Query:                   `sum(count_over_time({service_name=~"api|web"} |= "` + rareMarker + `" [20ms]))`,
			ExpectedCount:           2,
			ObservedCount:           &observed,
			CountsConsistent:        true,
			WarmupLatenciesMS:       []float64{4.0},
			Samples:                 []QuerySample{{Index: 0, LatencyMS: 5, StatusCode: 200, ObservedCount: &observed}},
			P50MS:                   &latency,
			P99MS:                   &p99,
			SampleCount:             2,
			Valid:                   true,
			FilesScanned:            &files,
			FilesPrunedIndex:        &files,
			FilesPrunedLabels:       nil,
			BytesScanned:            &bytesRead,
			CacheFetchedBytes:       nil,
			CacheDownloadsSuccess:   &zeros,
			CacheDownloadsError:     nil,
			MetricsConsistent:       true,
			MetricsMissing:          []string{"parkana_querier_cache_fetched_bytes_total"},
			LokiTotalBytesProcessed: &lokiBytes,
			LokiBytesConsistent:     true,
		}},
	}
	return report
}

func TestMarshalQueryReportRoundTrip(t *testing.T) {
	report := testQueryReport(backendParkana)
	encoded, err := marshalQueryReport(report)
	if err != nil {
		t.Fatalf("marshalQueryReport: %v", err)
	}
	if !bytes.HasSuffix(encoded, []byte("\n")) {
		t.Fatal("report does not end with a newline")
	}
	var decoded QueryReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	reencoded, err := marshalQueryReport(&decoded)
	if err != nil {
		t.Fatalf("re-marshal report: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip changed the report:\n%s\n%s", encoded, reencoded)
	}
	for _, field := range []string{
		`"first_observation"`, `"series"`, `"expected_count"`, `"observed_count"`,
		`"p50_ms"`, `"p99_ms"`, `"bytes_scanned"`, `"stats_total_bytes_processed"`,
		`"metrics"`, `"sample_count"`,
	} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("report is missing JSON field %s", field)
		}
	}
	if strings.Contains(string(encoded), `"cold"`) {
		t.Fatal("report uses the old cold field name")
	}
	// A null counter must stay null, not become zero.
	if !strings.Contains(string(encoded), `"files_pruned_labels": null`) {
		t.Fatalf("missing counter is not null:\n%s", encoded)
	}
}

func TestWriteMarkdownParkana(t *testing.T) {
	report := testQueryReport(backendParkana)
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, report); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	text := buf.String()
	for _, want := range []string{
		"## parkana query benchmark",
		"valid measurement",
		"first observation",
		"not a cache-state measurement",
		"### Latency and counts",
		"### Parkana scan and prune counters (per query)",
		"| rare | short |",
		"candidate bytes",
		"sidecar fetch B",
		"a note with a | pipe and a newline",
		"### Queries",
		report.Series[0].Query,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("markdown is missing %q:\n%s", want, text)
		}
	}
	// Unavailable values render as an em dash, never zero.
	if !strings.Contains(text, "—") {
		t.Fatalf("markdown has no unavailable marker:\n%s", text)
	}
	if strings.Contains(strings.ToLower(text), "cold") {
		t.Fatalf("markdown calls the first request cold:\n%s", text)
	}
}

func TestWriteMarkdownLoki(t *testing.T) {
	report := testQueryReport(backendLoki)
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, report); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	text := buf.String()
	for _, want := range []string{
		"## loki query benchmark",
		"### Loki logical bytes (per query)",
		"totalBytesProcessed",
		"8192",
		"not physical I/O",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("markdown is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "Parkana scan") {
		t.Fatalf("loki markdown rendered parkana counters:\n%s", text)
	}
}

func TestMarkdownEscaping(t *testing.T) {
	if got := mdCell(`a|b\c` + "\n" + "d"); got != `a\|b\\c d` {
		t.Fatalf("mdCell = %q", got)
	}
	if got := mdText("a|b`c\nd"); got != "a|b'c d" {
		t.Fatalf("mdText = %q", got)
	}
	// A class/window cell with a pipe must not add a column.
	report := testQueryReport(backendLoki)
	report.Series[0].Class = "a|b"
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, report); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	line := ""
	header := ""
	for candidate := range strings.SplitSeq(buf.String(), "\n") {
		if strings.HasPrefix(candidate, "| class | window | from |") {
			header = candidate
		}
		if strings.HasPrefix(candidate, "| a\\|b | short | 2026-") {
			line = candidate
		}
	}
	if header == "" || line == "" {
		t.Fatalf("escaped class row or header missing:\n%s", buf.String())
	}
	if strings.Count(line, " | ") != strings.Count(header, " | ") {
		t.Fatalf("escaped row has a different cell count than the header:\n%s\n%s", header, line)
	}
	if !strings.HasSuffix(line, "| true |") {
		t.Fatalf("escaped row does not end with the valid cell: %s", line)
	}
}

// failingWriter fails every write.
type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriteMarkdownPropagatesWriterError(t *testing.T) {
	sentinel := errors.New("writer failed")
	if err := WriteMarkdown(failingWriter{err: sentinel}, testQueryReport(backendParkana)); !errors.Is(err, sentinel) {
		t.Fatalf("WriteMarkdown error = %v, want the writer error", err)
	}
	if err := WriteMarkdown(nil, testQueryReport(backendLoki)); err == nil {
		t.Fatal("WriteMarkdown accepted a nil writer")
	}
}

func TestWriteMarkdownInvalidAndEmpty(t *testing.T) {
	report := testQueryReport(backendParkana)
	report.Valid = false
	report.Errors = []string{"frequent/long: expected 10 occurrences, backend reported 9", "a | pipe"}
	report.Series[0].P50MS = nil
	report.Series[0].P99MS = nil
	report.Series[0].SampleCount = 0
	report.FirstObservation = nil
	var buf bytes.Buffer
	if err := WriteMarkdown(&buf, report); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	text := buf.String()
	for _, want := range []string{
		"INVALID measurement",
		"### Errors",
		"expected 10 occurrences",
		"a | pipe",
		"percentiles summarize only validated samples",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("markdown is missing %q:\n%s", want, text)
		}
	}
	if err := WriteMarkdown(&bytes.Buffer{}, nil); err == nil {
		t.Fatal("WriteMarkdown accepted a nil report")
	}

	empty := &QueryReport{Version: queryReportVersion, Backend: backendLoki, Valid: true}
	buf.Reset()
	if err := WriteMarkdown(&buf, empty); err != nil {
		t.Fatalf("WriteMarkdown(empty): %v", err)
	}
	if !strings.Contains(buf.String(), "## loki query benchmark") {
		t.Fatalf("empty report did not render:\n%s", buf.String())
	}
}
