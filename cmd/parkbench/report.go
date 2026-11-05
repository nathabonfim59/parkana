// Report rendering for the query benchmark: the JSON artifact and the Markdown
// summary.
//
// The JSON report is the source artifact; Markdown is a human view of the same
// values. Every unavailable value renders as an em dash, never as zero; a value
// that differed between identical queries renders as "varies" and its summary
// field is null in the JSON. The byte definitions are printed below the tables
// so no reader can mistake a logical byte count for physical I/O.
package main

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// marshalQueryReport renders the report with a stable, indented JSON shape.
func marshalQueryReport(report *QueryReport) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.WithIndent("  "))
	if err := json.MarshalEncode(enc, report); err != nil {
		return nil, fmt.Errorf("parkbench query: encode report: %w", err)
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// WriteMarkdown writes one query report as Markdown and returns the first
// writer error. It is the --markdown artifact, not a reporting framework: the
// JSON remains the source of truth.
func WriteMarkdown(w io.Writer, report *QueryReport) error {
	if report == nil {
		return fmt.Errorf("parkbench query: no report to render")
	}
	if w == nil {
		return fmt.Errorf("parkbench query: no writer to render into")
	}
	recorder := &errorWriter{writer: w}
	writeMarkdown(recorder, report)
	return recorder.err
}

// errorWriter records the first write error so a renderer that ignores
// individual fmt results still fails the operation.
type errorWriter struct {
	writer io.Writer
	err    error
}

func (w *errorWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.writer.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

// writeMarkdown renders the report; write errors are surfaced by WriteMarkdown.
func writeMarkdown(w io.Writer, report *QueryReport) {
	fmt.Fprintf(w, "## %s query benchmark\n\n", mdCell(report.Backend))
	fmt.Fprintf(w, "- status: **%s**\n", validLabel(report.Valid))
	fmt.Fprintf(w, "- endpoint: `%s`\n", mdText(report.QueryEndpoint))
	if report.Tenant != "" {
		fmt.Fprintf(w, "- tenant: `%s`\n", mdText(report.Tenant))
	} else {
		fmt.Fprintln(w, "- tenant: (none; backend default)")
	}
	fmt.Fprintf(w, "- selector: `%s`\n", mdText(report.Selector))
	fmt.Fprintf(w, "- corpus hash: `%s` (content `%s`)\n", mdText(report.Corpus.Hash), mdText(report.Corpus.ContentHash))
	fmt.Fprintf(w, "- corpus interval: %s .. %s, step %dµs, %d lines, %d raw bytes\n",
		mdTime(report.Corpus.Start), mdTime(report.Corpus.Through), report.Corpus.StepMicros,
		report.Corpus.Lines, report.Corpus.RawLineBytes)
	fmt.Fprintf(w, "- repetitions: %d, warmups: %d, timeout: %s, elapsed: %.2fs\n",
		report.Config.Repetitions, report.Config.Warmups, mdDuration(time.Duration(report.Config.TimeoutSeconds*float64(time.Second))),
		report.ElapsedSeconds)
	if report.FirstObservation != nil {
		first := report.FirstObservation
		fmt.Fprintf(w, "- first observation (page-cache state not controlled, not a cache-state measurement): %s/%s single request latency %.2fms, observed %s, expected %d\n",
			mdCell(first.Class), mdCell(first.Window), first.LatencyMS, mdInt64(first.ObservedCount), first.ExpectedCount)
		if first.Error != "" {
			fmt.Fprintf(w, "  - first observation error: %s\n", mdText(first.Error))
		}
	}
	fmt.Fprintln(w)

	writeLatencyTable(w, report)
	if report.Backend == backendParkana {
		writeParkanaTables(w, report)
	} else {
		writeLokiBytesTable(w, report)
	}
	writeErrors(w, report)
	writeNotes(w, report)
	writeQueries(w, report)
}

// writeLatencyTable renders per-class counts and nearest-rank percentiles.
func writeLatencyTable(w io.Writer, report *QueryReport) {
	fmt.Fprintln(w, "### Latency and counts")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| class | window | from | through | range | expected | observed | p50 ms | p99 ms | samples | failures | timeouts | valid |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, series := range report.Series {
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %d | %s | %s | %s | %d | %d | %d | %t |\n",
			mdCell(series.Class), mdCell(series.Window), mdTime(series.From), mdTime(series.Through),
			mdCell(formatLogQLRange(time.Duration(series.RangeMS)*time.Millisecond)),
			series.ExpectedCount, mdInt64(series.ObservedCount), mdFloat(series.P50MS), mdFloat(series.P99MS),
			series.SampleCount, series.Failures, series.Timeouts, series.Valid)
	}
	if !report.Valid {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Any failure, count mismatch or invalid counter marks the series and the whole run invalid; percentiles summarize only validated samples.")
	}
	fmt.Fprintln(w)
}

// writeParkanaTables renders the scan/prune counters and the cache diagnostics.
func writeParkanaTables(w io.Writer, report *QueryReport) {
	fmt.Fprintln(w, "### Parkana scan and prune counters (per query)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| class | window | scanned | pruned (index) | pruned (labels) | candidate bytes | cache fetch B | cache downloads (ok/err) | sidecar fetch B |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|")
	for i := range report.Series {
		series := &report.Series[i]
		downloads := metricCell(series, series.CacheDownloadsSuccess, pickCacheDownloadsSuccess) + "/" + metricCell(series, series.CacheDownloadsError, pickCacheDownloadsError)
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			mdCell(series.Class), mdCell(series.Window), metricCell(series, series.FilesScanned, pickFilesScanned),
			metricCell(series, series.FilesPrunedIndex, pickFilesPrunedIndex), metricCell(series, series.FilesPrunedLabels, pickFilesPrunedLabels),
			metricCell(series, series.BytesScanned, pickBytesScanned), metricCell(series, series.CacheFetchedBytes, pickCacheFetchedBytes),
			downloads, metricCell(series, series.SidecarCacheFetchedBytes, pickSidecarFetchedBytes))
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Definitions: `candidate bytes` = sum of catalog compressed candidate file sizes submitted to each scan attempt, including retries; it is not physical I/O. `cache fetch B` = serialized object bytes copied into the remote object cache (partial copies included, FS direct hits excluded). `sidecar fetch B` = serialized sidecar bytes read into the decoded in-memory sidecar cache, including FS reads and partial failures. A dash means the counter was not reported; `varies` means identical queries reported different values, so no per-query summary exists and the per-sample values in the JSON are authoritative. Neither is network or disk traffic. `scanned` and both pruned columns are required: a missing series invalidates the measurement.")
	fmt.Fprintln(w)
}

// writeLokiBytesTable renders Loki's logical processed bytes.
func writeLokiBytesTable(w io.Writer, report *QueryReport) {
	fmt.Fprintln(w, "### Loki logical bytes (per query)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| class | window | totalBytesProcessed |")
	fmt.Fprintln(w, "|---|---|---|")
	for i := range report.Series {
		series := &report.Series[i]
		fmt.Fprintf(w, "| %s | %s | %s |\n", mdCell(series.Class), mdCell(series.Window), statsCell(series))
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Definition: `totalBytesProcessed` is Loki's logical byte count from `data.stats.summary.totalBytesProcessed` (it includes structured-metadata bytes). It is not physical I/O and cannot be compared with compressed candidate file bytes or network fetch bytes. A dash means Loki did not report it, which invalidates the byte-bearing measurement; `varies` means repetitions disagreed and the per-sample values in the JSON are authoritative.")
	fmt.Fprintln(w)
}

// writeErrors lists the run's errors.
func writeErrors(w io.Writer, report *QueryReport) {
	if len(report.Errors) == 0 {
		return
	}
	fmt.Fprintln(w, "### Errors")
	fmt.Fprintln(w)
	for _, message := range report.Errors {
		fmt.Fprintf(w, "- %s\n", mdText(message))
	}
	fmt.Fprintln(w)
}

// writeNotes lists the interpretation notes, including byte definitions.
func writeNotes(w io.Writer, report *QueryReport) {
	if len(report.Notes) == 0 {
		return
	}
	fmt.Fprintln(w, "### Notes")
	fmt.Fprintln(w)
	for _, note := range report.Notes {
		fmt.Fprintf(w, "- %s\n", mdText(note))
	}
	fmt.Fprintln(w)
}

// writeQueries reproduces the exact query, instant time and range of every
// series.
func writeQueries(w io.Writer, report *QueryReport) {
	if len(report.Series) == 0 {
		return
	}
	fmt.Fprintln(w, "### Queries")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Each line is `class/window eval=<instant> range=<range>: <query>`; the instant time is Through-1µs, so the backend window (t-range, t] is exactly the reported half-open interval.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "```")
	for _, series := range report.Series {
		fmt.Fprintf(w, "%s/%s eval=%s range=%s: %s\n", series.Class, series.Window, mdTime(series.EvalTime),
			formatLogQLRange(time.Duration(series.RangeMS)*time.Millisecond), series.Query)
	}
	fmt.Fprintln(w, "```")
}

// Counter accessors used by the Markdown tables.
func pickFilesScanned(m *SampleMetrics) *int64          { return m.FilesScanned }
func pickFilesPrunedIndex(m *SampleMetrics) *int64      { return m.FilesPrunedIndex }
func pickFilesPrunedLabels(m *SampleMetrics) *int64     { return m.FilesPrunedLabels }
func pickBytesScanned(m *SampleMetrics) *int64          { return m.BytesScanned }
func pickCacheFetchedBytes(m *SampleMetrics) *int64     { return m.CacheFetchedBytes }
func pickCacheDownloadsSuccess(m *SampleMetrics) *int64 { return m.CacheDownloadsSuccess }
func pickCacheDownloadsError(m *SampleMetrics) *int64   { return m.CacheDownloadsError }
func pickSidecarFetchedBytes(m *SampleMetrics) *int64   { return m.SidecarCacheFetchedBytes }

// metricCell renders one per-query counter summary. A nil summary with
// differing sample values is "varies", not a missing counter.
func metricCell(series *QuerySeries, summary *int64, pick func(*SampleMetrics) *int64) string {
	if summary != nil {
		return strconv.FormatInt(*summary, 10)
	}
	if _, _, varies := summarizeField(series.Samples, pick); varies {
		return "varies"
	}
	return "—"
}

// statsCell renders the Loki byte statistic summary.
func statsCell(series *QuerySeries) string {
	if series.LokiTotalBytesProcessed != nil {
		return strconv.FormatInt(*series.LokiTotalBytesProcessed, 10)
	}
	if _, _, varies := summarizeStats(series.Samples); varies {
		return "varies"
	}
	return "—"
}

// validLabel renders the run status.
func validLabel(valid bool) string {
	if valid {
		return "valid measurement"
	}
	return "INVALID measurement"
}

// mdInt64 renders a nullable integer, using an em dash when unavailable.
func mdInt64(value *int64) string {
	if value == nil {
		return "—"
	}
	return strconv.FormatInt(*value, 10)
}

// mdFloat renders a nullable latency with two decimals.
func mdFloat(value *float64) string {
	if value == nil {
		return "—"
	}
	return strconv.FormatFloat(*value, 'f', 2, 64)
}

// mdTime renders a timestamp with fixed microsecond precision, matching the
// instant time parameter format exactly.
func mdTime(t time.Time) string { return t.UTC().Format(apiTimeLayout) }

// mdDuration renders a configuration duration.
func mdDuration(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.String()
}

// mdText escapes text for a Markdown bullet or code span: table separators do
// not need escaping there, only line breaks and backticks must not leak.
func mdText(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ", "`", "'").Replace(value)
}

// mdCell escapes one Markdown table cell.
func mdCell(value string) string {
	return strings.NewReplacer("\\", "\\\\", "|", "\\|", "\r", " ", "\n", " ").Replace(value)
}
