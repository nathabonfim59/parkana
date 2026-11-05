package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Corpus presets. The raw-byte targets are decimal (2e9 and 25e9 bytes), not
// GiB, and the generator always achieves them exactly.
const (
	presetSmall = "small"
	presetFull  = "full"

	smallTargetBytes int64 = 2_000_000_000
	fullTargetBytes  int64 = 25_000_000_000

	defaultSeed uint64 = 0x7062_656e_6368_0001 // "pbench" + 1
)

// command is one parkbench subcommand. B2b extends this list with query and
// report without touching the ingest implementation.
type command struct {
	Name    string
	Summary string
	Run     func(ctx context.Context, args []string, stdout, stderr io.Writer) error
}

// commands returns the registered subcommands in help order.
func commands() []command {
	return []command{
		{
			Name:    "ingest",
			Summary: "generate a deterministic corpus and push it to one Loki push URL",
			Run:     runIngestCommand,
		},
		{
			Name:    "query",
			Summary: "measure one backend with the queries defined by an ingest manifest",
			Run:     runQueryCommand,
		},
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "parkbench:", err)
		os.Exit(1)
	}
}

// run dispatches one parkbench invocation.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(stdout)
		return nil
	}
	name := args[0]
	for _, cmd := range commands() {
		if cmd.Name == name {
			return cmd.Run(ctx, args[1:], stdout, stderr)
		}
	}
	return fmt.Errorf("parkbench: unknown subcommand %q (try parkbench help)", name)
}

// printUsage writes the top-level help.
func printUsage(w io.Writer) {
	fmt.Fprintln(w, `parkbench: reproducible Parkana/Loki ingest and query benchmarks

Usage:
  parkbench <command> [flags]

Commands:`)
	for _, cmd := range commands() {
		fmt.Fprintf(w, "  %-8s %s\n", cmd.Name, cmd.Summary)
	}
	fmt.Fprintln(w, `
Run "parkbench <command> -h" for the flags of one command.
The ingest report is JSON: keep it as the corpus manifest for the query run.
parkbench query writes one JSON artifact per backend; -markdown renders the
same values as a table.`)
}

// runIngestCommand parses the ingest flags, runs the corpus and writes the
// report. The report is written even when delivery fails, so an invalid run
// still leaves evidence.
func runIngestCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("parkbench ingest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	urlFlag := fs.String("url", "", "Loki-compatible push URL, e.g. http://localhost:3100/loki/api/v1/push")
	tenantFlag := fs.String("tenant", "", "tenant id sent as X-Scope-OrgID (empty: no header, tenant 'default')")
	seed := fs.Uint64("seed", defaultSeed, "corpus seed")
	start := fs.String("start", "", "timestamp of record 0, RFC3339/RFC3339Nano microsecond-aligned (default: derived from --bytes/--step so the corpus ends before now; pass the manifest start to both backends)")
	step := fs.Duration("step", 100*time.Microsecond, "timestamp step between consecutive records (microsecond multiple)")
	bytesFlag := fs.Int64("bytes", 0, "exact raw line bytes to generate (decimal)")
	preset := fs.String("preset", "", "corpus size preset: small ("+presetSmall+" = 2 GB) or full ("+presetFull+" = 25 GB)")
	batchLines := fs.Int("batch-lines", 1250, "maximum entries per HTTP push for one full label set (final request may be smaller)")
	concurrency := fs.Int("concurrency", 16, "maximum in-flight HTTP requests")
	instances := fs.Int("instances-per-service", defaultInstancesPerService, "deterministic instance label values per stream (one sequential sender each)")
	timeout := fs.Duration("timeout", 30*time.Second, "per-attempt HTTP deadline (0 disables)")
	retries := fs.Int("retries", 3, "diagnostic retries after transport/timeout/5xx/429 failures")
	retryBackoff := fs.Duration("retry-backoff", 100*time.Millisecond, "first retry delay; doubles per retry")
	streamsFlag := fs.String("streams", strings.Join(defaultStreamNames(), ","), "comma-separated stream names")
	streamLabel := fs.String("stream-label", defaultStreamLabel, "label that carries the stream name")
	output := fs.String("output", "", "write the JSON report to this file instead of stdout")
	var labelFlags labelList
	fs.Var(&labelFlags, "label", "extra stream label name=value (repeatable)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("parkbench ingest: unexpected arguments %q", fs.Args())
	}
	if *instances <= 0 {
		return fmt.Errorf("parkbench ingest: -instances-per-service must be positive, got %d", *instances)
	}
	target, err := resolveTargetBytes(*bytesFlag, *preset)
	if err != nil {
		return err
	}
	startTime, err := parseStart(*start, time.Now().UTC(), target, *step)
	if err != nil {
		return err
	}
	var streams []string
	for s := range strings.SplitSeq(*streamsFlag, ",") {
		streams = append(streams, strings.TrimSpace(s))
	}
	labels, err := labelFlags.labels()
	if err != nil {
		return err
	}
	tid := tenant.ID(*tenantFlag)
	if tid != "" {
		if err := tid.Validate(); err != nil {
			return fmt.Errorf("parkbench ingest: -tenant: %w", err)
		}
	}

	cfg := IngestConfig{
		URL:                 *urlFlag,
		Tenant:              tid,
		Seed:                *seed,
		Start:               startTime,
		Step:                *step,
		TargetBytes:         target,
		Streams:             streams,
		StreamLabel:         *streamLabel,
		Labels:              labels,
		InstancesPerService: *instances,
		BatchLines:          *batchLines,
		Concurrency:         *concurrency,
		Timeout:             *timeout,
		Retries:             *retries,
		RetryBackoff:        *retryBackoff,
	}
	report, runErr := RunIngest(ctx, cfg)
	if report == nil {
		return runErr
	}
	encoded, err := marshalReport(report)
	if err != nil {
		return errors.Join(runErr, err)
	}
	if *output != "" {
		if err := os.WriteFile(*output, encoded, 0o644); err != nil {
			return errors.Join(runErr, fmt.Errorf("parkbench ingest: write %s: %w", *output, err))
		}
		fmt.Fprintf(stdout, "parkbench: %d lines, %d raw bytes acknowledged in %.2fs (%.2f MB/s, %.0f lines/s); report: %s\n",
			report.AckLines, report.AckRawLineBytes, report.ElapsedSeconds, report.AckMBps, report.AckLinesPerSec, *output)
		return runErr
	}
	if _, err := stdout.Write(encoded); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

// resolveTargetBytes combines -bytes and -preset into one exact byte target.
func resolveTargetBytes(bytesFlag int64, preset string) (int64, error) {
	var presetBytes int64
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "":
	case presetSmall:
		presetBytes = smallTargetBytes
	case presetFull:
		presetBytes = fullTargetBytes
	default:
		return 0, fmt.Errorf("parkbench ingest: -preset must be %q or %q, got %q", presetSmall, presetFull, preset)
	}
	switch {
	case bytesFlag < 0:
		return 0, fmt.Errorf("parkbench ingest: -bytes must not be negative, got %d", bytesFlag)
	case bytesFlag > 0 && presetBytes > 0 && bytesFlag != presetBytes:
		return 0, fmt.Errorf("parkbench ingest: -bytes %d conflicts with -preset %s (%d)", bytesFlag, preset, presetBytes)
	case bytesFlag > 0:
		return bytesFlag, nil
	case presetBytes > 0:
		return presetBytes, nil
	default:
		return 0, fmt.Errorf("parkbench ingest: set -bytes or -preset (%s|%s)", presetSmall, presetFull)
	}
}

// parseStart parses the -start flag. An empty value derives a safe past start
// so the whole corpus ends before now; both backends must use the explicit
// start recorded in the manifest for comparisons.
func parseStart(s string, now time.Time, target int64, step time.Duration) (time.Time, error) {
	if s == "" {
		return defaultStart(now, target, step), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parkbench ingest: -start: %w", err)
	}
	return t.UTC(), nil
}

// defaultStart returns a start timestamp that keeps the whole corpus in the
// past even under the conservative minimum line length. The actual corpus is
// shorter, so it ends earlier than now; that is intentional: a default must
// never push future data (Loki rejects it) and the manifest records the exact
// interval either way.
func defaultStart(now time.Time, target int64, step time.Duration) time.Time {
	const margin = time.Hour
	if step <= 0 || target <= 0 {
		return now.Add(-margin).UTC().Truncate(time.Microsecond)
	}
	maxRecords := target/minLineBytes + 2
	if maxRecords > math.MaxInt64/int64(step) {
		// NewCorpus rejects this combination as a timestamp overflow; return
		// a harmless past timestamp so the error comes from one place.
		return now.Add(-margin).UTC().Truncate(time.Microsecond)
	}
	span := time.Duration(maxRecords) * step
	if span > math.MaxInt64-margin {
		return now.Add(-margin).UTC().Truncate(time.Microsecond)
	}
	return now.Add(-(span + margin)).UTC().Truncate(time.Microsecond)
}

// marshalReport renders the report with a stable, indented JSON shape.
func marshalReport(report *Report) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.WithIndent("  "))
	if err := json.MarshalEncode(enc, report); err != nil {
		return nil, fmt.Errorf("parkbench ingest: encode report: %w", err)
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// labelList collects repeated -label name=value flags.
type labelList []logs.Label

// String renders the flags for flag help.
func (l labelList) String() string {
	parts := make([]string, len(l))
	for i, pair := range l {
		parts[i] = pair.Name + "=" + pair.Value
	}
	return strings.Join(parts, ",")
}

// Set appends one name=value pair.
func (l *labelList) Set(s string) error {
	name, value, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("-label %q is not name=value", s)
	}
	*l = append(*l, logs.Label{Name: strings.TrimSpace(name), Value: strings.TrimSpace(value)})
	return nil
}

// labels builds a validated sorted label set.
func (l labelList) labels() (logs.Labels, error) {
	out := logs.New([]logs.Label(l)...)
	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("parkbench ingest: -label: %w", err)
	}
	return out, nil
}
