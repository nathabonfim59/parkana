// Command otlpcompare sends a deterministic OTLP fixture corpus to one OTLP
// receiver and compares the Loki read API results of the fan-out targets.
//
// It exists for the phase 4 OTLP e2e: Alloy receives OTLP once and forwards the
// same records to Loki's native OTLP receiver and Parkana's /v1/logs, so the
// two stores must agree on stream labels, timestamps, lines and structured
// metadata. The command imports no storage, query or OTLP conversion code: it
// only speaks HTTP, uses the OTLP wire model to build fixtures, and decodes the
// documented Loki JSON responses.
//
// Two subcommands:
//
//	otlpcompare send -target URL -manifest PATH
//	otlpcompare compare -loki URL -parkana URL -manifest PATH
//
// send posts one OTLP export per fixture case (protobuf and JSON alternating by
// default, optionally gzip), retries bounded, and writes the manifest. compare
// waits until each side shows the exact fixture counts from that manifest (both
// empty is a failure, never equality), then runs a fixed query set: full-window
// forward/backward entries, limit=1, label selectors, metadata filters,
// normalization collision filters, substring filters, series and exact
// first/last time boundaries. Entries are compared as complete values
// (labels, nanosecond timestamp, line, structured metadata, parsed metadata).
// Only ordering across streams is normalized.
//
// Exit codes: 0 every case equal, 1 at least one semantic difference, 2 invalid
// invocation or a readiness, count-precheck, transport, HTTP or decode failure.
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
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	exitEqual     = 0
	exitDifferent = 1
	exitFailure   = 2

	// maxReportedDiffs caps the difference lines printed by text reports.
	maxReportedDiffs = 10
)

const topUsage = `otlpcompare sends deterministic OTLP fixtures and compares two Loki APIs.

Usage:
  otlpcompare send    -target URL -manifest PATH [flags]
  otlpcompare compare -loki URL -parkana URL -manifest PATH [flags]

Run "otlpcompare send -h" or "otlpcompare compare -h" for the flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches one invocation and returns the process exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, "%s", topUsage)
		return exitFailure
	}
	switch args[0] {
	case "send":
		cfg, err, printed := parseSendFlags(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return exitEqual
		}
		if err != nil {
			if !printed {
				fmt.Fprintf(stderr, "otlpcompare send: %v\n", err)
			}
			return exitFailure
		}
		return runSend(ctx, cfg, stdout, stderr)
	case "compare":
		cfg, err, printed := parseCompareFlags(args[1:], stderr)
		if errors.Is(err, flag.ErrHelp) {
			return exitEqual
		}
		if err != nil {
			if !printed {
				fmt.Fprintf(stderr, "otlpcompare compare: %v\n", err)
			}
			return exitFailure
		}
		return runCompare(ctx, cfg, stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, topUsage)
		return exitEqual
	default:
		fmt.Fprintf(stderr, "otlpcompare: unknown subcommand %q\n%s", args[0], topUsage)
		return exitFailure
	}
}

// compareConfig is the validated invocation of the compare subcommand.
type compareConfig struct {
	loki         string
	parkana      string
	manifestPath string
	tenant       tenant.ID
	timeout      time.Duration
	waitTimeout  time.Duration
	pollInterval time.Duration
	readyTimeout time.Duration
	format       string
	reportPath   string
}

const compareUsage = `Usage:
  otlpcompare compare -loki URL -parkana URL -manifest PATH [flags]

compare waits for the fixture counts recorded in the manifest to be visible on
both servers, then runs the fixed query set against both Loki APIs.

Query windows are half-open, [start, end), and are sent unshifted. Pinned Loki
3.7.9 uses inclusive mint and exclusive maxt (pkg/iter/entry_iterator.go), which
matches Parkana's From <= ts < Through; boundary fixtures one microsecond apart
catch any endpoint shift or sub-microsecond rounding.

The fixed query set is:
  entries-backward/forward   full fixture window, direction both ways
  limit-newest/oldest        limit=1, exact expected entry timestamp
  selector-service-present   indexed service_name label
  selector-service-empty     empty service_name (native discovery replaces it)
  selector-service-unknown   unknown_service fallback for empty/missing names
  selector-service-discovered
                             container_name-based default discovery
  selector-container-label   indexed container_name label
  metadata-job               non-indexed resource attribute job is metadata
  selector-k8s-pod           indexed k8s_pod_name label
  collision-meta-*           exact collide_name=resource|scope|log filters
  collision-http-*           exact http_request_method=GET|SCOPE|LOG filters
  collision-label-*          exact service_name=label-svc|shadow-log filters
  negative-label-selector    a metadata value must not select a stream
  unusual-name-*             exact a_b=dash|space|dot filters
  fixed-*-attr/field         exact collision filters for attributes that
                             normalize onto converter fixed fields
                             (severity_text, trace_id, span_id, observed_timestamp,
                             flags, dropped_attributes_count, scope_name, scope_version)
  substring-present/absent/negated
                             |= and != with exact counts
  boundary-first/last/before exact From <= ts < Through behavior
  boundary-micro-*           adjacent 1µs points that catch endpoint shifts
  series                     label sets for the fixture selector

Each collision contender has its own exact-value filter. Their per-contender
counts are deliberately not assumed: real Loki's filter semantics decide the
winner, and a converter that picks a different contender differs on at least
one filter. The global count gate still proves the corpus exists, so a
both-empty answer can never pass.

Every entry is compared completely: full label map, exact nanosecond timestamp,
line, structuredMetadata and parsed metadata. Ordering across streams is
normalized; entries within a stream keep response order so direction is checked.
A filter query matching nothing on both sides fails when the fixture requires a
match.

Exit codes:
  0  every query equal
  1  at least one semantic difference
  2  invalid invocation, readiness/count timeout, transport, HTTP or decode
     failure

Flags:
`

func parseCompareFlags(args []string, stderr io.Writer) (compareConfig, error, bool) {
	fs := flag.NewFlagSet("otlpcompare compare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		lokiRaw    = fs.String("loki", "", "Loki read API base URL, e.g. http://127.0.0.1:3101")
		parkanaRaw = fs.String("parkana", "", "Parkana read API base URL, e.g. http://127.0.0.1:3100")
		manifest   = fs.String("manifest", "", "manifest written by otlpcompare send")
		tenantRaw  = fs.String("tenant", string(tenant.Default), "tenant id sent as X-Scope-OrgID")
		timeout    = fs.Duration("timeout", 30*time.Second, "per-query HTTP timeout")
		wait       = fs.Duration("wait-timeout", 60*time.Second, "wait this long for the fixture counts to appear on both servers")
		poll       = fs.Duration("poll-interval", time.Second, "count polling interval")
		ready      = fs.Duration("ready-timeout", 30*time.Second, "wait this long for both /ready endpoints (0 skips)")
		format     = fs.String("format", "text", "stdout report format: text or json")
		report     = fs.String("report", "", "also write a JSON report to this path")
	)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), compareUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return compareConfig{}, err, errors.Is(err, flag.ErrHelp)
	}
	if fs.NArg() > 0 {
		return compareConfig{}, fmt.Errorf("unexpected arguments %q", fs.Args()), false
	}
	if *lokiRaw == "" || *parkanaRaw == "" || *manifest == "" {
		return compareConfig{}, errors.New("-loki, -parkana and -manifest are required"), false
	}
	loki, err := normalizeBase(*lokiRaw)
	if err != nil {
		return compareConfig{}, fmt.Errorf("-loki: %w", err), false
	}
	parkana, err := normalizeBase(*parkanaRaw)
	if err != nil {
		return compareConfig{}, fmt.Errorf("-parkana: %w", err), false
	}
	id := tenant.ID(*tenantRaw)
	if err := id.Validate(); err != nil {
		return compareConfig{}, fmt.Errorf("-tenant: %w", err), false
	}
	if *timeout <= 0 {
		return compareConfig{}, fmt.Errorf("-timeout must be positive, got %v", *timeout), false
	}
	if *wait < 0 {
		return compareConfig{}, fmt.Errorf("-wait-timeout must not be negative, got %v", *wait), false
	}
	if *poll <= 0 {
		return compareConfig{}, fmt.Errorf("-poll-interval must be positive, got %v", *poll), false
	}
	if *ready < 0 {
		return compareConfig{}, fmt.Errorf("-ready-timeout must not be negative, got %v", *ready), false
	}
	if *format != "text" && *format != "json" {
		return compareConfig{}, fmt.Errorf("-format must be text or json, got %q", *format), false
	}
	return compareConfig{
		loki:         loki,
		parkana:      parkana,
		manifestPath: *manifest,
		tenant:       id,
		timeout:      *timeout,
		waitTimeout:  *wait,
		pollInterval: *poll,
		readyTimeout: *ready,
		format:       *format,
		reportPath:   *report,
	}, nil, false
}

// normalizeBase validates a read API base URL. A path prefix is kept; a query
// or fragment is rejected because requests are appended to it.
func normalizeBase(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%q: missing host", raw)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("%q: query and fragment are not allowed", raw)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u.String(), nil
}

// caseCount is one fixture case's expected and observed entry counts.
type caseCount struct {
	Name     string `json:"name"`
	Instance string `json:"instance"`
	Want     int    `json:"want"`
	Left     int    `json:"left"`
	Right    int    `json:"right"`
}

// precheckResult records the count gate that runs before any comparison.
type precheckResult struct {
	OK          bool        `json:"ok"`
	Expected    int         `json:"expectedTotal"`
	LeftTotal   int         `json:"leftTotal"`
	RightTotal  int         `json:"rightTotal"`
	Cases       []caseCount `json:"cases"`
	WaitTimeout string      `json:"waitTimeout"`
	Error       string      `json:"error,omitempty"`
}

// queryOutcome is the result of one fixed query.
type queryOutcome struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	Endpoint   string   `json:"endpoint"`
	LeftCount  int      `json:"leftItems"`
	RightCount int      `json:"rightItems"`
	Diffs      []string `json:"diffs,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// compareReport is the machine-readable artifact.
type compareReport struct {
	Tool        string         `json:"tool"`
	GeneratedAt string         `json:"generatedAt"`
	Fixture     fixtureReport  `json:"fixture"`
	Targets     targetsReport  `json:"targets"`
	Precheck    precheckResult `json:"precheck"`
	Queries     []queryOutcome `json:"queries"`
	Summary     summaryReport  `json:"summary"`
}

type fixtureReport struct {
	ID             string `json:"id"`
	Version        int    `json:"version"`
	PDataModule    string `json:"pdataModule"`
	PDataVersion   string `json:"pdataVersion"`
	Namespace      string `json:"namespace"`
	RunID          string `json:"runId"`
	Anchor         string `json:"anchor"`
	FirstTimestamp string `json:"firstTimestamp"`
	LastTimestamp  string `json:"lastTimestamp"`
	TotalRecords   int    `json:"totalRecords"`
	ManifestPath   string `json:"manifestPath"`
}

type targetsReport struct {
	Loki    string `json:"loki"`
	Parkana string `json:"parkana"`
	Tenant  string `json:"tenant"`
}

type summaryReport struct {
	Equal     int `json:"equal"`
	Different int `json:"different"`
	Failed    int `json:"failed"`
	ExitCode  int `json:"exitCode"`
}

func runCompare(ctx context.Context, cfg compareConfig, stdout, stderr io.Writer) int {
	m, times, err := readManifest(cfg.manifestPath)
	if err != nil {
		fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
		return exitFailure
	}
	queries, err := fixedQueries(m, times)
	if err != nil {
		fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
		return exitFailure
	}

	client := &http.Client{}
	report := compareReport{
		Tool:        toolName,
		GeneratedAt: formatTime(time.Now()),
		Fixture: fixtureReport{
			ID:             m.FixtureID,
			Version:        m.FixtureVersion,
			PDataModule:    m.PDataModule,
			PDataVersion:   m.PDataVersion,
			Namespace:      m.Namespace,
			RunID:          m.RunID,
			Anchor:         m.Anchor,
			FirstTimestamp: m.FirstTimestamp,
			LastTimestamp:  m.LastTimestamp,
			TotalRecords:   m.TotalRecords,
			ManifestPath:   cfg.manifestPath,
		},
		Targets: targetsReport{Loki: cfg.loki, Parkana: cfg.parkana, Tenant: string(cfg.tenant)},
	}

	finish := func(code int) int {
		report.Summary.ExitCode = code
		if err := emitReport(cfg, report, stdout); err != nil {
			fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
			return exitFailure
		}
		return code
	}

	if cfg.readyTimeout > 0 {
		for _, target := range []struct {
			name, base string
		}{{"loki", cfg.loki}, {"parkana", cfg.parkana}} {
			if err := waitForReady(ctx, client, target.base, cfg.readyTimeout, cfg.pollInterval); err != nil {
				report.Precheck = precheckResult{WaitTimeout: cfg.waitTimeout.String(), Error: err.Error()}
				report.Summary.Failed = 1
				fmt.Fprintf(stderr, "otlpcompare: %s readiness: %v\n", target.name, err)
				return finish(exitFailure)
			}
		}
	}

	precheck, precheckErr := runPrecheck(ctx, client, cfg, m, queries)
	report.Precheck = precheck
	if precheckErr != nil {
		report.Summary.Failed = 1
		fmt.Fprintf(stderr, "otlpcompare: precheck: %v\n", precheckErr)
		return finish(exitFailure)
	}

	equal, different, failed := 0, 0, 0
	for _, q := range queries {
		outcome := runQuery(ctx, client, cfg, q, stderr)
		report.Queries = append(report.Queries, outcome)
		switch {
		case outcome.Error != "":
			failed++
		case len(outcome.Diffs) > 0:
			different++
		default:
			equal++
		}
	}
	report.Summary = summaryReport{Equal: equal, Different: different, Failed: failed}
	code := exitEqual
	switch {
	case failed > 0:
		code = exitFailure
	case different > 0:
		code = exitDifferent
	}
	report.Summary.ExitCode = code
	if err := emitReport(cfg, report, stdout); err != nil {
		fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
		return exitFailure
	}
	return code
}

// waitForReady polls a server's /ready endpoint, never letting one probe run
// past the shared readiness budget.
func waitForReady(ctx context.Context, client *http.Client, base string, timeout, poll time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("not ready after %s: %w", timeout, lastErr)
		}
		reqTimeout := min(5*time.Second, remaining)
		reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+"/ready", nil)
		if err != nil {
			cancel()
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				cancel()
				return nil
			}
			lastErr = fmt.Errorf("HTTP %s", resp.Status)
		} else {
			lastErr = err
		}
		cancel()
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("not ready after %s: %w", timeout, lastErr)
		}
		if err := sleepContext(ctx, min(poll, remaining)); err != nil {
			return err
		}
	}
}

// runPrecheck polls both servers until the manifest's exact per-case and total
// counts are visible. A both-empty answer never passes this gate.
func runPrecheck(ctx context.Context, client *http.Client, cfg compareConfig, m fixtureManifest, queries []queryCase) (precheckResult, error) {
	baseQuery, ok := queryByName(queries, "entries-backward")
	if !ok {
		return precheckResult{}, errors.New("fixed query set has no entries-backward query")
	}
	result := precheckResult{
		Expected:    m.TotalRecords,
		WaitTimeout: cfg.waitTimeout.String(),
		Cases:       make([]caseCount, 0, len(m.Cases)),
	}
	for _, c := range m.Cases {
		result.Cases = append(result.Cases, caseCount{Name: c.Name, Instance: c.Instance, Want: c.Records})
	}

	deadline := time.Now().Add(cfg.waitTimeout)
	firstAttempt := true
	var lastErr error
	for {
		attemptCtx := ctx
		cancel := func() {}
		if cfg.waitTimeout > 0 {
			if !firstAttempt && time.Until(deadline) <= 0 {
				err := lastError(lastErr)
				result.Error = err.Error()
				return result, fmt.Errorf("fixture counts not visible after %s: %w", cfg.waitTimeout, err)
			}
			attemptCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		left, leftErr := fetchResult(attemptCtx, client, cfg.loki, baseQuery, cfg.tenant, cfg.timeout)
		right, rightErr := fetchResult(attemptCtx, client, cfg.parkana, baseQuery, cfg.tenant, cfg.timeout)
		cancel()
		firstAttempt = false
		switch {
		case leftErr != nil:
			lastErr = preferDefinitiveError(lastErr, fmt.Errorf("loki: %w", leftErr))
		case rightErr != nil:
			lastErr = preferDefinitiveError(lastErr, fmt.Errorf("parkana: %w", rightErr))
		default:
			leftCounts := instanceCounts(left)
			rightCounts := instanceCounts(right)
			result.LeftTotal = left.entries
			result.RightTotal = right.entries
			matched := left.entries == m.TotalRecords && right.entries == m.TotalRecords
			for i := range result.Cases {
				c := &result.Cases[i]
				c.Left = leftCounts[c.Instance]
				c.Right = rightCounts[c.Instance]
				if c.Left != c.Want || c.Right != c.Want {
					matched = false
				}
			}
			if matched {
				result.OK = true
				return result, nil
			}
			lastErr = fmt.Errorf("expected %d records (%d cases); loki has %d, parkana has %d",
				m.TotalRecords, len(m.Cases), left.entries, right.entries)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			err := lastError(lastErr)
			result.Error = err.Error()
			return result, fmt.Errorf("fixture counts not visible after %s: %w", cfg.waitTimeout, err)
		}
		if err := sleepContext(ctx, min(cfg.pollInterval, remaining)); err != nil {
			result.Error = err.Error()
			return result, err
		}
	}
}

// lastError renders the final precheck failure, including the case where no
// attempt ever observed a usable response.
func lastError(err error) error {
	if err != nil {
		return err
	}
	return errors.New("no fixture counts observed")
}

// preferDefinitiveError keeps the first concrete failure (HTTP status, decode,
// connection refusal) instead of replacing it with a context deadline that the
// precheck's own budget produced on a later attempt.
func preferDefinitiveError(prev, next error) error {
	if next == nil {
		return prev
	}
	nextTransient := errors.Is(next, context.DeadlineExceeded) || errors.Is(next, context.Canceled)
	if prev == nil {
		return next
	}
	prevTransient := errors.Is(prev, context.DeadlineExceeded) || errors.Is(prev, context.Canceled)
	if nextTransient && !prevTransient {
		return prev
	}
	if !nextTransient {
		return next
	}
	return prev
}

// instanceCounts groups a full-window result by its service_instance_id label.
func instanceCounts(result canonicalResult) map[string]int {
	counts := make(map[string]int)
	for _, s := range result.streams {
		counts[s.labels["service_instance_id"]] += len(s.entries)
	}
	return counts
}

func queryByName(queries []queryCase, name string) (queryCase, bool) {
	for _, q := range queries {
		if q.name == name {
			return q, true
		}
	}
	return queryCase{}, false
}

// runQuery fetches one query from both servers and compares the results.
func runQuery(ctx context.Context, client *http.Client, cfg compareConfig, q queryCase, stderr io.Writer) queryOutcome {
	outcome := queryOutcome{Name: q.name, Kind: string(q.kind), Endpoint: q.endpoint()}
	left, err := fetchResult(ctx, client, cfg.loki, q, cfg.tenant, cfg.timeout)
	if err != nil {
		outcome.Error = fmt.Sprintf("loki: %v", err)
		fmt.Fprintf(stderr, "FAIL %s: %s\n", q.name, outcome.Error)
		return outcome
	}
	right, err := fetchResult(ctx, client, cfg.parkana, q, cfg.tenant, cfg.timeout)
	if err != nil {
		outcome.Error = fmt.Sprintf("parkana: %v", err)
		fmt.Fprintf(stderr, "FAIL %s: %s\n", q.name, outcome.Error)
		return outcome
	}
	outcome.LeftCount = left.itemCount()
	outcome.RightCount = right.itemCount()
	if q.direction != "" {
		for _, stream := range left.streams {
			if msg := directionViolation(stream, q.direction); msg != "" {
				outcome.Diffs = append(outcome.Diffs, "left "+msg)
			}
		}
		for _, stream := range right.streams {
			if msg := directionViolation(stream, q.direction); msg != "" {
				outcome.Diffs = append(outcome.Diffs, "right "+msg)
			}
		}
	}
	outcome.Diffs = append(outcome.Diffs, checkQueryExpectation(q, left, right)...)
	outcome.Diffs = append(outcome.Diffs, diffResults(left, right)...)
	return outcome
}

// emitReport writes the text or JSON report to stdout and, when configured,
// the JSON artifact to -report.
func emitReport(cfg compareConfig, report compareReport, stdout io.Writer) error {
	if cfg.format == "json" {
		if err := writeJSON(stdout, report); err != nil {
			return fmt.Errorf("write JSON report: %w", err)
		}
	} else {
		writeTextReport(stdout, report)
	}
	if cfg.reportPath != "" {
		var buf bytes.Buffer
		if err := writeJSON(&buf, report); err != nil {
			return fmt.Errorf("encode report: %w", err)
		}
		if err := os.WriteFile(cfg.reportPath, buf.Bytes(), 0o644); err != nil {
			return fmt.Errorf("write report %s: %w", cfg.reportPath, err)
		}
	}
	return nil
}

func writeJSON(w io.Writer, value any) error {
	enc := jsontext.NewEncoder(w, jsontext.WithIndent("  "))
	return json.MarshalEncode(enc, value)
}

// writeTextReport prints a bounded human-readable summary.
func writeTextReport(w io.Writer, report compareReport) {
	fmt.Fprintf(w, "fixture %s v%d (%s@%s) namespace=%s run=%s\n",
		report.Fixture.ID, report.Fixture.Version, report.Fixture.PDataModule,
		report.Fixture.PDataVersion, report.Fixture.Namespace, report.Fixture.RunID)
	fmt.Fprintf(w, "manifest %s anchor=%s first=%s last=%s\n",
		report.Fixture.ManifestPath, report.Fixture.Anchor, report.Fixture.FirstTimestamp,
		report.Fixture.LastTimestamp)
	fmt.Fprintf(w, "loki=%s parkana=%s tenant=%s\n",
		report.Targets.Loki, report.Targets.Parkana, report.Targets.Tenant)
	if report.Precheck.Error != "" {
		fmt.Fprintf(w, "precheck FAILED: %s\n", report.Precheck.Error)
	} else {
		fmt.Fprintf(w, "precheck ok: %d records in %d cases\n",
			report.Precheck.Expected, len(report.Precheck.Cases))
	}
	for _, c := range report.Precheck.Cases {
		status := "ok  "
		if c.Left != c.Want || c.Right != c.Want {
			status = "FAIL"
		}
		fmt.Fprintf(w, "  %s %s left=%d right=%d want=%d\n", status, c.Name, c.Left, c.Right, c.Want)
	}
	for _, q := range report.Queries {
		switch {
		case q.Error != "":
			fmt.Fprintf(w, "FAIL %s: %s\n", q.Name, q.Error)
		case len(q.Diffs) > 0:
			fmt.Fprintf(w, "DIFF %s (%s) left=%d right=%d\n", q.Name, q.Endpoint, q.LeftCount, q.RightCount)
			for i, diff := range q.Diffs {
				if i >= maxReportedDiffs {
					fmt.Fprintf(w, "  ... and %d more difference(s)\n", len(q.Diffs)-maxReportedDiffs)
					break
				}
				fmt.Fprintf(w, "  %s\n", diff)
			}
		default:
			unit := "entries"
			if q.Kind == string(querySeries) {
				unit = "series"
			}
			fmt.Fprintf(w, "ok   %s (%d %s)\n", q.Name, q.LeftCount, unit)
		}
	}
	fmt.Fprintf(w, "summary: %d equal, %d different, %d failed (exit %d)\n",
		report.Summary.Equal, report.Summary.Different, report.Summary.Failed, report.Summary.ExitCode)
}
