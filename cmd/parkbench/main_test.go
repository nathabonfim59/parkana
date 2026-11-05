package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
)

func TestRunUsage(t *testing.T) {
	var out, stderr bytes.Buffer
	if err := run(t.Context(), nil, &out, &stderr); err != nil {
		t.Fatalf("run with no args: %v", err)
	}
	if !strings.Contains(out.String(), "ingest") {
		t.Fatalf("usage does not list ingest: %q", out.String())
	}
	names := make([]string, 0, 2)
	for _, cmd := range commands() {
		names = append(names, cmd.Name)
	}
	if !slices.Contains(names, "ingest") {
		t.Fatalf("command registry = %v, want ingest", names)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	err := run(t.Context(), []string{"bogus"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unknown subcommand") {
		t.Fatalf("run(bogus) error = %v", err)
	}
}

func TestResolveTargetBytes(t *testing.T) {
	tests := []struct {
		name    string
		bytes   int64
		preset  string
		want    int64
		wantErr bool
	}{
		{"small preset", 0, "small", smallTargetBytes, false},
		{"full preset", 0, "full", fullTargetBytes, false},
		{"explicit bytes", 12345, "", 12345, false},
		{"matching preset and bytes", smallTargetBytes, "small", smallTargetBytes, false},
		{"conflicting", smallTargetBytes, "full", 0, true},
		{"unknown preset", 0, "huge", 0, true},
		{"nothing", 0, "", 0, true},
		{"negative", -1, "", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTargetBytes(tc.bytes, tc.preset)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveTargetBytes(%d, %q) = %d, want error", tc.bytes, tc.preset, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTargetBytes: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveTargetBytes = %d, want %d", got, tc.want)
			}
		})
	}
	if smallTargetBytes != 2_000_000_000 || fullTargetBytes != 25_000_000_000 {
		t.Fatalf("presets are not decimal 2 GB / 25 GB: %d, %d", smallTargetBytes, fullTargetBytes)
	}
}

func TestParseStart(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	got, err := parseStart("2026-11-04T12:00:00.123456Z", now, 4096, time.Microsecond)
	if err != nil {
		t.Fatalf("parseStart: %v", err)
	}
	want := time.Date(2026, 11, 4, 12, 0, 0, 123456000, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("parseStart = %v, want %v", got, want)
	}
	if _, err := parseStart("not a time", now, 4096, time.Microsecond); err == nil {
		t.Fatal("parseStart accepted an invalid timestamp")
	}
	derived, err := parseStart("", now, 4096, time.Microsecond)
	if err != nil {
		t.Fatalf("parseStart(empty): %v", err)
	}
	if derived.Nanosecond()%1000 != 0 {
		t.Fatalf("default start is not microsecond-aligned: %v", derived)
	}
	if !derived.Before(now) {
		t.Fatalf("default start is not in the past: %v", derived)
	}
}

func TestDefaultStartFullSizePastBounds(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		target int64
	}{
		{"small", smallTargetBytes},
		{"full", fullTargetBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			step := 100 * time.Microsecond
			start := defaultStart(now, tc.target, step)
			if start.Nanosecond()%1000 != 0 {
				t.Fatalf("start not microsecond-aligned: %v", start)
			}
			maxRecords := tc.target/minLineBytes + 2
			end := start.Add(time.Duration(maxRecords) * step)
			if !end.Before(now) && !end.Equal(now) {
				t.Fatalf("conservative end %v is not before now %v", end, now)
			}
			if now.Sub(start) < time.Hour {
				t.Fatalf("start %v is less than the safety margin in the past", start)
			}
			if now.Sub(start) > 7*24*time.Hour {
				t.Fatalf("start %v is unreasonably far in the past", start)
			}
		})
	}
}

func TestRetryDelaySaturates(t *testing.T) {
	tests := []struct {
		base    time.Duration
		attempt int
		want    time.Duration
	}{
		{0, 0, 0},
		{-1, 3, 0},
		{time.Millisecond, 0, time.Millisecond},
		{time.Millisecond, 3, 8 * time.Millisecond},
		{time.Millisecond, 60, 64 * time.Millisecond},
		{time.Second, 10, 5 * time.Second},
		{time.Duration(math.MaxInt64), 1, 5 * time.Second},
		{time.Hour, 2, 5 * time.Second},
	}
	for _, tc := range tests {
		if got := retryDelay(tc.base, tc.attempt); got != tc.want {
			t.Fatalf("retryDelay(%v, %d) = %v, want %v", tc.base, tc.attempt, got, tc.want)
		}
	}
}

func TestMarshalReportRoundTrip(t *testing.T) {
	report := &Report{
		Corpus: CorpusInfo{
			Version:      corpusVersion,
			Hash:         "config-hash",
			ContentHash:  "content-hash",
			Seed:         1,
			Start:        testStart(),
			StepMicros:   100,
			RawLineBytes: 4096,
			Lines:        12,
			Markers:      []MarkerInfo{{Name: "rare", Literal: rareMarker, Ranges: []MarkerRange{}}},
		},
		ElapsedSeconds:  1.5,
		AckRawLineBytes: 4096,
		AckLines:        12,
		AckMBps:         0.0027,
		AckLinesPerSec:  8,
		Delivery:        DeliveryStats{Clean: true},
		Parallelism: ParallelismStats{
			LabelSets:          16,
			ActiveSenders:      12,
			ConcurrencyLimit:   16,
			PeakInFlightPushes: 10,
		},
		Batching: BatchingStats{
			EntriesPerRequest:     1250,
			PushRequests:          25,
			AttemptedEntries:      30001,
			MinEntriesPerRequest:  1,
			MaxEntriesPerRequest:  1250,
			MeanEntriesPerRequest: 30001.0 / 25,
		},
	}
	encoded, err := marshalReport(report)
	if err != nil {
		t.Fatalf("marshalReport: %v", err)
	}
	if !bytes.HasSuffix(encoded, []byte("\n")) {
		t.Fatal("report does not end with a newline")
	}
	var got Report
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if got.AckRawLineBytes != report.AckRawLineBytes || got.Corpus.Hash != report.Corpus.Hash {
		t.Fatalf("round trip changed the report: %+v", got)
	}
	if got.Parallelism != report.Parallelism {
		t.Fatalf("round trip changed parallelism: %+v vs %+v", got.Parallelism, report.Parallelism)
	}
	if got.Batching != report.Batching {
		t.Fatalf("round trip changed batching: %+v vs %+v", got.Batching, report.Batching)
	}
	for _, field := range []string{`"ack_mb_s"`, `"schedule"`, `"every_records"`, `"index_eligible_grams"`, `"instances_per_service"`, `"parallelism"`, `"peak_in_flight_pushes"`, `"active_senders"`, `"batching"`, `"entries_per_request"`, `"mean_entries_per_request"`} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("report is missing JSON field %s: %s", field, encoded)
		}
	}
}

// quietPushServer accepts every push and discards the body.
func quietPushServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestIngestCommandWritesReportFile(t *testing.T) {
	srv := quietPushServer(t)
	path := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"ingest",
		"--url", srv.URL,
		"--bytes", "50000",
		"--batch-lines", "100",
		"--concurrency", "2",
		"--retries", "0",
		"--retry-backoff", "0",
		"--output", path,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("ingest command: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), path) {
		t.Fatalf("summary does not mention the report path: %q", stdout.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.AckRawLineBytes != 50000 || report.Corpus.RawLineBytes != 50000 {
		t.Fatalf("report bytes = %d / %d, want 50000", report.AckRawLineBytes, report.Corpus.RawLineBytes)
	}
	if !report.Delivery.Clean {
		t.Fatalf("delivery not clean: %+v", report.Delivery)
	}
	if report.Parallelism.LabelSets != 16 || report.Parallelism.ConcurrencyLimit != 2 {
		t.Fatalf("report parallelism = %+v, want 16 label sets at concurrency 2", report.Parallelism)
	}
	if report.Batching.EntriesPerRequest != 100 {
		t.Fatalf("entries_per_request = %d, want the --batch-lines value 100", report.Batching.EntriesPerRequest)
	}
	if report.Corpus.InstancesPerService != defaultInstancesPerService {
		t.Fatalf("manifest instances = %d, want %d", report.Corpus.InstancesPerService, defaultInstancesPerService)
	}
}

func TestIngestCommandWritesReportToStdout(t *testing.T) {
	srv := quietPushServer(t)
	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"ingest",
		"--url", srv.URL,
		"--preset", "small",
		"--bytes", "60000", // conflicting on purpose below
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("conflicting --preset and --bytes was accepted")
	}

	stdout.Reset()
	stderr.Reset()
	err = run(t.Context(), []string{
		"ingest",
		"--url", srv.URL,
		"--bytes", "60000",
		"--batch-lines", "64",
		"--concurrency", "2",
		"--retries", "0",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("ingest command: %v\nstderr: %s", err, stderr.String())
	}
	var report Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("stdout is not a report: %v\n%s", err, stdout.String())
	}
	if report.Corpus.Tenant != "" {
		t.Fatalf("tenant without --tenant = %q", report.Corpus.Tenant)
	}
}

// TestIngestCommandDefaultParallelism runs the CLI with no parallelism flags:
// the defaults must be 16 label sets and 16 in-flight requests, proven by a
// server barrier that only releases once 16 requests overlap.
func TestIngestCommandDefaultParallelism(t *testing.T) {
	gate := newConcurrencyGate(16)
	var (
		mu       sync.Mutex
		labels   = make(map[string]bool)
		services = make(map[string]bool)
		counts   = make(map[string][]int)
		inflight atomic.Int64
		peak     atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			seen := peak.Load()
			if cur <= seen || peak.CompareAndSwap(seen, cur) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read push body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := pushcodec.DecodeJSON(r.Context(), body)
		if err != nil {
			t.Errorf("decode push body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gate.wait(r.Context())
		mu.Lock()
		for _, stream := range req.Streams {
			key := stream.Labels.String()
			labels[key] = true
			counts[key] = append(counts[key], len(stream.Entries))
			if service, ok := stream.Labels.Get(defaultStreamLabel); ok {
				services[service] = true
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	err := run(ctx, []string{
		"ingest",
		"--url", srv.URL,
		"--bytes", "8000000",
		"--retries", "0",
		"--retry-backoff", "0",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("ingest command: %v\nstderr: %s", err, stderr.String())
	}
	var report Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("stdout is not a report: %v\n%s", err, stdout.String())
	}
	if want := (ParallelismStats{LabelSets: 16, ActiveSenders: 16, ConcurrencyLimit: 16, PeakInFlightPushes: 16}); report.Parallelism != want {
		t.Fatalf("default parallelism = %+v, want %+v", report.Parallelism, want)
	}
	if report.Batching.EntriesPerRequest != 1250 {
		t.Fatalf("default entries_per_request = %d, want 1250", report.Batching.EntriesPerRequest)
	}
	if got := peak.Load(); got != 16 {
		t.Fatalf("server admitted peak %d, want the default 16", got)
	}
	mu.Lock()
	gotLabels, gotServices := len(labels), len(services)
	gotCounts := make(map[string][]int, len(counts))
	for key, values := range counts {
		gotCounts[key] = slices.Clone(values)
	}
	mu.Unlock()
	if gotLabels != 16 || gotServices != 4 {
		t.Fatalf("server saw %d label sets over %d services, want 16 over 4", gotLabels, gotServices)
	}
	for key, values := range gotCounts {
		if len(values) < 2 {
			t.Fatalf("label set %s made %d requests, want a full and a final request", key, len(values))
		}
		for i, got := range values[:len(values)-1] {
			if got != 1250 {
				t.Fatalf("label set %s nonfinal request %d carried %d entries, want 1250", key, i, got)
			}
		}
		if last := values[len(values)-1]; last < 1 || last > 1250 {
			t.Fatalf("label set %s final request carried %d entries, want 1..1250", key, last)
		}
	}
}

// TestIngestCommandInstancesFlag checks the CLI instance flag plumbing and that
// an explicit non-positive value is rejected instead of being normalized.
func TestIngestCommandInstancesFlag(t *testing.T) {
	srv := quietPushServer(t)
	for _, value := range []string{"0", "-1"} {
		var stdout, stderr bytes.Buffer
		err := run(t.Context(), []string{
			"ingest", "--url", srv.URL, "--bytes", "1000", "--instances-per-service", value,
		}, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "instances-per-service") {
			t.Fatalf("--instances-per-service %s error = %v, want a flag rejection", value, err)
		}
	}

	var stdout, stderr bytes.Buffer
	err := run(t.Context(), []string{
		"ingest",
		"--url", srv.URL,
		"--bytes", "50000",
		"--instances-per-service", "2",
		"--retries", "0",
		"--retry-backoff", "0",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("ingest command: %v\nstderr: %s", err, stderr.String())
	}
	var report Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("stdout is not a report: %v", err)
	}
	if report.Parallelism.LabelSets != 8 || report.Corpus.InstancesPerService != 2 {
		t.Fatalf("instances=2 produced %d label sets, manifest instances %d",
			report.Parallelism.LabelSets, report.Corpus.InstancesPerService)
	}
}

func TestIngestCommandHelpAndErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(t.Context(), []string{"ingest", "-h"}, &stdout, &stderr); err != nil {
		t.Fatalf("ingest -h: %v", err)
	}
	if !strings.Contains(stderr.String(), "batch-lines") || !strings.Contains(stderr.String(), "instances-per-service") {
		t.Fatalf("help does not list flags: %q", stderr.String())
	}
	for _, args := range [][]string{
		{"ingest"},
		{"ingest", "--preset", "huge"},
		{"ingest", "--url", "http://127.0.0.1:1/push", "--bytes", "1000", "--streams", "bad name"},
	} {
		if err := run(t.Context(), args, io.Discard, io.Discard); err == nil {
			t.Fatalf("args %v were accepted", args)
		}
	}
}
