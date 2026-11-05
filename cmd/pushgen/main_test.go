// The main tests decode real wire formats with the shared pushcodec; they
// compile and run once internal/ingest/pushcodec is available in this
// worktree (see A1).
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json/v2"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// pushRecord is one decoded push request received by the test server.
type pushRecord struct {
	tenant  tenant.ID
	streams []logs.Stream
}

// pushServer accepts pushes in the given format (with optional gzip) and
// decodes them with the shared codec.
func pushServer(t *testing.T, format string) (*httptest.Server, func() []pushRecord) {
	t.Helper()
	var mu sync.Mutex
	var recs []pushRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Errorf("gunzip: %v", err)
				return
			}
			if body, err = io.ReadAll(zr); err != nil {
				t.Errorf("gunzip read: %v", err)
				return
			}
		}
		id := tenant.ID(r.Header.Get(tenant.Header))
		if id == "" {
			id = tenant.Default
		}
		if err := id.Validate(); err != nil {
			t.Errorf("tenant: %v", err)
			return
		}
		var req *logs.PushRequest
		switch format {
		case "protobuf":
			// The wire body is block-snappy compressed; decode like the
			// push handler does before parsing the protobuf.
			if raw, err := snappy.Decode(nil, body); err != nil {
				t.Errorf("snappy decode: %v", err)
				return
			} else {
				body = raw
			}
			req, err = pushcodec.DecodeProto(tenant.With(r.Context(), id), body)
		default:
			req, err = pushcodec.DecodeJSON(tenant.With(r.Context(), id), body)
		}
		if err != nil {
			t.Errorf("decode %s push: %v", format, err)
			return
		}
		mu.Lock()
		recs = append(recs, pushRecord{tenant: id, streams: req.Streams})
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	return srv, func() []pushRecord {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(recs)
	}
}

// ids collects the deterministic line identifiers of a decoded push.
func ids(records []pushRecord) map[int]int {
	out := make(map[int]int)
	for _, rec := range records {
		for _, s := range rec.streams {
			for _, e := range s.Entries {
				if n, ok := lineID(e.Line); ok {
					out[n]++
				}
			}
		}
	}
	return out
}

// lineID reads the deterministic identifier of a synthetic line
// "id=<n> stream=<name>".
func lineID(line string) (int, bool) {
	rest, ok := strings.CutPrefix(line, "id=")
	if !ok {
		return 0, false
	}
	if i := strings.IndexByte(rest, ' '); i >= 0 {
		rest = rest[:i]
	}
	n, err := strconv.Atoi(rest)
	return n, err == nil
}

// TestRunHelp checks that -h prints the flag help through the flag package:
// usage rendering builds a zero repeatList, which must not panic.
func TestRunHelp(t *testing.T) {
	var out bytes.Buffer
	if err := run(t.Context(), []string{"-h"}, &out); err != nil {
		t.Fatalf("run(-h) = %v, want nil (help exits successfully)", err)
	}
	help := out.String()
	for _, want := range []string{"-url", "-tenant", "-format", "-stream", "-label", "-rate", "-batch", "-total", "-profile", "-timeout", "-gzip"} {
		if !strings.Contains(help, want) {
			t.Errorf("help output missing -%s flag:\n%s", want, help)
		}
	}
	if strings.Contains(help, "panic") {
		t.Errorf("help output contains a panic diagnostic:\n%s", help)
	}
}

func TestRunProtobufWire(t *testing.T) {
	srv, records := pushServer(t, "protobuf")
	defer srv.Close()

	err := run(t.Context(), []string{
		"-url=" + srv.URL + "/loki/api/v1/push", "-format=protobuf",
		"-tenant=acme", "-stream=a", "-stream=b", "-label=level=info",
		"-total=7", "-batch=3",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	recs := records()
	if len(recs) != 3 {
		t.Fatalf("requests = %d, want 3", len(recs))
	}
	got := ids(recs)
	if len(got) != 7 {
		t.Fatalf("line ids = %v, want exactly 0..6", got)
	}
	for id := range 7 {
		if got[id] != 1 {
			t.Errorf("line id=%d received %d times, want once", id, got[id])
		}
	}
	total := 0
	for _, rec := range recs {
		if rec.tenant != "acme" {
			t.Errorf("tenant = %q, want acme", rec.tenant)
		}
		for _, s := range rec.streams {
			if v, _ := s.Labels.Get("level"); v != "info" {
				t.Errorf("stream %v missing label level=info", s.Labels)
			}
			if name, _ := s.Labels.Get("service_name"); name != "a" && name != "b" {
				t.Errorf("stream label service_name = %q, want a or b", name)
			}
			total += len(s.Entries)
		}
	}
	if total != 7 {
		t.Errorf("decoded entries = %d, want 7", total)
	}
}

func TestRunJSONWireGzip(t *testing.T) {
	srv, records := pushServer(t, "json")
	defer srv.Close()

	err := run(t.Context(), []string{
		"-url=" + srv.URL + "/loki/api/v1/push", "-format=json", "-gzip",
		"-tenant=acme", "-stream=one", "-total=2", "-batch=5",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	recs := records()
	if len(recs) != 1 {
		t.Fatalf("requests = %d, want 1 (2 lines, batch 5)", len(recs))
	}
	rec := recs[0]
	if rec.tenant != "acme" {
		t.Errorf("tenant = %q, want acme", rec.tenant)
	}
	if len(rec.streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(rec.streams))
	}
	s := rec.streams[0]
	if name, _ := s.Labels.Get("service_name"); name != "one" {
		t.Errorf("stream label service_name = %q, want one", name)
	}
	if len(s.Entries) != 2 {
		t.Errorf("entries = %d, want 2", len(s.Entries))
	}
}

// corpusEntryID extracts the embedded record id of a corpus line in any
// format (json field, logfmt id= prefix, plain trailing " id=<n>").
func corpusEntryID(t *testing.T, line string) int {
	t.Helper()
	var digits string
	switch {
	case strings.HasPrefix(line, "{"):
		var v struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatalf("corpus json line %q: %v", line, err)
		}
		return v.ID
	case strings.HasPrefix(line, "id="):
		digits = line[len("id="):]
		if i := strings.IndexByte(digits, ' '); i >= 0 {
			digits = digits[:i]
		}
	default:
		_, tail, ok := strings.CutLast(line, " id=")
		if !ok {
			t.Fatalf("corpus plain line %q has no id", line)
		}
		digits = tail
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		t.Fatalf("corpus line %q: %v", line, err)
	}
	return n
}

// corpusEntry is one decoded corpus entry with its stream's labels.
type corpusEntry struct {
	id       int
	ts       time.Time
	line     string
	labels   string // canonical label set
	metadata map[string]string
}

func collectCorpus(t *testing.T, records []pushRecord) map[int]corpusEntry {
	t.Helper()
	out := make(map[int]corpusEntry)
	for _, rec := range records {
		for _, s := range rec.streams {
			for _, e := range s.Entries {
				id := corpusEntryID(t, e.Line)
				if _, dup := out[id]; dup {
					t.Fatalf("line id=%d received more than once", id)
				}
				out[id] = corpusEntry{
					id: id, ts: e.Timestamp, line: e.Line,
					labels: s.Labels.String(), metadata: e.Metadata.Map(),
				}
			}
		}
	}
	return out
}

// lineFormat detects the corpus line encoding from its shape.
func lineFormat(line string) string {
	switch {
	case strings.HasPrefix(line, "{"):
		return "json"
	case strings.HasPrefix(line, "id="):
		return "logfmt"
	default:
		return "plain"
	}
}

// corpusLineLevelPod extracts the level and pod fields inside a corpus line
// of the given format, so wire rows can be checked against their labels.
func corpusLineLevelPod(t *testing.T, format, line string) (level, pod string) {
	t.Helper()
	switch format {
	case "json":
		var v struct {
			Level string `json:"level"`
			Pod   string `json:"pod"`
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatalf("corpus json line %q: %v", line, err)
		}
		return v.Level, v.Pod
	case "logfmt":
		for tok := range strings.FieldsSeq(line) {
			if v, ok := strings.CutPrefix(tok, "level="); ok {
				level = v
			}
			if v, ok := strings.CutPrefix(tok, "pod="); ok {
				pod = v
			}
		}
		if level == "" || pod == "" {
			t.Fatalf("corpus logfmt line %q missing level/pod", line)
		}
		return level, pod
	default: // plain: "<LEVEL> <pod> ..."
		level, rest, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("corpus plain line %q too short", line)
		}
		pod, _, _ = strings.Cut(rest, " ")
		return strings.ToLower(level), pod
	}
}

// TestRunTimestampsMicroAligned is a regression: every generated timestamp,
// in both profiles and both wire formats including a partial final batch,
// must land exactly on the microsecond storage grid.
func TestRunTimestampsMicroAligned(t *testing.T) {
	for _, format := range []string{"json", "protobuf"} {
		for _, profile := range []string{"", "corpus"} {
			name := format
			if profile != "" {
				name += "/" + profile
			}
			t.Run(name, func(t *testing.T) {
				srv, records := pushServer(t, format)
				defer srv.Close()

				args := []string{
					"-url=" + srv.URL + "/loki/api/v1/push", "-format=" + format,
					"-stream=a", "-stream=b", "-total=53", "-batch=7",
				}
				if profile != "" {
					args = append(args, "-profile="+profile)
				}
				if err := run(t.Context(), args, io.Discard); err != nil {
					t.Fatal(err)
				}

				entries := 0
				for _, rec := range records() {
					for _, s := range rec.streams {
						for _, e := range s.Entries {
							entries++
							if ns := e.Timestamp.Nanosecond() % 1000; ns != 0 {
								t.Errorf("entry %q timestamp %v has %d ns sub-microsecond, want aligned",
									e.Line, e.Timestamp, ns)
							}
						}
					}
				}
				if entries != 53 {
					t.Errorf("decoded %d entries, want 53 (odd total, partial batch)", entries)
				}
			})
		}
	}
}

// TestRunCorpusJSONWire pushes the full 1200-line corpus fixture over real
// JSON and checks the decoded streams end to end.
func TestRunCorpusJSONWire(t *testing.T) {
	srv, records := pushServer(t, "json")
	defer srv.Close()

	err := run(t.Context(), []string{
		"-url=" + srv.URL + "/loki/api/v1/push", "-profile=corpus", "-format=json",
		"-tenant=acme", "-stream=corpus", "-label=job=corpus", "-label=env=dev",
		"-total=1200", "-batch=100",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	recs := records()
	if len(recs) != 12 {
		t.Fatalf("requests = %d, want 12 (1200 lines, batch 100)", len(recs))
	}
	formats := map[string]int{}
	levels := map[string]int{}
	pods := map[string]int{}
	entries := 0
	for _, rec := range recs {
		if rec.tenant != "acme" {
			t.Errorf("tenant = %q, want acme", rec.tenant)
		}
		labelSets := map[string]int{}
		for _, s := range rec.streams {
			labelSets[s.Labels.String()]++
			if labelSets[s.Labels.String()] > 1 {
				t.Errorf("push carries duplicate stream label set %s", s.Labels)
			}
			if err := s.Labels.Validate(); err != nil {
				t.Fatalf("stream labels %v: %v", s.Labels, err)
			}
			format, _ := s.Labels.Get("format")
			level, _ := s.Labels.Get("level")
			pod, _ := s.Labels.Get("pod")
			streamName, _ := s.Labels.Get("service_name")
			if format == "" || level == "" || pod == "" {
				t.Errorf("stream %v missing corpus labels", s.Labels)
			}
			if streamName != "corpus" {
				t.Errorf("stream %v label service_name = %q, want corpus", s.Labels, streamName)
			}
			if v, _ := s.Labels.Get("job"); v != "corpus" {
				t.Errorf("stream %v missing label job=corpus", s.Labels)
			}
			if v, _ := s.Labels.Get("env"); v != "dev" {
				t.Errorf("stream %v missing label env=dev", s.Labels)
			}
			var prev time.Time
			for _, e := range s.Entries {
				entries++
				formats[format]++
				levels[level]++
				pods[pod]++
				if got := lineFormat(e.Line); got != format {
					t.Errorf("entry in format=%s stream carries %s line %q", format, got, e.Line)
				}
				if lv, pd := corpusLineLevelPod(t, format, e.Line); lv != level || pd != pod {
					t.Errorf("entry line level/pod %q/%q != stream labels %q/%q: %q",
						lv, pd, level, pod, e.Line)
				}
				if e.Timestamp.Before(prev) {
					t.Errorf("stream %v entries out of order at %v", s.Labels, e.Timestamp)
				}
				prev = e.Timestamp
				tid, _ := e.Metadata.Get("trace_id")
				rid, _ := e.Metadata.Get("request_id")
				if len(tid) != 16 || rid == "" {
					t.Errorf("entry %q metadata = %v, want trace_id+request_id", e.Line, e.Metadata)
				}
			}
		}
	}
	if entries != 1200 {
		t.Fatalf("decoded %d entries, want exactly 1200", entries)
	}
	for _, want := range []string{"json", "logfmt", "plain"} {
		if formats[want] == 0 {
			t.Errorf("format %s has no entries", want)
		}
	}
	for _, want := range []string{"info", "warn", "error"} {
		if levels[want] == 0 {
			t.Errorf("level %s has no entries", want)
		}
	}
	for _, want := range []string{"pod-a", "pod-b"} {
		if pods[want] == 0 {
			t.Errorf("pod %s has no entries", want)
		}
	}
	got := collectCorpus(t, recs)
	if len(got) != 1200 {
		t.Fatalf("distinct line ids = %d, want exactly 0..1199", len(got))
	}
	for id := range 1200 {
		if _, ok := got[id]; !ok {
			t.Errorf("line id=%d missing", id)
		}
	}
}

// TestRunCorpusProtobufWire checks corpus metadata and label grouping
// survive the protobuf/snappy codec on an odd total with partial batches.
func TestRunCorpusProtobufWire(t *testing.T) {
	srv, records := pushServer(t, "protobuf")
	defer srv.Close()

	err := run(t.Context(), []string{
		"-url=" + srv.URL + "/loki/api/v1/push", "-profile=corpus", "-format=protobuf",
		"-stream=corpus", "-label=job=corpus", "-total=53", "-batch=7",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	recs := records()
	if len(recs) != 8 {
		t.Fatalf("requests = %d, want 8 (53 lines, batch 7)", len(recs))
	}
	got := collectCorpus(t, recs)
	if len(got) != 53 {
		t.Fatalf("distinct line ids = %d, want exactly 0..52", len(got))
	}
	for id := range 53 {
		e, ok := got[id]
		if !ok {
			t.Errorf("line id=%d missing", id)
			continue
		}
		if e.metadata["trace_id"] == "" || e.metadata["request_id"] == "" {
			t.Errorf("line id=%d metadata = %v, want trace_id+request_id", id, e.metadata)
		}
		if !strings.Contains(e.labels, `format="`) {
			t.Errorf("line id=%d labels %s missing format", id, e.labels)
		}
	}
}

// TestRunCorpusDeterministicAcrossBatches pushes the same corpus twice with
// different batch sizes; every line, label set and metadata must match.
func TestRunCorpusDeterministicAcrossBatches(t *testing.T) {
	push := func(batch int) map[int]corpusEntry {
		srv, records := pushServer(t, "json")
		defer srv.Close()
		err := run(t.Context(), []string{
			"-url=" + srv.URL + "/loki/api/v1/push", "-profile=corpus", "-format=json",
			"-stream=corpus", "-stream=alt", "-label=job=corpus",
			"-total=250", "-batch=" + strconv.Itoa(batch),
		}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		return collectCorpus(t, records())
	}
	a := push(13)
	b := push(64)
	if len(a) != 250 || len(b) != 250 {
		t.Fatalf("entries = %d/%d, want 250 each", len(a), len(b))
	}
	for id := range 250 {
		ea, eb := a[id], b[id]
		if ea.line != eb.line {
			t.Errorf("id=%d line differs across batch sizes:\n%q vs %q", id, ea.line, eb.line)
		}
		if ea.labels != eb.labels {
			t.Errorf("id=%d labels differ across batch sizes:\n%s vs %s", id, ea.labels, eb.labels)
		}
		if !maps.Equal(ea.metadata, eb.metadata) {
			t.Errorf("id=%d metadata differs across batch sizes:\n%v vs %v", id, ea.metadata, eb.metadata)
		}
		// Round-robin stream assignment: id -> stream names[id % 2].
		wantStream := "corpus"
		if id%2 == 1 {
			wantStream = "alt"
		}
		if !strings.Contains(ea.labels, `service_name="`+wantStream+`"`) {
			t.Errorf("id=%d labels %s, want stream %s", id, ea.labels, wantStream)
		}
	}
}

func TestRunFlagErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no url", []string{"-total=1"}, "no -url"},
		{"bad format", []string{"-url=http://x", "-format=xml"}, "-format"},
		{"bad label", []string{"-url=http://x", "-label=foo"}, "not name=value"},
		{"empty label name", []string{"-url=http://x", "-label==v"}, "-label"},
		{"bad tenant", []string{"-url=http://x", "-tenant=bad tenant!"}, "-tenant"},
		{"empty stream", []string{"-url=http://x", "-stream= "}, "-stream"},
		{"negative timeout", []string{"-url=http://x", "-timeout=-1s"}, "-timeout"},
		{"bad profile", []string{"-url=http://x", "-profile=bogus"}, "-profile"},
		{"extra arg", []string{"-url=http://x", "go"}, "unexpected arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Validation fails before any HTTP call; the URL need not exist.
			if err := run(t.Context(), tc.args, io.Discard); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("run(%v) = %v, want error containing %q", tc.args, err, tc.want)
			}
		})
	}
}
