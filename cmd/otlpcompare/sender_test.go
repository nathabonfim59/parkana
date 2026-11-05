package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nathabonfim59/parkana/internal/tenant"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/protobuf/encoding/protowire"
)

// capturedOTLP is one request the fake receiver saw.
type capturedOTLP struct {
	contentType string
	encoding    string
	tenant      string
	records     int
	probe       bool
}

// fakeOTLP is a bounded OTLP/HTTP receiver for sender tests.
type fakeOTLP struct {
	mu sync.Mutex
	// failFixture, when positive, answers that many non-probe requests with
	// failStatus before accepting.
	failFixture int
	failStatus  int
	failProbe   bool
	requests    []capturedOTLP
}

func (f *fakeOTLP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			http.Error(w, "bad gzip", http.StatusBadRequest)
			return
		}
		body, err = io.ReadAll(reader)
		reader.Close()
		if err != nil {
			http.Error(w, "bad gzip", http.StatusBadRequest)
			return
		}
	}
	request := plogotlp.NewExportRequest()
	switch r.Header.Get("Content-Type") {
	case contentTypeProtobuf:
		err = request.UnmarshalProto(body)
	case contentTypeJSON:
		err = request.UnmarshalJSON(body)
	default:
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	records := request.Logs().LogRecordCount()
	captured := capturedOTLP{
		contentType: r.Header.Get("Content-Type"),
		encoding:    r.Header.Get("Content-Encoding"),
		tenant:      r.Header.Get(tenant.Header),
		records:     records,
		probe:       records == 0,
	}
	f.mu.Lock()
	f.requests = append(f.requests, captured)
	fail := false
	if captured.probe {
		fail = f.failProbe
	} else if f.failFixture > 0 {
		f.failFixture--
		fail = true
	}
	status := f.failStatus
	f.mu.Unlock()
	if fail {
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "not ready", status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeOTLP) snapshot() []capturedOTLP {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedOTLP(nil), f.requests...)
}

func sendArgs(serverURL, manifestPath string, extra ...string) []string {
	args := []string{
		"send",
		"-target", serverURL + "/v1/logs",
		"-manifest", manifestPath,
		"-anchor", formatTime(testAnchor()),
		"-namespace", "sender-ns",
		"-run-id", "sender-run",
		"-retry-backoff", "1ms",
		"-timeout", "2s",
		"-ready-timeout", "2s",
	}
	return append(args, extra...)
}

func TestSendEncodesCorpusAndWritesManifest(t *testing.T) {
	tests := []struct {
		name  string
		extra []string
		codec string
		gzip  bool
	}{
		{"protobuf", []string{"-codec", codecProtobuf}, codecProtobuf, false},
		{"json", []string{"-codec", codecJSON}, codecJSON, false},
		{"mixed gzip", []string{"-gzip"}, codecMixed, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeOTLP{failStatus: http.StatusServiceUnavailable}
			server := httptest.NewServer(fake)
			defer server.Close()
			manifestPath := filepath.Join(t.TempDir(), "manifest.json")

			var stdout, stderr bytes.Buffer
			code := run(t.Context(), sendArgs(server.URL, manifestPath, tc.extra...), &stdout, &stderr)
			if code != exitEqual {
				t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
			}
			requests := fake.snapshot()
			if len(requests) != len(fixtureCases())+1 {
				t.Fatalf("requests = %d, want %d (probe + cases)", len(requests), len(fixtureCases())+1)
			}
			if !requests[0].probe {
				t.Fatalf("first request is not the readiness probe: %+v", requests[0])
			}
			for i, req := range requests {
				if req.tenant != string(tenant.Default) {
					t.Fatalf("request %d tenant = %q", i, req.tenant)
				}
				if tc.gzip && req.encoding != "gzip" {
					t.Fatalf("request %d encoding = %q", i, req.encoding)
				}
				if !tc.gzip && req.encoding != "" {
					t.Fatalf("request %d encoding = %q", i, req.encoding)
				}
			}
			cases := fixtureCases()
			for i, req := range requests[1:] {
				wantCodec := codecFor(tc.codec, i)
				wantType := contentTypeProtobuf
				if wantCodec == codecJSON {
					wantType = contentTypeJSON
				}
				if req.contentType != wantType {
					t.Fatalf("case %d content type = %q, want %q", i, req.contentType, wantType)
				}
				if req.records != len(cases[i].records) {
					t.Fatalf("case %q records = %d, want %d", cases[i].name, req.records, len(cases[i].records))
				}
			}

			loaded, times, err := readManifest(manifestPath)
			if err != nil {
				t.Fatalf("read manifest: %v", err)
			}
			if loaded.Namespace != "sender-ns" || loaded.RunID != "sender-run" {
				t.Fatalf("manifest identity = %q/%q", loaded.Namespace, loaded.RunID)
			}
			if !times.anchor.Equal(testAnchor()) {
				t.Fatalf("manifest anchor = %s", times.anchor)
			}
			if loaded.Gzip != tc.gzip || loaded.CodecMode != tc.codec {
				t.Fatalf("manifest transport = %+v", loaded)
			}
		})
	}
}

// lokiStatusWire builds the canonical protobuf google.rpc.Status encoding
// (proto3 omits zero code, so the usual body is the message field alone).
func lokiStatusWire(message string, code *uint64, details bool) []byte {
	var body []byte
	if code != nil {
		body = protowire.AppendTag(body, 1, protowire.VarintType)
		body = protowire.AppendVarint(body, *code)
	}
	body = protowire.AppendTag(body, 2, protowire.BytesType)
	body = protowire.AppendString(body, message)
	if details {
		body = protowire.AppendTag(body, 3, protowire.BytesType)
		body = protowire.AppendString(body, "unused detail")
	}
	return body
}

func uint64Ptr(v uint64) *uint64 { return &v }

func TestLokiMissingStreamsCanonicalBytes(t *testing.T) {
	// This is the exact canonical encoding of
	// google.rpc.Status{Code: 0, Message: "error at least one valid stream is
	// required for ingestion"}: field 2 (0x12), length 57 (0x39), message.
	want, err := hex.DecodeString("1239" + hex.EncodeToString([]byte(lokiMissingStreamsMessage)))
	if err != nil {
		t.Fatalf("decode expected bytes: %v", err)
	}
	got := lokiStatusWire(lokiMissingStreamsMessage, nil, false)
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical Status bytes = %x, want %x", got, want)
	}
	if !isLokiMissingStreamsStatus(got) {
		t.Fatal("canonical Status bytes rejected")
	}
	if isLokiMissingStreamsStatus([]byte("Missing Streams\n")) {
		t.Fatal("plain-text Missing Streams accepted")
	}
}

func TestSendReadinessStatusPolicy(t *testing.T) {
	plainText := []byte("Missing Streams\n")
	canonical := lokiStatusWire(lokiMissingStreamsMessage, nil, false)
	explicitCode := lokiStatusWire(lokiMissingStreamsMessage, uint64Ptr(0), false)
	withDetails := lokiStatusWire(lokiMissingStreamsMessage, nil, true)
	wrongMessage := lokiStatusWire("something else", nil, false)
	nonzeroCode := lokiStatusWire(lokiMissingStreamsMessage, uint64Ptr(2), false)
	malformed := []byte{0x12, 0x05, 'a'} // length says 5, one byte follows
	unknownField := append([]byte(nil), canonical...)
	unknownField = protowire.AppendTag(unknownField, 4, protowire.VarintType)
	unknownField = protowire.AppendVarint(unknownField, 1)
	tests := []struct {
		name   string
		status int
		body   []byte
		want   int
	}{
		{"200", http.StatusOK, []byte("ok"), exitEqual},
		{"204", http.StatusNoContent, nil, exitEqual},
		{"loki canonical status", http.StatusUnprocessableEntity, canonical, exitEqual},
		{"explicit zero code", http.StatusUnprocessableEntity, explicitCode, exitEqual},
		{"with details", http.StatusUnprocessableEntity, withDetails, exitEqual},
		{"plain missing streams", http.StatusUnprocessableEntity, plainText, exitFailure},
		{"wrong 422 message", http.StatusUnprocessableEntity, wrongMessage, exitFailure},
		{"nonzero code", http.StatusUnprocessableEntity, nonzeroCode, exitFailure},
		{"malformed protobuf", http.StatusUnprocessableEntity, malformed, exitFailure},
		{"empty 422", http.StatusUnprocessableEntity, nil, exitFailure},
		{"unknown field", http.StatusUnprocessableEntity, unknownField, exitFailure},
		{"404 canonical status", http.StatusNotFound, canonical, exitFailure},
		{"500 canonical status", http.StatusInternalServerError, canonical, exitFailure},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
				if len(body) == 0 {
					// The readiness probe is an empty OTLP export.
					w.WriteHeader(tc.status)
					w.Write(tc.body)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			manifestPath := filepath.Join(t.TempDir(), "manifest.json")
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), sendArgs(server.URL, manifestPath,
				"-codec", codecProtobuf, "-ready-timeout", "20ms", "-retry-backoff", "1ms", "-retries", "1"),
				&stdout, &stderr)
			if code != tc.want {
				t.Fatalf("exit = %d, want %d; stderr=%q", code, tc.want, stderr.String())
			}
		})
	}
}

func TestSendRetriesTransientFailures(t *testing.T) {
	fake := &fakeOTLP{failFixture: 2, failStatus: http.StatusServiceUnavailable}
	server := httptest.NewServer(fake)
	defer server.Close()
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")

	var stdout, stderr bytes.Buffer
	code := run(t.Context(), sendArgs(server.URL, manifestPath,
		"-codec", codecProtobuf, "-retries", "3"), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	requests := fake.snapshot()
	// probe + two rejected attempts + one accepted attempt + the other cases.
	if got, want := len(requests), len(fixtureCases())+3; got != want {
		t.Fatalf("requests = %d, want %d", got, want)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("manifest not written: %v", err)
	}
}

func TestSendStopsOnNonRetryableStatus(t *testing.T) {
	fake := &fakeOTLP{failFixture: 1, failStatus: http.StatusBadRequest}
	server := httptest.NewServer(fake)
	defer server.Close()
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")

	var stdout, stderr bytes.Buffer
	code := run(t.Context(), sendArgs(server.URL, manifestPath,
		"-codec", codecProtobuf, "-retries", "5"), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d; requests=%+v stderr=%q", code, exitFailure, fake.snapshot(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "not retryable") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("manifest written after failure: %v", err)
	}
	// The probe plus one fixture request; 400 must not be retried.
	if got := len(fake.snapshot()); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
}

func TestSendFailsAfterBoundedRetries(t *testing.T) {
	fake := &fakeOTLP{failFixture: 100, failStatus: http.StatusServiceUnavailable}
	server := httptest.NewServer(fake)
	defer server.Close()
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")

	var stdout, stderr bytes.Buffer
	code := run(t.Context(), sendArgs(server.URL, manifestPath,
		"-codec", codecProtobuf, "-retries", "2"), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "failed after 2 attempts") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSendReadinessFailureIsBounded(t *testing.T) {
	fake := &fakeOTLP{failProbe: true, failStatus: http.StatusServiceUnavailable}
	server := httptest.NewServer(fake)
	defer server.Close()
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")

	var stdout, stderr bytes.Buffer
	code := run(t.Context(), sendArgs(server.URL, manifestPath,
		"-codec", codecProtobuf, "-ready-timeout", "30ms"), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "not ready after") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	for _, req := range fake.snapshot() {
		if !req.probe {
			t.Fatal("fixture request was sent before readiness")
		}
	}
}

func TestSendRejectsInvalidFlags(t *testing.T) {
	tests := [][]string{
		{"send"},
		{"send", "-target", "ftp://x/v1/logs", "-manifest", "m.json"},
		{"send", "-target", "http://x/v1/logs", "-manifest", "m.json", "-codec", "xml"},
		{"send", "-target", "http://x/v1/logs", "-manifest", "m.json", "-tenant", "bad tenant"},
		{"send", "-target", "http://x/v1/logs", "-manifest", "m.json", "-retries", "0"},
		{"send", "-target", "http://x/v1/logs", "-manifest", "m.json", "-namespace", "bad ns"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), args, &stdout, &stderr); code != exitFailure {
			t.Fatalf("%v: exit = %d, want %d", args, code, exitFailure)
		}
	}
}
