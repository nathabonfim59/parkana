package gen_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/cmd/pushgen/gen"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// jsonEncoder renders real Loki JSON push tuples. Test-side only; production
// JSON encoding lives in pushcodec.
type jsonEncoder struct{}

func (jsonEncoder) ContentType() string { return "application/json" }

type wireStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

type wirePush struct {
	Streams []wireStream `json:"streams"`
}

func (jsonEncoder) Encode(_ context.Context, req *logs.PushRequest) ([]byte, error) {
	out := wirePush{}
	for _, s := range req.Streams {
		ws := wireStream{Stream: s.Labels.Map()}
		for _, e := range s.Entries {
			tuple := []any{strconv.FormatInt(e.Timestamp.UnixNano(), 10), e.Line}
			if len(e.Metadata) > 0 {
				tuple = append(tuple, e.Metadata.Map())
			}
			ws.Values = append(ws.Values, tuple)
		}
		out.Streams = append(out.Streams, ws)
	}
	return json.Marshal(out)
}

// opaqueEncoder stands in for the protobuf encoder: protobuf-format bodies are
// opaque bytes to this test, so it emits non-JSON lines with the protobuf
// content type. This exercises headers, gzip and exact totals without the
// wire codec.
type opaqueEncoder struct{}

func (opaqueEncoder) ContentType() string { return "application/x-protobuf" }

func (opaqueEncoder) Encode(_ context.Context, req *logs.PushRequest) ([]byte, error) {
	var b []byte
	for _, s := range req.Streams {
		for _, e := range s.Entries {
			b = append(b, e.Line...)
			b = append(b, '\n')
		}
	}
	return b, nil
}

// streamEntries is one decoded wire stream with its entries.
type streamEntries struct {
	labels map[string]string
	lines  []string
	metas  []map[string]string // aligned with lines; nil when no metadata
}

// record is one received push request.
type record struct {
	contentType string
	contentEnc  string
	tenant      string
	lines       []string
	streams     []map[string]string // JSON bodies only
	entries     []streamEntries     // JSON bodies only
}

// recorder is a push endpoint that records every request.
type recorder struct {
	mu      sync.Mutex
	records []record
}

func (rec *recorder) handle(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /loki/api/v1/push", func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		if enc := req.Header.Get("Content-Encoding"); enc == "gzip" {
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
		r := record{
			contentType: req.Header.Get("Content-Type"),
			contentEnc:  req.Header.Get("Content-Encoding"),
			tenant:      req.Header.Get("X-Scope-OrgID"),
		}
		if req.Header.Get("Content-Type") == "application/json" {
			var push wirePush
			if err := json.Unmarshal(body, &push); err != nil {
				t.Errorf("decode json push: %v", err)
				return
			}
			for _, s := range push.Streams {
				r.streams = append(r.streams, s.Stream)
				se := streamEntries{labels: s.Stream}
				for _, v := range s.Values {
					line, _ := v[1].(string)
					r.lines = append(r.lines, line)
					se.lines = append(se.lines, line)
					var meta map[string]string
					if len(v) > 2 {
						if m, ok := v[2].(map[string]any); ok {
							meta = make(map[string]string, len(m))
							for k, val := range m {
								meta[k], _ = val.(string)
							}
						}
					}
					se.metas = append(se.metas, meta)
				}
				r.entries = append(r.entries, se)
			}
		} else {
			for line := range strings.SplitSeq(string(body), "\n") {
				if line != "" {
					r.lines = append(r.lines, line)
				}
			}
		}
		rec.mu.Lock()
		rec.records = append(rec.records, r)
		rec.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (rec *recorder) all() []record {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return slices.Clone(rec.records)
}

// parseLine reads a synthetic line "id=<n> stream=<name>".
func parseLine(line string) (id int, stream string, ok bool) {
	idPart, rest, found := strings.Cut(line, " ")
	if !found || !strings.HasPrefix(idPart, "id=") || !strings.HasPrefix(rest, "stream=") {
		return 0, "", false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(idPart, "id="))
	if err != nil {
		return 0, "", false
	}
	return n, strings.TrimPrefix(rest, "stream="), true
}

func TestRunFormats(t *testing.T) {
	for _, tc := range []struct {
		name   string
		enc    gen.Encoder
		gzip   bool
		wantCT string
		wantCE string
	}{
		{"json", jsonEncoder{}, false, "application/json", ""},
		{"json gzip", jsonEncoder{}, true, "application/json", "gzip"},
		{"protobuf-shaped", opaqueEncoder{}, false, "application/x-protobuf", ""},
		{"protobuf-shaped gzip", opaqueEncoder{}, true, "application/x-protobuf", "gzip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handle(t))
			defer srv.Close()

			var out bytes.Buffer
			err := gen.Run(t.Context(), gen.Config{
				URL:       srv.URL + "/loki/api/v1/push",
				Tenant:    "acme",
				Streams:   []string{"a", "b"},
				Labels:    logs.Labels{{Name: "level", Value: "info"}},
				BatchSize: 3,
				Total:     7,
				Gzip:      tc.gzip,
				Encoder:   tc.enc,
				Output:    &out,
			})
			if err != nil {
				t.Fatal(err)
			}

			recs := rec.all()
			if len(recs) != 3 {
				t.Fatalf("requests = %d, want 3 (7 lines, batch 3)", len(recs))
			}
			if len(recs[2].lines) != 1 {
				t.Errorf("final partial batch carries %d lines, want 1", len(recs[2].lines))
			}
			var gotLines []string
			for i, r := range recs {
				if r.contentType != tc.wantCT {
					t.Errorf("request %d Content-Type = %q, want %q", i, r.contentType, tc.wantCT)
				}
				if r.contentEnc != tc.wantCE {
					t.Errorf("request %d Content-Encoding = %q, want %q", i, r.contentEnc, tc.wantCE)
				}
				if r.tenant != "acme" {
					t.Errorf("request %d tenant = %q, want acme", i, r.tenant)
				}
				gotLines = append(gotLines, r.lines...)
			}
			if len(gotLines) != 7 {
				t.Fatalf("received %d lines, want exactly 7", len(gotLines))
			}
			wantStream := func(id int) string { return []string{"a", "b"}[id%2] }
			seen := make(map[int]int, 7)
			for _, line := range gotLines {
				id, stream, ok := parseLine(line)
				if !ok {
					t.Errorf("unparseable line %q", line)
					continue
				}
				seen[id]++
				if stream != wantStream(id) {
					t.Errorf("line id=%d on stream %q, want %q", id, stream, wantStream(id))
				}
			}
			for id := range 7 {
				if seen[id] != 1 {
					t.Errorf("line id=%d received %d times, want once", id, seen[id])
				}
			}
			for _, r := range recs {
				for _, labels := range r.streams {
					if labels["level"] != "info" {
						t.Errorf("stream labels missing extra label: %v", labels)
					}
					if name := labels["service_name"]; name != "a" && name != "b" {
						t.Errorf("stream label service_name = %q, want a or b", name)
					}
				}
			}
			if want := "pushgen: sent 7 lines in 3 requests"; !strings.Contains(out.String(), want) {
				t.Errorf("summary = %q, want it to contain %q", out.String(), want)
			}
		})
	}
}

func TestRunDefaults(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handle(t))
	defer srv.Close()

	err := gen.Run(t.Context(), gen.Config{
		URL: srv.URL + "/loki/api/v1/push", Total: 2, BatchSize: 1, Encoder: jsonEncoder{},
	})
	if err != nil {
		t.Fatal(err)
	}
	recs := rec.all()
	if len(recs) != 2 {
		t.Fatalf("requests = %d, want 2", len(recs))
	}
	for _, r := range recs {
		for _, labels := range r.streams {
			if labels["service_name"] != gen.DefaultStreamName {
				t.Errorf("default stream label = %q, want %q", labels["service_name"], gen.DefaultStreamName)
			}
		}
		for _, line := range r.lines {
			if _, stream, ok := parseLine(line); !ok || stream != gen.DefaultStreamName {
				t.Errorf("line %q not on default stream %q", line, gen.DefaultStreamName)
			}
		}
	}
}

func TestRunOmitsTenantHeaderWhenEmpty(t *testing.T) {
	var mu sync.Mutex
	var gotTenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		mu.Lock()
		gotTenant = r.Header.Get("X-Scope-OrgID")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	err := gen.Run(t.Context(), gen.Config{
		URL: srv.URL, Total: 1, BatchSize: 1, Tenant: "", Encoder: jsonEncoder{},
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotTenant != "" {
		t.Errorf("tenant header sent although Tenant is empty: %q", gotTenant)
	}
}

func TestRunFailsOnNon204(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				http.Error(w, "boom", status)
			}))
			defer srv.Close()

			err := gen.Run(t.Context(), gen.Config{
				URL: srv.URL, Total: 1, BatchSize: 1, Encoder: jsonEncoder{},
			})
			if err == nil || !strings.Contains(err.Error(), strconv.Itoa(status)) || !strings.Contains(err.Error(), "boom") {
				t.Errorf("Run() = %v, want failure mentioning status %d and body", err, status)
			}
		})
	}
}

func TestRunTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	err := gen.Run(t.Context(), gen.Config{
		URL: srv.URL, Total: 1, BatchSize: 1, Encoder: jsonEncoder{},
		Client: &http.Client{Timeout: time.Millisecond},
	})
	if err == nil || !strings.Contains(err.Error(), "Timeout") {
		t.Errorf("Run() = %v, want a client timeout error", err)
	}
}

func TestRunCancelWhilePacing(t *testing.T) {
	var reqs atomic.Int32
	first := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		reqs.Add(1)
		once.Do(func() { close(first) })
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- gen.Run(ctx, gen.Config{
			URL: srv.URL, Total: 100, BatchSize: 1, Rate: 10, Encoder: jsonEncoder{}, // 100ms between batches
		})
	}()
	<-first
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run() = %v, want context.Canceled", err)
	}
	if n := reqs.Load(); n != 1 {
		t.Errorf("requests = %d, want 1 (canceled during pacing)", n)
	}
}

// ctxTenantEncoder records the tenant carried by the Encode context. Its
// content type is neither JSON nor protobuf so the recorder treats the body
// opaquely.
type ctxTenantEncoder struct {
	mu      sync.Mutex
	tenants []tenant.ID
}

func (e *ctxTenantEncoder) ContentType() string { return "application/x-tenant-check" }

func (e *ctxTenantEncoder) Encode(ctx context.Context, _ *logs.PushRequest) ([]byte, error) {
	e.mu.Lock()
	e.tenants = append(e.tenants, tenant.From(ctx))
	e.mu.Unlock()
	return []byte("x"), nil
}

func TestRunTenantInContextAndHeader(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cfgTenant  tenant.ID
		wantCtx    tenant.ID
		wantHeader string
	}{
		{"explicit tenant", "acme", "acme", "acme"},
		{"empty tenant defaults context, omits header", "", tenant.Default, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			srv := httptest.NewServer(rec.handle(t))
			defer srv.Close()

			enc := &ctxTenantEncoder{}
			err := gen.Run(t.Context(), gen.Config{
				URL: srv.URL + "/loki/api/v1/push", Tenant: tc.cfgTenant,
				Total: 2, BatchSize: 1, Encoder: enc,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(enc.tenants, []tenant.ID{tc.wantCtx, tc.wantCtx}) {
				t.Errorf("encoder contexts carried %v, want [%v %v]", enc.tenants, tc.wantCtx, tc.wantCtx)
			}
			for i, r := range rec.all() {
				if r.tenant != tc.wantHeader {
					t.Errorf("request %d tenant header = %q, want %q", i, r.tenant, tc.wantHeader)
				}
			}
		})
	}
}

func TestRunInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  gen.Config
		want string
	}{
		{"no url", gen.Config{Total: 1, BatchSize: 1, Encoder: jsonEncoder{}}, "no -url"},
		{"zero total", gen.Config{URL: "http://x", BatchSize: 1, Encoder: jsonEncoder{}}, "-total"},
		{"negative total", gen.Config{URL: "http://x", Total: -1, BatchSize: 1, Encoder: jsonEncoder{}}, "-total"},
		{"zero batch", gen.Config{URL: "http://x", Total: 1, Encoder: jsonEncoder{}}, "-batch"},
		{"negative rate", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Rate: -1, Encoder: jsonEncoder{}}, "-rate"},
		{"nan rate", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Rate: math.NaN(), Encoder: jsonEncoder{}}, "must be finite"},
		{"inf rate", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Rate: math.Inf(1), Encoder: jsonEncoder{}}, "must be finite"},
		{"bad tenant", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Tenant: "bad tenant", Encoder: jsonEncoder{}}, "-tenant"},
		{"invalid label name", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Labels: logs.Labels{{Name: "bad name", Value: "x"}}, Encoder: jsonEncoder{}}, "invalid label name"},
		{"invalid stream label", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, StreamLabel: "9bad", Encoder: jsonEncoder{}}, "invalid label name"},
		{"bad profile", gen.Config{URL: "http://x", Total: 1, BatchSize: 1, Profile: "bogus", Encoder: jsonEncoder{}}, "-profile"},
		{"no encoder", gen.Config{URL: "http://x", Total: 1, BatchSize: 1}, "encoder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := gen.Run(t.Context(), tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Run() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

// bodyTracker counts opened and closed response bodies seen by the client.
type bodyTracker struct {
	base           http.RoundTripper
	mu             sync.Mutex
	opened, closed int
}

func (bt *bodyTracker) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := bt.base.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	bt.mu.Lock()
	bt.opened++
	bt.mu.Unlock()
	resp.Body = &trackedBody{ReadCloser: resp.Body, bt: bt}
	return resp, nil
}

type trackedBody struct {
	io.ReadCloser
	bt *bodyTracker
}

func (b *trackedBody) Close() error {
	b.bt.mu.Lock()
	b.bt.opened--
	b.bt.closed++
	b.bt.mu.Unlock()
	return b.ReadCloser.Close()
}

func (bt *bodyTracker) counts() (opened, closed int) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	return bt.opened, bt.closed
}

func TestRunClosesResponseBodies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		wantClosed int
	}{
		{"success", http.StatusNoContent, 3},
		{"failure", http.StatusBadGateway, 1}, // fails on the first non-204
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			bt := &bodyTracker{base: http.DefaultTransport}
			_ = gen.Run(t.Context(), gen.Config{
				URL: srv.URL, Total: 3, BatchSize: 1, Encoder: jsonEncoder{},
				Client: &http.Client{Transport: bt},
			})
			opened, closed := bt.counts()
			if closed != tc.wantClosed || opened != 0 {
				t.Errorf("bodies opened=%d closed=%d, want %d closed and none left open", opened, closed, tc.wantClosed)
			}
		})
	}
}

func TestRunRateCountsLines(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handle(t))
	defer srv.Close()

	// 100 lines in 10 batches at 100 lines/s: the first batch is immediate,
	// the nine gaps are paced by the batch's line count (100ms each), not by
	// the request count.
	start := time.Now()
	err := gen.Run(t.Context(), gen.Config{
		URL: srv.URL + "/loki/api/v1/push", Total: 100, BatchSize: 10, Rate: 100, Encoder: jsonEncoder{},
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(rec.all()); n != 10 {
		t.Fatalf("requests = %d, want 10", n)
	}
	if elapsed < 850*time.Millisecond {
		t.Errorf("elapsed = %v, want >= ~900ms; pacing must count lines, not requests", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Errorf("elapsed = %v, pacing runaway", elapsed)
	}
}

// TestRunCorpusGrouping pushes the corpus profile through the send loop:
// entries must be grouped into one stream per complete label identity, each
// stream's format label must match its lines, and every entry must carry
// structured metadata.
func TestRunCorpusGrouping(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handle(t))
	defer srv.Close()

	err := gen.Run(t.Context(), gen.Config{
		URL:       srv.URL + "/loki/api/v1/push",
		Streams:   []string{"a", "b"},
		Labels:    logs.Labels{{Name: "job", Value: "x"}},
		BatchSize: 20,
		Total:     60,
		Profile:   gen.ProfileCorpus,
		Encoder:   jsonEncoder{},
	})
	if err != nil {
		t.Fatal(err)
	}

	recs := rec.all()
	if len(recs) != 3 {
		t.Fatalf("requests = %d, want 3 (60 lines, batch 20)", len(recs))
	}
	sets := map[string]int{} // distinct complete label sets
	seen := map[int]string{} // line id -> stream name it was pushed under
	for _, r := range recs {
		for _, se := range r.entries {
			format := se.labels["format"]
			if format == "" || se.labels["level"] == "" || se.labels["pod"] == "" {
				t.Errorf("stream labels %v missing format/level/pod", se.labels)
			}
			if se.labels["job"] != "x" {
				t.Errorf("stream labels %v missing configured label job=x", se.labels)
			}
			name := se.labels["service_name"]
			if name != "a" && name != "b" {
				t.Errorf("stream label service_name = %q, want a or b", name)
			}
			sets[fmt.Sprint(se.labels)]++
			for i, line := range se.lines {
				id := corpusLineID(t, line)
				if _, dup := seen[id]; dup {
					t.Errorf("line id=%d received more than once", id)
				}
				seen[id] = name
				if got := corpusLineFormat(line); got != format {
					t.Errorf("line %q in format=%s stream looks like %s", line, format, got)
				}
				m := se.metas[i]
				if m == nil || m["trace_id"] == "" || m["request_id"] == "" {
					t.Errorf("corpus line %q metadata = %v, want trace_id+request_id", line, m)
				}
			}
		}
	}
	if len(sets) < 2 {
		t.Errorf("only %d distinct label set; corpus must regroup into several streams", len(sets))
	}
	if len(seen) != 60 {
		t.Fatalf("distinct corpus ids = %d, want 60", len(seen))
	}
	for id, name := range seen {
		if want := []string{"a", "b"}[id%2]; name != want {
			t.Errorf("line id=%d pushed under stream %q, want %q", id, name, want)
		}
	}
}
