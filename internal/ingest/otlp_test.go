package ingest_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fixtureTime is the timestamp of every default fixture entry, matching the
// push fixtures in push_test.go so differential tests compare like requests.
var fixtureTime = time.Unix(0, 1600000000500000001).UTC()

// otlpLogs builds one resource (service.name=api) with one record: line and
// timestamp ns.
func otlpLogs(line string, ns uint64) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.Body().SetStr(line)
	rec.SetTimestamp(pcommon.Timestamp(ns))
	return ld
}

// otlpProto marshals ld as an OTLP protobuf export request.
func otlpProto(t *testing.T, ld plog.Logs) []byte {
	t.Helper()
	b, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// otlpJSON marshals ld as an OTLP JSON export request.
func otlpJSON(t *testing.T, ld plog.Logs) []byte {
	t.Helper()
	b, err := plogotlp.NewExportRequestFromLogs(ld).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// otlpFixture is the default differential body: one "hi" record whose decoded
// entry matches the push fixtures' entry.
func otlpFixture(t *testing.T) []byte {
	t.Helper()
	return otlpProto(t, otlpLogs("hi", 1600000000500000001))
}

func newOTLPHandler(t *testing.T, o ingest.PushOptions) http.Handler {
	t.Helper()
	h, err := ingest.NewOTLPHandler(o)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// otlpBodySized marshals a valid single-record protobuf export request of
// exactly size bytes by padding the record body, iterating past varint length
// boundaries.
func otlpBodySized(t *testing.T, size int) []byte {
	t.Helper()
	lineLen := 8
	body := otlpProto(t, otlpLogs(strings.Repeat("x", lineLen), 1))
	for range 4 {
		if len(body) == size {
			return body
		}
		lineLen += size - len(body)
		body = otlpProto(t, otlpLogs(strings.Repeat("x", lineLen), 1))
	}
	t.Fatalf("could not build a %d-byte export request (got %d)", size, len(body))
	return nil
}

func TestOTLPFormats(t *testing.T) {
	for _, c := range []struct {
		name string
		ct   string
		enc  string
		body func(t *testing.T) []byte
	}{
		{"protobuf", "application/x-protobuf", "", func(t *testing.T) []byte { return otlpFixture(t) }},
		{"protobuf gzip", "application/x-protobuf", "gzip", func(t *testing.T) []byte {
			return gzipBody(t, otlpFixture(t))
		}},
		{"json", "application/json", "", func(t *testing.T) []byte {
			return otlpJSON(t, otlpLogs("hi", 1600000000500000001))
		}},
		{"json gzip", "application/json", "gzip", func(t *testing.T) []byte {
			return gzipBody(t, otlpJSON(t, otlpLogs("hi", 1600000000500000001)))
		}},
		{"json with charset", "application/json; charset=utf-8", "", func(t *testing.T) []byte {
			return otlpJSON(t, otlpLogs("hi", 1600000000500000001))
		}},
		{"identity encoding", "application/x-protobuf", "identity", func(t *testing.T) []byte {
			return otlpFixture(t)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{Append: append})
			w := post(t, h, c.ct, c.enc, c.body(t), nil)
			if w.Code != http.StatusNoContent {
				t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
			}
			if len(*calls) != 1 {
				t.Fatalf("append called %d times, want 1", len(*calls))
			}
			got := (*calls)[0]
			if got.id != tenant.Default {
				t.Errorf("tenant = %q, want default", got.id)
			}
			s := got.req.Streams[0]
			if v := s.Labels.Map()["service_name"]; v != "api" {
				t.Errorf("service_name = %q, want api", v)
			}
			e := s.Entries[0]
			if e.Line != "hi" {
				t.Errorf("line = %q, want hi", e.Line)
			}
			if !e.Timestamp.Equal(fixtureTime) {
				t.Errorf("timestamp = %s, want %s", e.Timestamp, fixtureTime)
			}
		})
	}
}

// TestOTLPEncodings: the OTLP endpoint carries no snappy (unlike push) and the
// approved formats promise gzip only; everything else is a client error.
func TestOTLPEncodings(t *testing.T) {
	for _, enc := range []string{"snappy", "zstd", "lz4", "deflate"} {
		t.Run(enc, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{Append: append})
			w := post(t, h, "application/x-protobuf", enc, otlpFixture(t), nil)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("encoding %q: code = %d, want 400", enc, w.Code)
			}
			if !strings.Contains(w.Body.String(), "unsupported content encoding") {
				t.Errorf("encoding %q: body = %q, want unsupported content encoding", enc, w.Body)
			}
			if len(*calls) != 0 {
				t.Errorf("encoding %q: append called on a rejected push", enc)
			}
		})
	}
}

// TestOTLPEmptyNoAppend: an export request that decodes to no entries is
// acknowledged with 204 and never reaches Append or the WAL. The push
// endpoint keeps its frozen behavior for an empty request (Append decides),
// documenting the deliberate difference between the two endpoints.
func TestOTLPEmptyNoAppend(t *testing.T) {
	emptyJSON := otlpJSON(t, plog.NewLogs())
	resourceJSON := []byte(`{"resourceLogs":[{"resource":{"attributes":[` +
		`{"key":"service.name","value":{"stringValue":"api"}}]},` +
		`"scopeLogs":[{"logRecords":[]}]}]}`)
	scopeJSON := []byte(`{"resourceLogs":[{"resource":{"attributes":[` +
		`{"key":"service.name","value":{"stringValue":"api"}}]},` +
		`"scopeLogs":[{"scope":{}}]}]}`)
	for _, c := range []struct {
		name string
		ct   string
		body []byte
	}{
		{"empty json object", "application/json", []byte(`{}`)},
		{"empty json export", "application/json", emptyJSON},
		{"empty protobuf export", "application/x-protobuf", otlpProto(t, plog.NewLogs())},
		{"resource without records", "application/json", resourceJSON},
		{"scope without records", "application/json", scopeJSON},
	} {
		t.Run(c.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{Append: append})
			w := post(t, h, c.ct, "", c.body, nil)
			if w.Code != http.StatusNoContent {
				t.Fatalf("%s: code = %d, want 204; body %q", c.name, w.Code, w.Body)
			}
			if len(*calls) != 0 {
				t.Fatalf("%s: append called %d times, want 0 (no WAL admission)", c.name, len(*calls))
			}
		})
	}

	t.Run("push contrast appends an empty request", func(t *testing.T) {
		// Frozen push behavior: without a transform the empty request is
		// handed to Append. Parkana's OTLP endpoint skips it instead.
		append, calls := recorder(nil)
		h := newHandler(t, ingest.PushOptions{Append: append})
		empty := []byte(`{"streams":[{"stream":{"job":"api"},"values":[]}]}`)
		w := post(t, h, "application/json", "", empty, nil)
		if w.Code != http.StatusNoContent {
			t.Fatalf("code = %d, want 204", w.Code)
		}
		if len(*calls) != 1 {
			t.Fatalf("push append called %d times, want 1", len(*calls))
		}
	})
}

// TestOTLPEmptyTransformExactlyOnce: a decoded empty request still reaches
// Transform exactly once, and the transform's outcome — injected entries, a
// rejection, a drop or a server fault — is honored before the empty shortcut.
func TestOTLPEmptyTransformExactlyOnce(t *testing.T) {
	injected := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "service_name", Value: "api"}),
		Entries: []logs.Entry{{Timestamp: fixtureTime, Line: "injected"}},
	}}}
	cases := []struct {
		name      string
		transform ingest.TransformFunc
		code      int
		appends   int
	}{
		{
			name: "identity drop",
			transform: func(_ context.Context, _ tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				return req, nil
			},
			code: http.StatusNoContent,
		},
		{
			name: "inject entries",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return injected, nil
			},
			code:    http.StatusNoContent,
			appends: 1,
		},
		{
			name: "reject",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, fmt.Errorf("pipeline: empty: %w", ingest.ErrRejected)
			},
			code: http.StatusBadRequest,
		},
		{
			name: "nil drop",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, nil
			},
			code: http.StatusNoContent,
		},
		{
			name: "server fault",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, errors.New("pipeline: registry closed")
			},
			code: http.StatusInternalServerError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			tf := func(ctx context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				calls++
				return tc.transform(ctx, id, req)
			}
			append, appends := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{Append: append, Transform: tf})
			w := post(t, h, "application/json", "", []byte(`{}`), nil)
			if w.Code != tc.code {
				t.Fatalf("code = %d, want %d; body %q", w.Code, tc.code, w.Body)
			}
			if calls != 1 {
				t.Fatalf("transform called %d times, want exactly 1", calls)
			}
			if len(*appends) != tc.appends {
				t.Fatalf("append called %d times, want %d", len(*appends), tc.appends)
			}
			if tc.appends > 0 && (*appends)[0].req != injected {
				t.Errorf("appended request = %+v, want the injected one", (*appends)[0].req)
			}
		})
	}
}

// TestOTLPTransformOutcomesMatchPush drives the push endpoint (JSON body) and
// the OTLP endpoint (protobuf body) with fixtures decoding to the same entry
// and asserts identical outcomes for every Transform and Append result.
func TestOTLPTransformOutcomesMatchPush(t *testing.T) {
	long := strings.Repeat("x", 64)
	cases := []struct {
		name      string
		appendErr error
		maxLine   int64
		transform ingest.TransformFunc
		code      int
		calls     int // Append attempts
		line      string
		wantBody  string // asserted on both endpoints' response bodies
	}{
		{
			name: "identity",
			transform: func(_ context.Context, _ tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				return req, nil
			},
			code: http.StatusNoContent, calls: 1, line: "hi",
		},
		{
			name: "rewrite",
			transform: func(_ context.Context, _ tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				return &logs.PushRequest{Streams: []logs.Stream{{
					Labels:  req.Streams[0].Labels,
					Entries: []logs.Entry{{Timestamp: req.Streams[0].Entries[0].Timestamp, Line: "HI"}},
				}}}, nil
			},
			code: http.StatusNoContent, calls: 1, line: "HI",
		},
		{
			name: "drop everything",
			transform: func(_ context.Context, _ tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				return &logs.PushRequest{Streams: []logs.Stream{{Labels: req.Streams[0].Labels}}}, nil
			},
			code: http.StatusNoContent,
		},
		{
			name: "nil request",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, nil
			},
			code: http.StatusNoContent,
		},
		{
			name: "rejected",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, fmt.Errorf("pipeline: bad json: %w", ingest.ErrRejected)
			},
			code:     http.StatusBadRequest,
			wantBody: "pipeline: bad json",
		},
		{
			name: "internal error",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return nil, errors.New("pipeline: registry closed")
			},
			code: http.StatusInternalServerError,
		},
		{
			name: "output revalidated",
			transform: func(_ context.Context, _ tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				return &logs.PushRequest{Streams: []logs.Stream{{
					Labels:  req.Streams[0].Labels,
					Entries: []logs.Entry{{Timestamp: req.Streams[0].Entries[0].Timestamp, Line: long}},
				}}}, nil
			},
			maxLine:  32,
			code:     http.StatusBadRequest,
			wantBody: "transformed push is invalid",
		},
		{
			name:      "append unavailable",
			appendErr: ingest.ErrUnavailable,
			code:      http.StatusServiceUnavailable, calls: 1,
		},
		{
			name:      "append unavailable wrapped",
			appendErr: fmt.Errorf("replaying: %w", ingest.ErrUnavailable),
			code:      http.StatusServiceUnavailable, calls: 1,
		},
		{
			name:      "append fault",
			appendErr: errors.New("disk full"),
			code:      http.StatusInternalServerError, calls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Each endpoint gets its own counting wrapper over the shared
			// transform so both are proven to run it exactly once. Cases
			// without a transform (append-fault mapping) run an identity
			// wrapper, which leaves a valid request unchanged.
			wrap := func(tf ingest.TransformFunc) (ingest.TransformFunc, *int) {
				n := 0
				return func(ctx context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
					n++
					if tf == nil {
						return req, nil
					}
					return tf(ctx, id, req)
				}, &n
			}
			pushTf, pushCount := wrap(tc.transform)
			pushAppend, pushCalls := recorder(tc.appendErr)
			pushH := newHandler(t, ingest.PushOptions{
				Append: pushAppend, Transform: pushTf, MaxLineBytes: tc.maxLine,
			})
			pw := post(t, pushH, "application/json", "", []byte(jsonBody), nil)

			otlpTf, otlpCount := wrap(tc.transform)
			otlpAppend, otlpCalls := recorder(tc.appendErr)
			otlpH := newOTLPHandler(t, ingest.PushOptions{
				Append: otlpAppend, Transform: otlpTf, MaxLineBytes: tc.maxLine,
			})
			ow := post(t, otlpH, "application/x-protobuf", "", otlpFixture(t), nil)

			if *pushCount != 1 || *otlpCount != 1 {
				t.Errorf("transform calls = push %d, otlp %d; want exactly 1 each", *pushCount, *otlpCount)
			}
			if pw.Code != tc.code {
				t.Errorf("push code = %d, want %d; body %q", pw.Code, tc.code, pw.Body)
			}
			if ow.Code != tc.code {
				t.Errorf("otlp code = %d, want %d; body %q", ow.Code, tc.code, ow.Body)
			}
			if len(*pushCalls) != tc.calls || len(*otlpCalls) != tc.calls {
				t.Errorf("append calls = push %d, otlp %d; want %d", len(*pushCalls), len(*otlpCalls), tc.calls)
			}
			if tc.line != "" {
				pe := (*pushCalls)[0].req.Streams[0].Entries[0]
				oe := (*otlpCalls)[0].req.Streams[0].Entries[0]
				if pe.Line != tc.line || oe.Line != tc.line {
					t.Errorf("appended line = push %q, otlp %q; want %q", pe.Line, oe.Line, tc.line)
				}
				if !oe.Timestamp.Equal(pe.Timestamp) || !pe.Timestamp.Equal(fixtureTime) {
					t.Errorf("appended timestamp = push %s, otlp %s; want %s", pe.Timestamp, oe.Timestamp, fixtureTime)
				}
			}
			if tc.wantBody != "" {
				if msg := ow.Body.String(); !strings.Contains(msg, tc.wantBody) {
					t.Errorf("otlp body = %q, want it to contain %q", msg, tc.wantBody)
				}
				if msg := pw.Body.String(); !strings.Contains(msg, tc.wantBody) {
					t.Errorf("push body = %q, want it to contain %q", msg, tc.wantBody)
				}
			}
		})
	}
}

// TestOTLPTenantAndContext: the request's tenant reaches Transform and
// Append, and Transform runs on the request's context.
func TestOTLPTenantAndContext(t *testing.T) {
	append, calls := recorder(nil)
	seenTenant := make(chan tenant.ID, 1)
	tf := func(ctx context.Context, id tenant.ID, _ *logs.PushRequest) (*logs.PushRequest, error) {
		// The transform must run on the request's context and tenant.
		if tenant.From(ctx) != id {
			t.Errorf("transform ctx tenant = %q, param = %q", tenant.From(ctx), id)
		}
		seenTenant <- id
		return nil, nil
	}
	h := newOTLPHandler(t, ingest.PushOptions{Append: append, Transform: tf})
	ctx := tenant.With(t.Context(), tenant.ID("acme"))
	w := post(t, h, "application/x-protobuf", "", otlpFixture(t), ctx)
	if w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
	}
	if got := <-seenTenant; got != "acme" {
		t.Errorf("transform tenant = %q, want acme", got)
	}
	if len(*calls) != 0 {
		t.Fatalf("append called %d times, want 0 (transform dropped all)", len(*calls))
	}

	// Without a transform the tenant reaches Append directly.
	append2, calls2 := recorder(nil)
	h2 := newOTLPHandler(t, ingest.PushOptions{Append: append2})
	post(t, h2, "application/x-protobuf", "", otlpFixture(t), tenant.With(t.Context(), tenant.ID("acme")))
	if (*calls2)[0].id != "acme" {
		t.Fatalf("append tenant = %q, want acme", (*calls2)[0].id)
	}
}

// TestOTLPCancelSilentMatchesPush: when the client context is canceled during
// Transform or Append, both endpoints answer nothing instead of reporting a
// client or server error.
func TestOTLPCancelSilentMatchesPush(t *testing.T) {
	t.Run("transform cancels client", func(t *testing.T) {
		// Each endpoint gets its own valid body: a push JSON body is not an
		// OTLP body, and pdata's tolerance of unknown fields must never let
		// this pass on decoder leniency. hits==1 proves the transform ran.
		run := func(h func(ingest.PushOptions) (http.Handler, error), body []byte, ct string) (*httptest.ResponseRecorder, *int) {
			ctx, cancel := context.WithCancel(t.Context())
			hits := 0
			tf := func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				hits++
				cancel()
				return nil, ctx.Err()
			}
			append, calls := recorder(nil)
			handler, err := h(ingest.PushOptions{Append: append, Transform: tf})
			if err != nil {
				t.Fatal(err)
			}
			w := post(t, handler, ct, "", body, ctx)
			if len(*calls) != 0 {
				t.Errorf("append called %d times, want 0", len(*calls))
			}
			return w, &hits
		}
		pw, phits := run(func(o ingest.PushOptions) (http.Handler, error) { return ingest.NewPushHandler(o) }, []byte(jsonBody), "application/json")
		ow, ohits := run(func(o ingest.PushOptions) (http.Handler, error) { return ingest.NewOTLPHandler(o) }, otlpFixture(t), "application/x-protobuf")
		if *phits != 1 || *ohits != 1 {
			t.Errorf("transform hits = push %d, otlp %d; want 1 each", *phits, *ohits)
		}
		// 200 is the recorder's untouched default: nothing was written.
		if pw.Code != 200 || ow.Code != 200 {
			t.Errorf("code = push %d, otlp %d; want nothing written (200)", pw.Code, ow.Code)
		}
		if pw.Body.Len() != 0 || ow.Body.Len() != 0 {
			t.Errorf("body written after cancellation: push %q, otlp %q", pw.Body, ow.Body)
		}
	})
	t.Run("append cancels client", func(t *testing.T) {
		run := func(h func(ingest.PushOptions) (http.Handler, error), body []byte, ct string) *httptest.ResponseRecorder {
			ctx, cancel := context.WithCancel(t.Context())
			append := func(ctx context.Context, _ tenant.ID, _ *logs.PushRequest) error {
				cancel()
				return ctx.Err()
			}
			handler, err := h(ingest.PushOptions{Append: append})
			if err != nil {
				t.Fatal(err)
			}
			return post(t, handler, ct, "", body, ctx)
		}
		pw := run(func(o ingest.PushOptions) (http.Handler, error) { return ingest.NewPushHandler(o) }, []byte(jsonBody), "application/json")
		ow := run(func(o ingest.PushOptions) (http.Handler, error) { return ingest.NewOTLPHandler(o) }, otlpFixture(t), "application/x-protobuf")
		if pw.Code != 200 || ow.Code != 200 {
			t.Errorf("code = push %d, otlp %d; want nothing written (200)", pw.Code, ow.Code)
		}
	})
}

func TestOTLPRejects(t *testing.T) {
	valid := otlpFixture(t)
	bigLine := otlpProto(t, otlpLogs(strings.Repeat("x", 1024), 1))
	cases := []struct {
		name    string
		ct, enc string
		body    []byte
		maxReq  int64
		maxLine int64
	}{
		{"bad json", "application/json", "", []byte(`{"resourceLogs":[`), 0, 0},
		{"bad proto", "application/x-protobuf", "", []byte{0xff, 0xff}, 0, 0},
		{"truncated proto", "application/x-protobuf", "", valid[:len(valid)/2], 0, 0},
		{"unsupported content type", "text/plain", "", valid, 0, 0},
		{"missing content type", "", "", valid, 0, 0},
		{"malformed content type", "not a media type", "", valid, 0, 0},
		{"bad gzip", "application/x-protobuf", "gzip", []byte("not gzip"), 0, 0},
		{"line too large", "application/x-protobuf", "", bigLine, 0, 512},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{
				Append:          append,
				MaxRequestBytes: c.maxReq,
				MaxLineBytes:    c.maxLine,
			})
			w := post(t, h, c.ct, c.enc, c.body, nil)
			if w.Code != http.StatusBadRequest {
				t.Errorf("%s: code = %d, want 400; body %q", c.name, w.Code, w.Body)
			}
			if len(*calls) != 0 {
				t.Errorf("%s: append called on a rejected push", c.name)
			}
		})
	}
}

// TestOTLPWireLimitBoundaries: the wire limit rejects above and accepts at
// exactly the bound, for the raw and the gzip-expanded body.
func TestOTLPWireLimitBoundaries(t *testing.T) {
	const limit = 4096
	atLimit := otlpBodySized(t, limit)
	if len(atLimit) != limit {
		t.Fatalf("fixture is %d bytes, want %d", len(atLimit), limit)
	}
	overLimit := otlpBodySized(t, limit+1)

	over, callsOver := recorder(nil)
	h := newOTLPHandler(t, ingest.PushOptions{Append: over, MaxRequestBytes: limit})
	if w := post(t, h, "application/x-protobuf", "", overLimit, nil); w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 for %d bytes", w.Code, len(overLimit))
	}
	if len(*callsOver) != 0 {
		t.Error("append called on an oversized push")
	}

	at, callsAt := recorder(nil)
	h = newOTLPHandler(t, ingest.PushOptions{Append: at, MaxRequestBytes: limit})
	if w := post(t, h, "application/x-protobuf", "", atLimit, nil); w.Code != http.StatusNoContent {
		t.Errorf("code = %d, want 204 at exactly the limit", w.Code)
	}
	if len(*callsAt) != 1 {
		t.Errorf("append called %d times, want 1", len(*callsAt))
	}

	// Compressed below the wire limit but expanding past it.
	gz, gcalls := recorder(nil)
	h = newOTLPHandler(t, ingest.PushOptions{Append: gz, MaxRequestBytes: limit})
	big := otlpProto(t, otlpLogs(strings.Repeat("x", 1<<20), 1))
	body := gzipBody(t, big)
	if int64(len(body)) >= limit {
		t.Skip("fixture did not compress enough to exercise the expansion bound")
	}
	if w := post(t, h, "application/x-protobuf", "gzip", body, nil); w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 for an over-limit expanded body", w.Code)
	}
	if len(*gcalls) != 0 {
		t.Error("append called on an oversized expanded push")
	}

	// The expanded body may fill the limit exactly: atLimit compresses far
	// below the wire limit and expands to exactly limit bytes.
	gzAt, gzAtCalls := recorder(nil)
	h = newOTLPHandler(t, ingest.PushOptions{Append: gzAt, MaxRequestBytes: limit})
	atGz := gzipBody(t, atLimit)
	if int64(len(atGz)) >= limit {
		t.Skip("fixture did not compress enough to exercise the expansion bound")
	}
	if w := post(t, h, "application/x-protobuf", "gzip", atGz, nil); w.Code != http.StatusNoContent {
		t.Errorf("code = %d, want 204 for a body expanding to exactly the limit", w.Code)
	}
	if len(*gzAtCalls) != 1 {
		t.Errorf("append called %d times, want 1", len(*gzAtCalls))
	}

	// One byte past the expanded limit.
	gzOver, gzOverCalls := recorder(nil)
	h = newOTLPHandler(t, ingest.PushOptions{Append: gzOver, MaxRequestBytes: limit})
	overGz := gzipBody(t, overLimit)
	if int64(len(overGz)) >= limit {
		t.Skip("fixture did not compress enough to exercise the expansion bound")
	}
	if w := post(t, h, "application/x-protobuf", "gzip", overGz, nil); w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 for a body expanding past the limit", w.Code)
	}
	if len(*gzOverCalls) != 0 {
		t.Error("append called on an over-limit expanded push")
	}
}

// TestOTLPCorruptGzip: a valid gzip header followed by a corrupt deflate
// member or a missing checksum footer fails inside the bounded read — after
// reader creation — and never reaches Append.
func TestOTLPCorruptGzip(t *testing.T) {
	valid := otlpFixture(t)
	corrupt := gzipBody(t, valid)
	corrupt[10+(len(corrupt)-10)/2] ^= 0xff // flip a bit inside the deflate member
	truncated := gzipBody(t, valid)
	truncated = truncated[:len(truncated)-8] // drop the crc32 and isize footer
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"corrupt deflate member", corrupt},
		{"missing checksum footer", truncated},
	} {
		t.Run(c.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newOTLPHandler(t, ingest.PushOptions{Append: append})
			w := post(t, h, "application/x-protobuf", "gzip", c.body, nil)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s: code = %d, want 400; body %q", c.name, w.Code, w.Body)
			}
			if !strings.Contains(w.Body.String(), "reading request body") {
				t.Errorf("%s: body = %q, want a read failure, not a header failure", c.name, w.Body)
			}
			if len(*calls) != 0 {
				t.Errorf("%s: append called on an unreadable push", c.name)
			}
		})
	}
}

// TestOTLPLineLimitBoundaries: a line of exactly MaxLineBytes is accepted and
// one byte more is rejected.
func TestOTLPLineLimitBoundaries(t *testing.T) {
	const maxLine = 300
	append, calls := recorder(nil)
	h := newOTLPHandler(t, ingest.PushOptions{Append: append, MaxLineBytes: maxLine})

	at := otlpProto(t, otlpLogs(strings.Repeat("x", maxLine), 1))
	if w := post(t, h, "application/x-protobuf", "", at, nil); w.Code != http.StatusNoContent {
		t.Errorf("code = %d, want 204 at exactly the line limit", w.Code)
	}
	if len(*calls) != 1 {
		t.Fatalf("append called %d times, want 1", len(*calls))
	}

	over := otlpProto(t, otlpLogs(strings.Repeat("x", maxLine+1), 1))
	overAppend, overCalls := recorder(nil)
	h2 := newOTLPHandler(t, ingest.PushOptions{Append: overAppend, MaxLineBytes: maxLine})
	if w := post(t, h2, "application/x-protobuf", "", over, nil); w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 past the line limit", w.Code)
	}
	if len(*overCalls) != 0 {
		t.Error("append called on a line-limit rejection")
	}
}

func TestOTLPBodyReadError(t *testing.T) {
	append, calls := recorder(nil)
	h := newOTLPHandler(t, ingest.PushOptions{Append: append})
	r := httptest.NewRequest(http.MethodPost, "/v1/logs", iotest.ErrReader(errors.New("wire cut")))
	r.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400; body %q", w.Code, w.Body)
	}
	if len(*calls) != 0 {
		t.Error("append called on an unreadable push")
	}
}

// TestOTLPCanceledContextDecode: a context canceled before decoding makes the
// codec return the context error, answered as a client-visible 400 like the
// push endpoint's decode errors.
func TestOTLPCanceledContextDecode(t *testing.T) {
	append, calls := recorder(nil)
	h := newOTLPHandler(t, ingest.PushOptions{Append: append})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := post(t, h, "application/x-protobuf", "", otlpFixture(t), ctx)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
	if len(*calls) != 0 {
		t.Error("append called on a canceled push")
	}
}

func TestNewOTLPHandlerValidation(t *testing.T) {
	if _, err := ingest.NewOTLPHandler(ingest.PushOptions{}); err == nil {
		t.Error("nil Append accepted")
	}
	append, _ := recorder(nil)
	if _, err := ingest.NewOTLPHandler(ingest.PushOptions{Append: append, MaxLineBytes: -1}); err == nil {
		t.Error("negative line limit accepted")
	}
	if _, err := ingest.NewOTLPHandler(ingest.PushOptions{Append: append, MaxRequestBytes: -1}); err == nil {
		t.Error("negative request limit accepted")
	}
}
