package ingest_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var jsonBody = `{"streams":[{"stream":{"job":"api"},"values":[["1600000000500000001","hi"]]}]}`

func protoBody(t *testing.T) []byte {
	t.Helper()
	b, err := pushcodec.MarshalSnappy(t.Context(), &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "job", Value: "api"}),
		Entries: []logs.Entry{{Timestamp: time.Unix(1600000000, 500000001).UTC(), Line: "hi"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type call struct {
	id  tenant.ID
	req *logs.PushRequest
}

// recorder returns an AppendFunc that captures calls and replies with err.
func recorder(err error) (ingest.AppendFunc, *[]call) {
	var calls []call
	return func(ctx context.Context, id tenant.ID, req *logs.PushRequest) error {
		calls = append(calls, call{id, req})
		return err
	}, &calls
}

func newHandler(t *testing.T, o ingest.PushOptions) http.Handler {
	t.Helper()
	h, err := ingest.NewPushHandler(o)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func post(t *testing.T, h http.Handler, ct, enc string, body []byte, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	if ctx == nil {
		ctx = t.Context()
	}
	r := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader(body)).WithContext(ctx)
	if ct != "" {
		r.Header.Set("Content-Type", ct)
	}
	if enc != "" {
		r.Header.Set("Content-Encoding", enc)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func gzipBody(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestPushJSON(t *testing.T) {
	append, calls := recorder(nil)
	h := newHandler(t, ingest.PushOptions{Append: append})
	w := post(t, h, "application/json", "", []byte(jsonBody), nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
	}
	if len(*calls) != 1 {
		t.Fatalf("append called %d times, want 1", len(*calls))
	}
	c := (*calls)[0]
	if c.id != tenant.Default {
		t.Errorf("tenant = %q, want default", c.id)
	}
	if len(c.req.Streams) != 1 || len(c.req.Streams[0].Entries) != 1 {
		t.Fatalf("request = %+v", c.req)
	}
	if got := c.req.Streams[0].Entries[0].Line; got != "hi" {
		t.Errorf("line = %q", got)
	}
}

func TestPushProtoSnappy(t *testing.T) {
	// Alloy's and Promtail's loki.write send Content-Encoding: snappy with
	// the block-snappy protobuf body; Loki treats the header as describing
	// that body, not as an extra layer.
	for _, enc := range []string{"", "snappy"} {
		append, calls := recorder(nil)
		h := newHandler(t, ingest.PushOptions{Append: append})
		w := post(t, h, "application/x-protobuf", enc, protoBody(t), nil)
		if w.Code != http.StatusNoContent {
			t.Fatalf("encoding %q: code = %d, want 204; body %q", enc, w.Code, w.Body)
		}
		if len(*calls) != 1 || (*calls)[0].req.Streams[0].Entries[0].Line != "hi" {
			t.Fatalf("encoding %q: calls = %+v", enc, *calls)
		}
	}
}

func TestPushGzip(t *testing.T) {
	for _, c := range []struct {
		name string
		ct   string
		body []byte
	}{
		{"json", "application/json", []byte(jsonBody)},
		{"proto", "application/x-protobuf", protoBody(t)},
	} {
		t.Run(c.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newHandler(t, ingest.PushOptions{Append: append})
			w := post(t, h, c.ct, "gzip", gzipBody(t, c.body), nil)
			if w.Code != http.StatusNoContent {
				t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
			}
			if len(*calls) != 1 {
				t.Fatalf("append called %d times, want 1", len(*calls))
			}
		})
	}
}

func TestPushTenantFromContext(t *testing.T) {
	append, calls := recorder(nil)
	h := newHandler(t, ingest.PushOptions{Append: append})
	ctx := tenant.With(t.Context(), tenant.ID("acme"))
	post(t, h, "application/json", "", []byte(jsonBody), ctx)
	if (*calls)[0].id != "acme" {
		t.Fatalf("tenant = %q, want acme", (*calls)[0].id)
	}
}

func TestPushAppendErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"durability failure", errors.New("disk full"), http.StatusInternalServerError},
		{"unavailable", fmt.Errorf("replaying: %w", ingest.ErrUnavailable), http.StatusServiceUnavailable},
		{"unavailable sentinel", ingest.ErrUnavailable, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		append, _ := recorder(c.err)
		h := newHandler(t, ingest.PushOptions{Append: append})
		w := post(t, h, "application/json", "", []byte(jsonBody), nil)
		if w.Code != c.want {
			t.Errorf("%s: code = %d, want %d", c.name, w.Code, c.want)
		}
	}
}

func TestPushCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	append := func(ctx context.Context, _ tenant.ID, _ *logs.PushRequest) error {
		cancel()
		return ctx.Err()
	}
	h := newHandler(t, ingest.PushOptions{Append: append})
	// Must not panic nor report a client error after cancellation.
	post(t, h, "application/json", "", []byte(jsonBody), ctx)
}

func TestPushRejects(t *testing.T) {
	bigLine := strings.Repeat("x", 1024)
	cases := []struct {
		name    string
		ct, enc string
		body    []byte
		maxReq  int64
		maxLine int64
	}{
		{"bad json", "application/json", "", []byte(`{"streams":[`), 0, 0},
		{"bad proto", "application/x-protobuf", "", []byte{0xff, 0xff}, 0, 0},
		{"bad proto with snappy header", "application/x-protobuf", "snappy", []byte{0xff, 0xff}, 0, 0},
		{"uncompressed proto", "application/x-protobuf", "", []byte{0x0a, 0x00}, 0, 0},
		{"bad encoding", "application/json", "deflate", []byte(jsonBody), 0, 0},
		{"bad gzip", "application/json", "gzip", []byte("not gzip"), 0, 0},
		{"body too large", "application/json", "", bytes.Repeat([]byte("x"), 2048), 1024, 0},
		{"line too large", "application/json", "",
			[]byte(fmt.Sprintf(`{"streams":[{"stream":{},"values":[["1","%s"]]}]}`, bigLine)), 0, 512},
		{"duplicate labels proto", "application/x-protobuf", "", snappy.Encode(nil, []byte{
			0x0a, 0x0d, 0x0a, 0x0b, '{', 'a', '=', '"', '1', '"', ',', 'a', '=', '"', '2', '"', '}',
		}), 0, 0},
		{"missing content type", "", "", []byte(jsonBody), 0, 0},
		{"invalid label name json", "application/json", "",
			[]byte(`{"streams":[{"stream":{"bad-name":"x"},"values":[]}]}`), 0, 0},
	}
	for _, c := range cases {
		append, calls := recorder(nil)
		h := newHandler(t, ingest.PushOptions{
			Append:          append,
			MaxRequestBytes: c.maxReq,
			MaxLineBytes:    c.maxLine,
		})
		w := post(t, h, c.ct, c.enc, c.body, nil)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: code = %d, want 400", c.name, w.Code)
		}
		if len(*calls) != 0 {
			t.Errorf("%s: append called on a rejected push", c.name)
		}
	}
}

func TestPushGzipExpansionLimit(t *testing.T) {
	// Compresses below the wire limit but expands past it.
	append, calls := recorder(nil)
	h := newHandler(t, ingest.PushOptions{Append: append, MaxRequestBytes: 4096})
	body := gzipBody(t, bytes.Repeat([]byte("x"), 1<<20))
	if int64(len(body)) >= 4096 {
		t.Skip("fixture did not compress enough to exercise the expansion bound")
	}
	w := post(t, h, "application/json", "gzip", body, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
	if len(*calls) != 0 {
		t.Fatal("append called on a rejected push")
	}
}

func TestPushSnappyExpansionLimit(t *testing.T) {
	// The snappy block declares a decoded size past the limit: the handler
	// must reject before allocating it.
	append, calls := recorder(nil)
	h := newHandler(t, ingest.PushOptions{Append: append, MaxRequestBytes: 4096})
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(),
		Entries: []logs.Entry{{Timestamp: time.Now(), Line: strings.Repeat("y", 1<<16)}},
	}}}
	b, err := pushcodec.MarshalSnappy(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := snappy.DecodedLen(b); err != nil || n <= 4096 {
		t.Skip("fixture too small to exercise the expansion bound")
	}
	for _, enc := range []string{"", "snappy"} {
		w := post(t, h, "application/x-protobuf", enc, b, nil)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("encoding %q: code = %d, want 400", enc, w.Code)
		}
	}
	if len(*calls) != 0 {
		t.Fatal("append called on a rejected push")
	}
}

func TestNewPushHandlerValidation(t *testing.T) {
	if _, err := ingest.NewPushHandler(ingest.PushOptions{}); err == nil {
		t.Error("nil Append accepted")
	}
	append, _ := recorder(nil)
	if _, err := ingest.NewPushHandler(ingest.PushOptions{Append: append, MaxLineBytes: -1}); err == nil {
		t.Error("negative limit accepted")
	}
}

func TestPushTransform(t *testing.T) {
	long := strings.Repeat("x", 64)
	cases := []struct {
		name      string
		transform ingest.TransformFunc
		code      int
		appended  int    // Append calls
		line      string // first appended line, when appended
	}{
		{
			name: "rewrite",
			transform: func(_ context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
				if id != tenant.Default {
					return nil, fmt.Errorf("tenant %q", id)
				}
				return &logs.PushRequest{Streams: []logs.Stream{{
					Labels:  req.Streams[0].Labels,
					Entries: []logs.Entry{{Timestamp: req.Streams[0].Entries[0].Timestamp, Line: "HI"}},
				}}}, nil
			},
			code: http.StatusNoContent, appended: 1, line: "HI",
		},
		{
			name: "drop everything",
			transform: func(context.Context, tenant.ID, *logs.PushRequest) (*logs.PushRequest, error) {
				return &logs.PushRequest{Streams: []logs.Stream{{Labels: logs.New(logs.Label{Name: "job", Value: "api"})}}}, nil
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
			code: http.StatusBadRequest,
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
			code: http.StatusBadRequest,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			append, calls := recorder(nil)
			h := newHandler(t, ingest.PushOptions{Append: append, Transform: tc.transform, MaxLineBytes: 32})
			w := post(t, h, "application/json", "", []byte(jsonBody), nil)
			if w.Code != tc.code {
				t.Fatalf("code = %d, want %d; body %q", w.Code, tc.code, w.Body)
			}
			if len(*calls) != tc.appended {
				t.Fatalf("append called %d times, want %d", len(*calls), tc.appended)
			}
			if tc.appended > 0 {
				if got := (*calls)[0].req.Streams[0].Entries[0].Line; got != tc.line {
					t.Errorf("appended line = %q, want %q", got, tc.line)
				}
			}
		})
	}
}
