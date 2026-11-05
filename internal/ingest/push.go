// Package ingest implements the ingester role: the Loki push endpoint, the
// WAL/memtable append path and the flush/upload loops to local Parquet and
// object storage.
package ingest

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/golang/snappy"

	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Default push limits, matching the -ingest.max-request-size and
// -ingest.max-line-size defaults.
const (
	DefaultMaxRequestBytes = 16 << 20  // 16 MiB
	DefaultMaxLineBytes    = 256 << 10 // 256 KiB
)

// ErrUnavailable is returned by an AppendFunc while the ingester is replaying
// its WAL or shutting down; the handler answers 503.
var ErrUnavailable = errors.New("ingest: not accepting pushes")

// AppendFunc records one fully validated push and returns at the configured
// acknowledgment point: WAL fsync plus memtable admission in always mode, or
// WAL write plus memtable admission in interval mode. Durable publication
// still waits for fsync. The handler answers 204 only on a nil return. Wrap
// ErrUnavailable for a 503; any other error maps to 500.
type AppendFunc func(ctx context.Context, id tenant.ID, req *logs.PushRequest) error

// ErrRejected marks a TransformFunc error caused by the pushed data; the
// handler answers 400 with the error text. Any other transform error is a
// server fault and answers 500.
var ErrRejected = errors.New("ingest: push rejected")

// TransformFunc rewrites a validated push before Append makes it durable
// (ingest pipelines, PLAN.md §6). It may drop entries or whole streams and
// change labels, metadata and lines; it must not modify req's backing arrays
// in place unless it returns req itself. The handler validates the returned
// request again, so a pipeline cannot store what a client could not push.
// A nil request or one without entries is acknowledged with 204 and nothing
// is appended. Wrap ErrRejected for a 400.
type TransformFunc func(ctx context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error)

// PushOptions configures NewPushHandler.
type PushOptions struct {
	// MaxRequestBytes bounds the received body and every expanded
	// representation (gzip, snappy). Zero uses DefaultMaxRequestBytes.
	MaxRequestBytes int64
	// MaxLineBytes bounds one log line. Zero uses DefaultMaxLineBytes.
	MaxLineBytes int64
	// Append records the decoded push for the request's tenant. Required.
	Append AppendFunc
	// Transform, when set, runs after validation and before Append.
	Transform TransformFunc
}

// NewPushHandler returns a handler for lokiapi.Push. It decodes protobuf
// (snappy block format) and JSON bodies, optionally gzip-compressed,
// validates the whole request, calls Append and answers 204 only after it
// returns successfully.
func NewPushHandler(o PushOptions) (http.Handler, error) {
	if o.Append == nil {
		return nil, errors.New("ingest: PushOptions.Append is required")
	}
	if o.MaxRequestBytes < 0 || o.MaxLineBytes < 0 {
		return nil, fmt.Errorf("ingest: negative push limit (%d, %d)", o.MaxRequestBytes, o.MaxLineBytes)
	}
	o.MaxRequestBytes = cmp.Or(o.MaxRequestBytes, DefaultMaxRequestBytes)
	o.MaxLineBytes = cmp.Or(o.MaxLineBytes, DefaultMaxLineBytes)
	return &pushHandler{o: o}, nil
}

type pushHandler struct{ o PushOptions }

func (h *pushHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := h.decode(r)
	if err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.validate(req); err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()
	id := tenant.From(ctx)
	if h.o.Transform != nil {
		req, err = h.o.Transform(ctx, id, req)
		switch {
		case err != nil && ctx.Err() != nil:
			return // the client is gone; nothing to answer
		case errors.Is(err, ErrRejected):
			lokiapi.Error(w, http.StatusBadRequest, err.Error())
			return
		case err != nil:
			lokiapi.Error(w, http.StatusInternalServerError, "ingest: push transform failed")
			return
		case req == nil || req.Entries() == 0:
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := h.validate(req); err != nil {
			lokiapi.Error(w, http.StatusBadRequest, "ingest: transformed push is invalid: "+err.Error())
			return
		}
	}
	if err := h.o.Append(ctx, id, req); err != nil {
		if ctx.Err() != nil {
			return // the client is gone; nothing to answer
		}
		if errors.Is(err, ErrUnavailable) {
			lokiapi.Error(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		lokiapi.Error(w, http.StatusInternalServerError, "ingest: failed to persist push")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// decode reads the bounded body, expands gzip, and decodes per Content-Type,
// matching Loki's parsePushRequestBody (pkg/loghttp/push): "application/json"
// decodes JSON; every other parseable type (typically
// "application/x-protobuf") is a snappy-block-compressed logproto.PushRequest.
// A missing or unparseable Content-Type is a client error, as in Loki.
func (h *pushHandler) decode(r *http.Request) (*logs.PushRequest, error) {
	limit := h.o.MaxRequestBytes
	raw, err := readBounded(http.MaxBytesReader(nil, r.Body, limit), limit)
	if err != nil {
		return nil, err
	}
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity", "snappy":
		// Snappy is block compression decoded below for protobuf bodies.
	case "gzip":
		gz, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("ingest: gzip: %v", err)
		}
		defer gz.Close()
		raw, err = readBounded(gz, limit)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("ingest: unsupported content encoding %q", enc)
	}
	ct := r.Header.Get("Content-Type")
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return nil, fmt.Errorf("ingest: unsupported content type %q", ct)
	}
	if mt == "application/json" {
		return pushcodec.DecodeJSON(r.Context(), raw)
	}
	n, err := snappy.DecodedLen(raw)
	if err != nil {
		return nil, fmt.Errorf("ingest: malformed snappy data: %v", err)
	}
	if int64(n) > limit {
		return nil, errTooLarge
	}
	raw, err = snappy.Decode(make([]byte, n), raw)
	if err != nil {
		return nil, fmt.Errorf("ingest: malformed snappy data: %v", err)
	}
	return pushcodec.DecodeProto(r.Context(), raw)
}

var errTooLarge = errors.New("ingest: request body exceeds the limit")

// readBounded reads all of r and fails once the data exceeds max bytes.
func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("ingest: reading request body: %v", err)
	}
	if int64(len(b)) > max {
		return nil, errTooLarge
	}
	return b, nil
}

// validate checks every stream's labels and metadata and applies the
// per-line limit before the request is handed to Append.
func (h *pushHandler) validate(req *logs.PushRequest) error {
	for si := range req.Streams {
		s := &req.Streams[si]
		if err := s.Labels.Validate(); err != nil {
			return err
		}
		for ei := range s.Entries {
			e := &s.Entries[ei]
			if int64(len(e.Line)) > h.o.MaxLineBytes {
				return fmt.Errorf("ingest: line exceeds %d bytes", h.o.MaxLineBytes)
			}
			if err := e.Metadata.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}
