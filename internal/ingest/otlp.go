package ingest

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"mime"
	"net/http"

	"github.com/nathabonfim59/parkana/internal/ingest/otlpcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// NewOTLPHandler returns a handler for lokiapi.OTLPLogs, the OTLP HTTP logs
// endpoint (POST /v1/logs). It decodes bounded application/x-protobuf and
// application/json OTLP export requests, optionally gzip-compressed, maps
// them to logs.PushRequest with Loki's pinned defaults (otlpcodec) and then
// follows the push endpoint's semantics: the same limits, the same
// TransformFunc contract and the same AppendFunc durability path. Only request
// decoding and the empty-export shortcut differ from NewPushHandler.
//
// An export request that decodes to no entries — an empty body, or resources
// without records — still runs the optional Transform, which may inject
// entries or reject the request; whenever the final request (after that
// optional Transform) has no entries it is acknowledged with 204 without
// calling Append: Parkana never admits an empty WAL record.
func NewOTLPHandler(o PushOptions) (http.Handler, error) {
	// NewPushHandler owns option validation and defaulting; its concrete
	// handler is embedded so both endpoints validate identically.
	h, err := NewPushHandler(o)
	if err != nil {
		return nil, err
	}
	return &otlpHandler{pushHandler: h.(*pushHandler)}, nil
}

// otlpHandler serves lokiapi.OTLPLogs. It embeds the push handler to reuse
// its validation and options; ServeHTTP mirrors pushHandler.ServeHTTP for
// everything after decoding.
type otlpHandler struct {
	*pushHandler
}

func (h *otlpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		}
	}
	// Checked before revalidation, like the push handler's transform branch:
	// validate would dereference a nil request. Applies with or without a
	// transform, so an empty OTLP export never reaches Append.
	if req == nil || req.Entries() == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if h.o.Transform != nil {
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

// decode reads the bounded body, expands gzip and decodes the OTLP export
// request per Content-Type, matching Loki's extractLogs
// (pkg/loghttp/push/otlp.go:59): both content types carry an
// ExportLogsServiceRequest — never a snappy block, unlike the Loki push
// endpoint. The wire and the gzip-expanded body share MaxRequestBytes.
// A missing, unparseable or unsupported Content-Type and any content encoding
// other than identity and gzip is a client error.
func (h *otlpHandler) decode(r *http.Request) (*logs.PushRequest, error) {
	limit := h.o.MaxRequestBytes
	raw, err := readBounded(http.MaxBytesReader(nil, r.Body, limit), limit)
	if err != nil {
		return nil, err
	}
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
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
	var d otlpcodec.Decoder
	switch mt {
	case "application/x-protobuf":
		return d.DecodeProto(r.Context(), raw)
	case "application/json":
		return d.DecodeJSON(r.Context(), raw)
	default:
		return nil, fmt.Errorf("ingest: unsupported content type %q", ct)
	}
}
