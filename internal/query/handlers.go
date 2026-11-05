package query

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// handleQuery serves GET and POST /loki/api/v1/query.
func (s *service) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.serveQuery(w, r, instantQuery)
}

// handleQueryRange serves GET and POST /loki/api/v1/query_range.
func (s *service) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	s.serveQuery(w, r, rangeQuery)
}

// serveQuery parses one request into plan.Params, runs it through the engine
// over the catalog-backed source and encodes the result. The response reports
// only what was measured: execution wall time, entries returned and the raw
// request encoding flags.
func (s *service) serveQuery(w http.ResponseWriter, r *http.Request, kind queryKind) {
	p, err := parseQueryParams(r, kind, time.Now())
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	start := time.Now()
	res, err := s.engine.Query(r.Context(), s.logSrc, p)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	lokiapi.WriteQueryResult(w, res, lokiapi.ResponseOptions{
		EncodingFlags: r.Header.Get(lokiapi.EncodingFlagsHeader),
		Direction:     p.Direction,
		Stats: lokiapi.Stats{
			ExecTime:             time.Since(start),
			TotalEntriesReturned: resultEntries(res),
		},
	})
}

// writeQueryError logs and answers a failed query. Logging distinguishes
// client faults and cancellations from genuine server failures; the response
// itself is writeMetaError's pinned 400/499/504/500 mapping, shared with the
// metadata handlers.
func (s *service) writeQueryError(w http.ResponseWriter, err error) {
	level := slog.LevelWarn
	if isClientQueryError(err) {
		level = slog.LevelDebug
	} else if _, _, genuine := classifyContext(err); !genuine {
		level = slog.LevelDebug
	}
	s.log.Log(context.Background(), level, "query failed", "err", err)
	writeMetaError(w, err)
}

// isClientQueryError reports failures caused by the request's query.
func isClientQueryError(err error) bool {
	return errors.Is(err, plan.ErrBadQuery) ||
		errors.Is(err, plan.ErrUnsupported) ||
		errors.Is(err, plan.ErrLimit)
}

// resultEntries counts the entries or samples in a result, as Loki's
// totalEntriesReturned does.
func resultEntries(res plan.Result) int64 {
	switch res.Type {
	case plan.TypeStreams:
		var n int64
		for _, stream := range res.Streams {
			n += int64(len(stream.Entries))
		}
		return n
	case plan.TypeMatrix:
		var n int64
		for _, series := range res.Matrix {
			n += int64(len(series.Points))
		}
		return n
	case plan.TypeVector:
		return int64(len(res.Vector))
	case plan.TypeScalar:
		return 1
	default:
		return 0
	}
}
