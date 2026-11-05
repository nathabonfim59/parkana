// HTTP handlers for Loki's metadata endpoints (PLAN.md §8, part B2c). The
// handlers parse and validate the request parameters exactly as Loki 3.7.9's
// pkg/loghttp does (sharing the helpers in params.go), resolve the tenant
// from the request context, bound every read with the querier timeout and
// encode the private metaReader's answers in Loki's response shapes.
//
// Sources for parameter and shape semantics, pinned against Loki 3.7.9:
//
//   - pkg/loghttp/params.go (bounds, determineBounds, limit, step)
//   - pkg/loghttp/labels.go (ParseLabelQuery), series.go (ParseSeriesQuery,
//     match[] union and the lone "{}" special case)
//   - pkg/loghttp/query.go (ParseIndexStatsQuery = ParseRangeQuery,
//     ParseVolumeInstantQuery, ParseVolumeRangeQuery, volumeLimit,
//     volumeAggregateBy, targetLabels)
//   - pkg/util/marshal/marshal.go (WriteLabelResponseJSON with data omitted
//     when empty, WriteSeriesResponseJSON with data: [], WriteIndexStats-
//     ResponseJSON without a status envelope)
//   - pkg/querier/queryrange/volume.go + prometheus.go (volume and
//     volume_range answer with the Prometheus vector/matrix shape and the
//     stats object inside data)
//   - pkg/util/server/error.go (deadline 504 and client-cancel 499 texts)

package query

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Loki's pinned plain-text error bodies for deadline and client-cancellation
// responses, from pkg/util/server/error.go in Loki 3.7.9.
const (
	lokiErrDeadlineExceeded = "request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed"
	lokiErrClientCanceled   = "the request was cancelled by the client"
)

// statusClientClosedRequest is Loki's status for a client that went away
// mid-request; it is nginx's convention, not an RFC code.
const statusClientClosedRequest = 499

// errMetaTimeout is the cause carried by every metadata request deadline, so
// a timeout reads as "query timeout reached" in logs while still matching
// context.DeadlineExceeded.
var errMetaTimeout = errors.New("query timeout reached")

// errMalformedForm wraps a form parse failure as the client's fault.
func errMalformedForm(err error) error {
	return fmt.Errorf("query: malformed request parameters: %w: %w", plan.ErrBadQuery, err)
}

// metadataHTTP serves Loki's metadata endpoints from a metaReader. It is
// private to the querier role: the service wiring (part B2d) constructs it
// with the DuckDB reader, the injected plan.Engine, the configured querier
// timeout and a clock; the tests construct it with fakes.
type metadataHTTP struct {
	reader  metaReader
	engine  plan.Engine
	timeout time.Duration
	now     func() time.Time
}

// register wires every metadata route onto r. Only patterns that already
// exist in lokiapi are used: the labels and series endpoints accept POST like
// Loki's, the label values and index endpoints are GET-only in Parkana's
// route table.
func (h *metadataHTTP) register(r lokiapi.Registrar) {
	labels := http.HandlerFunc(h.labels)
	series := http.HandlerFunc(h.series)
	r.Handle(lokiapi.Labels, labels)
	r.Handle(lokiapi.LabelsPost, labels)
	r.Handle(lokiapi.LabelValues, http.HandlerFunc(h.labelValues))
	r.Handle(lokiapi.Series, series)
	r.Handle(lokiapi.SeriesPost, series)
	r.Handle(lokiapi.IndexStats, http.HandlerFunc(h.indexStats))
	r.Handle(lokiapi.IndexVolume, http.HandlerFunc(h.volumeInstant))
	r.Handle(lokiapi.IndexVolumeRng, http.HandlerFunc(h.volumeRange))
}

// labels serves /loki/api/v1/labels: the sorted distinct label names over
// [start, end], optionally restricted by the single query selector. An
// inverted range is accepted and yields nothing, as in Loki.
func (h *metadataHTTP) labels(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeMetaError(w, errMalformedForm(err))
		return
	}
	now := h.now()
	start, end, err := parseBounds(r.Form, now)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	groups, err := parseMetaQuerySelector(h.engine.ParseSelector, r.Form.Get("query"), false)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	ctx, cancel := context.WithTimeoutCause(r.Context(), h.timeout, errMetaTimeout)
	defer cancel()
	names, err := h.reader.LabelNames(ctx, h.metaQuery(start, end, groups, r))
	if err != nil {
		writeMetaError(w, err)
		return
	}
	writeLabelsResponse(w, names)
}

// labelValues serves /loki/api/v1/label/{name}/values: the sorted distinct
// values of one label over [start, end], with the same selector and inverted
// range handling as the labels endpoint.
func (h *metadataHTTP) labelValues(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeMetaError(w, errMalformedForm(err))
		return
	}
	now := h.now()
	start, end, err := parseBounds(r.Form, now)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	groups, err := parseMetaQuerySelector(h.engine.ParseSelector, r.Form.Get("query"), false)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	ctx, cancel := context.WithTimeoutCause(r.Context(), h.timeout, errMetaTimeout)
	defer cancel()
	values, err := h.reader.LabelValues(ctx, h.metaQuery(start, end, groups, r), r.PathValue("name"))
	if err != nil {
		writeMetaError(w, err)
		return
	}
	writeLabelsResponse(w, values)
}

// series serves /loki/api/v1/series: the distinct label sets matching the
// match[] (or match) selectors over [start, end]. No selectors, or a sole
// "{}", means every stream; multiple selectors are OR-combined and their
// results deduplicated by the reader.
func (h *metadataHTTP) series(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeMetaError(w, errMalformedForm(err))
		return
	}
	now := h.now()
	start, end, err := parseBounds(r.Form, now)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	groups, err := parseMetaMatchSelectors(h.engine.ParseSelector, append(r.Form["match[]"], r.Form["match"]...))
	if err != nil {
		writeMetaError(w, err)
		return
	}
	ctx, cancel := context.WithTimeoutCause(r.Context(), h.timeout, errMetaTimeout)
	defer cancel()
	series, err := h.reader.Series(ctx, h.metaQuery(start, end, groups, r))
	if err != nil {
		writeMetaError(w, err)
		return
	}
	writeSeriesResponse(w, series)
}

// indexStats serves /loki/api/v1/index/stats. The query selector is required
// (plan.Params.Validate rejects an empty one, matching Loki's ParseMatchers
// failure on the empty string) and the range must not be inverted; both
// bounds then go through roundToMilliseconds before the read, whose mirrored
// rounding — and its pre-epoch and out-of-range caveats — is documented
// there.
func (h *metadataHTTP) indexStats(w http.ResponseWriter, r *http.Request) {
	p, err := parseQueryParams(r, rangeQuery, h.now())
	if err != nil {
		writeMetaError(w, err)
		return
	}
	groups, err := parseMetaQuerySelector(h.engine.ParseSelector, p.Query, false)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	start, end := roundToMilliseconds(p.Start, p.End)
	ctx, cancel := context.WithTimeoutCause(r.Context(), h.timeout, errMetaTimeout)
	defer cancel()
	stats, err := h.reader.Stats(ctx, h.metaQuery(start, end, groups, r))
	if err != nil {
		writeMetaError(w, err)
		return
	}
	lokiapi.WriteJSON(w, http.StatusOK, indexStatsResponse(stats))
}

// volumeInstant serves /loki/api/v1/index/volume: volumes aggregated over the
// whole range, answered as a Loki vector. The query selector is required and
// the bare "{}" selects every stream. The time, limit and direction
// parameters are validated exactly as Loki's ParseInstantQuery does, even
// though only the bounds feed the read.
func (h *metadataHTTP) volumeInstant(w http.ResponseWriter, r *http.Request) {
	now := h.now()
	if err := normalizeVolumeLimit(r); err != nil {
		writeMetaError(w, errMalformedForm(err))
		return
	}
	p, err := parseQueryParams(r, instantQuery, now)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	start, end, err := parseBounds(r.Form, now)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	if end.Before(start) {
		writeMetaError(w, fmt.Errorf("query: 'end' must not be before 'start': %w", plan.ErrBadQuery))
		return
	}
	req, err := h.volumeParams(r, p.Query, start, end, 0, p.Limit)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	h.serveVolume(w, r, req)
}

// volumeRange serves /loki/api/v1/index/volume_range: volumes bucketed by
// step, answered as a Loki matrix (a vector when every name lands in a single
// bucket). The request parameters are the range query set plus the volume
// grouping parameters; limit=0 is reset to the default like the instant
// endpoint's, and the query selector is required.
func (h *metadataHTTP) volumeRange(w http.ResponseWriter, r *http.Request) {
	if err := normalizeVolumeLimit(r); err != nil {
		writeMetaError(w, errMalformedForm(err))
		return
	}
	p, err := parseQueryParams(r, rangeQuery, h.now())
	if err != nil {
		writeMetaError(w, err)
		return
	}
	req, err := h.volumeParams(r, p.Query, p.Start, p.End, p.Step, p.Limit)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	h.serveVolume(w, r, req)
}

// volumeParams assembles the volume grouping parameters shared by the instant
// and range endpoints: the selector (bare "{}" allowed), aggregateBy and
// targetLabels, with the caller-supplied (already validated) bounds and step.
func (h *metadataHTTP) volumeParams(r *http.Request, query string, start, end time.Time, step time.Duration, limit int) (metaVolumeRequest, error) {
	groups, err := parseMetaQuerySelector(h.engine.ParseSelector, query, true)
	if err != nil {
		return metaVolumeRequest{}, err
	}
	byLabels, err := parseVolumeAggregateBy(r.Form.Get("aggregateBy"))
	if err != nil {
		return metaVolumeRequest{}, err
	}
	start, end = roundToMilliseconds(start, end)
	// Loki's frontend codec hands the store whole-millisecond steps
	// (queryrange/codec.go builds the VolumeRequest with
	// Step.Milliseconds()), so a sub-millisecond step truncates after the
	// range validation: 1500µs becomes 1ms and anything below 1ms becomes
	// 0, the instant whole-range path.
	step = step.Truncate(time.Millisecond)
	return metaVolumeRequest{
		Query:             h.metaQuery(start, end, groups, r),
		Limit:             limit,
		Step:              step,
		TargetLabels:      parseVolumeTargetLabels(r.Form.Get("targetLabels")),
		AggregateByLabels: byLabels,
	}, nil
}

// serveVolume runs one volume request against the reader and encodes the
// answer.
func (h *metadataHTTP) serveVolume(w http.ResponseWriter, r *http.Request, req metaVolumeRequest) {
	ctx, cancel := context.WithTimeoutCause(r.Context(), h.timeout, errMetaTimeout)
	defer cancel()
	entries, err := h.reader.Volume(ctx, req)
	if err != nil {
		writeMetaError(w, err)
		return
	}
	writeVolumeResponse(w, entries)
}

// metaQuery assembles the reader query from the request's tenant and the
// parsed bounds and selector groups.
func (h *metadataHTTP) metaQuery(start, end time.Time, groups [][]plan.Matcher, r *http.Request) metaQuery {
	return metaQuery{
		Tenant: tenant.From(r.Context()),
		Start:  start,
		End:    end,
		Groups: groups,
	}
}

// normalizeVolumeLimit parses the form and rewrites an explicit limit=0 to
// the default in the form, exactly as Loki's volumeLimit does, so the shared
// limit parsing accepts it. The parse error is returned rather than
// swallowed: once the form is populated a second ParseForm reports nil, so
// this is the only chance to reject a malformed form on the volume routes.
func normalizeVolumeLimit(r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return err
	}
	if r.Form.Get("limit") == "0" {
		r.Form.Set("limit", fmt.Sprint(defaultQueryLimit))
	}
	return nil
}

// parseVolumeAggregateBy validates aggregateBy: empty and "series" group by
// the selector's matcher names (the full label set without a selector),
// "labels" by label name, anything else is a client error.
func parseVolumeAggregateBy(v string) (bool, error) {
	switch v {
	case "", "series":
		return false, nil
	case "labels":
		return true, nil
	}
	return false, fmt.Errorf("query: invalid 'aggregateBy' %q: %w", v, plan.ErrBadQuery)
}

// parseVolumeTargetLabels splits targetLabels on commas without trimming,
// as Loki does; nil when the parameter is absent. Empty elements are preserved:
// the reader answers a request containing one with a successful empty result,
// exactly as the pinned live Loki 3.7.9 does.
func parseVolumeTargetLabels(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

// labelsResponse is the JSON shape of /labels and /label/{name}/values:
// Loki omits the data field entirely when there are no values.
type labelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data,omitempty"`
}

// writeLabelsResponse writes sorted label names or values.
func writeLabelsResponse(w http.ResponseWriter, values []string) {
	lokiapi.WriteJSON(w, http.StatusOK, labelsResponse{Status: "success", Data: values})
}

// seriesResponse is the JSON shape of /series: data is always an array, empty
// when nothing matched.
type seriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

// writeSeriesResponse writes the distinct matching label sets.
func writeSeriesResponse(w http.ResponseWriter, labels []logs.Labels) {
	data := make([]map[string]string, 0, len(labels))
	for _, ls := range labels {
		data = append(data, ls.Map())
	}
	lokiapi.WriteJSON(w, http.StatusOK, seriesResponse{Status: "success", Data: data})
}

// indexStatsResponse is the JSON shape of /index/stats: bare counts with no
// status envelope, all four fields always present.
type indexStatsResponse struct {
	Streams uint64 `json:"streams"`
	Chunks  uint64 `json:"chunks"`
	Bytes   uint64 `json:"bytes"`
	Entries uint64 `json:"entries"`
}

// writeVolumeResponse encodes volume results the way Loki's query frontend
// does (queryrange.ToPrometheusResponse over prometheus.go's marshalers): a
// matrix as soon as any name carries more than one bucket, a vector when
// every name has a single one, inside the standard query envelope with the
// stats object in data. Statistics stay zero: the reader reports measured
// volumes only, and no step evaluation runs here.
func writeVolumeResponse(w http.ResponseWriter, entries []metaVolumeEntry) {
	for _, e := range entries {
		if len(e.Buckets) > 1 {
			series := make([]plan.Series, 0, len(entries))
			for _, e := range entries {
				if len(e.Buckets) == 0 {
					continue
				}
				points := make([]plan.Point, 0, len(e.Buckets))
				for _, b := range e.Buckets {
					points = append(points, plan.Point{T: b.End, V: float64(b.Volume)})
				}
				series = append(series, plan.Series{Metric: e.Metric, Points: points})
			}
			lokiapi.WriteQueryResult(w, plan.Result{Type: plan.TypeMatrix, Matrix: series}, lokiapi.ResponseOptions{})
			return
		}
	}
	vector := make([]plan.Sample, 0, len(entries))
	for _, e := range entries {
		if len(e.Buckets) == 0 {
			continue
		}
		vector = append(vector, plan.Sample{
			Metric: e.Metric,
			Point:  plan.Point{T: e.Buckets[0].End, V: float64(e.Buckets[0].Volume)},
		})
	}
	lokiapi.WriteQueryResult(w, plan.Result{Type: plan.TypeVector, Vector: vector}, lokiapi.ResponseOptions{})
}

// classifyContext walks err's tree — single wraps, joined errors and their
// combinations — and reports whether it contains deadlines, cancellations or
// genuine (non-context) errors. The request timeout cause counts as a
// deadline, since it only ever annotates one.
func classifyContext(err error) (deadline, canceled, other bool) {
	if err == nil {
		return false, false, false
	}
	switch err := err.(type) {
	case interface{ Unwrap() error }:
		return classifyContext(err.Unwrap())
	case interface{ Unwrap() []error }:
		for _, e := range err.Unwrap() {
			d, c, o := classifyContext(e)
			deadline, canceled, other = deadline || d, canceled || c, other || o
		}
		return deadline, canceled, other
	}
	switch {
	case errors.Is(err, errMetaTimeout), errors.Is(err, context.DeadlineExceeded):
		return true, false, false
	case errors.Is(err, context.Canceled):
		return false, true, false
	}
	return false, false, true
}

// writeMetaError maps a metadata error to Loki's HTTP error responses.
// Client faults (plan.ErrBadQuery, ErrUnsupported, ErrLimit) answer 400 with
// the error text and win over everything else. Beyond those, only a
// unanimous context outcome is a timeout or a cancellation: an expired
// request deadline answers 504 and a client that went away 499, both with
// Loki's pinned plain-text bodies, while a genuine error joined with either
// — or a deadline/cancellation mix — keeps its 500 so the real failure is
// not masked by the deadline.
func writeMetaError(w http.ResponseWriter, err error) {
	if errors.Is(err, plan.ErrBadQuery) ||
		errors.Is(err, plan.ErrUnsupported) ||
		errors.Is(err, plan.ErrLimit) {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	deadline, canceled, other := classifyContext(err)
	// deadline == canceled covers the mix (both true) and errors carrying no
	// context information at all (both false).
	if other || deadline == canceled {
		lokiapi.Error(w, http.StatusInternalServerError, err.Error())
		return
	}
	if deadline {
		lokiapi.Error(w, http.StatusGatewayTimeout, lokiErrDeadlineExceeded)
		return
	}
	lokiapi.Error(w, statusClientClosedRequest, lokiErrClientCanceled)
}
