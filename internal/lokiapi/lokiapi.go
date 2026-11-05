// Package lokiapi defines Parkana's Loki-compatible HTTP surface (PLAN.md §8):
// the route patterns, the buildinfo endpoint, Loki-style errors, and 501
// placeholders for every endpoint no role has implemented yet.
//
// Handlers live with the role that owns them (the ingester registers Push,
// the querier the read endpoints) and register on the shared server under the
// patterns below.
package lokiapi

import (
	"encoding/json/v2"
	"fmt"
	"net/http"

	"github.com/nathabonfim59/parkana/internal/version"
)

// Route patterns (http.ServeMux syntax). Loki accepts GET and POST for the
// read endpoints; Grafana mostly uses GET.
const (
	Push           = "POST /loki/api/v1/push"
	Query          = "GET /loki/api/v1/query"
	QueryPost      = "POST /loki/api/v1/query"
	QueryRange     = "GET /loki/api/v1/query_range"
	QueryRangePost = "POST /loki/api/v1/query_range"
	Labels         = "GET /loki/api/v1/labels"
	LabelsPost     = "POST /loki/api/v1/labels"
	LabelValues    = "GET /loki/api/v1/label/{name}/values"
	Series         = "GET /loki/api/v1/series"
	SeriesPost     = "POST /loki/api/v1/series"
	IndexStats     = "GET /loki/api/v1/index/stats"
	IndexVolume    = "GET /loki/api/v1/index/volume"
	IndexVolumeRng = "GET /loki/api/v1/index/volume_range"
	Tail           = "GET /loki/api/v1/tail"
	BuildInfo      = "GET /loki/api/v1/status/buildinfo"
	OTLPLogs       = "POST /v1/logs"
)

// Endpoints lists every pattern above.
var Endpoints = []string{
	Push, Query, QueryPost, QueryRange, QueryRangePost, Labels, LabelsPost, LabelValues,
	Series, SeriesPost, IndexStats, IndexVolume, IndexVolumeRng, Tail, BuildInfo, OTLPLogs,
}

// Registrar is the subset of *server.Server this package needs.
type Registrar interface {
	Handle(pattern string, h http.Handler)
	Registered(pattern string) bool
}

// RegisterBuildInfo serves Loki's buildinfo. Grafana probes it and gates
// features on "version", so that field reports version.LokiCompat; Parkana's
// own version is in "parkanaVersion".
func RegisterBuildInfo(r Registrar) {
	r.Handle(BuildInfo, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		info := version.Get()
		WriteJSON(w, http.StatusOK, map[string]string{
			"version":        version.LokiCompat,
			"revision":       info.Revision,
			"branch":         info.Branch,
			"buildUser":      "",
			"buildDate":      info.BuildDate,
			"goVersion":      info.GoVersion,
			"parkanaVersion": info.Version,
		})
	}))
}

// RegisterUnimplemented answers 501 on every endpoint not registered yet.
// Call it after all roles registered their routes.
func RegisterUnimplemented(r Registrar) {
	for _, p := range Endpoints {
		if !r.Registered(p) {
			r.Handle(p, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				Error(w, http.StatusNotImplemented, fmt.Sprintf("parkana: %s is not implemented yet", p))
			}))
		}
	}
}

// Error writes a Loki-style error: a plain-text body with the status code.
func Error(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	fmt.Fprintln(w, msg)
}

// WriteJSON writes v as a JSON response with encoding/json/v2. It encodes
// before writing headers, so an encoding failure still becomes a clean 500.
// Large streaming responses should use json/v2's jsontext.Encoder directly.
func WriteJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		Error(w, http.StatusInternalServerError, "parkana: encoding response: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}
