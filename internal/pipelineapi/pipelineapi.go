// Package pipelineapi serves Parkana's pipeline administration API
// (PLAN.md §6, §8): CRUD for tenant-scoped pipeline definitions plus a
// dry-run endpoint that runs an unsaved candidate over sample entries.
//
// The routes are process-wide: a role registers them once on the shared
// server, and the server's tenant middleware resolves the tenant every
// handler uses. Definitions are compiled before they are persisted, so the
// catalog only ever stores valid pipelines; the catalog row's Spec is the
// steps-only JSON document (pipeline.Spec) while name, stage and selector
// stay in authoritative catalog columns — Spec members can never override
// them. Errors are Loki-style plain text via lokiapi.Error.
package pipelineapi

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"encoding/json/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Route patterns (http.ServeMux syntax) registered by Register.
const (
	List    = "GET /api/v1/pipelines"
	Create  = "POST /api/v1/pipelines"
	Get     = "GET /api/v1/pipelines/{name}"
	Replace = "PUT /api/v1/pipelines/{name}"
	Delete  = "DELETE /api/v1/pipelines/{name}"
	DryRun  = "POST /api/v1/pipelines/dry-run"
)

// Defaults for the zero fields of Options.
const (
	// DefaultMaxBodyBytes bounds every request body: 1 MiB.
	DefaultMaxBodyBytes int64 = 1 << 20
	// DefaultMaxDryRunEntries bounds a dry-run request's entry count.
	DefaultMaxDryRunEntries = 1000
)

// errBodyTooLarge reports a request body over MaxBodyBytes (413).
var errBodyTooLarge = errors.New("pipelineapi: request body exceeds the limit")

// errInternalBody is the fixed response for every internal failure: the
// real detail (file paths, SQL, catalog internals, stored bytes) goes to
// the configured logger and never to the client.
const errInternalBody = "pipelineapi: internal error"

// Options configures Register.
type Options struct {
	// Catalog stores and serves pipeline definitions. Required.
	Catalog catalog.Catalog
	// CompileOptions configures every definition compile, both for writes
	// (compile-before-persist) and dry-runs. StreamLabels is required; the
	// byte limits keep the package defaults when zero.
	CompileOptions pipeline.CompileOptions
	// MaxBodyBytes bounds every request body; over-limit requests answer
	// 413. Zero uses DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// MaxDryRunEntries bounds a dry-run request's entries. Zero uses
	// DefaultMaxDryRunEntries.
	MaxDryRunEntries int
	// Logger receives the detail of internal failures (catalog errors,
	// undecodable stored specs, dry-run faults), which clients only ever
	// see as errInternalBody. Nil discards the records.
	Logger *slog.Logger
}

// Register installs the six pipeline administration routes on r. It fails
// when any pattern is already registered or the options are invalid.
func Register(r lokiapi.Registrar, o Options) error {
	if o.Catalog == nil {
		return errors.New("pipelineapi: Options.Catalog is required")
	}
	if len(o.CompileOptions.StreamLabels) == 0 {
		return errors.New("pipelineapi: Options.CompileOptions.StreamLabels is required")
	}
	if o.MaxBodyBytes < 0 || o.MaxDryRunEntries < 0 {
		return fmt.Errorf("pipelineapi: negative limit (%d, %d)", o.MaxBodyBytes, o.MaxDryRunEntries)
	}
	o.MaxBodyBytes = cmp.Or(o.MaxBodyBytes, DefaultMaxBodyBytes)
	o.MaxDryRunEntries = cmp.Or(o.MaxDryRunEntries, DefaultMaxDryRunEntries)
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	// A harmless compile validates the whole CompileOptions now — invalid
	// routing labels and non-positive byte limits fail registration, not
	// the first request.
	probe := pipeline.Definition{
		Name:     "validate",
		Stage:    pipeline.StageIngest,
		Selector: "*",
		Steps:    []pipeline.Step{{Type: pipeline.StepRename, From: "a", To: "b"}},
	}
	if _, err := pipeline.Compile(probe, o.CompileOptions); err != nil {
		return fmt.Errorf("pipelineapi: invalid Options.CompileOptions: %w", err)
	}
	// The API owns its options copy: mutating the caller's StreamLabels
	// slice after Register must not change later compilation, and
	// administration/dry-run execution never emits role metrics, so a
	// supplied observer is dropped.
	o.CompileOptions.StreamLabels = slices.Clone(o.CompileOptions.StreamLabels)
	o.CompileOptions.ObserveExecution = nil

	a := &api{o: o}
	routes := []struct {
		pattern string
		handler http.Handler
	}{
		{List, http.HandlerFunc(a.list)},
		{Create, http.HandlerFunc(a.create)},
		{Get, http.HandlerFunc(a.get)},
		{Replace, http.HandlerFunc(a.replace)},
		{Delete, http.HandlerFunc(a.delete)},
		{DryRun, http.HandlerFunc(a.dryRun)},
	}
	for _, rt := range routes {
		if r.Registered(rt.pattern) {
			return fmt.Errorf("pipelineapi: route %q already registered", rt.pattern)
		}
	}
	for _, rt := range routes {
		r.Handle(rt.pattern, rt.handler)
	}
	return nil
}

type api struct{ o Options }

// readBody returns the request body bounded to MaxBodyBytes; over-limit
// bodies fail with errBodyTooLarge.
func (a *api) readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.o.MaxBodyBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return nil, errBodyTooLarge
		}
		return nil, fmt.Errorf("pipelineapi: reading request body: %w", err)
	}
	return b, nil
}

// writeBodyErr writes the response for a readBody failure.
func (a *api) writeBodyErr(w http.ResponseWriter, err error) {
	if errors.Is(err, errBodyTooLarge) {
		lokiapi.Error(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("%s (%d bytes)", errBodyTooLarge, a.o.MaxBodyBytes))
		return
	}
	lokiapi.Error(w, http.StatusBadRequest, err.Error())
}

// internalErr logs err with the request's method, path and tenant, then
// answers the fixed internal-error body.
func (a *api) internalErr(w http.ResponseWriter, r *http.Request, err error) {
	a.o.Logger.ErrorContext(r.Context(), "pipelineapi: internal error",
		"err", err, "method", r.Method, "path", r.URL.Path, "tenant", tenant.From(r.Context()))
	lokiapi.Error(w, http.StatusInternalServerError, errInternalBody)
}

// writeCatalogErr maps a catalog pipeline failure to its status code:
// client faults (not found, version conflict, invalid definition) keep
// their detail; anything else is an internal error.
func (a *api) writeCatalogErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		lokiapi.Error(w, http.StatusNotFound, err.Error())
	case errors.Is(err, catalog.ErrVersionConflict):
		lokiapi.Error(w, http.StatusConflict, err.Error())
	case errors.Is(err, catalog.ErrInvalidPipeline):
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
	default:
		a.internalErr(w, r, err)
	}
}

// decodeDoc decodes one JSON document into v, rejecting unknown members,
// duplicate members and trailing data (the jsontext defaults plus
// RejectUnknownMembers).
func decodeDoc(b []byte, v any) error {
	return json.Unmarshal(b, v, json.RejectUnknownMembers(true))
}

// definitionDoc is a full pipeline definition plus the
// optimistic-concurrency version the write endpoints carry.
type definitionDoc struct {
	Name     string          `json:"name"`
	Stage    pipeline.Stage  `json:"stage"`
	Selector string          `json:"selector"`
	Steps    []pipeline.Step `json:"steps"`
	Version  int64           `json:"version"`
}

func (d definitionDoc) definition() pipeline.Definition {
	return pipeline.Definition{
		Name:     d.Name,
		Stage:    d.Stage,
		Selector: d.Selector,
		Steps:    d.Steps,
	}
}

// deleteDoc is DELETE /api/v1/pipelines/{name}'s body.
type deleteDoc struct {
	Version int64 `json:"version"`
}

// pipelineView is one pipeline as the API reports it: the definition fields
// plus the catalog-owned version and timestamps, never the tenant.
type pipelineView struct {
	Name      string          `json:"name"`
	Stage     pipeline.Stage  `json:"stage"`
	Selector  string          `json:"selector"`
	Steps     []pipeline.Step `json:"steps"`
	Version   int64           `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// listResponse is GET /api/v1/pipelines's body.
type listResponse struct {
	Pipelines []pipelineView `json:"pipelines"`
}

// view renders a stored definition. The steps come from decoding the
// catalog's Spec bytes, so a view is exactly what a PUT would re-accept
// (minus the version).
func view(d catalog.PipelineDefinition) (pipelineView, error) {
	spec, err := pipeline.DecodeSpec(d.Spec)
	if err != nil {
		return pipelineView{}, fmt.Errorf("pipelineapi: stored spec for %q: %w", d.Name, err)
	}
	return pipelineView{
		Name:      d.Name,
		Stage:     pipeline.Stage(d.Stage),
		Selector:  d.Selector,
		Steps:     spec.Steps,
		Version:   d.Version,
		CreatedAt: d.CreatedAt,
		UpdatedAt: d.UpdatedAt,
	}, nil
}
