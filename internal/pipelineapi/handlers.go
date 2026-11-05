package pipelineapi

import (
	"fmt"
	"net/http"

	"encoding/json/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// list answers GET /api/v1/pipelines: the tenant's live pipelines sorted by
// name.
func (a *api) list(w http.ResponseWriter, r *http.Request) {
	defs, err := a.o.Catalog.ListPipelines(r.Context(), tenant.From(r.Context()))
	if err != nil {
		a.writeCatalogErr(w, r, err)
		return
	}
	views := make([]pipelineView, 0, len(defs))
	for _, d := range defs {
		v, err := view(d)
		if err != nil {
			a.internalErr(w, r, err)
			return
		}
		views = append(views, v)
	}
	lokiapi.WriteJSON(w, http.StatusOK, listResponse{Pipelines: views})
}

// create answers POST /api/v1/pipelines: a full definition, expected
// version zero.
func (a *api) create(w http.ResponseWriter, r *http.Request) {
	doc, ok := a.readDefinition(w, r)
	if !ok {
		return
	}
	if doc.Version != 0 {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: create does not take a version")
		return
	}
	a.put(w, r, doc, 0, http.StatusCreated)
}

// get answers GET /api/v1/pipelines/{name}.
func (a *api) get(w http.ResponseWriter, r *http.Request) {
	d, err := a.o.Catalog.GetPipeline(r.Context(), tenant.From(r.Context()), r.PathValue("name"))
	if err != nil {
		a.writeCatalogErr(w, r, err)
		return
	}
	v, err := view(d)
	if err != nil {
		a.internalErr(w, r, err)
		return
	}
	lokiapi.WriteJSON(w, http.StatusOK, v)
}

// replace answers PUT /api/v1/pipelines/{name}: a full definition plus the
// live version it replaces. The body's name must equal the path's.
func (a *api) replace(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	doc, ok := a.readDefinition(w, r)
	if !ok {
		return
	}
	if doc.Name != name {
		lokiapi.Error(w, http.StatusBadRequest,
			fmt.Sprintf("pipelineapi: name %q does not match path name %q", doc.Name, name))
		return
	}
	if doc.Version <= 0 {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: version must be a positive integer")
		return
	}
	a.put(w, r, doc, doc.Version, http.StatusOK)
}

// delete answers DELETE /api/v1/pipelines/{name}: {"version": n} tombstones
// the live pipeline at version n.
func (a *api) delete(w http.ResponseWriter, r *http.Request) {
	b, err := a.readBody(w, r)
	if err != nil {
		a.writeBodyErr(w, err)
		return
	}
	var doc deleteDoc
	if err := decodeDoc(b, &doc); err != nil {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: invalid delete body: "+err.Error())
		return
	}
	if doc.Version <= 0 {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: version must be a positive integer")
		return
	}
	_, err = a.o.Catalog.DeletePipeline(r.Context(), tenant.From(r.Context()), r.PathValue("name"), doc.Version)
	if err != nil {
		a.writeCatalogErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// put compiles doc's definition (the catalog only ever stores valid
// pipelines) and writes it with optimistic concurrency.
func (a *api) put(w http.ResponseWriter, r *http.Request, doc definitionDoc, expected int64, code int) {
	def := doc.definition()
	if _, err := pipeline.Compile(def, a.o.CompileOptions); err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	spec, err := json.Marshal(def.Spec())
	if err != nil {
		a.internalErr(w, r, fmt.Errorf("pipelineapi: encoding spec: %w", err))
		return
	}
	stored, err := a.o.Catalog.PutPipeline(r.Context(), catalog.PipelineDefinition{
		Tenant:   tenant.From(r.Context()),
		Name:     def.Name,
		Stage:    catalog.PipelineStage(def.Stage),
		Selector: def.Selector,
		Spec:     spec,
	}, expected)
	if err != nil {
		a.writeCatalogErr(w, r, err)
		return
	}
	v, err := view(stored)
	if err != nil {
		a.internalErr(w, r, err)
		return
	}
	lokiapi.WriteJSON(w, code, v)
}

// readDefinition decodes the bounded body as a definition document; on
// failure it writes the response and returns false.
func (a *api) readDefinition(w http.ResponseWriter, r *http.Request) (definitionDoc, bool) {
	b, err := a.readBody(w, r)
	if err != nil {
		a.writeBodyErr(w, err)
		return definitionDoc{}, false
	}
	var doc definitionDoc
	if err := decodeDoc(b, &doc); err != nil {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: invalid definition: "+err.Error())
		return definitionDoc{}, false
	}
	return doc, true
}
