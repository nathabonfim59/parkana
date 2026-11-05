package pipelineapi

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/pipeline"
)

// dryRunDoc is POST /api/v1/pipelines/dry-run's body: an unsaved candidate
// definition plus sample entries.
type dryRunDoc struct {
	Pipeline pipeline.Definition `json:"pipeline"`
	Entries  []dryRunEntry       `json:"entries"`
}

// dryRunEntry is one sample or transformed log entry. Timestamp is
// optional; pipelines never change it, so it echoes the input.
type dryRunEntry struct {
	Timestamp time.Time         `json:"timestamp,omitzero"`
	Line      string            `json:"line"`
	Labels    map[string]string `json:"labels"`
	Meta      map[string]string `json:"meta"`
}

// dryRunResponse is the dry-run answer: one output entry per non-dropped
// input entry, in input order, plus the dropped count.
type dryRunResponse struct {
	Entries []dryRunEntry `json:"entries"`
	Dropped int           `json:"dropped"`
}

// dryRun answers POST /api/v1/pipelines/dry-run. It compiles the unsaved
// definition and runs it on each entry whose original dataset (from the
// entry's own labels over the routing labels) matches the selector —
// mirroring snapshot semantics, where an entry outside the selector passes
// through untouched. Nothing is written: no catalog call, no WAL, no
// object. A step's on_error reject answers 400, mirroring the ingest
// adapter's ErrRejected mapping; any other Apply failure is an internal
// error — fixed body, real cause logged.
func (a *api) dryRun(w http.ResponseWriter, r *http.Request) {
	if r.Context().Err() != nil {
		return // the client is gone; nothing to answer
	}
	b, err := a.readBody(w, r)
	if err != nil {
		a.writeBodyErr(w, err)
		return
	}
	var doc dryRunDoc
	if err := decodeDoc(b, &doc); err != nil {
		lokiapi.Error(w, http.StatusBadRequest, "pipelineapi: invalid dry-run: "+err.Error())
		return
	}
	if len(doc.Entries) > a.o.MaxDryRunEntries {
		lokiapi.Error(w, http.StatusBadRequest,
			fmt.Sprintf("pipelineapi: %d entries exceeds the maximum of %d", len(doc.Entries), a.o.MaxDryRunEntries))
		return
	}
	for i := range doc.Entries {
		if r.Context().Err() != nil {
			return
		}
		e := &doc.Entries[i]
		if err := logs.FromMap(e.Labels).Validate(); err != nil {
			lokiapi.Error(w, http.StatusBadRequest, fmt.Sprintf("pipelineapi: entry %d labels: %v", i, err))
			return
		}
		if err := logs.FromMap(e.Meta).Validate(); err != nil {
			lokiapi.Error(w, http.StatusBadRequest, fmt.Sprintf("pipelineapi: entry %d meta: %v", i, err))
			return
		}
	}
	c, err := pipeline.Compile(doc.Pipeline, a.o.CompileOptions)
	if err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	matches := func(dataset string) bool {
		return c.Selector() == "*" || c.Selector() == dataset
	}
	out := make([]dryRunEntry, 0, len(doc.Entries))
	dropped := 0
	for _, e := range doc.Entries {
		if r.Context().Err() != nil {
			return
		}
		res := pipeline.Entry{
			TS:     e.Timestamp,
			Line:   e.Line,
			Labels: logs.FromMap(e.Labels),
			Meta:   logs.FromMap(e.Meta),
		}
		dataset := c.DatasetOf(res.Labels)
		if matches(dataset) {
			o, oc, err := c.Apply(r.Context(), dataset, res)
			if err != nil {
				if rej, yes := errors.AsType[*pipeline.RejectError](err); yes {
					lokiapi.Error(w, http.StatusBadRequest, rej.Error())
					return
				}
				a.internalErr(w, r, err)
				return
			}
			if oc == pipeline.OutcomeDrop {
				dropped++
				continue
			}
			res = o
		}
		out = append(out, dryRunEntry{
			Timestamp: res.TS,
			Line:      res.Line,
			Labels:    res.Labels.Map(),
			Meta:      res.Meta.Map(),
		})
	}
	if r.Context().Err() != nil {
		return // canceled between compile and response
	}
	lokiapi.WriteJSON(w, http.StatusOK, dryRunResponse{Entries: out, Dropped: dropped})
}
