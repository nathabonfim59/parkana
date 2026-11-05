package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// SnapshotProvider supplies the immutable pipeline snapshot covering one
// pushed request. Snapshot is called exactly once per invocation of the
// transform returned by NewTransform, with the request's tenant; the returned
// Snapshot must stay valid for the whole call and be safe for concurrent use.
// Implementations live in the registry (one per role, no shared state).
type SnapshotProvider interface {
	// Snapshot returns the complete, self-contained view of id's compiled
	// pipelines. A first-use load failure is an error, never an empty
	// snapshot that silently bypasses configured transforms.
	Snapshot(ctx context.Context, id tenant.ID) (Snapshot, error)
}

// NewTransform builds the ingest transform that runs a push through the
// tenant's ingest pipelines (PLAN.md §6). Snapshot is taken once per push;
// matches are selected once per input stream from the stream's ORIGINAL
// dataset, so set_stream never re-selects. The returned transform never
// modifies req or any slice it references: it returns req itself when the
// tenant has no ingest pipelines or none match, and otherwise builds a fresh
// request regrouped by each entry's final full label set.
//
// streamLabels are the configured routing labels (config.StreamLabels); the
// first present, non-empty one names the dataset, exactly as logs.Labels
// .StreamName does for storage.
func NewTransform(provider SnapshotProvider, streamLabels []string) (ingest.TransformFunc, error) {
	if provider == nil {
		return nil, errors.New("pipeline: NewTransform requires a SnapshotProvider")
	}
	if len(streamLabels) == 0 {
		return nil, errors.New("pipeline: NewTransform requires at least one stream label")
	}
	labels := slices.Clone(streamLabels)
	seen := make(map[string]struct{}, len(labels))
	for _, name := range labels {
		if err := validLabelNameErr(name); err != nil {
			return nil, fmt.Errorf("pipeline: stream label: %w", err)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("pipeline: duplicate stream label %q", name)
		}
		seen[name] = struct{}{}
	}
	return func(ctx context.Context, id tenant.ID, req *logs.PushRequest) (*logs.PushRequest, error) {
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("pipeline: transform: %w", err)
		}
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		// Exactly one snapshot per valid invocation, even when no pipeline
		// ends up matching.
		snap, err := provider.Snapshot(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("pipeline: snapshot tenant %q: %w", id, err)
		}
		if snap == nil {
			return nil, errors.New("pipeline: SnapshotProvider returned a nil snapshot")
		}
		// The provider may ignore cancellation; never report success for a
		// canceled request, including the no-definition pass-through.
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		if !snap.Has(StageIngest) {
			return req, nil
		}
		return transformRequest(ctx, snap, labels, req)
	}, nil
}

// contextErr renders a canceled or expired context as a transform error that
// keeps errors.Is(err, context.Canceled/DeadlineExceeded) intact.
func contextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pipeline: transform: %w", err)
	}
	return nil
}

// transformRequest applies snap to every entry of req and regroups the
// survivors by their final full label set. Streams and entries keep their
// input order: a label set appears where it was first seen and its entries
// follow in input order. An all-dropped request comes back with no streams.
func transformRequest(ctx context.Context, snap Snapshot, streamLabels []string, req *logs.PushRequest) (*logs.PushRequest, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.Entries() == 0 {
		return req, nil
	}
	type streamPlan struct {
		dataset string
		matched []*Compiled
	}
	plans := make([]streamPlan, len(req.Streams))
	matchedAny := false
	for i := range req.Streams {
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		dataset := req.Streams[i].Labels.StreamName(streamLabels)
		matched := snap.For(StageIngest, dataset)
		plans[i] = streamPlan{dataset: dataset, matched: matched}
		matchedAny = matchedAny || len(matched) > 0
	}
	// An unmatched request (or one for a tenant with definitions that do not
	// apply) is still work; a cancellation during planning must not report
	// success.
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if !matchedAny {
		return req, nil
	}

	out := &logs.PushRequest{Streams: make([]logs.Stream, 0, len(req.Streams))}
	index := make(map[string]int, len(req.Streams))
	for si := range req.Streams {
		st := &req.Streams[si]
		plan := &plans[si]
		for ei := range st.Entries {
			if err := contextErr(ctx); err != nil {
				return nil, err
			}
			e := &st.Entries[ei]
			entry, drop, err := applyEntry(ctx, plan.matched, plan.dataset, Entry{
				TS:     e.Timestamp,
				Line:   e.Line,
				Labels: st.Labels,
				Meta:   e.Metadata,
			})
			if err != nil {
				return nil, err
			}
			if drop {
				continue
			}
			if len(plan.matched) == 0 {
				// This row passed through untouched while another stream was
				// transformed. Apply never ran, so its slices still alias the
				// input; clone them before exposing them in a new request.
				entry.Labels = slices.Clone(entry.Labels)
				entry.Meta = slices.Clone(entry.Meta)
			}
			key := entry.Labels.String()
			oi, ok := index[key]
			if !ok {
				oi = len(out.Streams)
				index[key] = oi
				out.Streams = append(out.Streams, logs.Stream{Labels: entry.Labels})
			}
			out.Streams[oi].Entries = append(out.Streams[oi].Entries, logs.Entry{
				Timestamp: entry.TS,
				Line:      entry.Line,
				Metadata:  entry.Meta,
			})
		}
	}
	return out, nil
}

// applyEntry runs one entry through the matched pipelines in order. A drop
// removes it; a stop keeps the error-marked entry and halts the remaining
// pipelines; a keep continues the chain. An explicit on_error reject becomes
// an error wrapping ingest.ErrRejected; every other error (cancellation, an
// internal fault) is passed through with errors.Is intact and is NOT a
// rejection.
func applyEntry(ctx context.Context, matched []*Compiled, dataset string, cur Entry) (Entry, bool, error) {
	for _, c := range matched {
		next, outcome, err := c.Apply(ctx, dataset, cur)
		if err != nil {
			if rej, ok := errors.AsType[*RejectError](err); ok {
				return Entry{}, false, fmt.Errorf("%w: %w", ingest.ErrRejected, rej)
			}
			return Entry{}, false, fmt.Errorf("pipeline: apply %q: %w", c.Name(), err)
		}
		cur = next
		switch outcome {
		case OutcomeDrop:
			return Entry{}, true, nil
		case OutcomeStop:
			return cur, false, nil
		}
	}
	return cur, false, nil
}
