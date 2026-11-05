package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// trStreamLabels is the routing label set every test transform and compiled
// pipeline uses.
var trStreamLabels = []string{"job"}

var trBase = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

// trDef builds an ingest definition.
func trDef(name, selector string, steps ...pipeline.Step) pipeline.Definition {
	return pipeline.Definition{Name: name, Stage: pipeline.StageIngest, Selector: selector, Steps: steps}
}

// trCompile compiles def with the shared test routing labels.
func trCompile(t *testing.T, def pipeline.Definition) *pipeline.Compiled {
	t.Helper()
	c, err := pipeline.Compile(def, pipeline.CompileOptions{StreamLabels: trStreamLabels})
	if err != nil {
		t.Fatalf("Compile(%s): %v", def.Name, err)
	}
	return c
}

// trSnapshot builds a fixed snapshot over defs.
func trSnapshot(t *testing.T, defs ...*pipeline.Compiled) pipeline.Snapshot {
	t.Helper()
	s, err := pipeline.NewFixedSnapshot(defs...)
	if err != nil {
		t.Fatalf("NewFixedSnapshot: %v", err)
	}
	return s
}

// trNew builds the transform under test.
func trNew(t *testing.T, p pipeline.SnapshotProvider) ingest.TransformFunc {
	t.Helper()
	tr, err := pipeline.NewTransform(p, trStreamLabels)
	if err != nil {
		t.Fatalf("NewTransform: %v", err)
	}
	return tr
}

// trProvider is a SnapshotProvider over a per-tenant map that records calls.
type trProvider struct {
	mu    sync.Mutex
	snaps map[tenant.ID]pipeline.Snapshot
	err   error
	calls []tenant.ID
}

func (p *trProvider) Snapshot(_ context.Context, id tenant.ID) (pipeline.Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, id)
	if p.err != nil {
		return nil, p.err
	}
	s, ok := p.snaps[id]
	if !ok {
		return nil, errors.New("no snapshot for tenant " + string(id))
	}
	return s, nil
}

func (p *trProvider) callIDs() []tenant.ID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

// trProviderFor builds a provider with one snapshot for the default tenant.
func trProviderFor(s pipeline.Snapshot) *trProvider {
	return &trProvider{snaps: map[tenant.ID]pipeline.Snapshot{tenant.Default: s}}
}

// trMeta builds a sorted metadata/label set from name/value pairs.
func trMeta(pairs ...string) logs.Labels {
	m := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return logs.FromMap(m)
}

// trClone deep-copies req.
func trClone(req *logs.PushRequest) *logs.PushRequest {
	out := &logs.PushRequest{Streams: make([]logs.Stream, len(req.Streams))}
	for i, s := range req.Streams {
		out.Streams[i].Labels = slices.Clone(s.Labels)
		out.Streams[i].Entries = make([]logs.Entry, len(s.Entries))
		for j, e := range s.Entries {
			out.Streams[i].Entries[j] = logs.Entry{
				Timestamp: e.Timestamp,
				Line:      e.Line,
				Metadata:  slices.Clone(e.Metadata),
			}
		}
	}
	return out
}

// trFindStream returns the stream whose labels equal the given pairs.
func trFindStream(t *testing.T, out *logs.PushRequest, pairs ...string) *logs.Stream {
	t.Helper()
	want := trMeta(pairs...).String()
	for i := range out.Streams {
		if out.Streams[i].Labels.String() == want {
			return &out.Streams[i]
		}
	}
	t.Fatalf("no stream with labels %s in %v", want, out.Streams)
	return nil
}

// trFindEntry returns the entry with the given line in stream.
func trFindEntry(t *testing.T, stream *logs.Stream, line string) *logs.Entry {
	t.Helper()
	for i := range stream.Entries {
		if stream.Entries[i].Line == line {
			return &stream.Entries[i]
		}
	}
	t.Fatalf("no entry with line %q in %+v", line, stream.Entries)
	return nil
}

func TestTransformNoDefinitionsExactPointer(t *testing.T) {
	cases := []struct {
		name string
		snap pipeline.Snapshot
	}{
		{name: "empty snapshot", snap: trSnapshot(t)},
		{
			name: "read-only snapshot",
			snap: trSnapshot(t, trCompile(t, pipeline.Definition{
				Name:     "read.rename",
				Stage:    pipeline.StageRead,
				Selector: "*",
				Steps:    []pipeline.Step{{Type: pipeline.StepRename, From: "k", To: "v"}},
			})),
		},
		{
			name: "ingest definitions that match no dataset",
			snap: trSnapshot(t, trCompile(t, trDef("app.only", "app", pipeline.Step{
				Type: pipeline.StepRename, From: "k", To: "v",
			}))),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := trProviderFor(tc.snap)
			tr := trNew(t, p)
			req := &logs.PushRequest{Streams: []logs.Stream{{
				Labels:  trMeta("job", "other"),
				Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
			}}}
			out, err := tr(t.Context(), tenant.Default, req)
			if err != nil {
				t.Fatalf("transform: %v", err)
			}
			if out != req {
				t.Fatalf("out = %p, want the exact input pointer %p", out, req)
			}
			if calls := p.callIDs(); len(calls) != 1 || calls[0] != tenant.Default {
				t.Fatalf("Snapshot calls = %v, want exactly [default]", calls)
			}
		})
	}
}

func TestTransformSnapshotOncePerPush(t *testing.T) {
	p := trProviderFor(trSnapshot(t, trCompile(t, trDef("a.rename", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	}))))
	tr := trNew(t, p)
	req := &logs.PushRequest{Streams: []logs.Stream{
		{
			Labels: trMeta("job", "one"),
			Entries: []logs.Entry{
				{Timestamp: trBase, Line: "a", Metadata: trMeta("k", "1")},
				{Timestamp: trBase.Add(time.Second), Line: "b", Metadata: trMeta("k", "2")},
			},
		},
		{
			Labels:  trMeta("job", "two"),
			Entries: []logs.Entry{{Timestamp: trBase.Add(2 * time.Second), Line: "c", Metadata: trMeta("k", "3")}},
		},
	}}
	if _, err := tr(t.Context(), tenant.Default, req); err != nil {
		t.Fatalf("transform: %v", err)
	}
	if calls := p.callIDs(); len(calls) != 1 {
		t.Fatalf("Snapshot calls = %v, want exactly one", calls)
	}
}

func TestTransformTenantIsolation(t *testing.T) {
	defA := trDef("a.rename", "*", pipeline.Step{Type: pipeline.StepRename, From: "k", To: "a"})
	defB := trDef("b.rename", "*", pipeline.Step{Type: pipeline.StepRename, From: "k", To: "b"})
	p := &trProvider{snaps: map[tenant.ID]pipeline.Snapshot{
		tenant.Default:   trSnapshot(t, trCompile(t, defA)),
		tenant.ID("t-b"): trSnapshot(t, trCompile(t, defB)),
	}}
	tr := trNew(t, p)
	newReq := func() *logs.PushRequest {
		return &logs.PushRequest{Streams: []logs.Stream{{
			Labels:  trMeta("job", "app"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
		}}}
	}
	outA, err := tr(t.Context(), tenant.Default, newReq())
	if err != nil {
		t.Fatalf("tenant default: %v", err)
	}
	if _, ok := outA.Streams[0].Entries[0].Metadata.Get("a"); !ok {
		t.Fatalf("default tenant metadata = %v, want a", outA.Streams[0].Entries[0].Metadata)
	}
	outB, err := tr(t.Context(), tenant.ID("t-b"), newReq())
	if err != nil {
		t.Fatalf("tenant t-b: %v", err)
	}
	if _, ok := outB.Streams[0].Entries[0].Metadata.Get("b"); !ok {
		t.Fatalf("tenant t-b metadata = %v, want b", outB.Streams[0].Entries[0].Metadata)
	}
	got := p.callIDs()
	want := []tenant.ID{tenant.Default, tenant.ID("t-b")}
	if !slices.Equal(got, want) {
		t.Fatalf("Snapshot calls = %v, want %v", got, want)
	}
}

func TestTransformDeterministicNameOrder(t *testing.T) {
	// Compile in reverse name order; the snapshot selects in name order, so
	// a-first must run before b-second.
	second := trCompile(t, trDef("b-second", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "mid", To: "second",
	}))
	first := trCompile(t, trDef("a-first", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "mid",
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, second, first)))
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
	}}}
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	meta := out.Streams[0].Entries[0].Metadata
	if v, ok := meta.Get("second"); !ok || v != "1" {
		t.Fatalf("metadata = %v, want second=1", meta)
	}
	if _, ok := meta.Get("mid"); ok {
		t.Fatalf("metadata = %v, mid must not survive", meta)
	}
}

func TestTransformChainedSplitRecombine(t *testing.T) {
	promote := trCompile(t, trDef("a-promote", "*", pipeline.Step{
		Type: pipeline.StepPromote, From: "svc", To: "service",
	}))
	route := trCompile(t, trDef("b-route", "*", pipeline.Step{
		Type: pipeline.StepSetStream, From: "route",
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, promote, route)))
	req := &logs.PushRequest{Streams: []logs.Stream{
		{
			Labels: trMeta("job", "app"),
			Entries: []logs.Entry{
				{Timestamp: trBase, Line: "e0", Metadata: trMeta("route", "a", "svc", "api")},
				{Timestamp: trBase.Add(time.Second), Line: "e1", Metadata: trMeta("route", "b", "svc", "api")},
			},
		},
		{
			Labels:  trMeta("job", "other"),
			Entries: []logs.Entry{{Timestamp: trBase.Add(2 * time.Second), Line: "e2", Metadata: trMeta("route", "a", "svc", "api")}},
		},
	}}
	before := trClone(req)
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(out.Streams) != 2 {
		t.Fatalf("streams = %v, want two split streams", out.Streams)
	}
	a := trFindStream(t, out, "job", "a", "service", "api")
	if want := []string{"e0", "e2"}; !slices.Equal(trLines(a), want) {
		t.Fatalf("job=a lines = %v, want %v (e2 recombined)", trLines(a), want)
	}
	if got := []time.Time{a.Entries[0].Timestamp, a.Entries[1].Timestamp}; !got[0].Equal(trBase) || !got[1].Equal(trBase.Add(2*time.Second)) {
		t.Fatalf("timestamps = %v, want original order", got)
	}
	b := trFindStream(t, out, "job", "b", "service", "api")
	if want := []string{"e1"}; !slices.Equal(trLines(b), want) {
		t.Fatalf("job=b lines = %v, want %v", trLines(b), want)
	}
	if out.Streams[0].Labels.String() != a.Labels.String() {
		t.Fatalf("first output stream = %v, want job=a first (input order)", out.Streams[0].Labels)
	}
	if !reflect.DeepEqual(req, before) {
		t.Fatalf("transform mutated its input:\n got %+v\nwant %+v", req, before)
	}
}

func trLines(s *logs.Stream) []string {
	lines := make([]string, len(s.Entries))
	for i, e := range s.Entries {
		lines[i] = e.Line
	}
	return lines
}

func TestTransformUsesOriginalDataset(t *testing.T) {
	route := trCompile(t, trDef("a-route", "*", pipeline.Step{
		Type: pipeline.StepSetStream, From: "target",
	}))
	appOnly := trCompile(t, trDef("b-app-only", "app",
		pipeline.Step{Type: pipeline.StepRename, From: "flag", To: "applied"},
		pipeline.Step{Type: pipeline.StepPromote, Value: "stream", To: "origin"},
	))
	tr := trNew(t, trProviderFor(trSnapshot(t, route, appOnly)))
	req := &logs.PushRequest{Streams: []logs.Stream{
		{
			Labels:  trMeta("job", "app"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: "from-app", Metadata: trMeta("target", "app", "flag", "x")}},
		},
		{
			Labels:  trMeta("job", "other"),
			Entries: []logs.Entry{{Timestamp: trBase.Add(time.Second), Line: "from-other", Metadata: trMeta("target", "app", "flag", "y")}},
		},
	}}
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	fromApp := trFindStream(t, out, "job", "app", "origin", "app")
	e0 := trFindEntry(t, fromApp, "from-app")
	if v, ok := e0.Metadata.Get("applied"); !ok || v != "x" {
		t.Fatalf("from-app metadata = %v, want applied=x", e0.Metadata)
	}
	// from-other was rerouted to job=app by a-route but selected pipelines
	// against its ORIGINAL dataset "other", so b-app-only must not run.
	fromOther := trFindStream(t, out, "job", "app")
	e1 := trFindEntry(t, fromOther, "from-other")
	if _, ok := e1.Metadata.Get("applied"); ok {
		t.Fatalf("from-other metadata = %v, must not have been re-selected after routing", e1.Metadata)
	}
	if _, ok := e1.Metadata.Get("origin"); ok {
		t.Fatalf("from-other metadata = %v, promote must not have run", e1.Metadata)
	}
}

func TestTransformDefaultKeepStopsChain(t *testing.T) {
	parse := trCompile(t, trDef("a-parse", "*", pipeline.Step{Type: pipeline.StepParseJSON}))
	route := trCompile(t, trDef("b-route", "*", pipeline.Step{Type: pipeline.StepSetStream, From: "target"}))
	tr := trNew(t, trProviderFor(trSnapshot(t, parse, route)))
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "not json", Metadata: trMeta("target", "leaked")}},
	}}}
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if out.Entries() != 1 {
		t.Fatalf("entries = %d, want the error-marked row kept", out.Entries())
	}
	stream := trFindStream(t, out, "job", "app")
	e := &stream.Entries[0]
	if e.Line != "not json" {
		t.Fatalf("line = %q, want unchanged", e.Line)
	}
	if v, ok := e.Metadata.Get("pipeline_error"); !ok || v != "parse_json" {
		t.Fatalf("metadata = %v, want pipeline_error=parse_json", e.Metadata)
	}
	if v, ok := e.Metadata.Get("pipeline_error_details"); !ok || v == "" {
		t.Fatalf("metadata = %v, want non-empty pipeline_error_details", e.Metadata)
	}
	if v, ok := e.Metadata.Get("target"); !ok || v != "leaked" {
		t.Fatalf("metadata = %v, want original metadata preserved", e.Metadata)
	}
	if v, ok := stream.Labels.Get("job"); !ok || v != "app" {
		t.Fatalf("labels = %v, stop must skip the remaining matched pipeline", stream.Labels)
	}
	if _, ok := req.Streams[0].Entries[0].Metadata.Get("pipeline_error"); ok {
		t.Fatal("transform mutated the input entry")
	}
}

func TestTransformDropAndAllDrop(t *testing.T) {
	drop := trCompile(t, trDef("a-drop", "*", pipeline.Step{
		Type: pipeline.StepDropIf, Expr: `meta.drop == "yes"`,
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, drop)))
	mk := func(dropValues ...string) *logs.PushRequest {
		entries := make([]logs.Entry, len(dropValues))
		for i, v := range dropValues {
			entries[i] = logs.Entry{
				Timestamp: trBase.Add(time.Duration(i) * time.Second),
				Line:      "line-" + v,
				Metadata:  trMeta("drop", v),
			}
		}
		return &logs.PushRequest{Streams: []logs.Stream{{Labels: trMeta("job", "app"), Entries: entries}}}
	}
	out, err := tr(t.Context(), tenant.Default, mk("yes", "no"))
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if out.Entries() != 1 || trLines(&out.Streams[0])[0] != "line-no" {
		t.Fatalf("out = %+v, want only line-no", out)
	}
	all, err := tr(t.Context(), tenant.Default, mk("yes", "yes"))
	if err != nil {
		t.Fatalf("transform all drop: %v", err)
	}
	if all == nil || all.Entries() != 0 {
		t.Fatalf("all-drop out = %+v, want a zero-entry request", all)
	}
}

func TestTransformRejectError(t *testing.T) {
	reject := trCompile(t, trDef("a-reject", "*", pipeline.Step{
		Type: pipeline.StepParseJSON, OnError: pipeline.OnErrorReject,
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, reject)))
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "not json"}},
	}}}
	_, err := tr(t.Context(), tenant.Default, req)
	if err == nil {
		t.Fatal("transform succeeded, want rejection")
	}
	if !errors.Is(err, ingest.ErrRejected) {
		t.Fatalf("error = %v, want errors.Is(ErrRejected)", err)
	}
	rej, ok := errors.AsType[*pipeline.RejectError](err)
	if !ok {
		t.Fatalf("error = %v, want a *pipeline.RejectError", err)
	}
	if rej.Pipeline != "a-reject" || rej.Step != "parse_json" || rej.Details == "" {
		t.Fatalf("RejectError = %+v, want the pipeline, step and details", rej)
	}
}

func TestTransformInternalAndCancellationErrors(t *testing.T) {
	sentinel := errors.New("catalog unavailable")
	tr := trNew(t, &trProvider{err: sentinel})
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
	}}}
	if _, err := tr(t.Context(), tenant.Default, req); !errors.Is(err, sentinel) {
		t.Fatalf("provider error = %v, want errors.Is(sentinel)", err)
	} else if errors.Is(err, ingest.ErrRejected) {
		t.Fatalf("provider error = %v, must not be a rejection", err)
	}

	rename := trCompile(t, trDef("a-rename", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	}))
	tr = trNew(t, trProviderFor(trSnapshot(t, rename)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req = &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
	}}}
	_, err := tr(ctx, tenant.Default, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want errors.Is(context.Canceled)", err)
	}
	if errors.Is(err, ingest.ErrRejected) {
		t.Fatalf("canceled error = %v, must not be a rejection", err)
	}
}

func TestTransformCanceledBeforeSnapshot(t *testing.T) {
	p := trProviderFor(trSnapshot(t))
	tr := trNew(t, p)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
	}}}
	_, err := tr(ctx, tenant.Default, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want errors.Is(context.Canceled)", err)
	}
	if errors.Is(err, ingest.ErrRejected) {
		t.Fatalf("canceled error = %v, must not be a rejection", err)
	}
	if calls := p.callIDs(); len(calls) != 0 {
		t.Fatalf("Snapshot calls = %v, want none for a canceled invocation", calls)
	}
}

// trCancelOnSnapshot cancels ctx inside Snapshot and then returns snap, so the
// transform cannot rely on the provider to notice cancellation.
type trCancelOnSnapshot struct {
	snap   pipeline.Snapshot
	cancel context.CancelFunc
}

func (p *trCancelOnSnapshot) Snapshot(context.Context, tenant.ID) (pipeline.Snapshot, error) {
	p.cancel()
	return p.snap, nil
}

func TestTransformCanceledAfterSnapshotNeverSucceeds(t *testing.T) {
	noMatch := trSnapshot(t, trCompile(t, trDef("app.only", "app", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	})))
	match := trSnapshot(t, trCompile(t, trDef("any", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	})))
	cases := []struct {
		name string
		snap pipeline.Snapshot
		req  *logs.PushRequest
	}{
		{
			name: "no definitions",
			snap: trSnapshot(t),
			req: &logs.PushRequest{Streams: []logs.Stream{{
				Labels:  trMeta("job", "app"),
				Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
			}}},
		},
		{
			name: "definitions match nothing",
			snap: noMatch,
			req: &logs.PushRequest{Streams: []logs.Stream{{
				Labels:  trMeta("job", "other"),
				Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
			}}},
		},
		{
			name: "mixed matched and unmatched",
			snap: noMatch,
			req: &logs.PushRequest{Streams: []logs.Stream{
				{
					Labels:  trMeta("job", "app"),
					Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
				},
				{
					Labels:  trMeta("job", "other"),
					Entries: []logs.Entry{{Timestamp: trBase.Add(time.Second), Line: "bye"}},
				},
			}},
		},
		{
			name: "matched",
			snap: match,
			req: &logs.PushRequest{Streams: []logs.Stream{{
				Labels:  trMeta("job", "app"),
				Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
			}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			p := &trCancelOnSnapshot{snap: tc.snap, cancel: cancel}
			tr := trNew(t, p)
			out, err := tr(ctx, tenant.Default, tc.req)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("out = %+v, error = %v, want errors.Is(context.Canceled)", out, err)
			}
			if errors.Is(err, ingest.ErrRejected) {
				t.Fatalf("canceled error = %v, must not be a rejection", err)
			}
		})
	}
}

// trCancelInFor cancels ctx from inside Snapshot.For, after the transform's
// post-Snapshot check, to exercise cancellation during planning and between
// streams.
type trCancelInFor struct {
	snap   pipeline.Snapshot
	cancel context.CancelFunc
}

func (s *trCancelInFor) Has(stage pipeline.Stage) bool { return s.snap.Has(stage) }

func (s *trCancelInFor) For(stage pipeline.Stage, dataset string) []*pipeline.Compiled {
	out := s.snap.For(stage, dataset)
	s.cancel()
	return out
}

func TestTransformCanceledDuringPlanning(t *testing.T) {
	snap := trSnapshot(t, trCompile(t, trDef("any", "*", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	})))
	cases := []struct {
		name    string
		streams []logs.Stream
	}{
		{
			name: "post-plan check",
			streams: []logs.Stream{{
				Labels:  trMeta("job", "app"),
				Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
			}},
		},
		{
			name: "next stream check",
			streams: []logs.Stream{
				{
					Labels:  trMeta("job", "app"),
					Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
				},
				{
					Labels:  trMeta("job", "other"),
					Entries: []logs.Entry{{Timestamp: trBase.Add(time.Second), Line: "bye"}},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			tr := trNew(t, trProviderFor(&trCancelInFor{snap: snap, cancel: cancel}))
			out, err := tr(ctx, tenant.Default, &logs.PushRequest{Streams: tc.streams})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("out = %+v, error = %v, want errors.Is(context.Canceled)", out, err)
			}
			if errors.Is(err, ingest.ErrRejected) {
				t.Fatalf("canceled error = %v, must not be a rejection", err)
			}
		})
	}
}

func TestTransformInvalidTenant(t *testing.T) {
	p := trProviderFor(trSnapshot(t))
	tr := trNew(t, p)
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
	}}}
	for _, id := range []tenant.ID{"", "bad id", "x/y"} {
		_, err := tr(t.Context(), id, req)
		if !errors.Is(err, tenant.ErrInvalid) {
			t.Fatalf("tenant %q error = %v, want errors.Is(tenant.ErrInvalid)", id, err)
		}
	}
	if calls := p.callIDs(); len(calls) != 0 {
		t.Fatalf("Snapshot calls = %v, want none for invalid tenants", calls)
	}
}

func TestTransformNeverMutatesInput(t *testing.T) {
	parse := trCompile(t, trDef("a-parse", "*", pipeline.Step{Type: pipeline.StepParseJSON}))
	promote := trCompile(t, trDef("b-promote", "*", pipeline.Step{
		Type: pipeline.StepPromote, From: "svc", To: "service",
	}))
	route := trCompile(t, trDef("c-route", "*", pipeline.Step{
		Type: pipeline.StepSetStream, From: "route",
	}))
	redact := trCompile(t, trDef("d-redact", "*", pipeline.Step{
		Type: pipeline.StepRedact, Pattern: "api", Replace: new("***"),
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, parse, promote, route, redact)))
	req := &logs.PushRequest{Streams: []logs.Stream{
		{
			Labels:  trMeta("job", "orig"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: `{"svc":"api","route":"new"}`, Metadata: trMeta("keep", "me")}},
		},
		{
			Labels:  trMeta("job", "untouched"),
			Entries: []logs.Entry{{Timestamp: trBase.Add(time.Second), Line: "plain"}},
		},
	}}
	before := trClone(req)
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if !reflect.DeepEqual(req, before) {
		t.Fatalf("transform mutated its input:\n got %+v\nwant %+v", req, before)
	}
	// The output must not alias the input either: the push handler only
	// promises not to mutate what the pipeline returned, so a later reader
	// changing the output must not rewrite the request.
	out.Streams[0].Labels[0].Value = "changed"
	out.Streams[0].Entries[0].Line = "changed"
	out.Streams[0].Entries[0].Metadata[0].Value = "changed"
	if !reflect.DeepEqual(req, before) {
		t.Fatalf("output aliases the input:\n got %+v\nwant %+v", req, before)
	}
}

func TestTransformMixedRequestClonesPassThroughRows(t *testing.T) {
	// An exact selector matches only job=app; job=other passes through while
	// a fresh transformed request is built, so its slices must be cloned too.
	matched := trCompile(t, trDef("app.only", "app", pipeline.Step{
		Type: pipeline.StepRename, From: "k", To: "v",
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, matched)))
	req := &logs.PushRequest{Streams: []logs.Stream{
		{
			Labels:  trMeta("job", "app"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: "matched", Metadata: trMeta("k", "1")}},
		},
		{
			Labels:  trMeta("job", "other"),
			Entries: []logs.Entry{{Timestamp: trBase.Add(time.Second), Line: "passed", Metadata: trMeta("keep", "me")}},
		},
	}}
	before := trClone(req)
	out, err := tr(t.Context(), tenant.Default, req)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	passed := trFindStream(t, out, "job", "other")
	if len(passed.Labels) == 0 || len(passed.Entries[0].Metadata) == 0 {
		t.Fatalf("pass-through stream = %+v, want labels and metadata", passed)
	}
	passed.Labels[0].Value = "changed"
	passed.Entries[0].Metadata[0].Value = "changed"
	if !reflect.DeepEqual(req, before) {
		t.Fatalf("pass-through output aliases the input:\n got %+v\nwant %+v", req, before)
	}
}

func TestNewTransformValidation(t *testing.T) {
	snap := trSnapshot(t)
	cases := []struct {
		name   string
		p      pipeline.SnapshotProvider
		labels []string
	}{
		{name: "nil provider", p: nil, labels: trStreamLabels},
		{name: "empty stream labels", p: trProviderFor(snap), labels: nil},
		{name: "invalid stream label", p: trProviderFor(snap), labels: []string{"bad-name"}},
		{name: "duplicate stream label", p: trProviderFor(snap), labels: []string{"job", "job"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pipeline.NewTransform(tc.p, tc.labels); err == nil {
				t.Fatal("NewTransform succeeded, want an error")
			}
		})
	}
}

// trAppend records one Append call.
type trAppend struct {
	id  tenant.ID
	req *logs.PushRequest
}

func trHandler(t *testing.T, tr ingest.TransformFunc) (http.Handler, *[]trAppend) {
	t.Helper()
	var calls []trAppend
	h, err := ingest.NewPushHandler(ingest.PushOptions{
		Append: func(_ context.Context, id tenant.ID, req *logs.PushRequest) error {
			calls = append(calls, trAppend{id: id, req: req})
			return nil
		},
		Transform: tr,
	})
	if err != nil {
		t.Fatalf("NewPushHandler: %v", err)
	}
	return h, &calls
}

func trPost(t *testing.T, h http.Handler, req *logs.PushRequest, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	body, err := pushcodec.MarshalSnappy(t.Context(), req)
	if err != nil {
		t.Fatalf("MarshalSnappy: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/x-protobuf")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestPushHandlerTransformsBeforeAppend(t *testing.T) {
	promote := trCompile(t, trDef("a-promote", "*", pipeline.Step{
		Type: pipeline.StepPromote, From: "svc", To: "service",
	}))
	tr := trNew(t, trProviderFor(trSnapshot(t, promote)))
	h, calls := trHandler(t, tr)
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("svc", "api")}},
	}}}
	w := trPost(t, h, req, t.Context())
	if w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
	}
	if len(*calls) != 1 {
		t.Fatalf("Append calls = %d, want 1", len(*calls))
	}
	appended := (*calls)[0]
	if appended.id != tenant.Default {
		t.Fatalf("Append tenant = %q, want default", appended.id)
	}
	if v, ok := appended.req.Streams[0].Labels.Get("service"); !ok || v != "api" {
		t.Fatalf("appended labels = %v, want promoted service=api", appended.req.Streams[0].Labels)
	}
	if v, ok := appended.req.Streams[0].Entries[0].Metadata.Get("svc"); !ok || v != "api" {
		t.Fatalf("appended metadata = %v, want svc preserved", appended.req.Streams[0].Entries[0].Metadata)
	}
}

func TestPushHandlerDefaultKeepAppendsMarked(t *testing.T) {
	parse := trCompile(t, trDef("a-parse", "*", pipeline.Step{Type: pipeline.StepParseJSON}))
	h, calls := trHandler(t, trNew(t, trProviderFor(trSnapshot(t, parse))))
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "not json", Metadata: trMeta("keep", "me")}},
	}}}
	w := trPost(t, h, req, t.Context())
	if w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
	}
	if len(*calls) != 1 {
		t.Fatalf("Append calls = %d, want 1", len(*calls))
	}
	meta := (*calls)[0].req.Streams[0].Entries[0].Metadata
	if v, ok := meta.Get("pipeline_error"); !ok || v != "parse_json" {
		t.Fatalf("appended metadata = %v, want pipeline_error=parse_json", meta)
	}
	if v, ok := meta.Get("pipeline_error_details"); !ok || v == "" {
		t.Fatalf("appended metadata = %v, want non-empty details", meta)
	}
	if v, ok := meta.Get("keep"); !ok || v != "me" {
		t.Fatalf("appended metadata = %v, want original metadata preserved", meta)
	}
}

func TestPushHandlerUsesRequestTenant(t *testing.T) {
	p := &trProvider{snaps: map[tenant.ID]pipeline.Snapshot{
		tenant.ID("t-b"): trSnapshot(t, trCompile(t, trDef("a-rename", "*", pipeline.Step{
			Type: pipeline.StepRename, From: "k", To: "v",
		}))),
	}}
	tr := trNew(t, p)
	h, calls := trHandler(t, tr)
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels:  trMeta("job", "app"),
		Entries: []logs.Entry{{Timestamp: trBase, Line: "hi", Metadata: trMeta("k", "1")}},
	}}}
	ctx := tenant.With(t.Context(), tenant.ID("t-b"))
	if w := trPost(t, h, req, ctx); w.Code != http.StatusNoContent {
		t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
	}
	if len(*calls) != 1 {
		t.Fatalf("Append calls = %d, want 1", len(*calls))
	}
	if (*calls)[0].id != tenant.ID("t-b") {
		t.Fatalf("Append tenant = %q, want t-b", (*calls)[0].id)
	}
	if v, ok := (*calls)[0].req.Streams[0].Entries[0].Metadata.Get("v"); !ok || v != "1" {
		t.Fatalf("metadata = %v, want v=1", (*calls)[0].req.Streams[0].Entries[0].Metadata)
	}
}

func TestPushHandlerRejectAndAllDropNeverAppend(t *testing.T) {
	t.Run("reject", func(t *testing.T) {
		reject := trCompile(t, trDef("a-reject", "*", pipeline.Step{
			Type: pipeline.StepParseJSON, OnError: pipeline.OnErrorReject,
		}))
		h, calls := trHandler(t, trNew(t, trProviderFor(trSnapshot(t, reject))))
		req := &logs.PushRequest{Streams: []logs.Stream{{
			Labels: trMeta("job", "app"),
			Entries: []logs.Entry{
				{Timestamp: trBase, Line: `{"ok":"1"}`},
				{Timestamp: trBase.Add(time.Second), Line: "not json"},
			},
		}}}
		w := trPost(t, h, req, t.Context())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("code = %d, want 400; body %q", w.Code, w.Body)
		}
		// The first row succeeded, but rejection is atomic: nothing appends.
		if len(*calls) != 0 {
			t.Fatalf("Append calls = %d, want 0", len(*calls))
		}
		if !strings.Contains(w.Body.String(), "rejected") {
			t.Fatalf("body = %q, want the rejection text", w.Body)
		}
	})
	t.Run("all drop", func(t *testing.T) {
		drop := trCompile(t, trDef("a-drop", "*", pipeline.Step{
			Type: pipeline.StepDropIf, Expr: "true",
		}))
		h, calls := trHandler(t, trNew(t, trProviderFor(trSnapshot(t, drop))))
		req := &logs.PushRequest{Streams: []logs.Stream{{
			Labels:  trMeta("job", "app"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
		}}}
		w := trPost(t, h, req, t.Context())
		if w.Code != http.StatusNoContent {
			t.Fatalf("code = %d, want 204; body %q", w.Code, w.Body)
		}
		if len(*calls) != 0 {
			t.Fatalf("Append calls = %d, want 0", len(*calls))
		}
	})
	t.Run("internal error", func(t *testing.T) {
		h, calls := trHandler(t, trNew(t, &trProvider{err: errors.New("catalog unavailable")}))
		req := &logs.PushRequest{Streams: []logs.Stream{{
			Labels:  trMeta("job", "app"),
			Entries: []logs.Entry{{Timestamp: trBase, Line: "hi"}},
		}}}
		w := trPost(t, h, req, t.Context())
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("code = %d, want 500; body %q", w.Code, w.Body)
		}
		if len(*calls) != 0 {
			t.Fatalf("Append calls = %d, want 0", len(*calls))
		}
	})
}
