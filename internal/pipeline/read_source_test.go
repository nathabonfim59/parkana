package pipeline

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// testStreamLabels is the routing priority every test here compiles and wraps
// with, the config default.
var testStreamLabels = []string{"service_name", "job"}

// t0 is the base instant of the hand-built datasets.
var t0 = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

// countProvider is a SnapshotProvider that counts loads and can change what it
// returns between them.
type countProvider struct {
	mu      sync.Mutex
	snap    Snapshot
	err     error
	calls   int
	tenants []tenant.ID
}

func (p *countProvider) set(snap Snapshot, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snap, p.err = snap, err
}

func (p *countProvider) Snapshot(_ context.Context, id tenant.ID) (Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.tenants = append(p.tenants, id)
	return p.snap, p.err
}

func (p *countProvider) stats() (calls int, tenants []tenant.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, slices.Clone(p.tenants)
}

// emptySnapshot is a tenant with no pipelines at all.
func emptySnapshot(t *testing.T) Snapshot {
	t.Helper()
	s, err := NewFixedSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// compileRead compiles one read-stage definition with the test routing labels.
func compileRead(t *testing.T, name, selector string, steps ...Step) *Compiled {
	t.Helper()
	c, err := Compile(Definition{Name: name, Stage: StageRead, Selector: selector, Steps: steps}, CompileOptions{StreamLabels: testStreamLabels})
	if err != nil {
		t.Fatalf("compile %q: %v", name, err)
	}
	return c
}

// mustSnapshot builds a fixed snapshot, failing on the first error.
func mustSnapshot(t *testing.T, defs ...*Compiled) Snapshot {
	t.Helper()
	s, err := NewFixedSnapshot(defs...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// mustWrap wraps raw over snap, failing on construction errors.
func mustWrap(t *testing.T, raw plan.Source, snap Snapshot, streamLabels ...string) (plan.Source, *countProvider) {
	t.Helper()
	if streamLabels == nil {
		streamLabels = testStreamLabels
	}
	p := &countProvider{snap: snap}
	s, err := NewReadSource(raw, p, streamLabels)
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}

// row builds one stored row at t0+d.
func row(d time.Duration, line string, labels, meta map[string]string) plan.Row {
	return plan.Row{TS: t0.Add(d), Line: line, Labels: logs.FromMap(labels), Meta: logs.FromMap(meta)}
}

// apiLabels builds an api (or custom) service label set.
func apiLabels(service string) map[string]string {
	return map[string]string{"service_name": service, "env": "prod"}
}

// collectRows drains a Select sequence.
func collectRows(t *testing.T, seq iter.Seq2[plan.Row, error]) []plan.Row {
	t.Helper()
	var out []plan.Row
	for r, err := range seq {
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// collectBuckets drains an Aggregate sequence.
func collectBuckets(t *testing.T, seq iter.Seq2[plan.Bucket, error]) []plan.Bucket {
	t.Helper()
	var out []plan.Bucket
	for b, err := range seq {
		if err != nil {
			t.Fatalf("aggregate: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// bucketKey renders a bucket for comparison.
func bucketKey(b plan.Bucket) string {
	return fmt.Sprintf("%d|%s|%s|%d/%d", b.Start.UnixMicro(), b.Labels, b.Meta, b.Count, b.Bytes)
}

// baseSel is a valid whole-range selection over the given matchers.
func baseSel(ms ...plan.Matcher) plan.Selection {
	return plan.Selection{
		Tenant:    tenant.Default,
		Matchers:  ms,
		From:      t0.Add(-time.Hour),
		Through:   t0.Add(2 * time.Hour),
		Direction: plan.Forward,
	}
}

// eq is plan.MustMatcher(plan.MatchEqual, name, value).
func eq(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, name, value) }

// contains is plan.MustLineFilter(plan.LineContains, v).
func contains(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }

// selSig renders every field of a Selection the wrapper may or may not change.
func selSig(sel plan.Selection) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tenant=%s matchers=[", sel.Tenant)
	for _, m := range sel.Matchers {
		fmt.Fprintf(&b, "%s,", m.String())
	}
	fmt.Fprintf(&b, "] filters=[")
	for _, f := range sel.LineFilters {
		fmt.Fprintf(&b, "%s,", f.String())
	}
	fmt.Fprintf(&b, "] from=%d through=%d dir=%d limit=%d",
		sel.From.UnixMicro(), sel.Through.UnixMicro(), sel.Direction, sel.Limit)
	return b.String()
}

// recSel is one recorded Select call.
type recSel struct {
	sig string
	sel plan.Selection
}

// recordingSource is a plan.Source double that records every call and answers
// from canned rows and buckets. rowsErr, when set, is yielded after rows.
type recordingSource struct {
	mu         sync.Mutex
	selects    []recSel
	aggregates []string
	rows       []plan.Row
	rowsErr    error
	buckets    []plan.Bucket
	finished   int // Select iterations that ran their defer
}

func (s *recordingSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		s.mu.Lock()
		s.selects = append(s.selects, recSel{sig: selSig(sel), sel: sel})
		rows := slices.Clone(s.rows)
		rowsErr := s.rowsErr
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.finished++
			s.mu.Unlock()
		}()
		if err := sel.Validate(); err != nil {
			yield(plan.Row{}, err)
			return
		}
		for _, r := range rows {
			if err := ctx.Err(); err != nil {
				yield(plan.Row{}, fmt.Errorf("recording: %w", err))
				return
			}
			if !yield(r, nil) {
				return
			}
		}
		if rowsErr != nil {
			yield(plan.Row{}, rowsErr)
		}
	}
}

func (s *recordingSource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		s.mu.Lock()
		s.aggregates = append(s.aggregates, selSig(sel)+" agg="+fmt.Sprint(agg.Width, agg.IncludeMeta))
		buckets := slices.Clone(s.buckets)
		s.mu.Unlock()
		if err := sel.Validate(); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		for _, b := range buckets {
			if err := ctx.Err(); err != nil {
				yield(plan.Bucket{}, fmt.Errorf("recording: %w", err))
				return
			}
			if !yield(b, nil) {
				return
			}
		}
	}
}

func (s *recordingSource) selectCalls() []recSel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.selects)
}

func (s *recordingSource) aggregateCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.aggregates)
}

func (s *recordingSource) finishedSelects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished
}

func TestNewReadSourceValidation(t *testing.T) {
	raw := plan.NewMemSource()
	provider := &countProvider{snap: emptySnapshot(t)}
	tests := []struct {
		name         string
		raw          plan.Source
		provider     SnapshotProvider
		streamLabels []string
		wantErr      string
	}{
		{"nil raw", nil, provider, testStreamLabels, "requires a raw plan.Source"},
		{"nil provider", raw, nil, testStreamLabels, "requires a SnapshotProvider"},
		{"no labels", raw, provider, nil, "at least one stream label"},
		{"empty labels", raw, provider, []string{}, "at least one stream label"},
		{"invalid label", raw, provider, []string{"service_name", "9bad"}, "stream label"},
		{"duplicate label", raw, provider, []string{"service_name", "service_name"}, "duplicate stream label"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReadSource(tt.raw, tt.provider, tt.streamLabels)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}

	// The routing keys are cloned: query the wrapper built from the caller's
	// slice after mutating that slice. Only service_name can route this row to
	// the api pipeline, so a missing clone turns the rewrite into a no-op.
	labels := []string{"service_name", "job"}
	mem := plan.NewMemSource()
	mem.Add(tenant.Default, row(0, "plain", apiLabels("api"), nil))
	redact := compileRead(t, "redact", "api", Step{Type: StepRedact, Pattern: "plain", Replace: new("hidden")})
	cloneProvider := &countProvider{snap: mustSnapshot(t, redact)}
	s, err := NewReadSource(mem, cloneProvider, labels)
	if err != nil {
		t.Fatal(err)
	}
	labels[0] = "hijacked"
	got := collectRows(t, s.Select(t.Context(), baseSel(eq("service_name", "api"))))
	if len(got) != 1 || got[0].Line != "hidden" {
		t.Fatalf("clone check: rows = %v, want the api pipeline applied", got)
	}
}

func TestReadSourceDelegatesWithoutReadPipelines(t *testing.T) {
	raw := &recordingSource{rows: []plan.Row{row(0, "x", apiLabels("api"), nil)}}
	// An ingest-stage pipeline must not switch the wrapper off pass-through.
	ingestOnly, err := Compile(Definition{Name: "ing", Stage: StageIngest, Selector: "*", Steps: []Step{{Type: StepDropIf, Expr: `line == "x"`}}}, CompileOptions{StreamLabels: testStreamLabels})
	if err != nil {
		t.Fatal(err)
	}
	snap := mustSnapshot(t, ingestOnly)
	wrapped, provider := mustWrap(t, raw, snap)

	sel := baseSel(eq("service_name", "api"))
	sel.LineFilters = []plan.LineFilter{contains("x")}
	sel.Limit = 3
	got := collectRows(t, wrapped.Select(t.Context(), sel))
	if len(got) != 1 {
		t.Fatalf("rows = %v", got)
	}
	calls := raw.selectCalls()
	if len(calls) != 1 {
		t.Fatalf("raw Select calls = %d, want 1", len(calls))
	}
	if calls[0].sig != selSig(sel) {
		t.Fatalf("delegated sel = %s, want the exact original %s", calls[0].sig, selSig(sel))
	}

	agg := plan.BucketAgg{Width: time.Minute, Origin: t0}
	buckets := collectBuckets(t, wrapped.Aggregate(t.Context(), sel, agg))
	_ = buckets
	if raw.aggregateCalls() != 1 {
		t.Fatalf("raw Aggregate calls = %d, want 1", raw.aggregateCalls())
	}
	if calls, _ := provider.stats(); calls != 2 {
		t.Fatalf("provider calls = %d, want one per operation", calls)
	}
}

// TestReadSourcePassThroughSuite runs the plantest conformance suite over a
// wrapped MemSource twice: with an empty snapshot (exact delegation) and with a
// read pipeline that selects a dataset the fixtures do not contain (the
// conservative tenant-wide path with no per-dataset matches). Both must behave
// exactly like the raw source.
func TestReadSourcePassThroughSuite(t *testing.T) {
	for _, tc := range []struct {
		name string
		snap func(t *testing.T) Snapshot
	}{
		{"no-definitions", func(t *testing.T) Snapshot { return emptySnapshot(t) }},
		{"unmatched-definition", func(t *testing.T) Snapshot {
			return mustSnapshot(t, compileRead(t, "elsewhere", "no-such-stream", Step{Type: StepRedact, Pattern: "x", Replace: new("y")}))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plantest.RunSource(t, func(t *testing.T, files []plantest.File) plan.Source {
				w, _ := mustWrap(t, plantest.Mem(files), tc.snap(t))
				return w
			})
		})
	}
}

func TestReadSourceStripsFiltersAndLimitOnTransformedPath(t *testing.T) {
	raw := &recordingSource{rows: []plan.Row{
		row(0, "a secret line", apiLabels("api"), nil),
		row(time.Second, "b secret line", apiLabels("api"), nil),
	}}
	redact := compileRead(t, "redact", "*", Step{Type: StepRedact, Pattern: "secret", Replace: new("token")})
	wrapped, _ := mustWrap(t, raw, mustSnapshot(t, redact))

	sel := baseSel(eq("service_name", "api"))
	sel.LineFilters = []plan.LineFilter{contains("secret")}
	sel.Limit = 1
	sel.Direction = plan.Backward
	got := collectRows(t, wrapped.Select(t.Context(), sel))
	// The filter sees the transformed line: no row contains "secret" anymore.
	if len(got) != 0 {
		t.Fatalf("rows = %v, want none (needle rewritten away)", got)
	}
	calls := raw.selectCalls()
	if len(calls) != 1 {
		t.Fatalf("raw Select calls = %d, want 1", len(calls))
	}
	want := sel
	want.LineFilters = nil
	want.Limit = 0
	if calls[0].sig != selSig(want) {
		t.Fatalf("raw sel = %s, want stripped %s", calls[0].sig, selSig(want))
	}

	// Aggregate on the transformed path goes through raw Select, never raw
	// Aggregate.
	raw.mu.Lock()
	raw.selects = nil
	raw.mu.Unlock()
	_ = collectBuckets(t, wrapped.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: t0}))
	if raw.aggregateCalls() != 0 {
		t.Fatalf("raw Aggregate calls = %d, want 0 on the transformed path", raw.aggregateCalls())
	}
	if calls := raw.selectCalls(); len(calls) != 1 || calls[0].sel.LineFilters != nil || calls[0].sel.Limit != 0 {
		t.Fatalf("aggregate raw selects = %+v, want one stripped Select", calls)
	}
}

func TestReadSourceSelectTransforms(t *testing.T) {
	// redact rewrites SECRET→token; drop-if removes the forbidden and dropme
	// lines; ordering runs a-redact before b-drop so b sees a's output.
	aRedact := compileRead(t, "a-redact", "api", Step{Type: StepRedact, Pattern: "SECRET", Replace: new("token")})
	bDrop := compileRead(t, "b-drop", "api", Step{Type: StepDropIf, Expr: `line contains "forbidden" or line contains "dropme"`})
	snap := mustSnapshot(t, aRedact, bDrop)

	newMem := func() *plan.MemSource {
		m := plan.NewMemSource()
		m.Add(tenant.Default,
			row(0, "one SECRET here", apiLabels("api"), nil),
			row(time.Second, "two SECRET forbidden", apiLabels("api"), nil),
			row(2*time.Second, "three secret lowercase", apiLabels("api"), nil),
			row(3*time.Second, "web SECRET stays", apiLabels("web"), nil),
			row(4*time.Second, "dropme SECRET row", apiLabels("api"), nil),
		)
		return m
	}
	wrapped, _ := mustWrap(t, newMem(), snap)
	api := baseSel(eq("service_name", "api"))

	t.Run("rewritten-in needle survives filters", func(t *testing.T) {
		sel := api
		sel.LineFilters = []plan.LineFilter{contains("token")}
		got := collectRows(t, wrapped.Select(t.Context(), sel))
		if len(got) != 1 || got[0].Line != "one token here" {
			t.Fatalf("rows = %v, want the rewritten-in row only", got)
		}
	})
	t.Run("rewritten-away needle disappears", func(t *testing.T) {
		sel := api
		sel.LineFilters = []plan.LineFilter{contains("SECRET")}
		if got := collectRows(t, wrapped.Select(t.Context(), sel)); len(got) != 0 {
			t.Fatalf("rows = %v, want none", got)
		}
	})
	t.Run("filter operators", func(t *testing.T) {
		for _, tt := range []struct {
			op   plan.LineFilterType
			want []string
		}{
			{plan.LineContains, []string{"one token here"}},
			{plan.LineNotContains, []string{"three secret lowercase"}},
			{plan.LineRegexp, []string{"one token here"}},
			{plan.LineNotRegexp, []string{"three secret lowercase"}},
		} {
			sel := api
			sel.LineFilters = []plan.LineFilter{plan.MustLineFilter(tt.op, "token")}
			got := collectRows(t, wrapped.Select(t.Context(), sel))
			var lines []string
			for _, r := range got {
				lines = append(lines, r.Line)
			}
			if !slices.Equal(lines, tt.want) {
				t.Fatalf("%s: lines = %v, want %v", tt.op, lines, tt.want)
			}
		}
	})
	t.Run("dropped rows do not consume the limit", func(t *testing.T) {
		sel := api
		sel.Limit = 2
		got := collectRows(t, wrapped.Select(t.Context(), sel))
		if len(got) != 2 {
			t.Fatalf("rows = %d (%v), want 2 survivors under the limit", len(got), got)
		}
		if got[0].Line != "one token here" || got[1].Line != "three secret lowercase" {
			t.Fatalf("limit rows = %v", got)
		}
	})
	t.Run("direction and boundaries", func(t *testing.T) {
		// The window includes the api row at +2s (From inclusive) and excludes
		// the web row at +3s (Through exclusive) even though its matcher
		// matches. Backward direction has nothing else to reorder.
		sel := baseSel(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web"))
		sel.Direction = plan.Backward
		sel.From = t0.Add(2 * time.Second)
		sel.Through = t0.Add(3 * time.Second)
		got := collectRows(t, wrapped.Select(t.Context(), sel))
		if len(got) != 1 || !got[0].TS.Equal(t0.Add(2*time.Second)) {
			t.Fatalf("rows = %v, want only the +2s row", got)
		}
	})
	t.Run("mixed streams transform only matches", func(t *testing.T) {
		sel := baseSel(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web"))
		got := collectRows(t, wrapped.Select(t.Context(), sel))
		var lines []string
		for _, r := range got {
			lines = append(lines, r.Line)
		}
		want := []string{"one token here", "three secret lowercase", "web SECRET stays"}
		if !slices.Equal(lines, want) {
			t.Fatalf("lines = %v, want %v", lines, want)
		}
	})
	t.Run("labels and ts immutable", func(t *testing.T) {
		got := collectRows(t, wrapped.Select(t.Context(), api))
		// The surviving stored rows are +0s and +2s; their timestamps must be
		// exactly the originals, not merely µs precision or UTC.
		wantTS := []time.Time{t0, t0.Add(2 * time.Second)}
		if len(got) != len(wantTS) {
			t.Fatalf("rows = %d (%v), want %d", len(got), got, len(wantTS))
		}
		for i, r := range got {
			if !r.TS.Equal(wantTS[i]) {
				t.Fatalf("row %d TS = %s, want exactly %s", i, r.TS, wantTS[i])
			}
			want := apiLabels("api")
			if r.Labels.String() != logs.FromMap(want).String() {
				t.Fatalf("row labels = %s, want the stored set", r.Labels)
			}
		}
	})
	t.Run("other tenant delegates exactly", func(t *testing.T) {
		m := newMem()
		m.Add(tenant.ID("other"), row(0, "other SECRET row", apiLabels("api"), nil))
		rec := &recordingSource{rows: []plan.Row{row(0, "other SECRET row", apiLabels("api"), nil)}}
		// The other tenant has no read definitions, so the wrapper delegates
		// even though the default tenant has some.
		provider := &countProvider{snap: emptySnapshot(t)}
		w, err := NewReadSource(rec, provider, testStreamLabels)
		if err != nil {
			t.Fatal(err)
		}
		sel := baseSel(eq("service_name", "api"))
		sel.Tenant = tenant.ID("other")
		sel.LineFilters = []plan.LineFilter{contains("SECRET")}
		sel.Limit = 5
		got := collectRows(t, w.Select(t.Context(), sel))
		if len(got) != 1 || got[0].Line != "other SECRET row" {
			t.Fatalf("rows = %v, want untransformed delegation", got)
		}
		if calls := rec.selectCalls(); len(calls) != 1 || calls[0].sig != selSig(sel) {
			t.Fatalf("delegation = %+v, want exact", calls)
		}
		if calls, tenants := provider.stats(); calls != 1 || tenants[0] != tenant.ID("other") {
			t.Fatalf("provider = %d calls for %v, want one load for the other tenant", calls, tenants)
		}
	})
}

func TestReadSourceErrorPolicies(t *testing.T) {
	newMem := func() *plan.MemSource {
		m := plan.NewMemSource()
		m.Add(tenant.Default,
			row(0, "not-json", apiLabels("api"), nil),
			row(time.Second, `{"v":"1"}`, apiLabels("api"), nil),
		)
		return m
	}
	api := baseSel(eq("service_name", "api"))

	t.Run("default keep marks without failing", func(t *testing.T) {
		parse := compileRead(t, "parse", "*", Step{Type: StepParseJSON})
		wrapped, _ := mustWrap(t, newMem(), mustSnapshot(t, parse))
		got := collectRows(t, wrapped.Select(t.Context(), api))
		if len(got) != 2 {
			t.Fatalf("rows = %d, want both kept", len(got))
		}
		v, ok := got[0].Meta.Get(ErrField)
		if !ok || v != string(StepParseJSON) {
			t.Fatalf("markers = %v, want pipeline_error=%s", got[0].Meta, StepParseJSON)
		}
		if v, ok := got[0].Meta.Get(ErrDetailsField); !ok || v == "" {
			t.Fatalf("details marker missing: %v", got[0].Meta)
		}
		if _, ok := got[1].Meta.Get(ErrField); ok {
			t.Fatalf("good row marked: %v", got[1].Meta)
		}
	})
	t.Run("stop halts remaining pipelines", func(t *testing.T) {
		parse := compileRead(t, "a-parse", "*", Step{Type: StepParseJSON})
		redact := compileRead(t, "b-redact", "*", Step{Type: StepRedact, Pattern: "not-json", Replace: new("gone")})
		wrapped, _ := mustWrap(t, newMem(), mustSnapshot(t, parse, redact))
		got := collectRows(t, wrapped.Select(t.Context(), api))
		if got[0].Line != "not-json" {
			t.Fatalf("line = %q, want unchanged (b must not run after a stops)", got[0].Line)
		}
	})
	t.Run("reject fails the query", func(t *testing.T) {
		parse := compileRead(t, "parse", "*", Step{Type: StepParseJSON, OnError: OnErrorReject})
		wrapped, _ := mustWrap(t, newMem(), mustSnapshot(t, parse))
		var sawReject bool
		for _, err := range wrapped.Select(t.Context(), api) {
			if err == nil {
				t.Fatal("reject policy: no error")
			}
			_, sawReject = errors.AsType[*RejectError](err)
			if !strings.Contains(err.Error(), "parse") {
				t.Fatalf("err = %v, want the pipeline named", err)
			}
		}
		if !sawReject {
			t.Fatal("error does not wrap *RejectError")
		}
		for _, err := range wrapped.Aggregate(t.Context(), api, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
			if err == nil {
				t.Fatal("aggregate: no error")
			}
			return
		}
		t.Fatal("aggregate: iterator ended without the reject error")
	})
}

func TestReadSourceAggregateTransforms(t *testing.T) {
	// rename moves trace→run so IncludeMeta grouping changes; redact makes two
	// distinct lines equal; drop_if removes rows before grouping.
	rename := compileRead(t, "a-rename", "api", Step{Type: StepRename, From: "trace", To: "run"})
	redact := compileRead(t, "a-redact", "red", Step{Type: StepRedact, Pattern: "[0-9]+", Replace: new("N")})
	dropIf := compileRead(t, "a-drop", "drop", Step{Type: StepDropIf, Expr: `line contains "dropme"`})

	t.Run("groups by transformed metadata", func(t *testing.T) {
		m := plan.NewMemSource()
		m.Add(tenant.Default,
			row(0, "x", apiLabels("api"), map[string]string{"trace": "t1"}),
			row(time.Second, "x", apiLabels("api"), map[string]string{"trace": "t2"}),
			row(2*time.Second, "x", apiLabels("api"), nil),
		)
		wrapped, _ := mustWrap(t, m, mustSnapshot(t, rename))
		agg := plan.BucketAgg{Width: time.Minute, Origin: t0, IncludeMeta: true}
		got := collectBuckets(t, wrapped.Aggregate(t.Context(), baseSel(eq("service_name", "api")), agg))
		var keys []string
		for _, b := range got {
			keys = append(keys, b.Meta.String())
		}
		// MemSource order: Labels.String then Meta.String, so the empty
		// metadata set "{}" comes last.
		want := []string{`{run="t1"}`, `{run="t2"}`, "{}"}
		if !slices.Equal(keys, want) {
			t.Fatalf("meta groups = %v, want %v", keys, want)
		}
		for _, b := range got {
			if b.Count != 1 {
				t.Fatalf("bucket %s count = %d, want 1", bucketKey(b), b.Count)
			}
		}
		// Without IncludeMeta the rename must not split buckets.
		agg.IncludeMeta = false
		got = collectBuckets(t, wrapped.Aggregate(t.Context(), baseSel(eq("service_name", "api")), agg))
		if len(got) != 1 || got[0].Count != 3 {
			t.Fatalf("no-meta buckets = %v, want one bucket of 3", got)
		}
		if got[0].Meta != nil {
			t.Fatalf("bucket meta = %v, want nil without IncludeMeta", got[0].Meta)
		}
	})
	t.Run("rewritten collisions still count", func(t *testing.T) {
		m := plan.NewMemSource()
		// The two distinct input lines share TS, labels and metadata and
		// differ only in bytes the redact step rewrites to the same output;
		// the stored duplicate of the first line is still one raw row.
		m.Add(tenant.Default,
			row(0, "req 12 ok", apiLabels("red"), nil),
			row(0, "req 34 ok", apiLabels("red"), nil),
			row(0, "req 12 ok", apiLabels("red"), nil),
		)
		wrapped, _ := mustWrap(t, m, mustSnapshot(t, redact))
		got := collectBuckets(t, wrapped.Aggregate(t.Context(), baseSel(eq("service_name", "red")), plan.BucketAgg{Width: time.Minute, Origin: t0}))
		if len(got) != 1 || got[0].Count != 2 {
			t.Fatalf("bucket = %v, want Count=2 (distinct rows, duplicate stored once)", got)
		}
		if got[0].Bytes != int64(len("req N ok"))*2 {
			t.Fatalf("bytes = %d, want twice the rewritten line", got[0].Bytes)
		}
	})
	t.Run("utf8 byte totals and transformed lines", func(t *testing.T) {
		// The rewrite replaces an ASCII run with three UTF-8 bytes, so the
		// total must be the byte length of the transformed line, not the
		// input line and not a rune count.
		utf8Redact := compileRead(t, "utf8", "red", Step{Type: StepRedact, Pattern: "[0-9]+", Replace: new("☕")})
		m := plan.NewMemSource()
		m.Add(tenant.Default, row(0, "café 7 zut", apiLabels("red"), nil))
		wrapped, _ := mustWrap(t, m, mustSnapshot(t, utf8Redact))
		got := collectBuckets(t, wrapped.Aggregate(t.Context(), baseSel(eq("service_name", "red")), plan.BucketAgg{Width: time.Minute, Origin: t0}))
		if len(got) != 1 || got[0].Bytes != int64(len("café ☕ zut")) {
			t.Fatalf("bytes = %v, want the byte length of the rewritten utf-8 line", got)
		}
	})
	t.Run("drop and filter apply before grouping", func(t *testing.T) {
		m := plan.NewMemSource()
		m.Add(tenant.Default,
			row(0, "keep 1", apiLabels("drop"), nil),
			row(time.Second, "dropme 2", apiLabels("drop"), nil),
			row(2*time.Second, "keep 3", apiLabels("drop"), nil),
		)
		wrapped, _ := mustWrap(t, m, mustSnapshot(t, dropIf))
		sel := baseSel(eq("service_name", "drop"))
		sel.LineFilters = []plan.LineFilter{contains("keep")}
		got := collectBuckets(t, wrapped.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: t0}))
		if len(got) != 1 || got[0].Count != 2 {
			t.Fatalf("bucket = %v, want the two kept rows", got)
		}
	})
	t.Run("negative bucket starts", func(t *testing.T) {
		m := plan.NewMemSource()
		m.Add(tenant.Default,
			row(0, "a", apiLabels("api"), nil),
			row(time.Second, "b", apiLabels("api"), nil),
		)
		wrapped, _ := mustWrap(t, m, mustSnapshot(t, rename))
		// Origin after all data: every bucket start is negative relative to it.
		agg := plan.BucketAgg{Width: 10 * time.Second, Origin: t0.Add(time.Minute)}
		got := collectBuckets(t, wrapped.Aggregate(t.Context(), baseSel(eq("service_name", "api")), agg))
		if len(got) != 1 {
			t.Fatalf("buckets = %v, want one", got)
		}
		if want := agg.Start(t0); !got[0].Start.Equal(want) || !got[0].Start.Before(agg.Origin) {
			t.Fatalf("start = %v, want %v before the origin", got[0].Start, want)
		}
	})
}

// TestReadSourceAggregateOrderMatchesMemSource pins the Source contract's
// bucket order: Start, then Labels.String, then Meta.String. In particular the
// empty metadata set renders as "{}", which sorts after a non-empty set, so a
// wrapper that special-cases it to sort first is observable through a no-op
// read pipeline.
func TestReadSourceAggregateOrderMatchesMemSource(t *testing.T) {
	m := plan.NewMemSource()
	m.Add(tenant.Default,
		row(0, "no meta", apiLabels("api"), nil),
		row(time.Second, "meta one", apiLabels("api"), map[string]string{"trace": "t1"}),
		row(2*time.Second, "meta two", apiLabels("api"), map[string]string{"trace": "t2"}),
	)
	noop := compileRead(t, "noop", "*", Step{Type: StepDropIf, Expr: `line == "never"`})
	wrapped, _ := mustWrap(t, m, mustSnapshot(t, noop))
	sel := baseSel(eq("service_name", "api"))
	agg := plan.BucketAgg{Width: time.Minute, Origin: t0, IncludeMeta: true}

	raw := collectBuckets(t, m.Aggregate(t.Context(), sel, agg))
	got := collectBuckets(t, wrapped.Aggregate(t.Context(), sel, agg))
	if len(got) != len(raw) || len(raw) != 3 {
		t.Fatalf("buckets = %d (wrapper) / %d (MemSource), want 3", len(got), len(raw))
	}
	for i := range raw {
		if bucketKey(got[i]) != bucketKey(raw[i]) {
			t.Fatalf("bucket %d = %s, want MemSource order/contents %s", i, bucketKey(got[i]), bucketKey(raw[i]))
		}
	}
	// Reference order is non-empty metadata first, then the empty set.
	if last := raw[len(raw)-1].Meta.String(); last != "{}" {
		t.Fatalf("last MemSource bucket metadata = %s, want the empty set", last)
	}
}

func TestReadSourceFailures(t *testing.T) {
	redact := compileRead(t, "redact", "*", Step{Type: StepRedact, Pattern: "x", Replace: new("y")})
	api := baseSel(eq("service_name", "api"))

	t.Run("canceled before the provider", func(t *testing.T) {
		rec := &recordingSource{}
		wrapped, provider := mustWrap(t, rec, mustSnapshot(t, redact))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var selectSaw, aggregateSaw bool
		for _, err := range wrapped.Select(ctx, api) {
			selectSaw = true
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("select err = %v, want context.Canceled", err)
			}
		}
		if !selectSaw {
			t.Fatal("Select yielded no error for a canceled context")
		}
		for _, err := range wrapped.Aggregate(ctx, api, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
			aggregateSaw = true
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("aggregate err = %v, want context.Canceled", err)
			}
		}
		if !aggregateSaw {
			t.Fatal("Aggregate yielded no error for a canceled context")
		}
		if calls, _ := provider.stats(); calls != 0 {
			t.Fatalf("provider calls = %d, want 0", calls)
		}
		if n := rec.finishedSelects(); n != 0 {
			t.Fatalf("raw selects = %d, want 0", n)
		}
	})
	t.Run("provider error", func(t *testing.T) {
		boom := errors.New("catalog down")
		wrapped, provider := mustWrap(t, &recordingSource{}, emptySnapshot(t))
		provider.set(nil, boom)
		var saw bool
		for _, err := range wrapped.Select(t.Context(), api) {
			saw = true
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the provider error", err)
			}
		}
		if !saw {
			t.Fatal("Select yielded no error for a provider failure")
		}
	})
	t.Run("nil snapshot", func(t *testing.T) {
		wrapped, provider := mustWrap(t, &recordingSource{}, emptySnapshot(t))
		var nilSnap Snapshot
		provider.set(nilSnap, nil)
		var saw bool
		for _, err := range wrapped.Select(t.Context(), api) {
			saw = true
			if err == nil || !strings.Contains(err.Error(), "nil snapshot") {
				t.Fatalf("err = %v, want nil snapshot", err)
			}
		}
		if !saw {
			t.Fatal("Select yielded no error for a nil snapshot")
		}
	})
	t.Run("source error mid-iteration", func(t *testing.T) {
		boom := errors.New("scan failed")
		rec := &recordingSource{rows: []plan.Row{row(0, "x", apiLabels("api"), nil)}, rowsErr: boom}
		wrapped, _ := mustWrap(t, rec, mustSnapshot(t, redact))
		var got int
		for _, err := range wrapped.Select(t.Context(), api) {
			if err != nil {
				if !errors.Is(err, boom) {
					t.Fatalf("err = %v, want the source error", err)
				}
				if got != 1 {
					t.Fatalf("rows before the source error = %d, want the one row the source yielded", got)
				}
				return
			}
			got++
		}
		t.Fatal("iteration ended without the source error")
	})
	t.Run("zero rows canceled select", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		rec := &cancelAfterEmptySource{cancel: cancel}
		wrapped, _ := mustWrap(t, rec, mustSnapshot(t, redact))
		var saw bool
		for _, err := range wrapped.Select(ctx, api) {
			saw = true
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled on the empty path", err)
			}
		}
		if !saw {
			t.Fatal("Select yielded no error after the scan canceled the context")
		}
	})
	t.Run("zero rows canceled aggregate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		rec := &cancelAfterEmptySource{cancel: cancel}
		wrapped, _ := mustWrap(t, rec, mustSnapshot(t, redact))
		var saw bool
		for _, err := range wrapped.Aggregate(ctx, api, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
			saw = true
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("aggregate err = %v, want context.Canceled", err)
			}
		}
		if !saw {
			t.Fatal("Aggregate yielded no error after the scan canceled the context")
		}
	})
	t.Run("early break releases the raw iterator", func(t *testing.T) {
		rec := &recordingSource{rows: []plan.Row{
			row(0, "a", apiLabels("api"), nil),
			row(time.Second, "b", apiLabels("api"), nil),
		}}
		wrapped, _ := mustWrap(t, rec, mustSnapshot(t, redact))
		for range 20 {
			for range wrapped.Select(t.Context(), api) {
				break
			}
			for range wrapped.Aggregate(t.Context(), api, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
				break
			}
		}
		if n := rec.finishedSelects(); n != 40 {
			t.Fatalf("raw iterations finalized = %d, want 40", n)
		}
	})
	t.Run("invalid arguments never reach the provider", func(t *testing.T) {
		rec := &recordingSource{}
		wrapped, provider := mustWrap(t, rec, mustSnapshot(t, redact))
		bad := api
		bad.Matchers = nil
		var saw bool
		for _, err := range wrapped.Select(t.Context(), bad) {
			saw = true
			if err == nil {
				t.Fatal("no error for an invalid selection")
			}
		}
		if !saw {
			t.Fatal("Select yielded no error for an invalid selection")
		}
		bad = api
		bad.Tenant = "a/b"
		saw = false
		for _, err := range wrapped.Aggregate(t.Context(), bad, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
			saw = true
			if err == nil {
				t.Fatal("no error for an invalid tenant")
			}
		}
		if !saw {
			t.Fatal("Aggregate yielded no error for an invalid tenant")
		}
		saw = false
		for _, err := range wrapped.Aggregate(t.Context(), api, plan.BucketAgg{Width: 0}) {
			saw = true
			if err == nil {
				t.Fatal("no error for an invalid bucket width")
			}
		}
		if !saw {
			t.Fatal("Aggregate yielded no error for an invalid bucket width")
		}
		if calls, _ := provider.stats(); calls != 0 {
			t.Fatalf("provider calls = %d, want 0", calls)
		}
		if calls, aggs := len(rec.selectCalls()), rec.aggregateCalls(); calls+aggs != 0 {
			t.Fatalf("raw calls = %d+%d, want none", calls, aggs)
		}
	})
}

// compileCancelOnApply compiles a read pipeline whose ObserveExecution hook
// cancels the query from inside Apply, before Apply returns to the wrapper.
func compileCancelOnApply(t *testing.T, cancel context.CancelFunc) *Compiled {
	t.Helper()
	c, err := Compile(Definition{
		Name: "observe-cancel", Stage: StageRead, Selector: "*",
		Steps: []Step{{Type: StepRedact, Pattern: "SECRET", Replace: new("token")}},
	}, CompileOptions{
		StreamLabels: testStreamLabels,
		ObserveExecution: func(Stage, Outcome, error, time.Duration) {
			cancel()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestReadSourceObserveExecutionCancellation(t *testing.T) {
	sel := baseSel(eq("service_name", "api"))
	agg := plan.BucketAgg{Width: time.Minute, Origin: t0}
	rows := []plan.Row{row(0, "one SECRET", apiLabels("api"), nil)}

	t.Run("select", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		wrapped, _ := mustWrap(t, &recordingSource{rows: rows}, mustSnapshot(t, compileCancelOnApply(t, cancel)))
		var got int
		var gotErr error
		for _, err := range wrapped.Select(ctx, sel) {
			if err != nil {
				gotErr = err
				break
			}
			got++
		}
		if got != 0 {
			t.Fatalf("rows = %d, want none after ObserveExecution canceled Apply", got)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", gotErr)
		}
	})

	t.Run("aggregate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		wrapped, _ := mustWrap(t, &recordingSource{rows: rows}, mustSnapshot(t, compileCancelOnApply(t, cancel)))
		var got int
		var gotErr error
		for _, err := range wrapped.Aggregate(ctx, sel, agg) {
			if err != nil {
				gotErr = err
				break
			}
			got++
		}
		if got != 0 {
			t.Fatalf("buckets = %d, want none after ObserveExecution canceled Apply", got)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", gotErr)
		}
	})
}

// cancelingSnapshot cancels the query from For, emulating a transform that
// only notices cancellation at one of its step boundaries.
type cancelingSnapshot struct {
	Snapshot
	cancel context.CancelFunc
}

func (s *cancelingSnapshot) For(stage Stage, dataset string) []*Compiled {
	s.cancel()
	return s.Snapshot.For(stage, dataset)
}

func TestReadSourceCancellationAfterTransform(t *testing.T) {
	// A read definition exists, but none matches the row's dataset, so
	// transformRow takes the unmatched no-Apply path. Cancellation must still
	// be checked after For returns, before the row is yielded or counted.
	read := compileRead(t, "elsewhere", "elsewhere", Step{Type: StepRedact, Pattern: "x", Replace: new("y")})
	sel := baseSel(eq("service_name", "api"))
	agg := plan.BucketAgg{Width: time.Minute, Origin: t0}

	t.Run("select unmatched For", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		raw := &recordingSource{rows: []plan.Row{row(0, "x", apiLabels("api"), nil)}}
		base := mustSnapshot(t, read)
		wrapped, err := NewReadSource(raw, &countProvider{snap: &cancelingSnapshot{Snapshot: base, cancel: cancel}}, testStreamLabels)
		if err != nil {
			t.Fatal(err)
		}
		var gotErr error
		var rows int
		for _, err := range wrapped.Select(ctx, sel) {
			if err != nil {
				gotErr = err
				break
			}
			rows++
		}
		if rows != 0 {
			t.Fatalf("rows = %d, want none after cancellation", rows)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", gotErr)
		}
	})

	t.Run("aggregate unmatched For", func(t *testing.T) {
		// The raw source reports a distinct sentinel after its row. The
		// post-transform context check must stop before pulling that error:
		// checking cancellation only at the end of the scan would let the raw
		// error mask context.Canceled and hide the missing early stop.
		sentinel := errors.New("raw scan error after the canceled row")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		raw := &recordingSource{
			rows:    []plan.Row{row(0, "x", apiLabels("api"), nil)},
			rowsErr: sentinel,
		}
		base := mustSnapshot(t, read)
		wrapped, err := NewReadSource(raw, &countProvider{snap: &cancelingSnapshot{Snapshot: base, cancel: cancel}}, testStreamLabels)
		if err != nil {
			t.Fatal(err)
		}
		var gotErr error
		var buckets int
		for _, err := range wrapped.Aggregate(ctx, sel, agg) {
			if err != nil {
				gotErr = err
				break
			}
			buckets++
		}
		if buckets != 0 {
			t.Fatalf("buckets = %d, want none after cancellation", buckets)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", gotErr)
		}
		if errors.Is(gotErr, sentinel) {
			t.Fatalf("raw error masked cancellation: %v", gotErr)
		}
	})
}

func TestReadSourceObserverCancellation(t *testing.T) {
	redact := compileRead(t, "redact", "*", Step{Type: StepRedact, Pattern: "SECRET", Replace: new("token")})
	sel := baseSel(eq("service_name", "api"))
	agg := plan.BucketAgg{Width: time.Minute, Origin: t0}

	t.Run("select", func(t *testing.T) {
		raw := &recordingSource{rows: []plan.Row{
			row(0, "one SECRET", apiLabels("api"), nil),
			row(time.Second, "two SECRET", apiLabels("api"), nil),
		}}
		wrapped, _ := mustWrap(t, raw, mustSnapshot(t, redact))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var rows int
		var gotErr error
		for r, err := range wrapped.Select(ctx, sel) {
			if err != nil {
				gotErr = err
				break
			}
			if r.Line != "one token" {
				t.Fatalf("first line = %q", r.Line)
			}
			rows++
			cancel()
		}
		if rows != 1 {
			t.Fatalf("rows = %d, want the one row yielded before cancellation", rows)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled instead of more rows", gotErr)
		}
	})

	t.Run("aggregate", func(t *testing.T) {
		raw := &recordingSource{rows: []plan.Row{
			row(0, "one SECRET", apiLabels("api"), nil),
			row(time.Second, "two SECRET", apiLabels("api"), nil),
		}}
		wrapped, _ := mustWrap(t, raw, mustSnapshot(t, redact))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var buckets int
		var gotErr error
		for _, err := range wrapped.Aggregate(ctx, sel, agg) {
			if err != nil {
				gotErr = err
				break
			}
			buckets++
			cancel()
		}
		if buckets != 1 {
			t.Fatalf("buckets = %d, want the one bucket yielded before cancellation", buckets)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled after the observer canceled", gotErr)
		}
	})
}

// cancelAfterEmptySource yields no rows and cancels ctx before ending, so a
// wrapper that would report success on an empty scan must be caught.
type cancelAfterEmptySource struct{ cancel context.CancelFunc }

func (s *cancelAfterEmptySource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Row{}, err)
			return
		}
		s.cancel()
	}
}

func (s *cancelAfterEmptySource) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if err := agg.Validate(); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		s.cancel()
	}
}

// reusingSource yields rows whose Labels and Meta share one backing array that
// is overwritten before every yield, the harshest legal reading of "may share
// backing arrays between rows". The wrapper must finish each row's matching,
// filtering and retention before the array moves on.
type reusingSource struct {
	rows   []plan.Row
	labels []logs.Label
	meta   []logs.Label
}

func newReusingSource(rows []plan.Row) *reusingSource {
	return &reusingSource{rows: rows, labels: make([]logs.Label, 0, 8), meta: make([]logs.Label, 0, 8)}
}

func (s *reusingSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Row{}, err)
			return
		}
		for _, r := range s.rows {
			if err := ctx.Err(); err != nil {
				yield(plan.Row{}, fmt.Errorf("reusing: %w", err))
				return
			}
			s.labels = append(s.labels[:0], r.Labels...)
			s.meta = append(s.meta[:0], r.Meta...)
			out := r
			out.Labels, out.Meta = s.labels, s.meta
			if !yield(out, nil) {
				return
			}
		}
	}
}

func (s *reusingSource) Aggregate(context.Context, plan.Selection, plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		yield(plan.Bucket{}, errors.New("reusing: aggregate must not be called"))
	}
}

func TestReadSourceReusedBackingArrays(t *testing.T) {
	// Rows alternate datasets; the pipeline matches api only. Matching must
	// read each row's own labels before the shared array is overwritten.
	rows := []plan.Row{
		row(0, "api one SECRET", apiLabels("api"), map[string]string{"k": "a"}),
		row(time.Second, "web one SECRET", apiLabels("web"), map[string]string{"k": "b"}),
		row(2*time.Second, "api two SECRET", apiLabels("api"), map[string]string{"k": "c"}),
		row(3*time.Second, "web two SECRET", apiLabels("web"), map[string]string{"k": "d"}),
	}
	redact := compileRead(t, "redact", "api", Step{Type: StepRedact, Pattern: "SECRET", Replace: new("token")})
	snap := mustSnapshot(t, redact)

	t.Run("aggregate owns retained arrays", func(t *testing.T) {
		wrapped, _ := mustWrap(t, newReusingSource(rows), snap)
		got := collectBuckets(t, wrapped.Aggregate(t.Context(),
			baseSel(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web")),
			plan.BucketAgg{Width: time.Minute, Origin: t0, IncludeMeta: true}))
		// One bucket per (stream, meta): the shared arrays must not collapse
		// them or leak the last row's content into every bucket.
		if len(got) != 4 {
			t.Fatalf("buckets = %d (%v), want 4 distinct", len(got), got)
		}
		// Each retained bucket must carry the labels and metadata of the row
		// that created it, not the shared array's latest contents.
		serviceOfMeta := map[string]string{"a": "api", "b": "web", "c": "api", "d": "web"}
		for _, b := range got {
			svc, _ := b.Labels.Get("service_name")
			meta, _ := b.Meta.Get("k")
			if want, ok := serviceOfMeta[meta]; !ok || svc != want {
				t.Fatalf("bucket %s mixes streams: labels %s meta %s", bucketKey(b), b.Labels, b.Meta)
			}
			if b.Count != 1 {
				t.Fatalf("bucket %s count = %d, want 1", bucketKey(b), b.Count)
			}
		}
	})

	t.Run("per-row matching and filtering use each row's data", func(t *testing.T) {
		wrapped, _ := mustWrap(t, newReusingSource(rows), snap)
		sel := baseSel(plan.MustMatcher(plan.MatchRegexp, "service_name", "api|web"))
		sel.LineFilters = []plan.LineFilter{contains("token")}
		// Only api rows are redacted, so only they pass the filter. Reading the
		// lines (immutable strings) proves each row was filtered as itself.
		var lines []string
		for r, err := range wrapped.Select(t.Context(), sel) {
			if err != nil {
				t.Fatal(err)
			}
			lines = append(lines, r.Line)
		}
		want := []string{"api one token", "api two token"}
		if !slices.Equal(lines, want) {
			t.Fatalf("lines = %v, want %v", lines, want)
		}
	})
}

func TestReadSourceSnapshotOncePerIteration(t *testing.T) {
	redact := compileRead(t, "redact", "*", Step{Type: StepRedact, Pattern: "x", Replace: new("y")})
	m := plan.NewMemSource()
	m.Add(tenant.Default, row(0, "x", apiLabels("api"), nil))
	wrapped, provider := mustWrap(t, m, mustSnapshot(t, redact))
	api := baseSel(eq("service_name", "api"))

	collectRows(t, wrapped.Select(t.Context(), api))
	if calls, _ := provider.stats(); calls != 1 {
		t.Fatalf("select loads = %d, want 1", calls)
	}
	collectBuckets(t, wrapped.Aggregate(t.Context(), api, plan.BucketAgg{Width: time.Minute, Origin: t0}))
	if calls, _ := provider.stats(); calls != 2 {
		t.Fatalf("loads after aggregate = %d, want 2", calls)
	}
	// An empty result still loads once: the decision needs the snapshot.
	none := baseSel(eq("service_name", "absent"))
	collectRows(t, wrapped.Select(t.Context(), none))
	if calls, _ := provider.stats(); calls != 3 {
		t.Fatalf("loads after empty select = %d, want 3", calls)
	}
}

func TestReadSourceHolderTenantMismatch(t *testing.T) {
	redact := compileRead(t, "redact", "*", Step{Type: StepRedact, Pattern: "x", Replace: new("y")})
	provider := &countProvider{snap: mustSnapshot(t, redact)}
	raw := &recordingSource{rows: []plan.Row{row(0, "x", apiLabels("api"), nil)}}
	wrapped, err := NewReadSource(raw, provider, testStreamLabels)
	if err != nil {
		t.Fatal(err)
	}

	// A holder pinned to another tenant is in the context (as WrapEngine
	// installs it): reading a different tenant must be an execution error, not
	// a second snapshot.
	holderProvider := &countProvider{snap: emptySnapshot(t)}
	holder := &readSnapshot{tenant: tenant.ID("pinned"), provider: holderProvider}
	ctx := context.WithValue(t.Context(), readCtxKey{}, holder)
	sel := baseSel(eq("service_name", "api"))

	firstErr := func(t *testing.T, seq iter.Seq2[plan.Row, error]) error {
		t.Helper()
		for _, err := range seq {
			return err
		}
		return nil
	}
	wantMismatch := func(t *testing.T, op string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), `pinned to tenant "pinned"`) {
			t.Fatalf("%s err = %v, want a tenant mismatch error", op, err)
		}
	}
	wantMismatch(t, "select", firstErr(t, wrapped.Select(ctx, sel)))

	var firstBucketErr error
	for _, err := range wrapped.Aggregate(ctx, sel, plan.BucketAgg{Width: time.Minute, Origin: t0}) {
		firstBucketErr = err
		break
	}
	wantMismatch(t, "aggregate", firstBucketErr)

	if calls, _ := provider.stats(); calls != 0 {
		t.Fatalf("wrapper provider calls = %d, want 0 on mismatch", calls)
	}
	if calls, _ := holderProvider.stats(); calls != 0 {
		t.Fatalf("holder provider calls = %d, want 0 on mismatch", calls)
	}
}
