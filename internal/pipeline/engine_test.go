package pipeline

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// engineCall records one stubEngine.Query call: the arguments the wrapped
// engine received and whether the per-query holder was present.
type engineCall struct {
	src    plan.Source
	params plan.Params
	holder *readSnapshot
}

// stubEngine is a plan.Engine double. Its Query hook runs the test's Source
// calls and its ParseSelector answer is controllable.
type stubEngine struct {
	mu sync.Mutex

	queries    []engineCall
	parseCalls []string
	parseOut   []plan.Matcher
	parseErr   error
	query      func(context.Context, plan.Source, plan.Params) (plan.Result, error)
}

func (e *stubEngine) Query(ctx context.Context, src plan.Source, p plan.Params) (plan.Result, error) {
	e.mu.Lock()
	holder, _ := ctx.Value(readCtxKey{}).(*readSnapshot)
	e.queries = append(e.queries, engineCall{src: src, params: p, holder: holder})
	hook := e.query
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, src, p)
	}
	return plan.Result{Type: plan.TypeScalar}, nil
}

func (e *stubEngine) ParseSelector(s string) ([]plan.Matcher, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.parseCalls = append(e.parseCalls, s)
	return slices.Clone(e.parseOut), e.parseErr
}

func (e *stubEngine) calls() []engineCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.queries)
}

func TestWrapEngineValidation(t *testing.T) {
	provider := &countProvider{snap: emptySnapshot(t)}
	engine := &stubEngine{}
	if _, err := WrapEngine(nil, provider); err == nil {
		t.Fatal("nil engine accepted")
	}
	if _, err := WrapEngine(engine, nil); err == nil {
		t.Fatal("nil provider accepted")
	}
}

func TestWrapEngineForwardsEverythingButInstallsHolder(t *testing.T) {
	p := &countProvider{snap: emptySnapshot(t)}
	inner := &stubEngine{
		parseOut: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "api")},
	}
	wrapped, err := WrapEngine(inner, p)
	if err != nil {
		t.Fatal(err)
	}
	// ParseSelector is the embedded method: same result, same call, no load.
	got, err := wrapped.ParseSelector(`{service_name="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	want, err := inner.ParseSelector(`{service_name="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(got, want, func(a, b plan.Matcher) bool { return a.String() == b.String() }) {
		t.Fatalf("ParseSelector = %v, want %v", got, want)
	}
	if calls, _ := p.stats(); calls != 0 {
		t.Fatalf("provider calls after ParseSelector = %d, want 0", calls)
	}

	src := &recordingSource{}
	params := plan.Params{Tenant: tenant.Default, Query: `{service_name="api"}`, Limit: 7}
	if _, err := wrapped.Query(t.Context(), src, params); err != nil {
		t.Fatal(err)
	}
	calls := inner.calls()
	if len(calls) != 1 {
		t.Fatalf("inner Query calls = %d, want 1", len(calls))
	}
	if calls[0].src != src || calls[0].params != params {
		t.Fatalf("inner got src=%p params=%+v, want passthrough", calls[0].src, calls[0].params)
	}
	if calls[0].holder == nil {
		t.Fatal("inner Query did not receive a per-query holder")
	}
	if calls[0].holder.tenant != tenant.Default || calls[0].holder.provider != p {
		t.Fatalf("holder = %+v, want tenant %q and the wrapper provider", calls[0].holder, tenant.Default)
	}
	// A query that never touches a Source does not load the provider.
	if calls, _ := p.stats(); calls != 0 {
		t.Fatalf("provider calls after scalar query = %d, want 0", calls)
	}
}

func TestWrapEnginePinsOneSnapshotPerQuery(t *testing.T) {
	raw := plan.NewMemSource()
	raw.Add(tenant.Default, row(0, "x", apiLabels("api"), nil))

	snapOne := mustSnapshot(t, compileRead(t, "rewrite", "*", Step{Type: StepRedact, Pattern: "x", Replace: new("one")}))
	snapTwo := mustSnapshot(t, compileRead(t, "rewrite", "*", Step{Type: StepRedact, Pattern: "x", Replace: new("two")}))
	provider := &countProvider{snap: snapOne}
	wrappedSrc, err := NewReadSource(raw, provider, testStreamLabels)
	if err != nil {
		t.Fatal(err)
	}

	sel := baseSel(eq("service_name", "api"))
	lines := func(ctx context.Context, src plan.Source) ([]string, error) {
		var out []string
		for r, err := range src.Select(ctx, sel) {
			if err != nil {
				return nil, err
			}
			out = append(out, r.Line)
		}
		return out, nil
	}

	inner := &stubEngine{}
	// The query's two Source calls see one pinned snapshot: the provider is
	// changed between them, exactly the binary-metric regression.
	inner.query = func(ctx context.Context, src plan.Source, _ plan.Params) (plan.Result, error) {
		first, err := lines(ctx, src)
		if err != nil {
			return plan.Result{}, err
		}
		provider.set(snapTwo, nil)
		second, err := lines(ctx, src)
		if err != nil {
			return plan.Result{}, err
		}
		if !slices.Equal(first, []string{"one"}) || !slices.Equal(second, []string{"one"}) {
			t.Errorf("query lines = %v then %v, want one snapshot [one]", first, second)
		}
		return plan.Result{Type: plan.TypeScalar}, nil
	}
	wrapped, err := WrapEngine(inner, provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Query(t.Context(), wrappedSrc, plan.Params{Tenant: tenant.Default}); err != nil {
		t.Fatal(err)
	}
	if calls, _ := provider.stats(); calls != 1 {
		t.Fatalf("provider calls in one query = %d, want 1", calls)
	}

	// A separate query gets a new holder and sees the new provider snapshot.
	inner.mu.Lock()
	inner.query = func(ctx context.Context, src plan.Source, _ plan.Params) (plan.Result, error) {
		got, err := lines(ctx, src)
		if err == nil && !slices.Equal(got, []string{"two"}) {
			t.Errorf("second query lines = %v, want [two]", got)
		}
		return plan.Result{Type: plan.TypeScalar}, err
	}
	inner.mu.Unlock()
	if _, err := wrapped.Query(t.Context(), wrappedSrc, plan.Params{Tenant: tenant.Default}); err != nil {
		t.Fatal(err)
	}
	if calls, _ := provider.stats(); calls != 2 {
		t.Fatalf("provider calls after second query = %d, want 2", calls)
	}

	// The holder never outlives Query: a direct Source call loads again.
	if _, err := lines(t.Context(), wrappedSrc); err != nil {
		t.Fatal(err)
	}
	if calls, _ := provider.stats(); calls != 3 {
		t.Fatalf("provider calls after direct call = %d, want 3", calls)
	}
}

func TestWrapEngineCachesProviderErrorPerQuery(t *testing.T) {
	boom := errors.New("catalog down")
	provider := &countProvider{err: boom}
	raw := plan.NewMemSource()
	wrappedSrc, err := NewReadSource(raw, provider, testStreamLabels)
	if err != nil {
		t.Fatal(err)
	}
	sel := baseSel(eq("service_name", "api"))
	inner := &stubEngine{}
	inner.query = func(ctx context.Context, src plan.Source, _ plan.Params) (plan.Result, error) {
		var errs []error
		for range 2 {
			for _, err := range src.Select(ctx, sel) {
				errs = append(errs, err)
			}
		}
		if len(errs) != 2 || !errors.Is(errs[0], boom) || !errors.Is(errs[1], boom) {
			t.Errorf("errors = %v, want the same provider error twice", errs)
		}
		return plan.Result{Type: plan.TypeScalar}, nil
	}
	wrapped, err := WrapEngine(inner, provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Query(t.Context(), wrappedSrc, plan.Params{Tenant: tenant.Default}); err != nil {
		t.Fatal(err)
	}
	if calls, _ := provider.stats(); calls != 1 {
		t.Fatalf("provider calls = %d, want one cached failure per query", calls)
	}
}

// hookSource runs hook once, at the first Select, then delegates. It lets the
// test change the provider between the two Source calls of one LogQL binary
// metric query.
type hookSource struct {
	plan.Source
	once sync.Once
	hook func()
}

func (s *hookSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	s.once.Do(s.hook)
	return s.Source.Select(ctx, sel)
}

// TestWrapEnginePinsSnapshotForLogQLBinaryQuery is the end-to-end version of
// the binary-query regression: a real logql.Engine evaluates two Source calls
// in one query, the provider switches snapshots after the first call, and the
// whole query must still use the original snapshot. A separate query gets a
// new holder and therefore the new snapshot.
func TestWrapEnginePinsSnapshotForLogQLBinaryQuery(t *testing.T) {
	rawMem := plan.NewMemSource()
	// All rows are strictly before the instant query's t0: LogQL range
	// windows are [start, end) after the exclusive-end change.
	rawMem.Add(tenant.Default,
		row(-5*time.Second, "api a", apiLabels("api"), nil),
		row(-4*time.Second, "api b", apiLabels("api"), nil),
		row(-3*time.Second, "web a", apiLabels("web"), nil),
		row(-2*time.Second, "web b", apiLabels("web"), nil),
		row(-time.Second, "web c", apiLabels("web"), nil),
	)
	keepAll := mustSnapshot(t, compileRead(t, "keep", "*", Step{Type: StepDropIf, Expr: `line == "never"`}))
	dropAll := mustSnapshot(t, compileRead(t, "drop", "*", Step{Type: StepDropIf, Expr: `line != ""`}))
	provider := &countProvider{snap: keepAll}
	raw := &hookSource{Source: rawMem, hook: func() { provider.set(dropAll, nil) }}
	wrappedSrc, err := NewReadSource(raw, provider, testStreamLabels)
	if err != nil {
		t.Fatal(err)
	}

	query := `sum(count_over_time({service_name="api"}[1m])) + sum(count_over_time({service_name="web"}[1m]))`
	params := plan.Params{Tenant: tenant.Default, Query: query, Start: t0, End: t0, Direction: plan.Forward}
	engine, err := WrapEngine(logql.New(config.LogQL{MaxEntries: 5000, MaxSeries: 500}), provider)
	if err != nil {
		t.Fatal(err)
	}

	value := func(t *testing.T, res plan.Result) float64 {
		t.Helper()
		if res.Type != plan.TypeVector || len(res.Vector) != 1 {
			t.Fatalf("result = %+v, want one vector sample", res)
		}
		return res.Vector[0].V
	}
	// Pinned: api (2) + web (3) from the original snapshot, even though the
	// provider changed to drop-all after the first Source call.
	first, err := engine.Query(t.Context(), wrappedSrc, params)
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if got := value(t, first); got != 5 {
		t.Fatalf("first query value = %v, want 5 from the pinned snapshot", got)
	}
	if calls, _ := provider.stats(); calls != 1 {
		t.Fatalf("provider calls in one binary query = %d, want 1", calls)
	}
	// A separate query gets a fresh holder and observes the new snapshot,
	// which drops every row: the binary result is an empty vector.
	second, err := engine.Query(t.Context(), wrappedSrc, params)
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if second.Type != plan.TypeVector || len(second.Vector) != 0 {
		t.Fatalf("second query result = %+v, want the empty vector from the new snapshot", second)
	}
	if calls, _ := provider.stats(); calls != 2 {
		t.Fatalf("provider calls after two queries = %d, want 2", calls)
	}
}

func TestWrapEngineHolderDoesNotCrossQueries(t *testing.T) {
	provider := &countProvider{snap: emptySnapshot(t)}
	inner := &stubEngine{}
	wrapped, err := WrapEngine(inner, provider)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := wrapped.Query(t.Context(), &recordingSource{}, plan.Params{Tenant: tenant.ID("t1")}); err != nil {
			t.Fatal(err)
		}
	}
	calls := inner.calls()
	if len(calls) != 2 || calls[0].holder == calls[1].holder {
		t.Fatalf("holders = %+v, want one fresh holder per query", calls)
	}
	for i, c := range calls {
		if c.holder.tenant != tenant.ID("t1") {
			t.Fatalf("call %d holder tenant = %q, want t1", i, c.holder.tenant)
		}
	}
	// The stub did not touch a Source, and the providers were never loaded.
	if calls, _ := provider.stats(); calls != 0 {
		t.Fatalf("provider calls = %d, want 0", calls)
	}
}
