package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json/v2"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/pipeline"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	testStreamLabel = "service"
	testTenant      = tenant.ID("tenant-a")
)

// specJSON marshals the steps-only catalog document for a definition.
func specJSON(t *testing.T, steps ...pipeline.Step) []byte {
	t.Helper()
	b, err := json.Marshal(pipeline.Spec{Steps: steps})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return b
}

// keepStep extracts a metadata field and keeps the entry.
func keepStep() pipeline.Step {
	return pipeline.Step{Type: pipeline.StepRegexExtract, Pattern: `(?P<x>.*)`, From: "line"}
}

// dropStep drops every entry.
func dropStep() pipeline.Step {
	return pipeline.Step{Type: pipeline.StepDropIf, Expr: "true"}
}

// stopStep fails on a non-JSON line, exercising the default keep policy
// (OutcomeStop).
func stopStep() pipeline.Step {
	return pipeline.Step{Type: pipeline.StepParseJSON, From: "line"}
}

// mustPut writes one definition and returns the stored row.
func mustPut(t *testing.T, cat catalog.Catalog, id tenant.ID, name string, stage catalog.PipelineStage, selector string, version int64, steps ...pipeline.Step) catalog.PipelineDefinition {
	t.Helper()
	def, err := cat.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   id,
		Name:     name,
		Stage:    stage,
		Selector: selector,
		Spec:     specJSON(t, steps...),
	}, version)
	if err != nil {
		t.Fatalf("PutPipeline %s: %v", name, err)
	}
	return def
}

// newTestRegistry builds a registry with small polling intervals and closes it
// at test cleanup.
func newTestRegistry(t *testing.T, cat catalog.Catalog, role string, metrics prometheus.Registerer) *pipeline.Registry {
	t.Helper()
	if metrics == nil {
		metrics = prometheus.NewRegistry()
	}
	reg, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		PollInterval:   5 * time.Millisecond,
		ReloadTimeout:  2 * time.Second,
		Role:           role,
		Metrics:        metrics,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { reg.Close() })
	return reg
}

// runRegistry starts Run in the background and cancels it at cleanup.
func runRegistry(t *testing.T, reg *pipeline.Registry) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- reg.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// names returns the sorted pipeline names For selects.
func names(snap pipeline.Snapshot, stage pipeline.Stage, dataset string) []string {
	defs := snap.For(stage, dataset)
	out := make([]string, 0, len(defs))
	for _, c := range defs {
		out = append(out, c.Name())
	}
	slices.Sort(out)
	return out
}

func TestRegistryFirstUseAndTenantIsolation(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	mustPut(t, cat, testTenant, "r", catalog.PipelineStageRead, "app", 0, keepStep())

	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	ctx := t.Context()

	snap, err := reg.Snapshot(ctx, testTenant)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snap.Has(pipeline.StageIngest) || !snap.Has(pipeline.StageRead) {
		t.Fatalf("Has = ingest %v, read %v, want both true", snap.Has(pipeline.StageIngest), snap.Has(pipeline.StageRead))
	}
	if got := names(snap, pipeline.StageIngest, "anything"); !slices.Equal(got, []string{"a"}) {
		t.Errorf("ingest For = %v, want [a]", got)
	}
	if got := names(snap, pipeline.StageRead, "app"); !slices.Equal(got, []string{"r"}) {
		t.Errorf("read For(app) = %v, want [r]", got)
	}
	if got := names(snap, pipeline.StageRead, "other"); len(got) != 0 {
		t.Errorf("read For(other) = %v, want none", got)
	}

	// For returns a clone: mutating it must not affect later calls.
	cloned := snap.For(pipeline.StageIngest, "anything")
	cloned[0] = nil
	if got := names(snap, pipeline.StageIngest, "anything"); !slices.Equal(got, []string{"a"}) {
		t.Errorf("ingest For after mutating a previous result = %v, want [a]", got)
	}

	// Another tenant sees nothing and never inherits tenant-a's definitions.
	other, err := reg.Snapshot(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("Snapshot other tenant: %v", err)
	}
	if other.Has(pipeline.StageIngest) || other.Has(pipeline.StageRead) {
		t.Error("tenant-b inherited tenant-a's definitions")
	}
	if got := names(other, pipeline.StageIngest, "anything"); len(got) != 0 {
		t.Errorf("tenant-b For = %v, want none", got)
	}
	if !snap.Has(pipeline.StageIngest) {
		t.Error("tenant-a snapshot changed after loading tenant-b")
	}

	if _, err := reg.Snapshot(ctx, "bad tenant!"); err == nil {
		t.Error("Snapshot with an invalid tenant succeeded, want error")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reg.Snapshot(canceled, testTenant); !errors.Is(err, context.Canceled) {
		t.Errorf("Snapshot with a canceled context = %v, want context.Canceled", err)
	}
}

// countingCatalog counts the registry's catalog calls.
type countingCatalog struct {
	catalog.Catalog
	tokens atomic.Int64
	lists  atomic.Int64
}

func (c *countingCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	c.tokens.Add(1)
	return c.Catalog.PipelineChangeToken(ctx, t)
}

func (c *countingCatalog) ListPipelines(ctx context.Context, t tenant.ID) ([]catalog.PipelineDefinition, error) {
	c.lists.Add(1)
	return c.Catalog.ListPipelines(ctx, t)
}

func TestRegistryUnchangedTokenSkipsListAndCompile(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &countingCatalog{Catalog: base}

	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	if _, err := reg.Snapshot(t.Context(), testTenant); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("first load made %d ListPipelines calls, want 1", got)
	}
	runRegistry(t, reg)

	waitFor(t, 2*time.Second, "several polls", func() bool { return cat.tokens.Load() >= 5 })
	if got := cat.lists.Load(); got != 1 {
		t.Fatalf("unchanged token caused %d ListPipelines calls, want only the first load's 1", got)
	}

	// A real change is picked up on the next poll.
	mustPut(t, base, testTenant, "b", catalog.PipelineStageIngest, "*", 0, keepStep())
	waitFor(t, 2*time.Second, "the new definition", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && len(names(snap, pipeline.StageIngest, "app")) == 2
	})
	if got := cat.lists.Load(); got < 2 {
		t.Errorf("changed token made %d ListPipelines calls, want at least 2", got)
	}
}

func TestRegistryInvalidDefinitionRetainedAndRecovered(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	valid := mustPut(t, base, testTenant, "b", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &countingCatalog{Catalog: base}
	metricsReg := prometheus.NewRegistry()
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, metricsReg)
	snap, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("first snapshot = %v, want [a b]", got)
	}

	// The catalog stores specs opaquely; the registry's compile is what fails.
	bad, err := base.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   testTenant,
		Name:     "b",
		Stage:    catalog.PipelineStageIngest,
		Selector: "*",
		Spec:     []byte(`{"steps":[{"type":"bogus"}]}`),
	}, valid.Version)
	if err != nil {
		t.Fatalf("PutPipeline invalid spec: %v", err)
	}
	runRegistry(t, reg)

	// The failed token is retried on every poll, not cached as success.
	waitFor(t, 2*time.Second, "reload retries", func() bool { return cat.lists.Load() >= 4 })
	waitFor(t, 2*time.Second, "error reload metric", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "ingester", "result": "error"}) >= 1
	})
	if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("snapshot held by the caller changed after a failed reload: %v", got)
	}
	current, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot while invalid change pending: %v", err)
	}
	if got := names(current, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("current snapshot after failed reload = %v, want the good [a b]", got)
	}

	// Fixing the definition to a *different* behavior (b now selects only
	// "other") proves the reload published the fix: the old good snapshot had
	// both a and b matching "app".
	okBefore := sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
		map[string]string{"role": "ingester", "result": "ok"})
	if _, err := base.PutPipeline(t.Context(), catalog.PipelineDefinition{
		Tenant:   testTenant,
		Name:     "b",
		Stage:    catalog.PipelineStageIngest,
		Selector: "other",
		Spec:     specJSON(t, keepStep()),
	}, bad.Version); err != nil {
		t.Fatalf("PutPipeline fixed spec: %v", err)
	}
	waitFor(t, 2*time.Second, "the fixed definition", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		if err != nil || !snap.Has(pipeline.StageIngest) {
			return false
		}
		return slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"a"})
	})
	waitFor(t, 2*time.Second, "a successful reload after the fix", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "ingester", "result": "ok"}) > okBefore
	})
	// The per-tenant snapshot still carries every live definition; a failed
	// reload never published a partial one. The gauge is updated just after
	// publication, so wait for it.
	waitFor(t, 2*time.Second, "the active-definitions gauge", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_active_definitions",
			map[string]string{"role": "ingester"}) == 2
	})
}

func TestRegistryFirstLoadFailureAndRetry(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &failingCatalog{Catalog: base}
	cat.fail.Store(true)
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

	if snap, err := reg.Snapshot(t.Context(), testTenant); err == nil || snap != nil {
		t.Fatalf("Snapshot with a failing catalog = %v, %v, want nil, error", snap, err)
	}
	cat.fail.Store(false)
	snap, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot after the catalog recovered: %v", err)
	}
	if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a"}) {
		t.Errorf("snapshot after recovery = %v, want [a]", got)
	}
}

// failingCatalog fails change-token reads while fail is set.
type failingCatalog struct {
	catalog.Catalog
	fail atomic.Bool
}

func (c *failingCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	if c.fail.Load() {
		return 0, errors.New("catalog unavailable")
	}
	return c.Catalog.PipelineChangeToken(ctx, t)
}

func TestRegistryFirstLoadCancellation(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &blockingCatalog{Catalog: base, entered: make(chan struct{}), release: make(chan struct{})}
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

	first := make(chan error, 1)
	go func() {
		_, err := reg.Snapshot(t.Context(), testTenant)
		first <- err
	}()
	waitFor(t, 2*time.Second, "the blocked first load", func() bool {
		select {
		case <-cat.entered:
			return true
		default:
			return false
		}
	})

	// A second first-use waits for the same load; its own cancellation wins.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := reg.Snapshot(ctx, testTenant); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Snapshot waiting for an in-flight load = %v, want context.DeadlineExceeded", err)
	}

	close(cat.release)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Snapshot after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Snapshot did not finish after release")
	}
}

// blockingCatalog blocks change-token reads until release is closed or the
// caller's context ends.
type blockingCatalog struct {
	catalog.Catalog
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	c.once.Do(func() { close(c.entered) })
	select {
	case <-c.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return c.Catalog.PipelineChangeToken(ctx, t)
}

func TestRegistryCloseCancelsLoadAndRejectsNewSnapshots(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &blockingCatalog{Catalog: base, entered: make(chan struct{}), release: make(chan struct{})}
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

	loadDone := make(chan error, 1)
	go func() {
		_, err := reg.Snapshot(t.Context(), testTenant)
		loadDone <- err
	}()
	waitFor(t, 2*time.Second, "the load to start", func() bool {
		select {
		case <-cat.entered:
			return true
		default:
			return false
		}
	})

	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-loadDone:
		if !errors.Is(err, pipeline.ErrRegistryClosed) {
			t.Fatalf("in-flight load after Close = %v, want ErrRegistryClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the in-flight load")
	}
	// Release never happens: the test proves Close alone unblocked the load.
	if _, err := reg.Snapshot(t.Context(), testTenant); !errors.Is(err, pipeline.ErrRegistryClosed) {
		t.Errorf("Snapshot after Close = %v, want ErrRegistryClosed", err)
	}
	if err := reg.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestRegistryRunShutdown(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

	// Canceling the context returns nil promptly.
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- reg.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// Close also stops Run, and Run after Close returns immediately.
	runDone := make(chan error, 1)
	go func() { runDone <- reg.Run(t.Context()) }()
	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after Close = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}
	if err := reg.Run(t.Context()); err != nil {
		t.Errorf("Run on a closed registry = %v, want nil", err)
	}
}

func TestRegistryOneSnapshotPerRequest(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	a := mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	mustPut(t, cat, testTenant, "b", catalog.PipelineStageIngest, "*", 0, dropStep())

	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	held, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := names(held, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("initial snapshot = %v, want [a b]", got)
	}
	runRegistry(t, reg)

	// Mutate a definition and let the registry swap in a new snapshot.
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "other", a.Version, keepStep())
	waitFor(t, 2*time.Second, "the updated selector", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"b"})
	})
	// The held snapshot is immutable: the whole request keeps its old view.
	if got := names(held, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("held snapshot changed to %v after reload, want [a b]", got)
	}

	// Put a back on "*" so every later snapshot must contain both definitions.
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", a.Version+1, keepStep())
	waitFor(t, 2*time.Second, "a back on *", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"a", "b"})
	})

	// Concurrent readers always see a complete snapshot, never a partial swap.
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for ctx.Err() == nil {
				snap, err := reg.Snapshot(ctx, testTenant)
				if err != nil {
					return // shutdown: the test's readers stop here
				}
				got := names(snap, pipeline.StageIngest, "anything")
				if !slices.Contains(got, "a") || !slices.Contains(got, "b") {
					errs <- errors.New("snapshot missing a definition: " + errString(got))
					return
				}
			}
		})
	}
	// Flip a's steps while the readers run; the selector stays "*" so every
	// snapshot must contain both definitions.
	version := a.Version + 2
	for i := range 4 {
		step := keepStep()
		if i%2 == 1 {
			step = dropStep()
		}
		if _, err := cat.PutPipeline(t.Context(), catalog.PipelineDefinition{
			Tenant: testTenant, Name: "a", Stage: catalog.PipelineStageIngest,
			Selector: "*", Spec: specJSON(t, step),
		}, version); err != nil {
			t.Fatalf("mutation %d: %v", i, err)
		}
		version++
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	cancel()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent reader: %v", err)
	}
}

func errString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestRegistryMutationBetweenTokenAndList(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	count := &countingCatalog{Catalog: base}
	cat := &betweenCatalog{Catalog: count}
	var wrote atomic.Bool
	cat.between = func() {
		if wrote.CompareAndSwap(false, true) {
			// Lands after the token read but before the list: the reload stores
			// the OLD token beside a list that already contains the write.
			mustPut(t, base, testTenant, "late", catalog.PipelineStageIngest, "*", 0, keepStep())
		}
	}
	metricsReg := prometheus.NewRegistry()
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, metricsReg)

	snap, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"late"}) {
		t.Fatalf("first snapshot = %v, want the definition written after the token read", got)
	}
	if got := count.lists.Load(); got != 1 {
		t.Fatalf("first load made %d list calls, want 1", got)
	}

	// The stored token is the one read before the write, so the next poll sees
	// a newer token and reloads redundantly. That is the safe direction: a
	// later token must never be stamped on an earlier list.
	runRegistry(t, reg)
	waitFor(t, 2*time.Second, "the redundant reload", func() bool { return count.lists.Load() >= 2 })
	waitFor(t, 2*time.Second, "the redundant successful reload metric", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "ingester", "result": "ok"}) >= 2
	})
	snap, err = reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot after the redundant reload: %v", err)
	}
	if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"late"}) {
		t.Fatalf("snapshot after the redundant reload = %v, want [late]", got)
	}
}

// betweenCatalog runs between after each successful change-token read.
type betweenCatalog struct {
	catalog.Catalog
	once    sync.Once
	between func()
}

func (c *betweenCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	token, err := c.Catalog.PipelineChangeToken(ctx, t)
	if err == nil {
		c.once.Do(c.between)
	}
	return token, err
}

func TestRegistryTransientCatalogFailureRecoversWithoutNewWrite(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &flakyListCatalog{Catalog: base}
	metricsReg := prometheus.NewRegistry()
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, metricsReg)
	held, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}

	// A real write moves the token, then the catalog goes down for list calls.
	mustPut(t, base, testTenant, "b", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat.fail.Store(true)
	runRegistry(t, reg)
	waitFor(t, 2*time.Second, "the failed reload", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "ingester", "result": "error"}) >= 1
	})
	current, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot during the outage: %v", err)
	}
	if got := names(current, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("snapshot during the outage = %v, want the previous good [a]", got)
	}
	if !held.Has(pipeline.StageIngest) {
		t.Error("the snapshot held by a request lost its definition during the outage")
	}

	// No new catalog write: healing the transient error is enough, because the
	// pending token was never stored as applied.
	cat.fail.Store(false)
	waitFor(t, 2*time.Second, "recovery without a new write", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"a", "b"})
	})
}

// flakyListCatalog fails ListPipelines while fail is set.
type flakyListCatalog struct {
	catalog.Catalog
	fail atomic.Bool
}

func (c *flakyListCatalog) ListPipelines(ctx context.Context, t tenant.ID) ([]catalog.PipelineDefinition, error) {
	if c.fail.Load() {
		return nil, errors.New("catalog temporarily unavailable")
	}
	return c.Catalog.ListPipelines(ctx, t)
}

func TestRegistryDefaultStreamLabelsOrder(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	// The default routing labels are service_name then job: deliberately not
	// sorted. The constructor must accept them and preserve their priority.
	mustPut(t, cat, testTenant, "route", catalog.PipelineStageIngest, "*", 0,
		pipeline.Step{Type: pipeline.StepSetStream, Value: `"routed"`})

	metricsReg := prometheus.NewRegistry()
	reg, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{"service_name", "job"}},
		PollInterval:   5 * time.Millisecond,
		Role:           pipeline.RoleIngester,
		Metrics:        metricsReg,
	})
	if err != nil {
		t.Fatalf("NewRegistry with the default routing-label order: %v", err)
	}
	t.Cleanup(func() { reg.Close() })

	snap, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defs := snap.For(pipeline.StageIngest, "orig")
	if len(defs) != 1 {
		t.Fatalf("For returned %d definitions, want 1", len(defs))
	}
	out, outcome, err := defs[0].Apply(t.Context(), "orig", pipeline.Entry{
		Line:   "hello",
		Labels: logs.Labels{{Name: "job", Value: "orig"}},
	})
	if err != nil || outcome != pipeline.OutcomeKeep {
		t.Fatalf("Apply = %v, %v, want keep, nil", outcome, err)
	}
	// set_stream writes the FIRST configured key, never job.
	if v, _ := out.Labels.Get("service_name"); v != "routed" {
		t.Errorf("service_name = %q, want routed (first configured routing label)", v)
	}
	if v, _ := out.Labels.Get("job"); v != "orig" {
		t.Errorf("job = %q, want the unchanged orig", v)
	}
}

func TestRegistryCancellationAfterCatalogReturn(t *testing.T) {
	for _, when := range []string{"token", "list"} {
		t.Run(when, func(t *testing.T) {
			base := catalog.NewMemory()
			t.Cleanup(func() { base.Close() })
			mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
			cat := &cancelingCatalog{Catalog: base, when: when}
			reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

			ctx, cancel := context.WithCancel(t.Context())
			cat.cancel = cancel
			snap, err := reg.Snapshot(ctx, testTenant)
			if err == nil || snap != nil {
				t.Fatalf("Snapshot canceled after the %s call = %v, %v, want nil, error", when, snap, err)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Snapshot canceled after the %s call = %v, want context.Canceled", when, err)
			}

			// The failed load published nothing; a fresh call retries and works.
			cat.cancel = nil
			snap, err = reg.Snapshot(t.Context(), testTenant)
			if err != nil {
				t.Fatalf("Snapshot after cancellation: %v", err)
			}
			if got := names(snap, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"a"}) {
				t.Errorf("snapshot after retry = %v, want [a]", got)
			}

			// A cached snapshot is still not served to a canceled request.
			canceled, cancelNow := context.WithCancel(t.Context())
			cancelNow()
			if _, err := reg.Snapshot(canceled, testTenant); !errors.Is(err, context.Canceled) {
				t.Errorf("cached Snapshot with a canceled context = %v, want context.Canceled", err)
			}
		})
	}
}

// cancelingCatalog cancels the caller's context right after the named call
// returns, simulating a catalog that finishes as the caller gives up.
type cancelingCatalog struct {
	catalog.Catalog
	when   string
	cancel context.CancelFunc
}

func (c *cancelingCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	token, err := c.Catalog.PipelineChangeToken(ctx, t)
	if c.when == "token" && c.cancel != nil {
		c.cancel()
	}
	return token, err
}

func (c *cancelingCatalog) ListPipelines(ctx context.Context, t tenant.ID) ([]catalog.PipelineDefinition, error) {
	defs, err := c.Catalog.ListPipelines(ctx, t)
	if c.when == "list" && c.cancel != nil {
		c.cancel()
	}
	return defs, err
}

func TestRegistryCloseWakesFirstLoadWaiter(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &ignoringCancelCatalog{Catalog: base, entered: make(chan struct{}), release: make(chan struct{})}
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)

	loaderDone := make(chan error, 1)
	go func() {
		_, err := reg.Snapshot(t.Context(), testTenant)
		loaderDone <- err
	}()
	waitFor(t, 2*time.Second, "the loader to start", func() bool {
		select {
		case <-cat.entered:
			return true
		default:
			return false
		}
	})

	waiterDone := make(chan error, 1)
	go func() {
		_, err := reg.Snapshot(t.Context(), testTenant)
		waiterDone <- err
	}()
	// Give the waiter a moment to reach the initDone wait; Close must wake it
	// even though the catalog ignores cancellation and the loader never ends.
	time.Sleep(10 * time.Millisecond)
	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-waiterDone:
		if !errors.Is(err, pipeline.ErrRegistryClosed) {
			t.Fatalf("first-load waiter after Close = %v, want ErrRegistryClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the first-load waiter")
	}

	close(cat.release)
	select {
	case err := <-loaderDone:
		if !errors.Is(err, pipeline.ErrRegistryClosed) {
			t.Fatalf("loader after Close = %v, want ErrRegistryClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loader did not finish after release")
	}
}

// ignoringCancelCatalog blocks change-token reads without watching ctx.
type ignoringCancelCatalog struct {
	catalog.Catalog
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *ignoringCancelCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Catalog.PipelineChangeToken(context.Background(), t)
}

func TestRegistryTenantChangesAreIsolated(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	otherTenant := tenant.ID("tenant-b")
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	mustPut(t, cat, otherTenant, "b", catalog.PipelineStageIngest, "*", 0, keepStep())

	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	runRegistry(t, reg)
	for id, want := range map[tenant.ID][]string{testTenant: {"a"}, otherTenant: {"b"}} {
		if _, err := reg.Snapshot(t.Context(), id); err != nil {
			t.Fatalf("Snapshot %q: %v", id, err)
		}
		waitFor(t, 2*time.Second, "tenant "+string(id)+" first snapshot", func() bool {
			snap, err := reg.Snapshot(t.Context(), id)
			return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), want)
		})
	}

	// A create and a delete in one tenant never disturb the other.
	mustPut(t, cat, testTenant, "a2", catalog.PipelineStageIngest, "*", 0, keepStep())
	waitFor(t, 2*time.Second, "tenant-a's create", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && len(names(snap, pipeline.StageIngest, "app")) == 2
	})
	snapB, err := reg.Snapshot(t.Context(), otherTenant)
	if err != nil {
		t.Fatalf("Snapshot tenant-b: %v", err)
	}
	if got := names(snapB, pipeline.StageIngest, "app"); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("tenant-b = %v after tenant-a's create, want [b]", got)
	}
}

func TestRegistryActiveDefinitionsConcurrentTenants(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	const tenants = 8
	ids := make([]tenant.ID, tenants)
	defs := make([]catalog.PipelineDefinition, tenants)
	for i := range tenants {
		ids[i] = tenant.ID(fmt.Sprintf("tenant-%d", i))
		defs[i] = mustPut(t, cat, ids[i], "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	}
	metricsReg := prometheus.NewRegistry()
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, metricsReg)

	// Concurrent first uses publish distinct tenants at once; the gauge must
	// sum them, not let a stale computed total overwrite a newer one.
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			if _, err := reg.Snapshot(t.Context(), id); err != nil {
				t.Errorf("Snapshot %q: %v", id, err)
			}
		})
	}
	wg.Wait()
	waitFor(t, 2*time.Second, "the gauge to sum all concurrent tenants", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_active_definitions",
			map[string]string{"role": "ingester"}) == tenants
	})

	// Deleting the only definition of one tenant drops exactly that count.
	runRegistry(t, reg)
	if _, err := cat.DeletePipeline(t.Context(), ids[0], "a", defs[0].Version); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}
	waitFor(t, 2*time.Second, "the tenant's delete", func() bool {
		snap, err := reg.Snapshot(t.Context(), ids[0])
		return err == nil && !snap.Has(pipeline.StageIngest)
	})
	waitFor(t, 2*time.Second, "the delete's gauge delta", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_active_definitions",
			map[string]string{"role": "ingester"}) == tenants-1
	})
}

func TestRegistryDeleteRemovesDefinition(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	def := mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())

	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	held, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	runRegistry(t, reg)
	if _, err := cat.DeletePipeline(t.Context(), testTenant, "a", def.Version); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}
	waitFor(t, 2*time.Second, "the delete", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && !snap.Has(pipeline.StageIngest)
	})
	if !held.Has(pipeline.StageIngest) {
		t.Error("the snapshot held by a request lost its definition after a delete")
	}

	// Recreation continues above the tombstone version and becomes visible.
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	waitFor(t, 2*time.Second, "the recreation", func() bool {
		snap, err := reg.Snapshot(t.Context(), testTenant)
		return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"a"})
	})
}

func TestRegistryIndependentTenantDoesNotStallOthers(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, "tenant-fast", "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &gatedCatalog{Catalog: base, slowTenant: "tenant-slow", entered: make(chan struct{}), release: make(chan struct{})}

	reg, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		PollInterval:   10 * time.Millisecond,
		// Long enough that a scheduler which waits for the whole tick would
		// miss the second fast update while the slow tenant stays blocked.
		ReloadTimeout: 10 * time.Second,
		Role:          pipeline.RoleIngester,
		Metrics:       prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { reg.Close() })

	if _, err := reg.Snapshot(t.Context(), "tenant-fast"); err != nil {
		t.Fatalf("Snapshot fast: %v", err)
	}
	if _, err := reg.Snapshot(t.Context(), "tenant-slow"); err != nil {
		t.Fatalf("Snapshot slow: %v", err)
	}
	cat.block.Store(true)
	runRegistry(t, reg)
	waitFor(t, 2*time.Second, "the slow tenant to block a reload", func() bool {
		select {
		case <-cat.entered:
			return true
		default:
			return false
		}
	})

	// Tick 1 while slow is blocked.
	mustPut(t, base, "tenant-fast", "b", catalog.PipelineStageIngest, "*", 0, keepStep())
	waitFor(t, 2*time.Second, "the fast tenant's first update", func() bool {
		snap, err := reg.Snapshot(t.Context(), "tenant-fast")
		return err == nil && len(names(snap, pipeline.StageIngest, "app")) == 2
	})

	// Tick 2 must still be scheduled while the slow worker is blocked and has
	// not timed out: the tick loop never waits for earlier workers.
	mustPut(t, base, "tenant-fast", "c", catalog.PipelineStageIngest, "*", 0, keepStep())
	waitFor(t, 2*time.Second, "the fast tenant's second update before slow is released", func() bool {
		snap, err := reg.Snapshot(t.Context(), "tenant-fast")
		return err == nil && len(names(snap, pipeline.StageIngest, "app")) == 3
	})
	close(cat.release)
}

func TestRegistryRunJoinsPollWorkers(t *testing.T) {
	base := catalog.NewMemory()
	t.Cleanup(func() { base.Close() })
	mustPut(t, base, "tenant-slow", "a", catalog.PipelineStageIngest, "*", 0, keepStep())
	cat := &gatedCatalog{
		Catalog:      base,
		slowTenant:   "tenant-slow",
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		ignoreCancel: true,
	}
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, nil)
	if _, err := reg.Snapshot(t.Context(), "tenant-slow"); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	cat.block.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- reg.Run(ctx) }()
	waitFor(t, 2*time.Second, "the poll worker to block", func() bool {
		select {
		case <-cat.entered:
			return true
		default:
			return false
		}
	})

	cancel()
	// The worker ignores cancellation, so Run must not return until it is
	// released: Run joins every spawned poll worker.
	select {
	case err := <-runDone:
		t.Fatalf("Run returned %v while a poll worker was still running, want a join", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(cat.release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after the worker finished = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the poll worker finished")
	}
}

// gatedCatalog blocks change-token reads for slowTenant once block is set.
type gatedCatalog struct {
	catalog.Catalog
	slowTenant   tenant.ID
	block        atomic.Bool
	ignoreCancel bool
	entered      chan struct{}
	release      chan struct{}
	once         sync.Once
}

func (c *gatedCatalog) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	if t == c.slowTenant && c.block.Load() {
		c.once.Do(func() { close(c.entered) })
		if c.ignoreCancel {
			<-c.release
		} else {
			select {
			case <-c.release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}
	return c.Catalog.PipelineChangeToken(ctx, t)
}

func TestRegistryCatalogBackendsConverge(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T) (catalog.Catalog, catalog.Catalog)
	}{
		{"memory", func(t *testing.T) (catalog.Catalog, catalog.Catalog) {
			cat := catalog.NewMemory()
			t.Cleanup(func() { cat.Close() })
			return cat, cat
		}},
		{"sqlite", func(t *testing.T) (catalog.Catalog, catalog.Catalog) {
			path := filepath.Join(t.TempDir(), "catalog.db")
			a, err := catalog.NewSQLite(path)
			if err != nil {
				t.Fatalf("NewSQLite: %v", err)
			}
			b, err := catalog.NewSQLite(path)
			if err != nil {
				a.Close()
				t.Fatalf("second NewSQLite: %v", err)
			}
			t.Cleanup(func() {
				if err := b.Close(); err != nil {
					t.Errorf("closing second catalog: %v", err)
				}
				if err := a.Close(); err != nil {
					t.Errorf("closing first catalog: %v", err)
				}
			})
			return a, b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catA, catB := tc.open(t)
			regA := newTestRegistry(t, catA, pipeline.RoleIngester, nil)
			regB := newTestRegistry(t, catB, pipeline.RoleQuerier, nil)
			runRegistry(t, regA)
			runRegistry(t, regB)

			def := mustPut(t, catA, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())
			for _, reg := range []*pipeline.Registry{regA, regB} {
				waitFor(t, 2*time.Second, "both registries to see the create", func() bool {
					snap, err := reg.Snapshot(t.Context(), testTenant)
					return err == nil && slices.Equal(names(snap, pipeline.StageIngest, "app"), []string{"a"})
				})
			}

			mustPut(t, catA, testTenant, "a", catalog.PipelineStageIngest, "other", def.Version, keepStep())
			for _, reg := range []*pipeline.Registry{regA, regB} {
				waitFor(t, 2*time.Second, "both registries to see the update", func() bool {
					snap, err := reg.Snapshot(t.Context(), testTenant)
					return err == nil && len(names(snap, pipeline.StageIngest, "app")) == 0
				})
			}

			if _, err := catA.DeletePipeline(t.Context(), testTenant, "a", def.Version+1); err != nil {
				t.Fatalf("DeletePipeline: %v", err)
			}
			for _, reg := range []*pipeline.Registry{regA, regB} {
				waitFor(t, 2*time.Second, "both registries to see the delete", func() bool {
					snap, err := reg.Snapshot(t.Context(), testTenant)
					return err == nil && !snap.Has(pipeline.StageIngest)
				})
			}
		})
	}
}

func TestRegistryObserverOutcomes(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	mustPut(t, cat, testTenant, "k", catalog.PipelineStageIngest, "*", 0, keepStep())
	mustPut(t, cat, testTenant, "d", catalog.PipelineStageIngest, "*", 0, dropStep())
	mustPut(t, cat, testTenant, "s", catalog.PipelineStageIngest, "*", 0, stopStep())

	metricsReg := prometheus.NewRegistry()
	reg := newTestRegistry(t, cat, pipeline.RoleIngester, metricsReg)
	snap, err := reg.Snapshot(t.Context(), testTenant)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	byName := make(map[string]*pipeline.Compiled)
	for _, c := range snap.For(pipeline.StageIngest, "app") {
		byName[c.Name()] = c
	}
	entry := pipeline.Entry{
		Line:   `{"level":"info"}`,
		Labels: logs.Labels{{Name: testStreamLabel, Value: "app"}},
	}
	if _, out, err := byName["k"].Apply(t.Context(), "app", entry); err != nil || out != pipeline.OutcomeKeep {
		t.Fatalf("keep Apply = %v, %v, want keep, nil", out, err)
	}
	if _, out, err := byName["d"].Apply(t.Context(), "app", entry); err != nil || out != pipeline.OutcomeDrop {
		t.Fatalf("drop Apply = %v, %v, want drop, nil", out, err)
	}
	badLine := entry
	badLine.Line = "not json"
	if _, out, err := byName["s"].Apply(t.Context(), "app", badLine); err != nil || out != pipeline.OutcomeStop {
		t.Fatalf("stop Apply = %v, %v, want stop, nil", out, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := byName["k"].Apply(canceled, "app", entry); err == nil {
		t.Fatal("Apply with a canceled context returned no error")
	}

	for _, result := range []string{"keep", "drop", "stop", "error"} {
		if got := sampleValue(t, metricsReg, "parkana_pipeline_executions_total",
			map[string]string{"role": "ingester", "stage": "ingest", "result": result}); got != 1 {
			t.Errorf("executions{result=%q} = %v, want 1", result, got)
		}
	}
}

func TestRegistryMetricsPerRoleAndCleanup(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })
	mustPut(t, cat, testTenant, "a", catalog.PipelineStageIngest, "*", 0, keepStep())

	metricsReg := prometheus.NewRegistry()
	ing, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		PollInterval:   5 * time.Millisecond,
		Role:           pipeline.RoleIngester,
		Metrics:        metricsReg,
	})
	if err != nil {
		t.Fatalf("ingester NewRegistry: %v", err)
	}
	que, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		PollInterval:   5 * time.Millisecond,
		Role:           pipeline.RoleQuerier,
		Metrics:        metricsReg,
	})
	if err != nil {
		t.Fatalf("querier NewRegistry on the same registerer: %v", err)
	}

	// Both roles load the same definition; their metrics are independent and
	// carry their own role label.
	for _, reg := range []*pipeline.Registry{ing, que} {
		if _, err := reg.Snapshot(t.Context(), testTenant); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}
	for _, role := range []string{"ingester", "querier"} {
		if got := sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": role, "result": "ok"}); got < 1 {
			t.Errorf("reloads{role=%q} = %v, want at least 1", role, got)
		}
		waitFor(t, 2*time.Second, "active definitions for "+role, func() bool {
			return sampleValue(t, metricsReg, "parkana_pipeline_active_definitions",
				map[string]string{"role": role}) == 1
		})
	}

	// A same-role registry on the same registerer is rejected without
	// disturbing the running one.
	if dup, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		Role:           pipeline.RoleIngester,
		Metrics:        metricsReg,
	}); err == nil || dup != nil {
		t.Fatalf("same-role NewRegistry = %v, %v, want nil, error", dup, err)
	}

	if err := ing.Close(); err != nil {
		t.Fatalf("ingester Close: %v", err)
	}
	// The querier's collectors survive and still observe: a new tenant's
	// first use adds another observations on the querier's own series.
	if _, err := que.Snapshot(t.Context(), "tenant-b"); err != nil {
		t.Fatalf("querier Snapshot after ingester close: %v", err)
	}
	waitFor(t, 2*time.Second, "the querier's reload counter", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "querier", "result": "ok"}) >= 2
	})
	if err := que.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}
	if got := familyNames(t, metricsReg); len(got) != 0 {
		t.Fatalf("families after both roles closed = %v, want none", got)
	}

	// A restarted role can register the same names again: Close released
	// exactly its own collectors. (Prometheus keeps the family's label
	// signature for the process lifetime, so the new collectors must match.)
	restarted, err := pipeline.NewRegistry(cat, pipeline.RegistryOptions{
		CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}},
		Role:           pipeline.RoleIngester,
		Metrics:        metricsReg,
	})
	if err != nil {
		t.Fatalf("NewRegistry after ingester restart: %v", err)
	}
	t.Cleanup(func() { restarted.Close() })
	if _, err := restarted.Snapshot(t.Context(), testTenant); err != nil {
		t.Fatalf("Snapshot on the restarted registry: %v", err)
	}
	waitFor(t, 2*time.Second, "the restarted ingester's reload counter", func() bool {
		return sampleValue(t, metricsReg, "parkana_pipeline_reloads_total",
			map[string]string{"role": "ingester", "result": "ok"}) >= 1
	})
}

func TestNewRegistryValidation(t *testing.T) {
	cat := catalog.NewMemory()
	t.Cleanup(func() { cat.Close() })

	cases := []struct {
		name string
		cat  catalog.Catalog
		opts pipeline.RegistryOptions
	}{
		{"nil catalog", nil, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}}, Role: pipeline.RoleIngester, Metrics: prometheus.NewRegistry()}},
		{"bad role", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}}, Role: "worker", Metrics: prometheus.NewRegistry()}},
		{"nil metrics", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}}, Role: pipeline.RoleIngester}},
		{"negative poll", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}}, Role: pipeline.RoleIngester, PollInterval: -time.Second, Metrics: prometheus.NewRegistry()}},
		{"negative reload timeout", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}}, Role: pipeline.RoleIngester, ReloadTimeout: -time.Second, Metrics: prometheus.NewRegistry()}},
		{"no stream labels", cat, pipeline.RegistryOptions{Role: pipeline.RoleIngester, Metrics: prometheus.NewRegistry()}},
		{"duplicate stream labels", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{"service", "service"}}, Role: pipeline.RoleIngester, Metrics: prometheus.NewRegistry()}},
		{"negative line bound", cat, pipeline.RegistryOptions{CompileOptions: pipeline.CompileOptions{StreamLabels: []string{testStreamLabel}, LineBytes: -1}, Role: pipeline.RoleIngester, Metrics: prometheus.NewRegistry()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, err := pipeline.NewRegistry(tc.cat, tc.opts)
			if err == nil || reg != nil {
				t.Fatalf("NewRegistry = %v, %v, want nil, error", reg, err)
			}
			if gatherer, ok := tc.opts.Metrics.(prometheus.Gatherer); ok {
				if got := familyNames(t, gatherer); len(got) != 0 {
					t.Fatalf("failed constructor left %v registered", got)
				}
			}
		})
	}
}
