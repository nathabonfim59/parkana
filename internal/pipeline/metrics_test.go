package pipeline_test

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/pipeline"
)

// metricNames are the families RegisterMetrics exposes once observed.
var metricNames = []string{
	"parkana_pipeline_active_definitions",
	"parkana_pipeline_execution_duration_seconds",
	"parkana_pipeline_executions_total",
	"parkana_pipeline_reload_duration_seconds",
	"parkana_pipeline_reloads_total",
}

func TestRegisterMetricsNilRegisterer(t *testing.T) {
	if m, err := pipeline.RegisterMetrics(nil, pipeline.RoleIngester); err == nil || m != nil {
		t.Fatalf("RegisterMetrics(nil) = %v, %v, want nil, error", m, err)
	}
}

func TestRegisterMetricsInvalidRole(t *testing.T) {
	for _, role := range []string{"", "worker", "Ingester"} {
		if m, err := pipeline.RegisterMetrics(prometheus.NewRegistry(), role); err == nil || m != nil {
			t.Errorf("RegisterMetrics(role=%q) = %v, %v, want nil, error", role, m, err)
		}
	}
}

func TestRegisterMetricsObserves(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := pipeline.RegisterMetrics(reg, pipeline.RoleIngester)
	if err != nil {
		t.Fatalf("RegisterMetrics: %v", err)
	}
	defer m.Close()

	// Nothing is observed yet, so the vecs have no children and no families.
	if got := familyNames(t, reg); len(got) != 0 {
		t.Fatalf("fresh registry holds %v, want no families", got)
	}

	m.ObserveReload(20*time.Millisecond, nil)
	m.ObserveReload(10*time.Millisecond, errors.New("bad spec"))
	m.AddActiveDefinitions(3)
	m.ObserveExecution(pipeline.StageRead, pipeline.OutcomeKeep, nil, time.Millisecond)

	if got := familyNames(t, reg); !slices.Equal(slices.Sorted(slices.Values(metricNames)), got) {
		t.Fatalf("families = %v, want exactly %v", got, metricNames)
	}

	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"parkana_pipeline_reloads_total", map[string]string{"role": "ingester", "result": "ok"}, 1},
		{"parkana_pipeline_reloads_total", map[string]string{"role": "ingester", "result": "error"}, 1},
		{"parkana_pipeline_reload_duration_seconds", map[string]string{"role": "ingester", "result": "ok"}, 1},
		{"parkana_pipeline_reload_duration_seconds", map[string]string{"role": "ingester", "result": "error"}, 1},
		{"parkana_pipeline_active_definitions", map[string]string{"role": "ingester"}, 3},
		{"parkana_pipeline_executions_total", map[string]string{"role": "ingester", "stage": "read", "result": "keep"}, 1},
		{"parkana_pipeline_execution_duration_seconds", map[string]string{"role": "ingester", "stage": "read", "result": "keep"}, 1},
		// Never observed series report zero.
		{"parkana_pipeline_reloads_total", map[string]string{"role": "ingester", "result": "keep"}, 0},
	} {
		if got := sampleValue(t, reg, tc.name, tc.labels); got != tc.want {
			t.Errorf("%s%v = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

func TestExecutionObserverOutcomes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := pipeline.RegisterMetrics(reg, pipeline.RoleQuerier)
	if err != nil {
		t.Fatalf("RegisterMetrics: %v", err)
	}
	defer m.Close()
	obs := m.ExecutionObserver()

	obs(pipeline.StageRead, pipeline.OutcomeKeep, nil, time.Millisecond)
	obs(pipeline.StageRead, pipeline.OutcomeStop, nil, time.Millisecond)
	obs(pipeline.StageIngest, pipeline.OutcomeDrop, nil, time.Millisecond)
	obs(pipeline.StageRead, pipeline.OutcomeKeep, errors.New("canceled"), time.Millisecond)

	for _, tc := range []struct {
		stage  string
		result string
		want   float64
	}{
		{"read", "keep", 1},
		{"read", "stop", 1},
		{"read", "error", 1}, // an error overrides the returned outcome
		{"ingest", "drop", 1},
		{"ingest", "keep", 0},
	} {
		labels := map[string]string{"role": "querier", "stage": tc.stage, "result": tc.result}
		if got := sampleValue(t, reg, "parkana_pipeline_executions_total", labels); got != tc.want {
			t.Errorf("executions%v = %v, want %v", labels, got, tc.want)
		}
	}
}

func TestRegisterMetricsRolesCoexist(t *testing.T) {
	reg := prometheus.NewRegistry()
	ing, err := pipeline.RegisterMetrics(reg, pipeline.RoleIngester)
	if err != nil {
		t.Fatalf("RegisterMetrics(ingester): %v", err)
	}
	que, err := pipeline.RegisterMetrics(reg, pipeline.RoleQuerier)
	if err != nil {
		t.Fatalf("RegisterMetrics(querier) on the same registerer: %v", err)
	}

	ing.ObserveReload(time.Millisecond, nil)
	ing.AddActiveDefinitions(1)
	que.AddActiveDefinitions(0)
	que.ObserveExecution(pipeline.StageRead, pipeline.OutcomeDrop, nil, time.Millisecond)
	if got := familyNames(t, reg); !slices.Equal(slices.Sorted(slices.Values(metricNames)), got) {
		t.Fatalf("families = %v, want exactly %v", got, metricNames)
	}
	if got := sampleValue(t, reg, "parkana_pipeline_reloads_total",
		map[string]string{"role": "ingester", "result": "ok"}); got != 1 {
		t.Errorf("ingester reloads = %v, want 1", got)
	}
	if got := sampleValue(t, reg, "parkana_pipeline_executions_total",
		map[string]string{"role": "querier", "stage": "read", "result": "drop"}); got != 1 {
		t.Errorf("querier executions = %v, want 1", got)
	}

	// Closing one role releases only its own collectors.
	if err := ing.Close(); err != nil {
		t.Fatalf("ingester Close: %v", err)
	}
	if got := familyNames(t, reg); len(got) == 0 {
		t.Fatal("closing the ingester released the querier's collectors too")
	}
	que.ObserveReload(time.Millisecond, nil)
	if got := sampleValue(t, reg, "parkana_pipeline_reloads_total",
		map[string]string{"role": "querier", "result": "ok"}); got != 1 {
		t.Errorf("querier reloads after ingester close = %v, want 1", got)
	}
	if err := que.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}
	if got := familyNames(t, reg); len(got) != 0 {
		t.Fatalf("registry after both roles closed holds %v, want no families", got)
	}
	// Close is idempotent.
	if err := que.Close(); err != nil {
		t.Fatalf("second querier Close: %v", err)
	}
}

func TestRegisterMetricsSameRoleCollision(t *testing.T) {
	reg := prometheus.NewRegistry()
	first, err := pipeline.RegisterMetrics(reg, pipeline.RoleIngester)
	if err != nil {
		t.Fatalf("first RegisterMetrics: %v", err)
	}
	defer first.Close()

	if m, err := pipeline.RegisterMetrics(reg, pipeline.RoleIngester); err == nil || m != nil {
		t.Fatalf("second same-role RegisterMetrics = %v, %v, want nil, error", m, err)
	}
	// The failed call registered nothing new; the first handle still works.
	first.ObserveReload(time.Millisecond, nil)
	if got := sampleValue(t, reg, "parkana_pipeline_reloads_total",
		map[string]string{"role": "ingester", "result": "ok"}); got != 1 {
		t.Fatalf("ingester reloads = %v, want 1", got)
	}
}

func TestRegisterMetricsRollbackOnLateCollision(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := &recordingRegisterer{Registerer: reg}
	// Squat the fourth family so the first three of this call register and
	// must be rolled back when executions collides.
	squat := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parkana_pipeline_executions_total",
		Help: "squatter",
	}, []string{"stage", "result"})
	reg.MustRegister(squat)
	squat.WithLabelValues("read", "keep").Inc()

	m, err := pipeline.RegisterMetrics(rec, pipeline.RoleIngester)
	if err == nil {
		t.Fatal("RegisterMetrics succeeded over a squatted family, want error")
	}
	if m != nil {
		t.Fatalf("RegisterMetrics returned %v with an error, want nil", m)
	}
	if got := familyNames(t, reg); !slices.Equal(got, []string{"parkana_pipeline_executions_total"}) {
		t.Fatalf("registry after failed registration holds %v, want only the squatter's family", got)
	}
	// Exactly the three families registered before the collision are rolled
	// back by this call.
	rec.mu.Lock()
	rolled := len(rec.unregistered)
	rec.mu.Unlock()
	if rolled != 3 {
		t.Fatalf("failed registration rolled back %d collectors, want 3", rolled)
	}
}

// recordingRegisterer records Unregister calls on the registry it wraps.
type recordingRegisterer struct {
	prometheus.Registerer
	mu           sync.Mutex
	unregistered []prometheus.Collector
}

// Unregister delegates and records successful unregistrations.
func (r *recordingRegisterer) Unregister(c prometheus.Collector) bool {
	ok := r.Registerer.Unregister(c)
	if ok {
		r.mu.Lock()
		r.unregistered = append(r.unregistered, c)
		r.mu.Unlock()
	}
	return ok
}

// familyNames returns the sorted metric family names gathered from g.
func familyNames(t *testing.T, g prometheus.Gatherer) []string {
	t.Helper()
	fam, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make([]string, 0, len(fam))
	for _, mf := range fam {
		names = append(names, mf.GetName())
	}
	return slices.Sorted(slices.Values(names))
}

// sampleValue returns the counter value, gauge value or histogram sample count
// of the series with exactly these labels, or 0 when no such series exists.
func sampleValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	fam, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range fam {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !metricLabelsMatch(m.GetLabel(), labels) {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			default:
				t.Fatalf("%s%v is not a counter, gauge or histogram", name, labels)
			}
		}
	}
	return 0
}

// metricLabelsMatch reports whether got carries exactly the wanted label
// pairs. It is generic so the test file does not import client_model directly.
func metricLabelsMatch[T interface {
	GetName() string
	GetValue() string
}](got []T, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, lp := range got {
		v, ok := want[lp.GetName()]
		if !ok || v != lp.GetValue() {
			return false
		}
	}
	return true
}
