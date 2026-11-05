package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Role names label the parkana_pipeline_* series by the role that owns a
// registry. The set is closed, so role never adds unbounded cardinality.
const (
	// RoleIngester labels a registry built by the ingester.
	RoleIngester = "ingester"
	// RoleQuerier labels a registry built by the querier.
	RoleQuerier = "querier"
)

// validRole reports whether role is one of the registered role labels.
func validRole(role string) bool { return role == RoleIngester || role == RoleQuerier }

// resultError labels executions that returned an error instead of an outcome.
const resultError = "error"

// Metrics holds one role's parkana_pipeline_* collectors. Each Registry owns
// its own instance: the role is a constant label, so an ingester and a querier
// can share one process registerer without sharing mutable metric state. A
// second Metrics for the same role on the same registerer is a constructor
// error. The zero value is unusable. All methods are safe for concurrent use.
type Metrics struct {
	reloads           *prometheus.CounterVec
	reloadDuration    *prometheus.HistogramVec
	activeDefinitions *prometheus.GaugeVec
	executions        *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec

	// reg and collectors are this instance's own registrations, released by
	// Close. They are never shared with another Metrics.
	reg        prometheus.Registerer
	collectors []prometheus.Collector

	closeOnce sync.Once
	closeErr  error
}

// RegisterMetrics registers one role's pipeline collectors on reg and returns
// the handle that observes them. reg must not be nil and role must be
// RoleIngester or RoleQuerier.
//
// The collectors are registered one by one. If a later family collides with
// an existing registration (including another Metrics for the same role), the
// families this call already registered are unregistered again, so a failed
// call leaves nothing behind. Independent roles coexist on one registerer
// because role is a constant label of every series.
func RegisterMetrics(reg prometheus.Registerer, role string) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("pipeline: metrics registerer is required")
	}
	if !validRole(role) {
		return nil, fmt.Errorf("pipeline: metrics role %q must be %q or %q", role, RoleIngester, RoleQuerier)
	}
	constLabels := prometheus.Labels{"role": role}
	m := &Metrics{
		reg: reg,
		reloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "parkana_pipeline_reloads_total",
			Help:        "Pipeline snapshot reload attempts that saw a changed change token, by result.",
			ConstLabels: constLabels,
		}, []string{"result"}),
		reloadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "parkana_pipeline_reload_duration_seconds",
			Help:        "Pipeline snapshot reload latency for attempts that saw a changed change token, by result.",
			Buckets:     prometheus.ExponentialBuckets(0.001, 4, 8),
			ConstLabels: constLabels,
		}, []string{"result"}),
		activeDefinitions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "parkana_pipeline_active_definitions",
			Help:        "Compiled pipeline definitions currently held in this role's snapshots across all seen tenants.",
			ConstLabels: constLabels,
		}, nil),
		executions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "parkana_pipeline_executions_total",
			Help:        "Compiled pipeline applications, by stage and result.",
			ConstLabels: constLabels,
		}, []string{"stage", "result"}),
		executionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "parkana_pipeline_execution_duration_seconds",
			Help:        "Compiled pipeline application latency, by stage and result.",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 4, 10),
			ConstLabels: constLabels,
		}, []string{"stage", "result"}),
	}
	collectors := []prometheus.Collector{
		m.reloads,
		m.reloadDuration,
		m.activeDefinitions,
		m.executions,
		m.executionDuration,
	}
	for i, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, done := range m.collectors {
				reg.Unregister(done)
			}
			return nil, fmt.Errorf("pipeline: register metrics for role %q: %w (rolled back %d collector(s) from this call)",
				role, err, i)
		}
		m.collectors = append(m.collectors, c)
	}
	return m, nil
}

// Close unregisters this instance's collectors. It is idempotent and releases
// only the collectors this Metrics registered.
func (m *Metrics) Close() error {
	m.closeOnce.Do(func() {
		for _, c := range m.collectors {
			if !m.reg.Unregister(c) {
				m.closeErr = fmt.Errorf("pipeline: unregister %T: collector was not registered", c)
			}
		}
		m.collectors = nil
	})
	return m.closeErr
}

// ObserveReload records one reload attempt that saw a changed change token.
// err nil means the new complete snapshot was swapped in; non-nil means the
// previous good snapshot was kept. Unchanged tokens are not observed.
func (m *Metrics) ObserveReload(d time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = resultError
	}
	m.reloads.WithLabelValues(result).Inc()
	m.reloadDuration.WithLabelValues(result).Observe(d.Seconds())
}

// AddActiveDefinitions adjusts the compiled-definition count the role holds
// across every seen tenant. Deltas from concurrent tenant publications are
// summed by the gauge itself, so an older total can never overwrite a newer
// one.
func (m *Metrics) AddActiveDefinitions(delta int) {
	m.activeDefinitions.WithLabelValues().Add(float64(delta))
}

// ObserveExecution records one Compiled.Apply result: stage and outcome, with
// result "error" for a returned error instead of the outcome name.
func (m *Metrics) ObserveExecution(stage Stage, out Outcome, err error, d time.Duration) {
	result := resultError
	if err == nil {
		result = out.String()
	}
	stageLabel := string(stage)
	m.executions.WithLabelValues(stageLabel, result).Inc()
	m.executionDuration.WithLabelValues(stageLabel, result).Observe(d.Seconds())
}

// ExecutionObserver returns the CompileOptions.ObserveExecution hook bound to
// this Metrics instance. It is the only place the registry couples execution
// outcomes to metrics; the compiler itself stays free of metric dependencies.
func (m *Metrics) ExecutionObserver() func(Stage, Outcome, error, time.Duration) {
	return m.ObserveExecution
}
