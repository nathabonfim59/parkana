package tail

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// tailMetrics holds the querier-side live-tail collectors. They are
// registered on Deps.Metrics, never a package-level registry, and every name
// carries the parkana_ prefix. Partial registration is rolled back so a
// retried construction starts clean.
type tailMetrics struct {
	subscribers    *prometheus.GaugeVec
	entries        *prometheus.CounterVec
	droppedEntries *prometheus.CounterVec
	upstreamErrors *prometheus.CounterVec
}

func newTailMetrics(reg prometheus.Registerer) (*tailMetrics, error) {
	if reg == nil {
		return nil, errors.New("tail: metrics registerer is required")
	}
	m := &tailMetrics{
		subscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "parkana_tail_subscribers",
			Help: "Live-tail WebSocket subscribers currently connected, by tenant.",
		}, []string{"tenant"}),
		entries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_tail_entries_total",
			Help: "Entries written to live-tail WebSocket subscribers, by tenant.",
		}, []string{"tenant"}),
		droppedEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_tail_dropped_entries_total",
			Help: "Entries dropped from a live-tail subscriber's bounded queue, by tenant.",
		}, []string{"tenant"}),
		upstreamErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_tail_upstream_errors_total",
			Help: "Ingester tail subscriptions that ended with an explicit error, by tenant.",
		}, []string{"tenant"}),
	}
	collectors := []prometheus.Collector{m.subscribers, m.entries, m.droppedEntries, m.upstreamErrors}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, r := range registered {
				reg.Unregister(r)
			}
			return nil, fmt.Errorf("tail: register metrics: %w", err)
		}
		registered = append(registered, c)
	}
	return m, nil
}

func (m *tailMetrics) subscribe(id tenant.ID) {
	m.subscribers.WithLabelValues(string(id)).Inc()
}

func (m *tailMetrics) unsubscribe(id tenant.ID) {
	m.subscribers.WithLabelValues(string(id)).Dec()
}

func (m *tailMetrics) sent(id tenant.ID, n int) {
	m.entries.WithLabelValues(string(id)).Add(float64(n))
}

func (m *tailMetrics) dropped(id tenant.ID, n uint64) {
	if n > 0 {
		m.droppedEntries.WithLabelValues(string(id)).Add(float64(n))
	}
}

func (m *tailMetrics) upstreamError(id tenant.ID) {
	m.upstreamErrors.WithLabelValues(string(id)).Inc()
}
