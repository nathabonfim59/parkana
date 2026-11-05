// The SQL API's metrics: request outcomes, latency, result size and the number
// of requests currently executing. Every label is a bounded value set and no
// metric carries a tenant, SQL text or object path.
package query

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// sqlOutcomeValues returns the bounded values of the requests counter's
// outcome label. A function returning a fresh literal keeps the package free
// of mutable global state, like allReadPatterns.
func sqlOutcomeValues() []string {
	return []string{"success", "client_error", "deadline", "canceled", "internal"}
}

// sqlMetrics are the SQL endpoint's collectors. They are registered on the
// process registry only through newSQLMetrics and unregistered by unregister,
// which both failed construction rollback and service shutdown call.
type sqlMetrics struct {
	requests   *prometheus.CounterVec // outcome: success, client_error, deadline, canceled, internal
	duration   prometheus.Histogram   // request wall time, outcome included in the outcome counter
	rows       prometheus.Counter     // result rows returned by successful requests
	bytes      prometheus.Counter     // encoded response bytes written by successful requests
	inflight   prometheus.Gauge       // requests currently admitted and executing
	reg        prometheus.Registerer  // nil keeps the collectors usable but unregistered
	registered []prometheus.Collector // collectors this constructor registered
}

// unregister removes every collector these metrics successfully registered so
// the registerer stays reusable. Calling it twice is a no-op.
func (m *sqlMetrics) unregister() {
	if m == nil || m.reg == nil {
		return
	}
	for _, collector := range m.registered {
		m.reg.Unregister(collector)
	}
	m.registered = nil
}

// newSQLMetrics builds and registers the SQL endpoint metrics. A nil registerer
// returns usable but unregistered collectors. Every bounded outcome is
// initialized so a scrape shows 0 before the first request. A partial
// registration failure unregisters what it already registered.
func newSQLMetrics(reg prometheus.Registerer) (*sqlMetrics, error) {
	m := &sqlMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_sql_requests_total",
			Help: "SQL API requests by bounded outcome: success, client_error, deadline, canceled or internal.",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parkana_sql_duration_seconds",
			Help:    "SQL API request wall time in seconds, from decoding through response encoding.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}),
		rows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_sql_rows_total",
			Help: "Rows returned by successful SQL API requests.",
		}),
		bytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_sql_result_bytes_total",
			Help: "Encoded response bytes written by successful SQL API requests.",
		}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_sql_inflight",
			Help: "SQL API requests currently admitted and executing.",
		}),
		reg: reg,
	}
	all := []prometheus.Collector{m.requests, m.duration, m.rows, m.bytes, m.inflight}
	for _, outcome := range sqlOutcomeValues() {
		m.requests.WithLabelValues(outcome)
	}
	if reg == nil {
		return m, nil
	}
	for _, collector := range all {
		if err := reg.Register(collector); err != nil {
			m.unregister()
			return nil, fmt.Errorf("query: sql metrics: register: %w", err)
		}
		m.registered = append(m.registered, collector)
	}
	return m, nil
}

// observe records one finished request. Unlabeled counters and the latency
// histogram are updated only for finished requests; inflight is maintained by
// the handler around the executing section.
func (m *sqlMetrics) observe(outcome string, elapsed time.Duration, rows, resultBytes int64) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(outcome).Inc()
	m.duration.Observe(elapsed.Seconds())
	if outcome == "success" {
		m.rows.Add(float64(rows))
		m.bytes.Add(float64(resultBytes))
	}
}

// admit marks one request as executing.
func (m *sqlMetrics) admit() {
	if m != nil {
		m.inflight.Inc()
	}
}

// release marks one request as no longer executing.
func (m *sqlMetrics) release() {
	if m != nil {
		m.inflight.Dec()
	}
}
