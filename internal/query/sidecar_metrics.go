package query

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// sidecarMetrics are the decoded-sidecar cache's metrics. Every label is
// bounded to a fixed value set; no metric carries an object or tenant label.
type sidecarMetrics struct {
	requests     *prometheus.CounterVec // result: hit, miss
	fetches      *prometheus.CounterVec // result: success, fetch_error, decode_error, oversized
	evictions    prometheus.Counter
	fetchedBytes prometheus.Counter
	bytes        prometheus.Gauge // resident accounted decoded bytes
	entries      prometheus.Gauge // admitted entries

	reg        prometheus.Registerer
	registered []prometheus.Collector
}

// unregister removes every collector this cache successfully registered. It
// is called when construction fails after registration and on Close, so the
// registerer stays reusable.
func (m *sidecarMetrics) unregister() {
	if m == nil || m.reg == nil {
		return
	}
	for _, collector := range m.registered {
		m.reg.Unregister(collector)
	}
	m.registered = nil
}

// newSidecarMetrics builds and registers the sidecar cache metrics. A nil
// registerer returns usable but unregistered collectors. A partial
// registration failure unregisters what it already registered, so the
// registerer stays reusable.
func newSidecarMetrics(reg prometheus.Registerer) (*sidecarMetrics, error) {
	m := &sidecarMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_querier_sidecar_cache_requests_total",
			Help: "Sidecar cache lookups by result.",
		}, []string{"result"}),
		fetches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_querier_sidecar_cache_fetches_total",
			Help: "Sidecar fetch/decode attempts by result.",
		}, []string{"result"}),
		evictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_querier_sidecar_cache_evictions_total",
			Help: "Decoded sidecars evicted to stay within the resident byte budget.",
		}),
		fetchedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_querier_sidecar_cache_fetched_bytes_total",
			Help: "Serialized sidecar bytes read from the object store.",
		}),
		bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_querier_sidecar_cache_bytes",
			Help: "Accounted decoded bytes currently resident in the sidecar cache.",
		}),
		entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_querier_sidecar_cache_entries",
			Help: "Decoded sidecars currently admitted in the sidecar cache.",
		}),
		reg: reg,
	}
	all := []prometheus.Collector{
		m.requests, m.fetches, m.evictions, m.fetchedBytes, m.bytes, m.entries,
	}
	// Expose every bounded result value from the start, so a scrape shows 0
	// rather than a missing series before the first hit or failure.
	for _, result := range []string{"hit", "miss"} {
		m.requests.WithLabelValues(result)
	}
	for _, result := range []string{"success", "fetch_error", "decode_error", "oversized"} {
		m.fetches.WithLabelValues(result)
	}
	if reg == nil {
		return m, nil
	}
	for _, collector := range all {
		if err := reg.Register(collector); err != nil {
			m.unregister()
			return nil, fmt.Errorf("query: sidecar cache: register metrics: %w", err)
		}
		m.registered = append(m.registered, collector)
	}
	return m, nil
}
