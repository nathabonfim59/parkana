package query

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// scanMetrics are the querier's file-level scan accounting: how many catalog
// files each read attempt scanned, pruned by search sidecars, or pruned by
// complete label summaries, and how many compressed candidate bytes those
// scans submitted to DuckDB. The byte metric counts catalog File.Bytes
// submitted per attempt (failed SQL and disappearance retries included), so it
// measures candidate bytes, not physical disk or network reads. Every label is
// bounded to a fixed value set; no metric carries an object or tenant label.
type scanMetrics struct {
	files *prometheus.CounterVec // result: scanned, pruned_index, pruned_labels
	bytes prometheus.Counter

	reg        prometheus.Registerer
	registered []prometheus.Collector
}

// scanResultValues returns the bounded values of the files counter's result
// label. A function returning a fresh literal keeps the package free of
// mutable global state, like allReadPatterns.
func scanResultValues() []string {
	return []string{"scanned", "pruned_index", "pruned_labels"}
}

// unregister removes every collector these metrics successfully registered.
// It is called when construction fails after registration and on service
// close, so the registerer stays reusable. Calling it twice is a no-op.
func (m *scanMetrics) unregister() {
	if m == nil || m.reg == nil {
		return
	}
	for _, collector := range m.registered {
		m.reg.Unregister(collector)
	}
	m.registered = nil
}

// newScanMetrics builds and registers the scan metrics. A nil registerer
// returns usable but unregistered collectors. Every bounded result value is
// initialized so a scrape shows 0 rather than a missing series before the
// first scan. A partial registration failure unregisters what it already
// registered, so the registerer stays reusable.
func newScanMetrics(reg prometheus.Registerer) (*scanMetrics, error) {
	m := &scanMetrics{
		files: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_query_files_total",
			Help: "Catalog files entering query read attempts by result: scanned (submitted to DuckDB), pruned_index (dropped by search sidecars), pruned_labels (dropped by complete label summaries).",
		}, []string{"result"}),
		bytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_query_bytes_scanned_total",
			Help: "Compressed catalog file bytes submitted to DuckDB across query read attempts, failed attempts included; candidate bytes, not physical reads.",
		}),
		reg: reg,
	}
	all := []prometheus.Collector{m.files, m.bytes}
	for _, result := range scanResultValues() {
		m.files.WithLabelValues(result)
	}
	if reg == nil {
		return m, nil
	}
	for _, collector := range all {
		if err := reg.Register(collector); err != nil {
			m.unregister()
			return nil, fmt.Errorf("query: scan metrics: register: %w", err)
		}
		m.registered = append(m.registered, collector)
	}
	return m, nil
}
