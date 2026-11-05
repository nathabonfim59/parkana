package index

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Source labels the write path that built a sidecar. The set is closed: it
// is the only source of cardinality in the parkana_index_* metrics, so
// observe methods must only be called with these constants.
type Source string

const (
	// SourceIngest labels sidecars built by the ingester for a new upload.
	SourceIngest Source = "ingest"
	// SourceCompaction labels sidecars built from compactor merge output.
	SourceCompaction Source = "compaction"
	// SourceBackfill labels sidecars built by the compactor's backfill pass
	// for files that were published without one.
	SourceBackfill Source = "backfill"
)

// Metrics holds the shared parkana_index_* sidecar-build collectors as one
// prometheus collector. Obtain one with RegisterMetrics; the zero value is
// unusable. All methods are safe for concurrent use.
type Metrics struct {
	built    *prometheus.CounterVec
	skipped  *prometheus.CounterVec
	bytes    *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// RegisterMetrics registers the shared sidecar-build collectors on reg and
// returns a handle for observing builds.
//
// Roles that share one process registry (for example combined roles) each
// call it: when this collector is already registered, the existing one is
// reused through prometheus.AlreadyRegisteredError, so every handle observes
// the same series. Any other registration error, including a same-named
// collector registered by someone else, is returned to the caller; a failed
// RegisterMetrics leaves nothing registered by this call. reg must not be
// nil.
func RegisterMetrics(reg prometheus.Registerer) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("index: metrics registerer is required")
	}
	m := &Metrics{
		built: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_index_built_total",
			Help: "Sidecar index builds that returned encoded bytes, by write path.",
		}, []string{"source"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_index_skipped_too_many_grams_total",
			Help: "Sidecar index builds skipped because the file exceeded the gram cap, by write path.",
		}, []string{"source"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_index_bytes_total",
			Help: "Encoded sidecar bytes produced by successful index builds, by write path.",
		}, []string{"source"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "parkana_index_build_duration_seconds",
			Help:    "Sidecar index build latency for finished builds (successful or over the gram cap), by write path.",
			Buckets: prometheus.ExponentialBuckets(0.005, 4, 8),
		}, []string{"source"}),
	}
	if err := reg.Register(m); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := are.ExistingCollector.(*Metrics); ok {
				return existing, nil
			}
			return nil, fmt.Errorf("index: register metrics: parkana_index_* already registered as %T, want %T",
				are.ExistingCollector, m)
		}
		return nil, fmt.Errorf("index: register metrics: %w", err)
	}
	return m, nil
}

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.built.Describe(ch)
	m.skipped.Describe(ch)
	m.bytes.Describe(ch)
	m.duration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.built.Collect(ch)
	m.skipped.Collect(ch)
	m.bytes.Collect(ch)
	m.duration.Collect(ch)
}

// ObserveBuild records one successful sidecar build: the build counter, the
// encoded byte total and the build duration.
func (m *Metrics) ObserveBuild(src Source, d time.Duration, sidecarBytes int) {
	s := string(src)
	m.built.WithLabelValues(s).Inc()
	m.bytes.WithLabelValues(s).Add(float64(sidecarBytes))
	m.duration.WithLabelValues(s).Observe(d.Seconds())
}

// ObserveSkip records one build that returned ErrTooManyGrams: the skip
// counter and the build duration, because the file was still read in full.
// For every source the number of observed durations equals built_total plus
// skipped_too_many_grams_total; builds that fail for other reasons are not
// observed here and belong to the caller's error metrics.
func (m *Metrics) ObserveSkip(src Source, d time.Duration) {
	s := string(src)
	m.skipped.WithLabelValues(s).Inc()
	m.duration.WithLabelValues(s).Observe(d.Seconds())
}
