package ingest

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// putKind* are the bounded values of the upload Put histogram's kind label.
const (
	putKindData    = "data"
	putKindSidecar = "sidecar"
)

// metrics holds the ingester's collectors, all registered on Deps.Metrics.
// Names are prefixed parkana_ingest_ except the uploader's stage and job
// metrics, which use parkana_upload_. Collectors are safe for concurrent use;
// the WAL's OnSync callback may invoke observeSync and uploader workers may
// observe stages from several goroutines.
type metrics struct {
	lines   *prometheus.CounterVec // lines admitted into the memtable
	bytes   *prometheus.CounterVec // decoded request bytes admitted
	walSync prometheus.Histogram   // WAL segment fsync latency
	walErrs prometheus.Counter     // WAL sync failures
	replay  *prometheus.CounterVec // records replayed per tenant

	flush *prometheus.CounterVec // generation publish attempts by result

	upload      *prometheus.CounterVec // uploader passes by result
	uploadFiles *prometheus.CounterVec
	uploadRows  *prometheus.CounterVec
	uploadBytes *prometheus.CounterVec

	// Upload stage and job metrics. Labels stay bounded: merge and source
	// bytes carry none, Put carries kind=data|sidecar.
	mergeSeconds   prometheus.Histogram     // merge stage wall time, success or error
	putSeconds     *prometheus.HistogramVec // object Put wall time by kind
	activeJobs     prometheus.Gauge         // dispatched jobs currently in a worker
	jobSourceBytes prometheus.Histogram     // compressed source bytes per started job

	// Upload admission metrics. Both carry no labels: the estimate gauge is the
	// charged weight of admitted jobs and the counter counts admitted oversized
	// attempts only.
	inFlightEstimateBytes prometheus.Gauge   // estimated merge bytes held by admitted jobs
	oversizedJobs         prometheus.Counter // admitted jobs whose estimate exceeded the budget

	// rotations mirrors each tenant's durable cumulative upload-key rotation
	// count. rotMu guards rotLast so concurrent observes add each durable
	// increment exactly once.
	rotations *prometheus.CounterVec
	rotMu     sync.Mutex
	rotLast   map[tenant.ID]uint64

	pending *prometheus.GaugeVec // sealed generations awaiting publication
}

// newMetrics registers every collector on reg. If registration fails partway,
// the collectors already registered are rolled back so a retried New starts
// clean.
func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	if reg == nil {
		return nil, errors.New("ingest: metrics registerer is required")
	}
	// Per-registration local slices: no bucket list is shared mutable package
	// state. Duration buckets span 1 ms through 30 minutes; longer or stalled
	// calls fall into +Inf.
	durationBuckets := []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30,
		60, 120, 300, 600, 1200, 1800,
	}
	// Source-byte buckets span 1 MiB through 2 GiB, plus an 8 GiB bucket for
	// the oversized single source uploadMergeMax allows to run alone.
	sourceByteBuckets := []float64{
		1 << 20, 4 << 20, 16 << 20, 64 << 20, 128 << 20, 256 << 20,
		512 << 20, 1 << 30, 2 << 30, 8 << 30,
	}
	m := &metrics{
		lines: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_lines_total",
			Help: "Log lines admitted into the ingest memtable.",
		}, []string{"tenant"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_bytes_total",
			Help: "Decoded push bytes admitted into the ingest memtable.",
		}, []string{"tenant"}),
		walSync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parkana_ingest_wal_fsync_seconds",
			Help:    "WAL segment file sync latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 10),
		}),
		walErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_ingest_wal_errors_total",
			Help: "WAL segment file sync failures.",
		}),
		replay: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_replay_records_total",
			Help: "WAL records replayed per tenant at startup.",
		}, []string{"tenant"}),
		flush: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_flush_total",
			Help: "Local Parquet generation publications by result.",
		}, []string{"result"}),
		upload: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_upload_total",
			Help: "Object store upload passes by result.",
		}, []string{"result"}),
		uploadFiles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_upload_files_total",
			Help: "Objects published to the object store per tenant.",
		}, []string{"tenant"}),
		uploadRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_upload_rows_total",
			Help: "Rows published to the object store per tenant.",
		}, []string{"tenant"}),
		uploadBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_upload_bytes_total",
			Help: "Object bytes published to the object store per tenant.",
		}, []string{"tenant"}),
		mergeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parkana_upload_merge_seconds",
			Help:    "Uploader merge stage wall time, observed for successes and errors.",
			Buckets: durationBuckets,
		}),
		putSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "parkana_upload_put_seconds",
			Help:    "Uploader object store Put wall time, observed for successes and errors.",
			Buckets: durationBuckets,
		}, []string{"kind"}),
		activeJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_upload_active_jobs",
			Help: "Upload jobs actively processing in a worker.",
		}),
		jobSourceBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "parkana_upload_job_source_bytes",
			Help:    "Compressed source-file bytes selected by one started upload job.",
			Buckets: sourceByteBuckets,
		}),
		inFlightEstimateBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_upload_inflight_estimate_bytes",
			Help: "Estimated merge bytes held by admitted upload jobs.",
		}),
		oversizedJobs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_upload_oversized_jobs_total",
			Help: "Admitted upload jobs whose estimate exceeded the memory budget and were admitted alone. Counts an admitted attempt even if canceled after acquisition, so it is not a count of completed runs.",
		}),
		rotations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_upload_key_rotations_total",
			Help: "Cumulative object-store upload key rotations per tenant, restored from the local manifest.",
		}, []string{"tenant"}),
		rotLast: make(map[tenant.ID]uint64),
		pending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "parkana_ingest_pending_generations",
			Help: "Sealed generations awaiting local publication.",
		}, []string{"tenant"}),
	}
	collectors := []prometheus.Collector{
		m.lines, m.bytes, m.walSync, m.walErrs, m.replay,
		m.flush, m.upload, m.uploadFiles, m.uploadRows, m.uploadBytes,
		m.mergeSeconds, m.putSeconds, m.activeJobs, m.jobSourceBytes,
		m.inFlightEstimateBytes, m.oversizedJobs,
		m.rotations, m.pending,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, r := range registered {
				reg.Unregister(r)
			}
			return nil, fmt.Errorf("ingest: register metrics: %w", err)
		}
		registered = append(registered, c)
	}
	return m, nil
}

// observeSync is the wal.Config.OnSync callback; it is invoked outside tenant
// locks and possibly concurrently.
func (m *metrics) observeSync(d time.Duration, err error) {
	m.walSync.Observe(d.Seconds())
	if err != nil {
		m.walErrs.Inc()
	}
}

// admit records a successfully admitted push.
func (m *metrics) admit(id tenant.ID, lines, bytes int64) {
	m.lines.WithLabelValues(string(id)).Add(float64(lines))
	m.bytes.WithLabelValues(string(id)).Add(float64(bytes))
}

func (m *metrics) publishOK()     { m.flush.WithLabelValues("ok").Inc() }
func (m *metrics) publishFailed() { m.flush.WithLabelValues("error").Inc() }

// observeMerge records one merge-source-file stage's wall time: everything
// from opening the sources through the merge and finalize, excluding catalog
// or sidecar work. The uploader observes it from a defer once the stage is
// entered, so failed merges are counted too and their error is unchanged.
func (m *metrics) observeMerge(d time.Duration) {
	if m == nil {
		return
	}
	m.mergeSeconds.Observe(d.Seconds())
}

// observePut records one Store.Put call's wall time under the bounded kind
// label. The uploader times the Put itself, not opening or validating the
// object, and observes failed and context-canceled Puts too without changing
// their error.
func (m *metrics) observePut(kind string, d time.Duration) {
	if m == nil {
		return
	}
	m.putSeconds.WithLabelValues(kind).Observe(d.Seconds())
}

// jobStarted records that a dispatched upload job entered a worker. known
// reports whether the job's exact compressed source bytes could be resolved;
// when they could not, no sample is recorded rather than a misleading zero.
func (m *metrics) jobStarted(sourceBytes int64, known bool) {
	if m == nil {
		return
	}
	m.activeJobs.Inc()
	if known {
		m.jobSourceBytes.Observe(float64(sourceBytes))
	}
}

// jobFinished records that a started upload job left its worker, on success,
// error or cancellation alike.
func (m *metrics) jobFinished() {
	if m == nil {
		return
	}
	m.activeJobs.Dec()
}

// memoryAdmitted records one admitted job's charged weight in the in-flight
// estimate gauge. The core calls it after the weight was acquired and before
// the job starts.
func (m *metrics) memoryAdmitted(lease uploadMemoryLease) {
	if m == nil {
		return
	}
	m.inFlightEstimateBytes.Add(float64(lease.Weight))
}

// memoryReleased records one admitted job returning its charged weight. The
// core calls it before the semaphore exposes the weight to another waiter.
func (m *metrics) memoryReleased(lease uploadMemoryLease) {
	if m == nil {
		return
	}
	m.inFlightEstimateBytes.Sub(float64(lease.Weight))
}

// oversizedJob counts one oversized job that was admitted after waiting. The
// core never calls it for a request canceled while waiting.
func (m *metrics) oversizedJob() {
	if m == nil {
		return
	}
	m.oversizedJobs.Inc()
}

// uploaded records one uploader pass: the pass result plus the completed
// uploads' per-tenant object, row and byte totals.
func (m *metrics) uploaded(rep UploadReport, ok bool) {
	result := "ok"
	if !ok {
		result = "error"
	}
	m.upload.WithLabelValues(result).Inc()
	for _, r := range rep.Completed {
		id := string(r.Tenant)
		m.uploadFiles.WithLabelValues(id).Inc()
		m.uploadRows.WithLabelValues(id).Add(float64(r.Rows))
		m.uploadBytes.WithLabelValues(id).Add(float64(r.Bytes))
	}
}

// observeRotations records a tenant's cumulative upload-key rotation count
// snapshot from its committed manifest. The first observation restores the
// durable total; later ones add only the positive delta, so repeated
// snapshots of an unchanged count never double-count.
func (m *metrics) observeRotations(id tenant.ID, total uint64) {
	m.rotMu.Lock()
	defer m.rotMu.Unlock()
	prev, seen := m.rotLast[id]
	if !seen {
		m.rotLast[id] = total
		if total > 0 {
			m.rotations.WithLabelValues(string(id)).Add(float64(total))
		}
		return
	}
	if total > prev {
		m.rotations.WithLabelValues(string(id)).Add(float64(total - prev))
		m.rotLast[id] = total
	}
}
