package ingest

// Ingest back-pressure. Two process-wide bounds protect the ingester when
// it falls behind: the memory the memtable holds before a local flush, and
// the local Parquet waiting for upload. append checks both, lock-free, before
// a push reaches the WAL; past either, the push answers 429 with Retry-After
// and the client retries. Replay never checks: WAL data must always be
// rebuilt. There is no hysteresis: as soon as a flush or an upload brings a
// counter under its bound, the next push is admitted.

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/config"
)

// backpressureMemoryFactor is the memory bound as a multiple of
// -ingest.memtable-max: one active generation, one being flushed, and two of
// slack for a slow flush. Above it, flushes are failing or far behind.
const backpressureMemoryFactor = 4

// Values of BackpressureError.Where and of the where label of the
// back-pressure metrics.
const (
	// BackpressureMemory is the memtable bound: rows admitted but not yet
	// written to local Parquet.
	BackpressureMemory = "memory"
	// BackpressureLocal is the local disk bound: local Parquet not yet
	// uploaded to the object store.
	BackpressureLocal = "local"
)

// ErrBackpressure reports a push refused because the ingester is behind:
// memory waiting for a flush or local Parquet waiting for upload passed its
// bound. The push handlers answer 429 with Retry-After. Errors carrying it are
// *BackpressureError values.
var ErrBackpressure = errors.New("ingest: back-pressure")

// BackpressureError is a push refused by a back-pressure bound. It wraps
// ErrBackpressure; an AppendFunc may return one for the handler to answer 429
// with a Retry-After header.
type BackpressureError struct {
	// Where names the bound: BackpressureMemory or BackpressureLocal.
	Where string
	// Bytes is the counter the bound tripped on; Limit is the bound.
	Bytes, Limit int64
	// RetryAfter is how long the client should wait; the handler sends it in
	// whole seconds, rounded up, and at least 1.
	RetryAfter time.Duration
}

// Error names the bound, both byte counts and the flag that sets the bound.
func (e *BackpressureError) Error() string {
	switch e.Where {
	case BackpressureMemory:
		return fmt.Sprintf("%v: %d bytes wait in memory for a local flush, limit %d (%d x -ingest.memtable-max); retry after %ds",
			ErrBackpressure, e.Bytes, e.Limit, backpressureMemoryFactor, retryAfterSeconds(e.RetryAfter))
	case BackpressureLocal:
		return fmt.Sprintf("%v: %d bytes of local Parquet wait for upload, limit %d (-ingest.max-unuploaded); retry after %ds",
			ErrBackpressure, e.Bytes, e.Limit, retryAfterSeconds(e.RetryAfter))
	default:
		return fmt.Sprintf("%v: %s at %d bytes, limit %d; retry after %ds",
			ErrBackpressure, e.Where, e.Bytes, e.Limit, retryAfterSeconds(e.RetryAfter))
	}
}

// Unwrap returns ErrBackpressure.
func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// retryAfterSeconds rounds d up to whole seconds, at least 1, as the
// Retry-After header carries it.
func retryAfterSeconds(d time.Duration) int64 {
	return max(1, int64(math.Ceil(d.Seconds())))
}

// backpressureLimits resolves the two bounds and the Retry-After hint from
// the configuration. The memory bound saturates instead of overflowing.
func backpressureLimits(c config.Ingest) (mem, local int64, retry time.Duration) {
	mem = int64(c.MemtableMaxBytes)
	if mem > math.MaxInt64/backpressureMemoryFactor {
		mem = math.MaxInt64
	} else {
		mem *= backpressureMemoryFactor
	}
	// A flush or an upload pass is the earliest a counter can drop.
	return mem, c.MaxUnuploadedBytes(), max(c.FlushInterval, c.UploadInterval)
}

// checkBackpressure refuses a push when either bound is reached. It reads two
// atomics and takes no lock; pushes admitted concurrently can overshoot a
// bound by their own size.
func (s *service) checkBackpressure() error {
	if n := s.mem.Buffered(); n >= s.bpMemLimit {
		return s.rejectBackpressure(BackpressureMemory, n, s.bpMemLimit)
	}
	if ls := s.localRef.Load(); ls != nil {
		if n := ls.UnuploadedBytes(); n >= s.bpLocalLimit {
			return s.rejectBackpressure(BackpressureLocal, n, s.bpLocalLimit)
		}
	}
	return nil
}

func (s *service) rejectBackpressure(where string, n, limit int64) error {
	s.bp.rejections.WithLabelValues(where).Inc()
	return &BackpressureError{Where: where, Bytes: n, Limit: limit, RetryAfter: s.bpRetryAfter}
}

// bpMetrics are the back-pressure collectors. The byte gauges read the
// service's counters at scrape time.
type bpMetrics struct {
	rejections *prometheus.CounterVec
}

// newBackpressureMetrics registers the back-pressure collectors on reg:
// unuploaded bytes and limits per bound as gauge functions, and rejections per
// bound. memory and local read the current counters; they must be safe to
// call at any time. A partial registration is rolled back.
func newBackpressureMetrics(reg prometheus.Registerer, memory, local func() int64, memLimit, localLimit int64) (*bpMetrics, error) {
	m := &bpMetrics{rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parkana_ingest_backpressure_rejections_total",
		Help: "Pushes refused with 429 because a back-pressure bound was reached, by bound.",
	}, []string{"where"})}
	for _, where := range []string{BackpressureMemory, BackpressureLocal} {
		m.rejections.WithLabelValues(where)
	}
	gauge := func(name, help, where string, fn func() float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name, Help: help, ConstLabels: prometheus.Labels{"where": where},
		}, fn)
	}
	const (
		bytesName = "parkana_ingest_unuploaded_bytes"
		bytesHelp = "Bytes waiting on their way to the object store: memory is the memtable estimate of rows not yet in local Parquet, local is local Parquet not yet uploaded."
		limitName = "parkana_ingest_backpressure_limit_bytes"
		limitHelp = "Back-pressure bound per counter of parkana_ingest_unuploaded_bytes; pushes answer 429 at or above it."
	)
	collectors := []prometheus.Collector{
		m.rejections,
		gauge(bytesName, bytesHelp, BackpressureMemory, func() float64 { return float64(memory()) }),
		gauge(bytesName, bytesHelp, BackpressureLocal, func() float64 { return float64(local()) }),
		gauge(limitName, limitHelp, BackpressureMemory, func() float64 { return float64(memLimit) }),
		gauge(limitName, limitHelp, BackpressureLocal, func() float64 { return float64(localLimit) }),
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, r := range registered {
				reg.Unregister(r)
			}
			return nil, fmt.Errorf("ingest: register back-pressure metrics: %w", err)
		}
		registered = append(registered, c)
	}
	return m, nil
}
