package query

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// metricPresent reports whether a metric family of name is registered on g.
func metricPresent(t *testing.T, g prometheus.Gatherer, name string) bool {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return true
		}
	}
	return false
}

// seriesPresent reports whether family name carries a sample with exactly the
// labels. A family whose collectors were rolled back (or that never
// registered) reports false; a leaked collector's pre-initialized series
// would make it true, which is what the rollback tests assert against.
func seriesPresent(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) bool {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

// TestScanMetricsZeroInitAndUnregister checks the bounded series exist at 0
// from construction and that unregister makes the registerer reusable.
func TestScanMetricsZeroInitAndUnregister(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newScanMetrics(reg)
	if err != nil {
		t.Fatalf("newScanMetrics: %v", err)
	}
	for _, result := range scanResultValues() {
		if got := metricValue(t, reg, "parkana_query_files_total", map[string]string{"result": result}); got != 0 {
			t.Errorf("files{%s} = %v at construction, want 0", result, got)
		}
	}
	if got := metricFloat(t, reg, "parkana_query_bytes_scanned_total"); got != 0 {
		t.Errorf("bytes = %v at construction, want 0", got)
	}
	m.files.WithLabelValues("scanned").Inc()
	m.unregister()
	// The registerer must accept a fresh set after unregister.
	again, err := newScanMetrics(reg)
	if err != nil {
		t.Fatalf("scan metrics were not unregistered: %v", err)
	}
	again.unregister()
}

// TestScanMetricsNilRegisterer checks a nil registerer still yields usable
// counters without registering anything.
func TestScanMetricsNilRegisterer(t *testing.T) {
	m, err := newScanMetrics(nil)
	if err != nil {
		t.Fatalf("newScanMetrics(nil): %v", err)
	}
	m.files.WithLabelValues("pruned_index").Add(2)
	m.bytes.Add(7)
	m.unregister() // must be a no-op, not a panic
}

// TestScanMetricsCollisionRollback checks a name collision with an
// already-registered collector fails the constructor and rolls the partial
// registration back. Both collector orders collide: the files counter
// (registered first, so nothing new can leak) and the bytes counter
// (registered second, so the already-registered files counter must be rolled
// back with it — its pre-initialized series would stay visible if it leaked).
// Prometheus keeps the collided name's descriptor from the failed Register
// itself, so a fresh set cannot retake that name on the same registry; the
// rollback's promise is that every OTHER name stays available.
func TestScanMetricsCollisionRollback(t *testing.T) {
	t.Run("files-counter-first", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		squatter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "parkana_query_files_total", Help: "already here"}, []string{"result"})
		squatter.WithLabelValues("squatter")
		if err := reg.Register(squatter); err != nil {
			t.Fatal(err)
		}
		if _, err := newScanMetrics(reg); err == nil {
			t.Fatal("newScanMetrics succeeded despite a name collision")
		}
		if seriesPresent(t, reg, "parkana_query_files_total", map[string]string{"result": "scanned"}) {
			t.Error("the new files series survived a collision on the first collector")
		}
		if !seriesPresent(t, reg, "parkana_query_files_total", map[string]string{"result": "squatter"}) {
			t.Error("the squatter's series did not survive the rollback")
		}
		if metricPresent(t, reg, "parkana_query_bytes_scanned_total") {
			t.Error("the never-attempted bytes counter registered")
		}
	})
	t.Run("bytes-counter-second", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		if err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "parkana_query_bytes_scanned_total", Help: "already here"})); err != nil {
			t.Fatal(err)
		}
		if _, err := newScanMetrics(reg); err == nil {
			t.Fatal("newScanMetrics succeeded despite a name collision")
		}
		// The files counter registered before the collision and must be gone:
		// its pre-initialized series would keep the family visible if it leaked.
		if seriesPresent(t, reg, "parkana_query_files_total", map[string]string{"result": "scanned"}) {
			t.Error("the files counter registered before the collision was not rolled back")
		}
		if !metricPresent(t, reg, "parkana_query_bytes_scanned_total") {
			t.Error("the squatter did not survive the rollback")
		}
	})
	// The registerer still works for every other metric group after a rollback.
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "parkana_query_bytes_scanned_total", Help: "already here"}))
	if _, err := newScanMetrics(reg); err == nil {
		t.Fatal("newScanMetrics succeeded despite a name collision")
	}
	cache, err := newObjectCache(cacheOptions{store: newMemStore(), dir: t.TempDir(), budget: 1 << 20, register: reg})
	if err != nil {
		t.Fatalf("cache metrics refused the registerer after rollback: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: newMemStore(), budget: 1024, register: reg})
	if err != nil {
		t.Fatalf("sidecar metrics refused the registerer after rollback: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
}
