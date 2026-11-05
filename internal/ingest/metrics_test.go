package ingest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// gather returns the value of the named counter/gauge family's first metric,
// and whether it exists.
func gather(t *testing.T, reg *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range f.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				sum += g.GetValue()
			}
		}
		return sum, true
	}
	return 0, false
}

func gatherLabeled(t *testing.T, reg *prometheus.Registry, name, label, value string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range f.GetMetric() {
			match := false
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					match = true
				}
			}
			if !match {
				continue
			}
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				sum += g.GetValue()
			}
		}
		return sum, true
	}
	return 0, false
}

// All parkana_ingest_* collectors register on the provided registerer.
func TestMetricsRegisterAll(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]prometheus.Collector{
		"lines": m.lines, "bytes": m.bytes, "walSync": m.walSync,
		"walErrs": m.walErrs, "replay": m.replay, "flush": m.flush,
		"upload": m.upload, "uploadFiles": m.uploadFiles,
		"uploadRows": m.uploadRows, "uploadBytes": m.uploadBytes,
		"mergeSeconds": m.mergeSeconds, "putSeconds": m.putSeconds,
		"activeJobs": m.activeJobs, "jobSourceBytes": m.jobSourceBytes,
		"inFlightEstimateBytes": m.inFlightEstimateBytes, "oversizedJobs": m.oversizedJobs,
		"rotations": m.rotations, "pending": m.pending,
	} {
		if !reg.Unregister(c) {
			t.Fatalf("collector %s was not registered", name)
		}
		reg.MustRegister(c)
	}
	m.admit(tenant.Default, 3, 42)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_lines_total", "tenant", "default"); v != 3 {
		t.Fatalf("lines %v, want 3", v)
	}
	m.uploaded(UploadReport{Completed: []UploadResult{{Tenant: tenant.Default, Rows: 2, Bytes: 9}}}, true)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_files_total", "tenant", "default"); v != 1 {
		t.Fatalf("upload files %v, want 1", v)
	}
	m.observeSync(time.Millisecond, nil)
	m.observeSync(time.Millisecond, errors.New("sync"))
	if v, _ := gather(t, reg, "parkana_ingest_wal_errors_total"); v != 1 {
		t.Fatalf("wal errors %v, want 1", v)
	}
}

// A conflicting registration rolls back the collectors already registered so
// a retry starts clean.
func TestMetricsRollbackOnConflict(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Conflicting on a mid-list collector exercises the rollback of the
	// collectors registered before it.
	conflict := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_ingest_flush_total",
		Help: "conflict",
	})
	reg.MustRegister(conflict)
	if _, err := newMetrics(reg); err == nil {
		t.Fatal("expected registration conflict")
	}
	// Earlier collectors were rolled back: identical collectors register
	// cleanly now (a leftover registration would fail as a duplicate).
	reg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parkana_ingest_lines_total",
		Help: "Log lines admitted into the ingest memtable.",
	}, []string{"tenant"}))
	reg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parkana_ingest_bytes_total",
		Help: "Decoded push bytes admitted into the ingest memtable.",
	}, []string{"tenant"}))
	if _, err := newMetrics(nil); err == nil {
		t.Fatal("expected error for nil registerer")
	}
}

// The rotations counter restores a tenant's durable cumulative total on first
// observation, then adds only positive deltas: repeated snapshots of an
// unchanged count never double-count.
func TestMetricsRotationsRestoreAndDeltas(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	// First observation restores the durable total.
	m.observeRotations(tenant.Default, 5)
	if v, ok := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); !ok || v != 5 {
		t.Fatalf("rotations %v (found=%v), want 5", v, ok)
	}
	// A repeated snapshot of the same total adds nothing.
	m.observeRotations(tenant.Default, 5)
	// A positive delta adds only the difference.
	m.observeRotations(tenant.Default, 7)
	// A decrease — impossible for a durable cumulative count — adds nothing.
	m.observeRotations(tenant.Default, 4)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); v != 7 {
		t.Fatalf("rotations %v, want 7", v)
	}
}

// A tenant first observed at zero stays absent from the counter until it
// actually rotates.
func TestMetricsRotationsZeroStart(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	m.observeRotations(tenant.Default, 0)
	if v, ok := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); ok && v != 0 {
		t.Fatalf("rotations %v, want 0", v)
	}
	m.observeRotations(tenant.Default, 2)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); v != 2 {
		t.Fatalf("rotations %v, want 2", v)
	}
}

// Rotation totals are labeled per tenant and never bleed across labels.
func TestMetricsRotationsTenants(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	other := tenant.ID("other")
	m.observeRotations(tenant.Default, 3)
	m.observeRotations(other, 11)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); v != 3 {
		t.Fatalf("default rotations %v, want 3", v)
	}
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "other"); v != 11 {
		t.Fatalf("other rotations %v, want 11", v)
	}
}

// Concurrent observations never double-count: the tracker serializes the
// last-seen map with the counter add.
func TestMetricsRotationsConcurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	shared := tenant.Default
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Go(func() {
			for range 10 {
				m.observeRotations(shared, 9)
			}
			// Each goroutine also owns a private tenant with a rising count.
			m.observeRotations(tenant.ID(fmt.Sprintf("g%d", g)), 1)
			m.observeRotations(tenant.ID(fmt.Sprintf("g%d", g)), 2)
		})
	}
	wg.Wait()
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "default"); v != 9 {
		t.Fatalf("shared rotations %v, want 9", v)
	}
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_upload_key_rotations_total", "tenant", "g7"); v != 2 {
		t.Fatalf("g7 rotations %v, want 2", v)
	}
}
