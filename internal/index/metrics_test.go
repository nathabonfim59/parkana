package index_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/index"
)

// indexMetricNames are the families RegisterMetrics must expose.
var indexMetricNames = []string{
	"parkana_index_built_total",
	"parkana_index_skipped_too_many_grams_total",
	"parkana_index_bytes_total",
	"parkana_index_build_duration_seconds",
}

func TestSourceValues(t *testing.T) {
	for src, want := range map[index.Source]string{
		index.SourceIngest:     "ingest",
		index.SourceCompaction: "compaction",
		index.SourceBackfill:   "backfill",
	} {
		if string(src) != want {
			t.Errorf("source value = %q, want %q", string(src), want)
		}
	}
}

func TestRegisterMetricsNilRegisterer(t *testing.T) {
	if m, err := index.RegisterMetrics(nil); err == nil || m != nil {
		t.Fatalf("RegisterMetrics(nil) = %v, %v, want nil, error", m, err)
	}
}

func TestRegisterMetricsObserves(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("RegisterMetrics: %v", err)
	}

	// Before any observation the vecs have no children, so no family exists.
	if got := familyNames(t, reg); len(got) != 0 {
		t.Fatalf("fresh registry holds %v, want no families", got)
	}

	m.ObserveBuild(index.SourceIngest, 250*time.Millisecond, 1234)
	m.ObserveSkip(index.SourceBackfill, 10*time.Millisecond)
	m.ObserveBuild(index.SourceIngest, 100*time.Millisecond, 100)

	if got := familyNames(t, reg); !slices.Equal(slices.Sorted(slices.Values(indexMetricNames)), got) {
		t.Fatalf("families = %v, want exactly %v", got, indexMetricNames)
	}

	want := []struct {
		name   string
		source string
		value  float64
	}{
		{"parkana_index_built_total", "ingest", 2},
		{"parkana_index_bytes_total", "ingest", 1334},
		{"parkana_index_build_duration_seconds", "ingest", 2},
		{"parkana_index_skipped_too_many_grams_total", "backfill", 1},
		{"parkana_index_build_duration_seconds", "backfill", 1},
		{"parkana_index_built_total", "compaction", 0},
		{"parkana_index_skipped_too_many_grams_total", "ingest", 0},
	}
	for _, w := range want {
		if got := sampleValue(t, reg, w.name, w.source); got != w.value {
			t.Errorf("%s{source=%q} = %v, want %v", w.name, w.source, got, w.value)
		}
	}
}

func TestRegisterMetricsSharedRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m1, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("first RegisterMetrics: %v", err)
	}
	m2, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("second RegisterMetrics: %v", err)
	}
	if m1 != m2 {
		t.Fatal("second RegisterMetrics returned a different handle, want the reused collector")
	}

	m1.ObserveBuild(index.SourceIngest, time.Second, 10)
	m2.ObserveBuild(index.SourceIngest, time.Second, 10)
	if got := sampleValue(t, reg, "parkana_index_built_total", "ingest"); got != 2 {
		t.Fatalf("built{ingest} after two roles observed = %v, want 2 (one shared series)", got)
	}
	m2.ObserveSkip(index.SourceCompaction, time.Second)
	if got := sampleValue(t, reg, "parkana_index_skipped_too_many_grams_total", "compaction"); got != 1 {
		t.Fatalf("skipped{compaction} = %v, want 1", got)
	}

	// A fresh registry stays independent.
	other := prometheus.NewRegistry()
	if _, err := index.RegisterMetrics(other); err != nil {
		t.Fatalf("RegisterMetrics on a second registry: %v", err)
	}
	if got := sampleValue(t, other, "parkana_index_built_total", "ingest"); got != 0 {
		t.Fatalf("second registry built{ingest} = %v, want 0", got)
	}
}

func TestRegisterMetricsRejectsIncompatible(t *testing.T) {
	cases := []struct {
		name  string
		squat func(reg prometheus.Registerer)
	}{
		{
			name: "plain counter on built_total",
			squat: func(reg prometheus.Registerer) {
				c := prometheus.NewCounter(prometheus.CounterOpts{
					Name: "parkana_index_built_total", Help: "squatter",
				})
				reg.MustRegister(c)
				c.Inc()
			},
		},
		{
			name: "wrong labels on built_total",
			squat: func(reg prometheus.Registerer) {
				v := prometheus.NewCounterVec(prometheus.CounterOpts{
					Name: "parkana_index_built_total", Help: "squatter",
				}, []string{"tenant"})
				reg.MustRegister(v)
				v.WithLabelValues("t").Inc()
			},
		},
		{
			name: "same labels different help on skipped",
			squat: func(reg prometheus.Registerer) {
				v := prometheus.NewCounterVec(prometheus.CounterOpts{
					Name: "parkana_index_skipped_too_many_grams_total", Help: "squatter",
				}, []string{"source"})
				reg.MustRegister(v)
				v.WithLabelValues("ingest").Inc()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			tc.squat(reg)

			m, err := index.RegisterMetrics(reg)
			if err == nil {
				t.Fatal("RegisterMetrics succeeded over a squatted name, want error")
			}
			if m != nil {
				t.Fatalf("RegisterMetrics returned %v with an error, want nil", m)
			}

			// The failed call must not leave any of the other families
			// registered; only the squatter's family may exist.
			got := familyNames(t, reg)
			if len(got) != 1 {
				t.Fatalf("registry after failed registration holds %v, want only the squatted family", got)
			}
		})
	}
}

// descForwarder re-exposes a harvested descriptor set as a foreign collector
// type: the registry sees descriptors identical to *index.Metrics's, so a
// later RegisterMetrics hits prometheus.AlreadyRegisteredError with an
// ExistingCollector that is not a *index.Metrics.
type descForwarder struct {
	descs []*prometheus.Desc
}

func (d descForwarder) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range d.descs {
		ch <- desc
	}
}

func (d descForwarder) Collect(ch chan<- prometheus.Metric) {}

func TestRegisterMetricsWrongCollectorType(t *testing.T) {
	// Harvest the real descriptors from a Metrics on a scratch registry.
	scratch := prometheus.NewRegistry()
	m, err := index.RegisterMetrics(scratch)
	if err != nil {
		t.Fatalf("RegisterMetrics: %v", err)
	}
	descCh := make(chan *prometheus.Desc, 4)
	go func() {
		m.Describe(descCh)
		close(descCh)
	}()
	var harvested []*prometheus.Desc
	for desc := range descCh {
		harvested = append(harvested, desc)
	}
	if len(harvested) != 4 {
		t.Fatalf("harvested %d descriptors, want 4", len(harvested))
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(descForwarder{descs: harvested})

	got, err := index.RegisterMetrics(reg)
	if err == nil {
		t.Fatal("RegisterMetrics reused a foreign collector type, want error")
	}
	if got != nil {
		t.Fatalf("RegisterMetrics returned %v with an error, want nil", got)
	}
	if !strings.Contains(err.Error(), "*index.Metrics") {
		t.Errorf("error %q does not name the expected collector type", err)
	}

	// The failed call must leave nothing of ours behind: once the lookalike
	// is unregistered, a fresh RegisterMetrics succeeds and observes.
	if !reg.Unregister(descForwarder{descs: harvested}) {
		t.Fatal("Unregister(descForwarder) = false, want true")
	}
	m2, err := index.RegisterMetrics(reg)
	if err != nil {
		t.Fatalf("RegisterMetrics after removing the lookalike: %v", err)
	}
	m2.ObserveBuild(index.SourceIngest, time.Millisecond, 7)
	if v := sampleValue(t, reg, "parkana_index_built_total", "ingest"); v != 1 {
		t.Fatalf("built{ingest} after retry = %v, want 1", v)
	}
}

// familyNames returns the sorted metric family names gathered from g.
func familyNames(t *testing.T, g prometheus.Gatherer) []string {
	t.Helper()
	fam, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make([]string, 0, len(fam))
	for _, mf := range fam {
		names = append(names, mf.GetName())
	}
	return slices.Sorted(slices.Values(names))
}

// sampleValue returns the counter value, or the histogram sample count, of
// the name{source} series gathered from g; 0 when the series does not exist.
func sampleValue(t *testing.T, g prometheus.Gatherer, name, source string) float64 {
	t.Helper()
	fam, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range fam {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() != "source" || lp.GetValue() != source {
					continue
				}
				switch {
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue()
				case m.GetHistogram() != nil:
					return float64(m.GetHistogram().GetSampleCount())
				}
				t.Fatalf("%s{source=%q} is neither counter nor histogram", name, source)
			}
		}
	}
	return 0
}
