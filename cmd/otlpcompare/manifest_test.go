package main

import (
	"path/filepath"
	"slices"
	"testing"
)

func testManifest(t *testing.T) (fixtureManifest, fixturePlan) {
	t.Helper()
	plan := buildPlan(testAnchor(), "run-1", "ns-1")
	return plan.buildManifest("http://127.0.0.1:4318/v1/logs", codecMixed, false), plan
}

func TestManifestRoundTrip(t *testing.T) {
	m, plan := testManifest(t)
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := writeManifest(path, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	loaded, times, err := readManifest(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := checkManifestMatchesPlan(loaded, plan); err != nil {
		t.Fatalf("manifest does not match plan: %v", err)
	}
	if loaded.TotalRecords != plan.total || times.first.UnixNano() != plan.first.UnixNano() {
		t.Fatalf("loaded manifest = %+v times = %+v", loaded, times)
	}
	if got, want := len(loaded.Cases), len(plan.cases); got != want {
		t.Fatalf("cases = %d, want %d", got, want)
	}
	for i, c := range loaded.Cases {
		if c.Codec != codecFor(codecMixed, i) {
			t.Fatalf("case %q codec = %q", c.Name, c.Codec)
		}
	}
}

func TestManifestRejectsTampering(t *testing.T) {
	base, plan := testManifest(t)
	tests := []struct {
		name   string
		mutate func(*fixtureManifest)
	}{
		{"fixture version", func(m *fixtureManifest) { m.FixtureVersion++ }},
		{"fixture id", func(m *fixtureManifest) { m.FixtureID = "other" }},
		{"pdata version", func(m *fixtureManifest) { m.PDataVersion = "v0.0.0" }},
		{"namespace", func(m *fixtureManifest) { m.Namespace = "other" }},
		{"codec mode", func(m *fixtureManifest) { m.CodecMode = "junk" }},
		{"total", func(m *fixtureManifest) { m.TotalRecords++ }},
		{"case count", func(m *fixtureManifest) { m.Cases[0].Records++ }},
		{"case name", func(m *fixtureManifest) { m.Cases[0].Name = "renamed" }},
		{"case codec", func(m *fixtureManifest) { m.Cases[0].Codec = "junk" }},
		{"timestamp", func(m *fixtureManifest) { m.Cases[0].Timestamps[0] = m.Cases[1].Timestamps[0] }},
		{"window", func(m *fixtureManifest) { m.WindowEnd = m.WindowStart }},
		{"missing case", func(m *fixtureManifest) { m.Cases = slices.Delete(m.Cases, 0, 1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			m.Cases = slices.Clone(base.Cases)
			for i := range m.Cases {
				m.Cases[i].Timestamps = slices.Clone(m.Cases[i].Timestamps)
			}
			tc.mutate(&m)
			_, parseErr := m.parse()
			planErr := checkManifestMatchesPlan(m, plan)
			if parseErr == nil && planErr == nil {
				t.Fatal("tampered manifest passed both parse and plan check")
			}
		})
	}
}

func TestManifestRejectsMarkupInCaseOrder(t *testing.T) {
	m, plan := testManifest(t)
	m.Cases[0], m.Cases[1] = m.Cases[1], m.Cases[0]
	if err := checkManifestMatchesPlan(m, plan); err == nil {
		t.Fatal("plan check accepted reordered cases")
	}
}
