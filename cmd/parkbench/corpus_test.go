package main

import (
	"cmp"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// testStart() returns a fixed, microsecond-aligned corpus start.
func testStart() time.Time { return time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC) }

// testConfig returns a valid corpus config with an unusual batch-independent
// step so timestamp arithmetic is easy to check.
func testConfig(target int64) CorpusConfig {
	return CorpusConfig{
		Seed:        42,
		Start:       testStart(),
		Step:        100 * time.Microsecond,
		TargetBytes: target,
	}
}

// scanResult is what an independent pass over the generated lines observed.
type scanResult struct {
	lines      int64
	rawBytes   int64
	counts     map[string]int64
	firstTS    map[string]int64
	lastTS     map[string]int64
	timestamps map[int64]int
	badLine    string
	// byStream tallies marker occurrences per service (derived from the actual
	// label set of each batch slot), bySliceStream per markerBlockSize-record
	// time slice and service, and byServiceInstance per time slice, service and
	// instance. All are read from the rendered labels, not from the slot index.
	byStream          map[streamMarker]int64
	bySliceStream     map[sliceMarker]int64
	byServiceInstance map[instanceMarker]int64
	// occurrences keeps every marker hit so a test can count any time window
	// from the rendered output alone.
	occurrences []markerOccurrence
	// tail is the final filler record, which carries no marker.
	tailLine     string
	tailSlot     int
	tailService  int
	tailInstance int
	tailTS       int64
}

// streamMarker identifies one marker class on one service.
type streamMarker struct {
	name    string
	service int
}

// sliceMarker identifies one marker class in one markerBlockSize-record time
// slice on one service.
type sliceMarker struct {
	name    string
	slice   int64
	service int
}

// instanceMarker identifies one marker class in one time slice on one
// (service, instance) label set.
type instanceMarker struct {
	name     string
	slice    int64
	service  int
	instance int
}

// slotIdentity is the (service, instance) pair of one sender label set, derived
// from the rendered label values.
type slotIdentity struct {
	service  int
	instance int
}

// markerOccurrence is one rendered marker hit.
type markerOccurrence struct {
	name string
	ts   time.Time
}

// markerStreams returns the sorted services with at least one hit of name in
// the given markerBlockSize-record time slice.
func markerStreams(bySliceStream map[sliceMarker]int64, name string, slice int64) []int {
	var out []int
	for key, n := range bySliceStream {
		if key.name == name && key.slice == slice && n > 0 {
			out = append(out, key.service)
		}
	}
	slices.Sort(out)
	return out
}

// slotIdentities derives the service and instance of every sender label set
// from the actual label values. It proves the tests do not assume slot index
// equals service: with instances they differ.
func slotIdentities(t *testing.T, c *Corpus) []slotIdentity {
	t.Helper()
	ids := make([]slotIdentity, len(c.StreamLabels()))
	for slot, labels := range c.StreamLabels() {
		serviceName, ok := labels.Get(c.cfg.StreamLabel)
		if !ok {
			t.Fatalf("slot %d labels %s have no %s", slot, labels, c.cfg.StreamLabel)
		}
		service := slices.Index(c.cfg.Streams, serviceName)
		if service < 0 {
			t.Fatalf("slot %d has unknown service %q", slot, serviceName)
		}
		raw, ok := labels.Get(instanceLabel)
		if !ok {
			t.Fatalf("slot %d labels %s have no %s", slot, labels, instanceLabel)
		}
		instance, err := strconv.Atoi(raw)
		if err != nil || instance < 0 || instance >= c.cfg.InstancesPerService {
			t.Fatalf("slot %d has invalid instance %q", slot, raw)
		}
		ids[slot] = slotIdentity{service: service, instance: instance}
	}
	return ids
}

// countInWindow counts rendered marker occurrences in the half-open interval
// [from, through).
func countInWindow(scan scanResult, name string, from, through time.Time) int64 {
	var n int64
	for _, occ := range scan.occurrences {
		if occ.name == name && !occ.ts.Before(from) && occ.ts.Before(through) {
			n++
		}
	}
	return n
}

// emittedRecord is one rendered record as pushed: its sender slot, timestamp
// and line bytes.
type emittedRecord struct {
	slot int
	ts   int64
	line string
}

// scanRecords drains c and returns every emitted record in global index order,
// which is the only order that must be batch-independent.
func scanRecords(t *testing.T, c *Corpus, batchLines int) []emittedRecord {
	t.Helper()
	var out []emittedRecord
	for {
		batch, err := c.NextBatch(batchLines)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		for slot, entries := range batch.Streams {
			for _, entry := range entries {
				out = append(out, emittedRecord{slot: slot, ts: entry.Timestamp.UnixNano(), line: entry.Line})
			}
		}
		if batch.Done {
			break
		}
	}
	slices.SortFunc(out, func(a, b emittedRecord) int { return cmp.Compare(a.ts, b.ts) })
	return out
}

// scanCorpus drains c in batches and records marker occurrences directly from
// the emitted line text, independent of the generator's tracking code.
func scanCorpus(t *testing.T, c *Corpus, batchLines int) (scanResult, CorpusInfo) {
	t.Helper()
	res := scanResult{
		counts:            make(map[string]int64, markerKinds),
		firstTS:           make(map[string]int64, markerKinds),
		lastTS:            make(map[string]int64, markerKinds),
		timestamps:        make(map[int64]int),
		byStream:          make(map[streamMarker]int64),
		bySliceStream:     make(map[sliceMarker]int64),
		byServiceInstance: make(map[instanceMarker]int64),
	}
	ids := slotIdentities(t, c)
	for {
		batch, err := c.NextBatch(batchLines)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		var gotLines, gotBytes int64
		for slot, entries := range batch.Streams {
			id := ids[slot]
			for _, entry := range entries {
				gotLines++
				gotBytes += int64(len(entry.Line))
				res.lines++
				res.rawBytes += int64(len(entry.Line))
				res.timestamps[entry.Timestamp.UnixNano()]++
				if strings.ContainsAny(entry.Line, "\r\n") {
					res.badLine = entry.Line
				}
				if int64(len(entry.Line)) < minLineBytes {
					t.Fatalf("record %d line is %d bytes, below the %d-byte overflow bound",
						res.lines, len(entry.Line), minLineBytes)
				}
				if strings.HasPrefix(entry.Line, "tail=") {
					res.tailLine = entry.Line
					res.tailSlot = slot
					res.tailService = id.service
					res.tailInstance = id.instance
					res.tailTS = entry.Timestamp.UnixNano()
				}
				index := int64(entry.Timestamp.Sub(c.start) / c.step)
				slice := index / markerBlockSize
				for kind := range markerKinds {
					name, literal := markerName(kind), markerLiteral(kind)
					n := strings.Count(entry.Line, literal)
					if n == 0 {
						continue
					}
					res.counts[name] += int64(n)
					res.byStream[streamMarker{name, id.service}] += int64(n)
					res.bySliceStream[sliceMarker{name, slice, id.service}] += int64(n)
					res.byServiceInstance[instanceMarker{name, slice, id.service, id.instance}] += int64(n)
					ts := entry.Timestamp.UnixNano()
					if first, ok := res.firstTS[name]; !ok || ts < first {
						res.firstTS[name] = ts
					}
					if last, ok := res.lastTS[name]; !ok || ts > last {
						res.lastTS[name] = ts
					}
					for range n {
						res.occurrences = append(res.occurrences, markerOccurrence{
							name: name,
							ts:   entry.Timestamp,
						})
					}
				}
			}
		}
		if gotLines != batch.Lines || gotBytes != batch.RawBytes {
			t.Fatalf("batch totals = (%d lines, %d bytes), batch fields = (%d, %d)",
				gotLines, gotBytes, batch.Lines, batch.RawBytes)
		}
		if batch.Done {
			break
		}
	}
	return res, c.Manifest()
}

// checkManifest verifies the manifest against the scanned lines.
func checkManifest(t *testing.T, c *Corpus, scan scanResult, manifest CorpusInfo) {
	t.Helper()
	if manifest.RawLineBytes != manifest.TargetRawLineBytes {
		t.Fatalf("raw line bytes = %d, want exact target %d", manifest.RawLineBytes, manifest.TargetRawLineBytes)
	}
	if manifest.RawLineBytes != scan.rawBytes || manifest.Lines != scan.lines {
		t.Fatalf("manifest totals = (%d lines, %d bytes), scan = (%d, %d)",
			manifest.Lines, manifest.RawLineBytes, scan.lines, scan.rawBytes)
	}
	if manifest.InstancesPerService <= 0 {
		t.Fatalf("manifest instances_per_service = %d, want positive", manifest.InstancesPerService)
	}
	if want := len(manifest.Streams) * manifest.InstancesPerService; want != len(c.StreamLabels()) {
		t.Fatalf("manifest %d services times %d instances = %d label sets, corpus exposes %d",
			len(manifest.Streams), manifest.InstancesPerService, want, len(c.StreamLabels()))
	}
	wantThrough := testStart().Add(time.Duration(manifest.Lines) * c.step)
	if !manifest.Through.Equal(wantThrough) {
		t.Fatalf("through = %v, want %v", manifest.Through, wantThrough)
	}
	if scan.badLine != "" {
		t.Fatalf("line contains a newline: %q", scan.badLine)
	}

	// Every timestamp is the k-th step after the start, with no gaps or
	// duplicates, even though streams are pushed concurrently.
	if int64(len(scan.timestamps)) != manifest.Lines {
		t.Fatalf("distinct timestamps = %d, want %d", len(scan.timestamps), manifest.Lines)
	}
	for ts, n := range scan.timestamps {
		if n != 1 {
			t.Fatalf("timestamp %d emitted %d times", ts, n)
		}
		offset := ts - testStart().UnixNano()
		if offset%c.step.Nanoseconds() != 0 {
			t.Fatalf("timestamp %d is not step-aligned", ts)
		}
		if k := offset / c.step.Nanoseconds(); k < 0 || k >= manifest.Lines {
			t.Fatalf("timestamp index %d out of [0,%d)", k, manifest.Lines)
		}
	}

	var streamLines, streamBytes int64
	for _, s := range manifest.Streams {
		streamLines += s.Lines
		streamBytes += s.RawLineBytes
		if got, ok := s.Labels[manifest.StreamLabel]; !ok || got != s.Name {
			t.Fatalf("stream %q labels %v missing %s=%s", s.Name, s.Labels, manifest.StreamLabel, s.Name)
		}
	}
	if streamLines != manifest.Lines || streamBytes != manifest.RawLineBytes {
		t.Fatalf("stream totals = (%d, %d), want (%d, %d)", streamLines, streamBytes, manifest.Lines, manifest.RawLineBytes)
	}
	var templateLines, templateBytes int64
	for _, tmpl := range manifest.Templates {
		templateLines += tmpl.Count
		templateBytes += tmpl.RawLineBytes
	}
	if templateLines != manifest.Lines || templateBytes != manifest.RawLineBytes {
		t.Fatalf("template totals = (%d, %d), want (%d, %d)", templateLines, templateBytes, manifest.Lines, manifest.RawLineBytes)
	}

	for _, marker := range manifest.Markers {
		if got, want := marker.Count, scan.counts[marker.Name]; got != want {
			t.Fatalf("marker %s count = %d, scanned %d", marker.Name, got, want)
		}
		var rangeCount int64
		for _, r := range marker.Ranges {
			if !r.From.Before(r.Through) {
				t.Fatalf("marker %s has empty range %v", marker.Name, r)
			}
			rangeCount += r.Count
		}
		if rangeCount != marker.Count {
			t.Fatalf("marker %s range counts sum to %d, total %d", marker.Name, rangeCount, marker.Count)
		}
		if marker.Count == 0 {
			if len(marker.Ranges) != 0 {
				t.Fatalf("marker %s has ranges but no hits", marker.Name)
			}
			continue
		}
		if len(marker.Ranges) == 0 {
			t.Fatalf("marker %s has hits but no ranges", marker.Name)
		}
		if got, want := marker.Ranges[0].From.UnixNano(), scan.firstTS[marker.Name]; got != want {
			t.Fatalf("marker %s first range starts at %d, first hit %d", marker.Name, got, want)
		}
		last := marker.Ranges[len(marker.Ranges)-1]
		if got, want := last.Through.UnixNano(), scan.lastTS[marker.Name]+c.step.Nanoseconds(); got != want {
			t.Fatalf("marker %s last range ends at %d, last hit+step %d", marker.Name, got, want)
		}
		full, err := manifest.ExpectedMarkerCount(marker.Name, manifest.Start, manifest.Through)
		if err != nil {
			t.Fatalf("ExpectedMarkerCount(%s, full): %v", marker.Name, err)
		}
		if full != marker.Count {
			t.Fatalf("ExpectedMarkerCount(%s, full) = %d, manifest count %d", marker.Name, full, marker.Count)
		}
		for _, r := range marker.Ranges {
			got, err := manifest.ExpectedMarkerCount(marker.Name, r.From, r.Through)
			if err != nil {
				t.Fatalf("ExpectedMarkerCount(%s, range): %v", marker.Name, err)
			}
			if got != r.Count {
				t.Fatalf("ExpectedMarkerCount(%s, [%v,%v)) = %d, range count %d", marker.Name, r.From, r.Through, got, r.Count)
			}
		}
	}
}

// TestCorpusDefaultInstanceLabelSets is the regression for client parallelism:
// the default corpus must expose 16 full sender label sets (four services times
// four deterministic instances) while the manifest keeps four service
// aggregates. It uses only the existing API, so its failure is behavioural.
func TestCorpusDefaultInstanceLabelSets(t *testing.T) {
	c, err := NewCorpus(testConfig(200_000))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	labels := c.StreamLabels()
	if got, want := len(labels), 16; got != want {
		t.Fatalf("default full label sets = %d, want %d", got, want)
	}
	manifest := c.Manifest()
	if got, want := len(manifest.Streams), 4; got != want {
		t.Fatalf("manifest service aggregates = %d, want %d", got, want)
	}
	perService := make(map[string]map[string]bool)
	seen := make(map[string]bool, len(labels))
	for _, ls := range labels {
		key := ls.String()
		if seen[key] {
			t.Fatalf("duplicate full label set %s", key)
		}
		seen[key] = true
		service, ok := ls.Get(defaultStreamLabel)
		if !ok {
			t.Fatalf("label set %s has no %s label", key, defaultStreamLabel)
		}
		instance, ok := ls.Get("instance")
		if !ok {
			t.Fatalf("label set %s has no instance label", key)
		}
		if perService[service] == nil {
			perService[service] = make(map[string]bool)
		}
		perService[service][instance] = true
	}
	if len(perService) != 4 {
		t.Fatalf("full label sets cover %d services, want 4", len(perService))
	}
	for service, instances := range perService {
		if len(instances) != 4 {
			t.Fatalf("service %q covers %d instances, want 4", service, len(instances))
		}
	}
}

// TestCorpusConfigurableInstanceCounts checks the configurable full label set
// layout: one, two and four instances on the default services and a custom
// three-service corpus. The manifest keeps one aggregate per service.
func TestCorpusConfigurableInstanceCounts(t *testing.T) {
	names := defaultStreamNames()
	tests := []struct {
		name       string
		streams    []string
		instances  int
		wantLabels int
	}{
		{"one instance", nil, 1, 4},
		{"two instances", nil, 2, 8},
		{"four instances", nil, 4, 16},
		{"three services four instances", names[:3], 4, 12},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(4096)
			cfg.Streams = tc.streams
			cfg.InstancesPerService = tc.instances
			c, err := NewCorpus(cfg)
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			scan, manifest := scanCorpus(t, c, 32)
			checkManifest(t, c, scan, manifest)
			if got := len(c.StreamLabels()); got != tc.wantLabels {
				t.Fatalf("full label sets = %d, want %d", got, tc.wantLabels)
			}
			wantServices := len(cfg.Streams)
			if wantServices == 0 {
				wantServices = len(defaultStreamNames())
			}
			if len(manifest.Streams) != wantServices {
				t.Fatalf("manifest services = %d, want %d", len(manifest.Streams), wantServices)
			}
			if manifest.InstancesPerService != tc.instances {
				t.Fatalf("manifest instances_per_service = %d, want %d", manifest.InstancesPerService, tc.instances)
			}
			byService := make(map[string]map[string]bool, wantServices)
			for _, labels := range c.StreamLabels() {
				service, ok := labels.Get(defaultStreamLabel)
				if !ok {
					t.Fatalf("label set %s has no service name", labels)
				}
				instance, ok := labels.Get(instanceLabel)
				if !ok {
					t.Fatalf("label set %s has no instance", labels)
				}
				if byService[service] == nil {
					byService[service] = make(map[string]bool, tc.instances)
				}
				if byService[service][instance] {
					t.Fatalf("duplicate service %q instance %q", service, instance)
				}
				byService[service][instance] = true
			}
			if len(byService) != wantServices {
				t.Fatalf("label sets cover %d services, want %d", len(byService), wantServices)
			}
			for service, instances := range byService {
				if len(instances) != tc.instances {
					t.Fatalf("service %q covers %d instances, want %d", service, len(instances), tc.instances)
				}
			}
		})
	}
}

// TestCorpusInstanceAssignmentCoversStrides proves the pure instance formula
// cannot alias with the frequent, rare or short marker strides: for i=8j, 16j
// and 32j the generator's actual streamFor/instanceFor outputs stay in range and
// visit every (service, instance) pair over enough records.
func TestCorpusInstanceAssignmentCoversStrides(t *testing.T) {
	for _, services := range []int{1, 3, 4, 8} {
		for _, instances := range []int{1, 2, 4} {
			for _, stride := range []int64{8, 16, 32} {
				name := fmt.Sprintf("services=%d/instances=%d/stride=%d", services, instances, stride)
				t.Run(name, func(t *testing.T) {
					seen := make(map[[2]int]bool, services*instances)
					cohorts := int64(stride) * int64(services) * int64(instances)
					for j := range cohorts {
						i := int64(stride) * j
						service := streamFor(i, services)
						instance := instanceFor(i, services, instances)
						if service < 0 || service >= services {
							t.Fatalf("record %d service = %d, outside [0,%d)", i, service, services)
						}
						if instance < 0 || instance >= instances {
							t.Fatalf("record %d instance = %d, outside [0,%d)", i, instance, instances)
						}
						seen[[2]int{service, instance}] = true
					}
					if len(seen) != services*instances {
						t.Fatalf("stride %d covered %d of %d service-instance pairs", stride, len(seen), services*instances)
					}
				})
			}
		}
	}
}

// TestCorpusInstancesPreserveV2Rendering proves instances only relabel records:
// compared with instances=1, line bytes, timestamps, per-service totals and
// marker placement are identical, while the corpus identity hash changes. It
// also pins the service assignment to the version-2 streamFor mapping.
func TestCorpusInstancesPreserveV2Rendering(t *testing.T) {
	one := testConfig(200_000)
	one.InstancesPerService = 1
	first, err := NewCorpus(one)
	if err != nil {
		t.Fatalf("NewCorpus instances=1: %v", err)
	}
	four := testConfig(200_000)
	four.InstancesPerService = 4
	second, err := NewCorpus(four)
	if err != nil {
		t.Fatalf("NewCorpus instances=4: %v", err)
	}
	firstScan, firstManifest := scanCorpus(t, first, 97)
	secondScan, secondManifest := scanCorpus(t, second, 5000)
	checkManifest(t, first, firstScan, firstManifest)
	checkManifest(t, second, secondScan, secondManifest)

	if firstManifest.ContentHash != secondManifest.ContentHash {
		t.Fatal("instance count changed the emitted line bytes")
	}
	if firstManifest.Hash == secondManifest.Hash {
		t.Fatal("instance count is missing from the corpus identity hash")
	}
	if !reflect.DeepEqual(firstManifest.Streams, secondManifest.Streams) {
		t.Fatalf("per-service aggregates changed:\n%+v\n%+v", firstManifest.Streams, secondManifest.Streams)
	}
	if !reflect.DeepEqual(firstManifest.Markers, secondManifest.Markers) {
		t.Fatal("marker placement changed with the instance count")
	}
	if !reflect.DeepEqual(firstManifest.Templates, secondManifest.Templates) {
		t.Fatal("template totals changed with the instance count")
	}
	if firstManifest.Lines != secondManifest.Lines || firstManifest.RawLineBytes != secondManifest.RawLineBytes {
		t.Fatal("corpus size changed with the instance count")
	}
	if firstManifest.InstancesPerService != 1 || secondManifest.InstancesPerService != 4 {
		t.Fatalf("manifest instance counts = %d / %d", firstManifest.InstancesPerService, secondManifest.InstancesPerService)
	}
	// The single-instance corpus slot equals its service: no relabeling of the
	// version-2 layout. Every record keeps its version-2 line, timestamp and
	// service. The sampled indices include hits in rare block A and short block
	// A, so the instance-count comparison reaches those markers without
	// generating two large corpora.
	numServices := len(firstManifest.Streams)
	samples := make([]int64, 0, 2_000+6)
	for i := range int64(2_000) {
		samples = append(samples, i)
	}
	rareBase := rareBlockA * markerBlockSize
	shortBase := shortBlockA * markerBlockSize
	markerSamples := []struct {
		index   int64
		literal string
	}{
		{rareBase, rareMarker},
		{rareBase + rareEvery, rareMarker},
		{shortBase, shortNeedle},
		{shortBase + shortEvery, shortNeedle},
	}
	for _, sample := range markerSamples {
		samples = append(samples, sample.index)
	}
	for _, i := range samples {
		line1, service1, ts1, _, _ := first.record(i)
		line4, service4, ts4, _, _ := second.record(i)
		if line1 != line4 || service1 != service4 || !ts1.Equal(ts4) {
			t.Fatalf("record %d changed with instances: %q/%d/%v vs %q/%d/%v",
				i, line1, service1, ts1, line4, service4, ts4)
		}
		// The version-2 stream mapping, written out literally rather than
		// reusing streamFor, so a change to that helper cannot silently move
		// the reference the test compares against.
		want := int((i%int64(shortEvery) + i/int64(shortEvery)) % int64(numServices))
		if service4 != want {
			t.Fatalf("record %d service = %d, want literal version-2 mapping %d", i, service4, want)
		}
		if got := instanceFor(i, numServices, 4); got < 0 || got >= 4 {
			t.Fatalf("record %d instance = %d, want 0..3", i, got)
		}
	}
	for _, sample := range markerSamples {
		line, _, _, _, _ := second.record(sample.index)
		if !strings.Contains(line, sample.literal) {
			t.Fatalf("sampled record %d does not carry %s: %q", sample.index, sample.literal, line)
		}
	}
	if firstManifest.Streams[0].Labels[instanceLabel] != "" {
		t.Fatal("per-service aggregate labels must not carry the instance label")
	}
}

// TestCorpusBatchSlotsUseActualLabels drains batches and derives service and
// instance from each slot's rendered labels, proving the slot order and the
// generator's record contract agree without assuming slot==service.
func TestCorpusBatchSlotsUseActualLabels(t *testing.T) {
	c, err := NewCorpus(testConfig(200_000))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	ids := slotIdentities(t, c)
	used := make(map[int]bool, len(ids))
	for {
		batch, err := c.NextBatch(97)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		for slot, entries := range batch.Streams {
			id := ids[slot]
			var previous int64
			for _, entry := range entries {
				index := int64(entry.Timestamp.Sub(c.start) / c.step)
				line, service, _, _, _ := c.record(index)
				if service != id.service || instanceFor(index, len(c.cfg.Streams), c.cfg.InstancesPerService) != id.instance {
					t.Fatalf("slot %d holds record %d of service %d instance %d, labels say service %d instance %d",
						slot, index, service, instanceFor(index, len(c.cfg.Streams), c.cfg.InstancesPerService),
						id.service, id.instance)
				}
				if !strings.HasPrefix(entry.Line, "tail=") && line != entry.Line {
					t.Fatalf("slot %d record %d line differs from record(): %q vs %q", slot, index, entry.Line, line)
				}
				if slot != id.service*c.cfg.InstancesPerService+id.instance {
					t.Fatalf("slot %d is not service-major for service %d instance %d", slot, id.service, id.instance)
				}
				if ts := entry.Timestamp.UnixNano(); ts <= previous {
					t.Fatalf("slot %d entries are not ordered: %d after %d", slot, ts, previous)
				} else {
					previous = ts
				}
				used[slot] = true
			}
		}
		if batch.Done {
			break
		}
	}
	if len(used) != 16 {
		t.Fatalf("batches used %d of 16 label-set slots", len(used))
	}
}

func TestCorpusExactTargetAndDeterminism(t *testing.T) {
	const target = 200_000
	first, err := NewCorpus(testConfig(target))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	firstScan, firstManifest := scanCorpus(t, first, 97)
	checkManifest(t, first, firstScan, firstManifest)
	if len(firstScan.timestamps) < 100 {
		t.Fatalf("corpus has only %d records; batching test is not meaningful", len(firstScan.timestamps))
	}

	second, err := NewCorpus(testConfig(target))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	secondScan, secondManifest := scanCorpus(t, second, 5000)
	checkManifest(t, second, secondScan, secondManifest)
	if !reflect.DeepEqual(firstManifest, secondManifest) {
		t.Fatalf("manifest depends on batch size:\nfirst  %+v\nsecond %+v", firstManifest, secondManifest)
	}
	if firstManifest.Hash == "" || firstManifest.ContentHash == "" {
		t.Fatalf("manifest hash fields must be set: %+v", firstManifest)
	}

	other, err := NewCorpus(CorpusConfig{
		Seed: 43, Start: testStart(), Step: 100 * time.Microsecond, TargetBytes: target,
	})
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	_, otherManifest := scanCorpus(t, other, 1000)
	if otherManifest.Hash == firstManifest.Hash {
		t.Fatal("different seeds produced the same config hash")
	}
	if otherManifest.ContentHash == firstManifest.ContentHash {
		t.Fatal("different seeds produced the same content hash")
	}
}

// TestCorpusEmittedRecordsAreBatchIndependent compares the actual pushed tuples
// (stream, timestamp, line), not just aggregate hashes, across batch sizes. It
// proves batching never changes placement; it does not by itself prove which
// stream assignment is correct.
func TestCorpusEmittedRecordsAreBatchIndependent(t *testing.T) {
	tests := []struct {
		name    string
		streams []string
	}{
		{"default streams", nil},
		{"three streams", []string{"api", "checkout", "search"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(200_000)
			cfg.Streams = tc.streams
			first, err := NewCorpus(cfg)
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			smallBatches := scanRecords(t, first, 97)
			second, err := NewCorpus(cfg)
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			largeBatches := scanRecords(t, second, 5000)
			if !reflect.DeepEqual(smallBatches, largeBatches) {
				t.Fatalf("emitted (stream, timestamp, line) tuples depend on batch size")
			}
			streams := make(map[int]bool, len(first.StreamLabels()))
			for _, rec := range smallBatches {
				streams[rec.slot] = true
			}
			if want := len(first.StreamLabels()); len(streams) != want {
				t.Fatalf("corpus used %d streams, want all %d", len(streams), want)
			}
		})
	}
}

func TestCorpusMarkerSchedules(t *testing.T) {
	// 100 MB reaches rare block 2 (records 131072..196607) and short block 4
	// (records 262144..327679) in full, but not rare block 9.
	const target = 100_000_000
	c, err := NewCorpus(testConfig(target))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	scan, manifest := scanCorpus(t, c, 10_000)
	checkManifest(t, c, scan, manifest)

	byName := make(map[string]MarkerInfo, markerKinds)
	for _, marker := range manifest.Markers {
		byName[marker.Name] = marker
	}
	if got := byName["absent"].Count; got != 0 {
		t.Fatalf("absent marker emitted %d times", got)
	}
	rare := byName["rare"]
	if rare.Count == 0 {
		t.Fatal("rare marker not reached by the test corpus")
	}
	if len(rare.Ranges) == 0 || len(rare.Ranges) > len(rareBlockSet()) {
		t.Fatalf("rare ranges = %d, want at most one per rare block", len(rare.Ranges))
	}
	for i, r := range rare.Ranges {
		if r.Count <= 0 {
			t.Fatalf("rare range %d has count %d", i, r.Count)
		}
		if i > 0 && !rare.Ranges[i-1].Through.Before(r.From) {
			t.Fatalf("rare ranges are not disjoint and ordered: %+v", rare.Ranges)
		}
	}
	if got, want := rare.Ranges[0].Count, int64(markerBlockSize/rareEvery); got != want {
		t.Fatalf("full rare block count = %d, want %d", got, want)
	}
	short := byName["short_4byte"]
	if short.Count == 0 {
		t.Fatal("short needle not reached by the test corpus")
	}
	if got, want := short.Ranges[0].Count, int64(markerBlockSize/shortEvery); got != want {
		t.Fatalf("full short block count = %d, want %d", got, want)
	}
	if got, want := byName["frequent"].Count, (manifest.Lines+int64(frequentEvery)-2)/int64(frequentEvery); got != want {
		t.Fatalf("frequent count = %d, want %d", got, want)
	}

	// Nothing else in the corpus may contain the absent literal, and each
	// marker literal must be unique: counts above already prove the latter.
	if scan.counts["absent"] != 0 {
		t.Fatalf("absent literal appeared in generated text")
	}

	// B2b may pick any common half-open interval. Check one that does not
	// start or end on a hit against an independent record-index count.
	step := time.Duration(manifest.StepMicros) * time.Microsecond
	from := rare.Ranges[0].From.Add(7*step + 37*time.Microsecond)
	through := from.Add(500 * step)
	lo := int64(max((from.Sub(manifest.Start)+step-1)/step, 0))
	hi := int64((through.Sub(manifest.Start) + step - 1) / step)
	var want int64
	for k := lo; k < hi; k++ {
		if k%rareEvery == 0 && slices.Contains(rareBlockSet(), k/markerBlockSize) {
			want++
		}
	}
	got, err := manifest.ExpectedMarkerCount("rare", from, through)
	if err != nil {
		t.Fatalf("ExpectedMarkerCount(rare, arbitrary range): %v", err)
	}
	if got != want {
		t.Fatalf("ExpectedMarkerCount(rare, arbitrary range) = %d, want %d", got, want)
	}
	if got, err := manifest.ExpectedMarkerCount("absent", from, through); err != nil || got != 0 {
		t.Fatalf("ExpectedMarkerCount(absent) = %d, %v; want 0", got, err)
	}
	if _, err := manifest.ExpectedMarkerCount("nope", from, through); err == nil {
		t.Fatal("ExpectedMarkerCount accepted an unknown marker")
	}
	if got, err := manifest.ExpectedMarkerCount("rare", manifest.Start.Add(-time.Hour), manifest.Start); err != nil || got != 0 {
		t.Fatalf("ExpectedMarkerCount(range before corpus) = %d, %v; want 0", got, err)
	}
}

// TestCorpusMarkerStreamPlacement proves, from the rendered batches alone, that
// marker presence is not correlated with one stream: the frequent marker reaches
// every configured stream in every full 65536-record time slice, and the rare
// and four-byte markers stay in their existing blocks while spanning streams.
func TestCorpusMarkerStreamPlacement(t *testing.T) {
	// 100 MB reaches the first full time slices, rare block 2 and short block 4.
	const target = 100_000_000
	c, err := NewCorpus(testConfig(target))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	scan, manifest := scanCorpus(t, c, 10_000)
	checkManifest(t, c, scan, manifest)

	numStreams := len(manifest.Streams)
	if numStreams < 2 {
		t.Fatalf("corpus has %d streams, want the default multi-stream config", numStreams)
	}
	wantStreams := make([]int, numStreams)
	for i := range numStreams {
		wantStreams[i] = i
	}
	if got := scan.counts["absent"]; got != 0 {
		t.Fatalf("absent marker emitted %d times", got)
	}

	normal := manifest.Lines - 1
	fullSlices := normal / markerBlockSize
	if fullSlices < 2 {
		t.Fatalf("corpus has %d full %d-record time slices, want at least 2", fullSlices, markerBlockSize)
	}
	// checkExactPerStream asserts the exact per-service hit count in a full
	// markerBlockSize slice, markerBlockSize/(every*numStreams), from the
	// rendered counters, and the exact per-(service, instance) count
	// markerBlockSize/(every*numStreams*instances). The latter proves every
	// instance receives the marker in a full block, not just every service.
	checkExactPerStream := func(name string, every int64, slice int64) {
		t.Helper()
		want := int64(markerBlockSize) / (every * int64(numStreams))
		for _, service := range wantStreams {
			if got := scan.bySliceStream[sliceMarker{name, slice, service}]; got != want {
				t.Fatalf("%s marker in full slice %d service %d = %d hits, want %d", name, slice, service, got, want)
			}
		}
		wantEach := int64(markerBlockSize) / (every * int64(numStreams) * int64(manifest.InstancesPerService))
		for _, service := range wantStreams {
			for instance := range manifest.InstancesPerService {
				if got := scan.byServiceInstance[instanceMarker{name, slice, service, instance}]; got != wantEach {
					t.Fatalf("%s marker in full slice %d service %d instance %d = %d hits, want %d",
						name, slice, service, instance, got, wantEach)
				}
			}
		}
	}
	for slice := range fullSlices {
		if got := markerStreams(scan.bySliceStream, "frequent", slice); !slices.Equal(got, wantStreams) {
			t.Fatalf("frequent marker streams in slice %d = %v, want every stream %v", slice, got, wantStreams)
		}
		checkExactPerStream("frequent", frequentEvery, slice)
	}
	for service := range numStreams {
		if got := scan.byStream[streamMarker{"frequent", service}]; got == 0 {
			t.Fatalf("frequent marker never appeared in service %d", service)
		}
	}

	// checkBlocks verifies that a marker occurs only in its configured blocks
	// and spans streams there. A block truncated by the target only promises
	// the >=2 streams that its hits can prove.
	checkBlocks := func(name string, every int64, inBlock func(int64) bool) []int64 {
		t.Helper()
		var blocks []int64
		for key, n := range scan.bySliceStream {
			if key.name != name || n == 0 {
				continue
			}
			if !inBlock(key.slice) {
				t.Fatalf("%s marker appeared outside its configured blocks, in slice %d", name, key.slice)
			}
			if !slices.Contains(blocks, key.slice) {
				blocks = append(blocks, key.slice)
			}
		}
		if len(blocks) == 0 {
			t.Fatalf("%s marker not reached by the test corpus", name)
		}
		slices.Sort(blocks)
		for _, slice := range blocks {
			got := markerStreams(scan.bySliceStream, name, slice)
			if full := (slice+1)*markerBlockSize <= normal; full {
				if !slices.Equal(got, wantStreams) {
					t.Fatalf("%s marker streams in full slice %d = %v, want every stream %v", name, slice, got, wantStreams)
				}
				checkExactPerStream(name, every, slice)
			}
			if len(got) < min(2, numStreams) {
				t.Fatalf("%s marker in slice %d spans streams %v, want at least %d", name, slice, got, min(2, numStreams))
			}
		}
		return blocks
	}
	rareSlices := checkBlocks("rare", rareEvery, isRareBlock)
	shortSlices := checkBlocks("short_4byte", shortEvery, isShortBlock)
	if !slices.Contains(rareSlices, rareBlockA) {
		t.Fatalf("rare marker slices = %v, want existing block %d", rareSlices, rareBlockA)
	}
	if !slices.Contains(shortSlices, shortBlockA) {
		t.Fatalf("short marker slices = %v, want existing block %d", shortSlices, shortBlockA)
	}
	for _, slice := range shortSlices {
		if isRareBlock(slice) {
			t.Fatalf("short marker shares rare block %d", slice)
		}
	}

	// Exact schedule counts for partial windows must match the rendered
	// occurrences, including unaligned ranges that cross a marker-block
	// boundary.
	step := time.Duration(manifest.StepMicros) * time.Microsecond
	at := func(index int64, offset time.Duration) time.Time {
		return manifest.Start.Add(time.Duration(index)*step + offset)
	}
	windows := []struct {
		name          string
		from, through time.Time
	}{
		{"frequent", at(12_345, 37*time.Microsecond), at(54_321, 37*time.Microsecond)},
		{"rare", at(rareBlockA*markerBlockSize+1_000, 37*time.Microsecond), at(rareBlockA*markerBlockSize+5_000, 37*time.Microsecond)},
		{"rare", at(rareBlockA*markerBlockSize-3_000, 37*time.Microsecond), at(rareBlockA*markerBlockSize+3_000, 37*time.Microsecond)},
		{"short_4byte", at(shortBlockA*markerBlockSize+777, 37*time.Microsecond), at(shortBlockA*markerBlockSize+3_333, 37*time.Microsecond)},
		{"short_4byte", at(shortBlockA*markerBlockSize-2_000, 37*time.Microsecond), at(shortBlockA*markerBlockSize+2_000, 37*time.Microsecond)},
		{"absent", manifest.Start, manifest.Through},
	}
	for _, w := range windows {
		want, err := manifest.ExpectedMarkerCount(w.name, w.from, w.through)
		if err != nil {
			t.Fatalf("ExpectedMarkerCount(%s, [%v,%v)): %v", w.name, w.from, w.through, err)
		}
		if got := countInWindow(scan, w.name, w.from, w.through); got != want {
			t.Fatalf("ExpectedMarkerCount(%s, [%v,%v)) = %d, rendered occurrences = %d",
				w.name, w.from, w.through, want, got)
		}
	}
}

// TestCorpusMarkerPlacementCustomStreamCounts exercises stream counts whose
// strides alias with marker periods. A full block must reach every configured
// stream; the three-stream case rejects (i+i/shortEvery)%N, which leaves the
// four-byte marker in a single stream when shortEvery is not a multiple of 3.
func TestCorpusMarkerPlacementCustomStreamCounts(t *testing.T) {
	names := []string{"api", "checkout", "search", "worker", "billing", "auth", "media", "edge"}
	tests := []struct {
		name    string
		streams int
	}{
		{"one stream", 1},
		{"three streams", 3},
		{"eight streams", 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(4096)
			cfg.Streams = names[:tc.streams]
			c, err := NewCorpus(cfg)
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			bySliceStream := make(map[sliceMarker]int64)
			byServiceInstance := make(map[instanceMarker]int64)
			for _, block := range []struct {
				name string
				from int64
			}{
				{"frequent", 0},
				{"rare", rareBlockA * markerBlockSize},
				{"short_4byte", shortBlockA * markerBlockSize},
			} {
				for i := block.from; i < block.from+markerBlockSize; i++ {
					line, service, _, _, _ := c.record(i)
					instance := instanceFor(i, tc.streams, c.cfg.InstancesPerService)
					slice := i / markerBlockSize
					for kind := range markerKinds {
						if strings.Contains(line, markerLiteral(kind)) {
							bySliceStream[sliceMarker{markerName(kind), slice, service}]++
							byServiceInstance[instanceMarker{markerName(kind), slice, service, instance}]++
						}
					}
				}
			}
			wantStreams := make([]int, tc.streams)
			for i := range tc.streams {
				wantStreams[i] = i
			}
			if got := markerStreams(bySliceStream, "frequent", 0); !slices.Equal(got, wantStreams) {
				t.Fatalf("frequent marker streams in the first full slice = %v, want every stream %v", got, wantStreams)
			}
			for service := range tc.streams {
				for instance := range c.cfg.InstancesPerService {
					if got := byServiceInstance[instanceMarker{"frequent", 0, service, instance}]; got == 0 {
						t.Fatalf("frequent marker never reached service %d instance %d in the first full slice", service, instance)
					}
				}
			}
			for _, name := range []string{"rare", "short_4byte", "absent"} {
				if got := markerStreams(bySliceStream, name, 0); len(got) != 0 {
					t.Fatalf("%s marker appeared in slice 0: streams %v", name, got)
				}
			}
			for _, block := range []struct {
				name  string
				slice int64
			}{
				{"rare", rareBlockA},
				{"short_4byte", shortBlockA},
			} {
				if got := markerStreams(bySliceStream, block.name, block.slice); !slices.Equal(got, wantStreams) {
					t.Fatalf("%s marker streams in full block %d = %v, want every stream %v",
						block.name, block.slice, got, wantStreams)
				}
			}
		})
	}
}

// TestCorpusTailUsesRotatingStream proves the final filler record uses the same
// stream assignment as a normal record at its index, without promising marker
// coverage the unmarked tail cannot have.
func TestCorpusTailUsesRotatingStream(t *testing.T) {
	tests := []struct {
		name    string
		streams []string
	}{
		{"default streams", nil},
		{"three streams", []string{"api", "checkout", "search"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(100_000)
			cfg.Streams = tc.streams
			c, err := NewCorpus(cfg)
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			scan, manifest := scanCorpus(t, c, 37)
			checkManifest(t, c, scan, manifest)
			if scan.tailLine == "" {
				t.Fatal("corpus emitted no tail record")
			}
			tailIndex := manifest.Lines - 1
			_, wantService, wantTS, _, _ := c.record(tailIndex)
			numStreams := len(manifest.Streams)
			wantInstance := instanceFor(tailIndex, numStreams, manifest.InstancesPerService)
			if scan.tailService != wantService {
				t.Fatalf("tail service = %d, normal record %d service = %d", scan.tailService, tailIndex, wantService)
			}
			if scan.tailInstance != wantInstance {
				t.Fatalf("tail instance = %d, want %d", scan.tailInstance, wantInstance)
			}
			if wantSlot := wantService*manifest.InstancesPerService + wantInstance; scan.tailSlot != wantSlot {
				t.Fatalf("tail slot = %d, want service-major slot %d", scan.tailSlot, wantSlot)
			}
			if scan.tailTS != wantTS.UnixNano() {
				t.Fatalf("tail timestamp = %d, want %d", scan.tailTS, wantTS.UnixNano())
			}
			want := int((tailIndex%int64(shortEvery) + tailIndex/int64(shortEvery)) % int64(numStreams))
			if scan.tailService != want {
				t.Fatalf("tail service = %d, want rotating assignment %d", scan.tailService, want)
			}
			for kind := range markerKinds {
				if strings.Contains(scan.tailLine, markerLiteral(kind)) {
					t.Fatalf("tail carries marker %s: %q", markerName(kind), scan.tailLine)
				}
			}
		})
	}
}

func TestCorpusPartialBlockAndBeforeFirstRareBlock(t *testing.T) {
	tests := []struct {
		name   string
		target int64
		rare   bool
	}{
		{"before first rare block", 8_000_000, false},
		{"inside first rare block", 40_000_000, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewCorpus(testConfig(tc.target))
			if err != nil {
				t.Fatalf("NewCorpus: %v", err)
			}
			scan, manifest := scanCorpus(t, c, 4096)
			checkManifest(t, c, scan, manifest)

			var rare MarkerInfo
			for _, marker := range manifest.Markers {
				if marker.Name == "rare" {
					rare = marker
				}
			}
			if tc.rare {
				if rare.Count == 0 || len(rare.Ranges) != 1 {
					t.Fatalf("partial rare block = %+v, want one non-empty range", rare)
				}
				if rare.Ranges[0].Through.Equal(testStart().Add(time.Duration(markerBlockSize*2) * c.step)) {
					t.Fatalf("partial block range unexpectedly covers the whole block: %+v", rare.Ranges[0])
				}
			} else if rare.Count != 0 || len(rare.Ranges) != 0 {
				t.Fatalf("rare marker before its block = %+v", rare)
			}
		})
	}
}

func TestCorpusTinyTargets(t *testing.T) {
	tests := []struct {
		target  int64
		exactly int64 // 0 means "at least one line"
	}{
		{tailMinBytes, 1},     // the budget is the tail minimum: only the tail fits
		{tailMinBytes + 1, 1}, // one byte of headroom: every record is too long
		{100, 0},
		{4096, 0},
	}
	for _, tc := range tests {
		c, err := NewCorpus(testConfig(tc.target))
		if err != nil {
			t.Fatalf("target %d: NewCorpus: %v", tc.target, err)
		}
		scan, manifest := scanCorpus(t, c, 8)
		checkManifest(t, c, scan, manifest)
		if tc.exactly > 0 && manifest.Lines != tc.exactly {
			t.Fatalf("target %d lines = %d, want %d", tc.target, manifest.Lines, tc.exactly)
		}
		if manifest.Lines < 1 {
			t.Fatalf("target %d produced no lines", tc.target)
		}
		if scan.tailLine == "" {
			t.Fatalf("target %d emitted no unmarked tail record", tc.target)
		}
		for kind := range markerKinds {
			if strings.Contains(scan.tailLine, markerLiteral(kind)) {
				t.Fatalf("target %d tail carries marker %s", tc.target, markerName(kind))
			}
		}
	}
}

func TestCorpusValidation(t *testing.T) {
	valid := testConfig(4096)
	subMicro := valid
	subMicro.Start = testStart().Add(500 * time.Nanosecond)
	badStream := valid
	badStream.Streams = []string{"bad name"}
	markerStream := valid
	markerStream.Streams = []string{"api" + rareMarker}
	badLabel := valid
	badLabel.Streams = []string{"api"}
	badLabel.StreamLabel = "not a label"
	instanceLabelStream := valid
	instanceLabelStream.StreamLabel = instanceLabel
	instanceLabelExtra := valid
	instanceLabelExtra.Labels = logs.Labels{{Name: instanceLabel, Value: "0"}}
	tooManyInstances := valid
	tooManyInstances.Streams = defaultStreamNames()
	tooManyInstances.InstancesPerService = maxLabelSets
	hugeInstances := valid
	hugeInstances.InstancesPerService = math.MaxInt
	negativeInstances := valid
	negativeInstances.InstancesPerService = -1
	overflow := valid
	overflow.TargetBytes = 1 << 40
	overflow.Step = time.Hour
	year9999 := valid
	year9999.Start = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	maxAligned := time.Unix(0, math.MaxInt64/1000*1000).UTC()
	atMax := valid
	atMax.Start = maxAligned
	nearMax := valid
	nearMax.Start = maxAligned.Add(-10 * time.Millisecond)
	beforeMin := valid
	beforeMin.Start = time.Unix(0, math.MinInt64).UTC().Add(-time.Second)

	tests := []struct {
		name string
		cfg  CorpusConfig
	}{
		{"zero target", func() CorpusConfig { c := valid; c.TargetBytes = 0; return c }()},
		{"target below tail minimum", func() CorpusConfig { c := valid; c.TargetBytes = tailMinBytes - 1; return c }()},
		{"zero start", func() CorpusConfig { c := valid; c.Start = time.Time{}; return c }()},
		{"sub-microsecond start", subMicro},
		{"zero step", func() CorpusConfig { c := valid; c.Step = 0; return c }()},
		{"negative step", func() CorpusConfig { c := valid; c.Step = -time.Microsecond; return c }()},
		{"sub-microsecond step", func() CorpusConfig { c := valid; c.Step = 500 * time.Nanosecond; return c }()},
		{"timestamp overflow", overflow},
		{"start year 9999", year9999},
		{"start at max int64 nanoseconds", atMax},
		{"start plus corpus duration overflows nanoseconds", nearMax},
		{"start before min int64 nanoseconds", beforeMin},
		{"bad stream name", badStream},
		{"stream name contains marker", markerStream},
		{"bad stream label", badLabel},
		{"bad extra label", func() CorpusConfig { c := valid; c.Labels = logs.Labels{{Name: "bad name", Value: "x"}}; return c }()},
		{"negative instances", negativeInstances},
		{"stream label is instance", instanceLabelStream},
		{"extra label is instance", instanceLabelExtra},
		{"label set product above bound", tooManyInstances},
		{"instance count above bound", hugeInstances},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewCorpus(tc.cfg); err == nil {
				t.Fatalf("NewCorpus accepted invalid config %+v", tc.cfg)
			}
		})
	}
}

func TestCorpusStartNearMaxNanosecondsButFits(t *testing.T) {
	cfg := testConfig(4096)
	cfg.Start = time.Unix(0, math.MaxInt64/1000*1000).UTC().Add(-time.Second)
	if _, err := NewCorpus(cfg); err != nil {
		t.Fatalf("NewCorpus rejected a start that fits int64 nanoseconds: %v", err)
	}
}

func TestExpectedMarkerCountRejectsOutOfRangeStep(t *testing.T) {
	manifest := CorpusInfo{
		StepMicros: int64(1) << 61, // multiplying by 1000 wraps to zero
		Start:      testStart(),
		Through:    testStart().Add(time.Second),
		Lines:      100,
		Markers: []MarkerInfo{{
			Name: "rare",
			Schedule: MarkerSchedule{
				EveryRecords: rareEvery,
				Blocks:       []RecordRange{{From: 0, Through: 100}},
			},
		}},
	}
	if _, err := manifest.ExpectedMarkerCount("rare", manifest.Start, manifest.Through); err == nil {
		t.Fatal("ExpectedMarkerCount accepted a step that overflows nanoseconds")
	}
}

func TestCorpusTemplateShapes(t *testing.T) {
	c, err := NewCorpus(testConfig(4096))
	if err != nil {
		t.Fatalf("NewCorpus: %v", err)
	}
	seen := make(map[int]bool, templateCount-1)
	for i := range int64(2000) {
		line, stream, ts, tpl, hits := c.record(i)
		seen[tpl] = true
		if line == "" || strings.ContainsAny(line, "\r\n") {
			t.Fatalf("record %d has an invalid line %q", i, line)
		}
		if want := testStart().Add(time.Duration(i) * c.step); !ts.Equal(want) {
			t.Fatalf("record %d timestamp = %v, want %v", i, ts, want)
		}
		if stream != streamFor(i, len(c.cfg.Streams)) {
			t.Fatalf("record %d stream = %d", i, stream)
		}
		if tpl == templateFixed && !hits.frequent && !hits.rare && !hits.short {
			if len(line) != fixedLineBytes {
				t.Fatalf("fixed template record %d length = %d, want %d", i, len(line), fixedLineBytes)
			}
		}
	}
	if len(seen) != templateCount-1 {
		t.Fatalf("saw %d templates over 2000 records, want %d", len(seen), templateCount-1)
	}
}

func TestMarkerLiterals(t *testing.T) {
	markers := []struct {
		name    string
		literal string
		minLen  int
	}{
		{"frequent", frequentMarker, 1},
		{"rare", rareMarker, 8},
		{"absent", absentMarker, 8},
		{"short", shortNeedle, 4},
	}
	for _, marker := range markers {
		if len(marker.literal) < marker.minLen {
			t.Fatalf("marker %s is %d bytes, want at least %d", marker.name, len(marker.literal), marker.minLen)
		}
		for _, other := range markers {
			if other.name != marker.name && strings.Contains(other.literal, marker.literal) {
				t.Fatalf("marker %s %q is a substring of %s %q", marker.name, marker.literal, other.name, other.literal)
			}
		}
		texts := []string{
			"info", "warn", "error", "debug",
			"request completed", "connection timed out", "cache miss", "user login succeeded",
			"disk quota exceeded", "retrying upstream", "invalid token", "slow query",
			"GET", "POST", "PUT", "DELETE",
			"items", "users", "orders", "sessions", "health", "metrics", "cart", "profile",
			"access_json", "app_logfmt", "plain", "nginx", "fixed_256", "audit_json", "stack", "heartbeat",
		}
		for _, text := range texts {
			if strings.Contains(text, marker.literal) {
				t.Fatalf("marker %s %q appears in template text %q", marker.name, marker.literal, text)
			}
		}
		if strings.Contains(strings.Repeat("x", fixedLineBytes), marker.literal) {
			t.Fatalf("marker %s appears in fixed-template padding", marker.name)
		}
	}
	if len(shortNeedle) != 4 {
		t.Fatalf("short needle is %d bytes, want exactly 4", len(shortNeedle))
	}
}
