package main

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fakeEntry is one log entry the fake Loki API serves.
type fakeEntry struct {
	ts         int64
	labels     map[string]string
	line       string
	structured map[string]string
}

// fakeLoki serves /ready and the subset of the Loki read API the comparator
// uses. Base-corpus requests are answered from base; every other fixed query
// is answered from responses, so a test can mutate exactly one response.
type fakeLoki struct {
	mu           sync.Mutex
	baseKey      string
	base         []fakeEntry
	responses    map[string][]byte
	readyStatus  int
	failBase     int
	delayBase    time.Duration
	delayReady   time.Duration
	mutateBase   func([]fakeEntry) []fakeEntry
	requestCount int
	tenants      []string
}

func (f *fakeLoki) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requestCount++
	f.mu.Unlock()
	if r.URL.Path == "/ready" {
		if f.delayReady > 0 {
			time.Sleep(f.delayReady)
		}
		status := f.readyStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		return
	}
	if r.URL.Path != apiPrefix+"/query_range" && r.URL.Path != apiPrefix+"/series" {
		http.Error(w, "unexpected path", http.StatusNotFound)
		return
	}
	f.mu.Lock()
	f.tenants = append(f.tenants, r.Header.Get(tenant.Header))
	f.mu.Unlock()
	if f.delayBase > 0 {
		time.Sleep(f.delayBase)
	}
	if r.URL.Path == apiPrefix+"/series" {
		writeCanned(w, f.responses, "series:"+r.URL.RawQuery)
		return
	}
	query := r.URL.Query().Get("query")
	if r.URL.RawQuery == f.baseKey {
		if f.failBase != 0 {
			http.Error(w, "synthetic base failure", f.failBase)
			return
		}
		entries := f.base
		if f.mutateBase != nil {
			entries = f.mutateBase(cloneEntries(entries))
		}
		direction := r.URL.Query().Get("direction")
		if direction == "forward" {
			entries = slices.Clone(entries)
			slices.SortFunc(entries, func(a, b fakeEntry) int { return int(a.ts - b.ts) })
		} else {
			entries = slices.Clone(entries)
			slices.SortFunc(entries, func(a, b fakeEntry) int { return int(b.ts - a.ts) })
		}
		body := streamsBody(entries)
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
		return
	}
	if query == "" {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}
	writeCanned(w, f.responses, r.URL.RawQuery)
}

func (f *fakeLoki) tenantSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.tenants...)
}

func writeCanned(w http.ResponseWriter, responses map[string][]byte, key string) {
	body, ok := responses[key]
	if !ok {
		http.Error(w, "unexpected query "+key, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func cloneEntries(entries []fakeEntry) []fakeEntry {
	out := make([]fakeEntry, len(entries))
	for i, e := range entries {
		out[i] = fakeEntry{
			ts:         e.ts,
			labels:     mapsClone(e.labels),
			line:       e.line,
			structured: mapsClone(e.structured),
		}
	}
	return out
}

func mapsClone(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type streamOut struct {
	Stream map[string]string `json:"stream"`
	Values [][3]any          `json:"values"`
}

func streamsBody(entries []fakeEntry) []byte {
	byLabels := make(map[string]*streamOut)
	var order []string
	for _, e := range entries {
		key := canonicalLabels(e.labels)
		stream, ok := byLabels[key]
		if !ok {
			stream = &streamOut{Stream: e.labels}
			byLabels[key] = stream
			order = append(order, key)
		}
		stream.Values = append(stream.Values, [3]any{
			fmt.Sprintf("%d", e.ts),
			e.line,
			map[string]any{
				"structuredMetadata": e.structured,
				"parsed":             map[string]string{},
			},
		})
	}
	streams := make([]streamOut, 0, len(order))
	for _, key := range order {
		streams = append(streams, *byLabels[key])
	}
	body, err := json.Marshal(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "streams",
			"result":     streams,
		},
	})
	if err != nil {
		panic(err)
	}
	return body
}

func seriesBody(labelSets []map[string]string) []byte {
	body, err := json.Marshal(map[string]any{
		"status": "success",
		"data":   labelSets,
	})
	if err != nil {
		panic(err)
	}
	return body
}

// fakeDataset builds one entry per planned record.
func fakeDataset(plan fixturePlan) []fakeEntry {
	entries := make([]fakeEntry, 0, plan.total)
	for _, c := range plan.cases {
		for i, rec := range c.records {
			entries = append(entries, fakeEntry{
				ts: rec.timestamp.UnixNano(),
				labels: map[string]string{
					"service_namespace":   plan.namespace,
					"service_instance_id": c.instance,
					"service_name":        "fixture-" + c.name,
				},
				line:       fmt.Sprintf("%s record %d", c.name, i),
				structured: map[string]string{"fixture": c.name},
			})
		}
	}
	slices.SortFunc(entries, func(a, b fakeEntry) int { return int(a.ts - b.ts) })
	return entries
}

// queryKey identifies a fixed query by its exact encoded parameters, so
// queries that share a selector but differ in window, direction or limit get
// distinct canned responses.
func queryKey(t *testing.T, q queryCase) string {
	t.Helper()
	req, err := q.request(t.Context(), "http://fake", tenant.Default)
	if err != nil {
		t.Fatalf("build request for %q: %v", q.name, err)
	}
	return req.URL.RawQuery
}

// collisionOutcome is the consistent winner a fake server uses for one
// contender filter in the equal-run test. It is deliberately arbitrary:
// equality only requires both sides to make the same choice.
func collisionOutcome(name string) (caseName string, entries int, ok bool) {
	switch name {
	case "collision-meta-resource", "collision-meta-scope", "collision-meta-log",
		"collision-http-get", "collision-http-scope", "collision-http-log":
		caseName = caseCollisionMeta
	case "collision-label-stream", "collision-label-metadata":
		caseName = caseCollisionLabel
	case "unusual-name-dash", "unusual-name-space", "unusual-name-dot":
		caseName = caseUnusualNames
	case "fixed-severity-attr", "fixed-severity-field", "fixed-trace-attr", "fixed-trace-field",
		"fixed-span-attr", "fixed-span-field", "fixed-observed-attr", "fixed-observed-field",
		"fixed-flags-attr", "fixed-flags-field", "fixed-dropped-attr", "fixed-dropped-field",
		"fixed-scope-name-attr", "fixed-scope-name-field", "fixed-scope-version-attr", "fixed-scope-version-field":
		caseName = caseFixedField
	default:
		return "", 0, false
	}
	switch {
	case name == "collision-meta-scope", name == "collision-http-scope",
		name == "collision-label-metadata", name == "unusual-name-dot":
		return caseName, 1, true
	case strings.HasPrefix(name, "fixed-") && strings.HasSuffix(name, "-field"):
		return caseName, 1, true
	default:
		return caseName, 0, true
	}
}

// synthesizeResponses answers every fixed query from the dataset so an equal
// pair of servers can run the whole comparator unchanged.
func synthesizeResponses(t *testing.T, queries []queryCase, dataset []fakeEntry) map[string][]byte {
	t.Helper()
	byTS := make(map[int64]fakeEntry, len(dataset))
	byInstance := make(map[string][]fakeEntry)
	for _, e := range dataset {
		byTS[e.ts] = e
		byInstance[e.labels["service_instance_id"]] = append(byInstance[e.labels["service_instance_id"]], e)
	}
	responses := make(map[string][]byte, len(queries))
	for _, q := range queries {
		key := queryKey(t, q)
		if q.kind == querySeries {
			seen := make(map[string]struct{})
			var sets []map[string]string
			for _, e := range dataset {
				labelKey := canonicalLabels(e.labels)
				if _, ok := seen[labelKey]; ok {
					continue
				}
				seen[labelKey] = struct{}{}
				sets = append(sets, e.labels)
			}
			slices.SortFunc(sets, func(a, b map[string]string) int {
				return strings.Compare(canonicalLabels(a), canonicalLabels(b))
			})
			responses["series:"+key] = seriesBody(sets)
			continue
		}
		var entries []fakeEntry
		switch {
		case q.expectTimestamps != nil:
			for _, ts := range q.expectTimestamps {
				entry, ok := byTS[ts]
				if !ok {
					t.Fatalf("query %q expects timestamp %d which is not in the dataset", q.name, ts)
				}
				entries = append(entries, entry)
			}
		case q.expectEntries != nil:
			n := *q.expectEntries
			if n > len(dataset) {
				t.Fatalf("query %q expects %d entries but dataset has %d", q.name, n, len(dataset))
			}
			entries = slices.Clone(dataset[:n])
		default:
			caseName, n, ok := collisionOutcome(q.name)
			if !ok {
				t.Fatalf("query %q has no count, timestamp or collision synthesis rule", q.name)
			}
			entries = slices.Clone(byInstance[caseName])
			if n > len(entries) {
				t.Fatalf("query %q asks for %d entries but case %q has %d", q.name, n, caseName, len(entries))
			}
			entries = entries[:n]
		}
		if q.direction == "forward" {
			slices.SortFunc(entries, func(a, b fakeEntry) int { return int(a.ts - b.ts) })
		} else {
			slices.SortFunc(entries, func(a, b fakeEntry) int { return int(b.ts - a.ts) })
		}
		responses[key] = streamsBody(entries)
	}
	return responses
}

func newTestFixture(t *testing.T) (string, fixtureManifest, manifestTimes, fixturePlan) {
	t.Helper()
	m, plan := testManifest(t)
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := writeManifest(path, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	times, err := m.parse()
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	return path, m, times, plan
}

func compareArgs(loki, parkana, manifest, reportPath string, extra ...string) []string {
	args := []string{
		"compare",
		"-loki", loki,
		"-parkana", parkana,
		"-manifest", manifest,
		"-report", reportPath,
		"-wait-timeout", "200ms",
		"-poll-interval", "5ms",
		"-ready-timeout", "1s",
		"-timeout", "1s",
	}
	return append(args, extra...)
}

func runCompareTest(t *testing.T, loki, parkana *fakeLoki, manifestPath string, extra ...string) (int, string, string, compareReport) {
	t.Helper()
	lokiServer := httptest.NewServer(loki)
	defer lokiServer.Close()
	parkanaServer := httptest.NewServer(parkana)
	defer parkanaServer.Close()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), compareArgs(lokiServer.URL, parkanaServer.URL, manifestPath, reportPath, extra...), &stdout, &stderr)
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("report not written: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var report compareReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("report decode: %v", err)
	}
	if report.Summary.ExitCode != code {
		t.Fatalf("report exit = %d, process exit = %d", report.Summary.ExitCode, code)
	}
	return code, stdout.String(), stderr.String(), report
}

func equalPair(t *testing.T, plan fixturePlan, queries []queryCase) (*fakeLoki, *fakeLoki) {
	t.Helper()
	dataset := fakeDataset(plan)
	responses := synthesizeResponses(t, queries, dataset)
	baseKey := ""
	for _, q := range queries {
		if q.name == "entries-backward" {
			baseKey = queryKey(t, q)
		}
	}
	if baseKey == "" {
		t.Fatal("entries-backward query not found")
	}
	makeSide := func() *fakeLoki {
		return &fakeLoki{
			baseKey:   baseKey,
			base:      dataset,
			responses: cloneResponses(responses),
		}
	}
	return makeSide(), makeSide()
}

func cloneResponses(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for key, body := range in {
		out[key] = bytes.Clone(body)
	}
	return out
}

func TestRunCompareEqual(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	code, stdout, stderr, report := runCompareTest(t, loki, parkana, manifestPath)
	if code != exitEqual {
		t.Fatalf("exit = %d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	if !report.Precheck.OK {
		t.Fatalf("precheck not ok: %+v", report.Precheck)
	}
	if report.Summary.Equal != len(queries) || report.Summary.Different != 0 || report.Summary.Failed != 0 {
		t.Fatalf("summary = %+v", report.Summary)
	}
	if !strings.Contains(stdout, "summary: "+fmt.Sprint(len(queries))+" equal") {
		t.Fatalf("stdout = %s", stdout)
	}
	if report.Fixture.ID != fixtureID || report.Fixture.Version != fixtureVersion ||
		report.Fixture.PDataVersion != pdataVersion || report.Fixture.Namespace != "ns-1" {
		t.Fatalf("fixture identity = %+v", report.Fixture)
	}
	if report.Targets.Tenant != string(tenant.Default) {
		t.Fatalf("target tenant = %q", report.Targets.Tenant)
	}
	if len(loki.tenantSnapshot()) == 0 || len(parkana.tenantSnapshot()) == 0 {
		t.Fatal("no queries reached the fake servers")
	}
	for _, got := range append(loki.tenantSnapshot(), parkana.tenantSnapshot()...) {
		if got != string(tenant.Default) {
			t.Fatalf("query tenant header = %q", got)
		}
	}
}

func mustManifest(t *testing.T, path string) fixtureManifest {
	t.Helper()
	m, _, err := readManifest(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	return m
}

func TestRunCompareDetectsMismatches(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeEntry)
		want   string
	}{
		{
			name:   "line",
			mutate: func(e *fakeEntry) { e.line += " mutated" },
			want:   "line=",
		},
		{
			name:   "metadata",
			mutate: func(e *fakeEntry) { e.structured = map[string]string{"fixture": "mutated"} },
			want:   "structured=",
		},
		{
			name:   "label",
			mutate: func(e *fakeEntry) { e.labels = mapsClone(e.labels); e.labels["service_name"] = "mutated" },
			want:   "stream only on",
		},
		{
			name:   "timestamp",
			mutate: func(e *fakeEntry) { e.ts += int64(time.Second) },
			want:   "ts=",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifestPath, _, times, plan := newTestFixture(t)
			queries, err := fixedQueries(mustManifest(t, manifestPath), times)
			if err != nil {
				t.Fatalf("fixed queries: %v", err)
			}
			loki, parkana := equalPair(t, plan, queries)
			parkana.mutateBase = func(entries []fakeEntry) []fakeEntry {
				if len(entries) > 0 {
					tc.mutate(&entries[0])
				}
				return entries
			}
			code, stdout, stderr, report := runCompareTest(t, loki, parkana, manifestPath)
			if code != exitDifferent {
				t.Fatalf("exit = %d, want %d\nstdout=%s\nstderr=%s", code, exitDifferent, stdout, stderr)
			}
			if report.Summary.Different == 0 {
				t.Fatalf("summary = %+v", report.Summary)
			}
			if !strings.Contains(stdout, tc.want) {
				t.Fatalf("stdout does not contain %q:\n%s", tc.want, stdout)
			}
		})
	}
}

func TestRunCompareDetectsCollisionFilterDivergence(t *testing.T) {
	manifestPath, m, times, plan := newTestFixture(t)
	queries, err := fixedQueries(m, times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	dataset := fakeDataset(plan)
	var collisionEntry []fakeEntry
	for _, e := range dataset {
		if e.labels["service_instance_id"] == caseCollisionMeta {
			collisionEntry = append(collisionEntry, e)
		}
	}
	if len(collisionEntry) != 1 {
		t.Fatalf("collision-meta dataset entries = %d, want 1", len(collisionEntry))
	}
	byName := make(map[string]queryCase, len(queries))
	for _, q := range queries {
		byName[q.name] = q
	}
	scopeKey := queryKey(t, byName["collision-meta-scope"])
	logKey := queryKey(t, byName["collision-meta-log"])
	// Full entries, labels and metadata are identical on both sides (the base
	// response is shared), but Loki's filters observe the scope contender while
	// Parkana's observe the log contender. Only the per-contender filters can
	// catch this, and they must fail the run.
	loki.responses[scopeKey] = streamsBody(collisionEntry)
	loki.responses[logKey] = streamsBody(nil)
	parkana.responses[scopeKey] = streamsBody(nil)
	parkana.responses[logKey] = streamsBody(collisionEntry)

	code, stdout, stderr, report := runCompareTest(t, loki, parkana, manifestPath)
	if code != exitDifferent {
		t.Fatalf("exit = %d, want %d\nstdout=%s\nstderr=%s", code, exitDifferent, stdout, stderr)
	}
	if !report.Precheck.OK {
		t.Fatalf("precheck failed: %+v", report.Precheck)
	}
	seen := make(map[string]queryOutcome, len(report.Queries))
	for _, q := range report.Queries {
		seen[q.Name] = q
	}
	// The full-entry responses are equal: the mismatch is only in filter
	// semantics, which is exactly what the exact-value contender queries test.
	if diffs := seen["entries-backward"].Diffs; len(diffs) != 0 {
		t.Fatalf("full-entry comparison differed: %v", diffs)
	}
	scope, log := seen["collision-meta-scope"], seen["collision-meta-log"]
	if len(scope.Diffs) == 0 || len(log.Diffs) == 0 {
		t.Fatalf("collision filter divergence not reported: scope=%+v log=%+v", scope, log)
	}
	for _, name := range []string{"collision-meta-scope", "collision-meta-log"} {
		if !strings.Contains(stdout, name) {
			t.Fatalf("stdout does not name %q:\n%s", name, stdout)
		}
	}
}

func TestDiffResultsRejectsDuplicateStreamLabelsets(t *testing.T) {
	entry := func(ts int64, line string) canonicalEntry {
		return canonicalEntry{ts: ts, line: line, structured: map[string]string{}, parsed: map[string]string{}}
	}
	streamA := canonicalStream{labels: map[string]string{"s": "a"}, entries: []canonicalEntry{entry(1, "a")}}
	streamB := canonicalStream{labels: map[string]string{"s": "b"}, entries: []canonicalEntry{entry(2, "b")}}
	// The per-label map would collapse these to the same {A, B}; only a
	// duplicate-label-set check or a multiset comparison can tell them apart.
	left := canonicalResult{kind: "streams", entries: 3, streams: []canonicalStream{streamA, streamA, streamB}}
	right := canonicalResult{kind: "streams", entries: 3, streams: []canonicalStream{streamA, streamB, streamB}}
	if diffs := diffResults(left, right); len(diffs) == 0 {
		t.Fatal("[A,A,B] compared equal to [A,B,B]")
	}
}

func TestDiffSeriesComparesMultiplicity(t *testing.T) {
	a := map[string]string{"s": "a"}
	b := map[string]string{"s": "b"}
	left := canonicalResult{kind: "series", series: []map[string]string{a, a, b}}
	right := canonicalResult{kind: "series", series: []map[string]string{a, b, b}}
	if diffs := diffResults(left, right); len(diffs) == 0 {
		t.Fatal("[A,A,B] compared equal to [A,B,B]")
	}
}

func TestDecodeRejectsNumericMetadataValue(t *testing.T) {
	// Loki's encoder writes structured metadata as a JSON object of strings
	// (pkg/util/marshal encodes map[string]string), so a number is a
	// conformance violation, not an equivalent label value.
	body := []byte(`{"status":"success","data":{"resultType":"streams","result":[` +
		`{"stream":{"a":"b"},"values":[["1","x",{"structuredMetadata":{"k":1},"parsed":{}}]]}]}}`)
	if _, err := decodeRangeResponse(body); err == nil {
		t.Fatal("numeric metadata value accepted")
	}
}

func TestRunCompareWaitTimeoutBoundsBlockedRequest(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	parkana.delayBase = 2 * time.Second
	lokiServer := httptest.NewServer(loki)
	defer lokiServer.Close()
	parkanaServer := httptest.NewServer(parkana)
	defer parkanaServer.Close()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := run(t.Context(), compareArgs(lokiServer.URL, parkanaServer.URL, manifestPath, reportPath,
		"-timeout", "10s", "-wait-timeout", "40ms", "-poll-interval", "5ms"), &stdout, &stderr)
	elapsed := time.Since(start)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
	if elapsed > time.Second {
		t.Fatalf("wait-timeout 40ms took %s; per-request timeout is not capped by the remaining budget", elapsed)
	}
}

func TestRunCompareReadinessBudgetBoundsBlockedRequest(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	parkana.delayReady = 2 * time.Second
	lokiServer := httptest.NewServer(loki)
	defer lokiServer.Close()
	parkanaServer := httptest.NewServer(parkana)
	defer parkanaServer.Close()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := run(t.Context(), compareArgs(lokiServer.URL, parkanaServer.URL, manifestPath, reportPath,
		"-poll-interval", "1s", "-ready-timeout", "30ms"), &stdout, &stderr)
	elapsed := time.Since(start)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
	if elapsed > time.Second {
		t.Fatalf("ready-timeout 30ms took %s; per-request timeout is not capped by the remaining budget", elapsed)
	}
}

func TestRunCompareJSONFormat(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	lokiServer := httptest.NewServer(loki)
	defer lokiServer.Close()
	parkanaServer := httptest.NewServer(parkana)
	defer parkanaServer.Close()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), compareArgs(lokiServer.URL, parkanaServer.URL, manifestPath, reportPath, "-format", "json"), &stdout, &stderr)
	if code != exitEqual {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	var report compareReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if report.Summary.ExitCode != exitEqual || !report.Precheck.OK {
		t.Fatalf("report = %+v", report.Summary)
	}
}

func TestRunCompareRejectsTamperedManifest(t *testing.T) {
	manifestPath, m, _, _ := newTestFixture(t)
	m.FixtureVersion = fixtureVersion + 1
	if err := writeManifest(manifestPath, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), compareArgs("http://127.0.0.1:1", "http://127.0.0.1:1", manifestPath,
		filepath.Join(t.TempDir(), "report.json")), &stdout, &stderr)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "fixture") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestCompareRejectsInvalidFlags(t *testing.T) {
	tests := [][]string{
		{"compare"},
		{"compare", "-loki", "http://x", "-parkana", "http://y"},
		{"compare", "-loki", "ftp://x", "-parkana", "http://y", "-manifest", "m.json"},
		{"compare", "-loki", "http://x", "-parkana", "http://y", "-manifest", "m.json", "-format", "xml"},
		{"compare", "-loki", "http://x", "-parkana", "http://y", "-manifest", "m.json", "-timeout", "0s"},
		{"compare", "-loki", "http://x", "-parkana", "http://y", "-manifest", "m.json", "-tenant", "bad tenant"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), args, &stdout, &stderr); code != exitFailure {
			t.Fatalf("%v: exit = %d, want %d", args, code, exitFailure)
		}
	}
}

func TestRunCompareMissingDataIsNotEquality(t *testing.T) {
	tests := []struct {
		name         string
		emptyLoki    bool
		emptyParkana bool
	}{
		{"both empty", true, true},
		{"loki empty", true, false},
		{"parkana empty", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manifestPath, _, times, plan := newTestFixture(t)
			queries, err := fixedQueries(mustManifest(t, manifestPath), times)
			if err != nil {
				t.Fatalf("fixed queries: %v", err)
			}
			loki, parkana := equalPair(t, plan, queries)
			if tc.emptyLoki {
				loki.base = nil
			}
			if tc.emptyParkana {
				parkana.base = nil
			}
			code, stdout, stderr, report := runCompareTest(t, loki, parkana, manifestPath)
			if code != exitFailure {
				t.Fatalf("exit = %d, want %d\nstdout=%s\nstderr=%s", code, exitFailure, stdout, stderr)
			}
			if report.Precheck.OK {
				t.Fatalf("precheck passed with missing data: %+v", report.Precheck)
			}
			if !strings.Contains(stdout, "precheck FAILED") {
				t.Fatalf("stdout = %s", stdout)
			}
			if strings.Contains(stdout, "summary: 0 equal, 0 different, 0 failed") {
				t.Fatalf("missing data reported as no-op equality: %s", stdout)
			}
		})
	}
}

func TestRunCompareCountMismatchIsPrecheckFailure(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	parkana.base = parkana.base[:len(parkana.base)-1]
	code, _, _, _ := runCompareTest(t, loki, parkana, manifestPath)
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
}

func TestRunCompareTransportFailures(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		manifestPath, _, times, plan := newTestFixture(t)
		queries, err := fixedQueries(mustManifest(t, manifestPath), times)
		if err != nil {
			t.Fatalf("fixed queries: %v", err)
		}
		loki, parkana := equalPair(t, plan, queries)
		parkana.delayBase = 100 * time.Millisecond
		code, _, stderr, _ := runCompareTest(t, loki, parkana, manifestPath,
			"-timeout", "10ms", "-wait-timeout", "40ms")
		if code != exitFailure {
			t.Fatalf("exit = %d, want %d", code, exitFailure)
		}
		if !strings.Contains(stderr, "precheck") {
			t.Fatalf("stderr = %q", stderr)
		}
	})
	t.Run("non-2xx", func(t *testing.T) {
		manifestPath, _, times, plan := newTestFixture(t)
		queries, err := fixedQueries(mustManifest(t, manifestPath), times)
		if err != nil {
			t.Fatalf("fixed queries: %v", err)
		}
		loki, parkana := equalPair(t, plan, queries)
		parkana.failBase = http.StatusInternalServerError
		code, _, stderr, _ := runCompareTest(t, loki, parkana, manifestPath)
		if code != exitFailure {
			t.Fatalf("exit = %d, want %d", code, exitFailure)
		}
		if !strings.Contains(stderr, "HTTP 500") {
			t.Fatalf("stderr = %q", stderr)
		}
	})
	t.Run("readiness", func(t *testing.T) {
		manifestPath, _, times, plan := newTestFixture(t)
		queries, err := fixedQueries(mustManifest(t, manifestPath), times)
		if err != nil {
			t.Fatalf("fixed queries: %v", err)
		}
		loki, parkana := equalPair(t, plan, queries)
		parkana.readyStatus = http.StatusServiceUnavailable
		code, _, stderr, _ := runCompareTest(t, loki, parkana, manifestPath,
			"-ready-timeout", "20ms", "-poll-interval", "5ms")
		if code != exitFailure {
			t.Fatalf("exit = %d, want %d", code, exitFailure)
		}
		if !strings.Contains(stderr, "readiness") {
			t.Fatalf("stderr = %q", stderr)
		}
	})
}

func TestRunCompareUnreachableServer(t *testing.T) {
	manifestPath, _, times, plan := newTestFixture(t)
	queries, err := fixedQueries(mustManifest(t, manifestPath), times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	loki, parkana := equalPair(t, plan, queries)
	code, _, _, _ := runCompareTest(t, loki, parkana, manifestPath,
		"-loki", "http://127.0.0.1:1", "-ready-timeout", "0s", "-wait-timeout", "20ms")
	if code != exitFailure {
		t.Fatalf("exit = %d, want %d", code, exitFailure)
	}
}

func TestDecodeRangeResponseIsStrict(t *testing.T) {
	valid := streamsBody([]fakeEntry{{ts: 1, labels: map[string]string{"a": "b"}, line: "x", structured: map[string]string{"k": "v"}}})
	if _, err := decodeRangeResponse(valid); err != nil {
		t.Fatalf("decode valid response: %v", err)
	}

	missingMetadata := []byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"a":"b"},"values":[["1","x"]]}]}}`)
	if _, err := decodeRangeResponse(missingMetadata); err == nil {
		t.Fatal("uncategorized entry accepted")
	}
	unexpectedMember := []byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"a":"b"},"values":[["1","x",{"structuredMetadata":{},"parsed":{},"extra":"dropped"}]]}]}}`)
	if _, err := decodeRangeResponse(unexpectedMember); err == nil {
		t.Fatal("unexpected metadata member accepted")
	}
	notStreams := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	if _, err := decodeRangeResponse(notStreams); err == nil {
		t.Fatal("non-streams result accepted")
	}
	errorStatus := []byte(`{"status":"error","data":null}`)
	if _, err := decodeRangeResponse(errorStatus); err == nil {
		t.Fatal("error envelope accepted")
	}
}

func TestDiffResultsIsLiteral(t *testing.T) {
	entry := func() canonicalEntry {
		return canonicalEntry{
			ts:         1000,
			line:       "hello",
			structured: map[string]string{"a": "1", "b": "2"},
			parsed:     map[string]string{"p": "q"},
		}
	}
	left := canonicalResult{kind: "streams", entries: 1, streams: []canonicalStream{{
		labels: map[string]string{"stream": "x"}, entries: []canonicalEntry{entry()},
	}}}
	if diffs := diffResults(left, left); len(diffs) != 0 {
		t.Fatalf("identical results differ: %v", diffs)
	}

	tests := []struct {
		name   string
		mutate func(*canonicalEntry)
		want   string
	}{
		{"timestamp", func(e *canonicalEntry) { e.ts++ }, "ts="},
		{"line", func(e *canonicalEntry) { e.line += "!" }, "line="},
		{"structured", func(e *canonicalEntry) { e.structured = map[string]string{"a": "1"} }, "structured="},
		{"parsed", func(e *canonicalEntry) { e.parsed = map[string]string{"p": "r"} }, "parsed="},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			right := canonicalResult{kind: "streams", entries: 1, streams: []canonicalStream{{
				labels: map[string]string{"stream": "x"}, entries: []canonicalEntry{entry()},
			}}}
			tc.mutate(&right.streams[0].entries[0])
			diffs := diffResults(left, right)
			if len(diffs) == 0 || !strings.Contains(strings.Join(diffs, "\n"), tc.want) {
				t.Fatalf("diffs = %v", diffs)
			}
		})
	}
}

func TestDirectionViolation(t *testing.T) {
	stream := canonicalStream{labels: map[string]string{"a": "b"}, entries: []canonicalEntry{{ts: 1}, {ts: 2}}}
	if msg := directionViolation(stream, "backward"); msg == "" {
		t.Fatal("backward accepted ascending entries")
	}
	if msg := directionViolation(stream, "forward"); msg != "" {
		t.Fatalf("forward rejected ascending entries: %s", msg)
	}
	slices.Reverse(stream.entries)
	if msg := directionViolation(stream, "backward"); msg != "" {
		t.Fatalf("backward rejected descending entries: %s", msg)
	}
}

func TestCheckQueryExpectationRejectsBothEmpty(t *testing.T) {
	want := 3
	q := queryCase{name: "x", expectEntries: &want, requireMatch: true}
	empty := canonicalResult{kind: "streams"}
	diffs := checkQueryExpectation(q, empty, empty)
	if len(diffs) == 0 {
		t.Fatal("both-empty query passed its fixture count assertion")
	}
	full := canonicalResult{kind: "streams", entries: want}
	if diffs := checkQueryExpectation(q, full, full); len(diffs) != 0 {
		t.Fatalf("matching counts reported diffs: %v", diffs)
	}
}
