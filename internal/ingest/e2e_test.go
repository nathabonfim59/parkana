package ingest_test

// Black-box end-to-end crash-recovery tests (plan A10, e2e half).
//
// The tests start a real parkana executable as a child process, push
// deterministic log lines over HTTP, SIGKILL the child, restart it, and check
// that the exact pushed multiset of rows becomes catalog-visible (the query
// semantics) exactly once, with timestamps, labels, structured metadata and
// promoted columns intact across several UTC hours.
//
// Exact-once is judged over the live catalog rows, not over every physical
// object: the approved fresh-key retry rule may leave an old, harmless object
// behind after a key rotation, and a Put that precedes Catalog.AddFiles is not
// orphaned data yet. Two guards keep that leniency honest: every physical
// Parquet object the catalog does not reference must hold only rows from the
// scenario's expected set with full row content, and the durable orphan count
// must not exceed the tenant's cumulative key rotations (A9's keyRotations
// manifest field, absent legacy manifests reading as zero). Multiple sources
// are read through one bracketed snapshot — manifest, catalog, objects,
// manifest — and a phase only completes once that bracket is stable, no upload
// receipt is open and the orphan budget was actually evaluated, so a Put in
// flight can never be mistaken for an unexplained orphan.
//
// Gate: the suite runs only when PARKANA_TEST_INGEST_BINARY names a compiled
// parkana binary built from a tree that contains the ingester service (A9)
// and a Parquet storage implementation (Part B). Without the variable the
// tests skip, because this harness is written ahead of the services it
// exercises; recovery is NOT verified until the real binary runs on Boat at
// milestone 5. If the variable is set but the file is missing or not
// executable, the tests fail instead of skipping, to catch a typo'd setup.
//
// The child is only ever talked to through its documented surface: config
// flags, the push endpoint, /ready, and the object-store directory it
// writes. Nothing internal is imported: push encoding goes through the
// shared pushcodec package, rows are read back through the approved
// storage.Reader API, and key layout through objstore.LogPrefix.
//
// Interrupted-upload crash boundaries (between Store.Put, Catalog.AddFiles
// and receipt publication) are deliberately NOT covered here: they have no
// deterministic black-box trigger without production changes and stay
// reserved for A8's unit tests with injected failure wrappers.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// envIngestBinary gates the suite on a real parkana executable.
const envIngestBinary = "PARKANA_TEST_INGEST_BINARY"

// Deadlines for one scenario phase. Generous enough for Boat VMs; the poll
// loops return early as soon as the condition holds.
const (
	readyTimeout      = 20 * time.Second
	uploadTimeout     = 90 * time.Second
	idleRestartWindow = 3 * time.Second
	flushTickTimeout  = 15 * time.Second
	// pollInterval paces the convergence loop; idlePollInterval paces the
	// stable-state window. Both are poll cadences, never settle sleeps: a
	// phase only completes through the authoritative gate.
	pollInterval     = 100 * time.Millisecond
	idlePollInterval = 200 * time.Millisecond
)

func TestE2EIngestCrashRecovery(t *testing.T) {
	bin := os.Getenv(envIngestBinary)
	if bin == "" {
		t.Skipf("%s is not set: the e2e harness needs a compiled parkana binary with the ingester role and Parquet storage; recovery stays unverified until it runs on Boat at milestone 5", envIngestBinary)
	}
	fi, err := os.Stat(bin)
	if err != nil {
		t.Fatalf("%s=%s: %v", envIngestBinary, bin, err)
	}
	if fi.IsDir() || fi.Mode()&0o111 == 0 {
		t.Fatalf("%s=%s is not an executable file", envIngestBinary, bin)
	}
	t.Run("wal-only-crash", func(t *testing.T) { runCrashScenario(t, bin, walOnlyScenario()) })
	t.Run("crash-after-local-parquet", func(t *testing.T) { runCrashScenario(t, bin, localParquetScenario()) })
}

// TestE2EScenarioPlans checks the scenario planning without a gated binary,
// so a broken burst definition fails every default run instead of hiding
// until milestone 5: the acknowledged pushes must cover exactly the
// intended ID sets (200 entries in the WAL-only case, 80 in the
// mixed-state case), keep every wire format in the acknowledged set, and
// produce the expected row multiset.
func TestE2EScenarioPlans(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sc      scenario
		pushes  int // acknowledged pushes
		entries int // expected rows
		idsHi   int // exclusive upper bound of the acknowledged ID union
	}{
		{"wal-only", walOnlyScenario(), 4, 200, 200},
		{"mixed-state", localParquetScenario(), 4, 80, 80},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plans := sentPlans(tc.sc)
			if len(plans) != tc.pushes {
				t.Fatalf("scenario sends %d pushes, want %d", len(plans), tc.pushes)
			}
			formats := make(map[pushFormat]bool, len(plans))
			ids := make(map[int]bool, tc.entries)
			for _, p := range plans {
				formats[p.format] = true
				for id := p.lo; id < p.hi; id++ {
					if ids[id] {
						t.Fatalf("scenario sends ID %d twice", id)
					}
					ids[id] = true
				}
			}
			want := make(map[int]bool, tc.idsHi)
			for id := range tc.idsHi {
				want[id] = true
			}
			if !maps.Equal(ids, want) {
				t.Fatalf("acknowledged IDs are not exactly 0..%d (got %d distinct)", tc.idsHi-1, len(ids))
			}
			for _, f := range []pushFormat{{}, {jsonBody: true}, {gzipBody: true}, {jsonBody: true, gzipBody: true}} {
				if !formats[f] {
					t.Fatalf("format %+v is not among the acknowledged pushes", f)
				}
			}
			if expected := expectedForBurst(plans); len(expected) != tc.entries {
				t.Fatalf("expected multiset holds %d rows, want %d", len(expected), tc.entries)
			}
		})
	}
}

// manifestFixture and manifestFileFixture wrap the manifest types with
// unknown members the committed-manifest decoder must ignore.
type manifestFixture struct {
	Tenant   string `json:"tenant"`
	Frontier struct {
		Last int64 `json:"last"`
	} `json:"frontier"`
	Files []manifestFileFixture `json:"files"`
	Extra string                `json:"parkana-test-extra,omitempty"`
}

type manifestFileFixture struct {
	localFile
	Extra int `json:"parkana-test-extra,omitempty"`
}

// TestE2ELocalManifestCheck exercises the committed-manifest validation of
// the mixed-state scenario without a gated binary: a valid committed
// fixture passes, while a wrong tenant (manifest or file level), a zero
// frontier, no live files, a missing canonical file, and unknown JSON
// fields behave per contract. Fixtures stay narrow and are generated in
// t.TempDir; only file existence matters, never content.
func TestE2ELocalManifestCheck(t *testing.T) {
	const tid = tenant.Default
	hour := time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name   string
		mutate func(m *manifestFixture)
		create func(t *testing.T, dir string, f localFile) // fixture file setup; nil creates nothing
		wantOK bool
	}{
		{"valid committed fixture", func(*manifestFixture) {}, writePlaceholderFile, true},
		{"wrong manifest tenant", func(m *manifestFixture) { m.Tenant = "other" }, writePlaceholderFile, false},
		{"wrong file tenant", func(m *manifestFixture) { m.Files[0].Tenant = "other" }, writePlaceholderFile, false},
		{"zero frontier", func(m *manifestFixture) { m.Frontier.Last = 0 }, writePlaceholderFile, false},
		{"no live files", func(m *manifestFixture) { m.Files[0].Retired = true }, writePlaceholderFile, false},
		{"hour with minutes", func(m *manifestFixture) {
			m.Files[0].Hour = time.Date(2026, 3, 14, 9, 30, 0, 0, time.UTC)
		}, writePlaceholderFile, false},
		{"directory at canonical path", func(*manifestFixture) {}, func(t *testing.T, dir string, f localFile) {
			if err := os.MkdirAll(path.Join(dir, "parquet", f.Tenant, f.relPath()), 0o755); err != nil {
				t.Fatalf("e2e: create fixture directory: %v", err)
			}
		}, false},
		{"missing canonical file", func(*manifestFixture) {}, nil, false},
		{"ignored unknown fields", func(m *manifestFixture) {
			m.Extra = "ignored"
			m.Files[0].Extra = 1
		}, writePlaceholderFile, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fx := manifestFixture{
				Tenant: string(tid),
				Files:  []manifestFileFixture{{Tenant: string(tid), ID: uuid.NewV7(), Stream: "api", Hour: hour}},
			}
			fx.Frontier.Last = 7
			tc.mutate(&fx)

			manifestDir := path.Join(dir, "parquet", string(tid))
			if err := os.MkdirAll(manifestDir, 0o755); err != nil {
				t.Fatalf("e2e: create fixture dir: %v", err)
			}
			b, err := json.Marshal(fx)
			if err != nil {
				t.Fatalf("e2e: encode fixture: %v", err)
			}
			if err := os.WriteFile(path.Join(manifestDir, "manifest.json"), b, 0o644); err != nil {
				t.Fatalf("e2e: write fixture manifest: %v", err)
			}
			if tc.create != nil {
				tc.create(t, dir, fx.Files[0].localFile)
			}

			err = checkLocalManifest(dir, tid)
			if tc.wantOK && err != nil {
				t.Fatalf("e2e: valid fixture rejected: %v", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("e2e: invalid fixture accepted")
			}
		})
	}
}

// writePlaceholderFile creates an empty placeholder Parquet file at the
// canonical path of f under dir's tenant root.
func writePlaceholderFile(t *testing.T, dir string, f localFile) {
	t.Helper()
	p := path.Join(dir, "parquet", f.Tenant, f.relPath())
	if err := os.MkdirAll(path.Dir(p), 0o755); err != nil {
		t.Fatalf("e2e: create fixture file dir: %v", err)
	}
	if err := os.WriteFile(p, []byte("placeholder"), 0o644); err != nil {
		t.Fatalf("e2e: write fixture file: %v", err)
	}
}

// scenario describes one crash-recovery run: an ingest phase that produces a
// defined durable state, a SIGKILL, and a restart phase that must recover
// the exact acknowledged multiset of rows into the object store.
type scenario struct {
	name string
	// ingestPhase and restartPhase are the -ingest.* flags of the child that
	// gets killed and of the children that recover / idle, respectively.
	ingestPhase  childPhase
	restartPhase childPhase
	// pace is the sleep between pushes of the ingest phase; 0 sends the
	// burst as fast as the server accepts it.
	pace time.Duration
	// dropTail is the number of trailing burst pushes that are never sent:
	// the child is SIGKILLed once the earlier pushes are acknowledged, and
	// their entries alone define the expected multiset. 0 sends the whole
	// burst.
	dropTail int
	// waitForManifest waits until the committed local generation manifest
	// shows durable local Parquet before the kill; otherwise the scenario
	// asserts there is none.
	waitForManifest bool
}

// walOnlyScenario kills the child while every row can only be in the WAL:
// the flush interval cannot have fired and uploads are disabled.
func walOnlyScenario() scenario {
	return scenario{
		name:         "wal-only-crash",
		ingestPhase:  childPhase{flush: time.Hour, upload: 24 * time.Hour, uploadMaxAge: 24 * time.Hour, uploadMin: "2GiB"},
		restartPhase: childPhase{flush: time.Second, upload: 100 * time.Millisecond, uploadMaxAge: time.Second, uploadMin: "1KiB"},
	}
}

// localParquetScenario kills the child before anything was uploaded, in a
// mixed state: three paced pushes, then a wait for the committed manifest
// to show durable local Parquet (the flush tick must have sealed committed
// generations), then the last acknowledged push, then the SIGKILL. The
// final push's rows are WAL-only or already flushed — the exact-ID
// verification is state-agnostic — while the earlier ones sit in committed
// local Parquet.
func localParquetScenario() scenario {
	return scenario{
		name:            "crash-after-local-parquet",
		ingestPhase:     childPhase{flush: 500 * time.Millisecond, upload: 24 * time.Hour, uploadMaxAge: 24 * time.Hour, uploadMin: "2GiB"},
		restartPhase:    childPhase{flush: time.Second, upload: 100 * time.Millisecond, uploadMaxAge: time.Second, uploadMin: "1KiB"},
		pace:            150 * time.Millisecond,
		dropTail:        2,
		waitForManifest: true,
	}
}

// runCrashScenario drives one scenario end to end against child processes.
//
// All pushes go through both wire formats and gzip variants, with and
// without X-Scope-OrgID; entries span three UTC hours with sub-microsecond
// nanosecond components and arrive out of timestamp order across pushes.
// After recovery the test reads the catalog-visible Parquet objects through
// storage.Reader and requires the exact acknowledged ID multiset with all row
// content intact, while every unreferenced object must hold only expected rows
// and stay within the durable key-rotation budget. It then restarts the child
// once more with nothing new to do and requires the settled state to stay
// exact (a duplicate upload would break it). With every child stopped, it
// verifies a final time without skipping any guard and checks every catalog
// row against its object: matching key, row count, byte size and timestamp
// bounds.
func runCrashScenario(t *testing.T, bin string, sc scenario) {
	t.Helper()
	work := t.TempDir()
	dataDir, objDir := path.Join(work, "data"), path.Join(work, "objects")
	catalogDSN := path.Join(work, "catalog.db")
	tid := tenant.Default
	ctx := t.Context()

	// One port serves every phase of the scenario: a killed listener's port
	// is immediately reusable and nothing else binds it meanwhile.
	port := freePort(t)
	client := &http.Client{Timeout: 10 * time.Second}

	// Phase 1: ingest into the pre-crash state.
	sent := sentPlans(sc)
	c := startReadyIngester(t, bin, dataDir, objDir, catalogDSN, port, sc.ingestPhase)
	for i, p := range sent[:len(sent)-1] {
		pushBatch(t, client, c.addr(), p.tenantHeader, p.format, p.req)
		if sc.pace > 0 && i+1 < len(sent)-1 {
			time.Sleep(sc.pace)
		}
	}
	if sc.waitForManifest {
		// Authoritative wait before the final acknowledged push: the flush
		// tick must have committed local generations in the manifest while
		// the child still runs. Uploads are disabled in this phase, so
		// nothing can have reached the object store yet.
		waitForCommittedManifest(t, dataDir, tid)
	}
	last := sent[len(sent)-1]
	pushBatch(t, client, c.addr(), last.tenantHeader, last.format, last.req)
	expected := expectedForBurst(sent)

	if !sc.waitForManifest {
		// The flush tick cannot have fired in this phase; assert that the
		// kill really hits the WAL-only state.
		if n := localParquetCount(t, dataDir, tid); n != 0 {
			t.Fatalf("scenario %s: %d local Parquet files exist before the kill; the flush settings broke the scenario's determinism", sc.name, n)
		}
	}
	assertNoObjects(t, ctx, objDir, tid)
	t.Logf("scenario %s phase 1: physical object store empty before the kill; orphan count 0", sc.name)
	c.kill(t)
	assertNoObjects(t, ctx, objDir, tid)

	// Phase 2: restart and wait until the acknowledged multiset is
	// catalog-visible exactly once and the settled orphan budget passed.
	c = startReadyIngester(t, bin, dataDir, objDir, catalogDSN, port, sc.restartPhase)
	liveByKey, snap := waitForCatalogVisible(t, ctx, dataDir, objDir, catalogDSN, tid, expected)
	t.Logf("scenario %s: recovered %d rows in %d live objects; %d orphan(s) within %d key rotation(s), manifest seq %d",
		sc.name, countRows(liveByKey), len(liveByKey), len(snap.orphans), snap.rotations, snap.seq)
	c.kill(t)

	// Phase 3: a restart with nothing new must not duplicate or lose rows.
	c = startReadyIngester(t, bin, dataDir, objDir, catalogDSN, port, sc.restartPhase)
	assertCatalogVisibleStableFor(t, ctx, dataDir, objDir, catalogDSN, tid, expected, idleRestartWindow)
	c.kill(t)

	// Final state: every child stopped. The catalog-visible rows must still
	// be the exact multiset, every catalog row must have its physical object
	// with matching stats, and every orphan must pass its content and budget
	// guards. Nothing here is allowed to skip a guard.
	verifyFinalStore(t, ctx, dataDir, objDir, catalogDSN, tid, expected)
}

// --- deterministic push data -------------------------------------------------

// e2eBase is the epoch of the scenario data: every entry timestamp is base
// plus an offset derived from its ID, so the runs deterministically span
// several UTC hours.
var e2eBase = time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)

// e2eStreamLabel is the first of parkana's default -stream.labels, so it
// names the dataset (parkana stream) each entry lands in.
const e2eStreamLabel = "service_name"

// e2eJob is a plain (non-promoted) label that must stay in the labels map.
const e2eJob = "parkana-e2e"

// e2eStream is the dataset of an ID: the value of e2eStreamLabel.
func e2eStream(id int) string {
	if id%2 == 0 {
		return "api"
	}
	return "web"
}

// e2eLevel is the promoted label value of an ID. Promoted labels are stream
// labels, so the level value is part of the Loki label set: entries of
// different levels belong to different Loki streams of the same dataset.
func e2eLevel(id int) string {
	if id%3 == 0 {
		return "warn"
	}
	return "info"
}

// e2eTS derives an entry timestamp from its ID: three UTC hours (id%3),
// seconds, milliseconds, and a sub-microsecond nanosecond component that
// must truncate at the schema boundary.
func e2eTS(id int) time.Time {
	return e2eBase.
		Add(time.Duration(id%3) * time.Hour).
		Add(time.Duration(id%57) * 7 * time.Second).
		Add(time.Duration(id%13) * time.Millisecond).
		Add(time.Duration(id%5) * 100 * time.Nanosecond)
}

// e2eMeta is the structured metadata of an ID: nil for most entries.
func e2eMeta(id int) logs.Labels {
	if id%4 != 0 {
		return nil
	}
	return logs.New(logs.Label{Name: "trace_id", Value: fmt.Sprintf("%08x", uint32(id)*2654435761)})
}

// e2eLine is the pushed line of an ID; the ID is recoverable from it.
func e2eLine(id int) string {
	return fmt.Sprintf("id=%d stream=%s", id, e2eStream(id))
}

// e2eExpected is what one pushed entry must look like after recovery.
type e2eExpected struct {
	id int
	// ts is the pushed timestamp at full nanosecond precision; the stored
	// _ts is its truncation to microseconds (schema.TS).
	ts     time.Time
	line   string
	stream string            // dataset name (object key segment)
	labels map[string]string // non-promoted stream labels in the labels column
	meta   map[string]string // structured metadata; nil when the entry has none
	level  string            // value of the promoted "level" column
}

// expectedEntries builds the expected rows for IDs [lo, hi).
func expectedEntries(lo, hi int) map[int]e2eExpected {
	m := make(map[int]e2eExpected, hi-lo)
	for id := lo; id < hi; id++ {
		m[id] = e2eExpected{
			id:     id,
			ts:     e2eTS(id),
			line:   e2eLine(id),
			stream: e2eStream(id),
			labels: map[string]string{e2eStreamLabel: e2eStream(id), "job": e2eJob},
			meta:   maps.Clone(e2eMeta(id).Map()),
			level:  e2eLevel(id),
		}
	}
	return m
}

// streamLabels is the Loki label set of one (dataset, level) pair: the
// promoted level label plus the labels that stay in the labels column.
func streamLabels(dataset, level string) logs.Labels {
	return logs.New(
		logs.Label{Name: "job", Value: e2eJob},
		logs.Label{Name: "level", Value: level},
		logs.Label{Name: e2eStreamLabel, Value: dataset},
	)
}

// buildPush groups IDs [lo, hi) into Loki streams by label set, with each
// stream's entries sorted by timestamp (as Loki clients send them).
func buildPush(lo, hi int) *logs.PushRequest {
	type group struct {
		labels  logs.Labels
		entries []logs.Entry
	}
	groups := make(map[string]*group)
	for id := lo; id < hi; id++ {
		labels := streamLabels(e2eStream(id), e2eLevel(id))
		g := groups[labels.String()]
		if g == nil {
			g = new(group)
			g.labels = labels
			groups[labels.String()] = g
		}
		g.entries = append(g.entries, logs.Entry{Timestamp: e2eTS(id), Line: e2eLine(id), Metadata: e2eMeta(id)})
	}
	req := new(logs.PushRequest)
	for _, g := range groups {
		slices.SortStableFunc(g.entries, func(a, b logs.Entry) int { return a.Timestamp.Compare(b.Timestamp) })
		req.Streams = append(req.Streams, logs.Stream{Labels: g.labels, Entries: g.entries})
	}
	return req
}

// pushFormat selects the wire representation of one push.
type pushFormat struct {
	jsonBody bool // JSON, or snappy-block protobuf
	gzipBody bool // wrap the body in gzip
}

func (f pushFormat) contentType() string {
	if f.jsonBody {
		return "application/json"
	}
	return "application/x-protobuf"
}

func (f pushFormat) encode(ctx context.Context, req *logs.PushRequest) ([]byte, error) {
	var b []byte
	var err error
	if f.jsonBody {
		b, err = pushcodec.MarshalJSON(ctx, req)
	} else {
		b, err = pushcodec.MarshalSnappy(ctx, req)
	}
	if err != nil {
		return nil, fmt.Errorf("e2e: encode push: %w", err)
	}
	if !f.gzipBody {
		return b, nil
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, fmt.Errorf("e2e: gzip push: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("e2e: gzip push: %w", err)
	}
	return buf.Bytes(), nil
}

// pushPlan is one scheduled push of a scenario burst.
type pushPlan struct {
	req          *logs.PushRequest
	format       pushFormat
	tenantHeader tenant.ID // empty: omit X-Scope-OrgID
	lo, hi       int       // entry ID range [lo, hi) of the push
}

// burst schedules the pushes of a scenario. The WAL-only case sends four
// pushes in descending timestamp order (so the ingester must sort across
// pushes); the mixed-state case schedules six paced pushes of which the
// last sc.dropTail are never sent. Every wire format and both tenant-header
// variants appear within the acknowledged pushes.
func (sc scenario) burst() []pushPlan {
	var ranges [][2]int
	var formats []pushFormat
	var headers []tenant.ID
	if sc.dropTail == 0 {
		ranges = [][2]int{{120, 180}, {0, 60}, {180, 200}, {60, 120}}
		formats = []pushFormat{{}, {jsonBody: true}, {gzipBody: true}, {jsonBody: true, gzipBody: true}}
		headers = []tenant.ID{tenant.Default, "", tenant.Default, tenant.Default}
	} else {
		ranges = make([][2]int, 6)
		for i := range ranges {
			lo := i * 20
			ranges[i] = [2]int{lo, lo + 20}
		}
		formats = []pushFormat{
			{jsonBody: true}, {}, {jsonBody: true, gzipBody: true}, {gzipBody: true},
			{jsonBody: true}, {},
		}
		headers = []tenant.ID{tenant.Default, "", tenant.Default, tenant.Default, tenant.Default, ""}
	}
	plans := make([]pushPlan, len(ranges))
	for i, r := range ranges {
		plans[i] = pushPlan{req: buildPush(r[0], r[1]), format: formats[i], tenantHeader: headers[i], lo: r[0], hi: r[1]}
	}
	return plans
}

// sentPlans is the acknowledged prefix of a scenario's burst: the trailing
// dropTail pushes are never sent.
func sentPlans(sc scenario) []pushPlan {
	burst := sc.burst()
	return burst[:len(burst)-sc.dropTail]
}

// expectedForBurst builds the expected rows for the union of the sent
// pushes' ID ranges.
func expectedForBurst(sent []pushPlan) map[int]e2eExpected {
	out := make(map[int]e2eExpected)
	for _, p := range sent {
		maps.Copy(out, expectedEntries(p.lo, p.hi))
	}
	return out
}

// --- child process management ------------------------------------------------

// childPhase is the -ingest.* flag set of one child phase.
type childPhase struct {
	flush        time.Duration // -ingest.flush-interval
	upload       time.Duration // -ingest.upload-interval
	uploadMaxAge time.Duration // -ingest.upload-max-age
	uploadMin    string        // -ingest.upload-min (ByteSize string)
}

// ingesterArgs renders the child's command line: an isolated filesystem
// ingester on a fixed port with this phase's flush/upload pacing, and a
// SQLite catalog at a temp-dir DSN — the phase-1 acceptance path. Only
// existing config flags are used.
func ingesterArgs(dataDir, objDir, catalogDSN string, port int, ph childPhase) []string {
	return []string{
		"-roles=ingester",
		fmt.Sprintf("-http.addr=127.0.0.1:%d", port),
		"-data.dir=" + dataDir,
		"-storage.backend=fs",
		"-storage.fs.dir=" + objDir,
		"-catalog.driver=sqlite",
		"-catalog.dsn=" + catalogDSN,
		"-ingest.promoted-labels=*=level",
		"-ingest.flush-interval=" + ph.flush.String(),
		"-ingest.upload-interval=" + ph.upload.String(),
		"-ingest.upload-max-age=" + ph.uploadMaxAge.String(),
		"-ingest.upload-min=" + ph.uploadMin,
		"-ingest.upload-merge-max=2GiB",
	}
}

// childEnv is the parent environment without PARKANA_* variables: the
// child's config must come from its flags alone.
func childEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "PARKANA_") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// lockedBuffer collects child output; the child writes from its own
// goroutines, so access is mutex-guarded.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// e2eChild is one parkana child process. A single goroutine owns cmd.Wait:
// it stores the wait error and then closes done, so any number of readiness
// polls and stop checks can observe the exit without consuming a one-shot
// result.
type e2eChild struct {
	cmd      *exec.Cmd
	port     int
	log      lockedBuffer
	done     chan struct{} // closed once the child has been reaped
	mu       sync.Mutex
	waitErr  error
	killOnce sync.Once
}

// startIngester launches the child; it does not wait for readiness.
func startIngester(t *testing.T, bin string, dataDir, objDir, catalogDSN string, port int, ph childPhase) *e2eChild {
	t.Helper()
	c := &e2eChild{port: port, done: make(chan struct{})}
	c.cmd = exec.CommandContext(t.Context(), bin, ingesterArgs(dataDir, objDir, catalogDSN, port, ph)...)
	c.cmd.Env = childEnv()
	c.cmd.Stdout = &c.log
	c.cmd.Stderr = &c.log
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("e2e: starting %s: %v", bin, err)
	}
	// The only Wait owner. It never touches *testing.T.
	go func() {
		err := c.cmd.Wait()
		c.mu.Lock()
		c.waitErr = err
		c.mu.Unlock()
		close(c.done)
	}()
	// Registered after TempDir's cleanup, so processes die before the
	// directory is removed; kill is idempotent.
	t.Cleanup(func() { c.kill(t) })
	return c
}

// kill SIGKILLs the child and reaps it, exactly once. Safe from cleanups.
func (c *e2eChild) kill(t *testing.T) {
	t.Helper()
	c.killOnce.Do(func() {
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill() // SIGKILL: no graceful shutdown, no upload-on-stop
		}
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
			t.Errorf("e2e: ingester child did not exit within 10s of SIGKILL")
		}
	})
}

// exited reports whether the child has been reaped, and its wait error.
func (c *e2eChild) exited() (bool, error) {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return true, c.waitErr
	default:
		return false, nil
	}
}

func (c *e2eChild) addr() string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.port)
}

// logTail returns the end of the child's output for failure messages.
func (c *e2eChild) logTail() string {
	s := c.log.String()
	const limit = 8 * 1024
	if len(s) > limit {
		s = s[len(s)-limit:]
	}
	return strings.TrimRight(s, "\n")
}

// startReadyIngester starts a child and waits for /ready to answer 200
// (which, with the ingester role, gates on WAL replay being done). A child
// that dies before becoming ready — a lost bind race over the reserved
// port — is retried on a fresh port.
func startReadyIngester(t *testing.T, bin, dataDir, objDir, catalogDSN string, port int, ph childPhase) *e2eChild {
	t.Helper()
	p := port
	for attempt := 1; ; attempt++ {
		c := startIngester(t, bin, dataDir, objDir, catalogDSN, p, ph)
		err := c.waitReady(t)
		if err == nil {
			return c
		}
		c.kill(t)
		if attempt == 3 {
			t.Fatalf("e2e: ingester never became ready after %d attempts: %v\nlast child log:\n%s", attempt, err, c.logTail())
		}
		t.Logf("e2e: ingester attempt %d failed (%v); retrying on a fresh port", attempt, err)
		p = freePort(t)
	}
}

// waitReady polls GET /ready until it answers 200. It returns an error
// describing the last observed state on timeout or early child exit.
func (c *e2eChild) waitReady(t *testing.T) error {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(readyTimeout)
	var last string
	for time.Now().Before(deadline) {
		if done, err := c.exited(); done {
			return fmt.Errorf("child exited before ready: %v; log:\n%s", err, c.logTail())
		}
		resp, err := client.Get(c.addr() + "/ready")
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Sprintf("%s: %s", resp.Status, bytes.TrimSpace(body))
		} else {
			last = err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("not ready after %s (last response: %s)", readyTimeout, last)
}

// freePort reserves an ephemeral TCP port for the child to bind later.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("e2e: reserving a port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("e2e: releasing the reserved port: %v", err)
	}
	return port
}

// pushBatch POSTs one push request and requires a 204: the ingester must
// have fsynced the WAL before answering.
func pushBatch(t *testing.T, client *http.Client, addr string, tid tenant.ID, f pushFormat, req *logs.PushRequest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	body, err := f.encode(ctx, req)
	if err != nil {
		t.Fatalf("e2e: %v", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("e2e: %v", err)
	}
	httpReq.Header.Set("Content-Type", f.contentType())
	if f.gzipBody {
		httpReq.Header.Set("Content-Encoding", "gzip")
	}
	if tid != "" {
		httpReq.Header.Set(tenant.Header, string(tid))
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		t.Fatalf("e2e: push to %s: %v", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		t.Fatalf("e2e: push to %s: got %s, want 204: %s", addr, resp.Status, bytes.TrimSpace(snippet))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

// --- object-store verification ----------------------------------------------

// e2eRow is one recovered Parquet row.
type e2eRow struct {
	ts     int64 // _ts in UTC microseconds
	line   string
	labels map[string]string // labels column (promoted labels excluded)
	meta   map[string]string // meta column; nil when null
	level  string            // promoted "level" column; "" when null
}

// localParquetCount counts the local Parquet files of a tenant under the
// child's data directory (layout <data.dir>/parquet/<tenant>/<stream>/).
func localParquetCount(t *testing.T, dataDir string, tid tenant.ID) int {
	t.Helper()
	root, err := os.OpenRoot(dataDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0 // the child has not created the data dir yet
	}
	if err != nil {
		t.Fatalf("e2e: opening data dir %s: %v", dataDir, err)
	}
	defer root.Close()
	count := 0
	err = fs.WalkDir(root.FS(), path.Join("parquet", string(tid)), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, objstore.ExtData) {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("e2e: walking local parquet under %s: %v", dataDir, err)
	}
	return count
}

// localManifest is the subset of A7's committed generation manifest
// (<data.dir>/parquet/<tenant>/manifest.json) the scenarios inspect,
// decoded with encoding/json/v2; unknown members are ignored. Confirmed
// against the A7 source: LocalFile carries tenant, id (stdlib UUID text),
// stream, hour (time.Time) and retired; there is no path field.
//
// Seq is the manifest commit counter: every manifest commit increments it,
// which lets a cross-source snapshot detect a concurrent reservation,
// publication or cleanup. KeyRotations is A9's cumulative durable object-key
// rotation count and stays zero for legacy manifests without the field.
// Uploads are the open receipts: an object whose key is still reserved may be
// a Put in flight rather than an abandoned orphan.
type localManifest struct {
	Seq      uint64 `json:"seq"`
	Tenant   string `json:"tenant"`
	Frontier struct {
		Last int64 `json:"last"`
	} `json:"frontier"`
	KeyRotations uint64         `json:"keyRotations"`
	Files        []localFile    `json:"files"`
	Uploads      []uploadRecord `json:"uploads"`
}

// uploadRecord is the subset of an A9 upload receipt the orphan accounting
// needs.
type uploadRecord struct {
	Key string `json:"key"`
}

// manifestState is the read-only manifest projection the crash harness uses to
// bracket snapshots and bound orphans.
type manifestState struct {
	seq         uint64
	rotations   uint64
	openUploads int
}

// localFile mirrors one A7 LocalFile JSON row. A7 stores no path: the
// canonical location of a committed file derives as
// stream/yyyy/mm/dd/hh/<id>.parquet relative to the tenant root.
type localFile struct {
	Tenant  string    `json:"tenant"`
	ID      uuid.UUID `json:"id"`
	Stream  string    `json:"stream"`
	Hour    time.Time `json:"hour"`
	Retired bool      `json:"retired"`
}

// relPath is A7's canonical tenant-root-relative path of a committed file.
func (f localFile) relPath() string {
	return path.Join(f.Stream, f.Hour.UTC().Format("2006/01/02/15"), f.ID.String()+objstore.ExtData)
}

// waitForCommittedManifest polls the committed generation manifest until it
// shows an authoritative durable local state: the expected tenant at the
// manifest and file level, a positive WAL frontier, and committed unretired
// files that validate canonically (nonzero ID, sanitized stream, UTC hour)
// and exist as regular files at their canonical path. The manifest is
// written with a temp file, fsync and atomic rename, so a read never sees a
// torn state. It runs while the child is alive, before the final
// acknowledged push and kill.
func waitForCommittedManifest(t *testing.T, dataDir string, tid tenant.ID) {
	t.Helper()
	deadline := time.Now().Add(flushTickTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := checkLocalManifest(dataDir, tid); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("e2e: committed manifest under %s never reached the authoritative state within %s: %v", dataDir, flushTickTimeout, lastErr)
}

// readLocalManifest reads and decodes the committed generation manifest once.
// The manifest is published with a temp file, fsync and atomic rename, so a
// read never sees a torn state. A raw filesystem read cannot detect a pending
// post-rename directory sync, so callers must not treat a successful read as
// proof that the manifest is durably synced.
func readLocalManifest(dataDir string, tid tenant.ID) (localManifest, error) {
	tenantDir := path.Join(dataDir, "parquet", string(tid))
	root, err := os.OpenRoot(tenantDir)
	if err != nil {
		return localManifest{}, fmt.Errorf("open manifest dir: %w", err)
	}
	defer root.Close()
	mf, err := root.Open("manifest.json")
	if err != nil {
		return localManifest{}, fmt.Errorf("open manifest: %w", err)
	}
	defer mf.Close()
	b, err := io.ReadAll(mf)
	if err != nil {
		return localManifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m localManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return localManifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return m, nil
}

// readManifestState projects the committed manifest onto the fields the crash
// harness needs. Legacy manifests without seq/keyRotations/uploads decode their
// zero values, so an absent rotation counter reads as zero. The manifest tenant
// is checked before any field is trusted: a wrong-tenant file must never lend
// its sequence or rotation budget to this tenant's orphan accounting.
func readManifestState(dataDir string, tid tenant.ID) (manifestState, error) {
	m, err := readLocalManifest(dataDir, tid)
	if err != nil {
		return manifestState{}, err
	}
	if m.Tenant != string(tid) {
		return manifestState{}, fmt.Errorf("e2e: manifest tenant %q, want %q; refusing to trust its sequence or rotation budget", m.Tenant, tid)
	}
	return manifestState{seq: m.Seq, rotations: m.KeyRotations, openUploads: len(m.Uploads)}, nil
}

// checkLocalManifest validates one read of the committed manifest.
func checkLocalManifest(dataDir string, tid tenant.ID) error {
	m, err := readLocalManifest(dataDir, tid)
	if err != nil {
		return err
	}
	if m.Tenant != string(tid) {
		return fmt.Errorf("manifest tenant %q, want %q", m.Tenant, tid)
	}
	if m.Frontier.Last <= 0 {
		return fmt.Errorf("manifest frontier.last is %d, want > 0", m.Frontier.Last)
	}
	live := 0
	for _, f := range m.Files {
		if f.Retired {
			continue
		}
		live++
		if err := f.check(dataDir, tid); err != nil {
			return err
		}
	}
	if live == 0 {
		return fmt.Errorf("manifest lists no committed unretired files")
	}
	return nil
}

// check validates one committed file row canonically: its tenant, a nonzero
// ID, a sanitized stream name, an hour-aligned UTC instant, and a regular
// file at the derived canonical path under the tenant root of dataDir.
func (f localFile) check(dataDir string, tid tenant.ID) error {
	bad := func(msg string) error {
		return fmt.Errorf("committed file stream=%s id=%s hour=%s: %s", f.Stream, f.ID, f.Hour.UTC().Format(time.RFC3339), msg)
	}
	if f.Tenant != string(tid) {
		return bad(fmt.Sprintf("tenant %q, want %q", f.Tenant, tid))
	}
	if f.ID == uuid.Nil() {
		return bad("zero ID")
	}
	if logs.SanitizeStream(f.Stream) != f.Stream {
		return bad(fmt.Sprintf("stream %q is not canonical", f.Stream))
	}
	if !f.Hour.Equal(f.Hour.UTC().Truncate(time.Hour)) {
		return bad("hour is not a UTC hour boundary")
	}
	tenantDir := path.Join(dataDir, "parquet", f.Tenant)
	root, err := os.OpenRoot(tenantDir)
	if err != nil {
		return bad(fmt.Sprintf("open %s: %v", tenantDir, err))
	}
	defer root.Close()
	fh, err := root.Open(f.relPath())
	if err != nil {
		return bad(fmt.Sprintf("open canonical path %s: %v", f.relPath(), err))
	}
	defer fh.Close()
	fi, err := fh.Stat()
	if err != nil {
		return bad(fmt.Sprintf("stat canonical path %s: %v", f.relPath(), err))
	}
	if !fi.Mode().IsRegular() {
		return bad(fmt.Sprintf("canonical path %s is not a regular file", f.relPath()))
	}
	return nil
}

// catalogSnapshot lists every catalog row of the tenant, including deleted
// ones so tombstones can be rejected, through a fresh app.OpenCatalog handle on
// the child's DSN. Each poll uses its own handle, closed before returning, so a
// snapshot is one read transaction and never a stored pool that could drift
// from the child's view. Transient busy errors surface to the poll loop, which
// retries; the stopped final check fails with the error instead.
func catalogSnapshot(ctx context.Context, dsn string, tid tenant.ID) (map[string]catalog.File, error) {
	cat, err := app.OpenCatalog(config.Config{Catalog: config.Catalog{Driver: "sqlite", DSN: dsn}})
	if err != nil {
		return nil, fmt.Errorf("e2e: open catalog %s: %w", dsn, err)
	}
	defer func() { _ = cat.Close() }()
	live := make(map[string]catalog.File)
	for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: tid, IncludeDeleted: true}) {
		if err != nil {
			return nil, fmt.Errorf("e2e: list catalog files: %w", err)
		}
		if f.Deleted() {
			return nil, fmt.Errorf("e2e: catalog contains deleted row %s; no compactor runs in this scenario, tombstones must not exist", f.Key)
		}
		if _, dup := live[f.Key]; dup {
			return nil, fmt.Errorf("e2e: catalog lists %s twice", f.Key)
		}
		live[f.Key] = f
	}
	return live, nil
}

// storeSnapshot is one bracketed read of the durable state a phase verifies:
// the live catalog rows, every physical Parquet key, and the manifest
// projection. The manifest is read on both sides of the catalog and object
// listings. A changed commit sequence means a receipt, publication or cleanup
// committed between them, so the snapshot is discarded and retried instead of
// being compared across epochs.
type storeSnapshot struct {
	live      map[string]catalog.File
	objects   []string // physical Parquet keys at snapshot time
	orphans   []string // physical keys the live catalog does not reference
	rotations uint64
	// openUploads is the number of upload receipts still in the manifest. A
	// non-zero value means a non-catalog object may be a Put in flight rather
	// than a durably abandoned orphan, so phases do not complete on it.
	openUploads int
	seq         uint64
}

// takeSnapshot reads the durable state once. The bracketing manifest reads make
// the orphan set authoritative: with an unchanged commit sequence no receipt
// can have opened or closed while the catalog and object listings were taken.
// A catalog reference without a physical object is a hard durability failure
// and never becomes an ignored orphan.
func takeSnapshot(ctx context.Context, dataDir, objDir, dsn string, tid tenant.ID) (storeSnapshot, error) {
	before, err := readManifestState(dataDir, tid)
	if err != nil {
		return storeSnapshot{}, err
	}
	live, err := catalogSnapshot(ctx, dsn, tid)
	if err != nil {
		return storeSnapshot{}, err
	}
	objects, err := listObjectKeys(objDir, tid)
	if err != nil {
		return storeSnapshot{}, err
	}
	after, err := readManifestState(dataDir, tid)
	if err != nil {
		return storeSnapshot{}, err
	}
	if before.seq != after.seq || before.rotations != after.rotations || before.openUploads != after.openUploads {
		return storeSnapshot{}, fmt.Errorf("e2e: manifest changed while snapshotting (seq %d -> %d); retrying", before.seq, after.seq)
	}
	_, orphans, missing := classifyObjects(live, objects)
	if len(missing) > 0 {
		return storeSnapshot{}, fmt.Errorf("e2e: catalog references %d object(s) absent from the object store: %v", len(missing), missing)
	}
	return storeSnapshot{
		live:        live,
		objects:     objects,
		orphans:     orphans,
		rotations:   before.rotations,
		openUploads: before.openUploads,
		seq:         before.seq,
	}, nil
}

// classifyObjects splits the physical object keys against the live catalog
// rows: the keys the catalog references, the unreferenced orphans, and the
// catalog references with no physical object. Missing references are a hard
// failure, never an accepted orphan. All outputs are sorted for stable
// messages.
func classifyObjects(live map[string]catalog.File, objects []string) (referenced, orphans, missing []string) {
	physical := make(map[string]bool, len(objects))
	for _, key := range objects {
		physical[key] = true
	}
	referenced = make([]string, 0, len(objects))
	for _, key := range objects {
		if _, ok := live[key]; ok {
			referenced = append(referenced, key)
		} else {
			orphans = append(orphans, key)
		}
	}
	for key := range live {
		if !physical[key] {
			missing = append(missing, key)
		}
	}
	slices.Sort(referenced)
	slices.Sort(orphans)
	slices.Sort(missing)
	return referenced, orphans, missing
}

// checkOrphanBudget fails when the authoritative orphan count exceeds the
// tenant's cumulative durable object-key rotations. Each rotation is one
// opportunity for a previously Put object to be abandoned, so a larger count
// means an unexplained physical object.
func checkOrphanBudget(orphans int, rotations uint64) error {
	if uint64(orphans) > rotations {
		return fmt.Errorf("e2e: %d unreferenced Parquet object(s) exceed the tenant's %d durable key rotations (orphan budget)", orphans, rotations)
	}
	return nil
}

// errPhasePending means a polling phase has not settled yet: the caller keeps
// polling instead of failing the test.
var errPhasePending = errors.New("e2e: phase has not settled")

// checkPhaseComplete is the single completion gate of a polling phase. It
// reports a pending condition while the catalog-visible rows are incomplete or
// an upload receipt is still open — a fresh Put before AddFiles is not an
// abandoned orphan, and the manifest sequence alone does not make the Put and
// catalog writes atomic. Only when that authoritative settled state is
// reached does it evaluate the orphan budget. A nil result therefore always
// means the budget was actually checked and passed; a budget violation is
// returned unwrapped and must fail the phase immediately.
func checkPhaseComplete(snap storeSnapshot, liveProblem string) error {
	if liveProblem != "" {
		return fmt.Errorf("%w: catalog-visible rows incomplete: %s", errPhasePending, liveProblem)
	}
	if snap.openUploads != 0 {
		return fmt.Errorf("%w: %d upload receipt(s) still open, so unreferenced objects may be Puts in flight", errPhasePending, snap.openUploads)
	}
	if err := checkOrphanBudget(len(snap.orphans), snap.rotations); err != nil {
		return err
	}
	return nil
}

// listObjectKeys lists the uploaded Parquet object keys of a tenant.
func listObjectKeys(objDir string, tid tenant.ID) ([]string, error) {
	root, err := os.OpenRoot(objDir)
	if err != nil {
		return nil, fmt.Errorf("e2e: open object dir %s: %w", objDir, err)
	}
	defer root.Close()
	return objectKeys(root, tid)
}

// objectKeys lists the uploaded Parquet object keys of a tenant under root.
// The tenant-wide prefix comes from objstore.LogPrefix, whose empty stream name
// leaves a double slash ({tenant}/logs//); drop that extra stream slash so real
// {tenant}/logs/{stream}/... keys match.
func objectKeys(root *os.Root, tid tenant.ID) ([]string, error) {
	prefix := strings.TrimSuffix(objstore.LogPrefix(tid, ""), "/")
	var keys []string
	err := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, objstore.ExtData) && strings.HasPrefix(p, prefix) {
			keys = append(keys, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("e2e: listing objects under %s: %w", prefix, err)
	}
	return keys, nil
}

// readObjectRows reads one uploaded Parquet object through the approved
// storage.Reader API.
//
// It exercises the reader ownership contract from the consumer side: batches
// are borrowed (drained fully before the iterator advances, no Retain), and
// Reader.Close must release the reader without closing the caller's *os.File
// — verified by Stat-ing the file after Close.
func readObjectRows(ctx context.Context, root *os.Root, key string) ([]e2eRow, error) {
	f, err := root.Open(key)
	if err != nil {
		return nil, fmt.Errorf("e2e: open object %s: %w", key, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("e2e: stat object %s: %w", key, err)
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		return nil, fmt.Errorf("e2e: open parquet reader for %s: %w", key, err)
	}
	rows := []e2eRow{}
	prevTS := int64(-1) // files are written with ascending _ts; negative _ts never occurs here
	for rec, err := range rd.Records(ctx) {
		if err != nil {
			rd.Close()
			return nil, fmt.Errorf("e2e: read batches from %s: %w", key, err)
		}
		rows, prevTS, err = drainRows(key, rec, rows, prevTS)
		if err != nil {
			rd.Close()
			return nil, err
		}
	}
	if err := rd.Close(); err != nil {
		return nil, fmt.Errorf("e2e: close parquet reader for %s: %w", key, err)
	}
	if _, err := f.Stat(); err != nil {
		return nil, fmt.Errorf("e2e: storage.Reader closed the caller's file for %s, violating the reader ownership contract: %w", key, err)
	}
	return rows, nil
}

// drainRows appends one borrowed batch's rows to rows. Batches are drained
// fully into plain values before the iterator advances, so no Retain is
// needed; it also enforces the ascending-_ts file contract and the Arrow
// column types of internal/schema.
func drainRows(key string, rec arrow.RecordBatch, rows []e2eRow, prevTS int64) ([]e2eRow, int64, error) {
	cols := make(map[string]int, rec.NumCols())
	for i, f := range rec.Schema().Fields() {
		cols[f.Name] = i
	}
	for _, name := range []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta, "level"} {
		if _, ok := cols[name]; !ok {
			return rows, prevTS, fmt.Errorf("e2e: %s is missing column %q", key, name)
		}
	}
	bad := func(name string, col arrow.Array, want string) error {
		return fmt.Errorf("e2e: %s column %q has type %v, want %s", key, name, col.DataType(), want)
	}
	tsArr, ok := rec.Column(cols[schema.ColTS]).(*array.Timestamp)
	if !ok {
		return rows, prevTS, bad(schema.ColTS, rec.Column(cols[schema.ColTS]), "timestamp")
	}
	if tt, ok := tsArr.DataType().(*arrow.TimestampType); !ok || tt.Unit != arrow.Microsecond || tt.TimeZone != "UTC" {
		return rows, prevTS, fmt.Errorf("e2e: %s column %q must be TIMESTAMP(MICROS, UTC), got %v", key, schema.ColTS, tsArr.DataType())
	}
	lineArr, ok := rec.Column(cols[schema.ColLine]).(*array.String)
	if !ok {
		return rows, prevTS, bad(schema.ColLine, rec.Column(cols[schema.ColLine]), "string")
	}
	labelsArr, ok := rec.Column(cols[schema.ColLabels]).(*array.Map)
	if !ok {
		return rows, prevTS, bad(schema.ColLabels, rec.Column(cols[schema.ColLabels]), "map<string,string>")
	}
	metaArr, ok := rec.Column(cols[schema.ColMeta]).(*array.Map)
	if !ok {
		return rows, prevTS, bad(schema.ColMeta, rec.Column(cols[schema.ColMeta]), "map<string,string>")
	}
	levelArr, ok := rec.Column(cols["level"]).(*array.String)
	if !ok {
		return rows, prevTS, bad("level", rec.Column(cols["level"]), "string")
	}

	for j := range int(rec.NumRows()) {
		ts := int64(tsArr.Value(j))
		if ts < prevTS {
			return rows, prevTS, fmt.Errorf("e2e: %s rows are not ascending by _ts: %d after %d", key, ts, prevTS)
		}
		prevTS = ts
		row := e2eRow{
			ts:     ts,
			line:   lineArr.Value(j),
			labels: mapFromArrow(labelsArr, j),
			meta:   mapFromArrow(metaArr, j),
		}
		if !levelArr.IsNull(j) {
			row.level = levelArr.Value(j)
		}
		rows = append(rows, row)
	}
	return rows, prevTS, nil
}

// mapFromArrow reads row j of an Arrow string→string map as a plain map,
// nil when the value is null.
func mapFromArrow(m *array.Map, j int) map[string]string {
	if m.IsNull(j) {
		return nil
	}
	start, end := m.ValueOffsets(j)
	keys, items := m.Keys().(*array.String), m.Items().(*array.String)
	out := make(map[string]string, end-start)
	for k := start; k < end; k++ {
		out[keys.Value(int(k))] = items.Value(int(k))
	}
	return out
}

// readAllObjects reads every uploaded Parquet object of the tenant.
func readAllObjects(ctx context.Context, objDir string, tid tenant.ID) (map[string][]e2eRow, error) {
	keys, err := listObjectKeys(objDir, tid)
	if err != nil {
		return nil, err
	}
	return readObjectSet(ctx, objDir, keys)
}

// readObjectSet reads exactly keys, the physical listing of one snapshot, so a
// classification and its row reads never span two different object sets. An
// object that disappears mid-read (for example an orphan collected by the
// ingester) is an error and makes the poll retry instead of comparing partial
// state.
func readObjectSet(ctx context.Context, objDir string, keys []string) (map[string][]e2eRow, error) {
	root, err := os.OpenRoot(objDir)
	if err != nil {
		return nil, fmt.Errorf("e2e: open object dir %s: %w", objDir, err)
	}
	defer root.Close()
	out := make(map[string][]e2eRow, len(keys))
	for _, key := range keys {
		rows, err := readObjectRows(ctx, root, key)
		if err != nil {
			return nil, err
		}
		out[key] = rows
	}
	return out, nil
}

// selectRows returns the reads of the live catalog keys only.
func selectRows(byKey map[string][]e2eRow, live map[string]catalog.File) map[string][]e2eRow {
	out := make(map[string][]e2eRow, len(live))
	for key := range live {
		out[key] = byKey[key]
	}
	return out
}

// checkRowContent validates one recovered row against the scenario's expected
// rows: its line's ID, dataset, hour partition, truncated timestamp, line,
// promoted level, labels and structured metadata. It returns the parsed ID
// (valid whenever the line names one) and the problems found; an unknown or
// unparseable ID is itself a problem.
func checkRowContent(key string, r e2eRow, expected map[int]e2eExpected) (int, []string) {
	id, err := parseRowID(r.line)
	if err != nil {
		return -1, []string{fmt.Sprintf("%s: %v", key, err)}
	}
	exp, ok := expected[id]
	if !ok {
		return id, []string{fmt.Sprintf("unknown ID %d in %s (not part of this scenario)", id, key)}
	}
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	parts := strings.Split(key, "/")
	if len(parts) < 7 {
		add("object key %s does not match the {tenant}/logs/{stream}/yyyy/mm/dd/hh/ layout", key)
		return id, problems
	}
	stream := parts[len(parts)-6]
	stamp := strings.Join(parts[len(parts)-5:len(parts)-1], "/")
	if stream != exp.stream {
		add("ID %d recovered under stream %q, want %q", id, stream, exp.stream)
	}
	if st := schema.Time(r.ts).UTC().Format("2006/01/02/15"); st != stamp {
		add("ID %d has _ts hour %s but sits in hour partition %s", id, st, stamp)
	}
	if r.ts != exp.ts.UnixMicro() {
		add("ID %d has _ts %dµs, want %dµs", id, r.ts, exp.ts.UnixMicro())
	}
	if r.line != exp.line {
		add("ID %d line %q, want %q", id, r.line, exp.line)
	}
	if r.level != exp.level {
		add("ID %d promoted level %q, want %q", id, r.level, exp.level)
	}
	if !maps.Equal(r.labels, exp.labels) {
		add("ID %d labels %v, want %v", id, r.labels, exp.labels)
	}
	// Entries without metadata may come back as a null or empty map; both are
	// semantically empty. Entries with metadata must match exactly.
	if exp.meta == nil {
		if len(r.meta) > 0 {
			add("ID %d has unexpected meta %v, want none", id, r.meta)
		}
	} else if !maps.Equal(r.meta, exp.meta) {
		add("ID %d meta %v, want %v", id, r.meta, exp.meta)
	}
	return id, problems
}

// compareRows checks the catalog-visible objects against the expected
// multiset: every expected row exactly once, no unknown or duplicate IDs, all
// row content intact, and every row in the object its timestamp and dataset
// belong in (objstore.DataKey layout). It returns "" when everything matches,
// otherwise a bounded description of the first problems.
func compareRows(byKey map[string][]e2eRow, expected map[int]e2eExpected) string {
	var problems []string
	add := func(format string, args ...any) {
		if len(problems) < 5 {
			problems = append(problems, fmt.Sprintf(format, args...))
		}
	}
	got := make(map[int]int, len(expected))
	n := 0
	for key, rows := range byKey {
		for _, r := range rows {
			n++
			id, found := checkRowContent(key, r, expected)
			for _, problem := range found {
				add("%s", problem)
			}
			if id < 0 {
				continue
			}
			if _, ok := expected[id]; !ok {
				continue
			}
			got[id]++
			if got[id] == 2 {
				add("ID %d appears more than once (duplicate recovery)", id)
			}
		}
	}
	if n == 0 {
		return "no rows recovered yet"
	}
	var missing []string
	for id := range expected {
		if got[id] == 0 {
			missing = append(missing, strconv.Itoa(id))
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		if len(missing) > 5 {
			missing = append(missing[:5], fmt.Sprintf("… and %d more", len(missing)-5))
		}
		add("missing IDs %v", missing)
	}
	if len(problems) == 0 {
		return ""
	}
	return fmt.Sprintf("%d rows, %d problems: %s", n, len(problems), strings.Join(problems, "; "))
}

// checkOrphanRows validates every row of an object the live catalog does not
// reference: each row must belong to the scenario's expected set and carry the
// full expected content. Duplicates are allowed — an abandoned retry Put can
// legitimately hold rows that are also live elsewhere — but foreign, garbled
// or mispartitioned rows must never hide in a physically present object the
// catalog ignores.
func checkOrphanRows(key string, rows []e2eRow, expected map[int]e2eExpected) error {
	for _, r := range rows {
		if _, problems := checkRowContent(key, r, expected); len(problems) > 0 {
			return fmt.Errorf("e2e: unreferenced object %s holds a row outside the scenario's expected set: %s", key, strings.Join(problems, "; "))
		}
	}
	return nil
}

// checkOrphanObjects validates the rows of every unreferenced object in one
// read of the object store.
func checkOrphanObjects(byKey map[string][]e2eRow, orphans []string, expected map[int]e2eExpected) error {
	for _, key := range orphans {
		rows, ok := byKey[key]
		if !ok {
			return fmt.Errorf("e2e: unreferenced object %s was listed but could not be read", key)
		}
		if err := checkOrphanRows(key, rows, expected); err != nil {
			return err
		}
	}
	return nil
}

// parseRowID extracts the entry ID from a pushed line: "id=<n> stream=<s>".
func parseRowID(line string) (int, error) {
	rest, ok := strings.CutPrefix(line, "id=")
	if !ok {
		return 0, fmt.Errorf("line %q does not start with id=", line)
	}
	num, _, _ := strings.Cut(rest, " ")
	id, err := strconv.Atoi(num)
	if err != nil {
		return 0, fmt.Errorf("line %q has no numeric id", line)
	}
	return id, nil
}

// assertNoObjects requires that no uploaded object exists for the tenant: the
// phase-1 physical emptiness assertion, before any flush or upload.
func assertNoObjects(t *testing.T, ctx context.Context, objDir string, tid tenant.ID) {
	t.Helper()
	byKey, err := readAllObjects(ctx, objDir, tid)
	if err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			t.Fatalf("object Parquet cannot be read yet: %v (the storage implementation is still pending; recovery cannot be verified)", err)
		}
		t.Fatalf("e2e: reading objects: %v", err)
	}
	if len(byKey) > 0 {
		t.Fatalf("e2e: expected no uploaded objects, found %d: %v", len(byKey), slices.Collect(maps.Keys(byKey)))
	}
}

// pollStore takes one bracketed snapshot and reads every physical object, so
// the live rows and the unreferenced orphans are classified from the same
// object listing. The returned maps are the full read and its live-only subset.
func pollStore(ctx context.Context, dataDir, objDir, dsn string, tid tenant.ID) (storeSnapshot, map[string][]e2eRow, map[string][]e2eRow, error) {
	snap, err := takeSnapshot(ctx, dataDir, objDir, dsn, tid)
	if err != nil {
		return storeSnapshot{}, nil, nil, err
	}
	byKey, err := readObjectSet(ctx, objDir, snap.objects)
	if err != nil {
		return storeSnapshot{}, nil, nil, err
	}
	return snap, byKey, selectRows(byKey, snap.live), nil
}

// waitForCatalogVisible polls the durable state until the catalog-visible rows
// are exactly the expected multiset and the phase-completion gate passed: a
// stable manifest bracket, no open upload receipts, and an orphan budget that
// was actually evaluated. It returns the live read and the settled snapshot.
//
// The gate is deliberately not "rows look right": while a receipt is open, a
// non-catalog object can be a fresh Put awaiting Catalog.AddFiles, which is not
// an orphan. Only after the manifest settles with zero receipts can every
// unreferenced object be attributed to a durable key rotation. Orphan row
// content is validated on every poll regardless, so no foreign or garbled row
// can hide behind the pending state.
func waitForCatalogVisible(t *testing.T, ctx context.Context, dataDir, objDir, dsn string, tid tenant.ID, expected map[int]e2eExpected) (map[string][]e2eRow, storeSnapshot) {
	t.Helper()
	deadline := time.Now().Add(uploadTimeout)
	var lastErr error
	var lastProblem string
	var lastObjects string
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("e2e: %v while waiting for uploads", ctx.Err())
		}
		snap, byKey, liveRows, err := pollStore(ctx, dataDir, objDir, dsn, tid)
		if err != nil {
			lastErr = err
			time.Sleep(pollInterval)
			continue
		}
		if err := checkOrphanObjects(byKey, snap.orphans, expected); err != nil {
			t.Fatalf("e2e: during the upload wait: %v", err)
		}
		lastErr = nil
		lastProblem = compareRows(liveRows, expected)
		lastObjects = snapshotSummary(byKey)
		switch err := checkPhaseComplete(snap, lastProblem); {
		case err == nil:
			// The phase is settled: check every catalog row's object,
			// stats and timestamp bounds before returning, not only at the
			// final stop.
			verifyCatalogFiles(t, snap, byKey, objDir, tid, expected)
			return liveRows, snap
		case errors.Is(err, errPhasePending):
			lastErr = err
		default:
			t.Fatalf("e2e: after the catalog-visible dataset became complete: %v", err)
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("e2e: catalog-visible rows did not settle to the exact expected multiset within %s\nlast pending/error: %v\nlast comparison: %s\nobjects: %s",
		uploadTimeout, lastErr, lastProblem, lastObjects)
	return nil, storeSnapshot{}
}

// assertCatalogVisibleStableFor requires the catalog-visible multiset and the
// settled orphan budget to hold for the whole window while an idle restarted
// child runs its upload loop: a duplicate upload or lost source would change
// the live rows, and a reopened receipt would make the state pending again.
// Read errors are retried (the child may be replaying its WAL concurrently),
// but every semantic violation fails immediately.
func assertCatalogVisibleStableFor(t *testing.T, ctx context.Context, dataDir, objDir, dsn string, tid tenant.ID, expected map[int]e2eExpected, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	var lastSnap storeSnapshot
	var lastByKey map[string][]e2eRow
	var checked bool
	for time.Now().Before(deadline) {
		snap, byKey, liveRows, err := pollStore(ctx, dataDir, objDir, dsn, tid)
		if err != nil {
			time.Sleep(pollInterval)
			continue
		}
		if err := checkOrphanObjects(byKey, snap.orphans, expected); err != nil {
			t.Fatalf("e2e: during the idle-restart window: %v", err)
		}
		if err := checkPhaseComplete(snap, compareRows(liveRows, expected)); err != nil {
			t.Fatalf("e2e: the settled state changed while an idle ingester was running: %v", err)
		}
		lastSnap, lastByKey, checked = snap, byKey, true
		time.Sleep(idlePollInterval)
	}
	if !checked {
		t.Fatalf("e2e: could not read a settled snapshot during the %s idle-restart window", window)
	}
	// The phase must also leave every catalog row matching its object's
	// bytes, row count and timestamp bounds.
	verifyCatalogFiles(t, lastSnap, lastByKey, objDir, tid, expected)
	t.Logf("idle restart: %d orphan(s) within %d key rotation(s), manifest seq %d",
		len(lastSnap.orphans), lastSnap.rotations, lastSnap.seq)
}

// verifyFinalStore runs with every child stopped and proves the durable end
// state, failing rather than skipping any guard: the catalog-visible rows are
// exactly the acknowledged multiset, every catalog row has its physical object
// with matching stats, and every unreferenced object holds only expected rows
// within the tenant's durable rotation budget.
func verifyFinalStore(t *testing.T, ctx context.Context, dataDir, objDir, dsn string, tid tenant.ID, expected map[int]e2eExpected) {
	t.Helper()
	snap, byKey, liveRows, err := pollStore(ctx, dataDir, objDir, dsn, tid)
	if err != nil {
		t.Fatalf("e2e: final snapshot of the stopped store: %v", err)
	}
	if err := checkOrphanObjects(byKey, snap.orphans, expected); err != nil {
		t.Fatalf("e2e: final orphan check: %v", err)
	}
	// The final gate fails on pending state too: a stopped ingester must not
	// leave rows incomplete or receipts open, and the budget is never skipped.
	if err := checkPhaseComplete(snap, compareRows(liveRows, expected)); err != nil {
		t.Fatalf("e2e: final state did not settle: %v", err)
	}
	t.Logf("final store: %d rows in %d live objects, %d orphan(s) within %d key rotation(s), manifest seq %d",
		countRows(liveRows), len(liveRows), len(snap.orphans), snap.rotations, snap.seq)
	verifyCatalogFiles(t, snap, byKey, objDir, tid, expected)
}

// verifyCatalogFiles checks the settled catalog rows against the uploaded
// objects: every row belongs to this tenant, its stream matches the key layout,
// its object exists with the recorded byte size and row count, and its
// MinTS/MaxTS span the rows read from that object. Deleted rows were already
// rejected by the snapshot (no compactor runs here) and exact-once content was
// checked on the live row set before this runs.
func verifyCatalogFiles(t *testing.T, snap storeSnapshot, byKey map[string][]e2eRow, objDir string, tid tenant.ID, expected map[int]e2eExpected) {
	t.Helper()
	root, err := os.OpenRoot(objDir)
	if err != nil {
		t.Fatalf("e2e: open object dir %s: %v", objDir, err)
	}
	defer root.Close()
	total := 0
	for key, f := range snap.live {
		rows, ok := byKey[key]
		if !ok {
			t.Fatalf("e2e: catalog references %s but the object store does not have it", key)
		}
		if f.Tenant != tid {
			t.Fatalf("e2e: catalog row %s has tenant %q, want %q", key, f.Tenant, tid)
		}
		if stream := pathSegment(key, 6); f.Stream != stream {
			t.Fatalf("e2e: catalog row %s has stream %q, want %q", key, f.Stream, stream)
		}
		if got := int64(len(rows)); f.Rows != got {
			t.Fatalf("e2e: catalog row %s claims %d rows, the object holds %d", key, f.Rows, got)
		}
		fi, err := root.Stat(key)
		if err != nil {
			t.Fatalf("e2e: stat object %s listed in the catalog: %v", key, err)
		}
		if f.Bytes != fi.Size() {
			t.Fatalf("e2e: catalog row %s claims %d bytes, the object is %d", key, f.Bytes, fi.Size())
		}
		if len(rows) > 0 {
			// Objects are written in ascending _ts order; the first and last
			// rows are the file's actual bounds.
			if f.MinTS.UnixMicro() != rows[0].ts || f.MaxTS.UnixMicro() != rows[len(rows)-1].ts {
				t.Fatalf("e2e: catalog row %s claims _ts range [%d, %d]µs, the object spans [%d, %d]µs",
					key, f.MinTS.UnixMicro(), f.MaxTS.UnixMicro(), rows[0].ts, rows[len(rows)-1].ts)
			}
		}
		total += len(rows)
	}
	if total != len(expected) {
		t.Fatalf("e2e: catalog lists %d rows in total, want %d", total, len(expected))
	}
}

// snapshotSummary renders the per-object row counts of one read for failure
// messages.
func snapshotSummary(byKey map[string][]e2eRow) string {
	summary := make([]string, 0, len(byKey))
	for key, rows := range byKey {
		summary = append(summary, fmt.Sprintf("%s: %d rows", key, len(rows)))
	}
	slices.Sort(summary)
	return strings.Join(summary, ", ")
}

// pathSegment returns the segment offset levels from the end of a slash
// path (1 = last).
func pathSegment(key string, levels int) string {
	parts := strings.Split(key, "/")
	if levels > len(parts) {
		return ""
	}
	return parts[len(parts)-levels]
}

// countRows sums the rows of a per-key read.
func countRows(byKey map[string][]e2eRow) int {
	n := 0
	for _, rows := range byKey {
		n += len(rows)
	}
	return n
}
