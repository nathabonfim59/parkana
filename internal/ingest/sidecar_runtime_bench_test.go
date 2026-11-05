package ingest

// M8 real-size sidecar profiler (test-only; nothing here is a production knob,
// metric or endpoint).
//
// BenchmarkSidecarRuntimeProfile answers "where does sidecar-build time and
// memory go on one real object?" with two cases, both on GOMAXPROCS=4:
//
//   - single: one instrumented build of one fixture object.
//   - two-concurrent-ingest: two instrumented builds of two disjoint copies of
//     the same object while a bounded REAL ingest workload runs: the real JSON
//     push handler through the real HTTP server, the real WAL with fsync, the
//     real memtable admission, and the real local-Parquet flusher. Nothing is
//     a fake busy loop; the case fails unless WAL fsync, admission and at least
//     one flushed local Parquet object are observed.
//
// Fixture. The object is ONE api-stream production partition: 16 instance
// label sets of the api service, default Parkana schema (service_name and
// instance in the labels map, promoted "level" present but null, meta empty),
// and default index parameters (there are no parameter overrides here). It is
// not a four-stream mix. The corpus identity is the pinned parkbench v3
// small-run identity used by the A5 benchmarks: seed 8098146604754141185,
// start 2026-11-04T13:23:15.266317Z, step 100us, four streams and four
// instances in the source corpus; the fixture selects api records and labels
// them as 16 api instances, which keeps the object single-stream while
// exercising the label cardinality the parent asked for. The line renderer
// reuses the byte-pinned a5Record replica and adjusts only the corpus-v3
// stream mapping (streamFor) and its embedded service name; the fixed-length
// template's payload padding is adjusted by the service-length delta so its
// base stays exactly 256 bytes, matching the generator's pad-after-render
// order. The RNG draws, templates, timestamps and marker placement are
// unchanged. That is exactly what corpus.go changes between the v1 replica and
// v3 for the same seed/start, and it is pinned against digests emitted by the
// real cmd/parkbench generator through a temporary go-test overlay (first 4096
// records, all and api, 4 and 16 instances), not against another helper here.
// Lines are admitted through the real Memtable -> Generation.Parts
// -> storage.Writer path in ascending timestamp cohorts and written to one
// finalized Parquet object. PARKANA_A5_SIDE_RAW_BYTES defaults to 781250000
// (25e9/32, the mean raw bytes per object in the full v3 run) which is about
// 160 MB compressed; the run reports the actual raw, row and Parquet sizes.
//
// Baseline and read path. Before any profile starts, the real
// storage.BuildSidecar runs once on the fixture with index.DefaultParams.
// Each instrumented build must return byte-identical output to that baseline;
// there is no approximate comparison. The instrumented loop keeps production's
// full all-column Reader.Records decode even though BuildSidecar only consumes
// the line column, so the read stage includes the unneeded labels/meta decode
// exactly as production does; no projection or production change is applied.
//
// Stage split. The instrumented build is the production BuildSidecar loop with
// per-batch wall timers around (1) the reader iterator (all-column Parquet
// decode), (2) line-column resolution and extraction, (3) index.Builder.Add,
// and (4) Encode. Timers and pprof label switches happen once per batch, never
// per line or per gram. The extraction phase copies each non-null line's
// string header into a reusable slice so Add can be timed separately;
// production calls Add inline, so the stage split is a benchmark-only
// reordering of the same calls and the encoded bytes must still match the
// baseline exactly. Per-job stage walls are summed ACROSS JOBS and reported
// next to the concurrent case wall; they are wall durations, not CPU shares,
// and this file never derives a CPU share by subtracting stage times. CPU
// attribution comes from the per-case CPU profile, whose samples carry
// sidecar_stage=read|extract|add|encode labels. Go heap profiles do NOT
// preserve pprof labels, so allocation attribution is by allocation call stack
// (for example runtime.mapassign_fast64 under index.(*Builder).Add versus
// Arrow allocator/batch allocation under storage.(*Reader).Records and the
// bloom in Encode); the after profile is cumulative process allocation, so the
// base snapshot must be taken immediately before timed work and subtracted
// with -sample_index=alloc_space.
//
// Profiles. PARKANA_A5_SIDE_PROFILE_DIR selects the output directory. The
// baseline GC and allocs snapshot run after fixture generation, the baseline
// build, the push pool and the ingest service are ready, before the CPU
// profile starts and before timed work, so setup is outside the profile. Each
// case writes <case>.cpu.pprof, <case>.heap-base.pprof and <case>.heap.pprof;
// the heap files are the "allocs" profile (alloc_space and inuse_space), and
// the base file is subtracted with `go tool pprof -sample_index=alloc_space
// -base=<case>.heap-base.pprof <case>.heap.pprof`. Heap profiles are sampled
// at the runtime's default rate, so small allocations are estimates; the
// whole-case TotalAlloc delta is exact but includes all goroutines. Wall and
// getrusage endpoints are taken when the builds finish; the ingest workload is
// cancelled immediately afterwards so the CPU profiler drain cannot admit more
// work, and it is joined before the final GC. The CPU profile is stopped at
// that same build endpoint. A final GC runs after the work and ingest join,
// outside the measured CPU, wall and profile windows, before the after allocs
// snapshot: final inuse is post-GC/post-release, not peak, and the alloc-space
// delta can include the in-flight ingest drain as well as small
// profiler/snapshot bookkeeping. Useful analyses:
//
//	go tool pprof -tags <case>.cpu.pprof
//	go tool pprof -top -tagfocus=sidecar_stage=add <case>.cpu.pprof
//	go tool pprof -top -sample_index=alloc_space -base=<case>.heap-base.pprof <case>.heap.pprof
//
// The allocs diff still contains pprof's own fixed overhead (CPU-profile
// startup and heap-profile serialization), so discount runtime/pprof frames
// when reading alloc_space. Heap profiles do NOT preserve pprof labels, so
// allocation stage attribution is by call stack, not labels.
//
// The two objects in the concurrent case are separate copies on separate inodes,
// but both are page-cache warm after the fixture scan and baseline build. This
// is a local-FS warm-read CPU/alloc profile, NOT a cold-disk or S3 baseline.
//
// Cases are one-shot: run with -benchtime=1x -count=1.
//
// Environment:
//
//	PARKANA_A5_BENCH                     required gate
//	PARKANA_A5_SIDE_RAW_BYTES            fixture raw target (default 781250000)
//	PARKANA_A5_SIDE_SEAL_BYTES           fixture cohort seal size (default 32MiB)
//	PARKANA_A5_SIDE_INGEST_BYTES         pre-encoded ingest pool (default 256MiB)
//	PARKANA_A5_SIDE_INGEST_MAX_BYTES     ingest cap per case (default 2GiB)
//	PARKANA_A5_SIDE_PROFILE_DIR          optional CPU/heap output directory
//	PARKANA_A5_SIDE_FIXTURE              optional prebuilt Parquet fixture path
//
// Example (one heavy Boat round):
//
//	tm-boat test <worktree> -- env PARKANA_A5_BENCH=1 \
//	  PARKANA_A5_SIDE_PROFILE_DIR=/tmp/sidecar-profile \
//	  go test ./internal/ingest -run '^$' -bench '^BenchmarkSidecarRuntimeProfile$' \
//	  -benchtime=1x -count=1 -timeout=30m -v

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

const (
	// sidecarCorpusVersion is the parkbench corpus layout version.
	sidecarCorpusVersion = 3
	// sidecarFixtureStream is the single stream in the fixture object.
	sidecarFixtureStream = 0
	// sidecarFixtureInstances is the fixture object's instance label count:
	// one api-stream partition with 16 instance label sets.
	sidecarFixtureInstances = 16
	// sidecarIngestInstances is parkbench's default v3 instances per service
	// for the ingest workload (four streams times four instances).
	sidecarIngestInstances = 4
	// sidecarBatchEntries is parkbench's per-label-set request size.
	sidecarBatchEntries = 1250
	// sidecarCohortGlobal is one global record cohort: four streams times four
	// instances times 1250 entries. The fixture admits a whole cohort before
	// sealing so its generations stay timestamp-disjoint and storage.Writer
	// sees non-decreasing rows across generations.
	sidecarCohortGlobal int64 = int64(sidecarBatchEntries * len(a5Streams) * sidecarIngestInstances)
	// sidecarShortEvery is corpus.go's shortEvery: the v2/v3 stream rotation
	// period.
	sidecarShortEvery = 32
	// sidecarRawDefault is the fixture raw-byte default: 25e9/32.
	sidecarRawDefault int64 = 25_000_000_000 / 32
)

// sidecarStreamFor is corpus.go's streamFor for the v3 stream rotation.
func sidecarStreamFor(i int64, services int) int {
	return int((i%sidecarShortEvery + i/sidecarShortEvery) % int64(services))
}

// sidecarInstanceFor is corpus.go's instanceFor.
func sidecarInstanceFor(i int64, services, instances int) int {
	if services <= 0 || instances <= 0 {
		return 0
	}
	return int((i / sidecarShortEvery / int64(services)) % int64(instances))
}

// sidecarV3Service returns corpus-v3 record i's service index without
// rendering the line; the fixture uses it to skip the three quarters of the
// global corpus that are not api.
func sidecarV3Service(i int64) int {
	return sidecarStreamFor(i, len(a5Streams))
}

// sidecarV3Line renders corpus-v3 record i with the pinned a5Record line
// replica. The only differences from the v1 replica are the v3 stream mapping,
// the service name it embeds, and the fixed template's payload padding (which
// must shrink or grow to keep that template's base at 256 bytes); the RNG
// stream, template choice, timestamp and markers are untouched. For the nginx
// template the line embeds no service name, so no text substitution is needed.
func sidecarV3Line(i int64) (line string, service int, ts time.Time, tpl int, err error) {
	line, v1Stream, ts, tpl, _ := a5Record(i)
	service = sidecarV3Service(i)
	if tpl != a5TemplateNginx && v1Stream != service {
		if !strings.Contains(line, a5Streams[v1Stream]) {
			return "", 0, time.Time{}, 0, fmt.Errorf(
				"sidecar-profile: v1 record %d template %d has no %q service token", i, tpl, a5Streams[v1Stream])
		}
		// The service field precedes every other field that can contain a
		// stream name (the access template's path may also contain one), so
		// replacing the first occurrence substitutes exactly the service.
		line = strings.Replace(line, a5Streams[v1Stream], a5Streams[service], 1)
		if tpl == a5TemplateFixed {
			// This template pads its base line to exactly 256 bytes before any
			// marker suffix; the real generator pads after writing the v3
			// service, so undo the substitution's length change in the payload
			// x-run rather than changing the line length.
			delta := len(a5Streams[service]) - len(a5Streams[v1Stream])
			baseEnd := len(line)
			if i := strings.Index(line, ` marker="`); i >= 0 {
				baseEnd = i
			}
			switch {
			case delta > 0:
				start := baseEnd - delta
				if start < 0 || line[start:baseEnd] != strings.Repeat("x", delta) {
					return "", 0, time.Time{}, 0, fmt.Errorf(
						"sidecar-profile: fixed record %d cannot trim %d payload bytes", i, delta)
				}
				line = line[:start] + line[baseEnd:]
			case delta < 0:
				line = line[:baseEnd] + strings.Repeat("x", -delta) + line[baseEnd:]
			}
		}
	}
	return line, service, ts, tpl, nil
}

// Independent corpus-v3 parity constants. They were produced by the REAL
// cmd/parkbench generator (not by this adapter) through a temporary go-test
// -overlay harness shared with sol-a-p4-memprofile, for records 0..4095 with
// seed 8098146604754141185, start 2026-11-04T13:23:15.266317Z, step 100us and
// the four default streams. all = every record; api = the streamFor==0 subset.
// Digest formulas: line bytes + "\n"; timestamp as 8-byte little-endian
// UnixMicro; instance as 8-byte little-endian integer; tsinst = ts then inst.
// The line and timestamp digests are identical for 4 and 16 instances because
// instances only relabel records.
const (
	sidecarV3AllLinesSHA  = "d36c93664e3e488882a40376da5c91dd8e617978d9d461e127f59aed4e015d5d"
	sidecarV3AllRaw       = 613727
	sidecarV3AllTSSHA     = "31c78d22d6b04d13231ce01e82afe23abdc37aa86d4d485480a4f3742f32e12b"
	sidecarV3AllInst4SHA  = "d483d87e80e12d07690f425725e0c539168360d49b21fb7113cc2adbefc24965"
	sidecarV3AllInst16SHA = "2e1631210ee4d8b6b71c64e061bf46be3b7dd6ec709575c580d0e3fe4e1e3678"
	sidecarV3AllTSI4SHA   = "a269a3a11661c220a51329a16d1157af7e2c6c4a5a32760713bb07412c7f75d7"
	sidecarV3AllTSI16SHA  = "901eab187903e893a2163790cd73a59cd6e9e53c4e845bebe9b08cddce34d3b8"
	sidecarV3APILinesSHA  = "d8b2fee54d6f7343021ec0364c94ac3ddf2792db402d5ab26c309a3759eb1e6a"
	sidecarV3APIRaw       = 151508
	sidecarV3APICount     = 1024
	sidecarV3APITSSHA     = "7b27b726c21da900ac77b3cf9882efccfac07f34b6ee26c96700f1484bdbdb70"
	sidecarV3APIInst4SHA  = "429218491344830f209175e1997fba105e0ad8280071ceaa5669dc06f9a5fc5d"
	sidecarV3APIInst16SHA = "7acd5acb395d76f1db67c97d02ccd9b3b082cae0739894ba0b81e0264823c33f"
	sidecarV3APITSI4SHA   = "3ab5fafe17279ec8daf1006012099aabf2d8a960a3427123ee574bb2b5d44b30"
	sidecarV3APITSI16SHA  = "1918f0964e7d8ca34b9641959073c18dd4c797e1d9eff9bc452ea9da60816211"
)

// sidecarV3Digest is one subset's digest set.
type sidecarV3Digest struct {
	lines, timestamps, instances, tsInstances [sha256.Size]byte
	raw                                       int64
	n                                         int
}

// sidecarV3DigestFor computes the adapter's digests for the first 4096 v3
// records, either all records or the api streamFor==0 subset.
func sidecarV3DigestFor(instances int, apiOnly bool) (sidecarV3Digest, error) {
	var d sidecarV3Digest
	lines, timestamps, instancesH, tsInstances := sha256.New(), sha256.New(), sha256.New(), sha256.New()
	for i := int64(0); i < a5VerifyRecords; i++ {
		line, service, ts, _, err := sidecarV3Line(i)
		if err != nil {
			return d, err
		}
		if apiOnly && service != sidecarFixtureStream {
			continue
		}
		instance := sidecarInstanceFor(i, len(a5Streams), instances)
		var tsBuf, instBuf [8]byte
		binary.LittleEndian.PutUint64(tsBuf[:], uint64(ts.UnixMicro()))
		binary.LittleEndian.PutUint64(instBuf[:], uint64(instance))
		_, _ = io.WriteString(lines, line)
		_, _ = io.WriteString(lines, "\n")
		_, _ = timestamps.Write(tsBuf[:])
		_, _ = instancesH.Write(instBuf[:])
		_, _ = tsInstances.Write(tsBuf[:])
		_, _ = tsInstances.Write(instBuf[:])
		d.raw += int64(len(line))
		d.n++
	}
	copy(d.lines[:], lines.Sum(nil))
	copy(d.timestamps[:], timestamps.Sum(nil))
	copy(d.instances[:], instancesH.Sum(nil))
	copy(d.tsInstances[:], tsInstances.Sum(nil))
	return d, nil
}

// sidecarVerifyV3Adapter proves the adapter matches the real generator's
// pinned corpus-v3 digests, not another helper in this file.
func sidecarVerifyV3Adapter() error {
	if a5Streams[sidecarFixtureStream] != "api" {
		return fmt.Errorf("sidecar-profile: fixture stream index %d is %q, want api", sidecarFixtureStream, a5Streams[sidecarFixtureStream])
	}
	all4, err := sidecarV3DigestFor(sidecarIngestInstances, false)
	if err != nil {
		return err
	}
	all16, err := sidecarV3DigestFor(sidecarFixtureInstances, false)
	if err != nil {
		return err
	}
	api4, err := sidecarV3DigestFor(sidecarIngestInstances, true)
	if err != nil {
		return err
	}
	api16, err := sidecarV3DigestFor(sidecarFixtureInstances, true)
	if err != nil {
		return err
	}
	switch {
	case all4.n != a5VerifyRecords || all16.n != a5VerifyRecords:
		return fmt.Errorf("sidecar-profile: real-v3 all record count = %d/%d, want %d", all4.n, all16.n, a5VerifyRecords)
	case all4.raw != sidecarV3AllRaw || all16.raw != sidecarV3AllRaw:
		return fmt.Errorf("sidecar-profile: real-v3 all raw = %d/%d, want %d", all4.raw, all16.raw, sidecarV3AllRaw)
	case api4.n != sidecarV3APICount || api16.n != sidecarV3APICount:
		return fmt.Errorf("sidecar-profile: real-v3 api count = %d/%d, want %d", api4.n, api16.n, sidecarV3APICount)
	case api4.raw != sidecarV3APIRaw || api16.raw != sidecarV3APIRaw:
		return fmt.Errorf("sidecar-profile: real-v3 api raw = %d/%d, want %d", api4.raw, api16.raw, sidecarV3APIRaw)
	}
	checks := []struct {
		name string
		got  [sha256.Size]byte
		want string
	}{
		{"all lines", all4.lines, sidecarV3AllLinesSHA},
		{"all lines (16 instances)", all16.lines, sidecarV3AllLinesSHA},
		{"all timestamps", all4.timestamps, sidecarV3AllTSSHA},
		{"all instances (4)", all4.instances, sidecarV3AllInst4SHA},
		{"all instances (16)", all16.instances, sidecarV3AllInst16SHA},
		{"all tsinst (4)", all4.tsInstances, sidecarV3AllTSI4SHA},
		{"all tsinst (16)", all16.tsInstances, sidecarV3AllTSI16SHA},
		{"api lines", api4.lines, sidecarV3APILinesSHA},
		{"api lines (16 instances)", api16.lines, sidecarV3APILinesSHA},
		{"api timestamps", api4.timestamps, sidecarV3APITSSHA},
		{"api instances (4)", api4.instances, sidecarV3APIInst4SHA},
		{"api instances (16)", api16.instances, sidecarV3APIInst16SHA},
		{"api tsinst (4)", api4.tsInstances, sidecarV3APITSI4SHA},
		{"api tsinst (16)", api16.tsInstances, sidecarV3APITSI16SHA},
	}
	for _, check := range checks {
		if got := hex.EncodeToString(check.got[:]); got != check.want {
			return fmt.Errorf("sidecar-profile: real-v3 digest %s = %s, want %s", check.name, got, check.want)
		}
	}
	return nil
}

// sidecarGenerated is the generation-time identity of a fixture, compared with
// the independent post-write scan.
type sidecarGenerated struct {
	rows  int64
	raw   int64
	sha   [sha256.Size]byte
	first time.Time
	last  time.Time
}

// sidecarFixture is one finalized Parquet fixture object and its identity.
type sidecarFixture struct {
	path        string
	rows        int64
	raw         int64
	parquet     int64
	first       time.Time
	last        time.Time
	lineSHA     [sha256.Size]byte
	baseline    []byte
	baselineSHA [sha256.Size]byte
}

// BenchmarkSidecarRuntimeProfile profiles real-size sidecar builds on four
// CPUs. See the package comment.
func BenchmarkSidecarRuntimeProfile(b *testing.B) {
	if os.Getenv("PARKANA_A5_BENCH") == "" {
		b.Skip("sidecar-profile: set PARKANA_A5_BENCH=1 (plus optional PARKANA_A5_SIDE_* variables) to run the heavy sidecar profiler")
	}
	ctx := b.Context()
	rawTarget := a5EnvInt(b, "PARKANA_A5_SIDE_RAW_BYTES", sidecarRawDefault)
	seal := a5EnvInt(b, "PARKANA_A5_SIDE_SEAL_BYTES", 32<<20)
	ingestPool := a5EnvInt(b, "PARKANA_A5_SIDE_INGEST_BYTES", 256<<20)
	ingestCap := a5EnvInt(b, "PARKANA_A5_SIDE_INGEST_MAX_BYTES", 2<<30)
	if err := sidecarVerifyV3Adapter(); err != nil {
		b.Fatal(err)
	}
	if err := a5VerifyReplica(); err != nil {
		b.Fatal(err)
	}

	oldProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldProcs)

	b.Logf("sidecar-profile: v3 adapter parity: real cmd/parkbench overlay digests matched for first %d records (all+api subsets, instances-per-service 4 and 16; line/timestamp/instance)", a5VerifyRecords)

	commit, dirty, origin := a5SourceIdentity()
	b.Logf("sidecar-profile: identity commit=%s dirty=%s origin=%s corpus=v%d seed=%d start=%s step=%s streams=%v source-instances=%d fixture=one-api-stream/%d-instance-label-sets stream-label=service_name promoted=%v go=%s numcpu=%d gomaxprocs=%d",
		commit, dirty, origin, sidecarCorpusVersion, a5Seed, a5Start.Format(time.RFC3339Nano), a5Step,
		a5Streams, sidecarIngestInstances, sidecarFixtureInstances, a5PromotedFor("api"),
		runtime.Version(), runtime.NumCPU(), runtime.GOMAXPROCS(0))

	fx := sidecarPrepareFixture(b, ctx, rawTarget, seal)
	b.Logf("sidecar-profile: fixture path=%s rows=%d raw=%d parquet=%d (%.2f MiB) raw/parquet=%.3f span=[%s..%s] utc-hour=%s line-sha256=%x (one api-stream Parquet object, 16 instance label sets, default schema; not a 4-stream mix)",
		fx.path, fx.rows, fx.raw, fx.parquet, float64(fx.parquet)/(1<<20),
		float64(fx.raw)/float64(max(fx.parquet, 1)), fx.first, fx.last, fx.first.Truncate(time.Hour), fx.lineSHA)

	dir := sidecarProfileDir(b)
	// The second object is a byte copy, so two concurrent builds model two
	// real objects rather than two readers of one inode.
	copyPath := filepath.Join(b.TempDir(), "sidecar-fixture-copy.parquet")
	if err := sidecarCopyFile(fx.path, copyPath); err != nil {
		b.Fatalf("sidecar-profile: copy fixture: %v", err)
	}
	b.Logf("sidecar-profile: two disjoint fixture copies (separate inodes); both page-cache warm after the scan/baseline, so timings are local-FS warm reads, not a cold-disk or S3 baseline")

	b.Run("single", func(b *testing.B) {
		b.SetBytes(fx.raw)
		res := sidecarRunProfiledCase(b, dir, func(ctx context.Context, stopCPU func()) (sidecarCaseResult, error) {
			cpuBefore := a5ReadCPU()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)
			stat, encoded, err := sidecarInstrumentedBuild(ctx, fx.path)
			cpuAfter := a5ReadCPU()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)
			// Stop the CPU profile at the build endpoint, before the
			// byte-equality check, so the profile window is exactly the
			// instrumented build and the check is not attributed to it.
			stopCPU()
			if err != nil {
				return sidecarCaseResult{}, err
			}
			if err := sidecarRequireEquivalence(stat, encoded, fx); err != nil {
				return sidecarCaseResult{}, err
			}
			return sidecarCaseResult{
				wall:           stat.wall,
				jobs:           []sidecarBuildStat{stat},
				cpuUser:        cpuAfter.user - cpuBefore.user,
				cpuSys:         cpuAfter.sys - cpuBefore.sys,
				caseAllocDelta: memAfter.TotalAlloc - memBefore.TotalAlloc,
				gcCycles:       uint64(memAfter.NumGC - memBefore.NumGC),
			}, nil
		})
		sidecarReportCase(b, "single", res)
	})

	b.Run("two-concurrent-ingest", func(b *testing.B) {
		pool := sidecarBuildPushPool(b, ctx, ingestPool)
		svc := sidecarNewPushService(b)
		b.Cleanup(svc.stop)
		b.SetBytes(2 * fx.raw)
		res := sidecarRunProfiledCase(b, dir, func(ctx context.Context, stopCPU func()) (sidecarCaseResult, error) {
			return sidecarRunConcurrent(ctx, fx, copyPath, pool, svc.url, ingestCap, stopCPU)
		})
		// Record concurrent flush activity before the shutdown drain, then the
		// post-drain total separately. A tiny smoke may show only the drain;
		// label that honestly instead of presenting it as concurrent work.
		beforeFlush := sidecarFlushOK(b, svc.reg)
		b.Logf("sidecar-profile: ingest flush-before-stop=%g (flush-loop publishes observed while the case ran; shutdown drain is separate)", beforeFlush)
		svc.stop()
		afterFlush := sidecarFlushOK(b, svc.reg)
		concurrent := beforeFlush > 0
		b.Logf("sidecar-profile: ingest flush-before-stop=%g after-stop=%g shutdown-drain-delta=%g concurrent-flush-observed=%t",
			beforeFlush, afterFlush, afterFlush-beforeFlush, concurrent)
		if svc.runErr != nil {
			b.Fatalf("sidecar-profile: ingest service Run returned %v", svc.runErr)
		}
		if res.ingest.requests == 0 || res.ingest.raw == 0 || res.ingest.lines == 0 {
			b.Fatalf("sidecar-profile: ingest case admitted requests=%d raw=%d lines=%d, want positive",
				res.ingest.requests, res.ingest.raw, res.ingest.lines)
		}
		a5RequireRegistry(b, svc.reg,
			a5MetricReq{name: "parkana_ingest_wal_fsync_seconds_count", positive: true, family: "parkana_ingest_wal_fsync_seconds"},
			a5MetricReq{name: "parkana_ingest_lines_total", positive: true},
			a5MetricReq{name: "parkana_ingest_bytes_total", positive: true},
			a5MetricReq{name: "parkana_ingest_flush_total", positive: true},
			a5MetricReq{name: "parkana_ingest_wal_errors_total"})
		files, bytes := sidecarLocalParquet(svc.dataDir)
		if files == 0 || bytes == 0 {
			b.Fatalf("sidecar-profile: ingest wrote %d local Parquet files (%d bytes), want positive", files, bytes)
		}
		b.Logf("sidecar-profile: ingest local Parquet files=%d bytes=%d", files, bytes)
		sidecarReportCase(b, "two-concurrent-ingest", res)
	})
}

// sidecarPrepareFixture returns a scanned, baseline-verified fixture, either
// generated into b.TempDir or read from PARKANA_A5_SIDE_FIXTURE.
func sidecarPrepareFixture(b *testing.B, ctx context.Context, rawTarget, seal int64) sidecarFixture {
	b.Helper()
	path := os.Getenv("PARKANA_A5_SIDE_FIXTURE")
	generated := path == ""
	var want sidecarGenerated
	if generated {
		path = filepath.Join(b.TempDir(), "sidecar-fixture.parquet")
		var err error
		want, err = sidecarGenerateFixture(ctx, path, rawTarget, seal)
		if err != nil {
			b.Fatalf("sidecar-profile: generate fixture: %v", err)
		}
	}
	fx, err := sidecarDescribeFixture(ctx, path)
	if err != nil {
		b.Fatalf("sidecar-profile: describe fixture %s: %v", path, err)
	}
	if generated {
		if fx.rows != want.rows || fx.raw != want.raw || fx.lineSHA != want.sha ||
			!fx.first.Equal(want.first) || !fx.last.Equal(want.last) {
			b.Fatalf("sidecar-profile: fixture scan rows=%d raw=%d sha=%x span=[%s..%s], generation tracked rows=%d raw=%d sha=%x span=[%s..%s]",
				fx.rows, fx.raw, fx.lineSHA, fx.first, fx.last, want.rows, want.raw, want.sha, want.first, want.last)
		}
	} else {
		b.Logf("sidecar-profile: using caller-supplied fixture %s (contents are not checked against corpus v3)", path)
	}
	if fx.first.IsZero() || fx.last.IsZero() {
		b.Fatalf("sidecar-profile: fixture has no timestamps")
	}
	if fx.first.Truncate(time.Hour) != fx.last.Truncate(time.Hour) {
		b.Fatalf("sidecar-profile: fixture spans %s..%s across a UTC hour boundary; this is not one production stream/hour partition (lower PARKANA_A5_SIDE_RAW_BYTES)",
			fx.first, fx.last)
	}
	fx.baseline, err = sidecarProductionBaseline(ctx, path)
	if err != nil {
		b.Fatalf("sidecar-profile: production baseline: %v", err)
	}
	fx.baselineSHA = sha256.Sum256(fx.baseline)
	b.Logf("sidecar-profile: baseline storage.BuildSidecar params=N%d/FP%g/cap%d bytes=%d sha256=%x (full all-column Reader.Records decode; no projection)",
		index.DefaultParams.N, index.DefaultParams.FPRate, index.DefaultParams.MaxGrams,
		len(fx.baseline), fx.baselineSHA)
	return fx
}

// sidecarGenerateFixture builds one finalized api-stream Parquet object
// through the real Memtable -> Generation.Parts -> storage.Writer path. It
// admits complete timestamp cohorts and seals deliberately between cohorts so
// the writer sees non-decreasing timestamps; the fixture is setup and never
// timed.
func sidecarGenerateFixture(ctx context.Context, path string, target, seal int64) (sidecarGenerated, error) {
	var out sidecarGenerated
	mem, err := NewMemtable(MemtableOptions{
		StreamLabels: []string{"service_name"},
		PromotedFor:  a5PromotedFor,
		// Deliberate cohort seals keep the active generation and the writer's
		// row group bounded; production's auto-seal would be restart-order
		// dependent here because fixture requests are admitted per label set.
		MemtableMaxBytes: 0,
	})
	if err != nil {
		return out, err
	}
	defer mem.Close()
	f, err := os.Create(path)
	if err != nil {
		return out, err
	}
	defer func() { _ = f.Close() }()
	var (
		w            *storage.Writer
		seq          uint64
		rawSinceSeal int64
		cohortRaw    int64
		h            = sha256.New()
	)
	var batches [sidecarFixtureInstances][]logs.Entry
	defer func() {
		if w != nil {
			_, _ = w.Close()
		}
	}()

	writeGen := func(g *Generation) error {
		parts, err := g.Parts(ctx)
		if err != nil {
			return err
		}
		for _, p := range parts {
			if p.Stream != a5Streams[sidecarFixtureStream] {
				continue // the fixture corpus is api-only
			}
			if w == nil {
				w, err = storage.NewWriter(f, p.Record.Schema(), a5PromotedFor(p.Stream))
				if err != nil {
					return err
				}
			}
			if err := w.Write(p.Record); err != nil {
				return err
			}
		}
		return nil
	}
	drain := func() error {
		gens, err := mem.TakeSealed(ctx, tenant.Default)
		if err != nil {
			return err
		}
		for _, g := range gens {
			err := writeGen(g)
			g.Release()
			if err != nil {
				return err
			}
		}
		return nil
	}
	admitCohort := func() error {
		for instance := range batches {
			entries := batches[instance]
			if len(entries) == 0 {
				continue
			}
			batches[instance] = entries[:0]
			labels := logs.New(
				logs.Label{Name: "service_name", Value: a5Streams[sidecarFixtureStream]},
				logs.Label{Name: "instance", Value: strconv.Itoa(instance)},
			)
			req := &logs.PushRequest{Streams: []logs.Stream{{Labels: labels, Entries: entries}}}
			prepared, err := mem.Prepare(ctx, tenant.Default, req)
			if err != nil {
				return err
			}
			payload, err := wal.Encode(ctx, *req)
			if err != nil {
				return err
			}
			seq++
			if err := mem.AppendPrepared(ctx, tenant.Default, seq, int64(payload.Size()), prepared); err != nil {
				return err
			}
		}
		rawSinceSeal += cohortRaw
		cohortRaw = 0
		if rawSinceSeal >= seal {
			if err := mem.Seal(ctx, tenant.Default); err != nil {
				return err
			}
			rawSinceSeal = 0
		}
		return drain()
	}

	for i := int64(0); ; i++ {
		if sidecarV3Service(i) == sidecarFixtureStream {
			line, _, ts, _, err := sidecarV3Line(i)
			if err != nil {
				return out, err
			}
			instance := sidecarInstanceFor(i, len(a5Streams), sidecarFixtureInstances)
			batches[instance] = append(batches[instance], logs.Entry{Timestamp: ts, Line: line})
			if out.first.IsZero() {
				out.first = ts
			}
			out.last = ts
			_, _ = io.WriteString(h, line)
			_, _ = io.WriteString(h, "\n")
			out.rows++
			out.raw += int64(len(line))
			cohortRaw += int64(len(line))
		}
		if (i+1)%sidecarCohortGlobal != 0 {
			continue
		}
		if err := admitCohort(); err != nil {
			return out, err
		}
		if out.raw >= target {
			break
		}
	}
	if err := mem.Seal(ctx, tenant.Default); err != nil {
		return out, err
	}
	if err := drain(); err != nil {
		return out, err
	}
	if w == nil {
		return out, errors.New("sidecar-profile: fixture produced no rows")
	}
	stats, err := w.Close()
	w = nil
	if err != nil {
		return out, err
	}
	if stats.Rows != out.rows {
		return out, fmt.Errorf("sidecar-profile: fixture wrote %d rows, tracked %d", stats.Rows, out.rows)
	}
	if !stats.LabelsComplete {
		return out, errors.New("sidecar-profile: fixture lacks complete label summaries")
	}
	if err := f.Close(); err != nil {
		return out, err
	}
	copy(out.sha[:], h.Sum(nil))
	return out, nil
}

// sidecarDescribeFixture scans the file once (setup) for the actual row count,
// raw line bytes and line SHA-256, and stats its compressed size.
func sidecarDescribeFixture(ctx context.Context, path string) (sidecarFixture, error) {
	fx := sidecarFixture{path: path}
	f, err := os.Open(path)
	if err != nil {
		return fx, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fx, err
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return fx, err
	}
	h := sha256.New()
	lineIdx := -1
	for rec, rerr := range rd.Records(ctx) {
		if rerr != nil {
			_ = rd.Close()
			_ = f.Close()
			return fx, rerr
		}
		if lineIdx < 0 {
			idx := rec.Schema().FieldIndices(schema.ColLine)
			if len(idx) != 1 {
				_ = rd.Close()
				_ = f.Close()
				return fx, fmt.Errorf("fixture has %d %q columns, want 1", len(idx), schema.ColLine)
			}
			lineIdx = idx[0]
		}
		line, ok := rec.Column(lineIdx).(*array.String)
		if !ok {
			_ = rd.Close()
			_ = f.Close()
			return fx, fmt.Errorf("fixture line column has type %T", rec.Column(lineIdx))
		}
		tsCol, ok := rec.Column(0).(*array.Timestamp)
		if !ok {
			_ = rd.Close()
			_ = f.Close()
			return fx, fmt.Errorf("fixture timestamp column has type %T", rec.Column(0))
		}
		if vals := tsCol.TimestampValues(); len(vals) > 0 {
			if fx.first.IsZero() {
				fx.first = schema.Time(int64(vals[0]))
			}
			fx.last = schema.Time(int64(vals[len(vals)-1]))
		}
		for i := range int(rec.NumRows()) {
			if line.IsNull(i) {
				continue
			}
			v := line.Value(i)
			_, _ = io.WriteString(h, v)
			_, _ = io.WriteString(h, "\n")
			fx.rows++
			fx.raw += int64(len(v))
		}
	}
	if err := errors.Join(rd.Close(), f.Close()); err != nil {
		return fx, err
	}
	fx.parquet = fi.Size()
	copy(fx.lineSHA[:], h.Sum(nil))
	return fx, nil
}

// sidecarProductionBaseline runs the real production builder once, unprofiled.
func sidecarProductionBaseline(ctx context.Context, path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	data, buildErr := storage.BuildSidecar(ctx, rd, index.DefaultParams)
	closeErr := errors.Join(rd.Close(), f.Close())
	if buildErr != nil {
		if errors.Is(buildErr, index.ErrTooManyGrams) {
			return nil, fmt.Errorf("%w (fixture exceeds the production gram cap; lower PARKANA_A5_SIDE_RAW_BYTES)", buildErr)
		}
		return nil, buildErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) == 0 {
		return nil, errors.New("production builder returned no bytes")
	}
	if _, err := index.Decode(data); err != nil {
		return nil, fmt.Errorf("decode production sidecar: %w", err)
	}
	return data, nil
}

// sidecarCopyFile copies src to dst for the second concurrent object.
func sidecarCopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	return errors.Join(copyErr, closeErr)
}

// sidecarBuildStat is one instrumented build's per-stage wall and allocation
// identity. Stage durations are one job's wall, never CPU time.
type sidecarBuildStat struct {
	path         string
	wall         time.Duration
	read         time.Duration
	extract      time.Duration
	add          time.Duration
	encode       time.Duration
	rows         int64
	raw          int64
	grams        int
	sidecarBytes int
	// processAllocDelta is the process-wide runtime.MemStats TotalAlloc delta
	// over this build's wall window. Concurrent builds and ingest overlap, so
	// it is not a per-job allocation count and must never be summed or
	// compared as one.
	processAllocDelta uint64
}

// sidecarInstrumentedBuild is production BuildSidecar with per-batch stage
// timers and pprof stage labels. It keeps the full all-column Reader.Records
// decode: unused labels/meta are deliberately not projected out.
func sidecarInstrumentedBuild(ctx context.Context, path string) (sidecarBuildStat, []byte, error) {
	var st sidecarBuildStat
	st.path = path
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()

	f, err := os.Open(path)
	if err != nil {
		return st, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return st, nil, err
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return st, nil, err
	}
	b, err := index.NewBuilder(index.DefaultParams)
	if err != nil {
		_ = rd.Close()
		_ = f.Close()
		return st, nil, err
	}
	// Label switches happen once per batch, never per line or gram. The read
	// label is left in place across the iterator's next() call so decode
	// samples are attributed to the Parquet read, not to Add.
	readCtx := pprof.WithLabels(context.Background(), pprof.Labels("sidecar_stage", "read"))
	extractCtx := pprof.WithLabels(context.Background(), pprof.Labels("sidecar_stage", "extract"))
	addCtx := pprof.WithLabels(context.Background(), pprof.Labels("sidecar_stage", "add"))
	encodeCtx := pprof.WithLabels(context.Background(), pprof.Labels("sidecar_stage", "encode"))
	defer pprof.SetGoroutineLabels(context.Background())

	lineIdx := -1
	var lines []string
	last := time.Now()
	pprof.SetGoroutineLabels(readCtx)
	for rec, rerr := range rd.Records(ctx) {
		if rerr != nil {
			_ = rd.Close()
			_ = f.Close()
			return st, nil, rerr
		}
		now := time.Now()
		st.read += now.Sub(last)

		pprof.SetGoroutineLabels(extractCtx)
		extractStart := time.Now()
		if lineIdx < 0 {
			idx := rec.Schema().FieldIndices(schema.ColLine)
			if len(idx) != 1 {
				_ = rd.Close()
				_ = f.Close()
				return st, nil, fmt.Errorf("build: %d %q columns, want 1", len(idx), schema.ColLine)
			}
			lineIdx = idx[0]
		}
		line, ok := rec.Column(lineIdx).(*array.String)
		if !ok {
			_ = rd.Close()
			_ = f.Close()
			return st, nil, fmt.Errorf("build: line column has type %T", rec.Column(lineIdx))
		}
		n := int(rec.NumRows())
		if cap(lines) < n {
			lines = make([]string, n)
		} else {
			lines = lines[:n]
		}
		for i := range n {
			if line.IsNull(i) {
				lines[i] = ""
				continue
			}
			v := line.Value(i)
			lines[i] = v
			st.rows++
			st.raw += int64(len(v))
		}
		st.extract += time.Since(extractStart)

		pprof.SetGoroutineLabels(addCtx)
		addStart := time.Now()
		for i := range n {
			if !line.IsNull(i) {
				b.Add(lines[i])
			}
		}
		st.add += time.Since(addStart)
		// Drop the copied string headers so this batch's Arrow buffers are
		// not retained through the next read or Encode. Add already consumed
		// them, so clearing cannot change the encoded bytes.
		clear(lines[:cap(lines)])
		pprof.SetGoroutineLabels(readCtx)
		last = time.Now()
	}
	st.read += time.Since(last)

	pprof.SetGoroutineLabels(encodeCtx)
	encodeStart := time.Now()
	encoded, err := b.Encode()
	st.encode = time.Since(encodeStart)
	pprof.SetGoroutineLabels(context.Background())
	if err != nil {
		_ = rd.Close()
		_ = f.Close()
		return st, nil, err
	}
	st.wall = time.Since(start)
	st.grams = b.Grams()
	st.sidecarBytes = len(encoded)
	if err := errors.Join(rd.Close(), f.Close()); err != nil {
		return st, nil, err
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	st.processAllocDelta = after.TotalAlloc - before.TotalAlloc
	return st, encoded, nil
}

// sidecarRequireEquivalence proves the instrumented build is byte-identical to
// the production builder baseline for the same file.
func sidecarRequireEquivalence(stat sidecarBuildStat, encoded []byte, fx sidecarFixture) error {
	if !bytes.Equal(encoded, fx.baseline) {
		sum := sha256.Sum256(encoded)
		return fmt.Errorf("sidecar-profile: instrumented build differs from production baseline: bytes=%d sha256=%x want bytes=%d sha256=%x",
			len(encoded), sum, len(fx.baseline), fx.baselineSHA)
	}
	if stat.rows != fx.rows || stat.raw != fx.raw {
		return fmt.Errorf("sidecar-profile: instrumented build saw rows=%d raw=%d, fixture has rows=%d raw=%d",
			stat.rows, stat.raw, fx.rows, fx.raw)
	}
	return nil
}

// sidecarCaseResult is one case's concurrent wall, per-job stats and (for the
// ingest case) the real ingest workload's outcomes. caseAllocDelta is the
// process-wide TotalAlloc delta over the case window (all goroutines,
// including ingest), not an allocation count for one build.
type sidecarCaseResult struct {
	wall           time.Duration
	jobs           []sidecarBuildStat
	ingest         sidecarIngestStat
	cpuUser        float64
	cpuSys         float64
	caseAllocDelta uint64
	gcCycles       uint64
}

// sidecarRunProfiledCase runs one one-shot case inside the CPU/heap window.
func sidecarRunProfiledCase(b *testing.B, dir string, run func(context.Context, func()) (sidecarCaseResult, error)) sidecarCaseResult {
	b.Helper()
	stem := a5Sanitize(b.Name())
	var (
		res     sidecarCaseResult
		ran     bool
		err     error
		cpuPath string
	)
	if dir != "" {
		cpuPath = filepath.Join(dir, stem+".cpu.pprof")
	}
	for b.Loop() {
		if ran {
			b.Fatal("sidecar-profile: cases are one-shot: run with -benchtime=1x -count=1")
		}
		ran = true
		// GC and the base allocs snapshot happen outside the CPU profile and
		// before timed work, so the after-base difference is the case's
		// allocation picture and the CPU profile has no setup samples. Heap
		// profiles do not carry pprof labels; stage attribution is by call
		// stack.
		if dir != "" {
			runtime.GC()
			sidecarWriteAllocs(b, filepath.Join(dir, stem+".heap-base.pprof"))
		}
		stopCPU := sidecarStartCPU(b, cpuPath)
		res, err = run(b.Context(), stopCPU)
		stopCPU() // safety for an early error; the run stops it at its endpoint
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if dir != "" {
			// GC after the work and ingest join, outside the measured CPU,
			// wall and profile windows, so the final inuse view is settled
			// post-GC/post-release rather than a last-GC lagging peak. The
			// allocs base -> after difference can include the ingest work
			// that was in flight when the build endpoint cancelled it, plus
			// profiler/snapshot bookkeeping; the reported process CPU and
			// TotalAlloc deltas still end at the builds.
			runtime.GC()
			sidecarWriteAllocs(b, filepath.Join(dir, stem+".heap.pprof"))
			b.Logf("sidecar-profile: profiles cpu=%s heap=%s heap-base=%s (allocs profile after GC; final inuse is post-GC/post-release, not peak; alloc delta includes small profiler/snapshot bookkeeping; heap profiles do not preserve pprof labels, so use -sample_index=alloc_space -base=%s %s and allocation call stacks; CPU stage labels are sidecar_stage=read|extract|add|encode)",
				cpuPath, filepath.Join(dir, stem+".heap.pprof"), filepath.Join(dir, stem+".heap-base.pprof"),
				filepath.Join(dir, stem+".heap-base.pprof"), filepath.Join(dir, stem+".heap.pprof"))
		}
		// B.Loop requires the timer to be running when it resumes; the profile
		// writes above stay outside the timed window.
		b.StartTimer()
	}
	return res
}

// sidecarStartCPU starts a case-scoped CPU profile, or returns a no-op.
func sidecarStartCPU(b *testing.B, path string) func() {
	b.Helper()
	if path == "" {
		return func() {}
	}
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("sidecar-profile: create cpu profile %s: %v", path, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		b.Fatalf("sidecar-profile: start cpu profile %s: %v", path, err)
	}
	stop := sync.OnceFunc(func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	})
	b.Cleanup(stop)
	return stop
}

// sidecarWriteAllocs writes the allocs profile (alloc_space + inuse_space).
func sidecarWriteAllocs(b *testing.B, path string) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("sidecar-profile: create heap profile %s: %v", path, err)
	}
	p := pprof.Lookup("allocs")
	if p == nil {
		_ = f.Close()
		b.Fatalf("sidecar-profile: allocs profile unavailable")
	}
	writeErr := p.WriteTo(f, 0)
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		b.Fatalf("sidecar-profile: write heap profile %s: %v", path, err)
	}
}

// sidecarProfileDir returns the profile output directory, creating it.
func sidecarProfileDir(b *testing.B) string {
	b.Helper()
	dir := os.Getenv("PARKANA_A5_SIDE_PROFILE_DIR")
	if dir == "" {
		return ""
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("sidecar-profile: create profile dir %s: %v", dir, err)
	}
	return dir
}

// --- real bounded ingest workload -------------------------------------------

// sidecarPushBody is one pre-encoded push request.
type sidecarPushBody struct {
	data  []byte
	raw   int64
	lines int64
}

// sidecarPushSet is one v3 label set's request bodies.
type sidecarPushSet struct {
	labels logs.Labels
	bodies []sidecarPushBody
}

// sidecarPushPool is a bounded pre-encoded v3 ingest corpus.
type sidecarPushPool struct {
	sets     []sidecarPushSet
	raw      int64
	lines    int64
	requests int64
}

// sidecarBuildPushPool renders v3 records in parkbench cohorts and pre-encodes
// one request per label set, so corpus generation and JSON encoding stay out of
// the profile window.
func sidecarBuildPushPool(b *testing.B, ctx context.Context, target int64) *sidecarPushPool {
	b.Helper()
	labels := make([]logs.Labels, 0, len(a5Streams)*sidecarIngestInstances)
	slotOf := make(map[[2]int]int, cap(labels))
	for service := range a5Streams {
		for instance := range sidecarIngestInstances {
			slotOf[[2]int{service, instance}] = len(labels)
			labels = append(labels, logs.New(
				logs.Label{Name: "service_name", Value: a5Streams[service]},
				logs.Label{Name: "instance", Value: strconv.Itoa(instance)},
			))
		}
	}
	pool := &sidecarPushPool{sets: make([]sidecarPushSet, len(labels))}
	for i := range labels {
		pool.sets[i].labels = labels[i]
	}
	var (
		batches = make([][]logs.Entry, len(labels))
		raw     int64
		cohort  int64
	)
	flushCohort := func() error {
		for i := range batches {
			if len(batches[i]) == 0 {
				continue
			}
			entries := batches[i]
			batches[i] = entries[:0]
			body, err := pushcodec.MarshalJSON(ctx, &logs.PushRequest{
				Streams: []logs.Stream{{Labels: pool.sets[i].labels, Entries: entries}},
			})
			if err != nil {
				return err
			}
			bodyRaw := rawOf(entries)
			pool.sets[i].bodies = append(pool.sets[i].bodies, sidecarPushBody{
				data:  body,
				raw:   bodyRaw,
				lines: int64(len(entries)),
			})
			pool.raw += bodyRaw
			pool.lines += int64(len(entries))
			pool.requests++
		}
		return nil
	}
	for i := int64(0); ; i++ {
		line, service, ts, _, err := sidecarV3Line(i)
		if err != nil {
			b.Fatal(err)
		}
		instance := sidecarInstanceFor(i, len(a5Streams), sidecarIngestInstances)
		slot := slotOf[[2]int{service, instance}]
		batches[slot] = append(batches[slot], logs.Entry{Timestamp: ts, Line: line})
		raw += int64(len(line))
		cohort++
		if cohort == sidecarCohortGlobal {
			cohort = 0
			if err := flushCohort(); err != nil {
				b.Fatalf("sidecar-profile: encode push pool: %v", err)
			}
			if raw >= target {
				break
			}
		}
	}
	if cohort > 0 {
		if err := flushCohort(); err != nil {
			b.Fatalf("sidecar-profile: encode push pool: %v", err)
		}
	}
	b.Logf("sidecar-profile: ingest pool label-sets=%d requests=%d lines=%d raw=%d target=%d (pre-encoded before the profile window)",
		len(pool.sets), pool.requests, pool.lines, pool.raw, target)
	return pool
}

// sidecarPostLoop posts the pre-encoded pool with one sequential sender per
// label set until ctx ends or cap bytes have been acknowledged. Repeating the
// bounded pool is deliberate: the workload must last as long as the builds,
// and the cap keeps it bounded. Acknowledged bytes can overshoot the cap by at
// most one in-flight cohort.
func sidecarPostLoop(ctx context.Context, client *http.Client, url string, pool *sidecarPushPool, capBytes int64) (sidecarIngestStat, error) {
	var (
		stat     sidecarIngestStat
		raw      atomic.Int64
		requests atomic.Int64
		lines    atomic.Int64
		firstErr error
		mu       sync.Mutex
		wg       sync.WaitGroup
	)
	start := time.Now()
	for si := range pool.sets {
		wg.Go(func() {
			set := &pool.sets[si]
			for {
				if ctx.Err() != nil || raw.Load() >= capBytes {
					return
				}
				for _, body := range set.bodies {
					if ctx.Err() != nil || raw.Load() >= capBytes {
						return
					}
					if err := a5PostOnce(ctx, client, url, body.data); err != nil {
						if ctx.Err() != nil {
							return
						}
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
						return
					}
					raw.Add(body.raw)
					requests.Add(1)
					lines.Add(body.lines)
				}
			}
		})
	}
	wg.Wait()
	stat.wall = time.Since(start)
	stat.raw = raw.Load()
	stat.requests = requests.Load()
	stat.lines = lines.Load()
	stat.capHit = stat.raw >= capBytes
	mu.Lock()
	err := firstErr
	mu.Unlock()
	return stat, err
}

// sidecarIngestStat is the acknowledged ingest workload.
type sidecarIngestStat struct {
	wall     time.Duration
	raw      int64
	lines    int64
	requests int64
	capHit   bool
}

// sidecarRunConcurrent starts the real ingest workload and two instrumented
// builds together, waits for both builds, scripts the build-window endpoints,
// stops the CPU profile at that same endpoint, then cancels and joins ingest.
func sidecarRunConcurrent(ctx context.Context, fx sidecarFixture, secondPath string, pool *sidecarPushPool, url string, capBytes int64, stopCPU func()) (sidecarCaseResult, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type ingestDone struct {
		stat sidecarIngestStat
		err  error
	}
	ingestCh := make(chan ingestDone, 1)
	client := &http.Client{Timeout: 120 * time.Second}
	// Endpoints are taken around the build window, before the ingest goroutine
	// is joined and before the CPU profile is stopped.
	start := time.Now()
	cpuBefore := a5ReadCPU()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	go func() {
		stat, err := sidecarPostLoop(runCtx, client, url, pool, capBytes)
		ingestCh <- ingestDone{stat: stat, err: err}
	}()

	paths := [2]string{fx.path, secondPath}
	var (
		stats [2]sidecarBuildStat
		encs  [2][]byte
		errs  [2]error
		wg    sync.WaitGroup
	)
	for i := range paths {
		wg.Go(func() {
			stats[i], encs[i], errs[i] = sidecarInstrumentedBuild(runCtx, paths[i])
		})
	}
	wg.Wait()
	wall := time.Since(start)
	cpuAfter := a5ReadCPU()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	// Cancel the ingest workload immediately after the build-endpoint
	// snapshots, before the CPU profiler drain, so draining cannot admit more
	// work past the measured window. The workload is joined below, before the
	// final GC and allocs snapshot: the heap allocs diff (base -> after) can
	// still include that in-flight drain and profiler/snapshot bookkeeping,
	// while the reported process CPU and TotalAlloc deltas end at the builds.
	cancel()
	stopCPU()

	var ingest ingestDone
	select {
	case ingest = <-ingestCh:
	case <-time.After(30 * time.Second):
		return sidecarCaseResult{}, errors.New("sidecar-profile: ingest workload did not stop within 30s")
	}
	if ingest.err != nil {
		return sidecarCaseResult{}, ingest.err
	}
	for i := range stats {
		if errs[i] != nil {
			return sidecarCaseResult{}, fmt.Errorf("sidecar-profile: concurrent build %d: %w", i, errs[i])
		}
		if err := sidecarRequireEquivalence(stats[i], encs[i], fx); err != nil {
			return sidecarCaseResult{}, fmt.Errorf("sidecar-profile: concurrent build %d: %w", i, err)
		}
	}
	return sidecarCaseResult{
		wall:           wall,
		jobs:           []sidecarBuildStat{stats[0], stats[1]},
		ingest:         ingest.stat,
		cpuUser:        cpuAfter.user - cpuBefore.user,
		cpuSys:         cpuAfter.sys - cpuBefore.sys,
		caseAllocDelta: memAfter.TotalAlloc - memBefore.TotalAlloc,
		gcCycles:       uint64(memAfter.NumGC - memBefore.NumGC),
	}, nil
}

// sidecarFlushOK sums the successful publish counter, so the caller can record
// concurrent flush activity before svc.stop separately from the shutdown drain.
func sidecarFlushOK(b *testing.B, reg *prometheus.Registry) float64 {
	b.Helper()
	families, err := reg.Gather()
	if err != nil {
		b.Fatalf("sidecar-profile: gather flush metrics: %v", err)
	}
	var ok float64
	for _, mf := range families {
		if mf.GetName() != "parkana_ingest_flush_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "result" && label.GetValue() == "ok" {
					ok += m.GetCounter().GetValue()
				}
			}
		}
	}
	return ok
}

// sidecarLocalParquet counts the ingest service's committed local Parquet
// files and bytes after shutdown.
func sidecarLocalParquet(dir string) (files int, bytes int64) {
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}
		if info, statErr := d.Info(); statErr == nil {
			files++
			bytes += info.Size()
		}
		return nil
	})
	return files, bytes
}

// --- real ingest service ----------------------------------------------------

// sidecarPushService is a running real ingest service.
type sidecarPushService struct {
	reg     *prometheus.Registry
	dataDir string
	url     string
	termCh  chan struct{}
	termErr error
	runErr  error
	stop    func()
}

// sidecarNewPushService starts the real ingester with a real WAL, memtable and
// local-Parquet flusher over the bench-stack ingest settings. The uploader is
// left at its production implementation but its interval is one hour, so no
// upload pass runs in a profiling case.
func sidecarNewPushService(b *testing.B) *sidecarPushService {
	b.Helper()
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.New(server.Options{Logger: logger, Registry: reg})
	dataDir := filepath.Join(b.TempDir(), "ingest-data")
	store, err := objstore.NewFS(filepath.Join(b.TempDir(), "ingest-objects"))
	if err != nil {
		b.Fatalf("sidecar-profile: object store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })
	deps := app.Deps{
		Config: config.Config{
			DataDir:      dataDir,
			StreamLabels: []string{"service_name"},
			Ingest:       a5BenchIngest(),
		},
		Logger:  logger,
		Store:   store,
		Catalog: catalog.NewMemory(),
		HTTP:    srv,
		Metrics: reg,
	}
	s, err := newService(deps, serviceOptions{
		newFlusher: func(ls *LocalStore) (genFlusher, error) {
			return NewFlusher(FlushOptions{Store: ls, PromotedFor: a5PromotedFor})
		},
	})
	if err != nil {
		b.Fatalf("sidecar-profile: new ingest service: %v", err)
	}
	svc := &sidecarPushService{reg: reg, dataDir: dataDir, termCh: make(chan struct{})}
	runCtx, cancel := context.WithCancel(b.Context())
	go func() {
		svc.termErr = s.Run(runCtx)
		close(svc.termCh)
	}()
	svc.stop = sync.OnceFunc(func() {
		cancel()
		select {
		case <-svc.termCh:
			svc.runErr = svc.termErr
		case <-time.After(90 * time.Second):
			svc.runErr = errors.New("sidecar-profile: ingest service did not stop within 90s")
		}
	})
	b.Cleanup(svc.stop)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if s.ready(context.Background()) == nil {
			ts := httptest.NewServer(deps.HTTP.Handler())
			b.Cleanup(ts.Close)
			svc.url = ts.URL + a5PushPath()
			return svc
		}
		select {
		case <-svc.termCh:
			b.Fatalf("sidecar-profile: ingest service stopped during startup: %v", svc.termErr)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Fatal("sidecar-profile: ingest service never became ready")
	return svc
}

// --- reporting --------------------------------------------------------------

// sidecarReportCase logs and reports one case. Stage totals are summed across
// jobs; they are wall durations, not CPU shares.
func sidecarReportCase(b *testing.B, name string, res sidecarCaseResult) {
	b.Helper()
	var (
		buildWall, read, extract, add, encode time.Duration
		raw                                   int64
		sidecarBytes                          int
	)
	for _, job := range res.jobs {
		buildWall += job.wall
		read += job.read
		extract += job.extract
		add += job.add
		encode += job.encode
		raw += job.raw
		sidecarBytes += job.sidecarBytes
		b.Logf("sidecar-profile: case=%s job=%s wall=%s read=%s (%.1f MB/s) extract=%s (%.1f MB/s) add=%s (%.1f MB/s) encode=%s rows=%d grams=%d sidecar=%dB process-alloc-delta=%.2fMiB (process-wide window delta, not per-job allocation)",
			name, filepath.Base(job.path), job.wall, job.read, sidecarMBps(job.raw, job.read),
			job.extract, sidecarMBps(job.raw, job.extract), job.add, sidecarMBps(job.raw, job.add),
			job.encode, job.rows, job.grams, job.sidecarBytes, float64(job.processAllocDelta)/(1<<20))
	}
	// One compact line with the summed stage walls next to the concurrent
	// case wall; the two are different concepts and are never subtracted.
	b.Logf("sidecar-profile: case=%s jobs=%d case-wall=%s build-wall-sum=%s read-sum=%s extract-sum=%s add-sum=%s encode-sum=%s build-raw=%d (%.2f MiB) sidecar-bytes=%d (stage totals are summed across jobs; not CPU shares; per-job process-alloc deltas overlap and are not summed)",
		name, len(res.jobs), res.wall, buildWall, read, extract, add, encode, raw,
		float64(raw)/(1<<20), sidecarBytes)
	if res.ingest.requests > 0 {
		b.Logf("sidecar-profile: case=%s ingest requests=%d lines=%d raw=%d (%.2f MiB) active=%s (%.1f MB/s) cap-hit=%t (real push->WAL->memtable->local flush; pre-encoded pool setup excluded)",
			name, res.ingest.requests, res.ingest.lines, res.ingest.raw,
			float64(res.ingest.raw)/(1<<20), res.ingest.wall, sidecarMBps(res.ingest.raw, res.ingest.wall),
			res.ingest.capHit)
	}
	cpuPct := 0.0
	if res.wall > 0 {
		cpuPct = (res.cpuUser + res.cpuSys) / res.wall.Seconds() * 100
	}
	scope := ""
	if res.ingest.requests > 0 {
		scope = ", includes ingest"
	}
	b.Logf("sidecar-profile: case=%s cpu-user=%.3fs cpu-sys=%.3fs cpu-pct=%.1f case-wall=%s case-process-alloc=%.2fMiB gc=%d (cpu-pct and case-process-alloc are process-wide over the case wall%s)",
		name, res.cpuUser, res.cpuSys, cpuPct, res.wall, float64(res.caseAllocDelta)/(1<<20), res.gcCycles, scope)

	b.ReportMetric(res.wall.Seconds(), "case-wall-seconds")
	b.ReportMetric(buildWall.Seconds(), "build-wall-seconds")
	b.ReportMetric(read.Seconds(), "read-seconds")
	b.ReportMetric(extract.Seconds(), "extract-seconds")
	b.ReportMetric(add.Seconds(), "add-seconds")
	b.ReportMetric(encode.Seconds(), "encode-seconds")
	b.ReportMetric(sidecarMBps(raw, buildWall), "build-MBps")
	b.ReportMetric(sidecarMBps(raw, read), "read-MBps")
	b.ReportMetric(sidecarMBps(raw, extract), "extract-MBps")
	b.ReportMetric(sidecarMBps(raw, add), "add-MBps")
	b.ReportMetric(float64(raw)/(1<<20), "build-raw-MiB")
	b.ReportMetric(float64(sidecarBytes)/1024, "sidecar-KiB")
	b.ReportMetric(cpuPct, "cpu-pct")
	b.ReportMetric(float64(res.caseAllocDelta)/(1<<20), "case-process-alloc-MiB")
	b.ReportMetric(float64(res.gcCycles), "gc-cycles")
	if res.ingest.requests > 0 {
		b.ReportMetric(float64(res.ingest.raw)/(1<<20), "ingest-raw-MiB")
		b.ReportMetric(sidecarMBps(res.ingest.raw, res.ingest.wall), "ingest-MBps")
	}
}

// sidecarMBps returns raw bytes per second of wall duration in MB (decimal).
func sidecarMBps(raw int64, d time.Duration) float64 {
	if d <= 0 || raw <= 0 {
		return 0
	}
	return float64(raw) / 1e6 / d.Seconds()
}
