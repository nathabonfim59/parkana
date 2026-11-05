package ingest

// Baseline stage profile for the phase 4 ingest runtime (plan M1, package
// A1-profile). BenchmarkIngestRuntimeStageProfile measures the flush and
// upload pipeline on a bounded MULTI-PARTITION workload at configurable
// worker counts, so the dominant stages are visible and the same workload
// can be re-run to compare serial and parallel profiles after each A3/A4
// change.
//
// It keeps the original A5 bench semantics: heavy benchmarks stay behind
// PARKANA_A5_BENCH, the corpus replica is the pinned parkbench fixture, setup
// (corpus generation, admission, store construction) stays outside the timed
// window, and the CPU profile is benchmark-scoped and optional.
//
// What this adds next to BenchmarkA5UploadPipeline:
//
//   - Four streams in one UTC hour instead of one stream, so one RunOnce
//     publishes four stream/hour partitions through four merges, four
//     sidecar builds, four data Puts, four sidecar Puts and four catalog
//     adds, driven by the configured flush and upload worker counts. The
//     api-only A5 benchmark cannot show multi-partition cost.
//   - Stage split for the pipeline: generation Parts materialization
//     versus the rest of Flush, plus the real merge, Put, sidecar-build and
//     catalog timings. Flush internals beyond Parts are not split, keeping the
//     instrumentation small; the dominance question is answered by comparing
//     flush, merge, sidecar build and Put totals.
//   - Backlog before and after flush and after upload: local files and bytes,
//     upload receipts, groups, due groups and selected sources.
//   - Peak Go heap, TotalAlloc, GC cycles, peak goroutines, sampled RSS for the
//     timed window, whole-process VmHWM, OS thread peak and process CPU
//     user+system seconds.
//   - One identity line with source commit (PARKANA_A5_SOURCE_COMMIT or build
//     VCS settings with an honest fallback), Go, OS/arch, NumCPU, GOMAXPROCS,
//     stage settings, local fsync scope and the excluded WAL.
//
// Workload bounds: PARKANA_A5_RUNTIME_BYTES caps raw line bytes (default
// 128 MiB) and PARKANA_A5_RUNTIME_SEAL caps the memtable seal size (default
// 16 MiB). The generation and file counts that fall out of those bounds are
// reported by the run (generations= in the workload line, flushes=/parts=
// and per-boundary backlog lines), not assumed here. One run flushes
// serially (PARKANA_A5_FLUSH_CONCURRENCY=1, the baseline); set it 1..64 to
// compare against parallel part writers. One RunOnce uploads serially
// (PARKANA_A5_UPLOAD_CONCURRENCY=1, the baseline); set it 1..64 to compare
// against parallel partition workers. It is one-shot: run with
// -benchtime=1x. The object store is the FS implementation and the catalog is
// in-memory, so Put/catalog wall times are local CPU/disk diagnostics, not S3
// latency. WAL admission and fsync are covered by BenchmarkA5PushJSON; this
// profile starts after admission, uses no WAL and excludes reclaim.

import (
	"cmp"
	"context"
	"io"
	"maps"
	"os"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

const (
	// a5RuntimeInterval is the runtime sampler period for peak RSS, heap,
	// goroutine and thread observations.
	a5RuntimeInterval = 20 * time.Millisecond
	// a5RuntimeBatchLines matches parkbench's 1250 entries per request per
	// label set.
	a5RuntimeBatchLines = 1250
)

// --- stage timing -------------------------------------------------------------

// a5StageCollector records benchmark-only stage durations and part counts.
type a5StageCollector struct {
	mu     sync.Mutex
	stages map[string][]time.Duration
	parts  int
}

// newA5StageCollector returns an empty collector.
func newA5StageCollector() *a5StageCollector {
	return &a5StageCollector{stages: make(map[string][]time.Duration)}
}

// observe records one call of stage spanning start through end.
func (c *a5StageCollector) observe(stage string, start, end time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stages[stage] = append(c.stages[stage], end.Sub(start))
}

// addParts adds one generation's materialized part count.
func (c *a5StageCollector) addParts(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parts += n
}

// partsTotal returns the materialized part count.
func (c *a5StageCollector) partsTotal() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.parts
}

// durations returns a copy of one stage's call durations.
func (c *a5StageCollector) durations(stage string) []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.stages[stage])
}

// report logs and reports one stage's wall total.
func (c *a5StageCollector) report(b *testing.B, stage string) {
	b.Helper()
	ds := c.durations(stage)
	if len(ds) == 0 {
		return
	}
	total := durationsSum(ds)
	var maxD time.Duration
	for _, d := range ds {
		maxD = max(maxD, d)
	}
	b.Logf("a5-runtime: %s calls=%d total=%s mean=%s max=%s",
		stage, len(ds), total, total/time.Duration(len(ds)), maxD)
	b.ReportMetric(total.Seconds(), stage+"-seconds")
}

// a5TimedGeneration times parts materialization around the real generation.
type a5TimedGeneration struct {
	inner *Generation
	c     *a5StageCollector
}

// Tenant implements SealedGeneration.
func (g *a5TimedGeneration) Tenant() tenant.ID { return g.inner.Tenant() }

// Frontier implements SealedGeneration.
func (g *a5TimedGeneration) Frontier() (first, last uint64) { return g.inner.Frontier() }

// Parts implements SealedGeneration.
func (g *a5TimedGeneration) Parts(ctx context.Context) ([]Part, error) {
	start := time.Now()
	parts, err := g.inner.Parts(ctx)
	g.c.observe("generation-parts", start, time.Now())
	if err == nil {
		g.c.addParts(len(parts))
	}
	return parts, err
}

// --- runtime sampler ----------------------------------------------------------

// a5RuntimeSample is the peak view of one timed pass.
type a5RuntimeSample struct {
	PeakHeapBytes  uint64
	PeakGoroutines uint64
	PeakRSSBytes   uint64
	PeakThreads    uint64
	HWM            uint64
}

// a5RuntimePeaks accumulates maxima; atomics keep the sampler lock-free.
type a5RuntimePeaks struct {
	heap       atomic.Uint64
	goroutines atomic.Uint64
	rss        atomic.Uint64
	threads    atomic.Uint64
}

// a5AtomicMax stores n in v when it exceeds the current value.
func a5AtomicMax(v *atomic.Uint64, n uint64) {
	for {
		old := v.Load()
		if n <= old || v.CompareAndSwap(old, n) {
			return
		}
	}
}

// a5ProcStatus reads RSS, peak RSS and thread count from /proc/self/status.
// It returns zeros where that file is unavailable; the sampled Go metrics
// still provide heap and goroutine peaks there.
func a5ProcStatus() (rss, hwm, threads uint64) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "VmRSS":
			rss = n << 10
		case "VmHWM":
			hwm = n << 10
		case "Threads":
			threads = n
		}
	}
	return rss, hwm, threads
}

// a5StartRuntimeSampler samples peak heap, goroutines, RSS and threads until
// stop is called. The returned stop is idempotent and also registered with
// b.Cleanup, so a b.Fatal path cannot leave the sampler running.
func a5StartRuntimeSampler(b *testing.B) func() a5RuntimeSample {
	b.Helper()
	var peaks a5RuntimePeaks
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/sched/goroutines:goroutines"},
	}
	update := func() {
		runtimemetrics.Read(samples)
		if samples[0].Value.Kind() == runtimemetrics.KindUint64 {
			a5AtomicMax(&peaks.heap, samples[0].Value.Uint64())
		}
		if samples[1].Value.Kind() == runtimemetrics.KindUint64 {
			a5AtomicMax(&peaks.goroutines, samples[1].Value.Uint64())
		}
		rss, _, threads := a5ProcStatus()
		a5AtomicMax(&peaks.rss, rss)
		a5AtomicMax(&peaks.threads, threads)
	}
	update()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(a5RuntimeInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				update()
			}
		}
	})
	stopOnce := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
		update()
	})
	stop := func() a5RuntimeSample {
		stopOnce()
		_, hwm, _ := a5ProcStatus()
		return a5RuntimeSample{
			PeakHeapBytes:  peaks.heap.Load(),
			PeakGoroutines: peaks.goroutines.Load(),
			PeakRSSBytes:   peaks.rss.Load(),
			PeakThreads:    peaks.threads.Load(),
			HWM:            hwm,
		}
	}
	b.Cleanup(func() { _ = stop() })
	return stop
}

// a5CPUTime is the process's cumulative user and system CPU time.
type a5CPUTime struct {
	user, sys float64
}

// a5ReadCPU reads process CPU time with getrusage, so utilization never
// includes idle processor capacity the way runtime/metrics totals do.
func a5ReadCPU() a5CPUTime {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return a5CPUTime{}
	}
	return a5CPUTime{
		user: float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6,
		sys:  float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6,
	}
}

// --- backlog ------------------------------------------------------------------

// a5Backlog is the queue state at one pipeline boundary.
type a5Backlog struct {
	Files        int
	FileBytes    int64
	Pending      int
	PendingBytes int64
	Receipts     int
	Groups       int
	DueGroups    int
	Sources      int
	SourceBytes  int64
}

// a5SnapshotBacklog reads the durable local state with the uploader's own
// selection helpers; it performs no reservations and no writes.
func a5SnapshotBacklog(b *testing.B, ctx context.Context, u *Uploader, ts *TenantStore) a5Backlog {
	b.Helper()
	var out a5Backlog
	files, err := ts.Files(ctx)
	if err != nil {
		b.Fatalf("a5-runtime: snapshot files: %v", err)
	}
	out.Files = len(files)
	for _, f := range files {
		out.FileBytes += f.Bytes
	}
	pending, err := ts.PendingFiles(ctx)
	if err != nil {
		b.Fatalf("a5-runtime: snapshot pending files: %v", err)
	}
	out.Pending = len(pending)
	for _, f := range pending {
		out.PendingBytes += f.Bytes
	}
	receipts, err := ts.Uploads(ctx)
	if err != nil {
		b.Fatalf("a5-runtime: snapshot receipts: %v", err)
	}
	out.Receipts = len(receipts)
	busy := make(map[uuid.UUID]bool)
	for _, up := range receipts {
		for _, src := range up.Sources {
			busy[src] = true
		}
	}
	groups := groupPendingFiles(pending, busy)
	out.Groups = len(groups)
	now := u.now()
	for _, key := range slices.SortedFunc(maps.Keys(groups), compareGroupKeys) {
		group := groups[key]
		if !u.due(group, now) {
			continue
		}
		out.DueGroups++
		chosen := u.selectSources(group)
		out.Sources += len(chosen)
		for _, f := range chosen {
			out.SourceBytes += f.Bytes
		}
	}
	return out
}

// a5LogBacklog logs one boundary in compact single-line form.
func a5LogBacklog(b *testing.B, boundary string, bl a5Backlog) {
	b.Helper()
	b.Logf("a5-runtime: backlog %s files=%d file-bytes=%d pending=%d pending-bytes=%d receipts=%d groups=%d due=%d sources=%d source-bytes=%d",
		boundary, bl.Files, bl.FileBytes, bl.Pending, bl.PendingBytes, bl.Receipts, bl.Groups, bl.DueGroups, bl.Sources, bl.SourceBytes)
}

// --- identity and metrics -----------------------------------------------------

// a5SourceIdentity resolves the source commit without running git on the VM:
// the lead supplies PARKANA_A5_SOURCE_COMMIT (and optionally _DIRTY), else the
// test binary's build VCS settings are used, else the value is honestly
// unavailable.
func a5SourceIdentity() (commit, dirty, origin string) {
	if v := os.Getenv("PARKANA_A5_SOURCE_COMMIT"); v != "" {
		return v, cmp.Or(os.Getenv("PARKANA_A5_SOURCE_DIRTY"), "unknown"), "env"
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		var revision, modified string
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.modified":
				modified = s.Value
			}
		}
		if revision != "" {
			return revision, cmp.Or(modified, "unknown"), "buildinfo"
		}
	}
	return "unavailable", "unknown", "unavailable"
}

// a5LogIdentity logs one compact line naming the exact code and settings
// under measurement.
func a5LogIdentity(b *testing.B) {
	b.Helper()
	commit, dirty, origin := a5SourceIdentity()
	b.Logf("a5-runtime: identity commit=%s dirty=%s origin=%s go=%s os=%s arch=%s numcpu=%d gomaxprocs=%d",
		commit, dirty, origin, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
}

// a5IndexBuildStats is the ingester's sidecar build evidence from the shared
// index registry.
type a5IndexBuildStats struct {
	Seconds float64
	Count   float64
	Built   float64
	Bytes   float64
}

// a5IngestIndexBuilds sums only the source="ingest" series of the index
// metrics. reg.Gather returns typed families, so no HTTP server or text parser
// is needed and a future extra label cannot silently undercount.
func a5IngestIndexBuilds(b *testing.B, reg *prometheus.Registry) a5IndexBuildStats {
	b.Helper()
	families, err := reg.Gather()
	if err != nil {
		b.Fatalf("a5-runtime: gather metrics: %v", err)
	}
	var out a5IndexBuildStats
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			var source string
			for _, label := range m.GetLabel() {
				if label.GetName() == "source" {
					source = label.GetValue()
				}
			}
			if source != string(index.SourceIngest) {
				continue
			}
			switch mf.GetName() {
			case "parkana_index_build_duration_seconds":
				hist := m.GetHistogram()
				out.Seconds += hist.GetSampleSum()
				out.Count += float64(hist.GetSampleCount())
			case "parkana_index_built_total":
				out.Built += m.GetCounter().GetValue()
			case "parkana_index_bytes_total":
				out.Bytes += m.GetCounter().GetValue()
			}
		}
	}
	return out
}

// --- benchmark ----------------------------------------------------------------

// BenchmarkIngestRuntimeStageProfile measures the flush and upload pipeline
// on a bounded multi-partition workload at the configured worker counts.
// Run with -benchtime=1x; see the package comment.
func BenchmarkIngestRuntimeStageProfile(b *testing.B) {
	if os.Getenv("PARKANA_A5_BENCH") == "" {
		b.Skip("a5-runtime: set PARKANA_A5_BENCH=1 (plus optional PARKANA_A5_RUNTIME_BYTES, PARKANA_A5_RUNTIME_SEAL, PARKANA_A5_FLUSH_CONCURRENCY=1..64, PARKANA_A5_UPLOAD_CONCURRENCY=1..64, PARKANA_A5_SOURCE_COMMIT, PARKANA_A5_CPUPROFILE=DIR) to run the heavy multi-partition runtime benchmark")
	}
	if err := a5VerifyReplica(); err != nil {
		b.Fatal(err)
	}
	target := a5EnvInt(b, "PARKANA_A5_RUNTIME_BYTES", 128<<20)
	seal := a5EnvInt(b, "PARKANA_A5_RUNTIME_SEAL", 16<<20)
	flushConc := a5EnvInt(b, "PARKANA_A5_FLUSH_CONCURRENCY", 1)
	if flushConc > maxFlushConcurrency {
		b.Fatalf("a5-runtime: PARKANA_A5_FLUSH_CONCURRENCY must be between 1 and %d, got %d", maxFlushConcurrency, flushConc)
	}
	uploadConc := a5EnvInt(b, "PARKANA_A5_UPLOAD_CONCURRENCY", 1)
	if uploadConc < 1 || uploadConc > uploadMaxConcurrency {
		b.Fatalf("a5-runtime: PARKANA_A5_UPLOAD_CONCURRENCY must be between 1 and %d, got %d", uploadMaxConcurrency, uploadConc)
	}
	ctx := b.Context()
	a5LogIdentity(b)

	// Setup (unprofiled): admit the pinned corpus across the four parkbench
	// streams, one 1250-entry request per stream per round, until the raw byte
	// bound is reached. Admission accounts WAL-encoded bytes exactly as the
	// service does, so the memtable seal threshold matches production.
	mem, err := NewMemtable(MemtableOptions{
		StreamLabels:     []string{"service_name"},
		PromotedFor:      a5PromotedFor,
		MemtableMaxBytes: seal,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer mem.Close()
	var (
		raw     int64
		lines   int64
		seq     uint64
		batches [len(a5Streams)][]logs.Entry
	)
	expected := make(map[uploadGroupKey]int64)
	admit := func(stream int) error {
		batch := batches[stream]
		if len(batch) == 0 {
			return nil
		}
		batches[stream] = batch[:0]
		req := &logs.PushRequest{Streams: []logs.Stream{{
			Labels:  logs.New(logs.Label{Name: "service_name", Value: a5Streams[stream]}),
			Entries: batch,
		}}}
		prepared, err := mem.Prepare(ctx, tenant.Default, req)
		if err != nil {
			return err
		}
		payload, err := wal.Encode(ctx, *req)
		if err != nil {
			return err
		}
		seq++
		return mem.AppendPrepared(ctx, tenant.Default, seq, int64(payload.Size()), prepared)
	}
	for i := int64(0); ; i++ {
		line, stream, ts, _, _ := a5Record(i)
		batches[stream] = append(batches[stream], logs.Entry{Timestamp: ts, Line: line})
		raw += int64(len(line))
		lines++
		expected[uploadGroupKey{stream: a5Streams[stream], hour: ts.UTC().Truncate(time.Hour)}]++
		if len(batches[stream]) == a5RuntimeBatchLines {
			if err := admit(stream); err != nil {
				b.Fatal(err)
			}
		}
		if raw >= target {
			break
		}
	}
	for stream := range batches {
		if err := admit(stream); err != nil {
			b.Fatal(err)
		}
	}
	if err := mem.Seal(ctx, tenant.Default); err != nil {
		b.Fatal(err)
	}
	gens, err := mem.TakeSealed(ctx, tenant.Default)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		for _, gen := range gens {
			if gen != nil {
				gen.Release()
			}
		}
	})

	// Real local publication, real uploader, real sidecar builder.
	collector := newA5StageCollector()
	ls, err := OpenLocal(ctx, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	ts, err := ls.Tenant(ctx, tenant.Default)
	if err != nil {
		b.Fatal(err)
	}
	realFlusher, err := NewFlusher(FlushOptions{
		Store:       ls,
		PromotedFor: a5PromotedFor,
		Concurrency: int(flushConc),
	})
	if err != nil {
		b.Fatal(err)
	}
	tFlush := &a5TimingFlusher{inner: realFlusher}
	reg := prometheus.NewRegistry()
	im, err := index.RegisterMetrics(reg)
	if err != nil {
		b.Fatal(err)
	}
	fsStore, err := objstore.NewFS(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { fsStore.Close() })
	tStore := &a5TimingStore{inner: fsStore}
	tCat := &a5TimingCatalog{Catalog: catalog.NewMemory()}
	up, err := NewUploader(UploaderOptions{
		Local:        ls,
		Objects:      tStore,
		Catalog:      tCat,
		PromotedFor:  a5PromotedFor,
		MinBytes:     1,
		MaxAge:       time.Hour,
		MergeMax:     2 << 30,
		Concurrency:  int(uploadConc),
		Index:        index.DefaultParams,
		IndexMetrics: im,
	})
	if err != nil {
		b.Fatal(err)
	}
	up.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		start := time.Now()
		stats, err := storage.Merge(ctx, w, inputs, promoted)
		collector.observe("merge", start, time.Now())
		return stats, err
	}

	partitions := len(expected)
	b.SetBytes(raw)
	b.ReportAllocs()
	b.Logf("a5-runtime: workload tenant=%s streams=%d partitions=%d generations=%d seal=%dB raw=%dB lines=%d request-lines=%d index=N%d/FP%g/cap%d upload-min=1 upload-max-age=1h merge-max=2GiB flush-workers=%d upload-workers=%d durability=local-fsync+object-publish wal=excluded(A5PushJSON covers admission/fsync) reclaim=excluded",
		tenant.Default, len(a5Streams), partitions, len(gens), seal, raw, lines, a5RuntimeBatchLines,
		index.DefaultParams.N, index.DefaultParams.FPRate, index.DefaultParams.MaxGrams, flushConc, uploadConc)

	before := a5SnapshotBacklog(b, ctx, up, ts)
	a5LogBacklog(b, "before-flush", before)

	stopSampler := a5StartRuntimeSampler(b)
	stopProfile := a5StartCPUProfile(b)
	cpuBefore := a5ReadCPU()
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	var afterFlush a5Backlog
	var report UploadReport
	flushed := false
	start := time.Now()
	for b.Loop() {
		if flushed {
			b.Fatal("a5-runtime: runtime pipeline is one-shot: run with -benchtime=1x (second iteration refused)")
		}
		for i, gen := range gens {
			if err := tFlush.Flush(ctx, &a5TimedGeneration{inner: gen, c: collector}); err != nil {
				b.Fatal(err)
			}
			gen.Release()
			gens[i] = nil
		}
		flushed = true
		afterFlush = a5SnapshotBacklog(b, ctx, up, ts)
		a5LogBacklog(b, "after-flush", afterFlush)
		uploadStart := time.Now()
		report, err = up.RunOnce(ctx)
		collector.observe("upload-pass", uploadStart, time.Now())
		if err != nil {
			b.Fatal(err)
		}
	}
	wall := time.Since(start)
	cpuAfter := a5ReadCPU()
	runtime.ReadMemStats(&memAfter)
	sample := stopSampler()
	// Stop the CPU profile only after the timed-window snapshots: stopping it
	// drains/writes the profile and must not pollute the CPU or memory window.
	stopProfile()
	afterUpload := a5SnapshotBacklog(b, ctx, up, ts)
	a5LogBacklog(b, "after-upload", afterUpload)

	// Exactly the expected partitions, each with its own rows and object.
	if report.Uploads != partitions || len(report.Completed) != partitions {
		b.Fatalf("a5-runtime: uploads=%d completed=%d, want %d partitions", report.Uploads, len(report.Completed), partitions)
	}
	got := make(map[uploadGroupKey]int64, len(report.Completed))
	for _, res := range report.Completed {
		got[uploadGroupKey{stream: res.Stream, hour: res.Hour.UTC()}] += res.Rows
	}
	if !maps.Equal(expected, got) {
		b.Fatalf("a5-runtime: published rows per partition=%v, want %v", got, expected)
	}
	var live int
	var rows int64
	for f, err := range tCat.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			b.Fatal(err)
		}
		live++
		rows += f.Rows
		if !f.HasSidecar || !f.LabelsComplete {
			b.Fatalf("a5-runtime: catalog row %s has sidecar=%v labels-complete=%v", f.Key, f.HasSidecar, f.LabelsComplete)
		}
	}
	if live != partitions || rows != lines {
		b.Fatalf("a5-runtime: catalog lists %d files with %d rows, want %d files with %d rows", live, rows, partitions, lines)
	}
	if afterUpload.Pending != 0 || afterUpload.Receipts != 0 || afterUpload.Files != 0 {
		b.Fatalf("a5-runtime: backlog not drained: %+v", afterUpload)
	}
	tStore.mu.Lock()
	dataPuts, sidecarPuts := len(tStore.dataPut), len(tStore.sidecarPut)
	dataBytes, sidecarBytes := tStore.dataBytes, tStore.sidecarBytes
	tStore.mu.Unlock()
	if dataPuts != partitions || sidecarPuts != partitions {
		b.Fatalf("a5-runtime: data Puts=%d sidecar Puts=%d, want %d each", dataPuts, sidecarPuts, partitions)
	}
	if parts := collector.partsTotal(); parts != afterFlush.Files {
		b.Fatalf("a5-runtime: materialized %d parts but flush published %d local files", parts, afterFlush.Files)
	}

	// Stage, memory, CPU and data-size evidence.
	tFlush.mu.Lock()
	flushes := slices.Clone(tFlush.flushes)
	tFlush.mu.Unlock()
	b.Logf("a5-runtime: flushes=%d parts=%d flush-workers=%d", len(flushes), collector.partsTotal(), flushConc)
	if n := a5LogDurations(b, "flush-total", flushes); n > 0 {
		b.ReportMetric(durationsSum(flushes).Seconds(), "flush-seconds")
	}
	collector.report(b, "generation-parts")
	collector.report(b, "merge")
	collector.report(b, "upload-pass")
	tStore.mu.Lock()
	dataPut, sidecarPut := slices.Clone(tStore.dataPut), slices.Clone(tStore.sidecarPut)
	tStore.mu.Unlock()
	if n := a5LogDurations(b, "data-put(FS)", dataPut); n > 0 {
		b.ReportMetric(durationsSum(dataPut).Seconds(), "data-put-seconds")
	}
	if n := a5LogDurations(b, "sidecar-put(FS)", sidecarPut); n > 0 {
		b.ReportMetric(durationsSum(sidecarPut).Seconds(), "sidecar-put-seconds")
	}
	tCat.mu.Lock()
	addFiles := slices.Clone(tCat.addFiles)
	tCat.mu.Unlock()
	if n := a5LogDurations(b, "catalog-addfiles(memory)", addFiles); n > 0 {
		b.ReportMetric(durationsSum(addFiles).Seconds(), "addfiles-seconds")
	}
	builds := a5IngestIndexBuilds(b, reg)
	if builds.Count < float64(partitions) || builds.Seconds <= 0 {
		b.Fatalf("a5-runtime: index build metrics sum=%g count=%g, want %d ingest builds", builds.Seconds, builds.Count, partitions)
	}
	b.ReportMetric(builds.Seconds, "sidecar-build-seconds")
	b.Logf("a5-runtime: sidecar builds count=%g total=%gs built-objects=%g index-bytes=%g",
		builds.Count, builds.Seconds, builds.Built, builds.Bytes)

	rssMB := float64(sample.PeakRSSBytes) / (1 << 20)
	hwmMB := float64(sample.HWM) / (1 << 20)
	heapMB := float64(sample.PeakHeapBytes) / (1 << 20)
	b.ReportMetric(rssMB, "peak-rss-MiB")
	b.ReportMetric(heapMB, "peak-heap-MiB")
	b.ReportMetric(hwmMB, "hwm-rss-MiB")
	b.ReportMetric(float64(sample.PeakGoroutines), "peak-goroutines")
	b.ReportMetric(float64(sample.PeakThreads), "peak-threads")
	cpuUser := cpuAfter.user - cpuBefore.user
	cpuSys := cpuAfter.sys - cpuBefore.sys
	cpuPct := 0.0
	if wall > 0 {
		cpuPct = (cpuUser + cpuSys) / wall.Seconds() * 100
	}
	b.ReportMetric(cpuPct, "cpu-pct")
	b.ReportMetric(wall.Seconds(), "timed-seconds")
	totalAlloc := memAfter.TotalAlloc - memBefore.TotalAlloc
	b.ReportMetric(float64(totalAlloc)/(1<<20), "total-alloc-MiB")
	b.ReportMetric(float64(memAfter.NumGC-memBefore.NumGC), "gc-cycles")
	b.Logf("a5-runtime: memory cpu-user=%.3fs cpu-sys=%.3fs cpu-pct=%.1f wall=%s timed-rss=%.1fMiB process-hwm-rss=%.1fMiB(incl setup) peak-heap=%.1fMiB peak-goroutines=%d peak-threads=%d total-alloc=%.1fMiB gc=%d",
		cpuUser, cpuSys, cpuPct, wall, rssMB, hwmMB, heapMB,
		sample.PeakGoroutines, sample.PeakThreads, float64(totalAlloc)/(1<<20), memAfter.NumGC-memBefore.NumGC)
	b.ReportMetric(float64(afterFlush.FileBytes)/(1<<20), "parquet-MiB")
	b.ReportMetric(float64(report.Bytes)/(1<<20), "object-MiB")
	b.ReportMetric(float64(sidecarBytes)/(1<<20), "sidecar-MiB")
	b.Logf("a5-runtime: sizes raw=%dB lines=%d local-parquet=%dB/%d files objects=%dB/%d sidecars=%dB/%d",
		raw, lines, afterFlush.FileBytes, afterFlush.Files, dataBytes, dataPuts, sidecarBytes, sidecarPuts)
}
