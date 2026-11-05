package ingest

// A5 profiling preparation (plan A5.1, runner sol-a-profile): bounded
// benchmark-only measurements of the ingest write path, per the A5 brief.
// Nothing here is a production knob, metric or endpoint.
//
// Two benchmarks:
//
//   - BenchmarkA5PushJSON drives the REAL push path: the real JSON push
//     handler and middleware through the real HTTP server, real WAL enqueue
//     with real fsync (group commit per tenant worker) and real memtable
//     admission, against the bench-stack parkana configuration (flush 1s,
//     memtable 512MiB, WAL 256MiB, default level promotion). The flusher is a
//     private benchmark fake that records the generation and lets
//     service.publish Release its Arrow memory: local Parquet writing stays
//     out of this measurement (it has its own benchmark below) while a
//     disabled-looking flusher cannot let generations pile up and hang the
//     run. Request bodies are pre-encoded in setup from the same pinned
//     corpus replica as internal/storage/a5_bench_test.go, in parkbench's
//     exact ingest topology: parkbench runs one sequential sender per label
//     set (4 streams: api, checkout, search, worker) sharing a 16-slot
//     -concurrency semaphore, so with 4 streams the effective maximum of
//     in-flight requests is 4; each 5000-record generated batch
//     (-batch-lines 5000) splits into ~1250-entry application/json requests,
//     one per stream. One benchmark iteration posts the whole bounded pool
//     once; bound it with PARKANA_A5_PUSH_BYTES (default 32 MiB raw) and a
//     bounded -benchtime (1x-3x).
//
//   - BenchmarkA5UploadPipeline drives the real visibility path after
//     admission: the REAL Flusher writes local Parquet (real fsynced files
//     and manifest commit), then the REAL Uploader.RunOnce merges the local
//     files through storage.Merge, builds the sidecar with the real
//     storage.BuildSidecar, Puts data plus sidecar and publishes the catalog
//     row. The corpus is api-only with the same ~125 MB raw target as the
//     storage fixture, so the pass publishes exactly ONE final object of one
//     stream/hour, the same shape as one real uploaded file. Benchmark-only
//     wall timers wrap Flush, the merge seam, data and sidecar Store.Put and
//     Catalog.AddFiles/ReplaceFiles so the stages are distinguishable in the
//     log next to the CPU profile; the FS Put and memory-catalog timings are
//     local CPU/disk diagnostics, NOT S3 network latency. Generation
//     ownership makes the pass one-shot: run it with -benchtime=1x (a second
//     iteration has nothing to flush or upload and only records as an extra
//     pass).
//
// Gating and profiling match the storage A5 file: PARKANA_A5_BENCH must be
// set or everything skips; PARKANA_A5_CPUPROFILE=DIR starts runtime/pprof
// AFTER setup and stops it after the loop so corpus generation, encoding,
// memtable fill and service start stay out of the profile; `go test
// -cpuprofile` (whole process) must not be used for these.
//
// The corpus replica below is byte-identical to the one in
// internal/storage/a5_bench_test.go (both were generated from corpus.go in
// one piece) and self-verifies against the same digests from
// /tmp/sol-a-a5/corpus-verify before any work.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// --- pinned corpus replica --------------------------------------------------

// Corpus identity of the recorded 2 GB small run. Changing any of these
// changes the fixture's meaning; they must stay exactly what
// /tmp/sol-c/small-artifacts/ingest-parkana.json records.
const (
	a5Seed uint64 = 8098146604754141185
	// a5Step is the corpus timestamp step (step_micros 100).
	a5Step = 100 * time.Microsecond
	// a5TimeLayout is parkbench's line-embedded timestamp layout.
	a5TimeLayout = "2006-01-02T15:04:05.000000Z"
)

var (
	// a5Start is record 0's timestamp from the small-run manifest.
	a5Start = time.Date(2026, 11, 4, 13, 23, 15, 266317000, time.UTC)
	// a5Streams is parkbench's default stream set; record i goes to
	// a5Streams[i%4].
	a5Streams = [...]string{"api", "checkout", "search", "worker"}
)

// Marker literals, clustering blocks and schedules, copied from corpus.go.
const (
	a5FrequentMarker = "PKFREQ-2v"
	a5RareMarker     = "PKRARE-9xqz"
	a5ShortNeedle    = "zq9x"

	a5RareBlockA  = 2
	a5RareBlockB  = 9
	a5RareBlockC  = 25
	a5ShortBlockA = 4
	a5ShortBlockB = 20

	a5FrequentEvery   = 8
	a5RareEvery       = 16
	a5ShortEvery      = 32
	a5MarkerBlockSize = 65536
	a5FixedLineBytes  = 256
)

// Template indexes, as in corpus.go.
const (
	a5TemplateAccessJSON = iota
	a5TemplateAppLogfmt
	a5TemplatePlain
	a5TemplateNginx
	a5TemplateFixed
	a5TemplateAuditJSON
	a5TemplateStack
	a5TemplateHeartbeat
	a5TemplateTail
	a5TemplateCount
)

func a5IsRareBlock(b int64) bool {
	return b == a5RareBlockA || b == a5RareBlockB || b == a5RareBlockC
}

func a5IsShortBlock(b int64) bool {
	return b == a5ShortBlockA || b == a5ShortBlockB
}

// a5MarkerHits says which marker classes one record carries.
type a5MarkerHits struct {
	frequent bool
	rare     bool
	short    bool
}

// a5Hits reports the marker classes record i carries.
func a5Hits(i int64) a5MarkerHits {
	block := i / a5MarkerBlockSize
	return a5MarkerHits{
		frequent: i%a5FrequentEvery == 0,
		rare:     a5IsRareBlock(block) && i%a5RareEvery == 0,
		short:    a5IsShortBlock(block) && i%a5ShortEvery == 0,
	}
}

// a5Timestamp returns the timestamp of record i.
func a5Timestamp(i int64) time.Time {
	return a5Start.Add(time.Duration(i) * a5Step)
}

// a5RNG is a splitmix64 stream.
type a5RNG uint64

func (r *a5RNG) next() uint64 {
	*r += 0x9e3779b97f4a7c15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// a5AppendHex appends n lowercase hex digits from the rng stream.
func a5AppendHex(b []byte, r *a5RNG, n int) []byte {
	const digits = "0123456789abcdef"
	for range n {
		b = append(b, digits[r.next()&0xf])
	}
	return b
}

// a5Record renders record i, its stream index, timestamp, template index and
// the markers it carries. It is a byte-exact standalone copy of corpus.go's
// (*Corpus).record as of fcddd53; the body must not be edited by hand:
// regenerate it from corpus.go if parkbench's corpus changes, and re-derive
// the a5Verify* digests with /tmp/sol-a-a5/corpus-verify.
func a5Record(i int64) (line string, stream int, ts time.Time, tpl int, hits a5MarkerHits) {
	// Choice tables live here, not at package level, so nothing can mutate
	// the fixture between calls.
	levels := [...]string{"info", "warn", "error", "debug"}
	msgs := [...]string{
		"request completed",
		"connection timed out",
		"cache miss",
		"user login succeeded",
		"disk quota exceeded",
		"retrying upstream",
		"invalid token",
		"slow query",
	}
	methods := [...]string{"GET", "POST", "PUT", "DELETE"}
	words := [...]string{"items", "users", "orders", "sessions", "health", "metrics", "cart", "profile"}
	statuses := [...]uint64{200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}
	r := a5RNG(a5Seed ^ (uint64(i)+1)*0x9e3779b97f4a7c15)
	tpl = int(r.next() % uint64(a5TemplateCount-1)) // the tail template is not selected
	stream = int(i % int64(len(a5Streams)))
	ts = a5Timestamp(i)
	level := levels[r.next()%uint64(len(levels))]
	msg := msgs[r.next()%uint64(len(msgs))]
	method := methods[r.next()%uint64(len(methods))]
	word := words[r.next()%uint64(len(words))]
	status := statuses[r.next()%uint64(len(statuses))]
	duration := 1 + r.next()%5000
	size := 1 + r.next()%65536

	hits = a5Hits(i)
	var markers []byte
	if hits.frequent {
		markers = append(markers, a5FrequentMarker...)
	}
	if hits.rare {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, a5RareMarker...)
	}
	if hits.short {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, a5ShortNeedle...)
	}

	b := make([]byte, 0, 512)
	switch tpl {
	case a5TemplateAccessJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, a5Streams[stream]...)
		b = append(b, `","msg":"`...)
		b = append(b, msg...)
		b = append(b, `","method":"`...)
		b = append(b, method...)
		b = append(b, `","path":"/api/v1/`...)
		b = append(b, word...)
		b = append(b, '/')
		b = strconv.AppendInt(b, i, 10)
		b = append(b, `","status":`...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, `,"duration_ms":`...)
		b = strconv.AppendUint(b, duration, 10)
		if len(markers) > 0 {
			b = append(b, `,"marker":"`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
		b = append(b, '}')
	case a5TemplateAppLogfmt:
		b = append(b, "ts="...)
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, " level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, a5Streams[stream]...)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" id=`...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " latency_ms="...)
		b = strconv.AppendUint(b, duration, 10)
		b = append(b, " trace_id="...)
		b = a5AppendHex(b, &r, 16)
		b = append(b, " span_id="...)
		b = a5AppendHex(b, &r, 8)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case a5TemplatePlain:
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, ' ')
		b = append(b, strings.ToUpper(level)...)
		b = append(b, ' ')
		b = append(b, a5Streams[stream]...)
		b = append(b, ' ')
		b = append(b, msg...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " status="...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, " duration="...)
		b = strconv.AppendUint(b, duration, 10)
		b = append(b, "ms"...)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case a5TemplateNginx:
		b = append(b, "10.0."...)
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, '.')
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, ` - - [`...)
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, `] "GET /`...)
		b = append(b, word...)
		b = append(b, '/')
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` HTTP/1.1" `...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, ' ')
		b = strconv.AppendUint(b, size, 10)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case a5TemplateFixed:
		b = append(b, "level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, a5Streams[stream]...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" payload=`...)
		for len(b) < a5FixedLineBytes {
			b = append(b, 'x')
		}
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case a5TemplateAuditJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, a5Streams[stream]...)
		b = append(b, `","event":"audit","trace":"`...)
		b = a5AppendHex(b, &r, 32)
		b = append(b, `","token":"`...)
		b = a5AppendHex(b, &r, 64)
		b = append(b, `","id":`...)
		b = strconv.AppendInt(b, i, 10)
		if len(markers) > 0 {
			b = append(b, `,"marker":"`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
		b = append(b, '}')
	case a5TemplateStack:
		b = ts.AppendFormat(b, a5TimeLayout)
		b = append(b, " ERROR "...)
		b = append(b, a5Streams[stream]...)
		b = append(b, " goroutine 42: "...)
		b = append(b, msg...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` err="upstream returned status `...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, `" stack="main.handler->serverHandler->goexit"`...)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case a5TemplateHeartbeat:
		b = append(b, "tick="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " service="...)
		b = append(b, a5Streams[stream]...)
		b = append(b, " heartbeat ok sed="...)
		b = a5AppendHex(b, &r, 12)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	}
	return string(b), stream, ts, tpl, hits
}

// Replica self-verification: digests of the first a5VerifyRecords records
// produced by the REAL cmd/parkbench generator (small-run identity) through
// /tmp/sol-a-a5/corpus-verify. The tail template never fires this early.
const (
	a5VerifyRecords = 4096
	a5VerifyAllSHA  = "b79c486f328d1bd572763c26fc9e88ce2f1cf5d124b583b04ed0ecc16f7f051d"
	a5VerifyAllRaw  = 613787
	a5VerifyAPISHA  = "b450c4ddf7077872cbd012c4948b4277c698939f9c5e0e400393450cb5a8e041"
	a5VerifyAPIRaw  = 158805
	a5VerifyAPILine = 1024
)

// a5VerifyReplica fails when the corpus replica above no longer renders the
// same bytes as the pinned parkbench corpus for the small-run identity.
func a5VerifyReplica() error {
	hAll, hAPI := sha256.New(), sha256.New()
	var rawAll, rawAPI, apiLines int64
	for i := int64(0); i < a5VerifyRecords; i++ {
		line, stream, _, _, _ := a5Record(i)
		io.WriteString(hAll, line)
		io.WriteString(hAll, "\n")
		rawAll += int64(len(line))
		if stream == 0 {
			io.WriteString(hAPI, line)
			io.WriteString(hAPI, "\n")
			rawAPI += int64(len(line))
			apiLines++
		}
	}
	shaAll := hex.EncodeToString(hAll.Sum(nil))
	shaAPI := hex.EncodeToString(hAPI.Sum(nil))
	switch {
	case shaAll != a5VerifyAllSHA || rawAll != a5VerifyAllRaw:
		return fmt.Errorf("a5: corpus replica drifted from pinned parkbench corpus: all sha256=%s raw=%d, want %s %d (regenerate via /tmp/sol-a-a5/corpus-verify)",
			shaAll, rawAll, a5VerifyAllSHA, a5VerifyAllRaw)
	case shaAPI != a5VerifyAPISHA || rawAPI != a5VerifyAPIRaw || apiLines != a5VerifyAPILine:
		return fmt.Errorf("a5: corpus replica drifted from pinned parkbench corpus: api sha256=%s raw=%d lines=%d, want %s %d %d",
			shaAPI, rawAPI, apiLines, a5VerifyAPISHA, a5VerifyAPIRaw, a5VerifyAPILine)
	}
	return nil
}

// --- stream label helpers (shared shape with the storage A5 fixture) --------

// a5APILabels is the api stream's label set: exactly what parkbench pushes
// for stream api with the default -stream-label.
var a5APILabels = logs.New(logs.Label{Name: "service_name", Value: "api"})

// a5PromotedFor matches the benchmark stack's parkana, which leaves
// -ingest.promoted-labels at its default (promote "level"); parkbench labels
// carry no level, so the promoted column exists but stays null.
func a5PromotedFor(string) []string { return []string{"level"} }

// --- push pool ---------------------------------------------------------------

// a5PushPool is the bounded set of pre-encoded push request bodies.
type a5PushPool struct {
	streams  [len(a5Streams)]a5StreamPool
	raw      int64
	lines    int64
	batches  int
	requests int
}

// a5StreamPool holds one stream's label set and its request bodies in
// generation order, so one sender goroutine posts them sequentially.
type a5StreamPool struct {
	labels logs.Labels
	bodies [][]byte
	raw    int64
	lines  int64
}

// a5BuildPushPool pre-encodes every request body in setup, so corpus
// generation and JSON encoding stay off the timed loop and the CPU profile.
func a5BuildPushPool(b *testing.B) *a5PushPool {
	b.Helper()
	if err := a5VerifyReplica(); err != nil {
		b.Fatal(err)
	}
	target := a5EnvInt(b, "PARKANA_A5_PUSH_BYTES", 32<<20)
	ctx := b.Context()
	pool := &a5PushPool{}
	for i := range pool.streams {
		pool.streams[i].labels = logs.New(logs.Label{Name: "service_name", Value: a5Streams[i]})
	}
	// parkbench -batch-lines 5000: every generated batch spans 5000 records
	// across the four streams, and each non-empty per-stream slice becomes
	// one application/json request from that stream's sender.
	const batchLines = 5000
	var slices [len(a5Streams)][]logs.Entry
	count := 0
	flushBatch := func() {
		pool.batches++
		for si := range slices {
			if len(slices[si]) == 0 {
				continue
			}
			sp := &pool.streams[si]
			body, err := pushcodec.MarshalJSON(ctx, &logs.PushRequest{
				Streams: []logs.Stream{{Labels: sp.labels, Entries: slices[si]}},
			})
			if err != nil {
				b.Fatalf("a5: encode push body: %v", err)
			}
			sp.bodies = append(sp.bodies, body)
			sp.raw += rawOf(slices[si])
			sp.lines += int64(len(slices[si]))
			pool.requests++
			slices[si] = slices[si][:0]
		}
	}
	for i := int64(0); ; i++ {
		line, stream, ts, _, _ := a5Record(i)
		slices[stream] = append(slices[stream], logs.Entry{Timestamp: ts, Line: line})
		pool.raw += int64(len(line))
		pool.lines++
		count++
		if count == batchLines {
			flushBatch()
			count = 0
		}
		if pool.raw >= target {
			break
		}
	}
	if count > 0 {
		flushBatch()
	}
	return pool
}

// rawOf sums the raw line bytes of one request's entries.
func rawOf(entries []logs.Entry) int64 {
	var n int64
	for _, e := range entries {
		n += int64(len(e.Line))
	}
	return n
}

// a5PushPath returns the push endpoint path from the lokiapi route pattern.
func a5PushPath() string {
	_, path, _ := strings.Cut(lokiapi.Push, " ")
	return path
}

// --- push benchmark ----------------------------------------------------------

// a5BenchIngest mirrors the benchmark stack's parkana ingest settings
// (deploy/compose/bench.yaml) except that the upload interval is quiet: the
// fake flusher below never writes local files, so the uploader has nothing to
// do and its pass would only add noise.
func a5BenchIngest() config.Ingest {
	return config.Ingest{
		MaxLineSize:      256 * config.KiB,
		MaxRequestSize:   16 * config.MiB,
		MemtableMaxBytes: 512 * config.MiB,
		WALMaxBytes:      256 * config.MiB,
		FlushInterval:    time.Second,
		UploadInterval:   time.Hour,
		UploadMinBytes:   128 * config.MiB,
		UploadMaxAge:     time.Hour,
		UploadMergeMax:   2 * config.GiB,
		Promotions:       []config.StreamPromotion{{Stream: "*", Columns: []string{"level"}}},
	}
}

// a5PushBench owns one running push service: its deps, the recording
// flusher, the push URL and an idempotent stop that cancels the run context
// and joins Run with a bounded wait, recording its error.
type a5PushBench struct {
	deps    app.Deps
	ff      *fakeFlusher
	baseURL string
	url     string
	// termCh closes once Run has returned, after termErr is set. stop waits on
	// it, so a startup failure that already observed the terminal state joins
	// immediately instead of waiting on a drained channel.
	termCh  chan struct{}
	termErr error
	runErr  error
	stop    func()
}

// a5NewPushService builds and runs the real service with a real WAL, real
// memtable and the real push handler, but a flusher that records and drops
// generations: service.publish still waits for WAL durability and Releases
// each generation's Arrow memory, so nothing accumulates. The returned
// handle's stop is idempotent and also registered with b.Cleanup, so a
// b.Fatal on any path still cancels and joins the service.
func a5NewPushService(b *testing.B) *a5PushBench {
	b.Helper()
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.New(server.Options{Logger: logger, Registry: reg})
	store, err := objstore.NewFS(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	deps := app.Deps{
		Config: config.Config{
			DataDir:      b.TempDir(),
			StreamLabels: []string{"service_name"},
			Ingest:       a5BenchIngest(),
		},
		Logger:  logger,
		Store:   store,
		Catalog: catalog.NewMemory(),
		HTTP:    srv,
		Metrics: reg,
	}
	ff := &fakeFlusher{}
	s, err := newService(deps, serviceOptions{
		newFlusher: func(*LocalStore) (genFlusher, error) { return ff, nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	pb := &a5PushBench{deps: deps, ff: ff, termCh: make(chan struct{})}
	runCtx, cancel := context.WithCancel(b.Context())
	go func() {
		pb.termErr = s.Run(runCtx)
		close(pb.termCh)
	}()
	pb.stop = sync.OnceFunc(func() {
		cancel()
		select {
		case <-pb.termCh:
			pb.runErr = pb.termErr
		case <-time.After(90 * time.Second):
			pb.runErr = errors.New("a5: service did not stop within 90s")
		}
	})
	b.Cleanup(pb.stop)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if s.ready(context.Background()) == nil {
			ts := httptest.NewServer(deps.HTTP.Handler())
			b.Cleanup(ts.Close)
			pb.baseURL = ts.URL
			pb.url = ts.URL + a5PushPath()
			return pb
		}
		select {
		case <-pb.termCh:
			// Do not consume the terminal state: stop must still see it and
			// join immediately. b.Cleanup runs stop on this Fatal path.
			b.Fatalf("a5: service stopped during startup: %v", pb.termErr)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Fatal("a5: service never became ready")
	return pb
}

// BenchmarkA5PushJSON measures the bounded push/WAL/memtable path through the
// real JSON push handler. See the package comment for the topology and the
// bounds; the WAL fsync histogram is scraped from the real /metrics endpoint
// afterwards so blocked fsync time is distinguishable from CPU.
func BenchmarkA5PushJSON(b *testing.B) {
	if os.Getenv("PARKANA_A5_BENCH") == "" {
		b.Skip("a5: set PARKANA_A5_BENCH=1 (plus optional PARKANA_A5_PUSH_BYTES and PARKANA_A5_CPUPROFILE=DIR) to run the heavy A5 push benchmark")
	}
	pool := a5BuildPushPool(b) // setup, unprofiled
	pb := a5NewPushService(b)  // setup, unprofiled
	client := &http.Client{Timeout: 120 * time.Second}
	b.SetBytes(pool.raw)
	b.ReportAllocs()
	var reqLines strings.Builder
	for i := range pool.streams {
		sp := &pool.streams[i]
		if len(sp.bodies) == 0 {
			continue
		}
		fmt.Fprintf(&reqLines, " %s=requests=%d~entries=%d/raw=%dB", a5Streams[i], len(sp.bodies), sp.lines/int64(len(sp.bodies)), sp.raw)
	}
	b.Logf("a5: push pool: seed=%d start=%s step=%s batch-lines=5000 concurrency=16(4-senders) batches=%d requests=%d lines=%d raw=%d;%s",
		a5Seed, a5Start.Format(time.RFC3339Nano), a5Step, pool.batches, pool.requests, pool.lines, pool.raw, reqLines.String())
	stop := a5StartCPUProfile(b)
	for b.Loop() {
		if err := a5PostPool(b.Context(), client, pb.url, pool); err != nil {
			b.Fatal(err)
		}
	}
	stop()
	pb.stop()
	if pb.runErr != nil {
		b.Fatalf("a5: service Run returned %v", pb.runErr)
	}
	b.Logf("a5: generations published and released by the recording flusher: %d (local Parquet intentionally excluded; see BenchmarkA5UploadPipeline)", len(pb.ff.flushed()))
	// Durability and admission must both be visible in the real metrics: a
	// completed push pass always syncs WAL records and admits lines/bytes.
	a5RequireMetrics(b, pb.baseURL,
		a5MetricReq{name: "parkana_ingest_wal_fsync_seconds_sum", positive: true, family: "parkana_ingest_wal_fsync_seconds"},
		a5MetricReq{name: "parkana_ingest_wal_fsync_seconds_count", positive: true},
		a5MetricReq{name: "parkana_ingest_lines_total", positive: true},
		a5MetricReq{name: "parkana_ingest_bytes_total", positive: true},
		a5MetricReq{name: "parkana_ingest_wal_errors_total"})
}

// a5PostPool posts every pre-encoded request body once with parkbench's
// sender topology: one sequential goroutine per label set, all sharing one
// 16-slot semaphore. With four label sets the effective maximum of
// concurrent requests is four, matching the recorded small run.
func a5PostPool(ctx context.Context, client *http.Client, url string, pool *a5PushPool) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var acked int64
	sem := make(chan struct{}, 16)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	for si := range pool.streams {
		wg.Go(func() {
			for _, body := range pool.streams[si].bodies {
				if ctx.Err() != nil {
					return
				}
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				err := a5PostOnce(ctx, client, url, body)
				<-sem
				if err != nil {
					fail(err)
					return
				}
				mu.Lock()
				acked++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	switch {
	case ctx.Err() != nil:
		// Workers stop at the first cancellation, so a canceled pass can never
		// prove the full 204 count; report the cancellation itself.
		return fmt.Errorf("a5: push pass canceled after %d of %d acknowledged requests: %w", acked, pool.requests, ctx.Err())
	case firstErr != nil:
		return firstErr
	case acked != int64(pool.requests):
		return fmt.Errorf("a5: only %d of %d requests were acknowledged with 204", acked, pool.requests)
	}
	return nil
}

// a5PostOnce sends one pre-encoded application/json push and requires 204.
func a5PostOnce(ctx context.Context, client *http.Client, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("a5: push returned %s", resp.Status)
	}
	return nil
}

// --- upload pipeline benchmark -----------------------------------------------

// a5TimingStore wraps an objstore.Store and records data and sidecar Put wall
// time. FS Put timings are local CPU/disk diagnostics, not S3 latency.
type a5TimingStore struct {
	inner                   objstore.Store
	mu                      sync.Mutex
	dataPut, sidecarPut     []time.Duration
	dataBytes, sidecarBytes int64
}

// Put implements objstore.Store.
func (s *a5TimingStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	sidecar := strings.HasSuffix(key, objstore.ExtSidecar)
	t0 := time.Now()
	err := s.inner.Put(ctx, key, r, size)
	d := time.Since(t0)
	s.mu.Lock()
	defer s.mu.Unlock()
	if sidecar {
		s.sidecarPut = append(s.sidecarPut, d)
		s.sidecarBytes += size
	} else {
		s.dataPut = append(s.dataPut, d)
		s.dataBytes += size
	}
	return err
}

// Get implements objstore.Store.
func (s *a5TimingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

// GetRange implements objstore.Store.
func (s *a5TimingStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return s.inner.GetRange(ctx, key, offset, length)
}

// Stat implements objstore.Store.
func (s *a5TimingStore) Stat(ctx context.Context, key string) (objstore.Info, error) {
	return s.inner.Stat(ctx, key)
}

// List implements objstore.Store.
func (s *a5TimingStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return s.inner.List(ctx, prefix)
}

// Delete implements objstore.Store.
func (s *a5TimingStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

// Close implements objstore.Store.
func (s *a5TimingStore) Close() error { return s.inner.Close() }

// a5TimingCatalog wraps a catalog.Catalog and records AddFiles/ReplaceFiles
// wall time.
type a5TimingCatalog struct {
	catalog.Catalog
	mu                     sync.Mutex
	addFiles, replaceFiles []time.Duration
	adds                   int64
}

// AddFiles implements catalog.Catalog.
func (c *a5TimingCatalog) AddFiles(ctx context.Context, files ...catalog.File) error {
	t0 := time.Now()
	err := c.Catalog.AddFiles(ctx, files...)
	c.mu.Lock()
	c.addFiles = append(c.addFiles, time.Since(t0))
	if err == nil {
		c.adds += int64(len(files))
	}
	c.mu.Unlock()
	return err
}

// ReplaceFiles implements catalog.Catalog.
func (c *a5TimingCatalog) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []catalog.File) error {
	t0 := time.Now()
	err := c.Catalog.ReplaceFiles(ctx, t, remove, add)
	c.mu.Lock()
	c.replaceFiles = append(c.replaceFiles, time.Since(t0))
	if err == nil {
		c.adds += int64(len(add))
	}
	c.mu.Unlock()
	return err
}

// ListFiles implements catalog.Catalog.
func (c *a5TimingCatalog) ListFiles(ctx context.Context, f catalog.Filter) iter.Seq2[catalog.File, error] {
	return c.Catalog.ListFiles(ctx, f)
}

// Streams implements catalog.Catalog.
func (c *a5TimingCatalog) Streams(ctx context.Context, t tenant.ID) ([]string, error) {
	return c.Catalog.Streams(ctx, t)
}

// SetSidecar implements catalog.Catalog.
func (c *a5TimingCatalog) SetSidecar(ctx context.Context, t tenant.ID, key string) error {
	return c.Catalog.SetSidecar(ctx, t, key)
}

// PurgeDeleted implements catalog.Catalog.
func (c *a5TimingCatalog) PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error) {
	return c.Catalog.PurgeDeleted(ctx, t, cutoff)
}

// Close implements catalog.Catalog.
func (c *a5TimingCatalog) Close() error { return c.Catalog.Close() }

// a5TimingFlusher wraps the real Flusher and records per-generation wall time.
type a5TimingFlusher struct {
	inner   genFlusher
	mu      sync.Mutex
	flushes []time.Duration
}

// Flush implements genFlusher.
func (f *a5TimingFlusher) Flush(ctx context.Context, gen SealedGeneration) error {
	t0 := time.Now()
	err := f.inner.Flush(ctx, gen)
	f.mu.Lock()
	f.flushes = append(f.flushes, time.Since(t0))
	f.mu.Unlock()
	return err
}

// BenchmarkA5UploadPipeline measures the post-admission visibility path with
// real local Parquet flush and one real uploader pass. One-shot: run with
// -benchtime=1x. See the package comment.
func BenchmarkA5UploadPipeline(b *testing.B) {
	if os.Getenv("PARKANA_A5_BENCH") == "" {
		b.Skip("a5: set PARKANA_A5_BENCH=1 (plus optional PARKANA_A5_UPLOAD_BYTES and PARKANA_A5_CPUPROFILE=DIR) to run the heavy A5 upload benchmark")
	}
	if err := a5VerifyReplica(); err != nil {
		b.Fatal(err)
	}
	target := a5EnvInt(b, "PARKANA_A5_UPLOAD_BYTES", 125_000_000)
	ctx := b.Context()

	// Setup (unprofiled): api-only corpus into a memtable that auto-seals
	// every 32 MiB of admission, like the 1s flush cadence accumulating
	// several local files per stream in the real run. Request shape matches
	// parkbench's ~1250-entry api slices.
	mem, err := NewMemtable(MemtableOptions{
		StreamLabels:     []string{"service_name"},
		PromotedFor:      a5PromotedFor,
		MemtableMaxBytes: 32 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer mem.Close()
	var poolRaw, poolLines int64
	var batch []logs.Entry
	var seq uint64
	admit := func() error {
		if len(batch) == 0 {
			return nil
		}
		req := &logs.PushRequest{Streams: []logs.Stream{{Labels: a5APILabels, Entries: batch}}}
		batch = batch[:0]
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
		if stream != 0 {
			continue
		}
		batch = append(batch, logs.Entry{Timestamp: ts, Line: line})
		poolRaw += int64(len(line))
		poolLines++
		if len(batch) == 1250 {
			if err := admit(); err != nil {
				b.Fatal(err)
			}
		}
		if poolRaw >= target {
			break
		}
	}
	if err := admit(); err != nil {
		b.Fatal(err)
	}
	if err := mem.Seal(ctx, tenant.Default); err != nil {
		b.Fatal(err)
	}
	gens, err := mem.TakeSealed(ctx, tenant.Default)
	if err != nil {
		b.Fatal(err)
	}
	// Generations released inside the timed loop are nilled there; a failure
	// on any path must still release the remainder.
	b.Cleanup(func() {
		for _, gen := range gens {
			if gen != nil {
				gen.Release()
			}
		}
	})

	ls, err := OpenLocal(ctx, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	realFlusher, err := NewFlusher(FlushOptions{Store: ls, PromotedFor: a5PromotedFor})
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
		Index:        index.DefaultParams,
		IndexMetrics: im,
	})
	if err != nil {
		b.Fatal(err)
	}
	// Benchmark-only timing around the real merge seam. RunOnce is serial, so
	// the slice needs no lock.
	var mergeDurs []time.Duration
	up.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		t0 := time.Now()
		stats, err := storage.Merge(ctx, w, inputs, promoted)
		mergeDurs = append(mergeDurs, time.Since(t0))
		return stats, err
	}

	b.SetBytes(poolRaw)
	b.ReportAllocs()
	b.Logf("a5: upload pool: seed=%d start=%s step=%s stream=api generations=%d lines=%d raw=%d memtable-seal=32MiB index=N4/FP0.01/cap2097152",
		a5Seed, a5Start.Format(time.RFC3339Nano), a5Step, len(gens), poolLines, poolRaw)
	stop := a5StartCPUProfile(b)
	var report UploadReport
	flushed := false
	for b.Loop() {
		// The pipeline is one-shot: the first pass releases every generation,
		// so refuse a second iteration before touching the nilled slice.
		if flushed {
			b.Fatal("a5: upload pipeline is one-shot: run with -benchtime=1x (second iteration refused)")
		}
		// Stage 1: real local-Parquet flush of every generation, releasing
		// each one after publication exactly as service.publish does.
		for i, gen := range gens {
			if err := tFlush.Flush(ctx, gen); err != nil {
				b.Fatal(err)
			}
			gen.Release()
			gens[i] = nil
		}
		flushed = true
		// Stage 2: one real uploader pass: merge, sidecar build, data and
		// sidecar Put, catalog publication, source retirement.
		report, err = up.RunOnce(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
	stop() // idempotent; b.Cleanup still covers every b.Fatal path
	// The api-only pool must publish exactly one final object holding every row.
	if report.Uploads != 1 || len(report.Completed) != 1 {
		b.Fatalf("a5: uploads=%d completed=%d, want exactly 1 (one api stream/hour group)", report.Uploads, len(report.Completed))
	}
	res := report.Completed[0]
	if res.Stream != "api" || res.Rows != poolLines {
		b.Fatalf("a5: published stream=%s rows=%d, want api rows=%d", res.Stream, res.Rows, poolLines)
	}
	var live int
	var rows int64
	var pubFile catalog.File
	for f, err := range tCat.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			b.Fatal(err)
		}
		live++
		rows += f.Rows
		pubFile = f
	}
	if live != 1 || rows != poolLines {
		b.Fatalf("a5: catalog lists %d files with %d rows, want 1 file with %d rows", live, rows, poolLines)
	}
	if !pubFile.HasSidecar {
		b.Fatal("a5: published catalog row lacks HasSidecar")
	}
	if !pubFile.LabelsComplete {
		b.Fatal("a5: published catalog row lacks LabelsComplete")
	}
	tStore.mu.Lock()
	sidecarPuts := len(tStore.sidecarPut)
	tStore.mu.Unlock()
	if sidecarPuts != 1 {
		b.Fatalf("a5: %d sidecar Puts observed, want exactly 1", sidecarPuts)
	}
	if _, err := tStore.Stat(ctx, objstore.SidecarKey(pubFile.Key)); err != nil {
		b.Fatalf("a5: sidecar object missing for %s: %v", pubFile.Key, err)
	}

	// Benchmark-only stage evidence: wall timers next to the CPU profile.
	tFlush.mu.Lock()
	flushes := slices.Clone(tFlush.flushes)
	tFlush.mu.Unlock()
	tStore.mu.Lock()
	dataPut, sidecarPut := slices.Clone(tStore.dataPut), slices.Clone(tStore.sidecarPut)
	dataBytes, sidecarBytes := tStore.dataBytes, tStore.sidecarBytes
	tStore.mu.Unlock()
	tCat.mu.Lock()
	addFiles := slices.Clone(tCat.addFiles)
	tCat.mu.Unlock()
	// The pipeline is one-shot, so each total below is also the per-pass cost.
	if a5LogDurations(b, "flush(local-parquet)", flushes) > 0 {
		b.ReportMetric(durationsSum(flushes).Seconds(), "flush-seconds")
	}
	if a5LogDurations(b, "merge(storage.Merge)", mergeDurs) > 0 {
		b.ReportMetric(durationsSum(mergeDurs).Seconds(), "merge-seconds")
	}
	if a5LogDurations(b, "data-put(FS)", dataPut) > 0 {
		b.ReportMetric(durationsSum(dataPut).Seconds(), "data-put-seconds")
	}
	if a5LogDurations(b, "sidecar-put(FS)", sidecarPut) > 0 {
		b.ReportMetric(durationsSum(sidecarPut).Seconds(), "sidecar-put-seconds")
	}
	if a5LogDurations(b, "catalog-addfiles(memory)", addFiles) > 0 {
		b.ReportMetric(durationsSum(addFiles).Seconds(), "addfiles-seconds")
	}
	b.Logf("a5: published object: key=%s rows=%d bytes=%d sources-retired=%d; sidecar-bytes=%d data-bytes=%d (FS Put and memory-catalog wall times are local CPU/disk diagnostics, not S3 latency)",
		res.Key, res.Rows, res.Bytes, res.Sources, sidecarBytes, dataBytes)
	// The sidecar work must be visible in the real index metrics: one
	// successful build, its encoded bytes and its duration sample. Missing or
	// zero samples mean the measurement did not do the work it claims.
	a5RequireRegistry(b, reg,
		a5MetricReq{name: "parkana_index_build_duration_seconds_sum", positive: true, family: "parkana_index_build_duration_seconds"},
		a5MetricReq{name: "parkana_index_build_duration_seconds_count", positive: true},
		a5MetricReq{name: "parkana_index_built_total", positive: true},
		a5MetricReq{name: "parkana_index_bytes_total", positive: true},
	)
}

// --- shared helpers ----------------------------------------------------------

// a5EnvInt reads a positive int64 environment override with a default.
func a5EnvInt(b *testing.B, name string, def int64) int64 {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		b.Fatalf("a5: %s must be a positive decimal integer, got %q", name, v)
	}
	return n
}

// a5StartCPUProfile starts a benchmark-scoped CPU profile after setup when
// PARKANA_A5_CPUPROFILE names a directory, and returns the stop function.
// Unlike `go test -cpuprofile`, this excludes setup from the profile. When a
// profile was requested, a failure to start it is fatal, and the stop function
// is idempotent and registered with b.Cleanup so no exit path (including
// b.Fatal) can leave profiling active.
func a5StartCPUProfile(b *testing.B) func() {
	b.Helper()
	dir := os.Getenv("PARKANA_A5_CPUPROFILE")
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("a5: create profile dir %s: %v", dir, err)
	}
	path := filepath.Join(dir, a5Sanitize(b.Name())+".pprof")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("a5: create cpu profile %s: %v", path, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		b.Fatalf("a5: start cpu profile %s: %v", path, err)
	}
	b.Logf("a5: cpu profile -> %s", path)
	stop := sync.OnceFunc(func() {
		pprof.StopCPUProfile()
		f.Close()
	})
	b.Cleanup(stop)
	return stop
}

// a5Sanitize maps a benchmark name to a safe file stem.
func a5Sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, name)
}

// a5MetricReq is one metric sample the benchmark must observe after work.
type a5MetricReq struct {
	name string
	// positive requires at least one sample with value > 0. Metrics that are
	// legitimately zero after a clean run (for example the WAL error counter)
	// leave this false but must still be present.
	positive bool
	// family, when set, also logs every sample of that metric family (the
	// histogram's _sum/_count and every _bucket line) as wall-time evidence,
	// without requiring any particular bucket.
	family string
}

// a5MetricsTimeout bounds one /metrics scrape.
const a5MetricsTimeout = 30 * time.Second

// a5RequireMetrics fetches baseURL/metrics and fails unless every required
// sample exists (and is positive when required). A transport error, a non-200
// response or a missing required sample is fatal: these samples are the
// measurement evidence, not best-effort logging. The request is bound to the
// benchmark context and a bounded client timeout so a stuck scrape cannot hang
// the run.
func a5RequireMetrics(b *testing.B, baseURL string, reqs ...a5MetricReq) {
	b.Helper()
	ctx, cancel := context.WithTimeout(b.Context(), a5MetricsTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		b.Fatalf("a5: metrics request %s: %v", baseURL, err)
	}
	resp, err := (&http.Client{Timeout: a5MetricsTimeout}).Do(req)
	if err != nil {
		b.Fatalf("a5: metrics scrape %s: %v", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("a5: metrics scrape %s: status %s, want 200", baseURL, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		b.Fatalf("a5: metrics read %s: %v", baseURL, err)
	}
	a5CheckMetrics(b, string(body), reqs)
}

// a5RequireRegistry serves reg through a throwaway server and applies the same
// required-sample checks as a5RequireMetrics.
func a5RequireRegistry(b *testing.B, reg *prometheus.Registry, reqs ...a5MetricReq) {
	b.Helper()
	ts := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer ts.Close()
	a5RequireMetrics(b, ts.URL, reqs...)
}

// a5CheckMetrics parses a Prometheus text exposition and enforces reqs.
func a5CheckMetrics(b *testing.B, body string, reqs []a5MetricReq) {
	b.Helper()
	seen := make(map[string]float64, len(reqs))
	familyLines := make(map[string][]string, len(reqs))
	for line := range strings.SplitSeq(body, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		for _, req := range reqs {
			if req.family != "" && (name == req.family || strings.HasPrefix(name, req.family+"_")) {
				familyLines[req.family] = append(familyLines[req.family], line)
			}
			if name != req.name {
				continue
			}
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				b.Fatalf("a5: metrics: parse %s value %q: %v", req.name, value, err)
			}
			seen[req.name] = max(seen[req.name], v)
		}
	}
	for _, req := range reqs {
		v, ok := seen[req.name]
		if !ok {
			b.Fatalf("a5: metrics: required sample %s missing after completed work", req.name)
		}
		if req.positive && v <= 0 {
			b.Fatalf("a5: metrics: required sample %s = %v, want > 0 after completed work", req.name, v)
		}
	}
	// One compact line: Go trims benchmark logs after ten newlines, so the
	// required samples and each histogram family (sum, count, buckets) must
	// share one line or the wall-time evidence would be cut off.
	var parts []string
	for _, req := range reqs {
		parts = append(parts, fmt.Sprintf("%s=%v", req.name, seen[req.name]))
	}
	for _, req := range reqs {
		if req.family != "" {
			parts = append(parts, fmt.Sprintf("%s buckets=[%s]", req.family, strings.Join(familyLines[req.family], ", ")))
		}
	}
	b.Logf("a5: required metrics: %s", strings.Join(parts, "; "))
}

// a5LogDurations logs a stage's wall-time summary and returns the call count.
func a5LogDurations(b *testing.B, stage string, ds []time.Duration) int {
	b.Helper()
	if len(ds) == 0 {
		b.Logf("a5: %s: no calls", stage)
		return 0
	}
	total := durationsSum(ds)
	var maxD time.Duration
	for _, d := range ds {
		maxD = max(maxD, d)
	}
	b.Logf("a5: %s: calls=%d total=%s mean=%s max=%s", stage, len(ds), total, total/time.Duration(len(ds)), maxD)
	return len(ds)
}

// durationsSum totals a duration slice.
func durationsSum(ds []time.Duration) time.Duration {
	var t time.Duration
	for _, d := range ds {
		t += d
	}
	return t
}
