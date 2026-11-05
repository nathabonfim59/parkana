package storage_test

// A5 profiling preparation (plan A5.1, runner sol-a-profile): bounded
// benchmark-only measurements of the storage sidecar path against one
// representative finalized Parquet file. Nothing here is a production knob,
// metric or endpoint.
//
// The fixture is one ~125,000,000-raw-line-byte Parquet file holding the
// api-stream records of the corpus identity recorded for the 2 GB small
// benchmark run (/tmp/sol-c/small-artifacts/ingest-parkana.json: seed
// 8098146604754141185, start 2026-11-04T13:23:15.266317Z, step 100us,
// streams api/checkout/search/worker, stream label service_name). That is the
// shape of one real uploaded object in that run: 2 GB / 16 files means each
// final file held ~125 MB of one stream's raw lines, promoted "level" column
// all null (parkbench labels carry no level), labels map holding
// service_name, meta empty. The file is built through the real
// ingest.Memtable -> Generation.Parts -> storage.Writer path so its batches,
// labels/meta encoding and row groups are what the ingester stores, not a
// hand-built lookalike.
//
// Line rendering is a byte-exact replica of cmd/parkbench corpus.go's record
// (which is package main and cannot be imported). The replica self-verifies
// against SHA-256 digests produced by the REAL generator through
// /tmp/sol-a-a5/corpus-verify before any fixture work, so drift fails the run.
//
// Gating: every benchmark skips unless PARKANA_A5_BENCH is set, so `make
// check` and ordinary -bench runs never build the heavy fixture. Sizes:
// PARKANA_A5_RAW_BYTES (default 125000000). CPU profiles are written only
// when PARKANA_A5_CPUPROFILE names a directory; runtime/pprof is started
// AFTER fixture setup and stopped after the loop, because `go test
// -cpuprofile` covers the whole process including setup. Fixture generation
// and file creation are also outside the timed loop.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/ingest"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
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
// (*Corpus).record as of fcddd53; the body must not be edited by hand;
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

// --- fixture ----------------------------------------------------------------

// a5StorageFixture is the finalized Parquet file every storage benchmark
// reads, with its exact identity.
type a5StorageFixture struct {
	path    string
	rows    int64
	raw     int64
	parquet int64
	// Marker and template counts inside the fixture, for the record.
	frequent, rare, short int64
	templates             [a5TemplateCount]int64
	first, last           time.Time
}

// a5APILabels is the api stream's label set: exactly what parkbench pushes
// for stream api with the default -stream-label.
var a5APILabels = logs.New(logs.Label{Name: "service_name", Value: "api"})

// a5PromotedFor matches the benchmark stack's parkana, which leaves
// -ingest.promoted-labels at its default (promote "level"); parkbench labels
// carry no level, so the promoted column exists but stays null.
func a5PromotedFor(string) []string { return []string{"level"} }

// a5Fixture skips the heavy benchmarks unless explicitly enabled and builds
// the fixture fresh, in a per-benchmark b.TempDir that is removed with the
// benchmark. The build is setup: it happens before the timed loop and before
// any CPU profile starts. Each benchmark rebuilds its own copy instead of
// sharing process-global state.
func a5Fixture(b *testing.B) a5StorageFixture {
	b.Helper()
	if os.Getenv("PARKANA_A5_BENCH") == "" {
		b.Skip("a5: set PARKANA_A5_BENCH=1 (plus optional PARKANA_A5_RAW_BYTES and PARKANA_A5_CPUPROFILE=DIR) to run the heavy A5 storage benchmark")
	}
	fx, err := a5BuildFixture(b)
	if err != nil {
		b.Fatalf("a5: building storage fixture: %v", err)
	}
	return fx
}

// a5BuildFixture renders api-stream corpus records until the raw-byte target
// is reached and stores them as one finalized Parquet file through the real
// ingest path: Memtable admission (with real WAL payload sizing), Generation
// materialization, storage.Writer. Requests hold 1250 entries because the
// real corpus batches 5000 records across four streams, leaving ~1250 per
// stream per request.
func a5BuildFixture(b *testing.B) (a5StorageFixture, error) {
	if err := a5VerifyReplica(); err != nil {
		return a5StorageFixture{}, err
	}
	target := a5EnvInt(b, "PARKANA_A5_RAW_BYTES", 125_000_000)
	ctx := b.Context()
	mem, err := ingest.NewMemtable(ingest.MemtableOptions{
		StreamLabels: []string{"service_name"},
		PromotedFor:  a5PromotedFor,
	})
	if err != nil {
		return a5StorageFixture{}, err
	}
	defer mem.Close()

	var fx a5StorageFixture
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
		line, stream, ts, tpl, hits := a5Record(i)
		if stream != 0 {
			continue
		}
		batch = append(batch, logs.Entry{Timestamp: ts, Line: line})
		fx.rows++
		fx.raw += int64(len(line))
		fx.templates[tpl]++
		if hits.frequent {
			fx.frequent++
		}
		if hits.rare {
			fx.rare++
		}
		if hits.short {
			fx.short++
		}
		if fx.first.IsZero() {
			fx.first = ts
		}
		fx.last = ts
		if len(batch) == 1250 {
			if err := admit(); err != nil {
				return a5StorageFixture{}, err
			}
		}
		if fx.raw >= target {
			break
		}
	}
	if err := admit(); err != nil {
		return a5StorageFixture{}, err
	}
	if err := mem.Seal(ctx, tenant.Default); err != nil {
		return a5StorageFixture{}, err
	}
	gens, err := mem.TakeSealed(ctx, tenant.Default)
	if err != nil {
		return a5StorageFixture{}, err
	}
	defer func() {
		for _, g := range gens {
			g.Release()
		}
	}()
	if len(gens) != 1 {
		return a5StorageFixture{}, fmt.Errorf("a5: fixture produced %d generations, want 1 (memtable limits must stay unlimited)", len(gens))
	}
	parts, err := gens[0].Parts(ctx)
	if err != nil {
		return a5StorageFixture{}, err
	}
	if len(parts) == 0 {
		return a5StorageFixture{}, errors.New("a5: fixture produced 0 parts, want exactly one api part")
	}
	if len(parts) != 1 || parts[0].Stream != "api" {
		return a5StorageFixture{}, fmt.Errorf("a5: fixture produced %d parts (first stream %q), want exactly one api part", len(parts), parts[0].Stream)
	}
	// Per-benchmark scratch directory: removed with the benchmark.
	fx.path = filepath.Join(b.TempDir(), "a5-fixture.parquet")
	f, err := os.Create(fx.path)
	if err != nil {
		return a5StorageFixture{}, err
	}
	w, werr := storage.NewWriter(f, parts[0].Record.Schema(), a5PromotedFor("api"))
	writeErr := werr
	if w != nil {
		writeErr = errors.Join(w.Write(parts[0].Record), writeErr)
		stats, cerr := w.Close()
		if cerr == nil && stats.Rows != fx.rows {
			cerr = fmt.Errorf("a5: fixture wrote %d rows, want %d", stats.Rows, fx.rows)
		}
		if cerr == nil && !stats.LabelsComplete {
			cerr = errors.New("a5: fixture file lacks complete label summaries")
		}
		writeErr = errors.Join(writeErr, cerr)
	}
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return a5StorageFixture{}, err
	}
	fi, err := os.Stat(fx.path)
	if err != nil {
		return a5StorageFixture{}, err
	}
	fx.parquet = fi.Size()
	return fx, nil
}

// --- benchmarks --------------------------------------------------------------

// BenchmarkA5ReaderRecordsDecode measures raw Parquet decode only: one fresh
// Reader per iteration, every record batch decoded, the line column resolved
// by name once and every line value touched, with exact row and raw-byte
// assertions. This is the floor that BuildSidecar always pays, because
// BuildSidecar rereads the finalized Parquet through the same Records path.
func BenchmarkA5ReaderRecordsDecode(b *testing.B) {
	fx := a5Fixture(b)
	b.SetBytes(fx.raw)
	b.ReportAllocs()
	a5LogFixture(b, fx, "decode-only")
	stop := a5StartCPUProfile(b)
	for b.Loop() {
		rows, raw, err := a5DecodeOnce(b.Context(), fx)
		if err != nil {
			b.Fatal(err)
		}
		if rows != fx.rows || raw != fx.raw {
			b.Fatalf("a5: decode pass saw %d rows / %d raw bytes, want %d / %d", rows, raw, fx.rows, fx.raw)
		}
	}
	stop()
}

// BenchmarkA5BuildSidecarDefault measures the real storage.BuildSidecar with
// index.DefaultParams (N=4, FPRate=0.01, MaxGrams=2097152; the phase 3
// defaults the benchmark stack pins) over the same fixture and a fresh
// Reader: full Parquet decode plus gram collection plus bloom encoding.
func BenchmarkA5BuildSidecarDefault(b *testing.B) {
	fx := a5Fixture(b)
	b.SetBytes(fx.raw)
	b.ReportAllocs()
	a5LogFixture(b, fx, "build-sidecar default N4/FP0.01/cap2097152")
	var encoded int64
	stop := a5StartCPUProfile(b)
	for b.Loop() {
		n, err := a5BuildSidecarOnce(b.Context(), fx, index.DefaultParams)
		if err != nil {
			b.Fatal(err)
		}
		encoded = n
	}
	stop()
	b.ReportMetric(float64(encoded), "sidecar-bytes")
}

// BenchmarkA5BuildSidecarCap1 measures the same real BuildSidecar with
// MaxGrams=1: gram collection stops after the cap is exceeded, but the whole
// Parquet file is still decoded, so this isolates the gram-collection share
// from the decode share when compared with the default run. ErrTooManyGrams
// is the expected, asserted outcome.
func BenchmarkA5BuildSidecarCap1(b *testing.B) {
	fx := a5Fixture(b)
	b.SetBytes(fx.raw)
	b.ReportAllocs()
	a5LogFixture(b, fx, "build-sidecar cap1 (expected ErrTooManyGrams)")
	stop := a5StartCPUProfile(b)
	for b.Loop() {
		_, err := a5BuildSidecarOnce(b.Context(), fx,
			index.Params{N: index.DefaultParams.N, FPRate: index.DefaultParams.FPRate, MaxGrams: 1})
		if !errors.Is(err, index.ErrTooManyGrams) {
			b.Fatalf("a5: cap1 BuildSidecar error = %v, want index.ErrTooManyGrams", err)
		}
	}
	stop()
}

// a5BuildSidecarOnce opens a fresh Reader and runs the real BuildSidecar.
func a5BuildSidecarOnce(ctx context.Context, fx a5StorageFixture, params index.Params) (int64, error) {
	f, err := os.Open(fx.path)
	if err != nil {
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, err
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		f.Close()
		return 0, err
	}
	data, err := storage.BuildSidecar(ctx, rd, params)
	err = errors.Join(err, rd.Close())
	f.Close()
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// a5DecodeOnce is one decode-only scan; see BenchmarkA5ReaderRecordsDecode.
func a5DecodeOnce(ctx context.Context, fx a5StorageFixture) (rows, raw int64, err error) {
	f, err := os.Open(fx.path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	rd, err := storage.NewReader(f, fi.Size())
	if err != nil {
		return 0, 0, err
	}
	defer rd.Close()
	lineIdx := -1
	for rec, rerr := range rd.Records(ctx) {
		if rerr != nil {
			return 0, 0, rerr
		}
		if lineIdx < 0 {
			// Reader validation guarantees exactly one line column, but its
			// position is not fixed, so resolve it by name as BuildSidecar
			// does.
			idx := rec.Schema().FieldIndices(schema.ColLine)
			if len(idx) != 1 {
				return 0, 0, fmt.Errorf("a5: fixture has %d %q columns, want 1", len(idx), schema.ColLine)
			}
			lineIdx = idx[0]
		}
		line, ok := rec.Column(lineIdx).(*array.String)
		if !ok {
			return 0, 0, fmt.Errorf("a5: %s column has type %T", schema.ColLine, rec.Column(lineIdx))
		}
		for i := range int(rec.NumRows()) {
			// A null line holds no bytes; the corpus never emits one.
			if !line.IsNull(i) {
				rows++
				raw += int64(len(line.Value(i)))
			}
		}
	}
	return rows, raw, nil
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
// Unlike `go test -cpuprofile`, this excludes fixture setup from the profile.
// When a profile was requested, a failure to start it is fatal, and the stop
// function is idempotent and registered with b.Cleanup so no exit path
// (including b.Fatal) can leave profiling active.
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

// a5LogFixture records the fixture's exact identity in the benchmark log.
func a5LogFixture(b *testing.B, fx a5StorageFixture, mode string) {
	b.Helper()
	b.Logf("a5: %s: seed=%d start=%s step=%s stream=api stream_label=service_name promoted=[level] rows=%d raw=%d parquet=%d markers(frequent=%d rare=%d short=%d) ts=[%s..%s]",
		mode, a5Seed, a5Start.Format(time.RFC3339Nano), a5Step, fx.rows, fx.raw, fx.parquet,
		fx.frequent, fx.rare, fx.short, fx.first.Format(time.RFC3339Nano), fx.last.Format(time.RFC3339Nano))
	var tpl strings.Builder
	for i, n := range fx.templates {
		if n > 0 {
			fmt.Fprintf(&tpl, " %s=%d", a5TemplateName(i), n)
		}
	}
	b.Logf("a5: fixture templates:%s", tpl.String())
}

// a5TemplateName mirrors parkbench's template names for fixture logs.
func a5TemplateName(tpl int) string {
	switch tpl {
	case a5TemplateAccessJSON:
		return "access_json"
	case a5TemplateAppLogfmt:
		return "app_logfmt"
	case a5TemplatePlain:
		return "plain"
	case a5TemplateNginx:
		return "nginx"
	case a5TemplateFixed:
		return "fixed_256"
	case a5TemplateAuditJSON:
		return "audit_json"
	case a5TemplateStack:
		return "stack"
	case a5TemplateHeartbeat:
		return "heartbeat"
	case a5TemplateTail:
		return "tail"
	default:
		return "?"
	}
}
