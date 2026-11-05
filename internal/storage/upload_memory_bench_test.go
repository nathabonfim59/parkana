package storage

// Uploader merge memory profile: bounded, opt-in, one-shot measurements of
// the real storage.Merge fan-in path over pinned-corpus Parquet sources.
//
// This file is test-only. It adds no production API, metric or knob: most
// candidate readers are built by umNewReader, a local replica of newReader
// that owns the private Reader fields and therefore may set Arrow's
// BufferedStreamEnabled and a smaller input batch size. The baseline calls the
// real production newReader, and buffered1024 calls the real production
// newReaderWithOptions with MergeReaderOptions. All receive a test-only
// allocator wrapper that tracks the logical current/peak of requested live
// bytes on Allocate/Reallocate/Free; a reallocation is counted at its
// requested size after return, not as transient old+new bytes.
//
// Corpus: one deterministic source set built from the pinned v3 small-run
// identity (seed 8098146604754141185, start 2026-11-04T13:23:15.266317Z,
// step 100us). This file uses v3 streamFor
// ((i%shortEvery+i/shortEvery)%numStreams) and instanceFor, not the older A5
// fixture's v1 stream rotation, and scans the global record stream sequentially
// to keep only api records. The replica is duplicated here so this file can
// own package-private Reader construction; umVerifyReplica checks first-4096
// all-stream and api-stream line/timestamp/instance digests produced by the
// real cmd/parkbench v3 generator before any fixture work, so drift fails the
// run.
//
// Label shape is the real hot-stream benchmark shape: the api stream label
// service_name=api plus the generated instance label 0..15 (16 instances),
// and no entry metadata. cmd/parkbench's corpus assigns instance with
// instanceFor(i, numStreams, instances) = (i/shortEvery/numStreams)%instances;
// this fixture uses numStreams=4, shortEvery=32, instances=16, exactly like
// the full/hot corpus. cmd/parkbench builds logs.Entry{Timestamp, Line} with
// no Metadata, so the ingester's meta column is an empty map; the verifier
// asserts that empty map, the two labels and a null promoted "level" column
// for every row.
//
// Geometry: PARKANA_UPLOAD_MEMORY_RAW_BYTES (default 512 MiB) of api raw line
// bytes for EACH fan-in selected by PARKANA_UPLOAD_MEMORY_FANIN (default
// 16,32,64,128). Each fan-in gets its own deterministic contiguous partition
// of the same global record stream, filtered to the api stream, into exactly
// N actual-schema Parquet files, so bytes/file vary with N (32 MiB at 16,
// 4 MiB at 128) while every fan-in covers the same 512 MiB of api records.
// Every file is written through storage.NewWriter with
// schema.ForStream([]string{"level"}). The build asserts that every set has
// the same total rows/raw/SHA-256, which proves the partitions differ only in
// file boundaries.
//
// Merge variants (default baseline, buffered8192, buffered2048,
// buffered1024): baseline uses the real production newReader, buffered1024
// uses the production NewReader option path (newReaderWithOptions and
// MergeReaderOptions), and the others differ from baseline only in Arrow's
// BufferedStreamEnabled and input batch size. Every arm
// runs the real storage.Merge, asserts exact output rows, labels, metadata and
// dedup counts. All variants at one fan-in and all fan-in partitions must
// produce the same canonical decoded-output SHA-256; one variant per fan-in is
// additionally read back row by row and line-hashed. Every source must exceed
// 8192 rows for the real run; PARKANA_UPLOAD_MEMORY_SMOKE=1 relaxes that
// constraint for a tiny local compile/smoke invocation only.
//
// Metrics per arm: input-only Arrow logical peak/current from the allocator
// wrapper passed to the input Readers (with AssertSize(0) on cleanup), sampled
// Go HeapAlloc start/peak/current, sampled RSS (/proc/self/statm)
// start/peak/current, process CPU via getrusage and wall time, GC cycles and
// pause time, source compressed/uncompressed/footer/row-group aggregate
// metadata and output bytes. Merge's output concatenation and Writer use
// memory.DefaultAllocator, so input_arrow_* is exactly the input Reader
// footprint, not the whole-merge Arrow footprint; whole Go heap and RSS
// include output. A runtime.GC() runs before each arm, but RSS is process-wide
// and GC does not guarantee that allocated pages return to the OS; the
// reported start snapshots and absolute peaks include process baseline, so do
// not read a peak as a pure per-arm delta. The sidecar benchmark is separate;
// this file builds no sidecar and imports no index code.
//
// Profiles are written only when PARKANA_UPLOAD_MEMORY_PROFILEDIR names a
// directory; fixture generation and the per-arm pre-work GC happen before any
// profile starts. Heap profiles are cumulative allocation-history snapshots,
// not live in-use peaks: a base snapshot is written after the pre-arm GC and
// before measured work, and an end snapshot after the measured window with a
// forced GC outside the measured CPU/wall and after the CPU profile stops. The
// CPU profile includes the tiny final sampler telemetry, while the reported
// wall/CPU endpoints do not. The end in-use view is post-GC/post-release,
// never peak; the -alloc_space delta includes small snapshot/profiler
// bookkeeping as unclassified overhead. Use:
//
//	go tool pprof -alloc_space -base upload-memory-faninNNN-<variant>-merge.heap-base.pprof \
//	  upload-memory-faninNNN-<variant>-merge.heap-end.pprof
//
// Files:
//
//	upload-memory-faninNNN-<variant>-merge.cpu.pprof
//	upload-memory-faninNNN-<variant>-merge.heap-base.pprof
//	upload-memory-faninNNN-<variant>-merge.heap-end.pprof
//
// Gating and use:
//
//	PARKANA_UPLOAD_MEMORY_BENCH=1 \
//	PARKANA_UPLOAD_MEMORY_PROFILEDIR=/tmp/.../profiles \
//	go test ./internal/storage -run '^$' \
//	  -bench '^BenchmarkUploadMemoryMergeFanIn$' -benchtime=1x -count=1 -v
//
// The benchmark is one-shot by design (profiles and corpus identity are
// per-arm), so it skips any calibration call with b.N != 1; always pass
// -benchtime=1x. PARKANA_UPLOAD_MEMORY_RAW_BYTES, _FANIN, _VARIANTS,
// _SAMPLE_MS and _SMOKE exist for tiny local smoke runs; the full 512 MiB x 4
// fan-in x 4 variant matrix is intended for a dedicated benchmark host.

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/nathabonfim59/parkana/internal/schema"
)

// --- pinned corpus replica --------------------------------------------------

// Corpus identity of the pinned v3 small run. Changing any of these values
// changes the fixture's meaning.
const (
	umSeed uint64 = 8098146604754141185
	// umStep is the corpus timestamp step (step_micros 100).
	umStep = 100 * time.Microsecond
	// umTimeLayout is parkbench's line-embedded timestamp layout.
	umTimeLayout = "2006-01-02T15:04:05.000000Z"
)

var (
	// umStart is record 0's timestamp from the pinned small-run manifest.
	umStart = time.Date(2026, 11, 4, 13, 23, 15, 266317000, time.UTC)
	// umStreams is parkbench's default stream set; record i goes to
	// umStreamFor(i, 4). This fixture keeps only the api stream, so sources are
	// built by scanning the global record stream sequentially and filtering.
	umStreams = [...]string{"api", "checkout", "search", "worker"}
)

// Marker literals, clustering blocks and schedules, copied from corpus.go.
const (
	umFrequentMarker = "PKFREQ-2v"
	umRareMarker     = "PKRARE-9xqz"
	umShortNeedle    = "zq9x"

	umRareBlockA  = 2
	umRareBlockB  = 9
	umRareBlockC  = 25
	umShortBlockA = 4
	umShortBlockB = 20

	umFrequentEvery   = 8
	umRareEvery       = 16
	umShortEvery      = 32
	umMarkerBlockSize = 65536
	umFixedLineBytes  = 256
)

// Template indexes, as in corpus.go.
const (
	umTemplateAccessJSON = iota
	umTemplateAppLogfmt
	umTemplatePlain
	umTemplateNginx
	umTemplateFixed
	umTemplateAuditJSON
	umTemplateStack
	umTemplateHeartbeat
	umTemplateTail
	umTemplateCount
)

func umIsRareBlock(b int64) bool {
	return b == umRareBlockA || b == umRareBlockB || b == umRareBlockC
}

func umIsShortBlock(b int64) bool {
	return b == umShortBlockA || b == umShortBlockB
}

// umMarkerHits says which marker classes one record carries.
type umMarkerHits struct {
	frequent bool
	rare     bool
	short    bool
}

// umHits reports the marker classes record i carries.
func umHits(i int64) umMarkerHits {
	block := i / umMarkerBlockSize
	return umMarkerHits{
		frequent: i%umFrequentEvery == 0,
		rare:     umIsRareBlock(block) && i%umRareEvery == 0,
		short:    umIsShortBlock(block) && i%umShortEvery == 0,
	}
}

// umTimestamp returns the timestamp of record i.
func umTimestamp(i int64) time.Time {
	return umStart.Add(time.Duration(i) * umStep)
}

// umStreamFor is cmd/parkbench's v3 streamFor: records rotate through the
// configured streams within each shortEvery-record cohort.
func umStreamFor(i int64, numStreams int) int {
	return int((i%umShortEvery + i/umShortEvery) % int64(numStreams))
}

// umInstanceIndex is cmd/parkbench's instanceFor for four streams.
func umInstanceIndex(i int64) int {
	return int((i / umShortEvery / int64(len(umStreams))) % umInstances)
}

// umInstance returns the string instance label value of record i.
func umInstance(i int64) string {
	return strconv.Itoa(umInstanceIndex(i))
}

// umNextAPI returns the next global record index whose v3 stream is api.
func umNextAPI(i *int64) int64 {
	for {
		cur := *i
		*i = cur + 1
		if umStreamFor(cur, len(umStreams)) == 0 {
			return cur
		}
	}
}

// umRNG is a splitmix64 stream.
type umRNG uint64

func (r *umRNG) next() uint64 {
	*r += 0x9e3779b97f4a7c15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// umAppendHex appends n lowercase hex digits from the rng stream.
func umAppendHex(b []byte, r *umRNG, n int) []byte {
	const digits = "0123456789abcdef"
	for range n {
		b = append(b, digits[r.next()&0xf])
	}
	return b
}

// umRecord renders record i, its stream index, timestamp, template index and
// the markers it carries. The line rendering is the generator's v3 record
// body; stream selection uses v3 streamFor. umVerifyReplica pins line,
// timestamp and instance digests produced by the real cmd/parkbench v3
// generator, so divergence fails before any fixture work.
func umRecord(i int64) (line string, stream int, ts time.Time, tpl int, hits umMarkerHits) {
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
	r := umRNG(umSeed ^ (uint64(i)+1)*0x9e3779b97f4a7c15)
	tpl = int(r.next() % uint64(umTemplateCount-1)) // the tail template is not selected
	stream = umStreamFor(i, len(umStreams))
	ts = umTimestamp(i)
	level := levels[r.next()%uint64(len(levels))]
	msg := msgs[r.next()%uint64(len(msgs))]
	method := methods[r.next()%uint64(len(methods))]
	word := words[r.next()%uint64(len(words))]
	status := statuses[r.next()%uint64(len(statuses))]
	duration := 1 + r.next()%5000
	size := 1 + r.next()%65536

	hits = umHits(i)
	var markers []byte
	if hits.frequent {
		markers = append(markers, umFrequentMarker...)
	}
	if hits.rare {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, umRareMarker...)
	}
	if hits.short {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, umShortNeedle...)
	}

	b := make([]byte, 0, 512)
	switch tpl {
	case umTemplateAccessJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, umTimeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, umStreams[stream]...)
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
	case umTemplateAppLogfmt:
		b = append(b, "ts="...)
		b = ts.AppendFormat(b, umTimeLayout)
		b = append(b, " level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, umStreams[stream]...)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" id=`...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " latency_ms="...)
		b = strconv.AppendUint(b, duration, 10)
		b = append(b, " trace_id="...)
		b = umAppendHex(b, &r, 16)
		b = append(b, " span_id="...)
		b = umAppendHex(b, &r, 8)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case umTemplatePlain:
		b = ts.AppendFormat(b, umTimeLayout)
		b = append(b, ' ')
		b = append(b, strings.ToUpper(level)...)
		b = append(b, ' ')
		b = append(b, umStreams[stream]...)
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
	case umTemplateNginx:
		b = append(b, "10.0."...)
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, '.')
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, ` - - [`...)
		b = ts.AppendFormat(b, umTimeLayout)
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
	case umTemplateFixed:
		b = append(b, "level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, umStreams[stream]...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" payload=`...)
		for len(b) < umFixedLineBytes {
			b = append(b, 'x')
		}
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case umTemplateAuditJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, umTimeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, umStreams[stream]...)
		b = append(b, `","event":"audit","trace":"`...)
		b = umAppendHex(b, &r, 32)
		b = append(b, `","token":"`...)
		b = umAppendHex(b, &r, 64)
		b = append(b, `","id":`...)
		b = strconv.AppendInt(b, i, 10)
		if len(markers) > 0 {
			b = append(b, `,"marker":"`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
		b = append(b, '}')
	case umTemplateStack:
		b = ts.AppendFormat(b, umTimeLayout)
		b = append(b, " ERROR "...)
		b = append(b, umStreams[stream]...)
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
	case umTemplateHeartbeat:
		b = append(b, "tick="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " service="...)
		b = append(b, umStreams[stream]...)
		b = append(b, " heartbeat ok sed="...)
		b = umAppendHex(b, &r, 12)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	}
	return string(b), stream, ts, tpl, hits
}

// Replica self-verification: digests of the first umVerifyRecords global
// records produced by the REAL cmd/parkbench v3 generator with seed
// 8098146604754141185, start 2026-11-04T13:23:15.266317Z, step 100us, four
// streams and 16 instances. The harness was a temporary go-test overlay under
// cmd/parkbench (no committed cmd edits); the values below are for the
// api-only 16-instance fixture used here.
//
// Canonical encoding: line = line bytes + newline; ts = 8-byte little-endian
// UnixMicro; inst = 8-byte little-endian integer; tsinst = ts then inst.
// The older A5 fixture used v1 stream assignment and different line totals, so
// these digests intentionally do not match its constants.
const (
	umVerifyRecords            = 4096
	umVerifyAllLineSHA         = "d36c93664e3e488882a40376da5c91dd8e617978d9d461e127f59aed4e015d5d"
	umVerifyAllRaw       int64 = 613727
	umVerifyAllTSSHA           = "31c78d22d6b04d13231ce01e82afe23abdc37aa86d4d485480a4f3742f32e12b"
	umVerifyAllInstSHA         = "2e1631210ee4d8b6b71c64e061bf46be3b7dd6ec709575c580d0e3fe4e1e3678"
	umVerifyAllTSInstSHA       = "901eab187903e893a2163790cd73a59cd6e9e53c4e845bebe9b08cddce34d3b8"
	umVerifyAPILineSHA         = "d8b2fee54d6f7343021ec0364c94ac3ddf2792db402d5ab26c309a3759eb1e6a"
	umVerifyAPIRaw       int64 = 151508
	umVerifyAPICount     int64 = 1024
	umVerifyAPITSSHA           = "7b27b726c21da900ac77b3cf9882efccfac07f34b6ee26c96700f1484bdbdb70"
	umVerifyAPIInstSHA         = "7acd5acb395d76f1db67c97d02ccd9b3b082cae0739894ba0b81e0264823c33f"
	umVerifyAPITSInstSHA       = "1918f0964e7d8ca34b9641959073c18dd4c797e1d9eff9bc452ea9da60816211"
)

// umVerifyReplica fails when the duplicated corpus replica no longer renders
// the same v3 bytes, timestamps and instance labels as the pinned generator.
func umVerifyReplica() error {
	allLine, allTS, allInst, allTSInst := sha256.New(), sha256.New(), sha256.New(), sha256.New()
	apiLine, apiTS, apiInst, apiTSInst := sha256.New(), sha256.New(), sha256.New(), sha256.New()
	var rawAll, rawAPI, apiCount int64
	for i := int64(0); i < umVerifyRecords; i++ {
		line, stream, ts, _, _ := umRecord(i)
		inst := umInstanceIndex(i)
		umHashLine(allLine, line)
		umHashTS(allTS, ts)
		umHashInst(allInst, inst)
		umHashTSInst(allTSInst, ts, inst)
		rawAll += int64(len(line))
		if stream == 0 {
			umHashLine(apiLine, line)
			umHashTS(apiTS, ts)
			umHashInst(apiInst, inst)
			umHashTSInst(apiTSInst, ts, inst)
			rawAPI += int64(len(line))
			apiCount++
		}
	}
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"all line sha256", hex.EncodeToString(allLine.Sum(nil)), umVerifyAllLineSHA},
		{"all raw bytes", rawAll, umVerifyAllRaw},
		{"all timestamp sha256", hex.EncodeToString(allTS.Sum(nil)), umVerifyAllTSSHA},
		{"all instance sha256", hex.EncodeToString(allInst.Sum(nil)), umVerifyAllInstSHA},
		{"all timestamp+instance sha256", hex.EncodeToString(allTSInst.Sum(nil)), umVerifyAllTSInstSHA},
		{"api line sha256", hex.EncodeToString(apiLine.Sum(nil)), umVerifyAPILineSHA},
		{"api raw bytes", rawAPI, umVerifyAPIRaw},
		{"api records", apiCount, umVerifyAPICount},
		{"api timestamp sha256", hex.EncodeToString(apiTS.Sum(nil)), umVerifyAPITSSHA},
		{"api instance sha256", hex.EncodeToString(apiInst.Sum(nil)), umVerifyAPIInstSHA},
		{"api timestamp+instance sha256", hex.EncodeToString(apiTSInst.Sum(nil)), umVerifyAPITSInstSHA},
	}
	for _, c := range checks {
		if c.got != c.want {
			return fmt.Errorf("upload-memory: corpus replica drifted from pinned cmd/parkbench v3 corpus: %s = %v, want %v", c.name, c.got, c.want)
		}
	}
	return nil
}

func umHashLine(h hash.Hash, line string) {
	io.WriteString(h, line)
	io.WriteString(h, "\n")
}

func umHashTS(h hash.Hash, ts time.Time) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(ts.UnixMicro()))
	h.Write(buf[:])
}

func umHashInst(h hash.Hash, inst int) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(inst))
	h.Write(buf[:])
}

func umHashTSInst(h hash.Hash, ts time.Time, inst int) {
	umHashTS(h, ts)
	umHashInst(h, inst)
}

// --- configuration ----------------------------------------------------------

const (
	umBenchEnv        = "PARKANA_UPLOAD_MEMORY_BENCH"
	umProfileDirEnv   = "PARKANA_UPLOAD_MEMORY_PROFILEDIR"
	umRawEnv          = "PARKANA_UPLOAD_MEMORY_RAW_BYTES"
	umFanInEnv        = "PARKANA_UPLOAD_MEMORY_FANIN"
	umVariantsEnv     = "PARKANA_UPLOAD_MEMORY_VARIANTS"
	umSampleMSEnv     = "PARKANA_UPLOAD_MEMORY_SAMPLE_MS"
	umSmokeEnv        = "PARKANA_UPLOAD_MEMORY_SMOKE"
	umDefaultRawBytes = int64(512 << 20)
	umDefaultFanIn    = "16,32,64,128"
	umDefaultVariants = "baseline,buffered8192,buffered2048,buffered1024"
	umMinSourceRows   = int64(8192) // every real input must exceed this
	umStreamLabel     = "service_name"
	umStreamValue     = "api"
	umInstanceLabel   = "instance"
	// umInstances matches the full/hot benchmark's --instances-per-service 16.
	umInstances = 16
	umPromoted  = "level"
)

// umVariant is one input-reader configuration. name is the profile/benchmark
// stem; batch and buffered are the only knobs the candidate constructor sets.
type umVariant struct {
	name     string
	batch    int
	buffered bool
}

// umKnownVariants maps PARKANA_UPLOAD_MEMORY_VARIANTS names to readers.
var umKnownVariants = map[string]umVariant{
	"baseline":       {name: "baseline", batch: readerBatchSize, buffered: false},
	"buffered8192":   {name: "buffered8192", batch: 8192, buffered: true},
	"buffered2048":   {name: "buffered2048", batch: 2048, buffered: true},
	"buffered1024":   {name: "buffered1024", batch: 1024, buffered: true},
	"unbuffered2048": {name: "unbuffered2048", batch: 2048, buffered: false},
	"unbuffered1024": {name: "unbuffered1024", batch: 1024, buffered: false},
}

// umConfig is the resolved environment: one raw-byte target per fan-in and
// one arm matrix.
type umConfig struct {
	rawBytes    int64
	fanIn       []int
	variants    []umVariant
	sampleEvery time.Duration
	profileDir  string
	smoke       bool
}

// umLoadConfig reads and validates the opt-in benchmark environment.
func umLoadConfig(b *testing.B) umConfig {
	b.Helper()
	cfg := umConfig{
		rawBytes:    umEnvInt64(b, umRawEnv, umDefaultRawBytes),
		fanIn:       umParseFanIn(b, umEnvString(b, umFanInEnv, umDefaultFanIn)),
		sampleEvery: time.Duration(umEnvInt64(b, umSampleMSEnv, 5)) * time.Millisecond,
		profileDir:  os.Getenv(umProfileDirEnv),
		smoke:       os.Getenv(umSmokeEnv) != "",
	}
	for _, name := range strings.Split(umEnvString(b, umVariantsEnv, umDefaultVariants), ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		v, ok := umKnownVariants[name]
		if !ok {
			b.Fatalf("upload-memory: unknown variant %q (known: %s)", name, strings.Join(slices.Sorted(maps.Keys(umKnownVariants)), ", "))
		}
		cfg.variants = append(cfg.variants, v)
	}
	if len(cfg.variants) == 0 {
		b.Fatalf("upload-memory: no variants selected")
	}
	if cfg.profileDir != "" {
		if err := os.MkdirAll(cfg.profileDir, 0o755); err != nil {
			b.Fatalf("upload-memory: create profile dir %s: %v", cfg.profileDir, err)
		}
	}
	return cfg
}

func umEnvString(b *testing.B, name, def string) string {
	b.Helper()
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func umEnvInt64(b *testing.B, name string, def int64) int64 {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		b.Fatalf("upload-memory: %s must be a positive decimal integer, got %q", name, v)
	}
	return n
}

func umParseFanIn(b *testing.B, raw string) []int {
	b.Helper()
	var out []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 {
			b.Fatalf("upload-memory: %s entries must be positive integers, got %q", umFanInEnv, raw)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		b.Fatalf("upload-memory: %s selected no fan-in values", umFanInEnv)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// --- fixture ----------------------------------------------------------------

// umRow is one rendered source row before it is put in an Arrow batch.
type umRow struct {
	line     string
	ts       time.Time
	instance string
}

// umSource describes one actual-schema Parquet input file.
type umSource struct {
	path string

	rows  int64
	raw   int64
	first time.Time
	last  time.Time
	meta  umSourceMeta
}

// umSourceMeta is the footer data the scheduler/estimator could read.
type umSourceMeta struct {
	fileBytes         int64
	parquetRows       int64
	rowGroups         int
	compressedBytes   int64
	uncompressedBytes int64
	footerBytes       int64
	maxRGUncompressed int64
	maxRGCompressed   int64
	maxRGRows         int64
}

// umSourceAgg is the aggregate source metadata reported per arm.
type umSourceAgg struct {
	compressed   int64
	uncompressed int64
	footer       int64
	rowGroups    int
	maxRGUncomp  int64
	maxRGComp    int64
	maxRGRows    int64
}

// umFixtureSet is one fan-in's deterministic partition of the same records.
type umFixtureSet struct {
	fanIn   int
	dir     string
	sources []umSource

	rows  int64
	raw   int64
	sha   [32]byte
	first time.Time
	last  time.Time
	agg   umSourceAgg
}

// umBuildFixtureSet scans the pinned global record stream sequentially,
// keeps v3's api-stream records, and splits them into exactly fanIn contiguous
// actual-schema Parquet files, each holding about rawBytes/fanIn raw line
// bytes. The stopping rule depends only on cumulative api raw bytes, so every
// fan-in set contains exactly the same records.
func umBuildFixtureSet(b *testing.B, cfg umConfig, fanIn int) *umFixtureSet {
	b.Helper()
	promoted := []string{umPromoted}
	sch, err := schema.ForStream(promoted)
	if err != nil {
		b.Fatalf("upload-memory: schema: %v", err)
	}
	minRows := umMinSourceRows
	if cfg.smoke {
		minRows = 0
	}
	set := &umFixtureSet{fanIn: fanIn, dir: b.TempDir()}
	set.sources = make([]umSource, 0, fanIn)

	h := sha256.New()
	var cumRaw, cumRows int64
	perSource := cfg.rawBytes / int64(fanIn)
	if perSource <= 0 {
		perSource = 1
	}
	record := int64(0)
	for s := range fanIn {
		end := int64(s+1) * perSource
		if s == fanIn-1 {
			end = cfg.rawBytes
		}
		rows := make([]umRow, 0, 32*1024)
		for {
			i := record
			record++
			line, stream, ts, _, _ := umRecord(i)
			if stream != 0 {
				continue
			}
			rows = append(rows, umRow{line: line, ts: ts, instance: umInstance(i)})
			cumRaw += int64(len(line))
			cumRows++
			io.WriteString(h, line)
			io.WriteString(h, "\n")
			if cumRaw >= end && int64(len(rows)) > minRows {
				break
			}
		}
		if int64(len(rows)) <= minRows {
			b.Fatalf("upload-memory: fan-in %d source %d has %d rows, want > %d", fanIn, s, len(rows), minRows)
		}
		src := umSource{
			path:  filepath.Join(set.dir, fmt.Sprintf("src-%03d.parquet", s)),
			rows:  int64(len(rows)),
			raw:   umRowsRaw(rows),
			first: rows[0].ts,
			last:  rows[len(rows)-1].ts,
		}
		if err := umWriteSource(src.path, sch, promoted, rows); err != nil {
			b.Fatalf("upload-memory: fan-in %d write source %d: %v", fanIn, s, err)
		}
		meta, err := umReadSourceMeta(src.path)
		if err != nil {
			b.Fatalf("upload-memory: fan-in %d read source %d footer: %v", fanIn, s, err)
		}
		src.meta = meta
		if meta.parquetRows != src.rows {
			b.Fatalf("upload-memory: fan-in %d source %d parquet rows %d, want %d", fanIn, s, meta.parquetRows, src.rows)
		}
		set.sources = append(set.sources, src)
		set.agg.compressed += meta.compressedBytes
		set.agg.uncompressed += meta.uncompressedBytes
		set.agg.footer += meta.footerBytes
		set.agg.rowGroups += meta.rowGroups
		set.agg.maxRGUncomp = max(set.agg.maxRGUncomp, meta.maxRGUncompressed)
		set.agg.maxRGComp = max(set.agg.maxRGComp, meta.maxRGCompressed)
		set.agg.maxRGRows = max(set.agg.maxRGRows, meta.maxRGRows)
		b.Logf("upload-memory: fanin=%d source=%03d rows=%d raw=%d parquet=%d rgs=%d comp=%d uncomp=%d footer=%d maxRG=%d/%d ts=[%s..%s]",
			fanIn, s, src.rows, src.raw, meta.fileBytes, meta.rowGroups, meta.compressedBytes,
			meta.uncompressedBytes, meta.footerBytes, meta.maxRGCompressed, meta.maxRGUncompressed,
			src.first.Format(time.RFC3339Nano), src.last.Format(time.RFC3339Nano))
	}
	set.rows = cumRows
	set.raw = cumRaw
	copy(set.sha[:], h.Sum(nil))
	set.first = set.sources[0].first
	set.last = set.sources[len(set.sources)-1].last
	b.Logf("upload-memory: corpus fanin=%d seed=%d start=%s step=%s instances=%d rows=%d raw=%d sha256=%s files=%d avg_bytes_per_file=%.1f",
		fanIn, umSeed, umStart.Format(time.RFC3339Nano), umStep, umInstances,
		set.rows, set.raw, hex.EncodeToString(set.sha[:]), len(set.sources), float64(set.raw)/float64(len(set.sources)))
	return set
}

// umRowsRaw sums raw line bytes.
func umRowsRaw(rows []umRow) int64 {
	var n int64
	for _, r := range rows {
		n += int64(len(r.line))
	}
	return n
}

// umWriteSource publishes one source through the real storage.Writer with the
// schema, labels and meta shape the ingester produces for the api stream.
func umWriteSource(path string, sch *arrow.Schema, promoted []string, rows []umRow) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()

	w, err := NewWriter(f, sch, promoted)
	if err != nil {
		return err
	}
	rec, err := umBuildRecord(sch, rows)
	if err != nil {
		_, _ = w.Close()
		return err
	}
	writeErr := w.Write(rec)
	rec.Release()
	stats, closeErr := w.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if stats.Rows != int64(len(rows)) {
		return fmt.Errorf("wrote %d rows, want %d", stats.Rows, len(rows))
	}
	if !stats.LabelsComplete {
		return errors.New("source file lacks complete label summaries")
	}
	if !slices.Equal(stats.LabelValues[umPromoted], []string{""}) {
		return fmt.Errorf("source promoted level values = %v, want [\"\"]", stats.LabelValues[umPromoted])
	}
	return nil
}

// umBuildRecord builds one source batch with the actual schema:
// timestamp[us,UTC], line, labels{instance,service_name}, empty meta, null
// level.
func umBuildRecord(sch *arrow.Schema, rows []umRow) (arrow.RecordBatch, error) {
	rb := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer rb.Release()
	tsb, ok := rb.Field(0).(*array.TimestampBuilder)
	if !ok {
		return nil, fmt.Errorf("upload-memory: column 0 builder is %T", rb.Field(0))
	}
	lineb, ok := rb.Field(1).(*array.StringBuilder)
	if !ok {
		return nil, fmt.Errorf("upload-memory: column 1 builder is %T", rb.Field(1))
	}
	labelsb, ok := rb.Field(2).(*array.MapBuilder)
	if !ok {
		return nil, fmt.Errorf("upload-memory: column 2 builder is %T", rb.Field(2))
	}
	metab, ok := rb.Field(3).(*array.MapBuilder)
	if !ok {
		return nil, fmt.Errorf("upload-memory: column 3 builder is %T", rb.Field(3))
	}
	levelb, ok := rb.Field(4).(*array.StringBuilder)
	if !ok {
		return nil, fmt.Errorf("upload-memory: column 4 builder is %T", rb.Field(4))
	}
	keyb := labelsb.KeyBuilder().(*array.StringBuilder)
	itemb := labelsb.ItemBuilder().(*array.StringBuilder)
	for _, row := range rows {
		tsb.Append(arrow.Timestamp(schema.TS(row.ts)))
		lineb.Append(row.line)
		labelsb.Append(true)
		// logs.Labels.New sorts labels by name; keep the same order.
		keyb.Append(umInstanceLabel)
		itemb.Append(row.instance)
		keyb.Append(umStreamLabel)
		itemb.Append(umStreamValue)
		metab.Append(true)
		levelb.AppendNull()
	}
	return rb.NewRecordBatch(), nil
}

// umReadSourceMeta reads compressed/uncompressed/footer/row-group metadata
// straight from the source Parquet footer.
func umReadSourceMeta(path string) (umSourceMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return umSourceMeta{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return umSourceMeta{}, err
	}
	pf, err := file.NewParquetReader(io.NewSectionReader(f, 0, fi.Size()))
	if err != nil {
		return umSourceMeta{}, err
	}
	defer pf.Close()

	md := pf.MetaData()
	meta := umSourceMeta{
		fileBytes:   fi.Size(),
		rowGroups:   md.NumRowGroups(),
		footerBytes: int64(md.Size()),
	}
	for i := range meta.rowGroups {
		rg := md.RowGroup(i)
		meta.parquetRows += rg.NumRows()
		meta.compressedBytes += rg.TotalCompressedSize()
		meta.uncompressedBytes += rg.TotalByteSize()
		meta.maxRGUncompressed = max(meta.maxRGUncompressed, rg.TotalByteSize())
		meta.maxRGCompressed = max(meta.maxRGCompressed, rg.TotalCompressedSize())
		meta.maxRGRows = max(meta.maxRGRows, rg.NumRows())
	}
	return meta, nil
}

// --- test-only reader constructor ------------------------------------------

// umNewReader is a test-only replica of newReader that may set Arrow's
// buffered-stream switch and a different input record-batch size. It keeps
// the production validation and close semantics; only the two candidate knobs
// differ. It lives here so the benchmark measures the real merge path without
// adding a production option.
func umNewReader(r parquet.ReaderAtSeeker, size int64, mem memory.Allocator, batchRows int, buffered bool) (*Reader, error) {
	if r == nil {
		return nil, errNilReaderInput
	}
	if size < 8 {
		return nil, fmt.Errorf("%w: %d bytes cannot contain a parquet footer", errInvalidFileSize, size)
	}
	if mem == nil {
		return nil, errors.New("upload-memory: nil arrow allocator")
	}
	if batchRows < 1 {
		return nil, fmt.Errorf("upload-memory: invalid batch size %d", batchRows)
	}
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("upload-memory: determine parquet position: %w", err)
	}
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		_, _ = r.Seek(pos, io.SeekStart)
		return nil, fmt.Errorf("upload-memory: determine parquet size: %w", err)
	}
	if _, err := r.Seek(pos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("upload-memory: restore parquet position: %w", err)
	}
	if end != size {
		return nil, fmt.Errorf("%w: supplied size %d, reader size %d", errInvalidFileSize, size, end)
	}

	props := parquet.NewReaderProperties(mem)
	props.BufferedStreamEnabled = buffered
	pf, err := file.NewParquetReader(io.NewSectionReader(r, 0, size), file.WithReadProps(props))
	if err != nil {
		return nil, fmt.Errorf("upload-memory: open parquet: %w", err)
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		BatchSize: int64(batchRows),
		Parallel:  false,
	}, mem)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("upload-memory: open arrow reader: %w", err)
	}
	sch, err := fr.Schema()
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("upload-memory: read parquet schema: %w", err)
	}
	if err := validateBaseSchema(sch); err != nil {
		_ = pf.Close()
		return nil, err
	}
	return &Reader{
		fr:        fr,
		schema:    sch,
		closeOnce: sync.OnceValue(func() error { return pf.Close() }),
	}, nil
}

// --- input allocator tracker -----------------------------------------------

// umTrackedAllocator wraps the test CheckedAllocator and tracks the logical
// current and peak of requested live bytes across Allocate/Reallocate/Free.
// The peak is updated atomically when the request is accounted, not inferred
// from polling CurrentAlloc. Reallocate contributes size-len(b) after the
// call returns, so the transient old+new bytes some allocators materialize
// during reallocation are not counted as a logical peak.
type umTrackedAllocator struct {
	*memory.CheckedAllocator
	current atomic.Int64
	peak    atomic.Int64
}

func umNewTrackedAllocator() *umTrackedAllocator {
	return &umTrackedAllocator{CheckedAllocator: memory.NewCheckedAllocator(memory.NewGoAllocator())}
}

func (a *umTrackedAllocator) track(delta int64) {
	cur := a.current.Add(delta)
	for {
		peak := a.peak.Load()
		if cur <= peak || a.peak.CompareAndSwap(peak, cur) {
			return
		}
	}
}

func (a *umTrackedAllocator) Allocate(size int) []byte {
	out := a.CheckedAllocator.Allocate(size)
	a.track(int64(size))
	return out
}

func (a *umTrackedAllocator) Reallocate(size int, b []byte) []byte {
	out := a.CheckedAllocator.Reallocate(size, b)
	a.track(int64(size - len(b)))
	return out
}

func (a *umTrackedAllocator) Free(b []byte) {
	a.CheckedAllocator.Free(b)
	a.track(-int64(len(b)))
}

func (a *umTrackedAllocator) Current() int64 { return a.current.Load() }
func (a *umTrackedAllocator) Peak() int64    { return a.peak.Load() }

// --- fast contract checks (always run, not gated) ---------------------------

// TestUploadMemoryCorpusReplica pins the duplicated corpus replica even when
// the heavy benchmark is not running, so corpus drift fails make check.
func TestUploadMemoryCorpusReplica(t *testing.T) {
	t.Helper()
	if err := umVerifyReplica(); err != nil {
		t.Fatal(err)
	}
}

// TestUploadMemoryStreamFormula pins the v3 stream assignment used by the
// fixture's api filter.
func TestUploadMemoryStreamFormula(t *testing.T) {
	tests := []struct {
		record int64
		want   int
	}{
		{record: 0, want: 0},
		{record: 1, want: 1},
		{record: 2, want: 2},
		{record: 3, want: 3},
		{record: 4, want: 0},
		{record: 31, want: 3},
		{record: 32, want: 1},
		{record: 33, want: 2},
		{record: 34, want: 3},
		{record: 35, want: 0},
		{record: 63, want: 0},
		{record: 64, want: 2},
		{record: 95, want: 1},
		{record: 96, want: 3},
		{record: 127, want: 2},
		{record: 128, want: 0},
	}
	for _, tt := range tests {
		if got := umStreamFor(tt.record, len(umStreams)); got != tt.want {
			t.Errorf("umStreamFor(%d, %d) = %d, want %d", tt.record, len(umStreams), got, tt.want)
		}
	}
}

// TestUploadMemoryInstanceFormula pins the parkbench instance assignment used
// by the fixture's label shape.
func TestUploadMemoryInstanceFormula(t *testing.T) {
	tests := []struct {
		record int64
		want   string
	}{
		{record: 0, want: "0"},
		{record: 4, want: "0"},
		{record: 124, want: "0"},
		{record: 128, want: "1"},
		{record: 128 * 15, want: "15"},
		{record: 128 * 16, want: "0"},
	}
	for _, tt := range tests {
		if got := umInstance(tt.record); got != tt.want {
			t.Errorf("umInstance(%d) = %q, want %q", tt.record, got, tt.want)
		}
	}
}

// TestUploadMemoryTrackedAllocator proves the exact current/peak accounting
// used by the merge matrix, independent of the heavy benchmark.
func TestUploadMemoryTrackedAllocator(t *testing.T) {
	a := umNewTrackedAllocator()
	defer a.CheckedAllocator.AssertSize(t, 0)

	first := a.Allocate(100)
	if got := a.Current(); got != 100 {
		t.Fatalf("Current after Allocate(100) = %d, want 100", got)
	}
	second := a.Reallocate(200, first)
	if got := a.Current(); got != 200 {
		t.Fatalf("Current after Reallocate(200) = %d, want 200", got)
	}
	third := a.Allocate(1000)
	if got := a.Current(); got != 1200 {
		t.Fatalf("Current after Allocate(1000) = %d, want 1200", got)
	}
	if got := a.Peak(); got != 1200 {
		t.Fatalf("Peak = %d, want 1200", got)
	}
	a.Free(third)
	a.Free(second)
	if got := a.Current(); got != 0 {
		t.Fatalf("Current after freeing all = %d, want 0", got)
	}
	if got := a.Peak(); got != 1200 {
		t.Fatalf("Peak after freeing all = %d, want 1200", got)
	}
}

// --- merge benchmark --------------------------------------------------------

// BenchmarkUploadMemoryMergeFanIn is the opt-in one-shot fan-in matrix. It
// builds one deterministic partition per fan-in, then runs every selected
// reader variant through the real storage.Merge.
func BenchmarkUploadMemoryMergeFanIn(b *testing.B) {
	if os.Getenv(umBenchEnv) == "" {
		b.Skip("upload-memory: set " + umBenchEnv + "=1 to run the merge fan-in profile")
	}
	if b.N != 1 {
		b.Skip("upload-memory: one-shot benchmark; run with -benchtime=1x")
	}
	if err := umVerifyReplica(); err != nil {
		b.Fatal(err)
	}
	cfg := umLoadConfig(b)

	// Build every partition BEFORE any CPU/heap profile starts. They all come
	// from the same record stream and must be byte-identical as a corpus.
	sets := make([]*umFixtureSet, len(cfg.fanIn))
	for i, n := range cfg.fanIn {
		sets[i] = umBuildFixtureSet(b, cfg, n)
		if i > 0 {
			first := sets[0]
			if sets[i].rows != first.rows || sets[i].raw != first.raw || sets[i].sha != first.sha {
				b.Fatalf("upload-memory: fanin=%d corpus rows/raw/sha = %d/%d/%s, want %d/%d/%s",
					n, sets[i].rows, sets[i].raw, hex.EncodeToString(sets[i].sha[:]),
					first.rows, first.raw, hex.EncodeToString(first.sha[:]))
			}
		}
	}

	var globalSHA [32]byte
	var haveGlobal bool
	for i, n := range cfg.fanIn {
		set := sets[i]
		var faninSHA [32]byte
		var haveFanin, verified bool
		for _, v := range cfg.variants {
			b.Run(fmt.Sprintf("fanin%03d/%s", n, v.name), func(b *testing.B) {
				umRunMergeArm(b, cfg, set, v, &faninSHA, &haveFanin, &verified, &globalSHA, &haveGlobal)
			})
		}
		if err := os.RemoveAll(set.dir); err != nil {
			b.Logf("upload-memory: remove fanin=%d sources: %v", n, err)
		}
	}
}

// umRunMergeArm measures one reader variant at one fan-in.
func umRunMergeArm(b *testing.B, cfg umConfig, set *umFixtureSet, v umVariant, faninSHA *[32]byte, haveFanin, verified *bool, globalSHA *[32]byte, haveGlobal *bool) {
	b.Helper()
	if b.N != 1 {
		b.Skip("upload-memory: one-shot arm; run with -benchtime=1x")
	}
	n := set.fanIn
	outPath := filepath.Join(set.dir, fmt.Sprintf("merge-%03d-%s.parquet", n, v.name))
	stem := fmt.Sprintf("upload-memory-fanin%03d-%s-merge", n, v.name)

	tracker := umNewTrackedAllocator()
	defer tracker.CheckedAllocator.AssertSize(b, 0)
	runtime.GC()
	// Base heap snapshot and CPU-profile startup are outside the measured
	// window. The heap profile is cumulative allocation history; the end
	// snapshot is meant to be used with this base, not read as live peak.
	umWriteHeapProfile(b, cfg.profileDir, stem, "base")
	stopCPU := umStartCPUProfile(b, cfg.profileDir, stem)
	sampler := umStartSampler(cfg.sampleEvery)

	readers := make([]*Reader, 0, n)
	files := make([]*os.File, 0, n)
	closeInputs := func() error {
		var errs []error
		for _, rd := range readers {
			if rd != nil {
				errs = append(errs, rd.Close())
			}
		}
		for _, f := range files {
			if f != nil {
				errs = append(errs, f.Close())
			}
		}
		return errors.Join(errs...)
	}
	fail := func(format string, args ...any) {
		_ = closeInputs()
		end := sampler.endpoint()
		_ = sampler.stop(end)
		stopCPU()
		b.Fatalf(format, args...)
	}
	for i := range n {
		f, err := os.Open(set.sources[i].path)
		if err != nil {
			fail("upload-memory: open source %d: %v", i, err)
		}
		files = append(files, f)
		fi, err := f.Stat()
		if err != nil {
			fail("upload-memory: stat source %d: %v", i, err)
		}
		var rd *Reader
		switch v.name {
		case "baseline":
			// The baseline must exercise the real production constructor.
			rd, err = newReader(f, fi.Size(), tracker)
		case "buffered1024":
			// The 1024-row buffered arm must exercise the production option
			// path, so its digest also certifies MergeReaderOptions. It differs
			// from the other arms only in which constructor builds the reader.
			opts := MergeReaderOptions()
			if v.batch != opts.BatchSize || v.buffered != opts.BufferedStream {
				fail("upload-memory: buffered1024 variant %+v does not match MergeReaderOptions %+v", v, opts)
			}
			rd, err = newReaderWithOptions(f, fi.Size(), tracker, opts)
		default:
			rd, err = umNewReader(f, fi.Size(), tracker, v.batch, v.buffered)
		}
		if err != nil {
			fail("upload-memory: open reader %d: %v", i, err)
		}
		readers = append(readers, rd)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		fail("upload-memory: create output: %v", err)
	}
	stats, mergeErr := Merge(b.Context(), outFile, readers, []string{umPromoted})
	closeOutputErr := outFile.Close()
	closeInputsErr := closeInputs()
	if mergeErr != nil {
		fail("upload-memory: merge: %v", mergeErr)
	}
	if closeOutputErr != nil {
		fail("upload-memory: close output: %v", closeOutputErr)
	}
	if closeInputsErr != nil {
		fail("upload-memory: close inputs: %v", closeInputsErr)
	}

	// Endpoint first: wall and getrusage stop before sampler join, final
	// ReadMemStats and heap-profile writing. Join the sampler next so heap/RSS
	// end values do not include CPU-profile drain; the CPU profile itself
	// includes the tiny final sampler telemetry, while the reported wall/CPU
	// endpoints do not.
	end := sampler.endpoint()
	win := sampler.stop(end)
	stopCPU()
	inputArrowPeak, inputArrowEnd := tracker.Peak(), tracker.Current()
	if inputArrowEnd != 0 {
		b.Fatalf("upload-memory: tracked input Arrow bytes after close = %d, want 0", inputArrowEnd)
	}
	// The end heap snapshot is cumulative allocation history. Force a GC
	// after the measured window and after the CPU profile so completed arm
	// allocations are included in the base diff. Its in-use view remains
	// post-GC/post-release, never a live peak.
	runtime.GC()
	umWriteHeapProfile(b, cfg.profileDir, stem, "end")

	if stats.Rows != set.rows {
		b.Fatalf("upload-memory: merge wrote %d rows, want %d", stats.Rows, set.rows)
	}
	if stats.DeduplicatedRows != 0 {
		b.Fatalf("upload-memory: merge deduplicated %d rows, want 0", stats.DeduplicatedRows)
	}
	if !stats.LabelsComplete {
		b.Fatal("upload-memory: merged output lacks complete label summaries")
	}
	if !slices.Equal(stats.LabelValues[umPromoted], []string{""}) {
		b.Fatalf("upload-memory: merged promoted level values = %v, want [\"\"]", stats.LabelValues[umPromoted])
	}
	if !stats.MinTS.Equal(set.first) || !stats.MaxTS.Equal(set.last) {
		b.Fatalf("upload-memory: merged ts range = [%s..%s], want [%s..%s]",
			stats.MinTS.Format(time.RFC3339Nano), stats.MaxTS.Format(time.RFC3339Nano),
			set.first.Format(time.RFC3339Nano), set.last.Format(time.RFC3339Nano))
	}

	outInfo, err := os.Stat(outPath)
	if err != nil {
		b.Fatalf("upload-memory: stat output: %v", err)
	}
	outDigest := umDigestOutput(b, set, outPath, !*verified)
	*verified = true
	if !*haveFanin {
		*faninSHA = outDigest
		*haveFanin = true
	} else if outDigest != *faninSHA {
		b.Fatalf("upload-memory: fanin=%d %s output digest=%s, fan-in baseline=%s: reader variant changed merge output",
			n, v.name, hex.EncodeToString(outDigest[:]), hex.EncodeToString(faninSHA[:]))
	}
	if !*haveGlobal {
		*globalSHA = outDigest
		*haveGlobal = true
	} else if outDigest != *globalSHA {
		b.Fatalf("upload-memory: fanin=%d variant=%s output digest=%s, first arm=%s: partition changed merge output",
			n, v.name, hex.EncodeToString(outDigest[:]), hex.EncodeToString(globalSHA[:]))
	}

	b.SetBytes(set.raw)
	win.reportMerge(b)
	b.ReportMetric(float64(inputArrowPeak)/(1<<20), "input_arrow_peak_MiB")
	b.ReportMetric(float64(inputArrowEnd)/(1<<20), "input_arrow_end_MiB")
	b.ReportMetric(float64(set.rows), "src_rows")
	b.ReportMetric(float64(set.raw)/(1<<20), "src_raw_MiB")
	b.ReportMetric(float64(set.agg.compressed)/(1<<20), "src_comp_MiB")
	b.ReportMetric(float64(set.agg.uncompressed)/(1<<20), "src_uncomp_MiB")
	b.ReportMetric(float64(set.agg.footer)/(1<<10), "src_footer_KiB")
	b.ReportMetric(float64(set.agg.rowGroups), "src_rowgroups")
	b.ReportMetric(float64(set.agg.maxRGUncomp)/(1<<20), "src_max_rg_uncomp_MiB")
	b.ReportMetric(float64(set.agg.maxRGComp)/(1<<20), "src_max_rg_comp_MiB")
	b.ReportMetric(float64(outInfo.Size()), "out_bytes")
	b.ReportMetric(float64(inputArrowPeak)/float64(n)/(1<<20), "input_arrow_peak_per_src_MiB")
	b.ReportMetric(float64(win.peakHeap)/float64(n)/(1<<20), "heap_peak_per_src_MiB")

	b.Logf("upload-memory: fanin=%d variant=%s batch=%d buffered=%t rows=%d raw=%d files=%d avg_file_raw=%d out_bytes=%d out_digest_sha256=%s wall=%s cpu=%s heap_start=%d heap_peak=%d heap_end=%d rss_start=%d rss_peak=%d rss_end=%d input_arrow_peak=%d gc=%d pause=%s",
		n, v.name, v.batch, v.buffered, stats.Rows, set.raw, n, set.raw/int64(n), outInfo.Size(),
		hex.EncodeToString(outDigest[:]), win.wall, win.cpu, win.startHeap, win.peakHeap, win.endHeap,
		win.startRSS, win.peakRSS, win.endRSS, inputArrowPeak, win.gcCycles, win.gcPause)

	if err := os.Remove(outPath); err != nil {
		b.Logf("upload-memory: remove output: %v", err)
	}
}

// umKV is one decoded map entry for canonical digest ordering.
type umKV struct {
	key, value string
}

// umDigestOutput reads one merged file row by row and returns a SHA-256 over
// the canonical decoded row content (timestamp, line bytes, sorted label
// entries, sorted metadata entries, promoted values and row count).
//
// It deliberately does not hash the Parquet file's raw bytes: identical merges
// were observed to produce different Parquet byte layouts across repeated runs
// in the same process, so raw file SHA is not a stable equivalence witness.
// The decoded digest is the output contract. When verify is true it also
// asserts the full pinned-corpus identity: exact rows, ordered line bytes,
// timestamps, 16 instance labels, empty metadata and null promoted level.
func umDigestOutput(b *testing.B, set *umFixtureSet, path string, verify bool) [32]byte {
	b.Helper()
	f, err := os.Open(path)
	if err != nil {
		b.Fatalf("upload-memory: verify open: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			b.Errorf("upload-memory: verify close: %v", err)
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		b.Fatalf("upload-memory: verify stat: %v", err)
	}
	rd, err := NewReader(f, fi.Size())
	if err != nil {
		b.Fatalf("upload-memory: verify reader: %v", err)
	}
	defer func() {
		if err := rd.Close(); err != nil {
			b.Errorf("upload-memory: verify reader close: %v", err)
		}
	}()

	h := sha256.New()
	lineH := sha256.New()
	var rows int64
	scan := int64(0)
	var prev arrow.Timestamp
	var haveTS bool
	var first, last time.Time
	var instances map[string]struct{}
	if verify {
		instances = make(map[string]struct{}, umInstances)
	}
	for rec, rerr := range rd.Records(b.Context()) {
		if rerr != nil {
			b.Fatalf("upload-memory: verify records: %v", rerr)
		}
		if rec.NumCols() != 5 {
			b.Fatalf("upload-memory: verify record has %d columns, want 5", rec.NumCols())
		}
		ts := rec.Column(0).(*array.Timestamp)
		line := rec.Column(1).(*array.String)
		labels := rec.Column(2).(*array.Map)
		meta := rec.Column(3).(*array.Map)
		level := rec.Column(4).(*array.String)
		for i := range int(rec.NumRows()) {
			if ts.IsNull(i) {
				b.Fatalf("upload-memory: verify row %d has null %s", rows, schema.ColTS)
			}
			v := ts.Value(i)
			if haveTS && v <= prev {
				b.Fatalf("upload-memory: verify timestamp not strictly ascending at row %d: %d <= %d", rows, v, prev)
			}
			prev, haveTS = v, true
			if line.IsNull(i) {
				b.Fatalf("upload-memory: verify row %d has null %s", rows, schema.ColLine)
			}
			lineStr := line.Value(i)
			umDigestU64(h, uint64(v))
			umDigestString(h, lineStr)
			umDigestMap(b, h, labels, i)
			umDigestMap(b, h, meta, i)
			if level.IsNull(i) {
				umDigestU64(h, 0)
			} else {
				umDigestU64(h, 1)
				umDigestString(h, level.Value(i))
			}
			if verify {
				record := umNextAPI(&scan)
				t := schema.Time(int64(v))
				if want := umTimestamp(record); !t.Equal(want) {
					b.Fatalf("upload-memory: verify row %d timestamp %s, want %s", rows, t.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
				}
				if rows == 0 {
					first = t
				}
				last = t
				io.WriteString(lineH, lineStr)
				io.WriteString(lineH, "\n")
				instance := umInstance(record)
				instances[instance] = struct{}{}
				umVerifyLabels(b, labels, i, rows, instance)
				umVerifyEmptyMeta(b, meta, i, rows)
				if !level.IsNull(i) {
					b.Fatalf("upload-memory: verify row %d promoted level = %q, want null", rows, level.Value(i))
				}
			}
			rows++
		}
	}
	umDigestU64(h, uint64(rows))
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	if !verify {
		return digest
	}
	if rows != set.rows {
		b.Fatalf("upload-memory: verify read %d rows, want %d", rows, set.rows)
	}
	var gotLineSHA [32]byte
	copy(gotLineSHA[:], lineH.Sum(nil))
	if gotLineSHA != set.sha {
		b.Fatalf("upload-memory: verify line sha256=%s, want %s", hex.EncodeToString(gotLineSHA[:]), hex.EncodeToString(set.sha[:]))
	}
	if !first.Equal(set.first) || !last.Equal(set.last) {
		b.Fatalf("upload-memory: verify ts range = [%s..%s], want [%s..%s]",
			first.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano),
			set.first.Format(time.RFC3339Nano), set.last.Format(time.RFC3339Nano))
	}
	if len(instances) != umInstances {
		b.Fatalf("upload-memory: verify saw %d distinct instance labels, want %d", len(instances), umInstances)
	}
	b.Logf("upload-memory: verified output rows=%d line_sha256=%s digest=%s labels=%s=api+%s=0..%d meta=empty level=null",
		rows, hex.EncodeToString(gotLineSHA[:]), hex.EncodeToString(digest[:]),
		umStreamLabel, umInstanceLabel, umInstances-1)
	return digest
}

func umDigestU64(h hash.Hash, v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	h.Write(buf[:])
}

func umDigestString(h hash.Hash, s string) {
	umDigestU64(h, uint64(len(s)))
	io.WriteString(h, s)
}

// umDigestMap hashes a string map canonically (entries sorted by key), so map
// encoding order cannot change the decoded-output digest.
func umDigestMap(b *testing.B, h hash.Hash, m *array.Map, row int) {
	b.Helper()
	if m.IsNull(row) {
		umDigestU64(h, ^uint64(0))
		return
	}
	keys, ok := m.Keys().(*array.String)
	if !ok {
		b.Fatalf("upload-memory: verify map keys are %T", m.Keys())
	}
	items, ok := m.Items().(*array.String)
	if !ok {
		b.Fatalf("upload-memory: verify map values are %T", m.Items())
	}
	beg, end := m.ValueOffsets(row)
	pairs := make([]umKV, 0, end-beg)
	for i := int(beg); i < int(end); i++ {
		if keys.IsNull(i) || items.IsNull(i) {
			b.Fatalf("upload-memory: verify row %d has null map entry at %d", row, i)
		}
		pairs = append(pairs, umKV{key: keys.Value(i), value: items.Value(i)})
	}
	slices.SortFunc(pairs, func(a, c umKV) int { return strings.Compare(a.key, c.key) })
	umDigestU64(h, uint64(len(pairs)))
	for _, p := range pairs {
		umDigestString(h, p.key)
		umDigestString(h, p.value)
	}
}

func umVerifyLabels(b *testing.B, m *array.Map, row int, outRow int64, wantInstance string) {
	b.Helper()
	if m.IsNull(row) {
		b.Fatalf("upload-memory: verify row %d has null %s", outRow, schema.ColLabels)
	}
	beg, end := m.ValueOffsets(row)
	if end-beg != 2 {
		b.Fatalf("upload-memory: verify row %d has %d labels, want 2", outRow, end-beg)
	}
	keys, ok := m.Keys().(*array.String)
	if !ok {
		b.Fatalf("upload-memory: verify row %d label keys are %T", outRow, m.Keys())
	}
	items, ok := m.Items().(*array.String)
	if !ok {
		b.Fatalf("upload-memory: verify row %d label values are %T", outRow, m.Items())
	}
	got := make(map[string]string, 2)
	for i := int(beg); i < int(end); i++ {
		if keys.IsNull(i) || items.IsNull(i) {
			b.Fatalf("upload-memory: verify row %d has a null label entry", outRow)
		}
		got[keys.Value(i)] = items.Value(i)
	}
	if len(got) != 2 {
		b.Fatalf("upload-memory: verify row %d has labels %v, want exactly %q and %q", outRow, got, umInstanceLabel, umStreamLabel)
	}
	if got[umInstanceLabel] != wantInstance {
		b.Fatalf("upload-memory: verify row %d instance label = %q, want %q", outRow, got[umInstanceLabel], wantInstance)
	}
	if got[umStreamLabel] != umStreamValue {
		b.Fatalf("upload-memory: verify row %d %s label = %q, want %q", outRow, umStreamLabel, got[umStreamLabel], umStreamValue)
	}
}

func umVerifyEmptyMeta(b *testing.B, m *array.Map, row int, outRow int64) {
	b.Helper()
	if m.IsNull(row) {
		b.Fatalf("upload-memory: verify row %d has null %s", outRow, schema.ColMeta)
	}
	beg, end := m.ValueOffsets(row)
	if end != beg {
		b.Fatalf("upload-memory: verify row %d has %d metadata entries, want 0", outRow, end-beg)
	}
}

// --- profile and sampler helpers -------------------------------------------

// umWindowStats is one timed arm window.
type umWindowStats struct {
	wall      time.Duration
	cpu       time.Duration
	startHeap int64
	peakHeap  int64
	endHeap   int64
	startRSS  int64
	peakRSS   int64
	endRSS    int64
	gcCycles  uint32
	gcPause   time.Duration
	samples   int64
}

func (s umWindowStats) reportMerge(b *testing.B) {
	b.ReportMetric(s.wall.Seconds(), "merge_wall_s")
	b.ReportMetric(s.cpu.Seconds(), "merge_cpu_s")
	if s.wall > 0 {
		b.ReportMetric(100*s.cpu.Seconds()/s.wall.Seconds(), "merge_cpu_pct")
	}
	b.ReportMetric(float64(s.startHeap)/(1<<20), "heap_start_MiB")
	b.ReportMetric(float64(s.peakHeap)/(1<<20), "heap_peak_MiB")
	b.ReportMetric(float64(s.endHeap)/(1<<20), "heap_end_MiB")
	b.ReportMetric(float64(s.startRSS)/(1<<20), "rss_start_MiB")
	b.ReportMetric(float64(s.peakRSS)/(1<<20), "rss_peak_MiB")
	b.ReportMetric(float64(s.endRSS)/(1<<20), "rss_end_MiB")
	b.ReportMetric(float64(s.gcCycles), "gc_cycles")
	b.ReportMetric(s.gcPause.Seconds()*1000, "gc_pause_ms")
	b.ReportMetric(float64(s.samples), "samples")
}

// umSampler samples Go heap and process RSS while one merge arm runs. It
// starts after a pre-arm runtime.GC(), but RSS is process-wide: GC cannot
// guarantee that pages return to the OS, so callers must report the start
// snapshots and absolute peaks rather than treating peak-start as pure arm
// cost. Input Arrow bytes come from umTrackedAllocator, not from polling.
type umSampler struct {
	every time.Duration

	mu         sync.Mutex
	startHeap  int64
	peakHeap   int64
	endHeap    int64
	startRSS   int64
	peakRSS    int64
	endRSS     int64
	samples    int64
	startGC    uint32
	startPause uint64
	startCPU   time.Duration
	startWall  time.Time

	done    chan struct{}
	stopped chan struct{}
}

func umStartSampler(every time.Duration) *umSampler {
	if every <= 0 {
		every = 5 * time.Millisecond
	}
	s := &umSampler{
		every:     every,
		startCPU:  umCPUTime(),
		startWall: time.Now(),
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.startGC = ms.NumGC
	s.startPause = ms.PauseTotalNs
	s.startHeap = int64(ms.HeapAlloc)
	s.peakHeap = s.startHeap
	s.endHeap = s.startHeap
	s.startRSS = umRSSBytes()
	s.peakRSS = s.startRSS
	s.endRSS = s.startRSS
	go func() {
		defer close(s.stopped)
		ticker := time.NewTicker(s.every)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.sample()
			}
		}
	}()
	return s
}

func (s *umSampler) sample() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heap := int64(ms.HeapAlloc)
	rss := umRSSBytes()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peakHeap = max(s.peakHeap, heap)
	s.endHeap = heap
	s.peakRSS = max(s.peakRSS, rss)
	s.endRSS = rss
	s.samples++
}

// umEndpoint captures the measured wall and CPU endpoints. Callers take it
// before sampler join, final ReadMemStats, CPU-profile stop or heap-profile
// writing, so telemetry work is outside the reported window.
type umEndpoint struct {
	wall time.Duration
	cpu  time.Duration
}

func (s *umSampler) endpoint() umEndpoint {
	return umEndpoint{wall: time.Since(s.startWall), cpu: umCPUTime() - s.startCPU}
}

func (s *umSampler) stop(end umEndpoint) umWindowStats {
	close(s.done)
	<-s.stopped
	// Final sample after the caller has closed inputs, so the end values
	// describe the post-merge state. It is telemetry, not part of the
	// endpoint already captured by the caller.
	s.sample()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.mu.Lock()
	stats := umWindowStats{
		wall:      end.wall,
		cpu:       end.cpu,
		startHeap: s.startHeap,
		peakHeap:  s.peakHeap,
		endHeap:   s.endHeap,
		startRSS:  s.startRSS,
		peakRSS:   s.peakRSS,
		endRSS:    s.endRSS,
		gcCycles:  ms.NumGC - s.startGC,
		gcPause:   time.Duration(ms.PauseTotalNs - s.startPause),
		samples:   s.samples,
	}
	s.mu.Unlock()
	return stats
}

// umRSSBytes reads current resident-set size from /proc. Non-Linux processes
// report 0, which the benchmark logs rather than inventing a value.
func umRSSBytes() int64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || pages < 0 {
		return 0
	}
	return pages * int64(os.Getpagesize())
}

// umCPUTime returns cumulative process CPU time.
func umCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Sec+ru.Stime.Sec)*time.Second +
		time.Duration(ru.Utime.Usec+ru.Stime.Usec)*time.Microsecond
}

// umStartCPUProfile starts a per-arm CPU profile when dir is set.
func umStartCPUProfile(b *testing.B, dir, stem string) func() {
	b.Helper()
	if dir == "" {
		return func() {}
	}
	path := filepath.Join(dir, stem+".cpu.pprof")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("upload-memory: create cpu profile %s: %v", path, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		b.Fatalf("upload-memory: start cpu profile %s: %v", path, err)
	}
	b.Logf("upload-memory: cpu profile -> %s", path)
	stop := sync.OnceFunc(func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	})
	b.Cleanup(stop)
	return stop
}

// umWriteHeapProfile writes one heap snapshot when dir is set. A Go heap
// profile is cumulative allocation history, not a live in-use peak. The
// caller forces GC before the base and after the measured window; the end
// in-use view is post-GC/post-release, and the -alloc_space base diff includes
// small snapshot/profiler bookkeeping as unclassified overhead. Diff with:
//
//	go tool pprof -alloc_space -base <stem>.heap-base.pprof <stem>.heap-end.pprof
func umWriteHeapProfile(b *testing.B, dir, stem, phase string) {
	b.Helper()
	if dir == "" {
		return
	}
	path := filepath.Join(dir, stem+".heap-"+phase+".pprof")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("upload-memory: create heap profile %s: %v", path, err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		b.Fatalf("upload-memory: write heap profile %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("upload-memory: close heap profile %s: %v", path, err)
	}
	b.Logf("upload-memory: heap %s snapshot -> %s", phase, path)
}
