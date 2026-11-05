package ingest

// Parallel upload/flush crash matrix. The subprocess half starts this same
// test binary in a child mode, seeds deterministic stream/hour partitions,
// lets the real uploader or flusher reach a named durability boundary, records
// that the boundary was reached (fsynced operation log and ack file), and then
// blocks until the parent SIGKILLs it. The parent resumes the child, reopens
// the local store, SQLite catalog and filesystem object store, and proves:
//
//   - the target stage was really reached, the child was killed (not a failed
//     setup), and at least two jobs ran concurrently;
//   - every data and sidecar object key was Put at most once across both
//     processes, catalog keys are unique, and a HasSidecar flag always follows
//     the sidecar Put for that key;
//   - catalog-visible rows are exactly the seeded rows once, orphan objects
//     only contain expected rows and stay within the manifest key-rotation
//     budget, and no upload receipt or pending source survives;
//   - a catalog row tombstoned after the AddFiles crash is never resurrected
//     and its sources are retired without another Put.
//
// TestParallelCrashSamePartitionMatrix is the narrow variant of that matrix:
// it seeds four disjoint MergeMax-bounded chunks of ONE tenant/stream/hour
// partition, so the overlap proof covers two or more receipts of the same
// partition instead of two independent partitions. Its last subtest caps the
// worker pool at one and proves the overlap check rejects a serial pass.
//
// The gated half needs PARKANA_TEST_PARALLEL_CRASH=1 because it forks test
// binaries and SIGKILLs them; it is intended for the Boat crash run. The
// in-process fault tests below run on every `go test`: they inject ambiguous
// Put/AddFiles failures and filesystem sync failures, where a process SIGKILL
// cannot prove the machine-crash ordering. The approved Concurrency defaults
// (flush and upload) are deliberately not set: the serial implementations must
// fail the gated overlap proof, and the parallel implementations pass it
// without test-local configuration.

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// parallelCrashGate enables the forking/SIGKILL matrix.
	parallelCrashGate = "PARKANA_TEST_PARALLEL_CRASH"
	// parallelCrashChildEnv marks a child test process of the matrix.
	parallelCrashChildEnv = "PARKANA_PARALLEL_CRASH_CHILD"
	// parallelCrashWorkEnv is the parent-created work directory.
	parallelCrashWorkEnv = "PARKANA_PARALLEL_CRASH_WORK"
	// parallelCrashModeEnv selects crash or resume child behavior.
	parallelCrashModeEnv = "PARKANA_PARALLEL_CRASH_MODE"
	// parallelCrashStageEnv names the durability boundary to crash at.
	parallelCrashStageEnv = "PARKANA_PARALLEL_CRASH_STAGE"
	// parallelCrashTargetEnv optionally restricts a stage to one stream.
	parallelCrashTargetEnv = "PARKANA_PARALLEL_CRASH_TARGET"
	// parallelCrashBarrierTimeout bounds the proof that two workers overlap.
	parallelCrashBarrierTimeout = 5 * time.Second
	// parallelCrashAckTimeout bounds child setup and the stage ack.
	parallelCrashAckTimeout = 60 * time.Second
	// parallelCrashResumeTimeout bounds one resumed uploader child.
	parallelCrashResumeTimeout = 90 * time.Second
	// parallelCrashVariantEnv selects the narrow same-partition dataset.
	parallelCrashVariantEnv = "PARKANA_PARALLEL_CRASH_VARIANT"
	// parallelCrashConcurrencyEnv overrides the child uploader's worker cap.
	// The serial negative control sets it to 1.
	parallelCrashConcurrencyEnv = "PARKANA_PARALLEL_CRASH_CONCURRENCY"
	// parallelCrashVariantSamePartition seeds one tenant/stream/hour as
	// disjoint MergeMax-bounded chunks instead of the wide multi-partition
	// dataset.
	parallelCrashVariantSamePartition = "same-partition"
	// parallelCrashSamePartitionParts is how many source parts the narrow
	// variant seeds; with MergeMax equal to one part's bytes, each part becomes
	// its own receipt.
	parallelCrashSamePartitionParts = 4
	// parallelCrashSamePartitionConcurrency is the positive variant's explicit
	// worker cap: one worker per same-partition receipt, so all chunks can be
	// in flight together.
	parallelCrashSamePartitionConcurrency = parallelCrashSamePartitionParts
)

// Crash stage names. The flush stages run before any upload work; upload
// stages exercise the existing reservation → merge → Put → AddFiles →
// published → cleanup transitions.
const (
	parallelCrashStageFlushRename   = "local-flush-part-rename"
	parallelCrashStageFlushManifest = "local-flush-manifest"
	parallelCrashStageReservation   = "upload-reservation"
	parallelCrashStageFinalize      = "upload-merge-finalize"
	parallelCrashStageDataPut       = "upload-data-put"
	parallelCrashStageSidecarPut    = "upload-sidecar-put"
	parallelCrashStageCatalogAdd    = "upload-catalog-add"
	parallelCrashStagePublished     = "upload-published-marker"
	parallelCrashStageCleanup       = "upload-cleanup"
)

// parallelCrashGroup is one deterministic stream/hour partition seeded by the
// child. Two source files per group make the uploader merge work non-trivial.
type parallelCrashGroup struct {
	tenantID tenant.ID
	stream   string
	hour     time.Time
}

// parallelCrashExpectedRow is one seeded row the parent expects to find once.
type parallelCrashExpectedRow struct {
	ts     time.Time
	group  parallelCrashGroup
	line   string
	tenant tenant.ID
	stream string
	labels map[string]string
	meta   map[string]string
}

// parallelCrashLabels is the stored label map of every seeded row.
func parallelCrashLabels(g parallelCrashGroup) map[string]string {
	return map[string]string{"job": "parallel-crash", "stream": g.stream}
}

// parallelCrashMeta is a deterministic structured-metadata value carried by
// every seeded row, so exact-entry verification is not limited to ts+line.
func parallelCrashMeta(g parallelCrashGroup, part, row int) map[string]string {
	return map[string]string{
		"crash_id": fmt.Sprintf("%s-%s-%d-%d", g.tenantID, g.stream, part, row),
	}
}

// parallelCrashPlan returns the fixed matrix dataset: six groups for the
// default tenant (three streams across two hours) and two groups for a second
// tenant. Every group has two source parts with three rows each.
func parallelCrashPlan() []parallelCrashGroup {
	base := time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)
	var groups []parallelCrashGroup
	for _, stream := range []string{"api", "web", "auth"} {
		for h := range 2 {
			groups = append(groups, parallelCrashGroup{
				tenantID: tenant.Default, stream: stream, hour: base.Add(time.Duration(h) * time.Hour),
			})
		}
	}
	for _, stream := range []string{"api", "web"} {
		groups = append(groups, parallelCrashGroup{
			tenantID: tenant.ID("other"), stream: stream, hour: base,
		})
	}
	return groups
}

// parallelCrashExpected returns every seeded row keyed by its unique line.
func parallelCrashExpected(groups []parallelCrashGroup) map[string]parallelCrashExpectedRow {
	out := make(map[string]parallelCrashExpectedRow, len(groups)*6)
	for _, g := range groups {
		for part := range 2 {
			for row := range 3 {
				ts := g.hour.Add(time.Duration(part)*10*time.Minute + time.Duration(row)*time.Minute)
				line := fmt.Sprintf("crash tenant=%s stream=%s hour=%s part=%d row=%d",
					g.tenantID, g.stream, g.hour.Format(time.RFC3339), part, row)
				out[line] = parallelCrashExpectedRow{
					ts: ts, group: g, line: line, tenant: g.tenantID, stream: g.stream,
					labels: parallelCrashLabels(g), meta: parallelCrashMeta(g, part, row),
				}
			}
		}
	}
	return out
}

// parallelCrashRows builds the sorted rows of one source part.
func parallelCrashRows(g parallelCrashGroup, part int) []uploadRow {
	rows := make([]uploadRow, 0, 3)
	for row := range 3 {
		ts := g.hour.Add(time.Duration(part)*10*time.Minute + time.Duration(row)*time.Minute)
		rows = append(rows, uploadRow{
			ts:     ts,
			line:   fmt.Sprintf("crash tenant=%s stream=%s hour=%s part=%d row=%d", g.tenantID, g.stream, g.hour.Format(time.RFC3339), part, row),
			labels: parallelCrashLabels(g),
			meta:   parallelCrashMeta(g, part, row),
		})
	}
	return rows
}

// parallelCrashRecord builds one Arrow batch for rows.
func parallelCrashRecord(t *testing.T, rows []uploadRow) arrow.RecordBatch {
	t.Helper()
	s, err := schema.ForStream(nil)
	if err != nil {
		t.Fatalf("schema.ForStream: %v", err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	defer b.Release()
	tsB := b.Field(0).(*array.TimestampBuilder)
	lineB := b.Field(1).(*array.StringBuilder)
	labelsB := b.Field(2).(*array.MapBuilder)
	metaB := b.Field(3).(*array.MapBuilder)
	for _, r := range rows {
		tsB.Append(arrow.Timestamp(schema.TS(r.ts)))
		lineB.Append(r.line)
		parallelCrashAppendMap(labelsB, r.labels)
		parallelCrashAppendMap(metaB, r.meta)
	}
	return b.NewRecordBatch()
}

func parallelCrashAppendMap(mb *array.MapBuilder, m map[string]string) {
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		kb.Append(k)
		ib.Append(m[k])
	}
}

// parallelCrashTracker records concurrent pipeline operations. A barrier at
// the start of each merge/writer makes the overlap proof deterministic: the
// serial implementation waits for the timeout and finishes with max == 1,
// while a bounded pool releases both workers immediately.
type parallelCrashTracker struct {
	active atomic.Int64
	max    atomic.Int64
	bar    *parallelCrashBarrier
}

func newParallelCrashTracker(enabled bool) *parallelCrashTracker {
	t := new(parallelCrashTracker)
	if enabled {
		t.bar = newParallelCrashBarrier(2, parallelCrashBarrierTimeout)
	}
	return t
}

// enter records one active operation and reports whether a concurrent worker
// arrived before the barrier timeout. Only a true return proves real overlap;
// the serial path returns false after the timeout.
func (t *parallelCrashTracker) enter(wait bool) bool {
	n := t.active.Add(1)
	for {
		old := t.max.Load()
		if n <= old || t.max.CompareAndSwap(old, n) {
			break
		}
	}
	if wait && t.bar != nil {
		return t.bar.wait()
	}
	return false
}

func (t *parallelCrashTracker) leave() { t.active.Add(-1) }

func (t *parallelCrashTracker) maxActive() int { return int(t.max.Load()) }

// parallelCrashBarrier releases when need callers arrive. On timeout it opens
// the gate for everyone so a serial implementation fails fast without
// multiplying the timeout by every part.
type parallelCrashBarrier struct {
	mu       sync.Mutex
	need     int
	arrived  int
	open     bool
	released chan struct{}
	timeout  time.Duration
}

func newParallelCrashBarrier(need int, timeout time.Duration) *parallelCrashBarrier {
	return &parallelCrashBarrier{
		need: need, released: make(chan struct{}), timeout: timeout,
	}
}

func (b *parallelCrashBarrier) wait() bool {
	b.mu.Lock()
	b.arrived++
	concurrent := b.arrived >= b.need
	if concurrent && !b.open {
		b.open = true
		close(b.released)
	}
	open, ch, timeout := b.open, b.released, b.timeout
	b.mu.Unlock()
	if open {
		return concurrent
	}
	select {
	case <-ch:
		return concurrent
	case <-time.After(timeout):
		b.mu.Lock()
		if !b.open {
			b.open = true
			close(b.released)
		}
		b.mu.Unlock()
		return false
	}
}

// parallelCrashEvent is one append-only operation-log record. The log is
// fsynced before a stage ack, so a record that names a Put proves the write
// happened before the crash.
type parallelCrashEvent struct {
	Phase     string `json:"phase"`
	Kind      string `json:"kind"`
	Stage     string `json:"stage,omitempty"`
	Key       string `json:"key,omitempty"`
	Sidecar   string `json:"sidecar,omitempty"`
	Tenant    string `json:"tenant,omitempty"`
	Stream    string `json:"stream,omitempty"`
	Upload    string `json:"upload,omitempty"`
	Err       string `json:"err,omitempty"`
	Active    int    `json:"active,omitempty"`
	MaxActive int    `json:"maxActive,omitempty"`
}

// parallelCrashLog is the child's durable operation log.
type parallelCrashLog struct {
	mu    sync.Mutex
	file  *os.File
	phase string
}

func openParallelCrashLog(pathName, phase string) (*parallelCrashLog, error) {
	f, err := os.OpenFile(pathName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open operation log: %w", err)
	}
	return &parallelCrashLog{file: f, phase: phase}, nil
}

func (l *parallelCrashLog) event(e parallelCrashEvent) error {
	if l == nil {
		return nil
	}
	e.Phase = l.phase
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode operation event: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.file.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write operation event: %w", err)
	}
	if n != len(data)+1 {
		return fmt.Errorf("write operation event: %w", io.ErrShortWrite)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync operation event: %w", err)
	}
	return nil
}

func (l *parallelCrashLog) close() error {
	if l == nil {
		return nil
	}
	return l.file.Close()
}

// parallelCrashAck is the parent-visible proof that the crash stage ran.
type parallelCrashAck struct {
	Stage     string    `json:"stage"`
	PID       int       `json:"pid"`
	Active    int       `json:"active"`
	MaxActive int       `json:"maxActive"`
	Detail    string    `json:"detail,omitempty"`
	Tenant    string    `json:"tenant,omitempty"`
	Stream    string    `json:"stream,omitempty"`
	Upload    string    `json:"upload,omitempty"`
	Key       string    `json:"key,omitempty"`
	Hour      time.Time `json:"hour,omitzero"`
}

// parallelCrashArm decides whether a seam invocation is the target stage and,
// when it is, writes the ack and blocks forever for the parent's SIGKILL.
type parallelCrashArm struct {
	stage  string
	target string
	armed  atomic.Bool
	fired  atomic.Bool

	log     *parallelCrashLog
	tracker *parallelCrashTracker
	ackPath string
}

func (a *parallelCrashArm) fire(detail, tenantID, stream, upload, key string, hour time.Time) error {
	if a == nil || !a.armed.Load() || a.stage == "" {
		return nil
	}
	if a.target != "" && a.target != stream {
		return nil
	}
	if !a.fired.CompareAndSwap(false, true) {
		return nil
	}
	ack := parallelCrashAck{
		Stage: a.stage, PID: os.Getpid(), Active: int(a.tracker.active.Load()),
		MaxActive: a.tracker.maxActive(), Detail: detail,
		Tenant: tenantID, Stream: stream, Upload: upload, Key: key, Hour: hour,
	}
	if err := a.log.event(parallelCrashEvent{
		Kind: "stage", Stage: a.stage, Key: key, Tenant: tenantID, Stream: stream,
		Upload: upload, Active: ack.Active, MaxActive: ack.MaxActive,
	}); err != nil {
		return fmt.Errorf("record stage evidence: %w", err)
	}
	if err := parallelCrashWriteAck(a.ackPath, ack); err != nil {
		return fmt.Errorf("write stage ack: %w", err)
	}
	// The parent now has durable proof that the stage ran. Block until it
	// SIGKILLs this process; no cleanup or transition may follow the ack.
	for {
		time.Sleep(time.Hour)
	}
}

// parallelCrashSeams installs the process-local wrappers once. Every wrapped
// call delegates to the real implementation; only the named boundary arms.
type parallelCrashSeams struct {
	arm     *parallelCrashArm
	log     *parallelCrashLog
	tracker *parallelCrashTracker

	// mergeSeq orders concurrent merge entries. The first upload worker past
	// the overlap barrier stays blocked inside its merge for the rest of the
	// process, giving every upload target stage a deterministic second job in
	// flight without holding a tenant manifest lock. writerSeq does the same
	// for the local-flush part-rename boundary, where no upload exists yet.
	mergeSeq  atomic.Int64
	writerSeq atomic.Int64
}

func (s *parallelCrashSeams) ops(base localFileOps) localFileOps {
	out := base
	out.rename = func(root *os.Root, oldName, newName string) error {
		s.tracker.enter(false)
		defer s.tracker.leave()
		if err := base.rename(root, oldName, newName); err != nil {
			return err
		}
		if s.arm.stage == parallelCrashStageFlushRename && s.arm.armed.Load() && parallelCrashIsPartPath(newName) {
			return s.arm.fire(newName, "", "", "", "", time.Time{})
		}
		return nil
	}
	out.syncDir = func(root *os.Root, name string) error {
		s.tracker.enter(false)
		defer s.tracker.leave()
		if err := base.syncDir(root, name); err != nil {
			return err
		}
		if s.arm == nil || !s.arm.armed.Load() {
			return nil
		}
		switch {
		case name == "." && s.arm.stage == parallelCrashStageFlushManifest:
			if m, err := parallelCrashProbeManifest(root); err == nil && m.Frontier.Last > 0 {
				return s.arm.fire("manifest committed", "", "", "", "", time.Time{})
			}
		case name == uploadsDir && s.arm.stage == parallelCrashStageFinalize:
			if name := parallelCrashFinalizedTemp(root); name != "" {
				return s.arm.fire(name, "", "", strings.TrimSuffix(path.Base(name), parquetExt), "", time.Time{})
			}
		case name == "." && s.arm.stage == parallelCrashStagePublished:
			if m, err := parallelCrashProbeManifest(root); err == nil {
				for _, u := range m.Uploads {
					if u.Stage == UploadPublished {
						return s.arm.fire("published marker committed", string(u.Tenant), u.Stream, u.ID.String(), u.Key, u.Hour)
					}
				}
			}
		}
		return nil
	}
	out.remove = func(root *os.Root, name string) error {
		s.tracker.enter(false)
		defer s.tracker.leave()
		err := base.remove(root, name)
		if err != nil || s.arm == nil || !s.arm.armed.Load() || s.arm.stage != parallelCrashStageCleanup {
			return err
		}
		if m, mErr := parallelCrashProbeManifest(root); mErr == nil {
			for _, u := range m.Uploads {
				if u.Stage == UploadPublished {
					return s.arm.fire(name, string(u.Tenant), u.Stream, u.ID.String(), u.Key, u.Hour)
				}
			}
		}
		return err
	}
	return out
}

// parallelCrashIsPartPath reports whether name is a committed local part path
// (not a manifest, upload temp or quarantine path).
func parallelCrashIsPartPath(name string) bool {
	if !strings.HasSuffix(name, parquetExt) || strings.HasPrefix(name, ".") {
		return false
	}
	parts := strings.Split(name, "/")
	return len(parts) >= 6
}

// parallelCrashProbeManifest decodes the fields the crash seams inspect. It
// is a read-only projection, never a second manifest implementation.
type parallelCrashProbe struct {
	Frontier struct {
		Last uint64 `json:"last"`
	} `json:"frontier"`
	Uploads []struct {
		ID     uuid.UUID   `json:"id"`
		Tenant tenant.ID   `json:"tenant"`
		Stream string      `json:"stream"`
		Hour   time.Time   `json:"hour"`
		Key    string      `json:"key"`
		Stage  UploadStage `json:"stage"`
	} `json:"uploads"`
}

func parallelCrashProbeManifest(root *os.Root) (parallelCrashProbe, error) {
	data, err := root.ReadFile(manifestName)
	if err != nil {
		return parallelCrashProbe{}, err
	}
	var m parallelCrashProbe
	if err := json.Unmarshal(data, &m); err != nil {
		return parallelCrashProbe{}, err
	}
	return m, nil
}

// parallelCrashFinalizedTemp returns one finalized upload temp path, if any.
func parallelCrashFinalizedTemp(root *os.Root) string {
	entries, err := fs.ReadDir(root.FS(), uploadsDir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), parquetExt) {
			return uploadsDir + "/" + e.Name()
		}
	}
	return ""
}

// parallelCrashWriteAck publishes the ack atomically: temp, fsync, rename,
// directory fsync.
func parallelCrashWriteAck(pathName string, ack parallelCrashAck) error {
	data, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("encode ack: %w", err)
	}
	dir := filepath.Dir(pathName)
	tmp := pathName + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create ack: %w", err)
	}
	if n, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write ack: %w", err)
	} else if n != len(data) {
		f.Close()
		return fmt.Errorf("write ack: %w", io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync ack: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close ack: %w", err)
	}
	if err := os.Rename(tmp, pathName); err != nil {
		return fmt.Errorf("rename ack: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open ack directory: %w", err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("sync ack directory: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close ack directory: %w", err)
	}
	return nil
}

// parallelCrashStore logs and arms every object Put.
type parallelCrashStore struct {
	objstore.Store
	seams *parallelCrashSeams
}

func (s *parallelCrashStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	sidecar := strings.HasSuffix(key, objstore.ExtSidecar)
	stream, tenantID := parallelCrashKeyStream(key)
	if err := s.seams.log.event(parallelCrashEvent{
		Kind: "put-start", Key: key, Sidecar: key, Tenant: string(tenantID), Stream: stream,
	}); err != nil {
		return fmt.Errorf("record Put start: %w", err)
	}
	s.seams.tracker.enter(false)
	defer s.seams.tracker.leave()
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		if logErr := s.seams.log.event(parallelCrashEvent{
			Kind: "put-done", Key: key, Sidecar: key, Tenant: string(tenantID), Stream: stream, Err: err.Error(),
		}); logErr != nil {
			return errors.Join(err, fmt.Errorf("record Put failure: %w", logErr))
		}
		return err
	}
	if err := s.seams.log.event(parallelCrashEvent{
		Kind: "put-done", Key: key, Sidecar: key, Tenant: string(tenantID), Stream: stream,
	}); err != nil {
		return fmt.Errorf("record Put result: %w", err)
	}
	switch s.seams.arm.stage {
	case parallelCrashStageDataPut:
		if !sidecar {
			return s.seams.arm.fire("data object Put returned", string(tenantID), stream, "", key, time.Time{})
		}
	case parallelCrashStageSidecarPut:
		if sidecar {
			return s.seams.arm.fire("sidecar Put returned", string(tenantID), stream, "", key, time.Time{})
		}
	}
	return nil
}

// parallelCrashKeyStream derives tenant and stream from a canonical data or
// sidecar key: {tenant}/logs/{stream}/yyyy/....
func parallelCrashKeyStream(key string) (string, tenant.ID) {
	parts := strings.Split(strings.TrimSuffix(key, objstore.ExtSidecar), "/")
	if len(parts) < 3 || parts[1] != "logs" {
		return "", ""
	}
	return parts[2], tenant.ID(parts[0])
}

// parallelCrashCatalog logs and arms AddFiles.
type parallelCrashCatalog struct {
	catalog.Catalog
	seams *parallelCrashSeams
}

func (c *parallelCrashCatalog) AddFiles(ctx context.Context, files ...catalog.File) error {
	c.seams.tracker.enter(false)
	defer c.seams.tracker.leave()
	for _, f := range files {
		if err := c.seams.log.event(parallelCrashEvent{
			Kind: "addfiles-start", Key: f.Key, Tenant: string(f.Tenant), Stream: f.Stream,
		}); err != nil {
			return fmt.Errorf("record AddFiles start: %w", err)
		}
	}
	err := c.Catalog.AddFiles(ctx, files...)
	for _, f := range files {
		if logErr := c.seams.log.event(parallelCrashEvent{
			Kind: "addfiles-done", Key: f.Key, Tenant: string(f.Tenant), Stream: f.Stream, Err: parallelCrashErrString(err),
		}); logErr != nil {
			return fmt.Errorf("record AddFiles result: %w", logErr)
		}
	}
	if err == nil && c.seams.arm.stage == parallelCrashStageCatalogAdd {
		for _, f := range files {
			hour, _ := parallelCrashHourFromKey(f.Key)
			if fireErr := c.seams.arm.fire("catalog AddFiles committed", string(f.Tenant), f.Stream, "", f.Key, hour); fireErr != nil {
				return fireErr
			}
		}
	}
	return err
}

// parallelCrashHourFromKey extracts the hour partition from a canonical data
// key: {tenant}/logs/{stream}/yyyy/mm/dd/hh/<uuid>.parquet.
func parallelCrashHourFromKey(key string) (time.Time, error) {
	parts := strings.Split(key, "/")
	if len(parts) < 8 {
		return time.Time{}, fmt.Errorf("key %q has no hour partition", key)
	}
	return time.Parse("2006/01/02/15", strings.Join(parts[len(parts)-5:len(parts)-1], "/"))
}

func parallelCrashErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// parallelCrashMerge wraps storage.Merge with the tracker barrier, a holder
// worker and the reservation-stage trigger. The trigger runs before the real
// merge, so the durable receipt exists but no temp has been finalized.
//
// The first merge that observes a genuinely concurrent worker stays blocked
// for the rest of the process. It holds no tenant lock, so the target job can
// still complete its merge, finalize, Put, catalog write and cleanup while
// that holder remains a second in-flight job. The serial path never gets a
// holder: its barrier times out, the target fires with active == 1, and the
// parent rejects it.
func (s *parallelCrashSeams) merge(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
	concurrent := s.tracker.enter(true)
	if concurrent && s.mergeSeq.Add(1) == 1 {
		if err := s.log.event(parallelCrashEvent{Kind: "holder-start", Active: int(s.tracker.active.Load()), MaxActive: s.tracker.maxActive()}); err != nil {
			s.tracker.leave()
			return storage.FileStats{}, fmt.Errorf("record holder evidence: %w", err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	defer s.tracker.leave()
	if s.arm.stage == parallelCrashStageReservation {
		if err := s.arm.fire("upload receipt reserved", "", "", "", "", time.Time{}); err != nil {
			return storage.FileStats{}, err
		}
	}
	return storage.Merge(ctx, w, inputs, promoted)
}

// parallelCrashWriter tracks flush part writers and arms the flush rename
// stage; the dirsync/manifest seam lives in parallelCrashSeams.ops.
func (s *parallelCrashSeams) writer(base WriterFactory) WriterFactory {
	return func(w io.Writer, sc *arrow.Schema, promoted []string) (ParquetWriter, error) {
		concurrent := s.tracker.enter(true)
		if s.arm.stage == parallelCrashStageFlushRename && concurrent && s.writerSeq.Add(1) == 1 {
			if err := s.log.event(parallelCrashEvent{Kind: "holder-start", Active: int(s.tracker.active.Load()), MaxActive: s.tracker.maxActive()}); err != nil {
				s.tracker.leave()
				return nil, fmt.Errorf("record holder evidence: %w", err)
			}
			// Keep one part writer in flight while another part reaches its
			// post-rename boundary. No tenant lock is held here.
			for {
				time.Sleep(time.Hour)
			}
		}
		pw, err := base(w, sc, promoted)
		if err != nil {
			s.tracker.leave()
			return nil, err
		}
		return &parallelCrashTrackedWriter{ParquetWriter: pw, tracker: s.tracker}, nil
	}
}

type parallelCrashTrackedWriter struct {
	ParquetWriter
	tracker *parallelCrashTracker
	once    sync.Once
}

func (w *parallelCrashTrackedWriter) Close() (storage.FileStats, error) {
	stats, err := w.ParquetWriter.Close()
	w.once.Do(w.tracker.leave)
	return stats, err
}

// parallelCrashGeneration is a sealed generation fixture for the flush crash
// stages.
type parallelCrashGeneration struct {
	tenantID tenant.ID
	parts    []Part
}

func (g *parallelCrashGeneration) Tenant() tenant.ID          { return g.tenantID }
func (g *parallelCrashGeneration) Frontier() (uint64, uint64) { return 1, 1 }
func (g *parallelCrashGeneration) Parts(context.Context) ([]Part, error) {
	return slices.Clone(g.parts), nil
}

// parallelCrashSeed publishes the deterministic upload matrix dataset into
// the child's local store. It is idempotent: an existing non-empty manifest is
// left untouched so a resumed child never reseeds or duplicates rows.
func parallelCrashSeed(t *testing.T, ctx context.Context, local *LocalStore) error {
	t.Helper()
	plan := parallelCrashPlan()
	byTenant := make(map[tenant.ID][]parallelCrashGroup)
	for _, g := range plan {
		byTenant[g.tenantID] = append(byTenant[g.tenantID], g)
	}
	for _, id := range slices.Sorted(maps.Keys(byTenant)) {
		ts, err := local.Tenant(ctx, id)
		if err != nil {
			return err
		}
		files, err := ts.Files(ctx)
		if err != nil {
			return err
		}
		if len(files) > 0 {
			continue
		}
		var committed []LocalFile
		for _, g := range byTenant[id] {
			for part := range 2 {
				rec := parallelCrashRecord(t, parallelCrashRows(g, part))
				lf, err := ts.writeRecord(ctx, g.stream, g.hour, rec, defaultWriterFactory, nil)
				rec.Release()
				if err != nil {
					return fmt.Errorf("seed %s/%s/%s: %w", g.tenantID, g.stream, g.hour, err)
				}
				if lf.ID != uuid.Nil() {
					committed = append(committed, lf)
				}
			}
		}
		if err := ts.Publish(ctx, Frontier{First: 1, Last: 1}, committed); err != nil {
			return fmt.Errorf("seed publish %s: %w", id, err)
		}
	}
	return nil
}

// parallelCrashSamePartitionGroup is the narrow matrix partition. Its hour is
// outside parallelCrashPlan's two hours, so the variant never shares a
// partition with the wide dataset.
func parallelCrashSamePartitionGroup() parallelCrashGroup {
	return parallelCrashGroup{
		tenantID: tenant.Default,
		stream:   "api",
		hour:     time.Date(2026, 3, 14, 14, 0, 0, 0, time.UTC),
	}
}

// parallelCrashSamePartitionExpected returns every row the narrow variant
// seeds: parallelCrashSamePartitionParts parts of three rows each.
func parallelCrashSamePartitionExpected() map[string]parallelCrashExpectedRow {
	g := parallelCrashSamePartitionGroup()
	out := make(map[string]parallelCrashExpectedRow, parallelCrashSamePartitionParts*3)
	for part := range parallelCrashSamePartitionParts {
		for row := range 3 {
			ts := g.hour.Add(time.Duration(part)*10*time.Minute + time.Duration(row)*time.Minute)
			line := fmt.Sprintf("crash tenant=%s stream=%s hour=%s part=%d row=%d",
				g.tenantID, g.stream, g.hour.Format(time.RFC3339), part, row)
			out[line] = parallelCrashExpectedRow{
				ts: ts, group: g, line: line, tenant: g.tenantID, stream: g.stream,
				labels: parallelCrashLabels(g), meta: parallelCrashMeta(g, part, row),
			}
		}
	}
	return out
}

// parallelCrashSeedSamePartition publishes the narrow variant's source parts
// into one tenant/stream/hour partition and returns the largest part size. A
// MergeMax equal to that size makes every part its own chunk as long as no
// pair of parts fits under the cap: selectSources always takes the first file
// and then stops once the next file would exceed MergeMax. So one pass
// reserves parallelCrashSamePartitionParts independent receipts in the same
// partition. The returned size is computed from the seeded files rather than
// hardcoded, keeping the chunking exact if the writer's byte layout changes.
func parallelCrashSeedSamePartition(t *testing.T, ctx context.Context, local *LocalStore) (int64, error) {
	t.Helper()
	g := parallelCrashSamePartitionGroup()
	ts, err := local.Tenant(ctx, g.tenantID)
	if err != nil {
		return 0, fmt.Errorf("open tenant store: %w", err)
	}
	files, err := ts.Files(ctx)
	if err != nil {
		return 0, fmt.Errorf("list committed files: %w", err)
	}
	if len(files) > 0 {
		return 0, fmt.Errorf("same-partition seed found %d committed file(s)", len(files))
	}
	var committed []LocalFile
	var mergeMax int64
	for part := range parallelCrashSamePartitionParts {
		rec := parallelCrashRecord(t, parallelCrashRows(g, part))
		lf, err := ts.writeRecord(ctx, g.stream, g.hour, rec, defaultWriterFactory, nil)
		rec.Release()
		if err != nil {
			return 0, fmt.Errorf("seed %s/%s/%s part %d: %w", g.tenantID, g.stream, g.hour, part, err)
		}
		if lf.ID != uuid.Nil() {
			committed = append(committed, lf)
			mergeMax = max(mergeMax, lf.Bytes)
		}
	}
	if err := ts.Publish(ctx, Frontier{First: 1, Last: 1}, committed); err != nil {
		return 0, fmt.Errorf("publish same-partition seed: %w", err)
	}
	if mergeMax <= 0 {
		return 0, errors.New("same-partition seed wrote no source bytes")
	}
	if len(committed) != parallelCrashSamePartitionParts {
		return 0, fmt.Errorf("same-partition seed committed %d part(s), want %d",
			len(committed), parallelCrashSamePartitionParts)
	}
	// Every pair must exceed MergeMax, otherwise selectSources could fuse two
	// parts into one chunk and the overlap proof would silently cover fewer
	// receipts than the fixture claims.
	for _, lf := range committed {
		if lf.Bytes <= mergeMax/2 {
			return 0, fmt.Errorf("same-partition part %s is %d bytes, want more than half the %d-byte MergeMax",
				lf.ID, lf.Bytes, mergeMax)
		}
	}
	return mergeMax, nil
}

// parallelCrashChild is the forked test entry point. Without the child env it
// is a no-op so a normal package run never forks.
func TestParallelCrashChild(t *testing.T) {
	if os.Getenv(parallelCrashChildEnv) != "1" {
		t.Skip("not a parallel crash child")
	}
	work := os.Getenv(parallelCrashWorkEnv)
	mode := os.Getenv(parallelCrashModeEnv)
	stage := os.Getenv(parallelCrashStageEnv)
	target := os.Getenv(parallelCrashTargetEnv)
	if work == "" || mode == "" {
		t.Fatalf("child env missing: work=%q mode=%q", work, mode)
	}
	ctx := t.Context()
	if err := parallelCrashChildRun(t, ctx, work, mode, stage, target); err != nil {
		t.Fatalf("parallel crash child: %v", err)
	}
}

func parallelCrashChildRun(t *testing.T, ctx context.Context, work, mode, stage, target string) error {
	t.Helper()
	dataDir := filepath.Join(work, "data")
	objDir := filepath.Join(work, "objects")
	dsn := filepath.Join(work, "catalog.sqlite")
	logPath := filepath.Join(work, "ops.jsonl")
	ackPath := filepath.Join(work, "ack.json")
	donePath := filepath.Join(work, "done.json")

	log, err := openParallelCrashLog(logPath, mode)
	if err != nil {
		return err
	}
	defer func() { _ = log.close() }()

	tracker := newParallelCrashTracker(mode == "crash")
	arm := &parallelCrashArm{
		stage: stage, target: target, log: log, tracker: tracker, ackPath: ackPath,
	}
	seams := &parallelCrashSeams{arm: arm, log: log, tracker: tracker}

	if mode == "resume" {
		return parallelCrashResume(ctx, dataDir, objDir, dsn, donePath, seams)
	}
	if mode != "crash" {
		return fmt.Errorf("unknown child mode %q", mode)
	}
	if os.Getenv(parallelCrashVariantEnv) == parallelCrashVariantSamePartition {
		concurrency, err := parallelCrashChildConcurrency()
		if err != nil {
			return err
		}
		return parallelCrashRunSamePartitionCrash(t, ctx, dataDir, objDir, dsn, stage, seams, arm, concurrency)
	}
	if stage == parallelCrashStageFlushRename || stage == parallelCrashStageFlushManifest {
		return parallelCrashRunFlushCrash(t, ctx, dataDir, stage, seams, arm)
	}
	return parallelCrashRunUploadCrash(t, ctx, dataDir, objDir, dsn, stage, seams, arm)
}

// parallelCrashChildConcurrency reads the optional worker-cap override. Zero
// means the uploader's default; the serial negative control sets one.
func parallelCrashChildConcurrency() (int, error) {
	raw := os.Getenv(parallelCrashConcurrencyEnv)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid %s=%q", parallelCrashConcurrencyEnv, raw)
	}
	return n, nil
}

func parallelCrashRunUploadCrash(t *testing.T, ctx context.Context, dataDir, objDir, dsn, stage string, seams *parallelCrashSeams, arm *parallelCrashArm) error {
	t.Helper()
	local, err := openLocal(ctx, dataDir, seams.ops(defaultLocalOps()))
	if err != nil {
		return err
	}
	defer func() { _ = local.Close() }()
	if err := parallelCrashSeed(t, ctx, local); err != nil {
		return err
	}
	arm.armed.Store(true)

	objects, err := objstore.NewFS(objDir)
	if err != nil {
		return err
	}
	defer func() { _ = objects.Close() }()
	cat, err := catalog.NewSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	u, err := NewUploader(UploaderOptions{
		Local: local, Objects: &parallelCrashStore{Store: objects, seams: seams},
		Catalog:     &parallelCrashCatalog{Catalog: cat, seams: seams},
		PromotedFor: func(string) []string { return nil },
		MinBytes:    1,
		MaxAge:      time.Hour,
		MergeMax:    8 << 20,
		Now:         time.Now,
	})
	if err != nil {
		return err
	}
	u.merge = seams.merge
	_, runErr := u.RunOnce(ctx)
	return fmt.Errorf("stage %q was not reached (RunOnce error: %v)", stage, runErr)
}

// parallelCrashRunSamePartitionCrash seeds the narrow one-partition variant,
// arms the stage and runs one real uploader pass with an explicit worker cap.
// It returns an error only when the stage never fired; the parent then fails
// with the child log instead of accepting a pass that completed normally.
func parallelCrashRunSamePartitionCrash(t *testing.T, ctx context.Context, dataDir, objDir, dsn, stage string, seams *parallelCrashSeams, arm *parallelCrashArm, concurrency int) error {
	t.Helper()
	local, err := openLocal(ctx, dataDir, seams.ops(defaultLocalOps()))
	if err != nil {
		return err
	}
	defer func() { _ = local.Close() }()
	mergeMax, err := parallelCrashSeedSamePartition(t, ctx, local)
	if err != nil {
		return err
	}
	arm.armed.Store(true)

	objects, err := objstore.NewFS(objDir)
	if err != nil {
		return err
	}
	defer func() { _ = objects.Close() }()
	cat, err := catalog.NewSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	u, err := NewUploader(UploaderOptions{
		Local: local, Objects: &parallelCrashStore{Store: objects, seams: seams},
		Catalog:     &parallelCrashCatalog{Catalog: cat, seams: seams},
		PromotedFor: func(string) []string { return nil },
		MinBytes:    1,
		MaxAge:      time.Hour,
		MergeMax:    mergeMax,
		Concurrency: concurrency,
		Now:         time.Now,
	})
	if err != nil {
		return err
	}
	u.merge = seams.merge
	_, runErr := u.RunOnce(ctx)
	return fmt.Errorf("stage %q was not reached (RunOnce error: %v)", stage, runErr)
}

func parallelCrashRunFlushCrash(t *testing.T, ctx context.Context, dataDir, stage string, seams *parallelCrashSeams, arm *parallelCrashArm) error {
	t.Helper()
	local, err := openLocal(ctx, dataDir, seams.ops(defaultLocalOps()))
	if err != nil {
		return err
	}
	defer func() { _ = local.Close() }()

	gen := &parallelCrashGeneration{tenantID: tenant.Default}
	base := time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)
	for _, stream := range []string{"api", "web", "auth", "audit"} {
		for h := range 2 {
			g := parallelCrashGroup{tenantID: tenant.Default, stream: stream, hour: base.Add(time.Duration(h) * time.Hour)}
			rec := parallelCrashRecord(t, parallelCrashRows(g, 0))
			gen.parts = append(gen.parts, Part{Stream: stream, Hour: g.hour, Record: rec})
		}
	}
	defer func() {
		for _, p := range gen.parts {
			p.Record.Release()
		}
	}()

	arm.armed.Store(true)
	fl, err := NewFlusher(FlushOptions{Store: local, NewWriter: seams.writer(defaultWriterFactory)})
	if err != nil {
		return err
	}
	flushErr := fl.Flush(ctx, gen)
	return fmt.Errorf("stage %q was not reached (Flush error: %v)", stage, flushErr)
}

// parallelCrashResume runs uploader passes until no receipt or pending source
// remains, then records the settled state. It never seeds or arms seams.
func parallelCrashResume(ctx context.Context, dataDir, objDir, dsn, donePath string, seams *parallelCrashSeams) error {
	local, err := OpenLocal(ctx, dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = local.Close() }()
	objects, err := objstore.NewFS(objDir)
	if err != nil {
		return err
	}
	defer func() { _ = objects.Close() }()
	cat, err := catalog.NewSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	u, err := NewUploader(UploaderOptions{
		Local: local, Objects: &parallelCrashStore{Store: objects, seams: seams},
		Catalog:     &parallelCrashCatalog{Catalog: cat, seams: seams},
		PromotedFor: func(string) []string { return nil },
		MinBytes:    1,
		MaxAge:      time.Hour,
		MergeMax:    8 << 20,
		Now:         time.Now,
	})
	if err != nil {
		return err
	}
	for pass := range 10 {
		report, err := u.RunOnce(ctx)
		if err != nil {
			return fmt.Errorf("resume pass %d: %w", pass, err)
		}
		settled, err := parallelCrashSettled(ctx, local)
		if err != nil {
			return err
		}
		if settled {
			data, _ := json.Marshal(report)
			return os.WriteFile(donePath, data, 0o644)
		}
	}
	return errors.New("uploader did not settle within 10 passes")
}

func parallelCrashSettled(ctx context.Context, local *LocalStore) (bool, error) {
	ids, err := local.Tenants(ctx)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		ts, err := local.Tenant(ctx, id)
		if err != nil {
			return false, err
		}
		uploads, err := ts.Uploads(ctx)
		if err != nil {
			return false, err
		}
		pending, err := ts.PendingFiles(ctx)
		if err != nil {
			return false, err
		}
		if len(uploads) != 0 || len(pending) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// --- parent-side subprocess driver -----------------------------------------

type parallelCrashBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *parallelCrashBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *parallelCrashBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type parallelCrashProcess struct {
	cmd  *exec.Cmd
	log  *parallelCrashBuffer
	done chan struct{}
	mu   sync.Mutex
	err  error
	once sync.Once
}

func startParallelCrashChild(t *testing.T, work, mode, stage, target string, extraEnv ...string) *parallelCrashProcess {
	t.Helper()
	p := &parallelCrashProcess{log: new(parallelCrashBuffer), done: make(chan struct{})}
	args := []string{"-test.run=^TestParallelCrashChild$", "-test.count=1"}
	p.cmd = exec.CommandContext(t.Context(), os.Args[0], args...)
	p.cmd.Env = append(os.Environ(),
		parallelCrashChildEnv+"=1",
		parallelCrashWorkEnv+"="+work,
		parallelCrashModeEnv+"="+mode,
		parallelCrashStageEnv+"="+stage,
		parallelCrashTargetEnv+"="+target,
	)
	p.cmd.Env = append(p.cmd.Env, extraEnv...)
	p.cmd.Stdout = p.log
	p.cmd.Stderr = p.log
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start crash child: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	}()
	t.Cleanup(func() { p.kill(t) })
	return p
}

func (p *parallelCrashProcess) kill(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			t.Errorf("parallel crash child did not exit after SIGKILL")
		}
	})
}

func (p *parallelCrashProcess) exited() (bool, error) {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return true, p.err
	default:
		return false, nil
	}
}

func (p *parallelCrashProcess) logTail() string {
	s := p.log.String()
	const limit = 16 << 10
	if len(s) > limit {
		s = s[len(s)-limit:]
	}
	return strings.TrimRight(s, "\n")
}

func (p *parallelCrashProcess) waitAck(t *testing.T, pathName string) (parallelCrashAck, []parallelCrashEvent) {
	t.Helper()
	deadline := time.Now().Add(parallelCrashAckTimeout)
	for time.Now().Before(deadline) {
		if done, err := p.exited(); done {
			t.Fatalf("child exited before the crash stage was reached: %v\nlog:\n%s", err, p.logTail())
		}
		data, err := os.ReadFile(pathName)
		if err == nil {
			var ack parallelCrashAck
			if err := json.Unmarshal(data, &ack); err != nil {
				t.Fatalf("decode ack %s: %v", pathName, err)
			}
			return ack, readParallelCrashEvents(t, filepath.Join(filepath.Dir(pathName), "ops.jsonl"))
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("read ack: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stage ack %s not written within %s\nlog:\n%s", pathName, parallelCrashAckTimeout, p.logTail())
	return parallelCrashAck{}, nil
}

func readParallelCrashEvents(t *testing.T, pathName string) []parallelCrashEvent {
	t.Helper()
	data, err := os.ReadFile(pathName)
	if err != nil {
		t.Fatalf("read operation log: %v", err)
	}
	events, err := parseParallelCrashEvents(data)
	if err != nil {
		t.Fatalf("parse operation log: %v", err)
	}
	return events
}

// parseParallelCrashEvents accepts only complete newline-terminated records.
// SIGKILL can tear the final write, so a trailing non-newline-terminated
// fragment is ignored; any malformed complete line is corruption and fails.
func parseParallelCrashEvents(data []byte) ([]parallelCrashEvent, error) {
	complete := data
	if len(data) > 0 && data[len(data)-1] != '\n' {
		if i := bytes.LastIndexByte(data, '\n'); i >= 0 {
			complete = data[:i+1]
		} else {
			complete = nil
		}
	}
	var out []parallelCrashEvent
	for line := range bytes.SplitSeq(complete, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var e parallelCrashEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("malformed complete event %q: %w", line, err)
		}
		out = append(out, e)
	}
	return out, nil
}

func (p *parallelCrashProcess) assertKilled(t *testing.T) {
	t.Helper()
	p.kill(t)
	if p.cmd.ProcessState == nil {
		t.Fatal("child process state is nil after wait")
	}
	if code := p.cmd.ProcessState.ExitCode(); code != -1 {
		t.Fatalf("child exited with code %d; the crash matrix requires SIGKILL, not a normal exit\nlog:\n%s", code, p.logTail())
	}
}

// TestParallelCrashMatrix is the gated upload crash matrix. Each subtest
// proves the target stage was reached and two jobs overlapped before the
// SIGKILL, then resumes and verifies exact rows, unique keys, single Puts,
// sidecar ordering and the orphan budget.
func TestParallelCrashMatrix(t *testing.T) {
	if os.Getenv(parallelCrashGate) == "" {
		t.Skipf("set %s=1 to run the forking SIGKILL crash matrix", parallelCrashGate)
	}
	if runtime.GOOS == "windows" {
		t.Skip("the crash matrix uses SIGKILL process semantics")
	}
	for _, stage := range []string{
		parallelCrashStageReservation,
		parallelCrashStageFinalize,
		parallelCrashStageDataPut,
		parallelCrashStageSidecarPut,
		parallelCrashStageCatalogAdd,
		parallelCrashStagePublished,
		parallelCrashStageCleanup,
	} {
		t.Run(stage, func(t *testing.T) {
			work := t.TempDir()
			child := startParallelCrashChild(t, work, "crash", stage, "")
			ack, events := child.waitAck(t, filepath.Join(work, "ack.json"))
			child.assertKilled(t)
			parallelCrashAssertAck(t, stage, ack, events)

			resume := startParallelCrashChild(t, work, "resume", "", "")
			parallelCrashWaitResume(t, resume, work)

			expected := parallelCrashExpected(parallelCrashPlan())
			parallelCrashVerifyStore(t, work, expected, nil)
		})
	}
}

// TestParallelCrashSamePartitionMatrix is the gated same-partition variant of
// TestParallelCrashMatrix. One tenant/stream/hour is seeded as four disjoint
// one-file MergeMax chunks, so a single uploader pass holds four receipts of
// the SAME partition. Each stage kills the child while at least two of those
// receipts are in flight, then the parent resumes and proves the durable
// state: every object key is Put at most once, catalog-visible rows are the
// exact seeded entries (labels and structured metadata included), catalog keys
// are unique, and no pending source or open receipt survives. The last subtest
// is the serial negative control: an explicit one-worker cap cannot overlap
// and must not produce overlap evidence.
func TestParallelCrashSamePartitionMatrix(t *testing.T) {
	if os.Getenv(parallelCrashGate) == "" {
		t.Skipf("set %s=1 to run the forking SIGKILL crash matrix", parallelCrashGate)
	}
	if runtime.GOOS == "windows" {
		t.Skip("the crash matrix uses SIGKILL process semantics")
	}
	variant := []string{
		parallelCrashVariantEnv + "=" + parallelCrashVariantSamePartition,
		parallelCrashConcurrencyEnv + "=" + strconv.Itoa(parallelCrashSamePartitionConcurrency),
	}
	for _, stage := range []string{
		parallelCrashStageDataPut,
		parallelCrashStageCatalogAdd,
		parallelCrashStageCleanup,
	} {
		t.Run(stage, func(t *testing.T) {
			work := t.TempDir()
			child := startParallelCrashChild(t, work, "crash", stage, "", variant...)
			ack, events := child.waitAck(t, filepath.Join(work, "ack.json"))
			child.assertKilled(t)
			parallelCrashAssertAck(t, stage, ack, events)
			if g := parallelCrashSamePartitionGroup(); ack.Tenant != string(g.tenantID) || ack.Stream != g.stream {
				t.Fatalf("stage %s fired on %s/%s, want the narrow partition %s/%s",
					stage, ack.Tenant, ack.Stream, g.tenantID, g.stream)
			}

			resume := startParallelCrashChild(t, work, "resume", "", "")
			parallelCrashWaitResume(t, resume, work)

			parallelCrashVerifyStore(t, work, parallelCrashSamePartitionExpected(), nil)
		})
	}
	t.Run("serial cap cannot overlap", func(t *testing.T) {
		work := t.TempDir()
		child := startParallelCrashChild(t, work, "crash", parallelCrashStageDataPut, "",
			parallelCrashVariantEnv+"="+parallelCrashVariantSamePartition,
			parallelCrashConcurrencyEnv+"=1")
		ack, events := child.waitAck(t, filepath.Join(work, "ack.json"))
		child.assertKilled(t)
		if ack.Stage != parallelCrashStageDataPut {
			t.Fatalf("ack stage = %q, want %q", ack.Stage, parallelCrashStageDataPut)
		}
		if ack.MaxActive >= 2 || ack.Active >= 2 {
			t.Fatalf("one-worker cap reached the stage with max=%d active=%d; the overlap proof would wrongly accept a serial pass",
				ack.MaxActive, ack.Active)
		}
		var stageEvent *parallelCrashEvent
		for i := range events {
			if events[i].Kind == "stage" && events[i].Stage == parallelCrashStageDataPut {
				stageEvent = &events[i]
				break
			}
		}
		if stageEvent == nil {
			t.Fatalf("operation log has no fsynced stage event for %q", parallelCrashStageDataPut)
		}
		if stageEvent.MaxActive >= 2 || stageEvent.Active >= 2 {
			t.Fatalf("serial stage event %+v claims overlap", stageEvent)
		}

		resume := startParallelCrashChild(t, work, "resume", "", "")
		parallelCrashWaitResume(t, resume, work)
		parallelCrashVerifyStore(t, work, parallelCrashSamePartitionExpected(), nil)
	})
}

// TestParallelCrashTombstoneAfterCatalogAdd kills the uploader after
// AddFiles committed, tombstones the row and deletes its object as a
// compactor would, then proves the resumed uploader neither re-Puts nor
// resurrects the data.
func TestParallelCrashTombstoneAfterCatalogAdd(t *testing.T) {
	if os.Getenv(parallelCrashGate) == "" {
		t.Skipf("set %s=1 to run the forking SIGKILL crash matrix", parallelCrashGate)
	}
	if runtime.GOOS == "windows" {
		t.Skip("the crash matrix uses SIGKILL process semantics")
	}
	work := t.TempDir()
	child := startParallelCrashChild(t, work, "crash", parallelCrashStageCatalogAdd, "api")
	ack, events := child.waitAck(t, filepath.Join(work, "ack.json"))
	child.assertKilled(t)
	parallelCrashAssertAck(t, parallelCrashStageCatalogAdd, ack, events)
	if ack.Key == "" || ack.Stream == "" || ack.Tenant == "" || ack.Hour.IsZero() {
		t.Fatalf("catalog crash ack lacks identity: %+v", ack)
	}

	ctx := t.Context()
	cat, err := catalog.NewSQLite(filepath.Join(work, "catalog.sqlite"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	if err := cat.ReplaceFiles(ctx, tenant.ID(ack.Tenant), []string{ack.Key}, nil); err != nil {
		t.Fatalf("tombstone %s: %v", ack.Key, err)
	}
	if err := cat.Close(); err != nil {
		t.Fatalf("close catalog: %v", err)
	}
	objects, err := objstore.NewFS(filepath.Join(work, "objects"))
	if err != nil {
		t.Fatalf("open objects: %v", err)
	}
	if err := objects.Delete(ctx, objstore.SidecarKey(ack.Key)); err != nil {
		t.Fatalf("delete sidecar: %v", err)
	}
	if err := objects.Delete(ctx, ack.Key); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatalf("close objects: %v", err)
	}

	resume := startParallelCrashChild(t, work, "resume", "", "")
	parallelCrashWaitResume(t, resume, work)

	expected := parallelCrashExpected(parallelCrashPlan())
	for line, row := range expected {
		if row.tenant == tenant.ID(ack.Tenant) && row.stream == ack.Stream && row.group.hour.Equal(ack.Hour) {
			delete(expected, line)
		}
	}
	parallelCrashVerifyStore(t, work, expected, &parallelCrashTombstone{
		key: ack.Key, tenant: tenant.ID(ack.Tenant),
	})
}

// TestParallelCrashFlushMatrix covers the local flush boundaries that have no
// upload work yet: a part renamed but not committed is quarantined after the
// crash, while a committed generation manifest resumes into exactly-once
// upload.
func TestParallelCrashFlushMatrix(t *testing.T) {
	if os.Getenv(parallelCrashGate) == "" {
		t.Skipf("set %s=1 to run the forking SIGKILL crash matrix", parallelCrashGate)
	}
	if runtime.GOOS == "windows" {
		t.Skip("the crash matrix uses SIGKILL process semantics")
	}
	base := time.Date(2026, 3, 14, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		stage    string
		expected map[string]parallelCrashExpectedRow
		empty    bool
	}{
		{stage: parallelCrashStageFlushRename, empty: true},
		{stage: parallelCrashStageFlushManifest, expected: parallelCrashFlushExpected(base)},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			work := t.TempDir()
			child := startParallelCrashChild(t, work, "crash", tc.stage, "")
			ack, events := child.waitAck(t, filepath.Join(work, "ack.json"))
			child.assertKilled(t)
			parallelCrashAssertAck(t, tc.stage, ack, events)

			resume := startParallelCrashChild(t, work, "resume", "", "")
			parallelCrashWaitResume(t, resume, work)

			if tc.empty {
				parallelCrashAssertNoCommittedRows(t, work)
				return
			}
			parallelCrashVerifyStore(t, work, tc.expected, nil)
		})
	}
}

func parallelCrashFlushExpected(base time.Time) map[string]parallelCrashExpectedRow {
	out := make(map[string]parallelCrashExpectedRow)
	for _, stream := range []string{"api", "web", "auth", "audit"} {
		for h := range 2 {
			g := parallelCrashGroup{tenantID: tenant.Default, stream: stream, hour: base.Add(time.Duration(h) * time.Hour)}
			for row := range 3 {
				ts := g.hour.Add(time.Duration(row) * time.Minute)
				line := fmt.Sprintf("crash tenant=%s stream=%s hour=%s part=0 row=%d", g.tenantID, stream, g.hour.Format(time.RFC3339), row)
				out[line] = parallelCrashExpectedRow{
					ts: ts, group: g, line: line, tenant: g.tenantID, stream: stream,
					labels: parallelCrashLabels(g), meta: parallelCrashMeta(g, 0, row),
				}
			}
		}
	}
	return out
}

// parallelCrashStageNeedsCurrentOverlap distinguishes stages where a second
// worker must still be in flight at the ack from a flush-manifest crash, which
// necessarily happens after all part writers joined. For that one stage the
// historical maximum is the correct proof; every other stage keeps a holder
// worker blocked until SIGKILL and asserts the current count.
func parallelCrashStageNeedsCurrentOverlap(stage string) bool {
	return stage != parallelCrashStageFlushManifest
}

func parallelCrashAssertAck(t *testing.T, stage string, ack parallelCrashAck, events []parallelCrashEvent) {
	t.Helper()
	if ack.Stage != stage {
		t.Fatalf("ack stage = %q, want %q (ack %+v)", ack.Stage, stage, ack)
	}
	if ack.MaxActive < 2 {
		t.Fatalf("stage %s reached with max %d concurrent jobs; the target ran on the serial path", stage, ack.MaxActive)
	}
	if parallelCrashStageNeedsCurrentOverlap(stage) && ack.Active < 2 {
		t.Fatalf("stage %s ack has only %d job(s) in flight; the target had no independent second job at SIGKILL", stage, ack.Active)
	}
	if ack.PID == 0 {
		t.Fatalf("ack has no child PID: %+v", ack)
	}
	var stageEvent *parallelCrashEvent
	for i := range events {
		if events[i].Kind == "stage" && events[i].Stage == stage {
			stageEvent = &events[i]
			break
		}
	}
	if stageEvent == nil {
		t.Fatalf("operation log has no fsynced stage event for %q", stage)
	}
	if stageEvent.MaxActive < 2 {
		t.Fatalf("stage event %+v proves only %d concurrent jobs", stageEvent, stageEvent.MaxActive)
	}
	if parallelCrashStageNeedsCurrentOverlap(stage) && stageEvent.Active < 2 {
		t.Fatalf("stage event %+v proves no second job in flight at the ack", stageEvent)
	}
}

func parallelCrashWaitResume(t *testing.T, p *parallelCrashProcess, work string) {
	t.Helper()
	deadline := time.Now().Add(parallelCrashResumeTimeout)
	for time.Now().Before(deadline) {
		if done, err := p.exited(); done {
			if err != nil {
				t.Fatalf("resume child failed: %v\nlog:\n%s", err, p.logTail())
			}
			if _, err := os.Stat(filepath.Join(work, "done.json")); err != nil {
				t.Fatalf("resume child exited without a done marker: %v\nlog:\n%s", err, p.logTail())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("resume child did not settle within %s\nlog:\n%s", parallelCrashResumeTimeout, p.logTail())
}

// --- parent verification ----------------------------------------------------

type parallelCrashTombstone struct {
	key    string
	tenant tenant.ID
}

// parallelCrashCheckRow proves one stored row is the exact expected entry:
// tenant/stream/hour partition, microsecond timestamp, line, labels and
// structured metadata.
func parallelCrashCheckRow(t *testing.T, key string, row parallelCrashReadRow, exp parallelCrashExpectedRow) {
	t.Helper()
	stream, tenantID := parallelCrashKeyStream(key)
	if tenantID != exp.tenant || stream != exp.stream {
		t.Fatalf("object %s is in %s/%s, want %s/%s for row %q",
			key, tenantID, stream, exp.tenant, exp.stream, row.line)
	}
	hour, err := parallelCrashHourFromKey(key)
	if err != nil {
		t.Fatalf("object %s: %v", key, err)
	}
	if !hour.Equal(exp.group.hour) {
		t.Fatalf("object %s hour = %s, want %s", key, hour, exp.group.hour)
	}
	if row.line != exp.line {
		t.Fatalf("row line = %q, want %q", row.line, exp.line)
	}
	if row.ts != exp.ts.UnixMicro() {
		t.Fatalf("row %q ts=%d, want %d", row.line, row.ts, exp.ts.UnixMicro())
	}
	if got := schema.Time(row.ts).UTC().Truncate(time.Hour); !got.Equal(exp.group.hour) {
		t.Fatalf("row %q _ts hour = %s, want %s", row.line, got, exp.group.hour)
	}
	if !maps.Equal(row.labels, exp.labels) {
		t.Fatalf("row %q labels = %v, want %v", row.line, row.labels, exp.labels)
	}
	if !maps.Equal(row.meta, exp.meta) {
		t.Fatalf("row %q meta = %v, want %v", row.line, row.meta, exp.meta)
	}
}

// parallelCrashVerifyStore reopens the local store, catalog and objects and
// checks the settled durable state.
func parallelCrashVerifyStore(t *testing.T, work string, expected map[string]parallelCrashExpectedRow, tomb *parallelCrashTombstone) {
	t.Helper()
	ctx := t.Context()
	dataDir, objDir := filepath.Join(work, "data"), filepath.Join(work, "objects")
	local, err := OpenLocal(ctx, dataDir)
	if err != nil {
		t.Fatalf("reopen local store: %v", err)
	}
	defer func() { _ = local.Close() }()

	ids, err := local.Tenants(ctx)
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	rotations := make(map[tenant.ID]uint64, len(ids))
	for _, id := range ids {
		ts, err := local.Tenant(ctx, id)
		if err != nil {
			t.Fatalf("tenant %s: %v", id, err)
		}
		uploads, err := ts.Uploads(ctx)
		if err != nil {
			t.Fatalf("tenant %s uploads: %v", id, err)
		}
		pending, err := ts.PendingFiles(ctx)
		if err != nil {
			t.Fatalf("tenant %s pending: %v", id, err)
		}
		if len(uploads) != 0 {
			t.Fatalf("tenant %s has %d open upload receipt(s): %+v", id, len(uploads), uploads)
		}
		if len(pending) != 0 {
			t.Fatalf("tenant %s has %d pending local source file(s)", id, len(pending))
		}
		r, err := ts.KeyRotations(ctx)
		if err != nil {
			t.Fatalf("tenant %s rotations: %v", id, err)
		}
		rotations[id] = r
	}

	cat, err := catalog.NewSQLite(filepath.Join(work, "catalog.sqlite"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	all := make(map[string]catalog.File)
	for _, id := range parallelCrashCatalogTenants(ids) {
		for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: id, IncludeDeleted: true}) {
			if err != nil {
				t.Fatalf("list catalog for %s: %v", id, err)
			}
			if _, dup := all[f.Key]; dup {
				t.Fatalf("catalog lists key %s twice", f.Key)
			}
			all[f.Key] = f
		}
	}
	live := make(map[string]catalog.File)
	for key, f := range all {
		if !f.Deleted() {
			live[key] = f
		}
	}
	if tomb != nil {
		row, ok := all[tomb.key]
		if !ok {
			t.Fatalf("tombstoned catalog row %s disappeared", tomb.key)
		}
		if !row.Deleted() {
			t.Fatalf("tombstoned catalog row %s was resurrected: %+v", tomb.key, row)
		}
		if _, ok := live[tomb.key]; ok {
			t.Fatalf("tombstoned row %s is still live", tomb.key)
		}
	}

	objects, err := objstore.NewFS(objDir)
	if err != nil {
		t.Fatalf("open objects: %v", err)
	}
	defer func() { _ = objects.Close() }()

	physical, err := parallelCrashListObjects(objDir)
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	seen := make(map[string]bool, len(expected))
	for key, f := range live {
		rows, err := parallelCrashReadObject(ctx, objects, f.Tenant, key)
		if err != nil {
			t.Fatalf("read live object %s: %v", key, err)
		}
		if int64(len(rows)) != f.Rows {
			t.Fatalf("catalog row %s claims %d rows, object holds %d", key, f.Rows, len(rows))
		}
		if f.Bytes != parallelCrashObjectSize(t, objects, key) {
			t.Fatalf("catalog row %s byte size differs from its object", key)
		}
		sidecarErr := parallelCrashStat(ctx, objects, objstore.SidecarKey(key))
		if f.HasSidecar != (sidecarErr == nil) {
			t.Fatalf("catalog row %s HasSidecar=%v, sidecar stat err=%v", key, f.HasSidecar, sidecarErr)
		}
		if len(rows) > 0 {
			if f.MinTS.UnixMicro() != rows[0].ts || f.MaxTS.UnixMicro() != rows[len(rows)-1].ts {
				t.Fatalf("catalog row %s timestamp bounds do not match its object", key)
			}
		}
		for _, row := range rows {
			exp, ok := expected[row.line]
			if !ok {
				t.Fatalf("live object %s holds unknown row %q", key, row.line)
			}
			if f.Tenant != exp.tenant || f.Stream != exp.stream {
				t.Fatalf("catalog row %s tenant/stream = %s/%s, want %s/%s",
					key, f.Tenant, f.Stream, exp.tenant, exp.stream)
			}
			parallelCrashCheckRow(t, key, row, exp)
			if seen[row.line] {
				t.Fatalf("live row %q appears more than once", row.line)
			}
			seen[row.line] = true
		}
	}
	var missing []string
	for line := range expected {
		if !seen[line] {
			missing = append(missing, line)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		if len(missing) > 5 {
			missing = append(missing[:5], fmt.Sprintf("... and %d more", len(missing)-5))
		}
		t.Fatalf("catalog-visible rows are incomplete: missing %v", missing)
	}

	orphansByTenant := make(map[tenant.ID]int)
	for _, key := range physical {
		if _, ok := live[key]; ok {
			continue
		}
		id := parallelCrashKeyTenant(key)
		orphansByTenant[id]++
		rows, err := parallelCrashReadObject(ctx, objects, id, key)
		if err != nil {
			t.Fatalf("read orphan object %s: %v", key, err)
		}
		for _, row := range rows {
			exp, ok := expected[row.line]
			if !ok {
				t.Fatalf("orphan object %s holds unknown row %q", key, row.line)
			}
			parallelCrashCheckRow(t, key, row, exp)
		}
	}
	for id, n := range orphansByTenant {
		if uint64(n) > rotations[id] {
			t.Fatalf("tenant %s has %d orphan object(s) but only %d key rotation(s)", id, n, rotations[id])
		}
	}

	// Every physical data/sidecar key must have exactly one Put start across
	// both child phases, and a live HasSidecar must follow the sidecar Put and
	// precede AddFiles.
	events := readParallelCrashEvents(t, filepath.Join(work, "ops.jsonl"))
	putStarts := make(map[string]int)
	putDone := make(map[string]int)
	lastSidecarDone := make(map[string]int)
	lastAddStart := make(map[string]int)
	for i, e := range events {
		switch e.Kind {
		case "put-start":
			putStarts[e.Key]++
		case "put-done":
			if e.Err == "" {
				putDone[e.Key]++
				if strings.HasSuffix(e.Key, objstore.ExtSidecar) {
					lastSidecarDone[strings.TrimSuffix(e.Key, objstore.ExtSidecar)+objstore.ExtData] = i
				}
			}
		case "addfiles-start":
			lastAddStart[e.Key] = i
		}
	}
	for key, n := range putStarts {
		if n != 1 {
			t.Fatalf("object key %s has %d Put starts; object keys are immutable", key, n)
		}
	}
	for _, key := range physical {
		if putStarts[key] != 1 {
			t.Fatalf("physical object %s has %d recorded Put start(s), want exactly 1", key, putStarts[key])
		}
		if putDone[key] > 1 {
			t.Fatalf("physical object %s has %d completed Puts, object keys are immutable", key, putDone[key])
		}
		if _, ok := live[key]; ok && putDone[key] != 1 {
			t.Fatalf("live object %s has %d completed Puts, want exactly 1 after AddFiles", key, putDone[key])
		}
	}
	for key, f := range live {
		if !f.HasSidecar {
			continue
		}
		sidecarIndex, ok := lastSidecarDone[key]
		if !ok {
			t.Fatalf("live row %s claims a sidecar but no sidecar Put was recorded", key)
		}
		addIndex, ok := lastAddStart[key]
		if !ok {
			t.Fatalf("live row %s has no recorded AddFiles", key)
		}
		if sidecarIndex > addIndex {
			t.Fatalf("live row %s was cataloged before its sidecar Put", key)
		}
	}
}

// parallelCrashCatalogTenants returns every tenant whose catalog rows must be
// inspected: the reopened local tenants plus the deterministic plan tenants.
func parallelCrashCatalogTenants(ids []tenant.ID) []tenant.ID {
	set := make(map[tenant.ID]bool, len(ids)+2)
	for _, id := range ids {
		set[id] = true
	}
	for _, g := range parallelCrashPlan() {
		set[g.tenantID] = true
	}
	return slices.Sorted(maps.Keys(set))
}

func parallelCrashAssertNoCommittedRows(t *testing.T, work string) {
	t.Helper()
	ctx := t.Context()
	local, err := OpenLocal(ctx, filepath.Join(work, "data"))
	if err != nil {
		t.Fatalf("reopen local store: %v", err)
	}
	defer func() { _ = local.Close() }()
	ids, err := local.Tenants(ctx)
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	for _, id := range ids {
		ts, err := local.Tenant(ctx, id)
		if err != nil {
			t.Fatalf("tenant %s: %v", id, err)
		}
		files, err := ts.Files(ctx)
		if err != nil {
			t.Fatalf("tenant %s files: %v", id, err)
		}
		if len(files) != 0 {
			t.Fatalf("tenant %s has %d committed local file(s) after an uncommitted crash", id, len(files))
		}
	}
	cat, err := catalog.NewSQLite(filepath.Join(work, "catalog.sqlite"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()
	for _, id := range parallelCrashCatalogTenants(ids) {
		for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: id, IncludeDeleted: true}) {
			if err != nil {
				t.Fatalf("list catalog for %s: %v", id, err)
			}
			t.Fatalf("uncommitted crash left catalog row %s", f.Key)
		}
	}
	quarantined := 0
	root, err := os.OpenRoot(filepath.Join(work, "data", "parquet"))
	if err == nil {
		_ = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.Contains(p, quarantineDir) && strings.HasSuffix(p, objstore.ExtData) {
				quarantined++
			}
			return nil
		})
		_ = root.Close()
	}
	if quarantined == 0 {
		t.Fatalf("uncommitted part was neither committed nor quarantined")
	}
}

type parallelCrashReadRow struct {
	ts     int64
	line   string
	labels map[string]string
	meta   map[string]string
}

func parallelCrashReadObject(ctx context.Context, objects objstore.Store, id tenant.ID, key string) ([]parallelCrashReadRow, error) {
	or, err := storage.NewObjectReader(ctx, objects, id, key)
	if err != nil {
		return nil, err
	}
	rd, err := storage.NewReader(or, or.Size())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rd.Close() }()
	var out []parallelCrashReadRow
	for rec, err := range rd.Records(ctx) {
		if err != nil {
			return nil, err
		}
		cols := make(map[string]int, rec.NumCols())
		for i, field := range rec.Schema().Fields() {
			cols[field.Name] = i
		}
		tsCol, ok := rec.Column(cols[schema.ColTS]).(*array.Timestamp)
		if !ok {
			return nil, fmt.Errorf("object %s: %s column is not timestamp", key, schema.ColTS)
		}
		lineCol, ok := rec.Column(cols[schema.ColLine]).(*array.String)
		if !ok {
			return nil, fmt.Errorf("object %s: %s column is not string", key, schema.ColLine)
		}
		labelsCol, ok := rec.Column(cols[schema.ColLabels]).(*array.Map)
		if !ok {
			return nil, fmt.Errorf("object %s: %s column is not a map", key, schema.ColLabels)
		}
		metaCol, ok := rec.Column(cols[schema.ColMeta]).(*array.Map)
		if !ok {
			return nil, fmt.Errorf("object %s: %s column is not a map", key, schema.ColMeta)
		}
		for i := range int(rec.NumRows()) {
			out = append(out, parallelCrashReadRow{
				ts: int64(tsCol.Value(i)), line: lineCol.Value(i),
				labels: uploadReadMap(labelsCol, i), meta: uploadReadMap(metaCol, i),
			})
		}
	}
	return out, nil
}

func parallelCrashListObjects(objDir string) ([]string, error) {
	root, err := os.OpenRoot(objDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var keys []string
	err = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, objstore.ExtData) {
			keys = append(keys, p)
		}
		return nil
	})
	slices.Sort(keys)
	return keys, err
}

func parallelCrashKeyTenant(key string) tenant.ID {
	first, _, _ := strings.Cut(key, "/")
	return tenant.ID(first)
}

func parallelCrashObjectSize(t *testing.T, objects objstore.Store, key string) int64 {
	t.Helper()
	info, err := objects.Stat(t.Context(), key)
	if err != nil {
		t.Fatalf("stat object %s: %v", key, err)
	}
	return info.Size
}

func parallelCrashStat(ctx context.Context, objects objstore.Store, key string) error {
	_, err := objects.Stat(ctx, key)
	return err
}

// TestParallelCrashEvidenceParsing pins the evidence parser: a torn final
// record is the only thing a SIGKILL may leave behind, so it is discarded;
// every complete line must decode or the crash evidence is corrupt.
func TestParallelCrashEvidenceParsing(t *testing.T) {
	valid := []byte(`{"phase":"crash","kind":"stage","stage":"upload-data-put"}` + "\n")
	for _, tc := range []struct {
		name    string
		data    []byte
		want    int
		wantErr bool
	}{
		{name: "complete record", data: valid, want: 1},
		{name: "torn final record", data: append(slices.Clone(valid), []byte(`{"phase":"crash","kind":"put`)...), want: 1},
		{name: "malformed complete record", data: append(slices.Clone(valid), []byte("not-json\n")...), wantErr: true},
		{name: "malformed first complete record", data: []byte(`{"phase":}` + "\n"), wantErr: true},
		{name: "only a torn fragment", data: []byte(`{"phase":`), want: 0},
		{name: "empty", data: nil, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseParallelCrashEvents(tc.data)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("malformed complete evidence accepted: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse evidence: %v", err)
			}
			if len(got) != tc.want {
				t.Fatalf("events = %d, want %d", len(got), tc.want)
			}
		})
	}
}

// --- in-process fault and ambiguity tests -----------------------------------

// parallelCrashStoreThenFail writes the object and then reports an error,
// simulating a Put whose success the uploader never observed.
type parallelCrashStoreThenFail struct {
	objstore.Store
	err      error
	dataOnly bool
	sideOnly bool
	keys     []string
	mu       sync.Mutex
}

func (s *parallelCrashStoreThenFail) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	sidecar := strings.HasSuffix(key, objstore.ExtSidecar)
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	if s.dataOnly && sidecar || s.sideOnly && !sidecar {
		return s.Store.Put(ctx, key, r, size)
	}
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		return err
	}
	return s.err
}

// parallelCrashAddThenFail commits the catalog row and then reports an error.
type parallelCrashAddThenFail struct {
	catalog.Catalog
	err   error
	calls int
}

func (c *parallelCrashAddThenFail) AddFiles(ctx context.Context, files ...catalog.File) error {
	c.calls++
	if err := c.Catalog.AddFiles(ctx, files...); err != nil {
		return err
	}
	return c.err
}

func TestParallelCrashAmbiguousDataPut(t *testing.T) {
	hour := time.Date(2026, 11, 4, 13, 0, 0, 0, time.UTC)
	f := uploadNewFixtureKind(t, uploadCatalogSQLite, nil)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "ambiguous data one"},
		{ts: hour.Add(2 * time.Minute), line: "ambiguous data two"},
	})
	putErr := errors.New("ambiguous data put")
	firstPuts := new(parallelCrashKeyListPut)
	firstPuts.Store = f.objects
	amb := &parallelCrashStoreThenFail{Store: firstPuts, err: putErr, dataOnly: true}
	_, err := f.uploader(UploaderOptions{Objects: amb, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadMerged || !receipt.PutCompletedAt.IsZero() {
		t.Fatalf("receipt = %+v, want merged without a Put completion", receipt)
	}
	oldKey := receipt.Key
	if _, err := f.objects.Stat(t.Context(), oldKey); err != nil {
		t.Fatalf("ambiguous data Put did not write the object: %v", err)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
		t.Fatalf("catalog rows after ambiguous data Put = %d, want none", len(got))
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); err != nil {
		t.Fatalf("source lost after ambiguous data Put: %v", err)
	}

	f.reopen()
	retryPuts := new(parallelCrashKeyListPut)
	retryPuts.Store = f.objects
	report, err := f.uploader(UploaderOptions{Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("retry report = %+v, want one resumed upload", report)
	}
	file := f.liveCatalogFiles(tenant.Default)[0]
	if file.Key == oldKey {
		t.Fatalf("retry reused the ambiguous key %q", oldKey)
	}
	if len(firstPuts.keys) != 1 || firstPuts.keys[0] != oldKey {
		t.Fatalf("first-pass Put keys = %v, want only %q", firstPuts.keys, oldKey)
	}
	want := []string{file.Key, objstore.SidecarKey(file.Key)}
	if !slices.Equal(retryPuts.keys, want) {
		t.Fatalf("retry Put keys = %v, want %v", retryPuts.keys, want)
	}
	if rotations, err := f.tenantStore(tenant.Default).KeyRotations(t.Context()); err != nil || rotations == 0 {
		t.Fatalf("key rotations = %d, err=%v; want a durable rotation", rotations, err)
	}
	if rows := f.readObject(tenant.Default, file.Key); len(rows) != 2 || rows[0].line != "ambiguous data one" {
		t.Fatalf("retry rows = %+v, want both source rows", rows)
	}
}

func TestParallelCrashAmbiguousSidecarPut(t *testing.T) {
	hour := time.Date(2026, 11, 4, 14, 0, 0, 0, time.UTC)
	f := uploadNewFixtureKind(t, uploadCatalogSQLite, nil)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "ambiguous sidecar"}})
	putErr := errors.New("ambiguous sidecar put")
	firstPuts := new(parallelCrashKeyListPut)
	firstPuts.Store = f.objects
	amb := &parallelCrashStoreThenFail{Store: firstPuts, err: putErr, sideOnly: true}
	_, err := f.uploader(UploaderOptions{Objects: amb, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored || receipt.PutCompletedAt.IsZero() {
		t.Fatalf("receipt = %+v, want stored with a data Put completion", receipt)
	}
	oldKey := receipt.Key
	if _, err := f.objects.Stat(t.Context(), objstore.SidecarKey(oldKey)); err != nil {
		t.Fatalf("ambiguous sidecar Put did not write the sidecar: %v", err)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
		t.Fatalf("catalog rows after ambiguous sidecar Put = %d, want none", len(got))
	}

	f.reopen()
	retryPuts := new(parallelCrashKeyListPut)
	retryPuts.Store = f.objects
	report, err := f.uploader(UploaderOptions{Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("retry report = %+v, want one resumed upload", report)
	}
	file := f.liveCatalogFiles(tenant.Default)[0]
	if file.Key == oldKey {
		t.Fatalf("retry reused the stored key %q", oldKey)
	}
	want := []string{file.Key, objstore.SidecarKey(file.Key)}
	if !slices.Equal(retryPuts.keys, want) {
		t.Fatalf("retry Put keys = %v, want %v", retryPuts.keys, want)
	}
	if !file.HasSidecar {
		t.Fatal("retry published without HasSidecar")
	}
	uploadSidecarBytes(t, f, file.Key)
}

func TestParallelCrashAmbiguousAddFiles(t *testing.T) {
	hour := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	f := uploadNewFixtureKind(t, uploadCatalogSQLite, nil)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "ambiguous addfiles"}})
	addErr := errors.New("ambiguous catalog add")
	firstCat := &parallelCrashAddThenFail{Catalog: f.cat, err: addErr}
	firstPuts := new(parallelCrashKeyListPut)
	firstPuts.Store = f.objects
	_, err := f.uploader(UploaderOptions{Objects: firstPuts, Catalog: firstCat, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if !errors.Is(err, addErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, addErr)
	}
	if firstCat.calls != 1 {
		t.Fatalf("AddFiles calls = %d, want 1", firstCat.calls)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored {
		t.Fatalf("receipt stage = %q, want stored", receipt.Stage)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 1 {
		t.Fatalf("catalog rows after ambiguous AddFiles = %d, want 1", len(got))
	}
	key := receipt.Key

	f.reopen()
	retryPuts := new(parallelCrashKeyListPut)
	retryPuts.Store = f.objects
	counter := &uploadCountingCatalog{Catalog: f.cat}
	report, err := f.uploader(UploaderOptions{Objects: retryPuts, Catalog: counter, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("retry report = %+v, want one resumed upload", report)
	}
	if len(retryPuts.keys) != 0 {
		t.Fatalf("retry Puts = %v, want no second Put after an acknowledged catalog row", retryPuts.keys)
	}
	if counter.calls != 0 {
		t.Fatalf("retry AddFiles calls = %d, want 0", counter.calls)
	}
	live := f.liveCatalogFiles(tenant.Default)
	if len(live) != 1 || live[0].Key != key {
		t.Fatalf("live rows = %+v, want the original key %q", live, key)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source survived publication cleanup: %v", err)
	}
}

func TestParallelCrashTombstoneIsNotResurrected(t *testing.T) {
	hour := time.Date(2026, 11, 4, 16, 0, 0, 0, time.UTC)
	f := uploadNewFixtureKind(t, uploadCatalogSQLite, nil)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "tombstoned"}})
	addErr := errors.New("ambiguous catalog add")
	firstCat := &parallelCrashAddThenFail{Catalog: f.cat, err: addErr}
	if _, err := f.uploader(UploaderOptions{
		Objects: f.objects, Catalog: firstCat, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context()); !errors.Is(err, addErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, addErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	key := receipt.Key
	if err := f.cat.ReplaceFiles(t.Context(), tenant.Default, []string{key}, nil); err != nil {
		t.Fatalf("tombstone %s: %v", key, err)
	}
	if err := f.objects.Delete(t.Context(), objstore.SidecarKey(key)); err != nil {
		t.Fatalf("delete sidecar: %v", err)
	}
	if err := f.objects.Delete(t.Context(), key); err != nil {
		t.Fatalf("delete object: %v", err)
	}

	f.reopen()
	retryPuts := new(parallelCrashKeyListPut)
	retryPuts.Store = f.objects
	report, err := f.uploader(UploaderOptions{Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("retry report = %+v, want one resumed upload", report)
	}
	if len(retryPuts.keys) != 0 {
		t.Fatalf("tombstoned upload was re-Put: %v", retryPuts.keys)
	}
	rows := f.allCatalogFiles(tenant.Default)
	if len(rows) != 1 || !rows[0].Deleted() || rows[0].Key != key {
		t.Fatalf("catalog rows = %+v, want only the tombstone %q", rows, key)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("retired source survived cleanup: %v", err)
	}
}

// TestParallelCrashPartDirsyncFaultQuarantines proves the machine-crash half
// of a renamed-but-unsynced part: writeRecord reports the sync failure, the
// manifest stays empty, and recovery quarantines the unreferenced file rather
// than trusting it.
func TestParallelCrashPartDirsyncFaultQuarantines(t *testing.T) {
	const targetDir = "api/2026/11/04/17"
	var renamed atomic.Bool
	var hits atomic.Int64
	s, dir := testStore(t, func(ops *localFileOps) {
		baseRename := ops.rename
		ops.rename = func(root *os.Root, oldName, newName string) error {
			err := baseRename(root, oldName, newName)
			if err == nil && strings.HasPrefix(newName, targetDir+"/") &&
				strings.HasSuffix(newName, parquetExt) && !strings.HasSuffix(newName, tempSuffix) {
				renamed.Store(true)
			}
			return err
		}
		base := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			// Only the final partition-directory sync after the rename may
			// fail. The ensureDir setup syncs the parent, never targetDir, so
			// requiring renamed here proves the injected return is post-rename.
			if name == targetDir && renamed.Load() {
				hits.Add(1)
				return errors.New("injected part dirsync failure")
			}
			return base(root, name)
		}
	})
	ts, err := s.Tenant(t.Context(), tenant.Default)
	if err != nil {
		t.Fatalf("tenant: %v", err)
	}
	hour := time.Date(2026, 11, 4, 17, 0, 0, 0, time.UTC)
	rec := parallelCrashRecord(t, []uploadRow{{ts: hour.Add(time.Minute), line: "uncommitted"}})
	_, err = ts.writeRecord(t.Context(), "api", hour, rec, defaultWriterFactory, nil)
	rec.Release()
	if err == nil {
		t.Fatal("writeRecord succeeded despite the injected dirsync failure")
	}
	if !renamed.Load() {
		t.Fatal("part dirsync fault fired before the temp was renamed into place")
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("part dirsync fault injected %d times, want exactly 1 post-rename return", got)
	}
	if files, _ := ts.Files(t.Context()); len(files) != 0 {
		t.Fatalf("manifest has %d files after an uncommitted part", len(files))
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close local: %v", err)
	}
	local, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatalf("reopen local: %v", err)
	}
	defer func() { _ = local.Close() }()
	ts, err = local.Tenant(t.Context(), tenant.Default)
	if err != nil {
		t.Fatalf("reopened tenant: %v", err)
	}
	if files, err := ts.Files(t.Context()); err != nil || len(files) != 0 {
		t.Fatalf("reopened manifest files = %d, err=%v; want none", len(files), err)
	}
	root, err := os.OpenRoot(filepath.Join(dir, "parquet"))
	if err != nil {
		t.Fatalf("open parquet root: %v", err)
	}
	defer root.Close()
	quarantined := 0
	_ = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(p, quarantineDir) && strings.HasSuffix(p, objstore.ExtData) {
			quarantined++
		}
		return nil
	})
	if quarantined == 0 {
		t.Fatal("uncommitted part was not quarantined on reopen")
	}
}

// TestParallelCrashFinalizeDirsyncFault proves a finalize rename whose uploads
// directory sync fails leaves no catalog row; the retry re-merges and
// publishes exactly once.
func TestParallelCrashFinalizeDirsyncFault(t *testing.T) {
	var renamed, sawFinal atomic.Bool
	var hits atomic.Int64
	f := uploadNewFixtureWithOps(t, func(ops *localFileOps) {
		baseRename := ops.rename
		ops.rename = func(root *os.Root, oldName, newName string) error {
			err := baseRename(root, oldName, newName)
			if err == nil && strings.HasPrefix(newName, uploadsDir+"/") &&
				strings.HasSuffix(newName, parquetExt) && !strings.HasSuffix(newName, tempSuffix) {
				renamed.Store(true)
			}
			return err
		}
		base := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if name == uploadsDir && renamed.Load() {
				entries, err := fs.ReadDir(root.FS(), uploadsDir)
				if err == nil {
					for _, e := range entries {
						if !e.IsDir() && strings.HasSuffix(e.Name(), parquetExt) {
							sawFinal.Store(true)
							hits.Add(1)
							return errors.New("injected uploads dirsync failure")
						}
					}
				}
			}
			return base(root, name)
		}
	})
	hour := time.Date(2026, 11, 4, 18, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "finalize fault"}})
	if _, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context()); err == nil {
		t.Fatal("RunOnce succeeded despite the injected finalize dirsync failure")
	}
	if !renamed.Load() {
		t.Fatal("uploads dirsync fault fired before the finalize rename completed")
	}
	if !sawFinal.Load() {
		t.Fatal("uploads dirsync fault fired without a finalized temp in .uploads")
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("uploads dirsync fault injected %d times, want exactly 1 post-finalize return", got)
	}
	if got := f.liveCatalogFiles(tenant.Default); len(got) != 0 {
		t.Fatalf("catalog rows after failed finalize = %d, want none", len(got))
	}

	f.reopen()
	// Reopen with default seams: the fault is process-local and gone.
	report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("retry report = %+v, want one upload", report)
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 1 {
		t.Fatalf("live rows = %d, want 1", len(rows))
	}
	if got := f.readObject(tenant.Default, f.liveCatalogFiles(tenant.Default)[0].Key); len(got) != 1 || got[0].line != "finalize fault" {
		t.Fatalf("published rows = %+v", got)
	}
}

// TestParallelCrashManifestSyncFault proves a reservation manifest whose
// tenant-directory fsync fails is retried, not duplicated: the reservation is
// already on disk, the retry resumes it, and exactly one catalog row results.
func TestParallelCrashManifestSyncFault(t *testing.T) {
	var faulted atomic.Bool
	f := uploadNewFixtureWithOps(t, func(ops *localFileOps) {
		base := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if name == "." && !faulted.Load() {
				if data, err := root.ReadFile(manifestName); err == nil && bytes.Contains(data, []byte(`"stage":"reserved"`)) {
					faulted.Store(true)
					return errors.New("injected manifest dirsync failure")
				}
			}
			return base(root, name)
		}
	})
	hour := time.Date(2026, 11, 4, 19, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "manifest sync fault"}})
	if _, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context()); err == nil {
		t.Fatal("RunOnce succeeded despite the injected manifest dirsync failure")
	}
	if !faulted.Load() {
		t.Fatal("injected manifest dirsync fault was not reached")
	}
	f.reopen()
	report, err := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("retry report = %+v, want one upload", report)
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 1 || rows[0].Key == "" {
		t.Fatalf("live rows = %+v, want exactly one", rows)
	}
	if got := f.readObject(tenant.Default, f.liveCatalogFiles(tenant.Default)[0].Key); len(got) != 1 || got[0].line != "manifest sync fault" {
		t.Fatalf("published rows = %+v", got)
	}
}

// parallelCrashKeyListPut records Put keys and delegates.
type parallelCrashKeyListPut struct {
	objstore.Store
	keys []string
	mu   sync.Mutex
}

func (s *parallelCrashKeyListPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Store.Put(ctx, key, r, size)
}
