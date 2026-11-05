package compactor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var errSpy = errors.New("spy: injected failure")

// New satisfies the role factory contract.
var _ app.Factory = New

// testIndexConfig is the explicit, valid index section every hand-built test
// config carries: New rejects a zero or invalid Index instead of normalizing
// it, mirroring config.Load's defaults without duplicating their tuning.
func testIndexConfig() config.Index {
	return config.Index{
		N:        index.DefaultParams.N,
		FPRate:   index.DefaultParams.FPRate,
		MaxGrams: index.DefaultParams.MaxGrams,
	}
}

// testDeps wires a service to a filesystem store, a memory catalog and a real
// registry, with spies in between to inject failures and count calls.
// SidecarBackfill stays false unless a test opts in: backfill reads through
// the same spy Store, so merge-focused tests keep their call counts meaning
// "merge traffic only".
type testDeps struct {
	dir   string
	fs    *objstore.FS
	store *spyStore
	cat   *spyCatalog
	reg   *prometheus.Registry
	deps  app.Deps
}

func newTestDeps(t *testing.T, interval time.Duration, target config.ByteSize) *testDeps {
	t.Helper()
	dir := t.TempDir()
	fs, err := objstore.NewFS(dir)
	if err != nil {
		t.Fatalf("NewFS: %v", err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	ops := &opLog{}
	d := &testDeps{
		dir:   dir,
		fs:    fs,
		store: &spyStore{Store: fs, ops: ops},
		cat:   &spyCatalog{Catalog: catalog.NewMemory(), ops: ops},
		reg:   prometheus.NewRegistry(),
	}
	d.deps = app.Deps{
		Config: config.Config{
			Compactor: config.Compactor{Interval: interval, TargetSize: target},
			Index:     testIndexConfig(),
		},
		Logger:  slog.New(slog.DiscardHandler),
		Store:   d.store,
		Catalog: d.cat,
		Metrics: d.reg,
	}
	return d
}

func mustService(t *testing.T, d *testDeps) *service {
	t.Helper()
	svc, err := New(d.deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s, ok := svc.(*service)
	if !ok {
		t.Fatalf("New returned %T, want *service", svc)
	}
	return s
}

// opLog records the order of service-visible store and catalog operations as
// formatted entries, shared by the store and catalog spies of one testDeps so
// tests can prove ordering across both.
type opLog struct {
	mu  sync.Mutex
	ops []string
}

// add appends one formatted entry.
func (l *opLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, fmt.Sprintf(format, args...))
}

// index returns the position of the first entry equal to op, or -1.
func (l *opLog) index(op string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Index(l.ops, op)
}

// indexFunc returns the position of the first entry satisfying keep, or -1.
func (l *opLog) indexFunc(keep func(string) bool) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.IndexFunc(l.ops, keep)
}

// snapshot returns all entries in order.
func (l *opLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ops)
}

// spyStore wraps an objstore.Store with failure injection and call counts.
// ops records every service-visible store operation as "put:<key>",
// "stat:<key>" and "range:<key>" entries.
type spyStore struct {
	objstore.Store
	ops          *opLog
	putErr       error // fail Put after reading putReadLimit bytes
	putReadLimit int64 // negative: fail before reading anything
	statFailPut  bool  // Stat fails for the most recently Put key
	blockPut     bool  // Put ignores data and waits for ctx.Done

	// sidecarPutErr fails Put for sidecar keys only, so the data upload of a
	// merge or backfill still succeeds.
	sidecarPutErr error

	// failRangesAfterPut fails GetRange for the most recently Put key. A
	// merge's input reads target other keys, so only the sidecar reread of
	// the just-uploaded final object fails, with the producer already joined.
	failRangesAfterPut bool

	// getRangeDelay slows every ranged read. blockRanges makes merge-phase
	// reads (those started after a Put began) wait for ctx.Done, and
	// waitBlocked makes Put itself hold until one is in flight: together
	// they pin the producer inside a ranged read when the upload fails.
	getRangeDelay time.Duration
	blockRanges   bool
	waitBlocked   bool

	lastPut       atomic.Value // string
	putCalls      atomic.Int64
	statCalls     atomic.Int64
	rangeCalls    atomic.Int64
	blockedRanges atomic.Int64
}

func (s *spyStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.putCalls.Add(1)
	s.lastPut.Store(key)
	s.ops.add("put:%s", key)
	if s.sidecarPutErr != nil && strings.HasSuffix(key, objstore.ExtSidecar) {
		return s.sidecarPutErr
	}
	switch {
	case s.blockPut:
		<-ctx.Done()
		return ctx.Err()
	case s.waitBlocked:
		// Merge writes the 4-byte PAR1 magic before its first input read;
		// consuming it lets the producer reach the ranged read we block on.
		if _, err := io.ReadFull(r, make([]byte, 4)); err != nil {
			return err
		}
		deadline := time.Now().Add(10 * time.Second)
		for s.blockedRanges.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if s.putErr != nil {
			return s.putErr
		}
		return errSpy
	case s.putErr != nil:
		if s.putReadLimit >= 0 {
			_, _ = io.CopyN(io.Discard, r, s.putReadLimit)
		}
		return s.putErr
	}
	return s.Store.Put(ctx, key, r, size)
}

func (s *spyStore) Stat(ctx context.Context, key string) (objstore.Info, error) {
	s.statCalls.Add(1)
	s.ops.add("stat:%s", key)
	if s.statFailPut && s.lastPut.Load() == key {
		return objstore.Info{}, errSpy
	}
	return s.Store.Stat(ctx, key)
}

func (s *spyStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.rangeCalls.Add(1)
	s.ops.add("range:%s", key)
	if s.failRangesAfterPut && s.lastPut.Load() == key {
		return nil, errSpy
	}
	if s.blockRanges && s.putCalls.Load() > 0 {
		s.blockedRanges.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.getRangeDelay > 0 {
		timer := time.NewTimer(s.getRangeDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return s.Store.GetRange(ctx, key, offset, length)
}

// spyCatalog wraps a catalog.Catalog with failure injection. It records the
// files passed to ReplaceFiles even when the call is failed, and logs
// "replace" and "setsidecar:<key>" entries to the shared op log.
type spyCatalog struct {
	catalog.Catalog
	ops          *opLog
	streamsFails atomic.Int64 // number of Streams calls to fail
	replaceErr   error
	replaceCalls atomic.Int64
	// setSidecarErr fails SetSidecar while set; setSidecarFails counts down
	// the calls to fail before calling through.
	setSidecarErr   error
	setSidecarFails atomic.Int64
	setSidecarCalls atomic.Int64
	// listErr fails ListFiles. With listErrAfter set, all rows are yielded
	// first and the error closes the iteration; otherwise the error is the
	// first yield.
	listErr      error
	listErrAfter bool
	listCalls    atomic.Int64
	// onSetSidecar runs before the wrapped SetSidecar call, letting a test
	// win the listing race by replacing or purging the row first.
	onSetSidecar func(t tenant.ID, key string)

	mu    sync.Mutex
	added []catalog.File
}

func (c *spyCatalog) Streams(ctx context.Context, t tenant.ID) ([]string, error) {
	if c.streamsFails.Add(-1) >= 0 {
		return nil, errSpy
	}
	return c.Catalog.Streams(ctx, t)
}

func (c *spyCatalog) ListFiles(ctx context.Context, flt catalog.Filter) iter.Seq2[catalog.File, error] {
	c.listCalls.Add(1)
	if c.listErr != nil && !c.listErrAfter {
		return func(yield func(catalog.File, error) bool) {
			yield(catalog.File{}, c.listErr)
		}
	}
	return func(yield func(catalog.File, error) bool) {
		for f, err := range c.Catalog.ListFiles(ctx, flt) {
			if !yield(f, err) {
				return
			}
		}
		if c.listErr != nil {
			yield(catalog.File{}, c.listErr)
		}
	}
}

func (c *spyCatalog) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []catalog.File) error {
	c.replaceCalls.Add(1)
	c.ops.add("replace:%s", strings.Join(remove, ","))
	c.mu.Lock()
	c.added = append(c.added, add...)
	c.mu.Unlock()
	if c.replaceErr != nil {
		return c.replaceErr
	}
	return c.Catalog.ReplaceFiles(ctx, t, remove, add)
}

func (c *spyCatalog) SetSidecar(ctx context.Context, t tenant.ID, key string) error {
	c.setSidecarCalls.Add(1)
	c.ops.add("setsidecar:%s", key)
	if c.onSetSidecar != nil {
		c.onSetSidecar(t, key)
	}
	if c.setSidecarFails.Add(-1) >= 0 {
		return c.setSidecarErr
	}
	return c.Catalog.SetSidecar(ctx, t, key)
}

func (c *spyCatalog) captured() []catalog.File {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.added)
}

// --- test data builders ---

// col builds one Arrow array for dt from vals; nil entries become nulls.
// Supported values: int64 Unix-micro times for timestamps, string, int64,
// float64, bool, map[string]string.
func col(t *testing.T, dt arrow.DataType, vals []any) arrow.Array {
	t.Helper()
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for _, v := range vals {
		if v == nil {
			b.AppendNull()
			continue
		}
		switch bb := b.(type) {
		case *array.TimestampBuilder:
			switch ts := v.(type) {
			case int64:
				bb.Append(arrow.Timestamp(ts))
			default:
				bb.Append(arrow.Timestamp(schema.TS(ts.(time.Time))))
			}
		case *array.StringBuilder:
			bb.Append(v.(string))
		case *array.Int64Builder:
			bb.Append(v.(int64))
		case *array.Float64Builder:
			bb.Append(v.(float64))
		case *array.BooleanBuilder:
			bb.Append(v.(bool))
		case *array.MapBuilder:
			bb.Append(true)
			kb := bb.KeyBuilder().(*array.StringBuilder)
			ib := bb.ItemBuilder().(*array.StringBuilder)
			for _, k := range slices.Sorted(maps.Keys(v.(map[string]string))) {
				kb.Append(k)
				ib.Append(v.(map[string]string)[k])
			}
		default:
			t.Fatalf("col: unsupported type %s", dt)
		}
	}
	return b.NewArray()
}

// batch builds a record batch matching s from positional column values.
func batch(t *testing.T, s *arrow.Schema, cols ...[]any) arrow.RecordBatch {
	t.Helper()
	fields := s.Fields()
	if len(cols) != len(fields) {
		t.Fatalf("batch: %d columns for %d fields", len(cols), len(fields))
	}
	arrays := make([]arrow.Array, len(fields))
	var rows int64 = -1
	for i, field := range fields {
		arrays[i] = col(t, field.Type, cols[i])
		defer arrays[i].Release()
		if rows < 0 {
			rows = int64(arrays[i].Len())
		} else if int64(arrays[i].Len()) != rows {
			t.Fatalf("batch: column %d has %d rows, want %d", i, arrays[i].Len(), rows)
		}
	}
	return array.NewRecordBatch(s, arrays, rows)
}

// Value helpers: timestamps are Unix microseconds, a negative value is null.
func tss(us ...int64) []any {
	vals := make([]any, len(us))
	for i, u := range us {
		if u >= 0 {
			vals[i] = u
		}
	}
	return vals
}

func strs(ss ...string) []any {
	vals := make([]any, len(ss))
	for i, s := range ss {
		vals[i] = s
	}
	return vals
}

func mapVals(ms ...map[string]string) []any {
	vals := make([]any, len(ms))
	for i, m := range ms {
		if m != nil {
			vals[i] = m
		}
	}
	return vals
}

// putFile writes one Parquet object through storage.Writer under a fresh data
// key partitioned by minTS and lists it in the catalog. cols are positional
// column values matching s; pass none for an empty file.
func putFile(t *testing.T, d *testDeps, tid tenant.ID, stream string, minTS time.Time, s *arrow.Schema, promoted []string, cols ...[]any) catalog.File {
	t.Helper()
	ctx := t.Context()

	var buf bytes.Buffer
	w, err := storage.NewWriter(&buf, s, promoted)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if len(cols) > 0 {
		rec := batch(t, s, cols...)
		defer rec.Release()
		if err := w.Write(rec); err != nil {
			t.Fatalf("write parquet: %v", err)
		}
	}
	stats, err := w.Close()
	if err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}

	key := objstore.NewDataKey(tid, stream, minTS)
	// Write through the raw store so the spy's counters only observe service
	// traffic.
	if err := d.fs.Put(ctx, key, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	f := catalog.File{
		Tenant: tid, Stream: stream, Key: key,
		MinTS: stats.MinTS, MaxTS: stats.MaxTS,
		Rows: stats.Rows, Bytes: int64(buf.Len()),
		LabelValues: stats.LabelValues,
	}
	if err := d.cat.AddFiles(ctx, f); err != nil {
		t.Fatalf("add %s: %v", key, err)
	}
	return f
}

// readObjectRows reads a stored object and returns its schema plus rows as Go
// values: int64 micros for timestamps, string, int64, float64, bool,
// map[string]string, nil for nulls.
func readObjectRows(t *testing.T, d *testDeps, tid tenant.ID, key string) (*arrow.Schema, [][]any) {
	t.Helper()
	ctx := t.Context()
	or, err := storage.NewObjectReader(ctx, d.store, tid, key)
	if err != nil {
		t.Fatalf("NewObjectReader %s: %v", key, err)
	}
	r, err := storage.NewReader(or, or.Size())
	if err != nil {
		t.Fatalf("NewReader %s: %v", key, err)
	}
	defer func() { _ = r.Close() }()

	var rows [][]any
	for rec, err := range r.Records(ctx) {
		if err != nil {
			t.Fatalf("records %s: %v", key, err)
		}
		for i := range int(rec.NumRows()) {
			row := make([]any, rec.NumCols())
			for c := range int(rec.NumCols()) {
				row[c] = colValue(t, rec.Column(c), i)
			}
			rows = append(rows, row)
		}
	}
	return r.Schema(), rows
}

func colValue(t *testing.T, a arrow.Array, i int) any {
	t.Helper()
	if !a.IsValid(i) {
		return nil
	}
	switch c := a.(type) {
	case *array.Timestamp:
		return int64(c.Value(i))
	case *array.String:
		return c.Value(i)
	case *array.Int64:
		return c.Value(i)
	case *array.Float64:
		return c.Value(i)
	case *array.Boolean:
		return c.Value(i)
	case *array.Map:
		start, end := c.ValueOffsets(i)
		keys := c.Keys().(*array.String)
		items := c.Items().(*array.String)
		m := make(map[string]string, end-start)
		for k := start; k < end; k++ {
			m[keys.Value(int(k))] = items.Value(int(k))
		}
		return m
	default:
		t.Fatalf("colValue: unsupported array %T", a)
		return nil
	}
}

// listFiles returns the catalog rows matching flt.
func listFiles(t *testing.T, cat catalog.Catalog, flt catalog.Filter) []catalog.File {
	t.Helper()
	var out []catalog.File
	for f, err := range cat.ListFiles(t.Context(), flt) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		out = append(out, f)
	}
	return out
}

func liveFiles(t *testing.T, cat catalog.Catalog, tid tenant.ID, stream string) []catalog.File {
	t.Helper()
	return listFiles(t, cat, catalog.Filter{Tenant: tid, Stream: stream})
}

func fieldNames(s *arrow.Schema) []string {
	names := make([]string, len(s.Fields()))
	for i, f := range s.Fields() {
		names[i] = f.Name
	}
	return names
}

// waitFor polls cond until it holds or the test deadline is nearly reached.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitGoroutines waits until no more than want goroutines run, catching
// producer goroutines leaked when an upload fails early.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	waitFor(t, "goroutines to settle", func() bool {
		return runtime.NumGoroutine() <= want
	})
}

// metricValue returns the summed value of a counter/gauge or the sample count
// of a histogram gathered from g.
func metricValue(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			switch {
			case m.GetCounter() != nil:
				sum += m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				sum += m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
		return sum
	}
	return 0
}

// metricLabeledValue returns the value of a counter gathered from g for the
// series whose source label equals src, or 0 when that series is absent.
func metricLabeledValue(t *testing.T, g prometheus.Gatherer, name, src string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetCounter() == nil {
				continue
			}
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "source" && lp.GetValue() == src {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// --- service tests ---

func TestNewValidatesDeps(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(d *testDeps)
	}{
		{"zero interval", func(d *testDeps) { d.deps.Config.Compactor.Interval = 0 }},
		{"negative interval", func(d *testDeps) { d.deps.Config.Compactor.Interval = -time.Second }},
		{"zero target size", func(d *testDeps) { d.deps.Config.Compactor.TargetSize = 0 }},
		{"negative retention", func(d *testDeps) { d.deps.Config.Compactor.Retention = -time.Hour }},
		{"negative delete delay", func(d *testDeps) { d.deps.Config.Compactor.DeleteDelay = -time.Hour }},
		{"nil store", func(d *testDeps) { d.deps.Store = nil }},
		{"nil catalog", func(d *testDeps) { d.deps.Catalog = nil }},
		{"nil logger", func(d *testDeps) { d.deps.Logger = nil }},
		{"nil metrics", func(d *testDeps) { d.deps.Metrics = nil }},
		{"zero index config", func(d *testDeps) { d.deps.Config.Index = config.Index{} }},
		{"index n above range", func(d *testDeps) { d.deps.Config.Index.N = 9 }},
		{"index fp rate zero", func(d *testDeps) { d.deps.Config.Index.FPRate = 0 }},
		{"index max grams zero", func(d *testDeps) { d.deps.Config.Index.MaxGrams = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t, time.Minute, 512*config.MiB)
			tc.mutate(d)
			if _, err := New(d.deps); err == nil {
				t.Fatal("New succeeded, want validation error")
			}
		})
	}

	t.Run("valid", func(t *testing.T) {
		d := newTestDeps(t, time.Minute, 512*config.MiB)
		if _, err := New(d.deps); err != nil {
			t.Fatalf("New: %v", err)
		}
	})
}

func TestNewRegistersMetrics(t *testing.T) {
	d := newTestDeps(t, time.Minute, 512*config.MiB)
	mustService(t, d)

	mfs, err := d.reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"parkana_compactor_cycles_total",
		"parkana_compactor_cycle_errors_total",
		"parkana_compactor_merge_errors_total",
		"parkana_compactor_files_merged_total",
		"parkana_compactor_bytes_merged_total",
		"parkana_compactor_cycle_duration_seconds",
	} {
		if !names[want] {
			t.Fatalf("metric %s not registered; have %v", want, slices.Sorted(maps.Keys(names)))
		}
	}
	// The shared parkana_index_* collectors are registered alongside (vector
	// collectors expose no series until first observed, so the sidecar tests
	// assert their values). A second service on the same registry reuses both
	// registrations rather than panicking or failing.
	if _, err := New(d.deps); err != nil {
		t.Fatalf("second New on same registry: %v", err)
	}
}

func TestRunMergesAndCancels(t *testing.T) {
	d := newTestDeps(t, 5*time.Millisecond, 512*config.MiB)
	s := mustService(t, d)
	hour := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	base := schema.TS(hour)
	sch, err := schema.ForStream(nil)
	if err != nil {
		t.Fatalf("ForStream: %v", err)
	}
	putFile(t, d, tenant.Default, "svc", hour, sch, nil,
		tss(base, base+20), strs("a0", "a1"),
		mapVals(map[string]string{"app": "web"}, map[string]string{"app": "web"}),
		mapVals(map[string]string{"m": "1"}, map[string]string{"m": "2"}),
	)
	putFile(t, d, tenant.Default, "svc", hour, sch, nil,
		tss(base+10), strs("b0"), mapVals(map[string]string{"app": "db"}), mapVals(map[string]string{}),
	)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	waitFor(t, "files to merge", func() bool {
		return len(liveFiles(t, d.cat, tenant.Default, "svc")) == 1
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if got := metricValue(t, d.reg, "parkana_compactor_files_merged_total"); got != 2 {
		t.Fatalf("files_merged_total = %v, want 2", got)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_cycle_errors_total"); got != 0 {
		t.Fatalf("cycle_errors_total = %v, want 0", got)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_cycles_total"); got < 1 {
		t.Fatalf("cycles_total = %v, want >= 1", got)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_cycle_duration_seconds"); got < 1 {
		t.Fatalf("cycle_duration_seconds count = %v, want >= 1", got)
	}
}

func TestRunRetriesAfterCycleFailure(t *testing.T) {
	d := newTestDeps(t, 5*time.Millisecond, 512*config.MiB)
	s := mustService(t, d)
	hour := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	sch, err := schema.ForStream(nil)
	if err != nil {
		t.Fatalf("ForStream: %v", err)
	}
	putFile(t, d, tenant.Default, "svc", hour, sch, nil,
		tss(schema.TS(hour)), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}),
	)
	putFile(t, d, tenant.Default, "svc", hour, sch, nil,
		tss(schema.TS(hour)+1), strs("b0"), mapVals(map[string]string{}), mapVals(map[string]string{}),
	)

	// Two Streams failures make the first cycles error out; the service must
	// retry instead of dying.
	d.cat.streamsFails.Store(2)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	waitFor(t, "files to merge after catalog failures", func() bool {
		return len(liveFiles(t, d.cat, tenant.Default, "svc")) == 1
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_cycle_errors_total"); got < 1 {
		t.Fatalf("cycle_errors_total = %v, want >= 1", got)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_files_merged_total"); got != 2 {
		t.Fatalf("files_merged_total = %v, want 2", got)
	}
}

func TestRunCanceledBeforeStart(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return on canceled context")
	}
}

// TestCycleCleansOrphansWithoutStreams: the per-tenant cleanup hook runs even
// when the tenant has no live stream rows at all, so an old unreferenced
// object is removed while a fresh one inside the grace period survives.
func TestCycleCleansOrphansWithoutStreams(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)

	oldKey := objstore.NewDataKey(tenant.Default, "svc", testHour)
	freshKey := objstore.NewDataKey(tenant.Default, "svc", testHour)
	for _, k := range []string{oldKey, freshKey} {
		if err := d.fs.Put(t.Context(), k, strings.NewReader("x"), -1); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	root, err := os.OpenRoot(d.dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	defer root.Close()
	if err := root.Chtimes(filepath.FromSlash(oldKey), old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if _, err := d.fs.Stat(t.Context(), oldKey); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("old orphan still present: %v", err)
	}
	if _, err := d.fs.Stat(t.Context(), freshKey); err != nil {
		t.Fatalf("fresh orphan removed: %v", err)
	}
}
