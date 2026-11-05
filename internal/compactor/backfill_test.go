package compactor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// enableBackfill turns the sidecar backfill pass on for s. Tests opt in per
// service so merge-focused tests keep their store counters clean.
func enableBackfill(s *service) { s.cfg.SidecarBackfill = true }

// corruptDataPage flips one byte in the first data page of the stored object
// at key, rewriting it through the raw FS so service-visible spy counters
// stay meaningful. The Parquet footer stays intact; reads fail mid-file.
func corruptDataPage(t *testing.T, d *testDeps, key string) {
	t.Helper()
	body, err := d.fs.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil {
		t.Fatalf("read %s: %v", key, readErr)
	}
	if closeErr != nil {
		t.Fatalf("close %s: %v", key, closeErr)
	}
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open parquet %s: %v", key, err)
	}
	chunk, err := pf.MetaData().RowGroup(0).ColumnChunk(0)
	if err != nil {
		t.Fatalf("column chunk %s: %v", key, err)
	}
	offset := chunk.DataPageOffset()
	if err := pf.Close(); err != nil {
		t.Fatalf("close parquet %s: %v", key, err)
	}
	corrupt := bytes.Clone(data)
	corrupt[offset] ^= 0xff
	if err := d.fs.Put(t.Context(), key, bytes.NewReader(corrupt), int64(len(corrupt))); err != nil {
		t.Fatalf("rewrite corrupt %s: %v", key, err)
	}
}

// liveByStream returns the live rows of one stream.
func liveByStream(t *testing.T, cat catalog.Catalog, tid tenant.ID, stream string) []catalog.File {
	t.Helper()
	return liveFiles(t, cat, tid, stream)
}

func TestSkipSet(t *testing.T) {
	t.Run("evicts least recently used", func(t *testing.T) {
		s := newSkipSet(2)
		s.Add("a")
		s.Add("b")
		if !s.Contains("a") { // refresh a past b
			t.Fatal("a missing right after Add")
		}
		s.Add("c") // full: evicts b, not a
		for key, want := range map[string]bool{"a": true, "b": false, "c": true} {
			if got := s.Contains(key); got != want {
				t.Fatalf("Contains(%q) = %t, want %t", key, got, want)
			}
		}
		if s.Len() != 2 {
			t.Fatalf("Len = %d, want 2", s.Len())
		}
	})
	t.Run("add refreshes existing key", func(t *testing.T) {
		s := newSkipSet(2)
		s.Add("a")
		s.Add("b")
		s.Add("a") // a becomes most recent; b is evicted next
		s.Add("c")
		if !s.Contains("a") || s.Contains("b") {
			t.Fatalf("after re-adding a: a=%t b=%t, want a=true b=false", s.Contains("a"), s.Contains("b"))
		}
	})
	t.Run("zero capacity remembers nothing", func(t *testing.T) {
		s := newSkipSet(0)
		s.Add("a")
		if s.Contains("a") {
			t.Fatal("zero-capacity set remembered a key")
		}
		if s.Len() != 0 {
			t.Fatalf("Len = %d, want 0", s.Len())
		}
	})
}

// TestBackfillAllEligibleLiveFiles: one cycle backfills every live file no
// merge covered (a file above the merge target and a single-file stream)
// while the merged output gets its sidecar from the compaction path. All
// three sidecars exist, decode, and never miss a contained needle.
func TestBackfillAllEligibleLiveFiles(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	put := func(stream string, us int64, line string, n int) catalog.File {
		ts := make([]any, n)
		lines := make([]any, n)
		lbls := make([]any, n)
		meta := make([]any, n)
		for i := range n {
			ts[i] = base + us + int64(i)
			lines[i] = line
			lbls[i] = map[string]string{}
			meta[i] = map[string]string{}
		}
		return putFile(t, d, tenant.Default, stream, testHour, sch, nil, ts, lines, lbls, meta)
	}

	small1 := put("svc", 0, "small-one", 1)
	small2 := put("svc", 100, "small-two", 1)
	big := put("svc", 200, "big-line", 2000) // above any pair target
	solo := put("solo", 0, "solo-line", 1)

	target := config.ByteSize(small1.Bytes + small2.Bytes)
	if big.Bytes < int64(target) {
		t.Fatalf("test setup: big file %d < target %d", big.Bytes, target)
	}
	s.cfg.TargetSize = target

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	svcLive := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(svcLive) != 2 {
		t.Fatalf("svc live files = %d, want merged output plus big", len(svcLive))
	}
	var merged, bigLive *catalog.File
	for i := range svcLive {
		if svcLive[i].Key == big.Key {
			bigLive = &svcLive[i]
		} else {
			merged = &svcLive[i]
		}
	}
	if merged == nil || bigLive == nil {
		t.Fatalf("svc live = %+v, want merged output and %s", svcLive, big.Key)
	}
	soloLive := liveByStream(t, d.cat, tenant.Default, "solo")
	if len(soloLive) != 1 || soloLive[0].Key != solo.Key {
		t.Fatalf("solo live = %+v, want %s untouched", soloLive, solo.Key)
	}
	if !merged.HasSidecar || !bigLive.HasSidecar || !soloLive[0].HasSidecar {
		t.Fatalf("sidecar flags: merged=%t big=%t solo=%t, want all true",
			merged.HasSidecar, bigLive.HasSidecar, soloLive[0].HasSidecar)
	}

	requireSidecar(t, d, big.Key, []string{"big-line"})
	requireSidecar(t, d, solo.Key, []string{"solo-line"})
	requireSidecar(t, d, merged.Key, []string{"small-one", "small-two"})

	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 3 {
		t.Fatalf("index_built_total = %v, want 3 (one compaction, two backfill)", got)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 2 {
		t.Fatalf("SetSidecar calls = %d, want 2", got)
	}
	if got := d.cat.replaceCalls.Load(); got != 1 {
		t.Fatalf("ReplaceFiles calls = %d, want 1", got)
	}
}

// TestBackfillDisabled: with compactor.sidecar-backfill off, files no merge
// covered stay unflagged and get no sidecar object, while merged output still
// publishes one through the compaction path.
func TestBackfillDisabled(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	solo := putFile(t, d, tenant.Default, "solo", testHour, sch, nil,
		tss(base), strs("only"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	svcLive := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(svcLive) != 1 || !svcLive[0].HasSidecar {
		t.Fatalf("svc live = %+v, want one merged file with sidecar", svcLive)
	}
	soloLive := liveByStream(t, d.cat, tenant.Default, "solo")
	if len(soloLive) != 1 || soloLive[0].HasSidecar {
		t.Fatalf("solo live = %+v, want the file left unflagged", soloLive)
	}
	if _, err := d.fs.Stat(t.Context(), objstore.SidecarKey(solo.Key)); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("solo sidecar = %v, want ErrNotExist", err)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 0 {
		t.Fatalf("SetSidecar calls = %d, want 0", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 1 {
		t.Fatalf("index_built_total = %v, want 1 (compaction only)", got)
	}
}

// TestBackfillReadFailureIsolated: an unreadable object fails only its own
// file. The corrupt file is retried on the next cycle (failures are never
// remembered as skips); its neighbors still get flagged in both passes.
func TestBackfillReadFailureIsolated(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	ok1 := putFile(t, d, tenant.Default, "one", testHour, sch, nil,
		tss(base), strs("first"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	bad := putFile(t, d, tenant.Default, "two", testHour, sch, nil,
		tss(base), strs("second"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	ok2 := putFile(t, d, tenant.Default, "three", testHour, sch, nil,
		tss(base), strs("third"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	corruptDataPage(t, d, bad.Key)

	err := s.cycle(t.Context())
	if err == nil || !strings.Contains(err.Error(), bad.Key) {
		t.Fatalf("cycle error = %v, want an isolated failure naming %s", err, bad.Key)
	}
	live := listFiles(t, d.cat, catalog.Filter{Tenant: tenant.Default})
	byKey := make(map[string]catalog.File, len(live))
	for _, f := range live {
		byKey[f.Key] = f
	}
	for key, want := range map[string]bool{ok1.Key: true, ok2.Key: true, bad.Key: false} {
		if byKey[key].HasSidecar != want {
			t.Fatalf("%s HasSidecar = %t, want %t", key, byKey[key].HasSidecar, want)
		}
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 2 {
		t.Fatalf("index_built_total = %v, want 2", got)
	}

	// The failed file is retried, not remembered: same error again, and the
	// healthy files stay flagged without being rebuilt.
	if err := s.cycle(t.Context()); err == nil || !strings.Contains(err.Error(), bad.Key) {
		t.Fatalf("second cycle error = %v, want the corrupt file retried", err)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 2 {
		t.Fatalf("index_built_total after retry = %v, want 2 (no rebuild of healthy files)", got)
	}
}

// TestBackfillMissingObjectIsolated: a catalog row whose object is gone fails
// its own file at the open, without touching the store further or blocking
// its neighbors.
func TestBackfillMissingObjectIsolated(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	gone := putFile(t, d, tenant.Default, "gone", testHour, sch, nil,
		tss(base), strs("lost"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	kept := putFile(t, d, tenant.Default, "kept", testHour, sch, nil,
		tss(base), strs("here"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	if err := d.fs.Delete(t.Context(), gone.Key); err != nil {
		t.Fatalf("delete object: %v", err)
	}

	err := s.backfillTenant(t.Context(), tenant.Default)
	if err == nil || !strings.Contains(err.Error(), gone.Key) {
		t.Fatalf("backfill error = %v, want an isolated failure naming %s", err, gone.Key)
	}
	if keptLive := liveByStream(t, d.cat, tenant.Default, "kept"); len(keptLive) != 1 || keptLive[0].Key != kept.Key || !keptLive[0].HasSidecar {
		t.Fatalf("kept live = %+v, want %s flagged", keptLive, kept.Key)
	}
	if liveByStream(t, d.cat, tenant.Default, "gone")[0].HasSidecar {
		t.Fatal("missing-object file flagged")
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 1 {
		t.Fatalf("index_built_total = %v, want 1", got)
	}
}

// TestBackfillCanceled: cancellation during a backfill sidecar Put unblocks
// the pass, returns the context error and leaves the file unflagged.
func TestBackfillCanceled(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.blockPut = true
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.backfillTenant(ctx, tenant.Default) }()
	waitFor(t, "backfill sidecar Put", func() bool { return d.store.putCalls.Load() > 0 })
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("backfillTenant error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("backfill did not return after cancel")
	}
	live := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].Key != f.Key || live[0].HasSidecar {
		t.Fatalf("live = %+v, want %s unflagged", live, f.Key)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 0 {
		t.Fatalf("SetSidecar calls = %d, want 0", got)
	}
}

// TestBackfillSetSidecarFailThenRetry: a SetSidecar failure leaves the sidecar
// object in place (Put strictly before the flag) and the row unflagged; the
// next cycle rebuilds, re-puts and flags it.
func TestBackfillSetSidecarFailThenRetry(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.cat.setSidecarFails.Store(1)
	d.cat.setSidecarErr = errSpy

	err := s.cycle(t.Context())
	if err == nil || !strings.Contains(err.Error(), "set sidecar flag") {
		t.Fatalf("cycle error = %v, want a SetSidecar failure", err)
	}
	// The object exists even though the flag was never observed: Put ran
	// strictly before SetSidecar.
	if _, err := d.fs.Stat(t.Context(), objstore.SidecarKey(f.Key)); err != nil {
		t.Fatalf("sidecar object missing after failed flag update: %v", err)
	}
	live := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].HasSidecar {
		t.Fatalf("live = %+v, want the file still unflagged", live)
	}
	putIdx := d.store.ops.index("put:" + objstore.SidecarKey(f.Key))
	setIdx := d.store.ops.index("setsidecar:" + f.Key)
	if putIdx < 0 || setIdx < 0 {
		t.Fatalf("op log %v missing sidecar put @%d or setsidecar @%d", d.store.ops.snapshot(), putIdx, setIdx)
	}
	if setIdx < putIdx {
		t.Fatalf("op order: setsidecar @%d before sidecar put @%d", setIdx, putIdx)
	}

	// Retry: the file is listed without a flag again, so it is rebuilt,
	// re-put and flagged; no error this time.
	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("retry cycle: %v", err)
	}
	live = liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || !live[0].HasSidecar {
		t.Fatalf("live after retry = %+v, want the file flagged", live)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 2 {
		t.Fatalf("SetSidecar calls = %d, want 2 (failed then retried)", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 2 {
		t.Fatalf("index_built_total = %v, want 2 (built once per pass)", got)
	}
}

// TestBackfillSetSidecarNotFoundBenign: a row that was replaced or purged
// between listing and the flag update turns SetSidecar into ErrNotFound;
// that race is benign (no error, no flag) and the sidecar object stays for
// cleanup to reclaim.
func TestBackfillSetSidecarNotFoundBenign(t *testing.T) {
	cases := []struct {
		name string
		// race runs when SetSidecar is first called; it removes the row
		// before the wrapped catalog sees the call.
		race func(t *testing.T, c *spyCatalog, key string)
		// wantRowLeft: whether the tombstone survives the race.
		wantRowLeft bool
	}{
		{"row replaced", func(t *testing.T, c *spyCatalog, key string) {
			if err := c.Catalog.ReplaceFiles(t.Context(), tenant.Default, []string{key}, nil); err != nil {
				t.Errorf("race replace: %v", err)
			}
		}, true},
		{"row purged", func(t *testing.T, c *spyCatalog, key string) {
			if err := c.Catalog.ReplaceFiles(t.Context(), tenant.Default, []string{key}, nil); err != nil {
				t.Errorf("race mark: %v", err)
			}
			if _, err := c.Catalog.PurgeDeleted(t.Context(), tenant.Default, time.Now().Add(time.Hour)); err != nil {
				t.Errorf("race purge: %v", err)
			}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t, time.Hour, 512*config.MiB)
			s := mustService(t, d)
			enableBackfill(s)
			sch := baseSchema(t)
			base := schema.TS(testHour)
			f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
				tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

			c := d.cat
			var raced bool
			c.onSetSidecar = func(tid tenant.ID, key string) {
				if raced {
					return
				}
				raced = true
				tc.race(t, c, key)
			}

			if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
				t.Fatalf("backfillTenant = %v, want nil for a benign race", err)
			}
			if got := c.setSidecarCalls.Load(); got != 1 {
				t.Fatalf("SetSidecar calls = %d, want 1", got)
			}
			if _, err := d.fs.Stat(t.Context(), objstore.SidecarKey(f.Key)); err != nil {
				t.Fatalf("sidecar object missing: %v", err)
			}
			rows := listFiles(t, d.cat, catalog.Filter{Tenant: tenant.Default, Stream: "svc", IncludeDeleted: true})
			if tc.wantRowLeft {
				if len(rows) != 1 || !rows[0].Deleted() || rows[0].HasSidecar {
					t.Fatalf("rows = %+v, want one deleted unflagged row", rows)
				}
			} else if len(rows) != 0 {
				t.Fatalf("rows = %+v, want the purged row gone", rows)
			}
		})
	}
}

// TestBackfillOverCapRememberedAndEvicted: over-cap files are never flagged
// and never given a sidecar; their skip observations are counted; a
// remembered key is not read again; and eviction (here: another tenant's
// over-cap key taking the last slot of a capacity-1 set) makes the next pass
// re-read the evicted key. The foreign key itself can never satisfy another
// file's lookup, because keys are full tenant-prefixed paths.
func TestBackfillOverCapRememberedAndEvicted(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	// The gram length is pinned (see TestCycleOverCapOutputQueryableWithout
	// Sidecar) and the cap of 8 sits below the 16 distinct 4-grams each
	// fixture line carries, so both files are over cap.
	d.deps.Config.Index.N = 4
	d.deps.Config.Index.MaxGrams = 8
	s := mustService(t, d)
	enableBackfill(s)
	s.skips = newSkipSet(1)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	mine := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("alpha-over-cap-line"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	theirs := putFile(t, d, tenant.ID("acme"), "svc", testHour, sch, nil,
		tss(base), strs("omega-over-cap-line"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	skips := func() float64 { return metricValue(t, d.reg, "parkana_index_skipped_too_many_grams_total") }
	unflagged := func() {
		t.Helper()
		for _, tid := range []tenant.ID{tenant.Default, tenant.ID("acme")} {
			for _, f := range liveByStream(t, d.cat, tid, "svc") {
				if f.HasSidecar {
					t.Fatalf("%s flagged over cap", f.Key)
				}
				if _, err := d.fs.Stat(t.Context(), objstore.SidecarKey(f.Key)); !errors.Is(err, objstore.ErrNotExist) {
					t.Fatalf("over-cap sidecar for %s: %v, want ErrNotExist", f.Key, err)
				}
			}
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
	}

	// First pass: mine is over cap and remembered.
	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("default pass: %v", err)
	}
	if got := skips(); got != 1 {
		t.Fatalf("skips = %v, want 1", got)
	}
	unflagged()

	// Second pass: the remembered key is not read at all.
	rangesBefore := d.store.rangeCalls.Load()
	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("repeat pass: %v", err)
	}
	if got := skips(); got != 1 {
		t.Fatalf("skips after repeat = %v, want 1 (remembered key not re-read)", got)
	}
	if got := d.store.rangeCalls.Load(); got != rangesBefore {
		t.Fatalf("range calls grew from %d to %d; a remembered key must not be read", rangesBefore, got)
	}

	// Another tenant's over-cap file takes the last slot and evicts mine.
	if err := s.backfillTenant(t.Context(), tenant.ID("acme")); err != nil {
		t.Fatalf("acme pass: %v", err)
	}
	if got := skips(); got != 2 {
		t.Fatalf("skips after acme pass = %v, want 2", got)
	}
	if !s.skips.Contains(theirs.Key) || s.skips.Contains(mine.Key) {
		t.Fatal("eviction left the wrong key remembered")
	}

	// The evicted key is retried on the next pass: exactly one more read.
	rangesBefore = d.store.rangeCalls.Load()
	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("post-eviction pass: %v", err)
	}
	if got := skips(); got != 3 {
		t.Fatalf("skips after eviction = %v, want 3 (evicted key retried)", got)
	}
	if got := d.store.rangeCalls.Load(); got <= rangesBefore {
		t.Fatal("evicted key was not re-read")
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 0 {
		t.Fatalf("index_built_total = %v, want 0", got)
	}
	unflagged()
}

// TestBackfillKeyTenantRejection: rows whose keys are malformed, foreign to
// the tenant or foreign to the stream are never read, written or flagged;
// their valid neighbors still complete the pass with no error.
func TestBackfillKeyTenantRejection(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	valid := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("good"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	bad := []catalog.File{
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/svc/badkey.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "other/logs/svc/2026/11/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/other/2026/11/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/svc/2026/13/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
	}
	if err := d.cat.AddFiles(t.Context(), bad...); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("backfillTenant = %v, want nil: malformed keys are skipped, not failed", err)
	}
	live := listFiles(t, d.cat, catalog.Filter{Tenant: tenant.Default})
	byKey := make(map[string]catalog.File, len(live))
	for _, f := range live {
		byKey[f.Key] = f
	}
	if !byKey[valid.Key].HasSidecar {
		t.Fatal("valid file not backfilled")
	}
	for _, b := range bad {
		if byKey[b.Key].HasSidecar {
			t.Fatalf("malformed key %s flagged", b.Key)
		}
		if idx := d.store.ops.index("stat:" + b.Key); idx >= 0 {
			t.Fatalf("malformed key %s reached the store (stat @%d)", b.Key, idx)
		}
		if idx := d.store.ops.index("put:" + objstore.SidecarKey(b.Key)); idx >= 0 {
			t.Fatalf("malformed key %s got a sidecar Put @%d", b.Key, idx)
		}
		if idx := d.store.ops.index("setsidecar:" + b.Key); idx >= 0 {
			t.Fatalf("malformed key %s was flagged @%d", b.Key, idx)
		}
	}
	if got := d.cat.setSidecarCalls.Load(); got != 1 {
		t.Fatalf("SetSidecar calls = %d, want exactly the valid file", got)
	}
}

// TestBackfillMultipleTenants: the pass is tenant-scoped through the helper:
// each tenant's files are read and flagged under their own prefixes, and one
// tenant's pass never touches another's keys.
func TestBackfillMultipleTenants(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	mine := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("mine"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	theirs := putFile(t, d, tenant.ID("acme"), "svc", testHour, sch, nil,
		tss(base), strs("theirs"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("default backfill: %v", err)
	}
	if !liveByStream(t, d.cat, tenant.Default, "svc")[0].HasSidecar {
		t.Fatal("default tenant file not flagged")
	}
	if liveByStream(t, d.cat, tenant.ID("acme"), "svc")[0].HasSidecar {
		t.Fatal("acme file flagged by the default tenant's pass")
	}
	if idx := d.store.ops.index("stat:" + theirs.Key); idx >= 0 {
		t.Fatalf("default pass read acme key @%d", idx)
	}

	if err := s.backfillTenant(t.Context(), tenant.ID("acme")); err != nil {
		t.Fatalf("acme backfill: %v", err)
	}
	if !liveByStream(t, d.cat, tenant.ID("acme"), "svc")[0].HasSidecar {
		t.Fatal("acme file not flagged by its own pass")
	}
	requireSidecar(t, d, mine.Key, []string{"mine"})
	requireSidecar(t, d, theirs.Key, []string{"theirs"})
}

// TestBackfillRunsWhenStreamsListingFails: the backfill pass needs only the
// file listing. A Streams failure skips merges for the cycle but must not
// skip backfill; the cycle still reports the listing error.
func TestBackfillRunsWhenStreamsListingFails(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.cat.streamsFails.Store(1)

	err := s.cycle(t.Context())
	if err == nil {
		t.Fatal("cycle succeeded, want the Streams failure reported")
	}
	// Merges were skipped (two live files), yet both got sidecars.
	live := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 2 {
		t.Fatalf("live files = %d, want 2: merges are skipped when Streams fails", len(live))
	}
	for _, f := range live {
		if !f.HasSidecar {
			t.Fatalf("%s not backfilled after Streams failure", f.Key)
		}
	}
	requireSidecar(t, d, f1.Key, []string{"a0"})
	requireSidecar(t, d, f2.Key, []string{"a1"})
	if got := d.cat.replaceCalls.Load(); got != 0 {
		t.Fatalf("ReplaceFiles calls = %d, want 0", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 2 {
		t.Fatalf("index_built_total = %v, want 2 (backfill only)", got)
	}
}

// errListSpy is a listing failure distinct from errSpy so a test can require
// both a per-file error and a listing error in one result.
var errListSpy = errors.New("spy: injected listing failure")

// TestBackfillListingFailures covers how the pass treats catalog listing
// trouble: an immediate listing error aborts before any file is touched, a
// listing error after a failed file keeps both errors, cancellation is
// detected before work, on each yield, and after a clean iterator end (an
// iterator that yields nothing), and accumulated per-file errors survive
// cancellation.
func TestBackfillListingFailures(t *testing.T) {
	putOne := func(t *testing.T, d *testDeps, stream, line string) {
		t.Helper()
		putFile(t, d, tenant.Default, stream, testHour, baseSchema(t), nil,
			tss(schema.TS(testHour)), strs(line), mapVals(map[string]string{}), mapVals(map[string]string{}))
	}

	t.Run("list error", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "svc", "a0")

		d.cat.listErr = errListSpy
		err := s.backfillTenant(t.Context(), tenant.Default)
		if err == nil || !strings.Contains(err.Error(), "list files") {
			t.Fatalf("backfillTenant = %v, want the listing failure", err)
		}
		if got := d.store.rangeCalls.Load(); got != 0 {
			t.Fatalf("range calls = %d, want 0", got)
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
	})
	t.Run("list error after failed file keeps both", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "svc", "a0")

		// The file's sidecar Put fails first; the listing then fails after
		// yielding it. Both errors must surface in the result.
		d.store.sidecarPutErr = errSpy
		d.cat.listErr = errListSpy
		d.cat.listErrAfter = true

		err := s.backfillTenant(t.Context(), tenant.Default)
		if err == nil {
			t.Fatal("backfillTenant = nil, want both failures")
		}
		if !errors.Is(err, errSpy) {
			t.Fatalf("backfillTenant = %v, want the per-file Put failure preserved", err)
		}
		if !errors.Is(err, errListSpy) {
			t.Fatalf("backfillTenant = %v, want the listing failure preserved", err)
		}
	})
	t.Run("canceled before work", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "svc", "a0")

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := s.backfillTenant(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
			t.Fatalf("backfillTenant = %v, want context.Canceled", err)
		}
		if got := d.cat.listCalls.Load(); got != 0 {
			t.Fatalf("ListFiles calls = %d, want 0: a canceled pass must not start work", got)
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
	})
	t.Run("clean iterator end after cancellation", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "svc", "a0")
		// A catalog that yields rows without checking the context, the way a
		// racing implementation can end cleanly after cancellation.
		d.cat.Catalog = ctxBlindLister{d.cat.Catalog}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := s.backfillTenant(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
			t.Fatalf("backfillTenant = %v, want context.Canceled", err)
		}
		if got := d.store.rangeCalls.Load(); got != 0 {
			t.Fatalf("range calls = %d, want 0", got)
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
	})
	t.Run("zero-yield clean end after cancellation", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "svc", "a0")
		inner := d.cat.Catalog
		f := liveByStream(t, d.cat, tenant.Default, "svc")[0]

		// The context dies inside the iterator, before it yields anything,
		// and the iterator then ends cleanly: no error yield, no rows. The
		// pass must still report cancellation and touch nothing.
		ctx, cancel := context.WithCancel(t.Context())
		d.cat.Catalog = cancelBeforeYieldLister{Catalog: inner, cancel: cancel}
		if err := s.backfillTenant(ctx, tenant.Default); !errors.Is(err, context.Canceled) {
			t.Fatalf("backfillTenant = %v, want context.Canceled after a zero-yield clean end", err)
		}
		cancel()
		if got := d.store.rangeCalls.Load(); got != 0 {
			t.Fatalf("range calls = %d, want 0", got)
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
		if idx := d.store.ops.index("stat:" + f.Key); idx >= 0 {
			t.Fatalf("canceled file reached the store (stat @%d)", idx)
		}
		if listFiles(t, inner, catalog.Filter{Tenant: tenant.Default, Stream: "svc"})[0].HasSidecar {
			t.Fatal("file flagged by a canceled pass")
		}
	})
	t.Run("accumulated errors preserved on cancellation", func(t *testing.T) {
		d := newTestDeps(t, time.Hour, 512*config.MiB)
		s := mustService(t, d)
		enableBackfill(s)
		putOne(t, d, "one", "a0")
		putOne(t, d, "two", "b0")
		inner := d.cat.Catalog
		second := liveByStream(t, d.cat, tenant.Default, "two")[0]

		// The first file fails; the context dies before the second is
		// processed. Both the file error and the cancellation must surface.
		d.store.sidecarPutErr = errSpy
		ctx, cancel := context.WithCancel(t.Context())
		d.cat.Catalog = cancelAfterFirstLister{Catalog: inner, cancel: cancel}

		err := s.backfillTenant(ctx, tenant.Default)
		cancel()
		if !errors.Is(err, errSpy) {
			t.Fatalf("backfillTenant = %v, want the per-file failure preserved past cancellation", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("backfillTenant = %v, want the cancellation preserved", err)
		}
		if got := listFiles(t, inner, catalog.Filter{Tenant: tenant.Default, Stream: "two"}); len(got) != 1 || got[0].Key != second.Key || got[0].HasSidecar {
			t.Fatalf("second file = %+v, want %s untouched and unflagged", got, second.Key)
		}
		if got := d.cat.setSidecarCalls.Load(); got != 0 {
			t.Fatalf("SetSidecar calls = %d, want 0", got)
		}
	})
}

// ctxBlindLister yields ListFiles rows on a detached context, so a canceled
// caller still receives files with no error.
type ctxBlindLister struct{ catalog.Catalog }

func (c ctxBlindLister) ListFiles(ctx context.Context, flt catalog.Filter) iter.Seq2[catalog.File, error] {
	return func(yield func(catalog.File, error) bool) {
		for f, err := range c.Catalog.ListFiles(context.WithoutCancel(ctx), flt) {
			if !yield(f, err) {
				return
			}
		}
	}
}

// cancelBeforeYieldLister cancels the pass context when iteration starts and
// then ends cleanly without yielding anything.
type cancelBeforeYieldLister struct {
	catalog.Catalog
	cancel context.CancelFunc
}

func (c cancelBeforeYieldLister) ListFiles(ctx context.Context, flt catalog.Filter) iter.Seq2[catalog.File, error] {
	return func(yield func(catalog.File, error) bool) {
		c.cancel()
	}
}

// cancelAfterFirstLister yields the first row, cancels the pass context and
// keeps delivering the remaining rows cleanly on a detached context, so the
// pass receives a row after cancellation without a context error yield.
type cancelAfterFirstLister struct {
	catalog.Catalog
	cancel context.CancelFunc
}

func (c cancelAfterFirstLister) ListFiles(ctx context.Context, flt catalog.Filter) iter.Seq2[catalog.File, error] {
	return func(yield func(catalog.File, error) bool) {
		first := true
		for f, err := range c.Catalog.ListFiles(context.WithoutCancel(ctx), flt) {
			if !yield(f, err) {
				return
			}
			if first {
				first = false
				c.cancel()
			}
		}
	}
}

// TestBackfillSQLiteCatalog runs the pass against a real SQLite catalog: two
// files are listed, and each SetSidecar write lands through the same backend
// while the listing iteration is still in flight. Both files end flagged with
// readable sidecars, proving the list-then-flag sequence works against the
// production catalog, not only the in-memory one.
func TestBackfillSQLiteCatalog(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	sq, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() {
		if err := sq.Close(); err != nil {
			t.Errorf("close sqlite catalog: %v", err)
		}
	})
	d.cat.Catalog = sq
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	first := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("first-row"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	second := putFile(t, d, tenant.Default, "svc", testHour.Add(time.Hour), sch, nil,
		tss(base+int64(time.Hour/time.Microsecond)), strs("second-row"),
		mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.backfillTenant(t.Context(), tenant.Default); err != nil {
		t.Fatalf("backfillTenant over sqlite: %v", err)
	}

	live := listFiles(t, sq, catalog.Filter{Tenant: tenant.Default, Stream: "svc"})
	if len(live) != 2 {
		t.Fatalf("live rows = %d, want 2", len(live))
	}
	for _, f := range live {
		if !f.HasSidecar {
			t.Fatalf("row %s not flagged through sqlite", f.Key)
		}
	}
	requireSidecar(t, d, first.Key, []string{"first-row"})
	requireSidecar(t, d, second.Key, []string{"second-row"})
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 2 {
		t.Fatalf("index_built_total = %v, want 2", got)
	}
}

// TestBackfillMetricsCountBuildsNotPuts: a sidecar Put failure still counts
// the successful encode and its duration: the build work was consumed. The
// file stays unflagged and the failure surfaces as an error.
func TestBackfillMetricsCountBuildsNotPuts(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.sidecarPutErr = errSpy

	err := s.backfillTenant(t.Context(), tenant.Default)
	if err == nil || !strings.Contains(err.Error(), "put") {
		t.Fatalf("backfill error = %v, want the sidecar Put failure", err)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 1 {
		t.Fatalf("index_built_total = %v, want 1: the encode was observed despite the Put failure", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_build_duration_seconds"); got != 1 {
		t.Fatalf("index_build_duration_seconds count = %v, want 1", got)
	}
	live := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].Key != f.Key || live[0].HasSidecar {
		t.Fatalf("live = %+v, want %s unflagged", live, f.Key)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 0 {
		t.Fatalf("SetSidecar calls = %d, want 0", got)
	}
	if !slices.Contains(d.store.ops.snapshot(), "put:"+objstore.SidecarKey(f.Key)) {
		t.Fatal("sidecar Put missing from op log")
	}
}
