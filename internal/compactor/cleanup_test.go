package compactor

import (
	"context"
	"errors"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// cleanupNow is the fixed clock every cleanup test uses.
func cleanupNow() time.Time {
	return time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
}

// cleanupHour keeps test literals short.
const cleanupHour = time.Hour

// cleanupStore is a small in-memory Store spy. It only implements the methods
// cleanup uses (List and Delete); the embedded nil Store catches accidental
// calls to the rest.
type cleanupStore struct {
	objstore.Store
	objects      map[string]time.Time
	deleteErrs   map[string]error
	listErr      error
	failList     bool
	ignorePrefix bool
	deletes      []string
	onList       func()
	listCalls    int
}

func newCleanupStore() *cleanupStore {
	return &cleanupStore{
		objects:    make(map[string]time.Time),
		deleteErrs: make(map[string]error),
	}
}

func (s *cleanupStore) put(key string, modTime time.Time) {
	s.objects[key] = modTime
}

func (s *cleanupStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.deletes = append(s.deletes, key)
	if err := s.deleteErrs[key]; err != nil {
		return err
	}
	delete(s.objects, key)
	return nil
}

func (s *cleanupStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	s.listCalls++
	return func(yield func(objstore.Info, error) bool) {
		if s.failList {
			yield(objstore.Info{}, s.listErr)
			return
		}
		first := true
		for _, key := range slices.Sorted(maps.Keys(s.objects)) {
			if err := ctx.Err(); err != nil {
				yield(objstore.Info{}, err)
				return
			}
			if !s.ignorePrefix && !strings.HasPrefix(key, prefix) {
				continue
			}
			if !yield(objstore.Info{Key: key, ModTime: s.objects[key]}, nil) {
				return
			}
			if first {
				first = false
				if s.onList != nil {
					s.onList()
				}
			}
		}
		if s.listErr != nil {
			yield(objstore.Info{}, s.listErr)
		}
	}
}

// cleanupCatalog wraps a real catalog to inject failures and count calls.
type cleanupCatalog struct {
	catalog.Catalog
	replaceErr   error
	purgeErr     error
	listErr      error
	calls        int
	replaceCalls int
	// cancelAfterList cancels cancelList once ListFiles has yielded that many
	// files, proving cleanup re-checks the context even when the iterator
	// ends without an error.
	cancelAfterList int
	cancelList      func()
}

func (c *cleanupCatalog) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []catalog.File) error {
	c.calls++
	c.replaceCalls++
	if c.replaceErr != nil {
		return c.replaceErr
	}
	return c.Catalog.ReplaceFiles(ctx, t, remove, add)
}

func (c *cleanupCatalog) PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error) {
	c.calls++
	if c.purgeErr != nil {
		return nil, c.purgeErr
	}
	return c.Catalog.PurgeDeleted(ctx, t, cutoff)
}

func (c *cleanupCatalog) ListFiles(ctx context.Context, f catalog.Filter) iter.Seq2[catalog.File, error] {
	c.calls++
	if c.listErr != nil {
		return func(yield func(catalog.File, error) bool) {
			yield(catalog.File{}, c.listErr)
		}
	}
	return func(yield func(catalog.File, error) bool) {
		seen := 0
		for file, err := range c.Catalog.ListFiles(ctx, f) {
			if !yield(file, err) {
				return
			}
			if err != nil {
				return
			}
			seen++
			if c.cancelList != nil && seen >= c.cancelAfterList {
				c.cancelList()
				return
			}
		}
	}
}

func newCleanupCatalog(t *testing.T) *cleanupCatalog {
	t.Helper()
	cat := &cleanupCatalog{Catalog: catalog.NewMemory()}
	t.Cleanup(func() {
		if err := cat.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	})
	return cat
}

func cleanupAdd(t *testing.T, cat catalog.Catalog, files ...catalog.File) {
	t.Helper()
	if err := cat.AddFiles(t.Context(), files...); err != nil {
		t.Fatalf("add files: %v", err)
	}
}

func cleanupFile(id tenant.ID, stream, key string, minTS, maxTS time.Time) catalog.File {
	return catalog.File{
		Tenant: id,
		Stream: stream,
		Key:    key,
		MinTS:  minTS,
		MaxTS:  maxTS,
		Rows:   1,
	}
}

func cleanupDeletedFile(id tenant.ID, stream, key string, minTS, maxTS, deletedAt time.Time) catalog.File {
	f := cleanupFile(id, stream, key, minTS, maxTS)
	f.DeletedAt = deletedAt
	return f
}

func cleanupFiles(t *testing.T, cat catalog.Catalog, id tenant.ID) map[string]catalog.File {
	t.Helper()
	files := make(map[string]catalog.File)
	for f, err := range cat.ListFiles(t.Context(), catalog.Filter{Tenant: id, IncludeDeleted: true}) {
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		files[f.Key] = f
	}
	return files
}

func cleanupRequireObject(t *testing.T, store *cleanupStore, key string, want bool) {
	t.Helper()
	_, ok := store.objects[key]
	if ok != want {
		t.Fatalf("object %s present = %t, want %t (deletes: %v)", key, ok, want, store.deletes)
	}
}

func TestCleanupRetentionBoundary(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	retention := 24 * time.Hour
	cutoff := now.Add(-retention)

	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	stale := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"stale.parquet",
		cutoff.Add(-48*cleanupHour), cutoff.Add(-time.Nanosecond))
	exact := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"exact.parquet",
		cutoff.Add(-48*cleanupHour), cutoff)
	fresh := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"fresh.parquet",
		cutoff.Add(-48*cleanupHour), cutoff.Add(time.Nanosecond))
	oldMinRecentMax := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"old-min.parquet",
		cutoff.Add(-72*cleanupHour), cutoff.Add(cleanupHour))
	cleanupAdd(t, cat, stale, exact, fresh, oldMinRecentMax)

	// The marked file's objects are old; retention must still not touch them.
	old := now.Add(-30 * 24 * cleanupHour)
	store.put(stale.Key, old)
	store.put(objstore.SidecarKey(stale.Key), old)

	stats, err := cleanup(t.Context(), store, cat, id, now, retention, 10*time.Minute)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if want := (cleanupStats{marked: 1}); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}

	files := cleanupFiles(t, cat, id)
	if !files[stale.Key].Deleted() {
		t.Errorf("file with MaxTS just before cutoff was not marked")
	}
	for _, f := range []catalog.File{exact, fresh, oldMinRecentMax} {
		if files[f.Key].Deleted() {
			t.Errorf("file %s with MaxTS %v was marked; MaxTS must be strictly before %v", f.Key, f.MaxTS, cutoff)
		}
	}
	cleanupRequireObject(t, store, stale.Key, true)
	cleanupRequireObject(t, store, objstore.SidecarKey(stale.Key), true)
}

func TestCleanupRetentionDisabled(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	ancient := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"ancient.parquet",
		now.Add(-1000*cleanupHour), now.Add(-999*cleanupHour))
	cleanupAdd(t, cat, ancient)
	store.put(ancient.Key, now.Add(-1000*cleanupHour))

	stats, err := cleanup(t.Context(), store, cat, id, now, 0, 10*time.Minute)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if stats != (cleanupStats{}) {
		t.Fatalf("stats = %+v, want zero", stats)
	}
	if cleanupFiles(t, cat, id)[ancient.Key].Deleted() {
		t.Error("retention 0 marked a file")
	}
	cleanupRequireObject(t, store, ancient.Key, true)
}

func TestCleanupPurgeDelayBoundary(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	delay := 10 * time.Minute
	cutoff := now.Add(-delay)
	old := now.Add(-100 * cleanupHour)

	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	expired := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"expired.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour), cutoff.Add(-time.Nanosecond))
	exact := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"exact.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour), cutoff)
	live := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"live.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour))
	cleanupAdd(t, cat, expired, exact, live)

	// expired says it has no sidecar; purge must delete the sibling anyway.
	expired.HasSidecar = false
	for _, key := range []string{expired.Key, objstore.SidecarKey(expired.Key), exact.Key, objstore.SidecarKey(exact.Key), live.Key, objstore.SidecarKey(live.Key)} {
		store.put(key, old)
	}

	stats, err := cleanup(t.Context(), store, cat, id, now, 0, delay)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if want := (cleanupStats{purged: 1}); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	cleanupRequireObject(t, store, expired.Key, false)
	cleanupRequireObject(t, store, objstore.SidecarKey(expired.Key), false)
	for _, key := range []string{exact.Key, objstore.SidecarKey(exact.Key), live.Key, objstore.SidecarKey(live.Key)} {
		cleanupRequireObject(t, store, key, true)
	}

	files := cleanupFiles(t, cat, id)
	if _, ok := files[expired.Key]; ok {
		t.Error("expired catalog row was not purged")
	}
	if _, ok := files[exact.Key]; !ok {
		t.Error("row deleted exactly at the cutoff was purged")
	}
	if files[live.Key].Deleted() {
		t.Error("live row was marked deleted")
	}
}

func TestCleanupPurgeMissingObjects(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	delay := 10 * time.Minute
	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	f := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"gone.parquet",
		now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), now.Add(-2*cleanupHour))
	f.HasSidecar = true
	cleanupAdd(t, cat, f)

	stats, err := cleanup(t.Context(), store, cat, id, now, 0, delay)
	if err != nil {
		t.Fatalf("cleanup with missing objects: %v", err)
	}
	if want := (cleanupStats{purged: 1}); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	if len(store.deletes) != 2 {
		t.Fatalf("deletes = %v, want data and sidecar", store.deletes)
	}
}

func TestCleanupPurgeRejectsForeignKeys(t *testing.T) {
	id := tenant.ID("acme")
	other := tenant.ID("other")
	now := cleanupNow()
	delay := 10 * time.Minute
	deletedAt := now.Add(-2 * cleanupHour)

	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	valid := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"valid.parquet",
		now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), deletedAt)
	// A malformed row may claim this tenant while pointing at another
	// tenant's valid data key.
	foreign := cleanupDeletedFile(id, "svc", objstore.LogPrefix(other, "svc")+"foreign.parquet",
		now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), deletedAt)
	// This one passes the prefix check but fails key validation.
	invalid := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"../escape.parquet",
		now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), deletedAt)
	cleanupAdd(t, cat, valid, foreign, invalid)

	old := now.Add(-2 * cleanupHour)
	for _, key := range []string{valid.Key, objstore.SidecarKey(valid.Key), foreign.Key, objstore.SidecarKey(foreign.Key)} {
		store.put(key, old)
	}

	stats, err := cleanup(t.Context(), store, cat, id, now, 0, delay)
	if err == nil {
		t.Fatal("cleanup with bad purge keys returned no error")
	}
	if !strings.Contains(err.Error(), "invalid key") || !strings.Contains(err.Error(), "not under tenant") {
		t.Fatalf("err = %v, want invalid-key and foreign-prefix reports", err)
	}
	// Bad rows are still catalog rows that purge dropped; their objects are
	// never touched.
	if want := (cleanupStats{purged: 3}); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	cleanupRequireObject(t, store, foreign.Key, true)
	cleanupRequireObject(t, store, objstore.SidecarKey(foreign.Key), true)
	cleanupRequireObject(t, store, valid.Key, false)
	cleanupRequireObject(t, store, objstore.SidecarKey(valid.Key), false)
	for _, key := range []string{foreign.Key, objstore.SidecarKey(foreign.Key), invalid.Key, objstore.SidecarKey(invalid.Key)} {
		if slices.Contains(store.deletes, key) {
			t.Errorf("attempted delete of rejected purge key %q", key)
		}
	}
}

func TestCleanupOrphans(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	old := now.Add(-2 * cleanupHour)

	t.Run("empty catalog old only", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		oldKey := objstore.LogPrefix(id, "svc") + "orphan.parquet"
		freshKey := objstore.LogPrefix(id, "svc") + "fresh.parquet"
		exactKey := objstore.LogPrefix(id, "svc") + "exact.parquet"
		unknownKey := objstore.LogPrefix(id, "svc") + "unknown.parquet"
		store.put(oldKey, old)
		store.put(freshKey, now.Add(-30*time.Minute))
		store.put(exactKey, now.Add(-cleanupOrphanGrace))
		store.put(unknownKey, time.Time{})

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 0)
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
		if want := (cleanupStats{orphaned: 1}); stats != want {
			t.Fatalf("stats = %+v, want %+v", stats, want)
		}
		cleanupRequireObject(t, store, oldKey, false)
		for _, key := range []string{freshKey, exactKey, unknownKey} {
			cleanupRequireObject(t, store, key, true)
		}
	})

	t.Run("stream with only deleted rows", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		deletedKey := objstore.LogPrefix(id, "known") + "deleted.parquet"
		f := cleanupDeletedFile(id, "known", deletedKey, now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), now)
		cleanupAdd(t, cat, f)
		store.put(deletedKey, old)
		store.put(objstore.SidecarKey(deletedKey), old)

		orphanKey := objstore.LogPrefix(id, "empty-stream") + "orphan.parquet"
		store.put(orphanKey, old)

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 10*time.Minute)
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
		if want := (cleanupStats{orphaned: 1}); stats != want {
			t.Fatalf("stats = %+v, want %+v", stats, want)
		}
		// The deleted row is inside the delay, so its data and sidecar are
		// protected; the stream without any row is still scanned.
		cleanupRequireObject(t, store, deletedKey, true)
		cleanupRequireObject(t, store, objstore.SidecarKey(deletedKey), true)
		cleanupRequireObject(t, store, orphanKey, false)
	})

	t.Run("live sidecar protected without HasSidecar", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		key := objstore.LogPrefix(id, "svc") + "live.parquet"
		f := cleanupFile(id, "svc", key, now.Add(-3*cleanupHour), now.Add(-2*cleanupHour))
		f.HasSidecar = false
		cleanupAdd(t, cat, f)
		store.put(key, old)
		store.put(objstore.SidecarKey(key), old)

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 0)
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
		if stats != (cleanupStats{}) {
			t.Fatalf("stats = %+v, want zero", stats)
		}
		cleanupRequireObject(t, store, key, true)
		cleanupRequireObject(t, store, objstore.SidecarKey(key), true)
	})
}

// TestCleanupProtectsSidecarWhoseFlagFailed is the service-level regression
// for a backfill whose sidecar Put succeeded but whose SetSidecar failed:
// the live row's sidecar sibling survives a later orphan scan even when the
// object is older than the grace period and the flag is still false, so the
// next backfill attempt can complete the publication instead of racing a
// reclaimed object.
func TestCleanupProtectsSidecarWhoseFlagFailed(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	enableBackfill(s)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	// Backfill builds and puts the sidecar, then the flag update fails.
	d.cat.setSidecarFails.Store(1)
	d.cat.setSidecarErr = errSpy
	if err := s.cycle(t.Context()); err == nil {
		t.Fatal("cycle succeeded, want the SetSidecar failure")
	}
	sidecar := objstore.SidecarKey(f.Key)
	if _, err := d.fs.Stat(t.Context(), sidecar); err != nil {
		t.Fatalf("sidecar missing after failed flag update: %v", err)
	}
	live := liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].HasSidecar {
		t.Fatalf("live = %+v, want the row unflagged", live)
	}

	// Age the sidecar past the orphan grace period; with backfill off, the
	// next cycle's orphan scan sees an old unreferenced-by-flag object that
	// the live row must still protect.
	old := time.Now().Add(-2 * cleanupOrphanGrace)
	root, err := os.OpenRoot(d.dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	if err := root.Chtimes(filepath.FromSlash(sidecar), old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if err := root.Close(); err != nil {
		t.Fatalf("root close: %v", err)
	}
	s.cfg.SidecarBackfill = false

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cleanup cycle: %v", err)
	}
	if _, err := d.fs.Stat(t.Context(), sidecar); err != nil {
		t.Fatalf("aged sidecar reclaimed while its row is live and retryable: %v", err)
	}
	live = liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].HasSidecar || live[0].Deleted() {
		t.Fatalf("live = %+v, want the row untouched", live)
	}

	// The protected sidecar lets the retry complete the publication.
	enableBackfill(s)
	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("retry cycle: %v", err)
	}
	live = liveByStream(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || !live[0].HasSidecar {
		t.Fatalf("live after retry = %+v, want the file flagged", live)
	}
	requireSidecar(t, d, f.Key, []string{"a0"})
}

func TestCleanupTenantIsolation(t *testing.T) {
	id := tenant.ID("t1")
	other := tenant.ID("t2")
	now := cleanupNow()
	retention := 24 * time.Hour
	cutoff := now.Add(-retention)
	old := now.Add(-2 * cleanupHour)

	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	store.ignorePrefix = true

	stale := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"stale.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour))
	t1Orphan := objstore.LogPrefix(id, "svc") + "orphan.parquet"
	otherLive := cleanupFile(other, "svc", objstore.LogPrefix(other, "svc")+"live.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour))
	otherDeleted := cleanupDeletedFile(other, "svc", objstore.LogPrefix(other, "svc")+"deleted.parquet",
		cutoff.Add(-2*cleanupHour), cutoff.Add(-cleanupHour), now.Add(-2*cleanupHour))
	cleanupAdd(t, cat, stale, otherLive, otherDeleted)

	otherOrphan := objstore.LogPrefix(other, "svc") + "orphan.parquet"
	for _, key := range []string{stale.Key, t1Orphan, otherLive.Key, otherDeleted.Key, objstore.SidecarKey(otherDeleted.Key), otherOrphan} {
		store.put(key, old)
	}

	stats, err := cleanup(t.Context(), store, cat, id, now, retention, 10*time.Minute)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if want := (cleanupStats{marked: 1, orphaned: 1}); stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}

	// The Store ignored the requested prefix; the tenant check must still
	// keep every t2 object.
	for _, key := range []string{otherLive.Key, otherDeleted.Key, objstore.SidecarKey(otherDeleted.Key), otherOrphan} {
		cleanupRequireObject(t, store, key, true)
	}
	cleanupRequireObject(t, store, t1Orphan, false)
	cleanupRequireObject(t, store, stale.Key, true)

	if !cleanupFiles(t, cat, id)[stale.Key].Deleted() {
		t.Error("t1 stale file was not marked")
	}
	otherFiles := cleanupFiles(t, cat, other)
	if otherFiles[otherLive.Key].Deleted() {
		t.Error("t2 live file was marked")
	}
	if !otherFiles[otherDeleted.Key].Deleted() {
		t.Error("t2 deleted row changed")
	}
}

func TestCleanupErrors(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	errReplace := errors.New("replace failed")
	errPurge := errors.New("purge failed")
	errList := errors.New("list failed")
	errDeleteData := errors.New("delete data failed")
	errDeleteSidecar := errors.New("delete sidecar failed")

	t.Run("retention failure still purges and scans", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		cat.replaceErr = errReplace
		store := newCleanupStore()
		stale := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"stale.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour))
		purge := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"purged.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour), now.Add(-2*cleanupHour))
		cleanupAdd(t, cat, stale, purge)
		old := now.Add(-2 * cleanupHour)
		store.put(stale.Key, old)
		orphan := objstore.LogPrefix(id, "svc") + "orphan.parquet"
		store.put(orphan, old)

		stats, err := cleanup(t.Context(), store, cat, id, now, cleanupHour, 10*time.Minute)
		if !errors.Is(err, errReplace) {
			t.Fatalf("err = %v, want %v", err, errReplace)
		}
		if want := (cleanupStats{purged: 1, orphaned: 1}); stats != want {
			t.Fatalf("stats = %+v, want %+v", stats, want)
		}
		cleanupRequireObject(t, store, stale.Key, true)
		cleanupRequireObject(t, store, orphan, false)
		if cleanupFiles(t, cat, id)[stale.Key].Deleted() {
			t.Error("stale row was marked despite ReplaceFiles failure")
		}
	})

	t.Run("purge failure still scans", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		cat.purgeErr = errPurge
		store := newCleanupStore()
		deletedKey := objstore.LogPrefix(id, "svc") + "deleted.parquet"
		purge := cleanupDeletedFile(id, "svc", deletedKey,
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour), now.Add(-2*cleanupHour))
		cleanupAdd(t, cat, purge)
		old := now.Add(-2 * cleanupHour)
		store.put(deletedKey, old)
		store.put(objstore.SidecarKey(deletedKey), old)
		orphan := objstore.LogPrefix(id, "svc") + "orphan.parquet"
		store.put(orphan, old)

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 10*time.Minute)
		if !errors.Is(err, errPurge) {
			t.Fatalf("err = %v, want %v", err, errPurge)
		}
		if want := (cleanupStats{orphaned: 1}); stats != want {
			t.Fatalf("stats = %+v, want %+v", stats, want)
		}
		cleanupRequireObject(t, store, deletedKey, true)
		cleanupRequireObject(t, store, objstore.SidecarKey(deletedKey), true)
		cleanupRequireObject(t, store, orphan, false)
	})

	t.Run("catalog list failure disables orphan scan", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		cat.listErr = errList
		store := newCleanupStore()
		orphan := objstore.LogPrefix(id, "svc") + "orphan.parquet"
		store.put(orphan, now.Add(-2*cleanupHour))

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 0)
		if !errors.Is(err, errList) {
			t.Fatalf("err = %v, want %v", err, errList)
		}
		if stats != (cleanupStats{}) {
			t.Fatalf("stats = %+v, want zero", stats)
		}
		if len(store.deletes) != 0 {
			t.Fatalf("deletes = %v, want none", store.deletes)
		}
		cleanupRequireObject(t, store, orphan, true)
	})

	t.Run("store list failure is reported", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		store.failList = true
		store.listErr = errList

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 0)
		if !errors.Is(err, errList) {
			t.Fatalf("err = %v, want %v", err, errList)
		}
		if stats != (cleanupStats{}) {
			t.Fatalf("stats = %+v, want zero", stats)
		}
		if len(store.deletes) != 0 {
			t.Fatalf("deletes = %v, want none", store.deletes)
		}
	})

	t.Run("delete failures continue and join", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		old := now.Add(-2 * cleanupHour)
		first := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"first.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour), now.Add(-2*cleanupHour))
		second := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"second.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour), now.Add(-2*cleanupHour))
		cleanupAdd(t, cat, first, second)
		for _, key := range []string{first.Key, objstore.SidecarKey(first.Key), second.Key, objstore.SidecarKey(second.Key)} {
			store.put(key, old)
		}
		store.deleteErrs[first.Key] = errDeleteData
		store.deleteErrs[objstore.SidecarKey(second.Key)] = errDeleteSidecar

		stats, err := cleanup(t.Context(), store, cat, id, now, 0, 10*time.Minute)
		if !errors.Is(err, errDeleteData) || !errors.Is(err, errDeleteSidecar) {
			t.Fatalf("err = %v, want both delete failures", err)
		}
		if want := (cleanupStats{purged: 2}); stats != want {
			t.Fatalf("stats = %+v, want %+v", stats, want)
		}
		cleanupRequireObject(t, store, first.Key, true)
		cleanupRequireObject(t, store, objstore.SidecarKey(first.Key), false)
		cleanupRequireObject(t, store, second.Key, false)
		cleanupRequireObject(t, store, objstore.SidecarKey(second.Key), true)
	})
}

func TestCleanupFailedDeleteRecoveredByOrphanScan(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	delay := 10 * time.Minute
	errDelete := errors.New("transient delete failure")

	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	f := cleanupDeletedFile(id, "svc", objstore.LogPrefix(id, "svc")+"purged.parquet",
		now.Add(-3*cleanupHour), now.Add(-2*cleanupHour), now.Add(-2*cleanupHour))
	cleanupAdd(t, cat, f)
	store.put(f.Key, now.Add(-2*cleanupHour))
	store.deleteErrs[f.Key] = errDelete

	stats, err := cleanup(t.Context(), store, cat, id, now, 0, delay)
	if !errors.Is(err, errDelete) {
		t.Fatalf("first cleanup err = %v, want %v", err, errDelete)
	}
	if want := (cleanupStats{purged: 1}); stats != want {
		t.Fatalf("first stats = %+v, want %+v", stats, want)
	}
	cleanupRequireObject(t, store, f.Key, true)

	// The catalog row is gone, so a later pass sees the leftover object as an
	// orphan once it is past the grace period.
	delete(store.deleteErrs, f.Key)
	later := now.Add(2 * cleanupHour)
	stats, err = cleanup(t.Context(), store, cat, id, later, 0, delay)
	if err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if want := (cleanupStats{orphaned: 1}); stats != want {
		t.Fatalf("second stats = %+v, want %+v", stats, want)
	}
	cleanupRequireObject(t, store, f.Key, false)
}

func TestCleanupRetentionCancellationBeforeReplace(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()
	cat := newCleanupCatalog(t)
	store := newCleanupStore()
	stale := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"stale.parquet",
		now.Add(-100*cleanupHour), now.Add(-99*cleanupHour))
	cleanupAdd(t, cat, stale)

	// Cancel on the final yielded file: the iterator then ends without an
	// error, so only an explicit context check can stop ReplaceFiles.
	ctx, cancel := context.WithCancel(t.Context())
	cat.cancelAfterList = 1
	cat.cancelList = cancel

	stats, err := cleanup(ctx, store, cat, id, now, cleanupHour, 10*time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if stats != (cleanupStats{}) {
		t.Fatalf("stats = %+v, want zero", stats)
	}
	if cat.replaceCalls != 0 {
		t.Fatalf("ReplaceFiles calls = %d, want 0", cat.replaceCalls)
	}
	if cleanupFiles(t, cat, id)[stale.Key].Deleted() {
		t.Error("file was marked after cancellation")
	}
}

func TestCleanupCancellation(t *testing.T) {
	id := tenant.ID("acme")
	now := cleanupNow()

	t.Run("before start touches nothing", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		f := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"stale.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour))
		cleanupAdd(t, cat, f)
		store.put(f.Key, now.Add(-2*cleanupHour))
		store.put(objstore.LogPrefix(id, "svc")+"orphan.parquet", now.Add(-2*cleanupHour))

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		stats, err := cleanup(ctx, store, cat, id, now, cleanupHour, 10*time.Minute)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if stats != (cleanupStats{}) {
			t.Fatalf("stats = %+v, want zero", stats)
		}
		if cat.calls != 0 {
			t.Fatalf("catalog calls = %d, want 0", cat.calls)
		}
		if len(store.deletes) != 0 {
			t.Fatalf("deletes = %v, want none", store.deletes)
		}
		if cleanupFiles(t, cat, id)[f.Key].Deleted() {
			t.Error("file was marked after cancellation")
		}
	})

	t.Run("after reference collection stops before object list", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		live := cleanupFile(id, "svc", objstore.LogPrefix(id, "svc")+"live.parquet",
			now.Add(-100*cleanupHour), now.Add(-99*cleanupHour))
		cleanupAdd(t, cat, live)
		orphan := objstore.LogPrefix(id, "svc") + "orphan.parquet"
		store.put(orphan, now.Add(-2*cleanupHour))

		ctx, cancel := context.WithCancel(t.Context())
		cat.cancelAfterList = 1
		cat.cancelList = cancel

		stats, err := cleanup(ctx, store, cat, id, now, 0, 0)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if stats != (cleanupStats{}) {
			t.Fatalf("stats = %+v, want zero", stats)
		}
		if store.listCalls != 0 {
			t.Fatalf("Store.List calls = %d, want 0", store.listCalls)
		}
		if len(store.deletes) != 0 {
			t.Fatalf("deletes = %v, want none", store.deletes)
		}
		cleanupRequireObject(t, store, orphan, true)
	})

	t.Run("mid orphan scan stops", func(t *testing.T) {
		cat := newCleanupCatalog(t)
		store := newCleanupStore()
		first := objstore.LogPrefix(id, "svc") + "first.parquet"
		second := objstore.LogPrefix(id, "svc") + "second.parquet"
		store.put(first, now.Add(-2*cleanupHour))
		store.put(second, now.Add(-2*cleanupHour))

		ctx, cancel := context.WithCancel(t.Context())
		store.onList = cancel
		stats, err := cleanup(ctx, store, cat, id, now, 0, 0)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if stats.orphaned != 1 {
			t.Fatalf("orphaned = %d, want 1", stats.orphaned)
		}
		cleanupRequireObject(t, store, first, false)
		cleanupRequireObject(t, store, second, true)
	})
}
