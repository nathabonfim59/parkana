package ingest

// This file covers the durable per-tenant key-rotation counter: its manifest
// field, the getter and the single atomic increment RefreshUploadKey commits
// with the fresh object key.

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestManifestKeyRotationsJSON covers the on-disk format: legacy manifests
// without the member keep reading as zero, zero is omitted, and a nonzero
// count survives encode/decode and cloning.
func TestManifestKeyRotationsJSON(t *testing.T) {
	// A v1 manifest written before the field existed.
	legacy := []byte(`{"version":1,"tenant":"default","frontier":{"first":1,"last":3},"files":[],"uploads":[]}`)
	m, err := decodeManifest(legacy, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if m.KeyRotations != 0 {
		t.Fatalf("legacy manifest rotations = %d, want 0", m.KeyRotations)
	}
	// Zero stays off the wire, so an untouched manifest declares no field and
	// older readers of the same version keep decoding it.
	zero, err := encodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(zero, []byte(`"keyRotations"`)) {
		t.Fatalf("zero count encoded: %s", zero)
	}
	m.KeyRotations = 3
	data, err := encodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeManifest(data, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if got.KeyRotations != 3 {
		t.Fatalf("round trip rotations = %d, want 3", got.KeyRotations)
	}
	c := cloneManifest(got)
	if c.KeyRotations != 3 {
		t.Fatalf("clone rotations = %d, want 3", c.KeyRotations)
	}
	c.KeyRotations = 9
	if got.KeyRotations != 3 {
		t.Fatalf("clone shares counter: original = %d", got.KeyRotations)
	}
}

// refreshableUpload publishes one small file and drives a reserved receipt to
// merged, the state RefreshUploadKey accepts. Each call advances the tenant
// frontier by one generation.
func refreshableUpload(t *testing.T, ts *TenantStore, stream string) Upload {
	t.Helper()
	f := writeTestFile(t, ts, stream, testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	fr, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	next := Frontier{First: fr.Last + 1, Last: fr.Last + 1}
	if err := ts.Publish(t.Context(), next, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	u, err := ts.ReserveUpload(t.Context(), Upload{Stream: f.Stream, Hour: f.Hour, Sources: []uuid.UUID{f.ID}})
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := ts.CreateUploadTemp(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write([]byte("merged")); err != nil {
		t.Fatal(err)
	}
	if err := ts.FinalizeUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	stats := storage.FileStats{Rows: f.Rows, MinTS: f.MinTS, MaxTS: f.MaxTS}
	if err := ts.RecordMerge(t.Context(), u.ID, stats, tmp.Bytes()); err != nil {
		t.Fatal(err)
	}
	return u
}

// mustKeyRotations reads the count and fails the test on error.
func mustKeyRotations(t *testing.T, ts *TenantStore) uint64 {
	t.Helper()
	n, err := ts.KeyRotations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// mustUpload returns the only receipt, failing when it is not unique.
func mustUpload(t *testing.T, ts *TenantStore) Upload {
	t.Helper()
	uploads, err := ts.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 1 {
		t.Fatalf("uploads = %+v, want exactly one", uploads)
	}
	return uploads[0]
}

// TestLocalKeyRotationsRefreshAndIsolation proves each committed refresh
// increments exactly once and that the counter belongs to one tenant.
func TestLocalKeyRotationsRefreshAndIsolation(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	u := refreshableUpload(t, ts, "checkout")
	if got := mustKeyRotations(t, ts); got != 0 {
		t.Fatalf("initial rotations = %d, want 0", got)
	}
	key := u.Key
	for want := uint64(1); want <= 2; want++ {
		ref, err := ts.RefreshUploadKey(t.Context(), u.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Key == key {
			t.Fatalf("rotation %d reused key %q", want, key)
		}
		key = ref.Key
		if got := mustKeyRotations(t, ts); got != want {
			t.Fatalf("after %d refreshes rotations = %d", want, got)
		}
	}
	// A second tenant has its own handle and its own counter.
	other := tenant.ID("team-b")
	tsOther, err := s.Tenant(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustKeyRotations(t, tsOther); got != 0 {
		t.Fatalf("other tenant initial rotations = %d, want 0", got)
	}
	uOther := refreshableUpload(t, tsOther, "payments")
	if _, err := tsOther.RefreshUploadKey(t.Context(), uOther.ID); err != nil {
		t.Fatal(err)
	}
	if got := mustKeyRotations(t, tsOther); got != 1 {
		t.Fatalf("other tenant rotations = %d, want 1", got)
	}
	if got := mustKeyRotations(t, ts); got != 2 {
		t.Fatalf("first tenant rotations = %d, want 2 (isolated)", got)
	}
}

// TestLocalKeyRotationsDurableAndCleanup proves the count and the rekeyed
// receipt survive a real Close/OpenLocal and that upload cleanup never
// resets the cumulative counter.
func TestLocalKeyRotationsDurableAndCleanup(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	u := refreshableUpload(t, ts, "checkout")
	ref, err := ts.RefreshUploadKey(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustKeyRotations(t, ts); got != 1 {
		t.Fatalf("rotations = %d, want 1", got)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ts2 := testTenantStore(t, s2)
	if got := mustKeyRotations(t, ts2); got != 1 {
		t.Fatalf("reopened rotations = %d, want 1", got)
	}
	if got := mustUpload(t, ts2); got.Key != ref.Key {
		t.Fatalf("reopened key = %q, want %q", got.Key, ref.Key)
	}

	// Complete the upload lifecycle: the receipt and its sources disappear,
	// but the historical rotation count must not.
	if err := ts2.RecordPut(t.Context(), u.ID, testHour.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := ts2.MarkPublished(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	if err := ts2.CompleteUpload(t.Context(), u.ID); err != nil {
		t.Fatal(err)
	}
	uploads, err := ts2.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 0 {
		t.Fatalf("receipts after cleanup = %+v", uploads)
	}
	if got := mustKeyRotations(t, ts2); got != 1 {
		t.Fatalf("rotations after cleanup = %d, want 1", got)
	}

	// And the cleaned-up snapshot still carries it across another reopen.
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if got := mustKeyRotations(t, testTenantStore(t, s3)); got != 1 {
		t.Fatalf("rotations after cleanup reopen = %d, want 1", got)
	}
}

// manifestFaultFile fails one durability step of the manifest temp write when
// armed, counting how often the intended commit was actually reached.
type manifestFaultFile struct {
	syncFile
	name      string
	armed     *bool
	hits      *int
	failWrite bool
}

func (f *manifestFaultFile) Write(p []byte) (int, error) {
	if f.failWrite && *f.armed && f.name == manifestTempName {
		*f.hits++
		return 0, errInjected
	}
	return f.syncFile.Write(p)
}

func (f *manifestFaultFile) Sync() error {
	if !f.failWrite && *f.armed && f.name == manifestTempName {
		*f.hits++
		return errInjected
	}
	return f.syncFile.Sync()
}

// TestLocalKeyRotationsPreRenameFailure proves a failed manifest commit before
// the rename leaves both the durable key and the counter unchanged, in memory
// and after a real reopen, and that the failure was actually injected at the
// refresh commit (not at some earlier write).
func TestLocalKeyRotationsPreRenameFailure(t *testing.T) {
	cases := []struct {
		name  string
		apply func(ops *localFileOps, armed *bool, hits *int)
	}{
		{
			name: "write",
			apply: func(ops *localFileOps, armed *bool, hits *int) {
				def := ops.create
				ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
					f, err := def(root, name, flag, perm)
					if err != nil {
						return nil, err
					}
					return &manifestFaultFile{syncFile: f, name: name, armed: armed, hits: hits, failWrite: true}, nil
				}
			},
		},
		{
			name: "fsync",
			apply: func(ops *localFileOps, armed *bool, hits *int) {
				def := ops.create
				ops.create = func(root *os.Root, name string, flag int, perm os.FileMode) (syncFile, error) {
					f, err := def(root, name, flag, perm)
					if err != nil {
						return nil, err
					}
					return &manifestFaultFile{syncFile: f, name: name, armed: armed, hits: hits}, nil
				}
			},
		},
		{
			name: "rename",
			apply: func(ops *localFileOps, armed *bool, hits *int) {
				def := ops.rename
				ops.rename = func(root *os.Root, old, new string) error {
					if *armed && old == manifestTempName {
						*hits++
						return errInjected
					}
					return def(root, old, new)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			armed := false
			hits := 0
			s, dir := testStore(t, func(ops *localFileOps) { tc.apply(ops, &armed, &hits) })
			ts := testTenantStore(t, s)
			u := refreshableUpload(t, ts, "checkout")

			armed = true
			if _, err := ts.RefreshUploadKey(t.Context(), u.ID); !errors.Is(err, errInjected) {
				t.Fatalf("refresh = %v, want the injected commit failure", err)
			}
			if hits != 1 {
				t.Fatalf("fault reached %d times, want exactly the manifest commit", hits)
			}
			armed = false

			if got := mustKeyRotations(t, ts); got != 0 {
				t.Fatalf("rotations after failed commit = %d, want 0", got)
			}
			if got := mustUpload(t, ts); got.Key != u.Key {
				t.Fatalf("key after failed commit = %q, want %q", got.Key, u.Key)
			}

			// The failure must not be observable after a reopen either.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := OpenLocal(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			ts2 := testTenantStore(t, s2)
			if got := mustKeyRotations(t, ts2); got != 0 {
				t.Fatalf("reopened rotations = %d, want 0", got)
			}
			if got := mustUpload(t, ts2); got.Key != u.Key {
				t.Fatalf("reopened key = %q, want %q", got.Key, u.Key)
			}
			// Sanity: the receipt was refreshable all along, so the injected
			// fault was the only reason the first attempt failed.
			ref, err := ts2.RefreshUploadKey(t.Context(), u.ID)
			if err != nil {
				t.Fatal(err)
			}
			if ref.Key == u.Key {
				t.Fatalf("retry reused key %q", ref.Key)
			}
			if got := mustKeyRotations(t, ts2); got != 1 {
				t.Fatalf("rotations after retry = %d, want 1", got)
			}
		})
	}
}

// TestLocalKeyRotationsPendingRenameSync proves the rename-then-fsync-failure
// window: reads fail until the pending directory sync succeeds, then reveal
// exactly the one committed increment and never count it twice.
func TestLocalKeyRotationsPendingRenameSync(t *testing.T) {
	failing := false
	s, dir := testStore(t, func(ops *localFileOps) {
		def := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if failing && name == "." {
				return errInjected
			}
			return def(root, name)
		}
	})
	ts := testTenantStore(t, s)
	u := refreshableUpload(t, ts, "checkout")

	failing = true // the tenant-dir fsync after the manifest rename fails
	if _, err := ts.RefreshUploadKey(t.Context(), u.ID); !errors.Is(err, errInjected) {
		t.Fatalf("refresh = %v, want the injected dir-sync failure", err)
	}
	// The rename already happened: the on-disk manifest carries the increment
	// even though no read is allowed to reveal it yet.
	data, err := ts.root.ReadFile(manifestName)
	if err != nil {
		t.Fatal(err)
	}
	disk, err := decodeManifest(data, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if disk.KeyRotations != 1 {
		t.Fatalf("renamed manifest rotations = %d, want 1", disk.KeyRotations)
	}
	// Unconfirmed state is never reported.
	if _, err := ts.KeyRotations(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("rotations during pending sync = %v", err)
	}
	if _, err := ts.Uploads(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("uploads during pending sync = %v", err)
	}
	// Completing the retried directory sync reveals the single committed
	// increment; it is not counted again.
	failing = false
	if got := mustKeyRotations(t, ts); got != 1 {
		t.Fatalf("rotations after retry = %d, want 1", got)
	}
	ref := mustUpload(t, ts)
	if ref.Key == u.Key {
		t.Fatalf("retry did not reveal the committed key, still %q", ref.Key)
	}
	// Further reads must not increment anything.
	if got := mustKeyRotations(t, ts); got != 1 {
		t.Fatalf("second read rotations = %d, want 1", got)
	}

	// The renamed manifest holds the increment; reopening confirms it was not
	// rolled back and not double-counted.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenLocal(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	ts2 := testTenantStore(t, s2)
	if got := mustKeyRotations(t, ts2); got != 1 {
		t.Fatalf("reopened rotations = %d, want 1", got)
	}
	if got := mustUpload(t, ts2); got.Key != ref.Key {
		t.Fatalf("reopened key = %q, want %q", got.Key, ref.Key)
	}
}

// TestLocalKeyRotationsOverflow proves the counter rejects wrapping without
// mutating the key or the count.
func TestLocalKeyRotationsOverflow(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	u := refreshableUpload(t, ts, "checkout")

	ts.mu.Lock()
	ts.state.KeyRotations = math.MaxUint64
	ts.mu.Unlock()
	if _, err := ts.RefreshUploadKey(t.Context(), u.ID); !errors.Is(err, ErrKeyRotationOverflow) {
		t.Fatalf("refresh at max = %v, want overflow", err)
	}
	if got := mustKeyRotations(t, ts); got != math.MaxUint64 {
		t.Fatalf("rotations after overflow = %d, want %d", got, uint64(math.MaxUint64))
	}
	if got := mustUpload(t, ts); got.Key != u.Key {
		t.Fatalf("overflow changed key to %q", got.Key)
	}

	// One below the maximum is still accepted and lands exactly on it.
	ts.mu.Lock()
	ts.state.KeyRotations = math.MaxUint64 - 1
	ts.mu.Unlock()
	ref, err := ts.RefreshUploadKey(t.Context(), u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Key == u.Key {
		t.Fatalf("last rotation reused key %q", ref.Key)
	}
	if got := mustKeyRotations(t, ts); got != math.MaxUint64 {
		t.Fatalf("rotations after last rotation = %d, want %d", got, uint64(math.MaxUint64))
	}
}

// TestLocalRecoverySyncsTenantRoot proves a manifest rename whose tenant
// directory fsync never completed (a crash window) is not exposed by recovery:
// the reopened tenant root is fsynced before any frontier or count is
// reported, and a failed root sync fails the open.
func TestLocalRecoverySyncsTenantRoot(t *testing.T) {
	failing := false
	s, dir := testStore(t, func(ops *localFileOps) {
		def := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			if failing && name == "." {
				return errInjected
			}
			return def(root, name)
		}
	})
	ts := testTenantStore(t, s)
	u := refreshableUpload(t, ts, "checkout")
	base, err := ts.Frontier(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Only the refresh commit's final tenant-dir fsync fails: the rename that
	// carries the new key and the count already happened.
	failing = true
	if _, err := ts.RefreshUploadKey(t.Context(), u.ID); !errors.Is(err, errInjected) {
		t.Fatalf("refresh = %v, want the injected dir-sync failure", err)
	}
	failing = false
	// Simulate the crash: never retry the pending sync, just drop the process.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with tenant-root fsyncs failing: recovery must refuse to expose
	// coverage it cannot confirm. The parquet and data roots contain no
	// manifest, so this hook only matches the tenant root.
	failOpen := true
	tenantSyncs := 0
	ops := defaultLocalOps()
	defSync := ops.syncDir
	ops.syncDir = func(root *os.Root, name string) error {
		if name == "." {
			if _, err := root.Stat(manifestName); err == nil {
				tenantSyncs++
				if failOpen {
					return errInjected
				}
			}
		}
		return defSync(root, name)
	}
	if s2, err := openLocal(t.Context(), dir, ops); !errors.Is(err, errInjected) {
		if s2 != nil {
			s2.Close()
		}
		t.Fatalf("open = %v, want recovery to refuse the unconfirmed tenant root", err)
	}
	if tenantSyncs != 1 {
		t.Fatalf("tenant-root syncs during failed open = %d, want 1", tenantSyncs)
	}

	// The retried open confirms the root during recovery, then exposes exactly
	// the one committed increment, the rekeyed receipt and the same frontier.
	failOpen = false
	s3, err := openLocal(t.Context(), dir, ops)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if tenantSyncs != 2 {
		t.Fatalf("tenant-root syncs during recovery = %d, want 2 (one failed attempt, one success)", tenantSyncs)
	}
	ts3 := testTenantStore(t, s3)
	if got := mustKeyRotations(t, ts3); got != 1 {
		t.Fatalf("recovered rotations = %d, want exactly 1", got)
	}
	if got, err := ts3.Frontier(t.Context()); err != nil || got != base {
		t.Fatalf("recovered frontier = %+v, %v; want %+v", got, err, base)
	}
	if got := mustUpload(t, ts3); got.Key == u.Key {
		t.Fatalf("recovered key still %q, want the committed rekey", got.Key)
	}
}

// TestLocalRecoveryQuarantineSyncFails proves reconcile's quarantine
// directory syncs propagate: an open that cannot confirm the rename fails
// instead of silently exposing the recovered state.
func TestLocalRecoveryQuarantineSyncFails(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	f, err := ts.root.OpenFile("stray.parquet", os.O_CREATE|os.O_WRONLY, filePerm)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	ops := defaultLocalOps()
	def := ops.syncDir
	ops.syncDir = func(root *os.Root, name string) error {
		if name == quarantineDir {
			return errInjected
		}
		return def(root, name)
	}
	if s2, err := openLocal(t.Context(), dir, ops); !errors.Is(err, errInjected) {
		if s2 != nil {
			s2.Close()
		}
		t.Fatalf("open with failed quarantine sync = %v", err)
	}
	// With the sync working the same snapshot recovers cleanly.
	s3, err := openLocal(t.Context(), dir, defaultLocalOps())
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if _, err := testTenantStore(t, s3).root.Stat("stray.parquet"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stray after recovery = %v, want it quarantined", err)
	}
}

// TestLocalKeyRotationsGetterCancellation proves the getter honors context
// cancellation and the closed-store contract like Frontier.
func TestLocalKeyRotationsGetterCancellation(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ts.KeyRotations(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rotations = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.KeyRotations(t.Context()); !errors.Is(err, ErrLocalClosed) {
		t.Fatalf("closed rotations = %v", err)
	}
}
