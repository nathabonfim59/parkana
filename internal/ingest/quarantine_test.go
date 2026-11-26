package ingest

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"uuid"
)

// quarantineClock is a fixed instant the quarantine tests run at.
var quarantineClock = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// plantStray closes s and leaves an unreferenced Parquet-looking file in the
// tenant's stream directory with an mtime of old, so the next recovery
// quarantines it. It returns the file's tenant-relative path.
func plantStray(t *testing.T, s *LocalStore, dir string, data []byte, old time.Time) string {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(filepath.Join(dir, "parquet", string(testTenant)))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	rel := "checkout/2026/10/01/00/" + uuid.NewV7().String() + ".parquet"
	if err := root.MkdirAll(filepath.Dir(rel), dirPerm); err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(rel, data, filePerm); err != nil {
		t.Fatal(err)
	}
	if err := root.Chtimes(rel, old, old); err != nil {
		t.Fatal(err)
	}
	return rel
}

// quarantinedName is where quarantineLocked puts rel.
func quarantinedName(rel string) string {
	return quarantineDir + "/" + strings.ReplaceAll(rel, "/", "__")
}

// reopenAt reopens the store in dir with its clock fixed at now.
func reopenAt(t *testing.T, dir string, now time.Time) (*LocalStore, *TenantStore) {
	t.Helper()
	ops := defaultLocalOps()
	ops.now = func() time.Time { return now }
	s, err := openLocal(t.Context(), dir, ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, testTenantStore(t, s)
}

// A stray file written long ago gets a fresh mtime when recovery
// quarantines it: rename keeps the old mtime, which would otherwise make the
// first sweep delete a file that was only just quarantined.
func TestQuarantineRecoveryFreshMtime(t *testing.T) {
	s, dir := testStore(t, nil)
	testTenantStore(t, s)
	old := quarantineClock.Add(-90 * 24 * time.Hour)
	rel := plantStray(t, s, dir, []byte("stray"), old)
	_, ts := reopenAt(t, dir, quarantineClock)
	info, err := ts.root.Lstat(quarantinedName(rel))
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(quarantineClock) {
		t.Fatalf("quarantined mtime = %s, want the quarantine time %s", info.ModTime().UTC(), quarantineClock)
	}
}

// The fresh mtime keeps the file through the first sweep; once it has been
// in quarantine longer than the retention, the sweep deletes it.
func TestQuarantineRecoveredSurvivesFirstSweep(t *testing.T) {
	s, dir := testStore(t, nil)
	testTenantStore(t, s)
	rel := plantStray(t, s, dir, []byte("stray"), quarantineClock.Add(-90*24*time.Hour))
	_, ts := reopenAt(t, dir, quarantineClock)
	const retention = 168 * time.Hour

	res, err := ts.SweepQuarantine(t.Context(), retention, quarantineClock.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 0 || res.Usage != (QuarantineUsage{Files: 1, Bytes: 5}) {
		t.Fatalf("first sweep = %+v, want nothing deleted and 1 file of 5 bytes", res)
	}
	if _, err := ts.root.Lstat(quarantinedName(rel)); err != nil {
		t.Fatalf("quarantined file gone after first sweep: %v", err)
	}

	res, err = ts.SweepQuarantine(t.Context(), retention, quarantineClock.Add(retention+time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Usage != (QuarantineUsage{}) {
		t.Fatalf("expired sweep = %+v, want 1 deleted and nothing left", res)
	}
	if _, err := ts.root.Lstat(quarantinedName(rel)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expired file still present: %v", err)
	}
	if ts.QuarantineUsage() != (QuarantineUsage{}) {
		t.Fatalf("cached usage = %+v after sweep", ts.QuarantineUsage())
	}
}

// plantQuarantined writes name directly under .quarantine/ with mtime at.
func plantQuarantined(t *testing.T, ts *TenantStore, name string, data []byte, at time.Time) {
	t.Helper()
	if err := ts.root.MkdirAll(quarantineDir, dirPerm); err != nil {
		t.Fatal(err)
	}
	rel := quarantineDir + "/" + name
	if err := ts.root.WriteFile(rel, data, filePerm); err != nil {
		t.Fatal(err)
	}
	if err := ts.root.Chtimes(rel, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestQuarantineSweepRetention(t *testing.T) {
	const retention = 24 * time.Hour
	for _, tc := range []struct {
		name      string
		retention time.Duration
		deleted   int64
		left      QuarantineUsage
		present   []string
	}{
		{"expired file deleted", retention, 1, QuarantineUsage{Files: 1, Bytes: 3}, []string{"young"}},
		{"zero keeps forever", 0, 0, QuarantineUsage{Files: 2, Bytes: 6}, []string{"old", "young"}},
		{"negative keeps everything", -time.Hour, 0, QuarantineUsage{Files: 2, Bytes: 6}, []string{"old", "young"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t, nil)
			ts := testTenantStore(t, s)
			plantQuarantined(t, ts, "old", []byte("aaa"), quarantineClock.Add(-retention-time.Second))
			plantQuarantined(t, ts, "young", []byte("bbb"), quarantineClock.Add(-retention+time.Second))
			res, err := ts.SweepQuarantine(t.Context(), tc.retention, quarantineClock)
			if err != nil {
				t.Fatal(err)
			}
			if res.Deleted != tc.deleted || res.Usage != tc.left {
				t.Fatalf("sweep = %+v, want deleted %d, left %+v", res, tc.deleted, tc.left)
			}
			entries, err := fs.ReadDir(ts.root.FS(), quarantineDir)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			if len(names) != len(tc.present) {
				t.Fatalf("left %v, want %v", names, tc.present)
			}
			for i := range names {
				if names[i] != tc.present[i] {
					t.Fatalf("left %v, want %v", names, tc.present)
				}
			}
		})
	}
}

func TestQuarantineSweepMissingDir(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	res, err := ts.SweepQuarantine(t.Context(), time.Hour, quarantineClock)
	if err != nil || res != (SweepResult{}) {
		t.Fatalf("sweep without .quarantine = %+v, %v", res, err)
	}
}

// Symlinks (to a file outside the data directory, to a file inside the
// tenant, dangling), a directory and a socket under .quarantine are skipped:
// nothing is followed, nothing outside .quarantine is touched, and only
// regular files count in the usage.
func TestQuarantineSweepSkipsNonRegular(t *testing.T) {
	s, dir := testStore(t, nil)
	ts := testTenantStore(t, s)
	tenantDir := filepath.Join(dir, "parquet", string(testTenant))
	qdir := filepath.Join(tenantDir, quarantineDir)
	long := quarantineClock.Add(-365 * 24 * time.Hour)

	outside := filepath.Join(t.TempDir(), "outside.parquet")
	if err := os.WriteFile(outside, []byte("outside"), filePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(outside, long, long); err != nil {
		t.Fatal(err)
	}
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(tenantDir, filepath.FromSlash(f.RelPath()))
	if err := os.Chtimes(inside, long, long); err != nil {
		t.Fatal(err)
	}

	plantQuarantined(t, ts, "expired", []byte("x"), long)
	for name, target := range map[string]string{
		"link-outside":  outside,
		"link-inside":   inside,
		"link-dangling": filepath.Join(qdir, "nothing"),
	} {
		if err := os.Symlink(target, filepath.Join(qdir, name)); err != nil {
			t.Fatal(err)
		}
	}
	sub := filepath.Join(qdir, "Bad.Dir")
	if err := os.MkdirAll(sub, dirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "junk"), []byte("junk"), filePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(sub, "junk"), long, long); err != nil {
		t.Fatal(err)
	}
	// A socket is a non-regular entry that, unlike a FIFO, never blocks an
	// open. Bind by a relative name: the absolute temp path may exceed the
	// socket path limit.
	t.Chdir(qdir)
	ln, err := net.Listen("unix", "sock")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	res, err := ts.SweepQuarantine(t.Context(), time.Hour, quarantineClock.Add(1000*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 || res.Usage != (QuarantineUsage{}) {
		t.Fatalf("sweep = %+v, want only the expired regular file deleted and nothing counted", res)
	}
	if _, err := os.Lstat(filepath.Join(qdir, "expired")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expired regular file kept: %v", err)
	}
	for _, p := range []string{
		outside, inside,
		filepath.Join(qdir, "link-outside"), filepath.Join(qdir, "link-inside"), filepath.Join(qdir, "link-dangling"),
		sub, filepath.Join(sub, "junk"), filepath.Join(qdir, "sock"),
	} {
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("%s removed by the sweep: %v", p, err)
		}
	}
}

// Every quarantine raises the cached usage the gauges read, without a scan.
func TestQuarantineUsageTracksAbort(t *testing.T) {
	s, _ := testStore(t, nil)
	ts := testTenantStore(t, s)
	f := writeTestFile(t, ts, "checkout", testHour, nil, []rowSpec{{us: us(testHour, 0), line: "a"}})
	if err := ts.Publish(t.Context(), Frontier{First: 1, Last: 1}, []LocalFile{f}); err != nil {
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
	if _, err := tmp.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := ts.AbortUploadTemp(t.Context(), tmp); err != nil {
		t.Fatal(err)
	}
	if got := ts.QuarantineUsage(); got != (QuarantineUsage{Files: 1, Bytes: 7}) {
		t.Fatalf("usage after abort = %+v, want 1 file of 7 bytes", got)
	}
}

// Run sweeps once after recovery and sets the gauges; a later tick past the
// sweep interval and the retention deletes the file and zeroes them. A sweep
// failure is counted and never stops the service.
func TestServiceQuarantineGauges(t *testing.T) {
	dataDir := t.TempDir()
	ls, err := OpenLocal(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ls.Tenant(t.Context(), testTenant); err != nil {
		t.Fatal(err)
	}
	plantStray(t, ls, dataDir, []byte("stray!"), time.Now().Add(-90*24*time.Hour))

	deps, reg := testDeps(t, dataDir)
	deps.Config.Ingest.QuarantineRetention = 168 * time.Hour
	s := newTestService(t, deps, serviceOptions{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_quarantine_files", "tenant", string(testTenant)); v != 1 {
		t.Errorf("quarantine files after recovery = %v, want 1", v)
	}
	if v, _ := gatherLabeled(t, reg, "parkana_ingest_quarantine_bytes", "tenant", string(testTenant)); v != 6 {
		t.Errorf("quarantine bytes after recovery = %v, want 6", v)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Drive the tick directly on a stopped-but-open local store, with the
	// service clock past the retention.
	ls2, err := OpenLocal(t.Context(), dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ls2.Close() })
	deps2, reg2 := testDeps(t, dataDir)
	deps2.Config.Ingest.QuarantineRetention = 168 * time.Hour
	later := time.Now().Add(169 * time.Hour)
	s2 := newTestService(t, deps2, serviceOptions{now: func() time.Time { return later }})
	s2.local = ls2
	s2.lastQuarantineSweep = later.Add(-time.Minute)
	s2.quarantineTick(t.Context()) // too soon: gauges from the cache only
	if v, _ := gatherLabeled(t, reg2, "parkana_ingest_quarantine_files", "tenant", string(testTenant)); v != 1 {
		t.Errorf("quarantine files before the sweep is due = %v, want 1", v)
	}
	s2.lastQuarantineSweep = later.Add(-quarantineSweepInterval)
	s2.quarantineTick(t.Context())
	if v, _ := gatherLabeled(t, reg2, "parkana_ingest_quarantine_files", "tenant", string(testTenant)); v != 0 {
		t.Errorf("quarantine files after the sweep = %v, want 0", v)
	}
	if v, _ := gatherLabeled(t, reg2, "parkana_ingest_quarantine_bytes", "tenant", string(testTenant)); v != 0 {
		t.Errorf("quarantine bytes after the sweep = %v, want 0", v)
	}
	if !s2.lastQuarantineSweep.Equal(later) {
		t.Errorf("last sweep = %s, want %s", s2.lastQuarantineSweep, later)
	}

	// A closed store makes the sweep fail: counted, not fatal.
	ls2.Close()
	s2.lastQuarantineSweep = time.Time{}
	s2.quarantineTick(t.Context())
	if v, _ := gather(t, reg2, "parkana_ingest_quarantine_sweep_errors_total"); v != 1 {
		t.Errorf("sweep errors = %v, want 1", v)
	}
}

func TestValidateIngestQuarantineRetention(t *testing.T) {
	for _, tc := range []struct {
		retention time.Duration
		ok        bool
	}{{0, true}, {time.Hour, true}, {-time.Second, false}} {
		c := testIngest()
		c.QuarantineRetention = tc.retention
		if err := validateIngest(c); (err == nil) != tc.ok {
			t.Errorf("retention %s: validateIngest = %v, want ok=%v", tc.retention, err, tc.ok)
		}
	}
}
