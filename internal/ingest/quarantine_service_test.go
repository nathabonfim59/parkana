package ingest

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
)

// TestServiceRecoverySweepDeletesExpired starts the ingester over a data
// directory whose quarantine already holds a file and with the service
// clock past the retention: the sweep that runs after recovery deletes it,
// and retention 0 keeps it. The retention comes from the environment through
// config.Load, so the test exercises the flag wiring end to end.
func TestServiceRecoverySweepDeletesExpired(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention string
		kept      bool
	}{
		{"expired file deleted", "168h", false},
		{"retention 0 keeps it", "0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			ls, err := OpenLocal(t.Context(), dataDir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ls.Tenant(t.Context(), testTenant); err != nil {
				t.Fatal(err)
			}
			if err := ls.Close(); err != nil {
				t.Fatal(err)
			}
			qdir := filepath.Join(dataDir, "parquet", string(testTenant), quarantineDir)
			if err := os.MkdirAll(qdir, dirPerm); err != nil {
				t.Fatal(err)
			}
			planted := filepath.Join(qdir, "checkout__2026__10__01__00__stray.parquet")
			if err := os.WriteFile(planted, []byte("stray"), filePerm); err != nil {
				t.Fatal(err)
			}

			loaded, err := config.Load([]string{"-data.dir=" + dataDir}, func(k string) string {
				if k == "PARKANA_INGEST_QUARANTINE_RETENTION" {
					return tc.retention
				}
				return ""
			}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			deps, _ := testDeps(t, dataDir)
			ingest := loaded.Ingest
			ingest.FlushInterval = time.Hour
			ingest.UploadInterval = time.Hour
			deps.Config.Ingest = ingest
			later := time.Now().Add(400 * time.Hour)
			s := newTestService(t, deps, serviceOptions{now: func() time.Time { return later }})
			cancel, done := startRun(t, s)
			waitReady(t, s)
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}

			_, err = os.Lstat(planted)
			switch {
			case tc.kept && err != nil:
				t.Fatalf("retention %s: quarantined file removed: %v", tc.retention, err)
			case !tc.kept && !errors.Is(err, fs.ErrNotExist):
				t.Fatalf("retention %s: expired quarantined file still present after the recovery sweep (err %v)", tc.retention, err)
			}
		})
	}
}
