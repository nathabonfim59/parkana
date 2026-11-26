//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos

package main

import (
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
)

// assembleOn assembles a process from args on dataDir, with an fs object
// store and a memory catalog, and returns assemble's error instead of failing.
// A successful assembly is released by the returned function (idempotent) and
// at the latest when the test ends.
func assembleOn(t *testing.T, dataDir string, args ...string) (release func(), err error) {
	t.Helper()
	base := []string{"-data.dir=" + dataDir, "-http.addr=127.0.0.1:0", "-catalog.driver=memory", "-storage.backend=fs"}
	cfg, err := config.Load(append(base, args...), func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	a, err := assemble(t.Context(), cfg, slog.New(slog.DiscardHandler), reg, reg)
	if err != nil {
		return func() {}, err
	}
	done := false
	release = func() {
		if done {
			return
		}
		done = true
		var roles []app.Service
		for name, svc := range a.services {
			if name != "http" {
				roles = append(roles, svc)
			}
		}
		if err := releaseServices(roles); err != nil {
			t.Errorf("release services: %v", err)
		}
		a.close()
	}
	t.Cleanup(release)
	return release, nil
}

// lockFiles lists the *.lock files below dir, relative to it.
func lockFiles(t *testing.T, dir string) []string {
	t.Helper()
	var locks []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".lock") {
			rel, _ := filepath.Rel(dir, p)
			locks = append(locks, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return locks
}

func wantLockError(t *testing.T, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("second assembly succeeded")
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestAssembleTwoIngestersShareDataDir(t *testing.T) {
	dir := t.TempDir()
	release, err := assembleOn(t, dir, "-roles=ingester")
	if err != nil {
		t.Fatalf("first ingester: %v", err)
	}
	_, err = assembleOn(t, dir, "-roles=ingester")
	wantLockError(t, err,
		"ingester lock "+filepath.Join(dir, "ingester.lock"),
		"held by another parkana process (pid "+strconv.Itoa(os.Getpid())+")",
		"two ingesters cannot share -data.dir "+dir,
	)

	// Released with the first assembly: the next ingester starts.
	release()
	if _, err := assembleOn(t, dir, "-roles=ingester"); err != nil {
		t.Fatalf("ingester after the first was released: %v", err)
	}
}

func TestAssembleTwoQueriersShareCacheDir(t *testing.T) {
	dir := t.TempDir()
	if _, err := assembleOn(t, dir, "-roles=querier"); err != nil {
		t.Fatalf("first querier: %v", err)
	}
	_, err := assembleOn(t, dir, "-roles=querier")
	wantLockError(t, err,
		"querier lock "+filepath.Join(dir, "cache.lock"),
		"held by another parkana process (pid "+strconv.Itoa(os.Getpid())+")",
		"two queriers cannot share the cache dir "+filepath.Join(dir, "cache"),
	)
	// The lock sits beside the cache dir, never in it: the cache's startup
	// scan deletes every file that is not a cached object.
	if got := lockFiles(t, dir); len(got) != 1 || got[0] != "cache.lock" {
		t.Errorf("lock files %q, want [cache.lock]", got)
	}
}

func TestAssembleIngesterAndQuerierShareDataDir(t *testing.T) {
	dir := t.TempDir()
	if _, err := assembleOn(t, dir, "-roles=ingester"); err != nil {
		t.Fatalf("ingester: %v", err)
	}
	if _, err := assembleOn(t, dir, "-roles=querier"); err != nil {
		t.Fatalf("querier on the ingester's data dir: %v", err)
	}
	if _, err := assembleOn(t, dir, "-roles=compactor"); err != nil {
		t.Fatalf("compactor on the same data dir: %v", err)
	}
	if got := lockFiles(t, dir); len(got) != 2 || got[0] != "cache.lock" || got[1] != "ingester.lock" {
		t.Errorf("lock files %q, want [cache.lock ingester.lock]", got)
	}
}

func TestAssembleQuerierCustomCacheDir(t *testing.T) {
	dataA, dataB, base := t.TempDir(), t.TempDir(), t.TempDir()
	cache := filepath.Join(base, "objects-cache")
	// A trailing separator must not move the lock into the cache dir.
	if _, err := assembleOn(t, dataA, "-roles=querier", "-querier.cache-dir="+cache+"/"); err != nil {
		t.Fatalf("querier: %v", err)
	}
	if got := lockFiles(t, base); len(got) != 1 || got[0] != "objects-cache.lock" {
		t.Errorf("lock files beside the cache dir %q, want [objects-cache.lock]", got)
	}
	if got := lockFiles(t, dataA); len(got) != 0 {
		t.Errorf("lock files in the data dir %q, want none", got)
	}
	// Another data dir does not help: the cache dir is what they would share.
	_, err := assembleOn(t, dataB, "-roles=querier", "-querier.cache-dir="+cache)
	wantLockError(t, err,
		"querier lock "+cache+".lock",
		"two queriers cannot share the cache dir "+cache,
	)
}

func TestAssembleCompactorTakesNoLock(t *testing.T) {
	dir := t.TempDir()
	for range 2 {
		if _, err := assembleOn(t, dir, "-roles=compactor,scheduler"); err != nil {
			t.Fatalf("compactor: %v", err)
		}
	}
	if got := lockFiles(t, dir); len(got) != 0 {
		t.Errorf("lock files %q, want none", got)
	}
}
