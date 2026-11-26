//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos

package app_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/app"
)

// lockHelperEnv names the lock file a re-executed test binary locks. A flock
// belongs to an open file description, so only a separate process shows what
// a second parkana process sees.
const lockHelperEnv = "PARKANA_TEST_LOCK_HELPER"

// TestLockFileHelperProcess is not a test of its own: the lock tests re-execute
// the test binary to run it as a separate process. It takes the lock named by
// lockHelperEnv, prints "locked" (or "error: ..." and exits 2), then holds the
// lock until it is killed or its stdin closes. It exits by itself after a
// minute so a lost helper cannot outlive the test run by much.
func TestLockFileHelperProcess(t *testing.T) {
	path := os.Getenv(lockHelperEnv)
	if path == "" {
		t.Skip("helper process for the LockFile tests")
	}
	time.AfterFunc(time.Minute, func() { os.Exit(3) })
	unlock, err := app.LockFile(path)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
	fmt.Println("locked")
	_, _ = io.Copy(io.Discard, os.Stdin)
	_ = unlock()
	os.Exit(0)
}

// helperCmd builds the helper process for path, bounded by a timeout and
// killed when the test ends.
func helperCmd(t *testing.T, path string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLockFileHelperProcess$")
	cmd.Env = append(os.Environ(), lockHelperEnv+"="+path)
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// signalWriter collects a helper's stdout and reports the first line that
// starts with "locked" or "error:".
type signalWriter struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	sent bool
	line chan string
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, _ := w.buf.Write(p)
	if !w.sent {
		for l := range strings.Lines(w.buf.String()) {
			if !strings.HasSuffix(l, "\n") {
				break
			}
			if strings.HasPrefix(l, "locked") || strings.HasPrefix(l, "error:") {
				w.sent = true
				w.line <- strings.TrimSpace(l)
				break
			}
		}
	}
	return n, nil
}

// startLockHolder starts a helper process that holds the lock at path and
// waits until it reports holding it. The helper is killed when the test ends.
func startLockHolder(t *testing.T, path string) *exec.Cmd {
	t.Helper()
	cmd := helperCmd(t, path)
	out := &signalWriter{line: make(chan string, 1)}
	cmd.Stdout = out
	// An open stdin pipe keeps the helper holding the lock until it is killed.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	select {
	case line := <-out.line:
		if line != "locked" {
			t.Fatalf("helper did not take the lock: %s", line)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("helper did not report the lock within 20s")
	}
	return cmd
}

func wantHeld(t *testing.T, err error, path string, pid int) {
	t.Helper()
	held, ok := errors.AsType[*app.LockHeldError](err)
	if !ok {
		t.Fatalf("err = %v, want *app.LockHeldError", err)
	}
	if held.Path != path || held.PID != pid {
		t.Errorf("held = {%q, %d}, want {%q, %d}", held.Path, held.PID, path, pid)
	}
	for _, want := range []string{path, "pid " + strconv.Itoa(pid)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestLockFileHeldByOtherProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "dir", "role.lock")
	holder := startLockHolder(t, path)

	unlock, err := app.LockFile(path)
	if err == nil {
		_ = unlock()
		t.Fatal("second lock on a held path succeeded")
	}
	wantHeld(t, err, path, holder.Process.Pid)

	// The kernel drops a flock when its holder dies, even by SIGKILL.
	if err := holder.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()

	unlock, err = app.LockFile(path)
	if err != nil {
		t.Fatalf("lock after the holder was killed: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(raw)), strconv.Itoa(os.Getpid()); got != want {
		t.Errorf("lock file holds %q, want this pid %q", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&^0o640 != 0 {
		t.Errorf("lock file mode %v, want at most 0640", perm)
	}
	if err := unlock(); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := unlock(); err != nil {
		t.Fatalf("second unlock: %v", err)
	}
}

func TestLockFileSecondProcessFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "role.lock")
	unlock, err := app.LockFile(path)
	if err != nil {
		t.Fatal(err)
	}

	out, err := helperCmd(t, path).Output()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 2 {
		t.Fatalf("helper on a held lock: err %v, output %q; want exit status 2", err, out)
	}
	want := fmt.Sprintf("is held by another process (pid %d)", os.Getpid())
	if !strings.Contains(string(out), path) || !strings.Contains(string(out), want) {
		t.Errorf("helper output %q lacks %q and the path %q", out, want, path)
	}

	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	// Released: the helper takes it, and exits because its stdin is empty.
	out, err = helperCmd(t, path).Output()
	if err != nil || !strings.HasPrefix(string(out), "locked") {
		t.Fatalf("helper after unlock: err %v, output %q", err, out)
	}
}

func TestLockFileSameProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "role.lock")
	unlock, err := app.LockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A flock belongs to an open file, so a second LockFile in the same
	// process conflicts too: two roles' assemblies in one test see each other.
	if again, err := app.LockFile(path); err == nil {
		_ = again()
		t.Fatal("second lock in the same process succeeded")
	} else {
		wantHeld(t, err, path, os.Getpid())
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("unlock removed the lock file: %v", err)
	}
	unlock, err = app.LockFile(path)
	if err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestLockFileBadParent(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	unlock, err := app.LockFile(filepath.Join(file, "role.lock"))
	if err == nil {
		_ = unlock()
		t.Fatal("lock under a regular file succeeded")
	}
	if _, ok := errors.AsType[*app.LockHeldError](err); ok {
		t.Errorf("err = %v, want a plain error, not LockHeldError", err)
	}
}

func TestLockFileRefusesEscapingSymlink(t *testing.T) {
	dir, outside := t.TempDir(), t.TempDir()
	victim := filepath.Join(outside, "victim")
	if err := os.WriteFile(victim, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "role.lock")
	if err := os.Symlink(victim, path); err != nil {
		t.Fatal(err)
	}
	unlock, err := app.LockFile(path)
	if err == nil {
		_ = unlock()
		t.Error("lock through a symlink leaving its directory succeeded")
	} else if _, ok := errors.AsType[*app.LockHeldError](err); ok {
		t.Errorf("err = %v, want a plain error, not LockHeldError", err)
	}
	if raw, err := os.ReadFile(victim); err != nil || string(raw) != "keep\n" {
		t.Errorf("file behind the symlink = %q, %v; want it untouched", raw, err)
	}
}

func TestLockHeldErrorHolder(t *testing.T) {
	for _, tc := range []struct {
		pid  int
		want string
	}{
		{0, "pid unknown"},
		{-1, "pid unknown"},
		{42, "pid 42"},
	} {
		if got := (&app.LockHeldError{Path: "/x.lock", PID: tc.pid}).Holder(); got != tc.want {
			t.Errorf("Holder() for pid %d = %q, want %q", tc.pid, got, tc.want)
		}
	}
}
