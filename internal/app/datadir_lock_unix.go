//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos

package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/nathabonfim59/parkana/internal/objstore"
)

// LockFile takes an exclusive, non-blocking flock(2) on the file at path, so
// two processes of one role cannot share the directories the lock guards. It
// creates the file (mode 0640) and its parent directories (durably, as
// objstore.MkdirAllDurable) when missing. When another holder has the lock it
// returns a *LockHeldError naming the holder's pid; any other failure is a
// plain error.
//
// The holder writes its pid into the file, for that error text only. The file
// is never removed: the kernel drops the lock when unlock closes it or the
// process dies, by SIGKILL too, so a restart after a crash takes it again.
// A flock belongs to an open file, so a second LockFile on the same path in
// the same process fails as well. flock is not reliable on network
// filesystems such as NFS; keep locked directories on a local disk.
//
// unlock releases the lock; it is safe to call more than once.
func LockFile(path string) (unlock func() error, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("app: lock %s: %w", path, err)
	}
	if err := objstore.MkdirAllDurable(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("app: lock %s: %w", path, err)
	}
	f, err := openInDir(path)
	if err != nil {
		return nil, fmt.Errorf("app: lock %s: %w", path, err)
	}
	if err := flock(f, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		pid := readLockPID(f)
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, &LockHeldError{Path: path, PID: pid}
		}
		return nil, fmt.Errorf("app: lock %s: %w", path, err)
	}
	// Best effort: the pid only improves the error a second process prints,
	// so failing to record it must not stop this one from starting.
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	}
	return sync.OnceValue(func() error {
		// Closing drops the flock too; unlocking first also covers a
		// descriptor that leaked into a child process.
		return errors.Join(flock(f, syscall.LOCK_UN), f.Close())
	}), nil
}

// openInDir opens (creating) the lock file through an os.Root on its
// directory, so a symlink at the lock name cannot make it lock, or truncate,
// a file outside that directory. The root is only needed for the open.
func openInDir(path string) (*os.File, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.OpenFile(filepath.Base(path), os.O_RDWR|os.O_CREATE, 0o640)
}

// flock applies how to f, retrying when a signal interrupts the call.
func flock(f *os.File, how int) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var opErr error
	if err := conn.Control(func(fd uintptr) {
		for {
			opErr = syscall.Flock(int(fd), how)
			if opErr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	return opErr
}

// readLockPID returns the pid a holder wrote into the lock file, or 0 when it
// has written none yet or the content is not a pid.
func readLockPID(f *os.File) int {
	raw, err := io.ReadAll(io.LimitReader(f, 32))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}
