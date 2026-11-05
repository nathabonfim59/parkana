package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var errWalSync = errors.New("wal sync injected")

// TestOpenCreatesDataDirChain proves Open bootstraps a data directory with
// several missing ancestors durably (objstore.MkdirAllDurable) and that a
// reopen over the now-existing chain works.
func TestOpenCreatesDataDirChain(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "deeper", "waldata")
	w, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dir, "wal"))
	if err != nil || !st.IsDir() {
		t.Fatalf("wal dir missing: %v", err)
	}
	// Reopen over the existing chain and enqueue: rooted tenant creation.
	w2, err := Open(t.Context(), dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(t.Context())
	p, err := Encode(t.Context(), logs.PushRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Enqueue(t.Context(), tenant.Default, p); err != nil {
		t.Fatal(err)
	}
}

// TestOpenSyncsWalRootBeforeRecovery proves the wal-root fsync runs at open
// before tenant discovery, so a tenant dir left by a crashed or failed
// parent sync cannot be acknowledged as durable by recovery. Its failure
// aborts the open.
func TestOpenSyncsWalRootBeforeRecovery(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	var failAll atomic.Bool
	mk := func() fileOps {
		ops := defaultFileOps()
		def := ops.syncDir
		ops.syncDir = func(root *os.Root) error {
			calls.Add(1)
			if failAll.Load() {
				return errWalSync
			}
			return def(root)
		}
		return ops
	}
	// Call order at open: 1 = data dir, 2 = wal root (before the tenant scan).
	var syncs atomic.Int32
	gated := mk()
	def := gated.syncDir
	gated.syncDir = func(root *os.Root) error {
		if syncs.Add(1) == 2 {
			return errWalSync
		}
		return def(root)
	}
	if _, err := open(t.Context(), dir, Config{}, gated); !errors.Is(err, errWalSync) {
		t.Fatalf("open with failed wal-root sync = %v", err)
	}
	w, err := open(t.Context(), dir, Config{}, mk())
	if err != nil {
		t.Fatal(err)
	}
	p, err := Encode(t.Context(), logs.PushRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Enqueue(t.Context(), tenant.Default, p); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Simulate the crash boundary: a tenant dir created by a parent sync that
	// never ran sits in the wal root, unsynced. Reopen must fsync the wal
	// root before discovery may acknowledge it.
	if err := os.MkdirAll(filepath.Join(dir, "wal", "orphan_tenant"), 0o750); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	w2, err := open(t.Context(), dir, Config{}, mk())
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close(t.Context())
	if got := calls.Load() - before; got < 2 {
		t.Fatalf("open ran %d dir syncs, want data-dir + wal-root", got)
	}
	p2, err := Encode(tenant.With(t.Context(), tenant.ID("orphan_tenant")), logs.PushRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Enqueue(t.Context(), tenant.ID("orphan_tenant"), p2); err != nil {
		t.Fatalf("reopen lost tenant: %v", err)
	}
}
