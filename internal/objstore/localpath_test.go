package objstore_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/parkana/internal/objstore"
)

func TestFSLocalPath(t *testing.T) {
	dir := t.TempDir()
	s, err := objstore.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := []byte("parquet bytes")
	key := "default/logs/api/2026/11/01/10/x.parquet"
	if err := s.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}

	p, err := s.LocalPath(key)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(p) {
		t.Errorf("LocalPath = %q, want absolute", p)
	}
	got, err := os.ReadFile(p)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reading LocalPath: %q, %v", got, err)
	}

	for _, bad := range []string{"", "../x", "/abs", "default/logs/api/2026/11/01/10"} {
		if _, err := s.LocalPath(bad); err == nil {
			t.Errorf("LocalPath(%q): no error", bad)
		}
	}
	if _, err := s.LocalPath("default/logs/api/missing.parquet"); !errors.Is(err, objstore.ErrNotExist) {
		t.Errorf("missing key: err = %v, want ErrNotExist", err)
	}

	// A symlink escaping the root is rejected by os.Root.
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "default/logs/api/2026/11/01/10/escape.parquet")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LocalPath("default/logs/api/2026/11/01/10/escape.parquet"); err == nil {
		t.Error("escaping symlink: no error")
	}

	// A relative root still yields an absolute path.
	t.Chdir(dir)
	rel, err := objstore.NewFS(".")
	if err != nil {
		t.Fatal(err)
	}
	defer rel.Close()
	if p, err := rel.LocalPath(key); err != nil || !filepath.IsAbs(p) {
		t.Errorf("relative root: LocalPath = %q, %v", p, err)
	}
}
