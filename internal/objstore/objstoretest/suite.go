// Package objstoretest is the conformance suite every objstore.Store
// implementation must pass.
package objstoretest

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/objstore"
)

// Run exercises s. s must be empty at the start; Run does not close it.
func Run(t *testing.T, s objstore.Store) {
	t.Helper()
	ctx := t.Context()

	put := func(t *testing.T, key, body string) {
		t.Helper()
		if err := s.Put(ctx, key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	t.Run("PutGetOverwrite", func(t *testing.T) {
		put(t, "t/a/one.parquet", "hello world")
		if got := read(s.Get(ctx, "t/a/one.parquet")); got != "hello world" {
			t.Fatalf("Get = %q", got)
		}
		put(t, "t/a/one.parquet", "v2")
		if got := read(s.Get(ctx, "t/a/one.parquet")); got != "v2" {
			t.Fatalf("Get after overwrite = %q", got)
		}
	})

	t.Run("UnknownSize", func(t *testing.T) {
		big := bytes.Repeat([]byte("0123456789"), 100_000)
		if err := s.Put(ctx, "t/big", bytes.NewReader(big), -1); err != nil {
			t.Fatal(err)
		}
		info, err := s.Stat(ctx, "t/big")
		if err != nil || info.Size != int64(len(big)) || info.Key != "t/big" {
			t.Fatalf("Stat = %+v, %v", info, err)
		}
	})

	t.Run("GetRange", func(t *testing.T) {
		put(t, "t/range", "0123456789")
		for _, tc := range []struct {
			off, n int64
			want   string
		}{{0, 4, "0123"}, {6, -1, "6789"}, {8, 10, "89"}, {10, -1, ""}} {
			if got := read(s.GetRange(ctx, "t/range", tc.off, tc.n)); got != tc.want {
				t.Errorf("GetRange(%d, %d) = %q, want %q", tc.off, tc.n, got, tc.want)
			}
		}
	})

	t.Run("NotExist", func(t *testing.T) {
		if _, err := s.Get(ctx, "t/missing"); !errors.Is(err, objstore.ErrNotExist) {
			t.Errorf("Get missing: %v, want ErrNotExist", err)
		}
		if _, err := s.Stat(ctx, "t/missing"); !errors.Is(err, objstore.ErrNotExist) {
			t.Errorf("Stat missing: %v, want ErrNotExist", err)
		}
		if err := s.Delete(ctx, "t/missing"); err != nil {
			t.Errorf("Delete missing: %v", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		for _, k := range []string{"l/x/1", "l/x/2", "l/xy/3", "l/y/4"} {
			put(t, k, k)
		}
		list := func(prefix string) []string {
			var keys []string
			for info, err := range s.List(ctx, prefix) {
				if err != nil {
					t.Fatalf("List(%q): %v", prefix, err)
				}
				keys = append(keys, info.Key)
			}
			slices.Sort(keys)
			return keys
		}
		for _, tc := range []struct {
			prefix string
			want   []string
		}{
			{"l/x/", []string{"l/x/1", "l/x/2"}},
			{"l/x", []string{"l/x/1", "l/x/2", "l/xy/3"}},
			{"l/", []string{"l/x/1", "l/x/2", "l/xy/3", "l/y/4"}},
			{"nope/", nil},
		} {
			if got := list(tc.prefix); !slices.Equal(got, tc.want) {
				t.Errorf("List(%q) = %v, want %v", tc.prefix, got, tc.want)
			}
		}
		// Early break must not panic or leak.
		for range s.List(ctx, "l/") {
			break
		}
	})

	t.Run("Delete", func(t *testing.T) {
		put(t, "t/del", "x")
		if err := s.Delete(ctx, "t/del"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Stat(ctx, "t/del"); !errors.Is(err, objstore.ErrNotExist) {
			t.Fatalf("Stat after Delete: %v", err)
		}
	})

	t.Run("InvalidKeys", func(t *testing.T) {
		for _, k := range []string{"", "/abs", "a/../b", "a//b", "a/./b", `a\b`} {
			if err := s.Put(ctx, k, strings.NewReader("x"), 1); err == nil {
				t.Errorf("Put(%q) accepted", k)
			}
		}
	})
}

// read drains rc; it takes Get's results directly. Errors come back as the
// string "error: ..." so the caller's comparison fails with the cause shown.
func read(rc io.ReadCloser, err error) string {
	if err != nil {
		return "error: " + err.Error()
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "error: " + err.Error()
	}
	return string(b)
}
