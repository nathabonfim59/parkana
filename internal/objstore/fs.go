package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
	"strings"
	"uuid"
)

// tmpDir holds in-flight uploads inside the root; List skips it.
const tmpDir = ".tmp"

// FS is a Store backed by a local directory. All access goes through os.Root,
// so keys can't escape the directory.
type FS struct {
	root *os.Root
}

var _ Store = (*FS)(nil)

// NewFS opens (creating if needed) dir as an object store. Put and Delete are
// durable across machine crashes: files and their directory entries are
// fsynced before they return.
func NewFS(dir string) (*FS, error) {
	if err := MkdirAllDurable(dir, 0o755); err != nil {
		return nil, fmt.Errorf("objstore: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("objstore: %w", err)
	}
	if err := root.MkdirAll(tmpDir, 0o755); err != nil {
		root.Close()
		return nil, fmt.Errorf("objstore: %w", err)
	}
	return &FS{root: root}, nil
}

// validate is ValidateKey plus rejecting the store's own temp directory.
func (s *FS) validate(key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if key == tmpDir || strings.HasPrefix(key, tmpDir+"/") {
		return fmt.Errorf("objstore: invalid key %q: %s is reserved", key, tmpDir)
	}
	return nil
}

func (s *FS) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.validate(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.root.MkdirAll(path.Dir(key), 0o755); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	tmp := path.Join(tmpDir, uuid.NewV4().String())
	f, err := s.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			s.root.Remove(tmp)
		}
	}()
	n, err := io.Copy(f, ctxReader{ctx, r})
	if err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	if size >= 0 && n != size {
		return fmt.Errorf("objstore: put %s: wrote %d bytes, expected %d", key, n, size)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	if err := s.root.Rename(tmp, key); err != nil {
		s.root.Remove(tmp)
		ok = true
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	ok = true
	// Make the rename and any directories MkdirAll created durable: sync the
	// key's directory and each ancestor up to the root. Without this a machine
	// crash can lose an object the caller already treats as stored.
	if err := s.syncDirs(path.Dir(key)); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	return nil
}

// syncDirs fsyncs dir and every ancestor directory up to and including the root.
func (s *FS) syncDirs(dir string) error {
	for {
		if err := s.syncDir(dir); err != nil {
			return err
		}
		if dir == "." {
			return nil
		}
		dir = path.Dir(dir)
	}
}

func (s *FS) syncDir(dir string) error {
	d, err := s.root.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return nil
}

func (s *FS) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.GetRange(ctx, key, 0, -1)
}

func (s *FS) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := s.validate(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("objstore: get %s: negative offset %d", key, offset)
	}
	f, err := s.root.Open(key)
	if err != nil {
		return nil, fmt.Errorf("objstore: get %s: %w", key, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("objstore: get %s: %w", key, err)
	}
	if st.IsDir() {
		f.Close()
		return nil, fmt.Errorf("objstore: get %s: %w", key, ErrNotExist)
	}
	if length < 0 {
		length = max(st.Size()-offset, 0)
	}
	return readCloser{io.NewSectionReader(f, offset, length), f}, nil
}

func (s *FS) Stat(ctx context.Context, key string) (Info, error) {
	if err := s.validate(key); err != nil {
		return Info{}, err
	}
	st, err := s.root.Stat(key)
	if err != nil {
		return Info{}, fmt.Errorf("objstore: stat %s: %w", key, err)
	}
	if st.IsDir() {
		return Info{}, fmt.Errorf("objstore: stat %s: %w", key, ErrNotExist)
	}
	return Info{Key: key, Size: st.Size(), ModTime: st.ModTime()}, nil
}

func (s *FS) List(ctx context.Context, prefix string) iter.Seq2[Info, error] {
	return func(yield func(Info, error) bool) {
		// Walk only the deepest directory the prefix fully names.
		start := "."
		if dir, _, ok := strings.CutLast(prefix, "/"); ok && dir != "" {
			start = dir
		}
		err := fs.WalkDir(s.root.FS(), start, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) && p == start {
					return fs.SkipAll
				}
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if d.IsDir() {
				if p == tmpDir {
					return fs.SkipDir
				}
				return nil
			}
			if !strings.HasPrefix(p, prefix) {
				return nil
			}
			fi, err := d.Info()
			if err != nil {
				return err
			}
			if !yield(Info{Key: p, Size: fi.Size(), ModTime: fi.ModTime()}, nil) {
				return fs.SkipAll
			}
			return nil
		})
		if err != nil {
			yield(Info{}, fmt.Errorf("objstore: list %s: %w", prefix, err))
		}
	}
}

func (s *FS) Delete(ctx context.Context, key string) error {
	if err := s.validate(key); err != nil {
		return err
	}
	if err := s.root.Remove(key); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("objstore: delete %s: %w", key, err)
	}
	if err := s.syncDir(path.Dir(key)); err != nil {
		return fmt.Errorf("objstore: delete %s: %w", key, err)
	}
	return nil
}

func (s *FS) Close() error { return s.root.Close() }

type readCloser struct {
	io.Reader
	io.Closer
}

// ctxReader stops a long copy when ctx is canceled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// MkdirAllDurable is os.MkdirAll that also makes the directory chain survive a
// machine crash: it fsyncs the parent of every component of dir, deepest first,
// up to the filesystem root. It does this on every call, not only for the
// directories it created, so a call that failed after creating directories can
// simply be retried. It is meant for startup, where the few extra fsyncs are
// cheap. Any open or sync failure is returned.
func MkdirAllDurable(dir string, perm os.FileMode) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, perm); err != nil {
		return err
	}
	for d := dir; ; {
		parent := filepath.Dir(d)
		if parent == d {
			return nil
		}
		if err := syncDirPath(parent); err != nil {
			return err
		}
		d = parent
	}
}

// syncDirPath is a variable so tests can inject sync failures.
var syncDirPath = syncPath

func syncPath(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return nil
}
