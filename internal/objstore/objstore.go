// Package objstore is the object-storage interface every role uses for
// Parquet files and sidecars, with a local-directory implementation. S3/MinIO
// implements the same interface; both must pass objstoretest.Run.
package objstore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"strings"
	"time"
)

// ErrNotExist is returned (possibly wrapped) when a key does not exist. It is
// fs.ErrNotExist, so errors.Is works across implementations.
var ErrNotExist = fs.ErrNotExist

// Info describes a stored object.
type Info struct {
	Key     string
	Size    int64
	ModTime time.Time
}

// Store is a flat key/value object store. Keys are slash-separated relative
// paths (see ValidateKey). All methods are safe for concurrent use.
type Store interface {
	// Put stores the full contents of r under key, replacing any existing
	// object. Readers never observe a partial object. size is the content
	// length, or -1 when unknown.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Get opens the object for reading.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// GetRange reads length bytes starting at offset; length < 0 reads to the
	// end. Used for Parquet footers and page reads.
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	// Stat returns the object's metadata.
	Stat(ctx context.Context, key string) (Info, error)
	// List yields every object whose key starts with prefix, in no particular
	// order (sort if you need one). Iteration stops at the first error, which
	// is yielded last.
	List(ctx context.Context, prefix string) iter.Seq2[Info, error]
	// Delete removes the object. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// Close releases resources.
	Close() error
}

// ValidateKey rejects keys that are empty, absolute, contain empty, "." or
// ".." segments, or contain a backslash or NUL.
func ValidateKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\x00") {
		return fmt.Errorf("objstore: invalid key %q", key)
	}
	for seg := range strings.SplitSeq(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("objstore: invalid key %q", key)
		}
	}
	return nil
}
