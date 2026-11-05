package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const objectReadAhead = 64 * 1024

// ObjectReader reads a stored object through bounded range requests. It is
// safe for concurrent use. The context is used for every read and must remain
// alive for the lifetime of the reader.
//
// ObjectReader does not own or close the store. It closes each ranged response
// after copying its contents.
type ObjectReader struct {
	ctx   context.Context
	store objstore.Store
	key   string
	size  int64

	posMu sync.Mutex
	pos   int64

	cacheMu     sync.Mutex
	cacheOffset int64
	cache       []byte
}

// NewObjectReader stats key and returns a random-access reader for it. The key
// must be valid and belong to id's top-level tenant prefix.
func NewObjectReader(ctx context.Context, store objstore.Store, id tenant.ID, key string) (*ObjectReader, error) {
	if store == nil {
		return nil, fmt.Errorf("storage: open object %q: nil store", key)
	}
	if err := id.Validate(); err != nil {
		return nil, fmt.Errorf("storage: open object %q: %w", key, err)
	}
	if err := objstore.ValidateKey(key); err != nil {
		return nil, fmt.Errorf("storage: open object: %w", err)
	}
	prefix := string(id) + "/"
	if !strings.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("storage: object key %q is outside tenant %q", key, id)
	}
	info, err := store.Stat(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("storage: stat object %q: %w", key, err)
	}
	if info.Size < 0 {
		return nil, fmt.Errorf("storage: stat object %q: negative size %d", key, info.Size)
	}
	return &ObjectReader{
		ctx:   ctx,
		store: store,
		key:   key,
		size:  info.Size,
		cache: make([]byte, 0, objectReadAhead),
	}, nil
}

// Size returns the object size reported when the reader was created.
func (r *ObjectReader) Size() int64 { return r.size }

// Read reads at the current offset and advances it by the number of bytes
// read. Concurrent ReadAt calls do not change the current offset.
func (r *ObjectReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.posMu.Lock()
	defer r.posMu.Unlock()
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

// ReadAt reads p from off without changing the current offset. Successful
// small reads are cached with at most 64 KiB of read-ahead.
func (r *ObjectReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("storage: read object %q: negative offset %d", r.key, off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, fmt.Errorf("storage: read object %q: %w", r.key, err)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	want := min(int64(len(p)), r.size-off)

	r.cacheMu.Lock()
	cacheEnd := r.cacheOffset + int64(len(r.cache))
	if off >= r.cacheOffset && off+want <= cacheEnd {
		n := copy(p, r.cache[off-r.cacheOffset:off-r.cacheOffset+want])
		r.cacheMu.Unlock()
		if want < int64(len(p)) {
			return n, io.EOF
		}
		return n, nil
	}
	r.cacheMu.Unlock()

	length := min(max(int64(len(p)), int64(objectReadAhead)), r.size-off)
	data, err := r.readRange(off, length)
	if err == nil && int64(len(data)) == length && len(data) <= objectReadAhead {
		r.cacheMu.Lock()
		r.cache = append(r.cache[:0], data...)
		r.cacheOffset = off
		r.cacheMu.Unlock()
	}
	n := copy(p, data)
	if err != nil {
		return n, err
	}
	if int64(n) < want {
		return n, fmt.Errorf("storage: read object %q at offset %d: %w", r.key, off, io.ErrUnexpectedEOF)
	}
	if want < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

// Seek sets the offset for the next Read. It permits seeking beyond EOF but
// rejects a negative resulting offset.
func (r *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	r.posMu.Lock()
	defer r.posMu.Unlock()

	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.pos
	case io.SeekEnd:
		base = r.size
	default:
		return 0, fmt.Errorf("storage: seek object %q: invalid whence %d", r.key, whence)
	}
	next := base + offset
	if next < 0 {
		return 0, fmt.Errorf("storage: seek object %q: negative resulting offset %d", r.key, next)
	}
	r.pos = next
	return next, nil
}

func (r *ObjectReader) readRange(off, length int64) (data []byte, err error) {
	body, err := r.store.GetRange(r.ctx, r.key, off, length)
	if err != nil {
		return nil, fmt.Errorf("storage: read object %q at offset %d: %w", r.key, off, err)
	}
	defer func() {
		if closeErr := body.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("storage: close range for object %q: %w", r.key, closeErr)
		}
	}()

	data = make([]byte, int(length))
	n, readErr := io.ReadFull(body, data)
	data = data[:n]
	if readErr != nil {
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = io.ErrUnexpectedEOF
		}
		return data, fmt.Errorf("storage: read object %q at offset %d: %w", r.key, off, readErr)
	}
	return data, nil
}
