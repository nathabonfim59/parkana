package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const testReadAhead = 64 * 1024

var testKey = objstore.LogPrefix(tenant.Default, "service") + "2026/11/03/12/file.parquet"

type rangeRequest struct {
	offset int64
	length int64
}

type fakeObjectStore struct {
	objstore.Store
	data         []byte
	statErr      error
	rangeErr     error
	closeErr     error
	overrideSize bool
	statSize     int64
	getRange     func(context.Context, string, int64, int64) (io.ReadCloser, error)

	mu        sync.Mutex
	statCalls int
	requests  []rangeRequest
	open      int
	closed    int
}

func (s *fakeObjectStore) Stat(ctx context.Context, key string) (objstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return objstore.Info{}, err
	}
	s.mu.Lock()
	s.statCalls++
	s.mu.Unlock()
	if s.statErr != nil {
		return objstore.Info{}, s.statErr
	}
	size := int64(len(s.data))
	if s.overrideSize {
		size = s.statSize
	}
	return objstore.Info{Key: key, Size: size}, nil
}

func (s *fakeObjectStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.requests = append(s.requests, rangeRequest{offset: offset, length: length})
	s.mu.Unlock()
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	if s.getRange != nil {
		return s.getRange(ctx, key, offset, length)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range %d+%d", offset, length)
	}
	start := min(offset, int64(len(s.data)))
	end := start
	if length > 0 && start < int64(len(s.data)) {
		end = min(start+length, int64(len(s.data)))
	}
	return s.trackReader(bytes.NewReader(s.data[start:end])), nil
}

func (s *fakeObjectStore) trackReader(r io.Reader) io.ReadCloser {
	s.mu.Lock()
	s.open++
	s.mu.Unlock()
	return &trackedRange{Reader: r, close: func() error {
		s.mu.Lock()
		s.open--
		s.closed++
		s.mu.Unlock()
		return s.closeErr
	}}
}

func (s *fakeObjectStore) stats() (statCalls int, requests []rangeRequest, open, closed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statCalls, slices.Clone(s.requests), s.open, s.closed
}

type trackedRange struct {
	io.Reader
	close func() error
}

func (r *trackedRange) Close() error { return r.close() }

func TestNewObjectReaderValidation(t *testing.T) {
	tests := []struct {
		name     string
		id       tenant.ID
		key      string
		nilStore bool
	}{
		{name: "empty tenant", key: testKey},
		{name: "invalid tenant", id: "bad/tenant", key: testKey},
		{name: "wrong tenant", id: tenant.Default, key: "other/logs/file.parquet"},
		{name: "prefix lookalike", id: tenant.Default, key: "defaultish/logs/file.parquet"},
		{name: "tenant only", id: tenant.Default, key: "default"},
		{name: "escaping key", id: tenant.Default, key: "default/../other/file.parquet"},
		{name: "absolute key", id: tenant.Default, key: "/default/logs/file.parquet"},
		{name: "nil store", id: tenant.Default, key: testKey, nilStore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeObjectStore{data: []byte("data")}
			var backend objstore.Store = store
			if tt.nilStore {
				backend = nil
			}
			r, err := storage.NewObjectReader(t.Context(), backend, tt.id, tt.key)
			if err == nil {
				r.Read(make([]byte, 1))
				t.Fatal("NewObjectReader() succeeded, want error")
			}
			statCalls, _, _, _ := store.stats()
			if statCalls != 0 {
				t.Fatalf("Stat called %d times for invalid input", statCalls)
			}
		})
	}
}

func TestNewObjectReaderStatsObject(t *testing.T) {
	store := &fakeObjectStore{data: []byte("0123456789")}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatalf("NewObjectReader() error: %v", err)
	}
	if got := r.Size(); got != 10 {
		t.Fatalf("Size() = %d, want 10", got)
	}
	statCalls, requests, _, _ := store.stats()
	if statCalls != 1 || len(requests) != 0 {
		t.Fatalf("constructor calls: Stat=%d GetRange=%d, want 1 and 0", statCalls, len(requests))
	}
}

func TestNewObjectReaderErrors(t *testing.T) {
	t.Run("stat", func(t *testing.T) {
		want := errors.New("stat failed")
		store := &fakeObjectStore{statErr: want}
		_, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if !errors.Is(err, want) {
			t.Fatalf("error = %v, want wrapped %v", err, want)
		}
	})
	t.Run("negative size", func(t *testing.T) {
		store := &fakeObjectStore{overrideSize: true, statSize: -1}
		_, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if err == nil {
			t.Fatal("NewObjectReader() succeeded with negative size")
		}
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		store := &fakeObjectStore{data: []byte("data")}
		_, err := storage.NewObjectReader(ctx, store, tenant.Default, testKey)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	})
}

func TestObjectReaderReadAt(t *testing.T) {
	store := &fakeObjectStore{data: []byte("0123456789")}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		off     int64
		length  int
		want    string
		wantN   int
		wantEOF bool
	}{
		{name: "interior", off: 2, length: 4, want: "2345", wantN: 4},
		{name: "exact end", off: 7, length: 3, want: "789", wantN: 3},
		{name: "clipped end", off: 7, length: 5, want: "789", wantN: 3, wantEOF: true},
		{name: "at end", off: 10, length: 1, wantN: 0, wantEOF: true},
		{name: "past end", off: 20, length: 1, wantN: 0, wantEOF: true},
		{name: "empty at end", off: 10, length: 0, wantN: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.length)
			n, err := r.ReadAt(buf, tt.off)
			if n != tt.wantN || string(buf[:n]) != tt.want {
				t.Fatalf("ReadAt() = %d, %q; want %d, %q", n, buf[:n], tt.wantN, tt.want)
			}
			if tt.wantEOF && !errors.Is(err, io.EOF) {
				t.Fatalf("error = %v, want io.EOF", err)
			}
			if !tt.wantEOF && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
	if n, err := r.ReadAt(make([]byte, 1), -1); n != 0 || err == nil {
		t.Fatalf("negative ReadAt() = %d, %v; want 0 and error", n, err)
	}
}

func TestObjectReaderReadAhead(t *testing.T) {
	data := make([]byte, 4*testReadAhead)
	for i := range data {
		data[i] = byte(i * 31)
	}
	store := &fakeObjectStore{data: data}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 16)
	if _, err := r.ReadAt(buf, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(buf, 100); err != nil {
		t.Fatal(err)
	}
	_, requests, _, _ := store.stats()
	if len(requests) != 1 {
		t.Fatalf("cached read caused another request: %#v", requests)
	}
	if requests[0] != (rangeRequest{offset: 12, length: testReadAhead}) {
		t.Fatalf("first range = %#v, want 64 KiB read-ahead", requests[0])
	}

	large := make([]byte, 2*testReadAhead)
	if _, err := r.ReadAt(large, testReadAhead); err != nil {
		t.Fatal(err)
	}
	_, requests, _, _ = store.stats()
	last := requests[len(requests)-1]
	if last != (rangeRequest{offset: testReadAhead, length: 2 * testReadAhead}) {
		t.Fatalf("large range = %#v, want exact requested length", last)
	}

	if n, err := r.ReadAt(buf, int64(len(data)-8)); n != 8 || !errors.Is(err, io.EOF) {
		t.Fatalf("read at clipped end = %d, %v; want 8, io.EOF", n, err)
	}
	_, requests, _, _ = store.stats()
	last = requests[len(requests)-1]
	if last != (rangeRequest{offset: int64(len(data) - 8), length: 8}) {
		t.Fatalf("final range = %#v, want clipped to object size", last)
	}
}

func TestObjectReaderReadAndSeek(t *testing.T) {
	store := &fakeObjectStore{data: []byte("0123456789")}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, err := io.ReadFull(r, buf); n != 4 || err != nil || string(buf) != "0123" {
		t.Fatalf("Read() = %d, %v, %q", n, err, buf)
	}
	if off, err := r.Seek(-2, io.SeekCurrent); off != 2 || err != nil {
		t.Fatalf("Seek(Current) = %d, %v; want 2", off, err)
	}
	if n, err := io.ReadFull(r, buf); n != 4 || err != nil || string(buf) != "2345" {
		t.Fatalf("Read() after seek = %d, %v, %q", n, err, buf)
	}
	if off, err := r.Seek(-1, io.SeekEnd); off != 9 || err != nil {
		t.Fatalf("Seek(End) = %d, %v; want 9", off, err)
	}
	buf = make([]byte, 2)
	if n, err := r.Read(buf); n != 1 || !errors.Is(err, io.EOF) || buf[0] != '9' {
		t.Fatalf("final Read() = %d, %v, %q", n, err, buf[:n])
	}
	if off, err := r.Seek(5, io.SeekEnd); off != 15 || err != nil {
		t.Fatalf("Seek beyond end = %d, %v; want 15", off, err)
	}
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read beyond end = %d, %v; want 0, io.EOF", n, err)
	}
	if _, err := r.Seek(-16, io.SeekStart); err == nil {
		t.Fatal("Seek to negative offset succeeded")
	}
	if off, err := r.Seek(0, io.SeekCurrent); off != 15 || err != nil {
		t.Fatalf("position after rejected seek = %d, %v; want 15", off, err)
	}
	if _, err := r.Seek(math.MaxInt64, io.SeekCurrent); err == nil {
		t.Fatal("overflowing Seek succeeded")
	}
	if _, err := r.Seek(0, 99); err == nil {
		t.Fatal("Seek with invalid whence succeeded")
	}
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty Read() = %d, %v; want 0, nil", n, err)
	}
}

func TestObjectReaderConcurrentReadAt(t *testing.T) {
	data := make([]byte, 2*testReadAhead)
	for i := range data {
		data[i] = byte(i*17 + 3)
	}
	store := &fakeObjectStore{data: data}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatal(err)
	}

	const (
		workers    = 16
		iterations = 50
		length     = 137
	)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			for j := range iterations {
				off := (i*997 + j*131) % (len(data) - length)
				buf := make([]byte, length)
				n, err := r.ReadAt(buf, int64(off))
				if err != nil || n != length {
					errs[i] = fmt.Errorf("ReadAt(%d) = %d, %w", off, n, err)
					return
				}
				if !bytes.Equal(buf, data[off:off+length]) {
					errs[i] = fmt.Errorf("ReadAt(%d) returned corrupt data", off)
					return
				}
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	_, _, open, closed := store.stats()
	if open != 0 || closed == 0 {
		t.Fatalf("range lifecycle: open=%d closed=%d", open, closed)
	}
}

func TestObjectReaderCancellation(t *testing.T) {
	t.Run("before cached read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		store := &fakeObjectStore{data: []byte("0123456789")}
		r, err := storage.NewObjectReader(ctx, store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadAt(make([]byte, 2), 0); err != nil {
			t.Fatal(err)
		}
		cancel()
		_, requests, _, _ := store.stats()
		if _, err := r.ReadAt(make([]byte, 2), 3); !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadAt() error = %v, want context.Canceled", err)
		}
		_, after, _, _ := store.stats()
		if len(after) != len(requests) {
			t.Fatal("canceled cached read made another range request")
		}
	})
	t.Run("during range", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		started := make(chan struct{})
		store := &fakeObjectStore{data: []byte("0123456789")}
		store.getRange = func(ctx context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		r, err := storage.NewObjectReader(ctx, store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			_, err := r.ReadAt(make([]byte, 2), 0)
			result <- err
		}()
		<-started
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadAt() error = %v, want context.Canceled", err)
		}
	})
}

func TestObjectReaderRangeErrors(t *testing.T) {
	t.Run("get range", func(t *testing.T) {
		want := errors.New("range failed")
		store := &fakeObjectStore{data: []byte("data"), rangeErr: want}
		r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadAt(make([]byte, 2), 0); !errors.Is(err, want) {
			t.Fatalf("ReadAt() error = %v, want wrapped %v", err, want)
		}
	})
	t.Run("body read and close", func(t *testing.T) {
		want := errors.New("body failed")
		store := &fakeObjectStore{data: []byte("data")}
		store.getRange = func(context.Context, string, int64, int64) (io.ReadCloser, error) {
			return store.trackReader(iotest.ErrReader(want)), nil
		}
		r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadAt(make([]byte, 2), 0); !errors.Is(err, want) {
			t.Fatalf("ReadAt() error = %v, want wrapped %v", err, want)
		}
		_, _, open, closed := store.stats()
		if open != 0 || closed != 1 {
			t.Fatalf("range lifecycle: open=%d closed=%d, want 0 and 1", open, closed)
		}
	})
	t.Run("close", func(t *testing.T) {
		want := errors.New("close failed")
		store := &fakeObjectStore{data: []byte("data"), closeErr: want}
		r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := r.ReadAt(make([]byte, 2), 0); n != 2 || !errors.Is(err, want) {
			t.Fatalf("ReadAt() = %d, %v; want 2 and wrapped close error", n, err)
		}
	})
	t.Run("short object", func(t *testing.T) {
		store := &fakeObjectStore{data: []byte("short"), overrideSize: true, statSize: 100}
		r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if n, err := r.ReadAt(make([]byte, 5), 0); n != 5 || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("ReadAt() = %d, %v; want 5, io.ErrUnexpectedEOF", n, err)
			}
		}
		_, requests, _, _ := store.stats()
		if len(requests) != 2 {
			t.Fatalf("truncated response was cached: requests = %d, want 2", len(requests))
		}
	})
}

func TestObjectReaderEmptyObject(t *testing.T) {
	store := &fakeObjectStore{}
	r, err := storage.NewObjectReader(t.Context(), store, tenant.Default, testKey)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read() = %d, %v; want 0, io.EOF", n, err)
	}
}
