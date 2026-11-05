package objstore_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/objstore/objstoretest"
)

const (
	testBucket    = "parkana-test"
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"
)

// newFakeS3 starts an in-process S3 fake, creates the test bucket through a
// separate client (NewS3 without CreateBucket never creates buckets), and
// returns a store for it.
func newFakeS3(t *testing.T) *objstore.S3 {
	t.Helper()
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)

	endpoint := strings.TrimPrefix(srv.URL, "http://")
	admin, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(testAccessKey, testSecretKey, ""),
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.MakeBucket(t.Context(), testBucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
	s, err := objstore.NewS3(objstore.S3Options{
		Endpoint:  endpoint,
		Bucket:    testBucket,
		Region:    "us-east-1",
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
		Insecure:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// s3Request is one request the fake S3 server handled.
type s3Request struct {
	method string
	host   string
	path   string
}

// recordingS3 wraps an S3 handler and records the requests it serves.
type recordingS3 struct {
	handler  http.Handler
	mu       sync.Mutex
	requests []s3Request
}

func (r *recordingS3) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.requests = append(r.requests, s3Request{method: req.Method, host: req.Host, path: req.URL.Path})
	r.mu.Unlock()
	r.handler.ServeHTTP(w, req)
}

// seen returns a copy of the requests recorded so far.
func (r *recordingS3) seen() []s3Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.requests)
}

// reset forgets recorded requests.
func (r *recordingS3) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = nil
}

// newRecordingFakeS3 starts an in-process S3 fake and returns its recording
// handler and host:port endpoint.
func newRecordingFakeS3(t *testing.T) (*recordingS3, string) {
	t.Helper()
	rec := &recordingS3{handler: gofakes3.New(s3mem.New()).Server()}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	return rec, strings.TrimPrefix(srv.URL, "http://")
}

// newS3TestOptions returns options for the in-process fake.
func newS3TestOptions(endpoint string) objstore.S3Options {
	return objstore.S3Options{
		Endpoint:  endpoint,
		Bucket:    testBucket,
		Region:    "us-east-1",
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
		Insecure:  true,
	}
}

// newTestAdmin returns a client for setting up buckets before a test.
func newTestAdmin(t *testing.T, endpoint, region string) *minio.Client {
	t.Helper()
	admin, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(testAccessKey, testSecretKey, ""),
		Region: region,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admin
}

// TestNewS3CreateBucketDisabled is the zero-network default: a missing bucket
// is neither checked nor created.
func TestNewS3CreateBucketDisabled(t *testing.T) {
	rec, endpoint := newRecordingFakeS3(t)
	s, err := objstore.NewS3(newS3TestOptions(endpoint))
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if reqs := rec.seen(); len(reqs) != 0 {
		t.Fatalf("NewS3 without CreateBucket made %d requests: %+v", len(reqs), reqs)
	}
	exists, err := newTestAdmin(t, endpoint, "us-east-1").BucketExists(t.Context(), testBucket)
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if exists {
		t.Fatal("NewS3 without CreateBucket created the missing bucket")
	}
}

// TestNewS3CreateBucket covers the opt-in path: check, create, then use.
func TestNewS3CreateBucket(t *testing.T) {
	rec, endpoint := newRecordingFakeS3(t)
	opts := newS3TestOptions(endpoint)
	opts.CreateBucket = true
	s, err := objstore.NewS3(opts)
	if err != nil {
		t.Fatalf("NewS3 with CreateBucket: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	exists, err := newTestAdmin(t, endpoint, "us-east-1").BucketExists(t.Context(), testBucket)
	if err != nil {
		t.Fatalf("BucketExists: %v", err)
	}
	if !exists {
		t.Fatal("NewS3 with CreateBucket did not create the bucket")
	}

	const key = "create/object"
	if err := s.Put(t.Context(), key, strings.NewReader("rows"), 4); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := s.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "rows" {
		t.Fatalf("Get = %q, want rows", got)
	}

	reqs := rec.seen()
	if len(reqs) < 2 || reqs[0].method != http.MethodHead || reqs[1].method != http.MethodPut {
		t.Fatalf("bucket setup requests = %+v, want HEAD then PUT", reqs)
	}
	for _, req := range reqs {
		if want := "/" + testBucket; req.path != want && !strings.HasPrefix(req.path, want+"/") {
			t.Errorf("%s %s is not a path-style request for bucket %q", req.method, req.path, testBucket)
		}
		if strings.HasPrefix(req.host, testBucket+".") {
			t.Errorf("%s request used virtual-host style host %q", req.method, req.host)
		}
	}
}

// TestNewS3CreateBucketExisting checks that an existing bucket is reused: two
// constructions succeed and neither issues a bucket create.
func TestNewS3CreateBucketExisting(t *testing.T) {
	rec, endpoint := newRecordingFakeS3(t)
	if err := newTestAdmin(t, endpoint, "us-east-1").MakeBucket(t.Context(), testBucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	rec.reset()

	opts := newS3TestOptions(endpoint)
	opts.CreateBucket = true
	for range 2 {
		s, err := objstore.NewS3(opts)
		if err != nil {
			t.Fatalf("NewS3 with existing bucket: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	for _, req := range rec.seen() {
		if req.method == http.MethodPut {
			t.Errorf("existing bucket was recreated: %s %s", req.method, req.path)
		}
	}
}

// TestNewS3CreateBucketRegion checks that the effective configured region is
// sent as the bucket location.
func TestNewS3CreateBucketRegion(t *testing.T) {
	var (
		mu        sync.Mutex
		locations []string
	)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			var cfg struct {
				Location string `xml:"LocationConstraint"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&cfg); err != nil {
				t.Errorf("decode create-bucket body: %v", err)
			}
			mu.Lock()
			locations = append(locations, cfg.Location)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	opts := newS3TestOptions(strings.TrimPrefix(srv.URL, "http://"))
	opts.Region = "garage"
	opts.CreateBucket = true
	s, err := objstore.NewS3(opts)
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(locations, []string{"garage"}) {
		t.Fatalf("bucket location = %v, want [garage]", locations)
	}
}

// TestNewS3CreateBucketErrors checks that bucket permission failures surface
// with enough context to tell check from create.
func TestNewS3CreateBucketErrors(t *testing.T) {
	const accessDenied = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>access denied</Message></Error>`
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{
			name: "bucket check denied",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusForbidden)
				io.WriteString(w, accessDenied)
			},
			want: `check bucket "parkana-test"`,
		},
		{
			name: "bucket create denied",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusForbidden)
				io.WriteString(w, accessDenied)
			},
			want: `create bucket "parkana-test"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			t.Cleanup(srv.Close)
			opts := newS3TestOptions(strings.TrimPrefix(srv.URL, "http://"))
			opts.CreateBucket = true
			s, err := objstore.NewS3(opts)
			if s != nil {
				s.Close()
				t.Fatal("NewS3 returned a store together with an error")
			}
			if err == nil {
				t.Fatal("NewS3 accepted a denied bucket operation")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewS3 error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestS3Conformance(t *testing.T) {
	objstoretest.Run(t, newFakeS3(t))
}

func TestS3GetRangeContract(t *testing.T) {
	s := newFakeS3(t)
	ctx := t.Context()
	const key = "range/object"
	const body = "0123456789"
	if err := s.Put(ctx, key, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		off    int64
		length int64
		want   string
	}{
		{"full", 0, -1, body},
		{"prefix", 0, 4, "0123"},
		{"tail", 6, -1, "6789"},
		{"clipped", 8, 10, "89"},
		{"exactly at EOF", 10, -1, ""},
		{"zero length", 4, 0, ""},
		{"zero length at EOF", 10, 0, ""},
		{"offset past EOF", 11, -1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := s.GetRange(ctx, key, tc.off, tc.length)
			if err != nil {
				t.Fatalf("GetRange(%d, %d): %v", tc.off, tc.length, err)
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("GetRange(%d, %d) = %q, want %q", tc.off, tc.length, got, tc.want)
			}
		})
	}

	t.Run("negative offset", func(t *testing.T) {
		if _, err := s.GetRange(ctx, key, -1, 4); err == nil {
			t.Fatal("GetRange accepted a negative offset")
		}
	})

	t.Run("missing object", func(t *testing.T) {
		for _, tc := range []struct{ off, length int64 }{{0, 4}, {0, 0}, {0, -1}, {10, -1}, {5, 1}} {
			if _, err := s.GetRange(ctx, "range/missing", tc.off, tc.length); !errors.Is(err, objstore.ErrNotExist) {
				t.Errorf("GetRange(%d, %d) missing: %v, want ErrNotExist", tc.off, tc.length, err)
			}
		}
	})
}

func TestS3UnknownSizeMultipart(t *testing.T) {
	s := newFakeS3(t)
	ctx := t.Context()
	body := bytes.Repeat([]byte("0123456789abcdef"), (17<<20)/16+1)
	const key = "multipart/unknown"
	if err := s.Put(ctx, key, bytes.NewReader(body), -1); err != nil {
		t.Fatal(err)
	}
	info, err := s.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(body)) || info.Key != key {
		t.Fatalf("Stat = %+v, want size %d", info, len(body))
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(body))
	}
}

func TestS3EmptyObject(t *testing.T) {
	s := newFakeS3(t)
	ctx := t.Context()
	for _, tc := range []struct {
		name string
		size int64
	}{{"known size", 0}, {"unknown size", -1}} {
		t.Run(tc.name, func(t *testing.T) {
			key := "empty/" + tc.name
			if err := s.Put(ctx, key, strings.NewReader(""), tc.size); err != nil {
				t.Fatalf("Put: %v", err)
			}
			info, err := s.Stat(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size != 0 {
				t.Fatalf("Stat size = %d, want 0", info.Size)
			}
			rc, err := s.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			rc.Close()
			rc, err = s.GetRange(ctx, key, 0, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			if got, err := io.ReadAll(rc); err != nil || len(got) != 0 {
				t.Fatalf("GetRange = %q, %v, want empty", got, err)
			}
		})
	}
}

// chunkReader yields 64 KiB chunks forever and cancels the upload once it has
// produced limit bytes, producing small chunks the way a live source would.
type chunkReader struct {
	cancel context.CancelFunc
	limit  int64
	n      atomic.Int64
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.n.Load() >= r.limit {
		r.cancel()
	}
	n := min(len(p), 64<<10)
	r.n.Add(int64(n))
	return n, nil
}

// TestS3PutCancellationDuringSourceRead covers minio-go's multipart part-fill
// loop, which has no context check of its own: without ctxReader around the
// source it would drain the reader until a whole 16 MiB part was full.
func TestS3PutCancellationDuringSourceRead(t *testing.T) {
	s := newFakeS3(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := &chunkReader{cancel: cancel, limit: 1 << 20}

	err := s.Put(ctx, "cancel/during-put", src, -1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put = %v, want context.Canceled", err)
	}
	if got := src.n.Load(); got == 0 {
		t.Fatal("source was never read")
	} else if got > 2<<20 {
		t.Fatalf("Put drained %d source bytes after cancellation, want at most %d", got, int64(2<<20))
	}
}

func TestS3ListPrefixesAndEarlyStop(t *testing.T) {
	s := newFakeS3(t)
	ctx := t.Context()
	for _, k := range []string{"a/x/1", "a/x/2", "a/xy/3", "b/4"} {
		if err := s.Put(ctx, k, strings.NewReader(k), int64(len(k))); err != nil {
			t.Fatal(err)
		}
	}

	list := func(prefix string) []string {
		t.Helper()
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
		{"", []string{"a/x/1", "a/x/2", "a/xy/3", "b/4"}},
		{"a/x/", []string{"a/x/1", "a/x/2"}},
		{"a/x", []string{"a/x/1", "a/x/2", "a/xy/3"}},
		{"nothing/", nil},
	} {
		if got := list(tc.prefix); !slices.Equal(got, tc.want) {
			t.Errorf("List(%q) = %v, want %v", tc.prefix, got, tc.want)
		}
	}

	// Breaking early must return promptly and leave no stuck lister.
	seen := 0
	for range s.List(ctx, "") {
		seen++
		break
	}
	if seen != 1 {
		t.Fatalf("early stop saw %d objects, want 1", seen)
	}
}

func TestS3Cancellation(t *testing.T) {
	s := newFakeS3(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := s.Put(ctx, "c/put", strings.NewReader("x"), 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Put = %v, want context.Canceled", err)
	}
	if _, err := s.Get(ctx, "c/get"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get = %v, want context.Canceled", err)
	}
	if _, err := s.GetRange(ctx, "c/range", 0, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("GetRange = %v, want context.Canceled", err)
	}
	if _, err := s.Stat(ctx, "c/stat"); !errors.Is(err, context.Canceled) {
		t.Errorf("Stat = %v, want context.Canceled", err)
	}
	if err := s.Delete(ctx, "c/delete"); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete = %v, want context.Canceled", err)
	}
	var gotErr error
	for _, err := range s.List(ctx, "") {
		gotErr = err
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("List = %v, want context.Canceled", gotErr)
	}
}

func TestS3DeleteMissingTwice(t *testing.T) {
	s := newFakeS3(t)
	for range 2 {
		if err := s.Delete(t.Context(), "never/created"); err != nil {
			t.Fatalf("Delete missing: %v", err)
		}
	}
}

func TestS3InvalidKeys(t *testing.T) {
	s := newFakeS3(t)
	ctx := t.Context()
	for _, key := range []string{"", "/abs", "a/../b", "a//b", "a/./b", `a\b`} {
		if _, err := s.Get(ctx, key); err == nil {
			t.Errorf("Get(%q) accepted", key)
		}
		if _, err := s.GetRange(ctx, key, 0, 1); err == nil {
			t.Errorf("GetRange(%q) accepted", key)
		}
		if _, err := s.Stat(ctx, key); err == nil {
			t.Errorf("Stat(%q) accepted", key)
		}
		if err := s.Delete(ctx, key); err == nil {
			t.Errorf("Delete(%q) accepted", key)
		}
	}
}

func TestNewS3Config(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts objstore.S3Options
	}{
		{"no endpoint", objstore.S3Options{Bucket: "b"}},
		{"no bucket", objstore.S3Options{Endpoint: "127.0.0.1:9000"}},
	} {
		if _, err := objstore.NewS3(tc.opts); err == nil {
			t.Errorf("%s: NewS3 accepted invalid options", tc.name)
		}
	}

	// Construction must not touch the network: an unreachable endpoint is a
	// valid configuration until the first operation.
	s, err := objstore.NewS3(objstore.S3Options{
		Endpoint:  "127.0.0.1:1",
		Bucket:    "b",
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
		Insecure:  true,
	})
	if err != nil {
		t.Fatalf("NewS3 with unreachable endpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestS3Close(t *testing.T) {
	s := newFakeS3(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := s.Put(t.Context(), "after/close", strings.NewReader("x"), 1); err == nil {
		t.Fatal("Put after Close accepted")
	}
	var gotErr error
	for _, err := range s.List(t.Context(), "") {
		gotErr = err
	}
	if gotErr == nil {
		t.Fatal("List after Close succeeded")
	}
}
