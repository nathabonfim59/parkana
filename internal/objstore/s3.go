package objstore

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// s3PartSize is the multipart part size used when an upload's size is unknown.
// minio-go buffers one part at a time, so this bounds upload memory.
const s3PartSize = 16 << 20

// s3CreateBucketTimeout bounds the bucket check and create done by NewS3 when
// S3Options.CreateBucket is set. NewS3 has no context, so without this bound a
// hung endpoint would block startup forever.
const s3CreateBucketTimeout = 30 * time.Second

// errS3Closed is returned by operations after Close.
var errS3Closed = errors.New("objstore: s3: store is closed")

// S3Options configures an S3 store. It mirrors config.S3 field for field, but
// lives here so objstore does not import config.
type S3Options struct {
	// Endpoint is the service host, optionally with a port; no scheme.
	Endpoint string
	// Bucket is the bucket that holds every object.
	Bucket string
	// Region is the signing region; empty means us-east-1. NewS3 also uses
	// it as the location when it creates Bucket.
	Region string
	// AccessKey and SecretKey are static credentials; both must be set for
	// signed requests.
	AccessKey string
	SecretKey string
	// Insecure uses plain HTTP, for a local MinIO.
	Insecure bool
	// CreateBucket makes NewS3 check that Bucket exists and create it when it
	// does not, using Region as the location. The default is false: NewS3 does
	// no network I/O and the bucket must already exist.
	CreateBucket bool
}

// S3 is a Store backed by an S3-compatible service (AWS S3, MinIO, Garage).
// NewS3 does not touch the network unless CreateBucket is set, in which case it
// verifies the bucket and may create it.
type S3 struct {
	client    *minio.Client
	bucket    string
	transport *http.Transport
	// disableContentSHA256 makes requests send an unsigned payload. minio-go
	// otherwise streams a per-chunk signature over plain HTTP, which some
	// S3-compatible servers (and the gofakes3 test server) do not decode for
	// multipart parts.
	disableContentSHA256 bool
	closed               atomic.Bool
}

var _ Store = (*S3)(nil)

// NewS3 returns a store for opts.Bucket. Construction performs no network I/O
// unless opts.CreateBucket is set, so callers get configuration errors
// immediately and connectivity errors from the first operation. With
// CreateBucket, construction checks that the bucket exists and creates it when
// missing, bounded by s3CreateBucketTimeout.
func NewS3(opts S3Options) (*S3, error) {
	if opts.Endpoint == "" {
		return nil, errors.New("objstore: s3: endpoint is required")
	}
	if opts.Bucket == "" {
		return nil, errors.New("objstore: s3: bucket is required")
	}
	region := cmp.Or(opts.Region, "us-east-1")
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	client, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure:    !opts.Insecure,
		Region:    region,
		Transport: transport,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("objstore: s3: %w", err)
	}
	s := &S3{
		client:               client,
		bucket:               opts.Bucket,
		transport:            transport,
		disableContentSHA256: opts.Insecure,
	}
	if opts.CreateBucket {
		ctx, cancel := context.WithTimeout(context.Background(), s3CreateBucketTimeout)
		defer cancel()
		if err := s.ensureBucket(ctx, region); err != nil {
			s.transport.CloseIdleConnections()
			return nil, err
		}
	}
	return s, nil
}

// ensureBucket verifies that the bucket exists and creates it when missing.
// ctx bounds the calls; callers supply it because NewS3 has no context.
func (s *S3) ensureBucket(ctx context.Context, region string) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("objstore: s3: check bucket %q: %w", s.bucket, err)
	}
	if exists {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		return fmt.Errorf("objstore: s3: create bucket %q: %w", s.bucket, err)
	}
	return nil
}

func (s *S3) validate(key string) error {
	if s.closed.Load() {
		return errS3Closed
	}
	return ValidateKey(key)
}

// wrapS3 adds operation context and maps a missing object to ErrNotExist.
func wrapS3(op, key string, err error) error {
	if isS3NotExist(err) {
		return fmt.Errorf("objstore: %s %s: %w", op, key, ErrNotExist)
	}
	return fmt.Errorf("objstore: %s %s: %w", op, key, err)
}

// isS3NotExist reports whether err is an S3 missing-object response.
func isS3NotExist(err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	resp, ok := errors.AsType[minio.ErrorResponse](err)
	if !ok {
		p, pok := errors.AsType[*minio.ErrorResponse](err)
		if !pok {
			return false
		}
		resp = *p
	}
	switch resp.Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return true
	case "NoSuchBucket":
		return false
	}
	return resp.StatusCode == http.StatusNotFound
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.validate(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// ctxReader checks cancellation between source reads: minio-go's
	// multipart part-fill loop has no context check of its own.
	r = ctxReader{ctx, r}
	opts := minio.PutObjectOptions{DisableContentSha256: s.disableContentSHA256}
	if size < 0 {
		// S3 needs a Content-Length, so an unknown length is uploaded in
		// parts. Peek first: S3 rejects zero-length parts, and an empty
		// stream has nothing to upload.
		br := bufio.NewReader(r)
		switch _, err := br.Peek(1); {
		case err == nil:
			// minio-go buffers one part at a time, so this bounds memory.
			opts.PartSize = s3PartSize
			r = br
		case errors.Is(err, io.EOF):
			size = 0
			r = bytes.NewReader(nil)
		default:
			return fmt.Errorf("objstore: put %s: %w", key, err)
		}
	}
	if _, err := s.client.PutObject(ctx, s.bucket, key, r, size, opts); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, err)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.open(ctx, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject defers the request until the first read; Stat forces it so a
	// missing object surfaces here, as FS does. The body stays readable.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		return nil, wrapS3("get", key, err)
	}
	return obj, nil
}

// open validates key and starts a lazy GetObject. Errors for a missing object
// are only returned once the request runs, so callers that need them eagerly
// stat first; a stat on the *Object would drop a preset Range header.
func (s *S3) open(ctx context.Context, key string, opts minio.GetObjectOptions) (*minio.Object, error) {
	if err := s.validate(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, wrapS3("get", key, err)
	}
	return obj, nil
}

func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := s.validate(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("objstore: get %s: negative offset %d", key, offset)
	}
	// Stat first for the object size: it keeps the empty-tail (offset >=
	// size) and clipping behavior identical to FS, and a missing object is
	// reported as ErrNotExist before the ranged read starts.
	info, err := s.Stat(ctx, key)
	if err != nil {
		return nil, err
	}
	if length == 0 || offset >= info.Size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if length < 0 || length > info.Size-offset {
		length = info.Size - offset
	}
	var opts minio.GetObjectOptions
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, fmt.Errorf("objstore: get %s: %w", key, err)
	}
	return s.open(ctx, key, opts)
}

func (s *S3) Stat(ctx context.Context, key string) (Info, error) {
	if err := s.validate(key); err != nil {
		return Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	oi, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return Info{}, wrapS3("stat", key, err)
	}
	return Info{Key: key, Size: oi.Size, ModTime: oi.LastModified}, nil
}

func (s *S3) List(ctx context.Context, prefix string) iter.Seq2[Info, error] {
	return func(yield func(Info, error) bool) {
		if s.closed.Load() {
			yield(Info{}, errS3Closed)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(Info{}, err)
			return
		}
		// ListObjects keeps a goroutine alive until the context ends, so
		// canceling on return also covers an early consumer break.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				yield(Info{}, fmt.Errorf("objstore: list %s: %w", prefix, obj.Err))
				return
			}
			if !yield(Info{Key: obj.Key, Size: obj.Size, ModTime: obj.LastModified}, nil) {
				return
			}
		}
		if err := ctx.Err(); err != nil {
			yield(Info{}, fmt.Errorf("objstore: list %s: %w", prefix, err))
		}
	}
}

func (s *S3) Delete(ctx context.Context, key string) error {
	if err := s.validate(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil && !isS3NotExist(err) {
		return fmt.Errorf("objstore: delete %s: %w", key, err)
	}
	return nil
}

// Close releases idle HTTP connections. It is safe to call more than once; a
// closed store rejects further operations.
func (s *S3) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.transport.CloseIdleConnections()
	return nil
}
