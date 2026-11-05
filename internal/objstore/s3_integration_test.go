package objstore_test

import (
	"cmp"
	"context"
	"io"
	"iter"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/objstore/objstoretest"
)

// TestS3Integration runs the Store conformance suite against a real
// S3-compatible endpoint. It is skipped unless PARKANA_TEST_S3_ENDPOINT,
// PARKANA_TEST_S3_BUCKET, PARKANA_TEST_S3_ACCESS_KEY and
// PARKANA_TEST_S3_SECRET_KEY are set. PARKANA_TEST_S3_REGION defaults to
// us-east-1 and PARKANA_TEST_S3_INSECURE=true uses plain HTTP.
//
// Every object lives under a unique prefix in the configured bucket; cleanup
// removes only that prefix, never another object. The bucket must already
// exist: the store never creates buckets.
func TestS3Integration(t *testing.T) {
	endpoint := os.Getenv("PARKANA_TEST_S3_ENDPOINT")
	bucket := os.Getenv("PARKANA_TEST_S3_BUCKET")
	accessKey := os.Getenv("PARKANA_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("PARKANA_TEST_S3_SECRET_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set PARKANA_TEST_S3_ENDPOINT, PARKANA_TEST_S3_BUCKET, PARKANA_TEST_S3_ACCESS_KEY and PARKANA_TEST_S3_SECRET_KEY to run")
	}
	insecure, err := strconv.ParseBool(os.Getenv("PARKANA_TEST_S3_INSECURE"))
	if err != nil && os.Getenv("PARKANA_TEST_S3_INSECURE") != "" {
		t.Fatalf("PARKANA_TEST_S3_INSECURE: %v", err)
	}
	region := cmp.Or(os.Getenv("PARKANA_TEST_S3_REGION"), "us-east-1")

	s, err := objstore.NewS3(objstore.S3Options{
		Endpoint:  endpoint,
		Bucket:    bucket,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Insecure:  insecure,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	prefix := "objstoretest/" + uuid.NewV7().String() + "/"
	t.Cleanup(func() { deletePrefix(t, s, prefix) })

	objstoretest.Run(t, prefixedStore{Store: s, prefix: prefix})
}

// deletePrefix removes every object under prefix, the only keys the
// integration test owns. t.Context is already canceled in cleanup.
func deletePrefix(t *testing.T, s *objstore.S3, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
	defer cancel()
	for info, err := range s.List(ctx, prefix) {
		if err != nil {
			t.Errorf("cleanup list %s: %v", prefix, err)
			return
		}
		if err := s.Delete(ctx, info.Key); err != nil {
			t.Errorf("cleanup delete %s: %v", info.Key, err)
		}
	}
}

// prefixedStore isolates a real bucket by mapping every key under prefix.
type prefixedStore struct {
	objstore.Store
	prefix string
}

var _ objstore.Store = prefixedStore{}

func (p prefixedStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return p.Store.Put(ctx, p.prefix+key, r, size)
}

func (p prefixedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return p.Store.Get(ctx, p.prefix+key)
}

func (p prefixedStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return p.Store.GetRange(ctx, p.prefix+key, offset, length)
}

func (p prefixedStore) Stat(ctx context.Context, key string) (objstore.Info, error) {
	info, err := p.Store.Stat(ctx, p.prefix+key)
	info.Key = key
	return info, err
}

func (p prefixedStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return func(yield func(objstore.Info, error) bool) {
		for info, err := range p.Store.List(ctx, p.prefix+prefix) {
			if err != nil {
				yield(info, err)
				return
			}
			info.Key = strings.TrimPrefix(info.Key, p.prefix)
			if !yield(info, nil) {
				return
			}
		}
	}
}

func (p prefixedStore) Delete(ctx context.Context, key string) error {
	return p.Store.Delete(ctx, p.prefix+key)
}
