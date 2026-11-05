package ingest

// Wiring tests for -ingest.upload-concurrency: they prove the service's
// default uploader factory hands the configured worker count to NewUploader,
// that a programmatic zero still selects the documented default, and that
// out-of-range values are rejected at construction instead of surfacing
// inside Run.

import (
	"strings"
	"testing"
)

// TestServiceUploadConcurrencyPropagation builds the service with each
// configured worker count and checks the uploader the default factory
// produces. The factory runs inside opts.defaults during newService, so the
// test exercises the real wiring without opening durable resources.
func TestServiceUploadConcurrencyPropagation(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "serial", in: 1, want: 1},
		{name: "configured", in: 4, want: 4},
		{name: "max", in: uploadMaxConcurrency, want: uploadMaxConcurrency},
		{name: "zero selects default", in: 0, want: uploadDefaultConcurrency},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps, _ := testDeps(t, t.TempDir())
			deps.Config.Ingest.UploadConcurrency = tc.in
			s := newTestService(t, deps, serviceOptions{})
			ls, _ := testStore(t, nil)
			ua, err := s.opts.newUploader(ls, s.objects, s.catalog)
			if err != nil {
				t.Fatal(err)
			}
			u, ok := ua.(*Uploader)
			if !ok {
				t.Fatalf("default newUploader built %T, want *Uploader", ua)
			}
			if u.concurrency != tc.want {
				t.Fatalf("uploader concurrency = %d, want %d", u.concurrency, tc.want)
			}
		})
	}
}

// TestServiceUploadConcurrencyValidation pins that explicit out-of-range
// worker counts are rejected by newService's config recheck, before any
// durable resource opens.
func TestServiceUploadConcurrencyValidation(t *testing.T) {
	for _, n := range []int{-1, uploadMaxConcurrency + 1} {
		deps, _ := testDeps(t, t.TempDir())
		deps.Config.Ingest.UploadConcurrency = n
		_, err := newService(deps, serviceOptions{})
		if err == nil || !strings.Contains(err.Error(), "upload-concurrency") {
			t.Errorf("UploadConcurrency %d: err = %v, want upload-concurrency rejection", n, err)
		}
	}
}
