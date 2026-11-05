package ingest

// Wiring tests for -ingest.flush-concurrency: they prove the service's
// default flusher factory hands the configured worker count to NewFlusher,
// that a programmatic zero still selects the documented default, and that
// out-of-range values are rejected at construction instead of surfacing
// inside Run.

import (
	"strings"
	"testing"
)

// TestServiceFlushConcurrencyPropagation builds the service with each
// configured worker count and checks the flusher the default factory
// produces. The factory runs inside opts.defaults during newService, so the
// test exercises the real wiring without opening durable resources.
func TestServiceFlushConcurrencyPropagation(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "serial", in: 1, want: 1},
		{name: "configured", in: 4, want: 4},
		{name: "max", in: maxFlushConcurrency, want: maxFlushConcurrency},
		{name: "zero selects default", in: 0, want: defaultFlushConcurrency},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps, _ := testDeps(t, t.TempDir())
			deps.Config.Ingest.FlushConcurrency = tc.in
			s := newTestService(t, deps, serviceOptions{})
			ls, _ := testStore(t, nil)
			gf, err := s.opts.newFlusher(ls)
			if err != nil {
				t.Fatal(err)
			}
			f, ok := gf.(*Flusher)
			if !ok {
				t.Fatalf("default newFlusher built %T, want *Flusher", gf)
			}
			if f.concurrency != tc.want {
				t.Fatalf("flusher concurrency = %d, want %d", f.concurrency, tc.want)
			}
		})
	}
}

// TestServiceFlushConcurrencyValidation pins that explicit out-of-range
// worker counts are rejected by newService's config recheck, before any
// durable resource opens.
func TestServiceFlushConcurrencyValidation(t *testing.T) {
	for _, n := range []int{-1, maxFlushConcurrency + 1} {
		deps, _ := testDeps(t, t.TempDir())
		deps.Config.Ingest.FlushConcurrency = n
		_, err := newService(deps, serviceOptions{})
		if err == nil || !strings.Contains(err.Error(), "flush-concurrency") {
			t.Errorf("FlushConcurrency %d: err = %v, want flush-concurrency rejection", n, err)
		}
	}
}
