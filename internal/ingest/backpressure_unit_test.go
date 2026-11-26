package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// bpPushBytes is the admission estimate of bpPush(i, 20, 200): 20 entries of
// a 200-byte line, 96 bytes of overhead and 15 label bytes.
const bpPushBytes = 20 * (200 + admissionOverhead + 15)

// TestMemtableBuffered: Buffered counts every admitted row until the
// generation holding it is released, through sealing, TakeSealed, a double
// Release and Close.
func TestMemtableBuffered(t *testing.T) {
	ctx := t.Context()
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	m, err := NewMemtable(MemtableOptions{
		Allocator:    ca,
		StreamLabels: []string{"job"},
		PromotedFor:  func(string) []string { return []string{"level"} },
		// The third push seals the generation by size.
		MemtableMaxBytes: 3 * bpPushBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := tenant.Default
	seq := uint64(0)
	appendPush := func(i int) {
		t.Helper()
		seq++
		if err := m.Append(ctx, id, seq, 1, bpPush(i, 20, 200)); err != nil {
			t.Fatal(err)
		}
	}
	want := func(what string, n int64) {
		t.Helper()
		if got := m.Buffered(); got != n {
			t.Fatalf("%s: Buffered() = %d, want %d", what, got, n)
		}
	}
	want("empty", 0)
	appendPush(0)
	appendPush(1)
	want("two pushes", 2*bpPushBytes)
	appendPush(2) // auto-seals
	want("auto-sealed", 3*bpPushBytes)
	appendPush(3)
	if err := m.Seal(ctx, id); err != nil {
		t.Fatal(err)
	}
	want("sealed", 4*bpPushBytes)
	gens, err := m.TakeSealed(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 2 {
		t.Fatalf("TakeSealed = %d generations, want 2", len(gens))
	}
	want("taken, not released", 4*bpPushBytes)
	if _, err := gens[0].Parts(ctx); err != nil {
		t.Fatal(err)
	}
	gens[0].Release()
	gens[0].Release()
	want("first generation released twice", bpPushBytes)
	appendPush(4)
	want("new active push", 2*bpPushBytes)
	gens[1].Release()
	want("second generation released", bpPushBytes)
	appendPush(5)
	if err := m.Seal(ctx, id); err != nil {
		t.Fatal(err)
	}
	appendPush(6)
	want("one sealed, one active", 3*bpPushBytes)
	m.Close()
	want("closed", 0)
	ca.AssertSize(t, 0)
}

// bpSeedFile publishes one local file for id through the upload fixture.
func bpSeedFile(f *uploadFixture, id tenant.ID, stream string, hour time.Time, line string) LocalFile {
	return f.seed(id, stream, hour, []uploadRow{{ts: hour.Add(time.Minute), line: line}})
}

// TestLocalUnuploadedBytes: LocalStore.UnuploadedBytes follows the committed
// manifests of every tenant through publication, a merge whose Put failed
// (sources and merge temp both count), recovery, a completed upload and a
// recovery after it.
func TestLocalUnuploadedBytes(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	other := tenant.ID("other")
	sum := func() int64 {
		t.Helper()
		return bpLocalOnDisk(t, f.local, tenant.Default) + bpLocalOnDisk(t, f.local, other)
	}
	want := func(what string, n int64) {
		t.Helper()
		if got := f.local.UnuploadedBytes(); got != n {
			t.Fatalf("%s: UnuploadedBytes() = %d, want %d", what, got, n)
		}
	}
	want("empty", 0)
	a := bpSeedFile(f, tenant.Default, "api", hour, "a")
	b := bpSeedFile(f, tenant.Default, "api", hour, "b")
	c := bpSeedFile(f, other, "web", hour, "c")
	published := a.Bytes + b.Bytes + c.Bytes
	want("published", published)

	failing := f.uploader(UploaderOptions{Objects: &uploadFailBeforePut{Store: f.objects, err: errBPPut}})
	if _, err := failing.RunOnce(t.Context()); !errors.Is(err, errBPPut) {
		t.Fatalf("RunOnce = %v, want the Put failure", err)
	}
	var temps int64
	for _, id := range []tenant.ID{tenant.Default, other} {
		for _, u := range f.receipts(id) {
			if u.Stage != UploadMerged {
				t.Fatalf("receipt %s stage %q, want merged", u.ID, u.Stage)
			}
			temps += u.Bytes
		}
	}
	if temps == 0 {
		t.Fatal("no merge temp recorded")
	}
	want("merged, Put failed", published+temps)
	want("merged, matches the manifests", sum())

	f.reopen()
	want("recovered with merged receipts", published+temps)

	if _, err := f.uploader(UploaderOptions{}).RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	want("uploaded", 0)
	f.reopen()
	want("recovered after upload", 0)
	d := bpSeedFile(f, other, "web", hour, "d")
	want("published after recovery", d.Bytes)
}

// TestLocalUnuploadedBytesFailedRecovery: a tenant whose recovery fails is not
// cached, and its manifest's bytes do not stay in the store total.
func TestLocalUnuploadedBytesFailedRecovery(t *testing.T) {
	src := uploadNewFixture(t)
	hour := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	file := bpSeedFile(src, "broken", "api", hour, "lost")
	dst := uploadNewFixture(t)
	bpSeedFile(dst, tenant.Default, "api", hour, "kept")
	before := dst.local.UnuploadedBytes()
	if before == 0 {
		t.Fatal("no bytes counted for the healthy tenant")
	}
	// Copy the tenant into the open store, minus its live file: recovery
	// reads the manifest, then fails on the missing file.
	from := filepath.Join(src.dataDir, "parquet", "broken")
	to := filepath.Join(dst.dataDir, "parquet", "broken")
	if err := os.CopyFS(to, os.DirFS(from)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(to, filepath.FromSlash(file.RelPath()))); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.local.Tenant(t.Context(), "broken"); !errors.Is(err, ErrFileMissing) {
		t.Fatalf("Tenant(broken) = %v, want ErrFileMissing", err)
	}
	if got := dst.local.UnuploadedBytes(); got != before {
		t.Fatalf("UnuploadedBytes() = %d after a failed recovery, want %d", got, before)
	}
}

// TestAppendErrorStatus: both push routes map an AppendFunc's back-pressure
// error to 429 with Retry-After in whole seconds, rounded up, and keep 503 and
// 500 for the other failures.
func TestAppendErrorStatus(t *testing.T) {
	bp := &BackpressureError{Where: BackpressureLocal, Bytes: 9000, Limit: 8192, RetryAfter: 2500 * time.Millisecond}
	for _, tc := range []struct {
		name       string
		err        error
		code       int
		retryAfter string
		body       string
	}{
		{"back-pressure", bp, http.StatusTooManyRequests, "3", "9000 bytes of local Parquet wait for upload, limit 8192 (-ingest.max-unuploaded)"},
		{"wrapped back-pressure", errors.Join(errors.New("ctx"), bp), http.StatusTooManyRequests, "3", "limit 8192"},
		{"bare sentinel", ErrBackpressure, http.StatusTooManyRequests, "", "back-pressure"},
		{"sub-second retry", &BackpressureError{Where: BackpressureMemory, Bytes: 2, Limit: 1, RetryAfter: time.Millisecond}, http.StatusTooManyRequests, "1", "4 x -ingest.memtable-max"},
		{"unavailable", ErrUnavailable, http.StatusServiceUnavailable, "", "not accepting"},
		{"other", errors.New("disk on fire"), http.StatusInternalServerError, "", "failed to persist push"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := PushOptions{Append: func(context.Context, tenant.ID, *logs.PushRequest) error { return tc.err }}
			push, err := NewPushHandler(opts)
			if err != nil {
				t.Fatal(err)
			}
			otlp, err := NewOTLPHandler(opts)
			if err != nil {
				t.Fatal(err)
			}
			for route, h := range map[string]http.Handler{"loki": push, "otlp": otlp} {
				var r *http.Request
				if route == "loki" {
					r = httptest.NewRequest(http.MethodPost, "/loki/api/v1/push",
						strings.NewReader(`{"streams":[{"stream":{"job":"api"},"values":[["1600000000000000000","hi"]]}]}`))
					r.Header.Set("Content-Type", "application/json")
				} else {
					r = httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(string(otlpServiceBody(t, "hi"))))
					r.Header.Set("Content-Type", "application/x-protobuf")
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, r)
				if rec.Code != tc.code {
					t.Fatalf("%s: got %d, want %d; body %q", route, rec.Code, tc.code, rec.Body)
				}
				if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
					t.Errorf("%s: Retry-After = %q, want %q", route, got, tc.retryAfter)
				}
				if !strings.Contains(rec.Body.String(), tc.body) {
					t.Errorf("%s: body %q does not contain %q", route, rec.Body, tc.body)
				}
			}
		})
	}
}

// TestBackpressureLimits: the memory bound is 4 x -ingest.memtable-max and
// saturates, the local bound follows config.Ingest.MaxUnuploadedBytes, and
// Retry-After is the longer of the flush and upload intervals.
func TestBackpressureLimits(t *testing.T) {
	c := testIngest()
	c.FlushInterval = 5 * time.Second
	c.UploadInterval = 10 * time.Second
	mem, local, retry := backpressureLimits(c)
	if mem != 4*int64(c.MemtableMaxBytes) || local != 8*int64(c.UploadMergeMax) || retry != 10*time.Second {
		t.Errorf("limits = %d, %d, %s; want %d, %d, 10s", mem, local, retry, 4*int64(c.MemtableMaxBytes), 8*int64(c.UploadMergeMax))
	}
	c.MaxUnuploaded = 3 * c.UploadMergeMax
	c.MemtableMaxBytes = 1 << 62
	c.FlushInterval = time.Minute
	mem, local, retry = backpressureLimits(c)
	if mem != 1<<63-1 || local != 3*int64(c.UploadMergeMax) || retry != time.Minute {
		t.Errorf("limits = %d, %d, %s; want saturated, %d, 1m", mem, local, retry, 3*int64(c.UploadMergeMax))
	}
}

// TestValidateIngestMaxUnuploaded: a hand-built config is rechecked by the
// ingester itself.
func TestValidateIngestMaxUnuploaded(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    config.ByteSize
		ok   bool
	}{
		{"default", 0, true},
		{"equal to merge max", 2 * config.GiB, true},
		{"below merge max", config.GiB, false},
		{"negative", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testIngest()
			c.MaxUnuploaded = tc.v
			err := validateIngest(c)
			if (err == nil) != tc.ok {
				t.Fatalf("validateIngest(MaxUnuploaded=%d) = %v, want ok=%v", tc.v, err, tc.ok)
			}
			if err != nil && !strings.Contains(err.Error(), "-ingest.max-unuploaded") {
				t.Errorf("error %q does not name the flag", err)
			}
		})
	}
}
