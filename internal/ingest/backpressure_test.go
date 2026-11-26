package ingest

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// bpStore is an object store whose Put fails while fail is set.
type bpStore struct {
	objstore.Store
	fail atomic.Bool
}

var errBPPut = errors.New("injected put failure")

func (s *bpStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.fail.Load() {
		return errBPPut
	}
	return s.Store.Put(ctx, key, r, size)
}

// bpPush builds push number i: n entries of size bytes each. The lines are
// seeded random hex so local Parquet does not compress them away.
func bpPush(i, n, size int) *logs.PushRequest {
	rng := rand.New(rand.NewPCG(uint64(i), 42))
	raw := make([]byte, size/2)
	st := logs.Stream{Labels: logs.New(
		logs.Label{Name: "job", Value: "api"},
		logs.Label{Name: "level", Value: "info"},
	)}
	base := time.Unix(1600000000, 0).UTC()
	for j := range n {
		for k := range raw {
			raw[k] = byte(rng.Uint32())
		}
		st.Entries = append(st.Entries, logs.Entry{
			Timestamp: base.Add(time.Duration(i*n+j) * time.Microsecond),
			Line:      hex.EncodeToString(raw),
		})
	}
	return &logs.PushRequest{Streams: []logs.Stream{st}}
}

// bpLocalOnDisk sums what the local bound counts from the tenant manifest:
// live (not retired) files plus the merge temps of uploads not yet published.
func bpLocalOnDisk(t *testing.T, ls *LocalStore, id tenant.ID) int64 {
	t.Helper()
	ts, err := ls.Tenant(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	files, err := ts.Files(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	uploads, err := ts.Uploads(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, f := range files {
		if !f.Retired {
			n += f.Bytes
		}
	}
	for _, u := range uploads {
		if u.Temp != "" && u.Stage != UploadPublished {
			n += u.Bytes
		}
	}
	return n
}

// bpMetric returns the where-labeled sample of a back-pressure metric and
// fails when it is not registered.
func bpMetric(t *testing.T, reg *prometheus.Registry, name, where string) float64 {
	t.Helper()
	v, ok := gatherLabeled(t, reg, name, "where", where)
	if !ok {
		t.Fatalf("metric %s not registered", name)
	}
	return v
}

// bpCheck429 asserts a back-pressure answer: 429, Retry-After in whole
// seconds and a plain-text body naming the bound and both byte counts.
func bpCheck429(t *testing.T, rec *httptest.ResponseRecorder, retryAfter, flag string, limit int64) {
	t.Helper()
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("got %d, want 429; body %q", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Retry-After"); got != retryAfter {
		t.Errorf("Retry-After = %q, want %q", got, retryAfter)
	}
	body := rec.Body.String()
	if !strings.Contains(body, flag) || !strings.Contains(body, "limit "+strconv.FormatInt(limit, 10)) {
		t.Errorf("body %q does not name %s and the limit %d", body, flag, limit)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want Loki-style plain text", ct)
	}
}

// bpDeps returns test deps with uploads eligible at once and a small local
// bound. The store's Put fails until the test heals it.
func bpDeps(t *testing.T, dataDir string, bound config.ByteSize) (app.Deps, *prometheus.Registry, *bpStore) {
	t.Helper()
	deps, reg := testDeps(t, dataDir)
	store := &bpStore{Store: deps.Store}
	deps.Store = store
	deps.Config.Ingest.UploadMinBytes = 1
	deps.Config.Ingest.UploadMergeMax = bound
	deps.Config.Ingest.MaxUnuploaded = bound
	return deps, reg, store
}

// TestBackpressureLocalBound: with every object store Put failing, local
// Parquet piles up; pushes answer 204 until the bytes not yet uploaded reach
// -ingest.max-unuploaded, then 429 with Retry-After and the metrics set. Once
// the store heals and an upload pass runs, pushes answer 204 again.
func TestBackpressureLocalBound(t *testing.T) {
	const bound = 32 * config.KiB
	deps, reg, store := bpDeps(t, t.TempDir(), bound)
	store.fail.Store(true)
	s := newTestService(t, deps, serviceOptions{})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	ctx := t.Context()

	var rejected *httptest.ResponseRecorder
	accepted := 0
	for i := range 100 {
		rec := pushHTTP(t, deps, bpPush(i, 20, 200))
		if rec.Code == http.StatusTooManyRequests {
			rejected = rec
			break
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("push %d: got %d, want 204 or 429; body %q", i, rec.Code, rec.Body)
		}
		accepted++
		if err := s.flushPass(ctx); err != nil {
			t.Fatalf("flush pass: %v", err)
		}
		if _, err := s.uploader.RunOnce(ctx); err == nil {
			t.Fatal("upload pass succeeded against a failing store")
		}
	}
	local := bpLocalOnDisk(t, s.local, tenant.Default)
	if rejected == nil {
		t.Fatalf("no 429 after %d pushes: %d local bytes wait for upload, bound %d", accepted, local, bound)
	}
	if accepted < 2 {
		t.Fatalf("only %d pushes accepted before the first 429; the bound tripped too early", accepted)
	}
	if local < int64(bound) {
		t.Fatalf("429 with %d local bytes waiting, below the bound %d", local, bound)
	}
	bpCheck429(t, rejected, "3600", "-ingest.max-unuploaded", int64(bound))
	if got := bpMetric(t, reg, "parkana_ingest_unuploaded_bytes", "local"); got != float64(local) {
		t.Errorf("unuploaded_bytes{where=local} = %v, want %d (manifest)", got, local)
	}
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_limit_bytes", "local"); got != float64(bound) {
		t.Errorf("backpressure_limit_bytes{where=local} = %v, want %d", got, bound)
	}
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_limit_bytes", "memory"); got != float64(4*testIngest().MemtableMaxBytes) {
		t.Errorf("backpressure_limit_bytes{where=memory} = %v, want %d", got, 4*testIngest().MemtableMaxBytes)
	}
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_rejections_total", "local"); got != 1 {
		t.Errorf("rejections{where=local} = %v, want 1", got)
	}
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_rejections_total", "memory"); got != 0 {
		t.Errorf("rejections{where=memory} = %v, want 0", got)
	}

	// The store heals: one upload pass publishes everything and the next push
	// is admitted. No hysteresis.
	store.fail.Store(false)
	if _, err := s.uploader.RunOnce(ctx); err != nil {
		t.Fatalf("upload pass after healing: %v", err)
	}
	if local := bpLocalOnDisk(t, s.local, tenant.Default); local != 0 {
		t.Fatalf("%d local bytes still wait after a healthy upload pass", local)
	}
	if got := bpMetric(t, reg, "parkana_ingest_unuploaded_bytes", "local"); got != 0 {
		t.Errorf("unuploaded_bytes{where=local} = %v after upload, want 0", got)
	}
	if rec := pushHTTP(t, deps, bpPush(1000, 20, 200)); rec.Code != http.StatusNoContent {
		t.Fatalf("push after healing: got %d, want 204; body %q", rec.Code, rec.Body)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestBackpressureMemoryBound: with every local publication failing, sealed
// generations pile up in memory; pushes answer 204 until the buffered bytes
// reach 4 x -ingest.memtable-max, then 429 on the Loki and the OTLP route.
// Once publication heals and a flush pass runs, pushes answer 204 again.
func TestBackpressureMemoryBound(t *testing.T) {
	const memMax = 16 * config.KiB
	deps, reg := testDeps(t, t.TempDir())
	deps.Config.Ingest.MemtableMaxBytes = memMax
	flusher := &fakeFlusher{}
	errFlush := errors.New("injected flush failure")
	flusher.setErr(func(SealedGeneration) error { return errFlush })
	s := newTestService(t, deps, serviceOptions{
		newFlusher: func(*LocalStore) (genFlusher, error) { return flusher, nil },
	})
	cancel, done := startRun(t, s)
	waitReady(t, s)
	ctx := t.Context()

	var rejected *httptest.ResponseRecorder
	accepted := 0
	for i := range 100 {
		rec := pushHTTP(t, deps, bpPush(i, 20, 200))
		if rec.Code == http.StatusTooManyRequests {
			rejected = rec
			break
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("push %d: got %d, want 204 or 429; body %q", i, rec.Code, rec.Body)
		}
		accepted++
		if err := s.flushPass(ctx); err != nil {
			t.Fatalf("flush pass: %v", err)
		}
	}
	if rejected == nil {
		t.Fatalf("no 429 after %d pushes with every flush failing (%d publish attempts)", accepted, len(flusher.flushed()))
	}
	// Each push is 20 entries of 200 bytes plus 96 bytes of overhead and 15
	// label bytes: 6220 bytes. The 11th push crosses 4 x 16 KiB.
	if accepted != 11 {
		t.Errorf("accepted %d pushes before the first 429, want 11", accepted)
	}
	limit := int64(4 * memMax)
	bpCheck429(t, rejected, "3600", "-ingest.memtable-max", limit)
	if got := bpMetric(t, reg, "parkana_ingest_unuploaded_bytes", "memory"); got != float64(11*6220) {
		t.Errorf("unuploaded_bytes{where=memory} = %v, want %d", got, 11*6220)
	}
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_rejections_total", "memory"); got != 1 {
		t.Errorf("rejections{where=memory} = %v, want 1", got)
	}

	// OTLP shares the append path and the answer.
	otlp := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), "")
	bpCheck429(t, otlp, "3600", "-ingest.memtable-max", limit)
	if got := bpMetric(t, reg, "parkana_ingest_backpressure_rejections_total", "memory"); got != 2 {
		t.Errorf("rejections{where=memory} = %v after the OTLP push, want 2", got)
	}

	flusher.setErr(nil)
	if err := s.flushPass(ctx); err != nil {
		t.Fatalf("flush pass after healing: %v", err)
	}
	if got := bpMetric(t, reg, "parkana_ingest_unuploaded_bytes", "memory"); got != 0 {
		t.Errorf("unuploaded_bytes{where=memory} = %v after a healthy flush, want 0", got)
	}
	if rec := pushHTTP(t, deps, bpPush(1000, 20, 200)); rec.Code != http.StatusNoContent {
		t.Fatalf("push after healing: got %d, want 204; body %q", rec.Code, rec.Body)
	}
	if rec := otlpHTTP(t, deps, otlpServiceBody(t, "hi"), ""); rec.Code != http.StatusNoContent {
		t.Fatalf("OTLP push after healing: got %d, want 204; body %q", rec.Code, rec.Body)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestBackpressureReplayIgnoresBounds: a WAL holding more than both bounds is
// replayed in full at startup (replay never checks them), and the local
// bound then rejects pushes until an upload pass drains the backlog.
func TestBackpressureReplayIgnoresBounds(t *testing.T) {
	const (
		memMax = 16 * config.KiB
		bound  = 8 * config.KiB
		pushes = 12 // 12 x 6220 bytes: more than 4 x memMax and far more than bound
	)
	dataDir := t.TempDir()

	// First run: every publication fails, so the WAL keeps every push.
	depsA, _ := testDeps(t, dataDir)
	flusher := &fakeFlusher{}
	flusher.setErr(func(SealedGeneration) error { return errors.New("injected flush failure") })
	a := newTestService(t, depsA, serviceOptions{
		newFlusher: func(*LocalStore) (genFlusher, error) { return flusher, nil },
	})
	cancelA, doneA := startRun(t, a)
	waitReady(t, a)
	for i := range pushes {
		if rec := pushHTTP(t, depsA, bpPush(i, 20, 200)); rec.Code != http.StatusNoContent {
			t.Fatalf("first run push %d: got %d; body %q", i, rec.Code, rec.Body)
		}
	}
	cancelA()
	if err := <-doneA; err == nil {
		t.Fatal("first run: Run = nil, want the failed final flush")
	}

	// Second run with small bounds and a healthy store: replay rebuilds and
	// publishes all of it.
	depsB, regB, _ := bpDeps(t, dataDir, bound)
	depsB.Config.Ingest.MemtableMaxBytes = memMax
	b := newTestService(t, depsB, serviceOptions{})
	cancelB, doneB := startRun(t, b)
	waitReady(t, b)
	if got, _ := gather(t, regB, "parkana_ingest_replay_records_total"); got != pushes {
		t.Fatalf("replayed %v records, want %d", got, pushes)
	}
	local := bpLocalOnDisk(t, b.local, tenant.Default)
	if local < int64(bound) {
		t.Fatalf("replay published %d local bytes, want more than the bound %d", local, bound)
	}
	rec := pushHTTP(t, depsB, bpPush(100, 20, 200))
	bpCheck429(t, rec, "3600", "-ingest.max-unuploaded", int64(bound))
	if got := bpMetric(t, regB, "parkana_ingest_unuploaded_bytes", "local"); got != float64(local) {
		t.Errorf("unuploaded_bytes{where=local} after recovery = %v, want %d (manifest)", got, local)
	}
	if _, err := b.uploader.RunOnce(t.Context()); err != nil {
		t.Fatalf("upload pass: %v", err)
	}
	if rec := pushHTTP(t, depsB, bpPush(101, 20, 200)); rec.Code != http.StatusNoContent {
		t.Fatalf("push after upload: got %d, want 204; body %q", rec.Code, rec.Body)
	}
	cancelB()
	if err := <-doneB; err != nil {
		t.Fatalf("second run: %v", err)
	}
}
