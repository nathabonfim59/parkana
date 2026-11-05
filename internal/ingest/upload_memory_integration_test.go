package ingest

// Tests for the product wiring of upload memory admission: one estimate and
// acquire per job, admission before work, release on every outcome, queued
// cancellation, oversized/unknown handling, the structured admission log, and
// the registered gauges/counters.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadGatherValue reads one unlabeled counter or gauge from reg without
// calling testing.T, so merge callbacks can use it safely.
func uploadGatherValue(reg *prometheus.Registry, name string) (float64, bool, error) {
	fams, err := reg.Gather()
	if err != nil {
		return 0, false, err
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range f.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				sum += g.GetValue()
			}
		}
		return sum, true, nil
	}
	return 0, false, nil
}

// uploadTestEstimate returns a fixed estimate for deterministic admission.
func uploadTestEstimate(estimate int64) func(context.Context, *TenantStore, Upload) (uploadMemoryEstimate, error) {
	return func(context.Context, *TenantStore, Upload) (uploadMemoryEstimate, error) {
		return uploadMemoryEstimate{Bytes: estimate, Known: estimate > 0, Merge: estimate > 0}, nil
	}
}

// startupLog builds a text slog logger capturing into buf.
func startupLog(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// requireFullBudget proves no admission permits leaked: a full-budget acquire
// must succeed immediately.
func requireFullBudget(t *testing.T, u *Uploader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := u.memory.Acquire(ctx, u.memory.Budget().Bytes)
	if err != nil {
		t.Fatalf("full-budget acquire leaked permits: %v", err)
	}
	release()
}

// TestUploadAdmissionBeforeWorkAndLog runs one real job with a fixed estimate
// and checks the gauge/active state inside the merge plus the bounded log.
func TestUploadAdmissionBeforeWorkAndLog(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 18, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "admitted"}})

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	const estimate int64 = 4 << 20
	u := f.uploader(UploaderOptions{
		UploadMemory: estimate,
		metrics:      m,
		log:          startupLog(&logBuf),
	})
	u.estimate = uploadTestEstimate(estimate)
	observed := make(chan [2]float64, 1)
	observeErr := make(chan error, 1)
	u.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		inflight, _, err := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes")
		if err != nil {
			observeErr <- err
			return storage.FileStats{}, err
		}
		active, _, err := uploadGatherValue(reg, "parkana_upload_active_jobs")
		if err != nil {
			observeErr <- err
			return storage.FileStats{}, err
		}
		observed <- [2]float64{inflight, active}
		return storage.Merge(ctx, w, inputs, promoted)
	}
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	select {
	case err := <-observeErr:
		t.Fatalf("gather in merge: %v", err)
	default:
	}
	if report.Uploads != 1 {
		t.Fatalf("uploads = %d, want 1", report.Uploads)
	}
	got := <-observed
	if got[0] != float64(estimate) {
		t.Fatalf("in-flight estimate during merge = %v, want %d", got[0], estimate)
	}
	if got[1] != 1 {
		t.Fatalf("active jobs during merge = %v, want 1", got[1])
	}
	if v, ok, err := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); err != nil || !ok || v != 0 {
		t.Fatalf("in-flight estimate after pass = %v (found=%v, err=%v), want 0", v, ok, err)
	}
	if v, ok, err := uploadGatherValue(reg, "parkana_upload_active_jobs"); err != nil || !ok || v != 0 {
		t.Fatalf("active jobs after pass = %v (found=%v, err=%v), want 0", v, ok, err)
	}
	requireFullBudget(t, u)

	text := logBuf.String()
	for _, want := range []string{
		"msg=\"upload receipt admitted\"",
		"sourceCount=1",
		"estimateBytes=4194304",
		"weightBytes=4194304",
		"budgetBytes=4194304",
		"estimateKnown=true",
		"mergeRequired=true",
		"sourceBytes=",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("admission log missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "line=") || strings.Contains(text, "labels=") {
		t.Fatalf("admission log leaked payload fields:\n%s", text)
	}
}

// TestUploadAdmissionReleasesOnErrorPaths covers merge, data Put, sidecar Put
// and catalog failures: the sentinel survives and every permit returns.
func TestUploadAdmissionReleasesOnErrorPaths(t *testing.T) {
	sentinel := errors.New("injected admission-path failure")
	tests := []struct {
		name string
		opts func(*uploadFixture) UploaderOptions
	}{
		{
			name: "merge",
			opts: func(*uploadFixture) UploaderOptions {
				return UploaderOptions{}
			},
		},
		{
			name: "data put",
			opts: func(f *uploadFixture) UploaderOptions {
				return UploaderOptions{Objects: &uploadFailKindPut{Store: f.objects, err: sentinel}}
			},
		},
		{
			name: "sidecar put",
			opts: func(f *uploadFixture) UploaderOptions {
				return UploaderOptions{Objects: &uploadFailKindPut{Store: f.objects, failSidecar: true, err: sentinel}}
			},
		},
		{
			name: "catalog",
			opts: func(f *uploadFixture) UploaderOptions {
				return UploaderOptions{Catalog: &uploadFailingCatalog{Catalog: f.cat, err: sentinel}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := uploadNewFixture(t)
			hour := time.Date(2026, time.November, 6, 19, 0, 0, 0, time.UTC)
			f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "fail " + tt.name}})
			const estimate int64 = 4 << 20
			reg := prometheus.NewRegistry()
			m, err := newMetrics(reg)
			if err != nil {
				t.Fatal(err)
			}
			opts := tt.opts(f)
			opts.UploadMemory = estimate
			opts.metrics = m
			u := f.uploader(opts)
			u.estimate = uploadTestEstimate(estimate)
			if tt.name == "merge" {
				u.merge = func(context.Context, io.Writer, []*storage.Reader, []string) (storage.FileStats, error) {
					return storage.FileStats{}, sentinel
				}
			}
			_, err = u.RunOnce(t.Context())
			if !errors.Is(err, sentinel) {
				t.Fatalf("RunOnce error = %v, want the sentinel preserved", err)
			}
			if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || v != 0 {
				t.Fatalf("in-flight estimate after %s error = %v (found=%v, err=%v), want 0", tt.name, v, ok, gerr)
			}
			requireFullBudget(t, u)
		})
	}
}

// TestUploadAdmissionCancelQueuedLeavesReceipt proves a pass canceled while a
// job waits for admission leaves the receipt durable and a later pass completes
// it.
func TestUploadAdmissionCancelQueuedLeavesReceipt(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 20, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "queued"}})
	const estimate int64 = 4 << 20
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	u := f.uploader(UploaderOptions{UploadMemory: estimate, metrics: m})
	estimateStarted := make(chan struct{})
	u.estimate = func(context.Context, *TenantStore, Upload) (uploadMemoryEstimate, error) {
		close(estimateStarted)
		return uploadMemoryEstimate{Bytes: estimate, Known: true, Merge: true}, nil
	}
	ts := f.tenantStore(tenant.Default)
	pending, err := ts.PendingFiles(t.Context())
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending files = %+v, %v", pending, err)
	}
	up, err := ts.ReserveUpload(t.Context(), Upload{
		Stream: "api", Hour: hour, Sources: []uuid.UUID{pending[0].ID},
	})
	if err != nil {
		t.Fatalf("ReserveUpload: %v", err)
	}
	hold, err := u.memory.Acquire(t.Context(), u.memory.Budget().Bytes)
	if err != nil {
		t.Fatalf("hold budget: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	outcome := make(chan jobOutcome, 1)
	go func() {
		outcome <- u.processJob(ctx, uploadJob{ts: ts, up: up, resumed: true})
	}()
	select {
	case <-estimateStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("queued job never started estimating")
	}
	// The budget is still held, so the job must be waiting in Acquire and
	// must not have produced an outcome yet.
	select {
	case got := <-outcome:
		t.Fatalf("queued job produced an outcome while the budget was held: %+v", got)
	default:
	}
	cancel()
	select {
	case got := <-outcome:
		if !got.canceled || got.err != nil || got.done {
			t.Fatalf("canceled queued outcome = %+v, want untouched cancellation", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued job did not return after cancellation")
	}
	hold()
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || v != 0 {
		t.Fatalf("in-flight estimate after canceled waiter = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
	if got := f.onlyReceipt(tenant.Default); got.ID != up.ID || got.Stage != UploadReserved {
		t.Fatalf("receipt after canceled waiter = %+v, want reserved %s", got, up.ID)
	}
	if _, err := f.uploader(UploaderOptions{UploadMemory: 8 << 20}).RunOnce(t.Context()); err != nil {
		t.Fatalf("resume RunOnce: %v", err)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after resume = %+v, want none", got)
	}
}

// TestUploadAdmissionOversizedRunsAlone proves an oversized estimate clamps to
// the full budget, runs, warns and increments the counter once, and never
// becomes a permanent error.
func TestUploadAdmissionOversizedRunsAlone(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 21, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "oversized"}})
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	const budget int64 = 4 << 20
	u := f.uploader(UploaderOptions{UploadMemory: budget, metrics: m, log: startupLog(&logBuf)})
	u.estimate = uploadTestEstimate(budget + 1)
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce oversized: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("uploads = %d, want 1", report.Uploads)
	}
	if got, ok, gerr := uploadGatherValue(reg, "parkana_upload_oversized_jobs_total"); gerr != nil || !ok || got != 1 {
		t.Fatalf("oversized counter = %v (found=%v, err=%v), want 1", got, ok, gerr)
	}
	if !strings.Contains(logBuf.String(), "oversized estimate runs alone") {
		t.Fatalf("missing oversized warning:\n%s", logBuf.String())
	}
	requireFullBudget(t, u)
}

// TestUploadAdmissionUnknownChargesFullBudget proves an unknown estimate is
// admitted with the full budget and still completes.
func TestUploadAdmissionUnknownChargesFullBudget(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 22, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "unknown"}})
	var logBuf bytes.Buffer
	const budget int64 = 4 << 20
	u := f.uploader(UploaderOptions{UploadMemory: budget, log: startupLog(&logBuf)})
	u.estimate = uploadTestEstimate(0)
	if _, err := u.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce unknown: %v", err)
	}
	text := logBuf.String()
	if !strings.Contains(text, "estimateKnown=false") || !strings.Contains(text, "weightBytes=4194304") {
		t.Fatalf("unknown admission log = %s", text)
	}
	requireFullBudget(t, u)
}

// TestUploadAdmissionSerializesAtBudget proves two jobs whose estimates exceed
// the budget together never overlap: the first merge is held while the test
// asserts the second has not entered merge, and both gauges return to zero.
func TestUploadAdmissionSerializesAtBudget(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 23, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "one"}})
	f.seed(tenant.Default, "web", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "two"}})
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	const (
		budget   int64 = 4 << 20
		estimate int64 = 3 << 20
	)
	u := f.uploader(UploaderOptions{UploadMemory: budget, Concurrency: 2, metrics: m})
	estimates := make(chan struct{}, 2)
	u.estimate = func(context.Context, *TenantStore, Upload) (uploadMemoryEstimate, error) {
		estimates <- struct{}{}
		return uploadMemoryEstimate{Bytes: estimate, Known: true, Merge: true}, nil
	}

	var merges, maxActive, maxInFlight atomic.Int64
	firstEntered := make(chan struct{})
	secondMergeEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	unblockFirst := sync.OnceFunc(func() { close(releaseFirst) })
	observeErr := make(chan error, 1)
	u.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		switch n := merges.Add(1); n {
		case 1:
			close(firstEntered)
			<-releaseFirst
		case 2:
			close(secondMergeEntered)
		}
		active, _, gerr := uploadGatherValue(reg, "parkana_upload_active_jobs")
		if gerr != nil {
			observeErr <- gerr
			return storage.FileStats{}, gerr
		}
		inflight, _, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes")
		if gerr != nil {
			observeErr <- gerr
			return storage.FileStats{}, gerr
		}
		if v := int64(active); v > maxActive.Load() {
			maxActive.Store(v)
		}
		if v := int64(inflight); v > maxInFlight.Load() {
			maxInFlight.Store(v)
		}
		return storage.Merge(ctx, w, inputs, promoted)
	}
	done := make(chan error, 1)
	runDone := make(chan struct{})
	go func() {
		_, err := u.RunOnce(t.Context())
		done <- err
		close(runDone)
	}()
	// Never strand the first merge (or its RunOnce goroutine) when an
	// assertion fails before the test releases it.
	defer func() {
		unblockFirst()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
		}
	}()
	select {
	case <-firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first merge never started")
	}
	// Both jobs must have reached their estimators; the second is then
	// blocked in Acquire because the first holds most of the budget.
	for range 2 {
		select {
		case <-estimates:
		case <-time.After(5 * time.Second):
			t.Fatal("second job never reached its estimator")
		}
	}
	// While the first merge is held, no second merge may start. Wait a bounded
	// window after both estimators were entered, so an unscheduled second
	// worker still has time to attempt (and be blocked by) admission.
	select {
	case <-secondMergeEntered:
		t.Fatal("admission bypassed: second merge started while the first was held")
	case <-time.After(30 * time.Millisecond):
	}
	if got := merges.Load(); got != 1 {
		t.Fatalf("merge calls while first held = %d, want 1 (admission bypassed)", got)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_active_jobs"); gerr != nil || !ok || v != 1 {
		t.Fatalf("active jobs while first merge held = %v (found=%v, err=%v), want 1", v, ok, gerr)
	}
	unblockFirst()
	if err := <-done; err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	select {
	case err := <-observeErr:
		t.Fatalf("gather in merge: %v", err)
	default:
	}
	if got := merges.Load(); got != 2 {
		t.Fatalf("merge calls = %d, want 2", got)
	}
	if got := maxActive.Load(); got > 1 {
		t.Fatalf("max active jobs = %d, want at most 1 when estimates exceed the budget together", got)
	}
	if got := maxInFlight.Load(); got > budget {
		t.Fatalf("max in-flight estimate = %d, want <= budget %d", got, budget)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_active_jobs"); gerr != nil || !ok || v != 0 {
		t.Fatalf("active jobs after pass = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || v != 0 {
		t.Fatalf("in-flight estimate after pass = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
}

// TestUploadAdmissionPublishedPartialCleanup runs the real published-cleanup
// stage after one retired source was already deleted: no source open, no Put,
// receipt completion and zeroed gauges.
func TestUploadAdmissionPublishedPartialCleanup(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	hour := time.Date(2026, time.November, 6, 10, 0, 0, 0, time.UTC)
	removed := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "one"}})
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "two"}})

	if _, err := f.uploader(UploaderOptions{
		Catalog: &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")},
	}).RunOnce(ctx); err == nil {
		t.Fatal("RunOnce with failing catalog succeeded")
	}
	receipt := f.onlyReceipt(tenant.Default)
	if err := f.cat.AddFiles(ctx, uploadCatalogFile(receipt)); err != nil {
		t.Fatalf("seed catalog row: %v", err)
	}
	if err := f.tenantStore(tenant.Default).MarkPublished(ctx, receipt.ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := os.Remove(f.localPath(tenant.Default, removed)); err != nil {
		t.Fatalf("remove one retired source: %v", err)
	}
	f.reopen()

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	objects := &uploadCountingPut{Store: f.objects}
	u := f.uploader(UploaderOptions{
		UploadMemory: 16 << 20,
		Objects:      objects,
		metrics:      m,
		log:          startupLog(&logBuf),
	})
	report, err := u.RunOnce(ctx)
	if err != nil {
		t.Fatalf("cleanup RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("cleanup report = %+v, want resumed receipt %s", report, receipt.ID)
	}
	if objects.calls != 0 {
		t.Fatalf("Put calls during published cleanup = %d, want 0", objects.calls)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after cleanup = %+v, want none", got)
	}
	text := logBuf.String()
	if !strings.Contains(text, "mergeRequired=false") {
		t.Fatalf("published cleanup log missing mergeRequired=false:\n%s", text)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_active_jobs"); gerr != nil || !ok || v != 0 {
		t.Fatalf("active jobs after cleanup = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || v != 0 {
		t.Fatalf("in-flight estimate after cleanup = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
	requireFullBudget(t, u)
}

// TestUploadAdmissionCleanupRemoveErrorReleases injects one cleanup remove
// failure, proves the permit returns and the error is the real one, then
// retries to completion.
func TestUploadAdmissionCleanupRemoveErrorReleases(t *testing.T) {
	f := uploadNewFixture(t)
	ctx := t.Context()
	hour := time.Date(2026, time.November, 6, 11, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "cleanup"}})

	if _, err := f.uploader(UploaderOptions{
		Catalog: &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")},
	}).RunOnce(ctx); err == nil {
		t.Fatal("RunOnce with failing catalog succeeded")
	}
	receipt := f.onlyReceipt(tenant.Default)
	if err := f.cat.AddFiles(ctx, uploadCatalogFile(receipt)); err != nil {
		t.Fatalf("seed catalog row: %v", err)
	}
	if err := f.tenantStore(tenant.Default).MarkPublished(ctx, receipt.ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	sentinel := errors.New("injected cleanup remove failure")
	origRemove := f.local.ops.remove
	var hits atomic.Int64
	f.local.ops.remove = func(root *os.Root, name string) error {
		if hits.Add(1) == 1 {
			return sentinel
		}
		return origRemove(root, name)
	}

	const budget int64 = 16 << 20
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	u := f.uploader(UploaderOptions{UploadMemory: budget, metrics: m})
	u.estimate = uploadTestEstimate(budget)
	if _, err := u.RunOnce(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("cleanup RunOnce error = %v, want %v", err, sentinel)
	}
	if hits.Load() < 1 {
		t.Fatal("cleanup remove failure was never hit")
	}
	if got := f.onlyReceipt(tenant.Default); got.Stage != UploadPublished {
		t.Fatalf("receipt stage after failed cleanup = %q, want published", got.Stage)
	}
	if v, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || v != 0 {
		t.Fatalf("in-flight estimate after cleanup error = %v (found=%v, err=%v), want 0", v, ok, gerr)
	}
	requireFullBudget(t, u)

	if _, err := f.uploader(UploaderOptions{UploadMemory: budget}).RunOnce(ctx); err != nil {
		t.Fatalf("retry cleanup RunOnce: %v", err)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after retry = %+v, want none", got)
	}
}

// TestUploadMemoryBudgetResolution checks explicit and injected host limits
// plus the one-time resolution log.
func TestUploadMemoryBudgetResolution(t *testing.T) {
	f := uploadNewFixture(t)
	cases := []struct {
		name       string
		explicit   int64
		limits     uploadMemoryLimits
		wantBytes  int64
		wantSource uploadMemorySource
	}{
		{
			name:       "explicit wins",
			explicit:   32 << 20,
			limits:     uploadMemoryLimits{Cgroup: func() (int64, bool) { return 2 << 30, true }},
			wantBytes:  32 << 20,
			wantSource: uploadMemorySourceExplicit,
		},
		{
			name:       "cgroup half",
			limits:     uploadMemoryLimits{Cgroup: func() (int64, bool) { return 2 << 30, true }},
			wantBytes:  1 << 30,
			wantSource: uploadMemorySourceCgroup,
		},
		{
			name:       "tie prefers cgroup",
			limits:     uploadMemoryLimits{Cgroup: func() (int64, bool) { return 2 << 30, true }, GoMemoryLimit: func() int64 { return 2 << 30 }},
			wantBytes:  1 << 30,
			wantSource: uploadMemorySourceCgroup,
		},
		{
			name:       "fallback",
			wantBytes:  uploadMemoryFallbackBudget,
			wantSource: uploadMemorySourceFallback,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			limits := tt.limits
			u, err := NewUploader(UploaderOptions{
				Local:        f.local,
				Objects:      f.objects,
				Catalog:      f.cat,
				PromotedFor:  f.promote,
				MinBytes:     1,
				MaxAge:       time.Hour,
				MergeMax:     1 << 20,
				UploadMemory: tt.explicit,
				memoryLimits: &limits,
				log:          startupLog(&logBuf),
			})
			if err != nil {
				t.Fatalf("NewUploader: %v", err)
			}
			if got := u.memory.Budget(); got.Bytes != tt.wantBytes || got.Source != tt.wantSource {
				t.Fatalf("budget = %+v, want %+v", got, uploadMemoryBudget{Bytes: tt.wantBytes, Source: tt.wantSource})
			}
			text := logBuf.String()
			if !strings.Contains(text, "msg=\"upload memory budget\"") ||
				!strings.Contains(text, tt.wantSource.String()) {
				t.Fatalf("resolution log = %q, want budget line with source %q", text, tt.wantSource.String())
			}
		})
	}
}

// TestServiceUploadMemoryPropagation asserts the service factory passes the
// explicit budget and logger through to the real uploader admission.
func TestServiceUploadMemoryPropagation(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
	deps.Config.Ingest.UploadMemory = 32 << 20
	var logBuf bytes.Buffer
	deps.Logger = startupLog(&logBuf)
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
	if got := u.memory.Budget(); got.Bytes != 32<<20 || got.Source != uploadMemorySourceExplicit {
		t.Fatalf("service uploader budget = %+v, want explicit %d", got, 32<<20)
	}
	if u.log != deps.Logger {
		t.Fatalf("service uploader logger = %p, want deps logger %p", u.log, deps.Logger)
	}
	if text := logBuf.String(); !strings.Contains(text, "bytes=33554432") || !strings.Contains(text, "source=explicit") {
		t.Fatalf("service resolution log = %q, want explicit budget line", text)
	}
}

// TestUploadAdmissionMetrics pins the two new collectors and their nil safety.
func TestUploadAdmissionMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	lease := uploadMemoryLease{Weight: 123}
	m.memoryAdmitted(lease)
	if got, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || got != 123 {
		t.Fatalf("in-flight estimate = %v (found=%v, err=%v), want 123", got, ok, gerr)
	}
	m.memoryReleased(lease)
	if got, ok, gerr := uploadGatherValue(reg, "parkana_upload_inflight_estimate_bytes"); gerr != nil || !ok || got != 0 {
		t.Fatalf("in-flight estimate after release = %v (found=%v, err=%v), want 0", got, ok, gerr)
	}
	m.oversizedJob()
	if got, ok, gerr := uploadGatherValue(reg, "parkana_upload_oversized_jobs_total"); gerr != nil || !ok || got != 1 {
		t.Fatalf("oversized counter = %v (found=%v, err=%v), want 1", got, ok, gerr)
	}
	var none *metrics
	none.memoryAdmitted(lease)
	none.memoryReleased(lease)
	none.oversizedJob()
}
