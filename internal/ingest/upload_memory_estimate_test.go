package ingest

// Tests for the upload memory estimator: the calibration arithmetic,
// saturating helpers, stage-aware source/temp handling, bounded and closed
// footer preflight, and read-only behavior that never consumes a pending
// manifest-directory sync from the real processing path.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadMemoryCountedFile wraps a real file to count closes and bytes read.
type uploadMemoryCountedFile struct {
	*os.File
	closes    *atomic.Int64
	readBytes *atomic.Int64
}

func (f *uploadMemoryCountedFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.readBytes.Add(int64(n))
	return n, err
}

func (f *uploadMemoryCountedFile) Close() error {
	f.closes.Add(1)
	return f.File.Close()
}

// TestUploadMemoryEstimateCalibration pins the coefficients against the four
// buffered1024 merged-arm measurements. The source summaries use the
// measured row and uncompressed totals; expected estimates are exact byte
// values from that arithmetic.
func TestUploadMemoryEstimateCalibration(t *testing.T) {
	tests := []struct {
		fanIn        int
		uncompressed int64
		want         int64
		peak         int64
	}{
		{fanIn: 16, uncompressed: 595771501, want: 450211840, peak: 188353616},
		{fanIn: 32, uncompressed: 595713935, want: 554618880, peak: 279572400},
		{fanIn: 64, uncompressed: 595482655, want: 763432960, peak: 468886464},
		{fanIn: 128, uncompressed: 595189926, want: 1181061120, peak: 794691136},
	}
	const rows int64 = 3571307
	for _, tt := range tests {
		t.Run(fmt.Sprintf("fanin%d", tt.fanIn), func(t *testing.T) {
			worst := uploadMemoryCeilDiv(tt.uncompressed, rows)
			summary := uploadMemorySourceSummary{
				FanIn:             tt.fanIn,
				Rows:              rows,
				UncompressedBytes: tt.uncompressed,
				WorstRowBytes:     worst,
				DecodedBytes:      uploadMemoryMul(int64(tt.fanIn)*int64(storage.MergeReaderOptions().BatchSize), worst),
			}
			got := newUploadMemoryModel().estimateMerge(summary, index.DefaultParams)
			if !got.Known || !got.Merge {
				t.Fatalf("estimate = %+v, want known merge", got)
			}
			if got.Bytes != tt.want {
				t.Errorf("fan-in %d estimate = %d, want %d", tt.fanIn, got.Bytes, tt.want)
			}
			if got.Bytes <= tt.peak {
				t.Errorf("fan-in %d estimate %d does not exceed measured buffered1024 heap peak %d", tt.fanIn, got.Bytes, tt.peak)
			}
		})
	}
}

// TestUploadMemoryGramBytesGrowthTransient proves the gram bound includes the
// old table plus its doubled replacement when the (MaxGrams+1)th insertion
// grows before the cap resets the set.
func TestUploadMemoryGramBytesGrowthTransient(t *testing.T) {
	maxGrams := int64(2 << 20)
	got := uploadMemoryGramBytes(maxGrams, maxGrams)
	want := int64(96 << 20) // 32MiB old + 64MiB doubled replacement
	if got != want {
		t.Fatalf("gram bytes at default cap = %d, want %d (old+new transient)", got, want)
	}
	if got <= 32<<20 {
		t.Fatalf("gram bytes = %d, want the growth transient above one 32MiB table", got)
	}
	if small := uploadMemoryGramBytes(1024, maxGrams); small <= 0 || small >= got {
		t.Fatalf("small gram bound = %d, want positive and below the cap transient", small)
	}
}

// TestUploadMemoryWorstRowBytesPerGroup proves the worst average is max over
// each row group, not a ratio of separately-maximized totals.
func TestUploadMemoryWorstRowBytesPerGroup(t *testing.T) {
	groups := []uploadMemoryRowGroup{
		{Rows: 1, UncompressedBytes: 1000}, // average 1000
		{Rows: 10, UncompressedBytes: 100}, // average 10
	}
	if got, want := uploadMemoryWorstRowBytes(groups), int64(1000); got != want {
		t.Fatalf("worst row bytes = %d, want %d", got, want)
	}
	// The wrong max-bytes/max-rows construction would report 100 here.
	if got := uploadMemoryWorstRowBytes(groups); got == uploadMemoryCeilDiv(1000, 10) {
		t.Fatal("worst row bytes used separately-maximized row-group totals")
	}
	if got := uploadMemoryWorstRowBytes([]uploadMemoryRowGroup{{Rows: 0, UncompressedBytes: 100}, {Rows: 2, UncompressedBytes: 20}}); got != 10 {
		t.Fatalf("worst row bytes with an empty group = %d, want 10", got)
	}
	if got := uploadMemoryWorstRowBytes(nil); got != 0 {
		t.Fatalf("empty worst row bytes = %d, want 0", got)
	}
}

// TestUploadMemoryClassifyMatchesAcquire proves the logging helper is the
// same policy the core applies, including unknown and oversized clamping.
func TestUploadMemoryClassifyMatchesAcquire(t *testing.T) {
	tests := []struct {
		name        string
		estimate    int64
		budget      int64
		wantWeight  int64
		wantUnknown bool
		wantClamped bool
	}{
		{"unknown zero", 0, 100, 100, true, false},
		{"unknown negative", -5, 100, 100, true, false},
		{"within", 40, 100, 40, false, false},
		{"oversized", 250, 100, 100, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			weight, unknown, clamped := uploadMemoryClassify(tt.estimate, tt.budget)
			if weight != tt.wantWeight || unknown != tt.wantUnknown || clamped != tt.wantClamped {
				t.Fatalf("classify = (%d,%v,%v), want (%d,%v,%v)",
					weight, unknown, clamped, tt.wantWeight, tt.wantUnknown, tt.wantClamped)
			}
			var lease uploadMemoryLease
			admission := newUploadMemoryTestAdmission(t, tt.budget, uploadMemoryHooks{
				Acquire: func(got uploadMemoryLease) { lease = got },
			})
			release, err := admission.Acquire(t.Context(), tt.estimate)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			release()
			if lease.Weight != weight || lease.Unknown != unknown || lease.Clamped != clamped {
				t.Fatalf("core lease = %+v, helper = (%d,%v,%v)", lease, weight, unknown, clamped)
			}
		})
	}
}

// TestUploadMemorySaturatingHelpers proves pathological metadata never wraps.
func TestUploadMemorySaturatingHelpers(t *testing.T) {
	if got := uploadMemoryAdd(math.MaxInt64, 1); got != math.MaxInt64 {
		t.Fatalf("add saturated = %d, want MaxInt64", got)
	}
	if got := uploadMemoryAdd(1, math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("add saturated reversed = %d, want MaxInt64", got)
	}
	if got := uploadMemoryMul(math.MaxInt64, 2); got != math.MaxInt64 {
		t.Fatalf("mul saturated = %d, want MaxInt64", got)
	}
	if got := uploadMemoryMulFloat(math.MaxInt64, 3); got != math.MaxInt64 {
		t.Fatalf("mul float saturated = %d, want MaxInt64", got)
	}
	if got := uploadMemoryNextPow2(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("next pow2 saturated = %d, want MaxInt64", got)
	}
	if got := uploadMemorySub(5, 10); got != 0 {
		t.Fatalf("sub underflow = %d, want 0", got)
	}
	model := uploadMemoryModel{
		SidecarBatchRows:     8192,
		InputFixedBytes:      math.MaxInt64,
		OutputWriterBytes:    math.MaxInt64,
		SidecarOverheadBytes: math.MaxInt64,
		CleanupBytes:         1,
		DecodeAmplification:  math.MaxFloat64,
		GCSlack:              math.MaxFloat64,
	}
	est := model.estimateMerge(uploadMemorySourceSummary{
		FanIn: 1, Rows: 1, UncompressedBytes: math.MaxInt64, WorstRowBytes: math.MaxInt64, DecodedBytes: math.MaxInt64,
	}, index.DefaultParams)
	if est.Bytes != math.MaxInt64 {
		t.Fatalf("saturated estimate = %d, want MaxInt64", est.Bytes)
	}
}

// TestUploadMemoryEstimatePublishedSkipsSources proves cleanup planning needs
// no TenantStore and therefore cannot fail on a partially removed source set.
func TestUploadMemoryEstimatePublishedSkipsSources(t *testing.T) {
	u := &Uploader{memoryModel: newUploadMemoryModel()}
	est, err := u.estimateUploadMemory(t.Context(), nil, Upload{
		Stage:   UploadPublished,
		Sources: []uuid.UUID{uuid.NewV7()},
	})
	if err != nil {
		t.Fatalf("estimate published cleanup: %v", err)
	}
	if !est.Known || est.Merge || est.Bytes != uploadEstimateCleanupBytes {
		t.Fatalf("published estimate = %+v, want known cleanup %d", est, uploadEstimateCleanupBytes)
	}
}

// TestUploadMemoryEstimateReservedSurveysSources proves a fresh merge is
// planned from the live source footers.
func TestUploadMemoryEstimateReservedSurveysSources(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 12, 0, 0, 0, time.UTC)
	first := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "one"}})
	second := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "two"}})
	ts := f.tenantStore(tenant.Default)
	up, err := ts.ReserveUpload(t.Context(), Upload{
		Stream: "api", Hour: hour, Sources: []uuid.UUID{first.ID, second.ID},
	})
	if err != nil {
		t.Fatalf("ReserveUpload: %v", err)
	}
	u := f.uploader(UploaderOptions{UploadMemory: 1 << 30})
	est, err := u.estimateUploadMemory(t.Context(), ts, up)
	if err != nil {
		t.Fatalf("estimate reserved: %v", err)
	}
	if !est.Known || !est.Merge || est.FanIn != 2 {
		t.Fatalf("reserved estimate = %+v, want known merge with fan-in 2", est)
	}
	if est.SourceBytes <= 0 || est.SourceBytes > first.Bytes+second.Bytes {
		t.Fatalf("source bytes = %d, want positive and at most the two file sizes %d", est.SourceBytes, first.Bytes+second.Bytes)
	}
	if est.Bytes <= uploadEstimateInputFixedBytes {
		t.Fatalf("reserved estimate = %d, want more than one input allowance", est.Bytes)
	}
}

// TestUploadMemoryEstimateExistingTempSkipsSources proves a valid finalized
// temp is planned without opening sources, so damaged sources do not matter.
func TestUploadMemoryEstimateExistingTempSkipsSources(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 13, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "keep"}})
	sentinel := errors.New("injected put failure")
	if _, err := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: sentinel},
	}).RunOnce(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, sentinel)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadMerged {
		t.Fatalf("receipt stage = %q, want merged", receipt.Stage)
	}
	// Corrupt the source: an estimate that opened it would become unknown.
	path := f.localPath(tenant.Default, source)
	if err := os.WriteFile(path, []byte("not a parquet file"), 0o640); err != nil {
		t.Fatalf("corrupt source: %v", err)
	}
	u := f.uploader(UploaderOptions{UploadMemory: 1 << 30})
	est, err := u.estimateUploadMemory(t.Context(), f.tenantStore(tenant.Default), receipt)
	if err != nil {
		t.Fatalf("estimate existing temp: %v", err)
	}
	if !est.Known || est.Merge {
		t.Fatalf("existing-temp estimate = %+v, want known put-only", est)
	}
	if _, err := f.uploader(UploaderOptions{UploadMemory: 1 << 30}).RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce with valid temp and corrupted sources: %v", err)
	}
}

// TestUploadMemoryEstimateMissingTempReplansMerge proves a missing temp falls
// back to a source merge plan.
func TestUploadMemoryEstimateMissingTempReplansMerge(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 14, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "temp"}})
	sentinel := errors.New("injected put failure")
	if _, err := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: sentinel},
	}).RunOnce(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, sentinel)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if err := os.Remove(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("remove temp: %v", err)
	}
	u := f.uploader(UploaderOptions{UploadMemory: 1 << 30})
	est, err := u.estimateUploadMemory(t.Context(), f.tenantStore(tenant.Default), receipt)
	if err != nil {
		t.Fatalf("estimate missing temp: %v", err)
	}
	if !est.Known || !est.Merge {
		t.Fatalf("missing-temp estimate = %+v, want known merge", est)
	}
}

// TestUploadMemoryEstimateUnknownDoesNotBypassMergeFault proves an unreadable
// source becomes an unknown estimate, the real merge still fails at its own
// stage, and the receipt resumes once the source is restored.
func TestUploadMemoryEstimateUnknownDoesNotBypassMergeFault(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 15, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "fault"}})
	sentinel := errors.New("injected put failure")
	if _, err := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: sentinel},
	}).RunOnce(t.Context()); !errors.Is(err, sentinel) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, sentinel)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if err := os.Remove(f.tempPath(tenant.Default, receipt.ID.String())); err != nil {
		t.Fatalf("remove temp: %v", err)
	}
	path := f.localPath(tenant.Default, source)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	if err := os.WriteFile(path, []byte("not a parquet file"), 0o640); err != nil {
		t.Fatalf("corrupt source: %v", err)
	}

	u := f.uploader(UploaderOptions{UploadMemory: 1 << 30})
	est, err := u.estimateUploadMemory(t.Context(), f.tenantStore(tenant.Default), receipt)
	if err != nil {
		t.Fatalf("estimate corrupted source: %v", err)
	}
	if est.Known || est.Bytes != 0 {
		t.Fatalf("corrupted-source estimate = %+v, want unknown", est)
	}
	if _, err := f.uploader(UploaderOptions{UploadMemory: 1 << 30}).RunOnce(t.Context()); err == nil {
		t.Fatal("RunOnce with corrupted source succeeded, want the real merge-stage error")
	}
	if got := f.onlyReceipt(tenant.Default); got.ID != receipt.ID {
		t.Fatalf("receipt id = %s, want the resumable %s", got.ID, receipt.ID)
	}
	if err := os.WriteFile(path, original, 0o640); err != nil {
		t.Fatalf("restore source: %v", err)
	}
	report, err := f.uploader(UploaderOptions{UploadMemory: 1 << 30}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce after restoring source: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("restored report = %+v, want one upload", report)
	}
}

// TestUploadMemorySurveyClosesAndBounds proves the preflight reads only the
// trailer when the declared footer is over the cap and closes every file.
func TestUploadMemorySurveyClosesAndBounds(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.November, 6, 16, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "closed"}})
	path := f.localPath(tenant.Default, source)

	t.Run("valid", func(t *testing.T) {
		var closes, readBytes atomic.Int64
		open := func(context.Context, uuid.UUID) (uploadEstimateFile, error) {
			f, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			return &uploadMemoryCountedFile{File: f, closes: &closes, readBytes: &readBytes}, nil
		}
		summary, ok, err := surveyUploadSources(t.Context(), []uuid.UUID{source.ID}, open, uploadEstimateFooterBudgetBytes)
		if err != nil || !ok {
			t.Fatalf("survey = (%+v, %v, %v), want ok", summary, ok, err)
		}
		if closes.Load() != 1 {
			t.Fatalf("closes = %d, want 1", closes.Load())
		}
		if readBytes.Load() < 8 || readBytes.Load() > uploadEstimateFooterMaxBytes+8 {
			t.Fatalf("read bytes = %d, want the bounded footer", readBytes.Load())
		}
		if summary.FanIn != 1 || summary.Rows != 1 {
			t.Fatalf("summary = %+v, want one source with one row", summary)
		}
	})

	t.Run("footer over cap", func(t *testing.T) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read source: %v", err)
		}
		binary.LittleEndian.PutUint32(data[len(data)-8:], uint32(uploadEstimateFooterMaxBytes+1))
		copy(data[len(data)-4:], "PAR1")
		bad := filepath.Join(t.TempDir(), "over-cap.parquet")
		if err := os.WriteFile(bad, data, 0o640); err != nil {
			t.Fatalf("write over-cap file: %v", err)
		}
		var closes, readBytes atomic.Int64
		open := func(context.Context, uuid.UUID) (uploadEstimateFile, error) {
			f, err := os.Open(bad)
			if err != nil {
				return nil, err
			}
			return &uploadMemoryCountedFile{File: f, closes: &closes, readBytes: &readBytes}, nil
		}
		if _, ok, err := surveyUploadSources(t.Context(), []uuid.UUID{source.ID}, open, uploadEstimateFooterBudgetBytes); ok || err != nil {
			t.Fatalf("over-cap survey = (ok=%v, err=%v), want unknown without error", ok, err)
		}
		if closes.Load() != 1 {
			t.Fatalf("over-cap closes = %d, want 1", closes.Load())
		}
		if readBytes.Load() != 8 {
			t.Fatalf("over-cap read bytes = %d, want only the 8-byte trailer", readBytes.Load())
		}
	})
}

// TestUploadMemoryEstimateDoesNotSyncDir arms a pending manifest-directory
// sync and a failing syncDir seam, proves the estimator returns unknown with
// zero sync attempts and leaves the pending flag set, then proves the real
// processing API hits the armed failure and that published cleanup progresses
// once the fault is disarmed.
func TestUploadMemoryEstimateDoesNotSyncDir(t *testing.T) {
	var (
		syncs atomic.Int64
		armed atomic.Bool
		fault error
	)
	f := uploadNewFixtureWithOps(t, func(ops *localFileOps) {
		base := ops.syncDir
		ops.syncDir = func(root *os.Root, name string) error {
			syncs.Add(1)
			if armed.Load() {
				return fault
			}
			return base(root, name)
		}
	})
	hour := time.Date(2026, time.November, 6, 17, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "pending sync"}})
	putErr := errors.New("injected put failure")
	if _, err := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: putErr},
	}).RunOnce(t.Context()); !errors.Is(err, putErr) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, putErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	ts := f.tenantStore(tenant.Default)
	fault = errors.New("injected pending manifest-dir sync failure")
	armed.Store(true)
	// Arm the pending flag a failed directory sync would leave behind. This
	// makes the negative control reachable: a reverted estimator that called
	// OpenUploadTemp/OpenFile would run ensureSyncedLocked and hit the seam.
	ts.mu.Lock()
	ts.pendingSync = true
	ts.mu.Unlock()
	before := syncs.Load()

	u := f.uploader(UploaderOptions{UploadMemory: 1 << 30})
	est, err := u.estimateUploadMemory(t.Context(), ts, receipt)
	if err != nil {
		t.Fatalf("estimate with pending sync: %v", err)
	}
	if est.Known || est.Bytes != 0 {
		t.Fatalf("pending-sync estimate = %+v, want unknown", est)
	}
	if after := syncs.Load(); after != before {
		t.Fatalf("estimator performed %d directory syncs, want 0", after-before)
	}
	ts.mu.Lock()
	stillPending := ts.pendingSync
	ts.mu.Unlock()
	if !stillPending {
		t.Fatal("estimator completed the pending manifest-directory sync")
	}

	// The real processing API must still see and hit the armed fault.
	if _, err := ts.Files(t.Context()); !errors.Is(err, fault) {
		t.Fatalf("real Files error = %v, want the injected sync failure", err)
	}
	if hits := syncs.Load() - before; hits == 0 {
		t.Fatal("real processing never attempted the armed directory sync")
	}

	// Disarm: the real API completes the sync and published cleanup progresses.
	armed.Store(false)
	if _, err := ts.Files(t.Context()); err != nil {
		t.Fatalf("Files after disarming: %v", err)
	}
	if err := ts.MarkStored(t.Context(), receipt.ID); err != nil {
		t.Fatalf("MarkStored: %v", err)
	}
	if err := ts.MarkPublished(t.Context(), receipt.ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if _, err := f.uploader(UploaderOptions{UploadMemory: 1 << 30}).RunOnce(t.Context()); err != nil {
		t.Fatalf("published cleanup after disarming: %v", err)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after cleanup = %+v, want none", got)
	}
	if _, err := os.Stat(f.localPath(tenant.Default, source)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired source still on disk: %v", err)
	}
}

// TestUploadMemoryEstimateCancel proves a canceled context stops the survey
// without opening the next source.
func TestUploadMemoryEstimateCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, ok, err := surveyUploadSources(ctx, []uuid.UUID{uuid.NewV7()}, nil, uploadEstimateFooterBudgetBytes); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("survey canceled = (ok=%v, err=%v), want context.Canceled", ok, err)
	}
}

// ensure the io import is used by the file contract through the concrete
// *os.File wrapper above.
var _ io.Closer = (*uploadMemoryCountedFile)(nil)
