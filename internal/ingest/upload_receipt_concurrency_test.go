package ingest

// Tests for same-partition receipt concurrency: every receipt of one
// tenant/stream/hour partition is an independent upload job, so its merge,
// Put, publication and cleanup may overlap with any other receipt of the same
// partition. The tests drive the real LocalStore, filesystem object store and
// memory catalog, using rendezvous stores and merges to prove genuine overlap
// and the tenant store's own manifest to prove disjoint chunks, stable report
// order, exactly-once Put and shared-directory cleanup.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadReceiptSeed is one seeded source file and the line it holds, kept
// together so a test can predict the rows of every chunk.
type uploadReceiptSeed struct {
	file LocalFile
	line string
}

// uploadReceiptSeedPartition seeds n equal-shaped one-row files of one
// tenant/stream/hour partition and returns them in durable (id) order.
func uploadReceiptSeedPartition(t *testing.T, f *uploadFixture, hour time.Time, n int, prefix string) []uploadReceiptSeed {
	t.Helper()
	seeds := make([]uploadReceiptSeed, 0, n)
	for i := range n {
		line := fmt.Sprintf("%s-%d", prefix, i)
		file := f.seed(tenant.Default, "api", hour, []uploadRow{
			{ts: hour.Add(time.Duration(i) * time.Minute), line: line},
		})
		seeds = append(seeds, uploadReceiptSeed{file: file, line: line})
	}
	slices.SortFunc(seeds, func(a, b uploadReceiptSeed) int { return a.file.ID.Compare(b.file.ID) })
	return seeds
}

// uploadReceiptUnitBytes returns the shared byte size of the seeded files and
// fails when they differ: tests size MergeMax and MinBytes in units of this
// value, so equal files are part of the fixture contract.
func uploadReceiptUnitBytes(t *testing.T, seeds []uploadReceiptSeed) int64 {
	t.Helper()
	if len(seeds) == 0 {
		t.Fatal("no seeded files")
	}
	unit := seeds[0].file.Bytes
	for _, seed := range seeds[1:] {
		if seed.file.Bytes != unit {
			t.Fatalf("seeded file %s is %d bytes, want the shared %d", seed.file.ID, seed.file.Bytes, unit)
		}
	}
	return unit
}

// uploadConcurrentKeyListPut records Put keys from concurrent workers.
type uploadConcurrentKeyListPut struct {
	objstore.Store

	mu   sync.Mutex
	keys []string
}

// Put implements objstore.Store.
func (s *uploadConcurrentKeyListPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Store.Put(ctx, key, r, size)
}

// snapshot returns a copy of the recorded keys.
func (s *uploadConcurrentKeyListPut) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

// uploadStoreThenFailOncePut stores the first matching data object and then
// reports err, simulating a Put whose success the caller never observed. The
// object exists under its key even though the receipt never recorded the Put;
// later Puts delegate normally.
type uploadStoreThenFailOncePut struct {
	objstore.Store
	prefix string
	err    error

	armed atomic.Bool
	mu    sync.Mutex
	keys  []string
}

// Put implements objstore.Store.
func (s *uploadStoreThenFailOncePut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	if err := s.Store.Put(ctx, key, r, size); err != nil {
		return err
	}
	if strings.HasPrefix(key, s.prefix) && strings.HasSuffix(key, objstore.ExtData) &&
		s.armed.CompareAndSwap(false, true) {
		return s.err
	}
	return nil
}

// snapshot returns a copy of the recorded keys.
func (s *uploadStoreThenFailOncePut) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.keys)
}

// TestUploaderReceiptFreshSamePartitionOverlap seeds four due chunks of one
// tenant/stream/hour partition and proves they merge and Put at the same time.
// The Concurrency-1 half runs the same fixture and must fail the rendezvous,
// proving the overlap comes from the receipt-level pool and not from Put
// timing.
func TestUploaderReceiptFreshSamePartitionOverlap(t *testing.T) {
	hour := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	seed := func(t *testing.T) (*uploadFixture, int64) {
		t.Helper()
		f := uploadNewFixture(t)
		seeds := uploadReceiptSeedPartition(t, f, hour, 4, "overlap")
		return f, uploadReceiptUnitBytes(t, seeds)
	}

	t.Run("concurrent", func(t *testing.T) {
		f, unit := seed(t)
		merges := newUploadRendezvousMerge(storage.Merge, 4, 30*time.Second)
		objects := newUploadRendezvousPut(f.objects, 4, 30*time.Second)
		u := f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 4,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
		})
		u.merge = merges.Merge
		report, err := u.RunOnce(t.Context())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if report.Uploads != 4 {
			t.Fatalf("uploads = %d, want 4 same-partition chunks: %+v", report.Uploads, report.Completed)
		}
		if peak := merges.Peak(); peak != 4 {
			t.Fatalf("peak concurrent merges = %d, want 4 same-partition merges in flight", peak)
		}
		if got := f.receipts(tenant.Default); len(got) != 0 {
			t.Fatalf("receipts after pass = %+v, want none", got)
		}
		if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 4 {
			t.Fatalf("catalog rows = %d, want 4", len(rows))
		}
	})

	t.Run("concurrency 1 cannot meet the rendezvous", func(t *testing.T) {
		f, unit := seed(t)
		objects := newUploadRendezvousPut(f.objects, 2, 500*time.Millisecond)
		report, err := f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 1,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
		}).RunOnce(t.Context())
		if err == nil || !strings.Contains(err.Error(), "did not overlap") {
			t.Fatalf("serial RunOnce error = %v, want the rendezvous timeout", err)
		}
		if report.Uploads != 0 {
			t.Fatalf("serial report = %+v, want no uploads", report)
		}
	})
}

// TestUploaderReceiptResumedSamePartitionOverlap leaves three stored receipts
// of one tenant/stream/hour partition behind a failing catalog pass, then
// proves the resume phase rotates and Puts all three concurrently.
func TestUploaderReceiptResumedSamePartitionOverlap(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 3, "resume")
	unit := uploadReceiptUnitBytes(t, seeds)

	catErr := errors.New("catalog unavailable")
	if _, err := f.uploader(UploaderOptions{
		Catalog:     &uploadFailingCatalog{Catalog: f.cat, err: catErr},
		Concurrency: 1, MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, catErr)
	}
	pending := f.receipts(tenant.Default)
	if len(pending) != 3 {
		t.Fatalf("seeded receipts = %d, want 3: %+v", len(pending), pending)
	}
	for _, up := range pending {
		if up.Stage != UploadStored {
			t.Fatalf("seeded receipt %s stage = %q, want stored", up.ID, up.Stage)
		}
	}

	objects := newUploadRendezvousPut(f.objects, 3, 30*time.Second)
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("resume RunOnce: %v", err)
	}
	if report.Uploads != 3 || report.Resumed != 3 {
		t.Fatalf("report = %+v, want 3 resumed uploads", report)
	}
	for i, res := range report.Completed {
		if res.ID != pending[i].ID {
			t.Fatalf("completed[%d] = %s, want durable receipt order %s", i, res.ID, pending[i].ID)
		}
		if res.Key == pending[i].Key {
			t.Fatalf("resumed receipt %s re-Put its old key %q instead of rotating", res.ID, res.Key)
		}
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending files = %d, want 0", len(got))
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 3 {
		t.Fatalf("catalog rows = %d, want 3", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after pass = %+v, want none", got)
	}
}

// TestUploaderReceiptChunksPartitionSources seeds six equal files with
// MergeMax sized for two, then proves one pass reserves three two-file chunks
// whose objects hold exactly their own source rows, whose source sets
// partition disjointly and whose report order follows durable file order.
func TestUploaderReceiptChunksPartitionSources(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 11, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 6, "chunk")
	unit := uploadReceiptUnitBytes(t, seeds)

	objects := &uploadConcurrentKeyListPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 2 * unit,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 3 {
		t.Fatalf("uploads = %d, want 3 chunks of two: %+v", report.Uploads, report.Completed)
	}
	var totalRows int
	for i, res := range report.Completed {
		if res.Sources != 2 {
			t.Fatalf("completed[%d] took %d sources, want 2", i, res.Sources)
		}
		rows := f.readObject(tenant.Default, res.Key)
		totalRows += len(rows)
		got := make([]string, len(rows))
		for j, row := range rows {
			got[j] = row.line
		}
		want := []string{seeds[2*i].line, seeds[2*i+1].line}
		if !slices.Equal(got, want) {
			t.Fatalf("object %d rows = %v, want exactly the chunk's sources %v", i, got, want)
		}
	}
	if totalRows != len(seeds) {
		t.Fatalf("published rows = %d, want %d: chunks double-counted or dropped rows", totalRows, len(seeds))
	}
	uploadPutCounts(t, objects.snapshot())
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending files = %d, want 0 (disjoint chunks drained every source)", len(got))
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 3 {
		t.Fatalf("catalog rows = %d, want 3", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after pass = %+v, want none", got)
	}
}

// TestUploaderReceiptMinBytesRemainderThenAge seeds five equal files with
// MinBytes and MergeMax of two files: one pass drains two two-file chunks and
// leaves one file, the next pass leaves that sub-MinBytes remainder alone, and
// an aged pass drains it.
func TestUploaderReceiptMinBytesRemainderThenAge(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 5, "remainder")
	unit := uploadReceiptUnitBytes(t, seeds)
	now := time.Now()
	options := func(at time.Time) UploaderOptions {
		return UploaderOptions{
			Concurrency: 2, MinBytes: 2 * unit, MaxAge: time.Hour, MergeMax: 2 * unit,
			Now: func() time.Time { return at },
		}
	}

	report, err := f.uploader(options(now)).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if report.Uploads != 2 || report.Completed[0].Sources != 2 || report.Completed[1].Sources != 2 {
		t.Fatalf("first pass = %+v, want two two-file chunks", report)
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 1 {
		t.Fatalf("pending after first pass = %d, want the one-file remainder", len(got))
	}

	report, err = f.uploader(options(now)).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if report.Uploads != 0 {
		t.Fatalf("second pass = %+v, want the sub-MinBytes remainder left alone", report)
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 1 {
		t.Fatalf("pending after second pass = %d, want 1", len(got))
	}

	report, err = f.uploader(options(now.Add(2 * time.Hour))).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("aged RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Completed[0].Sources != 1 {
		t.Fatalf("aged pass = %+v, want the aged remainder", report)
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending after aged pass = %d, want 0", len(got))
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 3 {
		t.Fatalf("catalog rows = %d, want 3", len(rows))
	}
}

// TestUploaderReceiptCancelJoinsAndRetriesPutOnce cancels a same-partition
// pass while two workers are blocked mid-Put, proves RunOnce joins them and
// leaves every chunk resumable, then retries concurrently and Puts every
// object key exactly once.
func TestUploaderReceiptCancelJoinsAndRetriesPutOnce(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 13, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 4, "cancel")
	unit := uploadReceiptUnitBytes(t, seeds)

	objects := newUploadBlockPut(f.objects)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var report UploadReport
	var err error
	go func() {
		report, err = f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 2,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
		}).RunOnce(ctx)
		close(done)
	}()
	objects.waitArrivals(t, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("RunOnce did not join its workers after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context.Canceled", err)
	}
	if report.Uploads != 0 {
		t.Fatalf("canceled report = %+v, want no uploads", report)
	}
	if got := f.receipts(tenant.Default); len(got) != 4 {
		t.Fatalf("receipts after cancellation = %d, want 4 resumable receipts", len(got))
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 0 {
		t.Fatalf("catalog rows after cancellation = %d, want 0", len(rows))
	}

	retry := &uploadConcurrentKeyListPut{Store: f.objects}
	report, err = f.uploader(UploaderOptions{
		Objects: retry, Concurrency: 4,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 4 || report.Resumed != 4 {
		t.Fatalf("retry report = %+v, want 4 resumed uploads", report)
	}
	uploadPutCounts(t, retry.snapshot())
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 4 {
		t.Fatalf("catalog rows after retry = %d, want 4", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after retry = %+v, want none", got)
	}
}

// TestUploaderReceiptConcurrentSameDirCleanup runs four overlapping chunks of
// one partition and proves each removes its own source and temp from the
// shared tenant directory without leaving anything behind.
func TestUploaderReceiptConcurrentSameDirCleanup(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 14, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 4, "cleanup")
	unit := uploadReceiptUnitBytes(t, seeds)

	merges := newUploadRendezvousMerge(storage.Merge, 4, 30*time.Second)
	objects := newUploadRendezvousPut(f.objects, 4, 30*time.Second)
	u := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 4,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	})
	u.merge = merges.Merge
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 4 {
		t.Fatalf("uploads = %d, want 4: %+v", report.Uploads, report.Completed)
	}
	for _, seed := range seeds {
		if _, err := os.Stat(f.localPath(tenant.Default, seed.file)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source %s survived cleanup: %v", seed.file.ID, err)
		}
	}
	uploadsDir := filepath.Join(f.dataDir, "parquet", string(tenant.Default), ".uploads")
	entries, err := os.ReadDir(uploadsDir)
	switch {
	case err == nil && len(entries) != 0:
		t.Fatalf("upload temps left behind in the shared dir: %v", entries)
	case err != nil && !errors.Is(err, os.ErrNotExist):
		t.Fatalf("read uploads dir: %v", err)
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending files = %d, want 0", len(got))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 4 {
		t.Fatalf("catalog rows = %d, want 4", len(rows))
	}
}

// TestUploaderReceiptAmbiguousPutRotatesUnderOverlap fails one same-partition
// data Put after the object was stored, while the other chunks overlap. The
// retry must rotate the poisoned receipt to a new key and never rewrite the
// object the caller never observed as Put.
func TestUploaderReceiptAmbiguousPutRotatesUnderOverlap(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 15, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 3, "ambiguous")
	unit := uploadReceiptUnitBytes(t, seeds)

	putErr := errors.New("ambiguous put")
	poisoned := &uploadStoreThenFailOncePut{
		Store:  f.objects,
		prefix: objstore.LogPrefix(tenant.Default, "api"),
		err:    putErr,
	}
	// All three same-partition data Puts must enter the rendezvous before any
	// delegates to the poisoned store, so the ambiguous Put is reached only
	// while the sibling chunks genuinely overlap.
	objects := newUploadRendezvousPut(poisoned, 3, 30*time.Second)
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context())
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	if report.Uploads != 2 {
		t.Fatalf("report = %+v, want the two healthy chunks to publish", report)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadMerged {
		t.Fatalf("poisoned receipt stage = %q, want merged (Put failed before RecordPut)", receipt.Stage)
	}
	oldKey := receipt.Key
	oldBytes := uploadObjectBytes(t, f, oldKey)
	if got := poisoned.snapshot(); !slices.Contains(got, oldKey) {
		t.Fatalf("poisoned key %q was not Put: %v", oldKey, got)
	}

	retry := &uploadConcurrentKeyListPut{Store: f.objects}
	report, err = f.uploader(UploaderOptions{
		Objects: retry, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("retry report = %+v, want the resumed receipt %s", report, receipt.ID)
	}
	if report.Completed[0].Key == oldKey {
		t.Fatalf("retry re-Put the poisoned key %q instead of rotating", oldKey)
	}
	uploadPutCounts(t, slices.Concat(poisoned.snapshot(), retry.snapshot()))
	if got := uploadObjectBytes(t, f, oldKey); !bytes.Equal(got, oldBytes) {
		t.Fatal("retry rewrote the ambiguous Put's object")
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 3 {
		t.Fatalf("catalog rows = %d, want 3", len(rows))
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending files = %d, want 0", len(got))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
}

// TestUploaderReceiptTombstoneRecoveryUnderOverlap tombstones one stored
// receipt's catalog row while two sibling receipts of the same partition
// resume concurrently. The tombstone retires its sources without any Put, the
// live siblings overlap and rotate, and the tombstone is neither resurrected
// nor overwritten.
func TestUploaderReceiptTombstoneRecoveryUnderOverlap(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 6, 16, 0, 0, 0, time.UTC)
	seeds := uploadReceiptSeedPartition(t, f, hour, 3, "tombstone")
	unit := uploadReceiptUnitBytes(t, seeds)

	catErr := errors.New("catalog unavailable")
	if _, err := f.uploader(UploaderOptions{
		Catalog:     &uploadFailingCatalog{Catalog: f.cat, err: catErr},
		Concurrency: 1, MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, catErr)
	}
	pending := f.receipts(tenant.Default)
	if len(pending) != 3 {
		t.Fatalf("seeded receipts = %d, want 3: %+v", len(pending), pending)
	}
	tomb := pending[0]
	if err := f.cat.AddFiles(t.Context(), uploadCatalogFile(tomb)); err != nil {
		t.Fatalf("seed catalog row: %v", err)
	}
	if err := f.cat.ReplaceFiles(t.Context(), tenant.Default, []string{tomb.Key}, nil); err != nil {
		t.Fatalf("tombstone catalog row: %v", err)
	}
	for _, key := range []string{tomb.Key, objstore.SidecarKey(tomb.Key)} {
		if err := f.objects.Delete(t.Context(), key); err != nil {
			t.Fatalf("delete object %q: %v", key, err)
		}
	}

	objects := newUploadRendezvousPut(f.objects, 2, 30*time.Second)
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: unit,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 3 || report.Resumed != 3 {
		t.Fatalf("report = %+v, want 3 resumed uploads", report)
	}
	if report.Completed[0].ID != tomb.ID {
		t.Fatalf("completed[0] = %s, want the tombstoned receipt %s", report.Completed[0].ID, tomb.ID)
	}
	for _, res := range report.Completed {
		if res.ID != tomb.ID && res.Key == tomb.Key {
			t.Fatalf("live sibling %s reused the tombstoned key %q", res.ID, tomb.Key)
		}
	}
	rows := f.allCatalogFiles(tenant.Default)
	if len(rows) != 3 {
		t.Fatalf("catalog rows = %+v, want the tombstone plus two live rows", rows)
	}
	var deleted int
	for _, row := range rows {
		if row.Deleted() {
			deleted++
			if row.Key != tomb.Key {
				t.Fatalf("tombstoned row = %+v, want key %q", row, tomb.Key)
			}
		}
	}
	if deleted != 1 {
		t.Fatalf("deleted rows = %d, want exactly the seeded tombstone", deleted)
	}
	for _, key := range []string{tomb.Key, objstore.SidecarKey(tomb.Key)} {
		if _, err := f.objects.Stat(t.Context(), key); !errors.Is(err, objstore.ErrNotExist) {
			t.Fatalf("tombstoned object %q was resurrected: %v", key, err)
		}
	}
	if got := mustPending(t, f.tenantStore(tenant.Default)); len(got) != 0 {
		t.Fatalf("pending files = %d, want 0", len(got))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts = %+v, want none", got)
	}
}
