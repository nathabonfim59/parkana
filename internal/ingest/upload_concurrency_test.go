package ingest

// Tests for the uploader's bounded parallel scheduling (upload_parallel.go).
// They run against the real LocalStore, filesystem object store and memory
// catalog, using narrow upload-prefixed wrappers to prove that independent
// receipts — across and within tenant/stream/hour partitions — overlap, that
// concurrency bounds the overlap, that one job's failure does not lose
// another's success, that cancellation joins the workers, and that resumes
// still win over fresh reservations. No interface is changed for tests.

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// uploadRendezvousPut proves overlap: every Put waits until want Puts are in
// flight simultaneously, then lets them all through. A Put that waits longer
// than the timeout fails the pass (and poisons the store so a serial scheduler
// fails fast instead of hanging once per group), so a pass that cannot
// overlap work of the wanted width cannot pass a test sized for it.
type uploadRendezvousPut struct {
	objstore.Store
	want    int
	timeout time.Duration

	mu     sync.Mutex
	live   int
	ready  chan struct{}
	opened sync.Once
	failed atomic.Bool
}

func newUploadRendezvousPut(inner objstore.Store, want int, timeout time.Duration) *uploadRendezvousPut {
	return &uploadRendezvousPut{Store: inner, want: want, timeout: timeout, ready: make(chan struct{})}
}

func (s *uploadRendezvousPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.failed.Load() {
		return errors.New("upload: rendezvous already failed")
	}
	s.mu.Lock()
	s.live++
	met := s.live >= s.want
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.live--
		s.mu.Unlock()
	}()
	if met {
		s.opened.Do(func() { close(s.ready) })
	}
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case <-s.ready:
	case <-timer.C:
		s.failed.Store(true)
		return fmt.Errorf("upload: only %d of %d Puts in flight after %s: uploads did not overlap",
			s.snapshot(), s.want, s.timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Store.Put(ctx, key, r, size)
}

// snapshot returns how many Puts are currently waiting at the rendezvous.
func (s *uploadRendezvousPut) snapshot() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

// uploadPeakPut tracks the maximum number of simultaneous Puts. Each worker
// runs one job at a time and a job Puts its data and sidecar objects one after
// the other, so the peak can never exceed the worker count.
type uploadPeakPut struct {
	objstore.Store

	mu   sync.Mutex
	cur  int
	peak int
}

func (s *uploadPeakPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.cur++
	s.peak = max(s.peak, s.cur)
	s.mu.Unlock()
	err := s.Store.Put(ctx, key, r, size)
	s.mu.Lock()
	s.cur--
	s.mu.Unlock()
	return err
}

// Peak returns the largest number of Puts observed in flight at once.
func (s *uploadPeakPut) Peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// uploadBlockPut holds every Put until the pass context is done, then fails
// with the context error without writing anything, so a test can cancel the
// pass while workers are mid-flight and prove they all join.
type uploadBlockPut struct {
	objstore.Store
	arrived chan struct{}
}

func newUploadBlockPut(inner objstore.Store) *uploadBlockPut {
	return &uploadBlockPut{Store: inner, arrived: make(chan struct{}, 64)}
}

func (s *uploadBlockPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.arrived <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

// waitArrivals waits for n Puts to be blocked inside Put.
func (s *uploadBlockPut) waitArrivals(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-s.arrived:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for %d blocked Puts", n)
		}
	}
}

// uploadSequencedPut records the order in which each stream's Puts ran, so a
// test can prove one stream's uploads all completed before another's began.
type uploadSequencedPut struct {
	objstore.Store

	mu     sync.Mutex
	events []uploadPutEvent
}

// uploadPutEvent is one recorded Put: the stream whose key it wrote and its
// position in the pass's Put sequence.
type uploadPutEvent struct {
	stream string
	seq    int
}

func (s *uploadSequencedPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	parts := strings.Split(key, "/") // {tenant}/logs/{stream}/...
	s.mu.Lock()
	s.events = append(s.events, uploadPutEvent{stream: parts[2], seq: len(s.events)})
	s.mu.Unlock()
	return s.Store.Put(ctx, key, r, size)
}

// firstOf returns the first recorded Put sequence number of stream, or a
// number past the end when the stream never uploaded.
func (s *uploadSequencedPut) firstOf(stream string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev.stream == stream {
			return ev.seq
		}
	}
	return len(s.events)
}

// lastOf returns the last recorded Put sequence number of stream.
func (s *uploadSequencedPut) lastOf(stream string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := -1
	for _, ev := range s.events {
		if ev.stream == stream {
			last = ev.seq
		}
	}
	return last
}

// uploadFailPutFor records every attempted Put and fails the ones whose key
// starts with prefix before writing, so exactly one partition of a pass fails
// while the others succeed.
type uploadFailPutFor struct {
	objstore.Store
	prefix string
	err    error

	mu       sync.Mutex
	attempts []string
}

func (s *uploadFailPutFor) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.attempts = append(s.attempts, key)
	s.mu.Unlock()
	if strings.HasPrefix(key, s.prefix) {
		return s.err
	}
	return s.Store.Put(ctx, key, r, size)
}

// uploadRendezvousMerge is the merge-seam twin of uploadRendezvousPut: every
// merge waits until want merges are running simultaneously, then proceeds. A
// merge that waits longer than the timeout fails (and poisons the seam), so a
// scheduler that serializes the heavy merge stage cannot pass a test sized
// for concurrent merges. This measures the real worker unit — merge, sidecar
// build and Put all happen inside one job — at its most expensive stage.
type uploadRendezvousMerge struct {
	want    int
	timeout time.Duration
	inner   mergeFunc

	mu     sync.Mutex
	live   int
	peak   int
	ready  chan struct{}
	opened sync.Once
	failed atomic.Bool
}

func newUploadRendezvousMerge(inner mergeFunc, want int, timeout time.Duration) *uploadRendezvousMerge {
	return &uploadRendezvousMerge{inner: inner, want: want, timeout: timeout, ready: make(chan struct{})}
}

// Merge implements mergeFunc.
func (m *uploadRendezvousMerge) Merge(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
	if m.failed.Load() {
		return storage.FileStats{}, errors.New("upload: merge rendezvous already failed")
	}
	m.mu.Lock()
	m.live++
	m.peak = max(m.peak, m.live)
	met := m.live >= m.want
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.live--
		m.mu.Unlock()
	}()
	if met {
		m.opened.Do(func() { close(m.ready) })
	}
	timer := time.NewTimer(m.timeout)
	defer timer.Stop()
	select {
	case <-m.ready:
	case <-timer.C:
		m.failed.Store(true)
		return storage.FileStats{}, fmt.Errorf("upload: only %d of %d merges running after %s: merges did not overlap",
			m.snapshot(), m.want, m.timeout)
	case <-ctx.Done():
		return storage.FileStats{}, ctx.Err()
	}
	return m.inner(ctx, w, inputs, promoted)
}

// snapshot returns how many merges are currently waiting at the rendezvous.
func (m *uploadRendezvousMerge) snapshot() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.live
}

// Peak returns the largest number of merges observed running at once; with a
// met rendezvous it is at least want, proving that many merges were genuinely
// simultaneous.
func (m *uploadRendezvousMerge) Peak() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peak
}

// uploadPeakMerge counts how many merges run at once and reports the peak, so
// a bounded workload test can show the active merge-worker high-water mark.
type uploadPeakMerge struct {
	inner mergeFunc

	mu   sync.Mutex
	cur  int
	peak int
}

func newUploadPeakMerge(inner mergeFunc) *uploadPeakMerge {
	return &uploadPeakMerge{inner: inner}
}

// Merge implements mergeFunc.
func (m *uploadPeakMerge) Merge(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
	m.mu.Lock()
	m.cur++
	m.peak = max(m.peak, m.cur)
	m.mu.Unlock()
	stats, err := m.inner(ctx, w, inputs, promoted)
	m.mu.Lock()
	m.cur--
	m.mu.Unlock()
	return stats, err
}

// Peak returns the largest number of merges observed running at once, the
// active merge-worker high-water mark a bounded workload must respect.
func (m *uploadPeakMerge) Peak() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peak
}

func TestUploaderConcurrencyValidation(t *testing.T) {
	f := uploadNewFixture(t)
	options := func(n int) UploaderOptions {
		return UploaderOptions{
			Local: f.local, Objects: f.objects, Catalog: f.cat,
			PromotedFor: func(string) []string { return nil },
			MinBytes:    1, MaxAge: time.Hour, MergeMax: 1024,
			Concurrency: n,
		}
	}
	for _, n := range []int{-1, 65, 128} {
		if _, err := NewUploader(options(n)); err == nil || !strings.Contains(err.Error(), "Concurrency") {
			t.Fatalf("NewUploader(Concurrency=%d) error = %v, want a Concurrency rejection", n, err)
		}
	}
	u, err := NewUploader(options(0))
	if err != nil {
		t.Fatalf("NewUploader(Concurrency=0): %v", err)
	}
	if u.concurrency != uploadDefaultConcurrency {
		t.Fatalf("default concurrency = %d, want %d", u.concurrency, uploadDefaultConcurrency)
	}
	for _, n := range []int{1, 64} {
		u, err := NewUploader(options(n))
		if err != nil {
			t.Fatalf("NewUploader(Concurrency=%d): %v", n, err)
		}
		if u.concurrency != n {
			t.Fatalf("Concurrency=%d stored as %d", n, u.concurrency)
		}
	}
}

// TestUploaderParallelPartitionsOverlap proves three same-tenant partitions
// actually run at the same time at the two expensive stages of the worker
// unit: their merges must rendezvous simultaneously (the stage that holds up
// to MergeMax bytes each), and so must their data Puts. The serial half of the
// test drives the same fixture with Concurrency 1, where neither rendezvous
// can be met and the pass fails.
func TestUploaderParallelPartitionsOverlap(t *testing.T) {
	seedThree := func(t *testing.T) *uploadFixture {
		t.Helper()
		f := uploadNewFixture(t)
		hour := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
		for _, stream := range []string{"api", "auth", "web"} {
			f.seed(tenant.Default, stream, hour, []uploadRow{{ts: hour.Add(time.Minute), line: stream}})
		}
		return f
	}

	t.Run("concurrent", func(t *testing.T) {
		f := seedThree(t)
		merges := newUploadRendezvousMerge(storage.Merge, 3, 30*time.Second)
		objects := newUploadRendezvousPut(f.objects, 3, 30*time.Second)
		u := f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 3,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		})
		u.merge = merges.Merge
		report, err := u.RunOnce(t.Context())
		if err != nil {
			t.Fatalf("concurrent RunOnce: %v", err)
		}
		if report.Uploads != 3 {
			t.Fatalf("uploads = %d, want 3: %+v", report.Uploads, report.Completed)
		}
		if peak := merges.Peak(); peak != 3 {
			t.Fatalf("peak concurrent merges = %d, want exactly 3 simultaneously running merges", peak)
		}
	})

	t.Run("serial cannot meet the rendezvous", func(t *testing.T) {
		f := seedThree(t)
		merges := newUploadRendezvousMerge(storage.Merge, 3, 500*time.Millisecond)
		objects := newUploadRendezvousPut(f.objects, 3, 500*time.Millisecond)
		u := f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 1,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		})
		u.merge = merges.Merge
		report, err := u.RunOnce(t.Context())
		if err == nil || !strings.Contains(err.Error(), "did not overlap") {
			t.Fatalf("serial RunOnce error = %v, want the rendezvous timeout", err)
		}
		if report.Uploads != 0 {
			t.Fatalf("serial report = %+v, want no uploads", report)
		}
	})
}

// TestUploaderParallelMultipleTenantsOverlap proves partitions of different
// tenants overlap too: four Puts across two tenants must be in flight at once.
func TestUploaderParallelMultipleTenantsOverlap(t *testing.T) {
	f := uploadNewFixture(t)
	other := tenant.ID("other")
	hour := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	for _, id := range []tenant.ID{tenant.Default, other} {
		for _, stream := range []string{"api", "web"} {
			f.seed(id, stream, hour, []uploadRow{{ts: hour.Add(time.Minute), line: string(id) + "-" + stream}})
		}
	}
	objects := newUploadRendezvousPut(f.objects, 4, 30*time.Second)
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 4,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 4 {
		t.Fatalf("uploads = %d, want 4: %+v", report.Uploads, report.Completed)
	}
	for _, id := range []tenant.ID{tenant.Default, other} {
		if got := len(f.liveCatalogFiles(id)); got != 2 {
			t.Fatalf("tenant %q catalog rows = %d, want 2", id, got)
		}
	}
}

// TestUploaderParallelBoundAndDeterministicReport seeds eight partitions
// across two tenants and proves the pass respects the worker bound (never more
// than Concurrency Puts in flight) while every group still publishes, the
// report keeps the serial pass's deterministic order, and the sixteen sources
// partition disjointly into eight uploads.
func TestUploaderParallelBoundAndDeterministicReport(t *testing.T) {
	f := uploadNewFixture(t)
	other := tenant.ID("other")
	hour := time.Date(2026, 11, 5, 11, 0, 0, 0, time.UTC)
	streams := []string{"api", "auth", "billing", "checkout"}
	type group struct {
		id     tenant.ID
		stream string
	}
	var want []group
	for _, id := range []tenant.ID{tenant.Default, other} {
		for _, stream := range streams {
			for i := range 2 {
				f.seed(id, stream, hour, []uploadRow{{ts: hour.Add(time.Duration(i) * time.Minute), line: "row"}})
			}
			want = append(want, group{id: id, stream: stream})
		}
	}
	objects := &uploadPeakPut{Store: f.objects}
	merges := newUploadPeakMerge(storage.Merge)
	u := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	})
	u.merge = merges.Merge
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if peak := objects.Peak(); peak > 3 {
		t.Fatalf("peak concurrent Puts = %d, want at most the concurrency bound 3", peak)
	}
	if peak := merges.Peak(); peak > 3 {
		t.Fatalf("peak concurrent merges = %d, want at most the concurrency bound 3", peak)
	}
	t.Logf("upload concurrency evidence: peak merges=%d peak puts=%d bound=3 groups=8 sources=16",
		merges.Peak(), objects.Peak())
	if report.Uploads != len(want) || report.Resumed != 0 {
		t.Fatalf("report = %+v, want %d fresh uploads", report, len(want))
	}
	// The report order must equal the serial dispatch order: tenants sorted,
	// then each tenant's groups by stream and hour.
	for i, res := range report.Completed {
		if res.Tenant != want[i].id || res.Stream != want[i].stream || !res.Hour.Equal(hour) {
			t.Fatalf("completed[%d] = %+v, want tenant/stream %q/%s in deterministic order", i, res, want[i].id, want[i].stream)
		}
		if res.Sources != 2 {
			t.Fatalf("upload of %q %s took %d sources, want 2", res.Tenant, res.Stream, res.Sources)
		}
	}
	var sources int
	for _, res := range report.Completed {
		sources += res.Sources
	}
	if sources != 16 {
		t.Fatalf("total sources = %d, want 16 (disjoint, none double-counted)", sources)
	}
	for _, id := range []tenant.ID{tenant.Default, other} {
		if pending := mustPending(t, f.tenantStore(id)); len(pending) != 0 {
			t.Fatalf("tenant %q pending files = %d, want 0", id, len(pending))
		}
		if got := f.receipts(id); len(got) != 0 {
			t.Fatalf("tenant %q receipts = %+v, want none", id, got)
		}
		if rows := f.liveCatalogFiles(id); len(rows) != len(streams) {
			t.Fatalf("tenant %q catalog rows = %d, want %d", id, len(rows), len(streams))
		}
	}
}

// TestUploaderParallelErrorContinuesOtherGroups fails exactly one partition's
// Put while the others succeed, then retries: the failed receipt rotates to a
// fresh key, everything publishes, and no object key is ever Put twice.
func TestUploaderParallelErrorContinuesOtherGroups(t *testing.T) {
	f := uploadNewFixture(t)
	other := tenant.ID("other")
	hour := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "ok-api"}})
	f.seed(tenant.Default, "auth", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "fails"}})
	f.seed(other, "api", hour, []uploadRow{{ts: hour.Add(3 * time.Minute), line: "ok-other"}})

	putErr := errors.New("auth stream rejected")
	objects := &uploadFailPutFor{
		Store:  f.objects,
		prefix: objstore.LogPrefix(tenant.Default, "auth"),
		err:    putErr,
	}
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 3,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if !errors.Is(err, putErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, putErr)
	}
	if report.Uploads != 2 {
		t.Fatalf("report = %+v, want the two healthy uploads to complete", report)
	}
	receipt := f.onlyReceipt(tenant.Default) // auth is the tenant's only receipt left
	if receipt.Stream != "auth" || receipt.Stage != UploadMerged {
		t.Fatalf("failed receipt = %+v, want auth at merged (Put failed before RecordPut)", receipt)
	}
	oldKey := receipt.Key
	if pending := mustPending(t, f.tenantStore(tenant.Default)); len(pending) != 1 {
		t.Fatalf("auth sources = %d, want the one unretired source", len(pending))
	}

	// Retry with a healthy store: the failed receipt resumes, rotates (its
	// Put outcome is unknown to the new pass) and publishes. One resumed job
	// runs alone, so the unsynchronized key recorder is safe.
	retry := &uploadKeyListPut{Store: f.objects}
	report, err = f.uploader(UploaderOptions{
		Objects: retry, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 || report.Completed[0].ID != receipt.ID {
		t.Fatalf("retry report = %+v, want the resumed auth receipt %s", report, receipt.ID)
	}
	if report.Completed[0].Key == oldKey {
		t.Fatalf("retry re-Put the failed key %q instead of rotating", oldKey)
	}
	// Across both passes every object key was Put at most once, and the failed
	// key's single attempt never wrote.
	allPuts := slices.Concat(objects.attempts, retry.keys)
	counts := uploadPutCounts(t, allPuts)
	for key, n := range counts {
		if n != 1 {
			t.Fatalf("object key %q Put %d times, want 1 (all puts %v)", key, n, allPuts)
		}
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 2 {
		t.Fatalf("tenant %q catalog rows = %d, want 2 (api and the retried auth)", tenant.Default, len(rows))
	}
	if rows := f.liveCatalogFiles(other); len(rows) != 1 {
		t.Fatalf("tenant %q catalog rows = %d, want 1", other, len(rows))
	}
	for _, id := range []tenant.ID{tenant.Default, other} {
		if got := f.receipts(id); len(got) != 0 {
			t.Fatalf("tenant %q receipts = %+v, want none", id, got)
		}
	}
}

// TestUploaderParallelCancelJoinsWorkers cancels the pass while two workers
// are blocked mid-Put with four jobs still waiting. RunOnce must return
// (joining its workers and its dispatch), report the cancellation, leave
// every group's receipt resumable and publish nothing; a later pass resumes
// all six receipts and Puts every object key exactly once.
func TestUploaderParallelCancelJoinsWorkers(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	for i := range 6 {
		f.seed(tenant.Default, fmt.Sprintf("s%d", i), hour, []uploadRow{{ts: hour.Add(time.Minute), line: "cancel"}})
	}

	objects := newUploadBlockPut(f.objects)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var report UploadReport
	var err error
	go func() {
		report, err = f.uploader(UploaderOptions{
			Objects: objects, Concurrency: 2,
			MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
		}).RunOnce(ctx)
		close(done)
	}()
	objects.waitArrivals(t, 2) // both workers are inside their first Put
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
	if got := len(f.receipts(tenant.Default)); got != 6 {
		t.Fatalf("receipts after cancellation = %d, want 6 resumable receipts", got)
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 0 {
		t.Fatalf("catalog rows after cancellation = %d, want 0", len(rows))
	}

	// The blocked Puts never delegated, so no object exists; the retry pass
	// publishes everything. It runs serially (Concurrency 1), which also
	// exercises the one-worker resume path.
	retry := &uploadKeyListPut{Store: f.objects}
	report, err = f.uploader(UploaderOptions{
		Objects: retry, Concurrency: 1,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("retry RunOnce: %v", err)
	}
	if report.Uploads != 6 || report.Resumed != 6 {
		t.Fatalf("retry report = %+v, want 6 resumed uploads", report)
	}
	dataKeys, sidecarKeys := uploadSplitPutKeys(t, retry.keys)
	if len(dataKeys) != 6 || len(sidecarKeys) != 6 {
		t.Fatalf("retry Puts = %d data %d sidecar, want 6 of each", len(dataKeys), len(sidecarKeys))
	}
	counts := uploadPutCounts(t, retry.keys)
	for key, n := range counts {
		if n != 1 {
			t.Fatalf("object key %q Put %d times, want 1", key, n)
		}
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 6 {
		t.Fatalf("catalog rows after retry = %d, want 6", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after retry = %+v, want none", got)
	}
}

// TestUploaderParallelResumeBeforeFreshReservations leaves one stored receipt
// in stream api and fresh work in stream web, then proves with four workers
// that every api Put completed before any web Put: the resume phase joins
// before the fresh reservation even exists.
func TestUploaderParallelResumeBeforeFreshReservations(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 5, 14, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "resumed"}})
	catErr := errors.New("catalog unavailable")
	if _, err := f.uploader(UploaderOptions{
		Catalog:  &uploadFailingCatalog{Catalog: f.cat, err: catErr},
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("seed RunOnce error = %v, want %v", err, catErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored {
		t.Fatalf("seeded receipt stage = %q, want stored", receipt.Stage)
	}
	// Fresh, never-reserved work in a second stream.
	f.seed(tenant.Default, "web", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "fresh"}})

	objects := &uploadSequencedPut{Store: f.objects}
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 4,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 2 || report.Resumed != 1 {
		t.Fatalf("report = %+v, want one resumed and one fresh upload", report)
	}
	if lastAPI, firstWeb := objects.lastOf("api"), objects.firstOf("web"); lastAPI >= firstWeb {
		t.Fatalf("api Puts ran up to %d while web started at %d: fresh work raced the resume phase",
			lastAPI, firstWeb)
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after pass = %+v, want none", got)
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 2 {
		t.Fatalf("catalog rows = %d, want 2", len(rows))
	}
}

// TestUploaderParallelSamePartitionReceiptsReportDurableOrder builds two
// stored receipts of one tenant/stream/hour partition behind a failing catalog
// pass, then seeds one more fresh source of that partition. With four workers
// the resumed pair overlaps (the Put rendezvous would time out if the receipts
// were serialized), while the pass reports the partition's uploads in durable
// order — the pending receipts by id first, the fresh chunk after — and their
// sources partition disjointly into the published objects.
func TestUploaderParallelSamePartitionReceiptsReportDurableOrder(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 5, 15, 0, 0, 0, time.UTC)
	files := []LocalFile{
		f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(time.Minute), line: "one"}}),
		f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(2 * time.Minute), line: "two"}}),
	}
	slices.SortFunc(files, func(a, b LocalFile) int { return a.ID.Compare(b.ID) })
	one := files[0].Bytes
	if files[1].Bytes != one {
		t.Fatalf("seeded file sizes differ: %d and %d", files[0].Bytes, files[1].Bytes)
	}

	// Pass 1: cap merges at one source and fail the catalog, leaving two
	// stored receipts of this partition.
	if _, err := f.uploader(UploaderOptions{
		Catalog:     &uploadFailingCatalog{Catalog: f.cat, err: errors.New("catalog unavailable")},
		Concurrency: 1, MinBytes: 1, MaxAge: time.Hour, MergeMax: one,
	}).RunOnce(t.Context()); err == nil {
		t.Fatal("seed RunOnce succeeded with a failing catalog")
	}
	ups := f.receipts(tenant.Default)
	if len(ups) != 2 {
		t.Fatalf("seeded receipts = %d, want 2: %+v", len(ups), ups)
	}
	for i, up := range ups {
		if len(up.Sources) != 1 || up.Sources[0] != files[i].ID || up.Stage != UploadStored {
			t.Fatalf("seeded receipt %d = %+v, want files[%d] stored", i, up, i)
		}
	}

	// One fresh source of the same partition, reserved only in the fresh
	// phase after the resume barrier.
	f.seed(tenant.Default, "api", hour, []uploadRow{{ts: hour.Add(3 * time.Minute), line: "three"}})

	// Pass 2: four workers. The rendezvous only closes once both resumed
	// receipts are inside Put, proving same-partition receipts overlap; the
	// later single fresh Put passes straight through the closed rendezvous.
	objects := newUploadRendezvousPut(f.objects, 2, 30*time.Second)
	report, err := f.uploader(UploaderOptions{
		Objects: objects, Concurrency: 4,
		MinBytes: 1, MaxAge: time.Hour, MergeMax: one,
	}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 3 || report.Resumed != 2 {
		t.Fatalf("report = %+v, want 2 resumed and 1 fresh upload", report)
	}
	if report.Completed[0].ID != ups[0].ID || report.Completed[1].ID != ups[1].ID {
		t.Fatalf("resumed order = %s then %s, want durable order %s then %s",
			report.Completed[0].ID, report.Completed[1].ID, ups[0].ID, ups[1].ID)
	}
	if report.Completed[0].Sources != 1 || report.Completed[1].Sources != 1 {
		t.Fatalf("resumed sources = %d, %d, want one each",
			report.Completed[0].Sources, report.Completed[1].Sources)
	}
	if report.Completed[2].Resumed || report.Completed[2].Sources != 1 {
		t.Fatalf("fresh upload = %+v, want the last source", report.Completed[2])
	}
	if pending := mustPending(t, f.tenantStore(tenant.Default)); len(pending) != 0 {
		t.Fatalf("pending files = %d, want 0", len(pending))
	}
	if rows := f.liveCatalogFiles(tenant.Default); len(rows) != 3 {
		t.Fatalf("catalog rows = %d, want 3", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after pass = %+v, want none", got)
	}
}
