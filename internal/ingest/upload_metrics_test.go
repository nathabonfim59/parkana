package ingest

// Tests for the uploader's stage and job metrics: exact label sets, sample
// counts and sums, the bounded bucket ranges, active-job accounting on
// success, error and cancellation, and source-byte handling when a resumed
// receipt's metadata is unavailable.

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// uploadFailKindPut fails every Put of one object kind with err and delegates
// the other kind, so a test can fail data or sidecar without touching the
// other.
type uploadFailKindPut struct {
	objstore.Store
	failSidecar bool
	err         error
}

func (s *uploadFailKindPut) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if strings.HasSuffix(key, objstore.ExtSidecar) == s.failSidecar {
		return s.err
	}
	return s.Store.Put(ctx, key, r, size)
}

// uploadHistogramValue is one gathered histogram series in a form tests can
// compare without depending on the exposition types.
type uploadHistogramValue struct {
	labels map[string]string
	count  uint64
	sum    float64
	bounds []float64
}

// uploadHistograms gathers every series of the named histogram family. A
// family that was registered but never observed still yields one zero series.
func uploadHistograms(t *testing.T, reg prometheus.Gatherer, name string) []uploadHistogramValue {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range fams {
		if mf.GetName() != name {
			continue
		}
		out := make([]uploadHistogramValue, 0, len(mf.GetMetric()))
		for _, metric := range mf.GetMetric() {
			h := metric.GetHistogram()
			if h == nil {
				t.Fatalf("%s series is not a histogram", name)
			}
			v := uploadHistogramValue{
				labels: map[string]string{},
				count:  h.GetSampleCount(),
				sum:    h.GetSampleSum(),
			}
			for _, lp := range metric.GetLabel() {
				v.labels[lp.GetName()] = lp.GetValue()
			}
			for _, b := range h.GetBucket() {
				v.bounds = append(v.bounds, b.GetUpperBound())
			}
			out = append(out, v)
		}
		return out
	}
	return nil
}

// uploadHistogram returns the series whose labels equal want, and whether it
// exists. Nil and empty want both match a series with no labels.
func uploadHistogram(t *testing.T, reg prometheus.Gatherer, name string, want map[string]string) (uploadHistogramValue, bool) {
	t.Helper()
	for _, v := range uploadHistograms(t, reg, name) {
		if maps.Equal(v.labels, want) {
			return v, true
		}
	}
	return uploadHistogramValue{}, false
}

// uploadFiniteMax returns the largest finite bucket upper bound.
func uploadFiniteMax(bounds []float64) float64 {
	var max float64
	for _, b := range bounds {
		if !math.IsInf(b, 1) && b > max {
			max = b
		}
	}
	return max
}

// TestUploadMetricsObserveCompletedJob runs one real upload and checks every
// stage/job target: the merge and both Put kinds have exactly one sample, the
// source bytes equal the seeded file's compressed size exactly, and the active
// gauge is back to zero.
func TestUploadMetricsObserveCompletedJob(t *testing.T) {
	f := uploadNewFixture(t)
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	hour := time.Date(2026, time.March, 4, 5, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "app", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "first"},
		{ts: hour.Add(2 * time.Minute), line: "second"},
	})
	report, err := f.uploader(UploaderOptions{metrics: m}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if report.Uploads != 1 {
		t.Fatalf("uploads = %d, want 1", report.Uploads)
	}

	merge, ok := uploadHistogram(t, reg, "parkana_upload_merge_seconds", nil)
	if !ok || merge.count != 1 || merge.sum <= 0 {
		t.Fatalf("merge = %+v (found=%v), want exactly one positive sample", merge, ok)
	}
	if len(uploadHistograms(t, reg, "parkana_upload_put_seconds")) != 2 {
		t.Fatalf("put series = %+v, want exactly data and sidecar", uploadHistograms(t, reg, "parkana_upload_put_seconds"))
	}
	for _, kind := range []string{putKindData, putKindSidecar} {
		got, ok := uploadHistogram(t, reg, "parkana_upload_put_seconds", map[string]string{"kind": kind})
		if !ok || got.count != 1 || got.sum <= 0 {
			t.Fatalf("put{kind=%q} = %+v (found=%v), want exactly one positive sample", kind, got, ok)
		}
		if !maps.Equal(got.labels, map[string]string{"kind": kind}) {
			t.Fatalf("put labels = %v, want only kind=%q", got.labels, kind)
		}
	}
	jobBytes, ok := uploadHistogram(t, reg, "parkana_upload_job_source_bytes", nil)
	if !ok || jobBytes.count != 1 {
		t.Fatalf("job source bytes = %+v (found=%v), want exactly one sample", jobBytes, ok)
	}
	if jobBytes.sum != float64(source.Bytes) {
		t.Fatalf("job source bytes sum = %v, want the exact compressed source size %d", jobBytes.sum, source.Bytes)
	}
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
		t.Fatalf("active jobs = %v (found=%v), want 0 after the pass", v, ok)
	}
}

// TestUploadMetricsResumedJobSourceBytes proves a resumed receipt whose merge
// output already exists records its source bytes without observing another
// merge.
func TestUploadMetricsResumedJobSourceBytes(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.March, 4, 6, 0, 0, 0, time.UTC)
	source := f.seed(tenant.Default, "app", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "resume me"},
	})
	failing := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: errors.New("injected put failure")},
	})
	if _, err := failing.RunOnce(t.Context()); err == nil || !strings.Contains(err.Error(), "injected put failure") {
		t.Fatalf("seed RunOnce error = %v, want the injected Put failure", err)
	}
	if got := f.onlyReceipt(tenant.Default); got.Stage != UploadMerged {
		t.Fatalf("receipt stage = %q, want merged before the resume", got.Stage)
	}

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	report, err := f.uploader(UploaderOptions{metrics: m}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("resume RunOnce: %v", err)
	}
	if report.Uploads != 1 || report.Resumed != 1 {
		t.Fatalf("resume report = %+v, want one resumed upload", report)
	}
	merge, ok := uploadHistogram(t, reg, "parkana_upload_merge_seconds", nil)
	if !ok || merge.count != 0 {
		t.Fatalf("merge = %+v (found=%v), want no merge sample for a reused temp", merge, ok)
	}
	jobBytes, ok := uploadHistogram(t, reg, "parkana_upload_job_source_bytes", nil)
	if !ok || jobBytes.count != 1 {
		t.Fatalf("job source bytes = %+v (found=%v), want exactly one sample", jobBytes, ok)
	}
	if jobBytes.sum != float64(source.Bytes) {
		t.Fatalf("job source bytes sum = %v, want the exact compressed source size %d", jobBytes.sum, source.Bytes)
	}
}

// TestUploadMetricsUnknownSourceBytesSkip makes a resumed job whose metadata
// is unavailable observe no source-byte sample, never a fake zero, while the
// upload itself still completes.
func TestUploadMetricsUnknownSourceBytesSkip(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, time.March, 4, 7, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "app", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "unknown sizes"},
	})
	failing := f.uploader(UploaderOptions{
		Objects: &uploadFailBeforePut{Store: f.objects, err: errors.New("injected put failure")},
	})
	if _, err := failing.RunOnce(t.Context()); err == nil {
		t.Fatal("seed RunOnce: want the injected Put failure")
	}

	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	u := f.uploader(UploaderOptions{metrics: m})
	jobs := newResumeJobs(f.tenantStore(tenant.Default), f.receipts(tenant.Default), nil)
	if len(jobs) != 1 || jobs[0].sourceBytesKnown {
		t.Fatalf("jobs = %+v, want one job with unknown source bytes", jobs)
	}
	outcomes := u.runJobs(t.Context(), jobs)
	if len(outcomes) != 1 || !outcomes[0].done || outcomes[0].err != nil {
		t.Fatalf("outcome = %+v, want the resumed upload done without error", outcomes)
	}
	jobBytes, ok := uploadHistogram(t, reg, "parkana_upload_job_source_bytes", nil)
	if !ok || jobBytes.count != 0 {
		t.Fatalf("job source bytes = %+v (found=%v), want no sample when metadata is unknown", jobBytes, ok)
	}
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
		t.Fatalf("active jobs = %v (found=%v), want 0 after the job", v, ok)
	}
}

// TestUploadMetricsActiveJobOnError holds one merge in flight to observe the
// active gauge at 1, then fails it: the gauge returns to 0 and the merge error
// is still recorded.
func TestUploadMetricsActiveJobOnError(t *testing.T) {
	f := uploadNewFixture(t)
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	hour := time.Date(2026, time.March, 4, 8, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "app", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "fail in merge"},
	})
	u := f.uploader(UploaderOptions{metrics: m})
	entered := make(chan struct{})
	release := make(chan error, 1)
	u.merge = func(context.Context, io.Writer, []*storage.Reader, []string) (storage.FileStats, error) {
		close(entered)
		return storage.FileStats{}, <-release
	}
	done := make(chan error, 1)
	go func() {
		_, err := u.RunOnce(t.Context())
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("merge did not start")
	}
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 1 {
		t.Fatalf("active jobs = %v (found=%v), want 1 with a merge in flight", v, ok)
	}
	release <- errors.New("injected merge failure")
	if err := <-done; err == nil || !strings.Contains(err.Error(), "injected merge failure") {
		t.Fatalf("RunOnce error = %v, want the injected merge failure", err)
	}
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
		t.Fatalf("active jobs = %v (found=%v), want 0 after the error", v, ok)
	}
	merge, ok := uploadHistogram(t, reg, "parkana_upload_merge_seconds", nil)
	if !ok || merge.count != 1 {
		t.Fatalf("merge = %+v (found=%v), want the failed merge observed", merge, ok)
	}
}

// TestUploadMetricsPutFailure injects one sentinel Put failure per object kind
// with live metrics: the failed kind still has one positive sample, only the
// bounded kind label is present, the sentinel survives wrapping and the active
// gauge returns to zero.
func TestUploadMetricsPutFailure(t *testing.T) {
	sentinel := errors.New("injected put failure")
	tests := []struct {
		name     string
		sidecar  bool
		wantKind string
	}{
		{name: "data", wantKind: putKindData},
		{name: "sidecar", sidecar: true, wantKind: putKindSidecar},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := uploadNewFixture(t)
			reg := prometheus.NewRegistry()
			m, err := newMetrics(reg)
			if err != nil {
				t.Fatal(err)
			}
			hour := time.Date(2026, time.March, 4, 10, 0, 0, 0, time.UTC)
			f.seed(tenant.Default, "app", hour, []uploadRow{
				{ts: hour.Add(time.Minute), line: "fail the " + tc.name + " put"},
			})
			u := f.uploader(UploaderOptions{
				Objects: &uploadFailKindPut{Store: f.objects, failSidecar: tc.sidecar, err: sentinel},
				metrics: m,
			})
			if _, err := u.RunOnce(t.Context()); !errors.Is(err, sentinel) {
				t.Fatalf("RunOnce error = %v, want the sentinel preserved", err)
			}
			failed, ok := uploadHistogram(t, reg, "parkana_upload_put_seconds", map[string]string{"kind": tc.wantKind})
			if !ok || failed.count != 1 || failed.sum <= 0 {
				t.Fatalf("put{kind=%q} = %+v (found=%v), want one positive sample", tc.wantKind, failed, ok)
			}
			for _, series := range uploadHistograms(t, reg, "parkana_upload_put_seconds") {
				if len(series.labels) != 1 {
					t.Fatalf("put labels = %v, want only kind", series.labels)
				}
				switch kind := series.labels["kind"]; kind {
				case putKindData, putKindSidecar:
				default:
					t.Fatalf("put kind = %q, want only data or sidecar", kind)
				}
			}
			if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
				t.Fatalf("active jobs = %v (found=%v), want 0 after the failure", v, ok)
			}
		})
	}
}

// TestUploadMetricsActiveJobOnCancel cancels the pass while a job is running:
// the gauge returns to 0 even though the merge ends with a context error.
func TestUploadMetricsActiveJobOnCancel(t *testing.T) {
	f := uploadNewFixture(t)
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	hour := time.Date(2026, time.March, 4, 9, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "app", hour, []uploadRow{
		{ts: hour.Add(time.Minute), line: "cancel in merge"},
	})
	u := f.uploader(UploaderOptions{metrics: m})
	entered := make(chan struct{})
	u.merge = func(ctx context.Context, _ io.Writer, _ []*storage.Reader, _ []string) (storage.FileStats, error) {
		close(entered)
		<-ctx.Done()
		return storage.FileStats{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := u.RunOnce(ctx)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("merge did not start")
	}
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 1 {
		t.Fatalf("active jobs = %v (found=%v), want 1 with a merge in flight", v, ok)
	}
	cancel()
	<-done
	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
		t.Fatalf("active jobs = %v (found=%v), want 0 after cancellation", v, ok)
	}
	merge, ok := uploadHistogram(t, reg, "parkana_upload_merge_seconds", nil)
	if !ok || merge.count != 1 {
		t.Fatalf("merge = %+v (found=%v), want the canceled merge observed", merge, ok)
	}
}

// TestUploadMetricsBucketsAndLabels pins the explicit bucket ranges and the
// exact sample counts/sums for direct observations.
func TestUploadMetricsBucketsAndLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	m.observeMerge(2 * time.Second)
	m.observeMerge(3 * time.Second)
	m.observePut(putKindData, 250*time.Millisecond)
	m.observePut(putKindSidecar, 1500*time.Millisecond)
	m.jobStarted(1<<20, true)
	m.jobFinished()
	m.jobStarted(3<<20, true)
	m.jobFinished()

	merge, ok := uploadHistogram(t, reg, "parkana_upload_merge_seconds", nil)
	if !ok || merge.count != 2 || merge.sum != 5 {
		t.Fatalf("merge = %+v (found=%v), want count 2 sum 5", merge, ok)
	}
	if first := merge.bounds[0]; first > 0.001 {
		t.Fatalf("merge first bucket = %v, want milliseconds", first)
	}
	if largest := uploadFiniteMax(merge.bounds); largest < 1800 {
		t.Fatalf("merge largest finite bucket = %v, want tens of minutes", largest)
	}

	putSeries := uploadHistograms(t, reg, "parkana_upload_put_seconds")
	if len(putSeries) != 2 {
		t.Fatalf("put series = %+v, want exactly data and sidecar", putSeries)
	}
	for kind, want := range map[string]float64{putKindData: 0.25, putKindSidecar: 1.5} {
		got, ok := uploadHistogram(t, reg, "parkana_upload_put_seconds", map[string]string{"kind": kind})
		if !ok || got.count != 1 || got.sum != want {
			t.Fatalf("put{kind=%q} = %+v (found=%v), want count 1 sum %v", kind, got, ok, want)
		}
	}

	jobBytes, ok := uploadHistogram(t, reg, "parkana_upload_job_source_bytes", nil)
	if !ok || jobBytes.count != 2 || jobBytes.sum != float64(4<<20) {
		t.Fatalf("job source bytes = %+v (found=%v), want count 2 sum %d", jobBytes, ok, 4<<20)
	}
	if first := jobBytes.bounds[0]; first > 1<<20 {
		t.Fatalf("source first bucket = %v, want MiB range", first)
	}
	if largest := uploadFiniteMax(jobBytes.bounds); largest <= 2<<30 {
		t.Fatalf("source largest finite bucket = %v, want an oversized bucket above 2 GiB", largest)
	}

	if v, ok := gather(t, reg, "parkana_upload_active_jobs"); !ok || v != 0 {
		t.Fatalf("active jobs = %v (found=%v), want 0", v, ok)
	}

	// A nil handle is a focused test's uploader without metrics.
	var none *metrics
	none.observeMerge(time.Second)
	none.observePut(putKindData, time.Second)
	none.jobStarted(1, true)
	none.jobFinished()
}

// TestUploadSourceByteHelpers pins the exact sum and the unknown-metadata
// signal, which is what keeps the histogram from recording a fake zero.
func TestUploadSourceByteHelpers(t *testing.T) {
	first := uuid.NewV7()
	second := uuid.NewV7()
	third := uuid.NewV7()
	bytesByID := map[uuid.UUID]int64{first: 10, second: 20}
	if got, ok := uploadReceiptSourceBytes([]uuid.UUID{first, second}, bytesByID); !ok || got != 30 {
		t.Fatalf("known sources = %d (ok=%v), want 30", got, ok)
	}
	if got, ok := uploadReceiptSourceBytes([]uuid.UUID{first, third}, bytesByID); ok {
		t.Fatalf("missing source = %d (ok=%v), want unknown", got, ok)
	}
	if _, ok := uploadReceiptSourceBytes(nil, bytesByID); ok {
		t.Fatal("empty receipt sources reported known bytes")
	}
	if got, ok := uploadLocalSourceBytes([]LocalFile{{Bytes: 5}, {Bytes: 7}}); !ok || got != 12 {
		t.Fatalf("fresh sources = %d (ok=%v), want 12", got, ok)
	}
	if _, ok := uploadLocalSourceBytes(nil); ok {
		t.Fatal("empty fresh sources reported known bytes")
	}
}

// TestServiceUploaderMetricsWiring checks the default uploader factory receives
// the service's registered metrics handle.
func TestServiceUploaderMetricsWiring(t *testing.T) {
	deps, _ := testDeps(t, t.TempDir())
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
	if u.metrics != s.m {
		t.Fatalf("uploader metrics = %p, want the service handle %p", u.metrics, s.m)
	}
}
