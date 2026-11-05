package ingest

// Bounded parallel scheduling for the uploader. RunOnce collects the pass's
// work as per-receipt jobs and drives them on a worker pool capped by
// UploaderOptions.Concurrency. Receipts are independent by construction —
// disjoint sources, per-receipt ids, object keys and temps, and one tenant
// store whose manifest commits serialize — so any two receipts may run in
// parallel, across partitions and within one tenant/stream/hour partition
// alike; the stage transitions of a single receipt always run sequentially on
// one worker.

import (
	"context"
	"fmt"
	"sync"
	"uuid"
)

// uploadJob is the pass's unit of schedulable work: one pending receipt, fresh
// or resumed. Jobs are independent by construction — disjoint sources,
// per-receipt object keys, temps and ids, and one tenant store whose manifest
// commits serialize under its own mutex — so they may run in parallel in any
// grouping, including several receipts of the same tenant/stream/hour
// partition.
type uploadJob struct {
	ts *TenantStore
	up Upload
	// resumed marks receipts that predated this pass; their results are
	// reported with Resumed set.
	resumed bool
	// sourceBytes is the exact compressed size of the receipt's local source
	// files, known when their manifest metadata was available at dispatch.
	sourceBytes int64
	// sourceBytesKnown is false when any source size could not be resolved;
	// the job then records no source-byte sample rather than a zero.
	sourceBytesKnown bool
}

// jobOutcome is what a worker did with one job. Outcomes live in a slice
// keyed by dispatch order, so the pass report never depends on which worker
// finished first.
type jobOutcome struct {
	result UploadResult
	err    error
	// done reports that the receipt completed and result holds its outcome.
	done bool
	// canceled records that the pass context ended before the job was
	// attempted or dispatched; the untouched receipt simply stays pending for
	// the next pass.
	canceled bool
}

// newResumeJobs flattens one tenant's pending receipts — which Uploads lists
// sorted by stream, hour and id — into per-receipt jobs that keep that durable
// dispatch and report order. Execution is concurrent; only the order in which
// the pass dispatches and reports the receipts stays deterministic.
// bytesByID carries the tenant's source-file sizes for the job source-byte
// metric; it may be nil, in which case those jobs record no sample.
func newResumeJobs(ts *TenantStore, receipts []Upload, bytesByID map[uuid.UUID]int64) []uploadJob {
	jobs := make([]uploadJob, 0, len(receipts))
	for _, up := range receipts {
		sourceBytes, known := uploadReceiptSourceBytes(up.Sources, bytesByID)
		jobs = append(jobs, uploadJob{
			ts: ts, up: up, resumed: true,
			sourceBytes: sourceBytes, sourceBytesKnown: known,
		})
	}
	return jobs
}

// runJobs drives jobs on at most u.concurrency workers and returns one
// outcome per job, in dispatch order. The job channel is capped at the worker
// count — never at the job list size — so pending dispatch is bounded by
// Concurrency, not by how much work one pass found. Dispatching stops at
// context cancellation, the channel is closed and every worker is joined
// before returning; a worker always finishes the job it is in. One receipt is
// dispatched exactly once per pass, so its stage transitions never overlap
// with themselves.
func (u *Uploader) runJobs(ctx context.Context, jobs []uploadJob) []jobOutcome {
	outcomes := make([]jobOutcome, len(jobs))
	if len(jobs) == 0 {
		return outcomes
	}
	workers := min(u.concurrency, len(jobs))
	queue := make(chan int, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range queue {
				outcomes[i] = u.processJob(ctx, jobs[i])
			}
		})
	}
	dispatched := make([]bool, len(jobs))
feed:
	for i := range jobs {
		select {
		case queue <- i:
			dispatched[i] = true
		case <-ctx.Done():
			// Stop feeding: workers drain the capped channel, finish their
			// current job and exit on the closed channel below.
			break feed
		}
	}
	close(queue)
	wg.Wait()
	// Jobs the canceled pass never dispatched report no result and no error;
	// RunOnce folds their cancellation into one context error for the whole
	// phase.
	for i, got := range dispatched {
		if !got {
			outcomes[i].canceled = true
		}
	}
	return outcomes
}

// processJob drives one receipt to published and cleans it up. A failure is
// reported and leaves the receipt retryable for the next pass; a canceled
// context leaves it untouched.
//
// The active-jobs gauge covers exactly the jobs a worker started processing: a
// job canceled before it was attempted is not counted, and every path out of a
// started job decrements through the defer.
func (u *Uploader) processJob(ctx context.Context, j uploadJob) jobOutcome {
	var out jobOutcome
	if err := ctx.Err(); err != nil {
		out.canceled = true
		return out
	}
	estimate, err := u.estimate(ctx, j.ts, j.up)
	if err != nil {
		if ctx.Err() != nil {
			out.canceled = true
			return out
		}
		out.err = fmt.Errorf("upload %s: estimate memory: %w", j.up.ID, err)
		return out
	}
	release, err := u.memory.Acquire(ctx, estimate.Bytes)
	if err != nil {
		if ctx.Err() != nil {
			out.canceled = true
			return out
		}
		out.err = fmt.Errorf("upload %s: admit memory: %w", j.up.ID, err)
		return out
	}
	// The release defer is registered before the active-jobs defer so the
	// active gauge drops before the core returns the weight to a waiter; the
	// core's own release bookkeeping runs before the semaphore exposes it.
	defer release()
	u.logAdmitted(j, estimate)
	u.metrics.jobStarted(j.sourceBytes, j.sourceBytesKnown)
	defer u.metrics.jobFinished()
	res, err := u.processUpload(ctx, j.ts, j.up)
	if err != nil {
		out.err = fmt.Errorf("upload %s: %w", j.up.ID, err)
		return out
	}
	res.Resumed = j.resumed
	out.result = res
	out.done = true
	return out
}

// logAdmitted emits the one bounded resource/source-byte event per admitted
// receipt. It carries no line bytes, label names or values, and adds no metric
// labels. sourceBytes is omitted when the receipt's manifest metadata did not
// resolve them.
func (u *Uploader) logAdmitted(j uploadJob, est uploadMemoryEstimate) {
	if u.log == nil {
		return
	}
	budget := u.memory.Budget()
	weight, _, _ := uploadMemoryClassify(est.Bytes, budget.Bytes)
	attrs := []any{
		"receiptID", j.up.ID.String(),
		"sourceCount", len(j.up.Sources),
		"estimateBytes", est.Bytes,
		"weightBytes", weight,
		"budgetBytes", budget.Bytes,
		"estimateKnown", est.Known,
		"mergeRequired", est.Merge,
	}
	if j.sourceBytesKnown {
		attrs = append(attrs, "sourceBytes", j.sourceBytes)
	}
	u.log.Info("upload receipt admitted", attrs...)
}

// collectJobs folds worker outcomes into the pass report and error list in
// dispatch order, so a parallel pass reports exactly what the serial one did.
// One context error is appended for a phase whose work was cut short by
// cancellation.
func (u *Uploader) collectJobs(ctx context.Context, outcomes []jobOutcome, report *UploadReport, errs *[]error) {
	canceled := false
	for _, oc := range outcomes {
		if oc.done {
			addUploadResult(report, oc.result)
		}
		if oc.err != nil {
			*errs = append(*errs, oc.err)
		}
		canceled = canceled || oc.canceled
	}
	if canceled {
		if err := ctx.Err(); err != nil {
			*errs = append(*errs, fmt.Errorf("ingest: uploader: %w", err))
		}
	}
}
