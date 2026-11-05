package ingest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// defaultShutdownTimeout bounds the detached drain context on shutdown.
const defaultShutdownTimeout = 30 * time.Second

// errDurability wraps failures to confirm that a generation's WAL frontier is
// durable. Publishing past such a frontier would let replay skip records that
// may not exist, so every error carrying it is fatal to the service.
var errDurability = errors.New("ingest: generation frontier durability unknown")

// walAPI is the service's slice of *wal.WAL. It exists so tests can script
// enqueue, durability and replay outcomes; production adapts *wal.WAL because
// wal.Receipt's waiter is unexported and cannot be fabricated by a fake.
type walAPI interface {
	Enqueue(context.Context, tenant.ID, wal.Payload) (enqueueResult, error)
	Replay(context.Context, tenant.ID, wal.Position) iter.Seq2[wal.Record, error]
	Reclaim(context.Context, tenant.ID, wal.Position) error
	Tenants(context.Context) ([]tenant.ID, error)
	// Sync is the shutdown force barrier: it makes already-admitted WAL
	// writes durable without closing the WAL, leaving reclaim for the final
	// flush pass.
	Sync(context.Context) error
	Close(context.Context) error
}

// enqueueResult is an admitted record's position plus its two completion
// outcomes, decoupled from wal.Receipt so fakes can construct one. written
// completes when the record's frames reach the segment file; wait completes
// only when a sync covers the record. A fake that leaves written nil keeps
// its old meaning: the service falls back to wait.
type enqueueResult struct {
	pos     wal.Position
	written func(context.Context) error
	wait    func(context.Context) error
}

// walAdapter maps *wal.WAL onto walAPI.
type walAdapter struct{ *wal.WAL }

// Enqueue implements walAPI.
func (a walAdapter) Enqueue(ctx context.Context, id tenant.ID, p wal.Payload) (enqueueResult, error) {
	r, err := a.WAL.Enqueue(ctx, id, p)
	if err != nil {
		return enqueueResult{}, err
	}
	return enqueueResult{pos: r.Position, written: r.Written, wait: r.Wait}, nil
}

// genFlusher publishes one sealed generation to local Parquet and commits its
// manifest frontier. *Flusher satisfies it.
type genFlusher interface {
	Flush(context.Context, SealedGeneration) error
}

// uploaderAPI is the service's slice of *Uploader.
type uploaderAPI interface {
	RunOnce(context.Context) (UploadReport, error)
}

// serviceOptions holds the replaceable seams of the service. Production uses
// the zero value; tests inject fakes through newService.
type serviceOptions struct {
	// openWAL opens the tenant WAL; default wal.Open + walAdapter.
	openWAL func(context.Context, string, wal.Config) (walAPI, error)
	// openLocal opens the durable local publication state; default OpenLocal.
	openLocal func(context.Context, string) (*LocalStore, error)
	// newFlusher builds the generation publisher; default NewFlusher without
	// a ReclaimFunc (the service reclaims at manifest frontiers itself).
	newFlusher func(*LocalStore) (genFlusher, error)
	// newUploader builds the upload worker; default NewUploader.
	newUploader func(*LocalStore, objstore.Store, catalog.Catalog) (uploaderAPI, error)
	// indexMetrics is the shared sidecar-build handle registered at
	// construction; the default newUploader passes it through.
	indexMetrics *index.Metrics
	// metrics is the handle registered at construction; the default
	// newUploader passes it through so stage and job timings land on
	// Deps.Metrics. It is assigned after defaults builds the factory.
	metrics *metrics
	// isFatal classifies worker errors; default defaultFatal.
	isFatal func(error) bool
	// allocator feeds the memtable; nil uses memory.DefaultAllocator.
	allocator memory.Allocator
	// now feeds the uploader's clock; nil uses time.Now.
	now func() time.Time
	// shutdownTimeout bounds the drain; zero uses defaultShutdownTimeout.
	shutdownTimeout time.Duration
	// transform is Options.Transform, passed to the push handler.
	transform TransformFunc
}

// service is the ingester role: it owns the WAL, memtable, local publication
// and upload scheduling for every tenant. New performs validation and route
// registration only; all durable resources open inside Run so a service that
// is constructed but never run holds nothing.
type service struct {
	cfg     config.Config
	log     *slog.Logger
	objects objstore.Store
	catalog catalog.Catalog
	mem     *Memtable
	m       *metrics
	tail    *tailHub
	// tailWriteTimeout bounds one live-tail NDJSON write. Tests shorten it.
	tailWriteTimeout time.Duration
	opts             serviceOptions
	walMax           int64
	// walSync is the configured acknowledgment policy. It is resolved at
	// construction so an invalid mode cannot reach wal.Open.
	walSync         wal.SyncMode
	walSyncInterval time.Duration

	gate      gate
	accepting atomic.Bool
	failed    atomic.Bool

	tmu     sync.Mutex
	tenants map[tenant.ID]*tenantState
	taken   map[tenant.ID][]*Generation // sealed gens held for flush retry

	recQ         *receiptQueue
	deadlineCh   chan struct{} // closed when the drain deadline expires
	deadlineOnce sync.Once
	fatalCh      chan error

	// The fields below are assigned in Run before the gate opens; admission
	// reads are ordered by the gate mutex.
	wal         walAPI
	local       *LocalStore
	flusher     genFlusher
	uploader    uploaderAPI
	runCtx      context.Context
	watchCancel context.CancelFunc
}

var _ app.Service = (*service)(nil)

// New is the ingester role factory without hooks.
func New(deps app.Deps) (app.Service, error) {
	return newService(deps, serviceOptions{})
}

// Options are the ingester's pluggable hooks. The zero value is New.
type Options struct {
	// Transform rewrites every validated push before it is made durable
	// (see TransformFunc). Every push endpoint the ingester serves applies it.
	Transform TransformFunc
}

// NewWith returns an ingester role factory with o's hooks.
func NewWith(o Options) app.Factory {
	return func(deps app.Deps) (app.Service, error) {
		return newService(deps, serviceOptions{transform: o.Transform})
	}
}

// newService validates deps and configuration, registers metrics, the push
// route and the readiness check, and returns the service. No durable resource
// is opened and no Arrow buffer allocated: a service that never runs owns
// nothing, and every error return leaves nothing to clean up.
func newService(deps app.Deps, opts serviceOptions) (*service, error) {
	cfg := deps.Config
	switch {
	case deps.Logger == nil:
		return nil, errors.New("ingest: Deps.Logger is required")
	case deps.HTTP == nil:
		return nil, errors.New("ingest: Deps.HTTP is required")
	case deps.Metrics == nil:
		return nil, errors.New("ingest: Deps.Metrics is required")
	case deps.Store == nil:
		return nil, errors.New("ingest: Deps.Store is required")
	case deps.Catalog == nil:
		return nil, errors.New("ingest: Deps.Catalog is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("ingest: -data.dir is required")
	}
	if err := validateIngest(cfg.Ingest); err != nil {
		return nil, err
	}
	walMax, err := walMaxRecordBytes(int64(cfg.Ingest.MaxRequestSize))
	if err != nil {
		return nil, err
	}
	walSync, walSyncInterval, err := walSyncSettings(cfg.Ingest)
	if err != nil {
		return nil, err
	}
	if deps.HTTP.Registered(lokiapi.Push) {
		return nil, fmt.Errorf("ingest: route %q already registered", lokiapi.Push)
	}
	if deps.HTTP.Registered(lokiapi.OTLPLogs) {
		return nil, fmt.Errorf("ingest: route %q already registered", lokiapi.OTLPLogs)
	}
	if deps.HTTP.Registered(wire.Pattern) {
		return nil, fmt.Errorf("ingest: route %q already registered", wire.Pattern)
	}
	opts.defaults(deps)
	m, err := newMetrics(deps.Metrics)
	if err != nil {
		return nil, err
	}
	// The default uploader factory, built above, reads this handle when Run
	// constructs the uploader.
	opts.metrics = m
	tm, err := newTailMetrics(deps.Metrics)
	if err != nil {
		return nil, err
	}
	// Sidecar-build metrics are shared with the other write paths; combined
	// roles may have registered this collector already and get the same handle.
	im, err := index.RegisterMetrics(deps.Metrics)
	if err != nil {
		return nil, err
	}
	opts.indexMetrics = im
	tailBuffer := cmp.Or(cfg.LogQL.Tail.BufferEntries, config.DefaultTailBufferEntries)
	tail, err := newTailHub(tailBuffer, tm)
	if err != nil {
		return nil, err
	}
	mem, err := NewMemtable(MemtableOptions{
		Allocator:        opts.allocator,
		StreamLabels:     cfg.StreamLabels,
		PromotedFor:      cfg.Ingest.PromotedFor,
		MemtableMaxBytes: int64(cfg.Ingest.MemtableMaxBytes),
		WALMaxBytes:      int64(cfg.Ingest.WALMaxBytes),
	})
	if err != nil {
		return nil, err
	}
	s := &service{
		cfg:              cfg,
		log:              deps.Logger,
		objects:          deps.Store,
		catalog:          deps.Catalog,
		mem:              mem,
		m:                m,
		tail:             tail,
		tailWriteTimeout: defaultTailWriteTimeout,
		opts:             opts,
		walMax:           walMax,
		walSync:          walSync,
		walSyncInterval:  walSyncInterval,
		gate:             newGate(),
		tenants:          make(map[tenant.ID]*tenantState),
		taken:            make(map[tenant.ID][]*Generation),
		recQ:             newReceiptQueue(),
		// Large enough that concurrent fail callers never block.
		fatalCh:    make(chan error, 4),
		deadlineCh: make(chan struct{}),
	}
	h, err := NewPushHandler(PushOptions{
		MaxRequestBytes: int64(cfg.Ingest.MaxRequestSize),
		MaxLineBytes:    int64(cfg.Ingest.MaxLineSize),
		Append:          s.append,
		Transform:       opts.transform,
	})
	if err != nil {
		return nil, err
	}
	deps.HTTP.Handle(lokiapi.Push, h)
	// The OTLP endpoint shares the push limits, transform hook and append
	// path; only its decoding and the empty-export shortcut differ.
	oh, err := NewOTLPHandler(PushOptions{
		MaxRequestBytes: int64(cfg.Ingest.MaxRequestSize),
		MaxLineBytes:    int64(cfg.Ingest.MaxLineSize),
		Append:          s.append,
		Transform:       opts.transform,
	})
	if err != nil {
		return nil, err
	}
	deps.HTTP.Handle(lokiapi.OTLPLogs, oh)
	deps.HTTP.Handle(wire.Pattern, http.HandlerFunc(s.serveTail))
	deps.HTTP.AddReadyCheck("ingest", s.ready)
	return s, nil
}

// defaults fills unset options from deps' configuration.
func (o *serviceOptions) defaults(deps app.Deps) {
	cfg := deps.Config
	if o.openWAL == nil {
		o.openWAL = func(ctx context.Context, dir string, c wal.Config) (walAPI, error) {
			w, err := wal.Open(ctx, dir, c)
			if err != nil {
				return nil, err
			}
			return walAdapter{w}, nil
		}
	}
	if o.openLocal == nil {
		o.openLocal = OpenLocal
	}
	if o.newFlusher == nil {
		o.newFlusher = func(ls *LocalStore) (genFlusher, error) {
			return NewFlusher(FlushOptions{
				Store:       ls,
				PromotedFor: cfg.Ingest.PromotedFor,
				Concurrency: cfg.Ingest.FlushConcurrency,
			})
		}
	}
	if o.newUploader == nil {
		o.newUploader = func(ls *LocalStore, objects objstore.Store, cat catalog.Catalog) (uploaderAPI, error) {
			return NewUploader(UploaderOptions{
				Local:        ls,
				Objects:      objects,
				Catalog:      cat,
				PromotedFor:  cfg.Ingest.PromotedFor,
				MinBytes:     int64(cfg.Ingest.UploadMinBytes),
				MaxAge:       cfg.Ingest.UploadMaxAge,
				MergeMax:     int64(cfg.Ingest.UploadMergeMax),
				Concurrency:  cfg.Ingest.UploadConcurrency,
				UploadMemory: int64(cfg.Ingest.UploadMemory),
				Index:        cfg.Index.Params(),
				IndexMetrics: o.indexMetrics,
				metrics:      o.metrics,
				log:          deps.Logger,
				Now:          o.now,
			})
		}
	}
	if o.isFatal == nil {
		o.isFatal = defaultFatal
	}
	if o.now == nil {
		o.now = time.Now
	}
	if o.shutdownTimeout <= 0 {
		o.shutdownTimeout = defaultShutdownTimeout
	}
}

// validateIngest rechecks the ingest limits and intervals so a Config built
// without config.Load is still rejected when incoherent.
func validateIngest(c config.Ingest) error {
	var errs []error
	if c.MaxLineSize <= 0 || c.MaxRequestSize <= 0 {
		errs = append(errs, errors.New("ingest: -ingest.max-line-size and -ingest.max-request-size must be positive"))
	} else if c.MaxLineSize > c.MaxRequestSize {
		errs = append(errs, errors.New("ingest: -ingest.max-line-size must not exceed -ingest.max-request-size"))
	}
	if c.MemtableMaxBytes <= 0 || c.WALMaxBytes <= 0 {
		errs = append(errs, errors.New("ingest: -ingest.memtable-max and -ingest.wal-max must be positive"))
	}
	for name, d := range map[string]time.Duration{
		"-ingest.flush-interval":  c.FlushInterval,
		"-ingest.upload-interval": c.UploadInterval,
		"-ingest.upload-max-age":  c.UploadMaxAge,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("ingest: %s must be positive", name))
		}
	}
	if c.UploadMinBytes <= 0 || c.UploadMergeMax <= 0 {
		errs = append(errs, errors.New("ingest: -ingest.upload-min and -ingest.upload-merge-max must be positive"))
	} else if c.UploadMinBytes > c.UploadMergeMax {
		errs = append(errs, errors.New("ingest: -ingest.upload-min must not exceed -ingest.upload-merge-max"))
	}
	// Zero selects the documented default; only an explicit out-of-range
	// count is rejected.
	if c.FlushConcurrency < 0 || c.FlushConcurrency > maxFlushConcurrency {
		errs = append(errs, fmt.Errorf("ingest: -ingest.flush-concurrency must be between 1 and %d, got %d", maxFlushConcurrency, c.FlushConcurrency))
	}
	if c.UploadConcurrency < 0 || c.UploadConcurrency > uploadMaxConcurrency {
		errs = append(errs, fmt.Errorf("ingest: -ingest.upload-concurrency must be between 1 and %d, got %d", uploadMaxConcurrency, c.UploadConcurrency))
	}
	return errors.Join(errs...)
}

// walMaxRecordBytes derives the largest versioned WAL payload a bounded push
// can produce. The expanded request is limited to maxRequest bytes, but the
// versioned JSON payload amplifies it: an empty protobuf entry costs ~2 wire
// bytes yet serializes to ~90 bytes of envelope (~45x), and line or label
// bytes can escape to 6x. A 64x factor covers both with margin plus a fixed
// envelope allowance. The bound must fit the WAL's uint32 frame length, which
// caps the supported -ingest.max-request-size.
func walMaxRecordBytes(maxRequest int64) (int64, error) {
	const (
		amplification = 64
		envelopeSlack = 8 << 10
	)
	if maxRequest <= 0 {
		return 0, fmt.Errorf("ingest: -ingest.max-request-size must be positive, got %d", maxRequest)
	}
	if maxRequest > (math.MaxUint32-envelopeSlack)/amplification {
		return 0, fmt.Errorf("ingest: -ingest.max-request-size %d unsupported: derived WAL record bound exceeds %d", maxRequest, int64(math.MaxUint32))
	}
	return maxRequest*amplification + envelopeSlack, nil
}

// defaultFatal classifies worker errors: corrupt state, ordering violations
// and closed-resource misuse can never be retried into consistency, so they
// stop the service. Ordinary IO failures are not listed and retry.
func defaultFatal(err error) bool {
	return errors.Is(err, ErrSequence) ||
		errors.Is(err, ErrPreparedMismatch) ||
		errors.Is(err, ErrGenerationGap) ||
		errors.Is(err, ErrGenerationOverlap) ||
		errors.Is(err, ErrManifestCorrupt) ||
		errors.Is(err, ErrManifestVersion) ||
		errors.Is(err, ErrFileMissing) ||
		errors.Is(err, ErrUploadConflict) ||
		errors.Is(err, ErrUploadStage) ||
		errors.Is(err, ErrKeyRotationOverflow) ||
		errors.Is(err, ErrTenantMismatch) ||
		errors.Is(err, ErrLocalClosed) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, wal.ErrClosed) ||
		errors.Is(err, wal.ErrCorrupt) ||
		errors.Is(err, wal.ErrInvalidFrontier) ||
		errors.Is(err, wal.ErrUnsupportedVersion) ||
		errors.Is(err, wal.ErrTenantMismatch)
}

// gate synchronizes admission with shutdown: while open, entrants are counted;
// once closed, wait returns after every entrant has left. No push can pass the
// check and then still be preparing when the drain seals the memtable or
// closes the WAL.
type gate struct {
	mu            sync.Mutex
	open          bool
	inflight      int
	drained       chan struct{}
	drainedClosed bool
}

func newGate() gate { return gate{drained: make(chan struct{})} }

// enter reports whether the caller may proceed. A true return must be paired
// with leave.
func (g *gate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return false
	}
	g.inflight++
	return true
}

func (g *gate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inflight--
	if g.inflight == 0 && !g.open && !g.drainedClosed {
		g.drainedClosed = true
		close(g.drained)
	}
}

// close stops new entrants. Idempotent.
func (g *gate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = false
	if g.inflight == 0 && !g.drainedClosed {
		g.drainedClosed = true
		close(g.drained)
	}
}

// wait blocks until every entrant has left or ctx ends.
func (g *gate) wait(ctx context.Context) error {
	select {
	case <-g.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// svcReceipt tracks one admitted WAL record's write and durability outcomes.
// The push path may wait for written directly; the receipt watcher completes
// done with the durable result, which the push path and the flusher observe.
type svcReceipt struct {
	id      tenant.ID
	seq     uint64
	written func(context.Context) error
	wait    func(context.Context) error
	done    chan struct{}
	err     error // set before done closes
}

// receiptQueue is an unbounded, close-aware FIFO between the admission path
// and the durability watcher. Pushes never block the admission path.
type receiptQueue struct {
	mu     sync.Mutex
	q      []*svcReceipt
	closed bool
	sig    chan struct{}
}

func newReceiptQueue() *receiptQueue {
	return &receiptQueue{sig: make(chan struct{}, 1)}
}

func (q *receiptQueue) push(r *svcReceipt) {
	q.mu.Lock()
	if !q.closed {
		q.q = append(q.q, r)
	}
	q.mu.Unlock()
	select {
	case q.sig <- struct{}{}:
	default:
	}
}

// pop returns the next receipt, or false once the queue is closed and empty
// or ctx ends.
func (q *receiptQueue) pop(ctx context.Context) (*svcReceipt, bool) {
	for {
		q.mu.Lock()
		if len(q.q) > 0 {
			r := q.q[0]
			q.q = q.q[1:]
			q.mu.Unlock()
			return r, true
		}
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		q.mu.Unlock()
		select {
		case <-q.sig:
		case <-ctx.Done():
			return nil, false
		}
	}
}

// close stops pushes and lets the watcher drain what remains.
func (q *receiptQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.sig <- struct{}{}:
	default:
	}
}

// tenantState serializes WAL enqueue with memtable admission for one tenant
// and tracks its durability watermark.
type tenantState struct {
	mu      sync.Mutex
	durable uint64                 // highest sequence observed durable
	pending map[uint64]*svcReceipt // admitted, outcome not yet observed
}

// Run opens the WAL and local store, replays every tenant above its committed
// manifest frontier, then accepts pushes while the flush and upload workers
// run. On ctx.Done or a fatal worker failure it stops admission, drains
// in-flight pushes, seals and flushes under a bounded detached context and
// closes every resource it opened; nil is returned only when the drain was
// clean. Run is single-use: a service instance runs once.
func (s *service) Run(ctx context.Context) (retErr error) {
	// Live-tail subscriptions only live as long as the service does; closing
	// the hub wakes every reader so handlers return during shutdown.
	defer s.tail.close()
	if ctx.Err() != nil {
		// A context dead before anything opened is a clean non-start, not a
		// service failure: returning ctx.Err() would turn a DeadlineExceeded
		// shutdown signal into a reported error.
		return nil
	}
	w, err := s.opts.openWAL(ctx, s.cfg.DataDir, wal.Config{
		MaxRecordBytes: int(s.walMax),
		SyncMode:       s.walSync,
		SyncInterval:   s.walSyncInterval,
		OnSync:         s.m.observeSync,
	})
	if err != nil {
		return fmt.Errorf("ingest: open wal: %w", err)
	}
	running := false
	var ls *LocalStore
	defer func() {
		if running {
			return
		}
		s.mem.Close()
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.shutdownTimeout)
		defer cancel()
		if ls != nil {
			retErr = errors.Join(retErr, ls.Close())
		}
		retErr = errors.Join(retErr, w.Close(cleanCtx))
	}()
	ls, err = s.opts.openLocal(ctx, s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("ingest: open local store: %w", err)
	}
	s.wal = w
	s.local = ls
	// The flusher is built before replay so auto-sealed replay generations
	// publish during iteration instead of accumulating in memory.
	if s.flusher, err = s.opts.newFlusher(ls); err != nil {
		return fmt.Errorf("ingest: flusher: %w", err)
	}
	if err := s.replay(ctx); err != nil {
		return err
	}
	if s.uploader, err = s.opts.newUploader(ls, s.objects, s.catalog); err != nil {
		return fmt.Errorf("ingest: uploader: %w", err)
	}
	// Restore each tenant's cumulative rotation total from its committed
	// manifest so the counter survives restarts; ready implies restored.
	if err := s.refreshRotations(ctx); err != nil {
		return err
	}
	s.runCtx = ctx
	running = true
	s.startAccepting()
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	watchCtx, watchCancel := context.WithCancel(context.WithoutCancel(ctx))
	s.watchCancel = watchCancel
	var wg sync.WaitGroup
	wg.Go(func() { s.watchReceipts(watchCtx) })
	wg.Go(func() { s.flushLoop(workerCtx) })
	wg.Go(func() { s.uploadLoop(workerCtx) })
	s.log.Info("ingest service ready", "dataDir", s.cfg.DataDir)
	var fatal error
	select {
	case <-ctx.Done():
	case err := <-s.fatalCh:
		fatal = err
	}
	err = s.drain(ctx, cancelWorkers, &wg)
	for len(s.fatalCh) > 0 {
		fatal = errors.Join(fatal, <-s.fatalCh)
	}
	return errors.Join(fatal, err)
}

// replay re-admits every WAL record above each tenant's committed manifest
// frontier into the memtable, marking replayed sequences durable (they are
// already on disk). It runs before the gate opens so no push can interleave.
func (s *service) replay(ctx context.Context) error {
	wt, err := s.wal.Tenants(ctx)
	if err != nil {
		return fmt.Errorf("ingest: wal tenants: %w", err)
	}
	lt, err := s.local.Tenants(ctx)
	if err != nil {
		return fmt.Errorf("ingest: local tenants: %w", err)
	}
	ids := unionTenants(wt, lt)
	for _, id := range ids {
		fr, err := s.local.Frontier(ctx, id)
		if err != nil {
			return fmt.Errorf("ingest: manifest frontier %s: %w", id, err)
		}
		for rec, err := range s.wal.Replay(ctx, id, wal.Position{Tenant: id, Seq: fr.Last}) {
			if err != nil {
				return fmt.Errorf("ingest: replay %s: %w", id, err)
			}
			if err := s.replayAdmit(ctx, id, rec); err != nil {
				return err
			}
			// Publish generations sealed by admission thresholds while the
			// replay is still streaming, bounding the rebuild memory. WAL
			// reclaim stays out of replay: the wal package excludes it while
			// a tenant replays, and the first flush pass covers it.
			if err := s.publishSealed(ctx, id); err != nil {
				return fmt.Errorf("ingest: replay publish %s: %w", id, err)
			}
		}
	}
	return nil
}

// publishSealed publishes every sealed generation of the tenant during
// replay. A failure aborts startup: unreplayed records stay in the WAL.
func (s *service) publishSealed(ctx context.Context, id tenant.ID) error {
	gens, err := s.mem.TakeSealed(ctx, id)
	if err != nil {
		return err
	}
	for i, gen := range gens {
		if err := s.publish(ctx, id, gen); err != nil {
			for _, g := range gens[i:] {
				g.Release()
			}
			return err
		}
		s.m.publishOK()
	}
	return nil
}

// replayAdmit feeds one replayed record through the same admission path as a
// live push, minus the WAL enqueue.
func (s *service) replayAdmit(ctx context.Context, id tenant.ID, rec wal.Record) error {
	tctx := tenant.With(ctx, id)
	req := rec.Request
	prepared, err := s.mem.Prepare(tctx, id, &req)
	if err != nil {
		return fmt.Errorf("ingest: replay %s sequence %d: %w", id, rec.Position.Seq, err)
	}
	// Re-encode to recover the payload byte size the generation accounting
	// saw at admission; replay is not a hot path.
	payload, err := wal.Encode(tctx, req)
	if err != nil {
		return fmt.Errorf("ingest: replay %s sequence %d: %w", id, rec.Position.Seq, err)
	}
	ts := s.state(id)
	ts.mu.Lock()
	err = s.mem.AppendPrepared(tctx, id, rec.Position.Seq, int64(payload.Size()), prepared)
	ts.mu.Unlock()
	if err != nil {
		return fmt.Errorf("ingest: replay %s sequence %d: %w", id, rec.Position.Seq, err)
	}
	ts.mu.Lock()
	if rec.Position.Seq > ts.durable {
		ts.durable = rec.Position.Seq
	}
	ts.mu.Unlock()
	s.m.replay.WithLabelValues(string(id)).Inc()
	return nil
}

// unionTenants returns the sorted set union of two tenant id lists.
func unionTenants(a, b []tenant.ID) []tenant.ID {
	out := slices.Clone(a)
	for _, id := range b {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// startAccepting opens the admission gate and marks the service ready. The
// transition happens under the gate mutex so a concurrent failure can never
// be overwritten back to ready; a recorded failure keeps the gate shut.
func (s *service) startAccepting() {
	s.gate.mu.Lock()
	if !s.failed.Load() {
		s.gate.open = true
		s.accepting.Store(true)
	}
	s.gate.mu.Unlock()
}

// stopAccepting closes the gate and marks the service unready. Idempotent.
func (s *service) stopAccepting() {
	s.gate.close()
	s.accepting.Store(false)
}

// fail records a fatal condition: admission stops, readiness fails and Run is
// told to drain and return the error. Safe for concurrent use.
func (s *service) fail(err error) {
	s.failed.Store(true)
	s.stopAccepting()
	select {
	case s.fatalCh <- err:
	default:
	}
}

// ready is the /ready check: failure takes precedence over every other
// state, then the gate decides.
func (s *service) ready(context.Context) error {
	if s.failed.Load() {
		return errors.New("ingest: failed")
	}
	if s.accepting.Load() {
		return nil
	}
	return errors.New("ingest: replaying WAL or shutting down")
}

// state returns the tenant's admission state, creating it on first use.
func (s *service) state(id tenant.ID) *tenantState {
	s.tmu.Lock()
	defer s.tmu.Unlock()
	ts := s.tenants[id]
	if ts == nil {
		ts = &tenantState{pending: make(map[uint64]*svcReceipt)}
		s.tenants[id] = ts
	}
	return ts
}

// append is the push handler's AppendFunc: it prepares and encodes outside the
// tenant lock, serializes WAL enqueue with memtable admission inside it, and
// waits outside the lock for the configured acknowledgment: the durable
// receipt in SyncAlways mode, the write receipt in SyncInterval mode.
func (s *service) append(ctx context.Context, id tenant.ID, req *logs.PushRequest) error {
	if !s.gate.enter() {
		return ErrUnavailable
	}
	defer s.gate.leave()
	if s.failed.Load() {
		return ErrUnavailable
	}
	tctx := tenant.With(ctx, id)
	prepared, err := s.mem.Prepare(tctx, id, req)
	if err != nil {
		return err
	}
	payload, err := wal.Encode(tctx, *req)
	if err != nil {
		return err
	}
	// opCtx detaches admission from client cancellation: once the WAL accepts
	// the record, memtable admission must complete so the WAL and memtable
	// sequences cannot diverge. Keep this critical section short — queue
	// admission plus builder append; phase 3 benchmarks it against 31 MB/s.
	opCtx := tenant.With(context.WithoutCancel(s.runCtx), id)
	ts := s.state(id)
	ts.mu.Lock()
	er, err := s.wal.Enqueue(ctx, id, payload)
	if err != nil {
		ts.mu.Unlock()
		return s.enqueueErr(ctx, err)
	}
	rec := &svcReceipt{id: id, seq: er.pos.Seq, written: er.written, wait: er.wait, done: make(chan struct{})}
	// The receipt is tracked before memtable admission: once the WAL owns
	// the sequence its durability must be observed even when admission
	// fails, so the outcome can never leave an unaccounted sequence.
	ts.pending[rec.seq] = rec
	s.recQ.push(rec)
	if err := s.mem.AppendPrepared(opCtx, id, rec.seq, int64(payload.Size()), prepared); err != nil {
		ts.mu.Unlock()
		err = fmt.Errorf("ingest: memtable admission after wal enqueue %s %d: %w", id, rec.seq, err)
		s.fail(err)
		return err
	}
	ts.mu.Unlock()
	lines, bytes := requestStats(req)
	s.m.admit(id, lines, bytes)
	// The HTTP wait unblocks on the shutdown deadline; the receipt itself
	// stays tracked so durability bookkeeping outlives the client.
	var writeDone <-chan error
	if s.walSync == wal.SyncInterval {
		written := rec.written
		if written == nil {
			// A fake WAL with no separate write receipt keeps its old
			// meaning: its wait is the only completion it can report.
			written = rec.wait
		}
		if written != nil {
			// The wait may stop as soon as append returns for any reason —
			// client cancellation, the durable receipt, or the drain
			// deadline. Canceling Receipt.Written never cancels the admitted
			// WAL work; the receipt watcher still owns its durability.
			waitCtx, cancelWait := context.WithCancel(ctx)
			defer cancelWait()
			ch := make(chan error, 1)
			writeDone = ch
			go func() { ch <- written(waitCtx) }()
		}
	}
	select {
	case <-rec.done:
		if rec.err == nil {
			// Publish only an acknowledged push: the WAL record is durable
			// and memtable admission already succeeded above. This follows
			// observed completion: if the receipt and the client's context
			// become ready together, select may choose either branch, so a
			// canceled client can still have its push tailed when durability
			// was observed concurrently. A cancellation observed before the
			// receipt completes returns below without publishing, even if the
			// WAL later makes the record durable.
			s.tail.publish(id, req)
		}
		return rec.err
	case err := <-writeDone:
		if err != nil {
			return err
		}
		// Interval acknowledgment: the WAL write and memtable admission are
		// complete. Live tail may show the entry now even though the durable
		// receipt is still pending; publication and reclaim keep waiting for
		// it, so a failed later sync still stops readiness.
		s.tail.publish(id, req)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.deadlineCh:
		return ErrUnavailable
	}
}

// enqueueErr maps a rejected enqueue: client cancellation is returned as-is,
// a closing WAL is unavailability, and anything else — a poisoned WAL, an
// oversized payload or exhausted sequences — is fatal to the service.
func (s *service) enqueueErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, wal.ErrClosed) {
		if s.accepting.Load() {
			// The WAL closing underneath an accepting service is a fatal
			// invariant break; readiness must reflect it, not a silent 503.
			s.fail(fmt.Errorf("ingest: wal closed while accepting: %w", errors.Join(errDurability, err)))
		}
		return ErrUnavailable
	}
	// errDurability rides alongside the cause so a context.Canceled leaf in
	// the failure can never be misread as a clean stop upstream.
	s.fail(fmt.Errorf("ingest: wal enqueue: %w", errors.Join(errDurability, err)))
	return err
}

// requestStats counts admitted lines and a byte estimate for metrics.
func requestStats(req *logs.PushRequest) (lines, bytes int64) {
	for _, st := range req.Streams {
		for _, l := range st.Labels {
			bytes += int64(len(l.Name) + len(l.Value))
		}
		for _, e := range st.Entries {
			lines++
			bytes += int64(len(e.Line))
			for _, l := range e.Metadata {
				bytes += int64(len(l.Name) + len(l.Value))
			}
		}
	}
	return lines, bytes
}

// watchReceipts completes every admitted receipt in arrival order, advancing
// the tenant durability watermark. A non-context wait failure is fatal: the
// WAL is poisoned and cannot accept more records.
func (s *service) watchReceipts(ctx context.Context) {
	for {
		rec, ok := s.recQ.pop(ctx)
		if !ok {
			return
		}
		err := rec.wait(ctx)
		rec.err = err
		close(rec.done)
		ts := s.state(rec.id)
		ts.mu.Lock()
		if err == nil && rec.seq > ts.durable {
			ts.durable = rec.seq
		}
		delete(ts.pending, rec.seq)
		ts.mu.Unlock()
		// Classify by the watcher's own context: a wait failure carrying a
		// context error while this watcher is still alive is a durability
		// failure (poisoned WAL), not a shutdown.
		if err != nil && ctx.Err() == nil {
			s.fail(fmt.Errorf("ingest: wal sync %s sequence %d: %w", rec.id, rec.seq, errors.Join(errDurability, err)))
		}
	}
}

// waitDurable blocks until seq is known durable for the tenant. A non-context
// failure — a poisoned WAL write or an untracked sequence — is fatal to
// callers; publish wraps it in errDurability.
func (s *service) waitDurable(ctx context.Context, id tenant.ID, seq uint64) error {
	ts := s.state(id)
	ts.mu.Lock()
	if seq <= ts.durable {
		ts.mu.Unlock()
		return nil
	}
	rec := ts.pending[seq]
	ts.mu.Unlock()
	if rec == nil {
		return fmt.Errorf("ingest: %s sequence %d has no tracked durability", id, seq)
	}
	select {
	case <-rec.done:
		return rec.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flushLoop seals and publishes generations every FlushInterval until ctx is
// done or a pass hits a fatal error.
func (s *service) flushLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.Ingest.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.flushPass(ctx); err != nil {
				s.fail(err)
				return
			}
		}
	}
}

// flushPass seals active generations, publishes every sealed generation whose
// WAL frontier is durable in order, then reclaims covered WAL segments at the
// committed manifest frontier — including passes that publish nothing, so a
// reclaim lost to an earlier crash is retried. Transient failures keep their
// generations queued for the next pass; only fatal errors are returned.
// flushPass is the periodic, retrying pass: publication failures are logged
// and retried next tick.
func (s *service) flushPass(ctx context.Context) error {
	return s.flushPassMode(ctx, false)
}

// flushPassMode seals and publishes sealed generations, then reclaims WAL at
// manifest frontiers. In strict mode — the shutdown drain — a publication
// failure is returned instead of being queued for retry, so Run reports it
// while the WAL keeps the data.
func (s *service) flushPassMode(ctx context.Context, strict bool) error {
	if err := s.mem.SealAll(ctx); err != nil {
		if strict || s.isFatal(err) || ctx.Err() == nil {
			return fmt.Errorf("ingest: seal: %w", err)
		}
		return nil
	}
	ids, err := s.flushTenants(ctx)
	if err != nil {
		if strict || s.isFatal(err) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		s.m.publishFailed()
		s.log.WarnContext(ctx, "ingest: list flush tenants; will retry", "err", err)
		return nil
	}
	for _, id := range ids {
		if err := s.flushTenant(ctx, id, strict); err != nil {
			if strict || s.isFatal(err) || ctx.Err() == nil {
				return err
			}
			return nil
		}
	}
	if err := s.reclaimPass(ctx); err != nil {
		if strict || s.isFatal(err) || ctx.Err() == nil {
			return err
		}
		return nil
	}
	return nil
}

// flushTenants returns the union of tenants with admitted state and tenants
// with durable local state.
func (s *service) flushTenants(ctx context.Context) ([]tenant.ID, error) {
	lt, err := s.local.Tenants(ctx)
	if err != nil {
		return nil, err
	}
	s.tmu.Lock()
	out := make([]tenant.ID, 0, len(lt)+len(s.tenants))
	out = append(out, lt...)
	for id := range s.tenants {
		out = append(out, id)
	}
	s.tmu.Unlock()
	slices.Sort(out)
	return slices.Compact(out), nil
}

// flushTenant publishes the tenant's sealed generations strictly in order: a
// failed generation stays queued with everything behind it, so a later
// frontier can never publish before an earlier one failed.
func (s *service) flushTenant(ctx context.Context, id tenant.ID, strict bool) error {
	gens, err := s.mem.TakeSealed(ctx, id)
	if err != nil {
		if strict || s.isFatal(err) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		s.m.publishFailed()
		s.log.WarnContext(ctx, "ingest: take sealed generations; will retry", "tenant", id, "err", err)
		return nil
	}
	pending := append(s.taken[id], gens...)
	published := 0
	defer func() {
		s.taken[id] = pending[published:]
		s.m.pending.WithLabelValues(string(id)).Set(float64(len(pending) - published))
	}()
	for ; published < len(pending); published++ {
		gen := pending[published]
		if err := s.publish(ctx, id, gen); err != nil {
			s.m.publishFailed()
			if strict || s.isFatal(err) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			first, last := gen.Frontier()
			s.log.WarnContext(ctx, "ingest: publish generation; will retry",
				"tenant", id, "first", first, "last", last, "err", err)
			return nil
		}
		pending[published] = nil // published: drop the reference
		s.m.publishOK()
	}
	return nil
}

// publish waits for the generation's WAL frontier to be durable, publishes it
// through the flusher and releases its Arrow memory.
func (s *service) publish(ctx context.Context, id tenant.ID, gen *Generation) error {
	first, last := gen.Frontier()
	if err := s.waitDurable(ctx, id, last); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("%w: generation %d-%d: %w", errDurability, first, last, err)
	}
	if err := s.flusher.Flush(ctx, gen); err != nil {
		return fmt.Errorf("ingest: publish %s generation %d-%d: %w", id, first, last, err)
	}
	gen.Release()
	return nil
}

// isFatal classifies a worker error. Fatal sentinels win even inside joined
// errors that also carry a context error; a pure cancellation is not fatal.
func (s *service) isFatal(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errDurability) || s.opts.isFatal(err)
}

// reclaimPass drops WAL segments covered by each tenant's committed manifest
// frontier. Ordinary removal and directory-sync failures retry on the next
// pass; frontier or corruption errors are fatal.
func (s *service) reclaimPass(ctx context.Context) error {
	ids, err := s.local.Tenants(ctx)
	if err != nil {
		if s.isFatal(err) {
			return err
		}
		s.log.WarnContext(ctx, "ingest: list reclaim tenants; will retry", "err", err)
		return nil
	}
	for _, id := range ids {
		if err := s.reclaimTenant(ctx, id); err != nil {
			if s.isFatal(err) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

// reclaimTenant reclaims the tenant's WAL below its manifest frontier.
func (s *service) reclaimTenant(ctx context.Context, id tenant.ID) error {
	fr, err := s.local.Frontier(ctx, id)
	if err != nil {
		if s.isFatal(err) {
			return err
		}
		s.log.WarnContext(ctx, "ingest: read manifest frontier; will retry", "tenant", id, "err", err)
		return nil
	}
	if fr.Last == 0 {
		return nil
	}
	if err := s.wal.Reclaim(ctx, id, wal.Position{Tenant: id, Seq: fr.Last}); err != nil {
		if s.isFatal(err) {
			return fmt.Errorf("ingest: wal reclaim %s at %d: %w", id, fr.Last, err)
		}
		s.log.WarnContext(ctx, "ingest: wal reclaim; will retry", "tenant", id, "frontier", fr.Last, "err", err)
	}
	return nil
}

// uploadLoop runs one uploader pass every UploadInterval until ctx is done or
// a pass reports a fatal error.
func (s *service) uploadLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.Ingest.UploadInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			report, err := s.uploader.RunOnce(ctx)
			s.m.uploaded(report, err == nil)
			// A rotation may commit before the pass fails on its Put or
			// catalog update, so the totals refresh after every pass,
			// failures included.
			if rerr := s.refreshRotations(ctx); rerr != nil {
				if err != nil {
					rerr = errors.Join(rerr, fmt.Errorf("ingest: uploader: %w", err))
				}
				s.fail(rerr)
				return
			}
			if err == nil {
				continue
			}
			if s.isFatal(err) {
				s.fail(fmt.Errorf("ingest: uploader: %w", err))
				return
			}
			if ctx.Err() != nil {
				return
			}
			s.log.WarnContext(ctx, "ingest: upload pass; will retry", "err", err)
		}
	}
}

// refreshRotations snapshots every tenant's cumulative upload-key rotation
// count from its committed manifest into the rotations metric. The metric
// keeps the last observed totals and adds only positive deltas, so repeated
// snapshots never double-count. Ordinary read failures are logged and picked
// up by the next refresh; fatal manifest errors are returned so the caller
// stops the service. No disk I/O happens at scrape time.
func (s *service) refreshRotations(ctx context.Context) error {
	ids, err := s.local.Tenants(ctx)
	if err != nil {
		if s.isFatal(err) {
			return fmt.Errorf("ingest: rotation tenants: %w", err)
		}
		s.log.WarnContext(ctx, "ingest: list rotation tenants; will retry", "err", err)
		return nil
	}
	for _, id := range ids {
		ts, err := s.local.Tenant(ctx, id)
		if err != nil {
			if s.isFatal(err) {
				return fmt.Errorf("ingest: rotation tenant %s: %w", id, err)
			}
			s.log.WarnContext(ctx, "ingest: open rotation tenant; will retry", "tenant", id, "err", err)
			return nil
		}
		total, err := ts.KeyRotations(ctx)
		if err != nil {
			if s.isFatal(err) {
				return fmt.Errorf("ingest: read rotations %s: %w", id, err)
			}
			s.log.WarnContext(ctx, "ingest: read rotations; will retry", "tenant", id, "err", err)
			return nil
		}
		s.m.observeRotations(id, total)
	}
	return nil
}

// drain stops admission, waits boundedly for in-flight pushes, joins the
// workers, then seals and flushes under a detached context bounded by the
// shutdown timeout and closes every resource Run opened. Errors are joined;
// the caller returns nil only when this returns nil.
func (s *service) drain(ctx context.Context, cancelWorkers context.CancelFunc, wg *sync.WaitGroup) error {
	s.stopAccepting()
	cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.shutdownTimeout)
	defer cancel()
	// The deadline unblocks HTTP receipt waits and the durability watcher;
	// tracked receipts keep whatever outcome the WAL already reached.
	stopDeadline := context.AfterFunc(cleanCtx, func() {
		s.deadlineOnce.Do(func() { close(s.deadlineCh) })
		if s.watchCancel != nil {
			s.watchCancel()
		}
	})
	defer stopDeadline()
	var errs []error
	gateDrained := true
	if err := s.gate.wait(cleanCtx); err != nil {
		errs = append(errs, fmt.Errorf("ingest: drain admissions: %w", err))
		gateDrained = false
	}
	cancelWorkers()
	s.recQ.close()
	clean := gateDrained
	if clean && s.walSync == wal.SyncInterval {
		// Force dirty WAL data durable before joining the receipt watcher:
		// the sync timer can be longer than the whole shutdown budget, and
		// the watcher waits for these receipts' fsync. The WAL stays open so
		// the final flush's reclaim still runs against it.
		if err := s.wal.Sync(cleanCtx); err != nil {
			errs = append(errs, fmt.Errorf("ingest: force wal sync: %w", err))
			clean = false
		}
	}
	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-cleanCtx.Done():
		errs = append(errs, fmt.Errorf("ingest: worker join: %w", cleanCtx.Err()))
		clean = false
	}
	if clean {
		// The final pass seals every active generation and publishes all
		// whose WAL frontier is durable; a publication failure is returned
		// while the WAL keeps the data. The pass runs bounded by the
		// remaining deadline: a flusher stuck in an uninterruptible sync
		// must not hold Run open past the shutdown deadline.
		flushDone := make(chan error, 1)
		go func() { flushDone <- s.flushPassMode(cleanCtx, true) }()
		select {
		case err := <-flushDone:
			errs = append(errs, s.finalizeFlush(err)...)
		case <-cleanCtx.Done():
			// A pass that completed as the deadline fired still counts.
			select {
			case err := <-flushDone:
				errs = append(errs, s.finalizeFlush(err)...)
			default:
				errs = append(errs, fmt.Errorf("ingest: final flush: %w", cleanCtx.Err()))
				// The stuck pass still owns the memtable, taken generations
				// and the local store; the reaper frees them once it exits.
				go s.reapFinalFlush(flushDone)
			}
		}
	} else {
		// Outstanding admissions or workers still own memtable, taken
		// generations and the local store: never release resources out from
		// under them. The reaper finishes cleanup once they actually exit.
		go s.reap(joined)
	}
	if err := s.wal.Close(cleanCtx); err != nil {
		errs = append(errs, fmt.Errorf("ingest: wal close: %w", err))
	}
	return errors.Join(errs...)
}

// reap finishes resource cleanup once every admission has left the gate and
// every worker has exited, after a drain that outlived its deadline.
func (s *service) reap(joined <-chan struct{}) {
	s.gate.wait(context.Background())
	<-joined
	s.releaseTaken()
	s.mem.Close()
	if err := s.local.Close(); err != nil {
		s.log.WarnContext(context.Background(), "ingest: local store close after drain deadline", "err", err)
	}
}

// finalizeFlush records the final flush pass outcome and releases the memtable,
// taken generations and local store it owned. It runs exactly once, on the
// goroutine that observed the pass complete.
func (s *service) finalizeFlush(err error) []error {
	var errs []error
	if err != nil {
		errs = append(errs, fmt.Errorf("ingest: final flush: %w", err))
	}
	s.releaseTaken()
	s.mem.Close()
	if err := s.local.Close(); err != nil {
		errs = append(errs, fmt.Errorf("ingest: local store close: %w", err))
	}
	return errs
}

// reapFinalFlush frees the memtable, taken generations and local store once a
// final flush pass that outlived the shutdown deadline actually exits. The
// stuck pass owns those resources until then; nothing is freed concurrently.
func (s *service) reapFinalFlush(done <-chan error) {
	if errs := s.finalizeFlush(<-done); len(errs) > 0 {
		s.log.WarnContext(context.Background(), "ingest: delayed final flush cleanup", "err", errors.Join(errs...))
	}
}

// releaseTaken frees every sealed generation still held for retry, for drain
// or fatal teardown.
func (s *service) releaseTaken() {
	for id, gens := range s.taken {
		for _, g := range gens {
			g.Release()
		}
		delete(s.taken, id)
	}
}
