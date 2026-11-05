// Package wal provides a tenant-scoped, versioned write-ahead log with
// checksummed records, crash-safe replay, and per-tenant group commit.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	defaultMaxRecordBytes  = 64 << 20
	defaultSegmentMaxBytes = 128 << 20
)

var (
	// ErrClosed reports that the WAL or one of its tenant logs is closing.
	ErrClosed = errors.New("wal: closed")
	// ErrCorrupt reports invalid or inconsistent on-disk WAL data.
	ErrCorrupt = errors.New("wal: corrupt data")
	// ErrUnsupportedVersion reports a WAL or payload version this build cannot read.
	ErrUnsupportedVersion = errors.New("wal: unsupported version")
	// ErrInvalidFrontier reports a replay or reclaim frontier that would skip
	// unavailable records or advance beyond durable data.
	ErrInvalidFrontier = errors.New("wal: invalid manifest frontier")
	// ErrTenantMismatch reports a payload or position belonging to another tenant.
	ErrTenantMismatch = errors.New("wal: tenant mismatch")
)

// Config bounds WAL record and segment sizes and selects the sync mode. Zero
// values select defaults.
type Config struct {
	// MaxRecordBytes is the largest encoded push payload accepted or read.
	MaxRecordBytes int
	// SegmentMaxBytes is the soft segment size at which the writer rotates.
	SegmentMaxBytes int64
	// SyncMode selects when appended records are fsynced. The zero value
	// SyncAlways keeps the phase 3 guarantee that Receipt.Wait returns only
	// after the batch's fsync completed.
	SyncMode SyncMode
	// SyncInterval is the timer period used in SyncInterval mode. Zero selects
	// 1s; it is ignored in SyncAlways mode.
	SyncInterval time.Duration
	// OnSync, when set, is invoked by the caller of each Sync the WAL issues on
	// a segment file — batch commit, segment header initialization, seal,
	// timer and close syncs — so both tenant worker goroutines (appends) and
	// the Open caller (recovery) may invoke it. Directory syncs and
	// torn-segment repair syncs are not reported. It is called outside tenant
	// locks and may be called concurrently, so it must be safe for concurrent
	// use.
	OnSync func(time.Duration, error)
}

func (c Config) withDefaults() (Config, error) {
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = defaultMaxRecordBytes
	}
	if c.SegmentMaxBytes == 0 {
		c.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if c.MaxRecordBytes < 1 || int64(c.MaxRecordBytes) > int64(^uint32(0)) {
		return Config{}, fmt.Errorf("wal: max record bytes must be between 1 and %d", uint64(^uint32(0)))
	}
	if c.SegmentMaxBytes < segmentHeaderSize(1) {
		return Config{}, fmt.Errorf("wal: segment max bytes must be at least %d", segmentHeaderSize(1))
	}
	if c.SyncMode != SyncAlways && c.SyncMode != SyncInterval {
		return Config{}, fmt.Errorf("wal: invalid sync mode %d", c.SyncMode)
	}
	if c.SyncInterval < 0 {
		return Config{}, fmt.Errorf("wal: sync interval must not be negative")
	}
	if c.SyncMode == SyncInterval && c.SyncInterval == 0 {
		c.SyncInterval = defaultSyncInterval
	}
	return c, nil
}

// Position identifies a tenant sequence boundary. Sequence numbers start at 1.
// The zero value means no replay frontier for the tenant supplied to an operation.
type Position struct {
	Tenant tenant.ID
	Seq    uint64
}

// Payload is an immutable, internally versioned encoded push request. Build it
// with Encode before taking the caller's tenant ingestion lock.
type Payload struct {
	tenant tenant.ID
	data   []byte
}

// Size returns the encoded payload size in bytes. It excludes segment and
// frame overhead, so callers should treat it as an approximation when bounding
// per-tenant generation WAL bytes.
func (p Payload) Size() int {
	return len(p.data)
}

// Receipt is an in-memory queue admission and its assigned position. Written
// waits for the record's frames to reach the segment file; Wait waits for the
// record's durability, which in SyncInterval mode happens at a later timer,
// rotation or close sync.
type Receipt struct {
	Position Position
	waiter   *receiptWaiter
}

// Record is one replayed push request and its tenant-scoped sequence position.
type Record struct {
	Position Position
	Request  logs.PushRequest
}

// WAL is a rooted collection of independent per-tenant append-only logs.
type WAL struct {
	root *os.Root
	cfg  Config
	ops  fileOps

	mu         sync.Mutex
	logs       map[tenant.ID]*tenantLog
	closing    bool
	wg         sync.WaitGroup
	operations sync.WaitGroup

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// Open opens or creates dataDir/wal, recovers every discovered tenant, and
// repairs only a torn tail in the newest active segment. All filesystem access
// beneath dataDir is rooted.
func Open(ctx context.Context, dataDir string, cfg Config) (*WAL, error) {
	return open(ctx, dataDir, cfg, defaultFileOps())
}

func open(ctx context.Context, dataDir string, cfg Config, ops fileOps) (*WAL, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := objstore.MkdirAllDurable(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("wal: create data directory %s: %w", dataDir, err)
	}
	base, err := os.OpenRoot(dataDir)
	if err != nil {
		return nil, fmt.Errorf("wal: open data directory %s: %w", dataDir, err)
	}
	defer base.Close()
	if err := base.MkdirAll("wal", 0o750); err != nil {
		return nil, fmt.Errorf("wal: create wal directory: %w", err)
	}
	if err := ops.syncDir(base); err != nil {
		return nil, fmt.Errorf("wal: sync data directory: %w", err)
	}
	root, err := base.OpenRoot("wal")
	if err != nil {
		return nil, fmt.Errorf("wal: open wal root: %w", err)
	}
	// Persist tenant directory entries created before a crashed or failed
	// parent sync before recovery may acknowledge their contents.
	if err := ops.syncDir(root); err != nil {
		root.Close()
		return nil, fmt.Errorf("wal: sync wal directory: %w", err)
	}
	w := &WAL{
		root:      root,
		cfg:       cfg,
		ops:       ops,
		logs:      make(map[tenant.ID]*tenantLog),
		closeDone: make(chan struct{}),
	}
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		root.Close()
		return nil, fmt.Errorf("wal: list tenants: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			w.closeRecoveredRoots()
			root.Close()
			return nil, fmt.Errorf("wal: open: %w", err)
		}
		if !entry.IsDir() {
			w.closeRecoveredRoots()
			root.Close()
			return nil, fmt.Errorf("wal: unexpected file %q in wal root", entry.Name())
		}
		id := tenant.ID(entry.Name())
		if err := id.Validate(); err != nil {
			w.closeRecoveredRoots()
			root.Close()
			return nil, fmt.Errorf("wal: discovered tenant: %w", err)
		}
		log, err := w.recoverTenant(ctx, id)
		if err != nil {
			w.closeRecoveredRoots()
			root.Close()
			return nil, err
		}
		w.logs[id] = log
	}
	for _, log := range w.logs {
		w.wg.Go(func() { log.run(w) })
	}
	return w, nil
}

func (w *WAL) closeRecoveredRoots() {
	for _, log := range w.logs {
		if log.root != nil {
			log.root.Close()
		}
	}
}

// Tenants returns the sorted union of discovered and active in-memory tenant IDs.
func (w *WAL) Tenants(ctx context.Context) ([]tenant.ID, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wal: tenants: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Sorted(maps.Keys(w.logs)), nil
}

// Enqueue validates and queues payload without disk I/O or an fsync wait. A
// receipt is returned only after in-memory queue admission and sequence assignment.
func (w *WAL) Enqueue(ctx context.Context, id tenant.ID, payload Payload) (Receipt, error) {
	if err := ctx.Err(); err != nil {
		return Receipt{}, fmt.Errorf("wal: enqueue %s: %w", id, err)
	}
	if err := id.Validate(); err != nil {
		return Receipt{}, fmt.Errorf("wal: enqueue: %w", err)
	}
	if len(payload.data) == 0 || len(payload.data) > w.cfg.MaxRecordBytes {
		return Receipt{}, fmt.Errorf("wal: enqueue %s: payload size %d outside 1..%d", id, len(payload.data), w.cfg.MaxRecordBytes)
	}
	if payload.tenant != id {
		return Receipt{}, fmt.Errorf("wal: enqueue %s with payload for %s: %w", id, payload.tenant, ErrTenantMismatch)
	}
	log, err := w.logForAppend(id)
	if err != nil {
		return Receipt{}, err
	}
	return log.enqueue(ctx, payload)
}

// Sync is a force barrier: it returns once every record admitted to any
// tenant before this call is durable, without closing the WAL. Each tenant's
// barrier is processed by that tenant's worker, serialized with its writes,
// so a blocked tenant never prevents another tenant's barrier from running;
// all barriers are enqueued before any is waited for. A barrier fsync is
// reported to Config.OnSync like any other and is skipped when the tenant has
// nothing dirty — in SyncAlways mode a quiesced Sync issues no fsync at all.
// A barrier sync failure poisons the tenant like a timer failure and is
// returned joined with the other tenants' outcomes. Canceling ctx stops only
// the caller's wait: every admitted barrier still completes on its worker.
// Sync returns ErrClosed once Close has begun.
func (w *WAL) Sync(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("wal: sync: %w", ErrClosed)
	}
	logs := slices.Collect(maps.Values(w.logs))
	w.mu.Unlock()

	var errs []error
	barriers := make([]*appendRequest, 0, len(logs))
	for _, log := range logs {
		barrier, err := log.enqueueBarrier()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		barriers = append(barriers, barrier)
	}
	for _, barrier := range barriers {
		select {
		case <-barrier.waiter.done:
			if err := barrier.waiter.err; err != nil {
				errs = append(errs, fmt.Errorf("wal: sync %s: %w", barrier.position.Tenant, err))
			}
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("wal: sync %s: %w", barrier.position.Tenant, ctx.Err()))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

func (w *WAL) logForAppend(id tenant.ID) (*tenantLog, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		return nil, fmt.Errorf("wal: enqueue %s: %w", id, ErrClosed)
	}
	if log := w.logs[id]; log != nil {
		return log, nil
	}
	log := newTenantLog(id, nil, 1)
	w.logs[id] = log
	w.wg.Go(func() { log.run(w) })
	return log, nil
}

func (w *WAL) existingLog(id tenant.ID) (*tenantLog, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		return nil, fmt.Errorf("wal: tenant %s: %w", id, ErrClosed)
	}
	if log := w.logs[id]; log != nil {
		return log, nil
	}
	log := newTenantLog(id, nil, 1)
	w.logs[id] = log
	w.wg.Go(func() { log.run(w) })
	return log, nil
}

// Close stops new appends and drains accepted work. In SyncInterval mode every
// tenant worker forces one final sync of written-but-unsynced data before its
// active segment closes; a failed final sync completes the affected receipts
// with the error and is reported in Close's result. If ctx expires, Close may
// return while draining continues; the close goroutine and workers still finish
// once their underlying file operations return.
func (w *WAL) Close(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closing = true
		logs := slices.Collect(maps.Values(w.logs))
		w.mu.Unlock()
		for _, log := range logs {
			log.beginClose()
		}
		go func() {
			w.wg.Wait()
			w.operations.Wait()
			var errs []error
			for _, log := range logs {
				if log.closeErr != nil {
					errs = append(errs, fmt.Errorf("wal: close tenant %s segment: %w", log.id, log.closeErr))
				}
				if log.root != nil {
					if err := log.root.Close(); err != nil {
						errs = append(errs, fmt.Errorf("wal: close tenant %s root: %w", log.id, err))
					}
				}
			}
			if err := w.root.Close(); err != nil {
				errs = append(errs, fmt.Errorf("wal: close root: %w", err))
			}
			w.closeErr = errors.Join(errs...)
			close(w.closeDone)
		}()
	})
	select {
	case <-w.closeDone:
		return w.closeErr
	case <-ctx.Done():
		return fmt.Errorf("wal: close: %w", ctx.Err())
	}
}

func validatePosition(id tenant.ID, pos Position) error {
	if pos.Tenant != "" && pos.Tenant != id {
		return fmt.Errorf("wal: position for %s used with %s: %w", pos.Tenant, id, ErrTenantMismatch)
	}
	if pos.Seq == 0 {
		return nil
	}
	if pos.Tenant == "" {
		return fmt.Errorf("wal: nonzero sequence without tenant: %w", ErrInvalidFrontier)
	}
	return nil
}

func syncRoot(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}
