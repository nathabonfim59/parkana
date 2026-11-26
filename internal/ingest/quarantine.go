package ingest

// This file owns quarantine retention: the fresh mtime a file gets when it is
// moved into parquet/<tenant>/.quarantine/, the per-tenant sweep that deletes
// regular files older than -ingest.quarantine-retention, the cached usage
// behind the quarantine gauges, and the service's sweep schedule.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// quarantineSweepInterval is the shortest time between two scheduled
// quarantine sweeps from the upload loop. The first sweep runs right after
// recovery.
const quarantineSweepInterval = time.Hour

// QuarantineUsage is what a tenant's .quarantine/ directory holds: the
// regular files directly inside it and their bytes. Directories, symlinks
// and other entries are not counted (the sweep never removes them either).
type QuarantineUsage struct {
	Files int64
	Bytes int64
}

// SweepResult reports one tenant's quarantine sweep.
type SweepResult struct {
	Deleted int64           // regular files removed
	Usage   QuarantineUsage // what is left after the sweep
}

// freshenQuarantinedLocked gives a file just renamed into .quarantine/ a
// fresh mtime, so retention counts from the quarantine, not from the file's
// last write (a rename keeps the old mtime), and fsyncs the file so the new
// mtime survives a crash. Only regular files are touched: a symlink is never
// followed. It returns what the entry adds to the quarantine usage. The
// caller holds t.mu.
func (t *TenantStore) freshenQuarantinedLocked(dst string) (QuarantineUsage, error) {
	info, err := t.root.Lstat(dst)
	if err != nil {
		return QuarantineUsage{}, fmt.Errorf("ingest: stat quarantined %s: %w", dst, err)
	}
	if !info.Mode().IsRegular() {
		return QuarantineUsage{}, nil
	}
	now := t.ops.now()
	if err := t.root.Chtimes(dst, now, now); err != nil {
		return QuarantineUsage{}, fmt.Errorf("ingest: touch quarantined %s: %w", dst, err)
	}
	f, err := t.root.OpenFile(dst, os.O_RDONLY, 0)
	if err != nil {
		return QuarantineUsage{}, fmt.Errorf("ingest: open quarantined %s: %w", dst, err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return QuarantineUsage{}, fmt.Errorf("ingest: sync quarantined %s: %w", dst, err)
	}
	return QuarantineUsage{Files: 1, Bytes: info.Size()}, nil
}

// QuarantineUsage returns the tenant's cached quarantine usage: scanned at
// recovery and by every sweep, and raised by every quarantine in between. It
// does no I/O.
func (t *TenantStore) QuarantineUsage() QuarantineUsage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.quarantine
}

// SweepQuarantine deletes the regular files directly under .quarantine/
// whose mtime is older than now-retention, then fsyncs the directory. A
// retention of zero or less deletes nothing and only rescans the usage.
// Entries that are not regular files (directories, symlinks, devices) are
// skipped and never followed: every call goes through the tenant's os.Root
// and Lstat. A missing .quarantine/ is empty. Per-entry failures do not stop
// the sweep; they are joined into the returned error, and the result still
// reports what was deleted and what is left.
func (t *TenantStore) SweepQuarantine(ctx context.Context, retention time.Duration, now time.Time) (SweepResult, error) {
	if err := ctx.Err(); err != nil {
		return SweepResult{}, fmt.Errorf("ingest: sweep quarantine %s: %w", t.id, err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed.Load() {
		return SweepResult{}, ErrLocalClosed
	}
	res, err := t.sweepQuarantineLocked(ctx, retention, now)
	t.quarantine = res.Usage
	return res, err
}

// scanQuarantineLocked refreshes the cached usage without deleting anything.
func (t *TenantStore) scanQuarantineLocked(ctx context.Context) error {
	res, err := t.sweepQuarantineLocked(ctx, 0, time.Time{})
	t.quarantine = res.Usage
	return err
}

func (t *TenantStore) sweepQuarantineLocked(ctx context.Context, retention time.Duration, now time.Time) (SweepResult, error) {
	var res SweepResult
	entries, err := fs.ReadDir(t.root.FS(), quarantineDir)
	if errors.Is(err, fs.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("ingest: list %s %s: %w", t.id, quarantineDir, err)
	}
	cutoff := now.Add(-retention)
	var errs []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if !entry.Type().IsRegular() {
			continue
		}
		rel := quarantineDir + "/" + entry.Name()
		info, err := t.root.Lstat(rel)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("ingest: stat %s %s: %w", t.id, rel, err))
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if retention > 0 && info.ModTime().Before(cutoff) {
			// Root.Remove unlinks the name itself; it never follows a
			// symlink swapped in after the Lstat.
			if err := t.ops.remove(t.root, rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("ingest: delete %s %s: %w", t.id, rel, err))
				res.Usage.Files++
				res.Usage.Bytes += info.Size()
				continue
			}
			res.Deleted++
			continue
		}
		res.Usage.Files++
		res.Usage.Bytes += info.Size()
	}
	if res.Deleted > 0 {
		if err := t.ops.syncDir(t.root, quarantineDir); err != nil {
			errs = append(errs, fmt.Errorf("ingest: sync %s %s: %w", t.id, quarantineDir, err))
		}
	}
	return res, errors.Join(errs...)
}

// sweepQuarantine sweeps every tenant with the configured retention and
// refreshes the quarantine gauges. Failures are logged and counted, never
// fatal: a full or broken quarantine must not stop ingestion.
func (s *service) sweepQuarantine(ctx context.Context) {
	now := s.opts.now()
	s.lastQuarantineSweep = now
	ids, err := s.local.Tenants(ctx)
	if err != nil {
		s.m.quarantineSweepFailed()
		s.log.WarnContext(ctx, "ingest: quarantine sweep: list tenants", "err", err)
		return
	}
	for _, id := range ids {
		ts, err := s.local.Tenant(ctx, id)
		if err != nil {
			s.m.quarantineSweepFailed()
			s.log.WarnContext(ctx, "ingest: quarantine sweep: open tenant", "tenant", id, "err", err)
			continue
		}
		res, err := ts.SweepQuarantine(ctx, s.cfg.Ingest.QuarantineRetention, now)
		if err != nil {
			s.m.quarantineSweepFailed()
			s.log.WarnContext(ctx, "ingest: quarantine sweep", "tenant", id, "err", err)
		}
		if res.Deleted > 0 {
			s.log.InfoContext(ctx, "ingest: deleted expired quarantined files", "tenant", id,
				"files", res.Deleted, "retention", s.cfg.Ingest.QuarantineRetention)
		}
		s.m.observeQuarantine(id, res.Usage)
	}
}

// quarantineTick runs after every upload pass: it sweeps when the last sweep
// is at least quarantineSweepInterval old, and otherwise only refreshes the
// gauges from the cached usage, which picks up files an aborted upload temp
// moved into quarantine during the pass.
func (s *service) quarantineTick(ctx context.Context) {
	if s.opts.now().Sub(s.lastQuarantineSweep) >= quarantineSweepInterval {
		s.sweepQuarantine(ctx)
		return
	}
	s.refreshQuarantineGauges(ctx)
}

// refreshQuarantineGauges copies every tenant's cached usage into the gauges.
func (s *service) refreshQuarantineGauges(ctx context.Context) {
	ids, err := s.local.Tenants(ctx)
	if err != nil {
		return
	}
	for _, id := range ids {
		ts, err := s.local.Tenant(ctx, id)
		if err != nil {
			continue
		}
		s.m.observeQuarantine(id, ts.QuarantineUsage())
	}
}

// observeQuarantine sets one tenant's quarantine gauges.
func (m *metrics) observeQuarantine(id tenant.ID, u QuarantineUsage) {
	m.quarantineFiles.WithLabelValues(string(id)).Set(float64(u.Files))
	m.quarantineBytes.WithLabelValues(string(id)).Set(float64(u.Bytes))
}

// quarantineSweepFailed counts one failed tenant sweep or sweep step.
func (m *metrics) quarantineSweepFailed() { m.quarantineErrs.Inc() }
