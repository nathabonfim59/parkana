package compactor

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// backfillSkipCapacity bounds how many over-cap keys the service remembers,
// keeping the skip set's memory proportional to a constant rather than the
// number of over-cap files. It is a deliberately generous bound: an entry
// that falls out of it only costs one rebuilt (and re-skipped) sidecar on a
// later cycle.
const backfillSkipCapacity = 4096

// skipSet remembers the keys of files whose sidecar build exceeded the gram
// cap, so backfill does not re-read the same over-cap file every cycle. It is
// a least-recently-used cache: Contains refreshes recency and Add into a full
// set evicts the least recently used key, so an evicted key is retried on a
// later cycle. That retry is safe because object keys are immutable: the
// rebuild either finds the same over-cap file again or finds the row gone,
// and eviction itself cannot lose data, only repeat one build.
//
// Keys are full object keys, which embed the tenant prefix, so entries can
// never suppress another tenant's file. The set lives on the service only:
// the catalog contract is frozen, so there is no persistent skip state and
// the first pass after a restart re-attempts every over-cap file, paying one
// full read each. Safe for concurrent use.
type skipSet struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List // front is the most recently used key
	elems map[string]*list.Element
}

// newSkipSet returns an empty skipSet of the given capacity. A capacity of
// zero or less remembers nothing.
func newSkipSet(capacity int) *skipSet {
	return &skipSet{
		cap:   capacity,
		ll:    list.New(),
		elems: make(map[string]*list.Element, max(capacity, 0)),
	}
}

// Add records key, or refreshes it when already present, evicting the least
// recently used key when the set is full.
func (s *skipSet) Add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cap <= 0 {
		return
	}
	if el, ok := s.elems[key]; ok {
		s.ll.MoveToFront(el)
		return
	}
	s.elems[key] = s.ll.PushFront(key)
	for s.ll.Len() > s.cap {
		oldest := s.ll.Back()
		delete(s.elems, oldest.Value.(string))
		s.ll.Remove(oldest)
	}
}

// Contains reports whether key was remembered, refreshing its recency.
func (s *skipSet) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.elems[key]
	if !ok {
		return false
	}
	s.ll.MoveToFront(el)
	return true
}

// Len returns the number of remembered keys.
func (s *skipSet) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

// backfillOutcome reports what one backfill visit did.
type backfillOutcome int

const (
	// backfillNoop means the file needed no work: it already has a sidecar
	// flag, its key is malformed or foreign, or its key is remembered in the
	// over-cap skip set.
	backfillNoop backfillOutcome = iota
	// backfillBuilt means the sidecar was built, put and flagged.
	backfillBuilt
	// backfillOverCap means the file exceeded the gram cap: no sidecar, no
	// flag, the skip metric observed and the key remembered.
	backfillOverCap
)

// backfillTenant builds and publishes search sidecars for every live file of
// tenant t that its catalog row lists without one: files uploaded before
// sidecars existed, files published over the gram cap and files whose sidecar
// flag update failed. Files above the merge target and single-file streams,
// which no merge ever visits, are included.
//
// The pass is serial and per-file failures are isolated: an unreadable object
// or a failed sidecar Put is collected and the next file is still processed,
// so one bad file cannot starve the rest or block the next cycle's cleanup
// and merges. Every collected failure is preserved in the returned join, even
// when a later listing error or cancellation ends the pass early. Missing or
// deleted catalog rows during SetSidecar are benign concurrent changes (the
// file was replaced or purged between listing and the flag update): the
// sidecar object may be orphaned, which a later cleanup pass reclaims.
// Over-cap builds record the key in the service's bounded skip set and leave
// the row unflagged.
func (s *service) backfillTenant(ctx context.Context, t tenant.ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var (
		errs    []error
		built   int
		overCap int
	)
	for f, err := range s.cat.ListFiles(ctx, catalog.Filter{Tenant: t}) {
		if err != nil {
			// The listing failed; work already done is still reported, and
			// the pass stops because the iteration is unreliable from here.
			errs = append(errs, fmt.Errorf("compactor: backfill %s: list files: %w", t, err))
			break
		}
		// The iterator may end normally after the context was canceled; never
		// touch the store or catalog past that point.
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		outcome, err := s.backfillFile(ctx, t, f)
		if err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
			continue
		}
		switch outcome {
		case backfillBuilt:
			built++
		case backfillOverCap:
			overCap++
		}
	}
	// A canceled context is reported even when the iterator ended cleanly
	// without yielding an error, unless it is already part of the result.
	if err := ctx.Err(); err != nil && !slices.ContainsFunc(errs, func(e error) bool {
		return errors.Is(e, err)
	}) {
		errs = append(errs, err)
	}
	if built > 0 || overCap > 0 {
		s.log.Info("compactor backfill pass",
			"tenant", t, "built", built, "over_cap", overCap, "failed", len(errs))
	}
	return errors.Join(errs...)
}

// backfillFile backfills one live catalog row, returning what it did. The
// caller owns the row's freshness: a row replaced while the sidecar was being
// built turns SetSidecar into catalog.ErrNotFound, which is a benign race.
func (s *service) backfillFile(ctx context.Context, t tenant.ID, f catalog.File) (backfillOutcome, error) {
	if f.HasSidecar {
		return backfillNoop, nil
	}
	// Validate the key against the row's tenant and stream before touching
	// anything: a malformed or foreign key must never be read, written or
	// flagged, whatever the catalog claims.
	if _, err := keyHour(t, f.Stream, f.Key); err != nil {
		s.log.Warn("compactor backfill skipping file with malformed key",
			"tenant", t, "stream", f.Stream, "key", f.Key, "err", err)
		return backfillNoop, nil
	}
	if s.skips.Contains(f.Key) {
		return backfillNoop, nil
	}

	has, err := s.buildSidecar(ctx, t, f.Key, index.SourceBackfill)
	if err != nil {
		return backfillNoop, err
	}
	if !has {
		return backfillOverCap, nil
	}
	if err := s.cat.SetSidecar(ctx, t, f.Key); err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			// The row was replaced or purged between listing and the flag
			// update. The sidecar object stays for cleanup to reclaim; the
			// file is gone, so there is nothing left to flag.
			s.log.Debug("compactor backfill race: file no longer listed",
				"tenant", t, "key", f.Key)
			return backfillNoop, nil
		}
		return backfillNoop, fmt.Errorf("compactor: backfill %s: set sidecar flag: %w", f.Key, err)
	}
	return backfillBuilt, nil
}
