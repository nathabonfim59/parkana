package query

import (
	"cmp"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// maxSidecarInput bounds the serialized bytes one fetch may read from the
// store. Sidecar writers stop at index.Params.MaxGrams, which keeps a sidecar
// at a few MB (2M grams at 1% FP is about 2.4 MB), so anything near this bound
// is corruption or misconfiguration; refusing it keeps a bad object from
// exhausting querier memory. Tests shrink it through sidecarCache.maxInput.
const maxSidecarInput = 64 << 20

// sidecarWireOverhead is the non-bloom bytes of a sidecar: the 8-byte header
// (magic, version, gram length, reserved), bloom's m and k words (8 bytes
// each), the bitset length word (8) and the trailing CRC32. The decoded
// bitset is exactly the wire bloom words, so a valid sidecar's decoded bloom
// bytes are len(data) - sidecarWireOverhead.
const sidecarWireOverhead = 8 + 8 + 8 + 8 + 4

// sidecarEntryOverhead is a fixed allowance for the pointers, map slot, list
// element and filter structs an admitted entry keeps alive, so the resident
// budget tracks real memory rather than only the bloom words.
const sidecarEntryOverhead = 256

// errSidecarCacheClosed is returned by Get after Close.
var errSidecarCacheClosed = errors.New("query: sidecar cache is closed")

// sidecarCacheOptions configures a sidecarCache.
type sidecarCacheOptions struct {
	store    objstore.Store
	budget   int64 // resident decoded-byte budget, must be positive
	logger   *slog.Logger
	register prometheus.Registerer
}

// sidecarCache holds decoded search sidecars (index.Filter) in memory so a
// query that prunes many files with the same needles decodes each sidecar at
// most once. Entries are keyed by the immutable data-object key, which
// embeds the tenant, and the sidecar object is always derived through
// objstore.SidecarKey. Admission and eviction track accounted decoded bytes
// (bloom words plus entry overhead) against the budget; a sidecar too large
// for the budget is still served to its caller, just not admitted, because
// pruning with it once is correct and refetching beats an unbounded resident
// set. Errors are never cached: a sidecar repaired after a transient read or
// decode error is picked up by the next Get.
//
// Concurrent Gets for the same key share one fetch. A waiter that gives up
// (its context ends) never cancels the shared fetch for the others; the last
// waiter to leave and Close do. The cache lock is never held during store
// I/O, decoding or filter evaluation.
type sidecarCache struct {
	store   objstore.Store
	budget  int64
	log     *slog.Logger
	metrics *sidecarMetrics

	// maxInput overrides maxSidecarInput when positive; tests use it to
	// exercise the read bound without a 64 MB object.
	maxInput int64

	mu       sync.Mutex
	closing  chan struct{} // non-nil once Close began
	entries  map[string]*sidecarEntry
	lru      *list.List // of *sidecarEntry, most recently used first
	used     int64      // accounted decoded bytes of resident entries
	inflight map[string]*sidecarFetch
	wg       sync.WaitGroup

	// afterWaiter, when non-nil, is called after a Get registers on an
	// in-flight fetch (not when it starts one). Tests use it as a barrier
	// instead of sleeping.
	afterWaiter func()

	// teardownOnce waits for the fetch goroutines and unregisters the
	// metrics exactly once, so concurrent Close callers all return only
	// after teardown finished. It is a sync.OnceFunc built at construction.
	teardownOnce func()
}

// sidecarEntry is one admitted filter. Filters are immutable and safe for
// concurrent use, so entries carry no lease count: eviction drops the map
// reference while callers still holding the filter keep using it.
type sidecarEntry struct {
	key    string // data-object key, tenant-prefixed
	filter *index.Filter
	size   int64 // accounted decoded bytes
	elem   *list.Element
}

// sidecarFetch is one in-flight fetch shared by every caller that wants key.
// The fetch runs on a context of its own: a caller that gives up cannot cancel
// a fetch another caller is waiting for. waiters counts the callers still
// interested; the last one to leave cancels the fetch.
type sidecarFetch struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	finished bool // guarded by sidecarCache.mu
	filter   *index.Filter
	err      error
	waiters  int // guarded by sidecarCache.mu
}

// newSidecarCache builds a decoded-sidecar cache over opts.store. A failed
// construction leaves no metrics registered.
func newSidecarCache(opts sidecarCacheOptions) (*sidecarCache, error) {
	if opts.store == nil {
		return nil, errors.New("query: sidecar cache: store is required")
	}
	if opts.budget <= 0 {
		return nil, fmt.Errorf("query: sidecar cache: budget must be positive, got %d", opts.budget)
	}
	metrics, err := newSidecarMetrics(opts.register)
	if err != nil {
		return nil, err
	}
	c := &sidecarCache{
		store:    opts.store,
		budget:   opts.budget,
		log:      cmpLogger(opts.logger),
		metrics:  metrics,
		entries:  make(map[string]*sidecarEntry),
		lru:      list.New(),
		inflight: make(map[string]*sidecarFetch),
	}
	c.teardownOnce = sync.OnceFunc(func() {
		c.wg.Wait()
		c.metrics.unregister()
	})
	return c, nil
}

// Get returns the decoded sidecar filter for the tenant's data object at
// dataKey, fetching and decoding it at most once per concurrent caller group.
// The tenant must own dataKey (which must end in .parquet); validation happens
// before any I/O. Fetch and decode errors fail open: they are returned to the
// caller, which must keep the file scannable.
func (c *sidecarCache) Get(ctx context.Context, t tenant.ID, dataKey string) (*index.Filter, error) {
	if err := validateSidecarGet(t, dataKey); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A healthy caller may arrive just as the last waiter of a fetch gives up.
	// The shared fetch then fails with context.Canceled even though this
	// caller's own context is fine; retry with a fresh fetch instead of
	// returning another caller's cancellation.
	const maxAttempts = 3
	counted := false
	for range maxAttempts {
		c.mu.Lock()
		if c.closing != nil {
			c.mu.Unlock()
			return nil, errSidecarCacheClosed
		}
		if e := c.entries[dataKey]; e != nil {
			c.lru.MoveToFront(e.elem)
			c.mu.Unlock()
			if !counted {
				c.metrics.requests.WithLabelValues("hit").Inc()
			}
			return e.filter, nil
		}
		f := c.inflight[dataKey]
		joined := f != nil
		if joined {
			f.waiters++
		} else {
			f = c.startFetchLocked(dataKey)
		}
		c.mu.Unlock()
		if joined && c.afterWaiter != nil {
			c.afterWaiter()
		}
		if !counted {
			// One request per Get: joining an existing fetch is a miss too,
			// and retrying a canceled generation must not count again.
			c.metrics.requests.WithLabelValues("miss").Inc()
			counted = true
		}
		filter, err := c.awaitFetch(ctx, f)
		if err != nil && ctx.Err() == nil && errors.Is(err, context.Canceled) {
			continue
		}
		return filter, err
	}
	return nil, fmt.Errorf("query: sidecar cache: fetch %s kept being canceled", dataKey)
}

// awaitFetch blocks until the shared fetch finishes or ctx ends. A caller that
// gives up leaves the fetch, canceling it only when it was the last interested
// caller.
func (c *sidecarCache) awaitFetch(ctx context.Context, f *sidecarFetch) (*index.Filter, error) {
	select {
	case <-f.done:
		if f.err != nil {
			return nil, f.err
		}
		return f.filter, nil
	case <-ctx.Done():
		c.leaveFetch(f)
		return nil, ctx.Err()
	}
}

// leaveFetch removes one interested caller from f. If the fetch is still
// running and this was the last caller, it cancels the fetch.
func (c *sidecarCache) leaveFetch(f *sidecarFetch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.finished {
		return
	}
	f.waiters--
	if f.waiters == 0 {
		f.cancel()
	}
}

// startFetchLocked registers and starts a fetch for key. c.mu must be held.
func (c *sidecarCache) startFetchLocked(key string) *sidecarFetch {
	ctx, cancel := context.WithCancel(context.Background())
	f := &sidecarFetch{ctx: ctx, cancel: cancel, done: make(chan struct{}), waiters: 1}
	c.inflight[key] = f
	c.wg.Go(func() { c.fetchAndPublish(f, key) })
	return f
}

// fetchAndPublish fetches and decodes the sidecar, then publishes the result
// to every waiter. An entry too large for the budget is served but not
// admitted. Publishing and closing f.done happen under the lock so a leaving
// waiter cannot race the result assignment.
func (c *sidecarCache) fetchAndPublish(f *sidecarFetch, key string) {
	filter, size, err := c.fetch(f.ctx, key)
	oversized := err == nil && size > c.budget
	if oversized {
		c.log.Debug("sidecar cache: sidecar exceeds decoded budget, serving uncached",
			"key", key, "size", size, "budget", c.budget)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, key)
	f.finished = true
	if err != nil {
		f.err = err
	} else {
		f.filter = filter
		// A result that lands after Close began is served to the waiters but
		// never admitted: the cache is being torn down.
		if !oversized && c.closing == nil {
			e := &sidecarEntry{key: key, filter: filter, size: size}
			e.elem = c.lru.PushFront(e)
			c.entries[key] = e
			c.used += size
			c.evictLocked()
		}
	}
	c.setGaugesLocked()
	close(f.done)
}

// fetch reads the sidecar of key's data object within the input bound and
// decodes it, returning the filter and its accounted decoded size. The reader
// is always closed, and a read that fails halfway or a Close that fails both
// fail the fetch, preserving either cause in the returned error. The bytes
// actually transferred are counted as fetched even when the read then fails.
// Fetch and decode errors are counted separately and wrap their cause; decode
// errors keep index.ErrFormat in the chain.
func (c *sidecarCache) fetch(ctx context.Context, key string) (*index.Filter, int64, error) {
	sidecarKey := objstore.SidecarKey(key)
	rc, err := c.store.Get(ctx, sidecarKey)
	if err != nil {
		c.metrics.fetches.WithLabelValues("fetch_error").Inc()
		return nil, 0, fmt.Errorf("query: sidecar cache: fetch %s: %w", sidecarKey, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(rc, c.inputCap()+1))
	c.metrics.fetchedBytes.Add(float64(len(data)))
	closeErr := rc.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		c.metrics.fetches.WithLabelValues("fetch_error").Inc()
		return nil, 0, fmt.Errorf("query: sidecar cache: read %s: %w", sidecarKey, err)
	}
	if int64(len(data)) > c.inputCap() {
		c.metrics.fetches.WithLabelValues("oversized").Inc()
		return nil, 0, fmt.Errorf("query: sidecar cache: sidecar %s exceeds the %d-byte read bound", sidecarKey, c.inputCap())
	}
	filter, err := index.Decode(data)
	if err != nil {
		c.metrics.fetches.WithLabelValues("decode_error").Inc()
		return nil, 0, fmt.Errorf("query: sidecar cache: decode %s: %w", sidecarKey, err)
	}
	c.metrics.fetches.WithLabelValues("success").Inc()
	return filter, sidecarAccountedSize(key, data), nil
}

// inputCap is the per-sidecar read bound.
func (c *sidecarCache) inputCap() int64 {
	return cmp.Or(c.maxInput, maxSidecarInput)
}

// evictLocked removes least-recently-used entries until the resident set fits
// the budget. The just-admitted entry sits at the front and its size never
// exceeds the budget (admission refuses larger entries), so once it is the
// only entry left the loop stops: what it accounts for already fits.
func (c *sidecarCache) evictLocked() {
	for c.used > c.budget && c.lru.Len() > 0 {
		victim := c.lru.Back().Value.(*sidecarEntry)
		c.lru.Remove(victim.elem)
		delete(c.entries, victim.key)
		c.used -= victim.size
		c.metrics.evictions.Inc()
	}
}

func (c *sidecarCache) setGaugesLocked() {
	c.metrics.bytes.Set(float64(c.used))
	c.metrics.entries.Set(float64(len(c.entries)))
}

// Close stops new Gets, cancels in-flight fetches, drops the resident set
// (gauges included), waits for the fetch goroutines and unregisters the
// metrics, so a service that rolls back or shuts down can rebuild a cache on
// the same registry. Concurrent and repeated callers all wait for the same
// teardown instead of returning while a store that ignores cancellation still
// holds a fetch.
func (c *sidecarCache) Close() error {
	c.mu.Lock()
	if c.closing == nil {
		c.closing = make(chan struct{})
		close(c.closing)
		for _, f := range c.inflight {
			f.cancel()
		}
		clear(c.entries)
		c.lru.Init()
		c.used = 0
		c.setGaugesLocked()
	}
	c.mu.Unlock()
	c.teardownOnce()
	return nil
}

// sidecarAccountedSize is the decoded memory an admitted entry occupies: its
// bloom words (the wire words are the decoded bitset), the cached key and a
// fixed entry-overhead allowance.
func sidecarAccountedSize(dataKey string, data []byte) int64 {
	bloomBytes := max(len(data)-sidecarWireOverhead, 0)
	return int64(bloomBytes + len(dataKey) + sidecarEntryOverhead)
}

// validateSidecarGet checks the tenant and that dataKey names one of its data
// objects, before any I/O: a valid object-store key whose first segment is
// the tenant and whose extension is a data file. The extension check also
// keeps a sidecar key from being passed off as a data key, whose derived
// sidecar key would collide with itself.
func validateSidecarGet(t tenant.ID, dataKey string) error {
	if err := t.Validate(); err != nil {
		return fmt.Errorf("query: sidecar cache: %w", err)
	}
	if err := objstore.ValidateKey(dataKey); err != nil {
		return fmt.Errorf("query: sidecar cache: %w", err)
	}
	first, _, _ := strings.Cut(dataKey, "/")
	if first != string(t) {
		return fmt.Errorf("query: sidecar cache: key %q is not in tenant %q", dataKey, t)
	}
	if !strings.HasSuffix(dataKey, objstore.ExtData) {
		return fmt.Errorf("query: sidecar cache: key %q is not a data object", dataKey)
	}
	if err := objstore.ValidateKey(objstore.SidecarKey(dataKey)); err != nil {
		return fmt.Errorf("query: sidecar cache: %w", err)
	}
	return nil
}
