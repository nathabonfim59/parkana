package query

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// cacheTmpDir holds in-flight downloads inside the cache root. Only complete
// downloads are renamed out of it, so its contents are stale at startup.
const cacheTmpDir = ".tmp"

// errCacheClosed is returned by Acquire after Close.
var errCacheClosed = errors.New("query: object cache is closed")

// localPathStore is the optional capability an object store exposes when it
// can hand a local file straight to DuckDB. objstore.FS implements it; remote
// stores such as S3 do not.
type localPathStore interface {
	LocalPath(key string) (string, error)
}

// cacheOptions configures an objectCache.
type cacheOptions struct {
	store    objstore.Store
	dir      string
	budget   int64
	local    bool
	logger   *slog.Logger
	register prometheus.Registerer
}

// objectCache resolves catalog object keys to absolute local filenames for
// DuckDB. For a store exposing LocalPath (the fs backend) it returns the
// store's own file and copies nothing. For a remote store (S3) it downloads
// each immutable key once into dir, deduplicates concurrent downloads and
// evicts least-recently-used files above a byte budget while never evicting a
// file a caller still holds a lease on.
type objectCache struct {
	store   objstore.Store
	local   localPathStore // nil for remote stores
	dir     string
	budget  int64
	root    *os.Root
	log     *slog.Logger
	metrics *cacheMetrics

	mu       sync.Mutex
	closed   bool
	entries  map[string]*cacheEntry
	lru      *list.List // of *cacheEntry, most recently used first
	used     int64
	inflight map[string]*download
	wg       sync.WaitGroup

	// afterWaiter, when non-nil, is called after a caller registers on an
	// in-flight download. Tests use it as a barrier instead of sleeping.
	afterWaiter func()
}

// cacheEntry is one complete file on disk. refs counts callers that hold a
// lease; an entry is evictable only when refs is zero.
type cacheEntry struct {
	key  string // object key, also the path relative to the cache root
	path string // absolute path handed to DuckDB
	size int64
	refs int
	elem *list.Element
}

// download is one in-flight fetch shared by every caller that wants key.
// The fetch runs on a context of its own: a caller that gives up cannot
// cancel a download another caller is waiting for. waiters counts the callers
// still interested; the last one to leave cancels the fetch.
type download struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	finished bool // guarded by objectCache.mu
	err      error
	waiters  int // guarded by objectCache.mu
}

// newObjectCache opens dir and loads the files already in it. The local flag
// declares that store is a local-directory backend, which must therefore
// expose LocalPath: copying or deriving storage paths behind the store's
// contract is refused. A local store is read in place, so dir, the cache root
// and the byte budget are not used and nothing below dir is created, scanned
// or evicted. A nil logger discards; a nil registerer discards metrics.
func newObjectCache(opts cacheOptions) (*objectCache, error) {
	if opts.store == nil {
		return nil, errors.New("query: object cache: store is required")
	}
	local, ok := opts.store.(localPathStore)
	if opts.local && !ok {
		return nil, fmt.Errorf("query: object cache: local object store %T does not expose LocalPath; refusing to copy objects", opts.store)
	}

	c := &objectCache{
		store:    opts.store,
		local:    local,
		dir:      opts.dir,
		budget:   opts.budget,
		log:      cmpLogger(opts.logger),
		entries:  make(map[string]*cacheEntry),
		lru:      list.New(),
		inflight: make(map[string]*download),
	}
	if ok {
		// The FS backend serves its own files: creating a cache root here could
		// write to, scan or evict the object store itself.
		metrics, err := newCacheMetrics(opts.register)
		if err != nil {
			return nil, err
		}
		c.metrics = metrics
		return c, nil
	}

	if c.dir == "" {
		return nil, errors.New("query: object cache: cache dir is required")
	}
	if c.budget <= 0 {
		return nil, fmt.Errorf("query: object cache: budget must be positive, got %d", c.budget)
	}
	dir, err := filepath.Abs(c.dir)
	if err != nil {
		return nil, fmt.Errorf("query: object cache: cache dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("query: object cache: cache dir: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("query: object cache: cache dir: %w", err)
	}
	c.dir = dir
	c.root = root
	metrics, err := newCacheMetrics(opts.register)
	if err != nil {
		root.Close()
		return nil, err
	}
	c.metrics = metrics
	if err := c.resetTmp(); err != nil {
		metrics.unregister()
		root.Close()
		return nil, err
	}
	if err := c.load(); err != nil {
		metrics.unregister()
		root.Close()
		return nil, err
	}
	return c, nil
}

// cmpLogger returns logger, or a discarding logger when it is nil.
func cmpLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.New(slog.DiscardHandler)
}

// Acquire returns an absolute local filename for key and a release function
// the caller must call when DuckDB no longer needs the file. It never returns
// a path with a zero release. The key must be valid and belong to t.
func (c *objectCache) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	if err := validateAcquire(t, key); err != nil {
		return "", nil, err
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if c.local != nil {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return "", nil, errCacheClosed
		}
		p, err := c.local.LocalPath(key)
		if err != nil {
			// Keep objstore.ErrNotExist in the chain: the Source retries a
			// disappeared file from a fresh catalog listing.
			return "", nil, fmt.Errorf("query: object cache: local path %s: %w", key, err)
		}
		c.metrics.requests.WithLabelValues("local").Inc()
		return p, func() {}, nil
	}
	// A healthy caller may arrive just as the last waiter of a download gives
	// up. The shared fetch then fails with context.Canceled even though this
	// caller's own context is fine; retry with a fresh download instead of
	// returning another caller's cancellation.
	const maxAttempts = 3
	counted := false
	for range maxAttempts {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return "", nil, errCacheClosed
		}
		if e := c.entries[key]; e != nil {
			e.refs++
			c.lru.MoveToFront(e.elem)
			c.metrics.leases.Inc()
			c.mu.Unlock()
			if !counted {
				c.metrics.requests.WithLabelValues("hit").Inc()
			}
			return e.path, c.release(e), nil
		}
		d := c.inflight[key]
		started := d == nil
		if started {
			d = c.startLocked(key)
		} else {
			d.waiters++
		}
		c.mu.Unlock()
		if !counted {
			// One request per accepted acquisition: a caller that joins an
			// existing download is a miss too, and retrying a canceled
			// generation must not count again.
			c.metrics.requests.WithLabelValues("miss").Inc()
			counted = true
		}
		if c.afterWaiter != nil {
			c.afterWaiter()
		}
		p, release, err := c.wait(ctx, d, key)
		if err != nil && ctx.Err() == nil && errors.Is(err, context.Canceled) {
			continue
		}
		return p, release, err
	}
	return "", nil, fmt.Errorf("query: object cache: download %s kept being canceled", key)
}

// wait blocks until the shared download finishes or ctx is canceled. On
// success the entry already holds a lease for this caller, so it must not
// acquire another one; on cancellation it gives that lease back if the
// download completed in the meantime.
func (c *objectCache) wait(ctx context.Context, d *download, key string) (string, func(), error) {
	select {
	case <-d.done:
		if d.err != nil {
			return "", nil, d.err
		}
		c.mu.Lock()
		e := c.entries[key]
		c.mu.Unlock()
		if e == nil {
			return "", nil, fmt.Errorf("query: object cache: downloaded %s is not indexed", key)
		}
		return e.path, c.release(e), nil
	case <-ctx.Done():
		c.leave(d, key)
		return "", nil, ctx.Err()
	}
}

// leave removes one interested caller from d. If the download is still
// running and this was the last caller, it cancels the fetch; if the download
// already completed, it was counted as a lease and that lease is released.
func (c *objectCache) leave(d *download, key string) {
	c.mu.Lock()
	if !d.finished {
		d.waiters--
		if d.waiters == 0 {
			d.cancel()
		}
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	if d.err == nil {
		c.mu.Lock()
		if e := c.entries[key]; e != nil {
			c.unrefLocked(e)
		}
		c.mu.Unlock()
	}
}

// startLocked registers and starts a download. c.mu must be held.
func (c *objectCache) startLocked(key string) *download {
	ctx, cancel := context.WithCancel(context.Background())
	d := &download{ctx: ctx, cancel: cancel, done: make(chan struct{}), waiters: 1}
	c.inflight[key] = d
	c.wg.Go(func() { c.fetchAndPublish(d, key) })
	return d
}

// fetchAndPublish downloads key, then publishes the entry with one lease per
// caller still waiting. Publishing and closing d.done happen under the lock so
// a canceled waiter cannot race the lease assignment.
func (c *objectCache) fetchAndPublish(d *download, key string) {
	size, err := c.fetch(d.ctx, key)
	c.mu.Lock()
	delete(c.inflight, key)
	d.finished = true
	d.err = err
	if err == nil {
		e := &cacheEntry{key: key, path: c.abs(key), size: size, refs: d.waiters}
		e.elem = c.lru.PushFront(e)
		c.entries[key] = e
		c.used += size
		c.metrics.leases.Add(float64(d.waiters))
		c.metrics.downloads.WithLabelValues("success").Inc()
		c.evictLocked()
	} else {
		c.metrics.downloads.WithLabelValues("error").Inc()
	}
	c.setGaugesLocked()
	close(d.done)
	c.mu.Unlock()
}

// fetch copies key into a temporary file under the cache root and renames it
// into place only after the copy completes. A failed or canceled copy removes
// its temporary file. Cache files need no fsync.
func (c *objectCache) fetch(ctx context.Context, key string) (int64, error) {
	tmp := path.Join(cacheTmpDir, uuid.NewV7().String()+".part")
	f, err := c.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return 0, fmt.Errorf("query: object cache: temp file: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			if err := c.root.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
				c.log.Warn("object cache: remove temp file", "key", key, "err", err)
			}
		}
	}()
	rc, err := c.store.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("query: object cache: download %s: %w", key, err)
	}
	defer rc.Close()
	n, err := io.Copy(f, ctxReader{ctx: ctx, r: rc})
	// Count what was copied from the store's reader before looking at the
	// error, so a copy that failed halfway still records the serialized
	// object bytes that made it into the cache.
	c.metrics.fetchedBytes.Add(float64(n))
	if err != nil {
		return 0, fmt.Errorf("query: object cache: download %s: %w", key, err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("query: object cache: download %s: %w", key, err)
	}
	if err := c.root.MkdirAll(path.Dir(key), 0o755); err != nil {
		return 0, fmt.Errorf("query: object cache: download %s: %w", key, err)
	}
	if err := c.root.Rename(tmp, key); err != nil {
		return 0, fmt.Errorf("query: object cache: publish %s: %w", key, err)
	}
	ok = true
	return n, nil
}

// release returns an idempotent lease release.
func (c *objectCache) release(e *cacheEntry) func() {
	return sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.unrefLocked(e)
	})
}

// unrefLocked drops one lease and evicts when the entry became evictable.
func (c *objectCache) unrefLocked(e *cacheEntry) {
	if e.refs == 0 {
		return
	}
	e.refs--
	c.metrics.leases.Dec()
	if e.refs == 0 && !c.closed {
		c.evictLocked()
		c.setGaugesLocked()
	}
}

// evictLocked removes least-recently-used entries until used fits the budget,
// skipping any entry with an active lease. A single object larger than the
// budget therefore stays while leased and disappears after the last release.
// A failed removal keeps the entry indexed and accounted and stops the pass:
// a later release or acquisition retries instead of spinning on it while the
// disk still holds the bytes.
func (c *objectCache) evictLocked() {
	for c.used > c.budget && !c.closed {
		var victim *cacheEntry
		for le := c.lru.Back(); le != nil; le = le.Prev() {
			if e := le.Value.(*cacheEntry); e.refs == 0 {
				victim = e
				break
			}
		}
		if victim == nil {
			return
		}
		if err := c.root.Remove(victim.key); err != nil && !errors.Is(err, fs.ErrNotExist) {
			c.log.Warn("object cache: evict", "key", victim.key, "err", err)
			return
		}
		c.lru.Remove(victim.elem)
		delete(c.entries, victim.key)
		c.used -= victim.size
		c.metrics.evictions.Inc()
	}
}

func (c *objectCache) setGaugesLocked() {
	c.metrics.bytes.Set(float64(c.used))
	c.metrics.entries.Set(float64(len(c.entries)))
}

// resetTmp removes the temporary directory left by an earlier process. No
// download can be in flight before construction, so everything in it is
// stale.
func (c *objectCache) resetTmp() error {
	if err := c.root.RemoveAll(cacheTmpDir); err != nil {
		return fmt.Errorf("query: object cache: reset temp dir: %w", err)
	}
	if err := c.root.MkdirAll(cacheTmpDir, 0o755); err != nil {
		return fmt.Errorf("query: object cache: create temp dir: %w", err)
	}
	return nil
}

// load indexes complete files left by an earlier process so they can be
// reused as cache hits, and removes anything that could not have been written
// by this cache: stale temp files, symlinks, non-regular files and paths that
// fail key or tenant validation. Files over the byte budget are evicted
// oldest first.
func (c *objectCache) load() error {
	type scanned struct {
		key  string
		size int64
		mod  time.Time
	}
	var files []scanned
	err := fs.WalkDir(c.root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		if d.IsDir() {
			if p == cacheTmpDir {
				return fs.SkipDir
			}
			return nil
		}
		if p == cacheTmpDir || !d.Type().IsRegular() || validateCachedKey(p) != nil {
			if err := c.root.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, scanned{key: p, size: info.Size(), mod: info.ModTime()})
		return nil
	})
	if err != nil {
		return fmt.Errorf("query: object cache: scan %s: %w", c.dir, err)
	}
	slices.SortFunc(files, func(a, b scanned) int { return a.mod.Compare(b.mod) })
	for _, f := range files {
		e := &cacheEntry{key: f.key, path: c.abs(f.key), size: f.size}
		e.elem = c.lru.PushFront(e)
		c.entries[f.key] = e
		c.used += f.size
	}
	c.evictLocked()
	c.setGaugesLocked()
	return nil
}

func (c *objectCache) abs(key string) string {
	return filepath.Join(c.dir, filepath.FromSlash(key))
}

// Close stops new acquisitions, cancels in-flight downloads and closes the
// cache root. Releases that happen afterwards remain safe: they only update
// counters, never touch files.
func (c *objectCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for _, d := range c.inflight {
		d.cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
	if c.root != nil {
		if err := c.root.Close(); err != nil {
			return fmt.Errorf("query: object cache: close: %w", err)
		}
	}
	return nil
}

// validateAcquire checks the tenant and that key names an object in it.
func validateAcquire(t tenant.ID, key string) error {
	if err := t.Validate(); err != nil {
		return fmt.Errorf("query: object cache: %w", err)
	}
	return validateCachedKey(key, string(t))
}

// validateCachedKey checks an object key, and that its first segment is a
// valid tenant. When want is non-empty the first segment must equal it.
func validateCachedKey(key string, want ...string) error {
	if err := objstore.ValidateKey(key); err != nil {
		return fmt.Errorf("query: object cache: %w", err)
	}
	first, _, _ := strings.Cut(key, "/")
	if err := tenant.ID(first).Validate(); err != nil {
		return fmt.Errorf("query: object cache: %w", err)
	}
	if len(want) > 0 && want[0] != first {
		return fmt.Errorf("query: object cache: key %q is not in tenant %q", key, want[0])
	}
	return nil
}

// ctxReader stops a copy promptly when the shared download is canceled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// cacheMetrics are the querier cache's bounded-label metrics.
type cacheMetrics struct {
	requests     *prometheus.CounterVec // result: hit, miss, local
	downloads    *prometheus.CounterVec // result: success, error
	evictions    prometheus.Counter
	fetchedBytes prometheus.Counter
	bytes        prometheus.Gauge
	entries      prometheus.Gauge
	leases       prometheus.Gauge

	reg        prometheus.Registerer
	registered []prometheus.Collector
}

// unregister removes every collector this cache successfully registered. It is
// called when construction fails after registration.
func (m *cacheMetrics) unregister() {
	if m == nil || m.reg == nil {
		return
	}
	for _, collector := range m.registered {
		m.reg.Unregister(collector)
	}
	m.registered = nil
}

// newCacheMetrics builds and registers the cache metrics. A nil registerer
// returns usable but unregistered collectors. A partial registration failure
// unregisters what it already registered, so the registerer stays reusable.
func newCacheMetrics(reg prometheus.Registerer) (*cacheMetrics, error) {
	m := &cacheMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_querier_cache_requests_total",
			Help: "Object cache acquisitions by result.",
		}, []string{"result"}),
		downloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_querier_cache_downloads_total",
			Help: "Remote object downloads by result.",
		}, []string{"result"}),
		evictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_querier_cache_evictions_total",
			Help: "Cached objects evicted to stay within the byte budget.",
		}),
		fetchedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "parkana_querier_cache_fetched_bytes_total",
			Help: "Serialized object bytes copied from the object store into the cache, partial failed copies included; local-path hits copy nothing and do not count. Candidate transfer bytes, not physical disk reads.",
		}),
		bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_querier_cache_bytes",
			Help: "Bytes currently occupied by cached objects.",
		}),
		entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_querier_cache_entries",
			Help: "Cached objects currently indexed.",
		}),
		leases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "parkana_querier_cache_leases",
			Help: "Object cache leases currently held by callers.",
		}),
		reg: reg,
	}
	all := []prometheus.Collector{
		m.requests, m.downloads, m.evictions, m.fetchedBytes, m.bytes, m.entries, m.leases,
	}
	// Expose every bounded result value from the start, so a scrape shows 0
	// rather than a missing series before the first hit or failure.
	for _, result := range []string{"hit", "miss", "local"} {
		m.requests.WithLabelValues(result)
	}
	for _, result := range []string{"success", "error"} {
		m.downloads.WithLabelValues(result)
	}
	if reg == nil {
		return m, nil
	}
	for _, collector := range all {
		if err := reg.Register(collector); err != nil {
			m.unregister()
			return nil, fmt.Errorf("query: object cache: register metrics: %w", err)
		}
		m.registered = append(m.registered, collector)
	}
	return m, nil
}
