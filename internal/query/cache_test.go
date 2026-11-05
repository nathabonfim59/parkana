package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// memStore is a minimal in-memory objstore.Store for cache tests. It counts
// Gets per key, can fail them on demand and can block them on a gate so
// concurrent-acquisition tests use barriers instead of sleeps.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]int
	fails   map[string][]error
	gates   map[string]*gate
}

// gate blocks Get until release is closed or the caller's context ends.
// A deaf gate ignores cancellation until release, which lets tests hold a
// canceled download in flight while a later caller joins it.
type gate struct {
	started chan struct{}
	release chan struct{}
	deaf    bool
	once    sync.Once
}

func newMemStore() *memStore {
	return &memStore{
		objects: make(map[string][]byte),
		gets:    make(map[string]int),
		fails:   make(map[string][]error),
		gates:   make(map[string]*gate),
	}
}

func (s *memStore) put(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = bytes.Clone(data)
}

func (s *memStore) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *memStore) totalGets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.gets {
		n += c
	}
	return n
}

func (s *memStore) failNext(key string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[key] = append(s.fails[key], err)
}

func (s *memStore) block(key string) *gate {
	g := &gate{started: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gates[key] = g
	return g
}

// blockDeaf blocks Get until release even after the caller's context ends, so
// a canceled download stays registered while a new caller joins it.
func (s *memStore) blockDeaf(key string) *gate {
	g := &gate{started: make(chan struct{}), release: make(chan struct{}), deaf: true}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gates[key] = g
	return g
}

func (s *memStore) clearBlock(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.gates, key)
}

func (s *memStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets[key]++
	g := s.gates[key]
	var failErr error
	if q := s.fails[key]; len(q) > 0 {
		failErr = q[0]
		s.fails[key] = q[1:]
	}
	data, ok := s.objects[key]
	s.mu.Unlock()

	if g != nil {
		g.once.Do(func() { close(g.started) })
		if g.deaf {
			<-g.release
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		} else {
			select {
			case <-g.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if failErr != nil {
		return nil, failErr
	}
	if !ok {
		return nil, fmt.Errorf("memstore: get %s: %w", key, objstore.ErrNotExist)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.put(key, data)
	return nil
}

func (s *memStore) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memstore: get %s: %w", key, objstore.ErrNotExist)
	}
	if offset < 0 || offset > int64(len(data)) {
		return nil, fmt.Errorf("memstore: get %s: bad offset %d", key, offset)
	}
	if length < 0 || offset+length > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	return io.NopCloser(bytes.NewReader(data[offset : offset+length])), nil
}

func (s *memStore) Stat(_ context.Context, key string) (objstore.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return objstore.Info{}, fmt.Errorf("memstore: stat %s: %w", key, objstore.ErrNotExist)
	}
	return objstore.Info{Key: key, Size: int64(len(data))}, nil
}

func (s *memStore) List(_ context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return func(yield func(objstore.Info, error) bool) {
		s.mu.Lock()
		keys := slices.Sorted(maps.Keys(s.objects))
		sizes := make(map[string]int, len(keys))
		for _, k := range keys {
			sizes[k] = len(s.objects[k])
		}
		s.mu.Unlock()
		for _, k := range keys {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			if !yield(objstore.Info{Key: k, Size: int64(sizes[k])}, nil) {
				return
			}
		}
	}
}

func (s *memStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

func (s *memStore) Close() error { return nil }

// localStore is an fs-like store: it exposes LocalPath instead of moving
// bytes through Get, and records both kinds of access so tests can prove the
// cache copies nothing.
type localStore struct {
	*memStore
	dir string

	localMu    sync.Mutex
	localCalls int
}

func newLocalStore(dir string) *localStore {
	return &localStore{memStore: newMemStore(), dir: dir}
}

func (s *localStore) LocalPath(key string) (string, error) {
	s.localMu.Lock()
	s.localCalls++
	s.localMu.Unlock()
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("localstore: %s: %w", key, objstore.ErrNotExist)
	}
	p := filepath.Join(s.dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return "", err
	}
	return p, nil
}

func (s *localStore) calls() int {
	s.localMu.Lock()
	defer s.localMu.Unlock()
	return s.localCalls
}

func testKey(t tenant.ID, name string) string {
	return string(t) + "/logs/api/2026/11/01/10/" + name
}

func newTestCache(t *testing.T, store objstore.Store, mutate func(*cacheOptions)) *objectCache {
	t.Helper()
	opts := cacheOptions{
		store:  store,
		dir:    t.TempDir(),
		budget: 1 << 20,
		logger: slog.New(slog.DiscardHandler),
	}
	if mutate != nil {
		mutate(&opts)
	}
	c, err := newObjectCache(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func (c *objectCache) refs(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		return e.refs
	}
	return 0
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeTestFile(root, key string, data []byte) error {
	p := filepath.Join(root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func touchTestFile(root, key string, mod time.Time) error {
	p := filepath.Join(root, filepath.FromSlash(key))
	return os.Chtimes(p, mod, mod)
}

func symlinkTestFile(target, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, path)
}

// cacheFiles lists regular files below dir, relative and slash-separated,
// excluding the temporary download directory.
func cacheFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == cacheTmpDir || strings.HasPrefix(rel, cacheTmpDir+"/") {
			return nil
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

func tempFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, cacheTmpDir))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

// metricFloat returns the value of an unlabeled gauge or counter family.
func metricFloat(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

// metricLabel returns the value of a counter or gauge whose label has value.
func metricLabel(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			matched := false
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					matched = true
				}
			}
			if !matched {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatalf("metric %q{%s=%q} not found", name, label, value)
	return 0
}

// countingLocal wraps a real objstore.FS and counts remote reads so a test can
// prove the local cache mode never copies through Get.
type countingLocal struct {
	*objstore.FS
	gets atomic.Int64
}

func (s *countingLocal) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.gets.Add(1)
	return s.FS.Get(ctx, key)
}

func TestObjectCacheConcurrentAcquireDownloadsOnce(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "once.parquet")
	data := bytes.Repeat([]byte("x"), 4096)
	st.put(key, data)
	c := newTestCache(t, st, nil)
	gate := st.block(key)
	signals := make(chan struct{}, 32)
	c.afterWaiter = func() { signals <- struct{}{} }

	const n = 16
	var wg sync.WaitGroup
	paths := make([]string, n)
	releases := make([]func(), n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			paths[i], releases[i], errs[i] = c.Acquire(t.Context(), tenant.Default, key)
		})
	}
	// The first Get blocks, so every caller must register before the download
	// can finish: no sleeps needed to prove deduplication.
	for range n {
		<-signals
	}
	close(gate.release)
	wg.Wait()

	if got := st.getCount(key); got != 1 {
		t.Errorf("downloads = %d, want 1", got)
	}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("Acquire %d: %v", i, errs[i])
		}
		if releases[i] == nil {
			t.Fatalf("Acquire %d: nil release", i)
		}
		if paths[i] != paths[0] {
			t.Errorf("Acquire %d path = %q, want %q", i, paths[i], paths[0])
		}
		if got := readTestFile(t, paths[i]); !bytes.Equal(got, data) {
			t.Errorf("Acquire %d content = %q, want %q", i, got, data)
		}
		releases[i]()
	}
	if refs := c.refs(key); refs != 0 {
		t.Errorf("refs after release = %d, want 0", refs)
	}
}

func TestObjectCacheHitAfterRelease(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "hit.parquet")
	st.put(key, []byte("hit"))
	c := newTestCache(t, st, nil)

	p1, rel1, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel1()
	p2, rel2, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel2()
	if p1 != p2 {
		t.Errorf("paths %q and %q differ", p1, p2)
	}
	if got := st.getCount(key); got != 1 {
		t.Errorf("downloads = %d, want 1", got)
	}
}

func TestObjectCacheSeparateTenants(t *testing.T) {
	st := newMemStore()
	a := testKey(tenant.ID("alpha"), "x.parquet")
	b := testKey(tenant.ID("beta"), "x.parquet")
	st.put(a, []byte("alpha"))
	st.put(b, []byte("beta"))
	c := newTestCache(t, st, nil)

	pa, rela, err := c.Acquire(t.Context(), tenant.ID("alpha"), a)
	if err != nil {
		t.Fatal(err)
	}
	pb, relb, err := c.Acquire(t.Context(), tenant.ID("beta"), b)
	if err != nil {
		t.Fatal(err)
	}
	defer rela()
	defer relb()
	if pa == pb {
		t.Fatalf("tenants share path %q", pa)
	}
	if got := readTestFile(t, pa); string(got) != "alpha" {
		t.Errorf("alpha content = %q", got)
	}
	if got := readTestFile(t, pb); string(got) != "beta" {
		t.Errorf("beta content = %q", got)
	}
	if st.getCount(a) != 1 || st.getCount(b) != 1 {
		t.Errorf("downloads alpha=%d beta=%d, want 1 each", st.getCount(a), st.getCount(b))
	}
}

func TestObjectCacheRejectsInvalidKeys(t *testing.T) {
	st := newMemStore()
	c := newTestCache(t, st, nil)
	cases := []struct {
		name   string
		tenant tenant.ID
		key    string
	}{
		{"empty", tenant.Default, ""},
		{"absolute", tenant.Default, "/default/logs/x.parquet"},
		{"dotdot", tenant.Default, "default/logs/../x.parquet"},
		{"backslash", tenant.Default, `default/logs\x.parquet`},
		{"empty segment", tenant.Default, "default//x.parquet"},
		{"other tenant", tenant.Default, "other/logs/x.parquet"},
		{"invalid tenant", tenant.ID("bad tenant"), "bad tenant/logs/x.parquet"},
		{"dot tenant", tenant.ID(".."), "../logs/x.parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := c.Acquire(t.Context(), tc.tenant, tc.key); err == nil {
				t.Fatalf("Acquire(%q, %q): no error", tc.tenant, tc.key)
			}
		})
	}
	if got := st.totalGets(); got != 0 {
		t.Errorf("downloads = %d, want 0", got)
	}
}

func TestObjectCacheFailedDownloadCleanupAndRetry(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "fail.parquet")
	data := []byte("after retry")
	st.put(key, data)
	st.failNext(key, errors.New("boom"))
	dir := t.TempDir()
	c := newTestCache(t, st, func(o *cacheOptions) { o.dir = dir })

	if _, _, err := c.Acquire(t.Context(), tenant.Default, key); err == nil {
		t.Fatal("Acquire: no error")
	}
	if files := cacheFiles(t, dir); len(files) != 0 {
		t.Errorf("files after failed download = %v, want none", files)
	}
	if tmp := tempFiles(t, dir); len(tmp) != 0 {
		t.Errorf("temp files after failed download = %v, want none", tmp)
	}

	p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	if got := readTestFile(t, p); !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	if got := st.getCount(key); got != 2 {
		t.Errorf("downloads = %d, want 2 (failure then retry)", got)
	}
}

func TestObjectCacheLeaderCancelKeepsWaiter(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "leader.parquet")
	data := []byte("leader canceled")
	st.put(key, data)
	c := newTestCache(t, st, nil)
	gate := st.block(key)
	signals := make(chan struct{}, 4)
	c.afterWaiter = func() { signals <- struct{}{} }

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	leaderErr := make(chan error, 1)
	go func() {
		_, rel, err := c.Acquire(leaderCtx, tenant.Default, key)
		if rel != nil {
			rel()
		}
		leaderErr <- err
	}()
	<-signals // leader registered on the download

	type result struct {
		path string
		rel  func()
		err  error
	}
	waiterDone := make(chan result, 1)
	go func() {
		p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
		waiterDone <- result{p, rel, err}
	}()
	<-signals // waiter registered too

	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	close(gate.release)
	res := <-waiterDone
	if res.err != nil {
		t.Fatalf("waiter: %v", res.err)
	}
	if got := readTestFile(t, res.path); !bytes.Equal(got, data) {
		t.Errorf("waiter content = %q, want %q", got, data)
	}
	res.rel()
	if got := st.getCount(key); got != 1 {
		t.Errorf("downloads = %d, want 1", got)
	}
	if refs := c.refs(key); refs != 0 {
		t.Errorf("refs after release = %d, want 0", refs)
	}
}

func TestObjectCacheAllWaitersCancelThenRetry(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "cancel.parquet")
	data := []byte("retried")
	st.put(key, data)
	dir := t.TempDir()
	c := newTestCache(t, st, func(o *cacheOptions) { o.dir = dir })
	st.block(key)
	signals := make(chan struct{}, 4)
	c.afterWaiter = func() { signals <- struct{}{} }

	ctx1, cancel1 := context.WithCancel(t.Context())
	ctx2, cancel2 := context.WithCancel(t.Context())
	errs := make(chan error, 2)
	go func() {
		_, _, err := c.Acquire(ctx1, tenant.Default, key)
		errs <- err
	}()
	<-signals
	go func() {
		_, _, err := c.Acquire(ctx2, tenant.Default, key)
		errs <- err
	}()
	<-signals

	cancel1()
	cancel2()
	for range 2 {
		if err := <-errs; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller error = %v, want context.Canceled", err)
		}
	}
	// Every waiter is gone, so the shared download must have been canceled and
	// cleaned up before a retry.
	c.wg.Wait()
	if files := cacheFiles(t, dir); len(files) != 0 {
		t.Errorf("files after canceled download = %v, want none", files)
	}
	if tmp := tempFiles(t, dir); len(tmp) != 0 {
		t.Errorf("temp files after canceled download = %v, want none", tmp)
	}

	st.clearBlock(key)
	p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	if got := readTestFile(t, p); !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	if got := st.getCount(key); got != 2 {
		t.Errorf("downloads = %d, want 2 (canceled then retried)", got)
	}
}

func TestObjectCacheCancelRacesCompletion(t *testing.T) {
	for i := range 20 {
		st := newMemStore()
		key := testKey(tenant.Default, "race.parquet")
		data := []byte("race")
		st.put(key, data)
		c := newTestCache(t, st, nil)
		gate := st.block(key)
		signals := make(chan struct{}, 4)
		c.afterWaiter = func() { signals <- struct{}{} }

		type result struct {
			rel func()
			err error
		}
		leaderDone := make(chan result, 1)
		go func() {
			_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
			leaderDone <- result{rel: rel, err: err}
		}()
		<-signals

		waiterCtx, cancelWaiter := context.WithCancel(t.Context())
		waiterDone := make(chan result, 1)
		go func() {
			_, rel, err := c.Acquire(waiterCtx, tenant.Default, key)
			waiterDone <- result{rel: rel, err: err}
		}()
		<-signals

		// Complete the download and cancel the waiter at the same moment; the
		// select in wait decides which side wins on each iteration.
		close(gate.release)
		cancelWaiter()

		leader := <-leaderDone
		waiter := <-waiterDone
		if leader.err != nil {
			t.Fatalf("iteration %d: leader: %v", i, leader.err)
		}
		if waiter.err == nil {
			if refs := c.refs(key); refs < 1 {
				t.Fatalf("iteration %d: success with %d refs", i, refs)
			}
			waiter.rel()
		} else if !errors.Is(waiter.err, context.Canceled) {
			t.Fatalf("iteration %d: waiter: %v", i, waiter.err)
		}
		leader.rel()
		if refs := c.refs(key); refs != 0 {
			t.Fatalf("iteration %d: refs after release = %d, want 0", i, refs)
		}
		if got := st.getCount(key); got != 1 {
			t.Fatalf("iteration %d: downloads = %d, want 1", i, got)
		}
	}
}

func TestObjectCacheLRUEvictsOldest(t *testing.T) {
	st := newMemStore()
	a := testKey(tenant.Default, "a.parquet")
	b := testKey(tenant.Default, "b.parquet")
	cc := testKey(tenant.Default, "c.parquet")
	for _, k := range []string{a, b, cc} {
		st.put(k, bytes.Repeat([]byte("x"), 100))
	}
	dir := t.TempDir()
	c := newTestCache(t, st, func(o *cacheOptions) {
		o.dir = dir
		o.budget = 200
	})

	use := func(key string) {
		t.Helper()
		_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
		if err != nil {
			t.Fatal(err)
		}
		rel()
	}
	use(a)
	use(b)
	use(cc) // 300 bytes: evicts a, the least recently used
	if files := cacheFiles(t, dir); len(files) != 2 || !slices.Contains(files, b) || !slices.Contains(files, cc) {
		t.Errorf("files = %v, want b and c", files)
	}
	use(a)
	if got := st.getCount(a); got != 2 {
		t.Errorf("a downloads = %d, want 2 (evicted then re-downloaded)", got)
	}
}

func TestObjectCachePinnedFileSurvivesOverBudget(t *testing.T) {
	st := newMemStore()
	a := testKey(tenant.Default, "a.parquet")
	b := testKey(tenant.Default, "b.parquet")
	st.put(a, bytes.Repeat([]byte("a"), 80))
	st.put(b, bytes.Repeat([]byte("b"), 80))
	dir := t.TempDir()
	c := newTestCache(t, st, func(o *cacheOptions) {
		o.dir = dir
		o.budget = 100
	})

	_, relA, err := c.Acquire(t.Context(), tenant.Default, a)
	if err != nil {
		t.Fatal(err)
	}
	_, relB, err := c.Acquire(t.Context(), tenant.Default, b)
	if err != nil {
		t.Fatal(err)
	}
	// b pushes the cache over budget, but both files are leased, so nothing
	// may be evicted yet.
	if files := cacheFiles(t, dir); len(files) != 2 {
		t.Fatalf("files while leased = %v, want both", files)
	}
	relB()
	// a is still pinned; b is the least recently used unpinned file.
	if files := cacheFiles(t, dir); len(files) != 1 || files[0] != a {
		t.Errorf("files after releasing b = %v, want only a", files)
	}
	relA()
	if refs := c.refs(a); refs != 0 {
		t.Errorf("a refs = %d, want 0", refs)
	}
	c.mu.Lock()
	used := c.used
	c.mu.Unlock()
	if used != 80 {
		t.Errorf("used bytes = %d, want 80", used)
	}
}

func TestObjectCacheOversizedObjectKeepsOneDownloadWhileLeased(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "big.parquet")
	st.put(key, bytes.Repeat([]byte("z"), 250))
	dir := t.TempDir()
	c := newTestCache(t, st, func(o *cacheOptions) {
		o.dir = dir
		o.budget = 100
	})

	_, rel1, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	_, rel2, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.getCount(key); got != 1 {
		t.Errorf("downloads while leased = %d, want 1", got)
	}
	rel1()
	if files := cacheFiles(t, dir); len(files) != 1 {
		t.Errorf("oversized file evicted while still leased: %v", files)
	}
	rel2()
	if files := cacheFiles(t, dir); len(files) != 0 {
		t.Errorf("oversized file after last release = %v, want evicted", files)
	}
	// A later acquisition must download again, not reuse a stale index entry.
	_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if got := st.getCount(key); got != 2 {
		t.Errorf("downloads = %d, want 2", got)
	}
}

func TestObjectCacheStartupReusesCompleteFiles(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "old.parquet")
	data := []byte("old cache file")
	dir := t.TempDir()
	if err := writeTestFile(dir, key, data); err != nil {
		t.Fatal(err)
	}
	if err := writeTestFile(dir, cacheTmpDir+"/stale.part", []byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := touchTestFile(dir, key, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	c := newTestCache(t, st, func(o *cacheOptions) { o.dir = dir })
	p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	if got := readTestFile(t, p); !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	if got := st.getCount(key); got != 0 {
		t.Errorf("downloads = %d, want 0 (startup file reused)", got)
	}
	if tmp := tempFiles(t, dir); len(tmp) != 0 {
		t.Errorf("stale temp files = %v, want removed", tmp)
	}
}

func TestObjectCacheStartupEvictsOverBudget(t *testing.T) {
	st := newMemStore()
	dir := t.TempDir()
	keys := []string{
		testKey(tenant.Default, "old.parquet"),
		testKey(tenant.Default, "middle.parquet"),
		testKey(tenant.Default, "new.parquet"),
	}
	now := time.Now()
	for i, key := range keys {
		if err := writeTestFile(dir, key, bytes.Repeat([]byte("x"), 100)); err != nil {
			t.Fatal(err)
		}
		if err := touchTestFile(dir, key, now.Add(time.Duration(i-3)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	_ = newTestCache(t, st, func(o *cacheOptions) {
		o.dir = dir
		o.budget = 150
	})
	if files := cacheFiles(t, dir); len(files) != 1 || files[0] != keys[2] {
		t.Errorf("files = %v, want only %q", files, keys[2])
	}
}

func TestObjectCacheStartupRejectsUnsafeEntries(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "escape.parquet")
	st.put(key, []byte("real"))
	dir := t.TempDir()

	// A valid key left by an earlier process must be reusable...
	good := testKey(tenant.Default, "good.parquet")
	if err := writeTestFile(dir, good, []byte("good")); err != nil {
		t.Fatal(err)
	}
	// ...while invalid tenants and symlinks must be removed without following
	// the link out of the cache root.
	bad := "bad tenant/logs/x.parquet"
	if err := writeTestFile(dir, bad, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, filepath.FromSlash(key))
	if err := symlinkTestFile(outside, linkPath); err != nil {
		t.Fatal(err)
	}

	c := newTestCache(t, st, func(o *cacheOptions) { o.dir = dir })
	if files := cacheFiles(t, dir); !slices.Equal(files, []string{good}) {
		t.Fatalf("startup files = %v, want only %q", files, good)
	}
	if _, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(bad))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("invalid tenant file still present: %v", err)
	}

	p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	if got := readTestFile(t, p); string(got) != "real" {
		t.Errorf("content = %q, want the store object, not the symlink target", got)
	}
	if st.getCount(key) != 1 {
		t.Errorf("downloads = %d, want 1", st.getCount(key))
	}
	if got := readTestFile(t, outside); string(got) != "secret" {
		t.Errorf("outside file changed to %q", got)
	}
	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		t.Error("symlink was not replaced by a regular file")
	}
	if files := cacheFiles(t, dir); len(files) != 2 || !slices.Contains(files, good) || !slices.Contains(files, key) {
		t.Errorf("files = %v, want %q and %q", files, good, key)
	}
}

func TestObjectCacheLocalStoreCopiesNothing(t *testing.T) {
	storeDir := t.TempDir()
	st := newLocalStore(storeDir)
	key := testKey(tenant.Default, "local.parquet")
	data := []byte("local parquet bytes")
	st.put(key, data)
	cacheDir := t.TempDir()
	c, err := newObjectCache(cacheOptions{
		store:  st,
		dir:    cacheDir,
		budget: 1 << 20,
		local:  true,
		logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })

	p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	want := filepath.Join(storeDir, filepath.FromSlash(key))
	if p != want {
		t.Errorf("path = %q, want %q", p, want)
	}
	if got := readTestFile(t, p); !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	if got := st.getCount(key); got != 0 {
		t.Errorf("remote Get calls = %d, want 0", got)
	}
	if got := st.calls(); got != 1 {
		t.Errorf("LocalPath calls = %d, want 1", got)
	}
	if files := cacheFiles(t, cacheDir); len(files) != 0 {
		t.Errorf("cache copies = %v, want none", files)
	}

	if _, _, err := c.Acquire(t.Context(), tenant.Default, "../escape.parquet"); err == nil {
		t.Error("invalid key: no error")
	}
	if got := st.calls(); got != 1 {
		t.Errorf("LocalPath calls after invalid key = %d, want 1", got)
	}
	if _, _, err := c.Acquire(t.Context(), tenant.Default, testKey(tenant.Default, "missing.parquet")); !errors.Is(err, objstore.ErrNotExist) {
		t.Errorf("missing object error = %v, want objstore.ErrNotExist", err)
	}
}

func TestObjectCacheLocalStoreRequiresLocalPath(t *testing.T) {
	st := newMemStore() // no LocalPath
	_, err := newObjectCache(cacheOptions{
		store:  st,
		dir:    t.TempDir(),
		budget: 1024,
		local:  true,
		logger: slog.New(slog.DiscardHandler),
	})
	if err == nil || !strings.Contains(err.Error(), "LocalPath") {
		t.Fatalf("error = %v, want a clear LocalPath error", err)
	}
}

func TestObjectCacheConstructorValidation(t *testing.T) {
	st := newMemStore()
	cases := []struct {
		name string
		opts cacheOptions
	}{
		{"nil store", cacheOptions{dir: t.TempDir(), budget: 1}},
		{"empty dir", cacheOptions{store: st, budget: 1}},
		{"zero budget", cacheOptions{store: st, dir: t.TempDir()}},
		{"negative budget", cacheOptions{store: st, dir: t.TempDir(), budget: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newObjectCache(tc.opts); err == nil {
				t.Fatal("newObjectCache: no error")
			}
		})
	}
}

func TestObjectCacheReleaseIsIdempotent(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "release.parquet")
	st.put(key, []byte("release"))
	c := newTestCache(t, st, nil)
	_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel()
	rel()
	if refs := c.refs(key); refs != 0 {
		t.Errorf("refs = %d, want 0", refs)
	}
	// The file is still indexed and reusable after a double release.
	_, rel2, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel2()
	if got := st.getCount(key); got != 1 {
		t.Errorf("downloads = %d, want 1", got)
	}
}

func TestObjectCacheCloseCancelsWaitersAndRejectsNewWork(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "close.parquet")
	st.put(key, []byte("close"))
	c, err := newObjectCache(cacheOptions{
		store:  st,
		dir:    t.TempDir(),
		budget: 1 << 20,
		logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	st.block(key)
	signals := make(chan struct{}, 2)
	c.afterWaiter = func() { signals <- struct{}{} }

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Acquire(t.Context(), tenant.Default, key)
		done <- err
	}()
	<-signals
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Error("in-flight Acquire: no error after Close")
	}
	if _, _, err := c.Acquire(t.Context(), tenant.Default, key); !errors.Is(err, errCacheClosed) {
		t.Errorf("Acquire after Close = %v, want errCacheClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestObjectCacheMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	st := newMemStore()
	failKey := testKey(tenant.Default, "metrics-fail.parquet")
	key := testKey(tenant.Default, "metrics.parquet")
	other := testKey(tenant.Default, "metrics-other.parquet")
	st.put(key, bytes.Repeat([]byte("x"), 30))
	st.put(other, bytes.Repeat([]byte("y"), 30))
	st.failNext(failKey, errors.New("boom"))
	c := newTestCache(t, st, func(o *cacheOptions) {
		o.register = reg
		o.budget = 50
	})

	if _, _, err := c.Acquire(t.Context(), tenant.Default, failKey); err == nil {
		t.Fatal("failed download: no error")
	}
	_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel()
	_, rel, err = c.Acquire(t.Context(), tenant.Default, key) // hit
	if err != nil {
		t.Fatal(err)
	}
	rel()
	_, rel, err = c.Acquire(t.Context(), tenant.Default, other) // miss, evicts key
	if err != nil {
		t.Fatal(err)
	}
	rel()

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool, len(families))
	values := make(map[string]map[string]float64, len(families))
	for _, f := range families {
		names[f.GetName()] = true
		vals := make(map[string]float64, len(f.GetMetric()))
		for _, m := range f.GetMetric() {
			label := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" {
					label = l.GetValue()
				}
			}
			switch {
			case m.GetGauge() != nil:
				vals[label] = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				vals[label] = m.GetCounter().GetValue()
			}
		}
		values[f.GetName()] = vals
	}
	checks := []struct {
		name   string
		family string
		label  string
		want   float64
	}{
		{"requests hit", "parkana_querier_cache_requests_total", "hit", 1},
		{"requests miss", "parkana_querier_cache_requests_total", "miss", 3},
		{"downloads success", "parkana_querier_cache_downloads_total", "success", 2},
		{"downloads error", "parkana_querier_cache_downloads_total", "error", 1},
		{"evictions", "parkana_querier_cache_evictions_total", "", 1},
		{"leases", "parkana_querier_cache_leases", "", 0},
		{"entries", "parkana_querier_cache_entries", "", 1},
		{"bytes", "parkana_querier_cache_bytes", "", 30},
	}
	for _, check := range checks {
		got, ok := values[check.family][check.label]
		if !ok {
			t.Errorf("%s: metric %q{%q} not registered", check.name, check.family, check.label)
			continue
		}
		if got != check.want {
			t.Errorf("%s = %v, want %v", check.name, got, check.want)
		}
	}
	for _, want := range []string{
		"parkana_querier_cache_requests_total",
		"parkana_querier_cache_downloads_total",
		"parkana_querier_cache_evictions_total",
		"parkana_querier_cache_bytes",
		"parkana_querier_cache_entries",
		"parkana_querier_cache_leases",
	} {
		if !names[want] {
			t.Errorf("metric %q not registered", want)
		}
	}
}

func TestObjectCacheLocalAcquireAfterClose(t *testing.T) {
	storeDir := t.TempDir()
	st, err := objstore.NewFS(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	key := testKey(tenant.Default, "closed.parquet")
	if err := st.Put(t.Context(), key, strings.NewReader("closed"), 6); err != nil {
		t.Fatal(err)
	}
	c, err := newObjectCache(cacheOptions{
		store:  st,
		dir:    t.TempDir(),
		budget: 1 << 20,
		local:  true,
		logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Acquire(t.Context(), tenant.Default, key); !errors.Is(err, errCacheClosed) {
		t.Errorf("Acquire after Close = %v, want errCacheClosed", err)
	}
}

func TestObjectCacheLocalModeLeavesStoreUntouched(t *testing.T) {
	newStore := func(t *testing.T) (*countingLocal, string) {
		t.Helper()
		dir := t.TempDir()
		fsStore, err := objstore.NewFS(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fsStore.Close() })
		return &countingLocal{FS: fsStore}, dir
	}

	t.Run("cache dir equals store dir", func(t *testing.T) {
		st, storeDir := newStore(t)
		key := testKey(tenant.Default, "keep.parquet")
		data := []byte("object that must survive")
		if err := st.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		before := cacheFiles(t, storeDir)

		c, err := newObjectCache(cacheOptions{
			store:  st,
			dir:    storeDir,
			budget: 1,
			local:  true,
			logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if after := cacheFiles(t, storeDir); !slices.Equal(before, after) {
			t.Fatalf("store contents changed on construction: %v, want %v", after, before)
		}

		p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
		if err != nil {
			t.Fatalf("Acquire: %v (store object deleted by cache startup)", err)
		}
		defer rel()
		if got := readTestFile(t, p); !bytes.Equal(got, data) {
			t.Errorf("content = %q, want %q", got, data)
		}
		if got := st.gets.Load(); got != 0 {
			t.Errorf("remote Get calls = %d, want 0", got)
		}
		if after := cacheFiles(t, storeDir); !slices.Equal(before, after) {
			t.Errorf("store contents changed by Acquire: %v, want %v", after, before)
		}
	})

	t.Run("missing cache dir is not created", func(t *testing.T) {
		st, _ := newStore(t)
		key := testKey(tenant.Default, "nocache.parquet")
		if err := st.Put(t.Context(), key, strings.NewReader("data"), 4); err != nil {
			t.Fatal(err)
		}
		cacheDir := filepath.Join(t.TempDir(), "cache")
		c, err := newObjectCache(cacheOptions{
			store:  st,
			dir:    cacheDir,
			budget: 1,
			local:  true,
			logger: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if _, err := os.Stat(cacheDir); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("cache dir was created in local mode: %v", err)
		}
		if _, rel, err := c.Acquire(t.Context(), tenant.Default, key); err != nil {
			t.Errorf("Acquire: %v", err)
		} else {
			rel()
		}
	})
}

func TestObjectCacheCanceledDownloadNewCallerRetries(t *testing.T) {
	st := newMemStore()
	key := testKey(tenant.Default, "doomed.parquet")
	data := []byte("survives the doomed generation")
	st.put(key, data)
	reg := prometheus.NewRegistry()
	c := newTestCache(t, st, func(o *cacheOptions) { o.register = reg })
	g := st.blockDeaf(key)
	signals := make(chan struct{}, 8)
	c.afterWaiter = func() { signals <- struct{}{} }

	ctxA, cancelA := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() {
		_, _, err := c.Acquire(ctxA, tenant.Default, key)
		canceled <- err
	}()
	<-signals
	cancelA()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller error = %v, want context.Canceled", err)
	}

	// The canceled download is still blocked in the backend, so the next
	// healthy caller joins that doomed generation. It must end up with a fresh
	// download, not the other caller's context.Canceled.
	type result struct {
		path string
		rel  func()
		err  error
	}
	newCaller := make(chan result, 1)
	go func() {
		p, rel, err := c.Acquire(t.Context(), tenant.Default, key)
		newCaller <- result{path: p, rel: rel, err: err}
	}()
	<-signals
	close(g.release)

	res := <-newCaller
	if res.err != nil {
		t.Fatalf("new caller inherited a canceled download: %v", res.err)
	}
	defer res.rel()
	if got := readTestFile(t, res.path); !bytes.Equal(got, data) {
		t.Errorf("content = %q, want %q", got, data)
	}
	if got := st.getCount(key); got != 2 {
		t.Errorf("downloads = %d, want 2 (canceled generation plus retry)", got)
	}
	// Two acquisitions, counted once each: the retried generation is internal
	// and must not add a third request.
	if got := metricLabel(t, reg, "parkana_querier_cache_requests_total", "result", "miss"); got != 2 {
		t.Errorf("misses = %v, want 2", got)
	}
}

func TestObjectCacheRequestsCountedPerAcquire(t *testing.T) {
	reg := prometheus.NewRegistry()
	st := newMemStore()
	key := testKey(tenant.Default, "counted.parquet")
	data := bytes.Repeat([]byte("x"), 1024)
	st.put(key, data)
	c := newTestCache(t, st, func(o *cacheOptions) { o.register = reg })
	gate := st.block(key)
	signals := make(chan struct{}, 64)
	c.afterWaiter = func() { signals <- struct{}{} }

	const n = 16
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			rel()
		})
	}
	for range n {
		<-signals
	}
	close(gate.release)
	wg.Wait()

	if got := metricLabel(t, reg, "parkana_querier_cache_requests_total", "result", "miss"); got != n {
		t.Errorf("misses = %v, want %d (one per acquisition, not one per download)", got, n)
	}
	if got := metricLabel(t, reg, "parkana_querier_cache_requests_total", "result", "hit"); got != 0 {
		t.Errorf("hits = %v, want 0", got)
	}
	if got := metricLabel(t, reg, "parkana_querier_cache_downloads_total", "result", "success"); got != 1 {
		t.Errorf("downloads = %v, want 1", got)
	}
	if got := st.getCount(key); got != 1 {
		t.Errorf("backend gets = %d, want 1", got)
	}

	// A later acquisition is a hit, still counted exactly once.
	_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if got := metricLabel(t, reg, "parkana_querier_cache_requests_total", "result", "hit"); got != 1 {
		t.Errorf("hits after cached acquisition = %v, want 1", got)
	}
	if got := metricLabel(t, reg, "parkana_querier_cache_requests_total", "result", "miss"); got != n {
		t.Errorf("misses after cached acquisition = %v, want %d", got, n)
	}
}

func TestObjectCacheMetricsRollbackOnPartialRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	conflict := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_querier_cache_evictions_total",
		Help: "Cached objects evicted to stay within the byte budget.",
	})
	reg.MustRegister(conflict)
	st := newMemStore()

	_, err := newObjectCache(cacheOptions{
		store:    st,
		dir:      t.TempDir(),
		budget:   1024,
		register: reg,
		logger:   slog.New(slog.DiscardHandler),
	})
	if err == nil {
		t.Fatal("want a duplicate registration error")
	}
	reg.Unregister(conflict)

	c, err := newObjectCache(cacheOptions{
		store:    st,
		dir:      t.TempDir(),
		budget:   1024,
		register: reg,
		logger:   slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("registry not reusable after partially failed construction: %v", err)
	}
	t.Cleanup(func() { c.Close() })
}

func TestObjectCacheMetricsRollbackOnConstructorFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	reg := prometheus.NewRegistry()
	st := newMemStore()
	dir := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	_, err := newObjectCache(cacheOptions{
		store:    st,
		dir:      dir,
		budget:   1024,
		register: reg,
		logger:   slog.New(slog.DiscardHandler),
	})
	if err == nil {
		t.Fatal("want an error creating the temp dir in a read-only cache dir")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := newObjectCache(cacheOptions{
		store:    st,
		dir:      dir,
		budget:   1024,
		register: reg,
		logger:   slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("registry not reusable after constructor failure: %v", err)
	}
	t.Cleanup(func() { c.Close() })
}

func TestObjectCacheEvictionFailureKeepsAccounting(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	st := newMemStore()
	key := testKey(tenant.Default, "stuck.parquet")
	other := testKey(tenant.Default, "other.parquet")
	st.put(key, bytes.Repeat([]byte("s"), 100))
	st.put(other, bytes.Repeat([]byte("o"), 10))
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	c := newTestCache(t, st, func(o *cacheOptions) {
		o.dir = dir
		o.budget = 50
		o.register = reg
	})

	_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(filepath.Join(dir, filepath.FromSlash(key)))
	if err := os.Chmod(parent, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0o755) })
	rel() // eviction fails: the parent directory is read-only

	if files := cacheFiles(t, dir); len(files) != 1 || files[0] != key {
		t.Fatalf("file while removal fails = %v, want %q", files, key)
	}
	c.mu.Lock()
	used := c.used
	_, indexed := c.entries[key]
	c.mu.Unlock()
	if used != 100 {
		t.Errorf("used bytes = %d, want 100 while the file still exists", used)
	}
	if !indexed {
		t.Error("eviction dropped the entry although its file remains")
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_bytes"); got != 100 {
		t.Errorf("bytes gauge = %v, want 100", got)
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_evictions_total"); got != 0 {
		t.Errorf("evictions = %v, want 0 for a failed removal", got)
	}

	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	// A later acquisition must retry the removal and recover the budget.
	_, rel2, err := c.Acquire(t.Context(), tenant.Default, other)
	if err != nil {
		t.Fatal(err)
	}
	rel2()
	if _, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(key))); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stuck file was not retried and removed: %v", err)
	}
	if files := cacheFiles(t, dir); len(files) != 1 || files[0] != other {
		t.Errorf("files = %v, want only %q", files, other)
	}
	c.mu.Lock()
	used = c.used
	c.mu.Unlock()
	if used != 10 {
		t.Errorf("used bytes after recovery = %d, want 10", used)
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_evictions_total"); got != 1 {
		t.Errorf("evictions after recovery = %v, want 1", got)
	}
}

// failingReader delivers nothing and always fails: the tail of a partial copy.
type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// partialCopyStore hands out a reader that delivers the first part bytes of an
// object and then fails, so a download dies halfway through copying. Only Get
// differs from memStore.
type partialCopyStore struct {
	*memStore
	part int
}

func (s *partialCopyStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.memStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	rc.Close()
	data, _ := s.peek(key)
	return io.NopCloser(io.MultiReader(bytes.NewReader(data[:s.part]), failingReader{err: errors.New("connection reset")})), nil
}

// peek returns the stored bytes of key.
func (s *partialCopyStore) peek(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	return data, ok
}

// TestObjectCacheFetchedBytes pins the transferred-byte counter's definition:
// a cold download counts the object's serialized size, a warm hit counts
// nothing, a second object's download counts too, a Get that fails before any
// byte counts nothing, and a successful refetch after eviction counts the
// full size again — with the refetched file verified on disk.
func TestObjectCacheFetchedBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	st := newMemStore()
	const size, otherSize = 30, 10
	key := testKey(tenant.Default, "fetched.parquet")
	other := testKey(tenant.Default, "fetched-other.parquet")
	st.put(key, bytes.Repeat([]byte("x"), size))
	st.put(other, bytes.Repeat([]byte("o"), otherSize))
	// The budget fits only one of the two: acquiring other evicts key, so the
	// last acquisition really goes to the store.
	c := newTestCache(t, st, func(o *cacheOptions) { o.register = reg; o.budget = 35 })

	_, rel, err := c.Acquire(t.Context(), tenant.Default, key) // cold download
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != size {
		t.Fatalf("fetched bytes after the cold download = %v, want %d", got, size)
	}

	_, rel, err = c.Acquire(t.Context(), tenant.Default, key) // warm hit
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != size {
		t.Fatalf("fetched bytes after the warm hit = %v, still want %d", got, size)
	}

	_, rel, err = c.Acquire(t.Context(), tenant.Default, other) // evicts key
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != size+otherSize {
		t.Fatalf("fetched bytes after the second download = %v, want %d", got, size+otherSize)
	}

	st.failNext(key, errors.New("boom"))
	if _, _, err := c.Acquire(t.Context(), tenant.Default, key); err == nil {
		t.Fatal("failed download: no error")
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != size+otherSize {
		t.Fatalf("fetched bytes after a failed Get = %v, still want %d (no bytes crossed)", got, size+otherSize)
	}

	// The one-shot failure is gone: the evicted key downloads again, counts
	// its full size once more, and the cached file really holds the object.
	path, rel, err := c.Acquire(t.Context(), tenant.Default, key)
	if err != nil {
		t.Fatal(err)
	}
	if data := readTestFile(t, path); !bytes.Equal(data, bytes.Repeat([]byte("x"), size)) {
		t.Fatalf("refetched cache file = %q, want %d x's", data, size)
	}
	rel()
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != 2*size+otherSize {
		t.Fatalf("fetched bytes after the successful refetch = %v, want %d", got, 2*size+otherSize)
	}
}

// TestObjectCacheFetchedBytesPartialCopy checks a download that fails halfway
// still records the bytes that were transferred before the error.
func TestObjectCacheFetchedBytesPartialCopy(t *testing.T) {
	reg := prometheus.NewRegistry()
	const size, part = 40, 17
	st := &partialCopyStore{memStore: newMemStore(), part: part}
	key := testKey(tenant.Default, "partial.parquet")
	st.put(key, bytes.Repeat([]byte("y"), size))
	c := newTestCache(t, st, func(o *cacheOptions) { o.register = reg })

	if _, _, err := c.Acquire(t.Context(), tenant.Default, key); err == nil {
		t.Fatal("partial download: no error")
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != part {
		t.Fatalf("fetched bytes after the partial copy = %v, want %d", got, part)
	}
	// The failed download left no complete entry behind.
	if refs := c.refs(key); refs != 0 {
		t.Errorf("partial download left %d refs, want 0", refs)
	}
	if files := cacheFiles(t, c.dir); len(files) != 0 {
		t.Errorf("partial download left %v behind, want nothing", files)
	}
}

// TestObjectCacheFetchedBytesLocalExcluded checks the local-path fast path
// copies nothing and therefore counts nothing.
func TestObjectCacheFetchedBytesLocalExcluded(t *testing.T) {
	reg := prometheus.NewRegistry()
	ls := newLocalStore(t.TempDir())
	key := testKey(tenant.Default, "local.parquet")
	ls.put(key, bytes.Repeat([]byte("z"), 25))
	c := newTestCache(t, ls, func(o *cacheOptions) { o.register = reg; o.local = true })

	for range 2 {
		_, rel, err := c.Acquire(t.Context(), tenant.Default, key)
		if err != nil {
			t.Fatal(err)
		}
		rel()
	}
	if got := metricFloat(t, reg, "parkana_querier_cache_fetched_bytes_total"); got != 0 {
		t.Fatalf("fetched bytes after local-path acquisitions = %v, want 0", got)
	}
	if n := ls.calls(); n != 2 {
		t.Fatalf("LocalPath calls = %d, want 2", n)
	}
}
