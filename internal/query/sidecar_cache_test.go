package query

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// newSidecarTestCache builds a cache over s registered on a fresh registry,
// closed at test end.
func newSidecarTestCache(t *testing.T, s objstore.Store, budget int64) (*sidecarCache, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	c, err := newSidecarCache(sidecarCacheOptions{store: s, budget: budget, register: reg})
	if err != nil {
		t.Fatalf("newSidecarCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, reg
}

// buildSidecar encodes a valid sidecar for lines.
func buildSidecar(t *testing.T, lines ...string) []byte {
	t.Helper()
	b, err := index.NewBuilder(index.DefaultParams)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	for _, line := range lines {
		b.Add(line)
	}
	data, err := b.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data
}

// putSidecar stores a valid sidecar for lines under dataKey's sidecar key.
func putSidecar(t *testing.T, s *memStore, dataKey string, lines ...string) []byte {
	t.Helper()
	data := buildSidecar(t, lines...)
	s.put(objstore.SidecarKey(dataKey), data)
	return data
}

// newDefaultDataKey returns a fresh data key in the default tenant.
func newDefaultDataKey(stream string) string {
	return objstore.NewDataKey(tenant.Default, stream, time.Now().UTC())
}

// metricValue returns the current value of the single metric sample of name
// (with labels, if any) gathered from g.
func metricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := len(m.GetLabel()) == len(labels)
			for _, lp := range m.GetLabel() {
				if v, ok := labels[lp.GetName()]; !ok || v != lp.GetValue() {
					match = false
				}
			}
			if !match {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if gg := m.GetGauge(); gg != nil {
				return gg.GetValue()
			}
		}
	}
	t.Fatalf("metric %s%v not found", name, labels)
	return 0
}

// closeCountingStore wraps memStore so tests can prove every reader the cache
// opens is closed, and can fail reads and closes after the reader was handed
// out. Its gates are always deaf: a gated Get parks until release even after
// the fetch context ends, and then succeeds on a fresh context, which is how
// tests reproduce a store that ignores cancellation until a barrier.
type closeCountingStore struct {
	*memStore
	mu         sync.Mutex
	produced   int
	closed     int
	readErr    error // fails every Read at once
	partialErr error // fails Read once partialN bytes were delivered
	partialN   int
	closeErr   error // fails Close
	gates      map[string]*gate
}

func (s *closeCountingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	g := s.gates[key]
	readErr, partialErr, partialN, closeErr := s.readErr, s.partialErr, s.partialN, s.closeErr
	s.mu.Unlock()
	if g != nil {
		g.once.Do(func() { close(g.started) })
		<-g.release // deaf by design: ctx is ignored until the barrier opens
	}
	rc, err := s.memStore.Get(context.WithoutCancel(ctx), key)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.produced++
	s.mu.Unlock()
	return &countingReader{parent: s, rc: rc, readErr: readErr, partialErr: partialErr, partialN: partialN, closeErr: closeErr}, nil
}

func (s *closeCountingStore) counts() (produced, closed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.produced, s.closed
}

func (s *closeCountingStore) setReadErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readErr = err
}

// setPartialRead makes every reader deliver n bytes and then fail with err.
func (s *closeCountingStore) setPartialRead(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partialN, s.partialErr = n, err
}

func (s *closeCountingStore) setCloseErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeErr = err
}

// blockDeafFetch parks Gets of key on a barrier the test controls.
func (s *closeCountingStore) blockDeafFetch(key string) *gate {
	g := &gate{started: make(chan struct{}), release: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gates == nil {
		s.gates = make(map[string]*gate)
	}
	s.gates[key] = g
	return g
}

// countingReader counts Close calls and can fail reads and closes.
type countingReader struct {
	parent     *closeCountingStore
	rc         io.ReadCloser
	readErr    error
	partialErr error
	partialN   int
	closeErr   error
	delivered  int
	closed     bool
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	if r.partialErr != nil {
		if r.delivered >= r.partialN {
			return 0, r.partialErr
		}
		// Deliver exactly partialN bytes in total, then fail.
		p = p[:min(len(p), r.partialN-r.delivered)]
	}
	n, err := r.rc.Read(p)
	r.delivered += n
	if r.partialErr != nil && r.delivered >= r.partialN {
		return n, r.partialErr
	}
	return n, err
}

func (r *countingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.parent.mu.Lock()
	r.parent.closed++
	r.parent.mu.Unlock()
	if r.closeErr != nil {
		_ = r.rc.Close()
		return r.closeErr
	}
	return r.rc.Close()
}

// sidecarGetResult carries one asynchronous Get outcome.
type sidecarGetResult struct {
	filter *index.Filter
	err    error
}

// sidecarFetchState reads the in-flight fetch for key under the cache lock:
// whether one exists, how many callers are registered on it, and whether its
// context was canceled (the mark leaveFetch and Close leave on a generation).
func sidecarFetchState(c *sidecarCache, key string) (present bool, waiters int, canceled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.inflight[key]
	if f == nil {
		return false, 0, false
	}
	return true, f.waiters, f.ctx.Err() != nil
}

// startSidecarGet runs Get in the background and returns its result channel.
func startSidecarGet(ctx context.Context, c *sidecarCache, t tenant.ID, key string) <-chan sidecarGetResult {
	ch := make(chan sidecarGetResult, 1)
	go func() {
		filter, err := c.Get(ctx, t, key)
		ch <- sidecarGetResult{filter: filter, err: err}
	}()
	return ch
}

func TestSidecarCacheConstructor(t *testing.T) {
	if _, err := newSidecarCache(sidecarCacheOptions{budget: 1024}); err == nil {
		t.Fatal("expected an error for a missing store")
	}
	for _, budget := range []int64{0, -1} {
		if _, err := newSidecarCache(sidecarCacheOptions{store: newMemStore(), budget: budget}); err == nil {
			t.Fatalf("expected an error for budget %d", budget)
		}
	}
	// Nil logger and registerer must both be usable.
	c, err := newSidecarCache(sidecarCacheOptions{store: newMemStore(), budget: 1024})
	if err != nil {
		t.Fatalf("newSidecarCache: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSidecarCacheGetHitMiss(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	dataKey := newDefaultDataKey("app")
	data := putSidecar(t, s.memStore, dataKey,
		"error connecting to database",
		"timeout waiting for backend",
	)
	c, reg := newSidecarTestCache(t, s, 1<<20)

	f1, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !f1.MayContain("error") {
		t.Error("MayContain(error) = false, want true")
	}
	if f1.MayContain("ZZQQWVK") {
		t.Error("MayContain(ZZQQWVK) = true, want false")
	}
	f2, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if f1 != f2 {
		t.Error("second Get returned a different filter, want the cached one")
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 1 {
		t.Errorf("sidecar fetched %d times, want 1", n)
	}
	produced, closed := s.counts()
	if produced != 1 || closed != 1 {
		t.Errorf("readers produced=%d closed=%d, want 1 and 1", produced, closed)
	}

	size := sidecarAccountedSize(dataKey, data)
	checks := []struct {
		name   string
		got    float64
		want   float64
		labels map[string]string
	}{
		{"requests miss", metricValue(t, reg, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "miss"}), 1, nil},
		{"requests hit", metricValue(t, reg, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "hit"}), 1, nil},
		{"fetches success", metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "success"}), 1, nil},
		{"fetches fetch_error", metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}), 0, nil},
		{"fetches decode_error", metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "decode_error"}), 0, nil},
		{"evictions", metricValue(t, reg, "parkana_querier_sidecar_cache_evictions_total", nil), 0, nil},
		{"fetched bytes", metricValue(t, reg, "parkana_querier_sidecar_cache_fetched_bytes_total", nil), float64(len(data)), nil},
		{"resident bytes", metricValue(t, reg, "parkana_querier_sidecar_cache_bytes", nil), float64(size), nil},
		{"entries", metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil), 1, nil},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}

func TestSidecarCacheValidation(t *testing.T) {
	s := newMemStore()
	c, _ := newSidecarTestCache(t, s, 1<<20)
	other := tenant.ID("acme")
	valid := "default/logs/app/2026/11/04/10/018f0000-0000-7000-8000-000000000001.parquet"

	for _, tc := range []struct {
		name    string
		tenant  tenant.ID
		key     string
		wantErr string
	}{
		{"empty tenant", "", valid, "tenant"},
		{"bad tenant chars", "acme!", valid, "tenant"},
		{"key of another tenant", other, valid, "not in tenant"},
		{"empty key", tenant.Default, "", "invalid key"},
		{"absolute key", tenant.Default, "/default/logs/x.parquet", "invalid key"},
		{"dotdot segment", tenant.Default, "default/logs/../x.parquet", "invalid key"},
		{"backslash", tenant.Default, `default\logs\x.parquet`, "invalid key"},
		{"sidecar key", tenant.Default, "default/logs/app/2026/11/04/10/018f0000-0000-7000-8000-000000000002.idx", "not a data object"},
		{"bare key", tenant.Default, "default.parquet", "not in tenant"},
	} {
		_, err := c.Get(t.Context(), tc.tenant, tc.key)
		if err == nil {
			t.Errorf("%s: Get succeeded, want error", tc.name)
			continue
		}
		if !bytes.Contains([]byte(err.Error()), []byte(tc.wantErr)) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.wantErr)
		}
	}
	if n := s.totalGets(); n != 0 {
		t.Errorf("store saw %d Gets during validation, want 0", n)
	}
}

func TestSidecarCacheSingleFlight(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	otherKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	putSidecar(t, s, otherKey, "timeout waiting for backend")

	c, _ := newSidecarTestCache(t, s, 1<<20)

	// Warm a second key so the lock-not-held-during-I/O check below has a
	// cached entry to hit while the first fetch is parked inside the store.
	warm, err := c.Get(t.Context(), tenant.Default, otherKey)
	if err != nil {
		t.Fatalf("warm Get: %v", err)
	}

	g := s.block(objstore.SidecarKey(dataKey))
	release := sync.OnceFunc(func() { close(g.release) })
	defer release()
	first := startSidecarGet(t.Context(), c, tenant.Default, dataKey)
	<-g.started

	// While the fetch is parked inside the store, a Get for the cached key
	// must return: the cache lock is not held across I/O.
	cached := startSidecarGet(t.Context(), c, tenant.Default, otherKey)
	select {
	case r := <-cached:
		if r.err != nil {
			t.Fatalf("Get of cached key during fetch: %v", r.err)
		}
		if r.filter != warm {
			t.Error("Get of cached key returned a different filter")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get of a cached key blocked while another fetch was in flight")
	}

	const joiners = 8
	results := make([]<-chan sidecarGetResult, joiners)
	joined := make(chan struct{}, joiners)
	c.afterWaiter = func() { joined <- struct{}{} }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for i := range joiners {
		results[i] = startSidecarGet(ctx, c, tenant.Default, dataKey)
	}
	// Every joiner must register on the one in-flight fetch before the gate
	// opens; otherwise a late joiner could pass as a cache hit and the test
	// would stop exercising single-flight.
	for range joiners {
		<-joined
	}
	present, waiters, canceled := sidecarFetchState(c, dataKey)
	if !present || canceled {
		t.Fatalf("in-flight fetch present=%v canceled=%v, want present and running", present, canceled)
	}
	if waiters != joiners+1 {
		t.Fatalf("waiters = %d, want %d (starter plus every joiner)", waiters, joiners+1)
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 1 {
		t.Fatalf("sidecar fetched %d times before release, want 1", n)
	}
	release()

	r := <-first
	if r.err != nil {
		t.Fatalf("first Get: %v", r.err)
	}
	for i, ch := range results {
		res := <-ch
		if res.err != nil {
			t.Fatalf("joiner %d: %v", i, res.err)
		}
		if res.filter != r.filter {
			t.Errorf("joiner %d decoded a different filter, want the shared one", i)
		}
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 1 {
		t.Errorf("sidecar fetched %d times, want 1", n)
	}
}

func TestSidecarCacheLRUEviction(t *testing.T) {
	s := newMemStore()
	keys := make([]string, 3)
	sizes := make([]int64, 3)
	for i := range keys {
		keys[i] = newDefaultDataKey("app")
		data := putSidecar(t, s, keys[i], "error connecting to database")
		sizes[i] = sidecarAccountedSize(keys[i], data)
	}
	// The budget holds all three minus one byte. k1 and k2 are admitted
	// first; then a hit on k1 makes k2 the least recently used, so admitting
	// k3 must evict k2, not the earlier-inserted k1: LRU, not FIFO.
	budget := sizes[0] + sizes[1] + sizes[2] - 1
	c, reg := newSidecarTestCache(t, s, budget)
	sidecarKeys := make([]string, 3)
	for i := range keys {
		sidecarKeys[i] = objstore.SidecarKey(keys[i])
	}

	for _, key := range keys[:2] {
		if _, err := c.Get(t.Context(), tenant.Default, key); err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
	}
	// Touch k1: it becomes the most recently used entry.
	if _, err := c.Get(t.Context(), tenant.Default, keys[0]); err != nil {
		t.Fatalf("hit %s: %v", keys[0], err)
	}
	// Admitting k3 exceeds the budget by one byte and evicts k2.
	if _, err := c.Get(t.Context(), tenant.Default, keys[2]); err != nil {
		t.Fatalf("Get %s: %v", keys[2], err)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_evictions_total", nil); got != 1 {
		t.Fatalf("evictions = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 2 {
		t.Fatalf("entries = %v, want 2", got)
	}

	// k1 survived (a FIFO cache would have dropped it): still a hit.
	if _, err := c.Get(t.Context(), tenant.Default, keys[0]); err != nil {
		t.Fatalf("Get %s after the eviction round: %v", keys[0], err)
	}
	if n := s.getCount(sidecarKeys[0]); n != 1 {
		t.Errorf("key0 sidecar read %d times, want 1 (the touched entry was not evicted)", n)
	}
	// k2 was the victim: getting it refetches, and that readmission evicts
	// k3, the least recently used by then.
	if _, err := c.Get(t.Context(), tenant.Default, keys[1]); err != nil {
		t.Fatalf("Get %s after its eviction: %v", keys[1], err)
	}
	if n := s.getCount(sidecarKeys[1]); n != 2 {
		t.Errorf("key1 sidecar read %d times, want 2 (the untouched entry was evicted)", n)
	}
	if n := s.getCount(sidecarKeys[2]); n != 1 {
		t.Errorf("key2 sidecar read %d times, want 1 (evicted silently by the readmission)", n)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_evictions_total", nil); got != 2 {
		t.Errorf("evictions = %v, want 2", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_bytes", nil); got != float64(sizes[0]+sizes[1]) {
		t.Errorf("resident bytes = %v, want %v", got, sizes[0]+sizes[1])
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 2 {
		t.Errorf("entries = %v, want 2", got)
	}
}

func TestSidecarCacheOversizedEntry(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	// A positive budget smaller than the entry's accounted size.
	c, reg := newSidecarTestCache(t, s, 1)

	f, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err != nil {
		t.Fatalf("Get with a too-small budget: %v", err)
	}
	if !f.MayContain("error") {
		t.Error("oversized filter lost its needle, want MayContain(error) = true")
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries = %v, want 0 (oversized entry must not be admitted)", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_bytes", nil); got != 0 {
		t.Errorf("resident bytes = %v, want 0", got)
	}
	// Nothing was admitted, so the next Get refetches.
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 2 {
		t.Errorf("sidecar read %d times, want 2", n)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "miss"}); got != 2 {
		t.Errorf("misses = %v, want 2", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_evictions_total", nil); got != 0 {
		t.Errorf("evictions = %v, want 0", got)
	}
}

func TestSidecarCacheInputBound(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s.memStore, dataKey, "error connecting to database")
	c, reg := newSidecarTestCache(t, s, 1<<20)
	c.maxInput = 32 // far below any valid sidecar

	_, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err == nil {
		t.Fatal("Get succeeded past the read bound, want an error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("read bound")) {
		t.Errorf("error %q does not mention the read bound", err)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "oversized"}); got != 1 {
		t.Errorf("oversized fetches = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries = %v, want 0", got)
	}
	if produced, closed := s.counts(); produced != 1 || closed != 1 {
		t.Errorf("readers produced=%d closed=%d, want 1 and 1", produced, closed)
	}

	// The same store and cache succeed once the bound covers the sidecar.
	c.maxInput = 0
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); err != nil {
		t.Fatalf("Get with the default bound: %v", err)
	}
}

func TestSidecarCacheTenantIsolation(t *testing.T) {
	s := newMemStore()
	acme, zorg := tenant.ID("acme"), tenant.ID("zorg")
	// Same stream, timestamp and uuid: the keys differ only by tenant, so a
	// cache keyed on the tenant-stripped rest would conflate them.
	ts := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	id := uuid.NewV7()
	acmeKey := objstore.DataKey(acme, "app", ts, id)
	zorgKey := objstore.DataKey(zorg, "app", ts, id)
	if acmeKey[len(acme):] != zorgKey[len(zorg):] {
		t.Fatalf("test keys differ below the tenant prefix: %q vs %q", acmeKey, zorgKey)
	}
	putSidecar(t, s, acmeKey, "acme error connecting to database")
	putSidecar(t, s, zorgKey, "zorg timeout waiting for backend")

	c, reg := newSidecarTestCache(t, s, 1<<20)
	if _, err := c.Get(t.Context(), zorg, acmeKey); err == nil {
		t.Fatal("Get of acme's key under tenant zorg succeeded, want an error")
	}
	if n := s.totalGets(); n != 0 {
		t.Errorf("store saw %d Gets, want 0: the mismatch must be rejected before I/O", n)
	}

	fa, err := c.Get(t.Context(), acme, acmeKey)
	if err != nil {
		t.Fatalf("Get acme: %v", err)
	}
	fz, err := c.Get(t.Context(), zorg, zorgKey)
	if err != nil {
		t.Fatalf("Get zorg: %v", err)
	}
	if !fa.MayContain("acme") || fa.MayContain("zorg") {
		t.Error("acme filter answers for the wrong tenant's needle")
	}
	if !fz.MayContain("zorg") || fz.MayContain("acme") {
		t.Error("zorg filter answers for the wrong tenant's needle")
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 2 {
		t.Errorf("entries = %v, want 2 (one per tenant)", got)
	}
}

func TestSidecarCacheFetchErrorRetry(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	sidecarKey := objstore.SidecarKey(dataKey)
	putSidecar(t, s, dataKey, "error connecting to database")
	c, reg := newSidecarTestCache(t, s, 1<<20)

	boom := errors.New("disk on fire")
	s.failNext(sidecarKey, boom)
	_, err := c.Get(t.Context(), tenant.Default, dataKey)
	if !errors.Is(err, boom) {
		t.Fatalf("Get error = %v, want it to wrap %v", err, boom)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}); got != 1 {
		t.Errorf("fetch errors = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries after a failed fetch = %v, want 0", got)
	}

	// No negative cache: the retry after the transient error succeeds and
	// admits the entry.
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); err != nil {
		t.Fatalf("retry Get: %v", err)
	}
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); err != nil {
		t.Fatalf("third Get: %v", err)
	}
	if n := s.getCount(sidecarKey); n != 2 {
		t.Errorf("sidecar read %d times, want 2 (failure not cached, success cached)", n)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_requests_total", map[string]string{"result": "hit"}); got != 1 {
		t.Errorf("hits = %v, want 1", got)
	}
}

// readFailingStore proves a reader that fails mid-stream is closed and the
// failure is reported as a fetch error without caching anything.
func TestSidecarCacheReadError(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s.memStore, dataKey, "error connecting to database")
	c, reg := newSidecarTestCache(t, s, 1<<20)

	s.setReadErr(errors.New("short read"))
	_, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err == nil {
		t.Fatal("Get succeeded despite a failing reader, want an error")
	}
	if produced, closed := s.counts(); produced != 1 || closed != 1 {
		t.Errorf("readers produced=%d closed=%d, want 1 and 1", produced, closed)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}); got != 1 {
		t.Errorf("fetch errors = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries = %v, want 0", got)
	}

	// Recovering the reader makes the next Get succeed.
	s.setReadErr(nil)
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); err != nil {
		t.Fatalf("Get after reader recovery: %v", err)
	}
}

// rawSidecar assembles a CRC-correct sidecar with an arbitrary bloom header
// and payload, following the frozen version 1 layout of index.Encode: magic,
// version, gram length, two reserved bytes, bloom's m and k words, the
// bitset length word, the bitset words, CRC32 of everything before it.
func rawSidecar(t *testing.T, m, k, bits uint64, words []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write([]byte{'P', 'K', 'I', 'X', index.Version, 4, 0, 0})
	for _, v := range []uint64{m, k, bits} {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			t.Fatalf("write bloom header: %v", err)
		}
	}
	buf.Write(words)
	return binary.BigEndian.AppendUint32(buf.Bytes(), crc32.ChecksumIEEE(buf.Bytes()))
}

func TestSidecarCacheCorruptSidecar(t *testing.T) {
	valid := buildSidecar(t, "error connecting to database")
	badMagic := bytes.Clone(valid)
	badMagic[0] = 'X'
	badMagic = binary.BigEndian.AppendUint32(badMagic[:len(badMagic)-4], crc32.ChecksumIEEE(badMagic[:len(badMagic)-4]))
	// A CRC-correct header declaring a 1 MiB bitset but carrying no words:
	// the decode must fail rather than trust the declared size.
	shortPayload := rawSidecar(t, 1<<20, 4, 1<<20, nil)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"garbage", []byte("not a sidecar at all")},
		{"truncated", valid[:len(valid)-1]},
		{"trailing junk", append(bytes.Clone(valid), 0)},
		{"bad magic", badMagic},
		{"declared bitset without payload", shortPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newMemStore()
			dataKey := newDefaultDataKey("app")
			s.put(objstore.SidecarKey(dataKey), tc.data)
			c, reg := newSidecarTestCache(t, s, 1<<20)

			_, err := c.Get(t.Context(), tenant.Default, dataKey)
			if !errors.Is(err, index.ErrFormat) {
				t.Fatalf("Get error = %v, want it to wrap %v", err, index.ErrFormat)
			}
			if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "decode_error"}); got != 1 {
				t.Errorf("decode errors = %v, want 1", got)
			}
			if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
				t.Errorf("entries = %v, want 0", got)
			}

			// The corrupt result is not cached: repairing the object makes
			// the very same key decode on the next Get.
			s.put(objstore.SidecarKey(dataKey), valid)
			f, err := c.Get(t.Context(), tenant.Default, dataKey)
			if err != nil {
				t.Fatalf("Get after repair: %v", err)
			}
			if !f.MayContain("error") {
				t.Error("repaired filter lost its needle")
			}
		})
	}
}

// TestSidecarCacheMalformedBloomHeader pins the behavior against the
// foundation fix (fac5ba0b) that validates a sidecar's bloom header before
// decoding: a CRC-correct sidecar declaring m=0 must be rejected by Decode,
// not handed out as a filter whose MayContain divides by zero. On the
// pre-fix baseline index.Decode accepts it, so this test fails there; it is
// green on the assembled branch with the fixed foundation.
func TestSidecarCacheMalformedBloomHeader(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	s.put(objstore.SidecarKey(dataKey), rawSidecar(t, 0, 4, 0, nil))
	c, _ := newSidecarTestCache(t, s, 1<<20)

	if _, err := c.Get(t.Context(), tenant.Default, dataKey); !errors.Is(err, index.ErrFormat) {
		t.Errorf("Get error = %v, want Decode to reject a zero-bit bloom as %v", err, index.ErrFormat)
	}
}

func TestSidecarCacheWaiterCancel(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	c, _ := newSidecarTestCache(t, s, 1<<20)

	g := s.block(objstore.SidecarKey(dataKey))
	release := sync.OnceFunc(func() { close(g.release) })
	defer release()
	stayCtx, stayCancel := context.WithCancel(t.Context())
	defer stayCancel()
	staying := startSidecarGet(stayCtx, c, tenant.Default, dataKey)
	<-g.started

	goneCtx, goneCancel := context.WithCancel(t.Context())
	joined := make(chan struct{}, 1)
	c.afterWaiter = func() { joined <- struct{}{} }
	leaving := startSidecarGet(goneCtx, c, tenant.Default, dataKey)
	// The departing waiter must be registered on the shared fetch before its
	// context ends, or it might return without ever exercising leaveFetch.
	<-joined
	present, waiters, canceled := sidecarFetchState(c, dataKey)
	if !present || canceled || waiters != 2 {
		t.Fatalf("fetch state before cancel: present=%v canceled=%v waiters=%d, want present, running, 2", present, canceled, waiters)
	}
	goneCancel()

	select {
	case r := <-leaving:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("departing waiter error = %v, want context.Canceled", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("departing waiter did not return after its context ended")
	}
	// One departure out of two must not cancel the fetch for the remaining
	// waiter.
	present, waiters, canceled = sidecarFetchState(c, dataKey)
	if !present || canceled || waiters != 1 {
		t.Fatalf("fetch state after one departure: present=%v canceled=%v waiters=%d, want present, running, 1", present, canceled, waiters)
	}

	// The remaining waiter still owns the fetch: releasing the store must
	// deliver the filter to it, with exactly one read.
	release()
	r := <-staying
	if r.err != nil {
		t.Fatalf("remaining waiter: %v", r.err)
	}
	if !r.filter.MayContain("error") {
		t.Error("filter lost its needle")
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 1 {
		t.Errorf("sidecar read %d times, want 1", n)
	}
}

// TestSidecarCacheJoinCanceledFetch covers the hand-off window: the last
// waiter's departure cancels the shared fetch, but a deaf store parks that
// fetch in flight, so the next caller joins the canceled generation, gets its
// cancellation, and retries with a fresh fetch.
func TestSidecarCacheJoinCanceledFetch(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	c, _ := newSidecarTestCache(t, s, 1<<20)

	g := s.blockDeaf(objstore.SidecarKey(dataKey))
	release := sync.OnceFunc(func() { close(g.release) })
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	first := startSidecarGet(ctx, c, tenant.Default, dataKey)
	<-g.started
	cancel()

	select {
	case r := <-first:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("departing waiter error = %v, want context.Canceled", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("departing waiter did not return after its context ended")
	}

	// The canceled fetch is still parked in the store; a healthy caller
	// joins it and, once the store lets it fail, retries a fresh fetch. The
	// join must happen before the release, or the caller could start a fresh
	// fetch directly and skip the canceled generation entirely.
	joined := make(chan struct{}, 1)
	c.afterWaiter = func() { joined <- struct{}{} }
	second := startSidecarGet(t.Context(), c, tenant.Default, dataKey)
	<-joined
	present, waiters, canceled := sidecarFetchState(c, dataKey)
	if !present || !canceled || waiters != 1 {
		t.Fatalf("fetch state after the healthy caller joined: present=%v canceled=%v waiters=%d, want the canceled generation with 1 waiter", present, canceled, waiters)
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 1 {
		t.Fatalf("sidecar fetched %d times before release, want 1 (no fresh fetch yet)", n)
	}
	release()
	r := <-second
	if r.err != nil {
		t.Fatalf("joining Get: %v", r.err)
	}
	if !r.filter.MayContain("error") {
		t.Error("filter lost its needle")
	}
	if n := s.getCount(objstore.SidecarKey(dataKey)); n != 2 {
		t.Errorf("sidecar read %d times, want 2 (canceled generation plus retry)", n)
	}
}

func TestSidecarCacheClose(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	reg := prometheus.NewRegistry()
	c, err := newSidecarCache(sidecarCacheOptions{store: s, budget: 1 << 20, register: reg})
	if err != nil {
		t.Fatalf("newSidecarCache: %v", err)
	}

	g := s.block(objstore.SidecarKey(dataKey))
	waiter := startSidecarGet(t.Context(), c, tenant.Default, dataKey)
	<-g.started

	// Close cancels the parked fetch, so it returns without the gate being
	// released and the waiter learns the cache is closed.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r := <-waiter
	if !errors.Is(r.err, errSidecarCacheClosed) {
		t.Errorf("waiter error = %v, want %v", r.err, errSidecarCacheClosed)
	}
	if _, err := c.Get(t.Context(), tenant.Default, dataKey); !errors.Is(err, errSidecarCacheClosed) {
		t.Errorf("Get after Close = %v, want %v", err, errSidecarCacheClosed)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if families, err := reg.Gather(); err != nil || len(families) != 0 {
		t.Errorf("registry after Close holds %d families (err %v), want 0", len(families), err)
	}

	// The registerer is reusable after Close unregistered the collectors.
	if _, err := newSidecarCache(sidecarCacheOptions{store: s, budget: 1 << 20, register: reg}); err != nil {
		t.Errorf("rebuilding on the same registry after Close: %v", err)
	}
}

func TestSidecarCacheMetricsRollback(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Pre-register a collector whose name the cache registers fifth, so the
	// construction fails partway through and must roll back the first four.
	if err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "parkana_querier_sidecar_cache_evictions_total",
		Help: "occupied",
	})); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	if _, err := newSidecarCache(sidecarCacheOptions{store: newMemStore(), budget: 1024, register: reg}); err == nil {
		t.Fatal("construction with a colliding metric name succeeded, want an error")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(families) != 1 || families[0].GetName() != "parkana_querier_sidecar_cache_evictions_total" {
		var names []string
		for _, mf := range families {
			names = append(names, mf.GetName())
		}
		t.Fatalf("registry holds %v after the failed construction, want only the pre-registered family", names)
	}
}

// TestSidecarCacheCloseDuringDeafFetch pins shutdown against a store that
// ignores cancellation until a barrier and then succeeds: Close must tear
// down without the barrier, drop the resident set, keep a result completed
// after shutdown out of the cache, and make concurrent Close callers all
// wait for teardown. A second deaf fetch keeps teardown parked while the
// first fetch's result is published, so the no-admission check below reads
// gauges that are still registered.
func TestSidecarCacheCloseDuringDeafFetch(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	warmKey := newDefaultDataKey("app")
	lateKey := newDefaultDataKey("app")
	heldKey := newDefaultDataKey("app")
	putSidecar(t, s.memStore, warmKey, "warm error connecting to database")
	putSidecar(t, s.memStore, lateKey, "late error connecting to database")
	putSidecar(t, s.memStore, heldKey, "held error connecting to database")
	reg := prometheus.NewRegistry()
	c, err := newSidecarCache(sidecarCacheOptions{store: s, budget: 1 << 20, register: reg})
	if err != nil {
		t.Fatalf("newSidecarCache: %v", err)
	}
	if _, err := c.Get(t.Context(), tenant.Default, warmKey); err != nil {
		t.Fatalf("warm Get: %v", err)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 1 {
		t.Fatalf("entries before Close = %v, want 1", got)
	}

	lateGate := s.blockDeafFetch(objstore.SidecarKey(lateKey))
	heldGate := s.blockDeafFetch(objstore.SidecarKey(heldKey))
	releaseLate := sync.OnceFunc(func() { close(lateGate.release) })
	releaseHeld := sync.OnceFunc(func() { close(heldGate.release) })
	defer releaseLate()
	defer releaseHeld()
	lateWaiter := startSidecarGet(t.Context(), c, tenant.Default, lateKey)
	heldWaiter := startSidecarGet(t.Context(), c, tenant.Default, heldKey)
	<-lateGate.started
	<-heldGate.started
	// Registration precedes the parked store call, so both fetch contexts
	// are observable now and are immutable from here on.
	c.mu.Lock()
	lateFetch := c.inflight[lateKey]
	heldFetch := c.inflight[heldKey]
	c.mu.Unlock()
	if lateFetch == nil || heldFetch == nil {
		t.Fatalf("in-flight fetches missing: late=%v held=%v", lateFetch != nil, heldFetch != nil)
	}

	closeDone := make(chan error, 2)
	for range 2 {
		go func() { closeDone <- c.Close() }()
	}
	// Shutdown observably began: Close canceled both fetch contexts.
	<-lateFetch.ctx.Done()
	<-heldFetch.ctx.Done()
	// Close cancels and drops the resident set inside one locked section, so
	// acquiring the lock after the cancels observes the cleared set — no
	// scheduling window.
	c.mu.Lock()
	cleared := len(c.entries) == 0 && c.used == 0
	c.mu.Unlock()
	if !cleared {
		t.Fatal("resident set was not dropped when Close began")
	}
	// Teardown is still parked on the held fetch, so the gauges are
	// registered and must already read empty.
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries after Close began = %v, want 0", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_bytes", nil); got != 0 {
		t.Errorf("resident bytes after Close began = %v, want 0", got)
	}
	// Neither Close may return while the deaf fetches still park in the
	// store: their release barriers below are the only things that can end
	// teardown.
	select {
	case err := <-closeDone:
		t.Fatalf("a concurrent Close returned %v before teardown finished", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Let the ignored-cancellation late fetch complete successfully: its
	// result is served to the waiter but must not be admitted. Teardown is
	// still parked on the held fetch, so the gauges are observable.
	releaseLate()
	r := <-lateWaiter
	if r.err != nil {
		t.Fatalf("late waiter after release: %v", r.err)
	}
	if !r.filter.MayContain("late") {
		t.Error("late filter lost its needle")
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries after a fetch completed post-Close = %v, want 0", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_bytes", nil); got != 0 {
		t.Errorf("resident bytes after a fetch completed post-Close = %v, want 0", got)
	}

	releaseHeld()
	if r := <-heldWaiter; r.err != nil {
		t.Fatalf("held waiter after release: %v", r.err)
	}
	for i := range 2 {
		if err := <-closeDone; err != nil {
			t.Errorf("Close %d: %v", i, err)
		}
	}
	if _, err := c.Get(t.Context(), tenant.Default, warmKey); !errors.Is(err, errSidecarCacheClosed) {
		t.Errorf("Get after Close = %v, want %v", err, errSidecarCacheClosed)
	}
	if families, err := reg.Gather(); err != nil || len(families) != 0 {
		t.Errorf("registry after Close holds %d families (err %v), want 0", len(families), err)
	}
}

// TestSidecarCacheCloseError proves a reader that fails to close fails the
// fetch instead of being admitted, and that a simultaneous read failure keeps
// its cause alongside the close failure.
func TestSidecarCacheCloseError(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s.memStore, dataKey, "error connecting to database")
	c, reg := newSidecarTestCache(t, s, 1<<20)

	closeBoom := errors.New("close failed")
	s.setCloseErr(closeBoom)
	_, err := c.Get(t.Context(), tenant.Default, dataKey)
	if !errors.Is(err, closeBoom) {
		t.Fatalf("Get error = %v, want it to wrap the close failure %v", err, closeBoom)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}); got != 1 {
		t.Errorf("fetch errors = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries = %v, want 0 (a failed close must not admit)", got)
	}

	// Both a partial read failure and a close failure keep their causes.
	readBoom := errors.New("short read")
	s.setPartialRead(8, readBoom)
	_, err = c.Get(t.Context(), tenant.Default, dataKey)
	if !errors.Is(err, closeBoom) || !errors.Is(err, readBoom) {
		t.Fatalf("Get error = %v, want it to wrap both %v and %v", err, readBoom, closeBoom)
	}
}

// TestSidecarCachePartialReadBytes proves the bytes a failing read did
// transfer are accounted as fetched before the error is tested.
func TestSidecarCachePartialReadBytes(t *testing.T) {
	s := &closeCountingStore{memStore: newMemStore()}
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s.memStore, dataKey, "error connecting to database")
	c, reg := newSidecarTestCache(t, s, 1<<20)

	const partial = 9
	s.setPartialRead(partial, errors.New("short read"))
	_, err := c.Get(t.Context(), tenant.Default, dataKey)
	if err == nil {
		t.Fatal("Get succeeded despite a partial read, want an error")
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetched_bytes_total", nil); got != partial {
		t.Errorf("fetched bytes = %v, want %d (partial transfer counted)", got, partial)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_fetches_total", map[string]string{"result": "fetch_error"}); got != 1 {
		t.Errorf("fetch errors = %v, want 1", got)
	}
	if got := metricValue(t, reg, "parkana_querier_sidecar_cache_entries", nil); got != 0 {
		t.Errorf("entries = %v, want 0", got)
	}
}

func TestSidecarCacheMetricsLabels(t *testing.T) {
	s := newMemStore()
	dataKey := newDefaultDataKey("app")
	putSidecar(t, s, dataKey, "error connecting to database")
	_, reg := newSidecarTestCache(t, s, 1<<20)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	bounded := map[string]map[string]bool{
		"parkana_querier_sidecar_cache_requests_total": {"hit": true, "miss": true},
		"parkana_querier_sidecar_cache_fetches_total":  {"success": true, "fetch_error": true, "decode_error": true, "oversized": true},
	}
	seen := map[string]bool{}
	for _, mf := range families {
		want, ok := bounded[mf.GetName()]
		if !ok {
			continue
		}
		seen[mf.GetName()] = true
		if len(mf.GetMetric()) != len(want) {
			t.Errorf("%s exposes %d series, want %d", mf.GetName(), len(mf.GetMetric()), len(want))
		}
		for _, m := range mf.GetMetric() {
			if len(m.GetLabel()) != 1 || m.GetLabel()[0].GetName() != "result" || !want[m.GetLabel()[0].GetValue()] {
				t.Errorf("%s exposes a series outside the fixed result set: %v", mf.GetName(), m.GetLabel())
			}
		}
	}
	for name := range bounded {
		if !seen[name] {
			t.Errorf("%s missing from the registry; every fixed result must be exposed from the start", name)
		}
	}
}
