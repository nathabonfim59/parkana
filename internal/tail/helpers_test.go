package tail

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fakeClock is a deterministic per-test clock. Advance fires every waiter
// whose deadline has passed.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
			continue
		}
		kept = append(kept, w)
	}
	c.waiters = kept
}

// decodedTail is the public Loki wire shape as a client sees it.
type decodedTail struct {
	Streams []struct {
		Stream map[string]string `json:"stream"`
		Values []jsontext.Value  `json:"values"`
	} `json:"streams"`
	Dropped []struct {
		Timestamp string            `json:"timestamp"`
		Labels    map[string]string `json:"labels"`
	} `json:"dropped_entries"`
}

type decodedEntry struct {
	ts       string
	line     string
	labels   map[string]string
	metadata map[string]string
	parsed   map[string]string
}

func (m decodedTail) entries(t *testing.T) []decodedEntry {
	t.Helper()
	var out []decodedEntry
	for _, s := range m.Streams {
		for _, raw := range s.Values {
			var parts []jsontext.Value
			if err := json.Unmarshal(raw, &parts); err != nil {
				t.Fatalf("decode tail tuple %s: %v", raw, err)
			}
			switch len(parts) {
			case 2, 3:
			default:
				t.Fatalf("tail tuple has %d elements, want 2 or 3: %s", len(parts), raw)
			}
			e := decodedEntry{labels: s.Stream}
			if err := json.Unmarshal(parts[0], &e.ts); err != nil {
				t.Fatalf("decode timestamp %s: %v", parts[0], err)
			}
			if err := json.Unmarshal(parts[1], &e.line); err != nil {
				t.Fatalf("decode line %s: %v", parts[1], err)
			}
			if len(parts) == 3 {
				var cats struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				if err := json.Unmarshal(parts[2], &cats); err != nil {
					t.Fatalf("decode categories %s: %v", parts[2], err)
				}
				e.metadata, e.parsed = cats.StructuredMetadata, cats.Parsed
			}
			out = append(out, e)
		}
	}
	return out
}

func marshalRecord(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func readyRecord(id tenant.ID) []byte {
	return mustJSON(wire.Message{Type: wire.TypeReady, Tenant: string(id), BufferEntries: 16})
}

func wireEntry(ts time.Time, line string, labels map[string]string, meta map[string]string) wire.Entry {
	return wire.Entry{
		Timestamp: strconv.FormatInt(ts.UnixNano(), 10),
		Line:      line,
		Labels:    wireLabelsFromMap(labels),
		Metadata:  wireLabelsFromMap(meta),
	}
}

func wireLabelsFromMap(m map[string]string) []wire.Label {
	ls := make([]wire.Label, 0, len(m))
	for k, v := range m {
		ls = append(ls, wire.Label{Name: k, Value: v})
	}
	// The wire contract is sorted by name.
	for i := 1; i < len(ls); i++ {
		for j := i; j > 0 && ls[j].Name < ls[j-1].Name; j-- {
			ls[j], ls[j-1] = ls[j-1], ls[j]
		}
	}
	return ls
}

// fakeIngesterOptions configures one NDJSON test ingester.
type fakeIngesterOptions struct {
	// status, when non-zero, makes the handler fail with that HTTP status.
	status int
	// failAfter, when set with status, delays the failure until it is
	// closed (or the request ends), so a test can order it after another
	// ingester's subscription.
	failAfter <-chan struct{}
	// ready builds the first record; nil means a correct ready for the
	// request tenant.
	ready func(id tenant.ID) []byte
	// rawFirst is written instead of a ready record (oversized/malformed).
	rawFirst []byte
	// noReady accepts the request and then stays silent.
	noReady bool
	// eofAfterReady ends the response right after the ready record.
	eofAfterReady bool
}

// fakeIngester serves wire.Pattern for one or more tenants. Each tenant has
// its own buffered record queue, so a test can prove subscriptions do not
// cross tenants.
type fakeIngester struct {
	srv     *httptest.Server
	tenants chan tenant.ID
	queries chan url.Values
	hangups chan tenant.ID
	mu      sync.Mutex
	queues  map[tenant.ID]chan []byte
	pending map[tenant.ID]int
}

func newFakeIngester(t *testing.T, opts fakeIngesterOptions) *fakeIngester {
	t.Helper()
	f := &fakeIngester{
		tenants: make(chan tenant.ID, 16),
		queries: make(chan url.Values, 16),
		hangups: make(chan tenant.ID, 16),
		queues:  make(map[tenant.ID]chan []byte),
		pending: make(map[tenant.ID]int),
	}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != internalTailPath() {
			http.NotFound(w, r)
			return
		}
		id, err := tenant.Resolve(r.Header.Get(tenant.Header), true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		defer func() {
			select {
			case f.hangups <- id:
			default:
			}
		}()
		select {
		case f.tenants <- id:
		default:
		}
		select {
		case f.queries <- r.URL.Query():
		default:
		}
		if opts.status != 0 {
			if opts.failAfter != nil {
				select {
				case <-opts.failAfter:
				case <-r.Context().Done():
					return
				}
			}
			http.Error(w, "ingester unavailable", opts.status)
			return
		}
		w.Header().Set("Content-Type", wire.ContentType)
		w.WriteHeader(http.StatusOK)
		flush, _ := w.(http.Flusher)
		write := func(b []byte) bool {
			if _, err := w.Write(append(b, '\n')); err != nil {
				return false
			}
			if flush != nil {
				flush.Flush()
			}
			return true
		}
		switch {
		case opts.rawFirst != nil:
			if !write(opts.rawFirst) {
				return
			}
		case opts.noReady:
		case opts.ready != nil:
			if !write(opts.ready(id)) {
				return
			}
		default:
			if !write(readyRecord(id)) {
				return
			}
		}
		if opts.noReady {
			<-r.Context().Done()
			return
		}
		if opts.eofAfterReady && opts.rawFirst == nil {
			return
		}
		q := f.queue(id)
		for {
			select {
			case rec := <-q:
				if !write(rec) {
					return
				}
			case <-r.Context().Done():
				return
			}
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIngester) queue(id tenant.ID) chan []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := f.queues[id]
	if q == nil {
		q = make(chan []byte, 64)
		f.queues[id] = q
	}
	return q
}

func (f *fakeIngester) send(t *testing.T, id tenant.ID, v any) {
	t.Helper()
	f.queue(id) <- marshalRecord(t, v)
}

func (f *fakeIngester) sendRaw(t *testing.T, id tenant.ID, b []byte) {
	t.Helper()
	f.queue(id) <- b
}

// claimSubscribe moves every observed subscription event into per-tenant
// pending counts, then claims one event for id. Events for other tenants
// stay pending, so waits may run in any order and repeated subscriptions
// each remain claimable.
func (f *fakeIngester) claimSubscribe(id tenant.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		select {
		case got := <-f.tenants:
			f.pending[got]++
		default:
			if f.pending[id] == 0 {
				return false
			}
			f.pending[id]--
			if f.pending[id] == 0 {
				delete(f.pending, id)
			}
			return true
		}
	}
}

func (f *fakeIngester) waitSubscribe(t *testing.T, id tenant.ID) {
	t.Helper()
	eventually(t, "ingester subscription", func() bool {
		return f.claimSubscribe(id)
	})
}

func (f *fakeIngester) waitHangup(t *testing.T) tenant.ID {
	t.Helper()
	var id tenant.ID
	eventually(t, "ingester hangup", func() bool {
		select {
		case id = <-f.hangups:
			return true
		default:
			return false
		}
	})
	return id
}

func (f *fakeIngester) lastQuery(t *testing.T) url.Values {
	t.Helper()
	var q url.Values
	eventually(t, "ingester request", func() bool {
		select {
		case q = <-f.queries:
			return true
		default:
			return false
		}
	})
	return q
}

// tailHarness runs the real service behind the real server middleware.
type tailHarness struct {
	svc      *service
	srv      *httptest.Server
	base     string
	reg      *prometheus.Registry
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
	stopErr  error
	stopSlow bool
}

func newTailHarness(t *testing.T, urls []string, mutate func(*service)) *tailHarness {
	t.Helper()
	reg := prometheus.NewRegistry()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_ = log
	srv := server.New(server.Options{Logger: log, Registry: reg, MultiTenant: true})
	deps := app.Deps{
		Config: config.Config{
			LogQL: config.LogQL{
				MaxEntries: 5000,
				Tail: config.Tail{
					IngesterURLs:  urls,
					BufferEntries: 64,
					FlushInterval: 5 * time.Millisecond,
				},
			},
		},
		Logger:  log,
		HTTP:    srv,
		Metrics: reg,
	}
	built, err := NewQuerier(deps)
	if err != nil {
		t.Fatalf("NewQuerier: %v", err)
	}
	svc := built.(*service)
	if mutate != nil {
		mutate(svc)
	}
	hs := httptest.NewServer(srv.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	h := &tailHarness{
		svc:    svc,
		srv:    hs,
		base:   "ws" + strings.TrimPrefix(hs.URL, "http"),
		reg:    reg,
		cancel: cancel,
		done:   done,
	}
	t.Cleanup(func() {
		h.stop(t)
		hs.Close()
	})
	return h
}

func (h *tailHarness) stop(t *testing.T) {
	t.Helper()
	h.stopOnce.Do(func() {
		h.cancel()
		select {
		case h.stopErr = <-h.done:
		case <-time.After(10 * time.Second):
			h.stopSlow = true
		}
	})
	if h.stopSlow {
		t.Fatal("tail service did not stop")
	}
}

func (h *tailHarness) dial(t *testing.T, query string, header http.Header) *websocket.Conn {
	t.Helper()
	return h.dialValues(t, url.Values{"query": {query}}, header)
}

func (h *tailHarness) dialValues(t *testing.T, values url.Values, header http.Header) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	u := h.base + "/loki/api/v1/tail?" + values.Encode()
	conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial tail: %v", err)
	}
	conn.SetReadLimit(16 << 20)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func readTail(t *testing.T, conn *websocket.Conn, timeout time.Duration) (decodedTail, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return decodedTail{}, err
	}
	if typ != websocket.MessageText {
		t.Fatalf("tail message type = %d, want text", typ)
	}
	var m decodedTail
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode tail message %q: %v", data, err)
	}
	return m, nil
}

// readEntries collects at least want entries across messages.
func readEntries(t *testing.T, conn *websocket.Conn, want int, timeout time.Duration) []decodedEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []decodedEntry
	for len(got) < want {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out with %d/%d entries", len(got), want)
		}
		m, err := readTail(t, conn, remaining)
		if err != nil {
			t.Fatalf("read tail after %d entries: %v", len(got), err)
		}
		got = append(got, m.entries(t)...)
	}
	return got
}

// readClose returns the close error a client sees.
func readClose(t *testing.T, conn *websocket.Conn, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		_ = data // drain any data messages written before the close
	}
}

func closeInfo(err error) (websocket.StatusCode, string) {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return ce.Code, ce.Reason
	}
	return -1, err.Error()
}

// waitQueued waits until the (single) active session's delay queue holds n
// entries; it is how delay tests make sure input arrived before advancing the
// fake clock.
func waitQueued(t *testing.T, svc *service, n int) {
	t.Helper()
	eventually(t, "queued entries", func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		for sess := range svc.sessions {
			sess.queue.mu.Lock()
			got := len(sess.queue.h)
			sess.queue.mu.Unlock()
			if got >= n {
				return true
			}
		}
		return false
	})
}

// waitNoSessions asserts every public session has returned, so its readers,
// handler and connection are gone.
func waitNoSessions(t *testing.T, h *tailHarness) {
	t.Helper()
	eventually(t, "all sessions to exit", func() bool {
		h.svc.mu.Lock()
		defer h.svc.mu.Unlock()
		return len(h.svc.sessions) == 0
	})
}

// waitNoSubscribers asserts the subscriber gauge drained; unregister runs
// after the session's own metric decrement.
func waitNoSubscribers(t *testing.T, h *tailHarness) {
	t.Helper()
	eventually(t, "subscriber gauge to drain", func() bool {
		return metricValue(t, h.reg, "parkana_tail_subscribers") == 0
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// metricValue reads a collector's current value through a fresh registry.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	v, ok := gatherMetric(t, reg, name)
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	return v
}

// gatherMetric is metricValue that reports a missing metric (a labeled
// counter nobody has incremented yet) instead of failing.
func gatherMetric(t *testing.T, reg *prometheus.Registry, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var total float64
		for _, m := range f.GetMetric() {
			if c := m.GetCounter(); c != nil {
				total += c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				total += g.GetValue()
			}
		}
		return total, true
	}
	return 0, false
}
