// Package tail is Loki's live tail (GET /loki/api/v1/tail, WebSocket): the
// querier side subscribes to the ingesters' recent entries over HTTP and runs
// the query's LogQL pipeline on them. Ingesters and queriers share no
// in-process state; the ingester side lives in internal/ingest.
//
// The public contract is Loki's: one WebSocket text message per flush holding
// {"streams":[{"stream":{...},"values":[["<ns>","line"]]}]} plus optional
// dropped_entries. Entries are only those published after a successful push
// (the ingester publishes after WAL durability); there is deliberately no
// historical backfill, so start only filters incoming rows. A live tail is
// best effort: an ingester eviction, upstream failure or querier overflow is
// surfaced as an explicit dropped_entries record or close, never as a
// lossless-looking reconnect.
package tail

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// maxRecordBytes bounds one NDJSON record read from an ingester. A larger
	// record is a protocol error: the ingester violated the wire contract or
	// an attacker is trying to grow querier memory. A push larger than this
	// can therefore be refused as an explicit gap; the ingester's own limit
	// is the authoritative bound.
	maxRecordBytes = 4 << 20
	// maxFrameBytes bounds one encoded public WebSocket JSON message. Labels,
	// metadata, JSON escaping and the response envelope all count, so a batch
	// is split (or the stream closed explicitly) rather than trusting line
	// lengths alone.
	maxFrameBytes = 4 << 20
	// maxDroppedDescriptors mirrors Loki's maxDroppedEntriesPerTailResponse
	// (pkg/querier/tail/tail.go) bound on in-memory descriptors. Once it is
	// full, a further local drop cannot be described, so the session closes
	// with the undescribed count instead of silently clearing it.
	maxDroppedDescriptors = 1000

	defaultReadyTimeout = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
	// pingPeriod is Loki's wsPingPeriod (pkg/querier/tail/http.go). Pings keep
	// dead clients from holding a session open; Grafana answers them.
	defaultPingPeriod  = time.Second
	defaultPingTimeout = 5 * time.Second
)

// ingester is one configured internal tail endpoint.
type ingester struct {
	url *url.URL
	// display is the URL as shown in errors and logs: scheme, host and path
	// only, so a misconfigured credential or query never leaks.
	display string
}

// service is the querier's live-tail role. It owns no goroutine until it
// runs; each public WebSocket request starts its own session and subscribers.
type service struct {
	engine        *logql.Engine
	maxEntries    int
	bufferEntries int
	targets       []ingester
	log           *slog.Logger
	m             *tailMetrics
	client        *http.Client
	clock         clock

	// ctx is canceled by Run when the service shuts down. Every session and
	// every upstream subscription derives from it.
	ctx    context.Context
	cancel context.CancelFunc

	// closing is the lock-free fast path for "reject new requests"; mu also
	// orders session registration against Run's Wait, so Add cannot race a
	// zero-counter Wait.
	closing  atomic.Bool
	mu       sync.Mutex
	closed   bool
	sessions map[*session]struct{}
	wg       sync.WaitGroup

	readyTimeout  time.Duration
	writeTimeout  time.Duration
	flushInterval time.Duration
	pingPeriod    time.Duration
	pingTimeout   time.Duration
}

var _ app.Service = (*service)(nil)

// NewQuerier builds the querier side of live tail and registers its route.
// Querier-only processes with no configured ingester URLs still start; the
// public endpoint then answers 503 and ordinary queries are unaffected.
func NewQuerier(deps app.Deps) (app.Service, error) {
	switch {
	case deps.Logger == nil:
		return nil, errors.New("tail: Deps.Logger is required")
	case deps.HTTP == nil:
		return nil, errors.New("tail: Deps.HTTP is required")
	case deps.Metrics == nil:
		return nil, errors.New("tail: Deps.Metrics is required")
	}
	if deps.HTTP.Registered(lokiapi.Tail) {
		return nil, fmt.Errorf("tail: route %q already registered", lokiapi.Tail)
	}
	targets, err := parseIngesters(deps.Config.TailIngesterURLs())
	if err != nil {
		return nil, err
	}
	m, err := newTailMetrics(deps.Metrics)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Bounds how long a subscription may sit before its ready record arrives.
	transport.ResponseHeaderTimeout = defaultReadyTimeout
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{
		engine:        logql.New(deps.Config.LogQL),
		maxEntries:    deps.Config.LogQL.MaxEntries,
		bufferEntries: cmp.Or(deps.Config.LogQL.Tail.BufferEntries, config.DefaultTailBufferEntries),
		targets:       targets,
		log:           deps.Logger,
		m:             m,
		client:        &http.Client{Transport: transport},
		clock:         realClock{},
		ctx:           ctx,
		cancel:        cancel,
		sessions:      make(map[*session]struct{}),
		readyTimeout:  defaultReadyTimeout,
		writeTimeout:  defaultWriteTimeout,
		flushInterval: cmp.Or(deps.Config.LogQL.Tail.FlushInterval, config.DefaultTailFlushInterval),
		pingPeriod:    defaultPingPeriod,
		pingTimeout:   defaultPingTimeout,
	}
	deps.HTTP.Handle(lokiapi.Tail, http.HandlerFunc(s.serveTail))
	return s, nil
}

// parseIngesters validates the configured base URLs and builds their display
// form. config already rejects bad entries for the real process; this repeats
// the checks so a directly constructed Deps cannot smuggle credentials.
func parseIngesters(raw []string) ([]ingester, error) {
	out := make([]ingester, 0, len(raw))
	for _, r := range raw {
		u, err := url.Parse(r)
		if err != nil {
			return nil, errors.New("tail: invalid ingester URL")
		}
		switch {
		case u.Scheme != "http" && u.Scheme != "https":
			return nil, errors.New("tail: ingester URL scheme must be http or https")
		case u.Host == "":
			return nil, errors.New("tail: ingester URL is missing a host")
		case u.User != nil:
			return nil, errors.New("tail: ingester URL must not contain credentials")
		case u.RawQuery != "" || u.ForceQuery:
			return nil, errors.New("tail: ingester URL must not contain a query string")
		case u.Fragment != "":
			return nil, errors.New("tail: ingester URL must not contain a fragment")
		}
		out = append(out, ingester{url: u, display: displayURL(u)})
	}
	return out, nil
}

// displayURL renders a URL without credentials, query or fragment.
func displayURL(u *url.URL) string {
	c := u.Clone()
	c.User = nil
	c.RawQuery = ""
	c.ForceQuery = false
	c.Fragment = ""
	return c.String()
}

// Run waits for shutdown, then cancels every active session and waits for its
// handler and upstream readers to finish. Requests that arrive after shutdown
// started are rejected before the WebSocket upgrade.
func (s *service) Run(ctx context.Context) error {
	<-ctx.Done()
	s.closing.Store(true)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
	s.client.CloseIdleConnections()
	return nil
}

// register adds a live session if the service is still accepting them. The
// caller must call unregister on success.
func (s *service) register(sess *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.sessions[sess] = struct{}{}
	s.wg.Add(1)
	return true
}

func (s *service) unregister(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
	s.wg.Done()
}

// serveTail validates the request before the upgrade, then runs one live-tail
// session until the client disconnects, an upstream fails or the service
// shuts down. Invalid requests therefore get a normal plain-text Loki 400/503,
// never a WebSocket.
func (s *service) serveTail(w http.ResponseWriter, r *http.Request) {
	if s.closing.Load() {
		lokiapi.Error(w, http.StatusServiceUnavailable, "live tail is shutting down")
		return
	}
	req, err := parseTailRequest(r)
	if err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.maxEntries > 0 && req.limit > s.maxEntries {
		lokiapi.Error(w, http.StatusBadRequest, fmt.Sprintf(
			"max entries limit per query exceeded, limit > max_entries_limit_per_query (%d > %d)",
			req.limit, s.maxEntries))
		return
	}
	query, err := s.engine.CompileLogQuery(req.query)
	if err != nil {
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(s.targets) == 0 {
		lokiapi.Error(w, http.StatusServiceUnavailable, "live tail is unavailable: no ingester URLs are configured")
		return
	}
	if s.closing.Load() {
		lokiapi.Error(w, http.StatusServiceUnavailable, "live tail is shutting down")
		return
	}

	// Loki's tail handler accepts every origin (pkg/querier/tail/http.go
	// CheckOrigin returns true): Grafana runs in a browser on another host.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return // Accept has already written the HTTP error.
	}
	id := tenant.From(r.Context())
	sess := &session{
		svc:         s,
		conn:        conn,
		tenant:      id,
		query:       query,
		req:         req,
		categorized: wantsCategorizedLabels(r),
		queue:       newDelayQueue(s.bufferEntries, s.m, id),
		ready:       make(chan struct{}, len(s.targets)),
		errs:        make(chan error, len(s.targets)+1),
	}
	if !s.register(sess) {
		sess.finish(tailResult{code: websocket.StatusGoingAway, reason: "live tail service is shutting down"})
		return
	}
	defer s.unregister(sess)
	s.m.subscribe(sess.tenant)
	defer s.m.unsubscribe(sess.tenant)

	res := sess.run(r.Context())
	if res.err != nil {
		s.log.Warn("live tail session ended with an error", "tenant", sess.tenant, "err", res.err)
	}
	sess.finish(res)
}

// wantsCategorizedLabels mirrors Loki's X-Loki-Response-Encoding-Flags
// handling: the header is a comma-separated flag set.
func wantsCategorizedLabels(r *http.Request) bool {
	for flag := range strings.SplitSeq(r.Header.Get("X-Loki-Response-Encoding-Flags"), ",") {
		if strings.TrimSpace(flag) == "categorize-labels" {
			return true
		}
	}
	return false
}

// internalTailPath is the ingester route without its method token.
func internalTailPath() string {
	return strings.TrimPrefix(wire.Pattern, http.MethodGet+" ")
}
