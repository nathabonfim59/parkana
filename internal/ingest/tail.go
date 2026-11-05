package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// tailHub fans freshly acknowledged pushes out to live-tail subscribers. It is
// tenant-scoped, never blocks a push and evicts a subscriber whose queued
// entries exceed the configured budget. Publishing happens only after WAL fsync
// and memtable admission, so tail never shows a row the ingester would lose on
// restart.
type tailHub struct {
	mu         sync.Mutex
	maxEntries int
	subs       map[tenant.ID]map[*tailSubscription]struct{}
	closed     bool
	m          *tailMetrics
}

// newTailHub returns a hub whose per-subscriber queue holds at most maxEntries
// log entries. m is required.
func newTailHub(maxEntries int, m *tailMetrics) (*tailHub, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("ingest: tail buffer entries must be positive, got %d", maxEntries)
	}
	if m == nil {
		return nil, errors.New("ingest: tail metrics are required")
	}
	return &tailHub{
		maxEntries: maxEntries,
		subs:       make(map[tenant.ID]map[*tailSubscription]struct{}),
		m:          m,
	}, nil
}

// publish offers one acknowledged push to every subscriber of id. The request
// is cloned and timestamps are truncated to storage precision before the
// entries leave the caller's ownership. A push with no subscriber costs one
// map lookup; a subscriber that would overflow is evicted and counted instead
// of delaying the push.
func (h *tailHub) publish(id tenant.ID, req *logs.PushRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	subs := h.subs[id]
	if len(subs) == 0 {
		return
	}
	entries := tailEntries(req)
	if len(entries) == 0 {
		return
	}
	var evicted []*tailSubscription
	for s := range subs {
		if s.offer(entries) {
			evicted = append(evicted, s)
		}
	}
	for _, s := range evicted {
		h.removeLocked(s)
	}
}

// subscribe registers a subscriber for id. The caller must eventually call
// unsubscribe. A hub already shutting down returns a closed subscription, so a
// racing handler still writes its ready record before exiting.
func (h *tailHub) subscribe(id tenant.ID) *tailSubscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := &tailSubscription{hub: h, tenant: id, sig: make(chan struct{}, 1)}
	if h.closed {
		s.closed = true
		return s
	}
	subs := h.subs[id]
	if subs == nil {
		subs = make(map[*tailSubscription]struct{})
		h.subs[id] = subs
	}
	subs[s] = struct{}{}
	h.m.subscribers.WithLabelValues(string(id)).Inc()
	return s
}

// unsubscribe removes s and wakes its reader. Idempotent.
func (h *tailHub) unsubscribe(s *tailSubscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(s)
}

// removeLocked deletes s from the hub, drops its queue and signals its reader.
// The hub mutex must be held.
func (h *tailHub) removeLocked(s *tailSubscription) {
	subs := h.subs[s.tenant]
	if _, ok := subs[s]; !ok {
		return
	}
	delete(subs, s)
	if len(subs) == 0 {
		delete(h.subs, s.tenant)
	}
	h.m.subscribers.WithLabelValues(string(s.tenant)).Dec()
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.queued = 0
		s.queue = nil
	}
	s.mu.Unlock()
	s.signal()
}

// close stops the hub and wakes every subscriber; Run calls it on shutdown.
// Idempotent.
func (h *tailHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, subs := range h.subs {
		for s := range subs {
			h.m.subscribers.WithLabelValues(string(id)).Dec()
			s.mu.Lock()
			s.closed = true
			s.queued = 0
			s.queue = nil
			s.mu.Unlock()
			s.signal()
		}
	}
	clear(h.subs)
}

// tailSubscription is one live-tail request's bounded entry queue. All queue
// state is guarded by mu; hub.publish holds the hub mutex and takes this one in
// the same order as unsubscribe and close.
type tailSubscription struct {
	hub    *tailHub
	tenant tenant.ID

	mu      sync.Mutex
	queue   []wire.Entry
	queued  int    // len(queue), kept to bound entries without rescanning
	dropped uint64 // entries lost by an eviction, reported once
	closed  bool
	sig     chan struct{}
}

// offer enqueues entries when they fit, or evicts the subscriber. It never
// blocks: a slow reader cannot stall the push that found it behind.
func (s *tailSubscription) offer(entries []wire.Entry) (evicted bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	if len(entries) > s.hub.maxEntries-s.queued {
		// A subscriber that cannot absorb even this push (or has no room
		// left) loses its whole queue. Report every lost entry once.
		lost := uint64(s.queued + len(entries))
		s.dropped += lost
		s.queued = 0
		s.queue = nil
		s.closed = true
		s.mu.Unlock()
		s.hub.m.drop(s.tenant, lost)
		s.hub.m.evict(s.tenant)
		s.signal()
		return true
	}
	s.queue = append(s.queue, entries...)
	s.queued += len(entries)
	s.mu.Unlock()
	s.hub.m.publish(s.tenant, uint64(len(entries)))
	s.signal()
	return false
}

// pop returns the next queued entries, or a dropped count once when the
// subscription was evicted, or ok=false when it is closed and drained or ctx
// ends.
func (s *tailSubscription) pop(ctx context.Context) (entries []wire.Entry, dropped uint64, ok bool) {
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			entries = s.queue
			s.queue = nil
			s.queued = 0
			s.mu.Unlock()
			return entries, 0, true
		}
		if s.closed {
			dropped = s.dropped
			s.dropped = 0
			s.mu.Unlock()
			if dropped > 0 {
				return nil, dropped, true
			}
			return nil, 0, false
		}
		s.mu.Unlock()
		select {
		case <-s.sig:
		case <-ctx.Done():
			return nil, 0, false
		}
	}
}

// signal wakes pop without blocking when it is already awake.
func (s *tailSubscription) signal() {
	select {
	case s.sig <- struct{}{}:
	default:
	}
}

// tailEntries snapshots a push for the wire. Labels and metadata are copied
// once per stream and shared by that stream's entries; consumers treat them as
// read-only. Timestamps are truncated to the stored microsecond grid and
// rendered as decimal Unix nanoseconds, so both sides agree on the exact row
// the ingester would persist.
func tailEntries(req *logs.PushRequest) []wire.Entry {
	n := 0
	for _, st := range req.Streams {
		n += len(st.Entries)
	}
	if n == 0 {
		return nil
	}
	out := make([]wire.Entry, 0, n)
	for _, st := range req.Streams {
		labels := make([]wire.Label, len(st.Labels))
		for i, l := range st.Labels {
			labels[i] = wire.Label{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)}
		}
		for _, e := range st.Entries {
			meta := make([]wire.Label, len(e.Metadata))
			for i, l := range e.Metadata {
				meta[i] = wire.Label{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)}
			}
			out = append(out, wire.Entry{
				Timestamp: strconv.FormatInt(schema.TS(e.Timestamp)*1000, 10),
				Line:      strings.Clone(e.Line),
				Labels:    labels,
				Metadata:  meta,
			})
		}
	}
	return out
}

// tailMetrics holds the ingester's live-tail collectors. They live here, with
// the hub, rather than in the push metrics.
type tailMetrics struct {
	subscribers    *prometheus.GaugeVec
	published      *prometheus.CounterVec
	droppedEntries *prometheus.CounterVec
	evictions      *prometheus.CounterVec
}

// newTailMetrics registers the live-tail collectors on reg. Partial
// registration is rolled back so a retried construction starts clean.
func newTailMetrics(reg prometheus.Registerer) (*tailMetrics, error) {
	if reg == nil {
		return nil, errors.New("ingest: tail metrics registerer is required")
	}
	m := &tailMetrics{
		subscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "parkana_ingest_tail_subscribers",
			Help: "Live-tail subscribers currently connected, by tenant.",
		}, []string{"tenant"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_tail_published_entries_total",
			Help: "Entries queued for live-tail subscribers, by tenant.",
		}, []string{"tenant"}),
		droppedEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_tail_dropped_entries_total",
			Help: "Entries lost when a live-tail subscriber was too slow, by tenant.",
		}, []string{"tenant"}),
		evictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_ingest_tail_evictions_total",
			Help: "Slow live-tail subscribers disconnected, by tenant.",
		}, []string{"tenant"}),
	}
	collectors := []prometheus.Collector{m.subscribers, m.published, m.droppedEntries, m.evictions}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			for _, r := range registered {
				reg.Unregister(r)
			}
			return nil, fmt.Errorf("ingest: register tail metrics: %w", err)
		}
		registered = append(registered, c)
	}
	return m, nil
}

// publish records entries queued for a tenant's subscribers.
func (m *tailMetrics) publish(id tenant.ID, n uint64) {
	m.published.WithLabelValues(string(id)).Add(float64(n))
}

// drop records entries lost to a slow subscriber.
func (m *tailMetrics) drop(id tenant.ID, n uint64) {
	m.droppedEntries.WithLabelValues(string(id)).Add(float64(n))
}

// evict records one slow subscriber disconnected.
func (m *tailMetrics) evict(id tenant.ID) {
	m.evictions.WithLabelValues(string(id)).Inc()
}
