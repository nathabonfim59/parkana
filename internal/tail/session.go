package tail

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"

	"github.com/nathabonfim59/parkana/internal/logql"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// socket is the subset of *websocket.Conn a session uses. The interface keeps
// writeLoop's error-precedence behavior directly testable without a network.
type socket interface {
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	Close(websocket.StatusCode, string) error
	CloseNow() error
	CloseRead(context.Context) context.Context
}

var _ socket = (*websocket.Conn)(nil)

// session is one public live-tail WebSocket. It owns its ingester readers,
// bounded delay queue and writer; nothing is shared with another session
// except the immutable service configuration and compiled query.
type session struct {
	svc         *service
	conn        socket
	tenant      tenant.ID
	query       *logql.CompiledLogQuery
	req         tailRequest
	categorized bool
	queue       *delayQueue

	// ready receives one token per ingester whose ready record arrived.
	ready chan struct{}
	// errs receives the first fatal upstream error from a reader.
	errs chan error

	wg sync.WaitGroup
}

// tailResult says how a session ended: an optional WebSocket close frame and
// the diagnostic error for the log. A zero code means the client is already
// gone, so there is nothing to send.
type tailResult struct {
	code   websocket.StatusCode
	reason string
	err    error
}

// run subscribes to every configured ingester, waits until each acknowledges
// the subscription, then streams entries until the client leaves, an upstream
// fails or the service shuts down. All reader goroutines are joined before it
// returns so a session never leaks a body or goroutine.
func (s *session) run(parent context.Context) tailResult {
	// CloseRead both detects client disconnects (including a normal close
	// frame) and services ping/pong control frames; Ping requires it.
	readCtx := s.conn.CloseRead(parent)
	ctx, cancel := context.WithCancel(readCtx)
	stop := context.AfterFunc(s.svc.ctx, cancel)
	defer stop()

	for _, target := range s.svc.targets {
		s.wg.Go(func() { s.subscribe(ctx, target) })
	}
	res := s.stream(ctx)
	cancel()
	s.wg.Wait()
	// The session owns the queue; clearing it here drops any remaining
	// entries and descriptors once the client/readers are gone, so a finished
	// session holds no buffered log content.
	s.queue.close()
	return res
}

// stream waits for readiness, then runs the writer and ping loops.
func (s *session) stream(ctx context.Context) tailResult {
	readyTimer := time.NewTimer(s.svc.readyTimeout)
	defer readyTimer.Stop()
	for remaining := len(s.svc.targets); remaining > 0; {
		select {
		case <-s.ready:
			remaining--
		case err := <-s.errs:
			return s.upstreamFailure(err)
		case <-readyTimer.C:
			return tailResult{
				code:   websocket.StatusInternalError,
				reason: "timed out waiting for ingesters to acknowledge the tail subscription",
			}
		case <-ctx.Done():
			return s.canceled()
		}
	}

	pingCtx, pingCancel := context.WithCancel(ctx)
	var pingWG sync.WaitGroup
	pingWG.Go(func() { s.pingLoop(pingCtx) })
	res := s.writeLoop(ctx)
	pingCancel()
	pingWG.Wait()
	return res
}

// writeLoop is the only data writer. It drains due entries in timestamp
// order, sends one message per flush, and otherwise sleeps until the next
// delay deadline, the flush interval, a push signal, a client event or an
// upstream failure.
func (s *session) writeLoop(ctx context.Context) tailResult {
	for {
		// Check failures and shutdown before doing more work: a busy due
		// queue must never delay a fatal upstream gap indefinitely.
		select {
		case err := <-s.errs:
			return s.upstreamFailure(err)
		case <-ctx.Done():
			return s.canceled()
		default:
		}
		due, drops, overflow, wait := s.queue.take(s.svc.clock.Now(), s.req.delayFor, s.req.limit, maxFrameBytes, s.svc.flushInterval)
		if len(due) > 0 || len(drops) > 0 {
			if err := s.writeBatch(ctx, due, drops); err != nil {
				if ctx.Err() != nil {
					return s.canceled()
				}
				if errors.Is(err, errFrameTooLarge) {
					return tailResult{
						code:   websocket.StatusInternalError,
						reason: "live tail entry or dropped descriptor exceeds the maximum WebSocket frame size; the stream is incomplete",
						err:    err,
					}
				}
				return tailResult{code: websocket.StatusInternalError, reason: "failed to write to the tail WebSocket", err: err}
			}
		}
		if overflow > 0 {
			return overflowResult(overflow)
		}
		if len(due) > 0 || len(drops) > 0 {
			continue
		}
		if wait <= 0 {
			// Entries are due but the previous take stopped at the frame
			// limit; re-run without blocking.
			continue
		}
		select {
		case <-s.queue.notify:
		case <-s.svc.clock.After(wait):
		case err := <-s.errs:
			return s.upstreamFailure(err)
		case <-ctx.Done():
			return s.canceled()
		}
	}
}

// overflowResult ends a healthy connection when local loss outgrew the
// descriptor budget. Sending a bare dropped_entries subset would silently
// hide the rest, so an explicit close carries the undescribed count instead.
func overflowResult(n uint64) tailResult {
	return tailResult{
		code:   websocket.StatusInternalError,
		reason: fmt.Sprintf("live tail querier buffer overflow dropped %d entries without descriptors; the stream is incomplete", n),
		err:    fmt.Errorf("tail: %d dropped entries exceeded the descriptor limit", n),
	}
}

// writeBatch encodes and writes one flush as one or more Loki tail responses,
// each within maxFrameBytes. The write context bounds a client that stops
// reading, so it cannot pin the session forever.
func (s *session) writeBatch(ctx context.Context, entries []queuedEntry, drops []droppedEntry) error {
	frames, err := buildFrames(entries, drops, s.categorized)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		wctx, cancel := context.WithTimeout(ctx, s.svc.writeTimeout)
		err := s.conn.Write(wctx, websocket.MessageText, frame)
		cancel()
		if err != nil {
			return err
		}
	}
	s.svc.m.sent(s.tenant, len(entries))
	return nil
}

// pingLoop mirrors Loki's once-per-second WebSocket ping. A failed ping means
// the client is gone or stuck; closing the connection unblocks the writer and
// CloseRead goroutine.
func (s *session) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(s.svc.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, s.svc.pingTimeout)
			err := s.conn.Ping(pctx)
			cancel()
			if err != nil {
				_ = s.conn.CloseNow()
				return
			}
		}
	}
}

// upstreamFailure turns an upstream or protocol error into an explicit close.
// TypeDropped from an ingester carries no descriptors, so surfacing the gap as
// a close reason is the only honest option: inventing Loki dropped_entries or
// reconnecting as if nothing happened would hide lost rows.
func (s *session) upstreamFailure(err error) tailResult {
	s.svc.m.upstreamError(s.tenant)
	return tailResult{code: websocket.StatusInternalError, reason: err.Error(), err: err}
}

// canceled classifies a context cancellation: service shutdown gets a
// GoingAway close, a client disconnect nothing.
func (s *session) canceled() tailResult {
	if s.svc.closing.Load() {
		return tailResult{code: websocket.StatusGoingAway, reason: "live tail service is shutting down"}
	}
	return tailResult{}
}

// subscribe runs one ingester reader. A failure is reported once and ends the
// whole session; there is no silent lossless reconnect.
func (s *session) subscribe(ctx context.Context, target ingester) {
	err := s.readSubscription(ctx, target)
	if err == nil || ctx.Err() != nil {
		return
	}
	select {
	case s.errs <- err:
	default:
	}
}

// finish sends the close frame when there is one, then closes the connection.
// coder/websocket's Close is bounded (it waits up to five seconds for the peer
// close handshake), so a wedged client cannot keep the handler alive forever.
func (s *session) finish(res tailResult) {
	if res.code == 0 {
		_ = s.conn.CloseNow()
		return
	}
	_ = s.conn.Close(res.code, truncateReason(res.reason))
}

// truncateReason keeps a close reason inside RFC 6455's 123-byte limit
// without splitting a UTF-8 rune.
func truncateReason(reason string) string {
	const maxReason = 123
	if len(reason) <= maxReason {
		return reason
	}
	cut := maxReason
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}

// processBatch validates one ingester batch, runs the compiled LogQL pipeline
// row by row and converts survivors into queue entries with their final
// response labels.
func (s *session) processBatch(ctx context.Context, target ingester, entries []wire.Entry) ([]queuedEntry, error) {
	out := make([]queuedEntry, 0, len(entries))
	for _, raw := range entries {
		ts, err := parseWireTimestamp(raw.Timestamp)
		if err != nil {
			return nil, protocolError(target, "invalid timestamp")
		}
		if !s.req.start.IsZero() && ts.Before(s.req.start) {
			continue
		}
		labels, err := wireLabels(raw.Labels)
		if err != nil {
			return nil, protocolError(target, "invalid stream labels")
		}
		meta, err := wireLabels(raw.Metadata)
		if err != nil {
			return nil, protocolError(target, "invalid structured metadata")
		}
		final, entry, keep, err := s.query.Process(ctx, s.tenant, plan.Row{TS: ts, Labels: labels, Meta: meta, Line: raw.Line})
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		out = append(out, queuedEntry{
			ts:     entry.TS,
			labels: s.responseLabels(final, entry.Meta, entry.Parsed),
			entry:  entry,
		})
	}
	return out, nil
}

// responseLabels produces the label set a response groups by. With
// categorize-labels the categories stay separate and their names are removed
// from the stream labels, exactly as Loki's categorizeLabelsIterator does.
// Without the flag, metadata and parsed labels are flattened into the stream
// labels with Loki's precedence: parsed over structured metadata over stream
// labels.
func (s *session) responseLabels(base, meta, parsed logs.Labels) logs.Labels {
	if len(meta) == 0 && len(parsed) == 0 {
		return base
	}
	m := base.Map()
	if s.categorized {
		for _, l := range meta {
			delete(m, l.Name)
		}
		for _, l := range parsed {
			delete(m, l.Name)
		}
	} else {
		for _, l := range meta {
			m[l.Name] = l.Value
		}
		for _, l := range parsed {
			m[l.Name] = l.Value
		}
	}
	return logs.FromMap(m)
}
