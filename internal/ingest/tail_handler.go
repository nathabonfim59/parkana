package ingest

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// defaultTailWriteTimeout bounds one NDJSON write plus flush, so a client that
// stops reading cannot pin a handler after its queue was evicted or the
// service shut down.
const defaultTailWriteTimeout = 10 * time.Second

// serveTail is the ingester's private live-tail endpoint (wire.Pattern). It
// streams NDJSON records for the request's tenant: a ready line, then batches
// of entries published after a successful push. The handler flushes every
// record and bounds each write with a deadline, so a subscriber sees a line as
// soon as the push is acknowledged and a slow reader cannot hold the handler
// open indefinitely.
//
// Cancellation policy: when the client disconnects or cancels the request, the
// subscription is removed when the handler next observes it and the handler
// returns; no push waits for a tail reader. The end a subscriber sees is
// whichever event it observes first: a client disconnect, an upstream eviction
// (reported as a dropped record), or shutdown. Ready means "subscribed", not
// "the ingester is accepting pushes": during WAL replay the stream opens and
// stays empty until the first acknowledged push.
func (s *service) serveTail(w http.ResponseWriter, r *http.Request) {
	id := tenant.From(r.Context())
	sub := s.tail.subscribe(id)
	defer s.tail.unsubscribe(sub)

	h := w.Header()
	h.Set("Content-Type", wire.ContentType)
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if err := writeTailRecord(w, rc, s.tailWriteTimeout, wire.Message{
		Type:          wire.TypeReady,
		Tenant:        string(id),
		BufferEntries: s.tail.maxEntries,
	}); err != nil {
		return
	}
	for {
		entries, dropped, ok := sub.pop(r.Context())
		if !ok {
			return
		}
		if dropped > 0 {
			if err := writeTailRecord(w, rc, s.tailWriteTimeout, wire.Message{
				Type:    wire.TypeDropped,
				Dropped: dropped,
				Reason:  "slow subscriber",
			}); err != nil {
				return
			}
		}
		if len(entries) == 0 {
			continue
		}
		if err := writeTailRecord(w, rc, s.tailWriteTimeout, wire.Message{Type: wire.TypeBatch, Entries: entries}); err != nil {
			return
		}
	}
}

// writeTailRecord encodes one NDJSON record, writes it, flushes it and clears
// the write deadline again. A blocked client is cut off after timeout; an
// encoding, write or flush failure ends the stream.
func writeTailRecord(w io.Writer, rc *http.ResponseController, timeout time.Duration, m wire.Message) error {
	if err := rc.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	defer rc.SetWriteDeadline(time.Time{})
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}
