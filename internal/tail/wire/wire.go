// Package wire defines the private NDJSON protocol between an ingester's live
// tail hub and a querier: the route pattern, the record types and the
// timestamp format. Both roles import it, but it depends on neither, so the
// ingester and querier still share no in-process state.
//
// A response is application/x-ndjson: one Message per line. The first line is
// always ready; batches carry the entries appended after the subscription
// started; a dropped line reports entries the subscriber never saw because it
// could not keep up. Timestamps are decimal Unix nanoseconds truncated to
// microsecond precision (the storage precision), so a querier can pass them to
// the LogQL engine without re-deriving the grid.
package wire

// Pattern is the ingester's internal live-tail route. It is served by the
// ingester role and must not be exposed as a public Loki endpoint: a querier
// subscribes to it over HTTP, and both sides carry the request's tenant
// through X-Scope-OrgID.
const Pattern = "GET /internal/ingest/tail"

// ContentType is the response media type: one JSON object per line.
const ContentType = "application/x-ndjson"

// Message types in the NDJSON stream.
const (
	// TypeReady is the first record. It acknowledges the subscription and
	// states the tenant and entry budget it was given.
	TypeReady = "ready"
	// TypeBatch carries zero or more entries. Entries are in push order and
	// may span several Loki streams; Label slices are shared within a batch.
	TypeBatch = "batch"
	// TypeDropped reports entries lost by an evicted subscriber. It is the
	// last record on that connection. Dropped is a count only: it carries no
	// descriptors, so a consumer must surface an explicit gap (an error or a
	// documented loss marker) and must not invent Loki dropped_entries
	// descriptors or present the stream as lossless.
	TypeDropped = "dropped"
)

// Message is one NDJSON record. Type selects which of the other fields apply:
// ready uses Tenant and BufferEntries, batch uses Entries, and dropped uses
// Dropped and Reason.
type Message struct {
	Type          string  `json:"type"`
	Tenant        string  `json:"tenant,omitempty"`
	BufferEntries int     `json:"bufferEntries,omitzero"`
	Entries       []Entry `json:"entries,omitempty"`
	Dropped       uint64  `json:"dropped,omitzero"`
	Reason        string  `json:"reason,omitempty"`
}

// Entry is one log line as the ingester accepted it. Timestamp is a decimal
// Unix-nanosecond value truncated to microseconds. Labels and Metadata are
// sorted by name and read-only: entries of one batch may share the same
// slices, so a consumer must clone before mutating.
type Entry struct {
	Timestamp string  `json:"timestamp"`
	Line      string  `json:"line"`
	Labels    []Label `json:"labels"`
	Metadata  []Label `json:"metadata,omitempty"`
}

// Label is one name/value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
