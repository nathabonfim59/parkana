# Live tail

`GET /loki/api/v1/tail` is a WebSocket on the querier. Lines in a live tail do
not come from the object store: the querier subscribes to every ingester over
`GET /internal/ingest/tail`, an internal NDJSON stream, and forwards the lines
that match the query. A tail therefore shows lines as soon as the ingester
acknowledges them, long before they are uploaded and queryable (see
[visibility](../ingest.md#visibility)). In `-wal.sync=interval` mode that means
before the fsync.

- Ingesters come from [`-tail.ingester-urls`](../configuration.md#live-tail).
  When it is empty and the same process runs the ingester role, the querier
  uses a loopback URL derived from `-http.addr`. With neither, the tail route
  answers 503. With split roles, list every ingester.
- Every ingester must confirm the subscription within 10 seconds.
- `limit` (default 100, at most `-logql.max-entries`) caps the lines in one
  frame, not in the session. `delay_for` takes 0 to 5 seconds. `start` only
  filters incoming lines; there is no backfill of older data.
- A tail runs until the client closes it: `-querier.timeout` does not apply.
- Live tail applies ingest pipelines, because it sees lines after the ingest
  transform, and does not apply read pipelines. A tail can therefore show a
  line differently from `query_range` for a stream with a read pipeline.

Slow clients have two buffers, both sized by
[`-tail.buffer-entries`](../configuration.md#live-tail) (default `1024`):

- On the ingester, a subscriber that cannot take a push is dropped with its
  queue. The querier then closes the WebSocket with status 1011, and the client
  has to reconnect.
- On the querier, a session that falls behind drops the newest lines and
  reports them in `dropped_entries`, as Loki does. It closes the WebSocket only
  after 1000 drops it could not describe.

`-tail.flush-interval` (default `100ms`) is the longest the querier waits
before it sends buffered lines. The `parkana_tail_*` metrics count
subscribers, entries, dropped entries and upstream errors.

## Grafana compatibility

This section records what the Grafana Loki data source sends to the tail
route and how Parkana answers it, including the failure and lifecycle
rules.

Plugin source:
[`src/datasource.ts#L407-L442`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L407-L442),
[`src/LiveStreams.ts`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LiveStreams.ts),
[`src/liveStreamsResultTransformer.ts`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/liveStreamsResultTransformer.ts).

The plugin builds a WebSocket URL and sends **only `query`** by default:

```text
ws://<datasource-base>/loki/api/v1/tail?query=%7Bservice_name%3D%22api%22%7D
```

- It does not send `start`, `limit` or `delay_for`. `maxLines` becomes the
  frontend circular-buffer capacity (`size`) and is never transmitted.
- A tail handler therefore has to work with only `query`. Implemented
  defaults: `limit` is 100 and caps one response frame (it is not a lifetime
  limit and is capped by `-logql.max-entries`); `delay_for` is integer seconds,
  default 0 and at most 5, matching Loki's `maxDelayForInTailing`.
- Tail has no historical backfill: it streams rows that arrive after the
  subscription is established. `start` only filters future incoming rows; it
  does not pull older rows from storage. Use `query_range` for history. When
  `start` is absent there is no lower bound: Loki's implied now-1h default only
  bounds its optional store backfill, which Parkana deliberately does not do.
- Requests are validated before the WebSocket upgrade. Malformed or duplicate
  parameters, non-log queries (including metric queries) and an absent ingester
  configuration produce Loki-style plain-text 400/503 responses; no socket is
  opened for an invalid request.
- `limit` caps one response frame; it never closes a healthy connection after
  N total rows. Remaining entries continue in later frames.
- Every configured ingester must acknowledge the private subscription with a
  matching-tenant `ready` record within 10 seconds before any row is written.
  All ingesters are subscribed, and their entries are merged by timestamp in a
  per-session delay queue bounded by `-tail.buffer-entries` (default 1024).
- The ingester flushes each private NDJSON record immediately. The querier's
  writer wakes on every queued batch and drains all currently due entries
  without waiting for the flush interval; it immediately continues while more
  batches are due, so frames can be written back-to-back. `-tail.flush-interval`
  (default 100ms) only bounds how long an idle writer sleeps before re-evaluating
  the queue, and caps the wait while `delay_for` holds entries back; it is
  neither a frame-rate cap nor a minimum coalescing window. `delay_for` is
  separate and defaults to zero. The latency acceptance test exercises this
  default interval, not a shortened one.
- Responses use Loki's `streams[].stream` plus `values` tuples of decimal
  Unix-nanosecond timestamp and line. The client transformer slices timestamp
  strings and does not read a third metadata tuple
  (`liveStreamsResultTransformer.ts#L16-L45`); with
  `X-Loki-Response-Encoding-Flags: categorize-labels` Parkana still emits
  Loki's third tuple object with `structuredMetadata` and `parsed`, and
  without it flattens those categories into the stream labels with
  parsed-over-metadata-over-stream precedence, as Loki's tail wire does.
- The client ignores `dropped_entries`
  (`liveStreamsResultTransformer.ts#L17`). Parkana still emits them
  faithfully rather than silently pretending an evicted subscriber saw
  everything. A normally closed connection is not retried; only abnormal
  close code 1006 is retried, up to 30 attempts
  (`LiveStreams.ts#L46-L65`), so an explicit loss must close with a meaningful
  code/reason rather than masquerade as a lossless reconnect.
- Bounded wire: the private NDJSON record budget is 4 MiB; a larger record
  from an ingester is an explicit gap. A public WebSocket frame is split at
  entry boundaries until its actual encoded JSON is at most 4 MiB; if one
  entry or dropped-entry descriptor cannot fit, the stream closes with 1011
  rather than truncating it. The splits and gaps are observable; no row is
  silently dropped to satisfy a size limit.
- The approved WebSocket implementation is
  [`github.com/coder/websocket` v1.8.15](https://github.com/coder/websocket/releases/tag/v1.8.15)
  (published 2026-06-15T03:38:49Z), a context-aware RFC 6455 server/client. The
  Go standard library has no WebSocket implementation.

The tail endpoint accepts all WebSocket origins, matching Loki; place Parkana
behind Grafana or a proxy instead of exposing it directly to browsers.

Failure and lifecycle semantics:

- Any upstream HTTP error, malformed or oversized record, wrong ready
  tenant/type, EOF/disconnect, unknown record type or a count-only
  `TypeDropped` closes the public stream with code 1011 and a meaningful
  reason. There is no silent lossless reconnect: the plugin retries only
  abnormal code 1006, so 1011 surfaces the gap. A `TypeDropped` count is never
  turned into invented Loki `dropped_entries` descriptors.
- When the querier's own bounded queue drops entries whose timestamp and
  labels it knows, it emits real `dropped_entries` in dedicated
  `{"streams":[],"dropped_entries":[...]}` frames (they may not be attached to
  the following data frame). Once the descriptor buffer (1000 entries) is
  exhausted, a further local drop cannot be described, so the session closes
  1011 with the undescribed count instead of silently clearing it.
- Service shutdown closes active streams with code 1001; a client disconnect
  simply ends its session. Sessions own and clear their queues and internal
  subscriptions when they end.
- Live tail is best effort. A push whose client cancellation is observed
  before its WAL receipt completes is omitted from tail even if the push later
  becomes durable. When cancellation and receipt race, whichever completion is
  observed wins.

Test status: the transport, defaults, tenant forwarding, pipeline/metadata
handling, delay ordering, queue bounds, failure/gap paths and lifecycle are
covered by source, unit and real-ingester push integration tests (the latter
pushes through the real push API after WAL durability and reads the row through
a real WebSocket client). Live tail is live-verified on `a89e876`: Explore
Live sockets against both datasources saw exactly the same 24 marked rows
through Alloy with equal timestamps, labels and lines.
