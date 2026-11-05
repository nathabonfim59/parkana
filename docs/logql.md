# LogQL and Grafana compatibility

This page records what Grafana sends to a Loki-compatible datasource, what
Parkana answers, and how the comparison corpus proves it.

Status for release 1.0.0: on the release commit the 112-entry Loki comparison
(`scripts/e2e-phase2.sh --full`, against Loki 3.7.9) is 112/112 equal, and the
OTLP comparison (`scripts/e2e-phase4-otlp.sh --full`) is 49/49 equal for every
transport. Each row below keeps its own evidence status; no row claims
compatibility beyond what its evidence covers. Log queries select
`[start, end)` like Loki: a line stamped exactly at `end` is not returned.

## Status vocabulary

| Status | Meaning |
|---|---|
| Source-verified | The exact request shape was read in the pinned primary source. This is evidence about the client, not proof that Parkana answers it correctly. |
| Engine-tested | Implemented and covered by unit/integration tests in the engine; not yet proven equal in a live run. A feature may have been exercised live with known divergences. |
| Live-verified | The same request was answered by both Loki and Parkana with equal semantics. Applies only to the paths a run exercised; Grafana-rendered captures are supporting evidence recorded separately. |
| Planned | Accepted design, implementation and tests still outstanding. |
| Deferred | Recognized LogQL syntax that must return `400` with a clear unsupported message (`plan.ErrUnsupported`). |

Target stack:

- Grafana **13.3.0**.
- External Loki datasource plugin **grafana/grafana-loki-datasource v13.2.5**
  (published 2026-10-29), installed synchronously with plugin auto-update
  disabled. Each e2e run must record the runtime plugin version from Grafana's
  plugin API next to the Grafana version.
- Loki **3.7.9** remains the side-by-side comparison baseline.

Grafana core no longer bundles the Loki datasource implementation; core points
at the external plugin (`public/app/features/loki-helpers/README.md` in
Grafana 13.3.0). Request shapes therefore come from the plugin tag, not from
Grafana core.

## How a request reaches Loki

Two plugin paths produce HTTP requests:

1. **Data queries** (`query_range`, `query`) go through the Grafana backend
   plugin. `makeDataRequest` builds them in
   [`pkg/loki/api.go#L46-L107`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L46-L107).
2. **Metadata/browser queries** (`labels`, `label/<name>/values`, `series`) are
   frontend resource calls. The backend prefixes the relative path with
   `/loki/api/v1/`
   ([`pkg/loki/loki.go#L127-L171`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/loki.go#L127-L171)).

Only the parameters below reach Loki under the plugin's own names. The
plugin's `queryType`, `supportingQueryType`, `refId`, `maxLines` and `expr`
are query-model fields: they select an endpoint or a header. The dashboard
time range is not sent under a model name; it becomes `start`/`end` (range) or
`time` (instant).

### Shared data-query parameters

From `makeDataRequest` (`api.go#L46-L107`):

- `query`: the LogQL expression.
- `direction`: always sent. The backend defaults to `backward` when the query
  model has no direction
  ([`pkg/loki/parse_query.go#L85-L100`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/parse_query.go#L85-L100)).
- `limit`: sent only when the model's `maxLines` is greater than zero. The
  plugin default is 1000
  ([`src/datasource.ts#L109-L110`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L109-L110),
  [`src/datasource.ts#L166`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L166));
  the normal data-query path fills it in
  ([`src/datasource.ts#L332-L343`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L332-L343)).
  Do not assume `limit` is present on every request: a request without
  `maxLines` sends none.
- `X-Loki-Response-Encoding-Flags: categorize-labels` is always set on data
  queries (`api.go#L107`).
- `X-Scope-OrgID` is set from Grafana's tenant metadata when present
  ([`api.go#L359-L384`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L359-L384)).
- `X-Query-Tags: Source=<value>` is set for supplementary queries. The logs
  volume value is `logvolhist`
  ([`api.go#L336-L357`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L336-L357)).

### Range queries

`GET /loki/api/v1/query_range` (`api.go#L66-L82`):

- `start` and `end`: decimal Unix **nanoseconds** (`UnixNano`).
- `step`: always sent, as integer milliseconds plus `ms`, for example
  `5000ms`. It is derived from the panel interval and time range; Loki ignores
  it for log-producing queries but the plugin sends it regardless. The corpus
  pins the equivalent duration forms `5s` and `1m`, which Loki parses
  identically.
- plus the shared `query`, `direction` and optional `limit`.

Concrete Explore logs request (bounds and step illustrative):

```text
GET /loki/api/v1/query_range?direction=backward&end=1793754000000000000&limit=1000&query=%7Bservice_name%3D%22api%22%7D&start=1793750400000000000&step=5s
X-Loki-Response-Encoding-Flags: categorize-labels
```

### Instant queries

`GET /loki/api/v1/query` (`api.go#L83-L87`):

- `time`: decimal Unix nanoseconds, taken from the query range end.
- plus `query`, `direction` and optional `limit`.
- **No** `start`, `end` or `step` is sent.

```text
GET /loki/api/v1/query?direction=backward&limit=1000&query=sum+by+%28level%29+%28count_over_time%28%7Bservice_name%3D%22corpus%22%7D%5B5m%5D%29%29&time=1793754000000000000
```

## Explore logs

A normal Explore logs pane is the range request above. The expression is the
user's selector plus optional pipeline; `direction`, `limit` and `step` are
added by the plugin. Parkana must accept the full request, apply the pipeline,
then order and limit globally as Loki does.

Corpus entries: `logs-base-api-range`, `logs-base-web-line-filter`,
`logs-job-only-legacy`, `logs-corpus-json-parser-filter`,
`logs-corpus-logfmt-line-filter`, `logs-corpus-plain-regexp`; all six
returned `EQUAL` in the integrated run (see
[Unverified / next evidence](#unverified--next-evidence)).

## Logs volume histogram

The plugin's Explore histogram path is **not**
`/loki/api/v1/index/volume` in v13.2.5. It is a supplementary `query_range`
whose expression is a `count_over_time` metric query
([`src/datasource.ts#L238-L267`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L238-L267)):

```logql
sum by (level, detected_level) (count_over_time(<log expression> | drop __error__ [$__auto]))
```

Details that matter:

- `<log expression>` is the user's expression with comments removed.
- The plugin appends `| drop __error__`, interpolates the combined expression,
  then parses it and checks for Lezer syntax-error nodes (`isQueryWithError`).
  If the interpolated combined expression has no syntax-error node, it uses
  the appended form; if it has one, it keeps the original expression. This is
  a syntax-error check, not a check for existing `__error__` usage: a query
  that already filters `__error__` still gets the drop appended (which is
  harmless), while an unparsable expression does not
  ([`src/queryUtils.ts#L209-L216`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/queryUtils.ts#L209-L216),
  [`src/queryUtils.ts#L164-L176`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/queryUtils.ts#L164-L176)).
- `$__auto` is not a Loki variable. The plugin backend replaces it before the
  request: for a range query with the step duration (`stepText`), for an
  instant query with the range duration
  ([`pkg/loki/parse_query.go#L34-L55`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/parse_query.go#L34-L55)).
  Because the histogram is a range query, `$__auto` equals the formatted
  `step`. The captured request used intervalMs 60000 and the plugin backend
  logged `Step: 1m0s`, so `$__auto` became `[1m]`; the corpus pins `step=1m`
  and Loki never sees the variable.
- `queryType=range` selects `query_range`; `supportingQueryType=logsVolume`
  reaches Loki only as `X-Query-Tags: Source=logvolhist` (the value maps in
  [`api.go#L336-L357`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L336-L357)).
  Neither changes LogQL semantics.
- The request inherits `direction` from the original target; `limit` follows
  the shared rule (sent only when the model's `maxLines` is positive). The
  corpus pins `limit=1000` as a representative Explore value, not as a claim
  that every histogram request carries it.

Exact no-parser expression, with `service_name="api"`; bounds, `step` and
`limit` in the URL below are representative:

```logql
sum by (level, detected_level) (count_over_time({service_name="api"} | drop __error__[1m]))
```

```text
GET /loki/api/v1/query_range?direction=backward&end=1793754000000000000&limit=1000&query=sum+by+%28level%2C+detected_level%29+%28count_over_time%28%7Bservice_name%3D%22api%22%7D+%7C+drop+__error__%5B1m%5D%29%29&start=1793750400000000000&step=1m
X-Query-Tags: Source=logvolhist
X-Loki-Response-Encoding-Flags: categorize-labels
```

Parser-compatible variants (drop comes after the existing pipeline):

```logql
sum by (level, detected_level) (count_over_time({service_name="corpus", format="json"} | json | drop __error__ [1m]))
sum by (level, detected_level) (count_over_time({service_name="corpus", format="logfmt"} | logfmt | drop __error__ [1m]))
```

Grouping semantics to match Loki:

- A missing `detected_level` must group as an **absent** label. The output
  series has `level` only; it must not gain `detected_level=""`.
- `drop __error__` removes the `__error__` label **and clears the pipeline's
  internal error state** (`LabelsBuilder.ResetError`), but its `Process`
  always keeps the line: `return line, true`. So the row is retained and a
  parser-failed row still counts in the histogram. Drop suppresses the error
  that would otherwise fail a metric query; it does not exclude anything
  ([Loki `drop_labels.go#L29-L58`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/drop_labels.go#L29-L58),
  [`labels.go#L254-L257`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/labels.go#L254-L257)).
- To exclude failed rows, the expression itself must filter, for example
  `| __error__=""`; Grafana does not add that filter when it appends the drop.
- On the no-parser form there is no error label or error state to clear, so the
  drop cannot change membership. That makes the required
  `plan.Source.Aggregate` fast path valid; the engine must still match
  evaluating the pipeline with `Select`.
- When a parser runs, parser failures set `__error__`. A metric query that
  leaves the error state set fails instead of returning a partial result;
  Grafana's appended drop avoids that failure but keeps the failed rows in the
  counts. Dropping only keeps the error text out of the grouping labels.

Corpus entries: `grafana-histogram-api`, `grafana-histogram-corpus-json`,
`grafana-histogram-corpus-logfmt`. Live Grafana rendering against Loki 3.7.9
was captured (see [Unverified / next evidence](#unverified--next-evidence)).
In the `c41c277` run all three returned the expected Parkana `400` because
the run predates the A7 metric implementation; on `a89e876` they answered
with correct values apart from a known retry overcount, and the millisecond
rounding and step alignment divergences are fixed in A9 with a live re-run
pending.

## Label names, label values and series

Plugin source:
[`src/LanguageProvider.ts#L156-L199`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L156-L199),
[`src/LanguageProvider.ts#L213-L235`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L213-L235),
[`src/LanguageProvider.ts#L245-L255`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L245-L255),
[`src/LanguageProvider.ts#L408-L431`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L408-L431).

- **Label names** use `GET /loki/api/v1/labels` with `start`, `end` and, when a
  stream selector exists, `query=<selector>`.
- When the `lokiLabelNamesQueryApi` feature toggle is off and a selector is
  present, the client instead calls `GET /loki/api/v1/series` with
  `match[]=<selector>`, `start` and `end`, then derives names from the returned
  label sets. Both paths must work.
- **Label values** use
  `GET /loki/api/v1/label/<url-escaped-name>/values` with `start`, `end` and
  optional `query=<selector>`.
- **Series** is the only one that uses `match[]`:
  `GET /loki/api/v1/series?match[]=<selector>&start=<ns>&end=<ns>`. The corpus
  CLI turns an entry's `query` into the first `match[]` and keeps any extra
  `match[]` values from `params`.
- `labels` and `label/<name>/values` scope with `query`, never `match[]`.
- The empty selector `{}` is deliberately omitted by the client; never send
  `query={}`. LogQL stream selectors require at least one non-empty matcher.
- Time bounds come from `getTimeRangeParams`, which multiplies Grafana
  millisecond bounds by 1,000,000
  ([`src/datasource.ts#L503-L505`](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L503-L505)).
  They are therefore always microsecond-aligned, but they are transmitted as
  decimal nanoseconds.
- The client filters `__aggregated_metric__`, `__tenant_id__` and
  `__stream_shard__` from label names itself; that is client behavior, not an
  endpoint contract.

Examples:

```text
GET /loki/api/v1/labels?end=1793754000000000000&query=%7Bservice_name%3D%22corpus%22%7D&start=1793750400000000000
GET /loki/api/v1/labels?end=1793754000000000000&start=1793750400000000000
GET /loki/api/v1/label/service_name/values?end=1793754000000000000&query=%7Bjob%3D%22corpus%22%7D&start=1793750400000000000
GET /loki/api/v1/series?end=1793754000000000000&match%5B%5D=%7Bservice_name%3D%22corpus%22%7D&start=1793750400000000000
```

Corpus entries: `grafana-labels-scoped`, `grafana-labels-unscoped`,
`grafana-label-values-service-name`, `grafana-label-values-format-scoped`,
`grafana-series-corpus`, `series-job-only-legacy`,
`series-multiple-selectors`; all returned `EQUAL` in the integrated run.

## Live tail

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
- **No historical backfill.** Tail streams rows that arrive after the
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

## Microsecond storage and strict comparison

Parkana stores timestamps as Parquet `TIMESTAMP(MICROS, UTC)`
(`internal/schema`). Loki pushes and Grafana transmits nanoseconds; Parkana
truncates to microseconds at ingest. Real nanosecond traffic therefore loses
sub-microsecond distinctions:

- Two entries whose timestamps differ only below 1 µs get the same stored
  timestamp. If line, complete labels and metadata also match they are
  identical stored rows and the `plan.Source` invariant below collapses
  them into one; if any of them differs, both rows are returned and only
  the order between equal timestamps is unspecified. Output timestamps
  remain decimal nanoseconds, as `microseconds * 1000`.
- Source selection is `From <= ts < Through` (half-open) while LogQL request
  bounds are inclusive; the engine translates them without inventing a
  timestamp.
- Grafana's metadata bounds are millisecond-derived (`ms * 1e6`), so they are
  already microsecond-aligned.

The comparison is strict by default:

- Every `pushgen` profile, including the default profile and the corpus
  profile, truncates its start time to `time.Microsecond` and advances in
  millisecond steps, so generated timestamps are microsecond-aligned and
  strict equality is meaningful.
- `lokicompare` defaults to `-timestamp-precision=exact`. The opt-in
  `-timestamp-precision=us` floors both sides onto the stored microsecond grid
  for real-world nanosecond traffic. It never applies silently.
- The default end-to-end comparison is strict. Do not weaken it to hide a
  precision bug.

`plan.Source` treats identical stored rows as one row: the same stored
timestamp plus the complete original labels, structured metadata and line.
The identity check runs before `Select` order/limit and before `Aggregate`
grouping, holds across files and promoted-label representations, and is a
`plan.Source` invariant with no flag, deliberately stronger than Loki's
active-head dedup. Push retries that would store the
same row twice are therefore idempotent. Two otherwise-identical events at
distinct nanoseconds that truncate to the same stored microsecond cannot be
distinguished and collapse; a different line, stream labels or metadata
keeps rows distinct.

## Upload and catalog visibility

A successful push is acknowledged after WAL durability, but that does not make
the row queryable immediately:

- Sealed memtables are written to local Parquet every
  `-ingest.flush-interval` (default 5s); memtables seal at
  `-ingest.memtable-max` (default 256 MiB).
- The uploader runs on `-ingest.upload-interval` (default 10s), and uploads
  when a partition reaches `-ingest.upload-min` (default 128 MiB) or a file is
  older than `-ingest.upload-max-age` (default 600s).
- The object is uploaded first, then its rows are added atomically to the
  catalog (`Catalog.AddFiles`). Only live catalog rows are queried. An object
  without a catalog row is invisible to readers.
- Compaction swaps files atomically: `Catalog.ReplaceFiles` marks the input
  rows deleted and lists the merged output in one transaction, so new
  `ListFiles` calls no longer return the inputs immediately. The old objects
  are removed only after the configured delete delay, which protects readers
  that already opened them.

Consequences for compatibility tests: push, then let the ingester flush,
upload and catalog before comparing (`lokicompare` and the e2e script must
wait for catalog-visible data). A newly pushed row may be visible in live tail
before `query_range` can see it. Parkana roles share nothing in process; the
querier never reads the ingester's WAL or memtable directly.

## Stats and volume

Parkana uses Loki 3.7.9 response shapes for the index endpoints. Volume responses use vector or matrix results, include `data.stats`, and encode sample timestamps as numeric Unix seconds.

`/loki/api/v1/index/stats` reports:

- `streams`: distinct matched stream label sets.
- `entries`: matched log rows.
- `bytes`: the sum of `len(line)` for matched rows, measured in bytes.
- `chunks`: Parquet files touched by the request.

`/loki/api/v1/index/volume` and `/loki/api/v1/index/volume_range` sum the same line bytes for the requested grouping. Label names and values, structured metadata, compression, and Parquet overhead do not contribute to this byte count. A bare `{}` selector groups by each full stream label set.

These values differ from Loki storage estimates. Loki derives statistics and volume from chunk metadata, including KiB-sized byte estimates and overlap estimates for partial chunks. Its chunks also have a different layout from Parquet files. Parkana computes row and line-byte totals directly; its chunks field counts Parquet objects. Loki can also collapse a bare volume selector to a single empty label set on its store path, while its ingester path retains full label sets. Parkana retains full label sets consistently. Numeric stats and volume totals therefore need storage-aware validation even when both stores contain the same log entries. The comparator follows that rule for all four stats counts: `streams`, `entries`, `bytes` and `chunks` are printed as LEFT/RIGHT estimates and never fail, while the documented non-negative integer shape is still required and `expect.stats` asserts exact RIGHT values.

Parkana partitions the millisecond-rounded volume range into adjacent windows: each non-final window includes its start and excludes the next boundary, and the final window includes the requested end. This differs from Loki 3.7.9, whose frontend inserts a one-millisecond split gap and truncates child request bounds to whole seconds before estimating volume. Parkana counts every matching stored row exactly once, so range bucket bytes sum to the instant volume over the same range when the result limit does not remove series. Response shapes and sample timestamps remain Loki-compatible: non-final samples use the boundary minus one millisecond, and the final sample uses the requested end.

Loki's `volume_range` result type is not tied to the number of request buckets: its frontend starts with a vector and switches to a matrix only when at least one result series has more than one sample (`pkg/querier/queryrange/volume.go`). One bucket is therefore a one-sample vector, and even multi-bucket requests can be vectors when each series appears in only one bucket. The comparator requires both sides to have the same result type; a vector must have exactly one sample per metric key, a matrix must contain at least one multi-sample series, and the empty result is always a vector (an empty matrix is rejected).

For a point range where start equals end, Parkana returns one final bucket at that timestamp, including when Loki would return no bucket because the point is step-aligned.

For aggregateBy=series, nonempty selectors group volumes by their matcher label names unless targetLabels is supplied; explicit targetLabels require a nonempty value for each target not already named in the selector, while bare {} without targets groups by the full labelset.

The comparator's optional RIGHT `expect` assertions use the same byte
definition: `bytes` is the sum of `len(line)` for matched rows and excludes
labels, structured metadata, compression and Parquet overhead.

The comparator keeps volume-range timestamp grids exact. The default e2e
corpus excludes a step-aligned point range (`start == end`) because Loki and
Parkana return different bucket sets for that case.

### Metric series limits

Parkana applies `-logql.max-series` (Loki's `JoinSampleVector` limit) to the
final response of a metric expression. Loki's query frontend can differ because
it maps an unshardable parent aggregation's child downstream: for
`stddev by (x) (count_over_time(...))` the frontend leaves `stddev`/`stdvar`
unsharded (`shardmapper.go` avoids their cross-shard merge), evaluates the
unreduced `count_over_time` child on a querier, and that querier applies its own
500-series limit to the child's intermediate cardinality. Parkana therefore
answers some queries that Loki rejects with `maximum number of series (500)
reached for a single query`. The corpus keeps such a comparison bounded by
adding a grouped child, `sum by (format, level) (count_over_time(...))`, so
both engines aggregate the child to a handful of series before the outer
`stddev`/`stdvar`; the outer aggregation still exercises the population
variance. This is a frontend-inner versus Parkana-final limit difference, not a
reason to weaken either engine's final limit.

## Feature matrix

Statuses follow the vocabulary above. *Live-verified* claims only the paths
the `c41c277` integrated run exercised plus live tail on `a89e876`;
*engine-tested* means implemented with unit/integration coverage and not
proven equal live; *pending* rows are not implemented.
The milestone column names the plan section that lands or landed the row.

### Syntax, parsers and filters

| Feature | Status | Milestone |
|---|---|---|
| Stream selectors, all matcher operators (`=`, `!=`, `=~`, `!~`), anchoring, missing label as `""` | Engine-tested; `=` and multi-matcher selectors live-verified | A1, A4 |
| Reject selectors whose matchers all match the empty string | Engine-tested; not exercised live | A1 |
| Line filters `|=`, `!=`, `|~`, `!~`, OR alternatives | Engine-tested; `|=` and `|~` live-verified | A2, A4 |
| `json` (including named paths), `logfmt` (`--strict`, `--keep-empty`, named extraction), `regexp` (named captures), `pattern` (`<_>` and named captures) | `json` named extraction live-verified; `logfmt`, `regexp` and `pattern` engine-tested | A5 |
| Label filters: string/numeric/duration/bytes with `and`/`or`/comma | Engine-tested; the numeric form is live-verified | A5 |
| `line_format`, `label_format`, `drop`, `keep` | Engine-tested | A6 |
| Template stages with the allowlist in [Template helpers](#template-helpers) | Engine-tested | A6 |
| `unwrap` and `duration_seconds`/`bytes` conversion | Engine-tested | A8b |
| Range functions: `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` | Engine-tested | A7 |
| Range functions: `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `stddev_over_time`, `stdvar_over_time`, `first_over_time`, `last_over_time`, `quantile_over_time`, `absent_over_time` | Engine-tested | A8b |
| Vector aggregations: `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `topk`, `bottomk`, `sort`, `sort_desc`, with `by`/`without` | Engine-tested | A8a |
| `vector(NUMBER)`: a number literal only, not arbitrary expressions (Grafana's health check sends `vector(1)`) | Engine-tested | A9 |
| `offset` | Engine-tested | A7/A8b |
| Scalar literals, arithmetic, precedence, vector-scalar comparison with `bool`, and default identical-label one-to-one vector/vector arithmetic and comparison | Engine-tested | A9 |
| Nested vector aggregation, grouping, parenthesized precedence | Engine-tested | A2, A8a |

The `json` stage's known limits are documented in the
`internal/logql/parsers.go` package doc: object recursion is capped at 1000
levels; concatenated-root resynchronisation finds the next opener (brace or
bracket) rather than tracking byte levels; a quoted bracket-looking key such
as `"["` is always a literal object key, never an array index (the pinned
`EachKey` uses a string-prefix heuristic); an extraction whose sanitized
nested name is empty is skipped; and a parsed field named `__error__` or
`__error_details__` cannot emit Loki's duplicate labels, so the pipeline's
error state wins.

### Template helpers

`line_format` and `label_format` run Go text/templates with
`missingkey=zero`. The allowlist below is unit-tested in the engine; a live
Loki/Grafana comparison of these stages is **pending**, so the matrix status
stays engine-tested rather than live-verified.

Supported helpers, matching the pinned Loki 3.7.9
[`functionMap` and reserved functions](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/fmt.go#L24-L133):

- Reserved functions: `__line__` is the line at this stage; `__timestamp__` is
  the entry timestamp as a `time.Time` (Loki binds `time.Unix(0, tsNanos)`).
- Legacy uppercase helpers, subject first: `ToLower`, `ToUpper`, `TrimSpace`,
  `Trim`, `TrimLeft`, `TrimRight`, `TrimPrefix`, `TrimSuffix`, `Replace`.
- Loki formatter helpers: `regexReplaceAll`, `regexReplaceAllLiteral`, `count`
  (the regular expression is the first argument), `urlencode`, `urldecode`,
  `bytes`, `duration`, `duration_seconds`.
- Sprig helpers, operand first: `lower`, `upper`, `contains`,
  `hasPrefix`, `hasSuffix`, `trim` (whitespace), `trimPrefix`, `trimSuffix`,
  `replace` (all occurrences), `default` (fallback, value).
- Go template builtins, operators, pipelines and control flow, including
  `printf`, `eq`, `if`, `range` and `define`/`template`.

Argument order differs by spelling: uppercase `TrimPrefix`/`TrimSuffix` take
the subject first, while sprig `trimPrefix`/`trimSuffix` take the prefix or
suffix first; sprig `contains`/`hasPrefix`/`hasSuffix` likewise take the
operand first. `replace` is `replace old new src` (all occurrences); uppercase
`Replace` is `strings.Replace` and needs the count argument.

A helper outside this set is rejected at compilation with `400`
`plan.ErrUnsupported`, before the query reads any files. This includes the
deferred Loki and sprig numeric and date APIs (`now`, `date`, `toDate`,
`unixEpoch`, `alignLeft`, `alignRight`, `toDateInZone`, `unixToTime`, `add`,
`div`, and the like). A template grammar error, including one that appears
after a deferred helper, is a `400` `plan.ErrBadQuery` instead.

A template execution error, such as an invalid regular expression, records
`__error__="TemplateFormatErr"` plus `__error_details__`, keeps the row, and
leaves the original line or the label destination unchanged.

A9 request normalization (implemented, source- and unit-tested; a live
re-run is pending): for metric range queries only, a positive
sub-millisecond `step` is rejected with a clear `400`, a deliberate
divergence from the Loki frontend panic; otherwise `step` is truncated to
whole milliseconds and `start` is aligned down / `end` aligned up by the
pinned metric splitter's signed-remainder formula before windows and
recursion are computed. Instant and log queries keep their bounds
unchanged, metric sample timestamps are emitted on the millisecond grid,
and `offset` retains sub-millisecond precision after normalization.
Reference: the pinned Loki 3.7.9 metric splitter
([`splitters.go#L308-L318`](https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/splitters.go#L308-L318),
[`codec.go#L629`](https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/codec.go#L629)).

### Deferred with `ErrUnsupported`

These are recognized as valid LogQL families but out of the phase-2 MVP.
Parsing must not misreport them as supported, and evaluation must return a
clear `400` (`plan.ErrUnsupported`), not a wrong result:

- Recognized pipeline stages, functions and filter forms outside the MVP,
  including `unpack`, `decolorize`, `label_replace`, `rate_counter`,
  `approx_topk`, `variants(...)`, pattern line filters (`|>`, `!>`) and
  `ip()` matchers.
- Vector/vector operations that use `on`, `ignoring`, `group_left` or
  `group_right` modifiers, and the logical set operators `and`, `or` and
  `unless`. The default identical-label one-to-one arithmetic/comparison
  case, required by Grafana's `vector(1)+vector(1)` health query, is not
  deferred: it is implemented and unit-tested under A9, with a live re-run
  pending (see [Unverified / next evidence](#unverified--next-evidence)).
- Arbitrary undocumented template helpers (see
  [Template helpers](#template-helpers)).

Subquery syntax (a trailing `[range:step]`) and recording-rule grammar are
invalid grammar in Parkana and return a `400` `plan.ErrBadQuery`, not
`plan.ErrUnsupported`: Loki 3.7.9 does not parse them either.

Deferred cases are also listed in
[`unsupported-probes.json`](../cmd/lokicompare/testdata/unsupported-probes.json):
10 RIGHT-only probe entries with the schema `name`/`query`/`endpoint`/
`params`/`expectedStatus`/`bodyContains`, hitting the raw `query` and
`query_range` endpoints and asserting a `400` status whose body contains
`unsupported`. It is a probe list, not a CLI corpus, and its RIGHT-only e2e
checks are pending. The default corpus's `expect.status` checks run against
both servers, so it covers only cases that are invalid on both sides.

### Endpoints

| Endpoint | Status | Owner |
|---|---|---|
| `GET /loki/api/v1/query` (metrics only), `/query_range` (logs and metrics) | Logs live-verified; metrics engine-tested, exercised live on `a89e876` with divergences since fixed and a re-run pending | A4-A9, part B |
| `GET /loki/api/v1/labels` (scoped and unscoped) | Live-verified | part B |
| `GET /loki/api/v1/label/{name}/values` | Live-verified | part B |
| `GET /loki/api/v1/series` with repeated `match[]` | Live-verified | part B |
| `GET /loki/api/v1/tail` (WebSocket) | Implemented; source/unit/real-ingester integration tested and live-verified on Grafana Live sockets; no historical backfill | A11 |
| `GET /loki/api/v1/index/stats` | Implementation- and comparator-tested, live endpoint parity pending; semantic policy in [Stats and volume](#stats-and-volume) | part B |
| `GET /loki/api/v1/index/volume` and `/loki/api/v1/index/volume_range` | Implementation- and comparator-tested, live endpoint parity pending; semantic policy in [Stats and volume](#stats-and-volume) | part B |
| `/ready`, `/loki/api/v1/status/buildinfo` | Existing foundation | done |

The `stats` and index `volume` corpus entries follow the comparison policy
in [Stats and volume](#stats-and-volume).

## Comparison corpus

File: [`cmd/lokicompare/testdata/grafana-corpus.json`](../cmd/lokicompare/testdata/grafana-corpus.json).

The corpus uses the frozen M1 schema: a JSON array of
`{name, type, query, params, expect}` plus the optional `lokiQuirk` opt-in.

- `type` is one of `range`, `instant`, `labels`, `label_values`, `series`, and
  the later `stats`, `volume`, `volume_range`.
- `query` is the LogQL expression, or the scoping selector for `labels`,
  `label_values` and the first `series` matcher.
- `params` values are a string or an array of strings. `limit`, `direction` and
  `step` pass through as Loki query parameters. `label_values` consumes exactly
  one non-empty `params.label`; it must not put the query in `params.query`.
- `lokiQuirk` is an optional flat string that opts into one documented Loki
  deviation. The only value is `scalar-timestamp`, valid only on `instant`
  entries without `expect`. Under it both servers must return a scalar result
  (otherwise the invocation fails with exit 2), values compare with the normal
  float tolerances, the RIGHT timestamp must be the request instant truncated
  to milliseconds, and the LEFT timestamp is only checked to be a numeric JSON
  timestamp, reported in a note and never compared. Loki 3.7.9 calls
  `model.TimeFromUnix` on an already-millisecond
  `LegacySample.TimestampMs` in `pkg/querier/queryrange/prometheus.go`
  (`marshalScalar`) and the protobuf scalar round trip overflows the timestamp
  again. Null, non-string, unknown and misplaced quirk values fail before any
  HTTP request; unopt-in entries keep the strict scalar comparison.
- `expect` is optional and asserts exact RIGHT counts on `stats`, `volume` and
  `volume_range`. It uses the schema documented by `lokicompare -h`: stats
  `streams`/`entries`/`bytes`, instant volume `metric`+`value`, and range
  volume `metric`+`values`. An empty `series` array asserts that RIGHT returns
  no series. Values compare as exact rationals, so the float flags never apply.
  Invalid expectations fail before any request.
- `expect.status` is a JSON integer in `[400, 599]`, valid on every entry
  type and exclusive with `stats`/`series`. Both servers must return exactly
  that status with a non-empty UTF-8 `text/plain` body (the message wording
  is not compared). A wrong status or an invalid error body is a semantic
  difference (exit 1); transport, read and size failures still exit 2. Loki
  3.7.9 rejects a bare log-selector instant query with HTTP 400
  (`ErrUnsupportedSyntaxForInstantQuery`,
  [`pkg/querier/http.go#L122-L127`](https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/http.go#L122-L127));
  `instant-log-selector-rejected` pins that and the form is reusable for
  other invalid or unsupported request fixtures.
- The full e2e corpus should use `expect.stats` and instant `volume`
  `expect.series` over the complete uploaded range. A checked-in corpus cannot
  carry static `volume_range` expectations for data that starts at generation
  time; use a runtime-generated corpus or a unit test for those timestamps.
- Each entry uses the CLI's `-start`/`-end` unless it supplies its own bounds.
  The CLI canonicalizes bounds to RFC3339Nano on both sides (Loki and Parkana
  both accept that form); the plugin itself sends decimal Unix nanoseconds to
  Loki. The corpus compares response semantics, not request bytes.
- Run with server roots (not full API URLs):

```sh
go build -o bin/lokicompare ./cmd/lokicompare
bin/lokicompare \
  -left http://localhost:3100 \
  -right http://localhost:3200 \
  -corpus cmd/lokicompare/testdata/grafana-corpus.json \
  -start 2026-11-04T00:00:00Z \
  -end 2026-11-04T01:00:00Z
```

The CLI still exits 2 while any entry fails: the `c41c277` run exited 2 with
14 equal and 4 expected unsupported-metric failures.

The checked-in corpus now covers the feature families above with 112
entries: 51 `range`, 42 `instant`, 2 `labels`, 2 `label_values`, 3
`series`, 7 `stats`, 4 `volume` and 1 `volume_range`, totalling 89 positive
metric/log queries, 4 mutually-invalid `400` cases and 19 metadata-endpoint
entries. `stats` and instant `volume` entries carry exact RIGHT
expectations derived from the 22,200-entry dataset (553,491 line bytes
across 21 label sets); `volume_range` has no static timestamp expectation.
The corpus runner's tests assert concrete `scalar`/`vector`/`matrix`/
`streams` result types (scalar values 7, 9, 512 and 1), real unsupported
classification and mutual-invalid `BadQuery`, with no skips. The four scalar
instant entries carry `lokiQuirk: "scalar-timestamp"`; `agg-stddev` and
`agg-stdvar` use a bounded `sum by (format, level)` child so both engines
evaluate the population variance without hitting Loki's inner child limit.
The original 18 fixtures are preserved as the first entries, including the two
captured request shapes: `logs-base-api-range` at `step=5s` and the byte-exact
`drop __error__[1m]` histogram at `step=1m`, both at `limit=1000` with
`direction=backward`. The corpus covers the three deterministic pushgen
datasets and follows the frozen M1 schema.

### Required datasets

All three generators send to Alloy's push URL,
`http://alloy:3100/loki/api/v1/push`.

Base dataset (20,000 entries, 10,000 per stream):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=api -stream=web \
  -label=job=frontend -label=env=dev -label=level=info \
  -total=20000 -rate=1000 -batch=100
```

Labels: `service_name=api|web`, `job=frontend`, `env=dev`, `level=info`.
Lines are `id=<n> stream=<name>`.

Job-only dataset (1,000 entries, no `service_name` label):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=legacy -stream-label=job -label=env=dev \
  -total=1000 -rate=1000 -batch=100
```

Labels: `job=legacy`, `env=dev`.

Corpus dataset (1,200 entries, deterministic mixed formats):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -profile=corpus \
  -format=json -stream=corpus -label=job=corpus -label=env=dev \
  -total=1200 -rate=1000 -batch=100
```

Labels: `service_name=corpus`, `job=corpus`, `env=dev`, plus per-record
`format=json|logfmt|plain`, `level=info|warn|error`, `pod=pod-a|pod-b`.
Structured metadata: `trace_id`, `request_id`. The 1,200 records always total
exactly 1,200 rows and start at a microsecond-aligned instant, one millisecond
apart. Only configured stream names are assigned round-robin; `format`,
`level` and `pod` are chosen per record from the seeded splitmix64 stream. For
the default total the fixture yields 423 JSON, 384 logfmt and 393 plain rows
and covers all 18 format×level×pod label sets, so the mix is deterministic but
not even.

Corpus queries rely on these fixture fields:

- JSON lines nest status at `response.status`; the JSON corpus query uses
  named extraction, `| json status="response.status"`, then filters `status`.
  Logfmt and plain lines carry flat `status=`.
- All formats embed `id`, numeric `latency` and `bytes`, a Go `duration`
  string, a byte-unit `size`, and a `msg`.
- `format` is a stream label, so parser-specific queries select compatible
  rows.
- `grafana-histogram-api` pins `step=1m`, matching the captured intervalMs
  60000 whose backend `$__auto` interpolation yields the `[1m]` already
  present in the expression; the two corpus-dataset histograms pin
  `step=60000ms`.

### Corpus entries

The original 18 representative entries, preserved inside the full corpus:

| Name | Type | Purpose |
|---|---|---|
| `logs-base-api-range` | range | Plain Explore logs request on the base dataset; `step` pinned to `5s`. |
| `logs-base-web-line-filter` | range | `|=` filter on the second base stream. |
| `logs-job-only-legacy` | range | Dataset without `service_name`. |
| `logs-corpus-json-parser-filter` | range | `json` parser plus numeric label filter. |
| `logs-corpus-logfmt-line-filter` | range | Logfmt rows with a substring filter. |
| `logs-corpus-plain-regexp` | range | Plain rows with an unanchored RE2 line filter. |
| `count-instant-by-level` | instant | Instant `sum by (level)` `count_over_time`. |
| `grafana-histogram-api` | range | Captured no-parser Grafana histogram (`drop __error__[1m]`, `step=1m`); missing `detected_level`. |
| `grafana-histogram-corpus-json` | range | Histogram over a JSON pipeline. |
| `grafana-histogram-corpus-logfmt` | range | Histogram over a logfmt pipeline. |
| `grafana-labels-scoped` | labels | `query=<selector>` label names. |
| `grafana-labels-unscoped` | labels | No selector, as the browser sends initially. |
| `grafana-label-values-service-name` | label_values | `label/service_name/values` scoped by `query`. |
| `grafana-label-values-format-scoped` | label_values | Values for `format` under a narrower selector. |
| `grafana-series-corpus` | series | `match[]` series request. |
| `series-job-only-legacy` | series | Series on the job-only dataset. |
| `series-multiple-selectors` | series | Repeated `match[]` array handling. |
| `instant-log-selector-rejected` | instant | Loki rejects a bare log selector at the instant endpoint with HTTP 400; `expect.status` pins it. |

Outside the corpus: tail (WebSocket, not an HTTP compare), the RIGHT-only
unsupported probes above, and `volume_range` static timestamp expectations
(the deterministic generators start at `time.Now`, so those belong to unit
tests or runtime-generated corpora).

## Unverified / next evidence

- Live browser capture 2026-11-04T07:42:24Z: Grafana 13.3.0 with the Loki
  datasource plugin 13.2.5 against Loki 3.7.9 (HAR
  `/tmp/sol-b/grafana-explore.har`, histogram request
  `/tmp/sol-b/grafana-histogram-request.json`, route evidence
  `/tmp/sol-b/loki-histogram-route-evidence.json`, screenshot
  `/tmp/sol-b/grafana-explore-loki.png`). This is a Loki baseline request
  capture, not Parkana compatibility verification; feature rows stay pending
  until exercised against Parkana. It shows intervalMs 5000 for the logs
  browser and 60000 for the histogram, with `$__auto` interpolated to `[1m]`
  (`Step: 1m0s`) in the Grafana datasource backend before Loki and no
  `index/volume` request issued. The captured bounds span one hour; the
  corpus carries no bounds and replays with `-start` exactly one hour before
  `-end` (absolute RFC3339 or Unix nanoseconds, both accepted by the CLI).
  Everything request-shaped outside this capture remains source-verified
  from v13.2.5.
- Integrated parity run on combined stack `c41c277` (part A `192b27f` plus
  the approved IncludeMeta amendment); evidence produced by sol-b and
  reviewed by the orchestrator. Pinned stack: Garage 2.5.0, Loki 3.7.9,
  Alloy 1.21.3, Grafana 13.3.0, datasource plugin 13.2.5 (runtime-reported);
  22,200 generated/Loki/catalog rows. The compiled `lokicompare` ran the
  then-current 18-entry default corpus once: **14 EQUAL, 0 DIFF, 4 failed,
  exit 2**. Every failure was the expected Parkana `400` "metric and scalar
  queries are not implemented yet" (the run predates A7) on
  `count-instant-by-level`, `grafana-histogram-api`,
  `grafana-histogram-corpus-json` and `grafana-histogram-corpus-logfmt`.
  `EQUAL` covered all six log entries (`logs-base-api-range`,
  `logs-base-web-line-filter`, `logs-job-only-legacy`,
  `logs-corpus-json-parser-filter`, `logs-corpus-logfmt-line-filter`,
  `logs-corpus-plain-regexp`), all labels/label-values/series entries and
  `instant-log-selector-rejected` (both sides `400`). Separately, a Grafana
  Explore `{service_name="api"}` query returned 1,000 actual rows per
  datasource with frame data equal except Grafana-assigned frame IDs, a
  Grafana-rendered capture rather than a lokicompare `EQUAL`
  (`grafana-summary.json`; screenshots and HAR under `/tmp/sol-b/`).
- Follow-up parity run on combined stack `a89e876` (includes A7 `e117851`
  and A8a `20c9300`); evidence produced by sol-b and reviewed by the
  orchestrator. The compiled `lokicompare` ran the same corpus: **14 EQUAL,
  4 DIFF, 0 failed**. All metric values were correct except a known retry
  overcount; the remaining differences were sample millisecond rounding and
  range step alignment, both fixed in A9 and awaiting a live re-run. The
  retry overcount is tracked separately under the `plan.Source` dedup
  invariant, not A9. Live tail was verified in the same run: both Grafana
  Explore Live sockets saw exactly the same 24 marked rows through Alloy
  with equal timestamps, labels and lines
  (`/tmp/sol-b/a7-tail-parity/summary.md`, `tail-summary.json`,
  `/tmp/sol-b/grafana-a7-parkana-live.png`,
  `/tmp/sol-b/grafana-a7-loki-live.png`, `/tmp/sol-b/grafana-a7-tail.har`).
- Grafana datasource health check: the plugin issues the instant query
  `vector(1)+vector(1)`; Loki returned `200`/OK and Parkana `400`
  (`vector() is not supported`) in the `c41c277` run
  (`grafana-health-logs.json`). `vector(NUMBER)` plus the default
  identical-label vector/vector arithmetic case is implemented and
  unit-tested under A9; a live re-run of the health check is pending.
- Runtime plugin version must be read from Grafana's plugin API in every e2e
  run and recorded with the Grafana version.
- Public tail WebSocket: the ordinary Grafana Live path is live-verified on
  `a89e876` as recorded above. `TypeDropped` handling, default
  `limit`/`delay_for` and visible-gap close codes are implemented and
  unit/integration-tested (M4), not exercised live.
- Exact `step` values depend on panel width and max data points; the corpus
  pins the captured `5s` and `1m` forms, not every Grafana possibility.
- Label-name feature-toggle path (`labels` vs `series`) is source-verified;
  which path the deployed Grafana 13.3.0 uses depends on its feature-toggle
  configuration and must be recorded during live verification.
- Index stats/volume entries in the corpus apply the
  [Stats and volume](#stats-and-volume) policy rather than comparing
  implementation-specific chunk or byte estimates. A checked-in default
  corpus cannot carry static `volume_range` expectations because the
  deterministic generators start at `time.Now`; those belong to unit tests
  or runtime-generated corpora.
- All corpus expressions were checked against Loki 3.7.9's pinned grammar
  (`pkg/logql/syntax/syntax.y`): `logRangeExpr`, `rangeAggregationExpr`,
  `vectorAggregationExpr`, `pipelineStage` (line filter, JSON expression
  parser, label filter, drop), `labelFilter`/`numberFilter` and
  `jsonExpressionParser`. See the source list below.
- Baseline histogram rendering in Grafana was captured against Loki 3.7.9;
  dashed "absent" series and legend behavior remain pending. The `a89e876`
  metric divergences (millisecond rounding and step alignment) are fixed in
  A9 pending a live re-run; the retry overcount is tracked under the
  `plan.Source` dedup invariant.

## Primary sources

- Grafana Loki datasource v13.2.5, data request builder:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L46-L107>
- Query model defaults (direction, query type, `$__auto`):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/parse_query.go#L34-L100>
- Supplementary logs-volume expression:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L238-L267>
- Live query URL construction:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L407-L442>
- Time bounds (ms × 1e6):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L503-L505>
- Label names, values and series:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L156-L255>
  and
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L408-L431>
- Resource path prefix `/loki/api/v1/`:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/loki.go#L127-L171>
- Histogram suitability and the Lezer syntax-error check
  (`isQueryWithError`):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/queryUtils.ts#L209-L216>
- Loki 3.7.9 grammar: log ranges and pipelines
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L128-L155>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L215-L227>;
  aggregations
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L169-L185>;
  JSON expression parser
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L276-L281>;
  label and number filters
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L302-L312>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L352-L360>;
  drop labels
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L371>
- Loki 3.7.9 drop stage keeps the row and clears the error:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/drop_labels.go#L29-L58>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/labels.go#L254-L257>
- Tail result transformer and dropped entries:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/liveStreamsResultTransformer.ts#L16-L45>
- Tail retry behavior:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LiveStreams.ts#L46-L65>
- Loki 3.7.9 scalar timestamp unit error and proto overflow:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/prometheus.go#L163-L175>
  and
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/codec.go#L1761-L1772>
- Loki 3.7.9 volume_range result-shape rule:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/volume.go#L158-L165>;
  bucket alignment and millisecond rounding:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/util/time.go#L94-L119>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/util/conv.go#L29-L39>
- Loki 3.7.9 unshardable stddev/stdvar mapping:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/shardmapper.go#L238-L343>
  with the range-downstream path at
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/shardmapper.go#L436-L451>
