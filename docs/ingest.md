# Ingest

The ingester role accepts logs over the Loki push API and OTLP, makes them
durable in a write-ahead log, buffers them in memory, writes them to local
Parquet and uploads them to the object store. This page covers what you can
send, how Parkana names and stores it, what an acknowledgment guarantees, and
when the data becomes queryable. Flags and their defaults are in
[configuration](configuration.md#ingest).

```text
 push / OTLP ──► decode, limits, validation ──► ingest pipelines
                                                     │
                                                     ▼
                                WAL (per tenant) + memtable ──► 204
                                                     │ every -ingest.flush-interval
                                                     ▼
                              local Parquet under <data.dir>/parquet
                                                     │ -ingest.upload-min or -ingest.upload-max-age
                                                     ▼
                   object store: data object, then sidecar ──► catalog row
                                                     │
                                                     ▼
                                           visible to queries
```

## Push API

`POST /loki/api/v1/push` takes the same bodies as Loki:

- `Content-Type: application/json`: Loki's JSON push format. Timestamps are
  nanosecond strings or RFC 3339.
- Any other content type, usually `application/x-protobuf`: a
  snappy-compressed `logproto.PushRequest`.
- `Content-Encoding: gzip` is expanded first; empty, `identity` and `snappy`
  are taken as they are. Any other encoding answers 400.

A request without a parseable `Content-Type` answers 400, as in Loki.

```sh
curl -s -w '%{http_code}\n' -H 'Content-Type: application/json' \
  --data-raw "{\"streams\":[{\"stream\":{\"service_name\":\"checkout\",\"level\":\"error\"},\"values\":[[\"$(date +%s%N)\",\"payment failed order=42\"]]}]}" \
  http://localhost:3100/loki/api/v1/push
```

prints `204`.

Every push is checked as a whole before anything is stored; one bad stream or
line rejects the request:

| check | limit | answer |
|---|---|---|
| request body | [`-ingest.max-request-size`](configuration.md#ingest), default `16MiB`, also applied after gzip and snappy expansion | 400 `ingest: reading request body: http: request body too large` or `ingest: request body exceeds the limit` |
| one line | `-ingest.max-line-size`, default `256KiB` | 400 `ingest: line exceeds 262144 bytes` |
| label and metadata names | `[a-zA-Z_][a-zA-Z0-9_]*`, no duplicates | 400 |
| tenant | `X-Scope-OrgID` absent or `default` | 401 |

An oversized body answers 400, not 413. Label and metadata values are not
checked, and there is no limit on the number of labels. Parkana does not
reject out-of-order, old or future timestamps, and has no rate limits.

The full set of answers:

| status | when |
|---|---|
| 204 | the push is acknowledged (see [durability](#durability-and-acknowledgment)), or its streams carry no entries and nothing is stored |
| 400 | the body cannot be decoded, fails a check above, or an ingest pipeline rejected it |
| 401 | `X-Scope-OrgID` names another tenant |
| 422 | the request has no streams: `error at least one valid stream is required for ingestion`, as in Loki (new in 1.2.0) |
| 500 | an ingest pipeline failed, or the push could not be stored |
| 503 | the ingester is replaying its WAL, shutting down or has failed (`ingest: not accepting pushes`) |

Parkana never answers 429. When the client goes away before the push is
acknowledged, nothing is answered.

## OTLP

`POST /v1/logs` is the OTLP/HTTP logs endpoint. It takes
`application/x-protobuf` and `application/json` export requests, optionally
with `Content-Encoding: gzip`, and applies the same limits, pipelines and
durability as the push API. An export without log records answers 204 and
stores nothing. OTLP over gRPC is not served; send it through a collector.

Records are mapped to labels and structured metadata as Loki 3.7.9 does with
its default OTLP configuration:

- These resource attributes become stream labels: `service.name`,
  `service.namespace`, `service.instance.id`, `deployment.environment`,
  `deployment.environment.name`, `cloud.region`, `cloud.availability_zone`,
  `k8s.cluster.name`, `k8s.namespace.name`, `k8s.pod.name`,
  `k8s.container.name`, `container.name`, `k8s.replicaset.name`,
  `k8s.deployment.name`, `k8s.statefulset.name`, `k8s.daemonset.name`,
  `k8s.cronjob.name` and `k8s.job.name`.
- Without `service.name`, the first of those labels whose name is one of
  `service`, `app`, `application`, `app_name`, `name`,
  `app_kubernetes_io_name`, `container`, `container_name`,
  `k8s_container_name`, `component`, `workload`, `job` or `k8s_job_name`
  supplies `service_name`; with none, it is `unknown_service`.
- Every other resource attribute, every scope and log attribute, and the
  record fields `severity_text`, `severity_number`, `trace_id`, `span_id`,
  `flags`, `observed_timestamp` and the dropped-attribute counts become
  structured metadata, as do `scope_name` and `scope_version`. Nested maps are
  flattened with `_`.
- Attribute names are normalized like Prometheus names: characters outside
  `[A-Za-z0-9]` become `_`, runs of `_` collapse, and a leading digit gets a
  `key_` prefix (`k8s.pod.name` becomes `k8s_pod_name`).
- The entry time is the record's time, else its observed time, else the time
  Parkana received it. The line is the body rendered as a string.

The [OTLP comparison stack](development/otlp-comparison.md) checks this mapping against a real Loki.

## Sending logs

The configs below send to a Parkana at `parkana.example.com:3100`.

Grafana Alloy, Loki push. Add a second `endpoint` block to tee the same logs
to Loki:

```hcl
loki.write "parkana" {
  endpoint {
    url = "http://parkana.example.com:3100/loki/api/v1/push"
  }
}
```

Grafana Alloy, OTLP:

```hcl
otelcol.receiver.otlp "default" {
  http {}
  grpc {}

  output {
    logs = [otelcol.exporter.otlphttp.parkana.input]
  }
}

otelcol.exporter.otlphttp "parkana" {
  client {
    endpoint    = "http://parkana.example.com:3100"
    compression = "gzip"
  }
  encoding = "proto"
}
```

OpenTelemetry Collector:

```yaml
receivers:
  otlp:
    protocols:
      grpc: {}
      http: {}

exporters:
  otlphttp/parkana:
    endpoint: http://parkana.example.com:3100
    compression: gzip

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp/parkana]
```

The `otlphttp` exporter appends `/v1/logs` to `endpoint`. Promtail and any
other Loki client need only the push URL:

```yaml
clients:
  - url: http://parkana.example.com:3100/loki/api/v1/push
```

Clients deliver at least once: a retry after a timeout can send a batch
Parkana already stored. Queries count identical rows once and the compactor
removes the copies (see [query](query/index.md#what-runs-in-duckdb-and-what-runs-in-go)).

## Stream naming

A stream is the dataset a log line belongs to. It decides the object key
prefix, which files a query reads and which pipelines apply. It is not a Loki
label set: one stream holds many label sets.

[`-stream.labels`](configuration.md#streams) (default `service_name,job`)
lists the labels that name the stream. The first label in the list that is
present with a non-empty value wins; a line with none of them goes to the
`default` stream. The value is lower-cased, cut to 128 bytes, and every byte
outside `[a-z0-9_-]` becomes `_`: `service_name="Checkout.API"` is the stream
`checkout_api`. Different values can therefore map to one stream
(`My.App` and `my_app`).

Ingest pipelines can change the stream with `set_stream`; see
[pipelines](pipelines/index.md).

## Promoted labels

[`-ingest.promoted-labels`](configuration.md#ingest) chooses, per stream, the
labels stored as their own Parquet columns instead of in the `labels` map. The
value is a list of `stream=col1,col2` assignments:

```sh
parkana -ingest.promoted-labels 'checkout=level,region;*=level'
```

- `*` is every stream without its own assignment. The default is `*=level`.
- Separate assignments with `;` or repeat the flag. The first `*` you give
  replaces the default one; giving `*` or a stream twice is an error.
- An empty value removes every assignment, the default included.
- Stream names must already be in sanitized form (`[a-z0-9_-]`); column names
  must be label names. `line`, `labels`, `meta` and names starting with `_`
  are reserved, and two names that differ only in case are rejected.

A promoted label is removed from the `labels` map; rows without it store a
null. Promote labels you filter on often and that have few values: the catalog
records their values per file, so queries can skip files (see
[query](query/index.md#how-a-query-finds-its-files)). Changes apply to new files
only; see [schema evolution](storage.md#schema-evolution).

## Durability and acknowledgment

Each tenant has an append-only WAL under `<data.dir>/wal/<tenant>/`, in
segments of about 128 MiB. Pushes are written in batches: all pushes waiting
when the writer is free go out in one write, and in `always` mode one fsync
covers the batch.

[`-wal.sync`](configuration.md#wal) decides when the ingester answers 204:

| mode | 204 after | a process crash loses | a machine crash loses |
|---|---|---|---|
| `always` (default) | the WAL write and its fsync | nothing acknowledged | nothing acknowledged |
| `interval` | the WAL write | nothing acknowledged | up to `-wal.sync-interval` (default `1s`) of acknowledged pushes |

In both modes the push is also in the memtable when the 204 is sent. In
`interval` mode the WAL is fsynced every `-wal.sync-interval`, when a segment
rotates and at shutdown. A failed fsync stops the ingester: `/ready` fails and
pushes answer 503.

Live tail shows a push when it is acknowledged, so in `interval` mode a tail
can show lines that a machine crash then loses.

## Memtable, flush and upload

Pushes land in a per-tenant memtable generation. A generation is sealed when
its size estimate (line, label and metadata bytes plus 96 bytes per entry)
reaches [`-ingest.memtable-max`](configuration.md#ingest) (`256MiB`), when the
WAL bytes it covers reach `-ingest.wal-max` (`128MiB`), or at the next flush,
whichever comes first. Neither limit by itself bounds the process memory
or the local disk; the [back-pressure](#back-pressure) bounds do.

Every `-ingest.flush-interval` (`5s`) the ingester seals every generation and
writes it to local Parquet under `<data.dir>/parquet/<tenant>/`, one file per
stream and UTC hour, sorted by time. Up to `-ingest.flush-concurrency` (`4`)
files are written in parallel. A generation becomes visible locally in one
atomic commit of the tenant's `manifest.json`; only then is its part of the
WAL deleted.

Every `-ingest.upload-interval` (`10s`) the uploader looks at each stream and
hour. A group is uploaded when its local files reach `-ingest.upload-min`
(`128MiB`) or its oldest file was written locally at least
`-ingest.upload-max-age` (`10m`) ago. The files of a group are merged into one
object of up to `-ingest.upload-merge-max` (`2GiB`); a single larger file is
uploaded alone. Up to `-ingest.upload-concurrency` (`4`) uploads run in
parallel. Merges remove exact duplicate rows.

Each upload goes through the same steps:

1. Reserve the upload in the manifest.
2. Merge the local files into one Parquet file under
   `<data.dir>/parquet/<tenant>/.uploads/`.
3. Put the data object in the store.
4. Build the search sidecar and put it next to the object (see
   [search sidecars](storage.md#search-sidecars)).
5. Add the file to the catalog. From now on it is queryable.
6. Mark it published in the manifest and delete the local files.

Each step is recorded in the manifest before the next starts.

### Upload memory

Uploads are admitted against a memory budget,
[`-ingest.upload-memory`](configuration.md#ingest). With the default `0`
the budget is half of the smaller of the container (cgroup v1 or v2) memory
limit and `GOMEMLIMIT`, or 512 MiB when neither is set. The startup log line
`upload memory budget` shows the bytes and where they came from (`explicit`,
`cgroup`, `gomemlimit` or `fallback`).

Each upload is charged an estimate computed from the Parquet footers of its
inputs; an upload whose footers cannot be read is charged the whole budget. An
upload larger than the whole budget is not refused: it waits until nothing
else runs, then runs alone, and is counted in
`parkana_upload_oversized_jobs_total`. `parkana_upload_active_jobs`,
`parkana_upload_inflight_estimate_bytes`, `parkana_upload_job_source_bytes`,
`parkana_upload_merge_seconds` and `parkana_upload_put_seconds` show what the
uploader is doing.

## Back-pressure

Two bounds stop the ingester from taking more data than it can get rid of
(new in 1.2.0). Both count bytes still on their way to the object store,
summed over every tenant of the process:

| bound | counts | limit (default) | grows when |
|---|---|---|---|
| `memory` | the memtable size estimate (line, label and metadata bytes plus 96 bytes per entry) of every row not yet in local Parquet: active generations, sealed generations waiting for a flush and generations whose flush failed and waits for the next pass | 4 x `-ingest.memtable-max` (`1GiB`) | writing local Parquet fails or falls behind |
| `local` | local Parquet not yet uploaded: committed files not yet uploaded, plus the merge output under `.uploads/` of every upload not yet published (its sources still count too, since both are on disk); quarantined files are not counted | [`-ingest.max-unuploaded`](configuration.md#ingest); `0` is 8 x `-ingest.upload-merge-max` (`16GiB`) | uploads fail, for example because the object store is down, or fall behind |

The ingester checks both before a push reaches the WAL. When a counter is at
or above its limit, the push answers `429 Too Many Requests` on
`/loki/api/v1/push` and on `/v1/logs`, nothing is stored, and the one-line
body names the bound, the bytes waiting and the limit:

```text
ingest: back-pressure: 17179869200 bytes of local Parquet wait for upload, limit 17179869184 (-ingest.max-unuploaded); retry after 10s
```

`Retry-After` is the longer of `-ingest.flush-interval` and
`-ingest.upload-interval` in whole seconds, `10` at the defaults: a flush or an
upload pass is the earliest moment a counter can drop. There is no hysteresis:
the first push after a pass brings the counter under its limit is accepted.
The check reads two counters without a lock, so pushes admitted at the same
moment can pass a limit by their own size.

The common clients all retry a `429` by default, but only the OpenTelemetry
Collector waits as `Retry-After` says; the others use their own backoff.
Promtail, Alloy and the Collector drop a batch once their retries run out.
From the source of the versions listed:

| client (version read) | waits between retries | gives up |
|---|---|---|
| Promtail (Loki v3.7.10) | its own backoff, `backoff_config`: 0.5 s doubling up to 5 min; ignores `Retry-After` | after 10 retries (about 8.5 minutes), dropping the batch; `drop_rate_limited_batches: true` drops a `429` batch at once |
| Alloy `loki.write` (v1.22.0) | its own backoff, `min_backoff_period` 500 ms up to `max_backoff_period` 5 min; ignores `Retry-After` | after `max_backoff_retries` (10), dropping the batch; `retry_on_http_429 = false` drops a `429` batch at once |
| Vector `loki` sink (v0.62.0) | its own backoff, `request.retry_initial_backoff_secs` 1 s up to `request.retry_max_duration_secs` 30 s; ignores `Retry-After` | practically never: `request.retry_attempts` defaults to the largest integer |
| OpenTelemetry Collector `otlphttp` exporter (v0.166.0) | at least the `Retry-After` delay (seconds or an HTTP date), for `429` and `503` | after `retry_on_failure.max_elapsed_time` (5 min by default) |

A client that drops batches loses data during a long outage, so raise its
retry limits to cover how long the object store may be down.

The memory limit is 4 memtables: one being filled, one being flushed and two
of slack for a slow disk. Past that, flushes are failing. A healthy ingester
keeps about one `-ingest.upload-min` (`128MiB`) per stream and hour locally,
plus the merge outputs of running uploads, far below the 16 GiB local
default. An explicit `-ingest.max-unuploaded` must be at least
`-ingest.upload-merge-max`, so a full merge always fits.

WAL replay at startup never checks the bounds: everything in the WAL is
rebuilt. After a long outage the first pushes can answer `429` until uploads
catch up. `/ready` stays ready under back-pressure: it is process-wide, so with
`-roles=all` failing it would also take the querier out of a load balancer,
and clients retry a `429` anyway. A `503` keeps meaning "not ready": the WAL
replay, shutdown or a fatal error such as a failed fsync. A push that cannot
be stored answers `500`.

### Back-pressure and upload memory

The [upload memory](#upload-memory) budget and the two bounds limit different
things, and the budget never refuses anything:

- The budget (`-ingest.upload-memory`; half the container limit or
  `GOMEMLIMIT`, or 512 MiB when neither is set) limits how much merge memory
  uploads use at once. An upload that does not fit waits for running uploads;
  no upload and no push is ever refused because of it.
- The `memory` bound covers the memtable only, not merge memory. At the
  defaults the ingester can hold about 1 GiB of memtable estimate plus the
  upload budget, plus whatever queries use.
- A small budget makes uploads wait, so its only visible effect is a `local`
  counter that grows faster than it would otherwise. It can lead to a `local`
  rejection, never to a `memory` one.

Which bound trips first depends on what fails:

| failure | what happens at the defaults | trips first |
|---|---|---|
| object store down or refusing writes | flushes still work, so memory stays near one memtable (`256MiB` or less, well under `1GiB`); local Parquet grows by the compressed size of the push rate | `local`, at 16 GiB |
| uploads slower than pushes (small upload budget, slow store) | uploads queue behind the budget; local Parquet grows by the difference | `local`, at 16 GiB |
| local Parquet cannot be written (I/O or permission errors) | generations stay in memory and the local counter cannot grow | `memory`, after 4 memtables (`1GiB`) |

A full disk under `-data.dir` is different: the WAL lives there too, and a
failed WAL write or fsync stops the ingester (see
[durability](#durability-and-acknowledgment)) before either bound matters.
Watch the free space of that disk on its own (see
[The disk fills up](operations.md#the-disk-fills-up)).

`parkana_ingest_unuploaded_bytes{where="memory|local"}`,
`parkana_ingest_backpressure_limit_bytes{where}` and
`parkana_ingest_backpressure_rejections_total{where}` show how close each
bound is and how often it refused a push (see
[what to alert on](operations.md#what-to-alert-on)).
`parkana_ingest_pending_generations` and the slower 204s of a slow fsync
(`parkana_ingest_wal_fsync_seconds`) show the ingester falling behind before
any bound trips. The benchmarks measure how fast one node gets data from push
to queryable (see [benchmarks](benchmarks.md)).

## Crash recovery

On start the ingester answers 503 to pushes and `/ready` until recovery ends:

1. Local Parquet files that no manifest commit covers (a flush or merge that
   was interrupted) are moved to `<data.dir>/parquet/<tenant>/.quarantine/`.
   Their rows are still in the WAL or in committed local files, so nothing is
   lost. A moved file's modification time is set to the time it was
   quarantined. New in 1.2.0: the ingester deletes the regular files in
   `.quarantine/` once they have been there longer than
   `-ingest.quarantine-retention` (168h by default), in a sweep right after
   recovery and then at most once an hour. `0` keeps them forever.
   Directories, symlinks and other entries in `.quarantine/` are never
   followed or deleted; remove them yourself. A failed sweep is logged and
   counted in `parkana_ingest_quarantine_sweep_errors_total` and never stops
   the ingester.
2. The WAL is replayed from the last committed generation, so every
   acknowledged push that was not yet in local Parquet is rebuilt. A torn
   last record from the crash is cut off.
3. Uploads recorded in the manifest resume at the step they reached. An upload
   that reached the catalog is only finished locally; one that did not is
   merged again if needed and uploaded under a new key.

Every object key is written at most once. When an upload is resumed after its
data object was stored but before the catalog row was added, or when a put
took more than 30 minutes, the object is uploaded again under a fresh key and
the old one is left for the compactor's
[orphan collection](compactor.md#orphan-collection). That keeps the querier's
cache correct and keeps an upload from being mistaken for an orphan.

On shutdown (SIGTERM or SIGINT) the ingester stops taking pushes, finishes the
ones in flight, fsyncs the WAL in `interval` mode and writes everything in
memory to local Parquet, within 30 seconds. It does not upload on shutdown;
local files upload after the next start.

## Visibility

Queries read only files the catalog lists, so pushed data is queryable once
it is uploaded and cataloged, not when the push is acknowledged. A stream that
receives less than `-ingest.upload-min` per hour waits for
`-ingest.upload-max-age`: with the defaults, up to about 10 minutes plus one
flush interval, one upload interval and the upload time. Lower
`-ingest.upload-max-age` for fresher results; the cost is more, smaller
objects until the compactor merges them (see [compactor](compactor.md)). The
development stack in `deploy/compose/` uses `30s`.

[Live tail](query/tail.md) is the exception: it streams lines from the
ingesters as soon as they are acknowledged.
