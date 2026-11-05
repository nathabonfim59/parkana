# Configuration

Parkana is configured with command-line flags. Every flag also reads an
environment variable, so the same settings work in containers and systemd
units without a config file.

## How settings are read

- **Flags win over the environment.** A flag given on the command line is used
  as is; otherwise Parkana reads its environment variable; otherwise the
  default applies.
- **Environment names** are `PARKANA_` plus the flag name in upper case, with
  `.` and `-` turned into `_`: `-storage.s3.bucket` reads
  `PARKANA_STORAGE_S3_BUCKET`, `-ingest.upload-max-age` reads
  `PARKANA_INGEST_UPLOAD_MAX_AGE`.
- **Sizes** take binary units (`KiB`, `MiB`, `GiB`, `TiB`), decimal units
  (`KB`, `MB`, `GB`, `TB`) or plain bytes (`B` or no suffix): `256MiB`,
  `2GB`, `1048576`.
- **Durations** use Go syntax: `500ms`, `10s`, `5m`, `1h30m`.
- **Lists** are comma-separated: `-roles=ingester,querier`,
  `-stream.labels=service_name,job`.
- `parkana -help` prints every flag with its default. The tables at the end of
  this page list the same flags with their environment variables.

Invalid values stop startup with a message naming the flag.

## Roles

One binary runs every role. `-roles=all` (the default) runs the ingester,
querier and compactor in one process; the scheduler role is reserved for
materialized views and does nothing yet.

| role | does | serves |
|---|---|---|
| `ingester` | accepts pushes, writes the WAL, flushes local Parquet, uploads to the object store | `POST /loki/api/v1/push`, `POST /v1/logs` (OTLP), the live-tail stream |
| `querier` | runs LogQL and SQL over the catalog's files | the Loki read API, `POST /api/v1/sql`, live tail for clients |
| `compactor` | merges small files, applies retention, builds missing search sidecars | — |

Every process also serves `/ready`, `/metrics` and, when an ingester or
querier runs, `/api/v1/pipelines`. Roles share nothing in memory: they talk
through the catalog, the object store and HTTP, so you can run them as
separate processes against the same catalog and store. With separate
processes, point each querier at the ingesters with `-tail.ingester-urls`.

## Quick starts

### One node, local disk

```sh
parkana -data.dir /var/lib/parkana
```

Logs, the WAL, the SQLite catalog, the object cache and the Parquet objects all
live under `/var/lib/parkana` (`objects/`, `cache/`, `catalog.db`). Good for a
single machine and for trying Parkana out.

### One node, S3-compatible storage

```sh
export PARKANA_STORAGE_S3_ACCESS_KEY=...
export PARKANA_STORAGE_S3_SECRET_KEY=...
parkana \
  -data.dir /var/lib/parkana \
  -storage.backend s3 \
  -storage.s3.endpoint s3.example.com \
  -storage.s3.bucket parkana-logs \
  -storage.s3.region eu-west-1
```

Pass credentials through the environment, not flags, so they don't show up in
process listings. Any S3-compatible store works (AWS S3, Garage, MinIO, Ceph,
R2). For a local store without TLS add `-storage.s3.insecure`;
`-storage.s3.create-bucket` creates the bucket at startup if it is missing.
`-data.dir` still holds the WAL, local Parquet waiting for upload, the catalog
and the query cache, so keep it on a persistent disk.

### Connecting Grafana

Add Parkana as a **Loki** datasource with URL `http://<host>:3100`. Explore,
dashboards, the logs panel and live tail work as with Loki. For SQL, install
the Parkana SQL datasource plugin; see [SQL](sql.md).

### Sending logs

Anything that pushes to Loki works unchanged: point Alloy's `loki.write`,
Promtail or `logcli` at `http://<host>:3100/loki/api/v1/push`. OpenTelemetry
exporters send to `http://<host>:3100/v1/logs`; see [OTLP](otlp.md).

## Tuning

### Streams and promoted labels

A *stream* is the dataset a log belongs to; it decides the object-key prefix
and which pipelines apply. `-stream.labels` lists the labels that name it, and
the first one present wins (default `service_name,job`). Logs with none of
them go to the `default` stream.

`-ingest.promoted-labels` turns chosen labels into their own Parquet columns
per stream (default `*=level`: `level` everywhere). Promote labels you filter
on often and whose values are few; leave high-cardinality labels in the label
map. Promotion is additive: you can add columns later, and older files keep
working.

### Durability

`-wal.sync=always` (the default) acknowledges a push only after its WAL write
is fsynced, so an acknowledged log survives a machine crash.
`-wal.sync=interval` acknowledges after the write and fsyncs every
`-wal.sync-interval` (default 1s): faster acknowledgments, but a machine crash
can lose up to one interval of acknowledged logs (a process crash cannot).
Live tail shows lines as soon as they are written, so in interval mode a tail
viewer can see lines that such a crash then loses. Loki's own WAL behaves like
interval mode.

### Ingest speed, memory and visibility

Data becomes queryable once it is uploaded and cataloged. Uploads start when a
partition reaches `-ingest.upload-min` (128MiB) or its oldest file reaches
`-ingest.upload-max-age` (10m). Lower the age for fresher results; the cost is
more, smaller objects until the compactor merges them.

Flush and upload run in parallel (`-ingest.flush-concurrency`,
`-ingest.upload-concurrency`, both 4). Uploads are also admitted against a
memory budget, `-ingest.upload-memory`: with the default `0`, Parkana uses
half of the smallest container (cgroup) memory limit or `GOMEMLIMIT` it can
find, or 512MiB when neither is set. A single upload larger than the whole
budget waits until nothing else runs and then runs alone. Set an explicit
budget when Parkana shares a host without a container limit. The
`parkana_upload_*` metrics show active jobs, waits and merge sizes.

Run the ingester in a container with a memory limit (or set `GOMEMLIMIT`) so
the automatic budget has something to work from. 8 GiB handled a sustained
70 MB/s push rate in our benchmarks; see [benchmarks](benchmarks.md).

### Search sidecars

Each object gets a small n-gram bloom filter so `|=` searches skip files that
cannot match. The defaults (`-index.ngram=4`, `-index.fp-rate=0.01`,
`-index.max-grams=2097152`) were chosen from benchmarks; files with more
distinct grams than the cap get no sidecar and are always scanned. The
compactor builds sidecars for older files that lack one
(`-compactor.sidecar-backfill`).

### Queries

The querier keeps local copies of objects it reads, up to
`-querier.cache-size` (10GiB) under `-querier.cache-dir`, and decoded sidecars
up to `-querier.sidecar-cache-size`. Size the cache to your hot data: repeated
queries over cached files avoid the object store entirely.
`-querier.timeout` bounds one request; `-logql.max-entries` and
`-logql.max-series` cap result sizes.

### Retention and compaction

`-compactor.retention` deletes data older than the given age (0 keeps
everything). The compactor merges small files up to `-compactor.target-size`
and keeps replaced objects for `-compactor.delete-delay` so queries that
already listed them finish.

### Pipelines

Pipeline definitions live in the catalog and are managed over
`/api/v1/pipelines`; there are no pipeline flags beyond reload timing. Each
role checks for changes every `-pipeline.poll-interval` (1s). See
[pipelines](pipelines.md).

### SQL endpoint

`POST /api/v1/sql` runs read-only DuckDB SQL in a per-request sandbox. Its
limits are flags: `-sql.timeout`, `-sql.max-rows`, `-sql.default-limit`,
`-sql.max-result-bytes`, `-sql.memory-limit`, `-sql.threads` and
`-sql.max-concurrent`. `-sql.memory-limit` bounds DuckDB's buffer manager, not
the process: some queries can allocate more. Expose the endpoint only to
callers you trust with querier memory. See [SQL](sql.md).

## Production checklist

- Persistent disk for `-data.dir`; the WAL and not-yet-uploaded Parquet live
  there.
- A container memory limit or `GOMEMLIMIT` for the ingester.
- S3 credentials in `PARKANA_STORAGE_S3_ACCESS_KEY` /
  `PARKANA_STORAGE_S3_SECRET_KEY`.
- `-log.format=json` for log shipping; scrape `/metrics`.
- A retention period, unless you keep logs forever.
- Network access to `/api/v1/sql` and `/api/v1/pipelines` limited to trusted
  callers; Parkana does not authenticate requests itself.
- Parkana is single-tenant: `X-Scope-OrgID` must be absent or `default`.

## Flag reference

Generated from `parkana -help`.

### Roles, HTTP and logging

| flag | environment variable | default | description |
|---|---|---|---|
| `-http.addr` | `PARKANA_HTTP_ADDR` | `:3100` | HTTP listen address (Loki's default port) |
| `-log.format` | `PARKANA_LOG_FORMAT` | `text` | log format: text or json |
| `-log.level` | `PARKANA_LOG_LEVEL` | `info` | log level: debug, info, warn, error |
| `-multi-tenant` | `PARKANA_MULTI_TENANT` | — | accept tenants other than 'default' in X-Scope-OrgID (not supported yet) |
| `-roles` | `PARKANA_ROLES` | `all` | comma-separated roles to run: all, or any of ingester,querier,compactor,scheduler |

### Storage and catalog

| flag | environment variable | default | description |
|---|---|---|---|
| `-catalog.driver` | `PARKANA_CATALOG_DRIVER` | `sqlite` | catalog: memory or sqlite |
| `-catalog.dsn` | `PARKANA_CATALOG_DSN` | — | catalog data source (sqlite: file path; empty: <data.dir>/catalog.db) |
| `-data.dir` | `PARKANA_DATA_DIR` | `./data` | directory for the WAL and local Parquet files |
| `-storage.backend` | `PARKANA_STORAGE_BACKEND` | `fs` | object store: fs or s3 |
| `-storage.fs.dir` | `PARKANA_STORAGE_FS_DIR` | — | object store directory for -storage.backend=fs (empty: <data.dir>/objects) |
| `-storage.s3.access-key` | `PARKANA_STORAGE_S3_ACCESS_KEY` | — | S3 access key (prefer the env var) |
| `-storage.s3.bucket` | `PARKANA_STORAGE_S3_BUCKET` | — | S3 bucket |
| `-storage.s3.create-bucket` | `PARKANA_STORAGE_S3_CREATE_BUCKET` | — | create the S3 bucket at startup when it does not exist |
| `-storage.s3.endpoint` | `PARKANA_STORAGE_S3_ENDPOINT` | — | S3 endpoint host[:port] |
| `-storage.s3.insecure` | `PARKANA_STORAGE_S3_INSECURE` | — | use plain HTTP (local MinIO) |
| `-storage.s3.region` | `PARKANA_STORAGE_S3_REGION` | `us-east-1` | S3 region |
| `-storage.s3.secret-key` | `PARKANA_STORAGE_S3_SECRET_KEY` | — | S3 secret key (prefer the env var) |

### Streams

| flag | environment variable | default | description |
|---|---|---|---|
| `-stream.labels` | `PARKANA_STREAM_LABELS` | `service_name,job` | labels that name a log's stream, first present wins |

### Ingest

| flag | environment variable | default | description |
|---|---|---|---|
| `-ingest.flush-concurrency` | `PARKANA_INGEST_FLUSH_CONCURRENCY` | `4` | how many memtable parts may write local Parquet in parallel |
| `-ingest.flush-interval` | `PARKANA_INGEST_FLUSH_INTERVAL` | `5s` | how often sealed memtables are written to local Parquet |
| `-ingest.max-line-size` | `PARKANA_INGEST_MAX_LINE_SIZE` | `256KiB` | reject push lines longer than this |
| `-ingest.max-request-size` | `PARKANA_INGEST_MAX_REQUEST_SIZE` | `16MiB` | reject push requests larger than this |
| `-ingest.memtable-max` | `PARKANA_INGEST_MEMTABLE_MAX` | `256MiB` | seal the memtable at this size |
| `-ingest.promoted-labels` | `PARKANA_INGEST_PROMOTED_LABELS` | `*=level` | labels to promote to columns per stream, as stream=col1,col2; '*' is every stream without an own assignment; repeat the flag or use ';' to add streams; an empty value resets |
| `-ingest.upload-concurrency` | `PARKANA_INGEST_UPLOAD_CONCURRENCY` | `4` | how many independent upload receipts may upload in parallel |
| `-ingest.upload-interval` | `PARKANA_INGEST_UPLOAD_INTERVAL` | `10s` | how often local Parquet is considered for upload |
| `-ingest.upload-max-age` | `PARKANA_INGEST_UPLOAD_MAX_AGE` | `10m0s` | upload a local file once it is this old |
| `-ingest.upload-memory` | `PARKANA_INGEST_UPLOAD_MEMORY` | — | memory budget for the upload pipeline; 0 derives an automatic budget from the container and GOMEMLIMIT limits |
| `-ingest.upload-merge-max` | `PARKANA_INGEST_UPLOAD_MERGE_MAX` | `2GiB` | merge local files up to this size per uploaded object |
| `-ingest.upload-min` | `PARKANA_INGEST_UPLOAD_MIN` | `128MiB` | upload a partition once its local files reach this size |
| `-ingest.wal-max` | `PARKANA_INGEST_WAL_MAX` | `128MiB` | seal when the WAL reaches this size |

### WAL

| flag | environment variable | default | description |
|---|---|---|---|
| `-wal.sync` | `PARKANA_WAL_SYNC` | `always` | WAL durability mode: 'always' fsyncs every write before ack; 'interval' acks after the write and fsyncs on -wal.sync-interval |
| `-wal.sync-interval` | `PARKANA_WAL_SYNC_INTERVAL` | `1s` | how often dirty WAL data is fsynced in -wal.sync=interval mode |

### Search sidecars

| flag | environment variable | default | description |
|---|---|---|---|
| `-index.fp-rate` | `PARKANA_INDEX_FP_RATE` | `0.01` | target bloom false-positive rate per gram lookup, in (0, 1) |
| `-index.max-grams` | `PARKANA_INDEX_MAX_GRAMS` | `2097152` | distinct-gram cap per file; larger files get no sidecar |
| `-index.ngram` | `PARKANA_INDEX_NGRAM` | `4` | gram length in bytes for search sidecars (1-8) |

### Querier

| flag | environment variable | default | description |
|---|---|---|---|
| `-querier.cache-dir` | `PARKANA_QUERIER_CACHE_DIR` | — | directory for cached copies of Parquet objects (empty: <data.dir>/cache) |
| `-querier.cache-size` | `PARKANA_QUERIER_CACHE_SIZE` | `10GiB` | evict cached objects above this total size |
| `-querier.sidecar-cache-size` | `PARKANA_QUERIER_SIDECAR_CACHE_SIZE` | `64MiB` | evict cached search sidecars above this decoded size |
| `-querier.timeout` | `PARKANA_QUERIER_TIMEOUT` | `2m0s` | per-request query deadline |

### LogQL limits

| flag | environment variable | default | description |
|---|---|---|---|
| `-logql.max-entries` | `PARKANA_LOGQL_MAX_ENTRIES` | `5000` | largest limit a log query may ask for |
| `-logql.max-series` | `PARKANA_LOGQL_MAX_SERIES` | `500` | most series a metric query may return |

### Live tail

| flag | environment variable | default | description |
|---|---|---|---|
| `-tail.buffer-entries` | `PARKANA_TAIL_BUFFER_ENTRIES` | `1024` | most entries buffered for one slow live-tail subscriber before it is evicted |
| `-tail.flush-interval` | `PARKANA_TAIL_FLUSH_INTERVAL` | `100ms` | how often a querier may coalesce live-tail batches |
| `-tail.ingester-urls` | `PARKANA_TAIL_INGESTER_URLS` | — | comma-separated HTTP(S) base URLs of tail ingesters (empty: derive a loopback URL when this process runs the ingester role) |

### Pipelines

| flag | environment variable | default | description |
|---|---|---|---|
| `-pipeline.poll-interval` | `PARKANA_PIPELINE_POLL_INTERVAL` | `1s` | how often each role checks for changed pipeline definitions |
| `-pipeline.reload-timeout` | `PARKANA_PIPELINE_RELOAD_TIMEOUT` | `5s` | upper bound on one tenant's pipeline reload, including catalog requests |

### SQL endpoint

| flag | environment variable | default | description |
|---|---|---|---|
| `-sql.default-limit` | `PARKANA_SQL_DEFAULT_LIMIT` | `1000` | row limit applied when an /api/v1/sql request omits limit |
| `-sql.max-concurrent` | `PARKANA_SQL_MAX_CONCURRENT` | `2` | most /api/v1/sql requests executing at once |
| `-sql.max-result-bytes` | `PARKANA_SQL_MAX_RESULT_BYTES` | `8MiB` | largest encoded /api/v1/sql response body |
| `-sql.max-rows` | `PARKANA_SQL_MAX_ROWS` | `10000` | most rows one /api/v1/sql response may return |
| `-sql.memory-limit` | `PARKANA_SQL_MEMORY_LIMIT` | `1GiB` | DuckDB memory budget per /api/v1/sql request |
| `-sql.threads` | `PARKANA_SQL_THREADS` | `1` | DuckDB worker threads per /api/v1/sql request |
| `-sql.timeout` | `PARKANA_SQL_TIMEOUT` | `30s` | per-request /api/v1/sql deadline |

### Compactor

| flag | environment variable | default | description |
|---|---|---|---|
| `-compactor.delete-delay` | `PARKANA_COMPACTOR_DELETE_DELAY` | `10m0s` | keep replaced files this long before deleting the objects |
| `-compactor.interval` | `PARKANA_COMPACTOR_INTERVAL` | `1m0s` | how often the compactor looks for work |
| `-compactor.retention` | `PARKANA_COMPACTOR_RETENTION` | — | delete data older than this (0 = keep forever) |
| `-compactor.sidecar-backfill` | `PARKANA_COMPACTOR_SIDECAR_BACKFILL` | `true` | build search sidecars for files cataloged without one |
| `-compactor.target-size` | `PARKANA_COMPACTOR_TARGET_SIZE` | `512MiB` | merge small files up to about this size |

