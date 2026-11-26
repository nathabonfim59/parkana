# Changelog

All notable changes to Parkana are recorded here. Versions follow
[Semantic Versioning](https://semver.org/).

## 1.2.0 - 2026-11-26

### Ingest

- New in 1.2.0: a Loki push without streams (`{"streams":[]}`, `{}` or an
  empty protobuf message) answers 422 with Loki's `error at least one valid
  stream is required for ingestion`, and a push whose streams carry no entries
  answers 204 without writing a WAL record. Both used to answer 204 after
  writing an empty WAL record. Labels of entry-less streams are still
  validated (400), as in Loki.
- New in 1.2.0: quarantined local files are deleted once they are older than
  `-ingest.quarantine-retention` (`PARKANA_INGEST_QUARANTINE_RETENTION`,
  168h by default, `0` keeps them forever). A file gets a fresh modification
  time when it is quarantined, so retention counts from the quarantine. The
  sweep runs after recovery and at most once an hour, deletes only regular
  files and never follows a symlink. New metrics
  `parkana_ingest_quarantine_files`, `parkana_ingest_quarantine_bytes` and
  `parkana_ingest_quarantine_sweep_errors_total`. Before, quarantined files
  were kept until removed by hand.
- New in 1.2.0: ingest back-pressure. A Loki or OTLP push answers 429 with
  `Retry-After` (the longer of `-ingest.flush-interval` and
  `-ingest.upload-interval`, 10 seconds at the defaults) when the rows waiting
  in memory for a local flush reach 4 x `-ingest.memtable-max` (1 GiB at the
  defaults) or the local Parquet waiting for upload reaches
  `-ingest.max-unuploaded` (`PARKANA_INGEST_MAX_UNUPLOADED`; default `0`
  means 8 x `-ingest.upload-merge-max`, 16 GiB; an explicit value must be at
  least `-ingest.upload-merge-max`). Before, a failing object store or local
  disk let memory and local Parquet grow without limit while pushes kept
  answering 204. WAL replay is never refused and `/ready` is unchanged. New
  metrics: `parkana_ingest_unuploaded_bytes`,
  `parkana_ingest_backpressure_limit_bytes` and
  `parkana_ingest_backpressure_rejections_total`, all labeled `where`
  (`memory` or `local`).

### Storage

- PostgreSQL catalog: `-catalog.driver=postgres` with `-catalog.dsn`
  (`PARKANA_CATALOG_DSN`), a URL or key=value DSN, so roles on several
  machines share one catalog. Tables are created and migrated at startup; no
  extension is needed. Tested with PostgreSQL 16.
- `-catalog.dsn` defaults to `<data.dir>/catalog.db` only for the sqlite
  driver; postgres without a DSN is a configuration error.
- Configuration errors and startup output never show a password from a
  connection string.
- A SQLite or Postgres catalog migrated by a newer Parkana is refused at
  startup (`catalog.ErrSchemaTooNew`): the error names the catalog's schema
  version and the highest this binary supports, and nothing is written.
- Orphan collection is safer. With `-catalog.driver=memory` the compactor no
  longer deletes unreferenced objects, because that catalog forgets every file
  on restart; it logs `orphan collection disabled` once at startup.
- `-compactor.orphan-grace` (`PARKANA_COMPACTOR_ORPHAN_GRACE`, default `1h`)
  sets how old an unreferenced object must be before the compactor deletes it.
  `0` turns orphan collection off; a negative value or one below `1h` is a
  configuration error.
- Orphan safety stop: a sweep with 10 or more candidates that are more than
  `-compactor.orphan-max-fraction` (`PARKANA_COMPACTOR_ORPHAN_MAX_FRACTION`,
  default `0.1`, `1` turns it off) of the listed objects deletes nothing,
  logs `orphan collection skipped` at error level with the counts and sample
  keys, and increments `parkana_compactor_orphan_sweeps_skipped_total`.
- The orphan scan lists every object before it deletes any, and an object
  listing that fails or is canceled part way deletes nothing. Before, the
  orphans seen until the failure were deleted.
- The `-storage.s3.insecure` help now says what the flag does: plain HTTP
  and unsigned payloads, for local test stores only.

### Deployment

- Role locks: an ingester locks `<data.dir>/ingester.lock` and a querier
  locks `<cache dir>.lock` next to its cache directory before they open
  anything, so a second ingester on the same `-data.dir`, or a second querier
  on the same cache directory, fails at startup with an error naming the lock
  file and the holder's pid. Different roles can still share one `-data.dir`;
  the compactor takes no lock. The lock is a flock(2), released when the
  process exits (also after `kill -9`); keep these directories on a local
  disk, not NFS.

### Packages

- Upgrading the deb or rpm package keeps the service as it was: enabled or
  disabled, and a running service is restarted on the new binary. In 1.0.0
  the old package stopped and disabled it on every upgrade. An rpm upgrade
  from 1.0.0 restores the state too; a deb upgrade from 1.0.0 cannot, and
  prints a notice to run `systemctl enable --now parkana` once.
- `make packaging-test`, part of `make check`, runs the maintainer scripts in
  dpkg's and rpm's order for every install, upgrade and remove.

### Release

- The Grafana SQL data source plugin is a release file:
  `parkana-sql-datasource-<version>.zip` with the backend for linux/amd64,
  linux/arm64, darwin/amd64, darwin/arm64 and windows/amd64, listed in
  `SHA256SUMS`. `make plugin-dist` builds it; install it with
  `grafana cli --pluginUrl` or by unzipping it into the plugins directory.
- Releases are built by a GitHub Actions workflow on `v*` tags in the
  digest-pinned `golang:1.27.3-bookworm` image (`make build`,
  `make packages`, the new `make tarball`, `make plugin-dist`) and attached
  to a draft release. The same targets build a release locally. CI builds
  and tests the plugin on every change.

## 1.0.0 - 2026-11-05

First release.

### Ingest

- Loki push API: `POST /loki/api/v1/push` with protobuf+snappy and JSON bodies,
  optionally gzip-compressed; request, line and metadata limits.
- OTLP logs: `POST /v1/logs`, protobuf and JSON, optionally gzip. Resource,
  scope and log attributes map to labels and structured metadata as in Loki
  3.7.9's default OTLP configuration; checked against a real Loki through
  direct pushes and an Alloy tee.
- Per-tenant write-ahead log with group commit. `-wal.sync=always` (default)
  acknowledges after fsync; `-wal.sync=interval` acknowledges after the write
  and fsyncs on a timer.
- Memtable to local Parquet to object store, with crash-safe manifests:
  every object key is written at most once, a sidecar is stored before the
  catalog points at it, and kill tests cover every upload stage.
- Parallel flush and upload (`-ingest.flush-concurrency`,
  `-ingest.upload-concurrency`), admitted against a memory budget
  (`-ingest.upload-memory`, automatic from the container limit or
  `GOMEMLIMIT`); merges read inputs in buffered 1024-row batches.
- Exact duplicate rows are removed when files are merged.

### Storage

- Parquet objects on a local directory or any S3-compatible store, keyed
  `{tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet`.
- SQLite catalog of files, time ranges, label values and pipelines.
- Configurable promoted label columns per stream (`-ingest.promoted-labels`);
  schema evolution is additive only.
- Compactor: merges small files, applies retention, builds missing search
  sidecars.

### Query

- Loki read API: `query`, `query_range`, `labels`, `label/{name}/values`,
  `series`, `index/stats`, `index/volume` and `index/volume_range`, and live
  tail over WebSocket. On the release commit the 112-entry comparison against
  Loki 3.7.9 is fully equal.
- LogQL log and metric queries compiled to DuckDB over cached local copies of
  the catalog's objects; unsupported LogQL returns 400 with a clear message
  (see [docs/query/logql.md](docs/query/logql.md)).
- Log queries select `[start, end)` like Loki.
- Search sidecars: per-file byte n-gram bloom filters let `|=` searches skip
  files that cannot match; label summaries prune files by label.
- Duplicate rows delivered more than once count once at read time.

### Pipelines

- Ingest and read pipelines with `parse_json`, `parse_logfmt`,
  `regex_extract`, `drop_if`, `rename`, `redact`, `promote` and `set_stream`,
  conditions in `expr`. Data errors mark the line with `pipeline_error` instead
  of rejecting the push.
- `/api/v1/pipelines`: create, list, get, replace and delete with optimistic
  versions, plus `POST /api/v1/pipelines/dry-run`. Definitions are stored in
  the catalog and hot-reloaded by every role.
- Read pipelines rewrite lines and metadata in LogQL results without changing
  stored data; one LogQL query sees one pipeline version.

### SQL

- `POST /api/v1/sql`: read-only DuckDB SQL over one tenant's data in a time
  range, through a `logs` relation. Each request runs in its own in-memory
  database with file, network, extension and settings access disabled; one
  `SELECT` per request; limits on rows, bytes, time, memory and concurrency.
- Grafana SQL datasource plugin (`grafana/parkana-datasource/`): SQL editor,
  time macros, table and time-series results.

### Operations

- One binary, roles selected with `-roles`; every flag has a `PARKANA_*`
  environment variable (see [docs/configuration.md](docs/configuration.md)).
- `/ready`, `/metrics` (Prometheus, `parkana_*`), Loki build info for Grafana.
- Development stack in `deploy/compose/`: Garage, Parkana, Loki, Alloy and
  Grafana side by side, with OTLP and SQL overlays.
- Benchmarks against Loki with all evidence in `docs/benchmarks/`.
- deb and rpm packages (`make packages`, nfpm) with a `parkana` system user,
  `/etc/parkana/parkana.env` and a hardened systemd unit. The release binary
  runs on glibc 2.34 or newer: the C++ runtime is linked statically and only
  glibc and `libgcc_s` are dynamic.

### Known limits

- Single tenant; no request authentication.
- The SQL memory limit bounds DuckDB's buffers, not process memory.
- Data is queryable after upload (live tail excepted).
- A tenant with any read pipeline loses filter and limit pushdown for log
  queries.
- With all traffic in one stream, throughput from push to queryable measured
  28.4 MB/s in the 4 GB benchmark.
- The Grafana plugin is unsigned.
- No Alpine (musl) package: DuckDB publishes no musl build.
