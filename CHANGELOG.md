# Changelog

All notable changes to Parkana are recorded here. Versions follow
[Semantic Versioning](https://semver.org/).

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
  (see [docs/logql.md](docs/logql.md)).
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
