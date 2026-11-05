# Parkana

Parkana is a log store with a Loki-compatible HTTP API. Logs are stored as
Parquet files on local disk or any S3-compatible object store and queried with
embedded DuckDB. Grafana reads Parkana through its built-in Loki datasource, so
Explore, dashboards, the logs panel, live tail and Grafana-managed alerts work
unchanged,
and a SQL datasource plugin adds ad-hoc SQL over the same data.

Current release: **1.0.0**. See the [changelog](CHANGELOG.md).

## Features

- **Loki API.** Push (protobuf+snappy and JSON), `query`, `query_range`,
  labels, label values, series, index stats and volume, and live tail over
  WebSocket. Alloy, Promtail and `logcli` work unchanged.
- **OTLP ingest.** `POST /v1/logs` (protobuf and JSON, gzip) maps resources,
  scopes and attributes to labels and structured metadata the way Loki does.
- **Parquet on S3.** Parquet objects keyed by tenant, stream and hour, with a
  SQLite catalog of files, time ranges and label values. Any S3-compatible
  store works (AWS S3, Garage, MinIO, Ceph, R2), or a local directory.
- **Fast search.** Per-file n-gram bloom sidecars let `|=` searches skip files
  that cannot match; label summaries prune files by stream labels.
- **Pipelines.** Per-stream steps (`parse_json`, `parse_logfmt`,
  `regex_extract`, `drop_if`, `rename`, `redact`, `promote`, `set_stream`)
  with `expr` conditions, run at ingest or when reading. Managed over
  `/api/v1/pipelines`, versioned, hot-reloaded, with a dry run.
- **SQL.** `POST /api/v1/sql` runs read-only DuckDB SQL over one tenant's data
  in a time range, inside a per-request sandbox. A Grafana datasource plugin
  returns tables and time series.
- **Durable ingest.** Every acknowledged push is in an fsynced WAL by default;
  `-wal.sync=interval` trades that for Loki-like acknowledgment latency.
  Parallel flush and upload, admitted against a memory budget.
- **One binary.** Ingester, querier and compactor run together by default and
  share nothing in memory, so they can run as separate processes.

## Install

Release packages for x86-64 Linux with glibc 2.34 or newer (Debian 12,
Ubuntu 22.04 and later, RHEL/Rocky/Alma 9, Fedora):

```sh
sudo apt install ./parkana_1.0.0-1_amd64.deb      # Debian, Ubuntu
sudo dnf install ./parkana-1.0.0-1.x86_64.rpm     # RHEL, Rocky, Alma, Fedora
sudoedit /etc/parkana/parkana.env                 # storage, retention, ...
sudo systemctl enable --now parkana
```

The packages add a `parkana` system user, `/var/lib/parkana`, a hardened
systemd unit and `/etc/parkana/parkana.env`; the service is not started until
you enable it. A plain tarball with the binary is also published. Alpine is
not packaged in 1.0.0 (DuckDB has no musl build yet); use a glibc container
there.

## Quick start

Build from source (Go 1.27 and a C toolchain; DuckDB links statically):

```sh
make build            # bin/parkana
./bin/parkana -data.dir ./data -ingest.upload-max-age 30s
```

Parkana listens on `:3100`. Logs become queryable once uploaded; the short
`-ingest.upload-max-age` above makes that happen within about half a minute
(the default is 10 minutes). Push a line and read it back:

```sh
now=$(date +%s%N)
curl -s -X POST localhost:3100/loki/api/v1/push -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"service_name":"demo"},"values":[["'$now'","hello parkana"]]}]}'
sleep 45
curl -s -G localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name="demo"}'
```

In Grafana, add a **Loki** datasource with URL `http://<host>:3100`.

For S3 storage, roles, durability, memory and every other setting, see the
[configuration guide](docs/configuration.md).

### Development stack

`deploy/compose/` runs Garage (S3), Parkana, Loki, Alloy and Grafana side by
side, with Alloy sending the same logs to both stores:

```sh
cd deploy/compose
docker compose --profile phase2 up --build
```

Grafana is on `http://127.0.0.1:3000` with both datasources provisioned. The
stack uses throwaway credentials and anonymous admin; it is for development
only. Overlays add OTLP (`otlp.compose.yaml`) and the SQL plugin (`sql.yaml`).

## Documentation

| page | covers |
|---|---|
| [Configuration](docs/configuration.md) | every flag and environment variable, roles, tuning, production checklist |
| [LogQL compatibility](docs/logql.md) | supported LogQL, Grafana behavior, the Loki comparison corpus |
| [Pipelines](docs/pipelines.md) | step reference, API, ingest and read semantics, costs |
| [OTLP](docs/otlp.md) | `/v1/logs` mapping, Alloy and Collector setup |
| [SQL](docs/sql.md) | `/api/v1/sql`, the `logs` relation, sandbox and limits, Grafana plugin |
| [Benchmarks](docs/benchmarks.md) | Parkana vs Loki: ingest, search latency, storage, method and evidence |
| [PLAN.md](PLAN.md) | design and recorded decisions |

## Performance

On a Boat large VM (8 vCPU, Parkana limited to 4 CPUs and 8 GiB), measured as
described in [benchmarks](docs/benchmarks.md):

- **Search** (25 GB of logs, phase 3 run): `|=` for a rare string over the full
  range answered in 166 ms median vs 20.2 s for Loki 3.7.9; absent strings in
  under 1 ms.
- **Ingest** (4 GB across four streams, 1.0.0): 68.6 MB/s acknowledged, every
  line queryable 35.7 s after the last acknowledgment (42.5 MB/s end to end).
- **Storage** (the same 25 GB run): 5.13 GB of Parquet vs 5.75 GB in Loki;
  search sidecars add 0.07%.

## Limits in 1.0.0

- Single tenant: `X-Scope-OrgID` must be absent or `default`.
- No request authentication; put `/api/v1/sql` and `/api/v1/pipelines` behind
  your own access control. The SQL memory limit bounds DuckDB's buffers, not
  the process.
- Data is queryable after upload, not straight from the ingester's memory
  (live tail is the exception).
- A tenant with any read pipeline loses filter and limit pushdown for its log
  queries.
- The Grafana SQL plugin is unsigned.
- Materialized views, the Postgres catalog and multi-node fan-out are planned
  (PLAN.md §10).

## License

MIT. See [LICENSE](LICENSE).
