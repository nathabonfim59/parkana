# Parkana

Parkana is a log store with a Loki-compatible HTTP API. Logs are stored as
Parquet files on local disk or any S3-compatible object store and queried with
embedded DuckDB. Grafana reads Parkana through its built-in Loki datasource, so
Explore, dashboards, the logs panel, live tail and Grafana-managed alerts work
unchanged,
and a SQL datasource plugin adds ad-hoc SQL over the same data.

Current release: **1.2.0**. See the [changelog](CHANGELOG.md).

## Features

- It serves the Loki push and read API with live tail, so Alloy, Promtail and `logcli` work unchanged.
- It takes OTLP logs on `POST /v1/logs` and maps them to labels and metadata the way Loki does.
- It stores Parquet on local disk or an S3-compatible store (tested with Garage).
- Search sidecars let `|=` searches skip files that cannot match.
- Ingest and read pipelines parse, redact, drop and promote fields.
- `POST /api/v1/sql` runs read-only DuckDB SQL over one tenant's logs, with a Grafana plugin.
- Every acknowledged push is in an fsynced write-ahead log by default.
- One binary runs the ingester, querier and compactor, together or as separate processes.

## Install

Release packages for x86-64 Linux with glibc 2.34 or newer (Debian 12,
Ubuntu 22.04 and later, RHEL/Rocky/Alma 9, Fedora), from the
[1.2.0 release](https://github.com/nathabonfim59/parkana/releases/tag/v1.2.0):

```sh
sudo apt install ./parkana_1.2.0-1_amd64.deb      # Debian, Ubuntu
sudo dnf install ./parkana-1.2.0-1.x86_64.rpm     # RHEL, Rocky, Alma, Fedora
sudoedit /etc/parkana/parkana.env                 # storage, retention, ...
sudo systemctl enable --now parkana
```

The tarball, a container build and a build from source are in
[Getting started](docs/getting-started.md).

## Documentation

The documentation site is <https://nathabonfim59.github.io/parkana/>. The
same pages are in [`docs/`](docs/):

- [Getting started](docs/getting-started.md): install, first run, push a line and query it
- [Install with an agent](docs/agent-install.md): the steps a coding agent follows to install and check Parkana
- [Deploy](docs/deploy.md): the systemd package, Docker, the development stack, separate roles, sizing
- [Configuration](docs/configuration.md): every flag and environment variable, tuning, production checklist
- [Grafana](docs/grafana.md): the Loki data source, alerting, differences from Loki, the SQL plugin
- [LogQL compatibility](docs/query/logql.md): supported LogQL, what Grafana sends; the [Loki comparison](docs/development/loki-comparison.md) corpus
- [Ingest](docs/ingest.md) and [OTLP](docs/ingest.md#otlp): sending logs, the WAL and uploads, the OTLP mapping
- [Pipelines](docs/pipelines/index.md): [step reference](docs/pipelines/steps.md), [API](docs/pipelines/api.md), ingest and [read semantics](docs/pipelines/read.md)
- [SQL](docs/query/sql.md): `/api/v1/sql`, the `logs` relation, the sandbox and limits
- [Operations](docs/operations.md): health checks, metrics, logging, troubleshooting
- [Benchmarks](docs/benchmarks.md): Parkana against Loki for ingest, search latency and storage
- [Architecture](docs/architecture.md) and [PLAN.md](PLAN.md): how it works and the recorded decisions

## Limits in 1.2.0

- Single tenant: `X-Scope-OrgID` must be absent or `default`.
- No request authentication; put `/api/v1/sql` and `/api/v1/pipelines` behind
  your own access control ([Security](docs/security.md) shows how). The SQL
  memory limit bounds DuckDB's buffers, not the process.
- Data is queryable after upload, not straight from the ingester's memory
  (live tail is the exception).
- A tenant with any read pipeline loses filter and limit pushdown for its log
  queries.
- The Grafana SQL plugin is unsigned.
- Materialized views and multi-node fan-out are planned (PLAN.md §10).

## License

MIT. See [LICENSE](LICENSE).
