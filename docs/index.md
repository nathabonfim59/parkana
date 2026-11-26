---
layout: home
hero:
  name: Parkana
  text: Logs in Parquet, read through the Loki API
  tagline: A log store with a Loki-compatible HTTP API. Logs are stored as Parquet files on local disk or any S3-compatible object store and queried with embedded DuckDB. Grafana reads Parkana through its built-in Loki datasource.
  actions:
    - theme: brand
      text: Getting started
      link: /getting-started
    - theme: alt
      text: HTTP API
      link: /api-reference/http/
features:
  - title: Loki API
    details: Push (protobuf with snappy, and JSON), query, query_range, labels, label values, series, index stats and volume, and live tail over WebSocket. Alloy, Promtail and logcli work unchanged.
  - title: OTLP ingest
    details: POST /v1/logs takes protobuf and JSON, gzip or not, and maps resources, scopes and attributes to labels and structured metadata the way Loki does.
  - title: Parquet on S3
    details: Parquet objects keyed by tenant, stream and hour, on any S3-compatible store or a local directory, with a SQLite catalog of files, time ranges and label values.
  - title: Fast search
    details: Per-file n-gram bloom sidecars let |= searches skip files that cannot match. On 25 GB of logs a rare string came back in 166 ms median, against 20.2 s for Loki 3.7.9.
  - title: SQL and pipelines
    details: POST /api/v1/sql runs read-only DuckDB SQL over one tenant in a sandbox. Per-stream pipelines parse, redact, drop and promote fields at ingest or at read time.
  - title: One binary
    details: Ingester, querier and compactor run together by default and share nothing in memory, so each can run as its own process. Every acknowledged push is in an fsynced WAL.
---
