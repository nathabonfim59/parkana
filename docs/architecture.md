# Architecture

Parkana is a log store with a Loki-compatible HTTP API. Collectors push logs
to it as they would to Loki, over the Loki push API or OTLP; Parkana stores
them as Parquet files in a local directory or an S3-compatible object store,
and Grafana reads them through its built-in Loki datasource. Queries are LogQL,
compiled to DuckDB SQL over the Parquet files, plus a read-only SQL endpoint
over the same data.

It is one Go binary. The parts that would be separate services elsewhere
(ingester, querier, compactor) are roles of that binary, and they share
nothing in memory: they meet only in the object store, the catalog and over
HTTP. This page shows how the roles fit together and how a log line travels
in and out. The details are in [ingest](ingest.md), [storage](storage.md),
[catalog](catalog.md), [compactor](compactor.md) and [query](query/index.md).

## Roles

[`-roles`](configuration.md#roles-http-and-logging) chooses the roles a
process runs. The default `all` runs every implemented role in one process.

| role | does | serves |
|---|---|---|
| `ingester` | accepts pushes, writes the WAL, flushes local Parquet, uploads objects and adds them to the catalog | `POST /loki/api/v1/push`, `POST /v1/logs`, `GET /internal/ingest/tail` |
| `querier` | answers LogQL and SQL over the catalog's files, live tail | the Loki read API, `GET /loki/api/v1/tail`, `POST /api/v1/sql` |
| `compactor` | merges small files, applies retention, deletes unreferenced objects, builds missing sidecars | no routes |
| `scheduler` | reserved for materialized views; not implemented | no routes |

The scheduler has no implementation: a process that is asked to run it logs
`role not implemented yet; skipping` and runs the other roles. Every process
serves `/ready`, `/metrics` and `/loki/api/v1/status/buildinfo`; processes
with an ingester or a querier also serve the pipeline API
(`/api/v1/pipelines`). A Loki route whose role does not run in the process
answers 501, for example a push sent to a querier-only process.

## What roles share

Roles talk to each other only through:

- the object store, where the ingester and the compactor write Parquet files
  and sidecars and the querier reads them;
- the catalog, where the ingester and the compactor record files, the querier
  lists them, and every role reads pipeline definitions;
- HTTP, for live tail: the querier subscribes to the ingesters'
  `/internal/ingest/tail`.

No role reads another role's memory. The querier does not see what the
ingester holds in memory or on local disk; that is why data is queryable only
after upload (see [visibility](ingest.md#visibility)). The same design lets the
roles run as separate processes, as long as every process opens the same
object store and the same catalog. A SQLite file can be shared by processes
on one machine; only the Postgres catalog, which is new in 1.2.0, can be shared
across machines, and a memory catalog lives in one process only. Two
processes with separate catalogs on one object store lose data: the
compactor deletes the objects its catalog does not list (see
[orphan collection](compactor.md#orphan-collection)). With split processes,
point each querier at every ingester with
[`-tail.ingester-urls`](configuration.md#live-tail).

```text
                     ┌───────────────── one process, -roles=all ──────────────────┐
 Alloy, Promtail,    │                                                            │
 OTel Collector ────►│ ingester ──── WAL, memtable, local Parquet in -data.dir    │
                     │    │  │                                                    │
                     │    │  └── /internal/ingest/tail ───► querier ◄────────────┼──── Grafana,
                     │    │                                  │  ▲                 │     LogQL, SQL
                     │    │                                  │  │                 │
                     │    │               compactor          │  │                 │
                     └────┼──────────────────┼───────────────┼──┼─────────────────┘
                          ▼                  ▼               ▼  │
                ┌──────────────────────────────────┐  ┌─────────┴───────┐
                │ object store: Parquet + sidecars │  │ catalog: files, │
                │ (local dir or S3)                │  │ pipelines       │
                └──────────────────────────────────┘  └─────────────────┘
```

The ingester and the compactor write both the object store and the catalog;
the querier reads both.

## Ingest path

```text
 POST /loki/api/v1/push, POST /v1/logs
   │  decode (JSON, protobuf+snappy, OTLP), gzip, size and line limits
   ▼
 ingest pipelines (per stream, from the catalog)
   │
   ▼
 WAL, per tenant ──► 204 (after fsync with -wal.sync=always)
   │                 └──► live tail subscribers
   ▼
 memtable generation, sealed by size or every -ingest.flush-interval
   │
   ▼
 local Parquet, one file per stream and UTC hour, committed in a manifest
   │  -ingest.upload-min reached or -ingest.upload-max-age passed
   ▼
 merge ──► object store: data object, then search sidecar
   │
   ▼
 catalog row ──► queryable
```

The push is decoded and checked, then the ingest pipelines may rewrite or drop
entries and choose the stream. The entry goes to the tenant's WAL and memtable;
the 204 means the push survives a crash as [`-wal.sync`](configuration.md#wal)
promises. Generations become local Parquet files, and the uploader merges a
stream's files per hour into one object, puts its sidecar next to it and adds
it to the catalog. Every step is recorded in a manifest so a crash at any
point resumes without loss or duplicate keys. See [ingest](ingest.md).

## Query path

```text
 LogQL (query, query_range)
   │  parse in Go (internal/logql)
   ▼
 plan.Selection: tenant, stream matchers, line filters, [from, through), limit
   │
   ▼
 catalog: live files in the time range and stream
   │  label summaries, search sidecars (|= only)
   ▼
 object cache (S3) or files in place (fs)
   │
   ▼
 DuckDB over Parquet ──► read pipelines (if any) ──► LogQL stages in Go ──► JSON
```

LogQL compiles to an internal plan, and the plan to DuckDB SQL: the parts
storage can answer (stream matchers, line filters, time range, order, limit,
count and byte buckets) run in DuckDB, the rest (parsers, label filters,
formatting, most metric functions) in Go. The SQL endpoint uses the same
storage path with its own sandboxed DuckDB per request. See [query](query/index.md)
and [SQL](query/sql.md).

## Compactor

The compactor merges the small files of each stream and UTC hour into files
of up to [`-compactor.target-size`](configuration.md#compactor), swaps them in
the catalog atomically, keeps the replaced objects for
`-compactor.delete-delay` so running queries finish, applies retention,
deletes objects the catalog does not reference and builds missing search
sidecars. Run it in exactly one process per catalog. See
[compactor](compactor.md).

## Live tail

Live tail does not go through storage. The querier holds the client's
WebSocket (`GET /loki/api/v1/tail`) and opens one NDJSON stream per ingester
on `GET /internal/ingest/tail`; each ingester sends the lines it acknowledges,
and the querier filters them with the LogQL query and forwards them. A tail
therefore shows lines seconds after the push, while queries see them only
after upload. See [live tail](query/tail.md).

## Tenancy

Parkana is single tenant. `X-Scope-OrgID` must be absent or `default`;
any other value answers 401, and `-multi-tenant` stops startup with
`-multi-tenant is not supported yet`. The tenant is still part of every path
and key: WAL directories (`<data.dir>/wal/default/`), local Parquet
(`<data.dir>/parquet/default/`), object keys (`default/logs/...`), catalog
rows and cache keys all carry it, so turning multi-tenancy on later needs no
data migration.

## Durability model

Parkana does not replicate data. An acknowledged push is in the WAL on the
ingester's disk (fsynced with `-wal.sync=always`) until it is uploaded; after
that, the object store's durability applies. Everything Parkana reports as
durable on local disk is fsynced together with the directories that make it
reachable. Keep `-data.dir` on a persistent disk.
