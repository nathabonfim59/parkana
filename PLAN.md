# Parkana — plan

A Go log store with a Loki-compatible API. It keeps logs as Parquet files in object storage, can transform records at ingest and query time, and keeps materialized views. It sits alongside an existing Grafana stack as one more data source.

## 1. Fit with the Grafana stack

Yes, the existing stack stays and Parkana is added next to it.

```
                    ┌──────────────── Grafana ────────────────┐
                    │  Prometheus/Mimir   Tempo   Loki?  Parkana│  ← Parkana is added as a
                    └──────▲────────────────▲──────▲──────▲────┘     "Loki" type datasource
                           │                │      │      │
apps ──► Alloy / OTel Collector ──metrics──┘      │      │
                │          └─────traces────────────┘      │
                └──logs──► (optional tee) ──► Loki        │
                └──logs──────────────────────────────► Parkana ──► S3 (Parquet)
```

- **Parkana speaks Loki's HTTP API**, so Grafana adds it with the built-in Loki datasource, with no plugin. Explore, dashboards, the logs panel, live tail, Loki alert rules, and derived fields that jump from `trace_id` to Tempo all work.
- **Collectors send logs to Parkana through the endpoints they already use**: Alloy `loki.write` / Promtail (Loki push) or OTLP. Alloy tees the same logs to Loki and Parkana by listing two `endpoint` blocks, and **Loki stays long-term as the comparison baseline** (same queries, same data: results, latency, bytes read, storage cost). Prometheus and Tempo stay as they are.
- **LogQL first; SQL soon after.** Phase 4 adds a Grafana backend plugin for SQL (Go SDK), for queries LogQL can't express (joins, ad-hoc analytics over views). The query path is built so this is an addition, not a rewrite: LogQL compiles to an internal plan, the plan compiles to DuckDB SQL, and the executor, catalog pruning and result encoding take any plan. `/api/v1/sql` then plugs a second front end into the same executor.

## 2. Architecture

- **Roles.** One Go binary with `--roles=all` by default (or `ingester,querier,...`): ingester, querier, compactor and scheduler. Single node first; roles share nothing in memory, so they can split across processes later.
- **Storage backends.** A local directory or any S3-compatible store (AWS S3, Garage, MinIO, …) behind one object-store interface, with a SQLite catalog first and Postgres later. Single-node ingest target: 31 MB/s (~2.6 TB/day).
- **Ingest order.** Pipelines (§6) → `_ts` normalization to µs (or now() if missing) → additive schema evolution under a per-stream lock → WAL + memtable.
- **WAL and memtable.** An append-only WAL per tenant with an in-memory Arrow buffer. Seal at 256 MB in memory or 128 MB of WAL (configurable).
- **Flush and upload.** Every 5 s sealed memtables become local Parquet; every 10 s a partition uploads once it reaches 128 MB or a file is 600 s old, merging up to 2 GB per object.
- **Reads.** The querier reads Parquet from the object store through a local LRU cache of files and footers; live tail reads straight from the ingesters.
- **Compactor.** Merges small files, enforces retention, updates the catalog and builds search sidecars (§4).
- **Durability.** No in-app replication: object storage durability (EBS, S3) is relied on.
- **Later.** Leader/worker fan-out of the file list across queriers, and a coordination layer only if multi-node needs it.

## 3. Data model

- **Tenant**: single tenant at first, but multi-tenant-ready. Every WAL path, object key, catalog row, cache key and query carries a tenant id, set to `default` for now. `X-Scope-OrgID` (Loki convention) is accepted and, while multi-tenancy is off, must be absent or `default`. Turning multi-tenancy on later is a config switch plus per-tenant limits, not a data migration.
- **Stream** is a named dataset, not Loki's label-set sense of the word. It's chosen by a configurable label, `service_name` by default and falling back to `job`. This keeps the number of datasets small and the files large.
- **Columns** in each Parquet file:
  - `_ts` int64 µs (sort key)
  - `line` string
  - promoted labels as real columns, low-cardinality and configured per stream (e.g. `level`, `namespace`, `pod`)
  - `labels` MAP<string,string> for the remaining stream labels
  - `meta` MAP<string,string> for Loki structured metadata / OTLP attributes
  - optional fields extracted by ingest pipelines, which become typed columns. Schema evolution is additive only.
- **Object layout**: `s3://bucket/{tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/[{pkey}={val}/]{uuidv7}.parquet` (stdlib `uuid.NewV7`, time-ordered), with a `.idx` sidecar.
- **Catalog** (`file_list`): tenant, stream, path, min_ts, max_ts, rows, bytes, the label-value set for promoted columns (for pruning), a flag for whether the sidecar exists, and `deleted`.

## 4. Search

LogQL `|= "needle"` is the expensive path. We cut down what gets read in layers:

1. Catalog pruning by time range plus stream plus the values of promoted labels.
2. Parquet row-group min/max stats and page index (built into Parquet).
3. **A per-file token bloom filter sidecar**, written by the compactor (and by ingesters on flush), which can skip whole files on `|=`.
4. Later: a per-file inverted index for chosen full-text fields, via bluge/bleve.
5. Regex and `!=` fall back to a scan.

## 5. Query engine

- **DuckDB embedded** (`go get github.com/duckdb/duckdb-go/v2`, cgo) with httpfs for S3 reads. Go has no DataFusion, and DuckDB is the best Parquet engine available from Go.
  - **Linking, default**: duckdb-go statically links prebuilt DuckDB libraries (linux amd64/arm64, darwin amd64/arm64, windows amd64). We need cgo and a C toolchain, but we don't build DuckDB ourselves. The libraries come from `go.duckdb.org`, so `GOPROXY`/`GOPRIVATE` setups must allow it. The binary still links glibc dynamically, so the build and runtime images should use the same distro release.
  - **Linking, fallback**: if the prebuilt path doesn't work for a target, we require libduckdb to be installed already (`curl https://install.duckdb.org | DUCKDB_INSTALL=shared sh`, or baked into the image) and build with `CGO_LDFLAGS="-lduckdb -L…" go build -tags=duckdb_use_lib`.
  - **httpfs is not in the prebuilt bundle**, which has parquet, json, icu and core_functions. It has to be `INSTALL`ed/`LOAD`ed at runtime, and the container image pre-installs it so we don't download it on first query. An alternative is to read S3 objects ourselves and give DuckDB local cached paths (§2 cache), which removes the httpfs dependency.
  - Versions: `v2.MAJOR_MINOR_PATCH.x` encodes the DuckDB version (e.g. `v2.20000.0` = DuckDB 2.0.0). Pin it in `go.mod`.
- **LogQL → plan → SQL**: we parse with Loki's own `logql/syntax` package if we can vendor it cheaply, otherwise with a hand-written parser. The AST is compiled into:
  - stream selector → catalog file list + `WHERE` on promoted columns / `labels[...]`
  - line filters → `contains` / `regexp_matches`, and the bloom check runs before the files are added to the scan
  - `| json`, `| logfmt`, `| regexp`, `| pattern` → DuckDB JSON functions / `regexp_extract`, or a Go post-filter for logfmt and pattern
  - label filters → `WHERE` on extracted fields
  - `count_over_time`, `rate`, `bytes_over_time`, `sum/avg/min/max/topk by` → `GROUP BY time_bucket(step, _ts), ...`
  - `quantile_over_time`, `unwrap` → DuckDB `quantile_cont`
- **MVP subset**: selectors, line filters, json/logfmt, label filters, count_over_time/rate/sum by/topk. Everything else returns `400 unsupported` with a clear message.
- **Query-time functions**: the same pipeline steps as §6, applied to result rows, so you can change how logs are parsed without reingesting.

## 6. Pipelines (pre-transform)

- **Ingest pipelines** per stream are ordered steps: `parse_json`, `parse_logfmt`, `regex_extract`, `drop_if`, `rename`, `redact`, `promote`, `set_stream`. Conditions and computed values use `expr-lang/expr`, a fast embeddable Go expression engine with no VRL dependency. WASM (wazero) can come later as an escape hatch.
- Pipelines are stored in the catalog, hot-reloaded, versioned, and get a dry-run endpoint that runs sample lines through a pipeline.
- **Real-time alerts at ingest** are out of scope. Grafana alerting on Loki rules covers it.

## 7. Materialized views

- A view has a definition (`source stream`, LogQL metric query or SQL, `step`, `group by`), a `frequency`, a `delay` (for late data) and a `retention`.
- The scheduler runs `[watermark, now - delay)` in step-aligned windows and writes the results as Parquet to an internal stream `__mv_{name}` (`_ts, group cols…, value cols…`). The watermark lives in the catalog. Backfill uses the same loop starting from an older watermark.
- **Query rewrite**: a LogQL metric query is served from a view when:
  - the selector and filters match the view's,
  - the query step is a multiple of the view step,
  - the query's `by` labels are a subset of the view's, and
  - the aggregation decomposes: count/sum/min/max directly, avg as sum+count, rate as count/step.

  Data past the watermark is computed live and unioned in. A response header `X-Parkana-View: name` makes the rewrite visible.
- Quantiles need sketches (DDSketch). Phase 5.

## 8. Loki API surface

Must have, so Grafana works:
`POST /loki/api/v1/push` (protobuf+snappy and JSON), `GET /loki/api/v1/query`, `query_range`, `labels`, `label/{name}/values`, `series`, `index/stats`, `index/volume` (logs volume histogram), `GET /loki/api/v1/tail` (WebSocket), `/ready`, `/loki/api/v1/status/buildinfo` (Grafana probes it).
Also: `POST /v1/logs` (OTLP HTTP).
Own API: `/api/v1/pipelines`, `/api/v1/views`, `/api/v1/sql` (used by the phase 4 plugin).

## 9. Go stack

- `github.com/apache/arrow-go/v18` (arrow + pqarrow writer), zstd, 1–8 MB row groups sorted by `_ts`
- `github.com/duckdb/duckdb-go/v2` for queries (prebuilt static libs; `duckdb_use_lib` with a preinstalled libduckdb as the fallback, see §5)
- `modernc.org/sqlite` (no cgo) for the catalog; `pgx` later
- `github.com/minio/minio-go/v7` as the S3 client (any S3-compatible store; GCS via interop)
- `github.com/grafana/loki/pkg/push` protobufs + `github.com/golang/snappy`
- `github.com/expr-lang/expr`; `github.com/bits-and-blooms/bloom/v3`
- `go.opentelemetry.io/collector/pdata` for OTLP decode
- Our own `/metrics` (Prometheus), so Grafana can monitor Parkana itself

```
cmd/parkana/            main, role wiring (roleFactories)
internal/config/        flags + PARKANA_* env, roles, sizes
internal/app/           Deps, Service, service group, store/catalog openers
internal/server/        shared HTTP server, tenant/metrics/recover middleware, /ready, /metrics
internal/lokiapi/       Loki route patterns, buildinfo, errors, 501 placeholders
internal/tenant/        tenant id, X-Scope-OrgID
internal/logs/          labels, entries, push requests (in-memory model)
internal/schema/        Parquet/Arrow columns
internal/objstore/      Store interface, fs + S3, key layout, conformance suite
internal/catalog/       Catalog interface, memory + sqlite/postgres, conformance suite
internal/ingest/        push + OTLP handlers, pipeline exec, memtable, flush/upload loops
internal/wal/
internal/storage/       parquet writer, local cache
internal/compactor/     merge, retention, sidecar build
internal/index/         bloom (later inverted)
internal/logql/         parse → plan → SQL
internal/query/         duckdb exec, result → Loki JSON
internal/views/         scheduler, rewrite
deploy/compose/         Garage (S3) + Grafana + Alloy + Parkana (+ Loki for side-by-side)
```

## 10. Phases

1. **Ingest to Parquet**: Loki push → WAL → memtable → local Parquet → S3 (Garage in the dev stack), with the catalog and compactor. Check with DuckDB CLI over S3.
2. **Grafana reads it**: the Loki read API with the MVP LogQL subset, plus labels/series/volume/tail, then a compose stack with Grafana and Alloy teeing to Loki and Parkana. Done when Explore shows the same results from both datasources.
3. **Fast search**: bloom sidecars, the Parquet footer cache, and benchmarks against Loki on the same data (ingest MB/s, `|=` p50/p99, bytes read).
4. **Pipelines**: ingest and query-time pipelines, OTLP ingest, and the SQL endpoint with its Grafana backend plugin.
5. **Materialized views**: scheduler, watermark/backfill, query rewrite, then sketches.
6. **Scale-out**: Postgres catalog, separate roles, querier fan-out, and the NATS/membership layer only if needed.

## 11. Risks

- **LogQL compatibility** is a long tail. We publish a supported-feature matrix and return clean errors for the rest.
- **cgo (DuckDB)** complicates cross-compiling, and a fully static binary isn't possible (glibc stays dynamic; don't use `-static`). It's acceptable here: we ship container images and build in the same distro as the runtime. Targets without prebuilt libs (e.g. FreeBSD) require a preinstalled libduckdb.
- **Small files**: WAL batching and compaction must be in place from phase 1.
- **High-cardinality labels as columns** would explode the schema, so only configured labels are promoted; everything else goes in `labels`/`meta` maps.
- **Loki API details** (stats fields, encoding flags, the `direction`/`limit` semantics) only show up against a real Grafana. Test against Grafana from phase 2.

## Decisions (2026-11-03)

1. **Loki stays long-term**, teed from Alloy, as the baseline to compare Parkana against.
2. **Single tenant first, multi-tenant-ready**: the tenant id is everywhere in the data model and code paths (§3), fixed to `default` until we turn it on.
3. **Single node first, sized for hundreds of GB/day**, with multi-node kept open: roles talk only through the catalog, the object store and narrow interfaces (no shared in-process state between ingester and querier), so `--roles=` can split them across processes and nodes later (phase 6).
4. **LogQL first, SQL in the near future**: LogQL → internal plan → SQL, so the SQL endpoint and Grafana plugin (phase 4) reuse the executor (§1, §5).
5. **Dev/test S3 server is Garage**, not MinIO: MinIO's community edition went source-only (no published images or binaries). Parkana itself stays S3-generic (minio-go client, path-style addressing), so AWS S3, MinIO or any compatible store still works.

## Decisions (2026-11-04, phase 2)

6. **Hand-written LogQL parser** (`internal/logql`), not Loki's `logql/syntax`: importing it pulls in most of the Loki module. The supported subset is published as a feature matrix; everything else returns 400 with a clear message.
7. **The query contract is `internal/plan`**: LogQL compiles to a `plan.Selection` (stream matchers, line filters, time range, order, limit, optional count/bytes buckets) that a `plan.Source` executes; the rest of the pipeline and the step evaluation run in Go. `plantest.RunSource` pins the semantics for every Source.
8. **The querier reads cached local copies of objects** with DuckDB instead of httpfs: no runtime extension install, no S3 credentials in DuckDB, and the cache is the start of the §2 Parquet cache. Object keys are immutable, so cached copies never go stale.
9. **Visibility in phase 2**: queries see data once the ingester has uploaded and cataloged it (`-ingest.upload-max-age`; the dev stack sets it to 30s). Live tail streams straight from the ingesters over an internal HTTP endpoint. Querying the ingesters' not-yet-uploaded data is left for later.
10. **Catalog label-value pruning is off in phase 2.** After schema-evolution compaction, `storage.Merge` keeps map-only values of a now-promoted label in the `labels` map while `LabelValues` only records the promoted column, so `catalog.Filter.Match` would drop files that do match (found by sol-b). The querier prunes by tenant, time and stream only. Phase 3 makes the summaries complete (include map fallbacks for promoted names) and decides how to treat already-cataloged files before re-enabling it.
11. **Duplicate rows count once, at read time.** Collectors deliver at least once: an Alloy retry after a push timeout re-sends a batch the ingester already made durable (seen in the phase 2 stack after a >10s WAL fsync). Loki hides such copies at ingest (last-entry and active-head checks); Parkana stores what it acknowledged and every `plan.Source` treats rows with the same timestamp, complete label set, structured metadata and line as one row, in `Select` (before ordering and limit) and `Aggregate` (before grouping). This is stable retry idempotency, slightly stronger than Loki's bounded rule; rows that differ in metadata stay distinct. Read-time `DISTINCT` costs a scan of line and metadata and state proportional to distinct rows; phase 3 can cut it with exact-duplicate removal in upload merges and compaction.

## Decisions (2026-11-04, phase 3)

12. **Search sidecars are byte n-gram blooms** (`internal/index`, format version 1: magic, version, gram length, a `bits-and-blooms/bloom/v3` filter, CRC32). Byte n-grams, not word tokens: `|=` is a byte-substring match, and a needle like `rror` must still find `error`. Rules: no false negatives ever; only `plan.LineContains` prunes (and literals a caller provably derives from a regexp); needles shorter than the gram length don't prune; a missing, corrupt or unknown-version sidecar means "scan the file". The writer puts the sidecar object before the catalog says `HasSidecar` (on add, or later with `catalog.SetSidecar` for backfill). Files with more distinct grams than `MaxGrams` get no sidecar. Gram length, FP rate and the cap start at 4 / 1% / 2M and are tuned by the phase 3 benchmarks; they are stored per sidecar, so tuning never invalidates old ones.
13. **Complete label summaries** (closes decision 10). A file's `LabelValues` is complete when, for every promoted name, it lists the distinct values of `coalesce(column, labels[name], "")` over all rows, map-only fallbacks and `""` included. The catalog records this per file (`LabelsComplete`, SQLite migration 5); `Filter.Match` prunes only complete files, so files cataloged before phase 3 stay unpruned until compaction rewrites them.

## Decisions (2026-11-05, phase 4)

14. **Ingest visibility is part of phase 4.** Phase 3's full run acknowledged 43 MB/s but needed another 1,349 s before every line was queryable (12.96 MB/s through visibility) because flush, sidecar build and upload run one file at a time. Target on the same 25 GB preset and machine type: exact-queryable ≤ 120 s after the last acknowledgment, ≥ 31 MB/s through visibility, acknowledgment rate within 10% of phase 3. A `-wal.sync=always|interval` flag (default `always`) lets an operator acknowledge before fsync, as Loki does; `always` keeps today's guarantee.
15. **Read pipelines apply when reading.** A pipeline attached to a stream for the read stage runs on every row the LogQL query routes return for that stream: it can add or overwrite structured metadata, rewrite the line and drop rows, but never change stream labels, so catalog pruning and the metadata routes (labels, series, stats, volume) keep reading stored labels. Stored data never changes. Line filters and line- or metadata-derived aggregates for such a stream are evaluated after the pipeline, never pushed down or pruned by sidecars. With no read pipeline configured, results are unchanged. The querier gets the hook as `query.Options.WrapSource`; ingest pipelines plug in as `ingest.Options.Transform` and run before the WAL.
16. **SQL endpoint** `POST /api/v1/sql` on the querier: read-only DuckDB SQL over the request tenant's stored data, exposed as a `logs` relation for a required time range. Read pipelines are not applied. DuckDB must be sandboxed (no files, network, settings, extensions, ATTACH/COPY/INSTALL/LOAD/SET), with row, byte, time and memory limits, and a test for every escape class.
17. **The Grafana SQL plugin** is a separate Go module (`grafana/parkana-datasource/`) so `grafana-plugin-sdk-go` stays out of the main module: a backend plugin over `/api/v1/sql`, a plain SQL editor, table and time-series frames, unsigned, loaded in the dev compose stack.
