# SQL API

Parkana's querier exposes `POST /api/v1/sql`: one read-only DuckDB `SELECT`
over the tenant's stored log rows, inside a per-request sandbox. A Grafana
datasource plugin (`parkana-sql-datasource`) speaks this endpoint so panels
can be built with SQL instead of LogQL; [Grafana](../grafana.md#sql-data-source-plugin)
covers the plugin.

This page documents the endpoint contract, what the `logs` relation contains,
and the sandbox and its limits. Example
values match the shipped defaults; every limit is configurable or listed as
fixed.

## Endpoint

`POST /api/v1/sql` on any Parkana process running the `querier` role
(`-roles`). Like every data route it is tenant-scoped: the tenant comes from
the `X-Scope-OrgID` header through the same middleware as the Loki API. While
multi-tenancy is off, the header must be empty or `default`; anything else
gets `401`. Parkana itself performs no other authentication; access control
follows the deployment boundary (reverse proxy, network policy) the operator
puts in front of it.

### Request

A strict JSON object, at most 1 MiB:

```json
{
  "query": "SELECT _ts, line FROM logs WHERE _ts >= TIMESTAMP '2026-11-05 10:00:00' AND _ts < TIMESTAMP '2026-11-05 11:00:00'",
  "from": "2026-11-05T10:00:00Z",
  "to": "2026-11-05T11:00:00Z",
  "limit": 500
}
```

- `query` (string, required): the SQL text, non-empty. Additional caps on the
  text are listed under [Limits](#limits).
- `from`, `to` (string, required): RFC3339Nano timestamps, normalized to UTC.
  They bound which stored rows the query can see, half-open: `_ts >= from`
  and `_ts < to`. `from` must be before `to`; the zero timestamp is rejected.
- `limit` (integer, optional): maximum rows returned, `1` to `sql.max-rows`
  (default 10000). When omitted, `sql.default-limit` applies (default 1000).

Unknown members, JSON `null` members, duplicate keys, trailing data after the
object and bodies over 1 MiB are all `400`.

Note that `from`/`to` and a `_ts` predicate are separate things: the bounds
decide which rows are materialized at all (a query can never read rows
outside them), while a `WHERE _ts` clause (or a `$__timeFrom`/`$__timeTo`
macro in Grafana) filters inside that window. Keeping them equal is the
normal choice.

### Response

`200` with `Content-Type: application/json`:

```json
{
  "columns": [
    {"name": "_ts", "type": "time", "databaseType": "TIMESTAMPTZ"},
    {"name": "n", "type": "number", "databaseType": "BIGINT"}
  ],
  "rows": [["2026-11-05T10:00:01.123456Z", 42]],
  "truncated": false
}
```

- `columns[].type` is one of `time`, `number`, `string`, `boolean`, `json`;
  `databaseType` is the DuckDB type name.
- Instant types (`TIMESTAMP*`, `DATE`) are `time` columns and encode as UTC
  RFC3339Nano strings. `TIME`/`TIMETZ` are clock values and encode as
  `string` (`15:04:05.999999`, with offset for `TIMETZ`).
- Every numeric type is a `number` column carrying an exact JSON number
  token: integers (including `HUGEINT`) as decimal digits, `DECIMAL` as the
  driver's exact decimal text (trailing zeros dropped: `3.0` encodes as `3`),
  `FLOAT`/`DOUBLE` as shortest round-trip. Consumers that need precision must
  decode with `json.Number` semantics, not `float64`; the Grafana plugin
  does.
- SQL `NULL` is JSON `null`. `MAP`, `LIST`, `STRUCT` and `UNION` values are
  `json` columns holding nested JSON values. `INTERVAL` encodes as
  `{"months","days","micros"}`; `BLOB`/`GEOMETRY` as base64 strings; `UUID`
  as canonical text.
- `truncated` is `true` when more rows existed than the limit returned; the
  response then holds exactly `limit` rows.

A result that cannot be represented fails rather than degrading: non-finite
floats (`NaN`, `±Inf`), temporal infinity and dates outside the RFC3339
range (years 0000 to 9999) return `400`, as does an encoded response larger
than `-sql.max-result-bytes`, before any success header is written.

### Errors

Errors are Loki-style plain text bodies:

| Status | Meaning |
|---|---|
| `400` | Anything the caller can fix: malformed request, bad bounds or limit, query text over the SQL caps, not exactly one read-only `SELECT`, sandbox policy violations (relations, functions, `WITH RECURSIVE`), DuckDB parse/bind/runtime errors on the statement, materialization over `sql.memory-limit`, encoded result over `sql.max-result-bytes`. |
| `401` | `X-Scope-OrgID` named a tenant that is not allowed (single-tenant mode accepts only `default`). |
| `499` | Client canceled the request (Loki's client-closed status). |
| `500` | Internal fault (catalog, object store, sandbox setup). The body is a fixed sanitized message; details go to the server log, never the response. |
| `503` | Querier is shutting down. |
| `504` | The request deadline expired (`sql.timeout` or the querier read timeout). The wait for a concurrency slot is covered by the same deadline. |

`400` texts name the actionable class without echoing SQL text, parser
detail, paths or other tenant information; the full error is logged.

## The `logs` relation

Every request materializes the same base table:

| Column | DuckDB type | Contents |
|---|---|---|
| `_ts` | `TIMESTAMPTZ` | Entry timestamp, microsecond precision, UTC |
| `line` | `VARCHAR` | The log line |
| `labels` | `MAP(VARCHAR, VARCHAR)` | Stream labels as pushed |
| `meta` | `MAP(VARCHAR, VARCHAR)` | Entry metadata |
| *promoted labels* | `VARCHAR` | One column per label the ingester promoted |

Rows are the stored rows exactly: no LogQL normalization, no deduplication,
no read pipelines. Retry-delivered duplicates appear as duplicates;
`SELECT count(*)` counts what is stored.

Only the tenant's catalog files overlapping `[from, to)` are read, and only
their in-range rows are loaded (`union_by_name` merge, so files with
different promoted-column sets combine cleanly). Because storage is
microsecond-precision, sub-microsecond bounds round up to the µs grid so a
nanosecond boundary never lands on the wrong side of a stored row.

A label can be a promoted column in one file and a `labels` map entry in
another, and promotion removes the label from the map. In the materialized
table the promoted column is `NULL` for map-backed rows, and
`labels['level']` is `NULL` for rows where the label was promoted. A
promoted column also only exists when at least one selected file carries
it: if no file in the window promoted `level`, referencing the column is a
bind error and `labels['level']` alone is the right query. When both shapes
may be present, unify them with `coalesce`:

```sql
SELECT coalesce("level", labels['level']) AS lvl, count(*) AS n
FROM logs
GROUP BY lvl
ORDER BY n DESC
```

This assumes some file in the window promoted `level`. Promoted columns may
need double-quoting when the name is not a plain identifier (labels are
pushed as-is).

## What the sandbox allows

The endpoint runs exactly one read-only `SELECT` per request:

- The statement must lead with `SELECT` or `WITH`. Everything else (batches,
  `INSERT`/`CREATE`/`DROP`, `PRAGMA`, `SET`, `ATTACH`, `COPY`,
  `PREPARE`/`EXECUTE`, `VALUES`/parenthesized leads) is `400`. Bind
  parameters (`?`, `$1`) are rejected; there is nothing to bind.
- The only named relations are `logs` and CTEs visible at the reference's
  lexical scope. Schema- or catalog-qualified names are rejected. Ordinary
  `WITH` (multiple CTEs included) works; `WITH RECURSIVE` is rejected, so
  recursive CTEs are not available. This is a documented limitation.
- Function policy: names starting with `duckdb_`, `pragma_`, `sqlite_` or
  `read_`, plus `current_setting`, `getvariable`, `getenv`, `query`,
  `query_table`, `json_execute_serialized_sql`, `glob`, `glob_parquet`,
  `parquet_scan`, `parquet_metadata`, `parquet_schema`,
  `parquet_kv_metadata` and `sniff_csv`, are rejected wherever they appear.
  Scalar, aggregate and window functions outside these classes stay
  available, subject to what the pinned DuckDB build provides.

Enforcement is structural, not just lexical. Each request builds its own
in-memory DuckDB database, sets its budgets, materializes `logs`, releases
every object-store lease, then disables extension install/load and
persistent secrets, turns off external access and the local filesystem, and
locks the configuration, all before the user statement is validated
(lexical preflight, lead keyword, a parse-only `json_serialize_sql` check
under the request deadline, and an AST policy walk) and executed. No file
path, settings surface or second statement remains reachable.

## Limits

Configured flags on the querier role. Each flag falls back to a `PARKANA_*`
environment variable (`-sql.max-rows` ⇄ `PARKANA_SQL_MAX_ROWS`) and wins
when both are set:

| Flag | Default | Meaning |
|---|---|---|
| `-sql.max-rows` | `10000` | Most rows one response may return; upper bound of `limit` |
| `-sql.default-limit` | `1000` | Row limit when the request omits `limit` |
| `-sql.max-result-bytes` | `8MiB` | Largest encoded response body |
| `-sql.timeout` | `30s` | Per-request SQL deadline, covering the body read, queue wait, materialization, validation, execution and response encoding |
| `-sql.memory-limit` | `1GiB` | Per-request DuckDB memory budget |
| `-sql.threads` | `1` | DuckDB worker threads per request |
| `-sql.max-concurrent` | `2` | Most SQL requests executing at once; others wait up to the deadline |

Fixed caps: request body 1 MiB; SQL text 64 KiB; lexical nesting depth 1000
(parentheses, brackets and `CASE`, counted outside strings/comments, with
`max_expression_depth=1000` as a second layer inside DuckDB). Over any of
these the request is `400` before DuckDB sees the text.

### Resource behavior

- `sql.memory-limit` and `sql.max-concurrent` constrain configured DuckDB
  buffer-manager budgets, not process RSS. Scalar and intermediate
  allocations can exceed them: measured `SELECT repeat('x', 100000000)`
  returned a 100,000,000-byte string in about 0.5 s under
  `memory_limit = 16MiB`. The HTTP response-byte cap applies only after
  this allocation. Expose the endpoint only to callers trusted with
  querier memory; strict isolation through a killable subprocess with an
  RSS cap is future work, and there is no amplifying-function denylist.
- Spilling is disabled, so a window holding more than the buffer budget can
  materialize fails with a `400` naming `sql.memory-limit` and suggesting a
  narrower range, instead of writing tenant data to disk.
- With the defaults the configured concurrent budgets total 2 GiB; actual
  RSS can be higher.
- The deadline covers every phase, and cancellation interrupts the
  validation parse too. One residual: the re-parse inside statement
  execution is not actively interruptible, so its cost is bounded by the
  text caps rather than the deadline. On cap-sized adversarial inputs at a
  100 ms budget (three runs) the observed maxima were 303.9 ms total
  validation and 249.8 ms for the raw execute-path prepare; the regression
  test uses a 5 s bound. These are observations, not a worst-case
  guarantee. Hard wall-clock/RSS isolation through a killable subprocess is
  future work.

### Metrics

`parkana_sql_requests_total` (by bounded outcome), `parkana_sql_duration_seconds`,
`parkana_sql_rows_total`, `parkana_sql_result_bytes_total` and
`parkana_sql_inflight` are registered on the querier's registry when the
endpoint is built.

## Examples

Direct calls use ordinary SQL literals. Macros exist only in the Grafana
plugin ([Macros](../grafana.md#macros)); they never reach this endpoint.

```sh
# Latest lines in a window.
curl -s http://localhost:3100/api/v1/sql \
  -H 'Content-Type: application/json' \
  -H 'X-Scope-OrgID: default' \
  --data-binary @- <<'JSON'
{
  "query": "SELECT _ts, line, labels['level'] AS level FROM logs ORDER BY _ts DESC",
  "from": "2026-11-05T10:00:00Z",
  "to": "2026-11-05T11:00:00Z",
  "limit": 100
}
JSON
```

Per-level counts. This assumes some file in the window promoted `level`;
see the `logs` section for when the map alone is the right query.

```sql
SELECT coalesce("level", labels['level']) AS lvl, count(*) AS n
FROM logs
GROUP BY lvl
ORDER BY n DESC
```

Per-minute volume as a `time` column plus a numeric column, the shape the
plugin's `time_series` format wants.

```sql
SELECT time_bucket(INTERVAL '1 minute', _ts) AS time, count(*) AS n
FROM logs
GROUP BY time
ORDER BY time
```
