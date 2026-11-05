# SQL API and Grafana datasource

Parkana's querier exposes `POST /api/v1/sql`: one read-only DuckDB `SELECT`
over the tenant's stored log rows, inside a per-request sandbox. A Grafana
datasource plugin (`parkana-sql-datasource`) speaks this endpoint so panels
can be built with SQL instead of LogQL.

This page documents the endpoint contract, what the `logs` relation contains,
the sandbox and its limits, the plugin, and the development stack. Example
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
range (years 0000–9999) return `400`, as does an encoded response larger
than `sql.max-result-bytes`, before any success header is written.

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
SELECT coalesce("level", labels['level']) AS level, count(*) AS n
FROM logs
GROUP BY level
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
plugin (below); they never reach this endpoint.

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
SELECT coalesce("level", labels['level']) AS level, count(*) AS n
FROM logs
GROUP BY level
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

## Grafana plugin

`grafana/parkana-datasource` is a backend datasource plugin (separate Go
module, plugin ID `parkana-sql-datasource`, Grafana `>=13.3.0`) that renders
the endpoint into data frames.

### Settings

- `url`: base URL of the Parkana HTTP API (e.g. `http://parkana:3100`); the
  plugin appends `/api/v1/sql`. Required, http/https only.
- `tenant` (optional): sent as `X-Scope-OrgID`; empty uses the server
  default.

Both live in the datasource configuration, so queries can never supply a
URL or tenant. The health check is a bounded `SELECT 1` through the
endpoint.

### Queries

The editor model is `{refId, rawSQL, format, limit?}`. `format` is `table`
(default) or `time_series`; unset, the backend infers time series when the
result is exactly a `time` column plus numeric value columns.

- `table`: every column, in order (duplicate names preserved).
- `time_series`: requires a column named `time` of type `time`, at least
  one numeric value column, unique timestamps and unique value-column
  names. The frame is sorted by time ascending with the time field first
  (`timeseries-wide`). A non-conforming result is a clear error; aggregate
  by time (e.g. `time_bucket`) or use `table`.

Numbers keep exact tokens end to end. Integers within ±(2^53−1) become
numeric fields; `DECIMAL` and larger integers become string fields rather
than silently rounding. Cast explicitly when an approximate chartable value
is wanted: `CAST(labels['code'] AS DOUBLE) AS value`. A truncated result
carries a warning notice.

### Macros

Expanded by the plugin backend before the SQL reaches Parkana, only outside
strings, quoted identifiers, dollar-quoted strings and comments; unknown
`$__` names are left alone. The direct endpoint never sees macros; it
receives the resulting literals.

| Macro | Substitutes |
|---|---|
| `$__timeFrom`, `$__timeFrom()` | Start of the panel range (inclusive), `TIMESTAMP '...'` UTC, µs-ceiled like the endpoint bounds |
| `$__timeTo`, `$__timeTo()` | End of the panel range (exclusive), same form |
| `$__interval`, `$__interval()` | Panel grouping interval, `INTERVAL '...' milliseconds` (minimum 1 ms) |

Typical use: `WHERE _ts >= $__timeFrom AND _ts < $__timeTo`, and
`time_bucket($__interval, _ts) AS time`.

### Building and installing

```sh
# Frontend: pinned Node container (node:24.15.0-bookworm, digest-pinned);
# --check also runs typecheck and unit tests. No host Node needed.
grafana/parkana-datasource/scripts/build-frontend.sh --check

# Backend: writes dist/gpx_parkana_sql_datasource_<goos>_<goarch>
# (GOOS/GOARCH overridable); needs Go on the host or a container.
make -C grafana/parkana-datasource backend
```

Install the `dist/` tree as a Grafana plugin directory. The plugin is
unsigned, so Grafana loads it only when its ID is listed in
`GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS`, appropriate for local and
development deployments.

### Dependency advisories

The frontend installs from the lockfile with `npm ci` in the pinned Node
image above (npm 11.12.1) and is audited with `npm audit --omit=dev`. For the
1.0.0 build that production-tree audit reports nine moderate entries and
none high or critical. All nine concern packages whose code is not in the
plugin bundle:

| Reported package | Installed | Flagged through | In `dist/`? |
|---|---|---|---|
| `@grafana/data` | 13.3.0 | `@grafana/i18n` | No: Grafana external |
| `@grafana/i18n` | 13.3.0 | `@typescript-eslint/utils` | No: declared, never imported |
| `@grafana/runtime` | 13.3.0 | `@grafana/data`, `@grafana/ui` | No: Grafana external |
| `@grafana/ui` | 13.3.0 | `@grafana/data`, `@grafana/i18n`, `react-router-dom-v5-compat` | No: Grafana external |
| `@typescript-eslint/typescript-estree` | 8.72.1 | `minimatch` | No: dependency of `@grafana/i18n` |
| `@typescript-eslint/utils` | 8.72.1 | `@typescript-eslint/typescript-estree` | No: dependency of `@grafana/i18n` |
| `minimatch` | 10.2.6 | GHSA-g3hv-75wg-m249 (denial of service via globparts) | No |
| `react-router` (nested under `react-router-dom-v5-compat`) | 6.30.6 | GHSA-wrjc-x8rr-h8h6, GHSA-337j-9hxr-rhxg | No: dependency of `@grafana/ui` |
| `react-router-dom-v5-compat` | 6.30.6 | `react-router` | No: dependency of `@grafana/ui` |

Three upstream advisories sit behind those rows: the `minimatch` denial of
service and two React Router issues (open redirect through `Link` and
`useNavigate`, constructor injection during SSR hydration). They reach the
production tree through the Grafana packages the plugin depends on, not
through plugin code.

The shipped JavaScript confirms this. `dist/module.js` is an AMD module that
declares `@emotion/css`, `@grafana/data`, `@grafana/runtime`, `@grafana/ui`,
`module` and `react` as its only external dependencies, per
`.config/bundler/externals.ts`. Its source map contains the plugin's own
TypeScript modules, the webpack runtime, those externals and a generated
`node_modules/grafana-public-path.js` shim (a seven-line snippet emitted by
`VirtualModulesPlugin` that set the bundle's public path); no module of a
flagged package is compiled into the bundle. Grafana supplies the external
packages at run time, so that code is Grafana's copy, not one vendored into
`dist/`.

The full `npm ci` install also audits development and build-only tooling.
It reports 37 vulnerability entries: the nine production-tree moderates
above plus 28 that exist only in the build tool chain (5 high, 19 moderate,
4 low). The high and moderate build-only entries are:

| Build-only packages | Severity | Advisory |
|---|---|---|
| `@grafana/sign-plugin` 3.3.3, `proxy-agent` 8.0.1, `pac-proxy-agent` 9.0.1, `get-uri` 8.0.0, `basic-ftp` 5.3.1 | high | GHSA-c475-qrg2-pj4r (`basic-ftp`, quadratic-time directory listing) through the signing tool's proxy chain |
| `@grafana/eslint-config` 10.0.0, `@typescript-eslint/eslint-plugin` 8.72.1, `@typescript-eslint/parser` 8.72.1, `@typescript-eslint/type-utils` 8.72.1 | moderate | through `@typescript-eslint/typescript-estree` and its `minimatch` |
| `@jest/core`, `@jest/expect`, `@jest/globals`, `@jest/reporters`, `@jest/transform`, `babel-jest`, `jest-circus`, `jest-config`, `jest-resolve-dependencies`, `jest-runner`, `jest-runtime`, `jest-snapshot` (all 30.5.2), `babel-plugin-istanbul` 8.0.0, `test-exclude` 7.0.2, `glob` 13.0.6 and 10.5.0 | moderate | GHSA-3g3m-2fjr-6pw4 (`glob` abort-signal bypass), GHSA-g3hv-75wg-m249 (`minimatch` 9.0.9 through `glob`/`test-exclude`) |

Those packages run on the build machine: `webpack` and the lint, test and
plugin-signing tools. `dist/` is produced only from `src/`, the plugin
assets and the webpack runtime, so none of them is deployed. The four low
advisories (`deepmerge` and the `fork-ts-checker-webpack-plugin` that depends
on it, plus `jest` and `jest-cli`) are build-only for the same reason.

No dependency upgrade is made for this release: nothing flagged is in the
plugin bundle, and the flagged production packages are pinned Grafana
dependencies that execute inside the Grafana installation. Keep the Grafana
installation and the pinned build image current; this section classifies
what ships, and makes no claim about the upstream projects.

### Development stack

`deploy/compose/sql.yaml` is an overlay on the phase 2 compose stack: it
mounts the built `dist/` read-only, enables the unsigned plugin, and sets
`GF_PATHS_PROVISIONING=/etc/grafana/provisioning-sql` with its own
provisioning tree (`deploy/compose/grafana-sql/provisioning`) mounted
read-only at that path. The tree holds a byte-identical copy of the base
`loki.yaml` (the Loki and Parkana datasources) plus the SQL datasource file
(uid `parkana-sql`, type `parkana-sql-datasource`,
`url: http://parkana:3100`, `jsonData.tenant: default`). The base
provisioning mount stays untouched and is simply not the directory Grafana
reads with the overlay applied, so a plain phase 2 stack still works as
before:

```sh
cd deploy/compose
docker compose -f compose.yaml -f sql.yaml --profile phase2 \
  up -d garage garage-init parkana grafana
```

The dev stack's anonymous-admin Grafana and unsigned-plugin allowance are
properties of that local setup, not of the endpoint's security model.

## End-to-end check

`scripts/e2e-phase4-sql.sh` builds the plugin, starts an isolated compose
project (base stack plus the SQL overlay), pushes a deterministic fixture
and asserts through Grafana `/api/ds/query`: health, exact table values,
exact time-series timestamps/points, macro expansion, rejection of a
filesystem escape (`read_csv_auto('/etc/passwd')`) and the tenant boundary.
It owns its compose project's cleanup.

```sh
scripts/e2e-phase4-sql.sh              # full run (needs Docker)
scripts/e2e-phase4-sql.sh --self-test  # offline checks only, no Docker
scripts/e2e-phase4-sql.sh --dry-run    # print the resolved plan
scripts/e2e-phase4-sql.sh --keep       # leave the stack up for debugging
```

Ports, project name and the readiness budget are overridable
(`--parkana-port`, `--garage-port`, `--loki-port`, `--grafana-port`,
`--project`, `--timeout`); see `--help`.
