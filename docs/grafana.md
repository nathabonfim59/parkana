# Grafana

Grafana reads Parkana through its Loki data source, the same one it uses for
Loki. Explore, dashboards, the logs panel, live tail and Grafana-managed alert
rules work without changes. A separate data source plugin adds SQL over the
same data.

Parkana is checked against Grafana 13.3.0 with the Loki data source plugin
13.2.5 and Loki 3.7.9 as the baseline; [LogQL](query/logql.md) records what Grafana
sends and how Parkana answers it.

## Add the Loki data source

1. In Grafana, open Connections, then Data sources, then Add new data source.
2. Pick **Loki**.
3. Set the URL to Parkana's HTTP address as the Grafana server sees it, for
   example `http://parkana.example.com:3100`. Grafana's backend makes the
   requests (proxy access), so the browser does not need to reach Parkana.
4. Leave authentication off and add no custom headers.
5. Save and test. The test sends the instant query `vector(1)+vector(1)`,
   which Parkana answers even when it holds no data yet.

Parkana is single tenant. Grafana does not need to send
`X-Scope-OrgID`; if a proxy or the data source does send it, the value must be
`default`, and any other value is rejected. Parkana does not authenticate
requests either, so if you need access control put a reverse proxy in front
of it and give Grafana its credentials in the data source settings.

Grafana 13.3.0 does not bundle the Loki data source: it is an external plugin
that Grafana installs from grafana.com at startup. A Grafana without
internet access needs the plugin installed by hand (`grafana cli plugins install loki`) or
preinstalled with `GF_PLUGINS_PREINSTALL_SYNC`, as the
[development stack](deploy.md#development-stack-with-compose) does.

## Provision it

To create the data source from a file instead of the UI, put this in
Grafana's provisioning directory, for example
`/etc/grafana/provisioning/datasources/parkana.yaml`:

```yaml
apiVersion: 1

datasources:
  - name: Parkana
    uid: parkana
    type: loki
    access: proxy
    url: http://parkana.example.com:3100
    editable: false
    jsonData:
      maxLines: 1000
      timeout: 60
```

`maxLines` is the line limit Explore asks for, and `timeout` is in seconds.
Keep `timeout` above Parkana's own query deadline, `-querier.timeout` (2
minutes by default), or lower that deadline, so the error a slow query gets
comes from Parkana. A fixed `uid` lets dashboards keep referring to the data
source when it is provisioned again. The development stack provisions Parkana and Loki side by side the
same way, in `deploy/compose/grafana/provisioning/datasources/loki.yaml`.

## What works

### Explore

Log queries in Explore send `query_range` with the selector and pipeline you
type. Parkana applies the pipeline, orders and limits the result the way Loki
does, and returns the same JSON. Label and value pickers use the `labels`,
`label/{name}/values` and `series` endpoints. The volume histogram above the
logs is a second `query_range` with a `count_over_time` metric query, which
Parkana answers like any other metric query.

Data is visible once the ingester uploads it, after at most
`-ingest.upload-max-age` (10 minutes by default). Lower it if Explore must
show the last minute; the cost is more, smaller objects until the compactor
merges them. [LogQL](query/logql.md#upload-and-catalog-visibility) explains how
this looks in Grafana.

### Dashboards and the logs panel

Panels use the same data source and the same two kinds of query: log queries
for the logs panel and tables, metric queries such as
`sum by (level) (count_over_time({service_name="api"}[5m]))` for time series
and stat panels. The [feature matrix](query/logql.md#feature-matrix) lists the
range functions and aggregations Parkana evaluates.

### Live tail

The Live button in Explore opens a WebSocket to `/loki/api/v1/tail`. Parkana
streams lines as the ingester writes them, before they are uploaded, so live
tail is the one view that shows data right away. It sends only new lines and
does not backfill history. A reverse proxy between Grafana and Parkana must
pass WebSocket upgrades. When the ingester runs in another process, the
querier needs `-tail.ingester-urls`; see
[Deploy](deploy.md#split-roles-across-processes). [Live tail](query/tail.md) has the
details.

### Alerting

Grafana-managed alert rules work: Grafana runs the rule's LogQL metric query
through the data source on its own schedule, which is the same `query` and
`query_range` path a dashboard uses. Parkana has no ruler, so data
source-managed alert and recording rules (rules stored in Loki and evaluated
by it) are not available. Keep in mind the upload delay above: an alert on the
last five minutes sees only uploaded data, so set `-ingest.upload-max-age`
shorter than the alert's window.

## What differs from Loki

The LogQL [feature matrix](query/logql.md#feature-matrix) lists, row by row, what
Parkana supports, how each row was verified and what returns a 400 with an
`unsupported` message. Beyond LogQL:

- Queries read uploaded objects only, so a query can miss the last
  `-ingest.upload-max-age` of data that Loki would already return from its
  ingesters. Live tail is the exception.
- Parkana stores timestamps in microseconds. Two lines that differ only below
  the microsecond are the same instant to Parkana;
  [LogQL](query/logql.md#microsecond-storage-and-strict-comparison) has the details.
- Live tail has no historical backfill.
- There is no ruler API, as above.
- Only the `default` tenant exists.

## SQL data source plugin

The Parkana SQL data source (`parkana-sql-datasource`) is a separate Grafana
plugin for panels written in SQL instead of LogQL. Its backend sends each
query to Parkana's `POST /api/v1/sql`, which runs one read-only DuckDB
`SELECT` over the same stored logs, and it returns tables and time series.
It needs Grafana 13.3.0 or newer.

The plugin is not in the Grafana catalog, and it is unsigned, so Grafana
loads it only when its ID is allowed as an unsigned plugin. Starting with
1.2.0 each release carries it as a zip; for 1.0.0, build it from the
repository. `/api/v1/sql` has no authentication, so expose it only
to Grafana and other callers you trust.

- The [plugin page](../grafana/parkana-datasource/README.md) covers its
  settings, macros, result formats and build.
- [SQL](query/sql.md) covers the endpoint, the `logs` relation, the sandbox
  and the limits.
- The sections below cover the plugin's installation, settings, queries,
  macros, build and dependency advisories. Its development stack and end-to-end check are in
  [Development](development/index.md#sql-plugin-development-stack).

### Install

Each release from 1.2.0 on has
`parkana-sql-datasource-<version>.zip` next to the Parkana packages, listed
in the release's `SHA256SUMS`. Its top-level directory is the plugin ID and
it holds the backend for linux/amd64, linux/arm64, darwin/amd64,
darwin/arm64 and windows/amd64. Download it and check it:

```sh
base=https://github.com/nathabonfim59/parkana/releases/download/v1.2.0
curl -fLO $base/parkana-sql-datasource-1.2.0.zip
curl -fLO $base/SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
```

Then install it in one of two ways and restart Grafana:

- With the Grafana CLI, which unpacks a local zip into the plugins
  directory:

  ```sh
  grafana cli --pluginUrl ./parkana-sql-datasource-1.2.0.zip \
    plugins install parkana-sql-datasource
  ```

- By hand: unzip it into Grafana's plugins directory (`paths.plugins`,
  `/var/lib/grafana/plugins` in the official image), which creates
  `parkana-sql-datasource/` there.

Grafana loads the unsigned plugin only when its ID is listed in
`allow_loading_unsigned_plugins` under `[plugins]` in `grafana.ini`, or in
the `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS` environment variable:

```sh
GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=parkana-sql-datasource
```

`GET /api/plugins/parkana-sql-datasource/settings` on Grafana answers once it
is loaded. Add the data source in the UI (Connections, Data sources, Parkana
SQL), or provision it with a file in `provisioning/datasources/`:

```yaml
apiVersion: 1
datasources:
  - name: Parkana SQL
    uid: parkana-sql
    type: parkana-sql-datasource
    access: proxy
    url: http://parkana.example.com:3100
    jsonData:
      tenant: default
```

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

### Building

`make plugin-dist` builds the same zip as a release from a checkout:

```sh
make plugin-dist VERSION=v1.2.0
# dist/parkana-sql-datasource-1.2.0.zip, and its line in dist/SHA256SUMS
```

The frontend builds in the plugin's pinned Node container
(node:24.15.0-bookworm, digest-pinned), so no host Node is needed: Docker by
default, `CONTAINER=podman` for Podman, or `PLUGIN_FRONTEND=host` to use the
npm on the `PATH`. `PLUGIN_FRONTEND_FLAGS=--check` also runs the typecheck,
the unit tests and the linter. The backends are built for linux/amd64,
linux/arm64, darwin/amd64, darwin/arm64 and windows/amd64 with
`CGO_ENABLED=0`, `-trimpath` and stripped symbols (`PLUGIN_PLATFORMS`
narrows the list). The zip's entries are sorted and carry the commit time,
so two builds of one commit give the same file. `make plugin-check` runs the
backend's `go vet` and `go test`.

For a quick local build of one part:

```sh
# Frontend only, into grafana/parkana-datasource/dist/;
# --check also runs the typecheck, the unit tests and the linter.
grafana/parkana-datasource/scripts/build-frontend.sh --check

# One backend: dist/gpx_parkana_sql_datasource_<goos>_<goarch>
# (GOOS/GOARCH overridable).
make -C grafana/parkana-datasource backend
```

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
