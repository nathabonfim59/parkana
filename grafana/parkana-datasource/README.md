# Parkana SQL datasource for Grafana

A Grafana datasource that queries a [Parkana](https://github.com/nathabonfim59/parkana)
log store with SQL. Queries are executed by the plugin backend against the
Parkana HTTP API (`POST /api/v1/sql`); this frontend only provides the query
and configuration editors.

## Setup

Point the datasource at a Parkana server:

- **URL**: base URL of the Parkana HTTP API, e.g. `http://parkana:3100`
  (the compose stack endpoint). All requests are made by the plugin backend,
  never by the browser.
- **Tenant** (optional): tenant ID sent with Parkana requests
  (`X-Scope-OrgID`). Leave empty for the default tenant.

## Querying

Write DuckDB SQL against the `logs` relation (`_ts`, `line`, `labels`, `meta`
plus promoted columns). The backend expands these macros before execution:

| Macro         | Substituted with                   |
| ------------- | ---------------------------------- |
| `$__timeFrom` | Start of the time range, inclusive |
| `$__timeTo`   | End of the time range, exclusive   |
| `$__interval` | Panel grouping interval            |

Result formats:

- **Table**: every column of the SQL result.
- **Time series**: requires a `time` timestamp column plus numeric columns.

The optional **row limit** caps returned rows; when empty the server default
applies and results beyond the cap are marked truncated.

## Installing

Releases from Parkana 1.2.0 (unreleased) on carry this plugin as
`parkana-sql-datasource-<version>.zip`. Install it with
`grafana cli --pluginUrl <zip> plugins install parkana-sql-datasource`, or
unzip it into Grafana's plugins directory. The plugin is unsigned: list
`parkana-sql-datasource` in `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS`.
Requires Grafana 13.3.0 or newer.

## Building

From the repository root, `make plugin-dist VERSION=v1.2.0` builds the
release zip: the frontend in a pinned Node container (no host Node
required), the backend for linux/amd64, linux/arm64, darwin/amd64,
darwin/arm64 and windows/amd64, into
`dist/parkana-sql-datasource-1.2.0.zip`.

To build the parts separately in this directory:

```sh
scripts/build-frontend.sh           # production build into dist/
scripts/build-frontend.sh --check   # also run typecheck, unit tests and lint
make backend                        # dist/gpx_parkana_sql_datasource_<goos>_<goarch>
make check                          # backend go vet and go test
```
