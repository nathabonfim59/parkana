# HTTP API behavior

Parkana serves every route from one HTTP server on `-http.addr` (`:3100`,
Loki's port). The [HTTP reference](api-reference/http/) lists every route with
its parameters, bodies and status codes; it is generated from the OpenAPI
document [`api/parkana.yaml`](../api/parkana.yaml), which is also the file to
read on GitHub. This page covers the rules that hold across routes.

## Loki compatibility

The Loki routes use Loki's paths and answer with Loki 3.7.9's JSON shapes, so
Grafana's built-in Loki datasource, `logcli` and Loki push clients work
unchanged:

| area | routes |
|---|---|
| push | `POST /loki/api/v1/push` (protobuf with snappy, or JSON) and `POST /v1/logs` (OTLP) |
| query | `/loki/api/v1/query` and `/loki/api/v1/query_range`, GET and POST |
| metadata | `/loki/api/v1/labels`, `/loki/api/v1/label/{name}/values`, `/loki/api/v1/series`, `/loki/api/v1/index/stats`, `/loki/api/v1/index/volume`, `/loki/api/v1/index/volume_range` |
| tail | `GET /loki/api/v1/tail` (WebSocket) |
| build info | `GET /loki/api/v1/status/buildinfo` |

OTLP is served at `/v1/logs`, the OTLP/HTTP default path, not at Loki's
`/otlp/v1/logs`. Loki routes that are not in the table, such as the ruler and
the delete API, answer `404`.

`/loki/api/v1/status/buildinfo` reports `"version": "3.0.0"`, because Grafana
enables features by the Loki version it reads there; Parkana's own version is
in `parkanaVersion`.

Which LogQL Parkana accepts, what Grafana sends and where answers differ from
Loki's is in [LogQL](query/logql.md). Unsupported LogQL answers `400` with a message
that names what is missing. Timestamps are stored with microsecond precision:
nanosecond timestamps are truncated at ingest, and responses carry nanosecond
strings that end in `000`.

Parkana adds its own routes next to Loki's:

| route | role | reference |
|---|---|---|
| `POST /api/v1/sql` | querier | [SQL](query/sql.md) |
| `/api/v1/pipelines`, `/api/v1/pipelines/{name}`, `POST /api/v1/pipelines/dry-run` | ingester or querier | [Pipelines API](pipelines/api.md) |
| `GET /ready`, `GET /metrics` | every process | [Operations](operations.md) |
| `GET /internal/ingest/tail` | ingester | [Internal tail route](#internal-tail-route) |

## Roles and missing routes

`-roles` decides which handlers are real in a process. A Loki route whose role
does not run answers `501` with a body such as
`parkana: POST /loki/api/v1/push is not implemented yet`. The Parkana routes
are not registered without their role and answer `404`. A path no route serves
answers `404`, a method a path does not serve answers `405`, and every GET
route also answers HEAD.

## Tenants

Every request passes the tenant middleware first, `/ready` and `/metrics`
included. Parkana is single-tenant: a request without an `X-Scope-OrgID`
header, or with `X-Scope-OrgID: default`, runs as the tenant `default`. Any
other value answers `401`:

```console
$ curl -i -H 'X-Scope-OrgID: team-a' http://localhost:3100/ready
HTTP/1.1 401 Unauthorized
Content-Type: text/plain; charset=utf-8

tenant: multi-tenancy is disabled; only the default tenant is accepted
```

A malformed value (longer than 64 characters, or with a character outside
`A-Z`, `a-z`, `0-9`, `_` and `-`) also answers `401`, with a body that names
the problem. An empty header counts as absent. `-multi-tenant` is reserved: it
lets the middleware accept other tenant ids, but the roles do not support
them yet.

## Authentication

Parkana does not authenticate requests. Anyone who can reach the port can
push, query, run SQL and change pipelines. Put Parkana behind a reverse proxy
or a network policy that authenticates callers, and limit `/api/v1/sql`,
`/api/v1/pipelines` and `/internal/ingest/tail` to the callers that need them.
The proxy must pass WebSocket upgrades for live tail.
[Security](security.md) classifies every route and has proxy, firewall and
overlay network examples.

## Errors

Errors are one line of plain text with `Content-Type: text/plain;
charset=utf-8`, as Loki writes them, never JSON:

```console
$ curl -i 'http://localhost:3100/loki/api/v1/query_range?query=%7Bjob%3D'
HTTP/1.1 400 Bad Request
Content-Type: text/plain; charset=utf-8
X-Content-Type-Options: nosniff

logql: parse error at 1:6: expected quoted string label value, found end of query: bad query
```

The status codes mean the same on every route:

| status | meaning |
|---|---|
| `400` | the request is wrong; the body says what to fix. Nothing from a rejected push is stored. |
| `401` | the `X-Scope-OrgID` header names an invalid or disallowed tenant |
| `404` | no route, or a Parkana route whose role does not run in this process |
| `413` | a pipeline API body is over 1 MiB |
| `422` | a Loki push without streams, as Loki answers it (new in 1.2.0); nothing is stored |
| `429` | a push (Loki or OTLP) found the ingester behind: memory waiting for a local flush or local Parquet waiting for upload passed its bound (new in 1.2.0). The `Retry-After` header gives the seconds to wait; nothing is stored. See [back-pressure](ingest.md#back-pressure). |
| `499` | the client went away before the answer (`the request was cancelled by the client`), Loki's convention |
| `500` | a server fault; the server log has the detail |
| `501` | a Loki route whose role does not run in this process |
| `503` | the ingester is not accepting pushes (WAL replay, shutdown, fatal error) or the querier is shutting down; retry later |
| `504` | the request ran past its deadline; the body is Loki's `request timed out, decrease the duration of the request or add more label matchers ...` |

A handler panic answers `500` with the body `internal error`. Push size limits
answer `400`, not `413`, as Loki does.

## Limits and timeouts

| limit | default | applies to |
|---|---|---|
| `-ingest.max-request-size` | 16 MiB | the push and OTLP body, and separately its gzip and snappy expansions |
| `-ingest.max-line-size` | 256 KiB | every pushed line |
| `-querier.timeout` | 2 minutes | every query, metadata and SQL request, from the start of the handler |
| `-logql.max-entries` | 5000 | the largest `limit` a log query or a tail may ask for |
| `-logql.max-series` | 500 | the series a metric query may return |
| `-sql.timeout`, `-sql.max-rows`, `-sql.max-result-bytes`, `-sql.max-concurrent` | 30s, 10000, 8 MiB, 2 | `POST /api/v1/sql`; see [SQL](query/sql.md#limits) |
| request headers | 10 seconds | the time a client may take to send the request headers |
| pipeline API body | 1 MiB | `/api/v1/pipelines` writes and dry runs |

The server sets no other read, write or idle timeout. A push waits for its
acknowledgment as long as the client keeps the connection open. On shutdown
the server stops accepting connections and gives open requests 10 seconds to
finish.

Query and metadata responses are encoded in full before the first byte is
sent, so a failure while encoding becomes a clean `500` instead of a truncated
body; a large result is held in memory once. `POST /api/v1/sql` works the same
way, which is why `-sql.max-result-bytes` exists.

## Live tail

`GET /loki/api/v1/tail` is validated before the WebSocket upgrade: a bad
parameter, a metric query or a querier with no ingester to subscribe to
answers with a plain-text `400` or `503` and no socket is opened. After the
upgrade:

- Only entries pushed after the subscription started are sent. There is no
  backfill from storage; use `query_range` for history.
- Each text message holds at most `limit` entries (default 100) and at most
  4 MiB of JSON; larger batches are split and the connection stays open.
- The server pings every second and drops a client that does not answer
  within 5 seconds, or whose write blocks for 10 seconds.
- Lost entries are never hidden. When the querier's per-connection buffer
  (`-tail.buffer-entries`, 1024) overflows, the message carries
  `dropped_entries`. When an ingester fails or evicts the subscriber, the
  server closes with code `1011` and a reason; on shutdown it closes with
  `1001`.

The querier subscribes to every ingester in `-tail.ingester-urls`, or to its
own process over loopback when it also runs the ingester role. The full
parameter and close-code list is on the [Tail](api-reference/http/tail.md)
reference page.

## Internal tail route

`GET /internal/ingest/tail` is the ingester side of live tail. The querier
calls it on each ingester and receives newline-delimited JSON records of the
entries the ingester acknowledges from then on; the querier then applies the
LogQL query. It is not a Loki route and not a public API: its records can
change between versions. It has no authentication beyond the tenant header
and streams every acknowledged entry of the tenant, so expose it only to the
queriers. It is registered only when the ingester role runs. Its records are
described on the [Internal](api-reference/http/internal.md) reference page.
