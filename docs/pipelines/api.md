# Pipelines HTTP API

Pipelines are administered over `/api/v1/pipelines`. The definition schema
is in [Pipelines](index.md#definition-schema) and the step types in
[Steps](steps.md). The generated [HTTP reference](../api-reference/http/pipelines.md)
lists every route and schema.

The routes are registered once on the process server when an ingester or
querier is enabled, and go through the normal tenant middleware
(`X-Scope-OrgID`; single-tenant mode uses the default tenant). Errors are
plain text in Loki style, and every request body is capped at 1 MiB.

| method and path | body | success |
|---|---|---|
| `POST /api/v1/pipelines` | definition without `version` (0/omitted) | `201` + stored pipeline |
| `GET /api/v1/pipelines` | n/a | `200` `{"pipelines":[...]}` sorted by name |
| `GET /api/v1/pipelines/{name}` | n/a | `200` + one pipeline |
| `PUT /api/v1/pipelines/{name}` | definition with `name` equal to the path and a positive `version` | `200` + stored pipeline |
| `DELETE /api/v1/pipelines/{name}` | `{"version": n}` | `204` |
| `POST /api/v1/pipelines/dry-run` | `{"pipeline": {...}, "entries": [...]}` | `200` `{"entries":[...],"dropped":n}` |

A pipeline view is the definition plus `version`, `created_at` and
`updated_at`. Status mapping: validation and invalid definition `400`; missing
pipeline `404`; stale or conflicting version `409`; body over 1 MiB or more
than 1000 dry-run entries `413`/`400`; internal failure `500` with the fixed
plain-text message `pipelineapi: internal error` (details are logged, never
returned).

## Versions, delete and recreate

`version` is optimistic concurrency per pipeline name. A create stores version
1. Every successful replace or delete increments it. A replace or delete must
send the live version; a stale writer gets `409`, and a missing name gets
`404`. Two concurrent creates for one name have exactly one winner.

Delete is a soft delete (tombstone). The version is retained and incremented,
`GET`/`LIST` omit the pipeline, and a later create with the same name starts
above the tombstone version and gets a fresh `created_at`. Versions therefore
never go backwards across delete/recreate cycles.

## Dry-run

Dry-run validates the candidate exactly like a write and runs it over sample
entries without any catalog, WAL or object write. Each entry's original
dataset comes from its own `labels`; an entry outside the selector passes
through untouched. `timestamp` is optional and echoed unchanged. The response
lists every surviving entry in input order with its resulting `line`,
`labels` and `meta`, plus the number of dropped entries. An `on_error: reject`
candidate answers `400`.

## curl examples

Create an ingest pipeline that redacts card numbers, parses JSON and promotes
the user id:

```sh
curl -sS -X POST http://localhost:3100/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "checkout-redact",
    "stage": "ingest",
    "selector": "checkout",
    "steps": [
      {"type": "redact", "pattern": "card=[0-9]{12,19}", "replace": "card=***"},
      {"type": "parse_json"},
      {"type": "promote", "from": "user_id", "to": "user_id"}
    ]
  }'
```

List, fetch and update it (use the version the API returned):

```sh
curl -sS http://localhost:3100/api/v1/pipelines
curl -sS http://localhost:3100/api/v1/pipelines/checkout-redact
curl -sS -X PUT http://localhost:3100/api/v1/pipelines/checkout-redact \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "checkout-redact",
    "stage": "ingest",
    "selector": "checkout",
    "version": 1,
    "steps": [
      {"type": "redact", "pattern": "(?i)card=[0-9]{12,19}", "replace": "card=***"}
    ]
  }'
```

Dry-run before writing:

```sh
curl -sS -X POST http://localhost:3100/api/v1/pipelines/dry-run \
  -H 'Content-Type: application/json' \
  -d '{
    "pipeline": {
      "name": "checkout-parse",
      "stage": "ingest",
      "selector": "checkout",
      "steps": [{"type": "parse_logfmt"}]
    },
    "entries": [
      {"line": "user.id=alice http-status=200", "labels": {"service_name": "checkout"}}
    ]
  }'
```

A read pipeline that rewrites the line (stored data is unchanged):

```sh
curl -sS -X POST http://localhost:3100/api/v1/pipelines \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "read-mask-email",
    "stage": "read",
    "selector": "*",
    "steps": [
      {"type": "redact", "pattern": "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "replace": "<email>"}
    ]
  }'
```

Then query as usual; a read pipeline transforms the rows the LogQL routes
return:

```sh
curl -sS -G http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name="checkout"} |= "<email>"' \
  --data-urlencode 'start=2026-01-01T00:00:00Z' \
  --data-urlencode 'end=2026-01-01T01:00:00Z'
```

The time range is `[start, end)`: an entry exactly at `end` is not returned.

Delete it afterwards, sending the live version:

```sh
curl -sS -X DELETE http://localhost:3100/api/v1/pipelines/read-mask-email \
  -H 'Content-Type: application/json' \
  -d '{"version": 1}'
```
