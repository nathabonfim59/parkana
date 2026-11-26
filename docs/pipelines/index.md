# Pipelines

Pipelines are ordered steps that parse, rewrite, promote or drop log entries.
They are stored per tenant in the catalog, versioned, hot-reloaded by the
roles that use them, and administered over HTTP.

Two stages exist:

- Ingest pipelines (`"stage": "ingest"`) run in the ingester after a push
  is validated and before it is written to the WAL. They can change the line,
  labels and structured metadata, drop entries, or reject the whole push.
- Read pipelines (`"stage": "read"`) run in the querier on the rows the
  LogQL routes (`query`, `query_range`) return. They can change the line and
  metadata or drop rows. They cannot change stream labels or timestamps.

Stored data is never rewritten by a read pipeline. SQL (`/api/v1/sql`) and the
metadata routes return stored rows and stored labels; ingest pipelines have
already changed what was stored, read pipelines have not run.

This page covers configuration, the definition schema, which pipelines
run, reloading and metrics. [Steps](steps.md) lists the step types,
expressions, limits and the per-step error policy. [Read semantics](read.md)
explains what a read pipeline changes in a query and what it costs.
[HTTP API](api.md) covers the routes, versions, dry-run and curl examples.
[Redact and anonymize at ingestion](redaction.md) covers removing secrets
and personal data before they are stored, with a tested pattern catalog.

## Configuration

Pipelines are always enabled for the ingester and querier roles. Two flags
size the reload loop:

| flag | env | default | meaning |
|---|---|---|---|
| `-pipeline.poll-interval` | `PARKANA_PIPELINE_POLL_INTERVAL` | `1s` | how often each role checks seen tenants for a changed pipeline set |
| `-pipeline.reload-timeout` | `PARKANA_PIPELINE_RELOAD_TIMEOUT` | `5s` | upper bound on one tenant's poll reload, including catalog requests |

Both must be positive. Flags win over environment variables. A
compactor-only process has no registry and serves no pipeline API. The
administration API is registered once per process when an ingester or a
querier is enabled, never once per role.

## Definition schema

A pipeline definition is a JSON object:

```json
{
  "name": "checkout-redact",
  "stage": "ingest",
  "selector": "checkout",
  "steps": [
    {"type": "redact", "pattern": "card=[0-9]{12,19}", "replace": "card=***"},
    {"type": "parse_json"},
    {"type": "promote", "from": "user_id", "to": "user_id"}
  ]
}
```

| field | rule |
|---|---|
| `name` | 1-128 bytes of `A-Za-z0-9._-`, not `.` or `..`. Unique per tenant across both stages. Matching pipelines run in byte order of `name`. |
| `stage` | `ingest` or `read`. |
| `selector` | `"*"` or an exact, already-sanitized dataset name: lowercase `[a-z0-9_-]`, at most 128 bytes. |
| `steps` | 1-32 step objects, executed in order. |

A stored definition also reports the catalog-owned `version`, `created_at` and
`updated_at`. `POST` takes `name`, `stage`, `selector` and `steps` with
`version` omitted or zero; `PUT` also requires the live positive `version`;
`DELETE` takes `{"version": n}`. Unknown members are rejected, as are members
a step type does not use.

### Which pipelines run

The tenant's matching list is chosen once per stream or row, against the
**original** dataset, never against a dataset produced by `set_stream`.
A pipeline matches when its selector is `"*"` or equals that original
sanitized dataset. All matching pipelines of the entry's stage run in name
order. `set_stream` does not re-select pipelines, so it cannot recurse.

The dataset is chosen exactly as the ingester stores it: the value of the
first configured routing label present and non-empty (`-stream.labels`,
defaults `service_name,job`), sanitized to lowercase `[a-z0-9_-]`, 128 bytes,
or `default` when none is present.

Every step and its fields are listed in [Steps](steps.md).

## Reloading

Each ingester and querier owns its own registry; roles share no in-process
pipeline state and converge only through the catalog.

- The first operation for a tenant loads and compiles its definitions
  synchronously. If that load fails, the operation fails; configured
  transforms are never silently bypassed.
- `-pipeline.poll-interval` (default 1s) checks the catalog change token of
  every tenant the role has seen. `-pipeline.reload-timeout` (default 5s)
  bounds one tenant's poll reload, including its catalog requests. Slow
  tenants do not delay other tenants' reloads.
- A request, push or query holds one immutable snapshot for its whole
  execution, so a concurrent update cannot mix old and new versions within
  one push or query.
- A failed reload or an invalid new definition keeps the previous complete
  good snapshot; the failed change token stays pending and is retried on the
  next tick. A delete becomes visible atomically with the rest of the new
  snapshot.
- Across roles, a change becomes effective after the next poll that observes
  it plus the reload itself. With no reload of that tenant in flight, that is
  at most one poll interval plus one reload. If a prior same-tenant reload is
  still running when the change lands, that tick is skipped and the bound
  becomes the remainder of the prior reload plus one interval plus the next
  reload (conservatively two reload timeouts plus one interval). The bound
  assumes catalog calls respect context cancellation and compile checks stay
  bounded.

The change token is the maximum mutation token retained for the tenant's
pipeline rows. Tombstones are retained so versions never reset, so the token
lookup scans that tenant's pipeline rows, including deleted ones. The cost
grows with the number of retained tombstones for that tenant.

## Metrics

Each registry registers its own bounded-cardinality collectors, labelled by
role; no tenant or pipeline name is ever a label:

| metric | labels |
|---|---|
| `parkana_pipeline_reloads_total` | `role`, `result` |
| `parkana_pipeline_reload_duration_seconds` | `role`, `result` |
| `parkana_pipeline_active_definitions` | `role` |
| `parkana_pipeline_executions_total` | `role`, `stage`, `result` |
| `parkana_pipeline_execution_duration_seconds` | `role`, `stage`, `result` |

`role` is `ingester` or `querier`; `stage` is `ingest` or `read`; `result` is
`ok`, `error`, `keep`, `drop` or `stop`. Reload metrics count attempts that saw
a changed token plus token-read failures; an unchanged token is not observed.
An ingester and a querier in one process each have their own series and do not
duplicate registrations.
