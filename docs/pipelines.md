# Pipelines

Pipelines are ordered steps that parse, rewrite, promote or drop log entries.
They are stored per tenant in the catalog, versioned, hot-reloaded by the
roles that use them, and administered over HTTP.

Two stages exist:

- **Ingest pipelines** (`"stage": "ingest"`) run in the ingester after a push
  is validated and before it is written to the WAL. They can change the line,
  labels and structured metadata, drop entries, or reject the whole push.
- **Read pipelines** (`"stage": "read"`) run in the querier on the rows the
  LogQL routes (`query`, `query_range`) return. They can change the line and
  metadata or drop rows. They cannot change stream labels or timestamps.

Stored data is never rewritten by a read pipeline. SQL (`/api/v1/sql`) and the
metadata routes return stored rows and stored labels; ingest pipelines have
already changed what was stored, read pipelines have not run.

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

### Steps

Every step accepts `type`, and optionally `when` (a boolean expression; false
skips the step) and `on_error` (`keep`, `drop` or `reject`; default `keep`).
A member a step type does not use is a validation error.

| `type` | fields | behavior |
|---|---|---|
| `parse_json` | `from` (optional) | Parses a JSON **object** from the line (default) or from a metadata field. Extracted fields are merged into metadata, overwriting existing values with the same name. Invalid JSON, a non-object document, duplicate member names or trailing data is a data error. |
| `parse_logfmt` | `from` (optional) | Parses whitespace-separated `key=value` pairs from the line (default) or a metadata field. Values may be unquoted or double-quoted. The quoted form recognizes logfmt escapes: `\"`, `\n`, `\t` and `\r` use one backslash; `\\` (two backslashes) is a literal backslash. A bare word, empty key, unterminated quote or unknown escape is a data error. Later duplicate keys win. |
| `regex_extract` | `pattern` (required), `from` (optional) | Applies a Go RE2 pattern to the line (default) or a metadata field and merges the **first** match's named captures into metadata. At least one named group is required. No match is a data error; captures that did not participate are skipped. |
| `rename` | `from`, `to` (required) | Moves one metadata field to another metadata name. A missing source is a no-op. Read stages may rename metadata whose name coincides with a routing label: stream labels themselves are untouched. |
| `redact` | `pattern` (required), `replace` (optional), `target` (optional) | Replaces every match with a Go `regexp` replacement template. Default replacement is `***`; `"replace": ""` deletes matches. `target` is the line (default) or a metadata field; a missing target or no match is a no-op. |
| `drop_if` | `expr` (required) | Drops the entry when the boolean expression is true. |
| `promote` | exactly one of `from` (metadata field) or `value` (string expression), plus `to` (label name) | Copies a string into a stream label. A missing `from` field is a no-op; an empty value writes an empty label. **Ingest only.** |
| `set_stream` | exactly one of `from` (metadata field) or `value` (string expression) | Sanitizes the value and always writes it to the first configured routing label. An empty result routes to `default`, so a later routing label cannot take over by accident. **Ingest only.** |

`promote` and `set_stream` are rejected at the read stage at validation time.
`promote` and `rename` destinations must be legal metadata/label names
(`[A-Za-z_][A-Za-z0-9_]*`) and must not start with `__`, which is reserved.
A `promote` target must also not collide with a reserved column: `_ts`,
`line`, `labels` or `meta` (the base names compare case-insensitively), or any
name starting with `_`; and it must not differ only in case from an earlier
`promote` target of the same definition. At execution time, a `promote` target
that differs only in case from a label already present on the row is a data
error, so `on_error` applies (default `keep` marks the entry and stops its
steps). A promoted label may target a routing label.

#### Extracted value representations

`parse_json` renders values as deterministic strings:

- Strings are unescaped and stay strings.
- Numbers, booleans and `null` keep their exact JSON spelling (`1e3` stays
  `1e3`, `9007199254740993` stays exact, `null` becomes the literal `null`).
- Arrays become compact JSON with no insignificant whitespace; numbers inside
  them keep their raw spelling.
- Nested objects are flattened into separate metadata leaves. Path segments
  are joined with `_`: `{"user":{"id":7}}` produces `user_id=7`.

#### Extracted names

Extracted names (JSON member names, logfmt keys, regex capture names) are
sanitized for use as metadata keys, like Loki 3.7.9:

1. Leading and trailing whitespace is trimmed.
2. A leading ASCII digit gets a `_` prefix.
3. Every rune outside `A-Za-z0-9_` becomes `_`. `user.id` becomes `user_id`,
   `http-status` becomes `http_status`, `@timestamp` becomes `_timestamp`.
4. An automatic name that would still start with `__` gets the Parkana
   `extracted` prefix, so `__error__` becomes `extracted__error__`. This
   protection is Parkana-specific (Loki sanitizes the name but does not add
   it): pipeline steps never write `__`-prefixed metadata, which keeps LogQL
   metric queries valid. Incoming metadata is preserved; a step only changes
   names it writes itself.

Name sanitizing and nested flattening follow Loki 3.7.9
([`pkg/logql/log/util.go#L22-L38`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/util.go#L22-L38),
[`#L42-L66`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/util.go#L42-L66),
[`pkg/logql/log/parser.go#L103-L116`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/parser.go#L103-L116),
[`#L213-L228`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/parser.go#L213-L228)).
Value rendering, duplicate handling and the `extracted` prefix are Parkana's
own: arrays and `null` keep the representation described above rather than
Loki's label-value conversion.

Flattened path segments only get the digit prefix when the path is empty:
`{"1a":1}` becomes `_1a`, while `{"user":{"2b":1}}` becomes `user_2b`.
Blank (after trimming) segments are skipped while the parent prefix stays
usable: `{"":{"a":1}}` produces `a`, and `{"user":{"":{"id":1}}}` produces
`user_id`. Empty or whitespace-only names that still cannot be named are data
errors. Raw duplicate JSON member names are data errors. When distinct raw
names sanitize or flatten to the same metadata name, the later occurrence wins
deterministically (JSON source order, later logfmt pair, later regex capture);
never map iteration order.

#### Expressions

`when`, `drop_if`'s `expr` and `promote`/`set_stream`'s `value` use a bounded,
method-free expression subset. The environment has:

- `line`: the current line (string).
- `labels.<name>`, `meta.<name>`: string values; an absent name reads `""`.
  Only literal member names are allowed, e.g. `meta.user_id`; a bare `labels`
  or `meta` is rejected.
- `stream`: the entry's original sanitized dataset (string).

Allowed:

- Comparisons `==`, `!=`, `<`, `<=`, `>`, `>=`; logical `&&`, `||`, `and`,
  `or`; ternary `cond ? a : b`.
- Arithmetic `+`, `-`, `*`, `/`, `%`; string `+` concatenation.
- String operators `contains`, `startsWith`, `endsWith`,
  `matches "literal-regexp"`, and `in [literal, list]`.
- Builtins `len`, `lower`, `upper`, `trim`, `trimPrefix`, `trimSuffix`,
  `indexOf`, `int`, `float`, `string`.

Not available: methods, optional member access, `if`/`else`, `let`,
`replace`, ranges, dynamic regexps, time, randomness, collections other than
the literal membership list of `in`, or any I/O.
Conditions and values are type-checked at definition compile time; an
expression of the wrong type fails validation.

Hard limits (one definition or one evaluation):

| limit | value |
|---|---|
| steps per pipeline | 32 |
| expression source | 4096 bytes |
| regex pattern | 1024 bytes |
| `redact` replacement | 256 bytes |
| expression AST nodes | 1000 |
| expression VM allocation budget | 100,000 units |
| `redact` match-index budget | 1,048,576 ints |
| `pipeline_error_details` | 256 bytes, one line |
| line before/after a step | the ingester's `-ingest.max-line-size` (256 KiB by default) |
| total metadata per entry | 256 KiB |
| one metadata value | 16 KiB |
| one label value | 2048 bytes |
| JSON nesting depth | 10,000 levels |
| one flattened JSON key name | the metadata budget (256 KiB by default) |
| cumulative flattened-key work | 4 × the metadata budget (1 MiB by default) |

String concatenation is additionally checked statically so it cannot build a
line larger than the configured line bound. Exceeding any limit above is a
data error, so the failing step's `on_error` policy applies and the step rolls
back atomically. These bounds keep a `keep` policy from producing an entry the
push handler's output validation would reject.

### Errors and per-step policy

Compile errors, unknown fields, invalid names/regexes/expressions and
read-stage `promote`/`set_stream` fail the write (HTTP 400) and dry-run; an
invalid pipeline is never stored.

Data-dependent failures, such as malformed JSON/logfmt, a regex that does not
match, an expression runtime error, or an exceeded bound, follow the failing
step's `on_error`:

- `keep` (default): the entry is restored to its state before the failing
  step, `pipeline_error` metadata is set to the step type and
  `pipeline_error_details` to a one-line, truncated message, and the entry is
  kept. Its remaining steps are skipped, and so are any remaining matched
  pipelines for that entry. Ingest answers 204 and stores the entry.
- `drop`: the entry is removed (an all-dropped push answers 204 and appends
  nothing).
- `reject`: the whole ingest push fails before the WAL with HTTP 400; on read,
  the query fails with the reject error.

The marker names are deliberately not `__`-prefixed, so ordinary LogQL metric
queries over the stream remain valid. Filter clean entries with
`| pipeline_error=""` or errored entries with `| pipeline_error!=""`.
Existing user metadata is only changed when a step writes it.

## Read semantics

Read pipelines run on rows from `query` and `query_range`; they never run for
the metadata routes (`labels`, `label/<name>/values`, `series`, index stats,
index volume) and never in SQL. Stream labels and timestamps are immutable.

When a tenant has **no** read pipelines at all, the querier forwards the
original call byte-for-byte, so behavior is unchanged from a build without
pipelines.

When a tenant has any read pipeline, the querier takes a conservative,
tenant-wide path for that query:

1. Stream matchers, label matchers and the time range are still pushed down.
2. Line filters, the limit and sidecar `|=` pruning are **not** pushed down.
   LogQL metadata filters are not a selection pushdown field at all; they see
   the transformed metadata because they are evaluated after the pipelines.
3. Matching pipelines run per returned row (selected against the row's
   original stream), and dropped rows disappear before filtering.
4. The original line/metadata filters and then the original limit are applied
   to the transformed rows, preserving timestamp order and iterator
   early stop.

For aggregations (log-range metric queries) with any read pipeline, the
querier reads raw matched rows without line filters or limit, applies the
read pipelines, then evaluates filters and groups buckets. `count` and
`bytes` are computed from the transformed row, and included metadata is the
transformed metadata.

Cost follows from this: if the tenant has **any** read pipeline, **every**
LogQL log query for that tenant, on any stream, asks storage for all rows
matched by the request's time range and stored-label matchers. No line filter,
no limit and no line-sidecar pruning is pushed down. Time-range and
stored-label matcher pushdown are preserved. After transformation satisfies
the original limit, the wrapper stops pulling further rows from the source
iterator.

Aggregations have an additional memory cost. The querier groups transformed
rows in Go and keeps one accumulator per bucket and label set, plus one per
distinct metadata set when `IncludeMeta` applies. Memory therefore grows with
the number of groups the pipeline output produces, not just with the number of
rows read; a pipeline that rewrites or extracts high-cardinality values can
increase both group count and query memory.

Pushing down per dataset when a query's selector pins a dataset that no read
pipeline applies to is future work.

The fixture measurement used an 8-CPU VM with Go 1.27.3, GOMAXPROCS 8,
warm cache and no race instrumentation, with local filesystem storage and a
SQLite catalog. The plantest fixture (`3443` selected raw rows, `3000` bulk
matches) was queried as `{service_name="api"} |= "bulk"` in
`[T0-1us, T0+1h)`, Forward, `limit=100`. After 2 warmups per case, 7
alternating samples gave:

| case | median | raw iterator yields | raw request |
|---|---|---|---|
| no read definitions | 10.47 ms | 100 | `limit=100`, 1 filter |
| unmatched read pipeline on another selector | 14.25 ms | 503 | `limit=0`, 0 filters |

Both cases returned the same 100 rows. The unmatched-pipeline case made the
raw request over 5 candidate files with `pruned_index=0` and `bytes=37341`,
against 2 files, `pruned_index=3` and `bytes=25698` for the pass-through case.
These numbers are a fixture illustration, not a production guarantee.
Candidate bytes are compressed catalog bytes submitted to the executor, not
physical I/O; raw iterator yields are iterator pulls, not physical scanned
rows.

Reproduce with
`go test ./internal/pipeline/ -run '^TestReadSourceFixtureCost$' -count=1 -v`
(omit `-race`).

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

## HTTP API

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

### Versions, delete and recreate

`version` is optimistic concurrency per pipeline name. A create stores version
1. Every successful replace or delete increments it. A replace or delete must
send the live version; a stale writer gets `409`, and a missing name gets
`404`. Two concurrent creates for one name have exactly one winner.

Delete is a soft delete (tombstone). The version is retained and incremented,
`GET`/`LIST` omit the pipeline, and a later create with the same name starts
above the tombstone version and gets a fresh `created_at`. Versions therefore
never go backwards across delete/recreate cycles.

### Dry-run

Dry-run validates the candidate exactly like a write and runs it over sample
entries without any catalog, WAL or object write. Each entry's original
dataset comes from its own `labels`; an entry outside the selector passes
through untouched. `timestamp` is optional and echoed unchanged. The response
lists every surviving entry in input order with its resulting `line`,
`labels` and `meta`, plus the number of dropped entries. An `on_error: reject`
candidate answers `400`.

### curl examples

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

## Promote versus stored columns

`promote` copies a value into the entry's stream labels before ingest. That
label becomes its own physical Parquet column only when the existing
`-ingest.promoted-labels` configuration names it for that stream. The flag
takes `stream=col1,col2` assignments, repeated or separated by `;`; a `*`
assignment applies to every stream without its own, and the built-in default is
`*=level`. Pipelines never create typed columns dynamically and never change
the schema; a promoted label that is not configured as a column is stored in
the labels map like any other label. Adding the label to
`-ingest.promoted-labels` later applies to new files.

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
