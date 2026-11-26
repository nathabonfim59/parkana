# Query best practices

Parkana has no secondary index over log content or labels. A query costs
roughly the number of Parquet files it reads and the rows those files hold in
the query range. Four things decide which files a query reads: the time range,
the stream the selector names, the label summaries the catalog keeps per file,
and the search sidecar next to each file. [Query path](index.md#how-a-query-finds-its-files)
describes the mechanism; this page says how to write queries that use it, and
how to check that they did.

None of the pruning below can change a result. A file is skipped only when it
provably holds no matching row, and DuckDB re-checks every matcher, line
filter and the time range on the files that remain.

## What prunes

The table lists query shapes and what each one lets the querier skip. The
counter is the `result` label of `parkana_query_files_total` that grows when a
file is skipped (see [How to see what a query read](#how-to-see-what-a-query-read)).
Every row was checked against a local Parkana with two files in the
`checkout` stream (one holding `payment timeout`, one not) and one file in
`payments`.

| Query shape | What it skips | Counter |
|---|---|---|
| `start` and `end` of the request | files whose time span does not overlap the range | none (never listed) |
| `{service_name="checkout"}`: equality on the [`-stream.labels`](../configuration.md#streams-and-promoted-labels), in priority order | every other stream | none (never listed) |
| `{job="api"}` while `service_name` comes first in `-stream.labels` | nothing: the stream is not decided | |
| `{service_name="checkout", level="error"}`: `=` on a promoted label | files whose complete label summary lacks the value | `pruned_labels` |
| `level=~"error"`, `level!="info"`, `level!~"..."` | nothing: only `=` uses the summaries | |
| `\|= "timeout"`: a needle of at least [`-index.ngram`](../configuration.md#search-sidecars) bytes (default 4) | files whose sidecar proves the needle absent | `pruned_index` |
| `\|= "timeout" \|= "orders"` | files missing either needle: each `\|=` prunes on its own | `pruned_index` |
| `\|= "tim"`: a needle shorter than the gram length | nothing | |
| `\|~ "timeout"`, `\|~ ".*timeout.*"`: a regexp that is one literal | the same files as `\|= "timeout"` | `pruned_index` |
| `\|~ "time.*out"`, `\|~ "time.ut"`, any other regexp | nothing | |
| `\|~ "timeout\|refused"`, `\|~ "payment (timeout\|refused)"`, `\|= "timeout" or "refused"` | nothing: an OR cannot prune | |
| `\|~ "(?i)timeout"` and other case-insensitive matches | nothing | |
| `!= "timeout"`, `!~ "..."` | nothing | |
| `\| json \|= "timeout"`: a line filter after a parser | the same files as `\|= "timeout"` | `pruned_index` |
| `\| json \| level="error"`: a label filter on a parsed or stream label | nothing: label filters run in Go | |
| `count_over_time({service_name="checkout"} \|= "timeout" [5m])` | the same files as the log query | `pruned_index` |
| any query of a tenant that has a read pipeline | no line filter prunes, for every stream of that tenant | |
| `POST /api/v1/sql` with any `WHERE` | nothing beyond `from` and `to` | not counted |

Equality pruning by label summary needs two things in each file: the label is
a promoted column there ([`-ingest.promoted-labels`](../configuration.md#streams-and-promoted-labels),
default `*=level`), and the catalog row has a complete summary
(`LabelsComplete`, see [catalog](../catalog.md#what-the-catalog-stores)).
Files that miss either are read.

A sidecar that is missing, unreadable or corrupt makes the query slower, never
wrong: the file is read. Files with more distinct grams than
`-index.max-grams` get no sidecar, and the compactor builds sidecars for files
that lack one (`-compactor.sidecar-backfill`).

## Narrow the time range

The time range is the first and cheapest cut: the catalog lists only the
files whose `[min_ts, max_ts]` overlaps it. Every file is one stream and at
most one UTC hour, so a range of one hour reads at most two hours of files per
stream, and a range of a week reads every file of that week.

- Pass `start` and `end` explicitly. A log query without them reads the last
  hour.
- Search a long period as several bounded windows, newest first, and stop when
  you have the answer, instead of one query over the whole period.
- A query that times out (504 after `-querier.timeout`, `2m` by default) says
  nothing about whether the data exists. Make the window smaller or the
  selector narrower and run it again.

## Name the stream

A stream is a named dataset, chosen at ingest by the first label of
`-stream.labels` (default `service_name,job`) that a line carries. When the
selector's `=` matchers decide that choice the same way, the querier lists
only that stream's files.

- `{service_name="checkout"}` reads only the `checkout` stream.
- `{job="api"}` reads every stream with the default setting: a line with
  `job="api"` could also carry a `service_name`, which has priority and names
  another stream.
- `=~` and `!=` on a stream label never decide the stream.

Put `=` matchers on the stream labels first, then `=` matchers on promoted
labels such as `level`. Regexp and negative matchers are applied by DuckDB to
the rows of every file listed.

## Filter lines with `|=` first

Each file has a search sidecar: a bloom filter over every 4-byte sequence
(`-index.ngram`) of every line. A `|=` needle with a 4-byte sequence the
filter does not hold cannot be in the file, so the file is skipped.

- Use the most selective literal you know: a request id, an error code, an
  exception class. A rare needle skips more files than a common word.
- The match is case-sensitive and byte for byte. `|= "Timeout"` does not
  match `timeout`, and a case-insensitive match prunes nothing.
- A needle shorter than 4 bytes cannot prune.
- Several `|=` filters each prune, because a line must contain all of them.
- An OR prunes nothing. `|= "a" or "b"` and `|~ "a|b"` read every file of the
  stream. If the alternatives are selective, run one `|=` query per
  alternative.
- `!=` and `!~` prune nothing; put them after a `|=`.

Parkana compiles regexp line filters the way Loki does, and Loki simplifies
some regexps into plain substring matches. Where that simplification yields a
single case-sensitive literal, the result is the same filter as `|=` and it
prunes:

| Regexp | Becomes | Prunes |
|---|---|---|
| `\|~ "timeout"` | `\|= "timeout"` | yes |
| `\|~ ".*timeout.*"`, `\|~ "timeout.*"` | `\|= "timeout"` | yes |
| `\|~ "timeout\|refused"` | `\|= "timeout" or "refused"`, matched in Go | no |
| `\|~ "payment (timeout\|refused)"` | `\|= "payment timeout" or "payment refused"`, matched in Go | no |
| `\|~ "(?i)timeout"` | a case-insensitive substring match in Go | no |
| `\|~ "time.ut"`, `\|~ "time.*out"` | a regexp run by DuckDB | no |

Do not rely on the simplification: write `|= "literal"` when you mean a
literal, and add `|~` after it when you need a pattern. `{service_name="checkout"} |= "timeout" |~ "time(out|d out)"`
skips every file without `timeout` and matches the pattern only on the rest.

## Parsers, label filters and the limit

Line filters before the first `line_format` are pushed to DuckDB and the
sidecars, also when they come after `| json` or `| logfmt`. Everything else
runs in Go over the rows DuckDB returns: parsers, label filters,
`line_format`, `label_format`, `unwrap`, and line filters after
`line_format`.

- `{service_name="checkout"} | json | level="error"` reads every row of the
  stream in the range and parses each one in Go.
- `{service_name="checkout"} |= "timeout" | json | db="orders"` skips the
  files without `timeout` and parses only the lines that contain it.

The `limit` is pushed to DuckDB only when nothing after the selector and the
pushed line filters can drop or change a row. With a parser, a label filter
or a formatting stage, DuckDB returns every matching row in the range and Go
applies the limit. The limit never reduces which files are read: DuckDB
sorts the union of all candidate files before it returns the first row.

`direction=backward` (the default) returns the newest rows first and
`forward` the oldest. Pick the one where your answer is, so that a small
`limit` is enough.

## Metric queries

A metric query reads one range from `start` minus the range of its
`[range]` selector up to `end`, with the same pruning as the log query
inside it. `count_over_time(... [1h])` over the last hour reads two hours.
The `step` decides how many points come back, not how much is read, but a
range query with more than 11,000 points per series answers 400.

`count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` are counted in
DuckDB when only line filters (and `drop` or `keep`) come before the range.
With a parser or label filter every matching row goes to Go to be counted.
Aggregate with `sum by (...)` over few labels: more than `-logql.max-series`
series (500) answers 400.

## Labels, series and stats

`/loki/api/v1/labels`, `/label/{name}/values`, `/series` and the
`/index/stats` and `/index/volume` routes are not answered from an index.
Each one runs a DuckDB statement over the files of its window, so a wide
window reads every file of that window. Discover streams with a narrow
window (the last hour) and a selector on a stream label when you have one.

## Read pipelines

When a tenant has any read pipeline, its log and metric queries
(`query` and `query_range`) lose line filter, limit and sidecar pushdown for
every stream of that tenant, including streams no pipeline matches. The time
range, the stream and label pruning still apply. The pipelines need to see
every row before a filter can, so the narrow time range and the stream
selector are the only cuts left. See [read semantics](../pipelines/read.md#cost).

## SQL API

`POST /api/v1/sql` reads every file of the tenant whose time span overlaps
`from` and `to`, in every stream, and loads all of their rows in that window
into the `logs` table before your statement runs. A `WHERE` on `_ts`,
`labels`, promoted columns or `line` filters that table; it does not skip
files. `from` and `to` are the only thing that bounds the read.

- Keep `from` and `to` as narrow as the question allows, and repeat the window
  in a `WHERE _ts` clause.
- The load counts against `-sql.memory-limit` (`1GiB`): a window with more
  rows than fit answers 400.
- Select the columns you need and set a `limit`; that bounds the result and
  the work after the load, not the files read.
- For a search over a long period, use LogQL with `|=` and a stream selector,
  which prunes, and SQL for aggregation over a short window.

```sh
curl -s -X POST "http://localhost:3100/api/v1/sql" -H 'Content-Type: application/json' \
  --data-binary @- <<'EOF'
{"query": "SELECT _ts, line FROM logs WHERE labels['service_name'] = 'checkout' AND coalesce(\"level\", labels['level']) = 'error' ORDER BY _ts DESC LIMIT 20",
 "from": "2026-11-24T10:00:00Z", "to": "2026-11-24T11:00:00Z", "limit": 20}
EOF
```

[SQL](sql.md#the-logs-relation) explains why `coalesce` is needed for
promoted labels.

## How to see what a query read

Two process-wide counters show what queries read:

- `parkana_query_files_total{result="scanned"}` counts files submitted to
  DuckDB, `result="pruned_labels"` files skipped by label summaries and
  `result="pruned_index"` files skipped by sidecars.
- `parkana_query_bytes_scanned_total` counts the compressed bytes of the
  scanned files.

They cover LogQL queries, metric queries and the metadata routes, not the SQL
API. There is no per-query count in a response: the `stats` of a
`query_range` answer carry only the execution time and the number of entries
returned. To measure one query, read `/metrics` before and after it on a
querier that runs nothing else:

```sh
curl -s "http://localhost:3100/metrics" | grep '^parkana_query_files_total'
curl -s -G "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={service_name="checkout"} |= "timeout"' \
  --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z \
  --data-urlencode limit=100 --data-urlencode direction=backward
curl -s "http://localhost:3100/metrics" | grep '^parkana_query_files_total'
```

On the test data above, this query added 1 to `scanned` and 1 to
`pruned_index`; the same query with `|~ "time.ut"` added 2 to `scanned`.

`/loki/api/v1/index/stats` answers how many files (`chunks`) a selector
reads in a window, before line filters:

```sh
curl -s -G "http://localhost:3100/loki/api/v1/index/stats" \
  --data-urlencode 'query={service_name="checkout"}' \
  --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z
```

```json
{"streams":2,"chunks":2,"bytes":270,"entries":6}
```

For the SQL API, run the querier with `-log.level=debug`: each request logs
`query: sql: materialized` with `files=N`, the number of files it loaded.

## Checklist

1. Bound the window with `start` and `end`; search long periods in steps.
2. Name the stream with `=` on the `-stream.labels`.
3. Add `=` on promoted labels; avoid `=~` and `!=` where `=` works.
4. Put the most selective `|=` literal (4 bytes or more, exact case) first.
5. Split an OR into separate `|=` queries.
6. Filter lines before parsing, and parse only what you need.
7. Choose the `direction` and a small `limit`.
8. Keep SQL windows short; `WHERE` does not reduce the files read.
9. An empty result over a complete window means there is no match; a timeout
   means nothing.
