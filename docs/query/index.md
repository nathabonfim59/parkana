# Query path

The querier role answers the Loki read API, live tail and the SQL endpoint. A
LogQL query is parsed in Go, turned into a selection that says which streams,
time range and line filters it needs, and run with embedded DuckDB over the
Parquet files the catalog lists for that range. This page follows a query
from the request to the files it reads, then covers the caches, the limits,
read pipelines and live tail. What LogQL Parkana supports is in
[LogQL compatibility](logql.md); the SQL endpoint is in [SQL](sql.md); live
tail is in [Live tail](tail.md). How to write queries that read few files is
in [Query best practices](best-practices.md).

## Routes

The querier registers:

- `GET` and `POST /loki/api/v1/query` and `/loki/api/v1/query_range`;
- `GET` and `POST /loki/api/v1/labels` and `/loki/api/v1/series`, and
  `GET /loki/api/v1/label/{name}/values`;
- `GET /loki/api/v1/index/stats`, `/loki/api/v1/index/volume` and
  `/loki/api/v1/index/volume_range`;
- `GET /loki/api/v1/tail` (WebSocket);
- `POST /api/v1/sql`.

Every request runs for the tenant in `X-Scope-OrgID`, which must be absent or
`default`; any other value answers 401.

## From LogQL to DuckDB

```text
 LogQL ──► parser (internal/logql) ──► plan.Selection
                                         │ tenant, stream matchers, line filters,
                                         │ [from, through), direction, limit
                                         ▼
                      catalog: live files of the tenant in the time range,
                               of the stream the matchers force
                                         │
                      label summaries: drop files whose recorded label
                                       values cannot match
                                         │
                      search sidecars: drop files that cannot contain
                                       a |= needle
                                         ▼
                      DuckDB: read_parquet over the remaining files,
                              filters re-checked, DISTINCT, ORDER BY, LIMIT
                                         │
                                         ▼
               Go (internal/logql): parsers, label filters, formatting,
                                    metric steps and aggregation ──► JSON
```

DuckDB runs in the querier process with `TimeZone=UTC` and its Parquet
metadata cache on. It reads local files only: objects come from the object
store through the querier's cache (S3) or straight from disk (fs), so DuckDB
never needs S3 credentials or network access.

## How a query finds its files

Pruning happens in four steps, from cheapest to most expensive. None of them
can drop a file that has a matching row: DuckDB re-checks every matcher, line
filter and the time range on the files that remain.

1. Time range. The catalog lists the tenant's live files whose
   `[min_ts, max_ts]` overlaps the query range.
2. Stream. When the selector's equality matchers decide the stream the same
   way ingest does, only that stream's files are listed. `{service_name="checkout"}`
   reads only the `checkout` stream with the default
   [`-stream.labels`](../configuration.md#streams). `{job="api"}` alone does not
   decide it, because a row could also carry a `service_name`, which has
   priority, so it reads every stream.
3. Label summaries. Each `=` matcher on a promoted label is checked against
   the distinct values the catalog recorded for the file. Only files whose
   summary is complete (`LabelsComplete`) are pruned this way; see
   [catalog](../catalog.md#what-the-catalog-stores).
4. Search sidecars. Each `|=` needle is checked against the file's n-gram
   bloom filter; a file that cannot contain a needle is skipped. `!=`, `|~`,
   `!~` and needles shorter than [`-index.ngram`](../configuration.md#search-sidecars)
   never prune, and a sidecar that cannot be read keeps the file. See
   [search sidecars](../storage.md#search-sidecars).

`parkana_query_files_total` counts files by result (`scanned`,
`pruned_labels`, `pruned_index`) and `parkana_query_bytes_scanned_total` the
bytes of scanned files. [Query best practices](best-practices.md) has a table of
query shapes and what each one prunes, and how to measure one query with these
counters.

If an object disappears between the listing and the first row (a compactor
purge, for example), the querier lists the files again and retries once.

## What runs in DuckDB and what runs in Go

DuckDB gets the stream matchers (`=`, `!=`, `=~`, `!~`), the line filters, the
time range `from <= ts < through`, the direction and, when nothing else
changes the rows, the limit. Count and byte buckets for `count_over_time`,
`rate`, `bytes_over_time` and `bytes_rate` are computed in DuckDB too, when
the query has only line filters (and `drop`/`keep`) before the range.

Everything else runs in Go over the rows DuckDB returns:

- Line filters after the first `line_format`, and case-insensitive matches,
  are evaluated in Go.
- Parsers (`json`, `logfmt`, `regexp`, `pattern`), label filters,
  `line_format`, `label_format` and `unwrap` run in Go. When any of them is
  present, the limit is applied in Go as well, because DuckDB cannot know which
  rows survive.
- Metric steps, range aggregations other than the four above, vector
  aggregations and binary operations run in Go.

Rows that are identical in timestamp, label set, structured metadata and line
count once: every DuckDB query applies `DISTINCT` before ordering, limit and
grouping. Collectors retry pushes, so the same rows can be stored twice; the
compactor removes such copies when it merges files (see
[compactor](../compactor.md#what-it-merges)).

The semantics are those of the reference source in `internal/plan`: a missing
label equals `""`, label regular expressions are anchored RE2, line regular
expressions are unanchored RE2, and `|=` is a byte substring match.

## Object cache

With the s3 backend the querier downloads every object it reads into
[`-querier.cache-dir`](../configuration.md#querier) (default `<data.dir>/cache`)
and lets DuckDB read the local copy. Keys are immutable, so a copy never goes
stale. The cache keeps up to `-querier.cache-size` (default `10GiB`) and
evicts the least recently used objects above it; objects a running query uses
are never evicted. After a restart the querier picks up the copies already on
disk. Size the cache to the data you query often: repeated queries over cached
files do not touch the object store.

With the fs backend the querier reads objects in place from
`-storage.fs.dir`. It creates no cache directory, and `-querier.cache-dir` and
`-querier.cache-size` have no effect.

Decoded search sidecars are kept in memory, up to
`-querier.sidecar-cache-size` (default `64MiB`), with either backend. The
`parkana_querier_cache_*` and `parkana_querier_sidecar_cache_*` metrics show
hits, downloads, evictions and sizes.

## Limits

| flag | default | when exceeded |
|---|---|---|
| [`-querier.timeout`](../configuration.md#querier) | `2m` | 504 with Loki's "request timed out, decrease the duration of the request or add more label matchers" text |
| [`-logql.max-entries`](../configuration.md#logql-limits) | `5000` | a log query whose `limit` is larger answers 400 |
| [`-logql.max-series`](../configuration.md#logql-limits) | `500` | a metric query returning more series answers 400 and suggests a `by()` aggregation |

`-querier.timeout` covers every read route and `/api/v1/sql`, but not live
tail. A request whose client disconnects ends with status 499. A range query with more than
11,000 points per series (`(end - start) / step`) answers 400 "'step' too
small", as in Loki. Without parameters, a log query returns 100 entries of the
last hour, and a range query's step is the range divided by 250, at least one
second. There is no limit on the length of the query range. The SQL endpoint
has its own limits, listed in [SQL](sql.md#limits).

## Read pipelines

A read pipeline rewrites rows when they are read: it can change the line, add
or overwrite structured metadata and drop rows, but never changes stream
labels or stored data (see [read semantics](../pipelines/read.md)). Read
pipelines apply to `query` and `query_range` only; labels, series, stats,
volume, live tail and SQL read the stored data.

A LogQL query sees one version of the pipelines from start to end, even when a
definition changes while it runs.

When a tenant has any read pipeline, its log queries lose filter and limit
pushdown: the querier asks DuckDB for every row of the matched streams in the
time range, runs the pipelines, and only then applies line filters and the
limit in Go. Line filters see the rewritten line, and dropped rows do not use
up the limit. The cost is a full scan, without sidecar pruning, of the matched
streams, for every stream of that tenant, including streams no pipeline
matches. Metric queries on that path also aggregate in Go.


`GET /loki/api/v1/tail` is a WebSocket on the querier that streams lines
from the ingesters as they arrive, before they are uploaded.
[Live tail](tail.md) describes it.
