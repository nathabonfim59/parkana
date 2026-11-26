# Read semantics

A read pipeline (`"stage": "read"`, see [Pipelines](index.md)) changes the
rows a LogQL query returns, never the stored data. This page explains which
routes it applies to, what the querier still pushes down to storage, and
what a read pipeline costs.

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

## Cost

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

## Fixture measurement

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
