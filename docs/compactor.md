# Compactor

The compactor role keeps the object store tidy. Ingesters upload many small
Parquet files; the compactor merges them into larger ones so queries open fewer
objects, applies retention, deletes objects that the catalog no longer needs
and builds search sidecars for files that lack one. It talks to the rest of
Parkana only through the [catalog](catalog.md) and the
[object store](storage.md), so it can run in its own process. It is part of
`-roles=all`; run it in exactly one process per catalog.

## One cycle

The compactor runs one cycle at startup and then one every
[`-compactor.interval`](configuration.md#compactor) (default `1m`). A failed
cycle is logged, counted in `parkana_compactor_cycle_errors_total` and retried
on the next tick; it never stops the other roles in the process.

Each cycle does three things, in this order:

1. Cleanup: retention marking, purge of deleted files after the delete delay,
   and orphan garbage collection.
2. Merges, stream by stream.
3. Sidecar backfill, when `-compactor.sidecar-backfill` is on (the default).

The phases are independent: an error in one is reported with the others and
does not stop the rest of the cycle.

The compactor scans only the `default` tenant. Parkana is single tenant
(`-multi-tenant` is rejected at startup), so this covers all data it can
store.

## What it merges

Within one stream, the compactor groups the live files by the UTC hour in their
object key (`.../{yyyy}/{mm}/{dd}/{hh}/...`, see [object keys](storage.md#object-keys)).
Files of different streams or different hours are never merged together.

For each hour, oldest hour first:

- Files whose size is at or above
  [`-compactor.target-size`](configuration.md#compactor) (default `512MiB`) are
  left alone.
- The remaining files are taken smallest first while the batch stays at or
  under the target size and holds at most 32 files.
- Files whose promoted columns differ only in letter case from a file already
  in the batch are left out, because one Parquet schema cannot hold both.
- A batch needs at least two files; otherwise nothing happens for that hour.

One batch is merged per stream and hour per cycle; leftover files wait for the
next cycle. The merged rows keep timestamp order, and the merged file gets a
new key in the same hour.

A merge removes exact duplicates: rows with the same timestamp, the same
complete label set, the same structured metadata and the same line bytes are
kept once. Collectors deliver at least once, so a retried push can store the
same rows twice; queries already count such rows once (see
[query](query/index.md#what-runs-in-duckdb-and-what-runs-in-go)), and the merge stops
storing them twice. The compactor checks that the merged row count plus the
removed duplicates equals the input row count, and refuses the merge
otherwise.

Promoted columns from all inputs are unioned: a column that only some inputs
have is filled with nulls in the rows of the others. Values a label had in the
`labels` map stay in the map; the query path reads a label from its column
first and then from the map, so results are the same (see
[schema evolution](storage.md#schema-evolution)).

## How a merge is committed

1. The merged Parquet is streamed into a new object with a fresh key.
2. Its row counts are checked against the catalog rows of the inputs.
3. Its search sidecar is built from the stored object and uploaded next to it.
   A file with more distinct n-grams than `-index.max-grams` gets no sidecar.
4. The catalog swaps the files in one transaction (`ReplaceFiles`): the inputs
   are marked deleted and the merged file is added. If any input is already
   deleted or missing, nothing changes.

A merge never deletes objects. If it fails before the catalog swap, the inputs
and their catalog rows stay as they were, and the new object, if one was
written, is left unreferenced for orphan collection.

## Delete delay

Files marked deleted, by a merge or by retention, stay in the catalog for
[`-compactor.delete-delay`](configuration.md#compactor) (default `10m`).
During that time their objects are kept, so a query that listed the files
before the swap can still read them. After the delay the purge drops the
catalog rows and then deletes the data objects and their sidecars.

The purge cutoff comes from the compactor's clock at the start of the cycle,
so a file deleted in one cycle is purged at the earliest in a later cycle: the
real delay is the delete delay plus up to one interval. Keep the delay longer
than your longest query: [`-querier.timeout`](configuration.md#querier) is `2m`
by default.

## Retention

[`-compactor.retention`](configuration.md#compactor) (default `0`, keep
forever) works on whole files. Each cycle marks deleted every live file whose
newest row is older than now minus the retention. A file that still holds one
row inside the retention window is kept whole, so data can outlive the
retention by up to the time span of one file; ingest and compaction keep that
span within one UTC hour.

Retention only marks files. Their objects go through the same delete delay and
purge as merged inputs. Queries do not filter by retention: rows stay
queryable until their file is marked.

## Orphan collection

Each cycle also lists every object under `default/logs/` in the object store
and deletes the ones that no catalog row references, once the object is older
than [`-compactor.orphan-grace`](configuration.md#compactor) (default `1h`).
Rows marked deleted still count as references, so an object waiting for its
delete delay is safe. The age is the object's modification time as the store
reports it (the file mtime on the fs backend, `LastModified` on S3), compared
with the compactor's clock.

Orphans come from merges that failed after the upload and from uploads whose
catalog write failed. The grace protects uploads in flight: an ingester never
adds a file to the catalog more than 30 minutes after it uploaded the object,
so the grace is at least `1h`; a shorter one is a configuration error. Raise it
for a slow object store or when the clocks of the store and the compactor host
may drift apart. `-compactor.orphan-grace=0` turns orphan collection off.

New in 1.2.0, the compactor lists all objects before it deletes any, and three
rules keep it from deleting stored logs on a bad day:

- With `-catalog.driver=memory` orphan collection never runs. The compactor
  logs `orphan collection disabled` once at startup, because a memory catalog
  forgets every file on restart and every object would look orphaned.
  Retention and purge still run on the rows the catalog has.
- A safety stop skips the whole sweep when there are at least 10 candidates
  and they are more than
  [`-compactor.orphan-max-fraction`](configuration.md#compactor) (default
  `0.1`) of the objects listed under the tenant's prefix. Legitimate orphans
  are a few per crash or failed merge; a wrong, empty or rolled-back catalog
  makes most objects look orphaned. A skipped sweep deletes nothing, logs
  `orphan collection skipped` at error level and increments
  `parkana_compactor_orphan_sweeps_skipped_total`. It is not a cycle error. See
  [Orphan collection skipped](operations.md#orphan-collection-skipped) for
  what to do. The floor of 10 lets a small store remove a few crash leftovers,
  and limits what a wrong catalog can cost a tiny store to 9 objects. A value
  of `1` turns the stop off.
- A listing that fails or is canceled part way deletes nothing, so the safety
  stop always judges the complete listing. In 1.0.0 the compactor deleted the
  orphans it had seen before the listing failed.

> [!WARNING]
> The same rule still deletes real data when the compactor's catalog does not
> match the object store and the missing objects stay under the safety stop's
> fraction. Every unreferenced object older than the grace is deleted on the
> next cycle. This happens when:
>
> - the SQLite file is deleted or replaced, or `-catalog.dsn` points at
>   another database;
> - an older catalog backup is restored over newer objects;
> - two processes with separate catalogs share one bucket or one
>   `-storage.fs.dir`, for example an ingester and a compactor split into two
>   processes that each open their own local SQLite file. The compactor does
>   not find the ingester's files in its catalog and deletes them once they
>   are older than the grace.
>
> A catalog that misses most objects trips the safety stop; a catalog that
> misses a few of them, for example a backup a few minutes older than the
> objects, does not.

Every process that uses one object store must use the same catalog. Of the
three drivers, only Postgres (new in 1.2.0) is built to be shared by processes
on several machines (see [catalog](catalog.md#sharing-one-catalog)). A SQLite
file can only be shared by processes on one machine, and a memory catalog
exists only inside its own process. Back the catalog and the objects up
together (see [backup and restore](catalog.md#backup-and-restore)), and stop
the compactor before you restore or move either one. The orphan scan only runs
after the catalog listing succeeds: a catalog that cannot be read stops the
scan, and an empty one against a store with 10 or more old objects trips the
safety stop.

## Sidecar backfill

With `-compactor.sidecar-backfill` on, the last phase of each cycle visits
every live file the catalog lists without a sidecar: files from before
sidecars existed, files over the gram cap and files whose sidecar flag update
failed. It reads the stored object, builds the sidecar with the current
[`-index.*`](configuration.md#search-sidecars) settings, uploads it and then
sets the catalog flag, so the catalog never points at a missing sidecar.
Failures are isolated per file.

Files found over the gram cap are remembered in memory (up to 4096 keys) so
they are not read again every cycle. The memory is empty after a restart, so
the first cycle after a restart reads each of them once more.

## Label summaries

The catalog keeps, per file, the distinct values of each promoted label and a
`LabelsComplete` flag (see [catalog](catalog.md#what-the-catalog-stores)).
Queries prune files by these values only when the flag is set. Every file a
merge writes gets a complete summary. Backfill only adds sidecars and never
rewrites summaries, so a file cataloged before summaries were complete stays
outside label pruning until a merge rewrites it. Files at or over the target
size, and files alone in their hour, are never merged.

## What queries see during compaction

A query lists the live files once. Before the catalog swap it sees the inputs;
after it, the merged file; never both, and never neither. A query that listed
the inputs before the swap keeps reading them because their objects survive
the delete delay. If an object disappears before the first row is read, the
querier lists the files again and retries once. Duplicate rows count once at
read time, so overlapping copies never show twice.

## Metrics

`parkana_compactor_cycles_total`, `parkana_compactor_cycle_errors_total`,
`parkana_compactor_cycle_duration_seconds`,
`parkana_compactor_merge_errors_total`, `parkana_compactor_files_merged_total`
and `parkana_compactor_bytes_merged_total` cover cycles and merges;
`parkana_compactor_orphan_sweeps_skipped_total` counts orphan sweeps the safety
stop skipped; the `parkana_index_*` metrics cover sidecar builds. The full list
is in [operations](operations.md#metrics). Cleanup counts (marked, purged,
orphaned) are logged at info level as `compactor cleanup`.
