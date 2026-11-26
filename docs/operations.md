# Operations

This page covers running Parkana in production: the readiness probe, the
metrics worth alerting on, logging, upgrades, backups and troubleshooting by
symptom. Flags and their defaults are in [Configuration](configuration.md);
status codes and error bodies are in [HTTP API behavior](http-api.md).

## Readiness

`GET /ready` answers `200` with the body `ready` once the HTTP server is
serving and every readiness check passes:

```console
$ curl -i http://localhost:3100/ready
HTTP/1.1 200 OK
Content-Type: text/plain; charset=utf-8

ready
```

It answers `503` with a one-line body otherwise:

| body | cause |
|---|---|
| `not serving yet` | the HTTP server has not started listening, or is shutting down |
| `ingest: ingest: replaying WAL or shutting down` | the ingester is replaying its WAL after a restart, or is draining for shutdown |
| `ingest: ingest: failed` | the ingester stopped after a fatal error (a WAL sync failure, for example) |

The ingester registers the only check. The querier and the compactor register
none, so a process without the ingester role is ready as soon as it listens;
catalog or object store problems show up in its requests and logs instead.

Use `/ready` as a readiness probe, not a liveness probe: during WAL replay it
answers `503` for as long as the replay takes. A fatal error in any role stops
every role of the process, which then exits with status 1 (the log line
`service failed` names the role). The packaged systemd unit restarts it after
5 seconds (`Restart=on-failure`).

`/ready` and `/metrics` pass through the tenant middleware like every other
route, so a probe or scraper must not send an `X-Scope-OrgID` header other than
`default`.

## Metrics

`GET /metrics` serves every collector in the Prometheus format: the
`parkana_*` metrics below plus the Go runtime (`go_*`) and process
(`process_*`) collectors. Every role registers its metrics on the same
endpoint, so scrape every process.

### What to alert on

| expression | why |
|---|---|
| `rate(parkana_http_requests_total{code=~"5.."}[5m]) > 0` | Server faults. On the push route a `500` means the WAL or the memtable could not record the request; a `503` means the ingester is not accepting pushes. The `route` label names the route pattern. |
| `increase(parkana_ingest_wal_errors_total[5m]) > 0` | A WAL fsync failed. The ingester stops accepting pushes and the process exits. |
| `increase(parkana_ingest_upload_total{result="error"}[15m]) > 0` | Upload passes fail. Data stays on the local disk, is not queryable and keeps the WAL from being reclaimed. |
| `increase(parkana_ingest_flush_total{result="error"}[15m]) > 0` | Writing local Parquet fails, for example because the disk under `-data.dir` is full. |
| `increase(parkana_ingest_backpressure_rejections_total[5m]) > 0` | The ingester is behind and answers pushes with `429`. The `where` label names the bound: `memory` means local Parquet writes fail or lag, `local` means uploads fail or lag. Clients retry a `429`, but most drop a batch once their retries run out (see [back-pressure](ingest.md#back-pressure)). See [A push is rejected](#a-push-is-rejected). |
| `rate(parkana_ingest_lines_total[10m]) > 0 and rate(parkana_ingest_upload_rows_total[30m]) == 0` | Pushes are accepted but nothing reached the object store for 30 minutes. With the default `-ingest.upload-max-age` of 10 minutes, every accepted row should be uploaded well within that window. |
| `increase(parkana_compactor_cycle_errors_total[30m]) > 0` | Compaction cycles fail: small files pile up and retention is not applied. The log line `compaction cycle failed` has the error. |
| `increase(parkana_compactor_orphan_sweeps_skipped_total[1h]) > 0` | The orphan safety stop skipped a sweep because most old objects are missing from the compactor's catalog. Check the catalog before anything else; see [Orphan collection skipped](#orphan-collection-skipped). |
| `increase(parkana_querier_cache_downloads_total{result="error"}[10m]) > 0` | The querier cannot read objects from the object store, so queries fail. |
| `increase(parkana_pipeline_reloads_total{result="error"}[10m]) > 0` | A changed pipeline definition did not load; the role keeps running the previous version. |
| `increase(parkana_ingest_tail_evictions_total[10m]) > 0` | Live-tail viewers were too slow and were disconnected; they missed entries. |

Three more series help with capacity: `parkana_ingest_pending_generations`
should stay low (a value that keeps growing means local Parquet writes fall
behind the push rate), `parkana_ingest_unuploaded_bytes` should stay well
below `parkana_ingest_backpressure_limit_bytes` for the same `where` (alert
on a ratio above `0.8` to act before pushes are refused), and
`process_resident_memory_bytes` should stay below the container memory limit
the upload budget is derived from.

### Metric reference

The table below is generated from the collectors the all-in-one process
registers. It lists every metric `/metrics` exposes except the Go runtime
(`go_`) and process (`process_`) collectors.

<!-- metrics:begin (generated by make docs-metrics; do not edit) -->
| Metric | Type | Labels | Description |
|---|---|---|---|
| `parkana_compactor_bytes_merged_total` | counter |  | Input object bytes replaced by merges. |
| `parkana_compactor_cycle_duration_seconds` | histogram |  | Compaction cycle latency. |
| `parkana_compactor_cycle_errors_total` | counter |  | Compaction cycles that returned an error. |
| `parkana_compactor_cycles_total` | counter |  | Compaction cycles started. |
| `parkana_compactor_files_merged_total` | counter |  | Input files replaced by a merged object. |
| `parkana_compactor_merge_errors_total` | counter |  | File merges that failed. |
| `parkana_compactor_orphan_sweeps_skipped_total` | counter |  | Orphan GC sweeps that deleted nothing because the candidates exceeded -compactor.orphan-max-fraction of the listed objects. |
| `parkana_http_request_duration_seconds` | histogram | `route` | HTTP request latency by route pattern. |
| `parkana_http_requests_total` | counter | `code`, `route` | HTTP requests by route pattern and status code. |
| `parkana_index_build_duration_seconds` | histogram | `source` | Sidecar index build latency for finished builds (successful or over the gram cap), by write path. |
| `parkana_index_built_total` | counter | `source` | Sidecar index builds that returned encoded bytes, by write path. |
| `parkana_index_bytes_total` | counter | `source` | Encoded sidecar bytes produced by successful index builds, by write path. |
| `parkana_index_skipped_too_many_grams_total` | counter | `source` | Sidecar index builds skipped because the file exceeded the gram cap, by write path. |
| `parkana_ingest_backpressure_limit_bytes` | gauge | `where` | Back-pressure bound per counter of parkana_ingest_unuploaded_bytes; pushes answer 429 at or above it. |
| `parkana_ingest_backpressure_rejections_total` | counter | `where` | Pushes refused with 429 because a back-pressure bound was reached, by bound. |
| `parkana_ingest_bytes_total` | counter | `tenant` | Decoded push bytes admitted into the ingest memtable. |
| `parkana_ingest_flush_total` | counter | `result` | Local Parquet generation publications by result. |
| `parkana_ingest_lines_total` | counter | `tenant` | Log lines admitted into the ingest memtable. |
| `parkana_ingest_pending_generations` | gauge | `tenant` | Sealed generations awaiting local publication. |
| `parkana_ingest_quarantine_bytes` | gauge | `tenant` | Bytes of the regular files in the tenant's local quarantine directory. |
| `parkana_ingest_quarantine_files` | gauge | `tenant` | Regular files in the tenant's local quarantine directory. |
| `parkana_ingest_quarantine_sweep_errors_total` | counter |  | Quarantine sweep steps that failed; the files stay and the next sweep retries. |
| `parkana_ingest_replay_records_total` | counter | `tenant` | WAL records replayed per tenant at startup. |
| `parkana_ingest_tail_dropped_entries_total` | counter | `tenant` | Entries lost when a live-tail subscriber was too slow, by tenant. |
| `parkana_ingest_tail_evictions_total` | counter | `tenant` | Slow live-tail subscribers disconnected, by tenant. |
| `parkana_ingest_tail_published_entries_total` | counter | `tenant` | Entries queued for live-tail subscribers, by tenant. |
| `parkana_ingest_tail_subscribers` | gauge | `tenant` | Live-tail subscribers currently connected, by tenant. |
| `parkana_ingest_unuploaded_bytes` | gauge | `where` | Bytes waiting on their way to the object store: memory is the memtable estimate of rows not yet in local Parquet, local is local Parquet not yet uploaded. |
| `parkana_ingest_upload_bytes_total` | counter | `tenant` | Object bytes published to the object store per tenant. |
| `parkana_ingest_upload_files_total` | counter | `tenant` | Objects published to the object store per tenant. |
| `parkana_ingest_upload_key_rotations_total` | counter | `tenant` | Cumulative object-store upload key rotations per tenant, restored from the local manifest. |
| `parkana_ingest_upload_rows_total` | counter | `tenant` | Rows published to the object store per tenant. |
| `parkana_ingest_upload_total` | counter | `result` | Object store upload passes by result. |
| `parkana_ingest_wal_errors_total` | counter |  | WAL segment file sync failures. |
| `parkana_ingest_wal_fsync_seconds` | histogram |  | WAL segment file sync latency. |
| `parkana_pipeline_active_definitions` | gauge | `role` | Compiled pipeline definitions currently held in this role's snapshots across all seen tenants. |
| `parkana_pipeline_execution_duration_seconds` | histogram | `result`, `role`, `stage` | Compiled pipeline application latency, by stage and result. |
| `parkana_pipeline_executions_total` | counter | `result`, `role`, `stage` | Compiled pipeline applications, by stage and result. |
| `parkana_pipeline_reload_duration_seconds` | histogram | `result`, `role` | Pipeline snapshot reload latency for attempts that saw a changed change token, by result. |
| `parkana_pipeline_reloads_total` | counter | `result`, `role` | Pipeline snapshot reload attempts that saw a changed change token, by result. |
| `parkana_querier_cache_bytes` | gauge |  | Bytes currently occupied by cached objects. |
| `parkana_querier_cache_downloads_total` | counter | `result` | Remote object downloads by result. |
| `parkana_querier_cache_entries` | gauge |  | Cached objects currently indexed. |
| `parkana_querier_cache_evictions_total` | counter |  | Cached objects evicted to stay within the byte budget. |
| `parkana_querier_cache_fetched_bytes_total` | counter |  | Serialized object bytes copied from the object store into the cache, partial failed copies included; local-path hits copy nothing and do not count. Candidate transfer bytes, not physical disk reads. |
| `parkana_querier_cache_leases` | gauge |  | Object cache leases currently held by callers. |
| `parkana_querier_cache_requests_total` | counter | `result` | Object cache acquisitions by result. |
| `parkana_querier_sidecar_cache_bytes` | gauge |  | Accounted decoded bytes currently resident in the sidecar cache. |
| `parkana_querier_sidecar_cache_entries` | gauge |  | Decoded sidecars currently admitted in the sidecar cache. |
| `parkana_querier_sidecar_cache_evictions_total` | counter |  | Decoded sidecars evicted to stay within the resident byte budget. |
| `parkana_querier_sidecar_cache_fetched_bytes_total` | counter |  | Serialized sidecar bytes read from the object store. |
| `parkana_querier_sidecar_cache_fetches_total` | counter | `result` | Sidecar fetch/decode attempts by result. |
| `parkana_querier_sidecar_cache_requests_total` | counter | `result` | Sidecar cache lookups by result. |
| `parkana_query_bytes_scanned_total` | counter |  | Compressed catalog file bytes submitted to DuckDB across query read attempts, failed attempts included; candidate bytes, not physical reads. |
| `parkana_query_files_total` | counter | `result` | Catalog files entering query read attempts by result: scanned (submitted to DuckDB), pruned_index (dropped by search sidecars), pruned_labels (dropped by complete label summaries). |
| `parkana_sql_duration_seconds` | histogram |  | SQL API request wall time in seconds, from decoding through response encoding. |
| `parkana_sql_inflight` | gauge |  | SQL API requests currently admitted and executing. |
| `parkana_sql_requests_total` | counter | `outcome` | SQL API requests by bounded outcome: success, client_error, deadline, canceled or internal. |
| `parkana_sql_result_bytes_total` | counter |  | Encoded response bytes written by successful SQL API requests. |
| `parkana_sql_rows_total` | counter |  | Rows returned by successful SQL API requests. |
| `parkana_tail_dropped_entries_total` | counter | `tenant` | Entries dropped from a live-tail subscriber's bounded queue, by tenant. |
| `parkana_tail_entries_total` | counter | `tenant` | Entries written to live-tail WebSocket subscribers, by tenant. |
| `parkana_tail_subscribers` | gauge | `tenant` | Live-tail WebSocket subscribers currently connected, by tenant. |
| `parkana_tail_upstream_errors_total` | counter | `tenant` | Ingester tail subscriptions that ended with an explicit error, by tenant. |
| `parkana_upload_active_jobs` | gauge |  | Upload jobs actively processing in a worker. |
| `parkana_upload_inflight_estimate_bytes` | gauge |  | Estimated merge bytes held by admitted upload jobs. |
| `parkana_upload_job_source_bytes` | histogram |  | Compressed source-file bytes selected by one started upload job. |
| `parkana_upload_merge_seconds` | histogram |  | Uploader merge stage wall time, observed for successes and errors. |
| `parkana_upload_oversized_jobs_total` | counter |  | Admitted upload jobs whose estimate exceeded the memory budget and were admitted alone. Counts an admitted attempt even if canceled after acquisition, so it is not a count of completed runs. |
| `parkana_upload_put_seconds` | histogram | `kind` | Uploader object store Put wall time, observed for successes and errors. |
| `promhttp_metric_handler_errors_total` | counter | `cause` | Total number of internal errors encountered by the promhttp metric handler. |
<!-- metrics:end -->

## Logging

Parkana logs to standard error with `log/slog`. `-log.format` picks `text`
(the default) or `json`; the packaged `/etc/parkana/parkana.env` sets `json`.
`-log.level` picks `debug`, `info` (the default), `warn` or `error`. With the
package, read the logs with `journalctl -u parkana`.

At `info` Parkana logs its lifecycle and background work, not single requests.
`debug` adds one `http request` line per request (method, path, route, code,
duration, tenant); a response with a `5xx` status is logged at `warn` at every
level.

Lines worth knowing, by their `msg`:

| message | level | meaning |
|---|---|---|
| `starting parkana` | info | first line; carries `version`, `revision` and `roles` |
| `role not implemented yet; skipping` | warn | the scheduler role, part of `-roles=all`, does nothing yet; harmless |
| `http server listening` | info | the server accepts connections on `addr` |
| `upload memory budget` | info | the ingester's upload budget in `bytes`; `source` says where it came from (`fallback` means no container limit and no `GOMEMLIMIT` was found, so 512 MiB is used) |
| `ingest service ready` | info | WAL replay is done and pushes are accepted |
| `service failed` | error | a role returned an error; every role stops and the process exits with status 1 |
| `ingest: upload pass; will retry` and other `ingest: ...; will retry` lines | warn | a background flush, upload or WAL reclaim pass failed and runs again on its next interval |
| `upload memory: oversized estimate runs alone` | warn | one upload job needs more than the whole budget and waits until nothing else runs |
| `compaction cycle failed` | error | a compactor cycle returned an error; the next cycle runs after `-compactor.interval` |
| `compactor cleanup` | info | a cleanup pass changed something: `marked` (files past retention), `purged` (deleted files whose objects were removed), `orphaned` (objects deleted because no catalog row references them), `orphan_sweep_skipped` (the safety stop skipped the orphan sweep) |
| `orphan collection disabled: ...` | warn | logged once at startup with `-catalog.driver=memory`: the compactor never deletes unreferenced objects with that driver |
| `orphan collection skipped` | error | the orphan safety stop deleted nothing this cycle: `candidates` unreferenced old objects out of `listed`, `referenced` of them protected by the catalog, `fraction` the limit, `sample` up to 10 candidate keys; see [Orphan collection skipped](#orphan-collection-skipped) |
| `query failed` | warn | a query failed on the server side; client errors are logged at `debug` |
| `pipeline registry reload failed; keeping previous snapshot` | warn | a pipeline change could not be loaded |
| `live tail session ended with an error` | warn | a tail WebSocket closed because of an upstream or protocol failure |
| `http handler panic` | error | a handler panicked; the client got `500 internal error` |

## Upgrades

> [!WARNING]
> Every process that runs the compactor deletes objects that its catalog does
> not reference once they are older than `-compactor.orphan-grace` (default
> `1h`). Before starting an
> upgraded process, check that it opens the same catalog as before (the same
> `-data.dir` for SQLite, the same `-catalog.dsn` for Postgres). A process that
> starts with an empty or different catalog against the same object store
> deletes the existing data. See [Orphan objects are deleted](#orphan-objects-are-deleted).

With the deb or rpm package, install the new package over the old one. The
package keeps `/etc/parkana/parkana.env` and `/var/lib/parkana`, and keeps
`parkana.service` enabled or disabled as it was; a running service is
restarted on the new binary (new in 1.2.0). A deb upgrade from
1.0.0 is the exception: the 1.0.0 package stops and disables the service, so
start it again once after that upgrade, as
[Deploy](deploy.md#upgrade-and-remove) explains:

```sh
sudo systemctl enable --now parkana
```

On `SIGTERM` the ingester stops accepting pushes and drains; the unit allows it
2 minutes (`TimeoutStopSec=2min`). After the restart the ingester replays its
WAL, and `/ready` answers `503` until the log line `ingest service ready`.

The catalog migrates itself at startup, before the process serves: SQLite in
the catalog file, Postgres (new in 1.2.0) in one transaction under an advisory
lock, so several processes may start at once. Migrations only add tables and columns.
There is no downgrade: a binary older than the catalog's schema stops at
startup with `schema version N is newer than this binary supports (M)` and
writes nothing (new in 1.2.0; Parkana 1.0.0 does not check, so never start it
against a catalog a newer binary has migrated). Back up the catalog before an
upgrade (see [Backup and restore](#backup-and-restore)) so you can return to
the older binary with that backup.

Roles talk to each other through the catalog, the object store and the
internal tail route, with no version negotiation. Running different versions
side by side is not tested, so stop every process, upgrade them all, then
start them again.

## Backup and restore

Parkana keeps its state in three places, and they belong together:

| what | where | holds |
|---|---|---|
| catalog | `<data.dir>/catalog.db` (SQLite, with its `-wal` and `-shm` files) or the Postgres schema (Postgres is new in 1.2.0) | the list of objects, their time ranges and labels, and the pipeline definitions |
| object store | the bucket, or `<data.dir>/objects` with `-storage.backend=fs` | the Parquet objects and their search sidecars |
| data directory | `-data.dir` | the WAL and local Parquet with acknowledged logs that are not uploaded yet; the query cache under `cache/` can be rebuilt |

The catalog decides what exists: queries read only the objects it lists, and
an object it does not list is deleted by the compactor (see the warning
below). A catalog without its objects fails the queries that need them.
Parkana has no online backup command. For a consistent backup, stop every
process, copy the catalog (the SQLite files, or `pg_dump` of the schema), the
object store and the data directory, then start the processes again. Restore
all three from the same backup. See [Catalog](catalog.md) for the catalog
backends.

> [!WARNING]
> Restore the catalog and the objects together. A catalog restored from a
> backup older than the objects in the store does not list the newer objects,
> and the compactor deletes them once they are older than
> `-compactor.orphan-grace` (default `1h`). Objects restored next to an empty
> catalog trip the orphan safety stop instead (see
> [Orphan collection skipped](#orphan-collection-skipped)). See
> [Orphan objects are deleted](#orphan-objects-are-deleted).

## Troubleshooting

### A process does not start: the lock is held

New in 1.2.0. A process that runs the ingester or the querier role locks the
directories the role owns before it opens anything (see
[Split roles across processes](deploy.md#split-roles-across-processes)), and
stops when another process holds the lock:

```text
parkana: ingester lock /var/lib/parkana/ingester.lock is held by another parkana process (pid 4242): two ingesters cannot share -data.dir /var/lib/parkana; stop the other ingester or give this one its own -data.dir
```

The querier's error names `<cache dir>.lock` and the cache directory instead.
Check whether the pid in the message runs (`ps -p 4242`). If it does, it is
a second instance, often a manual start next to the systemd unit or two units
with the same directory: stop one, or give each process its own `-data.dir`
(ingester) or `-querier.cache-dir` (querier). If it does not, the holder runs
in another container or PID namespace with the same directory mounted, and
its pid means nothing on this host; `fuser -v /var/lib/parkana/ingester.lock`
or `lsof` on the host finds it. The message says `pid unknown` when the
holder had not written its pid yet.

Do not delete the lock file to get past the error. The running holder keeps
its lock on the deleted file, the new process creates and locks a fresh one,
and two ingesters then write over each other's WAL. The file stays after a
clean stop or a crash; that is expected and harmless.

### A push is rejected

The response body says why in one line. By status:

- `400`: the request itself. The usual causes are a missing `Content-Type`
  (anything but `application/json` is decoded as snappy protobuf), a body over
  `-ingest.max-request-size` (Parkana answers `400`, not `413`), a line over
  `-ingest.max-line-size`, an invalid label or metadata name, or an ingest
  pipeline that rejected the push. Nothing from a rejected request is stored.
- `401`: the `X-Scope-OrgID` header names a tenant other than `default`.
- `501`: this process does not run the ingester role; check `-roles`.
- `429` with `ingest: back-pressure`: the ingester is behind (new in 1.2.0).
  The body names the bound, the bytes waiting and the limit, and the
  `Retry-After` header says how many seconds to wait. `... wait in memory for
  a local flush` means writing local Parquet fails or lags: check
  `parkana_ingest_flush_total{result="error"}` and the disk under
  `-data.dir`. `... of local Parquet wait for upload` means uploads fail or
  lag: check `parkana_ingest_upload_total{result="error"}` and the object
  store. Pushes are accepted again as soon as a flush or upload brings the
  bytes under the limit; see [back-pressure](ingest.md#back-pressure), which
  also lists how long common clients retry a `429` before they drop data.
- `503` with `ingest: not accepting pushes`: the ingester is replaying its WAL
  (wait for `ingest service ready`), shutting down, or stopped after a fatal
  error (look for `service failed`). Clients should retry.
- `500` with `ingest: failed to persist push`: the WAL or the memtable could
  not record the request. Check `parkana_ingest_wal_errors_total` and the free
  space under `-data.dir`.
- `500` with `ingest: push transform failed`: an ingest pipeline failed for a
  reason other than the data. Check
  `parkana_pipeline_executions_total{stage="ingest",result="error"}`.

### Data is not visible yet

Queries read only objects that are uploaded and cataloged; live tail is the
only path that shows lines straight after the push. A partition is uploaded
once its local files reach `-ingest.upload-min` (128 MiB) or its oldest file
reaches `-ingest.upload-max-age` (10 minutes), so with little traffic new lines
take up to about ten minutes to appear. Lower `-ingest.upload-max-age` for
fresher results (see [Configuration](configuration.md#ingest-speed-memory-and-visibility)
and [LogQL](query/logql.md#upload-and-catalog-visibility)).

If lines stay invisible past that, check that `parkana_ingest_upload_rows_total`
grows, that `parkana_ingest_upload_total{result="error"}` does not, and look
for `ingest: upload pass; will retry` in the log. With separate processes,
check that the querier opens the same catalog and object store as the
ingester.

### Data disappeared

Three things delete data:

- Retention: `-compactor.retention` deletes whole files whose newest entry is
  older than the retention. The `compactor cleanup` log line counts them as
  `marked`, then as `purged` once `-compactor.delete-delay` has passed.
- Merges: the compactor replaces small files with merged ones and deletes the
  replaced objects after `-compactor.delete-delay`. No rows are lost.
- Orphan collection, below. A `compactor cleanup` line with `orphaned` above
  zero means objects were deleted because no catalog row referenced them. An
  `orphan collection skipped` line means the safety stop kept them; see
  [Orphan collection skipped](#orphan-collection-skipped).

#### Orphan objects are deleted

> [!WARNING]
> On every cycle (`-compactor.interval`, default 1 minute) the compactor lists
> the objects under the tenant's `{tenant}/logs/` prefix and deletes each one
> that no catalog row references (deleted rows included) and whose
> modification time in the store is older than `-compactor.orphan-grace`
> (default `1h`). It trusts its catalog, except that a safety stop skips the
> sweep when 10 or more objects are candidates and they are more than
> `-compactor.orphan-max-fraction` (default `0.1`) of the listed objects. Data
> is lost when the catalog does not list objects that hold data and they stay
> under that fraction:
>
> - a catalog restored from a backup older than the objects in the store;
> - two deployments with separate catalogs (two SQLite files, two Postgres
>   schemas) that share one bucket or one `-storage.fs.dir`: each compactor
>   deletes the other's objects;
> - a process that starts with a wrong `-data.dir` or `-catalog.dsn` and so
>   with a new, empty catalog against an existing store that holds fewer than
>   10 old objects.
>
> New in 1.2.0, a memory catalog (`-catalog.driver=memory`) turns orphan
> collection off, so its objects stay in the store after a restart, though no
> query finds them. Use the memory catalog only with an object store you can
> lose, and give every deployment its own bucket or prefix directory. See
> [Catalog](catalog.md) and [Compactor](compactor.md).

#### Orphan collection skipped

New in 1.2.0. The log line `orphan collection skipped` (error level) and
`parkana_compactor_orphan_sweeps_skipped_total` mean the compactor found at
least 10 unreferenced objects older than the grace, more than
`-compactor.orphan-max-fraction` of every object it listed, and deleted none of
them. It tries again, and skips again, on every cycle until the cause is
fixed. The line carries `candidates`, `listed`, `referenced` (listed objects
that a catalog row protects) and `sample`, up to 10 candidate keys.

A `referenced` count near zero next to a large `listed` count almost always
means the compactor opened the wrong catalog. Do not turn the stop off before
you know why:

1. Check which catalog the compactor process uses: `-catalog.driver`,
   `-catalog.dsn` and `-data.dir` on its command line or in its environment
   (`/etc/parkana/parkana.env` for the packages). The SQLite default is
   `<data.dir>/catalog.db`.
2. Count the catalog's files and compare with the store:
   `sqlite3 /var/lib/parkana/catalog.db "SELECT count(*) FROM files WHERE tenant = 'default'"`,
   or the same query through `psql` for Postgres. Each file has a data object
   and usually a `.idx` sidecar, so `listed` should be close to twice that
   count.
3. Look up a few `sample` keys in the catalog. A key that holds logs you
   expect to keep means the catalog is wrong: stop the compactor, point it at
   the right catalog (or restore the catalog and the objects together, see
   [Backup and restore](#backup-and-restore)) and start it again.

When the candidates really are garbage, for example after you discarded a
catalog on purpose, remove them in one of two ways:

- Start the compactor once with `-compactor.orphan-max-fraction=1`
  (`PARKANA_COMPACTOR_ORPHAN_MAX_FRACTION=1`), wait for the `compactor cleanup`
  line with `orphaned`, then remove the setting and restart it.
- Delete the candidates by hand, as listed below.

To list every candidate on the fs backend, take the objects older than the
grace, take every key the catalog lists together with its sidecar, and keep
the objects missing from the second list:

```sh
cd /var/lib/parkana/objects
find default/logs -type f -mmin +60 | sort > /tmp/objects.txt
sqlite3 /var/lib/parkana/catalog.db "SELECT key FROM files WHERE tenant = 'default'" \
  | sed -e p -e 's/\.parquet$/.idx/' | sort -u > /tmp/referenced.txt
comm -23 /tmp/objects.txt /tmp/referenced.txt > /tmp/candidates.txt
wc -l /tmp/objects.txt /tmp/referenced.txt /tmp/candidates.txt
```

`-mmin +60` matches the default `1h` grace; use the grace you configured. For
Postgres, replace the `sqlite3` command with
`psql "$PARKANA_CATALOG_DSN" -Atc "SELECT key FROM files WHERE tenant = 'default'"`.
On S3, list the keys under `default/logs/` older than the grace, for example
with
`aws s3api list-objects-v2 --endpoint-url https://s3.example.com --bucket parkana --prefix default/logs/ --query "Contents[?LastModified<'2026-01-01T12:00:00Z'].Key" --output text | tr '\t' '\n' | sort`
with the cutoff set to now minus the grace, and use that list in place of
`/tmp/objects.txt`. The catalog includes rows marked deleted on purpose: their
objects wait for `-compactor.delete-delay` and are not orphans.

If data is missing without any of these, check the query: the time range
(`From <= ts < Through`), the stream selector, and the `X-Scope-OrgID` header.

### Queries are slow or time out

A LogQL or metadata request answers `504` after `-querier.timeout` (2 minutes)
and `499` when the client gave up first. To find out why a query is slow:

- `parkana_query_files_total` by `result` shows how many files were scanned
  and how many were pruned by search sidecars (`pruned_index`) or label
  summaries (`pruned_labels`). Many scanned files means the query has little
  to prune with: add stream label matchers, narrow the time range, and use
  `|=` line filters, which the sidecars can prune.
- `parkana_querier_cache_requests_total{result="miss"}` and
  `parkana_querier_cache_evictions_total` growing together mean the hot data
  does not fit the cache: raise `-querier.cache-size`.
- Many small files slow every query. `parkana_compactor_files_merged_total`
  should grow while the ingester uploads.
- A tenant with any read pipeline loses filter and limit pushdown for log
  queries (see [read semantics](pipelines/read.md#cost)).

For `POST /api/v1/sql`, `parkana_sql_requests_total{outcome="deadline"}` counts
requests that hit `-sql.timeout`, and `parkana_sql_inflight` at
`-sql.max-concurrent` means requests wait for a slot, which counts against
their deadline.

### The disk fills up

`-data.dir` holds the WAL, local Parquet waiting for upload, the query cache
(`cache/`, bounded by `-querier.cache-size`, 10 GiB by default), the SQLite
catalog and, with `-storage.backend=fs`, the objects themselves. When it grows:

- Failing uploads keep local Parquet and the WAL from being reclaimed. Check
  `parkana_ingest_upload_total{result="error"}` and the `ingest: upload pass;
  will retry` and `ingest: wal reclaim; will retry` log lines.
- Quarantined local files (see [crash recovery](ingest.md#crash-recovery))
  stay for `-ingest.quarantine-retention` (168h by default; 0 keeps them
  forever). `parkana_ingest_quarantine_files` and
  `parkana_ingest_quarantine_bytes` show what each tenant's
  `<data.dir>/parquet/<tenant>/.quarantine/` holds; lower the retention or
  delete the files by hand when they take too much space.
- With the fs backend and no `-compactor.retention`, the objects grow forever.
- The cache uses up to `-querier.cache-size` (see `parkana_querier_cache_bytes`);
  lower it, or move it with `-querier.cache-dir`, when the disk is small.

### The Grafana datasource test fails

Grafana's Loki datasource tests the connection with the instant query
`vector(1)+vector(1)`, which Parkana answers. When the test fails, check:

- The datasource URL is the server root, such as `http://parkana.example.com:3100`,
  without `/loki/api/v1`.
- The process behind the URL runs the querier role; without it the query
  route answers `501`.
- No custom `X-Scope-OrgID` header is set in the datasource, or it is
  `default`; any other value answers `401`.
- `parkana_http_requests_total{route="GET /loki/api/v1/query"}` counts the
  test request; if it does not grow, the request never reached Parkana.

### Live tail disconnects

The querier closes a tail WebSocket with code `1011` and a reason when an
ingester fails or drops entries for a slow viewer, when the ingesters do not
acknowledge the subscription within 10 seconds, or when an entry does not fit
in a 4 MiB frame; it closes with `1001` on shutdown. The log line
`live tail session ended with an error` has the reason. Then check:

- `parkana_ingest_tail_evictions_total` and `parkana_tail_dropped_entries_total`
  growing: the viewer cannot keep up. Raise `-tail.buffer-entries`, or narrow
  the query.
- A `503` before the upgrade: a querier-only process has no
  `-tail.ingester-urls`.
- A proxy in front of Parkana must pass WebSocket upgrades. The server pings
  every second and drops a client that does not answer within 5 seconds.
- Restarting an ingester ends the tails subscribed to it. Live tail does not
  backfill: reopen the tail, and use a range query for what was missed.
