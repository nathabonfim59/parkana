# Catalog

The catalog is Parkana's metadata store: the list of Parquet files every query
prunes against, and the pipeline definitions every role reloads. The object
store holds the data; the catalog says which objects are live, what time range
and label values each one covers and whether it has a search sidecar. Three
drivers implement it, chosen with
[`-catalog.driver`](configuration.md#storage-and-catalog): `memory`, `sqlite`
(the default) and `postgres` (new in 1.2.0). They pass the same conformance
suite, so they behave the same way.

> [!WARNING]
> The catalog and the object store must always belong together. The compactor
> deletes every object older than `-compactor.orphan-grace` (default `1h`) that
> its catalog does not list (see
> [orphan collection](compactor.md#orphan-collection)). A deleted or replaced
> SQLite file, an older backup restored over newer objects, or two processes
> with separate catalogs sharing one bucket all make the compactor delete
> stored logs. A safety stop skips the sweep when the unlisted objects are
> more than a tenth of the store, but a catalog that misses fewer objects than
> that still loses them. Every process that uses one object store must point
> at the same catalog.

## What the catalog stores

The `files` table has one row per Parquet object:

| column | holds |
|---|---|
| `tenant`, `key` | the tenant and the object key (the primary key) |
| `stream` | the stream the file belongs to |
| `min_ts`, `max_ts` | the oldest and newest row, in Unix microseconds |
| `rows`, `bytes` | row count and object size |
| `label_values` | per promoted label, the distinct values in the file (JSON) |
| `labels_complete` | whether `label_values` is complete enough to prune by |
| `has_sidecar` | whether the search sidecar object exists |
| `created_at` | when the file was added |
| `deleted_at` | when a merge or retention replaced it; empty while live |

A deleted row stays in the catalog for
[`-compactor.delete-delay`](configuration.md#compactor) so queries that
already listed it can finish; the compactor then purges the row and deletes
the objects (see [compactor](compactor.md#delete-delay)).

`label_values` is complete when, for every promoted label, it lists every
distinct value of the column, or of the `labels` map entry where the column is
empty, including the empty value. Queries prune a file by its label values
only when `labels_complete` is set; see
[query](query/index.md#how-a-query-finds-its-files).

The `pipelines` table holds the ingest and read pipeline definitions managed
over `/api/v1/pipelines`, with a version per definition and a change token
every role polls (see [pipelines](pipelines/index.md#reloading)). The `schema_migrations`
table records which migrations ran.

## Drivers

### Memory

`-catalog.driver=memory` keeps the catalog in the process. It is lost when the
process stops, and no other process can see it. Use it for tests and short
experiments only.

> [!WARNING]
> After a restart a memory catalog is empty, so queries find nothing: the
> objects written before the restart stay in the store, but no catalog lists
> them again. New in 1.2.0, the compactor never runs orphan collection with
> this driver and logs `orphan collection disabled` at startup; 1.0.0 deleted
> every object older than one hour on the first cycle after a restart. Do not
> run the memory driver against an object store whose data you want to keep,
> and never with `-roles` split across processes.

### SQLite

`-catalog.driver=sqlite` (the default) stores the catalog in one file,
[`-catalog.dsn`](configuration.md#storage-and-catalog), which defaults to
`<data.dir>/catalog.db`. Parkana creates the file and its parent directory if
they are missing. It uses the pure Go `modernc.org/sqlite` driver, so there is
no system SQLite library to install.

The database runs in WAL journal mode with `synchronous=FULL`, so a committed
change survives a machine crash. You will see `catalog.db-wal` and
`catalog.db-shm` next to the file while Parkana runs. Writes go through one
connection and queue up in the process; reads use a separate pool of up to 8
connections, so a long file listing never blocks a write. A connection waits
up to 10 seconds for a lock.

Several processes on one machine can open the same SQLite file: SQLite locks
it, and a writer waits up to 10 seconds for another process's write (see
[deploy](deploy.md#on-one-machine)). The catalog's code and tests treat SQLite
as the catalog of one process, so the safe choice is one process per file; a
SQLite file cannot be shared between machines. Use Postgres to split roles
across machines.

### Postgres

New in 1.2.0.

`-catalog.driver=postgres` stores the catalog in PostgreSQL, so processes on
several machines share one catalog. `-catalog.dsn` is required and takes a URL
(`postgres://parkana@db.example.com:5432/parkana`) or a key=value string
(`host=db.example.com dbname=parkana user=parkana`), in any form the pgx driver
accepts, including `PGPASSWORD` and a password file. Put a DSN that contains a
password in `PARKANA_CATALOG_DSN` rather than on the command line, where
process listings show it. Configuration errors, startup output and catalog
errors never show the `password` or `sslpassword` of the DSN.

Each process opens a pool of up to 16 connections (4 idle, closed after 5
minutes idle and recycled after 30 minutes). A write transaction that fails on
a serialization failure or a deadlock is retried up to three times. Canceling
a request also cancels its statement on the server. No extension is needed;
the project tests it with PostgreSQL 16.

Parkana creates its tables in the current schema of the connection (the first
schema in `search_path` that exists). It never creates the schema itself and
stops with `migrate: no current schema` when none exists, so the user needs
`USAGE` and `CREATE` on an existing schema.

#### PgBouncer

Parkana has no PgBouncer setting, and its tests connect to PostgreSQL
directly, so running behind PgBouncer is untested. What the code does that
matters for a pooler:

- The catalog's advisory locks are transaction-scoped
  (`pg_advisory_xact_lock`), and the code sets no session state, uses no
  `LISTEN` and prepares no named statements itself.
- The DSN goes to the pgx driver unchanged, so pgx's own DSN parameters, such
  as `default_query_exec_mode`, apply. pgx caches prepared statements per
  server connection unless that parameter says otherwise, which matters for
  transaction pooling.

Session pooling keeps one server connection per client connection and needs
no change to the DSN.

## Migrations

Both SQL drivers bring their migrations inside the binary and apply them at
startup, before the process serves any request. Migrations are forward only:
there are no down migrations.

An upgrade applies the migrations the catalog has not recorded yet. A
downgrade is refused (new in 1.2.0): when `schema_migrations` records a version
above the highest migration the binary embeds, the process stops at startup
with `schema version N is newer than this binary supports (M)` and writes
nothing to the catalog. Run the newer Parkana again, or restore a catalog
backup taken before the upgrade (see [Backup and restore](#backup-and-restore)).
Each driver numbers its own migrations, so N and M are SQLite or Postgres
versions. Parkana 1.0.0 does not make this check: do not start it against a
catalog a newer binary has migrated.

- SQLite applies each pending migration in its own transaction and records it
  in `schema_migrations`.
- Postgres applies all pending migrations in one transaction under an advisory
  lock keyed by the schema name, so several processes can start at the same
  time and only one of them migrates.

Every schema change is additive: new columns and tables only, never a rename,
type change or drop, so rows written by older versions stay valid.

## Sharing one catalog

Roles share nothing in memory; they meet only in the catalog, the object store
and over HTTP. That lets you split `-roles` across processes, but only if
every process opens the same catalog and the same object store:

| driver | shared by | notes |
|---|---|---|
| `memory` | nothing | lives in one process, empty after a restart |
| `sqlite` | processes on one machine | one file on local disk; the code and tests treat it as the catalog of one process |
| `postgres` | processes on any number of machines | new in 1.2.0 |

Two processes that share a bucket but each open their own catalog, for
example an ingester and a querier plus compactor on two machines that each use
a local SQLite file, do not see each other's files, and the compactor deletes
the files it does not know after an hour.

## Clocks

The catalog takes every timestamp from the clock of the Parkana process that
writes the row, never from the database server. The ingester stamps
`created_at` when it adds a file; the compactor stamps `deleted_at` when it
replaces or expires files and computes the purge cutoff from its own clock, so
the delete delay does not depend on clock agreement between machines.

One comparison crosses machines: orphan collection compares an object's
modification time, as the object store reports it, with the compactor's clock
minus [`-compactor.orphan-grace`](configuration.md#compactor) (default `1h`).
An ingester adds a file to the catalog at most 30 minutes after its upload, so
if the compactor's clock runs more than about 30 minutes ahead of the object
store's, it can delete an upload before the ingester has added it to the
catalog. Keep every Parkana host and the object store on NTP, and raise the
grace when you cannot.

## Backup and restore

Parkana has no backup command of its own. The catalog and the object store are
one dataset: a catalog backup is only useful together with the objects it
lists, and objects are only reachable through a catalog that lists them.

> [!WARNING]
> Never restore an older catalog against the live object store. The compactor
> deletes every object newer than the backup within an hour, because the
> restored catalog does not list them, and the backup lists objects that
> compaction may already have deleted.

To take a consistent backup:

1. Stop the compactor role, or the whole process when you run `-roles=all`.
   While it is stopped, nothing deletes objects: ingesters only add new ones.
2. Back up the catalog.
3. Copy the objects (`<data.dir>/objects` or the bucket). Every object the
   catalog backup lists is in the copy; objects added after step 2 are copied
   too but not listed, and the compactor removes them as orphans after a
   restore.
4. Start the compactor again.

For SQLite, use SQLite's online backup, which is safe while Parkana runs:

```sh
sqlite3 /var/lib/parkana/catalog.db ".backup '/backup/parkana/catalog.db'"
```

Copying `catalog.db` with `cp` while Parkana runs can miss changes still in
`catalog.db-wal`. For Postgres, `pg_dump` of the database (or the schema
Parkana uses) gives a consistent snapshot of the catalog.

To restore, stop every Parkana process that uses the catalog or the store,
put back the catalog and the objects from the same backup, and start Parkana
again. Pipeline definitions live in the catalog, so a restore also brings back
the pipelines as they were at backup time. Logs ingested after the backup are
lost unless you keep the newer objects and the newer catalog.

The ingester's `-data.dir` also holds data that is acknowledged but not yet
uploaded (the WAL and local Parquet, see [ingest](ingest.md#crash-recovery)).
Keep it on a persistent disk; it is not part of a catalog backup.
