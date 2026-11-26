# AGENTS.md for internal/catalog

Read the root [AGENTS.md](../../AGENTS.md) first. This file adds the rules for
the catalog package.

## Every schema change migrates both SQLite and Postgres

The SQLite and Postgres catalogs share one schema. Every schema change ALWAYS
adds a new migration to both directories in the same change:

- `sqlite_migrations/NNNN_<description>.sql`
- `postgres_migrations/NNNN_<description>.sql`

A change with a migration for only one backend is incomplete. Never edit,
rename or delete a merged migration, because installed catalogs have already
applied it. Add a new one instead.

- **Each backend numbers its own migrations.** Postgres `0001_init.sql` holds
  what SQLite spreads over `0001` to `0006`, so the numbers differ. Take the
  next free number in each directory and don't try to align them.
- **SQLite takes one statement per file.** The driver runs only the first
  statement of a string, so the loader rejects a file with more. Split a
  change into several files when it needs more than one statement.
- **Postgres files may hold several statements.** Text columns that order or
  identify rows use `COLLATE "C"`. Timestamps are `BIGINT` Unix microseconds,
  as in SQLite, never `timestamptz`. Flags are `BOOLEAN`.
- **Changes are additive only**, as the root AGENTS.md says. Add columns or
  tables and never rename, retype or drop them. A new column needs a default
  or must allow NULL, so existing rows stay valid.
- **Both backends keep the same behavior.** After the change, both must pass
  `catalogtest.Run`. Add a `catalogtest` case for the new behavior, update
  `SQLite`, `Postgres` and `Memory`, and add an upgrade test that opens a
  catalog created before the change and checks its existing rows (see
  `sqlite_migration_test.go` and `postgres_migration_test.go`).
- **Postgres tests need a database.** They skip unless
  `PARKANA_TEST_POSTGRES_DSN` is set, so a local `make check` passes even when
  a Postgres migration is broken. Run `make check` on a machine with
  PostgreSQL and `PARKANA_TEST_POSTGRES_DSN` set, and check in `go test -v`
  output that the Postgres tests ran instead of skipping.
