# AGENTS.md

Instructions for coding agents (and humans) working on Parkana. Read this and
[PLAN.md](PLAN.md) before changing code. PLAN.md says *what* we build and why;
this file says *how* we write it.

## What Parkana is

A Go log store with a Loki-compatible HTTP API. Logs land as Parquet files in
object storage (local dir or any S3-compatible store), are queried with DuckDB, and Grafana
reads them through its built-in Loki datasource. Single binary, roles chosen
with `-roles`. Decisions that shape every change (PLAN.md "Decisions"):

1. Loki stays running next to Parkana as the comparison baseline.
2. Single tenant now, multi-tenant-ready: **every path carries a `tenant.ID`**.
3. Single node now, multi-node later: **roles share nothing in-process**.
4. LogQL now, SQL soon: LogQL → internal plan → DuckDB SQL, one executor.

## Commands

```sh
make build        # bin/parkana
make check        # gofmt check, go mod tidy check, go vet, staticcheck, go test -race
make test         # quick tests, no race detector
make run          # build and run with ./data
go test ./internal/objstore/ -run TestFS -v   # one package / test
```

`make check` must pass before you report work as done. Heavy runs (full race
suite once the cgo/DuckDB code lands, benchmarks) go to a Boat VM:
`tm-boat test <worktree> -- make check`.

Toolchain: Go 1.27 (`go.mod`). staticcheck is a module tool (`go tool
staticcheck`), so there is nothing to install. cgo and a C toolchain are
required: `internal/query` links DuckDB (`github.com/duckdb/duckdb-go/v2`,
prebuilt static libs from `go.duckdb.org`). The first build of a package that
imports it takes a while; later builds are cached.

## Layout

| path | owns |
|---|---|
| `cmd/parkana/` | `main`: config → logger → store + catalog → server → roles. `roleFactories` is where a role plugs in. |
| `internal/config/` | flags with `PARKANA_*` env fallback (`-storage.s3.bucket` ⇄ `PARKANA_STORAGE_S3_BUCKET`), `Role`, `ByteSize` |
| `internal/app/` | `Deps`, `Service`, `Factory`, `Run` (service group), `OpenStore`, `OpenCatalog` |
| `internal/server/` | the one HTTP server: tenant middleware, metrics, panic recovery, `/ready`, `/metrics` |
| `internal/lokiapi/` | Loki route patterns, buildinfo, `Error`/`WriteJSON`, 501 placeholders |
| `internal/tenant/` | `tenant.ID`, `X-Scope-OrgID` resolution, context helpers |
| `internal/logs/` | in-memory model: `Labels` (sorted), `Entry`, `Stream`, `PushRequest`, stream naming |
| `internal/schema/` | Parquet/Arrow column layout: `_ts`, `line`, `labels`, `meta`, promoted labels |
| `internal/objstore/` | `Store` interface, `FS` impl, key layout (`DataKey`, `SidecarKey`), `objstoretest` suite |
| `internal/catalog/` | `Catalog` interface (file list), `Memory` + `SQLite` impls, `catalogtest` suite |
| `internal/wal/`, `internal/ingest/` | ingester: push and OTLP handlers, WAL (sync modes), memtable, local Parquet, bounded parallel flush/upload with memory admission |
| `internal/storage/` | Parquet writer, reader, merge |
| `internal/compactor/` | merge small files, retention, delete replaced objects |
| `internal/plan/` | query contract: `Selection`/`Row`/`Bucket`, `Source` (what executors implement), `Engine`/`Params`/`Result` (what handlers call), `MemSource` reference, `plantest` suite + fixture dataset |
| `internal/logql/` | LogQL parser and evaluator, a `plan.Engine` |
| `internal/query/` | querier role: DuckDB `plan.Source` over catalog files, object cache, Loki read handlers, sandboxed `/api/v1/sql` (decision 16) |
| `internal/tail/` | live tail: querier WebSocket side (the ingester side lives in `internal/ingest`) |
| `internal/index/` | search sidecars: n-gram bloom `Builder`/`Encode`, `Decode`/`Filter.MayContain` (PLAN.md decision 12) |
| `internal/pipeline/` | ingest and read pipelines: step compiler/executor, catalog-backed registry with hot reload, ingest `TransformFunc`, read `plan.Source` wrapper (decisions 14-15) |
| `internal/pipelineapi/` | `/api/v1/pipelines` CRUD and dry-run |
| `grafana/parkana-datasource/` | Grafana SQL datasource plugin, its own Go module + frontend (decision 17) |

Planned packages (PLAN.md §9) go next to these: `views`.

## Contracts you must not bend

- **Tenant everywhere.** Functions that touch data take a `tenant.ID` (or a
  `ctx` that carries one via `tenant.With`/`tenant.From`). WAL paths, object
  keys (`objstore.DataKey`), catalog rows, cache keys: all tenant-scoped. Never
  hardcode `"default"`; use `tenant.Default` or the request's tenant.
- **Roles share nothing in-process.** The ingester and querier talk only
  through `objstore.Store`, `catalog.Catalog` and HTTP. No package-level
  mutable state, no singletons, no "the querier peeks at the ingester's
  memtable" shortcuts. (Live tail will get an explicit interface when it lands.)
  A role is an `app.Factory` that builds an `app.Service` from `app.Deps` and
  registers its routes on `Deps.HTTP`.
- **Interfaces have conformance suites.** A new `objstore.Store` (S3) must pass
  `objstoretest.Run`; a new `catalog.Catalog` (SQLite, Postgres) must pass
  `catalogtest.Run`; a `plan.Source` must pass `plantest.RunSource`, which
  compares it with `plan.MemSource` on a dataset loaded the way the ingester
  stores it. If you need new behavior, add it to the interface *and* the
  suite *and* every implementation in the same change.
- **Timestamps:** `_ts` is Parquet `TIMESTAMP(MICROS, UTC)`; convert with
  `schema.TS` / `schema.Time`. Loki pushes nanoseconds; truncate to µs at
  ingest. DuckDB sessions run with `TimeZone=UTC` (`query.OpenDB`).
- **Query path:** LogQL → `plan.Selection` → DuckDB SQL (decision 4). What
  storage can do (stream matchers, line filters, time range, order, limit,
  count/bytes buckets) is pushed down through `plan.Source`; everything else
  runs in `internal/logql`. The semantics are `plan.MemSource`'s: missing
  label == `""`, anchored RE2 matchers, unanchored RE2 line regexps, byte
  substring `|=` (escape `%`/`_` if you use LIKE), `From <= ts < Through`.
  A label can be a promoted column in one file and a `labels` map entry in
  another file of the same stream.
- **Schema evolution is additive only.** Promoted labels and pipeline fields
  are appended; never rename, retype or drop a column. Names starting with
  `_` and the base column names are reserved (`schema.IsReserved`).
- **Local durability:** anything reported as durable (WAL appends, local
  Parquet, manifests, `objstore.FS` objects) is fsynced *and* so are the
  directory entries that make it reachable: the file's directory and any
  directories created for it. Write to a temp name, fsync, rename, fsync the
  directory. A SIGKILL test is not enough; this covers machine crashes.
- **Search sidecars** never cause a false negative: only `|=` needles prune,
  and an unreadable sidecar means "scan the file". Put the sidecar object
  before the catalog flag (`HasSidecar` / `SetSidecar`). Label pruning
  (`Filter.Match`) trusts only files with `LabelsComplete` summaries.
- **Object keys** come from `objstore.DataKey`/`SidecarKey` only:
  `{tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet`.
- **Stream** means a named dataset, chosen by
  `Labels.StreamName(cfg.StreamLabels)`, not a Loki label set.
- **Loki API compatibility** is judged against real Loki + Grafana. Route
  patterns live in `lokiapi`; errors are Loki-style plain text
  (`lokiapi.Error`); unsupported LogQL returns 400 with a clear message.

## Go style

We write modern Go for the version in `go.mod`. Before editing Go code, load
the **use-modern-go** skill (vendored in `.agents/skills/`, linked from
`.claude/skills/` and `.devin/skills/`; without skill support run
`sh .agents/skills/use-modern-go/scripts/run-tool.sh list --file-path <file>`)
and follow every guideline it returns. The ones that come up most here:

- `encoding/json/v2` for all new JSON (`json.Marshal`, `json.MarshalWrite`,
  `jsontext` for streaming responses). Not `encoding/json`.
- Standard library `uuid` (`uuid.NewV7()` for time-ordered ids). No ULID or
  google/uuid dependency.
- `wg.Go(...)`, `t.Context()`, `b.Loop()`, `for i := range n`, `min`/`max`,
  `slices`/`maps` helpers and iterators (`iter.Seq2` for streaming results,
  as in `Store.List` and `Catalog.ListFiles`), `strings.SplitSeq`,
  `strings.CutLast`, `errors.AsType[T]`, `new(value)`, `cmp.Or`.
- `os.Root` for any filesystem access under a configured directory.
- `http.ServeMux` method patterns (`"GET /loki/api/v1/label/{name}/values"`,
  `r.PathValue("name")`). No router dependency.
- `log/slog` only, structured key/value pairs, logger passed in via `Deps`
  (no global logger). Prometheus metrics registered on `Deps.Metrics`, names
  prefixed `parkana_`.

And the usual rules:

- `context.Context` is the first parameter of anything that does I/O or can
  block; respect cancellation.
- Wrap errors with context (`fmt.Errorf("wal: open %s: %w", path, err)`),
  prefix messages with the package name, compare with `errors.Is`/`errors.AsType`.
  Export sentinel errors when callers need to branch on them.
- No panics on bad input; panics are for programmer errors only.
- Doc comments on every exported identifier; package doc on every package.
- Small interfaces, defined where they're consumed, unless they are one of the
  shared contracts above.
- gofmt'd, `go vet` and staticcheck clean. No `//nolint` without a comment
  saying why.

## Tests

- Table-driven, standard `testing` only (no testify). `t.Context()` for
  contexts, `t.TempDir()` for files.
- Race-clean: `make check` runs `-race`.
- Unit tests next to the code; external-package tests (`package foo_test`)
  when testing the public contract.
- Anything needing a real service (S3/Garage, Grafana, Loki) is an integration test
  behind the env var it needs, e.g. skip unless `PARKANA_TEST_S3_ENDPOINT` is
  set, and documented in the test's doc comment.
- Benchmarks use `b.Loop()` and go in `*_bench_test.go`.

## Dependencies

Stick to the stack in PLAN.md §9. Adding any other module needs a one-line
justification in your report (what it replaces, why the stdlib isn't enough).
Pin versions; run `go mod tidy`. The DuckDB driver is
`github.com/duckdb/duckdb-go/v2` (not the old `marcboeker/go-duckdb`).

## Working in the agent team

Parkana is built by a team of agents in separate tty59 worktrees: an
orchestrator, two leads that plan and review, and runners that implement.

- Stay inside your worktree and your assigned packages. If you need a change in
  a shared contract (`tenant`, `logs`, `schema`, `objstore`, `catalog`,
  `config`, `app`, `server`, `lokiapi`, `cmd/parkana`), ask your lead first.
  The lead coordinates with the other lead through the orchestrator.
- Runners don't commit, merge or push; leads commit on their own branch after
  review; only the orchestrator merges. Never force-push, never rewrite shared
  history. The repository is public on GitHub (github.com/nathabonfim59/parkana).
- Report done only after `make check` passes, with the command output summary
  and the list of files changed.
- Don't edit PLAN.md, README.md or AGENTS.md unless your task says so.
