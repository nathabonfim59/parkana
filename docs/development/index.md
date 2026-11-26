# Development

This page is for people who change Parkana: the build and check targets, the
tests and the services they need, the documentation workflow, and what to
update when you add a migration, a route, a flag, a metric or a page. The
coding rules are in [`AGENTS.md`](../../AGENTS.md); the design is in
[`PLAN.md`](../../PLAN.md).

## Toolchain

Parkana needs the Go version in [`go.mod`](../../go.mod) and a C toolchain:
the querier links DuckDB through cgo (`github.com/duckdb/duckdb-go/v2`, with
prebuilt static libraries). The first build of a package that imports DuckDB
takes a while; later builds are cached. staticcheck runs as a module tool
(`go tool staticcheck`), so there is nothing else to install. The
documentation site needs Node.js 24 and npm.

## Make targets

| target | does |
|---|---|
| `make build` | builds `bin/parkana` with the version, revision and branch stamped in, the C++ runtime linked statically, and the pure-Go resolver |
| `make run` | builds, then runs `bin/parkana` with `./data` as the data directory and `./data/objects` as the object store |
| `make test` | `go test ./...`, without the race detector |
| `make race` | `go test -race ./...` |
| `make lint` | `go vet ./...` and `go tool staticcheck ./...` |
| `make fmt` | rewrites every Go file with the toolchain's gofmt |
| `make check` | `fmt-check`, `tidy-check`, `lint`, `packaging-test` and `race`: what CI runs and what must pass before a change is done |
| `make clean` | removes `bin/` and `data/` |
| `make packaging-test` | runs the deb and rpm maintainer scripts in `packaging/scripts` in dpkg's and rpm's order for every install, upgrade and remove, against a stub `systemctl` (POSIX sh, no root) |
| `make packages` | builds the deb and rpm packages from `bin/parkana` into `dist/` with a pinned nfpm (`go run`, not a module dependency); run `make build` first |
| `make tarball` | packs `bin/parkana`, the README, changelog, license and configuration guide into `dist/parkana-<VERSION>-linux-amd64.tar.gz`, deterministically; run `make build` first |
| `make plugin-dist` | builds the [Grafana SQL plugin](../grafana.md#building) zip `dist/parkana-sql-datasource-<version>.zip` and adds its line to `dist/SHA256SUMS` |
| `make plugin-check` | the plugin backend's `go vet` and `go test` |
| `make dist-sums` | rewrites `dist/SHA256SUMS` over every file in `dist/` |
| `make docs` | `npm ci`, then `npm run docs:build` |
| `make docs-check` | the Go tests that keep the pages in sync with the code (`./internal/config/`, `./internal/doccheck/`, `./cmd/parkana/`), then `npm run docs:build` |
| `make docs-flags` | rewrites the flag tables in [Configuration](../configuration.md) from the flag set |
| `make docs-metrics` | rewrites the metrics table in [Operations](../operations.md) from the registered collectors |
| `make docs-redaction` | rewrites the pattern catalog in [Redaction](../pipelines/redaction.md) from `skills/parkana-redaction/patterns.json` |

`make all` is `check` followed by `build`.

## Release builds

A release is built from a `v*` tag in the `golang:1.27.3-bookworm` image,
pinned by digest (the image the [Dockerfile](../../Dockerfile) uses). The
binary needs glibc 2.34, which that image's glibc 2.36 satisfies, so it runs
on current LTS distributions. The
[release workflow](../../.github/workflows/release.yml) runs on every pushed
`v*` tag, or by hand with a tag name, and does the same as the commands
below. It attaches the files to a draft GitHub release, which it creates or,
while it is still a draft, updates. It never changes a published release;
publishing the draft is a manual step.

To build the same files on a workstation, with Podman (Docker works the same
way):

```sh
tag=v1.2.0
git checkout "$tag"
podman run --rm -v "$PWD":/src -w /src \
  golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 \
  sh -c "git config --global --add safe.directory /src &&
    make build VERSION=$tag REVISION=\$(git rev-parse HEAD) BRANCH=main &&
    make packages VERSION=$tag &&
    make tarball VERSION=$tag"
make plugin-dist VERSION=$tag CONTAINER=podman
make dist-sums
```

`dist/` then holds the deb, the rpm, the tarball, the plugin zip and
`SHA256SUMS`, the files listed in [Deploy](../deploy.md#release-files). The
tarball and the plugin zip are deterministic: sorted entries, the commit
time as every mtime and, in the tarball, owner 0, so two builds of one commit give the same
files.

## Tests

Tests use the standard `testing` package, are table-driven and run clean under
`-race`. Run one package or one test while you work:

```sh
go test ./internal/objstore/ -run TestFS -v
```

The full race suite with the cgo DuckDB code takes several minutes of CPU,
so run `make check` on a machine with a few cores to spare before you send a
change.

### Integration tests

Tests that need a real service skip unless the environment names it, so
`make check` passes without any of them. CI sets none, so these tests do not
run there.

| variable | enables |
|---|---|
| `PARKANA_TEST_POSTGRES_DSN` | the Postgres catalog tests in `internal/catalog` (the `catalogtest` suite, migrations, concurrency, cancellation, errors) and the Postgres cases in `internal/app`, `internal/compactor`, `internal/ingest`, `internal/pipeline`, `internal/query` and `cmd/parkana`. It is the DSN, URL or key=value, of a database the tests may create schemas in: each test creates its own schema with a random name and drops it at the end. `scripts/e2e-postgres.sh` takes the same variable in URL form. |
| `PARKANA_TEST_S3_ENDPOINT`, `PARKANA_TEST_S3_BUCKET`, `PARKANA_TEST_S3_ACCESS_KEY`, `PARKANA_TEST_S3_SECRET_KEY` | the `objstoretest` conformance suite against a real S3-compatible store (`TestS3Integration` in `internal/objstore`). The bucket must exist. `PARKANA_TEST_S3_REGION` defaults to `us-east-1`; `PARKANA_TEST_S3_INSECURE=true` uses plain HTTP. |
| `PARKANA_TEST_INGEST_BINARY` | the end-to-end ingest suite in `internal/ingest`, which runs and kills a compiled `parkana` binary named by the variable |
| `PARKANA_TEST_PARALLEL_CRASH=1` | the crash matrix in `internal/ingest` that forks test binaries and SIGKILLs them during parallel flush and upload |
| `PARKANA_TEST_SQL_PARSE_MEASUREMENT=1` | a measurement in `internal/query` of how long SQL validation takes to abort under short deadlines; it asserts nothing beyond both paths returning |

A Postgres test that skips looks like a pass. When you change the Postgres
catalog, run the tests with the variable set and check in the `go test -v`
output that they ran:

```sh
PARKANA_TEST_POSTGRES_DSN='postgres://parkana@localhost:5432/testdb?sslmode=disable' \
  go test -v ./internal/catalog/ | grep -c '^=== RUN   TestPostgres'
```

### End-to-end scripts

Every script in [`scripts/`](../../scripts/) but `e2e-postgres.sh` starts the
Docker Compose development stack in [`deploy/compose/`](../../deploy/compose/)
(Garage, Parkana, Loki, Alloy and Grafana, as each check needs) in its own
Compose project and removes it afterwards. They need Docker with the Compose
v2 plugin, Go and curl, and have `--self-test` (offline checks) and
`--dry-run` modes. `scripts/e2e-postgres.sh` runs the binary on the host
against a Postgres server and needs `psql`. Each script documents its options
and prerequisites in its header.

| script | checks |
|---|---|
| `scripts/e2e-phase1.sh` | push, upload, compaction and SIGKILL recovery against Garage; runbook in [`deploy/compose/e2e.md`](../../deploy/compose/e2e.md) |
| `scripts/e2e-phase2.sh` | the Grafana and Loki comparison stack and the LogQL comparison corpus; see [`deploy/compose/phase2.md`](../../deploy/compose/phase2.md) |
| `scripts/e2e-phase4-otlp.sh` | Parkana's OTLP converter against Loki's over the same records; see [OTLP comparison stack](otlp-comparison.md) |
| `scripts/e2e-phase4-sql.sh` | the Grafana SQL datasource plugin against `POST /api/v1/sql` |
| `scripts/e2e-postgres.sh` | Parkana with a Postgres catalog; needs `PARKANA_TEST_POSTGRES_DSN` |
| `scripts/bench-phase3.sh` | the Loki comparison benchmark; see [Benchmarks](../benchmarks.md) |

#### SQL plugin development stack


`deploy/compose/sql.yaml` is an overlay on the phase 2 compose stack: it
mounts the built `dist/` read-only, enables the unsigned plugin, and sets
`GF_PATHS_PROVISIONING=/etc/grafana/provisioning-sql` with its own
provisioning tree (`deploy/compose/grafana-sql/provisioning`) mounted
read-only at that path. The tree holds a byte-identical copy of the base
`loki.yaml` (the Loki and Parkana datasources) plus the SQL datasource file
(uid `parkana-sql`, type `parkana-sql-datasource`,
`url: http://parkana:3100`, `jsonData.tenant: default`). The base
provisioning mount stays untouched and is simply not the directory Grafana
reads with the overlay applied, so a plain phase 2 stack still works as
before:

```sh
cd deploy/compose
docker compose -f compose.yaml -f sql.yaml --profile phase2 \
  up -d garage garage-init parkana grafana
```

The dev stack's anonymous-admin Grafana and unsigned-plugin allowance are
properties of that local setup, not of the endpoint's security model.

#### SQL plugin end-to-end check


`scripts/e2e-phase4-sql.sh` builds the plugin, starts an isolated compose
project (base stack plus the SQL overlay), pushes a deterministic fixture
and asserts through Grafana `/api/ds/query`: health, exact table values,
exact time-series timestamps/points, macro expansion, rejection of a
filesystem escape (`read_csv_auto('/etc/passwd')`) and the tenant boundary.
It owns its compose project's cleanup.

```sh
scripts/e2e-phase4-sql.sh              # full run (needs Docker)
scripts/e2e-phase4-sql.sh --self-test  # offline checks only, no Docker
scripts/e2e-phase4-sql.sh --dry-run    # print the resolved plan
scripts/e2e-phase4-sql.sh --keep       # leave the stack up for debugging
```

Ports, project name and the readiness budget are overridable
(`--parkana-port`, `--garage-port`, `--loki-port`, `--grafana-port`,
`--project`, `--timeout`); see `--help`.

## Documentation

The Markdown files in `docs/` are the single source of the documentation:
GitHub renders them in place and VitePress builds the site from the same
files. The site configuration is in `site/.vitepress/`; the build helpers are
`scripts/doc-links.mjs` (which files are pages, how links resolve, the build
checks) and `scripts/docs-gen.mjs` (the generated references).

```sh
npm ci                # once, installs the pinned packages
npm run docs:dev      # generates the references, serves the site with live reload
npm run docs:build    # generates the references and builds the site with every check
npm run docs:preview  # serves the built site
```

`npm run docs:api` (run by `docs:dev` and `docs:build`) generates two
references into `docs/api-reference/`, which is not committed: the HTTP
reference from [`api/parkana.yaml`](../../api/parkana.yaml), one page per tag,
and the Go reference of every package with gomarkdoc, pinned and run through
`go run`. The first run downloads gomarkdoc.

`npm run docs:build` fails on:

- a link to a page or file that does not exist;
- a `#anchor` link that does not match a heading. A hand-written page must
  link anchors that exist on both GitHub and the site;
- an em dash or an en dash in the prose of a hand-written page (code spans
  and code blocks are exempt);
- a published page that is missing from [`docs/public/llms.txt`](../public/llms.txt),
  or an `llms.txt` link to a page that does not exist.

The Go side runs with `go test`, so `make check` covers it:

| test | fails when |
|---|---|
| `TestFlagReference` (`internal/config`) | the flag tables in [Configuration](../configuration.md) differ from the flag set, or a flag is in no table group |
| `TestMentions`, `TestEnvMentions` (`internal/config`) | a page names a flag or a `PARKANA_*` variable that does not exist |
| `TestMetricsReference` (`cmd/parkana`) | the metrics table in [Operations](../operations.md) differs from the registered collectors, or a page names a `parkana_*` metric that is not registered |
| `TestRoutesMatchSpec`, `TestRoleRoutesInSpec` (`cmd/parkana`) | a registered route is missing from `api/parkana.yaml`, or the spec documents a route no process serves |
| `TestDocComments` (`internal/doccheck`) | a package has no package comment or has one in more than one file, or an exported identifier has no doc comment or one that does not start with its name |

Pages follow the style rules in [`AGENTS.md`](../../AGENTS.md#documentation).

## How to add

### A catalog migration

Every schema change adds a migration to both
`internal/catalog/sqlite_migrations/` and `internal/catalog/postgres_migrations/`
in the same change, each with the next free number in its own directory.
SQLite files hold one statement each; Postgres files may hold several. Never
edit, rename or delete a merged migration, and only add tables and columns.
Update `Memory`, `SQLite` and `Postgres`, add a `catalogtest` case for the new
behavior and an upgrade test that opens a catalog created before the change.
The full rules are in
[`internal/catalog/AGENTS.md`](../../internal/catalog/AGENTS.md).
`catalogtest.Run` and the migration tests enforce it; the Postgres half needs
`PARKANA_TEST_POSTGRES_DSN`.

### An HTTP route

Register the handler on `Deps.HTTP` with a method pattern such as
`"GET /loki/api/v1/label/{name}/values"`; Loki route patterns are constants in
`internal/lokiapi`. Document the operation in `api/parkana.yaml` under one of
the fixed tags (Push, Query, Metadata, Tail, SQL, Pipelines, Operations,
Internal; `scripts/docs-gen.mjs` fails on any other), with its parameters,
bodies, status codes and the shared `LokiError` response for plain-text
errors. `TestRoutesMatchSpec` fails until the route and the spec agree, in
both directions. Add behavior that holds across routes to
[HTTP API behavior](../http-api.md).

### A flag

Add the flag in `internal/config`; its environment variable follows from the
name. Add its name or prefix to `flagGroups` in
`internal/config/docref_test.go`, run `make docs-flags` and commit the
regenerated tables. `TestFlagReference` fails while the tables are stale or
the flag is in no group, and `TestMentions` fails when a page names a flag
that does not exist.

### A metric

Register the collector on `Deps.Metrics` with a `parkana_` name and a help
text that says what one unit counts. Run `make docs-metrics` and commit the
regenerated table in [Operations](../operations.md); add it to the alerting
table there when it signals a fault. `TestMetricsReference` fails while the
table is stale or a page names a metric that is not registered.

### A page

Write the page in `docs/`, as `docs/<name>.md` or, for a section with
subpages, `docs/<name>/index.md`. Link to other pages and repository files
with relative paths, as GitHub resolves them. Add the page to
[`docs/public/llms.txt`](../public/llms.txt) and to the sidebar in
`site/.vitepress/config.mts`. `npm run docs:build` fails until `llms.txt`
lists the page and every link and anchor resolves.
