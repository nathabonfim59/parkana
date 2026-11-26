# Getting started

This page installs Parkana, starts it with the defaults, pushes one log line
and reads it back with curl and with Grafana. It takes about ten minutes.

The current release is 1.2.0. The [changelog](../CHANGELOG.md) lists what it
contains.

## Install

Pick one of the three ways below. All of them give you the same `parkana`
binary.

### Release packages (deb, rpm)

The [1.2.0 release](https://github.com/nathabonfim59/parkana/releases/tag/v1.2.0)
has packages for x86-64 Linux with glibc 2.34 or newer: Debian 12, Ubuntu
22.04 and later, RHEL, Rocky and Alma 9, and Fedora. The packages were tested
on Debian 12, Ubuntu 22.04 and 24.04, Rocky 9 and Fedora 42.

```sh
base=https://github.com/nathabonfim59/parkana/releases/download/v1.2.0
curl -LO $base/parkana_1.2.0-1_amd64.deb      # Debian, Ubuntu
curl -LO $base/parkana-1.2.0-1.x86_64.rpm     # RHEL, Rocky, Alma, Fedora
curl -LO $base/SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS

sudo apt install ./parkana_1.2.0-1_amd64.deb      # Debian, Ubuntu
sudo dnf install ./parkana-1.2.0-1.x86_64.rpm     # RHEL, Rocky, Alma, Fedora
sudoedit /etc/parkana/parkana.env                 # storage, retention, ...
sudo systemctl enable --now parkana
```

The package adds a `parkana` system user, the data directory
`/var/lib/parkana`, a hardened systemd unit and the settings file
`/etc/parkana/parkana.env`. It does not start the service: you start it with
`systemctl enable --now` once the settings are right. The defaults store
everything under `/var/lib/parkana` and listen on port 3100, so you can also
start it unchanged to try it. [Deploy](deploy.md#systemd-package) describes
the package layout in full.

There is no Alpine (musl) package: DuckDB publishes no musl build.
On Alpine, run Parkana in a glibc container.

### Release tarball

The same release has `parkana-v1.2.0-linux-amd64.tar.gz`, which holds the
binary, the README, the changelog, the configuration guide and the license.
It needs the same glibc 2.34 or newer.

```sh
curl -LO https://github.com/nathabonfim59/parkana/releases/download/v1.2.0/parkana-v1.2.0-linux-amd64.tar.gz
tar xzf parkana-v1.2.0-linux-amd64.tar.gz
./parkana-v1.2.0-linux-amd64/parkana -data.dir ./data
```

### Docker image

The project does not publish a container image. Build one from the
repository's `Dockerfile`, which compiles Parkana on Debian bookworm and runs
it as a non-root user on `debian:bookworm-slim`:

```sh
git clone https://github.com/nathabonfim59/parkana
cd parkana
docker build --build-arg PARKANA_VERSION=v1.2.0 -t parkana .
docker run -d --name parkana -p 3100:3100 -v parkana-data:/data \
  -e PARKANA_DATA_DIR=/data parkana
```

`git checkout v1.2.0` before the build gives you the released code; without
it you build main. Volumes, ports and settings for the image are in
[Deploy](deploy.md#docker).

### From source

You need Go 1.27, a C toolchain (`gcc` or `clang`) and `make`. DuckDB is
linked into the binary through cgo from prebuilt static libraries, so the
first build downloads them and takes a few minutes; later builds are cached.

```sh
git clone https://github.com/nathabonfim59/parkana
cd parkana
make build            # writes bin/parkana
```

## First run

```sh
./bin/parkana -data.dir ./data -ingest.upload-max-age 30s
```

Use `parkana` instead of `./bin/parkana` when you installed a package. With
no other flags Parkana runs every role in one process (ingester, querier and
compactor), listens on `:3100`, keeps its objects on local disk under
`-data.dir` and its catalog in a SQLite file there.

Parkana answers queries from uploaded objects, not from the ingester's
memory. By default a file is uploaded once it reaches 128 MiB or is 10 minutes
old (`-ingest.upload-max-age`). The `30s` above makes a test line queryable
within about half a minute; leave the default in production, where it keeps
objects larger.

Check that it is up. `/ready` answers `ready` with status 200 once every role
has started:

```sh
curl -s localhost:3100/ready
```

## Push a line

Parkana takes the Loki push API. This sends one line with the label
`service_name="demo"`, stamped with the current time in nanoseconds:

```sh
now=$(date +%s%N)
curl -s -X POST localhost:3100/loki/api/v1/push \
  -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"service_name":"demo","level":"info"},"values":[["'$now'","hello parkana"]]}]}'
```

An accepted push answers 204 with no body. The line is now in the
write-ahead log on disk. Alloy, Promtail, `logcli` and OpenTelemetry
exporters push the same way; see [Ingest](ingest.md).

## Query it with curl

Wait for the upload, then ask for the stream:

```sh
sleep 45
curl -s -G localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={service_name="demo"}'
```

The answer is a Loki response with the line in it:

```json
{"status":"success","data":{"resultType":"streams","result":[{"stream":{"service_name":"demo","level":"info"},"values":[["1795484805996573000","hello parkana"]]}],"stats":{...}}}
```

Asked before the upload, the same query returns an empty `result`. Parkana
stores time in microseconds, so the last three digits of the timestamp come
back as zeros. Metric queries work too:

```sh
curl -s -G localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query=sum by (level) (count_over_time({service_name="demo"}[5m]))'
```

[LogQL](query/logql.md) lists what Parkana supports and what it rejects with a 400.

## Query it in Grafana

In Grafana, open Connections, add a **Loki** data source and set its URL to
`http://<host>:3100`, where `<host>` is the machine Parkana runs on as seen
from the Grafana server. Leave authentication off and add no headers. Save and
test, then open Explore, pick the data source and run `{service_name="demo"}`.

[Grafana](grafana.md) covers dashboards, live tail, alerting, provisioning
and the SQL data source plugin.

## Where the data lives

Everything Parkana writes in this setup is under `-data.dir`:

| path | holds |
|---|---|
| `wal/default/` | the write-ahead log: every acknowledged push until it is uploaded |
| `parquet/default/` | local Parquet files and the upload manifest, waiting for upload |
| `objects/default/logs/` | the uploaded Parquet objects and their search sidecars (the `fs` object store) |
| `catalog.db` | the SQLite catalog: which objects exist, their time ranges and labels, and pipelines |

`default` is the tenant. Object keys follow
`{tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet`, where the
stream here is `demo` (taken from `service_name`). With S3 storage
(`-storage.backend s3`) the objects go to the bucket instead, and the querier
keeps local copies of the ones it reads under `cache/`. [Storage](storage.md)
and [Catalog](catalog.md) explain both in detail.

To start over, stop Parkana and delete the directory.

## Next steps

- [Configuration](configuration.md): S3 storage, retention, durability,
  memory and every flag.
- [Deploy](deploy.md): the systemd package, Docker, the development stack,
  separate roles and sizing.
- [Security](security.md): Parkana has no authentication or TLS of its own;
  how to restrict who reaches it before you expose the port.
- [Grafana](grafana.md): the Loki data source and the SQL plugin.
- [Ingest](ingest.md) and [OTLP](ingest.md#otlp): sending logs from Alloy, Promtail
  and OpenTelemetry.
- [Pipelines](pipelines/index.md): parse, redact and drop fields at ingest or read
  time.
- [SQL](query/sql.md): read-only DuckDB SQL over the same data.
- [Install with an agent](agent-install.md): let a coding agent do the
  install and the checks.
