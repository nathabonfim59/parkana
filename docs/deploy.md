# Deploy

Parkana is one binary. This page covers the ways to run it: the systemd
package, a container, the development stack, and separate processes per role.
It ends with sizing figures from the benchmarks. Every setting named here is
in [Configuration](configuration.md), with its environment variable.
Parkana has no authentication or TLS of its own: read [Security](security.md)
before you open its port to other hosts.

## Release files

Each [GitHub release](https://github.com/nathabonfim59/parkana/releases)
holds these files, `<v>` being the version:

| file | what |
|---|---|
| `parkana_<v>-1_amd64.deb` | the [systemd package](#systemd-package) for Debian and Ubuntu |
| `parkana-<v>-1.x86_64.rpm` | the same package for RHEL, Rocky and Fedora |
| `parkana-v<v>-linux-amd64.tar.gz` | the binary with the README, changelog, license and configuration guide, in one directory of the same name |
| `parkana-sql-datasource-<v>.zip` | the [Grafana SQL data source plugin](grafana.md#install), new in 1.2.0 |
| `SHA256SUMS` | the SHA-256 of every file above, for `sha256sum --ignore-missing -c SHA256SUMS` |

The binary, packages and tarball are built for linux/amd64 on glibc 2.36
(the `golang:1.27.3-bookworm` image) and need glibc 2.34 or newer. The
[release build](development/index.md#release-builds) is the same in CI
and on a workstation.

## Systemd package

The deb and rpm packages install Parkana as a systemd service.
[Getting started](getting-started.md#release-packages-deb-rpm) has the
install commands.

### What the package installs

| path | what |
|---|---|
| `/usr/bin/parkana` | the binary |
| `/etc/parkana/parkana.env` | settings, read by the unit; owned `root:parkana`, mode 0640 |
| `/usr/lib/systemd/system/parkana.service` | the unit |
| `/var/lib/parkana` | the data directory; owned `parkana:parkana`, mode 0750 |
| `/usr/share/doc/parkana/` | README, changelog, configuration guide and license |

Before it installs files, the package creates the `parkana` system group and
user if they are missing: no login shell, home `/var/lib/parkana`. After it
installs them, it sets the owners and modes above and reloads systemd. It
does not enable or start the service.

The deb depends on `libc6 (>= 2.34)`, `libgcc-s1` and `adduser`; the rpm on
`glibc >= 2.34`, `libgcc` and `shadow-utils`. The binary links the C++ runtime
statically, so glibc and `libgcc_s` are its only shared libraries.

### Settings

The unit passes no flags: every setting comes from `/etc/parkana/parkana.env`
as a `PARKANA_*` variable. The file sets three and lists the common others,
commented out:

```sh
PARKANA_DATA_DIR=/var/lib/parkana
PARKANA_HTTP_ADDR=:3100
PARKANA_LOG_FORMAT=json

#PARKANA_STORAGE_BACKEND=s3
#PARKANA_STORAGE_S3_ENDPOINT=s3.example.com
#PARKANA_STORAGE_S3_BUCKET=parkana-logs
#PARKANA_STORAGE_S3_REGION=us-east-1
#PARKANA_STORAGE_S3_ACCESS_KEY=
#PARKANA_STORAGE_S3_SECRET_KEY=

#PARKANA_COMPACTOR_RETENTION=720h
#PARKANA_INGEST_UPLOAD_MAX_AGE=1m

#GOMEMLIMIT=6GiB
```

The file is readable only by root and the `parkana` group, so it can hold the
S3 keys. Restart the service after you edit it:

```sh
sudoedit /etc/parkana/parkana.env
sudo systemctl restart parkana
journalctl -u parkana -f
```

### The unit

The service runs as `parkana:parkana` and restarts 5 seconds after a failure.
On stop it waits up to 2 minutes (`TimeoutStopSec=2min`) so the ingester can
drain, and it allows 65536 open files. The unit is hardened:
`ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `PrivateDevices`,
`ProtectKernelTunables`, `ProtectKernelModules`, `ProtectControlGroups`,
`NoNewPrivileges`, `RestrictSUIDSGID` and `LockPersonality`.

With `ProtectSystem=strict` the whole file system is read-only for the
service except `/var/lib/parkana`. If you move a path outside it, such as
`-querier.cache-dir` or `-storage.fs.dir`, allow that path in a drop-in. The
same drop-in is the place for a memory limit, which the ingester's automatic
upload budget reads (half of the limit; see
[Configuration](configuration.md#ingest-speed-memory-and-visibility)):

```sh
sudo systemctl edit parkana
```

```ini
[Service]
MemoryMax=8G
ReadWritePaths=/srv/parkana-cache
```

### Upgrade and remove

Install the new package over the old one with the same `apt install` or
`dnf install` command. A settings file you edited is kept. When the packaged
default changed as well, dpkg asks which one to keep, and rpm keeps yours and
writes the new default next to it as `parkana.env.rpmnew`. The package
scripts never touch the data in `/var/lib/parkana`.

An upgrade keeps the service as it was (new in 1.2.0). It stays
enabled or disabled, a running service is restarted on the new binary, and a
stopped one stays stopped. Check that it is ready after the upgrade:

```sh
curl -s localhost:3100/ready
```

The 1.0.0 package's remove script stops and disables the service when any
upgrade replaces it. For rpm the newer package records the state before that
script runs and restores it at the end, so an upgrade from 1.0.0 also keeps
the service as it was. For deb that script runs before anything from the new
package, so the state is lost: the upgrade from 1.0.0 prints a notice, and
you enable and start the service again once:

```sh
sudo systemctl enable --now parkana
```

Removing the package stops and disables the service first. It keeps
`/var/lib/parkana` with all its data and the `parkana` user. Delete them by
hand if you no longer need them.

## Docker

The project does not publish an image; build it from the repository's
`Dockerfile` as [Getting started](getting-started.md#docker-image) shows. The
image:

- runs `/usr/local/bin/parkana` as user `parkana`, uid 10001;
- declares the volume `/data` and exposes port 3100;
- has a health check that requests `/ready` every 10 seconds after a 30
  second start period;
- includes `ca-certificates` for TLS to S3 and `curl` for the health check.

Settings are `PARKANA_*` environment variables or flags after the image
name. Set `PARKANA_DATA_DIR=/data` so the WAL, local Parquet and catalog land
on the volume itself. A named volume gets the right owner on first use; a
bind mount must already be owned by uid 10001.

```sh
docker run -d --name parkana \
  -p 3100:3100 \
  -v parkana-data:/data \
  --memory 8g --stop-timeout 120 \
  -e PARKANA_DATA_DIR=/data \
  -e PARKANA_LOG_FORMAT=json \
  -e PARKANA_STORAGE_BACKEND=s3 \
  -e PARKANA_STORAGE_S3_ENDPOINT=s3.example.com \
  -e PARKANA_STORAGE_S3_BUCKET=parkana-logs \
  --env-file parkana-s3.env \
  parkana -compactor.retention 720h
```

`parkana-s3.env` holds `PARKANA_STORAGE_S3_ACCESS_KEY` and
`PARKANA_STORAGE_S3_SECRET_KEY`, so the keys stay out of the command line and
the shell history; `docker inspect` still shows them, as it shows every
environment variable. `--memory` gives the ingester's upload
budget a limit to work from, and `--stop-timeout` gives the ingester the same
2 minutes to drain as the systemd unit; Docker's default is 10 seconds.

## Development stack with compose

`deploy/compose/` runs Parkana next to Loki with the same input, for
development and comparison. It uses throwaway credentials and an anonymous
Grafana admin: do not use it as a production deployment.

```sh
cd deploy/compose
docker compose --profile phase2 up --build
```

A plain `docker compose up --build` starts only Garage and Parkana. The
`phase2` profile adds the rest:

| service | what it is | host port |
|---|---|---|
| `garage` | Garage 2.5.0, the S3-compatible object store | `127.0.0.1:3900` |
| `garage-init` | one-shot job: cluster layout, the development S3 key and the `parkana` bucket | none |
| `parkana` | Parkana built from the `Dockerfile`, all roles, S3 on Garage, SQLite catalog in the `parkana-data` volume, uploads after 30 seconds | `127.0.0.1:3100` |
| `loki` | Loki 3.7.9, filesystem storage, the comparison baseline | `127.0.0.1:3101` |
| `alloy` | Alloy 1.21.3: receives pushes and sends each one to both Loki and Parkana | `127.0.0.1:12345` |
| `grafana` | Grafana 13.3.0 with the Loki data source plugin 13.2.5 and both stores provisioned (`uid` `loki` and `parkana`) | `127.0.0.1:3000` |
| `pushgen-base`, `pushgen-corpus`, `pushgen-job` | finite log generators that push through Alloy: 22,200 lines by default | none |

Every host port is bound to `127.0.0.1` and can be changed with
`PARKANA_HTTP_PORT`, `PARKANA_S3_PORT`, `LOKI_HTTP_PORT`, `ALLOY_HTTP_PORT`
and `GRAFANA_HTTP_PORT`, so several stacks can run side by side.

Overlays add to the base file:

| file | adds |
|---|---|
| `sql.yaml` | the SQL data source plugin in Grafana, built from `grafana/parkana-datasource/` first |
| `postgres.yaml` | a PostgreSQL 17 catalog for Parkana, with `--profile postgres` (new in 1.2.0) |
| `otlp.compose.yaml` | the native OTLP comparison; `scripts/e2e-phase4-otlp.sh` runs it, see [OTLP comparison stack](development/otlp-comparison.md) |
| `bench.yaml`, `ingest-runtime.yaml` | the benchmark stack; `scripts/bench-phase3.sh` runs it, see [Benchmarks](benchmarks.md#reproducing) |

```sh
docker compose -f compose.yaml -f sql.yaml --profile phase2 \
  up -d garage garage-init parkana grafana
docker compose -f compose.yaml -f postgres.yaml --profile postgres up -d
```

Switching the Postgres overlay on or off starts from an empty catalog of the
other kind; run `docker compose down -v` first. The end-to-end checks that use
this stack are described in [e2e.md](../deploy/compose/e2e.md) and
[phase2.md](../deploy/compose/phase2.md).

## Split roles across processes

`-roles` picks what a process runs: `all` (the default) or a list of
`ingester`, `querier` and `compactor`. Roles share nothing in memory and talk
only through the catalog, the object store and HTTP, so each role can run in
its own process. Every process needs the same catalog and object store.

| role | serves | needs |
|---|---|---|
| `ingester` | `POST /loki/api/v1/push`, `POST /v1/logs`, the private tail stream for queriers, `/api/v1/pipelines` | its own `-data.dir` for the WAL and local Parquet |
| `querier` | the Loki read API, live tail, `POST /api/v1/sql`, `/api/v1/pipelines` | `-tail.ingester-urls` listing every ingester; a cache directory when objects are on S3 |
| `compactor` | only `/ready` and `/metrics` | nothing local |

The catalog driver decides how far the processes can spread:

| `-catalog.driver` | `-catalog.dsn` | use |
|---|---|---|
| `sqlite` (default) | a file path, default `<data.dir>/catalog.db` | processes on one machine |
| `postgres` (new in 1.2.0) | a URL or key=value DSN, required | processes on several machines |
| `memory` | ignored | tests only: the catalog is lost on restart and cannot be shared |

A process answers 501 to an endpoint of a role it does not run, so send
pushes to ingesters and point Grafana at queriers. Every process serves
`/ready` and `/metrics`. Run one compactor.

Give each ingester its own `-data.dir` and each querier its own cache
directory. Different roles may share one `-data.dir`. Before it opens
anything, a process locks what each of its roles owns (new in 1.2.0):

| role | lock file | guards |
|---|---|---|
| `ingester` | `<data.dir>/ingester.lock` | the WAL and local Parquet under `-data.dir` |
| `querier` | `<cache dir>.lock`, next to the cache directory (default `<data.dir>/cache.lock`) | the object cache, `-querier.cache-dir` or `<data.dir>/cache` |
| `compactor` | none | nothing local; run one compactor per catalog |

A second ingester on the same `-data.dir`, or a second querier on the same
cache directory (even with another `-data.dir`), does not start. Its error
names the lock file and the pid of the process that holds it, as in
`ingester lock /var/lib/parkana/ingester.lock is held by another parkana process (pid 4242)`.
The lock is released when its process exits, also after `kill -9` or a
crash, so a restart does not need the file removed; Parkana never deletes it.
Parkana 1.0.0 takes no lock: two ingesters on one directory write over each
other's WAL.

The lock is a flock(2) lock, which network file systems such as NFS do not
reliably enforce. Keep `-data.dir` and `-querier.cache-dir` on a local disk.
Do not delete a lock file to start a second process (see
[Troubleshooting](operations.md#a-process-does-not-start-the-lock-is-held)).

### On one machine

The SQLite catalog and the `fs` object store work for several processes on
one machine. The catalog and object paths default to locations under
`-data.dir`, so set them explicitly to the same paths in every process:

```sh
shared="-catalog.dsn /var/lib/parkana/catalog.db -storage.fs.dir /var/lib/parkana/objects"

parkana -roles ingester -http.addr :3100 -data.dir /var/lib/parkana/ingester $shared
parkana -roles querier -http.addr :3101 -data.dir /var/lib/parkana/querier $shared \
  -tail.ingester-urls http://127.0.0.1:3100
parkana -roles compactor -http.addr :3102 -data.dir /var/lib/parkana/compactor $shared
```

### On several machines

New in 1.2.0: this needs the Postgres catalog.

A SQLite file cannot be shared between machines: SQLite's WAL mode needs
every process on the same host, and network file systems are not supported.
With SQLite every role therefore runs on one machine. With the Postgres catalog (new in 1.2.0),
roles can run on separate machines that share a PostgreSQL database and an
S3 bucket. Every process gets the same storage and catalog settings, for
example in an env file:

```sh
PARKANA_STORAGE_BACKEND=s3
PARKANA_STORAGE_S3_ENDPOINT=s3.example.com
PARKANA_STORAGE_S3_BUCKET=parkana-logs
PARKANA_STORAGE_S3_REGION=eu-west-1
PARKANA_STORAGE_S3_ACCESS_KEY=<access key>
PARKANA_STORAGE_S3_SECRET_KEY=<secret key>
PARKANA_CATALOG_DRIVER=postgres
PARKANA_CATALOG_DSN=postgres://parkana:<password>@db.example.com:5432/parkana?sslmode=require
PARKANA_DATA_DIR=/var/lib/parkana
PARKANA_LOG_FORMAT=json
```

and then its role:

| machine | settings |
|---|---|
| ingesters 203.0.113.11 and 203.0.113.12 | `PARKANA_ROLES=ingester` |
| querier 203.0.113.21 | `PARKANA_ROLES=querier`, `PARKANA_TAIL_INGESTER_URLS=http://203.0.113.11:3100,http://203.0.113.12:3100` |
| compactor 203.0.113.31 | `PARKANA_ROLES=compactor` |

Log shippers push to the ingesters, through a load balancer if you have more
than one; any ingester takes any stream. Grafana points at the querier. Each
query runs on one querier: spreading one query across queriers is planned,
not built. Several queriers behind a load balancer each answer whole queries.

Every process stamps catalog changes with its own clock, so keep the machines
in sync with NTP. Parkana creates and migrates its tables at startup, so the
database user needs `USAGE` and `CREATE` on the schema; no extension is
needed, and Parkana is tested with PostgreSQL 16. The
[Postgres catalog](configuration.md#postgres-catalog) section covers
PgBouncer and permissions, and [Catalog](catalog.md) explains what the
catalog stores.

Parkana does not authenticate requests, and the ingesters' tail stream is an
internal endpoint. Let only log shippers and queriers reach the ingesters,
and only Grafana and trusted callers reach the queriers.

## Sizing

The figures below come from the benchmarks, measured on an 8 vCPU VM with
Parkana limited to 4 CPUs and 8 GiB of memory. They are single measurements
on one workload; [Benchmarks](benchmarks.md) has the method and the raw
evidence.

### CPU and memory

In the [4 GB acceptance run](benchmarks.md#final-4-gb-acceptance-and-hot-stream-stress)
(four streams, `-wal.sync=always`, default concurrency) Parkana acknowledged
68.6 MB/s of raw log lines and used 202 CPU seconds in the 58 seconds of
pushing: about 3.5 of its 4 CPUs. Every line was queryable 35.7 seconds after
the last acknowledgment, 42.5 MB/s end to end. The container's memory high
mark reached the 8 GiB limit, page cache included, with no OOM kill; the
automatic upload budget was 4 GiB, half the limit.

With all traffic in one stream, the same size measured 28.4 MB/s from push to
queryable.

Give the ingester a memory limit (a container limit, `MemoryMax=` in a
systemd drop-in, or `GOMEMLIMIT`). Without one the upload budget falls back to
512 MiB. 8 GiB was enough for about 70 MB/s.

### Disk

Object storage: 25 GB of raw lines became 5.13 GB of Parquet, about 20%,
against 5.75 GB for Loki on the same data. Search sidecars added 0.07% on top
([full 25 GB run](benchmarks.md#full-preset-25-gb-corpus-v3-cache-on)).
Retention (`-compactor.retention`) bounds the total.

Local disk under `-data.dir` holds the WAL and the local Parquet of data not
uploaded yet. A partition uploads at 128 MiB or after 10 minutes, so at a
steady rate this stays small, but it grows for as long as the object store is
unreachable: leave room for the outage you want to ride out at your ingest
rate. Use a disk that honors fsync, since every acknowledged push waits for
one by default.

With S3 storage the querier keeps copies of the objects it reads, up to
`-querier.cache-size` (10 GiB by default). Size the cache to the data you
query often; queries over cached files do not touch the object store. With
the `fs` backend the querier reads objects in place and keeps no cache.

### Query speed

On the 25 GB data set, a `|=` search for a rare string over the whole range
took 166 ms at the median against 20.2 s for Loki 3.7.9, and strings that do
not occur came back in under 1 ms, because the search sidecars let Parkana
skip files that cannot match.
