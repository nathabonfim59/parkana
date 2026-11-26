# Install with an agent

A coding agent can install Parkana
and check that it works. Paste this into it:

```text
Install Parkana for me: a log store with a Loki-compatible API.
Follow https://nathabonfim59.github.io/parkana/agent-install.md step by step.
Ask me before you install packages, run sudo or change system settings.
```

The rest of this page is written for the agent. It is the path a person
follows by hand in [Getting started](getting-started.md) and
[Deploy](deploy.md), in order, with the checks that prove each step worked.
Read it yourself to know what the agent will ask you.

## What you need

- An x86-64 Linux machine with glibc 2.34 or newer: Debian 12, Ubuntu 22.04
  or later, RHEL, Rocky or Alma 9, or Fedora. Alpine (musl) is not
  supported.
- systemd, if Parkana should run as a service from the deb or rpm package.
- Port 3100 free, or another port the user picks.
- For S3 storage: the endpoint, bucket, region and a key pair from the user.
- For a build from source instead of a release: Go 1.27, a C toolchain and
  `make`.

## Rules for the agent

1. Ask the user before every command that installs packages, uses `sudo`,
   writes outside the user's home or the working directory, enables a
   service or opens a port. Say what the command changes. Read-only checks
   need no approval.
2. Never print a secret. S3 keys and database passwords go into
   `/etc/parkana/parkana.env` (mode 0640) or another file only the service
   can read, never on a command line, in a shell history, in a log or in
   your reply. Do not `cat` a file that holds secrets; check it with
   `grep -c` or by the service starting.
3. Use documentation values only as placeholders. Ask the user for real
   hosts, buckets and keys; never guess them.
4. Stop at the first failure and report the command and its output. Do not
   work around a failed check, change unrelated settings or delete data to
   make it pass.
5. Prove the install: `/ready` answers 200, one pushed line comes back from
   a query. Report what you ran and what it returned.

## Steps

### 1. Check the machine

```sh
uname -m                 # must print x86_64
ldd --version | head -1  # glibc 2.34 or newer
systemctl --version | head -1
ss -ltn 'sport = :3100'  # nothing listening on 3100
```

If glibc is older than 2.34 or the machine runs musl, stop and tell the user:
the release binary does not run there. A glibc container
([Deploy](deploy.md#docker)) is the way out.

### 2. Ask the user how to install and store

Ask, and wait for answers:

- Package with a systemd service (needs `sudo`), or the tarball run by hand
  (no root)?
- Local disk (the default, everything under the data directory) or an
  S3-compatible bucket?
- How long to keep logs (`PARKANA_COMPACTOR_RETENTION`, for example `720h`),
  or forever?

The Postgres catalog for several machines needs 1.2.0 or later.

### 3. Download and verify the release

```sh
mkdir -p ~/parkana-install
cd ~/parkana-install
base=https://github.com/nathabonfim59/parkana/releases/download/v1.2.0
curl -fLO $base/SHA256SUMS
curl -fLO $base/parkana_1.2.0-1_amd64.deb             # Debian, Ubuntu
curl -fLO $base/parkana-1.2.0-1.x86_64.rpm            # RHEL, Rocky, Alma, Fedora
curl -fLO $base/parkana-v1.2.0-linux-amd64.tar.gz     # tarball
sha256sum --ignore-missing -c SHA256SUMS
```

Download only the file you need. Every downloaded file must print `OK`;
stop if one does not.

### 4. Install

Package, after the user approves `sudo`:

```sh
sudo apt install ./parkana_1.2.0-1_amd64.deb     # Debian, Ubuntu
sudo dnf install ./parkana-1.2.0-1.x86_64.rpm    # RHEL, Rocky, Alma, Fedora
```

The package creates the `parkana` user, `/var/lib/parkana`,
`/etc/parkana/parkana.env` and the `parkana` unit, and does not start it.

Tarball:

```sh
tar xzf parkana-v1.2.0-linux-amd64.tar.gz
./parkana-v1.2.0-linux-amd64/parkana -help | head -5
```

### 5. Configure

For the package, edit `/etc/parkana/parkana.env` with `sudoedit` (the user
can do this step themselves if they prefer to type the keys). Uncomment and
fill only what the user chose in step 2:

```sh
PARKANA_STORAGE_BACKEND=s3
PARKANA_STORAGE_S3_ENDPOINT=s3.example.com
PARKANA_STORAGE_S3_BUCKET=parkana-logs
PARKANA_STORAGE_S3_REGION=us-east-1
PARKANA_STORAGE_S3_ACCESS_KEY=<from the user>
PARKANA_STORAGE_S3_SECRET_KEY=<from the user>
PARKANA_COMPACTOR_RETENTION=720h
```

Leave the file unchanged for local disk. Every setting is a `PARKANA_*`
variable; [Configuration](configuration.md) lists them all. For the tarball,
pass the same settings as flags or environment variables, keys in the
environment only.

Queries see a line only after it is uploaded, which takes up to
`PARKANA_INGEST_UPLOAD_MAX_AGE` (10 minutes by default). To finish the check
in step 7 within about a minute, ask the user whether to set
`PARKANA_INGEST_UPLOAD_MAX_AGE=1m`, the value the file suggests. Shorter ages
mean more, smaller objects until the compactor merges them.

### 6. Start

Package:

```sh
sudo systemctl enable --now parkana
systemctl status parkana --no-pager
```

Tarball, in the foreground or under the user's own supervisor:

```sh
./parkana-v1.2.0-linux-amd64/parkana -data.dir ~/parkana-data
```

Wait for ready. `/ready` answers 503 while Parkana starts (WAL replay can
take a while after a crash) and 200 with `ready` once every role is up:

```sh
for i in $(seq 1 60); do curl -fs localhost:3100/ready && break; sleep 2; done
```

If it never answers 200, read the log (`journalctl -u parkana -n 50
--no-pager` for the package) and report it. A configuration error names the
flag; Parkana hides passwords in its messages.

### 7. Push one line and read it back

```sh
now=$(date +%s%N)
marker="agent-install-check-$now"
curl -fsS -o /dev/null -w '%{http_code}\n' -X POST localhost:3100/loki/api/v1/push \
  -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"service_name":"agent-install"},"values":[["'$now'","'$marker'"]]}]}'
```

It must print `204`. Then poll until the line is uploaded, for up to the
upload age plus a minute:

```sh
for i in $(seq 1 50); do
  curl -fsS -G localhost:3100/loki/api/v1/query_range \
    --data-urlencode 'query={service_name="agent-install"}' \
    --data-urlencode 'since=1h' | grep -q "$marker" && { echo found; break; }
  sleep 15
done
```

`found` means the push, the WAL, the upload, the catalog and the query path
all work. If the line does not come back, report the push status, the
`/ready` output and the last log lines.

### 8. Connect Grafana (if the user has one)

Ask the user for the Grafana URL and whether you may change it. Add a Loki
data source with URL `http://<parkana-host>:3100`, no authentication and no
headers, or give the user the provisioning file from
[Grafana](grafana.md#provision-it). In Explore, `{service_name="agent-install"}`
shows the check line.

### 9. Report

Tell the user:

- the version (`curl -s localhost:3100/loki/api/v1/status/buildinfo` shows
  `parkanaVersion`), how it was installed and how it runs;
- the HTTP address, the data directory and the storage backend;
- the retention and upload age that are set;
- the outputs of the `/ready` and push/query checks;
- that Parkana does not authenticate requests, so `/api/v1/sql` and
  `/api/v1/pipelines` need a firewall or reverse proxy in front;
- for the package, that an upgrade keeps the service enabled and running
  (new in 1.2.0), except a deb upgrade from 1.0.0, after which
  it must be started again once with `sudo systemctl enable --now parkana`
  ([Deploy](deploy.md#upgrade-and-remove)).

## Skills for querying Parkana

Once Parkana runs, an agent can query it with the Parkana agent skills. They
are listed, with their install commands, in [AI skills](skills/index.md).
