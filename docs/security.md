# Security

Parkana has no authentication, no authorization and no TLS of its own. This
page describes what that exposes, how to place Parkana on a network so that
only the right callers reach it, and how to handle its credentials. Every
statement about Parkana here is checked against Parkana 1.2.0 (see
[CHANGELOG.md](../CHANGELOG.md)).

## Threat model

The HTTP server listens on `-http.addr` (`:3100` by default, every address of
the host) with plain HTTP. It reads no credentials, checks no tokens and does
not terminate TLS. Anyone who can open a TCP connection to the port can:

- push log lines (`POST /loki/api/v1/push`, `POST /v1/logs`),
- query every stored line, label and series,
- run read-only SQL over the stored rows (`POST /api/v1/sql`),
- create, replace and delete pipelines (`/api/v1/pipelines`), which can drop,
  rewrite or redact lines at ingest and change what queries return,
- subscribe to every line pushed to the ingester as it arrives
  (`GET /internal/ingest/tail`, unfiltered, or the Loki live tail WebSocket),
- read `/metrics`, which shows per-tenant ingest volumes, errors and internal
  state.

The live tail WebSocket accepts every `Origin`, as Loki's does (its tail
handler's `CheckOrigin` returns true, in `pkg/querier/tail/http.go`). A web
page opened by a user who can reach Parkana can therefore open a tail as well.
Grafana's Loki data source runs server-side (`access: proxy`), so the tail
route needs to be reachable only from the Grafana host: the
[nginx example](#nginx) limits it to that address, and a proxy can also
reject requests whose `Origin` header is present and is not your Grafana URL.

The one header Parkana interprets is `X-Scope-OrgID`, and it selects a tenant,
it does not authenticate one (see [Multi-tenancy](#multi-tenancy)). The server
sets a 10 second limit on reading request headers and no limit on reading a
body or writing a response, so slow clients are bounded by the per-route
deadlines (`-querier.timeout`, `-sql.timeout`) and the body size limits, not by
the connection.

### Routes by class

Every route the server registers, from the route patterns in
`internal/lokiapi`, the role packages and [`api/parkana.yaml`](../api/parkana.yaml).
The class says what a caller can do with it: read stored data, write data,
administer the process (SQL and pipelines), reach an internal role-to-role
route, or operate the process.

| route | class | served by |
|---|---|---|
| `POST /loki/api/v1/push` | write | ingester |
| `POST /v1/logs` | write | ingester |
| `GET`, `POST /loki/api/v1/query` | read | querier |
| `GET`, `POST /loki/api/v1/query_range` | read | querier |
| `GET`, `POST /loki/api/v1/labels` | read | querier |
| `GET /loki/api/v1/label/{name}/values` | read | querier |
| `GET`, `POST /loki/api/v1/series` | read | querier |
| `GET /loki/api/v1/index/stats` | read | querier |
| `GET /loki/api/v1/index/volume` | read | querier |
| `GET /loki/api/v1/index/volume_range` | read | querier |
| `GET /loki/api/v1/tail` (WebSocket) | read | querier |
| `POST /api/v1/sql` | admin | querier |
| `GET`, `POST /api/v1/pipelines` | admin | ingester or querier |
| `GET`, `PUT`, `DELETE /api/v1/pipelines/{name}` | admin | ingester or querier |
| `POST /api/v1/pipelines/dry-run` | admin | ingester or querier |
| `GET /internal/ingest/tail` | internal | ingester |
| `GET /loki/api/v1/status/buildinfo` | ops | every process |
| `GET /ready` | ops | every process |
| `GET /metrics` | ops | every process |

A process that does not run the role behind a Loki route answers that route
with 501, so a route is never silently missing. The querier reaches
`/internal/ingest/tail` over HTTP: on the URLs in `-tail.ingester-urls`, or on
a loopback URL derived from `-http.addr` when the same process runs the
ingester. That request carries no credentials, so a proxy in front of
`/internal/ingest/tail` would break live tail unless the querier reaches the
ingester directly.

Logs often carry secrets and personal data. Everyone who can query sees
every stored line, so remove that data before it is stored with an ingest
pipeline; [Redact and anonymize at ingestion](pipelines/redaction.md)
covers what that does and does not protect, and gives tested patterns.

## Recommended layouts

From most to least preferred. They combine: a private network plus a proxy is
a common setup.

### Private network only

Run Parkana in a private subnet (in `10.0.0.0/8`, for example) with no public
address and no inbound path from the internet. Shippers, Grafana and
administrators are on the same network or reach it over a VPN. Add the
[host firewall](#host-firewall) so that inside the network only the shipper
subnets and the Grafana host reach port 3100. This is the simplest layout, and
everything else on this page narrows it further.

### Outside ingestion through a private path

When shippers run outside the network (another VPC, another account, a data
center), connect them through a private path such as VPC peering, a transit
gateway or a PrivateLink-style endpoint service, never over a public address.
Point that path at a separate ingester-only process (`-roles=ingester`), and
keep the querier, Grafana and SQL inside.

An ingester-only process does not serve the read routes (they answer 501) or
`/api/v1/sql` (404), but it is not push-only. It still serves:

- `POST /loki/api/v1/push` and `POST /v1/logs`,
- `GET /internal/ingest/tail`, which streams every pushed line,
- all of `/api/v1/pipelines`, including create and delete,
- `/ready`, `/metrics` and `/loki/api/v1/status/buildinfo`.

A security group, peering route or firewall works on addresses and ports, so
it cannot hide these paths from a shipper that can reach the port. To expose
only the two push routes, put the [reverse proxy](#reverse-proxy-with-tls-and-authentication)
in front of the ingester's port and let the private path reach only the proxy.
Splitting roles needs a catalog that both processes share (see
[Deploy](deploy.md#split-roles-across-processes)).

### Overlay networks

An overlay network gives every caller an identity and encrypts the traffic
without a public port. Two common choices follow. Their configuration syntax
below was checked against the vendors' documentation on 2026-11-23.

#### Tailscale

Tag the Parkana host, the shippers and Grafana, and grant only those tags
access. Grants allow addresses and ports, so they decide who connects, not
which path they call. The policy file is HuJSON:

```jsonc
{
  "groups": {
    "group:ops": ["admin@example.com"],
  },
  "tagOwners": {
    "tag:logs": ["group:ops"],
    "tag:shipper": ["group:ops"],
    "tag:grafana": ["group:ops"],
  },
  "grants": [
    {"src": ["tag:shipper"], "dst": ["tag:logs"], "ip": ["tcp:443"]},
    {"src": ["tag:grafana"], "dst": ["tag:logs"], "ip": ["tcp:443"]},
    {"src": ["group:ops"], "dst": ["tag:logs"], "ip": ["tcp:443", "tcp:22"]},
  ],
}
```

On the Parkana host, listen on loopback only (`PARKANA_HTTP_ADDR=127.0.0.1:3100`
in `/etc/parkana/parkana.env`) and let Tailscale Serve terminate TLS on the
tailnet:

```sh
sudo tailscale serve --bg 3100
```

Shippers and Grafana then use `https://<machine>.<tailnet>.ts.net`. Serve
publishes to the tailnet only; `tailscale funnel` would publish to the
internet, so do not use it for Parkana. Every request reaches Parkana from
`127.0.0.1`, so the host firewall cannot tell callers apart; put the
[reverse proxy](#reverse-proxy-with-tls-and-authentication) between Serve and
Parkana when shippers must not reach the read and admin routes.

Sources: [policy file syntax](https://tailscale.com/kb/1337/policy-syntax),
[grants syntax](https://tailscale.com/docs/reference/syntax/grants) and
[Tailscale Serve](https://tailscale.com/kb/1242/tailscale-serve), checked
2026-11-23.

#### Cloudflare Tunnel and Access

`cloudflared` runs next to Parkana and opens an outbound tunnel, so the host
needs no inbound port at all. Give push and reads separate hostnames, and route
only their paths:

```yaml
tunnel: <TUNNEL-ID>
credentials-file: /etc/cloudflared/<TUNNEL-ID>.json

ingress:
  - hostname: push.example.com
    path: ^/(loki/api/v1/push|v1/logs)$
    service: http://localhost:3100
  - hostname: logs.example.com
    path: ^/loki/api/v1/
    service: http://localhost:3100
  - service: http_status:404
```

The last rule is the catch-all that Cloudflare requires; it answers 404 for
`/api/v1/sql`, `/api/v1/pipelines`, `/internal/ingest/tail` and `/metrics`, so
those stay reachable only from inside. Check the file with
`cloudflared tunnel ingress validate`.

Protect both hostnames with Cloudflare Access applications. For
`push.example.com`, create a service token per shipper and a policy with the
action Service Auth; shippers send the token in the `CF-Access-Client-Id` and
`CF-Access-Client-Secret` headers. For `logs.example.com`, people sign in with
your identity provider. Grafana's Loki data source runs server-side
(`access: proxy`), so it cannot complete an interactive login: give it a
service token too, sent as custom HTTP headers in the data source settings,
and add a Service Auth policy for it on `logs.example.com`. Cloudflare limits
the size of a proxied request body by plan: check your plan's upload limit
against `-ingest.max-request-size` (16 MiB by default). The tunnel passes the
request path unchanged. Live tail needs a WebSocket through the tunnel; test it
before you rely on it.

Sources: [tunnel configuration file](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/configure-tunnels/local-management/configuration-file/)
and [service tokens](https://developers.cloudflare.com/cloudflare-one/identity/service-tokens/),
checked 2026-11-23.

### Reverse proxy with TLS and authentication

A reverse proxy on the Parkana host terminates TLS, authenticates callers and
forwards only the routes each caller needs. Parkana then listens on loopback:

```sh
PARKANA_HTTP_ADDR=127.0.0.1:3100
```

Both examples below follow the same rules:

- shippers reach only `POST /loki/api/v1/push` and `POST /v1/logs`, with a
  body limit of 16 MiB, the default `-ingest.max-request-size` (raise both
  together),
- Grafana reaches the Loki read routes, and the live tail WebSocket only from
  the Grafana host (`10.0.2.10`),
- `/api/v1/sql` and `/api/v1/pipelines` are reachable only from the private
  network (`10.0.0.0/8`) and with administrator credentials; Grafana needs
  them too if it uses the [SQL plugin](grafana.md),
- `/internal/ingest/tail`, `/metrics` and every other path get 404; scrape
  `/metrics` and probe `/ready` on `127.0.0.1:3100` from the host.

#### nginx

Shippers authenticate with client certificates on `push.example.com`;
Grafana and administrators use basic auth on `logs.example.com`. Put the file
in `/etc/nginx/conf.d/parkana.conf` (it belongs in nginx's `http` block):

```nginx
upstream parkana {
    server 127.0.0.1:3100;
}

map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

# Shippers: push only, authenticated by client certificate.
server {
    listen 443 ssl;
    server_name push.example.com;

    ssl_certificate        /etc/nginx/tls/push.example.com.crt;
    ssl_certificate_key    /etc/nginx/tls/push.example.com.key;
    ssl_client_certificate /etc/nginx/tls/shipper-ca.crt;
    ssl_verify_client      on;

    # Matches Parkana's -ingest.max-request-size default (16 MiB).
    client_max_body_size 16m;
    client_body_timeout  30s;

    location = /loki/api/v1/push {
        limit_except POST { deny all; }
        proxy_pass http://parkana;
    }

    location = /v1/logs {
        limit_except POST { deny all; }
        proxy_pass http://parkana;
    }

    location / {
        return 404;
    }
}

# Grafana and administrators: reads, live tail and the admin API.
server {
    listen 443 ssl;
    server_name logs.example.com;

    ssl_certificate     /etc/nginx/tls/logs.example.com.crt;
    ssl_certificate_key /etc/nginx/tls/logs.example.com.key;

    auth_basic           "Parkana";
    auth_basic_user_file /etc/nginx/parkana-readers.htpasswd;

    location /loki/api/v1/ {
        limit_except GET POST { deny all; }
        proxy_pass http://parkana;
    }

    # Push is not a read route.
    location = /loki/api/v1/push {
        return 404;
    }

    # Live tail: only the Grafana server opens it.
    location = /loki/api/v1/tail {
        allow 10.0.2.10;
        deny  all;
        proxy_pass         http://parkana;
        proxy_http_version 1.1;
        proxy_set_header   Upgrade $http_upgrade;
        proxy_set_header   Connection $connection_upgrade;
        proxy_read_timeout 1h;
    }

    # SQL and pipelines: private network and administrator accounts only.
    location /api/v1/ {
        allow 10.0.0.0/8;
        deny  all;
        auth_basic_user_file /etc/nginx/parkana-admins.htpasswd;
        proxy_pass http://parkana;
    }

    location / {
        return 404;
    }
}
```

Create the password files with `htpasswd -B` (from `apache2-utils` or
`httpd-tools`), one account per Grafana instance and administrator. For basic
auth on the push host instead of certificates, replace the four `ssl_*client*`
lines with `auth_basic` and `auth_basic_user_file`, as on the read host.

#### Caddy

One hostname with basic auth for every caller and separate accounts per route
set. Caddy obtains the certificate itself and passes WebSocket upgrades
without extra settings. The password hashes come from the environment
(`caddy hash-password` prints one), so the Caddyfile holds no secret:

```text
logs.example.com {
	@push {
		method POST
		path /loki/api/v1/push /v1/logs
	}
	handle @push {
		basic_auth {
			shipper {$SHIPPER_PASSWORD_HASH}
		}
		request_body {
			max_size 16MiB
		}
		reverse_proxy 127.0.0.1:3100
	}

	@read {
		method GET POST
		path /loki/api/v1/query /loki/api/v1/query_range /loki/api/v1/labels /loki/api/v1/label/* /loki/api/v1/series /loki/api/v1/index/* /loki/api/v1/status/buildinfo
	}
	handle @read {
		basic_auth {
			grafana {$GRAFANA_PASSWORD_HASH}
		}
		reverse_proxy 127.0.0.1:3100
	}

	@tail {
		remote_ip 10.0.2.10
		path /loki/api/v1/tail
	}
	handle @tail {
		basic_auth {
			grafana {$GRAFANA_PASSWORD_HASH}
		}
		reverse_proxy 127.0.0.1:3100
	}

	@admin {
		remote_ip 10.0.0.0/8
		path /api/v1/sql /api/v1/pipelines /api/v1/pipelines/*
	}
	handle @admin {
		basic_auth {
			admin {$ADMIN_PASSWORD_HASH}
		}
		reverse_proxy 127.0.0.1:3100
	}

	handle {
		respond 404
	}
}
```

For client certificates instead of basic auth on the push routes, give them
their own site block with a `tls` block that sets `client_auth` with `mode
require_and_verify` and your shipper CA (`trust_pool file`).

## Credentials

Parkana holds at most two secrets: the S3 access key pair
(`-storage.s3.access-key`, `-storage.s3.secret-key`) and the Postgres catalog
DSN (`-catalog.dsn`) when it contains a password. The Postgres catalog is
new in 1.2.0.

### Where to keep them

Keep them in environment variables (`PARKANA_STORAGE_S3_ACCESS_KEY`,
`PARKANA_STORAGE_S3_SECRET_KEY`, `PARKANA_CATALOG_DSN`), never as flags on the
command line: every user of the host can read a process's arguments with `ps`
or `/proc/<pid>/cmdline`, while `/proc/<pid>/environ` is readable only by the
process's own user and root.

The package installs `/etc/parkana/parkana.env` with mode 0640, owner `root`
and group `parkana`, and the unit reads it with `EnvironmentFile=`. The
`parkana` group can read it, and so can anything else running as that user.
To keep the secrets away from the service user too, put them in a second file
readable only by root (systemd reads environment files as root before it
starts the service):

```sh
sudo install -m 0600 -o root -g root /dev/null /etc/parkana/secrets.env
sudo systemctl edit parkana
```

```ini
[Service]
EnvironmentFile=/etc/parkana/secrets.env
```

A later `EnvironmentFile=` adds to the packaged one, and a variable set in
both takes the later value. The S3 client uses only the static key pair it is
given: it does not read `AWS_*` variables, shared credential files or instance
roles. The unit sets no `LoadCredential=`, and Parkana reads no secret from a
file of its own. For Postgres, the pgx driver accepts `PGPASSWORD` and a
password file (see [Catalog](catalog.md#postgres)), so the password can also
stay out of the DSN.

### Least-privilege S3 policy

Parkana calls PutObject, GetObject (whole objects and byte ranges),
HeadObject, ListObjectsV2 and DeleteObject on its bucket. Large writes are
multipart uploads: the S3 client (minio-go 7.4.1) uploads in parts when the
object size is unknown or above its 16 MiB part size. The compactor writes
merged files with an unknown size, so every merge is a multipart upload; the
ingester's uploads and the sidecars have a known size and are multipart above
16 MiB. When a multipart upload fails, the client sends
AbortMultipartUpload, which needs its own permission. A bucket-scoped AWS IAM
policy for the bucket `parkana-logs`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListParkanaBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::parkana-logs"
    },
    {
      "Sid": "ReadWriteParkanaObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::parkana-logs/*"
    }
  ]
}
```

On AWS, `s3:PutObject` covers starting a multipart upload, uploading parts
and completing it, and HeadObject needs `s3:GetObject`. Parkana never lists
multipart uploads or their parts. `s3:CreateBucket` is needed only with
`-storage.s3.create-bucket`, which first checks for the bucket (HeadBucket,
which needs `s3:ListBucket`) in every process that has the flag; create the
bucket yourself and leave the flag off.

The abort is best effort, and a process that crashes during an upload leaves
its parts behind, so add a lifecycle rule that removes incomplete multipart
uploads (for `aws s3api put-bucket-lifecycle-configuration`):

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": {},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
    }
  ]
}
```

When roles run in separate processes, each can have its own key with only the
operations its role calls:

| role | operations | IAM actions |
|---|---|---|
| querier | GetObject (object cache and sidecar cache fills) | `s3:GetObject` |
| ingester | PutObject (uploads and their sidecars) | `s3:PutObject`, `s3:AbortMultipartUpload` |
| compactor | HeadObject and ranged GetObject (merge input, sidecar backfill), PutObject, ListObjectsV2 (orphan collection), DeleteObject | the whole policy above |

Other S3-compatible stores have their own policy languages; grant the same
operations.

### Least-privilege Postgres role

The catalog needs a login role with `USAGE` and `CREATE` on an existing
schema; Parkana creates its tables there at startup and never creates the
schema itself (see [Catalog](catalog.md#postgres)):

```sql
CREATE ROLE parkana LOGIN PASSWORD 'change-me';
CREATE SCHEMA parkana AUTHORIZATION CURRENT_USER;
GRANT USAGE, CREATE ON SCHEMA parkana TO parkana;
ALTER ROLE parkana SET search_path = parkana;
```

Connect with `sslmode=verify-full` in the DSN when the database is on another
host.

### Rotation

Parkana reads its configuration once at startup and has no reload, so a new
credential takes effect at the next restart. For S3, create the new key, put it
in the environment file, `sudo systemctl restart parkana` (every process that
uses the bucket, when roles are split), check `/ready`, then delete the old
key. For Postgres, change the password in the environment file and with `ALTER
ROLE ... PASSWORD`, then restart right away: open connections stay valid, but
new ones fail until the restart.

## Host firewall

A host firewall with a default-deny input policy limits who connects to port
3100. The examples allow the shipper subnet `10.0.1.0/24`, the Grafana host
`10.0.2.10`, loopback (Grafana on the same host, a local proxy, the querier's
loopback tail URL) and SSH from an administration subnet `10.0.9.0/24`. Adjust
the addresses and open 443 instead of 3100 when a proxy fronts Parkana.

nftables (`/etc/nftables.conf`; `flush ruleset` also removes rules other
software added, such as a container runtime's, so merge rather than replace on
such hosts):

```text
#!/usr/sbin/nft -f

flush ruleset

table inet filter {
    chain input {
        type filter hook input priority filter; policy drop;

        ct state established,related accept
        ct state invalid drop
        iif "lo" accept
        meta l4proto { icmp, ipv6-icmp } accept

        ip saddr 10.0.9.0/24 tcp dport 22 accept comment "administration"
        ip saddr 10.0.1.0/24 tcp dport 3100 accept comment "log shippers"
        ip saddr 10.0.2.10 tcp dport 3100 accept comment "Grafana"
    }

    chain forward {
        type filter hook forward priority filter; policy drop;
    }

    chain output {
        type filter hook output priority filter; policy accept;
    }
}
```

iptables, in the `iptables-restore` format (`/etc/iptables/rules.v4` on Debian
and Ubuntu with `iptables-persistent`); IPv6 needs its own rules in
`rules.v6`:

```text
*filter
:INPUT DROP [0:0]
:FORWARD DROP [0:0]
:OUTPUT ACCEPT [0:0]
-A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
-A INPUT -m conntrack --ctstate INVALID -j DROP
-A INPUT -i lo -j ACCEPT
-A INPUT -p icmp -j ACCEPT
-A INPUT -s 10.0.9.0/24 -p tcp --dport 22 -m comment --comment "administration" -j ACCEPT
-A INPUT -s 10.0.1.0/24 -p tcp --dport 3100 -m comment --comment "log shippers" -j ACCEPT
-A INPUT -s 10.0.2.10/32 -p tcp --dport 3100 -m comment --comment "Grafana" -j ACCEPT
COMMIT
```

A firewall works on addresses and ports. Every caller it lets in can call
every route, so a shipper allowed to push can also query, run SQL and change
pipelines. Limiting paths needs the
[reverse proxy](#reverse-proxy-with-tls-and-authentication), or a separate
ingester-only process on its own port with the proxy in front of it (see
[Outside ingestion](#outside-ingestion-through-a-private-path)).

## Multi-tenancy

Multi-tenancy is off, and `-multi-tenant` is rejected at startup. Every
request uses the tenant `default`: a request without `X-Scope-OrgID` gets it,
one with `X-Scope-OrgID: default` gets it, and any other value is answered
with 401. The header selects a tenant; it does not prove who the caller is.
When multi-tenancy lands, a proxy that authenticates callers will have to set
`X-Scope-OrgID` itself and drop the value a client sent.

## SQL API sandbox

`POST /api/v1/sql` runs one caller-supplied statement, so it has its own
limits on top of the network rules. Each request gets a new in-memory DuckDB
database. Parkana copies the tenant's rows in the requested time range into a
table named `logs`, releases every file, and only then locks the database
down:

- automatic extension install and load are off, community and unsigned
  extensions are refused, persistent secrets are off,
- external access is off and DuckDB's local file system is disabled, so no
  query can read or write a file, including the cached Parquet objects,
- the configuration is locked, so a query cannot change a setting back,
- the spill directory is empty, so a large query fails with an out-of-memory
  error instead of writing tenant data to disk.

Before the statement runs, Parkana rejects it unless it is exactly one
`SELECT` (or `WITH ... SELECT`) statement, parsed (not executed) by DuckDB.
`WITH RECURSIVE`, `PRAGMA`, `SET`, `ATTACH`, `COPY` and every write are
rejected. The only relations a query may name are `logs` and its own CTEs;
schema- or catalog-qualified names are rejected, and so are the functions that
read settings or the environment (`current_setting`, `getenv`,
`getvariable`), run SQL indirectly (`query`, `query_table`,
`json_execute_serialized_sql`), read files (`read_*`, `glob`, `parquet_scan`,
`parquet_metadata`, `sniff_csv` and similar) or introspect DuckDB (`duckdb_*`,
`pragma_*`, `sqlite_*`).

The limits per request: 64 KiB of query text and a nesting depth of 1000, a
memory budget (`-sql.memory-limit`, 1 GiB), threads (`-sql.threads`, 1), a
deadline (`-sql.timeout`, 30 s), result caps (`-sql.max-rows`, 10000;
`-sql.max-result-bytes`, 8 MiB) and at most `-sql.max-concurrent` (2) requests
at once. The memory budget bounds DuckDB's buffers, not the process's memory.

The sandbox keeps a query inside the tenant's rows and away from the host, but
any caller can still read every stored line in the time range and spend the
CPU and memory budgets. Treat the SQL API as an administrator tool for trusted
callers, behind the same access control as the pipelines API.

## Hardening in the package

The systemd unit in the deb and rpm packages runs Parkana as the system user
`parkana` and sets:

| setting | effect |
|---|---|
| `User=parkana`, `Group=parkana` | runs without root |
| `NoNewPrivileges=true` | no privilege gain through setuid binaries or file capabilities |
| `ProtectSystem=strict` | the whole file system is read-only except `ReadWritePaths` |
| `ReadWritePaths=/var/lib/parkana` | the only writable path (WAL, local Parquet, cache, SQLite catalog) |
| `StateDirectory=parkana`, `StateDirectoryMode=0750` | creates `/var/lib/parkana`, owned by the service user |
| `ProtectHome=true` | `/home`, `/root` and `/run/user` are inaccessible |
| `PrivateTmp=true` | a private `/tmp` and `/var/tmp` |
| `PrivateDevices=true` | no access to physical devices |
| `ProtectKernelTunables=true` | `/proc/sys`, `/sys` and similar are read-only |
| `ProtectKernelModules=true` | no kernel module loading |
| `ProtectControlGroups=true` | the cgroup tree is read-only |
| `RestrictSUIDSGID=true` | the service cannot create setuid or setgid files |
| `LockPersonality=true` | the execution domain cannot change |
| `LimitNOFILE=65536` | the open file limit (not a hardening setting) |

These settings do not restrict the network: the unit sets no
`IPAddressAllow=`/`IPAddressDeny=` and no `RestrictAddressFamilies=`, so
Parkana can connect anywhere and anyone can connect to it. There is no system
call filter (`SystemCallFilter=`), no `MemoryDenyWriteExecute=` and no
`CapabilityBoundingSet=` (the service user has no capabilities to begin with).
They do nothing about the HTTP surface on this page. If you add
`IPAddressAllow=` in a drop-in, include the addresses of the object store, the
catalog database and the callers.

## Shipper side

Each example sends through the proxy with TLS and credentials. Keep secrets in
files or environment variables of the shipper, not in its configuration file.

Grafana Alloy, Loki push. The first endpoint uses basic auth (the Caddy
example), the second a client certificate (the nginx example);
`bearer_token_file` takes the place of `basic_auth` when your proxy checks
bearer tokens:

```hcl
loki.write "parkana" {
  endpoint {
    url = "https://logs.example.com/loki/api/v1/push"

    basic_auth {
      username      = "shipper"
      password_file = "/etc/alloy/parkana-password"
    }
  }
}

loki.write "mtls" {
  endpoint {
    url = "https://push.example.com/loki/api/v1/push"

    tls_config {
      cert_file = "/etc/alloy/shipper.crt"
      key_file  = "/etc/alloy/shipper.key"
    }
  }
}
```

OpenTelemetry Collector (contrib distribution, which has the `basicauth`
extension; `bearertokenauth` works the same way). The `otlphttp` exporter
appends `/v1/logs` to `endpoint`:

```yaml
extensions:
  basicauth/parkana:
    client_auth:
      username: shipper
      password: ${env:SHIPPER_PASSWORD}

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:4317

exporters:
  otlphttp/parkana:
    endpoint: https://logs.example.com
    compression: gzip
    auth:
      authenticator: basicauth/parkana

service:
  extensions: [basicauth/parkana]
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp/parkana]
```

Grafana's Loki data source, provisioned with basic auth. For a client
certificate, set `tlsAuth: true` in `jsonData` and `tlsClientCert` and
`tlsClientKey` in `secureJsonData` instead; Grafana expands the `$`
variables from its environment:

```yaml
apiVersion: 1

datasources:
  - name: Parkana
    uid: parkana
    type: loki
    access: proxy
    url: https://logs.example.com
    basicAuth: true
    basicAuthUser: grafana
    secureJsonData:
      basicAuthPassword: $GRAFANA_LOGS_PASSWORD
```
