# Native OTLP comparison stack

This stack compares two native OTLP log converters over the same wire records:

* Grafana Loki 3.7.9 receives OTLP at `POST /otlp/v1/logs`.
* Parkana receives OTLP at `POST /v1/logs`.

Grafana Alloy is only a transport in this stack. It receives OTLP with
`otelcol.receiver.otlp` and fans the same OTLP `ResourceLogs` to both stores
with two `otelcol.exporter.otlphttp` components. There is no `loki.write`
stage and no conversion to the Loki push JSON/protobuf format, so the
converters under test are Loki's and Parkana's, not Alloy's.

The overlay lives next to the existing development stack files and leaves them
untouched:

| file | purpose |
|---|---|
| `deploy/compose/otlp.compose.yaml` | overlay: ingester+querier roles, Alloy/Loki config commands, Alloy OTLP ports, short Parkana flush/upload settings, explicit WAL mode |
| `deploy/compose/otlp.alloy` | native OTLP receiver + Loki/Parkana OTLP exporters |
| `deploy/compose/otlp.loki.yaml` | Loki 3.7.9 with default OTLP mapping and default service-name discovery |
| `scripts/e2e-phase4-otlp.sh` | fresh project/ports/output, readiness, transports, compares, restart; manifests and reports are preserved |

```sh
# offline: shell syntax, mocked failure paths, static config checks
bash -n scripts/e2e-phase4-otlp.sh
scripts/e2e-phase4-otlp.sh --self-test
scripts/e2e-phase4-otlp.sh --dry-run

# Boat VM only (Docker, Go and the OTLP endpoint available)
tm-boat test <worktree> -- scripts/e2e-phase4-otlp.sh --smoke
tm-boat test <worktree> -- scripts/e2e-phase4-otlp.sh --full
```

`--smoke` starts the stack, proves all readiness and OTLP routes, and checks
the effective settings. `--full` adds the transport cases and the restart
recheck. The script never starts a second stack, refuses an existing project,
held project lock or busy explicit port, and removes only its own Compose
project. Manifests, reports and the output directory are kept on success and
on failure; `--keep` controls only whether the Compose project is retained.

## Pinned images and exact endpoints

Alloy and Loki use the same tags and manifest-list digests as the existing
development stack: `grafana/alloy:v1.21.3` and `grafana/loki:3.7.9`. The
overlay adds no `image:` keys; `scripts/e2e-phase4-otlp.sh` asserts that and
re-checks both digests in the base Compose file.

The Alloy configuration is verified against the pinned source:

* Receiver: [`internal/component/otelcol/receiver/otlp/otlp.go` @ v1.21.3](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/receiver/otlp/otlp.go#L22).
  The HTTP server defaults to `0.0.0.0:4318`, accepts `/v1/logs`, and its
  accepted compression list includes gzip
  ([lines 190-200](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/receiver/otlp/otlp.go#L190),
  [`config_http.go` line 50](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/config_http.go#L50)).
* Exporter: [`otelcol.exporter.otlphttp` @ v1.21.3](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/exporter/otlphttp/otlphttp.go#L22).
  `logs_endpoint` overrides `client.endpoint` for logs and is used as given;
  Alloy appends `/v1/logs` only when `logs_endpoint` is empty
  ([lines 44-52](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/exporter/otlphttp/otlphttp.go#L44),
  [Convert at line 96](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/exporter/otlphttp/otlphttp.go#L96)).
  The exporter defaults are protobuf and gzip
  ([lines 63-72](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/exporter/otlphttp/otlphttp.go#L63),
  [lines 139-150](https://github.com/grafana/alloy/blob/v1.21.3/internal/component/otelcol/exporter/otlphttp/otlphttp.go#L139)).

Because both exporters set `logs_endpoint` explicitly, the wire paths are
exactly:

```text
http://loki:3100/otlp/v1/logs
http://parkana:3100/v1/logs
```

The Loki route is registered in
[`pkg/loki/modules.go:422` @ v3.7.9](https://github.com/grafana/loki/blob/v3.7.9/pkg/loki/modules.go#L422).
Parkana's route constant is `lokiapi.OTLPLogs` (`internal/lokiapi/lokiapi.go`).

Both exporters send `X-Scope-OrgID: default`, use `encoding = "proto"` and
`compression = "gzip"` toward the stores, and keep bounded queues with
`block_on_overflow = true` and a 60-second retry budget. Each exporter has its
own queue, so a slow store cannot silently change the other branch's payload.

## Readiness: store health versus OTLP target probes

There are two different checks, and the script keeps them separate:

1. **Process readiness:** `GET /ready` on Parkana, `GET /ready` on Loki,
   `GET /-/ready` on Alloy, plus the pinned Loki version from
   `/loki/api/v1/status/buildinfo`.
2. **OTLP route readiness:** an empty OTLP `ExportLogsServiceRequest` is posted
   as `Content-Type: application/x-protobuf` with an empty body. A 2xx proves
   the native route is mounted and the decoder accepts a well-formed request
   without writing an entry. This is the same probe `otlpcompare send` performs
   through `-ready-timeout 30s`; the script runs it against Alloy,
   `Loki /otlp/v1/logs` and `Parkana /v1/logs` before sending fixtures. Loki
   answers an empty push with 422 after the OTLP route has accepted it, so the
   Loki probe accepts 2xx or exactly 422 **whose body contains the full
   validation message** `error at least one valid stream is required for
   ingestion`. That string is `validation.MissingStreamsErrorMsg`
   ([`pkg/validation/validate.go:15` @ v3.7.9](https://github.com/grafana/loki/blob/v3.7.9/pkg/validation/validate.go#L15)),
   carried as the `Status.message` protobuf by Loki's `OTLPError`
   ([`pkg/loghttp/push/push.go:571-601` @ v3.7.9](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/push.go#L571));
   it is not the literal response text "Missing Streams". The probe body is
   kept in a tempfile and matched there because it is binary protobuf; a shell
   command substitution would drop its NUL bytes. Requiring that marker rejects
   an unrelated 422, and a missing route (404) still fails. `compare`
   independently polls `GET /ready` on both stores before its count gate.

The raw effective Loki config is written to
`<output>/loki-effective-config.yaml` before validation. It is always kept, so
a checker failure can be diagnosed against the exact `/config` response rather
than a re-query.

Every wait is bounded by `--ready-timeout SECONDS` (default 180, maximum
86400). There is no unbounded `until` loop. Any other probe status is a hard
failure with the status in the message, so a missing Parkana route fails the
run instead of being mistaken for "no data yet".

## Loki mapping under test

`otlp.loki.yaml` intentionally differs from the base development Loki config
in two ways:

1. **Service-name discovery uses the upstream default list.** The base config
   sets an empty list. The default list is in
   [`pkg/validation/limits.go:329` @ v3.7.9](https://github.com/grafana/loki/blob/v3.7.9/pkg/validation/limits.go#L329):
   `service`, `app`, `application`, `app_name`, `name`,
   `app_kubernetes_io_name`, `container`, `container_name`,
   `k8s_container_name`, `component`, `workload`, `job`, `k8s_job_name`; when
   none is present the stream gets `service_name="unknown_service"`.
2. **`distributor.otlp_config` is not set**, so Loki uses the default OTLP
   mapping. The 18 default resource attributes that become index labels are
   listed in
   [`pkg/loghttp/push/otlp_config.go:56` @ v3.7.9](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp_config.go#L56)
   (`service.name`, `service.namespace`, `service.instance.id`,
   `deployment.environment`, `deployment.environment.name`, `cloud.region`,
   `cloud.availability_zone`, `k8s.cluster.name`, `k8s.namespace.name`,
   `k8s.pod.name`, `k8s.container.name`, `container.name`,
   `k8s.replicaset.name`, `k8s.deployment.name`, `k8s.statefulset.name`,
   `k8s.daemonset.name`, `k8s.cronjob.name`, `k8s.job.name`). The fallback action
   for every other attribute is structured metadata
   ([line 103](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp_config.go#L103)).
   No tenant override is configured.

The only discovery feature kept off is log-level discovery
(`discover_log_levels: false`). Parkana's OTLP path does not invent a detected
level, so leaving Loki's log-level enrichment on would compare an upstream
behavior Parkana does not implement instead of the converter. This is the
documented exception and is asserted both from the config file and from Loki's
effective `/config` response.

The rest of the mapping under test is source-verified:

| concern | source |
|---|---|
| gzip, protobuf and JSON decode; wire/expanded body bounds | [`pkg/loghttp/push/otlp.go:60`](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L60), [decode at 119-141](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L119) |
| resource labels/metadata and scope fields | [`otlp.go:161`](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L161) |
| record attributes and fields | [`otlp.go:476`](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L476) |
| nested map flattening | [`otlp.go:602-639`](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L602) |
| timestamp, then observed timestamp, then now | [`otlp.go:642-651`](https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go#L642) |
| label normalization | `github.com/prometheus/otlptranslator v1.0.0`, [`label_namer.go:61`](https://github.com/prometheus/otlptranslator/blob/v1.0.0/label_namer.go#L61) (pinned by Loki's `go.mod`) |
| success status | 204 from the shared push path, [`pkg/distributor/http.go:151-159`](https://github.com/grafana/loki/blob/v3.7.9/pkg/distributor/http.go#L151) |

Loki's OTLP body limits are not overridden by this overlay. In the pinned
release, `distributor.max-recv-msg-size` defaults to 100 MiB and
`distributor.max-decompressed-size` to 5000 MiB
([`pkg/distributor/distributor.go:124-125`](https://github.com/grafana/loki/blob/v3.7.9/pkg/distributor/distributor.go#L124)).

## Fixtures, transports and mandatory cases

`cmd/otlpcompare` contains the deterministic fixture corpus. `send` builds the
plan from an anchor and writes a manifest; `compare` reads that manifest and
runs a fixed query set against both stores. Counts are never guessed from the
stores: they come from the fixture plan recorded in the manifest.

The command contract used by the script (single-dash flags, as in the other
comparison tools):

```sh
# One tee case: Alloy receives the fixture and fans it out to both stores.
otlpcompare send -target http://127.0.0.1:14318/v1/logs \
  -manifest out/manifest-tee.json -namespace run-tee -run-id run-tee \
  -anchor 2026-11-05T12:00:00.000000Z -codec mixed -gzip \
  -tenant default -timeout 30s -retries 5 -ready-timeout 30s

# compare takes store base URLs (it appends /loki/api/v1 itself).
otlpcompare compare -loki http://127.0.0.1:14101 \
  -parkana http://127.0.0.1:14100 -manifest out/manifest-tee.json \
  -tenant default -timeout 30s -wait-timeout 60s -poll-interval 1s \
  -ready-timeout 30s -format text -report out/report-tee.json
```

For a direct case the script runs `send` twice, once with
`-target http://127.0.0.1:14101/otlp/v1/logs` and once with
`-target http://127.0.0.1:14100/v1/logs`, using the same anchor, namespace and
run id. It then compares using either manifest. `-format json` prints the
machine-readable report on stdout; `-report PATH` always writes the report
artifact next to the manifests.

`compare` has a count gate before any query comparison: it polls
`{service_namespace="<ns>"}` on both stores until each side and each
`service_instance_id` group has exactly the manifest count. A timeout is exit
2; it never compares on partial or extra data. The script additionally waits
for `/ready` on both stores and passes `-ready-timeout` explicitly.

The script runs three isolated namespaces in one stack:

| case | transport | codec | gzip | target(s) |
|---|---|---|---|---|
| `direct-protobuf` | direct to each store | protobuf | no | `Loki /otlp/v1/logs`, `Parkana /v1/logs` |
| `direct-json-gzip` | direct to each store | JSON | yes | `Loki /otlp/v1/logs`, `Parkana /v1/logs` |
| `alloy-tee-mixed-gzip` | Alloy tee | mixed protobuf/JSON | yes | `Alloy /v1/logs` |

Each case gets `service.namespace=<project>-<case>` and a matching run id; the
two direct sends of one case share one anchor, so their timestamps are
identical. `compare` scopes every query to that namespace, so cases cannot
collide and every compare asserts exact entry counts on **both** stores.

Two fixture groups are mandatory gates:

* **Native discovery:** `service-present` supplies `service.name`;
  `service-empty` and `service-missing` must fall back to `unknown_service`;
  `service-discovered` must map the indexed `container.name` resource
  attribute to `service_name=discovered-container`; `service-missing-job` must
  not discover from `job`, which is structured metadata rather than an index
  label. This is the upstream default behavior the base development config
  suppresses.
* **Collisions:** `collide.name`/`collide_name`, `http.request.method`/
  `http_request_method`, and a stream label versus log metadata with the same
  normalized name must return real Loki's exact winning value for each
  normalized name. Merely reaching the entry through another contender is not
  equality; a different winner is a failed comparison, not a tolerated
  representation difference.

The other fixture cases cover nested maps, slices, bytes, scalars, unusual and
invalid attribute names, severity/flags, trace/span ids, body shapes and the
timestamp boundaries. Log range queries use the store contract directly:
inclusive `start`, exclusive `end` (`start <= ts < end`), with no nanosecond
adjustment; adjacent-microsecond probes pin both sides of the boundary.

After all three compares pass, `--full` restarts Parkana, Loki and Alloy and
reruns every compare against the same manifests. That proves exact counts
survive a restart of every writer; it does not resend the fixtures.

Reports are written to the output directory as
`report-<case>.json` and `report-<case>-after-restart.json`, next to the
manifests. The directory and every artifact are kept on success and failure;
`--keep` only leaves the Compose project running. The comparator exit status is
propagated: `0` equal, `1` semantic difference, `2` invocation/request/data
failure. `send` failures are reported as exit 2.

## Parkana ingest, WAL and publication settings

The comparison runs Parkana's normal ingest path. The overlay shortens the
publication cadence so a finite fixture is queryable inside `-wait-timeout`:

| setting | Parkana default | OTLP e2e |
|---|---|---|
| `-roles` | `all` | `ingester,querier` (no compactor or scheduler process) |
| `-ingest.flush-interval` | 5s | 1s |
| `-ingest.upload-interval` | 10s | 1s |
| `-ingest.upload-min` | 128 MiB | 1B |
| `-ingest.upload-max-age` | 600s | 2s |
| `-ingest.flush-concurrency` | 4 | 2 |
| `-ingest.upload-concurrency` | 4 | 2 |
| `-wal.sync` | `always` | `always` (overridable with `PARKANA_WAL_SYNC`) |
| `-wal.sync-interval` | 1s | 1s (unused in `always`) |

The defaults live in `internal/config/ingest.go:44-63` and the runtime fields
and bounds in `internal/config/ingest_runtime.go:11-62`. Naming the roles
explicitly means the compactor never starts, so its old+new overlap cannot make
physical rows temporarily exceed the fixture count.

Durability semantics are explicit:

* `always` (default) fsyncs every WAL write before the push is acknowledged
  (`internal/config/ingest_runtime.go:13-15`). The e2e uses it.
* `interval` acknowledges after the WAL write and memtable admission and
  fsyncs dirty data on `-wal.sync-interval`
  (`internal/config/ingest_runtime.go:16-18`). A machine crash can lose data
  written since the last completed sync; the interval schedules syncs, it is
  not a hard bound on a stalled filesystem. Choose it only when that window is
  acceptable.
* `PARKANA_INGEST_UPLOAD_MAX_AGE=2s` means a query sees an entry only after it
  has been uploaded and cataloged; `otlpcompare compare` polls until exact
  counts are visible or `-wait-timeout` expires. It does not treat a short
  count as equality.

`POST /v1/logs` accepts `application/x-protobuf` or `application/json`,
optionally gzip-compressed. The compressed wire body and the expanded body are
each checked against the same configured `MaxRequestBytes` value, exposed as
`-ingest.max-request-size` (16 MiB default); an over-limit request is rejected
rather than buffered without bound. `-ingest.max-line-size` (256 KiB default)
applies after OTLP conversion. Success is 204; an unsupported content type or
encoding, an invalid OTLP body, or invalid transformed output is 400; a
transform rejection is 400, other transform failures are 500, and an all-drop
request is 204 without appending. If Parkana rejects a fixture request, `send`
reports the HTTP status and exits 2.

Parkana is single-tenant. `X-Scope-OrgID` may be absent or `default`; the script
rejects any other `--tenant` before starting a stack
(`internal/tenant/tenant.go:64-76`).

## Cleanup, preserved evidence and secrets

* The Compose project defaults to `parkana-e2e-p4-otlp-<UTC timestamp>-<pid>`
  and is always passed explicitly with `-p`.
* A per-project lock directory (`parkana-otlp-e2e-lock-<project>`) is created
  before the project listing. A second invocation for the same project fails
  immediately; an invocation removes only the lock and Compose project it
  created.
* Project, container, volume and network listings fail closed: a listing error
  refuses startup instead of being treated as "nothing exists".
* The output directory is a fresh `mktemp -d` by default, or an explicit
  `--out` path that must not already exist. Manifests, reports and the
  directory are always preserved; `--keep` only leaves the Compose project for
  debugging and prints the exact teardown command.
* Cold builds/starts use a separate bounded Compose timeout
  (`PARKANA_OTLP_E2E_COMPOSE_TIMEOUT`, default 900s); short probes keep the
  30-second bound.
* Ports default to `14100/14900/14101/14245/14318/14317` and are checked free
  and distinct. An explicit busy port fails; a busy default is auto-picked to a
  free high port and the choice is printed.
* Cleanup never runs a broad prune. On failure it prints sanitized `ps`/`logs`
  diagnostics for its own project.
* Garage credentials are generated per run unless
  `PARKANA_DEV_GARAGE_KEY_ID`/`PARKANA_DEV_GARAGE_KEY_SECRET` are supplied
  explicitly. Diagnostics filter the secret line-by-line; the dry run and logs
  print only "values hidden".
* This is a development stack: Garage uses throwaway credentials, Loki auth is
  disabled, and every published port binds to `127.0.0.1`.

## Scope and non-goals

* No Grafana, pushgen, `loki.source.api` or `loki.write` is started by this
  overlay; the finite OTLP sender is `cmd/otlpcompare`.
* The stack tests OTLP/HTTP. Alloy also exposes OTLP/gRPC on 4317 for
  completeness, but the comparator and readiness probes use HTTP.
* Alloy delivery is at-least-once on transient failures. A retry that reaches
  a store twice is a failed exact-count comparison by design; the e2e does not
  hide retry copies.
* `--full` builds `cmd/otlpcompare` before starting the stack and fails if the
  command cannot be built. Readiness fails if `POST /v1/logs` is absent. No run
  is reported as passing without a real comparator and a live OTLP endpoint.
