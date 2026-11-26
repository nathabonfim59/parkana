# Loki comparison

Parkana's LogQL support is checked against Loki 3.7.9 by running the same
requests against both servers and comparing the answers. This page
describes the comparison corpus and its datasets, records the evidence of
each run and what is still unverified, and lists the primary sources. The
supported LogQL and the status of each feature are in
[LogQL](../query/logql.md).

## Comparison corpus

File: [`cmd/lokicompare/testdata/grafana-corpus.json`](../../cmd/lokicompare/testdata/grafana-corpus.json).

The corpus uses the frozen M1 schema: a JSON array of
`{name, type, query, params, expect}` plus the optional `lokiQuirk` opt-in.

- `type` is one of `range`, `instant`, `labels`, `label_values`, `series`, and
  the later `stats`, `volume`, `volume_range`.
- `query` is the LogQL expression, or the scoping selector for `labels`,
  `label_values` and the first `series` matcher.
- `params` values are a string or an array of strings. `limit`, `direction` and
  `step` pass through as Loki query parameters. `label_values` consumes exactly
  one non-empty `params.label`; it must not put the query in `params.query`.
- `lokiQuirk` is an optional flat string that opts into one documented Loki
  deviation. The only value is `scalar-timestamp`, valid only on `instant`
  entries without `expect`. Under it both servers must return a scalar result
  (otherwise the invocation fails with exit 2), values compare with the normal
  float tolerances, the RIGHT timestamp must be the request instant truncated
  to milliseconds, and the LEFT timestamp is only checked to be a numeric JSON
  timestamp, reported in a note and never compared. Loki 3.7.9 calls
  `model.TimeFromUnix` on an already-millisecond
  `LegacySample.TimestampMs` in `pkg/querier/queryrange/prometheus.go`
  (`marshalScalar`) and the protobuf scalar round trip overflows the timestamp
  again. Null, non-string, unknown and misplaced quirk values fail before any
  HTTP request; unopt-in entries keep the strict scalar comparison.
- `expect` is optional and asserts exact RIGHT counts on `stats`, `volume` and
  `volume_range`. It uses the schema documented by `lokicompare -h`: stats
  `streams`/`entries`/`bytes`, instant volume `metric`+`value`, and range
  volume `metric`+`values`. An empty `series` array asserts that RIGHT returns
  no series. Values compare as exact rationals, so the float flags never apply.
  Invalid expectations fail before any request.
- `expect.status` is a JSON integer in `[400, 599]`, valid on every entry
  type and exclusive with `stats`/`series`. Both servers must return exactly
  that status with a non-empty UTF-8 `text/plain` body (the message wording
  is not compared). A wrong status or an invalid error body is a semantic
  difference (exit 1); transport, read and size failures still exit 2. Loki
  3.7.9 rejects a bare log-selector instant query with HTTP 400
  (`ErrUnsupportedSyntaxForInstantQuery`,
  [`pkg/querier/http.go#L122-L127`](https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/http.go#L122-L127));
  `instant-log-selector-rejected` pins that and the form is reusable for
  other invalid or unsupported request fixtures.
- The full e2e corpus should use `expect.stats` and instant `volume`
  `expect.series` over the complete uploaded range. A checked-in corpus cannot
  carry static `volume_range` expectations for data that starts at generation
  time; use a runtime-generated corpus or a unit test for those timestamps.
- Each entry uses the CLI's `-start`/`-end` unless it supplies its own bounds.
  The CLI canonicalizes bounds to RFC3339Nano on both sides (Loki and Parkana
  both accept that form); the plugin itself sends decimal Unix nanoseconds to
  Loki. The corpus compares response semantics, not request bytes.
- Run with server roots (not full API URLs):

```sh
go build -o bin/lokicompare ./cmd/lokicompare
bin/lokicompare \
  -left http://localhost:3100 \
  -right http://localhost:3200 \
  -corpus cmd/lokicompare/testdata/grafana-corpus.json \
  -start 2026-11-04T00:00:00Z \
  -end 2026-11-04T01:00:00Z
```

The CLI still exits 2 while any entry fails: the `c41c277` run exited 2 with
14 equal and 4 expected unsupported-metric failures.

The checked-in corpus now covers the feature families of the
[feature matrix](../query/logql.md#feature-matrix) with 112
entries: 51 `range`, 42 `instant`, 2 `labels`, 2 `label_values`, 3
`series`, 7 `stats`, 4 `volume` and 1 `volume_range`, totalling 89 positive
metric/log queries, 4 mutually-invalid `400` cases and 19 metadata-endpoint
entries. `stats` and instant `volume` entries carry exact RIGHT
expectations derived from the 22,200-entry dataset (553,491 line bytes
across 21 label sets); `volume_range` has no static timestamp expectation.
The corpus runner's tests assert concrete `scalar`/`vector`/`matrix`/
`streams` result types (scalar values 7, 9, 512 and 1), real unsupported
classification and mutual-invalid `BadQuery`, with no skips. The four scalar
instant entries carry `lokiQuirk: "scalar-timestamp"`; `agg-stddev` and
`agg-stdvar` use a bounded `sum by (format, level)` child so both engines
evaluate the population variance without hitting Loki's inner child limit.
The original 18 fixtures are preserved as the first entries, including the two
captured request shapes: `logs-base-api-range` at `step=5s` and the byte-exact
`drop __error__[1m]` histogram at `step=1m`, both at `limit=1000` with
`direction=backward`. The corpus covers the three deterministic pushgen
datasets and follows the frozen M1 schema.

### Required datasets

All three generators send to Alloy's push URL,
`http://alloy:3100/loki/api/v1/push`.

Base dataset (20,000 entries, 10,000 per stream):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=api -stream=web \
  -label=job=frontend -label=env=dev -label=level=info \
  -total=20000 -rate=1000 -batch=100
```

Labels: `service_name=api|web`, `job=frontend`, `env=dev`, `level=info`.
Lines are `id=<n> stream=<name>`.

Job-only dataset (1,000 entries, no `service_name` label):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=legacy -stream-label=job -label=env=dev \
  -total=1000 -rate=1000 -batch=100
```

Labels: `job=legacy`, `env=dev`.

Corpus dataset (1,200 entries, deterministic mixed formats):

```sh
pushgen -url=http://alloy:3100/loki/api/v1/push -profile=corpus \
  -format=json -stream=corpus -label=job=corpus -label=env=dev \
  -total=1200 -rate=1000 -batch=100
```

Labels: `service_name=corpus`, `job=corpus`, `env=dev`, plus per-record
`format=json|logfmt|plain`, `level=info|warn|error`, `pod=pod-a|pod-b`.
Structured metadata: `trace_id`, `request_id`. The 1,200 records always total
exactly 1,200 rows and start at a microsecond-aligned instant, one millisecond
apart. Only configured stream names are assigned round-robin; `format`,
`level` and `pod` are chosen per record from the seeded splitmix64 stream. For
the default total the fixture yields 423 JSON, 384 logfmt and 393 plain rows
and covers all 18 format×level×pod label sets, so the mix is deterministic but
not even.

Corpus queries rely on these fixture fields:

- JSON lines nest status at `response.status`; the JSON corpus query uses
  named extraction, `| json status="response.status"`, then filters `status`.
  Logfmt and plain lines carry flat `status=`.
- All formats embed `id`, numeric `latency` and `bytes`, a Go `duration`
  string, a byte-unit `size`, and a `msg`.
- `format` is a stream label, so parser-specific queries select compatible
  rows.
- `grafana-histogram-api` pins `step=1m`, matching the captured intervalMs
  60000 whose backend `$__auto` interpolation yields the `[1m]` already
  present in the expression; the two corpus-dataset histograms pin
  `step=60000ms`.

### Corpus entries

The original 18 representative entries, preserved inside the full corpus:

| Name | Type | Purpose |
|---|---|---|
| `logs-base-api-range` | range | Plain Explore logs request on the base dataset; `step` pinned to `5s`. |
| `logs-base-web-line-filter` | range | `|=` filter on the second base stream. |
| `logs-job-only-legacy` | range | Dataset without `service_name`. |
| `logs-corpus-json-parser-filter` | range | `json` parser plus numeric label filter. |
| `logs-corpus-logfmt-line-filter` | range | Logfmt rows with a substring filter. |
| `logs-corpus-plain-regexp` | range | Plain rows with an unanchored RE2 line filter. |
| `count-instant-by-level` | instant | Instant `sum by (level)` `count_over_time`. |
| `grafana-histogram-api` | range | Captured no-parser Grafana histogram (`drop __error__[1m]`, `step=1m`); missing `detected_level`. |
| `grafana-histogram-corpus-json` | range | Histogram over a JSON pipeline. |
| `grafana-histogram-corpus-logfmt` | range | Histogram over a logfmt pipeline. |
| `grafana-labels-scoped` | labels | `query=<selector>` label names. |
| `grafana-labels-unscoped` | labels | No selector, as the browser sends initially. |
| `grafana-label-values-service-name` | label_values | `label/service_name/values` scoped by `query`. |
| `grafana-label-values-format-scoped` | label_values | Values for `format` under a narrower selector. |
| `grafana-series-corpus` | series | `match[]` series request. |
| `series-job-only-legacy` | series | Series on the job-only dataset. |
| `series-multiple-selectors` | series | Repeated `match[]` array handling. |
| `instant-log-selector-rejected` | instant | Loki rejects a bare log selector at the instant endpoint with HTTP 400; `expect.status` pins it. |

Outside the corpus: tail (WebSocket, not an HTTP compare), the RIGHT-only
unsupported probes of
[Deferred with `ErrUnsupported`](../query/logql.md#deferred-with-errunsupported), and `volume_range` static timestamp expectations
(the deterministic generators start at `time.Now`, so those belong to unit
tests or runtime-generated corpora).

## Unverified and next evidence

- Live browser capture 2026-11-04T07:42:24Z: Grafana 13.3.0 with the Loki
  datasource plugin 13.2.5 against Loki 3.7.9 (HAR
  `/tmp/sol-b/grafana-explore.har`, histogram request
  `/tmp/sol-b/grafana-histogram-request.json`, route evidence
  `/tmp/sol-b/loki-histogram-route-evidence.json`, screenshot
  `/tmp/sol-b/grafana-explore-loki.png`). This is a Loki baseline request
  capture, not Parkana compatibility verification; feature rows stay pending
  until exercised against Parkana. It shows intervalMs 5000 for the logs
  browser and 60000 for the histogram, with `$__auto` interpolated to `[1m]`
  (`Step: 1m0s`) in the Grafana datasource backend before Loki and no
  `index/volume` request issued. The captured bounds span one hour; the
  corpus carries no bounds and replays with `-start` exactly one hour before
  `-end` (absolute RFC3339 or Unix nanoseconds, both accepted by the CLI).
  Everything request-shaped outside this capture remains source-verified
  from v13.2.5.
- Integrated parity run on combined stack `c41c277` (part A `192b27f` plus
  the approved IncludeMeta amendment); evidence produced by sol-b and
  reviewed by the orchestrator. Pinned stack: Garage 2.5.0, Loki 3.7.9,
  Alloy 1.21.3, Grafana 13.3.0, datasource plugin 13.2.5 (runtime-reported);
  22,200 generated/Loki/catalog rows. The compiled `lokicompare` ran the
  then-current 18-entry default corpus once: **14 EQUAL, 0 DIFF, 4 failed,
  exit 2**. Every failure was the expected Parkana `400` "metric and scalar
  queries are not implemented yet" (the run predates A7) on
  `count-instant-by-level`, `grafana-histogram-api`,
  `grafana-histogram-corpus-json` and `grafana-histogram-corpus-logfmt`.
  `EQUAL` covered all six log entries (`logs-base-api-range`,
  `logs-base-web-line-filter`, `logs-job-only-legacy`,
  `logs-corpus-json-parser-filter`, `logs-corpus-logfmt-line-filter`,
  `logs-corpus-plain-regexp`), all labels/label-values/series entries and
  `instant-log-selector-rejected` (both sides `400`). Separately, a Grafana
  Explore `{service_name="api"}` query returned 1,000 actual rows per
  datasource with frame data equal except Grafana-assigned frame IDs, a
  Grafana-rendered capture rather than a lokicompare `EQUAL`
  (`grafana-summary.json`; screenshots and HAR under `/tmp/sol-b/`).
- Follow-up parity run on combined stack `a89e876` (includes A7 `e117851`
  and A8a `20c9300`); evidence produced by sol-b and reviewed by the
  orchestrator. The compiled `lokicompare` ran the same corpus: **14 EQUAL,
  4 DIFF, 0 failed**. All metric values were correct except a known retry
  overcount; the remaining differences were sample millisecond rounding and
  range step alignment, both fixed in A9 and awaiting a live re-run. The
  retry overcount is tracked separately under the `plan.Source` dedup
  invariant, not A9. Live tail was verified in the same run: both Grafana
  Explore Live sockets saw exactly the same 24 marked rows through Alloy
  with equal timestamps, labels and lines
  (`/tmp/sol-b/a7-tail-parity/summary.md`, `tail-summary.json`,
  `/tmp/sol-b/grafana-a7-parkana-live.png`,
  `/tmp/sol-b/grafana-a7-loki-live.png`, `/tmp/sol-b/grafana-a7-tail.har`).
- Grafana datasource health check: the plugin issues the instant query
  `vector(1)+vector(1)`; Loki returned `200`/OK and Parkana `400`
  (`vector() is not supported`) in the `c41c277` run
  (`grafana-health-logs.json`). `vector(NUMBER)` plus the default
  identical-label vector/vector arithmetic case is implemented and
  unit-tested under A9, and the current binary answers
  `vector(1)+vector(1)` at `/loki/api/v1/query` with `200` and the value `2`,
  even with no data. A live re-run of the health check through Grafana is not
  recorded.
- Runtime plugin version must be read from Grafana's plugin API in every e2e
  run and recorded with the Grafana version.
- Public tail WebSocket: the ordinary Grafana Live path is live-verified on
  `a89e876` as recorded above. `TypeDropped` handling, default
  `limit`/`delay_for` and visible-gap close codes are implemented and
  unit/integration-tested (M4), not exercised live.
- Exact `step` values depend on panel width and max data points; the corpus
  pins the captured `5s` and `1m` forms, not every Grafana possibility.
- Label-name feature-toggle path (`labels` vs `series`) is source-verified;
  which path the deployed Grafana 13.3.0 uses depends on its feature-toggle
  configuration and must be recorded during live verification.
- Index stats/volume entries in the corpus apply the
  [Stats and volume](../query/logql.md#stats-and-volume) policy rather than comparing
  implementation-specific chunk or byte estimates. A checked-in default
  corpus cannot carry static `volume_range` expectations because the
  deterministic generators start at `time.Now`; those belong to unit tests
  or runtime-generated corpora.
- All corpus expressions were checked against Loki 3.7.9's pinned grammar
  (`pkg/logql/syntax/syntax.y`): `logRangeExpr`, `rangeAggregationExpr`,
  `vectorAggregationExpr`, `pipelineStage` (line filter, JSON expression
  parser, label filter, drop), `labelFilter`/`numberFilter` and
  `jsonExpressionParser`. See the source list below.
- Baseline histogram rendering in Grafana was captured against Loki 3.7.9;
  dashed "absent" series and legend behavior remain pending. The `a89e876`
  metric divergences (millisecond rounding and step alignment) are fixed in
  A9 pending a live re-run; the retry overcount is tracked under the
  `plan.Source` dedup invariant.

## Primary sources

- Grafana Loki datasource v13.2.5, data request builder:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/api.go#L46-L107>
- Query model defaults (direction, query type, `$__auto`):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/parse_query.go#L34-L100>
- Supplementary logs-volume expression:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L238-L267>
- Live query URL construction:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L407-L442>
- Time bounds (ms × 1e6):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/datasource.ts#L503-L505>
- Label names, values and series:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L156-L255>
  and
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LanguageProvider.ts#L408-L431>
- Resource path prefix `/loki/api/v1/`:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/pkg/loki/loki.go#L127-L171>
- Histogram suitability and the Lezer syntax-error check
  (`isQueryWithError`):
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/queryUtils.ts#L209-L216>
- Loki 3.7.9 grammar: log ranges and pipelines
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L128-L155>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L215-L227>;
  aggregations
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L169-L185>;
  JSON expression parser
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L276-L281>;
  label and number filters
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L302-L312>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L352-L360>;
  drop labels
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/syntax/syntax.y#L371>
- Loki 3.7.9 drop stage keeps the row and clears the error:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/drop_labels.go#L29-L58>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/labels.go#L254-L257>
- Tail result transformer and dropped entries:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/liveStreamsResultTransformer.ts#L16-L45>
- Tail retry behavior:
  <https://github.com/grafana/grafana-loki-datasource/blob/v13.2.5/src/LiveStreams.ts#L46-L65>
- Loki 3.7.9 scalar timestamp unit error and proto overflow:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/prometheus.go#L163-L175>
  and
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/codec.go#L1761-L1772>
- Loki 3.7.9 volume_range result-shape rule:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/querier/queryrange/volume.go#L158-L165>;
  bucket alignment and millisecond rounding:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/util/time.go#L94-L119>
  and <https://github.com/grafana/loki/blob/v3.7.9/pkg/util/conv.go#L29-L39>
- Loki 3.7.9 unshardable stddev/stdvar mapping:
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/shardmapper.go#L238-L343>
  with the range-downstream path at
  <https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/shardmapper.go#L436-L451>
