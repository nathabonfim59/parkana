---
name: parkana
description: Query a Parkana log store (Loki-compatible HTTP API, Parquet files queried with DuckDB) with LogQL and SQL in ways that read few files. Use when searching logs, counting errors or exploring streams in Parkana, or when a Parkana query is slow or times out. Read-only by default.
---

# Parkana log queries

Parkana stores logs as Parquet files, one stream and at most one UTC hour per
file, and answers the Loki read API plus a SQL endpoint. It has no secondary
index. A query costs roughly the files it reads, and only four things skip
files: the time range, the stream, `=` on promoted labels, and `|=` needles
checked against per-file bloom filters. Write every query so those four do
the work.

The full rules, with the evidence behind them, are in
[Query best practices](https://nathabonfim59.github.io/parkana/query/best-practices).
This skill is the short form.

## Setup

- `PARKANA_URL` is the base URL, for example `http://localhost:3100`.
- If the server sits behind authentication, keep the credentials in a curl
  config file and pass it with `-K <file>` (or `-K -` to read it from
  stdin). Never put a token or password on the command line, in a script
  argument or in a log.
- Single tenant: send no `X-Scope-OrgID` header, or `default`.

`PARKANA_URL`, `PARKANA_CURL_CONFIG` (the curl config file) and
`PARKANA_MAX_WINDOW` (the helper's longest window) are read by you and by
`scripts/parkana-query.sh`, not by the Parkana server; setting them changes
nothing in Parkana itself.

```sh
export PARKANA_URL=http://localhost:3100
curl -fsS "$PARKANA_URL/ready"   # prints "ready"
```

A curl config file holds one option per line, for example
`header = "Authorization: Bearer <token>"`. Make it readable only by you.

## What you will and will not do

- Read only. Do not push logs, create, change or delete pipelines, or call
  any write route unless the user asks for that exact change.
- Always pass explicit `start`, `end`, `limit` and `direction`.
- Never run an open-ended query over days or weeks in one request. Search in
  bounded windows, newest first, and stop when you have the answer.

## Runbook

1. Check `/ready`.
2. Discover streams cheaply, in a narrow window (the last hour). Label,
   value and series routes read the files of their window; over a wide window
   they are expensive.

   ```sh
   curl -fsS -G "$PARKANA_URL/loki/api/v1/label/service_name/values" \
     --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z
   ```

3. Ask how many files a selector reads before you search it (`chunks` is the
   file count, before line filters):

   ```sh
   curl -fsS -G "$PARKANA_URL/loki/api/v1/index/stats" \
     --data-urlencode 'query={service_name="checkout"}' \
     --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z
   ```

4. Search with `query_range`, explicit bounds, a small limit and a direction:

   ```sh
   curl -fsS -G "$PARKANA_URL/loki/api/v1/query_range" \
     --data-urlencode 'query={service_name="checkout"} |= "timeout"' \
     --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z \
     --data-urlencode limit=100 --data-urlencode direction=backward
   ```

5. If you need more, move the window back one step and repeat. Widen the
   selector only when the narrow one found nothing.

`scripts/parkana-query.sh` in this skill runs step 4 with guards: it refuses
windows longer than a maximum, windows in the future and LogQL log queries
without a `|=` needle. Run it with `--help`.

## What prunes

| Query shape | Skips files |
|---|---|
| `start`/`end` | outside the range |
| `{service_name="x"}`: `=` on the `-stream.labels` labels, in priority order | of other streams |
| `{job="x"}` while `service_name` has priority | none |
| `=` on a promoted label (`level` by default) | whose complete label summary lacks the value |
| `=~`, `!=`, `!~` matchers | none |
| `\|= "literal"` of 4 bytes or more (the gram length) | that cannot contain it |
| several `\|=` | that miss any one of them |
| `\|~ "literal"`, `\|~ ".*literal.*"`, `\|~ "literal.*"` (Loki turns these into `\|=`) | same as `\|=` |
| other `\|~` regexps | none |
| `\|= "a" or "b"`, `\|~ "a\|b"`, `\|~ "x(a\|b)"` | none (an OR) |
| `(?i)` and other case-insensitive matches | none |
| `!=`, `!~`, needles shorter than 4 bytes | none |
| `\|=` after `\| json` or `\| logfmt` (before `line_format`) | same as `\|=` |
| label filters such as `\| json \| level="error"` | none (they run in Go) |
| any query of a tenant that has a read pipeline | no line filter prunes, in any stream |
| SQL `WHERE` | none: only `from`/`to` bound the read |

## Practical rules

- Put the most selective literal first as `|=`, then a `|~` for the pattern:
  `{service_name="checkout"} |= "timeout" |~ "time(out|d out)"`.
- Needles match bytes, case-sensitively. `|= "Timeout"` does not find
  `timeout`.
- An OR prunes nothing. For selective alternatives, run one `|=` query per
  alternative and merge the results.
- Filter before you parse; parsers, label filters and formatting run in Go
  over every row DuckDB returns, and they stop the limit from being pushed
  down.
- A metric query reads from `start` minus its `[range]` to `end`. Keep
  `[range]` and the window small; group with `sum by (...)` over few labels
  (more than 500 series answers 400).
- A missing or corrupt sidecar only makes a query slower, never wrong.
- An empty result over a window that completed is evidence that nothing
  matched. A timeout (HTTP 504) is not evidence of anything: shrink the
  window or narrow the selector and run it again.

## SQL

`POST /api/v1/sql` loads every row of every stream of the tenant in
`[from, to)` into a `logs` table before your statement runs, so `from` and
`to` decide the cost. A `WHERE` filters that table; it skips no files.

- Keep `from`/`to` short and repeat the window in `WHERE _ts`.
- Filter on `labels['service_name']` and promoted columns, select only the
  columns you need, and always set `LIMIT` and `limit`.
- Promoted labels can live in a column or in the `labels` map, so read them
  with `coalesce("level", labels['level'])`. Give it an alias that is not
  a column name (`AS lvl`, not `AS level`): `GROUP BY level` would group by
  the column, and the statement fails.
- Prefer LogQL with `|=` for searches over long periods; use SQL for
  aggregation over short windows.

```sh
curl -fsS -X POST "$PARKANA_URL/api/v1/sql" -H 'Content-Type: application/json' \
  --data-binary @- <<'EOF'
{"query": "SELECT coalesce(\"level\", labels['level']) AS lvl, count(*) AS n FROM logs WHERE labels['service_name'] = 'checkout' GROUP BY lvl ORDER BY n DESC LIMIT 10",
 "from": "2026-11-24T10:00:00Z", "to": "2026-11-24T11:00:00Z", "limit": 10}
EOF
```

## Checking what a query read

`/metrics` has `parkana_query_files_total{result="scanned"|"pruned_index"|"pruned_labels"}`
and `parkana_query_bytes_scanned_total`. They are process-wide: compare them
before and after one query on an otherwise idle querier. They do not count the
SQL endpoint.
