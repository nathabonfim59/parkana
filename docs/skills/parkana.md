# Parkana query skill

The `parkana` skill teaches an agent to query a running Parkana with LogQL
and SQL in ways that read few files, and to stay read-only while doing it. It
is new in 1.2.0. Its source is
[`skills/parkana/SKILL.md`](../../skills/parkana/SKILL.md), with a helper
script in [`skills/parkana/scripts/parkana-query.sh`](../../skills/parkana/scripts/parkana-query.sh).

```sh
npx skills add nathabonfim59/parkana/skills/parkana
```

`npx skills add nathabonfim59/parkana --skill parkana` installs the same
skill. [AI skills](index.md#install) lists the other options.

## When to use it

Use it when you ask an agent to search logs in Parkana, count errors, find
which streams exist, or find out why a query is slow or times out. The rules
it follows are those of [Query best practices](../query/best-practices.md),
in a shorter form.

## What it asks for

The skill reads three environment variables. They are read by the agent and
by the helper script only; the Parkana server does not read them.

| variable | default | meaning |
|---|---|---|
| `PARKANA_URL` | `http://localhost:3100` | base URL of a Parkana querier |
| `PARKANA_CURL_CONFIG` | none | a curl config file with credentials, passed to curl with `-K` |
| `PARKANA_MAX_WINDOW` | `6h` | the longest window the helper accepts |

Parkana does not authenticate requests. When yours sits behind a proxy that
does, put the header in a curl config file readable only by you, for example
`header = "Authorization: Bearer <token>"`, and point `PARKANA_CURL_CONFIG` at
it. The skill never puts a credential on a command line.

## What it does

- It checks `/ready` first.
- It discovers streams and label values in a narrow window, and uses
  `/loki/api/v1/index/stats` to see how many files a selector reads before it
  searches.
- It sends `query_range` with explicit `start`, `end`, `limit` and
  `direction`, and searches long periods as a sequence of bounded windows,
  newest first.
- It puts a selective `|=` literal of at least 4 bytes in front of every
  search, splits an OR into separate queries, and filters before it parses.
- It keeps SQL windows short, because a `WHERE` does not reduce the files
  `/api/v1/sql` reads.
- It treats an empty result over a completed window as evidence of no match,
  and a timeout as no evidence.

The helper script runs one `query_range` and refuses a window longer than
`PARKANA_MAX_WINDOW`, a window in the future, and a query without a `|=`
needle of 4 bytes or more. `--lock <file>` makes concurrent runs wait for
each other (it needs `flock`). It needs bash, curl and GNU `date`. Run it
from the folder the skills CLI installed it into:

```sh
scripts/parkana-query.sh --start 2026-11-24T10:00:00Z --end 2026-11-24T11:00:00Z \
  '{service_name="checkout"} |= "timeout"'
```

## What it will not do

Unless you ask for that exact change, it does not push logs, create, change
or delete pipelines, or call any other route that writes. It does not run
one query over days or weeks, and it does not report a timeout as "no logs".

## Example prompts

- "Find the checkout errors with `timeout` in the last hour in Parkana."
- "Which services sent logs to Parkana in the last 30 minutes?"
- "Count the error lines per level for the checkout service between 10:00
  and 11:00 UTC today, with SQL."
- "This LogQL query times out on Parkana. Make it cheaper:
  `{job="api"} |~ "(?i)refused"`."
