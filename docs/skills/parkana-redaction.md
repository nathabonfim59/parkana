# Parkana redaction skill

The `parkana-redaction` skill teaches an agent to find secrets and personal
data in Parkana's log streams, report them without showing a raw value, and
build one ingest redaction pipeline per stream, created only after you
confirm it. It is new in 1.2.0. Its source is
[`skills/parkana-redaction/SKILL.md`](../../skills/parkana-redaction/SKILL.md),
with the pattern catalog
[`skills/parkana-redaction/patterns.json`](../../skills/parkana-redaction/patterns.json)
and a helper script in
[`skills/parkana-redaction/scripts/redaction-scan.sh`](../../skills/parkana-redaction/scripts/redaction-scan.sh).

```sh
npx skills add nathabonfim59/parkana/skills/parkana-redaction
```

`npx skills add nathabonfim59/parkana --skill parkana-redaction` installs the
same skill. [AI skills](index.md#install) lists the other options.

## When to use it

Use it when you want to know whether a stream stores passwords, tokens,
email addresses, card numbers or IP addresses, or when you want them masked
before Parkana stores them. What an ingest pipeline does and does not
protect, the step order and the catalog's false positives are explained in
[Redact and anonymize at ingestion](../pipelines/redaction.md); the skill
follows that page and reads the same catalog.

## What it asks for

The skill reads these environment variables. They are read by the agent and
by the helper script only; the Parkana server does not read them.

| variable | default | meaning |
|---|---|---|
| `PARKANA_URL` | `http://localhost:3100` | base URL of a Parkana process that serves the query and pipeline routes |
| `PARKANA_CURL_CONFIG` | none | a curl config file with credentials, passed to curl with `-K` |
| `PARKANA_MAX_WINDOW` | `1h` | the longest sample window the helper accepts |

It asks you which streams to scan, or for local sample files with one log
line per line. It needs bash, curl and jq. Parkana does not authenticate
requests; when yours sits behind a proxy that does, put the header in a curl
config file readable only by you and point `PARKANA_CURL_CONFIG` at it. The
skill never puts a credential on a command line.

## What it does

- It samples each stream with `query_range` in one bounded window (at most
  1000 entries, newest first), straight into a private file, or imports your
  sample file.
- It runs every catalog pattern over the samples through
  `POST /api/v1/pipelines/dry-run`, so the matches are those of a real
  ingest pipeline, and checks field names such as `password`, `token`,
  `secret`, `authorization`, `email` and `national_id` in the lines, in
  structured metadata and in labels.
- It reports per stream how many lines, metadata values and labels each
  pattern matches, which sensitive fields no pattern masked, and a few
  examples with everything it found masked.
- It builds one ingest pipeline per stream with the redact steps first,
  each with `"on_error": "drop"`, dry-runs it over the samples and checks
  that nothing sensitive is left.
- After you confirm that one pipeline, it creates it, or updates it with the
  version it read, and treats a `409` as a reason to read, check and ask
  again. Then it samples new entries to verify the result.

## What it will not do

It does not print, quote or summarize a raw log line or value. It does not
push logs, and it does not create, change or delete a pipeline that you have
not confirmed. It does not write a `redact` step for a stream label, which a
step cannot change: it tells you to drop the label at the shipper or offers
a `promote` step that overwrites it. It does not claim that old entries are
redacted: pipelines only change new entries.

## Example prompts

- "Check the checkout and payments streams in Parkana for leaked passwords
  and tokens."
- "Scan this sample file from our shipper for personal data and propose an
  ingest pipeline for the `web` stream."
- "Mask email addresses in the checkout stream but keep the domain."
- "Update the redaction pipeline of the checkout stream to keep only the last
  four card digits."
