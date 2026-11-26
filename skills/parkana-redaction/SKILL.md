---
name: parkana-redaction
description: Find secrets and personal data (passwords, tokens, emails, card numbers, IP addresses) in a Parkana log store's streams or in local log samples, report them per stream without printing a raw value, and build, dry-run and (only after the user confirms each one) create or update one ingest redaction pipeline per stream. Use when asked to redact, mask or anonymize logs in Parkana or to check logs for leaked secrets or PII.
---

# Parkana redaction

Parkana can rewrite log entries with an ingest pipeline before it stores
them. This skill samples each stream, finds what should not be stored, and
builds one redaction pipeline per stream, checked with the server's dry-run
before anything is created.

The full explanation (what an ingest pipeline does and does not protect,
step order, labels, the pattern catalog and its false positives, rollout) is
in [Redact and anonymize at ingestion](https://nathabonfim59.github.io/parkana/pipelines/redaction).
The pattern catalog is `patterns.json` next to this file; the page's table is
generated from it.

## Setup

- `PARKANA_URL` is the base URL, for example `http://localhost:3100`.
- If the server sits behind authentication, keep the credentials in a curl
  config file and set `PARKANA_CURL_CONFIG` to its path (the helper passes it
  with `-K`). Never put a token or password on the command line, in a script
  argument or in a log. A curl config file holds one option per line, for
  example `header = "Authorization: Bearer <token>"`; make it readable only
  by you. Put `header = "X-Scope-OrgID: <tenant>"` there too in multi-tenant
  mode.
- The helper needs `bash`, `curl` and `jq`.

```sh
export PARKANA_URL=http://localhost:3100
curl -fsS "$PARKANA_URL/ready"   # prints "ready"
```

`scripts/redaction-scan.sh` in this skill does the sampling, scanning and
checking. Run it with `--help`. Every pattern runs inside Parkana through
`POST /api/v1/pipelines/dry-run`, so the matches are exactly what the ingest
pipeline will do, and nothing is written to Parkana.

## What you will and will not do

- Never print, quote or summarize a raw log line or value. Show only the
  helper's counts and its masked examples. Do not open sample files, do not
  `cat`, `head` or `grep` them, and do not run `query_range` yourself for
  sampling: the helper writes the lines straight to a file.
- Read only until the user confirms. Do not push logs. Create or update a
  pipeline only after the user has seen its steps and its check result and
  said yes to that one pipeline. Never delete a pipeline unless the user asks
  for that exact deletion.
- Keep every query bounded: explicit `start` and `end`, one hour at most per
  sample (`--max-window`), at most 1000 entries, newest first.
- Delete the sample files when you are done; they hold the raw data.

## Runbook

1. Check `/ready`.
2. Find the streams. Ask the user which ones to scan, or list the dataset
   names of the last hour (labels routes over wide windows are expensive):

   ```sh
   curl -fsS -G "$PARKANA_URL/loki/api/v1/label/service_name/values" \
     --data-urlencode start=2026-11-24T10:00:00Z --data-urlencode end=2026-11-24T11:00:00Z
   ```

   The user can also give local sample files (one log line per line) from
   the shipper; use `import` for those.

3. List the pipelines that already exist, with their stage, selector and
   version:

   ```sh
   curl -fsS "$PARKANA_URL/api/v1/pipelines" | jq -c '.pipelines[] | {name, stage, selector, version}'
   ```

   If a read pipeline matches a stream (selector `*` or the stream), the
   samples you read are already masked by it, while the stored data is not.
   Tell the user, and scan samples from the shipper for that stream instead.

4. Sample each stream into a private directory. Take one or two windows (for
   example the last 15 minutes, and a busy hour), 200 to 1000 entries each:

   ```sh
   dir=$(mktemp -d)
   scripts/redaction-scan.sh sample --selector '{service_name="checkout"}' \
     --start 2026-11-24T10:45:00Z --end 2026-11-24T11:00:00Z --limit 500 \
     --out "$dir/checkout.jsonl"
   # or, for a local file:
   scripts/redaction-scan.sh import --stream checkout --in ./checkout.log --out "$dir/checkout.jsonl"
   ```

   `--needle password` adds `|= "password"`, which reads only files that can
   contain it; use it to look for a specific leak in a busy stream. The
   helper refuses windows longer than `--max-window` (default 1h), windows in
   the future and metric queries.

5. Scan each sample and report per stream:

   ```sh
   scripts/redaction-scan.sh scan --in "$dir/checkout.jsonl" --examples 5
   ```

   It prints how many lines, metadata values and labels each catalog
   pattern matches, which field names look sensitive (`password`, `token`, `secret`,
   `authorization`, `email`, `national_id` and others, from `field_names` in
   `patterns.json`) and how many of their values no pattern masked, and up
   to N examples masked by all of that plus a mask for long token-like
   strings. Give the user that report as it is. A field with `unmasked`
   above zero is something the catalog does not recognize: ask whether it
   is sensitive. Point out the false-positive risk of what matched (see the
   `false_positives` of each pattern in `patterns.json`), for example epoch
   milliseconds matching `card`.

6. Build one pipeline per stream:

   ```sh
   scripts/redaction-scan.sh propose --in "$dir/checkout.jsonl" \
     --name 00-redact-checkout --selector checkout > "$dir/checkout-redact.json"
   ```

   It takes the patterns that matched and the sensitive fields the catalog
   left unmasked; `--patterns id,...` and `--fields name,...` override that
   (use the user's choices, such as `card-keep-last4` instead of `card`).
   Every step has `"on_error": "drop"`, so a step that fails never stores
   the unredacted entry. Rules for the definition:

   - The `selector` is the stream's dataset name, never `*` unless the user
     asks for one pipeline for every stream.
   - Redact steps come first. If the stream already has an ingest pipeline
     that parses or promotes, keep it and name the redaction pipeline so it
     sorts before it: matching pipelines run in byte order of `name`, and
     `00-redact-checkout` runs before `checkout-parse`. A `*` pipeline whose
     name sorts earlier still runs first; tell the user if one exists.
   - At most 32 steps per pipeline.
   - A `redact` step cannot change a stream label. `scan` reports labels as
     `label:<name>`, and `propose` warns about them instead of adding steps.
     Tell the user to drop such a label at the shipper or, at ingest, to
     overwrite it with a constant, which you add only if they agree:
     `{"type": "promote", "value": "\"\"", "to": "<label>", "when": "labels.<label> != \"\""}`
     (without the `when`, every entry gets the empty label).

7. Check it with dry-run over the same samples:

   ```sh
   scripts/redaction-scan.sh check --def "$dir/checkout-redact.json" --in "$dir/checkout.jsonl"
   ```

   It prints how many entries were dropped or got `pipeline_error`, what the
   catalog and the field names still find after the pipeline, and masked
   examples. Exit status 3 means something sensitive is left: fix the
   definition and check again. Dropped entries or `pipeline_error` above
   zero also need a fix before you go on.

8. Show the user, for that one pipeline: its name and selector, the list of
   steps (patterns and replacements are fine to show), the check summary and
   the masked examples. Ask for a yes for that pipeline. Then:

   Create (the answer holds `"version": 1`):

   ```sh
   curl -fsS -X POST "$PARKANA_URL/api/v1/pipelines" \
     -H 'Content-Type: application/json' --data-binary @"$dir/checkout-redact.json"
   ```

   Update an existing pipeline: read it, show the user the old and new
   steps, and send the version you read.

   ```sh
   curl -fsS "$PARKANA_URL/api/v1/pipelines/00-redact-checkout" | jq '.version'
   jq --argjson v 3 '. + {version: $v}' "$dir/checkout-redact.json" > "$dir/checkout-redact-v.json"
   curl -fsS -X PUT "$PARKANA_URL/api/v1/pipelines/00-redact-checkout" \
     -H 'Content-Type: application/json' --data-binary @"$dir/checkout-redact-v.json"
   ```

   A `409` means someone else changed the pipeline since you read it (or,
   on create, that the name exists). Do not retry blindly: read it again,
   show the user what changed, run `check` on the merged definition and ask
   again. A `400` is a validation error; its text says which step.

9. Verify. The ingester reloads pipelines within about one poll interval
   (1s by default) plus one reload. Sample a window that starts after the
   change and run `scan` on it: the patterns you redacted should report 0.
   A placeholder such as `<email>` is a good `--needle` for finding the
   redacted lines.

10. Repeat for the next stream, then delete the sample directory:
    `rm -rf "$dir"`.

## What to tell the user

- Only new entries are redacted. Stored entries keep the original values
  until `-compactor.retention` deletes them; a read pipeline only masks
  query results, not SQL, the metadata routes or the files.
- Parkana anonymizes (fixed placeholders, partial masks such as the last four
  card digits); it has no hash or tokenize step, so it cannot replace a value
  with a stable pseudonym.
- Regexps find shapes. They miss secrets without a known shape and mask
  some harmless text; the scan's counts are evidence for these samples, not
  proof for the whole stream.
- Anyone who can call `/api/v1/pipelines` can change or delete a redaction
  pipeline; that route belongs on a private network.
