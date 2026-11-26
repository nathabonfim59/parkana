# Redact and anonymize at ingestion

An ingest pipeline can remove secrets and personal data from log entries
before Parkana stores them. This page says where the unredacted data still
exists, how to write `redact` steps and in which order, gives a pattern
catalog whose every pattern is tested, and shows how to roll redaction out
one stream at a time. The step itself is described in [Steps](steps.md); the
routes in [HTTP API](api.md).

## Ingest or read pipeline

Use an ingest pipeline (`"stage": "ingest"`). It runs in the ingester after
the push is decoded and validated and before the entry is appended to the
WAL, so the WAL, the local Parquet files, the object store, the search
sidecars and the catalog label summaries only ever see the redacted entry.
Live tail (`GET /internal/ingest/tail` and the Loki tail WebSocket) also
shows the entry after the ingest pipelines ran.

A read pipeline (`"stage": "read"`) only masks rows at query time. The
stored files keep the original line, and read pipelines do not run for
`/api/v1/sql`, the metadata routes (`labels`, `label/<name>/values`,
`series`, index stats and volume) or live tail (see
[Read semantics](read.md)). Anyone who can reach those routes, or the object
store, sees the original data. A read pipeline is a stopgap for data that is
already stored, not a redaction.

Pipelines never rewrite stored data. Entries stored before you enable a
pipeline stay as they were until `-compactor.retention` deletes them.

## Where the original data still exists

Even with an ingest pipeline, the original entry exists in these places:

- In the ingester's memory while the push request is handled: the request
  body, the decompressed body and the decoded entries.
- In the error answer to the client. A push that fails validation answers
  400 with the error text; a label syntax error quotes the label set it
  could not parse. That answer goes only to the client that sent the push.
- In `pipeline_error_details`, a metadata field a pipeline stores when a step
  fails under `on_error: keep`. The message can quote part of the data, for
  example the first bare word of a line `parse_logfmt` could not parse. Redact
  before any step that can fail, as described below.
- In the body of a dry-run request. Dry-run writes nothing to the WAL,
  catalog or object store and returns the result to the caller.
- At the shipper, in its buffers, retries and its own logs, and on the
  network between the shipper and Parkana when the push is plain HTTP
  (see [Security](../security.md)).

Parkana does not log request bodies or log lines. The request log (debug
level, and warn for 5xx answers) has the method, path, route, status,
duration and tenant. The pipeline API logs the error, method, path and
tenant of an internal error, never a definition or an entry.

Whoever can call `/api/v1/pipelines` can also delete a redaction pipeline.
Keep that route on the private network as [Security](../security.md#routes-by-class)
describes.

## The redact step

```json
{"type": "redact", "pattern": "(?i)\\b(password=)[^\\s&]+", "replace": "${1}***", "on_error": "drop"}
```

- `pattern` is a Go RE2 regexp of at most 1024 bytes. RE2 has no lookahead,
  lookbehind or backreferences, and a match takes time linear in the input.
- Every match is replaced. `replace` is a Go replacement template of at most
  256 bytes: `$1` or `${1}` inserts the first capture group, `${name}` a
  named group, `$$` a literal `$`. Write `${1}` when letters, digits or `_`
  follow the reference: `$1x` means the group named `1x`, which is empty.
  Without `replace` the match becomes `***`; `"replace": ""` deletes it.
- `target` is the line (the default) or one metadata field, such as an OTLP
  log attribute. A missing field or no match changes nothing. One step
  rewrites one target; repeat the step for more fields.
- The step fails when the result would exceed the line limit
  (`-ingest.max-line-size`, 256 KiB by default) or the 16 KiB metadata value
  limit, or when the matches in one target exceed the match-index budget
  of 1,048,576 ints (each match takes a few ints per capture group, so this
  takes on the order of 100,000 matches). Under the default `on_error: keep` a failed step restores the
  **unredacted** value and the entry is stored. Set `"on_error": "drop"` on
  redaction steps so a failure drops the entry instead, or `"reject"` to
  fail the whole push with 400.

Every step in the catalog below sets `"on_error": "drop"`. A redact step
that does not match is not a failure.

## Step order

Put the `redact` steps first, before `parse_json`, `parse_logfmt`,
`regex_extract`, `rename` and `promote`. The compiler does not enforce an
order; these two dry-runs show why it matters.

A parser copies the values it extracts into metadata. Redacting the line
afterwards leaves the copy:

```json
{"steps": [
  {"type": "parse_json"},
  {"type": "redact", "pattern": "(\"password\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\"", "replace": "${1}\"***\""}
]}
```

```text
line: {"user":"alice","password":"hunter2"}
out:  line {"user":"alice","password":"***"}
      meta {"user":"alice","password":"hunter2"}
```

A parser that fails under `on_error: keep` skips every later step, so the
redact never runs and the original line is stored:

```text
steps: parse_logfmt, then redact "hunter2"
line:  login user=alice hunter2
out:   line login user=alice hunter2
       meta pipeline_error=parse_logfmt, pipeline_error_details=key "login" has no '='
```

With the redact steps first, the parser reads the redacted line, so the
extracted fields hold the placeholders and an error message can only quote
redacted text. When metadata arrives already set (Loki structured metadata,
OTLP log and resource attributes), add a `redact` with that field as
`target`. OTLP attribute names arrive normalized: `user.email` is the
metadata field `user_email`.

## Labels

A `redact` step cannot target a stream label. Labels carry the most risk:
they are stored in every file of the stream, in the catalog label summaries
and in every `series` and `labels` answer. Keep personal data out of labels
at the shipper.

At ingest a `promote` step can overwrite a label with a constant. This
step blanks `user_email`; the `when` keeps it from adding an empty
`user_email` to entries that do not have one:

```json
{"type": "promote", "value": "\"\"", "to": "user_email", "when": "labels.user_email != \"\""}
```

A push with `{service_name="web", user_email="jane@example.com"}` through it
is stored as `{service_name="web", user_email=""}`. A `promote` value is an
expression without regexp replacement, so a label can be replaced, not
partly masked. The dataset is still chosen from the original routing label
(`-stream.labels`), and read pipelines cannot change labels at all.

Grafana Alloy, Loki push: drop the labels before `loki.write`.

```hcl
loki.process "drop_pii_labels" {
  forward_to = [loki.write.parkana.receiver]

  stage.label_drop {
    values = ["user_email", "client_ip"]
  }
}
```

OpenTelemetry Collector: delete the attributes before the exporter. Only the
resource attributes listed in [OTLP](../ingest.md#otlp) become labels; every
other attribute becomes metadata, which a `redact` step with `target` can
also handle.

```yaml
processors:
  resource/drop_pii:
    attributes:
      - key: user.email
        action: delete
  attributes/drop_pii:
    actions:
      - key: client.address
        action: delete

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [resource/drop_pii, attributes/drop_pii]
      exporters: [otlphttp/parkana]
```

## Pattern catalog

Each pattern below is a `redact` step. The catalog lives in
[`skills/parkana-redaction/patterns.json`](../../skills/parkana-redaction/patterns.json),
which the [redaction skill](../../skills/parkana-redaction/SKILL.md) reads
too. `make check` runs every pattern and every example in that file through
the pipeline compiler and executor, and fails when this table differs from
the file; `make docs-redaction` rewrites it.

Regexps find shapes, not meaning. Every pattern has false positives (masked
text that was not sensitive) and misses secrets that do not have its shape,
so check each one on your own lines with a dry-run. Patterns run in step
order: put the specific ones (tokens, named secrets) before the broad ones
(digit runs, addresses).

<!-- redaction-patterns:begin (generated by make docs-redaction from skills/parkana-redaction/patterns.json; do not edit) -->

| pattern | finds | example | false positives |
|---|---|---|---|
| `jwt` | JSON Web Tokens (three base64url parts, the first two start with eyJ) | `auth ok token=eyJhbGciOiJFUyJ9.eyJzdWIiOiIxIn0.EXAMPLEsignature user=7` becomes `auth ok token=<jwt> user=7` | Rare: any word starting eyJ followed by a dot and another eyJ part. |
| `bearer` | Bearer tokens after the word Bearer (any case), 16 characters or more | `Authorization: Bearer 3f9c2a7b1d4e8f60a5b2c9d7e1f40a3b` becomes `Authorization: Bearer ***` | Prose such as "bearer" followed by a long word; the 16 character minimum keeps most prose intact. |
| `basic-auth` | HTTP Basic credentials in an Authorization header or field | `Authorization: Basic dXNlcjpzZWNyZXQ=` becomes `Authorization: Basic ***` | None seen; the match needs the authorization name in front. |
| `url-userinfo` | Passwords in URLs (scheme://user:password@host) | `connecting to postgres://app:s3cr3t@db.example.com:5432/app` becomes `connecting to postgres://app:***@db.example.com:5432/app` | None seen; a URL without @ is never touched. |
| `secret-kv` | Secrets in key=value pairs (logfmt, query strings): password, passwd, pwd, secret, token, api_key, apikey, access_key, client_secret | `level=info msg="login" user=alice password=hunter2 ok=true` becomes `level=info msg="login" user=alice password=*** ok=true` | A key such as secret=false or token=none is masked too. Keys that only start with a listed name (token_count) are kept, because the name must be followed by =. |
| `secret-json` | Secrets in JSON string members with the same names, plus authorization; the result stays valid JSON | `{"user":"alice","password":"hu\"nter2","ok":true}` becomes `{"user":"alice","password":"***","ok":true}` | Only string values are masked; a number or object under a secret name is left as it is. |
| `access-key-id` | Access key ids with the AKIA or ASIA prefix and 16 more upper-case letters or digits | `using key AKIAIOSFODNN7EXAMPLE for upload` becomes `using key <access-key-id> for upload` | None seen. The secret key that goes with it has no prefix; catch it by name with secret-kv or secret-json. |
| `prefixed-token` | Tokens with a well-known prefix: ghp_, gho_, ghu_, ghs_, ghr_, github_pat_, xoxb-, xoxp-, xoxa-, xoxr-, xoxs-, sk_live_, sk_test_, rk_live_, rk_test_, AIza | `clone with ghp_000000000000000000000000000000000000 done` becomes `clone with <token> done` | None seen; each shape has a fixed prefix and a minimum length. |
| `email` | Email addresses | `sent receipt to jane.doe+shop@example.com ok` becomes `sent receipt to <email> ok` | Anything shaped like an address: git remotes (git@example.com:repo) and principal names such as user@EXAMPLE.COM are masked too. |
| `email-keep-domain` (variant of `email`) | Email addresses, keeping the domain | `sent receipt to jane.doe@example.com` becomes `sent receipt to ***@example.com` | As email. |
| `card` | Payment card numbers: 13 to 19 digits, optionally grouped with spaces or dashes | `charged card 4111 1111 1111 1111 amount=10` becomes `charged card <card> amount=10` | Any long digit run: epoch milliseconds (13 digits), order ids, phone numbers without a plus. RE2 cannot check the Luhn digit. Anchor it to a field name (card=, pan=) where you can. |
| `card-keep-last4` (variant of `card`) | Payment card numbers, keeping the last four digits | `charged card 4111 1111 1111 1234 amount=10` becomes `charged card ****1234 amount=10` | As card. |
| `ipv4` | IPv4 addresses | `client=203.0.113.42 status=200` becomes `client=<ipv4> status=200` | Four-part version numbers such as 1.2.3.4 and dotted object identifiers. |
| `ipv4-keep-net24` (variant of `ipv4`) | IPv4 addresses, zeroing the last octet (keeps the /24 network) | `client=203.0.113.42 status=200` becomes `client=203.0.113.0 status=200` | As ipv4. |
| `ipv6` | IPv6 addresses, full and compressed (::) | `client=2001:db8:85a3:0:0:8a2e:370:7334 ok` becomes `client=<ipv6> ok` | Hex words joined by :: (code paths such as add::new). Run ipv4 first: an IPv4-mapped address ::ffff:192.0.2.1 then becomes &lt;ipv6&gt;:&lt;ipv4&gt;. |
| `phone-e164` | Phone numbers in E.164 form (+ and 8 to 15 digits). An example: national formats vary, write your own for them | `sms to +12025550143 sent` becomes `sms to <phone> sent` | A plus followed by eight or more digits in another meaning. |
| `national-id-example` | An example national identifier with a fixed shape (NNN-NN-NNNN). Replace the shape with your country's | `applicant id 123-45-6789 verified` becomes `applicant id <national-id> verified` | Any number with the same grouping. |

The step of each pattern, ready to paste into `steps`:

`jwt`:

```json
{"type":"redact","pattern":"\\beyJ[A-Za-z0-9_-]+\\.eyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]*","replace":"<jwt>","on_error":"drop"}
```

`bearer`:

```json
{"type":"redact","pattern":"(?i)\\b(bearer\\s+)[A-Za-z0-9._~+/-]{16,}=*","replace":"${1}***","on_error":"drop"}
```

`basic-auth`:

```json
{"type":"redact","pattern":"(?i)(authorization[\"']?\\s*[:=]\\s*[\"']?basic\\s+)[A-Za-z0-9+/]+=*","replace":"${1}***","on_error":"drop"}
```

`url-userinfo`:

```json
{"type":"redact","pattern":"(://[^:/@\\s]+:)[^@/\\s]+@","replace":"${1}***@","on_error":"drop"}
```

`secret-kv`:

```json
{"type":"redact","pattern":"(?i)\\b((?:pass(?:word|wd)?|pwd|secret|client_secret|token|access_token|refresh_token|api[_-]?key|access[_-]?key)=)(?:\"(?:[^\"\\\\]|\\\\.)*\"|[^\\s&\"]+)","replace":"${1}***","on_error":"drop"}
```

`secret-json`:

```json
{"type":"redact","pattern":"(?i)(\"(?:pass(?:word|wd)?|pwd|secret|client_secret|token|access_token|refresh_token|api[_-]?key|access[_-]?key|authorization)\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\"","replace":"${1}\"***\"","on_error":"drop"}
```

`access-key-id`:

```json
{"type":"redact","pattern":"\\b(?:AKIA|ASIA)[A-Z0-9]{16}\\b","replace":"<access-key-id>","on_error":"drop"}
```

`prefixed-token`:

```json
{"type":"redact","pattern":"\\b(?:gh[pousr]_[A-Za-z0-9]{36,255}|github_pat_[A-Za-z0-9_]{22,255}|xox[abprs]-[A-Za-z0-9-]{10,}|[rs]k_(?:live|test)_[A-Za-z0-9]{16,}|AIza[A-Za-z0-9_-]{35})\\b","replace":"<token>","on_error":"drop"}
```

`email`:

```json
{"type":"redact","pattern":"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\\.)+[A-Za-z]{2,}","replace":"<email>","on_error":"drop"}
```

`email-keep-domain`:

```json
{"type":"redact","pattern":"[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\\.)+[A-Za-z]{2,})","replace":"***@${1}","on_error":"drop"}
```

`card`:

```json
{"type":"redact","pattern":"\\b(?:[0-9][ -]?){12,18}[0-9]\\b","replace":"<card>","on_error":"drop"}
```

`card-keep-last4`:

```json
{"type":"redact","pattern":"\\b(?:[0-9][ -]?){9,15}([0-9]{4})\\b","replace":"****${1}","on_error":"drop"}
```

`ipv4`:

```json
{"type":"redact","pattern":"\\b(?:(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\b","replace":"<ipv4>","on_error":"drop"}
```

`ipv4-keep-net24`:

```json
{"type":"redact","pattern":"\\b((?:(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\.){3})(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\b","replace":"${1}0","on_error":"drop"}
```

`ipv6`:

```json
{"type":"redact","pattern":"(?i)\\b(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}\\b|\\b(?:[0-9a-f]{1,4}:){1,6}(?::[0-9a-f]{1,4}){1,6}\\b|\\b(?:[0-9a-f]{1,4}:){1,7}:|::(?:[0-9a-f]{1,4}:){0,5}[0-9a-f]{1,4}\\b","replace":"<ipv6>","on_error":"drop"}
```

`phone-e164`:

```json
{"type":"redact","pattern":"\\+[1-9][0-9]{7,14}\\b","replace":"<phone>","on_error":"drop"}
```

`national-id-example`:

```json
{"type":"redact","pattern":"\\b[0-9]{3}-[0-9]{2}-[0-9]{4}\\b","replace":"<national-id>","on_error":"drop"}
```

Every pattern that is not a variant, in this order, as one pipeline for the `checkout` stream (`jwt`, `bearer`, `basic-auth`, `url-userinfo`, `secret-kv`, `secret-json`, `access-key-id`, `prefixed-token`, `email`, `card`, `ipv4`, `ipv6`, `phone-e164`, `national-id-example`):

```json
{
  "name": "checkout-redact",
  "stage": "ingest",
  "selector": "checkout",
  "steps": [
    {"type":"redact","pattern":"\\beyJ[A-Za-z0-9_-]+\\.eyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]*","replace":"<jwt>","on_error":"drop"},
    {"type":"redact","pattern":"(?i)\\b(bearer\\s+)[A-Za-z0-9._~+/-]{16,}=*","replace":"${1}***","on_error":"drop"},
    {"type":"redact","pattern":"(?i)(authorization[\"']?\\s*[:=]\\s*[\"']?basic\\s+)[A-Za-z0-9+/]+=*","replace":"${1}***","on_error":"drop"},
    {"type":"redact","pattern":"(://[^:/@\\s]+:)[^@/\\s]+@","replace":"${1}***@","on_error":"drop"},
    {"type":"redact","pattern":"(?i)\\b((?:pass(?:word|wd)?|pwd|secret|client_secret|token|access_token|refresh_token|api[_-]?key|access[_-]?key)=)(?:\"(?:[^\"\\\\]|\\\\.)*\"|[^\\s&\"]+)","replace":"${1}***","on_error":"drop"},
    {"type":"redact","pattern":"(?i)(\"(?:pass(?:word|wd)?|pwd|secret|client_secret|token|access_token|refresh_token|api[_-]?key|access[_-]?key|authorization)\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\"","replace":"${1}\"***\"","on_error":"drop"},
    {"type":"redact","pattern":"\\b(?:AKIA|ASIA)[A-Z0-9]{16}\\b","replace":"<access-key-id>","on_error":"drop"},
    {"type":"redact","pattern":"\\b(?:gh[pousr]_[A-Za-z0-9]{36,255}|github_pat_[A-Za-z0-9_]{22,255}|xox[abprs]-[A-Za-z0-9-]{10,}|[rs]k_(?:live|test)_[A-Za-z0-9]{16,}|AIza[A-Za-z0-9_-]{35})\\b","replace":"<token>","on_error":"drop"},
    {"type":"redact","pattern":"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\\.)+[A-Za-z]{2,}","replace":"<email>","on_error":"drop"},
    {"type":"redact","pattern":"\\b(?:[0-9][ -]?){12,18}[0-9]\\b","replace":"<card>","on_error":"drop"},
    {"type":"redact","pattern":"\\b(?:(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\b","replace":"<ipv4>","on_error":"drop"},
    {"type":"redact","pattern":"(?i)\\b(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}\\b|\\b(?:[0-9a-f]{1,4}:){1,6}(?::[0-9a-f]{1,4}){1,6}\\b|\\b(?:[0-9a-f]{1,4}:){1,7}:|::(?:[0-9a-f]{1,4}:){0,5}[0-9a-f]{1,4}\\b","replace":"<ipv6>","on_error":"drop"},
    {"type":"redact","pattern":"\\+[1-9][0-9]{7,14}\\b","replace":"<phone>","on_error":"drop"},
    {"type":"redact","pattern":"\\b[0-9]{3}-[0-9]{2}-[0-9]{4}\\b","replace":"<national-id>","on_error":"drop"}
  ]
}
```

What that pipeline does to two sample lines:

```text
before: level=info user=jane.doe@example.com client=203.0.113.42 password=hunter2 card=4111111111111111 auth="Bearer eyJhbGciOiJFUyJ9.eyJzdWIiOiIxIn0.EXAMPLEsignature"
after:  level=info user=<email> client=<ipv4> password=*** card=<card> auth="Bearer <jwt>"
before: {"user":"jane@example.com","password":"hunter2","peer":"::ffff:192.0.2.1","phone":"+12025550143"}
after:  {"user":"<email>","password":"***","peer":"<ipv6>:<ipv4>","phone":"<phone>"}
```

Field names that usually hold sensitive values (the skill uses this to pick candidates; it is not a pipeline step): `(?i)^(?:.*_)?(?:pass(?:word\|wd)?\|pwd\|secret\|token\|api_?key\|access_?key\|private_?key\|auth(?:orization)?\|cookie\|session(?:_?id)?\|e?mail\|phone\|mobile\|ssn\|national_?id\|tax_?id\|card(?:_?number)?\|pan\|cvv\|iban\|ip\|client_?ip\|remote_?addr\|birth_?date\|dob)(?:_.*)?$`

<!-- redaction-patterns:end -->

## Anonymization, not pseudonymization

Parkana has no step that hashes or tokenizes a value: `redact` writes a fixed
template, and the expression language has no hash function. So a pipeline
can anonymize (replace a value with a fixed placeholder such as `<email>`, or
keep part of it, such as the last four card digits or the /24 network of an
address), but it cannot pseudonymize (replace each value with a stable token,
so that the same user still correlates across lines).

Capture groups keep the part you choose:

| goal | `pattern` | `replace` | before | after |
|---|---|---|---|---|
| fixed placeholder | `[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}` | `<email>` | `jane@example.com` | `<email>` |
| keep the domain | `[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})` | `***@${1}` | `jane@example.com` | `***@example.com` |
| keep the last four digits | `\b(?:[0-9][ -]?){9,15}([0-9]{4})\b` | `****${1}` | `4111 1111 1111 1234` | `****1234` |
| keep the network | the `ipv4-keep-net24` pattern | `${1}0` | `203.0.113.42` | `203.0.113.0` |
| keep the key, drop the value | `(?i)\b(password=)[^\s&]+` | `${1}` | `password=hunter2` | `password=` |

If you need stable tokens, compute them at the shipper. A plain hash of a
value with few possible inputs (an email address, an IPv4 address, a phone
number) can be reversed by hashing every candidate, so use a keyed hash
there and keep the key out of the logs.

## Dry-run before you enable

`POST /api/v1/pipelines/dry-run` validates a candidate exactly like a write
and runs it over up to 1000 sample entries without writing anything. Send
real lines from the stream, for example the newest lines of a bounded window
(see [Query best practices](../query/best-practices.md)), and compare each
line before and after:

```sh
curl -sS -X POST "http://localhost:3100/api/v1/pipelines/dry-run" \
  -H 'Content-Type: application/json' --data-binary @- <<'EOF'
{
  "pipeline": {
    "name": "checkout-redact",
    "stage": "ingest",
    "selector": "checkout",
    "steps": [
      {"type": "redact", "pattern": "(?i)\\b((?:pass(?:word|wd)?|pwd|secret|client_secret|token|access_token|refresh_token|api[_-]?key|access[_-]?key)=)(?:\"(?:[^\"\\\\]|\\\\.)*\"|[^\\s&\"]+)", "replace": "${1}***", "on_error": "drop"},
      {"type": "redact", "pattern": "[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\\.)+[A-Za-z]{2,}", "replace": "<email>", "on_error": "drop"},
      {"type": "parse_logfmt"}
    ]
  },
  "entries": [
    {"line": "level=info msg=login user=jane@example.com password=hunter2", "labels": {"service_name": "checkout"}}
  ]
}
EOF
```

```json
{"entries":[{"line":"level=info msg=login user=<email> password=***","labels":{"service_name":"checkout"},"meta":{"level":"info","msg":"login","password":"***","user":"<email>"}}],"dropped":0}
```

Check three things in the answer: no original value is left in `line` or
`meta`, the lines that should not change did not change, and `dropped` is
zero (a dropped entry means a step failed).

## Cost

A redact step costs one regexp scan of its target per entry. Measured
through dry-run on one machine, 1000 entries of a 186-byte logfmt line per
request, median of 15 requests, minus a baseline pipeline whose one redact
never matches: the catalog patterns cost between about 0 and 45 µs per entry
each, and all 14 primary patterns in one pipeline about 260 µs per entry.
The numbers include HTTP noise of a few µs. Patterns that start with a
literal (`://`, `+`, a quote) are near the bottom because the scan skips to
the literal; case-insensitive patterns and patterns that start with a
character class are near the top. The cost grows with line length and with
the number of steps. Measure your own pipeline with the same dry-run on your
lines, and watch `parkana_pipeline_execution_duration_seconds` after you
enable it.

## Roll out per stream

Write one ingest pipeline per stream, with that stream's dataset name as
`selector`, so a change to one stream's redaction cannot affect another.
Use `"selector": "*"` only for patterns every stream needs. Matching
pipelines run in byte order of `name`, so a `*` pipeline named
`00-redact-common` runs before `checkout-parse`.

1. Take sample lines from the stream and run the candidate through dry-run.
2. Create it. The answer holds `"version": 1`.

   ```sh
   curl -sS -X POST "http://localhost:3100/api/v1/pipelines" \
     -H 'Content-Type: application/json' --data-binary @checkout-redact.json
   ```

3. Wait for the reload: each ingester picks the change up within about one
   `-pipeline.poll-interval` plus one reload (see
   [Reloading](index.md#reloading)). Then query lines pushed after that
   point and check that no original value is left.
4. To change it, fetch the live version, dry-run the new steps and send a
   `PUT` with that version. A `409` means someone changed the pipeline since
   you read it: fetch it again, review the change and retry with the new
   version. Versions never go back, even across delete and recreate.

   ```sh
   curl -sS "http://localhost:3100/api/v1/pipelines/checkout-redact"
   curl -sS -X PUT "http://localhost:3100/api/v1/pipelines/checkout-redact" \
     -H 'Content-Type: application/json' --data-binary @checkout-redact-v2.json
   ```

5. Repeat for the next stream.

Deleting a pipeline (`DELETE` with `{"version": n}`) stops the redaction for
new entries at the next reload. Entries stored while it ran stay redacted.
