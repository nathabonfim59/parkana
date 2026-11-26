# Pipeline steps

A pipeline definition (see [Pipelines](index.md#definition-schema)) lists up
to 32 steps that run in order. This page lists every step type, how
extracted values and names are written, the expression language, the hard
limits, what happens when a step fails, and how `promote` relates to stored
columns.

Every step accepts `type`, and optionally `when` (a boolean expression; false
skips the step) and `on_error` (`keep`, `drop` or `reject`; default `keep`).
A member a step type does not use is a validation error.

| `type` | fields | behavior |
|---|---|---|
| `parse_json` | `from` (optional) | Parses a JSON **object** from the line (default) or from a metadata field. Extracted fields are merged into metadata, overwriting existing values with the same name. Invalid JSON, a non-object document, duplicate member names or trailing data is a data error. |
| `parse_logfmt` | `from` (optional) | Parses whitespace-separated `key=value` pairs from the line (default) or a metadata field. Values may be unquoted or double-quoted. The quoted form recognizes logfmt escapes: `\"`, `\n`, `\t` and `\r` use one backslash; `\\` (two backslashes) is a literal backslash. A bare word, empty key, unterminated quote or unknown escape is a data error. Later duplicate keys win. |
| `regex_extract` | `pattern` (required), `from` (optional) | Applies a Go RE2 pattern to the line (default) or a metadata field and merges the **first** match's named captures into metadata. At least one named group is required. No match is a data error; captures that did not participate are skipped. |
| `rename` | `from`, `to` (required) | Moves one metadata field to another metadata name. A missing source is a no-op. Read stages may rename metadata whose name coincides with a routing label: stream labels themselves are untouched. |
| `redact` | `pattern` (required), `replace` (optional), `target` (optional) | Replaces every match with a Go `regexp` replacement template. Default replacement is `***`; `"replace": ""` deletes matches. `target` is the line (default) or a metadata field; a missing target or no match is a no-op. |
| `drop_if` | `expr` (required) | Drops the entry when the boolean expression is true. |
| `promote` | exactly one of `from` (metadata field) or `value` (string expression), plus `to` (label name) | Copies a string into a stream label. A missing `from` field is a no-op; an empty value writes an empty label. **Ingest only.** |
| `set_stream` | exactly one of `from` (metadata field) or `value` (string expression) | Sanitizes the value and always writes it to the first configured routing label. An empty result routes to `default`, so a later routing label cannot take over by accident. **Ingest only.** |

`promote` and `set_stream` are rejected at the read stage at validation time.
`promote` and `rename` destinations must be legal metadata/label names
(`[A-Za-z_][A-Za-z0-9_]*`) and must not start with `__`, which is reserved.
A `promote` target must also not collide with a reserved column: `_ts`,
`line`, `labels` or `meta` (the base names compare case-insensitively), or any
name starting with `_`; and it must not differ only in case from an earlier
`promote` target of the same definition. At execution time, a `promote` target
that differs only in case from a label already present on the row is a data
error, so `on_error` applies (default `keep` marks the entry and stops its
steps). A promoted label may target a routing label.

### Extracted value representations

`parse_json` renders values as deterministic strings:

- Strings are unescaped and stay strings.
- Numbers, booleans and `null` keep their exact JSON spelling (`1e3` stays
  `1e3`, `9007199254740993` stays exact, `null` becomes the literal `null`).
- Arrays become compact JSON with no insignificant whitespace; numbers inside
  them keep their raw spelling.
- Nested objects are flattened into separate metadata leaves. Path segments
  are joined with `_`: `{"user":{"id":7}}` produces `user_id=7`.

### Extracted names

Extracted names (JSON member names, logfmt keys, regex capture names) are
sanitized for use as metadata keys, like Loki 3.7.9:

1. Leading and trailing whitespace is trimmed.
2. A leading ASCII digit gets a `_` prefix.
3. Every rune outside `A-Za-z0-9_` becomes `_`. `user.id` becomes `user_id`,
   `http-status` becomes `http_status`, `@timestamp` becomes `_timestamp`.
4. An automatic name that would still start with `__` gets the Parkana
   `extracted` prefix, so `__error__` becomes `extracted__error__`. This
   protection is Parkana-specific (Loki sanitizes the name but does not add
   it): pipeline steps never write `__`-prefixed metadata, which keeps LogQL
   metric queries valid. Incoming metadata is preserved; a step only changes
   names it writes itself.

Name sanitizing and nested flattening follow Loki 3.7.9
([`pkg/logql/log/util.go#L22-L38`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/util.go#L22-L38),
[`#L42-L66`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/util.go#L42-L66),
[`pkg/logql/log/parser.go#L103-L116`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/parser.go#L103-L116),
[`#L213-L228`](https://github.com/grafana/loki/blob/v3.7.9/pkg/logql/log/parser.go#L213-L228)).
Value rendering, duplicate handling and the `extracted` prefix are Parkana's
own: arrays and `null` keep the representation described above rather than
Loki's label-value conversion.

Flattened path segments only get the digit prefix when the path is empty:
`{"1a":1}` becomes `_1a`, while `{"user":{"2b":1}}` becomes `user_2b`.
Blank (after trimming) segments are skipped while the parent prefix stays
usable: `{"":{"a":1}}` produces `a`, and `{"user":{"":{"id":1}}}` produces
`user_id`. Empty or whitespace-only names that still cannot be named are data
errors. Raw duplicate JSON member names are data errors. When distinct raw
names sanitize or flatten to the same metadata name, the later occurrence wins
deterministically (JSON source order, later logfmt pair, later regex capture);
never map iteration order.

### Expressions

`when`, `drop_if`'s `expr` and `promote`/`set_stream`'s `value` use a bounded,
method-free expression subset. The environment has:

- `line`: the current line (string).
- `labels.<name>`, `meta.<name>`: string values; an absent name reads `""`.
  Only literal member names are allowed, e.g. `meta.user_id`; a bare `labels`
  or `meta` is rejected.
- `stream`: the entry's original sanitized dataset (string).

Allowed:

- Comparisons `==`, `!=`, `<`, `<=`, `>`, `>=`; logical `&&`, `||`, `and`,
  `or`; ternary `cond ? a : b`.
- Arithmetic `+`, `-`, `*`, `/`, `%`; string `+` concatenation.
- String operators `contains`, `startsWith`, `endsWith`,
  `matches "literal-regexp"`, and `in [literal, list]`.
- Builtins `len`, `lower`, `upper`, `trim`, `trimPrefix`, `trimSuffix`,
  `indexOf`, `int`, `float`, `string`.

Not available: methods, optional member access, `if`/`else`, `let`,
`replace`, ranges, dynamic regexps, time, randomness, collections other than
the literal membership list of `in`, or any I/O.
Conditions and values are type-checked at definition compile time; an
expression of the wrong type fails validation.

Hard limits (one definition or one evaluation):

| limit | value |
|---|---|
| steps per pipeline | 32 |
| expression source | 4096 bytes |
| regex pattern | 1024 bytes |
| `redact` replacement | 256 bytes |
| expression AST nodes | 1000 |
| expression VM allocation budget | 100,000 units |
| `redact` match-index budget | 1,048,576 ints |
| `pipeline_error_details` | 256 bytes, one line |
| line before/after a step | the ingester's `-ingest.max-line-size` (256 KiB by default) |
| total metadata per entry | 256 KiB |
| one metadata value | 16 KiB |
| one label value | 2048 bytes |
| JSON nesting depth | 10,000 levels |
| one flattened JSON key name | the metadata budget (256 KiB by default) |
| cumulative flattened-key work | 4 × the metadata budget (1 MiB by default) |

String concatenation is additionally checked statically so it cannot build a
line larger than the configured line bound. Exceeding any limit above is a
data error, so the failing step's `on_error` policy applies and the step rolls
back atomically. These bounds keep a `keep` policy from producing an entry the
push handler's output validation would reject.

## Errors and per-step policy

Compile errors, unknown fields, invalid names/regexes/expressions and
read-stage `promote`/`set_stream` fail the write (HTTP 400) and dry-run; an
invalid pipeline is never stored.

Data-dependent failures, such as malformed JSON/logfmt, a regex that does not
match, an expression runtime error, or an exceeded bound, follow the failing
step's `on_error`:

- `keep` (default): the entry is restored to its state before the failing
  step, `pipeline_error` metadata is set to the step type and
  `pipeline_error_details` to a one-line, truncated message, and the entry is
  kept. Its remaining steps are skipped, and so are any remaining matched
  pipelines for that entry. Ingest answers 204 and stores the entry.
- `drop`: the entry is removed (an all-dropped push answers 204 and appends
  nothing).
- `reject`: the whole ingest push fails before the WAL with HTTP 400; on read,
  the query fails with the reject error.

The marker names are deliberately not `__`-prefixed, so ordinary LogQL metric
queries over the stream remain valid. Filter clean entries with
`| pipeline_error=""` or errored entries with `| pipeline_error!=""`.
Existing user metadata is only changed when a step writes it.

## Promote versus stored columns

`promote` copies a value into the entry's stream labels before ingest. That
label becomes its own physical Parquet column only when the existing
`-ingest.promoted-labels` configuration names it for that stream. The flag
takes `stream=col1,col2` assignments, repeated or separated by `;`; a `*`
assignment applies to every stream without its own, and the built-in default is
`*=level`. Pipelines never create typed columns dynamically and never change
the schema; a promoted label that is not configured as a column is stored in
the labels map like any other label. Adding the label to
`-ingest.promoted-labels` later applies to new files.
