# AI skills

Parkana ships agent skills: folders with a `SKILL.md` that teach a coding
agent how to work with
Parkana. They live in [`skills/`](https://github.com/nathabonfim59/parkana/tree/main/skills) in the repository and install
with the [`skills`](https://www.npmjs.com/package/skills) CLI, which copies
them into the agent's skills directory of your project (or of your home
directory with `-g`). The commands below were tested with `skills` 1.9.0 and
need Node.js 22.20 or later. The skills are new in 1.2.0.

## Catalog

Each row is one skill. The description is the skill's own `description`
field, the text the agent reads to decide when to use it.

<!-- skills-catalog:begin -->
| Skill | What it does | Access | Install |
|---|---|---|---|
| [parkana](parkana.md) | Query a Parkana log store (Loki-compatible HTTP API, Parquet files queried with DuckDB) with LogQL and SQL in ways that read few files. Use when searching logs, counting errors or exploring streams in Parkana, or when a Parkana query is slow or times out. Read-only by default. | read-only | `npx skills add nathabonfim59/parkana/skills/parkana` |
| [parkana-redaction](parkana-redaction.md) | Find secrets and personal data (passwords, tokens, emails, card numbers, IP addresses) in a Parkana log store's streams or in local log samples, report them per stream without printing a raw value, and build, dry-run and (only after the user confirms each one) create or update one ingest redaction pipeline per stream. Use when asked to redact, mask or anonymize logs in Parkana or to check logs for leaked secrets or PII. | writes | `npx skills add nathabonfim59/parkana/skills/parkana-redaction` |
<!-- skills-catalog:end -->

## Install

Install one skill with the command in its row, which names the skill's
folder in the repository:

```sh
npx skills add nathabonfim59/parkana/skills/<name>
```

Two other forms install the same skill:

```sh
npx skills add nathabonfim59/parkana --skill <name>
npx skills add nathabonfim59/parkana@<name>
```

The `@<name>` form only works for a name that does not look like a version:
`skills` reads `@v1.2.0`, `@1.2.0` or a commit hash after `@` as the version
to fetch, not as a skill name. No Parkana skill has such a name.

Install every skill:

```sh
npx skills add nathabonfim59/parkana --skill '*'
```

To choose the agent, add `-a` with its name (`npx skills add --help` lists them);
to install for every project of your user, add `-g`; to skip the prompts,
add `-y`. List what the repository offers without installing anything:

```sh
npx skills add nathabonfim59/parkana --list
```

To pin a version of the skills, add a tag or commit after `#`, as in
`nathabonfim59/parkana/skills/<name>#<tag>` or
`nathabonfim59/parkana#<tag>`. A tag shaped like a version (`v1.2.0`) or a
commit hash also works after `@` on the repository, as in
`nathabonfim59/parkana@<tag>`, but not after a skill folder. `npx skills list` shows what is installed,
`npx skills update` updates it and `npx skills remove <name>` removes it.

A skill runs with the permissions of the agent that loads it. Read its
`SKILL.md` and scripts before you install it.

## Adding a skill

A new skill is a folder `skills/<name>/` with a `SKILL.md` whose frontmatter
has `name` (the folder name) and `description`, one row in the catalog above
and one page `docs/skills/<name>.md`, listed in the sidebar and in
`llms.txt`. `go test ./internal/doccheck/` fails when a skill has no row or
no page, when a row's name or description differs from the frontmatter, or
when a row or page has no skill.
