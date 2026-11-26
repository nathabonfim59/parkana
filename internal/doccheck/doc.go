// Package doccheck holds repository-wide documentation checks that run with
// go test: every package of the module has a package comment in exactly one
// file, every exported identifier has a doc comment that starts with its
// name, the AI skills catalog (docs/skills) matches the skills/*/SKILL.md
// frontmatter, and the redaction pattern catalog in
// skills/parkana-redaction/patterns.json works as documented and matches the
// table generated into docs/pipelines/redaction.md. It has no code of its
// own; the checks live in its tests.
package doccheck
