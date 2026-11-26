package doccheck

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Markers around the catalog table in docs/skills/index.md.
const (
	catalogBegin = "<!-- skills-catalog:begin -->"
	catalogEnd   = "<!-- skills-catalog:end -->"
)

// skillMeta is the frontmatter of one skills/<dir>/SKILL.md.
type skillMeta struct {
	dir, name, description string
}

// catalogRow is one row of the skills catalog table.
type catalogRow struct {
	line                            int
	name, page, description, access string
	install                         string
}

var (
	skillName    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	rowSkillCell = regexp.MustCompile(`^\[([^\]]+)\]\(([^)]+)\)$`)
)

// TestSkillsCatalog checks the AI skills section against skills/: every
// skills/<name>/SKILL.md has name <name> and a one-line description in its
// frontmatter, one catalog row in docs/skills/index.md whose name, page link
// and description equal the frontmatter and whose install cell is
// `npx skills add nathabonfim59/parkana/skills/<name>`, and a page docs/skills/<name>.md
// listed in the sidebar; and every row and page belongs to a skill. Adding a
// skill is one folder, one row and one page.
func TestSkillsCatalog(t *testing.T) {
	for _, problem := range checkSkills(moduleRoot(t)) {
		t.Error(problem)
	}
}

// TestSkillsCatalogDrift runs checkSkills on a fixture repository with one
// consistent skill and then with each kind of drift, so the check is known to
// fail when it should.
func TestSkillsCatalogDrift(t *testing.T) {
	const desc = "Do one thing. Use when asked."
	good := map[string]string{
		"skills/one/SKILL.md": "---\nname: one\ndescription: " + desc + "\n---\n\n# One\n",
		"docs/skills/index.md": "# AI skills\n\n" + catalogBegin + "\n" +
			"| Skill | What it does | Access | Install |\n|---|---|---|---|\n" +
			"| [one](one.md) | " + desc + " | read-only | `npx skills add nathabonfim59/parkana/skills/one` |\n" +
			catalogEnd + "\n",
		"docs/skills/one.md":         "# One\n",
		"site/.vitepress/config.mts": "{ text: 'One', link: '/skills/one' },\n",
	}
	tests := []struct {
		name string
		edit func(files map[string]string)
		want string // substring of one reported problem; "" means none
	}{
		{"consistent", func(map[string]string) {}, ""},
		{"description edited in row", func(f map[string]string) {
			f["docs/skills/index.md"] = strings.Replace(f["docs/skills/index.md"], "Do one thing.", "Do two things.", 1)
		}, "description differs"},
		{"page missing", func(f map[string]string) { delete(f, "docs/skills/one.md") }, "no page docs/skills/one.md"},
		{"row missing", func(f map[string]string) {
			f["docs/skills/index.md"] = catalogBegin + "\n| Skill | What it does | Access | Install |\n|---|---|---|---|\n" + catalogEnd + "\n"
		}, "no catalog row"},
		{"row without skill", func(f map[string]string) {
			f["docs/skills/index.md"] = strings.Replace(f["docs/skills/index.md"], catalogEnd,
				"| [two](two.md) | x | writes | `npx skills add nathabonfim59/parkana/skills/two` |\n"+catalogEnd, 1)
		}, "row for two has no skills/two/SKILL.md"},
		{"page without skill", func(f map[string]string) { f["docs/skills/two.md"] = "# Two\n" }, "docs/skills/two.md has no skill"},
		{"name differs from folder", func(f map[string]string) {
			f["skills/one/SKILL.md"] = strings.Replace(f["skills/one/SKILL.md"], "name: one", "name: uno", 1)
		}, `name "uno", want "one"`},
		{"wrong install path", func(f map[string]string) {
			f["docs/skills/index.md"] = strings.Replace(f["docs/skills/index.md"], "parkana/skills/one`", "parkana/skills/two`", 1)
		}, "install cell"},
		{"old install form", func(f map[string]string) {
			f["docs/skills/index.md"] = strings.Replace(f["docs/skills/index.md"], "parkana/skills/one`", "parkana --skill one`", 1)
		}, "install cell"},
		{"bad access", func(f map[string]string) {
			f["docs/skills/index.md"] = strings.Replace(f["docs/skills/index.md"], "read-only", "maybe", 1)
		}, "access"},
		{"not in sidebar", func(f map[string]string) { f["site/.vitepress/config.mts"] = "" }, "sidebar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := maps.Clone(good)
			tc.edit(files)
			root := t.TempDir()
			for rel, content := range files {
				path := filepath.Join(root, filepath.FromSlash(rel))
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			problems := checkSkills(root)
			if tc.want == "" {
				if len(problems) != 0 {
					t.Fatalf("problems on a consistent fixture: %q", problems)
				}
				return
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }) {
				t.Fatalf("no problem containing %q; got %q", tc.want, problems)
			}
		})
	}
}

// checkSkills returns every inconsistency between skills/, the catalog in
// docs/skills/index.md, the pages in docs/skills/ and the sidebar under root.
func checkSkills(root string) []string {
	var problems []string
	report := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	skills, err := readSkills(root)
	if err != nil {
		return []string{err.Error()}
	}
	rows, err := readCatalog(filepath.Join(root, "docs", "skills", "index.md"))
	if err != nil {
		return []string{err.Error()}
	}
	sidebar, err := os.ReadFile(filepath.Join(root, "site", ".vitepress", "config.mts"))
	if err != nil {
		return []string{err.Error()}
	}

	byName := make(map[string]catalogRow, len(rows))
	for _, r := range rows {
		if _, dup := byName[r.name]; dup {
			report("docs/skills/index.md:%d: second catalog row for %s", r.line, r.name)
		}
		byName[r.name] = r
	}
	known := make(map[string]bool, len(skills))
	for _, s := range skills {
		known[s.dir] = true
		md := "skills/" + s.dir + "/SKILL.md"
		if s.name != s.dir {
			report("%s: frontmatter name %q, want %q (the folder name)", md, s.name, s.dir)
		}
		if !skillName.MatchString(s.dir) {
			report("%s: folder name %q is not lowercase letters, digits and hyphens", md, s.dir)
		}
		if s.description == "" {
			report("%s: no one-line description in the frontmatter", md)
		}
		page := filepath.Join(root, "docs", "skills", s.dir+".md")
		if _, err := os.Stat(page); err != nil {
			report("%s: no page docs/skills/%s.md", md, s.dir)
		}
		if !strings.Contains(string(sidebar), "'/skills/"+s.dir+"'") {
			report("%s: page /skills/%s is not in the sidebar (site/.vitepress/config.mts)", md, s.dir)
		}
		r, ok := byName[s.dir]
		if !ok {
			report("%s: no catalog row in docs/skills/index.md", md)
			continue
		}
		at := fmt.Sprintf("docs/skills/index.md:%d", r.line)
		if r.page != s.dir+".md" {
			report("%s: row for %s links %q, want %q", at, s.dir, r.page, s.dir+".md")
		}
		if r.description != s.description {
			report("%s: description differs from %s:\n  row:         %q\n  frontmatter: %q", at, md, r.description, s.description)
		}
		if r.access != "read-only" && r.access != "writes" {
			report("%s: access %q, want read-only or writes", at, r.access)
		}
		if want := "`npx skills add nathabonfim59/parkana/skills/" + s.dir + "`"; r.install != want {
			report("%s: install cell %s, want %s", at, r.install, want)
		}
	}
	for _, r := range rows {
		if !known[r.name] {
			report("docs/skills/index.md:%d: row for %s has no skills/%s/SKILL.md", r.line, r.name, r.name)
		}
	}
	pages, err := filepath.Glob(filepath.Join(root, "docs", "skills", "*.md"))
	if err != nil {
		return append(problems, err.Error())
	}
	for _, p := range pages {
		name := strings.TrimSuffix(filepath.Base(p), ".md")
		if name != "index" && !known[name] {
			report("docs/skills/%s.md has no skill: no skills/%s/SKILL.md", name, name)
		}
	}
	return problems
}

// readSkills reads the frontmatter of every skills/*/SKILL.md, sorted by
// folder.
func readSkills(root string) ([]skillMeta, error) {
	paths, err := filepath.Glob(filepath.Join(root, "skills", "*", "SKILL.md"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)
	skills := make([]skillMeta, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		s := skillMeta{dir: filepath.Base(filepath.Dir(p))}
		s.name, s.description = frontmatter(string(data))
		skills = append(skills, s)
	}
	return skills, nil
}

// frontmatter returns the name and description fields of a SKILL.md's YAML
// frontmatter. Only single-line scalars (plain, or single- or double-quoted)
// are read; a block scalar (| or >) yields an empty description, which the
// check reports, so the catalog can compare the text exactly.
func frontmatter(text string) (name, description string) {
	rest, ok := strings.CutPrefix(text, "---\n")
	if !ok {
		return "", ""
	}
	block, _, ok := strings.Cut(rest, "\n---")
	if !ok {
		return "", ""
	}
	for line := range strings.SplitSeq(block, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.HasPrefix(key, " ") {
			continue
		}
		value = scalar(strings.TrimSpace(value))
		switch key {
		case "name":
			name = value
		case "description":
			description = value
		}
	}
	return name, description
}

// scalar unquotes a single-line YAML scalar; block indicators give "".
func scalar(v string) string {
	switch {
	case v == "|" || v == ">" || strings.HasPrefix(v, "|") || strings.HasPrefix(v, ">"):
		return ""
	case len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"':
		if u, err := strconv.Unquote(v); err == nil {
			return u
		}
		return ""
	case len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'':
		return strings.ReplaceAll(v[1:len(v)-1], "''", "'")
	}
	return v
}

// readCatalog parses the table between the catalog markers of path.
func readCatalog(path string) ([]catalogRow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	begin := slices.Index(lines, catalogBegin)
	end := slices.Index(lines, catalogEnd)
	if begin < 0 || end < begin {
		return nil, fmt.Errorf("docs/skills/index.md: no %s ... %s block", catalogBegin, catalogEnd)
	}
	var rows []catalogRow
	for i := begin + 1; i < end; i++ {
		line := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(line, "|") || strings.HasPrefix(line, "|---") || strings.HasPrefix(line, "| Skill ") {
			continue
		}
		cells := tableCells(line)
		if len(cells) != 4 {
			return nil, fmt.Errorf("docs/skills/index.md:%d: want 4 cells, got %d", i+1, len(cells))
		}
		m := rowSkillCell.FindStringSubmatch(cells[0])
		if m == nil {
			return nil, fmt.Errorf("docs/skills/index.md:%d: first cell %q is not [name](name.md)", i+1, cells[0])
		}
		rows = append(rows, catalogRow{
			line: i + 1, name: m[1], page: m[2],
			description: cells[1], access: cells[2], install: cells[3],
		})
	}
	return rows, nil
}

// tableCells splits a Markdown table row on unescaped pipes and unescapes
// \| inside cells.
func tableCells(line string) []string {
	line = strings.TrimSuffix(strings.TrimPrefix(line, "|"), "|")
	var cells []string
	var cell strings.Builder
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\\' && i+1 < len(line) && line[i+1] == '|':
			cell.WriteByte('|')
			i++
		case line[i] == '|':
			cells = append(cells, strings.TrimSpace(cell.String()))
			cell.Reset()
		default:
			cell.WriteByte(line[i])
		}
	}
	return append(cells, strings.TrimSpace(cell.String()))
}
