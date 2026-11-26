package doccheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// moduleRoot returns the directory of the go.mod that holds this package.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("doccheck: no go.mod above the working directory")
		}
		dir = parent
	}
}

// findings are the doc-comment problems of the module, by rule, each as
// "file:line: identifier: problem".
type findings struct {
	pkg, single, missing, name []string
}

// TestDocComments checks the doc comments of every non-test Go file of the
// module. It walks the module from its root, skipping what the go tool
// ignores (directories starting with "." or "_", testdata), node_modules
// and nested modules (grafana/parkana-datasource has its own go.mod). The
// rules, each a subtest:
//
//   - package: every package has a package comment in a non-test file.
//   - single: at most one non-test file of a package has a package
//     comment. Go merges them all into the package doc, and the generated
//     reference shows the first one it reads as the package summary. A file
//     comment that is not the package doc is separated from the package
//     clause by a blank line.
//   - missing: every exported top-level func, type, const and var, and
//     every exported method of an exported type, has a doc comment. In a
//     parenthesized const, var or type group the group's comment counts for
//     its specs, as golint did.
//   - name: the doc comment of an exported func, method or type starts with
//     its name, for types optionally after "A ", "An " or "The ". A comment
//     starting with "Deprecated:" passes.
func TestDocComments(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	var f findings
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		if path != root {
			name := d.Name()
			if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "node_modules" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			}
		}
		return f.checkDir(fset, root, path)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []struct {
		name string
		list []string
	}{
		{"package", f.pkg},
		{"single", f.single},
		{"missing", f.missing},
		{"name", f.name},
	} {
		t.Run(rule.name, func(t *testing.T) {
			if len(rule.list) > 0 {
				t.Errorf("%d problems:\n%s", len(rule.list), strings.Join(rule.list, "\n"))
			}
		})
	}
}

// checkDir parses the non-test Go files of dir and records their problems.
func (f *findings) checkDir(fset *token.FileSet, root, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	rel := func(pos token.Pos) string {
		p := fset.Position(pos)
		name, err := filepath.Rel(root, p.Filename)
		if err != nil {
			name = p.Filename
		}
		return name + ":" + strconv.Itoa(p.Line)
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			return err
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil
	}
	var documented []string
	for _, file := range files {
		if file.Doc.Text() != "" {
			documented = append(documented, filepath.Base(fset.Position(file.Package).Filename))
		}
	}
	switch {
	case len(documented) == 0:
		f.pkg = append(f.pkg, rel(files[0].Package)+": package "+files[0].Name.Name+": no package comment")
	case len(documented) > 1:
		f.single = append(f.single, rel(files[0].Package)+": package "+files[0].Name.Name+": package comment in "+strconv.Itoa(len(documented))+" files: "+strings.Join(documented, ", "))
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				name := d.Name.Name
				if d.Recv != nil {
					typ := recvType(d.Recv.List[0].Type)
					if !ast.IsExported(typ) {
						continue
					}
					name = typ + "." + name
				}
				if ast.IsExported(d.Name.Name) {
					f.check(rel(d.Pos()), name, d.Name.Name, d.Doc, false)
				}
			case *ast.GenDecl:
				f.checkGenDecl(rel, d)
			}
		}
	}
	return nil
}

// checkGenDecl records the problems of a const, var or type declaration.
func (f *findings) checkGenDecl(rel func(token.Pos) string, d *ast.GenDecl) {
	grouped := d.Lparen.IsValid()
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			if !ast.IsExported(s.Name.Name) {
				continue
			}
			doc := s.Doc
			if doc == nil && !grouped {
				doc = d.Doc
			}
			if doc == nil && grouped && d.Doc.Text() != "" {
				continue // the group's comment documents it
			}
			f.check(rel(s.Pos()), s.Name.Name, s.Name.Name, doc, true)
		case *ast.ValueSpec:
			if s.Doc.Text() != "" || d.Doc.Text() != "" {
				continue
			}
			for _, n := range s.Names {
				if ast.IsExported(n.Name) {
					f.missing = append(f.missing, rel(n.Pos())+": "+n.Name+": no doc comment")
				}
			}
		}
	}
}

// check records a missing doc comment, or one that does not start with
// name, for the func, method or type ident (the method's Type.Name).
func (f *findings) check(pos, ident, name string, doc *ast.CommentGroup, isType bool) {
	text := doc.Text()
	switch {
	case text == "":
		f.missing = append(f.missing, pos+": "+ident+": no doc comment")
	case !startsWithName(text, name, isType):
		first, _, _ := strings.Cut(text, "\n")
		f.name = append(f.name, pos+": "+ident+": doc comment does not start with "+name+": "+first)
	}
}

// startsWithName reports whether doc comment text starts with name followed
// by a non-identifier character, after "A ", "An " or "The " when article
// is set. A comment starting with "Deprecated:" passes.
func startsWithName(text, name string, article bool) bool {
	if strings.HasPrefix(text, "Deprecated:") {
		return true
	}
	if article {
		for _, a := range []string{"A ", "An ", "The "} {
			if rest, ok := strings.CutPrefix(text, a); ok {
				text = rest
				break
			}
		}
	}
	rest, ok := strings.CutPrefix(text, name)
	if !ok {
		return false
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return rest == "" || !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
}

// recvType returns the type name of a method receiver: T for T, *T, T[K]
// and *T[K, V].
func recvType(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.IndexListExpr:
			e = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}
