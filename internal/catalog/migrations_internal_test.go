package catalog

import (
	"strings"
	"testing"
	"testing/fstest"
)

// TestReadMigrations checks the shared migration loader orders migrations
// by numeric version, skips what is not a .sql file, and rejects bad names,
// empty bodies, duplicate versions and an empty directory. It also loads
// the embedded Postgres set.
func TestReadMigrations(t *testing.T) {
	file := func(body string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(body)} }

	t.Run("ordered by numeric version", func(t *testing.T) {
		fsys := fstest.MapFS{
			"m/10_ten.sql":   file("SELECT 10;"),
			"m/2_two.sql":    file("  SELECT 2;\n"),
			"m/0001_one.sql": file("SELECT 1;"),
			"m/README.md":    file("not a migration"),
			"m/3_dir.sql/x":  file("a directory named like a migration"),
		}
		got, err := readMigrations(fsys, "m")
		if err != nil {
			t.Fatalf("readMigrations: %v", err)
		}
		var names []string
		for _, m := range got {
			names = append(names, m.name)
		}
		if want := "0001_one.sql 2_two.sql 10_ten.sql"; strings.Join(names, " ") != want {
			t.Fatalf("migrations = %v, want %s", names, want)
		}
		if got[1].version != 2 || got[1].stmt != "SELECT 2;" {
			t.Fatalf("2_two.sql = %+v, want version 2 with a trimmed body", got[1])
		}
	})

	for _, tc := range []struct {
		name string
		fsys fstest.MapFS
		want string
	}{
		{"no underscore", fstest.MapFS{"m/0001.sql": file("SELECT 1;")}, "name must be"},
		{"non-numeric version", fstest.MapFS{"m/one_init.sql": file("SELECT 1;")}, "invalid version"},
		{"zero version", fstest.MapFS{"m/0000_init.sql": file("SELECT 1;")}, "invalid version"},
		{"negative version", fstest.MapFS{"m/-1_init.sql": file("SELECT 1;")}, "invalid version"},
		{"empty body", fstest.MapFS{"m/0001_init.sql": file(" \n\t")}, "empty"},
		{"duplicate version", fstest.MapFS{"m/0001_a.sql": file("SELECT 1;"), "m/1_b.sql": file("SELECT 1;")}, "duplicate migration version 1"},
		{"no migrations", fstest.MapFS{"m/README.md": file("x")}, "no migrations"},
		{"missing directory", fstest.MapFS{"other/0001_a.sql": file("SELECT 1;")}, "read migrations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readMigrations(tc.fsys, "m")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readMigrations = %v, want an error containing %q", err, tc.want)
			}
		})
	}

	t.Run("embedded", func(t *testing.T) {
		if m, err := readMigrations(postgresMigrations, postgresMigrationsDir); err != nil || len(m) == 0 {
			t.Fatalf("postgres migrations = %d, %v", len(m), err)
		}
	})
}
