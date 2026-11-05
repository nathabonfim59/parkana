package objstore_test

import (
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/objstore/objstoretest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestFS(t *testing.T) {
	s, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	objstoretest.Run(t, s)

	if err := s.Put(t.Context(), ".tmp/x", strings.NewReader("x"), 1); err == nil {
		t.Error("Put into the temp dir accepted")
	}
}

func TestDataKey(t *testing.T) {
	id := uuid.MustParse("01890a5d-ac96-774b-bcce-b302099a8057")
	ts := time.Date(2026, 11, 3, 7, 30, 0, 0, time.FixedZone("x", -3*3600))
	got := objstore.DataKey(tenant.Default, "api", ts, id)
	want := "default/logs/api/2026/11/03/10/01890a5d-ac96-774b-bcce-b302099a8057.parquet"
	if got != want {
		t.Fatalf("DataKey = %s, want %s", got, want)
	}
	if err := objstore.ValidateKey(got); err != nil {
		t.Fatal(err)
	}
	if sc := objstore.SidecarKey(got); !strings.HasSuffix(sc, "8057.idx") {
		t.Fatalf("SidecarKey = %s", sc)
	}
}

func TestMkdirAllDurable(t *testing.T) {
	base := t.TempDir()
	dir := base + "/a/b/c"
	if err := objstore.MkdirAllDurable(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		t.Fatalf("Stat(%s) = %v, %v", dir, st, err)
	}
	// Existing directories are fine.
	if err := objstore.MkdirAllDurable(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A file in the way is an error, not a silent skip.
	if err := os.WriteFile(base+"/file", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := objstore.MkdirAllDurable(base+"/file/x", 0o755); err == nil {
		t.Fatal("MkdirAllDurable under a file succeeded")
	}
}
