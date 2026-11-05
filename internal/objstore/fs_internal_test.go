package objstore

import (
	"errors"
	"path/filepath"
	"testing"
)

// A sync failure after MkdirAll created the directories must be retried by
// the next call, even though nothing is missing any more.
func TestMkdirAllDurableRetriesSync(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "a", "b")
	var synced []string
	fail := true
	syncDirPath = func(d string) error {
		synced = append(synced, d)
		if fail {
			return errors.New("injected sync failure")
		}
		return nil
	}
	t.Cleanup(func() { syncDirPath = syncPath })

	if err := MkdirAllDurable(dir, 0o755); err == nil {
		t.Fatal("first call: want the injected sync error")
	}
	fail, synced = false, nil
	if err := MkdirAllDurable(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// The retry must sync the whole chain: a, base, and everything above.
	want := []string{filepath.Join(base, "a"), base}
	if len(synced) < len(want) || synced[0] != want[0] || synced[1] != want[1] {
		t.Fatalf("retry synced %v, want it to start with %v", synced, want)
	}
	if last := synced[len(synced)-1]; last != "/" {
		t.Fatalf("retry stopped at %s, want the filesystem root", last)
	}
}
