package ingest

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/config"
)

// TestUploadOrphanGraceBelowCompactorMin checks the cross-role contract with
// the compactor's orphan GC: the ingester stops publishing a Put under its
// old key strictly before the shortest grace the compactor accepts, so a
// successful upload is never collected as an orphan before its catalog row
// lands.
func TestUploadOrphanGraceBelowCompactorMin(t *testing.T) {
	if uploadOrphanGrace >= config.MinOrphanGrace {
		t.Fatalf("uploadOrphanGrace = %s, want below config.MinOrphanGrace = %s", uploadOrphanGrace, config.MinOrphanGrace)
	}
}
