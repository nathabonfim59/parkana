package ingest

import (
	"fmt"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/wal"
)

// walSyncSettings resolves the configured WAL acknowledgment policy. An empty
// WALSync keeps the zero-value wal.SyncAlways behavior, so a Config built
// without config.Load still gets the documented default. Invalid modes and
// negative intervals are rejected here so the service fails at construction
// instead of opening a WAL with a policy it cannot honor.
func walSyncSettings(c config.Ingest) (wal.SyncMode, time.Duration, error) {
	mode := wal.SyncAlways
	if c.WALSync != "" {
		parsed, err := wal.ParseSyncMode(c.WALSync)
		if err != nil {
			return 0, 0, fmt.Errorf("ingest: %w", err)
		}
		mode = parsed
	}
	if c.WALSyncInterval < 0 {
		return 0, 0, fmt.Errorf("ingest: -wal.sync-interval must not be negative, got %s", c.WALSyncInterval)
	}
	return mode, c.WALSyncInterval, nil
}
