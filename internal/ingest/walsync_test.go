package ingest

import (
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/wal"
)

func TestWALSyncSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ingest   config.Ingest
		wantMode wal.SyncMode
		wantInt  time.Duration
		wantErr  bool
	}{
		{
			name:     "empty is always",
			ingest:   config.Ingest{WALSyncInterval: 250 * time.Millisecond},
			wantMode: wal.SyncAlways,
			wantInt:  250 * time.Millisecond,
		},
		{
			name:     "always",
			ingest:   config.Ingest{WALSync: config.WALSyncModeAlways, WALSyncInterval: 2 * time.Second},
			wantMode: wal.SyncAlways,
			wantInt:  2 * time.Second,
		},
		{
			name:     "interval",
			ingest:   config.Ingest{WALSync: config.WALSyncModeInterval, WALSyncInterval: 750 * time.Millisecond},
			wantMode: wal.SyncInterval,
			wantInt:  750 * time.Millisecond,
		},
		{
			name:     "interval zero lets the wal default apply",
			ingest:   config.Ingest{WALSync: config.WALSyncModeInterval},
			wantMode: wal.SyncInterval,
		},
		{
			name:    "invalid mode",
			ingest:  config.Ingest{WALSync: "sometimes"},
			wantErr: true,
		},
		{
			name:    "negative interval",
			ingest:  config.Ingest{WALSyncInterval: -time.Second},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mode, interval, err := walSyncSettings(tt.ingest)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("walSyncSettings(%+v) accepted", tt.ingest)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode != tt.wantMode || interval != tt.wantInt {
				t.Fatalf("walSyncSettings(%+v) = %v, %s; want %v, %s",
					tt.ingest, mode, interval, tt.wantMode, tt.wantInt)
			}
		})
	}
}
