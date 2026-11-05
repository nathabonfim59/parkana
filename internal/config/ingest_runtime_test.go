package config

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

func TestIngestRuntimeDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt := c.Ingest.IngestRuntime
	if rt.WALSync != WALSyncModeAlways || rt.WALSyncInterval != time.Second ||
		rt.FlushConcurrency != 4 || rt.UploadConcurrency != 4 {
		t.Errorf("unexpected runtime defaults: %+v", rt)
	}
}

func TestIngestRuntimeFlagsAndEnv(t *testing.T) {
	env := map[string]string{
		"PARKANA_WAL_SYNC":                  "interval",
		"PARKANA_WAL_SYNC_INTERVAL":         "2s",
		"PARKANA_INGEST_FLUSH_CONCURRENCY":  "6",
		"PARKANA_INGEST_UPLOAD_CONCURRENCY": "2",
	}
	// Flags beat env for the interval and the upload workers.
	c, err := load(t, []string{"-wal.sync-interval=250ms", "-ingest.upload-concurrency=8"}, env)
	if err != nil {
		t.Fatal(err)
	}
	rt := c.Ingest.IngestRuntime
	if rt.WALSync != WALSyncModeInterval {
		t.Errorf("env not applied: WALSync = %q", rt.WALSync)
	}
	if rt.WALSyncInterval != 250*time.Millisecond {
		t.Errorf("flag should beat env: WALSyncInterval = %s", rt.WALSyncInterval)
	}
	if rt.FlushConcurrency != 6 {
		t.Errorf("env not applied: FlushConcurrency = %d", rt.FlushConcurrency)
	}
	if rt.UploadConcurrency != 8 {
		t.Errorf("flag should beat env: UploadConcurrency = %d", rt.UploadConcurrency)
	}
}

func TestIngestRuntimeFlagBeatsEnv(t *testing.T) {
	env := map[string]string{
		"PARKANA_WAL_SYNC":                  "interval",
		"PARKANA_WAL_SYNC_INTERVAL":         "9s",
		"PARKANA_INGEST_FLUSH_CONCURRENCY":  "40",
		"PARKANA_INGEST_UPLOAD_CONCURRENCY": "40",
	}
	for _, tc := range []struct {
		name string
		args []string
		rt   IngestRuntime
	}{
		{
			name: "wal.sync",
			args: []string{"-wal.sync=always"},
			rt:   IngestRuntime{WALSync: WALSyncModeAlways, WALSyncInterval: 9 * time.Second, FlushConcurrency: 40, UploadConcurrency: 40},
		},
		{
			name: "wal.sync-interval",
			args: []string{"-wal.sync-interval=3s"},
			rt:   IngestRuntime{WALSync: WALSyncModeInterval, WALSyncInterval: 3 * time.Second, FlushConcurrency: 40, UploadConcurrency: 40},
		},
		{
			name: "ingest.flush-concurrency",
			args: []string{"-ingest.flush-concurrency=2"},
			rt:   IngestRuntime{WALSync: WALSyncModeInterval, WALSyncInterval: 9 * time.Second, FlushConcurrency: 2, UploadConcurrency: 40},
		},
		{
			name: "ingest.upload-concurrency",
			args: []string{"-ingest.upload-concurrency=7"},
			rt:   IngestRuntime{WALSync: WALSyncModeInterval, WALSyncInterval: 9 * time.Second, FlushConcurrency: 40, UploadConcurrency: 7},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Ingest.IngestRuntime != tc.rt {
				t.Errorf("IngestRuntime = %+v, want %+v", c.Ingest.IngestRuntime, tc.rt)
			}
		})
	}
}

func TestIngestRuntimeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		rt   IngestRuntime
	}{
		{
			name: "smallest workers and interval",
			args: []string{"-wal.sync-interval=1ms", "-ingest.flush-concurrency=1", "-ingest.upload-concurrency=1"},
			rt:   IngestRuntime{WALSync: WALSyncModeAlways, WALSyncInterval: time.Millisecond, FlushConcurrency: 1, UploadConcurrency: 1},
		},
		{
			name: "largest workers",
			args: []string{"-ingest.flush-concurrency=64", "-ingest.upload-concurrency=64"},
			rt:   IngestRuntime{WALSync: WALSyncModeAlways, WALSyncInterval: time.Second, FlushConcurrency: 64, UploadConcurrency: 64},
		},
		{
			name: "explicit always mode keeps defaults",
			args: []string{"-wal.sync=always", "-ingest.flush-concurrency=4"},
			rt:   IngestRuntime{WALSync: WALSyncModeAlways, WALSyncInterval: time.Second, FlushConcurrency: 4, UploadConcurrency: 4},
		},
		{
			name: "interval mode via env",
			env:  map[string]string{"PARKANA_WAL_SYNC": "interval", "PARKANA_WAL_SYNC_INTERVAL": "500ms"},
			rt:   IngestRuntime{WALSync: WALSyncModeInterval, WALSyncInterval: 500 * time.Millisecond, FlushConcurrency: 4, UploadConcurrency: 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Ingest.IngestRuntime != tc.rt {
				t.Errorf("IngestRuntime = %+v, want %+v", c.Ingest.IngestRuntime, tc.rt)
			}
		})
	}
}

func TestIngestRuntimeInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"invalid sync mode", []string{"-wal.sync=sometimes"}, nil, "-wal.sync"},
		{"invalid sync mode case", []string{"-wal.sync=Always"}, nil, "-wal.sync"},
		{"empty sync mode", []string{"-wal.sync="}, nil, "-wal.sync"},
		{"env invalid sync mode", nil, map[string]string{"PARKANA_WAL_SYNC": "nightly"}, "PARKANA_WAL_SYNC"},
		{"zero sync interval", []string{"-wal.sync-interval=0"}, nil, "wal.sync-interval"},
		{"negative sync interval", []string{"-wal.sync-interval=-1s"}, nil, "wal.sync-interval"},
		{"garbage sync interval", []string{"-wal.sync-interval=soon"}, nil, "wal.sync-interval"},
		{"env zero sync interval", nil, map[string]string{"PARKANA_WAL_SYNC_INTERVAL": "0"}, "PARKANA_WAL_SYNC_INTERVAL"},
		{"env negative sync interval", nil, map[string]string{"PARKANA_WAL_SYNC_INTERVAL": "-5s"}, "PARKANA_WAL_SYNC_INTERVAL"},
		{"zero flush concurrency", []string{"-ingest.flush-concurrency=0"}, nil, "flush-concurrency"},
		{"negative flush concurrency", []string{"-ingest.flush-concurrency=-2"}, nil, "flush-concurrency"},
		{"excessive flush concurrency", []string{"-ingest.flush-concurrency=65"}, nil, "flush-concurrency"},
		{"huge flush concurrency", []string{"-ingest.flush-concurrency=1000000"}, nil, "flush-concurrency"},
		{"garbage flush concurrency", []string{"-ingest.flush-concurrency=many"}, nil, "flush-concurrency"},
		{"env zero flush concurrency", nil, map[string]string{"PARKANA_INGEST_FLUSH_CONCURRENCY": "0"}, "PARKANA_INGEST_FLUSH_CONCURRENCY"},
		{"env excessive flush concurrency", nil, map[string]string{"PARKANA_INGEST_FLUSH_CONCURRENCY": "128"}, "PARKANA_INGEST_FLUSH_CONCURRENCY"},
		{"zero upload concurrency", []string{"-ingest.upload-concurrency=0"}, nil, "upload-concurrency"},
		{"negative upload concurrency", []string{"-ingest.upload-concurrency=-8"}, nil, "upload-concurrency"},
		{"excessive upload concurrency", []string{"-ingest.upload-concurrency=65"}, nil, "upload-concurrency"},
		{"garbage upload concurrency", []string{"-ingest.upload-concurrency=1.5"}, nil, "upload-concurrency"},
		{"env negative upload concurrency", nil, map[string]string{"PARKANA_INGEST_UPLOAD_CONCURRENCY": "-1"}, "PARKANA_INGEST_UPLOAD_CONCURRENCY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load accepted %v / %v; want error containing %q, got %v", tc.args, tc.env, tc.want, err)
			}
		})
	}
}

func TestIngestRuntimeValidateHandBuilt(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A hand-built config with zero runtime fields keeps the defaults and
	// must stay valid.
	zero := c
	zero.Ingest.IngestRuntime = IngestRuntime{}
	if err := zero.Validate(); err != nil {
		t.Errorf("zero IngestRuntime rejected: %v", err)
	}
	for _, tc := range []struct {
		name string
		mut  func(*IngestRuntime)
		want string
	}{
		{"bad mode", func(r *IngestRuntime) { r.WALSync = "sometimes" }, "-wal.sync"},
		{"negative interval", func(r *IngestRuntime) { r.WALSyncInterval = -time.Second }, "-wal.sync-interval"},
		{"negative flush", func(r *IngestRuntime) { r.FlushConcurrency = -1 }, "-ingest.flush-concurrency"},
		{"over-cap flush", func(r *IngestRuntime) { r.FlushConcurrency = maxRuntimeConcurrency + 1 }, "-ingest.flush-concurrency"},
		{"negative upload", func(r *IngestRuntime) { r.UploadConcurrency = -3 }, "-ingest.upload-concurrency"},
		{"over-cap upload", func(r *IngestRuntime) { r.UploadConcurrency = maxRuntimeConcurrency + 1 }, "-ingest.upload-concurrency"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alt := c
			tc.mut(&alt.Ingest.IngestRuntime)
			if err := alt.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

// -h runs flag.PrintDefaults, which calls String on a zero value of every
// custom flag.Value; the runtime flag String methods must not panic on nil
// receivers and must print the documented defaults.
func TestIngestRuntimeUsageDefaults(t *testing.T) {
	var buf bytes.Buffer
	_, err := Load([]string{"-h"}, func(string) string { return "" }, &buf)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Load(-h) = %v, want flag.ErrHelp", err)
	}
	out := buf.String()
	for _, want := range []string{
		"-wal.sync", "-wal.sync-interval",
		"-ingest.flush-concurrency", "-ingest.upload-concurrency",
		"(default always)", "(default 1s)", "(default 4)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("usage missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "panic calling String") {
		t.Errorf("usage shows a zero-value String() panic:\n%s", out)
	}
}
