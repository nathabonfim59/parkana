package config

import (
	"bytes"
	"errors"
	"flag"
	"math"
	"strings"
	"testing"
)

func TestIngestUploadMemoryDefaultIsAuto(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.UploadMemory != 0 {
		t.Errorf("UploadMemory default = %d, want 0 (automatic budget)", c.Ingest.UploadMemory)
	}
}

func TestIngestUploadMemoryValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want ByteSize
	}{
		{"explicit zero is auto", []string{"-ingest.upload-memory=0"}, nil, 0},
		{"env zero is auto", nil, map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "0"}, 0},
		{"plain bytes", []string{"-ingest.upload-memory=512"}, nil, 512},
		{"bytes suffix", []string{"-ingest.upload-memory=64B"}, nil, 64},
		{"mebibytes", []string{"-ingest.upload-memory=256MiB"}, nil, 256 * MiB},
		{"env gibibytes", nil, map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "2GiB"}, 2 * GiB},
		{"max int64 bytes", []string{"-ingest.upload-memory=9223372036854775807"}, nil, math.MaxInt64},
		{"largest TiB that fits", []string{"-ingest.upload-memory=8388607TiB"}, nil, 8388607 * TiB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Ingest.UploadMemory != tc.want {
				t.Errorf("UploadMemory = %d, want %d", c.Ingest.UploadMemory, tc.want)
			}
		})
	}
}

func TestIngestUploadMemoryFlagBeatsEnv(t *testing.T) {
	env := map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "512MiB"}
	c, err := load(t, []string{"-ingest.upload-memory=1GiB"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.UploadMemory != GiB {
		t.Errorf("flag should beat env: UploadMemory = %d, want %d", c.Ingest.UploadMemory, GiB)
	}
	// An explicit 0 keeps the automatic budget and still beats the env var.
	c, err = load(t, []string{"-ingest.upload-memory=0"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.UploadMemory != 0 {
		t.Errorf("explicit 0 should beat env: UploadMemory = %d, want 0", c.Ingest.UploadMemory)
	}
}

func TestIngestUploadMemoryInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"negative bytes", []string{"-ingest.upload-memory=-1"}, nil, "upload-memory"},
		{"negative mebibytes", []string{"-ingest.upload-memory=-5MiB"}, nil, "upload-memory"},
		{"garbage", []string{"-ingest.upload-memory=lots"}, nil, "upload-memory"},
		{"fraction", []string{"-ingest.upload-memory=1.5GiB"}, nil, "upload-memory"},
		{"empty", []string{"-ingest.upload-memory="}, nil, "upload-memory"},
		{"int64 overflow", []string{"-ingest.upload-memory=9223372036854775808"}, nil, "upload-memory"},
		{"unit overflow", []string{"-ingest.upload-memory=8388608TiB"}, nil, "upload-memory"},
		// ByteSize.Set would silently wrap these; the flag must not.
		{"unit wraps to zero", []string{"-ingest.upload-memory=16777216TiB"}, nil, "upload-memory"},
		{"unit wraps positive", []string{"-ingest.upload-memory=16777217TiB"}, nil, "upload-memory"},
		{"KiB wraps positive", []string{"-ingest.upload-memory=18014398509481985KiB"}, nil, "upload-memory"},
		{"env negative", nil, map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "-1GiB"}, "PARKANA_INGEST_UPLOAD_MEMORY"},
		{"env garbage", nil, map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "lots"}, "PARKANA_INGEST_UPLOAD_MEMORY"},
		{"env unit overflow", nil, map[string]string{"PARKANA_INGEST_UPLOAD_MEMORY": "16777216TiB"}, "PARKANA_INGEST_UPLOAD_MEMORY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load accepted %v / %v; want error containing %q, got %v", tc.args, tc.env, tc.want, err)
			}
		})
	}
}

// A hand-built Config keeps the automatic budget (zero) as valid, while a
// programmatically negative budget fails Validate.
func TestIngestUploadMemoryValidateHandBuilt(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	zero := c
	zero.Ingest.UploadMemory = 0
	if err := zero.Validate(); err != nil {
		t.Errorf("zero UploadMemory rejected: %v", err)
	}
	fixed := c
	fixed.Ingest.UploadMemory = 512 * MiB
	if err := fixed.Validate(); err != nil {
		t.Errorf("positive UploadMemory rejected: %v", err)
	}
	neg := c
	neg.Ingest.UploadMemory = -1
	if err := neg.Validate(); err == nil || !strings.Contains(err.Error(), "-ingest.upload-memory") {
		t.Errorf("negative UploadMemory: Validate() = %v, want error containing %q", err, "-ingest.upload-memory")
	}
}

// -h runs flag.PrintDefaults, which calls String on a zero value of every
// custom flag.Value; uploadMemoryFlag must not panic on a nil receiver.
func TestIngestUploadMemoryUsage(t *testing.T) {
	if got := (uploadMemoryFlag{}).String(); got != "0" {
		t.Errorf("zero uploadMemoryFlag.String() = %q, want %q", got, "0")
	}
	var buf bytes.Buffer
	_, err := Load([]string{"-h"}, func(string) string { return "" }, &buf)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Load(-h) = %v, want flag.ErrHelp", err)
	}
	out := buf.String()
	if !strings.Contains(out, "-ingest.upload-memory") {
		t.Errorf("usage missing -ingest.upload-memory:\n%s", out)
	}
	if strings.Contains(out, "panic calling String") {
		t.Errorf("usage shows a zero-value String() panic:\n%s", out)
	}
}
