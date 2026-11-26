package config

import (
	"math"
	"strings"
	"testing"
)

func TestIngestMaxUnuploadedValues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		env      map[string]string
		want     ByteSize
		resolved int64
	}{
		{"default is 8 x merge max", nil, nil, 0, 16 * int64(GiB)},
		{"explicit zero is the default", []string{"-ingest.max-unuploaded=0"}, nil, 0, 16 * int64(GiB)},
		{"default follows merge max", []string{"-ingest.upload-merge-max=512MiB"}, nil, 0, 4 * int64(GiB)},
		{"equal to merge max", []string{"-ingest.max-unuploaded=2GiB"}, nil, 2 * GiB, 2 * int64(GiB)},
		{"explicit", []string{"-ingest.max-unuploaded=100GiB"}, nil, 100 * GiB, 100 * int64(GiB)},
		{"env", nil, map[string]string{"PARKANA_INGEST_MAX_UNUPLOADED": "4GiB"}, 4 * GiB, 4 * int64(GiB)},
		{"flag beats env", []string{"-ingest.max-unuploaded=3GiB"}, map[string]string{"PARKANA_INGEST_MAX_UNUPLOADED": "4GiB"}, 3 * GiB, 3 * int64(GiB)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Ingest.MaxUnuploaded != tc.want {
				t.Errorf("MaxUnuploaded = %d, want %d", c.Ingest.MaxUnuploaded, tc.want)
			}
			if got := c.Ingest.MaxUnuploadedBytes(); got != tc.resolved {
				t.Errorf("MaxUnuploadedBytes() = %d, want %d", got, tc.resolved)
			}
		})
	}
}

func TestIngestMaxUnuploadedInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"below merge max", []string{"-ingest.max-unuploaded=1GiB"}, nil, "-ingest.max-unuploaded"},
		{"one byte below merge max", []string{"-ingest.upload-merge-max=1024", "-ingest.upload-min=1", "-ingest.max-unuploaded=1023"}, nil, "at least -ingest.upload-merge-max"},
		{"negative", []string{"-ingest.max-unuploaded=-1"}, nil, "max-unuploaded"},
		{"garbage", []string{"-ingest.max-unuploaded=lots"}, nil, "max-unuploaded"},
		{"unit wraps to zero", []string{"-ingest.max-unuploaded=16777216TiB"}, nil, "max-unuploaded"},
		{"env below merge max", nil, map[string]string{"PARKANA_INGEST_MAX_UNUPLOADED": "1MiB"}, "-ingest.max-unuploaded"},
		{"env garbage", nil, map[string]string{"PARKANA_INGEST_MAX_UNUPLOADED": "lots"}, "PARKANA_INGEST_MAX_UNUPLOADED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load accepted %v / %v; want error containing %q, got %v", tc.args, tc.env, tc.want, err)
			}
		})
	}
}

// A hand-built Config is validated the same way, and the resolved default
// saturates instead of overflowing.
func TestIngestMaxUnuploadedHandBuilt(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	neg := c
	neg.Ingest.MaxUnuploaded = -1
	if err := neg.Validate(); err == nil || !strings.Contains(err.Error(), "-ingest.max-unuploaded") {
		t.Errorf("negative MaxUnuploaded: Validate() = %v, want an -ingest.max-unuploaded error", err)
	}
	small := c
	small.Ingest.MaxUnuploaded = small.Ingest.UploadMergeMax - 1
	if err := small.Validate(); err == nil || !strings.Contains(err.Error(), "-ingest.max-unuploaded") {
		t.Errorf("MaxUnuploaded below merge max: Validate() = %v, want an -ingest.max-unuploaded error", err)
	}
	huge := Ingest{UploadMergeMax: math.MaxInt64 / 2}
	if got := huge.MaxUnuploadedBytes(); got != math.MaxInt64 {
		t.Errorf("MaxUnuploadedBytes() with a huge merge max = %d, want saturation at %d", got, int64(math.MaxInt64))
	}
}
