package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/index"
)

func TestIndexDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Index.Params(); got != index.DefaultParams {
		t.Errorf("Index.Params() = %+v, want index.DefaultParams %+v", got, index.DefaultParams)
	}
}

func TestIndexFlags(t *testing.T) {
	c, err := load(t, []string{"-index.ngram=3", "-index.fp-rate=0.001", "-index.max-grams=4096"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := index.Params{N: 3, FPRate: 0.001, MaxGrams: 4096}
	if got := c.Index.Params(); got != want {
		t.Errorf("Index.Params() = %+v, want %+v", got, want)
	}
}

func TestIndexEnv(t *testing.T) {
	env := map[string]string{
		"PARKANA_INDEX_NGRAM":     "5",
		"PARKANA_INDEX_FP_RATE":   "0.005",
		"PARKANA_INDEX_MAX_GRAMS": "1024",
	}
	c, err := load(t, nil, env)
	if err != nil {
		t.Fatal(err)
	}
	want := index.Params{N: 5, FPRate: 0.005, MaxGrams: 1024}
	if got := c.Index.Params(); got != want {
		t.Errorf("Index.Params() = %+v, want %+v", got, want)
	}
}

func TestIndexFlagBeatsEnv(t *testing.T) {
	env := map[string]string{
		"PARKANA_INDEX_NGRAM":     "5",
		"PARKANA_INDEX_FP_RATE":   "0.005",
		"PARKANA_INDEX_MAX_GRAMS": "1024",
	}
	args := []string{"-index.ngram=6", "-index.fp-rate=0.5", "-index.max-grams=2048"}
	c, err := load(t, args, env)
	if err != nil {
		t.Fatal(err)
	}
	want := index.Params{N: 6, FPRate: 0.5, MaxGrams: 2048}
	if got := c.Index.Params(); got != want {
		t.Errorf("flags should beat env: Index.Params() = %+v, want %+v", got, want)
	}
}

// TestIndexInvalid checks that values the flags can parse but
// index.Params.Validate rejects fail Load. Every case is otherwise valid, so
// the only error must be the joined index-section validation error; a flag
// parse failure or an unrelated section error does not satisfy the test.
func TestIndexInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"ngram zero", []string{"-index.ngram=0"}, nil},
		{"ngram negative", []string{"-index.ngram=-1"}, nil},
		{"ngram too large", []string{"-index.ngram=9"}, nil},
		{"max-grams zero", []string{"-index.max-grams=0"}, nil},
		{"max-grams negative", []string{"-index.max-grams=-10"}, nil},
		{"fp-rate zero", []string{"-index.fp-rate=0"}, nil},
		{"fp-rate one", []string{"-index.fp-rate=1"}, nil},
		{"fp-rate negative", []string{"-index.fp-rate=-0.5"}, nil},
		{"fp-rate above one", []string{"-index.fp-rate=1.5"}, nil},
		{"fp-rate NaN", []string{"-index.fp-rate=NaN"}, nil},
		{"fp-rate +Inf", []string{"-index.fp-rate=+Inf"}, nil},
		{"fp-rate -Inf", []string{"-index.fp-rate=-Inf"}, nil},
		{"env ngram too large", nil, map[string]string{"PARKANA_INDEX_NGRAM": "9"}},
		{"env max-grams zero", nil, map[string]string{"PARKANA_INDEX_MAX_GRAMS": "0"}},
		{"env fp-rate NaN", nil, map[string]string{"PARKANA_INDEX_FP_RATE": "NaN"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil {
				t.Fatalf("Load(args=%v, env=%v) accepted", tc.args, tc.env)
			}
			joined, ok := errors.AsType[interface {
				error
				Unwrap() []error
			}](err)
			if !ok {
				t.Fatalf("not a validation error (flag parse?): %v", err)
			}
			if errs := joined.Unwrap(); len(errs) != 1 || !strings.Contains(errs[0].Error(), "-index.") {
				t.Fatalf("want exactly the -index.* validation error, got %v", errs)
			}
		})
	}
}

func TestSidecarBackfill(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want bool
	}{
		{"default on", nil, nil, true},
		{"flag off", []string{"-compactor.sidecar-backfill=false"}, nil, false},
		{"flag on", []string{"-compactor.sidecar-backfill"}, nil, true},
		{"env off", nil, map[string]string{"PARKANA_COMPACTOR_SIDECAR_BACKFILL": "false"}, false},
		{"flag off beats env on", []string{"-compactor.sidecar-backfill=false"}, map[string]string{"PARKANA_COMPACTOR_SIDECAR_BACKFILL": "true"}, false},
		{"flag on beats env off", []string{"-compactor.sidecar-backfill"}, map[string]string{"PARKANA_COMPACTOR_SIDECAR_BACKFILL": "false"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Compactor.SidecarBackfill != tc.want {
				t.Errorf("Compactor.SidecarBackfill = %t, want %t", c.Compactor.SidecarBackfill, tc.want)
			}
		})
	}
}
