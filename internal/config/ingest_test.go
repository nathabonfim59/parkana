package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestIngestDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.MaxLineSize != 256*KiB || c.Ingest.MaxRequestSize != 16*MiB {
		t.Errorf("unexpected push limit defaults: %+v", c.Ingest)
	}
	want := []StreamPromotion{{Stream: "*", Columns: []string{"level"}}}
	if !reflect.DeepEqual(c.Ingest.Promotions, want) {
		t.Errorf("Promotions = %+v, want %+v", c.Ingest.Promotions, want)
	}
}

func TestIngestLimitsFlagsAndEnv(t *testing.T) {
	env := map[string]string{
		"PARKANA_INGEST_MAX_REQUEST_SIZE": "32MiB",
		"PARKANA_INGEST_PROMOTED_LABELS":  "job=level",
	}
	c, err := load(t, []string{"-ingest.max-line-size=1MiB"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.MaxLineSize != 1*MiB {
		t.Errorf("flag should beat env: MaxLineSize = %d", int64(c.Ingest.MaxLineSize))
	}
	if c.Ingest.MaxRequestSize != 32*MiB {
		t.Errorf("env not applied: MaxRequestSize = %d", int64(c.Ingest.MaxRequestSize))
	}
	want := []StreamPromotion{{Stream: "*", Columns: []string{"level"}}, {Stream: "job", Columns: []string{"level"}}}
	if !reflect.DeepEqual(c.Ingest.Promotions, want) {
		t.Errorf("Promotions = %+v, want %+v", c.Ingest.Promotions, want)
	}
	// Sizes equal to their ceiling are coherent.
	if _, err := load(t, []string{"-ingest.max-line-size=16MiB", "-ingest.upload-min=2GiB"}, nil); err != nil {
		t.Errorf("boundary sizes rejected: %v", err)
	}
}

func TestPromotedLabels(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want []StreamPromotion
	}{
		{
			name: "one env value with wildcard and stream",
			env:  map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "*=level;checkout=level,namespace"},
			want: []StreamPromotion{{"*", []string{"level"}}, {"checkout", []string{"level", "namespace"}}},
		},
		{
			name: "whitespace tolerated",
			env:  map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": " *=level ; checkout = level , namespace "},
			want: []StreamPromotion{{"*", []string{"level"}}, {"checkout", []string{"level", "namespace"}}},
		},
		{
			name: "exact stream keeps default wildcard",
			args: []string{"-ingest.promoted-labels=checkout=namespace"},
			want: []StreamPromotion{{"*", []string{"level"}}, {"checkout", []string{"namespace"}}},
		},
		{
			name: "repeated flags append",
			args: []string{"-ingest.promoted-labels=*=error", "-ingest.promoted-labels=checkout=level,namespace"},
			want: []StreamPromotion{{"*", []string{"error"}}, {"checkout", []string{"level", "namespace"}}},
		},
		{
			name: "explicit wildcard replaces default",
			args: []string{"-ingest.promoted-labels=*=error,job"},
			want: []StreamPromotion{{"*", []string{"error", "job"}}},
		},
		{
			name: "column order preserved",
			env:  map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "checkout=namespace,level"},
			want: []StreamPromotion{{"*", []string{"level"}}, {"checkout", []string{"namespace", "level"}}},
		},
		{
			name: "flag beats env",
			args: []string{"-ingest.promoted-labels=a=x"},
			env:  map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "*=level"},
			want: []StreamPromotion{{"*", []string{"level"}}, {"a", []string{"x"}}},
		},
		{
			name: "empty value resets",
			args: []string{"-ingest.promoted-labels="},
			want: nil,
		},
		{
			name: "reset then wildcard",
			args: []string{"-ingest.promoted-labels=", "-ingest.promoted-labels=*=error"},
			want: []StreamPromotion{{"*", []string{"error"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.Ingest.Promotions, tc.want) {
				t.Errorf("Promotions = %+v, want %+v", c.Ingest.Promotions, tc.want)
			}
		})
	}
}

func TestPromotedFor(t *testing.T) {
	c, err := load(t, []string{"-ingest.promoted-labels=*=level;checkout=level,namespace"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.Ingest.PromotedFor("checkout"), []string{"level", "namespace"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PromotedFor(checkout) = %v, want %v", got, want)
	}
	if got, want := c.Ingest.PromotedFor("other"), []string{"level"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PromotedFor(other) = %v, want wildcard %v", got, want)
	}
	// The result is a copy: mutating it must not change the config.
	got := c.Ingest.PromotedFor("checkout")
	got[0] = "x"
	if again := c.Ingest.PromotedFor("checkout"); !reflect.DeepEqual(again, []string{"level", "namespace"}) {
		t.Errorf("PromotedFor result not a copy: %v", again)
	}
	// No promotions at all select nothing.
	c.Ingest.Promotions = nil
	if got := c.Ingest.PromotedFor("checkout"); got != nil {
		t.Errorf("PromotedFor with no promotions = %v, want nil", got)
	}
}

func TestIngestInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"zero line size", []string{"-ingest.max-line-size=0"}, nil, "-ingest.max-line-size"},
		{"zero request size", []string{"-ingest.max-request-size=0"}, nil, "-ingest.max-request-size"},
		{"negative line size", []string{"-ingest.max-line-size=-1MiB"}, nil, "invalid size"},
		{"line above request", []string{"-ingest.max-line-size=32MiB"}, nil, "must not exceed -ingest.max-request-size"},
		{"line above env request", []string{"-ingest.max-line-size=32MiB"}, map[string]string{"PARKANA_INGEST_MAX_REQUEST_SIZE": "1MiB"}, "must not exceed"},
		{"upload min above merge max", []string{"-ingest.upload-min=4GiB"}, nil, "must not exceed -ingest.upload-merge-max"},
		{"zero upload min", []string{"-ingest.upload-min=0"}, nil, "must be positive"},
		{"zero upload merge max", []string{"-ingest.upload-merge-max=0"}, nil, "must be positive"},
		{"assignment without equals", []string{"-ingest.promoted-labels=checkout"}, nil, "not stream=col1,col2"},
		{"empty stream", []string{"-ingest.promoted-labels==level"}, nil, "empty stream name"},
		{"no columns", []string{"-ingest.promoted-labels=checkout="}, nil, "empty column"},
		{"trailing comma", []string{"-ingest.promoted-labels=*=level,"}, nil, "empty column"},
		{"invalid stream name", []string{"-ingest.promoted-labels=Checkout=level"}, nil, "invalid stream name"},
		{"stream too long", []string{"-ingest.promoted-labels=" + strings.Repeat("a", 129) + "=level"}, nil, "invalid stream name"},
		{"invalid column name", []string{"-ingest.promoted-labels=*=my level"}, nil, "invalid column name"},
		{"column starting with digit", []string{"-ingest.promoted-labels=*=9bad"}, nil, "invalid column name"},
		{"reserved column line", []string{"-ingest.promoted-labels=*=line"}, nil, "reserved"},
		{"reserved column ts", []string{"-ingest.promoted-labels=*=_ts"}, nil, "reserved"},
		{"reserved column labels", []string{"-ingest.promoted-labels=*=labels"}, nil, "reserved"},
		{"reserved column meta", []string{"-ingest.promoted-labels=*=meta"}, nil, "reserved"},
		{"duplicate column", []string{"-ingest.promoted-labels=*=level,level"}, nil, "listed twice"},
		{"case-colliding columns", []string{"-ingest.promoted-labels=*=level,Level"}, nil, "differs only in case"},
		{"reserved column any case", []string{"-ingest.promoted-labels=*=LINE"}, nil, "reserved"},
		{"duplicate stream flags", []string{"-ingest.promoted-labels=a=x", "-ingest.promoted-labels=a=y"}, nil, "duplicate assignment"},
		{"duplicate wildcard flags", []string{"-ingest.promoted-labels=*=x", "-ingest.promoted-labels=*=y"}, nil, "duplicate assignment"},
		{"env reserved column", nil, map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "*=line"}, "reserved"},
		{"env invalid stream", nil, map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "Check=level"}, "invalid stream name"},
		{"env duplicate wildcards", nil, map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "*=x;*=y"}, "duplicate assignment"},
		{"env duplicate streams", nil, map[string]string{"PARKANA_INGEST_PROMOTED_LABELS": "a=x;a=y"}, "duplicate assignment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Load accepted %v / %v; want error containing %q, got %v", tc.args, tc.env, tc.want, err)
			}
		})
	}
}

func TestIngestValidateHandBuilt(t *testing.T) {
	c, err := load(t, []string{"-roles=ingester"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ps   []StreamPromotion
		want string
	}{
		{"two wildcards", []StreamPromotion{{"*", []string{"a"}}, {"*", []string{"b"}}}, "duplicate wildcard"},
		{"duplicate stream", []StreamPromotion{{"a", []string{"x"}}, {"a", []string{"y"}}}, "duplicate assignment"},
		{"reserved column", []StreamPromotion{{"a", []string{"line"}}}, "reserved"},
		{"no columns", []StreamPromotion{{"a", nil}}, "has no columns"},
		{"invalid column", []StreamPromotion{{"a", []string{"9bad"}}}, "invalid column name"},
		{"invalid stream", []StreamPromotion{{"BAD", []string{"x"}}}, "invalid stream name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alt := c
			alt.Ingest.Promotions = tc.ps
			if err := alt.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
	ok := c
	ok.Ingest.Promotions = []StreamPromotion{{"*", []string{"level"}}, {"checkout", []string{"level", "namespace"}}}
	if err := ok.Validate(); err != nil {
		t.Errorf("Validate() rejected a valid promotion list: %v", err)
	}
}
