package config

import (
	"strings"
	"testing"
	"time"
)

func TestPipelineDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Pipeline.PollInterval != DefaultPipelinePollInterval ||
		c.Pipeline.ReloadTimeout != DefaultPipelineReloadTimeout {
		t.Fatalf("Pipeline defaults = %+v, want %+v",
			c.Pipeline, Pipeline{PollInterval: DefaultPipelinePollInterval, ReloadTimeout: DefaultPipelineReloadTimeout})
	}
	if c.Pipeline.PollInterval != time.Second || c.Pipeline.ReloadTimeout != 5*time.Second {
		t.Fatalf("documented defaults changed: %+v", c.Pipeline)
	}
}

func TestPipelineEnvAndFlagPrecedence(t *testing.T) {
	env := map[string]string{
		"PARKANA_PIPELINE_POLL_INTERVAL":  "250ms",
		"PARKANA_PIPELINE_RELOAD_TIMEOUT": "2s",
	}
	for _, tc := range []struct {
		name string
		args []string
		want Pipeline
	}{
		{
			name: "env",
			want: Pipeline{PollInterval: 250 * time.Millisecond, ReloadTimeout: 2 * time.Second},
		},
		{
			name: "flags beat env",
			args: []string{"-pipeline.poll-interval=3s", "-pipeline.reload-timeout=40s"},
			want: Pipeline{PollInterval: 3 * time.Second, ReloadTimeout: 40 * time.Second},
		},
		{
			name: "flags without env",
			args: []string{"-pipeline.poll-interval=1m", "-pipeline.reload-timeout=30s"},
			want: Pipeline{PollInterval: time.Minute, ReloadTimeout: 30 * time.Second},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Pipeline != tc.want {
				t.Errorf("Pipeline = %+v, want %+v", c.Pipeline, tc.want)
			}
		})
	}
}

func TestPipelineRejectsNonPositiveDurations(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{
			name: "zero poll flag",
			args: []string{"-pipeline.poll-interval=0s"},
			want: "-pipeline.poll-interval must be positive",
		},
		{
			name: "negative poll flag",
			args: []string{"-pipeline.poll-interval=-1s"},
			want: "-pipeline.poll-interval must be positive",
		},
		{
			name: "zero reload flag",
			args: []string{"-pipeline.reload-timeout=0s"},
			want: "-pipeline.reload-timeout must be positive",
		},
		{
			name: "negative reload flag",
			args: []string{"-pipeline.reload-timeout=-500ms"},
			want: "-pipeline.reload-timeout must be positive",
		},
		{
			name: "zero poll env",
			env:  map[string]string{"PARKANA_PIPELINE_POLL_INTERVAL": "0s"},
			want: "-pipeline.poll-interval must be positive",
		},
		{
			name: "negative reload env",
			env:  map[string]string{"PARKANA_PIPELINE_RELOAD_TIMEOUT": "-2s"},
			want: "-pipeline.reload-timeout must be positive",
		},
		{
			name: "bad duration",
			args: []string{"-pipeline.poll-interval=soon"},
			want: "parse error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil {
				t.Fatal("Load accepted a non-positive pipeline duration")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
