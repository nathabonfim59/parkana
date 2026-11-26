package config

import (
	"strings"
	"testing"
	"time"
)

func TestIngestQuarantineRetention(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		want    time.Duration
		wantErr string
	}{
		{name: "default is a week", want: 168 * time.Hour},
		{name: "env", env: map[string]string{"PARKANA_INGEST_QUARANTINE_RETENTION": "36h"}, want: 36 * time.Hour},
		{name: "zero keeps forever", env: map[string]string{"PARKANA_INGEST_QUARANTINE_RETENTION": "0"}, want: 0},
		{
			name: "flag beats env",
			args: []string{"-ingest.quarantine-retention=2h"},
			env:  map[string]string{"PARKANA_INGEST_QUARANTINE_RETENTION": "36h"},
			want: 2 * time.Hour,
		},
		{name: "negative flag", args: []string{"-ingest.quarantine-retention=-1h"}, wantErr: "-ingest.quarantine-retention must not be negative"},
		{name: "negative env", env: map[string]string{"PARKANA_INGEST_QUARANTINE_RETENTION": "-1s"}, wantErr: "-ingest.quarantine-retention must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Ingest.QuarantineRetention != tc.want {
				t.Fatalf("QuarantineRetention = %s, want %s", c.Ingest.QuarantineRetention, tc.want)
			}
		})
	}
}
