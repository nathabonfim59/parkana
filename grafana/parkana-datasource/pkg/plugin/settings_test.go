package plugin

import (
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestLoadSettings(t *testing.T) {
	tests := []struct {
		name       string
		settings   backend.DataSourceInstanceSettings
		wantURL    string
		wantTenant string
		wantErr    string
	}{
		{
			name: "json data",
			settings: backend.DataSourceInstanceSettings{
				URL:      "http://standard:8080",
				JSONData: []byte(`{"url":"http://custom:9090/base/","tenant":"tenant-a"}`),
			},
			wantURL:    "http://custom:9090/base/api/v1/sql",
			wantTenant: "tenant-a",
		},
		{
			name: "standard URL fallback",
			settings: backend.DataSourceInstanceSettings{
				URL:      "http://parkana:8080/",
				JSONData: []byte(`{"tenant":" tenant-b "}`),
			},
			wantURL:    "http://parkana:8080/api/v1/sql",
			wantTenant: "tenant-b",
		},
		{
			name: "no tenant",
			settings: backend.DataSourceInstanceSettings{
				URL: "https://parkana.example.com",
			},
			wantURL: "https://parkana.example.com/api/v1/sql",
		},
		{
			name:     "missing URL",
			settings: backend.DataSourceInstanceSettings{},
			wantErr:  "URL is required",
		},
		{
			name: "bad scheme",
			settings: backend.DataSourceInstanceSettings{
				URL: "ftp://parkana:8080",
			},
			wantErr: "http or https",
		},
		{
			name: "missing host",
			settings: backend.DataSourceInstanceSettings{
				URL: "http://",
			},
			wantErr: "no host",
		},
		{
			name: "query string",
			settings: backend.DataSourceInstanceSettings{
				URL: "http://parkana:8080?x=1",
			},
			wantErr: "query or fragment",
		},
		{
			name: "tenant newline",
			settings: backend.DataSourceInstanceSettings{
				URL:      "http://parkana:8080",
				JSONData: []byte(`{"tenant":"bad\nvalue"}`),
			},
			wantErr: "line breaks",
		},
		{
			name: "malformed JSON data",
			settings: backend.DataSourceInstanceSettings{
				URL:      "http://parkana:8080",
				JSONData: []byte(`{"url":`),
			},
			wantErr: "parse data source settings",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadSettings(tt.settings)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadSettings: %v", err)
			}
			if got.Endpoint != tt.wantURL || got.Tenant != tt.wantTenant {
				t.Fatalf("settings = %#v, want URL %q tenant %q", got, tt.wantURL, tt.wantTenant)
			}
		})
	}
}

func TestLoadSettingsIgnoresUnknownJSONData(t *testing.T) {
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{"url":"http://parkana:8080","tenant":"t","extra":true}`),
	}
	got, err := loadSettings(settings)
	if err != nil {
		t.Fatalf("loadSettings: %v", err)
	}
	if got.Endpoint != "http://parkana:8080/api/v1/sql" || got.Tenant != "t" {
		t.Fatalf("settings = %#v", got)
	}
}
