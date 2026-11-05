package plugin

import (
	"encoding/json/v2"
	"fmt"
	"net/url"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// PluginID is the Grafana plugin identifier. It must match plugin.json.
const PluginID = "parkana-sql-datasource"

// Settings is the resolved data source configuration. The backend URL and
// tenant come only from the Grafana data source settings; queries can never
// supply either.
type Settings struct {
	// Endpoint is the absolute /api/v1/sql URL.
	Endpoint string
	// Tenant is the value sent as X-Scope-OrgID, or empty to let the backend
	// use its default tenant.
	Tenant string
}

// jsonSettings is the subset of the data source JSONData this plugin reads.
type jsonSettings struct {
	URL    string `json:"url"`
	Tenant string `json:"tenant"`
}

// loadSettings resolves Settings from Grafana's data source instance settings.
// The JSONData "url" wins over the standard URL field; an empty JSONData URL
// falls back to the standard field so both configuration styles work.
func loadSettings(settings backend.DataSourceInstanceSettings) (Settings, error) {
	var js jsonSettings
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &js); err != nil {
			return Settings{}, fmt.Errorf("plugin: parse data source settings: %w", err)
		}
	}

	rawURL := strings.TrimSpace(js.URL)
	if rawURL == "" {
		rawURL = strings.TrimSpace(settings.URL)
	}
	if rawURL == "" {
		return Settings{}, fmt.Errorf("plugin: data source URL is required")
	}

	endpoint, err := sqlEndpoint(rawURL)
	if err != nil {
		return Settings{}, err
	}

	tenant := strings.TrimSpace(js.Tenant)
	if strings.ContainsAny(tenant, "\r\n") {
		return Settings{}, fmt.Errorf("plugin: tenant must not contain line breaks")
	}
	return Settings{Endpoint: endpoint, Tenant: tenant}, nil
}

// sqlEndpoint validates the configured URL and appends the SQL API path.
func sqlEndpoint(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("plugin: parse data source URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("plugin: data source URL must use http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("plugin: data source URL %q has no host", rawURL)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("plugin: data source URL must not contain a query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v1/sql"
	u.RawPath = ""
	u.ForceQuery = false
	return u.String(), nil
}
