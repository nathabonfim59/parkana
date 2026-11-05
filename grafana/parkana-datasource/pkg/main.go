// Command gpx_parkana_sql_datasource is the Parkana Grafana data source
// backend. Grafana starts it as an external gRPC plugin.
package main

import (
	"log/slog"
	"os"

	"github.com/nathabonfim59/parkana/grafana/parkana-datasource/pkg/plugin"
)

func main() {
	if err := plugin.Serve(); err != nil {
		slog.Error("parkana plugin: serve failed", "error", err)
		os.Exit(1)
	}
}
