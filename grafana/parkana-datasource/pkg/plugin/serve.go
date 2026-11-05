package plugin

import (
	"context"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

// NewInstance builds one data source instance from Grafana's settings.
func NewInstance(_ context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	instance, err := newDatasource(settings, defaultOptions())
	if err != nil {
		return nil, err
	}
	return instance, nil
}

// Serve starts the gRPC backend plugin. It blocks until Grafana closes the
// plugin process.
func Serve() error {
	if err := datasource.Manage(PluginID, NewInstance, datasource.ManageOpts{}); err != nil {
		return fmt.Errorf("plugin: serve %s: %w", PluginID, err)
	}
	return nil
}
