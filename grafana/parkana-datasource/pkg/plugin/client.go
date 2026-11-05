package plugin

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

const (
	defaultTimeout          = 60 * time.Second
	defaultHealthTimeout    = 10 * time.Second
	defaultMaxResponseBytes = 9 << 20
	maxErrorTextBytes       = 8 << 10
)

// options tunes the HTTP behavior of a Datasource. Zero values select the
// defaults; tests use it to shorten timeouts and response caps.
type options struct {
	timeout          time.Duration
	healthTimeout    time.Duration
	maxResponseBytes int64
	// transport overrides the per-instance cloned default transport. Tests use
	// it to observe disposal.
	transport http.RoundTripper
}

// defaultOptions returns the production HTTP settings.
func defaultOptions() options {
	return options{
		timeout:          defaultTimeout,
		healthTimeout:    defaultHealthTimeout,
		maxResponseBytes: defaultMaxResponseBytes,
	}
}

// Datasource is one configured Parkana data source instance. It implements
// QueryData, CheckHealth and lifecycle disposal.
type Datasource struct {
	settings         Settings
	client           *http.Client
	maxResponseBytes int64
	healthTimeout    time.Duration
	now              func() time.Time
}

var (
	_ backend.QueryDataHandler   = (*Datasource)(nil)
	_ backend.CheckHealthHandler = (*Datasource)(nil)
	_ interface{ Dispose() }     = (*Datasource)(nil)
)

// newDatasource builds a Datasource from Grafana settings.
func newDatasource(settings backend.DataSourceInstanceSettings, opts options) (*Datasource, error) {
	resolved, err := loadSettings(settings)
	if err != nil {
		return nil, err
	}
	if opts.timeout <= 0 {
		opts.timeout = defaultTimeout
	}
	if opts.healthTimeout <= 0 {
		opts.healthTimeout = defaultHealthTimeout
	}
	if opts.maxResponseBytes <= 0 {
		opts.maxResponseBytes = defaultMaxResponseBytes
	}
	transport := opts.transport
	if transport == nil {
		// Clone the default transport so Dispose owns this instance's
		// connections and never closes the process-wide pool.
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = base.Clone()
		} else {
			transport = http.DefaultTransport
		}
	}
	return &Datasource{
		settings: resolved,
		client: &http.Client{
			Transport:     transport,
			Timeout:       opts.timeout,
			CheckRedirect: rejectRedirect,
		},
		maxResponseBytes: opts.maxResponseBytes,
		healthTimeout:    opts.healthTimeout,
		now:              time.Now,
	}, nil
}

// rejectRedirect keeps every SQL request, tenant header included, at the
// configured endpoint instead of following a redirect to another host.
func rejectRedirect(req *http.Request, _ []*http.Request) error {
	return fmt.Errorf("plugin: refusing SQL endpoint redirect to %s", req.URL.Redacted())
}

// Dispose releases idle HTTP connections when Grafana drops the instance.
func (d *Datasource) Dispose() {
	d.client.CloseIdleConnections()
}

// sqlRequest is the frozen /api/v1/sql request body.
type sqlRequest struct {
	Query string `json:"query"`
	From  string `json:"from"`
	To    string `json:"to"`
	Limit int    `json:"limit,omitzero"`
}

// queryModel is the per-query JSON from the SQL editor.
type queryModel struct {
	RawSQL string `json:"rawSQL"`
	Format string `json:"format"`
	Limit  *int   `json:"limit"`
}

// preparedQuery is one validated query ready for the endpoint and frame
// conversion.
type preparedQuery struct {
	request sqlRequest
	format  format
}

// QueryData handles every query in one Grafana request. Per-query failures are
// reported in that query's DataResponse so other queries can still succeed.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	for _, query := range req.Queries {
		response.Responses[query.RefID] = d.query(ctx, query)
	}
	return response, nil
}

// query handles one Grafana query.
func (d *Datasource) query(ctx context.Context, query backend.DataQuery) backend.DataResponse {
	prepared, err := d.prepare(query)
	if err != nil {
		return failure(err, backend.StatusBadRequest, backend.ErrorSourcePlugin)
	}
	response, err := d.execute(ctx, prepared.request)
	if err != nil {
		return failure(err, statusFor(err), backend.ErrorSourceDownstream)
	}
	frame, err := buildFrame(response.Columns, response.Rows, prepared.format, prepared.request.Query, response.Truncated)
	if err != nil {
		return failure(err, backend.StatusBadRequest, backend.ErrorSourcePlugin)
	}
	return backend.DataResponse{Frames: data.Frames{frame}}
}

// prepare decodes the query, validates its time range and limit, expands
// macros against the Grafana time range, and builds the endpoint request.
func (d *Datasource) prepare(query backend.DataQuery) (preparedQuery, error) {
	var model queryModel
	if err := json.Unmarshal(query.JSON, &model); err != nil {
		return preparedQuery{}, fmt.Errorf("plugin: parse query: %w", err)
	}
	if strings.TrimSpace(model.RawSQL) == "" {
		return preparedQuery{}, fmt.Errorf("plugin: rawSQL is required")
	}

	resultFormat := format(strings.ToLower(strings.TrimSpace(model.Format)))
	switch resultFormat {
	case formatAuto, formatTable, formatTimeSeries:
	default:
		return preparedQuery{}, fmt.Errorf("plugin: unsupported format %q; use table or time_series", model.Format)
	}

	from := query.TimeRange.From
	to := query.TimeRange.To
	if from.IsZero() || to.IsZero() {
		return preparedQuery{}, fmt.Errorf("plugin: query time range is required")
	}
	if !from.Before(to) {
		return preparedQuery{}, fmt.Errorf("plugin: query time range start must be before end")
	}
	from = from.UTC()
	to = to.UTC()

	var limit int
	if model.Limit != nil {
		switch {
		case *model.Limit < 0:
			return preparedQuery{}, fmt.Errorf("plugin: limit must not be negative")
		case *model.Limit > 0:
			limit = *model.Limit
		}
	}

	interval := query.Interval
	if interval <= 0 && query.MaxDataPoints > 0 {
		interval = to.Sub(from) / time.Duration(query.MaxDataPoints)
	}
	if interval <= 0 {
		interval = time.Second
	}

	expanded := expandMacros(model.RawSQL, MacroValues{From: from, To: to, Interval: interval})
	return preparedQuery{
		request: sqlRequest{
			Query: expanded,
			From:  from.Format(time.RFC3339Nano),
			To:    to.Format(time.RFC3339Nano),
			Limit: limit,
		},
		format: resultFormat,
	}, nil
}

// execute posts one request and decodes a bounded response.
func (d *Datasource) execute(ctx context.Context, request sqlRequest) (*sqlResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("plugin: encode SQL request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, d.settings.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("plugin: build SQL request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	if d.settings.Tenant != "" {
		httpRequest.Header.Set("X-Scope-OrgID", d.settings.Tenant)
	}

	httpResponse, err := d.client.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("plugin: call SQL endpoint: %w", err)
	}
	defer httpResponse.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(httpResponse.Body, d.maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("plugin: read SQL response: %w", err)
	}
	if int64(len(responseBody)) > d.maxResponseBytes {
		return nil, fmt.Errorf("plugin: SQL response exceeds %d bytes", d.maxResponseBytes)
	}
	if httpResponse.StatusCode < http.StatusOK || httpResponse.StatusCode >= http.StatusMultipleChoices {
		return nil, newEndpointError(httpResponse.StatusCode, responseBody)
	}

	var response sqlResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("plugin: decode SQL response: %w", err)
	}
	return &response, nil
}

// endpointError is a non-2xx response with the backend's plain-text message.
type endpointError struct {
	StatusCode int
	Message    string
}

// newEndpointError builds a bounded, readable endpoint error.
func newEndpointError(statusCode int, body []byte) *endpointError {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(statusCode)
	}
	if len(message) > maxErrorTextBytes {
		message = message[:maxErrorTextBytes] + "..."
	}
	return &endpointError{StatusCode: statusCode, Message: message}
}

// Error renders the status and the backend's plain-text message.
func (e *endpointError) Error() string {
	return fmt.Sprintf("plugin: SQL endpoint returned %d %s: %s", e.StatusCode, http.StatusText(e.StatusCode), e.Message)
}

// statusFor maps an execution error to a Grafana response status.
func statusFor(err error) backend.Status {
	if endpoint, ok := errors.AsType[*endpointError](err); ok {
		switch endpoint.StatusCode {
		case http.StatusUnauthorized:
			return backend.StatusUnauthorized
		case http.StatusForbidden:
			return backend.StatusForbidden
		case http.StatusNotFound:
			return backend.StatusNotFound
		case http.StatusTooManyRequests:
			return backend.StatusTooManyRequests
		}
		if endpoint.StatusCode >= 400 && endpoint.StatusCode < 500 {
			return backend.StatusBadRequest
		}
		return backend.StatusInternal
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return backend.StatusTimeout
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return backend.StatusTimeout
	}
	return backend.StatusInternal
}

// failure builds an error DataResponse.
func failure(err error, status backend.Status, source backend.ErrorSource) backend.DataResponse {
	return backend.DataResponse{Error: err, Status: status, ErrorSource: source}
}

// CheckHealth runs a bounded SELECT 1 through the SQL endpoint, so a healthy
// data source proves it can reach the backend and authenticate its tenant.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	ctx, cancel := context.WithTimeout(ctx, d.healthTimeout)
	defer cancel()

	now := d.now().UTC()
	response, err := d.execute(ctx, sqlRequest{
		Query: "SELECT 1",
		From:  now.Format(time.RFC3339Nano),
		To:    now.Add(time.Microsecond).Format(time.RFC3339Nano),
		Limit: 1,
	})
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	if err := validateHealthResponse(response); err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "Parkana SQL endpoint reachable"}, nil
}

// validateHealthResponse requires the exact one-column, one-row numeric 1
// answer to SELECT 1. An empty or reshaped response means the endpoint is not
// actually serving SQL and must not be reported healthy.
func validateHealthResponse(response *sqlResponse) error {
	if len(response.Columns) != 1 {
		return fmt.Errorf("plugin: health check expected 1 column, got %d", len(response.Columns))
	}
	if columnType := strings.ToLower(strings.TrimSpace(response.Columns[0].Type)); columnType != "number" {
		return fmt.Errorf("plugin: health check expected a number column, got %q", response.Columns[0].Type)
	}
	if len(response.Rows) != 1 || len(response.Rows[0]) != 1 {
		return fmt.Errorf("plugin: health check expected 1 row with 1 cell, got %d rows", len(response.Rows))
	}
	cell := response.Rows[0][0]
	if cell.Kind() != jsontext.KindNumber {
		return fmt.Errorf("plugin: health check expected numeric 1, got %s", cell.Kind())
	}
	value, err := strconv.ParseFloat(string(cell), 64)
	if err != nil || value != 1 {
		return fmt.Errorf("plugin: health check expected numeric 1, got %q", string(cell))
	}
	return nil
}
