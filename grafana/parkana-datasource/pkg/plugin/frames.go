package plugin

import (
	// The SDK's JSON frame field needs the v1 RawMessage type. This is the
	// only v1 use: every encode/decode operation is encoding/json/v2.
	stdjson "encoding/json"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// format is the requested result shape.
type format string

const (
	// formatAuto asks buildFrame to infer time_series for a time column plus
	// numeric value columns and table otherwise.
	formatAuto       format = ""
	formatTable      format = "table"
	formatTimeSeries format = "time_series"
)

// maxSafeInteger is the largest integer exactly representable as a float64.
// Grafana panels ultimately graph float64 values, so integers beyond this
// range become string fields instead of silently losing precision.
const maxSafeInteger = 1<<53 - 1

// fieldKind is the data frame field type chosen for one result column.
type fieldKind int

const (
	kindTime fieldKind = iota
	kindString
	kindBool
	kindJSON
	kindInt64
	kindFloat64
	kindNumericString
)

// responseColumn is one column description from the SQL endpoint.
type responseColumn struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DatabaseType string `json:"databaseType"`
}

// sqlResponse is the /api/v1/sql response. Rows keep the exact JSON tokens
// the endpoint sent, so numbers never pass through float64 on the way in.
type sqlResponse struct {
	Columns   []responseColumn   `json:"columns"`
	Rows      [][]jsontext.Value `json:"rows"`
	Truncated bool               `json:"truncated"`
}

// buildFrame converts a SQL response into one Grafana data frame. An omitted
// format infers time_series for a "time" column plus numeric-only value
// columns, and table otherwise. Table format keeps the endpoint's column
// order and duplicate names. time_series requires the time column, at least
// one numeric value column, unique timestamps and no precision fallback; it
// returns the time field first with rows sorted by time ascending.
func buildFrame(columns []responseColumn, rows [][]jsontext.Value, resultFormat format, executedSQL string, truncated bool) (*data.Frame, error) {
	switch resultFormat {
	case formatAuto, formatTable, formatTimeSeries:
	default:
		return nil, fmt.Errorf("plugin: unsupported format %q", resultFormat)
	}

	for i, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("plugin: row %d has %d cells, want %d", i, len(row), len(columns))
		}
	}

	if len(columns) == 0 {
		if resultFormat == formatTimeSeries {
			return nil, fmt.Errorf(`plugin: format time_series requires a column named "time" with type time`)
		}
		frame := data.NewFrame("")
		frame.SetMeta(frameMeta(formatTable, executedSQL, truncated))
		return frame, nil
	}

	kinds := make([]fieldKind, len(columns))
	for i, column := range columns {
		if column.Name == "" {
			return nil, fmt.Errorf("plugin: column %d has no name", i)
		}
		kind, err := columnKind(column, rows, i)
		if err != nil {
			return nil, err
		}
		kinds[i] = kind
	}

	effectiveFormat := resultFormat
	if effectiveFormat == formatAuto {
		if canInferTimeSeries(columns, kinds) {
			effectiveFormat = formatTimeSeries
		} else {
			effectiveFormat = formatTable
		}
	}

	var order []int
	columnOrder := make([]int, len(columns))
	for i := range columnOrder {
		columnOrder[i] = i
	}
	if effectiveFormat == formatTimeSeries {
		timeIndex := timeColumnIndex(columns)
		if timeIndex < 0 {
			return nil, fmt.Errorf(`plugin: format time_series requires a column named "time" with type time`)
		}
		if len(columns) < 2 {
			return nil, fmt.Errorf("plugin: format time_series requires at least one numeric value column")
		}
		for i, kind := range kinds {
			if i == timeIndex {
				continue
			}
			switch kind {
			case kindInt64, kindFloat64:
			case kindNumericString:
				return nil, fmt.Errorf("plugin: format time_series cannot chart column %q exactly; CAST it AS DOUBLE or use format table", columns[i].Name)
			default:
				return nil, fmt.Errorf("plugin: format time_series requires numeric value columns, but %q is %s", columns[i].Name, columns[i].Type)
			}
		}
		// Wide series are identified by value field name plus labels. This
		// plugin attaches no labels, so duplicate value names would produce
		// indistinguishable series.
		valueNames := make(map[string]struct{}, len(columns)-1)
		for i, column := range columns {
			if i == timeIndex {
				continue
			}
			if _, exists := valueNames[column.Name]; exists {
				return nil, fmt.Errorf("plugin: format time_series requires unique value column names, but %q is repeated; alias each value column (for example AS value_1, AS value_2) or use format table", column.Name)
			}
			valueNames[column.Name] = struct{}{}
		}
		var err error
		order, err = timeOrder(columns, rows, timeIndex)
		if err != nil {
			return nil, err
		}
		// Grafana time series frames expect the time field first, even when
		// the SQL selected it in another position.
		columnOrder[0], columnOrder[timeIndex] = columnOrder[timeIndex], columnOrder[0]
	}

	fields := make([]*data.Field, len(columns))
	for pos, index := range columnOrder {
		field, err := buildField(columns[index], kinds[index], rows, index, order)
		if err != nil {
			return nil, err
		}
		fields[pos] = field
	}
	frame := data.NewFrame("", fields...)
	frame.SetMeta(frameMeta(effectiveFormat, executedSQL, truncated))
	return frame, nil
}

// canInferTimeSeries reports whether the result naturally fits the data plane
// time-series wide contract: a column named time plus at least one numeric
// value column and no other column kinds that would make it a table.
func canInferTimeSeries(columns []responseColumn, kinds []fieldKind) bool {
	timeIndex := timeColumnIndex(columns)
	if timeIndex < 0 || len(columns) < 2 {
		return false
	}
	for i, kind := range kinds {
		if i == timeIndex {
			continue
		}
		if kind != kindInt64 && kind != kindFloat64 {
			return false
		}
	}
	return true
}

// columnKind validates a column's declared type and picks the frame field
// kind for a number column.
func columnKind(column responseColumn, rows [][]jsontext.Value, index int) (fieldKind, error) {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "time":
		return kindTime, nil
	case "string":
		return kindString, nil
	case "boolean":
		return kindBool, nil
	case "json":
		return kindJSON, nil
	case "number":
		return numberKind(column, rows, index)
	default:
		return 0, fmt.Errorf("plugin: column %q has unsupported type %q", column.Name, column.Type)
	}
}

// numberKind decides whether a number column can be a finite numeric field.
// DECIMAL is always kept as exact text; integers beyond the float64-safe range
// are kept as text; FLOAT/DOUBLE/REAL columns become float64 fields after a
// finite-value check during conversion.
func numberKind(column responseColumn, rows [][]jsontext.Value, index int) (fieldKind, error) {
	base := baseDatabaseType(column.DatabaseType)
	switch base {
	case "DECIMAL", "NUMERIC":
		return kindNumericString, validateNumberCells(column.Name, rows, index)
	case "FLOAT", "DOUBLE", "REAL":
		return kindFloat64, validateNumberCells(column.Name, rows, index)
	}

	anyValue := false
	allIntegers := true
	allSafe := true
	for i, row := range rows {
		cell := row[index]
		if cell.Kind() == jsontext.KindNull {
			continue
		}
		if cell.Kind() != jsontext.KindNumber {
			return 0, fmt.Errorf("plugin: column %q row %d is not a JSON number", column.Name, i)
		}
		anyValue = true
		token := string(cell)
		if strings.ContainsAny(token, ".eE") {
			allIntegers = false
			continue
		}
		n, err := strconv.ParseInt(token, 10, 64)
		if err != nil || n > maxSafeInteger || n < -maxSafeInteger {
			allSafe = false
		}
	}

	if !anyValue {
		if isIntegerDatabaseType(base) {
			return kindInt64, nil
		}
		return kindNumericString, nil
	}
	if allIntegers && allSafe {
		return kindInt64, nil
	}
	return kindNumericString, nil
}

// validateNumberCells rejects cells that are not JSON numbers or null.
func validateNumberCells(name string, rows [][]jsontext.Value, index int) error {
	for i, row := range rows {
		switch row[index].Kind() {
		case jsontext.KindNull, jsontext.KindNumber:
		default:
			return fmt.Errorf("plugin: column %q row %d is not a JSON number", name, i)
		}
	}
	return nil
}

// baseDatabaseType strips the precision/scale suffix and upper-cases a
// database type name.
func baseDatabaseType(name string) string {
	base := strings.ToUpper(strings.TrimSpace(name))
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	if i := strings.IndexByte(base, ' '); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	return base
}

// isIntegerDatabaseType reports whether a database type is a DuckDB integer
// type. Wide integer types are included: their values still become numeric
// fields when every value fits the safe range.
func isIntegerDatabaseType(base string) bool {
	switch base {
	case "TINYINT", "INT1", "SMALLINT", "INT2", "INTEGER", "INT4", "BIGINT", "INT8",
		"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "HUGEINT", "UHUGEINT":
		return true
	}
	return false
}

// timeColumnIndex returns the index of the first column named "time" with
// type time, or -1.
func timeColumnIndex(columns []responseColumn) int {
	for i, column := range columns {
		if column.Name == "time" && strings.EqualFold(strings.TrimSpace(column.Type), "time") {
			return i
		}
	}
	return -1
}

// timeOrder parses the time column and returns row indices sorted by time
// ascending. A null or malformed time cannot form a time series.
func timeOrder(columns []responseColumn, rows [][]jsontext.Value, index int) ([]int, error) {
	times := make([]time.Time, len(rows))
	for i, row := range rows {
		t, err := parseTimeCell(row[index])
		if err != nil {
			return nil, fmt.Errorf("plugin: column %q row %d: %w", columns[index].Name, i, err)
		}
		if t == nil {
			return nil, fmt.Errorf("plugin: column %q row %d is null; time_series requires a time for every row", columns[index].Name, i)
		}
		times[i] = *t
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return times[a].Compare(times[b])
	})
	for i := 1; i < len(order); i++ {
		prev, cur := times[order[i-1]], times[order[i]]
		if cur.Equal(prev) {
			return nil, fmt.Errorf(
				"plugin: format time_series requires unique timestamps, but %s repeats; aggregate rows by time (for example with time_bucket) or use format table",
				cur.Format(time.RFC3339Nano))
		}
	}
	return order, nil
}

// buildField converts one column into a typed data frame field. order is nil
// for table frames and a time-ascending row permutation for time series.
func buildField(column responseColumn, kind fieldKind, rows [][]jsontext.Value, index int, order []int) (*data.Field, error) {
	n := len(rows)
	switch kind {
	case kindTime:
		values := make([]*time.Time, n)
		for pos := range n {
			i := rowAt(order, pos)
			t, err := parseTimeCell(rows[i][index])
			if err != nil {
				return nil, fmt.Errorf("plugin: column %q row %d: %w", column.Name, i, err)
			}
			values[pos] = t
		}
		return data.NewField(column.Name, nil, values), nil

	case kindString:
		values := make([]*string, n)
		for pos := range n {
			i := rowAt(order, pos)
			s, err := parseStringCell(rows[i][index])
			if err != nil {
				return nil, fmt.Errorf("plugin: column %q row %d: %w", column.Name, i, err)
			}
			values[pos] = s
		}
		return data.NewField(column.Name, nil, values), nil

	case kindBool:
		values := make([]*bool, n)
		for pos := range n {
			i := rowAt(order, pos)
			v, err := parseBoolCell(rows[i][index])
			if err != nil {
				return nil, fmt.Errorf("plugin: column %q row %d: %w", column.Name, i, err)
			}
			values[pos] = v
		}
		return data.NewField(column.Name, nil, values), nil

	case kindJSON:
		values := make([]*stdjson.RawMessage, n)
		for pos := range n {
			i := rowAt(order, pos)
			cell := rows[i][index]
			if cell.Kind() == jsontext.KindNull {
				continue
			}
			raw := stdjson.RawMessage(slices.Clone(cell))
			values[pos] = &raw
		}
		return data.NewField(column.Name, nil, values), nil

	case kindInt64:
		values := make([]*int64, n)
		for pos := range n {
			i := rowAt(order, pos)
			cell := rows[i][index]
			if cell.Kind() == jsontext.KindNull {
				continue
			}
			v, err := strconv.ParseInt(string(cell), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("plugin: column %q row %d: %w", column.Name, i, err)
			}
			values[pos] = &v
		}
		return data.NewField(column.Name, nil, values), nil

	case kindFloat64:
		values := make([]*float64, n)
		for pos := range n {
			i := rowAt(order, pos)
			cell := rows[i][index]
			if cell.Kind() == jsontext.KindNull {
				continue
			}
			v, err := strconv.ParseFloat(string(cell), 64)
			if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
				return nil, fmt.Errorf("plugin: column %q row %d is not a finite number", column.Name, i)
			}
			values[pos] = &v
		}
		return data.NewField(column.Name, nil, values), nil

	case kindNumericString:
		values := make([]*string, n)
		for pos := range n {
			i := rowAt(order, pos)
			cell := rows[i][index]
			if cell.Kind() == jsontext.KindNull {
				continue
			}
			if cell.Kind() != jsontext.KindNumber {
				return nil, fmt.Errorf("plugin: column %q row %d is not a JSON number", column.Name, i)
			}
			text := string(cell)
			values[pos] = &text
		}
		return data.NewField(column.Name, nil, values), nil
	}
	return nil, fmt.Errorf("plugin: column %q has no field kind", column.Name)
}

// rowAt maps a frame position to the source row.
func rowAt(order []int, pos int) int {
	if order == nil {
		return pos
	}
	return order[pos]
}

// parseTimeCell decodes a null or RFC3339 timestamp cell.
func parseTimeCell(cell jsontext.Value) (*time.Time, error) {
	if cell.Kind() == jsontext.KindNull {
		return nil, nil
	}
	s, err := parseStringCell(cell)
	if err != nil {
		return nil, fmt.Errorf("timestamp must be an RFC3339 string: %w", err)
	}
	if s == nil {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		return nil, fmt.Errorf("timestamp %q is not RFC3339: %w", *s, err)
	}
	t = t.UTC()
	return &t, nil
}

// parseStringCell decodes a null or JSON string cell.
func parseStringCell(cell jsontext.Value) (*string, error) {
	if cell.Kind() == jsontext.KindNull {
		return nil, nil
	}
	if cell.Kind() != jsontext.KindString {
		return nil, fmt.Errorf("value is not a JSON string")
	}
	var s string
	if err := json.Unmarshal(cell, &s); err != nil {
		return nil, fmt.Errorf("decode string: %w", err)
	}
	return &s, nil
}

// parseBoolCell decodes a null or JSON boolean cell.
func parseBoolCell(cell jsontext.Value) (*bool, error) {
	switch cell.Kind() {
	case jsontext.KindNull:
		return nil, nil
	case jsontext.KindTrue:
		v := true
		return &v, nil
	case jsontext.KindFalse:
		v := false
		return &v, nil
	default:
		return nil, fmt.Errorf("value is not a JSON boolean")
	}
}

// frameMeta describes the frame to Grafana.
func frameMeta(resultFormat format, executedSQL string, truncated bool) *data.FrameMeta {
	meta := &data.FrameMeta{ExecutedQueryString: executedSQL}
	if resultFormat == formatTimeSeries {
		// One shared time field with one or more numeric value fields is the
		// data plane wide format (FrameTypeTimeSeriesWide).
		meta.Type = data.FrameTypeTimeSeriesWide
		meta.PreferredVisualization = data.VisTypeGraph
	} else {
		meta.Type = data.FrameTypeTable
		meta.PreferredVisualization = data.VisTypeTable
	}
	if truncated {
		meta.Notices = append(meta.Notices, data.Notice{
			Severity: data.NoticeSeverityWarning,
			Text:     "Result truncated at the requested limit. Increase the limit or narrow the time range.",
		})
	}
	return meta
}
