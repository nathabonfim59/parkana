package plugin

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// rowsFrom decodes fixture rows while preserving raw JSON cells.
func rowsFrom(t *testing.T, raw string) [][]jsontext.Value {
	t.Helper()
	var rows [][]jsontext.Value
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("decode fixture rows: %v", err)
	}
	return rows
}

func TestBuildTableFrame(t *testing.T) {
	columns := []responseColumn{
		{Name: "time", Type: "time", DatabaseType: "TIMESTAMP"},
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
		{Name: "label", Type: "string", DatabaseType: "VARCHAR"},
		{Name: "meta", Type: "json", DatabaseType: "JSON"},
		{Name: "ok", Type: "boolean", DatabaseType: "BOOLEAN"},
	}
	rows := rowsFrom(t, `[
		["2026-11-05T12:00:00.123456789Z", 1.5, "hello", {"a":1,"b":[true,null]}, true],
		[null, null, null, null, null]
	]`)

	frame, err := buildFrame(columns, rows, formatTable, "SELECT 1", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if len(frame.Fields) != len(columns) {
		t.Fatalf("fields = %d, want %d", len(frame.Fields), len(columns))
	}
	if got := frame.Fields[0].Type(); got != data.FieldTypeNullableTime {
		t.Fatalf("time field type = %s, want nullable time", got)
	}
	if got := frame.Fields[1].Type(); got != data.FieldTypeNullableFloat64 {
		t.Fatalf("value field type = %s, want nullable float64", got)
	}
	if got := frame.Fields[2].Type(); got != data.FieldTypeNullableString {
		t.Fatalf("label field type = %s, want nullable string", got)
	}
	if got := frame.Fields[3].Type(); got != data.FieldTypeNullableJSON {
		t.Fatalf("meta field type = %s, want nullable JSON", got)
	}
	if got := frame.Fields[4].Type(); got != data.FieldTypeNullableBool {
		t.Fatalf("ok field type = %s, want nullable bool", got)
	}

	ts, ok := frame.Fields[0].ConcreteAt(0)
	if !ok || ts.(time.Time) != time.Date(2026, 11, 5, 12, 0, 0, 123456789, time.UTC) {
		t.Fatalf("time value = %#v, %v", ts, ok)
	}
	if _, ok := frame.Fields[0].ConcreteAt(1); ok {
		t.Fatal("null time was not preserved")
	}
	value, ok := frame.Fields[1].ConcreteAt(0)
	if !ok || value.(float64) != 1.5 {
		t.Fatalf("value = %#v, %v", value, ok)
	}
	label, ok := frame.Fields[2].ConcreteAt(0)
	if !ok || label.(string) != "hello" {
		t.Fatalf("label = %#v, %v", label, ok)
	}
	meta, ok := frame.Fields[3].ConcreteAt(0)
	if !ok || string(meta.(jsontext.Value)) != `{"a":1,"b":[true,null]}` {
		t.Fatalf("meta = %#v, %v", meta, ok)
	}
	flag, ok := frame.Fields[4].ConcreteAt(0)
	if !ok || !flag.(bool) {
		t.Fatalf("flag = %#v, %v", flag, ok)
	}
	if frame.Meta == nil || frame.Meta.PreferredVisualization != data.VisTypeTable || frame.Meta.Type != data.FrameTypeTable {
		t.Fatalf("table meta = %#v", frame.Meta)
	}
	if frame.Meta.ExecutedQueryString != "SELECT 1" {
		t.Fatalf("executed query = %q", frame.Meta.ExecutedQueryString)
	}
}

func TestBuildFrameNumberPrecision(t *testing.T) {
	columns := []responseColumn{
		{Name: "safe", Type: "number", DatabaseType: "BIGINT"},
		{Name: "unsafe", Type: "number", DatabaseType: "BIGINT"},
		{Name: "huge", Type: "number", DatabaseType: "HUGEINT"},
		{Name: "dec", Type: "number", DatabaseType: "DECIMAL(18,2)"},
		{Name: "dbl", Type: "number", DatabaseType: "DOUBLE"},
		{Name: "unknown", Type: "number"},
	}
	rows := rowsFrom(t, `[[
		9007199254740991,
		9007199254740993,
		170141183460469231731687303715884105727,
		123.45,
		0.1,
		1.5
	]]`)

	frame, err := buildFrame(columns, rows, formatTable, "", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	wantTypes := []data.FieldType{
		data.FieldTypeNullableInt64,
		data.FieldTypeNullableString,
		data.FieldTypeNullableString,
		data.FieldTypeNullableString,
		data.FieldTypeNullableFloat64,
		data.FieldTypeNullableString,
	}
	for i, want := range wantTypes {
		if got := frame.Fields[i].Type(); got != want {
			t.Errorf("field %q type = %s, want %s", frame.Fields[i].Name, got, want)
		}
	}

	wantStrings := map[string]string{
		"unsafe":  "9007199254740993",
		"huge":    "170141183460469231731687303715884105727",
		"dec":     "123.45",
		"unknown": "1.5",
	}
	for _, field := range frame.Fields {
		want, ok := wantStrings[field.Name]
		if !ok {
			continue
		}
		got, ok := field.ConcreteAt(0)
		if !ok || got.(string) != want {
			t.Errorf("field %q = %#v, %v; want %q", field.Name, got, ok, want)
		}
	}
}

func TestBuildFrameNumberErrors(t *testing.T) {
	tests := []struct {
		name   string
		column responseColumn
		cell   string
		want   string
	}{
		{
			name:   "non-finite double",
			column: responseColumn{Name: "v", Type: "number", DatabaseType: "DOUBLE"},
			cell:   `1e999`,
			want:   "finite",
		},
		{
			name:   "string in number column",
			column: responseColumn{Name: "v", Type: "number", DatabaseType: "BIGINT"},
			cell:   `"12"`,
			want:   "JSON number",
		},
		{
			name:   "string in decimal column",
			column: responseColumn{Name: "v", Type: "number", DatabaseType: "DECIMAL(10,2)"},
			cell:   `"12.5"`,
			want:   "JSON number",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := rowsFrom(t, "[[ "+tt.cell+" ]]")
			_, err := buildFrame([]responseColumn{tt.column}, rows, formatTable, "", false)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBuildFrameAllNullNumbers(t *testing.T) {
	columns := []responseColumn{
		{Name: "i", Type: "number", DatabaseType: "BIGINT"},
		{Name: "d", Type: "number", DatabaseType: "DECIMAL(10,2)"},
		{Name: "f", Type: "number", DatabaseType: "DOUBLE"},
	}
	rows := rowsFrom(t, `[[null,null,null]]`)
	frame, err := buildFrame(columns, rows, formatTable, "", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	wantTypes := []data.FieldType{
		data.FieldTypeNullableInt64,
		data.FieldTypeNullableString,
		data.FieldTypeNullableFloat64,
	}
	for i, want := range wantTypes {
		if got := frame.Fields[i].Type(); got != want {
			t.Errorf("field %q type = %s, want %s", frame.Fields[i].Name, got, want)
		}
	}
	for _, field := range frame.Fields {
		if _, ok := field.ConcreteAt(0); ok {
			t.Errorf("field %q value is not null", field.Name)
		}
	}
}

func TestBuildTimeSeriesFrameSorted(t *testing.T) {
	columns := []responseColumn{
		{Name: "time", Type: "time", DatabaseType: "TIMESTAMP"},
		{Name: "avg", Type: "number", DatabaseType: "DOUBLE"},
		{Name: "count", Type: "number", DatabaseType: "BIGINT"},
	}
	rows := rowsFrom(t, `[
		["2026-11-05T12:02:00Z", 2.5, 20],
		["2026-11-05T12:00:00Z", 1.5, 10],
		["2026-11-05T12:01:00Z", 2.0, 15]
	]`)

	frame, err := buildFrame(columns, rows, formatTimeSeries, "SELECT time, avg, count FROM x", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if frame.Meta == nil || frame.Meta.Type != data.FrameTypeTimeSeriesWide || frame.Meta.PreferredVisualization != data.VisTypeGraph {
		t.Fatalf("time series meta = %#v", frame.Meta)
	}
	if len(frame.Fields) != 3 {
		t.Fatalf("fields = %d, want 3", len(frame.Fields))
	}

	wantTimes := []time.Time{
		time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 5, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 11, 5, 12, 2, 0, 0, time.UTC),
	}
	for i, want := range wantTimes {
		got, ok := frame.Fields[0].ConcreteAt(i)
		if !ok || got.(time.Time) != want {
			t.Fatalf("time[%d] = %#v, %v; want %s", i, got, ok, want)
		}
	}
	wantAvg := []float64{1.5, 2.0, 2.5}
	for i, want := range wantAvg {
		got, ok := frame.Fields[1].ConcreteAt(i)
		if !ok || got.(float64) != want {
			t.Fatalf("avg[%d] = %#v, %v; want %v", i, got, ok, want)
		}
	}
	wantCount := []int64{10, 15, 20}
	for i, want := range wantCount {
		got, ok := frame.Fields[2].ConcreteAt(i)
		if !ok || got.(int64) != want {
			t.Fatalf("count[%d] = %#v, %v; want %v", i, got, ok, want)
		}
	}
}

func TestBuildTimeSeriesTimeColumnReordered(t *testing.T) {
	columns := []responseColumn{
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
		{Name: "time", Type: "time", DatabaseType: "TIMESTAMP"},
	}
	rows := rowsFrom(t, `[[1.5, "2026-11-05T12:00:00Z"]]`)
	frame, err := buildFrame(columns, rows, formatTimeSeries, "", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if len(frame.Fields) != 2 || frame.Fields[0].Name != "time" || frame.Fields[1].Name != "value" {
		t.Fatalf("time field is not first: %#v", frame.Fields)
	}
}

func TestBuildFrameTimeSeriesErrors(t *testing.T) {
	tests := []struct {
		name    string
		columns []responseColumn
		rows    string
		want    string
	}{
		{
			name:    "no time column",
			columns: []responseColumn{{Name: "v", Type: "number", DatabaseType: "DOUBLE"}},
			rows:    `[[1]]`,
			want:    `column named "time"`,
		},
		{
			name: "no value column",
			columns: []responseColumn{
				{Name: "time", Type: "time"},
			},
			rows: `[["2026-11-05T12:00:00Z"]]`,
			want: "numeric value column",
		},
		{
			name: "string value",
			columns: []responseColumn{
				{Name: "time", Type: "time"},
				{Name: "v", Type: "string"},
			},
			rows: `[["2026-11-05T12:00:00Z","x"]]`,
			want: "numeric",
		},
		{
			name: "precision fallback value",
			columns: []responseColumn{
				{Name: "time", Type: "time"},
				{Name: "v", Type: "number", DatabaseType: "DECIMAL(18,2)"},
			},
			rows: `[["2026-11-05T12:00:00Z", 1.25]]`,
			want: "CAST it AS DOUBLE",
		},
		{
			name: "null time",
			columns: []responseColumn{
				{Name: "time", Type: "time"},
				{Name: "v", Type: "number", DatabaseType: "DOUBLE"},
			},
			rows: `[[null, 1.0]]`,
			want: "is null",
		},
		{
			name: "malformed time",
			columns: []responseColumn{
				{Name: "time", Type: "time"},
				{Name: "v", Type: "number", DatabaseType: "DOUBLE"},
			},
			rows: `[["yesterday", 1.0]]`,
			want: "not RFC3339",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildFrame(tt.columns, rowsFrom(t, tt.rows), formatTimeSeries, "", false)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBuildFrameEmptyResults(t *testing.T) {
	t.Run("columns with no rows", func(t *testing.T) {
		columns := []responseColumn{
			{Name: "time", Type: "time"},
			{Name: "v", Type: "number", DatabaseType: "DOUBLE"},
		}
		frame, err := buildFrame(columns, nil, formatTimeSeries, "", false)
		if err != nil {
			t.Fatalf("buildFrame: %v", err)
		}
		if len(frame.Fields) != 2 || frame.Fields[0].Len() != 0 || frame.Fields[1].Len() != 0 {
			t.Fatalf("unexpected empty frame: %#v", frame.Fields)
		}
	})

	t.Run("no columns", func(t *testing.T) {
		for _, format := range []format{formatAuto, formatTable} {
			frame, err := buildFrame(nil, nil, format, "", false)
			if err != nil {
				t.Fatalf("format %s: buildFrame: %v", format, err)
			}
			if len(frame.Fields) != 0 {
				t.Fatalf("format %s: fields = %d, want 0", format, len(frame.Fields))
			}
		}
		if _, err := buildFrame(nil, nil, formatTimeSeries, "", false); err == nil {
			t.Fatal("explicit time_series accepted an empty schema")
		}
	})
}

func TestBuildFrameInfersFormat(t *testing.T) {
	timeSeriesColumns := []responseColumn{
		{Name: "time", Type: "time"},
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
	}
	timeSeriesRows := rowsFrom(t, `[
		["2026-11-05T12:01:00Z", 2.0],
		["2026-11-05T12:00:00Z", 1.0]
	]`)
	frame, err := buildFrame(timeSeriesColumns, timeSeriesRows, formatAuto, "", false)
	if err != nil {
		t.Fatalf("auto time series: %v", err)
	}
	if frame.Meta.Type != data.FrameTypeTimeSeriesWide || frame.Fields[0].Name != "time" {
		t.Fatalf("auto time series frame = %#v", frame.Meta)
	}
	if first, _ := frame.Fields[0].ConcreteAt(0); !first.(time.Time).Equal(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("auto time series not sorted: %v", first)
	}

	tableColumns := []responseColumn{
		{Name: "time", Type: "time"},
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
		{Name: "label", Type: "string"},
	}
	tableRows := rowsFrom(t, `[
		["2026-11-05T12:01:00Z", 2.0, "b"],
		["2026-11-05T12:00:00Z", 1.0, "a"]
	]`)
	frame, err = buildFrame(tableColumns, tableRows, formatAuto, "", false)
	if err != nil {
		t.Fatalf("auto table: %v", err)
	}
	if frame.Meta.Type != data.FrameTypeTable {
		t.Fatalf("auto table frame type = %v", frame.Meta.Type)
	}
	if first, _ := frame.Fields[0].ConcreteAt(0); first.(time.Time).Before(time.Date(2026, 11, 5, 12, 1, 0, 0, time.UTC)) {
		t.Fatal("auto table was sorted unexpectedly")
	}

	decimalColumns := []responseColumn{
		{Name: "time", Type: "time"},
		{Name: "value", Type: "number", DatabaseType: "DECIMAL(10,2)"},
	}
	decimalRows := rowsFrom(t, `[["2026-11-05T12:00:00Z", 1.25]]`)
	frame, err = buildFrame(decimalColumns, decimalRows, formatAuto, "", false)
	if err != nil {
		t.Fatalf("auto decimal: %v", err)
	}
	if frame.Meta.Type != data.FrameTypeTable {
		t.Fatalf("decimal auto frame type = %v, want table", frame.Meta.Type)
	}
}

func TestBuildFrameExplicitTableStaysTable(t *testing.T) {
	columns := []responseColumn{
		{Name: "time", Type: "time"},
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
	}
	rows := rowsFrom(t, `[["2026-11-05T12:00:00Z", 1.0]]`)
	frame, err := buildFrame(columns, rows, formatTable, "", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if frame.Meta.Type != data.FrameTypeTable {
		t.Fatalf("frame type = %v, want table", frame.Meta.Type)
	}
}

func TestBuildFrameRejectsDuplicateTimeSeriesValueNames(t *testing.T) {
	columns := []responseColumn{
		{Name: "time", Type: "time", DatabaseType: "TIMESTAMP"},
		{Name: "value", Type: "number", DatabaseType: "INTEGER"},
		{Name: "value", Type: "number", DatabaseType: "INTEGER"},
	}
	rows := rowsFrom(t, `[["2026-11-05T12:00:00Z", 1, 2]]`)

	for _, format := range []format{formatAuto, formatTimeSeries} {
		_, err := buildFrame(columns, rows, format, "", false)
		if err == nil {
			t.Fatalf("format %s accepted duplicate value names", format)
		}
		if !strings.Contains(err.Error(), "unique value column names") ||
			!strings.Contains(err.Error(), "AS") ||
			!strings.Contains(err.Error(), "format table") {
			t.Fatalf("format %s error = %v, want alias hint", format, err)
		}
	}

	frame, err := buildFrame(columns, rows, formatTable, "", false)
	if err != nil {
		t.Fatalf("explicit table rejected duplicate names: %v", err)
	}
	if len(frame.Fields) != 3 || frame.Fields[1].Name != "value" || frame.Fields[2].Name != "value" {
		t.Fatalf("table duplicate names not preserved: %#v", frame.Fields)
	}
}

func TestBuildFrameRejectsDuplicateTimestamps(t *testing.T) {
	columns := []responseColumn{
		{Name: "time", Type: "time"},
		{Name: "value", Type: "number", DatabaseType: "DOUBLE"},
	}
	rows := rowsFrom(t, `[
		["2026-11-05T12:00:00Z", 1.0],
		["2026-11-05T12:00:00Z", 2.0]
	]`)
	for _, format := range []format{formatAuto, formatTimeSeries} {
		_, err := buildFrame(columns, rows, format, "", false)
		if err == nil || !strings.Contains(err.Error(), "unique timestamps") {
			t.Fatalf("format %s error = %v, want unique timestamps", format, err)
		}
	}
}

func TestBuildFrameDuplicateColumnNames(t *testing.T) {
	columns := []responseColumn{
		{Name: "x", Type: "number", DatabaseType: "BIGINT"},
		{Name: "x", Type: "number", DatabaseType: "BIGINT"},
	}
	rows := rowsFrom(t, `[[1,2]]`)
	frame, err := buildFrame(columns, rows, formatTable, "", false)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if len(frame.Fields) != 2 || frame.Fields[0].Name != "x" || frame.Fields[1].Name != "x" {
		t.Fatalf("duplicate fields not preserved: %#v", frame.Fields)
	}
	if first, _ := frame.Fields[0].ConcreteAt(0); first.(int64) != 1 {
		t.Fatalf("first x = %#v", first)
	}
	if second, _ := frame.Fields[1].ConcreteAt(0); second.(int64) != 2 {
		t.Fatalf("second x = %#v", second)
	}
}

func TestBuildFrameValidation(t *testing.T) {
	tests := []struct {
		name    string
		columns []responseColumn
		rows    string
		format  format
		want    string
	}{
		{
			name:    "row width",
			columns: []responseColumn{{Name: "a", Type: "string"}},
			rows:    `[["x","y"]]`,
			format:  formatTable,
			want:    "has 2 cells",
		},
		{
			name:    "unsupported type",
			columns: []responseColumn{{Name: "a", Type: "decimal"}},
			rows:    `[["1"]]`,
			format:  formatTable,
			want:    "unsupported type",
		},
		{
			name:    "empty name",
			columns: []responseColumn{{Name: "", Type: "string"}},
			rows:    `[["1"]]`,
			format:  formatTable,
			want:    "no name",
		},
		{
			name:    "unsupported format",
			columns: []responseColumn{{Name: "a", Type: "string"}},
			rows:    `[["1"]]`,
			format:  format("chart"),
			want:    "unsupported format",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildFrame(tt.columns, rowsFrom(t, tt.rows), tt.format, "", false)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBuildFrameTruncatedNotice(t *testing.T) {
	columns := []responseColumn{{Name: "a", Type: "string"}}
	rows := rowsFrom(t, `[["x"]]`)
	frame, err := buildFrame(columns, rows, formatTable, "", true)
	if err != nil {
		t.Fatalf("buildFrame: %v", err)
	}
	if frame.Meta == nil || len(frame.Meta.Notices) != 1 {
		t.Fatalf("notices = %#v, want one", frame.Meta)
	}
	if !strings.Contains(frame.Meta.Notices[0].Text, "truncated") {
		t.Fatalf("notice = %q", frame.Meta.Notices[0].Text)
	}
}
