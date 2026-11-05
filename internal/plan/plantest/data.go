// Package plantest is the conformance suite every plan.Source must pass, plus
// the fixture dataset it runs on and a loader that stores the dataset the way
// the ingester does (Parquet objects listed in the catalog).
package plantest

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// T0 is the start of the fixture data.
var T0 = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)

// OtherTenant holds rows no default-tenant selection may return.
const OtherTenant tenant.ID = "other"

// File is one Parquet file of the fixture dataset.
type File struct {
	Tenant tenant.ID
	Stream string
	// Promoted labels are stored as columns, the rest in the labels map.
	Promoted []string
	// Rows sorted by TS (µs precision).
	Rows []plan.Row
	// NoSidecar makes Load store the file without a search sidecar.
	NoSidecar bool
}

// Lines used by the fixtures: SQL LIKE wildcards, quotes, backslashes,
// unicode, JSON, logfmt, an empty line and a newline.
var Lines = []string{
	"GET /api/users 200 12ms",
	"error: connection timeout",
	"ERROR disk full",
	"café ☕ ünïcode",
	`{"msg":"hi","status":500}`,
	"100% done_ok",
	"it's a 'quote'",
	`path C:\temp\x`,
	`level=info msg="ok" dur=3ms`,
	"",
	"multi\nline",
	"POST /api/orders 201 40ms",
}

func row(ts time.Time, line string, labels, meta map[string]string) plan.Row {
	return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(labels), Meta: logs.FromMap(meta)}
}

// Dataset returns the fixture files. It is deterministic.
//
//   - api (default tenant) is split over files with different promotions:
//     level is a column in some files and a map entry in another, and the
//     files overlap in time with equal timestamps across files.
//   - web promotes level and pod; batch has no service_name (job only).
//   - bulk adds 3000 api rows 1ms apart, more than one DuckDB vector.
//   - OtherTenant has api rows that must stay invisible to the default tenant.
//   - caseLabels (api) promotes "Level", a different label from "level", next
//     to files promoting "level"; it also carries map labels "LINE" and
//     "Labels" that differ from base column names only in case. DuckDB
//     resolves column names case-insensitively; label names are not.
//   - dups (api, nothing promoted) re-delivers 30 a1 rows exactly (a retried
//     push), one of them twice, plus one copy whose metadata differs and so is
//     a distinct row. A Source must count each exact copy once. It is the
//     one file Load stores without a search sidecar.
func Dataset() []File {
	envs := []string{"prod", "dev", "prod"}
	levels := []string{"info", "warn", "error", ""}
	api := func(i int, base time.Time) plan.Row {
		l := map[string]string{"service_name": "api", "env": envs[i%3], "pod": fmt.Sprintf("p%d", i%2+1)}
		if lv := levels[i%4]; lv != "" {
			l["level"] = lv
		}
		if i%7 == 0 {
			delete(l, "env")
		}
		var meta map[string]string
		if i%5 == 0 {
			meta = map[string]string{"trace_id": fmt.Sprintf("t%04d", i)}
		}
		return row(base.Add(time.Duration(i)*time.Second+time.Duration(i%3)*time.Microsecond), Lines[i%len(Lines)], l, meta)
	}

	var a1, a2, web, batch, bulk, other, caseLabels []plan.Row
	for i := range 200 {
		a1 = append(a1, api(i, T0))
	}
	// Boundary rows: exactly T0+60s and one µs later.
	a1 = append(a1,
		row(T0.Add(60*time.Second), "boundary exact", map[string]string{"service_name": "api", "level": "info", "env": "prod"}, nil),
		row(T0.Add(60*time.Second+time.Microsecond), "boundary plus one", map[string]string{"service_name": "api", "level": "info", "env": "prod"}, nil),
	)
	// a2 overlaps a1 (100s..299s) with the same timestamps for i in 100..199.
	for i := 100; i < 300; i++ {
		r := api(i, T0)
		r.Line = "a2 " + r.Line
		a2 = append(a2, r)
	}
	for i := range 150 {
		l := map[string]string{"service_name": "web", "pod": fmt.Sprintf("w%d", i%3), "env": envs[(i+1)%3]}
		if lv := levels[(i+1)%4]; lv != "" {
			l["level"] = lv
		}
		if i%10 == 0 {
			l["Weird_Label9"] = "x"
		}
		web = append(web, row(T0.Add(time.Duration(i)*1500*time.Millisecond), Lines[(i+3)%len(Lines)], l, nil))
	}
	for i := range 50 {
		batch = append(batch, row(T0.Add(time.Duration(i)*7*time.Second), fmt.Sprintf("batch step %d error=%v", i, i%4 == 0),
			map[string]string{"job": "batch", "host": "h1"}, map[string]string{"run": "r1"}))
	}
	for i := range 3000 {
		l := map[string]string{"service_name": "api", "env": "prod", "level": levels[i%3], "pod": "bulk"}
		bulk = append(bulk, row(T0.Add(400*time.Second+time.Duration(i)*time.Millisecond), fmt.Sprintf("bulk %d %s", i, Lines[i%len(Lines)]), l, nil))
	}
	for i := range 50 {
		other = append(other, api(i, T0))
	}
	for i := range 40 {
		l := map[string]string{"service_name": "api", "Level": []string{"High", "Low"}[i%2], "env": "prod"}
		if i%4 < 2 {
			l["level"] = "info"
		}
		if i%5 == 0 {
			l["LINE"] = "x"
			l["Labels"] = "y"
		}
		caseLabels = append(caseLabels, row(T0.Add(600*time.Second+time.Duration(i)*time.Second), fmt.Sprintf("case %d %s", i, Lines[i%len(Lines)]), l, nil))
	}
	sortRows := func(rs []plan.Row) []plan.Row {
		slices.SortStableFunc(rs, func(a, b plan.Row) int { return a.TS.Compare(b.TS) })
		return rs
	}
	var dups []plan.Row
	for i := 0; i < 90; i += 3 {
		dups = append(dups, a1[i])
	}
	dups = append(dups, a1[0])
	changed := a1[3]
	changed.Meta = logs.FromMap(map[string]string{"trace_id": "retry-differs"})
	dups = append(dups, changed)
	return []File{
		{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: sortRows(a1)},
		{Tenant: tenant.Default, Stream: "api", Promoted: nil, Rows: sortRows(a2)},
		{Tenant: tenant.Default, Stream: "web", Promoted: []string{"level", "pod"}, Rows: sortRows(web)},
		{Tenant: tenant.Default, Stream: "batch", Promoted: []string{"level"}, Rows: sortRows(batch)},
		{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level", "pod"}, Rows: sortRows(bulk)},
		{Tenant: tenant.Default, Stream: "api", Promoted: []string{"Level"}, Rows: sortRows(caseLabels)},
		{Tenant: tenant.Default, Stream: "api", Promoted: nil, Rows: sortRows(dups), NoSidecar: true},
		{Tenant: OtherTenant, Stream: "api", Promoted: []string{"level"}, Rows: sortRows(other)},
	}
}

// Mem returns a MemSource holding files.
func Mem(files []File) *plan.MemSource {
	m := plan.NewMemSource()
	for _, f := range files {
		m.Add(f.Tenant, f.Rows...)
	}
	return m
}

// Encode writes f as one Parquet file in Parkana's schema and returns its
// bytes and stats.
func Encode(f File) ([]byte, storage.FileStats, error) {
	sc, err := schema.ForStream(f.Promoted)
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), sc)
	defer rb.Release()
	fields := rb.Fields()
	appendMap := func(b *array.MapBuilder, ls logs.Labels) {
		b.Append(true)
		kb := b.KeyBuilder().(*array.StringBuilder)
		ib := b.ItemBuilder().(*array.StringBuilder)
		for _, l := range ls {
			kb.Append(l.Name)
			ib.Append(l.Value)
		}
	}
	for _, r := range f.Rows {
		kept, rest := r.Labels.Split(f.Promoted)
		fields[0].(*array.TimestampBuilder).Append(arrow.Timestamp(schema.TS(r.TS)))
		fields[1].(*array.StringBuilder).Append(r.Line)
		appendMap(fields[2].(*array.MapBuilder), rest)
		appendMap(fields[3].(*array.MapBuilder), r.Meta)
		for i, name := range f.Promoted {
			pb := fields[4+i].(*array.StringBuilder)
			if v, ok := kept.Get(name); ok {
				pb.Append(v)
			} else {
				pb.AppendNull()
			}
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	w, err := storage.NewWriter(&buf, sc, f.Promoted)
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	if err := w.Write(rec); err != nil {
		return nil, storage.FileStats{}, err
	}
	st, err := w.Close()
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	return buf.Bytes(), st, nil
}

// Load stores files as Parquet objects under objstore.DataKey and lists them
// in the catalog, as the ingester's uploader does. Files without NoSidecar
// also get a search sidecar (index.DefaultParams) and HasSidecar; the
// dataset's "dups" file has none, so Sources see both kinds side by side.
func Load(t testing.TB, store objstore.Store, cat catalog.Catalog, files []File) {
	t.Helper()
	ctx := t.Context()
	for _, f := range files {
		data, st, err := Encode(f)
		if err != nil {
			t.Fatalf("plantest: encode %s/%s: %v", f.Tenant, f.Stream, err)
		}
		key := objstore.DataKey(f.Tenant, f.Stream, st.MinTS, uuid.NewV7())
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("plantest: put %s: %v", key, err)
		}
		sidecar := !f.NoSidecar
		if sidecar {
			b, err := index.NewBuilder(index.DefaultParams)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range f.Rows {
				b.Add(r.Line)
			}
			idx, err := b.Encode()
			if err != nil {
				t.Fatalf("plantest: sidecar %s: %v", key, err)
			}
			if err := store.Put(ctx, objstore.SidecarKey(key), bytes.NewReader(idx), int64(len(idx))); err != nil {
				t.Fatalf("plantest: put sidecar %s: %v", key, err)
			}
		}
		err = cat.AddFiles(ctx, catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, LabelsComplete: st.LabelsComplete,
			HasSidecar: sidecar, CreatedAt: time.Now().Truncate(time.Microsecond),
		})
		if err != nil {
			t.Fatalf("plantest: catalog add %s: %v", key, err)
		}
	}
}
