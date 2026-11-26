package catalog_test

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// opener opens a new catalog instance on one store: every call returns
// another instance over the same data, as a restarted process would. The
// instance is closed when t ends.
type opener func(t *testing.T) catalog.Catalog

// parityBackends are the SQL backends whose stored-value semantics must be
// identical: each newStore makes a fresh, empty store and returns its
// opener. The Postgres one skips via catalogtest.PostgresDSN.
var parityBackends = []struct {
	name     string
	newStore func(t *testing.T) opener
}{
	{"sqlite", func(t *testing.T) opener {
		path := filepath.Join(t.TempDir(), "catalog.db")
		return func(t *testing.T) catalog.Catalog {
			t.Helper()
			c, err := catalog.NewSQLite(path)
			if err != nil {
				t.Fatalf("NewSQLite: %v", err)
			}
			t.Cleanup(func() { c.Close() })
			return c
		}
	}},
	{"postgres", func(t *testing.T) opener {
		dsn := catalogtest.PostgresDSN(t)
		return func(t *testing.T) catalog.Catalog {
			t.Helper()
			return openPostgres(t, dsn)
		}
	}},
}

// TestBackendParity runs the timestamp and boundary cases of the SQLite
// tests (TestSQLiteTimestampPrecision, FilterTimeBoundaries,
// PreEpochTimestamps, PurgeBoundaries, AddFilesPreservesDeletedAt) on every
// SQL backend from one table each, so both store and compare microseconds
// the same way.
func TestBackendParity(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, open opener)
	}{
		{"TimestampPrecision", parityTimestampPrecision},
		{"FilterTimeBoundaries", parityFilterTimeBoundaries},
		{"PreEpochTimestamps", parityPreEpochTimestamps},
		{"PurgeBoundaries", parityPurgeBoundaries},
		{"AddFilesPreservesDeletedAt", parityAddFilesPreservesDeletedAt},
	}
	for _, b := range parityBackends {
		t.Run(b.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) { tc.run(t, b.newStore(t)) })
			}
		})
	}
}

// parityTimestampPrecision: sub-microsecond parts of MinTS, MaxTS and
// CreatedAt are floored to the microsecond.
func parityTimestampPrecision(t *testing.T, open opener) {
	c := open(t)
	f := testFile("k", "api", 0, 0)
	f.MinTS = testStart.Add(1500 * time.Nanosecond)
	f.MaxTS = testStart.Add(2500 * time.Nanosecond)
	f.CreatedAt = testStart.Add(3500 * time.Nanosecond)
	if err := c.AddFiles(t.Context(), f); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default})
	if len(got) != 1 {
		t.Fatalf("got %d files", len(got))
	}
	for _, tc := range []struct {
		field     string
		got, want time.Time
	}{
		{"MinTS", got[0].MinTS, testStart.Add(time.Microsecond)},
		{"MaxTS", got[0].MaxTS, testStart.Add(2 * time.Microsecond)},
		{"CreatedAt", got[0].CreatedAt, testStart.Add(3 * time.Microsecond)},
	} {
		if !tc.got.Equal(tc.want) || tc.got.Location() != time.UTC {
			t.Errorf("%s = %v, want %v UTC (floor to µs)", tc.field, tc.got, tc.want)
		}
	}
}

// parityFilterTimeBoundaries: From and Through are exact for stored
// microsecond values, including sub-microsecond bounds.
func parityFilterTimeBoundaries(t *testing.T, open opener) {
	c := open(t)
	// m covers the single microsecond instant testStart; n covers the next one.
	m := testFile("m", "api", 0, 0)
	n := testFile("n", "api", time.Microsecond, time.Microsecond)
	if err := c.AddFiles(t.Context(), m, n); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	for _, tc := range []struct {
		name string
		flt  catalog.Filter
		want []string
	}{
		{"from exact", catalog.Filter{Tenant: tenant.Default, From: testStart}, []string{"m", "n"}},
		{"from sub-µs after m", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(time.Nanosecond)}, []string{"n"}},
		{"from sub-µs below n", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(time.Microsecond - time.Nanosecond)}, []string{"n"}},
		{"from sub-µs after n", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(time.Microsecond + time.Nanosecond)}, nil},
		{"through exact", catalog.Filter{Tenant: tenant.Default, Through: testStart}, []string{"m"}},
		{"through sub-µs below m", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(-time.Nanosecond)}, nil},
		{"through sub-µs above m", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(999 * time.Nanosecond)}, []string{"m"}},
		{"through sub-µs below n", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(time.Microsecond - time.Nanosecond)}, []string{"m"}},
		{"through exact n", catalog.Filter{Tenant: tenant.Default, Through: testStart.Add(time.Microsecond)}, []string{"m", "n"}},
		{"window", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(500 * time.Nanosecond), Through: testStart.Add(time.Microsecond + 500*time.Nanosecond)}, []string{"n"}},
		{"empty window inside one µs", catalog.Filter{Tenant: tenant.Default, From: testStart.Add(100 * time.Nanosecond), Through: testStart.Add(900 * time.Nanosecond)}, nil},
	} {
		if got := listKeys(t, c, tc.flt); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// parityPreEpochTimestamps: timestamps before 1970 floor toward negative
// infinity and filter like any other.
func parityPreEpochTimestamps(t *testing.T, open opener) {
	c := open(t)
	// -500ns rounds toward negative infinity to -1µs.
	pre := time.Date(1969, 12, 31, 23, 59, 59, 999_999_500, time.UTC)
	stored := time.Date(1969, 12, 31, 23, 59, 59, 999_999_000, time.UTC)
	f := testFile("pre", "api", 0, 0)
	f.MinTS = pre
	f.MaxTS = pre
	if err := c.AddFiles(t.Context(), f); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default})
	if len(got) != 1 || !got[0].MinTS.Equal(stored) || !got[0].MaxTS.Equal(stored) {
		t.Fatalf("pre-epoch round trip = %+v, want %v", got, stored)
	}
	for _, tc := range []struct {
		name string
		flt  catalog.Filter
		want []string
	}{
		{"from before stored", catalog.Filter{Tenant: tenant.Default, From: stored.Add(-time.Nanosecond)}, []string{"pre"}},
		{"from stored", catalog.Filter{Tenant: tenant.Default, From: stored}, []string{"pre"}},
		{"from original", catalog.Filter{Tenant: tenant.Default, From: pre}, nil},
		{"through stored", catalog.Filter{Tenant: tenant.Default, Through: stored}, []string{"pre"}},
		{"through original", catalog.Filter{Tenant: tenant.Default, Through: pre}, []string{"pre"}},
		{"through before stored", catalog.Filter{Tenant: tenant.Default, Through: stored.Add(-time.Nanosecond)}, nil},
		{"through epoch", catalog.Filter{Tenant: tenant.Default, Through: time.Unix(0, 0)}, []string{"pre"}},
	} {
		if keys := listKeys(t, c, tc.flt); !slices.Equal(keys, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, keys, tc.want)
		}
	}
}

// parityPurgeBoundaries: PurgeDeleted removes only files deleted strictly
// before the cutoff, exact at sub-microsecond cutoffs.
func parityPurgeBoundaries(t *testing.T, open opener) {
	c := open(t)
	ctx := t.Context()
	if err := c.AddFiles(ctx,
		testFile("gone", "api", 0, time.Minute),
		testFile("live", "api", time.Minute, 2*time.Minute),
	); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{"gone"}, nil); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	var deletedAt time.Time
	for _, f := range listFiles(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}) {
		if f.Key == "gone" {
			deletedAt = f.DeletedAt
		}
	}
	if deletedAt.IsZero() {
		t.Fatal("gone has no DeletedAt")
	}
	for _, tc := range []struct {
		name   string
		cutoff time.Time
		want   []string
	}{
		{"zero cutoff", time.Time{}, nil},
		{"well before", deletedAt.Add(-time.Hour), nil},
		{"cutoff equal (strict)", deletedAt, nil},
		{"cutoff just below", deletedAt.Add(-time.Nanosecond), nil},
		{"cutoff just after", deletedAt.Add(time.Nanosecond), []string{"gone"}},
	} {
		purged, err := c.PurgeDeleted(ctx, tenant.Default, tc.cutoff)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !slices.Equal(purged, tc.want) {
			t.Fatalf("%s: purged %v, want %v", tc.name, purged, tc.want)
		}
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("all after purge = %v, want [live]", got)
	}
}

// parityAddFilesPreservesDeletedAt: a nonzero DeletedAt given to AddFiles,
// the Unix epoch included, is kept (also across a reopen), a zero one means
// live, and purges are strict at both values.
func parityAddFilesPreservesDeletedAt(t *testing.T, open opener) {
	ctx := t.Context()
	c1 := open(t)
	live := testFile("live", "api", 0, time.Minute)
	deleted := testFile("deleted", "api", time.Minute, 2*time.Minute)
	deleted.DeletedAt = testStart
	epoch := testFile("epoch", "api", 2*time.Minute, 3*time.Minute)
	epoch.DeletedAt = time.Unix(0, 0)
	if err := c1.AddFiles(ctx, live, deleted, epoch); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	wantDeleted := map[string]time.Time{
		"live":    {},
		"deleted": testStart,
		"epoch":   time.Unix(0, 0).UTC(),
	}
	check := func(t *testing.T, c catalog.Catalog, when string) {
		t.Helper()
		if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"live"}) {
			t.Fatalf("%s: live files = %v, want [live]", when, got)
		}
		all := listFiles(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true})
		if len(all) != 3 {
			t.Fatalf("%s: all files = %+v", when, all)
		}
		for _, f := range all {
			want, ok := wantDeleted[f.Key]
			if !ok {
				t.Fatalf("%s: unexpected file %q", when, f.Key)
			}
			if !f.DeletedAt.Equal(want) || f.Deleted() != !want.IsZero() {
				t.Fatalf("%s: %s DeletedAt = %v (deleted %v), want %v", when, f.Key, f.DeletedAt, f.Deleted(), want)
			}
		}
		if got, err := c.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"api"}) {
			t.Fatalf("%s: Streams = %v, %v", when, got, err)
		}
	}
	check(t, c1, "before reopen")
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c2 := open(t)
	check(t, c2, "after reopen")

	// Epoch is purged first because every later cutoff is also after it.
	for _, tc := range []struct {
		name   string
		cutoff time.Time
		want   []string
	}{
		{"at epoch", time.Unix(0, 0), nil},
		{"before epoch", time.Unix(0, 0).Add(-time.Nanosecond), nil},
		{"after epoch", time.Unix(0, 0).Add(time.Nanosecond), []string{"epoch"}},
		{"at deleted-at", testStart, nil},
		{"before deleted-at", testStart.Add(-time.Nanosecond), nil},
		{"after deleted-at", testStart.Add(time.Nanosecond), []string{"deleted"}},
	} {
		purged, err := c2.PurgeDeleted(ctx, tenant.Default, tc.cutoff)
		if err != nil || !slices.Equal(purged, tc.want) {
			t.Fatalf("purge %s = %v, %v, want %v", tc.name, purged, err, tc.want)
		}
	}
	if got := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("all after purges = %v, want [live]", got)
	}
}
