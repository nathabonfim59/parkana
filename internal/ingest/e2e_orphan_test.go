package ingest_test

// Non-gated regressions for the A10 orphan accounting helpers. They run on
// every default `go test` without a binary: fixtures are generated in
// t.TempDir or built from the scenario's own expected rows, so a broken
// referenced/orphan distinction, a skipped budget check or a legacy manifest
// decoding mistake fails immediately instead of hiding until milestone 5.

import (
	"errors"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// liveFiles builds the live catalog row map keyed by object key. Only the key
// matters for classification; stats are exercised by the gated final checks.
func liveFiles(keys ...string) map[string]catalog.File {
	m := make(map[string]catalog.File, len(keys))
	for _, key := range keys {
		m[key] = catalog.File{Tenant: tenant.Default, Key: key}
	}
	return m
}

// rowForExpected renders the recovered form of one expected row.
func rowForExpected(exp e2eExpected) e2eRow {
	return e2eRow{
		ts:     exp.ts.UnixMicro(),
		line:   exp.line,
		labels: maps.Clone(exp.labels),
		meta:   maps.Clone(exp.meta),
		level:  exp.level,
	}
}

// keyForExpected derives an object key whose stream/hour partition matches
// exp; the UUID is irrelevant to the checks.
func keyForExpected(exp e2eExpected) string {
	return objstore.DataKey(tenant.Default, exp.stream, exp.ts, uuid.NewV7())
}

// TestE2EObjectClassification covers the referenced/orphan/missing split: a
// live catalog row is accepted, an unreferenced object is an orphan, and a
// catalog row without its physical object is a hard missing-reference failure
// that never becomes an ignored orphan.
func TestE2EObjectClassification(t *testing.T) {
	const (
		a = "default/logs/api/2026/03/14/09/a.parquet"
		b = "default/logs/api/2026/03/14/09/b.parquet"
	)
	for _, tc := range []struct {
		name     string
		live     map[string]catalog.File
		objects  []string
		wantRef  []string
		wantOrph []string
		wantMiss []string
	}{
		{"all referenced", liveFiles(a, b), []string{a, b}, []string{a, b}, nil, nil},
		{"abandoned old key", liveFiles(a), []string{a, b}, []string{a}, []string{b}, nil},
		{"unreferenced only", liveFiles(), []string{a}, nil, []string{a}, nil},
		{"missing referenced object", liveFiles(a, b), []string{a}, []string{a}, nil, []string{b}},
		{"no physical objects", liveFiles(a), nil, nil, nil, []string{a}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, orphans, missing := classifyObjects(tc.live, tc.objects)
			if !slices.Equal(ref, tc.wantRef) {
				t.Fatalf("referenced = %v, want %v", ref, tc.wantRef)
			}
			if !slices.Equal(orphans, tc.wantOrph) {
				t.Fatalf("orphans = %v, want %v", orphans, tc.wantOrph)
			}
			if !slices.Equal(missing, tc.wantMiss) {
				t.Fatalf("missing = %v, want %v", missing, tc.wantMiss)
			}
		})
	}
}

// TestE2EOrphanBudget covers the durable rotation budget, including legacy
// manifests that predate keyRotations: a missing counter reads as zero, so any
// orphan is over budget.
func TestE2EOrphanBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		orphans   int
		rotations uint64
		wantOK    bool
	}{
		{"none at all", 0, 0, true},
		{"one orphan one rotation", 1, 1, true},
		{"two orphans two rotations", 2, 2, true},
		{"one orphan without rotation", 1, 0, false},
		{"two orphans one rotation", 2, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOrphanBudget(tc.orphans, tc.rotations)
			if tc.wantOK && err != nil {
				t.Fatalf("budget rejected: %v", err)
			}
			if !tc.wantOK {
				if err == nil {
					t.Fatalf("budget accepted %d orphan(s) over %d rotation(s)", tc.orphans, tc.rotations)
				}
				if !strings.Contains(err.Error(), "durable key rotations") {
					t.Fatalf("error %q does not identify the orphan budget", err)
				}
			}
		})
	}
}

// TestE2EPhaseCompletion proves a phase can only return once the orphan budget
// was evaluated and passed: exact live rows with an open receipt stay pending
// (a fresh Put may await AddFiles), and the same over-budget orphans fail only
// after the receipt closes.
func TestE2EPhaseCompletion(t *testing.T) {
	orphans := []string{
		"default/logs/api/2026/03/14/09/a.parquet",
		"default/logs/api/2026/03/14/09/b.parquet",
	}
	for _, tc := range []struct {
		name      string
		snap      storeSnapshot
		problem   string
		want      string // "", "pending" or "budget"
		wantInErr string
	}{
		{"settled with no orphans", storeSnapshot{}, "", "", ""},
		{"settled within budget", storeSnapshot{rotations: 2}, "", "", ""},
		{"settled with one orphan", storeSnapshot{orphans: orphans[:1], rotations: 1}, "", "", ""},
		{"incomplete live rows", storeSnapshot{}, "missing IDs [3]", "pending", "catalog-visible rows incomplete"},
		{
			name:      "incomplete live rows win over an over-budget orphan",
			snap:      storeSnapshot{orphans: orphans, rotations: 0},
			problem:   "missing IDs [3]",
			want:      "pending",
			wantInErr: "catalog-visible rows incomplete",
		},
		{
			name:      "exact rows with open receipt are pending, not success",
			snap:      storeSnapshot{orphans: orphans, rotations: 0, openUploads: 1},
			want:      "pending",
			wantInErr: "upload receipt",
		},
		{
			name:      "over budget after the receipt closes",
			snap:      storeSnapshot{orphans: orphans, rotations: 1},
			want:      "budget",
			wantInErr: "durable key rotations",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPhaseComplete(tc.snap, tc.problem)
			switch tc.want {
			case "":
				if err != nil {
					t.Fatalf("completion rejected: %v", err)
				}
			case "pending":
				if !errors.Is(err, errPhasePending) {
					t.Fatalf("error = %v, want a pending phase", err)
				}
				if !strings.Contains(err.Error(), tc.wantInErr) {
					t.Fatalf("error %q does not explain %q", err, tc.wantInErr)
				}
			case "budget":
				if err == nil || errors.Is(err, errPhasePending) {
					t.Fatalf("error = %v, want a hard orphan-budget failure", err)
				}
				if !strings.Contains(err.Error(), tc.wantInErr) {
					t.Fatalf("error %q does not identify %q", err, tc.wantInErr)
				}
			}
		})
	}
}

// TestE2EManifestProjection covers the read-only manifest projection against
// fixtures in t.TempDir: current manifests expose seq, keyRotations and open
// receipts, legacy manifests without the counter read as zero rotations, and
// unknown members are ignored.
func TestE2EManifestProjection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		want      manifestState
		wantErr   bool
		wantInErr string
	}{
		{
			name: "current manifest",
			body: `{"version":1,"seq":7,"tenant":"default","frontier":{"first":1,"last":9},` +
				`"keyRotations":3,"files":[],"uploads":[{"key":"a"},{"key":"b"}]}`,
			want: manifestState{seq: 7, rotations: 3, openUploads: 2},
		},
		{
			name: "legacy manifest has zero rotations",
			body: `{"version":1,"tenant":"default","frontier":{"first":1,"last":9},"files":[]}`,
			want: manifestState{},
		},
		{
			name: "unknown members are ignored",
			body: `{"tenant":"default","frontier":{"first":1,"last":9},"keyRotations":2,` +
				`"parkana-test-extra":"ignored","files":[{"parkana-test-extra":1}]}`,
			want: manifestState{rotations: 2},
		},
		{
			name: "wrong tenant budget is refused",
			body: `{"version":1,"seq":7,"tenant":"other","frontier":{"first":1,"last":9},` +
				`"keyRotations":3,"files":[]}`,
			wantErr:   true,
			wantInErr: "manifest tenant",
		},
		{name: "invalid JSON", body: `{`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeManifestFixture(t, dir, tc.body)
			got, err := readManifestState(dir, tenant.Default)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("invalid manifest accepted: %+v", got)
				}
				if tc.wantInErr != "" && !strings.Contains(err.Error(), tc.wantInErr) {
					t.Fatalf("error %q does not mention %q", err, tc.wantInErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("read manifest state: %v", err)
			}
			if got != tc.want {
				t.Fatalf("manifest state = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// writeManifestFixture writes body as the committed manifest of the default
// tenant under dir.
func writeManifestFixture(t *testing.T, dir, body string) {
	t.Helper()
	manifestDir := path.Join(dir, "parquet", string(tenant.Default))
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatalf("e2e: create fixture dir: %v", err)
	}
	if err := os.WriteFile(path.Join(manifestDir, "manifest.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("e2e: write fixture manifest: %v", err)
	}
}

// TestE2EOrphanRows covers full-row validation of an unreferenced object:
// expected rows with intact content are accepted (duplicates included, since an
// abandoned retry Put can duplicate live rows), while foreign, garbled or
// mispartitioned rows are rejected so nothing can hide in an ignored orphan.
func TestE2EOrphanRows(t *testing.T) {
	expected := expectedEntries(0, 3)
	exp := expected[1]
	valid := rowForExpected(exp)
	validKey := keyForExpected(exp)

	for _, tc := range []struct {
		name    string
		key     string
		rows    []e2eRow
		wantErr string
	}{
		{"valid expected row", validKey, []e2eRow{valid}, ""},
		{"duplicate expected rows", validKey, []e2eRow{valid, valid}, ""},
		{"empty object", validKey, nil, ""},
		{"unknown ID", validKey, []e2eRow{{ts: valid.ts, line: "id=999 stream=api", labels: valid.labels, level: valid.level}}, "unknown ID 999"},
		{"garbled line", validKey, []e2eRow{{ts: valid.ts, line: "not a pushed line", labels: valid.labels, level: valid.level}}, "does not start with id="},
		{"mismatched line", validKey, []e2eRow{{ts: valid.ts, line: "id=1 stream=api", labels: valid.labels, level: valid.level}}, "line"},
		{"mismatched timestamp", validKey, []e2eRow{{ts: valid.ts + 1, line: valid.line, labels: valid.labels, level: valid.level}}, "_ts"},
		{"mismatched level", validKey, []e2eRow{{ts: valid.ts, line: valid.line, labels: valid.labels, level: "garbled"}}, "level"},
		{"unexpected metadata", validKey, []e2eRow{{ts: valid.ts, line: valid.line, labels: valid.labels, meta: map[string]string{"trace_id": "x"}, level: valid.level}}, "meta"},
		{"wrong stream partition", keyForExpected(e2eExpected{stream: "other", ts: exp.ts}), []e2eRow{valid}, "stream"},
		{"wrong hour partition", keyForExpected(e2eExpected{stream: exp.stream, ts: exp.ts.Add(time.Hour)}), []e2eRow{valid}, "hour partition"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOrphanRows(tc.key, tc.rows, expected)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("valid orphan rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("orphan row accepted, want a %q failure", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestE2EObjectKeysTenantPrefix covers the tenant-wide object listing with real
// files under an os.Root at objstore.DataKey keys: both streams of the requested
// tenant must be listed exactly, while another tenant's data key and the
// tenant's sidecar files must not leak in. A tenant prefix with the empty-stream
// double slash lists nothing and is the regression this test pins down.
func TestE2EObjectKeysTenantPrefix(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("e2e: open fixture root: %v", err)
	}
	defer root.Close()

	const otherTenant = tenant.ID("other")
	apiKey := objstore.DataKey(tenant.Default, "api", e2eBase, uuid.NewV7())
	webKey := objstore.DataKey(tenant.Default, "web", e2eBase.Add(2*time.Hour), uuid.NewV7())
	otherKey := objstore.DataKey(otherTenant, "api", e2eBase, uuid.NewV7())
	for _, key := range []string{apiKey, webKey, otherKey, objstore.SidecarKey(apiKey)} {
		writeObjectFixture(t, root, key)
	}

	got, err := objectKeys(root, tenant.Default)
	if err != nil {
		t.Fatalf("e2e: list tenant objects: %v", err)
	}
	slices.Sort(got)
	want := []string{apiKey, webKey}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("objectKeys(default) = %v, want exactly %v", got, want)
	}

	other, err := objectKeys(root, otherTenant)
	if err != nil {
		t.Fatalf("e2e: list other tenant objects: %v", err)
	}
	if !slices.Equal(other, []string{otherKey}) {
		t.Fatalf("objectKeys(other) = %v, want exactly [%s]", other, otherKey)
	}
}

// writeObjectFixture creates key with placeholder bytes under root.
func writeObjectFixture(t *testing.T, root *os.Root, key string) {
	t.Helper()
	if err := root.MkdirAll(path.Dir(key), 0o755); err != nil {
		t.Fatalf("e2e: create fixture dir for %s: %v", key, err)
	}
	f, err := root.OpenFile(key, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("e2e: create fixture object %s: %v", key, err)
	}
	if _, err := f.WriteString("placeholder"); err != nil {
		f.Close()
		t.Fatalf("e2e: write fixture object %s: %v", key, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("e2e: close fixture object %s: %v", key, err)
	}
}

// TestE2EExpectedOrphanWithinBudget ties the guards together: an unreferenced
// object holding a valid expected row is allowed while the tenant has a durable
// rotation for it, and the same orphan row is still rejected when its content
// is wrong.
func TestE2EExpectedOrphanWithinBudget(t *testing.T) {
	expected := expectedEntries(0, 3)
	liveKey := keyForExpected(expected[1])
	orphanKey := keyForExpected(expected[2])
	row := rowForExpected(expected[2])

	ref, orphans, missing := classifyObjects(liveFiles(liveKey), []string{liveKey, orphanKey})
	if !slices.Equal(ref, []string{liveKey}) || !slices.Equal(orphans, []string{orphanKey}) || len(missing) != 0 {
		t.Fatalf("classification = ref %v, orphans %v, missing %v", ref, orphans, missing)
	}
	if err := checkOrphanRows(orphanKey, []e2eRow{row}, expected); err != nil {
		t.Fatalf("expected-row orphan rejected: %v", err)
	}
	if err := checkOrphanBudget(len(orphans), 1); err != nil {
		t.Fatalf("orphan within its rotation budget rejected: %v", err)
	}
	if err := checkOrphanBudget(len(orphans), 0); err == nil {
		t.Fatalf("orphan accepted with a legacy zero-rotation manifest")
	}

	row.level = "garbled"
	if err := checkOrphanRows(orphanKey, []e2eRow{row}, expected); err == nil {
		t.Fatalf("orphan with mismatched content accepted")
	}
}
