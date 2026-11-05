package ingest

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var (
	testTenant = tenant.Default
	testHour   = time.Date(2026, 11, 3, 21, 0, 0, 0, time.UTC)
	testID1    = uuid.MustParse("01800000-0000-7000-8000-000000000001")
	testID2    = uuid.MustParse("01800000-0000-7000-8000-000000000002")
	testID3    = uuid.MustParse("01800000-0000-7000-8000-000000000003")
)

func testLocalFile(id uuid.UUID, retired bool) LocalFile {
	return LocalFile{
		Tenant: testTenant, ID: id, Stream: "checkout", Hour: testHour,
		Bytes: 100, Rows: 3,
		MinTS:       testHour.Add(time.Second),
		MaxTS:       testHour.Add(time.Minute),
		LabelValues: map[string][]string{"level": {"info", "warn"}},
		PublishedAt: testHour,
		Retired:     retired,
	}
}

func testUpload(id uuid.UUID, stage UploadStage, sources ...uuid.UUID) Upload {
	u := Upload{
		Tenant: testTenant, ID: id, Stream: "checkout", Hour: testHour,
		Key:     objstore.DataKey(testTenant, "checkout", testHour, id),
		Sources: sources, Stage: stage,
		CreatedAt: testHour,
	}
	if stage != UploadReserved {
		u.Temp = uploadTempName(id)
		u.Rows, u.Bytes = 3, 100
		u.MinTS, u.MaxTS = testHour.Add(time.Second), testHour.Add(time.Minute)
	}
	return u
}

func encodeRaw(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestManifestRoundTrip(t *testing.T) {
	file := testLocalFile(testID1, true)
	file.LabelsComplete = true
	m := tenantManifest{
		Version: manifestVersion, Seq: 7, Tenant: testTenant,
		Frontier: Frontier{First: 1, Last: 10},
		Files:    []LocalFile{file, testLocalFile(testID2, true)},
		Uploads:  []Upload{testUpload(testID3, UploadPublished, testID1, testID2)},
	}
	m.Uploads[0].LabelsComplete = true
	data, err := encodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeManifest(data, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 7 || got.Frontier != m.Frontier || len(got.Files) != 2 || len(got.Uploads) != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Files[0].LabelValues["level"][1] != "warn" {
		t.Fatalf("label values lost: %+v", got.Files[0].LabelValues)
	}
	if !got.Files[0].LabelsComplete || got.Files[1].LabelsComplete {
		t.Fatalf("file completeness = %v/%v, want true/false", got.Files[0].LabelsComplete, got.Files[1].LabelsComplete)
	}
	if !got.Uploads[0].LabelsComplete {
		t.Fatal("upload completeness lost in round trip")
	}

	// A manifest written before the fields existed decodes as incomplete. Its
	// encoding must genuinely omit the fields: json/v2's omitempty does not
	// drop a false bool, so only omitzero produces the legacy shape.
	legacy := tenantManifest{
		Version: manifestVersion, Seq: 1, Tenant: testTenant,
		Frontier: Frontier{First: 1, Last: 5},
		Files:    []LocalFile{testLocalFile(testID1, false)},
		Uploads:  []Upload{testUpload(testID2, UploadMerged, testID1)},
	}
	legacyBytes := encodeRaw(t, legacy)
	if bytes.Contains(legacyBytes, []byte("labelsComplete")) {
		t.Fatalf("legacy manifest encodes the labelsComplete field: %s", legacyBytes)
	}
	decoded, err := decodeManifest(legacyBytes, testTenant)
	if err != nil {
		t.Fatalf("legacy manifest rejected: %v", err)
	}
	if decoded.Files[0].LabelsComplete || decoded.Uploads[0].LabelsComplete {
		t.Fatal("manifest without completeness fields decoded as complete")
	}
}

func TestManifestDecodeRejects(t *testing.T) {
	validFile := testLocalFile(testID1, false)
	validUpload := testUpload(testID2, UploadReserved, testID1)

	cases := map[string]func() (tenantManifest, tenant.ID){
		"version": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: 99, Tenant: testTenant}
			return m, testTenant
		},
		"tenant dir mismatch": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant}
			return m, tenant.ID("other")
		},
		"frontier zero bound": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Frontier: Frontier{First: 0, Last: 5}}
			return m, testTenant
		},
		"frontier inverted": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Frontier: Frontier{First: 9, Last: 5}}
			return m, testTenant
		},
		"file nil id": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.ID = uuid.Nil()
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file bad stream": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.Stream = "Bad Stream"
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file non-hour": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.Hour = testHour.Add(time.Minute)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file no publication time": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.PublishedAt = time.Time{}
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file stats outside hour": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.MaxTS = testHour.Add(2 * time.Hour)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file empty rows": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.Rows = 0
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"file cross tenant": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.Tenant = tenant.ID("other")
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}}
			return m, testTenant
		},
		"duplicate file id": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile, validFile}}
			return m, testTenant
		},
		"upload wrong key": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = "default/logs/checkout/2026/11/03/21/deadbeef.parquet"
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload key uuid not v7": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = objstore.DataKey(testTenant, u.Stream, u.Hour, uuid.MustParse("01800000-0000-4000-8000-000000000001"))
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload key non-RFC variant": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = objstore.DataKey(testTenant, u.Stream, u.Hour, uuid.MustParse("01800000-0000-7000-4000-000000000001"))
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload key double slash": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = "default/logs/checkout/2026/11/03/21//" + testID2.String() + ".parquet"
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload key dot component": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = "default/logs/checkout/2026/11/03/21/./" + testID2.String() + ".parquet"
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload key wrong hour": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Key = objstore.DataKey(testTenant, u.Stream, u.Hour.Add(time.Hour), u.ID)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"merged carries put completion": func() (tenantManifest, tenant.ID) {
			u := testUpload(testID2, UploadMerged, testID1)
			u.PutCompletedAt = testHour
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"files without frontier": func() (tenantManifest, tenant.ID) {
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}}
			return m, testTenant
		},
		"upload bad stage": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Stage = "warp"
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload no sources": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Sources = nil
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload nil source": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Sources = []uuid.UUID{uuid.Nil()}
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload dup source": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Sources = []uuid.UUID{testID1, testID1}
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload unknown source": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Sources = []uuid.UUID{testID3}
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"reserved carries stats": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Temp = uploadTempName(u.ID)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"reserved carries completeness": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.LabelsComplete = true
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"merged no temp": func() (tenantManifest, tenant.ID) {
			u := testUpload(testID2, UploadMerged, testID1)
			u.Temp = ""
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"merged zero stats": func() (tenantManifest, tenant.ID) {
			u := testUpload(testID2, UploadMerged, testID1)
			u.Rows = 0
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"merged stats outside hour": func() (tenantManifest, tenant.ID) {
			u := testUpload(testID2, UploadMerged, testID1)
			u.MaxTS = testHour.Add(2 * time.Hour)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"no creation time": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.CreatedAt = time.Time{}
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"shared source across uploads": func() (tenantManifest, tenant.ID) {
			u2 := testUpload(testID3, UploadReserved, testID1)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile},
				Uploads: []Upload{validUpload, u2}}
			return m, testTenant
		},
		"retired source on reserved": func() (tenantManifest, tenant.ID) {
			f := validFile
			f.Retired = true
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{f}, Uploads: []Upload{validUpload}}
			return m, testTenant
		},
		"live source on published": func() (tenantManifest, tenant.ID) {
			u := testUpload(testID2, UploadPublished, testID1)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload cross tenant": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Tenant = tenant.ID("other")
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
		"upload mixes hour": func() (tenantManifest, tenant.ID) {
			u := validUpload
			u.Hour = testHour.Add(time.Hour)
			u.Key = objstore.DataKey(testTenant, u.Stream, u.Hour, u.ID)
			m := tenantManifest{Version: manifestVersion, Tenant: testTenant, Files: []LocalFile{validFile}, Uploads: []Upload{u}}
			return m, testTenant
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			m, id := build()
			// Give every fixture a coherent nonzero frontier except the case
			// that tests the files/uploads-without-frontier guard itself, so
			// each case reaches only its intended check.
			if name != "files without frontier" && m.Frontier == (Frontier{}) {
				m.Frontier = Frontier{First: 1, Last: 5}
			}
			data := encodeRaw(t, m)
			_, err := decodeManifest(data, id)
			if err == nil {
				t.Fatalf("decoded corrupt manifest %q", name)
			}
			if !errors.Is(err, ErrManifestCorrupt) && !errors.Is(err, ErrManifestVersion) {
				t.Fatalf("error %v is not manifest corruption", err)
			}
		})
	}
	// Valid control: the baseline every fixture is built on must decode, or
	// the table proves nothing about the intended invariants.
	m := tenantManifest{Version: manifestVersion, Seq: 1, Tenant: testTenant,
		Frontier: Frontier{First: 1, Last: 5},
		Files:    []LocalFile{validFile}, Uploads: []Upload{validUpload}}
	if _, err := decodeManifest(encodeRaw(t, m), testTenant); err != nil {
		t.Fatalf("valid baseline rejected: %v", err)
	}
}

func TestManifestDecodeRejectsMalformed(t *testing.T) {
	if _, err := decodeManifest([]byte(`{"version":1,"tenant":"default","mystery":1}`), testTenant); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("unknown member accepted: %v", err)
	}
	if _, err := decodeManifest([]byte(`not json`), testTenant); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("malformed manifest accepted: %v", err)
	}
	if _, err := decodeManifest([]byte(`{"version":1,"tenant":"default","frontier":{"first":1,"last":3},"files":[],"uploads":[]}`), testTenant); err != nil {
		t.Fatalf("minimal manifest rejected: %v", err)
	}
}

func TestEarlyHourIsValid(t *testing.T) {
	if err := checkHour(time.Time{}); err != nil {
		t.Fatalf("zero time rejected: %v", err)
	}
	f := testLocalFile(testID1, false)
	f.Hour = time.Time{} // 0001-01-01T00:00:00Z is a valid UTC hour boundary
	f.MinTS, f.MaxTS = time.Time{}, time.Time{}.Add(time.Minute)
	if err := f.validate(testTenant); err != nil {
		t.Fatalf("year-1 file rejected: %v", err)
	}
	if got := f.RelPath(); got != "checkout/0001/01/01/00/"+testID1.String()+".parquet" {
		t.Fatalf("year-1 path = %q", got)
	}
}

func TestCloneManifestIndependence(t *testing.T) {
	m := tenantManifest{
		Version: manifestVersion, Seq: 1, Tenant: testTenant,
		Files:   []LocalFile{testLocalFile(testID1, false)},
		Uploads: []Upload{testUpload(testID2, UploadReserved, testID1)},
	}
	c := cloneManifest(m)
	c.Files[0].Retired = true
	c.Files[0].LabelValues["level"][0] = "mutated"
	c.Uploads[0].Sources[0] = testID3
	c.Uploads[0].LabelValues = map[string][]string{"x": {"y"}}
	if m.Files[0].Retired || m.Files[0].LabelValues["level"][0] != "info" || m.Uploads[0].Sources[0] != testID1 || m.Uploads[0].LabelValues != nil {
		t.Fatal("clone shares state with original")
	}

	scoped := LocalFile{LabelsComplete: true}
	uploadScoped := Upload{LabelsComplete: true}
	clonedFile := scoped.Clone()
	clonedUpload := uploadScoped.Clone()
	if !clonedFile.LabelsComplete || !clonedUpload.LabelsComplete {
		t.Fatal("clone dropped LabelsComplete")
	}
	clonedFile.LabelsComplete = false
	clonedUpload.LabelsComplete = false
	if !scoped.LabelsComplete || !uploadScoped.LabelsComplete {
		t.Fatal("clone shares LabelsComplete storage with original")
	}
}
