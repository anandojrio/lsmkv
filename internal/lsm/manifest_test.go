package lsm

import (
	"os"
	"testing"
)

func TestLoadManifestMissingReturnsEmpty(t *testing.T) {
	cfg := testConfig(t)

	m, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if m.Version != 1 {
		t.Fatalf("Version = %d, want 1", m.Version)
	}
	if m.Epoch != 0 {
		t.Fatalf("Epoch = %d, want 0", m.Epoch)
	}
	if len(m.Tables) != 0 {
		t.Fatalf("Tables len = %d, want 0", len(m.Tables))
	}
}

func TestSaveAndLoadManifestRoundTrip(t *testing.T) {
	cfg := testConfig(t)

	want := &Manifest{
		Version: 1,
		Epoch:   3,
		Tables: []ManifestTable{
			{
				ID:       1,
				File:     "000001.sst",
				MinKey:   "apple",
				MaxKey:   "banana",
				MinSeqNo: 1,
				MaxSeqNo: 5,
				FileSize: 123,
			},
		},
	}

	if err := saveManifest(cfg, want); err != nil {
		t.Fatalf("saveManifest: %v", err)
	}

	got, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}

	if got.Version != want.Version || got.Epoch != want.Epoch || len(got.Tables) != 1 {
		t.Fatalf("loaded manifest mismatch: got %+v want %+v", got, want)
	}
	if got.Tables[0].File != want.Tables[0].File {
		t.Fatalf("table file = %q, want %q", got.Tables[0].File, want.Tables[0].File)
	}
}

func TestLoadManifestCorruptedReturnsError(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	if err := os.WriteFile(path, []byte(`{"version":1,"epoch":`), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected corruption error")
	}
}
