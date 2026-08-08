package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestSSTable(t *testing.T, cfg Config, path string, entries []sstEntry) {
	t.Helper()
	writer := NewSSTableWriter(cfg)
	for _, e := range entries {
		writer.Add(e.key, e.value, e.seqNo, e.tombstone)
	}
	if err := writer.Flush(path); err != nil {
		t.Fatalf("flush %s: %v", path, err)
	}
}

func TestRunCompactionOnceMergesTwoOldestTables(t *testing.T) {
	cfg := testConfig(t)

	writeTestSSTable(t, cfg, filepath.Join(cfg.DataDir, "000001.sst"), []sstEntry{
		{key: []byte("alpha"), value: []byte("old-alpha"), seqNo: 1},
		{key: []byte("bravo"), value: []byte("only-in-1"), seqNo: 2},
	})
	writeTestSSTable(t, cfg, filepath.Join(cfg.DataDir, "000002.sst"), []sstEntry{
		{key: []byte("alpha"), value: []byte("new-alpha"), seqNo: 3},
	})

	current := &Manifest{
		Version: 1,
		Epoch:   1,
		Tables: []ManifestTable{
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"},
		},
	}

	next, err := runCompactionOnce(cfg, current)
	if err != nil {
		t.Fatalf("runCompactionOnce: %v", err)
	}

	if len(next.Tables) != 1 {
		t.Fatalf("expected 1 table after compaction, got %d", len(next.Tables))
	}
	if next.Tables[0].ID != 3 {
		t.Fatalf("expected output table ID 3, got %d", next.Tables[0].ID)
	}
	if next.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", next.Epoch)
	}

	if _, err := os.Stat(filepath.Join(cfg.DataDir, "000001.sst")); !os.IsNotExist(err) {
		t.Fatalf("expected 000001.sst to be removed, stat err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "000002.sst")); !os.IsNotExist(err) {
		t.Fatalf("expected 000002.sst to be removed, stat err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, next.Tables[0].File)); err != nil {
		t.Fatalf("expected output file to exist: %v", err)
	}

	savedManifest, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if savedManifest.Epoch != 2 || len(savedManifest.Tables) != 1 {
		t.Fatalf("manifest on disk not updated correctly: %+v", savedManifest)
	}

	reader, err := OpenSSTableReader(filepath.Join(cfg.DataDir, next.Tables[0].File))
	if err != nil {
		t.Fatalf("open compacted table: %v", err)
	}
	defer reader.Close()

	entry, err := reader.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get alpha: %v", err)
	}
	if string(entry.value) != "new-alpha" {
		t.Fatalf("expected new-alpha, got %q", entry.value)
	}

	entry, err = reader.Get([]byte("bravo"))
	if err != nil {
		t.Fatalf("Get bravo: %v", err)
	}
	if string(entry.value) != "only-in-1" {
		t.Fatalf("expected only-in-1, got %q", entry.value)
	}
}

func TestRunCompactionOnceNoOpWithFewerThanTwoTables(t *testing.T) {
	cfg := testConfig(t)

	current := &Manifest{
		Version: 1,
		Epoch:   5,
		Tables: []ManifestTable{
			{ID: 1, File: "000001.sst"},
		},
	}

	next, err := runCompactionOnce(cfg, current)
	if err != nil {
		t.Fatalf("runCompactionOnce: %v", err)
	}

	if next != current {
		t.Fatal("expected the same manifest pointer to be returned as a no-op")
	}
}
