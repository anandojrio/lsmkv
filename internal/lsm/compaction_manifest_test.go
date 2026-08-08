package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManifestAfterCompactionReplacesInputTables(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "000005.sst")

	outputBytes := []byte("compacted output")
	if err := os.WriteFile(outputPath, outputBytes, 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	current := &Manifest{
		Version: 1,
		Epoch:   7,
		Tables: []ManifestTable{
			{ID: 4, File: "000004.sst"},
			{ID: 3, File: "000003.sst"},
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"},
		},
	}

	plan := &compactionPlan{
		Inputs: []ManifestTable{
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"},
		},
		OutputID:   5,
		OutputFile: "000005.sst",
	}

	outputEntries := []sstEntry{
		{key: []byte("alpha"), value: []byte("1"), seqNo: 1},
		{key: []byte("bravo"), value: nil, seqNo: 2, tombstone: true},
		{key: []byte("charlie"), value: []byte("3"), seqNo: 3},
	}

	next, err := manifestAfterCompaction(current, plan, outputPath, outputEntries)
	if err != nil {
		t.Fatalf("manifestAfterCompaction: %v", err)
	}

	if next.Version != current.Version {
		t.Fatalf("expected version %d, got %d", current.Version, next.Version)
	}

	if next.Epoch != 8 {
		t.Fatalf("expected epoch 8, got %d", next.Epoch)
	}

	if len(next.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(next.Tables))
	}

	wantIDs := []uint64{5, 4, 3}
	for i, wantID := range wantIDs {
		if next.Tables[i].ID != wantID {
			t.Fatalf(
				"table %d: expected ID %d, got %d",
				i,
				wantID,
				next.Tables[i].ID,
			)
		}
	}

	output := next.Tables[0]
	if output.File != "000005.sst" {
		t.Fatalf("expected output file 000005.sst, got %q", output.File)
	}
	if output.MinKey != "alpha" {
		t.Fatalf("expected min key alpha, got %q", output.MinKey)
	}
	if output.MaxKey != "charlie" {
		t.Fatalf("expected max key charlie, got %q", output.MaxKey)
	}
	if output.MinSeqNo != 1 {
		t.Fatalf("expected min seqNo 1, got %d", output.MinSeqNo)
	}
	if output.MaxSeqNo != 3 {
		t.Fatalf("expected max seqNo 3, got %d", output.MaxSeqNo)
	}
	if output.FileSize != int64(len(outputBytes)) {
		t.Fatalf("expected size %d, got %d", len(outputBytes), output.FileSize)
	}

	// Confirm the input manifest was not modified in place.
	if len(current.Tables) != 4 {
		t.Fatalf("current manifest was mutated; expected 4 tables, got %d", len(current.Tables))
	}
	if current.Epoch != 7 {
		t.Fatalf("current manifest epoch changed; expected 7, got %d", current.Epoch)
	}
}

func TestManifestAfterCompactionRejectsInvalidInputs(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "000005.sst")

	if err := os.WriteFile(outputPath, []byte("output"), 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	current := &Manifest{Version: 1}
	plan := &compactionPlan{
		Inputs: []ManifestTable{
			{ID: 1, File: "000001.sst"},
		},
		OutputID:   2,
		OutputFile: "000002.sst",
	}
	entries := []sstEntry{
		{key: []byte("key"), value: []byte("value"), seqNo: 1},
	}

	tests := []struct {
		name    string
		current *Manifest
		plan    *compactionPlan
		path    string
		entries []sstEntry
	}{
		{
			name:    "nil manifest",
			current: nil,
			plan:    plan,
			path:    outputPath,
			entries: entries,
		},
		{
			name:    "nil plan",
			current: current,
			plan:    nil,
			path:    outputPath,
			entries: entries,
		},
		{
			name:    "empty plan inputs",
			current: current,
			plan: &compactionPlan{
				OutputID:   2,
				OutputFile: "000002.sst",
			},
			path:    outputPath,
			entries: entries,
		},
		{
			name:    "empty output entries",
			current: current,
			plan:    plan,
			path:    outputPath,
			entries: nil,
		},
		{
			name:    "missing output file",
			current: current,
			plan:    plan,
			path:    filepath.Join(cfg.DataDir, "missing.sst"),
			entries: entries,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := manifestAfterCompaction(
				test.current,
				test.plan,
				test.path,
				test.entries,
			); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}
