package lsm

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCompactReadersWritesMergedSSTable(t *testing.T) {
	cfg := testConfig(t)

	olderPath := filepath.Join(cfg.DataDir, "older.sst")
	newerPath := filepath.Join(cfg.DataDir, "newer.sst")
	outputPath := filepath.Join(cfg.DataDir, "compacted.sst")

	olderWriter := NewSSTableWriter(cfg)
	olderWriter.Add([]byte("alpha"), []byte("old-alpha"), 1, false)
	olderWriter.Add([]byte("bravo"), []byte("only-in-older"), 2, false)
	olderWriter.Add([]byte("charlie"), []byte("old-charlie"), 3, false)
	if err := olderWriter.Flush(olderPath); err != nil {
		t.Fatalf("flush older table: %v", err)
	}

	newerWriter := NewSSTableWriter(cfg)
	newerWriter.Add([]byte("alpha"), []byte("new-alpha"), 4, false)
	newerWriter.Add([]byte("charlie"), nil, 5, true)
	newerWriter.Add([]byte("delta"), []byte("only-in-newer"), 6, false)
	if err := newerWriter.Flush(newerPath); err != nil {
		t.Fatalf("flush newer table: %v", err)
	}

	olderReader, err := OpenSSTableReader(olderPath)
	if err != nil {
		t.Fatalf("open older table: %v", err)
	}
	defer olderReader.Close()

	newerReader, err := OpenSSTableReader(newerPath)
	if err != nil {
		t.Fatalf("open newer table: %v", err)
	}
	defer newerReader.Close()

	compacted, err := compactReaders(outputPath, cfg, olderReader, newerReader)
	if err != nil {
		t.Fatalf("compactReaders: %v", err)
	}
	defer compacted.Close()

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected compacted output file: %v", err)
	}

	tests := []struct {
		key       string
		value     string
		seqNo     uint64
		tombstone bool
	}{
		{key: "alpha", value: "new-alpha", seqNo: 4, tombstone: false},
		{key: "bravo", value: "only-in-older", seqNo: 2, tombstone: false},
		{key: "charlie", value: "", seqNo: 5, tombstone: true},
		{key: "delta", value: "only-in-newer", seqNo: 6, tombstone: false},
	}

	for _, test := range tests {
		entry, err := compacted.Get([]byte(test.key))
		if err != nil {
			t.Fatalf("Get %q: %v", test.key, err)
		}

		if string(entry.value) != test.value {
			t.Fatalf(
				"Get %q: expected value %q, got %q",
				test.key,
				test.value,
				entry.value,
			)
		}

		if entry.seqNo != test.seqNo {
			t.Fatalf(
				"Get %q: expected seqNo %d, got %d",
				test.key,
				test.seqNo,
				entry.seqNo,
			)
		}

		if entry.tombstone != test.tombstone {
			t.Fatalf(
				"Get %q: expected tombstone=%v, got tombstone=%v",
				test.key,
				test.tombstone,
				entry.tombstone,
			)
		}
	}
}

func TestCompactReadersRejectsNoReaders(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "unused.sst")

	_, err := compactReaders(outputPath, cfg)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCompactReadersRejectsNilReader(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "unused.sst")

	_, err := compactReaders(outputPath, cfg, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}
