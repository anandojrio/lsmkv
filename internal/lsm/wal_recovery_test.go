package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplayWALMissingFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()

	records, err := ReplayWAL(cfg)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

func TestReplayWALValidRecords(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()

	wal, err := OpenWAL(cfg)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	record1 := WALRecord{
		Op:    WALOpPut,
		SeqNo: 1,
		Key:   []byte("alpha"),
		Value: []byte("one"),
	}

	record2 := WALRecord{
		Op:    WALOpDel,
		SeqNo: 2,
		Key:   []byte("beta"),
	}

	if err := wal.Append(record1); err != nil {
		t.Fatalf("append record1 failed: %v", err)
	}

	if err := wal.Append(record2); err != nil {
		t.Fatalf("append record2 failed: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close wal failed: %v", err)
	}

	records, err := ReplayWAL(cfg)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0].Op != WALOpPut {
		t.Fatalf("record 1 op mismatch: got %d", records[0].Op)
	}

	if records[0].SeqNo != 1 {
		t.Fatalf("record 1 seqno mismatch: got %d", records[0].SeqNo)
	}

	if string(records[0].Key) != "alpha" {
		t.Fatalf("record 1 key mismatch: got %q", records[0].Key)
	}

	if string(records[0].Value) != "one" {
		t.Fatalf("record 1 value mismatch: got %q", records[0].Value)
	}

	if records[1].Op != WALOpDel {
		t.Fatalf("record 2 op mismatch: got %d", records[1].Op)
	}

	if records[1].SeqNo != 2 {
		t.Fatalf("record 2 seqno mismatch: got %d", records[1].SeqNo)
	}

	if string(records[1].Key) != "beta" {
		t.Fatalf("record 2 key mismatch: got %q", records[1].Key)
	}

	if len(records[1].Value) != 0 {
		t.Fatalf("record 2 value should be empty, got len=%d", len(records[1].Value))
	}
}

func TestReplayWALTruncatedTail(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()

	walPath := filepath.Join(cfg.DataDir, "wal.log")

	record1 := WALRecord{
		Op:    WALOpPut,
		SeqNo: 1,
		Key:   []byte("first"),
		Value: []byte("ok"),
	}

	record2 := WALRecord{
		Op:    WALOpPut,
		SeqNo: 2,
		Key:   []byte("second"),
		Value: []byte("cut"),
	}

	encoded1, err := record1.Encode()
	if err != nil {
		t.Fatalf("encode record1 failed: %v", err)
	}

	encoded2, err := record2.Encode()
	if err != nil {
		t.Fatalf("encode record2 failed: %v", err)
	}

	truncated2 := encoded2[:len(encoded2)-3]

	data := append(encoded1, truncated2...)

	if err := os.WriteFile(walPath, data, 0o644); err != nil {
		t.Fatalf("write wal file failed: %v", err)
	}

	records, err := ReplayWAL(cfg)
	if err != nil {
		t.Fatalf("ReplayWAL failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 recovered record, got %d", len(records))
	}

	if records[0].SeqNo != 1 {
		t.Fatalf("unexpected recovered seqno: got %d", records[0].SeqNo)
	}

	if string(records[0].Key) != "first" {
		t.Fatalf("unexpected recovered key: got %q", records[0].Key)
	}
}
