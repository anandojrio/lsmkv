package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALRollsWhenSegmentLimitReached(t *testing.T) {
	cfg := testConfig(t)
	cfg.WALSegmentRollBytes = 100
	cfg.WALFsyncEveryN = 1

	wal, err := OpenWAL(cfg)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 10; i++ {
		record := WALRecord{
			Op:    WALOpPut,
			SeqNo: uint64(i + 1),
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		if err := wal.Append(record); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	if wal.ActiveSegmentID() < 2 {
		t.Fatalf(
			"expected active segment id >= 2 after rolling, got %d",
			wal.ActiveSegmentID(),
		)
	}

	if wal.TotalSegments() < 2 {
		t.Fatalf(
			"expected at least two WAL segments, got %d",
			wal.TotalSegments(),
		)
	}
}

func TestReplayWALAcrossMultipleSegments(t *testing.T) {
	cfg := testConfig(t)
	cfg.WALSegmentRollBytes = 100
	cfg.WALFsyncEveryN = 1

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		value := []byte{byte('0' + i)}

		if err := store.Put(key, value); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		want := string([]byte{byte('0' + i)})

		got, found, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		if !found {
			t.Fatalf("key %q missing after replay", key)
		}
		if string(got) != want {
			t.Fatalf("key %q: got %q, want %q", key, got, want)
		}
	}
}

func TestWALResetRemovesOldSegments(t *testing.T) {
	cfg := testConfig(t)
	cfg.WALSegmentRollBytes = 100

	wal, err := OpenWAL(cfg)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	for i := 0; i < 10; i++ {
		record := WALRecord{
			Op:    WALOpPut,
			SeqNo: uint64(i + 1),
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		if err := wal.Append(record); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}

	if wal.TotalSegments() < 2 {
		t.Fatalf(
			"test setup failed: expected multiple segments, got %d",
			wal.TotalSegments(),
		)
	}

	if err := wal.Reset(); err != nil {
		t.Fatalf("reset wal: %v", err)
	}

	if wal.ActiveSegmentID() != 1 {
		t.Fatalf(
			"active segment after reset: got %d, want 1",
			wal.ActiveSegmentID(),
		)
	}

	if wal.TotalSegments() != 1 {
		t.Fatalf(
			"total segments after reset: got %d, want 1",
			wal.TotalSegments(),
		)
	}

	files, err := filepath.Glob(filepath.Join(wal.Dir(), "*.wal"))
	if err != nil {
		t.Fatalf("glob wal segments: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("expected exactly one segment after reset, got %d: %v", len(files), files)
	}

	if filepath.Base(files[0]) != "000001.wal" {
		t.Fatalf("remaining WAL file: got %q, want 000001.wal", filepath.Base(files[0]))
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
}

func TestReplayWALTruncatesIncompleteSegmentTail(t *testing.T) {
	cfg := testConfig(t)
	cfg.WALSegmentRollBytes = 1024

	wal, err := OpenWAL(cfg)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	first := WALRecord{
		Op:    WALOpPut,
		SeqNo: 1,
		Key:   []byte("good"),
		Value: []byte("record"),
	}
	if err := wal.Append(first); err != nil {
		t.Fatalf("append first record: %v", err)
	}

	second := WALRecord{
		Op:    WALOpPut,
		SeqNo: 2,
		Key:   []byte("partial"),
		Value: []byte("record"),
	}
	encodedSecond, err := second.Encode()
	if err != nil {
		t.Fatalf("encode second record: %v", err)
	}

	// Write only part of the second record directly to simulate a crash.
	if _, err := wal.file.Write(encodedSecond[:10]); err != nil {
		t.Fatalf("write partial record: %v", err)
	}

	path := wal.Path()

	if err := wal.file.Close(); err != nil {
		t.Fatalf("close wal file: %v", err)
	}
	wal.file = nil

	records, err := ReplayWAL(cfg)
	if err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("replayed records: got %d, want 1", len(records))
	}

	if string(records[0].Key) != "good" {
		t.Fatalf("replayed key: got %q, want good", records[0].Key)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat repaired segment: %v", err)
	}

	firstEncoded, err := first.Encode()
	if err != nil {
		t.Fatalf("encode first: %v", err)
	}

	wantSize := int64(walSegmentHeaderSize + len(firstEncoded))
	if info.Size() != wantSize {
		t.Fatalf(
			"repaired segment size: got %d, want %d",
			info.Size(),
			wantSize,
		)
	}
}
