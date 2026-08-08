package lsm

import "testing"

func TestStoreCompactMergesFlushedTables(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("alpha"), []byte("v1")); err != nil {
		t.Fatalf("Put alpha v1: %v", err)
	}
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll 1: %v", err)
	}

	if err := store.Put([]byte("alpha"), []byte("v2")); err != nil {
		t.Fatalf("Put alpha v2: %v", err)
	}
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll 2: %v", err)
	}

	if got := len(store.manifest.Tables); got != 2 {
		t.Fatalf("expected 2 sstables before compaction, got %d", got)
	}

	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := len(store.manifest.Tables); got != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", got)
	}

	value, found, err := store.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "v2" {
		t.Fatalf("expected alpha=v2 after compaction, got value=%q found=%v", value, found)
	}
}

func TestStoreCompactNoOpWithOneTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("only"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	value, found, err := store.Get([]byte("only"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "value" {
		t.Fatalf("expected only=value, got value=%q found=%v", value, found)
	}
}
