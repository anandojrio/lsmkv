package lsm

import (
	"testing"
)

func tinyMemtableConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1
	return cfg
}

func TestPutTriggersFlushAndPublishesSSTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if len(store.immutables) != 0 {
		t.Fatalf("expected no pending immutables after successful flush, got %d", len(store.immutables))
	}

	if len(store.version.SSTables) != 1 {
		t.Fatalf("expected 1 sstable in version, got %d", len(store.version.SSTables))
	}

	if len(store.manifest.Tables) != 1 {
		t.Fatalf("expected 1 table in manifest, got %d", len(store.manifest.Tables))
	}
}

func TestGetFindsValueAfterFlush(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, found, err := store.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected flushed key to be found")
	}
	if string(val) != "v" {
		t.Fatalf("expected value 'v', got %q", val)
	}
}

func TestDeleteFlushPublishesTombstone(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("gone"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete([]byte("gone")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := store.Get([]byte("gone"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("expected tombstoned key to be hidden after flush")
	}
}

func TestReopenLoadsFlushedSSTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}

	if err := s1.Put([]byte("persisted"), []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	val, found, err := s2.Get([]byte("persisted"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found {
		t.Fatal("expected reopened store to find flushed key")
	}
	if string(val) != "yes" {
		t.Fatalf("expected 'yes', got %q", val)
	}
}

func TestStatsReflectFlushedTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("stats-key"), []byte("stats-value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	stats := store.Stats()
	if stats.SSTCount != 1 {
		t.Fatalf("expected SSTCount=1, got %d", stats.SSTCount)
	}
	if stats.ImmutablesCount != 0 {
		t.Fatalf("expected ImmutablesCount=0 after synchronous flush, got %d", stats.ImmutablesCount)
	}
}
