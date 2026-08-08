package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	cfg    Config
	closed bool
	stats  Stats

	mu sync.RWMutex

	wal        *WAL
	mem        *Memtable
	immutables []*Memtable
	seqNo      uint64
	version    *Version
	manifest   *Manifest
}

func Open(cfg Config) (*Store, error) {
	manifest, err := loadManifest(cfg)
	if err != nil {
		return nil, err
	}

	readers := make([]*SSTableReader, 0, len(manifest.Tables))
	for _, t := range manifest.Tables {
		r, err := OpenSSTableReader(filepath.Join(cfg.DataDir, t.File))
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open manifest sstable %s: %w", t.File, err)
		}
		readers = append(readers, r)
	}

	version := newVersionFromManifest(manifest, readers)

	wal, err := OpenWAL(cfg)
	if err != nil {
		_ = version.Close()
		return nil, err
	}

	records, err := ReplayWAL(cfg)
	if err != nil {
		_ = wal.Close()
		_ = version.Close()
		return nil, err
	}

	mem := newMemtable()
	var maxSeq uint64

	for _, rec := range records {
		switch rec.Op {
		case WALOpPut:
			mem.Put(rec.Key, rec.Value, rec.SeqNo)
		case WALOpDel:
			mem.Delete(rec.Key, rec.SeqNo)
		}

		if rec.SeqNo > maxSeq {
			maxSeq = rec.SeqNo
		}
	}

	store := &Store{
		cfg:        cfg,
		wal:        wal,
		mem:        mem,
		immutables: nil,
		seqNo:      maxSeq,
		version:    version,
		manifest:   manifest,
		stats: Stats{
			EngineStatus: "open",
		},
	}

	return store, nil
}

func (s *Store) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpPut, SeqNo: s.seqNo, Key: key, Value: value}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Put(key, value, s.seqNo)

	return s.rotateAndFlushIfNeededLocked()
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if value, tombstone, found := s.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return value, true, nil
	}

	for i := len(s.immutables) - 1; i >= 0; i-- {
		if value, tombstone, found := s.immutables[i].Get(key); found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	entry, err := s.version.Get(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if entry.tombstone {
		return nil, false, nil
	}

	return entry.value, true, nil
}

func (s *Store) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpDel, SeqNo: s.seqNo, Key: key}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Delete(key, s.seqNo)

	return s.rotateAndFlushIfNeededLocked()
}

func (s *Store) rotateAndFlushIfNeededLocked() error {
	if s.mem.Bytes() < int64(s.cfg.MemtableMaxBytes) {
		return nil
	}

	if s.cfg.MaxImmutableTables > 0 && len(s.immutables) >= s.cfg.MaxImmutableTables {
		return ErrTooManyImmutables
	}

	s.immutables = append(s.immutables, s.mem)
	s.mem = newMemtable()

	return s.flushOldestImmutableLocked()
}

func (s *Store) flushOldestImmutableLocked() error {
	if len(s.immutables) == 0 {
		return nil
	}

	imm := s.immutables[0]
	entries := imm.AllEntries()
	if len(entries) == 0 {
		s.immutables = s.immutables[1:]
		return nil
	}

	id := s.manifest.nextTableID()
	filename := fmt.Sprintf("%06d.sst", id)
	path := filepath.Join(s.cfg.DataDir, filename)

	w := NewSSTableWriter(s.cfg)
	for _, e := range entries {
		w.Add(e.key, e.value, e.seqNo, e.tombstone)
	}

	if err := w.Flush(path); err != nil {
		return fmt.Errorf("flush immutable to sstable: %w", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat flushed sstable: %w", err)
	}

	minKey, maxKey := entries[0].key, entries[len(entries)-1].key
	minSeq, maxSeq := entries[0].seqNo, entries[0].seqNo
	for _, e := range entries[1:] {
		if e.seqNo < minSeq {
			minSeq = e.seqNo
		}
		if e.seqNo > maxSeq {
			maxSeq = e.seqNo
		}
	}

	info := ManifestTable{
		ID:       id,
		File:     filename,
		MinKey:   string(minKey),
		MaxKey:   string(maxKey),
		MinSeqNo: minSeq,
		MaxSeqNo: maxSeq,
		FileSize: fi.Size(),
	}

	newManifest := s.manifest.withNewTable(info)
	if err := saveManifest(s.cfg, newManifest); err != nil {
		return fmt.Errorf("save manifest after flush: %w", err)
	}

	reader, err := OpenSSTableReader(path)
	if err != nil {
		return fmt.Errorf("open freshly flushed sstable: %w", err)
	}

	s.version = s.version.withPublishedFlush(reader, newManifest.Epoch)
	s.manifest = newManifest
	s.immutables = s.immutables[1:]

	if err := s.wal.Reset(); err != nil {
		return fmt.Errorf("reset wal after flush: %w", err)
	}

	return nil
}

// Compact runs at most one size-tiered compaction cycle: it merges the two
// oldest SSTables in the current manifest into one, publishes the resulting
// manifest, and rebuilds the store's Version so future reads see the merged
// table instead of the two originals.
//
// It is a no-op (returns nil) if fewer than two SSTables currently exist.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	nextManifest, err := runCompactionOnce(s.cfg, s.manifest)
	if err != nil {
		return err
	}

	if nextManifest == s.manifest {
		return nil
	}

	readers := make([]*SSTableReader, 0, len(nextManifest.Tables))
	for _, table := range nextManifest.Tables {
		reader, err := OpenSSTableReader(filepath.Join(s.cfg.DataDir, table.File))
		if err != nil {
			for _, opened := range readers {
				opened.Close()
			}
			return fmt.Errorf("reopen sstables after compaction: %w", err)
		}
		readers = append(readers, reader)
	}

	oldVersion := s.version
	s.version = newVersionFromManifest(nextManifest, readers)
	s.manifest = nextManifest

	if oldVersion != nil {
		oldVersion.Close()
	}

	return nil
}

// FlushAll writes every queued immutable memtable to an SSTable.
//
// It is intentionally synchronous for now. Later, the background flush worker
// will use the same internal flush routine, while this method remains useful
// for tests, explicit shutdown handling, and a future CLI command.
func (s *Store) FlushAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	for len(s.immutables) > 0 {
		if err := s.flushOldestImmutableLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.stats.BytesWritten = s.wal.BytesWritten()
	s.stats.LastSeqNo = s.seqNo
	s.stats.ActiveEntries = s.mem.Len()
	s.stats.ActiveBytes = s.mem.Bytes()
	s.stats.SSTCount = len(s.version.SSTables)

	var sstTotalBytes int64
	for _, t := range s.manifest.Tables {
		sstTotalBytes += t.FileSize
	}
	s.stats.SSTTotalBytes = sstTotalBytes

	s.stats.ImmutablesCount = len(s.immutables)
	s.stats.ImmutablesBytes = 0
	for _, imm := range s.immutables {
		s.stats.ImmutablesBytes += imm.Bytes()
	}

	return s.stats
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if err := s.wal.Close(); err != nil {
		return err
	}

	if err := s.version.Close(); err != nil {
		return err
	}

	s.closed = true
	s.stats.EngineStatus = "closed"

	return nil
}
