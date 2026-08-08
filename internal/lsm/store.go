package lsm

import (
	"errors"
	"fmt"
	"path/filepath"
)

type Store struct {
	cfg    Config
	closed bool
	stats  Stats

	wal     *WAL
	mem     *Memtable
	seqNo   uint64
	version *Version
}

func Open(cfg Config) (*Store, error) {
	manifest, err := loadManifest(cfg)
	if err != nil {
		return nil, err
	}

	// Open a reader for every table the manifest claims is live. If any
	// one of them fails to open, close everything opened so far and
	// refuse to start rather than run in a degraded state with a gap in
	// the table set.
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
		cfg:     cfg,
		wal:     wal,
		mem:     mem,
		seqNo:   maxSeq,
		version: version,
		stats: Stats{
			EngineStatus: "open",
		},
	}

	return store, nil
}

func (s *Store) Put(key, value []byte) error {
	if s.closed {
		return ErrStoreClosed
	}

	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.seqNo++

	rec := WALRecord{
		Op:    WALOpPut,
		SeqNo: s.seqNo,
		Key:   key,
		Value: value,
	}

	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Put(key, value, s.seqNo)

	return nil
}

// Get returns the latest visible value for key. It checks the active
// memtable first; on a miss there, it falls through to the SSTables held
// by the store's current Version, searched newest-to-oldest. A tombstone
// hit at any layer stops the search immediately and reports "not found",
// since a tombstone means the key was deleted after whatever older value
// might still exist on disk.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	if value, tombstone, found := s.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return value, true, nil
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
	if s.closed {
		return ErrStoreClosed
	}

	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.seqNo++

	rec := WALRecord{
		Op:    WALOpDel,
		SeqNo: s.seqNo,
		Key:   key,
	}

	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Delete(key, s.seqNo)

	return nil
}

func (s *Store) Stats() Stats {
	s.stats.BytesWritten = s.wal.BytesWritten()
	s.stats.LastSeqNo = s.seqNo
	s.stats.ActiveEntries = s.mem.Len()
	s.stats.ActiveBytes = s.mem.Bytes()
	s.stats.SSTCount = len(s.version.SSTables)

	return s.stats
}

func (s *Store) Close() error {
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
