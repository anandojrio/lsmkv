package lsm

type Store struct {
	cfg    Config
	closed bool
	stats  Stats

	wal   *WAL
	mem   *Memtable
	seqNo uint64
}

func Open(cfg Config) (*Store, error) {
	wal, err := OpenWAL(cfg)
	if err != nil {
		return nil, err
	}

	records, err := ReplayWAL(cfg)
	if err != nil {
		_ = wal.Close()
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
		cfg:   cfg,
		wal:   wal,
		mem:   mem,
		seqNo: maxSeq,
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

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	value, tombstone, found := s.mem.Get(key)
	if !found || tombstone {
		return nil, false, nil
	}

	return value, true, nil
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

	return s.stats
}

func (s *Store) Close() error {
	if s.closed {
		return ErrStoreClosed
	}

	if err := s.wal.Close(); err != nil {
		return err
	}

	s.closed = true
	s.stats.EngineStatus = "closed"

	return nil
}
