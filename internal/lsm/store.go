package lsm

type Store struct {
	cfg    Config
	closed bool
	stats  Stats
}

func Open(cfg Config) (*Store, error) {
	store := &Store{
		cfg: cfg,
		stats: Stats{
			EngineStatus: "stub",
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

	return ErrNotImplemented
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	return nil, false, ErrNotImplemented
}

func (s *Store) Delete(key []byte) error {
	if s.closed {
		return ErrStoreClosed
	}

	if len(key) == 0 {
		return ErrInvalidArgument
	}

	return ErrNotImplemented
}

func (s *Store) Stats() Stats {
	return s.stats
}

func (s *Store) Close() error {
	if s.closed {
		return ErrStoreClosed
	}

	s.closed = true
	s.stats.EngineStatus = "closed"

	return nil
}
