package lsm

import (
	"fmt"
	"os"
	"path/filepath"
)

type WAL struct {
	path         string
	file         *os.File
	fsyncEveryN  int
	appendCount  int
	bytesWritten int64
	lastSeqNo    uint64
}

func OpenWAL(cfg Config) (*WAL, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	path := filepath.Join(cfg.DataDir, "wal.log")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}

	wal := &WAL{
		path:         path,
		file:         file,
		fsyncEveryN:  cfg.WALFsyncEveryN,
		bytesWritten: info.Size(),
	}

	return wal, nil
}

func (w *WAL) Append(record WALRecord) error {
	if w.file == nil {
		return ErrStoreClosed
	}

	encoded, err := record.Encode()
	if err != nil {
		return err
	}

	n, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("write wal record: %w", err)
	}

	if n != len(encoded) {
		return fmt.Errorf("%w: partial wal write", ErrIOFailure)
	}

	w.appendCount++
	w.bytesWritten += int64(n)
	w.lastSeqNo = record.SeqNo

	if w.fsyncEveryN > 0 && w.appendCount%w.fsyncEveryN == 0 {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal: %w", err)
		}
	}

	return nil
}

func (w *WAL) Path() string {
	return w.path
}

func (w *WAL) BytesWritten() int64 {
	return w.bytesWritten
}

func (w *WAL) LastSeqNo() uint64 {
	return w.lastSeqNo
}

func (w *WAL) Close() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	err := w.file.Close()
	w.file = nil
	if err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	return nil
}
