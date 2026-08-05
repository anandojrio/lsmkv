package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func ReplayWAL(cfg Config) ([]WALRecord, error) {
	path := filepath.Join(cfg.DataDir, "wal.log")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read wal for replay: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	var records []WALRecord
	offset := 0

	for offset < len(data) {
		if len(data[offset:]) < walHeaderSize {
			break
		}

		keyLen := binary.LittleEndian.Uint32(data[offset+walKeyLenOffset : offset+walValueLenOffset])
		valueLen := binary.LittleEndian.Uint32(data[offset+walValueLenOffset : offset+walCRCOffset])

		recordLen := walHeaderSize + int(keyLen) + int(valueLen)

		if len(data[offset:]) < recordLen {
			break
		}

		record, err := DecodeWALRecord(data[offset : offset+recordLen])
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("decode wal record at offset %d: %w", offset, err)
		}

		records = append(records, record)
		offset += recordLen
	}

	return records, nil
}
