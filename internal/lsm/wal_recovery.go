package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

func ReplayWAL(cfg Config) ([]WALRecord, error) {
	dir := walDirectory(cfg.DataDir)

	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	var records []WALRecord

	for _, id := range ids {
		path := walSegmentPath(dir, id)

		segmentRecords, err := replayWALSegment(path)
		if err != nil {
			return nil, fmt.Errorf("replay wal segment %06d: %w", id, err)
		}

		records = append(records, segmentRecords...)
	}

	return records, nil
}

func replayWALSegment(path string) ([]WALRecord, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open wal segment: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat wal segment: %w", err)
	}

	if info.Size() < walSegmentHeaderSize {
		return nil, fmt.Errorf("%w: segment is smaller than header", ErrCorruptionDetected)
	}

	header := make([]byte, walSegmentHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("read wal segment header: %w", err)
	}

	if binary.LittleEndian.Uint32(header[0:4]) != walSegmentMagic {
		return nil, fmt.Errorf("%w: bad wal segment magic", ErrCorruptionDetected)
	}

	if header[4] != walSegmentVersion {
		return nil, fmt.Errorf(
			"%w: unsupported wal segment version %d",
			ErrCorruptionDetected,
			header[4],
		)
	}

	var records []WALRecord
	offset := int64(walSegmentHeaderSize)

	for {
		record, recordLen, err := readWALRecord(file)

		if err == io.EOF {
			break
		}

		if err != nil {
			// A crash can leave only an incomplete final record. The safe
			// response is to discard exactly the incomplete tail and retain
			// all preceding valid records.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if err := file.Truncate(offset); err != nil {
					return nil, fmt.Errorf("truncate incomplete wal tail: %w", err)
				}

				if err := file.Sync(); err != nil {
					return nil, fmt.Errorf("sync truncated wal segment: %w", err)
				}

				break
			}

			// A checksum error or invalid record is corruption, not a normal
			// end-of-file condition. Do not silently return potentially wrong
			// data.
			return nil, fmt.Errorf(
				"decode wal record at byte offset %d: %w",
				offset,
				err,
			)
		}

		records = append(records, record)
		offset += int64(recordLen)
	}

	return records, nil
}

// readWALRecord reads exactly one record encoded by WALRecord.Encode.
//
// Your record format is:
//
//	op        : 1 byte
//	seqNo     : 8 bytes
//	keyLen    : 4 bytes
//	valueLen  : 4 bytes
//	checksum  : 4 bytes
//	key       : keyLen bytes
//	value     : valueLen bytes
//
// The fixed first part is walHeaderSize (21 bytes) from wal_record.go.
func readWALRecord(file *os.File) (WALRecord, int, error) {
	header := make([]byte, walHeaderSize)

	n, err := io.ReadFull(file, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return WALRecord{}, 0, io.EOF
		}

		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return WALRecord{}, 0, io.ErrUnexpectedEOF
		}

		return WALRecord{}, 0, err
	}

	keyLen := binary.LittleEndian.Uint32(
		header[walKeyLenOffset:walValueLenOffset],
	)
	valueLen := binary.LittleEndian.Uint32(
		header[walValueLenOffset:walCRCOffset],
	)

	// Protect recovery from corrupt length fields causing enormous allocation.
	const maxWALRecordBytes = 64 * 1024 * 1024

	payloadLen := uint64(keyLen) + uint64(valueLen)
	if payloadLen > maxWALRecordBytes {
		return WALRecord{}, 0, fmt.Errorf(
			"%w: wal record payload too large: %d bytes",
			ErrCorruptionDetected,
			payloadLen,
		)
	}

	recordLen := walHeaderSize + int(payloadLen)
	recordBytes := make([]byte, recordLen)
	copy(recordBytes, header)

	if payloadLen > 0 {
		if _, err := io.ReadFull(file, recordBytes[walHeaderSize:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return WALRecord{}, 0, io.ErrUnexpectedEOF
			}
			return WALRecord{}, 0, err
		}
	}

	record, err := DecodeWALRecord(recordBytes)
	if err != nil {
		return WALRecord{}, 0, err
	}

	return record, recordLen, nil
}
