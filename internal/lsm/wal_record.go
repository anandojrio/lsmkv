package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	WALOpPut byte = 1
	WALOpDel byte = 2
)

const (
	walOpOffset       = 0
	walSeqNoOffset    = 1
	walKeyLenOffset   = 9
	walValueLenOffset = 13
	walCRCOffset      = 17
	walHeaderSize     = 21
)

var walCRC32Table = crc32.MakeTable(crc32.Castagnoli)

type WALRecord struct {
	Op    byte
	SeqNo uint64
	Key   []byte
	Value []byte
}

func (r WALRecord) Validate() error {
	if r.Op != WALOpPut && r.Op != WALOpDel {
		return fmt.Errorf("%w: invalid wal op %d", ErrInvalidArgument, r.Op)
	}

	if len(r.Key) == 0 {
		return fmt.Errorf("%w: wal key cannot be empty", ErrInvalidArgument)
	}

	if r.Op == WALOpDel && len(r.Value) != 0 {
		return fmt.Errorf("%w: delete record must not contain value bytes", ErrInvalidArgument)
	}

	return nil
}

func (r WALRecord) Encode() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}

	keyLen := len(r.Key)
	valueLen := len(r.Value)

	buf := make([]byte, walHeaderSize+keyLen+valueLen)

	buf[walOpOffset] = r.Op
	binary.LittleEndian.PutUint64(buf[walSeqNoOffset:walKeyLenOffset], r.SeqNo)
	binary.LittleEndian.PutUint32(buf[walKeyLenOffset:walValueLenOffset], uint32(keyLen))
	binary.LittleEndian.PutUint32(buf[walValueLenOffset:walCRCOffset], uint32(valueLen))

	copy(buf[walHeaderSize:walHeaderSize+keyLen], r.Key)
	copy(buf[walHeaderSize+keyLen:], r.Value)

	checksum := crc32.Checksum(walChecksumInput(buf), walCRC32Table)
	binary.LittleEndian.PutUint32(buf[walCRCOffset:walHeaderSize], checksum)

	return buf, nil
}

func DecodeWALRecord(data []byte) (WALRecord, error) {
	if len(data) < walHeaderSize {
		return WALRecord{}, io.ErrUnexpectedEOF
	}

	op := data[walOpOffset]
	seqNo := binary.LittleEndian.Uint64(data[walSeqNoOffset:walKeyLenOffset])
	keyLen := binary.LittleEndian.Uint32(data[walKeyLenOffset:walValueLenOffset])
	valueLen := binary.LittleEndian.Uint32(data[walValueLenOffset:walCRCOffset])
	wantChecksum := binary.LittleEndian.Uint32(data[walCRCOffset:walHeaderSize])

	totalLen := walHeaderSize + int(keyLen) + int(valueLen)
	if len(data) < totalLen {
		return WALRecord{}, io.ErrUnexpectedEOF
	}

	payload := data[:totalLen]

	gotChecksum := crc32.Checksum(walChecksumInput(payload), walCRC32Table)
	if gotChecksum != wantChecksum {
		return WALRecord{}, fmt.Errorf("%w: wal checksum mismatch", ErrCorruptionDetected)
	}

	keyStart := walHeaderSize
	keyEnd := keyStart + int(keyLen)
	valueStart := keyEnd
	valueEnd := valueStart + int(valueLen)

	record := WALRecord{
		Op:    op,
		SeqNo: seqNo,
		Key:   append([]byte(nil), payload[keyStart:keyEnd]...),
		Value: append([]byte(nil), payload[valueStart:valueEnd]...),
	}

	if err := record.Validate(); err != nil {
		return WALRecord{}, err
	}

	return record, nil
}

func walChecksumInput(record []byte) []byte {
	out := make([]byte, 0, len(record)-4)
	out = append(out, record[:walCRCOffset]...)
	out = append(out, record[walHeaderSize:]...)
	return out
}
