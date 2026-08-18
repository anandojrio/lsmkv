package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	walDirectoryName = "wal"

	// Segment header:
	// magic (4 bytes) + version (1 byte) + reserved (3 bytes).
	walSegmentMagic      uint32 = 0x4C534D57 // bytes: "WMSL" on disk in little-endian; identity only
	walSegmentVersion    byte   = 1
	walSegmentHeaderSize        = 8
)

type WAL struct {
	dir          string
	path         string
	file         *os.File
	fsyncEveryN  int
	rollBytes    int64
	appendCount  int
	bytesWritten int64
	activeID     int
	activeSize   int64
	lastSeqNo    uint64
}

func walDirectory(dataDir string) string {
	return filepath.Join(dataDir, walDirectoryName)
}

func walSegmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", id))
}

func OpenWAL(cfg Config) (*WAL, error) {
	dir := walDirectory(cfg.DataDir)
	if cfg.WALFsyncEveryN < 0 { //dodato: ranije je OpenWAL mogao da prihvati besmislen WALSegmentRollBytes, i negativan WALFsyncEveryN, sada to odmah odbija sa jasnom greškom.
		return nil, fmt.Errorf("%w: wal fsync interval must be >= 0", ErrInvalidArgument)
	}

	if cfg.WALSegmentRollBytes <= walSegmentHeaderSize {
		return nil, fmt.Errorf(
			"%w: wal segment roll bytes must be > %d",
			ErrInvalidArgument,
			walSegmentHeaderSize,
		)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}

	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		dir:         dir,
		fsyncEveryN: cfg.WALFsyncEveryN,
		rollBytes:   int64(cfg.WALSegmentRollBytes),
	}

	if len(ids) == 0 {
		if err := wal.openNewSegment(1); err != nil {
			return nil, err
		}
		return wal, nil
	}

	if err := wal.openExistingSegment(ids[len(ids)-1]); err != nil {
		return nil, err
	}

	return wal, nil
}

func (w *WAL) openNewSegment(id int) error {
	path := walSegmentPath(w.dir, id)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("create wal segment %s: %w", path, err)
	}

	header := make([]byte, walSegmentHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], walSegmentMagic)
	header[4] = walSegmentVersion

	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return fmt.Errorf("write wal segment header: %w", err)
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync wal segment header: %w", err)
	}

	w.file = file
	w.path = path
	w.activeID = id
	w.activeSize = walSegmentHeaderSize
	w.bytesWritten = 0
	w.appendCount = 0

	return nil
}

func (w *WAL) openExistingSegment(id int) error {
	path := walSegmentPath(w.dir, id)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open wal segment %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat wal segment: %w", err)
	}

	if info.Size() < walSegmentHeaderSize {
		_ = file.Close()
		return fmt.Errorf("%w: wal segment %s is smaller than header", ErrCorruptionDetected, path)
	}

	header := make([]byte, walSegmentHeaderSize) //dodato:Zamenili smo minimalnu proveru “fajl je dovoljno velik” sa pravom proverom identiteta i verzije segmenta.(Sada startup odmah staje ako aktivni segment nije validan)
	if _, err := file.ReadAt(header, 0); err != nil {
		_ = file.Close()
		return fmt.Errorf("read wal segment header: %w", err)
	}

	if binary.LittleEndian.Uint32(header[0:4]) != walSegmentMagic {
		_ = file.Close()
		return fmt.Errorf("%w: bad wal segment magic in %s", ErrCorruptionDetected, path)
	}

	if header[4] != walSegmentVersion {
		_ = file.Close()
		return fmt.Errorf(
			"%w: unsupported wal segment version %d in %s",
			ErrCorruptionDetected,
			header[4],
			path,
		)
	}

	w.file = file
	w.path = path
	w.activeID = id
	w.activeSize = info.Size()
	w.bytesWritten = info.Size() - walSegmentHeaderSize
	w.appendCount = 0

	return nil
}

func (w *WAL) Append(record WALRecord) error {
	if w.file == nil {
		return ErrStoreClosed
	}

	encoded, err := record.Encode()
	if err != nil {
		return err
	}

	// Roll before appending if the current segment already has records and
	// adding this record would exceed the configured size threshold.
	if w.bytesWritten > 0 && w.activeSize+int64(len(encoded)) > w.rollBytes {
		if err := w.roll(); err != nil {
			return err
		}
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
	w.activeSize += int64(n)
	w.lastSeqNo = record.SeqNo

	if w.fsyncEveryN > 0 && w.appendCount%w.fsyncEveryN == 0 {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal: %w", err)
		}
	}

	return nil
}

func (w *WAL) roll() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal before roll: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal before roll: %w", err)
	}

	w.file = nil

	if err := w.openNewSegment(w.activeID + 1); err != nil {
		return err
	}

	return nil
}

// Reset is called only after a successful memtable-to-SSTable flush.
// At that point, all mutations represented in the WAL are already durable
// in the newly published SSTable and manifest, so all existing WAL segments
// can be removed safely.
func (w *WAL) Reset() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal before reset: %w", err)
	}
	w.file = nil

	ids, err := listWALSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		path := walSegmentPath(w.dir, id)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove wal segment %s: %w", path, err)
		}
	}

	if err := w.openNewSegment(1); err != nil {
		return err
	}

	w.lastSeqNo = 0
	return nil
}

func (w *WAL) Path() string {
	return w.path
}

func (w *WAL) Dir() string {
	return w.dir
}

func (w *WAL) BytesWritten() int64 {
	return w.bytesWritten
}

func (w *WAL) LastSeqNo() uint64 {
	return w.lastSeqNo
}

func (w *WAL) ActiveSegmentID() int {
	return w.activeID
}

func (w *WAL) TotalSegments() int {
	ids, err := listWALSegmentIDs(w.dir)
	if err != nil {
		return 0
	}
	return len(ids)
}

func (w *WAL) Close() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		w.file = nil
		return fmt.Errorf("sync wal on close: %w", err)
	}

	err := w.file.Close()
	w.file = nil

	if err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	return nil
}

func listWALSegmentIDs(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list wal directory: %w", err)
	}

	ids := make([]int, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}

		idText := strings.TrimSuffix(name, ".wal")
		id, err := strconv.Atoi(idText)
		if err != nil || id <= 0 {
			continue
		}

		ids = append(ids, id)
	}

	sort.Ints(ids)
	return ids, nil
}
