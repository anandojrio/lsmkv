package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// sstEntry is an in-memory key/value pair destined for an SSTable.
// tombstone=true means this is a deletion marker.
type sstEntry struct {
	key       []byte
	value     []byte
	seqNo     uint64
	tombstone bool
}

// sstIndexEntry records the first key of a data block and its byte offset
// in the file. The reader uses this to binary-search for a target key.
type sstIndexEntry struct {
	firstKey   []byte
	byteOffset uint64
}

// SSTableWriter collects entries, sorts them, and flushes them to disk
// as a complete SSTable file.
type SSTableWriter struct {
	cfg     Config
	entries []sstEntry
}

func NewSSTableWriter(cfg Config) *SSTableWriter {
	return &SSTableWriter{cfg: cfg}
}

// Add stages an entry for writing. Call this for every key in the memtable.
func (w *SSTableWriter) Add(key, value []byte, seqNo uint64, tombstone bool) {
	w.entries = append(w.entries, sstEntry{
		key:       append([]byte(nil), key...),
		value:     append([]byte(nil), value...),
		seqNo:     seqNo,
		tombstone: tombstone,
	})
}

// Flush sorts all staged entries, writes the SSTable to a temp file,
// then atomically renames it to its final path.
// It returns the final file path.
func (w *SSTableWriter) Flush(path string) error {
	// 1. Sort entries by key (lexicographic byte order).
	sort.Slice(w.entries, func(i, j int) bool {
		ki := string(w.entries[i].key)
		kj := string(w.entries[j].key)
		return ki < kj
	})

	// 2. Write to a temp file first. If we crash mid-write, the .tmp
	// file is incomplete and the final path never appears — atomicity.
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create sst tmp: %w", err)
	}

	// bufio.Writer wraps the file with an in-memory buffer so we don't
	// issue one syscall per field — we batch writes into 64KB chunks.
	bw := bufio.NewWriterSize(f, 65536)

	var (
		index        []sstIndexEntry
		offset       uint64
		blockStart   uint64
		blockEntries int
	)

	// 3. Write DATA BLOCKS: pack entries until blockSize bytes are reached.
	for i, entry := range w.entries {
		// Record the start of a new block in the index.
		if blockEntries == 0 {
			index = append(index, sstIndexEntry{
				firstKey:   append([]byte(nil), entry.key...),
				byteOffset: offset,
			})
			blockStart = offset
		}

		n, err := writeSSEntry(bw, entry)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("write sst entry %d: %w", i, err)
		}
		offset += uint64(n)
		blockEntries++

		// When the current block reaches blockSize, close it and start fresh.
		if int(offset-blockStart) >= w.cfg.BlockSize {
			blockEntries = 0
		}
	}

	// 4. Flush buffered data block bytes to the underlying file.
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush sst data: %w", err)
	}

	indexOffset := offset // remember where the index starts

	// 5. Write INDEX BLOCK.
	ibw := bufio.NewWriter(f)
	for _, ie := range index {
		n, err := writeSSIndexEntry(ibw, ie)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("write sst index: %w", err)
		}
		offset += uint64(n)
	}
	if err := ibw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush sst index: %w", err)
	}

	bloomOffset := offset // remember where the bloom filter starts

	// 6. Write BLOOM FILTER.
	bloom := buildBloomFilter(w.entries, w.cfg.BloomFalsePositive)
	bn, err := f.Write(bloom)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write sst bloom: %w", err)
	}
	offset += uint64(bn)

	// 7. Write FOOTER: 8 bytes indexOffset + 8 bytes bloomOffset = 16 bytes.
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomOffset)
	if _, err := f.Write(footer); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write sst footer: %w", err)
	}

	// 8. Sync to disk (ensure bytes leave the OS buffer cache).
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync sst: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close sst tmp: %w", err)
	}

	// 9. Atomic rename: the final path either has the complete file or nothing.
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename sst: %w", err)
	}

	return nil
}

// --- Entry encoding ---

// writeSSEntry encodes one key/value entry into the writer.
// Format per entry:
//
//	[1 byte flags][8 bytes seqNo][4 bytes keyLen][4 bytes valueLen][4 bytes crc]
//	[key bytes][value bytes]
//
// flags bit 0: tombstone (1=tombstone, 0=live)
func writeSSEntry(w *bufio.Writer, e sstEntry) (int, error) {
	keyLen := len(e.key)
	valueLen := len(e.value)
	headerSize := 1 + 8 + 4 + 4 + 4 // flags+seqNo+keyLen+valueLen+crc
	total := headerSize + keyLen + valueLen

	buf := make([]byte, total)

	var flags byte
	if e.tombstone {
		flags = 1
	}
	buf[0] = flags
	binary.LittleEndian.PutUint64(buf[1:9], e.seqNo)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(keyLen))
	binary.LittleEndian.PutUint32(buf[13:17], uint32(valueLen))
	// CRC at buf[17:21] — computed over everything except the CRC field itself
	copy(buf[headerSize:headerSize+keyLen], e.key)
	copy(buf[headerSize+keyLen:], e.value)

	checksum := crc32.Checksum(sstChecksumInput(buf), crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[17:21], checksum)

	n, err := w.Write(buf)
	return n, err
}

// sstChecksumInput returns the bytes to checksum: header (without CRC) + body.
func sstChecksumInput(buf []byte) []byte {
	out := make([]byte, 0, len(buf)-4)
	out = append(out, buf[:17]...) // before CRC
	out = append(out, buf[21:]...) // after CRC (key+value)
	return out
}

// writeSSIndexEntry encodes one index entry.
// Format: [4 bytes keyLen][key bytes][8 bytes byteOffset]
func writeSSIndexEntry(w *bufio.Writer, ie sstIndexEntry) (int, error) {
	keyLen := len(ie.firstKey)
	buf := make([]byte, 4+keyLen+8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keyLen))
	copy(buf[4:4+keyLen], ie.firstKey)
	binary.LittleEndian.PutUint64(buf[4+keyLen:], ie.byteOffset)
	n, err := w.Write(buf)
	return n, err
}
