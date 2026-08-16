package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTableReader opens an SSTable file and provides point lookups.
// It is safe to keep open across multiple Get calls.
type SSTableReader struct {
	path        string
	index       []sstIndexEntry // in-memory index loaded once at open
	bloom       *bloomFilter    // in-memory bloom filter loaded once at open
	size        int64           // total file size in bytes
	indexOffset int64

	// Unit 8: wired by version.Get; nil means no metrics collection.
	metrics *Metrics
}

// OpenSSTableReader opens the file, reads the footer, loads the index and
// bloom filter into memory. Data blocks are NOT loaded — they stay on disk.
func OpenSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sst %s: %w", path, err)
	}
	defer f.Close()

	// 1. Determine file size.
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat sst: %w", err)
	}
	size := info.Size()
	if size < 16 {
		return nil, fmt.Errorf("sst file too small (%d bytes): %w", size, ErrCorruptionDetected)
	}

	// 2. Read FOOTER — always the last 16 bytes.
	footer := make([]byte, 16)
	if _, err := f.ReadAt(footer, size-16); err != nil {
		return nil, fmt.Errorf("read sst footer: %w", err)
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footer[8:16])

	// 3. Validate footer offsets.
	if int64(indexOffset) < 0 || int64(bloomOffset) < 0 {
		return nil, fmt.Errorf("negative offsets in sst footer: %w", ErrCorruptionDetected)
	}
	if int64(indexOffset) > size-16 || int64(bloomOffset) > size-16 {
		return nil, fmt.Errorf("footer offsets out of bounds: %w", ErrCorruptionDetected)
	}
	if indexOffset > bloomOffset {
		return nil, fmt.Errorf("invalid footer offsets order: %w", ErrCorruptionDetected)
	}

	// 4. Read BLOOM FILTER bytes (from bloomOffset to start of footer).
	bloomSize := (size - 16) - int64(bloomOffset)
	if bloomSize < 0 {
		return nil, fmt.Errorf("invalid bloom offset in sst: %w", ErrCorruptionDetected)
	}
	bloomBytes := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, fmt.Errorf("read sst bloom: %w", err)
	}
	bloom, err := unmarshalBloom(bloomBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal sst bloom: %w", err)
	}

	// 5. Read INDEX BLOCK bytes (from indexOffset to bloomOffset).
	indexSize := int64(bloomOffset) - int64(indexOffset)
	if indexSize < 0 {
		return nil, fmt.Errorf("invalid index offset in sst: %w", ErrCorruptionDetected)
	}
	indexBytes := make([]byte, indexSize)
	if _, err := f.ReadAt(indexBytes, int64(indexOffset)); err != nil {
		return nil, fmt.Errorf("read sst index: %w", err)
	}
	index, err := decodeIndexBlock(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("decode sst index: %w", err)
	}

	return &SSTableReader{
		path:        path,
		index:       index,
		bloom:       bloom,
		size:        size,
		indexOffset: int64(indexOffset),
	}, nil
}

// Get searches for key in the SSTable. Returns the entry if found.
// Returns ErrNotFound if the key is definitely absent (bloom or index miss).
// Returns ErrCorruptionDetected if a CRC check fails.
func (r *SSTableReader) Get(key []byte) (sstEntry, error) {
	// Stage 1: Bloom filter check — free exit for missing keys.
	// Unit 8: count every bloom probe, and count skips (definite misses).
	if r.metrics != nil {
		r.metrics.BloomChecksTotal.Add(1)
	}
	if !r.bloom.mayContain(key) {
		if r.metrics != nil {
			r.metrics.BloomSkipsTotal.Add(1)
		}
		return sstEntry{}, ErrNotFound
	}

	// Stage 2: Binary search the index to find the right data block.
	blockOffset, blockEnd := r.locateBlock(key)
	if blockOffset < 0 {
		return sstEntry{}, ErrNotFound
	}

	// Stage 3: Read that data block from disk.
	// Unit 8: count every data block read from disk.
	f, err := os.Open(r.path)
	if err != nil {
		return sstEntry{}, fmt.Errorf("open sst for read: %w", err)
	}
	defer f.Close()

	blockSize := blockEnd - blockOffset
	blockBytes := make([]byte, blockSize)
	if _, err := f.ReadAt(blockBytes, blockOffset); err != nil {
		return sstEntry{}, fmt.Errorf("read sst block: %w", err)
	}
	if r.metrics != nil {
		r.metrics.BlockReadsTotal.Add(1)
	}

	// Stage 4: Linear scan within the block for the key.
	return scanBlock(blockBytes, key)
}

// AllEntries returns every entry stored in this SSTable in ascending key order.
//
// Compaction uses this method to read complete SSTables before merging them.
// Tombstones are intentionally returned too: a tombstone is a real record that
// must participate in newest-write-wins merge logic.
func (r *SSTableReader) AllEntries() ([]sstEntry, error) {
	if len(r.index) == 0 {
		return nil, nil
	}

	f, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("open sstable for full scan: %w", err)
	}
	defer f.Close()

	var out []sstEntry

	for i := 0; i < len(r.index); i++ {
		blockStart := int64(r.index[i].byteOffset)

		var blockEnd int64
		if i+1 < len(r.index) {
			blockEnd = int64(r.index[i+1].byteOffset)
		} else {
			blockEnd = r.indexOffset
		}

		if blockStart < 0 || blockEnd < blockStart {
			return nil, fmt.Errorf("invalid sstable block range: %w", ErrCorruptionDetected)
		}

		blockSize := blockEnd - blockStart
		if blockSize == 0 {
			continue
		}

		block := make([]byte, blockSize)
		if _, err := f.ReadAt(block, blockStart); err != nil {
			return nil, fmt.Errorf("read sstable block: %w", err)
		}

		br := bufio.NewReader(newByteReader(block))
		for {
			entry, err := readSSEntry(br)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			out = append(out, entry)
		}
	}

	return out, nil
}

// Close releases resources held by the reader. It is currently a no-op
// because the reader opens and closes the underlying file per Get call
// rather than holding a long-lived descriptor. It exists so call sites
// (e.g. Version.Close) are forward-compatible if a future change
// introduces a persistent handle or memory-mapped file.
func (r *SSTableReader) Close() error {
	return nil
}

// locateBlock binary-searches the index for the last entry whose firstKey ≤ key.
// Returns the byte offset of the block start and the byte offset of its end.
// Returns -1, -1 if no block could contain the key.
func (r *SSTableReader) locateBlock(key []byte) (start int64, end int64) {
	if len(r.index) == 0 {
		return -1, -1
	}

	n := len(r.index)
	pos := sort.Search(n, func(i int) bool {
		return string(r.index[i].firstKey) > string(key)
	})

	if pos == 0 {
		return -1, -1
	}
	blockIdx := pos - 1

	blockStart := int64(r.index[blockIdx].byteOffset)
	var blockEnd int64
	if blockIdx+1 < n {
		blockEnd = int64(r.index[blockIdx+1].byteOffset)
	} else {
		blockEnd = r.indexOffset
	}

	return blockStart, blockEnd
}

// --- Block scanning ---

// scanBlock linearly reads all entries in a raw block byte slice,
// returning the entry whose key matches, or ErrNotFound.
func scanBlock(block []byte, key []byte) (sstEntry, error) {
	r := bufio.NewReader(newByteReader(block))
	for {
		entry, err := readSSEntry(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return sstEntry{}, err
		}
		if string(entry.key) == string(key) {
			return entry, nil
		}
	}
	return sstEntry{}, ErrNotFound
}

// readSSEntry decodes one entry from a bufio.Reader.
// Must mirror the exact format written by writeSSEntry in sstable_writer.go.
func readSSEntry(r *bufio.Reader) (sstEntry, error) {
	header := make([]byte, 21)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return sstEntry{}, io.EOF
		}
		return sstEntry{}, fmt.Errorf("read sst entry header: %w", err)
	}

	flags := header[0]
	seqNo := binary.LittleEndian.Uint64(header[1:9])
	keyLen := binary.LittleEndian.Uint32(header[9:13])
	valueLen := binary.LittleEndian.Uint32(header[13:17])
	storedCRC := binary.LittleEndian.Uint32(header[17:21])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return sstEntry{}, fmt.Errorf("read sst entry key: %w", err)
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return sstEntry{}, fmt.Errorf("read sst entry value: %w", err)
	}

	checksumInput := make([]byte, 0, 17+len(key)+len(value))
	checksumInput = append(checksumInput, header[:17]...)
	checksumInput = append(checksumInput, key...)
	checksumInput = append(checksumInput, value...)

	computed := crc32.Checksum(checksumInput, crc32.MakeTable(crc32.Castagnoli))
	if computed != storedCRC {
		return sstEntry{}, fmt.Errorf("crc mismatch in sst entry: %w", ErrCorruptionDetected)
	}

	return sstEntry{
		key:       key,
		value:     value,
		seqNo:     seqNo,
		tombstone: flags&1 == 1,
	}, nil
}

// --- Index block decoding ---

// decodeIndexBlock parses the raw index block bytes into a slice of sstIndexEntry.
// Must mirror the format written by writeSSIndexEntry in sstable_writer.go.
func decodeIndexBlock(data []byte) ([]sstIndexEntry, error) {
	var entries []sstIndexEntry
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("index block truncated at keyLen: %w", ErrCorruptionDetected)
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen > len(data) {
			return nil, fmt.Errorf("index block truncated at key: %w", ErrCorruptionDetected)
		}
		key := append([]byte(nil), data[pos:pos+keyLen]...)
		pos += keyLen

		if pos+8 > len(data) {
			return nil, fmt.Errorf("index block truncated at offset: %w", ErrCorruptionDetected)
		}
		offset := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		entries = append(entries, sstIndexEntry{firstKey: key, byteOffset: offset})
	}
	return entries, nil
}

// --- byteReader helper ---

// byteReader wraps a []byte so it satisfies io.Reader.
// Used to feed scanBlock into bufio.NewReader without copying to a temp file.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

func (b *byteReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
