package lsm

import (
	"encoding/binary"
	"math"
)

// bloomFilter is a simple in-memory bit array Bloom filter.
type bloomFilter struct {
	bits []byte // the bit array, packed 8 bits per byte
	m    uint64 // total number of bits
	k    uint64 // number of hash functions
}

// newBloomFilter creates a filter sized for n expected elements
// with the given false-positive rate.
func newBloomFilter(n int, fpRate float64) *bloomFilter {
	// Optimal bit count formula: m = -n*ln(p) / (ln(2)^2)
	m := uint64(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))
	if m < 64 {
		m = 64
	}
	// Round up to next multiple of 8 so we pack cleanly into bytes.
	m = (m + 7) &^ 7

	// Optimal number of hash functions: k = (m/n) * ln(2)
	k := uint64(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}

	return &bloomFilter{
		bits: make([]byte, m/8),
		m:    m,
		k:    k,
	}
}

// add inserts a key into the filter.
func (bf *bloomFilter) add(key []byte) {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < bf.k; i++ {
		// Double hashing: combine h1 and h2 to get k independent positions.
		pos := (h1 + i*h2) % bf.m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// mayContain returns false if the key is definitely absent, true if maybe present.
func (bf *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// marshal serializes the filter to bytes for writing to disk.
// Format: [8 bytes m][8 bytes k][bit array bytes]
func (bf *bloomFilter) marshal() []byte {
	header := make([]byte, 16)
	binary.LittleEndian.PutUint64(header[0:8], bf.m)
	binary.LittleEndian.PutUint64(header[8:16], bf.k)
	out := append(header, bf.bits...)
	return out
}

// unmarshalBloom reconstructs a bloomFilter from its serialized bytes.
func unmarshalBloom(data []byte) (*bloomFilter, error) {
	if len(data) < 16 {
		return nil, ErrCorruptionDetected
	}
	m := binary.LittleEndian.Uint64(data[0:8])
	k := binary.LittleEndian.Uint64(data[8:16])
	bits := append([]byte(nil), data[16:]...)
	return &bloomFilter{bits: bits, m: m, k: k}, nil
}

// bloomHash returns two independent 64-bit hashes for double-hashing.
// Uses FNV-like mixing — simple and fast, no external dependency.
func bloomHash(key []byte) (uint64, uint64) {
	var h1, h2 uint64
	h1 = 14695981039346656037 // FNV offset basis
	h2 = 1099511628211        // FNV prime as seed for second hash

	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= 1099511628211
		h2 ^= uint64(b)
		h2 *= 1000000007
	}
	return h1, h2
}

// buildBloomFilter is a helper used by SSTableWriter: add all entry keys
// and return the serialized bytes ready to write to the SSTable file.
func buildBloomFilter(entries []sstEntry, fpRate float64) []byte {
	if len(entries) == 0 {
		return newBloomFilter(1, fpRate).marshal()
	}
	bf := newBloomFilter(len(entries), fpRate)
	for _, e := range entries {
		bf.add(e.key)
	}
	return bf.marshal()
}
