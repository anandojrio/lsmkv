package lsm

import "errors"

// Version is the immutable, in-memory snapshot of "which SSTables currently
// exist" that a reader consults. It is built once from a loaded Manifest
// plus the SSTableReader handles that were successfully opened for each
// table listed there. Readers held here are ordered newest-first, mirroring
// manifest order, so Get can stop at the first real hit.
type Version struct {
	Epoch uint64

	// SSTables holds one open reader per live table, newest table first.
	SSTables []*SSTableReader

	// Later:
	// Active     *Memtable
	// Immutables []*Memtable
}

// newVersionFromManifest builds a Version from a loaded manifest and the
// slice of readers already opened for its tables (same order as m.Tables).
func newVersionFromManifest(m *Manifest, readers []*SSTableReader) *Version {
	return &Version{
		Epoch:    m.Epoch,
		SSTables: readers,
	}

}

// Get searches every SSTable held by this Version, newest first, and
// returns the first entry found. A tombstone entry is returned as-is
// (the caller decides how to interpret it) so the search can stop as soon
// as *any* record for the key is found, matching "newest write wins".
//
// If no table contains the key, it returns ErrNotFound. If a table read
// fails for a reason other than "key absent" (e.g. corruption), that error
// is returned immediately rather than silently continuing to older tables.
func (v *Version) Get(key []byte) (sstEntry, error) {
	for _, r := range v.SSTables {
		entry, err := r.Get(key)
		if err == nil {
			return entry, nil
		}
		if errors.Is(err, ErrNotFound) {
			continue
		}
		return sstEntry{}, err
	}
	return sstEntry{}, ErrNotFound
}

// Close closes every SSTable reader held by this Version. Safe to call on
// a Version with zero tables.
func (v *Version) Close() error {
	for _, r := range v.SSTables {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

// withPublishedFlush returns a new Version with newReader prepended (newest
// first) and the epoch bumped to newEpoch. The original Version and its
// readers are left untouched — callers are responsible for closing the old
// Version's readers list minus the ones still referenced, if ever needed.
func (v *Version) withPublishedFlush(newReader *SSTableReader, newEpoch uint64) *Version {
	tables := make([]*SSTableReader, 0, len(v.SSTables)+1)
	tables = append(tables, newReader)
	tables = append(tables, v.SSTables...)

	return &Version{
		Epoch:    newEpoch,
		SSTables: tables,
	}
}
