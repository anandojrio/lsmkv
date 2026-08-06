package lsm

type memtableEntry struct {
	value     []byte
	seqNo     uint64
	tombstone bool
}

type Memtable struct {
	entries map[string]memtableEntry
	bytes   int64
}

func newMemtable() *Memtable {
	return &Memtable{
		entries: make(map[string]memtableEntry),
	}
}

func (m *Memtable) Put(key, value []byte, seqNo uint64) {
	m.applyEntry(key, memtableEntry{
		value: append([]byte(nil), value...),
		seqNo: seqNo,
	})
}

func (m *Memtable) Delete(key []byte, seqNo uint64) {
	m.applyEntry(key, memtableEntry{
		seqNo:     seqNo,
		tombstone: true,
	})
}

func (m *Memtable) applyEntry(key []byte, entry memtableEntry) {
	k := string(key)

	if old, ok := m.entries[k]; ok {
		m.bytes -= int64(len(k) + len(old.value))
	}

	m.entries[k] = entry
	m.bytes += int64(len(k) + len(entry.value))
}

func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	entry, ok := m.entries[string(key)]
	if !ok {
		return nil, false, false
	}

	return entry.value, entry.tombstone, true
}

func (m *Memtable) Len() int {
	return len(m.entries)
}

func (m *Memtable) Bytes() int64 {
	return m.bytes
}
