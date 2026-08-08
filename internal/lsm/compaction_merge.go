package lsm

import "sort"

// mergeEntries combines entries from multiple SSTables into one sorted set.
//
// For duplicate keys, the entry with the highest sequence number wins.
// Tombstones are preserved because they must hide older values until a later,
// safe tombstone-GC policy decides they can be removed.
//
// The returned entries are sorted by ascending key, which is the order required
// by SSTableWriter and expected by SSTableReader.AllEntries.
func mergeEntries(tables ...[]sstEntry) []sstEntry {
	newestByKey := make(map[string]sstEntry)

	for _, entries := range tables {
		for _, entry := range entries {
			key := string(entry.key)

			current, exists := newestByKey[key]
			if !exists || entry.seqNo > current.seqNo {
				newestByKey[key] = sstEntry{
					key:       append([]byte(nil), entry.key...),
					value:     append([]byte(nil), entry.value...),
					seqNo:     entry.seqNo,
					tombstone: entry.tombstone,
				}
			}
		}
	}

	merged := make([]sstEntry, 0, len(newestByKey))
	for _, entry := range newestByKey {
		merged = append(merged, entry)
	}

	sort.Slice(merged, func(i, j int) bool {
		return string(merged[i].key) < string(merged[j].key)
	})

	return merged
}
