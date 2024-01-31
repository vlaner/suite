package database

import (
	"bytes"
	"errors"
)

type MemoryTableEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

var entryDoesNotExist = errors.New("entry not found")

type MemoryTable struct {
	entries []MemoryTableEntry
}

func NewMemoryTable() *MemoryTable {
	return &MemoryTable{
		entries: make([]MemoryTableEntry, 0),
	}
}

func (mt *MemoryTable) search(key []byte) int {
	for i, entry := range mt.entries {
		if entry.deleted {
			continue
		}
		if bytes.Equal(entry.key, key) {
			return i
		}
	}

	return -1
}

func (mt *MemoryTable) Set(key, value []byte) {
	entry := MemoryTableEntry{
		key:     key,
		value:   value,
		deleted: false,
	}

	index := mt.search(key)
	if index == -1 {
		mt.entries = append(mt.entries, entry)
		return
	}
	mt.entries[index] = entry
}
func (mt *MemoryTable) Get(key []byte) *MemoryTableEntry {
	index := mt.search(key)
	if index == -1 {
		return nil
	}

	return &mt.entries[index]
}

func (mt *MemoryTable) Delete(key []byte) error {
	entry := MemoryTableEntry{
		key:     key,
		value:   nil,
		deleted: true,
	}

	index := mt.search(key)
	if index == -1 {
		return entryDoesNotExist
	}

	mt.entries[index] = entry
	return nil
}
