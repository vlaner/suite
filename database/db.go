package database

import (
	"errors"
)

var ENTRY_SCHEMA = "%s:%s\n"
var SUITE_DB_MAGIC = []byte("SUITEDB")

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrInvalidMagic = errors.New("invalid file magic")
)

type DatabaseEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

type Database struct {
	dirPath  string
	WAL      *WalFile
	MemTable *MemoryTable
}

func Open(dirPath string) (*Database, error) {
	var err error
	wal, err := NewWal(dirPath)
	if err != nil {
		return nil, err
	}

	memTable := NewMemoryTable()
	for {
		entry, err := wal.ReadNextEntry()
		if err != nil {
			break
		}
		memTable.Set(entry.key, entry.value)
		if entry.deleted {
			memTable.Delete(entry.key)
		}
	}

	db := &Database{
		dirPath:  dirPath,
		WAL:      wal,
		MemTable: memTable,
	}

	return db, err
}

func (db *Database) Close() error {
	return db.WAL.Close()
}

func (db *Database) Set(key []byte, value []byte) error {
	db.WAL.Set(key, value)
	db.MemTable.Set(key, value)
	return nil
}

func (db *Database) Get(key []byte) (*DatabaseEntry, error) {
	entry := db.MemTable.Get(key)
	if entry != nil {
		return &DatabaseEntry{
			key:     key,
			value:   entry.value,
			deleted: entry.deleted,
		}, nil
	}

	return nil, ErrKeyNotFound
}

func (db *Database) Delete(key []byte) {
	db.WAL.Delete(key)
	db.MemTable.Delete(key)
}
