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
	Key     []byte
	Value   []byte
	Deleted bool
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
			err = memTable.Delete(entry.key)
			if err != nil {
				break
			}
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
	if err := db.WAL.Set(key, value); err != nil {
		return err
	}

	db.MemTable.Set(key, value)
	return nil
}

func (db *Database) Get(key []byte) (*DatabaseEntry, error) {
	entry := db.MemTable.Get(key)
	if entry == nil {
		return nil, ErrKeyNotFound
	}
	if entry.deleted {
		return nil, ErrKeyNotFound
	}

	return &DatabaseEntry{
		Key:     key,
		Value:   entry.value,
		Deleted: entry.deleted,
	}, nil

}

func (db *Database) Delete(key []byte) error {
	if err := db.WAL.Delete(key); err != nil {
		return err
	}
	if err := db.MemTable.Delete(key); err != nil {
		return err
	}
	return nil
}
