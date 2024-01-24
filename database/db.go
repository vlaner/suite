package database

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
)

var ENTRY_SCHEMA = "%s:%s\n"
var SUITE_DB_MAGIC = []byte("SUITEDB")

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrInvalidMagic = errors.New("invalid file magic")
)

type Database struct {
	path       string
	fileHandle *os.File
	mu         sync.RWMutex
	data       map[string][]byte
}

func Open(path string) (*Database, error) {
	handle, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	db := &Database{
		path:       path,
		fileHandle: handle,
		mu:         sync.RWMutex{},
		data:       make(map[string][]byte),
	}

	file, err := handle.Stat()
	if err != nil {
		return db, err
	}

	if file.Size() == 0 {
		_, err := handle.Write(SUITE_DB_MAGIC)
		if err != nil {
			return db, err
		}
	} else {
		magicBuf := make([]byte, len(SUITE_DB_MAGIC))
		_, err = handle.Read(magicBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return db, err
			}
		}
		if !bytes.Equal(magicBuf, SUITE_DB_MAGIC) {
			return db, ErrInvalidMagic
		}
		d := gob.NewDecoder(handle)
		err := d.Decode(&db.data)
		if !errors.Is(err, io.EOF) {
			return db, err
		}
	}

	return db, nil
}

func (db *Database) Close() error {
	defer db.fileHandle.Close()
	db.fileHandle.Seek(int64(len(SUITE_DB_MAGIC)), 0)

	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(db.data)

	if err != nil {
		return err
	}
	_, err = db.fileHandle.Write(b.Bytes())
	return err
}

func (db *Database) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = value

	return nil
}

func (db *Database) Get(key string) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, exists := db.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

func (db *Database) Delete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	delete(db.data, key)
}
