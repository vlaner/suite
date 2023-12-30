package database

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

var ENTRY_SCHEMA = "%s:%s\n"
var SUITE_DB_MAGIC = []byte("SUITEDB\n")

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrInvalidMagic = errors.New("invalid file magic")
)

type Database struct {
	path       string
	fileHandle *os.File
	mu         sync.RWMutex
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
	}

	return db, nil
}

func (db *Database) Close() error {
	return db.fileHandle.Close()
}

func (db *Database) Set(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	v := fmt.Sprintf(ENTRY_SCHEMA, key, value)
	_, err := db.fileHandle.Write([]byte(v))

	return err
}

func (db *Database) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.fileHandle.Seek(int64(len(SUITE_DB_MAGIC)), 0)
	r := bufio.NewReader(db.fileHandle)
	for {
		line, _, err := r.ReadLine()
		log.Println(line)
		if err != nil {
			break
		}
		if bytes.Equal(line[:len(key)], key) {
			return line[len(key)+1:], nil
		}
	}

	return nil, ErrKeyNotFound
}
