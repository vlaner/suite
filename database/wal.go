package database

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

type WalFile struct {
	path       string
	fileHandle *os.File
}

func NewWal(dirPath string) (*WalFile, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.Mkdir(dirPath, os.ModeDir)
		if err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dirPath, "suite.db.wal")
	handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	return &WalFile{
		path:       path,
		fileHandle: handle,
	}, nil
}

func (w *WalFile) ReadNextEntry() (*MemoryTableEntry, error) {
	var keyLenBuf uint32
	var deletedBuf bool

	if err := binary.Read(w.fileHandle, binary.LittleEndian, &keyLenBuf); err != nil {
		return nil, err
	}
	keyBuf := make([]byte, keyLenBuf)

	if err := binary.Read(w.fileHandle, binary.LittleEndian, &deletedBuf); err != nil {
		return nil, err
	}

	if !deletedBuf {
		var valueLenBuf uint32
		if err := binary.Read(w.fileHandle, binary.LittleEndian, &valueLenBuf); err != nil {
			return nil, err
		}
		valueBuf := make([]byte, valueLenBuf)

		if err := binary.Read(w.fileHandle, binary.LittleEndian, &keyBuf); err != nil {
			return nil, err
		}
		if err := binary.Read(w.fileHandle, binary.LittleEndian, &valueBuf); err != nil {
			return nil, err
		}

		return &MemoryTableEntry{
			key:     keyBuf,
			value:   valueBuf,
			deleted: deletedBuf,
		}, nil
	} else {
		if err := binary.Read(w.fileHandle, binary.LittleEndian, &keyBuf); err != nil {
			return nil, err
		}

		return &MemoryTableEntry{
			key:     keyBuf,
			value:   nil,
			deleted: deletedBuf,
		}, nil
	}

}
func (w *WalFile) Close() error {
	return w.fileHandle.Close()
}

func (w *WalFile) Set(key, value []byte) {
	binary.Write(w.fileHandle, binary.LittleEndian, int32(len(key)))
	binary.Write(w.fileHandle, binary.LittleEndian, false)
	binary.Write(w.fileHandle, binary.LittleEndian, int32(len(value)))
	binary.Write(w.fileHandle, binary.LittleEndian, key)
	binary.Write(w.fileHandle, binary.LittleEndian, value)
}

func (w *WalFile) Delete(key []byte) {
	binary.Write(w.fileHandle, binary.LittleEndian, int32(len(key)))
	binary.Write(w.fileHandle, binary.LittleEndian, true)
	binary.Write(w.fileHandle, binary.LittleEndian, key)
}
