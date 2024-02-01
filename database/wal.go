package database

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

type WalReadWriter struct {
	r   io.ReadWriter
	err error
}

func (wr *WalReadWriter) read(data any) {
	if wr.err == nil {
		wr.err = binary.Read(wr.r, binary.LittleEndian, data)
	}
}

func (wr *WalReadWriter) write(data any) {
	if wr.err == nil {
		wr.err = binary.Write(wr.r, binary.LittleEndian, data)
	}
}

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
	rw := WalReadWriter{r: w.fileHandle, err: nil}
	var keyLenBuf uint32
	var deletedBuf bool

	rw.read(&keyLenBuf)
	keyBuf := make([]byte, keyLenBuf)
	rw.read(&deletedBuf)

	if !deletedBuf {
		var valueLenBuf uint32
		rw.read(&valueLenBuf)
		valueBuf := make([]byte, valueLenBuf)

		rw.read(&keyBuf)
		rw.read(&valueBuf)

		return &MemoryTableEntry{
			key:     keyBuf,
			value:   valueBuf,
			deleted: deletedBuf,
		}, rw.err
	} else {
		rw.read(&keyBuf)

		return &MemoryTableEntry{
			key:     keyBuf,
			value:   nil,
			deleted: deletedBuf,
		}, rw.err
	}

}
func (w *WalFile) Close() error {
	return w.fileHandle.Close()
}

func (w *WalFile) Set(key, value []byte) error {
	rw := WalReadWriter{r: w.fileHandle, err: nil}

	rw.write(int32(len(key)))
	rw.write(false)
	rw.write(int32(len(value)))
	rw.write(key)
	rw.write(value)

	return rw.err
}

func (w *WalFile) Delete(key []byte) error {
	rw := WalReadWriter{r: w.fileHandle, err: nil}

	rw.write(int32(len(key)))
	rw.write(true)
	rw.write(key)

	return rw.err
}
