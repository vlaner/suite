package database

import (
	"bytes"
	"os"
	"testing"
)

func TestWalCreatesFile(t *testing.T) {
	wal, err := NewWal("./testdata")
	defer os.RemoveAll("testdata")
	defer wal.Close()

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if wal == nil {
		t.Error("unexpected nil wal")
	}
}

func TestWalSets(t *testing.T) {
	key := []byte("testkey")
	value := []byte("testdata")
	defer os.RemoveAll("testdata")

	wal, err := NewWal("./testdata")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if wal == nil {
		t.Error("unexpected nil file")
	}

	err = wal.Set(key, value)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	wal.Close()

	wal, err = NewWal("./testdata")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if wal == nil {
		t.Error("unexpected nil file")
	}
	defer wal.Close()

	entry, err := wal.ReadNextEntry()
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(entry.key, key) {
		t.Errorf("keys are not same")
	}
	if !bytes.Equal(entry.value, value) {
		t.Error("values are not same")
	}
}

func TestWalDeletes(t *testing.T) {
	key := []byte("testkey")
	value := []byte("testdata")
	defer os.RemoveAll("testdata")

	wal, err := NewWal("./testdata")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if wal == nil {
		t.Error("unexpected nil file")
	}

	err = wal.Set(key, value)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	err = wal.Delete(key)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	wal.Close()

	wal, err = NewWal("./testdata")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if wal == nil {
		t.Error("unexpected nil file")
	}
	defer wal.Close()
	entry, err := wal.ReadNextEntry()
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(entry.key, key) {
		t.Errorf("keys are not same")
	}
	if !bytes.Equal(entry.value, value) {
		t.Error("values are not same")
	}

	entry, err = wal.ReadNextEntry()
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(entry.key, key) {
		t.Errorf("keys are not same, want %s got %s", key, entry.key)
	}
	if !entry.deleted {
		t.Errorf("entry should be deleted but it is not")
	}
}
