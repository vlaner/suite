package database

import (
	"bytes"
	"os"
	"testing"
)

func TestSetGet(t *testing.T) {
	dirPath := "testdata"
	defer os.RemoveAll("testdata")
	key := []byte("testkey")
	value := []byte("testvalue")

	db, err := Open(dirPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.Set(key, value)
	if err != nil {
		t.Error(err)
	}

	gotEntry, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(gotEntry.Key, key) {
		t.Fatalf("got wrong key, want %s, got %s", key, gotEntry.Key)
	}
	if !bytes.Equal(gotEntry.Value, value) {
		t.Fatalf("got wrong value, want %s, got %s", value, gotEntry.Value)
	}
}

func TestDelete(t *testing.T) {
	dirPath := "testdata"
	defer os.RemoveAll("testdata")
	key := []byte("testkey")
	value := []byte("testvalue")

	db, err := Open(dirPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.Set(key, value)
	if err != nil {
		t.Error(err)
	}

	gotEntry, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(gotEntry.Key, key) {
		t.Fatalf("got wrong key, want %s, got %s", key, gotEntry.Key)
	}
	if !bytes.Equal(gotEntry.Value, value) {
		t.Fatalf("got wrong value, want %s, got %s", value, gotEntry.Value)
	}

	err = db.Delete(key)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	_, err = db.Get(key)
	if err == nil {
		t.Fatalf("expected entry to be deleted but it is not")
	}
}
