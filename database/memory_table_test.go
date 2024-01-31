package database

import (
	"bytes"
	"testing"
)

func TestMemoryTableSets(t *testing.T) {
	mt := NewMemoryTable()

	mt.Set([]byte("testkey"), []byte("testvalue"))
	mt.Set([]byte("testkey1"), []byte("testvalue1"))
	mt.Set([]byte("testkey2"), []byte("testvalue2"))

	if !bytes.Equal(mt.entries[0].key, []byte("testkey")) {
		t.Errorf("keys are not same, want 'testkey', got '%s'", string(mt.entries[0].key))
	}
	if !bytes.Equal(mt.entries[1].key, []byte("testkey1")) {
		t.Errorf("keys are not same, want 'testkey1', got '%s'", string(mt.entries[1].key))
	}
	if !bytes.Equal(mt.entries[2].key, []byte("testkey2")) {
		t.Errorf("keys are not same, want 'testkey2', got '%s'", string(mt.entries[2].key))
	}
}

func TestMemoryTableOverwritesExistingKey(t *testing.T) {
	mt := NewMemoryTable()

	mt.Set([]byte("testkey"), []byte("testvalue"))
	mt.Set([]byte("testkey"), []byte("testvalue1"))

	if !bytes.Equal(mt.entries[0].value, []byte("testvalue1")) {
		t.Errorf("values are not same, want 'testvalue1', got '%s'", string(mt.entries[0].value))
	}
}

func TestMemoryTableGets(t *testing.T) {
	mt := NewMemoryTable()

	mt.Set([]byte("testkey"), []byte("testvalue"))
	entry := mt.Get([]byte("testkey"))
	if entry == nil {
		t.Fatal("unexpected nil entry")
	}
	if !bytes.Equal(entry.key, []byte("testkey")) {
		t.Errorf("keys are not same, want 'testkey', got '%s'", string(entry.key))
	}
	if !bytes.Equal(entry.value, []byte("testvalue")) {
		t.Errorf("values are not same, want 'testvalue', got '%s'", string(entry.value))
	}
}

func TestMemoryTableDeletes(t *testing.T) {
	mt := NewMemoryTable()

	mt.Set([]byte("testkey"), []byte("testvalue"))
	entry := mt.Get([]byte("testkey"))
	if entry == nil {
		t.Fatal("unexpected nil entry")
	}
	if !bytes.Equal(entry.key, []byte("testkey")) {
		t.Errorf("keys are not same, want 'testkey', got '%s'", string(entry.key))
	}
	if !bytes.Equal(entry.value, []byte("testvalue")) {
		t.Errorf("values are not same, want 'testvalue', got '%s'", string(entry.value))
	}
	err := mt.Delete([]byte("testkey"))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestMemoryTableCannotDeleteNonExistingValue(t *testing.T) {
	mt := NewMemoryTable()
	err := mt.Delete([]byte("testkey"))
	if err == nil {
		t.Fatalf("expected error but got nil")
	}
	if err != entryDoesNotExist {
		t.Errorf("wrong error: wanted %s but got %s", entryDoesNotExist, err)
	}
}
