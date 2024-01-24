package database_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/vlaner/suite/database"
)

func TestOpenClose(t *testing.T) {
	path := "test1.db"
	defer os.Remove(path)

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db.Close()
}

func TestMagicNumberWorks(t *testing.T) {
	path := "test2.db"
	defer os.Remove(path)

	handle, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	_, err = handle.Write(database.SUITE_DB_MAGIC)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	err = handle.Close()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db.Close()
}

func TestMagicNumberWrong(t *testing.T) {
	path := "test3.db"
	defer os.Remove(path)

	handle, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	_, err = handle.Write([]byte("databaseWrong"))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	err = handle.Close()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	db, err := database.Open(path)
	if err == nil {
		t.Errorf("expected error but got nil: %s", err)
	}

	defer db.Close()
}

func TestSetGet(t *testing.T) {
	path := "testsetget.db"
	defer os.Remove(path)

	key := "test"
	value := []byte("value")

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db.Close()

	err = db.Set(key, value)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	val, err := db.Get(key)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("got wrong value (%s), want (%s)", val, value)
	}
}

func TestSetGetMany(t *testing.T) {
	type Test struct {
		key   string
		value []byte
	}
	tests := []Test{
		{key: "test1", value: []byte("value1")},
		{key: "test2", value: []byte("value2")},
		{key: "test3", value: []byte("value3")},
		{key: "test4", value: []byte("value4")},
		{key: "test5", value: []byte("other value")},
	}
	path := "testsetgetmany.db"
	defer os.Remove(path)

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db.Close()
	for _, test := range tests {
		db.Set(test.key, test.value)
	}

	for _, test := range tests {
		val, err := db.Get(test.key)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !bytes.Equal(val, test.value) {
			t.Errorf("got wrong value (%s), want (%s)", val, test.value)
		}
	}
}

func TestDelete(t *testing.T) {
	path := "testdelete.db"
	defer os.Remove(path)

	key := "test"
	value := []byte("value")

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db.Close()

	err = db.Set(key, value)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	db.Delete(key)

	_, err = db.Get(key)
	if err == nil {
		t.Errorf("expected error: %s, got nil", database.ErrKeyNotFound)
	}
}

func TestPersistWork(t *testing.T) {
	path := "testpersist.db"
	defer os.Remove(path)

	key := "test"
	value := []byte("value")

	db, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	err = db.Set(key, value)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	db.Close()

	db2, err := database.Open(path)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	defer db2.Close()

	val, err := db2.Get(key)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if !bytes.Equal(val, value) {
		t.Errorf("got wrong value (%s), want (%s)", val, value)
	}
}
