package protocol

import (
	"bytes"
	"testing"
)

func TestParseBinaryString(t *testing.T) {
	input := Value{
		ValType: 1,
		Str:     "test",
		Array:   nil,
	}

	inputBytes, err := input.Marshal()
	if err != nil {
		t.Fatalf("unexpected error while marshalling input: %s\n", err)
	}

	buf := new(bytes.Buffer)
	buf.Write(inputBytes)

	r := NewProtoReader(buf)
	val, err := r.ParseInput()
	if err != nil {
		t.Fatalf("unexpected error parsing binary string: %s\n", err)
	}

	if val.ValType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.ValType)
	}

	if val.Str != "test" {
		t.Errorf("unexpected parse output: want %s got %s\n", input.Str, val.Str)
	}
}

func TestParseArrayOfBinaryString(t *testing.T) {
	input := Value{
		ValType: ARRAY,
		Str:     "",
		Array: []*Value{
			{ValType: BINARY_STRING, Str: "test1"},
			{ValType: BINARY_STRING, Str: "test2"},
			{ValType: BINARY_STRING, Str: "test3"},
		},
	}

	inputBytes, err := input.Marshal()
	if err != nil {
		t.Fatalf("unexpected error while marshalling input: %s\n", err)
	}
	buf := new(bytes.Buffer)
	buf.Write(inputBytes)

	r := NewProtoReader(buf)
	val, err := r.ParseInput()
	if err != nil {
		t.Fatalf("unexpected error while parsing array: %s\n", err)
	}

	if val.Array[0].Str != input.Array[0].Str {
		t.Errorf("unexpected parse output: want %s got %s", input.Array[0].Str, val.Array[0].Str)
	}
	if val.Array[0].ValType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.Array[0].ValType)
	}

	if val.Array[1].Str != input.Array[1].Str {
		t.Errorf("unexpected parse output: want %s got %s", input.Array[1].Str, val.Array[1].Str)
	}
	if val.Array[1].ValType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.Array[1].ValType)
	}

	if val.Array[2].Str != input.Array[2].Str {
		t.Errorf("unexpected parse output: want %s got %s", input.Array[2].Str, val.Array[2].Str)
	}
	if val.Array[2].ValType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.Array[2].ValType)
	}
}
func TestParseError(t *testing.T) {
	input := Value{
		ValType: ERROR,
		Str:     "test",
	}

	inputBytes, err := input.Marshal()
	if err != nil {
		t.Fatalf("unexpected error while marshalling input: %s\n", err)
	}
	buf := new(bytes.Buffer)
	buf.Write(inputBytes)

	r := NewProtoReader(buf)
	val, err := r.ParseInput()
	if err != nil {
		t.Fatalf("unexpected error while parsing array: %s\n", err)
	}

	if val.Str != input.Str {
		t.Errorf("unexpected parse output: want %s got %s", input.Str, val.Str)
	}
	if val.ValType != ERROR {
		t.Errorf("unexpected parse type value: want %d got %d\n", ERROR, val.ValType)
	}
}
