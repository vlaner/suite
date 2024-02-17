package protocol

import (
	"bytes"
	"testing"
)

func TestParseBinaryString(t *testing.T) {
	input := Value{
		valType: 1,
		str:     "test",
		array:   nil,
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

	if val.valType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.valType)
	}

	if val.str != "test" {
		t.Errorf("unexpected parse output: want %s got %s\n", input.str, val.str)
	}
}

func TestParseArrayOfBinaryString(t *testing.T) {
	input := Value{
		valType: ARRAY,
		str:     "",
		array: []*Value{
			{valType: BINARY_STRING, str: "test1"},
			{valType: BINARY_STRING, str: "test2"},
			{valType: BINARY_STRING, str: "test3"},
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

	if val.array[0].str != input.array[0].str {
		t.Errorf("unexpected parse output: want %s got %s", input.array[0].str, val.array[0].str)
	}
	if val.array[0].valType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.array[0].valType)
	}

	if val.array[1].str != input.array[1].str {
		t.Errorf("unexpected parse output: want %s got %s", input.array[1].str, val.array[1].str)
	}
	if val.array[1].valType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.array[1].valType)
	}

	if val.array[2].str != input.array[2].str {
		t.Errorf("unexpected parse output: want %s got %s", input.array[2].str, val.array[2].str)
	}
	if val.array[2].valType != BINARY_STRING {
		t.Errorf("unexpected parse type value: want %d got %d\n", BINARY_STRING, val.array[2].valType)
	}
}
func TestParseError(t *testing.T) {
	input := Value{
		valType: ERROR,
		str:     "test",
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

	if val.str != input.str {
		t.Errorf("unexpected parse output: want %s got %s", input.str, val.str)
	}
	if val.valType != ERROR {
		t.Errorf("unexpected parse type value: want %d got %d\n", ERROR, val.valType)
	}
}