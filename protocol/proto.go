package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrUnknownValueType = errors.New("unknown value type")
)

var (
	ERROR         uint8 = 0
	BINARY_STRING uint8 = 1
	ARRAY         uint8 = 2
)

type BufferWriter struct {
	b   *bytes.Buffer
	err error
}

func (bw *BufferWriter) Write(data any) {
	if bw.err == nil {
		bw.err = binary.Write(bw.b, binary.LittleEndian, data)
	}
}

type Value struct {
	ValType uint8
	Str     string
	Array   []*Value
}

func (v *Value) Marshal() ([]byte, error) {
	switch v.ValType {
	case BINARY_STRING:
		return v.marshalString()
	case ARRAY:
		return v.marshalArray()
	case ERROR:
		return v.marshalError()
	}

	return nil, ErrUnknownValueType
}

func (v *Value) marshalString() ([]byte, error) {
	bw := BufferWriter{
		b:   new(bytes.Buffer),
		err: nil,
	}
	bw.Write(BINARY_STRING)
	bw.Write(uint32(len(v.Str)))
	bw.Write([]byte(v.Str))

	if bw.err != nil {
		return nil, bw.err
	}

	return bw.b.Bytes(), nil
}

func (v *Value) marshalArray() ([]byte, error) {
	bw := BufferWriter{
		b:   new(bytes.Buffer),
		err: nil,
	}
	bw.Write(ARRAY)
	bw.Write(uint32(len(v.Array)))

	for _, v := range v.Array {
		b, err := v.Marshal()
		if err != nil {
			return nil, err
		}

		bw.Write(b)
	}

	if bw.err != nil {
		return nil, bw.err
	}

	return bw.b.Bytes(), nil
}

func (v *Value) marshalError() ([]byte, error) {
	bw := BufferWriter{
		b:   new(bytes.Buffer),
		err: nil,
	}
	bw.Write(ERROR)
	bw.Write(uint32(len(v.Str)))
	bw.Write([]byte(v.Str))

	if bw.err != nil {
		return nil, bw.err
	}

	return bw.b.Bytes(), nil
}
