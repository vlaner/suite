package protocol

import (
	"bufio"
	"encoding/binary"
	"io"
)

type ProtoReader struct {
	reader *bufio.Reader
}

func NewProtoReader(r io.Reader) *ProtoReader {
	rd := bufio.NewReader(r)
	return &ProtoReader{
		reader: rd,
	}
}

func (r *ProtoReader) ParseInput() (*Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch typ {
	case BINARY_STRING:
		return r.parseBinaryString()
	case ARRAY:
		return r.parseArray()
	case ERROR:
		return r.parseError()
	}
	return nil, ErrUnknownValueType
}

func (r *ProtoReader) parseBinaryString() (*Value, error) {
	var v Value
	v.valType = BINARY_STRING

	var length uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	strBuf := make([]byte, length)
	if err := binary.Read(r.reader, binary.LittleEndian, &strBuf); err != nil {
		return nil, err
	}

	v.str = string(strBuf)

	return &v, nil
}

func (r *ProtoReader) parseArray() (*Value, error) {
	var v Value
	v.valType = ARRAY
	var length uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	v.array = make([]*Value, length)
	for i := 0; i < int(length); i++ {
		val, err := r.ParseInput()
		if err != nil {
			return nil, err
		}
		v.array[i] = val
	}

	return &v, nil
}

func (r *ProtoReader) parseError() (*Value, error) {
	var v Value
	v.valType = ERROR

	var length uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	strBuf := make([]byte, length)
	if err := binary.Read(r.reader, binary.LittleEndian, &strBuf); err != nil {
		return nil, err
	}

	v.str = string(strBuf)

	return &v, nil
}
