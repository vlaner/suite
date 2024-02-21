package protocol

import "io"

type Writer struct {
	writer io.Writer
}

func NewProtoWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w Writer) Write(v Value) error {
	b, err := v.Marshal()
	if err != nil {
		return err
	}

	_, err = w.writer.Write(b)
	if err != nil {
		return err
	}

	return nil
}
