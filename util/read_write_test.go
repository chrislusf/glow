package util

import (
	"bytes"
	"reflect"
	"testing"
)

func TestReadBytes(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x05}
	data = append(data, []byte("testa")...)
	buffer := bytes.NewBuffer(data)
	lenBuf := make([]byte, 4)
	if flag, msg, err := ReadBytes(buffer, lenBuf); flag != 'a' || string(msg.data) != "test" || err != nil {
		t.Errorf("Failed, flag: %v, message: %v, error: %v", flag, msg, err)
	}
}

func TestWriteBytes(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0, 10))
	msg := Message{
		flag: 'a',
		data: []byte("d"),
	}
	lenBuf := make([]byte, 4)
	WriteBytes(buffer, lenBuf, &msg)
	want := []byte{0x00, 0x00, 0x00, 0x02, 'd', 'a'}
	if got := buffer.Bytes(); !reflect.DeepEqual(got, want) {
		t.Error(got)
	}
}

func TestWriteData(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0, 10))
	lenBuf := make([]byte, 4)
	WriteData(buffer, lenBuf, []byte("a"), []byte("b"), []byte("c"))
	want := []byte{0x00, 0x00, 0x00, 0x04, 'a', 'b', 'c', byte(Data)}
	if got := buffer.Bytes(); !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v want %v", got, want)
	}
}
