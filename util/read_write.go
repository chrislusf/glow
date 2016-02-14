package util

import (
	"io"
	"log"
)

// on wire io reader writer actual transmitted: size(4), bytes(n), flag(1),
// actual typed queue or channel:  bytes(n), flag(1)

type Message struct {
	flag ControlFlag
	data []byte
}

func NewMessage(flag ControlFlag, data []byte) *Message {
	m := &Message{flag: flag, data: data}
	return m
}

// LoadMessage load from typed channels or queues
func LoadMessage(rawdata []byte) *Message {
	m := &Message{
		flag: ControlFlag(rawdata[len(rawdata)-1]),
		data: rawdata[0 : len(rawdata)-1],
	}
	return m
}

func (m *Message) Flag() ControlFlag {
	return m.flag
}
func (m *Message) Data() []byte {
	return m.data
}
func (m *Message) Bytes() []byte {
	return append(m.data, byte(m.flag))
}

// data has actual data plus one byte of flag
func ReadBytes(r io.Reader, lenBuf []byte) (flag ControlFlag, m *Message, err error) {
	_, err = io.ReadAtLeast(r, lenBuf, 4)
	if err == io.EOF {
		flag = CloseChannel
		return flag, NewMessage(CloseChannel, nil), err
	}
	size := BytesToUint32(lenBuf)
	data := make([]byte, int(size))
	_, err = io.ReadAtLeast(r, data, int(size))
	if err != nil || size == 0 {
		return CloseChannel, NewMessage(CloseChannel, nil), err
	}
	message := LoadMessage(data)
	// println("read size:", size, string(message.Data()), ".")
	return message.Flag(), message, nil
}

// data has actual data plus one byte of flag
func WriteBytes(w io.Writer, lenBuf []byte, m *Message) {
	rawData := m.Bytes()
	size := len([]byte(rawData))
	// println("write size:", size, string(rawData), ".")
	Uint32toBytes(lenBuf, uint32(size))
	w.Write(lenBuf)
	w.Write(rawData)
}

func WriteData(w io.Writer, lenBuf []byte, dataList ...[]byte) error {
	size := 1 // start with 1 flag byte
	for _, d := range dataList {
		size += len(d)
	}
	Uint32toBytes(lenBuf, uint32(size))
	w.Write(lenBuf)
	for _, d := range dataList {
		w.Write(d)
	}
	lenBuf[0] = byte(Data)
	w.Write(lenBuf[0:1])

	// FIXME: get and return proper error here
	return nil
}

func WriteUint64(w io.Writer, number uint64) {
	buf := make([]byte, 8)
	Uint64toBytes(buf, number)
	w.Write(buf)
}

func ReadUint64(r io.Reader) uint64 {
	buf := make([]byte, 8)
	_, err := io.ReadAtLeast(r, buf, 8)
	if err == io.EOF {
		log.Printf("Failed to read uint64: %v", err)
		return 0
	}
	return BytesToUint64(buf)
}
