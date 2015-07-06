// message.go
package message

import (
	"fmt"
	"io"

	"github.com/chrislusf/glow/go/util"
	"github.com/golang/protobuf/proto"
)

type MessageType uint8

const (
	ControlType MessageType = 1 + iota
	DataType
)

// if Type == ControlType, use ControlMessage
// if Type == DataType, use Data
type Message struct {
	Type           MessageType
	ControlMessage ControlMessage
	Data           []byte
}

func (m *Message) Write(w io.Writer) (err error) {
	buf := make([]byte, 5)
	buf[0] = byte(m.Type)
	var data []byte
	if m.Type == ControlType {
		data, err = proto.Marshal(&m.ControlMessage)
		if err != nil {
			return fmt.Errorf("Write error:%v", err)
		}
	} else {
		data = m.Data
	}
	util.Uint32toBytes(buf[1:], uint32(len(data)))
	w.Write(buf)
	w.Write(data)
	return nil
}

func Read(r io.Reader) (m *Message, err error) {
	buf := make([]byte, 5)
	count, err := r.Read(buf)
	if count == 0 || err != nil {
		return nil, err
	}
	mt := MessageType(buf[0])
	size := int(util.BytesToUint32(buf[1:]))
	data := make([]byte, size)
	count, err = r.Read(data)
	if err != nil {
		return nil, err
	}
	if count != size {
		return nil, fmt.Errorf("Wrong message size: expected %d, actual %d", size, count)
	}
	m = &Message{
		Type: mt,
	}
	if mt == ControlType {
		err = proto.Unmarshal(data, &m.ControlMessage)
		if err != nil {
			return nil, err
		}
	} else {
		m.Data = data
	}
	return nil, nil
}
