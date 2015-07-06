// message.go
package message

import ()

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
