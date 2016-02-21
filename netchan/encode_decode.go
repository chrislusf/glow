package netchan

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

func Register(i interface{}) {
	gob.Register(i)
}

func DecodeData(data []byte, t reflect.Type) (reflect.Value, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	v := reflect.New(t)
	if err := dec.DecodeValue(v); err != nil {
		return v, fmt.Errorf("data type: %s decode error: %v", v.Kind(), err)
	} else {
		return reflect.Indirect(v), nil
	}
}

func EncodeData(t reflect.Value) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.EncodeValue(t); err != nil {
		return nil, fmt.Errorf("data type: %s kind: %s encode error: %v", t.Type().String(), t.Kind(), err)
	}
	return buf.Bytes(), nil
}
