package util

import (
	"reflect"
	"testing"
)

func TestBytesToUint64(t *testing.T) {
	cases := []struct {
		bytes   []byte
		int_val uint64
		desc    string
	}{{
		bytes:   []byte{0x00, 0x00},
		int_val: 0,
		desc:    "all zero",
	}, {
		bytes:   []byte{0x01, 0x00},
		int_val: 256,
		desc:    "one in first byte",
	}}
	for _, c := range cases {
		got_bytes := make([]byte, 8, 8)
		Uint64toBytes(got_bytes, c.int_val)
		got_int64 := BytesToUint64(c.bytes)
		if got_int64 != c.int_val || !reflect.DeepEqual(got_bytes[6:], c.bytes) {
			t.Errorf("Failed: %v, got %v %v", c, got_bytes, got_int64)
		}

		got_bytes = make([]byte, 4, 4)
		Uint32toBytes(got_bytes, uint32(c.int_val))
		got_int32 := BytesToUint32(c.bytes)
		if uint64(got_int32) != c.int_val || !reflect.DeepEqual(got_bytes[2:], c.bytes) {
			t.Errorf("Failed: %v, got %v %v", c, got_bytes, got_int32)
		}

		got_bytes = make([]byte, 2, 2)
		Uint16toBytes(got_bytes, uint16(c.int_val))
		got_int16 := BytesToUint16(c.bytes)
		if got_int16 != uint16(c.int_val) || !reflect.DeepEqual(got_bytes, c.bytes) {
			t.Errorf("Failed: %v, got %v %v", c, got_bytes, got_int16)
		}
	}
}
