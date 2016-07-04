package netchan

import (
	"reflect"
	"testing"
)

type SubStrangeType struct {
	A int
	B string
}

type StrangeType struct {
	X bool
	Y float32
	Z SubStrangeType
}

func TestStruct(t *testing.T) {
	x := StrangeType{
		X: true,
		Y: 100.0,
		Z: SubStrangeType{
			A: 250,
			B: "now what",
		},
	}

	in := map[string]StrangeType{"x": x}

	bytes, err := EncodeData(reflect.ValueOf(in))
	if err != nil {
		t.Fatalf("encoding error: %v", err)
	}

	value, err := DecodeData(bytes, reflect.TypeOf(in))

	if err != nil || !reflect.DeepEqual(value.Interface(), in) {
		t.Errorf("Got: %v want: %v error: %v", value, in, err)
	}
}

// Tests registration is needed to encode/decode objects with gob.
func TestNormal(t *testing.T) {
	x := StrangeType{
		X: true,
		Y: 100.0,
		Z: SubStrangeType{
			A: 250,
			B: "now what",
		},
	}

	in := map[string]interface{}{"foo": 1, "hello": "world", "x": x}

	// first try without registering
	bytes, err := EncodeData(reflect.ValueOf(in))
	if err == nil {
		t.Fatalf("there should be some error here!")
	}

	// now we registering
	Register(x)

	// second try with registering
	bytes, err = EncodeData(reflect.ValueOf(in))
	if err != nil {
		t.Fatalf("encoding error: %v", err)
	}

	value, err := DecodeData(bytes, reflect.TypeOf(in))

	if err != nil || !reflect.DeepEqual(value.Interface(), in) {
		t.Fatalf("decoding error: %v, got: %v want %v", err, value, in)
	}
}
