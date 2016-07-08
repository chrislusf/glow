package flow

import (
	"reflect"
	"testing"
)

func TestGuessFunctionOutputType(t *testing.T) {
	f := func(i int) int {
		return 1
	}
	if got, want := guessFunctionOutputType(f), reflect.TypeOf(1); got != want {
		t.Errorf("Type mismatch, got %v want %v", got, want)
	}
}

func TestGuessKey(t *testing.T) {
	testStruct := struct {
		A string
	}{
		A: "test",
	}
	cases := []struct {
		input reflect.Value
		want  reflect.Value
	}{{
		input: reflect.ValueOf(1),
		want:  reflect.ValueOf(1),
	}, {
		input: reflect.ValueOf("test"),
		want:  reflect.ValueOf("test"),
	}, {
		input: reflect.ValueOf([1]int{1}),
		want:  reflect.ValueOf(1),
	}, {
		input: reflect.ValueOf([]int{1}),
		want:  reflect.ValueOf(1),
	}, {
		input: reflect.ValueOf(testStruct),
		want:  reflect.ValueOf("test"),
	}}

	for _, c := range cases {
		if got := guessKey(c.input); got.Interface() != c.want.Interface() {
			t.Errorf("Got %v want %v", got, c.want)
		}
	}
}
