package flow

import (
	"reflect"
	"testing"
)

func TestSource(t *testing.T) {
	fc := New()
	intputFunction := func(in chan string) {
		in <- "test1"
		in <- "test2"
	}

	dataset := fc.Source(intputFunction, 1)

	outChan := make(chan string, 0)
	dataset.AddOutput(outChan)
	go dataset.Run()

	got := make([]string, 0, 2)
	for str := range outChan {
		got = append(got, str)
	}
	want := []string{"test1", "test2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Got %v want %v", got, want)
	}
}
