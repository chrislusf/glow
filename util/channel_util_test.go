package util

import (
	"reflect"
	"sort"
	"testing"
)

func doubleIt(v reflect.Value) reflect.Value {
	return reflect.ValueOf(v.Int() * 2)
}

func TestMergeChannel(t *testing.T) {
	chan1 := make(chan reflect.Value)
	chan2 := make(chan reflect.Value)

	go func() {
		chan1 <- reflect.ValueOf(1)
		close(chan1)
	}()

	go func() {
		chan2 <- reflect.ValueOf(2)
		close(chan2)
	}()

	outChan := make(chan reflect.Value, 2)
	MergeChannelTo([]chan reflect.Value{chan1, chan2}, doubleIt, outChan)

	got := make([]int, 0, 2)
	for v := range outChan {
		got = append(got, int(v.Int()))
	}
	sort.Ints(got)
	want := []int{2, 4}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v want %v", got, want)
	}
}
