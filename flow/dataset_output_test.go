package flow

import (
	"reflect"
	"testing"
)

func TestCollectOutput(t *testing.T) {
	fCtx := New()
	dataset := fCtx.Source(func(output chan string) {
		output <- "a"
		output <- "b"
		output <- "c"
	}, 1)

	got := collectOutput(dataset)

	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v want %v", got, want)
	}
}
