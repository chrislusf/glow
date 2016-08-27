package flow

import (
	"reflect"
	"testing"
)

func TestRunFlowInStandAloneMode(t *testing.T) {
	fCtx := New()
	got := make([]int, 0, 3)
	fCtx.Slice([]int{1, 2, 3}).Map(func(v int) {
		got = append(got, v)
	})
	fCtx.runFlowContextInStandAloneMode()
	if want := []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v want %v", got, want)
	}
}
