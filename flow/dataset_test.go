package flow

import (
	"reflect"
	"testing"
)

func TestNewDataset(t *testing.T) {
	flowContext := New()
	got := NewDataset(flowContext, reflect.TypeOf(1))
	if got.Id != 0 {
		t.Errorf("Got id: %v want: 0", got)
	}
}
