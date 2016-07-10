package flow

import "testing"

func TestNew(t *testing.T) {
	if got, want := len(Contexts), 0; got != want {
		t.Errorf("len(Contexts), got %d, want %d", got, want)
	}
	flowContext := New()

	if got, want := flowContext.Id, 0; got != want {
		t.Errorf("flowContext.Id, got %d, want %d", got, want)
	}
	if got, want := len(Contexts), 1; got != want {
		t.Errorf("len(Contexts), got %d, want %d", got, want)
	}
}
