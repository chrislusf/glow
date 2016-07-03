package util

import (
	"strings"
	"testing"
)

func TestCleanPath(t *testing.T) {
	if got := CleanPath("~/test"); strings.Contains(got, "~") {
		t.Errorf("got: %s, should not contain ~", got)
	}
}
