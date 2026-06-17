package pqueue_test

import (
	"os/exec"
	"testing"
)

func TestGoCompatibilityModules(t *testing.T) {
	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = "crates/pqueue-kafka/tests/compat/producer_oracle"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("producer oracle go tests failed: %v\n%s", err, out)
	}
}
