package ops

import "testing"

func TestHardRestartRequiresGracefulHook(t *testing.T) {
	restarter := NewRestarter("service", nil)
	if _, err := restarter.Restart(HardRestart); err == nil {
		t.Fatal("hard restart without lifecycle hook succeeded")
	}
}

func TestHardRestartUsesLifecycleHook(t *testing.T) {
	called := false
	restarter := NewRestarterWithHooks("service", nil, func() error {
		called = true
		return nil
	})
	result, err := restarter.Restart(HardRestart)
	if err != nil {
		t.Fatalf("Restart() error = %v", err)
	}
	if !called || result.Mode != string(HardRestart) {
		t.Fatalf("result = %#v called=%t", result, called)
	}
}
