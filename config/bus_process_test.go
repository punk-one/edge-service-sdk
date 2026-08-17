package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOldConfigLeavesBusAndProcessesDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("service:\n  port: 19994\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadMainConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Bus.Enabled || len(config.Process.Enabled) != 0 {
		t.Fatalf("old config unexpectedly enabled optional runtime: %+v %+v", config.Bus, config.Process)
	}
}

func TestBusAndProcessConfigAreOptionalExtensions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
bus:
  enabled: true
  maxAge: 24h
process:
  enabled:
    - alarm
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadMainConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !config.Bus.Enabled || config.Bus.MaxAge != "24h" {
		t.Fatalf("bus config = %+v", config.Bus)
	}
	if len(config.Process.Enabled) != 1 || config.Process.Enabled[0] != "alarm" {
		t.Fatalf("process config = %+v", config.Process)
	}
}
