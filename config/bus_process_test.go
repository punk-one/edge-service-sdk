package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestOldConfigLeavesNATSBusDisabledAndDefaultsProcessDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("service:\n  port: 19994\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadMainConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.NATSBus.Enabled {
		t.Fatalf("old config unexpectedly enabled optional NATS bus: %+v", config.NATSBus)
	}
	if config.Device.ProcessDir != filepath.FromSlash("./configs/process") {
		t.Fatalf("processDir = %q", config.Device.ProcessDir)
	}
}

func TestLegacyBusNameIsNotRecognized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
bus:
  enabled: true
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadMainConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.NATSBus.Enabled {
		t.Fatalf("legacy bus key unexpectedly enabled natsBus: %+v", config.NATSBus)
	}
}

func TestNATSBusConfigUsesNewName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
natsBus:
  enabled: true
  maxAge: 24h
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadMainConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !config.NATSBus.Enabled || config.NATSBus.MaxAge != "24h" {
		t.Fatalf("natsBus config = %+v", config.NATSBus)
	}
}

func TestNormalizeDeviceConfigNormalizesProcessNames(t *testing.T) {
	device := NormalizeDeviceConfig(contracts.DeviceConfig{
		Name:         " device-1 ",
		ProcessNames: []string{" dbal ", "", "alarm", "dbal"},
	})
	if !reflect.DeepEqual(device.ProcessNames, []string{"dbal", "alarm"}) {
		t.Fatalf("processNames = %#v", device.ProcessNames)
	}
}
