package eventruntime

import (
	"os"
	"path/filepath"
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	coreevent "github.com/punk-one/edge-service-sdk/event"
	appconfig "github.com/punk-one/edge-service-sdk/runtime/config"
)

func TestServicePersistsAndRestoresEventState(t *testing.T) {
	root := t.TempDir()
	eventDir := filepath.Join(root, "events")
	if err := os.MkdirAll(eventDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	profile := `name: service-events
type: EVENT
version: 1
config:
  initialization:
    emitInitialState: true
  categories:
    oee:
      stateModel: exclusive
      exclusiveGroup: oee
      allowMultipleActive: false
      transitionOrder: clear-then-raise
      priority: [running, unknown]
      events:
        - eventCode: RUN
          name: running
          eventType: rise-clear
          state: running
          when: "connection.online == true && data.mode == 'run'"
          recover: "connection.online == false || data.mode != 'run'"
        - eventCode: UNKNOWN
          name: unknown
          eventType: rise-clear
          state: unknown
          fallback: true
`
	if err := os.WriteFile(filepath.Join(eventDir, "service-events.yaml"), []byte(profile), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	device := contracts.DeviceConfig{
		Name:         "D1",
		InternalName: "D1",
		ProductCode:  "P1",
		EventProfile: "service-events",
		Telemetry:    contracts.TelemetryConfig{Groups: []contracts.TelemetryGroup{{Name: "state", Points: []contracts.PointConfig{{Name: "mode", ValueType: "String"}}}}},
	}
	config := appconfig.Config{
		Storage: appconfig.StorageConfig{SQLitePath: filepath.Join(root, "runtime.db")},
		Device:  appconfig.DeviceConfig{EventDir: eventDir},
		Devices: []contracts.DeviceConfig{device},
	}

	service, err := NewService(config, nil, nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if service == nil {
		t.Fatal("expected EVENT service")
	}
	service.Start()
	if err := service.ObserveConnection(coreevent.ConnectionObservation{DeviceCode: "D1", Online: true, State: "connected", ObservedAt: 1000, Known: true}); err != nil {
		t.Fatalf("ObserveConnection() error = %v", err)
	}
	if err := service.ObserveTelemetry("D1", 2000, []*contracts.CommandValue{{DeviceResourceName: "mode", Type: "String", Value: "run"}}); err != nil {
		t.Fatalf("ObserveTelemetry() error = %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	statePath := filepath.Join(root, "event-state.json")
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected persisted state file: %v", err)
	}
	state, err := NewFileStateStore(statePath).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if state.ConfigHashes["D1"] == "" || state.Devices["D1"].Values["mode"] != "run" {
		t.Fatalf("unexpected persisted state: %#v", state)
	}

	restored, err := NewService(config, nil, nil)
	if err != nil {
		t.Fatalf("NewService() restore error = %v", err)
	}
	if restored == nil {
		t.Fatal("expected restored EVENT service")
	}
	restoredState := restored.engine.ExportState()
	if !restoredState.Devices["D1"].Connection.Known || restoredState.Devices["D1"].Values["mode"] != "run" {
		t.Fatalf("event state was not restored: %#v", restoredState)
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("restored Close() error = %v", err)
	}
}
