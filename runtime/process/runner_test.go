package process

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	processapi "github.com/punk-one/edge-service-sdk/process"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"
)

func TestRunnerConsumesAllSubjectsForBoundDevicesAndSkipsOwnOutput(t *testing.T) {
	configDir := t.TempDir()
	definition := `
name: echo
timeout: 2s
maxHop: 4
business:
  arbitrary: value
`
	if err := os.WriteFile(filepath.Join(configDir, "echo.yaml"), []byte(definition), 0o600); err != nil {
		t.Fatal(err)
	}
	busService, err := runtimebus.Start("process-test", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer busService.Close()

	var mu sync.Mutex
	var received []busapi.MessageType
	registry := processapi.NewRegistry()
	registry.MustRegister("echo", processapi.HandlerFunc(func(_ context.Context, message busapi.Message) ([]busapi.Message, error) {
		mu.Lock()
		received = append(received, message.Type)
		mu.Unlock()
		if message.Type != busapi.TelemetryReport {
			return nil, nil
		}
		return []busapi.Message{{
			Type:       busapi.PropertySet,
			Data:       []byte(`{"trace_id":"trace-1","device_code":"d-1","data":{"mode":1}}`),
			DataFormat: "json",
		}}, nil
	}))
	devices := []contracts.DeviceConfig{{Name: "d-1", InternalName: "d-1", ProcessNames: []string{"echo"}}}
	runner := NewRunner(configDir, devices, registry, busService, nil)
	started, err := runner.Start()
	if err != nil {
		t.Fatal(err)
	}
	if started != 1 {
		t.Fatalf("started = %d, want 1", started)
	}

	output := make(chan busapi.Message, 1)
	if err := busService.StartConsumer(runtimebus.ConsumerConfig{
		Durable:       "output-observer",
		FilterSubject: busapi.SubjectPropertySet,
	}, func(_ context.Context, message busapi.Message) error {
		if message.Origin == busapi.OriginProcess {
			output <- message
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	publishProcessInput(t, busService, busapi.Message{
		Type:        busapi.TelemetryReport,
		Data:        []byte(`{"device_code":"d-1","data":{"v":1}}`),
		DataFormat:  "rule",
		Origin:      busapi.OriginSDK,
		TraceID:     "trace-1",
		ProductCode: "p-1",
		DeviceCode:  "d-1",
	})
	select {
	case message := <-output:
		if message.ProcessName != "echo" || message.Hop != 1 || message.DeviceCode != "d-1" {
			t.Fatalf("unexpected output metadata: %+v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for process output")
	}

	publishProcessInput(t, busService, busapi.Message{
		Type:       busapi.StatusReport,
		Data:       []byte(`{"device_code":"d-1"}`),
		DataFormat: "json",
		Origin:     busapi.OriginSDK,
		DeviceCode: "d-1",
	})
	publishProcessInput(t, busService, busapi.Message{
		Type:        busapi.EventReport,
		Data:        []byte(`{"device_code":"other"}`),
		DataFormat:  "json",
		Origin:      busapi.OriginSDK,
		DeviceCode:  "other",
		ProductCode: "p-1",
	})
	waitForCalls(t, func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(received)
	}, 2)
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != busapi.TelemetryReport || received[1] != busapi.StatusReport {
		t.Fatalf("received = %#v", received)
	}
}

func TestRunnerAcceptsOtherProcessMessagesAndHonorsMaxHop(t *testing.T) {
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "watch.yaml"), []byte("name: watch\nmaxHop: 4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	busService, err := runtimebus.Start("process-chain-test", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer busService.Close()

	calls := make(chan busapi.Message, 2)
	registry := processapi.NewRegistry()
	registry.MustRegister("watch", processapi.HandlerFunc(func(_ context.Context, message busapi.Message) ([]busapi.Message, error) {
		calls <- message
		return nil, nil
	}))
	devices := []contracts.DeviceConfig{{Name: "d-1", InternalName: "d-1", ProcessNames: []string{"watch"}}}
	if started, err := NewRunner(configDir, devices, registry, busService, nil).Start(); err != nil || started != 1 {
		t.Fatalf("Start() = %d, %v", started, err)
	}

	publishProcessInput(t, busService, busapi.Message{Type: busapi.PropertySet, Data: []byte(`{"device_code":"d-1"}`), Origin: busapi.OriginProcess, ProcessName: "other", DeviceCode: "d-1", Hop: 1})
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("other Process message was not delivered")
	}
	publishProcessInput(t, busService, busapi.Message{Type: busapi.PropertySet, Data: []byte(`{"device_code":"d-1"}`), Origin: busapi.OriginProcess, ProcessName: "other", DeviceCode: "d-1", Hop: 4})
	select {
	case message := <-calls:
		t.Fatalf("max-hop message was delivered: %+v", message)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestConfiguredProcessCountDeduplicatesDeviceBindings(t *testing.T) {
	devices := []contracts.DeviceConfig{
		{Name: "d-1", ProcessNames: []string{"a", "b"}},
		{Name: "d-2", ProcessNames: []string{"a"}},
	}
	if got := ConfiguredProcessCount(devices); got != 2 {
		t.Fatalf("ConfiguredProcessCount() = %d, want 2", got)
	}
}

func publishProcessInput(t *testing.T, busService *runtimebus.Service, message busapi.Message) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := busService.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
}

func waitForCalls(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("calls = %d, want at least %d", count(), want)
}
