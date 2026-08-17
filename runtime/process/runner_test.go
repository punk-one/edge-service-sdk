package process

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	processapi "github.com/punk-one/edge-service-sdk/process"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"
)

func TestRunnerPublishesDeclaredOutputAndSkipsSelfMessage(t *testing.T) {
	configDir := t.TempDir()
	definition := `
name: echo
handler: echo
subscribe:
  - telemetry.report
publish:
  - telemetry.report
dataFormats:
  - compact
timeout: 2s
maxHop: 4
`
	if err := os.WriteFile(filepath.Join(configDir, "echo.yaml"), []byte(definition), 0o600); err != nil {
		t.Fatal(err)
	}
	busService, err := runtimebus.Start("process-test", appconfig.BusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer busService.Close()

	var calls atomic.Int32
	registry := processapi.NewRegistry()
	registry.MustRegister("echo", processapi.HandlerFunc(func(_ context.Context, message busapi.Message) ([]busapi.Message, error) {
		calls.Add(1)
		return []busapi.Message{{
			Type:       busapi.TelemetryReport,
			Data:       append([]byte(nil), message.Data...),
			DataFormat: message.DataFormat,
		}}, nil
	}))
	runner := NewRunner(appconfig.ProcessConfig{ConfigDir: configDir, Enabled: []string{"echo"}}, registry, busService, nil)
	started, err := runner.Start()
	if err != nil {
		t.Fatal(err)
	}
	if started != 1 {
		t.Fatalf("started = %d, want 1", started)
	}

	output := make(chan busapi.Message, 2)
	if err := busService.StartConsumer(runtimebus.ConsumerConfig{
		Durable:       "output-observer",
		FilterSubject: busapi.SubjectTelemetryReport,
	}, func(_ context.Context, message busapi.Message) error {
		if message.Origin == busapi.OriginProcess {
			output <- message
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := busService.Publish(context.Background(), busapi.Message{
		Type:        busapi.TelemetryReport,
		Data:        []byte(`{"d":{"v":1}}`),
		DataFormat:  "compact",
		Origin:      busapi.OriginSDK,
		TraceID:     "trace-1",
		ProductCode: "p-1",
		DeviceCode:  "d-1",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-output:
		if message.ProcessName != "echo" || message.Hop != 1 || message.TraceID != "trace-1" {
			t.Fatalf("unexpected output metadata: %+v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for process output")
	}
	time.Sleep(300 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1; process consumed its own output", got)
	}
}
