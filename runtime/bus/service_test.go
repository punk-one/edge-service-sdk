package bus

import (
	"context"
	"net"
	"testing"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
)

func TestEmbeddedServersUseDifferentRandomPorts(t *testing.T) {
	first, err := Start("test-one", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Start("test-two", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_, firstPort, _ := net.SplitHostPort(first.Address())
	_, secondPort, _ := net.SplitHostPort(second.Address())
	if firstPort == "" || secondPort == "" || firstPort == secondPort {
		t.Fatalf("random addresses = %q and %q", first.Address(), second.Address())
	}
}

func TestPublishAndConsumePreservesPayloadAndMetadata(t *testing.T) {
	service, err := Start("test-bus", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	received := make(chan busapi.Message, 1)
	if err := service.StartConsumer(ConsumerConfig{
		Durable:       "test-telemetry",
		FilterSubject: busapi.SubjectTelemetryReport,
	}, func(_ context.Context, message busapi.Message) error {
		received <- message
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"trace_id":"t-1","data":{"v":1}}`)
	if err := service.Publish(context.Background(), busapi.Message{
		Type:        busapi.TelemetryReport,
		Data:        payload,
		Origin:      busapi.OriginSDK,
		DataFormat:  "compact",
		TraceID:     "t-1",
		ProductCode: "p-1",
		DeviceCode:  "d-1",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if string(message.Data) != string(payload) || message.Origin != busapi.OriginSDK || message.DataFormat != "compact" {
			t.Fatalf("unexpected message: %+v payload=%s", message, message.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for JetStream message")
	}
}
