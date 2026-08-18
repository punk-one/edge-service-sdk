package bus

import (
	"context"
	"testing"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

func TestMQTTObserverPreservesPayloadAndDirection(t *testing.T) {
	service, err := Start("observer-test", appconfig.NATSBusConfig{Enabled: true, StoreDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	received := make(chan busapi.Message, 1)
	if err := service.StartConsumer(ConsumerConfig{
		Durable:       "observer-consumer",
		FilterSubject: busapi.SubjectTelemetryReport,
	}, func(_ context.Context, message busapi.Message) error {
		received <- message
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	observer := NewMQTTObserver(service, nil)
	defer observer.Close()
	payload := []byte(`{"trace_id":"trace-1","device_code":"device-1"}`)
	observer.ObserveMQTT(mqtt.Observation{
		Direction:   mqtt.DirectionOutbound,
		Type:        busapi.TelemetryReport,
		Topic:       "v1/gateway/p/telemetry/report",
		Payload:     payload,
		DataFormat:  "compact",
		ProductCode: "p",
	})
	select {
	case message := <-received:
		if string(message.Data) != string(payload) || message.Origin != busapi.OriginSDK {
			t.Fatalf("unexpected mirror: %+v payload=%s", message, message.Data)
		}
		if message.TraceID != "trace-1" || message.DeviceCode != "device-1" {
			t.Fatalf("metadata was not extracted: %+v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for mirror")
	}
}
