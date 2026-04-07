package app

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type statusTestLogger struct{}

func (l *statusTestLogger) Debugf(template string, args ...interface{}) {}
func (l *statusTestLogger) Infof(template string, args ...interface{})  {}
func (l *statusTestLogger) Warnf(template string, args ...interface{})  {}
func (l *statusTestLogger) Errorf(template string, args ...interface{}) {}
func (l *statusTestLogger) Error(args ...interface{})                   {}

type publishedStatusMessage struct {
	Device  contracts.DeviceConfig
	Message statusMessage
}

type fakeStatusPublisher struct {
	mu       sync.Mutex
	messages []publishedStatusMessage
}

func (p *fakeStatusPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	return nil
}

func (p *fakeStatusPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	return nil
}

func (p *fakeStatusPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return nil
}

func (p *fakeStatusPublisher) PublishPropertyPost(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}

func (p *fakeStatusPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var message statusMessage
	if err := json.Unmarshal(body, &message); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, publishedStatusMessage{Device: device, Message: message})
	return nil
}

func (p *fakeStatusPublisher) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	return nil
}

func (p *fakeStatusPublisher) HealthCheck() error {
	return nil
}

func (p *fakeStatusPublisher) Close() error {
	return nil
}

func (p *fakeStatusPublisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func (p *fakeStatusPublisher) Message(index int) publishedStatusMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.messages[index]
}

func TestInstallStatusPublisherUsesIncrementalAndHeartbeat(t *testing.T) {
	tracker := rtstatus.NewTracker()
	config := rtconfig.Config{
		Devices: []contracts.DeviceConfig{
			{Name: "qhl0001", ProductCode: "qhl"},
		},
	}
	sdk := NewDeviceSDK(config, logger.LoggingClient(&statusTestLogger{}), tracker)
	publisher := &fakeStatusPublisher{}

	installStatusPublisher(tracker, sdk, publisher, mqtt.TopicConfig{
		Topic:             "v1/gateway/{productCode}/status/post",
		HeartbeatInterval: "50ms",
	}, &statusTestLogger{})

	waitForStatusMessages(t, publisher, 1, 300*time.Millisecond)
	initial := publisher.Message(0).Message
	if initial.DeviceCode != "qhl0001" {
		t.Fatalf("unexpected initial device code: %#v", initial)
	}
	if initial.Data.Online {
		t.Fatalf("expected initial offline status: %#v", initial)
	}
	if initial.Data.ConnectionState != rtstatus.StateUnknown {
		t.Fatalf("unexpected initial state: %#v", initial)
	}
	if initial.Data.Error != nil {
		t.Fatalf("expected nil initial error: %#v", initial)
	}

	tracker.MarkConnected("qhl0001")
	waitForStatusMessages(t, publisher, 2, 300*time.Millisecond)
	connected := publisher.Message(1).Message
	if !connected.Data.Online || connected.Data.ConnectionState != rtstatus.StateConnected {
		t.Fatalf("unexpected connected payload: %#v", connected)
	}

	tracker.MarkReadSuccess("qhl0001")
	time.Sleep(20 * time.Millisecond)
	if publisher.Count() != 2 {
		t.Fatalf("lastSeenAt update should not trigger immediate publish, got %d messages", publisher.Count())
	}

	waitForStatusMessages(t, publisher, 3, 300*time.Millisecond)
	heartbeat := publisher.Message(2).Message
	if heartbeat.Data.ConnectionState != rtstatus.StateConnected || !heartbeat.Data.Online {
		t.Fatalf("unexpected heartbeat payload: %#v", heartbeat)
	}
	if heartbeat.Data.LastSeenAt == 0 {
		t.Fatalf("expected heartbeat to carry latest lastSeenAt: %#v", heartbeat)
	}

	tracker.MarkReadError("qhl0001", errors.New("read timeout"))
	waitForStatusMessages(t, publisher, 4, 300*time.Millisecond)
	failed := publisher.Message(3).Message
	if failed.Data.Online {
		t.Fatalf("expected degraded state to report offline: %#v", failed)
	}
	if failed.Data.ConnectionState != rtstatus.StateDegraded {
		t.Fatalf("unexpected degraded payload: %#v", failed)
	}
	if failed.Data.Error == nil || failed.Data.Error.Message != "read timeout" || failed.Data.Error.Time == 0 {
		t.Fatalf("unexpected error payload: %#v", failed)
	}
}

func waitForStatusMessages(t *testing.T, publisher *fakeStatusPublisher, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if publisher.Count() >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected at least %d status messages, got %d", count, publisher.Count())
}
