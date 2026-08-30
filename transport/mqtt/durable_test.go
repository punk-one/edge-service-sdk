package mqtt

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

type durableTargetStub struct {
	mu                  sync.Mutex
	failPropertyReports bool
	propertyReports     int
}

func (s *durableTargetStub) PublishTelemetry(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableTargetStub) PublishCommandValues(contracts.DeviceConfig, []*contracts.CommandValue) error {
	return nil
}
func (s *durableTargetStub) PublishTelemetryEvent(outevent.TelemetryEvent, bool) error {
	return nil
}
func (s *durableTargetStub) PublishTelemetryEventAt(outevent.TelemetryEvent, bool, int64) error {
	return nil
}
func (s *durableTargetStub) PublishPropertyResult(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableTargetStub) PublishPropertyReport(contracts.DeviceConfig, map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.propertyReports++
	if s.failPropertyReports {
		return errors.New("broker unavailable")
	}
	return nil
}
func (s *durableTargetStub) PublishCommandResult(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableTargetStub) PublishStatus(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableTargetStub) PublishJSON(string, byte, bool, interface{}) error { return nil }
func (s *durableTargetStub) Subscribe(string, byte, MessageHandler) error      { return nil }
func (s *durableTargetStub) HealthCheck() error                                { return nil }
func (s *durableTargetStub) Close() error                                      { return nil }
func (s *durableTargetStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.propertyReports
}
func (s *durableTargetStub) setFailure(value bool) {
	s.mu.Lock()
	s.failPropertyReports = value
	s.mu.Unlock()
}

type durableMultiStub struct {
	names   []string
	targets []Publisher
}

func (s *durableMultiStub) GroupPublishers() []Publisher { return s.targets }
func (s *durableMultiStub) GroupName(i int) string       { return s.names[i] }
func (s *durableMultiStub) GroupStatusTopic(int) TopicConfig {
	return TopicConfig{}
}
func (s *durableMultiStub) PublishTelemetry(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableMultiStub) PublishCommandValues(contracts.DeviceConfig, []*contracts.CommandValue) error {
	return nil
}
func (s *durableMultiStub) PublishTelemetryEvent(outevent.TelemetryEvent, bool) error {
	return nil
}
func (s *durableMultiStub) PublishPropertyResult(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableMultiStub) PublishPropertyReport(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableMultiStub) PublishCommandResult(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableMultiStub) PublishStatus(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableMultiStub) PublishJSON(string, byte, bool, interface{}) error { return nil }
func (s *durableMultiStub) Subscribe(string, byte, MessageHandler) error      { return nil }
func (s *durableMultiStub) HealthCheck() error                                { return nil }
func (s *durableMultiStub) Close() error                                      { return nil }

func TestDurablePublisherAcknowledgesEachMQTTGroupIndependently(t *testing.T) {
	first := &durableTargetStub{}
	second := &durableTargetStub{failPropertyReports: true}
	base := &durableMultiStub{
		names:   []string{"primary", "mirror"},
		targets: []Publisher{first, second},
	}
	publisher, err := NewDurablePublisher(base, DurablePublisherConfig{
		SQLitePath:       filepath.Join(t.TempDir(), "outbox.db"),
		MaxDatabaseBytes: 64 << 20,
		RetryInitial:     10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("NewDurablePublisher() error = %v", err)
	}
	defer publisher.Close()

	if err := publisher.PublishPropertyReport(contracts.DeviceConfig{Name: "D1", ProductCode: "P1"}, map[string]interface{}{
		"device_code": "D1",
		"time":        int64(1),
		"data":        map[string]interface{}{"value": 7},
	}); err != nil {
		t.Fatalf("PublishPropertyReport() error = %v", err)
	}

	waitForDurable(t, func() bool { return first.count() == 1 && second.count() >= 2 })
	firstCount := first.count()
	second.setFailure(false)
	waitForDurable(t, func() bool {
		return pendingDurableRows(t, unwrapDurablePublisher(t, publisher)) == 0
	})
	if got := first.count(); got != firstCount {
		t.Fatalf("healthy group received %d reports after failed group retry; want %d", got, firstCount)
	}
}

func TestDurablePublisherReplaysPropertyReportAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	failing := &durableTargetStub{failPropertyReports: true}
	first, err := NewDurablePublisher(failing, DurablePublisherConfig{
		SQLitePath:       path,
		MaxDatabaseBytes: 64 << 20,
		RetryInitial:     10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("first NewDurablePublisher() error = %v", err)
	}
	if err := first.PublishPropertyReport(contracts.DeviceConfig{Name: "D1", ProductCode: "P1"}, map[string]interface{}{
		"device_code": "D1", "time": int64(2), "data": map[string]interface{}{"value": 8},
	}); err != nil {
		t.Fatalf("PublishPropertyReport() error = %v", err)
	}
	waitForDurable(t, func() bool { return failing.count() > 0 })
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	recovered := &durableTargetStub{}
	second, err := NewDurablePublisher(recovered, DurablePublisherConfig{
		SQLitePath:       path,
		MaxDatabaseBytes: 64 << 20,
		RetryInitial:     10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("second NewDurablePublisher() error = %v", err)
	}
	defer second.Close()
	waitForDurable(t, func() bool {
		return recovered.count() == 1 && pendingDurableRows(t, unwrapDurablePublisher(t, second)) == 0
	})
}

func TestDurableSinglePublisherDoesNotAdvertiseMultiGroup(t *testing.T) {
	publisher, err := NewDurablePublisher(&durableTargetStub{}, DurablePublisherConfig{
		SQLitePath:       filepath.Join(t.TempDir(), "outbox.db"),
		MaxDatabaseBytes: 64 << 20,
	}, nil)
	if err != nil {
		t.Fatalf("NewDurablePublisher() error = %v", err)
	}
	defer publisher.Close()
	if _, ok := publisher.(MultiGroupPublisher); ok {
		t.Fatal("single durable publisher unexpectedly implements MultiGroupPublisher")
	}
}

func TestDurablePublisherRejectsRemovedPendingDestination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	failing := &durableTargetStub{failPropertyReports: true}
	first, err := NewDurablePublisher(&durableMultiStub{
		names:   []string{"old-group"},
		targets: []Publisher{failing},
	}, DurablePublisherConfig{
		SQLitePath:       path,
		MaxDatabaseBytes: 64 << 20,
		RetryInitial:     10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("first NewDurablePublisher() error = %v", err)
	}
	if err := first.PublishPropertyReport(contracts.DeviceConfig{Name: "D1"}, map[string]interface{}{
		"trace_id": "pending-destination",
		"time":     int64(3),
	}); err != nil {
		t.Fatalf("PublishPropertyReport() error = %v", err)
	}
	waitForDurable(t, func() bool { return failing.count() > 0 })
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	replacement := &durableMultiStub{
		names:   []string{"new-group"},
		targets: []Publisher{&durableTargetStub{}},
	}
	if publisher, err := NewDurablePublisher(replacement, DurablePublisherConfig{
		SQLitePath:       path,
		MaxDatabaseBytes: 64 << 20,
	}, nil); err == nil {
		_ = publisher.Close()
		t.Fatal("NewDurablePublisher() accepted a removed pending destination")
	}
}

func unwrapDurablePublisher(t *testing.T, publisher Publisher) *durablePublisher {
	t.Helper()
	switch value := publisher.(type) {
	case *durablePublisher:
		return value
	case *durableMultiPublisher:
		return value.durablePublisher
	default:
		t.Fatalf("publisher type = %T, want durable publisher", publisher)
		return nil
	}
}

func pendingDurableRows(t *testing.T, publisher *durablePublisher) int64 {
	t.Helper()
	var count int64
	if err := publisher.store.db.QueryRow(`SELECT COUNT(1) FROM mqtt_destination_outbox`).Scan(&count); err != nil {
		t.Fatalf("count durable rows: %v", err)
	}
	return count
}

func waitForDurable(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for durable MQTT delivery")
}
