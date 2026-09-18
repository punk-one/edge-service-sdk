package mqtt

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
)

type durableTargetStub struct {
	mu                  sync.Mutex
	failPropertyReports bool
	propertyReports     int
	telemetry           []string
}

func (s *durableTargetStub) PublishTelemetry(contracts.DeviceConfig, map[string]interface{}) error {
	return nil
}
func (s *durableTargetStub) PublishCommandValues(contracts.DeviceConfig, []*contracts.CommandValue) error {
	return nil
}
func (s *durableTargetStub) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return s.PublishTelemetryEventAt(event, replayed, time.Now().UnixMilli())
}
func (s *durableTargetStub) PublishTelemetryEventAt(event outevent.TelemetryEvent, _ bool, _ int64) error {
	s.mu.Lock()
	s.telemetry = append(s.telemetry, event.TraceID)
	s.mu.Unlock()
	return nil
}
func (s *durableTargetStub) PublishTelemetryBatchAt(items []reliable.TelemetryPublishRequest) []error {
	results := make([]error, len(items))
	for i, item := range items {
		results[i] = s.PublishTelemetryEventAt(item.Event, item.Replayed, item.SendAt)
	}
	return results
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
func (s *durableTargetStub) telemetrySnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.telemetry...)
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

func TestDurableSinglePublisherDrainsLegacyTelemetryBeforeDirectBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	store, err := newDurablePublisherStore(path, 64<<20)
	if err != nil {
		t.Fatalf("newDurablePublisherStore() error = %v", err)
	}
	legacy := testDurableTelemetryEvent("legacy")
	payload, err := json.Marshal(durableEnvelope{Telemetry: &legacy})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := store.append([]string{durableDefaultDestination}, durableKindTelemetryEvent, durableKey(durableKindTelemetryEvent, legacy.TraceID, payload), payload, true); err != nil {
		t.Fatalf("append legacy telemetry error = %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatalf("store.close() error = %v", err)
	}

	target := &durableTargetStub{}
	publisher, err := NewDurablePublisher(target, DurablePublisherConfig{
		SQLitePath:       path,
		MaxDatabaseBytes: 64 << 20,
		RetryInitial:     10 * time.Millisecond,
		RetryMax:         20 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatalf("NewDurablePublisher() error = %v", err)
	}
	defer publisher.Close()

	batch, ok := publisher.(reliable.BatchTelemetryTransport)
	if !ok {
		t.Fatal("durable publisher does not expose batch telemetry transport")
	}
	items := []reliable.TelemetryPublishRequest{
		{Event: testDurableTelemetryEvent("new-1"), SendAt: time.Now().UnixMilli()},
		{Event: testDurableTelemetryEvent("new-2"), SendAt: time.Now().UnixMilli()},
	}
	for i, result := range batch.PublishTelemetryBatchAt(items) {
		if result != nil {
			t.Fatalf("batch result %d = %v", i, result)
		}
	}
	if got := target.telemetrySnapshot(); !reflect.DeepEqual(got, []string{"legacy", "new-1", "new-2"}) {
		t.Fatalf("telemetry delivery order = %#v", got)
	}
	if count := pendingDurableRows(t, unwrapDurablePublisher(t, publisher)); count != 0 {
		t.Fatalf("pending durable telemetry rows = %d, want 0", count)
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

func testDurableTelemetryEvent(traceID string) outevent.TelemetryEvent {
	return outevent.TelemetryEvent{
		TraceID: traceID, DeviceName: "D1", ProductCode: "P1", SourceName: "telemetry", CollectedAt: 1,
		Values: map[string]outevent.TelemetryValue{},
	}
}
