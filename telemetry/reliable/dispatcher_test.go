package reliable

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

type telemetryDelivery struct {
	event    outevent.TelemetryEvent
	replayed bool
	sendAt   int64
}

type telemetryTransportStub struct {
	mu         sync.Mutex
	healthy    bool
	failing    bool
	received   []telemetryDelivery
	batchSizes []int
	hooks      []func()
	started    chan struct{}
	block      chan struct{}
}

func (s *telemetryTransportStub) PublishTelemetryBatchAt(items []TelemetryPublishRequest) []error {
	s.mu.Lock()
	s.batchSizes = append(s.batchSizes, len(items))
	s.mu.Unlock()
	results := make([]error, len(items))
	for i, item := range items {
		results[i] = s.PublishTelemetryEventAt(item.Event, item.Replayed, item.SendAt)
	}
	return results
}

func (s *telemetryTransportStub) batchSnapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.batchSizes...)
}

func (s *telemetryTransportStub) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return s.PublishTelemetryEventAt(event, replayed, time.Now().UnixMilli())
}

func (s *telemetryTransportStub) PublishTelemetryEventAt(event outevent.TelemetryEvent, replayed bool, sendAt int64) error {
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.healthy || s.failing {
		return errors.New("transport unavailable")
	}
	s.received = append(s.received, telemetryDelivery{event: event, replayed: replayed, sendAt: sendAt})
	return nil
}

func (s *telemetryTransportStub) HealthCheck() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.healthy {
		return errors.New("offline")
	}
	return nil
}

func (s *telemetryTransportStub) RegisterOnConnect(hook func()) {
	s.mu.Lock()
	s.hooks = append(s.hooks, hook)
	s.mu.Unlock()
}

func (s *telemetryTransportStub) setOnline(value bool) {
	s.mu.Lock()
	s.healthy = value
	s.failing = !value
	hooks := append([]func(){}, s.hooks...)
	s.mu.Unlock()
	if value {
		for _, hook := range hooks {
			hook()
		}
	}
}

func (s *telemetryTransportStub) snapshot() []telemetryDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]telemetryDelivery, len(s.received))
	copy(result, s.received)
	return result
}

func TestTelemetryDispatcherCommitsBeforeMQTTPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry-outbox.db")
	transport := &telemetryTransportStub{
		healthy: true,
		started: make(chan struct{}, 1),
		block:   make(chan struct{}),
	}
	dispatcher := newTestTelemetryDispatcher(t, path, transport)
	defer func() {
		select {
		case <-transport.block:
		default:
			close(transport.block)
		}
		_ = dispatcher.Close()
	}()

	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("trace-online", 1_000, 1)); err != nil {
		t.Fatalf("PublishAsyncValues() error = %v", err)
	}
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("MQTT publish did not start")
	}

	stats, err := dispatcher.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.PendingCount != 1 {
		t.Fatalf("pending count while MQTT is blocked = %d, want 1", stats.PendingCount)
	}
	records, err := dispatcher.store.FetchPending(1, 0)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(records) != 1 || !records[0].HasSendAt || records[0].SendAt <= records[0].Time {
		t.Fatalf("send_at was not persisted before MQTT: %#v", records)
	}
	close(transport.block)
	waitUntil(t, time.Second, func() bool {
		stats, statsErr := dispatcher.Stats()
		return statsErr == nil && stats.PendingCount == 0
	})
	deliveries := transport.snapshot()
	if len(deliveries) != 1 || deliveries[0].replayed || deliveries[0].sendAt != records[0].SendAt {
		t.Fatalf("unexpected online delivery metadata: %#v", deliveries)
	}
}

func TestValidateTelemetryOutboxConfigRejectsNegativeValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TelemetryOutboxConfig)
	}{
		{name: "retention", mutate: func(cfg *TelemetryOutboxConfig) { cfg.RetentionDays = -1 }},
		{name: "batch", mutate: func(cfg *TelemetryOutboxConfig) { cfg.SendBatchSize = -1 }},
		{name: "in flight", mutate: func(cfg *TelemetryOutboxConfig) { cfg.MaxInFlight = -1 }},
		{name: "in flight over batch", mutate: func(cfg *TelemetryOutboxConfig) { cfg.MaxInFlight = cfg.SendBatchSize + 1 }},
		{name: "in flight over limit", mutate: func(cfg *TelemetryOutboxConfig) { cfg.MaxInFlight = 257; cfg.SendBatchSize = 257 }},
		{name: "rate", mutate: func(cfg *TelemetryOutboxConfig) { cfg.MaxSendRatePerSec = -1 }},
		{name: "retry initial", mutate: func(cfg *TelemetryOutboxConfig) { cfg.RetryInitialMs = -1 }},
		{name: "retry max", mutate: func(cfg *TelemetryOutboxConfig) { cfg.RetryMaxMs = cfg.RetryInitialMs - 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultTelemetryOutboxConfig()
			test.mutate(&cfg)
			if err := ValidateTelemetryOutboxConfig(cfg); err == nil {
				t.Fatalf("ValidateTelemetryOutboxConfig(%#v) returned nil", cfg)
			}
		})
	}
}

func TestTelemetryDispatcherDrainsOfflineDataInTimeOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry-outbox.db")
	transport := &telemetryTransportStub{}
	dispatcher := newTestTelemetryDispatcher(t, path, transport)
	defer dispatcher.Close()

	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("later", 2_000, 2)); err != nil {
		t.Fatalf("PublishAsyncValues(later) error = %v", err)
	}
	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("earlier", 1_000, 1)); err != nil {
		t.Fatalf("PublishAsyncValues(earlier) error = %v", err)
	}
	waitUntil(t, time.Second, func() bool {
		stats, statsErr := dispatcher.Stats()
		return statsErr == nil && stats.PendingCount == 2
	})

	transport.setOnline(true)
	waitUntil(t, 2*time.Second, func() bool { return len(transport.snapshot()) == 2 })
	deliveries := transport.snapshot()
	if deliveries[0].event.TraceID != "earlier" || deliveries[1].event.TraceID != "later" {
		t.Fatalf("offline telemetry was not time ordered: %#v", deliveries)
	}
	for _, delivery := range deliveries {
		if !delivery.replayed || delivery.sendAt <= delivery.event.CollectedAt {
			t.Fatalf("offline replay fields are invalid: %#v", delivery)
		}
	}
}

func TestTelemetryDispatcherHonorsConfiguredMaxInFlight(t *testing.T) {
	transport := &telemetryTransportStub{}
	dispatcher, err := NewTelemetryDispatcher(TelemetryOutboxConfig{
		SQLitePath:        filepath.Join(t.TempDir(), "telemetry-outbox.db"),
		SendBatchSize:     5,
		MaxInFlight:       2,
		MaxSendRatePerSec: 0,
		RetryInitialMs:    10,
		RetryMaxMs:        20,
		MaxDatabaseBytes:  64 << 20,
	}, transport, nil)
	if err != nil {
		t.Fatalf("NewTelemetryDispatcher() error = %v", err)
	}
	defer dispatcher.Close()

	for i := 0; i < 5; i++ {
		if err := dispatcher.PublishAsyncValues(testDevice(), testAsync(fmt.Sprintf("trace-%d", i), int64(1_000+i), i)); err != nil {
			t.Fatalf("PublishAsyncValues(%d) error = %v", i, err)
		}
	}
	transport.setOnline(true)
	waitUntil(t, 2*time.Second, func() bool {
		stats, statsErr := dispatcher.Stats()
		return statsErr == nil && stats.PendingCount == 0
	})
	if got := transport.batchSnapshot(); !reflect.DeepEqual(got, []int{2, 2, 1}) {
		t.Fatalf("batch windows = %#v, want [2 2 1]", got)
	}
}

func TestTelemetryDispatcherMarksStartupRowsReplayedAndKeepsNewRowsRealtime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry-outbox.db")
	store, err := newSQLiteStore(path)
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	if _, err := store.Append(testTelemetryEvent("startup", "D1", 1_000, map[string]interface{}{"value": 1}), false, 1_000); err != nil {
		t.Fatalf("Append(startup) error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error = %v", err)
	}

	transport := &telemetryTransportStub{
		healthy: true,
		started: make(chan struct{}, 1),
		block:   make(chan struct{}),
	}
	dispatcher := newTestTelemetryDispatcher(t, path, transport)
	defer func() {
		select {
		case <-transport.block:
		default:
			close(transport.block)
		}
		_ = dispatcher.Close()
	}()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not begin")
	}
	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("new", 2_000, 2)); err != nil {
		t.Fatalf("PublishAsyncValues(new) error = %v", err)
	}
	close(transport.block)
	waitUntil(t, 2*time.Second, func() bool { return len(transport.snapshot()) == 2 })
	deliveries := transport.snapshot()
	if deliveries[0].event.TraceID != "startup" || !deliveries[0].replayed {
		t.Fatalf("startup row was not replayed first: %#v", deliveries)
	}
	if deliveries[1].event.TraceID != "new" || deliveries[1].replayed {
		t.Fatalf("post-start online row was incorrectly marked replayed: %#v", deliveries)
	}
}

func TestTelemetryDispatcherReplaysEntirePendingSetAfterPublishFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry-outbox.db")
	transport := &telemetryTransportStub{
		healthy: true,
		failing: true,
		started: make(chan struct{}, 1),
		block:   make(chan struct{}),
	}
	dispatcher := newTestTelemetryDispatcher(t, path, transport)
	defer func() {
		select {
		case <-transport.block:
		default:
			close(transport.block)
		}
		_ = dispatcher.Close()
	}()

	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("attempted", 1_000, 1)); err != nil {
		t.Fatalf("PublishAsyncValues(attempted) error = %v", err)
	}
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("MQTT publish did not start")
	}
	if err := dispatcher.PublishAsyncValues(testDevice(), testAsync("waiting", 2_000, 2)); err != nil {
		t.Fatalf("PublishAsyncValues(waiting) error = %v", err)
	}
	close(transport.block)
	waitUntil(t, time.Second, func() bool { return !dispatcher.isOnline() })

	transport.setOnline(true)
	waitUntil(t, 2*time.Second, func() bool { return len(transport.snapshot()) == 2 })
	for _, delivery := range transport.snapshot() {
		if !delivery.replayed {
			t.Fatalf("publish failure did not replay the entire pending set: %#v", transport.snapshot())
		}
	}
}

func newTestTelemetryDispatcher(t *testing.T, path string, transport *telemetryTransportStub) *TelemetryDispatcher {
	t.Helper()
	dispatcher, err := NewTelemetryDispatcher(TelemetryOutboxConfig{
		SQLitePath:        path,
		RetentionDays:     0,
		SendBatchSize:     10,
		MaxSendRatePerSec: 0,
		RetryInitialMs:    10,
		RetryMaxMs:        20,
	}, transport, nil)
	if err != nil {
		t.Fatalf("NewTelemetryDispatcher() error = %v", err)
	}
	return dispatcher
}

func testDevice() contracts.DeviceConfig {
	return contracts.DeviceConfig{Name: "D1", ProductCode: "P1"}
}

func testAsync(traceID string, timestamp int64, value int) *contracts.AsyncValues {
	return &contracts.AsyncValues{
		TraceID:     traceID,
		DeviceName:  "D1",
		SourceName:  "telemetry",
		CollectedAt: timestamp,
		Values: []*contracts.CommandValue{{
			DeviceResourceName: "value",
			Type:               "Int64",
			Value:              int64(value),
			Origin:             timestamp,
		}},
	}
}
