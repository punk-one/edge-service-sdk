package reliable

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	coreevent "github.com/punk-one/edge-service-sdk/event"
)

type eventTransportStub struct {
	mu       sync.Mutex
	failing  bool
	received []receivedEvent
}

type receivedEvent struct {
	event    coreevent.Event
	replayed bool
}

func (s *eventTransportStub) PublishEvent(event coreevent.Event, replayed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failing {
		return errors.New("transport unavailable")
	}
	s.received = append(s.received, receivedEvent{event: event, replayed: replayed})
	return nil
}

func (s *eventTransportStub) setFailing(value bool) {
	s.mu.Lock()
	s.failing = value
	s.mu.Unlock()
}

func (s *eventTransportStub) snapshot() []receivedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]receivedEvent, len(s.received))
	copy(result, s.received)
	return result
}

func testReliableEvent() coreevent.Event {
	return coreevent.Event{
		Time:        1710000000123,
		DeviceCode:  "D1",
		ProductCode: "P1",
		TraceID:     "trace-event-1",
		CreatedAt:   1710000000999,
		Data: coreevent.EventData{
			EventCode:       "EVENT_TEST",
			Category:        coreevent.CategoryOEE,
			Type:            coreevent.EventTypePulse,
			EventInstanceID: "evt-test-1",
			Payload:         map[string]interface{}{"value": 42},
		},
	}
}

func TestEventSQLiteStorePreservesOrderAndInternalMetadata(t *testing.T) {
	store, err := newEventSQLiteStore(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatalf("newEventSQLiteStore() error = %v", err)
	}
	defer store.close()

	first := testReliableEvent()
	second := first
	second.Time--
	second.Data.EventInstanceID = "evt-test-2"
	if err := store.appendBatch([]coreevent.Event{first, second}); err != nil {
		t.Fatalf("appendBatch() error = %v", err)
	}
	stats, err := store.stats()
	if err != nil {
		t.Fatalf("stats() error = %v", err)
	}
	if stats.pendingCount != 2 {
		t.Fatalf("pending count = %d, want 2", stats.pendingCount)
	}
	items, err := store.fetchPending(10)
	if err != nil {
		t.Fatalf("fetchPending() error = %v", err)
	}
	if len(items) != 2 || items[0].Event.Data.EventInstanceID != "evt-test-2" {
		t.Fatalf("unexpected event order: %#v", items)
	}
	if items[0].Event.ProductCode != "P1" || items[0].Event.TraceID != "trace-event-1" || items[0].Event.CreatedAt != first.CreatedAt {
		t.Fatalf("internal transport metadata was not preserved: %#v", items[0].Event)
	}
	if err := store.ack([]int64{items[0].ID, items[1].ID}); err != nil {
		t.Fatalf("ack() error = %v", err)
	}
	stats, err = store.stats()
	if err != nil {
		t.Fatalf("stats() after ack error = %v", err)
	}
	if stats.pendingCount != 0 {
		t.Fatalf("pending count after ack = %d, want 0", stats.pendingCount)
	}
}

func TestEventDispatcherQueuesOfflineEventsAndReplaysThem(t *testing.T) {
	transport := &eventTransportStub{failing: true}
	dispatcher, err := NewEventDispatcher(Config{
		Enabled:          true,
		SQLitePath:       filepath.Join(t.TempDir(), "events.db"),
		MemoryQueueSize:  1,
		BatchSize:        1,
		FlushIntervalMs:  5,
		ReplayIntervalMs: 10,
		ReplayRatePerSec: 100,
		RetentionDays:    0,
	}, transport, nil)
	if err != nil {
		t.Fatalf("NewEventDispatcher() error = %v", err)
	}
	defer dispatcher.Close()

	event := testReliableEvent()
	if err := dispatcher.Publish(event); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	waitUntil(t, time.Second, func() bool {
		stats, statsErr := dispatcher.Stats()
		return statsErr == nil && stats.BufferDepth == 1
	})

	transport.setFailing(false)
	waitUntil(t, 2*time.Second, func() bool {
		stats, statsErr := dispatcher.Stats()
		return statsErr == nil && stats.BufferDepth == 0
	})
	received := transport.snapshot()
	if len(received) == 0 || !received[len(received)-1].replayed {
		t.Fatalf("expected a replayed event, got %#v", received)
	}
	replayed := received[len(received)-1].event
	if replayed.Time != event.Time || replayed.Data.EventInstanceID != event.Data.EventInstanceID || replayed.TraceID != event.TraceID {
		t.Fatalf("replay changed event identity or time: original=%#v replay=%#v", event, replayed)
	}
}

func TestEventDispatcherCloseDrainsMemoryQueue(t *testing.T) {
	transport := &eventTransportStub{failing: true}
	path := filepath.Join(t.TempDir(), "events.db")
	dispatcher, err := NewEventDispatcher(Config{
		Enabled:          true,
		SQLitePath:       path,
		MemoryQueueSize:  8,
		BatchSize:        100,
		FlushIntervalMs:  1_000,
		ReplayIntervalMs: 1_000,
		ReplayRatePerSec: 1,
	}, transport, nil)
	if err != nil {
		t.Fatalf("NewEventDispatcher() error = %v", err)
	}
	if err := dispatcher.Publish(testReliableEvent()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	store, err := newEventSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen event store error = %v", err)
	}
	defer store.close()
	stats, err := store.stats()
	if err != nil {
		t.Fatalf("reopened stats() error = %v", err)
	}
	if stats.pendingCount != 1 {
		t.Fatalf("pending count after close = %d, want 1", stats.pendingCount)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
