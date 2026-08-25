package reliable

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

func TestTelemetrySQLiteStorePreservesDynamicDataAndTimeOrder(t *testing.T) {
	store, err := newSQLiteStore(filepath.Join(t.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	first := testTelemetryEvent("trace-1", "D1", 2_000, map[string]interface{}{"temperature": 31.5})
	second := testTelemetryEvent("trace-2", "D1", 1_000, map[string]interface{}{"total_count": int64(139473), "running": true})
	firstID, err := store.Append(first, false, 10_000)
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	secondID, err := store.Append(second, true, 10_001)
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if firstID <= 0 || secondID <= firstID {
		t.Fatalf("ids are not positive monotonic int64 values: first=%d second=%d", firstID, secondID)
	}

	records, err := store.FetchPending(10, 0)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(records) != 2 || records[0].Event.TraceID != "trace-2" || records[1].Event.TraceID != "trace-1" {
		t.Fatalf("unexpected time order: %#v", records)
	}
	if !records[0].IsReplayed || records[0].Time != 1_000 {
		t.Fatalf("offline fields were not preserved: %#v", records[0])
	}
	data, err := records[0].Event.DataMap()
	if err != nil {
		t.Fatalf("DataMap() error = %v", err)
	}
	if got := actualTelemetryValue(data["total_count"]); got != float64(139473) {
		t.Fatalf("dynamic data was not preserved: %#v", data)
	}
}

func TestTelemetrySQLiteStorePersistsSendAttemptAndDoesNotReuseIDs(t *testing.T) {
	store, err := newSQLiteStore(filepath.Join(t.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	firstID, err := store.Append(testTelemetryEvent("trace-1", "D1", 1_000, map[string]interface{}{"value": 1}), false, 2_000)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := store.MarkAttempt(firstID, 3_000, false); err != nil {
		t.Fatalf("MarkAttempt() error = %v", err)
	}
	if err := store.MarkFailed(firstID, "offline"); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	records, err := store.FetchPending(1, 0)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(records) != 1 || !records[0].HasSendAt || records[0].SendAt != 3_000 || !records[0].IsReplayed || records[0].DeliveryAttempts != 1 {
		t.Fatalf("attempt metadata was not persisted: %#v", records)
	}
	if err := store.Ack(firstID); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	secondID, err := store.Append(testTelemetryEvent("trace-2", "D1", 4_000, map[string]interface{}{"value": 2}), false, 4_000)
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if secondID <= firstID {
		t.Fatalf("AUTOINCREMENT id was reused after emptying table: first=%d second=%d", firstID, secondID)
	}
}

func TestTelemetrySQLiteStoreUsesCreatedAtForRetention(t *testing.T) {
	store, err := newSQLiteStore(filepath.Join(t.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	if _, err := store.Append(testTelemetryEvent("old", "D1", 99_000, map[string]interface{}{"value": 1}), false, 1_000); err != nil {
		t.Fatalf("Append(old) error = %v", err)
	}
	if _, err := store.Append(testTelemetryEvent("new", "D1", 1_000, map[string]interface{}{"value": 2}), false, 10_000); err != nil {
		t.Fatalf("Append(new) error = %v", err)
	}
	removed, err := store.PurgeExpired(5_000)
	if err != nil {
		t.Fatalf("PurgeExpired() error = %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	records, err := store.FetchPending(10, 0)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(records) != 1 || records[0].Event.TraceID != "new" {
		t.Fatalf("retention used telemetry time instead of created_at: %#v", records)
	}
}

func TestTelemetrySQLiteStoreSerializesConcurrentWriters(t *testing.T) {
	store, err := newSQLiteStore(filepath.Join(t.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	const writers = 64
	errorsCh := make(chan error, writers)
	ids := make(chan int64, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			id, appendErr := store.Append(
				testTelemetryEvent(fmt.Sprintf("trace-%d", index), "D1", int64(index+1), map[string]interface{}{"value": index}),
				false,
				int64(index+1),
			)
			if appendErr != nil {
				errorsCh <- appendErr
				return
			}
			ids <- id
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	close(ids)
	for appendErr := range errorsCh {
		t.Fatalf("concurrent Append() error = %v", appendErr)
	}
	seen := make(map[int64]struct{}, writers)
	for id := range ids {
		if id <= 0 {
			t.Fatalf("concurrent Append() id = %d, want positive int64", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != writers {
		t.Fatalf("unique IDs = %d, want %d", len(seen), writers)
	}
}

func TestTelemetrySQLiteStoreFailsAtMaximumIDWithoutWrappingToZero(t *testing.T) {
	store, err := newSQLiteStore(filepath.Join(t.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	id, err := store.Append(testTelemetryEvent("seed", "D1", 1, map[string]interface{}{"value": 1}), false, 1)
	if err != nil {
		t.Fatalf("Append(seed) error = %v", err)
	}
	if err := store.Ack(id); err != nil {
		t.Fatalf("Ack(seed) error = %v", err)
	}
	const maxSQLiteID int64 = 9_223_372_036_854_775_807
	if _, err := store.db.Exec(`UPDATE sqlite_sequence SET seq = ? WHERE name = 'telemetry_outbox'`, maxSQLiteID); err != nil {
		t.Fatalf("set sqlite sequence error = %v", err)
	}
	if wrappedID, appendErr := store.Append(testTelemetryEvent("overflow", "D1", 2, map[string]interface{}{"value": 2}), false, 2); appendErr == nil {
		t.Fatalf("Append() at maximum SQLite id returned id=%d; want SQLITE_FULL error and no wrap", wrappedID)
	}
}

func BenchmarkTelemetrySQLiteStoreAppend(b *testing.B) {
	store, err := newSQLiteStore(filepath.Join(b.TempDir(), "telemetry-outbox.db"))
	if err != nil {
		b.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	event := testTelemetryEvent("", "D1", 1, map[string]interface{}{
		"total_count": 139473,
		"running":     true,
		"temperature": 31.5,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event.TraceID = fmt.Sprintf("benchmark-%d", i)
		event.CollectedAt = int64(i + 1)
		if _, err := store.Append(event, false, int64(i+1)); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func testTelemetryEvent(traceID, device string, timestamp int64, data map[string]interface{}) outevent.TelemetryEvent {
	values := make(map[string]outevent.TelemetryValue, len(data))
	for name, value := range data {
		raw, _ := json.Marshal(value)
		values[name] = outevent.TelemetryValue{Type: "Object", Value: raw, Origin: timestamp}
	}
	return outevent.TelemetryEvent{
		TraceID:     traceID,
		DeviceName:  device,
		ProductCode: "P1",
		SourceName:  "telemetry",
		CollectedAt: timestamp,
		Values:      values,
	}
}

func actualTelemetryValue(value interface{}) interface{} {
	item, _ := value.(map[string]interface{})
	return item["value"]
}
