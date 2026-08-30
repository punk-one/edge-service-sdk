package reliable

import "testing"

func TestMalformedTelemetryRowIsQuarantinedWithoutBlockingQueue(t *testing.T) {
	store, err := newSQLiteStore(t.TempDir() + "/telemetry.db")
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()
	firstID, err := store.Append(testTelemetryEvent("bad", "D1", 1, map[string]interface{}{"value": 1}), false, 1)
	if err != nil {
		t.Fatalf("Append(bad) error = %v", err)
	}
	if _, err := store.Append(testTelemetryEvent("good", "D1", 2, map[string]interface{}{"value": 2}), false, 2); err != nil {
		t.Fatalf("Append(good) error = %v", err)
	}
	if _, err := store.db.Exec(`UPDATE telemetry_outbox SET data_json = '{broken' WHERE id = ?`, firstID); err != nil {
		t.Fatalf("corrupt row error = %v", err)
	}
	items, err := store.FetchPending(10, 0)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(items) != 1 || items[0].Event.TraceID != "good" {
		t.Fatalf("pending = %#v, want only good row", items)
	}
	deadLetters, err := store.DeadLetterCount()
	if err != nil || deadLetters != 1 {
		t.Fatalf("dead letters = %d err=%v, want 1", deadLetters, err)
	}
}

func TestTelemetryStoreUsesFullSynchronousMode(t *testing.T) {
	store, err := newSQLiteStore(t.TempDir() + "/telemetry.db")
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()
	var mode int
	if err := store.db.QueryRow(`PRAGMA synchronous;`).Scan(&mode); err != nil {
		t.Fatalf("PRAGMA synchronous error = %v", err)
	}
	if mode != 2 {
		t.Fatalf("synchronous mode = %d, want FULL (2)", mode)
	}
}
