package reliable

import (
	"testing"

	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

func TestSQLiteStoreKeepLatestOnly(t *testing.T) {
	store, err := newSQLiteStore(t.TempDir()+"/queue.db", true)
	if err != nil {
		t.Fatalf("newSQLiteStore() error = %v", err)
	}
	defer store.Close()

	if err := store.AppendBatch([]outevent.TelemetryEvent{
		{TraceID: "t1", DeviceName: "acm006", ProductCode: "acm", SourceName: "telemetry", CollectedAt: 1000},
		{TraceID: "t2", DeviceName: "acm006", ProductCode: "acm", SourceName: "telemetry", CollectedAt: 2000},
	}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.PendingCount != 1 {
		t.Fatalf("pending count = %d, want 1", stats.PendingCount)
	}

	records, err := store.FetchPending(10)
	if err != nil {
		t.Fatalf("FetchPending() error = %v", err)
	}
	if len(records) != 1 || records[0].Event.TraceID != "t2" {
		t.Fatalf("expected latest event to remain, got %+v", records)
	}
}
