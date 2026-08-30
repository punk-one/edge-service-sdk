package control

import (
	"sync"
	"testing"
	"time"

	ctl "github.com/punk-one/edge-service-sdk/control"
)

func TestInterruptedExecutionBecomesAmbiguousAndIsNotReplayed(t *testing.T) {
	path := t.TempDir() + "/control.db"
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	claimer := store.(ExecutionClaimer)
	now := time.Now().UnixMilli()
	claimed, err := claimer.ClaimExecution(JobState{TraceID: "trace-interrupted", DeviceCode: "device-a", ProductCode: "product-a", Kind: "command:start", Identifier: "start", Code: ctl.CodeProcessing, Message: "processing", CreatedAt: now, UpdatedAt: now})
	if err != nil || !claimed {
		t.Fatalf("ClaimExecution() = %t, %v", claimed, err)
	}
	if _, err := store.SavePendingCommand(PendingCommand{
		TraceID:     "trace-interrupted",
		DeviceCode:  "device-a",
		ProductCode: "product-a",
		Identifier:  "start",
		Request:     ctl.Request{TraceID: "trace-interrupted", DeviceCode: "device-a", Data: map[string]interface{}{}},
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("SavePendingCommand() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	job, found, err := reopened.LoadJob("trace-interrupted")
	if err != nil || !found {
		t.Fatalf("LoadJob() found=%t err=%v", found, err)
	}
	if job.Code != ctl.CodeAmbiguous {
		t.Fatalf("job code = %d, want %d", job.Code, ctl.CodeAmbiguous)
	}
	deliveries, err := reopened.(ResultOutbox).ListResultDeliveries("command", 10)
	if err != nil || len(deliveries) != 1 || deliveries[0].Result.Code != ctl.CodeAmbiguous {
		t.Fatalf("deliveries = %#v err=%v", deliveries, err)
	}
	pending, err := reopened.ListPendingCommands()
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending commands = %#v err=%v, want none", pending, err)
	}
}

func TestOnlyOneConcurrentExecutionClaimSucceeds(t *testing.T) {
	store, err := NewSQLiteStore(t.TempDir() + "/control.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer store.Close()
	now := time.Now().UnixMilli()
	if _, err := store.UpsertJob(JobState{TraceID: "trace-race", DeviceCode: "device-a", Kind: "property:set", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("UpsertJob() error = %v", err)
	}
	claimer := store.(ExecutionClaimer)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, claimErr := claimer.ClaimExecution(JobState{TraceID: "trace-race", DeviceCode: "device-a", Kind: "property:set", Code: ctl.CodeProcessing, Message: "processing", UpdatedAt: time.Now().UnixMilli()})
			if claimErr != nil {
				t.Errorf("ClaimExecution() error = %v", claimErr)
			}
			results <- claimed
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for claimed := range results {
		if claimed {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
}
