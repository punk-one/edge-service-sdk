package control

import (
	pathpkg "path/filepath"
	"testing"
	"time"

	ctl "github.com/punk-one/edge-service-sdk/control"
)

func TestSQLiteStoreFinalStatePreventsRollback(t *testing.T) {
	store, err := NewSQLiteStore(pathpkg.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	applied, err := store.UpsertJob(JobState{TraceID: "trace-1", DeviceCode: "dev1", Kind: "command:test", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now})
	if err != nil || !applied {
		t.Fatalf("accepted upsert = (%v, %v), want (true, nil)", applied, err)
	}
	if err := store.SaveResult("trace-1", ctl.Result{TraceID: "trace-1", Code: ctl.CodeAccepted, Message: "accepted", Data: map[string]interface{}{}, Time: now}, false); err != nil {
		t.Fatalf("SaveResult accepted error = %v", err)
	}
	applied, err = store.UpsertJob(JobState{TraceID: "trace-1", DeviceCode: "dev1", Kind: "command:test", Code: ctl.CodeSuccess, Message: "success", CreatedAt: now, UpdatedAt: now + 1, FinishedAt: now + 1})
	if err != nil || !applied {
		t.Fatalf("final upsert = (%v, %v), want (true, nil)", applied, err)
	}
	applied, err = store.UpsertJob(JobState{TraceID: "trace-1", DeviceCode: "dev1", Kind: "command:test", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now + 2})
	if err != nil {
		t.Fatalf("rollback upsert error = %v", err)
	}
	if applied {
		t.Fatalf("rollback upsert applied = true, want false")
	}
	job, found, err := store.LoadJob("trace-1")
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
}

func TestSQLiteStoreListJobsSupportsFilters(t *testing.T) {
	store, err := NewSQLiteStore(pathpkg.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	jobs := []JobState{
		{TraceID: "trace-cmd-final", DeviceCode: "qhl0001", ProductCode: "qhl", Kind: "command:program_install", Identifier: "program_install", Code: ctl.CodeSuccess, Message: "success", CreatedAt: now, UpdatedAt: now, FinishedAt: now},
		{TraceID: "trace-cmd-pending", DeviceCode: "qhl0001", ProductCode: "qhl", Kind: "command:program_delete", Identifier: "program_delete", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now + 1, UpdatedAt: now + 1},
		{TraceID: "trace-cmd-processing", DeviceCode: "qhl0001", ProductCode: "qhl", Kind: "command:program_sync", Identifier: "program_sync", Code: ctl.CodeProcessing, Message: "processing", CreatedAt: now + 2, UpdatedAt: now + 2},
		{TraceID: "trace-prop-final", DeviceCode: "acm006", ProductCode: "acm", Kind: "property:set", Identifier: "", Code: ctl.CodePartialSuccess, Message: "partial", CreatedAt: now + 3, UpdatedAt: now + 3, FinishedAt: now + 3},
		{TraceID: "trace-prop-failed", DeviceCode: "acm006", ProductCode: "acm", Kind: "property:set", Identifier: "", Code: ctl.CodeDriverError, Message: "driver error", CreatedAt: now + 4, UpdatedAt: now + 4, FinishedAt: now + 4},
	}
	for _, job := range jobs {
		if _, err := store.UpsertJob(job); err != nil {
			t.Fatalf("UpsertJob(%s) error = %v", job.TraceID, err)
		}
	}
	if _, err := store.SavePendingCommand(PendingCommand{TraceID: "trace-cmd-pending", DeviceCode: "qhl0001", ProductCode: "qhl", Identifier: "program_delete", Request: ctl.Request{TraceID: "trace-cmd-pending", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, CreatedAt: now + 1, UpdatedAt: now + 1}); err != nil {
		t.Fatalf("SavePendingCommand error = %v", err)
	}
	if _, err := store.SavePendingProperty(PendingProperty{TraceID: "trace-prop-pending", DeviceCode: "acm006", ProductCode: "acm", Request: ctl.Request{TraceID: "trace-prop-pending", DeviceCode: "acm006", Data: map[string]interface{}{}}, CreatedAt: now + 5, UpdatedAt: now + 5}); err != nil {
		t.Fatalf("SavePendingProperty error = %v", err)
	}

	items, err := store.ListJobs(JobFilter{DeviceCode: "qhl0001", Limit: 10})
	if err != nil {
		t.Fatalf("ListJobs device filter error = %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("ListJobs device filter len = %d, want 3", len(items))
	}
	if items[0].TraceID != "trace-cmd-processing" {
		t.Fatalf("first trace = %s, want trace-cmd-processing", items[0].TraceID)
	}

	items, err = store.ListJobs(JobFilter{Kind: "command", FinalSet: true, Final: false, Limit: 10})
	if err != nil {
		t.Fatalf("ListJobs pending filter error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("pending jobs len = %d, want 2", len(items))
	}

	items, err = store.ListJobs(JobFilter{Identifier: "program_install", Limit: 10})
	if err != nil {
		t.Fatalf("ListJobs identifier filter error = %v", err)
	}
	if len(items) != 1 || items[0].TraceID != "trace-cmd-final" {
		t.Fatalf("identifier jobs = %#v, want only trace-cmd-final", items)
	}

	items, err = store.ListJobs(JobFilter{CreatedFrom: now + 2, CreatedTo: now + 4, Limit: 10, Offset: 1})
	if err != nil {
		t.Fatalf("ListJobs time filter error = %v", err)
	}
	if len(items) != 2 || items[0].TraceID != "trace-prop-final" {
		t.Fatalf("time window jobs = %#v, want offset window starting at trace-prop-final", items)
	}

	diagnostics, err := store.JobDiagnostics(JobFilter{Kind: "command"})
	if err != nil {
		t.Fatalf("JobDiagnostics error = %v", err)
	}
	if diagnostics.Total != 3 || diagnostics.Pending != 2 || diagnostics.Success != 1 {
		t.Fatalf("unexpected command diagnostics: %#v", diagnostics)
	}
	if diagnostics.PendingCommandQueue != 1 || diagnostics.PendingPropertyQueue != 0 {
		t.Fatalf("unexpected pending queues: %#v", diagnostics)
	}

	diagnostics, err = store.JobDiagnostics(JobFilter{Kind: "property"})
	if err != nil {
		t.Fatalf("JobDiagnostics property error = %v", err)
	}
	if diagnostics.Property != 2 || diagnostics.PartialSuccess != 1 || diagnostics.Failed != 1 {
		t.Fatalf("unexpected property diagnostics: %#v", diagnostics)
	}
	if diagnostics.PendingPropertyQueue != 1 {
		t.Fatalf("pending property queue = %d, want 1", diagnostics.PendingPropertyQueue)
	}
}

func TestSQLiteStoreListResultsReturnsChronologicalTail(t *testing.T) {
	store, err := NewSQLiteStore(pathpkg.Join(t.TempDir(), "control-results.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	traceID := "trace-events-1"
	results := []ctl.Result{
		{TraceID: traceID, Code: ctl.CodeAccepted, Message: "accepted", Data: map[string]interface{}{"queue": "command"}, Time: now},
		{TraceID: traceID, Code: ctl.CodeProcessing, Message: "processing", Data: map[string]interface{}{"step": "download"}, Time: now + 1},
		{TraceID: traceID, Code: ctl.CodeSuccess, Message: "success", Data: map[string]interface{}{"done": true}, Time: now + 2},
	}
	for _, result := range results {
		if err := store.SaveResult(traceID, result, IsFinalCode(result.Code)); err != nil {
			t.Fatalf("SaveResult(%d) error = %v", result.Code, err)
		}
	}
	items, err := store.ListResults(traceID, 2)
	if err != nil {
		t.Fatalf("ListResults error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
	if items[0].Code != ctl.CodeProcessing || items[1].Code != ctl.CodeSuccess {
		t.Fatalf("items codes = %#v, want [processing success]", []int{items[0].Code, items[1].Code})
	}
}
