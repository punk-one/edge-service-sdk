package command

import (
	pathpkg "path/filepath"
	"testing"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
)

func TestExecuteAsyncCommandReturnsAcceptedAndPublishesFinalResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "async.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	device := testDevice("qhl0001", "qhl", "program_install")
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{Identifier: "program_install", Mode: "async"},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			ctx.ReportProgress(cmdapi.ProgressPayload{"message": "downloading", "phase": "download"})
			time.Sleep(20 * time.Millisecond)
			ctx.ReportProgress(cmdapi.ProgressPayload{"message": "installing", "phase": "install"})
			return map[string]interface{}{"program_install_done": true}, nil
		},
	})
	publisher := &commandTestPublisher{}
	service := NewService(&commandTestCatalog{device: device}, &commandTestDriver{}, publisher, store, nil, registry, nil)
	service.commandResultEnabled = true

	result, statusCode := service.Execute("program_install", cmdapi.CommandRequest{TraceID: "trace-async-1", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 202 {
		t.Fatalf("statusCode = %d, want 202", statusCode)
	}
	if result.Code != ctl.CodeAccepted {
		t.Fatalf("code = %d, want %d", result.Code, ctl.CodeAccepted)
	}

	waitForAsyncCommandMessages(t, publisher, 1, time.Second)
	message := publisher.Message(0).payload
	if got := message["trace_id"]; got != "trace-async-1" {
		t.Fatalf("trace_id = %#v, want trace-async-1", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}
	events, err := store.ListResults("trace-async-1", 10)
	if err != nil {
		t.Fatalf("ListResults error = %v", err)
	}
	if len(events) < 4 {
		t.Fatalf("len(events) = %d, want at least 4", len(events))
	}
	if events[1].Code != ctl.CodeProcessing || events[2].Code != ctl.CodeProcessing {
		t.Fatalf("processing events = %#v, want processing codes", []int{events[1].Code, events[2].Code})
	}
	progress, ok := events[1].Data["progress"].(map[string]interface{})
	if !ok {
		t.Fatalf("progress event data = %#v, want object", events[1].Data)
	}
	if got := progress["phase"]; got != "download" {
		t.Fatalf("progress.phase = %#v, want download", got)
	}
	job, found, err := store.LoadJob("trace-async-1")
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
	pending, err := store.ListPendingCommands()
	if err != nil {
		t.Fatalf("ListPendingCommands error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending commands = %d, want 0", len(pending))
	}
}

func TestExecuteWithCompletedTraceReturnsStoredResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "duplicate.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	device := testDevice("qhl0001", "qhl", "program_install")
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{Identifier: "program_install", Mode: "async"},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			return map[string]interface{}{"done": true}, nil
		},
	})
	publisher := &commandTestPublisher{}
	service := NewService(&commandTestCatalog{device: device}, &commandTestDriver{}, publisher, store, nil, registry, nil)
	service.commandResultEnabled = true

	first, statusCode := service.Execute("program_install", cmdapi.CommandRequest{TraceID: "trace-dup", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 202 || first.Code != ctl.CodeAccepted {
		t.Fatalf("first execute = (%d, %#v), want accepted", statusCode, first)
	}
	waitForAsyncCommandMessages(t, publisher, 1, time.Second)

	second, statusCode := service.Execute("program_install", cmdapi.CommandRequest{TraceID: "trace-dup", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 200 {
		t.Fatalf("statusCode = %d, want 200", statusCode)
	}
	if second.Code != ctl.CodeSuccess {
		t.Fatalf("code = %d, want %d", second.Code, ctl.CodeSuccess)
	}
	if publisher.Count() != 1 {
		t.Fatalf("publisher.Count = %d, want 1", publisher.Count())
	}
}

func TestResumePendingAsyncCommand(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "resume.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	request := cmdapi.CommandRequest{TraceID: "trace-resume", DeviceCode: "qhl0001", Data: map[string]interface{}{}}
	if _, err := store.UpsertJob(rtcontrol.JobState{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "qhl", Kind: "command:program_install", Identifier: "program_install", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("UpsertJob accepted error = %v", err)
	}
	if err := store.SaveResult(request.TraceID, ctl.Result{TraceID: request.TraceID, Code: ctl.CodeAccepted, Message: "accepted", Data: map[string]interface{}{"accepted": true}, Time: now}, false); err != nil {
		t.Fatalf("SaveResult accepted error = %v", err)
	}
	if _, err := store.SavePendingCommand(rtcontrol.PendingCommand{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "qhl", Identifier: "program_install", Request: ctl.Request(request), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("SavePendingCommand error = %v", err)
	}

	device := testDevice("qhl0001", "qhl", "program_install")
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{Identifier: "program_install", Mode: "async"},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			return map[string]interface{}{"done": true}, nil
		},
	})
	publisher := &commandTestPublisher{}
	service := NewService(&commandTestCatalog{device: device}, &commandTestDriver{}, publisher, store, nil, registry, nil)
	service.commandResultEnabled = true

	if err := service.ResumePending(); err != nil {
		t.Fatalf("ResumePending error = %v", err)
	}
	waitForAsyncCommandMessages(t, publisher, 1, time.Second)

	job, found, err := store.LoadJob(request.TraceID)
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
}

func waitForAsyncCommandMessages(t *testing.T, publisher *commandTestPublisher, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if publisher.Count() >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d command messages; got %d", count, publisher.Count())
}
