package app

import (
	"net/http"
	pathpkg "path/filepath"
	"testing"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
)

type queryTestCommand struct {
	desc cmdapi.CommandDescriptor
}

func (c queryTestCommand) Descriptor() cmdapi.CommandDescriptor {
	return c.desc
}

func (c queryTestCommand) Execute(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
	return map[string]interface{}{}, nil
}

func TestBuildDeviceModelQueries(t *testing.T) {
	onChange := true
	registry := cmdapi.NewRegistry()
	registry.MustRegister(queryTestCommand{desc: cmdapi.CommandDescriptor{
		Identifier: "program_install",
		Name:       "Program Install",
		Mode:       "async",
		InputParams: []cmdapi.CommandParam{{
			Identifier: "package_url",
			ValueType:  "String",
			Required:   true,
		}},
		OutputParams: []cmdapi.CommandParam{{
			Identifier: "program_install_done",
			ValueType:  "Bool",
		}},
	}})
	registry.MustRegister(queryTestCommand{desc: cmdapi.CommandDescriptor{
		Identifier: "hidden_command",
		Name:       "Hidden Command",
		Mode:       "sync",
	}})
	sdk := NewDeviceSDK(rtconfig.Config{Devices: []contracts.DeviceConfig{{
		Name:        "qhl0001",
		ProductCode: "qhl",
		Telemetry: contracts.TelemetryConfig{
			Interval:      "5s",
			WatchedFields: []string{"program_fault"},
			Points: []contracts.PointConfig{{
				Name:      "program_fault",
				ValueType: "Bool",
				NodeName:  "MX10.3",
			}},
		},
		Property: contracts.PropertyConfig{
			Interval: "10s",
			Points: []contracts.PointConfig{{
				Name:      "program_install_trigger",
				ValueType: "Bool",
				NodeName:  "MX20.0",
				ReadWrite: "RW",
				OnChange:  &onChange,
			}},
			Structs: []contracts.PropertyStruct{{
				Name:      "packages",
				Kind:      "array",
				IndexBase: 1,
				MaxItems:  4,
				Address: contracts.PropertyStructAddress{
					DBNumber:    300,
					BaseOffset:  0,
					IndexStride: 64,
				},
				Fields: []contracts.PropertyStructField{{
					Name:        "path",
					ValueType:   "String",
					FieldOffset: 0,
					MaxLength:   64,
					ReadWrite:   "RW",
				}},
			}},
		},
		Commands: []contracts.CommandConfig{{
			Identifier: "program_install",
		}},
	}}}, nil, nil)

	propertyBody, status := buildPropertyModelQuery(sdk)("qhl0001")
	if status != http.StatusOK {
		t.Fatalf("property status = %d, want %d", status, http.StatusOK)
	}
	if got := propertyBody["device_code"]; got != "qhl0001" {
		t.Fatalf("device_code = %#v, want qhl0001", got)
	}
	points := propertyBody["points"].([]map[string]interface{})
	if len(points) != 1 || points[0]["name"] != "program_install_trigger" {
		t.Fatalf("unexpected property points: %#v", points)
	}
	structs := propertyBody["structs"].([]map[string]interface{})
	if len(structs) != 1 || structs[0]["name"] != "packages" {
		t.Fatalf("unexpected property structs: %#v", structs)
	}

	telemetryBody, status := buildTelemetryModelQuery(sdk)("qhl0001")
	if status != http.StatusOK {
		t.Fatalf("telemetry status = %d, want %d", status, http.StatusOK)
	}
	telemetryPoints := telemetryBody["points"].([]map[string]interface{})
	if len(telemetryPoints) != 1 || telemetryPoints[0]["name"] != "program_fault" {
		t.Fatalf("unexpected telemetry points: %#v", telemetryPoints)
	}

	listBody, status := buildCommandListQuery(sdk, registry)("qhl0001")
	if status != http.StatusOK {
		t.Fatalf("command list status = %d, want %d", status, http.StatusOK)
	}
	commands := listBody["commands"].([]map[string]interface{})
	if len(commands) != 1 || commands[0]["identifier"] != "program_install" {
		t.Fatalf("unexpected commands: %#v", commands)
	}

	commandBody, status := buildCommandDetailQuery(sdk, registry)("qhl0001", "program_install")
	if status != http.StatusOK {
		t.Fatalf("command detail status = %d, want %d", status, http.StatusOK)
	}
	if got := commandBody["mode"]; got != "async" {
		t.Fatalf("mode = %#v, want async", got)
	}
	inputs := commandBody["input_params"].([]map[string]interface{})
	if len(inputs) != 1 || inputs[0]["identifier"] != "package_url" {
		t.Fatalf("unexpected input params: %#v", inputs)
	}

	inputBody, status := buildCommandInputQuery(sdk, registry)("qhl0001", "program_install")
	if status != http.StatusOK {
		t.Fatalf("command input status = %d, want %d", status, http.StatusOK)
	}
	inputs = inputBody["input_params"].([]map[string]interface{})
	if len(inputs) != 1 || inputs[0]["identifier"] != "package_url" {
		t.Fatalf("unexpected input params: %#v", inputs)
	}

	outputBody, status := buildCommandOutputQuery(sdk, registry)("qhl0001", "program_install")
	if status != http.StatusOK {
		t.Fatalf("command output status = %d, want %d", status, http.StatusOK)
	}
	outputs := outputBody["output_params"].([]map[string]interface{})
	if len(outputs) != 1 || outputs[0]["identifier"] != "program_install_done" {
		t.Fatalf("unexpected output params: %#v", outputs)
	}

	_, status = buildCommandDetailQuery(sdk, registry)("qhl0001", "hidden_command")
	if status != http.StatusNotFound {
		t.Fatalf("hidden command status = %d, want %d", status, http.StatusNotFound)
	}
}

func TestBuildControlJobQueries(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	if _, err := store.UpsertJob(rtcontrol.JobState{
		TraceID:     "trace-q-1",
		DeviceCode:  "qhl0001",
		ProductCode: "qhl",
		Kind:        "command",
		Identifier:  "program_install",
		Code:        ctl.CodeAccepted,
		Message:     "accepted",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("UpsertJob() error = %v", err)
	}

	jobBody, status := buildControlJobQuery(store)("trace-q-1")
	if status != http.StatusOK {
		t.Fatalf("job status = %d, want %d", status, http.StatusOK)
	}
	if got := jobBody["code"]; got != ctl.CodeAccepted {
		t.Fatalf("job code = %#v, want %d", got, ctl.CodeAccepted)
	}
	if got := jobBody["final"]; got != false {
		t.Fatalf("job final = %#v, want false", got)
	}
	if got := jobBody["status_phase"]; got != "accepted" {
		t.Fatalf("job status_phase = %#v, want accepted", got)
	}

	result := ctl.Result{
		TraceID: "trace-q-1",
		Code:    ctl.CodeSuccess,
		Message: "success",
		Data: map[string]interface{}{
			"program_install_done": true,
		},
		Time: now + 100,
	}
	if err := store.SaveResult(result.TraceID, result, true); err != nil {
		t.Fatalf("SaveResult() error = %v", err)
	}

	resultBody, status := buildControlJobResultQuery(store)("trace-q-1")
	if status != http.StatusOK {
		t.Fatalf("result status = %d, want %d", status, http.StatusOK)
	}
	if got := resultBody["code"]; got != ctl.CodeSuccess {
		t.Fatalf("result code = %#v, want %d", got, ctl.CodeSuccess)
	}
	if got := resultBody["final"]; got != true {
		t.Fatalf("result final = %#v, want true", got)
	}
	eventsBody, status := buildControlJobEventsQuery(store)("trace-q-1", 10)
	if status != http.StatusOK {
		t.Fatalf("events status = %d, want %d", status, http.StatusOK)
	}
	events := eventsBody["events"].([]map[string]interface{})
	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1", len(events))
	}
	if got := events[0]["status_phase"]; got != "success" {
		t.Fatalf("events[0].status_phase = %#v, want success", got)
	}
	if got := eventsBody["event_count"]; got != 1 {
		t.Fatalf("event_count = %#v, want 1", got)
	}
	jobBody, status = buildControlJobQuery(store)("trace-q-1")
	if status != http.StatusOK {
		t.Fatalf("job status = %d, want %d", status, http.StatusOK)
	}
	if got := jobBody["event_count"]; got != 1 {
		t.Fatalf("job event_count = %#v, want 1", got)
	}
	if got := jobBody["duration_ms"]; got == nil {
		t.Fatalf("job duration_ms = nil, want number")
	}
	if got := jobBody["total_duration_ms"]; got == nil {
		t.Fatalf("job total_duration_ms = nil, want number")
	}
}

func TestBuildTypedControlResultQueryReturnsStandardResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "typed-result.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	if _, err := store.UpsertJob(rtcontrol.JobState{
		TraceID:     "trace-prop-result-1",
		DeviceCode:  "qhl0001",
		ProductCode: "qhl",
		Kind:        "property:get",
		Code:        ctl.CodeSuccess,
		Message:     "success",
		CreatedAt:   now,
		UpdatedAt:   now,
		FinishedAt:  now,
	}); err != nil {
		t.Fatalf("UpsertJob property error = %v", err)
	}
	if err := store.SaveResult("trace-prop-result-1", ctl.Result{TraceID: "trace-prop-result-1", Code: ctl.CodeSuccess, Message: "success", Data: map[string]interface{}{"temp": 25.5}, Time: now}, true); err != nil {
		t.Fatalf("SaveResult property error = %v", err)
	}
	body, status := buildPropertyResultQuery(store)("trace-prop-result-1")
	if status != http.StatusOK {
		t.Fatalf("property result status = %d, want %d", status, http.StatusOK)
	}
	if got := body["trace_id"]; got != "trace-prop-result-1" {
		t.Fatalf("trace_id = %#v, want trace-prop-result-1", got)
	}
	if got := body["code"]; got != ctl.CodeSuccess {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}

	if _, err := store.UpsertJob(rtcontrol.JobState{
		TraceID:     "trace-cmd-result-1",
		DeviceCode:  "qhl0001",
		ProductCode: "qhl",
		Kind:        "command:program_install",
		Identifier:  "program_install",
		Code:        ctl.CodeAccepted,
		Message:     "accepted",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("UpsertJob command error = %v", err)
	}
	cmdBody, status := buildCommandResultQuery(store)("trace-cmd-result-1")
	if status != http.StatusOK {
		t.Fatalf("command result status = %d, want %d", status, http.StatusOK)
	}
	if got := cmdBody["code"]; got != ctl.CodeAccepted {
		t.Fatalf("command code = %#v, want %d", got, ctl.CodeAccepted)
	}
}

var _ httpserver.ControlJobListQuery
