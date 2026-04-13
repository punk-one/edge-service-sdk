package app

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type mqttQueryTestTransport struct {
	subscriptions map[string]mqtt.MessageHandler
	published     []mqttQueryPublished
}

type mqttQueryPublished struct {
	topic   string
	qos     byte
	retain  bool
	payload ctl.Result
}

func (t *mqttQueryTestTransport) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	if t.subscriptions == nil {
		t.subscriptions = map[string]mqtt.MessageHandler{}
	}
	t.subscriptions[topic] = handler
	return nil
}

func (t *mqttQueryTestTransport) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	result, ok := payload.(ctl.Result)
	if !ok {
		body, _ := json.Marshal(payload)
		_ = json.Unmarshal(body, &result)
	}
	t.published = append(t.published, mqttQueryPublished{topic: topic, qos: qos, retain: retain, payload: result})
	return nil
}

func TestMQTTQueryServicePublishesModelQueryResult(t *testing.T) {
	onChange := true
	registry := cmdapi.NewRegistry()
	registry.MustRegister(queryTestCommand{desc: cmdapi.CommandDescriptor{
		Identifier: "program_install",
		Name:       "Program Install",
		Mode:       "async",
	}})
	sdk := NewDeviceSDK(rtconfig.Config{Devices: []contracts.DeviceConfig{{
		Name:        "qhl0001",
		ProductCode: "qhl",
		Telemetry:   contracts.TelemetryConfig{Points: []contracts.PointConfig{{Name: "program_fault", ValueType: "Bool", NodeName: "MX10.3"}}},
		Property:    contracts.PropertyConfig{Points: []contracts.PointConfig{{Name: "program_install_trigger", ValueType: "Bool", NodeName: "MX20.0", ReadWrite: "RW", OnChange: &onChange}}},
		Commands:    []contracts.CommandConfig{{Identifier: "program_install"}},
	}}}, nil, nil)
	transport := &mqttQueryTestTransport{}
	service := newMQTTQueryService(sdk, registry, nil, transport, nil)
	service.RegisterMQTTHandlers(rtconfig.Config{
		Devices:      []contracts.DeviceConfig{{Name: "qhl0001", ProductCode: "qhl"}},
		QueryRequest: mqtt.TopicConfig{Topic: "v1/gateway/{productCode}/query/request", QoS: 1},
		QueryResult:  mqtt.TopicConfig{Topic: "v1/gateway/{productCode}/query/result", QoS: 1},
	})

	handler := transport.subscriptions["v1/gateway/qhl/query/request"]
	if handler == nil {
		t.Fatalf("expected query request subscription")
	}
	body, _ := json.Marshal(ctl.Request{
		TraceID:    "trace-query-model",
		DeviceCode: "qhl0001",
		Time:       time.Now().UnixMilli(),
		Data: map[string]interface{}{
			"target": "model.commands",
		},
	})
	handler("v1/gateway/qhl/query/request", body)
	if len(transport.published) != 1 {
		t.Fatalf("published len = %d, want 1", len(transport.published))
	}
	published := transport.published[0]
	if published.topic != "v1/gateway/qhl/query/result" {
		t.Fatalf("topic = %s, want v1/gateway/qhl/query/result", published.topic)
	}
	if published.payload.Code != ctl.CodeSuccess {
		t.Fatalf("code = %d, want %d", published.payload.Code, ctl.CodeSuccess)
	}
	commands, ok := published.payload.Data["commands"].([]map[string]interface{})
	if !ok || len(commands) != 1 {
		t.Fatalf("commands = %#v, want one command", published.payload.Data["commands"])
	}
	if commands[0]["identifier"] != "program_install" {
		t.Fatalf("identifier = %#v, want program_install", commands[0]["identifier"])
	}
}

func TestMQTTQueryServicePublishesControlJobResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer store.Close()
	now := time.Now().UnixMilli()
	if _, err := store.UpsertJob(rtcontrol.JobState{TraceID: "job-trace-1", DeviceCode: "qhl0001", ProductCode: "qhl", Kind: "command", Identifier: "program_install", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("UpsertJob() error = %v", err)
	}
	if err := store.SaveResult("job-trace-1", ctl.Result{TraceID: "job-trace-1", Code: ctl.CodeSuccess, Message: "success", Data: map[string]interface{}{"program_install_done": true}, Time: now + 1}, true); err != nil {
		t.Fatalf("SaveResult() error = %v", err)
	}
	sdk := NewDeviceSDK(rtconfig.Config{Devices: []contracts.DeviceConfig{{Name: "qhl0001", ProductCode: "qhl"}}}, nil, nil)
	transport := &mqttQueryTestTransport{}
	service := newMQTTQueryService(sdk, cmdapi.NewRegistry(), store, transport, nil)
	result := service.executeQuery("qhl", ctl.Request{
		TraceID:    "trace-query-job-result",
		DeviceCode: "qhl0001",
		Data: map[string]interface{}{
			"target":       "control.job.result",
			"job_trace_id": "job-trace-1",
		},
	})
	if result.Code != ctl.CodeSuccess {
		t.Fatalf("code = %d, want %d", result.Code, ctl.CodeSuccess)
	}
	if got := result.Data["trace_id"]; got != "job-trace-1" {
		t.Fatalf("trace_id = %#v, want job-trace-1", got)
	}
	if got := result.Data["code"]; got != ctl.CodeSuccess {
		t.Fatalf("result code = %#v, want %d", got, ctl.CodeSuccess)
	}
}

func TestMQTTQueryServiceRejectsUnknownTarget(t *testing.T) {
	service := newMQTTQueryService(nil, cmdapi.NewRegistry(), nil, &mqttQueryTestTransport{}, nil)
	result := service.executeQuery("qhl", ctl.Request{
		TraceID: "trace-query-invalid",
		Data: map[string]interface{}{
			"target": "unknown.target",
		},
	})
	if result.Code != ctl.CodeBadRequest {
		t.Fatalf("code = %d, want %d", result.Code, ctl.CodeBadRequest)
	}
	if result.Message != "unsupported data.target" {
		t.Fatalf("message = %q, want unsupported data.target", result.Message)
	}
}
