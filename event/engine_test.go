package event

import (
	"encoding/json"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestEventPublicEnvelopeOmitsSourceAndKeepsTraceMetadata(t *testing.T) {
	event := Event{
		Time:        1000,
		DeviceCode:  "D1",
		ProductCode: "P1",
		TraceID:     "trace-1",
		Data: EventData{
			EventCode:       "EVENT_TEST",
			Category:        CategoryOEE,
			Type:            EventTypePulse,
			Payload:         map[string]interface{}{"x": 1},
			EventInstanceID: "evt-1",
		},
	}
	body, err := event.MarshalPublicJSON(false, 2000)
	if err != nil {
		t.Fatalf("MarshalPublicJSON() error = %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded["device_code"] != "D1" || decoded["trace_id"] != "trace-1" || decoded["send_at"] != float64(2000) {
		t.Fatalf("unexpected envelope: %#v", decoded)
	}
	if _, ok := decoded["source"]; ok {
		t.Fatal("event envelope must not contain source")
	}
	if _, ok := decoded["product_code"]; ok {
		t.Fatal("transport product code must not be exposed")
	}
}

func TestExpressionSupportsNamespacesAndUnknownValues(t *testing.T) {
	expression, err := ParseExpression("connection.online == true && data.mode == 'run' && LAST_VALUE.count != 0")
	if err != nil {
		t.Fatalf("ParseExpression() error = %v", err)
	}
	if got := expression.References(); len(got) != 3 {
		t.Fatalf("expected 3 references, got %#v", got)
	}
	value, known := expression.Evaluate(EvalContext{
		Data:       map[string]interface{}{"mode": "run"},
		LastValue:  map[string]interface{}{"count": 1},
		Connection: map[string]interface{}{"online": true},
	})
	if !known || !value {
		t.Fatalf("expected known true result, got value=%t known=%t", value, known)
	}
	_, known = expression.Evaluate(EvalContext{Data: map[string]interface{}{"mode": "run"}, Connection: map[string]interface{}{"online": true}})
	if known {
		t.Fatal("expected missing LAST_VALUE to remain unknown")
	}
	collection, known := mustExpression(t, "data.alarm_code != 0").Evaluate(EvalContext{Data: map[string]interface{}{"alarm_code": []uint16{0, 12}}})
	if !known || !collection {
		t.Fatal("expected a mixed alarm-code collection to be active")
	}
}

func mustExpression(t *testing.T, raw string) *Expression {
	t.Helper()
	expression, err := ParseExpression(raw)
	if err != nil {
		t.Fatalf("ParseExpression(%q) error = %v", raw, err)
	}
	return expression
}

func TestEngineGeneratesConnectOEEAndAggregatedAlarmEvents(t *testing.T) {
	profile := EventProfileFile{
		Name: "test-events",
		Type: "EVENT",
		Config: EventConfig{
			Initialization: InitializationConfig{EmitInitialState: true},
			Categories: map[string]CategoryConfig{
				"oee": {
					StateModel: "exclusive", ExclusiveGroup: "oee_state", AllowMultipleActive: boolPtr(false), TransitionOrder: "clear-then-raise", Priority: []string{"running", "idle", "unknown"},
					Events: []EventRule{
						{EventCode: "EVENT_OEE_RUNNING", Name: "running", EventType: EventTypeRiseClear, State: "running", When: "connection.online == true && data.mode == 'run'", Recover: "connection.online == false || data.mode != 'run'", Payload: PayloadSelector{Groups: []string{"state"}}},
						{EventCode: "EVENT_OEE_IDLE", Name: "idle", EventType: EventTypeRiseClear, State: "idle", When: "connection.online == true && data.mode == 'idle'", Recover: "connection.online == false || data.mode != 'idle'", Payload: PayloadSelector{Groups: []string{"state"}}},
						{EventCode: "EVENT_OEE_UNKNOWN", Name: "unknown", EventType: EventTypeRiseClear, State: "unknown", Fallback: true, Payload: PayloadSelector{Groups: []string{"state"}}},
					},
				},
				"alarm": {
					StateModel: "aggregate",
					Events: []EventRule{{
						EventCode: "EVENT_TOTAL_ALARM", Name: "total alarm", EventType: EventTypeRiseClear,
						When: "data.alarm_code != 0", Recover: "data.alarm_code == 0",
						Aggregate: &AggregateConfig{
							Mode: "total", CodeField: "data.alarm_code", ZeroValue: 0,
							Code: AggregateCodeConfig{Prefix: "ALARM_"}, CodeSetChange: "update", UpdateType: EventTypePulse, KeepEventInstanceID: true,
							Generated: AggregateGenerated{Codes: "alarm_codes", Text: "alarm_codes_text", Count: "alarm_count", Active: "active", Delimiter: "|"},
						},
						Payload: PayloadSelector{Groups: []string{"alarm"}},
					}},
				},
			},
		},
	}
	device := testEventDevice()
	engine, err := NewEngine(EngineOptions{Bindings: []DeviceBinding{{Device: device, Profile: profile}}})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	connected := mustConnectionEvents(t, engine, device.Name, 1000, true)
	if !containsEvent(connected, "EVENT_CONNECT_ONLINE", EventActionRaise) || !containsEvent(connected, "EVENT_OEE_UNKNOWN", EventActionRaise) {
		t.Fatalf("expected online and initial unknown events, got %#v", eventSummaries(connected))
	}
	running, err := engine.ObserveTelemetry(device.Name, 2000, []*contracts.CommandValue{value("mode", "String", "run"), value("alarm_code", "Uint16", uint16(12))})
	if err != nil {
		t.Fatalf("ObserveTelemetry() error = %v", err)
	}
	if !containsEvent(running, "EVENT_OEE_UNKNOWN", EventActionClear) || !containsEvent(running, "EVENT_OEE_RUNNING", EventActionRaise) || !containsEvent(running, "EVENT_TOTAL_ALARM", EventActionRaise) {
		t.Fatalf("expected OEE transition and alarm raise, got %#v", eventSummaries(running))
	}
	alarmRaise := findEvent(running, "EVENT_TOTAL_ALARM", EventActionRaise)
	if alarmRaise == nil || alarmRaise.Data.Payload["alarm_codes_text"] != "ALARM_12" {
		t.Fatalf("unexpected aggregate payload: %#v", alarmRaise)
	}
	update, err := engine.ObserveTelemetry(device.Name, 3000, []*contracts.CommandValue{value("alarm_code", "Uint16", uint16(13))})
	if err != nil {
		t.Fatalf("ObserveTelemetry() update error = %v", err)
	}
	alarmUpdate := findEvent(update, "EVENT_TOTAL_ALARM", EventTypePulse)
	if alarmUpdate == nil || alarmUpdate.Data.EventInstanceID != alarmRaise.Data.EventInstanceID {
		t.Fatalf("expected aggregate update to reuse instance, raise=%#v update=%#v", alarmRaise, alarmUpdate)
	}
	idle, err := engine.ObserveTelemetry(device.Name, 4000, []*contracts.CommandValue{value("mode", "String", "idle")})
	if err != nil {
		t.Fatalf("ObserveTelemetry() idle error = %v", err)
	}
	if !containsEvent(idle, "EVENT_OEE_RUNNING", EventActionClear) || !containsEvent(idle, "EVENT_OEE_IDLE", EventActionRaise) {
		t.Fatalf("expected mutually exclusive OEE transition, got %#v", eventSummaries(idle))
	}
	offline := mustConnectionEvents(t, engine, device.Name, 5000, false)
	if !containsEvent(offline, "EVENT_CONNECT_ONLINE", EventActionClear) || !containsEvent(offline, "EVENT_CONNECT_OFFLINE", EventActionRaise) || !containsEvent(offline, "EVENT_OEE_IDLE", EventActionClear) || !containsEvent(offline, "EVENT_OEE_UNKNOWN", EventActionRaise) {
		t.Fatalf("expected offline and OEE unknown transition, got %#v", eventSummaries(offline))
	}
	if containsEvent(offline, "EVENT_TOTAL_ALARM", EventActionClear) {
		t.Fatal("connection loss must not clear alarm without an alarm observation")
	}
}

func TestEngineSummarySplitsRunningAndIdleDurations(t *testing.T) {
	profile := EventProfileFile{
		Name: "summary-events",
		Type: "EVENT",
		Config: EventConfig{
			Time:           TimeConfig{Timezone: "Asia/Shanghai", BusinessDayStart: "08:00:00"},
			Initialization: InitializationConfig{EmitInitialState: true},
			Categories: map[string]CategoryConfig{
				"oee": {
					StateModel: "exclusive", ExclusiveGroup: "oee", AllowMultipleActive: boolPtr(false), TransitionOrder: "clear-then-raise", Priority: []string{"running", "idle"},
					Events: []EventRule{
						{EventCode: "RUN", Name: "running", EventType: EventTypeRiseClear, State: "running", When: "connection.online == true && data.mode == 'run'", Recover: "data.mode != 'run'", Payload: PayloadSelector{Groups: []string{"state"}}, Report: ReportConfig{Mode: ReportModeSummary, Interval: "1h", Window: "1h", Align: "business_day"}},
						{EventCode: "IDLE", Name: "idle", EventType: EventTypeRiseClear, State: "idle", When: "connection.online == true && data.mode == 'idle'", Recover: "data.mode != 'idle'", Payload: PayloadSelector{Groups: []string{"state"}}, Report: ReportConfig{Mode: ReportModeSummary, Interval: "1h", Window: "1h", Align: "business_day"}},
					},
				},
			},
		},
	}
	device := testEventDevice()
	engine, err := NewEngine(EngineOptions{Bindings: []DeviceBinding{{Device: device, Profile: profile}}})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start := time.Date(2026, 8, 15, 8, 0, 0, 0, loc).UnixMilli()
	_, _ = engine.ObserveConnection(ConnectionObservation{DeviceCode: device.Name, Online: true, State: "connected", ObservedAt: start, Known: true})
	_, _ = engine.ObserveTelemetry(device.Name, start, []*contracts.CommandValue{value("mode", "String", "run")})
	_, _ = engine.ObserveTelemetry(device.Name, start+20*time.Minute.Milliseconds(), []*contracts.CommandValue{value("mode", "String", "idle")})
	_, _ = engine.ObserveTelemetry(device.Name, start+35*time.Minute.Milliseconds(), []*contracts.CommandValue{value("mode", "String", "run")})
	items := engine.Flush(start + time.Hour.Milliseconds())
	run := findEvent(items, "RUN", EventTypePulse)
	idle := findEvent(items, "IDLE", EventTypePulse)
	if run == nil || idle == nil {
		t.Fatalf("expected two summary events, got %#v", eventSummaries(items))
	}
	if run.Data.Payload["duration_ms"] != int64(45*time.Minute/time.Millisecond) || idle.Data.Payload["duration_ms"] != int64(15*time.Minute/time.Millisecond) {
		t.Fatalf("unexpected summary durations: run=%#v idle=%#v", run.Data.Payload, idle.Data.Payload)
	}
}

func TestEngineUsesExplicitRecoveryExpression(t *testing.T) {
	profile := EventProfileFile{
		Name: "recovery-events",
		Type: "EVENT",
		Config: EventConfig{Initialization: InitializationConfig{EmitInitialState: true}, Categories: map[string]CategoryConfig{
			"alarm": {Events: []EventRule{{
				EventCode: "RECOVERY_TEST", Name: "recovery", EventType: EventTypeRiseClear,
				When: "data.trigger == 1", Recover: "data.reset == true",
			}}},
		}},
	}
	engine, err := NewEngine(EngineOptions{Bindings: []DeviceBinding{{Device: testEventDevice(), Profile: profile}}})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	raised, err := engine.ObserveTelemetry("D1", 1000, []*contracts.CommandValue{value("trigger", "Uint16", uint16(1))})
	if err != nil || !containsEvent(raised, "RECOVERY_TEST", EventActionRaise) {
		t.Fatalf("expected raise event, events=%#v err=%v", eventSummaries(raised), err)
	}
	notRecovered, err := engine.ObserveTelemetry("D1", 2000, []*contracts.CommandValue{value("trigger", "Uint16", uint16(0))})
	if err != nil {
		t.Fatalf("ObserveTelemetry() not recovered error = %v", err)
	}
	if containsEvent(notRecovered, "RECOVERY_TEST", EventActionClear) {
		t.Fatal("when=false must not clear before explicit recovery expression")
	}
	cleared, err := engine.ObserveTelemetry("D1", 3000, []*contracts.CommandValue{value("reset", "Bool", true)})
	if err != nil || !containsEvent(cleared, "RECOVERY_TEST", EventActionClear) {
		t.Fatalf("expected explicit recovery clear, events=%#v err=%v", eventSummaries(cleared), err)
	}
}

func testEventDevice() contracts.DeviceConfig {
	return contracts.DeviceConfig{Name: "D1", InternalName: "D1", ProductCode: "P1", Telemetry: contracts.TelemetryConfig{Groups: []contracts.TelemetryGroup{{Name: "state", Points: []contracts.PointConfig{{Name: "mode", ValueType: "String"}}}, {Name: "alarm", Points: []contracts.PointConfig{{Name: "alarm_code", ValueType: "Uint16"}}}}}}
}

func value(name, valueType string, value interface{}) *contracts.CommandValue {
	return &contracts.CommandValue{DeviceResourceName: name, Type: valueType, Value: value}
}

func boolPtr(value bool) *bool { return &value }

func mustConnectionEvents(t *testing.T, engine *Engine, device string, timestamp int64, online bool) []Event {
	t.Helper()
	items, err := engine.ObserveConnection(ConnectionObservation{DeviceCode: device, Online: online, State: "connected", ObservedAt: timestamp, Known: true})
	if err != nil {
		t.Fatalf("ObserveConnection() error = %v", err)
	}
	return items
}

func containsEvent(items []Event, code, eventType string) bool {
	return findEvent(items, code, eventType) != nil
}

func findEvent(items []Event, code, eventType string) *Event {
	for i := range items {
		if items[i].Data.EventCode == code && items[i].Data.Type == eventType {
			return &items[i]
		}
	}
	return nil
}

func eventSummaries(items []Event) []string {
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, item.Data.EventCode+":"+item.Data.Type)
	}
	return result
}
