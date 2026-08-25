package mqtt

import (
	"encoding/json"
	"testing"

	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

func TestRuleFormatUsesTimeAsCollectedAtAndAddsSendAt(t *testing.T) {
	publisher := &MQTTPublisher{
		telemetry: TopicConfig{DataFormat: "rule"},
	}

	event := outevent.TelemetryEvent{
		TraceID:     "trace-1",
		DeviceName:  "acm006",
		ProductCode: "acm",
		CollectedAt: 1710000000000,
	}

	const sendAt int64 = 1710000001234
	body, err := publisher.formatTelemetryAt(event, map[string]interface{}{
		"temperature": map[string]interface{}{
			"value":  36.5,
			"type":   "Float32",
			"origin": int64(1710000000000),
		},
	}, true, sendAt)
	if err != nil {
		t.Fatalf("formatTelemetry() error = %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if got := int64(payload["time"].(float64)); got != event.CollectedAt {
		t.Fatalf("time = %d, want %d", got, event.CollectedAt)
	}
	if got, ok := payload["trace_id"].(string); !ok || got != event.TraceID {
		t.Fatalf("trace_id = %#v, want %q", payload["trace_id"], event.TraceID)
	}
	if got := int64(payload["send_at"].(float64)); got != sendAt {
		t.Fatalf("send_at = %d, want %d", got, sendAt)
	}
	if got, ok := payload["is_replayed"].(bool); !ok || !got {
		t.Fatalf("is_replayed = %#v, want true", payload["is_replayed"])
	}
	if got, ok := payload["device_code"].(string); !ok || got != event.DeviceName {
		t.Fatalf("device_code = %#v, want %q", payload["device_code"], event.DeviceName)
	}
	if _, ok := payload["traceId"]; ok {
		t.Fatal("did not expect traceId field in rule payload")
	}
	if _, ok := payload["sendAt"]; ok {
		t.Fatal("did not expect sendAt field in rule payload")
	}
	if _, ok := payload["isReplayed"]; ok {
		t.Fatal("did not expect isReplayed field in rule payload")
	}
	if _, ok := payload["collectedAt"]; ok {
		t.Fatal("did not expect collectedAt field in rule payload")
	}
}

func TestCompactFormatIncludesReplayMetadata(t *testing.T) {
	publisher := &MQTTPublisher{telemetry: TopicConfig{DataFormat: "compact"}}
	event := outevent.TelemetryEvent{
		TraceID:     "trace-compact",
		DeviceName:  "device-01",
		CollectedAt: 1710000000000,
	}
	const sendAt int64 = 1710000001234
	body, err := publisher.formatTelemetryAt(event, map[string]interface{}{"count": 3}, true, sendAt)
	if err != nil {
		t.Fatalf("formatTelemetryAt() error = %v", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if replayed, ok := payload["is_replayed"].(bool); !ok || !replayed {
		t.Fatalf("is_replayed = %#v, want true", payload["is_replayed"])
	}
	if got := int64(payload["send_at"].(float64)); got != sendAt {
		t.Fatalf("send_at = %d, want %d", got, sendAt)
	}
}
