package mqtt

import (
	"encoding/json"
	"strings"
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

func TestTelemetryFormatsPreserveNumericJSONText(t *testing.T) {
	event := outevent.TelemetryEvent{
		TraceID:     "trace-numeric",
		DeviceName:  "device-01",
		CollectedAt: 1710000000000,
		Values: map[string]outevent.TelemetryValue{
			"temperature": {
				Type: "Float32", Value: json.RawMessage("12.30"), Origin: 1710000000000,
			},
			"counter": {
				Type: "Uint64", Value: json.RawMessage("18446744073709551615"), Origin: 1710000000000,
			},
		},
	}
	data, err := event.DataMap()
	if err != nil {
		t.Fatalf("DataMap() error = %v", err)
	}

	for _, format := range []string{"rule", "raw", "compact", "telemetry", "influx"} {
		t.Run(format, func(t *testing.T) {
			publisher := &MQTTPublisher{telemetry: TopicConfig{DataFormat: format}}
			body, err := publisher.formatTelemetryAt(event, data, true, 1710000001234)
			if err != nil {
				t.Fatalf("formatTelemetryAt() error = %v", err)
			}
			payload := string(body)
			if !strings.Contains(payload, "12.30") {
				t.Fatalf("payload lost configured decimal text: %s", payload)
			}
			if !strings.Contains(payload, "18446744073709551615") {
				t.Fatalf("payload lost uint64 precision: %s", payload)
			}
		})
	}
}
