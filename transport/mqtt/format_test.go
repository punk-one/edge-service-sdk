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

	body, err := publisher.formatTelemetry(event, map[string]interface{}{
		"temperature": map[string]interface{}{
			"value":  36.5,
			"type":   "Float32",
			"origin": int64(1710000000000),
		},
	}, true)
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
	if _, ok := payload["sendAt"]; !ok {
		t.Fatal("expected sendAt field")
	}
	if _, ok := payload["collectedAt"]; ok {
		t.Fatal("did not expect collectedAt field in rule payload")
	}
}
