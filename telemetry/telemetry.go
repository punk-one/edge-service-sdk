package telemetry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

// TelemetryEvent is the normalized internal event passed to outbound transports.
type TelemetryEvent struct {
	TraceID     string                    `json:"traceId"`
	DeviceName  string                    `json:"deviceName"`
	ProductCode string                    `json:"productCode"`
	SourceName  string                    `json:"sourceName"`
	CollectedAt int64                     `json:"collectedAt"`
	Values      map[string]TelemetryValue `json:"values"`
}

// TelemetryValue preserves a point's typed value for replay.
type TelemetryValue struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value"`
	Origin int64           `json:"origin"`
}

// NewTelemetryEvent converts async command values to the normalized outbound event.
func NewTelemetryEvent(device contracts.DeviceConfig, async *contracts.AsyncValues) (TelemetryEvent, error) {
	if async == nil {
		return TelemetryEvent{}, fmt.Errorf("async values cannot be nil")
	}

	traceID := strings.TrimSpace(async.TraceID)
	if traceID == "" {
		traceID = NewTraceID(device.Name)
	}

	collectedAt := async.CollectedAt
	if collectedAt == 0 {
		collectedAt = time.Now().UnixMilli()
	}

	values := make(map[string]TelemetryValue, len(async.Values))
	for _, value := range async.Values {
		if value == nil {
			continue
		}

		raw, err := json.Marshal(value.Value)
		if err != nil {
			return TelemetryEvent{}, fmt.Errorf("marshal point %s: %w", value.DeviceResourceName, err)
		}

		values[value.DeviceResourceName] = TelemetryValue{
			Type:   value.Type,
			Value:  raw,
			Origin: value.Origin,
		}
	}

	return TelemetryEvent{
		TraceID:     traceID,
		DeviceName:  device.Name,
		ProductCode: device.ProductCode,
		SourceName:  async.SourceName,
		CollectedAt: collectedAt,
		Values:      values,
	}, nil
}

// DataMap converts the event into the map structure expected by outbound serializers.
func (e TelemetryEvent) DataMap() (map[string]interface{}, error) {
	data := make(map[string]interface{}, len(e.Values))
	for name, point := range e.Values {
		var decoded interface{}
		if len(point.Value) > 0 {
			if err := json.Unmarshal(point.Value, &decoded); err != nil {
				return nil, fmt.Errorf("unmarshal point %s: %w", name, err)
			}
		}

		data[name] = map[string]interface{}{
			"value":  decoded,
			"type":   point.Type,
			"origin": point.Origin,
		}
	}
	return data, nil
}

// CommandValues reconstructs contracts.CommandValue values from the event.
func (e TelemetryEvent) CommandValues() ([]*contracts.CommandValue, error) {
	values := make([]*contracts.CommandValue, 0, len(e.Values))
	for name, point := range e.Values {
		var decoded interface{}
		if len(point.Value) > 0 {
			if err := json.Unmarshal(point.Value, &decoded); err != nil {
				return nil, fmt.Errorf("unmarshal point %s: %w", name, err)
			}
		}

		values = append(values, &contracts.CommandValue{
			DeviceResourceName: name,
			Type:               point.Type,
			Value:              decoded,
			Origin:             point.Origin,
		})
	}
	return values, nil
}

// NewTraceID generates a lightweight trace identifier for telemetry delivery.
func NewTraceID(deviceName string) string {
	deviceName = strings.TrimSpace(deviceName)
	if deviceName == "" {
		deviceName = "telemetry"
	}
	return fmt.Sprintf("%s-%d", deviceName, time.Now().UnixNano())
}
