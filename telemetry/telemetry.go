package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	contracts "github.com/punk-one/edge-service-sdk/driver"
)

const MaxDecimalPrecision = 18

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

		raw, err := marshalPointValue(device, value)
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
			value, err := decodeJSONValue(point.Value)
			if err != nil {
				return nil, fmt.Errorf("unmarshal point %s: %w", name, err)
			}
			decoded = value
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
			value, err := decodeCommandValue(point.Type, point.Value)
			if err != nil {
				return nil, fmt.Errorf("unmarshal point %s: %w", name, err)
			}
			decoded = value
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

func marshalPointValue(device contracts.DeviceConfig, value *contracts.CommandValue) ([]byte, error) {
	point, configured := findPointConfig(device, value.DeviceResourceName)
	if !configured || point.Precision <= 0 {
		return json.Marshal(value.Value)
	}
	if point.Precision > MaxDecimalPrecision {
		return nil, fmt.Errorf("precision %d exceeds maximum %d", point.Precision, MaxDecimalPrecision)
	}

	valueType := contracts.NormalizedValueType(value.Type)
	if valueType != contracts.ValueTypeFloat32 && valueType != contracts.ValueTypeFloat64 {
		return json.Marshal(value.Value)
	}

	floatValue, bitSize, err := telemetryFloat(value.Value, valueType)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
		return nil, fmt.Errorf("%s value must be finite", valueType)
	}

	// FormatFloat produces a JSON number with exactly the configured decimal
	// places. Keeping the number as RawMessage avoids a float64 conversion before
	// the SQLite durability boundary.
	return []byte(strconv.FormatFloat(floatValue, 'f', point.Precision, bitSize)), nil
}

func telemetryFloat(value interface{}, valueType string) (float64, int, error) {
	var converted float64
	switch typed := value.(type) {
	case float32:
		converted = float64(typed)
	case float64:
		converted = typed
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, 0, fmt.Errorf("parse %s value: %w", valueType, err)
		}
		converted = parsed
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return 0, 0, fmt.Errorf("encode %s value: %w", valueType, err)
		}
		if err := json.Unmarshal(raw, &converted); err != nil {
			return 0, 0, fmt.Errorf("convert %T to %s: %w", value, valueType, err)
		}
	}

	if valueType == contracts.ValueTypeFloat32 {
		converted = float64(float32(converted))
		if math.IsInf(converted, 0) {
			return 0, 0, fmt.Errorf("value %v exceeds Float32 range", value)
		}
		return converted, 32, nil
	}
	return converted, 64, nil
}

func findPointConfig(device contracts.DeviceConfig, name string) (contracts.PointConfig, bool) {
	for _, point := range device.Telemetry.Points {
		if pointConfigMatches(point, name) {
			return point, true
		}
	}
	for _, group := range device.Telemetry.Groups {
		for _, point := range group.Points {
			if pointConfigMatches(point, name) {
				return point, true
			}
		}
	}
	for _, point := range device.Property.Points {
		if pointConfigMatches(point, name) {
			return point, true
		}
	}
	return contracts.PointConfig{}, false
}

func pointConfigMatches(point contracts.PointConfig, name string) bool {
	if point.Name == name {
		return true
	}
	if point.NodeNameTemplate == "" {
		return false
	}

	pattern := point.ArrayKeyPattern
	if pattern == "" {
		pattern = point.Name + "[{index}]"
	}
	prefix, suffix, found := strings.Cut(pattern, "{index}")
	if !found || strings.Contains(suffix, "{index}") ||
		!strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	indexText := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if indexText == "" {
		return false
	}
	for _, char := range indexText {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func decodeJSONValue(raw json.RawMessage) (interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func decodeCommandValue(valueType string, raw json.RawMessage) (interface{}, error) {
	valueType = contracts.NormalizedValueType(valueType)
	switch valueType {
	case contracts.ValueTypeBool:
		var value bool
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeString:
		var value string
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeUint8:
		var value uint8
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeUint16:
		var value uint16
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeUint32:
		var value uint32
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeUint64:
		var value uint64
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeInt8:
		var value int8
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeInt16:
		var value int16
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeInt32:
		var value int32
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeInt64:
		var value int64
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeFloat32:
		var value float32
		return value, json.Unmarshal(raw, &value)
	case contracts.ValueTypeFloat64:
		var value float64
		return value, json.Unmarshal(raw, &value)
	default:
		return decodeJSONValue(raw)
	}
}

// NewTraceID generates a lightweight trace identifier for telemetry delivery.
func NewTraceID(deviceName string) string {
	deviceName = strings.TrimSpace(deviceName)
	if deviceName == "" {
		deviceName = "telemetry"
	}
	return fmt.Sprintf("%s-%s", deviceName, uuid.NewString())
}
