package telemetry

import (
	"encoding/json"
	"math"
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestNewTelemetryEventAppliesPointPrecisionBeforePersistence(t *testing.T) {
	device := contracts.DeviceConfig{
		Name: "device-01",
		Telemetry: contracts.TelemetryConfig{
			Groups: []contracts.TelemetryGroup{{
				Points: []contracts.PointConfig{
					{Name: "temperature", ValueType: contracts.ValueTypeFloat32, Precision: 2},
					{Name: "pressure", ValueType: contracts.ValueTypeFloat64, Precision: 3},
					{Name: "level", ValueType: contracts.ValueTypeFloat32, Precision: 2},
				},
			}},
		},
	}
	async := &contracts.AsyncValues{
		DeviceName: "device-01",
		Values: []*contracts.CommandValue{
			{DeviceResourceName: "temperature", Type: contracts.ValueTypeFloat32, Value: float64(float32(12.3456))},
			{DeviceResourceName: "pressure", Type: contracts.ValueTypeFloat64, Value: 98.76549},
			{DeviceResourceName: "level", Type: contracts.ValueTypeFloat32, Value: float32(12.3)},
		},
	}

	event, err := NewTelemetryEvent(device, async)
	if err != nil {
		t.Fatalf("NewTelemetryEvent() error = %v", err)
	}
	for name, want := range map[string]string{
		"temperature": "12.35",
		"pressure":    "98.765",
		"level":       "12.30",
	} {
		if got := string(event.Values[name].Value); got != want {
			t.Errorf("%s raw value = %q, want %q", name, got, want)
		}
	}
}

func TestNewTelemetryEventAppliesPrecisionToExpandedPoint(t *testing.T) {
	device := contracts.DeviceConfig{
		Name: "device-01",
		Telemetry: contracts.TelemetryConfig{Points: []contracts.PointConfig{{
			Name:             "wheel",
			NodeNameTemplate: "DB1.DBD{index}",
			ArrayKeyPattern:  "wheel_{index}",
			ValueType:        contracts.ValueTypeFloat32,
			Precision:        1,
		}}},
	}
	event, err := NewTelemetryEvent(device, &contracts.AsyncValues{
		Values: []*contracts.CommandValue{{
			DeviceResourceName: "wheel_42",
			Type:               contracts.ValueTypeFloat32,
			Value:              float32(3.14159),
		}},
	})
	if err != nil {
		t.Fatalf("NewTelemetryEvent() error = %v", err)
	}
	if got := string(event.Values["wheel_42"].Value); got != "3.1" {
		t.Fatalf("expanded point raw value = %q, want 3.1", got)
	}
}

func TestNewTelemetryEventRejectsInvalidConfiguredFloat(t *testing.T) {
	device := contracts.DeviceConfig{
		Telemetry: contracts.TelemetryConfig{Points: []contracts.PointConfig{{
			Name: "temperature", ValueType: contracts.ValueTypeFloat64, Precision: 2,
		}}},
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := NewTelemetryEvent(device, &contracts.AsyncValues{
			Values: []*contracts.CommandValue{{
				DeviceResourceName: "temperature",
				Type:               contracts.ValueTypeFloat64,
				Value:              value,
			}},
		})
		if err == nil {
			t.Fatalf("NewTelemetryEvent(%v) expected error", value)
		}
	}
}

func TestDataMapPreservesJSONNumberText(t *testing.T) {
	event := TelemetryEvent{Values: map[string]TelemetryValue{
		"temperature": {Type: contracts.ValueTypeFloat32, Value: json.RawMessage("12.30")},
		"counter":     {Type: contracts.ValueTypeUint64, Value: json.RawMessage("18446744073709551615")},
		"object":      {Type: contracts.ValueTypeObject, Value: json.RawMessage(`{"nested":9007199254740993}`)},
	}}
	data, err := event.DataMap()
	if err != nil {
		t.Fatalf("DataMap() error = %v", err)
	}

	assertNumber := func(name, want string, value interface{}) {
		t.Helper()
		number, ok := value.(json.Number)
		if !ok || number.String() != want {
			t.Fatalf("%s = %#v (%T), want json.Number(%q)", name, value, value, want)
		}
	}
	assertNumber("temperature", "12.30", data["temperature"].(map[string]interface{})["value"])
	assertNumber("counter", "18446744073709551615", data["counter"].(map[string]interface{})["value"])
	object := data["object"].(map[string]interface{})["value"].(map[string]interface{})
	assertNumber("object.nested", "9007199254740993", object["nested"])
}

func TestCommandValuesRestoresDeclaredNumericTypes(t *testing.T) {
	event := TelemetryEvent{Values: map[string]TelemetryValue{
		"float32": {Type: contracts.ValueTypeFloat32, Value: json.RawMessage("12.30")},
		"float64": {Type: contracts.ValueTypeFloat64, Value: json.RawMessage("98.765")},
		"int64":   {Type: contracts.ValueTypeInt64, Value: json.RawMessage("-9007199254740993")},
		"uint64":  {Type: contracts.ValueTypeUint64, Value: json.RawMessage("18446744073709551615")},
	}}
	values, err := event.CommandValues()
	if err != nil {
		t.Fatalf("CommandValues() error = %v", err)
	}
	byName := make(map[string]interface{}, len(values))
	for _, value := range values {
		byName[value.DeviceResourceName] = value.Value
	}
	if got, ok := byName["float32"].(float32); !ok || got != float32(12.3) {
		t.Fatalf("float32 = %#v (%T)", byName["float32"], byName["float32"])
	}
	if got, ok := byName["float64"].(float64); !ok || got != 98.765 {
		t.Fatalf("float64 = %#v (%T)", byName["float64"], byName["float64"])
	}
	if got, ok := byName["int64"].(int64); !ok || got != int64(-9007199254740993) {
		t.Fatalf("int64 = %#v (%T)", byName["int64"], byName["int64"])
	}
	if got, ok := byName["uint64"].(uint64); !ok || got != uint64(18446744073709551615) {
		t.Fatalf("uint64 = %#v (%T)", byName["uint64"], byName["uint64"])
	}
}

func TestNewTelemetryEventRejectsExcessivePrecision(t *testing.T) {
	device := contracts.DeviceConfig{
		Telemetry: contracts.TelemetryConfig{Points: []contracts.PointConfig{{
			Name: "temperature", ValueType: contracts.ValueTypeFloat32, Precision: MaxDecimalPrecision + 1,
		}}},
	}
	_, err := NewTelemetryEvent(device, &contracts.AsyncValues{
		Values: []*contracts.CommandValue{{
			DeviceResourceName: "temperature",
			Type:               contracts.ValueTypeFloat32,
			Value:              float32(1.25),
		}},
	})
	if err == nil {
		t.Fatal("NewTelemetryEvent() expected excessive precision error")
	}
}
