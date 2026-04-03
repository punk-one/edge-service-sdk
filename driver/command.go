package driver

import (
	"fmt"
	"time"
)

// CommandRequest represents a command request for reading or writing device resources
type CommandRequest struct {
	DeviceResourceName string                 `json:"deviceResourceName"`
	Type               string                 `json:"type"`
	Attributes         map[string]interface{} `json:"attributes"`
	Properties         ResourceProperties     `json:"properties"` // Resource properties including scale, valueType, etc.
}

// CommandValue represents a command value with its metadata
type CommandValue struct {
	DeviceResourceName string      `json:"deviceResourceName"`
	Type               string      `json:"type"`
	Value              interface{} `json:"value"`
	Origin             int64       `json:"origin"`
}

// AsyncValues represents asynchronous values from device
type AsyncValues struct {
	TraceID     string          `json:"traceId"`
	DeviceName  string          `json:"deviceName"`
	SourceName  string          `json:"sourceName"`
	CollectedAt int64           `json:"collectedAt"`
	Values      []*CommandValue `json:"values"`
}

// NewCommandValue creates a new CommandValue instance
func NewCommandValue(name, valueType string, value interface{}) (*CommandValue, error) {
	if name == "" {
		return nil, fmt.Errorf("device resource name cannot be empty")
	}
	if valueType == "" {
		return nil, fmt.Errorf("value type cannot be empty")
	}

	return &CommandValue{
		DeviceResourceName: name,
		Type:               valueType,
		Value:              value,
		Origin:             time.Now().UnixNano(),
	}, nil
}

// BoolValue returns the boolean value
func (cv *CommandValue) BoolValue() (bool, error) {
	if cv.Type != "Bool" {
		return false, fmt.Errorf("value type is not Bool, got %s", cv.Type)
	}
	if val, ok := cv.Value.(bool); ok {
		return val, nil
	}
	return false, fmt.Errorf("cannot convert value to bool")
}

// StringValue returns the string value
func (cv *CommandValue) StringValue() (string, error) {
	if cv.Type != "String" {
		return "", fmt.Errorf("value type is not String, got %s", cv.Type)
	}
	if val, ok := cv.Value.(string); ok {
		return val, nil
	}
	return "", fmt.Errorf("cannot convert value to string")
}

// Uint8Value returns the uint8 value
func (cv *CommandValue) Uint8Value() (uint8, error) {
	if cv.Type != "Uint8" {
		return 0, fmt.Errorf("value type is not Uint8, got %s", cv.Type)
	}
	if val, ok := cv.Value.(uint8); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to uint8")
}

// Uint16Value returns the uint16 value
func (cv *CommandValue) Uint16Value() (uint16, error) {
	if cv.Type != "Uint16" {
		return 0, fmt.Errorf("value type is not Uint16, got %s", cv.Type)
	}
	if val, ok := cv.Value.(uint16); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to uint16")
}

// Uint32Value returns the uint32 value
func (cv *CommandValue) Uint32Value() (uint32, error) {
	if cv.Type != "Uint32" {
		return 0, fmt.Errorf("value type is not Uint32, got %s", cv.Type)
	}
	if val, ok := cv.Value.(uint32); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to uint32")
}

// Uint64Value returns the uint64 value
func (cv *CommandValue) Uint64Value() (uint64, error) {
	if cv.Type != "Uint64" {
		return 0, fmt.Errorf("value type is not Uint64, got %s", cv.Type)
	}
	if val, ok := cv.Value.(uint64); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to uint64")
}

// Int8Value returns the int8 value
func (cv *CommandValue) Int8Value() (int8, error) {
	if cv.Type != "Int8" {
		return 0, fmt.Errorf("value type is not Int8, got %s", cv.Type)
	}
	if val, ok := cv.Value.(int8); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to int8")
}

// Int16Value returns the int16 value
func (cv *CommandValue) Int16Value() (int16, error) {
	if cv.Type != "Int16" {
		return 0, fmt.Errorf("value type is not Int16, got %s", cv.Type)
	}
	if val, ok := cv.Value.(int16); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to int16")
}

// Int32Value returns the int32 value
func (cv *CommandValue) Int32Value() (int32, error) {
	if cv.Type != "Int32" {
		return 0, fmt.Errorf("value type is not Int32, got %s", cv.Type)
	}
	if val, ok := cv.Value.(int32); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to int32")
}

// Int64Value returns the int64 value
func (cv *CommandValue) Int64Value() (int64, error) {
	if cv.Type != "Int64" {
		return 0, fmt.Errorf("value type is not Int64, got %s", cv.Type)
	}
	if val, ok := cv.Value.(int64); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to int64")
}

// Float32Value returns the float32 value
func (cv *CommandValue) Float32Value() (float32, error) {
	if cv.Type != "Float32" {
		return 0, fmt.Errorf("value type is not Float32, got %s", cv.Type)
	}
	if val, ok := cv.Value.(float32); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to float32")
}

// Float64Value returns the float64 value
func (cv *CommandValue) Float64Value() (float64, error) {
	if cv.Type != "Float64" {
		return 0, fmt.Errorf("value type is not Float64, got %s", cv.Type)
	}
	if val, ok := cv.Value.(float64); ok {
		return val, nil
	}
	return 0, fmt.Errorf("cannot convert value to float64")
}
