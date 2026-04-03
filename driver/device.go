package driver

import (
	"fmt"
	"strings"
)

// Device represents a device in the system
type Device struct {
	Name        string                        `json:"name" yaml:"name"`
	ProductCode string                        `json:"productCode,omitempty" yaml:"productCode,omitempty"`
	Protocols   map[string]ProtocolProperties `json:"protocols" yaml:"protocols"`
}

// ProtocolProperties represents protocol-specific properties
type ProtocolProperties map[string]interface{}

// AdminState represents the administrative state of a device
type AdminState int

const (
	// Locked indicates the device is locked
	Locked AdminState = iota
	// Unlocked indicates the device is unlocked
	Unlocked
)

// String returns the string representation of AdminState
func (a AdminState) String() string {
	switch a {
	case Locked:
		return "LOCKED"
	case Unlocked:
		return "UNLOCKED"
	default:
		return "UNKNOWN"
	}
}

// DeviceConfig represents device configuration loaded from YAML
type DeviceConfig struct {
	Name               string                 `yaml:"name"`
	ProfileName        string                 `yaml:"profileName"`
	ProductCode        string                 `yaml:"productCode"`
	Description        string                 `yaml:"description"`
	Labels             []string               `yaml:"labels"`
	Protocols          map[string]interface{} `yaml:"protocols"`
	ConnectionStrategy string                 `yaml:"connectionStrategy"`
	Telemetry          TelemetryConfig        `yaml:"telemetry"`
	Property           PropertyConfig         `yaml:"property"`
}

// DeviceProfile defines a reusable point/profile template shared by many devices.
type DeviceProfile struct {
	Name        string          `yaml:"name"`
	Description string          `yaml:"description"`
	Labels      []string        `yaml:"labels"`
	Telemetry   TelemetryConfig `yaml:"telemetry"`
	Property    PropertyConfig  `yaml:"property"`
}

// TelemetryConfig defines periodic telemetry collection for a device.
type TelemetryConfig struct {
	Interval          string        `yaml:"interval"`
	OnChange          bool          `yaml:"onChange"`
	WatchedFields     []string      `yaml:"watchedFields"`
	HeartbeatInterval string        `yaml:"heartbeatInterval"`
	Points            []PointConfig `yaml:"points"`
}

// PropertyConfig defines remotely readable/writable properties for a device.
type PropertyConfig struct {
	Points  []PointConfig    `yaml:"points"`
	Structs []PropertyStruct `yaml:"structs"`
}

// PointConfig defines a telemetry/property point configured directly on the device.
type PointConfig struct {
	Name              string  `yaml:"name"`
	ValueType         string  `yaml:"valueType"`
	NodeName          string  `yaml:"nodeName"`
	NodeNameTemplate  string  `yaml:"nodeNameTemplate"`
	ArrayKeyPattern   string  `yaml:"arrayKeyPattern"`
	MaxLength         int     `yaml:"maxLength"`
	Scale             string  `yaml:"scale"`
	Precision         int     `yaml:"precision"`
	ReadWrite         string  `yaml:"readWrite"`
	OnChange          *bool   `yaml:"onChange"`
	Deadband          float64 `yaml:"deadband"`
	HeartbeatInterval string  `yaml:"heartbeatInterval"`
	KeepLatestOnly    bool    `yaml:"keepLatestOnly"`
}

// PropertyStruct describes a structured property collection such as wheels[1..450].
type PropertyStruct struct {
	Name      string                `yaml:"name"`
	Kind      string                `yaml:"kind"`
	IndexBase int                   `yaml:"indexBase"`
	MaxItems  int                   `yaml:"maxItems"`
	Address   PropertyStructAddress `yaml:"address"`
	Fields    []PropertyStructField `yaml:"fields"`
}

// PropertyStructAddress defines the base address mapping for a struct array.
type PropertyStructAddress struct {
	DBNumber    int    `yaml:"dbNumber"`
	BaseOffset  int    `yaml:"baseOffset"`
	IndexStride int    `yaml:"indexStride"`
	Unit        string `yaml:"unit"`
}

// PropertyStructField defines one field inside a struct array element.
type PropertyStructField struct {
	Name        string `yaml:"name"`
	ValueType   string `yaml:"valueType"`
	FieldOffset int    `yaml:"fieldOffset"`
	MaxLength   int    `yaml:"maxLength"`
	ReadWrite   string `yaml:"readWrite"`
}

// ResourceProperties represents resource properties
type ResourceProperties struct {
	ValueType string `yaml:"valueType"`
	ReadWrite string `yaml:"readWrite"`
	Scale     string `yaml:"scale"`     // Scale factor as string (e.g., "0.1", "10")
	Precision int    `yaml:"precision"` // Precision: number of decimal places (e.g., 2 means 2 decimal places)
	MaxLength int    `yaml:"maxLength"`
}

// NormalizedValueType maps friendly aliases used in device YAML to the driver's canonical names.
func NormalizedValueType(valueType string) string {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "bool", "boolean":
		return "Bool"
	case "string":
		return "String"
	case "int", "int16":
		return "Int16"
	case "int32":
		return "Int32"
	case "int64":
		return "Int64"
	case "uint8":
		return "Uint8"
	case "uint", "uint16":
		return "Uint16"
	case "uint32":
		return "Uint32"
	case "uint64":
		return "Uint64"
	case "float", "float32":
		return "Float32"
	case "float64":
		return "Float64"
	default:
		return strings.TrimSpace(valueType)
	}
}

// ToCommandRequest converts a configured point into a driver request.
func (p PointConfig) ToCommandRequest(nodeName string) (CommandRequest, error) {
	if p.Name == "" {
		return CommandRequest{}, fmt.Errorf("point name cannot be empty")
	}
	if nodeName == "" {
		nodeName = p.NodeName
	}
	if nodeName == "" {
		return CommandRequest{}, fmt.Errorf("point %s missing nodeName", p.Name)
	}
	valueType := NormalizedValueType(p.ValueType)
	if valueType == "" {
		return CommandRequest{}, fmt.Errorf("point %s missing valueType", p.Name)
	}
	readWrite := p.ReadWrite
	if readWrite == "" {
		readWrite = "R"
	}
	return CommandRequest{
		DeviceResourceName: p.Name,
		Type:               valueType,
		Attributes: map[string]interface{}{
			"NodeName": nodeName,
		},
		Properties: ResourceProperties{
			ValueType: valueType,
			ReadWrite: readWrite,
			Scale:     p.Scale,
			Precision: p.Precision,
			MaxLength: p.MaxLength,
		},
	}, nil
}
