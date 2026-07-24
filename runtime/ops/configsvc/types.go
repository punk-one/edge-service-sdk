// Package configsvc provides runtime configuration CRUD for SDK-based edge services.
package configsvc

// ConfigChange describes a single configuration change event.
type ConfigChange struct {
	Scope      string      // "config" / "device" / "profile"
	Name       string      // device name or profile name
	ConfigPath string      // dot-separated path within the config
	OldValue   interface{} // previous value (nil if new)
	NewValue   interface{} // new value
	TargetFile string      // affected YAML file path
	Persist    bool        // true if written to file, false if in-memory only
}

// OverrideRecord describes one temporary override entry.
type OverrideRecord struct {
	Scope      string      `json:"scope"`       // "config" / "device:{name}" / "profile:{name}"
	Name       string      `json:"name"`        // device or profile name (empty for config scope)
	ConfigPath string      `json:"config_path"` // dot-separated path
	Value      interface{} `json:"value"`       // override value
}

// ChangeCallback is invoked after a configuration change is applied.
type ChangeCallback func(change ConfigChange)

// DeviceInfo contains summary information about a device.
type DeviceInfo struct {
	Name        string `json:"name"`
	ProductCode string `json:"productCode"`
	ProfileName string `json:"profileName"`
	Description string `json:"description"`
	File        string `json:"file"`
}

// ProfileInfo contains summary information about a profile.
type ProfileInfo struct {
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	File          string   `json:"file"`
	UsedByDevices []string `json:"used_by_devices"`
}

// ConfigSource indicates where a configuration value originates.
type ConfigSource string

const (
	SourceFile     ConfigSource = "file"
	SourceOverride ConfigSource = "override"
	SourceDevice   ConfigSource = "device"
	SourceProfile  ConfigSource = "profile"
)

// ConfigResult is the standard result for get/list operations.
type ConfigResult struct {
	ConfigPath string       `json:"config_path"`
	Value      interface{}  `json:"value"`
	Source     ConfigSource `json:"source"`
	SourceFile string       `json:"source_file,omitempty"`
}

// ConfigSetResult is the standard result for set operations.
type ConfigSetResult struct {
	ConfigPath    string      `json:"config_path"`
	PreviousValue interface{} `json:"previous_value"`
	CurrentValue  interface{} `json:"current_value"`
	TargetFile    string      `json:"target_file,omitempty"`
	Persist       bool        `json:"persist"`
	NeedRestart   bool        `json:"need_restart"`
}

// LogFileInfo describes a log file.
type LogFileInfo struct {
	Name       string `json:"name"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt int64  `json:"modified_at"`
}