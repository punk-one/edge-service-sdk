package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"

	"gopkg.in/yaml.v3"
)

const DefaultRuntimeDatabaseMaxBytes int64 = 2 << 30

// Config represents the application configuration.
type Config struct {
	Logging         logger.Config                  `yaml:"logging"`
	Service         ServiceConfig                  `yaml:"service"`
	Storage         StorageConfig                  `yaml:"storage"`
	Auth            AuthConfig                     `yaml:"auth"`
	MQTT            mqtt.MQTTConfig                `yaml:"mqtt"`
	TelemetryOutbox reliable.TelemetryOutboxConfig `yaml:"telemetryOutbox"`
	Device          DeviceConfig                   `yaml:"device"`
	TelemetryReport mqtt.TopicConfig               `yaml:"telemetryReport"`
	PropertySet     mqtt.TopicConfig               `yaml:"propertySet"`
	PropertyGet     mqtt.TopicConfig               `yaml:"propertyGet"`
	PropertyResult  mqtt.TopicConfig               `yaml:"propertyResult"`
	PropertyReport  mqtt.TopicConfig               `yaml:"propertyReport"`
	CommandCall     mqtt.TopicConfig               `yaml:"commandCall"`
	CommandResult   mqtt.TopicConfig               `yaml:"commandResult"`
	QueryRequest    mqtt.TopicConfig               `yaml:"queryRequest"`
	QueryResult     mqtt.TopicConfig               `yaml:"queryResult"`
	StatusReport    mqtt.TopicConfig               `yaml:"statusReport"`
	EventReport     mqtt.TopicConfig               `yaml:"eventReport"`
	ControlStore    ControlStoreConfig             `yaml:"controlStore"`
	NATSBus         NATSBusConfig                  `yaml:"natsBus"`
	Devices         []contracts.DeviceConfig       `yaml:"deviceList"`
	LogLevel        string                         `yaml:"logLevel"`
}

// NATSBusConfig controls the optional embedded JetStream server. Subjects and the
// listen port are SDK conventions and are intentionally not configurable.
type NATSBusConfig struct {
	Enabled  bool   `yaml:"enabled"`
	StoreDir string `yaml:"storeDir"`
	MaxAge   string `yaml:"maxAge"`
	MaxBytes int64  `yaml:"maxBytes"`
}

// StorageConfig represents shared runtime storage.
type StorageConfig struct {
	SQLitePath       string `yaml:"sqlitePath"`
	MaxDatabaseBytes int64  `yaml:"maxDatabaseBytes"`
}

// ControlStoreConfig controls local control job persistence.
type ControlStoreConfig struct {
	SQLitePath       string `yaml:"sqlitePath"`
	RetentionDays    int    `yaml:"retentionDays"`
	MaxDatabaseBytes int64  `yaml:"maxDatabaseBytes"`
}

// AuthConfig represents auth-related runtime configuration.
type AuthConfig struct {
	AccessTokenTTLMin int    `yaml:"accessTokenTTLMin"`
	BootstrapToken    string `yaml:"bootstrapToken"`
	KeyFile           string `yaml:"keyFile"`
}

// ServiceConfig represents service configuration.
type ServiceConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	PortEnd    int    `yaml:"portEnd"`
	StartupMsg string `yaml:"startupMsg"`
	Type       string `yaml:"type"`
}

// DeviceConfig represents device-related configuration.
type DeviceConfig struct {
	ProfilesDir string `yaml:"profilesDir"`
	DevicesDir  string `yaml:"devicesDir"`
	ProcessDir  string `yaml:"processDir"`
	EventDir    string `yaml:"eventDir"`
}

// LoadConfig loads configuration from YAML file.
func LoadConfig(configPath string) (Config, error) {
	config, err := loadMainConfig(configPath)
	if err != nil {
		return config, err
	}

	devicesDir := config.Device.DevicesDir
	if devicesDir == "" {
		devicesDir = "./configs/devices"
	}
	profilesDir := config.Device.ProfilesDir
	if profilesDir == "" {
		profilesDir = "./configs/profiles"
	}

	profiles, err := loadDeviceProfiles(profilesDir)
	if err != nil {
		return config, err
	}

	devices, err := loadDeviceConfigs(devicesDir)
	if err != nil {
		return config, err
	}
	devices, err = applyProfiles(devices, profiles)
	if err != nil {
		return config, err
	}
	config.Devices = devices
	return config, nil
}

func loadMainConfig(configPath string) (Config, error) {
	config := Config{
		Logging: logger.Config{
			Level:  "info",
			Format: "json",
		},
		Service: ServiceConfig{
			Host:       "localhost",
			Port:       59994,
			StartupMsg: "Edge device service started",
			Type:       "sensor",
		},
		Storage: StorageConfig{
			SQLitePath:       "./data/runtime.db",
			MaxDatabaseBytes: DefaultRuntimeDatabaseMaxBytes,
		},
		Auth: AuthConfig{
			AccessTokenTTLMin: 10,
			KeyFile:           "./data/auth.key",
		},
		Device: DeviceConfig{
			ProfilesDir: "./configs/profiles",
			DevicesDir:  "./configs/devices",
			ProcessDir:  "./configs/process",
			// EventDir intentionally remains empty by default. This preserves
			// compatibility: old services do not initialize EVENT rules until
			// they explicitly opt in.
			EventDir: "",
		},
		TelemetryOutbox: reliable.DefaultTelemetryOutboxConfig(),
		MQTT: mqtt.MQTTConfig{
			CAPath:                  "",
			CertPath:                "",
			PrivKeyPath:             "",
			QoS:                     0,
			Retain:                  false,
			SkipTLSVer:              false,
			URL:                     "tcp://localhost:1883",
			KeepAliveSec:            60,
			PingTimeoutSec:          5,
			ConnectTimeoutSec:       15,
			PublishTimeoutSec:       10,
			HealthCheckIntervalSec:  30,
			InitialRetryIntervalMs:  1000,
			MaxReconnectIntervalSec: 60,
			DisconnectQuiesceMs:     250,
		},
		TelemetryReport: mqtt.TopicConfig{
			Topic:      "v1/gateway/{productCode}/telemetry/report",
			QoS:        1,
			Retain:     false,
			DataFormat: "rule",
		},
		PropertySet: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/property/set",
			QoS:    0,
			Retain: false,
		},
		PropertyGet: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/property/get",
			QoS:    0,
			Retain: false,
		},
		PropertyResult: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/property/result",
			QoS:    0,
			Retain: false,
		},
		PropertyReport: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/property/report",
			QoS:    0,
			Retain: false,
		},
		CommandCall: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/command/call/{identifier}",
			QoS:    0,
			Retain: false,
		},
		CommandResult: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/command/result",
			QoS:    0,
			Retain: false,
		},
		QueryRequest: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/query/request",
			QoS:    0,
			Retain: false,
		},
		QueryResult: mqtt.TopicConfig{
			Topic:  "v1/gateway/{productCode}/query/result",
			QoS:    0,
			Retain: false,
		},
		StatusReport: mqtt.TopicConfig{
			Topic:             "v1/gateway/{productCode}/status/report",
			QoS:               0,
			Retain:            false,
			HeartbeatInterval: "30s",
		},
		ControlStore: ControlStoreConfig{
			SQLitePath:       "./data/runtime.db",
			RetentionDays:    7,
			MaxDatabaseBytes: DefaultRuntimeDatabaseMaxBytes,
		},
		LogLevel: "INFO",
	}

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return config, fmt.Errorf("failed to read config file: %v", err)
		}
		var raw map[string]interface{}
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return config, fmt.Errorf("failed to parse config file: %v", err)
		}
		if _, exists := raw["reliableQueue"]; exists {
			return config, fmt.Errorf("unsupported configuration %q; use %q", "reliableQueue", "telemetryOutbox")
		}
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse config file: %v", err)
		}
	}

	config = NormalizeConfig(config)
	if err := ValidateConfig(config); err != nil {
		return config, err
	}
	return config, nil
}

func loadDeviceConfigs(devicesDir string) ([]contracts.DeviceConfig, error) {
	var devices []contracts.DeviceConfig

	if _, err := os.Stat(devicesDir); os.IsNotExist(err) {
		return devices, nil
	}

	files, err := filepath.Glob(filepath.Join(devicesDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}

		var deviceFile struct {
			DeviceList []contracts.DeviceConfig `yaml:"deviceList"`
		}
		if err := yaml.Unmarshal(data, &deviceFile); err != nil {
			return nil, fmt.Errorf("failed to parse device file %s: %w", file, err)
		}

		for _, device := range deviceFile.DeviceList {
			devices = append(devices, NormalizeDeviceConfig(device))
		}
	}

	return devices, nil
}

func loadDeviceProfiles(profilesDir string) (map[string]contracts.DeviceProfile, error) {
	profiles := make(map[string]contracts.DeviceProfile)

	if _, err := os.Stat(profilesDir); os.IsNotExist(err) {
		return profiles, nil
	}

	files, err := filepath.Glob(filepath.Join(profilesDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read profile file %s: %w", file, err)
		}

		var profile contracts.DeviceProfile
		if err := yaml.Unmarshal(data, &profile); err != nil {
			return nil, fmt.Errorf("failed to parse profile file %s: %w", file, err)
		}
		if strings.TrimSpace(profile.Name) == "" {
			return nil, fmt.Errorf("profile file %s missing name", file)
		}

		profiles[profile.Name] = NormalizeProfile(profile)
	}

	return profiles, nil
}

func ApplyProfiles(devices []contracts.DeviceConfig, profiles map[string]contracts.DeviceProfile) ([]contracts.DeviceConfig, error) {
	merged := make([]contracts.DeviceConfig, 0, len(devices))
	for _, device := range devices {
		if strings.TrimSpace(device.ProfileName) == "" {
			merged = append(merged, NormalizeDeviceConfig(device))
			continue
		}

		profile, ok := profiles[device.ProfileName]
		if !ok {
			return nil, fmt.Errorf("device %s references unknown profile %s", device.Name, device.ProfileName)
		}

		merged = append(merged, mergeDeviceWithProfile(device, profile))
	}
	return merged, nil
}

func applyProfiles(devices []contracts.DeviceConfig, profiles map[string]contracts.DeviceProfile) ([]contracts.DeviceConfig, error) {
	return ApplyProfiles(devices, profiles)
}

func mergeDeviceWithProfile(device contracts.DeviceConfig, profile contracts.DeviceProfile) contracts.DeviceConfig {
	device = NormalizeDeviceConfig(device)
	profile = NormalizeProfile(profile)
	deviceHasTelemetryOverride := strings.TrimSpace(device.Telemetry.Interval) != "" || len(device.Telemetry.WatchedFields) > 0 || len(device.Telemetry.Points) > 0 || len(device.Telemetry.Groups) > 0 || len(device.Telemetry.Structs) > 0
	deviceHasPropertyOverride := strings.TrimSpace(device.Property.Interval) != "" || len(device.Property.WatchedFields) > 0 || len(device.Property.Points) > 0 || len(device.Property.Structs) > 0
	deviceHasCommandOverride := len(device.Commands) > 0

	if strings.TrimSpace(device.Description) == "" {
		device.Description = profile.Description
	}
	if len(device.Labels) == 0 && len(profile.Labels) > 0 {
		device.Labels = append([]string(nil), profile.Labels...)
	}

	if strings.TrimSpace(device.Telemetry.Interval) == "" {
		device.Telemetry.Interval = profile.Telemetry.Interval
	}
	if len(device.Telemetry.WatchedFields) == 0 && len(profile.Telemetry.WatchedFields) > 0 {
		device.Telemetry.WatchedFields = append([]string(nil), profile.Telemetry.WatchedFields...)
	}
	if len(device.Telemetry.Points) == 0 && len(profile.Telemetry.Points) > 0 {
		device.Telemetry.Points = clonePoints(profile.Telemetry.Points)
	}
	if len(device.Telemetry.Groups) == 0 && len(profile.Telemetry.Groups) > 0 {
		device.Telemetry.Groups = cloneGroups(profile.Telemetry.Groups)
	}
	if len(device.Telemetry.Structs) == 0 && len(profile.Telemetry.Structs) > 0 {
		device.Telemetry.Structs = cloneStructs(profile.Telemetry.Structs)
	}
	if !deviceHasTelemetryOverride {
		device.Telemetry.OnChange = profile.Telemetry.OnChange
	}

	if len(device.Property.Points) == 0 && len(profile.Property.Points) > 0 {
		device.Property.Points = clonePoints(profile.Property.Points)
	}
	if len(device.Property.Structs) == 0 && len(profile.Property.Structs) > 0 {
		device.Property.Structs = cloneStructs(profile.Property.Structs)
	}
	if strings.TrimSpace(device.Property.Interval) == "" {
		device.Property.Interval = profile.Property.Interval
	}
	if len(device.Property.WatchedFields) == 0 && len(profile.Property.WatchedFields) > 0 {
		device.Property.WatchedFields = append([]string(nil), profile.Property.WatchedFields...)
	}
	if strings.TrimSpace(device.Property.HeartbeatInterval) == "" {
		device.Property.HeartbeatInterval = profile.Property.HeartbeatInterval
	}
	if !deviceHasPropertyOverride {
		device.Property.OnChange = profile.Property.OnChange
	}
	if !deviceHasCommandOverride && len(profile.Commands) > 0 {
		device.Commands = cloneCommands(profile.Commands)
	}

	return NormalizeDeviceConfig(device)
}

func NormalizeConfig(config Config) Config {
	if strings.TrimSpace(config.Device.ProcessDir) == "" {
		config.Device.ProcessDir = "./configs/process"
	}
	config.Device.ProfilesDir = filepath.FromSlash(config.Device.ProfilesDir)
	config.Device.DevicesDir = filepath.FromSlash(config.Device.DevicesDir)
	config.Device.ProcessDir = filepath.FromSlash(config.Device.ProcessDir)
	config.Device.EventDir = filepath.FromSlash(config.Device.EventDir)
	config.NATSBus.StoreDir = filepath.FromSlash(config.NATSBus.StoreDir)
	config.Logging = EffectiveLoggerConfig(config)
	if config.TelemetryReport.DataFormat == "" {
		config.TelemetryReport.DataFormat = "rule"
	}
	if strings.TrimSpace(config.StatusReport.HeartbeatInterval) == "" {
		config.StatusReport.HeartbeatInterval = "30s"
	}
	if strings.TrimSpace(config.Storage.SQLitePath) == "" {
		config.Storage.SQLitePath = "./data/runtime.db"
	}
	if config.Storage.MaxDatabaseBytes == 0 {
		config.Storage.MaxDatabaseBytes = DefaultRuntimeDatabaseMaxBytes
	}
	config.Storage.SQLitePath = filepath.FromSlash(config.Storage.SQLitePath)
	config.TelemetryOutbox = reliable.NormalizeTelemetryOutboxConfig(config.TelemetryOutbox)
	config.TelemetryOutbox.SQLitePath = filepath.FromSlash(config.TelemetryOutbox.SQLitePath)
	if strings.TrimSpace(config.ControlStore.SQLitePath) == "" {
		config.ControlStore.SQLitePath = config.Storage.SQLitePath
	}
	if config.ControlStore.RetentionDays <= 0 {
		config.ControlStore.RetentionDays = 7
	}
	if config.ControlStore.MaxDatabaseBytes == 0 {
		config.ControlStore.MaxDatabaseBytes = config.Storage.MaxDatabaseBytes
	}
	if strings.EqualFold(filepath.Clean(config.Storage.SQLitePath), filepath.Clean(config.ControlStore.SQLitePath)) && config.ControlStore.MaxDatabaseBytes > config.Storage.MaxDatabaseBytes {
		config.ControlStore.MaxDatabaseBytes = config.Storage.MaxDatabaseBytes
	}
	if config.Auth.AccessTokenTTLMin <= 0 {
		config.Auth.AccessTokenTTLMin = 10
	}
	if strings.TrimSpace(config.Auth.KeyFile) == "" {
		config.Auth.KeyFile = "./data/auth.key"
	}
	return config
}

// ValidateConfig checks invariants that cannot be represented by YAML types.
func ValidateConfig(config Config) error {
	outbox := config.TelemetryOutbox
	if err := reliable.ValidateTelemetryOutboxConfig(outbox); err != nil {
		return err
	}
	if config.Storage.MaxDatabaseBytes < 64<<20 {
		return fmt.Errorf("storage.maxDatabaseBytes must be >= 67108864")
	}
	if config.ControlStore.MaxDatabaseBytes < 64<<20 {
		return fmt.Errorf("controlStore.maxDatabaseBytes must be >= 67108864")
	}
	if config.Service.Port < -1 || config.Service.Port > 65_535 {
		return fmt.Errorf("service.port must be -1 or between 0 and 65535")
	}
	if config.Service.PortEnd < 0 || config.Service.PortEnd > 65_535 || (config.Service.PortEnd > 0 && config.Service.Port > 0 && config.Service.PortEnd < config.Service.Port) {
		return fmt.Errorf("service.portEnd must be 0 or a valid port not lower than service.port")
	}
	if err := validateQoS("mqtt.qos", config.MQTT.QoS); err != nil {
		return err
	}
	for name, topic := range map[string]mqtt.TopicConfig{
		"telemetryReport": config.TelemetryReport, "propertySet": config.PropertySet,
		"propertyGet": config.PropertyGet, "propertyResult": config.PropertyResult,
		"propertyReport": config.PropertyReport, "commandCall": config.CommandCall,
		"commandResult": config.CommandResult, "queryRequest": config.QueryRequest,
		"queryResult": config.QueryResult, "statusReport": config.StatusReport,
		"eventReport": config.EventReport,
	} {
		if err := validateQoS(name+".qos", topic.QoS); err != nil {
			return err
		}
	}
	if err := validatePositiveDuration("statusReport.heartbeatInterval", config.StatusReport.HeartbeatInterval, false); err != nil {
		return err
	}
	groupNames := make(map[string]struct{}, len(config.MQTT.Groups))
	for i, group := range config.MQTT.Groups {
		name := strings.TrimSpace(group.Name)
		if name == "" {
			return fmt.Errorf("mqtt.groups[%d].name is required", i)
		}
		if _, exists := groupNames[name]; exists {
			return fmt.Errorf("mqtt group name %q is duplicated", name)
		}
		groupNames[name] = struct{}{}
		if err := validatePositiveDuration(fmt.Sprintf("mqtt.groups[%d].heartbeatInterval", i), group.HeartbeatInterval, true); err != nil {
			return err
		}
		if group.QOS < 0 || group.QOS > 2 {
			return fmt.Errorf("mqtt.groups[%d].qos must be between 0 and 2", i)
		}
	}
	deviceNames := make(map[string]struct{}, len(config.Devices))
	for i, rawDevice := range config.Devices {
		device := NormalizeDeviceConfig(rawDevice)
		if device.Name == "" {
			return fmt.Errorf("deviceList[%d].name is required", i)
		}
		if _, exists := deviceNames[device.InternalName]; exists {
			return fmt.Errorf("device internal name %q is duplicated", device.InternalName)
		}
		deviceNames[device.InternalName] = struct{}{}
		if err := validatePositiveDuration("device "+device.InternalName+" telemetry.interval", device.Telemetry.Interval, true); err != nil {
			return err
		}
		if err := validatePositiveDuration("device "+device.InternalName+" telemetry.heartbeatInterval", device.Telemetry.HeartbeatInterval, true); err != nil {
			return err
		}
		for _, group := range device.Telemetry.Groups {
			if err := validatePositiveDuration("device "+device.InternalName+" telemetry group "+group.Name+" interval", group.Interval, true); err != nil {
				return err
			}
			if err := validatePositiveDuration("device "+device.InternalName+" telemetry group "+group.Name+" heartbeatInterval", group.HeartbeatInterval, true); err != nil {
				return err
			}
		}
		if err := validatePositiveDuration("device "+device.InternalName+" property.interval", device.Property.Interval, true); err != nil {
			return err
		}
		if err := validatePositiveDuration("device "+device.InternalName+" property.heartbeatInterval", device.Property.HeartbeatInterval, true); err != nil {
			return err
		}
	}
	if strings.TrimSpace(config.TelemetryReport.Topic) == "" {
		return nil
	}
	runtimePath, err := filepath.Abs(filepath.Clean(config.Storage.SQLitePath))
	if err != nil {
		return fmt.Errorf("resolve storage.sqlitePath: %w", err)
	}
	outboxPath, err := filepath.Abs(filepath.Clean(outbox.SQLitePath))
	if err != nil {
		return fmt.Errorf("resolve telemetryOutbox.sqlitePath: %w", err)
	}
	if strings.EqualFold(runtimePath, outboxPath) {
		return fmt.Errorf("telemetryOutbox.sqlitePath must use a database file separate from storage.sqlitePath")
	}
	return nil
}

func validateQoS(name string, value int) error {
	if value < 0 || value > 2 {
		return fmt.Errorf("%s must be between 0 and 2", name)
	}
	return nil
}

func validatePositiveDuration(name, raw string, allowEmpty bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" && allowEmpty {
		return nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return fmt.Errorf("%s must be a positive duration", name)
	}
	return nil
}

func normalizeConfig(config Config) Config {
	return NormalizeConfig(config)
}

func NormalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
	device.Name = strings.TrimSpace(device.Name)
	device.SubName = strings.TrimSpace(device.SubName)
	device.EventProfile = strings.TrimSpace(device.EventProfile)
	device.ProcessNames = normalizedUniqueStrings(device.ProcessNames)
	if device.SubName != "" {
		device.InternalName = device.Name + "-" + device.SubName
	} else {
		device.InternalName = device.Name
	}
	device.ConnectionStrategy = strings.ToLower(strings.TrimSpace(device.ConnectionStrategy))
	if device.ConnectionStrategy == "" {
		device.ConnectionStrategy = "persistent"
	}
	for i := range device.Telemetry.Points {
		device.Telemetry.Points[i].ValueType = contracts.NormalizedValueType(device.Telemetry.Points[i].ValueType)
	}
	for i := range device.Telemetry.Groups {
		for j := range device.Telemetry.Groups[i].Points {
			device.Telemetry.Groups[i].Points[j].ValueType = contracts.NormalizedValueType(device.Telemetry.Groups[i].Points[j].ValueType)
		}
	}
	for i := range device.Property.Points {
		device.Property.Points[i].ValueType = contracts.NormalizedValueType(device.Property.Points[i].ValueType)
	}
	for i := range device.Property.Structs {
		normalizeFields(device.Property.Structs[i].Fields)
	}
	for i := range device.Commands {
		device.Commands[i].Identifier = strings.TrimSpace(device.Commands[i].Identifier)
		for j := range device.Commands[i].InputSchema {
			device.Commands[i].InputSchema[j].ValueType = contracts.NormalizedValueType(device.Commands[i].InputSchema[j].ValueType)
		}
		for j := range device.Commands[i].OutputSchema {
			device.Commands[i].OutputSchema[j].ValueType = contracts.NormalizedValueType(device.Commands[i].OutputSchema[j].ValueType)
		}
	}
	return device
}

func normalizedUniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
	return NormalizeDeviceConfig(device)
}

func NormalizeProfile(profile contracts.DeviceProfile) contracts.DeviceProfile {
	for i := range profile.Telemetry.Points {
		profile.Telemetry.Points[i].ValueType = contracts.NormalizedValueType(profile.Telemetry.Points[i].ValueType)
	}
	for i := range profile.Telemetry.Groups {
		for j := range profile.Telemetry.Groups[i].Points {
			profile.Telemetry.Groups[i].Points[j].ValueType = contracts.NormalizedValueType(profile.Telemetry.Groups[i].Points[j].ValueType)
		}
	}
	for i := range profile.Property.Points {
		profile.Property.Points[i].ValueType = contracts.NormalizedValueType(profile.Property.Points[i].ValueType)
	}
	for i := range profile.Property.Structs {
		for j := range profile.Property.Structs[i].Fields {
			profile.Property.Structs[i].Fields[j].ValueType = contracts.NormalizedValueType(profile.Property.Structs[i].Fields[j].ValueType)
		}
	}
	for i := range profile.Commands {
		profile.Commands[i].Identifier = strings.TrimSpace(profile.Commands[i].Identifier)
		for j := range profile.Commands[i].InputSchema {
			profile.Commands[i].InputSchema[j].ValueType = contracts.NormalizedValueType(profile.Commands[i].InputSchema[j].ValueType)
		}
		for j := range profile.Commands[i].OutputSchema {
			profile.Commands[i].OutputSchema[j].ValueType = contracts.NormalizedValueType(profile.Commands[i].OutputSchema[j].ValueType)
		}
	}
	return profile
}

func normalizeProfile(profile contracts.DeviceProfile) contracts.DeviceProfile {
	return NormalizeProfile(profile)
}

func clonePoints(points []contracts.PointConfig) []contracts.PointConfig {
	if len(points) == 0 {
		return nil
	}
	cloned := make([]contracts.PointConfig, len(points))
	copy(cloned, points)
	return cloned
}

func cloneGroups(groups []contracts.TelemetryGroup) []contracts.TelemetryGroup {
	if len(groups) == 0 {
		return nil
	}
	cloned := make([]contracts.TelemetryGroup, len(groups))
	for i := range groups {
		cloned[i] = groups[i]
		cloned[i].Points = clonePoints(groups[i].Points)
		cloned[i].ReadFirstFields = append([]string(nil), groups[i].ReadFirstFields...)
		cloned[i].WatchedFields = append([]string(nil), groups[i].WatchedFields...)
		cloned[i].Structs = cloneStructs(groups[i].Structs)
	}
	return cloned
}

func cloneStructs(items []contracts.PropertyStruct) []contracts.PropertyStruct {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]contracts.PropertyStruct, len(items))
	for i := range items {
		cloned[i] = items[i]
		cloned[i].Fields = cloneFields(items[i].Fields)
	}
	return cloned
}

func cloneFields(fields []contracts.PropertyStructField) []contracts.PropertyStructField {
	if len(fields) == 0 {
		return nil
	}
	cloned := make([]contracts.PropertyStructField, len(fields))
	for i := range fields {
		cloned[i] = fields[i]
		if len(fields[i].Fields) > 0 {
			cloned[i].Fields = cloneFields(fields[i].Fields)
		}
	}
	return cloned
}

func normalizeFields(fields []contracts.PropertyStructField) {
	for i := range fields {
		if fields[i].IsScalar() {
			fields[i].ValueType = contracts.NormalizedValueType(fields[i].ValueType)
		} else {
			normalizeFields(fields[i].Fields)
		}
	}
}

func cloneCommands(items []contracts.CommandConfig) []contracts.CommandConfig {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]contracts.CommandConfig, len(items))
	copy(cloned, items)
	return cloned
}

func EffectiveLogLevel(config Config) string {
	if config.Logging.Level != "" {
		return config.Logging.Level
	}
	if config.LogLevel != "" {
		return config.LogLevel
	}
	return "INFO"
}

func effectiveLogLevel(config Config) string {
	return EffectiveLogLevel(config)
}

func EffectiveLoggerConfig(config Config) logger.Config {
	cfg := config.Logging
	if cfg.Level == "" {
		cfg.Level = EffectiveLogLevel(config)
	}
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = 100
	}
	if cfg.MaxFiles == 0 {
		cfg.MaxFiles = 7
	}
	if cfg.MaxBackups == 0 {
		cfg.MaxBackups = 3
	}
	return cfg
}

func effectiveLoggerConfig(config Config) logger.Config {
	return EffectiveLoggerConfig(config)
}

func StringsReplaceProductCode(template string, productCode string) string {
	return strings.ReplaceAll(template, "{productCode}", productCode)
}

func stringsReplaceProductCode(template string, productCode string) string {
	return StringsReplaceProductCode(template, productCode)
}
