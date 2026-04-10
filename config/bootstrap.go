package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration.
type Config struct {
	Logging       logger.Config            `yaml:"logging"`
	Service       ServiceConfig            `yaml:"service"`
	Storage       StorageConfig            `yaml:"storage"`
	Auth          AuthConfig               `yaml:"auth"`
	MQTT          mqtt.MQTTConfig          `yaml:"mqtt"`
	ReliableQueue reliable.Config          `yaml:"reliableQueue"`
	Device        DeviceConfig             `yaml:"device"`
	TelemetryPost mqtt.TopicConfig         `yaml:"telemetryPost"`
	PropertySet   mqtt.TopicConfig         `yaml:"propertySet"`
	PropertyGet   mqtt.TopicConfig         `yaml:"propertyGet"`
	PropertyPost  mqtt.TopicConfig         `yaml:"propertyPost"`
	StatusReport  mqtt.TopicConfig         `yaml:"statusReport"`
	Devices       []contracts.DeviceConfig `yaml:"deviceList"`
	LogLevel      string                   `yaml:"logLevel"`
}

// StorageConfig represents shared runtime storage.
type StorageConfig struct {
	SQLitePath string `yaml:"sqlitePath"`
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
	StartupMsg string `yaml:"startupMsg"`
	Type       string `yaml:"type"`
}

// DeviceConfig represents device-related configuration.
type DeviceConfig struct {
	ProfilesDir string `yaml:"profilesDir"`
	DevicesDir  string `yaml:"devicesDir"`
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
			SQLitePath: "./data/runtime.db",
		},
		Auth: AuthConfig{
			AccessTokenTTLMin: 10,
			KeyFile:           "./data/auth.key",
		},
		Device: DeviceConfig{
			ProfilesDir: "./configs/profiles",
			DevicesDir:  "./configs/devices",
		},
		ReliableQueue: reliable.Config{
			Enabled:          true,
			SQLitePath:       "./data/runtime.db",
			MemoryQueueSize:  2048,
			BatchSize:        100,
			FlushIntervalMs:  1000,
			ReplayIntervalMs: 3000,
			ReplayRatePerSec: 20,
			RetentionDays:    7,
			KeepLatestOnly:   false,
		},
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
		TelemetryPost: mqtt.TopicConfig{
			Topic:      "v1/gateway/{productCode}/telemetry/post",
			QoS:        0,
			Retain:     false,
			DataFormat: "rule",
		},
		StatusReport: mqtt.TopicConfig{
			QoS:               0,
			Retain:            false,
			HeartbeatInterval: "30s",
		},
		LogLevel: "INFO",
	}

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return config, fmt.Errorf("failed to read config file: %v", err)
		}
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, fmt.Errorf("failed to parse config file: %v", err)
		}
	}

	return NormalizeConfig(config), nil
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
	deviceHasTelemetryOverride := strings.TrimSpace(device.Telemetry.Interval) != "" || len(device.Telemetry.WatchedFields) > 0 || len(device.Telemetry.Points) > 0
	deviceHasPropertyOverride := strings.TrimSpace(device.Property.Interval) != "" || len(device.Property.WatchedFields) > 0 || len(device.Property.Points) > 0 || len(device.Property.Structs) > 0

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

	return NormalizeDeviceConfig(device)
}

func NormalizeConfig(config Config) Config {
	config.Logging = EffectiveLoggerConfig(config)
	if config.TelemetryPost.DataFormat == "" {
		config.TelemetryPost.DataFormat = "rule"
	}
	if strings.TrimSpace(config.StatusReport.HeartbeatInterval) == "" {
		config.StatusReport.HeartbeatInterval = "30s"
	}
	if strings.TrimSpace(config.Storage.SQLitePath) == "" {
		if strings.TrimSpace(config.ReliableQueue.SQLitePath) != "" {
			config.Storage.SQLitePath = config.ReliableQueue.SQLitePath
		} else {
			config.Storage.SQLitePath = "./data/runtime.db"
		}
	}
	config.ReliableQueue.SQLitePath = config.Storage.SQLitePath
	if config.Auth.AccessTokenTTLMin <= 0 {
		config.Auth.AccessTokenTTLMin = 10
	}
	if strings.TrimSpace(config.Auth.KeyFile) == "" {
		config.Auth.KeyFile = "./data/auth.key"
	}
	return config
}

func normalizeConfig(config Config) Config {
	return NormalizeConfig(config)
}

func NormalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
	device.ConnectionStrategy = strings.ToLower(strings.TrimSpace(device.ConnectionStrategy))
	if device.ConnectionStrategy == "" {
		device.ConnectionStrategy = "persistent"
	}
	for i := range device.Telemetry.Points {
		device.Telemetry.Points[i].ValueType = contracts.NormalizedValueType(device.Telemetry.Points[i].ValueType)
	}
	for i := range device.Property.Points {
		device.Property.Points[i].ValueType = contracts.NormalizedValueType(device.Property.Points[i].ValueType)
	}
	for i := range device.Property.Structs {
		for j := range device.Property.Structs[i].Fields {
			device.Property.Structs[i].Fields[j].ValueType = contracts.NormalizedValueType(device.Property.Structs[i].Fields[j].ValueType)
		}
	}
	return device
}

func normalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
	return NormalizeDeviceConfig(device)
}

func NormalizeProfile(profile contracts.DeviceProfile) contracts.DeviceProfile {
	for i := range profile.Telemetry.Points {
		profile.Telemetry.Points[i].ValueType = contracts.NormalizedValueType(profile.Telemetry.Points[i].ValueType)
	}
	for i := range profile.Property.Points {
		profile.Property.Points[i].ValueType = contracts.NormalizedValueType(profile.Property.Points[i].ValueType)
	}
	for i := range profile.Property.Structs {
		for j := range profile.Property.Structs[i].Fields {
			profile.Property.Structs[i].Fields[j].ValueType = contracts.NormalizedValueType(profile.Property.Structs[i].Fields[j].ValueType)
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

func cloneStructs(items []contracts.PropertyStruct) []contracts.PropertyStruct {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]contracts.PropertyStruct, len(items))
	for i := range items {
		cloned[i] = items[i]
		cloned[i].Fields = append([]contracts.PropertyStructField(nil), items[i].Fields...)
	}
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
