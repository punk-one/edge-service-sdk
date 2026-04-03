package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	dependency "github.com/punk-one/edge-service-sdk/runtime/dependency"
	supervisor "github.com/punk-one/edge-service-sdk/runtime/scheduler"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
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

// DeviceSDK implements a DeviceServiceSDK.
type DeviceSDK struct {
	logger         logger.LoggingClient
	asyncCh        chan *contracts.AsyncValues
	devices        []contracts.Device
	deviceConfigs  map[string]contracts.DeviceConfig
	productDevices map[string][]contracts.DeviceConfig
	statusTracker  *rtstatus.Tracker
}

type telemetryState struct {
	lastValues    map[string]interface{}
	lastEmittedAt map[string]int64
}

// NewDeviceSDK creates a new DeviceSDK instance.
func NewDeviceSDK(config Config, logClient logger.LoggingClient, tracker *rtstatus.Tracker) *DeviceSDK {
	if logClient == nil {
		logClient = logger.NewLogger("device-s7", effectiveLoggerConfig(config))
	}
	asyncCh := make(chan *contracts.AsyncValues, 100)

	devices := make([]contracts.Device, 0, len(config.Devices))
	deviceConfigs := make(map[string]contracts.DeviceConfig, len(config.Devices))
	productDevices := make(map[string][]contracts.DeviceConfig)

	for _, deviceConfig := range config.Devices {
		deviceConfig = normalizeDeviceConfig(deviceConfig)
		deviceConfigs[deviceConfig.Name] = deviceConfig
		productDevices[deviceConfig.ProductCode] = append(productDevices[deviceConfig.ProductCode], deviceConfig)
		if tracker != nil {
			tracker.RegisterDevice(deviceConfig.Name, deviceConfig.ProductCode)
		}
		devices = append(devices, contracts.Device{
			Name:        deviceConfig.Name,
			ProductCode: deviceConfig.ProductCode,
			Protocols:   protocolPropertiesFromConfig(deviceConfig),
		})
	}

	return &DeviceSDK{
		logger:         logClient,
		asyncCh:        asyncCh,
		devices:        devices,
		deviceConfigs:  deviceConfigs,
		productDevices: productDevices,
		statusTracker:  tracker,
	}
}

func (s *DeviceSDK) LoggingClient() logger.LoggingClient {
	return s.logger
}

func (s *DeviceSDK) AsyncValuesChannel() chan<- *contracts.AsyncValues {
	return s.asyncCh
}

func (s *DeviceSDK) Devices() []contracts.Device {
	return s.devices
}

func (s *DeviceSDK) DeviceConfigByName(name string) (contracts.DeviceConfig, bool) {
	device, ok := s.deviceConfigs[name]
	return device, ok
}

func (s *DeviceSDK) DevicesByProductCode(productCode string) []contracts.DeviceConfig {
	return append([]contracts.DeviceConfig(nil), s.productDevices[productCode]...)
}

func (s *DeviceSDK) DeviceConnected(deviceName string) {
	if s.statusTracker != nil {
		s.statusTracker.MarkConnected(deviceName)
	}
}

func (s *DeviceSDK) DeviceDisconnected(deviceName string, err error) {
	if s.statusTracker != nil {
		s.statusTracker.MarkDisconnected(deviceName, err)
	}
}

func (s *DeviceSDK) DeviceReadSucceeded(deviceName string) {
	if s.statusTracker != nil {
		s.statusTracker.MarkReadSuccess(deviceName)
	}
}

func (s *DeviceSDK) DeviceReadFailed(deviceName string, err error) {
	if s.statusTracker != nil {
		s.statusTracker.MarkReadError(deviceName, err)
	}
}

func (s *DeviceSDK) DeviceWriteSucceeded(deviceName string) {
	if s.statusTracker != nil {
		s.statusTracker.MarkWriteSuccess(deviceName)
	}
}

func (s *DeviceSDK) DeviceWriteFailed(deviceName string, err error) {
	if s.statusTracker != nil {
		s.statusTracker.MarkWriteError(deviceName, err)
	}
}

// Bootstrap starts the device service.
func Bootstrap(serviceName, version string, driver contracts.ProtocolDriver) {
	fmt.Printf("Starting %s version %s\n", serviceName, version)

	config, err := LoadConfig("./configs/config.yaml")
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}
	config = normalizeConfig(config)

	logLevel := effectiveLogLevel(config)
	logClient := logger.NewLogger(serviceName, effectiveLoggerConfig(config))
	logCfg := effectiveLoggerConfig(config)
	logClient.Infof(
		"Logging configured: level=%s format=%s file=%s max_size_mb=%d max_files=%d max_backups=%d compress=%t",
		logCfg.Level,
		logCfg.Format,
		logCfg.File,
		logCfg.MaxSize,
		logCfg.MaxFiles,
		logCfg.MaxBackups,
		logCfg.Compress,
	)
	logClient.Infof("Logging level set to: %s", logLevel)

	publisher := mqtt.NewMQTTPublisher(config.MQTT, config.TelemetryPost, config.PropertyPost, config.StatusReport, logClient)
	telemetrySink, err := reliable.NewDispatcher(config.ReliableQueue, publisher, logClient)
	if err != nil {
		logClient.Errorf("Failed to initialize reliable telemetry dispatcher: %v", err)
		return
	}
	statusTracker := rtstatus.NewTracker()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     config.Storage.SQLitePath,
		KeyFile:        config.Auth.KeyFile,
		BootstrapToken: config.Auth.BootstrapToken,
		AccessTokenTTL: time.Duration(config.Auth.AccessTokenTTLMin) * time.Minute,
	})
	if err != nil {
		logClient.Errorf("Failed to initialize auth service: %v", err)
		return
	}
	sdk := NewDeviceSDK(config, logClient, statusTracker)

	if err := driver.Initialize(sdk); err != nil {
		logClient.Errorf("Failed to initialize driver: %v", err)
		return
	}

	dependencyManager := dependency.NewDependencyManager(logClient)
	dependencyManager.Register(dependency.NamedDependency("driver", func() error { return nil }))
	dependencyManager.Register(dependency.NamedDependency("auth", authService.HealthCheck))
	dependencyManager.Register(dependency.NamedDependency("mqtt", publisher.HealthCheck))
	if err := dependencyManager.CheckAll(); err != nil {
		logClient.Errorf("Dependency check failed: %v", err)
		return
	}

	installStatusPublisher(statusTracker, sdk, publisher, config.StatusReport, logClient)
	registerPropertyHandlers(config, sdk, driver, publisher, logClient)

	go processAsyncValues(sdk, telemetrySink, logClient)

	supervisor := supervisor.NewSupervisor(logClient, 5*time.Second)
	workerCount := 0
	for _, device := range config.Devices {
		device = normalizeDeviceConfig(device)
		if len(device.Telemetry.Points) == 0 {
			logClient.Warnf("Skipping device %s: telemetry.points is empty", device.Name)
			continue
		}
		workerCount++
		deviceCopy := device
		interval := deviceCopy.Telemetry.Interval
		if interval == "" {
			interval = "20s"
		}
		logClient.Infof(
			"Registering telemetry worker: device=%s product=%s interval=%s points=%d connection_strategy=%s",
			deviceCopy.Name,
			deviceCopy.ProductCode,
			interval,
			len(deviceCopy.Telemetry.Points),
			deviceCopy.ConnectionStrategy,
		)
		supervisor.Start(deviceCopy.Name, func() error {
			return runTelemetryWorker(driver, deviceCopy, sdk, logClient)
		})
	}

	httpRuntime := httpserver.New(httpserver.Config{
		ServiceName:          serviceName,
		Version:              version,
		Host:                 config.Service.Host,
		Port:                 config.Service.Port,
		StartupMsg:           config.Service.StartupMsg,
		ServiceType:          config.Service.Type,
		StartedAt:            time.Now(),
		DeviceCount:          len(config.Devices),
		TelemetryWorkerCount: workerCount,
		ReliableQueueEnabled: config.ReliableQueue.Enabled,
		Readiness:            buildRuntimeReadiness(authService, publisher),
		QueueStats:           telemetrySink.Stats,
		DeviceStates:         statusTracker.Snapshot,
		AuthService:          authService,
		PropertyGet: func(req rtapi.PropertyRequest) (rtapi.PropertyResponse, int) {
			return executePropertyGet(req, "", sdk, driver)
		},
		PropertySet: func(req rtapi.PropertyRequest) (rtapi.PropertySetResponse, int) {
			return executePropertySet(req, "", sdk, driver)
		},
		Logger: logClient,
	})
	if httpRuntime.Enabled() {
		supervisor.Start("http-runtime", func() error {
			return httpRuntime.Run()
		})
	} else {
		logClient.Infof("HTTP runtime server disabled: service.port=%d", config.Service.Port)
	}

	logClient.Infof("Device service %s started successfully with %d devices and %d telemetry workers", serviceName, len(config.Devices), workerCount)
	select {}
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
			StartupMsg: "S7 device service started",
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

	return normalizeConfig(config), nil
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
			devices = append(devices, normalizeDeviceConfig(device))
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

		profiles[profile.Name] = normalizeProfile(profile)
	}

	return profiles, nil
}

func applyProfiles(devices []contracts.DeviceConfig, profiles map[string]contracts.DeviceProfile) ([]contracts.DeviceConfig, error) {
	merged := make([]contracts.DeviceConfig, 0, len(devices))
	for _, device := range devices {
		if strings.TrimSpace(device.ProfileName) == "" {
			merged = append(merged, normalizeDeviceConfig(device))
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

func mergeDeviceWithProfile(device contracts.DeviceConfig, profile contracts.DeviceProfile) contracts.DeviceConfig {
	device = normalizeDeviceConfig(device)
	profile = normalizeProfile(profile)
	deviceHasTelemetryOverride := strings.TrimSpace(device.Telemetry.Interval) != "" || len(device.Telemetry.WatchedFields) > 0 || len(device.Telemetry.Points) > 0

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

	return normalizeDeviceConfig(device)
}

func processAsyncValues(sdk *DeviceSDK, telemetrySink reliable.TelemetrySink, logClient logger.LoggingClient) {
	for asyncValues := range sdk.asyncCh {
		device, ok := sdk.DeviceConfigByName(asyncValues.DeviceName)
		if !ok {
			logClient.Warnf("Dropping async values for unknown device %s", asyncValues.DeviceName)
			continue
		}
		if err := telemetrySink.PublishAsyncValues(device, asyncValues); err != nil {
			logClient.Errorf("Failed to publish async values for %s: %v", asyncValues.DeviceName, err)
		}
	}
}

func registerPropertyHandlers(config Config, sdk *DeviceSDK, driver contracts.ProtocolDriver, publisher mqtt.Publisher, logClient logger.LoggingClient) {
	for productCode := range sdk.productDevices {
		if config.PropertySet.Topic != "" {
			topic := stringsReplaceProductCode(config.PropertySet.Topic, productCode)
			_ = publisher.Subscribe(topic, byte(config.PropertySet.QoS), func(_ string, payload []byte) {
				handlePropertySet(productCode, payload, sdk, driver, publisher, logClient)
			})
		}

		if config.PropertyGet.Topic != "" && config.PropertyPost.Topic != "" {
			topic := stringsReplaceProductCode(config.PropertyGet.Topic, productCode)
			_ = publisher.Subscribe(topic, byte(config.PropertyGet.QoS), func(_ string, payload []byte) {
				handlePropertyGet(productCode, payload, sdk, driver, publisher, logClient)
			})
		} else if config.PropertyGet.Topic != "" && config.PropertyPost.Topic == "" {
			logClient.Warnf("PropertyGet configured but PropertyPost topic is empty; disabling property get for product %s", productCode)
		}
	}
}

func handlePropertySet(productCode string, payload []byte, sdk *DeviceSDK, driver contracts.ProtocolDriver, publisher mqtt.Publisher, logClient logger.LoggingClient) {
	req, err := parsePropertyRequest(payload)
	if err != nil {
		logClient.Warnf("Failed to parse property_set payload for product %s: %v", productCode, err)
		return
	}

	response, _ := executePropertySet(req, productCode, sdk, driver)
	if !response.Success && logClient != nil {
		logClient.Warnf("MQTT property set failed: product=%s device=%s err=%s", productCode, response.DeviceCode, response.Error)
	}
}

func handlePropertyGet(productCode string, payload []byte, sdk *DeviceSDK, driver contracts.ProtocolDriver, publisher mqtt.Publisher, logClient logger.LoggingClient) {
	req, err := parsePropertyRequest(payload)
	if err != nil {
		logClient.Warnf("Failed to parse property_get payload for product %s: %v", productCode, err)
		return
	}

	response, _ := executePropertyGet(req, productCode, sdk, driver)
	device, ok := sdk.DeviceConfigByName(response.DeviceCode)
	if !ok || device.ProductCode != response.ProductCode {
		return
	}
	_ = publisher.PublishPropertyPost(device, map[string]interface{}{
		"product_code": response.ProductCode,
		"device_code":  response.DeviceCode,
		"time":         response.Time,
		"success":      response.Success,
		"request_id":   response.RequestID,
		"error":        response.Error,
		"data":         response.Data,
	})
}

func runTelemetryWorker(driver contracts.ProtocolDriver, device contracts.DeviceConfig, sdk *DeviceSDK, logClient logger.LoggingClient) error {
	interval := device.Telemetry.Interval
	if interval == "" {
		interval = "20s"
	}

	duration, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("invalid telemetry interval %s for device %s: %w", interval, device.Name, err)
	}

	reqs, err := buildTelemetryRequests(device)
	if err != nil {
		return fmt.Errorf("invalid telemetry points for device %s: %w", device.Name, err)
	}

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	state := telemetryState{
		lastValues:    make(map[string]interface{}),
		lastEmittedAt: make(map[string]int64),
	}
	for {
		values, err := driver.HandleReadCommands(device.Name, protocolPropertiesFromConfig(device), reqs)
		if err != nil {
			logClient.Errorf("Telemetry read failed for device %s: %v", device.Name, err)
		} else if shouldEmitTelemetry(device.Telemetry, values, state, time.Now()) {
			updateTelemetryState(state, values, time.Now().UnixMilli())
			asyncValues := &contracts.AsyncValues{
				TraceID:     outevent.NewTraceID(device.Name),
				DeviceName:  device.Name,
				SourceName:  "telemetry",
				CollectedAt: time.Now().UnixMilli(),
				Values:      values,
			}
			select {
			case sdk.asyncCh <- asyncValues:
			default:
				logClient.Warnf("Async channel full; dropping telemetry for %s", device.Name)
			}
		}

		<-ticker.C
	}
}

func shouldEmitTelemetry(cfg contracts.TelemetryConfig, values []*contracts.CommandValue, state telemetryState, now time.Time) bool {
	if len(values) == 0 {
		return false
	}

	current := snapshotFromValues(values)
	if len(state.lastValues) == 0 && len(current) > 0 {
		return true
	}

	if !telemetryHasFilterStrategy(cfg) {
		return true
	}

	watched := watchedFieldSet(cfg.WatchedFields)
	for _, value := range values {
		pointCfg, hasPointCfg := findPointConfig(cfg, value.DeviceResourceName)
		lastValue, hasLast := state.lastValues[value.DeviceResourceName]
		if !hasLast {
			return true
		}

		if heartbeatDue(cfg, pointCfg, state.lastEmittedAt[value.DeviceResourceName], now) {
			return true
		}

		if pointCfg.Deadband > 0 {
			changed, comparable := exceedsDeadband(lastValue, value.Value, pointCfg.Deadband)
			if comparable {
				if changed {
					return true
				}
				continue
			}
		}

		onChange := cfg.OnChange
		if hasPointCfg && pointCfg.OnChange != nil {
			onChange = *pointCfg.OnChange
		}
		if !onChange {
			continue
		}

		if len(watched) > 0 && !(hasPointCfg && hasPointStrategy(pointCfg)) {
			if _, ok := watched[value.DeviceResourceName]; !ok {
				continue
			}
		}

		if !reflect.DeepEqual(lastValue, value.Value) {
			return true
		}
	}

	if len(cfg.WatchedFields) > 0 {
		for _, field := range cfg.WatchedFields {
			if !reflect.DeepEqual(state.lastValues[field], current[field]) {
				return true
			}
		}
	}
	return false
}

func snapshotFromValues(values []*contracts.CommandValue) map[string]interface{} {
	snapshot := make(map[string]interface{}, len(values))
	for _, value := range values {
		snapshot[value.DeviceResourceName] = value.Value
	}
	return snapshot
}

func updateTelemetryState(state telemetryState, values []*contracts.CommandValue, emittedAt int64) {
	for key := range state.lastValues {
		delete(state.lastValues, key)
	}
	for _, value := range values {
		state.lastValues[value.DeviceResourceName] = value.Value
		state.lastEmittedAt[value.DeviceResourceName] = emittedAt
	}
}

func telemetryHasFilterStrategy(cfg contracts.TelemetryConfig) bool {
	if cfg.OnChange || len(cfg.WatchedFields) > 0 || strings.TrimSpace(cfg.HeartbeatInterval) != "" {
		return true
	}
	for _, point := range cfg.Points {
		if hasPointStrategy(point) {
			return true
		}
	}
	return false
}

func hasPointStrategy(point contracts.PointConfig) bool {
	return point.OnChange != nil || point.Deadband > 0 || strings.TrimSpace(point.HeartbeatInterval) != ""
}

func watchedFieldSet(fields []string) map[string]struct{} {
	if len(fields) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		set[field] = struct{}{}
	}
	return set
}

func findPointConfig(cfg contracts.TelemetryConfig, name string) (contracts.PointConfig, bool) {
	for _, point := range cfg.Points {
		if point.Name == name {
			return point, true
		}
	}
	return contracts.PointConfig{}, false
}

func heartbeatDue(cfg contracts.TelemetryConfig, point contracts.PointConfig, lastEmittedAt int64, now time.Time) bool {
	interval := strings.TrimSpace(point.HeartbeatInterval)
	if interval == "" {
		interval = strings.TrimSpace(cfg.HeartbeatInterval)
	}
	if interval == "" || lastEmittedAt == 0 {
		return false
	}

	duration, err := time.ParseDuration(interval)
	if err != nil || duration <= 0 {
		return false
	}
	return now.UnixMilli()-lastEmittedAt >= duration.Milliseconds()
}

func exceedsDeadband(previous interface{}, current interface{}, deadband float64) (bool, bool) {
	if deadband <= 0 {
		return false, false
	}

	prev, okPrev := numericValue(previous)
	curr, okCurr := numericValue(current)
	if !okPrev || !okCurr {
		return false, false
	}
	return absFloat64(curr-prev) >= deadband, true
}

func numericValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func absFloat64(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func normalizeConfig(config Config) Config {
	config.Logging = effectiveLoggerConfig(config)
	if config.TelemetryPost.DataFormat == "" {
		config.TelemetryPost.DataFormat = "rule"
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

func normalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
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

func normalizeProfile(profile contracts.DeviceProfile) contracts.DeviceProfile {
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

func effectiveLogLevel(config Config) string {
	if config.Logging.Level != "" {
		return config.Logging.Level
	}
	if config.LogLevel != "" {
		return config.LogLevel
	}
	return "INFO"
}

func effectiveLoggerConfig(config Config) logger.Config {
	cfg := config.Logging
	if cfg.Level == "" {
		cfg.Level = effectiveLogLevel(config)
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

func stringsReplaceProductCode(template string, productCode string) string {
	return strings.ReplaceAll(template, "{productCode}", productCode)
}
