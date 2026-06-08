package app

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	cmdapi "github.com/punk-one/edge-service-sdk/command"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	rtcommand "github.com/punk-one/edge-service-sdk/runtime/command"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	dependency "github.com/punk-one/edge-service-sdk/runtime/dependency"
	rtproperty "github.com/punk-one/edge-service-sdk/runtime/property"
	supervisor "github.com/punk-one/edge-service-sdk/runtime/scheduler"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

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

func NewDeviceSDK(config rtconfig.Config, logClient logger.LoggingClient, tracker *rtstatus.Tracker) *DeviceSDK {
	if logClient == nil {
		logClient = logger.NewLogger("edge-device-service", rtconfig.EffectiveLoggerConfig(config))
	}
	asyncCh := make(chan *contracts.AsyncValues, 100)

	devices := make([]contracts.Device, 0, len(config.Devices))
	deviceConfigs := make(map[string]contracts.DeviceConfig, len(config.Devices))
	productDevices := make(map[string][]contracts.DeviceConfig)

	for _, deviceConfig := range config.Devices {
		deviceConfig = rtconfig.NormalizeDeviceConfig(deviceConfig)
		deviceConfigs[deviceConfig.Name] = deviceConfig
		productDevices[deviceConfig.ProductCode] = append(productDevices[deviceConfig.ProductCode], deviceConfig)
		if tracker != nil {
			tracker.RegisterDevice(deviceConfig.Name)
		}
		devices = append(devices, contracts.Device{
			Name:        deviceConfig.Name,
			ProductCode: deviceConfig.ProductCode,
			Protocols:   rtconfig.ProtocolPropertiesFromConfig(deviceConfig),
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

func (s *DeviceSDK) ProductCodes() []string {
	codes := make([]string, 0, len(s.productDevices))
	for code := range s.productDevices {
		codes = append(codes, code)
	}
	return codes
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

func validateCommandBindings(devices []contracts.DeviceConfig, registry cmdapi.Registry) error {
	for _, device := range devices {
		seen := map[string]struct{}{}
		for _, command := range device.Commands {
			identifier := strings.TrimSpace(command.Identifier)
			if identifier == "" {
				return fmt.Errorf("device %s profile %s has empty command identifier", strings.TrimSpace(device.Name), strings.TrimSpace(device.ProfileName))
			}
			if _, ok := seen[identifier]; ok {
				return fmt.Errorf("device %s profile %s declares duplicate command %q", strings.TrimSpace(device.Name), strings.TrimSpace(device.ProfileName), identifier)
			}
			seen[identifier] = struct{}{}
			if registry == nil {
				return fmt.Errorf("device %s profile %s declares command %q but no command registry is configured", strings.TrimSpace(device.Name), strings.TrimSpace(device.ProfileName), identifier)
			}
			if _, _, ok := registry.Lookup(identifier); !ok {
				return fmt.Errorf("device %s profile %s declares command %q but it is not registered", strings.TrimSpace(device.Name), strings.TrimSpace(device.ProfileName), identifier)
			}
		}
	}
	return nil
}

func Bootstrap(serviceName, version string, driver contracts.ProtocolDriver, registry cmdapi.Registry) {
	fmt.Printf("Starting %s version %s\n", serviceName, version)
	if registry == nil {
		registry = cmdapi.NewRegistry()
	}

	config, err := rtconfig.LoadConfig("./configs/config.yaml")
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}
	config = rtconfig.NormalizeConfig(config)

	logLevel := rtconfig.EffectiveLogLevel(config)
	logClient := logger.NewLogger(serviceName, rtconfig.EffectiveLoggerConfig(config))
	logCfg := rtconfig.EffectiveLoggerConfig(config)
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

	if err := validateCommandBindings(config.Devices, registry); err != nil {
		logClient.Errorf("Failed to validate command bindings: %v", err)
		return
	}

	publisher := mqtt.NewMQTTPublisher(config.MQTT, config.TelemetryReport, config.PropertyResult, config.PropertyReport, config.CommandResult, config.StatusReport, logClient)
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

	controlStore, err := rtcontrol.NewSQLiteStore(config.ControlStore.SQLitePath)
	if err != nil {
		logClient.Errorf("Failed to initialize control store: %v", err)
		return
	}
	defer func() {
		if closeErr := controlStore.Close(); closeErr != nil {
			logClient.Warnf("Failed to close control store: %v", closeErr)
		}
	}()
	propertyService := rtproperty.NewService(sdk, driver, publisher, controlStore, logClient)
	propertyService.RegisterMQTTHandlers(config)
	if err := propertyService.ResumePending(); err != nil {
		logClient.Warnf("Failed to resume pending property tasks: %v", err)
	}

	commandService := rtcommand.NewService(sdk, driver, publisher, controlStore, logClient, registry)
	commandService.RegisterMQTTHandlers(config)
	if err := commandService.ResumePending(); err != nil {
		logClient.Warnf("Failed to resume pending commands: %v", err)
	}

	queryService := newMQTTQueryService(sdk, registry, controlStore, publisher, logClient)
	queryService.RegisterMQTTHandlers(config)

	installStatusPublisher(statusTracker, sdk, publisher, config.StatusReport, logClient)

	go processAsyncValues(sdk, telemetrySink, logClient)

	super := supervisor.NewSupervisor(logClient, 5*time.Second)
	workerCount := 0
	for _, device := range config.Devices {
		device = rtconfig.NormalizeDeviceConfig(device)
		if len(device.Telemetry.Points) == 0 && len(device.Telemetry.Groups) == 0 {
			logClient.Warnf("Skipping device %s: no telemetry points or groups", device.Name)
			continue
		}
		workerCount++
		deviceCopy := device
		logClient.Infof(
			"Registering merged telemetry worker: device=%s product=%s connection_strategy=%s",
			deviceCopy.Name,
			deviceCopy.ProductCode,
			deviceCopy.ConnectionStrategy,
		)
		super.Start(deviceCopy.Name, func() error {
			return runMergedTelemetryWorker(driver, deviceCopy, sdk, logClient)
		})
	}
	if strings.TrimSpace(config.PropertyReport.Topic) != "" {
		for _, device := range config.Devices {
			device = rtconfig.NormalizeDeviceConfig(device)
			reqs, _, err := rtconfig.BuildAutoPropertyReadRequests(device)
			if err != nil {
				logClient.Warnf("Skipping property worker for device %s: invalid property config: %v", device.Name, err)
				continue
			}
			if len(reqs) == 0 || !propertyAutoReportingEnabled(device.Property) {
				continue
			}
			deviceCopy := device
			logClient.Infof(
				"Registering property worker: device=%s product=%s interval=%s points=%d",
				deviceCopy.Name,
				deviceCopy.ProductCode,
				strings.TrimSpace(deviceCopy.Property.Interval),
				len(reqs),
			)
			super.Start(deviceCopy.Name+"-property", func() error {
				return runPropertyWorker(driver, deviceCopy, publisher, logClient)
			})
		}
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
			return propertyService.ExecuteGet(req, "")
		},
		PropertySet: func(req rtapi.PropertyRequest) (rtapi.PropertySetResponse, int) {
			return propertyService.ExecuteSet(req, "")
		},
		CommandCall: func(identifier string, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int) {
			return commandService.Execute(identifier, req, "")
		},
		PropertyModelQuery:         buildPropertyModelQuery(sdk),
		TelemetryModelQuery:        buildTelemetryModelQuery(sdk),
		CommandListQuery:           buildCommandListQuery(sdk, registry),
		CommandDetailQuery:         buildCommandDetailQuery(sdk, registry),
		CommandInputQuery:          buildCommandInputQuery(sdk, registry),
		CommandOutputQuery:         buildCommandOutputQuery(sdk, registry),
		PropertyResultQuery:        buildPropertyResultQuery(controlStore),
		CommandResultQuery:         buildCommandResultQuery(controlStore),
		ControlJobListQuery:        buildControlJobListQuery(controlStore),
		ControlJobExportQuery:      buildControlJobExportQuery(controlStore),
		ControlJobDiagnosticsQuery: buildControlJobDiagnosticsQuery(controlStore),
		ControlJobQuery:            buildControlJobQuery(controlStore),
		ControlJobResultQuery:      buildControlJobResultQuery(controlStore),
		ControlJobEventsQuery:      buildControlJobEventsQuery(controlStore),
		Logger:                     logClient,
	})
	if httpRuntime.Enabled() {
		super.Start("http-runtime", func() error {
			return httpRuntime.Run()
		})
	} else {
		logClient.Infof("HTTP runtime server disabled: service.port=%d", config.Service.Port)
	}

	logClient.Infof("Device service %s started successfully with %d devices and %d telemetry workers", serviceName, len(config.Devices), workerCount)
	select {}
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

func runMergedTelemetryWorker(driver contracts.ProtocolDriver, device contracts.DeviceConfig, sdk *DeviceSDK, logClient logger.LoggingClient) error {
	gcdInterval, err := computeGCD(device.Telemetry)
	if err != nil {
		return fmt.Errorf("invalid telemetry interval for device %s: %w", device.Name, err)
	}

	reqs, devicePointNames, groupPointNames, err := buildAllRequests(device)
	if err != nil {
		return fmt.Errorf("invalid telemetry points for device %s: %w", device.Name, err)
	}
	if len(reqs) == 0 {
		return nil
	}

	hasDeviceLevel := len(devicePointNames) > 0
	var deviceLevelState telemetryState
	if hasDeviceLevel {
		deviceLevelState = telemetryState{
			lastValues:    make(map[string]interface{}),
			lastEmittedAt: make(map[string]int64),
		}
	}

	type groupWorkerState struct {
		cfg   contracts.TelemetryConfig
		names map[string]bool
		state telemetryState
	}
	var groupStates []groupWorkerState
	for _, group := range device.Telemetry.Groups {
		if len(group.Points) == 0 {
			continue
		}
		cfg := effectiveGroupConfig(device.Telemetry, group)
		names := groupPointNames[group.Name]
		interval := cfg.Interval
		if interval == "" {
			interval = "20s"
		}
		cfg.Interval = interval
		groupStates = append(groupStates, groupWorkerState{
			cfg:   cfg,
			names: names,
			state: telemetryState{
				lastValues:    make(map[string]interface{}),
				lastEmittedAt: make(map[string]int64),
			},
		})
	}

	deviceInterval := device.Telemetry.Interval
	if deviceInterval == "" {
		deviceInterval = "20s"
	}

	ticker := time.NewTicker(gcdInterval)
	defer ticker.Stop()

	tickCount := 0
	for {
		values, err := driver.HandleReadCommands(device.Name, rtconfig.ProtocolPropertiesFromConfig(device), reqs)
		if err != nil {
			logClient.Errorf("Telemetry read failed for device %s: %v", device.Name, err)
		} else {
			var mergedValues []*contracts.CommandValue
			now := time.Now()

			if hasDeviceLevel && isDue(deviceInterval, gcdInterval, tickCount) {
				deviceValues := filterValuesByNames(values, devicePointNames)
				if shouldEmitTelemetry(device.Telemetry, deviceValues, deviceLevelState, now) {
					updateTelemetryState(deviceLevelState, deviceValues, now.UnixMilli())
					mergedValues = append(mergedValues, deviceValues...)
				}
			}

			for _, gs := range groupStates {
				if isDue(gs.cfg.Interval, gcdInterval, tickCount) {
					groupValues := filterValuesByNames(values, gs.names)
					if shouldEmitTelemetry(gs.cfg, groupValues, gs.state, now) {
						updateTelemetryState(gs.state, groupValues, now.UnixMilli())
						mergedValues = append(mergedValues, groupValues...)
					}
				}
			}

			if len(mergedValues) > 0 {
				asyncValues := &contracts.AsyncValues{
					TraceID:     outevent.NewTraceID(device.Name),
					DeviceName:  device.Name,
					SourceName:  "telemetry",
					CollectedAt: now.UnixMilli(),
					Values:      mergedValues,
				}
				select {
				case sdk.asyncCh <- asyncValues:
				default:
					logClient.Warnf("Async channel full; dropping telemetry for %s", device.Name)
				}
			}
		}

		<-ticker.C
		tickCount++
	}
}

// computeGCD calculates the greatest common divisor of all telemetry intervals.
// Falls back to 20s if no intervals are configured.
func computeGCD(tc contracts.TelemetryConfig) (time.Duration, error) {
	var intervals []string
	if tc.Interval != "" {
		intervals = append(intervals, tc.Interval)
	}
	for _, group := range tc.Groups {
		interval := group.Interval
		if interval == "" {
			interval = tc.Interval
		}
		if interval != "" {
			intervals = append(intervals, interval)
		}
	}
	if len(intervals) == 0 {
		return 20 * time.Second, nil
	}

	var gcd time.Duration
	for _, raw := range intervals {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return 0, err
		}
		if gcd == 0 {
			gcd = d
		} else {
			gcd = gcdDuration(gcd, d)
		}
	}
	return gcd, nil
}

// gcdDuration computes the GCD of two durations in nanosecond precision.
func gcdDuration(a, b time.Duration) time.Duration {
	na, nb := a.Nanoseconds(), b.Nanoseconds()
	for nb != 0 {
		na, nb = nb, na%nb
	}
	return time.Duration(na)
}

// isDue checks whether the current tick should emit for the given interval.
func isDue(interval string, gcd time.Duration, tickCount int) bool {
	d, err := time.ParseDuration(interval)
	if err != nil {
		return false
	}
	skip := int(d / gcd)
	if skip <= 0 {
		skip = 1
	}
	return tickCount%skip == 0
}

// buildAllRequests builds deduplicated read requests for all telemetry points
// (device-level and groups) and returns their names organized by origin.
func buildAllRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, map[string]bool, map[string]map[string]bool, error) {
	seen := make(map[string]bool)
	var requests []contracts.CommandRequest
	devicePointNames := make(map[string]bool)
	groupPointNames := make(map[string]map[string]bool)

	for _, point := range device.Telemetry.Points {
		if seen[point.Name] {
			continue
		}
		seen[point.Name] = true
		req, err := point.ToCommandRequest(point.NodeName)
		if err != nil {
			return nil, nil, nil, err
		}
		requests = append(requests, req)
		devicePointNames[point.Name] = true
	}

	for _, group := range device.Telemetry.Groups {
		names := make(map[string]bool)
		for _, point := range group.Points {
			names[point.Name] = true
			if seen[point.Name] {
				continue
			}
			seen[point.Name] = true
			req, err := point.ToCommandRequest(point.NodeName)
			if err != nil {
				return nil, nil, nil, err
			}
			requests = append(requests, req)
		}
		if len(names) > 0 {
			groupPointNames[group.Name] = names
		}
	}

	return requests, devicePointNames, groupPointNames, nil
}

// filterValuesByNames filters a batch read result to only include values whose
// DeviceResourceName is in the given set.
func filterValuesByNames(values []*contracts.CommandValue, names map[string]bool) []*contracts.CommandValue {
	if len(names) == 0 {
		return nil
	}
	filtered := make([]*contracts.CommandValue, 0, len(names))
	for _, v := range values {
		if names[v.DeviceResourceName] {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func runPropertyWorker(driver contracts.ProtocolDriver, device contracts.DeviceConfig, publisher mqtt.Publisher, logClient logger.LoggingClient) error {
	duration, err := parsePropertyInterval(device.Property.Interval)
	if err != nil {
		return fmt.Errorf("invalid property interval %s for device %s: %w", device.Property.Interval, device.Name, err)
	}

	reqs, bindings, err := rtconfig.BuildAutoPropertyReadRequests(device)
	if err != nil {
		return fmt.Errorf("invalid property points for device %s: %w", device.Name, err)
	}
	if len(reqs) == 0 {
		return nil
	}

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	state := telemetryState{
		lastValues:    make(map[string]interface{}),
		lastEmittedAt: make(map[string]int64),
	}
	for {
		values, err := driver.HandleReadCommands(device.Name, rtconfig.ProtocolPropertiesFromConfig(device), reqs)
		if err != nil {
			logClient.Errorf("Property read failed for device %s: %v", device.Name, err)
		} else if shouldEmitProperty(device.Property, values, state, time.Now()) {
			now := time.Now().UnixMilli()
			updateTelemetryState(state, values, now)
			_ = publisher.PublishPropertyReport(device, map[string]interface{}{
				"device_code": device.Name,
				"time":        now,
				"data":        rtconfig.BuildPropertyResponse(values, bindings),
			})
		}

		<-ticker.C
	}
}

func buildRuntimeReadiness(authService *rtauth.Service, publisher mqtt.Publisher) func() error {
	return func() error {
		if authService != nil {
			if err := authService.HealthCheck(); err != nil {
				return err
			}
		}
		if publisher != nil {
			return publisher.HealthCheck()
		}
		return nil
	}
}

func installStatusPublisher(tracker *rtstatus.Tracker, sdk *DeviceSDK, publisher mqtt.Publisher, topicConfig mqtt.TopicConfig, logClient logger.LoggingClient) {
	reporter := newDeviceStatusPublisher(tracker, sdk, publisher, topicConfig, logClient)
	if reporter == nil {
		return
	}
	reporter.Start()
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

		deadbandMatched := false
		if pointCfg.Deadband > 0 {
			changed, comparable := exceedsDeadband(lastValue, value.Value, pointCfg.Deadband)
			if comparable {
				deadbandMatched = true
				if changed {
					return true
				}
			}
		}
		if pointCfg.DeadbandPercent > 0 {
			changed, comparable := exceedsPercentDeadband(lastValue, value.Value, pointCfg.DeadbandPercent)
			if comparable {
				deadbandMatched = true
				if changed {
					return true
				}
			}
		}
		if deadbandMatched {
			continue
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
	for _, group := range cfg.Groups {
		if group.OnChange || len(group.WatchedFields) > 0 || strings.TrimSpace(group.HeartbeatInterval) != "" {
			return true
		}
		for _, point := range group.Points {
			if hasPointStrategy(point) {
				return true
			}
		}
	}
	return false
}

func effectiveGroupConfig(device contracts.TelemetryConfig, group contracts.TelemetryGroup) contracts.TelemetryConfig {
	cfg := contracts.TelemetryConfig{
		Interval: group.Interval,
		Points:   group.Points,
	}

	if cfg.Interval == "" {
		cfg.Interval = device.Interval
	}

	if group.OnChange || (strings.TrimSpace(group.HeartbeatInterval) != "") || len(group.WatchedFields) > 0 {
		cfg.OnChange = group.OnChange
		cfg.HeartbeatInterval = group.HeartbeatInterval
		cfg.WatchedFields = group.WatchedFields
	} else {
		cfg.OnChange = device.OnChange
		cfg.HeartbeatInterval = device.HeartbeatInterval
		cfg.WatchedFields = device.WatchedFields
	}

	return cfg
}

func hasPointStrategy(point contracts.PointConfig) bool {
	return point.OnChange != nil || point.Deadband > 0 || point.DeadbandPercent > 0 || strings.TrimSpace(point.HeartbeatInterval) != ""
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

func propertyAutoReportingEnabled(cfg contracts.PropertyConfig) bool {
	duration, err := parsePropertyInterval(cfg.Interval)
	if err != nil || duration <= 0 {
		return false
	}
	return true
}

func shouldEmitProperty(cfg contracts.PropertyConfig, values []*contracts.CommandValue, state telemetryState, now time.Time) bool {
	if len(values) == 0 {
		return false
	}

	current := snapshotFromValues(values)
	if len(state.lastValues) == 0 && len(current) > 0 {
		return true
	}

	if !propertyHasFilterStrategy(cfg) {
		return true
	}

	watched := watchedFieldSet(cfg.WatchedFields)
	for _, value := range values {
		pointCfg, hasPointCfg := findPropertyPointConfig(cfg, value.DeviceResourceName)
		lastValue, hasLast := state.lastValues[value.DeviceResourceName]
		if !hasLast {
			return true
		}

		if propertyHeartbeatDue(cfg, pointCfg, state.lastEmittedAt[value.DeviceResourceName], now) {
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

func propertyHasFilterStrategy(cfg contracts.PropertyConfig) bool {
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

func findPropertyPointConfig(cfg contracts.PropertyConfig, name string) (contracts.PointConfig, bool) {
	for _, point := range cfg.Points {
		if point.Name == name {
			return point, true
		}
	}
	return contracts.PointConfig{}, false
}

func propertyHeartbeatDue(cfg contracts.PropertyConfig, point contracts.PointConfig, lastEmittedAt int64, now time.Time) bool {
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

func parsePropertyInterval(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}

	duration, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, nil
	}
	return duration, nil
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

func exceedsPercentDeadband(previous interface{}, current interface{}, percent float64) (bool, bool) {
	if percent <= 0 {
		return false, false
	}

	prev, okPrev := numericValue(previous)
	curr, okCurr := numericValue(current)
	if !okPrev || !okCurr {
		return false, false
	}

	if prev == 0 {
		return curr != 0, true
	}

	pctChange := absFloat64((curr - prev) / prev * 100)
	return pctChange >= percent, true
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
