package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	cmdapi "github.com/punk-one/edge-service-sdk/command"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	coreevent "github.com/punk-one/edge-service-sdk/event"
	"github.com/punk-one/edge-service-sdk/file"
	logger "github.com/punk-one/edge-service-sdk/logging"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	processapi "github.com/punk-one/edge-service-sdk/process"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"
	rtcommand "github.com/punk-one/edge-service-sdk/runtime/command"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	dependency "github.com/punk-one/edge-service-sdk/runtime/dependency"
	eventruntime "github.com/punk-one/edge-service-sdk/runtime/event"
	"github.com/punk-one/edge-service-sdk/runtime/ops"
	"github.com/punk-one/edge-service-sdk/runtime/ops/configsvc"
	"github.com/punk-one/edge-service-sdk/runtime/ops/logsvc"
	runtimeprocess "github.com/punk-one/edge-service-sdk/runtime/process"
	rtproperty "github.com/punk-one/edge-service-sdk/runtime/property"
	supervisor "github.com/punk-one/edge-service-sdk/runtime/scheduler"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

// ErrRestartRequested is returned by Run when an authenticated operations
// request asks the external service supervisor to restart the process.
var ErrRestartRequested = errors.New("service restart requested")

const restartExitCode = 75

type DeviceSDK struct {
	logger         logger.LoggingClient
	telemetrySink  reliable.TelemetrySink
	devices        []contracts.Device
	deviceConfigs  map[string]contracts.DeviceConfig
	productDevices map[string][]contracts.DeviceConfig
	nameIndex      map[string]string
	statusTracker  *rtstatus.Tracker
	eventService   *eventruntime.Service
	shuttingDown   atomic.Bool
	driverFaultMu  sync.RWMutex
	driverFault    func(deviceName, operation string, err error)

	// Ops services
	configService   *configsvc.ConfigService
	logSearcher     *logsvc.LogSearcher
	restarter       *ops.Restarter
	statusPublisher *deviceStatusPublisher
}

type telemetryState struct {
	lastValues    map[string]interface{}
	lastEmittedAt map[string]int64
}

// Ops services exposed for app-layer command access.
// These are set during Bootstrap() and safe to read after Bootstrap() completes.
var (
	ConfigService *configsvc.ConfigService
	LogSearcher   *logsvc.LogSearcher
	Restarter     *ops.Restarter
)

func NewDeviceSDK(config rtconfig.Config, logClient logger.LoggingClient, tracker *rtstatus.Tracker) *DeviceSDK {
	if logClient == nil {
		logClient = logger.NewLogger("edge-device-service", rtconfig.EffectiveLoggerConfig(config))
	}
	devices := make([]contracts.Device, 0, len(config.Devices))
	deviceConfigs := make(map[string]contracts.DeviceConfig, len(config.Devices))
	productDevices := make(map[string][]contracts.DeviceConfig)
	nameIndex := make(map[string]string)

	for _, deviceConfig := range config.Devices {
		deviceConfig = rtconfig.NormalizeDeviceConfig(deviceConfig)
		deviceConfigs[deviceConfig.InternalName] = deviceConfig
		productDevices[deviceConfig.ProductCode] = append(productDevices[deviceConfig.ProductCode], deviceConfig)
		if deviceConfig.InternalName != deviceConfig.Name {
			nameIndex[deviceConfig.Name] = deviceConfig.InternalName
		}
		if tracker != nil {
			tracker.RegisterDevice(deviceConfig.InternalName)
		}
		devices = append(devices, contracts.Device{
			Name:        deviceConfig.InternalName,
			ProductCode: deviceConfig.ProductCode,
			Protocols:   rtconfig.ProtocolPropertiesFromConfig(deviceConfig),
		})
	}

	return &DeviceSDK{
		logger:         logClient,
		devices:        devices,
		deviceConfigs:  deviceConfigs,
		productDevices: productDevices,
		nameIndex:      nameIndex,
		statusTracker:  tracker,
	}
}

func (s *DeviceSDK) LoggingClient() logger.LoggingClient {
	return s.logger
}

func (s *DeviceSDK) ReportAsyncValues(asyncValues *contracts.AsyncValues) error {
	if s == nil {
		return fmt.Errorf("device SDK is nil")
	}
	if asyncValues == nil {
		return nil
	}
	if s.isShuttingDown() {
		return fmt.Errorf("device SDK is shutting down")
	}
	if s.telemetrySink == nil {
		return fmt.Errorf("telemetry outbox is not initialized")
	}
	device, ok := s.DeviceConfigByName(asyncValues.DeviceName)
	if !ok {
		return fmt.Errorf("unknown telemetry device %s", asyncValues.DeviceName)
	}
	// SQLite COMMIT is the telemetry acceptance boundary. EVENT observation is
	// intentionally after persistence so it cannot delay or prevent durability.
	if err := s.telemetrySink.PublishAsyncValues(device, asyncValues); err != nil {
		return err
	}
	if s.eventService != nil {
		if err := s.eventService.ObserveTelemetry(device.InternalName, asyncValues.CollectedAt, asyncValues.Values); err != nil && s.logger != nil {
			s.logger.Warnf("Failed to process EVENT telemetry for %s: %v", asyncValues.DeviceName, err)
		}
	}
	return nil
}

func (s *DeviceSDK) Devices() []contracts.Device {
	return s.devices
}

func (s *DeviceSDK) DeviceConfigByName(name string) (contracts.DeviceConfig, bool) {
	device, ok := s.deviceConfigs[name]
	if ok {
		return device, true
	}
	if resolved, exists := s.nameIndex[name]; exists {
		device, ok = s.deviceConfigs[resolved]
		return device, ok
	}
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
	s.DeviceConnectedAt(deviceName, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceDisconnected(deviceName string, err error) {
	s.DeviceDisconnectedAt(deviceName, err, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceConnectedAt(deviceName string, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkConnected(deviceName)
	}
	s.observeConnection(deviceName, true, "connected", observedAt, 0, "")
}

func (s *DeviceSDK) DeviceReadSucceeded(deviceName string) {
	s.DeviceReadSucceededAt(deviceName, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceDisconnectedAt(deviceName string, err error, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkDisconnected(deviceName, err)
	}
	s.observeConnection(deviceName, false, "disconnected", observedAt, 0, errorString(err))
}

func (s *DeviceSDK) DeviceReadSucceededAt(deviceName string, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkReadSuccess(deviceName)
	}
	s.observeConnection(deviceName, true, "connected", observedAt, observedAt, "")
}

func (s *DeviceSDK) DeviceReadFailed(deviceName string, err error) {
	s.DeviceReadFailedAt(deviceName, err, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceReadFailedAt(deviceName string, err error, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkReadError(deviceName, err)
	}
	s.observeConnection(deviceName, false, "degraded", observedAt, 0, errorString(err))
}

func (s *DeviceSDK) DeviceWriteSucceeded(deviceName string) {
	s.DeviceWriteSucceededAt(deviceName, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceWriteSucceededAt(deviceName string, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkWriteSuccess(deviceName)
	}
	s.observeConnection(deviceName, true, "connected", observedAt, observedAt, "")
}

func (s *DeviceSDK) DeviceWriteFailed(deviceName string, err error) {
	s.DeviceWriteFailedAt(deviceName, err, time.Now().UnixMilli())
}

func (s *DeviceSDK) DeviceWriteFailedAt(deviceName string, err error, observedAt int64) {
	if s.statusTracker != nil {
		s.statusTracker.MarkWriteError(deviceName, err)
	}
	s.observeConnection(deviceName, false, "degraded", observedAt, 0, errorString(err))
}

// SetDriverFaultHandler installs the process-level recovery action for a
// driver call that remains blocked after its deadline.
func (s *DeviceSDK) SetDriverFaultHandler(handler func(deviceName, operation string, err error)) {
	if s == nil {
		return
	}
	s.driverFaultMu.Lock()
	s.driverFault = handler
	s.driverFaultMu.Unlock()
}

// ReportDriverFault requests recovery only for stuck calls. Ordinary protocol
// errors remain device-local and continue using their normal retry policy.
func (s *DeviceSDK) ReportDriverFault(deviceName, operation string, err error) {
	if s == nil || !errors.Is(err, contracts.ErrOperationStuck) || s.isShuttingDown() {
		return
	}
	s.driverFaultMu.RLock()
	handler := s.driverFault
	s.driverFaultMu.RUnlock()
	if handler != nil {
		handler(deviceName, operation, err)
	}
}

func (s *DeviceSDK) SetEventService(service *eventruntime.Service) {
	s.eventService = service
}

func (s *DeviceSDK) EventService() *eventruntime.Service {
	if s == nil {
		return nil
	}
	return s.eventService
}

func (s *DeviceSDK) ObserveEventProperty(deviceName string, observedAt int64, values map[string]interface{}) error {
	if s == nil || s.eventService == nil || s.isShuttingDown() {
		return nil
	}
	return s.eventService.ObserveProperty(deviceName, observedAt, values)
}

func (s *DeviceSDK) observeConnection(deviceName string, online bool, state string, observedAt, lastSeenAt int64, errMessage string) {
	if s == nil || s.eventService == nil || s.isShuttingDown() {
		return
	}
	if observedAt == 0 {
		observedAt = time.Now().UnixMilli()
	}
	if lastSeenAt == 0 && online {
		lastSeenAt = observedAt
	}
	if err := s.eventService.ObserveConnection(coreevent.ConnectionObservation{DeviceCode: deviceName, Online: online, State: state, ObservedAt: observedAt, LastSeenAt: lastSeenAt, Error: errMessage, Known: true}); err != nil && s.logger != nil {
		s.logger.Warnf("EVENT connection observation failed for device %s: %v", deviceName, err)
	}
}

func (s *DeviceSDK) isShuttingDown() bool {
	return s == nil || s.shuttingDown.Load()
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ConfigService returns the runtime configuration service.
func (s *DeviceSDK) ConfigService() *configsvc.ConfigService { return s.configService }

// LogSearcher returns the log search service.
func (s *DeviceSDK) LogSearcher() *logsvc.LogSearcher { return s.logSearcher }

// Restarter returns the service restarter.
func (s *DeviceSDK) Restarter() *ops.Restarter { return s.restarter }

// UpdateHeartbeatInterval updates the status report heartbeat interval at runtime.
func (s *DeviceSDK) UpdateHeartbeatInterval(interval time.Duration) {
	if s.statusPublisher != nil {
		s.statusPublisher.UpdateHeartbeatInterval(interval)
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

type BootstrapOptions struct {
	CommandRegistry cmdapi.Registry
	ProcessRegistry processapi.Registry
}

// Bootstrap preserves the original SDK entry point for every deployed
// application that does not define custom processors.
func Bootstrap(serviceName, version string, driver contracts.ProtocolDriver, registry cmdapi.Registry) {
	BootstrapWithOptions(serviceName, version, driver, BootstrapOptions{CommandRegistry: registry})
}

func BootstrapWithOptions(serviceName, version string, driver contracts.ProtocolDriver, options BootstrapOptions) {
	if err := RunWithOptions(serviceName, version, driver, options); err != nil {
		fmt.Fprintf(os.Stderr, "%s stopped: %v\n", serviceName, err)
		if errors.Is(err, ErrRestartRequested) {
			os.Exit(restartExitCode)
		}
		os.Exit(1)
	}
}

// Run executes the service lifecycle and propagates startup or runtime errors
// to callers that own their process exit policy.
func Run(serviceName, version string, driver contracts.ProtocolDriver, registry cmdapi.Registry) error {
	return RunWithOptions(serviceName, version, driver, BootstrapOptions{CommandRegistry: registry})
}

// RunWithOptions executes the complete service lifecycle. Unlike the legacy
// Bootstrap wrappers, it never terminates the process directly.
func RunWithOptions(serviceName, version string, driver contracts.ProtocolDriver, options BootstrapOptions) error {
	fmt.Printf("Starting %s version %s\n", serviceName, version)
	registry := options.CommandRegistry
	if registry == nil {
		registry = cmdapi.NewRegistry()
	}

	config, err := rtconfig.LoadConfig("./configs/config.yaml")
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
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
		return fmt.Errorf("validate command bindings: %w", err)
	}

	busService, busErr := runtimebus.Start(serviceName, config.NATSBus, logClient)
	if busErr != nil {
		// The bus is an optional side path. A failure must never prevent the
		// deployed MQTT/SQLite/device path from starting.
		logClient.Errorf("Optional JetStream bus is degraded and will be disabled: %v", busErr)
		busService = nil
	}
	configuredProcessCount := runtimeprocess.ConfiguredProcessCount(config.Devices)
	if busService != nil {
		defer busService.Close()
	}

	basePublisher := mqtt.NewPublisher(config.MQTT, config.TelemetryReport, config.PropertyResult, config.PropertyReport, config.CommandResult, config.StatusReport, logClient, config.EventReport)
	publisher, err := mqtt.NewDurablePublisher(basePublisher, mqtt.DurablePublisherConfig{
		SQLitePath:       config.TelemetryOutbox.SQLitePath,
		MaxDatabaseBytes: config.TelemetryOutbox.MaxDatabaseBytes,
		RetryInitial:     time.Duration(config.TelemetryOutbox.RetryInitialMs) * time.Millisecond,
		RetryMax:         time.Duration(config.TelemetryOutbox.RetryMaxMs) * time.Millisecond,
	}, logClient)
	if err != nil {
		_ = basePublisher.Close()
		return fmt.Errorf("initialize durable MQTT publisher: %w", err)
	}
	var publisherCloseOnce sync.Once
	closePublisher := func() {
		publisherCloseOnce.Do(func() {
			if err := publisher.Close(); err != nil {
				logClient.Warnf("Failed to close MQTT publisher cleanly: %v", err)
			}
		})
	}
	defer closePublisher()
	var mqttObserver *runtimebus.MQTTObserver
	if busService != nil {
		mqttObserver = runtimebus.NewMQTTObserver(busService, logClient)
		if !mqtt.AttachObserver(publisher, mqttObserver) {
			logClient.Warnf("MQTT publisher does not expose observation hooks; JetStream mirroring is disabled")
			mqttObserver.Close()
			mqttObserver = nil
		}
	}
	if mqttObserver != nil {
		defer mqttObserver.Close()
	}
	telemetryTransport, ok := publisher.(reliable.TelemetryTransport)
	if !ok {
		return fmt.Errorf("MQTT publisher does not preserve telemetry outbox send_at")
	}
	telemetrySink, err := reliable.NewTelemetryDispatcher(config.TelemetryOutbox, telemetryTransport, logClient)
	if err != nil {
		return fmt.Errorf("initialize telemetry outbox: %w", err)
	}
	var telemetryCloseOnce sync.Once
	closeTelemetry := func() {
		telemetryCloseOnce.Do(func() {
			if err := telemetrySink.Close(); err != nil {
				logClient.Warnf("Failed to close telemetry outbox cleanly: %v", err)
			}
		})
	}
	defer closeTelemetry()

	statusTracker := rtstatus.NewTracker()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:       config.Storage.SQLitePath,
		MaxDatabaseBytes: config.Storage.MaxDatabaseBytes,
		KeyFile:          config.Auth.KeyFile,
		BootstrapToken:   config.Auth.BootstrapToken,
		AccessTokenTTL:   time.Duration(config.Auth.AccessTokenTTLMin) * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("initialize auth service: %w", err)
	}
	defer func() {
		if err := authService.Close(); err != nil {
			logClient.Warnf("Failed to close auth service cleanly: %v", err)
		}
	}()

	sdk := NewDeviceSDK(config, logClient, statusTracker)
	sdk.telemetrySink = telemetrySink
	eventService, err := eventruntime.NewService(config, publisher, logClient)
	if err != nil {
		return fmt.Errorf("initialize EVENT service: %w", err)
	}
	sdk.SetEventService(eventService)
	if eventService != nil {
		eventService.Start()
	}
	var eventCloseOnce sync.Once
	closeEvent := func() {
		eventCloseOnce.Do(func() {
			if eventService != nil {
				if err := eventService.Close(); err != nil {
					logClient.Warnf("Failed to close EVENT service cleanly: %v", err)
				}
			}
		})
	}
	defer closeEvent()
	if err := driver.Initialize(sdk); err != nil {
		return fmt.Errorf("initialize driver: %w", err)
	}
	var driverStopOnce sync.Once
	stopDriver := func() {
		driverStopOnce.Do(func() {
			done := make(chan error, 1)
			go func() { done <- driver.Stop(false) }()
			select {
			case err := <-done:
				if err != nil {
					logClient.Warnf("Failed to stop driver cleanly: %v", err)
				}
			case <-time.After(5 * time.Second):
				logClient.Errorf("Driver Stop exceeded 5s; continuing process shutdown")
			}
		})
	}
	defer stopDriver()

	// Initialize ops services
	logDir := filepath.Dir(config.Logging.File)
	if logDir == "" || logDir == "." {
		logDir = "."
	}
	configService := configsvc.NewConfigService("./configs", config.Devices, nil)
	logSearcher := logsvc.NewLogSearcher(logDir)
	restartCh := make(chan ops.RestartMode, 1)
	requestRestart := func(mode ops.RestartMode) func() error {
		return func() error {
			select {
			case restartCh <- mode:
				logClient.Infof("Graceful %s restart requested for %s", mode, serviceName)
				return nil
			default:
				return fmt.Errorf("restart is already in progress")
			}
		}
	}
	restarter := ops.NewRestarterWithHooks(serviceName, requestRestart(ops.SoftRestart), requestRestart(ops.HardRestart))
	sdk.SetDriverFaultHandler(func(deviceName, operation string, fault error) {
		logClient.Errorf("Driver watchdog requesting hard restart: device=%s operation=%s err=%v", deviceName, operation, fault)
		select {
		case restartCh <- ops.HardRestart:
		default:
		}
	})

	// Wire config change callback for hot-reload
	configService.SetOnChange(func(change configsvc.ConfigChange) {
		if change.Scope == "config" && change.ConfigPath == "statusReport.heartbeatInterval" {
			if s, ok := change.NewValue.(string); ok {
				if d, err := time.ParseDuration(s); err == nil && d > 0 {
					sdk.UpdateHeartbeatInterval(d)
					logClient.Infof("Hot-reloaded heartbeat interval to %s", s)
				}
			}
		}
	})

	sdk.configService = configService
	sdk.logSearcher = logSearcher
	sdk.restarter = restarter

	// Expose for app-layer command access
	ConfigService = configService
	LogSearcher = logSearcher
	Restarter = restarter

	dependencyManager := dependency.NewDependencyManager(logClient)
	dependencyManager.Register(dependency.NamedDependency("driver", func() error { return nil }))
	dependencyManager.Register(dependency.NamedDependency("auth", authService.HealthCheck))
	if err := dependencyManager.CheckAll(); err != nil {
		return fmt.Errorf("dependency check: %w", err)
	}
	if err := publisher.HealthCheck(); err != nil {
		// MQTT is recoverable: continue collecting into telemetry_outbox and let
		// the publisher reconnect in the background. Readiness remains degraded.
		logClient.Warnf("MQTT unavailable at startup; continuing with SQLite outbox until reconnect: %v", err)
	} else {
		logClient.Infof("Dependency ready: mqtt")
	}

	controlStore, err := rtcontrol.NewSQLiteStoreWithRetentionAndCapacity(config.ControlStore.SQLitePath, config.ControlStore.RetentionDays, config.ControlStore.MaxDatabaseBytes)
	if err != nil {
		return fmt.Errorf("initialize control store: %w", err)
	}
	defer func() {
		if closeErr := controlStore.Close(); closeErr != nil {
			logClient.Warnf("Failed to close control store: %v", closeErr)
		}
	}()
	propertyService := rtproperty.NewService(sdk, driver, publisher, controlStore, logClient)
	propertyService.RegisterMQTTHandlers(config)
	if err := propertyService.ResumeResultDeliveries(); err != nil {
		logClient.Warnf("Failed to resume pending property result deliveries: %v", err)
	}
	if err := propertyService.ResumePending(); err != nil {
		logClient.Warnf("Failed to resume pending property tasks: %v", err)
	}

	fileClient := file.NewClient()
	commandService := rtcommand.NewService(sdk, driver, publisher, controlStore, logClient, registry, fileClient)
	commandService.RegisterMQTTHandlers(config)
	if err := commandService.ResumeResultDeliveries(); err != nil {
		logClient.Warnf("Failed to resume pending command result deliveries: %v", err)
	}
	if err := commandService.ResumePending(); err != nil {
		logClient.Warnf("Failed to resume pending commands: %v", err)
	}

	queryService := newMQTTQueryService(sdk, registry, controlStore, publisher, logClient)
	queryService.RegisterMQTTHandlers(config)

	if busService != nil {
		if err := installJetStreamRoutes(busService, sdk, publisher, config, propertyService, commandService, logClient); err != nil {
			logClient.Errorf("Some optional JetStream routes were not installed: %v", err)
		}
		processRunner := runtimeprocess.NewRunner(config.Device.ProcessDir, config.Devices, options.ProcessRegistry, busService, logClient)
		started, err := processRunner.Start()
		if err != nil {
			logClient.Errorf("Some optional processes were not started: %v", err)
		}
		if configuredProcessCount > 0 {
			logClient.Infof("Process runtime initialized: configured=%d started=%d", configuredProcessCount, started)
		}
	} else if configuredProcessCount > 0 {
		logClient.Warnf("Processes are configured but disabled because the optional JetStream bus is unavailable")
	}

	sdk.statusPublisher = installStatusPublisher(statusTracker, sdk, publisher, config.StatusReport, logClient)

	super := supervisor.NewSupervisor(logClient, 5*time.Second)
	workerCount := 0
	for _, device := range config.Devices {
		device = rtconfig.NormalizeDeviceConfig(device)
		if len(device.Telemetry.Points) == 0 && len(device.Telemetry.Groups) == 0 {
			logClient.Warnf("Skipping device %s: no telemetry points or groups", device.InternalName)
			continue
		}
		workerCount++
		deviceCopy := device
		logClient.Infof(
			"Registering merged telemetry worker: device=%s product=%s connection_strategy=%s",
			deviceCopy.InternalName,
			deviceCopy.ProductCode,
			deviceCopy.ConnectionStrategy,
		)
		super.Start(deviceCopy.InternalName, func() error {
			return runMergedTelemetryWorker(super.Context(), driver, deviceCopy, sdk, logClient)
		})
	}
	if strings.TrimSpace(config.PropertyReport.Topic) != "" {
		for _, device := range config.Devices {
			device = rtconfig.NormalizeDeviceConfig(device)
			reqs, _, err := rtconfig.BuildAutoPropertyReadRequests(device)
			if err != nil {
				logClient.Warnf("Skipping property worker for device %s: invalid property config: %v", device.InternalName, err)
				continue
			}
			if len(reqs) == 0 || !propertyAutoReportingEnabled(device.Property) {
				continue
			}
			deviceCopy := device
			logClient.Infof(
				"Registering property worker: device=%s product=%s interval=%s points=%d",
				deviceCopy.InternalName,
				deviceCopy.ProductCode,
				strings.TrimSpace(deviceCopy.Property.Interval),
				len(reqs),
			)
			super.Start(deviceCopy.InternalName+"-property", func() error {
				return runPropertyWorker(super.Context(), driver, deviceCopy, publisher, sdk, logClient)
			})
		}
	}

	httpRuntime := httpserver.New(httpserver.Config{
		ServiceName:            serviceName,
		Version:                version,
		Host:                   config.Service.Host,
		Port:                   config.Service.Port,
		PortEnd:                config.Service.PortEnd,
		StartupMsg:             config.Service.StartupMsg,
		ServiceType:            config.Service.Type,
		StartedAt:              time.Now(),
		DeviceCount:            len(config.Devices),
		TelemetryWorkerCount:   workerCount,
		TelemetryOutboxEnabled: strings.TrimSpace(config.TelemetryReport.Topic) != "",
		Readiness:              buildRuntimeReadiness(authService, publisher, telemetrySink, controlStore, eventService),
		TelemetryOutboxStats: func() (reliable.TelemetryOutboxStats, error) {
			stats, statsErr := telemetrySink.Stats()
			if statsErr != nil {
				return stats, statsErr
			}
			if durable, ok := publisher.(interface {
				DurableQueueStats() (mqtt.DurableQueueStats, error)
			}); ok {
				mqttStats, mqttErr := durable.DurableQueueStats()
				if mqttErr != nil {
					return stats, mqttErr
				}
				stats.MQTTPendingCount = mqttStats.PendingCount
				stats.MQTTOldestAgeMs = mqttStats.OldestPendingAgeMs
				stats.MQTTDeadLetterCount = mqttStats.DeadLetterCount
				stats.MQTTPendingByGroup = mqttStats.PerDestination
			}
			return stats, nil
		},
		DeviceStates: statusTracker.Snapshot,
		AuthService:  authService,
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

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(shutdownCh)
	var restartMode ops.RestartMode
	select {
	case sig := <-shutdownCh:
		logClient.Infof("Shutdown signal received: %s", sig)
	case restartMode = <-restartCh:
		logClient.Infof("Restart request accepted: mode=%s", restartMode)
	}

	// Stop accepting control requests first, then stop device production and
	// flush durable queues before closing transports.
	sdk.shuttingDown.Store(true)
	super.Cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	if err := httpRuntime.Shutdown(shutdownCtx); err != nil {
		logClient.Warnf("Failed to stop HTTP runtime cleanly: %v", err)
	}
	cancelShutdown()
	if sdk.statusPublisher != nil {
		sdk.statusPublisher.Close()
	}
	commandService.BeginShutdown()
	propertyService.BeginShutdown()
	stopDriver()
	controlStopCtx, cancelControlStop := context.WithTimeout(context.Background(), 15*time.Second)
	if err := commandService.Close(controlStopCtx); err != nil {
		logClient.Warnf("Active command tasks did not stop before deadline: %v", err)
	}
	if err := propertyService.Close(controlStopCtx); err != nil {
		logClient.Warnf("Active property tasks did not stop before deadline: %v", err)
	}
	cancelControlStop()
	workerStopCtx, cancelWorkerStop := context.WithTimeout(context.Background(), 15*time.Second)
	workerStopErr := super.Stop(workerStopCtx)
	cancelWorkerStop()
	if workerStopErr != nil {
		logClient.Warnf("Workers did not stop before deadline: %v", workerStopErr)
	}
	closeEvent()
	closeTelemetry()
	closePublisher()
	if restartMode != "" {
		return fmt.Errorf("%w: mode=%s", ErrRestartRequested, restartMode)
	}
	if workerStopErr != nil {
		return fmt.Errorf("stop workers: %w", workerStopErr)
	}
	return nil
}

func runMergedTelemetryWorker(ctx context.Context, driver contracts.ProtocolDriver, device contracts.DeviceConfig, sdk *DeviceSDK, logClient logger.LoggingClient) error {
	gcdInterval, err := computeGCD(device.Telemetry)
	if err != nil {
		return fmt.Errorf("invalid telemetry interval for device %s: %w", device.InternalName, err)
	}

	_, devicePointNames, groupPointNames, err := buildAllRequests(device)
	if err != nil {
		return fmt.Errorf("invalid telemetry points for device %s: %w", device.InternalName, err)
	}

	// Build telemetry struct read requests (device-level and group-level).
	deviceStructReqs, deviceStructBindings, err := appconfig.BuildTelemetryStructReadRequests(device.Telemetry.Structs)
	if err != nil {
		return fmt.Errorf("invalid device telemetry structs for device %s: %w", device.InternalName, err)
	}

	hasDeviceLevel := len(devicePointNames) > 0 || len(deviceStructReqs) > 0
	var deviceLevelState telemetryState
	if hasDeviceLevel {
		deviceLevelState = telemetryState{
			lastValues:    make(map[string]interface{}),
			lastEmittedAt: make(map[string]int64),
		}
	}

	type groupWorkerState struct {
		cfg            contracts.TelemetryConfig
		names          map[string]bool
		reqs           []contracts.CommandRequest
		structReqs     []contracts.CommandRequest
		structBindings []appconfig.PropertyBinding
		state          telemetryState
	}
	var groupStates []groupWorkerState
	for _, group := range device.Telemetry.Groups {
		if len(group.Points) == 0 && len(group.Structs) == 0 {
			continue
		}
		cfg := effectiveGroupConfig(device.Telemetry, group)
		names := groupPointNames[group.Name]
		interval := cfg.Interval
		if interval == "" {
			interval = "20s"
		}
		cfg.Interval = interval
		groupReqs, err := buildGroupRequests(group)
		if err != nil {
			return fmt.Errorf("invalid group %s telemetry points: %w", group.Name, err)
		}
		groupStructReqs, groupStructBindings, err := appconfig.BuildTelemetryStructReadRequests(group.Structs)
		if err != nil {
			return fmt.Errorf("invalid group %s telemetry structs: %w", group.Name, err)
		}
		groupStates = append(groupStates, groupWorkerState{
			cfg:            cfg,
			names:          names,
			reqs:           groupReqs,
			structReqs:     groupStructReqs,
			structBindings: groupStructBindings,
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

	// Build device-level read requests
	var deviceReqs []contracts.CommandRequest
	if len(devicePointNames) > 0 {
		for _, point := range device.Telemetry.Points {
			req, err := point.ToCommandRequest(point.NodeName)
			if err != nil {
				return fmt.Errorf("invalid device telemetry point %s: %w", point.Name, err)
			}
			deviceReqs = append(deviceReqs, req)
		}
	}

	if !hasDeviceLevel && len(groupStates) == 0 {
		return nil
	}

	startTime := time.Now()
	ticker := time.NewTicker(gcdInterval)
	defer ticker.Stop()
	isFirstTick := true

	for {
		now := time.Now()
		elapsed := now.Sub(startTime)

		// Phase 1: 收集到期 group 的读请求
		var dueReqs []contracts.CommandRequest
		deviceStructReqCount := 0
		groupStructReqCounts := make([]int, len(groupStates))

		if hasDeviceLevel && (isFirstTick || isDueWallClock(deviceInterval, gcdInterval, elapsed, false)) {
			dueReqs = append(dueReqs, deviceReqs...)
			dueReqs = append(dueReqs, deviceStructReqs...)
			deviceStructReqCount = len(deviceStructReqs)
		}
		groupStructReqCounts = make([]int, len(groupStates))
		for i := range groupStates {
			if isFirstTick || isDueWallClock(groupStates[i].cfg.Interval, gcdInterval, elapsed, false) {
				dueReqs = append(dueReqs, groupStates[i].reqs...)
				dueReqs = append(dueReqs, groupStates[i].structReqs...)
				groupStructReqCounts[i] = len(groupStates[i].structReqs)
			}
		}

		// Phase 2: 读
		if len(dueReqs) > 0 {
			operationCtx, cancelOperation := context.WithTimeout(ctx, contracts.DefaultOperationTimeout)
			values, err := contracts.HandleReadCommandsWithContext(operationCtx, driver, device.InternalName, rtconfig.ProtocolPropertiesFromConfig(device), dueReqs)
			cancelOperation()
			if err != nil {
				sdk.DeviceReadFailedAt(device.InternalName, err, now.UnixMilli())
				sdk.ReportDriverFault(device.InternalName, "telemetry-read", err)
				logClient.Errorf("Telemetry read failed for device %s: %v", device.InternalName, err)
			} else {
				sdk.DeviceReadSucceededAt(device.InternalName, now.UnixMilli())
				// Phase 3: 评估上报
				var mergedValues []*contracts.CommandValue

				// Process device-level values
				if hasDeviceLevel && (isFirstTick || isDueWallClock(deviceInterval, gcdInterval, elapsed, false)) {
					devicePointCount := len(deviceReqs)
					pointValues := values[:min(devicePointCount, len(values))]
					structStart := min(devicePointCount, len(values))
					structEnd := min(devicePointCount+deviceStructReqCount, len(values))
					structValues := values[structStart:structEnd]
					remaining := values[min(structEnd, len(values)):]
					if len(pointValues) < devicePointCount || len(structValues) < deviceStructReqCount {
						logClient.Warnf("Device %s: read returned fewer values (%d) than expected (points=%d, structs=%d)",
							device.InternalName, len(values), devicePointCount, deviceStructReqCount)
					}

					deviceValues := filterValuesByNames(pointValues, devicePointNames)
					if shouldEmitTelemetry(device.Telemetry, deviceValues, deviceLevelState, now) {
						updateTelemetryState(deviceLevelState, deviceValues, now.UnixMilli())
						mergedValues = append(mergedValues, deviceValues...)

						// Assemble device-level struct values
						if len(structValues) > 0 && len(deviceStructBindings) > 0 {
							structMaps := appconfig.BuildPropertyResponse(structValues, deviceStructBindings)
							for structName, structVal := range structMaps {
								cv, err := contracts.NewCommandValue(structName, "Object", structVal)
								if err != nil {
									logClient.Warnf("Failed to create struct command value %s: %v", structName, err)
									continue
								}
								mergedValues = append(mergedValues, cv)
							}
						}
					}

					values = remaining
				}

				// Process group-level values
				for i := range groupStates {
					if isFirstTick || isDueWallClock(groupStates[i].cfg.Interval, gcdInterval, elapsed, false) {
						groupPointCount := len(groupStates[i].reqs)
						pointValues := values[:min(groupPointCount, len(values))]
						structStart := min(groupPointCount, len(values))
						structEnd := min(groupPointCount+groupStructReqCounts[i], len(values))
						structValues := values[structStart:structEnd]
						values = values[min(structEnd, len(values)):]
						if len(pointValues) < groupPointCount || len(structValues) < groupStructReqCounts[i] {
							logClient.Warnf("Device %s group %s: read returned fewer values (%d) than expected",
								device.InternalName, groupStates[i].cfg.Interval, len(values))
						}

						groupValues := filterValuesByNames(pointValues, groupStates[i].names)
						if shouldEmitTelemetry(groupStates[i].cfg, groupValues, groupStates[i].state, now) {
							updateTelemetryState(groupStates[i].state, groupValues, now.UnixMilli())
							mergedValues = append(mergedValues, groupValues...)

							// Assemble group-level struct values
							if len(structValues) > 0 && len(groupStates[i].structBindings) > 0 {
								structMaps := appconfig.BuildPropertyResponse(structValues, groupStates[i].structBindings)
								for structName, structVal := range structMaps {
									cv, err := contracts.NewCommandValue(structName, "Object", structVal)
									if err != nil {
										logClient.Warnf("Failed to create group struct command value %s: %v", structName, err)
										continue
									}
									mergedValues = append(mergedValues, cv)
								}
							}
						}
					}
				}

				// Phase 4: 发送
				if len(mergedValues) > 0 {
					asyncValues := &contracts.AsyncValues{
						TraceID:     outevent.NewTraceID(device.InternalName),
						DeviceName:  device.InternalName,
						SourceName:  "telemetry",
						CollectedAt: now.UnixMilli(),
						Values:      mergedValues,
					}
					if err := sdk.ReportAsyncValues(asyncValues); err != nil {
						return fmt.Errorf("persist telemetry for device %s: %w", device.InternalName, err)
					}
				}
			}
		}

		isFirstTick = false
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
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
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("interval %q must be a positive duration", raw)
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

// isDueWallClock 用 wall clock elapsed 判断 interval 边界是否被跨越。
func isDueWallClock(interval string, gcd time.Duration, elapsed time.Duration, isFirstTick bool) bool {
	if isFirstTick {
		return true
	}
	d, err := time.ParseDuration(interval)
	if err != nil || d <= 0 {
		return false
	}
	currentSlot := elapsed.Nanoseconds() / d.Nanoseconds()
	prevSlot := (elapsed - gcd).Nanoseconds() / d.Nanoseconds()
	return currentSlot != prevSlot
}

// buildGroupRequests builds read requests for all points in a telemetry group.
func buildGroupRequests(group contracts.TelemetryGroup) ([]contracts.CommandRequest, error) {
	reqs := make([]contracts.CommandRequest, 0, len(group.Points))
	readFirstSet := make(map[string]bool, len(group.ReadFirstFields))
	watchedSet := make(map[string]bool, len(group.WatchedFields))
	for _, f := range group.ReadFirstFields {
		readFirstSet[f] = true
	}
	for _, f := range group.WatchedFields {
		watchedSet[f] = true
	}
	for _, point := range group.Points {
		req, err := point.ToCommandRequest(point.NodeName)
		if err != nil {
			return nil, err
		}
		if readFirstSet[point.Name] {
			if req.Attributes == nil {
				req.Attributes = make(map[string]interface{})
			}
			req.Attributes["readFirstField"] = true
		}
		if watchedSet[point.Name] {
			if req.Attributes == nil {
				req.Attributes = make(map[string]interface{})
			}
			req.Attributes["watchedField"] = true
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
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
		if v == nil {
			continue // skip nil placeholders
		}
		if names[v.DeviceResourceName] {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func runPropertyWorker(ctx context.Context, driver contracts.ProtocolDriver, device contracts.DeviceConfig, publisher mqtt.Publisher, sdk *DeviceSDK, logClient logger.LoggingClient) error {
	duration, err := parsePropertyInterval(device.Property.Interval)
	if err != nil {
		return fmt.Errorf("invalid property interval %s for device %s: %w", device.Property.Interval, device.InternalName, err)
	}

	reqs, bindings, err := rtconfig.BuildAutoPropertyReadRequests(device)
	if err != nil {
		return fmt.Errorf("invalid property points for device %s: %w", device.InternalName, err)
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
		operationCtx, cancelOperation := context.WithTimeout(ctx, contracts.DefaultOperationTimeout)
		values, err := contracts.HandleReadCommandsWithContext(operationCtx, driver, device.InternalName, rtconfig.ProtocolPropertiesFromConfig(device), reqs)
		cancelOperation()
		if err != nil {
			if sdk != nil {
				sdk.DeviceReadFailedAt(device.InternalName, err, time.Now().UnixMilli())
				sdk.ReportDriverFault(device.InternalName, "property-report-read", err)
			}
			logClient.Errorf("Property read failed for device %s: %v", device.InternalName, err)
		} else {
			now := time.Now()
			if sdk != nil {
				sdk.DeviceReadSucceededAt(device.InternalName, now.UnixMilli())
			}
			if !shouldEmitProperty(device.Property, values, state, now) {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
				continue
			}
			observedAt := now.UnixMilli()
			traceID := outevent.NewTraceID(device.InternalName)
			updateTelemetryState(state, values, observedAt)
			propertyData := appconfig.BuildPropertyResponse(values, bindings)
			if sdk != nil && sdk.eventService != nil {
				if eventErr := sdk.eventService.ObserveProperty(device.InternalName, observedAt, propertyData); eventErr != nil {
					logClient.Warnf("Failed to process EVENT property values for %s: %v", device.InternalName, eventErr)
				}
			}
			if err := publisher.PublishPropertyReport(device, map[string]interface{}{
				"trace_id":    traceID,
				"device_code": device.Name,
				"time":        observedAt,
				"data":        propertyData,
			}); err != nil {
				logClient.Warnf("Failed to publish property report for device %s: %v", device.InternalName, err)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func buildRuntimeReadiness(authService *rtauth.Service, publisher mqtt.Publisher, dependencies ...interface{}) func() error {
	return func() error {
		if authService != nil {
			if err := authService.HealthCheck(); err != nil {
				return err
			}
		}
		if publisher != nil {
			if err := publisher.HealthCheck(); err != nil {
				return err
			}
		}
		for _, dependency := range dependencies {
			if checker, ok := dependency.(interface{ HealthCheck() error }); ok {
				if err := checker.HealthCheck(); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func installStatusPublisher(tracker *rtstatus.Tracker, sdk *DeviceSDK, publisher mqtt.Publisher, topicConfig mqtt.TopicConfig, logClient logger.LoggingClient) *deviceStatusPublisher {
	// Multi-group: snapshot via MultiPublisher, heartbeat per group
	if mg, ok := publisher.(mqtt.MultiGroupPublisher); ok {
		groups := mg.GroupPublishers()
		if len(groups) == 0 {
			// The durable wrapper also implements the optional interface so it can
			// transparently forward real groups. An empty slice denotes a single
			// MQTT destination and retains the original heartbeat behavior.
			reporter := newDeviceStatusPublisher(tracker, sdk, publisher, topicConfig, logClient)
			if reporter != nil {
				reporter.Start()
			}
			return reporter
		}
		main := newDeviceStatusPublisher(tracker, sdk, publisher, topicConfig, logClient)
		if main == nil {
			return nil
		}
		main.StartChangeOnly()

		for i, gp := range groups {
			groupTopic := mg.GroupStatusTopic(i)
			gsp := newDeviceStatusPublisher(tracker, sdk, gp, groupTopic, logClient)
			if gsp != nil {
				gsp.StartHeartbeatOnly()
				main.children = append(main.children, gsp)
			}
		}
		return main
	}

	// Single publisher: existing behavior
	reporter := newDeviceStatusPublisher(tracker, sdk, publisher, topicConfig, logClient)
	if reporter == nil {
		return nil
	}
	reporter.Start()
	return reporter
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
		if value == nil {
			continue
		}
		snapshot[value.DeviceResourceName] = value.Value
	}
	return snapshot
}

func updateTelemetryState(state telemetryState, values []*contracts.CommandValue, emittedAt int64) {
	for key := range state.lastValues {
		delete(state.lastValues, key)
	}
	for _, value := range values {
		if value == nil {
			continue
		}
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
