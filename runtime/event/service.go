package eventruntime

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	coreevent "github.com/punk-one/edge-service-sdk/event"
	logger "github.com/punk-one/edge-service-sdk/logging"
	appconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

// Service owns the SDK EVENT runtime for all explicitly bound devices.
type Service struct {
	engine     *coreevent.Engine
	dispatcher *reliable.EventDispatcher
	stateStore StateStore
	log        logger.LoggingClient

	mu        sync.Mutex
	started   bool
	closed    bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	processMu sync.Mutex
}

// NewService creates an event runtime. An empty eventDir or a device set with
// no eventProfile is a valid disabled configuration for old services.
func NewService(config appconfig.Config, publisher interface{}, logClient logger.LoggingClient) (*Service, error) {
	config = appconfig.NormalizeConfig(config)
	eventDir := strings.TrimSpace(config.Device.EventDir)
	if eventDir == "" {
		return nil, nil
	}
	profiles, err := coreevent.LoadProfiles(eventDir)
	if err != nil {
		return nil, err
	}
	bindings := make([]coreevent.DeviceBinding, 0)
	for _, device := range config.Devices {
		device = appconfig.NormalizeDeviceConfig(device)
		if strings.TrimSpace(device.EventProfile) == "" {
			continue
		}
		profile, ok := coreevent.SelectProfile(profiles, device.EventProfile)
		if !ok {
			return nil, fmt.Errorf("device %s references unknown eventProfile %s", device.Name, device.EventProfile)
		}
		bindings = append(bindings, coreevent.DeviceBinding{Device: device, Profile: profile})
	}
	if len(bindings) == 0 {
		return nil, nil
	}
	engine, err := coreevent.NewEngine(coreevent.EngineOptions{Bindings: bindings, Logger: logClient})
	if err != nil {
		return nil, err
	}

	statePath := filepath.Join(filepath.Dir(config.Storage.SQLitePath), "event-state.json")
	stateStore := NewFileStateStore(statePath)
	state, err := stateStore.Load()
	if err != nil {
		return nil, err
	}
	engine.ImportState(state)

	service := &Service{
		engine:     engine,
		stateStore: stateStore,
		log:        logClient,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	if strings.TrimSpace(config.EventReport.Topic) != "" {
		eventPublisher, ok := publisher.(mqtt.EventPublisher)
		if !ok {
			return nil, fmt.Errorf("eventReport is configured but MQTT publisher does not support events")
		}
		queueConfig := config.ReliableQueue
		// EVENT delivery always uses the durable outbox. The telemetry queue
		// switch must not reintroduce a process-memory-only loss window for
		// event lifecycle records.
		queueConfig.Enabled = true
		queueConfig.SQLitePath = config.Storage.SQLitePath
		dispatcher, err := reliable.NewEventDispatcher(queueConfig, eventPublisher, logClient)
		if err != nil {
			return nil, err
		}
		service.dispatcher = dispatcher
	}
	return service, nil
}

func (s *Service) Start() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go s.flushLoop()
}

func (s *Service) ObserveTelemetry(deviceCode string, collectedAt int64, values []*contracts.CommandValue) error {
	if s == nil || s.engine == nil {
		return nil
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	events, err := s.engine.ObserveTelemetry(deviceCode, collectedAt, values)
	if err != nil {
		return err
	}
	return s.publishAndPersist(events)
}

func (s *Service) ObserveProperty(deviceCode string, observedAt int64, values map[string]interface{}) error {
	if s == nil || s.engine == nil {
		return nil
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	events, err := s.engine.ObserveProperty(deviceCode, observedAt, values)
	if err != nil {
		return err
	}
	return s.publishAndPersist(events)
}

func (s *Service) ObserveConnection(observation coreevent.ConnectionObservation) error {
	if s == nil || s.engine == nil {
		return nil
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	events, err := s.engine.ObserveConnection(observation)
	if err != nil {
		return err
	}
	return s.publishAndPersist(events)
}

func (s *Service) Flush(now int64) error {
	if s == nil || s.engine == nil {
		return nil
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	events := s.engine.Flush(now)
	return s.publishAndPersist(events)
}

func (s *Service) SaveState() error {
	if s == nil || s.engine == nil || s.stateStore == nil {
		return nil
	}
	s.processMu.Lock()
	defer s.processMu.Unlock()
	return s.saveState()
}

func (s *Service) saveState() error {
	if s == nil || s.engine == nil || s.stateStore == nil {
		return nil
	}
	return s.stateStore.Save(s.engine.ExportState())
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	started := s.started
	if started {
		select {
		case <-s.stopCh:
		default:
			close(s.stopCh)
		}
	}
	s.mu.Unlock()
	if started {
		<-s.doneCh
	}
	s.processMu.Lock()
	var flushErr error
	if s.engine != nil {
		flushErr = s.publishAndPersist(s.engine.FlushFinal(time.Now().UnixMilli()))
	}
	stateErr := s.saveState()
	if stateErr == nil {
		stateErr = flushErr
	}
	s.processMu.Unlock()
	if s.dispatcher != nil {
		if err := s.dispatcher.Close(); stateErr == nil && err != nil {
			stateErr = err
		}
	}
	return stateErr
}

func (s *Service) flushLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(s.doneCh)
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			if err := s.Flush(now.UnixMilli()); err != nil && s.log != nil {
				s.log.Warnf("EVENT summary flush failed: %v", err)
			}
		}
	}
}

func (s *Service) publishAndPersist(items []coreevent.Event) error {
	for _, item := range items {
		if s.dispatcher != nil {
			if err := s.dispatcher.Publish(item); err != nil {
				return err
			}
		} else if s.log != nil {
			s.log.Infof("EVENT generated without eventReport: device=%s category=%s event=%s event_type=%s instance=%s", item.DeviceCode, item.Data.Category, item.Data.EventIdentifier, item.Data.EventType, item.Data.EventInstanceID)
		}
		if s.log != nil {
			s.log.Debugf("EVENT processed: device=%s category=%s event=%s event_type=%s instance=%s time=%d", item.DeviceCode, item.Data.Category, item.Data.EventIdentifier, item.Data.EventType, item.Data.EventInstanceID, item.Time)
		}
	}
	return s.saveState()
}
