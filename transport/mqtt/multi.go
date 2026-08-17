package mqtt

import (
	"fmt"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	events "github.com/punk-one/edge-service-sdk/event"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// NewPublisher creates a Publisher. When cfg.Groups is empty, it returns a
// plain MQTTPublisher (backward-compatible). When groups are present, it
// returns a MultiPublisher that fans out to all groups.
func NewPublisher(cfg MQTTConfig, telemetry, propertyResult, propertyReport, commandResult, statusReport TopicConfig, logClient logger.LoggingClient, eventReport ...TopicConfig) Publisher {
	if len(cfg.Groups) == 0 {
		return NewMQTTPublisher(cfg, telemetry, propertyResult, propertyReport, commandResult, statusReport, logClient, eventReport...)
	}
	return newMultiPublisher(cfg, telemetry, propertyResult, propertyReport, commandResult, statusReport, logClient, eventReport...)
}

// ─── MultiPublisher ───────────────────────────────────────────────────────────

type multiPublisher struct {
	groups []*groupPublisher
	logger logger.LoggingClient
}

func newMultiPublisher(cfg MQTTConfig, telemetry, propertyResult, propertyReport, commandResult, statusReport TopicConfig, logClient logger.LoggingClient, eventReport ...TopicConfig) *multiPublisher {
	base := cfg
	base.Groups = nil // prevent recursion

	var groups []*groupPublisher
	for _, gc := range cfg.Groups {
		groupDefaults := mergeGroupToConfig(gc, base)

		groupTelemetry := buildGroupTopics(telemetry, gc.TelemetryFormat)
		groupPropertyResult := buildGroupTopics(propertyResult, gc.PropertyResultFormat)
		groupPropertyReport := buildGroupTopics(propertyReport, gc.PropertyReportFormat)
		groupCommandResult := buildGroupTopics(commandResult, gc.CommandResultFormat)
		groupStatusReport := buildGroupTopics(statusReport, gc.StatusReportFormat)
		groupEventReport := buildGroupTopics(firstTopic(eventReport), "")

		heartbeatInterval := gc.HeartbeatInterval
		if heartbeatInterval == "" {
			heartbeatInterval = statusReport.HeartbeatInterval
		}

		brokerCfgs := buildGroupBrokerList(gc, groupDefaults)
		gp := newGroupPublisher(gc.Name, gc.Mode, brokerCfgs, groupTelemetry, groupPropertyResult, groupPropertyReport, groupCommandResult, groupStatusReport, groupEventReport, heartbeatInterval, logClient)
		groups = append(groups, gp)
	}

	return &multiPublisher{groups: groups, logger: logClient}
}

func (m *multiPublisher) GroupPublishers() []Publisher {
	result := make([]Publisher, len(m.groups))
	for i, g := range m.groups {
		result[i] = g
	}
	return result
}

func (m *multiPublisher) GroupStatusTopic(i int) TopicConfig {
	if i < 0 || i >= len(m.groups) {
		return TopicConfig{}
	}
	return m.groups[i].statusReport
}

func (m *multiPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishTelemetry(device, data); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishTelemetry failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishCommandValues(device, values); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishCommandValues failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishTelemetryEvent(event, replayed); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishTelemetryEvent failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func firstTopic(topics []TopicConfig) TopicConfig {
	if len(topics) == 0 {
		return TopicConfig{}
	}
	return topics[0]
}

func (m *multiPublisher) PublishEvent(event events.Event, replayed bool) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishEvent(event, replayed); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishEvent failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishPropertyResult(device, payload); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishPropertyResult failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishPropertyReport(device, payload); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishPropertyReport failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishCommandResult(device, payload); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishCommandResult failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishStatus(device, payload); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishStatus failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) Subscribe(topic string, qos byte, handler MessageHandler) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.Subscribe(topic, qos, handler); err != nil {
			m.logger.Warnf("[mqtt] group %s: Subscribe failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.PublishJSON(topic, qos, retain, payload); err != nil {
			m.logger.Warnf("[mqtt] group %s: PublishJSON failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiPublisher) HealthCheck() error {
	healthyCount := 0
	for _, g := range m.groups {
		if err := g.HealthCheck(); err != nil {
			m.logger.Warnf("[mqtt] group %s: HealthCheck: %v", g.name, err)
		} else {
			healthyCount++
		}
	}
	if healthyCount == 0 {
		return fmt.Errorf("all %d mqtt groups unhealthy", len(m.groups))
	}
	return nil
}

func (m *multiPublisher) Close() error {
	var lastErr error
	for _, g := range m.groups {
		if err := g.Close(); err != nil {
			m.logger.Warnf("[mqtt] group %s: Close failed: %v", g.name, err)
			lastErr = err
		}
	}
	return lastErr
}

// ─── groupPublisher ───────────────────────────────────────────────────────────

type groupPublisher struct {
	name              string
	mode              string
	brokers           []MQTTConfig
	telemetry         TopicConfig
	propertyResult    TopicConfig
	propertyReport    TopicConfig
	commandResult     TopicConfig
	statusReport      TopicConfig
	eventReport       TopicConfig
	heartbeatInterval string
	logger            logger.LoggingClient

	mu            sync.RWMutex
	active        *MQTTPublisher
	activeIndex   int
	subscriptions []subscriptionRecord
	stopCh        chan struct{}
	started       bool
}

type subscriptionRecord struct {
	topic   string
	qos     byte
	handler MessageHandler
}

func newGroupPublisher(name, mode string, brokers []MQTTConfig, telemetry, propertyResult, propertyReport, commandResult, statusReport, eventReport TopicConfig, heartbeatInterval string, logClient logger.LoggingClient) *groupPublisher {
	gp := &groupPublisher{
		name:              name,
		mode:              mode,
		brokers:           brokers,
		telemetry:         telemetry,
		propertyResult:    propertyResult,
		propertyReport:    propertyReport,
		commandResult:     commandResult,
		statusReport:      statusReport,
		eventReport:       eventReport,
		heartbeatInterval: heartbeatInterval,
		logger:            logClient,
		stopCh:            make(chan struct{}),
	}

	if len(brokers) == 0 {
		logClient.Warnf("[mqtt] group %s: no brokers configured", name)
		return gp
	}

	gp.activate()
	return gp
}

func (g *groupPublisher) activate() {
	if g.mode == "failover" && len(g.brokers) > 1 {
		g.activateFailover()
	} else {
		g.activateSingle()
	}
}

func (g *groupPublisher) activateSingle() {
	g.mu.Lock()
	cfg := g.brokers[0]
	g.mu.Unlock()

	p := NewMQTTPublisher(cfg, g.telemetry, g.propertyResult, g.propertyReport, g.commandResult, g.statusReport, g.logger, g.eventReport)

	g.mu.Lock()
	old := g.active
	g.active = p
	g.activeIndex = 0
	g.started = true
	subs := g.subscriptions
	g.mu.Unlock()

	if old != nil {
		old.Close()
	}

	// Replay subscriptions on the new publisher
	for _, s := range subs {
		if err := p.Subscribe(s.topic, s.qos, s.handler); err != nil {
			g.logger.Warnf("[mqtt] group %s: replay subscribe %s: %v", g.name, s.topic, err)
		}
	}
}

func (g *groupPublisher) activateFailover() {
	brokers := g.brokers
	activeIdx := -1
	var active *MQTTPublisher

	for i := 0; i < len(brokers); i++ {
		p := NewMQTTPublisher(brokers[i], g.telemetry, g.propertyResult, g.propertyReport, g.commandResult, g.statusReport, g.logger, g.eventReport)
		if err := p.HealthCheck(); err != nil {
			g.logger.Warnf("[mqtt] group %s: broker[%d] %s initial health check failed: %v", g.name, i, brokers[i].URL, err)
			p.Close()
			continue
		}
		activeIdx = i
		active = p
		g.logger.Infof("[mqtt] group %s: connected to broker[%d] %s", g.name, i, brokers[i].URL)
		break
	}

	g.mu.Lock()
	old := g.active
	g.active = active
	g.activeIndex = activeIdx
	g.started = true
	subs := g.subscriptions
	g.mu.Unlock()

	if old != nil {
		old.Close()
	}

	if active != nil {
		for _, s := range subs {
			if err := active.Subscribe(s.topic, s.qos, s.handler); err != nil {
				g.logger.Warnf("[mqtt] group %s: replay subscribe %s: %v", g.name, s.topic, err)
			}
		}
	} else {
		g.logger.Warnf("[mqtt] group %s: all %d brokers failed initial connect", g.name, len(brokers))
	}

	// Start background failover monitor
	go g.failoverMonitor()
}

func (g *groupPublisher) failoverMonitor() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			if g.isStopping() {
				return
			}

			g.mu.RLock()
			active := g.active
			g.mu.RUnlock()

			if active != nil && active.HealthCheck() == nil {
				continue
			}

			// Switch to next broker
			g.switchToNext()
		}
	}
}

func (g *groupPublisher) switchToNext() {
	g.mu.Lock()
	brokers := g.brokers
	idx := g.activeIndex
	old := g.active
	g.mu.Unlock()

	if old != nil {
		g.logger.Warnf("[mqtt] group %s: broker[%d] %s unhealthy, switching", g.name, idx, brokers[idx].URL)
		old.Close()
	}

	// Try all brokers in order
	for attempt := 0; ; attempt++ {
		if g.isStopping() {
			return
		}

		for i := 0; i < len(brokers); i++ {
			nextIdx := (idx + 1 + i) % len(brokers)
			cfg := brokers[nextIdx]

			p := NewMQTTPublisher(cfg, g.telemetry, g.propertyResult, g.propertyReport, g.commandResult, g.statusReport, g.logger, g.eventReport)
			if err := p.HealthCheck(); err != nil {
				g.logger.Warnf("[mqtt] group %s: broker[%d] %s health check failed: %v", g.name, nextIdx, cfg.URL, err)
				p.Close()
				continue
			}

			g.mu.Lock()
			g.active = p
			g.activeIndex = nextIdx
			subs := g.subscriptions
			g.mu.Unlock()

			g.logger.Infof("[mqtt] group %s: switched to broker[%d] %s", g.name, nextIdx, cfg.URL)

			for _, s := range subs {
				if err := p.Subscribe(s.topic, s.qos, s.handler); err != nil {
					g.logger.Warnf("[mqtt] group %s: replay subscribe %s: %v", g.name, s.topic, err)
				}
			}
			return
		}

		// All brokers failed, backoff and retry
		backoff := time.Duration(attempt+1) * 5 * time.Second
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
		g.logger.Warnf("[mqtt] group %s: all brokers failed, retrying in %s", g.name, backoff)

		select {
		case <-g.stopCh:
			return
		case <-time.After(backoff):
		}
	}
}

func (g *groupPublisher) getActive() *MQTTPublisher {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.active
}

func (g *groupPublisher) isStopping() bool {
	select {
	case <-g.stopCh:
		return true
	default:
		return false
	}
}

func (g *groupPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishTelemetry(device, data)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	if p := g.getActive(); p != nil {
		return p.PublishCommandValues(device, values)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	if p := g.getActive(); p != nil {
		return p.PublishTelemetryEvent(event, replayed)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishEvent(event events.Event, replayed bool) error {
	if p := g.getActive(); p != nil {
		return p.PublishEvent(event, replayed)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishPropertyResult(device, payload)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishPropertyReport(device, payload)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishCommandResult(device, payload)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishStatus(device, payload)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) Subscribe(topic string, qos byte, handler MessageHandler) error {
	g.mu.Lock()
	g.subscriptions = append(g.subscriptions, subscriptionRecord{topic: topic, qos: qos, handler: handler})
	g.mu.Unlock()

	if p := g.getActive(); p != nil {
		return p.Subscribe(topic, qos, handler)
	}
	return nil
}

func (g *groupPublisher) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	if p := g.getActive(); p != nil {
		return p.PublishJSON(topic, qos, retain, payload)
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) HealthCheck() error {
	if p := g.getActive(); p != nil {
		return p.HealthCheck()
	}
	return fmt.Errorf("group %s: no active publisher", g.name)
}

func (g *groupPublisher) Close() error {
	close(g.stopCh)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active != nil {
		return g.active.Close()
	}
	return nil
}

// ─── Merge helpers ────────────────────────────────────────────────────────────

// mergeGroupToConfig merges group-level connection fields into base MQTTConfig.
func mergeGroupToConfig(group MQTTGroupConfig, base MQTTConfig) MQTTConfig {
	result := base
	if group.URL != "" {
		result.URL = group.URL
	}
	if group.Username != "" {
		result.Username = group.Username
	}
	if group.Password != "" {
		result.Password = group.Password
	}
	if group.ClientId != "" {
		result.ClientId = group.ClientId
	}
	if group.KeepAliveSec > 0 {
		result.KeepAliveSec = group.KeepAliveSec
	}
	if group.PingTimeoutSec > 0 {
		result.PingTimeoutSec = group.PingTimeoutSec
	}
	if group.ConnectTimeoutSec > 0 {
		result.ConnectTimeoutSec = group.ConnectTimeoutSec
	}
	if group.PublishTimeoutSec > 0 {
		result.PublishTimeoutSec = group.PublishTimeoutSec
	}
	if group.HealthCheckIntervalSec > 0 {
		result.HealthCheckIntervalSec = group.HealthCheckIntervalSec
	}
	if group.InitialRetryIntervalMs > 0 {
		result.InitialRetryIntervalMs = group.InitialRetryIntervalMs
	}
	if group.MaxReconnectIntervalSec > 0 {
		result.MaxReconnectIntervalSec = group.MaxReconnectIntervalSec
	}
	if group.DisconnectQuiesceMs > 0 {
		result.DisconnectQuiesceMs = group.DisconnectQuiesceMs
	}
	if group.SkipTLSVerify != nil {
		result.SkipTLSVer = *group.SkipTLSVerify
	}
	if group.MTLS != nil {
		result.MTLS = *group.MTLS
	}
	if group.QOS > 0 {
		result.QoS = group.QOS
	}
	if group.Retain != nil {
		result.Retain = *group.Retain
	}
	if group.CACert != "" {
		result.CACert = group.CACert
	}
	if group.CAPath != "" {
		result.CAPath = group.CAPath
	}
	if group.CertPath != "" {
		result.CertPath = group.CertPath
	}
	if group.ClientCert != "" {
		result.ClientCert = group.ClientCert
	}
	if group.ClientKey != "" {
		result.ClientKey = group.ClientKey
	}
	if group.PrivKeyPath != "" {
		result.PrivKeyPath = group.PrivKeyPath
	}
	return result
}

// mergeBrokerConfig merges override into base. Non-zero override fields win.
func mergeBrokerConfig(base, override MQTTConfig) MQTTConfig {
	result := base
	if override.URL != "" {
		result.URL = override.URL
	}
	if override.Username != "" {
		result.Username = override.Username
	}
	if override.Password != "" {
		result.Password = override.Password
	}
	if override.ClientId != "" {
		result.ClientId = override.ClientId
	}
	if override.KeepAliveSec > 0 {
		result.KeepAliveSec = override.KeepAliveSec
	}
	if override.PingTimeoutSec > 0 {
		result.PingTimeoutSec = override.PingTimeoutSec
	}
	if override.ConnectTimeoutSec > 0 {
		result.ConnectTimeoutSec = override.ConnectTimeoutSec
	}
	if override.PublishTimeoutSec > 0 {
		result.PublishTimeoutSec = override.PublishTimeoutSec
	}
	if override.HealthCheckIntervalSec > 0 {
		result.HealthCheckIntervalSec = override.HealthCheckIntervalSec
	}
	if override.InitialRetryIntervalMs > 0 {
		result.InitialRetryIntervalMs = override.InitialRetryIntervalMs
	}
	if override.MaxReconnectIntervalSec > 0 {
		result.MaxReconnectIntervalSec = override.MaxReconnectIntervalSec
	}
	if override.DisconnectQuiesceMs > 0 {
		result.DisconnectQuiesceMs = override.DisconnectQuiesceMs
	}
	if override.SkipTLSVer {
		result.SkipTLSVer = override.SkipTLSVer
	}
	if override.MTLS {
		result.MTLS = override.MTLS
	}
	if override.QoS > 0 {
		result.QoS = override.QoS
	}
	if override.Retain {
		result.Retain = override.Retain
	}
	if override.CACert != "" {
		result.CACert = override.CACert
	}
	if override.CAPath != "" {
		result.CAPath = override.CAPath
	}
	if override.CertPath != "" {
		result.CertPath = override.CertPath
	}
	if override.ClientCert != "" {
		result.ClientCert = override.ClientCert
	}
	if override.ClientKey != "" {
		result.ClientKey = override.ClientKey
	}
	if override.PrivKeyPath != "" {
		result.PrivKeyPath = override.PrivKeyPath
	}
	// Clear Groups to prevent recursive nesting
	result.Groups = nil
	return result
}

// buildGroupBrokerList builds the final list of broker configs for a group.
// Group-level defaults are merged into each broker config.
func buildGroupBrokerList(group MQTTGroupConfig, groupDefaults MQTTConfig) []MQTTConfig {
	brokers := group.Brokers
	if len(brokers) == 0 {
		// No brokers specified: use group defaults as single broker
		return []MQTTConfig{groupDefaults}
	}
	result := make([]MQTTConfig, len(brokers))
	for i, b := range brokers {
		result[i] = mergeBrokerConfig(groupDefaults, b)
	}
	return result
}

// buildGroupTopics returns a copy of base with DataFormat overridden if format is non-empty.
func buildGroupTopics(base TopicConfig, format string) TopicConfig {
	result := base
	if format != "" {
		result.DataFormat = format
	}
	return result
}
