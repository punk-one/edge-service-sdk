package app

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

const (
	defaultStatusHeartbeatInterval = 30 * time.Second
	defaultPingInterval            = 15 * time.Second
	pingTimeout                    = 2 * time.Second
)

type deviceStatusPublisher struct {
	tracker           *rtstatus.Tracker
	sdk               *DeviceSDK
	publisher         mqtt.Publisher
	logClient         logger.LoggingClient
	heartbeatInterval time.Duration

	mu            sync.Mutex
	lastPublished map[string]publishedDeviceStatus

	pingCache    map[string]bool
	pingMu       sync.RWMutex
	pingInterval time.Duration
}

type publishedDeviceStatus struct {
	summary     statusSummary
	data        statusPayloadData
	publishedAt int64
}

type statusSummary struct {
	Online          bool
	ConnectionState string
	ErrorMessage    string
}

type statusMessage struct {
	DeviceCode string            `json:"device_code"`
	Time       int64             `json:"time"`
	Data       statusPayloadData `json:"data"`
}

type statusPayloadData struct {
	Online          bool                `json:"online"`
	ConnectionState string              `json:"connection_state"`
	LastSeenAt      int64               `json:"last_seen_at"`
	Error           *statusPayloadError `json:"error"`
}

type statusPayloadError struct {
	Message string `json:"message"`
	Time    int64  `json:"time"`
}

func newDeviceStatusPublisher(tracker *rtstatus.Tracker, sdk *DeviceSDK, publisher mqtt.Publisher, topicConfig mqtt.TopicConfig, logClient logger.LoggingClient) *deviceStatusPublisher {
	if tracker == nil || sdk == nil || publisher == nil || strings.TrimSpace(topicConfig.Topic) == "" {
		return nil
	}

	return &deviceStatusPublisher{
		tracker:           tracker,
		sdk:               sdk,
		publisher:         publisher,
		logClient:         logClient,
		heartbeatInterval: parseStatusHeartbeatInterval(topicConfig.HeartbeatInterval, logClient),
		lastPublished:     make(map[string]publishedDeviceStatus),
		pingCache:         make(map[string]bool),
		pingInterval:      defaultPingInterval,
	}
}

func (p *deviceStatusPublisher) Start() {
	if p == nil {
		return
	}

	p.tracker.SetOnChange(func(states []rtstatus.DeviceState) {
		p.publishSnapshot(states, false, time.Now().UnixMilli())
	})

	p.publishSnapshot(p.tracker.Snapshot(), true, time.Now().UnixMilli())
	go p.runHeartbeatLoop()
	go p.startPingLoop()
}

// StartHeartbeatOnly starts only the heartbeat loop (no OnChange registration).
// Used for per-group heartbeat in multi-group setups.
func (p *deviceStatusPublisher) StartHeartbeatOnly() {
	if p == nil || p.heartbeatInterval <= 0 {
		return
	}
	go p.runHeartbeatLoop()
}

// UpdateHeartbeatInterval updates the heartbeat interval at runtime.
// The new interval takes effect on the next heartbeat tick.
func (p *deviceStatusPublisher) UpdateHeartbeatInterval(interval time.Duration) {
	if p == nil || interval <= 0 {
		return
	}
	p.mu.Lock()
	p.heartbeatInterval = interval
	p.mu.Unlock()
}

func (p *deviceStatusPublisher) runHeartbeatLoop() {
	if p == nil || p.heartbeatInterval <= 0 {
		return
	}

	interval := time.Second
	if p.heartbeatInterval < interval {
		interval = p.heartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for now := range ticker.C {
		p.publishHeartbeat(p.tracker.Snapshot(), now.UnixMilli())
	}
}

func (p *deviceStatusPublisher) publishSnapshot(states []rtstatus.DeviceState, force bool, now int64) {
	if p == nil || len(states) == 0 {
		return
	}

	for _, state := range states {
		device, ok := p.sdk.DeviceConfigByName(state.DeviceCode)
		if !ok {
			continue
		}

		data := statusPayloadDataFromState(state, p.isOnline(state.DeviceCode))
		summary := statusSummaryFromData(data)

		p.mu.Lock()
		record, exists := p.lastPublished[state.DeviceCode]
		record.data = data
		record.summary = summary
		p.lastPublished[state.DeviceCode] = record
		p.mu.Unlock()

		if !force && exists && record.summary == summary {
			continue
		}

		if err := p.publishDeviceStatus(device, state.DeviceCode, data, now); err != nil {
			if p.logClient != nil {
				p.logClient.Warnf("Failed to publish status for device %s: %v", state.DeviceCode, err)
			}
			continue
		}

		p.mu.Lock()
		record = p.lastPublished[state.DeviceCode]
		record.publishedAt = now
		p.lastPublished[state.DeviceCode] = record
		p.mu.Unlock()
	}
}

func (p *deviceStatusPublisher) publishHeartbeat(states []rtstatus.DeviceState, now int64) {
	if p == nil || len(states) == 0 {
		return
	}

	for _, state := range states {
		device, ok := p.sdk.DeviceConfigByName(state.DeviceCode)
		if !ok {
			continue
		}

		data := statusPayloadDataFromState(state, p.isOnline(state.DeviceCode))
		summary := statusSummaryFromData(data)

		p.mu.Lock()
		record, exists := p.lastPublished[state.DeviceCode]
		if !exists {
			record = publishedDeviceStatus{}
		}
		record.data = data
		record.summary = summary
		due := forceStatusHeartbeat(record, now, p.heartbeatInterval)
		p.lastPublished[state.DeviceCode] = record
		p.mu.Unlock()

		if !due {
			continue
		}

		if err := p.publishDeviceStatus(device, state.DeviceCode, data, now); err != nil {
			if p.logClient != nil {
				p.logClient.Warnf("Failed to publish status heartbeat for device %s: %v", state.DeviceCode, err)
			}
			continue
		}

		p.mu.Lock()
		record = p.lastPublished[state.DeviceCode]
		record.publishedAt = now
		p.lastPublished[state.DeviceCode] = record
		p.mu.Unlock()
	}
}

func (p *deviceStatusPublisher) publishDeviceStatus(device contracts.DeviceConfig, deviceCode string, data statusPayloadData, now int64) error {
	message := statusMessage{
		DeviceCode: device.Name,
		Time:       now,
		Data:       data,
	}
	return p.publisher.PublishStatus(device, statusMessageToMap(message))
}

func statusPayloadDataFromState(state rtstatus.DeviceState, online bool) statusPayloadData {
	data := statusPayloadData{
		Online:          online,
		ConnectionState: state.ConnectionState,
		LastSeenAt:      state.LastSuccessAt,
	}
	if strings.TrimSpace(state.LastError) != "" {
		data.Error = &statusPayloadError{
			Message: state.LastError,
			Time:    state.LastErrorAt,
		}
	}
	return data
}

func statusSummaryFromData(data statusPayloadData) statusSummary {
	summary := statusSummary{
		Online:          data.Online,
		ConnectionState: data.ConnectionState,
	}
	if data.Error != nil {
		summary.ErrorMessage = data.Error.Message
	}
	return summary
}

func statusMessageToMap(message statusMessage) map[string]interface{} {
	data := map[string]interface{}{
		"online":           message.Data.Online,
		"connection_state": message.Data.ConnectionState,
		"last_seen_at":     message.Data.LastSeenAt,
		"error":            nil,
	}
	if message.Data.Error != nil {
		data["error"] = map[string]interface{}{
			"message": message.Data.Error.Message,
			"time":    message.Data.Error.Time,
		}
	}
	return map[string]interface{}{
		"device_code": message.DeviceCode,
		"time":        message.Time,
		"data":        data,
	}
}

func parseStatusHeartbeatInterval(raw string, logClient logger.LoggingClient) time.Duration {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultStatusHeartbeatInterval
	}

	duration, err := time.ParseDuration(trimmed)
	if err != nil || duration <= 0 {
		if logClient != nil {
			logClient.Warnf("Invalid statusReport.heartbeatInterval %q, using default %s", raw, defaultStatusHeartbeatInterval)
		}
		return defaultStatusHeartbeatInterval
	}
	return duration
}

func forceStatusHeartbeat(record publishedDeviceStatus, now int64, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	if record.publishedAt == 0 {
		return true
	}
	return now-record.publishedAt >= interval.Milliseconds()
}

func (p *deviceStatusPublisher) startPingLoop() {
	if p == nil || p.pingInterval <= 0 {
		return
	}

	ticker := time.NewTicker(p.pingInterval)
	defer ticker.Stop()

	p.pingAllDevices()

	for range ticker.C {
		p.pingAllDevices()
	}
}

func (p *deviceStatusPublisher) pingAllDevices() {
	if p == nil || p.sdk == nil {
		return
	}

	devices := p.sdk.deviceConfigs
	for _, device := range devices {
		addr := extractHostPort(device)
		if addr == "" {
			continue
		}
		reachable := pingTCP(addr, pingTimeout)

		p.pingMu.Lock()
		p.pingCache[device.Name] = reachable
		p.pingMu.Unlock()
	}
}

func (p *deviceStatusPublisher) isOnline(deviceCode string) bool {
	p.pingMu.RLock()
	online, ok := p.pingCache[deviceCode]
	p.pingMu.RUnlock()
	if !ok {
		return false
	}
	return online
}

func extractHostPort(device contracts.DeviceConfig) string {
	host, port := "", ""
	for _, proto := range device.Protocols {
		props, ok := proto.(map[string]interface{})
		if !ok {
			continue
		}
		h, _ := props["Host"].(string)
		if h == "" {
			continue
		}
		host = h
		p := "102"
		if v, ok := props["Port"]; ok {
			switch val := v.(type) {
			case int:
				if val > 0 {
					p = strconv.Itoa(val)
				}
			case float64:
				if val > 0 {
					p = strconv.Itoa(int(val))
				}
			case string:
				if val != "" {
					p = val
				}
			}
		}
		port = p
		break
	}
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, port)
}

func pingTCP(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
