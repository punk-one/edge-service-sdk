package app

import (
	"strings"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

const defaultStatusHeartbeatInterval = 30 * time.Second

type deviceStatusPublisher struct {
	tracker           *rtstatus.Tracker
	sdk               *DeviceSDK
	publisher         mqtt.Publisher
	logClient         logger.LoggingClient
	heartbeatInterval time.Duration

	mu            sync.Mutex
	lastPublished map[string]publishedDeviceStatus
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
	DeviceCode string            `json:"deviceCode"`
	Time       int64             `json:"time"`
	Data       statusPayloadData `json:"data"`
}

type statusPayloadData struct {
	Online          bool                `json:"online"`
	ConnectionState string              `json:"connectionState"`
	LastSeenAt      int64               `json:"lastSeenAt"`
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

		data := statusPayloadDataFromState(state)
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

		data := statusPayloadDataFromState(state)
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
		DeviceCode: deviceCode,
		Time:       now,
		Data:       data,
	}
	return p.publisher.PublishStatus(device, statusMessageToMap(message))
}

func statusPayloadDataFromState(state rtstatus.DeviceState) statusPayloadData {
	data := statusPayloadData{
		Online:          state.ConnectionState == rtstatus.StateConnected,
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
		"online":          message.Data.Online,
		"connectionState": message.Data.ConnectionState,
		"lastSeenAt":      message.Data.LastSeenAt,
		"error":           nil,
	}
	if message.Data.Error != nil {
		data["error"] = map[string]interface{}{
			"message": message.Data.Error.Message,
			"time":    message.Data.Error.Time,
		}
	}
	return map[string]interface{}{
		"deviceCode": message.DeviceCode,
		"time":       message.Time,
		"data":       data,
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
