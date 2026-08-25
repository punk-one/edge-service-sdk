package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	events "github.com/punk-one/edge-service-sdk/event"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// NewMQTTPublisher creates a new MQTT publisher.
func NewMQTTPublisher(config MQTTConfig, telemetry TopicConfig, propertyResult TopicConfig, propertyReport TopicConfig, commandResult TopicConfig, statusReport TopicConfig, logger logger.LoggingClient, eventReport ...TopicConfig) *MQTTPublisher {
	configuredEventReport := TopicConfig{}
	if len(eventReport) > 0 {
		configuredEventReport = eventReport[0]
	}
	return &MQTTPublisher{
		telemetry:      telemetry,
		propertyResult: propertyResult,
		propertyReport: propertyReport,
		commandResult:  commandResult,
		statusReport:   statusReport,
		eventReport:    configuredEventReport,
		client:         newMQTTClient(config, logger),
	}
}

func (p *MQTTPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	event := outevent.TelemetryEvent{
		TraceID:     outevent.NewTraceID(device.Name),
		DeviceName:  device.Name,
		ProductCode: device.ProductCode,
		SourceName:  "telemetry",
		CollectedAt: time.Now().UnixMilli(),
	}
	jsonData, err := p.formatTelemetry(event, data, false)
	if err != nil {
		return err
	}

	return p.publishRaw(mqttMessage{
		Type:        busapi.TelemetryReport,
		Topic:       resolveTopic(p.telemetry.Topic, device.ProductCode),
		QoS:         byte(resolveQoS(p.telemetry.QoS, p.client.config.QoS)),
		Retain:      p.telemetry.Retain,
		Payload:     jsonData,
		DeviceName:  device.Name,
		ProductCode: device.ProductCode,
		TraceID:     event.TraceID,
		DataFormat:  p.telemetry.DataFormat,
	})
}

func (p *MQTTPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return p.PublishTelemetryEventAt(event, replayed, time.Now().UnixMilli())
}

// PublishTelemetryEventAt publishes telemetry with an outbox-selected send_at
// so the persisted attempt metadata and public MQTT envelope match exactly.
func (p *MQTTPublisher) PublishTelemetryEventAt(event outevent.TelemetryEvent, replayed bool, sendAt int64) error {
	data, err := event.DataMap()
	if err != nil {
		return err
	}

	body, err := p.formatTelemetryAt(event, data, replayed, sendAt)
	if err != nil {
		return err
	}

	return p.publishRaw(mqttMessage{
		Type:        busapi.TelemetryReport,
		Topic:       resolveTopic(p.telemetry.Topic, event.ProductCode),
		QoS:         byte(resolveQoS(p.telemetry.QoS, p.client.config.QoS)),
		Retain:      p.telemetry.Retain,
		Payload:     body,
		DeviceName:  event.DeviceName,
		ProductCode: event.ProductCode,
		TraceID:     event.TraceID,
		DataFormat:  p.telemetry.DataFormat,
	})
}

func (p *MQTTPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	event, err := outevent.NewTelemetryEvent(device, &contracts.AsyncValues{
		TraceID:     outevent.NewTraceID(device.Name),
		DeviceName:  device.Name,
		SourceName:  "telemetry",
		CollectedAt: time.Now().UnixMilli(),
		Values:      values,
	})
	if err != nil {
		return err
	}
	return p.PublishTelemetryEvent(event, false)
}

func (p *MQTTPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(mqttMessage{Type: busapi.PropertyResult, Topic: resolveTopic(p.propertyResult.Topic, device.ProductCode), DeviceName: device.Name, ProductCode: device.ProductCode, TraceID: traceIDFromPayload(payload)}, payload, p.propertyResult.QoS, p.propertyResult.Retain)
}

func (p *MQTTPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(mqttMessage{Type: busapi.PropertyReport, Topic: resolveTopic(p.propertyReport.Topic, device.ProductCode), DeviceName: device.Name, ProductCode: device.ProductCode, TraceID: traceIDFromPayload(payload)}, payload, p.propertyReport.QoS, p.propertyReport.Retain)
}

func (p *MQTTPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(mqttMessage{Type: busapi.CommandResult, Topic: resolveTopic(p.commandResult.Topic, device.ProductCode), DeviceName: device.Name, ProductCode: device.ProductCode, TraceID: traceIDFromPayload(payload)}, payload, p.commandResult.QoS, p.commandResult.Retain)
}

func (p *MQTTPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(mqttMessage{Type: busapi.StatusReport, Topic: resolveTopic(p.statusReport.Topic, device.ProductCode), DeviceName: device.Name, ProductCode: device.ProductCode, TraceID: traceIDFromPayload(payload)}, payload, p.statusReport.QoS, p.statusReport.Retain)
}

// PublishEvent publishes the public event envelope. Delivery metadata is
// generated at the last possible moment so replay does not alter event time or
// event_instance_id.
func (p *MQTTPublisher) PublishEvent(event events.Event, replayed bool) error {
	if p == nil || strings.TrimSpace(p.eventReport.Topic) == "" {
		return fmt.Errorf("event report topic is not configured")
	}
	body, err := event.MarshalPublicJSON(replayed, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	return p.publishRaw(mqttMessage{
		Type:        busapi.EventReport,
		Topic:       resolveTopic(p.eventReport.Topic, event.ProductCode),
		QoS:         byte(resolveQoS(p.eventReport.QoS, p.client.config.QoS)),
		Retain:      p.eventReport.Retain,
		Payload:     body,
		DeviceName:  event.DeviceCode,
		ProductCode: event.ProductCode,
		TraceID:     event.TraceID,
	})
}

func (p *MQTTPublisher) publishJSONTopic(message mqttMessage, payload map[string]interface{}, qos int, retain bool) error {
	if message.Topic == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	message.QoS = byte(resolveQoS(qos, p.client.config.QoS))
	message.Retain = retain
	message.Payload = body
	message.DataFormat = "json"
	return p.publishRaw(message)
}

func (p *MQTTPublisher) publishRaw(message mqttMessage) error {
	err := p.client.publishMessage(message)
	p.observe(Observation{
		Direction:   DirectionOutbound,
		Type:        message.Type,
		Topic:       message.Topic,
		QoS:         message.QoS,
		Retain:      message.Retain,
		Payload:     append([]byte(nil), message.Payload...),
		DataFormat:  message.DataFormat,
		DeviceName:  message.DeviceName,
		ProductCode: message.ProductCode,
		TraceID:     message.TraceID,
		Identifier:  message.Identifier,
	})
	return err
}

func (p *MQTTPublisher) setObserver(observer Observer) {
	p.observer = observer
}

func (p *MQTTPublisher) observe(message Observation) {
	if p != nil && p.observer != nil && message.Type != "" {
		p.observer.ObserveMQTT(message)
	}
}

func (p *MQTTPublisher) observeInbound(message Observation) {
	message.Direction = DirectionInbound
	p.observe(message)
}

func (p *MQTTPublisher) publishDirect(topic string, qos byte, retain bool, payload []byte) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("mqtt publisher is not initialized")
	}
	return p.client.Publish(topic, qos, retain, payload)
}

func traceIDFromPayload(payload map[string]interface{}) string {
	for _, key := range []string{"trace_id", "traceId"} {
		if value, ok := payload[key].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (p *MQTTPublisher) Subscribe(topic string, qos byte, handler MessageHandler) error {
	return p.client.Subscribe(topic, qos, handler)
}

func (p *MQTTPublisher) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	return p.client.PublishJSON(topic, qos, retain, payload)
}

func (p *MQTTPublisher) HealthCheck() error {
	if p == nil || p.client == nil {
		return fmt.Errorf("mqtt publisher is not initialized")
	}
	return p.client.HealthCheck()
}

// RegisterOnConnect exposes MQTT reconnect notifications to durable outboxes.
func (p *MQTTPublisher) RegisterOnConnect(hook func()) {
	if p == nil || p.client == nil || hook == nil {
		return
	}
	p.client.RegisterOnConnect(hook)
}

func (p *MQTTPublisher) Close() error {
	return p.client.Close()
}

func (p *MQTTPublisher) formatTelemetry(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool) ([]byte, error) {
	return p.formatTelemetryAt(event, data, replayed, time.Now().UnixMilli())
}

func (p *MQTTPublisher) formatTelemetryAt(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool, sendAt int64) ([]byte, error) {
	if sendAt <= 0 {
		sendAt = time.Now().UnixMilli()
	}
	sourceName := strings.TrimSpace(event.SourceName)
	if sourceName == "" {
		sourceName = "telemetry"
	}

	switch strings.ToLower(strings.TrimSpace(p.telemetry.DataFormat)) {
	case "raw":
		return json.Marshal(telemetryData{
			TraceID:    event.TraceID,
			Time:       event.CollectedAt,
			SendAt:     sendAt,
			IsReplayed: replayed,
			DeviceName: event.DeviceName,
			SourceName: sourceName,
			Values:     data,
		})
	case "influx":
		return p.convertToInfluxFormat(event, data, replayed)
	case "compact":
		return p.convertToCompactFormat(event, data, replayed, sendAt)
	case "telemetry":
		return p.convertToTelemetryFormat(event, data, replayed, sendAt)
	case "rule", "":
		fallthrough
	default:
		return p.convertToRuleFormat(event, data, replayed, sendAt)
	}
}

func (p *MQTTPublisher) convertToRuleFormat(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool, sendAt int64) ([]byte, error) {
	simplified := make(map[string]interface{}, len(data))
	for key, value := range data {
		simplified[key] = actualValue(value)
	}
	return json.Marshal(map[string]interface{}{
		"trace_id":    event.TraceID,
		"time":        event.CollectedAt,
		"send_at":     sendAt,
		"is_replayed": replayed,
		"data":        simplified,
		"device_code": event.DeviceName,
	})
}

func (p *MQTTPublisher) convertToInfluxFormat(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool) ([]byte, error) {
	timestamp := time.Now().UnixNano()
	var lines []string
	for key, value := range data {
		actual := actualValue(value)
		valueType := "unknown"
		origin := timestamp
		if valueMap, ok := value.(map[string]interface{}); ok {
			if typeVal, exists := valueMap["type"]; exists {
				valueType = fmt.Sprintf("%v", typeVal)
			}
			if originVal, exists := valueMap["origin"]; exists {
				switch v := originVal.(type) {
				case int64:
					origin = v
				case float64:
					origin = int64(v)
				}
			}
		}
		lines = append(lines, fmt.Sprintf("edge_device_data,device=%s,field=%s,type=%s,trace_id=%s value=%v,is_replayed=%t %d", event.DeviceName, key, valueType, event.TraceID, actual, replayed, origin))
	}
	return []byte(strings.Join(lines, "\n")), nil
}

func (p *MQTTPublisher) convertToTelemetryFormat(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool, sendAt int64) ([]byte, error) {
	deviceData := make(map[string]interface{}, len(data))
	for key, value := range data {
		deviceData[key] = actualValue(value)
	}
	return json.Marshal(map[string]interface{}{
		"traceId":    event.TraceID,
		"time":       event.CollectedAt,
		"sendAt":     sendAt,
		"isReplayed": replayed,
		"deviceName": event.DeviceName,
		"data":       deviceData,
	})
}

func (p *MQTTPublisher) convertToCompactFormat(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool, sendAt int64) ([]byte, error) {
	deviceData := make(map[string]interface{}, len(data))
	for key, value := range data {
		deviceData[key] = actualValue(value)
	}
	return json.Marshal(map[string]interface{}{
		"trace_id":       event.TraceID,
		"time":           event.CollectedAt,
		"send_at":        sendAt,
		"is_replayed":    replayed,
		event.DeviceName: deviceData,
	})
}

func actualValue(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]interface{}); ok {
		if actual, exists := valueMap["value"]; exists {
			return actual
		}
	}
	return value
}

func resolveTopic(template, productCode string) string {
	if template == "" {
		return ""
	}
	return strings.ReplaceAll(template, "{productCode}", productCode)
}

func resolveQoS(value int, fallback int) int {
	if value >= 0 {
		return value
	}
	if fallback >= 0 {
		return fallback
	}
	return 0
}
