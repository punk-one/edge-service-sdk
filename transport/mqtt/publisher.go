package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// NewMQTTPublisher creates a new MQTT publisher.
func NewMQTTPublisher(config MQTTConfig, telemetry TopicConfig, propertyPost TopicConfig, statusReport TopicConfig, logger logger.LoggingClient) *MQTTPublisher {
	return &MQTTPublisher{
		telemetry:    telemetry,
		propertyPost: propertyPost,
		statusReport: statusReport,
		client:       newMQTTClient(config, logger),
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
		Topic:       resolveTopic(p.telemetry.Topic, device.ProductCode),
		QoS:         byte(resolveQoS(p.telemetry.QoS, p.client.config.QoS)),
		Retain:      p.telemetry.Retain,
		Payload:     jsonData,
		DeviceName:  device.Name,
		ProductCode: device.ProductCode,
		TraceID:     event.TraceID,
	})
}

func (p *MQTTPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	data, err := event.DataMap()
	if err != nil {
		return err
	}

	body, err := p.formatTelemetry(event, data, replayed)
	if err != nil {
		return err
	}

	return p.publishRaw(mqttMessage{
		Topic:       resolveTopic(p.telemetry.Topic, event.ProductCode),
		QoS:         byte(resolveQoS(p.telemetry.QoS, p.client.config.QoS)),
		Retain:      p.telemetry.Retain,
		Payload:     body,
		DeviceName:  event.DeviceName,
		ProductCode: event.ProductCode,
		TraceID:     event.TraceID,
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

func (p *MQTTPublisher) PublishPropertyPost(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(resolveTopic(p.propertyPost.Topic, device.ProductCode), payload, p.propertyPost.QoS, p.propertyPost.Retain)
}

func (p *MQTTPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.publishJSONTopic(resolveTopic(p.statusReport.Topic, device.ProductCode), payload, p.statusReport.QoS, p.statusReport.Retain)
}

func (p *MQTTPublisher) publishJSONTopic(topic string, payload map[string]interface{}, qos int, retain bool) error {
	if topic == "" {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return p.publishRaw(mqttMessage{
		Topic:   topic,
		QoS:     byte(resolveQoS(qos, p.client.config.QoS)),
		Retain:  retain,
		Payload: body,
	})
}

func (p *MQTTPublisher) publishRaw(message mqttMessage) error {
	return p.client.publishMessage(message)
}

func (p *MQTTPublisher) Subscribe(topic string, qos byte, handler MessageHandler) error {
	return p.client.Subscribe(topic, qos, handler)
}

func (p *MQTTPublisher) HealthCheck() error {
	if p == nil || p.client == nil {
		return fmt.Errorf("mqtt publisher is not initialized")
	}
	return p.client.HealthCheck()
}

func (p *MQTTPublisher) Close() error {
	return p.client.Close()
}

func (p *MQTTPublisher) formatTelemetry(event outevent.TelemetryEvent, data map[string]interface{}, replayed bool) ([]byte, error) {
	sendAt := time.Now().UnixMilli()

	switch strings.ToLower(strings.TrimSpace(p.telemetry.DataFormat)) {
	case "raw":
		return json.Marshal(telemetryData{
			TraceID:    event.TraceID,
			Time:       event.CollectedAt,
			SendAt:     sendAt,
			IsReplayed: replayed,
			DeviceName: event.DeviceName,
			SourceName: "S7-Device",
			Values:     data,
		})
	case "influx":
		return p.convertToInfluxFormat(event, data, replayed)
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
		"traceId":     event.TraceID,
		"time":        event.CollectedAt,
		"sendAt":      sendAt,
		"isReplayed":  replayed,
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
		lines = append(lines, fmt.Sprintf("s7_data,device=%s,field=%s,type=%s,trace_id=%s value=%v,is_replayed=%t %d", event.DeviceName, key, valueType, event.TraceID, actual, replayed, origin))
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
