package bus

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	logger "github.com/punk-one/edge-service-sdk/logging"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

const observerQueueSize = 2048

// MQTTObserver mirrors MQTT traffic asynchronously so JetStream latency or
// failure cannot change the existing MQTT/SQLite return path.
type MQTTObserver struct {
	bus       *Service
	logger    logger.LoggingClient
	queue     chan mqtt.Observation
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func NewMQTTObserver(busService *Service, logClient logger.LoggingClient) *MQTTObserver {
	ctx, cancel := context.WithCancel(context.Background())
	observer := &MQTTObserver{
		bus:    busService,
		logger: logClient,
		queue:  make(chan mqtt.Observation, observerQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}
	go observer.run()
	return observer
}

func (o *MQTTObserver) ObserveMQTT(message mqtt.Observation) {
	if o == nil || o.bus == nil {
		return
	}
	message.Payload = append([]byte(nil), message.Payload...)
	select {
	case o.queue <- message:
	default:
		if o.logger != nil {
			o.logger.Warnf("JetStream mirror queue full; dropping %s observation", message.Type)
		}
	}
}

func (o *MQTTObserver) run() {
	for {
		select {
		case <-o.ctx.Done():
			return
		case observation := <-o.queue:
			o.publish(observation)
		}
	}
}

func (o *MQTTObserver) publish(observation mqtt.Observation) {
	origin := busapi.OriginSDK
	if observation.Direction == mqtt.DirectionInbound {
		origin = busapi.OriginMQTT
	}
	traceID, deviceCode := extractControlMetadata(observation.Payload)
	if observation.TraceID != "" {
		traceID = observation.TraceID
	}
	if observation.DeviceName != "" {
		deviceCode = observation.DeviceName
	}
	ctx, cancel := context.WithTimeout(o.ctx, 2*time.Second)
	defer cancel()
	err := o.bus.Publish(ctx, busapi.Message{
		Type:        observation.Type,
		Data:        observation.Payload,
		Origin:      origin,
		DataFormat:  observation.DataFormat,
		TraceID:     traceID,
		ProductCode: observation.ProductCode,
		DeviceCode:  deviceCode,
		Identifier:  observation.Identifier,
		Headers: map[string]string{
			busapi.HeaderMQTTTopic: observation.Topic,
		},
	})
	if err != nil && o.logger != nil {
		o.logger.Warnf("Failed to mirror %s to JetStream: %v", observation.Type, err)
	}
}

func (o *MQTTObserver) Close() {
	if o == nil {
		return
	}
	o.closeOnce.Do(o.cancel)
}

func extractControlMetadata(payload []byte) (string, string) {
	var envelope struct {
		TraceID    string `json:"trace_id"`
		TraceIDAlt string `json:"traceId"`
		DeviceCode string `json:"device_code"`
		DeviceName string `json:"deviceName"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return "", ""
	}
	traceID := envelope.TraceID
	if traceID == "" {
		traceID = envelope.TraceIDAlt
	}
	deviceCode := envelope.DeviceCode
	if deviceCode == "" {
		deviceCode = envelope.DeviceName
	}
	return traceID, deviceCode
}
