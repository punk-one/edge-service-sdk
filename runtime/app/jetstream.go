package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	logger "github.com/punk-one/edge-service-sdk/logging"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"
	rtcommand "github.com/punk-one/edge-service-sdk/runtime/command"
	rtproperty "github.com/punk-one/edge-service-sdk/runtime/property"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

func installJetStreamRoutes(busService *runtimebus.Service, sdk *DeviceSDK, publisher mqtt.Publisher, config appconfig.Config, propertyService *rtproperty.Service, commandService *rtcommand.Service, logClient logger.LoggingClient) error {
	if busService == nil {
		return nil
	}
	var installErrors []error
	for _, route := range []struct {
		messageType busapi.MessageType
		topic       mqtt.TopicConfig
	}{
		{busapi.TelemetryReport, config.TelemetryReport},
		{busapi.PropertyReport, config.PropertyReport},
		{busapi.PropertyResult, config.PropertyResult},
		{busapi.CommandResult, config.CommandResult},
		{busapi.EventReport, config.EventReport},
		{busapi.StatusReport, config.StatusReport},
	} {
		filter, err := busapi.FilterSubjectFor(route.messageType)
		if err != nil {
			installErrors = append(installErrors, err)
			continue
		}
		routeCopy := route
		err = busService.StartConsumer(runtimebus.ConsumerConfig{
			Durable:       "sdk-mqtt-out-" + strings.ReplaceAll(string(route.messageType), ".", "-"),
			FilterSubject: filter,
			AckWait:       30 * time.Second,
			MaxDeliver:    10,
		}, func(_ context.Context, message busapi.Message) error {
			if !requiresBusAction(message.Origin) {
				return nil
			}
			productCode, ok := resolveBusProduct(sdk, message)
			if !ok {
				if logClient != nil {
					logClient.Warnf("Skipping process bus output %s: product code cannot be resolved", message.Type)
				}
				return nil
			}
			topic := appconfig.StringsReplaceProductCode(routeCopy.topic.Topic, productCode)
			if strings.TrimSpace(topic) == "" {
				return nil
			}
			return mqtt.PublishDirect(publisher, topic, normalizedQoS(routeCopy.topic.QoS), routeCopy.topic.Retain, message.Data)
		})
		if err != nil {
			installErrors = append(installErrors, err)
		}
	}

	installControl := func(messageType busapi.MessageType, handler func(busapi.Message)) {
		filter, err := busapi.FilterSubjectFor(messageType)
		if err != nil {
			installErrors = append(installErrors, err)
			return
		}
		err = busService.StartConsumer(runtimebus.ConsumerConfig{
			Durable:       "sdk-control-in-" + strings.ReplaceAll(string(messageType), ".", "-"),
			FilterSubject: filter,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
		}, func(_ context.Context, message busapi.Message) error {
			if !requiresBusAction(message.Origin) {
				return nil
			}
			handler(message)
			return nil
		})
		if err != nil {
			installErrors = append(installErrors, err)
		}
	}

	installControl(busapi.PropertySet, func(message busapi.Message) {
		if productCode, ok := resolveBusProduct(sdk, message); ok {
			propertyService.HandleBusPropertySet(productCode, message.Data)
		} else if logClient != nil {
			logClient.Warnf("Skipping JetStream property.set: product code cannot be resolved")
		}
	})
	installControl(busapi.PropertyGet, func(message busapi.Message) {
		if productCode, ok := resolveBusProduct(sdk, message); ok {
			propertyService.HandleBusPropertyGet(productCode, message.Data)
		} else if logClient != nil {
			logClient.Warnf("Skipping JetStream property.get: product code cannot be resolved")
		}
	})
	installControl(busapi.CommandCall, func(message busapi.Message) {
		if productCode, ok := resolveBusProduct(sdk, message); ok && strings.TrimSpace(message.Identifier) != "" {
			commandService.HandleBusCommandCall(productCode, message.Identifier, message.Data)
		} else if logClient != nil {
			logClient.Warnf("Skipping JetStream command.call: product code or identifier cannot be resolved")
		}
	})

	return errors.Join(installErrors...)
}

func requiresBusAction(origin busapi.Origin) bool {
	return origin == busapi.OriginProcess || origin == busapi.OriginNATS
}

func resolveBusProduct(sdk *DeviceSDK, message busapi.Message) (string, bool) {
	if productCode := strings.TrimSpace(message.ProductCode); productCode != "" {
		return productCode, true
	}
	deviceCode := strings.TrimSpace(message.DeviceCode)
	if deviceCode == "" {
		var envelope struct {
			DeviceCode string `json:"device_code"`
			DeviceName string `json:"deviceName"`
		}
		if json.Unmarshal(message.Data, &envelope) == nil {
			deviceCode = strings.TrimSpace(envelope.DeviceCode)
			if deviceCode == "" {
				deviceCode = strings.TrimSpace(envelope.DeviceName)
			}
		}
	}
	if deviceCode == "" || sdk == nil {
		return "", false
	}
	device, ok := sdk.DeviceConfigByName(deviceCode)
	if !ok || strings.TrimSpace(device.ProductCode) == "" {
		return "", false
	}
	return device.ProductCode, true
}

func normalizedQoS(value int) byte {
	if value < 0 || value > 2 {
		return 0
	}
	return byte(value)
}
