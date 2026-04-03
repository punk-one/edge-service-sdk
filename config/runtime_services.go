package config

import (
	"fmt"
	"strings"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

func executePropertyGet(req rtapi.PropertyRequest, expectedProductCode string, sdk *DeviceSDK, driver contracts.ProtocolDriver) (rtapi.PropertyResponse, int) {
	device, normalized, statusCode, err := resolvePropertyDevice(req, expectedProductCode, sdk)
	response := rtapi.PropertyResponse{
		ProductCode: normalized.ProductCode,
		DeviceCode:  normalized.DeviceCode,
		Time:        time.Now().UnixMilli(),
		RequestID:   normalized.RequestID,
		Data:        map[string]interface{}{},
	}
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, statusCode
	}

	commandReqs, bindings, err := buildPropertyReadRequests(device, normalized.Data)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 400
	}

	values, err := driver.HandleReadCommands(device.Name, protocolPropertiesFromConfig(device), commandReqs)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 200
	}

	response.Success = true
	response.Data = buildPropertyResponse(values, bindings)
	return response, 200
}

func executePropertySet(req rtapi.PropertyRequest, expectedProductCode string, sdk *DeviceSDK, driver contracts.ProtocolDriver) (rtapi.PropertySetResponse, int) {
	device, normalized, statusCode, err := resolvePropertyDevice(req, expectedProductCode, sdk)
	response := rtapi.PropertySetResponse{
		ProductCode: normalized.ProductCode,
		DeviceCode:  normalized.DeviceCode,
		Time:        time.Now().UnixMilli(),
		RequestID:   normalized.RequestID,
	}
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, statusCode
	}

	commandReqs, params, err := buildPropertyWriteRequests(device, normalized.Data)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 400
	}

	if err := driver.HandleWriteCommands(device.Name, protocolPropertiesFromConfig(device), commandReqs, params); err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 200
	}

	response.Success = true
	return response, 200
}

func resolvePropertyDevice(req rtapi.PropertyRequest, expectedProductCode string, sdk *DeviceSDK) (contracts.DeviceConfig, rtapi.PropertyRequest, int, error) {
	req.ProductCode = strings.TrimSpace(req.ProductCode)
	req.DeviceCode = strings.TrimSpace(req.DeviceCode)
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}
	if req.ProductCode == "" {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("product_code is required")
	}
	if req.DeviceCode == "" {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("device_code is required")
	}
	if expectedProductCode != "" && req.ProductCode != expectedProductCode {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("product_code does not match the subscribed topic")
	}

	device, ok := sdk.DeviceConfigByName(req.DeviceCode)
	if !ok || device.ProductCode != req.ProductCode {
		return contracts.DeviceConfig{}, req, 404, fmt.Errorf("device_code and product_code do not match any configured device")
	}
	return device, req, 200, nil
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
	if tracker == nil || publisher == nil || strings.TrimSpace(topicConfig.Topic) == "" {
		return
	}

	publish := func(states []rtstatus.DeviceState) {
		if len(states) == 0 {
			return
		}
		now := time.Now().UnixMilli()
		if strings.Contains(topicConfig.Topic, "{productCode}") {
			grouped := make(map[string][]map[string]interface{})
			for _, state := range states {
				grouped[state.ProductCode] = append(grouped[state.ProductCode], statusMap(state))
			}
			for productCode, items := range grouped {
				devices := sdk.DevicesByProductCode(productCode)
				if len(devices) == 0 {
					continue
				}
				if err := publisher.PublishStatus(devices[0], map[string]interface{}{
					"time":         now,
					"product_code": productCode,
					"devices":      items,
				}); err != nil && logClient != nil {
					logClient.Warnf("Failed to publish status snapshot for product %s: %v", productCode, err)
				}
			}
			return
		}

		device, ok := firstDeviceConfig(sdk)
		if !ok {
			return
		}
		items := make([]map[string]interface{}, 0, len(states))
		for _, state := range states {
			items = append(items, statusMap(state))
		}
		if err := publisher.PublishStatus(device, map[string]interface{}{
			"time":    now,
			"devices": items,
		}); err != nil && logClient != nil {
			logClient.Warnf("Failed to publish global status snapshot: %v", err)
		}
	}

	tracker.SetOnChange(publish)
	publish(tracker.Snapshot())
}

func statusMap(state rtstatus.DeviceState) map[string]interface{} {
	return map[string]interface{}{
		"deviceCode":      state.DeviceCode,
		"productCode":     state.ProductCode,
		"connectionState": state.ConnectionState,
		"connected":       state.Connected,
		"lastConnectedAt": state.LastConnectedAt,
		"lastReadAt":      state.LastReadAt,
		"lastWriteAt":     state.LastWriteAt,
		"lastSuccessAt":   state.LastSuccessAt,
		"lastError":       state.LastError,
		"lastErrorAt":     state.LastErrorAt,
	}
}

func firstDeviceConfig(sdk *DeviceSDK) (contracts.DeviceConfig, bool) {
	if sdk == nil {
		return contracts.DeviceConfig{}, false
	}
	for _, device := range sdk.deviceConfigs {
		return device, true
	}
	return contracts.DeviceConfig{}, false
}
