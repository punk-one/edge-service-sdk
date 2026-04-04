package property

import (
	"fmt"
	"strings"
	"time"

	cfg "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type DeviceCatalog interface {
	DeviceConfigByName(name string) (contracts.DeviceConfig, bool)
	DevicesByProductCode(productCode string) []contracts.DeviceConfig
	ProductCodes() []string
}

type Service struct {
	catalog   DeviceCatalog
	driver    contracts.ProtocolDriver
	publisher mqtt.Publisher
	logger    logger.LoggingClient
}

func NewService(catalog DeviceCatalog, driver contracts.ProtocolDriver, publisher mqtt.Publisher, logClient logger.LoggingClient) *Service {
	return &Service{
		catalog:   catalog,
		driver:    driver,
		publisher: publisher,
		logger:    logClient,
	}
}

func (s *Service) ExecuteGet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertyResponse, int) {
	device, normalized, statusCode, err := s.resolvePropertyDevice(req, expectedProductCode)
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

	commandReqs, bindings, err := cfg.BuildPropertyReadRequests(device, normalized.Data)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 400
	}

	values, err := s.driver.HandleReadCommands(device.Name, cfg.ProtocolPropertiesFromConfig(device), commandReqs)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 200
	}

	response.Success = true
	response.Data = cfg.BuildPropertyResponse(values, bindings)
	return response, 200
}

func (s *Service) ExecuteSet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertySetResponse, int) {
	device, normalized, statusCode, err := s.resolvePropertyDevice(req, expectedProductCode)
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

	commandReqs, params, err := cfg.BuildPropertyWriteRequests(device, normalized.Data)
	if err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 400
	}

	if err := s.driver.HandleWriteCommands(device.Name, cfg.ProtocolPropertiesFromConfig(device), commandReqs, params); err != nil {
		response.Success = false
		response.Error = err.Error()
		return response, 200
	}

	response.Success = true
	return response, 200
}

func (s *Service) RegisterMQTTHandlers(config rtconfig.Config) {
	if s.publisher == nil {
		return
	}

	for _, productCode := range s.catalog.ProductCodes() {
		if config.PropertySet.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertySet.Topic, productCode)
			_ = s.publisher.Subscribe(topic, byte(config.PropertySet.QoS), func(_ string, payload []byte) {
				s.handlePropertySet(productCode, payload)
			})
		}

		if config.PropertyGet.Topic != "" && config.PropertyPost.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertyGet.Topic, productCode)
			_ = s.publisher.Subscribe(topic, byte(config.PropertyGet.QoS), func(_ string, payload []byte) {
				s.handlePropertyGet(productCode, payload)
			})
		} else if config.PropertyGet.Topic != "" && config.PropertyPost.Topic == "" && s.logger != nil {
			s.logger.Warnf("PropertyGet configured but PropertyPost topic is empty; disabling property get for product %s", productCode)
		}
	}
}

func (s *Service) resolvePropertyDevice(req rtapi.PropertyRequest, expectedProductCode string) (contracts.DeviceConfig, rtapi.PropertyRequest, int, error) {
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

	device, ok := s.catalog.DeviceConfigByName(req.DeviceCode)
	if !ok || device.ProductCode != req.ProductCode {
		return contracts.DeviceConfig{}, req, 404, fmt.Errorf("device_code and product_code do not match any configured device")
	}
	return device, req, 200, nil
}

func (s *Service) handlePropertySet(productCode string, payload []byte) {
	req, err := cfg.ParsePropertyRequest(payload)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to parse property_set payload for product %s: %v", productCode, err)
		}
		return
	}

	response, _ := s.ExecuteSet(req, productCode)
	if !response.Success && s.logger != nil {
		s.logger.Warnf("MQTT property set failed: product=%s device=%s err=%s", productCode, response.DeviceCode, response.Error)
	}
}

func (s *Service) handlePropertyGet(productCode string, payload []byte) {
	req, err := cfg.ParsePropertyRequest(payload)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to parse property_get payload for product %s: %v", productCode, err)
		}
		return
	}

	response, _ := s.ExecuteGet(req, productCode)
	device, ok := s.catalog.DeviceConfigByName(response.DeviceCode)
	if !ok || device.ProductCode != response.ProductCode {
		return
	}
	_ = s.publisher.PublishPropertyPost(device, map[string]interface{}{
		"product_code": response.ProductCode,
		"device_code":  response.DeviceCode,
		"time":         response.Time,
		"success":      response.Success,
		"request_id":   response.RequestID,
		"error":        response.Error,
		"data":         response.Data,
	})
}
