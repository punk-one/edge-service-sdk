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
	catalog             DeviceCatalog
	driver              contracts.ProtocolDriver
	publisher           mqtt.Publisher
	logger              logger.LoggingClient
	propertyPostEnabled bool
	setPostDelay        time.Duration
}

func NewService(catalog DeviceCatalog, driver contracts.ProtocolDriver, publisher mqtt.Publisher, logClient logger.LoggingClient) *Service {
	return &Service{
		catalog:      catalog,
		driver:       driver,
		publisher:    publisher,
		logger:       logClient,
		setPostDelay: time.Second,
	}
}

func (s *Service) ExecuteGet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertyResponse, int) {
	device, normalized, statusCode, err := s.resolvePropertyDevice(req, expectedProductCode)
	response := rtapi.PropertyResponse{
		DeviceCode: normalized.DeviceCode,
		Time:       time.Now().UnixMilli(),
		TraceID:    normalized.TraceID,
		Data:       map[string]interface{}{},
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
		DeviceCode: normalized.DeviceCode,
		Time:       time.Now().UnixMilli(),
		TraceID:    normalized.TraceID,
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
	s.propertyPostEnabled = strings.TrimSpace(config.PropertyPost.Topic) != ""

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
	req.DeviceCode = strings.TrimSpace(req.DeviceCode)
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}
	if req.DeviceCode == "" {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("device_code is required")
	}
	device, ok := s.catalog.DeviceConfigByName(req.DeviceCode)
	if !ok {
		return contracts.DeviceConfig{}, req, 404, fmt.Errorf("device_code does not match any configured device")
	}
	if expectedProductCode != "" && device.ProductCode != expectedProductCode {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("device_code does not match the subscribed topic")
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
	if s.propertyPostEnabled {
		s.schedulePropertySetPost(productCode, req, response)
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
	if !s.propertyPostEnabled {
		return
	}
	s.publishPropertyPost(productCode, resolvedDeviceCode(req.DeviceCode, response.DeviceCode), response)
}

func (s *Service) schedulePropertySetPost(productCode string, req rtapi.PropertyRequest, response rtapi.PropertySetResponse) {
	if s == nil || s.publisher == nil {
		return
	}

	go func() {
		time.Sleep(s.setPostDelay)

		post := rtapi.PropertyResponse{
			DeviceCode: resolvedDeviceCode(req.DeviceCode, response.DeviceCode),
			Time:       time.Now().UnixMilli(),
			Success:    response.Success,
			TraceID:    response.TraceID,
			Error:      response.Error,
			Data:       map[string]interface{}{},
		}
		if response.Success {
			device, normalized, _, err := s.resolvePropertyDevice(req, productCode)
			if err != nil {
				post.Success = false
				post.Error = err.Error()
			} else {
				selection := cfg.BuildPropertyReadSelection(normalized.Data)
				commandReqs, bindings, err := cfg.BuildPropertyReadRequests(device, selection)
				if err != nil {
					post.Success = false
					post.Error = err.Error()
				} else {
					values, err := s.driver.HandleReadCommands(device.Name, cfg.ProtocolPropertiesFromConfig(device), commandReqs)
					if err != nil {
						post.Success = false
						post.Error = err.Error()
					} else {
						post.Success = true
						post.Error = ""
						post.Data = cfg.BuildPropertyResponse(values, bindings)
					}
				}
			}
		}

		s.publishPropertyPost(productCode, post.DeviceCode, post)
	}()
}

func (s *Service) publishPropertyPost(productCode string, deviceCode string, response rtapi.PropertyResponse) {
	if s == nil || s.publisher == nil {
		return
	}
	device := contracts.DeviceConfig{
		Name:        deviceCode,
		ProductCode: productCode,
	}
	if resolved, ok := s.catalog.DeviceConfigByName(deviceCode); ok && resolved.ProductCode == productCode {
		device = resolved
	}
	_ = s.publisher.PublishPropertyPost(device, map[string]interface{}{
		"device_code": response.DeviceCode,
		"time":        response.Time,
		"success":     response.Success,
		"trace_id":    response.TraceID,
		"error":       response.Error,
		"data":        response.Data,
	})
}

func resolvedDeviceCode(primary string, fallback string) string {
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	return strings.TrimSpace(primary)
}
