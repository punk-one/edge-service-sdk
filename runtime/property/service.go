package property

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	cfg "github.com/punk-one/edge-service-sdk/config"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type DeviceCatalog interface {
	DeviceConfigByName(name string) (contracts.DeviceConfig, bool)
	DevicesByProductCode(productCode string) []contracts.DeviceConfig
	ProductCodes() []string
}

type Service struct {
	catalog               DeviceCatalog
	driver                contracts.ProtocolDriver
	publisher             mqtt.Publisher
	store                 rtcontrol.Store
	logger                logger.LoggingClient
	propertyResultEnabled bool
	setPostDelay          time.Duration

	mu          sync.Mutex
	activeAsync map[string]struct{}
}

type propertyProgressEvent struct {
	Stage           string
	Phase           string
	ProgressPercent int
	ElapsedMs       int64
	StageElapsedMs  int64
	PropertyCount   int
	PropertyNames   []string
	Data            map[string]interface{}
}

var ErrDeviceNotFound = errors.New("device_code does not match any configured device")

const (
	propertyOperationGet = "get"
	propertyOperationSet = "set"
)

func NewService(catalog DeviceCatalog, driver contracts.ProtocolDriver, publisher mqtt.Publisher, store rtcontrol.Store, logClient logger.LoggingClient) *Service {
	return &Service{
		catalog:      catalog,
		driver:       driver,
		publisher:    publisher,
		store:        store,
		logger:       logClient,
		setPostDelay: time.Second,
		activeAsync:  make(map[string]struct{}),
	}
}

func (s *Service) ExecuteGet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertyResponse, int) {
	if existing, ok := s.loadExistingResult(req.TraceID); ok {
		return existing, httpStatusForCode(existing.Code)
	}
	if expired := expiredRequestResult(req); expired != nil {
		s.record(req.DeviceCode, expectedProductCode, req.TraceID, *expired, propertyOperationGet)
		return *expired, httpStatusForCode(expired.Code)
	}
	if invalid := validateStrategy(req, true); invalid != nil {
		s.record(req.DeviceCode, expectedProductCode, req.TraceID, *invalid, propertyOperationGet)
		return *invalid, httpStatusForCode(invalid.Code)
	}

	device, normalized, statusCode, err := s.resolvePropertyDevice(req, expectedProductCode)
	response := newControlResult(normalized.TraceID, ctl.CodeSuccess, "success", map[string]interface{}{})
	if err != nil {
		response.Code = statusCode
		response.Message = err.Error()
		if !errors.Is(err, ErrDeviceNotFound) {
			s.record(normalized.DeviceCode, expectedProductCode, normalized.TraceID, response, propertyOperationGet)
		}
		return response, statusCode
	}
	if shouldAsyncPropertyGet(normalized) {
		return s.executeAsyncProperty(device, normalized, propertyOperationGet)
	}

	selection, err := propertyGetSelection(device, normalized.Data)
	if err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, result, propertyOperationGet)
		return result, httpStatusForCode(result.Code)
	}

	commandReqs, bindings, err := cfg.BuildPropertyReadRequests(device, selection)
	if err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, result, propertyOperationGet)
		return result, httpStatusForCode(result.Code)
	}

	values, err := s.driver.HandleReadCommands(device.InternalName, cfg.ProtocolPropertiesFromConfig(device), commandReqs)
	if err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeDriverError, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, result, propertyOperationGet)
		return result, httpStatusForCode(result.Code)
	}

	response.Data = cfg.BuildPropertyResponse(values, bindings)
	s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, response, propertyOperationGet)
	return response, httpStatusForCode(response.Code)
}

func (s *Service) ExecuteSet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertySetResponse, int) {
	if existing, ok := s.loadExistingResult(req.TraceID); ok {
		return existing, httpStatusForCode(existing.Code)
	}
	if expired := expiredRequestResult(req); expired != nil {
		s.record(req.DeviceCode, expectedProductCode, req.TraceID, *expired, propertyOperationSet)
		return *expired, httpStatusForCode(expired.Code)
	}
	if invalid := validateStrategy(req, false); invalid != nil {
		s.record(req.DeviceCode, expectedProductCode, req.TraceID, *invalid, propertyOperationSet)
		return *invalid, httpStatusForCode(invalid.Code)
	}

	device, normalized, statusCode, err := s.resolvePropertyDevice(req, expectedProductCode)
	response := newControlResult(normalized.TraceID, ctl.CodeSuccess, "success", map[string]interface{}{})
	if err != nil {
		response.Code = statusCode
		response.Message = err.Error()
		if !errors.Is(err, ErrDeviceNotFound) {
			s.record(normalized.DeviceCode, expectedProductCode, normalized.TraceID, response, propertyOperationSet)
		}
		return response, statusCode
	}
	if shouldAsyncPropertySet(normalized) {
		return s.executeAsyncProperty(device, normalized, propertyOperationSet)
	}

	commandReqs, params, err := cfg.BuildPropertyWriteRequests(device, normalized.Data)
	if err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, result, propertyOperationSet)
		return result, httpStatusForCode(result.Code)
	}

	if err := s.driver.HandleWriteCommands(device.InternalName, cfg.ProtocolPropertiesFromConfig(device), commandReqs, params); err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeDriverError, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, result, propertyOperationSet)
		return result, httpStatusForCode(result.Code)
	}

	s.record(normalized.DeviceCode, device.ProductCode, normalized.TraceID, response, propertyOperationSet)
	return response, httpStatusForCode(response.Code)
}

func (s *Service) RegisterMQTTHandlers(config rtconfig.Config) {
	if s.publisher == nil {
		return
	}
	s.propertyResultEnabled = strings.TrimSpace(config.PropertyResult.Topic) != ""

	for _, productCode := range s.catalog.ProductCodes() {
		if config.PropertySet.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertySet.Topic, productCode)
			_ = s.publisher.Subscribe(topic, byte(config.PropertySet.QoS), func(actualTopic string, payload []byte) {
				mqtt.ObserveInbound(s.publisher, mqtt.Observation{
					Type:        busapi.PropertySet,
					Topic:       actualTopic,
					QoS:         byte(config.PropertySet.QoS),
					Payload:     append([]byte(nil), payload...),
					DataFormat:  "json",
					ProductCode: productCode,
				})
				s.handlePropertySet(productCode, payload)
			})
		}

		if config.PropertyGet.Topic != "" && config.PropertyResult.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertyGet.Topic, productCode)
			_ = s.publisher.Subscribe(topic, byte(config.PropertyGet.QoS), func(actualTopic string, payload []byte) {
				mqtt.ObserveInbound(s.publisher, mqtt.Observation{
					Type:        busapi.PropertyGet,
					Topic:       actualTopic,
					QoS:         byte(config.PropertyGet.QoS),
					Payload:     append([]byte(nil), payload...),
					DataFormat:  "json",
					ProductCode: productCode,
				})
				s.handlePropertyGet(productCode, payload)
			})
		} else if config.PropertyGet.Topic != "" && config.PropertyResult.Topic == "" && s.logger != nil {
			s.logger.Warnf("PropertyGet configured but PropertyResult topic is empty; disabling property get for product %s", productCode)
		}
	}
}

// HandleBusPropertySet reuses the existing MQTT property path for a
// process/NATS-originated payload without reflecting the input back to the bus.
func (s *Service) HandleBusPropertySet(productCode string, payload []byte) {
	s.handlePropertySet(productCode, payload)
}

func (s *Service) HandleBusPropertyGet(productCode string, payload []byte) {
	s.handlePropertyGet(productCode, payload)
}

func (s *Service) ResumePending() error {
	if s == nil || s.store == nil {
		return nil
	}
	items, err := s.store.ListPendingProperties()
	if err != nil {
		return err
	}
	for _, item := range items {
		job, found, loadErr := s.store.LoadJob(item.TraceID)
		if loadErr == nil && found && rtcontrol.IsFinalCode(job.Code) {
			_ = s.store.DeletePendingProperty(item.TraceID)
			continue
		}
		if !s.markAsyncActive(item.TraceID) {
			continue
		}
		go s.runAsyncProperty(item)
	}
	return nil
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
		return contracts.DeviceConfig{}, req, 404, fmt.Errorf("%w: %s", ErrDeviceNotFound, req.DeviceCode)
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
	if response.Code != ctl.CodeSuccess && response.Code != ctl.CodeAccepted && s.logger != nil {
		s.logger.Warnf("MQTT property set failed: product=%s device=%s code=%d msg=%s", productCode, req.DeviceCode, response.Code, response.Message)
	}
	if !s.propertyResultEnabled {
		return
	}
	if response.Code == ctl.CodeNotFound {
		if s.logger != nil {
			s.logger.Debugf("Device %s not found in this instance, discarding property set", req.DeviceCode)
		}
		return
	}
	switch response.Code {
	case ctl.CodeSuccess:
		s.schedulePropertySetResult(productCode, req, response)
	case ctl.CodeAccepted, ctl.CodeProcessing:
		s.publishPropertyResult(productCode, resolvedDeviceCode(req.DeviceCode, req.DeviceCode), response)
	default:
		s.publishPropertyResult(productCode, resolvedDeviceCode(req.DeviceCode, req.DeviceCode), response)
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
	if !s.propertyResultEnabled {
		return
	}
	if response.Code == ctl.CodeNotFound {
		if s.logger != nil {
			s.logger.Debugf("Device %s not found in this instance, discarding property get", req.DeviceCode)
		}
		return
	}
	s.publishPropertyResult(productCode, resolvedDeviceCode(req.DeviceCode, req.DeviceCode), response)
}

func (s *Service) schedulePropertySetResult(productCode string, req rtapi.PropertyRequest, response rtapi.PropertySetResponse) {
	if s == nil || s.publisher == nil {
		return
	}

	go func() {
		time.Sleep(s.setPostDelay)
		post := s.executePropertySetReadback(productCode, req, response.TraceID)
		s.record(req.DeviceCode, productCode, response.TraceID, post, propertyOperationSet)
		s.publishPropertyResult(productCode, resolvedDeviceCode(req.DeviceCode, req.DeviceCode), post)
	}()
}

func (s *Service) publishPropertyResult(productCode string, deviceCode string, response rtapi.PropertyResponse) {
	if s == nil || s.publisher == nil || !s.propertyResultEnabled {
		return
	}
	device := contracts.DeviceConfig{
		Name:        deviceCode,
		ProductCode: productCode,
	}
	if resolved, ok := s.catalog.DeviceConfigByName(deviceCode); ok && resolved.ProductCode == productCode {
		device = resolved
	}
	_ = s.publisher.PublishPropertyResult(device, map[string]interface{}{
		"trace_id": response.TraceID,
		"code":     response.Code,
		"message":  response.Message,
		"data":     response.Data,
		"time":     response.Time,
	})
}

func clonePropertyProgressData(data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return map[string]interface{}{}
	}
	copy := make(map[string]interface{}, len(data))
	for key, value := range data {
		copy[key] = value
	}
	return copy
}

func propertyNames(data map[string]interface{}) []string {
	if len(data) == 0 {
		return []string{}
	}
	names := make([]string, 0, len(data))
	for key := range data {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func newPropertyProgressEvent(stage string, phase string, progressPercent int, propertyNames []string, data map[string]interface{}, taskStart time.Time, stageStart time.Time) propertyProgressEvent {
	if stageStart.IsZero() {
		stageStart = taskStart
	}
	copiedNames := append([]string(nil), propertyNames...)
	return propertyProgressEvent{
		Stage:           strings.TrimSpace(stage),
		Phase:           strings.TrimSpace(phase),
		ProgressPercent: progressPercent,
		ElapsedMs:       time.Since(taskStart).Milliseconds(),
		StageElapsedMs:  time.Since(stageStart).Milliseconds(),
		PropertyCount:   len(copiedNames),
		PropertyNames:   copiedNames,
		Data:            clonePropertyProgressData(data),
	}
}

func propertyProgressPayload(event propertyProgressEvent) map[string]interface{} {
	payload := map[string]interface{}{
		"stage":            event.Stage,
		"phase":            event.Phase,
		"progress_percent": event.ProgressPercent,
		"elapsed_ms":       event.ElapsedMs,
		"stage_elapsed_ms": event.StageElapsedMs,
		"property_count":   event.PropertyCount,
		"property_names":   append([]string(nil), event.PropertyNames...),
	}
	if len(event.Data) > 0 {
		payload["stage_data"] = clonePropertyProgressData(event.Data)
	}
	return payload
}

func enrichPropertyFailure(response rtapi.PropertyResponse, event propertyProgressEvent) rtapi.PropertyResponse {
	if response.Data == nil {
		response.Data = map[string]interface{}{}
	}
	response.Data["failure_context"] = propertyProgressPayload(event)
	return response
}

func (s *Service) recordPropertyProgress(pending rtcontrol.PendingProperty, event propertyProgressEvent) {
	message := fmt.Sprintf("%s %s", event.Stage, event.Phase)
	result := newControlResult(pending.TraceID, ctl.CodeProcessing, message, map[string]interface{}{
		"progress": propertyProgressPayload(event),
	})
	s.record(pending.DeviceCode, pending.ProductCode, pending.TraceID, result, pending.Operation)
}

func resolvedDeviceCode(primary string, fallback string) string {
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	return strings.TrimSpace(primary)
}

func newControlResult(traceID string, code int, message string, data map[string]interface{}) rtapi.PropertyResponse {
	if data == nil {
		data = map[string]interface{}{}
	}
	return rtapi.PropertyResponse{
		TraceID: traceID,
		Code:    code,
		Message: message,
		Data:    data,
		Time:    time.Now().UnixMilli(),
	}
}

func expiredRequestResult(req rtapi.PropertyRequest) *rtapi.PropertyResponse {
	if req.Metadata == nil || req.Metadata.ExpiryTime <= 0 {
		return nil
	}
	if time.Now().UnixMilli() <= req.Metadata.ExpiryTime {
		return nil
	}
	result := newControlResult(req.TraceID, ctl.CodeExpired, "request expired", nil)
	return &result
}

func validateStrategy(req rtapi.PropertyRequest, allowCacheFirst bool) *rtapi.PropertyResponse {
	if req.Metadata == nil {
		return nil
	}
	strategy := strings.TrimSpace(req.Metadata.Strategy)
	if strategy == "" || strategy == "always_realtime" {
		return nil
	}
	if allowCacheFirst && strategy == "cache_first" {
		return nil
	}
	result := newControlResult(req.TraceID, ctl.CodeBadRequest, fmt.Sprintf("unsupported strategy %q", strategy), nil)
	return &result
}

func httpStatusForCode(code int) int {
	switch code {
	case ctl.CodeSuccess, ctl.CodePartialSuccess:
		return 200
	case ctl.CodeProcessing, ctl.CodeAccepted:
		return 202
	case ctl.CodeBadRequest:
		return 400
	case 401:
		return 401
	case 403:
		return 403
	case 404:
		return 404
	case ctl.CodeNotSupported:
		return 405
	case ctl.CodeExpired:
		return 410
	case 429:
		return 429
	case ctl.CodeBusy:
		return 503
	case ctl.CodeTimeout:
		return 504
	default:
		return 500
	}
}

func propertyGetSelection(device contracts.DeviceConfig, data map[string]interface{}) (map[string]interface{}, error) {
	properties, ok := data["properties"]
	if !ok {
		return nil, fmt.Errorf("data.properties is required")
	}
	switch items := properties.(type) {
	case []string:
		return cfg.BuildPropertyReadSelectionFromNames(device, items)
	case []interface{}:
		names := make([]string, 0, len(items))
		for _, item := range items {
			name, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("data.properties must contain strings")
			}
			names = append(names, name)
		}
		return cfg.BuildPropertyReadSelectionFromNames(device, names)
	default:
		return nil, fmt.Errorf("data.properties must be an array")
	}
}

func propertyGetNames(device contracts.DeviceConfig, data map[string]interface{}) ([]string, error) {
	selection, err := propertyGetSelection(device, data)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(selection))
	for name := range selection {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func shouldAsyncPropertyGet(req rtapi.PropertyRequest) bool {
	return req.Metadata != nil && req.Metadata.ExpectAck
}

func shouldAsyncPropertySet(req rtapi.PropertyRequest) bool {
	return req.Metadata != nil && req.Metadata.ExpectAck
}

func (s *Service) executeAsyncProperty(device contracts.DeviceConfig, req rtapi.PropertyRequest, operation string) (rtapi.PropertySetResponse, int) {
	accepted := newControlResult(req.TraceID, ctl.CodeAccepted, "accepted", map[string]interface{}{"accepted": true})
	pending := rtcontrol.PendingProperty{
		TraceID:     req.TraceID,
		DeviceCode:  req.DeviceCode,
		ProductCode: device.ProductCode,
		Operation:   operation,
		Request:     ctl.Request(req),
		CreatedAt:   accepted.Time,
		UpdatedAt:   accepted.Time,
	}
	if s.store != nil {
		created, err := s.store.SavePendingProperty(pending)
		if err != nil {
			result := newControlResult(req.TraceID, ctl.CodeDriverError, err.Error(), nil)
			s.record(req.DeviceCode, device.ProductCode, req.TraceID, result, operation)
			return result, httpStatusForCode(result.Code)
		}
		if created && s.logger != nil {
			s.logger.Infof("Queued async property %s trace=%s device=%s", operation, req.TraceID, req.DeviceCode)
		}
	}
	s.record(req.DeviceCode, device.ProductCode, req.TraceID, accepted, operation)
	if s.markAsyncActive(req.TraceID) {
		go s.runAsyncProperty(pending)
	}
	return accepted, httpStatusForCode(accepted.Code)
}

func (s *Service) runAsyncProperty(pending rtcontrol.PendingProperty) {
	defer s.clearAsyncActive(pending.TraceID)
	result := s.executePendingProperty(pending, func(event propertyProgressEvent) {
		s.recordPropertyProgress(pending, event)
	})
	applied := s.record(pending.DeviceCode, pending.ProductCode, pending.TraceID, result, pending.Operation)
	if rtcontrol.IsFinalCode(result.Code) && s.store != nil {
		if err := s.store.DeletePendingProperty(pending.TraceID); err != nil && s.logger != nil {
			s.logger.Warnf("Failed to delete pending property trace=%s: %v", pending.TraceID, err)
		}
	}
	if !applied || !rtcontrol.IsFinalCode(result.Code) {
		return
	}
	if result.Code == ctl.CodeNotFound {
		return
	}
	s.publishPropertyResult(pending.ProductCode, pending.DeviceCode, result)
}

func (s *Service) executePendingProperty(pending rtcontrol.PendingProperty, progress func(propertyProgressEvent)) rtapi.PropertyResponse {
	switch pending.Operation {
	case propertyOperationGet:
		return s.executePendingPropertyGet(pending, progress)
	default:
		return s.executePendingPropertySet(pending, progress)
	}
}

func (s *Service) executePendingPropertySet(pending rtcontrol.PendingProperty, progress func(propertyProgressEvent)) rtapi.PropertyResponse {
	taskStart := time.Now()
	device, req, statusCode, err := s.resolvePropertyDevice(rtapi.PropertyRequest(pending.Request), pending.ProductCode)
	if err != nil {
		return newControlResult(req.TraceID, statusCode, err.Error(), nil)
	}
	propertyNames := propertyNames(req.Data)

	writeStart := time.Now()
	if progress != nil {
		progress(newPropertyProgressEvent("write", "started", 10, propertyNames, nil, taskStart, writeStart))
	}
	commandReqs, params, err := cfg.BuildPropertyWriteRequests(device, req.Data)
	if err != nil {
		result := newControlResult(req.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("write", "failed", 10, propertyNames, nil, taskStart, writeStart))
	}
	if err := s.driver.HandleWriteCommands(device.InternalName, cfg.ProtocolPropertiesFromConfig(device), commandReqs, params); err != nil {
		result := newControlResult(req.TraceID, ctl.CodeDriverError, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("write", "failed", 10, propertyNames, nil, taskStart, writeStart))
	}
	if progress != nil {
		progress(newPropertyProgressEvent("write", "completed", 55, propertyNames, req.Data, taskStart, writeStart))
	}
	if s.setPostDelay > 0 {
		time.Sleep(s.setPostDelay)
	}

	readbackStart := time.Now()
	if progress != nil {
		progress(newPropertyProgressEvent("readback", "started", 75, propertyNames, nil, taskStart, readbackStart))
	}
	result := s.executePropertySetReadback(device.ProductCode, req, req.TraceID)
	if result.Code != ctl.CodeSuccess {
		return enrichPropertyFailure(result, newPropertyProgressEvent("readback", "failed", 75, propertyNames, nil, taskStart, readbackStart))
	}
	if progress != nil {
		progress(newPropertyProgressEvent("readback", "completed", 100, propertyNames, result.Data, taskStart, readbackStart))
	}
	return result
}

func (s *Service) executePendingPropertyGet(pending rtcontrol.PendingProperty, progress func(propertyProgressEvent)) rtapi.PropertyResponse {
	taskStart := time.Now()
	device, req, statusCode, err := s.resolvePropertyDevice(rtapi.PropertyRequest(pending.Request), pending.ProductCode)
	if err != nil {
		return newControlResult(req.TraceID, statusCode, err.Error(), nil)
	}
	propertyNames, err := propertyGetNames(device, req.Data)
	if err != nil {
		result := newControlResult(req.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("read", "failed", 10, nil, nil, taskStart, taskStart))
	}

	readStart := time.Now()
	if progress != nil {
		progress(newPropertyProgressEvent("read", "started", 10, propertyNames, nil, taskStart, readStart))
	}
	selection, err := propertyGetSelection(device, req.Data)
	if err != nil {
		result := newControlResult(req.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("read", "failed", 10, propertyNames, nil, taskStart, readStart))
	}
	commandReqs, bindings, err := cfg.BuildPropertyReadRequests(device, selection)
	if err != nil {
		result := newControlResult(req.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("read", "failed", 10, propertyNames, nil, taskStart, readStart))
	}
	values, err := s.driver.HandleReadCommands(device.InternalName, cfg.ProtocolPropertiesFromConfig(device), commandReqs)
	if err != nil {
		result := newControlResult(req.TraceID, ctl.CodeDriverError, err.Error(), nil)
		return enrichPropertyFailure(result, newPropertyProgressEvent("read", "failed", 10, propertyNames, nil, taskStart, readStart))
	}
	result := newControlResult(req.TraceID, ctl.CodeSuccess, "success", cfg.BuildPropertyResponse(values, bindings))
	if progress != nil {
		progress(newPropertyProgressEvent("read", "completed", 100, propertyNames, result.Data, taskStart, readStart))
	}
	return result
}

func (s *Service) executePropertySetReadback(productCode string, req rtapi.PropertyRequest, traceID string) rtapi.PropertyResponse {
	post := newControlResult(traceID, ctl.CodeSuccess, "success", map[string]interface{}{})
	device, normalized, _, err := s.resolvePropertyDevice(req, productCode)
	if err != nil {
		post.Code = ctl.CodeBadRequest
		post.Message = err.Error()
		return post
	}
	selection := cfg.BuildPropertyReadSelection(normalized.Data)
	commandReqs, bindings, err := cfg.BuildPropertyReadRequests(device, selection)
	if err != nil {
		post.Code = ctl.CodeBadRequest
		post.Message = err.Error()
		return post
	}
	values, err := s.driver.HandleReadCommands(device.InternalName, cfg.ProtocolPropertiesFromConfig(device), commandReqs)
	if err != nil {
		post.Code = ctl.CodeDriverError
		post.Message = err.Error()
		return post
	}
	post.Data = cfg.BuildPropertyResponse(values, bindings)
	return post
}

func (s *Service) loadExistingResult(traceID string) (rtapi.PropertyResponse, bool) {
	if s == nil || s.store == nil || strings.TrimSpace(traceID) == "" {
		return rtapi.PropertyResponse{}, false
	}
	job, found, err := s.store.LoadJob(traceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to load property job trace=%s: %v", traceID, err)
		}
		return rtapi.PropertyResponse{}, false
	}
	if !found {
		return rtapi.PropertyResponse{}, false
	}
	result, foundResult, err := s.store.LoadLatestResult(traceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to load property result trace=%s: %v", traceID, err)
		}
		return rtapi.PropertyResponse{}, false
	}
	if foundResult {
		return rtapi.PropertyResponse(result), true
	}
	return newControlResult(traceID, job.Code, job.Message, map[string]interface{}{}), true
}

func (s *Service) record(deviceCode string, productCode string, traceID string, result rtapi.PropertyResponse, operation string) bool {
	if strings.TrimSpace(traceID) == "" {
		return false
	}
	if s.store == nil {
		return true
	}
	now := result.Time
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	job := rtcontrol.JobState{
		TraceID:     traceID,
		DeviceCode:  strings.TrimSpace(deviceCode),
		ProductCode: strings.TrimSpace(productCode),
		Kind:        "property:" + normalizePropertyOperation(operation),
		Code:        result.Code,
		Message:     result.Message,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if rtcontrol.IsFinalCode(result.Code) {
		job.FinishedAt = now
	}
	applied, err := s.store.UpsertJob(job)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to upsert property job trace=%s: %v", traceID, err)
		}
		return false
	}
	if !applied {
		return false
	}
	if err := s.store.SaveResult(traceID, ctl.Result(result), rtcontrol.IsFinalCode(result.Code)); err != nil && s.logger != nil {
		s.logger.Warnf("Failed to save property result trace=%s: %v", traceID, err)
	}
	return true
}

func normalizePropertyOperation(operation string) string {
	switch strings.TrimSpace(operation) {
	case propertyOperationGet:
		return propertyOperationGet
	default:
		return propertyOperationSet
	}
}

func (s *Service) markAsyncActive(traceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.activeAsync[traceID]; ok {
		return false
	}
	s.activeAsync[traceID] = struct{}{}
	return true
}

func (s *Service) clearAsyncActive(traceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeAsync, traceID)
}
