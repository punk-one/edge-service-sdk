package property

import (
	"context"
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

type driverFaultReporter interface {
	ReportDriverFault(deviceName, operation string, err error)
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
	deliveryMu  sync.Mutex
	activeAsync map[string]struct{}
	closed      bool
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
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
	serviceCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		catalog:      catalog,
		driver:       driver,
		publisher:    publisher,
		store:        store,
		logger:       logClient,
		setPostDelay: time.Second,
		activeAsync:  make(map[string]struct{}),
		ctx:          serviceCtx,
		cancel:       cancel,
	}
}

func (s *Service) ExecuteGet(req rtapi.PropertyRequest, expectedProductCode string) (rtapi.PropertyResponse, int) {
	if !s.beginRequest() {
		result := newControlResult(req.TraceID, ctl.CodeBusy, "property service is shutting down", nil)
		return result, httpStatusForCode(result.Code)
	}
	defer s.wg.Done()
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
	processing := newControlResult(normalized.TraceID, ctl.CodeProcessing, "processing", map[string]interface{}{})
	if !s.claimExecution(normalized.DeviceCode, device.ProductCode, normalized.TraceID, processing, propertyOperationGet) {
		if existing, ok := s.loadExistingResult(normalized.TraceID); ok {
			return existing, httpStatusForCode(existing.Code)
		}
		busy := newControlResult(normalized.TraceID, ctl.CodeBusy, "property read could not be claimed", nil)
		return busy, httpStatusForCode(busy.Code)
	}

	values, err := s.readDriver(device, commandReqs, normalized.Metadata)
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
	if !s.beginRequest() {
		result := newControlResult(req.TraceID, ctl.CodeBusy, "property service is shutting down", nil)
		return result, httpStatusForCode(result.Code)
	}
	defer s.wg.Done()
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
	processing := newControlResult(normalized.TraceID, ctl.CodeProcessing, "processing", map[string]interface{}{})
	if !s.claimExecution(normalized.DeviceCode, device.ProductCode, normalized.TraceID, processing, propertyOperationSet) {
		if existing, ok := s.loadExistingResult(normalized.TraceID); ok {
			return existing, httpStatusForCode(existing.Code)
		}
		busy := newControlResult(normalized.TraceID, ctl.CodeBusy, "property write could not be claimed", nil)
		return busy, httpStatusForCode(busy.Code)
	}

	if err := s.writeDriver(device, commandReqs, params, normalized.Metadata); err != nil {
		result := driverFailureResult(normalized.TraceID, err)
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
	if registrar, ok := s.publisher.(interface{ RegisterOnConnect(func()) }); ok {
		registrar.RegisterOnConnect(func() {
			if err := s.ResumeResultDeliveries(); err != nil && s.logger != nil {
				s.logger.Warnf("Failed to replay property results after MQTT reconnect: %v", err)
			}
		})
	}

	for _, productCode := range s.catalog.ProductCodes() {
		if config.PropertySet.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertySet.Topic, productCode)
			if err := s.publisher.Subscribe(topic, byte(config.PropertySet.QoS), func(actualTopic string, payload []byte) {
				mqtt.ObserveInbound(s.publisher, mqtt.Observation{
					Type:        busapi.PropertySet,
					Topic:       actualTopic,
					QoS:         byte(config.PropertySet.QoS),
					Payload:     append([]byte(nil), payload...),
					DataFormat:  "json",
					ProductCode: productCode,
				})
				s.handlePropertySet(productCode, payload)
			}); err != nil && s.logger != nil {
				s.logger.Warnf("Property-set subscription is pending retry: topic=%s err=%v", topic, err)
			}
		}

		if config.PropertyGet.Topic != "" && config.PropertyResult.Topic != "" {
			topic := cfg.StringsReplaceProductCode(config.PropertyGet.Topic, productCode)
			if err := s.publisher.Subscribe(topic, byte(config.PropertyGet.QoS), func(actualTopic string, payload []byte) {
				mqtt.ObserveInbound(s.publisher, mqtt.Observation{
					Type:        busapi.PropertyGet,
					Topic:       actualTopic,
					QoS:         byte(config.PropertyGet.QoS),
					Payload:     append([]byte(nil), payload...),
					DataFormat:  "json",
					ProductCode: productCode,
				})
				s.handlePropertyGet(productCode, payload)
			}); err != nil && s.logger != nil {
				s.logger.Warnf("Property-get subscription is pending retry: topic=%s err=%v", topic, err)
			}
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
		s.startAsyncProperty(item)
	}
	return nil
}

// ResumeResultDeliveries replays durable final property results.
func (s *Service) ResumeResultDeliveries() error {
	if !s.beginRequest() {
		return fmt.Errorf("property service is shutting down")
	}
	defer s.wg.Done()
	outbox, ok := s.store.(rtcontrol.ResultOutbox)
	if !ok {
		return nil
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	for {
		items, err := outbox.ListResultDeliveries("property", 100)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for _, item := range items {
			if err := s.publishPropertyResultLocked(item.ProductCode, item.DeviceCode, rtapi.PropertyResponse(item.Result)); err != nil {
				return err
			}
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
		return contracts.DeviceConfig{}, req, 404, fmt.Errorf("%w: %s", ErrDeviceNotFound, req.DeviceCode)
	}
	if expectedProductCode != "" && device.ProductCode != expectedProductCode {
		return contracts.DeviceConfig{}, req, 400, fmt.Errorf("device_code does not match the subscribed topic")
	}
	return device, req, 200, nil
}

func (s *Service) handlePropertySet(productCode string, payload []byte) {
	if !s.beginRequest() {
		return
	}
	defer s.wg.Done()
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
	if !s.beginRequest() {
		return
	}
	defer s.wg.Done()
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		timer := time.NewTimer(s.setPostDelay)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		post := s.executePropertySetReadback(productCode, req, response.TraceID)
		s.record(req.DeviceCode, productCode, response.TraceID, post, propertyOperationSet)
		s.publishPropertyResult(productCode, resolvedDeviceCode(req.DeviceCode, req.DeviceCode), post)
	}()
}

func (s *Service) publishPropertyResult(productCode string, deviceCode string, response rtapi.PropertyResponse) error {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	return s.publishPropertyResultLocked(productCode, deviceCode, response)
}

func (s *Service) publishPropertyResultLocked(productCode string, deviceCode string, response rtapi.PropertyResponse) error {
	if s == nil || s.publisher == nil || !s.propertyResultEnabled {
		s.finishResultDelivery(response.TraceID, nil)
		return nil
	}
	device := contracts.DeviceConfig{
		Name:        deviceCode,
		ProductCode: productCode,
	}
	if resolved, ok := s.catalog.DeviceConfigByName(deviceCode); ok && resolved.ProductCode == productCode {
		device = resolved
	}
	err := s.publisher.PublishPropertyResult(device, map[string]interface{}{
		"trace_id": response.TraceID,
		"code":     response.Code,
		"message":  response.Message,
		"data":     response.Data,
		"time":     response.Time,
	})
	s.finishResultDelivery(response.TraceID, err)
	return err
}

func (s *Service) finishResultDelivery(traceID string, publishErr error) {
	outbox, ok := s.store.(rtcontrol.ResultOutbox)
	if !ok {
		return
	}
	if publishErr != nil {
		_ = outbox.MarkResultDeliveryFailed(traceID, publishErr.Error())
		return
	}
	if err := outbox.AckResultDelivery(traceID); err != nil && s.logger != nil {
		s.logger.Warnf("Failed to ack property result delivery trace=%s: %v", traceID, err)
	}
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
	case ctl.CodeAmbiguous:
		return 409
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
		s.startAsyncProperty(pending)
	}
	return accepted, httpStatusForCode(accepted.Code)
}

func (s *Service) runAsyncProperty(pending rtcontrol.PendingProperty) {
	defer s.clearAsyncActive(pending.TraceID)
	processing := newControlResult(pending.TraceID, ctl.CodeProcessing, "processing", map[string]interface{}{})
	if !s.claimExecution(pending.DeviceCode, pending.ProductCode, pending.TraceID, processing, pending.Operation) {
		return
	}
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

func (s *Service) startAsyncProperty(pending rtcontrol.PendingProperty) {
	go func() {
		defer s.wg.Done()
		s.runAsyncProperty(pending)
	}()
}

// Close stops accepting new asynchronous work and waits for active requests.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.BeginShutdown()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BeginShutdown rejects new work and cancels context-aware in-flight work
// without waiting. It is safe to call before stopping the protocol driver.
func (s *Service) BeginShutdown() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
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
	if err := s.writeDriver(device, commandReqs, params, req.Metadata); err != nil {
		result := driverFailureResult(req.TraceID, err)
		return enrichPropertyFailure(result, newPropertyProgressEvent("write", "failed", 10, propertyNames, nil, taskStart, writeStart))
	}
	if progress != nil {
		progress(newPropertyProgressEvent("write", "completed", 55, propertyNames, req.Data, taskStart, writeStart))
	}
	if s.setPostDelay > 0 {
		timer := time.NewTimer(s.setPostDelay)
		select {
		case <-s.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			result := newControlResult(req.TraceID, ctl.CodeAmbiguous, "property write completed but readback was cancelled", nil)
			return enrichPropertyFailure(result, newPropertyProgressEvent("readback", "cancelled", 55, propertyNames, req.Data, taskStart, writeStart))
		case <-timer.C:
		}
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
	values, err := s.readDriver(device, commandReqs, req.Metadata)
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
	values, err := s.readDriver(device, commandReqs, normalized.Metadata)
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
	if recorder, ok := s.store.(rtcontrol.AtomicRecorder); ok {
		applied, err := recorder.RecordJobResult(job, ctl.Result(result), rtcontrol.IsFinalCode(result.Code))
		if err != nil && s.logger != nil {
			s.logger.Warnf("Failed to atomically record property result trace=%s: %v", traceID, err)
		}
		return err == nil && applied
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

func (s *Service) claimExecution(deviceCode, productCode, traceID string, result rtapi.PropertyResponse, operation string) bool {
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
		Code:        ctl.CodeProcessing,
		Message:     result.Message,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if claimer, ok := s.store.(rtcontrol.ExecutionClaimer); ok {
		claimed, err := claimer.ClaimExecution(job)
		if err != nil && s.logger != nil {
			s.logger.Warnf("Failed to claim property execution trace=%s: %v", traceID, err)
		}
		return err == nil && claimed
	}
	return s.record(deviceCode, productCode, traceID, result, operation)
}

func normalizePropertyOperation(operation string) string {
	switch strings.TrimSpace(operation) {
	case propertyOperationGet:
		return propertyOperationGet
	default:
		return propertyOperationSet
	}
}

func (s *Service) readDriver(device contracts.DeviceConfig, reqs []contracts.CommandRequest, metadata *ctl.Metadata) ([]*contracts.CommandValue, error) {
	ctx, cancel := controlOperationContext(s.ctx, metadata)
	defer cancel()
	values, err := contracts.HandleReadCommandsWithContext(ctx, s.driver, device.InternalName, cfg.ProtocolPropertiesFromConfig(device), reqs)
	s.reportDriverFault(device.InternalName, "property-read", err)
	return values, err
}

func (s *Service) writeDriver(device contracts.DeviceConfig, reqs []contracts.CommandRequest, params []*contracts.CommandValue, metadata *ctl.Metadata) error {
	ctx, cancel := controlOperationContext(s.ctx, metadata)
	defer cancel()
	err := contracts.HandleWriteCommandsWithContext(ctx, s.driver, device.InternalName, cfg.ProtocolPropertiesFromConfig(device), reqs, params)
	s.reportDriverFault(device.InternalName, "property-write", err)
	return err
}

func (s *Service) reportDriverFault(deviceName, operation string, err error) {
	if err == nil || !errors.Is(err, contracts.ErrOperationStuck) {
		return
	}
	if reporter, ok := s.catalog.(driverFaultReporter); ok {
		reporter.ReportDriverFault(deviceName, operation, err)
	}
}

func controlOperationContext(parent context.Context, metadata *ctl.Metadata) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := contracts.DefaultOperationTimeout
	if metadata != nil && metadata.ExpiryTime > 0 {
		remaining := time.Until(time.UnixMilli(metadata.ExpiryTime))
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(parent, timeout)
}

func driverFailureResult(traceID string, err error) rtapi.PropertyResponse {
	code := ctl.CodeDriverError
	switch {
	case contracts.IsAmbiguousWrite(err):
		code = ctl.CodeAmbiguous
	case errors.Is(err, contracts.ErrLegacyOperationBusy):
		code = ctl.CodeBusy
	case errors.Is(err, contracts.ErrOperationTimeout):
		code = ctl.CodeTimeout
	}
	return newControlResult(traceID, code, err.Error(), nil)
}

func (s *Service) markAsyncActive(traceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if _, ok := s.activeAsync[traceID]; ok {
		return false
	}
	s.activeAsync[traceID] = struct{}{}
	s.wg.Add(1)
	return true
}

func (s *Service) beginRequest() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.wg.Add(1)
	return true
}

func (s *Service) clearAsyncActive(traceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeAsync, traceID)
}
