package command

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	cfg "github.com/punk-one/edge-service-sdk/config"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
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
	catalog              DeviceCatalog
	driver               contracts.ProtocolDriver
	publisher            mqtt.Publisher
	store                rtcontrol.Store
	registry             cmdapi.Registry
	logger               logger.LoggingClient
	commandResultEnabled bool

	mu          sync.Mutex
	activeAsync map[string]struct{}
}

func NewService(catalog DeviceCatalog, driver contracts.ProtocolDriver, publisher mqtt.Publisher, store rtcontrol.Store, logClient logger.LoggingClient, registry cmdapi.Registry) *Service {
	if registry == nil {
		registry = cmdapi.NewRegistry()
	}
	return &Service{
		catalog:     catalog,
		driver:      driver,
		publisher:   publisher,
		store:       store,
		registry:    registry,
		logger:      logClient,
		activeAsync: make(map[string]struct{}),
	}
}

func (s *Service) Execute(identifier string, req cmdapi.CommandRequest, expectedProductCode string) (cmdapi.CommandResponse, int) {
	if existing, ok := s.loadExistingResult(req.TraceID); ok {
		return existing, httpStatusForCode(existing.Code)
	}
	if expired := expiredRequestResult(req); expired != nil {
		s.record(req.DeviceCode, expectedProductCode, strings.TrimSpace(identifier), req.TraceID, *expired)
		return *expired, httpStatusForCode(expired.Code)
	}
	if invalid := validateStrategy(req); invalid != nil {
		s.record(req.DeviceCode, expectedProductCode, strings.TrimSpace(identifier), req.TraceID, *invalid)
		return *invalid, httpStatusForCode(invalid.Code)
	}

	device, normalized, statusCode, err := s.resolveCommandDevice(req, expectedProductCode)
	if err != nil {
		result := newControlResult(normalized.TraceID, statusCode, err.Error(), nil)
		s.record(normalized.DeviceCode, expectedProductCode, strings.TrimSpace(identifier), normalized.TraceID, result)
		return result, httpStatusForCode(statusCode)
	}
	if !deviceSupportsCommand(device, identifier) {
		result := newControlResult(normalized.TraceID, ctl.CodeNotSupported, fmt.Sprintf("command %q is not supported by device %q", strings.TrimSpace(identifier), device.Name), nil)
		s.record(normalized.DeviceCode, device.ProductCode, strings.TrimSpace(identifier), normalized.TraceID, result)
		return result, httpStatusForCode(result.Code)
	}

	desc, cmd, ok := s.lookupRegisteredCommand(identifier)
	if !ok {
		result := newControlResult(normalized.TraceID, ctl.CodeNotSupported, fmt.Sprintf("command %q is not supported", strings.TrimSpace(identifier)), nil)
		s.record(normalized.DeviceCode, device.ProductCode, strings.TrimSpace(identifier), normalized.TraceID, result)
		return result, httpStatusForCode(result.Code)
	}
	if err := validateInputParams(desc.InputParams, normalized.Data); err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, desc.Identifier, normalized.TraceID, result)
		return result, httpStatusForCode(result.Code)
	}
	if err := validateInputSchema(desc.InputSchema, normalized.Data); err != nil {
		result := newControlResult(normalized.TraceID, ctl.CodeBadRequest, err.Error(), nil)
		s.record(normalized.DeviceCode, device.ProductCode, desc.Identifier, normalized.TraceID, result)
		return result, httpStatusForCode(result.Code)
	}

	switch desc.Mode {
	case "async":
		return s.executeAsync(device, desc, normalized)
	default:
		return s.executeSync(device, desc, cmd, normalized)
	}
}

func (s *Service) RegisterMQTTHandlers(config rtconfig.Config) {
	if s.publisher == nil {
		return
	}
	s.commandResultEnabled = strings.TrimSpace(config.CommandResult.Topic) != ""
	if strings.TrimSpace(config.CommandCall.Topic) == "" {
		return
	}

	for _, productCode := range s.catalog.ProductCodes() {
		subscribeTopic := strings.ReplaceAll(cfg.StringsReplaceProductCode(config.CommandCall.Topic, productCode), "{identifier}", "+")
		callTemplate := config.CommandCall.Topic
		_ = s.publisher.Subscribe(subscribeTopic, byte(config.CommandCall.QoS), func(topic string, payload []byte) {
			identifier, ok := extractIdentifier(callTemplate, productCode, topic)
			if !ok {
				if s.logger != nil {
					s.logger.Warnf("Failed to resolve command identifier from topic=%s product=%s", topic, productCode)
				}
				return
			}
			s.handleCommandCall(productCode, identifier, payload)
		})
	}
}

func (s *Service) ResumePending() error {
	if s == nil || s.store == nil {
		return nil
	}
	items, err := s.store.ListPendingCommands()
	if err != nil {
		return err
	}
	for _, item := range items {
		job, found, loadErr := s.store.LoadJob(item.TraceID)
		if loadErr == nil && found && rtcontrol.IsFinalCode(job.Code) {
			_ = s.store.DeletePendingCommand(item.TraceID)
			continue
		}
		if !s.markAsyncActive(item.TraceID) {
			continue
		}
		go s.runAsyncPending(item)
	}
	return nil
}

func (s *Service) handleCommandCall(productCode string, identifier string, payload []byte) {
	var req cmdapi.CommandRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to parse command payload for product %s identifier %s: %v", productCode, identifier, err)
		}
		return
	}
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}
	result, _ := s.Execute(identifier, req, productCode)
	if !s.commandResultEnabled {
		return
	}
	if result.Code == ctl.CodeNotFound {
		return
	}
	deviceCode := strings.TrimSpace(req.DeviceCode)
	if deviceCode == "" {
		return
	}
	device := contracts.DeviceConfig{Name: deviceCode, ProductCode: productCode}
	if resolved, ok := s.catalog.DeviceConfigByName(deviceCode); ok && resolved.ProductCode == productCode {
		device = resolved
	}
	_ = s.publisher.PublishCommandResult(device, map[string]interface{}{
		"trace_id": result.TraceID,
		"code":     result.Code,
		"message":  result.Message,
		"data":     ensureDataMap(result.Data),
		"time":     result.Time,
	})
}

func (s *Service) resolveCommandDevice(req cmdapi.CommandRequest, expectedProductCode string) (contracts.DeviceConfig, cmdapi.CommandRequest, int, error) {
	req.DeviceCode = strings.TrimSpace(req.DeviceCode)
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}
	if req.DeviceCode == "" {
		return contracts.DeviceConfig{}, req, ctl.CodeBadRequest, fmt.Errorf("device_code is required")
	}
	device, ok := s.catalog.DeviceConfigByName(req.DeviceCode)
	if !ok {
		return contracts.DeviceConfig{}, req, ctl.CodeNotFound, fmt.Errorf("device_code does not match any configured device")
	}
	if expectedProductCode != "" && device.ProductCode != expectedProductCode {
		return contracts.DeviceConfig{}, req, ctl.CodeBadRequest, fmt.Errorf("device_code does not match the subscribed topic")
	}
	return device, req, ctl.CodeSuccess, nil
}

func deviceSupportsCommand(device contracts.DeviceConfig, identifier string) bool {
	_, ok := cfg.FindCommandByIdentifier(device, identifier)
	return ok
}

func expiredRequestResult(req cmdapi.CommandRequest) *cmdapi.CommandResponse {
	if req.Metadata == nil || req.Metadata.ExpiryTime <= 0 {
		return nil
	}
	if time.Now().UnixMilli() <= req.Metadata.ExpiryTime {
		return nil
	}
	result := newControlResult(req.TraceID, ctl.CodeExpired, "request expired", nil)
	return &result
}

func validateStrategy(req cmdapi.CommandRequest) *cmdapi.CommandResponse {
	if req.Metadata == nil {
		return nil
	}
	strategy := strings.TrimSpace(req.Metadata.Strategy)
	if strategy == "" || strategy == "always_realtime" {
		return nil
	}
	result := newControlResult(req.TraceID, ctl.CodeBadRequest, fmt.Sprintf("unsupported strategy %q", strategy), nil)
	return &result
}

func newControlResult(traceID string, code int, message string, data map[string]interface{}) cmdapi.CommandResponse {
	if data == nil {
		data = map[string]interface{}{}
	}
	return cmdapi.CommandResponse{
		TraceID: traceID,
		Code:    code,
		Message: message,
		Data:    ensureDataMap(data),
		Time:    time.Now().UnixMilli(),
	}
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
	case ctl.CodeNotFound:
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

func extractIdentifier(template string, productCode string, topic string) (string, bool) {
	template = cfg.StringsReplaceProductCode(template, productCode)
	templateParts := strings.Split(strings.Trim(template, "/"), "/")
	topicParts := strings.Split(strings.Trim(topic, "/"), "/")
	if len(templateParts) != len(topicParts) {
		return "", false
	}
	identifier := ""
	for i := range templateParts {
		if templateParts[i] == "{identifier}" {
			identifier = topicParts[i]
			continue
		}
		if templateParts[i] != topicParts[i] {
			return "", false
		}
	}
	identifier = strings.TrimSpace(identifier)
	return identifier, identifier != ""
}

func (s *Service) executeSync(device contracts.DeviceConfig, desc cmdapi.CommandDescriptor, cmd cmdapi.Command, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int) {
	resultData, cmdErr := s.executeRegisteredCommand(device, desc, cmd, req, nil)
	if cmdErr != nil {
		result := newControlResult(req.TraceID, normalizedErrorCode(cmdErr.Code), cmdErr.Error(), cmdErr.Data)
		s.record(req.DeviceCode, device.ProductCode, desc.Identifier, req.TraceID, result)
		return result, httpStatusForCode(result.Code)
	}
	result := newControlResult(req.TraceID, ctl.CodeSuccess, "success", resultData)
	s.record(req.DeviceCode, device.ProductCode, desc.Identifier, req.TraceID, result)
	return result, httpStatusForCode(result.Code)
}

func (s *Service) executeAsync(device contracts.DeviceConfig, desc cmdapi.CommandDescriptor, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int) {
	accepted := newControlResult(req.TraceID, ctl.CodeAccepted, "accepted", map[string]interface{}{"accepted": true})
	pending := rtcontrol.PendingCommand{
		TraceID:     req.TraceID,
		DeviceCode:  req.DeviceCode,
		ProductCode: device.ProductCode,
		Identifier:  desc.Identifier,
		Request:     ctl.Request(req),
		CreatedAt:   accepted.Time,
		UpdatedAt:   accepted.Time,
	}
	if s.store != nil {
		created, err := s.store.SavePendingCommand(pending)
		if err != nil {
			result := newControlResult(req.TraceID, ctl.CodeDriverError, err.Error(), nil)
			s.record(req.DeviceCode, device.ProductCode, desc.Identifier, req.TraceID, result)
			return result, httpStatusForCode(result.Code)
		}
		if created && s.logger != nil {
			s.logger.Infof("Queued async command trace=%s identifier=%s device=%s", req.TraceID, desc.Identifier, req.DeviceCode)
		}
	}
	s.record(req.DeviceCode, device.ProductCode, desc.Identifier, req.TraceID, accepted)
	if s.markAsyncActive(req.TraceID) {
		go s.runAsyncPending(pending)
	}
	return accepted, httpStatusForCode(accepted.Code)
}

func (s *Service) runAsyncPending(pending rtcontrol.PendingCommand) {
	defer s.clearAsyncActive(pending.TraceID)
	result := s.executePendingCommand(pending)
	applied := s.record(pending.DeviceCode, pending.ProductCode, pending.Identifier, pending.TraceID, result)
	if rtcontrol.IsFinalCode(result.Code) && s.store != nil {
		if err := s.store.DeletePendingCommand(pending.TraceID); err != nil && s.logger != nil {
			s.logger.Warnf("Failed to delete pending command trace=%s: %v", pending.TraceID, err)
		}
	}
	if !applied || !rtcontrol.IsFinalCode(result.Code) {
		return
	}
	if err := s.publishAsyncResult(pending, result); err != nil && s.logger != nil {
		s.logger.Warnf("Failed to publish async command result trace=%s: %v", pending.TraceID, err)
		return
	}
	if s.logger != nil {
		s.logger.Infof("Async command completed trace=%s identifier=%s code=%d", pending.TraceID, pending.Identifier, result.Code)
	}
}

func (s *Service) executePendingCommand(pending rtcontrol.PendingCommand) cmdapi.CommandResponse {
	device, ok := s.catalog.DeviceConfigByName(strings.TrimSpace(pending.DeviceCode))
	if !ok {
		return newControlResult(pending.TraceID, ctl.CodeNotFound, "device_code does not match any configured device", nil)
	}
	if strings.TrimSpace(pending.ProductCode) != "" && device.ProductCode != strings.TrimSpace(pending.ProductCode) {
		return newControlResult(pending.TraceID, ctl.CodeBadRequest, "device_code does not match the subscribed topic", nil)
	}
	if !deviceSupportsCommand(device, pending.Identifier) {
		return newControlResult(pending.TraceID, ctl.CodeNotSupported, fmt.Sprintf("command %q is not supported by device %q", strings.TrimSpace(pending.Identifier), device.Name), nil)
	}
	desc, cmd, ok := s.lookupRegisteredCommand(pending.Identifier)
	if !ok {
		return newControlResult(pending.TraceID, ctl.CodeNotSupported, fmt.Sprintf("command %q is not supported", pending.Identifier), nil)
	}
	if err := validateInputParams(desc.InputParams, pending.Request.Data); err != nil {
		return newControlResult(pending.TraceID, ctl.CodeBadRequest, err.Error(), nil)
	}
	if err := validateInputSchema(desc.InputSchema, pending.Request.Data); err != nil {
		return newControlResult(pending.TraceID, ctl.CodeBadRequest, err.Error(), nil)
	}
	resultData, cmdErr := s.executeRegisteredCommand(device, desc, cmd, cmdapi.CommandRequest(pending.Request), func(progress cmdapi.ProgressPayload) {
		s.recordProgress(pending, progress)
	})
	if cmdErr != nil {
		return newControlResult(pending.TraceID, normalizedErrorCode(cmdErr.Code), cmdErr.Error(), cmdErr.Data)
	}
	return newControlResult(pending.TraceID, ctl.CodeSuccess, "success", resultData)
}

func (s *Service) executeRegisteredCommand(device contracts.DeviceConfig, desc cmdapi.CommandDescriptor, cmd cmdapi.Command, req cmdapi.CommandRequest, progress func(cmdapi.ProgressPayload)) (_ map[string]interface{}, cmdErr *cmdapi.CommandError) {
	ctx := &runtimeCommandContext{
		service:  s,
		device:   device,
		req:      req,
		progress: progress,
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			message := fmt.Sprintf("command %s panicked", desc.Identifier)
			if s.logger != nil {
				s.logger.Errorf("%s: %v", message, recovered)
			}
			cmdErr = &cmdapi.CommandError{Code: 500, Message: message}
		}
	}()
	result, err := cmd.Execute(ctx, req)
	if err != nil {
		return nil, err
	}
	return ensureDataMap(result), nil
}

func (s *Service) recordProgress(pending rtcontrol.PendingCommand, progress cmdapi.ProgressPayload) {
	payload := cloneProgress(progress)
	message := "processing"
	if raw, ok := payload["message"].(string); ok && strings.TrimSpace(raw) != "" {
		message = strings.TrimSpace(raw)
	}
	result := newControlResult(pending.TraceID, ctl.CodeProcessing, message, map[string]interface{}{"progress": payload})
	s.record(pending.DeviceCode, pending.ProductCode, pending.Identifier, pending.TraceID, result)
}

func (s *Service) publishAsyncResult(pending rtcontrol.PendingCommand, result cmdapi.CommandResponse) error {
	if s == nil || s.publisher == nil || !s.commandResultEnabled {
		return nil
	}
	device := contracts.DeviceConfig{Name: pending.DeviceCode, ProductCode: pending.ProductCode}
	if resolved, ok := s.catalog.DeviceConfigByName(pending.DeviceCode); ok {
		device = resolved
	}
	return s.publisher.PublishCommandResult(device, map[string]interface{}{
		"trace_id": result.TraceID,
		"code":     result.Code,
		"message":  result.Message,
		"data":     ensureDataMap(result.Data),
		"time":     result.Time,
	})
}

func (s *Service) loadExistingResult(traceID string) (cmdapi.CommandResponse, bool) {
	if s == nil || s.store == nil || strings.TrimSpace(traceID) == "" {
		return cmdapi.CommandResponse{}, false
	}
	job, found, err := s.store.LoadJob(traceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to load command job trace=%s: %v", traceID, err)
		}
		return cmdapi.CommandResponse{}, false
	}
	if !found {
		return cmdapi.CommandResponse{}, false
	}
	result, foundResult, err := s.store.LoadLatestResult(traceID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to load command result trace=%s: %v", traceID, err)
		}
		return cmdapi.CommandResponse{}, false
	}
	if foundResult {
		return cmdapi.CommandResponse(result), true
	}
	return newControlResult(traceID, job.Code, job.Message, map[string]interface{}{}), true
}

func (s *Service) record(deviceCode string, productCode string, identifier string, traceID string, result cmdapi.CommandResponse) bool {
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
		Kind:        rtcontrol.NormalizeKind("command", identifier),
		Identifier:  strings.TrimSpace(identifier),
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
			s.logger.Warnf("Failed to upsert command job trace=%s: %v", traceID, err)
		}
		return false
	}
	if !applied {
		return false
	}
	if err := s.store.SaveResult(traceID, ctl.Result(result), rtcontrol.IsFinalCode(result.Code)); err != nil && s.logger != nil {
		s.logger.Warnf("Failed to save command result trace=%s: %v", traceID, err)
	}
	return true
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

func (s *Service) lookupRegisteredCommand(identifier string) (cmdapi.CommandDescriptor, cmdapi.Command, bool) {
	if s == nil || s.registry == nil {
		return cmdapi.CommandDescriptor{}, nil, false
	}
	return s.registry.Lookup(strings.TrimSpace(identifier))
}

func validateInputParams(params []cmdapi.CommandParam, data map[string]interface{}) error {
	if data == nil {
		data = map[string]interface{}{}
	}
	for _, param := range params {
		if !param.Required {
			continue
		}
		value, ok := data[param.Identifier]
		if !ok || value == nil {
			return fmt.Errorf("command input %q is required", param.Identifier)
		}
	}
	return nil
}

func validateInputSchema(schema []cmdapi.SchemaField, data map[string]interface{}) error {
	if data == nil {
		data = map[string]interface{}{}
	}
	for _, field := range schema {
		if !field.Required {
			continue
		}
		value, ok := data[field.Identifier]
		if !ok || value == nil {
			return fmt.Errorf("command input %q is required", field.Identifier)
		}
	}
	return nil
}

func normalizedErrorCode(code int) int {
	if code > 0 {
		return code
	}
	return 500
}

func ensureDataMap(data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return map[string]interface{}{}
	}
	copied := make(map[string]interface{}, len(data))
	for key, value := range data {
		copied[key] = value
	}
	return copied
}

func cloneProgress(progress cmdapi.ProgressPayload) map[string]interface{} {
	if len(progress) == 0 {
		return map[string]interface{}{}
	}
	cloned := make(map[string]interface{}, len(progress))
	for key, value := range progress {
		cloned[key] = value
	}
	return cloned
}

type runtimeCommandContext struct {
	service  *Service
	device   contracts.DeviceConfig
	req      cmdapi.CommandRequest
	progress func(cmdapi.ProgressPayload)
}

func (c *runtimeCommandContext) TraceID() string {
	return c.req.TraceID
}

func (c *runtimeCommandContext) DeviceCode() string {
	return c.req.DeviceCode
}

func (c *runtimeCommandContext) Metadata() *ctl.Metadata {
	return c.req.Metadata
}

func (c *runtimeCommandContext) Logger() logger.LoggingClient {
	if c == nil || c.service == nil {
		return nil
	}
	return c.service.logger
}

func (c *runtimeCommandContext) GetProperties(names []string) (map[string]interface{}, error) {
	selection, err := rtconfig.BuildPropertyReadSelectionFromNames(c.device, names)
	if err != nil {
		return nil, err
	}
	commandReqs, bindings, err := rtconfig.BuildPropertyReadRequests(c.device, selection)
	if err != nil {
		return nil, err
	}
	values, err := c.service.driver.HandleReadCommands(c.device.InternalName, rtconfig.ProtocolPropertiesFromConfig(c.device), commandReqs)
	if err != nil {
		return nil, err
	}
	return rtconfig.BuildPropertyResponse(values, bindings), nil
}

func (c *runtimeCommandContext) SetProperties(values map[string]interface{}) error {
	commandReqs, params, err := rtconfig.BuildPropertyWriteRequests(c.device, values)
	if err != nil {
		return err
	}
	return c.service.driver.HandleWriteCommands(c.device.InternalName, rtconfig.ProtocolPropertiesFromConfig(c.device), commandReqs, params)
}

func (c *runtimeCommandContext) ReadTelemetry(names []string) (map[string]interface{}, error) {
	commandReqs, bindings, err := rtconfig.BuildTelemetryReadRequestsFromNames(c.device, names)
	if err != nil {
		return nil, err
	}
	values, err := c.service.driver.HandleReadCommands(c.device.InternalName, rtconfig.ProtocolPropertiesFromConfig(c.device), commandReqs)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{}, len(bindings))
	for index, name := range bindings {
		if index >= len(values) || values[index] == nil {
			continue
		}
		result[name] = values[index].Value
	}
	return result, nil
}

func (c *runtimeCommandContext) ReportProgress(progress cmdapi.ProgressPayload) {
	if c == nil || c.progress == nil {
		return
	}
	c.progress(progress)
}

func (c *runtimeCommandContext) IsCancelled() bool {
	return false
}
