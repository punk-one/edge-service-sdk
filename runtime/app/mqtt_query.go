package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	cfg "github.com/punk-one/edge-service-sdk/config"
	ctl "github.com/punk-one/edge-service-sdk/control"
	logger "github.com/punk-one/edge-service-sdk/logging"
	httpserver "github.com/punk-one/edge-service-sdk/ops/http"
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type mqttQueryTransport interface {
	Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error
	PublishJSON(topic string, qos byte, retain bool, payload interface{}) error
}

type mqttQueryService struct {
	catalog   *DeviceSDK
	registry  cmdapi.Registry
	store     rtcontrol.Store
	transport mqttQueryTransport
	logger    logger.LoggingClient

	requestTopic mqtt.TopicConfig
	resultTopic  mqtt.TopicConfig
}

func newMQTTQueryService(catalog *DeviceSDK, registry cmdapi.Registry, store rtcontrol.Store, transport mqttQueryTransport, logClient logger.LoggingClient) *mqttQueryService {
	if registry == nil {
		registry = cmdapi.NewRegistry()
	}
	return &mqttQueryService{
		catalog:   catalog,
		registry:  registry,
		store:     store,
		transport: transport,
		logger:    logClient,
	}
}

func (s *mqttQueryService) RegisterMQTTHandlers(config rtconfig.Config) {
	if s == nil || s.catalog == nil || s.transport == nil {
		return
	}
	if strings.TrimSpace(config.QueryRequest.Topic) == "" {
		return
	}
	if strings.TrimSpace(config.QueryResult.Topic) == "" {
		if s.logger != nil {
			s.logger.Warnf("QueryRequest configured but QueryResult topic is empty; disabling MQTT query handlers")
		}
		return
	}

	s.requestTopic = config.QueryRequest
	s.resultTopic = config.QueryResult
	for _, productCode := range s.catalog.ProductCodes() {
		topic := cfg.StringsReplaceProductCode(config.QueryRequest.Topic, productCode)
		_ = s.transport.Subscribe(topic, byte(resolveQueryQoS(config.QueryRequest.QoS)), func(_ string, payload []byte) {
			s.handleQuery(productCode, payload)
		})
	}
}

func (s *mqttQueryService) handleQuery(productCode string, payload []byte) {
	var req ctl.Request
	if err := json.Unmarshal(payload, &req); err != nil {
		if s.logger != nil {
			s.logger.Warnf("Failed to parse MQTT query payload for product %s: %v", productCode, err)
		}
		return
	}
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}
	result := s.executeQuery(productCode, req)
	topic := cfg.StringsReplaceProductCode(s.resultTopic.Topic, productCode)
	if err := s.transport.PublishJSON(topic, byte(resolveQueryQoS(s.resultTopic.QoS)), s.resultTopic.Retain, result); err != nil && s.logger != nil {
		s.logger.Warnf("Failed to publish MQTT query result: product=%s trace_id=%s err=%v", productCode, req.TraceID, err)
	}
}

func (s *mqttQueryService) executeQuery(productCode string, req ctl.Request) ctl.Result {
	traceID := strings.TrimSpace(req.TraceID)
	if traceID == "" {
		return ctl.Result{TraceID: traceID, Code: ctl.CodeBadRequest, Message: "trace_id is required", Data: map[string]interface{}{}, Time: time.Now().UnixMilli()}
	}
	target := strings.TrimSpace(stringValue(req.Data["target"]))
	if target == "" {
		return ctl.Result{TraceID: traceID, Code: ctl.CodeBadRequest, Message: "data.target is required", Data: map[string]interface{}{}, Time: time.Now().UnixMilli()}
	}

	var (
		body   map[string]interface{}
		status int
	)
	switch target {
	case "model.properties":
		body, status = buildPropertyModelQuery(s.catalog)(req.DeviceCode)
	case "model.telemetry":
		body, status = buildTelemetryModelQuery(s.catalog)(req.DeviceCode)
	case "model.commands":
		body, status = buildCommandListQuery(s.catalog, s.registry)(req.DeviceCode)
	case "model.command.detail":
		identifier := strings.TrimSpace(stringValue(req.Data["identifier"]))
		if identifier == "" {
			body, status = errorBody(http.StatusBadRequest, "data.identifier is required"), http.StatusBadRequest
			break
		}
		body, status = buildCommandDetailQuery(s.catalog, s.registry)(req.DeviceCode, identifier)
	case "model.command.input":
		identifier := strings.TrimSpace(stringValue(req.Data["identifier"]))
		if identifier == "" {
			body, status = errorBody(http.StatusBadRequest, "data.identifier is required"), http.StatusBadRequest
			break
		}
		body, status = buildCommandInputQuery(s.catalog, s.registry)(req.DeviceCode, identifier)
	case "model.command.output":
		identifier := strings.TrimSpace(stringValue(req.Data["identifier"]))
		if identifier == "" {
			body, status = errorBody(http.StatusBadRequest, "data.identifier is required"), http.StatusBadRequest
			break
		}
		body, status = buildCommandOutputQuery(s.catalog, s.registry)(req.DeviceCode, identifier)
	case "control.jobs":
		body, status = buildControlJobListQuery(s.store)(mqttControlJobListQuery(req))
	case "control.jobs.diagnostics":
		body, status = buildControlJobDiagnosticsQuery(s.store)(mqttControlJobListQuery(req))
	case "control.job":
		jobTraceID := strings.TrimSpace(stringValue(req.Data["job_trace_id"]))
		if jobTraceID == "" {
			body, status = errorBody(http.StatusBadRequest, "data.job_trace_id is required"), http.StatusBadRequest
			break
		}
		body, status = buildControlJobQuery(s.store)(jobTraceID)
	case "control.job.result":
		jobTraceID := strings.TrimSpace(stringValue(req.Data["job_trace_id"]))
		if jobTraceID == "" {
			body, status = errorBody(http.StatusBadRequest, "data.job_trace_id is required"), http.StatusBadRequest
			break
		}
		body, status = buildControlJobResultQuery(s.store)(jobTraceID)
	case "control.job.events":
		jobTraceID := strings.TrimSpace(stringValue(req.Data["job_trace_id"]))
		if jobTraceID == "" {
			body, status = errorBody(http.StatusBadRequest, "data.job_trace_id is required"), http.StatusBadRequest
			break
		}
		body, status = buildControlJobEventsQuery(s.store)(jobTraceID, intValue(req.Data["limit"]))
	default:
		body, status = errorBody(http.StatusBadRequest, "unsupported data.target"), http.StatusBadRequest
	}
	return mqttQueryResult(traceID, status, body)
}

func mqttQueryResult(traceID string, status int, body map[string]interface{}) ctl.Result {
	message := "success"
	if status >= http.StatusBadRequest {
		message = strings.TrimSpace(stringValue(body["error"]))
		if message == "" {
			message = http.StatusText(status)
		}
	}
	return ctl.Result{
		TraceID: traceID,
		Code:    httpStatusToControlCode(status),
		Message: message,
		Data:    ensureQueryData(body),
		Time:    time.Now().UnixMilli(),
	}
}

func mqttControlJobListQuery(req ctl.Request) httpserver.ControlJobListQuery {
	query := httpserver.ControlJobListQuery{
		DeviceCode:  strings.TrimSpace(req.DeviceCode),
		Kind:        strings.TrimSpace(stringValue(req.Data["kind"])),
		Identifier:  strings.TrimSpace(stringValue(req.Data["identifier"])),
		Limit:       intValue(req.Data["limit"]),
		Offset:      intValue(req.Data["offset"]),
		CreatedFrom: int64Value(req.Data["created_from"]),
		CreatedTo:   int64Value(req.Data["created_to"]),
		UpdatedFrom: int64Value(req.Data["updated_from"]),
		UpdatedTo:   int64Value(req.Data["updated_to"]),
	}
	if value, ok := parseBoolValue(req.Data["final"]); ok {
		query.FinalSet = true
		query.Final = value
	}
	return query
}

func httpStatusToControlCode(status int) int {
	switch status {
	case http.StatusOK:
		return ctl.CodeSuccess
	case http.StatusAccepted:
		return ctl.CodeAccepted
	case http.StatusBadRequest:
		return ctl.CodeBadRequest
	case http.StatusUnauthorized:
		return 401
	case http.StatusForbidden:
		return 403
	case http.StatusNotFound:
		return ctl.CodeNotFound
	case http.StatusMethodNotAllowed:
		return ctl.CodeNotSupported
	case http.StatusGone:
		return ctl.CodeExpired
	case http.StatusTooManyRequests:
		return 429
	case http.StatusServiceUnavailable:
		return ctl.CodeBusy
	case http.StatusGatewayTimeout:
		return ctl.CodeTimeout
	default:
		if status >= 500 {
			return 500
		}
		return ctl.CodeBadRequest
	}
}

func ensureQueryData(body map[string]interface{}) map[string]interface{} {
	if body == nil {
		return map[string]interface{}{}
	}
	copy := make(map[string]interface{}, len(body))
	for key, value := range body {
		copy[key] = value
	}
	return copy
}

func stringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func intValue(value interface{}) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return int(parsed)
		}
	case string:
		parsed, err := json.Number(strings.TrimSpace(typed)).Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return 0
}

func int64Value(value interface{}) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return parsed
		}
	case string:
		parsed, err := json.Number(strings.TrimSpace(typed)).Int64()
		if err == nil {
			return parsed
		}
	}
	return 0
}

func parseBoolValue(value interface{}) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes":
			return true, true
		case "false", "0", "no":
			return false, true
		}
	}
	return false, false
}

func resolveQueryQoS(value int) int {
	if value >= 0 {
		return value
	}
	return 0
}
