package httpserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	logger "github.com/punk-one/edge-service-sdk/logging"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"

	"github.com/gin-gonic/gin"
)

// ReadinessFunc checks whether the runtime is ready to serve.
type ReadinessFunc func() error

// QueueStatsFunc retrieves reliable queue runtime metrics.
type QueueStatsFunc func() (reliable.QueueStats, error)

// DeviceStatesFunc returns the current device states.
type DeviceStatesFunc func() []rtstatus.DeviceState

// PropertyGetFunc executes a property read request.
type PropertyGetFunc func(req rtapi.PropertyRequest) (rtapi.PropertyResponse, int)

// PropertySetFunc executes a property write request.
type PropertySetFunc func(req rtapi.PropertyRequest) (rtapi.PropertySetResponse, int)

// CommandCallFunc executes a command request.
type CommandCallFunc func(identifier string, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int)

// DeviceModelQueryFunc returns one device model view.
type DeviceModelQueryFunc func(deviceCode string) (map[string]interface{}, int)

// CommandModelQueryFunc returns one command model view.
type CommandModelQueryFunc func(deviceCode string, identifier string) (map[string]interface{}, int)

// ControlJobListQuery carries HTTP query parameters for listing control jobs.
type ControlJobListQuery struct {
	DeviceCode  string
	Kind        string
	Identifier  string
	FinalSet    bool
	Final       bool
	Limit       int
	Offset      int
	CreatedFrom int64
	CreatedTo   int64
	UpdatedFrom int64
	UpdatedTo   int64
}

// ControlTraceQueryFunc returns one control-job view by trace id.
type ControlTraceQueryFunc func(traceID string) (map[string]interface{}, int)

// ControlJobEventsQueryFunc returns one control-job event-history view by trace id.
type ControlJobEventsQueryFunc func(traceID string, limit int) (map[string]interface{}, int)

// ControlJobListQueryFunc returns a filtered control-job list view.
type ControlJobListQueryFunc func(query ControlJobListQuery) (map[string]interface{}, int)

// ControlJobExportQuery carries HTTP query parameters for exporting control jobs.
type ControlJobExportQuery struct {
	ControlJobListQuery
	Format string
}

// ControlJobExportQueryFunc returns one control-job export payload.
type ControlJobExportQueryFunc func(query ControlJobExportQuery) (map[string]interface{}, int)

// ControlJobDiagnosticsQueryFunc returns summarized control-job diagnostics.
type ControlJobDiagnosticsQueryFunc func(query ControlJobListQuery) (map[string]interface{}, int)

// Config describes the HTTP runtime service.
type Config struct {
	ServiceName                string
	Version                    string
	Host                       string
	Port                       int
	StartupMsg                 string
	ServiceType                string
	StartedAt                  time.Time
	DeviceCount                int
	TelemetryWorkerCount       int
	ReliableQueueEnabled       bool
	Readiness                  ReadinessFunc
	QueueStats                 QueueStatsFunc
	DeviceStates               DeviceStatesFunc
	AuthService                *rtauth.Service
	PropertyGet                PropertyGetFunc
	PropertySet                PropertySetFunc
	CommandCall                CommandCallFunc
	PropertyModelQuery         DeviceModelQueryFunc
	TelemetryModelQuery        DeviceModelQueryFunc
	CommandListQuery           DeviceModelQueryFunc
	CommandDetailQuery         CommandModelQueryFunc
	CommandInputQuery          CommandModelQueryFunc
	CommandOutputQuery         CommandModelQueryFunc
	PropertyResultQuery        ControlTraceQueryFunc
	CommandResultQuery         ControlTraceQueryFunc
	ControlJobListQuery        ControlJobListQueryFunc
	ControlJobExportQuery      ControlJobExportQueryFunc
	ControlJobDiagnosticsQuery ControlJobDiagnosticsQueryFunc
	ControlJobQuery            ControlTraceQueryFunc
	ControlJobResultQuery      ControlTraceQueryFunc
	ControlJobEventsQuery      ControlJobEventsQueryFunc
	Logger                     logger.LoggingClient
}

// Server exposes runtime HTTP APIs without blocking telemetry or MQTT workers.
type Server struct {
	cfg Config
}

// New creates a new HTTP runtime server.
func New(cfg Config) *Server {
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = time.Now()
	}
	return &Server{cfg: cfg}
}

// Enabled reports whether the HTTP service should be started.
func (s *Server) Enabled() bool {
	return s != nil && s.cfg.Port > 0
}

// Run starts the Gin HTTP server and blocks until it exits.
func (s *Server) Run() error {
	if s == nil {
		return nil
	}
	if !s.Enabled() {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Infof("HTTP runtime server disabled: service.port=%d", s.cfg.Port)
		}
		return nil
	}

	router := s.router()

	addr := listenAddress(s.cfg.Host, s.cfg.Port)
	if s.cfg.Logger != nil {
		s.cfg.Logger.Infof(
			"HTTP runtime server listening: addr=%s startupMsg=%s serviceType=%s",
			addr,
			strings.TrimSpace(s.cfg.StartupMsg),
			strings.TrimSpace(s.cfg.ServiceType),
		)
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	err := server.ListenAndServe()
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Errorf("HTTP runtime panic recovered: path=%s err=%v", c.Request.URL.Path, recovered)
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  "internal server error",
		})
	}))

	v1 := router.Group("/api/v1")
	v1.GET("/health", s.handleHealth)
	v1.GET("/ready", s.handleReady)
	v1.GET("/runtime/status", s.handleRuntimeStatus)
	v1.POST("/auth/bootstrap/init", s.handleBootstrapInit)
	v1.POST("/auth/token", s.handleAuthToken)
	v1.POST("/auth/credential/query", s.handleCredentialQuery)
	v1.GET("/device/model/properties", s.handlePropertyModel)
	v1.GET("/device/model/telemetry", s.handleTelemetryModel)
	v1.GET("/device/model/commands", s.handleCommandList)
	v1.GET("/device/model/commands/:identifier/input", s.handleCommandInput)
	v1.GET("/device/model/commands/:identifier/output", s.handleCommandOutput)
	v1.GET("/device/model/commands/:identifier", s.handleCommandDetail)
	v1.POST("/device/control/property/get", s.handlePropertyGet)
	v1.POST("/device/control/property/set", s.handlePropertySet)
	v1.GET("/device/control/property/result/:trace_id", s.handlePropertyResult)
	v1.POST("/device/control/command/call/:identifier", s.handleCommandCall)
	v1.GET("/device/control/command/result/:trace_id", s.handleCommandResult)
	v1.GET("/device/control/jobs/diagnostics", s.handleControlJobDiagnostics)
	v1.GET("/device/control/jobs/export", s.handleControlJobExport)
	v1.GET("/device/control/jobs", s.handleControlJobList)
	v1.GET("/device/control/jobs/:trace_id", s.handleControlJob)
	v1.GET("/device/control/jobs/:trace_id/result", s.handleControlJobResult)
	v1.GET("/device/control/jobs/:trace_id/events", s.handleControlJobEvents)
	return router
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "up",
		"serviceName": s.cfg.ServiceName,
		"serviceType": s.cfg.ServiceType,
		"version":     s.cfg.Version,
		"startedAt":   s.cfg.StartedAt.Format(time.RFC3339),
		"uptimeSec":   int64(time.Since(s.cfg.StartedAt).Seconds()),
		"time":        time.Now().Format(time.RFC3339),
	})
}

func (s *Server) handleReady(c *gin.Context) {
	ready, readyErr := s.readyState()

	body := gin.H{
		"status": "ready",
		"checks": gin.H{
			"runtime": gin.H{
				"ready": ready,
			},
		},
		"time": time.Now().Format(time.RFC3339),
	}
	if readyErr != nil {
		body["status"] = "notReady"
		body["checks"].(gin.H)["runtime"].(gin.H)["error"] = readyErr.Error()
		c.JSON(http.StatusServiceUnavailable, body)
		return
	}

	c.JSON(http.StatusOK, body)
}

func (s *Server) handleRuntimeStatus(c *gin.Context) {
	ready, readyErr := s.readyState()
	queueStats, queueErr := s.queueStats()
	credentialInfo := gin.H{"initialized": false}
	if s.cfg.AuthService != nil {
		if info, err := s.cfg.AuthService.CredentialInfo(); err == nil {
			credentialInfo = gin.H{
				"initialized": info.Initialized,
				"app_id":      info.AppID,
				"updated_at":  millisToRFC3339(info.UpdatedAt),
			}
		}
	}

	response := gin.H{
		"service": gin.H{
			"name":        s.cfg.ServiceName,
			"version":     s.cfg.Version,
			"type":        s.cfg.ServiceType,
			"host":        normalizedHost(s.cfg.Host),
			"port":        s.cfg.Port,
			"address":     listenAddress(s.cfg.Host, s.cfg.Port),
			"startup_msg": s.cfg.StartupMsg,
			"started_at":  s.cfg.StartedAt.Format(time.RFC3339),
			"uptime_sec":  int64(time.Since(s.cfg.StartedAt).Seconds()),
		},
		"runtime": gin.H{
			"device_count":           s.cfg.DeviceCount,
			"telemetry_worker_count": s.cfg.TelemetryWorkerCount,
			"ready":                  ready,
			"time":                   time.Now().Format(time.RFC3339),
			"auth":                   credentialInfo,
		},
		"devices": s.deviceStates(),
	}

	if readyErr != nil {
		response["runtime"].(gin.H)["error"] = readyErr.Error()
	}

	queueBody := gin.H{
		"enabled": s.cfg.ReliableQueueEnabled,
	}
	if queueErr != nil {
		queueBody["error"] = queueErr.Error()
	} else {
		queueBody["buffer_depth"] = queueStats.BufferDepth
		queueBody["oldest_pending_age_ms"] = queueStats.OldestPendingAgeMs
		queueBody["replay_rate_per_sec"] = queueStats.ReplayRatePerSec
		queueBody["last_replay_at"] = millisToRFC3339(queueStats.LastReplayAt)
	}
	response["runtime"].(gin.H)["reliable_queue"] = queueBody

	statusCode := http.StatusOK
	if !ready {
		statusCode = http.StatusServiceUnavailable
	}
	c.JSON(statusCode, response)
}

func (s *Server) handleBootstrapInit(c *gin.Context) {
	if s.cfg.AuthService == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "auth service is unavailable")
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	var req rtapi.BootstrapInitRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid JSON body")
		return
	}

	info, err := s.cfg.AuthService.BootstrapInit(req, c.GetHeader("X-Bootstrap-Token"))
	if err != nil {
		writeJSONError(c, errorStatus(err, http.StatusInternalServerError), err.Error())
		return
	}
	c.JSON(http.StatusOK, info)
}

func (s *Server) handleAuthToken(c *gin.Context) {
	if s.cfg.AuthService == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "auth service is unavailable")
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	var req rtapi.AuthTokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid JSON body")
		return
	}

	resp, err := s.cfg.AuthService.IssueToken(req)
	if err != nil {
		writeJSONError(c, errorStatus(err, http.StatusInternalServerError), err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) handleCredentialQuery(c *gin.Context) {
	if s.cfg.AuthService == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "auth service is unavailable")
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.cfg.AuthService.AuthorizeProtected(buildProtectedRequest(c, body)); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	info, err := s.cfg.AuthService.CredentialInfo()
	if err != nil {
		writeJSONError(c, errorStatus(err, http.StatusInternalServerError), err.Error())
		return
	}
	c.JSON(http.StatusOK, info)
}

func (s *Server) handlePropertyGet(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.PropertyGet == nil {
		writeControlError(c, http.StatusServiceUnavailable, ctl.Result{
			Code:    http.StatusServiceUnavailable,
			Message: "property get is unavailable",
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	if err := s.cfg.AuthService.AuthorizeProtected(buildProtectedRequest(c, body)); err != nil {
		writeControlError(c, errorStatus(err, http.StatusUnauthorized), ctl.Result{
			Code:    http.StatusUnauthorized,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	var req rtapi.PropertyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: "invalid JSON body",
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	response, statusCode := s.cfg.PropertyGet(req)
	c.JSON(statusCode, response)
}

func (s *Server) handlePropertySet(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.PropertySet == nil {
		writeControlError(c, http.StatusServiceUnavailable, ctl.Result{
			Code:    http.StatusServiceUnavailable,
			Message: "property set is unavailable",
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	if err := s.cfg.AuthService.AuthorizeProtected(buildProtectedRequest(c, body)); err != nil {
		writeControlError(c, errorStatus(err, http.StatusUnauthorized), ctl.Result{
			Code:    http.StatusUnauthorized,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	var req rtapi.PropertyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: "invalid JSON body",
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	response, statusCode := s.cfg.PropertySet(req)
	c.JSON(statusCode, response)
}

func (s *Server) handleCommandCall(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.CommandCall == nil {
		writeControlError(c, http.StatusServiceUnavailable, ctl.Result{
			Code:    http.StatusServiceUnavailable,
			Message: "command call is unavailable",
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	identifier := strings.TrimSpace(c.Param("identifier"))
	if identifier == "" {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: "command identifier is required",
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	body, err := readBody(c)
	if err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	if err := s.cfg.AuthService.AuthorizeProtected(buildProtectedRequest(c, body)); err != nil {
		writeControlError(c, errorStatus(err, http.StatusUnauthorized), ctl.Result{
			Code:    http.StatusUnauthorized,
			Message: err.Error(),
			Time:    time.Now().UnixMilli(),
		})
		return
	}

	var req cmdapi.CommandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeControlError(c, http.StatusBadRequest, ctl.Result{
			Code:    ctl.CodeBadRequest,
			Message: "invalid JSON body",
			Time:    time.Now().UnixMilli(),
		})
		return
	}
	response, statusCode := s.cfg.CommandCall(identifier, req)
	c.JSON(statusCode, response)
}

func (s *Server) handlePropertyModel(c *gin.Context) {
	s.handleDeviceModelQuery(c, s.cfg.PropertyModelQuery, "property model is unavailable")
}

func (s *Server) handleTelemetryModel(c *gin.Context) {
	s.handleDeviceModelQuery(c, s.cfg.TelemetryModelQuery, "telemetry model is unavailable")
}

func (s *Server) handleCommandList(c *gin.Context) {
	s.handleDeviceModelQuery(c, s.cfg.CommandListQuery, "command model is unavailable")
}

func (s *Server) handleCommandDetail(c *gin.Context) {
	s.handleCommandModelQuery(c, s.cfg.CommandDetailQuery, "command detail is unavailable")
}

func (s *Server) handleCommandInput(c *gin.Context) {
	s.handleCommandModelQuery(c, s.cfg.CommandInputQuery, "command input model is unavailable")
}

func (s *Server) handleCommandOutput(c *gin.Context) {
	s.handleCommandModelQuery(c, s.cfg.CommandOutputQuery, "command output model is unavailable")
}

func (s *Server) handleControlJobList(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.ControlJobListQuery == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "control job list query is unavailable")
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	query, err := parseControlJobListQuery(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	response, statusCode := s.cfg.ControlJobListQuery(query)
	c.JSON(statusCode, response)
}

func (s *Server) handleControlJobDiagnostics(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.ControlJobDiagnosticsQuery == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "control job diagnostics is unavailable")
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	query, err := parseControlJobListQuery(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	response, statusCode := s.cfg.ControlJobDiagnosticsQuery(query)
	c.JSON(statusCode, response)
}

func (s *Server) handleControlJobExport(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.ControlJobExportQuery == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "control job export is unavailable")
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	query, err := parseControlJobExportQuery(c)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	response, statusCode := s.cfg.ControlJobExportQuery(query)
	if query.Format == "csv" && statusCode == http.StatusOK {
		filename, csvBody, err := controlJobExportCSV(response)
		if err != nil {
			writeJSONError(c, http.StatusInternalServerError, err.Error())
			return
		}
		c.Header("Content-Type", "text/csv; charset=utf-8")
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		c.Data(http.StatusOK, "text/csv; charset=utf-8", csvBody)
		return
	}
	c.JSON(statusCode, response)
}

func (s *Server) handleControlJob(c *gin.Context) {
	s.handleControlTraceQuery(c, s.cfg.ControlJobQuery, "control job query is unavailable")
}

func (s *Server) handlePropertyResult(c *gin.Context) {
	s.handleControlTraceQuery(c, s.cfg.PropertyResultQuery, "property result query is unavailable")
}

func (s *Server) handleCommandResult(c *gin.Context) {
	s.handleControlTraceQuery(c, s.cfg.CommandResultQuery, "command result query is unavailable")
}

func (s *Server) handleControlJobResult(c *gin.Context) {
	s.handleControlTraceQuery(c, s.cfg.ControlJobResultQuery, "control job result query is unavailable")
}

func (s *Server) handleControlJobEvents(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.ControlJobEventsQuery == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "control job events query is unavailable")
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	traceID := strings.TrimSpace(c.Param("trace_id"))
	if traceID == "" {
		writeJSONError(c, http.StatusBadRequest, "trace_id is required")
		return
	}
	limit, err := parseOptionalPositiveIntQuery(c, "limit")
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, err.Error())
		return
	}
	response, statusCode := s.cfg.ControlJobEventsQuery(traceID, limit)
	c.JSON(statusCode, response)
}

func (s *Server) handleDeviceModelQuery(c *gin.Context, query DeviceModelQueryFunc, unavailable string) {
	if s.cfg.AuthService == nil || query == nil {
		writeJSONError(c, http.StatusServiceUnavailable, unavailable)
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	deviceCode := strings.TrimSpace(c.Query("device_code"))
	if deviceCode == "" {
		writeJSONError(c, http.StatusBadRequest, "device_code is required")
		return
	}
	response, statusCode := query(deviceCode)
	c.JSON(statusCode, response)
}

func (s *Server) handleCommandModelQuery(c *gin.Context, query CommandModelQueryFunc, unavailable string) {
	if s.cfg.AuthService == nil || query == nil {
		writeJSONError(c, http.StatusServiceUnavailable, unavailable)
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	deviceCode := strings.TrimSpace(c.Query("device_code"))
	if deviceCode == "" {
		writeJSONError(c, http.StatusBadRequest, "device_code is required")
		return
	}
	identifier := strings.TrimSpace(c.Param("identifier"))
	if identifier == "" {
		writeJSONError(c, http.StatusBadRequest, "command identifier is required")
		return
	}
	response, statusCode := query(deviceCode, identifier)
	c.JSON(statusCode, response)
}

func (s *Server) handleControlTraceQuery(c *gin.Context, query ControlTraceQueryFunc, unavailable string) {
	if s.cfg.AuthService == nil || query == nil {
		writeJSONError(c, http.StatusServiceUnavailable, unavailable)
		return
	}
	if err := s.authorizeQuery(c); err != nil {
		writeJSONError(c, errorStatus(err, http.StatusUnauthorized), err.Error())
		return
	}
	traceID := strings.TrimSpace(c.Param("trace_id"))
	if traceID == "" {
		writeJSONError(c, http.StatusBadRequest, "trace_id is required")
		return
	}
	response, statusCode := query(traceID)
	c.JSON(statusCode, response)
}

func (s *Server) authorizeQuery(c *gin.Context) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}
	return s.cfg.AuthService.AuthorizeProtected(buildProtectedRequest(c, body))
}

func parseControlJobListQuery(c *gin.Context) (ControlJobListQuery, error) {
	query := ControlJobListQuery{
		DeviceCode: strings.TrimSpace(c.Query("device_code")),
		Kind:       strings.TrimSpace(c.Query("kind")),
		Identifier: strings.TrimSpace(c.Query("identifier")),
	}
	if raw := strings.TrimSpace(c.Query("final")); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return ControlJobListQuery{}, fmt.Errorf("final must be true or false")
		}
		query.FinalSet = true
		query.Final = value
	}
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return ControlJobListQuery{}, fmt.Errorf("limit must be a non-negative integer")
		}
		query.Limit = int(value)
	}
	if raw := strings.TrimSpace(c.Query("offset")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return ControlJobListQuery{}, fmt.Errorf("offset must be a non-negative integer")
		}
		query.Offset = int(value)
	}
	var err error
	if query.CreatedFrom, err = parseOptionalInt64Query(c, "created_from"); err != nil {
		return ControlJobListQuery{}, err
	}
	if query.CreatedTo, err = parseOptionalInt64Query(c, "created_to"); err != nil {
		return ControlJobListQuery{}, err
	}
	if query.UpdatedFrom, err = parseOptionalInt64Query(c, "updated_from"); err != nil {
		return ControlJobListQuery{}, err
	}
	if query.UpdatedTo, err = parseOptionalInt64Query(c, "updated_to"); err != nil {
		return ControlJobListQuery{}, err
	}
	return query, nil
}

func parseControlJobExportQuery(c *gin.Context) (ControlJobExportQuery, error) {
	listQuery, err := parseControlJobListQuery(c)
	if err != nil {
		return ControlJobExportQuery{}, err
	}
	query := ControlJobExportQuery{ControlJobListQuery: listQuery, Format: "json"}
	if raw := strings.TrimSpace(c.Query("format")); raw != "" {
		switch raw {
		case "json", "csv":
			query.Format = raw
		default:
			return ControlJobExportQuery{}, fmt.Errorf("format must be json or csv")
		}
	}
	return query, nil
}

func controlJobExportCSV(response map[string]interface{}) (string, []byte, error) {
	filename, _ := response["filename"].(string)
	if strings.TrimSpace(filename) == "" {
		filename = "control_jobs_export.csv"
	}
	columns, err := exportColumns(response["columns"])
	if err != nil {
		return "", nil, err
	}
	rows, err := exportRows(response["rows"])
	if err != nil {
		return "", nil, err
	}
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	if err := writer.Write(columns); err != nil {
		return "", nil, err
	}
	for _, row := range rows {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			record = append(record, exportCellValue(row[column]))
		}
		if err := writer.Write(record); err != nil {
			return "", nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", nil, err
	}
	return filename, buffer.Bytes(), nil
}

func exportColumns(raw interface{}) ([]string, error) {
	items, ok := raw.([]string)
	if ok {
		return items, nil
	}
	generic, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("export columns are unavailable")
	}
	columns := make([]string, 0, len(generic))
	for _, item := range generic {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("export columns contain non-string values")
		}
		columns = append(columns, text)
	}
	return columns, nil
}

func exportRows(raw interface{}) ([]map[string]interface{}, error) {
	rows, ok := raw.([]map[string]interface{})
	if ok {
		return rows, nil
	}
	generic, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("export rows are unavailable")
	}
	rows = make([]map[string]interface{}, 0, len(generic))
	for _, item := range generic {
		row, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("export rows contain invalid items")
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func exportCellValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return fmt.Sprintf("%v", value)
		}
		return string(buf)
	}
}

func parseOptionalPositiveIntQuery(c *gin.Context, key string) (int, error) {
	if c == nil {
		return 0, nil
	}
	raw := strings.TrimSpace(c.Query(key))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return value, nil
}

func parseOptionalInt64Query(c *gin.Context, key string) (int64, error) {
	if c == nil {
		return 0, nil
	}
	raw := strings.TrimSpace(c.Query(key))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return value, nil
}

func (s *Server) readyState() (bool, error) {
	if s == nil || s.cfg.Readiness == nil {
		return true, nil
	}
	if err := s.cfg.Readiness(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) queueStats() (reliable.QueueStats, error) {
	if s == nil || s.cfg.QueueStats == nil {
		return reliable.QueueStats{}, nil
	}
	return s.cfg.QueueStats()
}

func (s *Server) deviceStates() []gin.H {
	if s == nil || s.cfg.DeviceStates == nil {
		return nil
	}
	states := s.cfg.DeviceStates()
	response := make([]gin.H, 0, len(states))
	for _, state := range states {
		response = append(response, gin.H{
			"device_code":       state.DeviceCode,
			"connection_state":  state.ConnectionState,
			"connected":         state.Connected,
			"last_connected_at": millisToRFC3339(state.LastConnectedAt),
			"last_read_at":      millisToRFC3339(state.LastReadAt),
			"last_write_at":     millisToRFC3339(state.LastWriteAt),
			"last_success_at":   millisToRFC3339(state.LastSuccessAt),
			"last_error":        state.LastError,
			"last_error_at":     millisToRFC3339(state.LastErrorAt),
		})
	}
	return response
}

func buildProtectedRequest(c *gin.Context, body []byte) rtauth.ProtectedRequest {
	return rtauth.ProtectedRequest{
		Method:    c.Request.Method,
		Path:      c.Request.URL.Path,
		Body:      body,
		AppID:     strings.TrimSpace(c.GetHeader("X-App-Id")),
		Token:     bearerToken(c.GetHeader("Authorization")),
		Timestamp: parseInt64(c.GetHeader("X-Timestamp")),
		Nonce:     strings.TrimSpace(c.GetHeader("X-Nonce")),
		Signature: strings.TrimSpace(c.GetHeader("X-Signature")),
	}
}

func readBody(c *gin.Context) ([]byte, error) {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return []byte{}, nil
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body")
	}
	if len(body) == 0 {
		return []byte{}, nil
	}
	return body, nil
}

func writeJSONError(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, gin.H{
		"status": "error",
		"error":  message,
	})
}

func writeControlError(c *gin.Context, statusCode int, result ctl.Result) {
	c.JSON(statusCode, result)
}

func errorStatus(err error, fallback int) int {
	if err == nil {
		return fallback
	}
	type statusCoder interface {
		Status() int
	}
	if withStatus, ok := err.(statusCoder); ok && withStatus.Status() > 0 {
		return withStatus.Status()
	}
	return fallback
}

func bearerToken(header string) string {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(header)), "bearer ") {
		return ""
	}
	return strings.TrimSpace(header[len("Bearer "):])
}

func parseInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return parsed
}

func listenAddress(host string, port int) string {
	return net.JoinHostPort(normalizedHost(host), fmt.Sprintf("%d", port))
}

func normalizedHost(host string) string {
	trimmed := strings.TrimSpace(host)
	if trimmed == "" {
		return "0.0.0.0"
	}
	return trimmed
}

func millisToRFC3339(millis int64) string {
	if millis <= 0 {
		return ""
	}
	return time.UnixMilli(millis).Format(time.RFC3339)
}
