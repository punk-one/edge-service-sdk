package httpserver

import (
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

// Config describes the HTTP runtime service.
type Config struct {
	ServiceName          string
	Version              string
	Host                 string
	Port                 int
	StartupMsg           string
	ServiceType          string
	StartedAt            time.Time
	DeviceCount          int
	TelemetryWorkerCount int
	ReliableQueueEnabled bool
	Readiness            ReadinessFunc
	QueueStats           QueueStatsFunc
	DeviceStates         DeviceStatesFunc
	AuthService          *rtauth.Service
	PropertyGet          PropertyGetFunc
	PropertySet          PropertySetFunc
	Logger               logger.LoggingClient
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
	v1.POST("/property/get", s.handlePropertyGet)
	v1.POST("/property/set", s.handlePropertySet)
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
				"appId":       info.AppID,
				"updatedAt":   millisToRFC3339(info.UpdatedAt),
			}
		}
	}

	response := gin.H{
		"service": gin.H{
			"name":       s.cfg.ServiceName,
			"version":    s.cfg.Version,
			"type":       s.cfg.ServiceType,
			"host":       normalizedHost(s.cfg.Host),
			"port":       s.cfg.Port,
			"address":    listenAddress(s.cfg.Host, s.cfg.Port),
			"startupMsg": s.cfg.StartupMsg,
			"startedAt":  s.cfg.StartedAt.Format(time.RFC3339),
			"uptimeSec":  int64(time.Since(s.cfg.StartedAt).Seconds()),
		},
		"runtime": gin.H{
			"deviceCount":          s.cfg.DeviceCount,
			"telemetryWorkerCount": s.cfg.TelemetryWorkerCount,
			"ready":                ready,
			"time":                 time.Now().Format(time.RFC3339),
			"auth":                 credentialInfo,
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
		queueBody["bufferDepth"] = queueStats.BufferDepth
		queueBody["oldestPendingAgeMs"] = queueStats.OldestPendingAgeMs
		queueBody["replayRatePerSec"] = queueStats.ReplayRatePerSec
		queueBody["lastReplayAt"] = millisToRFC3339(queueStats.LastReplayAt)
	}
	response["runtime"].(gin.H)["reliableQueue"] = queueBody

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
		writeJSONError(c, http.StatusServiceUnavailable, "property get is unavailable")
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

	var req rtapi.PropertyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid JSON body")
		return
	}
	response, statusCode := s.cfg.PropertyGet(req)
	c.JSON(statusCode, response)
}

func (s *Server) handlePropertySet(c *gin.Context) {
	if s.cfg.AuthService == nil || s.cfg.PropertySet == nil {
		writeJSONError(c, http.StatusServiceUnavailable, "property set is unavailable")
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

	var req rtapi.PropertyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid JSON body")
		return
	}
	response, statusCode := s.cfg.PropertySet(req)
	c.JSON(statusCode, response)
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
			"deviceCode":      state.DeviceCode,
			"productCode":     state.ProductCode,
			"connectionState": state.ConnectionState,
			"connected":       state.Connected,
			"lastConnectedAt": millisToRFC3339(state.LastConnectedAt),
			"lastReadAt":      millisToRFC3339(state.LastReadAt),
			"lastWriteAt":     millisToRFC3339(state.LastWriteAt),
			"lastSuccessAt":   millisToRFC3339(state.LastSuccessAt),
			"lastError":       state.LastError,
			"lastErrorAt":     millisToRFC3339(state.LastErrorAt),
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
