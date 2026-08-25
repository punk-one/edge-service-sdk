package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	pathpkg "path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	rtauth "github.com/punk-one/edge-service-sdk/auth"
	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	rtstatus "github.com/punk-one/edge-service-sdk/ops/status"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
)

func TestHandleHealthUsesAPIV1Shape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	server := New(Config{
		ServiceName: "device-s7",
		Version:     "1.0.0",
		ServiceType: "sensor",
		StartedAt:   time.Now(),
	})

	server.handleHealth(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload["status"] != "up" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestHandleCommandCallRejectsMissingAuthHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := t.TempDir()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     root + "/runtime.db",
		KeyFile:        root + "/auth.key",
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer authService.Close()

	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{
		AppID:     "demo",
		AppSecret: "secret",
	}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "identifier", Value: "reset_alarm"}}
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/device/control/command/call/reset_alarm", bytes.NewBufferString(`{"device_code":"acm006","data":{}}`))

	server := New(Config{
		AuthService: authService,
		CommandCall: func(identifier string, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int) {
			return cmdapi.CommandResponse{Code: ctl.CodeSuccess, Message: "success"}, http.StatusOK
		},
	})

	server.handleCommandCall(ctx)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusUnauthorized, recorder.Body.String())
	}
}

func TestHandlePropertyGetRejectsMissingAuthHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := t.TempDir()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     root + "/runtime.db",
		KeyFile:        root + "/auth.key",
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer authService.Close()

	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{
		AppID:     "demo",
		AppSecret: "secret",
	}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/device/control/property/get", bytes.NewBufferString(`{"device_code":"acm006","data":{"properties":["x"]}}`))

	server := New(Config{
		AuthService: authService,
		PropertyGet: func(req rtapi.PropertyRequest) (rtapi.PropertyResponse, int) {
			return rtapi.PropertyResponse{Code: ctl.CodeSuccess, Message: "success"}, http.StatusOK
		},
	})

	server.handlePropertyGet(ctx)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusUnauthorized, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["code"]; got != float64(http.StatusUnauthorized) {
		t.Fatalf("code = %#v, want %d", got, http.StatusUnauthorized)
	}
}

func TestRouterDoesNotExposeLegacyPropertyRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := New(Config{})
	router := server.router()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/property/get", bytes.NewBufferString(`{}`))
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestRouterDoesNotExposeCredentialUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := New(Config{})
	router := server.router()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/credential/update", bytes.NewBufferString(`{}`))
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestHandleRuntimeStatusUsesSnakeCaseFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/runtime/status", nil)

	server := New(Config{
		ServiceName:            "device-s7",
		Version:                "1.0.0",
		ServiceType:            "sensor",
		Host:                   "localhost",
		Port:                   59994,
		StartupMsg:             "S7 device service started",
		StartedAt:              time.Unix(1710000000, 0),
		DeviceCount:            1,
		TelemetryWorkerCount:   2,
		TelemetryOutboxEnabled: true,
		TelemetryOutboxStats: func() (reliable.TelemetryOutboxStats, error) {
			return reliable.TelemetryOutboxStats{
				PendingCount:       3,
				OldestPendingAgeMs: 1200,
				SendRatePerSec:     5,
				LastSendAt:         1710000000000,
			}, nil
		},
		DeviceStates: func() []rtstatus.DeviceState {
			return []rtstatus.DeviceState{
				{
					DeviceCode:      "acm006",
					ConnectionState: rtstatus.StateConnected,
					Connected:       true,
					LastConnectedAt: 1710000000000,
					LastReadAt:      1710000001000,
					LastWriteAt:     1710000002000,
					LastSuccessAt:   1710000003000,
					LastError:       "",
					LastErrorAt:     0,
				},
			}
		},
	})

	server.handleRuntimeStatus(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	runtimeBody, ok := payload["runtime"].(map[string]interface{})
	if !ok {
		t.Fatalf("runtime = %#v, want object", payload["runtime"])
	}
	if _, ok := runtimeBody["device_count"]; !ok {
		t.Fatalf("expected runtime.device_count in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["telemetry_worker_count"]; !ok {
		t.Fatalf("expected runtime.telemetry_worker_count in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["telemetry_outbox"]; !ok {
		t.Fatalf("expected runtime.telemetry_outbox in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["deviceCount"]; ok {
		t.Fatalf("did not expect runtime.deviceCount in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["telemetryWorkerCount"]; ok {
		t.Fatalf("did not expect runtime.telemetryWorkerCount in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["telemetryOutbox"]; ok {
		t.Fatalf("did not expect runtime.telemetryOutbox in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["reliable_queue"]; ok {
		t.Fatalf("did not expect removed runtime.reliable_queue in payload: %#v", runtimeBody)
	}

	devices, ok := payload["devices"].([]interface{})
	if !ok || len(devices) != 1 {
		t.Fatalf("devices = %#v, want single-element array", payload["devices"])
	}
	device, ok := devices[0].(map[string]interface{})
	if !ok {
		t.Fatalf("devices[0] = %#v, want object", devices[0])
	}
	if got := device["device_code"]; got != "acm006" {
		t.Fatalf("device_code = %#v, want acm006", got)
	}
	if got := device["connection_state"]; got != rtstatus.StateConnected {
		t.Fatalf("connection_state = %#v, want %q", got, rtstatus.StateConnected)
	}
	if _, ok := device["product_code"]; ok {
		t.Fatalf("did not expect device.product_code in payload: %#v", device)
	}
	if _, ok := device["last_success_at"]; !ok {
		t.Fatalf("expected device.last_success_at in payload: %#v", device)
	}
	if _, ok := device["deviceCode"]; ok {
		t.Fatalf("did not expect device.deviceCode in payload: %#v", device)
	}
	if _, ok := device["connectionState"]; ok {
		t.Fatalf("did not expect device.connectionState in payload: %#v", device)
	}
	if _, ok := device["lastSuccessAt"]; ok {
		t.Fatalf("did not expect device.lastSuccessAt in payload: %#v", device)
	}
}

func TestHandlePropertyModelQueryReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		PropertyModelQuery: func(deviceCode string) (map[string]interface{}, int) {
			return map[string]interface{}{
				"device_code": deviceCode,
				"points":      []map[string]interface{}{{"name": "status_text"}},
			}, http.StatusOK
		},
	})
	router := server.router()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/device/model/properties?device_code=acm006", nil)
	timestamp := time.Now().UnixMilli()
	nonce := "model-properties"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/model/properties", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["device_code"]; got != "acm006" {
		t.Fatalf("device_code = %#v, want acm006", got)
	}
}

func TestHandleControlJobResultQueryReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobResultQuery: func(traceID string) (map[string]interface{}, int) {
			return map[string]interface{}{
				"trace_id": traceID,
				"code":     ctl.CodeSuccess,
				"message":  "success",
				"data":     map[string]interface{}{},
				"final":    true,
			}, http.StatusOK
		},
	})
	router := server.router()

	traceID := "trace-model-1"
	path := "/api/v1/device/control/jobs/" + traceID + "/result"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-result"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, path, []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["trace_id"]; got != traceID {
		t.Fatalf("trace_id = %#v, want %s", got, traceID)
	}
}

func TestHandlePropertyResultQueryReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		PropertyResultQuery: func(traceID string) (map[string]interface{}, int) {
			return map[string]interface{}{
				"trace_id": traceID,
				"code":     ctl.CodeProcessing,
				"message":  "processing",
				"data":     map[string]interface{}{},
				"time":     time.Now().UnixMilli(),
			}, http.StatusOK
		},
	})
	router := server.router()

	traceID := "trace-property-result-1"
	path := "/api/v1/device/control/property/result/" + traceID
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "property-result"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, path, []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["trace_id"]; got != traceID {
		t.Fatalf("trace_id = %#v, want %s", got, traceID)
	}
}

func TestRouterExposesDeviceModelAndJobRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := t.TempDir()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     pathpkg.Join(root, "runtime.db"),
		KeyFile:        pathpkg.Join(root, "auth.key"),
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer authService.Close()
	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{AppID: "demo", AppSecret: "secret"}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}

	server := New(Config{
		AuthService: authService,
		PropertyModelQuery: func(deviceCode string) (map[string]interface{}, int) {
			return map[string]interface{}{}, http.StatusOK
		},
		ControlJobQuery: func(traceID string) (map[string]interface{}, int) {
			return map[string]interface{}{}, http.StatusOK
		},
	})
	router := server.router()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/device/model/properties", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusNotFound {
		t.Fatalf("expected property model route to exist")
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/device/control/jobs/trace-1", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusNotFound {
		t.Fatalf("expected control job route to exist")
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/device/control/property/result/trace-1", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusNotFound {
		t.Fatalf("expected property result route to exist")
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/device/control/command/result/trace-1", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code == http.StatusNotFound {
		t.Fatalf("expected command result route to exist")
	}
}

func TestHandleControlJobListQueryReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobListQuery: func(query ControlJobListQuery) (map[string]interface{}, int) {
			return map[string]interface{}{
				"jobs":  []map[string]interface{}{{"trace_id": "trace-list-1"}},
				"query": map[string]interface{}{"device_code": query.DeviceCode, "kind": query.Kind, "final_set": query.FinalSet, "final": query.Final, "limit": query.Limit},
			}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs?device_code=qhl0001&kind=command&final=false&limit=8"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-list"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	queryBody := payload["query"].(map[string]interface{})
	if got := queryBody["device_code"]; got != "qhl0001" {
		t.Fatalf("device_code = %#v, want qhl0001", got)
	}
	if got := queryBody["limit"]; got != float64(8) {
		t.Fatalf("limit = %#v, want 8", got)
	}
}

func TestHandleControlJobListQueryRejectsInvalidFinal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobListQuery: func(query ControlJobListQuery) (map[string]interface{}, int) {
			return map[string]interface{}{}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs?final=maybe"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-list-invalid"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestHandleControlJobListQueryParsesExtendedFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobListQuery: func(query ControlJobListQuery) (map[string]interface{}, int) {
			return map[string]interface{}{
				"query": map[string]interface{}{
					"identifier":   query.Identifier,
					"offset":       query.Offset,
					"created_from": query.CreatedFrom,
					"updated_to":   query.UpdatedTo,
				},
			}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs?identifier=program_install&offset=3&created_from=11&updated_to=22"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-list-extended"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	queryBody := payload["query"].(map[string]interface{})
	if got := queryBody["identifier"]; got != "program_install" {
		t.Fatalf("identifier = %#v, want program_install", got)
	}
	if got := queryBody["offset"]; got != float64(3) {
		t.Fatalf("offset = %#v, want 3", got)
	}
}

func TestHandleControlJobExportReturnsCSV(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobExportQuery: func(query ControlJobExportQuery) (map[string]interface{}, int) {
			return map[string]interface{}{
				"filename": "control_jobs_export.csv",
				"columns":  []string{"trace_id", "code", "latest_progress"},
				"rows": []map[string]interface{}{{
					"trace_id":        "trace-export-1",
					"code":            102,
					"latest_progress": map[string]interface{}{"step_type": "download_file"},
				}},
			}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs/export?kind=command&format=csv"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-export"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs/export", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got == "" || got[:8] != "text/csv" {
		t.Fatalf("content type = %q, want text/csv", got)
	}
	if body := recorder.Body.String(); !strings.Contains(body, "trace-export-1") || !strings.Contains(body, "download_file") {
		t.Fatalf("unexpected csv body: %s", body)
	}
}

func TestHandleControlJobDiagnosticsReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobDiagnosticsQuery: func(query ControlJobListQuery) (map[string]interface{}, int) {
			return map[string]interface{}{
				"summary":       map[string]interface{}{"total": 3},
				"pending_queue": map[string]interface{}{"command": 1},
				"breakdown": map[string]interface{}{
					"devices": []map[string]interface{}{{"device_code": "qhl0001", "count": 3}},
				},
			}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs/diagnostics?kind=command"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-diagnostics"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs/diagnostics", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	summary := payload["summary"].(map[string]interface{})
	if got := summary["total"]; got != float64(3) {
		t.Fatalf("summary.total = %#v, want 3", got)
	}
	breakdown := payload["breakdown"].(map[string]interface{})
	devices := breakdown["devices"].([]interface{})
	if len(devices) == 0 {
		t.Fatalf("devices breakdown = %#v, want non-empty", devices)
	}
}

func TestHandleControlJobListRejectsInvalidOffset(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, authService, token, secret := newControlFlowAuth(t)
	defer authService.Close()

	server := New(Config{
		AuthService: authService,
		ControlJobListQuery: func(query ControlJobListQuery) (map[string]interface{}, int) {
			return map[string]interface{}{}, http.StatusOK
		},
	})
	router := server.router()

	path := "/api/v1/device/control/jobs?offset=-1"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	timestamp := time.Now().UnixMilli()
	nonce := "job-list-invalid-offset"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "demo")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, http.MethodGet, "/api/v1/device/control/jobs", []byte{}, token, timestamp, nonce, "demo"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestRouterExposesControlJobExportRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := t.TempDir()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     pathpkg.Join(root, "runtime.db"),
		KeyFile:        pathpkg.Join(root, "auth.key"),
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer authService.Close()
	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{AppID: "demo", AppSecret: "secret"}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}
	server := New(Config{
		AuthService: authService,
		ControlJobExportQuery: func(query ControlJobExportQuery) (map[string]interface{}, int) {
			return map[string]interface{}{"format": query.Format}, http.StatusOK
		},
	})
	router := server.router()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/device/control/jobs/export", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestRouterExposesControlJobEventsRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := t.TempDir()
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     pathpkg.Join(root, "runtime.db"),
		KeyFile:        pathpkg.Join(root, "auth.key"),
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	defer authService.Close()
	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{AppID: "demo", AppSecret: "secret"}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}
	server := New(Config{
		AuthService: authService,
		ControlJobEventsQuery: func(traceID string, limit int) (map[string]interface{}, int) {
			return map[string]interface{}{"trace_id": traceID, "limit": limit}, http.StatusOK
		},
	})
	router := server.router()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/device/control/jobs/trace-1/events", nil)
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}
