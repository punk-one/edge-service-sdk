package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	rtauth "github.com/punk-one/edge-service-sdk/auth"
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
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/property/get", bytes.NewBufferString(`{"device_code":"acm006","data":{"x":true}}`))

	server := New(Config{
		AuthService: authService,
		PropertyGet: func(req rtapi.PropertyRequest) (rtapi.PropertyResponse, int) {
			return rtapi.PropertyResponse{Success: true}, http.StatusOK
		},
	})

	server.handlePropertyGet(ctx)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusUnauthorized, recorder.Body.String())
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
		ServiceName:          "device-s7",
		Version:              "1.0.0",
		ServiceType:          "sensor",
		Host:                 "localhost",
		Port:                 59994,
		StartupMsg:           "S7 device service started",
		StartedAt:            time.Unix(1710000000, 0),
		DeviceCount:          1,
		TelemetryWorkerCount: 2,
		ReliableQueueEnabled: true,
		QueueStats: func() (reliable.QueueStats, error) {
			return reliable.QueueStats{
				BufferDepth:        3,
				OldestPendingAgeMs: 1200,
				ReplayRatePerSec:   5,
				LastReplayAt:       1710000000000,
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
	if _, ok := runtimeBody["reliable_queue"]; !ok {
		t.Fatalf("expected runtime.reliable_queue in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["deviceCount"]; ok {
		t.Fatalf("did not expect runtime.deviceCount in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["telemetryWorkerCount"]; ok {
		t.Fatalf("did not expect runtime.telemetryWorkerCount in payload: %#v", runtimeBody)
	}
	if _, ok := runtimeBody["reliableQueue"]; ok {
		t.Fatalf("did not expect runtime.reliableQueue in payload: %#v", runtimeBody)
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
