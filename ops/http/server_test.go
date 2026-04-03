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
	rtapi "github.com/punk-one/edge-service-sdk/property"
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
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/property/get", bytes.NewBufferString(`{"product_code":"acm","device_code":"acm006","data":{"x":true}}`))

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
