package httpserver

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	pathpkg "path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	rtauth "github.com/punk-one/edge-service-sdk/auth"
	cmdapi "github.com/punk-one/edge-service-sdk/command"
	cfg "github.com/punk-one/edge-service-sdk/config"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	rtcommand "github.com/punk-one/edge-service-sdk/runtime/command"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	rtproperty "github.com/punk-one/edge-service-sdk/runtime/property"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type controlFlowCatalog struct {
	device contracts.DeviceConfig
}

func (c *controlFlowCatalog) DeviceConfigByName(name string) (contracts.DeviceConfig, bool) {
	if c.device.Name == name {
		return c.device, true
	}
	return contracts.DeviceConfig{}, false
}

func (c *controlFlowCatalog) DevicesByProductCode(productCode string) []contracts.DeviceConfig {
	if c.device.ProductCode == productCode {
		return []contracts.DeviceConfig{c.device}
	}
	return nil
}

func (c *controlFlowCatalog) ProductCodes() []string {
	if c.device.ProductCode == "" {
		return nil
	}
	return []string{c.device.ProductCode}
}

type controlFlowDriver struct {
	readValues []*contracts.CommandValue
	readErr    error
	writeErr   error
}

func (d *controlFlowDriver) Initialize(sdk contracts.DeviceServiceSDK) error { return nil }
func (d *controlFlowDriver) HandleReadCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest) ([]*contracts.CommandValue, error) {
	return d.readValues, d.readErr
}
func (d *controlFlowDriver) HandleWriteCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest, params []*contracts.CommandValue) error {
	return d.writeErr
}
func (d *controlFlowDriver) Stop(force bool) error { return nil }
func (d *controlFlowDriver) AddDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *controlFlowDriver) UpdateDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *controlFlowDriver) RemoveDevice(deviceName string, protocols map[string]contracts.ProtocolProperties) error {
	return nil
}
func (d *controlFlowDriver) ValidateDevice(device contracts.Device) error { return nil }
func (d *controlFlowDriver) Start() error                                 { return nil }
func (d *controlFlowDriver) Discover() error                              { return nil }

type flowPublisher struct {
	mu              sync.Mutex
	propertyResults []map[string]interface{}
	commandResults  []map[string]interface{}
	subscriptions   []string
}

func (p *flowPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	return nil
}
func (p *flowPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	return nil
}
func (p *flowPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return nil
}
func (p *flowPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.propertyResults = append(p.propertyResults, clonePayload(payload))
	return nil
}
func (p *flowPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *flowPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commandResults = append(p.commandResults, clonePayload(payload))
	return nil
}
func (p *flowPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *flowPublisher) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscriptions = append(p.subscriptions, topic)
	return nil
}
func (p *flowPublisher) HealthCheck() error { return nil }
func (p *flowPublisher) Close() error       { return nil }

func (p *flowPublisher) PropertyResultCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.propertyResults)
}
func (p *flowPublisher) CommandResultCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.commandResults)
}
func (p *flowPublisher) PropertyResult(index int) map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.propertyResults[index]
}
func (p *flowPublisher) CommandResult(index int) map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commandResults[index]
}

type flowCommand struct {
	desc cmdapi.CommandDescriptor
	fn   func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError)
}

func (c flowCommand) Descriptor() cmdapi.CommandDescriptor {
	return c.desc
}

func (c flowCommand) Execute(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
	if c.fn == nil {
		return map[string]interface{}{}, nil
	}
	return c.fn(ctx, req)
}

func clonePayload(payload map[string]interface{}) map[string]interface{} {
	body, _ := json.Marshal(payload)
	var copied map[string]interface{}
	_ = json.Unmarshal(body, &copied)
	return copied
}

func TestHTTPPropertySetAcceptedThenPublishesPropertyResult(t *testing.T) {
	store, authService, token, secret := newControlFlowAuth(t)
	defer store.Close()
	defer authService.Close()

	catalog := &controlFlowCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &controlFlowDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "status_text", Type: "String", Value: "RUNNING"}}}
	publisher := &flowPublisher{}
	service := rtproperty.NewService(catalog, driver, publisher, store, nil)
	service.RegisterMQTTHandlers(cfg.Config{PropertyResult: mqtt.TopicConfig{Topic: "v1/gateway/{productCode}/property/result"}})

	server := New(Config{
		AuthService: authService,
		PropertySet: func(req rtapi.PropertyRequest) (rtapi.PropertySetResponse, int) {
			return service.ExecuteSet(req, "")
		},
	})
	router := server.router()

	body := []byte(`{"trace_id":"trace-http-property-1","device_code":"acm006","metadata":{"expect_ack":true},"data":{"status_text":"RUNNING"}}`)
	req := newSignedProtectedRequest(http.MethodPost, "/api/v1/device/control/property/set", body, "demo", token, secret)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := response["code"]; got != float64(ctl.CodeAccepted) {
		t.Fatalf("response code = %#v, want %d", got, ctl.CodeAccepted)
	}
	waitForFlow(t, 2*time.Second, func() bool { return publisher.PropertyResultCount() == 1 })
	message := publisher.PropertyResult(0)
	if got := message["trace_id"]; got != "trace-http-property-1" {
		t.Fatalf("trace_id = %#v, want trace-http-property-1", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("property result code = %#v, want %d", got, ctl.CodeSuccess)
	}
}

func TestHTTPCommandCallAcceptedThenPublishesCommandResult(t *testing.T) {
	store, authService, token, secret := newControlFlowAuth(t)
	defer store.Close()
	defer authService.Close()

	catalog := &controlFlowCatalog{device: contracts.DeviceConfig{Name: "qhl0001", ProductCode: "qhl", Commands: []contracts.CommandConfig{{Identifier: "program_install"}}}}
	registry := cmdapi.NewRegistry()
	registry.MustRegister(flowCommand{
		desc: cmdapi.CommandDescriptor{Identifier: "program_install", Mode: "async"},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			time.Sleep(20 * time.Millisecond)
			return map[string]interface{}{"program_install_done": true}, nil
		},
	})
	publisher := &flowPublisher{}
	service := rtcommand.NewService(catalog, &controlFlowDriver{}, publisher, store, nil, registry, nil)
	service.RegisterMQTTHandlers(cfg.Config{CommandCall: mqtt.TopicConfig{Topic: "v1/gateway/{productCode}/command/call/{identifier}"}, CommandResult: mqtt.TopicConfig{Topic: "v1/gateway/{productCode}/command/result"}})

	server := New(Config{
		AuthService: authService,
		CommandCall: func(identifier string, req cmdapi.CommandRequest) (cmdapi.CommandResponse, int) {
			return service.Execute(identifier, req, "")
		},
	})
	router := server.router()

	body := []byte(`{"trace_id":"trace-http-command-1","device_code":"qhl0001","data":{}}`)
	req := newSignedProtectedRequest(http.MethodPost, "/api/v1/device/control/command/call/program_install", body, "demo", token, secret)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d body=%s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := response["code"]; got != float64(ctl.CodeAccepted) {
		t.Fatalf("response code = %#v, want %d", got, ctl.CodeAccepted)
	}
	waitForFlow(t, time.Second, func() bool { return publisher.CommandResultCount() == 1 })
	message := publisher.CommandResult(0)
	if got := message["trace_id"]; got != "trace-http-command-1" {
		t.Fatalf("trace_id = %#v, want trace-http-command-1", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("command result code = %#v, want %d", got, ctl.CodeSuccess)
	}
}

func newControlFlowAuth(t *testing.T) (rtcontrol.Store, *rtauth.Service, string, []byte) {
	t.Helper()
	root := t.TempDir()
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(root, "control.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	authService, err := rtauth.NewService(rtauth.Config{
		SQLitePath:     pathpkg.Join(root, "runtime.db"),
		KeyFile:        pathpkg.Join(root, "auth.key"),
		BootstrapToken: "bootstrap-secret",
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := authService.BootstrapInit(rtapi.BootstrapInitRequest{AppID: "demo", AppSecret: "secret"}, "bootstrap-secret"); err != nil {
		t.Fatalf("BootstrapInit() error = %v", err)
	}
	secret := []byte("secret")
	now := time.Now().UnixMilli()
	tokenResp, err := authService.IssueToken(rtapi.AuthTokenRequest{
		AppID:     "demo",
		Timestamp: now,
		Nonce:     "nonce-token",
		Signature: signTokenRequestForTest(secret, "demo", now, "nonce-token"),
	})
	if err != nil {
		t.Fatalf("IssueToken() error = %v", err)
	}
	return store, authService, tokenResp.AccessToken, secret
}

func newSignedProtectedRequest(method string, path string, body []byte, appID string, token string, secret []byte) *http.Request {
	timestamp := time.Now().UnixMilli()
	nonce := strings.ReplaceAll(path, "/", "-")
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", appID)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Timestamp", formatInt(timestamp))
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", signProtectedRequestForTest(secret, method, path, body, token, timestamp, nonce, appID))
	return req
}

func signTokenRequestForTest(secret []byte, appID string, timestamp int64, nonce string) string {
	canonical := "POST\n/api/v1/auth/token\n\n" + formatInt(timestamp) + "\n" + nonce + "\n" + appID
	return signForTest(secret, canonical)
}

func signProtectedRequestForTest(secret []byte, method string, path string, body []byte, token string, timestamp int64, nonce string, appID string) string {
	bodyHash := sha256.Sum256(body)
	canonical := strings.ToUpper(strings.TrimSpace(method)) + "\n" + strings.TrimSpace(path) + "\n" + hex.EncodeToString(bodyHash[:]) + "\n" + strings.TrimSpace(token) + "\n" + formatInt(timestamp) + "\n" + nonce + "\n" + appID
	return signForTest(secret, canonical)
}

func waitForFlow(t *testing.T, timeout time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for async flow")
}

func formatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}

func signForTest(secret []byte, canonical string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}
