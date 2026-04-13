package property

import (
	"encoding/json"
	pathpkg "path/filepath"
	"sync"
	"testing"
	"time"

	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	rtapi "github.com/punk-one/edge-service-sdk/property"
	rtcontrol "github.com/punk-one/edge-service-sdk/runtime/control"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type propertyTestCatalog struct {
	device contracts.DeviceConfig
}

func (c *propertyTestCatalog) DeviceConfigByName(name string) (contracts.DeviceConfig, bool) {
	if c.device.Name == name {
		return c.device, true
	}
	return contracts.DeviceConfig{}, false
}

func (c *propertyTestCatalog) DevicesByProductCode(productCode string) []contracts.DeviceConfig {
	if c.device.ProductCode == productCode {
		return []contracts.DeviceConfig{c.device}
	}
	return nil
}

func (c *propertyTestCatalog) ProductCodes() []string {
	if c.device.ProductCode == "" {
		return nil
	}
	return []string{c.device.ProductCode}
}

type propertyTestDriver struct {
	readValues []*contracts.CommandValue
	readErr    error
	writeErr   error
}

func (d *propertyTestDriver) Initialize(sdk contracts.DeviceServiceSDK) error { return nil }
func (d *propertyTestDriver) HandleReadCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest) ([]*contracts.CommandValue, error) {
	return d.readValues, d.readErr
}
func (d *propertyTestDriver) HandleWriteCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest, params []*contracts.CommandValue) error {
	return d.writeErr
}
func (d *propertyTestDriver) Stop(force bool) error { return nil }
func (d *propertyTestDriver) AddDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *propertyTestDriver) UpdateDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *propertyTestDriver) RemoveDevice(deviceName string, protocols map[string]contracts.ProtocolProperties) error {
	return nil
}
func (d *propertyTestDriver) ValidateDevice(device contracts.Device) error { return nil }
func (d *propertyTestDriver) Start() error                                 { return nil }
func (d *propertyTestDriver) Discover() error                              { return nil }

type propertyPublishedMessage struct {
	device  contracts.DeviceConfig
	payload map[string]interface{}
}

type propertyTestPublisher struct {
	mu       sync.Mutex
	messages []propertyPublishedMessage
}

func (p *propertyTestPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	return nil
}
func (p *propertyTestPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	return nil
}
func (p *propertyTestPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return nil
}
func (p *propertyTestPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var copied map[string]interface{}
	if err := json.Unmarshal(body, &copied); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, propertyPublishedMessage{device: device, payload: copied})
	return nil
}
func (p *propertyTestPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.PublishPropertyResult(device, payload)
}

func (p *propertyTestPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return p.PublishPropertyResult(device, payload)
}
func (p *propertyTestPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *propertyTestPublisher) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	return nil
}
func (p *propertyTestPublisher) HealthCheck() error { return nil }
func (p *propertyTestPublisher) Close() error       { return nil }

func (p *propertyTestPublisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func (p *propertyTestPublisher) Message(index int) propertyPublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.messages[index]
}

func TestHandlePropertyGetPublishesPropertyResultWithoutProductCode(t *testing.T) {
	catalog := &propertyTestCatalog{
		device: contracts.DeviceConfig{
			Name:        "acm006",
			ProductCode: "acm",
			Property: contracts.PropertyConfig{
				Points: []contracts.PointConfig{
					{Name: "status_text", ValueType: "String", NodeName: "DB1.DBB0"},
				},
			},
		},
	}
	driver := &propertyTestDriver{
		readValues: []*contracts.CommandValue{
			{DeviceResourceName: "status_text", Type: "String", Value: "READY"},
		},
	}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, nil, nil)
	service.propertyResultEnabled = true

	service.handlePropertyGet("acm", []byte(`{"device_code":"acm006","trace_id":"trace-1","data":{"properties":["status_text"]}}`))

	if publisher.Count() != 1 {
		t.Fatalf("expected 1 property result, got %d", publisher.Count())
	}
	message := publisher.Message(0).payload
	if _, ok := message["product_code"]; ok {
		t.Fatalf("did not expect product_code in property result: %#v", message)
	}
	if _, ok := message["device_code"]; ok {
		t.Fatalf("did not expect device_code in property result: %#v", message)
	}
	if got := message["trace_id"]; got != "trace-1" {
		t.Fatalf("trace_id = %#v, want trace-1", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}
}

func TestHandlePropertySetPublishesDelayedReadbackForMQTT(t *testing.T) {
	catalog := &propertyTestCatalog{
		device: contracts.DeviceConfig{
			Name:        "acm006",
			ProductCode: "acm",
			Property: contracts.PropertyConfig{
				Points: []contracts.PointConfig{
					{Name: "status_text", ValueType: "String", NodeName: "DB1.DBB0", ReadWrite: "RW"},
				},
			},
		},
	}
	driver := &propertyTestDriver{
		readValues: []*contracts.CommandValue{
			{DeviceResourceName: "status_text", Type: "String", Value: "RUNNING"},
		},
	}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, nil, nil)
	service.propertyResultEnabled = true
	service.setPostDelay = 10 * time.Millisecond

	service.handlePropertySet("acm", []byte(`{"device_code":"acm006","trace_id":"trace-set-1","data":{"status_text":"RUNNING"}}`))

	waitForPropertyMessages(t, publisher, 1, 500*time.Millisecond)
	message := publisher.Message(0).payload
	if got := message["trace_id"]; got != "trace-set-1" {
		t.Fatalf("trace_id = %#v, want trace-set-1", got)
	}
	if _, ok := message["product_code"]; ok {
		t.Fatalf("did not expect product_code in property result: %#v", message)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}
	data, ok := message["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data = %#v, want object", message["data"])
	}
	if got := data["status_text"]; got != "RUNNING" {
		t.Fatalf("status_text = %#v, want RUNNING", got)
	}
}

func TestExecuteGetRejectsExpiredRequest(t *testing.T) {
	catalog := &propertyTestCatalog{
		device: contracts.DeviceConfig{Name: "acm006", ProductCode: "acm"},
	}
	service := NewService(catalog, &propertyTestDriver{}, &propertyTestPublisher{}, nil, nil)

	response, statusCode := service.ExecuteGet(rtapi.PropertyRequest{
		TraceID:    "trace-expired",
		DeviceCode: "acm006",
		Data:       map[string]interface{}{"properties": []string{}},
		Metadata: &ctl.Metadata{
			ExpiryTime: time.Now().Add(-time.Second).UnixMilli(),
		},
	}, "")

	if statusCode != 410 {
		t.Fatalf("statusCode = %d, want 410", statusCode)
	}
	if response.Code != ctl.CodeExpired {
		t.Fatalf("code = %d, want %d", response.Code, ctl.CodeExpired)
	}
}

func TestExecuteGetAsyncReturnsAcceptedAndPublishesFinalResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "property-get-async.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	catalog := &propertyTestCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &propertyTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "status_text", Type: "String", Value: "READY"}}}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, store, nil)
	service.propertyResultEnabled = true

	response, statusCode := service.ExecuteGet(rtapi.PropertyRequest{
		TraceID:    "trace-property-get-async",
		DeviceCode: "acm006",
		Data:       map[string]interface{}{"properties": []string{"status_text"}},
		Metadata:   &ctl.Metadata{ExpectAck: true},
	}, "")
	if statusCode != 202 {
		t.Fatalf("statusCode = %d, want 202", statusCode)
	}
	if response.Code != ctl.CodeAccepted {
		t.Fatalf("code = %d, want %d", response.Code, ctl.CodeAccepted)
	}

	waitForPropertyMessages(t, publisher, 1, time.Second)
	message := publisher.Message(0).payload
	if got := message["trace_id"]; got != "trace-property-get-async" {
		t.Fatalf("trace_id = %#v, want trace-property-get-async", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}
	data, ok := message["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data = %#v, want object", message["data"])
	}
	if got := data["status_text"]; got != "READY" {
		t.Fatalf("status_text = %#v, want READY", got)
	}
	job, found, err := store.LoadJob("trace-property-get-async")
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Kind != "property:get" {
		t.Fatalf("job.Kind = %q, want property:get", job.Kind)
	}
	pending, err := store.ListPendingProperties()
	if err != nil {
		t.Fatalf("ListPendingProperties error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending properties = %d, want 0", len(pending))
	}
}

func TestExecuteSetRejectsCacheFirstStrategy(t *testing.T) {
	catalog := &propertyTestCatalog{
		device: contracts.DeviceConfig{Name: "acm006", ProductCode: "acm"},
	}
	service := NewService(catalog, &propertyTestDriver{}, &propertyTestPublisher{}, nil, nil)

	response, statusCode := service.ExecuteSet(rtapi.PropertyRequest{
		TraceID:    "trace-cache-first-set",
		DeviceCode: "acm006",
		Data:       map[string]interface{}{"mode": 1},
		Metadata: &ctl.Metadata{
			Strategy: "cache_first",
		},
	}, "")

	if statusCode != 400 {
		t.Fatalf("statusCode = %d, want 400", statusCode)
	}
	if response.Code != ctl.CodeBadRequest {
		t.Fatalf("code = %d, want %d", response.Code, ctl.CodeBadRequest)
	}
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func waitForPropertyMessages(t *testing.T, publisher *propertyTestPublisher, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if publisher.Count() >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected at least %d property messages, got %d", count, publisher.Count())
}

func TestExecuteSetAsyncReturnsAcceptedAndPublishesFinalResult(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "property-async.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	catalog := &propertyTestCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &propertyTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "status_text", Type: "String", Value: "RUNNING"}}}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, store, nil)
	service.propertyResultEnabled = true
	service.setPostDelay = 10 * time.Millisecond

	response, statusCode := service.ExecuteSet(rtapi.PropertyRequest{
		TraceID:    "trace-property-async",
		DeviceCode: "acm006",
		Data:       map[string]interface{}{"status_text": "RUNNING"},
		Metadata:   &ctl.Metadata{ExpectAck: true},
	}, "")
	if statusCode != 202 {
		t.Fatalf("statusCode = %d, want 202", statusCode)
	}
	if response.Code != ctl.CodeAccepted {
		t.Fatalf("code = %d, want %d", response.Code, ctl.CodeAccepted)
	}

	waitForPropertyMessages(t, publisher, 1, time.Second)
	message := publisher.Message(0).payload
	if got := message["trace_id"]; got != "trace-property-async" {
		t.Fatalf("trace_id = %#v, want trace-property-async", got)
	}
	if got := message["code"]; got != float64(ctl.CodeSuccess) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeSuccess)
	}
	job, found, err := store.LoadJob("trace-property-async")
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
	events, err := store.ListResults("trace-property-async", 10)
	if err != nil {
		t.Fatalf("ListResults error = %v", err)
	}
	if len(events) < 4 {
		t.Fatalf("len(events) = %d, want at least 4", len(events))
	}
	progress, ok := events[1].Data["progress"].(map[string]interface{})
	if !ok {
		t.Fatalf("progress data = %#v, want object", events[1].Data)
	}
	if got := progress["stage"]; got != "write" {
		t.Fatalf("progress.stage = %#v, want write", got)
	}
	if got := progress["property_count"]; got != float64(1) {
		t.Fatalf("progress.property_count = %#v, want 1", got)
	}
	pending, err := store.ListPendingProperties()
	if err != nil {
		t.Fatalf("ListPendingProperties error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending properties = %d, want 0", len(pending))
	}
}

func TestExecuteAsyncPropertySetRecordsFailureContext(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "property-failure.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	catalog := &propertyTestCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &propertyTestDriver{writeErr: assertErr("driver busy")}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, store, nil)
	service.propertyResultEnabled = true

	response, statusCode := service.ExecuteSet(rtapi.PropertyRequest{
		TraceID:    "trace-property-failed",
		DeviceCode: "acm006",
		Data:       map[string]interface{}{"status_text": "RUNNING"},
		Metadata:   &ctl.Metadata{ExpectAck: true},
	}, "")
	if statusCode != 202 || response.Code != ctl.CodeAccepted {
		t.Fatalf("unexpected accepted response: status=%d body=%#v", statusCode, response)
	}

	waitForPropertyMessages(t, publisher, 1, time.Second)
	message := publisher.Message(0).payload
	if got := message["code"]; got != float64(ctl.CodeDriverError) {
		t.Fatalf("code = %#v, want %d", got, ctl.CodeDriverError)
	}
	data, ok := message["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data = %#v, want object", message["data"])
	}
	failure, ok := data["failure_context"].(map[string]interface{})
	if !ok {
		t.Fatalf("failure_context = %#v, want object", data["failure_context"])
	}
	if got := failure["stage"]; got != "write" {
		t.Fatalf("failure_context.stage = %#v, want write", got)
	}
	if got := failure["phase"]; got != "failed" {
		t.Fatalf("failure_context.phase = %#v, want failed", got)
	}
}

func TestResumePendingPropertySet(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "property-resume.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	request := rtapi.PropertyRequest{TraceID: "trace-property-resume", DeviceCode: "acm006", Data: map[string]interface{}{"status_text": "READY"}}
	if _, err := store.UpsertJob(rtcontrol.JobState{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "acm", Kind: "property:set", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("UpsertJob accepted error = %v", err)
	}
	if err := store.SaveResult(request.TraceID, ctl.Result{TraceID: request.TraceID, Code: ctl.CodeAccepted, Message: "accepted", Data: map[string]interface{}{"accepted": true}, Time: now}, false); err != nil {
		t.Fatalf("SaveResult accepted error = %v", err)
	}
	if _, err := store.SavePendingProperty(rtcontrol.PendingProperty{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "acm", Request: ctl.Request(request), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("SavePendingProperty error = %v", err)
	}

	catalog := &propertyTestCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &propertyTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "status_text", Type: "String", Value: "READY"}}}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, store, nil)
	service.propertyResultEnabled = true
	service.setPostDelay = 10 * time.Millisecond

	if err := service.ResumePending(); err != nil {
		t.Fatalf("ResumePending error = %v", err)
	}
	waitForPropertyMessages(t, publisher, 1, time.Second)

	job, found, err := store.LoadJob(request.TraceID)
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
}

func TestResumePendingPropertyGet(t *testing.T) {
	store, err := rtcontrol.NewSQLiteStore(pathpkg.Join(t.TempDir(), "property-get-resume.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore error = %v", err)
	}
	defer store.Close()

	now := time.Now().UnixMilli()
	request := rtapi.PropertyRequest{TraceID: "trace-property-get-resume", DeviceCode: "acm006", Data: map[string]interface{}{"properties": []string{"status_text"}}}
	if _, err := store.UpsertJob(rtcontrol.JobState{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "acm", Kind: "property:get", Code: ctl.CodeAccepted, Message: "accepted", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("UpsertJob accepted error = %v", err)
	}
	if err := store.SaveResult(request.TraceID, ctl.Result{TraceID: request.TraceID, Code: ctl.CodeAccepted, Message: "accepted", Data: map[string]interface{}{"accepted": true}, Time: now}, false); err != nil {
		t.Fatalf("SaveResult accepted error = %v", err)
	}
	if _, err := store.SavePendingProperty(rtcontrol.PendingProperty{TraceID: request.TraceID, DeviceCode: request.DeviceCode, ProductCode: "acm", Operation: propertyOperationGet, Request: ctl.Request(request), CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("SavePendingProperty error = %v", err)
	}

	catalog := &propertyTestCatalog{device: contracts.DeviceConfig{
		Name:        "acm006",
		ProductCode: "acm",
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{
			Name:      "status_text",
			ValueType: "String",
			NodeName:  "DB1.DBB0",
			ReadWrite: "RW",
		}}},
	}}
	driver := &propertyTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "status_text", Type: "String", Value: "READY"}}}
	publisher := &propertyTestPublisher{}
	service := NewService(catalog, driver, publisher, store, nil)
	service.propertyResultEnabled = true

	if err := service.ResumePending(); err != nil {
		t.Fatalf("ResumePending error = %v", err)
	}
	waitForPropertyMessages(t, publisher, 1, time.Second)

	job, found, err := store.LoadJob(request.TraceID)
	if err != nil || !found {
		t.Fatalf("LoadJob = (%#v, %v, %v), want found job", job, found, err)
	}
	if job.Kind != "property:get" {
		t.Fatalf("job.Kind = %q, want property:get", job.Kind)
	}
	if job.Code != ctl.CodeSuccess {
		t.Fatalf("job.Code = %d, want %d", job.Code, ctl.CodeSuccess)
	}
}
