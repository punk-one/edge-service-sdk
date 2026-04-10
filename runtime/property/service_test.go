package property

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
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
func (p *propertyTestPublisher) PublishPropertyPost(device contracts.DeviceConfig, payload map[string]interface{}) error {
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

func TestHandlePropertyGetPublishesPropertyPostWithoutProductCode(t *testing.T) {
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
	service := NewService(catalog, driver, publisher, nil)
	service.propertyPostEnabled = true

	service.handlePropertyGet("acm", []byte(`{"device_code":"acm006","trace_id":"trace-1","data":{"status_text":true}}`))

	if publisher.Count() != 1 {
		t.Fatalf("expected 1 property post, got %d", publisher.Count())
	}
	message := publisher.Message(0).payload
	if _, ok := message["product_code"]; ok {
		t.Fatalf("did not expect product_code in property post: %#v", message)
	}
	if got := message["trace_id"]; got != "trace-1" {
		t.Fatalf("trace_id = %#v, want trace-1", got)
	}
	if got := message["device_code"]; got != "acm006" {
		t.Fatalf("device_code = %#v, want acm006", got)
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
	service := NewService(catalog, driver, publisher, nil)
	service.propertyPostEnabled = true
	service.setPostDelay = 10 * time.Millisecond

	service.handlePropertySet("acm", []byte(`{"device_code":"acm006","trace_id":"trace-set-1","data":{"status_text":"RUNNING"}}`))

	waitForPropertyMessages(t, publisher, 1, 500*time.Millisecond)
	message := publisher.Message(0).payload
	if got := message["trace_id"]; got != "trace-set-1" {
		t.Fatalf("trace_id = %#v, want trace-set-1", got)
	}
	if _, ok := message["product_code"]; ok {
		t.Fatalf("did not expect product_code in property post: %#v", message)
	}
	data, ok := message["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data = %#v, want object", message["data"])
	}
	if got := data["status_text"]; got != "RUNNING" {
		t.Fatalf("status_text = %#v, want RUNNING", got)
	}
}

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
