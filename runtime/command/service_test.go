package command

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	ctl "github.com/punk-one/edge-service-sdk/control"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

type commandTestCatalog struct {
	device contracts.DeviceConfig
}

func (c *commandTestCatalog) DeviceConfigByName(name string) (contracts.DeviceConfig, bool) {
	if c.device.Name == name {
		return c.device, true
	}
	return contracts.DeviceConfig{}, false
}

func (c *commandTestCatalog) DevicesByProductCode(productCode string) []contracts.DeviceConfig {
	if c.device.ProductCode == productCode {
		return []contracts.DeviceConfig{c.device}
	}
	return nil
}

func (c *commandTestCatalog) ProductCodes() []string {
	if c.device.ProductCode == "" {
		return nil
	}
	return []string{c.device.ProductCode}
}

type commandTestDriver struct {
	readValues []*contracts.CommandValue
	readErr    error
	writeErr   error
	lastWrites []*contracts.CommandValue
}

func (d *commandTestDriver) Initialize(sdk contracts.DeviceServiceSDK) error { return nil }
func (d *commandTestDriver) HandleReadCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest) ([]*contracts.CommandValue, error) {
	return d.readValues, d.readErr
}
func (d *commandTestDriver) HandleWriteCommands(deviceName string, protocols map[string]contracts.ProtocolProperties, reqs []contracts.CommandRequest, params []*contracts.CommandValue) error {
	d.lastWrites = params
	return d.writeErr
}
func (d *commandTestDriver) Stop(force bool) error { return nil }
func (d *commandTestDriver) AddDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *commandTestDriver) UpdateDevice(deviceName string, protocols map[string]contracts.ProtocolProperties, adminState contracts.AdminState) error {
	return nil
}
func (d *commandTestDriver) RemoveDevice(deviceName string, protocols map[string]contracts.ProtocolProperties) error {
	return nil
}
func (d *commandTestDriver) ValidateDevice(device contracts.Device) error { return nil }
func (d *commandTestDriver) Start() error                                 { return nil }
func (d *commandTestDriver) Discover() error                              { return nil }

type commandPublishedMessage struct {
	device  contracts.DeviceConfig
	payload map[string]interface{}
}

type commandTestPublisher struct {
	mu       sync.Mutex
	messages []commandPublishedMessage
}

func (p *commandTestPublisher) PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error {
	return nil
}
func (p *commandTestPublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	return nil
}
func (p *commandTestPublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return nil
}
func (p *commandTestPublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *commandTestPublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *commandTestPublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
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
	p.messages = append(p.messages, commandPublishedMessage{device: device, payload: copied})
	return nil
}
func (p *commandTestPublisher) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	return nil
}
func (p *commandTestPublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return nil
}
func (p *commandTestPublisher) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	return nil
}
func (p *commandTestPublisher) HealthCheck() error { return nil }
func (p *commandTestPublisher) Close() error       { return nil }

func (p *commandTestPublisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func (p *commandTestPublisher) Message(index int) commandPublishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.messages[index]
}

type stubCommand struct {
	desc cmdapi.CommandDescriptor
	fn   func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError)
}

func (c stubCommand) Descriptor() cmdapi.CommandDescriptor {
	return c.desc
}

func (c stubCommand) Execute(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
	if c.fn == nil {
		return map[string]interface{}{}, nil
	}
	return c.fn(ctx, req)
}

func newTestRegistry(commands ...cmdapi.Command) cmdapi.Registry {
	registry := cmdapi.NewRegistry()
	for _, item := range commands {
		registry.MustRegister(item)
	}
	return registry
}

func testDevice(name string, productCode string, commands ...string) contracts.DeviceConfig {
	device := contracts.DeviceConfig{Name: name, ProductCode: productCode}
	if len(commands) == 0 {
		return device
	}
	device.Commands = make([]contracts.CommandConfig, 0, len(commands))
	for _, identifier := range commands {
		device.Commands = append(device.Commands, contracts.CommandConfig{Identifier: identifier})
	}
	return device
}

func TestExecuteRegisteredCommandUsesTelemetryHelper(t *testing.T) {
	device := testDevice("qhl0001", "qhl", "read_line_snapshot")
	device.Telemetry = contracts.TelemetryConfig{Points: []contracts.PointConfig{{
		Name:      "in_production",
		ValueType: "Bool",
		NodeName:  "MX1.3",
	}}}
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{Identifier: "read_line_snapshot", Mode: "sync"},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			values, err := ctx.ReadTelemetry([]string{"in_production"})
			if err != nil {
				return nil, cmdapi.DriverError(err)
			}
			return values, nil
		},
	})
	driver := &commandTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "in_production", Type: "Bool", Value: true}}}
	service := NewService(&commandTestCatalog{device: device}, driver, &commandTestPublisher{}, nil, nil, registry, nil)

	result, statusCode := service.Execute("read_line_snapshot", cmdapi.CommandRequest{TraceID: "trace-telemetry", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 200 {
		t.Fatalf("statusCode = %d, want 200", statusCode)
	}
	if result.Code != ctl.CodeSuccess {
		t.Fatalf("code = %d, want %d", result.Code, ctl.CodeSuccess)
	}
	if got := result.Data["in_production"]; got != true {
		t.Fatalf("in_production = %#v, want true", got)
	}
}

func TestExecuteRegisteredCommandUsesPropertyHelpers(t *testing.T) {
	device := testDevice("qhl0001", "qhl", "set_flux_conv_speed")
	device.Property = contracts.PropertyConfig{Points: []contracts.PointConfig{{
		Name:      "flux_conv_speed_setpoint",
		ValueType: "Float32",
		NodeName:  "DB20.DBD54",
		ReadWrite: "RW",
	}}}
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{
			Identifier: "set_flux_conv_speed",
			Mode:       "sync",
			InputParams: []cmdapi.CommandParam{{
				Identifier: "flux_conv_speed_setpoint",
				ValueType:  "Float32",
				Required:   true,
			}},
		},
		fn: func(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
			if err := ctx.SetProperties(map[string]interface{}{"flux_conv_speed_setpoint": req.Data["flux_conv_speed_setpoint"]}); err != nil {
				return nil, cmdapi.DriverError(err)
			}
			values, err := ctx.GetProperties([]string{"flux_conv_speed_setpoint"})
			if err != nil {
				return nil, cmdapi.DriverError(err)
			}
			return values, nil
		},
	})
	driver := &commandTestDriver{readValues: []*contracts.CommandValue{{DeviceResourceName: "flux_conv_speed_setpoint", Type: "Float32", Value: float32(12.5)}}}
	service := NewService(&commandTestCatalog{device: device}, driver, &commandTestPublisher{}, nil, nil, registry, nil)

	result, statusCode := service.Execute("set_flux_conv_speed", cmdapi.CommandRequest{TraceID: "trace-set", DeviceCode: "qhl0001", Data: map[string]interface{}{"flux_conv_speed_setpoint": 12.5}}, "")
	if statusCode != 200 || result.Code != ctl.CodeSuccess {
		t.Fatalf("unexpected result: status=%d body=%#v", statusCode, result)
	}
	if len(driver.lastWrites) != 1 {
		t.Fatalf("expected 1 write param, got %d", len(driver.lastWrites))
	}
	if got := driver.lastWrites[0].Value; got != float32(12.5) {
		t.Fatalf("write value = %#v, want %v", got, float32(12.5))
	}
	if got := result.Data["flux_conv_speed_setpoint"]; got != float32(12.5) {
		t.Fatalf("result value = %#v, want %v", got, float32(12.5))
	}
}

func TestExecuteRejectsMissingRequiredInput(t *testing.T) {
	registry := newTestRegistry(stubCommand{
		desc: cmdapi.CommandDescriptor{
			Identifier: "set_flux_conv_speed",
			Mode:       "sync",
			InputParams: []cmdapi.CommandParam{{
				Identifier: "flux_conv_speed_setpoint",
				ValueType:  "Float32",
				Required:   true,
			}},
		},
	})
	service := NewService(&commandTestCatalog{device: testDevice("qhl0001", "qhl", "set_flux_conv_speed")}, &commandTestDriver{}, &commandTestPublisher{}, nil, nil, registry, nil)

	result, statusCode := service.Execute("set_flux_conv_speed", cmdapi.CommandRequest{TraceID: "trace-missing", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 400 || result.Code != ctl.CodeBadRequest {
		t.Fatalf("unexpected result: status=%d body=%#v", statusCode, result)
	}
}

func TestExecuteRejectsExpiredRequest(t *testing.T) {
	registry := newTestRegistry(stubCommand{desc: cmdapi.CommandDescriptor{Identifier: "start_machine", Mode: "sync"}})
	service := NewService(&commandTestCatalog{device: testDevice("qhl0001", "qhl", "start_machine")}, &commandTestDriver{}, &commandTestPublisher{}, nil, nil, registry, nil)

	result, statusCode := service.Execute("start_machine", cmdapi.CommandRequest{
		TraceID:    "trace-expired",
		DeviceCode: "qhl0001",
		Data:       map[string]interface{}{},
		Metadata:   &ctl.Metadata{ExpiryTime: time.Now().Add(-time.Second).UnixMilli()},
	}, "")
	if statusCode != 410 || result.Code != ctl.CodeExpired {
		t.Fatalf("unexpected result: status=%d body=%#v", statusCode, result)
	}
}

func TestExecuteReturnsUnsupportedForUnknownCommand(t *testing.T) {
	service := NewService(&commandTestCatalog{device: testDevice("qhl0001", "qhl")}, &commandTestDriver{}, &commandTestPublisher{}, nil, nil, cmdapi.NewRegistry(), nil)

	result, statusCode := service.Execute("unknown", cmdapi.CommandRequest{TraceID: "trace-unknown", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 405 || result.Code != ctl.CodeNotSupported {
		t.Fatalf("unexpected result: status=%d body=%#v", statusCode, result)
	}
}

func TestExecuteReturnsUnsupportedForCommandNotBoundToDevice(t *testing.T) {
	registry := newTestRegistry(stubCommand{desc: cmdapi.CommandDescriptor{Identifier: "start_machine", Mode: "sync"}})
	service := NewService(&commandTestCatalog{device: testDevice("qhl0001", "qhl")}, &commandTestDriver{}, &commandTestPublisher{}, nil, nil, registry, nil)

	result, statusCode := service.Execute("start_machine", cmdapi.CommandRequest{TraceID: "trace-unbound", DeviceCode: "qhl0001", Data: map[string]interface{}{}}, "")
	if statusCode != 405 || result.Code != ctl.CodeNotSupported {
		t.Fatalf("unexpected result: status=%d body=%#v", statusCode, result)
	}
	if result.Message != "command \"start_machine\" is not supported by device \"qhl0001\"" {
		t.Fatalf("message = %q, want device unsupported message", result.Message)
	}
}
