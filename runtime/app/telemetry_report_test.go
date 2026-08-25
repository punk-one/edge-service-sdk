package app

import (
	"errors"
	"testing"

	rtconfig "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
)

type telemetrySinkStub struct {
	device contracts.DeviceConfig
	values *contracts.AsyncValues
	err    error
}

func (s *telemetrySinkStub) PublishAsyncValues(device contracts.DeviceConfig, values *contracts.AsyncValues) error {
	s.device = device
	s.values = values
	return s.err
}

func (s *telemetrySinkStub) Stats() (reliable.TelemetryOutboxStats, error) {
	return reliable.TelemetryOutboxStats{}, nil
}

func (s *telemetrySinkStub) Close() error { return nil }

func TestReportAsyncValuesUsesSynchronousTelemetrySink(t *testing.T) {
	sdk := NewDeviceSDK(rtconfig.Config{Devices: []contracts.DeviceConfig{{
		Name:        "device-01",
		ProductCode: "product-01",
	}}}, nil, nil)
	sink := &telemetrySinkStub{}
	sdk.telemetrySink = sink
	values := &contracts.AsyncValues{DeviceName: "device-01", CollectedAt: 1_000}

	if err := sdk.ReportAsyncValues(values); err != nil {
		t.Fatalf("ReportAsyncValues() error = %v", err)
	}
	if sink.values != values || sink.device.Name != "device-01" || sink.device.ProductCode != "product-01" {
		t.Fatalf("telemetry sink received device=%#v values=%#v", sink.device, sink.values)
	}
}

func TestReportAsyncValuesPropagatesPersistenceFailure(t *testing.T) {
	sdk := NewDeviceSDK(rtconfig.Config{Devices: []contracts.DeviceConfig{{Name: "device-01"}}}, nil, nil)
	wantErr := errors.New("sqlite full")
	sdk.telemetrySink = &telemetrySinkStub{err: wantErr}

	err := sdk.ReportAsyncValues(&contracts.AsyncValues{DeviceName: "device-01"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ReportAsyncValues() error = %v, want %v", err, wantErr)
	}
}
