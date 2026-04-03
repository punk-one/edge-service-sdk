package config

import (
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestShouldEmitTelemetryRespectsDeadband(t *testing.T) {
	state := telemetryState{
		lastValues: map[string]interface{}{
			"temperature": 10.0,
		},
		lastEmittedAt: map[string]int64{
			"temperature": time.Now().Add(-2 * time.Second).UnixMilli(),
		},
	}

	cfg := contracts.TelemetryConfig{
		Points: []contracts.PointConfig{
			{
				Name:     "temperature",
				Deadband: 0.5,
			},
		},
	}

	values := []*contracts.CommandValue{
		{
			DeviceResourceName: "temperature",
			Value:              10.2,
		},
	}
	if shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected telemetry to stay silent when deadband is not exceeded")
	}

	values[0].Value = 10.6
	if !shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected telemetry emission when deadband is exceeded")
	}
}

func TestShouldEmitTelemetryRespectsHeartbeat(t *testing.T) {
	state := telemetryState{
		lastValues: map[string]interface{}{
			"alarm": false,
		},
		lastEmittedAt: map[string]int64{
			"alarm": time.Now().Add(-3 * time.Second).UnixMilli(),
		},
	}

	cfg := contracts.TelemetryConfig{
		Points: []contracts.PointConfig{
			{
				Name:              "alarm",
				HeartbeatInterval: "2s",
			},
		},
	}

	values := []*contracts.CommandValue{
		{
			DeviceResourceName: "alarm",
			Value:              false,
		},
	}

	if !shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected heartbeat to force telemetry emission")
	}
}
