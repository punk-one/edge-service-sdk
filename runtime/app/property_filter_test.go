package app

import (
	"testing"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestShouldEmitPropertyRespectsDeadband(t *testing.T) {
	state := telemetryState{
		lastValues: map[string]interface{}{
			"temperature": 10.0,
		},
		lastEmittedAt: map[string]int64{
			"temperature": time.Now().Add(-2 * time.Second).UnixMilli(),
		},
	}

	cfg := contracts.PropertyConfig{
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
	if shouldEmitProperty(cfg, values, state, time.Now()) {
		t.Fatal("expected property auto-report to stay silent when deadband is not exceeded")
	}

	values[0].Value = 10.6
	if !shouldEmitProperty(cfg, values, state, time.Now()) {
		t.Fatal("expected property auto-report when deadband is exceeded")
	}
}

func TestPropertyAutoReportingEnabledRequiresPositiveInterval(t *testing.T) {
	if propertyAutoReportingEnabled(contracts.PropertyConfig{Interval: "0s"}) {
		t.Fatal("expected zero interval to disable property auto-reporting")
	}
	if !propertyAutoReportingEnabled(contracts.PropertyConfig{Interval: "2s"}) {
		t.Fatal("expected positive interval to enable property auto-reporting")
	}
}
