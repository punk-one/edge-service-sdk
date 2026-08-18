package app

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

func TestExceedsPercentDeadband(t *testing.T) {
	changed, comparable := exceedsPercentDeadband(100.0, 103.0, 5.0)
	if !comparable {
		t.Fatal("expected numeric values to be comparable")
	}
	if changed {
		t.Fatal("expected 3% change to not exceed 5% deadband")
	}

	changed, comparable = exceedsPercentDeadband(100.0, 106.0, 5.0)
	if !comparable {
		t.Fatal("expected numeric values to be comparable")
	}
	if !changed {
		t.Fatal("expected 6% change to exceed 5% deadband")
	}

	changed, comparable = exceedsPercentDeadband(0.0, 0.0, 5.0)
	if !comparable {
		t.Fatal("expected numeric values to be comparable")
	}
	if changed {
		t.Fatal("expected no change from zero to zero")
	}

	changed, comparable = exceedsPercentDeadband(0.0, 10.0, 5.0)
	if !comparable {
		t.Fatal("expected numeric values to be comparable")
	}
	if !changed {
		t.Fatal("expected change from zero to trigger emission")
	}

	changed, comparable = exceedsPercentDeadband("a", "b", 5.0)
	if comparable {
		t.Fatal("expected non-numeric values to be non-comparable")
	}
}

func TestShouldEmitTelemetryRespectsDeadbandPercent(t *testing.T) {
	state := telemetryState{
		lastValues: map[string]interface{}{
			"speed": 100.0,
		},
		lastEmittedAt: map[string]int64{
			"speed": time.Now().Add(-2 * time.Second).UnixMilli(),
		},
	}

	cfg := contracts.TelemetryConfig{
		Points: []contracts.PointConfig{
			{
				Name:            "speed",
				DeadbandPercent: 5.0,
			},
		},
	}

	values := []*contracts.CommandValue{
		{
			DeviceResourceName: "speed",
			Value:              103.0,
		},
	}
	if shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected telemetry to stay silent when percent deadband is not exceeded")
	}

	values[0].Value = 106.0
	if !shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected telemetry emission when percent deadband is exceeded")
	}

	values[0].Value = 94.0
	state.lastValues["speed"] = 100.0
	if !shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected telemetry emission when 6% decrease exceeds 5% deadband")
	}
}

func TestShouldEmitTelemetryRespectsDeadbandPercentOr(t *testing.T) {
	state := telemetryState{
		lastValues: map[string]interface{}{
			"speed": 100.0,
		},
		lastEmittedAt: map[string]int64{
			"speed": time.Now().Add(-2 * time.Second).UnixMilli(),
		},
	}

	cfg := contracts.TelemetryConfig{
		Points: []contracts.PointConfig{
			{
				Name:            "speed",
				Deadband:        10.0,
				DeadbandPercent: 5.0,
			},
		},
	}

	values := []*contracts.CommandValue{
		{
			DeviceResourceName: "speed",
			Value:              103.0,
		},
	}
	if shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected silence when 3% change is within 5% deadband and 3 < 10 absolute")
	}

	values[0].Value = 106.0
	deadbandCfg := contracts.TelemetryConfig{
		Points: []contracts.PointConfig{
			{
				Name:            "speed",
				Deadband:        10.0,
				DeadbandPercent: 5.0,
			},
		},
	}
	if !shouldEmitTelemetry(deadbandCfg, values, state, time.Now()) {
		t.Fatal("expected emission when 6% > 5% percent deadband, OR condition")
	}

	state.lastValues["speed"] = 100.0
	values[0].Value = 101.0
	if shouldEmitTelemetry(cfg, values, state, time.Now()) {
		t.Fatal("expected silence when 1% < 5% and 1 < 10")
	}
}

func TestEffectiveGroupConfigFallback(t *testing.T) {
	device := contracts.TelemetryConfig{
		Interval:          "30s",
		OnChange:          true,
		WatchedFields:     []string{"alarm"},
		HeartbeatInterval: "60s",
	}

	group := contracts.TelemetryGroup{
		Name:     "fast",
		Interval: "100ms",
		Points:   []contracts.PointConfig{{Name: "vibration"}},
	}

	cfg := effectiveGroupConfig(device, group)
	if cfg.Interval != "100ms" {
		t.Fatalf("expected group interval 100ms, got %s", cfg.Interval)
	}
	if !cfg.OnChange {
		t.Fatal("expected device OnChange=true to fallback to group")
	}
	if cfg.HeartbeatInterval != "60s" {
		t.Fatalf("expected device heartbeat 60s to fallback to group, got %s", cfg.HeartbeatInterval)
	}
	if cfg.WatchedFields == nil || cfg.WatchedFields[0] != "alarm" {
		t.Fatal("expected device watchedFields to fallback to group")
	}
	if len(cfg.Points) != 1 || cfg.Points[0].Name != "vibration" {
		t.Fatal("expected group points")
	}
}

func TestEffectiveGroupConfigOverride(t *testing.T) {
	device := contracts.TelemetryConfig{
		Interval:          "30s",
		OnChange:          false,
		WatchedFields:     []string{"alarm"},
		HeartbeatInterval: "60s",
	}

	group := contracts.TelemetryGroup{
		Name:              "fast",
		Interval:          "100ms",
		OnChange:          true,
		WatchedFields:     []string{"vibration"},
		HeartbeatInterval: "10s",
		Points:            []contracts.PointConfig{{Name: "vibration"}},
	}

	cfg := effectiveGroupConfig(device, group)
	if cfg.Interval != "100ms" {
		t.Fatalf("expected group interval 100ms, got %s", cfg.Interval)
	}
	if !cfg.OnChange {
		t.Fatal("expected group OnChange=true to override device")
	}
	if cfg.HeartbeatInterval != "10s" {
		t.Fatalf("expected group heartbeat 10s, got %s", cfg.HeartbeatInterval)
	}
	if cfg.WatchedFields == nil || cfg.WatchedFields[0] != "vibration" {
		t.Fatal("expected group watchedFields to override device")
	}
}
