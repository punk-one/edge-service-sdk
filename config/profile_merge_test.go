package config

import (
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestApplyProfilesMergesReusableProfile(t *testing.T) {
	devices := []contracts.DeviceConfig{
		{
			Name:        "acm006",
			ProfileName: "acm-profile",
			ProductCode: "acm",
			Protocols: map[string]interface{}{
				"s7": map[string]interface{}{
					"Host": "127.0.0.1",
					"Port": 102,
					"Rack": 0,
					"Slot": 1,
				},
			},
		},
	}
	profiles := map[string]contracts.DeviceProfile{
		"acm-profile": {
			Name:        "acm-profile",
			Description: "shared profile",
			Labels:      []string{"industrial"},
			Telemetry: contracts.TelemetryConfig{
				Interval: "10s",
				OnChange: true,
				Points: []contracts.PointConfig{
					{Name: "alarm", ValueType: "Bool", NodeName: "DB1.DBX0.0"},
				},
			},
			Property: contracts.PropertyConfig{
				Interval:          "5s",
				OnChange:          true,
				WatchedFields:     []string{"reset"},
				HeartbeatInterval: "30s",
				Points: []contracts.PointConfig{
					{Name: "reset", ValueType: "Bool", NodeName: "DB1.DBX0.1", ReadWrite: "RW"},
				},
			},
		},
	}

	merged, err := applyProfiles(devices, profiles)
	if err != nil {
		t.Fatalf("applyProfiles() error = %v", err)
	}
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged device, got %d", len(merged))
	}
	if merged[0].Description != "shared profile" {
		t.Fatalf("expected description from profile, got %q", merged[0].Description)
	}
	if merged[0].Telemetry.Interval != "10s" || len(merged[0].Telemetry.Points) != 1 {
		t.Fatalf("expected telemetry merged from profile, got %#v", merged[0].Telemetry)
	}
	if len(merged[0].Property.Points) != 1 || merged[0].Property.Points[0].ReadWrite != "RW" {
		t.Fatalf("expected property points merged from profile, got %#v", merged[0].Property.Points)
	}
	if merged[0].Property.Interval != "5s" || !merged[0].Property.OnChange || merged[0].Property.HeartbeatInterval != "30s" {
		t.Fatalf("expected property strategy merged from profile, got %#v", merged[0].Property)
	}
	if len(merged[0].Property.WatchedFields) != 1 || merged[0].Property.WatchedFields[0] != "reset" {
		t.Fatalf("expected property watched fields merged from profile, got %#v", merged[0].Property.WatchedFields)
	}
}

func TestApplyProfilesFailsForUnknownProfile(t *testing.T) {
	_, err := applyProfiles([]contracts.DeviceConfig{{Name: "d1", ProfileName: "missing"}}, map[string]contracts.DeviceProfile{})
	if err == nil {
		t.Fatal("expected error for missing profile")
	}
}
