package event

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestLoadProfilesSelectsNamedEventProfile(t *testing.T) {
	dir := t.TempDir()
	content := `name: sample-events
type: EVENT
version: 1
config:
  categories:
    alarm:
      stateModel: aggregate
      events:
        - eventCode: EVENT_ALARM
          name: total alarm
          eventType: rise-clear
          when: "data.alarm_code != 0"
          recover: "data.alarm_code == 0"
          report:
            mode: immediate
            interval: 0s
`
	if err := os.WriteFile(filepath.Join(dir, "sample.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	profiles, err := LoadProfiles(dir)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v", err)
	}
	profile, ok := SelectProfile(profiles, "sample")
	if !ok || profile.Name != "sample-events" {
		t.Fatalf("unexpected selected profile: ok=%t profile=%#v", ok, profile)
	}
	if _, ok := SelectProfile(profiles, "missing"); ok {
		t.Fatal("missing profile should not be selected")
	}
}

func TestLoadProfilesRejectsMiddlewarePipelineKeys(t *testing.T) {
	dir := t.TempDir()
	content := `name: invalid
type: EVENT
config:
  pipeline:
    inputs: [telemetry]
  categories:
    alarm:
      events: []
`
	if err := os.WriteFile(filepath.Join(dir, "invalid.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	_, err := LoadProfiles(dir)
	if err == nil || !strings.Contains(err.Error(), "pipeline") {
		t.Fatalf("expected pipeline validation error, got %v", err)
	}
}

func TestValidateForDeviceUsesGroupsAndStandalonePoints(t *testing.T) {
	device := contracts.DeviceConfig{
		Name: "D1",
		Telemetry: contracts.TelemetryConfig{
			Points: []contracts.PointConfig{{Name: "batch_no", ValueType: "String"}},
			Groups: []contracts.TelemetryGroup{{Name: "state", Points: []contracts.PointConfig{{Name: "mode", ValueType: "String"}}}},
		},
		Property: contracts.PropertyConfig{Points: []contracts.PointConfig{{Name: "operator", ValueType: "String"}}},
	}
	profile := EventProfileFile{
		Name: "device-events",
		Type: "EVENT",
		Config: EventConfig{Categories: map[string]CategoryConfig{
			"alarm": {StateModel: "aggregate", Events: []EventRule{{
				EventCode: "ALARM", Name: "alarm", EventType: EventTypeRiseClear,
				When:    "data.alarm_code != 0 || property.operator == 'admin'",
				Recover: "data.alarm_code == 0",
				Payload: PayloadSelector{Groups: []string{"state"}, Points: []string{"batch_no"}},
			}}},
		}},
	}
	if err := ValidateForDevice(profile, device); err != nil {
		t.Fatalf("ValidateForDevice() error = %v", err)
	}

	profile.Config.Categories["alarm"] = CategoryConfig{Events: []EventRule{{
		EventCode: "BAD", Name: "bad", EventType: EventTypePulse,
		Payload: PayloadSelector{Points: []string{"mode"}},
	}}}
	if err := ValidateForDevice(profile, device); err == nil || !strings.Contains(err.Error(), "belongs to a telemetry group") {
		t.Fatalf("expected grouped point validation error, got %v", err)
	}
}

func TestValidateReportRejectsInconsistentSummarySettings(t *testing.T) {
	profile := EventProfileFile{
		Name: "invalid-report",
		Type: "EVENT",
		Config: EventConfig{Categories: map[string]CategoryConfig{
			"alarm": {Events: []EventRule{{
				EventCode: "PULSE", Name: "pulse", EventType: EventTypePulse,
				Report: ReportConfig{Mode: ReportModeSummary, Interval: "1h"},
			}}},
		}},
	}
	if err := ValidateProfile(profile); err == nil {
		t.Fatal("expected pulse summary validation error")
	}
}
