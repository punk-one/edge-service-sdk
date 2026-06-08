package app

import (
	"strings"
	"testing"
	"time"

	cmdapi "github.com/punk-one/edge-service-sdk/command"
	contracts "github.com/punk-one/edge-service-sdk/driver"
)

type bootstrapTestCommand struct {
	identifier string
}

func (c bootstrapTestCommand) Descriptor() cmdapi.CommandDescriptor {
	return cmdapi.CommandDescriptor{Identifier: c.identifier, Mode: "sync"}
}

func (c bootstrapTestCommand) Execute(ctx cmdapi.CommandContext, req cmdapi.CommandRequest) (map[string]interface{}, *cmdapi.CommandError) {
	return map[string]interface{}{}, nil
}

func TestValidateCommandBindingsRejectsMissingRegistryCommand(t *testing.T) {
	registry := cmdapi.NewRegistry()
	registry.MustRegister(bootstrapTestCommand{identifier: "start_machine"})

	err := validateCommandBindings([]contracts.DeviceConfig{{
		Name:        "qhl0001",
		ProfileName: "qhl-profile",
		Commands:    []contracts.CommandConfig{{Identifier: "program_install"}},
	}}, registry)
	if err == nil {
		t.Fatal("expected error for missing registered command")
	}
	if !strings.Contains(err.Error(), "program_install") {
		t.Fatalf("error = %v, want program_install", err)
	}
}

func TestValidateCommandBindingsRejectsDuplicateIdentifiers(t *testing.T) {
	registry := cmdapi.NewRegistry()
	registry.MustRegister(bootstrapTestCommand{identifier: "start_machine"})

	err := validateCommandBindings([]contracts.DeviceConfig{{
		Name:        "qhl0001",
		ProfileName: "qhl-profile",
		Commands: []contracts.CommandConfig{
			{Identifier: "start_machine"},
			{Identifier: "start_machine"},
		},
	}}, registry)
	if err == nil {
		t.Fatal("expected duplicate command error")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("error = %v, want duplicate text", err)
	}
}

func TestValidateCommandBindingsAcceptsUnusedRegisteredCommands(t *testing.T) {
	registry := cmdapi.NewRegistry()
	registry.MustRegister(bootstrapTestCommand{identifier: "start_machine"})
	registry.MustRegister(bootstrapTestCommand{identifier: "program_install"})

	if err := validateCommandBindings([]contracts.DeviceConfig{{
		Name:        "qhl0001",
		ProfileName: "qhl-profile",
		Commands:    []contracts.CommandConfig{{Identifier: "start_machine"}},
	}}, registry); err != nil {
		t.Fatalf("validateCommandBindings() error = %v", err)
	}
}

func TestComputeGCD_SingleInterval(t *testing.T) {
	gcd, err := computeGCD(contracts.TelemetryConfig{Interval: "10s"})
	if err != nil {
		t.Fatalf("computeGCD() error = %v", err)
	}
	if gcd != 10*time.Second {
		t.Fatalf("computeGCD() = %v, want 10s", gcd)
	}
}

func TestComputeGCD_MultipleIntervals(t *testing.T) {
	gcd, err := computeGCD(contracts.TelemetryConfig{
		Interval: "20s",
		Groups: []contracts.TelemetryGroup{
			{Name: "g1", Interval: "30s"},
			{Name: "g2", Interval: "10s"},
		},
	})
	if err != nil {
		t.Fatalf("computeGCD() error = %v", err)
	}
	if gcd != 10*time.Second {
		t.Fatalf("computeGCD() = %v, want 10s", gcd)
	}
}

func TestComputeGCD_GroupsOnly(t *testing.T) {
	gcd, err := computeGCD(contracts.TelemetryConfig{
		Groups: []contracts.TelemetryGroup{
			{Name: "g1", Interval: "60s"},
			{Name: "g2", Interval: "45s"},
		},
	})
	if err != nil {
		t.Fatalf("computeGCD() error = %v", err)
	}
	if gcd != 15*time.Second {
		t.Fatalf("computeGCD() = %v, want 15s", gcd)
	}
}

func TestComputeGCD_GroupFallsBackToDevice(t *testing.T) {
	gcd, err := computeGCD(contracts.TelemetryConfig{
		Interval: "30s",
		Groups: []contracts.TelemetryGroup{
			{Name: "g1"}, // no interval, falls back to device's 30s
		},
	})
	if err != nil {
		t.Fatalf("computeGCD() error = %v", err)
	}
	if gcd != 30*time.Second {
		t.Fatalf("computeGCD() = %v, want 30s", gcd)
	}
}

func TestComputeGCD_NoIntervals(t *testing.T) {
	gcd, err := computeGCD(contracts.TelemetryConfig{})
	if err != nil {
		t.Fatalf("computeGCD() error = %v", err)
	}
	if gcd != 20*time.Second {
		t.Fatalf("computeGCD() = %v, want 20s (default)", gcd)
	}
}

func TestComputeGCD_InvalidInterval(t *testing.T) {
	_, err := computeGCD(contracts.TelemetryConfig{Interval: "invalid"})
	if err == nil {
		t.Fatal("computeGCD() expected error for invalid interval")
	}
}

func TestIsDue_FirstTick(t *testing.T) {
	if !isDue("10s", 5*time.Second, 0) {
		t.Fatal("isDue() should be true at tick 0")
	}
}

func TestIsDue_ExactMultiple(t *testing.T) {
	// GCD = 5s, interval = 20s: emit at ticks 0, 4, 8, ...
	if !isDue("20s", 5*time.Second, 4) {
		t.Fatal("isDue() should be true at tick 4 for 20s interval with 5s GCD")
	}
	if isDue("20s", 5*time.Second, 2) {
		t.Fatal("isDue() should be false at tick 2 for 20s interval with 5s GCD")
	}
}

func TestIsDue_SameAsGCD(t *testing.T) {
	// GCD = interval: emit every tick
	for tick := 0; tick < 5; tick++ {
		if !isDue("5s", 5*time.Second, tick) {
			t.Fatalf("isDue() should be true at tick %d when interval equals GCD", tick)
		}
	}
}

func TestFilterValuesByNames_Empty(t *testing.T) {
	result := filterValuesByNames(nil, nil)
	if len(result) != 0 {
		t.Fatalf("filterValuesByNames() = %v, want empty", result)
	}

	result = filterValuesByNames([]*contracts.CommandValue{}, map[string]bool{})
	if len(result) != 0 {
		t.Fatalf("filterValuesByNames() = %v, want empty", result)
	}
}

func TestFilterValuesByNames_FiltersCorrectly(t *testing.T) {
	values := []*contracts.CommandValue{
		{DeviceResourceName: "temp", Value: 25.0},
		{DeviceResourceName: "humidity", Value: 60.0},
		{DeviceResourceName: "pressure", Value: 1013.0},
	}
	names := map[string]bool{"temp": true, "pressure": true}

	result := filterValuesByNames(values, names)
	if len(result) != 2 {
		t.Fatalf("filterValuesByNames() = %d results, want 2", len(result))
	}
	if result[0].DeviceResourceName != "temp" {
		t.Fatalf("first result = %s, want temp", result[0].DeviceResourceName)
	}
	if result[1].DeviceResourceName != "pressure" {
		t.Fatalf("second result = %s, want pressure", result[1].DeviceResourceName)
	}
}
