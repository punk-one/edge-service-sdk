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

func TestIsDueWallClock_FirstTick(t *testing.T) {
	if !isDueWallClock("20s", 1*time.Second, 0, true) {
		t.Fatal("isDueWallClock should return true when isFirstTick=true")
	}
	if !isDueWallClock("5s", 1*time.Second, 0, true) {
		t.Fatal("isDueWallClock should return true when isFirstTick=true")
	}
	if !isDueWallClock("60s", 5*time.Second, 0, true) {
		t.Fatal("isDueWallClock should return true when isFirstTick=true")
	}
}

func TestIsDueWallClock_BoundaryCrossed(t *testing.T) {
	// interval=5s, gcd=1s
	// At elapsed=5s: currentSlot=5/5=1, prevSlot=4/5=0 → crossed → due
	if !isDueWallClock("5s", 1*time.Second, 5*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=5s should detect boundary for 5s interval")
	}
	// At elapsed=6s: currentSlot=6/5=1, prevSlot=5/5=1 → same → not due
	if isDueWallClock("5s", 1*time.Second, 6*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=6s should NOT be due for 5s interval")
	}
	// At elapsed=10s: currentSlot=10/5=2, prevSlot=9/5=1 → crossed → due
	if !isDueWallClock("5s", 1*time.Second, 10*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=10s should detect boundary for 5s interval")
	}
}

func TestIsDueWallClock_20sIntervalWith1sGCD(t *testing.T) {
	// interval=20s, gcd=1s
	// elapsed 1..19s: currentSlot = 0, prevSlot = 0 → not due
	for e := 1; e < 20; e++ {
		if isDueWallClock("20s", 1*time.Second, time.Duration(e)*time.Second, false) {
			t.Fatalf("isDueWallClock at elapsed=%ds should NOT be due for 20s interval", e)
		}
	}
	// elapsed=20s: currentSlot=20/20=1, prevSlot=19/20=0 → crossed → due
	if !isDueWallClock("20s", 1*time.Second, 20*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=20s should be due for 20s interval")
	}
}

func TestIsDueWallClock_NoDrift(t *testing.T) {
	// Simulate drift: if elapsed=54s with 1s GCD (tickCount only reached 54 in 60 wall seconds)
	// 20s interval: currentSlot=54/20=2, prevSlot=53/20=2 → same → not due
	// With wall clock: at wall elapsed=60s, currentSlot=60/20=3 → due
	// This verifies that even if tickCount drifts, wall clock still correctly fires
	if isDueWallClock("20s", 1*time.Second, 54*time.Second, false) {
		t.Fatal("isDueWallClock at drifted elapsed=54s should NOT be due for 20s interval (still in slot 2)")
	}
	if !isDueWallClock("20s", 1*time.Second, 60*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=60s should be due for 20s interval (crossed into slot 3)")
	}
	// At the correct boundary (elapsed=40s): currentSlot=40/20=2, prevSlot=39/20=1 → due
	if !isDueWallClock("20s", 1*time.Second, 40*time.Second, false) {
		t.Fatal("isDueWallClock at elapsed=40s should be due for 20s interval (crossed into slot 2)")
	}
}

func TestIsDueWallClock_InvalidInterval(t *testing.T) {
	if isDueWallClock("invalid", 1*time.Second, 5*time.Second, false) {
		t.Fatal("isDueWallClock should return false for invalid interval")
	}
	if isDueWallClock("", 1*time.Second, 5*time.Second, false) {
		t.Fatal("isDueWallClock should return false for empty interval")
	}
}
