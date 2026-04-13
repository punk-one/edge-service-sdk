package app

import (
	"strings"
	"testing"

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
