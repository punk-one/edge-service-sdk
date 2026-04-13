package config

import (
	"fmt"
	"strings"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

// FindCommandByIdentifier resolves one configured command on the device.
func FindCommandByIdentifier(device contracts.DeviceConfig, identifier string) (contracts.CommandConfig, bool) {
	identifier = strings.TrimSpace(identifier)
	for _, command := range device.Commands {
		if strings.TrimSpace(command.Identifier) == identifier {
			return command, true
		}
	}
	return contracts.CommandConfig{}, false
}

// BuildTelemetryReadRequestsFromNames builds read requests for the selected telemetry points.
func BuildTelemetryReadRequestsFromNames(device contracts.DeviceConfig, names []string) ([]contracts.CommandRequest, []string, error) {
	if len(names) == 0 {
		reqs := make([]contracts.CommandRequest, 0, len(device.Telemetry.Points))
		bindings := make([]string, 0, len(device.Telemetry.Points))
		for _, point := range device.Telemetry.Points {
			req, err := point.ToCommandRequest(point.NodeName)
			if err != nil {
				return nil, nil, err
			}
			req.DeviceResourceName = point.Name
			reqs = append(reqs, req)
			bindings = append(bindings, point.Name)
		}
		if len(reqs) == 0 {
			return nil, nil, fmt.Errorf("no telemetry points configured")
		}
		return reqs, bindings, nil
	}

	reqs := make([]contracts.CommandRequest, 0, len(names))
	bindings := make([]string, 0, len(names))
	for _, rawName := range names {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		matched := false
		for _, point := range device.Telemetry.Points {
			if point.Name != name {
				continue
			}
			req, err := point.ToCommandRequest(point.NodeName)
			if err != nil {
				return nil, nil, err
			}
			req.DeviceResourceName = point.Name
			reqs = append(reqs, req)
			bindings = append(bindings, point.Name)
			matched = true
			break
		}
		if !matched {
			return nil, nil, fmt.Errorf("unknown telemetry point %q", name)
		}
	}
	if len(reqs) == 0 {
		return nil, nil, fmt.Errorf("no telemetry points resolved")
	}
	return reqs, bindings, nil
}
