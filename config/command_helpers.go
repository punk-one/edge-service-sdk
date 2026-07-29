package config

import (
	"fmt"
	"strconv"
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

// BuildTelemetryStructReadRequests generates read requests for all auto-report telemetry structs.
func BuildTelemetryStructReadRequests(structs []contracts.PropertyStruct) ([]contracts.CommandRequest, []PropertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []PropertyBinding

	for _, structDef := range structs {
		if !structDef.AutoReport {
			continue
		}

		effectiveKind := structDef.Kind
		if effectiveKind == "struct_array" {
			effectiveKind = "array"
		}

		switch effectiveKind {
		case "struct":
			if len(structDef.Fields) == 0 {
				continue
			}
			subReqs, subBindings, err := buildStructReadFields(structDef, structDef.Fields, nil, []string{structDef.Name}, 0)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			bindings = append(bindings, subBindings...)

		case "array":
			if structDef.MaxItems <= 0 {
				continue
			}
			for offset := 0; offset < structDef.MaxItems; offset++ {
				index := structIndexBase(structDef) + offset
				indexKey := strconv.Itoa(index)
				indexOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride

				if len(structDef.Fields) == 1 && structDef.Fields[0].IsScalar() {
					field := structDef.Fields[0]
					req, err := buildStructFieldReadWithOffset(structDef, field, indexOffset)
					if err != nil {
						return nil, nil, err
					}
					if structDef.ActualItem != "" {
						if req.Attributes == nil {
							req.Attributes = make(map[string]interface{})
						}
						req.Attributes["actualItem"] = structDef.ActualItem
						req.Attributes["arrayIndex"] = offset
					}
					reqs = append(reqs, req)
					bindings = append(bindings, PropertyBinding{Path: []string{structDef.Name, indexKey}})
				} else {
					subReqs, subBindings, err := buildStructReadFields(structDef, structDef.Fields, nil, []string{structDef.Name, indexKey}, indexOffset)
					if err != nil {
						return nil, nil, err
					}
					if structDef.ActualItem != "" {
						for i := range subReqs {
							if subReqs[i].Attributes == nil {
								subReqs[i].Attributes = make(map[string]interface{})
							}
							subReqs[i].Attributes["actualItem"] = structDef.ActualItem
							subReqs[i].Attributes["arrayIndex"] = offset
						}
					}
					reqs = append(reqs, subReqs...)
					bindings = append(bindings, subBindings...)
				}
			}

		default:
			return nil, nil, fmt.Errorf("unsupported telemetry struct kind %q for %s", structDef.Kind, structDef.Name)
		}
	}

	return reqs, bindings, nil
}
