package config

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	rtapi "github.com/punk-one/edge-service-sdk/property"

	"github.com/spf13/cast"
)

type PropertyBinding struct {
	Path []string
}

type propertyBinding = PropertyBinding

// leafField represents a flattened leaf field with its accumulated path and offset.
type leafField struct {
	Field        contracts.PropertyStructField
	PathPrefix   []string // path segments from root to this leaf
	OffsetPrefix int      // cumulative byte offset from root
}

func ParsePropertyRequest(payload []byte) (rtapi.PropertyRequest, error) {
	var req rtapi.PropertyRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return req, err
	}
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}
	return req, nil
}

// flattenStructFields recursively expands nested struct/array fields into leaf fields.
// arrayDepth tracks the current nesting level of arrays; exceeding 2 returns an error.
func flattenStructFields(
	fields []contracts.PropertyStructField,
	pathPrefix []string,
	offsetPrefix int,
	arrayDepth int,
) ([]leafField, error) {
	var leaves []leafField

	for _, field := range fields {
		switch {
		case field.IsScalar():
			leaves = append(leaves, leafField{
				Field:        field,
				PathPrefix:   append(append([]string(nil), pathPrefix...), field.Name),
				OffsetPrefix: offsetPrefix + field.FieldOffset,
			})

		case field.IsStruct():
			sub, err := flattenStructFields(field.Fields,
				append(append([]string(nil), pathPrefix...), field.Name),
				offsetPrefix+field.FieldOffset,
				arrayDepth,
			)
			if err != nil {
				return nil, err
			}
			leaves = append(leaves, sub...)

		case field.IsArray():
			if arrayDepth >= 2 {
				return nil, fmt.Errorf("field %s: array nesting exceeds maximum depth of 2", field.Name)
			}
			if field.MaxItems <= 0 {
				continue
			}
			base := 1 // default index base for nested arrays
			for offset := 0; offset < field.MaxItems; offset++ {
				index := base + offset
				indexKey := strconv.Itoa(index)
				sub, err := flattenStructFields(field.Fields,
					append(append([]string(nil), pathPrefix...), field.Name, indexKey),
					offsetPrefix+field.FieldOffset+offset*field.IndexStride,
					arrayDepth+1,
				)
				if err != nil {
					return nil, err
				}
				leaves = append(leaves, sub...)
			}

		default:
			return nil, fmt.Errorf("field %s: unsupported kind %q", field.Name, field.Kind)
		}
	}

	return leaves, nil
}

func ProtocolPropertiesFromConfig(device contracts.DeviceConfig) map[string]contracts.ProtocolProperties {
	protocols := make(map[string]contracts.ProtocolProperties, len(device.Protocols))
	for name, raw := range device.Protocols {
		switch v := raw.(type) {
		case map[string]interface{}:
			protocols[name] = contracts.ProtocolProperties(v)
		case contracts.ProtocolProperties:
			protocols[name] = v
		}
	}
	return protocols
}

func BuildTelemetryRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, error) {
	reqs := make([]contracts.CommandRequest, 0, len(device.Telemetry.Points))
	for _, point := range device.Telemetry.Points {
		req, err := point.ToCommandRequest(point.NodeName)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}

func BuildPropertyWriteRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
	var reqs []contracts.CommandRequest
	var params []*contracts.CommandValue

	for _, key := range sortedKeys(data) {
		raw := data[key]
		if req, value, ok, err := resolvePropertyPointWrite(device, key, raw); err != nil {
			return nil, nil, err
		} else if ok {
			reqs = append(reqs, req)
			params = append(params, value)
			continue
		}

		structName, indices, parseErr := parseStructNameWithIndices(key)
		if parseErr != nil {
			return nil, nil, parseErr
		}

		structDef, ok := findPropertyStruct(device, structName)
		if !ok {
			return nil, nil, fmt.Errorf("unknown property key %q", key)
		}

		var items map[string]interface{}
		if indices != nil {
			if len(indices) != 1 {
				return nil, nil, fmt.Errorf("property write on %q requires single index, got %d", key, len(indices))
			}
			items = map[string]interface{}{strconv.Itoa(indices[0]): raw}
		} else {
			var ok bool
			items, ok = raw.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("property struct %q expects object payload", key)
			}
		}

		structReqs, structParams, err := buildStructWriteRequests(structDef, items)
		if err != nil {
			return nil, nil, err
		}
		reqs = append(reqs, structReqs...)
		params = append(params, structParams...)
	}

	if len(reqs) == 0 {
		return nil, nil, fmt.Errorf("no writable property fields resolved")
	}

	return reqs, params, nil
}

func BuildPropertyReadRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []PropertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []PropertyBinding

	for _, key := range sortedKeys(data) {
		raw := data[key]
		if req, binding, ok, err := resolvePropertyPointRead(device, key, raw); err != nil {
			return nil, nil, err
		} else if ok {
			reqs = append(reqs, req)
			bindings = append(bindings, binding)
			continue
		}

		var items map[string]interface{}
		structDef, ok := findPropertyStruct(device, key)
		if !ok {
			structName, indices, parseErr := parseStructNameWithIndices(key)
			if parseErr != nil || indices == nil {
				return nil, nil, fmt.Errorf("unknown property key %q", key)
			}
			structDef, ok = findPropertyStruct(device, structName)
			if !ok {
				return nil, nil, fmt.Errorf("unknown property key %q", key)
			}
			items = make(map[string]interface{}, len(indices))
			for _, idx := range indices {
				items[strconv.Itoa(idx)] = raw
			}
		} else {
			items, ok = raw.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("property struct %q expects object payload", key)
			}
		}

		structReqs, structBindings, err := buildStructReadRequests(structDef, items)
		if err != nil {
			return nil, nil, err
		}
		reqs = append(reqs, structReqs...)
		bindings = append(bindings, structBindings...)
	}

	if len(reqs) == 0 {
		return nil, nil, fmt.Errorf("no readable property fields resolved")
	}

	return reqs, bindings, nil
}

func BuildPropertyReadSelection(data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return map[string]interface{}{}
	}

	selection := make(map[string]interface{}, len(data))
	for key, raw := range data {
		selection[key] = buildReadSelectionValue(raw)
	}
	return selection
}

func BuildAutoPropertyReadRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, []PropertyBinding, error) {
	reqs := make([]contracts.CommandRequest, 0, len(device.Property.Points))
	bindings := make([]PropertyBinding, 0, len(device.Property.Points))

	for _, point := range device.Property.Points {
		nodeName := strings.TrimSpace(point.NodeName)
		if nodeName == "" {
			continue
		}
		req, err := point.ToCommandRequest(nodeName)
		if err != nil {
			return nil, nil, err
		}
		req.DeviceResourceName = point.Name
		reqs = append(reqs, req)
		bindings = append(bindings, PropertyBinding{Path: []string{point.Name}})
	}

	for _, structDef := range device.Property.Structs {
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
					reqs = append(reqs, req)
					bindings = append(bindings, PropertyBinding{Path: []string{structDef.Name, indexKey}})
				} else {
					subReqs, subBindings, err := buildStructReadFields(structDef, structDef.Fields, nil, []string{structDef.Name, indexKey}, indexOffset)
					if err != nil {
						return nil, nil, err
					}
					reqs = append(reqs, subReqs...)
					bindings = append(bindings, subBindings...)
				}
			}

		default:
			return nil, nil, fmt.Errorf("unsupported struct kind %q for %s", structDef.Kind, structDef.Name)
		}
	}

	return reqs, bindings, nil
}

func BuildPropertyResponse(values []*contracts.CommandValue, bindings []PropertyBinding) map[string]interface{} {
	result := make(map[string]interface{})
	for i, binding := range bindings {
		if i >= len(values) {
			break
		}
		setNestedValue(result, binding.Path, values[i].Value)
	}
	return result
}

func BuildPropertyReadSelectionFromNames(device contracts.DeviceConfig, properties []string) (map[string]interface{}, error) {
	selection := make(map[string]interface{})
	if len(properties) == 0 {
		for _, point := range device.Property.Points {
			selection[point.Name] = true
		}
		for _, structDef := range device.Property.Structs {
			items, err := buildStructSelection(structDef)
			if err != nil {
				return nil, err
			}
			if len(items) > 0 {
				selection[structDef.Name] = items
			}
		}
		if len(selection) == 0 {
			return nil, fmt.Errorf("no readable property fields resolved")
		}
		return selection, nil
	}

	for _, rawName := range properties {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}

		matchedPoint := false
		for _, point := range device.Property.Points {
			if point.Name == name {
				selection[name] = true
				matchedPoint = true
				break
			}
		}
		if matchedPoint {
			continue
		}

		structName, indices, parseErr := parseStructNameWithIndices(name)
		if parseErr != nil {
			return nil, parseErr
		}

		structDef, ok := findPropertyStruct(device, structName)
		if !ok {
			return nil, fmt.Errorf("unknown property key %q", name)
		}

		var (
			items map[string]interface{}
			err   error
		)
		if indices != nil {
			items, err = buildStructSelectionForIndices(structDef, indices)
		} else {
			items, err = buildStructSelection(structDef)
		}
		if err != nil {
			return nil, err
		}
		selection[structName] = items
	}

	if len(selection) == 0 {
		return nil, fmt.Errorf("no readable property fields resolved")
	}
	return selection, nil
}

func parsePropertyRequest(payload []byte) (rtapi.PropertyRequest, error) {
	return ParsePropertyRequest(payload)
}

func protocolPropertiesFromConfig(device contracts.DeviceConfig) map[string]contracts.ProtocolProperties {
	return ProtocolPropertiesFromConfig(device)
}

func buildTelemetryRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, error) {
	return BuildTelemetryRequests(device)
}

func buildPropertyWriteRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
	return BuildPropertyWriteRequests(device, data)
}

func buildPropertyReadRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []PropertyBinding, error) {
	return BuildPropertyReadRequests(device, data)
}

func buildPropertyReadSelection(data map[string]interface{}) map[string]interface{} {
	return BuildPropertyReadSelection(data)
}

func buildAutoPropertyReadRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, []PropertyBinding, error) {
	return BuildAutoPropertyReadRequests(device)
}

func buildPropertyResponse(values []*contracts.CommandValue, bindings []PropertyBinding) map[string]interface{} {
	return BuildPropertyResponse(values, bindings)
}

func buildPropertyReadSelectionFromNames(device contracts.DeviceConfig, properties []string) (map[string]interface{}, error) {
	return BuildPropertyReadSelectionFromNames(device, properties)
}

func resolvePropertyPointWrite(device contracts.DeviceConfig, key string, raw interface{}) (contracts.CommandRequest, *contracts.CommandValue, bool, error) {
	for _, point := range device.Property.Points {
		nodeName, resolvedName, ok, err := resolvePointNodeName(point, key)
		if err != nil {
			return contracts.CommandRequest{}, nil, false, err
		}
		if !ok {
			continue
		}

		req, err := point.ToCommandRequest(nodeName)
		if err != nil {
			return contracts.CommandRequest{}, nil, false, err
		}
		req.DeviceResourceName = resolvedName
		value, err := commandValueFromRaw(resolvedName, req.Type, raw)
		if err != nil {
			return contracts.CommandRequest{}, nil, false, err
		}
		return req, value, true, nil
	}
	return contracts.CommandRequest{}, nil, false, nil
}

func resolvePropertyPointRead(device contracts.DeviceConfig, key string, raw interface{}) (contracts.CommandRequest, PropertyBinding, bool, error) {
	for _, point := range device.Property.Points {
		nodeName, resolvedName, ok, err := resolvePointNodeName(point, key)
		if err != nil {
			return contracts.CommandRequest{}, PropertyBinding{}, false, err
		}
		if !ok {
			continue
		}

		if !readSelectionEnabled(raw) {
			return contracts.CommandRequest{}, PropertyBinding{}, false, fmt.Errorf("property %q read selector must be true or empty object", key)
		}

		req, err := point.ToCommandRequest(nodeName)
		if err != nil {
			return contracts.CommandRequest{}, PropertyBinding{}, false, err
		}
		req.DeviceResourceName = resolvedName
		return req, PropertyBinding{Path: []string{resolvedName}}, true, nil
	}
	return contracts.CommandRequest{}, PropertyBinding{}, false, nil
}

func resolvePointNodeName(point contracts.PointConfig, key string) (nodeName string, resolvedName string, ok bool, err error) {
	if point.Name == key {
		return point.NodeName, key, true, nil
	}

	if point.NodeNameTemplate == "" {
		return "", "", false, nil
	}

	index, matched, err := matchIndexedKey(point, key)
	if err != nil || !matched {
		return "", "", matched, err
	}

	return strings.ReplaceAll(point.NodeNameTemplate, "{index}", strconv.Itoa(index)), key, true, nil
}

func matchIndexedKey(point contracts.PointConfig, key string) (int, bool, error) {
	pattern := point.ArrayKeyPattern
	if pattern == "" {
		pattern = point.Name + "[{index}]"
	}
	if !strings.Contains(pattern, "{index}") {
		return 0, false, fmt.Errorf("arrayKeyPattern for %s must contain {index}", point.Name)
	}

	re := regexp.QuoteMeta(pattern)
	re = strings.ReplaceAll(re, "\\{index\\}", `(\d+)`)
	matches := regexp.MustCompile("^" + re + "$").FindStringSubmatch(key)
	if len(matches) != 2 {
		return 0, false, nil
	}
	index, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, false, fmt.Errorf("invalid array index in %q", key)
	}
	return index, true, nil
}

func findPropertyStruct(device contracts.DeviceConfig, name string) (contracts.PropertyStruct, bool) {
	for _, item := range device.Property.Structs {
		if item.Name == name {
			return item, true
		}
	}
	return contracts.PropertyStruct{}, false
}

func buildStructWriteRequests(structDef contracts.PropertyStruct, items map[string]interface{}) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
	effectiveKind := structDef.Kind
	if effectiveKind == "struct_array" {
		effectiveKind = "array"
	}

	switch effectiveKind {
	case "struct":
		return buildStructWriteFields(structDef, structDef.Fields, items, 0)

	case "array":
		var reqs []contracts.CommandRequest
		var params []*contracts.CommandValue

		for _, indexKey := range sortedStructIndexKeys(items) {
			rawFields := items[indexKey]
			index, err := parseStructIndex(structDef, indexKey)
			if err != nil {
				return nil, nil, err
			}
			indexOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride

			// Determine if this is a scalar array (single scalar field) or struct array (multi-field).
			if len(structDef.Fields) == 1 && structDef.Fields[0].IsScalar() {
				field := structDef.Fields[0]
				req, value, err := buildStructFieldWriteWithOffset(structDef, field, indexKey, indexOffset, rawFields)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, req)
				params = append(params, value)
			} else {
				fields, ok := rawFields.(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("struct %s[%s] expects object payload", structDef.Name, indexKey)
				}
				subReqs, subParams, err := buildStructWriteFields(structDef, structDef.Fields, fields, indexOffset)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, subReqs...)
				params = append(params, subParams...)
			}
		}

		return reqs, params, nil

	default:
		return nil, nil, fmt.Errorf("unsupported struct kind %q for %s", structDef.Kind, structDef.Name)
	}
}

// buildStructWriteFields processes a map of field-name -> value for write, recursively
// handling nested struct and array fields. cumulativeOffset is the byte offset from ancestors.
func buildStructWriteFields(structDef contracts.PropertyStruct, fields []contracts.PropertyStructField, input map[string]interface{}, cumulativeOffset int) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
	var reqs []contracts.CommandRequest
	var params []*contracts.CommandValue

	for _, fieldName := range sortedKeys(input) {
		raw := input[fieldName]
		field, ok := findStructFieldByList(fields, fieldName)
		if !ok {
			return nil, nil, fmt.Errorf("unknown struct field %q on %s", fieldName, structDef.Name)
		}

		switch {
		case field.IsScalar():
			req, value, err := buildStructFieldWriteWithOffset(structDef, field, fieldName, cumulativeOffset, raw)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			params = append(params, value)

		case field.IsStruct():
			subInput, ok := raw.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("struct field %s.%s expects object payload", structDef.Name, fieldName)
			}
			subReqs, subParams, err := buildStructWriteFields(structDef, field.Fields, subInput, cumulativeOffset+field.FieldOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			params = append(params, subParams...)

		case field.IsArray():
			subInput, ok := raw.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("array field %s.%s expects object payload with index keys", structDef.Name, fieldName)
			}
			subReqs, subParams, err := buildNestedArrayWrite(structDef, field, subInput, cumulativeOffset+field.FieldOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			params = append(params, subParams...)

		default:
			return nil, nil, fmt.Errorf("field %s.%s: unsupported kind %q", structDef.Name, fieldName, field.Kind)
		}
	}

	return reqs, params, nil
}

// buildNestedArrayWrite handles writes to a nested array field (input-driven).
func buildNestedArrayWrite(structDef contracts.PropertyStruct, field contracts.PropertyStructField, input map[string]interface{}, cumulativeOffset int) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
	var reqs []contracts.CommandRequest
	var params []*contracts.CommandValue

	for _, indexKey := range sortedStructIndexKeys(input) {
		raw := input[indexKey]
		index, err := strconv.Atoi(indexKey)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid nested array index %q on %s.%s", indexKey, structDef.Name, field.Name)
		}
		indexOffset := cumulativeOffset + index*field.IndexStride

		if len(field.Fields) == 1 && field.Fields[0].IsScalar() {
			// scalar element
			subField := field.Fields[0]
			req, value, err := buildStructFieldWriteWithOffset(structDef, subField, indexKey, indexOffset, raw)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			params = append(params, value)
		} else {
			// struct element
			subInput, ok := raw.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("nested array %s.%s[%s] expects object payload", structDef.Name, field.Name, indexKey)
			}
			subReqs, subParams, err := buildStructWriteFields(structDef, field.Fields, subInput, indexOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			params = append(params, subParams...)
		}
	}

	return reqs, params, nil
}

func buildStructReadRequests(structDef contracts.PropertyStruct, items map[string]interface{}) ([]contracts.CommandRequest, []PropertyBinding, error) {
	effectiveKind := structDef.Kind
	if effectiveKind == "struct_array" {
		effectiveKind = "array"
	}

	switch effectiveKind {
	case "struct":
		// items maps field names to selections
		return buildStructReadFields(structDef, structDef.Fields, items, []string{structDef.Name}, 0)

	case "array":
		var reqs []contracts.CommandRequest
		var bindings []PropertyBinding

		for _, indexKey := range sortedStructIndexKeys(items) {
			rawFields := items[indexKey]
			index, err := parseStructIndex(structDef, indexKey)
			if err != nil {
				return nil, nil, err
			}
			indexOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride

			// Scalar array: single scalar field
			if len(structDef.Fields) == 1 && structDef.Fields[0].IsScalar() {
				if !readSelectionEnabled(rawFields) {
					return nil, nil, fmt.Errorf("struct %s[%s] read selector must be true or empty object", structDef.Name, indexKey)
				}
				field := structDef.Fields[0]
				req, err := buildStructFieldReadWithOffset(structDef, field, indexOffset)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, req)
				bindings = append(bindings, PropertyBinding{Path: []string{structDef.Name, indexKey}})
				continue
			}

			// Struct array
			fields, ok := rawFields.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("struct %s[%s] expects object payload", structDef.Name, indexKey)
			}

			// Empty selection = all fields
			if len(fields) == 0 {
				for _, field := range structDef.Fields {
					if field.IsScalar() {
						req, err := buildStructFieldReadWithOffset(structDef, field, indexOffset)
						if err != nil {
							return nil, nil, err
						}
						reqs = append(reqs, req)
						bindings = append(bindings, PropertyBinding{Path: []string{structDef.Name, indexKey, field.Name}})
					} else {
						subReqs, subBindings, err := buildStructReadFields(structDef, field.Fields, nil, []string{structDef.Name, indexKey, field.Name}, indexOffset+field.FieldOffset)
						if err != nil {
							return nil, nil, err
						}
						reqs = append(reqs, subReqs...)
						bindings = append(bindings, subBindings...)
					}
				}
				continue
			}

			// Specific field selections
			for _, fieldName := range sortedKeys(fields) {
				selector := fields[fieldName]
				field, ok := findStructFieldByList(structDef.Fields, fieldName)
				if !ok {
					return nil, nil, fmt.Errorf("unknown struct field %q on %s", fieldName, structDef.Name)
				}

				if field.IsScalar() {
					if !readSelectionEnabled(selector) {
						return nil, nil, fmt.Errorf("struct field selector %s.%s must be true or empty object", structDef.Name, fieldName)
					}
					req, err := buildStructFieldReadWithOffset(structDef, field, indexOffset)
					if err != nil {
						return nil, nil, err
					}
					reqs = append(reqs, req)
					bindings = append(bindings, PropertyBinding{Path: []string{structDef.Name, indexKey, fieldName}})
				} else {
					subSelection, ok := selector.(map[string]interface{})
					if !ok {
						return nil, nil, fmt.Errorf("struct field selector %s.%s must be an object", structDef.Name, fieldName)
					}
					subReqs, subBindings, err := buildStructReadFields(structDef, field.Fields, subSelection, []string{structDef.Name, indexKey, fieldName}, indexOffset+field.FieldOffset)
					if err != nil {
						return nil, nil, err
					}
					reqs = append(reqs, subReqs...)
					bindings = append(bindings, subBindings...)
				}
			}
		}

		return reqs, bindings, nil

	default:
		return nil, nil, fmt.Errorf("unsupported struct kind %q for %s", structDef.Kind, structDef.Name)
	}
}

// buildStructReadFields processes read requests for a set of fields (config-driven).
// cumulativeOffset is the byte offset from ancestors.
// If selection is nil or empty, all scalar sub-fields are expanded.
func buildStructReadFields(structDef contracts.PropertyStruct, fields []contracts.PropertyStructField, selection map[string]interface{}, pathPrefix []string, cumulativeOffset int) ([]contracts.CommandRequest, []PropertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []PropertyBinding

	// Empty/nil selection = read all fields
	if len(selection) == 0 {
		for _, field := range fields {
			switch {
			case field.IsScalar():
				req, err := buildStructFieldReadWithOffset(structDef, field, cumulativeOffset)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, req)
				bindings = append(bindings, PropertyBinding{Path: append(append([]string(nil), pathPrefix...), field.Name)})

			case field.IsStruct():
				subReqs, subBindings, err := buildStructReadFields(structDef, field.Fields, nil,
					append(append([]string(nil), pathPrefix...), field.Name),
					cumulativeOffset+field.FieldOffset)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, subReqs...)
				bindings = append(bindings, subBindings...)

			case field.IsArray():
				subReqs, subBindings, err := buildNestedArrayRead(structDef, field, nil,
					append(append([]string(nil), pathPrefix...), field.Name),
					cumulativeOffset+field.FieldOffset)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, subReqs...)
				bindings = append(bindings, subBindings...)
			}
		}
		return reqs, bindings, nil
	}

	// Specific field selections
	for _, fieldName := range sortedKeys(selection) {
		selector := selection[fieldName]
		field, ok := findStructFieldByList(fields, fieldName)
		if !ok {
			return nil, nil, fmt.Errorf("unknown struct field %q on %s", fieldName, structDef.Name)
		}

		switch {
		case field.IsScalar():
			if !readSelectionEnabled(selector) {
				return nil, nil, fmt.Errorf("struct field selector %s.%s must be true or empty object", structDef.Name, fieldName)
			}
			req, err := buildStructFieldReadWithOffset(structDef, field, cumulativeOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			bindings = append(bindings, PropertyBinding{Path: append(append([]string(nil), pathPrefix...), fieldName)})

		case field.IsStruct():
			subSelection, ok := selector.(map[string]interface{})
			if !ok && selector != nil {
				return nil, nil, fmt.Errorf("struct field selector %s.%s must be an object", structDef.Name, fieldName)
			}
			if !ok {
				subSelection = nil
			}
			subReqs, subBindings, err := buildStructReadFields(structDef, field.Fields, subSelection,
				append(append([]string(nil), pathPrefix...), fieldName),
				cumulativeOffset+field.FieldOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			bindings = append(bindings, subBindings...)

		case field.IsArray():
			subSelection, ok := selector.(map[string]interface{})
			if !ok && selector != nil {
				return nil, nil, fmt.Errorf("array field selector %s.%s must be an object", structDef.Name, fieldName)
			}
			if !ok {
				subSelection = nil
			}
			subReqs, subBindings, err := buildNestedArrayRead(structDef, field, subSelection,
				append(append([]string(nil), pathPrefix...), fieldName),
				cumulativeOffset+field.FieldOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			bindings = append(bindings, subBindings...)
		}
	}

	return reqs, bindings, nil
}

// buildNestedArrayRead handles reads for a nested array field (config-driven).
func buildNestedArrayRead(structDef contracts.PropertyStruct, field contracts.PropertyStructField, selection map[string]interface{}, pathPrefix []string, cumulativeOffset int) ([]contracts.CommandRequest, []PropertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []PropertyBinding

	// Determine which indices to read
	var indices []string
	if len(selection) > 0 {
		indices = sortedStructIndexKeys(selection)
	} else {
		for offset := 0; offset < field.MaxItems; offset++ {
			indices = append(indices, strconv.Itoa(1+offset))
		}
	}

	scalarElem := len(field.Fields) == 1 && field.Fields[0].IsScalar()

	for _, indexKey := range indices {
		index, err := strconv.Atoi(indexKey)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid nested array index %q on %s", indexKey, structDef.Name)
		}
		elemOffset := cumulativeOffset + index*field.IndexStride

		if scalarElem {
			subField := field.Fields[0]
			if len(selection) > 0 {
				if !readSelectionEnabled(selection[indexKey]) {
					return nil, nil, fmt.Errorf("nested array %s[%s] read selector must be true or empty object", field.Name, indexKey)
				}
			}
			req, err := buildStructFieldReadWithOffset(structDef, subField, elemOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			bindings = append(bindings, PropertyBinding{Path: append(append([]string(nil), pathPrefix...), indexKey)})
		} else {
			var subSelection map[string]interface{}
			if len(selection) > 0 {
				var ok bool
				subSelection, ok = selection[indexKey].(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("nested array %s[%s] selector must be an object", field.Name, indexKey)
				}
			}
			subReqs, subBindings, err := buildStructReadFields(structDef, field.Fields, subSelection,
				append(append([]string(nil), pathPrefix...), indexKey),
				elemOffset)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, subReqs...)
			bindings = append(bindings, subBindings...)
		}
	}

	return reqs, bindings, nil
}

func buildStructFieldWriteWithOffset(structDef contracts.PropertyStruct, field contracts.PropertyStructField, leafName string, cumulativeOffset int, raw interface{}) (contracts.CommandRequest, *contracts.CommandValue, error) {
	nodeName, err := structFieldNodeNameWithOffset(structDef, field, cumulativeOffset)
	if err != nil {
		return contracts.CommandRequest{}, nil, err
	}

	point := contracts.PointConfig{
		Name:      fmt.Sprintf("%s.%s", structDef.Name, leafName),
		ValueType: field.ValueType,
		MaxLength: field.MaxLength,
		ReadWrite: field.ReadWrite,
	}

	req, err := point.ToCommandRequest(nodeName)
	if err != nil {
		return contracts.CommandRequest{}, nil, err
	}
	value, err := commandValueFromRaw(req.DeviceResourceName, req.Type, raw)
	if err != nil {
		return contracts.CommandRequest{}, nil, err
	}
	return req, value, nil
}

func buildStructFieldReadWithOffset(structDef contracts.PropertyStruct, field contracts.PropertyStructField, cumulativeOffset int) (contracts.CommandRequest, error) {
	nodeName, err := structFieldNodeNameWithOffset(structDef, field, cumulativeOffset)
	if err != nil {
		return contracts.CommandRequest{}, err
	}

	point := contracts.PointConfig{
		Name:      fmt.Sprintf("%s.%s", structDef.Name, field.Name),
		ValueType: field.ValueType,
		MaxLength: field.MaxLength,
		ReadWrite: field.ReadWrite,
	}

	req, err := point.ToCommandRequest(nodeName)
	if err != nil {
		return contracts.CommandRequest{}, err
	}
	return req, nil
}

func buildStructFieldWrite(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int, indexKey string, raw interface{}) (contracts.CommandRequest, *contracts.CommandValue, error) {
	cumulativeOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride
	return buildStructFieldWriteWithOffset(structDef, field, indexKey, cumulativeOffset, raw)
}

func buildStructFieldRead(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int, indexKey string) (contracts.CommandRequest, PropertyBinding, error) {
	cumulativeOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride
	req, err := buildStructFieldReadWithOffset(structDef, field, cumulativeOffset)
	if err != nil {
		return contracts.CommandRequest{}, PropertyBinding{}, err
	}
	return req, PropertyBinding{Path: []string{structDef.Name, indexKey, field.Name}}, nil
}

func structFieldNodeName(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int) (string, error) {
	cumulativeOffset := (index - structIndexBase(structDef)) * structDef.Address.IndexStride
	return structFieldNodeNameWithOffset(structDef, field, cumulativeOffset)
}

func structFieldNodeNameWithOffset(structDef contracts.PropertyStruct, field contracts.PropertyStructField, cumulativeOffset int) (string, error) {
	if structDef.Address.DBNumber < 0 {
		return "", fmt.Errorf("struct %s missing dbNumber", structDef.Name)
	}
	baseOffset := structDef.Address.BaseOffset + cumulativeOffset + field.FieldOffset

	if field.BitOffset != nil {
		bit := *field.BitOffset
		if bit < 0 || bit > 7 {
			return "", fmt.Errorf("bitOffset %d on %s.%s out of range (0-7)", bit, structDef.Name, field.Name)
		}
		return fmt.Sprintf("DB%d.DBX%d.%d", structDef.Address.DBNumber, baseOffset, bit), nil
	}

	switch strings.ToLower(strings.TrimSpace(structDef.Address.Unit)) {
	case "", "word":
		return fmt.Sprintf("DB%d.DBW%d", structDef.Address.DBNumber, baseOffset), nil
	case "byte":
		return fmt.Sprintf("DB%d.DBB%d", structDef.Address.DBNumber, baseOffset), nil
	case "dword":
		return fmt.Sprintf("DB%d.DBD%d", structDef.Address.DBNumber, baseOffset), nil
	default:
		return "", fmt.Errorf("unsupported struct unit %q on %s", structDef.Address.Unit, structDef.Name)
	}
}

func structIndexBase(structDef contracts.PropertyStruct) int {
	if structDef.IndexBase > 0 {
		return structDef.IndexBase
	}
	return 1
}

func parseStructIndex(structDef contracts.PropertyStruct, raw string) (int, error) {
	index, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid struct index %q on %s", raw, structDef.Name)
	}
	base := structIndexBase(structDef)
	if index < base {
		return 0, fmt.Errorf("struct index %d on %s is below base %d", index, structDef.Name, base)
	}
	if structDef.MaxItems > 0 && index >= base+structDef.MaxItems {
		return 0, fmt.Errorf("struct index %d on %s exceeds maxItems %d", index, structDef.Name, structDef.MaxItems)
	}
	return index, nil
}

func findStructField(structDef contracts.PropertyStruct, name string) (contracts.PropertyStructField, bool) {
	return findStructFieldByList(structDef.Fields, name)
}

func findStructFieldByList(fields []contracts.PropertyStructField, name string) (contracts.PropertyStructField, bool) {
	for _, field := range fields {
		if field.Name == name {
			return field, true
		}
	}
	return contracts.PropertyStructField{}, false
}

func commandValueFromRaw(name string, valueType string, raw interface{}) (*contracts.CommandValue, error) {
	switch contracts.NormalizedValueType(valueType) {
	case "Bool":
		value, err := cast.ToBoolE(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Bool", value)
	case "String":
		value, err := cast.ToStringE(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "String", value)
	case "Uint8":
		value, err := cast.ToUint8E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Uint8", value)
	case "Uint16":
		value, err := cast.ToUint16E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Uint16", value)
	case "Uint32":
		value, err := cast.ToUint32E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Uint32", value)
	case "Uint64":
		value, err := cast.ToUint64E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Uint64", value)
	case "Int16":
		value, err := cast.ToInt16E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Int16", value)
	case "Int32":
		value, err := cast.ToInt32E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Int32", value)
	case "Int64":
		value, err := cast.ToInt64E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Int64", value)
	case "Float32":
		value, err := cast.ToFloat32E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Float32", value)
	case "Float64":
		value, err := cast.ToFloat64E(raw)
		if err != nil {
			return nil, err
		}
		return contracts.NewCommandValue(name, "Float64", value)
	default:
		return nil, fmt.Errorf("unsupported value type %q for %s", valueType, name)
	}
}

func readSelectionEnabled(raw interface{}) bool {
	if raw == nil {
		return true
	}
	switch v := raw.(type) {
	case bool:
		return v
	case map[string]interface{}:
		return len(v) == 0
	default:
		return false
	}
}

func setNestedValue(target map[string]interface{}, path []string, value interface{}) {
	if len(path) == 0 {
		return
	}
	current := target
	for i := 0; i < len(path)-1; i++ {
		next, ok := current[path[i]].(map[string]interface{})
		if !ok {
			next = make(map[string]interface{})
			current[path[i]] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
}

func sortedKeys(data map[string]interface{}) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStructIndexKeys(items map[string]interface{}) []string {
	keys := sortedKeys(items)
	sort.Slice(keys, func(i, j int) bool {
		left, leftErr := strconv.Atoi(keys[i])
		right, rightErr := strconv.Atoi(keys[j])
		if leftErr == nil && rightErr == nil {
			return left < right
		}
		return keys[i] < keys[j]
	})
	return keys
}

func buildStructSelection(structDef contracts.PropertyStruct) (map[string]interface{}, error) {
	effectiveKind := structDef.Kind
	if effectiveKind == "struct_array" {
		effectiveKind = "array"
	}

	switch effectiveKind {
	case "struct":
		return buildStructFieldSelection(structDef.Fields), nil

	case "array":
		if structDef.MaxItems <= 0 {
			return nil, fmt.Errorf("property struct %q requires maxItems > 0 for full selection", structDef.Name)
		}
		items := make(map[string]interface{}, structDef.MaxItems)
		base := structIndexBase(structDef)
		for offset := 0; offset < structDef.MaxItems; offset++ {
			if len(structDef.Fields) == 1 && structDef.Fields[0].IsScalar() {
				items[strconv.Itoa(base+offset)] = true
			} else {
				items[strconv.Itoa(base+offset)] = buildStructFieldSelection(structDef.Fields)
			}
		}
		return items, nil

	default:
		return nil, fmt.Errorf("unsupported struct kind %q for %s", structDef.Kind, structDef.Name)
	}
}

// buildStructFieldSelection builds a selection map for nested fields.
func buildStructFieldSelection(fields []contracts.PropertyStructField) map[string]interface{} {
	sel := make(map[string]interface{}, len(fields))
	for _, field := range fields {
		switch {
		case field.IsScalar():
			sel[field.Name] = true
		case field.IsStruct():
			sel[field.Name] = buildStructFieldSelection(field.Fields)
		case field.IsArray():
			if field.MaxItems > 0 {
				items := make(map[string]interface{}, field.MaxItems)
				for offset := 0; offset < field.MaxItems; offset++ {
					if len(field.Fields) == 1 && field.Fields[0].IsScalar() {
						items[strconv.Itoa(1+offset)] = true
					} else {
						items[strconv.Itoa(1+offset)] = buildStructFieldSelection(field.Fields)
					}
				}
				sel[field.Name] = items
			}
		}
	}
	return sel
}

func parseIndexList(inner string) ([]int, error) {
	parts := strings.Split(inner, ",")
	var indices []int
	seen := make(map[int]bool)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			rangeParts := strings.SplitN(part, "-", 2)
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range %q", part)
			}
			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid range start %q", rangeParts[0])
			}
			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid range end %q", rangeParts[1])
			}
			if start > end {
				return nil, fmt.Errorf("invalid range %q: start > end", part)
			}
			for i := start; i <= end; i++ {
				if !seen[i] {
					indices = append(indices, i)
					seen[i] = true
				}
			}
		} else {
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid index %q", part)
			}
			if !seen[n] {
				indices = append(indices, n)
				seen[n] = true
			}
		}
	}
	return indices, nil
}

func deduplicateAndSort(indices []int) []int {
	if len(indices) <= 1 {
		return indices
	}
	unique := make(map[int]bool)
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if !unique[idx] {
			unique[idx] = true
			result = append(result, idx)
		}
	}
	sort.Ints(result)
	return result
}

var structNameIndexPattern = regexp.MustCompile(`^(\w+)\[([^\]]+)\]$`)

func parseStructNameWithIndices(raw string) (structName string, indices []int, err error) {
	if strings.HasSuffix(raw, "[]") {
		return "", nil, fmt.Errorf("empty index spec in %q", raw)
	}
	matches := structNameIndexPattern.FindStringSubmatch(raw)
	if len(matches) == 3 {
		structName = matches[1]
		indices, err = parseIndexList(matches[2])
		if err != nil {
			return "", nil, fmt.Errorf("invalid index spec in %q: %w", raw, err)
		}
		indices = deduplicateAndSort(indices)
		return structName, indices, nil
	}
	return raw, nil, nil
}

func buildStructSelectionForIndices(structDef contracts.PropertyStruct, indices []int) (map[string]interface{}, error) {
	effectiveKind := structDef.Kind
	if effectiveKind == "struct_array" {
		effectiveKind = "array"
	}

	if effectiveKind == "struct" {
		// kind: struct has no indices; return field selection
		return buildStructFieldSelection(structDef.Fields), nil
	}

	if len(indices) == 0 {
		return nil, fmt.Errorf("property struct %q requires at least one index", structDef.Name)
	}
	base := structIndexBase(structDef)
	for _, idx := range indices {
		if idx < base {
			return nil, fmt.Errorf("struct index %d on %s is below base %d", idx, structDef.Name, base)
		}
		if structDef.MaxItems > 0 && idx >= base+structDef.MaxItems {
			return nil, fmt.Errorf("struct index %d on %s exceeds maxItems %d (base=%d)", idx, structDef.Name, structDef.MaxItems, base)
		}
	}
	items := make(map[string]interface{}, len(indices))
	for _, idx := range indices {
		if len(structDef.Fields) == 1 && structDef.Fields[0].IsScalar() {
			items[strconv.Itoa(idx)] = true
		} else {
			items[strconv.Itoa(idx)] = buildStructFieldSelection(structDef.Fields)
		}
	}
	return items, nil
}

func buildReadSelectionValue(raw interface{}) interface{} {
	if nested, ok := raw.(map[string]interface{}); ok {
		if len(nested) == 0 {
			return map[string]interface{}{}
		}
		selection := make(map[string]interface{}, len(nested))
		for key, value := range nested {
			selection[key] = buildReadSelectionValue(value)
		}
		return selection
	}
	return true
}
