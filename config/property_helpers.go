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

type propertyBinding struct {
	Path []string
}

func parsePropertyRequest(payload []byte) (rtapi.PropertyRequest, error) {
	var req rtapi.PropertyRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return req, err
	}
	if req.Data == nil {
		req.Data = make(map[string]interface{})
	}
	return req, nil
}

func protocolPropertiesFromConfig(device contracts.DeviceConfig) map[string]contracts.ProtocolProperties {
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

func buildTelemetryRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, error) {
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

func buildPropertyWriteRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []*contracts.CommandValue, error) {
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

		structDef, ok := findPropertyStruct(device, key)
		if !ok {
			return nil, nil, fmt.Errorf("unknown property key %q", key)
		}

		items, ok := raw.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("property struct %q expects object payload", key)
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

func buildPropertyReadRequests(device contracts.DeviceConfig, data map[string]interface{}) ([]contracts.CommandRequest, []propertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []propertyBinding

	for _, key := range sortedKeys(data) {
		raw := data[key]
		if req, binding, ok, err := resolvePropertyPointRead(device, key, raw); err != nil {
			return nil, nil, err
		} else if ok {
			reqs = append(reqs, req)
			bindings = append(bindings, binding)
			continue
		}

		structDef, ok := findPropertyStruct(device, key)
		if !ok {
			return nil, nil, fmt.Errorf("unknown property key %q", key)
		}

		items, ok := raw.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("property struct %q expects object payload", key)
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

func buildPropertyResponse(values []*contracts.CommandValue, bindings []propertyBinding) map[string]interface{} {
	result := make(map[string]interface{})
	for i, binding := range bindings {
		if i >= len(values) {
			break
		}
		setNestedValue(result, binding.Path, values[i].Value)
	}
	return result
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

func resolvePropertyPointRead(device contracts.DeviceConfig, key string, raw interface{}) (contracts.CommandRequest, propertyBinding, bool, error) {
	for _, point := range device.Property.Points {
		nodeName, resolvedName, ok, err := resolvePointNodeName(point, key)
		if err != nil {
			return contracts.CommandRequest{}, propertyBinding{}, false, err
		}
		if !ok {
			continue
		}

		if !readSelectionEnabled(raw) {
			return contracts.CommandRequest{}, propertyBinding{}, false, fmt.Errorf("property %q read selector must be true or empty object", key)
		}

		req, err := point.ToCommandRequest(nodeName)
		if err != nil {
			return contracts.CommandRequest{}, propertyBinding{}, false, err
		}
		req.DeviceResourceName = resolvedName
		return req, propertyBinding{Path: []string{resolvedName}}, true, nil
	}
	return contracts.CommandRequest{}, propertyBinding{}, false, nil
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
	var reqs []contracts.CommandRequest
	var params []*contracts.CommandValue

	for _, indexKey := range sortedStructIndexKeys(items) {
		rawFields := items[indexKey]
		index, err := parseStructIndex(structDef, indexKey)
		if err != nil {
			return nil, nil, err
		}

		fields, ok := rawFields.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("struct %s[%s] expects object payload", structDef.Name, indexKey)
		}

		for _, fieldName := range sortedKeys(fields) {
			raw := fields[fieldName]
			field, ok := findStructField(structDef, fieldName)
			if !ok {
				return nil, nil, fmt.Errorf("unknown struct field %q on %s", fieldName, structDef.Name)
			}
			req, value, err := buildStructFieldWrite(structDef, field, index, indexKey, raw)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			params = append(params, value)
		}
	}

	return reqs, params, nil
}

func buildStructReadRequests(structDef contracts.PropertyStruct, items map[string]interface{}) ([]contracts.CommandRequest, []propertyBinding, error) {
	var reqs []contracts.CommandRequest
	var bindings []propertyBinding

	for _, indexKey := range sortedStructIndexKeys(items) {
		rawFields := items[indexKey]
		index, err := parseStructIndex(structDef, indexKey)
		if err != nil {
			return nil, nil, err
		}

		fields, ok := rawFields.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("struct %s[%s] expects object payload", structDef.Name, indexKey)
		}

		if len(fields) == 0 {
			for _, field := range structDef.Fields {
				req, binding, err := buildStructFieldRead(structDef, field, index, indexKey)
				if err != nil {
					return nil, nil, err
				}
				reqs = append(reqs, req)
				bindings = append(bindings, binding)
			}
			continue
		}

		for _, fieldName := range sortedKeys(fields) {
			selector := fields[fieldName]
			if !readSelectionEnabled(selector) {
				return nil, nil, fmt.Errorf("struct field selector %s.%s must be true or empty object", structDef.Name, fieldName)
			}
			field, ok := findStructField(structDef, fieldName)
			if !ok {
				return nil, nil, fmt.Errorf("unknown struct field %q on %s", fieldName, structDef.Name)
			}
			req, binding, err := buildStructFieldRead(structDef, field, index, indexKey)
			if err != nil {
				return nil, nil, err
			}
			reqs = append(reqs, req)
			bindings = append(bindings, binding)
		}
	}

	return reqs, bindings, nil
}

func buildStructFieldWrite(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int, indexKey string, raw interface{}) (contracts.CommandRequest, *contracts.CommandValue, error) {
	nodeName, err := structFieldNodeName(structDef, field, index)
	if err != nil {
		return contracts.CommandRequest{}, nil, err
	}

	point := contracts.PointConfig{
		Name:      fmt.Sprintf("%s.%s.%s", structDef.Name, indexKey, field.Name),
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

func buildStructFieldRead(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int, indexKey string) (contracts.CommandRequest, propertyBinding, error) {
	nodeName, err := structFieldNodeName(structDef, field, index)
	if err != nil {
		return contracts.CommandRequest{}, propertyBinding{}, err
	}

	point := contracts.PointConfig{
		Name:      fmt.Sprintf("%s.%s.%s", structDef.Name, indexKey, field.Name),
		ValueType: field.ValueType,
		MaxLength: field.MaxLength,
		ReadWrite: field.ReadWrite,
	}

	req, err := point.ToCommandRequest(nodeName)
	if err != nil {
		return contracts.CommandRequest{}, propertyBinding{}, err
	}
	return req, propertyBinding{Path: []string{structDef.Name, indexKey, field.Name}}, nil
}

func structFieldNodeName(structDef contracts.PropertyStruct, field contracts.PropertyStructField, index int) (string, error) {
	if structDef.Address.DBNumber < 0 {
		return "", fmt.Errorf("struct %s missing dbNumber", structDef.Name)
	}
	baseOffset := structDef.Address.BaseOffset + (index-structIndexBase(structDef))*structDef.Address.IndexStride + field.FieldOffset
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
	for _, field := range structDef.Fields {
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
