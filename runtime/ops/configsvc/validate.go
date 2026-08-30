package configsvc

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// FieldType describes the expected type of a config field.
type FieldType string

const (
	TypeString   FieldType = "string"
	TypeInt      FieldType = "int"
	TypeFloat    FieldType = "float"
	TypeBool     FieldType = "bool"
	TypeDuration FieldType = "duration"
	TypeAny      FieldType = "any"
)

// FieldRule defines validation constraints for a configuration field.
type FieldRule struct {
	Type      FieldType     // expected type
	Required  bool          // must be non-empty
	Enum      []interface{} // allowed values
	Min       *float64      // minimum value (numeric)
	Max       *float64      // maximum value (numeric)
	Pattern   string        // regex pattern (string)
	MinLength *int          // minimum string length
	MaxLength *int          // maximum string length
}

// Validate validates a value against the given rule.
func (r *FieldRule) Validate(value interface{}) error {
	if value == nil {
		if r.Required {
			return fmt.Errorf("value is required")
		}
		return nil
	}

	switch r.Type {
	case TypeString:
		return r.validateString(value)
	case TypeInt:
		return r.validateInt(value)
	case TypeFloat:
		return r.validateFloat(value)
	case TypeBool:
		return r.validateBool(value)
	case TypeDuration:
		return r.validateDuration(value)
	default:
		return nil
	}
}

func (r *FieldRule) validateString(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}
	if r.Required && strings.TrimSpace(s) == "" {
		return fmt.Errorf("value is required")
	}
	if r.MinLength != nil && len(s) < *r.MinLength {
		return fmt.Errorf("string length %d is less than minimum %d", len(s), *r.MinLength)
	}
	if r.MaxLength != nil && len(s) > *r.MaxLength {
		return fmt.Errorf("string length %d exceeds maximum %d", len(s), *r.MaxLength)
	}
	if r.Pattern != "" {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern %q: %w", r.Pattern, err)
		}
		if !re.MatchString(s) {
			return fmt.Errorf("value %q does not match pattern %q", s, r.Pattern)
		}
	}
	return r.checkEnum(value)
}

func (r *FieldRule) validateInt(value interface{}) error {
	var n float64
	switch v := value.(type) {
	case int:
		n = float64(v)
	case int64:
		n = float64(v)
	case float64:
		if v != float64(int64(v)) {
			return fmt.Errorf("expected integer, got float %v", v)
		}
		n = v
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return fmt.Errorf("expected integer, got string %q", v)
		}
		n = float64(parsed)
	default:
		return fmt.Errorf("expected integer, got %T", value)
	}
	return r.checkNumericBounds(n)
}

func (r *FieldRule) validateFloat(value interface{}) error {
	var n float64
	switch v := value.(type) {
	case int:
		n = float64(v)
	case int64:
		n = float64(v)
	case float64:
		n = v
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return fmt.Errorf("expected float, got string %q", v)
		}
		n = parsed
	default:
		return fmt.Errorf("expected float, got %T", value)
	}
	return r.checkNumericBounds(n)
}

func (r *FieldRule) validateBool(value interface{}) error {
	switch v := value.(type) {
	case bool:
		return r.checkEnum(v)
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("expected bool, got string %q", v)
		}
		return r.checkEnum(parsed)
	default:
		return fmt.Errorf("expected bool, got %T", value)
	}
}

func (r *FieldRule) validateDuration(value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected duration string, got %T", value)
	}
	if r.Required && strings.TrimSpace(s) == "" {
		return fmt.Errorf("duration value is required")
	}
	if strings.TrimSpace(s) == "" && !r.Required {
		return nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	if duration <= 0 {
		return fmt.Errorf("duration %q must be positive", s)
	}
	return nil
}

func (r *FieldRule) checkNumericBounds(n float64) error {
	if r.Min != nil && n < *r.Min {
		return fmt.Errorf("value %v is below minimum %v", n, *r.Min)
	}
	if r.Max != nil && n > *r.Max {
		return fmt.Errorf("value %v exceeds maximum %v", n, *r.Max)
	}
	return nil
}

func (r *FieldRule) checkEnum(value interface{}) error {
	if len(r.Enum) == 0 {
		return nil
	}
	for _, allowed := range r.Enum {
		if fmt.Sprint(value) == fmt.Sprint(allowed) {
			return nil
		}
	}
	return fmt.Errorf("value %v is not in allowed values %v", value, r.Enum)
}

func ptr(f float64) *float64 { return &f }

// resolveRule finds the best-matching validation rule for a scope+path combination.
// Supports wildcards (*) in rule keys for dynamic names (groups, points, etc.).
func resolveRule(rules map[string]FieldRule, scope, configPath string) (*FieldRule, bool) {
	// Try exact match first
	key := scope + "::" + configPath
	if rule, ok := rules[key]; ok {
		return &rule, true
	}

	// Try wildcard matches
	parts := strings.Split(configPath, ".")
	for i := range parts {
		// Replace one segment at a time with * and try
		pattern := make([]string, len(parts))
		copy(pattern, parts)
		pattern[i] = "*"
		wildKey := scope + "::" + strings.Join(pattern, ".")
		if rule, ok := rules[wildKey]; ok {
			return &rule, true
		}
	}

	return nil, false
}

// DefaultValidationRules returns the built-in validation rules for SDK services.
func DefaultValidationRules() map[string]FieldRule {
	return map[string]FieldRule{
		// config.yaml
		"config::statusReport.heartbeatInterval": {Type: TypeDuration},
		"config::mqtt.url":                       {Type: TypeString, Pattern: `^(tcp|ssl|ws|wss)://.*:\d+$`},
		"config::mqtt.qos":                       {Type: TypeInt, Enum: []interface{}{0, 1, 2}},
		"config::mqtt.keepAliveSec":              {Type: TypeInt, Min: ptr(1), Max: ptr(3600)},
		"config::mqtt.pingTimeoutSec":            {Type: TypeInt, Min: ptr(1), Max: ptr(60)},
		"config::mqtt.connectTimeoutSec":         {Type: TypeInt, Min: ptr(1), Max: ptr(120)},
		"config::mqtt.healthCheckIntervalSec":    {Type: TypeInt, Min: ptr(1), Max: ptr(300)},
		"config::logging.level":                  {Type: TypeString, Enum: []interface{}{"debug", "info", "warn", "error"}},
		"config::telemetryReport.dataFormat":     {Type: TypeString, Enum: []interface{}{"rule", "raw", "influx", "telemetry", "compact"}},
		"config::telemetryReport.qos":            {Type: TypeInt, Enum: []interface{}{0, 1, 2}},

		// device common
		"device::protocols.s7.Host":        {Type: TypeString, Required: true},
		"device::protocols.s7.Port":        {Type: TypeInt, Min: ptr(1), Max: ptr(65535)},
		"device::protocols.s7.Rack":        {Type: TypeInt, Min: ptr(0), Max: ptr(15)},
		"device::protocols.s7.Slot":        {Type: TypeInt, Min: ptr(0), Max: ptr(15)},
		"device::protocols.s7.Timeout":     {Type: TypeInt, Min: ptr(1), Max: ptr(60)},
		"device::protocols.s7.IdleTimeout": {Type: TypeInt, Min: ptr(0), Max: ptr(300)},
		"device::connectionStrategy":       {Type: TypeString, Enum: []interface{}{"persistent", "on_demand"}},
		"device::profileName":              {Type: TypeString},
		"device::productCode":              {Type: TypeString},
		"device::description":              {Type: TypeString},

		// profile — telemetry
		"profile::telemetry.interval":                   {Type: TypeDuration},
		"profile::telemetry.groups.*.interval":          {Type: TypeDuration},
		"profile::telemetry.groups.*.heartbeatInterval": {Type: TypeDuration},
		"profile::telemetry.groups.*.onChange":          {Type: TypeBool},

		// profile — property
		"profile::property.interval":           {Type: TypeDuration},
		"profile::property.points.*.readWrite": {Type: TypeString, Enum: []interface{}{"R", "RW", "W"}},
		"profile::property.points.*.valueType": {Type: TypeString},
		"profile::property.points.*.maxLength": {Type: TypeInt, Min: ptr(0)},
		"profile::property.points.*.scale":     {Type: TypeString},
		"profile::property.points.*.precision": {Type: TypeInt, Min: ptr(0)},
	}
}
