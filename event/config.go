package event

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"

	"gopkg.in/yaml.v3"
)

var forbiddenProfileKeys = map[string]struct{}{
	"pipeline":  {},
	"inputs":    {},
	"outputs":   {},
	"topic":     {},
	"qos":       {},
	"retain":    {},
	"order":     {},
	"dependson": {},
}

// LoadProfiles loads all event YAML files from dir. An absent directory is a
// valid disabled configuration and returns an empty map.
func LoadProfiles(dir string) (map[string]EventProfileFile, error) {
	profiles := make(map[string]EventProfileFile)
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return profiles, nil
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return profiles, nil
	} else if err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	more, err := filepath.Glob(filepath.Join(dir, "*.yml"))
	if err != nil {
		return nil, err
	}
	files = append(files, more...)
	sort.Strings(files)

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read event profile %s: %w", file, err)
		}
		if err := validateForbiddenKeys(data, file); err != nil {
			return nil, err
		}

		var profile EventProfileFile
		if err := yaml.Unmarshal(data, &profile); err != nil {
			return nil, fmt.Errorf("parse event profile %s: %w", file, err)
		}
		profile.Name = strings.TrimSpace(profile.Name)
		profile.Type = strings.ToUpper(strings.TrimSpace(profile.Type))
		if profile.Type == "" {
			profile.Type = "EVENT"
		}
		profile.Config.Categories = normalizeCategories(profile.Config.Categories)
		if profile.Name == "" {
			return nil, fmt.Errorf("event profile %s missing name", file)
		}
		if _, exists := profiles[profile.Name]; exists {
			return nil, fmt.Errorf("duplicate event profile name %q", profile.Name)
		}
		if err := ValidateProfile(profile); err != nil {
			return nil, fmt.Errorf("invalid event profile %s: %w", file, err)
		}
		profiles[profile.Name] = profile
		fileName := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		if fileName != "" && fileName != profile.Name {
			if existing, exists := profiles[fileName]; exists && existing.Name != profile.Name {
				return nil, fmt.Errorf("event profile filename alias %q conflicts with profile %q", fileName, existing.Name)
			}
			profiles[fileName] = profile
		}
	}

	return profiles, nil
}

func normalizeCategories(input map[string]CategoryConfig) map[string]CategoryConfig {
	if len(input) == 0 {
		return input
	}
	result := make(map[string]CategoryConfig, len(input))
	for name, category := range input {
		result[strings.ToLower(strings.TrimSpace(name))] = category
	}
	return result
}

// SelectProfile resolves an explicit device.eventProfile reference. The
// reference may be the YAML name or the filename without its extension.
func SelectProfile(profiles map[string]EventProfileFile, name string) (EventProfileFile, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return EventProfileFile{}, false
	}
	if profile, ok := profiles[name]; ok {
		return profile, true
	}
	base := strings.TrimSuffix(strings.TrimSuffix(name, ".yaml"), ".yml")
	for key, profile := range profiles {
		if strings.TrimSuffix(strings.TrimSuffix(key, ".yaml"), ".yml") == base {
			return profile, true
		}
	}
	return EventProfileFile{}, false
}

// ValidateProfile validates syntax and generic rule semantics. Device field
// references are validated separately because the final profile is known only
// after device/profile merging.
func ValidateProfile(profile EventProfileFile) error {
	if profile.Type != "EVENT" {
		return fmt.Errorf("type must be EVENT")
	}
	if len(profile.Config.Categories) == 0 {
		return fmt.Errorf("categories cannot be empty")
	}
	if timezone := strings.TrimSpace(profile.Config.Time.Timezone); timezone != "" {
		if _, err := time.LoadLocation(timezone); err != nil {
			return fmt.Errorf("invalid timezone %q: %w", timezone, err)
		}
	}
	if businessDayStart := strings.TrimSpace(profile.Config.Time.BusinessDayStart); businessDayStart != "" {
		if _, err := parseBusinessDayStart(businessDayStart); err != nil {
			return err
		}
	}

	seenCodes := make(map[string]string)
	for categoryName, category := range profile.Config.Categories {
		categoryName = strings.ToLower(strings.TrimSpace(categoryName))
		if categoryName == "" {
			return fmt.Errorf("category name cannot be empty")
		}
		if categoryName == CategoryConnect {
			return fmt.Errorf("category %q is SDK-defined and must not be configured", CategoryConnect)
		}
		if strings.EqualFold(category.StateModel, "exclusive") {
			if category.AllowMultipleActive == nil || *category.AllowMultipleActive {
				return fmt.Errorf("category %s exclusive stateModel requires allowMultipleActive: false", categoryName)
			}
			if strings.TrimSpace(category.ExclusiveGroup) == "" {
				return fmt.Errorf("category %s exclusive stateModel requires exclusiveGroup", categoryName)
			}
			if len(category.Priority) == 0 {
				return fmt.Errorf("category %s exclusive stateModel requires priority", categoryName)
			}
			if strings.TrimSpace(category.TransitionOrder) == "" {
				return fmt.Errorf("category %s exclusive stateModel requires transitionOrder", categoryName)
			}
		}

		for index, rule := range category.Events {
			code := strings.TrimSpace(rule.EventCode)
			if code == "" {
				return fmt.Errorf("category %s event[%d] missing eventCode", categoryName, index)
			}
			if previous, exists := seenCodes[code]; exists {
				return fmt.Errorf("eventCode %q is duplicated by categories %s and %s", code, previous, categoryName)
			}
			seenCodes[code] = categoryName
			eventType := strings.ToLower(strings.TrimSpace(rule.EventType))
			if eventType != EventTypePulse && eventType != EventTypeRiseClear {
				return fmt.Errorf("event %s has unsupported eventType %q", code, rule.EventType)
			}
			if strings.TrimSpace(rule.Name) == "" {
				return fmt.Errorf("event %s missing name", code)
			}
			if eventType == EventTypeRiseClear && strings.TrimSpace(rule.When) == "" && strings.TrimSpace(rule.Recover) == "" && !rule.Fallback {
				return fmt.Errorf("rise-clear event %s requires when/recover or fallback", code)
			}
			if eventType == EventTypePulse && strings.TrimSpace(rule.State) != "" {
				return fmt.Errorf("pulse event %s must not declare state", code)
			}
			if err := validateReport(code, rule.Report); err != nil {
				return err
			}
			if eventType == EventTypePulse && rule.IsSummary() {
				return fmt.Errorf("pulse event %s cannot use summary report mode", code)
			}
			if err := validatePayloadSelector(code, rule.Payload); err != nil {
				return err
			}
			if rule.Aggregate != nil && strings.TrimSpace(rule.Aggregate.CodeField) == "" {
				return fmt.Errorf("aggregate event %s requires codeField", code)
			}
			if _, err := ParseExpression(rule.When); err != nil && strings.TrimSpace(rule.When) != "" {
				return fmt.Errorf("event %s invalid when expression: %w", code, err)
			}
			if _, err := ParseExpression(rule.Recover); err != nil && strings.TrimSpace(rule.Recover) != "" {
				return fmt.Errorf("event %s invalid recover expression: %w", code, err)
			}
		}
	}

	return nil
}

func parseBusinessDayStart(raw string) (time.Duration, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("invalid businessDayStart %q: expected HH:MM[:SS]", raw)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid businessDayStart %q", raw)
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid businessDayStart %q", raw)
	}
	second := 0
	if len(parts) == 3 {
		second, err = strconv.Atoi(parts[2])
		if err != nil {
			return 0, fmt.Errorf("invalid businessDayStart %q", raw)
		}
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return 0, fmt.Errorf("invalid businessDayStart %q", raw)
	}
	return time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second, nil
}

// ValidateForDevice verifies payload and aggregate fields against the merged
// device profile. Expression references are parsed for syntax, but missing
// expression fields remain runtime-unknown so devices without optional manual
// mode points continue to work.
func ValidateForDevice(profile EventProfileFile, device contracts.DeviceConfig) error {
	if err := ValidateProfile(profile); err != nil {
		return err
	}

	telemetryNames, groupNames, groupedNames := deviceTelemetryNames(device)
	propertyNames := devicePropertyNames(device)
	for categoryName, category := range profile.Config.Categories {
		for _, rule := range category.Events {
			for _, group := range rule.Payload.Groups {
				group = strings.TrimSpace(group)
				if group == "" {
					return fmt.Errorf("event %s has empty payload group", rule.EventCode)
				}
				if _, ok := groupNames[group]; !ok {
					return fmt.Errorf("event %s payload group %q is not defined for device %s", rule.EventCode, group, device.Name)
				}
			}
			seenPoints := make(map[string]struct{})
			for _, point := range rule.Payload.Points {
				point = strings.TrimSpace(point)
				if point == "" {
					return fmt.Errorf("event %s has empty payload point", rule.EventCode)
				}
				if _, ok := telemetryNames[point]; !ok {
					return fmt.Errorf("event %s payload point %q is not defined for device %s", rule.EventCode, point, device.Name)
				}
				if _, ok := groupedNames[point]; ok {
					return fmt.Errorf("event %s payload point %q belongs to a telemetry group; select the group instead", rule.EventCode, point)
				}
				if _, ok := seenPoints[point]; ok {
					return fmt.Errorf("event %s payload point %q is duplicated", rule.EventCode, point)
				}
				seenPoints[point] = struct{}{}
			}
			if rule.Aggregate != nil {
				field := fieldName(rule.Aggregate.CodeField)
				if field == "" {
					return fmt.Errorf("event %s aggregate codeField is empty", rule.EventCode)
				}
				if _, ok := telemetryNames[field]; !ok {
					return fmt.Errorf("event %s aggregate codeField %q is not telemetry field", rule.EventCode, field)
				}
			}
			for _, expression := range []string{rule.When, rule.Recover} {
				if strings.TrimSpace(expression) == "" {
					continue
				}
				parsed, err := ParseExpression(expression)
				if err != nil {
					return fmt.Errorf("category %s event %s expression: %w", categoryName, rule.EventCode, err)
				}
				for _, ref := range parsed.References() {
					if strings.HasPrefix(ref, "property.") {
						if _, ok := propertyNames[strings.TrimPrefix(ref, "property.")]; !ok {
							return fmt.Errorf("event %s references unknown property field %q", rule.EventCode, ref)
						}
					}
					if strings.HasPrefix(ref, "data.") || strings.HasPrefix(ref, "LAST_VALUE.") {
						// Missing telemetry references are deliberately runtime-unknown;
						// this is required for devices without manual-mode points.
						_ = telemetryNames[strings.SplitN(ref, ".", 2)[1]]
					}
				}
			}
		}
	}
	return nil
}

// ConfigHash is stable for a loaded profile and is used as a state namespace.
func ConfigHash(profile EventProfileFile) string {
	data, err := yaml.Marshal(profile)
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func validateReport(code string, report ReportConfig) error {
	mode := strings.ToLower(strings.TrimSpace(report.Mode))
	if mode == "" {
		mode = ReportModeImmediate
	}
	if mode != ReportModeImmediate && mode != ReportModeSummary {
		return fmt.Errorf("event %s has unsupported report mode %q", code, report.Mode)
	}
	interval, intervalSet, err := parseConfiguredDuration(report.Interval)
	if err != nil {
		return fmt.Errorf("event %s invalid report interval: %w", code, err)
	}
	window, windowSet, err := parseConfiguredDuration(report.Window)
	if err != nil {
		return fmt.Errorf("event %s invalid report window: %w", code, err)
	}
	if mode == ReportModeSummary && (!intervalSet || interval <= 0) {
		return fmt.Errorf("event %s summary mode requires interval > 0", code)
	}
	if mode == ReportModeImmediate && intervalSet && interval > 0 {
		return fmt.Errorf("event %s immediate report mode requires interval=0", code)
	}
	if windowSet && window > 0 && (!intervalSet || interval <= 0) {
		return fmt.Errorf("event %s report window > 0 requires interval > 0", code)
	}
	align := strings.ToLower(strings.TrimSpace(report.Align))
	if align != "" && align != "event" && align != "clock" && align != "business_day" {
		return fmt.Errorf("event %s has unsupported report align %q", code, report.Align)
	}
	return nil
}

func validatePayloadSelector(code string, payload PayloadSelector) error {
	seen := make(map[string]struct{})
	for _, group := range payload.Groups {
		group = strings.TrimSpace(group)
		if group == "" {
			return fmt.Errorf("event %s payload contains empty group", code)
		}
		key := "group:" + group
		if _, ok := seen[key]; ok {
			return fmt.Errorf("event %s payload group %q is duplicated", code, group)
		}
		seen[key] = struct{}{}
	}
	for _, point := range payload.Points {
		point = strings.TrimSpace(point)
		if point == "" {
			return fmt.Errorf("event %s payload contains empty point", code)
		}
		key := "point:" + point
		if _, ok := seen[key]; ok {
			return fmt.Errorf("event %s payload point %q is duplicated", code, point)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func parseConfiguredDuration(raw string) (time.Duration, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, true, err
	}
	if d < 0 {
		return 0, true, fmt.Errorf("duration cannot be negative")
	}
	return d, true, nil
}

func fieldName(ref string) string {
	ref = strings.TrimSpace(ref)
	for _, prefix := range []string{"data.", "LAST_VALUE."} {
		if strings.HasPrefix(ref, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(ref, prefix))
		}
	}
	return ref
}

func deviceTelemetryNames(device contracts.DeviceConfig) (map[string]struct{}, map[string]struct{}, map[string]struct{}) {
	all := make(map[string]struct{})
	groups := make(map[string]struct{})
	grouped := make(map[string]struct{})
	for _, point := range device.Telemetry.Points {
		all[point.Name] = struct{}{}
	}
	for _, item := range device.Telemetry.Structs {
		all[item.Name] = struct{}{}
	}
	for _, group := range device.Telemetry.Groups {
		groups[group.Name] = struct{}{}
		for _, point := range group.Points {
			all[point.Name] = struct{}{}
			grouped[point.Name] = struct{}{}
		}
		for _, item := range group.Structs {
			all[item.Name] = struct{}{}
			grouped[item.Name] = struct{}{}
		}
	}
	return all, groups, grouped
}

func devicePropertyNames(device contracts.DeviceConfig) map[string]struct{} {
	fields := make(map[string]struct{})
	for _, point := range device.Property.Points {
		fields[point.Name] = struct{}{}
	}
	for _, item := range device.Property.Structs {
		fields[item.Name] = struct{}{}
		collectStructFieldNames(item.Fields, fields)
	}
	return fields
}

func collectStructFieldNames(items []contracts.PropertyStructField, fields map[string]struct{}) {
	for _, item := range items {
		if item.Name != "" {
			fields[item.Name] = struct{}{}
		}
		collectStructFieldNames(item.Fields, fields)
	}
}

func validateForbiddenKeys(data []byte, file string) error {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("parse event profile %s: %w", file, err)
	}
	if err := walkForbiddenKeys(&node); err != nil {
		return fmt.Errorf("event profile %s: %w", file, err)
	}
	return nil
}

func walkForbiddenKeys(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.ToLower(strings.TrimSpace(node.Content[i].Value))
			if _, forbidden := forbiddenProfileKeys[key]; forbidden {
				return fmt.Errorf("key %q is not allowed in EVENT profiles", node.Content[i].Value)
			}
			if err := walkForbiddenKeys(node.Content[i+1]); err != nil {
				return err
			}
		}
		return nil
	}
	for _, child := range node.Content {
		if err := walkForbiddenKeys(child); err != nil {
			return err
		}
	}
	return nil
}
