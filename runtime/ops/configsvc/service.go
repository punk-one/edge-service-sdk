package configsvc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	appconfig "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	coreevent "github.com/punk-one/edge-service-sdk/event"
	"github.com/punk-one/edge-service-sdk/internal/configfile"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
)

// ConfigService provides runtime configuration CRUD operations.
// It supports both temporary (in-memory) and permanent (file-based) changes.
type ConfigService struct {
	mu        sync.RWMutex
	persistMu sync.Mutex

	// name → filePath mappings
	deviceFiles  map[string]string // "ZJJJG006" → "configs/devices/ZJJJG06.yaml"
	profileFiles map[string]string // "ZJJJG_PROFILE" → "configs/profiles/zjjjg_profile.yaml"

	// profile → devices that use it
	profileDevices map[string][]string

	// device → profile name
	deviceProfile map[string]string

	// temporary overrides: key = "scope|name|configPath"
	overrides sync.Map

	// validation rules
	rules map[string]FieldRule

	// change callback
	onChange ChangeCallback

	// loaded config reference (for the selected main JSON/YAML file)
	mainConfigPath string
	devicesDir     string
	profilesDir    string
}

// NewConfigService creates a ConfigService from loaded configuration data.
// devicesDir and profilesDir are the config directories used during loading.
// deviceConfigs is the list of merged device configs from LoadConfig.
// profiles is the map of loaded profiles.
func NewConfigService(configsDir string, deviceConfigs []contracts.DeviceConfig, profiles map[string]contracts.DeviceProfile) *ConfigService {
	mainConfigPath := filepath.Join(configsDir, "config.yaml")
	if resolved, exists, err := configfile.ResolvePreferred(mainConfigPath); err == nil && exists {
		mainConfigPath = resolved
	}
	return NewConfigServiceWithPaths(
		mainConfigPath,
		filepath.Join(configsDir, "devices"),
		filepath.Join(configsDir, "profiles"),
		deviceConfigs,
		profiles,
	)
}

// NewConfigServiceWithPaths creates a ConfigService using the exact paths that
// were selected by the loader. Persistent updates therefore keep the source
// format and cannot create a competing lower-priority configuration file.
func NewConfigServiceWithPaths(mainConfigPath, devicesDir, profilesDir string, deviceConfigs []contracts.DeviceConfig, profiles map[string]contracts.DeviceProfile) *ConfigService {
	svc := &ConfigService{
		deviceFiles:    make(map[string]string),
		profileFiles:   make(map[string]string),
		profileDevices: make(map[string][]string),
		deviceProfile:  make(map[string]string),
		rules:          DefaultValidationRules(),
		mainConfigPath: filepath.Clean(mainConfigPath),
		devicesDir:     filepath.Clean(devicesDir),
		profilesDir:    filepath.Clean(profilesDir),
	}

	// Build device → file mapping by scanning devices directory
	svc.scanDeviceFiles(devicesDir, deviceConfigs)

	// Build profile → file mapping by scanning profiles directory
	svc.scanProfileFiles(profilesDir, profiles)

	// Build profile → devices reverse index
	for _, device := range deviceConfigs {
		profileName := strings.TrimSpace(device.ProfileName)
		if profileName != "" {
			svc.profileDevices[profileName] = append(svc.profileDevices[profileName], device.Name)
			svc.deviceProfile[device.Name] = profileName
		}
	}

	return svc
}

// SetOnChange registers a callback for configuration changes.
func (s *ConfigService) SetOnChange(cb ChangeCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = cb
}

// notifyChange fires the change callback (if set).
func (s *ConfigService) notifyChange(change ConfigChange) {
	s.mu.RLock()
	cb := s.onChange
	s.mu.RUnlock()
	if cb != nil {
		cb(change)
	}
}

// overrideKey builds the key for the overrides map.
func overrideKey(scope, name, configPath string) string {
	return scope + "|" + name + "|" + configPath
}

// ---------------------------------------------------------------------------
// Main config scope
// ---------------------------------------------------------------------------

// GetConfig reads a value from the selected main configuration file.
func (s *ConfigService) GetConfig(configPath string) (*ConfigResult, error) {
	// Check temporary override first
	ovKey := overrideKey("config", "", configPath)
	if val, ok := s.overrides.Load(ovKey); ok {
		return &ConfigResult{
			ConfigPath: configPath,
			Value:      sanitizeConfigValue(configPath, val),
			Configured: configuredFlag(configPath, val),
			Source:     SourceOverride,
		}, nil
	}

	// Read from file
	data, err := readConfigFile(s.mainConfigPath)
	if err != nil {
		return nil, err
	}
	val, err := navigatePath(data, configPath)
	if err != nil {
		return nil, err
	}
	return &ConfigResult{
		ConfigPath: configPath,
		Value:      sanitizeConfigValue(configPath, val),
		Configured: configuredFlag(configPath, val),
		Source:     SourceFile,
		SourceFile: s.mainConfigPath,
	}, nil
}

// SetConfig sets a value in the selected main configuration file.
func (s *ConfigService) SetConfig(configPath string, value interface{}, persist bool) (*ConfigSetResult, error) {
	// Validate
	if rule, ok := resolveRule(s.rules, "config", configPath); ok {
		if err := rule.Validate(value); err != nil {
			return nil, fmt.Errorf("validation failed for %s: %w", configPath, err)
		}
	}

	// Get previous value
	prev, _ := s.GetConfig(configPath)
	var prevVal interface{}
	if prev != nil {
		prevVal = prev.Value
	}

	ovKey := overrideKey("config", "", configPath)

	if persist {
		if err := s.updateConfigFile(s.mainConfigPath, func(data map[string]interface{}) error {
			return setByPath(data, configPath, value)
		}); err != nil {
			return nil, err
		}
		// Also update in-memory override (so it takes effect immediately)
		s.overrides.Store(ovKey, value)
	} else {
		// Temporary: only in-memory
		s.overrides.Store(ovKey, value)
	}

	result := &ConfigSetResult{
		ConfigPath:    configPath,
		PreviousValue: prevVal,
		CurrentValue:  sanitizeConfigValue(configPath, value),
		TargetFile:    s.mainConfigPath,
		Persist:       persist,
		NeedRestart:   needsRestart("config", configPath),
	}

	s.notifyChange(ConfigChange{
		Scope:      "config",
		Name:       "",
		ConfigPath: configPath,
		OldValue:   prevVal,
		NewValue:   sanitizeConfigValue(configPath, value),
		TargetFile: s.mainConfigPath,
		Persist:    persist,
	})

	return result, nil
}

// ---------------------------------------------------------------------------
// Device scope
// ---------------------------------------------------------------------------

// resolveDeviceFile returns the selected device configuration file path.
func (s *ConfigService) resolveDeviceFile(deviceName string) (string, error) {
	if f, ok := s.deviceFiles[deviceName]; ok {
		return f, nil
	}
	return "", fmt.Errorf("device %q not found", deviceName)
}

// resolveDeviceProfileFile returns the selected profile file associated with a device.
func (s *ConfigService) resolveDeviceProfileFile(deviceName string) (string, error) {
	profileName, ok := s.deviceProfile[deviceName]
	if !ok {
		return "", fmt.Errorf("device %q has no associated profile", deviceName)
	}
	if f, ok := s.profileFiles[profileName]; ok {
		return f, nil
	}
	return "", fmt.Errorf("profile %q not found for device %q", profileName, deviceName)
}

// GetDeviceConfig reads a device configuration value.
// It checks the device file, profile file, and overrides in order.
func (s *ConfigService) GetDeviceConfig(deviceName, configPath string) (*ConfigResult, error) {
	// Check temporary override first
	ovKey := overrideKey("device", deviceName, configPath)
	if val, ok := s.overrides.Load(ovKey); ok {
		return &ConfigResult{
			ConfigPath: configPath,
			Value:      sanitizeConfigValue(configPath, val),
			Configured: configuredFlag(configPath, val),
			Source:     SourceOverride,
		}, nil
	}

	// Check device file
	devFile, err := s.resolveDeviceFile(deviceName)
	if err != nil {
		return nil, err
	}
	devData, err := readConfigFile(devFile)
	if err != nil {
		return nil, err
	}
	// Navigate into deviceList.{name} for device files
	devList, ok := devData["deviceList"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("device file %s has no deviceList", devFile)
	}
	var devEntry map[string]interface{}
	for _, item := range devList {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if fmt.Sprint(m["name"]) == deviceName {
			devEntry = m
			break
		}
	}
	if devEntry == nil {
		return nil, fmt.Errorf("device %q not found in %s", deviceName, devFile)
	}

	// Try to find value in device entry
	devPath := configPath
	if pathExists(devEntry, devPath) {
		val, err := navigatePath(devEntry, devPath)
		if err == nil {
			return &ConfigResult{
				ConfigPath: configPath,
				Value:      sanitizeConfigValue(configPath, val),
				Configured: configuredFlag(configPath, val),
				Source:     SourceDevice,
				SourceFile: devFile,
			}, nil
		}
	}

	// Try profile file
	profileFile, err := s.resolveDeviceProfileFile(deviceName)
	if err != nil {
		return nil, fmt.Errorf("config_path %q not found in device %q and no profile: %w", configPath, deviceName, err)
	}
	profileData, err := readConfigFile(profileFile)
	if err != nil {
		return nil, err
	}
	val, err := navigatePath(profileData, configPath)
	if err != nil {
		return nil, fmt.Errorf("config_path %q not found in device %q or profile %q", configPath, deviceName, profileFile)
	}
	return &ConfigResult{
		ConfigPath: configPath,
		Value:      sanitizeConfigValue(configPath, val),
		Configured: configuredFlag(configPath, val),
		Source:     SourceProfile,
		SourceFile: profileFile,
	}, nil
}

// SetDeviceConfig sets a device configuration value.
// target controls which file is modified: "auto" (default), "device", or "profile".
func (s *ConfigService) SetDeviceConfig(deviceName, configPath string, value interface{}, persist bool, target string) (*ConfigSetResult, error) {
	// Validate (try both device and profile scopes)
	validated := false
	for _, scope := range []string{"device", "profile"} {
		if rule, ok := resolveRule(s.rules, scope, configPath); ok {
			if err := rule.Validate(value); err != nil {
				return nil, fmt.Errorf("validation failed for %s: %w", configPath, err)
			}
			validated = true
			break
		}
	}
	if !validated {
		// Try generic validation if no specific rule
		if rule, ok := resolveRule(s.rules, "config", configPath); ok {
			if err := rule.Validate(value); err != nil {
				return nil, fmt.Errorf("validation failed for %s: %w", configPath, err)
			}
		}
	}

	// Get previous value
	prev, _ := s.GetDeviceConfig(deviceName, configPath)
	var prevVal interface{}
	if prev != nil {
		prevVal = prev.Value
	}

	// Determine target file
	var targetFile string
	if target == "profile" {
		f, err := s.resolveDeviceProfileFile(deviceName)
		if err != nil {
			return nil, err
		}
		targetFile = f
	} else if target == "device" {
		f, err := s.resolveDeviceFile(deviceName)
		if err != nil {
			return nil, err
		}
		targetFile = f
	} else {
		// auto: check device file first
		devFile, err := s.resolveDeviceFile(deviceName)
		if err != nil {
			return nil, err
		}
		devData, err := readConfigFile(devFile)
		if err != nil {
			return nil, err
		}
		devList, _ := devData["deviceList"].([]interface{})
		var devEntry map[string]interface{}
		for _, item := range devList {
			m, _ := item.(map[string]interface{})
			if m != nil && fmt.Sprint(m["name"]) == deviceName {
				devEntry = m
				break
			}
		}
		if devEntry != nil && pathExists(devEntry, configPath) {
			targetFile = devFile
		} else {
			f, err := s.resolveDeviceProfileFile(deviceName)
			if err != nil {
				return nil, fmt.Errorf("cannot determine target for %q: %w", configPath, err)
			}
			targetFile = f
		}
	}

	ovKey := overrideKey("device", deviceName, configPath)

	if persist {
		if err := s.updateConfigFile(targetFile, func(data map[string]interface{}) error {
			// For device files, navigate into deviceList.{name}.
			if s.isDeviceConfigFile(targetFile) {
				devList, ok := data["deviceList"].([]interface{})
				if !ok {
					return fmt.Errorf("device file %s has no deviceList", targetFile)
				}
				for _, item := range devList {
					m, ok := item.(map[string]interface{})
					if !ok {
						continue
					}
					if fmt.Sprint(m["name"]) == deviceName {
						return setByPath(m, configPath, value)
					}
				}
				return fmt.Errorf("device %q not found in %s", deviceName, targetFile)
			}
			return setByPath(data, configPath, value)
		}); err != nil {
			return nil, err
		}
		s.overrides.Store(ovKey, value)
	} else {
		s.overrides.Store(ovKey, value)
	}

	result := &ConfigSetResult{
		ConfigPath:    configPath,
		PreviousValue: prevVal,
		CurrentValue:  sanitizeConfigValue(configPath, value),
		TargetFile:    targetFile,
		Persist:       persist,
		NeedRestart:   needsRestart("device", configPath),
	}

	s.notifyChange(ConfigChange{
		Scope:      "device",
		Name:       deviceName,
		ConfigPath: configPath,
		OldValue:   prevVal,
		NewValue:   sanitizeConfigValue(configPath, value),
		TargetFile: targetFile,
		Persist:    persist,
	})

	return result, nil
}

// ---------------------------------------------------------------------------
// Profile scope
// ---------------------------------------------------------------------------

// GetProfileConfig reads a profile configuration value.
func (s *ConfigService) GetProfileConfig(profileName, configPath string) (*ConfigResult, error) {
	// Check temporary override
	ovKey := overrideKey("profile", profileName, configPath)
	if val, ok := s.overrides.Load(ovKey); ok {
		return &ConfigResult{
			ConfigPath: configPath,
			Value:      sanitizeConfigValue(configPath, val),
			Configured: configuredFlag(configPath, val),
			Source:     SourceOverride,
		}, nil
	}

	profileFile, ok := s.profileFiles[profileName]
	if !ok {
		return nil, fmt.Errorf("profile %q not found", profileName)
	}

	data, err := readConfigFile(profileFile)
	if err != nil {
		return nil, err
	}
	val, err := navigatePath(data, configPath)
	if err != nil {
		return nil, err
	}
	return &ConfigResult{
		ConfigPath: configPath,
		Value:      sanitizeConfigValue(configPath, val),
		Configured: configuredFlag(configPath, val),
		Source:     SourceFile,
		SourceFile: profileFile,
	}, nil
}

// SetProfileConfig sets a profile configuration value.
func (s *ConfigService) SetProfileConfig(profileName, configPath string, value interface{}, persist bool) (*ConfigSetResult, error) {
	// Validate
	if rule, ok := resolveRule(s.rules, "profile", configPath); ok {
		if err := rule.Validate(value); err != nil {
			return nil, fmt.Errorf("validation failed for %s: %w", configPath, err)
		}
	}

	profileFile, ok := s.profileFiles[profileName]
	if !ok {
		return nil, fmt.Errorf("profile %q not found", profileName)
	}

	prev, _ := s.GetProfileConfig(profileName, configPath)
	var prevVal interface{}
	if prev != nil {
		prevVal = prev.Value
	}

	ovKey := overrideKey("profile", profileName, configPath)

	if persist {
		if err := s.updateConfigFile(profileFile, func(data map[string]interface{}) error {
			return setByPath(data, configPath, value)
		}); err != nil {
			return nil, err
		}
		s.overrides.Store(ovKey, value)
	} else {
		s.overrides.Store(ovKey, value)
	}

	affectedDevices := s.profileDevices[profileName]

	result := &ConfigSetResult{
		ConfigPath:    configPath,
		PreviousValue: prevVal,
		CurrentValue:  sanitizeConfigValue(configPath, value),
		TargetFile:    profileFile,
		Persist:       persist,
		NeedRestart:   needsRestart("profile", configPath),
	}

	s.notifyChange(ConfigChange{
		Scope:      "profile",
		Name:       profileName,
		ConfigPath: configPath,
		OldValue:   prevVal,
		NewValue:   sanitizeConfigValue(configPath, value),
		TargetFile: profileFile,
		Persist:    persist,
	})

	_ = affectedDevices // used by caller via GetAffectedDevices

	return result, nil
}

// GetAffectedDevices returns the list of device names that use a given profile.
func (s *ConfigService) GetAffectedDevices(profileName string) []string {
	return append([]string(nil), s.profileDevices[profileName]...)
}

// ---------------------------------------------------------------------------
// List operations
// ---------------------------------------------------------------------------

// ListDevices returns all device names, or the config keys of a specific device.
func (s *ConfigService) ListDevices(deviceName, configPath string) ([]string, []DeviceInfo, error) {
	if deviceName == "" {
		devices := make([]DeviceInfo, 0, len(s.deviceFiles))
		for name, file := range s.deviceFiles {
			info := DeviceInfo{
				Name: name,
				File: file,
			}
			if pn, ok := s.deviceProfile[name]; ok {
				info.ProfileName = pn
			}
			devices = append(devices, info)
		}
		return nil, devices, nil
	}

	devFile, err := s.resolveDeviceFile(deviceName)
	if err != nil {
		return nil, nil, err
	}

	data, err := readConfigFile(devFile)
	if err != nil {
		return nil, nil, err
	}

	devList, ok := data["deviceList"].([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("device file %s has no deviceList", devFile)
	}
	var devEntry map[string]interface{}
	for _, item := range devList {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if fmt.Sprint(m["name"]) == deviceName {
			devEntry = m
			break
		}
	}
	if devEntry == nil {
		return nil, nil, fmt.Errorf("device %q not found in %s", deviceName, devFile)
	}

	target := interface{}(devEntry)
	if configPath != "" {
		target, err = navigatePath(devEntry, configPath)
		if err != nil {
			return nil, nil, err
		}
	}

	keys, err := listKeys(target)
	if err != nil {
		return nil, nil, err
	}
	return keys, nil, nil
}

// ListProfiles returns all profile names, or the config keys of a specific profile.
func (s *ConfigService) ListProfiles(profileName, configPath string) ([]string, []ProfileInfo, error) {
	if profileName == "" {
		profiles := make([]ProfileInfo, 0, len(s.profileFiles))
		for name, file := range s.profileFiles {
			profiles = append(profiles, ProfileInfo{
				Name:          name,
				File:          file,
				UsedByDevices: append([]string(nil), s.profileDevices[name]...),
			})
		}
		return nil, profiles, nil
	}

	profileFile, ok := s.profileFiles[profileName]
	if !ok {
		return nil, nil, fmt.Errorf("profile %q not found", profileName)
	}

	data, err := readConfigFile(profileFile)
	if err != nil {
		return nil, nil, err
	}

	target := interface{}(data)
	if configPath != "" {
		target, err = navigatePath(data, configPath)
		if err != nil {
			return nil, nil, err
		}
	}

	keys, err := listKeys(target)
	if err != nil {
		return nil, nil, err
	}
	return keys, nil, nil
}

// ---------------------------------------------------------------------------
// Overrides management
// ---------------------------------------------------------------------------

// GetOverrides returns all current temporary overrides.
func (s *ConfigService) GetOverrides() []OverrideRecord {
	var records []OverrideRecord
	s.overrides.Range(func(key, value interface{}) bool {
		parts := strings.SplitN(fmt.Sprint(key), "|", 3)
		if len(parts) == 3 {
			records = append(records, OverrideRecord{
				Scope:      parts[0],
				Name:       parts[1],
				ConfigPath: parts[2],
				Value:      sanitizeConfigValue(parts[2], value),
			})
		}
		return true
	})
	return records
}

// ResetOverrides clears all or scoped temporary overrides.
// scope: "all", "config", "device:{name}", "profile:{name}"
func (s *ConfigService) ResetOverrides(scope string) int {
	if scope == "" {
		scope = "all"
	}

	count := 0
	s.overrides.Range(func(key, value interface{}) bool {
		keyStr := fmt.Sprint(key)
		if scope == "all" {
			s.overrides.Delete(key)
			count++
			return true
		}

		parts := strings.SplitN(keyStr, "|", 3)
		if len(parts) < 2 {
			return true
		}

		shouldDelete := false
		switch {
		case scope == "config" && parts[0] == "config":
			shouldDelete = true
		case strings.HasPrefix(scope, "device:") && parts[0] == "device":
			scopeName := strings.TrimPrefix(scope, "device:")
			if scopeName == "" || parts[1] == scopeName {
				shouldDelete = true
			}
		case strings.HasPrefix(scope, "profile:") && parts[0] == "profile":
			scopeName := strings.TrimPrefix(scope, "profile:")
			if scopeName == "" || parts[1] == scopeName {
				shouldDelete = true
			}
		}

		if shouldDelete {
			s.overrides.Delete(key)
			count++
		}
		return true
	})
	return count
}

// ---------------------------------------------------------------------------
// Topic config helpers
// ---------------------------------------------------------------------------

// GetEffectiveTopicConfig returns the effective topic config after applying overrides.
func (s *ConfigService) GetEffectiveTopicConfig(topicKey string, base mqtt.TopicConfig) mqtt.TopicConfig {
	ovKey := overrideKey("config", "", topicKey)
	if val, ok := s.overrides.Load(ovKey); ok {
		if m, ok := val.(map[string]interface{}); ok {
			if topic, ok := m["topic"].(string); ok {
				base.Topic = topic
			}
			if qos, ok := m["qos"].(int); ok {
				base.QoS = qos
			}
			if retain, ok := m["retain"].(bool); ok {
				base.Retain = retain
			}
			if hi, ok := m["heartbeatInterval"].(string); ok {
				base.HeartbeatInterval = hi
			}
		}
	}
	return base
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (s *ConfigService) scanDeviceFiles(devicesDir string, deviceConfigs []contracts.DeviceConfig) {
	// Build a map of device name → source file from the loaded configs
	// We scan the directory to find which file contains each device
	entries, err := configfile.ListPreferred(devicesDir)
	if err != nil {
		return
	}
	for _, file := range entries {
		data, err := readConfigFile(file)
		if err != nil {
			continue
		}
		devList, ok := data["deviceList"].([]interface{})
		if !ok {
			continue
		}
		for _, item := range devList {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if name, ok := m["name"].(string); ok {
				s.deviceFiles[name] = file
			}
		}
	}

	// Fill in productCode and description from device configs
	for _, device := range deviceConfigs {
		if _, ok := s.deviceFiles[device.Name]; !ok {
			// Device loaded but we couldn't find its file (shouldn't happen)
			continue
		}
	}
}

func (s *ConfigService) scanProfileFiles(profilesDir string, profiles map[string]contracts.DeviceProfile) {
	entries, err := configfile.ListPreferred(profilesDir)
	if err != nil {
		return
	}
	for _, file := range entries {
		data, err := readConfigFile(file)
		if err != nil {
			continue
		}
		if name, ok := data["name"].(string); ok {
			s.profileFiles[name] = file
		}
	}
}

func (s *ConfigService) isDeviceConfigFile(path string) bool {
	cleaned := filepath.Clean(path)
	for _, candidate := range s.deviceFiles {
		if filepath.Clean(candidate) == cleaned {
			return true
		}
	}
	return false
}

func (s *ConfigService) updateConfigFile(path string, update func(map[string]interface{}) error) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	data, err := readConfigFile(path)
	if err != nil {
		return err
	}
	if err := update(data); err != nil {
		return err
	}
	if err := s.validateCandidateGraph(path, data); err != nil {
		return fmt.Errorf("candidate configuration is invalid: %w", err)
	}
	return writeConfigFile(path, data)
}

func (s *ConfigService) validateCandidateGraph(candidatePath string, candidateData map[string]interface{}) error {
	mainData, err := s.readCandidateMap(s.mainConfigPath, candidatePath, candidateData)
	if err != nil {
		return err
	}
	encodedMain, err := configfile.Marshal(s.mainConfigPath, mainData)
	if err != nil {
		return err
	}
	config, err := appconfig.DecodeMainConfig(s.mainConfigPath, encodedMain)
	if err != nil {
		return err
	}

	profiles, err := s.loadCandidateProfiles(candidatePath, candidateData)
	if err != nil {
		return err
	}
	devices, err := s.loadCandidateDevices(candidatePath, candidateData)
	if err != nil {
		return err
	}
	devices, err = appconfig.ApplyProfiles(devices, profiles)
	if err != nil {
		return err
	}
	config.Devices = devices
	config = appconfig.NormalizeConfig(config)
	if err := appconfig.ValidateConfig(config); err != nil {
		return err
	}
	return validateCandidateEvents(config)
}

func (s *ConfigService) loadCandidateProfiles(candidatePath string, candidateData map[string]interface{}) (map[string]contracts.DeviceProfile, error) {
	files, err := listConfigFiles(s.profilesDir)
	if err != nil {
		return nil, fmt.Errorf("read profile config directory %s: %w", s.profilesDir, err)
	}
	profiles := make(map[string]contracts.DeviceProfile, len(files))
	for _, path := range files {
		var profile contracts.DeviceProfile
		if err := s.decodeCandidate(path, candidatePath, candidateData, &profile); err != nil {
			return nil, err
		}
		profile = appconfig.NormalizeProfile(profile)
		if strings.TrimSpace(profile.Name) == "" {
			return nil, fmt.Errorf("profile file %s missing name", path)
		}
		if _, duplicate := profiles[profile.Name]; duplicate {
			return nil, fmt.Errorf("duplicate device profile name %q", profile.Name)
		}
		profiles[profile.Name] = profile
	}
	return profiles, nil
}

func (s *ConfigService) loadCandidateDevices(candidatePath string, candidateData map[string]interface{}) ([]contracts.DeviceConfig, error) {
	files, err := listConfigFiles(s.devicesDir)
	if err != nil {
		return nil, fmt.Errorf("read device config directory %s: %w", s.devicesDir, err)
	}
	var devices []contracts.DeviceConfig
	for _, path := range files {
		var file struct {
			DeviceList []contracts.DeviceConfig `yaml:"deviceList"`
		}
		if err := s.decodeCandidate(path, candidatePath, candidateData, &file); err != nil {
			return nil, err
		}
		for _, device := range file.DeviceList {
			devices = append(devices, appconfig.NormalizeDeviceConfig(device))
		}
	}
	return devices, nil
}

func (s *ConfigService) readCandidateMap(path, candidatePath string, candidateData map[string]interface{}) (map[string]interface{}, error) {
	if sameConfigPath(path, candidatePath) {
		return candidateData, nil
	}
	return readConfigFile(path)
}

func (s *ConfigService) decodeCandidate(path, candidatePath string, candidateData map[string]interface{}, target interface{}) error {
	if !sameConfigPath(path, candidatePath) {
		return configfile.Read(path, target)
	}
	data, err := configfile.Marshal(path, candidateData)
	if err != nil {
		return err
	}
	if err := configfile.Decode(path, data, target); err != nil {
		return fmt.Errorf("parse configuration file %s: %w", path, err)
	}
	return nil
}

func listConfigFiles(dir string) ([]string, error) {
	files, err := configfile.ListPreferred(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return files, err
}

func sameConfigPath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(filepath.Clean(left))
	rightAbs, rightErr := filepath.Abs(filepath.Clean(right))
	if leftErr == nil && rightErr == nil {
		return leftAbs == rightAbs
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func validateCandidateEvents(config appconfig.Config) error {
	eventDir := strings.TrimSpace(config.Device.EventDir)
	if eventDir == "" {
		return nil
	}
	profiles, err := coreevent.LoadProfiles(eventDir)
	if err != nil {
		return err
	}
	for _, device := range config.Devices {
		if strings.TrimSpace(device.EventProfile) == "" {
			continue
		}
		profile, ok := coreevent.SelectProfile(profiles, device.EventProfile)
		if !ok {
			return fmt.Errorf("device %s references unknown eventProfile %s", device.Name, device.EventProfile)
		}
		if err := coreevent.ValidateForDevice(profile, device); err != nil {
			return fmt.Errorf("device %s eventProfile %s: %w", device.Name, device.EventProfile, err)
		}
	}
	return nil
}

// needsRestart reports whether a config change requires a service restart.
func needsRestart(scope, configPath string) bool {
	if scope == "config" && strings.HasPrefix(configPath, "device.") && strings.HasSuffix(configPath, "Dir") {
		return true
	}
	// MQTT connection parameters
	if strings.HasPrefix(configPath, "mqtt.") {
		return true
	}
	// Topic templates
	topicPrefixes := []string{
		"telemetryReport.", "propertySet.", "propertyGet.", "propertyResult.",
		"propertyReport.", "commandCall.", "commandResult.", "queryRequest.",
		"queryResult.", "statusReport.",
	}
	for _, prefix := range topicPrefixes {
		if strings.HasPrefix(configPath, prefix) && configPath != prefix+"heartbeatInterval" {
			return true
		}
	}
	// Protocol connection parameters
	if strings.HasPrefix(configPath, "protocols.") {
		return true
	}
	// Status report heartbeat interval can be hot-reloaded
	// Telemetry group intervals can be hot-reloaded
	// Point RW attributes can be hot-reloaded
	return false
}

// GetMainConfigPath returns the selected main configuration path.
func (s *ConfigService) GetMainConfigPath() string {
	return s.mainConfigPath
}

// DeviceFiles returns a copy of the device→file mapping.
func (s *ConfigService) DeviceFiles() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]string, len(s.deviceFiles))
	for k, v := range s.deviceFiles {
		result[k] = v
	}
	return result
}

// ProfileFiles returns a copy of the profile→file mapping.
func (s *ConfigService) ProfileFiles() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]string, len(s.profileFiles))
	for k, v := range s.profileFiles {
		result[k] = v
	}
	return result
}

// DeviceProfileName returns the profile name for a device.
func (s *ConfigService) DeviceProfileName(deviceName string) (string, bool) {
	name, ok := s.deviceProfile[deviceName]
	return name, ok
}

// needRestartKey is a helper used by the needsRestart logic.
// topicPrefixes are config keys that require restart (except heartbeatInterval).
// This is referenced by the app-level commands to set need_restart in responses.
func NeedRestartKey(scope, configPath string) bool {
	return needsRestart(scope, configPath)
}

const redactedConfigValue = "<redacted>"

func sanitizeConfigValue(configPath string, value interface{}) interface{} {
	if isSensitiveConfigPath(configPath) {
		return redactScalar(value)
	}
	return redactSensitiveTree(value)
}

func configuredFlag(configPath string, value interface{}) *bool {
	if !isSensitiveConfigPath(configPath) {
		return nil
	}
	configured := true
	switch typed := value.(type) {
	case nil:
		configured = false
	case string:
		configured = strings.TrimSpace(typed) != ""
	case []interface{}:
		configured = len(typed) > 0
	case map[string]interface{}:
		configured = len(typed) > 0
	}
	return &configured
}

func isSensitiveConfigPath(configPath string) bool {
	parts := strings.FieldsFunc(configPath, func(r rune) bool {
		return r == '.' || r == '[' || r == ']'
	})
	if len(parts) == 0 {
		return false
	}
	return isSensitiveConfigKey(parts[len(parts)-1])
}

func isSensitiveConfigKey(key string) bool {
	normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(strings.TrimSpace(key)))
	switch normalized {
	case "password", "clientkey", "bootstraptoken":
		return true
	default:
		return false
	}
}

func redactScalar(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	if text, ok := value.(string); ok && text == "" {
		return ""
	}
	return redactedConfigValue
}

func redactSensitiveTree(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			if isSensitiveConfigKey(key) {
				result[key] = redactScalar(item)
			} else {
				result[key] = redactSensitiveTree(item)
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(typed))
		for index, item := range typed {
			result[index] = redactSensitiveTree(item)
		}
		return result
	default:
		return value
	}
}
