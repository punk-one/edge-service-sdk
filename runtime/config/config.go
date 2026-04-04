package config

import (
	cfg "github.com/punk-one/edge-service-sdk/config"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
)

type Config = cfg.Config
type StorageConfig = cfg.StorageConfig
type AuthConfig = cfg.AuthConfig
type ServiceConfig = cfg.ServiceConfig
type DeviceConfig = cfg.DeviceConfig

func LoadConfig(path string) (Config, error) {
	return cfg.LoadConfig(path)
}

func NormalizeConfig(value Config) Config {
	return cfg.NormalizeConfig(value)
}

func EffectiveLogLevel(value Config) string {
	return cfg.EffectiveLogLevel(value)
}

func EffectiveLoggerConfig(value Config) logger.Config {
	return cfg.EffectiveLoggerConfig(value)
}

func NormalizeDeviceConfig(device contracts.DeviceConfig) contracts.DeviceConfig {
	return cfg.NormalizeDeviceConfig(device)
}

func ProtocolPropertiesFromConfig(device contracts.DeviceConfig) map[string]contracts.ProtocolProperties {
	return cfg.ProtocolPropertiesFromConfig(device)
}

func BuildTelemetryRequests(device contracts.DeviceConfig) ([]contracts.CommandRequest, error) {
	return cfg.BuildTelemetryRequests(device)
}
