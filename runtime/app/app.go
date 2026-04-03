package app

import (
	cfg "github.com/punk-one/edge-service-sdk/config"
	driver "github.com/punk-one/edge-service-sdk/driver"
)

type Config = cfg.Config

func Bootstrap(serviceName, version string, protocolDriver driver.ProtocolDriver) {
	cfg.Bootstrap(serviceName, version, protocolDriver)
}

func LoadConfig(path string) (cfg.Config, error) {
	return cfg.LoadConfig(path)
}
