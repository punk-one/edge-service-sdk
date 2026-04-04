package app

import (
	rtconfig "github.com/punk-one/edge-service-sdk/runtime/config"
)

type Config = rtconfig.Config

func LoadConfig(path string) (Config, error) {
	return rtconfig.LoadConfig(path)
}
