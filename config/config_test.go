package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigParsesLowerCamelCaseConfig(t *testing.T) {
	root := t.TempDir()
	devicesDir := filepath.Join(root, "devices")
	profilesDir := filepath.Join(root, "profiles")
	if err := os.MkdirAll(devicesDir, 0o755); err != nil {
		t.Fatalf("MkdirAll devicesDir: %v", err)
	}
	if err := os.MkdirAll(profilesDir, 0o755); err != nil {
		t.Fatalf("MkdirAll profilesDir: %v", err)
	}

	configPath := filepath.Join(root, "config.yaml")
	configYAML := `logging:
  level: "debug"
  format: "json"
  file: "./logs/app-log.log"
  maxSize: 100
  maxFiles: 7
  maxBackups: 3
  compress: false
service:
  host: "127.0.0.1"
  port: 19994
  startupMsg: "S7 device service started"
  type: "sensor"
storage:
  sqlitePath: "./data/runtime.db"
auth:
  accessTokenTTLMin: 10
  bootstrapToken: "bootstrap-secret"
  keyFile: "./data/auth.key"
mqtt:
  url: "tcp://127.0.0.1:1883"
  username: "u"
  password: "p"
  keepAliveSec: 60
  pingTimeoutSec: 5
  connectTimeoutSec: 15
  publishTimeoutSec: 10
  healthCheckIntervalSec: 30
  initialRetryIntervalMs: 1000
  maxReconnectIntervalSec: 60
  disconnectQuiesceMs: 250
  skipTLSVerify: false
  caCert: ""
  caPath: ""
  certPath: ""
  clientCert: ""
  clientKey: ""
  mtls: false
  privateKeyPath: ""
  qos: 0
  retain: false
reliableQueue:
  enabled: true
  sqlitePath: "./data/runtime.db"
  memoryQueueSize: 2048
  batchSize: 100
  flushIntervalMs: 1000
  replayIntervalMs: 3000
  replayRatePerSec: 20
  retentionDays: 7
  keepLatestOnly: false
device:
  profilesDir: "` + profilesDir + `"
  devicesDir: "` + devicesDir + `"
telemetryPost:
  topic: "v1/gateway/{productCode}/telemetry/post"
  qos: 0
  retain: false
  dataFormat: "rule"
propertySet:
  topic: ""
  qos: 0
propertyGet:
  topic: ""
  qos: 0
propertyPost:
  topic: ""
  qos: 0
  retain: false
statusReport:
  topic: ""
  qos: 0
  retain: false
`
	if err := os.WriteFile(configPath, []byte(configYAML), 0o644); err != nil {
		t.Fatalf("WriteFile configPath: %v", err)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if config.Service.Host != "127.0.0.1" || config.Service.Port != 19994 {
		t.Fatalf("unexpected service config: %#v", config.Service)
	}
	if config.MQTT.KeepAliveSec != 60 {
		t.Fatalf("unexpected mqtt config: %#v", config.MQTT)
	}
	if config.Storage.SQLitePath != "./data/runtime.db" || config.ReliableQueue.SQLitePath != "./data/runtime.db" {
		t.Fatalf("unexpected sqlite config: storage=%#v reliable=%#v", config.Storage, config.ReliableQueue)
	}
	if config.Auth.AccessTokenTTLMin != 10 || config.Auth.KeyFile != "./data/auth.key" {
		t.Fatalf("unexpected auth config: %#v", config.Auth)
	}
	if config.Device.ProfilesDir != profilesDir || config.Device.DevicesDir != devicesDir {
		t.Fatalf("unexpected device config: %#v", config.Device)
	}
	if config.TelemetryPost.Topic != "v1/gateway/{productCode}/telemetry/post" || config.TelemetryPost.DataFormat != "rule" {
		t.Fatalf("unexpected telemetryPost config: %#v", config.TelemetryPost)
	}
}
