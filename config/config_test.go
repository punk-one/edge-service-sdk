package config

import (
	"os"
	"path/filepath"
	"testing"

	reliable "github.com/punk-one/edge-service-sdk/telemetry/reliable"
	mqtt "github.com/punk-one/edge-service-sdk/transport/mqtt"
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
telemetryOutbox:
  sqlitePath: "./data/telemetry-outbox.db"
  retentionDays: 7
  sendBatchSize: 100
  maxSendRatePerSec: 100
  retryInitialMs: 1000
  retryMaxMs: 30000
device:
  profilesDir: "` + filepath.ToSlash(profilesDir) + `"
  devicesDir: "` + filepath.ToSlash(devicesDir) + `"
telemetryReport:
  topic: "v1/gateway/{productCode}/telemetry/report"
  qos: 0
  retain: false
  dataFormat: "rule"
propertySet:
  topic: ""
  qos: 0
propertyGet:
  topic: ""
  qos: 0
propertyResult:
  topic: ""
  qos: 0
  retain: false
queryRequest:
  topic: "v1/gateway/{productCode}/query/request"
  qos: 1
queryResult:
  topic: "v1/gateway/{productCode}/query/result"
  qos: 1
  retain: false
statusReport:
  topic: ""
  qos: 0
  retain: false
  heartbeatInterval: "15s"
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
	if config.Storage.SQLitePath != filepath.FromSlash("./data/runtime.db") || config.TelemetryOutbox.SQLitePath != filepath.FromSlash("./data/telemetry-outbox.db") {
		t.Fatalf("unexpected sqlite config: storage=%#v telemetryOutbox=%#v", config.Storage, config.TelemetryOutbox)
	}
	if config.Auth.AccessTokenTTLMin != 10 || config.Auth.KeyFile != "./data/auth.key" {
		t.Fatalf("unexpected auth config: %#v", config.Auth)
	}
	if config.Device.ProfilesDir != profilesDir || config.Device.DevicesDir != devicesDir {
		t.Fatalf("unexpected device config: %#v", config.Device)
	}
	if config.TelemetryReport.Topic != "v1/gateway/{productCode}/telemetry/report" || config.TelemetryReport.DataFormat != "rule" {
		t.Fatalf("unexpected telemetryReport config: %#v", config.TelemetryReport)
	}
	if config.QueryRequest.Topic != "v1/gateway/{productCode}/query/request" || config.QueryResult.Topic != "v1/gateway/{productCode}/query/result" {
		t.Fatalf("unexpected query config: request=%#v result=%#v", config.QueryRequest, config.QueryResult)
	}
	if config.StatusReport.HeartbeatInterval != "15s" {
		t.Fatalf("unexpected statusReport config: %#v", config.StatusReport)
	}
}

func TestNormalizeConfigSetsDefaultStatusHeartbeatInterval(t *testing.T) {
	config := NormalizeConfig(Config{})
	if config.StatusReport.HeartbeatInterval != "30s" {
		t.Fatalf("unexpected default status heartbeat interval: %#v", config.StatusReport)
	}
}

func TestLoadMainConfigUsesTelemetryOutboxDefaults(t *testing.T) {
	config, err := loadMainConfig(filepath.Join(t.TempDir(), "missing-config.yaml"))
	if err != nil {
		t.Fatalf("loadMainConfig() error = %v", err)
	}
	want := reliable.DefaultTelemetryOutboxConfig()
	want.SQLitePath = filepath.FromSlash(want.SQLitePath)
	if config.TelemetryOutbox != want {
		t.Fatalf("telemetryOutbox defaults = %#v, want %#v", config.TelemetryOutbox, want)
	}
	if config.TelemetryReport.QoS != 1 {
		t.Fatalf("default telemetry QoS = %d, want 1", config.TelemetryReport.QoS)
	}
}

func TestLoadConfigRejectsReliableQueue(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(configPath, []byte("reliableQueue:\n  retentionDays: 7\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig() accepted removed reliableQueue configuration")
	}
}

func TestValidateConfigRequiresIndependentTelemetryDatabase(t *testing.T) {
	config := NormalizeConfig(Config{
		Storage:         StorageConfig{SQLitePath: "./data/runtime.db"},
		TelemetryOutbox: reliable.DefaultTelemetryOutboxConfig(),
		TelemetryReport: mqtt.TopicConfig{Topic: "telemetry"},
	})
	config.TelemetryOutbox.SQLitePath = config.Storage.SQLitePath
	if err := ValidateConfig(config); err == nil {
		t.Fatal("ValidateConfig() accepted shared runtime and telemetry database")
	}
}
