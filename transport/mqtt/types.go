package mqtt

import (
	"sync"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// MessageHandler handles incoming MQTT messages for subscribed topics.
type MessageHandler func(topic string, payload []byte)

// Client defines the reusable MQTT connection contract for other modules.
type Client interface {
	Publish(topic string, qos byte, retain bool, payload []byte) error
	PublishJSON(topic string, qos byte, retain bool, payload interface{}) error
	Subscribe(topic string, qos byte, handler MessageHandler) error
	RegisterOnConnect(hook func())
	Close() error
}

// Publisher defines the MQTT interface used by startup orchestration.
type Publisher interface {
	PublishTelemetry(device contracts.DeviceConfig, data map[string]interface{}) error
	PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error
	PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error
	PublishPropertyPost(device contracts.DeviceConfig, payload map[string]interface{}) error
	PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error
	Subscribe(topic string, qos byte, handler MessageHandler) error
	HealthCheck() error
	Close() error
}

// MQTTConfig represents MQTT configuration.
type MQTTConfig struct {
	CACert                  string `yaml:"caCert"`
	CAPath                  string `yaml:"caPath"`
	CertPath                string `yaml:"certPath"`
	ClientCert              string `yaml:"clientCert"`
	ClientKey               string `yaml:"clientKey"`
	MTLS                    bool   `yaml:"mtls"`
	Password                string `yaml:"password"`
	PrivKeyPath             string `yaml:"privateKeyPath"`
	QoS                     int    `yaml:"qos"`
	Retain                  bool   `yaml:"retain"`
	SkipTLSVer              bool   `yaml:"skipTLSVerify"`
	URL                     string `yaml:"url"`
	Username                string `yaml:"username"`
	KeepAliveSec            int    `yaml:"keepAliveSec"`
	PingTimeoutSec          int    `yaml:"pingTimeoutSec"`
	ConnectTimeoutSec       int    `yaml:"connectTimeoutSec"`
	PublishTimeoutSec       int    `yaml:"publishTimeoutSec"`
	HealthCheckIntervalSec  int    `yaml:"healthCheckIntervalSec"`
	InitialRetryIntervalMs  int    `yaml:"initialRetryIntervalMs"`
	MaxReconnectIntervalSec int    `yaml:"maxReconnectIntervalSec"`
	DisconnectQuiesceMs     int    `yaml:"disconnectQuiesceMs"`
}

// TopicConfig represents one MQTT topic section in config.
type TopicConfig struct {
	Topic             string `yaml:"topic"`
	QoS               int    `yaml:"qos"`
	Retain            bool   `yaml:"retain"`
	DataFormat        string `yaml:"dataFormat"`
	HeartbeatInterval string `yaml:"heartbeatInterval"`
}

// MQTTPublisher implements telemetry/property/status MQTT I/O.
type MQTTPublisher struct {
	telemetry    TopicConfig
	propertyPost TopicConfig
	statusReport TopicConfig
	client       *mqttClient
}

type mqttMessage struct {
	Topic       string
	QoS         byte
	Retain      bool
	Payload     []byte
	DeviceName  string
	ProductCode string
	TraceID     string
}

type subscription struct {
	qos     byte
	handler MessageHandler
}

type mqttClient struct {
	config MQTTConfig
	logger logger.LoggingClient

	mu             sync.Mutex
	client         paho.Client
	subscriptions  map[string]subscription
	reconnecting   bool
	lastConnectErr error
	onConnectHooks []func()

	healthMu sync.Mutex
	healthy  bool
	degraded bool

	stopCh    chan struct{}
	closeOnce sync.Once
}

type telemetryData struct {
	TraceID    string                 `json:"traceId"`
	Time       int64                  `json:"time"`
	SendAt     int64                  `json:"sendAt"`
	IsReplayed bool                   `json:"isReplayed"`
	DeviceName string                 `json:"deviceName"`
	SourceName string                 `json:"sourceName"`
	Values     map[string]interface{} `json:"values"`
}

var _ Client = (*mqttClient)(nil)
var _ Publisher = (*MQTTPublisher)(nil)
