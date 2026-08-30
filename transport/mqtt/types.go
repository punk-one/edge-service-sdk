package mqtt

import (
	"fmt"
	"sync"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	events "github.com/punk-one/edge-service-sdk/event"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type MessageDirection string

const (
	DirectionOutbound MessageDirection = "outbound"
	DirectionInbound  MessageDirection = "inbound"
)

// Observation is a copy of one logical MQTT message. Observers are optional;
// their failures must never alter the MQTT return path.
type Observation struct {
	Direction   MessageDirection
	Type        busapi.MessageType
	Topic       string
	QoS         byte
	Retain      bool
	Payload     []byte
	DataFormat  string
	DeviceName  string
	ProductCode string
	TraceID     string
	Identifier  string
}

type Observer interface {
	ObserveMQTT(message Observation)
}

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
	PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error
	PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error
	PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error
	PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error
	PublishJSON(topic string, qos byte, retain bool, payload interface{}) error
	Subscribe(topic string, qos byte, handler MessageHandler) error
	HealthCheck() error
	Close() error
}

// EventPublisher is an optional extension implemented by MQTT publishers that
// have an eventReport topic. Keeping it separate preserves compatibility with
// existing service-local publisher test doubles and integrations.
type EventPublisher interface {
	PublishEvent(event events.Event, replayed bool) error
}

// MultiGroupPublisher is implemented by publishers that fan out to multiple groups.
type MultiGroupPublisher interface {
	Publisher
	GroupPublishers() []Publisher
	GroupName(i int) string
	GroupStatusTopic(i int) TopicConfig
}

// MQTTConfig represents MQTT configuration.
type MQTTConfig struct {
	CACert                  string            `yaml:"caCert"`
	CAPath                  string            `yaml:"caPath"`
	CertPath                string            `yaml:"certPath"`
	ClientCert              string            `yaml:"clientCert"`
	ClientKey               string            `yaml:"clientKey"`
	MTLS                    bool              `yaml:"mtls"`
	Password                string            `yaml:"password"`
	PrivKeyPath             string            `yaml:"privateKeyPath"`
	QoS                     int               `yaml:"qos"`
	Retain                  bool              `yaml:"retain"`
	SkipTLSVer              bool              `yaml:"skipTLSVerify"`
	URL                     string            `yaml:"url"`
	Username                string            `yaml:"username"`
	KeepAliveSec            int               `yaml:"keepAliveSec"`
	PingTimeoutSec          int               `yaml:"pingTimeoutSec"`
	ConnectTimeoutSec       int               `yaml:"connectTimeoutSec"`
	PublishTimeoutSec       int               `yaml:"publishTimeoutSec"`
	HealthCheckIntervalSec  int               `yaml:"healthCheckIntervalSec"`
	InitialRetryIntervalMs  int               `yaml:"initialRetryIntervalMs"`
	MaxReconnectIntervalSec int               `yaml:"maxReconnectIntervalSec"`
	DisconnectQuiesceMs     int               `yaml:"disconnectQuiesceMs"`
	ClientId                string            `yaml:"clientId"` // optional, auto-generated if empty
	Groups                  []MQTTGroupConfig `yaml:"groups"`
}

// MQTTGroupConfig represents one parallel MQTT group.
type MQTTGroupConfig struct {
	Name                 string       `yaml:"name"`
	Mode                 string       `yaml:"mode"`                 // "" / "failover"
	HeartbeatInterval    string       `yaml:"heartbeatInterval"`    // overrides statusReport.heartbeatInterval
	TelemetryFormat      string       `yaml:"telemetryFormat"`      // overrides telemetryReport.dataFormat
	PropertyResultFormat string       `yaml:"propertyResultFormat"` // overrides propertyResult.dataFormat
	PropertyReportFormat string       `yaml:"propertyReportFormat"` // overrides propertyReport.dataFormat
	CommandResultFormat  string       `yaml:"commandResultFormat"`  // overrides commandResult.dataFormat
	StatusReportFormat   string       `yaml:"statusReportFormat"`   // overrides statusReport.dataFormat
	Brokers              []MQTTConfig `yaml:"brokers"`
	// Connection overrides (inherit from top-level MQTTConfig when empty)
	URL                     string `yaml:"url,omitempty"`
	Username                string `yaml:"username,omitempty"`
	Password                string `yaml:"password,omitempty"`
	ClientId                string `yaml:"clientId,omitempty"`
	KeepAliveSec            int    `yaml:"keepAliveSec,omitempty"`
	PingTimeoutSec          int    `yaml:"pingTimeoutSec,omitempty"`
	ConnectTimeoutSec       int    `yaml:"connectTimeoutSec,omitempty"`
	PublishTimeoutSec       int    `yaml:"publishTimeoutSec,omitempty"`
	HealthCheckIntervalSec  int    `yaml:"healthCheckIntervalSec,omitempty"`
	InitialRetryIntervalMs  int    `yaml:"initialRetryIntervalMs,omitempty"`
	MaxReconnectIntervalSec int    `yaml:"maxReconnectIntervalSec,omitempty"`
	DisconnectQuiesceMs     int    `yaml:"disconnectQuiesceMs,omitempty"`
	SkipTLSVerify           *bool  `yaml:"skipTLSVerify,omitempty"`
	MTLS                    *bool  `yaml:"mtls,omitempty"`
	QOS                     int    `yaml:"qos,omitempty"`
	Retain                  *bool  `yaml:"retain,omitempty"`
	CACert                  string `yaml:"caCert,omitempty"`
	CAPath                  string `yaml:"caPath,omitempty"`
	CertPath                string `yaml:"certPath,omitempty"`
	ClientCert              string `yaml:"clientCert,omitempty"`
	ClientKey               string `yaml:"clientKey,omitempty"`
	PrivKeyPath             string `yaml:"privateKeyPath,omitempty"`
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
	telemetry      TopicConfig
	propertyResult TopicConfig
	propertyReport TopicConfig
	commandResult  TopicConfig
	statusReport   TopicConfig
	eventReport    TopicConfig
	client         *mqttClient
	observer       Observer
}

type mqttMessage struct {
	Type        busapi.MessageType
	Topic       string
	QoS         byte
	Retain      bool
	Payload     []byte
	DeviceName  string
	ProductCode string
	TraceID     string
	Identifier  string
	DataFormat  string
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
	clientID       string

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

type observerPublisher interface {
	setObserver(Observer)
	observeInbound(Observation)
	publishDirect(topic string, qos byte, retain bool, payload []byte) error
}

// AttachObserver adds an optional message mirror without changing Publisher.
func AttachObserver(publisher Publisher, observer Observer) bool {
	target, ok := publisher.(interface{ setObserver(Observer) })
	if !ok {
		return false
	}
	target.setObserver(observer)
	return true
}

// ObserveInbound mirrors a received MQTT message while leaving its existing
// callback and error semantics untouched.
func ObserveInbound(publisher Publisher, message Observation) {
	if target, ok := publisher.(interface{ observeInbound(Observation) }); ok {
		target.observeInbound(message)
	}
}

// PublishDirect bypasses the observer and is used only for process-originated
// messages to prevent a JetStream -> MQTT -> JetStream loop.
func PublishDirect(publisher Publisher, topic string, qos byte, retain bool, payload []byte) error {
	target, ok := publisher.(interface {
		publishDirect(string, byte, bool, []byte) error
	})
	if !ok {
		return fmt.Errorf("mqtt publisher does not support direct publish")
	}
	return target.publishDirect(topic, qos, retain, payload)
}
