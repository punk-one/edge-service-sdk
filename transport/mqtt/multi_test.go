package mqtt

import (
	"testing"

	logger "github.com/punk-one/edge-service-sdk/logging"
)

var testLogger = logger.NewLogger("test", logger.Config{Level: "error", Format: "text"})

func TestMergeBrokerConfig_StringFields(t *testing.T) {
	base := MQTTConfig{
		URL:      "tcp://default:1883",
		Username: "default_user",
		Password: "default_pass",
		ClientId: "default-client",
	}
	override := MQTTConfig{
		URL:      "tcp://override:1883",
		Username: "override_user",
	}

	result := mergeBrokerConfig(base, override)
	if result.URL != "tcp://override:1883" {
		t.Errorf("URL: expected override, got %s", result.URL)
	}
	if result.Username != "override_user" {
		t.Errorf("Username: expected override, got %s", result.Username)
	}
	if result.Password != "default_pass" {
		t.Errorf("Password: expected inherited, got %s", result.Password)
	}
	if result.ClientId != "default-client" {
		t.Errorf("ClientId: expected inherited, got %s", result.ClientId)
	}
}

func TestMergeBrokerConfig_IntFields(t *testing.T) {
	base := MQTTConfig{
		KeepAliveSec:           60,
		PingTimeoutSec:         5,
		ConnectTimeoutSec:      15,
		PublishTimeoutSec:      10,
		HealthCheckIntervalSec: 30,
	}
	override := MQTTConfig{
		KeepAliveSec:   120,
		PingTimeoutSec: 0,
	}

	result := mergeBrokerConfig(base, override)
	if result.KeepAliveSec != 120 {
		t.Errorf("KeepAliveSec: expected 120, got %d", result.KeepAliveSec)
	}
	if result.PingTimeoutSec != 5 {
		t.Errorf("PingTimeoutSec: expected inherited 5, got %d", result.PingTimeoutSec)
	}
	if result.ConnectTimeoutSec != 15 {
		t.Errorf("ConnectTimeoutSec: expected inherited 15, got %d", result.ConnectTimeoutSec)
	}
}

func TestMergeBrokerConfig_ClearsGroups(t *testing.T) {
	base := MQTTConfig{URL: "tcp://base:1883"}
	override := MQTTConfig{
		URL:    "tcp://override:1883",
		Groups: []MQTTGroupConfig{{Name: "should-be-cleared"}},
	}

	result := mergeBrokerConfig(base, override)
	if len(result.Groups) != 0 {
		t.Errorf("Groups should be cleared, got %d", len(result.Groups))
	}
}

func TestBuildGroupBrokerList_EmptyBrokers(t *testing.T) {
	group := MQTTGroupConfig{Name: "test"}
	defaults := MQTTConfig{URL: "tcp://default:1883", Username: "user"}

	result := buildGroupBrokerList(group, defaults)
	if len(result) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(result))
	}
	if result[0].URL != "tcp://default:1883" {
		t.Errorf("expected default URL, got %s", result[0].URL)
	}
}

func TestBuildGroupBrokerList_WithBrokers(t *testing.T) {
	group := MQTTGroupConfig{
		Name: "test",
		Brokers: []MQTTConfig{
			{URL: "tcp://broker1:1883", Username: "user1"},
			{URL: "tcp://broker2:1883"},
		},
	}
	defaults := MQTTConfig{URL: "tcp://default:1883", Username: "default_user", Password: "default_pass"}

	result := buildGroupBrokerList(group, defaults)
	if len(result) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(result))
	}
	if result[0].URL != "tcp://broker1:1883" {
		t.Errorf("broker[0] URL: expected broker1, got %s", result[0].URL)
	}
	if result[0].Username != "user1" {
		t.Errorf("broker[0] Username: expected user1, got %s", result[0].Username)
	}
	if result[0].Password != "default_pass" {
		t.Errorf("broker[0] Password: expected inherited, got %s", result[0].Password)
	}
	if result[1].URL != "tcp://broker2:1883" {
		t.Errorf("broker[1] URL: expected broker2, got %s", result[1].URL)
	}
	if result[1].Username != "default_user" {
		t.Errorf("broker[1] Username: expected inherited, got %s", result[1].Username)
	}
}

func TestMergeGroupToConfig_StringFields(t *testing.T) {
	base := MQTTConfig{URL: "tcp://base:1883", Username: "base_user"}
	group := MQTTGroupConfig{
		URL:      "tcp://group:1883",
		Username: "",
		Password: "group_pass",
	}

	result := mergeGroupToConfig(group, base)
	if result.URL != "tcp://group:1883" {
		t.Errorf("URL: expected group, got %s", result.URL)
	}
	if result.Username != "base_user" {
		t.Errorf("Username: expected inherited, got %s", result.Username)
	}
	if result.Password != "group_pass" {
		t.Errorf("Password: expected group, got %s", result.Password)
	}
}

func TestMergeGroupToConfig_BoolPointers(t *testing.T) {
	base := MQTTConfig{SkipTLSVer: false, MTLS: false, Retain: false}
	trueVal := true
	group := MQTTGroupConfig{
		SkipTLSVerify: &trueVal,
	}

	result := mergeGroupToConfig(group, base)
	if !result.SkipTLSVer {
		t.Errorf("SkipTLSVer: expected true, got false")
	}
	if result.MTLS {
		t.Errorf("MTLS: expected false (inherited), got true")
	}
	if result.Retain {
		t.Errorf("Retain: expected false (inherited), got true")
	}
}

func TestBuildGroupTopics_NoOverride(t *testing.T) {
	base := TopicConfig{Topic: "test/topic", DataFormat: "compact", QoS: 1, Retain: true}
	result := buildGroupTopics(base, "")
	if result.DataFormat != "compact" {
		t.Errorf("DataFormat: expected compact, got %s", result.DataFormat)
	}
	if result.Topic != "test/topic" {
		t.Errorf("Topic: expected unchanged, got %s", result.Topic)
	}
}

func TestBuildGroupTopics_WithOverride(t *testing.T) {
	base := TopicConfig{Topic: "test/topic", DataFormat: "compact", QoS: 1, Retain: true}
	result := buildGroupTopics(base, "influx")
	if result.DataFormat != "influx" {
		t.Errorf("DataFormat: expected influx, got %s", result.DataFormat)
	}
	if result.Topic != "test/topic" {
		t.Errorf("Topic: expected unchanged, got %s", result.Topic)
	}
	if result.QoS != 1 {
		t.Errorf("QoS: expected unchanged, got %d", result.QoS)
	}
}

func TestNewPublisher_NoGroups(t *testing.T) {
	cfg := MQTTConfig{URL: "tcp://localhost:1883"}
	topics := TopicConfig{Topic: "test"}
	p := NewPublisher(cfg, topics, topics, topics, topics, topics, testLogger)

	if _, ok := p.(*MQTTPublisher); !ok {
		t.Errorf("expected *MQTTPublisher, got %T", p)
	}
	// Clean up
	p.Close()
}

func TestNewPublisher_GroupMethods(t *testing.T) {
	// Test that NewPublisher returns MultiGroupPublisher when groups are present,
	// without actually connecting (using empty URL = no broker connection attempted).
	cfg := MQTTConfig{
		Groups: []MQTTGroupConfig{
			{Name: "g1", TelemetryFormat: "influx", Brokers: []MQTTConfig{{}}},
			{Name: "g2", StatusReportFormat: "compact", Brokers: []MQTTConfig{{}}},
		},
	}
	telemetry := TopicConfig{Topic: "test", DataFormat: "compact"}
	status := TopicConfig{Topic: "status", DataFormat: "raw"}

	p := NewPublisher(cfg, telemetry, TopicConfig{}, TopicConfig{}, TopicConfig{}, status, testLogger)

	mp, ok := p.(*multiPublisher)
	if !ok {
		t.Fatalf("expected *multiPublisher, got %T", p)
	}
	if len(mp.groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(mp.groups))
	}
	if mp.groups[0].name != "g1" {
		t.Errorf("group[0] name: expected g1, got %s", mp.groups[0].name)
	}
	if mp.groups[0].telemetry.DataFormat != "influx" {
		t.Errorf("group[0] telemetry format: expected influx, got %s", mp.groups[0].telemetry.DataFormat)
	}
	if mp.groups[1].name != "g2" {
		t.Errorf("group[1] name: expected g2, got %s", mp.groups[1].name)
	}

	// Test GroupPublishers
	gps := mp.GroupPublishers()
	if len(gps) != 2 {
		t.Fatalf("expected 2 group publishers, got %d", len(gps))
	}

	// Test GroupStatusTopic
	topic := mp.GroupStatusTopic(1)
	if topic.DataFormat != "compact" {
		t.Errorf("group[1] status format: expected compact, got %s", topic.DataFormat)
	}
	if topic.Topic != "status" {
		t.Errorf("group[1] status topic: expected 'status', got %s", topic.Topic)
	}

	// Test out of bounds
	emptyTopic := mp.GroupStatusTopic(-1)
	if emptyTopic.Topic != "" {
		t.Errorf("expected empty topic for -1, got %s", emptyTopic.Topic)
	}
	emptyTopic = mp.GroupStatusTopic(99)
	if emptyTopic.Topic != "" {
		t.Errorf("expected empty topic for 99, got %s", emptyTopic.Topic)
	}

	// Clean up
	p.Close()
}
