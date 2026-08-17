package bus

import (
	"context"
	"fmt"
	"strings"
)

// MessageType identifies one fixed SDK bus route.
type MessageType string

const (
	TelemetryReport MessageType = "telemetry.report"
	PropertyReport  MessageType = "property.report"
	PropertyResult  MessageType = "property.result"
	CommandResult   MessageType = "command.result"
	PropertySet     MessageType = "property.set"
	PropertyGet     MessageType = "property.get"
	CommandCall     MessageType = "command.call"
	EventReport     MessageType = "event.report"
	StatusReport    MessageType = "status.report"
)

const (
	SubjectTelemetryReport = "edge.v1.telemetry.report"
	SubjectPropertyReport  = "edge.v1.property.report"
	SubjectPropertyResult  = "edge.v1.property.result"
	SubjectCommandResult   = "edge.v1.command.result"
	SubjectPropertySet     = "edge.v1.property.set"
	SubjectPropertyGet     = "edge.v1.property.get"
	SubjectCommandCall     = "edge.v1.command.call.*"
	SubjectEventReport     = "edge.v1.event.report"
	SubjectStatusReport    = "edge.v1.status.report"
)

const (
	StreamName    = "EDGE_SDK"
	StreamSubject = "edge.v1.>"

	HeaderOrigin      = "Edge-Origin"
	HeaderProcessName = "Edge-Process-Name"
	HeaderMessageType = "Edge-Message-Type"
	HeaderDataFormat  = "Edge-Data-Format"
	HeaderTraceID     = "Edge-Trace-Id"
	HeaderCausationID = "Edge-Causation-Id"
	HeaderHop         = "Edge-Hop"
	HeaderProductCode = "Edge-Product-Code"
	HeaderDeviceCode  = "Edge-Device-Code"
	HeaderMQTTTopic   = "Edge-Mqtt-Topic"
	HeaderCommandID   = "Edge-Command-Identifier"
	HeaderMessageID   = "Nats-Msg-Id"
)

type Origin string

const (
	OriginSDK     Origin = "sdk"
	OriginMQTT    Origin = "mqtt"
	OriginProcess Origin = "process"
	OriginNATS    Origin = "nats"
)

// Message is the transport-neutral message exposed to processors. Data is the
// same payload used by the corresponding MQTT route.
type Message struct {
	Subject     string
	Type        MessageType
	Data        []byte
	Headers     map[string]string
	Origin      Origin
	ProcessName string
	DataFormat  string
	TraceID     string
	CausationID string
	ProductCode string
	DeviceCode  string
	Identifier  string
	Hop         int
}

// Publisher is the only bus capability exposed to application processors.
type Publisher interface {
	Publish(ctx context.Context, message Message) error
}

func SubjectFor(messageType MessageType, identifier string) (string, error) {
	switch messageType {
	case TelemetryReport:
		return SubjectTelemetryReport, nil
	case PropertyReport:
		return SubjectPropertyReport, nil
	case PropertyResult:
		return SubjectPropertyResult, nil
	case CommandResult:
		return SubjectCommandResult, nil
	case PropertySet:
		return SubjectPropertySet, nil
	case PropertyGet:
		return SubjectPropertyGet, nil
	case CommandCall:
		identifier = strings.TrimSpace(identifier)
		if identifier == "" || strings.ContainsAny(identifier, ".*> \t\r\n") {
			return "", fmt.Errorf("command identifier is required and must be one NATS subject token")
		}
		return "edge.v1.command.call." + identifier, nil
	case EventReport:
		return SubjectEventReport, nil
	case StatusReport:
		return SubjectStatusReport, nil
	default:
		return "", fmt.Errorf("unsupported bus message type %q", messageType)
	}
}

func FilterSubjectFor(messageType MessageType) (string, error) {
	if messageType == CommandCall {
		return SubjectCommandCall, nil
	}
	return SubjectFor(messageType, "")
}

func ParseMessageType(value string) (MessageType, error) {
	messageType := MessageType(strings.ToLower(strings.TrimSpace(value)))
	if _, err := FilterSubjectFor(messageType); err != nil {
		return "", err
	}
	return messageType, nil
}

func ParseSubject(subject string) (MessageType, string, bool) {
	switch strings.TrimSpace(subject) {
	case SubjectTelemetryReport:
		return TelemetryReport, "", true
	case SubjectPropertyReport:
		return PropertyReport, "", true
	case SubjectPropertyResult:
		return PropertyResult, "", true
	case SubjectCommandResult:
		return CommandResult, "", true
	case SubjectPropertySet:
		return PropertySet, "", true
	case SubjectPropertyGet:
		return PropertyGet, "", true
	case SubjectEventReport:
		return EventReport, "", true
	case SubjectStatusReport:
		return StatusReport, "", true
	}
	const prefix = "edge.v1.command.call."
	if strings.HasPrefix(subject, prefix) {
		identifier := strings.TrimSpace(strings.TrimPrefix(subject, prefix))
		if identifier != "" && !strings.Contains(identifier, ".") {
			return CommandCall, identifier, true
		}
	}
	return "", "", false
}
