package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

// Category names reserved by the SDK event engine.
const (
	CategoryOEE     = "oee"
	CategoryConnect = "connect"
	CategoryAlarm   = "alarm"
)

const (
	EventTypePulse     = "pulse"
	EventTypeRiseClear = "rise-clear"

	EventActionRaise = "raise"
	EventActionClear = "clear"

	ReportModeImmediate = "immediate"
	ReportModeSummary   = "summary"
)

// Event is the public event envelope. ProductCode and CreatedAt are internal
// transport/state metadata; TraceID is part of the common outer transport
// envelope and is retained across replay.
type Event struct {
	Time        int64     `json:"time"`
	DeviceCode  string    `json:"device_code"`
	Data        EventData `json:"data"`
	ProductCode string    `json:"-"`
	TraceID     string    `json:"trace_id,omitempty"`
	CreatedAt   int64     `json:"-"`
}

// EventData contains the event-specific data required by the event contract.
type EventData struct {
	EventCode       string                 `json:"event_code"`
	Category        string                 `json:"category"`
	Level           interface{}            `json:"level,omitempty"`
	Type            string                 `json:"type"`
	Message         string                 `json:"message,omitempty"`
	Payload         map[string]interface{} `json:"payload,omitempty"`
	Meta            map[string]interface{} `json:"meta,omitempty"`
	Remark          string                 `json:"remark,omitempty"`
	EventInstanceID string                 `json:"event_instance_id"`
}

// PublicMap returns the cloud-facing envelope. send_at and is_replayed are
// delivery metadata; time, data and event_instance_id remain unchanged during
// replay.
func (e Event) PublicMap(replayed bool, sendAt int64) map[string]interface{} {
	result := map[string]interface{}{
		"time":        e.Time,
		"device_code": e.DeviceCode,
		"data":        e.Data,
		"send_at":     sendAt,
		"is_replayed": replayed,
	}
	if strings.TrimSpace(e.TraceID) != "" {
		result["trace_id"] = e.TraceID
	}
	return result
}

// MarshalPublicJSON serializes the public event envelope.
func (e Event) MarshalPublicJSON(replayed bool, sendAt int64) ([]byte, error) {
	return json.Marshal(e.PublicMap(replayed, sendAt))
}

// ConnectionObservation is the normalized connection input consumed by the
// event engine. ObservedAt is the edge/protocol observation time, not the
// later MQTT send time.
type ConnectionObservation struct {
	DeviceCode string
	Online     bool
	State      string
	ObservedAt int64
	LastSeenAt int64
	Error      string
	Known      bool
}

// EventProfileFile is one YAML file from the configured event directory.
type EventProfileFile struct {
	SchemaVersion int         `yaml:"schemaVersion"`
	Name          string      `yaml:"name"`
	Type          string      `yaml:"type"`
	Version       int         `yaml:"version"`
	Description   string      `yaml:"description"`
	Config        EventConfig `yaml:"config"`
}

// EventConfig contains the rules for one explicitly bound device event
// profile. A file is selected only through device.eventProfile.
type EventConfig struct {
	Time           TimeConfig                `yaml:"time"`
	Initialization InitializationConfig      `yaml:"initialization"`
	Categories     map[string]CategoryConfig `yaml:"categories"`
}

type TimeConfig struct {
	Timezone         string `yaml:"timezone"`
	BusinessDayStart string `yaml:"businessDayStart"`
}

type InitializationConfig struct {
	EmitInitialState   bool   `yaml:"emitInitialState"`
	InitialTimeQuality string `yaml:"initialTimeQuality"`
	MissingPolicy      string `yaml:"missingPolicy"`
}

type CategoryConfig struct {
	StateModel          string      `yaml:"stateModel"`
	ExclusiveGroup      string      `yaml:"exclusiveGroup"`
	AllowMultipleActive *bool       `yaml:"allowMultipleActive"`
	TransitionOrder     string      `yaml:"transitionOrder"`
	Priority            []string    `yaml:"priority"`
	MissingPolicy       string      `yaml:"missingPolicy"`
	Events              []EventRule `yaml:"events"`
}

type EventRule struct {
	EventCode  string           `yaml:"eventCode"`
	Name       string           `yaml:"name"`
	EventType  string           `yaml:"eventType"`
	State      string           `yaml:"state"`
	When       string           `yaml:"when"`
	Recover    string           `yaml:"recover"`
	Fallback   bool             `yaml:"fallback"`
	HoldFor    string           `yaml:"holdFor"`
	RecoverFor string           `yaml:"recoverFor"`
	Level      interface{}      `yaml:"level"`
	Message    string           `yaml:"message"`
	Payload    PayloadSelector  `yaml:"payload"`
	Aggregate  *AggregateConfig `yaml:"aggregate"`
	Report     ReportConfig     `yaml:"report"`
}

// PayloadSelector selects complete telemetry groups and standalone telemetry
// points. The output payload contains maps named groups and points.
type PayloadSelector struct {
	Groups []string `yaml:"groups"`
	Points []string `yaml:"points"`
}

type ReportConfig struct {
	Mode        string `yaml:"mode"`
	Interval    string `yaml:"interval"`
	Window      string `yaml:"window"`
	Align       string `yaml:"align"`
	EmitEmpty   bool   `yaml:"emitEmpty"`
	FlushOnStop bool   `yaml:"flushOnStop"`
}

type AggregateConfig struct {
	Mode                string              `yaml:"mode"`
	CodeField           string              `yaml:"codeField"`
	ZeroValue           interface{}         `yaml:"zeroValue"`
	Code                AggregateCodeConfig `yaml:"code"`
	CodeSetChange       string              `yaml:"codeSetChange"`
	UpdateType          string              `yaml:"updateType"`
	KeepEventInstanceID bool                `yaml:"keepEventInstanceId"`
	Generated           AggregateGenerated  `yaml:"generated"`
}

type AggregateCodeConfig struct {
	Prefix     string `yaml:"prefix"`
	Value      string `yaml:"value"`
	Dictionary string `yaml:"dictionary"`
}

type AggregateGenerated struct {
	Codes     string `yaml:"codes"`
	Text      string `yaml:"text"`
	Count     string `yaml:"count"`
	Active    string `yaml:"active"`
	Delimiter string `yaml:"delimiter"`
}

// DeviceBinding associates one merged device configuration with one event
// profile. It is intentionally kept in the event package so services do not
// need to implement profile matching themselves.
type DeviceBinding struct {
	Device  contracts.DeviceConfig
	Profile EventProfileFile
}

func (r EventRule) HoldDuration() time.Duration {
	return parseDurationOrZero(r.HoldFor)
}

func (r EventRule) RecoverDuration() time.Duration {
	return parseDurationOrZero(r.RecoverFor)
}

func (r EventRule) IsSummary() bool {
	return strings.EqualFold(strings.TrimSpace(r.Report.Mode), ReportModeSummary)
}

func parseDurationOrZero(raw string) time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || d < 0 {
		return 0
	}
	return d
}

func (p EventProfileFile) String() string {
	if strings.TrimSpace(p.Name) != "" {
		return p.Name
	}
	return fmt.Sprintf("event-profile-v%d", p.Version)
}
