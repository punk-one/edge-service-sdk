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

	EventPhaseStart  = "start"
	EventPhaseEnd    = "end"
	EventPhaseRecord = "record"

	EventStatusActive   = "active"
	EventStatusResolved = "resolved"
	EventStatusRecorded = "recorded"

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
// EventType is the lifecycle action (raise, clear, or pulse); the rule
// definition type remains in Meta under rule_type.
type EventData struct {
	EventIdentifier string                 `json:"event_identifier"`
	Category        string                 `json:"category"`
	Level           interface{}            `json:"level,omitempty"`
	EventType       string                 `json:"event_type"`
	Phase           string                 `json:"phase"`
	Status          string                 `json:"status"`
	Message         string                 `json:"message,omitempty"`
	Payload         map[string]interface{} `json:"payload,omitempty"`
	Meta            map[string]interface{} `json:"meta,omitempty"`
	Remark          string                 `json:"remark,omitempty"`
	EventInstanceID string                 `json:"event_instance_id"`
}

// EventLifecycle returns the normalized phase and status for one event
// lifecycle action. All public events must use one of these mappings.
func EventLifecycle(eventType string) (phase, status string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case EventActionRaise:
		return EventPhaseStart, EventStatusActive, true
	case EventActionClear:
		return EventPhaseEnd, EventStatusResolved, true
	case EventTypePulse:
		return EventPhaseRecord, EventStatusRecorded, true
	default:
		return "", "", false
	}
}

func lifecyclePhase(eventType string) string {
	phase, _, _ := EventLifecycle(eventType)
	return phase
}

func lifecycleStatus(eventType string) string {
	_, status, _ := EventLifecycle(eventType)
	return status
}

// NormalizeLifecycle fills the public lifecycle fields from EventType. The
// mapping is authoritative so callers cannot accidentally publish a
// contradictory phase or status.
func (d EventData) NormalizeLifecycle() EventData {
	if phase, status, ok := EventLifecycle(d.EventType); ok {
		d.Phase = phase
		d.Status = status
	}
	return d
}

// PublicMap returns the cloud-facing envelope. send_at and is_replayed are
// delivery metadata; time, data and event_instance_id remain unchanged during
// replay.
func (e Event) PublicMap(replayed bool, sendAt int64) map[string]interface{} {
	e.Data = e.Data.NormalizeLifecycle()
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
	if _, _, ok := EventLifecycle(e.Data.EventType); !ok {
		return nil, fmt.Errorf("unsupported event_type %q", e.Data.EventType)
	}
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
