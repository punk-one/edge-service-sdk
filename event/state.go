package event

import (
	"fmt"
	"strings"
)

type runtimeDeviceState struct {
	Values           map[string]interface{}
	LastValues       map[string]interface{}
	Properties       map[string]interface{}
	LastProperties   map[string]interface{}
	Connection       ConnectionObservation
	Rules            map[string]runtimeRuleState
	Exclusives       map[string]runtimeExclusiveState
	ConnectInstances map[string]string
	LastObservedAt   int64
	DurationBuckets  map[string]durationBucket
}

type runtimeRuleState struct {
	Active           bool
	ConditionKnown   bool
	Condition        bool
	Initialized      bool
	ActiveSince      int64
	LastTransitionAt int64
	PendingSince     int64
	RecoverySince    int64
	InstanceID       string
	LastCodes        []string
}

type runtimeExclusiveState struct {
	ActiveCode       string
	InstanceID       string
	ActiveSince      int64
	LastTransitionAt int64
	PendingCode      string
	PendingSince     int64
	InvalidSince     int64
	Initialized      bool
}

type durationBucket struct {
	Category        string
	EventCode       string
	WindowStart     int64
	WindowEnd       int64
	DurationMs      int64
	TransitionCount int
	FirstState      string
	FinalState      string
	FirstActiveAt   int64
	LastActiveAt    int64
}

// PersistedState is the JSON-compatible state boundary used by runtime/event.
// Compiled expressions and static event profile data are intentionally not
// persisted; they are rebuilt from the current config hash on startup.
type PersistedState struct {
	Devices      map[string]PersistedDeviceState `json:"devices"`
	ConfigHashes map[string]string               `json:"config_hashes,omitempty"`
}

type PersistedDeviceState struct {
	Values           map[string]interface{}             `json:"values,omitempty"`
	LastValues       map[string]interface{}             `json:"last_values,omitempty"`
	Properties       map[string]interface{}             `json:"properties,omitempty"`
	LastProperties   map[string]interface{}             `json:"last_properties,omitempty"`
	Connection       ConnectionObservationState         `json:"connection"`
	Rules            map[string]PersistedRuleState      `json:"rules,omitempty"`
	Exclusives       map[string]PersistedExclusiveState `json:"exclusives,omitempty"`
	ConnectInstances map[string]string                  `json:"connect_instances,omitempty"`
	LastObservedAt   int64                              `json:"last_observed_at"`
	DurationBuckets  []PersistedDurationBucket          `json:"duration_buckets,omitempty"`
}

type ConnectionObservationState struct {
	DeviceCode string `json:"device_code,omitempty"`
	Online     bool   `json:"online"`
	State      string `json:"state,omitempty"`
	ObservedAt int64  `json:"observed_at"`
	LastSeenAt int64  `json:"last_seen_at"`
	Error      string `json:"error,omitempty"`
	Known      bool   `json:"known"`
}

type PersistedRuleState struct {
	Active           bool     `json:"active"`
	ConditionKnown   bool     `json:"condition_known"`
	Condition        bool     `json:"condition"`
	Initialized      bool     `json:"initialized"`
	ActiveSince      int64    `json:"active_since"`
	LastTransitionAt int64    `json:"last_transition_at"`
	PendingSince     int64    `json:"pending_since"`
	RecoverySince    int64    `json:"recovery_since"`
	InstanceID       string   `json:"instance_id,omitempty"`
	LastCodes        []string `json:"last_codes,omitempty"`
}

type PersistedExclusiveState struct {
	ActiveCode       string `json:"active_code,omitempty"`
	InstanceID       string `json:"instance_id,omitempty"`
	ActiveSince      int64  `json:"active_since"`
	LastTransitionAt int64  `json:"last_transition_at"`
	PendingCode      string `json:"pending_code,omitempty"`
	PendingSince     int64  `json:"pending_since"`
	InvalidSince     int64  `json:"invalid_since"`
	Initialized      bool   `json:"initialized"`
}

type PersistedDurationBucket struct {
	Category        string `json:"category"`
	EventCode       string `json:"event_code"`
	WindowStart     int64  `json:"window_start"`
	WindowEnd       int64  `json:"window_end"`
	DurationMs      int64  `json:"duration_ms"`
	TransitionCount int    `json:"transition_count"`
	FirstState      string `json:"first_state,omitempty"`
	FinalState      string `json:"final_state,omitempty"`
	FirstActiveAt   int64  `json:"first_active_at"`
	LastActiveAt    int64  `json:"last_active_at"`
}

func newRuntimeDeviceState() *runtimeDeviceState {
	return &runtimeDeviceState{
		Values:           make(map[string]interface{}),
		LastValues:       make(map[string]interface{}),
		Properties:       make(map[string]interface{}),
		LastProperties:   make(map[string]interface{}),
		Rules:            make(map[string]runtimeRuleState),
		Exclusives:       make(map[string]runtimeExclusiveState),
		ConnectInstances: make(map[string]string),
		DurationBuckets:  make(map[string]durationBucket),
	}
}

func (s *runtimeDeviceState) ensureMaps() {
	if s.Values == nil {
		s.Values = make(map[string]interface{})
	}
	if s.LastValues == nil {
		s.LastValues = make(map[string]interface{})
	}
	if s.Properties == nil {
		s.Properties = make(map[string]interface{})
	}
	if s.LastProperties == nil {
		s.LastProperties = make(map[string]interface{})
	}
	if s.Rules == nil {
		s.Rules = make(map[string]runtimeRuleState)
	}
	if s.Exclusives == nil {
		s.Exclusives = make(map[string]runtimeExclusiveState)
	}
	if s.ConnectInstances == nil {
		s.ConnectInstances = make(map[string]string)
	}
	if s.DurationBuckets == nil {
		s.DurationBuckets = make(map[string]durationBucket)
	}
}

func (s *runtimeDeviceState) persisted() PersistedDeviceState {
	s.ensureMaps()
	result := PersistedDeviceState{
		Values:           cloneAnyMap(s.Values),
		LastValues:       cloneAnyMap(s.LastValues),
		Properties:       cloneAnyMap(s.Properties),
		LastProperties:   cloneAnyMap(s.LastProperties),
		Connection:       ConnectionObservationState{DeviceCode: s.Connection.DeviceCode, Online: s.Connection.Online, State: s.Connection.State, ObservedAt: s.Connection.ObservedAt, LastSeenAt: s.Connection.LastSeenAt, Error: s.Connection.Error, Known: s.Connection.Known},
		Rules:            make(map[string]PersistedRuleState, len(s.Rules)),
		Exclusives:       make(map[string]PersistedExclusiveState, len(s.Exclusives)),
		ConnectInstances: copyStringMap(s.ConnectInstances),
		LastObservedAt:   s.LastObservedAt,
		DurationBuckets:  make([]PersistedDurationBucket, 0, len(s.DurationBuckets)),
	}
	for key, value := range s.Rules {
		result.Rules[key] = PersistedRuleState{Active: value.Active, ConditionKnown: value.ConditionKnown, Condition: value.Condition, Initialized: value.Initialized, ActiveSince: value.ActiveSince, LastTransitionAt: value.LastTransitionAt, PendingSince: value.PendingSince, RecoverySince: value.RecoverySince, InstanceID: value.InstanceID, LastCodes: append([]string(nil), value.LastCodes...)}
	}
	for key, value := range s.Exclusives {
		result.Exclusives[key] = PersistedExclusiveState{ActiveCode: value.ActiveCode, InstanceID: value.InstanceID, ActiveSince: value.ActiveSince, LastTransitionAt: value.LastTransitionAt, PendingCode: value.PendingCode, PendingSince: value.PendingSince, InvalidSince: value.InvalidSince, Initialized: value.Initialized}
	}
	for _, value := range s.DurationBuckets {
		result.DurationBuckets = append(result.DurationBuckets, PersistedDurationBucket{Category: value.Category, EventCode: value.EventCode, WindowStart: value.WindowStart, WindowEnd: value.WindowEnd, DurationMs: value.DurationMs, TransitionCount: value.TransitionCount, FirstState: value.FirstState, FinalState: value.FinalState, FirstActiveAt: value.FirstActiveAt, LastActiveAt: value.LastActiveAt})
	}
	return result
}

func (s *runtimeDeviceState) restore(value PersistedDeviceState) {
	s.Values = cloneAnyMap(value.Values)
	s.LastValues = cloneAnyMap(value.LastValues)
	s.Properties = cloneAnyMap(value.Properties)
	s.LastProperties = cloneAnyMap(value.LastProperties)
	s.Connection = ConnectionObservation{DeviceCode: value.Connection.DeviceCode, Online: value.Connection.Online, State: value.Connection.State, ObservedAt: value.Connection.ObservedAt, LastSeenAt: value.Connection.LastSeenAt, Error: value.Connection.Error, Known: value.Connection.Known}
	s.Rules = make(map[string]runtimeRuleState, len(value.Rules))
	for key, item := range value.Rules {
		s.Rules[key] = runtimeRuleState{Active: item.Active, ConditionKnown: item.ConditionKnown, Condition: item.Condition, Initialized: item.Initialized, ActiveSince: item.ActiveSince, LastTransitionAt: item.LastTransitionAt, PendingSince: item.PendingSince, RecoverySince: item.RecoverySince, InstanceID: item.InstanceID, LastCodes: append([]string(nil), item.LastCodes...)}
	}
	s.Exclusives = make(map[string]runtimeExclusiveState, len(value.Exclusives))
	for key, item := range value.Exclusives {
		s.Exclusives[key] = runtimeExclusiveState{ActiveCode: item.ActiveCode, InstanceID: item.InstanceID, ActiveSince: item.ActiveSince, LastTransitionAt: item.LastTransitionAt, PendingCode: item.PendingCode, PendingSince: item.PendingSince, InvalidSince: item.InvalidSince, Initialized: item.Initialized}
	}
	s.ConnectInstances = copyStringMap(value.ConnectInstances)
	s.LastObservedAt = value.LastObservedAt
	s.DurationBuckets = make(map[string]durationBucket, len(value.DurationBuckets))
	for _, item := range value.DurationBuckets {
		key := durationBucketKey(item.Category, item.EventCode, item.WindowStart)
		s.DurationBuckets[key] = durationBucket{Category: item.Category, EventCode: item.EventCode, WindowStart: item.WindowStart, WindowEnd: item.WindowEnd, DurationMs: item.DurationMs, TransitionCount: item.TransitionCount, FirstState: item.FirstState, FinalState: item.FinalState, FirstActiveAt: item.FirstActiveAt, LastActiveAt: item.LastActiveAt}
	}
	s.ensureMaps()
}

func durationBucketKey(category, code string, start int64) string {
	return fmt.Sprintf("%s:%s:%d", category, code, start)
}

func cloneAnyMap(source map[string]interface{}) map[string]interface{} {
	if len(source) == 0 {
		return make(map[string]interface{})
	}
	result := make(map[string]interface{}, len(source))
	for key, value := range source {
		result[key] = cloneAny(value)
	}
	return result
}

func cloneAny(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneAnyMap(typed)
	case []interface{}:
		result := make([]interface{}, len(typed))
		for i := range typed {
			result[i] = cloneAny(typed[i])
		}
		return result
	default:
		return value
	}
}

func copyStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (s *runtimeDeviceState) activeCodes() []string {
	s.ensureMaps()
	set := make(map[string]struct{})
	for _, value := range s.Exclusives {
		if strings.TrimSpace(value.ActiveCode) != "" {
			set[value.ActiveCode] = struct{}{}
		}
	}
	for code, value := range s.Rules {
		if value.Active {
			set[code] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for code := range set {
		result = append(result, code)
	}
	return result
}

func (s *runtimeDeviceState) activeSince(code string) int64 {
	s.ensureMaps()
	var earliest int64
	for _, value := range s.Exclusives {
		if value.ActiveCode == code && value.ActiveSince > 0 && (earliest == 0 || value.ActiveSince < earliest) {
			earliest = value.ActiveSince
		}
	}
	if value, ok := s.Rules[code]; ok && value.Active && value.ActiveSince > 0 && (earliest == 0 || value.ActiveSince < earliest) {
		earliest = value.ActiveSince
	}
	return earliest
}
