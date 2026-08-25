package event

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
)

// EngineOptions configures the protocol-independent event engine.
type EngineOptions struct {
	Bindings []DeviceBinding
	Logger   logger.LoggingClient
	Now      func() time.Time
}

// Engine evaluates event profiles against normalized telemetry, property and
// connection observations. It is safe for concurrent observations from
// different protocol workers; each device has an independent state namespace.
type Engine struct {
	mu      sync.Mutex
	devices map[string]*compiledDevice
	log     logger.LoggingClient
	now     func() time.Time
}

type compiledDevice struct {
	binding    DeviceBinding
	hash       string
	rules      map[string]compiledRule
	categories []compiledCategory
	state      *runtimeDeviceState
}

type compiledCategory struct {
	name   string
	config CategoryConfig
	rules  []compiledRule
}

type compiledRule struct {
	category string
	rule     EventRule
	when     *Expression
	recover  *Expression
}

// NewEngine builds a validated engine from explicitly bound devices.
func NewEngine(options EngineOptions) (*Engine, error) {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	engine := &Engine{
		devices: make(map[string]*compiledDevice),
		log:     options.Logger,
		now:     now,
	}
	for _, binding := range options.Bindings {
		if strings.TrimSpace(binding.Device.InternalName) == "" {
			binding.Device.InternalName = strings.TrimSpace(binding.Device.Name)
		}
		binding.Profile.Config.Categories = normalizeCategories(binding.Profile.Config.Categories)
		if strings.TrimSpace(binding.Device.Name) == "" {
			return nil, fmt.Errorf("event binding has empty device name")
		}
		if err := ValidateForDevice(binding.Profile, binding.Device); err != nil {
			return nil, fmt.Errorf("device %s event profile %s: %w", binding.Device.Name, binding.Profile.Name, err)
		}
		compiled, err := compileDevice(binding)
		if err != nil {
			return nil, err
		}
		key := binding.Device.InternalName
		if strings.TrimSpace(key) == "" {
			key = binding.Device.Name
		}
		if _, exists := engine.devices[key]; exists {
			return nil, fmt.Errorf("duplicate event device binding %s", key)
		}
		engine.devices[key] = compiled
		if binding.Device.Name != key {
			if _, exists := engine.devices[binding.Device.Name]; !exists {
				engine.devices[binding.Device.Name] = compiled
			}
		}
	}
	return engine, nil
}

func compileDevice(binding DeviceBinding) (*compiledDevice, error) {
	compiled := &compiledDevice{
		binding: binding,
		hash:    ConfigHash(binding.Profile),
		rules:   make(map[string]compiledRule),
		state:   newRuntimeDeviceState(),
	}
	categoryNames := make([]string, 0, len(binding.Profile.Config.Categories))
	for name := range binding.Profile.Config.Categories {
		categoryNames = append(categoryNames, strings.ToLower(strings.TrimSpace(name)))
	}
	sort.Strings(categoryNames)
	for _, categoryName := range categoryNames {
		config := binding.Profile.Config.Categories[categoryName]
		category := compiledCategory{name: categoryName, config: config}
		for _, rule := range config.Events {
			when, err := ParseExpression(rule.When)
			if err != nil {
				return nil, fmt.Errorf("event %s when: %w", rule.EventCode, err)
			}
			recoverExpression, err := ParseExpression(rule.Recover)
			if err != nil {
				return nil, fmt.Errorf("event %s recover: %w", rule.EventCode, err)
			}
			compiledRule := compiledRule{category: categoryName, rule: rule, when: when, recover: recoverExpression}
			category.rules = append(category.rules, compiledRule)
			compiled.rules[strings.TrimSpace(rule.EventCode)] = compiledRule
		}
		compiled.categories = append(compiled.categories, category)
	}
	return compiled, nil
}

// ConfigHash returns the hash used by the engine's state namespace.
func (e *Engine) ConfigHash(deviceCode string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if device := e.devices[deviceCode]; device != nil {
		return device.hash
	}
	return ""
}

// ObserveTelemetry updates a device snapshot and evaluates all configured
// rules. The caller is responsible for delivering the returned events.
func (e *Engine) ObserveTelemetry(deviceCode string, collectedAt int64, values []*contracts.CommandValue) ([]Event, error) {
	return e.observe(deviceCode, collectedAt, values, nil, nil)
}

// ObserveProperty updates the optional property namespace used by expressions.
func (e *Engine) ObserveProperty(deviceCode string, observedAt int64, values map[string]interface{}) ([]Event, error) {
	return e.observe(deviceCode, observedAt, nil, values, nil)
}

// ObserveConnection feeds the SDK-standard connection state into the engine.
func (e *Engine) ObserveConnection(observation ConnectionObservation) ([]Event, error) {
	if strings.TrimSpace(observation.DeviceCode) == "" {
		return nil, fmt.Errorf("connection observation has empty device code")
	}
	if observation.ObservedAt == 0 {
		observation.ObservedAt = e.now().UnixMilli()
	}
	if !observation.Known {
		observation.Known = true
	}
	return e.observe(observation.DeviceCode, observation.ObservedAt, nil, nil, &observation)
}

func (e *Engine) observe(deviceCode string, observedAt int64, values []*contracts.CommandValue, properties map[string]interface{}, connection *ConnectionObservation) ([]Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	device := e.devices[deviceCode]
	if device == nil {
		return nil, fmt.Errorf("event device %s is not configured", deviceCode)
	}
	if observedAt == 0 {
		observedAt = e.now().UnixMilli()
	}
	state := device.state
	state.ensureMaps()
	if state.LastObservedAt > 0 && observedAt < state.LastObservedAt {
		// Protocol workers can race during reconnect. Preserve monotonic event
		// processing time while retaining the original collected time in the
		// telemetry transport.
		observedAt = state.LastObservedAt
	}
	e.accountDurations(device, observedAt)
	firstObservation := state.LastObservedAt == 0
	var previousConnection ConnectionObservation
	if connection != nil {
		previousConnection = state.Connection
		connection.ObservedAt = observedAt
		if connection.LastSeenAt == 0 && previousConnection.LastSeenAt > 0 {
			connection.LastSeenAt = previousConnection.LastSeenAt
		}
		state.Connection = *connection
		if state.Connection.DeviceCode == "" {
			state.Connection.DeviceCode = device.binding.Device.Name
		}
	}
	if len(values) > 0 {
		state.LastValues = cloneAnyMap(state.Values)
		for _, value := range values {
			if value == nil || strings.TrimSpace(value.DeviceResourceName) == "" {
				continue
			}
			state.Values[value.DeviceResourceName] = cloneAny(value.Value)
		}
	}
	if properties != nil {
		state.LastProperties = cloneAnyMap(state.Properties)
		for key, value := range properties {
			state.Properties[key] = cloneAny(value)
		}
	}
	state.LastObservedAt = observedAt

	var events []Event
	if connection != nil {
		events = append(events, e.connectionEvents(device, state, previousConnection, *connection, firstObservation)...)
	}
	events = append(events, e.evaluateRules(device, state, observedAt, firstObservation)...)
	return events, nil
}

// Flush emits completed summary windows. A window is removed after its event
// is returned, so runtime/event can immediately enqueue it into the durable
// event outbox.
func (e *Engine) Flush(nowMillis int64) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if nowMillis == 0 {
		nowMillis = e.now().UnixMilli()
	}
	var events []Event
	for _, device := range e.uniqueDevices() {
		if device.state.LastObservedAt > 0 && nowMillis > device.state.LastObservedAt {
			e.accountDurationsUntil(device, nowMillis)
			device.state.LastObservedAt = nowMillis
		}
		keys := make([]string, 0, len(device.state.DurationBuckets))
		for key := range device.state.DurationBuckets {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			bucket := device.state.DurationBuckets[key]
			if bucket.WindowEnd <= 0 || bucket.WindowEnd > nowMillis {
				continue
			}
			rule, ok := device.rules[bucket.EventCode]
			if ok && (bucket.DurationMs > 0 || rule.rule.Report.EmitEmpty) {
				events = append(events, e.summaryEvent(device, bucket, rule))
			}
			delete(device.state.DurationBuckets, key)
		}
	}
	return events
}

// FlushFinal emits completed windows and, for rules with flushOnStop enabled,
// the currently open partial window. It is intended for orderly service
// shutdown; ordinary periodic flushing must continue to wait for a window end.
func (e *Engine) FlushFinal(nowMillis int64) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if nowMillis == 0 {
		nowMillis = e.now().UnixMilli()
	}
	var events []Event
	for _, device := range e.uniqueDevices() {
		if device.state.LastObservedAt > 0 && nowMillis > device.state.LastObservedAt {
			e.accountDurationsUntil(device, nowMillis)
			device.state.LastObservedAt = nowMillis
		}
		keys := make([]string, 0, len(device.state.DurationBuckets))
		for key := range device.state.DurationBuckets {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			bucket := device.state.DurationBuckets[key]
			rule, ok := device.rules[bucket.EventCode]
			if !ok {
				continue
			}
			complete := bucket.WindowEnd <= nowMillis
			if !complete && !rule.rule.Report.FlushOnStop {
				continue
			}
			if !complete {
				bucket.WindowEnd = nowMillis
			}
			if bucket.DurationMs > 0 || rule.rule.Report.EmitEmpty {
				events = append(events, e.summaryEvent(device, bucket, rule))
			}
			delete(device.state.DurationBuckets, key)
		}
	}
	return events
}

// ExportState returns a JSON-compatible copy of all runtime state.
func (e *Engine) ExportState() PersistedState {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := PersistedState{Devices: make(map[string]PersistedDeviceState), ConfigHashes: make(map[string]string)}
	for key, device := range e.uniqueDevices() {
		result.Devices[key] = device.state.persisted()
		result.ConfigHashes[key] = device.hash
	}
	return result
}

// ImportState restores state only for currently configured devices. State from
// an incompatible config hash is intentionally ignored by runtime/event.
func (e *Engine) ImportState(state PersistedState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, value := range state.Devices {
		if device := e.devices[key]; device != nil {
			if storedHash := state.ConfigHashes[key]; storedHash != "" && storedHash != device.hash {
				continue
			}
			device.state.restore(value)
		}
	}
}

func (e *Engine) uniqueDevices() map[string]*compiledDevice {
	result := make(map[string]*compiledDevice)
	for key, device := range e.devices {
		deviceKey := strings.TrimSpace(device.binding.Device.InternalName)
		if deviceKey == "" {
			deviceKey = strings.TrimSpace(device.binding.Device.Name)
		}
		if deviceKey == "" {
			deviceKey = key
		}
		result[deviceKey] = device
	}
	return result
}

func (e *Engine) connectionEvents(device *compiledDevice, state *runtimeDeviceState, previous, observation ConnectionObservation, initial bool) []Event {
	// Connection transitions are determined from the previous normalized
	// observation. The active marker is kept separately so the two built-in
	// online/offline event codes can each use rise-clear semantics.
	currentCode := connectionEventCode(observation.Online)
	oldCode := ""
	if previous.Known {
		oldCode = connectionEventCode(previous.Online)
	}
	if previous.Known && oldCode == currentCode {
		state.ConnectInstances["__active"] = currentCode
		return nil
	}
	var events []Event
	if oldCode != "" && oldCode != currentCode {
		oldInstance := state.ConnectInstances[oldCode]
		if oldInstance != "" {
			events = append(events, e.standardConnectionEvent(device, observation, oldCode, EventActionClear, oldInstance, false))
		}
	}
	instanceID := state.ConnectInstances[currentCode]
	if instanceID == "" || oldCode != currentCode {
		instanceID = newEventInstanceID()
		state.ConnectInstances[currentCode] = instanceID
	}
	state.ConnectInstances["__active"] = currentCode
	if !previous.Known && !device.binding.Profile.Config.Initialization.EmitInitialState {
		return events
	}
	events = append(events, e.standardConnectionEvent(device, observation, currentCode, EventActionRaise, instanceID, !previous.Known || initial))
	return events
}

func connectionEventCode(online bool) string {
	if online {
		return "EVENT_CONNECT_ONLINE"
	}
	return "EVENT_CONNECT_OFFLINE"
}

func (e *Engine) standardConnectionEvent(device *compiledDevice, observation ConnectionObservation, code, action, instanceID string, initial bool) Event {
	data := map[string]interface{}{
		"online":           observation.Online,
		"connection_state": observation.State,
		"last_seen_at":     observation.LastSeenAt,
	}
	if observation.Error != "" {
		data["error"] = observation.Error
	}
	message := "device offline"
	if observation.Online {
		message = "device online"
	}
	meta := map[string]interface{}{
		"config_hash":  device.hash,
		"rule_type":    EventTypeRiseClear,
		"time_quality": timeQualityForProfile(device.binding.Profile, initial),
		"processed_at": e.now().UnixMilli(),
	}
	return Event{
		Time:        observation.ObservedAt,
		DeviceCode:  device.binding.Device.Name,
		ProductCode: device.binding.Device.ProductCode,
		TraceID:     newEventTraceID(device.binding.Device.Name),
		Data: EventData{
			EventIdentifier: code,
			Category:        CategoryConnect,
			Level:           "info",
			EventType:       action,
			Phase:           lifecyclePhase(action),
			Status:          lifecycleStatus(action),
			Message:         message,
			Payload:         data,
			Meta:            meta,
			EventInstanceID: instanceID,
		},
	}
}

func (e *Engine) evaluateRules(device *compiledDevice, state *runtimeDeviceState, observedAt int64, initial bool) []Event {
	ctx := state.evalContext()
	var events []Event
	for _, category := range device.categories {
		if category.name == CategoryConnect {
			continue
		}
		if strings.EqualFold(category.config.StateModel, "exclusive") {
			events = append(events, e.evaluateExclusive(device, state, category, ctx, observedAt, initial)...)
			continue
		}
		for _, rule := range category.rules {
			events = append(events, e.evaluateIndependent(device, state, rule, ctx, observedAt, initial)...)
		}
	}
	return events
}

func (e *Engine) evaluateIndependent(device *compiledDevice, state *runtimeDeviceState, compiled compiledRule, ctx EvalContext, observedAt int64, initial bool) []Event {
	rule := compiled.rule
	stateKey := strings.TrimSpace(rule.EventCode)
	runtimeState := state.Rules[stateKey]
	condition, known := evaluateRuleCondition(compiled, ctx)
	if strings.EqualFold(rule.EventType, EventTypePulse) {
		wasTrue := runtimeState.ConditionKnown && runtimeState.Condition
		runtimeState.ConditionKnown = known
		runtimeState.Condition = condition
		runtimeState.Initialized = true
		state.Rules[stateKey] = runtimeState
		if !known || !condition || wasTrue || (initial && !device.binding.Profile.Config.Initialization.EmitInitialState) {
			return nil
		}
		return []Event{e.ruleEvent(device, state, compiled, observedAt, EventTypePulse, newEventInstanceID(), timeQualityForProfile(device.binding.Profile, initial), nil)}
	}

	if known && condition {
		runtimeState.RecoverySince = 0
		if !runtimeState.Active {
			if runtimeState.PendingSince == 0 {
				runtimeState.PendingSince = observedAt
			}
			if observedAt-runtimeState.PendingSince < rule.HoldDuration().Milliseconds() {
				runtimeState.Initialized = true
				state.Rules[stateKey] = runtimeState
				return nil
			}
			runtimeState.Active = true
			runtimeState.ActiveSince = observedAt
			runtimeState.LastTransitionAt = observedAt
			runtimeState.PendingSince = 0
			runtimeState.InstanceID = newEventInstanceID()
			runtimeState.Initialized = true
			codes := aggregateCodes(rule, state.Values)
			runtimeState.LastCodes = codes
			state.Rules[stateKey] = runtimeState
			e.markSummaryTransition(device, state, compiled, observedAt)
			if initial && !device.binding.Profile.Config.Initialization.EmitInitialState {
				return nil
			}
			if rule.IsSummary() {
				return nil
			}
			return []Event{e.ruleEvent(device, state, compiled, observedAt, EventActionRaise, runtimeState.InstanceID, timeQualityForProfile(device.binding.Profile, initial), aggregatePayload(rule, codes, true))}
		}
		previousCodes := append([]string(nil), runtimeState.LastCodes...)
		currentCodes := aggregateCodes(rule, state.Values)
		if runtimeState.Active && rule.Aggregate != nil && strings.EqualFold(strings.TrimSpace(rule.Aggregate.CodeSetChange), "update") && !sameStringSet(previousCodes, currentCodes) {
			runtimeState.LastCodes = currentCodes
			state.Rules[stateKey] = runtimeState
			if !rule.IsSummary() && strings.EqualFold(rule.Aggregate.UpdateType, EventTypePulse) {
				return []Event{e.ruleEvent(device, state, compiled, observedAt, EventTypePulse, instanceIDOrNew(runtimeState.InstanceID), timeQuality(false), aggregatePayload(rule, currentCodes, true))}
			}
		}
		state.Rules[stateKey] = runtimeState
		return nil
	}

	if runtimeState.Active {
		recovered, recoveryKnown := evaluateRuleRecovery(compiled, ctx)
		if !recoveryKnown || !recovered {
			runtimeState.RecoverySince = 0
			state.Rules[stateKey] = runtimeState
			return nil
		}
		if runtimeState.RecoverySince == 0 {
			runtimeState.RecoverySince = observedAt
		}
		if observedAt-runtimeState.RecoverySince < rule.RecoverDuration().Milliseconds() {
			state.Rules[stateKey] = runtimeState
			return nil
		}
		runtimeState.Active = false
		runtimeState.LastTransitionAt = observedAt
		runtimeState.ActiveSince = 0
		instanceID := runtimeState.InstanceID
		runtimeState.InstanceID = ""
		runtimeState.LastCodes = nil
		runtimeState.Initialized = true
		state.Rules[stateKey] = runtimeState
		if rule.IsSummary() {
			return nil
		}
		return []Event{e.ruleEvent(device, state, compiled, observedAt, EventActionClear, instanceID, timeQuality(false), aggregatePayload(rule, nil, false))}
	}
	// Unknown input does not clear an inactive alarm or create a false clear.
	// An active rule is handled above through its explicit recovery expression.
	if !known {
		return nil
	}
	runtimeState.PendingSince = 0
	runtimeState.Initialized = true
	state.Rules[stateKey] = runtimeState
	return nil
}

func (e *Engine) evaluateExclusive(device *compiledDevice, state *runtimeDeviceState, category compiledCategory, ctx EvalContext, observedAt int64, initial bool) []Event {
	group := strings.TrimSpace(category.config.ExclusiveGroup)
	if group == "" {
		group = category.name
	}
	key := category.name + ":" + group
	exclusive := state.Exclusives[key]
	winner, hasWinner := chooseWinner(category, ctx)
	if !exclusive.Initialized {
		exclusive.Initialized = true
		if !hasWinner {
			state.Exclusives[key] = exclusive
			return nil
		}
		if winner.rule.HoldDuration() > 0 {
			exclusive.PendingCode = winner.rule.EventCode
			exclusive.PendingSince = observedAt
			state.Exclusives[key] = exclusive
			return nil
		}
		exclusive.ActiveCode = winner.rule.EventCode
		exclusive.InstanceID = newEventInstanceID()
		exclusive.ActiveSince = observedAt
		exclusive.LastTransitionAt = observedAt
		state.Exclusives[key] = exclusive
		e.markSummaryTransition(device, state, winner, observedAt)
		if initial && !device.binding.Profile.Config.Initialization.EmitInitialState {
			return nil
		}
		if winner.rule.IsSummary() {
			return nil
		}
		return []Event{e.ruleEvent(device, state, winner, observedAt, EventActionRaise, exclusive.InstanceID, timeQualityForProfile(device.binding.Profile, initial), nil)}
	}

	if hasWinner && winner.rule.EventCode == exclusive.ActiveCode {
		exclusive.PendingCode = ""
		exclusive.PendingSince = 0
		exclusive.InvalidSince = 0
		state.Exclusives[key] = exclusive
		return nil
	}
	if hasWinner {
		if exclusive.PendingCode != winner.rule.EventCode {
			exclusive.PendingCode = winner.rule.EventCode
			exclusive.PendingSince = observedAt
		}
		if observedAt-exclusive.PendingSince < winner.rule.HoldDuration().Milliseconds() {
			state.Exclusives[key] = exclusive
			return nil
		}
	}
	if exclusive.ActiveCode != "" {
		activeRule, ok := device.rules[exclusive.ActiveCode]
		if ok {
			recovered, recoveryKnown := evaluateRuleRecovery(activeRule, ctx)
			if recoveryKnown && recovered {
				if exclusive.InvalidSince == 0 {
					exclusive.InvalidSince = observedAt
				}
				if observedAt-exclusive.InvalidSince < activeRule.rule.RecoverDuration().Milliseconds() {
					state.Exclusives[key] = exclusive
					return nil
				}
				if !hasWinner {
					oldInstance := exclusive.InstanceID
					exclusive.ActiveCode = ""
					exclusive.InstanceID = ""
					exclusive.ActiveSince = 0
					exclusive.LastTransitionAt = observedAt
					exclusive.PendingCode = ""
					exclusive.PendingSince = 0
					exclusive.InvalidSince = 0
					state.Exclusives[key] = exclusive
					if !activeRule.rule.IsSummary() {
						return []Event{e.ruleEvent(device, state, activeRule, observedAt, EventActionClear, oldInstance, timeQuality(false), nil)}
					}
					return nil
				}
			} else if hasWinner {
				// A higher-priority candidate may replace a still-valid lower
				// priority state after its own hold duration.
				exclusive.InvalidSince = 0
			} else {
				// Missing or non-recovering input must not turn a valid active
				// state into a clear. The explicit fallback rule, when desired,
				// is responsible for entering unknown.
				state.Exclusives[key] = exclusive
				return nil
			}
		}
	}
	if !hasWinner {
		state.Exclusives[key] = exclusive
		return nil
	}
	oldCode := exclusive.ActiveCode
	oldInstance := exclusive.InstanceID
	newCode := winner.rule.EventCode
	newInstance := newEventInstanceID()
	exclusive.ActiveCode = newCode
	exclusive.InstanceID = newInstance
	exclusive.ActiveSince = observedAt
	exclusive.LastTransitionAt = observedAt
	exclusive.PendingCode = ""
	exclusive.PendingSince = 0
	exclusive.InvalidSince = 0
	state.Exclusives[key] = exclusive
	e.markSummaryTransition(device, state, winner, observedAt)

	var events []Event
	if oldCode != "" {
		if oldRule, ok := device.rules[oldCode]; ok && !oldRule.rule.IsSummary() {
			events = append(events, e.ruleEvent(device, state, oldRule, observedAt, EventActionClear, oldInstance, timeQuality(false), nil))
		}
	}
	if !winner.rule.IsSummary() {
		events = append(events, e.ruleEvent(device, state, winner, observedAt, EventActionRaise, newInstance, timeQuality(false), nil))
	}
	return events
}

func chooseWinner(category compiledCategory, ctx EvalContext) (compiledRule, bool) {
	fallback := compiledRule{}
	hasFallback := false
	for _, rule := range category.rules {
		if rule.rule.Fallback {
			if !hasFallback {
				fallback = rule
				hasFallback = true
			}
			continue
		}
		condition, known := evaluateRuleCondition(rule, ctx)
		if !known || !condition {
			continue
		}
		// Priority is resolved below; collect by returning the first rule only
		// when no explicit priority exists.
		if len(category.config.Priority) == 0 {
			return rule, true
		}
	}
	if len(category.config.Priority) > 0 {
		for _, stateName := range category.config.Priority {
			for _, rule := range category.rules {
				if rule.rule.Fallback || !strings.EqualFold(strings.TrimSpace(rule.rule.State), strings.TrimSpace(stateName)) {
					continue
				}
				condition, known := evaluateRuleCondition(rule, ctx)
				if known && condition {
					return rule, true
				}
			}
		}
		for _, rule := range category.rules {
			if rule.rule.Fallback {
				continue
			}
			condition, known := evaluateRuleCondition(rule, ctx)
			if known && condition {
				return rule, true
			}
		}
	}
	if hasFallback {
		return fallback, true
	}
	return compiledRule{}, false
}

func evaluateRuleCondition(rule compiledRule, ctx EvalContext) (bool, bool) {
	if rule.rule.Fallback && strings.TrimSpace(rule.rule.When) == "" {
		return true, true
	}
	if rule.when == nil {
		return false, false
	}
	return rule.when.Evaluate(ctx)
}

func evaluateRuleRecovery(rule compiledRule, ctx EvalContext) (bool, bool) {
	if rule.recover != nil {
		return rule.recover.Evaluate(ctx)
	}
	if rule.when == nil {
		return false, false
	}
	condition, known := rule.when.Evaluate(ctx)
	if !known {
		return false, false
	}
	return !condition, true
}

func (e *Engine) ruleEvent(device *compiledDevice, state *runtimeDeviceState, compiled compiledRule, observedAt int64, eventType, instanceID, quality string, extra map[string]interface{}) Event {
	payload := e.buildPayload(device, state, compiled.rule.Payload)
	if payload == nil {
		payload = make(map[string]interface{})
	}
	for key, value := range extra {
		payload[key] = cloneAny(value)
	}
	message := compiled.rule.Message
	if message == "" && compiled.rule.Aggregate != nil {
		codes := aggregateCodes(compiled.rule, state.Values)
		message = strings.Join(codes, compiled.rule.Aggregate.Generated.Delimiter)
	}
	if message == "" {
		message = compiled.rule.Name
	}
	if message == "" {
		message = compiled.rule.EventCode
	}
	meta := map[string]interface{}{
		"config_hash":  device.hash,
		"rule_name":    compiled.rule.Name,
		"time_quality": quality,
		"processed_at": e.now().UnixMilli(),
		"rule_type":    compiled.rule.EventType,
		"report_mode":  effectiveReportMode(compiled.rule.Report),
	}
	if compiled.rule.State != "" {
		meta["state"] = compiled.rule.State
	}
	return Event{
		Time:        observedAt,
		DeviceCode:  device.binding.Device.Name,
		ProductCode: device.binding.Device.ProductCode,
		TraceID:     newEventTraceID(device.binding.Device.Name),
		Data: EventData{
			EventIdentifier: compiled.rule.EventCode,
			Category:        compiled.category,
			Level:           compiled.rule.Level,
			EventType:       eventType,
			Phase:           lifecyclePhase(eventType),
			Status:          lifecycleStatus(eventType),
			Message:         message,
			Payload:         payload,
			Meta:            meta,
			EventInstanceID: instanceID,
		},
	}
}

func (e *Engine) summaryEvent(device *compiledDevice, bucket durationBucket, rule compiledRule) Event {
	payload := e.buildPayload(device, device.state, rule.rule.Payload)
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload["report_mode"] = ReportModeSummary
	payload["active_duration_ms"] = bucket.DurationMs
	payload["duration_ms"] = bucket.DurationMs
	payload["window_start"] = bucket.WindowStart
	payload["window_end"] = bucket.WindowEnd
	payload["transition_count"] = bucket.TransitionCount
	payload["initial_state"] = bucket.FirstState
	payload["final_state"] = bucket.FinalState
	payload["first_active_at"] = bucket.FirstActiveAt
	payload["last_active_at"] = bucket.LastActiveAt
	meta := map[string]interface{}{
		"config_hash":     device.hash,
		"rule_name":       rule.rule.Name,
		"time_quality":    "window",
		"rule_type":       rule.rule.EventType,
		"report_mode":     ReportModeSummary,
		"window_timezone": device.binding.Profile.Config.Time.Timezone,
		"window_start":    bucket.WindowStart,
		"window_end":      bucket.WindowEnd,
		"duration_ms":     bucket.DurationMs,
	}
	return Event{
		Time:        bucket.WindowEnd,
		DeviceCode:  device.binding.Device.Name,
		ProductCode: device.binding.Device.ProductCode,
		TraceID:     newEventTraceID(device.binding.Device.Name),
		Data: EventData{
			EventIdentifier: rule.rule.EventCode,
			Category:        rule.category,
			Level:           rule.rule.Level,
			EventType:       EventTypePulse,
			Phase:           EventPhaseRecord,
			Status:          EventStatusRecorded,
			Message:         summaryMessage(rule.rule),
			Payload:         payload,
			Meta:            meta,
			EventInstanceID: summaryInstanceID(device.binding.Device.Name, rule.category, rule.rule.EventCode, bucket.WindowStart, bucket.WindowEnd),
		},
	}
}

func (e *Engine) buildPayload(device *compiledDevice, state *runtimeDeviceState, selector PayloadSelector) map[string]interface{} {
	if len(selector.Groups) == 0 && len(selector.Points) == 0 {
		return nil
	}
	payload := make(map[string]interface{})
	if len(selector.Groups) > 0 {
		groups := make(map[string]interface{})
		for _, groupName := range selector.Groups {
			groupName = strings.TrimSpace(groupName)
			if groupName == "" {
				continue
			}
			groupData := make(map[string]interface{})
			for _, group := range device.binding.Device.Telemetry.Groups {
				if group.Name != groupName {
					continue
				}
				for _, point := range group.Points {
					if value, ok := state.Values[point.Name]; ok {
						groupData[point.Name] = cloneAny(value)
					}
				}
				for _, item := range group.Structs {
					if value, ok := state.Values[item.Name]; ok {
						groupData[item.Name] = cloneAny(value)
					}
				}
			}
			groups[groupName] = groupData
		}
		payload["groups"] = groups
	}
	if len(selector.Points) > 0 {
		points := make(map[string]interface{})
		for _, name := range selector.Points {
			if value, ok := state.Values[strings.TrimSpace(name)]; ok {
				points[strings.TrimSpace(name)] = cloneAny(value)
			}
		}
		payload["points"] = points
	}
	return payload
}

func aggregateCodes(rule EventRule, values map[string]interface{}) []string {
	if rule.Aggregate == nil {
		return nil
	}
	raw, ok := values[fieldName(rule.Aggregate.CodeField)]
	if !ok {
		return nil
	}
	items := flattenCodeValues(raw)
	zero := normalizeCodeValue(rule.Aggregate.ZeroValue)
	seen := make(map[string]struct{})
	result := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || (zero != "" && item == zero) {
			continue
		}
		prefix := rule.Aggregate.Code.Prefix
		if prefix != "" && !strings.HasPrefix(item, prefix) {
			item = prefix + item
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

func flattenCodeValues(value interface{}) []string {
	if value == nil {
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		result := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result = append(result, flattenCodeValues(rv.Index(i).Interface())...)
		}
		return result
	}
	if text, ok := value.(string); ok {
		parts := strings.FieldsFunc(text, func(r rune) bool { return r == '|' || r == ',' || r == ';' })
		return parts
	}
	return []string{normalizeCodeValue(value)}
}

func normalizeCodeValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func aggregatePayload(rule EventRule, codes []string, active bool) map[string]interface{} {
	if rule.Aggregate == nil {
		return nil
	}
	generated := rule.Aggregate.Generated
	delimiter := generated.Delimiter
	if delimiter == "" {
		delimiter = "|"
	}
	result := make(map[string]interface{})
	if generated.Codes != "" {
		result[generated.Codes] = append([]string(nil), codes...)
	}
	if generated.Text != "" {
		result[generated.Text] = strings.Join(codes, delimiter)
	}
	if generated.Count != "" {
		result[generated.Count] = len(codes)
	}
	if generated.Active != "" {
		result[generated.Active] = active
	}
	return result
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}
	return true
}

func instanceIDOrNew(value string) string {
	if strings.TrimSpace(value) == "" {
		return newEventInstanceID()
	}
	return value
}

func timeQuality(initial bool) string {
	if initial {
		return "inferred"
	}
	return "observed"
}

func timeQualityForProfile(profile EventProfileFile, initial bool) string {
	if !initial {
		return "observed"
	}
	if value := strings.TrimSpace(profile.Config.Initialization.InitialTimeQuality); value != "" {
		return value
	}
	return timeQuality(true)
}

func newEventInstanceID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("evt_%d", time.Now().UnixNano())
	}
	return "evt_" + hex.EncodeToString(buffer)
}

func newEventTraceID(device string) string {
	return fmt.Sprintf("%s-event-%d", strings.TrimSpace(device), time.Now().UnixNano())
}

func summaryInstanceID(device, category, code string, start, end int64) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%d", device, category, code, start, end)))
	return "evt_" + hex.EncodeToString(hash[:12])
}

func (s *runtimeDeviceState) evalContext() EvalContext {
	connection := map[string]interface{}{
		"online":       s.Connection.Online,
		"state":        s.Connection.State,
		"last_seen_at": s.Connection.LastSeenAt,
		"known":        s.Connection.Known,
	}
	return EvalContext{Data: s.Values, LastValue: s.LastValues, Property: s.Properties, Connection: connection}
}

func (e *Engine) accountDurations(device *compiledDevice, observedAt int64) {
	from := device.state.LastObservedAt
	if from <= 0 || observedAt <= from {
		return
	}
	e.accountDurationRange(device, from, observedAt)
}

func (e *Engine) accountDurationsUntil(device *compiledDevice, observedAt int64) {
	from := device.state.LastObservedAt
	if from <= 0 || observedAt <= from {
		return
	}
	e.accountDurationRange(device, from, observedAt)
}

func (e *Engine) accountDurationRange(device *compiledDevice, from, to int64) {
	for _, code := range device.state.activeCodes() {
		rule, ok := device.rules[code]
		if !ok || !rule.rule.IsSummary() {
			continue
		}
		window := reportWindow(rule.rule)
		if window <= 0 {
			continue
		}
		cursor := from
		if activeSince := device.state.activeSince(code); activeSince > cursor {
			cursor = activeSince
		}
		for cursor < to {
			windowStart, windowEnd := eventWindow(device.binding.Profile, rule.rule.Report, cursor, window)
			segmentEnd := to
			if windowEnd < segmentEnd {
				segmentEnd = windowEnd
			}
			if segmentEnd <= cursor {
				break
			}
			key := durationBucketKey(rule.category, code, windowStart)
			bucket := device.state.DurationBuckets[key]
			if bucket.Category == "" {
				bucket = durationBucket{Category: rule.category, EventCode: code, WindowStart: windowStart, WindowEnd: windowEnd, FirstState: rule.rule.State, FinalState: rule.rule.State}
			}
			bucket.DurationMs += segmentEnd - cursor
			if bucket.FirstActiveAt == 0 || cursor < bucket.FirstActiveAt {
				bucket.FirstActiveAt = cursor
			}
			if segmentEnd > bucket.LastActiveAt {
				bucket.LastActiveAt = segmentEnd
			}
			bucket.FinalState = rule.rule.State
			device.state.DurationBuckets[key] = bucket
			cursor = segmentEnd
		}
	}
}

func (e *Engine) markSummaryTransition(device *compiledDevice, state *runtimeDeviceState, rule compiledRule, observedAt int64) {
	if !rule.rule.IsSummary() {
		return
	}
	window := reportWindow(rule.rule)
	if window <= 0 {
		return
	}
	start, end := eventWindow(device.binding.Profile, rule.rule.Report, observedAt, window)
	key := durationBucketKey(rule.category, rule.rule.EventCode, start)
	bucket := state.DurationBuckets[key]
	if bucket.Category == "" {
		bucket = durationBucket{Category: rule.category, EventCode: rule.rule.EventCode, WindowStart: start, WindowEnd: end, FirstState: rule.rule.State, FinalState: rule.rule.State}
	}
	bucket.TransitionCount++
	if bucket.FirstActiveAt == 0 {
		bucket.FirstActiveAt = observedAt
	}
	if observedAt > bucket.LastActiveAt {
		bucket.LastActiveAt = observedAt
	}
	bucket.FinalState = rule.rule.State
	state.DurationBuckets[key] = bucket
}

func reportWindow(rule EventRule) time.Duration {
	if duration := parseDurationOrZero(rule.Report.Window); duration > 0 {
		return duration
	}
	return parseDurationOrZero(rule.Report.Interval)
}

func effectiveReportMode(report ReportConfig) string {
	if strings.EqualFold(strings.TrimSpace(report.Mode), ReportModeSummary) {
		return ReportModeSummary
	}
	return ReportModeImmediate
}

func summaryMessage(rule EventRule) string {
	if strings.TrimSpace(rule.Message) != "" {
		return rule.Message
	}
	if strings.TrimSpace(rule.Name) != "" {
		return rule.Name
	}
	return rule.EventCode
}

func eventWindow(profile EventProfileFile, report ReportConfig, timestamp int64, window time.Duration) (int64, int64) {
	location := eventLocation(profile.Config.Time.Timezone)
	value := time.UnixMilli(timestamp).In(location)
	base := time.Unix(0, 0).In(location)
	if strings.EqualFold(strings.TrimSpace(report.Align), "business_day") {
		base = businessDayStart(value, profile.Config.Time.BusinessDayStart)
	}
	elapsed := value.Sub(base)
	if elapsed < 0 {
		base = base.Add(-24 * time.Hour)
		elapsed = value.Sub(base)
	}
	steps := elapsed / window
	start := base.Add(steps * window)
	end := start.Add(window)
	return start.UnixMilli(), end.UnixMilli()
}

func eventLocation(raw string) *time.Location {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Local
	}
	location, err := time.LoadLocation(raw)
	if err != nil {
		return time.Local
	}
	return location
}

func businessDayStart(value time.Time, raw string) time.Time {
	hour, minute, second := 0, 0, 0
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) >= 2 {
		hour, _ = strconv.Atoi(parts[0])
		minute, _ = strconv.Atoi(parts[1])
		if len(parts) >= 3 {
			second, _ = strconv.Atoi(parts[2])
		}
	}
	start := time.Date(value.Year(), value.Month(), value.Day(), hour, minute, second, 0, value.Location())
	if value.Before(start) {
		return start.Add(-24 * time.Hour)
	}
	return start
}
