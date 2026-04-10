package status

import (
	"sort"
	"sync"
	"time"
)

const (
	StateUnknown      = "unknown"
	StateConnected    = "connected"
	StateDegraded     = "degraded"
	StateDisconnected = "disconnected"
)

// DeviceState is the runtime-visible device connection state.
type DeviceState struct {
	DeviceCode      string `json:"device_code"`
	ProductCode     string `json:"product_code"`
	ConnectionState string `json:"connection_state"`
	Connected       bool   `json:"connected"`
	LastConnectedAt int64  `json:"last_connected_at"`
	LastReadAt      int64  `json:"last_read_at"`
	LastWriteAt     int64  `json:"last_write_at"`
	LastSuccessAt   int64  `json:"last_success_at"`
	LastError       string `json:"last_error"`
	LastErrorAt     int64  `json:"last_error_at"`
}

// Tracker maintains device status for HTTP and MQTT reporting.
type Tracker struct {
	mu       sync.RWMutex
	states   map[string]DeviceState
	onChange func([]DeviceState)
}

// NewTracker creates a tracker.
func NewTracker() *Tracker {
	return &Tracker{
		states: make(map[string]DeviceState),
	}
}

// SetOnChange registers a callback invoked after state changes.
func (t *Tracker) SetOnChange(fn func([]DeviceState)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onChange = fn
}

// RegisterDevice initializes a device entry with unknown state.
func (t *Tracker) RegisterDevice(deviceCode, productCode string) {
	t.update(deviceCode, func(state *DeviceState) bool {
		changed := false
		if state.DeviceCode != deviceCode {
			state.DeviceCode = deviceCode
			changed = true
		}
		if state.ProductCode != productCode {
			state.ProductCode = productCode
			changed = true
		}
		if state.ConnectionState == "" {
			state.ConnectionState = StateUnknown
			changed = true
		}
		return changed
	})
}

// MarkConnected records a healthy connection event.
func (t *Tracker) MarkConnected(deviceCode string) {
	now := nowMillis()
	t.update(deviceCode, func(state *DeviceState) bool {
		changed := false
		if state.ConnectionState != StateConnected {
			state.ConnectionState = StateConnected
			changed = true
		}
		if !state.Connected {
			state.Connected = true
			changed = true
		}
		if state.LastConnectedAt != now {
			state.LastConnectedAt = now
			changed = true
		}
		if state.LastSuccessAt != now {
			state.LastSuccessAt = now
			changed = true
		}
		if state.LastError != "" {
			state.LastError = ""
			changed = true
		}
		return changed
	})
}

// MarkReadSuccess records a successful device read.
func (t *Tracker) MarkReadSuccess(deviceCode string) {
	now := nowMillis()
	t.update(deviceCode, func(state *DeviceState) bool {
		changed := false
		if state.ConnectionState != StateConnected {
			state.ConnectionState = StateConnected
			changed = true
		}
		if !state.Connected {
			state.Connected = true
			changed = true
		}
		if state.LastReadAt != now {
			state.LastReadAt = now
			changed = true
		}
		if state.LastSuccessAt != now {
			state.LastSuccessAt = now
			changed = true
		}
		if state.LastError != "" {
			state.LastError = ""
			changed = true
		}
		return changed
	})
}

// MarkWriteSuccess records a successful device write.
func (t *Tracker) MarkWriteSuccess(deviceCode string) {
	now := nowMillis()
	t.update(deviceCode, func(state *DeviceState) bool {
		changed := false
		if state.ConnectionState != StateConnected {
			state.ConnectionState = StateConnected
			changed = true
		}
		if !state.Connected {
			state.Connected = true
			changed = true
		}
		if state.LastWriteAt != now {
			state.LastWriteAt = now
			changed = true
		}
		if state.LastSuccessAt != now {
			state.LastSuccessAt = now
			changed = true
		}
		if state.LastError != "" {
			state.LastError = ""
			changed = true
		}
		return changed
	})
}

// MarkReadError records a read failure.
func (t *Tracker) MarkReadError(deviceCode string, err error) {
	t.markError(deviceCode, err, true)
}

// MarkWriteError records a write failure.
func (t *Tracker) MarkWriteError(deviceCode string, err error) {
	t.markError(deviceCode, err, true)
}

// MarkDisconnected records a connection failure.
func (t *Tracker) MarkDisconnected(deviceCode string, err error) {
	t.markError(deviceCode, err, false)
}

// Snapshot returns a sorted copy of all device states.
func (t *Tracker) Snapshot() []DeviceState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return cloneStates(t.states)
}

func (t *Tracker) markError(deviceCode string, err error, degraded bool) {
	now := nowMillis()
	message := ""
	if err != nil {
		message = err.Error()
	}
	targetState := StateDisconnected
	if degraded {
		targetState = StateDegraded
	}

	t.update(deviceCode, func(state *DeviceState) bool {
		changed := false
		if state.ConnectionState != targetState {
			state.ConnectionState = targetState
			changed = true
		}
		if state.Connected {
			state.Connected = false
			changed = true
		}
		if state.LastError != message {
			state.LastError = message
			changed = true
		}
		if state.LastErrorAt != now {
			state.LastErrorAt = now
			changed = true
		}
		return changed
	})
}

func (t *Tracker) update(deviceCode string, mutate func(*DeviceState) bool) {
	if t == nil || deviceCode == "" || mutate == nil {
		return
	}

	var (
		changed bool
		cb      func([]DeviceState)
		snap    []DeviceState
	)

	t.mu.Lock()
	state := t.states[deviceCode]
	if state.DeviceCode == "" {
		state.DeviceCode = deviceCode
		if state.ConnectionState == "" {
			state.ConnectionState = StateUnknown
		}
	}
	changed = mutate(&state)
	if changed {
		t.states[deviceCode] = state
		cb = t.onChange
		if cb != nil {
			snap = cloneStates(t.states)
		}
	}
	t.mu.Unlock()

	if changed && cb != nil {
		cb(snap)
	}
}

func cloneStates(states map[string]DeviceState) []DeviceState {
	items := make([]DeviceState, 0, len(states))
	for _, state := range states {
		items = append(items, state)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ProductCode == items[j].ProductCode {
			return items[i].DeviceCode < items[j].DeviceCode
		}
		return items[i].ProductCode < items[j].ProductCode
	})
	return items
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
