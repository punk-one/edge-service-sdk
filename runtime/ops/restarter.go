// Package ops provides runtime operations utilities for SDK-based edge services.
package ops

import (
	"fmt"
)

// RestartMode defines the type of service restart.
type RestartMode string

const (
	// SoftRestart triggers a graceful re-initialization without killing the process.
	SoftRestart RestartMode = "soft"
	// HardRestart exits the process (external supervisor should restart it).
	HardRestart RestartMode = "hard"
)

// Restarter provides service restart capabilities.
type Restarter struct {
	serviceName   string
	onSoftRestart func() error // called before soft restart
	onHardRestart func() error // requests an orderly process restart
}

// NewRestarter creates a new Restarter.
func NewRestarter(serviceName string, onSoftRestart func() error) *Restarter {
	return &Restarter{
		serviceName:   serviceName,
		onSoftRestart: onSoftRestart,
	}
}

// NewRestarterWithHooks creates a restarter whose hard-restart hook is owned
// by the application lifecycle. The hook must initiate graceful shutdown and
// let the process supervisor restart the service.
func NewRestarterWithHooks(serviceName string, onSoftRestart, onHardRestart func() error) *Restarter {
	return &Restarter{
		serviceName:   serviceName,
		onSoftRestart: onSoftRestart,
		onHardRestart: onHardRestart,
	}
}

// RestartResult holds the result of a restart operation.
type RestartResult struct {
	Mode    string `json:"mode"`
	Message string `json:"message"`
}

// Restart triggers a service restart.
func (r *Restarter) Restart(mode RestartMode) (*RestartResult, error) {
	switch mode {
	case SoftRestart:
		return r.softRestart()
	case HardRestart:
		return r.hardRestart()
	default:
		return nil, fmt.Errorf("unsupported restart mode: %s", mode)
	}
}

func (r *Restarter) softRestart() (*RestartResult, error) {
	if r.onSoftRestart != nil {
		if err := r.onSoftRestart(); err != nil {
			return nil, fmt.Errorf("soft restart preparation failed: %w", err)
		}
	}
	return &RestartResult{
		Mode:    "soft",
		Message: fmt.Sprintf("%s soft restart triggered successfully", r.serviceName),
	}, nil
}

func (r *Restarter) hardRestart() (*RestartResult, error) {
	if r.onHardRestart == nil {
		return nil, fmt.Errorf("hard restart is unavailable: no graceful shutdown hook configured")
	}
	if err := r.onHardRestart(); err != nil {
		return nil, fmt.Errorf("hard restart request failed: %w", err)
	}
	msg := fmt.Sprintf("%s hard restart requested; graceful shutdown is starting", r.serviceName)
	return &RestartResult{
		Mode:    "hard",
		Message: msg,
	}, nil
}
