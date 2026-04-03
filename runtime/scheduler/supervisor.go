package scheduler

import (
	"fmt"
	"runtime/debug"
	"time"

	logger "github.com/punk-one/edge-service-sdk/logging"
)

// Supervisor restarts failed workers with a fixed cooldown.
type Supervisor struct {
	logger       logger.LoggingClient
	restartDelay time.Duration
}

// NewSupervisor creates a worker supervisor.
func NewSupervisor(log logger.LoggingClient, restartDelay time.Duration) *Supervisor {
	if restartDelay <= 0 {
		restartDelay = 5 * time.Second
	}
	return &Supervisor{
		logger:       log,
		restartDelay: restartDelay,
	}
}

// Start runs a worker and keeps restarting it after failures or panics.
func (s *Supervisor) Start(name string, run func() error) {
	if s == nil || run == nil {
		return
	}

	go func() {
		attempt := 0
		for {
			attempt++
			s.logger.Infof("Supervisor starting worker=%s attempt=%d", name, attempt)

			err := s.runSafely(name, run)
			if err == nil {
				s.logger.Warnf("Worker exited unexpectedly: worker=%s attempt=%d; restarting in %s", name, attempt, s.restartDelay)
			} else {
				s.logger.Errorf("Worker failed: worker=%s attempt=%d err=%v; restarting in %s", name, attempt, err, s.restartDelay)
			}

			time.Sleep(s.restartDelay)
		}
	}()
}

func (s *Supervisor) runSafely(name string, run func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stack := string(debug.Stack())
			s.logger.Errorf("Worker panic recovered: worker=%s panic=%v stack=%s", name, recovered, stack)
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()

	return run()
}
