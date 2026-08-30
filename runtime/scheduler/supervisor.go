package scheduler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	logger "github.com/punk-one/edge-service-sdk/logging"
)

// Supervisor restarts failed workers and owns their cancellation lifecycle.
type Supervisor struct {
	logger       logger.LoggingClient
	restartDelay time.Duration
	maxDelay     time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewSupervisor creates a worker supervisor.
func NewSupervisor(log logger.LoggingClient, restartDelay time.Duration) *Supervisor {
	if restartDelay <= 0 {
		restartDelay = 5 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Supervisor{
		logger:       log,
		restartDelay: restartDelay,
		maxDelay:     time.Minute,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Context is cancelled when Stop begins. Workers should select on it.
func (s *Supervisor) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// Cancel asks all context-aware workers to stop without waiting for them.
func (s *Supervisor) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

// Start runs a worker and keeps restarting it after failures or panics.
func (s *Supervisor) Start(name string, run func() error) {
	if s == nil || run == nil {
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		attempt := 0
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			attempt++
			if s.logger != nil {
				s.logger.Infof("Supervisor starting worker=%s attempt=%d", name, attempt)
			}

			startedAt := time.Now()
			err := s.runSafely(name, run)
			if s.ctx.Err() != nil {
				return
			}
			if time.Since(startedAt) >= time.Minute {
				attempt = 1
			}
			delay := s.backoff(attempt)
			if s.logger != nil {
				if err == nil {
					s.logger.Warnf("Worker exited unexpectedly: worker=%s attempt=%d; restarting in %s", name, attempt, delay)
				} else {
					s.logger.Errorf("Worker failed: worker=%s attempt=%d err=%v; restarting in %s", name, attempt, err, delay)
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-s.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
	}()
}

// Stop cancels workers and waits for them until ctx expires.
func (s *Supervisor) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.Cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) backoff(attempt int) time.Duration {
	if attempt <= 1 {
		return s.restartDelay
	}
	delay := s.restartDelay
	for i := 1; i < attempt && delay < s.maxDelay/2; i++ {
		delay *= 2
	}
	if delay > s.maxDelay {
		return s.maxDelay
	}
	return delay
}

func (s *Supervisor) runSafely(name string, run func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stack := string(debug.Stack())
			if s.logger != nil {
				s.logger.Errorf("Worker panic recovered: worker=%s panic=%v stack=%s", name, recovered, stack)
			}
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()

	return run()
}
