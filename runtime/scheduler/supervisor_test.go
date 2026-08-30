package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSupervisorRestartsFailedWorkerAndStops(t *testing.T) {
	supervisor := NewSupervisor(nil, time.Millisecond)
	var attempts atomic.Int32
	secondAttempt := make(chan struct{})

	supervisor.Start("restartable", func() error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			return errors.New("transient failure")
		}
		if attempt == 2 {
			close(secondAttempt)
		}
		<-supervisor.Context().Done()
		return supervisor.Context().Err()
	})

	select {
	case <-secondAttempt:
	case <-time.After(time.Second):
		t.Fatal("worker was not restarted")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestSupervisorRecoversPanic(t *testing.T) {
	supervisor := NewSupervisor(nil, time.Millisecond)
	restarted := make(chan struct{})
	var attempts atomic.Int32

	supervisor.Start("panic-worker", func() error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		close(restarted)
		<-supervisor.Context().Done()
		return nil
	})

	select {
	case <-restarted:
	case <-time.After(time.Second):
		t.Fatal("panicked worker was not restarted")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}
