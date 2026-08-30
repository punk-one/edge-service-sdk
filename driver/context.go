package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const DefaultOperationTimeout = 30 * time.Second

var (
	ErrOperationTimeout    = errors.New("driver operation timed out")
	ErrLegacyOperationBusy = errors.New("previous driver operation is still running")
	// ErrOperationStuck means the SDK deadline expired but the driver call did
	// not return. Go cannot safely kill that goroutine, so the runtime must
	// perform a graceful hard restart to recover the device session.
	ErrOperationStuck = errors.New("driver operation is stuck")
)

// ContextProtocolDriver is the v1 reliability extension for drivers. Drivers
// should implement it so transport I/O can be cancelled at the source.
type ContextProtocolDriver interface {
	HandleReadCommandsContext(ctx context.Context, deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest) ([]*CommandValue, error)
	HandleWriteCommandsContext(ctx context.Context, deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest, params []*CommandValue) error
}

// AmbiguousWriteError means a write timed out after execution may have begun.
// Callers must not blindly retry it without device-level idempotency or state
// verification.
type AmbiguousWriteError struct{ Cause error }

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf("driver write outcome is ambiguous: %v", e.Cause)
}

func (e *AmbiguousWriteError) Unwrap() error { return e.Cause }

func IsAmbiguousWrite(err error) bool {
	var target *AmbiguousWriteError
	return errors.As(err, &target)
}

var operationGates sync.Map

// HandleReadCommandsWithContext bounds a driver read. Legacy drivers are
// isolated behind a per-device gate so a blocked call cannot create an
// unbounded number of leaked goroutines.
func HandleReadCommandsWithContext(ctx context.Context, protocolDriver ProtocolDriver, deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest) ([]*CommandValue, error) {
	if protocolDriver == nil {
		return nil, fmt.Errorf("protocol driver is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOperationTimeout, err)
	}
	release, err := acquireOperationGate(ctx, protocolDriver, deviceName, "read")
	if err != nil {
		return nil, err
	}
	type result struct {
		values []*CommandValue
		err    error
	}
	done := make(chan result, 1)
	go func() {
		defer release()
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- result{err: fmt.Errorf("driver read panic: %v", recovered)}
			}
		}()
		if contextual, ok := protocolDriver.(ContextProtocolDriver); ok {
			values, callErr := contextual.HandleReadCommandsContext(ctx, deviceName, protocols, reqs)
			done <- result{values: values, err: callErr}
			return
		}
		values, callErr := protocolDriver.HandleReadCommands(deviceName, protocols, reqs)
		done <- result{values: values, err: callErr}
	}()
	select {
	case result := <-done:
		return result.values, result.err
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w: device=%s: %v", ErrOperationStuck, ErrOperationTimeout, deviceName, ctx.Err())
	}
}

// HandleWriteCommandsWithContext bounds a driver write and reports timeout as
// ambiguous because the physical side effect may already have occurred.
func HandleWriteCommandsWithContext(ctx context.Context, protocolDriver ProtocolDriver, deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest, params []*CommandValue) error {
	if protocolDriver == nil {
		return fmt.Errorf("protocol driver is nil")
	}
	if err := ctx.Err(); err != nil {
		return &AmbiguousWriteError{Cause: fmt.Errorf("%w: %v", ErrOperationTimeout, err)}
	}
	release, err := acquireOperationGate(ctx, protocolDriver, deviceName, "write")
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		defer release()
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- fmt.Errorf("driver write panic: %v", recovered)
			}
		}()
		if contextual, ok := protocolDriver.(ContextProtocolDriver); ok {
			done <- contextual.HandleWriteCommandsContext(ctx, deviceName, protocols, reqs, params)
			return
		}
		done <- protocolDriver.HandleWriteCommands(deviceName, protocols, reqs, params)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return &AmbiguousWriteError{Cause: fmt.Errorf("%w: %w: device=%s: %v", ErrOperationStuck, ErrOperationTimeout, deviceName, ctx.Err())}
	}
}

func acquireOperationGate(ctx context.Context, protocolDriver ProtocolDriver, deviceName, operation string) (func(), error) {
	// Serialize reads and writes for one device. Protocol drivers are commonly
	// backed by a single PLC/serial session and are not necessarily reentrant.
	key := fmt.Sprintf("%T:%p:%s", protocolDriver, protocolDriver, deviceName)
	value, _ := operationGates.LoadOrStore(key, make(chan struct{}, 1))
	gate := value.(chan struct{})
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w: device=%s operation=%s: %v", ErrOperationStuck, ErrLegacyOperationBusy, deviceName, operation, ctx.Err())
	}
}
