package driver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type blockingProtocolDriver struct {
	readBlock  chan struct{}
	writeBlock chan struct{}
	readCalls  atomic.Int32
	writeCalls atomic.Int32
}

func (d *blockingProtocolDriver) Initialize(DeviceServiceSDK) error { return nil }
func (d *blockingProtocolDriver) HandleReadCommands(string, map[string]ProtocolProperties, []CommandRequest) ([]*CommandValue, error) {
	d.readCalls.Add(1)
	<-d.readBlock
	return []*CommandValue{}, nil
}
func (d *blockingProtocolDriver) HandleWriteCommands(string, map[string]ProtocolProperties, []CommandRequest, []*CommandValue) error {
	d.writeCalls.Add(1)
	<-d.writeBlock
	return nil
}
func (d *blockingProtocolDriver) Stop(bool) error { return nil }
func (d *blockingProtocolDriver) AddDevice(string, map[string]ProtocolProperties, AdminState) error {
	return nil
}
func (d *blockingProtocolDriver) UpdateDevice(string, map[string]ProtocolProperties, AdminState) error {
	return nil
}
func (d *blockingProtocolDriver) RemoveDevice(string, map[string]ProtocolProperties) error {
	return nil
}
func (d *blockingProtocolDriver) ValidateDevice(Device) error { return nil }
func (d *blockingProtocolDriver) Start() error                { return nil }
func (d *blockingProtocolDriver) Discover() error             { return nil }

func TestReadTimeoutKeepsSingleLegacyCallInFlight(t *testing.T) {
	driver := &blockingProtocolDriver{readBlock: make(chan struct{}), writeBlock: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := HandleReadCommandsWithContext(ctx, driver, "device-a", nil, nil)
	if !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("read error = %v, want ErrOperationTimeout", err)
	}
	if !errors.Is(err, ErrOperationStuck) {
		t.Fatalf("read error = %v, want ErrOperationStuck", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	_, err = HandleReadCommandsWithContext(ctx2, driver, "device-a", nil, nil)
	if !errors.Is(err, ErrLegacyOperationBusy) {
		t.Fatalf("second read error = %v, want ErrLegacyOperationBusy", err)
	}
	if !errors.Is(err, ErrOperationStuck) {
		t.Fatalf("second read error = %v, want ErrOperationStuck", err)
	}
	if calls := driver.readCalls.Load(); calls != 1 {
		t.Fatalf("legacy read calls = %d, want 1", calls)
	}
	close(driver.readBlock)
}

func TestLegacyReadAndWriteShareOneDeviceGate(t *testing.T) {
	driver := &blockingProtocolDriver{readBlock: make(chan struct{}), writeBlock: make(chan struct{})}
	readCtx, cancelRead := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelRead()
	if _, err := HandleReadCommandsWithContext(readCtx, driver, "device-shared", nil, nil); !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("read error = %v, want ErrOperationTimeout", err)
	}

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWrite()
	err := HandleWriteCommandsWithContext(writeCtx, driver, "device-shared", nil, nil, nil)
	if !errors.Is(err, ErrLegacyOperationBusy) {
		t.Fatalf("write error = %v, want ErrLegacyOperationBusy", err)
	}
	if !errors.Is(err, ErrOperationStuck) {
		t.Fatalf("write error = %v, want ErrOperationStuck", err)
	}
	if calls := driver.writeCalls.Load(); calls != 0 {
		t.Fatalf("legacy write calls = %d, want 0", calls)
	}
	close(driver.readBlock)
}

func TestWriteTimeoutIsAmbiguous(t *testing.T) {
	driver := &blockingProtocolDriver{readBlock: make(chan struct{}), writeBlock: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := HandleWriteCommandsWithContext(ctx, driver, "device-b", nil, nil, nil)
	if !IsAmbiguousWrite(err) || !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("write error = %v, want ambiguous timeout", err)
	}
	close(driver.writeBlock)
}
