package driver

import (
	logger "github.com/punk-one/edge-service-sdk/logging"
)

// ProtocolDriver defines the interface that device drivers must implement
type ProtocolDriver interface {
	// Initialize performs protocol-specific initialization for the device service
	Initialize(sdk DeviceServiceSDK) error

	// HandleReadCommands triggers a protocol Read operation for the specified device
	HandleReadCommands(deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest) (res []*CommandValue, err error)

	// HandleWriteCommands passes a slice of CommandRequest struct each representing
	// a ResourceOperation for a specific device resource
	HandleWriteCommands(deviceName string, protocols map[string]ProtocolProperties, reqs []CommandRequest, params []*CommandValue) error

	// Stop the protocol-specific DS code to shutdown gracefully
	Stop(force bool) error

	// AddDevice is a callback function that is invoked when a new Device is added
	AddDevice(deviceName string, protocols map[string]ProtocolProperties, adminState AdminState) error

	// UpdateDevice is a callback function that is invoked when a Device is updated
	UpdateDevice(deviceName string, protocols map[string]ProtocolProperties, adminState AdminState) error

	// RemoveDevice is a callback function that is invoked when a Device is removed
	RemoveDevice(deviceName string, protocols map[string]ProtocolProperties) error

	// ValidateDevice validates the device configuration
	ValidateDevice(device Device) error

	// Start starts the driver
	Start() error

	// Discover performs device discovery
	Discover() error
}

// DeviceServiceSDK defines the interface for device service SDK
type DeviceServiceSDK interface {
	// LoggingClient returns the logging client
	LoggingClient() logger.LoggingClient

	// AsyncValuesChannel returns the async values channel
	AsyncValuesChannel() chan<- *AsyncValues

	// Devices returns the list of devices
	Devices() []Device
}

// DeviceStatusReporter is an optional runtime hook for connection and I/O status.
type DeviceStatusReporter interface {
	DeviceConnected(deviceName string)
	DeviceDisconnected(deviceName string, err error)
	DeviceReadSucceeded(deviceName string)
	DeviceReadFailed(deviceName string, err error)
	DeviceWriteSucceeded(deviceName string)
	DeviceWriteFailed(deviceName string, err error)
}
