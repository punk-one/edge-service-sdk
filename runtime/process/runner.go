package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	processapi "github.com/punk-one/edge-service-sdk/process"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"

	"gopkg.in/yaml.v3"
)

const defaultConfigDir = "./configs/process"

type Runner struct {
	configDir string
	devices   []contracts.DeviceConfig
	registry  processapi.Registry
	bus       *runtimebus.Service
	logger    logger.LoggingClient
}

func NewRunner(configDir string, devices []contracts.DeviceConfig, registry processapi.Registry, busService *runtimebus.Service, logClient logger.LoggingClient) *Runner {
	return &Runner{configDir: configDir, devices: devices, registry: registry, bus: busService, logger: logClient}
}

// ConfiguredProcessCount returns the number of distinct Process names bound to
// at least one device.
func ConfiguredProcessCount(devices []contracts.DeviceConfig) int {
	return len(buildBindings(devices))
}

// Start loads and starts the distinct processors referenced by device
// processNames. Invalid processors are reported and skipped without affecting
// the authoritative device, MQTT, or SQLite paths.
func (r *Runner) Start() (int, error) {
	if r == nil {
		return 0, nil
	}
	bindings := buildBindings(r.devices)
	if len(bindings) == 0 {
		return 0, nil
	}
	if r.bus == nil {
		return 0, fmt.Errorf("device-bound processes are configured but the JetStream bus is unavailable")
	}
	if r.registry == nil {
		return 0, fmt.Errorf("device-bound processes are configured but no process registry was supplied")
	}
	dir := strings.TrimSpace(r.configDir)
	if dir == "" {
		dir = defaultConfigDir
	}
	definitions, err := loadDefinitions(dir)
	if err != nil {
		return 0, err
	}

	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)

	started := 0
	var startErrors []error
	for _, name := range names {
		definition, ok := definitions[name]
		if !ok {
			startErrors = append(startErrors, fmt.Errorf("device-bound process %q has no YAML definition in %s", name, dir))
			continue
		}
		handlerName := strings.TrimSpace(definition.Handler)
		if handlerName == "" {
			handlerName = definition.Name
		}
		handler, ok := r.registry.Lookup(handlerName)
		if !ok {
			startErrors = append(startErrors, fmt.Errorf("device-bound process %q has no registered handler %q", name, handlerName))
			continue
		}
		if err := r.startDefinition(definition, bindings[name], handler); err != nil {
			startErrors = append(startErrors, fmt.Errorf("start process %q: %w", name, err))
			continue
		}
		started++
		if r.logger != nil {
			r.logger.Infof("Process started: name=%s devices=%d subject=%s", name, len(bindings[name]), busapi.StreamSubject)
		}
	}
	return started, errors.Join(startErrors...)
}

func (r *Runner) startDefinition(definition processapi.Definition, devices map[string]struct{}, handler processapi.Handler) error {
	definition.Name = strings.TrimSpace(definition.Name)
	if definition.Name == "" {
		return fmt.Errorf("name is required")
	}
	if definition.MaxHop <= 0 {
		definition.MaxHop = processapi.DefaultMaxHop
	}
	if definition.MaxHop > processapi.MaximumMaxHop {
		return fmt.Errorf("maxHop %d exceeds SDK maximum %d", definition.MaxHop, processapi.MaximumMaxHop)
	}
	if definition.Concurrency <= 0 {
		definition.Concurrency = processapi.DefaultConcurrency
	}
	timeout := processapi.DefaultTimeout
	if strings.TrimSpace(definition.Timeout) != "" {
		parsed, err := time.ParseDuration(definition.Timeout)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("invalid timeout %q", definition.Timeout)
		}
		timeout = parsed
	}

	definitionCopy := definition
	return r.bus.StartConsumer(runtimebus.ConsumerConfig{
		Durable:       "process-" + definition.Name,
		FilterSubject: busapi.StreamSubject,
		Workers:       definition.Concurrency,
		AckWait:       timeout + 5*time.Second,
		MaxDeliver:    10,
	}, func(parent context.Context, message busapi.Message) error {
		return r.handle(parent, definitionCopy, devices, handler, timeout, message)
	})
}

func (r *Runner) handle(parent context.Context, definition processapi.Definition, devices map[string]struct{}, handler processapi.Handler, timeout time.Duration, message busapi.Message) (err error) {
	if message.Origin == busapi.OriginProcess && strings.EqualFold(strings.TrimSpace(message.ProcessName), definition.Name) {
		return nil
	}
	if message.Hop >= definition.MaxHop {
		if r.logger != nil {
			r.logger.Warnf("Process %s skipped message at max hop %d", definition.Name, message.Hop)
		}
		return nil
	}
	deviceCode := messageDeviceCode(message)
	if _, ok := devices[deviceCode]; !ok {
		return nil
	}
	message.DeviceCode = deviceCode

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("process %s panicked: %v", definition.Name, recovered)
		}
	}()
	outputs, err := handler.Handle(ctx, message)
	if err != nil {
		return err
	}
	for _, output := range outputs {
		if _, err := busapi.SubjectFor(output.Type, output.Identifier); err != nil {
			return fmt.Errorf("process %s attempted invalid publish type: %w", definition.Name, err)
		}
		output.Origin = busapi.OriginProcess
		output.ProcessName = definition.Name
		output.Hop = message.Hop + 1
		if output.TraceID == "" {
			output.TraceID = message.TraceID
		}
		if output.CausationID == "" {
			output.CausationID = message.TraceID
		}
		if output.ProductCode == "" {
			output.ProductCode = message.ProductCode
		}
		if output.DeviceCode == "" {
			output.DeviceCode = message.DeviceCode
		}
		if _, ok := devices[strings.TrimSpace(output.DeviceCode)]; !ok {
			return fmt.Errorf("process %s attempted output for unbound device %q", definition.Name, output.DeviceCode)
		}
		if output.DataFormat == "" {
			output.DataFormat = message.DataFormat
		}
		if err := r.bus.Publish(ctx, output); err != nil {
			return err
		}
	}
	return nil
}

func buildBindings(devices []contracts.DeviceConfig) map[string]map[string]struct{} {
	bindings := make(map[string]map[string]struct{})
	for _, device := range devices {
		deviceCode := strings.TrimSpace(device.InternalName)
		if deviceCode == "" {
			deviceCode = strings.TrimSpace(device.Name)
		}
		if deviceCode == "" {
			continue
		}
		for _, rawName := range device.ProcessNames {
			name := strings.TrimSpace(rawName)
			if name == "" {
				continue
			}
			if bindings[name] == nil {
				bindings[name] = make(map[string]struct{})
			}
			bindings[name][deviceCode] = struct{}{}
		}
	}
	return bindings
}

func messageDeviceCode(message busapi.Message) string {
	if deviceCode := strings.TrimSpace(message.DeviceCode); deviceCode != "" {
		return deviceCode
	}
	var envelope struct {
		DeviceCode    string `json:"device_code"`
		DeviceCodeAlt string `json:"deviceCode"`
		DeviceName    string `json:"deviceName"`
	}
	if json.Unmarshal(message.Data, &envelope) != nil {
		return ""
	}
	for _, value := range []string{envelope.DeviceCode, envelope.DeviceCodeAlt, envelope.DeviceName} {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func loadDefinitions(dir string) (map[string]processapi.Definition, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read process config directory %s: %w", dir, err)
	}
	definitions := make(map[string]processapi.Definition)
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext == ".yaml" || ext == ".yml" {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var definition processapi.Definition
		if err := yaml.Unmarshal(data, &definition); err != nil {
			return nil, fmt.Errorf("parse process definition %s: %w", path, err)
		}
		definition.Name = strings.TrimSpace(definition.Name)
		if definition.Name == "" {
			return nil, fmt.Errorf("process definition %s has no name", path)
		}
		if _, duplicate := definitions[definition.Name]; duplicate {
			return nil, fmt.Errorf("duplicate process definition %q", definition.Name)
		}
		definitions[definition.Name] = definition
	}
	return definitions, nil
}
