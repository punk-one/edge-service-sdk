package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	busapi "github.com/punk-one/edge-service-sdk/bus"
	appconfig "github.com/punk-one/edge-service-sdk/config"
	logger "github.com/punk-one/edge-service-sdk/logging"
	processapi "github.com/punk-one/edge-service-sdk/process"
	runtimebus "github.com/punk-one/edge-service-sdk/runtime/bus"

	"gopkg.in/yaml.v3"
)

const defaultConfigDir = "./configs/processes"

type Runner struct {
	config   appconfig.ProcessConfig
	registry processapi.Registry
	bus      *runtimebus.Service
	logger   logger.LoggingClient
}

func NewRunner(config appconfig.ProcessConfig, registry processapi.Registry, busService *runtimebus.Service, logClient logger.LoggingClient) *Runner {
	return &Runner{config: config, registry: registry, bus: busService, logger: logClient}
}

// Start loads and starts only the processors named by process.enabled. Invalid
// processors are reported and skipped without affecting the application path.
func (r *Runner) Start() (int, error) {
	if r == nil || len(r.config.Enabled) == 0 {
		return 0, nil
	}
	if r.bus == nil {
		return 0, fmt.Errorf("processes are configured but the JetStream bus is unavailable")
	}
	if r.registry == nil {
		return 0, fmt.Errorf("processes are configured but no process registry was supplied")
	}
	dir := strings.TrimSpace(r.config.ConfigDir)
	if dir == "" {
		dir = defaultConfigDir
	}
	definitions, err := loadDefinitions(dir)
	if err != nil {
		return 0, err
	}

	started := 0
	var startErrors []error
	seen := make(map[string]struct{}, len(r.config.Enabled))
	for _, rawName := range r.config.Enabled {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		definition, ok := definitions[name]
		if !ok {
			startErrors = append(startErrors, fmt.Errorf("enabled process %q has no YAML definition in %s", name, dir))
			continue
		}
		handlerName := strings.TrimSpace(definition.Handler)
		if handlerName == "" {
			handlerName = definition.Name
		}
		handler, ok := r.registry.Lookup(handlerName)
		if !ok {
			startErrors = append(startErrors, fmt.Errorf("enabled process %q has no registered handler %q", name, handlerName))
			continue
		}
		if err := r.startDefinition(definition, handler); err != nil {
			startErrors = append(startErrors, fmt.Errorf("start process %q: %w", name, err))
			continue
		}
		started++
		if r.logger != nil {
			r.logger.Infof("Process started: name=%s subscriptions=%d", name, len(definition.Subscribe))
		}
	}
	return started, errors.Join(startErrors...)
}

func (r *Runner) startDefinition(definition processapi.Definition, handler processapi.Handler) error {
	definition.Name = strings.TrimSpace(definition.Name)
	if definition.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(definition.Subscribe) == 0 {
		return fmt.Errorf("at least one subscribe message type is required")
	}
	if definition.MaxHop <= 0 {
		definition.MaxHop = 4
	}
	if definition.Concurrency <= 0 {
		definition.Concurrency = 1
	}
	timeout := 30 * time.Second
	if strings.TrimSpace(definition.Timeout) != "" {
		parsed, err := time.ParseDuration(definition.Timeout)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("invalid timeout %q", definition.Timeout)
		}
		timeout = parsed
	}
	allowedPublish, err := parseAllowedTypes(definition.Publish)
	if err != nil {
		return err
	}
	for _, rawType := range definition.Subscribe {
		messageType, err := busapi.ParseMessageType(rawType)
		if err != nil {
			return fmt.Errorf("invalid subscribe type %q: %w", rawType, err)
		}
		filter, err := busapi.FilterSubjectFor(messageType)
		if err != nil {
			return err
		}
		durable := "process-" + definition.Name + "-" + strings.ReplaceAll(string(messageType), ".", "-")
		definitionCopy := definition
		if err := r.bus.StartConsumer(runtimebus.ConsumerConfig{
			Durable:       durable,
			FilterSubject: filter,
			Workers:       definition.Concurrency,
			AckWait:       timeout + 5*time.Second,
			MaxDeliver:    10,
		}, func(parent context.Context, message busapi.Message) error {
			return r.handle(parent, definitionCopy, handler, allowedPublish, timeout, message)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) handle(parent context.Context, definition processapi.Definition, handler processapi.Handler, allowedPublish map[busapi.MessageType]struct{}, timeout time.Duration, message busapi.Message) (err error) {
	if message.Origin == busapi.OriginProcess {
		if strings.EqualFold(strings.TrimSpace(message.ProcessName), definition.Name) {
			return nil
		}
		if !definition.AcceptProcessMessages {
			return nil
		}
	}
	if message.Hop >= definition.MaxHop {
		if r.logger != nil {
			r.logger.Warnf("Process %s skipped message at max hop %d", definition.Name, message.Hop)
		}
		return nil
	}
	if message.Type == busapi.TelemetryReport && !supportsFormat(definition.DataFormats, message.DataFormat) {
		if r.logger != nil {
			r.logger.Warnf("Process %s skipped telemetry format %q", definition.Name, message.DataFormat)
		}
		return nil
	}
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
		if _, ok := allowedPublish[output.Type]; !ok {
			return fmt.Errorf("process %s attempted undeclared publish type %q", definition.Name, output.Type)
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
		if output.DataFormat == "" {
			output.DataFormat = message.DataFormat
		}
		if err := r.bus.Publish(ctx, output); err != nil {
			return err
		}
	}
	return nil
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

func parseAllowedTypes(values []string) (map[busapi.MessageType]struct{}, error) {
	result := make(map[busapi.MessageType]struct{}, len(values))
	for _, value := range values {
		messageType, err := busapi.ParseMessageType(value)
		if err != nil {
			return nil, fmt.Errorf("invalid publish type %q: %w", value, err)
		}
		result[messageType] = struct{}{}
	}
	return result, nil
}

func supportsFormat(supported []string, actual string) bool {
	if len(supported) == 0 || strings.TrimSpace(actual) == "" {
		return true
	}
	for _, candidate := range supported {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(actual)) {
			return true
		}
	}
	return false
}
