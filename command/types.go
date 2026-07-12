package command

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	ctl "github.com/punk-one/edge-service-sdk/control"
	logger "github.com/punk-one/edge-service-sdk/logging"
)

// CommandRequest uses the unified control request envelope.
type CommandRequest = ctl.Request

// CommandResponse uses the unified control result envelope.
type CommandResponse = ctl.Result

// CommandParam describes one command input or output parameter.
type CommandParam struct {
	Identifier  string `json:"identifier"`
	ValueType   string `json:"value_type"`
	Required    bool   `json:"required,omitempty"`
	Description string `json:"description,omitempty"`
}

// CommandDescriptor is the static metadata exposed for one registered command.
type CommandDescriptor struct {
	Identifier   string         `json:"identifier"`
	Name         string         `json:"name"`
	Mode         string         `json:"mode"`
	InputParams  []CommandParam `json:"input_params,omitempty"`
	OutputParams []CommandParam `json:"output_params,omitempty"`
}

// ProgressPayload is persisted as result.data.progress for processing events.
type ProgressPayload map[string]interface{}

// CommandError carries a structured command failure for SDK result assembly.
type CommandError struct {
	Code    int
	Message string
	Data    map[string]interface{}
	Err     error
}

func (e *CommandError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Message) != "" {
		return strings.TrimSpace(e.Message)
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "command execution failed"
}

// Error builds a structured command error.
func Error(code int, message string, data map[string]interface{}) *CommandError {
	return &CommandError{Code: code, Message: message, Data: cloneData(data)}
}

// DriverError maps an underlying driver or runtime error to a structured command error.
func DriverError(err error) *CommandError {
	if err == nil {
		return nil
	}
	return &CommandError{Code: ctl.CodeDriverError, Message: err.Error(), Err: err}
}

// BadRequestError builds a bad-request command error.
func BadRequestError(message string) *CommandError {
	return &CommandError{Code: ctl.CodeBadRequest, Message: strings.TrimSpace(message)}
}

// BusyError builds a busy command error.
func BusyError(message string, data map[string]interface{}) *CommandError {
	return &CommandError{Code: ctl.CodeBusy, Message: strings.TrimSpace(message), Data: cloneData(data)}
}

// TimeoutError builds a timeout command error.
func TimeoutError(message string, data map[string]interface{}) *CommandError {
	return &CommandError{Code: ctl.CodeTimeout, Message: strings.TrimSpace(message), Data: cloneData(data)}
}

// CommandContext exposes runtime helpers to one application command implementation.
type CommandContext interface {
	TraceID() string
	DeviceCode() string
	Metadata() *ctl.Metadata
	Logger() logger.LoggingClient

	GetProperties(names []string) (map[string]interface{}, error)
	SetProperties(values map[string]interface{}) error
	ReadTelemetry(names []string) (map[string]interface{}, error)

	ReportProgress(progress ProgressPayload)
	IsCancelled() bool
}

// Command is the only supported application command implementation contract.
type Command interface {
	Descriptor() CommandDescriptor
	Execute(ctx CommandContext, req CommandRequest) (map[string]interface{}, *CommandError)
}

// Registry stores registered commands.
type Registry interface {
	Register(cmd Command) error
	MustRegister(cmd Command)
	Lookup(identifier string) (CommandDescriptor, Command, bool)
	List() []CommandDescriptor
}

type registry struct {
	mu    sync.RWMutex
	items map[string]registeredCommand
}

type registeredCommand struct {
	desc CommandDescriptor
	cmd  Command
}

// NewRegistry creates a new command registry.
func NewRegistry() Registry {
	return &registry{items: map[string]registeredCommand{}}
}

func (r *registry) Register(cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("command is required")
	}
	value := reflect.ValueOf(cmd)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return fmt.Errorf("command is required")
	}
	desc, err := normalizeDescriptor(cmd.Descriptor())
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[desc.Identifier]; exists {
		return fmt.Errorf("command %q already registered", desc.Identifier)
	}
	r.items[desc.Identifier] = registeredCommand{desc: desc, cmd: cmd}
	return nil
}

func (r *registry) MustRegister(cmd Command) {
	if err := r.Register(cmd); err != nil {
		panic(err)
	}
}

func (r *registry) Lookup(identifier string) (CommandDescriptor, Command, bool) {
	if r == nil {
		return CommandDescriptor{}, nil, false
	}
	identifier = strings.TrimSpace(identifier)
	r.mu.RLock()
	entry, ok := r.items[identifier]
	r.mu.RUnlock()
	if !ok {
		return CommandDescriptor{}, nil, false
	}
	return cloneDescriptor(entry.desc), entry.cmd, true
}

func (r *registry) List() []CommandDescriptor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	items := make([]CommandDescriptor, 0, len(r.items))
	for _, entry := range r.items {
		items = append(items, cloneDescriptor(entry.desc))
	}
	r.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].Identifier < items[j].Identifier
	})
	return items
}

func normalizeDescriptor(desc CommandDescriptor) (CommandDescriptor, error) {
	desc.Identifier = strings.TrimSpace(desc.Identifier)
	if desc.Identifier == "" {
		return CommandDescriptor{}, fmt.Errorf("command identifier is required")
	}
	desc.Name = strings.TrimSpace(desc.Name)
	if desc.Name == "" {
		desc.Name = desc.Identifier
	}
	desc.Mode = strings.ToLower(strings.TrimSpace(desc.Mode))
	if desc.Mode == "" {
		desc.Mode = "sync"
	}
	if desc.Mode != "sync" && desc.Mode != "async" {
		return CommandDescriptor{}, fmt.Errorf("command %q mode %q is not supported", desc.Identifier, desc.Mode)
	}
	desc.InputParams = cloneParams(desc.InputParams)
	desc.OutputParams = cloneParams(desc.OutputParams)
	for i := range desc.InputParams {
		desc.InputParams[i].Identifier = strings.TrimSpace(desc.InputParams[i].Identifier)
		desc.InputParams[i].ValueType = strings.TrimSpace(desc.InputParams[i].ValueType)
		if desc.InputParams[i].Identifier == "" {
			return CommandDescriptor{}, fmt.Errorf("command %q has empty input parameter identifier", desc.Identifier)
		}
	}
	for i := range desc.OutputParams {
		desc.OutputParams[i].Identifier = strings.TrimSpace(desc.OutputParams[i].Identifier)
		desc.OutputParams[i].ValueType = strings.TrimSpace(desc.OutputParams[i].ValueType)
		if desc.OutputParams[i].Identifier == "" {
			return CommandDescriptor{}, fmt.Errorf("command %q has empty output parameter identifier", desc.Identifier)
		}
	}
	return desc, nil
}

func cloneDescriptor(desc CommandDescriptor) CommandDescriptor {
	return CommandDescriptor{
		Identifier:   desc.Identifier,
		Name:         desc.Name,
		Mode:         desc.Mode,
		InputParams:  cloneParams(desc.InputParams),
		OutputParams: cloneParams(desc.OutputParams),
	}
}

func cloneParams(items []CommandParam) []CommandParam {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]CommandParam, len(items))
	copy(cloned, items)
	return cloned
}

func cloneData(data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return map[string]interface{}{}
	}
	cloned := make(map[string]interface{}, len(data))
	for key, value := range data {
		cloned[key] = value
	}
	return cloned
}
