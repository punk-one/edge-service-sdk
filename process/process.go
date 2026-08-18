package process

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/punk-one/edge-service-sdk/bus"
)

const (
	DefaultConcurrency = 1
	DefaultTimeout     = 30 * time.Second
	DefaultMaxHop      = 4
	MaximumMaxHop      = 16
)

// Handler implements one application-owned processor.
type Handler interface {
	Handle(ctx context.Context, message bus.Message) ([]bus.Message, error)
}

type HandlerFunc func(ctx context.Context, message bus.Message) ([]bus.Message, error)

func (f HandlerFunc) Handle(ctx context.Context, message bus.Message) ([]bus.Message, error) {
	return f(ctx, message)
}

// Registry stores handlers compiled into one edge service application.
type Registry interface {
	Register(name string, handler Handler) error
	MustRegister(name string, handler Handler)
	Lookup(name string) (Handler, bool)
	Names() []string
}

type registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() Registry {
	return &registry{handlers: make(map[string]Handler)}
}

func (r *registry) Register(name string, handler Handler) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("process handler name is required")
	}
	if handler == nil {
		return fmt.Errorf("process handler %q is nil", name)
	}
	value := reflect.ValueOf(handler)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return fmt.Errorf("process handler %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("process handler %q is already registered", name)
	}
	r.handlers[name] = handler
	return nil
}

func (r *registry) MustRegister(name string, handler Handler) {
	if err := r.Register(name, handler); err != nil {
		panic(err)
	}
}

func (r *registry) Lookup(name string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[strings.TrimSpace(name)]
	return handler, ok
}

func (r *registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Definition is loaded from configs/process/*.yaml. Every enabled Process
// receives all fixed SDK message types. Runtime controls are optional.
type Definition struct {
	Name        string `yaml:"name"`
	Handler     string `yaml:"handler"`
	Concurrency int    `yaml:"concurrency"`
	Timeout     string `yaml:"timeout"`
	MaxHop      int    `yaml:"maxHop"`
}
