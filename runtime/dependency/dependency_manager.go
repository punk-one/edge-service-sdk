package dependency

import (
	"fmt"
	"strings"

	logger "github.com/punk-one/edge-service-sdk/logging"
)

// DependencyChecker checks whether a runtime dependency is ready.
type DependencyChecker interface {
	Name() string
	Check() error
}

type dependencyChecker struct {
	name string
	fn   func() error
}

func (d dependencyChecker) Name() string {
	return d.name
}

func (d dependencyChecker) Check() error {
	if d.fn == nil {
		return nil
	}
	return d.fn()
}

// DependencyManager validates startup dependencies before workers begin running.
type DependencyManager struct {
	logger   logger.LoggingClient
	checkers []DependencyChecker
}

// NewDependencyManager creates a dependency manager for startup checks.
func NewDependencyManager(log logger.LoggingClient) *DependencyManager {
	return &DependencyManager{
		logger:   log,
		checkers: make([]DependencyChecker, 0, 4),
	}
}

// Register appends a dependency checker.
func (m *DependencyManager) Register(checker DependencyChecker) {
	if m == nil || checker == nil {
		return
	}
	m.checkers = append(m.checkers, checker)
}

// CheckAll validates all registered dependencies and returns a combined error when any fail.
func (m *DependencyManager) CheckAll() error {
	if m == nil {
		return nil
	}

	var failures []string
	for _, checker := range m.checkers {
		if checker == nil {
			continue
		}

		if err := checker.Check(); err != nil {
			m.logger.Errorf("Dependency check failed: %s: %v", checker.Name(), err)
			failures = append(failures, fmt.Sprintf("%s: %v", checker.Name(), err))
			continue
		}

		m.logger.Infof("Dependency ready: %s", checker.Name())
	}

	if len(failures) > 0 {
		return fmt.Errorf("dependency check failed: %s", strings.Join(failures, "; "))
	}

	return nil
}

// NamedDependency creates a dependency checker from a simple function.
func NamedDependency(name string, fn func() error) DependencyChecker {
	return dependencyChecker{
		name: name,
		fn:   fn,
	}
}
