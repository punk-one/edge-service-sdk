package eventruntime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	coreevent "github.com/punk-one/edge-service-sdk/event"
)

// StateStore persists the event engine state independently from the event
// delivery queue. Clearing the queue must never clear rise-clear state.
type StateStore interface {
	Load() (coreevent.PersistedState, error)
	Save(coreevent.PersistedState) error
}

type fileStateStore struct {
	path string
}

func NewFileStateStore(path string) StateStore {
	return &fileStateStore{path: path}
}

func (s *fileStateStore) Load() (coreevent.PersistedState, error) {
	if s == nil || s.path == "" {
		return coreevent.PersistedState{}, nil
	}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return coreevent.PersistedState{}, nil
	}
	if err != nil {
		return coreevent.PersistedState{}, err
	}
	var state coreevent.PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return coreevent.PersistedState{}, fmt.Errorf("decode event state: %w", err)
	}
	return state, nil
}

func (s *fileStateStore) Save(state coreevent.PersistedState) error {
	if s == nil || s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode event state: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".event-state-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(temporaryName, s.path)
}
