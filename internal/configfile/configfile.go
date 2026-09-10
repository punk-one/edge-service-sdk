// Package configfile provides the shared JSON/YAML configuration file policy.
package configfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/punk-one/edge-service-sdk/internal/atomicfile"
	"gopkg.in/yaml.v3"
)

var preferredExtensions = []string{".json", ".yaml", ".yml"}

// Supported reports whether path has a supported configuration extension.
func Supported(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json", ".yaml", ".yml":
		return true
	default:
		return false
	}
}

// LogicalName returns a configuration filename without its supported extension.
func LogicalName(path string) string {
	name := filepath.Base(path)
	if Supported(name) {
		return strings.TrimSuffix(name, filepath.Ext(name))
	}
	return name
}

// ResolvePreferred resolves a logical configuration path using JSON > YAML > YML.
// The input may include a supported extension. If no candidate exists, the
// cleaned input path is returned with exists=false.
func ResolvePreferred(path string) (resolved string, exists bool, err error) {
	cleaned := filepath.Clean(path)
	base := cleaned
	if Supported(cleaned) {
		base = strings.TrimSuffix(cleaned, filepath.Ext(cleaned))
	}

	for _, ext := range preferredExtensions {
		candidate := base + ext
		info, statErr := os.Stat(candidate)
		switch {
		case statErr == nil:
			if info.IsDir() {
				return "", false, fmt.Errorf("configuration path %s is a directory", candidate)
			}
			return candidate, true, nil
		case errors.Is(statErr, os.ErrNotExist):
			continue
		default:
			return "", false, fmt.Errorf("stat configuration file %s: %w", candidate, statErr)
		}
	}
	return cleaned, false, nil
}

// ListPreferred lists one file per logical name using JSON > YAML > YML.
func ListPreferred(dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Clean(dir))
	if err != nil {
		return nil, err
	}

	type selectedFile struct {
		path     string
		priority int
	}
	selected := make(map[string]selectedFile)
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !Supported(entry.Name()) {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		priority := extensionPriority(ext)
		logicalName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		current, ok := selected[logicalName]
		if !ok || priority < current.priority {
			selected[logicalName] = selectedFile{
				path:     filepath.Join(dir, entry.Name()),
				priority: priority,
			}
		}
	}

	logicalNames := make([]string, 0, len(selected))
	for logicalName := range selected {
		logicalNames = append(logicalNames, logicalName)
	}
	sort.Strings(logicalNames)
	files := make([]string, 0, len(logicalNames))
	for _, logicalName := range logicalNames {
		files = append(files, selected[logicalName].path)
	}
	return files, nil
}

func extensionPriority(ext string) int {
	for index, candidate := range preferredExtensions {
		if ext == candidate {
			return index
		}
	}
	return len(preferredExtensions)
}

// Decode decodes JSON or YAML according to the file extension. JSON syntax is
// validated before YAML decoding so malformed JSON cannot be accepted as YAML.
func Decode(path string, data []byte, target interface{}) error {
	ext := strings.ToLower(filepath.Ext(path))
	if !Supported(path) {
		return fmt.Errorf("unsupported configuration extension %q", ext)
	}
	if ext == ".json" && !json.Valid(data) {
		return fmt.Errorf("invalid JSON syntax")
	}
	if err := yaml.Unmarshal(data, target); err != nil {
		return err
	}
	return nil
}

// Read reads and decodes one supported configuration file.
func Read(path string, target interface{}) error {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("read configuration file %s: %w", path, err)
	}
	if err := Decode(path, data, target); err != nil {
		return fmt.Errorf("parse configuration file %s: %w", path, err)
	}
	return nil
}

// ReadMap reads a configuration file into a generic string-keyed tree.
func ReadMap(path string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	if err := Read(path, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Marshal encodes a generic tree using the format selected by path.
func Marshal(path string, value map[string]interface{}) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		data, err = json.MarshalIndent(value, "", "  ")
		if err == nil {
			data = append(data, byte(10))
		}
	case ".yaml", ".yml":
		data, err = yaml.Marshal(value)
	default:
		return nil, fmt.Errorf("unsupported configuration extension %q", filepath.Ext(path))
	}
	if err != nil {
		return nil, fmt.Errorf("marshal configuration file %s: %w", path, err)
	}
	return data, nil
}

// WriteMap atomically writes a generic tree in the existing file format and
// preserves its permissions. New files default to mode 0644.
func WriteMap(path string, value map[string]interface{}) error {
	data, err := Marshal(path, value)
	if err != nil {
		return err
	}

	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(filepath.Clean(path)); statErr == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("stat configuration file %s: %w", path, statErr)
	}
	if err := atomicfile.WriteFile(filepath.Clean(path), data, mode); err != nil {
		return fmt.Errorf("write configuration file %s: %w", path, err)
	}
	return nil
}
