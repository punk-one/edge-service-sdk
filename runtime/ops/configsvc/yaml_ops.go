package configsvc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/punk-one/edge-service-sdk/internal/atomicfile"
	"gopkg.in/yaml.v3"
)

// readYAMLFile reads a YAML file and returns its contents as a generic map.
func readYAMLFile(filePath string) (map[string]interface{}, error) {
	data, err := os.ReadFile(filepath.Clean(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	var result map[string]interface{}
	if err := yaml.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse YAML file %s: %w", filePath, err)
	}
	return result, nil
}

// writeYAMLFile writes a generic map back to a YAML file.
func writeYAMLFile(filePath string, data map[string]interface{}) error {
	out, err := yaml.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML for %s: %w", filePath, err)
	}
	if err := atomicfile.WriteFile(filepath.Clean(filePath), out, 0o644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", filePath, err)
	}
	return nil
}

// navigatePath traverses a YAML tree using a dot-separated path.
// Named array elements are matched by their "name" field instead of requiring [index].
func navigatePath(data interface{}, configPath string) (interface{}, error) {
	parts := strings.Split(configPath, ".")
	if len(parts) == 0 || strings.TrimSpace(configPath) == "" {
		return nil, fmt.Errorf("empty config path")
	}

	current := data
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty path segment in %q", configPath)
		}

		switch m := current.(type) {
		case map[string]interface{}:
			val, ok := m[part]
			if !ok {
				return nil, fmt.Errorf("key %q not found in path %q", part, configPath)
			}
			current = val

		case []interface{}:
			// Named array: find the element whose "name" field matches
			found, _, err := findByName(m, part)
			if err != nil {
				return nil, fmt.Errorf("element %q not found in path %q: %w", part, configPath, err)
			}
			current = found

		default:
			return nil, fmt.Errorf("cannot navigate into %T at segment %q in path %q", current, part, configPath)
		}
	}

	return current, nil
}

// setByPath sets a value at the given dot-separated path in the YAML tree.
// Intermediate maps are created as needed. Named array elements are matched by name.
func setByPath(data map[string]interface{}, configPath string, value interface{}) error {
	parts := strings.Split(configPath, ".")
	if len(parts) == 0 {
		return fmt.Errorf("empty config path")
	}

	// Navigate to the parent
	if len(parts) == 1 {
		data[parts[0]] = value
		return nil
	}

	parent, err := navigatePath(data, strings.Join(parts[:len(parts)-1], "."))
	if err != nil {
		return err
	}

	lastKey := parts[len(parts)-1]
	switch m := parent.(type) {
	case map[string]interface{}:
		m[lastKey] = value
		return nil
	default:
		return fmt.Errorf("cannot set value at path %q: parent is %T, not a map", configPath, parent)
	}
}

// deleteByPath removes a value at the given dot-separated path.
func deleteByPath(data map[string]interface{}, configPath string) error {
	parts := strings.Split(configPath, ".")
	if len(parts) == 0 {
		return fmt.Errorf("empty config path")
	}

	if len(parts) == 1 {
		delete(data, parts[0])
		return nil
	}

	parent, err := navigatePath(data, strings.Join(parts[:len(parts)-1], "."))
	if err != nil {
		return err
	}

	lastKey := parts[len(parts)-1]
	switch m := parent.(type) {
	case map[string]interface{}:
		delete(m, lastKey)
		return nil
	default:
		return fmt.Errorf("cannot delete at path %q: parent is %T, not a map", configPath, parent)
	}
}

// findByName searches a []interface{} for an element whose "name" field matches.
// Returns the element, its index, and an error if not found.
func findByName(arr []interface{}, name string) (interface{}, int, error) {
	for i, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		itemName, exists := m["name"]
		if !exists {
			continue
		}
		if fmt.Sprint(itemName) == name {
			return m, i, nil
		}
	}
	return nil, -1, fmt.Errorf("element with name %q not found", name)
}

// listKeys returns the top-level keys of a map, or names of items in a named array.
func listKeys(data interface{}) ([]string, error) {
	switch m := data.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		return keys, nil
	case []interface{}:
		names := make([]string, 0, len(m))
		for _, item := range m {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if name, exists := itemMap["name"]; exists {
				names = append(names, fmt.Sprint(name))
			}
		}
		return names, nil
	default:
		return nil, fmt.Errorf("cannot list keys of %T", data)
	}
}

// pathExists checks whether a given config path exists in the tree.
func pathExists(data interface{}, configPath string) bool {
	_, err := navigatePath(data, configPath)
	return err == nil
}
