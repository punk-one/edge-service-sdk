// Package atomicfile provides durable same-directory file replacement.
package atomicfile

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFile writes data to a temporary file, flushes it, and atomically
// replaces path. The temporary file is always created on the same filesystem.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".atomic-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := Replace(tmpName, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	committed = true
	return nil
}

// WriteFileIfAbsent durably creates path without replacing an existing file.
// It is suitable for one-time secret material shared across process restarts.
func WriteFileIfAbsent(path string, data []byte, perm os.FileMode) (bool, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(dir, ".atomic-new-*.tmp")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(perm); err != nil {
		return false, err
	}
	if _, err := tmp.Write(data); err != nil {
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	created, err := replaceFileIfAbsent(tmpName, path)
	if err != nil {
		return false, fmt.Errorf("create %s: %w", path, err)
	}
	return created, nil
}

// Replace atomically replaces target with a fully-written temporary file.
func Replace(temporary, target string) error {
	if filepath.Dir(temporary) != filepath.Dir(target) {
		return fmt.Errorf("temporary file and target must share a directory")
	}
	return replaceFile(temporary, target)
}
