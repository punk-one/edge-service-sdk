//go:build !windows

package atomicfile

import (
	"os"
	"path/filepath"
)

func replaceFile(temporary, target string) error {
	if err := os.Rename(temporary, target); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func replaceFileIfAbsent(temporary, target string) (bool, error) {
	if err := os.Link(temporary, target); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := os.Remove(temporary); err != nil {
		return false, err
	}
	dir, err := os.Open(filepath.Dir(target))
	if err != nil {
		return false, err
	}
	defer dir.Close()
	return true, dir.Sync()
}
