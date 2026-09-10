//go:build !windows

package configfile

import (
	"errors"
	"os"
	"path/filepath"
)

// PermissionsTooOpen reports whether group or other permission bits are set.
func PermissionsTooOpen(path string) (bool, error) {
	info, err := os.Stat(filepath.Clean(path))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Mode().Perm()&0o077 != 0, nil
}
