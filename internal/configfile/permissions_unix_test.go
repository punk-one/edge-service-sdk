//go:build !windows

package configfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPermissionsTooOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tooOpen, err := PermissionsTooOpen(path)
	if err != nil || tooOpen {
		t.Fatalf("0600 PermissionsTooOpen() = %t, %v", tooOpen, err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	tooOpen, err = PermissionsTooOpen(path)
	if err != nil || !tooOpen {
		t.Fatalf("0640 PermissionsTooOpen() = %t, %v", tooOpen, err)
	}
}
