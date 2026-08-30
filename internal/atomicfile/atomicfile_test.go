package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicallyReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := WriteFile(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("content = %q, want new", data)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".atomic-*.tmp"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files = %#v err=%v", matches, err)
	}
}

func TestWriteFileIfAbsentDoesNotReplaceExistingContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret.key")
	created, err := WriteFileIfAbsent(path, []byte("first"), 0o600)
	if err != nil || !created {
		t.Fatalf("first WriteFileIfAbsent() created=%t err=%v", created, err)
	}
	created, err = WriteFileIfAbsent(path, []byte("second"), 0o600)
	if err != nil || created {
		t.Fatalf("second WriteFileIfAbsent() created=%t err=%v", created, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "first" {
		t.Fatalf("contents = %q, want first", data)
	}
}
