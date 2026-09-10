package configfile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveAndListPreferJSON(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"config.yml":   "service: yml\n",
		"config.yaml":  "service: yaml\n",
		"config.json":  `{"service":"json"}`,
		"other.yml":    "enabled: true\n",
		"ignored.tmp":  "partial",
		".hidden.json": `{"ignored":true}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	resolved, exists, err := ResolvePreferred(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !exists || filepath.Base(resolved) != "config.json" {
		t.Fatalf("ResolvePreferred() = %q, %t; want config.json", resolved, exists)
	}

	files, err := ListPreferred(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || filepath.Base(files[0]) != "config.json" || filepath.Base(files[1]) != "other.yml" {
		t.Fatalf("ListPreferred() = %#v", files)
	}
}

func TestInvalidPreferredJSONDoesNotFallBackToYAML(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(jsonPath, []byte(`{"service":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("service: yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	resolved, exists, err := ResolvePreferred(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !exists || resolved != jsonPath {
		t.Fatalf("ResolvePreferred() = %q, %t; want invalid JSON path", resolved, exists)
	}
	var value map[string]interface{}
	if err := Read(resolved, &value); err == nil || !strings.Contains(err.Error(), "invalid JSON syntax") {
		t.Fatalf("Read() error = %v; want invalid JSON syntax", err)
	}
}

func TestJSONUsesExistingYAMLTags(t *testing.T) {
	type sample struct {
		LowerCamel string `yaml:"lowerCamel"`
	}
	path := filepath.Join(t.TempDir(), "sample.json")
	if err := os.WriteFile(path, []byte(`{"lowerCamel":"ok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var value sample
	if err := Read(path, &value); err != nil {
		t.Fatal(err)
	}
	if value.LowerCamel != "ok" {
		t.Fatalf("decoded value = %#v", value)
	}
}

func TestWriteMapKeepsJSONAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteMap(path, map[string]interface{}{"mqtt": map[string]interface{}{"password": "secret"}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("written file is not JSON: %v\n%s", err, data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}
}
