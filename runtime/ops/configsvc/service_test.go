package configsvc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	contracts "github.com/punk-one/edge-service-sdk/driver"
)

func TestConfigServiceUsesSelectedJSONAndRedactsSecrets(t *testing.T) {
	root := t.TempDir()
	devicesDir := filepath.Join(root, "devices")
	profilesDir := filepath.Join(root, "profiles")
	if err := os.MkdirAll(devicesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(profilesDir, 0o755); err != nil {
		t.Fatal(err)
	}

	mainJSON := filepath.Join(root, "config.json")
	if err := os.WriteFile(mainJSON, []byte(`{"service":{"port":1000},"mqtt":{"username":"edge","password":"old-secret"},"auth":{"bootstrapToken":"bootstrap-secret"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "config.yaml"), []byte("mqtt:\n  password: stale-yaml-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deviceJSON := filepath.Join(devicesDir, "line.json")
	if err := os.WriteFile(deviceJSON, []byte(`{"deviceList":[{"name":"device-1","profileName":"profile-1","protocols":{"s7":{"address":"old"}}}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(devicesDir, "line.yaml"), []byte("deviceList:\n  - name: stale-device\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	profileJSON := filepath.Join(profilesDir, "base.json")
	if err := os.WriteFile(profileJSON, []byte(`{"name":"profile-1","telemetry":{"interval":"1s"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	service := NewConfigService(root, []contracts.DeviceConfig{{Name: "device-1", ProfileName: "profile-1"}}, nil)
	if service.GetMainConfigPath() != mainJSON {
		t.Fatalf("main config path = %q, want %q", service.GetMainConfigPath(), mainJSON)
	}
	if service.DeviceFiles()["device-1"] != deviceJSON {
		t.Fatalf("device files = %#v", service.DeviceFiles())
	}
	if service.ProfileFiles()["profile-1"] != profileJSON {
		t.Fatalf("profile files = %#v", service.ProfileFiles())
	}

	password, err := service.GetConfig("mqtt.password")
	if err != nil {
		t.Fatal(err)
	}
	if password.Value != redactedConfigValue || password.Configured == nil || !*password.Configured {
		t.Fatalf("password value = %#v", password.Value)
	}
	mqttConfig, err := service.GetConfig("mqtt")
	if err != nil {
		t.Fatal(err)
	}
	mqttMap, ok := mqttConfig.Value.(map[string]interface{})
	if !ok || mqttMap["password"] != redactedConfigValue || mqttMap["username"] != "edge" {
		t.Fatalf("redacted mqtt config = %#v", mqttConfig.Value)
	}

	var change ConfigChange
	service.SetOnChange(func(value ConfigChange) { change = value })
	setResult, err := service.SetConfig("mqtt.password", "new-secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if setResult.PreviousValue != redactedConfigValue || setResult.CurrentValue != redactedConfigValue {
		t.Fatalf("set result leaked secret: %#v", setResult)
	}
	if change.OldValue != redactedConfigValue || change.NewValue != redactedConfigValue {
		t.Fatalf("change callback leaked secret: %#v", change)
	}
	overrides := service.GetOverrides()
	if len(overrides) != 1 || overrides[0].Value != redactedConfigValue {
		t.Fatalf("overrides leaked secret: %#v", overrides)
	}

	mainData := readJSONMap(t, mainJSON)
	if mainData["mqtt"].(map[string]interface{})["password"] != "new-secret" {
		t.Fatalf("persisted config = %#v", mainData)
	}
	info, err := os.Stat(mainJSON)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("main config mode = %o, want 600", info.Mode().Perm())
	}
	if _, err := service.SetConfig("service.port", 70000, true); err == nil {
		t.Fatal("invalid service port was persisted")
	}
	mainData = readJSONMap(t, mainJSON)
	if mainData["service"].(map[string]interface{})["port"] != float64(1000) {
		t.Fatalf("invalid candidate changed main config: %#v", mainData)
	}

	if _, err := service.SetDeviceConfig("device-1", "protocols.s7.address", "new", true, "device"); err != nil {
		t.Fatal(err)
	}
	deviceData := readJSONMap(t, deviceJSON)
	deviceEntry := deviceData["deviceList"].([]interface{})[0].(map[string]interface{})
	address := deviceEntry["protocols"].(map[string]interface{})["s7"].(map[string]interface{})["address"]
	if address != "new" {
		t.Fatalf("device address = %#v", address)
	}
	if _, err := service.SetDeviceConfig("device-1", "profileName", "missing-profile", true, "device"); err == nil {
		t.Fatal("invalid profile reference was persisted")
	}
	deviceData = readJSONMap(t, deviceJSON)
	deviceEntry = deviceData["deviceList"].([]interface{})[0].(map[string]interface{})
	if deviceEntry["profileName"] != "profile-1" {
		t.Fatalf("invalid candidate changed device config: %#v", deviceEntry)
	}

	if _, err := service.SetProfileConfig("profile-1", "telemetry.interval", "2s", true); err != nil {
		t.Fatal(err)
	}
	profileData := readJSONMap(t, profileJSON)
	if profileData["telemetry"].(map[string]interface{})["interval"] != "2s" {
		t.Fatalf("profile config = %#v", profileData)
	}
}

func readJSONMap(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("%s is not valid JSON: %v", path, err)
	}
	return result
}

func TestConfigServiceSerializesConcurrentPersistentUpdates(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"service":{"host":"old","port":1000}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	service := NewConfigService(root, nil, nil)

	start := make(chan struct{})
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	for _, update := range []struct {
		path  string
		value interface{}
	}{
		{path: "service.host", value: "new-host"},
		{path: "service.port", value: 2000},
	} {
		wait.Add(1)
		go func(updatePath string, value interface{}) {
			defer wait.Done()
			<-start
			_, err := service.SetConfig(updatePath, value, true)
			errors <- err
		}(update.path, update.value)
	}
	close(start)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}

	data := readJSONMap(t, path)
	serviceConfig := data["service"].(map[string]interface{})
	if serviceConfig["host"] != "new-host" || serviceConfig["port"] != float64(2000) {
		t.Fatalf("concurrent updates were not both persisted: %#v", serviceConfig)
	}
}
