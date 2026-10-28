package config

import "testing"

func TestLoggingConsoleDefaultsToTrueWhenOmitted(t *testing.T) {
	cfg, err := DecodeMainConfig("config.json", []byte(`{"logging":{"level":"info","file":"./logs/app.log"}}`))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if !cfg.Logging.Console {
		t.Fatal("expected omitted logging.console to retain compatibility default true")
	}
}

func TestLoggingConsoleCanBeDisabled(t *testing.T) {
	cfg, err := DecodeMainConfig("config.json", []byte(`{"logging":{"level":"info","file":"./logs/app.log","console":false}}`))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if cfg.Logging.Console {
		t.Fatal("expected explicit logging.console=false")
	}
}
