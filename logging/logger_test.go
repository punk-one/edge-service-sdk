package logger

import (
	"path/filepath"
	"testing"

	"gopkg.in/natefinch/lumberjack.v2"
)

func TestBuildOutputFileOnlyWhenConsoleDisabled(t *testing.T) {
	output := buildOutput(Config{
		File:    filepath.Join(t.TempDir(), "app.log"),
		Console: false,
	})

	if _, ok := output.(*lumberjack.Logger); !ok {
		t.Fatalf("expected file-only lumberjack output, got %T", output)
	}
}

func TestBuildOutputDuplicatesToConsoleWhenEnabled(t *testing.T) {
	output := buildOutput(Config{
		File:    filepath.Join(t.TempDir(), "app.log"),
		Console: true,
	})

	if _, ok := output.(*lumberjack.Logger); ok {
		t.Fatalf("expected console and file multi-writer, got %T", output)
	}
}
