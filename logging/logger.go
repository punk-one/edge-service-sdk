package logger

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/sirupsen/logrus"
)

// LoggingClient defines the logging interface used by the service.
type LoggingClient interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Error(args ...interface{})
}

// Config controls logger output formatting and level.
type Config struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	File       string `yaml:"file"`
	MaxSize    int    `yaml:"maxSize"`
	MaxFiles   int    `yaml:"maxFiles"`
	MaxBackups int    `yaml:"maxBackups"`
	Compress   bool   `yaml:"compress"`
}

// Logger is a logrus-backed structured logger.
type Logger struct {
	entry *logrus.Entry
}

// NewLogger creates a new logger instance.
func NewLogger(serviceName string, config Config) *Logger {
	base := logrus.New()
	base.SetOutput(buildOutput(config))
	base.SetLevel(parseLogLevel(config.Level))
	base.SetFormatter(buildFormatter(config.Format))

	return &Logger{
		entry: logrus.NewEntry(base).WithField("service", serviceName),
	}
}

func buildOutput(config Config) io.Writer {
	path := strings.TrimSpace(config.File)
	if path == "" {
		return os.Stdout
	}

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}

	return &lumberjack.Logger{
		Filename:   path,
		MaxSize:    maxOrDefault(config.MaxSize, 100),
		MaxAge:     maxOrDefault(config.MaxFiles, 7),
		MaxBackups: maxOrDefault(config.MaxBackups, 3),
		Compress:   config.Compress,
	}
}

func maxOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func parseLogLevel(level string) logrus.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	case "info", "":
		return logrus.InfoLevel
	default:
		return logrus.InfoLevel
	}
}

func buildFormatter(format string) logrus.Formatter {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "text":
		return &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		}
	default:
		return &logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		}
	}
}

// Debugf logs a debug message.
func (l *Logger) Debugf(template string, args ...interface{}) {
	l.entry.Debugf(template, args...)
}

// Infof logs an info message.
func (l *Logger) Infof(template string, args ...interface{}) {
	l.entry.Infof(template, args...)
}

// Warnf logs a warning message.
func (l *Logger) Warnf(template string, args ...interface{}) {
	l.entry.Warnf(template, args...)
}

// Errorf logs an error message.
func (l *Logger) Errorf(template string, args ...interface{}) {
	l.entry.Errorf(template, args...)
}

// Error logs an error message.
func (l *Logger) Error(args ...interface{}) {
	l.entry.Error(args...)
}
