package reliable

import (
	"sync"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// TelemetryOutboxConfig controls the write-ahead telemetry outbox. Every
// reportable telemetry message is committed to SQLite before MQTT delivery.
type TelemetryOutboxConfig struct {
	SQLitePath        string `yaml:"sqlitePath"`
	RetentionDays     int    `yaml:"retentionDays"`
	SendBatchSize     int    `yaml:"sendBatchSize"`
	MaxSendRatePerSec int    `yaml:"maxSendRatePerSec"`
	RetryInitialMs    int    `yaml:"retryInitialMs"`
	RetryMaxMs        int    `yaml:"retryMaxMs"`
}

// EventOutboxConfig is intentionally separate from telemetry configuration.
// EVENT continues to use runtime.db and its existing event_outbox table.
type EventOutboxConfig struct {
	Enabled          bool
	SQLitePath       string
	BatchSize        int
	ReplayIntervalMs int
	ReplayRatePerSec int
	RetentionDays    int
}

// TelemetryTransport publishes telemetry with the exact send_at selected and
// persisted by the outbox.
type TelemetryTransport interface {
	PublishTelemetryEventAt(event outevent.TelemetryEvent, replayed bool, sendAt int64) error
}

type connectRegistrar interface {
	RegisterOnConnect(hook func())
}

type transportHealth interface {
	HealthCheck() error
}

// TelemetrySink is the runtime entry point used by telemetry processing.
type TelemetrySink interface {
	PublishAsyncValues(device contracts.DeviceConfig, async *contracts.AsyncValues) error
	Stats() (TelemetryOutboxStats, error)
	Close() error
}

type telemetryStore interface {
	Append(event outevent.TelemetryEvent, replayed bool, createdAt int64) (int64, error)
	MarkAllReplayed() (int64, error)
	MaxID() (int64, error)
	FetchPending(limit int, cutoffID int64) ([]StoredTelemetry, error)
	MarkAttempt(id, sendAt int64, replayed bool) error
	MarkFailed(id int64, message string) error
	Ack(id int64) error
	PurgeExpired(cutoffMillis int64) (int64, error)
	Stats() (StoreStats, error)
	Close() error
}

// StoredTelemetry is one pending telemetry delivery.
type StoredTelemetry struct {
	ID               int64
	Time             int64
	SendAt           int64
	HasSendAt        bool
	IsReplayed       bool
	CreatedAt        int64
	DeliveryAttempts int64
	Event            outevent.TelemetryEvent
}

// StoreStats describes persisted outbox depth and age.
type StoreStats struct {
	PendingCount           int64
	OldestPendingCreatedAt int64
}

// TelemetryOutboxStats describes runtime-visible telemetry delivery metrics.
type TelemetryOutboxStats struct {
	PendingCount       int64
	OldestPendingAgeMs int64
	SendRatePerSec     int
	LastSendAt         int64
}

// TelemetryDispatcher owns the single telemetry SQLite-to-MQTT delivery path.
type TelemetryDispatcher struct {
	cfg       TelemetryOutboxConfig
	logger    logger.LoggingClient
	transport TelemetryTransport
	store     telemetryStore

	stopCh    chan struct{}
	wakeCh    chan struct{}
	connectCh chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
	closeErr  error

	lifecycleMu  sync.RWMutex
	closed       bool
	acceptanceMu sync.Mutex

	stateMu        sync.Mutex
	online         bool
	recoveryCutoff int64

	metricsMu    sync.RWMutex
	lastSendRate int
	lastSendAt   int64
}
