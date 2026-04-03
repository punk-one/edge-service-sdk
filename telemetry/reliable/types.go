package reliable

import (
	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// Config controls reliable telemetry delivery and replay.
type Config struct {
	Enabled          bool   `yaml:"enabled"`
	SQLitePath       string `yaml:"sqlitePath"`
	MemoryQueueSize  int    `yaml:"memoryQueueSize"`
	BatchSize        int    `yaml:"batchSize"`
	FlushIntervalMs  int    `yaml:"flushIntervalMs"`
	ReplayIntervalMs int    `yaml:"replayIntervalMs"`
	ReplayRatePerSec int    `yaml:"replayRatePerSec"`
	RetentionDays    int    `yaml:"retentionDays"`
	KeepLatestOnly   bool   `yaml:"keepLatestOnly"`
}

// TelemetryTransport publishes telemetry events to an outbound protocol.
type TelemetryTransport interface {
	PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error
}

// TelemetrySink is the runtime entry point used by async telemetry processing.
type TelemetrySink interface {
	PublishAsyncValues(device contracts.DeviceConfig, async *contracts.AsyncValues) error
	Close() error
}

// Store persists telemetry events for replay.
type Store interface {
	AppendBatch(events []outevent.TelemetryEvent) error
	FetchPending(limit int) ([]StoredEvent, error)
	Ack(ids []int64) error
	PurgeExpired(cutoffMillis int64) (int64, error)
	Stats() (StoreStats, error)
	Close() error
}

// StoredEvent is one persisted telemetry event waiting for replay.
type StoredEvent struct {
	ID        int64
	CreatedAt int64
	Event     outevent.TelemetryEvent
}

// StoreStats describes persisted queue depth and oldest record time.
type StoreStats struct {
	PendingCount           int64
	OldestPendingCreatedAt int64
}

// QueueStats describes runtime-visible queue metrics.
type QueueStats struct {
	BufferDepth        int64
	OldestPendingAgeMs int64
	ReplayRatePerSec   int
	LastReplayAt       int64
}

type persistRequest struct {
	event outevent.TelemetryEvent
}

type Dispatcher struct {
	cfg       Config
	logger    logger.LoggingClient
	transport TelemetryTransport
	store     Store
	enabled   bool

	persistCh chan persistRequest
	stopCh    chan struct{}

	replayTokens     float64
	lastReplayRefill int64
	lastReplayRate   int
	lastReplayAt     int64
}
