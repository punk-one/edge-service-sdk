package reliable

import (
	"fmt"
	"math"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

// NewDispatcher creates the reliable telemetry dispatcher.
func NewDispatcher(cfg Config, transport TelemetryTransport, logClient logger.LoggingClient) (*Dispatcher, error) {
	dispatcher := &Dispatcher{
		cfg:       normalizeConfig(cfg),
		logger:    logClient,
		transport: transport,
		enabled:   cfg.Enabled,
		stopCh:    make(chan struct{}),
	}

	if dispatcher.enabled {
		store, err := newSQLiteStore(dispatcher.cfg.SQLitePath, dispatcher.cfg.KeepLatestOnly)
		if err != nil {
			return nil, err
		}
		dispatcher.store = store
		dispatcher.persistCh = make(chan persistRequest, dispatcher.cfg.memoryQueueSize())
		dispatcher.replayTokens = float64(dispatcher.cfg.replayRatePerSec())
		dispatcher.lastReplayRefill = time.Now().UnixMilli()
		go dispatcher.persistLoop()
		go dispatcher.replayLoop()
	}

	return dispatcher, nil
}

// NewPassthroughSink creates a no-buffer telemetry sink.
func NewPassthroughSink(transport TelemetryTransport, logClient logger.LoggingClient) *Dispatcher {
	return &Dispatcher{
		cfg:       normalizeConfig(Config{}),
		logger:    logClient,
		transport: transport,
		enabled:   false,
		stopCh:    make(chan struct{}),
	}
}

// PublishAsyncValues attempts realtime delivery and falls back to durable queueing.
func (d *Dispatcher) PublishAsyncValues(device contracts.DeviceConfig, async *contracts.AsyncValues) error {
	event, err := outevent.NewTelemetryEvent(device, async)
	if err != nil {
		return err
	}

	if err := d.transport.PublishTelemetryEvent(event, false); err == nil {
		return nil
	} else if !d.enabled {
		return err
	} else if persistErr := d.enqueuePersist(event); persistErr != nil {
		return fmt.Errorf("publish realtime telemetry: %w; persist telemetry: %v", err, persistErr)
	}

	d.logger.Warnf("Realtime publish failed, queued telemetry for replay: device=%s traceId=%s", event.DeviceName, event.TraceID)
	return nil
}

// Close stops background workers and flushes in-memory pending events.
func (d *Dispatcher) Close() error {
	if d == nil {
		return nil
	}

	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}

	if d.store != nil {
		return d.store.Close()
	}
	return nil
}

// Stats returns current queue depth, oldest backlog age, and replay metrics.
func (d *Dispatcher) Stats() (QueueStats, error) {
	stats := QueueStats{
		ReplayRatePerSec: d.lastReplayRate,
		LastReplayAt:     d.lastReplayAt,
	}
	if d.store == nil {
		return stats, nil
	}

	storeStats, err := d.store.Stats()
	if err != nil {
		return QueueStats{}, err
	}

	stats.BufferDepth = storeStats.PendingCount
	if storeStats.OldestPendingCreatedAt > 0 {
		stats.OldestPendingAgeMs = nowMillis() - storeStats.OldestPendingCreatedAt
	}
	return stats, nil
}

func (d *Dispatcher) enqueuePersist(event outevent.TelemetryEvent) error {
	if !d.enabled || d.store == nil {
		return fmt.Errorf("reliable queue is disabled")
	}

	req := persistRequest{event: event}
	select {
	case d.persistCh <- req:
		return nil
	default:
		return d.store.AppendBatch([]outevent.TelemetryEvent{event})
	}
}

func (d *Dispatcher) persistLoop() {
	ticker := time.NewTicker(d.cfg.flushInterval())
	defer ticker.Stop()

	buffer := make([]outevent.TelemetryEvent, 0, d.cfg.batchSize())
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		if err := d.store.AppendBatch(buffer); err != nil {
			d.logger.Errorf("Failed to persist telemetry buffer batch: %v", err)
			return
		}
		d.logQueueStats("persisted telemetry batch")
		buffer = buffer[:0]
	}

	for {
		select {
		case <-d.stopCh:
			flush()
			return
		case req := <-d.persistCh:
			buffer = append(buffer, req.event)
			if len(buffer) >= d.cfg.batchSize() {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (d *Dispatcher) replayLoop() {
	ticker := time.NewTicker(d.cfg.replayInterval())
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.replayOnce()
		}
	}
}

func (d *Dispatcher) replayOnce() {
	if d.store == nil {
		return
	}

	if removed, err := d.store.PurgeExpired(d.retentionCutoff()); err != nil {
		d.logger.Warnf("Failed to purge expired telemetry records: %v", err)
	} else if removed > 0 {
		d.logger.Infof("Purged expired telemetry records: removed=%d", removed)
	}

	limit := d.availableReplayBudget(time.Now())
	if limit <= 0 {
		return
	}

	records, err := d.store.FetchPending(limit)
	if err != nil {
		d.logger.Warnf("Failed to fetch pending telemetry records: %v", err)
		return
	}
	if len(records) == 0 {
		return
	}

	acked := make([]int64, 0, len(records))
	for _, record := range records {
		if err := d.transport.PublishTelemetryEvent(record.Event, true); err != nil {
			d.logger.Warnf("Telemetry replay paused on publish failure: device=%s traceId=%s err=%v", record.Event.DeviceName, record.Event.TraceID, err)
			break
		}
		acked = append(acked, record.ID)
		d.consumeReplayToken()
	}

	if err := d.store.Ack(acked); err != nil {
		d.logger.Warnf("Failed to ack replayed telemetry records: %v", err)
		return
	}

	if len(acked) > 0 {
		intervalSeconds := d.cfg.replayInterval().Seconds()
		if intervalSeconds <= 0 {
			intervalSeconds = 1
		}
		d.lastReplayRate = int(math.Ceil(float64(len(acked)) / intervalSeconds))
		d.lastReplayAt = nowMillis()

		stats, statsErr := d.Stats()
		if statsErr != nil {
			d.logger.Infof("Replayed telemetry records: acked=%d replayRate=%d/s", len(acked), d.lastReplayRate)
			return
		}
		d.logger.Infof(
			"Replayed telemetry records: acked=%d replayRate=%d/s bufferDepth=%d oldestPendingAgeMs=%d",
			len(acked),
			d.lastReplayRate,
			stats.BufferDepth,
			stats.OldestPendingAgeMs,
		)
	}
}

func (c Config) memoryQueueSize() int {
	if c.MemoryQueueSize > 0 {
		return c.MemoryQueueSize
	}
	return 2048
}

func (c Config) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return 100
}

func (c Config) flushInterval() time.Duration {
	if c.FlushIntervalMs > 0 {
		return time.Duration(c.FlushIntervalMs) * time.Millisecond
	}
	return time.Second
}

func (c Config) replayInterval() time.Duration {
	if c.ReplayIntervalMs > 0 {
		return time.Duration(c.ReplayIntervalMs) * time.Millisecond
	}
	return 3 * time.Second
}

func (c Config) replayRatePerSec() int {
	if c.ReplayRatePerSec > 0 {
		return c.ReplayRatePerSec
	}
	return 20
}

func normalizeConfig(cfg Config) Config {
	if cfg.SQLitePath == "" {
		cfg.SQLitePath = "./data/reliable-queue.db"
	}
	return cfg
}

func (d *Dispatcher) retentionCutoff() int64 {
	if d.cfg.RetentionDays <= 0 {
		return 0
	}
	return time.Now().Add(-time.Duration(d.cfg.RetentionDays) * 24 * time.Hour).UnixMilli()
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

func (d *Dispatcher) availableReplayBudget(now time.Time) int {
	rate := d.cfg.replayRatePerSec()
	if rate <= 0 {
		return d.cfg.batchSize()
	}

	lastRefill := time.UnixMilli(d.lastReplayRefill)
	if lastRefill.IsZero() {
		lastRefill = now
	}
	elapsed := now.Sub(lastRefill).Seconds()
	if elapsed > 0 {
		d.replayTokens = math.Min(float64(rate), d.replayTokens+elapsed*float64(rate))
		d.lastReplayRefill = now.UnixMilli()
	}

	available := int(math.Floor(d.replayTokens))
	if available <= 0 {
		return 0
	}
	if available > d.cfg.batchSize() {
		return d.cfg.batchSize()
	}
	return available
}

func (d *Dispatcher) consumeReplayToken() {
	if d.cfg.replayRatePerSec() <= 0 {
		return
	}
	if d.replayTokens >= 1 {
		d.replayTokens -= 1
	}
}

func (d *Dispatcher) logQueueStats(context string) {
	stats, err := d.Stats()
	if err != nil {
		d.logger.Warnf("Failed to collect queue stats after %s: %v", context, err)
		return
	}
	d.logger.Infof(
		"Reliable queue stats: context=%s bufferDepth=%d oldestPendingAgeMs=%d replayRate=%d/s",
		context,
		stats.BufferDepth,
		stats.OldestPendingAgeMs,
		stats.ReplayRatePerSec,
	)
}
