package reliable

import (
	"fmt"
	"math"
	"strings"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"
)

const telemetryCleanupInterval = time.Hour

// DefaultTelemetryOutboxConfig returns the SDK defaults for the mandatory
// SQLite-first telemetry delivery path.
func DefaultTelemetryOutboxConfig() TelemetryOutboxConfig {
	return TelemetryOutboxConfig{
		SQLitePath:        "./data/telemetry-outbox.db",
		RetentionDays:     0,
		SendBatchSize:     100,
		MaxSendRatePerSec: 100,
		RetryInitialMs:    1_000,
		RetryMaxMs:        30_000,
		MaxDatabaseBytes:  2 << 30,
	}
}

// NormalizeTelemetryOutboxConfig applies operational defaults. RetentionDays
// and MaxSendRatePerSec deliberately preserve zero (no expiry / no limit).
func NormalizeTelemetryOutboxConfig(cfg TelemetryOutboxConfig) TelemetryOutboxConfig {
	defaults := DefaultTelemetryOutboxConfig()
	if strings.TrimSpace(cfg.SQLitePath) == "" {
		cfg.SQLitePath = defaults.SQLitePath
	}
	if cfg.SendBatchSize == 0 {
		cfg.SendBatchSize = defaults.SendBatchSize
	}
	if cfg.RetryInitialMs == 0 {
		cfg.RetryInitialMs = defaults.RetryInitialMs
	}
	if cfg.RetryMaxMs == 0 {
		cfg.RetryMaxMs = defaults.RetryMaxMs
	}
	if cfg.MaxDatabaseBytes == 0 {
		cfg.MaxDatabaseBytes = defaults.MaxDatabaseBytes
	}
	return cfg
}

// ValidateTelemetryOutboxConfig rejects values that would make delivery or
// retry behavior ambiguous.
func ValidateTelemetryOutboxConfig(cfg TelemetryOutboxConfig) error {
	if cfg.RetentionDays < 0 {
		return fmt.Errorf("telemetryOutbox.retentionDays must be >= 0")
	}
	if cfg.SendBatchSize <= 0 {
		return fmt.Errorf("telemetryOutbox.sendBatchSize must be > 0")
	}
	if cfg.MaxSendRatePerSec < 0 {
		return fmt.Errorf("telemetryOutbox.maxSendRatePerSec must be >= 0")
	}
	if cfg.RetryInitialMs <= 0 {
		return fmt.Errorf("telemetryOutbox.retryInitialMs must be > 0")
	}
	if cfg.RetryMaxMs < cfg.RetryInitialMs {
		return fmt.Errorf("telemetryOutbox.retryMaxMs must be >= telemetryOutbox.retryInitialMs")
	}
	if cfg.MaxDatabaseBytes < 64<<20 {
		return fmt.Errorf("telemetryOutbox.maxDatabaseBytes must be >= 67108864")
	}
	return nil
}

// NewTelemetryDispatcher creates the single write-ahead telemetry dispatcher.
func NewTelemetryDispatcher(cfg TelemetryOutboxConfig, transport TelemetryTransport, logClient logger.LoggingClient) (*TelemetryDispatcher, error) {
	if transport == nil {
		return nil, fmt.Errorf("telemetry transport is nil")
	}
	cfg = NormalizeTelemetryOutboxConfig(cfg)
	if err := ValidateTelemetryOutboxConfig(cfg); err != nil {
		return nil, err
	}
	store, err := newSQLiteStore(cfg.SQLitePath)
	if err != nil {
		return nil, err
	}
	if err := store.ConfigureMaxBytes(cfg.MaxDatabaseBytes); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("configure telemetry outbox capacity: %w", err)
	}

	dispatcher := &TelemetryDispatcher{
		cfg:       cfg,
		logger:    logClient,
		transport: transport,
		store:     store,
		stopCh:    make(chan struct{}),
		wakeCh:    make(chan struct{}, 1),
		connectCh: make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
		online:    transportIsHealthy(transport),
	}
	dispatcher.purgeExpired()

	// Any row surviving process startup is recovery delivery, regardless of
	// whether the previous process attempted it before exiting.
	marked, err := store.MarkAllReplayed()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("mark startup telemetry as replayed: %w", err)
	}
	cutoff, err := store.MaxID()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("read startup telemetry cutoff: %w", err)
	}
	dispatcher.recoveryCutoff = cutoff
	if marked > 0 && logClient != nil {
		logClient.Infof("Telemetry outbox startup recovery: pending=%d cutoffId=%d", marked, cutoff)
	}

	if registrar, ok := transport.(connectRegistrar); ok {
		registrar.RegisterOnConnect(dispatcher.notifyConnected)
	}
	go dispatcher.run()
	return dispatcher, nil
}

// PublishAsyncValues commits telemetry to SQLite and only then wakes the MQTT
// sender. There is no realtime bypass or process-memory persistence queue.
func (d *TelemetryDispatcher) PublishAsyncValues(device contracts.DeviceConfig, async *contracts.AsyncValues) error {
	if d == nil {
		return fmt.Errorf("telemetry dispatcher is nil")
	}
	event, err := outevent.NewTelemetryEvent(device, async)
	if err != nil {
		return err
	}

	d.lifecycleMu.RLock()
	defer d.lifecycleMu.RUnlock()
	if d.closed {
		return fmt.Errorf("telemetry dispatcher is closed")
	}
	d.acceptanceMu.Lock()
	defer d.acceptanceMu.Unlock()
	replayed := !d.isOnline()
	rowID, err := d.store.Append(event, replayed, nowMillis())
	if err != nil {
		return fmt.Errorf("persist telemetry before publish: %w", err)
	}
	if d.logger != nil {
		d.logger.Debugf("Telemetry committed to outbox: id=%d device=%s traceId=%s isReplayed=%t", rowID, event.DeviceName, event.TraceID, replayed)
	}
	d.signalWake()
	return nil
}

// Close stops delivery after all in-progress SQLite or MQTT work returns. All
// unsent rows remain durable for the next process start.
func (d *TelemetryDispatcher) Close() error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() {
		d.lifecycleMu.Lock()
		d.closed = true
		close(d.stopCh)
		d.lifecycleMu.Unlock()
		<-d.doneCh
		d.closeErr = d.store.Close()
	})
	return d.closeErr
}

func (d *TelemetryDispatcher) Stats() (TelemetryOutboxStats, error) {
	if d == nil || d.store == nil {
		return TelemetryOutboxStats{}, nil
	}
	storeStats, err := d.store.Stats()
	if err != nil {
		return TelemetryOutboxStats{}, err
	}
	d.metricsMu.RLock()
	result := TelemetryOutboxStats{
		PendingCount:   storeStats.PendingCount,
		SendRatePerSec: d.lastSendRate,
		LastSendAt:     d.lastSendAt,
	}
	d.metricsMu.RUnlock()
	if sqlite, ok := d.store.(*sqliteStore); ok {
		result.DeadLetterCount, _ = sqlite.DeadLetterCount()
	}
	if storeStats.OldestPendingCreatedAt > 0 {
		result.OldestPendingAgeMs = nowMillis() - storeStats.OldestPendingCreatedAt
	}
	return result, nil
}

// HealthCheck verifies that the durable acceptance path is open and the
// SQLite outbox remains structurally readable. MQTT health is checked by the
// publisher separately.
func (d *TelemetryDispatcher) HealthCheck() error {
	if d == nil || d.store == nil {
		return fmt.Errorf("telemetry dispatcher is not initialized")
	}
	d.lifecycleMu.RLock()
	closed := d.closed
	d.lifecycleMu.RUnlock()
	if closed {
		return fmt.Errorf("telemetry dispatcher is closed")
	}
	if checker, ok := d.store.(interface{ HealthCheck() error }); ok {
		return checker.HealthCheck()
	}
	_, err := d.store.Stats()
	return err
}

func (d *TelemetryDispatcher) run() {
	defer close(d.doneCh)
	cleanupTicker := time.NewTicker(telemetryCleanupInterval)
	defer cleanupTicker.Stop()

	retryDelay := d.cfg.retryInitial()
	retryTimer := time.NewTimer(0)
	retryC := retryTimer.C
	defer retryTimer.Stop()

	stopRetry := func() {
		if !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryC = nil
	}
	scheduleRetry := func(delay time.Duration) {
		stopRetry()
		retryTimer.Reset(delay)
		retryC = retryTimer.C
	}

	for {
		attempt := false
		select {
		case <-d.stopCh:
			return
		case <-d.connectCh:
			d.acceptanceMu.Lock()
			d.refreshRecoveryCutoff()
			d.setOnline(true)
			d.acceptanceMu.Unlock()
			retryDelay = d.cfg.retryInitial()
			attempt = true
		case <-d.wakeCh:
			if d.isOnline() {
				attempt = true
			} else if retryC == nil {
				scheduleRetry(0)
			}
		case <-retryC:
			retryC = nil
			if !d.isOnline() {
				if !transportIsHealthy(d.transport) {
					scheduleRetry(retryDelay)
					retryDelay *= 2
					if maxDelay := d.cfg.retryMax(); retryDelay > maxDelay {
						retryDelay = maxDelay
					}
					continue
				}
				d.acceptanceMu.Lock()
				d.refreshRecoveryCutoff()
				d.setOnline(true)
				d.acceptanceMu.Unlock()
			}
			attempt = true
		case <-cleanupTicker.C:
			d.purgeExpired()
			attempt = d.isOnline()
		}

		if !attempt {
			continue
		}
		failed := d.drain()
		if failed {
			scheduleRetry(retryDelay)
			retryDelay *= 2
			if maxDelay := d.cfg.retryMax(); retryDelay > maxDelay {
				retryDelay = maxDelay
			}
			continue
		}
		stopRetry()
		retryDelay = d.cfg.retryInitial()
	}
}

// drain returns true when delivery must pause and retry later.
func (d *TelemetryDispatcher) drain() bool {
	d.purgeExpired()
	startedAt := time.Now()
	sent := 0
	for {
		select {
		case <-d.stopCh:
			d.recordSendMetrics(sent, startedAt)
			return false
		default:
		}

		cutoff := d.currentRecoveryCutoff()
		records, err := d.store.FetchPending(d.cfg.SendBatchSize, cutoff)
		if err != nil {
			d.logErrorf("Failed to fetch telemetry outbox: %v", err)
			d.recordSendMetrics(sent, startedAt)
			return true
		}
		if len(records) == 0 {
			if cutoff > 0 {
				d.clearRecoveryCutoff(cutoff)
				continue
			}
			d.recordSendMetrics(sent, startedAt)
			return false
		}

		for _, record := range records {
			sendAt := nowMillis()
			if err := d.store.MarkAttempt(record.ID, sendAt, record.IsReplayed); err != nil {
				d.logErrorf("Failed to persist telemetry send attempt: id=%d err=%v", record.ID, err)
				d.recordSendMetrics(sent, startedAt)
				return true
			}
			if err := d.publish(record.Event, record.IsReplayed, sendAt); err != nil {
				d.acceptanceMu.Lock()
				d.setOnline(false)
				if markErr := d.store.MarkFailed(record.ID, boundedError(err)); markErr != nil {
					d.logErrorf("Failed to mark telemetry replay after publish failure: id=%d err=%v", record.ID, markErr)
				}
				if _, markErr := d.store.MarkAllReplayed(); markErr != nil {
					d.logErrorf("Failed to mark pending telemetry as replayed after publish failure: %v", markErr)
				}
				d.acceptanceMu.Unlock()
				d.logWarnf("Telemetry delivery paused: id=%d device=%s traceId=%s err=%v", record.ID, record.Event.DeviceName, record.Event.TraceID, err)
				d.recordSendMetrics(sent, startedAt)
				return true
			}
			if err := d.store.Ack(record.ID); err != nil {
				if markErr := d.store.MarkFailed(record.ID, boundedError(err)); markErr != nil {
					d.logErrorf("Failed to preserve telemetry after ack failure: id=%d err=%v", record.ID, markErr)
				}
				d.logErrorf("MQTT accepted telemetry but SQLite ack failed: id=%d traceId=%s err=%v", record.ID, record.Event.TraceID, err)
				d.recordSendMetrics(sent, startedAt)
				return true
			}
			d.setOnline(true)
			sent++
			if !d.waitSendRate() {
				d.recordSendMetrics(sent, startedAt)
				return false
			}
		}
	}
}

func (d *TelemetryDispatcher) publish(event outevent.TelemetryEvent, replayed bool, sendAt int64) error {
	return d.transport.PublishTelemetryEventAt(event, replayed, sendAt)
}

func (d *TelemetryDispatcher) waitSendRate() bool {
	if d.cfg.MaxSendRatePerSec <= 0 {
		return true
	}
	delay := time.Second / time.Duration(d.cfg.MaxSendRatePerSec)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-d.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func (d *TelemetryDispatcher) purgeExpired() {
	cutoff := d.retentionCutoff()
	if cutoff <= 0 {
		return
	}
	removed, err := d.store.PurgeExpired(cutoff)
	if err != nil {
		d.logWarnf("Failed to purge expired telemetry outbox records: %v", err)
		return
	}
	if removed > 0 {
		d.logWarnf("Purged expired unsent telemetry: removed=%d retentionDays=%d", removed, d.cfg.RetentionDays)
	}
}

func (d *TelemetryDispatcher) retentionCutoff() int64 {
	if d.cfg.RetentionDays <= 0 {
		return 0
	}
	return time.Now().Add(-time.Duration(d.cfg.RetentionDays) * 24 * time.Hour).UnixMilli()
}

func (d *TelemetryDispatcher) notifyConnected() {
	select {
	case <-d.stopCh:
		return
	default:
	}
	select {
	case d.connectCh <- struct{}{}:
	default:
	}
}

func (d *TelemetryDispatcher) signalWake() {
	select {
	case <-d.stopCh:
		return
	default:
	}
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

func (d *TelemetryDispatcher) refreshRecoveryCutoff() {
	cutoff, err := d.store.MaxID()
	if err != nil {
		d.logWarnf("Failed to refresh telemetry recovery cutoff: %v", err)
		return
	}
	d.stateMu.Lock()
	if cutoff > d.recoveryCutoff {
		d.recoveryCutoff = cutoff
	}
	d.stateMu.Unlock()
}

func (d *TelemetryDispatcher) currentRecoveryCutoff() int64 {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.recoveryCutoff
}

func (d *TelemetryDispatcher) clearRecoveryCutoff(expected int64) {
	d.stateMu.Lock()
	if d.recoveryCutoff == expected {
		d.recoveryCutoff = 0
	}
	d.stateMu.Unlock()
}

func (d *TelemetryDispatcher) isOnline() bool {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	return d.online
}

func (d *TelemetryDispatcher) setOnline(value bool) {
	d.stateMu.Lock()
	d.online = value
	d.stateMu.Unlock()
}

func (d *TelemetryDispatcher) recordSendMetrics(sent int, startedAt time.Time) {
	if sent <= 0 {
		return
	}
	elapsed := time.Since(startedAt).Seconds()
	if elapsed < 1 {
		elapsed = 1
	}
	d.metricsMu.Lock()
	d.lastSendRate = int(math.Ceil(float64(sent) / elapsed))
	d.lastSendAt = nowMillis()
	d.metricsMu.Unlock()
}

func (d *TelemetryDispatcher) logWarnf(format string, args ...interface{}) {
	if d.logger != nil {
		d.logger.Warnf(format, args...)
	}
}

func (d *TelemetryDispatcher) logErrorf(format string, args ...interface{}) {
	if d.logger != nil {
		d.logger.Errorf(format, args...)
	}
}

func (c TelemetryOutboxConfig) retryInitial() time.Duration {
	return time.Duration(c.RetryInitialMs) * time.Millisecond
}

func (c TelemetryOutboxConfig) retryMax() time.Duration {
	return time.Duration(c.RetryMaxMs) * time.Millisecond
}

func transportIsHealthy(transport any) bool {
	health, ok := transport.(transportHealth)
	return !ok || health.HealthCheck() == nil
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	const maxLength = 2_048
	message := err.Error()
	if len(message) > maxLength {
		return message[:maxLength]
	}
	return message
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
