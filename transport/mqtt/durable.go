package mqtt

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	contracts "github.com/punk-one/edge-service-sdk/driver"
	events "github.com/punk-one/edge-service-sdk/event"
	"github.com/punk-one/edge-service-sdk/internal/sqliteutil"
	logger "github.com/punk-one/edge-service-sdk/logging"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"

	_ "modernc.org/sqlite"
)

const (
	durableKindTelemetry       = "telemetry"
	durableKindTelemetryEvent  = "telemetry_event"
	durableKindCommandValues   = "command_values"
	durableKindPropertyResult  = "property_result"
	durableKindPropertyReport  = "property_report"
	durableKindCommandResult   = "command_result"
	durableKindEvent           = "event"
	durableDefaultDestination  = "default"
	durableDefaultRetryInitial = time.Second
	durableDefaultRetryMax     = 30 * time.Second
)

// DurablePublisherConfig controls the destination-aware MQTT write-ahead
// queue. A call succeeds after every configured destination has been recorded
// in SQLite; each destination is then delivered and acknowledged separately.
type DurablePublisherConfig struct {
	SQLitePath       string
	MaxDatabaseBytes int64
	RetryInitial     time.Duration
	RetryMax         time.Duration
}

// DurableQueueStats exposes the network-side queue separately from the
// telemetry acceptance queue. PerDestination makes one degraded MQTT group
// visible even while other groups continue draining.
type DurableQueueStats struct {
	PendingCount       int64
	OldestPendingAgeMs int64
	DeadLetterCount    int64
	PerDestination     map[string]int64
}

type durableEnvelope struct {
	Device        contracts.DeviceConfig    `json:"device,omitempty"`
	Payload       map[string]interface{}    `json:"payload,omitempty"`
	Telemetry     *outevent.TelemetryEvent  `json:"telemetry,omitempty"`
	Event         *events.Event             `json:"event,omitempty"`
	CommandValues []*contracts.CommandValue `json:"command_values,omitempty"`
}

type durableRecord struct {
	ID          int64
	Destination string
	Kind        string
	Envelope    durableEnvelope
	Replayed    bool
	Attempts    int64
}

type durablePublisherStore struct {
	db *sql.DB
}

// durablePublisher preserves the existing Publisher API while moving the
// acceptance boundary in front of network I/O. This also makes property
// reports durable in single-broker deployments.
type durablePublisher struct {
	base         Publisher
	logger       logger.LoggingClient
	store        *durablePublisherStore
	targets      map[string]Publisher
	destinations []string
	retryInitial time.Duration
	retryMax     time.Duration

	stopCh    chan struct{}
	wake      map[string]chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
	mu        sync.RWMutex
	closed    bool
}

type durableMultiPublisher struct {
	*durablePublisher
}

// NewDurablePublisher wraps an MQTT publisher with a SQLite-first,
// destination-aware delivery queue. Multi-group publishers receive one row
// and one ACK per named group; single publishers use the default destination.
func NewDurablePublisher(base Publisher, cfg DurablePublisherConfig, logClient logger.LoggingClient) (Publisher, error) {
	if base == nil {
		return nil, fmt.Errorf("mqtt publisher is nil")
	}
	if strings.TrimSpace(cfg.SQLitePath) == "" {
		return nil, fmt.Errorf("durable mqtt sqlite path is empty")
	}
	if cfg.MaxDatabaseBytes == 0 {
		cfg.MaxDatabaseBytes = sqliteutil.DefaultMaxBytes
	}
	if cfg.MaxDatabaseBytes < sqliteutil.MinimumMaxBytes {
		return nil, fmt.Errorf("durable mqtt max database bytes must be >= %d", sqliteutil.MinimumMaxBytes)
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = durableDefaultRetryInitial
	}
	if cfg.RetryMax < cfg.RetryInitial {
		cfg.RetryMax = durableDefaultRetryMax
		if cfg.RetryMax < cfg.RetryInitial {
			cfg.RetryMax = cfg.RetryInitial
		}
	}

	store, err := newDurablePublisherStore(cfg.SQLitePath, cfg.MaxDatabaseBytes)
	if err != nil {
		return nil, err
	}
	targets, destinations, err := durableDestinations(base)
	if err != nil {
		_ = store.close()
		return nil, err
	}
	if err := store.validateDestinations(destinations); err != nil {
		_ = store.close()
		return nil, err
	}
	d := &durablePublisher{
		base:         base,
		logger:       logClient,
		store:        store,
		targets:      targets,
		destinations: destinations,
		retryInitial: cfg.RetryInitial,
		retryMax:     cfg.RetryMax,
		stopCh:       make(chan struct{}),
		wake:         make(map[string]chan struct{}, len(destinations)),
	}
	for _, destination := range destinations {
		d.wake[destination] = make(chan struct{}, 1)
		d.wg.Add(1)
		go d.runDestination(destination)
	}
	if registrar, ok := base.(interface{ RegisterOnConnect(func()) }); ok {
		registrar.RegisterOnConnect(d.signalAll)
	}
	d.signalAll()
	if _, ok := base.(MultiGroupPublisher); ok {
		return &durableMultiPublisher{durablePublisher: d}, nil
	}
	return d, nil
}

func durableDestinations(base Publisher) (map[string]Publisher, []string, error) {
	targets := map[string]Publisher{}
	if multi, ok := base.(MultiGroupPublisher); ok {
		groups := multi.GroupPublishers()
		destinations := make([]string, 0, len(groups))
		for i, target := range groups {
			name := strings.TrimSpace(multi.GroupName(i))
			if name == "" {
				return nil, nil, fmt.Errorf("mqtt group %d has no stable name", i)
			}
			if target == nil {
				return nil, nil, fmt.Errorf("mqtt group %s has no publisher", name)
			}
			if _, exists := targets[name]; exists {
				return nil, nil, fmt.Errorf("duplicate mqtt group name %q", name)
			}
			targets[name] = target
			destinations = append(destinations, name)
		}
		if len(destinations) == 0 {
			return nil, nil, fmt.Errorf("multi mqtt publisher has no destinations")
		}
		return targets, destinations, nil
	}
	targets[durableDefaultDestination] = base
	return targets, []string{durableDefaultDestination}, nil
}

func newDurablePublisherStore(path string, maxBytes int64) (*durablePublisherStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &durablePublisherStore{db: db}
	for _, statement := range []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = FULL;",
		"PRAGMA busy_timeout = 5000;",
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS mqtt_destination_outbox (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	dedupe_key TEXT NOT NULL,
	destination TEXT NOT NULL,
	kind TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	replayed INTEGER NOT NULL DEFAULT 0 CHECK (replayed IN (0, 1)),
	created_at INTEGER NOT NULL,
	delivery_attempts INTEGER NOT NULL DEFAULT 0,
	last_attempt_at INTEGER,
	last_error TEXT,
	UNIQUE(dedupe_key, destination)
);
CREATE INDEX IF NOT EXISTS idx_mqtt_destination_pending
	ON mqtt_destination_outbox(destination, id);
CREATE TABLE IF NOT EXISTS mqtt_destination_dead_letter (
	id INTEGER PRIMARY KEY,
	dedupe_key TEXT NOT NULL,
	destination TEXT NOT NULL,
	kind TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	reason TEXT NOT NULL,
	quarantined_at INTEGER NOT NULL
);`); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Everything left at startup crossed a process boundary and is replay.
	if _, err := db.Exec(`UPDATE mqtt_destination_outbox
		SET replayed = 1
		WHERE kind IN (?, ?, ?)`, durableKindTelemetry, durableKindTelemetryEvent, durableKindEvent); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := sqliteutil.ConfigureMaxBytes(db, maxBytes); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure durable mqtt capacity: %w", err)
	}
	return s, nil
}

func (s *durablePublisherStore) append(destinations []string, kind, key string, payload []byte, replayed bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().UnixMilli()
	for _, destination := range destinations {
		if _, err := tx.Exec(`
INSERT INTO mqtt_destination_outbox(
	dedupe_key, destination, kind, payload_json, replayed, created_at
) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(dedupe_key, destination) DO NOTHING`, key, destination, kind, string(payload), boolToInt(replayed), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *durablePublisherStore) validateDestinations(configured []string) error {
	allowed := make(map[string]struct{}, len(configured))
	for _, destination := range configured {
		allowed[destination] = struct{}{}
	}
	rows, err := s.db.Query(`SELECT DISTINCT destination FROM mqtt_destination_outbox`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var destination string
		if err := rows.Scan(&destination); err != nil {
			return err
		}
		if _, ok := allowed[destination]; !ok {
			return fmt.Errorf("durable MQTT outbox contains pending destination %q which is not configured; restore that group or drain/migrate its rows", destination)
		}
	}
	return rows.Err()
}

func (s *durablePublisherStore) next(destination string) (durableRecord, bool, error) {
	for {
		var record durableRecord
		var raw string
		var replayed int
		err := s.db.QueryRow(`
SELECT id, destination, kind, payload_json, replayed, delivery_attempts
FROM mqtt_destination_outbox
WHERE destination = ?
ORDER BY id ASC
LIMIT 1`, destination).Scan(&record.ID, &record.Destination, &record.Kind, &raw, &replayed, &record.Attempts)
		if err == sql.ErrNoRows {
			return durableRecord{}, false, nil
		}
		if err != nil {
			return durableRecord{}, false, err
		}
		if err := json.Unmarshal([]byte(raw), &record.Envelope); err != nil {
			if quarantineErr := s.quarantine(record.ID, err.Error()); quarantineErr != nil {
				return durableRecord{}, false, fmt.Errorf("decode durable mqtt row %d: %v; quarantine: %w", record.ID, err, quarantineErr)
			}
			continue
		}
		record.Replayed = replayed != 0
		return record, true, nil
	}
}

func (s *durablePublisherStore) markAttempt(id int64) error {
	result, err := s.db.Exec(`UPDATE mqtt_destination_outbox
		SET delivery_attempts = delivery_attempts + 1, last_attempt_at = ?, last_error = NULL
		WHERE id = ?`, time.Now().UnixMilli(), id)
	if err != nil {
		return err
	}
	return requireDurableRow(result, "mark durable MQTT attempt")
}

func (s *durablePublisherStore) fail(id int64, message string) error {
	result, err := s.db.Exec(`UPDATE mqtt_destination_outbox
		SET replayed = 1, last_error = ? WHERE id = ?`, boundedMQTTError(message), id)
	if err != nil {
		return err
	}
	return requireDurableRow(result, "mark durable MQTT failure")
}

func (s *durablePublisherStore) ack(id int64) error {
	result, err := s.db.Exec(`DELETE FROM mqtt_destination_outbox WHERE id = ?`, id)
	if err != nil {
		return err
	}
	return requireDurableRow(result, "ack durable MQTT delivery")
}

func (s *durablePublisherStore) quarantine(id int64, reason string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
INSERT OR IGNORE INTO mqtt_destination_dead_letter(
	id, dedupe_key, destination, kind, payload_json, reason, quarantined_at
)
SELECT id, dedupe_key, destination, kind, payload_json, ?, ?
FROM mqtt_destination_outbox WHERE id = ?`, boundedMQTTError(reason), time.Now().UnixMilli(), id); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM mqtt_destination_outbox WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *durablePublisherStore) healthCheck() error {
	var result string
	if err := s.db.QueryRow(`PRAGMA quick_check(1);`).Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("durable mqtt sqlite quick_check: %s", result)
	}
	return sqliteutil.CheckCapacity(s.db)
}

func (s *durablePublisherStore) stats() (DurableQueueStats, error) {
	stats := DurableQueueStats{PerDestination: map[string]int64{}}
	var oldest int64
	if err := s.db.QueryRow(`SELECT COUNT(1), COALESCE(MIN(created_at), 0)
		FROM mqtt_destination_outbox`).Scan(&stats.PendingCount, &oldest); err != nil {
		return DurableQueueStats{}, err
	}
	if oldest > 0 {
		stats.OldestPendingAgeMs = time.Now().UnixMilli() - oldest
	}
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM mqtt_destination_dead_letter`).Scan(&stats.DeadLetterCount); err != nil {
		return DurableQueueStats{}, err
	}
	rows, err := s.db.Query(`SELECT destination, COUNT(1)
		FROM mqtt_destination_outbox GROUP BY destination ORDER BY destination`)
	if err != nil {
		return DurableQueueStats{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var destination string
		var count int64
		if err := rows.Scan(&destination, &count); err != nil {
			return DurableQueueStats{}, err
		}
		stats.PerDestination[destination] = count
	}
	return stats, rows.Err()
}

func (s *durablePublisherStore) close() error { return s.db.Close() }

func (d *durablePublisher) enqueue(kind, explicitKey string, envelope durableEnvelope, replayed bool) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return fmt.Errorf("durable mqtt publisher is closed")
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal durable MQTT %s: %w", kind, err)
	}
	key := durableKey(kind, explicitKey, payload)
	if err := d.store.append(d.destinations, kind, key, payload, replayed); err != nil {
		return fmt.Errorf("persist MQTT %s for all destinations: %w", kind, err)
	}
	d.signalAll()
	return nil
}

func durableKey(kind, explicit string, payload []byte) string {
	if explicit = strings.TrimSpace(explicit); explicit != "" {
		return kind + ":" + explicit
	}
	sum := sha256.Sum256(payload)
	return kind + ":sha256:" + hex.EncodeToString(sum[:])
}

func (d *durablePublisher) runDestination(destination string) {
	defer d.wg.Done()
	delay := d.retryInitial
	for {
		record, found, err := d.store.next(destination)
		if err != nil {
			d.logError("load", destination, 0, err)
			if !d.wait(destination, delay) {
				return
			}
			delay = nextDurableDelay(delay, d.retryMax)
			continue
		}
		if !found {
			delay = d.retryInitial
			if !d.wait(destination, 0) {
				return
			}
			continue
		}
		if err := d.store.markAttempt(record.ID); err != nil {
			d.logError("mark attempt", destination, record.ID, err)
			if !d.wait(destination, delay) {
				return
			}
			delay = nextDurableDelay(delay, d.retryMax)
			continue
		}
		err = d.deliver(destination, record)
		if err != nil {
			_ = d.store.fail(record.ID, err.Error())
			d.logError("publish", destination, record.ID, err)
			if !d.wait(destination, delay) {
				return
			}
			delay = nextDurableDelay(delay, d.retryMax)
			continue
		}
		if err := d.store.ack(record.ID); err != nil {
			d.logError("ack", destination, record.ID, err)
			if !d.wait(destination, delay) {
				return
			}
			delay = nextDurableDelay(delay, d.retryMax)
			continue
		}
		delay = d.retryInitial
	}
}

func (d *durablePublisher) deliver(destination string, record durableRecord) error {
	target := d.targets[destination]
	if target == nil {
		return fmt.Errorf("MQTT destination %q is not configured", destination)
	}
	switch record.Kind {
	case durableKindTelemetry:
		return target.PublishTelemetry(record.Envelope.Device, record.Envelope.Payload)
	case durableKindTelemetryEvent:
		if record.Envelope.Telemetry == nil {
			return fmt.Errorf("telemetry envelope is empty")
		}
		transport, ok := target.(interface {
			PublishTelemetryEventAt(outevent.TelemetryEvent, bool, int64) error
		})
		if !ok {
			return fmt.Errorf("destination %q does not support persisted telemetry send_at", destination)
		}
		return transport.PublishTelemetryEventAt(*record.Envelope.Telemetry, record.Replayed || record.Attempts > 0, time.Now().UnixMilli())
	case durableKindCommandValues:
		return target.PublishCommandValues(record.Envelope.Device, record.Envelope.CommandValues)
	case durableKindPropertyResult:
		return target.PublishPropertyResult(record.Envelope.Device, record.Envelope.Payload)
	case durableKindPropertyReport:
		return target.PublishPropertyReport(record.Envelope.Device, record.Envelope.Payload)
	case durableKindCommandResult:
		return target.PublishCommandResult(record.Envelope.Device, record.Envelope.Payload)
	case durableKindEvent:
		if record.Envelope.Event == nil {
			return fmt.Errorf("event envelope is empty")
		}
		targetEvent, ok := target.(EventPublisher)
		if !ok {
			return fmt.Errorf("destination %q does not support EVENT publication", destination)
		}
		return targetEvent.PublishEvent(*record.Envelope.Event, record.Replayed || record.Attempts > 0)
	default:
		return fmt.Errorf("unsupported durable MQTT kind %q", record.Kind)
	}
}

func (d *durablePublisher) wait(destination string, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-d.stopCh:
			return false
		case <-d.wake[destination]:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-d.stopCh:
		return false
	case <-d.wake[destination]:
		return true
	case <-timer.C:
		return true
	}
}

func (d *durablePublisher) signalAll() {
	for _, destination := range d.destinations {
		select {
		case d.wake[destination] <- struct{}{}:
		default:
		}
	}
}

func (d *durablePublisher) logError(operation, destination string, id int64, err error) {
	if d.logger != nil {
		d.logger.Warnf("Durable MQTT %s failed: destination=%s id=%d err=%v", operation, destination, id, err)
	}
}

func nextDurableDelay(current, maximum time.Duration) time.Duration {
	if current <= 0 {
		return durableDefaultRetryInitial
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func (d *durablePublisher) PublishTelemetry(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return d.enqueue(durableKindTelemetry, "", durableEnvelope{Device: device, Payload: payload}, false)
}

func (d *durablePublisher) PublishCommandValues(device contracts.DeviceConfig, values []*contracts.CommandValue) error {
	return d.enqueue(durableKindCommandValues, "", durableEnvelope{Device: device, CommandValues: values}, false)
}

func (d *durablePublisher) PublishTelemetryEvent(event outevent.TelemetryEvent, replayed bool) error {
	return d.PublishTelemetryEventAt(event, replayed, time.Now().UnixMilli())
}

func (d *durablePublisher) PublishTelemetryEventAt(event outevent.TelemetryEvent, replayed bool, _ int64) error {
	return d.enqueue(durableKindTelemetryEvent, event.TraceID, durableEnvelope{Telemetry: &event}, replayed)
}

func (d *durablePublisher) PublishPropertyResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return d.enqueue(durableKindPropertyResult, traceIDFromPayload(payload), durableEnvelope{Device: device, Payload: payload}, false)
}

func (d *durablePublisher) PublishPropertyReport(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return d.enqueue(durableKindPropertyReport, traceIDFromPayload(payload), durableEnvelope{Device: device, Payload: payload}, false)
}

func (d *durablePublisher) PublishCommandResult(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return d.enqueue(durableKindCommandResult, traceIDFromPayload(payload), durableEnvelope{Device: device, Payload: payload}, false)
}

func (d *durablePublisher) PublishEvent(event events.Event, replayed bool) error {
	return d.enqueue(durableKindEvent, event.TraceID, durableEnvelope{Event: &event}, replayed)
}

func (d *durablePublisher) PublishStatus(device contracts.DeviceConfig, payload map[string]interface{}) error {
	return d.base.PublishStatus(device, payload)
}

func (d *durablePublisher) PublishJSON(topic string, qos byte, retain bool, payload interface{}) error {
	return d.base.PublishJSON(topic, qos, retain, payload)
}

func (d *durablePublisher) Subscribe(topic string, qos byte, handler MessageHandler) error {
	return d.base.Subscribe(topic, qos, handler)
}

func (d *durablePublisher) HealthCheck() error {
	if err := d.store.healthCheck(); err != nil {
		return err
	}
	return d.base.HealthCheck()
}

// DurableQueueStats returns network-side pending and dead-letter counts.
func (d *durablePublisher) DurableQueueStats() (DurableQueueStats, error) {
	if d == nil || d.store == nil {
		return DurableQueueStats{}, nil
	}
	return d.store.stats()
}

func (d *durablePublisher) RegisterOnConnect(hook func()) {
	if hook == nil {
		return
	}
	if registrar, ok := d.base.(interface{ RegisterOnConnect(func()) }); ok {
		registrar.RegisterOnConnect(hook)
	}
}

func (d *durableMultiPublisher) GroupPublishers() []Publisher {
	if multi, ok := d.base.(MultiGroupPublisher); ok {
		return multi.GroupPublishers()
	}
	return nil
}

func (d *durableMultiPublisher) GroupName(i int) string {
	if multi, ok := d.base.(MultiGroupPublisher); ok {
		return multi.GroupName(i)
	}
	return ""
}

func (d *durableMultiPublisher) GroupStatusTopic(i int) TopicConfig {
	if multi, ok := d.base.(MultiGroupPublisher); ok {
		return multi.GroupStatusTopic(i)
	}
	return TopicConfig{}
}

func (d *durablePublisher) setObserver(observer Observer) {
	if target, ok := d.base.(interface{ setObserver(Observer) }); ok {
		target.setObserver(observer)
	}
}

func (d *durablePublisher) observeInbound(message Observation) {
	ObserveInbound(d.base, message)
}

func (d *durablePublisher) publishDirect(topic string, qos byte, retain bool, payload []byte) error {
	return PublishDirect(d.base, topic, qos, retain, payload)
}

func (d *durablePublisher) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		close(d.stopCh)
		d.mu.Unlock()
		d.wg.Wait()
		if err := d.store.close(); err != nil {
			d.closeErr = err
		}
		if err := d.base.Close(); err != nil && d.closeErr == nil {
			d.closeErr = err
		}
	})
	return d.closeErr
}

func requireDurableRow(result sql.Result, operation string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("%s affected %d rows, want 1", operation, affected)
	}
	return nil
}

func boundedMQTTError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > 2048 {
		return message[:2048]
	}
	return message
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var (
	_ Publisher           = (*durablePublisher)(nil)
	_ EventPublisher      = (*durablePublisher)(nil)
	_ MultiGroupPublisher = (*durableMultiPublisher)(nil)
	_ interface {
		PublishTelemetryEventAt(outevent.TelemetryEvent, bool, int64) error
	} = (*durablePublisher)(nil)
)
