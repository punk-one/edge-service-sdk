package reliable

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	events "github.com/punk-one/edge-service-sdk/event"
	"github.com/punk-one/edge-service-sdk/internal/sqliteutil"
	logger "github.com/punk-one/edge-service-sdk/logging"

	_ "modernc.org/sqlite"
)

// EventTransport is implemented by the SDK MQTT publisher. EVENT keeps its
// own runtime.db outbox and lifecycle independently of telemetry delivery.
type EventTransport interface {
	PublishEvent(event events.Event, replayed bool) error
}

type EventQueueStats struct {
	BufferDepth        int64
	OldestPendingAgeMs int64
	ReplayRatePerSec   int
	LastReplayAt       int64
}

type EventDispatcher struct {
	cfg       EventOutboxConfig
	logger    logger.LoggingClient
	transport EventTransport
	store     *eventSQLiteStore
	enabled   bool

	stopCh      chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup
	publishMu   sync.Mutex
	metricsMu   sync.RWMutex
	lifecycleMu sync.Mutex
	closed      bool

	replayTokens     float64
	lastReplayRefill int64
	lastReplayRate   int
	lastReplayAt     int64
}

type storedEvent struct {
	ID        int64
	CreatedAt int64
	Event     events.Event
}

type storedEventPayload struct {
	Event       events.Event `json:"event"`
	ProductCode string       `json:"product_code,omitempty"`
	TraceID     string       `json:"trace_id,omitempty"`
	CreatedAt   int64        `json:"created_at"`
}

type eventSQLiteStore struct {
	db *sql.DB
}

// NewEventDispatcher creates an event-specific durable dispatcher. It uses a
// separate SQLite table and lifecycle from the telemetry outbox.
func NewEventDispatcher(cfg EventOutboxConfig, transport EventTransport, logClient logger.LoggingClient) (*EventDispatcher, error) {
	if transport == nil {
		return nil, fmt.Errorf("event transport is nil")
	}
	dispatcher := &EventDispatcher{
		cfg:       normalizeEventOutboxConfig(cfg),
		logger:    logClient,
		transport: transport,
		enabled:   cfg.Enabled,
		stopCh:    make(chan struct{}),
	}
	if dispatcher.cfg.MaxDatabaseBytes < sqliteutil.MinimumMaxBytes {
		return nil, fmt.Errorf("event outbox max database bytes must be >= %d", sqliteutil.MinimumMaxBytes)
	}
	if !dispatcher.enabled {
		return dispatcher, nil
	}
	store, err := newEventSQLiteStore(dispatcher.cfg.SQLitePath)
	if err != nil {
		return nil, err
	}
	if err := sqliteutil.ConfigureMaxBytes(store.db, dispatcher.cfg.MaxDatabaseBytes); err != nil {
		_ = store.close()
		return nil, fmt.Errorf("configure event sqlite capacity: %w", err)
	}
	dispatcher.store = store
	dispatcher.replayTokens = float64(dispatcher.cfg.replayRatePerSec())
	dispatcher.lastReplayRefill = time.Now().UnixMilli()
	dispatcher.wg.Add(1)
	go func() {
		defer dispatcher.wg.Done()
		dispatcher.replayLoop()
	}()
	return dispatcher, nil
}

func (d *EventDispatcher) Publish(event events.Event) error {
	if d == nil || d.transport == nil {
		return fmt.Errorf("event dispatcher is not initialized")
	}
	d.lifecycleMu.Lock()
	closed := d.closed
	d.lifecycleMu.Unlock()
	if closed {
		return fmt.Errorf("event dispatcher is closed")
	}
	if _, _, ok := events.EventLifecycle(event.Data.Type); !ok {
		return fmt.Errorf("unsupported event type %q", event.Data.Type)
	}
	event.Data = event.Data.NormalizeLifecycle()
	if event.CreatedAt == 0 {
		event.CreatedAt = time.Now().UnixMilli()
	}
	if !d.enabled || d.store == nil {
		return d.transport.PublishEvent(event, false)
	}

	// Write the event to SQLite before attempting MQTT delivery. This removes
	// the process-memory-only window that could lose a failed event on restart.
	d.publishMu.Lock()
	defer d.publishMu.Unlock()
	rowID, err := d.store.append(event)
	if err != nil {
		return fmt.Errorf("persist event before publish: %w", err)
	}
	if err := d.transport.PublishEvent(event, false); err != nil {
		if d.logger != nil {
			d.logger.Warnf("Realtime event publish failed, kept in outbox: device=%s category=%s event=%s instance=%s", event.DeviceCode, event.Data.Category, event.Data.EventCode, event.Data.EventInstanceID)
		}
		return nil
	}
	if err := d.store.ack([]int64{rowID}); err != nil {
		return fmt.Errorf("ack published event: %w", err)
	}
	if d.logger != nil {
		d.logger.Debugf("Event published and acked from outbox: device=%s category=%s event=%s instance=%s", event.DeviceCode, event.Data.Category, event.Data.EventCode, event.Data.EventInstanceID)
	}
	return nil
}

func (d *EventDispatcher) Close() error {
	if d == nil {
		return nil
	}
	d.lifecycleMu.Lock()
	if !d.closed {
		d.closed = true
		d.closeOnce.Do(func() { close(d.stopCh) })
	}
	d.lifecycleMu.Unlock()
	d.wg.Wait()
	d.publishMu.Lock()
	d.publishMu.Unlock()
	if d.store != nil {
		return d.store.close()
	}
	return nil
}

func (d *EventDispatcher) Stats() (EventQueueStats, error) {
	if d == nil {
		return EventQueueStats{}, nil
	}
	d.metricsMu.RLock()
	result := EventQueueStats{ReplayRatePerSec: d.lastReplayRate, LastReplayAt: d.lastReplayAt}
	d.metricsMu.RUnlock()
	if d.store == nil {
		return result, nil
	}
	stats, err := d.store.stats()
	if err != nil {
		return EventQueueStats{}, err
	}
	result.BufferDepth = stats.pendingCount
	if stats.oldestPendingCreatedAt > 0 {
		result.OldestPendingAgeMs = time.Now().UnixMilli() - stats.oldestPendingCreatedAt
	}
	return result, nil
}

// HealthCheck verifies that the durable EVENT queue is readable and has
// capacity remaining. Disabled EVENT delivery is healthy by definition.
func (d *EventDispatcher) HealthCheck() error {
	if d == nil {
		return nil
	}
	if !d.enabled {
		return nil
	}
	if d.store == nil || d.store.db == nil {
		return fmt.Errorf("event sqlite store is not initialized")
	}
	var result string
	if err := d.store.db.QueryRow(`PRAGMA quick_check(1);`).Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("event sqlite quick_check: %s", result)
	}
	return sqliteutil.CheckCapacity(d.store.db)
}

func (d *EventDispatcher) replayLoop() {
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

func (d *EventDispatcher) replayOnce() {
	if d.store == nil {
		return
	}
	d.publishMu.Lock()
	defer d.publishMu.Unlock()
	if removed, err := d.store.purgeExpired(d.retentionCutoff()); err != nil {
		if d.logger != nil {
			d.logger.Warnf("Failed to purge expired event records: %v", err)
		}
	} else if removed > 0 && d.logger != nil {
		d.logger.Infof("Purged expired event records: removed=%d", removed)
	}
	limit := d.availableReplayBudget(time.Now())
	if limit <= 0 {
		return
	}
	records, err := d.store.fetchPending(limit)
	if err != nil {
		if d.logger != nil {
			d.logger.Warnf("Failed to fetch pending event records: %v", err)
		}
		return
	}
	acked := make([]int64, 0, len(records))
	for _, record := range records {
		if err := d.transport.PublishEvent(record.Event, true); err != nil {
			if d.logger != nil {
				d.logger.Warnf("Event replay paused on publish failure: device=%s event=%s err=%v", record.Event.DeviceCode, record.Event.Data.EventCode, err)
			}
			break
		}
		acked = append(acked, record.ID)
		d.consumeReplayToken()
	}
	if err := d.store.ack(acked); err != nil {
		if d.logger != nil {
			d.logger.Warnf("Failed to ack replayed event records: %v", err)
		}
		return
	}
	if len(acked) > 0 {
		seconds := d.cfg.replayInterval().Seconds()
		if seconds <= 0 {
			seconds = 1
		}
		d.metricsMu.Lock()
		d.lastReplayRate = int(math.Ceil(float64(len(acked)) / seconds))
		d.lastReplayAt = time.Now().UnixMilli()
		d.metricsMu.Unlock()
	}
}

func (d *EventDispatcher) retentionCutoff() int64 {
	if d.cfg.RetentionDays <= 0 {
		return 0
	}
	return time.Now().Add(-time.Duration(d.cfg.RetentionDays) * 24 * time.Hour).UnixMilli()
}

func (d *EventDispatcher) availableReplayBudget(now time.Time) int {
	rate := d.cfg.replayRatePerSec()
	last := time.UnixMilli(d.lastReplayRefill)
	elapsed := now.Sub(last).Seconds()
	if elapsed > 0 {
		d.replayTokens = math.Min(float64(rate), d.replayTokens+elapsed*float64(rate))
		d.lastReplayRefill = now.UnixMilli()
	}
	available := int(math.Floor(d.replayTokens))
	if available > d.cfg.batchSize() {
		available = d.cfg.batchSize()
	}
	return available
}

func (d *EventDispatcher) consumeReplayToken() {
	if d.replayTokens >= 1 {
		d.replayTokens--
	}
}

func normalizeEventOutboxConfig(cfg EventOutboxConfig) EventOutboxConfig {
	if strings.TrimSpace(cfg.SQLitePath) == "" {
		cfg.SQLitePath = "./data/runtime.db"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.ReplayIntervalMs <= 0 {
		cfg.ReplayIntervalMs = 3_000
	}
	if cfg.ReplayRatePerSec <= 0 {
		cfg.ReplayRatePerSec = 20
	}
	if cfg.MaxDatabaseBytes == 0 {
		cfg.MaxDatabaseBytes = sqliteutil.DefaultMaxBytes
	}
	return cfg
}

func (c EventOutboxConfig) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return 100
}

func (c EventOutboxConfig) replayInterval() time.Duration {
	if c.ReplayIntervalMs > 0 {
		return time.Duration(c.ReplayIntervalMs) * time.Millisecond
	}
	return 3 * time.Second
}

func (c EventOutboxConfig) replayRatePerSec() int {
	if c.ReplayRatePerSec > 0 {
		return c.ReplayRatePerSec
	}
	return 20
}

func newEventSQLiteStore(path string) (*eventSQLiteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("event sqlite path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// EVENT writes are serialized by the dispatcher; one connection prevents
	// connection-local pragmas from being lost on a newly opened connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &eventSQLiteStore{db: db}
	for _, statement := range []string{"PRAGMA journal_mode = WAL;", "PRAGMA synchronous = FULL;", "PRAGMA busy_timeout = 5000;"} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS event_outbox (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	device_code TEXT NOT NULL,
	product_code TEXT NOT NULL,
	category TEXT NOT NULL,
	event_code TEXT NOT NULL,
	event_instance_id TEXT NOT NULL,
	event_time INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	payload_json BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_outbox_created_at ON event_outbox(created_at);
CREATE INDEX IF NOT EXISTS idx_event_outbox_order ON event_outbox(device_code, category, event_time, id);
CREATE TABLE IF NOT EXISTS event_outbox_dead_letter (
	id INTEGER PRIMARY KEY,
	payload_json BLOB NOT NULL,
	reason TEXT NOT NULL,
	quarantined_at INTEGER NOT NULL
);
`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *eventSQLiteStore) append(item events.Event) (int64, error) {
	createdAt := item.CreatedAt
	if createdAt == 0 {
		createdAt = time.Now().UnixMilli()
	}
	payload, err := json.Marshal(storedEventPayload{Event: item, ProductCode: item.ProductCode, TraceID: item.TraceID, CreatedAt: createdAt})
	if err != nil {
		return 0, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	result, err := tx.Exec(`INSERT INTO event_outbox(device_code, product_code, category, event_code, event_instance_id, event_time, created_at, payload_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, item.DeviceCode, item.ProductCode, item.Data.Category, item.Data.EventCode, item.Data.EventInstanceID, item.Time, createdAt, payload)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *eventSQLiteStore) appendBatch(items []events.Event) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`INSERT INTO event_outbox(device_code, product_code, category, event_code, event_instance_id, event_time, created_at, payload_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, item := range items {
		createdAt := item.CreatedAt
		if createdAt == 0 {
			createdAt = time.Now().UnixMilli()
		}
		payload, marshalErr := json.Marshal(storedEventPayload{Event: item, ProductCode: item.ProductCode, TraceID: item.TraceID, CreatedAt: createdAt})
		if marshalErr != nil {
			err = marshalErr
			return err
		}
		if _, err = stmt.Exec(item.DeviceCode, item.ProductCode, item.Data.Category, item.Data.EventCode, item.Data.EventInstanceID, item.Time, createdAt, payload); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func (s *eventSQLiteStore) fetchPending(limit int) ([]storedEvent, error) {
	if err := s.quarantineMalformed(); err != nil {
		return nil, fmt.Errorf("quarantine malformed event rows: %w", err)
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT id, created_at, payload_json FROM event_outbox ORDER BY event_time ASC, id ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]storedEvent, 0, limit)
	for rows.Next() {
		var item storedEvent
		var payload []byte
		if err := rows.Scan(&item.ID, &item.CreatedAt, &payload); err != nil {
			return nil, err
		}
		var stored storedEventPayload
		if err := json.Unmarshal(payload, &stored); err != nil {
			return nil, err
		}
		stored.Event.ProductCode = stored.ProductCode
		stored.Event.TraceID = stored.TraceID
		stored.Event.CreatedAt = stored.CreatedAt
		item.Event = stored.Event
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *eventSQLiteStore) quarantineMalformed() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
INSERT OR IGNORE INTO event_outbox_dead_letter(id, payload_json, reason, quarantined_at)
SELECT id, payload_json, 'invalid payload_json', ?
FROM event_outbox
WHERE json_valid(payload_json) = 0`, time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM event_outbox WHERE json_valid(payload_json) = 0`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *eventSQLiteStore) ack(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.Prepare(`DELETE FROM event_outbox WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err = stmt.Exec(id); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func (s *eventSQLiteStore) purgeExpired(cutoff int64) (int64, error) {
	if cutoff <= 0 {
		return 0, nil
	}
	result, err := s.db.Exec(`DELETE FROM event_outbox WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

type eventStoreStats struct {
	pendingCount           int64
	oldestPendingCreatedAt int64
}

func (s *eventSQLiteStore) stats() (eventStoreStats, error) {
	row := s.db.QueryRow(`SELECT COUNT(1), COALESCE(MIN(created_at), 0) FROM event_outbox`)
	var result eventStoreStats
	if err := row.Scan(&result.pendingCount, &result.oldestPendingCreatedAt); err != nil {
		return eventStoreStats{}, err
	}
	return result, nil
}

func (s *eventSQLiteStore) close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
