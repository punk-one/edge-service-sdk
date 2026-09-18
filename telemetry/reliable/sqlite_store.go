package reliable

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/punk-one/edge-service-sdk/internal/sqliteutil"
	outevent "github.com/punk-one/edge-service-sdk/telemetry"

	_ "modernc.org/sqlite"
)

type sqliteStore struct {
	db *sql.DB
}

func newSQLiteStore(path string) (*sqliteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("telemetry outbox sqlite path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// All telemetry INSERT/attempt/ACK writes are deliberately serialized. The
	// outbox has its own database file, so this does not serialize runtime.db.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &sqliteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *sqliteStore) init() error {
	for _, stmt := range []string{
		"PRAGMA journal_mode = WAL;",
		// FULL is intentional: a nil Append result is the SDK durability
		// boundary and must survive an operating-system crash or power loss.
		"PRAGMA synchronous = FULL;",
		"PRAGMA busy_timeout = 5000;",
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	if _, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS telemetry_outbox (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	trace_id TEXT NOT NULL UNIQUE,
	device_code TEXT NOT NULL,
	product_code TEXT NOT NULL DEFAULT '',
	source_name TEXT NOT NULL DEFAULT 'telemetry',
	time INTEGER NOT NULL,
	send_at INTEGER,
	is_replayed INTEGER NOT NULL DEFAULT 0 CHECK (is_replayed IN (0, 1)),
	data_json TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	delivery_attempts INTEGER NOT NULL DEFAULT 0,
	last_attempt_at INTEGER,
	last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_telemetry_outbox_order ON telemetry_outbox(time, id);
CREATE INDEX IF NOT EXISTS idx_telemetry_outbox_retention ON telemetry_outbox(created_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_outbox_device ON telemetry_outbox(device_code, time, id);
CREATE TABLE IF NOT EXISTS telemetry_outbox_dead_letter (
	id INTEGER PRIMARY KEY,
	trace_id TEXT,
	device_code TEXT,
	data_json TEXT NOT NULL,
	reason TEXT NOT NULL,
	quarantined_at INTEGER NOT NULL
);
`); err != nil {
		return err
	}
	_, err := s.db.Exec(`PRAGMA user_version = 1;`)
	return err
}

func (s *sqliteStore) Append(event outevent.TelemetryEvent, replayed bool, createdAt int64) (int64, error) {
	dataJSON, err := json.Marshal(event.Values)
	if err != nil {
		return 0, fmt.Errorf("marshal telemetry data: %w", err)
	}
	if createdAt <= 0 {
		createdAt = nowMillis()
	}
	sourceName := strings.TrimSpace(event.SourceName)
	if sourceName == "" {
		sourceName = "telemetry"
	}
	result, err := s.db.Exec(`
INSERT INTO telemetry_outbox(
	trace_id, device_code, product_code, source_name, time, is_replayed, data_json, created_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(trace_id) DO NOTHING`,
		event.TraceID, event.DeviceName, event.ProductCode, sourceName,
		event.CollectedAt, boolInt(replayed), string(dataJSON), createdAt,
	)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 1 {
		return result.LastInsertId()
	}
	var id int64
	if err := s.db.QueryRow(`SELECT id FROM telemetry_outbox WHERE trace_id = ?`, event.TraceID).Scan(&id); err != nil {
		return 0, fmt.Errorf("resolve idempotent telemetry append: %w", err)
	}
	return id, nil
}

func (s *sqliteStore) MarkAllReplayed() (int64, error) {
	result, err := s.db.Exec(`UPDATE telemetry_outbox SET is_replayed = 1`)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *sqliteStore) MaxID() (int64, error) {
	var id int64
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM telemetry_outbox`).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *sqliteStore) FetchPending(limit int, cutoffID int64) ([]StoredTelemetry, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
SELECT id, trace_id, device_code, product_code, source_name, time, send_at,
	   is_replayed, data_json, created_at, delivery_attempts
FROM telemetry_outbox
ORDER BY time ASC, id ASC
LIMIT ?`
	args := []interface{}{limit}
	if cutoffID > 0 {
		query = `
SELECT id, trace_id, device_code, product_code, source_name, time, send_at,
	   is_replayed, data_json, created_at, delivery_attempts
FROM telemetry_outbox
WHERE id <= ?
ORDER BY time ASC, id ASC
LIMIT ?`
		args = []interface{}{cutoffID, limit}
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	result := make([]StoredTelemetry, 0, limit)
	invalidIDs := make([]int64, 0)
	for rows.Next() {
		var (
			item       StoredTelemetry
			sendAt     sql.NullInt64
			replayed   int
			dataJSON   string
			traceID    string
			deviceCode string
			product    string
			source     string
		)
		if err := rows.Scan(
			&item.ID,
			&traceID,
			&deviceCode,
			&product,
			&source,
			&item.Time,
			&sendAt,
			&replayed,
			&dataJSON,
			&item.CreatedAt,
			&item.DeliveryAttempts,
		); err != nil {
			return nil, err
		}
		values := make(map[string]outevent.TelemetryValue)
		if err := json.Unmarshal([]byte(dataJSON), &values); err != nil {
			invalidIDs = append(invalidIDs, item.ID)
			continue
		}
		item.SendAt = sendAt.Int64
		item.HasSendAt = sendAt.Valid
		item.IsReplayed = replayed != 0
		item.Event = outevent.TelemetryEvent{
			TraceID:     traceID,
			DeviceName:  deviceCode,
			ProductCode: product,
			SourceName:  source,
			CollectedAt: item.Time,
			Values:      values,
		}
		result = append(result, item)
	}
	rowErr := rows.Err()
	if err := rows.Close(); rowErr == nil {
		rowErr = err
	}
	if rowErr != nil {
		return nil, rowErr
	}
	if len(invalidIDs) > 0 {
		if err := s.quarantineRows(invalidIDs, "invalid data_json"); err != nil {
			return nil, fmt.Errorf("quarantine malformed telemetry rows: %w", err)
		}
	}
	return result, nil
}

func (s *sqliteStore) quarantineRows(ids []int64, reason string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, id := range ids {
		if _, err := tx.Exec(`
	INSERT OR IGNORE INTO telemetry_outbox_dead_letter(
		id, trace_id, device_code, data_json, reason, quarantined_at
	)
	SELECT id, trace_id, device_code, data_json, ?, ?
	FROM telemetry_outbox WHERE id = ?`, reason, nowMillis(), id); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM telemetry_outbox WHERE id = ?`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) MarkAttempt(id, sendAt int64, replayed bool) error {
	result, err := s.db.Exec(`
UPDATE telemetry_outbox
SET send_at = ?,
	last_attempt_at = ?,
	delivery_attempts = delivery_attempts + 1,
	is_replayed = ?,
	last_error = NULL
WHERE id = ?`, sendAt, sendAt, boolInt(replayed), id)
	if err != nil {
		return err
	}
	return requireOneRow(result, "mark telemetry send attempt")
}

func (s *sqliteStore) MarkAttempts(records []StoredTelemetry, sendAt int64) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	statement, err := tx.Prepare(`
	UPDATE telemetry_outbox
	SET send_at = ?, last_attempt_at = ?, delivery_attempts = delivery_attempts + 1,
		is_replayed = ?, last_error = NULL
	WHERE id = ?`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for _, record := range records {
		result, execErr := statement.Exec(sendAt, sendAt, boolInt(record.IsReplayed), record.ID)
		if execErr != nil {
			return execErr
		}
		if err := requireOneRow(result, "mark telemetry send attempt"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) MarkFailed(id int64, message string) error {
	result, err := s.db.Exec(`
UPDATE telemetry_outbox
SET is_replayed = 1, last_error = ?
WHERE id = ?`, strings.TrimSpace(message), id)
	if err != nil {
		return err
	}
	return requireOneRow(result, "mark telemetry send failure")
}

func (s *sqliteStore) Ack(id int64) error {
	result, err := s.db.Exec(`DELETE FROM telemetry_outbox WHERE id = ?`, id)
	if err != nil {
		return err
	}
	return requireOneRow(result, "ack telemetry delivery")
}

func (s *sqliteStore) AckBatch(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	const chunkSize = 500
	for start := 0; start < len(ids); start += chunkSize {
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		args := make([]interface{}, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		query := `DELETE FROM telemetry_outbox WHERE id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",") + `)`
		result, execErr := tx.Exec(query, args...)
		if execErr != nil {
			return execErr
		}
		affected, affectedErr := result.RowsAffected()
		if affectedErr != nil {
			return affectedErr
		}
		if affected != int64(len(chunk)) {
			return fmt.Errorf("ack telemetry delivery affected %d rows, want %d", affected, len(chunk))
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) PurgeExpired(cutoffMillis int64) (int64, error) {
	if cutoffMillis <= 0 {
		return 0, nil
	}
	result, err := s.db.Exec(`DELETE FROM telemetry_outbox WHERE created_at < ?`, cutoffMillis)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *sqliteStore) Stats() (StoreStats, error) {
	row := s.db.QueryRow(`SELECT COUNT(1), COALESCE(MIN(created_at), 0) FROM telemetry_outbox`)
	var stats StoreStats
	if err := row.Scan(&stats.PendingCount, &stats.OldestPendingCreatedAt); err != nil {
		return StoreStats{}, err
	}
	return stats, nil
}

func (s *sqliteStore) ConfigureMaxBytes(maxBytes int64) error {
	return sqliteutil.ConfigureMaxBytes(s.db, maxBytes)
}

func (s *sqliteStore) DeadLetterCount() (int64, error) {
	var count int64
	if err := s.db.QueryRow(`SELECT COUNT(1) FROM telemetry_outbox_dead_letter`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *sqliteStore) HealthCheck() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("telemetry sqlite store is not initialized")
	}
	var result string
	if err := s.db.QueryRow(`PRAGMA quick_check(1);`).Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("telemetry sqlite quick_check: %s", result)
	}
	return sqliteutil.CheckCapacity(s.db)
}

func (s *sqliteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func requireOneRow(result sql.Result, operation string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("%s affected %d rows, want 1", operation, affected)
	}
	return nil
}
