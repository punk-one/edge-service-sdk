package reliable

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	outevent "github.com/punk-one/edge-service-sdk/telemetry"

	_ "modernc.org/sqlite"
)

type sqliteStore struct {
	db             *sql.DB
	keepLatestOnly bool
}

func newSQLiteStore(path string, keepLatestOnly bool) (*sqliteStore, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	store := &sqliteStore{db: db, keepLatestOnly: keepLatestOnly}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *sqliteStore) init() error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
	}
	for _, stmt := range pragmas {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}

	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS reliable_queue (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	trace_id TEXT NOT NULL,
	device_name TEXT NOT NULL,
	product_code TEXT NOT NULL,
	source_name TEXT NOT NULL DEFAULT 'telemetry',
	created_at INTEGER NOT NULL,
	payload_json BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reliable_queue_created_at ON reliable_queue(created_at);
CREATE INDEX IF NOT EXISTS idx_reliable_queue_device_source ON reliable_queue(device_name, source_name);
`)
	if err != nil {
		return err
	}

	if _, err := s.db.Exec(`ALTER TABLE reliable_queue ADD COLUMN source_name TEXT NOT NULL DEFAULT 'telemetry'`); err != nil && !isDuplicateColumnErr(err) {
		return err
	}
	return nil
}

func (s *sqliteStore) AppendBatch(events []outevent.TelemetryEvent) error {
	if len(events) == 0 {
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

	insertStmt, err := tx.Prepare(`INSERT INTO reliable_queue(trace_id, device_name, product_code, source_name, created_at, payload_json) VALUES(?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	var pruneStmt *sql.Stmt
	if s.keepLatestOnly {
		pruneStmt, err = tx.Prepare(`DELETE FROM reliable_queue WHERE device_name = ? AND source_name = ?`)
		if err != nil {
			return err
		}
		defer pruneStmt.Close()
	}

	for _, event := range events {
		payload, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			err = fmt.Errorf("marshal telemetry event: %w", marshalErr)
			return err
		}
		createdAt := event.CollectedAt
		if createdAt == 0 {
			createdAt = nowMillis()
		}
		sourceName := event.SourceName
		if sourceName == "" {
			sourceName = "telemetry"
		}
		if pruneStmt != nil {
			if _, execErr := pruneStmt.Exec(event.DeviceName, sourceName); execErr != nil {
				err = execErr
				return err
			}
		}
		if _, execErr := insertStmt.Exec(event.TraceID, event.DeviceName, event.ProductCode, sourceName, createdAt, payload); execErr != nil {
			err = execErr
			return err
		}
	}

	err = tx.Commit()
	return err
}

func (s *sqliteStore) FetchPending(limit int) ([]StoredEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.Query(`SELECT id, created_at, payload_json FROM reliable_queue ORDER BY id ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]StoredEvent, 0, limit)
	for rows.Next() {
		var (
			record  StoredEvent
			payload []byte
		)
		if err := rows.Scan(&record.ID, &record.CreatedAt, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &record.Event); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *sqliteStore) Ack(ids []int64) error {
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

	stmt, err := tx.Prepare(`DELETE FROM reliable_queue WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, id := range ids {
		if _, execErr := stmt.Exec(id); execErr != nil {
			err = execErr
			return err
		}
	}

	err = tx.Commit()
	return err
}

func (s *sqliteStore) PurgeExpired(cutoffMillis int64) (int64, error) {
	if cutoffMillis <= 0 {
		return 0, nil
	}
	result, err := s.db.Exec(`DELETE FROM reliable_queue WHERE created_at < ?`, cutoffMillis)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *sqliteStore) Stats() (StoreStats, error) {
	row := s.db.QueryRow(`SELECT COUNT(1), COALESCE(MIN(created_at), 0) FROM reliable_queue`)

	var stats StoreStats
	if err := row.Scan(&stats.PendingCount, &stats.OldestPendingCreatedAt); err != nil {
		return StoreStats{}, err
	}
	return stats, nil
}

func (s *sqliteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func isDuplicateColumnErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column name")
}
