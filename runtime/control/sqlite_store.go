package control

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/punk-one/edge-service-sdk/internal/sqliteutil"

	_ "modernc.org/sqlite"
)

type sqliteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (Store, error) {
	return NewSQLiteStoreWithRetentionAndCapacity(path, 0, sqliteutil.DefaultMaxBytes)
}

// NewSQLiteStoreWithRetention creates a control store and removes completed
// jobs older than retentionDays. Zero disables automatic retention.
func NewSQLiteStoreWithRetention(path string, retentionDays int) (Store, error) {
	return NewSQLiteStoreWithRetentionAndCapacity(path, retentionDays, sqliteutil.DefaultMaxBytes)
}

// NewSQLiteStoreWithRetentionAndCapacity creates a bounded control store.
func NewSQLiteStoreWithRetentionAndCapacity(path string, retentionDays int, maxDatabaseBytes int64) (Store, error) {
	if path == "" {
		return nil, fmt.Errorf("control sqlite path is empty")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// All state transitions for a trace must observe one total order. A single
	// connection also makes the connection-local busy_timeout/synchronous
	// pragmas apply consistently and avoids SQLITE_BUSY under concurrent claims.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &sqliteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.ConfigureMaxBytes(maxDatabaseBytes); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure control sqlite capacity: %w", err)
	}
	if retentionDays > 0 {
		cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour).UnixMilli()
		if _, err := store.PurgeFinishedBefore(cutoff); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("purge expired control jobs: %w", err)
		}
	}
	return store, nil
}
