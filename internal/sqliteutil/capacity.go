package sqliteutil

import (
	"database/sql"
	"fmt"
)

const (
	CapacityWarningPercent int64 = 95
	DefaultMaxBytes        int64 = 2 << 30
	MinimumMaxBytes        int64 = 64 << 20
)

// ConfigureMaxBytes applies a persistent SQLite page limit and bounds retained
// WAL journal files after checkpoints. It never shrinks or deletes user data.
func ConfigureMaxBytes(db *sql.DB, maxBytes int64) error {
	if db == nil {
		return fmt.Errorf("sqlite database is nil")
	}
	if maxBytes < 0 {
		return fmt.Errorf("sqlite maximum bytes must not be negative")
	}
	if maxBytes == 0 {
		return nil
	}
	pageSize, pageCount, err := pageUsage(db)
	if err != nil {
		return err
	}
	maxPages := maxBytes / pageSize
	if maxPages < 1 {
		maxPages = 1
	}
	if pageCount > maxPages {
		return fmt.Errorf("sqlite database already uses %d bytes, exceeding configured maximum %d bytes", pageCount*pageSize, maxBytes)
	}
	var appliedPages int64
	if err := db.QueryRow(fmt.Sprintf(`PRAGMA max_page_count = %d;`, maxPages)).Scan(&appliedPages); err != nil {
		return fmt.Errorf("set sqlite max_page_count: %w", err)
	}
	if appliedPages < maxPages {
		return fmt.Errorf("sqlite max_page_count is %d pages, want at least %d", appliedPages, maxPages)
	}
	journalLimit := maxBytes / 16
	if journalLimit > 64<<20 {
		journalLimit = 64 << 20
	}
	if journalLimit < 1<<20 {
		journalLimit = 1 << 20
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA journal_size_limit = %d;`, journalLimit)); err != nil {
		return fmt.Errorf("set sqlite journal_size_limit: %w", err)
	}
	return nil
}

// CheckCapacity fails once the database reaches the warning percentage of its
// configured page limit, allowing readiness monitoring to alert before full.
func CheckCapacity(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite database is nil")
	}
	pageSize, pageCount, err := pageUsage(db)
	if err != nil {
		return err
	}
	var maxPages int64
	if err := db.QueryRow(`PRAGMA max_page_count;`).Scan(&maxPages); err != nil {
		return fmt.Errorf("read sqlite max_page_count: %w", err)
	}
	if maxPages > 0 && pageCount*100 >= maxPages*CapacityWarningPercent {
		return fmt.Errorf("sqlite capacity is at least %d%%: used=%d bytes max=%d bytes", CapacityWarningPercent, pageCount*pageSize, maxPages*pageSize)
	}
	return nil
}

func pageUsage(db *sql.DB) (pageSize int64, pageCount int64, err error) {
	if err := db.QueryRow(`PRAGMA page_size;`).Scan(&pageSize); err != nil {
		return 0, 0, fmt.Errorf("read sqlite page_size: %w", err)
	}
	if pageSize <= 0 {
		return 0, 0, fmt.Errorf("invalid sqlite page size %d", pageSize)
	}
	if err := db.QueryRow(`PRAGMA page_count;`).Scan(&pageCount); err != nil {
		return 0, 0, fmt.Errorf("read sqlite page_count: %w", err)
	}
	return pageSize, pageCount, nil
}
