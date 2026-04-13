package control

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type sqliteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (Store, error) {
	if path == "" {
		return nil, fmt.Errorf("control sqlite path is empty")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &sqliteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}
