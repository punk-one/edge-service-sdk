package sqliteutil

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestConfigureMaxBytesEnforcesLimitAndCapacityWarning(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "capacity.db"))
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE payloads(data BLOB NOT NULL);`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if err := ConfigureMaxBytes(db, 128<<10); err != nil {
		t.Fatalf("ConfigureMaxBytes() error = %v", err)
	}

	var insertErr error
	for i := 0; i < 100; i++ {
		if _, insertErr = db.Exec(`INSERT INTO payloads(data) VALUES(zeroblob(8192));`); insertErr != nil {
			break
		}
	}
	if insertErr == nil {
		t.Fatal("database accepted writes beyond configured capacity")
	}
	if err := CheckCapacity(db); err == nil {
		t.Fatal("CheckCapacity() error = nil, want capacity warning")
	}
}
