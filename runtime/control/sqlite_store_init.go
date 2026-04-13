package control

import (
	"database/sql"
	"fmt"
	"strings"

	ctl "github.com/punk-one/edge-service-sdk/control"
)

func (s *sqliteStore) init() error {
	stmts := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = NORMAL;`,
		`PRAGMA busy_timeout = 5000;`,
		`CREATE TABLE IF NOT EXISTS control_jobs (
			trace_id TEXT PRIMARY KEY,
			device_code TEXT NOT NULL,
			product_code TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL,
			identifier TEXT NOT NULL DEFAULT '',
			code INTEGER NOT NULL,
			message TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			finished_at INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS control_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			trace_id TEXT NOT NULL,
			code INTEGER NOT NULL,
			message TEXT NOT NULL,
			result_json TEXT NOT NULL,
			is_final INTEGER NOT NULL,
			reported_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS control_pending_commands (
			trace_id TEXT PRIMARY KEY,
			device_code TEXT NOT NULL,
			product_code TEXT NOT NULL,
			identifier TEXT NOT NULL,
			request_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS control_pending_properties (
			trace_id TEXT PRIMARY KEY,
			device_code TEXT NOT NULL,
			product_code TEXT NOT NULL,
			operation TEXT NOT NULL DEFAULT 'set',
			request_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	if err := s.ensureColumn("control_jobs", "product_code", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("control_jobs", "identifier", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("control_jobs", "finished_at", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := s.ensureColumn("control_pending_properties", "operation", "TEXT NOT NULL DEFAULT 'set'"); err != nil {
		return err
	}
	_ = ctl.CodeSuccess
	return nil
}

func (s *sqliteStore) ensureColumn(table string, column string, definition string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)
	if _, err := s.db.Exec(query); err != nil {
		if containsSQLiteDuplicateColumn(err) {
			return nil
		}
		return err
	}
	return nil
}

func containsSQLiteDuplicateColumn(err error) bool {
	if err == nil {
		return false
	}
	return stringsContains(err.Error(), "duplicate column name")
}

func (s *sqliteStore) LoadJob(traceID string) (JobState, bool, error) {
	row := s.db.QueryRow(`SELECT trace_id, device_code, product_code, kind, identifier, code, message, created_at, updated_at, finished_at FROM control_jobs WHERE trace_id = ?`, traceID)
	var job JobState
	if err := row.Scan(&job.TraceID, &job.DeviceCode, &job.ProductCode, &job.Kind, &job.Identifier, &job.Code, &job.Message, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt); err != nil {
		if err == sql.ErrNoRows {
			return JobState{}, false, nil
		}
		return JobState{}, false, err
	}
	return job, true, nil
}

func (s *sqliteStore) ListJobs(filter JobFilter) ([]JobState, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite store is not initialized")
	}
	whereSQL, args := buildJobFilterSQL(filter)
	query := `SELECT trace_id, device_code, product_code, kind, identifier, code, message, created_at, updated_at, finished_at FROM control_jobs WHERE ` + whereSQL + ` ORDER BY updated_at DESC, created_at DESC`
	if filter.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, filter.Limit)
	}
	if filter.Offset > 0 {
		if filter.Limit <= 0 {
			query += ` LIMIT -1`
		}
		query += ` OFFSET ?`
		args = append(args, filter.Offset)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]JobState, 0)
	for rows.Next() {
		var item JobState
		if err := rows.Scan(&item.TraceID, &item.DeviceCode, &item.ProductCode, &item.Kind, &item.Identifier, &item.Code, &item.Message, &item.CreatedAt, &item.UpdatedAt, &item.FinishedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *sqliteStore) JobDiagnostics(filter JobFilter) (JobDiagnostics, error) {
	if s == nil || s.db == nil {
		return JobDiagnostics{}, fmt.Errorf("sqlite store is not initialized")
	}
	whereSQL, args := buildJobFilterSQL(filter)
	row := s.db.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN code IN (?, ?) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code NOT IN (?, ?) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN code NOT IN (?, ?, ?, ?) THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN kind = 'property' OR kind LIKE 'property:%' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN kind = 'command' OR kind LIKE 'command:%' THEN 1 ELSE 0 END), 0),
		COALESCE(MAX(updated_at), 0)
		FROM control_jobs WHERE `+whereSQL,
		append([]interface{}{
			ctl.CodeProcessing, ctl.CodeAccepted,
			ctl.CodeProcessing,
			ctl.CodeAccepted,
			ctl.CodeProcessing, ctl.CodeAccepted,
			ctl.CodeSuccess,
			ctl.CodePartialSuccess,
			ctl.CodeSuccess, ctl.CodeProcessing, ctl.CodeAccepted, ctl.CodePartialSuccess,
		}, args...)...)
	var diagnostics JobDiagnostics
	if err := row.Scan(
		&diagnostics.Total,
		&diagnostics.Pending,
		&diagnostics.Processing,
		&diagnostics.Accepted,
		&diagnostics.Final,
		&diagnostics.Success,
		&diagnostics.PartialSuccess,
		&diagnostics.Failed,
		&diagnostics.Property,
		&diagnostics.Command,
		&diagnostics.LatestUpdatedAt,
	); err != nil {
		return JobDiagnostics{}, err
	}
	pendingCommandCount, err := s.countPendingCommands(filter)
	if err != nil {
		return JobDiagnostics{}, err
	}
	pendingPropertyCount, err := s.countPendingProperties(filter)
	if err != nil {
		return JobDiagnostics{}, err
	}
	diagnostics.PendingCommandQueue = pendingCommandCount
	diagnostics.PendingPropertyQueue = pendingPropertyCount
	return diagnostics, nil
}

func buildJobFilterSQL(filter JobFilter) (string, []interface{}) {
	clauses := []string{"1 = 1"}
	args := make([]interface{}, 0, 10)
	if deviceCode := stringsTrimSpace(filter.DeviceCode); deviceCode != "" {
		clauses = append(clauses, "device_code = ?")
		args = append(args, deviceCode)
	}
	if identifier := stringsTrimSpace(filter.Identifier); identifier != "" {
		clauses = append(clauses, "identifier = ?")
		args = append(args, identifier)
	}
	if kind := stringsTrimSpace(filter.Kind); kind != "" {
		if strings.Contains(kind, ":") {
			clauses = append(clauses, "kind = ?")
			args = append(args, kind)
		} else {
			clauses = append(clauses, "(kind = ? OR kind LIKE ?)")
			args = append(args, kind, kind+":%")
		}
	}
	if filter.FinalSet {
		if filter.Final {
			clauses = append(clauses, "code NOT IN (?, ?)")
		} else {
			clauses = append(clauses, "code IN (?, ?)")
		}
		args = append(args, ctl.CodeProcessing, ctl.CodeAccepted)
	}
	if filter.CreatedFrom > 0 {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, filter.CreatedFrom)
	}
	if filter.CreatedTo > 0 {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, filter.CreatedTo)
	}
	if filter.UpdatedFrom > 0 {
		clauses = append(clauses, "updated_at >= ?")
		args = append(args, filter.UpdatedFrom)
	}
	if filter.UpdatedTo > 0 {
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, filter.UpdatedTo)
	}
	return strings.Join(clauses, " AND "), args
}

func (s *sqliteStore) countPendingCommands(filter JobFilter) (int, error) {
	if !jobFilterIncludesKind(filter.Kind, "command") || filter.FinalSet && filter.Final {
		return 0, nil
	}
	clauses := []string{"1 = 1"}
	args := make([]interface{}, 0, 6)
	if deviceCode := stringsTrimSpace(filter.DeviceCode); deviceCode != "" {
		clauses = append(clauses, "device_code = ?")
		args = append(args, deviceCode)
	}
	if identifier := stringsTrimSpace(filter.Identifier); identifier != "" {
		clauses = append(clauses, "identifier = ?")
		args = append(args, identifier)
	}
	if filter.CreatedFrom > 0 {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, filter.CreatedFrom)
	}
	if filter.CreatedTo > 0 {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, filter.CreatedTo)
	}
	if filter.UpdatedFrom > 0 {
		clauses = append(clauses, "updated_at >= ?")
		args = append(args, filter.UpdatedFrom)
	}
	if filter.UpdatedTo > 0 {
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, filter.UpdatedTo)
	}
	row := s.db.QueryRow(`SELECT COUNT(*) FROM control_pending_commands WHERE `+strings.Join(clauses, " AND "), args...)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *sqliteStore) countPendingProperties(filter JobFilter) (int, error) {
	if !jobFilterIncludesKind(filter.Kind, "property") || filter.FinalSet && filter.Final {
		return 0, nil
	}
	clauses := []string{"1 = 1"}
	args := make([]interface{}, 0, 5)
	if deviceCode := stringsTrimSpace(filter.DeviceCode); deviceCode != "" {
		clauses = append(clauses, "device_code = ?")
		args = append(args, deviceCode)
	}
	if filter.CreatedFrom > 0 {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, filter.CreatedFrom)
	}
	if filter.CreatedTo > 0 {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, filter.CreatedTo)
	}
	if filter.UpdatedFrom > 0 {
		clauses = append(clauses, "updated_at >= ?")
		args = append(args, filter.UpdatedFrom)
	}
	if filter.UpdatedTo > 0 {
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, filter.UpdatedTo)
	}
	row := s.db.QueryRow(`SELECT COUNT(*) FROM control_pending_properties WHERE `+strings.Join(clauses, " AND "), args...)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func jobFilterIncludesKind(raw string, root string) bool {
	kind := stringsTrimSpace(raw)
	root = stringsTrimSpace(root)
	if root == "" || kind == "" {
		return true
	}
	if strings.Contains(kind, ":") {
		return kind == root || strings.HasPrefix(kind, root+":")
	}
	return kind == root
}

func (s *sqliteStore) LoadLatestResult(traceID string) (ctl.Result, bool, error) {
	row := s.db.QueryRow(`SELECT result_json FROM control_results WHERE trace_id = ? ORDER BY id DESC LIMIT 1`, traceID)
	var raw string
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return ctl.Result{}, false, nil
		}
		return ctl.Result{}, false, err
	}
	var result ctl.Result
	if err := jsonUnmarshal([]byte(raw), &result); err != nil {
		return ctl.Result{}, false, err
	}
	if result.Data == nil {
		result.Data = map[string]interface{}{}
	}
	return result, true, nil
}

func (s *sqliteStore) ListResults(traceID string, limit int) ([]ctl.Result, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite store is not initialized")
	}
	traceID = stringsTrimSpace(traceID)
	if traceID == "" {
		return []ctl.Result{}, nil
	}
	query := `SELECT result_json FROM control_results WHERE trace_id = ? ORDER BY id ASC`
	args := []interface{}{traceID}
	if limit > 0 {
		query = `SELECT result_json FROM (SELECT id, result_json FROM control_results WHERE trace_id = ? ORDER BY id DESC LIMIT ?) ORDER BY id ASC`
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]ctl.Result, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var result ctl.Result
		if err := jsonUnmarshal([]byte(raw), &result); err != nil {
			return nil, err
		}
		if result.Data == nil {
			result.Data = map[string]interface{}{}
		}
		items = append(items, result)
	}
	return items, rows.Err()
}

func (s *sqliteStore) UpsertJob(job JobState) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("sqlite store is not initialized")
	}
	if job.TraceID == "" {
		return false, fmt.Errorf("trace_id is required")
	}
	current, found, err := s.LoadJob(job.TraceID)
	if err != nil {
		return false, err
	}
	if !found {
		if job.CreatedAt <= 0 {
			job.CreatedAt = job.UpdatedAt
		}
		if job.UpdatedAt <= 0 {
			job.UpdatedAt = job.CreatedAt
		}
		if IsFinalCode(job.Code) && job.FinishedAt <= 0 {
			job.FinishedAt = job.UpdatedAt
		}
		_, err = s.db.Exec(`INSERT INTO control_jobs(trace_id, device_code, product_code, kind, identifier, code, message, created_at, updated_at, finished_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			job.TraceID, job.DeviceCode, job.ProductCode, job.Kind, job.Identifier, job.Code, job.Message, job.CreatedAt, job.UpdatedAt, job.FinishedAt)
		return err == nil, err
	}
	if IsFinalCode(current.Code) {
		return false, nil
	}
	if job.CreatedAt <= 0 {
		job.CreatedAt = current.CreatedAt
		if job.CreatedAt <= 0 {
			job.CreatedAt = job.UpdatedAt
		}
	}
	if job.UpdatedAt <= 0 {
		job.UpdatedAt = current.UpdatedAt
	}
	if IsFinalCode(job.Code) && job.FinishedAt <= 0 {
		job.FinishedAt = job.UpdatedAt
	}
	if current.Code == job.Code && current.Message == job.Message && stringsTrimSpace(current.DeviceCode) == stringsTrimSpace(job.DeviceCode) && stringsTrimSpace(current.ProductCode) == stringsTrimSpace(job.ProductCode) && stringsTrimSpace(current.Identifier) == stringsTrimSpace(job.Identifier) {
		return false, nil
	}
	_, err = s.db.Exec(`UPDATE control_jobs SET device_code = ?, product_code = ?, kind = ?, identifier = ?, code = ?, message = ?, updated_at = ?, finished_at = ? WHERE trace_id = ?`,
		job.DeviceCode, job.ProductCode, job.Kind, job.Identifier, job.Code, job.Message, job.UpdatedAt, job.FinishedAt, job.TraceID)
	return err == nil, err
}

func (s *sqliteStore) SaveResult(traceID string, result ctl.Result, final bool) error {
	finalVal := 0
	if final {
		finalVal = 1
	}
	latest, found, err := s.LoadLatestResult(traceID)
	if err == nil && found {
		if latest.Code == result.Code && latest.Message == result.Message && latest.Time == result.Time && fmt.Sprintf("%v", latest.Data) == fmt.Sprintf("%v", result.Data) {
			return nil
		}
	}
	_, err = s.db.Exec(`INSERT INTO control_results(trace_id, code, message, result_json, is_final, reported_at) VALUES(?, ?, ?, ?, ?, ?)`, traceID, result.Code, result.Message, string(mustMarshal(result)), finalVal, result.Time)
	return err
}

func (s *sqliteStore) SavePendingCommand(job PendingCommand) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("sqlite store is not initialized")
	}
	payload := mustMarshal(job.Request)
	result, err := s.db.Exec(`INSERT OR IGNORE INTO control_pending_commands(trace_id, device_code, product_code, identifier, request_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)`,
		job.TraceID, job.DeviceCode, job.ProductCode, job.Identifier, string(payload), job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *sqliteStore) DeletePendingCommand(traceID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	_, err := s.db.Exec(`DELETE FROM control_pending_commands WHERE trace_id = ?`, traceID)
	return err
}

func (s *sqliteStore) ListPendingCommands() ([]PendingCommand, error) {
	rows, err := s.db.Query(`SELECT trace_id, device_code, product_code, identifier, request_json, created_at, updated_at FROM control_pending_commands ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []PendingCommand
	for rows.Next() {
		var item PendingCommand
		var raw string
		if err := rows.Scan(&item.TraceID, &item.DeviceCode, &item.ProductCode, &item.Identifier, &raw, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if err := jsonUnmarshal([]byte(raw), &item.Request); err != nil {
			return nil, err
		}
		if item.Request.Data == nil {
			item.Request.Data = map[string]interface{}{}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *sqliteStore) SavePendingProperty(job PendingProperty) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("sqlite store is not initialized")
	}
	payload := mustMarshal(job.Request)
	result, err := s.db.Exec(`INSERT OR IGNORE INTO control_pending_properties(trace_id, device_code, product_code, operation, request_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)`,
		job.TraceID, job.DeviceCode, job.ProductCode, normalizePendingPropertyOperation(job.Operation), string(payload), job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *sqliteStore) DeletePendingProperty(traceID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	_, err := s.db.Exec(`DELETE FROM control_pending_properties WHERE trace_id = ?`, traceID)
	return err
}

func (s *sqliteStore) ListPendingProperties() ([]PendingProperty, error) {
	rows, err := s.db.Query(`SELECT trace_id, device_code, product_code, operation, request_json, created_at, updated_at FROM control_pending_properties ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []PendingProperty
	for rows.Next() {
		var item PendingProperty
		var raw string
		if err := rows.Scan(&item.TraceID, &item.DeviceCode, &item.ProductCode, &item.Operation, &raw, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if err := jsonUnmarshal([]byte(raw), &item.Request); err != nil {
			return nil, err
		}
		item.Operation = normalizePendingPropertyOperation(item.Operation)
		if item.Request.Data == nil {
			item.Request.Data = map[string]interface{}{}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func normalizePendingPropertyOperation(operation string) string {
	switch stringsTrimSpace(operation) {
	case "get":
		return "get"
	default:
		return "set"
	}
}

func (s *sqliteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func mustMarshal(v interface{}) []byte {
	buf, err := jsonMarshal(v)
	if err != nil {
		return []byte(fmt.Sprintf(`{"marshal_error":%q}`, err.Error()))
	}
	return buf
}
