package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// StagedDB returns the per-import DuckDB connection for a staged import.
// Staging, typed, and quarantine tables all live in the same per-import
// database file, matching the existing one-file-per-import layout: cleanup
// is file deletion, and table names can be simple constants because there is
// no shared namespace to collide in.
func (db *DB) StagedDB(importID string) (*sql.DB, error) {
	return db.getDuckDBConnection(importID)
}

// Meta exposes the Turso metadata connection for stores that live alongside
// csv_table (mapping store, import state).
func (db *DB) Meta() *sql.DB {
	return db.tursoConn
}

// Simple constant table names inside each per-import database.
const (
	StagingTable    = "staging"
	TypedTable      = "csv_data" // same name ImportCSVFromReader uses, so
	                             // GetCSV/QueryCSVTable work unchanged
	QuarantineTable = "quarantine"
)

// PreviewStaging returns the first n rows of the staging table as strings,
// excluding the _row bookkeeping column, for the /load response preview.
func (db *DB) PreviewStaging(ctx context.Context, importID string, n int) ([][]string, error) {
	conn, err := db.getDuckDBConnection(importID)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`SELECT * EXCLUDE (_row) FROM %s ORDER BY _row LIMIT %d`, StagingTable, n))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringRows(rows)
}

// SampleStaging returns up to n rows for the mapping proposer's value
// classifiers. Sampling from staging (post noise-skip) rather than the raw
// file means the proposer sees data rows, not section headings.
func (db *DB) SampleStaging(ctx context.Context, importID string, n int) ([][]string, error) {
	conn, err := db.getDuckDBConnection(importID)
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`SELECT * EXCLUDE (_row) FROM %s USING SAMPLE %d ROWS`, StagingTable, n))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringRows(rows)
}

// CountStaging reports staging row count for the commit-time LoadResult.
func (db *DB) CountStaging(ctx context.Context, importID string, out *int64) error {
	conn, err := db.getDuckDBConnection(importID)
	if err != nil {
		return err
	}
	return conn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", StagingTable)).Scan(out)
}

// RegisterTable records a committed typed table in csv_table so the existing
// /api/{id} query path serves it with zero changes.
func (db *DB) RegisterTable(ctx context.Context, importID, filename string) (*CSVTable, error) {
	now := time.Now().UTC()
	_, err := db.tursoConn.ExecContext(ctx, `
		INSERT INTO csv_table (id, filename, table_name, created_at, persisted)
		VALUES (?, ?, ?, ?, 0)
		ON CONFLICT(id) DO UPDATE SET filename = excluded.filename`,
		importID, filename, TypedTable, now)
	if err != nil {
		return nil, fmt.Errorf("register table: %w", err)
	}
	return &CSVTable{
		ID:        importID,
		Filename:  filename,
		TableName: TypedTable,
		CreatedAt: now,
	}, nil
}

// DropStagingArtifacts removes staging + quarantine after a caller decides
// they're done with them. Quarantine is intentionally kept by default after
// commit — inspecting rejects is the point — so this is explicit, not
// automatic.
func (db *DB) DropStagingArtifacts(ctx context.Context, importID string, dropQuarantine bool) error {
	conn, err := db.getDuckDBConnection(importID)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf("DROP TABLE IF EXISTS %s", StagingTable)); err != nil {
		return err
	}
	if dropQuarantine {
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("DROP TABLE IF EXISTS %s", QuarantineTable)); err != nil {
			return err
		}
	}
	return nil
}

// QuarantineRow is one rejected row: its original (uncleaned) source cells
// plus the reason it failed required-field validation. Cells are the raw
// values so you can see exactly what tripped the cast — a cleaned row would
// hide the "$ 1,2 3" that failed.
type QuarantineRow struct {
	Row    int64    `json:"row"`    // original row number in the source
	Cells  []string `json:"cells"`
	Reason string   `json:"reason"`
}

// QuarantineRows reads rejected rows for a committed import. Columns are the
// source columns (the header names), so the caller can line cells up under
// the same headers the preview used. Returns (columns, rows).
func (db *DB) QuarantineRows(ctx context.Context, importID string, limit, offset int) ([]string, []QuarantineRow, error) {
	conn, err := db.getDuckDBConnection(importID)
	if err != nil {
		return nil, nil, err
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`SELECT * FROM %s ORDER BY _row LIMIT %d OFFSET %d`,
		QuarantineTable, limit, offset))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	// Layout is: _row, <source cols...>, reason. Peel off the bookkeeping
	// ends so the middle lines up with the header names.
	sourceCols := cols
	if len(cols) >= 2 {
		sourceCols = cols[1 : len(cols)-1]
	}

	var out []QuarantineRow
	for rows.Next() {
		raw := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		qr := QuarantineRow{}
		if raw[0].Valid {
			// _row is text via all_varchar staging; parse best-effort
			fmt.Sscanf(raw[0].String, "%d", &qr.Row)
		}
		for i := 1; i < len(raw)-1; i++ {
			if raw[i].Valid {
				qr.Cells = append(qr.Cells, raw[i].String)
			} else {
				qr.Cells = append(qr.Cells, "")
			}
		}
		if last := raw[len(raw)-1]; last.Valid {
			qr.Reason = last.String
		}
		out = append(out, qr)
	}
	return sourceCols, out, rows.Err()
}

// --- import state persistence (Turso) ---
//
// State between /load and /commit lives in Turso, not process memory, so a
// restart mid-flow doesn't strand staged imports. The spool file and the
// per-import .db file are both on disk already; this row is the map back to
// them. Report/proposal are stored as JSON blobs — they're read-modify-write
// by a single flow, not queried relationally.

type ImportState struct {
	ID           string
	Supplier     string
	SpoolPath    string
	ReportJSON   []byte
	ProposalJSON []byte
	SavedApplied bool
	CreatedAt    time.Time
}

func (db *DB) MigrateImportState(ctx context.Context) error {
	_, err := db.tursoConn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS import_state (
			id            TEXT PRIMARY KEY,
			supplier      TEXT,
			spool_path    TEXT NOT NULL,
			report        TEXT NOT NULL,
			proposal      TEXT,
			saved_applied INTEGER NOT NULL DEFAULT 0,
			created_at    TEXT NOT NULL
		)`)
	return err
}

func (db *DB) SaveImportState(ctx context.Context, s *ImportState) error {
	_, err := db.tursoConn.ExecContext(ctx, `
		INSERT INTO import_state
			(id, supplier, spool_path, report, proposal, saved_applied, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			report = excluded.report,
			proposal = excluded.proposal,
			saved_applied = excluded.saved_applied`,
		s.ID, s.Supplier, s.SpoolPath, string(s.ReportJSON),
		nullableStr(s.ProposalJSON), boolInt(s.SavedApplied),
		time.Now().UTC().Format(time.RFC3339))
	return err
}

func (db *DB) LoadImportState(ctx context.Context, id string) (*ImportState, error) {
	row := db.tursoConn.QueryRowContext(ctx, `
		SELECT id, coalesce(supplier,''), spool_path, report,
		       coalesce(proposal,''), saved_applied, created_at
		FROM import_state WHERE id = ?`, id)
	var s ImportState
	var report, proposal, created string
	var savedApplied int
	if err := row.Scan(&s.ID, &s.Supplier, &s.SpoolPath, &report,
		&proposal, &savedApplied, &created); err != nil {
		return nil, err
	}
	s.ReportJSON = []byte(report)
	if proposal != "" {
		s.ProposalJSON = []byte(proposal)
	}
	s.SavedApplied = savedApplied != 0
	s.CreatedAt, _ = time.Parse(time.RFC3339, created)
	return &s, nil
}

func (db *DB) DeleteImportState(ctx context.Context, id string) error {
	_, err := db.tursoConn.ExecContext(ctx,
		"DELETE FROM import_state WHERE id = ?", id)
	return err
}

// --- helpers ---

func scanStringRows(rows *sql.Rows) ([][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			if v.Valid {
				row[i] = v.String
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func nullableStr(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
