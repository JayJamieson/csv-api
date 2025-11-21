package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JayJamieson/csv-api/pkg/models"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb/v2"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

type DB struct {
	tursoConn *sql.DB
	duckDBMap map[string]*sql.DB
	dataDir   string
}

func New(dbURL string) (*DB, error) {
	conn, err := sql.Open("libsql", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	conn.SetConnMaxIdleTime(9)

	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS csv_table (
			id TEXT PRIMARY KEY,
			filename TEXT NOT NULL,
			table_name TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			persisted BOOLEAN DEFAULT 0
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create csv_table: %w", err)
	}

	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &DB{
		tursoConn: conn,
		duckDBMap: make(map[string]*sql.DB),
		dataDir:   dataDir,
	}, nil
}

func (db *DB) Close() error {
	for _, duckConn := range db.duckDBMap {
		if err := duckConn.Close(); err != nil {
			log.Printf("Error closing DuckDB connection: %v", err)
		}
	}

	return db.tursoConn.Close()
}

func (db *DB) getDuckDBPath(id string) string {
	return filepath.Join(db.dataDir, fmt.Sprintf("%s.db", id))
}

func (db *DB) getDuckDBConnection(id string) (*sql.DB, error) {
	if conn, ok := db.duckDBMap[id]; ok {
		return conn, nil
	}

	dbPath := db.getDuckDBPath(id)
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	db.duckDBMap[id] = conn
	return conn, nil
}

func (db *DB) ImportCSVFromReader(ctx context.Context, filename string, reader io.Reader) (*models.CSVTable, error) {
	id := uuid.New().String()
	tableName := "csv_data"

	tempDir, err := os.MkdirTemp("", "csv-import")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	tempFile := filepath.Join(tempDir, "data.csv")
	f, err := os.Create(tempFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := io.Copy(f, reader); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to write CSV data: %w", err)
	}
	f.Close()

	duckConn, err := db.getDuckDBConnection(id)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', auto_detect=TRUE, strict_mode=false, store_rejects=true)",
		tableName, tempFile)

	if _, err := duckConn.Exec(query); err != nil {
		return nil, fmt.Errorf("failed to import CSV into DuckDB: %w", err)
	}

	now := time.Now().UTC()
	_, err = db.tursoConn.ExecContext(ctx, `
		INSERT INTO csv_table (id, filename, table_name, created_at, persisted)
		VALUES (?, ?, ?, ?, 0)
	`, id, filename, tableName, now)
	if err != nil {
		return nil, fmt.Errorf("failed to store CSV reference: %w", err)
	}

	return &models.CSVTable{
		ID:        id,
		Filename:  filename,
		TableName: tableName,
		CreatedAt: now,
		Persisted: false,
	}, nil
}

func (db *DB) GetCSVTable(ctx context.Context, id string) (*models.CSVTable, error) {
	var csvTable models.CSVTable
	err := db.tursoConn.QueryRowContext(ctx, `
		SELECT id, filename, table_name, created_at, persisted
		FROM csv_table
		WHERE id = ?
	`, id).Scan(&csvTable.ID, &csvTable.Filename, &csvTable.TableName, &csvTable.CreatedAt, &csvTable.Persisted)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("CSV table with ID %s not found", id)
		}
		return nil, fmt.Errorf("failed to get CSV table: %w", err)
	}
	return &csvTable, nil
}

func (db *DB) QueryDuckDBTable(
	ctx context.Context,
	id string,
	tableName string,
	limit int,
	offset int,
	sortCol string,
	sortDesc bool,
	showRowID bool,
) ([]string, []map[string]any, [][]any, int, float64, error) {
	startTime := time.Now()

	duckConn, err := db.getDuckDBConnection(id)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}

	query := "SELECT "
	if showRowID {
		query += "row_number() OVER () as rowid, "
	}
	query += "* FROM " + tableName

	if sortCol != "" {
		direction := ""
		if sortDesc {
			direction = " DESC"
		}
		query += fmt.Sprintf(" ORDER BY \"%s\"%s", sortCol, direction)
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := duckConn.Query(query)
	if err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("failed to get columns: %w", err)
	}

	var objectRows []map[string]any
	var arrayRows [][]any

	for rows.Next() {
		values := make([]any, len(columns))

		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, nil, nil, 0, 0, fmt.Errorf("failed to scan row: %w", err)
		}

		objRow := make(map[string]any)
		arrRow := make([]any, len(columns))

		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			objRow[col] = val
			arrRow[i] = val
		}

		objectRows = append(objectRows, objRow)
		arrayRows = append(arrayRows, arrRow)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	queryTime := float64(time.Since(startTime).Microseconds()) / 1000.0

	return columns, objectRows, arrayRows, len(arrayRows), queryTime, nil
}

func (db *DB) QueryCSVTable(
	ctx context.Context,
	tableName string,
	limit int,
	offset int,
	sortCol string,
	sortDesc bool,
	showRowID bool,
) ([]string, []map[string]any, [][]any, int, float64, error) {
	startTime := time.Now()

	query := "SELECT "
	if showRowID {
		query += "rowid, "
	}
	query += "* FROM " + tableName

	if sortCol != "" {
		direction := ""
		if sortDesc {
			direction = " DESC"
		}
		query += fmt.Sprintf(" ORDER BY \"%s\"%s", sortCol, direction)
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := db.tursoConn.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("failed to get columns: %w", err)
	}

	var objectRows []map[string]any
	var arrayRows [][]any

	for rows.Next() {
		values := make([]any, len(columns))

		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, nil, nil, 0, 0, fmt.Errorf("failed to scan row: %w", err)
		}

		objRow := make(map[string]any)
		arrRow := make([]any, len(columns))

		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			objRow[col] = val
			arrRow[i] = val
		}

		objectRows = append(objectRows, objRow)
		arrayRows = append(arrayRows, arrRow)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, nil, 0, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	queryTime := float64(time.Since(startTime).Microseconds()) / 1000.0

	return columns, objectRows, arrayRows, len(arrayRows), queryTime, nil
}

func (db *DB) PersistToTurso(ctx context.Context, id string) error {
	csvTable, err := db.GetCSVTable(ctx, id)
	if err != nil {
		return err
	}

	if csvTable.Persisted {
		return fmt.Errorf("table already persisted")
	}

	duckConn, err := db.getDuckDBConnection(id)
	if err != nil {
		return err
	}

	columnQuery := fmt.Sprintf("PRAGMA table_info('%s')", csvTable.TableName)
	rows, err := duckConn.Query(columnQuery)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	var columns []models.ColumnInfo
	for rows.Next() {
		var col models.ColumnInfo
		if err := rows.Scan(&col.CID, &col.Name, &col.Type, &col.NotNull, &col.DefaultVal, &col.PK); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, col)
	}
	rows.Close()

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating column rows: %w", err)
	}

	tx, err := db.tursoConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("Error rolling back transaction: %v", rbErr)
			}
		}
	}()

	tursoPermanentTableName := "csv_" + strings.ReplaceAll(id, "-", "_")
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", tursoPermanentTableName)

	createTableSQL += fmt.Sprintf("\"%s\" TEXT", strings.ReplaceAll(columns[0].Name, "\"", ""))

	for _, col := range columns[1:] {
		sanitizedCol := strings.ReplaceAll(col.Name, "\"", "")
		createTableSQL += fmt.Sprintf(", \"%s\" TEXT", sanitizedCol)
	}
	createTableSQL += ")"

	_, err = tx.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create permanent table: %w", err)
	}

	dataQuery := fmt.Sprintf("SELECT * FROM %s", csvTable.TableName)
	dataRows, err := duckConn.Query(dataQuery)
	if err != nil {
		return fmt.Errorf("failed to query DuckDB data: %w", err)
	}
	defer dataRows.Close()

	columnNames, err := dataRows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	columnList := ""
	placeholders := ""
	for i, col := range columnNames {
		sanitizedCol := strings.ReplaceAll(col, "\"", "")
		if i > 0 {
			columnList += ", "
			placeholders += ", "
		}
		columnList += fmt.Sprintf("\"%s\"", sanitizedCol)
		placeholders += "?"
	}

	insertStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tursoPermanentTableName, columnList, placeholders))
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer insertStmt.Close()

	for dataRows.Next() {

		values := make([]any, len(columnNames))

		scanArgs := make([]any, len(columnNames))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := dataRows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan data row: %w", err)
		}

		stringValues := make([]any, len(values))
		for i, v := range values {
			if v == nil {
				stringValues[i] = nil
				continue
			}

			switch val := v.(type) {
			case []byte:
				stringValues[i] = string(val)
			default:
				stringValues[i] = fmt.Sprintf("%v", val)
			}
		}

		if _, err := insertStmt.ExecContext(ctx, stringValues...); err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
		}
	}

	if err = dataRows.Err(); err != nil {
		return fmt.Errorf("error iterating data rows: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE csv_table
		SET persisted = 1, table_name = ?
		WHERE id = ?
	`, tursoPermanentTableName, id)
	if err != nil {
		return fmt.Errorf("failed to update CSV table persistence status: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
