package db

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// newTestDB builds a DB with only the DuckDB half wired up (no Turso/libsql
// connection), which is all GetCSV touches.
func newTestDB(t *testing.T) *DB {
	t.Helper()
	return &DB{duckDBMap: make(map[string]*sql.DB), dataDir: t.TempDir()}
}

func seedTable(t *testing.T, d *DB, id string) {
	t.Helper()
	conn, err := d.getDuckDBConnection(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(),
		"CREATE TABLE csv_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(n, s)"); err != nil {
		t.Fatal(err)
	}
}

// TestGetCSV_OmittedFormatDefaultsToObjects is a regression test for the
// panic previously triggered by GetCSV(&QueryCSV{Format: ""}): oapi-codegen's
// query binding leaves Format as "" when the client omits `format` (it does
// not apply the OpenAPI spec's declared "objects" default), and the old code
// called transformFuncs[""](...) directly — a map miss returns a nil func,
// and calling it panics. This must instead behave exactly like format=objects.
func TestGetCSV_OmittedFormatDefaultsToObjects(t *testing.T) {
	d := newTestDB(t)
	seedTable(t, d, "omitted-format")

	cols, rows, total, _, err := d.GetCSV(context.Background(), &QueryCSV{
		ID: "omitted-format", TableName: "csv_data", Format: "",
	})
	if err != nil {
		t.Fatalf("GetCSV with omitted format: %v", err)
	}
	if total != 2 || len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", total)
	}
	if _, ok := rows[0].(map[string]any); !ok {
		t.Fatalf("omitted format: got row type %T, want map[string]any (objects shape)", rows[0])
	}
	_ = cols
}

// TestGetCSV_UnknownFormatErrorsInsteadOfPanicking covers the other half of
// the same bug: a client-supplied format that isn't "objects" or "array"
// (e.g. the README's documented `_shape` query param name being sent as a
// `format` value by mistake) used to hit the same nil-func panic. It must now
// surface as a normal error return.
func TestGetCSV_UnknownFormatErrorsInsteadOfPanicking(t *testing.T) {
	d := newTestDB(t)
	seedTable(t, d, "bad-format")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("GetCSV panicked on an unknown format instead of returning an error: %v", r)
		}
	}()

	_, _, _, _, err := d.GetCSV(context.Background(), &QueryCSV{
		ID: "bad-format", TableName: "csv_data", Format: "_shape",
	})
	if err == nil {
		t.Fatal("expected an error for an unrecognized format, got nil")
	}
}

func TestGetCSV_ArrayFormat(t *testing.T) {
	d := newTestDB(t)
	seedTable(t, d, "array-format")

	_, rows, total, _, err := d.GetCSV(context.Background(), &QueryCSV{
		ID: "array-format", TableName: "csv_data", Format: "array",
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("got %d rows, want 2", total)
	}
	if _, ok := rows[0].([]any); !ok {
		t.Fatalf("array format: got row type %T, want []any", rows[0])
	}
}
