package db

import (
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

type transformFunc func(columns []string, values []any) any

var transformFuncs = map[string]transformFunc{
	"array":   transformArray,
	"objects": transformObject,
}

type CSVTable struct {
	ID        string    `json:"id" db:"id"`
	Filename  string    `json:"filename" db:"filename"`
	TableName string    `json:"table_name" db:"table_name"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	Persisted bool      `json:"persisted" db:"persisted"`
}

type ColumnInfo struct {
	CID        int
	Name       string
	Type       string
	NotNull    bool
	DefaultVal any
	PK         bool
}

type QueryCSV struct {
	ID         string
	TableName  string
	Limit      int
	Offset     int
	SortColumn string
	SortOrder  string
	Format     string
}

// coerceValue normalizes a raw driver value into something that JSON-encodes
// sensibly. []byte becomes a string; a DuckDB DECIMAL (duckdb.Decimal) becomes
// a float64 so a typed price column serializes as a number (e.g. 1145.0) rather
// than the driver's {Width,Scale,Value} struct.
func coerceValue(val any) any {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case duckdb.Decimal:
		return v.Float64()
	case *duckdb.Decimal:
		return v.Float64()
	default:
		return val
	}
}

func transformArray(columns []string, values []any) any {
	arrRow := make([]any, len(columns))

	for i := range columns {
		arrRow[i] = coerceValue(values[i])
	}
	return arrRow
}

func transformObject(columns []string, values []any) any {
	objRow := make(map[string]any)

	for i, col := range columns {
		objRow[col] = coerceValue(values[i])
	}
	return objRow
}
