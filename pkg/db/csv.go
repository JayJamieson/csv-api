package db

import (
	"fmt"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

type transformFunc func(columns []string, values []any) any

var transformFuncs = map[string]transformFunc{
	"array":   transformArray,
	"objects": transformObject,
}

// resolveTransform looks up the row-shaping function for a `format` query
// param. oapi-codegen's query binding does not apply the OpenAPI spec's
// declared default ("objects") for value-typed params — an omitted format
// arrives here as "", not "objects" — so that case is defaulted explicitly.
// Anything else that isn't a known shape is a bad request and returns an
// error; the caller must not index transformFuncs directly, since a map miss
// there silently yields a nil func that panics when called.
func resolveTransform(format string) (transformFunc, error) {
	f := format
	if f == "" {
		f = "objects"
	}
	fn, ok := transformFuncs[f]
	if !ok {
		return nil, fmt.Errorf("invalid format %q: must be \"objects\" or \"array\"", format)
	}
	return fn, nil
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
