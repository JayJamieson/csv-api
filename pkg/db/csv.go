package db

import (
	"time"
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

func transformArray(columns []string, values []any) any {
	arrRow := make([]any, len(columns))

	for i, _ := range columns {
		val := values[i]
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		arrRow[i] = val
	}
	return arrRow
}

func transformObject(columns []string, values []any) any {
	objRow := make(map[string]any)

	for i, col := range columns {
		val := values[i]
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		objRow[col] = val
	}
	return objRow
}
