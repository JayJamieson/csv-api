package models

import (
	"time"
)

type CSVTable struct {
	ID        string    `json:"id" db:"id"`
	Filename  string    `json:"filename" db:"filename"`
	TableName string    `json:"table_name" db:"table_name"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	Persisted bool      `json:"persisted" db:"persisted"`
}

type ErrorResponse struct {
	Timestamp string `json:"timestamp"`
	Error     string `json:"error"`
	Message   string `json:"message"`
}

type LoadResponse struct {
	OK       bool   `json:"ok"`
	Endpoint string `json:"endpoint"`
}

type PersistResponse struct {
	OK        bool   `json:"ok"`
	Message   string `json:"message"`
	Persisted bool   `json:"persisted"`
}

type DataResponseBase struct {
	OK      bool     `json:"ok"`
	QueryMS float64  `json:"query_ms"`
	Columns []string `json:"columns"`
	Total   int      `json:"total,omitempty"`
}

type DataResponseObjects struct {
	DataResponseBase
	Rows []map[string]any `json:"rows"`
}

type DataResponseArray struct {
	DataResponseBase
	Rows [][]any `json:"rows"`
}

type ColumnInfo struct {
	CID        int
	Name       string
	Type       string
	NotNull    bool
	DefaultVal any
	PK         bool
}
