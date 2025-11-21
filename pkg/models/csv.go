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

type ColumnInfo struct {
	CID        int
	Name       string
	Type       string
	NotNull    bool
	DefaultVal any
	PK         bool
}
