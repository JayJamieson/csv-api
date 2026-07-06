package sniff

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// SavedMapping is a confirmed mapping keyed by header-set fingerprint.
// Mappings are stored by column NAME, not index, so a supplier reordering
// columns doesn't invalidate the learned mapping (the fingerprint sorts
// names for the same reason).
type SavedMapping struct {
	Fingerprint string         `json:"fingerprint"`
	Supplier    string         `json:"supplier,omitempty"`
	Mappings    []FieldMapping `json:"mappings"`
	HeaderIndex int            `json:"header_index"` // hint only; re-detected each import
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	UseCount    int64          `json:"use_count"`
}

// MappingStore persists confirmed mappings in the metadata DB (Turso/libsql).
// It deliberately does NOT store auto-proposals — only mappings a human
// confirmed (or auto-mode committed with high confidence). Learning from
// unconfirmed guesses would compound errors.
type MappingStore struct {
	DB *sql.DB
}

// Migrate creates the table. Idempotent.
func (s *MappingStore) Migrate(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS csv_mappings (
			fingerprint  TEXT PRIMARY KEY,
			supplier     TEXT,
			mappings     TEXT NOT NULL,   -- JSON array of FieldMapping
			header_index INTEGER NOT NULL DEFAULT 0,
			created_at   TEXT NOT NULL,
			updated_at   TEXT NOT NULL,
			use_count    INTEGER NOT NULL DEFAULT 0
		)`)
	return err
}

// Get returns the saved mapping for a fingerprint, or (nil, nil) if unknown.
func (s *MappingStore) Get(ctx context.Context, fingerprint string) (*SavedMapping, error) {
	row := s.DB.QueryRowContext(ctx, `
		SELECT fingerprint, coalesce(supplier,''), mappings, header_index,
		       created_at, updated_at, use_count
		FROM csv_mappings WHERE fingerprint = ?`, fingerprint)

	var m SavedMapping
	var mappingsJSON, created, updated string
	err := row.Scan(&m.Fingerprint, &m.Supplier, &mappingsJSON,
		&m.HeaderIndex, &created, &updated, &m.UseCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(mappingsJSON), &m.Mappings); err != nil {
		return nil, fmt.Errorf("corrupt saved mapping %s: %w", fingerprint, err)
	}
	m.CreatedAt, _ = time.Parse(time.RFC3339, created)
	m.UpdatedAt, _ = time.Parse(time.RFC3339, updated)
	return &m, nil
}

// Put upserts a confirmed mapping.
func (s *MappingStore) Put(ctx context.Context, m *SavedMapping) error {
	mappingsJSON, err := json.Marshal(m.Mappings)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = s.DB.ExecContext(ctx, `
		INSERT INTO csv_mappings
			(fingerprint, supplier, mappings, header_index, created_at, updated_at, use_count)
		VALUES (?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT(fingerprint) DO UPDATE SET
			supplier     = excluded.supplier,
			mappings     = excluded.mappings,
			header_index = excluded.header_index,
			updated_at   = excluded.updated_at`,
		m.Fingerprint, m.Supplier, string(mappingsJSON), m.HeaderIndex, now, now)
	return err
}

// Touch bumps use_count when a saved mapping is applied to an import; the
// count is a cheap signal of which suppliers' layouts are stable.
func (s *MappingStore) Touch(ctx context.Context, fingerprint string) error {
	_, err := s.DB.ExecContext(ctx, `
		UPDATE csv_mappings
		SET use_count = use_count + 1, updated_at = ?
		WHERE fingerprint = ?`,
		time.Now().UTC().Format(time.RFC3339), fingerprint)
	return err
}

// ApplySaved rebinds a saved (name-keyed) mapping onto this file's columns.
// Returns false if any mapped column name is missing — which means the
// fingerprint matched but the header set changed, i.e. a hash collision or a
// stale record; either way, fall back to proposal.
func ApplySaved(saved *SavedMapping, columns []string) ([]FieldMapping, bool) {
	pos := map[string]int{}
	for i, c := range columns {
		pos[c] = i
	}
	out := make([]FieldMapping, 0, len(saved.Mappings))
	for _, m := range saved.Mappings {
		if m.Field == FieldUnknown {
			continue
		}
		i, ok := pos[m.SourceColumn]
		if !ok {
			return nil, false
		}
		m.SourceIndex = i
		m.Confidence = 1.0
		m.Notes = "from saved mapping"
		out = append(out, m)
	}
	return out, true
}
