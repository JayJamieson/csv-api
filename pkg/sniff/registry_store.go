package sniff

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RegistryStore persists the field-type registry in the metadata DB
// (Turso/libsql), so a canonical field or synonym added through the API
// survives a restart and is shared by every server instance pointed at the
// same database. On first migrate it seeds the builtin fields and synonyms
// from DefaultRegistry; after that, the DB is the source of truth and
// DefaultRegistry is not consulted again.
type RegistryStore struct {
	DB *sql.DB
}

// Migrate creates the tables and seeds builtins. Idempotent: re-running on an
// already-seeded database changes nothing (INSERT OR IGNORE), so a field or
// synonym added later through the API is never clobbered by a restart.
func (s *RegistryStore) Migrate(ctx context.Context) error {
	if _, err := s.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS field_type (
			key         TEXT PRIMARY KEY,
			label       TEXT NOT NULL,
			value_kind  TEXT NOT NULL,
			required    INTEGER NOT NULL DEFAULT 0,
			claim_order INTEGER NOT NULL DEFAULT 100,
			builtin     INTEGER NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("migrate field_type: %w", err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS field_synonym (
			field_key TEXT NOT NULL REFERENCES field_type(key),
			synonym   TEXT NOT NULL,
			source    TEXT,
			PRIMARY KEY (field_key, synonym)
		)`); err != nil {
		return fmt.Errorf("migrate field_synonym: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for _, f := range builtinFields {
		if _, err := s.DB.ExecContext(ctx, `
			INSERT OR IGNORE INTO field_type
				(key, label, value_kind, required, claim_order, builtin, created_at)
			VALUES (?, ?, ?, ?, ?, 1, ?)`,
			string(f.Key), f.Label, string(f.ValueKind), boolInt(f.Required), f.ClaimOrder, now,
		); err != nil {
			return fmt.Errorf("seed field_type %s: %w", f.Key, err)
		}
	}
	for field, syns := range builtinSynonyms {
		for _, syn := range syns {
			if _, err := s.DB.ExecContext(ctx, `
				INSERT OR IGNORE INTO field_synonym (field_key, synonym, source)
				VALUES (?, ?, 'builtin')`, string(field), syn,
			); err != nil {
				return fmt.Errorf("seed field_synonym %s/%s: %w", field, syn, err)
			}
		}
	}
	return nil
}

// LoadRegistry builds a FieldRegistry entirely from DB state (seeded at
// Migrate time, then whatever's been added since via AddField/AddSynonym).
func (s *RegistryStore) LoadRegistry(ctx context.Context) (*FieldRegistry, error) {
	r := &FieldRegistry{synonyms: map[CanonicalField][]string{}}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT key, label, value_kind, required, claim_order, builtin
		FROM field_type`)
	if err != nil {
		return nil, fmt.Errorf("load field_type: %w", err)
	}
	for rows.Next() {
		var key, label, kind string
		var required, claimOrder, builtin int
		if err := rows.Scan(&key, &label, &kind, &required, &claimOrder, &builtin); err != nil {
			rows.Close()
			return nil, err
		}
		r.fields = append(r.fields, FieldDef{
			Key: CanonicalField(key), Label: label, ValueKind: ValueKind(kind),
			Required: required != 0, ClaimOrder: claimOrder, Builtin: builtin != 0,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	synRows, err := s.DB.QueryContext(ctx, `SELECT field_key, synonym FROM field_synonym`)
	if err != nil {
		return nil, fmt.Errorf("load field_synonym: %w", err)
	}
	defer synRows.Close()
	for synRows.Next() {
		var field, syn string
		if err := synRows.Scan(&field, &syn); err != nil {
			return nil, err
		}
		key := CanonicalField(field)
		r.synonyms[key] = append(r.synonyms[key], syn)
	}
	return r, synRows.Err()
}

// AddField persists a new canonical field and returns the freshly loaded
// definition. Fails if the key already exists or the value kind is
// unrecognized — the same validation FieldRegistry.AddField does in memory,
// checked here too since this is the actual write path the API uses.
func (s *RegistryStore) AddField(ctx context.Context, def FieldDef) error {
	if def.Key == FieldUnknown {
		return fmt.Errorf("field key cannot be empty")
	}
	if !validValueKind(def.ValueKind) {
		return fmt.Errorf("unknown value_kind %q", def.ValueKind)
	}
	if def.ClaimOrder == 0 {
		def.ClaimOrder = defaultClaimOrder
	}
	var exists int
	if err := s.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM field_type WHERE key = ?`, string(def.Key),
	).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return fmt.Errorf("field %q already exists", def.Key)
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO field_type (key, label, value_kind, required, claim_order, builtin, created_at)
		VALUES (?, ?, ?, ?, ?, 0, ?)`,
		string(def.Key), def.Label, string(def.ValueKind), boolInt(def.Required),
		def.ClaimOrder, time.Now().UTC().Format(time.RFC3339))
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// AddSynonym teaches a new header-name fragment for an existing field.
// source records provenance ("user" for a manual API call, "corpus" for a
// human-accepted corpus-scan suggestion) — never "auto": unconfirmed
// detector guesses must not be written here, or the dictionary compounds its
// own mistakes, the same invariant the confirmed-mapping store enforces.
func (s *RegistryStore) AddSynonym(ctx context.Context, key CanonicalField, synonym, source string) error {
	var exists int
	if err := s.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM field_type WHERE key = ?`, string(key),
	).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("unknown field %q", key)
	}
	if source == "" {
		source = "user"
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT OR IGNORE INTO field_synonym (field_key, synonym, source)
		VALUES (?, ?, ?)`, string(key), synonym, source)
	return err
}
