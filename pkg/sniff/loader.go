package sniff

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// LoadResult reports what happened during a staged import. This is the
// quality signal the old "Status=Success" log never had.
type LoadResult struct {
	StagingTable    string             `json:"staging_table"`
	TypedTable      string             `json:"typed_table,omitempty"`
	QuarantineTable string             `json:"quarantine_table,omitempty"`
	RowsRaw         int64              `json:"rows_raw"`   // rows landed in staging
	RowsTyped       int64              `json:"rows_typed"` // rows promoted
	RowsQuarantined int64              `json:"rows_quarantined"`
	RowsFiltered    RowsFiltered       `json:"rows_filtered"`        // structural noise removed
	NullRates       map[string]float64 `json:"null_rates,omitempty"` // per mapped field
	Warnings        []string           `json:"warnings,omitempty"`
}

// RowsFiltered counts structural noise removed before typing.
type RowsFiltered struct {
	Blank           int64 `json:"blank"`
	SectionHeadings int64 `json:"section_headings"`
	RepeatedHeaders int64 `json:"repeated_headers"`
}

// Loader runs the staged import against a DuckDB connection.
type Loader struct {
	DB *sql.DB
}

// LoadStaging reads the normalized file into an all-VARCHAR staging table.
//
// Why all_varchar: type inference is the single biggest failure mode in
// one-shot read_csv_auto. By loading text-only we make the load itself nearly
// infallible, and move typing into SQL where failures are per-cell TRY_CAST
// nulls we can inspect — not per-file exceptions.
//
// null_padding absorbs ragged short rows; ignore_errors catches the rest and
// store_rejects keeps them visible.
func (l *Loader) LoadStaging(ctx context.Context, path, table string, rep *StructureReport) (*LoadResult, error) {
	res := &LoadResult{StagingTable: table}

	if rep.HeaderIndex < 0 {
		return nil, fmt.Errorf("no header selected; refuse to stage without one")
	}

	cols := make([]string, len(rep.Columns))
	for i, c := range rep.Columns {
		cols[i] = fmt.Sprintf("%s VARCHAR", quoteIdent(c))
	}

	// skip = header index + 1: DuckDB's skip is "lines before parsing
	// begins", and we supply names ourselves with header=false so the header
	// line itself must be skipped too.
	q := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s AS
		SELECT row_number() OVER () AS _row, *
		FROM read_csv(%s,
			delim=%s, header=false, skip=%d,
			columns={%s},
			all_varchar=true, null_padding=true,
			ignore_errors=true, store_rejects=true
		)`,
		quoteIdent(table),
		quoteStr(path),
		quoteStr(string(rep.Delimiter)),
		rep.HeaderIndex+1,
		columnsMap(rep.Columns),
	)
	if _, err := l.DB.ExecContext(ctx, q); err != nil {
		return nil, fmt.Errorf("staging load: %w", err)
	}

	if err := l.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(table)),
	).Scan(&res.RowsRaw); err != nil {
		return nil, err
	}
	_ = cols
	return res, nil
}

// FieldType returns the DuckDB target type for a value_kind.
func FieldType(kind ValueKind) string {
	switch kind {
	case KindCurrency:
		return "DECIMAL(12,4)"
	case KindInteger:
		return "INTEGER"
	default:
		return "VARCHAR"
	}
}

// cleanExpr wraps a source column in the cleaning + TRY_CAST expression for
// its target value_kind. Currency cleaning strips $, thousands separators,
// accounting-style parens (negative), and stray whitespace before casting.
// Keying off value_kind rather than the canonical field means a user-added
// field with value_kind=currency gets real cleaning/typing for free, instead
// of silently falling through to VARCHAR passthrough.
func cleanExpr(src string, kind ValueKind) string {
	col := quoteIdent(src)
	switch kind {
	case KindCurrency:
		return fmt.Sprintf(`TRY_CAST(
			CASE WHEN regexp_matches(trim(%[1]s), '^\(.*\)$')
			     THEN '-' || regexp_replace(regexp_replace(trim(%[1]s), '[\(\)\$,%%\s]', '', 'g'), '^-', '', 'g')
			     ELSE regexp_replace(trim(%[1]s), '[\$,%%\s]', '', 'g')
			END AS DECIMAL(12,4))`, col)
	case KindInteger:
		return fmt.Sprintf(`TRY_CAST(regexp_replace(trim(%s), '[,\s]', '', 'g') AS INTEGER)`, col)
	case KindUOM:
		return fmt.Sprintf(`upper(trim(%s))`, col)
	default:
		return fmt.Sprintf(`nullif(trim(%s), '')`, col)
	}
}

// Promote filters structural noise from staging, applies the mapping, cleans
// and types values, and splits output into typed + quarantine tables.
//
// reg supplies which fields are required and what value_kind each mapped
// field cleans/types as. Passing the registry through (rather than reading a
// package-level required-fields list) means a required or typed custom field
// added via the field-types API is honored here exactly like a builtin.
//
// Quarantine criteria: a required field is NULL after cleaning — either it
// was empty in the source or TRY_CAST failed. The reason column says which.
func (l *Loader) Promote(ctx context.Context, staging, typed, quarantine string,
	rep *StructureReport, mapping []FieldMapping, reg *FieldRegistry, res *LoadResult) error {

	res.TypedTable = typed
	res.QuarantineTable = quarantine

	// Build filter for structural noise using the same classifiers as the
	// sampler, expressed in SQL so they apply to the whole file, not just the
	// sampled prefix.
	headerKey := rowKey(rep.RawColumns)
	nonEmptyCount := sqlNonEmptyCount(rep.Columns)
	numericAny := sqlAnyNumeric(rep.Columns)
	rowKeyExpr := sqlRowKey(rep.Columns)

	blankCond := fmt.Sprintf("(%s) = 0", nonEmptyCount)
	sectionCond := fmt.Sprintf("(%s) = 1 AND NOT (%s)", nonEmptyCount, numericAny)
	repeatCond := fmt.Sprintf("(%s) = %s", rowKeyExpr, quoteStr(headerKey))

	// Count what we're about to filter, for the report.
	countQ := fmt.Sprintf(`SELECT
		count(*) FILTER (WHERE %s),
		count(*) FILTER (WHERE %s),
		count(*) FILTER (WHERE %s)
		FROM %s`, blankCond, sectionCond, repeatCond, quoteIdent(staging))
	if err := l.DB.QueryRowContext(ctx, countQ).Scan(
		&res.RowsFiltered.Blank,
		&res.RowsFiltered.SectionHeadings,
		&res.RowsFiltered.RepeatedHeaders,
	); err != nil {
		return fmt.Errorf("noise count: %w", err)
	}

	// Select list: cleaned/typed expression per mapped field.
	//
	// Required-field NULL checks are built in two forms. The bare form is used
	// where only _clean is in scope (the typed table). The c.-qualified form is
	// used in the quarantine query, where staging (s) and _clean (c) are joined
	// and can share a column name — a source header like "Description" and the
	// canonical field "description" collide, so an unqualified reference is
	// ambiguous. Both refer to the cleaned value in _clean.
	var selects, requiredNullConds, requiredNullCondsC, reasonPartsC []string
	for _, m := range mapping {
		if m.Field == FieldUnknown {
			continue
		}
		expr := cleanExpr(m.SourceColumn, reg.ValueKind(m.Field))
		field := quoteIdent(string(m.Field))
		selects = append(selects, fmt.Sprintf("%s AS %s", expr, field))
		if reg.IsRequired(m.Field) {
			requiredNullConds = append(requiredNullConds,
				fmt.Sprintf("%s IS NULL", field))
			requiredNullCondsC = append(requiredNullCondsC,
				fmt.Sprintf("c.%s IS NULL", field))
			reasonPartsC = append(reasonPartsC, fmt.Sprintf(
				"CASE WHEN c.%s IS NULL THEN '%s missing/uncastable; ' ELSE '' END",
				field, string(m.Field)))
		}
	}
	if len(selects) == 0 {
		return fmt.Errorf("mapping contains no canonical fields")
	}

	cleanView := fmt.Sprintf(`
		CREATE OR REPLACE TEMP VIEW _clean AS
		SELECT _row, %s
		FROM %s
		WHERE NOT (%s) AND NOT (%s) AND NOT (%s)`,
		strings.Join(selects, ",\n\t\t\t"),
		quoteIdent(staging), blankCond, sectionCond, repeatCond)
	if _, err := l.DB.ExecContext(ctx, cleanView); err != nil {
		return fmt.Errorf("clean view: %w", err)
	}

	badCond := strings.Join(requiredNullConds, " OR ")
	badCondC := strings.Join(requiredNullCondsC, " OR ")

	if _, err := l.DB.ExecContext(ctx, fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s AS
		SELECT * EXCLUDE (_row) FROM _clean WHERE NOT (%s)`,
		quoteIdent(typed), badCond)); err != nil {
		return fmt.Errorf("typed table: %w", err)
	}

	// Quarantine keeps the ORIGINAL raw row (joined back from staging) plus
	// the reason — you can't debug a cleaned row. References to the cleaned
	// required fields are c.-qualified because staging and _clean share names.
	if _, err := l.DB.ExecContext(ctx, fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s AS
		SELECT s.*, (%s) AS reason
		FROM %s s JOIN _clean c USING (_row)
		WHERE (%s)`,
		quoteIdent(quarantine),
		strings.Join(reasonPartsC, " || "),
		quoteIdent(staging), badCondC)); err != nil {
		return fmt.Errorf("quarantine table: %w", err)
	}

	if err := l.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(typed))).Scan(&res.RowsTyped); err != nil {
		return err
	}
	if err := l.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", quoteIdent(quarantine))).Scan(&res.RowsQuarantined); err != nil {
		return err
	}

	// Per-field null rates on the typed output — the "is this import sane"
	// number. A price column that's 40% NULL passed every parse step and is
	// still wrong.
	res.NullRates = map[string]float64{}
	for _, m := range mapping {
		if m.Field == FieldUnknown {
			continue
		}
		var rate float64
		q := fmt.Sprintf(
			"SELECT coalesce(avg(CASE WHEN %s IS NULL THEN 1.0 ELSE 0 END), 0) FROM %s",
			quoteIdent(string(m.Field)), quoteIdent(typed))
		if err := l.DB.QueryRowContext(ctx, q).Scan(&rate); err != nil {
			return err
		}
		res.NullRates[string(m.Field)] = rate
		if rate > 0.2 && !reg.IsRequired(m.Field) {
			res.Warnings = append(res.Warnings, fmt.Sprintf(
				"field %q is %.0f%% NULL after typing — check mapping", m.Field, rate*100))
		}
	}
	return nil
}

// --- SQL fragment builders ---

func sqlNonEmptyCount(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("CASE WHEN nullif(trim(coalesce(%s,'')),'') IS NULL THEN 0 ELSE 1 END", quoteIdent(c))
	}
	return strings.Join(parts, " + ")
}

func sqlAnyNumeric(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf(
			`regexp_matches(coalesce(%s,''), '^\s*-?\(?\$?\s*\d')`, quoteIdent(c))
	}
	return strings.Join(parts, " OR ")
}

func sqlRowKey(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("lower(trim(coalesce(%s,'')))", quoteIdent(c))
	}
	return fmt.Sprintf("concat_ws(chr(31), %s)", strings.Join(parts, ", "))
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// columnsMap renders the columns={...} argument for read_csv.
func columnsMap(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("'%s': 'VARCHAR'", strings.ReplaceAll(c, "'", "''"))
	}
	return strings.Join(parts, ", ")
}
