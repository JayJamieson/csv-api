package sniff

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// corpusScanCap bounds how much of a file the scanner reads. Detection and
// mapping only need a head sample (sampleRows itself caps at 4MB), so this is
// generous headroom for encoding overhead, not an attempt to read whole
// multi-hundred-MB files into memory. One pathologically large file in a
// multi-GB corpus should not blow the scanner's memory budget.
const corpusScanCap = 16 * 1024 * 1024

// ColumnReport describes one source column's detected mapping (or lack of
// one) for the corpus report.
type ColumnReport struct {
	Raw      string   `json:"raw"`
	Norm     string   `json:"norm"`
	Proposed string   `json:"proposed"` // canonical field, or "" if unmapped
	Samples  []string `json:"samples"`
	// SuggestField/SuggestScore are set only when the column is unmapped by
	// name but its values still clearly match a known field's shape — the
	// "missing synonym" signal.
	SuggestField string  `json:"suggest_field,omitempty"`
	SuggestScore float64 `json:"suggest_score,omitempty"`
}

// FileReport is the per-file scan result, written one-per-line to the corpus
// detail JSONL.
type FileReport struct {
	File        string         `json:"file"`
	Encoding    string         `json:"encoding,omitempty"`
	Delimiter   string         `json:"delimiter,omitempty"`
	HeaderIndex int            `json:"header_index"`
	Confidence  string         `json:"confidence,omitempty"`
	RowsSampled int            `json:"rows_sampled"`
	Columns     []ColumnReport `json:"columns,omitempty"`
	Warnings    []string       `json:"warnings,omitempty"`
	Error       string         `json:"error,omitempty"`

	// Populated only when ScanOptions.Stage is set.
	RowsTyped       int64 `json:"rows_typed,omitempty"`
	RowsQuarantined int64 `json:"rows_quarantined,omitempty"`
}

// ScanOptions tunes ScanFile.
type ScanOptions struct {
	// Registry supplies the field set to propose against. Defaults to
	// DefaultRegistry() if nil, so the scanner runs standalone without a
	// database connection — it's read-only measurement, never a write path.
	Registry *FieldRegistry
	// SampleRows caps how many rows are sampled for detection, same meaning
	// as DetectOptions.SampleRows. Defaults to 200.
	SampleRows int
	// Stage additionally loads the file into an in-memory DuckDB and runs the
	// real Promote path, populating RowsTyped/RowsQuarantined. This is much
	// slower (one DuckDB instance + temp file per file) so it's opt-in; the
	// no-stage path answers "what would this map to" from sampled values
	// alone, which is enough for the aggregate report.
	Stage bool
}

// ScanFile runs the read-only sniff pipeline (normalize -> detect -> propose)
// against one file and reports what it found, including a best-effort
// missing-synonym suggestion for every unmapped column. It never writes to
// Turso or commits anything — this is pure measurement for the corpus report.
//
// Errors reading or decoding the file are reported on FileReport.Error rather
// than returned, so a caller scanning thousands of files can keep going; a
// non-nil error return is reserved for a bad path (couldn't even os.Open).
func ScanFile(path string, opts *ScanOptions) (*FileReport, error) {
	o := ScanOptions{SampleRows: 200}
	if opts != nil {
		if opts.Registry != nil {
			o.Registry = opts.Registry
		}
		if opts.SampleRows > 0 {
			o.SampleRows = opts.SampleRows
		}
		o.Stage = opts.Stage
	}
	reg := o.Registry
	if reg == nil {
		reg = DefaultRegistry()
	}

	rep := &FileReport{File: path, HeaderIndex: -1}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var buf bytes.Buffer
	nres, err := Normalize(&buf, io.LimitReader(f, corpusScanCap))
	if err != nil {
		rep.Error = fmt.Sprintf("normalize: %v", err)
		return rep, nil
	}
	rep.Encoding = string(nres.SourceEncoding)

	delim, rows, err := sampleRows(bytes.NewReader(buf.Bytes()), o.SampleRows)
	if err != nil {
		rep.Error = fmt.Sprintf("sample: %v", err)
		return rep, nil
	}
	if len(rows) == 0 {
		rep.Error = "no rows"
		return rep, nil
	}

	structRep := detectStructureFromRows(delim, rows, defaultDetectOptions())
	rep.Delimiter = string(structRep.Delimiter)
	rep.HeaderIndex = structRep.HeaderIndex
	rep.Confidence = string(structRep.Confidence)
	rep.RowsSampled = structRep.SampledRows
	rep.Warnings = structRep.Warnings

	if structRep.HeaderIndex < 0 {
		return rep, nil // no header found: nothing to propose against
	}

	var dataRows [][]string
	for i, k := range structRep.RowKinds {
		if k == RowData {
			dataRows = append(dataRows, rows[i])
		}
	}
	colValues := transpose(structRep.Columns, dataRows)

	prop := reg.Propose(structRep.Columns, dataRows)
	proposedByCol := make(map[int]CanonicalField, len(prop.Mappings))
	for _, m := range prop.Mappings {
		proposedByCol[m.SourceIndex] = m.Field
	}

	for i, raw := range structRep.RawColumns {
		cr := ColumnReport{
			Raw:     raw,
			Norm:    structRep.Columns[i],
			Samples: sampleNonEmpty(colValues[i], 3),
		}
		if field := proposedByCol[i]; field != FieldUnknown {
			cr.Proposed = string(field)
		} else if best, score := reg.BestValueMatch(colValues[i]); score >= 0.6 {
			cr.SuggestField = string(best)
			cr.SuggestScore = score
		}
		rep.Columns = append(rep.Columns, cr)
	}

	if o.Stage {
		if err := stageAndPromote(rep, structRep, prop.Mappings, reg, buf.Bytes()); err != nil {
			rep.Warnings = append(rep.Warnings, "stage: "+err.Error())
		}
	}

	return rep, nil
}

// stageAndPromote runs the real staging + promote path in a throwaway DuckDB
// so the report can include typed/quarantine row counts. Opt-in
// (ScanOptions.Stage) because it costs a temp file and a DuckDB instance per
// file, which matters at corpus scale.
//
// Each call gets its own temp directory and on-disk DuckDB file, not
// sql.Open("duckdb", "") — an empty-path "in-memory" DuckDB is a shared
// anonymous database, not an isolated one, so concurrent workers opening it
// concurrently collide on the same "staging" table name (a
// write-write-conflict, or worse, a query silently binding to a different
// file's columns because it landed on the wrong goroutine's data). A worker
// pool makes that collision the common case, not a rare race.
func stageAndPromote(rep *FileReport, structRep *StructureReport, mappings []FieldMapping, reg *FieldRegistry, normalized []byte) error {
	dir, err := os.MkdirTemp("", "corpus-scan-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, normalized, 0o600); err != nil {
		return err
	}

	db, err := sql.Open("duckdb", filepath.Join(dir, "scan.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	ctx := context.Background()
	loader := &Loader{DB: db}
	lres, err := loader.LoadStaging(ctx, csvPath, "staging", structRep)
	if err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := loader.Promote(ctx, "staging", "typed", "quarantine", structRep, mappings, reg, lres); err != nil {
		return fmt.Errorf("promote: %w", err)
	}
	rep.RowsTyped = lres.RowsTyped
	rep.RowsQuarantined = lres.RowsQuarantined
	return nil
}
