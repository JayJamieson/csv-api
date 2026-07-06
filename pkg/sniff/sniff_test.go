package sniff

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// --- encoding ---

func TestDetectEncoding(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want Encoding
	}{
		{"plain utf8", []byte("sku,price\nA1,2.50\n"), EncodingUTF8},
		{"utf8 bom", append([]byte{0xEF, 0xBB, 0xBF}, []byte("sku,price\n")...), EncodingUTF8BOM},
		{"utf16le bom", []byte{0xFF, 0xFE, 's', 0, 'k', 0, 'u', 0}, EncodingUTF16LE},
		{"utf16le no bom", utf16le("sku,description,price\nA1,Copper Elbow,2.50\n"), EncodingUTF16LE},
		{"cp1252 degree sign", []byte("sku,desc\nA1,90\xb0 Elbow\n"), EncodingCP1252},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DetectEncoding(c.in); got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	// CP1252 input with CRLF, a stray NUL, and a degree sign (0xB0).
	in := []byte("sku,desc\r\nA1,90\xb0 Elbow\x00\r\nA2,Tee\r")
	var out bytes.Buffer
	res, err := Normalize(&out, bytes.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if res.SourceEncoding != EncodingCP1252 {
		t.Errorf("encoding: got %q", res.SourceEncoding)
	}
	if res.NullsStripped != 1 {
		t.Errorf("nulls: got %d want 1", res.NullsStripped)
	}
	if !res.CRLFNormalized {
		t.Error("expected CRLF normalization")
	}
	if strings.Contains(got, "\r") || strings.Contains(got, "\x00") {
		t.Errorf("output still dirty: %q", got)
	}
	if !strings.Contains(got, "90° Elbow") {
		t.Errorf("cp1252 not decoded: %q", got)
	}
	if !strings.HasSuffix(got, "A2,Tee\n") {
		t.Errorf("trailing bare CR not normalized: %q", got)
	}
}

func utf16le(s string) []byte {
	out := make([]byte, 0, len(s)*2)
	for _, b := range []byte(s) {
		out = append(out, b, 0)
	}
	return out
}

// --- structure ---

// cursedPriceBook mimics a real supplier export: title preamble, blank rows,
// units row under the header, section headings, repeated page headers, a
// totals footer, currency formatting, and one ragged row.
const cursedPriceBook = `ACME PLUMBING SUPPLIES LTD,,,,
Price Book Effective 1 July 2026,,,,
,,,,
Product Code,Description,Trade Price,RRP,UOM
COPPER FITTINGS,,,,
CU-EL-15,Copper Elbow 15mm 90deg,"$2.50","$4.10",EA
CU-EL-20,Copper Elbow 20mm 90deg,"$3.80","$6.20",EA
CU-TE-15,Copper Tee 15mm,"$3.10","$5.00",EA
,,,,
BRASS VALVES,,,,
BV-GV-15,Brass Gate Valve 15mm,"$12.40","$19.95",EA
BV-BV-20,Brass Ball Valve 20mm,"$1,145.00","$1,800.00",EA
Product Code,Description,Trade Price,RRP,UOM
BV-CV-15,Brass Check Valve 15mm,"$8.90","$14.50",EA
BV-XX-99,Mystery Valve,,, 
TOTAL ITEMS: 7,,,,
`

func TestDetectStructure(t *testing.T) {
	rep, err := DetectStructure(strings.NewReader(cursedPriceBook), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.HeaderIndex != 3 {
		t.Fatalf("header index: got %d want 3 (candidates: %+v)", rep.HeaderIndex, rep.Candidates)
	}
	if rep.Confidence == ConfidenceLow {
		t.Errorf("confidence: got low, want medium/high")
	}
	wantCols := []string{"product_code", "description", "trade_price", "rrp", "uom"}
	for i, w := range wantCols {
		if rep.Columns[i] != w {
			t.Errorf("col %d: got %q want %q", i, rep.Columns[i], w)
		}
	}

	kindAt := func(i int) RowKind { return rep.RowKinds[i] }
	if kindAt(0) != RowPreamble || kindAt(1) != RowPreamble {
		t.Errorf("rows 0-1 should be preamble: %v %v", kindAt(0), kindAt(1))
	}
	if kindAt(2) != RowBlank {
		t.Errorf("row 2 should be blank: %v", kindAt(2))
	}
	if kindAt(4) != RowSection {
		t.Errorf("row 4 (COPPER FITTINGS) should be section: %v", kindAt(4))
	}
	if kindAt(12) != RowRepeat {
		t.Errorf("row 12 should be repeated header: %v", kindAt(12))
	}
	if kindAt(5) != RowData || kindAt(11) != RowData {
		t.Errorf("data rows misclassified: %v %v", kindAt(5), kindAt(11))
	}
}

func TestDetectStructureNoHeader(t *testing.T) {
	// Pure data, no header at all — must come back low confidence, not a
	// confident wrong guess.
	in := "CU-EL-15,2.50\nCU-EL-20,3.80\nCU-TE-15,3.10\n"
	rep, err := DetectStructure(strings.NewReader(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Confidence != ConfidenceLow {
		t.Errorf("got %q want low (header index %d)", rep.Confidence, rep.HeaderIndex)
	}
}

func TestDetectDelimiterSemicolon(t *testing.T) {
	in := "code;desc, long;price\nA1;elbow, copper;2,50\nA2;tee, brass;3,10\n"
	rep, err := DetectStructure(strings.NewReader(in), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Delimiter != ';' {
		t.Errorf("delimiter: got %q want ';'", rep.Delimiter)
	}
}

// --- mapping ---

func TestProposeMapping(t *testing.T) {
	cols := []string{"product_code", "description", "trade_price", "rrp", "uom"}
	samples := [][]string{
		{"CU-EL-15", "Copper Elbow 15mm 90deg", "$2.50", "$4.10", "EA"},
		{"CU-EL-20", "Copper Elbow 20mm 90deg", "$3.80", "$6.20", "EA"},
		{"BV-GV-15", "Brass Gate Valve 15mm", "$12.40", "$19.95", "EA"},
		{"BV-BV-20", "Brass Ball Valve 20mm", "$1,145.00", "$1,800.00", "EA"},
	}
	p := ProposeMapping(cols, samples)

	want := map[string]CanonicalField{
		"product_code": FieldSKU,
		"description":  FieldDescription,
		"trade_price":  FieldPrice,
		"rrp":          FieldListPrice,
		"uom":          FieldUOM,
	}
	got := map[string]CanonicalField{}
	for _, m := range p.Mappings {
		got[m.SourceColumn] = m.Field
	}
	for col, f := range want {
		if got[col] != f {
			t.Errorf("%s: got %q want %q", col, got[col], f)
		}
	}
	if len(p.Missing) != 0 {
		t.Errorf("missing fields: %v", p.Missing)
	}
}

func TestProposeMappingValuesBeatBadHeaders(t *testing.T) {
	// Headers are garbage (Column1..) — values must carry the mapping.
	cols := []string{"column1", "column2", "column3"}
	samples := [][]string{
		{"CU-EL-15", "Copper Elbow 15mm with joint", "$2.50"},
		{"CU-EL-20", "Copper Elbow 20mm with joint", "$3.80"},
		{"BV-GV-15", "Brass Gate Valve 15mm heavy", "$12.40"},
		{"BV-CV-15", "Brass Check Valve 15mm std", "$8.90"},
	}
	p := ProposeMapping(cols, samples)
	got := map[string]CanonicalField{}
	for _, m := range p.Mappings {
		got[m.SourceColumn] = m.Field
	}
	if got["column1"] != FieldSKU {
		t.Errorf("column1: got %q want sku", got["column1"])
	}
	if got["column3"] != FieldPrice && got["column3"] != FieldListPrice {
		t.Errorf("column3: got %q want a price field", got["column3"])
	}
}

func TestFingerprintOrderIndependent(t *testing.T) {
	a := Fingerprint([]string{"sku", "price", "uom"})
	b := Fingerprint([]string{"uom", "sku", "price"})
	if a != b {
		t.Error("fingerprint should be order-independent")
	}
	c := Fingerprint([]string{"sku", "price"})
	if a == c {
		t.Error("different header sets must differ")
	}
}

// --- end to end against real DuckDB ---

func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()

	// Write the cursed file as CP1252 with CRLF to exercise normalization too.
	raw := strings.ReplaceAll(cursedPriceBook, "\n", "\r\n")
	raw = strings.ReplaceAll(raw, "90deg", "90\xb0")
	src := filepath.Join(dir, "raw.csv")
	if err := os.WriteFile(src, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}

	// 1. Normalize
	norm := filepath.Join(dir, "norm.csv")
	out, err := os.Create(norm)
	if err != nil {
		t.Fatal(err)
	}
	in, _ := os.Open(src)
	nres, err := Normalize(out, in)
	in.Close()
	out.Close()
	if err != nil {
		t.Fatal(err)
	}
	if nres.SourceEncoding != EncodingCP1252 {
		t.Fatalf("encoding: %q", nres.SourceEncoding)
	}

	// 2. Detect
	f, _ := os.Open(norm)
	rep, err := DetectStructure(f, nil)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if rep.HeaderIndex != 3 {
		t.Fatalf("header index %d", rep.HeaderIndex)
	}

	// 3. Propose mapping from sampled data rows
	var dataRows [][]string
	// re-sample for mapping input
	f, _ = os.Open(norm)
	_, rows, _ := sampleRows(f, 200)
	f.Close()
	for i, r := range rows {
		if i < len(rep.RowKinds) && rep.RowKinds[i] == RowData {
			dataRows = append(dataRows, r)
		}
	}
	prop := ProposeMapping(rep.Columns, dataRows)
	if len(prop.Missing) != 0 {
		t.Fatalf("missing: %v", prop.Missing)
	}

	// 4. Load + promote in DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	l := &Loader{DB: db}
	res, err := l.LoadStaging(ctx, norm, "staging", rep)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Promote(ctx, "staging", "typed", "quarantine", rep, prop.Mappings, res); err != nil {
		t.Fatal(err)
	}

	// 7 real products, minus the one with no price (quarantined) = 6 typed.
	if res.RowsTyped != 6 {
		t.Errorf("typed: got %d want 6 (raw %d, quarantined %d, filtered %+v)",
			res.RowsTyped, res.RowsRaw, res.RowsQuarantined, res.RowsFiltered)
	}
	if res.RowsQuarantined != 1 {
		t.Errorf("quarantined: got %d want 1", res.RowsQuarantined)
	}
	if res.RowsFiltered.SectionHeadings < 2 {
		t.Errorf("sections filtered: got %d want >=2", res.RowsFiltered.SectionHeadings)
	}
	if res.RowsFiltered.RepeatedHeaders != 1 {
		t.Errorf("repeated headers: got %d want 1", res.RowsFiltered.RepeatedHeaders)
	}

	// Spot-check the $1,145.00 currency clean. price is DECIMAL(12,4); cast to
	// DOUBLE so the driver's decimal type reads cleanly into a float64.
	var price float64
	err = db.QueryRowContext(ctx,
		`SELECT CAST(price AS DOUBLE) FROM typed WHERE sku = 'BV-BV-20'`).Scan(&price)
	if err != nil {
		t.Fatal(err)
	}
	if price != 1145.0 {
		t.Errorf("currency cleaning: got %v want 1145.0", price)
	}

	// Quarantine row must carry a reason.
	var reason string
	err = db.QueryRowContext(ctx,
		`SELECT reason FROM quarantine LIMIT 1`).Scan(&reason)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, "price") {
		t.Errorf("quarantine reason: %q", reason)
	}
}
