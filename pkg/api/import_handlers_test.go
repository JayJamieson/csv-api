package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// messyBook is a supplier price book with the things one-shot read_csv_auto
// chokes on: a title/date preamble, blank rows, section headings, a repeated
// page header, currency formatting with thousands separators, and one row
// missing its (required) price. Header is at row index 3.
const messyBook = `ACME PLUMBING SUPPLIES LTD,,,,
Price Book Effective 1 July 2026,,,,
,,,,
Product Code,Description,Trade Price,RRP,UOM
COPPER FITTINGS,,,,
CU-EL-15,Copper Elbow 15mm,$2.50,$4.10,EA
CU-EL-20,Copper Elbow 20mm,$3.80,$6.20,EA
CU-TE-15,Copper Tee 15mm,$3.10,$5.00,EA
,,,,
BRASS VALVES,,,,
BV-GV-15,Brass Gate Valve 15mm,$12.40,$19.95,EA
BV-BV-20,Brass Ball Valve 20mm,"$1,145.00","$1,800.00",EA
Product Code,Description,Trade Price,RRP,UOM
BV-CV-15,Brass Check Valve 15mm,$8.90,$14.50,EA
BV-XX-99,Mystery Valve,,,
TOTAL ITEMS: 7,,,,
`

// newTestServer boots a real Server against a temp libsql (sqlite) metadata DB
// and a temp per-import DuckDB directory. It chdirs into a temp dir so both the
// metadata file and the ./data DuckDB files land there and are cleaned up.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	s, err := New(Config{Port: 0, DatabaseURL: "file:meta.db"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.db.Close() })
	return s
}

func (s *Server) do(t *testing.T, method, target, contentType string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T: %v (body: %s)", v, err, rec.Body.String())
	}
	return v
}

// TestLoadCommitQuarantineFlow drives the whole two-phase import over HTTP:
// stage a messy file, commit the proposal, read the quarantine (JSON and CSV),
// and confirm /api/{id} serves the clean typed table with numeric prices.
func TestLoadCommitQuarantineFlow(t *testing.T) {
	s := newTestServer(t)

	// --- phase 1: /load ---
	rec := s.do(t, http.MethodPost, "/load?name=book.csv&supplier=ACME", "text/csv",
		strings.NewReader(messyBook))
	if rec.Code != http.StatusOK {
		t.Fatalf("/load: got %d, body %s", rec.Code, rec.Body.String())
	}
	load := decode[LoadResponse](t, rec)
	if load.State != Staged {
		t.Errorf("state: got %q want staged", load.State)
	}
	if load.HeaderIndex != 3 {
		t.Errorf("header_index: got %d want 3", load.HeaderIndex)
	}
	gotFields := map[string]string{}
	for _, m := range load.ProposedMappings {
		gotFields[m.SourceColumn] = m.Field
	}
	for col, want := range map[string]string{
		"product_code": "sku", "description": "description",
		"trade_price": "price", "rrp": "list_price", "uom": "uom",
	} {
		if gotFields[col] != want {
			t.Errorf("mapping %s: got %q want %q", col, gotFields[col], want)
		}
	}
	id := load.ImportId.String()

	// --- phase 2: /commit (accept the proposal) ---
	rec = s.do(t, http.MethodPost, "/imports/"+id+"/commit", "application/json",
		strings.NewReader(`{}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("/commit: got %d, body %s", rec.Code, rec.Body.String())
	}
	commit := decode[CommitResponse](t, rec)
	if commit.RowsTyped != 6 {
		t.Errorf("rows_typed: got %d want 6", commit.RowsTyped)
	}
	if commit.RowsQuarantined != 1 {
		t.Errorf("rows_quarantined: got %d want 1", commit.RowsQuarantined)
	}
	if commit.RowsFilteredSections < 2 || commit.RowsFilteredRepeatedHeaders != 1 {
		t.Errorf("filtered noise: sections=%d repeated=%d",
			commit.RowsFilteredSections, commit.RowsFilteredRepeatedHeaders)
	}

	// --- quarantine (JSON): the one row missing a price, original cells ---
	rec = s.do(t, http.MethodGet, "/imports/"+id+"/quarantine", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("/quarantine: got %d, body %s", rec.Code, rec.Body.String())
	}
	q := decode[QuarantineResponse](t, rec)
	if q.Total != 1 || len(q.Rows) != 1 {
		t.Fatalf("quarantine total: got %d rows %d want 1", q.Total, len(q.Rows))
	}
	if !strings.Contains(q.Rows[0].Reason, "price") {
		t.Errorf("quarantine reason: got %q want contains 'price'", q.Rows[0].Reason)
	}
	if len(q.Rows[0].Cells) == 0 || q.Rows[0].Cells[0] != "BV-XX-99" {
		t.Errorf("quarantine cells: got %v want original BV-XX-99 row", q.Rows[0].Cells)
	}

	// --- quarantine (CSV export) ---
	rec = s.do(t, http.MethodGet, "/imports/"+id+"/quarantine?format=csv", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("/quarantine?format=csv: got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
		t.Errorf("csv content-type: got %q", ct)
	}
	csvBody := rec.Body.String()
	if !strings.Contains(csvBody, "_row") || !strings.Contains(csvBody, "_reason") {
		t.Errorf("csv missing header columns: %q", csvBody)
	}
	if !strings.Contains(csvBody, "BV-XX-99") {
		t.Errorf("csv missing quarantined row: %q", csvBody)
	}

	// --- /api/{id}: the committed typed table, with numeric prices ---
	rec = s.do(t, http.MethodGet, "/api/"+id+"?format=objects", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api: got %d, body %s", rec.Code, rec.Body.String())
	}
	api := decode[struct {
		Total int              `json:"total"`
		Rows  []map[string]any `json:"rows"`
	}](t, rec)
	if api.Total != 6 {
		t.Errorf("/api total: got %d want 6", api.Total)
	}
	// The DECIMAL price must serialize as a JSON number, not the driver's
	// {Width,Scale,Value} struct.
	var checkedPrice bool
	for _, row := range api.Rows {
		if row["sku"] == "BV-BV-20" {
			price, ok := row["price"].(float64)
			if !ok {
				t.Fatalf("price not a number: %T %v", row["price"], row["price"])
			}
			if price != 1145.0 {
				t.Errorf("BV-BV-20 price: got %v want 1145.0", price)
			}
			checkedPrice = true
		}
	}
	if !checkedPrice {
		t.Error("BV-BV-20 not found in /api output")
	}
}

// TestLoadNoHeaderStaysLowConfidence confirms a header-less file is not
// confidently guessed: it comes back low confidence with no header, so the
// caller must choose one (via redetect) rather than getting a wrong table.
func TestLoadNoHeaderStaysLowConfidence(t *testing.T) {
	s := newTestServer(t)
	rec := s.do(t, http.MethodPost, "/load?name=nohdr.csv", "text/csv",
		strings.NewReader("CU-EL-15,2.50\nCU-EL-20,3.80\nCU-TE-15,3.10\n"))
	if rec.Code != http.StatusOK {
		t.Fatalf("/load: got %d, body %s", rec.Code, rec.Body.String())
	}
	load := decode[LoadResponse](t, rec)
	if load.HeaderConfidence != Low {
		t.Errorf("confidence: got %q want low", load.HeaderConfidence)
	}
	if load.HeaderIndex != -1 {
		t.Errorf("header_index: got %d want -1", load.HeaderIndex)
	}
	if load.RowsStaged != 0 {
		t.Errorf("rows_staged: got %d want 0 (no header -> no staging)", load.RowsStaged)
	}
}

// TestRedetectPicksHeader drives the manual-header recovery path: load a
// header-less file (low confidence, nothing staged), then redetect around a
// chosen header row and confirm it re-stages with high confidence.
func TestRedetectPicksHeader(t *testing.T) {
	s := newTestServer(t)
	rec := s.do(t, http.MethodPost, "/load?name=nohdr.csv", "text/csv",
		strings.NewReader("sku,price\nCU-EL-15,2.50\nCU-EL-20,3.80\nCU-TE-15,3.10\n"))
	load := decode[LoadResponse](t, rec)
	id := load.ImportId.String()

	rec = s.do(t, http.MethodPost, "/imports/"+id+"/redetect", "application/json",
		strings.NewReader(`{"header_index":0}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("/redetect: got %d, body %s", rec.Code, rec.Body.String())
	}
	re := decode[LoadResponse](t, rec)
	if re.State != Staged {
		t.Errorf("state: got %q want staged", re.State)
	}
	if re.HeaderIndex != 0 {
		t.Errorf("header_index: got %d want 0", re.HeaderIndex)
	}
	if re.HeaderConfidence != High {
		t.Errorf("confidence: got %q want high (human-chosen header)", re.HeaderConfidence)
	}
	if re.RowsStaged != 3 {
		t.Errorf("rows_staged: got %d want 3", re.RowsStaged)
	}
	if len(re.Columns) != 2 || re.Columns[0] != "sku" || re.Columns[1] != "price" {
		t.Errorf("columns: got %v want [sku price]", re.Columns)
	}
}
