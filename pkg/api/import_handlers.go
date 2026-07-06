package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/JayJamieson/csv-api/pkg/db"
	"github.com/JayJamieson/csv-api/pkg/sniff"
	"github.com/JayJamieson/csv-api/pkg/utils"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/oapi-codegen/runtime/types"
)

// GetQuarantine implements ServerInterface: read rejected rows for a
// committed import so a human can see what failed and why.
func (h *Server) GetQuarantine(ctx echo.Context, id types.UUID, params GetQuarantineParams) error {
	reqCtx := ctx.Request().Context()

	// Limit/Offset are value types (prefer-skip-optional-pointer); an omitted
	// query param arrives as the zero value. Treat limit<=0 as "use default"
	// (QuarantineRows falls back to 100) and clamp a negative offset to 0.
	limit := params.Limit
	offset := max(params.Offset, 0)
	// A CSV export is for fixing-and-re-importing, so dump every reject rather
	// than one page — unless the caller explicitly asked for a page.
	if params.Format == Csv && params.Limit <= 0 {
		limit = quarantineExportCap
	}

	cols, rows, err := h.db.QuarantineRows(reqCtx, id.String(), limit, offset)
	if err != nil {
		return errorResponse(ctx, http.StatusNotFound, "No quarantine",
			"no quarantine table for this import; it may have been dropped or never committed")
	}

	if params.Format == Csv {
		return writeQuarantineCSV(ctx, id.String(), cols, rows)
	}

	resp := QuarantineResponse{
		Ok:      true,
		Columns: cols,
		Total:   int64(len(rows)),
	}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, QuarantineRow{
			Row:    r.Row,
			Cells:  r.Cells,
			Reason: r.Reason,
		})
	}
	return ctx.JSON(http.StatusOK, resp)
}

// quarantineExportCap bounds a CSV export so a pathological import can't stream
// unbounded rows; rejects are a small fraction of any real file, so this is far
// above any realistic count.
const quarantineExportCap = 1_000_000

// writeQuarantineCSV streams the rejected rows as a downloadable CSV: a _row
// column, the original source columns, and a _reason column. The original
// (uncleaned) cells are emitted so a user can fix them and re-import.
func writeQuarantineCSV(ctx echo.Context, importID string, cols []string, rows []db.QuarantineRow) error {
	resp := ctx.Response()
	resp.Header().Set(echo.HeaderContentType, "text/csv; charset=utf-8")
	resp.Header().Set(echo.HeaderContentDisposition,
		fmt.Sprintf(`attachment; filename="quarantine-%s.csv"`, importID))
	resp.WriteHeader(http.StatusOK)

	w := csv.NewWriter(resp)
	header := make([]string, 0, len(cols)+2)
	header = append(header, "_row")
	header = append(header, cols...)
	header = append(header, "_reason")
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range rows {
		rec := make([]string, 0, len(cols)+2)
		rec = append(rec, strconv.FormatInt(r.Row, 10))
		for i := range cols { // pad to column count so rows stay aligned
			if i < len(r.Cells) {
				rec = append(rec, r.Cells[i])
			} else {
				rec = append(rec, "")
			}
		}
		rec = append(rec, r.Reason)
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// LoadCSV implements ServerInterface: phase 1 of the two-phase import.
//
// Flow: resolve source -> normalize encoding to a spool file -> detect
// structure -> stage as all-VARCHAR in the per-import DuckDB -> propose (or
// recall) a mapping -> persist state -> maybe auto-commit.
func (h *Server) LoadCSV(ctx echo.Context, params LoadCSVParams) error {
	reqCtx := ctx.Request().Context()

	var reader io.ReadCloser
	switch {
	case params.Url != "":
		r, err := utils.DownloadFile(params.Url)
		if err != nil {
			return errorResponse(ctx, http.StatusInternalServerError, "URL fetch error", err.Error())
		}
		reader = r
	case params.Name != "":
		reader = ctx.Request().Body
	default:
		return errorResponse(ctx, http.StatusBadRequest, "Missing import parameters",
			"Either 'url' or 'name' parameter must be provided")
	}
	defer reader.Close()

	importID := uuid.NewString()

	// Normalize into the spool. The spool file is the single source of truth
	// between phases: committing re-reads it, never the original URL, so the
	// file the user previewed is the file that gets imported.
	spoolPath, normRes, err := h.spoolNormalized(importID, reader)
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Normalization error", err.Error())
	}

	f, err := os.Open(spoolPath)
	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "Spool read error", err.Error())
	}
	report, err := sniff.DetectStructure(f, nil)
	f.Close()
	if err != nil {
		os.Remove(spoolPath)
		return errorResponse(ctx, http.StatusBadRequest, "Detection error", err.Error())
	}

	resp := newStagedLoadResponse(importID, report)
	resp.SourceEncoding = string(normRes.SourceEncoding)

	// No convincing header: persist state and return candidates. Staging
	// needs column names, so we stop here; commit must supply header_index.
	if report.HeaderIndex < 0 {
		if err := h.persistState(reqCtx, importID, params.Supplier, spoolPath, report, nil, false); err != nil {
			return errorResponse(ctx, http.StatusInternalServerError, "State error", err.Error())
		}
		return ctx.JSON(http.StatusOK, resp)
	}

	// Stage all-VARCHAR into this import's own DuckDB file.
	conn, err := h.db.StagedDB(importID)
	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "DB error", err.Error())
	}
	loader := &sniff.Loader{DB: conn}
	if _, err := loader.LoadStaging(reqCtx, spoolPath, db.StagingTable, report); err != nil {
		os.Remove(spoolPath)
		return errorResponse(ctx, http.StatusInternalServerError, "Staging error", err.Error())
	}

	mappings, savedApplied, err := h.fillStagedMapping(reqCtx, importID, params.Supplier, spoolPath, report, resp)
	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "State error", err.Error())
	}

	// Auto mode: every gate must pass, and each refusal names itself so the
	// client knows what to fix rather than guessing.
	if params.Mode == Auto {
		switch {
		case report.Confidence != sniff.ConfidenceHigh:
			resp.AutoCommitBlockedReason = "header confidence below high"
		case !resp.KnownFingerprint:
			resp.AutoCommitBlockedReason = "no saved mapping for this header set"
		case len(resp.MissingFields) > 0:
			resp.AutoCommitBlockedReason = "required fields unmapped"
		default:
			commitResp, err := h.commitStaged(ctx, importID, params.Supplier,
				spoolPath, report, mappings, false, savedApplied)
			if err != nil {
				resp.AutoCommitBlockedReason = "commit failed: " + err.Error()
				return ctx.JSON(http.StatusOK, resp)
			}
			resp.State = Committed
			resp.Endpoint = commitResp.Endpoint
		}
	}

	return ctx.JSON(http.StatusOK, resp)
}

// RedetectImport implements ServerInterface: re-detect structure around a
// caller-chosen header row and re-stage, returning a fresh LoadResponse so the
// client can review the new proposed mapping before committing. This backs the
// UI's "change header row" action without re-uploading the file — the spool is
// reused, so a URL source that changed underneath can't swap the data.
func (h *Server) RedetectImport(ctx echo.Context, id types.UUID) error {
	reqCtx := ctx.Request().Context()
	importID := id.String()

	var req RedetectRequest
	if err := ctx.Bind(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Invalid request body", err.Error())
	}

	state, err := h.db.LoadImportState(reqCtx, importID)
	if err != nil {
		return errorResponse(ctx, http.StatusNotFound, "Unknown import",
			"no staged import with this id; it may have been committed or expired")
	}

	report, err := h.restage(reqCtx, importID, state.SpoolPath, req.HeaderIndex)
	if err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Restage error", err.Error())
	}

	resp := newStagedLoadResponse(importID, report)
	if _, _, err := h.fillStagedMapping(reqCtx, importID, state.Supplier, state.SpoolPath, report, resp); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "State error", err.Error())
	}
	return ctx.JSON(http.StatusOK, resp)
}

// newStagedLoadResponse builds the base staged response from a detection
// report: the detection facts and header candidates. Staging-dependent fields
// (staged row count, preview, proposed mapping) are filled by fillStagedMapping
// once the data is staged.
func newStagedLoadResponse(importID string, report *sniff.StructureReport) *LoadResponse {
	resp := &LoadResponse{
		Ok:               true,
		ImportId:         mustUUID(importID),
		State:            Staged,
		Delimiter:        string(report.Delimiter),
		HeaderIndex:      report.HeaderIndex,
		HeaderConfidence: LoadResponseHeaderConfidence(report.Confidence),
		Columns:          report.Columns,
		Warnings:         report.Warnings,
	}
	for _, c := range report.Candidates {
		resp.HeaderCandidates = append(resp.HeaderCandidates, HeaderCandidate{
			Index: c.Index, Score: c.Score, Cells: c.Cells,
		})
	}
	return resp
}

// fillStagedMapping fills the staging-dependent fields of resp — staged row
// count, preview, fingerprint, and a recalled-or-proposed mapping — then
// persists import state. Staging for report must already be loaded. Returns the
// chosen mapping and whether it came from a saved mapping. Shared by LoadCSV and
// RedetectImport so both propose identically.
func (h *Server) fillStagedMapping(ctx context.Context, importID, supplier, spoolPath string,
	report *sniff.StructureReport, resp *LoadResponse) ([]sniff.FieldMapping, bool, error) {

	var rowsStaged int64
	if err := h.db.CountStaging(ctx, importID, &rowsStaged); err == nil {
		resp.RowsStaged = rowsStaged
	}
	if preview, err := h.db.PreviewStaging(ctx, importID, 10); err == nil {
		resp.Preview = preview
	}

	// Mapping: recall by fingerprint first, propose otherwise.
	fp := sniff.Fingerprint(report.Columns)
	resp.Fingerprint = fp

	store := &sniff.MappingStore{DB: h.db.Meta()}
	var mappings []sniff.FieldMapping
	savedApplied := false
	if saved, err := store.Get(ctx, fp); err == nil && saved != nil {
		if applied, ok := sniff.ApplySaved(saved, report.Columns); ok {
			mappings = applied
			savedApplied = true
			resp.KnownFingerprint = true
		}
	}
	if mappings == nil {
		samples, _ := h.db.SampleStaging(ctx, importID, 100)
		prop := h.registry.Propose(report.Columns, samples)
		mappings = prop.Mappings
		for _, m := range prop.Missing {
			resp.MissingFields = append(resp.MissingFields, string(m))
		}
		resp.Warnings = append(resp.Warnings, prop.Warnings...)
	}
	for _, m := range mappings {
		resp.ProposedMappings = append(resp.ProposedMappings, toAPIMapping(m))
	}

	if err := h.persistState(ctx, importID, supplier, spoolPath, report, mappings, savedApplied); err != nil {
		return nil, false, err
	}
	return mappings, savedApplied, nil
}

// CommitImport implements ServerInterface: phase 2.
func (h *Server) CommitImport(ctx echo.Context, id types.UUID) error {
	reqCtx := ctx.Request().Context()
	importID := id.String()

	var req CommitRequest
	if err := ctx.Bind(&req); err != nil {
		return errorResponse(ctx, http.StatusBadRequest, "Invalid request body", err.Error())
	}

	state, err := h.db.LoadImportState(reqCtx, importID)
	if err != nil {
		return errorResponse(ctx, http.StatusNotFound, "Unknown import",
			"no staged import with this id; it may have been committed or expired")
	}

	var report sniff.StructureReport
	if err := json.Unmarshal(state.ReportJSON, &report); err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "State corrupt", err.Error())
	}

	// Manual header override: rebuild the report around the chosen row and
	// re-stage. Proposal is invalidated because the columns changed.
	var mappings []sniff.FieldMapping
	if req.HeaderIndex != nil && *req.HeaderIndex != report.HeaderIndex {
		newReport, err := h.restage(reqCtx, importID, state.SpoolPath, *req.HeaderIndex)
		if err != nil {
			return errorResponse(ctx, http.StatusBadRequest, "Restage error", err.Error())
		}
		report = *newReport
		state.ProposalJSON = nil
	}
	if report.HeaderIndex < 0 {
		return errorResponse(ctx, http.StatusBadRequest, "No header selected",
			"detection found no convincing header; supply header_index")
	}

	switch {
	case len(req.Mappings) > 0:
		for _, m := range req.Mappings {
			mappings = append(mappings, fromAPIMapping(m))
		}
	case state.ProposalJSON != nil:
		var prop sniff.MappingProposal
		if err := json.Unmarshal(state.ProposalJSON, &prop); err != nil {
			return errorResponse(ctx, http.StatusInternalServerError, "State corrupt", err.Error())
		}
		mappings = prop.Mappings
	default:
		return errorResponse(ctx, http.StatusBadRequest, "No mapping",
			"no stored proposal (header was overridden?); supply mappings")
	}

	saveMapping := req.SaveMapping == nil || *req.SaveMapping
	resp, err := h.commitStaged(ctx, importID, state.Supplier,
		state.SpoolPath, &report, mappings, saveMapping, state.SavedApplied)
	if err != nil {
		return errorResponse(ctx, http.StatusInternalServerError, "Commit error", err.Error())
	}
	return ctx.JSON(http.StatusOK, *resp)
}

// commitStaged promotes staging -> typed + quarantine, registers the typed
// table with the existing query path, persists learning, and cleans up.
func (h *Server) commitStaged(ctx echo.Context, importID, supplier, spoolPath string,
	report *sniff.StructureReport, mappings []sniff.FieldMapping,
	save, savedApplied bool) (*CommitResponse, error) {

	reqCtx := ctx.Request().Context()

	conn, err := h.db.StagedDB(importID)
	if err != nil {
		return nil, err
	}
	loader := &sniff.Loader{DB: conn}

	res := &sniff.LoadResult{StagingTable: db.StagingTable}
	if err := h.db.CountStaging(reqCtx, importID, &res.RowsRaw); err != nil {
		return nil, fmt.Errorf("staging missing: %w", err)
	}

	if err := loader.Promote(reqCtx, db.StagingTable, db.TypedTable,
		db.QuarantineTable, report, mappings, h.registry, res); err != nil {
		return nil, err
	}

	csvTable, err := h.db.RegisterTable(reqCtx, importID, filepath.Base(spoolPath))
	if err != nil {
		return nil, err
	}

	fp := sniff.Fingerprint(report.Columns)
	store := &sniff.MappingStore{DB: h.db.Meta()}
	if save {
		if err := store.Put(reqCtx, &sniff.SavedMapping{
			Fingerprint: fp,
			Supplier:    supplier,
			Mappings:    mappings,
			HeaderIndex: report.HeaderIndex,
		}); err != nil {
			ctx.Logger().Warnf("mapping save failed: %v", err) // import still succeeded
		}
	}
	if savedApplied {
		_ = store.Touch(reqCtx, fp)
	}

	// Staging is dead weight after promotion; quarantine stays for
	// inspection. Spool and state go too — the import is done.
	_ = h.db.DropStagingArtifacts(reqCtx, importID, false)
	os.Remove(spoolPath)
	_ = h.db.DeleteImportState(reqCtx, importID)

	endpoint := fmt.Sprintf("%s://%s/api/%s", ctx.Scheme(), ctx.Request().Host, csvTable.ID)
	out := &CommitResponse{
		Ok:                          true,
		Endpoint:                    endpoint,
		RowsRaw:                     res.RowsRaw,
		RowsTyped:                   res.RowsTyped,
		RowsQuarantined:             res.RowsQuarantined,
		RowsFilteredBlank:           res.RowsFiltered.Blank,
		RowsFilteredSections:        res.RowsFiltered.SectionHeadings,
		RowsFilteredRepeatedHeaders: res.RowsFiltered.RepeatedHeaders,
		Warnings:                    res.Warnings,
	}
	if len(res.NullRates) > 0 {
		out.NullRates = res.NullRates
	}
	return out, nil
}

// restage rebuilds detection around a user-chosen header and reloads staging.
func (h *Server) restage(ctx context.Context, importID, spoolPath string, headerIdx int) (*sniff.StructureReport, error) {
	f, err := os.Open(spoolPath)
	if err != nil {
		return nil, fmt.Errorf("spool file gone; re-run /load: %w", err)
	}
	defer f.Close()

	report, err := sniff.DetectStructureWithHeader(f, headerIdx)
	if err != nil {
		return nil, err
	}

	conn, err := h.db.StagedDB(importID)
	if err != nil {
		return nil, err
	}
	loader := &sniff.Loader{DB: conn}
	if _, err := loader.LoadStaging(ctx, spoolPath, db.StagingTable, report); err != nil {
		return nil, err
	}
	return report, nil
}

// spoolNormalized writes the normalized (UTF-8/LF) copy of the input to the
// spool directory.
func (h *Server) spoolNormalized(importID string, r io.Reader) (string, *sniff.NormalizeResult, error) {
	spoolDir := filepath.Join(os.TempDir(), "csvapi-spool")
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return "", nil, err
	}
	path := filepath.Join(spoolDir, importID+".csv")
	out, err := os.Create(path)
	if err != nil {
		return "", nil, err
	}
	res, err := sniff.Normalize(out, r)
	closeErr := out.Close()
	if err != nil {
		os.Remove(path)
		return "", nil, err
	}
	if closeErr != nil {
		os.Remove(path)
		return "", nil, closeErr
	}
	return path, res, nil
}

// persistState serializes report + proposal into the Turso-backed state row.
func (h *Server) persistState(ctx context.Context, importID, supplier, spoolPath string,
	report *sniff.StructureReport, mappings []sniff.FieldMapping, savedApplied bool) error {

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	var proposalJSON []byte
	if mappings != nil {
		proposalJSON, err = json.Marshal(&sniff.MappingProposal{
			Fingerprint: sniff.Fingerprint(report.Columns),
			Mappings:    mappings,
		})
		if err != nil {
			return err
		}
	}
	return h.db.SaveImportState(ctx, &db.ImportState{
		ID:           importID,
		Supplier:     supplier,
		SpoolPath:    spoolPath,
		ReportJSON:   reportJSON,
		ProposalJSON: proposalJSON,
		SavedApplied: savedApplied,
	})
}

// --- mapping converters ---

func toAPIMapping(m sniff.FieldMapping) FieldMapping {
	return FieldMapping{
		SourceColumn: m.SourceColumn,
		SourceIndex:  m.SourceIndex,
		Field:        string(m.Field),
		Confidence:   m.Confidence,
		Notes:        m.Notes,
	}
}

func fromAPIMapping(m FieldMapping) sniff.FieldMapping {
	return sniff.FieldMapping{
		SourceColumn: m.SourceColumn,
		SourceIndex:  m.SourceIndex,
		Field:        sniff.CanonicalField(m.Field),
		Confidence:   m.Confidence,
		Notes:        m.Notes,
	}
}

func mustUUID(s string) types.UUID {
	u, _ := uuid.Parse(s)
	return u
}
