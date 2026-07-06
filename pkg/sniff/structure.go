package sniff

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Confidence buckets for header detection. The API surfaces these so the
// caller (or a human) can decide whether to trust auto mode.
type Confidence string

const (
	ConfidenceHigh   Confidence = "high"   // clear winner, safe to auto-commit
	ConfidenceMedium Confidence = "medium" // winner exists but a rival is close
	ConfidenceLow    Confidence = "low"    // no convincing header row found
)

// RowKind classifies each sampled row after detection.
type RowKind string

const (
	RowPreamble RowKind = "preamble" // junk above the header (titles, dates)
	RowHeader   RowKind = "header"
	RowData     RowKind = "data"
	RowSection  RowKind = "section" // e.g. "COPPER FITTINGS" spanning one cell
	RowRepeat   RowKind = "repeated_header"
	RowBlank    RowKind = "blank"
)

// StructureReport is the output of DetectStructure: everything the loader
// needs to build a correct read_csv call plus the evidence behind it.
type StructureReport struct {
	Delimiter      rune         `json:"delimiter"`
	HeaderIndex    int          `json:"header_index"` // 0-based row index; -1 if none found
	Confidence     Confidence   `json:"confidence"`
	Columns        []string     `json:"columns"`      // normalized header names
	RawColumns     []string     `json:"raw_columns"`  // as they appear in the file
	ModalFields    int          `json:"modal_fields"` // dominant field count
	SampledRows    int          `json:"sampled_rows"`
	RowKinds       []RowKind    `json:"row_kinds"` // classification per sampled row
	Candidates     []Candidate  `json:"candidates,omitempty"`
	SectionSamples []string     `json:"section_samples,omitempty"`
	Warnings       []string     `json:"warnings,omitempty"`
}

// Candidate is a scored potential header row, surfaced for manual selection.
type Candidate struct {
	Index int      `json:"index"`
	Score float64  `json:"score"`
	Cells []string `json:"cells"`
}

// DetectOptions tunes DetectStructure.
type DetectOptions struct {
	// SampleRows caps how many rows are read for analysis. Header junk lives
	// in the first screenful; 200 is generous.
	SampleRows int
	// MinHeaderScore is the floor below which a row can never be a header.
	MinHeaderScore float64
	// RivalMargin: if the runner-up is within this fraction of the winner's
	// score, confidence drops to medium.
	RivalMargin float64
}

func defaultDetectOptions() DetectOptions {
	return DetectOptions{
		SampleRows:     200,
		MinHeaderScore: 0.45,
		RivalMargin:    0.15,
	}
}

var (
	currencyRe = regexp.MustCompile(`^\s*-?\(?\$?\s*\d{1,3}(,\d{3})*(\.\d+)?\)?\s*%?\s*$`)
	numericRe  = regexp.MustCompile(`^\s*-?\d+(\.\d+)?\s*$`)
	dateRe     = regexp.MustCompile(`^\s*\d{1,4}[-/]\d{1,2}[-/]\d{1,4}\s*$`)
	// A "wordy" cell: letters with optional spaces/punct — what header labels
	// look like. Deliberately permissive; scoring does the discrimination.
	wordyRe = regexp.MustCompile(`^[\p{L}][\p{L}\p{N}\s\.\#\%\$\(\)/&_-]*$`)
)

// DetectStructure samples the head of a normalized (UTF-8, LF) CSV stream and
// works out where the real table starts, what the header is, and which sampled
// rows are noise.
//
// The approach is scoring, not rules-first: every row gets a header-likeness
// score, and every row is separately classified against the winning header.
// This keeps one weird supplier layout from needing its own special case.
func DetectStructure(r io.Reader, opts *DetectOptions) (*StructureReport, error) {
	o := defaultDetectOptions()
	if opts != nil {
		if opts.SampleRows > 0 {
			o.SampleRows = opts.SampleRows
		}
		if opts.MinHeaderScore > 0 {
			o.MinHeaderScore = opts.MinHeaderScore
		}
		if opts.RivalMargin > 0 {
			o.RivalMargin = opts.RivalMargin
		}
	}

	delim, rows, err := sampleRows(r, o.SampleRows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("file contains no rows")
	}

	report := &StructureReport{
		Delimiter:   delim,
		HeaderIndex: -1,
		SampledRows: len(rows),
		ModalFields: modalFieldCount(rows),
	}

	// Score every sampled row for header-likeness.
	scores := make([]float64, len(rows))
	for i, row := range rows {
		scores[i] = headerScore(row, rows, i, report.ModalFields)
	}

	best, second := topTwo(scores)
	if best >= 0 && scores[best] >= o.MinHeaderScore {
		report.HeaderIndex = best
		report.RawColumns = rows[best]
		report.Columns = normalizeHeaders(rows[best])
		switch {
		case second >= 0 && scores[second] >= scores[best]*(1-o.RivalMargin):
			report.Confidence = ConfidenceMedium
		case scores[best] >= 0.7:
			report.Confidence = ConfidenceHigh
		default:
			report.Confidence = ConfidenceMedium
		}
	} else {
		report.Confidence = ConfidenceLow
		report.Warnings = append(report.Warnings,
			"no convincing header row found; manual selection required")
	}

	// Surface top candidates so a human can pick when confidence isn't high.
	for _, idx := range topN(scores, 3) {
		if scores[idx] <= 0 {
			continue
		}
		report.Candidates = append(report.Candidates, Candidate{
			Index: idx, Score: scores[idx], Cells: rows[idx],
		})
	}

	// Classify every sampled row relative to the chosen header.
	report.RowKinds = classifyRows(rows, report)
	for i, k := range report.RowKinds {
		if k == RowSection && len(report.SectionSamples) < 5 {
			report.SectionSamples = append(report.SectionSamples,
				strings.Join(nonEmpty(rows[i]), " "))
		}
	}

	return report, nil
}

// DetectStructureWithHeader builds a StructureReport around a caller-chosen
// header row instead of scoring. This is the manual-override path: detection
// was wrong or low-confidence, a human picked row N from the candidates (or
// the raw preview), and everything downstream should proceed exactly as if
// detection had chosen it — same classification, same normalization.
// Confidence is reported as high because a human decision outranks a score.
func DetectStructureWithHeader(r io.Reader, headerIdx int) (*StructureReport, error) {
	delim, rows, err := sampleRows(r, defaultDetectOptions().SampleRows)
	if err != nil {
		return nil, err
	}
	if headerIdx < 0 || headerIdx >= len(rows) {
		return nil, fmt.Errorf("header index %d out of sampled range (0..%d)",
			headerIdx, len(rows)-1)
	}
	if len(nonEmpty(rows[headerIdx])) < 2 {
		return nil, fmt.Errorf("row %d has fewer than 2 non-empty cells; cannot be a header", headerIdx)
	}

	rep := &StructureReport{
		Delimiter:   delim,
		HeaderIndex: headerIdx,
		Confidence:  ConfidenceHigh,
		RawColumns:  rows[headerIdx],
		Columns:     normalizeHeaders(rows[headerIdx]),
		SampledRows: len(rows),
		ModalFields: modalFieldCount(rows),
	}
	rep.RowKinds = classifyRows(rows, rep)
	return rep, nil
}

// sampleRows detects the delimiter over a raw prefix, then parses up to max
// rows with encoding/csv in lenient mode (FieldsPerRecord=-1, LazyQuotes).
func sampleRows(r io.Reader, max int) (rune, [][]string, error) {
	raw, err := io.ReadAll(io.LimitReader(r, 4*1024*1024))
	if err != nil {
		return 0, nil, err
	}

	delim := detectDelimiter(raw)

	cr := csv.NewReader(strings.NewReader(string(raw)))
	cr.Comma = delim
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true

	var rows [][]string
	for len(rows) < max {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// A malformed row inside the sample shouldn't abort detection;
			// note it by inserting an empty row so indices stay line-aligned.
			rows = append(rows, []string{})
			continue
		}
		rows = append(rows, rec)
	}
	return delim, rows, nil
}

// detectDelimiter picks the candidate whose per-line count is both frequent
// and *consistent* across lines. Consistency matters more than raw frequency:
// a description column full of commas inflates counts but not consistently.
func detectDelimiter(raw []byte) rune {
	candidates := []rune{',', ';', '\t', '|'}
	lines := strings.Split(string(raw), "\n")
	if len(lines) > 50 {
		lines = lines[:50]
	}

	bestDelim := ','
	bestScore := -1.0
	for _, d := range candidates {
		counts := map[int]int{}
		total := 0
		for _, ln := range lines {
			if strings.TrimSpace(ln) == "" {
				continue
			}
			c := strings.Count(ln, string(d))
			if c > 0 {
				counts[c]++
				total++
			}
		}
		if total == 0 {
			continue
		}
		mode := 0
		for _, n := range counts {
			if n > mode {
				mode = n
			}
		}
		// score = how many non-empty lines agree on the modal count
		score := float64(mode) / float64(len(lines))
		if score > bestScore {
			bestScore = score
			bestDelim = d
		}
	}
	return bestDelim
}

// headerScore rates a row's likelihood of being the header. Signals:
//   - field count matches the file's modal field count (headers head the table)
//   - cells are wordy, non-numeric, non-currency, non-date
//   - cells are unique (headers rarely repeat labels)
//   - the rows *below* look like data (numeric/currency mix) — a header is
//     defined as much by what follows it as by its own content
func headerScore(row []string, all [][]string, idx, modal int) float64 {
	cells := nonEmpty(row)
	if len(cells) < 2 {
		return 0 // one-cell rows are titles or section headings, never headers
	}

	var s float64

	// Field-count agreement with the table body.
	if len(row) == modal {
		s += 0.25
	} else if abs(len(row)-modal) == 1 {
		s += 0.10
	}

	// Cell content: wordy and non-data-like.
	wordy, dataLike := 0, 0
	for _, c := range cells {
		c = strings.TrimSpace(c)
		switch {
		case currencyRe.MatchString(c), numericRe.MatchString(c), dateRe.MatchString(c):
			dataLike++
		case wordyRe.MatchString(c):
			wordy++
		}
	}
	s += 0.30 * float64(wordy) / float64(len(cells))
	s -= 0.40 * float64(dataLike) / float64(len(cells))

	// Uniqueness.
	seen := map[string]bool{}
	dup := false
	for _, c := range cells {
		k := strings.ToLower(strings.TrimSpace(c))
		if seen[k] {
			dup = true
		}
		seen[k] = true
	}
	if !dup {
		s += 0.10
	}

	// Evidence from below: do the next few rows look like data?
	looked, dataish := 0, 0
	for j := idx + 1; j < len(all) && looked < 5; j++ {
		c := nonEmpty(all[j])
		if len(c) == 0 {
			continue
		}
		looked++
		if rowLooksLikeData(all[j]) {
			dataish++
		}
	}
	if looked > 0 {
		s += 0.35 * float64(dataish) / float64(looked)
	}

	// A row that is itself data-like (numeric/currency cells) is a poor header
	// candidate no matter how header-like its neighbours make it look. Without
	// this, a header-less file scores every data row as a plausible header —
	// its code-like first cell reads as "wordy" and the rows below it read as
	// "data" — so detection confidently mislabels row 0 as the header. A real
	// header is text with no data cells, so this never dampens a true header;
	// it keeps a genuinely header-less file honestly low-confidence.
	if rowLooksLikeData(row) {
		s *= 0.6
	}

	if s < 0 {
		s = 0
	}
	return s
}

// rowLooksLikeData: at least one numeric/currency cell and >=2 non-empty cells.
func rowLooksLikeData(row []string) bool {
	cells := nonEmpty(row)
	if len(cells) < 2 {
		return false
	}
	for _, c := range cells {
		c = strings.TrimSpace(c)
		if currencyRe.MatchString(c) || numericRe.MatchString(c) || dateRe.MatchString(c) {
			return true
		}
	}
	return false
}

// classifyRows labels each sampled row relative to the detected header.
func classifyRows(rows [][]string, rep *StructureReport) []RowKind {
	kinds := make([]RowKind, len(rows))
	headerKey := rowKey(rep.RawColumns)

	for i, row := range rows {
		cells := nonEmpty(row)
		switch {
		case len(cells) == 0:
			kinds[i] = RowBlank
		case rep.HeaderIndex >= 0 && i < rep.HeaderIndex:
			kinds[i] = RowPreamble
		case i == rep.HeaderIndex:
			kinds[i] = RowHeader
		case headerKey != "" && rowKey(row) == headerKey:
			// The same header repeating mid-file — paginated PDF-to-CSV
			// exports do this on every page break.
			kinds[i] = RowRepeat
		case len(cells) == 1 && !rowLooksLikeData(row):
			kinds[i] = RowSection
		default:
			kinds[i] = RowData
		}
	}
	return kinds
}

// normalizeHeaders produces stable snake_case column names and disambiguates
// duplicates ("price", "price_2"). Empty headers become col_N.
func normalizeHeaders(raw []string) []string {
	out := make([]string, len(raw))
	used := map[string]int{}
	for i, h := range raw {
		n := normalizeName(h)
		if n == "" {
			n = "col_" + itoa(i+1)
		}
		if c := used[n]; c > 0 {
			used[n] = c + 1
			n = n + "_" + itoa(c+1)
		} else {
			used[n] = 1
		}
		out[i] = n
	}
	return out
}

var nonAlnumRe = regexp.MustCompile(`[^a-z0-9]+`)

func normalizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "#", " number ")
	s = strings.ReplaceAll(s, "%", " percent ")
	s = nonAlnumRe.ReplaceAllString(s, "_")
	return strings.Trim(s, "_")
}

// --- small helpers ---

func nonEmpty(row []string) []string {
	var out []string
	for _, c := range row {
		if strings.TrimSpace(c) != "" {
			out = append(out, c)
		}
	}
	return out
}

func rowKey(row []string) string {
	if len(row) == 0 {
		return ""
	}
	parts := make([]string, 0, len(row))
	for _, c := range row {
		parts = append(parts, strings.ToLower(strings.TrimSpace(c)))
	}
	return strings.Join(parts, "\x1f")
}

func modalFieldCount(rows [][]string) int {
	counts := map[int]int{}
	for _, r := range rows {
		if len(nonEmpty(r)) > 0 {
			counts[len(r)]++
		}
	}
	mode, best := 0, 0
	for k, v := range counts {
		if v > best || (v == best && k > mode) {
			mode, best = k, v
		}
	}
	return mode
}

func topTwo(scores []float64) (int, int) {
	best, second := -1, -1
	for i, s := range scores {
		if best == -1 || s > scores[best] {
			second = best
			best = i
		} else if second == -1 || s > scores[second] {
			second = i
		}
	}
	return best, second
}

func topN(scores []float64, n int) []int {
	idx := make([]int, len(scores))
	for i := range idx {
		idx[i] = i
	}
	// insertion sort by score desc; sample sizes are tiny
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && scores[idx[j]] > scores[idx[j-1]]; j-- {
			idx[j], idx[j-1] = idx[j-1], idx[j]
		}
	}
	if len(idx) > n {
		idx = idx[:n]
	}
	return idx
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
