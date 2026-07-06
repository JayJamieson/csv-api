package sniff

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// CanonicalField is a target field in the price-book domain model. Its valid
// set is no longer compiled in: a FieldRegistry holds whichever fields are
// known at runtime, builtin or user-added.
type CanonicalField string

const (
	FieldSKU         CanonicalField = "sku"
	FieldDescription CanonicalField = "description"
	FieldPrice       CanonicalField = "price"      // primary trade/cost price
	FieldListPrice   CanonicalField = "list_price" // RRP / list
	FieldUOM         CanonicalField = "uom"
	FieldBarcode     CanonicalField = "barcode"
	FieldCategory    CanonicalField = "category"
	FieldBrand       CanonicalField = "brand"
	FieldDiscount    CanonicalField = "discount"
	FieldQtyBreak    CanonicalField = "qty_break"
	FieldUnknown     CanonicalField = ""
)

// ValueKind names a value-shape classifier. Fields declare which kind they
// are instead of each carrying its own bespoke pattern-matching code, so a
// new field (e.g. a supplier-specific "pack_qty") just picks an existing kind
// and inherits its classifier for free.
type ValueKind string

const (
	KindText     ValueKind = "text"     // no reliable value signal; name-only
	KindCurrency ValueKind = "currency" // $1,234.50, (12.00), 5%
	KindInteger  ValueKind = "integer"  // plain numeric counts
	KindBarcode  ValueKind = "barcode"  // EAN/UPC/GTIN digit runs
	KindUOM      ValueKind = "uom"      // unit-of-measure vocabulary
	KindDate     ValueKind = "date"     // dd/mm/yyyy-ish
	KindCode     ValueKind = "code"     // near-unique alnum product/SKU codes
	KindFreetext ValueKind = "freetext" // long wordy text (descriptions)
)

func validValueKind(k ValueKind) bool {
	switch k {
	case KindText, KindCurrency, KindInteger, KindBarcode, KindUOM, KindDate, KindCode, KindFreetext:
		return true
	default:
		return false
	}
}

// FieldMapping proposes a canonical field for one source column.
type FieldMapping struct {
	SourceColumn string         `json:"source_column"` // normalized header name
	SourceIndex  int            `json:"source_index"`
	Field        CanonicalField `json:"field"`
	Confidence   float64        `json:"confidence"` // 0..1
	NameScore    float64        `json:"name_score"`
	ValueScore   float64        `json:"value_score"`
	Notes        string         `json:"notes,omitempty"`
}

// MappingProposal is the full suggestion for an import.
type MappingProposal struct {
	Fingerprint string           `json:"fingerprint"`
	Mappings    []FieldMapping   `json:"mappings"`
	Missing     []CanonicalField `json:"missing"` // required fields with no candidate
	Warnings    []string         `json:"warnings,omitempty"`
}

// FieldDef is one canonical field known to a FieldRegistry: builtin or
// user-added, they are handled identically everywhere downstream.
type FieldDef struct {
	Key        CanonicalField `json:"key"`
	Label      string         `json:"label"`
	ValueKind  ValueKind      `json:"value_kind"`
	Required   bool           `json:"required"`
	ClaimOrder int            `json:"claim_order"`
	Builtin    bool           `json:"builtin"`
}

// FieldRegistry holds the set of canonical fields, their synonym lists, and
// (indirectly, via ValueKind) their value classifiers. ProposeMapping used to
// read package-level vars; those are now just the seed for DefaultRegistry,
// and every proposal goes through a registry so a DB-backed one (loaded with
// user-added fields and taught synonyms) behaves identically to the builtin
// set from the caller's point of view.
// A registry is shared across concurrent HTTP handlers and, unlike the
// package-level vars it replaces, is mutable at runtime (AddField/AddSynonym
// via the /field-types API) — so every access goes through mu.
type FieldRegistry struct {
	mu       sync.RWMutex
	fields   []FieldDef
	synonyms map[CanonicalField][]string
}

// DefaultRegistry returns a registry seeded with the compiled-in AU/NZ
// plumbing/electrical/building-supplier field set. It exists so callers that
// don't have (or don't need) a DB-backed registry — unit tests, the corpus
// scanner run without a database — get the exact behavior the hardcoded
// version used to provide.
func DefaultRegistry() *FieldRegistry {
	r := &FieldRegistry{synonyms: map[CanonicalField][]string{}}
	for _, d := range builtinFields {
		r.fields = append(r.fields, d)
	}
	for f, syns := range builtinSynonyms {
		r.synonyms[f] = append([]string(nil), syns...)
	}
	return r
}

// builtinFields is the compiled-in field set and claim order. Lower
// ClaimOrder claims a column first; more specific/higher-signal fields go
// first so e.g. list_price wins "rrp" before the generic price pattern sees
// it. New fields added at runtime default to ClaimOrder 100 (tied with
// description, last), so builtins always get first refusal on a column.
var builtinFields = []FieldDef{
	{Key: FieldBarcode, Label: "Barcode / GTIN", ValueKind: KindBarcode, Required: false, ClaimOrder: 10, Builtin: true},
	{Key: FieldSKU, Label: "SKU / product code", ValueKind: KindCode, Required: true, ClaimOrder: 20, Builtin: true},
	{Key: FieldListPrice, Label: "List price / RRP", ValueKind: KindCurrency, Required: false, ClaimOrder: 30, Builtin: true},
	{Key: FieldPrice, Label: "Price", ValueKind: KindCurrency, Required: true, ClaimOrder: 40, Builtin: true},
	{Key: FieldUOM, Label: "Unit of measure", ValueKind: KindUOM, Required: false, ClaimOrder: 50, Builtin: true},
	{Key: FieldDiscount, Label: "Discount", ValueKind: KindCurrency, Required: false, ClaimOrder: 60, Builtin: true},
	{Key: FieldQtyBreak, Label: "Qty break", ValueKind: KindInteger, Required: false, ClaimOrder: 70, Builtin: true},
	{Key: FieldCategory, Label: "Category", ValueKind: KindText, Required: false, ClaimOrder: 80, Builtin: true},
	{Key: FieldBrand, Label: "Brand", ValueKind: KindText, Required: false, ClaimOrder: 90, Builtin: true},
	{Key: FieldDescription, Label: "Description", ValueKind: KindFreetext, Required: true, ClaimOrder: 100, Builtin: true},
}

// defaultClaimOrder is what a newly-added field gets when it doesn't specify
// one: last, so builtins always claim ambiguous columns first.
const defaultClaimOrder = 100

// builtinSynonyms maps canonical fields to normalized header-name fragments
// seen in AU/NZ plumbing, electrical, and building supplier price books.
// Matching is substring-based over the normalized (snake_case) header, so
// "trade_price_ex_gst" hits both "trade" and "price".
var builtinSynonyms = map[CanonicalField][]string{
	FieldSKU: {
		"sku", "product_code", "prod_code", "item_code", "item_no",
		"item_number", "part_number", "part_no", "stock_code", "stockcode",
		"code", "catalogue_number", "cat_no", "supplier_code", "mpn",
	},
	FieldDescription: {
		"description", "desc", "product_name", "item_description",
		"product_description", "name", "details", "title",
	},
	FieldPrice: {
		"trade_price", "cost_price", "cost", "buy_price", "nett", "net_price",
		"price_ex_gst", "ex_gst", "unit_price", "price", "trade", "wholesale",
	},
	FieldListPrice: {
		"list_price", "rrp", "retail", "list", "recommended", "rec_retail",
		"price_inc_gst", "inc_gst",
	},
	FieldUOM: {
		"uom", "unit_of_measure", "unit", "sell_unit", "pack_unit", "per",
		"sold_by", "measure",
	},
	FieldBarcode: {
		"barcode", "ean", "gtin", "upc", "apn",
	},
	FieldCategory: {
		"category", "group", "product_group", "class", "dept", "department",
		"range", "section",
	},
	FieldBrand: {
		"brand", "manufacturer", "make", "vendor", "supplier",
	},
	FieldDiscount: {
		"discount", "disc", "rebate", "multiplier",
	},
	FieldQtyBreak: {
		"qty_break", "quantity_break", "min_qty", "moq", "pack_qty",
		"carton_qty", "break",
	},
}

var (
	skuValueRe     = regexp.MustCompile(`^[A-Z0-9][A-Z0-9\-\./]{2,24}$`)
	barcodeValueRe = regexp.MustCompile(`^\d{8}$|^\d{12,14}$`)
	uomVocab       = map[string]bool{
		"ea": true, "each": true, "m": true, "lm": true, "mtr": true,
		"metre": true, "meter": true, "bx": true, "box": true, "pk": true,
		"pack": true, "roll": true, "rl": true, "ctn": true, "carton": true,
		"pr": true, "pair": true, "set": true, "kg": true, "l": true,
		"lt": true, "ltr": true, "bag": true, "sht": true, "sheet": true,
		"len": true, "length": true, "100": true, "c": true, "unit": true,
	}
)

// Fields returns the registry's field definitions ordered by ClaimOrder (the
// order in which they get first refusal on a column).
func (r *FieldRegistry) Fields() []FieldDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fieldsLocked()
}

// fieldsLocked is Fields' body, callable from methods that already hold r.mu
// (RWMutex read-locks don't nest safely against a pending writer, so internal
// callers must not go through the public, locking methods).
func (r *FieldRegistry) fieldsLocked() []FieldDef {
	out := append([]FieldDef(nil), r.fields...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ClaimOrder != out[j].ClaimOrder {
			return out[i].ClaimOrder < out[j].ClaimOrder
		}
		return out[i].Key < out[j].Key // deterministic tiebreak
	})
	return out
}

// Get returns the definition for a field key, if known.
func (r *FieldRegistry) Get(key CanonicalField) (FieldDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLocked(key)
}

func (r *FieldRegistry) getLocked(key CanonicalField) (FieldDef, bool) {
	for _, f := range r.fields {
		if f.Key == key {
			return f, true
		}
	}
	return FieldDef{}, false
}

// IsRequired reports whether a field must be mapped (and non-NULL after
// cleaning) before a commit succeeds.
func (r *FieldRegistry) IsRequired(key CanonicalField) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.getLocked(key)
	return ok && f.Required
}

// ValueKind returns the value classifier kind for a field, defaulting to
// KindText (VARCHAR passthrough, no value signal) for an unknown field so
// callers never have to special-case a lookup miss.
func (r *FieldRegistry) ValueKind(key CanonicalField) ValueKind {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if f, ok := r.getLocked(key); ok {
		return f.ValueKind
	}
	return KindText
}

// RequiredFields returns every field currently marked required.
func (r *FieldRegistry) RequiredFields() []CanonicalField {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []CanonicalField
	for _, f := range r.fieldsLocked() {
		if f.Required {
			out = append(out, f.Key)
		}
	}
	return out
}

// Synonyms returns the taught header-name fragments for a field.
func (r *FieldRegistry) Synonyms(key CanonicalField) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.synonyms[key]...)
}

// AddField defines a new canonical field at runtime. Returns an error if the
// key already exists or the value kind is unrecognized. A field type that's
// "value_kind=text" is a generic passthrough — it types as VARCHAR and never
// fails a cast, which is what "no sensible option exists" needed: mapping a
// column to a text field keeps it, unmapped drops it, nothing forces a choice
// that doesn't fit.
func (r *FieldRegistry) AddField(def FieldDef) error {
	if def.Key == FieldUnknown {
		return fmt.Errorf("field key cannot be empty")
	}
	if !validValueKind(def.ValueKind) {
		return fmt.Errorf("unknown value_kind %q", def.ValueKind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.getLocked(def.Key); exists {
		return fmt.Errorf("field %q already exists", def.Key)
	}
	if def.ClaimOrder == 0 {
		def.ClaimOrder = defaultClaimOrder
	}
	r.fields = append(r.fields, def)
	return nil
}

// AddSynonym teaches a new header-name fragment for an existing field.
// Idempotent: teaching the same synonym twice is not an error.
func (r *FieldRegistry) AddSynonym(key CanonicalField, synonym string) error {
	synonym = strings.ToLower(strings.TrimSpace(synonym))
	if synonym == "" {
		return fmt.Errorf("synonym cannot be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.getLocked(key); !exists {
		return fmt.Errorf("unknown field %q", key)
	}
	for _, s := range r.synonyms[key] {
		if s == synonym {
			return nil
		}
	}
	r.synonyms[key] = append(r.synonyms[key], synonym)
	return nil
}

// BestValueMatch scores a column's sampled values against every field's value
// classifier (skipping KindText fields, which have no value signal) and
// returns the best-scoring field. Used by the corpus scanner to suggest a
// missing synonym: a column unmapped by name whose values still clearly match
// a known field's shape.
func (r *FieldRegistry) BestValueMatch(values []string) (CanonicalField, float64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best CanonicalField
	bestScore := 0.0
	for _, f := range r.fieldsLocked() {
		if f.ValueKind == KindText {
			continue
		}
		vs := valueScoreForKind(f.ValueKind, values)
		if vs > bestScore {
			bestScore = vs
			best = f.Key
		}
	}
	return best, bestScore
}

// ProposeMapping scores every (column, field) pair using header-name evidence
// and value-distribution evidence, then assigns greedily in claim order,
// against the compiled-in default field set. It exists so call sites that
// don't need a DB-backed registry (unit tests, one-off tooling) keep working
// unchanged; anything wired to persisted field types should call
// DefaultRegistry().Propose or a loaded registry's Propose directly.
func ProposeMapping(columns []string, samples [][]string) *MappingProposal {
	return DefaultRegistry().Propose(columns, samples)
}

// Propose is the registry-aware form of ProposeMapping: every candidate field
// (builtin or user-added) and every taught synonym participates in scoring.
//
// Name and value evidence are combined 50/50 when both exist. Value evidence
// alone can carry a mapping (mislabeled or blank headers are common); name
// evidence alone is capped at medium confidence because supplier headers lie.
func (r *FieldRegistry) Propose(columns []string, samples [][]string) *MappingProposal {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p := &MappingProposal{Fingerprint: Fingerprint(columns)}

	colValues := transpose(columns, samples)
	fields := r.fieldsLocked()

	type cand struct {
		col       int
		name, val float64
	}
	// score matrix
	scores := map[CanonicalField][]cand{}
	for _, f := range fields {
		syns := r.synonyms[f.Key]
		for i, col := range columns {
			ns := nameScore(col, syns)
			vs := valueScoreForKind(f.ValueKind, colValues[i])
			if ns > 0 || vs > 0 {
				scores[f.Key] = append(scores[f.Key], cand{i, ns, vs})
			}
		}
	}

	claimed := map[int]bool{}
	byField := map[CanonicalField]*FieldMapping{}

	for _, f := range fields {
		cands := scores[f.Key]
		sort.Slice(cands, func(a, b int) bool {
			return combined(cands[a].name, cands[a].val) >
				combined(cands[b].name, cands[b].val)
		})
		for _, c := range cands {
			if claimed[c.col] {
				continue
			}
			conf := combined(c.name, c.val)
			if conf < 0.30 {
				break
			}
			claimed[c.col] = true
			fm := &FieldMapping{
				SourceColumn: columns[c.col],
				SourceIndex:  c.col,
				Field:        f.Key,
				Confidence:   conf,
				NameScore:    c.name,
				ValueScore:   c.val,
			}
			if c.name > 0 && c.val == 0 {
				fm.Notes = "matched on header name only; values inconclusive"
			}
			if c.name == 0 && c.val > 0 {
				fm.Notes = "matched on values only; header name unrecognized"
			}
			byField[f.Key] = fm
			break
		}
	}

	for i, col := range columns {
		if !claimed[i] {
			p.Mappings = append(p.Mappings, FieldMapping{
				SourceColumn: col, SourceIndex: i, Field: FieldUnknown,
			})
		}
	}
	for _, f := range fields {
		if m, ok := byField[f.Key]; ok {
			p.Mappings = append(p.Mappings, *m)
		}
	}
	sort.Slice(p.Mappings, func(a, b int) bool {
		return p.Mappings[a].SourceIndex < p.Mappings[b].SourceIndex
	})

	for _, f := range fields {
		if !f.Required {
			continue
		}
		if _, ok := byField[f.Key]; !ok {
			p.Missing = append(p.Missing, f.Key)
		}
	}
	if len(p.Missing) > 0 {
		p.Warnings = append(p.Warnings,
			"required fields unmapped: manual mapping needed before commit")
	}
	return p
}

// combined merges name and value evidence. Value evidence is weighted
// slightly higher: what's *in* the column beats what it's called.
func combined(name, val float64) float64 {
	switch {
	case name > 0 && val > 0:
		return 0.45*name + 0.55*val
	case val > 0:
		return 0.75 * val
	case name > 0:
		return 0.60 * name // name-only capped: headers lie
	default:
		return 0
	}
}

// nameScore: exact normalized match = 1.0, token/substring hit scaled by
// specificity (longer synonym fragments are stronger evidence).
func nameScore(col string, syns []string) float64 {
	best := 0.0
	for _, s := range syns {
		var sc float64
		switch {
		case col == s:
			sc = 1.0
		case strings.Contains("_"+col+"_", "_"+s+"_"): // whole-token match
			sc = 0.85
		case strings.Contains(col, s):
			sc = 0.55 + 0.02*float64(len(s)) // substring, longer = stronger
			if sc > 0.75 {
				sc = 0.75
			}
		}
		if sc > best {
			best = sc
		}
	}
	return best
}

// valueScoreForKind checks a sample of column values against the pattern for
// a value_kind. This is what used to be a switch over CanonicalField
// (valueScore); indexing by kind instead means a new field just declares
// which kind it is and inherits an existing classifier rather than needing
// its own bespoke case.
func valueScoreForKind(kind ValueKind, values []string) float64 {
	if kind == KindText {
		return 0 // no reliable generic value signal; name-only fields
	}
	vals := sampleNonEmpty(values, 100)
	if len(vals) < 3 {
		return 0
	}
	hit := 0
	uniq := map[string]bool{}
	for _, v := range vals {
		v = strings.TrimSpace(v)
		uniq[v] = true
		switch kind {
		case KindCode:
			if skuValueRe.MatchString(strings.ToUpper(v)) {
				hit++
			}
		case KindBarcode:
			if barcodeValueRe.MatchString(v) {
				hit++
			}
		case KindCurrency:
			if currencyRe.MatchString(v) {
				hit++
			}
		case KindUOM:
			if uomVocab[strings.ToLower(v)] {
				hit++
			}
		case KindInteger:
			if numericRe.MatchString(v) {
				hit++
			}
		case KindDate:
			if dateRe.MatchString(v) {
				hit++
			}
		case KindFreetext:
			if len(v) >= 8 && strings.Contains(v, " ") {
				hit++
			}
		default:
			return 0 // unrecognized kind: no classifier, no signal
		}
	}
	ratio := float64(hit) / float64(len(vals))
	if ratio < 0.6 {
		return 0 // weak pattern agreement is worse than no signal
	}

	// Code/barcode values should also be near-unique; a repeating "code"
	// column is probably a category code.
	if kind == KindCode || kind == KindBarcode {
		uniqueness := float64(len(uniq)) / float64(len(vals))
		if uniqueness < 0.9 {
			ratio *= uniqueness
		}
	}
	return ratio
}

// Fingerprint hashes the sorted normalized header set. Sorted, so column
// reordering by the supplier doesn't invalidate a learned mapping; the saved
// mapping stores names, not indices, for the same reason.
func Fingerprint(columns []string) string {
	sorted := append([]string(nil), columns...)
	sort.Strings(sorted)
	h := sha256.Sum256([]byte(strings.Join(sorted, "\x1f")))
	return hex.EncodeToString(h[:8]) // 64 bits is plenty for this cardinality
}

func transpose(columns []string, rows [][]string) [][]string {
	out := make([][]string, len(columns))
	for _, r := range rows {
		for i := range columns {
			if i < len(r) {
				out[i] = append(out[i], r[i])
			}
		}
	}
	return out
}

func sampleNonEmpty(vals []string, max int) []string {
	var out []string
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
			if len(out) >= max {
				break
			}
		}
	}
	return out
}
