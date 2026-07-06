package sniff

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
)

// CanonicalField is a target field in the price-book domain model.
type CanonicalField string

const (
	FieldSKU         CanonicalField = "sku"
	FieldDescription CanonicalField = "description"
	FieldPrice       CanonicalField = "price"       // primary trade/cost price
	FieldListPrice   CanonicalField = "list_price"  // RRP / list
	FieldUOM         CanonicalField = "uom"
	FieldBarcode     CanonicalField = "barcode"
	FieldCategory    CanonicalField = "category"
	FieldBrand       CanonicalField = "brand"
	FieldDiscount    CanonicalField = "discount"
	FieldQtyBreak    CanonicalField = "qty_break"
	FieldUnknown     CanonicalField = ""
)

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
	Fingerprint string         `json:"fingerprint"`
	Mappings    []FieldMapping `json:"mappings"`
	Missing     []CanonicalField `json:"missing"` // required fields with no candidate
	Warnings    []string       `json:"warnings,omitempty"`
}

// requiredFields must be mapped (by suggestion or human) before commit.
var requiredFields = []CanonicalField{FieldSKU, FieldDescription, FieldPrice}

// synonyms maps canonical fields to normalized header-name fragments seen in
// AU/NZ plumbing, electrical, and building supplier price books. Matching is
// substring-based over the normalized (snake_case) header, so "trade_price_ex_gst"
// hits both "trade" and "price".
var synonyms = map[CanonicalField][]string{
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

// Order in which fields claim columns. More specific / higher-signal fields
// go first so e.g. list_price wins "rrp" before the generic price patterns
// see it.
var claimOrder = []CanonicalField{
	FieldBarcode, FieldSKU, FieldListPrice, FieldPrice, FieldUOM,
	FieldDiscount, FieldQtyBreak, FieldCategory, FieldBrand, FieldDescription,
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

// ProposeMapping scores every (column, field) pair using header-name evidence
// and value-distribution evidence, then assigns greedily in claimOrder.
//
// Name and value evidence are combined 50/50 when both exist. Value evidence
// alone can carry a mapping (mislabeled or blank headers are common); name
// evidence alone is capped at medium confidence because supplier headers lie.
func ProposeMapping(columns []string, samples [][]string) *MappingProposal {
	p := &MappingProposal{Fingerprint: Fingerprint(columns)}

	colValues := transpose(columns, samples)

	type cand struct {
		col        int
		name, val  float64
	}
	// score matrix
	scores := map[CanonicalField][]cand{}
	for f, syns := range synonyms {
		for i, col := range columns {
			ns := nameScore(col, syns)
			vs := valueScore(f, colValues[i])
			if ns > 0 || vs > 0 {
				scores[f] = append(scores[f], cand{i, ns, vs})
			}
		}
	}

	claimed := map[int]bool{}
	byField := map[CanonicalField]*FieldMapping{}

	for _, f := range claimOrder {
		cands := scores[f]
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
				Field:        f,
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
			byField[f] = fm
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
	for _, f := range claimOrder {
		if m, ok := byField[f]; ok {
			p.Mappings = append(p.Mappings, *m)
		}
	}
	sort.Slice(p.Mappings, func(a, b int) bool {
		return p.Mappings[a].SourceIndex < p.Mappings[b].SourceIndex
	})

	for _, rf := range requiredFields {
		if _, ok := byField[rf]; !ok {
			p.Missing = append(p.Missing, rf)
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

// valueScore checks a sample of column values against field-specific patterns.
func valueScore(f CanonicalField, values []string) float64 {
	vals := sampleNonEmpty(values, 100)
	if len(vals) < 3 {
		return 0
	}
	hit := 0
	uniq := map[string]bool{}
	for _, v := range vals {
		v = strings.TrimSpace(v)
		uniq[v] = true
		switch f {
		case FieldSKU:
			if skuValueRe.MatchString(strings.ToUpper(v)) {
				hit++
			}
		case FieldBarcode:
			if barcodeValueRe.MatchString(v) {
				hit++
			}
		case FieldPrice, FieldListPrice, FieldDiscount:
			if currencyRe.MatchString(v) {
				hit++
			}
		case FieldUOM:
			if uomVocab[strings.ToLower(v)] {
				hit++
			}
		case FieldQtyBreak:
			if numericRe.MatchString(v) {
				hit++
			}
		case FieldDescription:
			if len(v) >= 8 && strings.Contains(v, " ") {
				hit++
			}
		default:
			return 0 // category/brand: no reliable value signal
		}
	}
	ratio := float64(hit) / float64(len(vals))
	if ratio < 0.6 {
		return 0 // weak pattern agreement is worse than no signal
	}

	// SKU/barcode should also be near-unique; a repeating "code" column is
	// probably a category code.
	if f == FieldSKU || f == FieldBarcode {
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
