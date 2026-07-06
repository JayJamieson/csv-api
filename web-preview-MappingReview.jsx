import React, { useState, useMemo } from "react";
import {
  Upload, Check, AlertTriangle, ArrowRight, RotateCcw, Table2,
  FileWarning, Layers, ShieldQuestion, ChevronDown,
} from "lucide-react";

/* ------------------------------------------------------------------ *
 * Design tokens. Only Tailwind core utilities are available for layout,
 * so every custom color lives here and is applied via inline style.
 * ------------------------------------------------------------------ */
const T = {
  ground: "#EEEFEC",
  panel: "#FFFFFF",
  ink: "#17191E",
  mut: "#5C6069",
  faint: "#8A8E96",
  line: "#DADCD6",
  lineStrong: "#C3C6C0",
  navy: "#1F3A5F",
  navyInk: "#EAEEF4",
  high: "#147D64", // confidence: high / go
  highBg: "#E4F1EC",
  med: "#A66A00", // medium
  medBg: "#F6ECD9",
  low: "#B23A2E", // low / unmapped / danger
  lowBg: "#F5E3E0",
};

const MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
const SANS = "ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif";

/* Canonical fields — mirrors pkg/sniff/mapping.go. Order + required flag
 * match the backend so this UI never proposes a field the API won't accept. */
const FIELDS = [
  { key: "", label: "— unmapped —", req: false },
  { key: "sku", label: "SKU / product code", req: true },
  { key: "description", label: "Description", req: true },
  { key: "price", label: "Price (trade/cost)", req: true },
  { key: "list_price", label: "List price / RRP", req: false },
  { key: "uom", label: "Unit of measure", req: false },
  { key: "barcode", label: "Barcode / GTIN", req: false },
  { key: "category", label: "Category", req: false },
  { key: "brand", label: "Brand", req: false },
  { key: "discount", label: "Discount", req: false },
  { key: "qty_break", label: "Qty break", req: false },
];
const REQUIRED = FIELDS.filter((f) => f.req).map((f) => f.key);
const fieldLabel = (k) => FIELDS.find((f) => f.key === k)?.label ?? k;

function confBucket(c) {
  if (c >= 0.75) return "high";
  if (c >= 0.45) return "med";
  return "low";
}
const confColor = { high: T.high, med: T.med, low: T.low };
const confBg = { high: T.highBg, med: T.medBg, low: T.lowBg };

/* ================================================================== *
 * MOCK BACKEND
 * Every object below is shaped exactly like the real /load and /commit
 * JSON. To go live, replace mockLoad() with a fetch to POST /load and
 * mockCommit() with POST /imports/{id}/commit — the component reads the
 * same fields either way.
 * ================================================================== */

const SAMPLES = {
  mico: {
    label: "mico-pricebook-jul26.csv",
    note: "Messy supplier export — new layout",
    load: {
      ok: true,
      import_id: "b3f1c2a4-7d21-4e88-9a0c-1e5f6a2b9c33",
      state: "staged",
      source_encoding: "windows-1252",
      delimiter: ",",
      header_index: 3,
      header_confidence: "medium",
      header_candidates: [
        { index: 3, score: 0.82, cells: ["Product Code", "Description", "Trade Price", "RRP", "UOM"] },
        { index: 0, score: 0.21, cells: ["ACME PLUMBING SUPPLIES LTD", "", "", "", ""] },
      ],
      columns: ["product_code", "description", "trade_price", "rrp", "uom"],
      fingerprint: "9a1c4f7e2b6d0a83",
      known_fingerprint: false,
      proposed_mappings: [
        { source_column: "product_code", source_index: 0, field: "sku", confidence: 0.94, notes: "" },
        { source_column: "description", source_index: 1, field: "description", confidence: 0.9, notes: "" },
        { source_column: "trade_price", source_index: 2, field: "price", confidence: 0.88, notes: "" },
        { source_column: "rrp", source_index: 3, field: "list_price", confidence: 0.71, notes: "matched on header name only; values inconclusive" },
        { source_column: "uom", source_index: 4, field: "uom", confidence: 0.83, notes: "" },
      ],
      missing_fields: [],
      rows_staged: 214,
      preview: [
        ["CU-EL-15", "Copper Elbow 15mm 90°", "$2.50", "$4.10", "EA"],
        ["CU-EL-20", "Copper Elbow 20mm 90°", "$3.80", "$6.20", "EA"],
        ["CU-TE-15", "Copper Tee 15mm", "$3.10", "$5.00", "EA"],
        ["BV-GV-15", "Brass Gate Valve 15mm", "$12.40", "$19.95", "EA"],
        ["BV-BV-20", "Brass Ball Valve 20mm", "$1,145.00", "$1,800.00", "EA"],
        ["PP-PIPE-20", "PPR Pipe 20mm x 4m", "$18.90", "$29.50", "LM"],
      ],
      warnings: [],
    },
    commit: {
      ok: true,
      rows_raw: 214,
      rows_typed: 198,
      rows_quarantined: 4,
      rows_filtered_blank: 3,
      rows_filtered_sections: 8,
      rows_filtered_repeated_headers: 1,
      null_rates: { sku: 0.0, description: 0.0, price: 0.0, list_price: 0.06, uom: 0.02 },
      warnings: [],
    },
    quarantine: {
      ok: true,
      columns: ["product_code", "description", "trade_price", "rrp", "uom"],
      total: 4,
      rows: [
        { row: 47, cells: ["BV-XX-99", "Mystery Valve", "", "", "EA"], reason: "price missing/uncastable; " },
        { row: 88, cells: ["", "Copper Reducer 20-15mm", "$4.20", "$6.80", "EA"], reason: "sku missing/uncastable; " },
        { row: 132, cells: ["CU-CAP-15", "Copper End Cap 15mm", "P.O.A.", "$2.10", "EA"], reason: "price missing/uncastable; " },
        { row: 176, cells: ["FIX-BRK-01", "", "$8.00", "$12.50", "PK"], reason: "description missing/uncastable; " },
      ],
    },
  },

  ideal: {
    label: "ideal-electrical-q3.csv",
    note: "Known supplier — layout seen before",
    load: {
      ok: true,
      import_id: "1c77e0aa-9b02-4f13-a6d4-88b2ee901f5a",
      state: "staged",
      source_encoding: "utf-8",
      delimiter: ",",
      header_index: 0,
      header_confidence: "high",
      header_candidates: [
        { index: 0, score: 0.95, cells: ["Item No", "Item Description", "Cost Ex GST", "Barcode", "Pack"] },
      ],
      columns: ["item_no", "item_description", "cost_ex_gst", "barcode", "pack"],
      fingerprint: "42d8b0114ac9ff6e",
      known_fingerprint: true,
      proposed_mappings: [
        { source_column: "item_no", source_index: 0, field: "sku", confidence: 1.0, notes: "from saved mapping" },
        { source_column: "item_description", source_index: 1, field: "description", confidence: 1.0, notes: "from saved mapping" },
        { source_column: "cost_ex_gst", source_index: 2, field: "price", confidence: 1.0, notes: "from saved mapping" },
        { source_column: "barcode", source_index: 3, field: "barcode", confidence: 1.0, notes: "from saved mapping" },
        { source_column: "pack", source_index: 4, field: "uom", confidence: 1.0, notes: "from saved mapping" },
      ],
      missing_fields: [],
      rows_staged: 1902,
      preview: [
        ["4015113", "20A Weatherproof Switch", "14.20", "9310123456789", "EA"],
        ["4015114", "10A GPO Double White", "8.65", "9310123456796", "EA"],
        ["6620881", "2.5mm² TPS Cable 100m", "112.00", "9310987654321", "ROLL"],
        ["6620882", "1.5mm² TPS Cable 100m", "78.50", "9310987654338", "ROLL"],
      ],
      warnings: [],
    },
    commit: {
      ok: true,
      rows_raw: 1902,
      rows_typed: 1902,
      rows_quarantined: 0,
      rows_filtered_blank: 0,
      rows_filtered_sections: 0,
      rows_filtered_repeated_headers: 0,
      null_rates: { sku: 0.0, description: 0.0, price: 0.0, barcode: 0.0, uom: 0.0 },
      warnings: [],
    },
    quarantine: { ok: true, columns: ["item_no", "item_description", "cost_ex_gst", "barcode", "pack"], total: 0, rows: [] },
  },

  unknown: {
    label: "reece-export-raw.csv",
    note: "No clear header — needs manual pick",
    load: {
      ok: true,
      import_id: "7e5a91b3-2c4d-4a6f-b8e1-0d9c3f5a6b72",
      state: "staged",
      source_encoding: "utf-8",
      delimiter: ",",
      header_index: -1,
      header_confidence: "low",
      header_candidates: [
        { index: 4, score: 0.41, cells: ["Code", "Product", "Nett", "Barcode", "Sold By"] },
        { index: 5, score: 0.38, cells: ["", "PVC PRESSURE FITTINGS", "", "", ""] },
        { index: 0, score: 0.12, cells: ["Reece Group", "Price File", "", "", ""] },
      ],
      columns: [],
      fingerprint: "",
      known_fingerprint: false,
      proposed_mappings: [],
      missing_fields: ["sku", "description", "price"],
      rows_staged: 0,
      preview: [],
      warnings: ["no convincing header row found; manual selection required"],
    },
    // after the user picks a header we simulate a re-detect:
    reload: {
      ok: true,
      import_id: "7e5a91b3-2c4d-4a6f-b8e1-0d9c3f5a6b72",
      state: "staged",
      source_encoding: "utf-8",
      delimiter: ",",
      header_index: 4,
      header_confidence: "high",
      header_candidates: [{ index: 4, score: 1.0, cells: ["Code", "Product", "Nett", "Barcode", "Sold By"] }],
      columns: ["code", "product", "nett", "barcode", "sold_by"],
      fingerprint: "c0ffee1234ba5eed",
      known_fingerprint: false,
      proposed_mappings: [
        { source_column: "code", source_index: 0, field: "sku", confidence: 0.79, notes: "matched on values only; header name unrecognized" },
        { source_column: "product", source_index: 1, field: "description", confidence: 0.86, notes: "" },
        { source_column: "nett", source_index: 2, field: "price", confidence: 0.68, notes: "matched on values only; header name unrecognized" },
        { source_column: "barcode", source_index: 3, field: "barcode", confidence: 0.92, notes: "" },
        { source_column: "sold_by", source_index: 4, field: "uom", confidence: 0.64, notes: "" },
      ],
      missing_fields: [],
      rows_staged: 660,
      preview: [
        ["PVC-EL-20", "PVC Pressure Elbow 20mm", "1.85", "9311111000012", "EA"],
        ["PVC-EL-25", "PVC Pressure Elbow 25mm", "2.40", "9311111000029", "EA"],
        ["PVC-TE-20", "PVC Pressure Tee 20mm", "2.10", "9311111000036", "EA"],
      ],
      warnings: [],
    },
    commit: {
      ok: true,
      rows_raw: 660,
      rows_typed: 651,
      rows_quarantined: 2,
      rows_filtered_blank: 3,
      rows_filtered_sections: 4,
      rows_filtered_repeated_headers: 0,
      null_rates: { sku: 0.0, description: 0.0, price: 0.0, barcode: 0.0, uom: 0.03 },
      warnings: [],
    },
    quarantine: {
      ok: true,
      columns: ["code", "product", "nett", "barcode", "sold_by"],
      total: 2,
      rows: [
        { row: 219, cells: ["PVC-BALL-25", "PVC Ball Valve 25mm", "call", "9311111000432", "EA"], reason: "price missing/uncastable; " },
        { row: 404, cells: ["", "PVC Union 32mm", "3.90", "9311111000821", "EA"], reason: "sku missing/uncastable; " },
      ],
    },
  },
};

/* ================================================================== *
 * Small presentational pieces
 * ================================================================== */

function SignalMeter({ level }) {
  // three-bar meter; the recurring confidence motif
  const filled = level === "high" ? 3 : level === "med" ? 2 : 1;
  return (
    <span className="inline-flex items-end gap-0.5" style={{ height: 14 }} aria-hidden>
      {[0, 1, 2].map((i) => (
        <span
          key={i}
          style={{
            width: 3,
            height: 5 + i * 4,
            borderRadius: 1,
            background: i < filled ? confColor[level] : T.line,
          }}
        />
      ))}
    </span>
  );
}

function Chip({ children, color = T.mut, bg = "transparent", mono }) {
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 text-xs"
      style={{
        color, background: bg, borderRadius: 4,
        border: `1px solid ${bg === "transparent" ? T.line : "transparent"}`,
        fontFamily: mono ? MONO : SANS, letterSpacing: mono ? 0 : 0.2,
      }}
    >
      {children}
    </span>
  );
}

function Stat({ value, label, color = T.ink }) {
  return (
    <div className="flex flex-col">
      <span style={{ fontFamily: MONO, fontSize: 20, fontWeight: 600, color }}>{value}</span>
      <span className="text-xs mt-0.5" style={{ color: T.faint, letterSpacing: 0.3 }}>{label}</span>
    </div>
  );
}

/* ================================================================== *
 * Screen 1 — file picker (stands in for the real upload)
 * ================================================================== */
function Picker({ onPick }) {
  return (
    <div className="max-w-3xl mx-auto px-6 py-16">
      <div className="mb-10">
        <div className="flex items-center gap-2 mb-3">
          <span style={{ width: 8, height: 8, background: T.navy, borderRadius: 2 }} />
          <span className="text-xs" style={{ color: T.mut, letterSpacing: 2, textTransform: "uppercase" }}>
            Price book import
          </span>
        </div>
        <h1 style={{ fontFamily: SANS, fontSize: 34, fontWeight: 680, color: T.ink, letterSpacing: -0.5, lineHeight: 1.1 }}>
          Review before you import
        </h1>
        <p className="mt-3" style={{ color: T.mut, fontSize: 15, maxWidth: 520, lineHeight: 1.5 }}>
          Pick a staged file to see what the parser detected — header row, column mapping,
          and which rows it set aside. Nothing is written until you commit.
        </p>
      </div>

      <div className="flex flex-col gap-3">
        {Object.entries(SAMPLES).map(([id, s]) => (
          <button
            key={id}
            onClick={() => onPick(id)}
            className="flex items-center justify-between px-5 py-4 text-left transition-colors"
            style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8 }}
            onMouseEnter={(e) => (e.currentTarget.style.borderColor = T.lineStrong)}
            onMouseLeave={(e) => (e.currentTarget.style.borderColor = T.line)}
          >
            <div className="flex items-center gap-4">
              <div className="flex items-center justify-center" style={{ width: 40, height: 40, background: T.ground, borderRadius: 6 }}>
                <Upload size={18} color={T.navy} />
              </div>
              <div>
                <div style={{ fontFamily: MONO, fontSize: 14, color: T.ink, fontWeight: 600 }}>{s.label}</div>
                <div className="text-xs mt-0.5" style={{ color: T.faint }}>{s.note}</div>
              </div>
            </div>
            <ArrowRight size={18} color={T.faint} />
          </button>
        ))}
      </div>

      <p className="mt-8 text-xs" style={{ color: T.faint, lineHeight: 1.6 }}>
        Standalone preview. Responses are mocked but shaped exactly like the real
        <span style={{ fontFamily: MONO }}> /load </span> payload, so the same screen runs
        against the backend by swapping the fetch.
      </p>
    </div>
  );
}

/* ================================================================== *
 * Header-row selector — shown when confidence is low / no header found
 * ================================================================== */
function HeaderPicker({ candidates, onChoose, onCancel, busy }) {
  const [sel, setSel] = useState(candidates[0]?.index ?? 0);
  return (
    <div className="px-6 py-5" style={{ background: T.lowBg, border: `1px solid ${T.low}22`, borderRadius: 8 }}>
      <div className="flex items-center gap-2 mb-1">
        <ShieldQuestion size={16} color={T.low} />
        <span style={{ color: T.low, fontWeight: 600, fontSize: 14 }}>Pick the header row</span>
      </div>
      <p className="text-xs mb-4" style={{ color: T.mut }}>
        The parser couldn’t settle on a header. Choose the row that names the columns.
      </p>
      <div className="flex flex-col gap-2">
        {candidates.map((c) => (
          <label
            key={c.index}
            className="flex items-center gap-3 px-3 py-2 cursor-pointer"
            style={{
              background: sel === c.index ? T.panel : "transparent",
              border: `1px solid ${sel === c.index ? T.lineStrong : "transparent"}`,
              borderRadius: 6,
            }}
          >
            <input type="radio" checked={sel === c.index} onChange={() => setSel(c.index)} style={{ accentColor: T.navy }} />
            <span style={{ fontFamily: MONO, fontSize: 11, color: T.faint, minWidth: 40 }}>row {c.index}</span>
            <span className="flex-1 truncate" style={{ fontFamily: MONO, fontSize: 12, color: T.ink }}>
              {c.cells.filter(Boolean).join("  ·  ") || <em style={{ color: T.faint }}>empty</em>}
            </span>
            <Chip mono>{Math.round(c.score * 100)}%</Chip>
          </label>
        ))}
      </div>
      <div className="flex gap-2 mt-4">
        <button
          onClick={() => onChoose(sel)}
          disabled={busy}
          className="px-4 py-2 text-sm font-medium transition-opacity"
          style={{ background: T.navy, color: T.navyInk, borderRadius: 6, opacity: busy ? 0.6 : 1 }}
        >
          {busy ? "Re-reading…" : "Use this row"}
        </button>
        <button onClick={onCancel} className="px-4 py-2 text-sm" style={{ color: T.mut }}>
          Back
        </button>
      </div>
    </div>
  );
}

/* ================================================================== *
 * Column mapping editor — the header cell of the preview grid.
 * This is the signature: you review by reading across the header row,
 * each column tinted by how sure the parser is.
 * ================================================================== */
function ColumnHead({ col, mapping, onChange, duplicate }) {
  const conf = mapping.field ? confBucket(mapping.confidence) : "low";
  const unmapped = !mapping.field;
  const tint = unmapped ? T.lowBg : confBg[conf];
  const edge = unmapped ? T.low : confColor[conf];

  return (
    <th
      className="align-top text-left p-0"
      style={{ background: tint, borderBottom: `2px solid ${edge}`, minWidth: 168 }}
    >
      <div className="px-3 pt-2.5 pb-3">
        <div className="flex items-center justify-between mb-1.5">
          <span style={{ fontFamily: MONO, fontSize: 11, color: T.mut }} className="truncate">
            {col}
          </span>
          {!unmapped && <SignalMeter level={conf} />}
        </div>

        <div className="relative">
          <select
            value={mapping.field}
            onChange={(e) => onChange(e.target.value)}
            className="w-full appearance-none text-sm font-medium pr-6"
            style={{
              background: T.panel, color: unmapped ? T.low : T.ink,
              border: `1px solid ${unmapped ? T.low : T.line}`,
              borderRadius: 5, padding: "6px 8px", fontFamily: SANS,
            }}
          >
            {FIELDS.map((f) => (
              <option key={f.key} value={f.key}>{f.label}{f.req ? " *" : ""}</option>
            ))}
          </select>
          <ChevronDown size={14} color={T.faint} style={{ position: "absolute", right: 7, top: 9, pointerEvents: "none" }} />
        </div>

        <div style={{ minHeight: 28 }} className="mt-1.5">
          {duplicate && (
            <div className="flex items-start gap-1" style={{ color: T.low, fontSize: 10.5, lineHeight: 1.3 }}>
              <AlertTriangle size={11} style={{ marginTop: 1, flexShrink: 0 }} />
              <span>Mapped twice — only one column can be {fieldLabel(mapping.field)}.</span>
            </div>
          )}
          {!duplicate && mapping.notes && (
            <div style={{ color: T.mut, fontSize: 10.5, lineHeight: 1.3 }}>{mapping.notes}</div>
          )}
          {!duplicate && !mapping.notes && !unmapped && (
            <div style={{ color: T.faint, fontSize: 10.5 }}>
              {Math.round(mapping.confidence * 100)}% confidence
            </div>
          )}
        </div>
      </div>
    </th>
  );
}

/* ================================================================== *
 * Review screen
 * ================================================================== */
function Review({ sampleId, load, onCommit, onReheader, onReset }) {
  const [mappings, setMappings] = useState(() =>
    load.proposed_mappings.map((m) => ({ ...m }))
  );
  const [pickingHeader, setPickingHeader] = useState(load.header_index < 0);
  const [busy, setBusy] = useState(false);

  // keep local mapping state in sync if the parent hands us a new load
  // (e.g. after a header re-pick)
  React.useEffect(() => {
    setMappings(load.proposed_mappings.map((m) => ({ ...m })));
    setPickingHeader(load.header_index < 0);
  }, [load]);

  const setField = (idx, field) =>
    setMappings((ms) => ms.map((m) => (m.source_index === idx ? { ...m, field, notes: field === m.field ? m.notes : "" } : m)));

  // which fields are mapped more than once
  const dupFields = useMemo(() => {
    const seen = {};
    mappings.forEach((m) => { if (m.field) seen[m.field] = (seen[m.field] || 0) + 1; });
    return new Set(Object.entries(seen).filter(([, n]) => n > 1).map(([f]) => f));
  }, [mappings]);

  const mappedFields = new Set(mappings.filter((m) => m.field).map((m) => m.field));
  const missing = REQUIRED.filter((r) => !mappedFields.has(r));
  const hasDup = dupFields.size > 0;
  const canCommit = missing.length === 0 && !hasDup;

  const conf = load.header_confidence;

  if (pickingHeader) {
    return (
      <Shell load={load} onReset={onReset}>
        <HeaderPicker
          candidates={load.header_candidates}
          busy={busy}
          onCancel={onReset}
          onChoose={(idx) => { setBusy(true); setTimeout(() => { onReheader(idx); setBusy(false); }, 550); }}
        />
      </Shell>
    );
  }

  return (
    <Shell load={load} onReset={onReset}>
      {/* detection summary strip */}
      <div className="flex flex-wrap items-center gap-x-8 gap-y-4 px-5 py-4 mb-5"
        style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8 }}>
        <Stat value={load.rows_staged.toLocaleString()} label="rows staged" />
        <div style={{ width: 1, height: 34, background: T.line }} />
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <span style={{ fontFamily: MONO, fontSize: 20, fontWeight: 600, color: confColor[conf] }}>
              row {load.header_index}
            </span>
            <SignalMeter level={conf} />
          </div>
          <span className="text-xs mt-0.5" style={{ color: T.faint }}>header · {conf} confidence</span>
        </div>
        <div style={{ width: 1, height: 34, background: T.line }} />
        <Stat value={load.source_encoding} label="encoding" />
        <Stat value={load.delimiter === "," ? "comma" : load.delimiter} label="delimiter" />
        <div className="flex-1" />
        <div className="flex flex-col items-end">
          {load.known_fingerprint ? (
            <Chip color={T.high} bg={T.highBg}>◆ known supplier layout</Chip>
          ) : (
            <Chip color={T.mut}>new layout</Chip>
          )}
          <button onClick={() => setPickingHeader(true)}
            className="flex items-center gap-1 mt-1.5 text-xs" style={{ color: T.navy }}>
            <RotateCcw size={11} /> change header row
          </button>
        </div>
      </div>

      {/* required-field / duplicate banner */}
      {(missing.length > 0 || hasDup) && (
        <div className="flex items-start gap-2 px-4 py-3 mb-5"
          style={{ background: T.lowBg, border: `1px solid ${T.low}33`, borderRadius: 8 }}>
          <AlertTriangle size={16} color={T.low} style={{ marginTop: 1 }} />
          <div style={{ fontSize: 13, color: T.ink, lineHeight: 1.5 }}>
            {missing.length > 0 && (
              <div>Map the required {missing.length === 1 ? "field" : "fields"}:{" "}
                <b>{missing.map(fieldLabel).join(", ")}</b>. Every import needs SKU, description and price.</div>
            )}
            {hasDup && (
              <div>Resolve duplicate {dupFields.size === 1 ? "mapping" : "mappings"}:{" "}
                <b>{[...dupFields].map(fieldLabel).join(", ")}</b>.</div>
            )}
          </div>
        </div>
      )}

      {/* the grid: column heads are the mapping editors */}
      <div className="mb-2 flex items-center gap-2">
        <Table2 size={15} color={T.mut} />
        <span className="text-xs" style={{ color: T.mut, letterSpacing: 0.3 }}>
          Set each column’s meaning in its header. Tint shows the parser’s confidence.
        </span>
      </div>
      <div className="overflow-x-auto mb-6" style={{ border: `1px solid ${T.line}`, borderRadius: 8 }}>
        <table className="w-full border-collapse" style={{ background: T.panel }}>
          <thead>
            <tr>
              {load.columns.map((col, i) => {
                const m = mappings.find((x) => x.source_index === i) ?? { source_index: i, field: "", confidence: 0 };
                return (
                  <ColumnHead key={i} col={col} mapping={m}
                    duplicate={m.field && dupFields.has(m.field)}
                    onChange={(f) => setField(i, f)} />
                );
              })}
            </tr>
          </thead>
          <tbody>
            {load.preview.map((row, r) => (
              <tr key={r} style={{ borderTop: `1px solid ${T.ground}` }}>
                {row.map((cell, c) => (
                  <td key={c} className="px-3 py-2" style={{ fontFamily: MONO, fontSize: 12, color: T.ink, whiteSpace: "nowrap" }}>
                    {cell || <span style={{ color: T.faint }}>—</span>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* commit bar */}
      <div className="flex items-center justify-between px-5 py-4"
        style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8 }}>
        <div className="text-xs" style={{ color: T.mut, lineHeight: 1.5 }}>
          Committing cleans and types every row, quarantines rows missing a required field,
          {load.known_fingerprint ? " and refreshes" : " and saves"} this layout for next time.
          <div style={{ fontFamily: MONO, marginTop: 2, color: T.faint }}>
            fingerprint {load.fingerprint || "—"}
          </div>
        </div>
        <button
          onClick={() => { setBusy(true); setTimeout(() => onCommit(sampleId, mappings), 650); }}
          disabled={!canCommit || busy}
          className="flex items-center gap-2 px-5 py-2.5 text-sm font-semibold transition-opacity"
          style={{
            background: canCommit ? T.navy : T.line,
            color: canCommit ? T.navyInk : T.faint,
            borderRadius: 7, opacity: busy ? 0.7 : 1, cursor: canCommit ? "pointer" : "not-allowed",
          }}
        >
          {busy ? "Importing…" : <>Commit import <ArrowRight size={16} /></>}
        </button>
      </div>
    </Shell>
  );
}

/* ================================================================== *
 * Quarantine panel — the rejected rows, with the offending cell called
 * out. Reasons name canonical fields ("price missing/uncastable; ");
 * we map those back to source columns via the committed mapping so the
 * highlight lands on the right cell.
 * ================================================================== */
function QuarantinePanel({ quarantine, mappings }) {
  const [open, setOpen] = useState(false);

  // canonical field -> source column index, from the committed mapping
  const fieldToCol = useMemo(() => {
    const m = {};
    (mappings || []).forEach((x) => { if (x.field) m[x.field] = x.source_index; });
    return m;
  }, [mappings]);

  // which source-column indices does a reason string implicate?
  const badColsFor = (reason) => {
    const bad = new Set();
    Object.keys(fieldToCol).forEach((f) => {
      if (reason && reason.includes(f + " ")) bad.add(fieldToCol[f]);
    });
    return bad;
  };
  // human phrases from the reason string
  const reasonChips = (reason) =>
    (reason || "").split(";").map((s) => s.trim()).filter(Boolean);

  if (!quarantine || quarantine.total === 0) {
    return (
      <div className="flex items-center gap-2 px-5 py-4 mb-5"
        style={{ background: T.highBg, border: `1px solid ${T.high}22`, borderRadius: 8 }}>
        <Check size={16} color={T.high} />
        <span style={{ fontSize: 13, color: T.ink }}>
          Nothing quarantined — every row had all required fields.
        </span>
      </div>
    );
  }

  return (
    <div className="mb-5" style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8, overflow: "hidden" }}>
      <button onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center justify-between px-5 py-3.5"
        style={{ background: open ? T.medBg : T.panel, transition: "background 0.15s" }}>
        <div className="flex items-center gap-2.5">
          <FileWarning size={16} color={T.med} />
          <span style={{ fontSize: 14, fontWeight: 600, color: T.ink }}>
            {quarantine.total} quarantined {quarantine.total === 1 ? "row" : "rows"}
          </span>
          <span className="text-xs" style={{ color: T.faint }}>kept with a reason, not dropped</span>
        </div>
        <ChevronDown size={16} color={T.mut}
          style={{ transform: open ? "rotate(180deg)" : "none", transition: "transform 0.2s" }} />
      </button>

      {open && (
        <div className="overflow-x-auto" style={{ borderTop: `1px solid ${T.line}` }}>
          <table className="w-full border-collapse">
            <thead>
              <tr style={{ background: T.ground }}>
                <th className="px-3 py-2 text-left" style={{ fontFamily: MONO, fontSize: 10.5, color: T.faint, fontWeight: 500 }}>row</th>
                {quarantine.columns.map((c, i) => (
                  <th key={i} className="px-3 py-2 text-left" style={{ fontFamily: MONO, fontSize: 10.5, color: T.mut, fontWeight: 500, whiteSpace: "nowrap" }}>{c}</th>
                ))}
                <th className="px-3 py-2 text-left" style={{ fontSize: 10.5, color: T.low, fontWeight: 600, letterSpacing: 0.3 }}>WHY</th>
              </tr>
            </thead>
            <tbody>
              {quarantine.rows.map((r, ri) => {
                const bad = badColsFor(r.reason);
                return (
                  <tr key={ri} style={{ borderTop: `1px solid ${T.ground}` }}>
                    <td className="px-3 py-2" style={{ fontFamily: MONO, fontSize: 11, color: T.faint }}>{r.row}</td>
                    {r.cells.map((cell, ci) => {
                      const isBad = bad.has(ci);
                      const empty = !cell;
                      return (
                        <td key={ci} className="px-3 py-2" style={{
                          fontFamily: MONO, fontSize: 12, whiteSpace: "nowrap",
                          background: isBad ? T.lowBg : "transparent",
                          color: isBad ? T.low : empty ? T.faint : T.ink,
                          fontWeight: isBad ? 600 : 400,
                          boxShadow: isBad ? `inset 0 0 0 1px ${T.low}44` : "none",
                        }}>
                          {cell || (isBad ? "∅ empty" : "—")}
                        </td>
                      );
                    })}
                    <td className="px-3 py-2">
                      <div className="flex flex-wrap gap-1">
                        {reasonChips(r.reason).map((rc, i) => (
                          <span key={i} className="inline-flex items-center px-1.5 py-0.5"
                            style={{ background: T.lowBg, color: T.low, fontSize: 10.5, borderRadius: 3, whiteSpace: "nowrap" }}>
                            {rc}
                          </span>
                        ))}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="px-4 py-2.5 text-xs" style={{ color: T.faint, borderTop: `1px solid ${T.ground}`, lineHeight: 1.5 }}>
            Highlighted cells are what failed — empty, or a value that wouldn’t cast
            (e.g. <span style={{ fontFamily: MONO }}>P.O.A.</span> or <span style={{ fontFamily: MONO }}>call</span> in a price).
            Fix the source file and re-import, or map a different column.
          </div>
        </div>
      )}
    </div>
  );
}

/* ================================================================== *
 * Result screen
 * ================================================================== */
function Result({ commit, quarantine, mappings, load, onReset }) {
  const kept = commit.rows_typed;
  const keptPct = commit.rows_raw ? Math.round((kept / commit.rows_raw) * 100) : 0;
  const filtered = commit.rows_filtered_blank + commit.rows_filtered_sections + commit.rows_filtered_repeated_headers;

  return (
    <Shell load={load} onReset={onReset}>
      <div className="flex items-center gap-3 mb-6">
        <div className="flex items-center justify-center" style={{ width: 36, height: 36, background: T.highBg, borderRadius: 8 }}>
          <Check size={20} color={T.high} />
        </div>
        <div>
          <div style={{ fontSize: 18, fontWeight: 650, color: T.ink }}>Import committed</div>
          <div className="text-xs" style={{ color: T.faint }}>
            {kept.toLocaleString()} of {commit.rows_raw.toLocaleString()} rows typed · {keptPct}% kept
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 mb-5" style={{ gridTemplateColumns: "repeat(auto-fit,minmax(150px,1fr))" }}>
        <ResultCard icon={<Check size={15} color={T.high} />} value={kept.toLocaleString()} label="rows imported" color={T.high} bg={T.highBg} />
        <ResultCard icon={<FileWarning size={15} color={T.med} />} value={commit.rows_quarantined} label="quarantined" color={T.med} bg={T.medBg}
          hint="missing a required field — kept with a reason, not dropped" />
        <ResultCard icon={<Layers size={15} color={T.mut} />} value={filtered} label="noise filtered" color={T.ink} bg={T.ground}
          hint={`${commit.rows_filtered_sections} section · ${commit.rows_filtered_blank} blank · ${commit.rows_filtered_repeated_headers} repeat header`} />
      </div>

      {/* rejected rows, with the offending cell called out */}
      <QuarantinePanel quarantine={quarantine} mappings={mappings} />

      {/* null-rate bars — the "is this sane" signal */}
      <div className="px-5 py-4 mb-5" style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8 }}>
        <div className="text-xs mb-3" style={{ color: T.mut, letterSpacing: 0.3 }}>
          NULL rate per field after typing — a mapped column that’s mostly empty is a mapping worth a second look.
        </div>
        <div className="flex flex-col gap-2">
          {Object.entries(commit.null_rates).map(([f, rate]) => {
            const pct = Math.round(rate * 100);
            const warn = rate > 0.2;
            return (
              <div key={f} className="flex items-center gap-3">
                <span style={{ width: 110, fontFamily: MONO, fontSize: 12, color: T.ink }}>{f}</span>
                <div className="flex-1 h-2 rounded-full overflow-hidden" style={{ background: T.ground }}>
                  <div style={{ width: `${Math.max(pct, 2)}%`, height: "100%", background: warn ? T.low : T.high, borderRadius: 999 }} />
                </div>
                <span style={{ width: 40, textAlign: "right", fontFamily: MONO, fontSize: 11, color: warn ? T.low : T.faint }}>{pct}%</span>
              </div>
            );
          })}
        </div>
      </div>

      <div className="flex items-center justify-between px-5 py-4" style={{ background: T.navy, borderRadius: 8 }}>
        <div>
          <div style={{ color: T.navyInk, fontSize: 13, fontWeight: 600 }}>Query endpoint ready</div>
          <div style={{ fontFamily: MONO, fontSize: 12, color: "#AEBBCD", marginTop: 2 }}>
            /api/{load.import_id}
          </div>
        </div>
        <button onClick={onReset} className="px-4 py-2 text-sm font-medium"
          style={{ background: "#FFFFFF18", color: T.navyInk, borderRadius: 6 }}>
          Import another
        </button>
      </div>
    </Shell>
  );
}

function ResultCard({ icon, value, label, color, bg, hint }) {
  return (
    <div className="px-4 py-3" style={{ background: bg, border: `1px solid ${T.line}`, borderRadius: 8 }}>
      <div className="flex items-center gap-1.5 mb-1">{icon}
        <span style={{ fontFamily: MONO, fontSize: 22, fontWeight: 650, color }}>{value}</span>
      </div>
      <div className="text-xs" style={{ color: T.mut }}>{label}</div>
      {hint && <div className="text-xs mt-1" style={{ color: T.faint, lineHeight: 1.4 }}>{hint}</div>}
    </div>
  );
}

/* ================================================================== *
 * Shell — shared chrome
 * ================================================================== */
function Shell({ children, load, onReset }) {
  return (
    <div style={{ minHeight: "100%", background: T.ground, fontFamily: SANS }}>
      <div className="flex items-center justify-between px-6 py-3.5"
        style={{ background: T.panel, borderBottom: `1px solid ${T.line}` }}>
        <button onClick={onReset} className="flex items-center gap-2">
          <span style={{ width: 8, height: 8, background: T.navy, borderRadius: 2 }} />
          <span style={{ fontSize: 13, fontWeight: 650, color: T.ink, letterSpacing: -0.2 }}>Price book import</span>
        </button>
        <div className="flex items-center gap-2">
          <Chip mono>{load.import_id.slice(0, 8)}</Chip>
        </div>
      </div>
      <div className="max-w-5xl mx-auto px-6 py-7">{children}</div>
    </div>
  );
}

/* ================================================================== *
 * Root
 * ================================================================== */
export default function App() {
  const [screen, setScreen] = useState("pick"); // pick | review | result
  const [sampleId, setSampleId] = useState(null);
  const [load, setLoad] = useState(null);
  const [commit, setCommit] = useState(null);
  const [quarantine, setQuarantine] = useState(null);
  const [committedMappings, setCommittedMappings] = useState(null);

  // mockLoad — replace with: await fetch('/load?name=...').then(r=>r.json())
  const mockLoad = (id) => SAMPLES[id].load;
  // mockReheader — replace with: POST /imports/{id}/commit {header_index} then re-load,
  // or a dedicated re-detect endpoint. Here we return the pre-baked re-detect.
  const mockReheader = (id) => SAMPLES[id].reload ?? SAMPLES[id].load;
  // mockCommit — replace with: await fetch(`/imports/${importId}/commit`, {method:'POST', body: {mappings}})
  const mockCommit = (id) => SAMPLES[id].commit;
  // mockQuarantine — replace with: await fetch(`/imports/${importId}/quarantine`).then(r=>r.json())
  const mockQuarantine = (id) => SAMPLES[id].quarantine;

  const pick = (id) => { setSampleId(id); setLoad(mockLoad(id)); setScreen("review"); };
  const reheader = (idx) => setLoad({ ...mockReheader(sampleId), _pickedRow: idx });
  const doCommit = (id, mappings) => {
    setCommittedMappings(mappings);
    setCommit(mockCommit(id));
    setQuarantine(mockQuarantine(id)); // fetched after commit, once quarantine exists
    setScreen("result");
  };
  const reset = () => {
    setScreen("pick"); setSampleId(null); setLoad(null);
    setCommit(null); setQuarantine(null); setCommittedMappings(null);
  };

  if (screen === "pick") return <div style={{ minHeight: "100vh", background: T.ground }}><Picker onPick={pick} /></div>;
  if (screen === "review") return <Review sampleId={sampleId} load={load} onCommit={doCommit} onReheader={reheader} onReset={reset} />;
  return <Result commit={commit} quarantine={quarantine} mappings={committedMappings} load={load} onReset={reset} />;
}
