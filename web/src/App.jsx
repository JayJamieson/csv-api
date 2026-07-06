import React, { useState, useMemo, useCallback } from "react";
import {
  Upload, Check, AlertTriangle, ArrowRight, RotateCcw, Table2,
  FileWarning, Layers, ShieldQuestion, ChevronDown, Download,
} from "lucide-react";

/* ------------------------------------------------------------------ *
 * Design tokens. Only Tailwind core utilities are used for layout;
 * every custom color lives here and is applied via inline style.
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
// The backend header_confidence enum is high|medium|low; the meter/colors key
// on high|med|low. Normalize so "medium" lights the middle bar correctly.
const normConf = (c) => ({ high: "high", medium: "med", med: "med", low: "low" }[c] || "low");

/* ================================================================== *
 * Backend calls — the four seams the preview mocked.
 * ================================================================== */
async function apiError(r) {
  try {
    const j = await r.json();
    return j.message || j.error || `HTTP ${r.status}`;
  } catch {
    return `HTTP ${r.status}`;
  }
}

async function apiLoad(file) {
  const r = await fetch(`/load?name=${encodeURIComponent(file.name)}`, {
    method: "POST",
    headers: { "Content-Type": "text/csv" },
    body: file,
  });
  if (!r.ok) throw new Error(await apiError(r));
  return r.json();
}

async function apiRedetect(importId, headerIndex) {
  const r = await fetch(`/imports/${importId}/redetect`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ header_index: headerIndex }),
  });
  if (!r.ok) throw new Error(await apiError(r));
  return r.json();
}

async function apiCommit(importId, mappings) {
  const r = await fetch(`/imports/${importId}/commit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mappings }),
  });
  if (!r.ok) throw new Error(await apiError(r));
  return r.json();
}

async function apiQuarantine(importId) {
  const r = await fetch(`/imports/${importId}/quarantine`);
  if (!r.ok) throw new Error(await apiError(r));
  return r.json();
}

const quarantineCsvUrl = (importId) => `/imports/${importId}/quarantine?format=csv`;

/* ================================================================== *
 * Small presentational pieces
 * ================================================================== */

function SignalMeter({ level }) {
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

function ErrorBanner({ message, onDismiss }) {
  if (!message) return null;
  return (
    <div className="flex items-start gap-2 px-4 py-3 mb-5"
      style={{ background: T.lowBg, border: `1px solid ${T.low}33`, borderRadius: 8 }}>
      <AlertTriangle size={16} color={T.low} style={{ marginTop: 1 }} />
      <div className="flex-1" style={{ fontSize: 13, color: T.ink, lineHeight: 1.5 }}>{message}</div>
      <button onClick={onDismiss} className="text-xs" style={{ color: T.mut }}>dismiss</button>
    </div>
  );
}

/* ================================================================== *
 * Screen 1 — file-drop upload (the real POST /load)
 * ================================================================== */
function Uploader({ onFile, busy, error, onDismissError }) {
  const [drag, setDrag] = useState(false);
  const inputRef = React.useRef(null);

  const take = (files) => {
    if (files && files.length) onFile(files[0]);
  };

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
          Drop a supplier CSV to see what the parser detected — header row, column mapping,
          and which rows it set aside. Nothing is written until you commit.
        </p>
      </div>

      <ErrorBanner message={error} onDismiss={onDismissError} />

      <button
        type="button"
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
        onDragLeave={() => setDrag(false)}
        onDrop={(e) => { e.preventDefault(); setDrag(false); take(e.dataTransfer.files); }}
        className="w-full flex flex-col items-center justify-center gap-4 px-6 py-16 text-center transition-colors"
        style={{
          background: T.panel,
          border: `2px dashed ${drag ? T.navy : T.lineStrong}`,
          borderRadius: 12,
          opacity: busy ? 0.6 : 1,
          cursor: busy ? "wait" : "pointer",
        }}
      >
        <div className="flex items-center justify-center" style={{ width: 52, height: 52, background: T.ground, borderRadius: 10 }}>
          <Upload size={22} color={T.navy} />
        </div>
        <div>
          <div style={{ fontSize: 16, fontWeight: 600, color: T.ink }}>
            {busy ? "Staging…" : "Drop a CSV here, or click to choose"}
          </div>
          <div className="text-xs mt-1" style={{ color: T.faint }}>
            Messy supplier price books welcome — preamble, sections, currency formatting, mixed encodings.
          </div>
        </div>
        <input
          ref={inputRef}
          type="file"
          accept=".csv,text/csv"
          className="hidden"
          onChange={(e) => take(e.target.files)}
        />
      </button>

      <p className="mt-8 text-xs" style={{ color: T.faint, lineHeight: 1.6 }}>
        Phase 1 stages the file and proposes a mapping; nothing is committed until you confirm.
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
function Review({ load, candidates, onCommit, onReheader, onReset, error, onDismissError }) {
  const [mappings, setMappings] = useState(() =>
    load.proposed_mappings.map((m) => ({ ...m }))
  );
  const [pickingHeader, setPickingHeader] = useState(load.header_index < 0);
  const [busy, setBusy] = useState(false);

  React.useEffect(() => {
    setMappings((load.proposed_mappings || []).map((m) => ({ ...m })));
    setPickingHeader(load.header_index < 0);
    setBusy(false);
  }, [load]);

  const setField = (idx, field) =>
    setMappings((ms) => ms.map((m) => (m.source_index === idx ? { ...m, field, notes: field === m.field ? m.notes : "" } : m)));

  const dupFields = useMemo(() => {
    const seen = {};
    mappings.forEach((m) => { if (m.field) seen[m.field] = (seen[m.field] || 0) + 1; });
    return new Set(Object.entries(seen).filter(([, n]) => n > 1).map(([f]) => f));
  }, [mappings]);

  const mappedFields = new Set(mappings.filter((m) => m.field).map((m) => m.field));
  const missing = REQUIRED.filter((r) => !mappedFields.has(r));
  const hasDup = dupFields.size > 0;
  const canCommit = missing.length === 0 && !hasDup;

  const conf = normConf(load.header_confidence);

  const chooseHeader = async (idx) => {
    setBusy(true);
    try {
      await onReheader(idx);
    } finally {
      setBusy(false);
    }
  };

  const commit = async () => {
    setBusy(true);
    try {
      await onCommit(mappings);
    } finally {
      setBusy(false);
    }
  };

  if (pickingHeader) {
    return (
      <Shell load={load} onReset={onReset}>
        <ErrorBanner message={error} onDismiss={onDismissError} />
        {candidates.length > 0 ? (
          <HeaderPicker candidates={candidates} busy={busy} onCancel={onReset} onChoose={chooseHeader} />
        ) : (
          <div className="px-6 py-5" style={{ background: T.lowBg, border: `1px solid ${T.low}22`, borderRadius: 8 }}>
            <div style={{ color: T.low, fontWeight: 600, fontSize: 14 }}>No header candidates</div>
            <p className="text-xs mt-1" style={{ color: T.mut }}>
              Detection surfaced no candidate rows. Re-upload the file to try again.
            </p>
            <button onClick={onReset} className="mt-3 px-4 py-2 text-sm" style={{ color: T.navy }}>Start over</button>
          </div>
        )}
      </Shell>
    );
  }

  return (
    <Shell load={load} onReset={onReset}>
      <ErrorBanner message={error} onDismiss={onDismissError} />
      {/* detection summary strip */}
      <div className="flex flex-wrap items-center gap-x-8 gap-y-4 px-5 py-4 mb-5"
        style={{ background: T.panel, border: `1px solid ${T.line}`, borderRadius: 8 }}>
        <Stat value={(load.rows_staged ?? 0).toLocaleString()} label="rows staged" />
        <div style={{ width: 1, height: 34, background: T.line }} />
        <div className="flex flex-col">
          <div className="flex items-center gap-2">
            <span style={{ fontFamily: MONO, fontSize: 20, fontWeight: 600, color: confColor[conf] }}>
              row {load.header_index}
            </span>
            <SignalMeter level={conf} />
          </div>
          <span className="text-xs mt-0.5" style={{ color: T.faint }}>header · {load.header_confidence} confidence</span>
        </div>
        <div style={{ width: 1, height: 34, background: T.line }} />
        <Stat value={load.source_encoding || "utf-8"} label="encoding" />
        <Stat value={load.delimiter === "," ? "comma" : (load.delimiter || ",")} label="delimiter" />
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
              {(load.columns || []).map((col, i) => {
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
            {(load.preview || []).map((row, r) => (
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
          onClick={commit}
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
 * out and a CSV download of the rejects.
 * ================================================================== */
function QuarantinePanel({ quarantine, mappings, importId }) {
  const [open, setOpen] = useState(false);

  const fieldToCol = useMemo(() => {
    const m = {};
    (mappings || []).forEach((x) => { if (x.field) m[x.field] = x.source_index; });
    return m;
  }, [mappings]);

  const badColsFor = (reason) => {
    const bad = new Set();
    Object.keys(fieldToCol).forEach((f) => {
      if (reason && reason.includes(f + " ")) bad.add(fieldToCol[f]);
    });
    return bad;
  };
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
          <div className="flex items-center justify-between px-4 py-2.5" style={{ borderTop: `1px solid ${T.ground}` }}>
            <span className="text-xs" style={{ color: T.faint, lineHeight: 1.5 }}>
              Highlighted cells are what failed — empty, or a value that wouldn’t cast
              (e.g. <span style={{ fontFamily: MONO }}>P.O.A.</span> in a price).
            </span>
            <a href={quarantineCsvUrl(importId)} download
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium"
              style={{ color: T.navy, border: `1px solid ${T.line}`, borderRadius: 6, whiteSpace: "nowrap" }}>
              <Download size={13} /> Download rejects (CSV)
            </a>
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
  const filtered = (commit.rows_filtered_blank || 0) + (commit.rows_filtered_sections || 0) + (commit.rows_filtered_repeated_headers || 0);
  const endpoint = commit.endpoint || `/api/${load.import_id}`;

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
          hint={`${commit.rows_filtered_sections || 0} section · ${commit.rows_filtered_blank || 0} blank · ${commit.rows_filtered_repeated_headers || 0} repeat header`} />
      </div>

      {/* rejected rows, with the offending cell called out */}
      <QuarantinePanel quarantine={quarantine} mappings={mappings} importId={load.import_id} />

      {/* null-rate bars — the "is this sane" signal */}
      {commit.null_rates && Object.keys(commit.null_rates).length > 0 && (
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
      )}

      <div className="flex items-center justify-between px-5 py-4" style={{ background: T.navy, borderRadius: 8 }}>
        <div>
          <div style={{ color: T.navyInk, fontSize: 13, fontWeight: 600 }}>Query endpoint ready</div>
          <a href={endpoint} target="_blank" rel="noreferrer"
            style={{ fontFamily: MONO, fontSize: 12, color: "#AEBBCD", marginTop: 2, display: "inline-block" }}>
            {endpoint}
          </a>
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
          <Chip mono>{(load.import_id || "").slice(0, 8)}</Chip>
        </div>
      </div>
      <div className="max-w-5xl mx-auto px-6 py-7">{children}</div>
    </div>
  );
}

/* ================================================================== *
 * Root — real backend flow
 * ================================================================== */
export default function App() {
  const [screen, setScreen] = useState("pick"); // pick | review | result
  const [load, setLoad] = useState(null);
  const [candidates, setCandidates] = useState([]); // remembered across redetects
  const [commit, setCommit] = useState(null);
  const [quarantine, setQuarantine] = useState(null);
  const [committedMappings, setCommittedMappings] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  const dismissError = useCallback(() => setError(null), []);

  const pick = async (file) => {
    setBusy(true);
    setError(null);
    try {
      const res = await apiLoad(file);
      setLoad(res);
      setCandidates(res.header_candidates || []);
      setScreen("review");
    } catch (e) {
      setError(`Load failed: ${e.message}`);
    } finally {
      setBusy(false);
    }
  };

  // Redetection re-stages around the chosen header. DetectStructureWithHeader
  // doesn't recompute candidates, so keep the last non-empty set for a re-pick.
  const reheader = async (idx) => {
    setError(null);
    try {
      const res = await apiRedetect(load.import_id, idx);
      setLoad(res);
      if (res.header_candidates && res.header_candidates.length) {
        setCandidates(res.header_candidates);
      }
    } catch (e) {
      setError(`Re-detect failed: ${e.message}`);
      throw e;
    }
  };

  const doCommit = async (mappings) => {
    setError(null);
    try {
      const c = await apiCommit(load.import_id, mappings);
      setCommittedMappings(mappings);
      setCommit(c);
      // Quarantine exists only after commit; a read failure shouldn't block the
      // success screen, so treat it as "nothing to show".
      try {
        setQuarantine(await apiQuarantine(load.import_id));
      } catch {
        setQuarantine({ ok: true, columns: [], total: 0, rows: [] });
      }
      setScreen("result");
    } catch (e) {
      setError(`Commit failed: ${e.message}`);
      throw e;
    }
  };

  const reset = () => {
    setScreen("pick"); setLoad(null); setCandidates([]);
    setCommit(null); setQuarantine(null); setCommittedMappings(null); setError(null);
  };

  if (screen === "pick") {
    return (
      <div style={{ minHeight: "100vh", background: T.ground }}>
        <Uploader onFile={pick} busy={busy} error={error} onDismissError={dismissError} />
      </div>
    );
  }
  if (screen === "review") {
    return (
      <Review load={load} candidates={candidates} onCommit={doCommit} onReheader={reheader}
        onReset={reset} error={error} onDismissError={dismissError} />
    );
  }
  return <Result commit={commit} quarantine={quarantine} mappings={committedMappings} load={load} onReset={reset} />;
}
