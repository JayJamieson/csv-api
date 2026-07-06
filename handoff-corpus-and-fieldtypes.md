# Hand-off addendum: corpus scan + extensible field types

This extends `HANDOFF.md`. It covers two requests that are really
**one workstream**: a batch scan of ~3.6GB of price books that *produces*
evidence, and a data-driven field-type system that *consumes* it. Build them in
that order.

Context you (Claude Code) have that the sandbox did not: the whole thing is
built and running locally, and there are real files exhibiting columns that map
to no canonical field — and for which the manual mapping UI offers nothing
sensible either. That second half is the actual pain point. The corpus scan
exists to tell you exactly which field types are missing so you can add them.

The sandbox could not run any of this, so specifics below (struct shapes,
thresholds, table DDL) are a worked proposal, not tested code. Treat them as a
design to implement and adjust, not paste.

---

## Part A — corpus scan CLI (`cmd/corpus`)

### Why a CLI, not the API
Recommended: a standalone Go command reusing `pkg/sniff` directly. Reasons:
the scan is **read-only** (detect + propose, never commit, never write Turso),
running 3.6GB through the HTTP server would spool and stage every file for no
reason, and direct calls avoid per-file HTTP overhead across what is likely
thousands of files. The API path is for real imports; the corpus scan is pure
measurement. Do **not** route it through `/load`.

### What it does per file
For every `*.csv` (and `.txt`/`.tsv`) under a root dir:

1. `sniff.Normalize` → capture `SourceEncoding` (this alone will surface how
   much of the corpus is CP1252/UTF-16 — useful).
2. `sniff.DetectStructure` → header index, confidence, delimiter, row-kind
   histogram, section samples.
3. If a header was found: `sniff.ProposeMapping` over a sample of staged rows.
   You can stage into an **in-memory DuckDB** (`sql.Open("duckdb", "")`) per
   file, or — cheaper — reuse the `sampleRows` output directly and skip DuckDB
   entirely for the scan, since proposal only needs sampled values, not a
   staged table. Prefer the no-DuckDB path for throughput; only stage if you
   also want per-file typing/quarantine stats in the report.
4. Record every column and its proposed field (or **unmapped**), with the
   normalized header name and a few sample values.

Wrap each file in recover/timeout so one pathological file can't kill the run.
Bound concurrency with a worker pool (`GOMAXPROCS`-ish); 3.6GB is fine
single-box but parallel across files is a big win. Stream output — do not hold
all results in memory.

### Output: both aggregate + per-file detail
Two artifacts:

**`corpus-detail.jsonl`** — one JSON object per file:
```json
{"file":"reece/2026-07/pricebook.csv","encoding":"windows-1252","delimiter":",",
 "header_index":4,"confidence":"medium","rows_sampled":200,
 "columns":[{"raw":"Nett EA","norm":"nett_ea","proposed":"","samples":["1.85","2.40"]},
            {"raw":"Pack","norm":"pack","proposed":"uom","samples":["EA","BX"]}],
 "warnings":[]}
```

**`corpus-report.md` / `.json`** — the aggregate that seeds the fixes:

- **Recurring confirmed-style mappings**: `(normalized header → proposed field)`
  ranked by file count. Your "most common mappings" output.
- **Missing-synonym candidates**: normalized headers that were **unmapped** but
  whose *sample values* strongly match an existing field's value classifier.
  E.g. `nett_ea` unmapped by name, but its values are all currency → almost
  certainly `price`. These are one-line additions to the `synonyms` map. Rank by
  frequency; this is the highest-ROI list.
- **New-field candidates**: unmapped headers whose values match **no** existing
  classifier, clustered by normalized name similarity. E.g. many files with a
  `pack_qty`/`inner_qty`/`ctn_qty` column that is integer-valued and isn't
  `qty_break`. Each cluster is a candidate **new canonical field**. This is the
  list that feeds Part B.
- **Error catalogue**: files by failure mode — no header found (low
  confidence), delimiter ambiguous, encoding exotic, zero data rows, header
  field-count vs modal mismatch. Counts + example file paths.
- **Confidence distribution**: how many files land high/medium/low. Tells you
  how much manual review a real bulk import would need.

### Suggested shape
```
cmd/corpus/main.go        flags: -root, -out, -workers, -stage(bool), -limit
                          walks root, fans out to workers, streams JSONL,
                          accumulates aggregate counters behind a mutex,
                          writes report at the end.
pkg/sniff/corpus.go       ScanFile(path) (FileReport, error) — the per-file
                          logic, so it's unit-testable without the CLI.
```
Keep `ScanFile` in `pkg/sniff` (reuses internals like `sampleRows`,
`valueScore`); keep walking/pooling/output in `cmd/corpus`.

### The value-signal for missing synonyms (important)
The reason a column is "unmapped but obviously a price" is that
`ProposeMapping` combines name + value evidence, and an unrecognized header
contributes zero name score. `valueScore` already computes the value match per
field. For the report, run `valueScore` for **every** field against each
unmapped column and record the best-scoring field above ~0.6. That is precisely
your "missing synonym" suggestion: "header `nett_ea` looks like `price` (value
score 0.94) — add it to the synonyms map." You already have the function; the
scan just applies it in the unmapped case and reports it instead of discarding.

---

## Part B — extensible field types (the real fix)

Today `CanonicalField` is a hardcoded Go enum (`pkg/sniff/mapping.go`), the
`synonyms` map is a compiled literal, and `requiredFields`/`claimOrder` are
`var` slices. That is why the manual UI "has no option that makes sense" — the
dropdown can only offer compiled-in fields. Make field types **data**.

### Data model (Turso)
```sql
CREATE TABLE field_type (
  key         TEXT PRIMARY KEY,     -- 'pack_qty'
  label       TEXT NOT NULL,        -- 'Pack quantity'
  value_kind  TEXT NOT NULL,        -- 'text'|'currency'|'integer'|'barcode'|'uom'|'date'
  required    INTEGER NOT NULL DEFAULT 0,
  claim_order INTEGER NOT NULL DEFAULT 100,
  builtin     INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL
);
CREATE TABLE field_synonym (
  field_key   TEXT NOT NULL REFERENCES field_type(key),
  synonym     TEXT NOT NULL,        -- normalized fragment, e.g. 'nett_ea'
  source      TEXT,                 -- 'builtin'|'corpus'|'user'
  PRIMARY KEY (field_key, synonym)
);
```
Seed `field_type` + `field_synonym` from the current hardcoded values on first
migrate (`builtin=1`). Nothing about existing behavior changes at seed time —
this is a refactor from literals to a loaded registry.

### Refactor `pkg/sniff/mapping.go`
- Introduce a `FieldRegistry` struct holding the loaded field types + synonym
  index + the value classifiers keyed by `value_kind` (not by hardcoded field).
  The regexes/vocab in `mapping.go` (`currencyRe`, `skuValueRe`, `uomVocab`,
  etc.) become classifiers indexed by `value_kind`, so a new field just declares
  which kind it is and reuses an existing classifier.
- `ProposeMapping` takes a `*FieldRegistry` instead of reading package globals.
  `synonyms`, `claimOrder`, `requiredFields` all come from the registry.
- `CanonicalField` stays a string type, but its valid set is now runtime.
  `FieldUnknown` ("") stays.
- **Back-compat:** provide a `DefaultRegistry()` built from the current literals
  so existing tests pass unchanged; the DB-backed registry is loaded in the
  server/CLI and passed in.

### New value_kind = "text" gives you passthrough for free
A field with `value_kind='text'` types as VARCHAR and never fails a cast, so
"keep this column as-is" is just mapping it to a text field (built-in generic
one, or a user-named one like `notes`). No separate passthrough mode needed —
which is why the "allow adding a mapping type" answer subsumes the
passthrough/drop/block question. Drop is still available implicitly (leave a
column unmapped → it's excluded from typed output). Nothing is forced.

### API additions
```
GET  /field-types                    → list for the manual mapping dropdown
POST /field-types {key,label,value_kind,required?}
                                     → define a new field on the spot
POST /field-types/{key}/synonyms {synonym}
                                     → teach a synonym (also used to accept a
                                        corpus suggestion in bulk)
```
The UI's mapping dropdown fetches `/field-types` instead of a hardcoded list.
When a user hits a column with no good option, an "+ add field type" affordance
opens a small form (key, label, value kind) → POST → the new field is
immediately selectable. That directly closes the "no sensible option" gap.

### Closing the loop with the corpus
The corpus `new-field candidates` list is the pre-filled backlog for
`POST /field-types`, and the `missing-synonym candidates` list is a batch of
`POST /field-types/{key}/synonyms`. Consider a `-emit-suggestions` flag on the
CLI that writes a JSON file of proposed field types + synonyms, and a small
`corpus apply` path (or an admin endpoint) to review-and-accept them. Keep
acceptance **human-gated** — same invariant as saved mappings: never auto-learn
synonyms from unconfirmed guesses, or you'll poison the dictionary with the
scan's own mistakes.

---

## Guessed-at unknowns to confirm locally
- **Throughput / DuckDB per file**: I'm guessing the no-DuckDB scan path
  (proposal from `sampleRows` values only) is enough for the report and much
  faster. Confirm proposal quality is acceptable without staging; if you want
  per-file quarantine counts too, add an opt-in `-stage` flag.
- **File discovery**: guessing flat-ish `*.csv`. Real corpus may be zipped, or
  `.xls`/`.xlsx` (which `pkg/sniff` does **not** handle — it's CSV-only).
  If there are spreadsheets, that's a separate converter step (ssconvert/
  libreoffice headless, or a Go xlsx reader) before the scan. Report and ask.
- **Encoding surprises at scale**: the CP1252 fallback is by design "never
  fails, sometimes wrong". At 3.6GB you may find UTF-16 without BOM or genuine
  Latin-1 vs CP1252 edge cases. The detail JSONL's `encoding` field is your
  audit trail; spot-check a sample.
- **value_kind coverage**: current classifiers are text/currency/integer/
  barcode/uom/date-ish. The corpus may demand new kinds (percentage,
  boolean/GST-flag, effective-date ranges). Add classifiers as kinds, not as
  per-field special cases.
- **Registry threading**: every call site of `ProposeMapping` (handlers, tests,
  the new CLI) must pass a registry. Grep for it before refactoring so nothing
  silently keeps using a stale global.

## Sequence
```
[ ] cmd/corpus + pkg/sniff/corpus.go (ScanFile), no DB writes
[ ] run against a small subdir first; sanity-check the report
[ ] run full 3.6GB; review missing-synonym + new-field candidates
[ ] field_type/field_synonym tables + seed from current literals
[ ] refactor mapping.go to FieldRegistry; DefaultRegistry() for test back-compat
[ ] thread registry through handlers + CLI
[ ] /field-types endpoints; UI dropdown reads them + "add field type" form
[ ] accept corpus suggestions (human-gated) to backfill synonyms/fields
[ ] re-run corpus; confirm unmapped-column rate dropped
```

The success metric is measurable and worth capturing: unmapped-column rate on
the corpus before vs after adding the field types and synonyms the first scan
surfaced. That number is the whole point.
