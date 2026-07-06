# Hand-off: smart CSV import overlay

You (Claude Code, running locally with a real Go toolchain) are picking up work
that was drafted in a sandbox **without a Go compiler**. Nothing here has been
built or run. Your job is to verify it compiles, make it pass its own tests, and
finish the integration. This doc tells you what exists, what is known-uncertain,
what to check, and — importantly — which design decisions must **not** be
"fixed" away because they are deliberate.

Repo: `csv-api` (Go, Echo, oapi-codegen strict=false, DuckDB per-import files,
Turso/libsql metadata). Branch: work on a feature branch, not main.

---

## 1. What was added, and why

The original `POST /import` does a one-shot `read_csv_auto(strict_mode=false)`
with a user-supplied column mapping. It fails on the things real supplier price
books do: preamble junk above the header, section headings mid-table, repeated
page headers, `$1,234.50` prices, encoding from legacy Excel. This overlay adds
a **staged, two-phase, self-correcting** import path next to it. The original
endpoint is untouched.

New package `pkg/sniff`:

| file | responsibility |
|------|----------------|
| `encoding.go` | detect encoding (BOM, UTF-16 heuristic, UTF-8 validate, CP1252 fallback), normalize to UTF-8/LF, strip BOM/NULs. Uses `golang.org/x/text`. |
| `structure.go` | sample the head, score every row for header-likeness, classify rows (preamble/header/data/section/repeated/blank). `DetectStructureWithHeader` is the manual-override path. |
| `mapping.go` | propose canonical-field mapping from header synonyms **and** value classifiers; `Fingerprint` hashes the sorted header set. |
| `loader.go` | `LoadStaging` (all-VARCHAR staging) + `Promote` (SQL clean/type via `TRY_CAST`, split typed vs quarantine). |
| `store.go` | `MappingStore` — persists **confirmed** mappings per fingerprint in Turso. |
| `sniff_test.go` | unit + end-to-end tests against real DuckDB. |

Support + wiring:

| file | change |
|------|--------|
| `pkg/db/staged.go` | **new.** Staging helpers, `QuarantineRows`, import-state persistence, `RegisterTable`. |
| `pkg/api/import_handlers.go` | **new.** `LoadCSV`, `CommitImport`, `GetQuarantine`. |
| `pkg/api/server.go` | **patched.** Adds startup migrations + `sniff` import. |
| `api-spec.yaml` | **patched.** New paths/schemas: `/load`, `/imports/{id}/commit`, `/imports/{id}/quarantine`. |

---

## 2. Verification sequence (do this first, in order)

```bash
go get golang.org/x/text
go mod tidy
go generate ./...          # regenerates pkg/api/csv_api.gen.go from api-spec.yaml
go build ./...
go test ./pkg/sniff/ -v
go test ./...
```

Expected outcome and how to react at each step below.

### 2a. `go generate` — regenerate the API types

Config is `codegen.yaml` (already present): `package: api`,
`name-normalizer: ToCamelCase`, `strict-server: false`, echo-server, models,
embedded-spec. The directive lives in `generate.go`:
`go tool oapi-codegen --config=codegen.yaml api-spec.yaml`.

This regenerates request/response types and the `ServerInterface` the new
handlers implement: `LoadCSVParams`, `CommitRequest`, `CommitResponse`,
`LoadResponse`, `HeaderCandidate`, `FieldMapping`, `QuarantineResponse`,
`QuarantineRow`, `GetQuarantineParams`, and `ServerInterface` methods
`LoadCSV`, `CommitImport`, `GetQuarantine`.

If generation errors, the spec is malformed — it parses as YAML (verified) but
oapi-codegen is stricter. Fix the spec, not the generated file.

### 2b. `go build ./...` — THE most likely place for real fixes

**Known-uncertain: generated enum constant names.** The handlers reference enum
values whose exact Go identifiers are decided by the `ToCamelCase` normalizer,
which the sandbox could not run. Specifically these, in `import_handlers.go`:

- `Staged`, `Committed` — from `LoadResponse.state` enum `[staged, committed]`
- `Auto` — from `LoadCSVParams.mode` enum `[manual, auto]`
- `LoadResponseHeaderConfidence(...)` — cast around the `header_confidence` enum `[high, medium, low]`

`ToCamelCase` may instead emit prefixed names like `LoadResponseStateStaged`,
`LoadResponseStateCommitted`, `LoadCSVParamsModeAuto`, and a type named
`LoadResponseHeaderConfidence` with constants `...ConfidenceHigh` etc.

**To fix:** after `go generate`, grep the generated file for the real names and
reconcile:

```bash
grep -nE 'Staged|Committed|ModeAuto|ModeManual|ConfidenceHigh|ConfidenceMedium|ConfidenceLow' pkg/api/csv_api.gen.go
```

Then update the three references in `import_handlers.go` to match. These are
pure renames — the shapes and logic are correct. Do not restructure the
handlers to work around a name; just use the generated identifier.

**Other build items to check:**

- `LoadResponse` field access in the handler (`resp.HeaderCandidates`,
  `resp.ProposedMappings`, etc.) assumes non-pointer slices and scalar fields.
  With `prefer-skip-optional-pointer: true` most optional scalars are
  value types, but confirm a couple (e.g. `resp.Endpoint`,
  `resp.AutoCommitBlockedReason`) aren't generated as `*string`. If they are,
  either add `omitempty`-friendly assignment or adjust the spec to make them
  required-with-default. Prefer fixing at the assignment site.
- `CommitRequest.HeaderIndex` and `.SaveMapping` are read as pointers
  (`*int`, `*bool`) in the handler — that matches optional fields. Confirm.
- `types.UUID` import path is `github.com/oapi-codegen/runtime/types`. If the
  generated code uses `openapi_types` alias, align the handler import.

### 2c. `go test ./pkg/sniff/ -v` — the engine has NO generated deps

This is the highest-signal check and independent of the API layer. It should
build and run even if the handlers still have naming issues.

`sniff_test.go` imports the DuckDB driver as
`github.com/marcboeker/go-duckdb/v2`. **Check the actual driver import used
elsewhere in the repo** (`grep -rn duckdb go.mod pkg/db/db.go`). The user's
memory/preferences note a move to `github.com/duckdb/duckdb-go/v2` at v2.5.0 —
if the repo uses that, update the test's blank import to match. The driver
name registered with `database/sql` (`sql.Open("duckdb", ...)`) should be the
same either way, so only the import path changes.

Key cases and what they prove:
- `TestDetectEncoding`, `TestNormalize` — CP1252/UTF-16/BOM handling, NUL strip, CRLF.
- `TestDetectStructure` — the "cursed price book": header at row 3 under a
  2-line preamble, `COPPER FITTINGS` section rows, a repeated header mid-file.
- `TestDetectStructureNoHeader` — pure data returns **low** confidence (must
  not confidently guess).
- `TestProposeMapping`, `TestProposeMappingValuesBeatBadHeaders` — values
  override lying headers.
- `TestEndToEnd` — full chain: normalize CP1252 → detect → propose → stage →
  promote, asserting `$1,145.00` cleans to `1145.0`, one row quarantined with a
  reason containing `price`, and section/repeat rows filtered.

If a test fails, first decide whether the **test expectation** or the **code**
is wrong — the scoring thresholds in `structure.go` (`MinHeaderScore=0.45`,
`RivalMargin=0.15`) and the mapping weights in `mapping.go` (name 0.45 / value
0.55, name-only cap 0.60) are tuned by reasoning, not measurement. If real
detection is close but off, adjusting a threshold is legitimate; document any
change and re-run. Do **not** loosen a threshold just to make
`TestDetectStructureNoHeader` pass by letting it guess — that test guards a real
safety property.

---

## 3. Design invariants — do NOT "fix" these away

These look like they could be simplified or "corrected". They are deliberate.
Changing them reintroduces the bugs the overlay exists to prevent.

1. **Staging is all-VARCHAR.** `LoadStaging` loads every column as text so the
   load itself can't fail on type inference. Typing happens later in SQL. Do
   not add type inference to the staging read.
2. **Quarantine keeps the ORIGINAL raw row, not the cleaned one.** `Promote`
   joins back to staging so the quarantine table shows what actually failed
   (`P.O.A.` in a price cell), not a NULL. Do not simplify it to select from the
   cleaned view.
3. **`DropStagingArtifacts(ctx, id, false)` at commit keeps quarantine.** The
   `false` is "don't drop quarantine". `GET /imports/{id}/quarantine` depends on
   it surviving. Don't change it to drop everything.
4. **Only confirmed mappings are persisted.** `commitStaged` saves to
   `MappingStore` only on commit (human-confirmed or auto-committed under high
   confidence). Never persist auto-proposals from `/load` — that would compound
   detection errors into the learned set.
5. **Fingerprint hashes the SORTED header set; saved mappings key by column
   NAME not index.** So a supplier reordering columns doesn't invalidate a
   learned mapping. `ApplySaved` returns `false` if a name is missing, falling
   back to proposal. Keep both properties.
6. **`mode=auto` gates are AND, and each refusal names itself.** High confidence
   AND known fingerprint AND no missing required field. If any fails, it stages
   and sets `auto_commit_blocked_reason`. Don't relax to OR.
7. **Regex is intentionally left in the Go sample path.** It runs over ~200
   sampled rows only. Do not spend effort converting to codegen'd DFAs (this was
   discussed and deferred). The full-file passes are DuckDB SQL, not Go regex.

---

## 4. Integration checks against existing code (verified in sandbox)

These were confirmed by reading `pkg/db/db.go`, so they should hold — but
re-verify after any refactor:

- `getDuckDBConnection(id)` caches connections in `duckDBMap` keyed by id, one
  `.db` file per import at `dataDir/<id>.db`. Staging and commit reuse the same
  file because they pass the same import id. ✔
- `csv_table.id` is `PRIMARY KEY`, so `RegisterTable`'s `ON CONFLICT(id) DO
  UPDATE` is valid. ✔
- `RegisterTable` writes `table_name = "csv_data"` (const `db.TypedTable`), the
  **same** name `ImportCSVFromReader` uses, so the existing `GetCSV` /
  `QueryCSVTable` / `/api/{id}` query path serves smart-imported tables with
  zero changes. ✔ Confirm `GetCSV` reads `table_name` from `csv_table` rather
  than hardcoding — if it hardcodes `csv_data` it still works; if it reads the
  column it also works.
- `Server` struct has a `db` field of type `*db.DB` and `db` exposes `Meta()`
  (Turso handle) and `StagedDB(id)` (DuckDB handle). Confirm `Server.db` is that
  type and the field name matches what the handlers use (`h.db`).

**One thing to actively verify:** the `_row` column. `LoadStaging` adds
`row_number() OVER () AS _row`. Under `all_varchar=true` this may come back as
VARCHAR or BIGINT depending on DuckDB version. `QuarantineRows` parses it with
`fmt.Sscanf(raw[0].String, "%d", ...)` assuming text. If the driver returns it
as int64, the `sql.NullString` scan will still work (DuckDB→string coercion) but
confirm the quarantine `row` numbers come out correct in a real run.

---

## 5. Remaining integration work (after it builds + tests pass)

### 5a. Wire the UI to the backend
`web-preview-MappingReview.jsx` is a standalone preview with mocked responses
shaped exactly like the real JSON. It is **not** wired. Four functions at the
bottom of the file are the seam:

```
mockLoad       → POST /load?name=<f>            (body = file)
mockCommit     → POST /imports/{id}/commit      (body = {mappings})
mockQuarantine → GET  /imports/{id}/quarantine
mockReheader   → re-call /load or a re-detect with header_index
```

The user wants this to run **alongside** the Go API. Preferred approach not yet
chosen — ask, or default to a Vite dev server proxying `/load`, `/imports`,
`/api` to the Go server, built to static assets Echo serves. The real file-drop
upload replaces the sample picker on screen 1.

### 5b. `restage` / manual-header re-detect endpoint
`CommitImport` handles a `header_index` override by calling `restage`, which
re-runs `DetectStructureWithHeader` and reloads staging. That works server-side.
But the UI's "change header row" needs to see the **new** proposed mapping
before committing. Two options: (a) a dedicated `POST /imports/{id}/redetect`
that returns a fresh `LoadResponse`, or (b) have the UI re-call `/load`. Decide
with the user; (a) is cleaner because the spool file is already staged and
re-downloading a URL could fetch a changed file.

### 5c. Spool + state lifecycle
Staged imports write a spool file to `os.TempDir()/csvapi-spool/<id>.csv` and a
row in `import_state` (Turso). Commit deletes both. **Un-committed imports leak
both.** Add a sweeper (cron or startup) that deletes `import_state` rows and
spool files older than N hours, and drops their staging `.db` files. Not built.

### 5d. Quarantine export
UI shows quarantine rows. Users will want "download rejects as CSV to fix and
re-import". `QuarantineRows` already returns everything needed; add a
`?format=csv` branch or a sibling endpoint. Not built.

---

## 6. Test coverage status

- `pkg/sniff`: unit + e2e present. **Handlers and `pkg/db/staged.go` have no
  tests yet.** After the build is green, add: a handler test for the
  `/load`→`/commit` happy path (httptest + a temp DuckDB/Turso), a
  low-confidence `/load` returning candidates with no staging, and a
  `/quarantine` read. These need the real generated types, which is why they
  weren't drafted blind.

---

## 7. Summary checklist

```
[ ] go get golang.org/x/text && go mod tidy
[ ] confirm DuckDB driver import path (marcboeker vs duckdb/duckdb-go) & align test
[ ] go generate ./...
[ ] reconcile enum constant names in import_handlers.go with generated file
[ ] go build ./...  (fix pointer-vs-value field access if any)
[ ] go test ./pkg/sniff/ -v   (all green; adjust thresholds only with justification)
[ ] go test ./...
[ ] manual smoke: POST /load a messy CSV, inspect JSON; POST commit; GET quarantine
[ ] verify /api/{id} serves the committed typed table
[ ] decide UI serving model with user; wire the 4 fetch seams
[ ] add: redetect endpoint, spool/state sweeper, quarantine CSV export
[ ] add handler + db tests
```

Do not treat a passing build as done — the value is in a real messy file going
in one end and a clean typed table plus an inspectable quarantine coming out the
other. Run that path manually before calling it integrated.
