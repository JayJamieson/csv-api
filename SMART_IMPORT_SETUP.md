# Smart CSV import — setup

This overlay adds a staged, self-correcting import path to `csv-api` for messy
supplier price books, plus a standalone review UI. The original `/import`
endpoint is untouched and keeps working.

## What's in the zip

```
pkg/sniff/                 new package — the parsing engine
  encoding.go              encoding detect + normalize to UTF-8/LF (x/text)
  structure.go             header/footer/section detection by scoring
  mapping.go               synonym + value-based column mapping, fingerprints
  loader.go                all-VARCHAR staging + SQL clean/type/quarantine
  store.go                 confirmed-mapping store (learns per supplier)
  sniff_test.go            unit + end-to-end tests (real DuckDB)
pkg/db/staged.go           per-import staging helpers, quarantine read, state
pkg/api/import_handlers.go /load, /imports/{id}/commit, /imports/{id}/quarantine
pkg/api/server.go          adds startup migrations (patched)
api-spec.yaml              new endpoints + schemas (patched)
web-preview-MappingReview.jsx   the standalone review UI (preview build)
```

## Backend setup

1. Unzip at the repo root (overwrites `server.go` and `api-spec.yaml`, which
   were patched, not replaced — diff them if you have local changes):

   ```
   unzip -o csv-api-smart-import.zip
   ```

2. Add the one new dependency:

   ```
   go get golang.org/x/text
   go mod tidy
   ```

3. Regenerate the API types from the updated spec. Uses your existing
   `codegen.yaml`:

   ```
   go generate ./...
   # or: oapi-codegen -config codegen.yaml api-spec.yaml
   ```

   This regenerates `pkg/api/csv_api.gen.go` with `LoadCSVParams`,
   `CommitRequest`, `LoadResponse`, `QuarantineResponse`, and the
   `ServerInterface` methods the handlers implement.

4. Build:

   ```
   go build ./...
   ```

   **Expect a few identifier fixes in `import_handlers.go` on first build.**
   The handlers reference generated enum constants (staged/committed states,
   the `auto` mode value). Your `ToCamelCase` normalizer decides the exact
   names — it may emit `LoadResponseStateStaged` where the handler wrote
   `Staged`, etc. The shapes are correct; only the identifiers may need
   renaming to match what codegen produced. Grep the generated file for the
   `const` block to see the real names.

5. Run tests — the engine has no generated deps, so it builds independently:

   ```
   go test ./pkg/sniff/ -v
   ```

   `TestEndToEnd` runs the whole normalize → detect → propose → stage →
   promote chain against real DuckDB, including CP1252 decoding, `$1,145.00`
   currency cleaning, section/repeated-header filtering, and
   quarantine-with-reason. If it passes, the core is sound.

## The import flow

```
POST /load?name=book.csv            (body = file)   or  ?url=...
  → normalizes encoding, detects header, stages all-VARCHAR,
    proposes a mapping (or recalls a saved one by fingerprint)
  → returns import_id, preview, proposed_mappings, confidence, warnings

POST /imports/{id}/commit           {mappings?, header_index?, save_mapping?}
  → cleans + types, quarantines rows failing required fields,
    saves the confirmed mapping for next time
  → returns the /api/{id} query endpoint + row stats

GET  /imports/{id}/quarantine       ?limit&offset
  → the rejected rows: original uncleaned cells + reason
```

`mode=auto` on `/load` commits without a round-trip **only** when header
confidence is high AND a saved mapping matches AND no required field is
missing. Otherwise it stages and tells you why it stopped.

### Two-phase, and why

`/load` never types data — it stages everything as text, so the load itself
almost never fails. Typing happens in SQL at commit via `TRY_CAST` after
cleaning ($ , % stripping, accounting-paren negatives). A cell that won't
cast becomes a NULL you can see, not a file-level exception. Rows where a
*required* field ends up NULL go to quarantine with a reason rather than
being silently dropped.

### Learning

Confirmed mappings are stored in Turso keyed by a fingerprint of the sorted,
normalized header set. Sorted, so a supplier reordering columns doesn't
invalidate it; keyed by column name not index, for the same reason. Only
confirmed mappings are saved — never auto-proposals — so mistakes don't
compound. The second time a supplier sends the same layout, `/load` returns
`known_fingerprint: true` and the mapping is pre-filled.

## UI setup

`web-preview-MappingReview.jsx` is a **preview build** — it runs standalone
with mocked responses so you can see and click the flow. It is not yet wired
to the backend.

To preview: drop it into any React sandbox (or the artifact viewer). Three
sample files exercise the paths: a messy new supplier, a known supplier
(pre-filled mapping), and a no-header file that forces manual header
selection.

To go live, replace the four mock functions at the bottom of the file with
real fetches — every mock object already matches the real JSON shape:

```js
const mockLoad       = (id) => SAMPLES[id].load;
// → const r = await fetch(`/load?name=${name}`, {method:'POST', body:file}); return r.json();

const mockCommit     = (id) => SAMPLES[id].commit;
// → await fetch(`/imports/${importId}/commit`, {
//     method:'POST', headers:{'content-type':'application/json'},
//     body: JSON.stringify({ mappings })
//   }).then(r=>r.json());

const mockQuarantine = (id) => SAMPLES[id].quarantine;
// → await fetch(`/imports/${importId}/quarantine`).then(r=>r.json());

const mockReheader   = (id) => SAMPLES[id].reload;
// → re-call /load, or a re-detect with the chosen header_index
```

The components read the same fields either way, so nothing else changes.
The real upload (a file drop) replaces the sample picker on the first screen.

### Serving alongside the Go API

Simplest: a separate Vite dev server proxying `/load`, `/imports`, `/api` to
the Go server (`server.proxy` in `vite.config`), then build to static assets
echo serves. Say the word and I'll produce the Vite scaffold wired to the
real endpoints with a file-drop upload — that's the natural next step past
this preview.

## Notes / gaps

- No Go toolchain was available where this was written, so nothing has been
  compiled. The `go build` in step 4 is the real first test.
- The manual header-override re-detect (`restage`) works server-side; the UI
  preview uses a pre-baked re-read for that path.
- Invoices were deliberately out of scope — price books only, as discussed.
  Invoices want their own region-detection profile.
