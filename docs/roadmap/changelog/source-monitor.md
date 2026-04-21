# Changelog: R07 Deep Source Diagnostics + C04 Monitor Source Drilldown

## Scope
Implements the first vertical slice of source-health diagnostics across API, CLI, and monitor UI.

## Changes

### Backend

- **`crates/moraine-source-status`**
  - Added `glob` dependency for filesystem scanning.
  - New diagnostic queries:
    - `build_source_files_snapshot(cfg, source_name)` — combines on-disk glob results (size, mtime) with ClickHouse per-file checkpoint, raw-event count, and latest-error state.
    - `build_source_errors_snapshot(cfg, source_name, limit)` — queries `ingest_errors` for a specific source with a bounded limit.
    - `build_source_detail_snapshot(cfg, source_name)` now also reads `ingest_heartbeats` for runtime lag and watcher state, including heartbeat age, append-to-visible lag, watcher backend/error/reset counts, and reconcile/heartbeat cadence from config.
  - Both functions return partial results on query failure rather than blanket errors.
  - Added unit tests for glob behavior, file-size fallback, literal escaping, heartbeat lag classification, and source-detail warning classification.

- **`crates/moraine-monitor-core`**
  - New HTTP endpoints:
    - `GET /api/sources/:source`
    - `GET /api/sources/:source/files`
    - `GET /api/sources/:source/errors?limit=N`
  - Returns 404 when source is not in config, 503 on ClickHouse failure.
  - Source detail responses now keep the shared source summary and partial-query warnings localized to the selected source, with separate `query_error` and `runtime_query_error` surfaces.

- **`apps/moraine`**
  - New CLI subcommands:
    - `moraine sources files <source>` — per-file diagnostics table.
    - `moraine sources errors <source> [--limit N]` — recent errors table.
  - Supports Rich/Plain/Json output modes consistently with existing commands.
  - Added clap parser tests for both subcommands.

### Frontend

- **`web/monitor/src/lib/types/api.ts`**
  - Added typed runtime and warning models for source drilldown alongside `SourceDetailResponse`, `SourceFileRow`, `SourceFilesResponse`, `SourceErrorRow`, and `SourceErrorsResponse`.

- **`web/monitor/src/lib/api/client.ts`**
  - Added `fetchSourceDetail(source)`, `fetchSourceFiles(source)`, and `fetchSourceErrors(source, limit)`.

- **`web/monitor/src/lib/components/SourceDetail.svelte`** (new)
  - Detail panel with a compact source summary plus **Files** and **Errors** tabs.
  - Summary shows shared status, harness, format, watch root, glob, counts, latest checkpoint/error metadata, heartbeat age, append-to-visible lag, watcher health, and configured reconcile cadence.
  - Warning cards distinguish file-state problems from ingest-heartbeat lag and watcher degradation without redesigning the panel.
  - Files tab: sticky-header table with path, size, modified time, raw events, checkpoint offset, status.
  - Errors tab: error cards with time, kind, file, text, and optional raw fragment.
  - Close button now invalidates in-flight loads so stale drilldown results do not repopulate a dismissed panel.

- **`web/monitor/src/lib/components/SourcesStrip.svelte`**
  - Source chips are now clickable buttons (disabled for error/none states).
  - Emits `select` event with source name.

- **`web/monitor/src/App.svelte`**
  - Wires `SourcesStrip` `select` event to `selectedSource` state.
  - Renders `SourceDetail` bound to `selectedSource`.

## Validation

- `cargo test --workspace --locked` — passed
- `cargo build --workspace` — passed
- `cargo fmt --all -- --check` — passed
- `cargo clippy -p moraine-source-status -p moraine-monitor-core -p moraine-monitor -p moraine --all-targets -- -D warnings` — passed
- `bun run typecheck` — 0 errors, 0 warnings
- `bun run build` — built successfully
- `bun run test` — 12 passed

## Operational Notes

- No schema migrations required; uses existing `ingest_checkpoints`, `raw_events`, `ingest_errors`, and `ingest_heartbeats` tables.
- Runtime drilldown reads `ingest_heartbeats` opportunistically and falls back to legacy heartbeat columns when watcher metrics are unavailable.
- Filesystem scan uses the source's configured `glob` pattern directly, so results reflect current on-disk state.
- Partial failures (ClickHouse down, permission denied during glob, heartbeat query mismatch) surface warnings in the response rather than failing the entire request.
