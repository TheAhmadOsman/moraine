# Source File Runtime Detail

## Scope
This slice extends the existing source drilldown so `/api/sources/:source/files` and the monitor UI can answer which exact files are stale, missing on disk, still erroring, or currently showing SQLite WAL sidecars.

## Changes

### `crates/moraine-source-status/src/lib.rs`
- Expanded `SourceFileRow` with additive per-file runtime detail:
  - disk presence and modified age
  - checkpoint timestamp and age
  - latest raw-event timestamp and age
  - latest ingest-error timestamp and age
  - heuristic SQLite sidecar visibility for `opencode_sqlite` base `.db` files
  - additive `issues` classification and `stale_reason`
- Tightened file-level ClickHouse queries so checkpoint rows use the latest observed state per `source_file`.
- Included paths that still have raw rows or ingest errors even if no checkpoint row exists, so missing historical files remain visible in drilldown.
- Added focused tests for file issue classification, missing-on-disk rows, SQLite sidecar visibility, and stale-file reasoning.

### `web/monitor/src/lib/types/api.ts`
- Updated the drilldown API types to match the expanded per-file payload.

### `web/monitor/src/lib/components/SourceDetail.svelte`
- Reworked the existing files table to surface per-file state badges, disk presence, raw/checkpoint recency, latest error metadata, and SQLite WAL/SHM visibility without requiring JSON inspection.
- Added row highlighting so missing, stale, and erroring files stand out immediately.

### `docs/operations/source-health-and-monitor.md`
- Documented the richer `/api/sources/:source/files` response and the SQLite sidecar heuristic.

## Validation
```bash
cargo fmt --all
cargo test -p moraine-source-status -p moraine-monitor-core --locked
cd web/monitor && bun install --frozen-lockfile && bun run build
```

## Operational impact
- No schema migrations or config changes.
- Source drilldown is more actionable during ingest triage, especially for OpenCode SQLite sources where `.db-wal` and `.db-shm` siblings matter.
