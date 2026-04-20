# OpenCode Adapter Hardening + Fixture/Source Simulator Improvements

## Scope
This slice implements R08 (OpenCode adapter hardening) and C16 (fixture/source simulator improvements) from the April 20, 2026 roadmap. The disk-backed ingest retry spool (R06) was explored but reverted to avoid cross-crate config and workspace changes; it is documented here as a follow-up.

## Changes

### `crates/moraine-ingest-core/src/dispatch.rs`
- **Schema drift diagnostics:** `validate_opencode_sqlite_schema` now reports `observed_tables=[...]` and `observed_columns=[...]` in every schema-mismatch error, making upstream drift actionable without manual SQLite inspection.
- **Fixture generators (test-only):**
  - `create_opencode_schema_with_extra_columns` — generates a forward-compatible schema with extra columns on all three tables.
  - `create_opencode_schema` / `insert_opencode_fixture_rows` — existing helpers reused for empty-table and WAL-mode tests.
- **New tests:**
  - `process_opencode_sqlite_tolerates_extra_columns` — proves the adapter accepts schemas that add columns beyond the required set.
  - `process_opencode_sqlite_empty_tables_is_noop` — proves zero-row databases process cleanly with no batch emission.
  - `process_opencode_sqlite_with_wal_mode_reads_successfully` — proves the scanner tolerates SQLite databases in WAL mode (the watcher layer already maps `.db-wal`/`.db-shm` events to the parent `.db` path).
  - Updated `process_opencode_sqlite_reports_schema_drift_with_user_version` to assert that `observed_tables=` and `observed_columns=` appear in the error text.

### `fixtures/opencode/schema_matrix.json`
- New fixture documenting the required table/column contract, example compatibility statuses, and named corruption modes for future test expansion.

### `docs/core/ingestion-service.md`
- OpenCode section updated to note that schema drift errors now include observed table and column lists.

## Rejected / Follow-up
- **R06 disk-backed ingest retry spool:** A full `spool.rs` module and sink integration were prototyped, including startup replay, failure spooling, shutdown spool, and exponential backoff. This was reverted because it required:
  - Adding `Serialize`/`Deserialize` to `RowBatch`
  - Adding `rand` and `chrono/serde` dependencies to `moraine-ingest-core`
  - Modifying `spawn_sink_task` signature and `lib.rs` caller
  - Updating `Cargo.lock`
  
  A clean R06 implementation should be done as a standalone PR that can also add config fields (or use hardcoded paths) without mixing with adapter work. The existing test in `sink.rs` (`failed_flush_throttles_sink_consumption`) documents current in-memory retry behavior.

## Validation
```bash
cargo test -p moraine-ingest-core --offline
# 71 tests passed (58 lib + 13 integration/fixture)

cargo fmt --all -- --check
# clean
```

## Operational impact
- None: no config changes, no schema migrations, no new CLI surface.
- OpenCode sources with extra columns will now ingest successfully instead of failing schema validation.
- OpenCode schema drift errors are more actionable in logs and source health.
