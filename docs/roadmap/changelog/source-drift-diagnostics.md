# Changelog Fragment: Source Drift Diagnostics

Date: 2026-04-21
Scope: Wave 4 operational safety, C04 source diagnostics

## What Changed

- Added `moraine sources drift` with `--include-disabled` and JSON output.
- Extended shared source file diagnostics with `canonical_event_count` from `events FINAL`.
- Added source-level drift findings for expected idle sources, missing files, unobserved disk files, stale files, checkpoint-only files, raw rows without canonical events, canonical events without raw rows, ingest errors, SQLite sidecars, filesystem errors, and partial ClickHouse query failures.
- Updated monitor source file API typing and UI copy so raw rows and canonical events are visible together.

## Operational Use

Use `moraine sources drift` when source health looks ambiguous or after remote import refreshes. The command is read-only and is designed to answer whether data is stuck at a specific layer:

- disk/mirror file exists but no ingest state;
- checkpoint exists but no rows landed;
- raw rows landed but normalization emitted no events;
- canonical events exist without raw backing rows;
- ingest errors explain a source or file gap.

## Validation

```bash
cargo fmt --all -- --check
cargo test -p moraine-source-status --locked
cargo test -p moraine clap_parses_sources_drift_command --locked
git diff --check
scripts/dev/sandbox/moraine-sandbox up --quiet
docker exec --user moraine <sandbox> /home/moraine/target/debug/moraine --config /sandbox/moraine.toml sources drift
scripts/dev/sandbox/moraine-sandbox down <sandbox>
```
