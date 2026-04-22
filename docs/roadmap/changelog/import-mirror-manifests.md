# Changelog Fragment: Import Mirror Manifests

Date: 2026-04-21
Scope: C02 remote import profiles and sync automation

## What Changed

- Upgraded import sync manifests to version 2 while keeping older manifests readable.
- Added aggregate `files_seen` and per-remote-path `sources[]` records with source path, destination, status, file counts, bytes, duration, rsync exit code, and error text.
- Added `last_success` so a failed sync records the failed attempt without erasing the last known-good mirror state.
- Made manifest writes atomic through the existing local atomic write helper.
- Updated `moraine import status` to show last run state separately from last successful sync.

## Failure Behavior

- Missing local `rsync` still fails before writing a new manifest.
- A failed rsync run writes a manifest with `status = "failed"` and preserves the previous `last_success`.
- Remote paths skipped after a failure are recorded as `not_started`.
- Preview mode still does not mutate disk.

## Validation

```bash
cargo test -p moraine import_sync --locked
```
