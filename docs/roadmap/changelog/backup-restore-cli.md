# Backup and Restore CLI

## Summary

Adds the first real backup/restore operations slice:

- `moraine backup create`
- `moraine backup list`
- `moraine backup verify`
- `moraine restore --input ...`

## Implementation Notes

- Backup creation exports ClickHouse tables as `JSONEachRow` files and writes
  `manifest.json` last.
- The manifest records Moraine version, ClickHouse database/version metadata,
  bundled and applied migrations, configured source inventory, table row counts,
  and SHA-256 checksums.
- Base data and operational tables are required. Derived search/log tables are
  included only with `--include-derived` and skipped when absent.
- Verification rejects unsafe relative paths, duplicate entries, missing files,
  row-count mismatches, checksum mismatches, and unsupported manifest versions.
- Restore currently produces a dry-run safety plan. `--execute` is blocked until
  live import semantics are implemented and sandbox-tested.

## Operational Impact

- Backups are stored under `runtime.root_dir/backups/` by default.
- Privacy encryption key material is intentionally excluded from backups and
  must be managed separately by the operator.
- Backups are not a replacement for clean reindexing when the original source
  files are authoritative and a schema or privacy policy change should apply
  retroactively.

## Validation

- `cargo fmt --all -- --check`
- `cargo test -p moraine -p moraine-privacy -p moraine-config -p moraine-ingest-core -p moraine-clickhouse --locked`
