# Backup and Restore

## Purpose

Moraine stores its durable corpus in ClickHouse and derives search tables from
normalized event rows. Before schema migrations, privacy-policy changes, clean
reindexing, or destructive maintenance, create a backup and verify it.

The current backup format is a local directory containing:

- `manifest.json` with Moraine version, ClickHouse database/version metadata,
  configured ingest source inventory, bundled/applied migrations, table row
  counts, and SHA-256 checksums.
- `tables/*.jsonl` files exported with ClickHouse `JSONEachRow`.
- No encryption key material. Privacy encryption keys remain external operator
  secrets and must be backed up separately.

## Commands

Create a backup in the default runtime backup directory:

```bash
moraine backup create
```

Create a backup in a specific empty directory:

```bash
moraine backup create --out-dir ~/.moraine/backups/pre-migration-2026-04-20
```

Include derived search/log tables when you want a fuller operational snapshot:

```bash
moraine backup create --include-derived
```

List known backups:

```bash
moraine backup list
moraine backup list --root ~/.moraine/backups
```

Verify a backup before relying on it:

```bash
moraine backup verify ~/.moraine/backups/pre-migration-2026-04-20
```

Inspect a restore plan:

```bash
moraine restore --input ~/.moraine/backups/pre-migration-2026-04-20
moraine restore --input ~/.moraine/backups/pre-migration-2026-04-20 --target-database moraine_restore
```

Execute a verified backup into a staging database:

```bash
moraine restore --input ~/.moraine/backups/pre-migration-2026-04-20 --target-database moraine_restore --execute
```

`restore --execute` is staging-only in this slice. It refuses to target the
active configured database, requires an explicit `--target-database`, and
refuses a target database that already contains tables. The command creates the
current bundled schema in the staging database, imports verified corpus and
operational tables, and validates restored row counts.

Live replacement of the active database is not implemented. After a staging
restore, inspect the restored database with ClickHouse tools and Moraine doctor
checks before doing any manual cutover.

All commands support the global output flag:

```bash
moraine --output json backup verify ~/.moraine/backups/pre-migration-2026-04-20
```

## Tables

The base backup includes:

| Table | Purpose |
|---|---|
| `raw_events` | Canonical source records after source parsing and privacy policy. |
| `events` | Normalized trace events. |
| `event_links` | Parent/child and cross-row relationship edges. |
| `tool_io` | Tool-call input and output surfaces. |
| `ingest_errors` | Failed parse/ingest diagnostics. |
| `ingest_checkpoints` | Source offsets and watermarks. |
| `ingest_heartbeats` | Runtime ingest status history. |
| `schema_migrations` | Applied schema migration ledger. |

`--include-derived` also exports search and interaction tables when present.
Derived tables are skipped if they do not exist; base tables are required.

Restore execution currently imports corpus and operational tables only. It skips
`schema_migrations` because the staging schema is created from the current
bundled migrations, and it skips derived/search tables because materialized
views and explicit reindexing are the safer rebuild path. Use
`moraine reindex --search-only --execute` against a restored staging database
when you need to rebuild search artifacts there.

## Verification

`backup verify` checks:

- The manifest is readable JSON.
- The manifest version is supported.
- Table file paths are relative and do not escape the backup directory.
- Table entries do not duplicate table names or file paths.
- Each referenced table file exists.
- Each table file is UTF-8 JSONL.
- Non-empty JSONL line counts match the manifest.
- SHA-256 checksums match the manifest.

Verification does not prove that every row is semantically valid for the current
schema. It proves that the backup directory is complete relative to its manifest
and has not changed since export.

## Privacy Keys

When privacy `encrypt_raw` is enabled, encrypted fields are stored as
authenticated envelopes and the key ID is stored with affected rows. The key
bytes are not stored in ClickHouse and are not included in backup manifests or
table files.

Back up key material separately, for example from the configured
`privacy.encryption_key_env` secret source or `privacy.encryption_key_file`.
Losing the key means encrypted fields in backups and live ClickHouse cannot be
decrypted.

## Operational Guidance

Create and verify a backup before:

- Running migrations against a long-lived database.
- Changing privacy modes, especially `text_content_mode` or any
  `encrypt_raw` setting.
- Performing a clean reindex or deleting ClickHouse tables.
- Testing large ingest-source changes.
- Moving the corpus between machines.

Prefer a clean reindex from source files when:

- The original session files are complete and easier to trust than a stale
  backup.
- The schema or normalizer behavior changed intentionally.
- You need privacy policy changes to apply retroactively.

Prefer a verified backup when:

- Original source files may be incomplete or unavailable.
- You need to preserve ingest errors, checkpoints, or operational history.
- You need a before/after rollback point for a migration or repair.

## Failure Handling

If `backup create` fails, no manifest is written last unless all table files
were exported successfully. Treat a directory without a valid manifest as an
incomplete backup.

If `backup verify` fails, do not use that backup for recovery without manual
inspection. The error list identifies missing files, checksum mismatches, row
count mismatches, duplicate manifest entries, and unsafe paths.

If `restore --execute` fails after creating a staging database, leave the
staging database in place for inspection unless you are certain it can be
dropped. The command never drops or replaces the active configured database.

If disk space is tight, use `--output json` and external disk checks before a
large export. The initial implementation writes JSONEachRow files directly under
the selected backup directory and computes checksums from exported table bytes.

## Related Files

- CLI implementation: `apps/moraine/src/main.rs`
- ClickHouse client: `crates/moraine-clickhouse/src/lib.rs`
- Privacy operations: `docs/operations/privacy-and-redaction.md`
- Roadmap item: `docs/roadmap/feature-roadmap-2026-04-20.md`
