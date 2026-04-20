# Changelog Fragment: Privacy Encryption and Row Metadata

Date: 2026-04-20
Scope: R09 real privacy encryption, R10 policy metadata foundation

## What Changed

- Replaced the `encrypt_raw` placeholder marker with AES-256-GCM envelope encryption in `moraine-privacy`.
- Added key parsing for raw 32-byte, base64, and hex key material with explicit key IDs.
- Added decrypt helpers and tests for round trip, wrong-key failure, missing-key failure, and key parsing.
- Added `[privacy]` config fields for `encryption_key_id`, `encryption_key_env`, and `encryption_key_file`.
- Added migration `013_privacy_metadata.sql` with additive privacy metadata columns on stored row tables and search documents.
- Updated ingest privacy application to fail closed when `encrypt_raw` is configured without a valid key.
- Populated privacy policy version, redaction count, redaction kinds, and key ID on rows processed under an enabled privacy policy.
- Updated privacy operations docs and default config comments.

## Operational Notes

- Key material is external. It is not stored in ClickHouse and must be backed up separately by the operator.
- `encrypt_raw` encrypts the whole configured field. Encrypted `text_content` is not useful for search unless a separate searchable representation is also stored in a future slice.
- Privacy changes remain non-retroactive. Historical rows need backup, reingest, and search rebuild planning.

## Validation

```bash
cargo fmt --all -- --check
cargo test -p moraine-privacy -p moraine-config -p moraine-ingest-core -p moraine-clickhouse --locked
```
