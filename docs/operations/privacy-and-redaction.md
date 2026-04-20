# Privacy and Redaction

## Purpose

Moraine is local-first, but it still indexes raw prompts, tool arguments, tool outputs, and provider payloads. The privacy layer gives operators a configurable ingest-time secret redaction pass before normalized rows are written to ClickHouse. It is implemented by `moraine-privacy`, configured through `moraine-config`, and applied from the ingest dispatcher after source-specific normalization has produced a `NormalizedRecord`. [src: crates/moraine-privacy/src/lib.rs, crates/moraine-config/src/lib.rs, crates/moraine-ingest-core/src/normalize.rs, crates/moraine-ingest-core/src/dispatch.rs]

This layer is separate from MCP retrieval safety. Ingest-time redaction changes what is stored in ClickHouse. MCP safety controls what a retrieval response exposes to an agent at query time.

## Configuration

Privacy is disabled by default. Add a `[privacy]` section to the resolved config when you want ingest-time redaction:

```toml
[privacy]
enabled = true
redaction_policy_version = "1"
raw_events_mode = "store_raw"
text_content_mode = "redact_raw"
payload_json_mode = "redact_raw"
tool_io_mode = "redact_raw"
# Required only when one or more modes are set to "encrypt_raw".
encryption_key_id = "local-main"
encryption_key_env = "MORAINE_ENCRYPTION_KEY"
encryption_key_file = "~/.moraine/keys/local-main.key"
```

Supported modes are:

| Mode | Behavior |
|---|---|
| `store_raw` | Store matching content unchanged. This is the default for every field group. |
| `redact_raw` | Replace each detected secret with `[REDACTED:<detector>]` while preserving surrounding text. |
| `hash_raw` | Replace each detected secret with `[HASH:<short_sha256>]`. This supports stable equality checks without storing the literal secret. |
| `drop_raw` | Drop the whole string value only when a detector matches. Values with no detector hit are preserved. |
| `encrypt_raw` | Encrypt the entire configured field with an authenticated AES-256-GCM envelope. Requires `encryption_key_id` and either `encryption_key_env` or `encryption_key_file`. |

`redaction_policy_version` is stored on rows processed while privacy is enabled. It is still not retroactive: old rows keep the policy metadata and stored representation they had at ingest time.

Encryption key material must be exactly 32 bytes as raw bytes, base64, or 64-character hex. Environment variables are checked before files. Key IDs are stored with encrypted rows; key material is not stored in ClickHouse and is not included in backups.

## Field Groups

The privacy config separates storage surfaces because they have different forensic and retrieval value:

| Field group | ClickHouse surface | Typical risk |
|---|---|---|
| `raw_events_mode` | `raw_events.raw_json` | Exact upstream bytes, including full provider payloads. |
| `text_content_mode` | `events.text_content` | Prompt/response text that feeds views and search documents. |
| `payload_json_mode` | `events.payload_json` | Structured provider/tool metadata and raw nested payloads. |
| `tool_io_mode` | `tool_io.input_json`, `tool_io.output_json` | Tool requests, command output, file fragments, logs, and API responses. |

Changing `text_content_mode` changes the search corpus for future rows because `search_documents` is fed from `events.text_content`. Changing `raw_events_mode` affects forensic replay. Changing `tool_io_mode` affects tool reconstruction but not the primary event text unless the same content is also present in `events`.

## Detectors

The built-in detector set scans string content for common secret patterns:

- OpenAI API keys.
- Anthropic API keys.
- AWS access key IDs.
- AWS secret access key assignments.
- JSON Web Tokens.
- SSH private key blocks.
- Bearer tokens.
- Database URLs with passwords.
- `.env`-style secret assignments such as `API_KEY=...`, `TOKEN=...`, or `PASSWORD=...`.
- Generic lowercase hex strings of 32 or more characters.

Detectors are regex-based. They are fast and transparent, but they are not a full data loss prevention system. Expect both false negatives and false positives, especially for provider-specific token formats, long opaque IDs, hashes, or secrets embedded in unusual encodings.

## Operational Semantics

Redaction runs after a source record has normalized successfully and before sink batching writes rows to ClickHouse. Normalized `raw_events`, `events`, `tool_io`, and `ingest_errors` rows receive privacy metadata. Error rows created before source-specific normalization can still contain limited diagnostic context; validate `ingest_errors` when changing policy for a sensitive corpus.

The privacy layer mutates selected string fields in the normalized record. JSON payload columns are serialized strings in the canonical ClickHouse tables, so the stored string representation is scanned according to the configured mode.

Redaction and encryption are not retroactive. Existing rows keep whatever representation they had when they were ingested. To apply a new policy to historical data, back up ClickHouse first, clear or rebuild affected tables according to the migration plan, and reindex from source files. If you redact or encrypt `text_content`, rebuild search index tables after reingest so search documents and postings match the stored event text.

## Policy Guidance

Use `store_raw` when the host is trusted and exact trace replay is more important than reducing exposure inside the local database.

Use `redact_raw` for the best default balance when secrets should not be retrievable but surrounding conversational context should remain useful.

Use `hash_raw` when stable comparison is useful, for example verifying that the same secret appeared in multiple sessions without revealing it.

Use `drop_raw` sparingly. It can make rows much less useful for debugging because a single detector hit empties the whole string field.

Use `encrypt_raw` only when you can manage the external key lifecycle. Losing the key means the field cannot be decrypted. Rotating keys or changing policies requires backup, reingest, and search rebuild planning; Moraine does not re-encrypt historical rows automatically.

Encrypted `text_content` is not meaningfully searchable because the search corpus receives the stored ciphertext envelope. Prefer `redact_raw` for fields that should remain searchable.

## Interaction With MCP Safety

MCP retrieval safety does not undo ingest-time storage decisions. If a secret was stored raw, strict MCP mode can reduce some exposure by suppressing payload JSON and low-information system event expansion, but it is still a response-time filter over already stored data. If a secret was redacted at ingest time, MCP cannot recover it.

For agent-facing memory, prefer defense in depth:

1. Use ingest-time redaction for fields that should not be stored raw.
2. Keep MCP defaults conservative: payload JSON is opt-in, and non-user-facing content is redacted by default.
3. Use `safety_mode = "strict"` for agents or prompts where raw payloads and system/noise events are unnecessary.

## Validation

After changing privacy config:

1. Run a controlled ingest against a fixture or sandbox source.
2. Query `raw_events`, `events`, and `tool_io` for a known test token.
3. Check `privacy_policy_version`, `privacy_redaction_applied`, `privacy_redaction_count`, `privacy_redaction_kinds`, and `privacy_key_id` on affected rows.
4. Confirm `events.text_content` still contains enough context for search if `text_content_mode` changed.
5. Run `bin/backfill-search-index` or a clean reindex when historical search rows need to match the new policy.
6. Test MCP retrieval with `include_payload_json=true` and `safety_mode="strict"` to verify response-time behavior is still acceptable.

## Related Files

- Redaction implementation: `crates/moraine-privacy/src/lib.rs`
- Config structs and modes: `crates/moraine-config/src/lib.rs`
- Ingest application point: `crates/moraine-ingest-core/src/normalize.rs`
- Dispatcher calls: `crates/moraine-ingest-core/src/dispatch.rs`
- Privacy metadata migration: `sql/013_privacy_metadata.sql`
- Backup and key recovery: `docs/operations/backup-and-restore.md`
- MCP response safety: `crates/moraine-mcp-core/src/lib.rs`
