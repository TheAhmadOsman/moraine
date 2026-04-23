# Rust Ingestion Service

## Runtime Responsibility

`moraine-ingest` is the sole writer that transforms configured agent traces into canonical event rows. It tails append-oriented JSONL sources, pairs Factory Droid sessions with their settings sidecars, handles rewritten Hermes session JSON snapshots, reads OpenCode SQLite databases, records non-fatal errors, advances checkpoints, and emits ingest heartbeats. Retrieval and monitor surfaces depend on this service for stable event identity, source provenance, and freshness. [src: apps/moraine-ingest, crates/moraine-ingest-core]

Supported configured source formats are:

| Format | Typical sources | Processing model |
|---|---|---|
| `jsonl` | Codex, Claude Code, Kimi wire logs, Hermes trajectories | Resume by byte offset and line number. |
| `factory_droid_jsonl` | Factory Droid local sessions | Resume by JSONL byte offset and line number; read sibling `.settings.json` when the session advances. |
| `session_json` | Hermes live sessions | Read whole rewritten JSON document and emit newly appeared messages. |
| `opencode_sqlite` | OpenCode local database | Open read-only SQLite, validate schema, page rows by strict watermark. |

Sources are declared in `[[ingest.sources]]` with `name`, `harness`, `glob`, `watch_root`, `enabled`, and `format`. Empty `format` is inferred where possible, but explicit formats are preferred for non-JSONL sources. [src: crates/moraine-config/src/lib.rs, config/moraine.toml]

## Execution Model

The runtime is a hybrid of event-driven and reconciliation-driven scheduling. Watcher threads forward source path events to the dispatch queue, while reconcile periodically enumerates all enabled sources. This dual path exists because filesystem notifications are latency hints, not a completeness guarantee. [src: crates/moraine-ingest-core/src/watch.rs, crates/moraine-ingest-core/src/reconcile.rs]

Dispatch state tracks pending, inflight, and dirty work. If a source changes while it is already being processed, the path is marked dirty and replayed after the current run finishes. This avoids dropping writes that race with long reads without allowing unbounded duplicate work.

Concurrency is bounded by `max_file_workers` and channel capacities. Worker tasks parse and normalize source data; a single sink task serializes batched ClickHouse writes and checkpoint updates. This keeps the write path simple and makes flush ordering auditable. [src: crates/moraine-ingest-core/src/dispatch.rs, crates/moraine-ingest-core/src/sink.rs]

## Watchers, WAL Siblings, and Reconcile

Each enabled source registers a watcher on its `watch_root`. Native watchers are preferred; poll watchers are used as fallback when native setup fails. Watcher metrics are recorded in heartbeats, including backend label, registration count, watcher errors, reset count, and last reset timestamp. [src: crates/moraine-ingest-core/src/watch.rs, crates/moraine-ingest-core/src/sink.rs]

OpenCode writes live data through SQLite WAL sidecars. The watcher maps `opencode.db-wal` and `opencode.db-shm` events back to the configured base `.db` path, so OpenCode live writes can trigger ingest promptly instead of waiting for the next reconcile scan.

Reconcile remains the recovery path for missed watcher events, newly copied files, directory changes, and watcher resets. Disabling or starving reconcile weakens completeness guarantees.

## Checkpoint and File Identity Semantics

Checkpoint state is persisted in ClickHouse and cached in memory. The logical identity includes source name, source file, inode, generation, offset, and line/message/watermark cursor. The exact cursor meaning depends on format:

- `jsonl`: byte offset plus line number.
- `session_json`: emitted message count stored in `last_line_no`.
- `opencode_sqlite`: strict high watermark over extracted OpenCode rows.

Generation rollover protects append logs from truncation and rotation. If inode changes or file size drops below the stored offset, the source generation advances and offsets reset. Replacement policy prefers higher generation and then higher progress within a generation.

Checkpoint writes happen after data writes in the sink flush order. If a flush fails before the checkpoint lands, the source can be retried and may re-emit rows, but it will not skip unseen data.

## Normalization Contract

Every parseable source object first becomes a `raw_events` row with original JSON, hash, top-level type, inferred session ID, source coordinates, and source name. Canonical rows are then produced from typed normalizer branches and inserted into `events`, `event_links`, and `tool_io` as appropriate. [src: crates/moraine-ingest-core/src/normalize.rs, sql/001_schema.sql]

Ingest-time privacy redaction runs after source-specific normalization and before sink batching. When enabled, it mutates configured string surfaces in the normalized record before ClickHouse insertion, so `raw_events`, `events`, and `tool_io` store the redacted, hashed, or dropped representation for future rows. [src: crates/moraine-ingest-core/src/normalize.rs, crates/moraine-privacy/src/lib.rs]

Canonical event identity is deterministic and source-based. `event_uid` derives from source coordinates, generation, payload fingerprint, and suffix. When one raw record expands into several semantic events, each child receives a distinct suffix-scoped UID.

Payload size is bounded. Raw JSON and canonical `payload_json` are capped before ClickHouse insertion, and text extraction walks known structures with limits. This prevents one pathological source record from amplifying memory or storage usage without bound.

Token accounting is preserved in `token_usage_json` instead of being forced into provider-specific fixed columns. Downstream analytics can parse provider-specific token payloads when needed.

## Source-Specific Behavior

### Codex

Codex JSONL records are normalized into messages, reasoning, tool calls/results, compacted-history links, token usage, and provider/model metadata. Modern and legacy payload shapes are handled in the shared normalizer.

### Claude Code

Claude Code JSONL records map user/assistant/tool events into the same canonical fields as Codex. External IDs and tool-use/result relationships are split into stable event UIDs and links so conversation reconstruction and tool lineage remain queryable.

### Kimi CLI

Kimi CLI defaults to `~/.kimi/sessions/**/wire.jsonl`. Wire events carry typed envelopes such as turn boundaries, content parts, tool calls/results, status updates, and token counters. The normalizer splits Kimi parsing into context-record and wire-event paths.

If Kimi `context.jsonl` is explicitly configured, Moraine supports role-based records such as user, assistant, tool, system prompt, usage, and checkpoint. Untimestamped Kimi rows receive deterministic synthetic timestamps based on source line number; append-to-visible latency statistics skip those synthetic Kimi timestamps so latency metrics are not polluted by synthetic epoch-derived times. [src: crates/moraine-ingest-core/src/normalize.rs, crates/moraine-ingest-core/src/sink.rs]

### OpenCode

OpenCode uses `format = "opencode_sqlite"` against `~/.local/share/opencode/opencode.db`. The dispatcher opens SQLite with defensive read-only flags, validates expected `session`, `message`, and `part` tables, includes `PRAGMA user_version` and observed table/column lists in schema drift errors, and pages rows in strict watermark order with limits. [src: crates/moraine-ingest-core/src/dispatch.rs]

OpenCode `part` rows are the primary searchable content. Text parts become user/assistant messages, reasoning parts become reasoning rows, tool parts become tool I/O rows, and step-finish rows preserve token usage.

### Factory Droid

Factory Droid defaults to `~/.factory/sessions/**/*.jsonl` with `format = "factory_droid_jsonl"`. The dispatcher tails the JSONL file and, whenever that file advances, reads the sibling `<session>.settings.json` sidecar for model, provider, autonomy, and aggregate token metadata. The raw JSONL row is preserved in `raw_events.raw_json`; missing JSONL timestamps such as `session_start` use the first timestamped session record, sidecar timestamp, or file modification time for canonical ordering.

`session_start` rows become `session_meta` with session title and working directory. `message` rows map user/assistant text, thinking blocks, tool calls, and tool results into canonical message/reasoning/tool rows. `compaction_state` rows become summaries and keep anchor-message links. The sidecar is represented as a synthetic `session_settings` raw row and a stable `session_meta` event so settings updates replace the prior canonical settings event while exact sidecar content remains auditable in raw rows.

### Hermes

Hermes trajectories are JSONL exports where one row can represent a completed rollout. Moraine expands ShareGPT-style conversations into canonical events with synthetic ordering offsets inside the trajectory.

Hermes live sessions use `format = "session_json"` because the files are rewritten snapshots. The processor reads the whole file, compares message count to checkpoint progress, emits session metadata plus newly appeared messages, and keeps event IDs stable across atomic rewrites.

## Sink, Flush, and Durability

The sink is the ingestion durability boundary. It receives `RowBatch` messages from workers, aggregates rows, flushes by threshold or timer, and emits heartbeats on the configured interval.

Flush order is fixed:

1. `raw_events`
2. `events`
3. `event_links`
4. `tool_io`
5. `ingest_errors`
6. `ingest_checkpoints`

Data-before-progress ordering is the core safety property. It accepts at-least-once insertion and relies on stable identity plus replacing table semantics for convergence.

On successful flush, counters and checkpoint cache advance. On failure, buffers remain resident and flush failure metrics increase. Moraine does not currently spill failed batches to disk, so prolonged ClickHouse outages under sustained input can create memory pressure.

## Heartbeats and Observability

Ingest heartbeats report queue depth, active/watched files, flush counters, flush latency, watcher backend, watcher errors/resets, and latest error text. `moraine status` reads heartbeat data for service-level health. `moraine sources status` and monitor `/api/sources` read checkpoint/raw/error tables for source-level health. [src: crates/moraine-ingest-core/src/sink.rs, crates/moraine-source-status/src/lib.rs]

Use source health to answer questions heartbeats cannot answer alone:

- Which configured source has no data yet?
- Which source has errors but still has usable rows?
- Which source is disabled?
- Which source path/glob is being watched?
- What is the latest source-specific ingest error?

## Failure Modes and Recovery

Stale progress across source lifecycle changes is the highest-risk correctness failure. Preserve generation rollover and strict watermark behavior when editing dispatch code.

Malformed records are logged and skipped. This prevents pipeline stalls but can hide upstream schema drift if operators ignore `ingest_errors`.

Watcher failures are recoverable through reconcile, but only if reconcile remains enabled and source globs are correct.

OpenCode schema drift is recoverable by updating the dispatcher once upstream structure is understood. Until then, schema errors should be visible in source health and logs rather than silently producing partial rows.

ClickHouse outages should be handled by restoring DB availability quickly or reducing input rate. The current system is not a durable disk-backed queue.

## Extension Guidance

When adding a new source or payload shape:

1. Add or validate config harness/format semantics in `moraine-config`.
2. Keep dispatch format-specific behavior in `moraine-ingest-core/src/dispatch.rs`.
3. Keep canonical field mapping in `moraine-ingest-core/src/normalize.rs`.
4. Preserve source coordinates and deterministic event UID behavior.
5. Bound raw and canonical payload sizes.
6. Add fixture tests that cover raw-to-canonical behavior and checkpoint behavior.
7. Verify search projection if `text_content` changes.
8. Verify source health if checkpoints or error behavior changes.

Avoid format-specific branching in retrieval and monitor layers. Those layers should consume canonical tables, source status snapshots, and repository abstractions rather than parsing raw source records.
