# System Architecture

## System Objective

Moraine converts local agent traces into a queryable corpus that supports operational inspection, faithful trace reconstruction, and agent self-retrieval. Default sources include Codex JSONL, Claude Code JSONL, Kimi CLI wire logs, OpenCode SQLite, Hermes live session snapshots, and optional Hermes trajectory exports. The system is single-node by design: ClickHouse binds locally, runtime state lives under `~/.moraine`, and lifecycle is managed through the `moraine` binary. [src: config/moraine.toml, crates/moraine-config/src/lib.rs]

The design priorities are:

- Low append-to-visibility latency.
- Replay fidelity across evolving source formats.
- Deterministic source provenance and event identity.
- Durable write-time lexical indexing.
- Explicit storage policy for optional secret redaction.
- A thin local MCP retrieval process with explicit safety framing.
- Operator visibility through CLI status, source health, and the monitor UI.

## Component Topology

The storage layer is a local ClickHouse instance. It owns canonical tables, reconstruction views, search documents/postings, source checkpoints, heartbeats, ingest errors, and query telemetry. Services interact with it through SQL and the shared ClickHouse client crate.

The ingestion layer is `moraine-ingest` plus `moraine-ingest-core`. It owns watcher and reconcile scheduling, format-aware dispatch, normalization, batching, checkpoint persistence, and heartbeat emission. It is the only transformation boundary from raw source records to canonical event classes. [src: apps/moraine-ingest, crates/moraine-ingest-core]

The privacy layer is `moraine-privacy` plus `[privacy]` config. It is invoked by ingestion after normalization and before rows are written, so storage redaction policy is explicit and independent from MCP response-time safety. [src: crates/moraine-privacy, crates/moraine-config/src/lib.rs, crates/moraine-ingest-core/src/normalize.rs]

The source-status layer is `moraine-source-status`. It reads configured sources and ClickHouse source state, then returns shared status snapshots for both CLI and monitor. This keeps `moraine sources status` and `/api/sources` aligned. [src: crates/moraine-source-status, apps/moraine/src/main.rs, crates/moraine-monitor-core/src/lib.rs]

The retrieval layer is `moraine-mcp` plus `moraine-mcp-core` and `moraine-conversations`. It exposes six MCP tools, validates strict schemas, reads ClickHouse-backed conversation/search structures, formats prose/full responses, and attaches retrieval safety metadata. It does not own index lifecycle. [src: apps/moraine-mcp, crates/moraine-mcp-core, crates/moraine-conversations]

The monitor layer is `moraine-monitor`, `moraine-monitor-core`, and `web/monitor`. It serves operational APIs and a Svelte UI for health, status, analytics, sessions, and source health. [src: apps/moraine-monitor, crates/moraine-monitor-core, web/monitor]

The control-plane layer is `moraine`. It resolves config, supervises local services, starts managed ClickHouse when needed, runs migrations, reports status, exposes `sources status`, and provides service entrypoints. [src: apps/moraine]

## End-to-End Causal Flow

A typical append path begins when an agent writes a JSONL line, rewrites a Hermes session snapshot, or commits OpenCode rows to SQLite. Watchers and reconcile logic turn those source changes into work items. Reconcile remains mandatory because filesystem watchers can drop events; watcher events are an optimization for latency, not the only correctness path. [src: crates/moraine-ingest-core/src/watch.rs, crates/moraine-ingest-core/src/reconcile.rs]

Dispatch chooses the processor by source format:

- `jsonl` tails append-oriented logs.
- `session_json` reads whole rewritten session snapshots and emits only newly appeared messages.
- `opencode_sqlite` opens the OpenCode database read-only, validates expected tables, and pages rows by strict watermark.

Each processor emits raw rows, canonical event rows, optional link/tool rows, errors, and checkpoint updates. The sink flushes data before checkpoints so failed writes retry without skipping unseen data. [src: crates/moraine-ingest-core/src/dispatch.rs, crates/moraine-ingest-core/src/sink.rs]

As canonical rows land, ClickHouse materialized views update `search_documents` and `search_postings`. MCP search tools read those structures for ranked retrieval, while navigation tools read session and trace views through `moraine-conversations`. [src: sql/002_views.sql, sql/004_search_index.sql, crates/moraine-conversations/src/clickhouse_repo.rs]

Operators observe the system through:

- `moraine status` for process, DB, migration, and heartbeat status.
- `moraine sources status` for per-source inventory, counts, checkpoints, and latest errors.
- Monitor APIs and UI for health, analytics, sessions, and source chips.
- ClickHouse tables for direct investigation.

## Architectural Invariants

Source-addressable provenance is required. Canonical events preserve source file, generation, line, offset, and source reference so trace rows can be traced back to input records.

Checkpoint progress is monotonic within a source generation. Rotation or shrink resets offsets by moving to a new generation, preventing stale offsets from silently skipping data.

Processing is at-least-once with eventual replacement semantics. Reprocessing can happen from retries, dirty-path scheduling, or reconcile scans. Stable event UIDs and replacing table engines make this safe after convergence.

Conversation ordering is deterministic and query-time derived. `v_conversation_trace` centralizes event ordering and turn derivation so callers do not invent separate ordering rules.

Retrieval is index-backed and bounded. MCP does not rebuild global indexes, does not keep a private corpus cache as the source of truth, and does not widen result limits beyond config.

Retrieved memory is untrusted content. MCP responses label retrieved data as memory content, include `moraine-mcp` provenance, and support strict mode to reduce exposure of raw payloads and low-information system events.

Ingest-time privacy is non-retroactive. If redaction policy changes, historical ClickHouse rows and search index rows keep their old representation until explicitly rebuilt.

CLI and monitor source health share one status model. If source classification changes, it must change in `moraine-source-status`, not independently in the monitor UI or CLI renderer.

## Failure and Recovery Model

Watcher loss is expected. Reconcile periodically enumerates configured sources and requeues tracked files/databases. Recovery latency is bounded by reconcile interval, queue pressure, and sink health.

Malformed records are quarantined into `ingest_errors`. The source continues processing subsequent records. Rising error counts should be treated as schema drift or corrupted source data until proven otherwise.

OpenCode schema drift is detected before scanning. The dispatcher validates expected tables and includes `PRAGMA user_version` in schema errors, making upstream OpenCode changes visible during reconciliation.

SQLite WAL writes are handled by watcher path mapping. `opencode.db-wal` and `opencode.db-shm` notifications map back to the configured base database path so live writes do not rely only on periodic reconcile.

ClickHouse unavailability blocks startup and later appears as flush failures, heartbeat issues, monitor query errors, or source-health `query_error`. The system currently retries in memory; it does not spill ingest batches to disk.

MCP failures are narrow. Invalid JSON-RPC methods or arguments produce protocol errors; tool execution failures produce tool error results; telemetry write failures are best-effort warnings. Missing event/session targets are successful `found=false` payloads where the tool contract supports that shape.

## Performance Envelope

Ingestion throughput is controlled primarily by file-worker concurrency, queue/channel bounds, batch size, and flush interval. The defaults are biased toward fast local visibility while still batching enough rows to avoid excessive ClickHouse write amplification.

Backpressure is explicit. Bounded channels, semaphore-limited workers, and heartbeat queue-depth metrics surface pressure before it becomes silent data loss.

Retrieval runtime cost scales with query term count and posting fanout. `max_query_terms`, `max_results`, `min_should_match`, event-kind filters, tool-event filters, and codex-MCP self-exclusion are the main guardrails.

Monitor source health is lightweight but not free. It performs grouped table reads over checkpoint, raw event, and error tables. The endpoint returns partial snapshots when one query fails; it should not become an all-or-nothing health gate.

## Design Pressure and Rejected Alternatives

A watcher-only ingestion design is insufficient because local filesystem event streams are not a correctness guarantee. Reconcile remains part of the architecture even though it adds background work.

An MCP-owned in-memory BM25 index was rejected because it would couple retrieval correctness to process uptime and rebuild behavior. ClickHouse materialized views already provide durable incremental maintenance.

Exactly-once ingest was rejected as too brittle for file-based sources with rotation, rewrites, database WAL sidecars, watcher nondeterminism, and transient DB failures. At-least-once plus stable identity and replacement semantics is simpler and more resilient for this workload.

A separate monitor-specific source-health classifier was rejected because it would drift from CLI semantics. The shared `moraine-source-status` crate is the ownership boundary.

Protocol-version changes are treated separately from MCP schema metadata changes. Strict schemas and output schemas can improve tool contract clarity without bundling an operational protocol bump.

## Operator Implications

Health is a chain. A healthy stack includes running processes, reachable ClickHouse, applied migrations, recent heartbeats, source checkpoint/raw row progress, low or understood ingest errors, and MCP search tables that match current schema expectations.

Use this escalation order:

1. `moraine --output rich --verbose status`
2. `moraine sources status --include-disabled`
3. Monitor UI source strip and sessions panel.
4. `moraine logs ingest --lines 200`
5. `moraine db doctor`
6. Direct ClickHouse queries against `ingest_errors`, `ingest_checkpoints`, `raw_events`, `events`, and search tables.

For code changes, review cross-layer impact. A small normalization edit can alter search recall, source health, monitor displays, MCP output schemas, and benchmark fixtures. Preserve deterministic identity, bounded payload behavior, explicit schemas, and shared status semantics when extending the system.
