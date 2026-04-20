# Moraine Feature Roadmap - April 20, 2026

This roadmap is a product and engineering backlog for Moraine as it exists on
April 20, 2026. It assumes Moraine remains local-first: local agent traces are
ingested into local ClickHouse, normalized into deterministic event/session
tables, searched through BM25 and conversation APIs, exposed through MCP, and
operated through CLI plus monitor UI.

The goal is not to add features for their own sake. The goal is to make Moraine
more useful as durable agent memory, safer as an MCP retrieval surface, easier
to operate over months of local and remote sessions, and easier to extend as new
agent harnesses appear.

## Strategic Specifications

P2 and P3 features in this roadmap are too large to implement in a single PR. Detailed design specifications — including schema sketches, API contracts, acceptance criteria, and PR sequencing — are maintained in [`docs/roadmap/specs/`](specs/). Implementation should reference those specs before cutting branches.

## Sources Used

This roadmap is based on the current repository docs and schema, especially:

- [System architecture](../core/system-architecture.md)
- [Data model](../core/data-model.md)
- [Ingestion service](../core/ingestion-service.md)
- [MCP agent interface](../mcp/agent-interface.md)
- [Search indexing and retrieval](../search/indexing-and-retrieval.md)
- [Monitor and source health](../operations/source-health-and-monitor.md)
- [Privacy and redaction](../operations/privacy-and-redaction.md)
- [Design tradeoffs](../architecture/design-tradeoffs.md)

External references checked for current best practices:

- [MCP 2025-11-25 server feature overview](https://modelcontextprotocol.io/specification/2025-11-25/server/index)
- [MCP 2025-11-25 tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
- [MCP 2025-11-25 pagination](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/pagination)
- [MCP 2025-11-25 authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)
- [MCP security best practices](https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices)
- [SQLite write-ahead logging](https://www.sqlite.org/wal.html)
- [ClickHouse TTL](https://clickhouse.com/docs/guides/developer/ttl)
- [ClickHouse data skipping indexes](https://clickhouse.com/docs/optimize/skipping-indexes)
- [ClickHouse incremental materialized views](https://clickhouse.com/docs/materialized-view/incremental-materialized-view)
- [ClickHouse projections as secondary indexes, 2026](https://clickhouse.com/blog/projections-secondary-indices)
- [ClickHouse 26.3 release notes, 2026](https://clickhouse.com/blog/clickhouse-release-26-03)
- [OpenTelemetry documentation](https://opentelemetry.io/docs/)
- [OWASP Top 10 for LLM Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [OWASP LLM01 Prompt Injection](https://genai.owasp.org/llmrisk/llm01-prompt-injection/)
- [SLSA v1.2](https://slsa.dev/spec/v1.2/)

## Priority Model

| Priority | Meaning | Default bar |
|---|---|---|
| P0 | Reliability, safety, or correctness feature that protects existing data and trust. | Should land before bigger product surface expansion. |
| P1 | High-value product feature that makes Moraine materially better for daily use. | Should be reviewable as focused PRs and backed by tests. |
| P2 | Richness, scale, or polish feature that compounds once the P0/P1 base is stable. | Should not weaken local-first behavior or safety defaults. |
| P3 | Strategic expansion or optional deployment mode. | Should wait until local mode is boring and well documented. |

Effort is intentionally rough:

| Effort | Meaning |
|---|---|
| S | Small, usually one crate/module plus tests. |
| M | Multiple modules or one backend plus one UI surface. |
| L | Cross-cutting feature with migrations, docs, and e2e validation. |
| XL | Product area or multi-PR program. |

## Product Thesis

Moraine should become a dependable local memory system for agentic work:

1. It should remember every important local and remote agent trace without
   silently losing data.
2. It should make old work easy to retrieve, compare, cite, replay, and learn
   from.
3. It should expose retrieval to agents through MCP without turning retrieved
   memory into trusted instructions.
4. It should make operations visible: source lag, schema drift, ingest errors,
   backup freshness, disk pressure, query quality, and privacy policy state.
5. It should stay extensible. New harnesses should not require a new branch in
   the central dispatcher forever.

## Current Strengths To Preserve

- Local-first architecture with ClickHouse as the durable analytical store.
- Deterministic event identity and at-least-once ingest semantics.
- Source-specific normalizers with real fixtures and integration tests.
- Shared CLI and monitor source-health semantics.
- MCP tools with strict schemas, pagination, safety metadata, and prose/full
  response modes.
- Ingest-time privacy redaction with explicit non-retroactive behavior.
- Sandbox-based QA path for ingest, monitor, MCP, and schema changes.
- OpenCode WAL sibling mapping and schema drift visibility.

## Highest-Leverage Sequence

This is the recommended order if we want the fastest path to a richer product
without building on shaky operations:

1. P0 backup/restore, reindex, and doctor commands.
2. P0 source diagnostics, OpenCode hardening, and source drift reports.
3. P0 privacy encryption/key management and redaction migration workflow.
4. P0 MCP safety/conformance regression suite.
5. P1 monitor session explorer and source drilldown.
6. P1 retrieval evaluation, field weighting, phrase/proximity search, and query
   feedback loop.
7. P1 MCP resources/prompts and saved retrieval workflows.
8. P1 remote import profiles and sync automation.
9. P2 summaries, curated memory, graph/entity layer, alerts, and OTel export.
10. P3 hosted/team/multi-node modes after local mode is operationally mature.

## Roadmap Index

| ID | Feature | Priority | Effort | Primary value |
|---|---|---:|---:|---|
| R01 | Backup and restore CLI | P0 | L | Protects the corpus before migrations and reindexing. |
| R02 | Clean reindex and search rebuild orchestration | P0 | L | Makes parser/privacy/search changes operationally safe. |
| R03 | Database doctor and integrity audit | P0 | M | Detects drift, duplicates, orphan rows, and broken views. |
| R04 | Migration safety gates | P0 | M | Prevents partial schema changes from corrupting local state. |
| R05 | ClickHouse version compatibility matrix | P0 | M | Makes runtime upgrades deliberate and testable. |
| R06 | Disk-backed ingest retry spool | P0 | L | Avoids data loss while ClickHouse is down or busy. |
| R07 | Deep source diagnostics API and CLI | P0 | M | Turns "stale source" into actionable file/session facts. |
| R08 | OpenCode adapter hardening | P0 | M | Protects the newest SQLite source against upstream change. |
| R09 | Real privacy encryption and key management | P0 | XL | Makes `encrypt_raw` actually reversible and safe. |
| R10 | Redaction migration and privacy audit workflow | P0 | L | Handles non-retroactive policy changes responsibly. |
| R11 | MCP conformance and safety regression suite | P0 | M | Keeps agent-facing contracts stable and safe. |
| R12 | Untrusted-memory retrieval controls | P0 | M | Reduces prompt-injection and over-agency risk. |
| R13 | Release signing, SBOM, provenance, and checksums | P0 | L | Makes installs and updates verifiable. |
| C01 | Source trait and adapter registry | P1 | L | Stops dispatcher growth from becoming brittle. |
| C02 | Remote import profiles and sync automation | P1 | M | Makes PC/vm/remote-session imports first-class. |
| C03 | Config wizard and source auto-discovery | P1 | M | Reduces setup mistakes and stale globs. |
| C04 | Monitor source drilldown | P1 | M | Shows files, checkpoints, lag, errors, and WAL state. |
| C05 | Monitor session explorer | P1 | L | Gives humans a rich transcript/debugging surface. |
| C06 | Query workbench and saved searches | P1 | M | Lets users inspect search behavior and reuse queries. |
| C07 | Search relevance evaluation loop | P1 | M | Turns search quality into a measurable contract. |
| C08 | Field-weighted, phrase, and proximity search | P1 | L | Improves recall and precision without embeddings first. |
| C09 | Optional semantic/hybrid retrieval | P1 | XL | Finds related work beyond lexical overlap. |
| C10 | MCP resources, prompts, and workflow templates | P1 | L | Makes Moraine easier for agents and users to navigate. |
| C11 | Portable export/import archives | P1 | M | Supports sharing, backup validation, and reproducible tests. |
| C12 | Retention, compaction, and tiering policy | P1 | L | Controls storage growth while preserving provenance. |
| C13 | Observability and OpenTelemetry export | P1 | L | Makes runtime behavior visible outside the monitor. |
| C14 | Alerts and notification hooks | P1 | M | Surfaces source drift, backup failures, and disk risk. |
| C15 | Token, cost, and model analytics | P1 | M | Explains agent spend and model usage over time. |
| C16 | Fixture generator and source simulator | P1 | M | Makes adapter testing easier and less fragile. |
| P01 | Session summaries and memory cards | P2 | XL | Creates durable, curated memory from raw traces. |
| P02 | User-curated corrections and knowledge notes | P2 | L | Lets humans improve memory without rewriting history. |
| P03 | Entity and graph layer | P2 | XL | Connects sessions, files, tools, tasks, repos, and errors. |
| P04 | Replay and reproduction tooling | P2 | L | Recreates prior sessions or failure paths for debugging. |
| P05 | Project-aware retrieval profiles | P2 | M | Keeps memory relevant across repos and work contexts. |
| P06 | Python, CLI, and HTTP client polish | P2 | M | Makes Moraine easier to automate from notebooks and scripts. |
| P07 | Monitor accessibility, keyboard, and mobile pass | P2 | M | Makes the UI usable for long daily sessions. |
| P08 | Performance tuning and query plan snapshots | P2 | L | Keeps queries fast as local data grows. |
| P09 | Policy engine for retrieval and exports | P2 | L | Gives explicit rules for sensitive or scoped data. |
| P10 | Adapter marketplace and plugin packaging | P2 | XL | Makes third-party source support sustainable. |
| S01 | Team and multi-user mode | P3 | XL | Enables shared corpora after local safety is mature. |
| S02 | Hosted or remote-server mode | P3 | XL | Enables non-local deployment with auth and tenancy. |
| S03 | Cross-device sync and conflict resolution | P3 | XL | Keeps multiple personal machines in one corpus. |
| S04 | Desktop app or browser extension | P3 | XL | Improves adoption and capture surfaces. |
| S05 | Agent QA and anomaly detection | P3 | XL | Detects loops, regressions, tool failures, and risky traces. |
| S06 | Multimodal trace ingestion | P3 | XL | Adds images, audio, screenshots, and rendered artifacts. |

## P0 Details

### R01 - Backup and Restore CLI

Priority: P0  
Effort: L

Implement `moraine backup create`, `moraine backup list`, `moraine backup verify`,
and `moraine restore --dry-run`. A backup should include ClickHouse data, schema
version, migration checksums, resolved config, source inventory, binary/runtime
version, and a manifest with row counts and checksums.

First slice:

- Add a local backup directory under `~/.moraine/backups/`.
- Support cold backups when the stack is stopped and online backups when the
  ClickHouse version/config supports it.
- Write a JSON manifest and a human-readable summary.
- Add `verify` that can compare row counts, schema version, and table existence.

Edge cases:

- ClickHouse is down, busy, or mid-migration.
- Disk is nearly full before or during backup.
- The user has WAL-backed SQLite sources in import directories; source archives
  must not assume copying only `.db` files is enough.
- Backup is from an older schema or older ClickHouse pin.
- Restore target already has data.
- Privacy key material exists and must not be accidentally omitted or exposed.

Acceptance criteria:

- Backup and restore are documented and sandbox-tested.
- Restore can produce a clean database with matching table counts.
- `restore --dry-run` refuses incompatible or incomplete backups before changes.

### R02 - Clean Reindex and Search Rebuild Orchestration

Priority: P0  
Effort: L

Implement a first-class reindex command that can rebuild from source files,
remote import mirrors, or an existing `raw_events` corpus.

First slice:

- `moraine reindex --all`
- `moraine reindex --source <name>`
- `moraine reindex --search-only`
- `moraine reindex --privacy-policy-version <version>`
- `--dry-run` showing affected sources, checkpoints, rows, search docs, and
  estimated storage.

Edge cases:

- Non-retroactive privacy changes.
- Existing duplicate rows under ReplacingMergeTree convergence.
- Partially missing source files.
- A source path moved since original ingest.
- Rebuilding search while MCP is serving queries.
- Synthetic timestamps for untimestamped harness metadata.

Acceptance criteria:

- Reindex never destroys old data without an explicit backup check.
- Search rebuild is deterministic for the same corpus and config.
- The command reports exactly what it reset, rebuilt, and skipped.

### R03 - Database Doctor and Integrity Audit

Priority: P0  
Effort: M

Add `moraine doctor --deep` to validate the runtime, schema, source inventory,
and corpus integrity.

First slice:

- Check ClickHouse reachability and version.
- Check expected databases, tables, views, and materialized views.
- Check migration table state and embedded migration checksums.
- Check orphan `event_links`, orphan `tool_io`, missing `raw_events`, and
  sessions with impossible time ranges.
- Check search index freshness against `events`.
- Check source checkpoints against configured sources.

Edge cases:

- `ReplacingMergeTree` rows may not be physically collapsed yet.
- Some tables may be unavailable while ClickHouse is starting.
- Disabled sources should not be reported as broken.
- Clock skew and synthetic timestamps can make naive age checks noisy.

Acceptance criteria:

- Produces `ok`, `warning`, and `error` findings with remediation hints.
- Supports `--json` for monitor or scripts.
- Has tests against a fixture database with intentional corruption.

### R04 - Migration Safety Gates

Priority: P0  
Effort: M

Make schema changes safer by enforcing preflight checks and clear rollback
guidance.

First slice:

- Store migration checksums in ClickHouse.
- Add a migration lock.
- Require a recent backup or `--no-backup-check` override for destructive
  migrations.
- Add `moraine schema status`.
- Add sandbox tests that apply all migrations to an empty DB and an old fixture
  DB.

Edge cases:

- Interrupted migration after a table change but before a view recreation.
- Migrations that depend on a specific ClickHouse version.
- Idempotent `IF EXISTS` and `IF NOT EXISTS` statements hiding partial failures.
- User manually edits the database.

Acceptance criteria:

- Partial migrations are detected on the next startup.
- Schema status explains current version, pending migrations, and failed step.

### R05 - ClickHouse Version Compatibility Matrix

Priority: P0  
Effort: M

The project manages ClickHouse locally, so database version changes are
operational changes. Add an explicit compatibility matrix and test gate.

First slice:

- Document supported, tested, and experimental ClickHouse versions.
- Add CI/sandbox jobs for the pinned version plus the next candidate version.
- Add a runtime warning when the local version is outside the tested range.
- Track features that may affect Moraine: async inserts, TTL merge behavior,
  projections, JSON behavior, backup engines, and query planner changes.

Edge cases:

- New ClickHouse defaults changing insert acknowledgment semantics.
- Search query performance shifting after an optimizer change.
- Migrations using syntax not available in the pinned version.
- Older local installs after `git pull`.

Acceptance criteria:

- A ClickHouse pin bump requires test evidence in the PR.
- `moraine doctor` reports supported/unsupported versions clearly.

### R06 - Disk-Backed Ingest Retry Spool

Priority: P0  
Effort: L

Add a bounded local spool for normalized batches that could not be written to
ClickHouse. Checkpoints should only advance after durable sink acknowledgment or
after the batch is safely spooled with replay semantics.

First slice:

- Durable append-only spool under `~/.moraine/spool/`.
- Batch manifest with source, offsets, event UIDs, schema version, and retry
  count.
- Replay loop with exponential backoff.
- Size and age limits with source-health warnings.

Edge cases:

- Crash during spool write.
- Crash after ClickHouse insert but before spool deletion.
- Spool format changes across versions.
- Disk full while ClickHouse is unavailable.
- Privacy redaction must happen before spooling if the spool stores payloads.

Acceptance criteria:

- Simulated ClickHouse outage does not lose parseable records.
- Replay is idempotent and does not duplicate logical events.

### R07 - Deep Source Diagnostics API and CLI

Priority: P0  
Effort: M

Extend source health beyond counts to answer "what exactly is stale or broken?"

First slice:

- `GET /api/sources/:source/files`
- `GET /api/sources/:source/errors`
- CLI equivalents: `moraine sources files` and `moraine sources errors`
- Include file size, modified time, last checkpoint, last raw event, latest
  error, glob match count, watch root, watcher backend, and reconcile cadence.

Edge cases:

- Glob matches zero files.
- Permission denied on a subtree.
- Symlinks, hard links, NFS, iCloud/Dropbox, and file rotations.
- Same session ID appears in multiple imported roots.
- Query failures should produce partial results, not a blanket 503.

Acceptance criteria:

- Users can identify the file responsible for a source warning without reading
  ClickHouse manually.
- Monitor and CLI share the same status semantics.

### R08 - OpenCode Adapter Hardening

Priority: P0  
Effort: M

OpenCode is a SQLite source, not a line-oriented file source. Keep hardening it
around WAL behavior, schema drift, and incremental scans.

First slice:

- Add integration tests for `.db`, `.db-wal`, and `.db-shm` notifications.
- Keep strict watermark scans with page limits.
- Store observed `PRAGMA user_version`, table list, and column list in errors.
- Add a small schema compatibility matrix fixture.

Edge cases:

- Read-only connection sees SQLITE_BUSY.
- WAL checkpoint happens while the scanner is reading.
- Upstream adds/removes columns.
- Rows have equal timestamps.
- Source is copied from another machine without its WAL sidecars.

Acceptance criteria:

- OpenCode live writes are detected without waiting for periodic reconcile.
- Schema drift creates actionable source-health errors.

### R09 - Real Privacy Encryption and Key Management

Priority: P0  
Effort: XL

`encrypt_raw` is currently a marker/hash placeholder. Replace it with real
envelope encryption and explicit key lifecycle documentation.

First slice:

- Introduce key providers: local file key, OS keychain where available, and
  environment variable for CI.
- Encrypt configured fields before ClickHouse insert.
- Store key ID and encryption policy version on affected rows.
- Add decrypt path only to explicit admin/export tools, not default MCP output.

Edge cases:

- Key rotation and re-encryption.
- Backup without key material.
- Restore on another machine.
- Search index cannot search encrypted text unless a separate redacted/search
  representation is stored.
- Error tables and spools may contain sensitive fragments.

Acceptance criteria:

- Existing redaction modes still work.
- Encrypted data can be restored and decrypted only with the right key.
- Docs clearly state what is encrypted and what is not.

### R10 - Redaction Migration and Privacy Audit Workflow

Priority: P0  
Effort: L

Privacy changes are ingest-time and non-retroactive today. Add tooling that
makes this explicit and gives users a path to bring historical rows into a new
policy.

First slice:

- Store privacy policy version in row metadata.
- `moraine privacy audit` reports rows indexed under each policy.
- `moraine privacy reprocess --from-policy <old> --to-policy <new>` requires a
  backup and can rebuild affected rows/search docs.
- Monitor panel for detector hit counts and unredacted historical windows.

Edge cases:

- False positives and false negatives.
- Hash mode changes with salt/key changes.
- Raw events, event text, payload JSON, and tool I/O need aligned behavior.
- Ingest errors might preserve raw fragments outside normal redaction.

Acceptance criteria:

- Users can prove which policy was applied to which rows.
- Historical reprocessing is documented, testable, and reversible from backup.

### R11 - MCP Conformance and Safety Regression Suite

Priority: P0  
Effort: M

MCP is the agent-facing contract. Add a suite that treats the server as a
protocol product, not just Rust functions.

First slice:

- Snapshot `initialize`, `tools/list`, and all tool output schemas.
- Validate `additionalProperties: false` for input schemas.
- Validate output schemas against real tool responses.
- Test `safety_mode`, `response_format`, pagination, invalid cursors, and limit
  boundaries.
- Keep protocol version changes isolated from feature changes.

Edge cases:

- Different clients tolerate or reject schema details differently.
- Prose mode and full mode must preserve safety metadata.
- Tool execution errors vs JSON-RPC protocol errors.
- Cursors should be opaque and non-persistent.

Acceptance criteria:

- Any MCP schema drift is visible in tests.
- Existing tool arguments still deserialize.

### R12 - Untrusted-Memory Retrieval Controls

Priority: P0  
Effort: M

Retrieved memory can include malicious or stale instructions from prior agent
sessions. Harden the retrieval layer around OWASP GenAI risks such as prompt
injection, sensitive information disclosure, excessive agency, and unbounded
consumption.

First slice:

- Keep explicit "retrieved content is untrusted memory" safety preambles.
- Add redaction/filter counters to all retrieval tools.
- Add per-tool maximum output budgets and truncation metadata.
- Add optional strict mode that suppresses payload JSON, system/noise events,
  and long tool outputs unless explicitly requested.
- Add regression fixtures with prompt-injection content embedded in old traces.

Edge cases:

- Malicious text appears in source files, tool outputs, web-search results, or
  previous assistant messages.
- Retrieved content attempts to invoke tools or override instructions.
- Users may need forensic access to raw payloads.
- Some clients display structured content differently from text content.

Acceptance criteria:

- Safety labels survive every response format.
- Strict mode is at least as restrictive as current defaults.
- Prompt-injection fixtures do not remove access, but they are clearly framed as
  untrusted data.

### R13 - Release Signing, SBOM, Provenance, and Checksums

Priority: P0  
Effort: L

Moraine installs a local database service and MCP server, so users should be able
to verify what they run.

First slice:

- Generate checksums for release artifacts.
- Generate SBOMs for Rust binaries and web assets.
- Sign release artifacts and checksums.
- Produce SLSA provenance from hosted CI.
- Document verification in install docs.

Edge cases:

- Homebrew, shell installers, Cargo installs, and Python packages need different
  artifact paths.
- Local developer builds will not have the same provenance.
- Signing keys and CI tokens must not be available to untrusted build steps.

Acceptance criteria:

- Users can verify downloads without trusting only transport security.
- Release docs distinguish official artifacts from local builds.

## P1 Details

### C01 - Source Trait and Adapter Registry

Priority: P1  
Effort: L

Replace format-specific dispatcher branching with an adapter contract.

First slice:

- Define a `SourceAdapter` trait with discovery, scan, normalize, checkpoint,
  and fixture metadata methods.
- Move Codex, Claude Code, Hermes, Kimi CLI, and OpenCode into adapters.
- Keep the existing config format stable while mapping `format` to an adapter.
- Add adapter conformance tests.

Edge cases:

- Adapters can be file, directory, SQLite, remote mirror, or future API sources.
- Some sources emit multiple canonical events per raw record.
- Untimestamped records need source-specific deterministic timestamps.
- Adapter errors must map to shared source-health status.

Acceptance criteria:

- Adding a source does not require editing a central `process_file` branch.
- Existing fixtures produce the same logical rows.

### C02 - Remote Import Profiles and Sync Automation

Priority: P1  
Effort: M

Make PC, vm503, and future remote imports first-class.

First slice:

- Add `[imports.<name>]` config for SSH host, remote paths, local mirror path,
  include/exclude patterns, and cadence.
- Provide `moraine import sync <name>` and `moraine import status`.
- Record sync manifests with file counts, bytes, last success, and errors.

Edge cases:

- Remote machine offline.
- Partial rsync interruption.
- Remote path contains WAL sidecars.
- File permissions and shell differences.
- Duplicate sessions across machines.

Acceptance criteria:

- Syncing remote sessions no longer requires local-only scripts.
- Source health can distinguish "mirror stale" from "ingest stale".

### C03 - Config Wizard and Source Auto-Discovery

Priority: P1  
Effort: M

Add an interactive and non-interactive setup path that finds common agent
session directories and validates config before the stack starts.

First slice:

- `moraine config wizard`
- `moraine config detect --json`
- Path expansion preview for `~`, env vars, and globs.
- Warnings for missing dirs, unreadable paths, overlapping sources, and unknown
  formats.

Edge cases:

- Multiple installs of one harness.
- Imported mirrors vs live local harness paths.
- Paths with spaces.
- Disabled sources retained for later.

Acceptance criteria:

- A new user can get a valid config without editing TOML by hand.
- The wizard never overwrites existing config without backup.

### C04 - Monitor Source Drilldown

Priority: P1  
Effort: M

Build UI on top of R07.

First slice:

- Source list with status, lag, rows, errors, checkpoints, and watch metadata.
- Detail drawer with matched files, latest errors, and suggested remediation.
- Link from error to raw source coordinates where safe.

Edge cases:

- Large source lists.
- Partial API failures.
- Long paths and tiny screens.
- Time zone display and synthetic timestamps.

Acceptance criteria:

- A user can diagnose a stale source from the monitor without using SQL.

### C05 - Monitor Session Explorer

Priority: P1  
Effort: L

Give humans a rich UI for browsing sessions, events, tool calls, and source
payloads.

First slice:

- Session list with filters by source, harness, mode, model, project, time, and
  error state.
- Transcript view using `get_session_events` semantics.
- Tool call/result folding.
- Copyable event UID and source coordinates.
- Links to related events.

Edge cases:

- Very long sessions.
- Missing or duplicate timestamps.
- Huge payload JSON.
- Events with no text but useful tool I/O.
- Mixed providers inside one session.

Acceptance criteria:

- The UI can inspect a real imported corpus without freezing.
- It preserves deterministic event ordering.

### C06 - Query Workbench and Saved Searches

Priority: P1  
Effort: M

Expose search behavior to users and developers.

First slice:

- Search input with mode filters, time filters, source filters, and response
  format preview.
- Show BM25 scores, matched terms, ranking features, and query latency.
- Save named searches locally.
- Export result sets to JSONL.

Edge cases:

- Empty queries.
- Very broad queries.
- Quoted phrases before phrase support exists.
- User expects search to include redacted/encrypted fields.

Acceptance criteria:

- Search failures are debuggable from UI and CLI.
- Saved searches are versioned when query semantics change.

### C07 - Search Relevance Evaluation Loop

Priority: P1  
Effort: M

The schema already reserves query, hit, and interaction logs. Turn that into a
quality process.

First slice:

- Capture MCP and monitor search query metadata.
- Add optional user feedback: relevant, irrelevant, opened, copied, and hidden.
- Add evaluation fixtures with representative query sets.
- Report MRR, nDCG, recall at K, zero-result rate, and latency percentiles.

Edge cases:

- Telemetry may include sensitive query text.
- Offline local users may not want automatic feedback capture.
- Relevance can be project-specific.
- Query logs should have retention and privacy policy.

Acceptance criteria:

- Ranking changes can be compared before merge.
- The evaluation corpus can be run in CI or sandbox.

### C08 - Field-Weighted, Phrase, and Proximity Search

Priority: P1  
Effort: L

Improve lexical retrieval before adding embeddings.

First slice:

- Add field weights for title/session metadata, user text, assistant text, tool
  names, file paths, and error messages.
- Add phrase matching and term proximity where feasible.
- Add code-aware tokenization for paths, symbols, camelCase, snake_case, and
  stack traces.
- Add configurable ranking profiles.

Edge cases:

- Phrase syntax conflicts with current query tokenization.
- CJK and non-English tokenization.
- Long code blocks and minified JSON.
- Privacy-redacted or hashed text should not leak via term stats.

Acceptance criteria:

- Existing search results do not regress on evaluation fixtures.
- Ranking changes are explainable in the workbench.

### C09 - Optional Semantic and Hybrid Retrieval

Priority: P1  
Effort: XL

Add semantic retrieval only after lexical evaluation exists. Keep it optional,
local-friendly, and privacy-aware.

First slice:

- Store embeddings in a separate table with model name, dimensions, input policy,
  and source policy version.
- Support local embedding models first, then optional remote providers.
- Hybrid rank BM25 plus vector similarity.
- Add rebuild command for embedding tables.

Edge cases:

- Embeddings can leak sensitive information.
- Model upgrades require full rebuild or dual indexes.
- Vector retrieval can surface semantically related but unsafe old instructions.
- Multilingual behavior differs by model.
- Large corpora can make embedding storage expensive.

Acceptance criteria:

- Semantic search is off by default.
- Privacy docs explain what leaves the machine, if anything.
- Hybrid search beats lexical baseline on evaluation fixtures before default use.

### C10 - MCP Resources, Prompts, and Workflow Templates

Priority: P1  
Effort: L

MCP distinguishes tools, resources, and prompts. Moraine currently focuses on
tools. Add resources and prompts where they improve control and discoverability.

First slice:

- Resources for sessions, events, saved searches, source-health snapshots, and
  schema docs.
- Resource templates such as `moraine://sessions/{session_id}` and
  `moraine://events/{event_uid}`.
- Prompts for "recall similar work", "summarize this session", "compare prior
  debugging attempts", and "prepare PR context".
- Keep prompts user-controlled and retrieval output untrusted.

Edge cases:

- Some hosts may support tools but not resources/prompts.
- Resource URIs must not expose arbitrary file paths.
- Prompt templates can become unsafe if they treat memory as instructions.
- Pagination and output budgets still matter.

Acceptance criteria:

- Existing tools continue working.
- New resources/prompts have schema tests and safety framing.

### C11 - Portable Export and Import Archives

Priority: P1  
Effort: M

Let users package subsets of Moraine data without requiring raw ClickHouse
access.

First slice:

- Export sessions, events, links, tool I/O, source coordinates, schema version,
  and privacy metadata to JSONL or Parquet.
- Import archives into a clean namespace or staging database.
- Support `--redacted`, `--raw`, and `--manifest-only` modes.

Edge cases:

- Exporting encrypted rows.
- Exporting rows with raw payloads that contain secrets.
- Session IDs colliding on import.
- Very large exports.

Acceptance criteria:

- Archive round-trips in tests.
- Manifest lets users verify row counts and schema compatibility.

### C12 - Retention, Compaction, and Tiering Policy

Priority: P1  
Effort: L

Moraine intentionally stores rich provenance. Add controls for long-running
storage growth without silently dropping useful history.

First slice:

- Configurable TTL policy by table/source/class.
- Dry-run storage report before applying retention.
- Preserve canonical recent window plus older summaries or rollups.
- Add monitor storage dashboard.

Edge cases:

- ClickHouse TTL applies during background merges, not immediately.
- Dropping raw history can break forensic replay.
- Search docs may outlive source payloads unless policies are aligned.
- Privacy policies and retention policies can interact.

Acceptance criteria:

- Retention is explicit, documented, and dry-run capable.
- Users can see what will become unrecoverable.

### C13 - Observability and OpenTelemetry Export

Priority: P1  
Effort: L

Instrument Moraine services with traces, metrics, and structured logs. Export to
OTLP optionally while keeping local monitor useful by default.

First slice:

- Structured logs with request IDs and source names.
- Metrics for ingest lag, batch sizes, sink latency, errors, query latency,
  search result counts, MCP calls, and monitor API status.
- Traces around reconcile, dispatch, ClickHouse inserts, searches, and MCP calls.
- Configurable OTLP endpoint.

Edge cases:

- Logs can contain sensitive source paths or query text.
- Export must be opt-in.
- Local operation should not require an external collector.
- High-cardinality labels can overload metrics backends.

Acceptance criteria:

- Runtime behavior can be diagnosed without attaching a debugger.
- Sensitive fields are scrubbed or disabled by default.

### C14 - Alerts and Notification Hooks

Priority: P1  
Effort: M

Turn health checks into actionable alerts.

First slice:

- Configurable alerts for source stale, error spike, backup stale, disk low,
  ClickHouse down, spool growing, and remote sync failed.
- Hooks: shell command, webhook, desktop notification, and monitor banner.
- Alert state table with dedupe and cooldown.

Edge cases:

- Avoid alert storms after laptop sleep.
- Synthetic timestamps should not create false stale alerts.
- Disabled sources should not alert.
- Webhooks may fail and need retry limits.

Acceptance criteria:

- Alerts are explainable and suppressible.
- Monitor shows active and recently resolved alerts.

### C15 - Token, Cost, and Model Analytics

Priority: P1  
Effort: M

Expose how agents use models and tools over time.

First slice:

- Normalize token counts where harnesses provide them.
- Add configurable model-price metadata.
- Dashboards for tokens, estimated cost, latency, model/provider/harness mix,
  and tool-call frequency.

Edge cases:

- Different harnesses report tokens differently.
- Local models may have no monetary cost.
- Pricing changes over time.
- Cost estimates should not imply billing accuracy.

Acceptance criteria:

- Users can compare activity across harnesses and projects.
- Unknown token/cost fields are clearly marked unknown.

### C16 - Fixture Generator and Source Simulator

Priority: P1  
Effort: M

Make source adapter tests easier to write and review.

First slice:

- Generator for minimal valid sessions per adapter.
- Corruption modes: partial JSON, unknown fields, missing timestamps, duplicate
  IDs, huge payloads, secrets, rotated files, WAL sidecars.
- Golden extracted rows with stable UIDs.

Edge cases:

- Fixtures should not hide real-world quirks by being too clean.
- Synthetic data must not accidentally include secrets.
- SQLite fixtures need deterministic timestamps and row IDs.

Acceptance criteria:

- New adapter PRs can include small, clear fixture sets.
- Existing adapters gain edge-case coverage without huge binary fixtures.

## P2 Details

### P01 - Session Summaries and Memory Cards

Priority: P2  
Effort: XL

Build a curated memory layer over raw traces.

First slice:

- Generate session summaries with provenance links to event UIDs.
- Let users promote summaries into "memory cards".
- Store summary model, prompt version, source sessions, confidence, and review
  status.
- Keep summaries separate from raw events.

Edge cases:

- Summaries may hallucinate.
- Old prompt-injection content can poison generated summaries.
- Updating a session should invalidate or version summaries.
- Users need delete/edit capability.

Acceptance criteria:

- Every summary claim links back to source events.
- MCP retrieval can return summaries with provenance and safety labels.

### P02 - User-Curated Corrections and Knowledge Notes

Priority: P2  
Effort: L

Let humans add durable knowledge that complements raw traces.

First slice:

- Notes attached to session, event, source, repo, task, or arbitrary tag.
- Correction records for wrong summaries or bad search results.
- MCP and monitor surfaces for curated notes.

Edge cases:

- Notes can contain secrets.
- Notes may become stale.
- Same fact can conflict across notes.
- Exports must include or exclude notes explicitly.

Acceptance criteria:

- User-authored memory is distinguishable from agent-authored trace data.
- Notes have timestamps, authorship, and revision history.

### P03 - Entity and Graph Layer

Priority: P2  
Effort: XL

Use `event_links` as the foundation for a broader graph of work.

First slice:

- Entities: repo, branch, commit, file, command, tool, issue, PR, host, model,
  source, error signature, and task.
- Edges: mentioned, edited, ran, failed, retried, superseded, related session,
  generated artifact.
- Graph queries and monitor visualizations.

Edge cases:

- Entity extraction can be wrong.
- File paths differ across machines.
- Git branches and commits can be rewritten.
- Graph growth can outpace simple tables.

Acceptance criteria:

- Users can answer "where did I fix this before?" and "what sessions touched
  this file?" reliably.

### P04 - Replay and Reproduction Tooling

Priority: P2  
Effort: L

Make old sessions useful for debugging and regression testing.

First slice:

- Export a replay package for a session: prompts, tool calls/results, source
  coordinates, environment hints, and files referenced where available.
- Redacted replay mode.
- Test harness for normalizer regression using real sessions.

Edge cases:

- Tool calls may be destructive or depend on old external state.
- Replay must not auto-execute commands by default.
- Missing files and rewritten git history.
- Secrets in commands or outputs.

Acceptance criteria:

- Replay packages are read-only by default and safe to inspect.
- Normalizer regressions can be reproduced from archived traces.

### P05 - Project-Aware Retrieval Profiles

Priority: P2  
Effort: M

Make retrieval aware of the current repo/project without hiding global search.

First slice:

- Project detection from cwd, git remote, source path, and session metadata.
- Search filters and boosts for current project.
- User-configurable project aliases.

Edge cases:

- Monorepos and nested repos.
- Same repo on multiple machines with different paths.
- Imported remote sessions.
- Cross-project work should still be discoverable.

Acceptance criteria:

- Agents get more relevant defaults while users can broaden scope explicitly.

### P06 - Python, CLI, and HTTP Client Polish

Priority: P2  
Effort: M

Make Moraine a better library as well as a service.

First slice:

- Typed Python client for conversations, source health, search, and exports.
- Async Python support.
- CLI parity for monitor APIs.
- Stable JSON schemas for machine output.

Edge cases:

- API versioning.
- Cursor compatibility.
- Optional fields across schema versions.
- Local server not running.

Acceptance criteria:

- Common automation no longer needs ad hoc SQL.
- Client tests run against sandbox fixtures.

### P07 - Monitor Accessibility, Keyboard, and Mobile Pass

Priority: P2  
Effort: M

The monitor will become a daily tool. Treat it like one.

First slice:

- Keyboard navigation for search, session list, transcript, and source panels.
- Accessible labels and focus states.
- Responsive layouts for narrow screens.
- Color contrast audit.

Edge cases:

- Very long paths and unbroken tokens.
- Dynamic content should not shift layout.
- Tool outputs can contain code blocks and tables.

Acceptance criteria:

- UI passes automated accessibility checks and manual keyboard smoke tests.

### P08 - Performance Tuning and Query Plan Snapshots

Priority: P2  
Effort: L

Keep query and ingest performance visible as the corpus grows.

First slice:

- Add representative benchmark corpora.
- Store query plan snapshots for core searches and source-health queries.
- Evaluate primary keys, projections, skipping indexes, and materialized views
  against measured workload, not guesses.
- Add latency budgets to CI or nightly jobs.

Edge cases:

- ClickHouse planner changes across versions.
- Projections and skipping indexes can slow ingest or increase storage.
- `FINAL` can be expensive.
- Laptop hardware varies widely.

Acceptance criteria:

- Performance PRs include before/after numbers.
- Regressions are caught before users notice.

### P09 - Policy Engine for Retrieval and Exports

Priority: P2  
Effort: L

Add explicit policy for what can be retrieved, exported, or shown to agents.

First slice:

- Declarative policies by source, project, event class, age, privacy status,
  and destination.
- Enforcement points: MCP tools, monitor raw payload view, export, and Python
  client.
- Audit log for denied or filtered access.

Edge cases:

- Local single-user mode should not become cumbersome.
- Policy must be fail-closed for sensitive destinations.
- Policies need dry-run/explain mode.
- Encrypted rows and redacted rows need different semantics.

Acceptance criteria:

- Users can explain why a result was hidden or allowed.
- MCP cannot bypass policy through another tool format.

### P10 - Adapter Marketplace and Plugin Packaging

Priority: P2  
Effort: XL

Long term, harness support should scale beyond core maintainers.

First slice:

- Define adapter package metadata, fixture requirements, schema version, and
  compatibility tests.
- Start with in-tree adapters, but design for external crates later.
- Document review checklist for adapters.

Edge cases:

- Untrusted adapter code.
- Adapter version mismatch with Moraine schema.
- Privacy behavior must be consistent.
- Fixtures may contain proprietary traces.

Acceptance criteria:

- Third-party adapter proposals are reviewable without understanding all core
  internals.

## P3 Details

### S01 - Team and Multi-User Mode

Priority: P3  
Effort: XL

Enable shared corpora only after local backup, privacy, policy, and auth are
solid.

First slice:

- Tenancy model.
- User identity and row ownership.
- Access control for source/project/session.
- Shared monitor with auth.

Edge cases:

- Personal traces mixed with team traces.
- Right to delete.
- Per-user encryption keys.
- MCP clients acting on behalf of different users.

Acceptance criteria:

- Team mode cannot expose one user's private memory to another by default.

### S02 - Hosted or Remote-Server Mode

Priority: P3  
Effort: XL

Support remote access with proper MCP authorization instead of assuming local
stdio trust.

First slice:

- HTTP transport hardening.
- OAuth 2.1 style authorization for protected MCP servers where applicable.
- Token audience validation, scope minimization, and no token passthrough.
- Admin UI for keys, users, and scopes.

Edge cases:

- Local stdio and remote HTTP need different auth models.
- HTTPS and localhost redirect rules.
- Session hijacking and confused-deputy risk.
- Resource server metadata and client compatibility.

Acceptance criteria:

- Remote mode is not just local mode exposed on a port.

### S03 - Cross-Device Sync and Conflict Resolution

Priority: P3  
Effort: XL

Move beyond one-way remote mirrors to bidirectional personal sync.

First slice:

- Device identity.
- Sync manifests and tombstones.
- Conflict rules for notes, summaries, and imported source rows.
- Optional encrypted sync bundle.

Edge cases:

- Same source ingested on two machines.
- Clock skew.
- Deleted sessions.
- Divergent privacy policies.

Acceptance criteria:

- Sync is deterministic and auditable.

### S04 - Desktop App or Browser Extension

Priority: P3  
Effort: XL

Improve discoverability and capture, but keep core services independent.

First slice:

- Desktop wrapper for monitor, stack status, notifications, and backup prompts.
- Browser capture extension for web research sessions only with explicit user
  consent.

Edge cases:

- Browser content can contain secrets.
- Extension permissions are high-risk.
- Desktop packaging and auto-update need signing.

Acceptance criteria:

- App/extension adds convenience without becoming required infrastructure.

### S05 - Agent QA and Anomaly Detection

Priority: P3  
Effort: XL

Use the corpus to detect risky or low-quality agent behavior.

First slice:

- Detect loops, repeated failed commands, high-error sessions, excessive tool
  calls, abrupt model switches, and long-running unresolved tasks.
- Build dashboards and optional alerts.
- Tie findings to sessions and source events.

Edge cases:

- Some long loops are legitimate debugging.
- Models and harnesses produce different event shapes.
- False positives can erode trust.

Acceptance criteria:

- Findings are explainable and never overwrite source trace data.

### S06 - Multimodal Trace Ingestion

Priority: P3  
Effort: XL

Some future harnesses will store screenshots, images, audio, or rendered
artifacts. Plan for them but do not let them destabilize text trace ingest.

First slice:

- Attachment metadata table.
- Content-addressed local blob store.
- Optional OCR/transcription pipeline.
- MCP resources for attachments with size and MIME checks.

Edge cases:

- Prompt injection can be hidden in images or documents.
- Large files and GIF/frame bombs.
- Copyright and privacy concerns.
- OCR can hallucinate or omit text.

Acceptance criteria:

- Binary data is bounded, typed, and never inserted into prompts without clear
  user control.

## Cross-Cutting Edge-Case Checklist

Use this checklist when designing any feature above.

### Filesystem and Source Input

- Partial JSON lines and truncated writes.
- File rotation, truncation, deletion, and recreation.
- Duplicate lines or records after crash/reconcile.
- Non-UTF-8 bytes.
- Huge records and payload size caps.
- Symlinks, hard links, NFS, iCloud, Dropbox, and external disks.
- Permission denied in nested source directories.
- WAL sidecars for SQLite sources.
- Source paths changing across machines.
- Multiple sources producing the same session ID.
- Untimestamped metadata and synthetic timestamp ordering.

### ClickHouse and Storage

- Insert acknowledgment vs async buffering semantics.
- Too many small parts from tiny inserts.
- `ReplacingMergeTree` eventual convergence.
- Expensive `FINAL` reads.
- TTL applying during background merges rather than immediately.
- Schema changes that break dependent views or materialized views.
- Version-specific SQL syntax and planner behavior.
- Disk pressure, backup pressure, and part merge pressure.
- Query planner changes after ClickHouse upgrades.

### Privacy and Security

- Non-retroactive redaction.
- Secrets in raw JSON, text content, payload JSON, tool I/O, ingest errors,
  logs, spools, exports, and backups.
- Key rotation and lost keys.
- Prompt injection in retrieved memory.
- Sensitive information disclosure through MCP or monitor.
- Excessive agency when agents act on retrieved memory.
- Unbounded output, query, or export size.
- Auditability of policy decisions.

### MCP and Client Compatibility

- Protocol version negotiation.
- Host support differences for tools, resources, prompts, structured content,
  and pagination.
- Tool output schema drift.
- Invalid cursors and opaque cursor handling.
- Prose mode vs full structured mode.
- Tool execution errors vs JSON-RPC protocol errors.
- Safety metadata preservation in every response.
- Remote HTTP authorization differs from local stdio trust.

### Monitor and UX

- Partial backend query failures.
- Large sessions and huge tool outputs.
- Long file paths and unbroken tokens.
- Time zones and clock skew.
- Accessibility and keyboard navigation.
- Source-health warnings after laptop sleep.
- Empty states that explain what to do next.

### Release and Operations

- Dirty worktrees and local-only files.
- Sandbox vs host install differences.
- Backup before migrations or reindexing.
- Rollback after failed install/update.
- Signed artifacts, SBOMs, and provenance.
- Clear support window for runtime versions.

## Suggested PR Breakdown

The roadmap is intentionally too large for one branch. A practical first wave:

1. `feat(cli): add backup manifest and verify command`
2. `feat(cli): add doctor schema and source integrity checks`
3. `feat(ingest): add reindex and search rebuild orchestration`
4. `feat(source): expose file and error drilldown APIs`
5. `feat(monitor): add source drilldown UI`
6. `feat(mcp): add conformance snapshot tests`
7. `feat(search): add relevance telemetry and evaluation fixtures`
8. `feat(search): add field-weighted lexical ranking`
9. `feat(imports): add remote import profiles`
10. `feat(privacy): add policy version audit`

Do not start with hosted/team/cloud mode. Those features multiply the cost of
auth, privacy, backups, and policy enforcement. They should build on a local
system that already has durable backup, clear source diagnostics, and strong
retrieval safety.

## Definition Of Done For Roadmap Features

Every feature above should include:

- A focused design note when it touches schema, privacy, MCP contracts, or
  source identity.
- Unit tests close to the changed code.
- Sandbox validation for ingest, monitor, MCP, or schema behavior when relevant.
- Documentation updates under `docs/`.
- Backward compatibility notes for config, schema, CLI, and MCP contracts.
- Operational impact: backup/reindex requirement, disk growth, runtime version,
  privacy behavior, and failure mode.
- A monitor or CLI visibility path for new long-running behavior.

## Strategic Non-Goals For Now

- Do not make Moraine cloud-first.
- Do not expose raw payload JSON by default through MCP.
- Do not add semantic search as a default before lexical evaluation exists.
- Do not treat summaries as ground truth without provenance.
- Do not add remote HTTP MCP access without real authorization and audience
  validation.
- Do not broaden storage retention or exports without privacy policy metadata.
- Do not make adapter authors bypass shared source-health and privacy behavior.

