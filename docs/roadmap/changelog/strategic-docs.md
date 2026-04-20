# Strategic Documentation Changelog — 2026-04-20

## Added: P2/P3 Feature Specifications

Created the first strategic documentation slice for all P2 (richness/polish) and P3 (strategic expansion) roadmap features. These specs are intentionally too large for immediate implementation; they unblock future work by providing concrete design contracts.

### New Documents

| Document | What It Covers |
|---|---|
| [`specs/README.md`](../specs/README.md) | Index, cross-cutting ADR constraints, acceptance contract template, PR sequencing conventions |
| [`specs/OWNERSHIP.md`](../specs/OWNERSHIP.md) | Feature-to-crate mapping, review boundaries, sandbox validation owners, domain checklists |
| [`specs/P01-session-summaries.md`](../specs/P01-session-summaries.md) | LLM-generated session summaries, memory cards, provenance links, async generation queue |
| [`specs/P02-knowledge-notes.md`](../specs/P02-knowledge-notes.md) | User-authored notes, revision history, search integration, link targets |
| [`specs/P03-entity-graph.md`](../specs/P03-entity-graph.md) | Entity extraction, fuzzy resolution, bounded graph traversal, adjacency views |
| [`specs/P04-replay.md`](../specs/P04-replay.md) | Replay package format, manifest schema, export/import, normalizer regression harness |
| [`specs/P05-project-profiles.md`](../specs/P05-project-profiles.md) | Project detection, scoped search, ranking boosts, profile portability |
| [`specs/P06-client-polish.md`](../specs/P06-client-polish.md) | Typed Python client, CLI parity, HTTP API stabilization, OpenAPI generation |
| [`specs/P07-monitor-ux.md`](../specs/P07-monitor-ux.md) | Keyboard navigation, accessibility pass, responsive layouts, theme switching |
| [`specs/P08-performance.md`](../specs/P08-performance.md) | Query plan snapshots, fixture benchmarks, latency budgets, physical design evaluation |
| [`specs/P09-policy-engine.md`](../specs/P09-policy-engine.md) | Declarative access rules, fail-closed enforcement, audit log, dry-run mode |
| [`specs/P10-adapter-marketplace.md`](../specs/P10-adapter-marketplace.md) | Adapter package format, conformance harness, manifest validation, third-party loading |
| [`specs/S01-team-mode.md`](../specs/S01-team-mode.md) | Tenancy model, row ownership, per-user encryption, project-level ACLs |
| [`specs/S02-hosted-mode.md`](../specs/S02-hosted-mode.md) | MCP over HTTP/SSE, OAuth 2.1, token scopes, admin UI, TLS hardening |
| [`specs/S03-cross-device-sync.md`](../specs/S03-cross-device-sync.md) | Device identity, encrypted sync bundles, delta computation, conflict resolution |
| [`specs/S04-desktop-extension.md`](../specs/S04-desktop-extension.md) | Tauri desktop wrapper, WebExtension capture, research session ingest |
| [`specs/S05-anomaly-detection.md`](../specs/S05-anomaly-detection.md) | Loop/error/tool-abuse detectors, statistical thresholds, evidence provenance |
| [`specs/S06-multimodal.md`](../specs/S06-multimodal.md) | Blob store, content-addressed dedup, OCR/transcription, attachment search |

### Schema Sketches Provided

Every spec includes `CREATE TABLE` / `CREATE VIEW` / `CREATE MATERIALIZED VIEW` sketches that follow existing conventions:

- `ReplacingMergeTree(event_version)` for idempotent projections
- `MergeTree` for append-only audit streams
- Partition keys aligned with query patterns (`toYYYYMM`, `cityHash64(...) % N`)
- Materialized view fanout into `search_documents` where searchable

### API Contracts Defined

- **MCP tools:** Strict `inputSchema` with `additionalProperties: false`, `outputSchema`, `_safety` envelope, and `safety_mode` behavior
- **CLI commands:** Complete command signatures with flags and behavior
- **Monitor endpoints:** HTTP paths, methods, request/response shapes

### PR Sequencing

Each spec breaks its feature into 6–9 reviewable PRs (typically <400 lines of diff each), ordered to keep `main` green:

1. Schema migration
2. Backend/core crate
3. MCP/CLI surface
4. Monitor UI
5. Evaluation fixtures / conformance tests

### Updated Documents

- [`feature-roadmap-2026-04-20.md`](../feature-roadmap-2026-04-20.md) — Added "Strategic Specifications" section pointing to `docs/roadmap/specs/`
- `mkdocs.yml` — Added specs, ownership, and changelog to navigation

---

*Next expected update: when P0/P1 dependencies land and specs are revised to reflect implementation constraints.*
