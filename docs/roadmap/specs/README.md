# Moraine Strategic Specification Index — P2/P3 Features

This directory contains concrete design specifications for roadmap features rated P2 and P3. These features are intentionally too large or cross-cutting to implement in a single PR. Each document provides:

- **Design principles** anchored to existing architecture decisions.
- **Schema sketches** showing new or altered ClickHouse tables and views.
- **API sketches** for MCP tools, CLI commands, monitor endpoints, and Python client surfaces.
- **Data-flow diagrams** in prose (source → ingest → storage → retrieval → UI).
- **Edge-case matrices** with mitigations.
- **Acceptance contracts** — testable criteria that future PRs must satisfy.
- **PR sequencing** — a recommended merge order that keeps `main` green.
- **Dependencies** — prerequisite P0/P1 work that must land first.

## Specification Directory

| Spec | Feature | Priority | Effort | Depends On |
|---|---|---|---|---|
| [P01](P01-session-summaries.md) | Session summaries and memory cards | P2 | XL | C05 (session explorer), C07 (relevance loop), C10 (MCP resources/prompts) |
| [P02](P02-knowledge-notes.md) | User-curated corrections and knowledge notes | P2 | L | P01 (summaries), C05 (session explorer) |
| [P03](P03-entity-graph.md) | Entity and graph layer | P2 | XL | C01 (adapter registry), C05 (session explorer), C08 (phrase search) |
| [P04](P04-replay.md) | Replay and reproduction tooling | P2 | L | C11 (export archives), R01 (backup/restore), C05 (session explorer) |
| [P05](P05-project-profiles.md) | Project-aware retrieval profiles | P2 | M | C08 (field-weighted search), C06 (query workbench) |
| [P06](P06-client-polish.md) | Python, CLI, and HTTP client polish | P2 | M | C04 (source drilldown), C06 (query workbench) |
| [P07](P07-monitor-ux.md) | Monitor accessibility, keyboard, and mobile pass | P2 | M | C04 (source drilldown), C05 (session explorer) |
| [P08](P08-performance.md) | Performance tuning and query plan snapshots | P2 | L | C07 (relevance loop), C12 (retention policy) |
| [P09](P09-policy-engine.md) | Policy engine for retrieval and exports | P2 | L | R09 (privacy encryption), R12 (retrieval controls), C10 (MCP resources) |
| [P10](P10-adapter-marketplace.md) | Adapter marketplace and plugin packaging | P2 | XL | C01 (source trait/registry), C16 (fixture generator) |
| [S01](S01-team-mode.md) | Team and multi-user mode | P3 | XL | R09 (privacy encryption), P09 (policy engine), S02 (auth primitives) |
| [S02](S02-hosted-mode.md) | Hosted or remote-server mode | P3 | XL | R09 (privacy encryption), R11 (MCP conformance), S01 (tenancy model) |
| [S03](S03-cross-device-sync.md) | Cross-device sync and conflict resolution | P3 | XL | R01 (backup/restore), C02 (remote import), S01 (identity) |
| [S04](S04-desktop-extension.md) | Desktop app or browser extension | P3 | XL | C03 (config wizard), S02 (remote HTTP mode) |
| [S05](S05-anomaly-detection.md) | Agent QA and anomaly detection | P3 | XL | P01 (summaries), P03 (graph layer), C13 (OTel export) |
| [S06](S06-multimodal.md) | Multimodal trace ingestion | P3 | XL | P03 (entity/attachment layer), C13 (OTel export) |

## Cross-Cutting Architectural Constraints

All specifications below must respect the following invariants derived from current ADRs:

1. **ADR-001 (ClickHouse as system DB):** New features that need durable state must prefer ClickHouse tables/views over external stores or in-process caches. If a feature truly needs an embedded store (e.g., local sync tombstones), that must be justified explicitly.
2. **ADR-004 (Preserve raw payloads):** Features that transform or summarize events must keep provenance links back to source event UIDs and raw JSON coordinates. Summaries are projections, not replacements.
3. **ADR-005 (At-least-once ingest):** Features that write derived data (e.g., summaries, graph edges) must be idempotent or versioned so that reprocessing does not create logical duplicates.
4. **ADR-010 (Strict MCP schemas):** Any new MCP tools, resources, or prompts must publish `inputSchema` with `additionalProperties = false`, tool-specific `outputSchema`, and the `_safety` envelope.
5. **ADR-011 (Non-retroactive privacy):** Features that read historical rows must respect the privacy policy version stored on those rows. Re-processing to a new policy requires explicit user action (backup → rebuild).

## Acceptance Contract Template

Every spec uses a common acceptance contract structure:

- **Functional:** What the feature must do under normal conditions.
- **Operational:** How the feature behaves under pressure (disk full, DB down, large corpora).
- **Safety:** How the feature preserves privacy, retrieval safety, and non-trusted-memory semantics.
- **Compatibility:** Backward compatibility guarantees for config, schema, CLI, and MCP contracts.
- **Observability:** How operators can verify the feature is working correctly.

## PR Sequencing Conventions

PRs are labeled with Conventional Commit-style subjects:

- `feat(<scope>): ...` for new behavior.
- `schema(<scope>): ...` for SQL migrations.
- `docs(<scope>): ...` for design docs and user docs.
- `test(<scope>): ...` for evaluation fixtures or conformance tests.
- `refactor(<scope>): ...` for pure restructurings that unblock later PRs.

Each PR in a sequence should be reviewable in under 400 lines of diff (excluding fixtures and generated code). If a PR must be larger, it should be split by surface (schema first, then backend, then UI/API).

## Reading Order

If you are implementing one of these features, read in this order:

1. This README (constraints and conventions).
2. The relevant spec file (design and API).
3. The roadmap’s P0/P1 dependency items (linked in each spec).
4. Existing ADRs in `docs/architecture/design-tradeoffs.md`.
5. The schema files in `sql/` for current table contracts.

---

*Generated from `docs/roadmap/feature-roadmap-2026-04-20.md`. These specs are living documents: update them when dependencies land or when implementation reveals better designs.*
