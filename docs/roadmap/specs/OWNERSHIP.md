# Ownership and Review Boundaries — P2/P3 Features

This document maps each strategic spec to the crates, apps, SQL layers, and review domains that will own the implementation. It is intended for branch planning, code-review routing, and sandbox validation assignment.

## Ownership Model

Moraine uses a **layer ownership** model:

| Layer | Owner(s) | Review Focus |
|---|---|---|
| **Schema (`sql/`)** | Core maintainers + feature author | Engine choice, migration order, backward compatibility, MV fanout cost |
| **Ingest (`moraine-ingest-core`, `moraine-ingest`)** | Ingest maintainers | Checkpoint safety, privacy transform ordering, source-health semantics, backpressure |
| **Retrieval (`moraine-conversations`, `moraine-mcp-core`, `moraine-mcp`)** | Retrieval maintainers | Query cost, MCP schema strictness, `_safety` envelope, pagination, cursor stability |
| **Privacy (`moraine-privacy`)** | Privacy maintainers | Redaction/encryption ordering, policy version propagation, non-retroactive guarantees |
| **Monitor (`moraine-monitor-core`, `moraine-monitor`, `web/monitor`)** | UI maintainers | API contract fidelity, responsive design, keyboard/a11y, partial-failure UX |
| **Control Plane (`moraine`, `moraine-config`)** | Core maintainers | CLI UX, config validation, service lifecycle, sandbox compatibility |
| **Bindings (`bindings/python/`)** | Client maintainers | Type safety, async/sync parity, schema drift detection, error ergonomics |

## Feature-to-Owner Mapping

| Spec | Primary Crate/App | Secondary Crates | Schema Touch | Review Domains |
|---|---|---|---|---|
| **P01** Session summaries | `moraine-conversations` (backend), `moraine-mcp-core` | `moraine-ingest-core` (async queue), `web/monitor` | `summaries`, `memory_cards`, `summary_provenance`, MV into `search_documents` | Retrieval safety, LLM generation determinism, provenance integrity |
| **P02** Knowledge notes | `moraine-conversations` | `moraine-mcp-core`, `web/monitor` | `notes`, `note_links`, `note_revisions`, MV into `search_documents` | Authorship attribution, revision audit, search index bloat |
| **P03** Entity graph | New: `moraine-entities-core` | `moraine-conversations`, `moraine-mcp-core`, `web/monitor` | `entities`, `entity_occurrences`, `entity_edges`, `entity_resolution` | Extraction accuracy, graph query boundedness, fuzzy resolution correctness |
| **P04** Replay | New: `moraine-replay-core` | `moraine`, `moraine-conversations` | None (external archive format) | Archive integrity, privacy redaction in exports, cross-version compatibility |
| **P05** Project profiles | `moraine-conversations` | `moraine-mcp-core`, `web/monitor` | `project_profiles`, `session_projects` | Project detection accuracy, ranking boost fairness, backward compatibility of search |
| **P06** Client polish | `bindings/python/`, `apps/moraine` | `moraine-monitor-core` (HTTP API stabilization) | None (API-only) | OpenAPI correctness, Python type safety, CLI parity completeness |
| **P07** Monitor UX | `web/monitor` | `moraine-monitor-core` (lightweight status endpoint) | Optional: `user_preferences` | a11y audit, keyboard navigation, responsive breakpoints, bundle size |
| **P08** Performance | New: `moraine-perf-core` or scripts | `moraine-conversations`, `moraine-clickhouse` | `query_plan_snapshots`, `performance_benchmarks` | Benchmark reproducibility, fixture realism, CI runtime cost |
| **P09** Policy engine | New: `moraine-policy-core` | `moraine-conversations`, `moraine-mcp-core`, `moraine-privacy` | `policy_rules`, `policy_audit_log` | Fail-closed correctness, audit completeness, performance overhead per row |
| **P10** Adapter marketplace | `moraine-ingest-core` | New: `moraine-adapter-conformance` | None (manifest-driven) | Trait stability, conformance harness rigor, external code isolation |
| **S01** Team mode | `moraine-conversations`, `moraine-privacy` | `moraine-mcp-core`, `web/monitor` | `tenant_id`/`user_id` on all canonical tables; `tenants`, `users`, `project_memberships`, `source_permissions` | Tenant isolation (security-critical), encryption key handling, row-level security |
| **S02** Hosted mode | New: `moraine-mcp-transport`, auth crate | `moraine-monitor-core` (admin UI), `moraine-mcp-core` | `oauth_clients`, `access_tokens`, `auth_audit_log` | OAuth correctness, TLS hardening, scope enforcement, token lifecycle |
| **S03** Cross-device sync | New: `moraine-sync-core` | `moraine-privacy`, `moraine` | `devices`, `sync_manifests`, `tombstones` | Cryptographic correctness, delta efficiency, conflict determinism, blob integrity |
| **S04** Desktop / extension | `web/monitor` (shared UI), new `desktop/`, new `browser-extension/` | `moraine-monitor-core` | `browser_captures` (optional) | Code signing, auto-update security, extension permission minimization |
| **S05** Anomaly detection | New: `moraine-anomalies-core` | `moraine-conversations`, `web/monitor` | `anomaly_detectors`, `anomaly_findings`, `anomaly_metrics` | False positive rate, detector tunability, evidence provenance |
| **S06** Multimodal | `moraine-ingest-core` (normalizer), New: `moraine-attachments-core` | `moraine-conversations`, `web/monitor` | `attachments`, `attachment_links` | Blob store integrity, OCR accuracy, privacy on extracted text, size bounding |

## New Crate Guidelines

When a spec calls for a new crate, follow these rules:

1. **Keep binaries thin.** New logic belongs in `crates/`, not `apps/`.
2. **Name convention:** `moraine-<feature>-core` for reusable logic; `moraine-<feature>` only if a standalone binary is required.
3. **Dependency discipline:** New crates may depend on `moraine-config`, `moraine-clickhouse`, and `moraine-conversations`. They must NOT depend on `apps/*` or `web/monitor`.
4. **Test placement:** Unit tests inside the crate; integration tests in `tests/` at workspace root or in the crate's own `tests/` directory.

## Sandbox Validation Owners

For each feature, the **primary crate owner** is responsible for sandbox validation:

| Feature | Sandbox Validation Command |
|---|---|
| P01–P03, P05, P08, P09 | `cargo test -p <primary-crate> --locked` + `scripts/ci/e2e-stack.sh` |
| P04, P06 | `cargo test --workspace --locked` + Python client tests in sandbox |
| P07 | `make docs-build` + manual keyboard smoke test in sandbox monitor |
| P10 | `cargo test -p moraine-adapter-conformance --locked` + fixture validation |
| S01–S03 | Security-critical: dedicated `test/security/isolation.rs` + sandbox multi-user fixture |
| S04 | Build Tauri app in sandbox Linux container; verify no host-side leaks |
| S05 | `cargo test -p moraine-anomalies-core --locked` + golden fixture precision/recall |
| S06 | `cargo test -p moraine-attachments-core --locked` + OCR accuracy fixtures |

## Review Checklist by Domain

### Schema Review (all features)
- [ ] Table engine matches lifecycle (MergeTree vs ReplacingMergeTree).
- [ ] Partition key does not create excessive part counts.
- [ ] ORDER BY key supports the dominant query pattern.
- [ ] ALTER statements are idempotent and backward compatible.
- [ ] New MVs do not break existing MV fanout chain.

### MCP Review (P01–P06, P08–P10, S01–S03, S05–S06)
- [ ] `inputSchema` has `additionalProperties: false`.
- [ ] `outputSchema` is published in `tools/list`.
- [ ] `_safety` envelope present in full mode; preamble present in prose mode.
- [ ] Strict mode is reducing-only (no new exposure paths).
- [ ] Cursors are opaque and deterministic.

### Privacy Review (P01–P04, P09, S01–S04, S06)
- [ ] New tables include `privacy_policy_version` where user-facing.
- [ ] Raw payloads do not leak through new APIs in default/strict mode.
- [ ] Backup/restore handles new tables correctly (R01).

### Monitor Review (P01–P09, S01–S06)
- [ ] New endpoints return partial results on query failure, not blanket 500.
- [ ] Empty states explain what to do next.
- [ ] Mobile layout does not truncate actionable buttons.

---

*This ownership map is a living document. Update it when crates are renamed, responsibilities shift, or new layers are introduced.*
