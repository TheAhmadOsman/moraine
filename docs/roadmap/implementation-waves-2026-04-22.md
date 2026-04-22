# Roadmap Implementation Waves - April 22, 2026

This page is the operational index for the roadmap implementation work that
followed the April 20, 2026 feature roadmap. It records what landed, what was
intentionally postponed, which branches are stale duplicates, and which slices
should be picked next.

Implementation status is current through `60c7bf6 feat(sources): add ingest
error quarantine`. Later documentation-only commits may update this page
without changing product behavior.

## Status Vocabulary

| Status | Meaning |
|---|---|
| Landed | The behavior is in `main` and has a changelog or operations doc. |
| Partial | A safe foundation landed, but the roadmap item still has meaningful follow-up work. |
| Parked | The prototype or branch exists, but it should not be merged as-is. |
| Superseded | The useful behavior landed through a different commit or cleaner branch. |
| Not started | No accepted implementation has landed. |

## Wave Summary

| Wave | Theme | Status | Primary commits | Docs |
|---|---|---|---|---|
| 0 | Roadmap and strategic specs | Landed | `085b258`, `213e13b`, `1eb5c1a` | [feature roadmap](feature-roadmap-2026-04-20.md), [strategic specs](specs/README.md), [strategic docs changelog](changelog/strategic-docs.md) |
| 1 | Core source, MCP, OpenCode, and config foundations | Landed | `43f3a6c`, `438daac`, `36cb2a6`, `7f1fc90`, `6270549` | [source monitor](changelog/source-monitor.md), [OpenCode hardening](changelog/opencode-hardening-fixtures.md), [MCP safety R11/R12](changelog/mcp-conformance-safety-r11-r12.md), [import/config/export](changelog/2026-04-20-import-config-export-slice.md) |
| 2 | Operational safety foundations | Landed/partial | `11a0ca7`, `8377a8b`, `8daff9a`, `18de791`, `9553e1e`, `3f1b505`, `52cb140`, `6917c4f` | [privacy encryption](changelog/privacy-encryption.md), [backup/restore](changelog/backup-restore-cli.md), [MCP prompts/resources](changelog/mcp-prompts-resources.md), [reindex search rebuild](changelog/reindex-search-rebuild.md), [deep doctor](changelog/doctor-deep-core.md) |
| 3 | Runtime detail, migration gates, and MCP conformance | Landed | `62a96db`, `58c27bf`, `b0f6a4b`, `863ba2f`, `1ab1101` | [source file runtime detail](changelog/source-file-runtime-detail.md), [migration safety](changelog/migration-safety-compat.md), [MCP conformance corpus](changelog/mcp-conformance-corpus.md) |
| 4 | Operational closure and local corpus hygiene | Landed/partial | `53810f7`, `9349263`, `0011dfb`, `cf2d922`, `a414a4c`, `1700e3a`, `60c7bf6` | [import manifests](changelog/import-mirror-manifests.md), [source drift](changelog/source-drift-diagnostics.md), [operational e2e](changelog/operational-safety-e2e.md), [search quality evaluation](changelog/search-quality-evaluation.md), [source error quarantine](changelog/source-error-quarantine.md) |
| Source correctness follow-ups | Kimi and local source noise corrections | Landed | `ba64c81`, `6947178`, `1e56ca7`, `60c7bf6` | [source drift](changelog/source-drift-diagnostics.md), [source error quarantine](changelog/source-error-quarantine.md) |

## Landed By Roadmap Area

| Roadmap ID | Current status | Landed behavior | Remaining work |
|---|---|---|---|
| R01 Backup and restore CLI | Partial | Verified ClickHouse backups, backup listing, manifest/checksum verification, restore dry-run, and staging restore execution. | Active database replacement, richer post-restore doctor automation, backup compatibility matrix, and recovery runbooks. |
| R02 Clean reindex and search rebuild orchestration | Partial | `moraine reindex --search-only` supports dry-run, execute, batching, and resume. | Full canonical raw-source replay remains separate; privacy-policy reprocessing still needs explicit orchestration. |
| R03 Database doctor and integrity audit | Partial | Deep doctor findings expose orphan/raw/event/time-range issues as structured findings. | Host corpus repair workflow is intentionally postponed until a controlled rebuild or repair plan is chosen. |
| R04 Migration safety gates | Landed | `up`, `db migrate`, and search reindex execute paths require a recent verified backup when pending migrations or destructive derived-table rebuilds make that appropriate. | Extend gate coverage when active restore and privacy reprocess commands exist. |
| R05 ClickHouse version compatibility matrix | Landed | Doctor/status classify supported, experimental, unsupported, and unknown ClickHouse versions. | Keep the matrix updated with release pins. |
| R06 Disk-backed ingest retry spool | Not started | A prototype was rejected because it touched sink, shutdown, replay, privacy, and retry semantics too broadly. | Reimplement as a dedicated design with bounded spool format, replay ordering, privacy semantics, and failure visibility. |
| R07 Deep source diagnostics API and CLI | Landed | Shared source status, source files, source errors, source drift, runtime lag, and partial-query handling. | Add repair/remediation commands only after source adapter boundaries are clearer. |
| R08 OpenCode adapter hardening | Partial | WAL sibling mapping, strict watermark paging, schema drift diagnostics, safer MCP schema for OpenCode-shaped payloads, and schema fixture utilities. | Broader OpenCode schema compatibility matrix and upgrade-remediation docs. |
| R09 Real privacy encryption and key management | Partial | AES-256-GCM `encrypt_raw`, env/file key loading, privacy metadata columns, and fail-closed missing-key behavior. | Key rotation, decrypt/export admin workflow, key backup checks, and re-encryption policy. |
| R10 Redaction migration and privacy audit workflow | Partial | Privacy metadata foundation exists, and ingest policy is explicit and non-retroactive. | Audit CLI, policy migration planning, and historical reprocess workflow. |
| R11 MCP conformance and safety regression suite | Landed/partial | Tool schema hardening, output schemas, conformance corpus, resources, prompts, and MCP smoke coverage. | Broader hostile-memory corpus and multi-client compatibility fixtures. |
| R12 Untrusted-memory retrieval controls | Landed/partial | Safety envelope, prose preamble, strict/normal safety modes, truncation counters, and defaults that do not broaden payload exposure. | More adversarial retrieval tests and user-facing policy presets. |
| R13 Release signing, SBOM, provenance, and checksums | Not started | None. | Define release artifact inventory, signing key policy, SBOM generation, and verification docs. |
| C01 Source trait and adapter registry | Not started | Existing source-specific branches remain centralized in ingest dispatch. | Design and land adapter boundaries before adding more source types. |
| C02 Remote import profiles and sync automation | Partial | Import status, dry-run, execute mode, rsync-backed sync, and versioned mirror manifests with last-success state. | Scheduling, retry visibility, conflict handling, and monitor integration. |
| C03 Config wizard and source auto-discovery | Partial | Config validation, source detection, and wizard foundation exist. | Safer interactive edits, profile templating, and drift-aware suggestions. |
| C04 Monitor source drilldown | Landed/partial | Source strip, detail panel, file/runtime/error drilldown, drift-facing counts, and ignored error badges. | File timelines, remediation actions, and better empty/disabled source treatment. |
| C05 Monitor session explorer | Not started | None beyond existing monitor session surfaces. | Rich transcript explorer, filtering, compare views, and linked source provenance. |
| C06 Query workbench and saved searches | Not started | Search quality tooling exists, but no UI workbench. | Saved searches, result inspection, query variants, and exportable reports. |
| C07 Search relevance evaluation loop | Partial | Offline/live search quality harness with qrels, fixtures, and standard IR metrics. | Curated local qrels, CI thresholds, feedback capture, and ranking experiments. |
| C08 Field-weighted, phrase, and proximity search | Not started | None. | Requires evaluation harness baselines before ranking changes. |
| C09 Optional semantic/hybrid retrieval | Not started | None. | Should wait until lexical evaluation and privacy boundaries are stable. |
| C10 MCP resources, prompts, and workflow templates | Landed/partial | Static guide resources, prompt catalog, dynamic resource templates, and safe retrieval workflows. | More host-specific recipes and saved retrieval workflows. |
| C11 Portable export/import archives | Partial | Preview/import/verify contract and blocked execute paths. | Live export/import execution and cross-version compatibility validation. |
| C12-C16 P1 operations and analytics | Not started/partial | Search evaluation and latency benchmark tools exist; source simulator pieces were partly added for OpenCode tests. | Retention, OTel, alerts, model analytics, and fixture simulator productization. |

## Branch And Worktree Triage

These branches are safe cleanup candidates because their useful patches are
already present in `main` or the branch is merged:

- `origin/feat/import-mirror-manifests`
- `origin/feat/opencode-sqlite-ingest-review`
- `origin/feat/reindex-clean-resume`
- `origin/feat/restore-execution-staging`
- `origin/feat/source-drift-diagnostics`
- `origin/impl/roadmap-foundations-2026-04-20`
- `origin/test/operational-safety-e2e`
- `privacy-redaction-foundation` - patch-equivalent to accepted privacy work.
- `search-eval-harness` - patch-equivalent to `1700e3a`.

These branches are superseded by reviewed/split work and should not be merged:

- `feat/kimi-cli-ingest`
- `feat/kimi-opencode-ingest`
- `feat/kimi-opencode-sources`
- `feat/opencode-sqlite-ingest`

These branches need porting, not merging:

- `feature/live-analytics-long-ranges`
- `fix/live-analytics-range-meta`

Both live-analytics branches include the old combined `f882f30` Kimi/OpenCode
line. Port only the monitor analytics commits onto current `main` if the feature
is still wanted.

## Parked Or Rejected Work

The following artifacts are preserved for reference, but they are not the
implementation source of truth:

- `.worker-runs/rejected/monitor-source-health.diff` - superseded by the shared
  `moraine-source-status` implementation.
- `.worker-runs/rejected/mcp-contract-refresh.diff` - split into accepted MCP
  tool schema and retrieval safety work.
- `.worker-runs/rejected/roadmap-privacy-policy-superseded.diff` - superseded
  by narrower privacy encryption and metadata slices.
- Disk-backed ingest spool prototype inside the OpenCode hardening wave - needs
  a dedicated design and branch.

## Postponed Local Operations

These are intentionally not product blockers, but they remain useful local ops
tasks:

1. Host corpus repair plan for deep-doctor findings such as orphaned rows and
   impossible time ranges.
2. Launchd-aware service status, or a clearer local note that launchd health,
   monitor health, and ClickHouse ping are the source of truth for this host.

## Recommended Next Implementation Order

1. Clean stale local/remote branches listed above.
2. Port live analytics onto a fresh branch only if the monitor analytics UX is
   still a priority.
3. Build C07/C08 search quality work on top of the landed evaluation harness:
   curate qrels, establish baselines, then tune field weights/phrase/proximity.
4. Start C05 monitor session explorer after search quality has a measurable
   baseline.
5. Return to R06 disk-backed ingest retry spool with a small design doc and a
   narrow first slice.
6. Continue R09/R10 privacy operations with key audit/decrypt/export tooling
   only after backup/restore confidence is sufficient.

## Documentation Source Of Truth

Use this hierarchy when references disagree:

1. Code and SQL in `main`.
2. Changelog fragments under `docs/roadmap/changelog/`.
3. This implementation wave index.
4. The feature roadmap and P2/P3 specs.
5. `.worker-runs` prompts, archived diffs, and rejected prototypes.

`.worker-runs` is useful audit history, but accepted user-facing design and
operator guidance should be promoted into `docs/` before more work builds on it.
