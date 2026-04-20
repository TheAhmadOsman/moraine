# P10 — Adapter Marketplace and Plugin Packaging

**Priority:** P2  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** C01 (source trait/registry), C16 (fixture generator)

## Objective

Long term, harness support should scale beyond core maintainers. Define adapter package metadata, fixture requirements, schema version compatibility, and review checklist so third-party adapter proposals are reviewable without understanding all core internals.

## Design Principles

1. **Adapters are self-contained packages.** A package includes metadata, normalization logic, fixture data, and conformance tests. [src: ADR-004]
2. **Core defines the contract; adapters implement it.** The `SourceAdapter` trait (C01) is the boundary. Core handles scheduling, checkpointing, privacy, and health.
3. **Privacy and source-health are non-negotiable.** Adapters cannot bypass shared redaction or health reporting. [src: ADR-009, ADR-011]
4. **Review is lightweight but rigorous.** A checklist and automated conformance tests replace deep code archeology for reviewers.

## Schema Design

No new ClickHouse tables. This feature is about packaging, metadata, and process.

### Adapter Package Manifest

```toml
# adapter.toml — required in every adapter package root
[adapter]
name = "my-agent"
version = "1.0.0"
schema_version = 12          # moraine schema version this adapter targets
mcp_tools_version = "1.2.0"  # optional: MCP tools this adapter emits
author = "Jane Doe <jane@example.com>"
license = "MIT"
repository = "https://github.com/janedoe/moraine-my-agent-adapter"

[adapter.source]
kind = "file"                # "file" | "sqlite" | "directory" | "api"
format = "jsonl"             # mime-type or custom identifier
glob = "**/*.myagent.jsonl"
watch_extensions = [".jsonl"]
wal_extensions = []          # for sqlite sources

[adapter.normalizer]
entrypoint = "normalize"     # function name in the adapter
supported_event_kinds = ["message", "tool_call", "tool_result", "reasoning"]
deterministic_uid = true     # must produce stable event_uids for same input

[adapter.fixtures]
required_count = 3           # minimum fixture sessions
edge_cases = ["empty_file", "partial_json", "huge_payload", "unicode", "rotated_file"]

[adapter.privacy]
redaction_supported = true
encryption_supported = false # adapters are not required to support encryption
```

### Adapter Directory Structure

```
moraine-my-agent-adapter/
  adapter.toml
  src/
    lib.rs          # implements SourceAdapter trait
    normalize.rs    # format-specific normalization
    fixtures.rs     # fixture generation helpers
  fixtures/
    minimal/
      session_1.jsonl
      session_2.jsonl
    edge_cases/
      empty_file.jsonl
      partial_json.jsonl
      huge_payload.jsonl
  tests/
    conformance.rs  # auto-run by moraine test harness
  README.md
  CHANGELOG.md
```

## API Sketches

### CLI Commands

```bash
moraine adapter list                    # list installed adapters (built-in + external)
moraine adapter install <path|git-url>  # compile and register external adapter
moraine adapter validate <path>         # run conformance tests against an adapter
moraine adapter test <adapter-name>     # run adapter's own test suite
moraine adapter uninstall <adapter-name>
moraine adapter publish --dry-run       # validate package before publishing
```

### Registry API (Future)

Not implemented in P2. Design for it:

```
GET /registry/adapters          # list published adapters
GET /registry/adapters/:name    # adapter metadata and download URL
GET /registry/adapters/:name/versions
```

## Adapter Conformance Test Harness

The core crate `moraine-adapter-conformance` provides:

1. **Schema validation:** Does adapter output match expected canonical event schema?
2. **Determinism test:** Same input → same event_uids, same row counts.
3. **Privacy test:** Run adapter through redaction pipeline; verify secrets are handled.
4. **Health test:** Adapter reports correct source-health status for happy/sad paths.
5. **Fixture test:** Adapter provides required fixture count and passes edge-case handling.
6. **Performance test:** Adapter normalizes N events within budget.

```rust
// moraine-adapter-conformance/src/lib.rs
pub trait AdapterConformance {
    fn test_schema_validity(&self, fixtures: &[Fixture]) -> Result<()>;
    fn test_determinism(&self, fixture: &Fixture, runs: usize) -> Result<()>;
    fn test_privacy(&self, fixture: &Fixture, policy: &PrivacyPolicy) -> Result<()>;
    fn test_health_reporting(&self, source: &TestSource) -> Result<()>;
    fn test_fixture_coverage(&self) -> Result<()>;
    fn test_performance(&self, fixture: &Fixture, max_duration: Duration) -> Result<()>;
}
```

## Data Flow

1. **Author writes adapter:** Implements `SourceAdapter` trait, provides fixtures.
2. **Local validation:** `moraine adapter validate ./my-adapter` runs conformance harness.
3. **Install:** `moraine adapter install` compiles to shared library or wasm module, registers in `~/.moraine/adapters/`.
4. **Config:** User adds `[[ingest.sources]]` with `adapter = "my-agent"`.
5. **Runtime:** Ingestor loads adapter, dispatches through standard scheduling/privacy/health pipeline.
6. **Update:** Adapter version bumps trigger conformance re-validation on `moraine up`.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Untrusted adapter code | Adapters run in-process (Wasm sandbox or dlopen with restricted ABI). Start with in-tree only; external loading is opt-in with `--allow-external-adapters`. |
| Adapter version mismatch with Moraine schema | `adapter.toml` declares `schema_version`; core rejects adapters targeting incompatible versions. |
| Privacy behavior inconsistent | Conformance harness enforces redaction test; adapter cannot disable privacy pipeline. |
| Fixtures contain proprietary traces | Fixture data should be synthetic. Conformance harness checks for common secret patterns. |
| Adapter crash during ingest | Catch panics at adapter boundary; report source-health error; continue with other sources. |
| Adapter blocks ingest thread | Adapter calls are timeout-bounded; slow adapters produce warnings, not stalls. |

## Acceptance Contract

### Functional
- [ ] A new adapter can be written by implementing `SourceAdapter` without modifying core crates.
- [ ] `moraine adapter validate` runs all conformance tests and produces PASS/FAIL report.
- [ ] Adapter installation registers the adapter and makes it available in config.
- [ ] Built-in adapters (codex, claude, kimi, opencode, hermes) pass the same conformance harness.

### Operational
- [ ] Adapter load time < 100ms at startup.
- [ ] Adapter crash does not crash the ingestor (isolated error boundary).
- [ ] External adapter directory (`~/.moraine/adapters/`) is included in backup/restore (R01).

### Safety
- [ ] External adapters cannot bypass privacy redaction or source-health reporting.
- [ ] External adapters cannot read/write outside their configured source paths.
- [ ] Adapter manifest includes license and author for auditability.

### Compatibility
- [ ] `SourceAdapter` trait is versioned; breaking changes require trait version bump.
- [ ] Existing built-in adapters continue to work without changes when trait is extended (default methods).

### Observability
- [ ] Monitor shows installed adapters, versions, and conformance status.
- [ ] `moraine doctor` checks for adapter schema version mismatches.

## PR Sequencing

1. `refactor(adapter): stabilize SourceAdapter trait and extract built-in adapters`  
   - C01 work; this spec depends on it.
2. `feat(adapter): add adapter package manifest and directory structure`  
   - `adapter.toml` schema; validation logic.
3. `feat(adapter): add conformance test harness crate`  
   - `moraine-adapter-conformance` with determinism, privacy, health tests.
4. `feat(cli): add adapter list, validate, install, uninstall commands`  
   - CLI surface.
5. `feat(adapter): port one built-in adapter to package format as reference`  
   - e.g., codex or kimi adapter.
6. `feat(monitor): add adapter registry panel`  
   - List installed adapters, conformance status, version info.
7. `docs(adapter): publish adapter author guide and review checklist`  
   - `docs/operations/writing-adapters.md`.

## Open Questions

1. **Wasm vs native shared libraries:** Wasm provides sandboxing but adds overhead. Recommendation: start with native Rust dylibs loaded via `dlopen`/`libloading` with panic isolation. Evaluate Wasm for untrusted adapters later.
2. **Package distribution:** Git URL vs registry vs local path? Start with local path and git URL. Registry is P3.
3. **Should adapters be written in languages other than Rust?** Not in P2. Rust-only keeps ABI simple and performance predictable. Python/JS bindings are P3.
