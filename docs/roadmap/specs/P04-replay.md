# P04 — Replay and Reproduction Tooling

**Priority:** P2  
**Effort:** L  
**Status:** Specification / ready for design review  
**Dependencies:** C11 (portable export archives), R01 (backup/restore), C05 (session explorer), P01 (summaries)

## Objective

Make old sessions useful for debugging and regression testing. A replay package recreates the context of a prior session — prompts, tool calls, source coordinates, and referenced files — so developers can inspect, diff, or rerun traces in a controlled environment.

## Design Principles

1. **Replay packages are read-only by default.** They must be safe to inspect without auto-executing commands. [src: ADR-004]
2. **Replay is forensic, not magical.** Missing files or rewritten git history are expected; the package records what was available at export time, not a time-machine snapshot.
3. **Redaction is preserved.** Replay packages respect privacy policy versions. A redacted session produces a redacted replay package unless the user explicitly requests raw mode with proper authorization.
4. **Normalizers are testable via replay.** Replay packages serve as golden inputs for normalizer regression tests.

## Schema Design

### New Tables

No new canonical tables. Replay uses existing `events`, `tool_io`, `event_links`, and `raw_events`. Replay packages are external artifacts (JSONL/Parquet archives).

### Replay Package Format

A replay package is a `.moraine-replay.tar.gz` containing:

```
replay-<{session_id}>-<{timestamp}>/
  manifest.json          # metadata, schema version, privacy policy version, source inventory
  events.jsonl           # full v_conversation_trace for the session
  tool_io.jsonl          # tool calls/results linked to events
  raw_fragments/         # selected raw_events rows (configurable, capped size)
  files/                 # referenced files copied at export time (if available and permitted)
  summaries.jsonl        # linked summaries (P01) and notes (P02)
  environment.json       # git state, cwd, env hints (no secrets)
```

**`manifest.json` schema:**
```json
{
  "manifest_version": "1.0",
  "moraine_version": "0.5.0",
  "schema_version": 12,
  "privacy_policy_version": "v2",
  "session_id": "sess_abc123",
  "exported_at": "2026-04-20T17:09:00Z",
  "exported_by": "alice",
  "redaction_mode": "hash",
  "row_counts": {
    "events": 1200,
    "tool_io": 45,
    "raw_fragments": 50
  },
  "file_manifest": [
    {"path": "files/crates/foo/src/lib.rs", "source_ref": "...", "sha256": "..."}
  ],
  "missing_files": ["files/old_config.toml"],
  "checksums": {"events.jsonl": "sha256:...", ...}
}
```

## API Sketches

### MCP Tools

#### `export_replay` (new tool)

**Input schema:**
```json
{
  "session_id": "string",
  "include_raw": "boolean?",
  "include_files": "boolean?",
  "include_summaries": "boolean?",
  "max_file_bytes": "number?",
  "redaction_mode": "string?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "replay_path": "string",
  "manifest": { ... },
  "warnings": ["string"],
  "_safety": { ... }
}
```

**Behavior:**
- Writes package to `~/.moraine/replays/`.
- `include_raw` requires explicit opt-in; `strict` mode suppresses it.
- `include_files` copies referenced files only if they exist and policy permits (P09).
- Returns warnings for missing files, large payloads truncated, or policy-denied exports.

#### `inspect_replay` (new tool)

Read a replay package without extracting it.

**Input schema:**
```json
{"replay_path": "string", "verbosity": "prose | full", "safety_mode": "normal | strict"}
```

### CLI Commands

```bash
moraine replay export <session_id> [--include-raw] [--include-files] [--out ./replay.tar.gz]
moraine replay inspect <replay.tar.gz>           # show manifest and warnings
moraine replay diff <replay1.tar.gz> <replay2.tar.gz>  # compare sessions
moraine replay validate <replay.tar.gz>          # verify checksums and schema version
moraine replay import <replay.tar.gz> [--dry-run] # import into local DB (for shared repros)
```

### Monitor Endpoints

- `POST /api/replays/export` — queue export, return job ID.
- `GET /api/replays/:job_id` — export status and download link.
- `POST /api/replays/inspect` — upload and inspect (optional; may be CLI-only for size reasons).

## Data Flow

1. **Request:** User requests replay export for a session.
2. **Fetch:** Backend reads `v_conversation_trace`, `tool_io`, linked summaries/notes.
3. **Collect:** Optionally copies referenced files from source coordinates.
4. **Redact:** Applies current or requested privacy policy to package contents.
5. **Package:** Writes JSONL/Parquet files, manifest, and checksums into `.tar.gz`.
6. **Validate:** `moraine replay validate` verifies checksums and schema compatibility.
7. **Import:** `moraine replay import` reads package into a staging namespace or new database.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Tool calls are destructive | Replay package is read-only; no auto-execution. `environment.json` records commands for manual inspection only. |
| Missing files or rewritten git history | Listed in `missing_files`; package still valid. |
| Secrets in commands or outputs | Privacy redaction applied at export time. Raw mode requires `--include-raw` and appropriate auth. |
| Very large sessions (100k+ events) | `max_file_bytes` cap; streaming JSONL write; truncate with warning. |
| Export while session is being ingested | Export uses snapshot read (`events` as of query time); newer events appear in a subsequent export. |
| Cross-version import | `manifest.schema_version` checked against local DB; incompatible versions rejected with guidance. |
| Duplicate import | Import generates new `session_id` suffix or uses staging database to avoid collisions. |

## Acceptance Contract

### Functional
- [ ] `export_replay` produces a valid `.tar.gz` with checksum-verified contents.
- [ ] `inspect_replay` returns manifest and warnings without extracting to disk.
- [ ] `replay diff` identifies added/removed events and tool calls between two packages.
- [ ] `replay import` creates queryable sessions in a staging namespace.

### Operational
- [ ] Export of a 10k-event session completes in under 30 seconds.
- [ ] Package size is bounded: default `max_file_bytes = 50MB` per package.
- [ ] Replay packages included in backup/restore (R01) as opaque files in `~/.moraine/replays/`.

### Safety
- [ ] Default export applies current privacy redaction; secrets do not leak into packages.
- [ ] `include_raw` is suppressed in `strict` mode and requires explicit CLI flag.
- [ ] `environment.json` does not include env vars, API keys, or `~/.env` contents.

### Compatibility
- [ ] Manifest version is forward-compatible for one major version (N can read N-1).
- [ ] Schema version mismatch produces clear error, not silent data corruption.

### Observability
- [ ] Monitor shows export queue, success/failure counts, and average package size.
- [ ] `moraine doctor` validates checksums of recent replay packages on request.

## PR Sequencing

1. `feat(replay): define replay package manifest and archive format`  
   - Structs, serialization, validation logic. New crate `moraine-replay-core`.
2. `feat(cli): add replay export and inspect commands`  
   - CLI surface, file collection, redaction.
3. `feat(mcp): add export_replay and inspect_replay tools`  
   - MCP surface with safety envelope.
4. `feat(replay): add replay diff and normalizer regression harness`  
   - Diff logic; test harness that replays fixtures through normalizer and compares.
5. `feat(monitor): add replay export button on session explorer`  
   - UI integration.
6. `test(replay): add round-trip fixture tests`  
   - Export → validate → import → compare.

## Open Questions

1. **Should replay support multiple sessions in one package?** Yes, but v1 targets single session. Multi-session export is a future extension.
2. **Parquet vs JSONL:** JSONL for human readability and git diffs; Parquet as optional compact format. Default JSONL.
3. **Should replay include full git repo state?** No — too large. Record `git show --stat` and branch/commit hints only.
