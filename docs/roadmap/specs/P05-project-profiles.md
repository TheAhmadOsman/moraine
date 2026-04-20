# P05 — Project-Aware Retrieval Profiles

**Priority:** P2  
**Effort:** M  
**Status:** Specification / ready for design review  
**Dependencies:** C08 (field-weighted search), C06 (query workbench), P03 (entity graph), P02 (notes)

## Objective

Make retrieval aware of the current repo/project without hiding global search. When a developer is working in `moraine/`, retrieval should prioritize sessions, files, and notes from that project while still allowing broad cross-project discovery.

## Design Principles

1. **Project detection is heuristic, not authoritative.** Moraine guesses the current project from cwd, git remote, and source paths. Users can override with aliases. [src: ADR-007]
2. **Profiles are user-configurable but have smart defaults.** A profile includes project scope, source filters, ranking weights, and time window preferences.
3. **Global search remains one toggle away.** Project-aware defaults must not trap users in a local optimum where cross-project memory is invisible.
4. **Profiles are portable.** They can be exported/imported and shared across machines (links to S03).

## Schema Design

### New Tables

```sql
-- Project profiles: user-defined or auto-detected project scopes.
CREATE TABLE IF NOT EXISTS moraine.project_profiles (
  profile_id String,
  profile_name String,
  project_key String,                -- canonical project identifier
  detection_rules_json String,       -- [{kind: 'git_remote', pattern: '...'}, {kind: 'cwd', pattern: '...'}]
  source_names Array(String),        -- preferred sources for this project
  source_paths Array(String),        -- path prefixes that belong to this project
  repo_remotes Array(String),        -- git remote URL patterns
  ranking_boosts_json String,        -- {field: weight, ...}
  default_time_window_days UInt32,
  created_at DateTime64(3),
  updated_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY tuple()
ORDER BY (profile_id);

-- Session-to-project mapping (derived, can be rebuilt)
CREATE TABLE IF NOT EXISTS moraine.session_projects (
  session_id String,
  project_key String,
  confidence Float64,
  detection_method LowCardinality(String), -- 'git_remote', 'cwd', 'source_path', 'user_override', 'entity_link'
  detected_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY tuple()
ORDER BY (session_id, project_key);
```

## API Sketches

### MCP Tools

#### `get_current_profile` (new tool)

**Input schema:**
```json
{"verbosity": "prose | full"}
```

**Output schema (full):**
```json
{
  "profile_id": "string",
  "project_key": "string",
  "profile_name": "string",
  "detection_rules": [{"kind": "string", "pattern": "string"}],
  "source_names": ["string"],
  "ranking_boosts": {"field": "number"},
  "_safety": { ... }
}
```

**Behavior:** Detects current project from caller context (if available) or falls back to default profile.

#### `search` extension

Add optional `project` and `profile_id` arguments to existing `search` and `search_conversations` tools.

**Input schema additions:**
```json
{
  "project": "string?",
  "profile_id": "string?",
  "scope": "project | global | auto?"
}
```

**Behavior:**
- `scope = "project"` filters to sessions mapped to the detected or specified project.
- `scope = "global"` ignores project filters.
- `scope = "auto"` (default) boosts project-matching results but includes global results.

### CLI Commands

```bash
moraine profile detect                     # show detected profile for cwd
moraine profile create --name "Backend" --repo-remote "github.com/acme/backend" --source backend
moraine profile list
moraine profile edit <profile_id> --boost file_path=2.0 --boost text_content=1.0
moraine profile delete <profile_id>
moraine profile default <profile_id>       # set global default
moraine search "auth" --profile backend --scope project
```

### Monitor Endpoints

- `GET /api/profiles` — list profiles.
- `POST /api/profiles` — create profile.
- `PUT /api/profiles/:profile_id` — update.
- `DELETE /api/profiles/:profile_id` — delete.
- `GET /api/profiles/detect` — detect for current browser context (optional).

## Data Flow

1. **Detection:** On `moraine up` or explicit `detect`, scan cwd git remote and compare against `project_profiles.repo_remotes`.
2. **Mapping:** Derive `session_projects` from source paths, entity links (P03), and user overrides.
3. **Search:** `search` tool reads current profile; applies `project_key` filter or boost.
4. **Boosting:** Ranking SQL includes `if(session_projects.project_key = target, 1.5, 1.0)` multiplier.
5. **Feedback:** C07 relevance loop tracks whether project-scoped queries improve MRR.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Monorepos and nested repos | Support multiple `project_key` per session with confidence scores. User can define sub-project profiles. |
| Same repo on multiple machines with different paths | `repo_remotes` matching is machine-independent. `source_paths` is secondary heuristic. |
| Imported remote sessions | `session_projects` maps imported sessions based on source path or explicit user tagging. |
| Cross-project work should be discoverable | Default `scope = "auto"` includes global results with boost. `scope = "global"` overrides. |
| Profile deleted but sessions mapped | Orphan mappings remain but are ignored; `moraine doctor` warns. |

## Acceptance Contract

### Functional
- [ ] `search` with `scope = "project"` returns only sessions linked to the current/specified project.
- [ ] `search` with `scope = "auto"` returns global results with project matches ranked higher.
- [ ] Profile detection from cwd matches expected project for 90%+ of sessions in fixture set.
- [ ] Users can override auto-detection with explicit `project` or `profile_id`.

### Operational
- [ ] Profile lookup does not add >50ms to search latency.
- [ ] `session_projects` rebuild is supported via `moraine reindex --projects`.

### Safety
- [ ] Profile data does not expose repo remotes or paths in `_safety` envelope (irrelevant to retrieval safety).
- [ ] Project-scoped search still applies privacy redaction and policy engine (P09) filters.

### Compatibility
- [ ] Existing `search` behavior unchanged when `project`/`scope` omitted.
- [ ] New arguments are optional; strict schema rejects unknown fields.

### Observability
- [ ] Monitor shows active profile, detection confidence, and per-profile session counts.
- [ ] C07 relevance loop tracks `project_scope` usage and result quality.

## PR Sequencing

1. `schema(profiles): add project_profiles and session_projects tables`  
   - SQL only.
2. `feat(profiles): add project detection and session mapping logic`  
   - New module in `moraine-conversations` or standalone crate.
3. `feat(search): extend search and search_conversations with project scope and boosting`  
   - SQL ranking changes; evaluate against C07 fixtures.
4. `feat(cli): add profile CRUD and project-scoped search commands`  
   - CLI surface.
5. `feat(mcp): add get_current_profile and project search arguments`  
   - MCP schema extensions.
6. `feat(monitor): add profile selector in search/workbench UI`  
   - Monitor integration.

## Open Questions

1. **Auto-detection from MCP context:** Can the MCP host communicate the current working directory? If not, rely on CLI/monitor context or last-seen profile.
2. **Should profiles include time-window defaults?** Yes — e.g., "backend" profile defaults to last 30 days, "infra" to last 90 days.
3. **Should project profiles be shareable?** Yes, export/import as JSON. Links to S03 cross-device sync.
