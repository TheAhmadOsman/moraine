# P03 — Entity and Graph Layer

**Priority:** P2  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** C01 (adapter registry), C05 (session explorer), C08 (phrase/proximity search), P01 (summaries), P02 (notes)

## Objective

Use `event_links` as the foundation for a broader graph of work. Extract entities (repos, files, tools, errors, tasks) from raw traces and link them into a queryable graph. This enables questions like "where did I fix this before?" and "what sessions touched this file?"

## Design Principles

1. **Graph is derived, not primary.** The canonical source of truth remains `events` and `event_links`. Entity and edge tables are projections that can be rebuilt. [src: ADR-004]
2. **Entity extraction is best-effort and typed.** Extracted entities carry `confidence` and `extractor_version`. Wrong extractions are tolerable because they are projections.
3. **Graph queries are local and bounded.** No global graph traversal with unbounded depth. Depth limit (e.g., 3 hops) and result limits are enforced. [src: ADR-007]
4. **Entity resolution is fuzzy.** File paths may differ across machines; repos may have different remotes. Entity resolution uses normalization (path canonicalization, git remote normalization) rather than strict identity.

## Schema Design

### New Tables

```sql
-- Entity catalog: canonical entities extracted from traces.
CREATE TABLE IF NOT EXISTS moraine.entities (
  entity_uid String,
  entity_kind LowCardinality(String), -- 'repo', 'branch', 'commit', 'file', 'command', 'tool', 'issue', 'pr', 'host', 'model', 'source', 'error_signature', 'task', 'project'
  canonical_name String,
  display_name String,
  aliases Array(String),
  metadata_json String,               -- kind-specific metadata
  first_seen_at DateTime64(3),
  last_seen_at DateTime64(3),
  occurrence_count UInt64,
  extractor_version String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(entity_kind) % 16
ORDER BY (entity_kind, canonical_name, entity_uid);

-- Entity occurrences: where an entity was observed in traces.
CREATE TABLE IF NOT EXISTS moraine.entity_occurrences (
  occurrence_uid String,
  entity_uid String,
  event_uid String,
  session_id String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  occurrence_role LowCardinality(String), -- 'mentioned', 'edited', 'executed', 'produced', 'failed'
  excerpt String,
  occurrence_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(occurrence_at)
ORDER BY (entity_uid, occurrence_at, event_uid);

-- Edges between entities (derived, not manual)
CREATE TABLE IF NOT EXISTS moraine.entity_edges (
  edge_uid String,
  from_entity_uid String,
  to_entity_uid String,
  edge_kind LowCardinality(String),     -- 'mentioned', 'edited', 'ran', 'failed', 'retried', 'superseded', 'related_session', 'generated_artifact'
  session_id String,
  evidence_event_uid String,
  weight Float64,
  first_seen_at DateTime64(3),
  last_seen_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY cityHash64(edge_kind) % 16
ORDER BY (from_entity_uid, to_entity_uid, edge_kind);

-- Entity resolution log: maps raw extracted strings to canonical entities.
CREATE TABLE IF NOT EXISTS moraine.entity_resolution (
  raw_string String,
  entity_kind LowCardinality(String),
  resolved_entity_uid String,
  resolver_version String,
  confidence Float64,
  resolved_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY cityHash64(entity_kind) % 16
ORDER BY (raw_string, entity_kind);
```

### Views

```sql
-- Human-friendly entity lookup with latest occurrence.
CREATE VIEW IF NOT EXISTS moraine.v_entity_summary
AS
SELECT
  e.entity_uid,
  e.entity_kind,
  e.canonical_name,
  e.display_name,
  e.occurrence_count,
  e.last_seen_at,
  arraySlice(groupArrayDistinct(o.session_id), 1, 5) AS recent_sessions
FROM moraine.entities e
LEFT JOIN moraine.entity_occurrences o ON o.entity_uid = e.entity_uid
GROUP BY e.entity_uid, e.entity_kind, e.canonical_name, e.display_name, e.occurrence_count, e.last_seen_at;

-- Adjacency list for bounded graph traversal.
CREATE VIEW IF NOT EXISTS moraine.v_entity_adjacency
AS
SELECT
  from_entity_uid,
  to_entity_uid,
  edge_kind,
  weight,
  session_id,
  evidence_event_uid
FROM moraine.entity_edges
WHERE weight > 0.1;
```

## API Sketches

### MCP Tools

#### `extract_entities` (new tool, async)

Trigger entity extraction for a session or source.

**Input schema:**
```json
{
  "session_id": "string?",
  "source_name": "string?",
  "force": "boolean?",
  "verbosity": "prose | full"
}
```

**Behavior:** Queues extraction job; returns job ID if async. If session is small, may return inline.

#### `search_entities` (new tool)

**Input schema:**
```json
{
  "query": "string",
  "entity_kind": "string?",
  "project": "string?",
  "limit": "number?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "entities": [{
    "entity_uid": "string",
    "entity_kind": "string",
    "canonical_name": "string",
    "occurrence_count": "number",
    "recent_sessions": ["string"]
  }],
  "_safety": { ... }
}
```

#### `entity_graph` (new tool)

Bounded graph traversal from a starting entity.

**Input schema:**
```json
{
  "entity_uid": "string",
  "edge_kinds": ["string"]?,
  "max_depth": "number?",
  "limit": "number?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Constraints:** `max_depth` clamped to `[1, 3]`. `limit` clamped to `[1, 50]`.

### CLI Commands

```bash
moraine entities extract <session_id> [--force]
moraine entities search "UserRepository" [--kind file]
moraine entities show <entity_uid>        # occurrences and related entities
moraine entities graph <entity_uid> --depth 2 --kind "edited|failed"
moraine entities resolve "~/src/moraine"  # show canonical entity for raw string
```

### Monitor Endpoints

- `POST /api/entities/extract` — queue extraction.
- `GET /api/entities/search?q=...&kind=...` — entity search.
- `GET /api/entities/:entity_uid` — entity detail with top occurrences.
- `GET /api/entities/:entity_uid/graph?depth=...` — adjacency graph (JSON nodes + edges).
- `GET /api/entities/:entity_uid/sessions` — sessions where entity appeared.

## Data Flow

1. **Ingest:** Raw events land in `events`.
2. **Extract:** Async extractor (rule-based + optional LLM) scans `events.text_content`, `payload_json`, `tool_io` for entity patterns.
3. **Resolve:** Raw strings (e.g., `/Users/alice/src/moraine/crates/foo/src/lib.rs`) are normalized to canonical names (`repo:moraine file:crates/foo/src/lib.rs`).
4. **Store:** Write `entities`, `entity_occurrences`, `entity_edges`.
5. **Query:** MCP tools and monitor read from views. Graph traversal is bounded BFS in SQL or application code.
6. **Rebuild:** `moraine reindex --entities-only` drops and rebuilds derived tables from canonical events.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Entity extraction is wrong | `confidence` field; user can manually merge/split entities via monitor UI. Low-confidence extractions are marked tentative. |
| File paths differ across machines | Path canonicalization: resolve symlinks, strip home directory prefix, match against known repo roots. |
| Git branches/commits rewritten | Store `first_seen_at`/`last_seen_at`; historical edges remain valid as memory, not as live git state. |
| Graph growth outpaces tables | Partition by hash; bounded traversal limits; optional TTL on edges with weight < 0.1 (configurable). |
| Same session ID from multiple sources | `entity_occurrences` includes `source_name` and `harness` for disambiguation. |
| Extraction crashes mid-session | Async job checkpointing; reprocess from last successful `event_uid`. |

## Acceptance Contract

### Functional
- [ ] `search_entities` returns entities matching query text with BM25 over `canonical_name` + `aliases`.
- [ ] `entity_graph` with `max_depth = 2` returns within 2 seconds for an entity with 1000 occurrences.
- [ ] Every `entity_occurrence` links to an existing `events.event_uid`.
- [ ] Rebuilding entities from canonical events produces deterministic `entity_uid` for identical inputs.

### Operational
- [ ] Entity extraction for a 1000-event session completes in under 10 seconds (rule-based) or under 60 seconds (LLM-assisted).
- [ ] Entity tables included in backup/restore (R01).
- [ ] Reindex command supports `--entities-only`.

### Safety
- [ ] Entity search and graph traversal return `_safety` envelope.
- [ ] `strict` mode suppresses raw `excerpt` text in occurrences (returns only entity metadata).
- [ ] Entity extraction does not extract secrets: privacy redaction runs before extraction.

### Compatibility
- [ ] Entity tables do not alter existing `events` or `event_links` schema.
- [ ] MCP tools follow strict schema policy.

### Observability
- [ ] Monitor shows entity count by kind, extraction queue depth, and average confidence.
- [ ] `moraine doctor` checks for orphan `entity_occurrences` (missing `event_uid`).

## PR Sequencing

1. `schema(entities): add entities, entity_occurrences, entity_edges, entity_resolution tables`  
   - SQL + views.
2. `refactor(extract): introduce entity extraction framework and rule-based extractors`  
   - New crate `moraine-entities-core` with trait `EntityExtractor`.
3. `feat(entities): add file-path and repo extractors`  
   - Most valuable entity kinds first.
4. `feat(entities): add tool-name and error-signature extractors`  
   - Operational debugging value.
5. `feat(mcp): add search_entities and entity_graph tools`  
   - Retrieval surface.
6. `feat(cli): add entities search, show, graph commands`  
   - CLI surface.
7. `feat(monitor): add entity browser and graph visualization`  
   - Svelte + lightweight graph renderer (e.g., D3 force or simple adjacency table).
8. `test(entities): add extraction evaluation fixtures`  
   - Golden sessions with known entities; precision/recall targets.

## Open Questions

1. **LLM vs rule-based extraction:** Start with rule-based (regex + path/git parsers) for speed and determinism. Add LLM-assisted extraction as opt-in for complex cases.
2. **Graph storage model:** ClickHouse is not a graph DB. Bounded BFS in application code with batched SQL lookups is recommended over recursive CTEs (which ClickHouse supports but may be slow).
3. **Entity merge UI:** How does a user say "these two file paths are the same file"? Recommendation: monitor UI shows alias suggestions; user confirms merge.
