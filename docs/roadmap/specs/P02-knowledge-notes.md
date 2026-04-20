# P02 — User-Curated Corrections and Knowledge Notes

**Priority:** P2  
**Effort:** L  
**Status:** Specification / ready for design review  
**Dependencies:** P01 (summaries), C05 (session explorer), R02 (reindex)

## Objective

Let humans add durable knowledge that complements raw traces. Notes can correct bad summaries, capture lessons learned, or record facts that never appeared in agent traces. Notes are first-class retrieval citizens but are always distinguishable from agent-authored trace data.

## Design Principles

1. **User memory is explicitly authored.** Notes carry `created_by`, `authorship_kind` (`user`, `system`, `imported`), and revision history. They are never auto-generated without attribution. [src: ADR-004]
2. **Notes are linkable.** A note attaches to a session, event, source, repo, task, or arbitrary tag. This makes notes contextually retrievable.
3. **Notes participate in search but are typed.** Search can include or exclude notes independently of raw events. [src: ADR-007]
4. **Notes are subject to the same policy and safety rules as events.** P09 (policy engine) applies to notes for retrieval and export. [src: ADR-010, ADR-011]

## Schema Design

### New Tables

```sql
-- Knowledge notes: user-authored durable memory.
CREATE TABLE IF NOT EXISTS moraine.notes (
  note_uid String,
  note_kind LowCardinality(String), -- 'correction', 'lesson', 'fact', 'tag', 'task'
  authorship_kind LowCardinality(String), -- 'user', 'system', 'imported'
  created_by String,
  title String,
  body String,
  tags Array(String),
  project LowCardinality(String),
  repo String,
  branch String,
  created_at DateTime64(3),
  updated_at DateTime64(3),
  revision_count UInt32,
  status LowCardinality(String), -- 'active', 'archived', 'superseded'
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (note_uid);

-- Note attachments: links from notes to sessions, events, sources, etc.
CREATE TABLE IF NOT EXISTS moraine.note_links (
  note_uid String,
  target_kind LowCardinality(String), -- 'session', 'event', 'source', 'summary', 'memory_card', 'tag'
  target_id String,
  link_role LowCardinality(String),   -- 'corrects', 'explains', 'relates', 'blocks'
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (note_uid, target_kind, target_id);

-- Note revision history (append-only audit)
CREATE TABLE IF NOT EXISTS moraine.note_revisions (
  revision_uid String,
  note_uid String,
  revised_at DateTime64(3),
  revised_by String,
  previous_body String,
  change_summary String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(revised_at)
ORDER BY (note_uid, revised_at);
```

### Materialized Views

```sql
-- Search documents for notes
CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_documents_from_notes
TO moraine.search_documents
AS
SELECT
  event_version AS doc_version,
  created_at AS ingested_at,
  note_uid AS event_uid,
  '' AS compacted_parent_uid,
  '' AS session_id,
  toDate(created_at) AS session_date,
  '' AS source_name,
  '' AS harness,
  '' AS inference_provider,
  '' AS source_file,
  0 AS source_generation,
  0 AS source_line_no,
  0 AS source_offset,
  '' AS source_ref,
  '' AS record_ts,
  'note' AS event_class,
  note_kind AS payload_type,
  authorship_kind AS actor_role,
  '' AS name,
  '' AS phase,
  concat(title, ' ', body, ' ', arrayStringConcat(tags, ' ')) AS text_content,
  '' AS payload_json,
  '' AS token_usage_json
FROM moraine.notes
WHERE status = 'active' AND lengthUTF8(body) > 0;
```

## API Sketches

### MCP Tools

#### `create_note` (new tool)

**Input schema:**
```json
{
  "title": "string",
  "body": "string",
  "note_kind": "correction | lesson | fact | tag | task",
  "tags": ["string"]?,
  "project": "string?",
  "links": [{"target_kind": "string", "target_id": "string", "link_role": "string"}]?,
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "note_uid": "string",
  "created_at": "number",
  "title": "string",
  "_safety": { ... }
}
```

#### `search_notes` (new tool, or extend `search`)

Option A: extend `search` with `include_notes = true` and `event_kind = "note"` filter.  
Option B: separate `search_notes` tool with note-specific filters (project, tag, authorship).

**Recommendation:** Option A — notes become a searchable `event_class` in the existing index. A separate `list_notes` tool provides structured filtering.

#### `list_notes` (new tool)

**Input schema:**
```json
{
  "project": "string?",
  "tags": ["string"]?,
  "note_kind": "string?",
  "target_kind": "string?",
  "target_id": "string?",
  "limit": "number?",
  "cursor": "string?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

### CLI Commands

```bash
moraine note create --title "Fix auth pattern" --body "..." --tag backend --link session:abc123
moraine note list --project backend --tag auth
moraine note edit <note_uid> --body "..."
moraine note archive <note_uid>
moraine note show <note_uid>          # includes revision history
moraine note search "auth refactor"   # BM25 over notes
```

### Monitor Endpoints

- `POST /api/notes` — create note.
- `GET /api/notes` — list with filters.
- `GET /api/notes/:note_uid` — detail with links.
- `PUT /api/notes/:note_uid` — edit (creates revision row).
- `DELETE /api/notes/:note_uid` — soft-delete (status = 'archived').
- `GET /api/notes/:note_uid/revisions` — audit trail.

## Data Flow

1. **Create:** User submits note via MCP, CLI, or monitor. Server validates links (target IDs exist in respective tables).
2. **Store:** Writes `notes` + `note_links` rows. If editing, writes `note_revisions` row first.
3. **Index:** Materialized view fans note text into `search_documents` with `event_class = 'note'`.
4. **Retrieve:** `search` tool can include notes; `list_notes` provides structured navigation.
5. **Curate:** Notes can correct summaries (P01) by linking with `link_role = 'corrects'`.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Notes contain secrets | Ingest-time privacy redaction applies to note `body` if configured. Notes inherit the active privacy policy version. |
| Notes become stale | `status` field allows `archived`/`superseded`; user can explicitly mark outdated notes. |
| Same fact conflicts across notes | No automatic resolution; search returns both with `authorship_kind` and `updated_at` for user judgment. |
| Exports must include/exclude notes | Export commands accept `--include-notes` / `--exclude-notes` flags; P09 policy engine adds destination-based rules. |
| Link target deleted (session removed) | `note_links` foreign keys are logical, not enforced by ClickHouse. Monitor/CLI show broken links with warning. |
| Very large note body | Configurable max length (default 64KB); truncate with warning if exceeded. |

## Acceptance Contract

### Functional
- [ ] A note created via MCP is searchable within 5 seconds (materialized view freshness).
- [ ] `list_notes` filtered by `target_id` returns all notes linked to that session/event/source.
- [ ] Editing a note creates a `note_revisions` row preserving the previous body.
- [ ] Notes are distinguishable from raw events in `search` results (`event_class = 'note'`, `authorship_kind = 'user'`).

### Operational
- [ ] 10k notes do not materially impact search latency (measured via C07 evaluation loop).
- [ ] Note tables are included in backup/restore (R01) with manifest row counts.

### Safety
- [ ] Notes carry `_safety` envelope on retrieval.
- [ ] `strict` mode does not suppress note retrieval (notes are user-authored and intentional), but it does suppress linked raw payloads.
- [ ] Note `body` is redacted at creation time if privacy policy requires it.

### Compatibility
- [ ] New `event_class = 'note'` does not break existing event-kind domain constraints (separate table, separate search class).
- [ ] MCP `search` tool backward compatible: default `include_notes` behavior is `true` (or `false`? recommend `true` since notes are high-signal).

### Observability
- [ ] `moraine doctor` checks for orphan `note_links` (target missing).
- [ ] Monitor shows note count, active/archived split, and recent corrections.

## PR Sequencing

1. `schema(notes): add notes, note_links, note_revisions tables`  
   - SQL + MV for search integration.
2. `feat(notes): add note CRUD backend and MCP tools`  
   - New module in `moraine-conversations` or standalone crate `moraine-notes-core`.
3. `feat(cli): add note create, edit, list, search commands`  
   - CLI surface.
4. `feat(monitor): add note editor and linked note panels`  
   - UI for creating/editing notes on sessions and events.
5. `feat(search): integrate notes into search_documents and evaluate impact`  
   - Ensure note presence does not regress existing search quality (C07 fixtures).

## Open Questions

1. **Should notes support Markdown rendering in monitor?** Yes, but sanitize HTML to prevent XSS.
2. **Should notes be version-controlled like git?** No — simple revision log is enough. Deep branching is out of scope.
3. **Can agents create notes?** Only via explicit user instruction through MCP. `authorship_kind = 'system'` is reserved for automated annotations (e.g., anomaly detection from S05).
