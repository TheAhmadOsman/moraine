# P01 — Session Summaries and Memory Cards

**Priority:** P2  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** C05 (session explorer), C07 (relevance loop), C10 (MCP resources/prompts), R02 (reindex orchestration)

## Objective

Build a curated memory layer over raw traces. Raw events are complete but noisy. Summaries distill intent, outcomes, key decisions, and errors into durable, citable memory cards that agents and humans can retrieve without reading full transcripts.

## Design Principles

1. **Summaries are projections, not replacements.** Every claim in a summary links back to source event UIDs. [src: ADR-004]
2. **Summaries are versioned and reviewable.** A summary has a model, prompt version, confidence score, and review status. Users can reject, edit, or promote summaries into stable memory cards.
3. **Generation is asynchronous and idempotent.** Re-summarizing the same session with the same prompt version must produce the same logical summary (deterministic identity). [src: ADR-005]
4. **Retrieval safety applies to summaries too.** Summaries are memory content, not instructions. They carry the same `_safety` envelope and can be suppressed by policy. [src: ADR-010]

## Schema Design

### New Tables

```sql
-- Summary records, one per (session, prompt_version) pair.
-- Logical identity: (session_id, prompt_version).
CREATE TABLE IF NOT EXISTS moraine.summaries (
  summary_uid String,
  session_id String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  model LowCardinality(String),
  prompt_version String,
  prompt_hash UInt64,
  summary_kind LowCardinality(String), -- 'auto', 'user_prompted', 'memory_card'
  status LowCardinality(String),       -- 'pending', 'complete', 'reviewed', 'rejected', 'stale'
  confidence Float64,
  title String,
  abstract String,
  key_decisions Array(String),
  key_errors Array(String),
  outcome LowCardinality(String),      -- 'success', 'partial', 'failure', 'unknown'
  tags Array(String),
  provenance_json String,              -- [{event_uid, claim_type, quote}, ...]
  generated_at DateTime64(3),
  reviewed_at DateTime64(3),
  reviewed_by String,
  stale_reason String,
  event_version UInt64                 -- monotonic version for ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(generated_at)
ORDER BY (session_id, prompt_version, summary_uid);

-- Memory cards: user-promoted or high-confidence summaries that enter the curated corpus.
CREATE TABLE IF NOT EXISTS moraine.memory_cards (
  card_uid String,
  summary_uid String,
  session_id String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  title String,
  abstract String,
  content_markdown String,
  tags Array(String),
  project LowCardinality(String),
  created_at DateTime64(3),
  updated_at DateTime64(3),
  created_by String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (card_uid);

-- Link summaries to source events for traceability.
CREATE TABLE IF NOT EXISTS moraine.summary_provenance (
  summary_uid String,
  event_uid String,
  session_id String,
  claim_type LowCardinality(String), -- 'decision', 'error', 'fact', 'quote'
  excerpt String,
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM()
ORDER BY (summary_uid, event_uid, claim_type);
```

### Materialized Views

```sql
-- Search documents for summaries so they are discoverable via BM25.
CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_documents_from_summaries
TO moraine.search_documents
AS
SELECT
  event_version AS doc_version,
  generated_at AS ingested_at,
  summary_uid AS event_uid,
  '' AS compacted_parent_uid,
  session_id,
  toDate(generated_at) AS session_date,
  source_name,
  harness,
  '' AS inference_provider,
  '' AS source_file,
  0 AS source_generation,
  0 AS source_line_no,
  0 AS source_offset,
  '' AS source_ref,
  '' AS record_ts,
  'summary' AS event_class,
  'summary' AS payload_type,
  'system' AS actor_role,
  '' AS name,
  '' AS phase,
  concat(title, ' ', abstract, ' ', arrayStringConcat(key_decisions, ' '), ' ', arrayStringConcat(key_errors, ' ')) AS text_content,
  provenance_json AS payload_json,
  '' AS token_usage_json
FROM moraine.summaries
WHERE status IN ('complete', 'reviewed') AND lengthUTF8(abstract) > 0;
```

## API Sketches

### MCP Tools

#### `summarize_session` (new tool)

Generate or refresh a summary for a session.

**Input schema:**
```json
{
  "session_id": "string",
  "prompt_version": "string?",
  "force": "boolean?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "summary_uid": "string",
  "session_id": "string",
  "status": "pending | complete",
  "title": "string",
  "abstract": "string",
  "confidence": "number",
  "provenance": [{"event_uid": "string", "claim_type": "string"}],
  "_safety": { ... }
}
```

**Behavior:**
- If a complete summary exists for `(session_id, prompt_version)` and `force` is not set, return the cached summary.
- If the session has changed since the summary was generated, mark existing summary `stale` and queue/regenerate.
- `strict` mode suppresses `provenance` payload JSON and returns only high-level fields.

#### `list_memory_cards` (new tool)

List curated memory cards with filters.

**Input schema:**
```json
{
  "project": "string?",
  "tags": ["string"]?,
  "outcome": "string?",
  "query": "string?",
  "limit": "number?",
  "cursor": "string?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Behavior:** Supports BM25 search over `memory_cards.title` + `abstract` + `content_markdown` via the search pipeline (either a dedicated search index or reuse `search_documents` with `event_class = 'summary'`).

### CLI Commands

```bash
# Trigger summary generation for one or all sessions
moraine summary generate <session_id> [--prompt-version v2] [--force]
moraine summary generate --all-stale

# Review and curate
moraine summary review <summary_uid> [--approve|--reject|--edit]
moraine summary promote <summary_uid>  # creates a memory card

# List and search
moraine memory list [--project <name>] [--tag <tag>] [--limit 25]
moraine memory search "auth refactor" [--project backend]

# Export memory cards (respects policy engine when P09 lands)
moraine memory export [--format jsonl|markdown] [--out ./cards.md]
```

### Monitor Endpoints

- `GET /api/summaries?session_id=...&status=...` — paginated summary list.
- `GET /api/summaries/:summary_uid` — full summary with provenance.
- `POST /api/summaries/:summary_uid/review` — approve/reject/edit.
- `GET /api/memory-cards` — curated card list.
- `POST /api/memory-cards/:card_uid` — update card metadata.

## Data Flow

1. **Trigger:** User calls `summarize_session`, CLI `generate`, or monitor UI requests summary.
2. **Fetch:** Backend reads `v_conversation_trace` for the session.
3. **Generate:** An LLM call (local or remote, configurable) produces structured summary JSON matching the schema.
4. **Validate:** Server validates JSON schema, checks provenance links exist in `events`.
5. **Store:** Writes `summaries` row + `summary_provenance` rows.
6. **Index:** Materialized view fans out into `search_documents` so summaries are searchable.
7. **Curate:** User reviews and promotes to `memory_cards`.
8. **Retrieve:** MCP `list_memory_cards` or `search` returns summaries/cards with `_safety` envelope.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Summaries hallucinate | Store confidence score; require review for auto-promotion; allow user edit/reject. |
| Old prompt-injection content poisons summary | Generation prompt includes safety framing; strict mode retrieval strips raw payloads; user review gate. |
| Session updated after summary | `event_version` or `max(event_ts)` comparison; stale detection job marks outdated summaries. |
| Generation model unavailable | Async queue with retry; status stays `pending`; CLI shows queue depth. |
| Very long sessions exceed context window | Chunk by turn ranges, generate per-chunk summaries, then synthesize. |
| Duplicate generation requests | Unique constraint on `(session_id, prompt_version)`; `ReplacingMergeTree` converges. |
| Privacy-redacted sessions | Summaries inherit the privacy policy version of the session; re-summarize after reindex if needed. |
| Search index mixed with raw events | `event_class = 'summary'` filter lets callers choose summary-only, event-only, or both. |

## Acceptance Contract

### Functional
- [ ] `summarize_session` returns a cached summary on second call for the same `(session_id, prompt_version)`.
- [ ] Every `key_decisions` and `key_errors` entry in a summary has at least one `summary_provenance` row linking to an existing `events.event_uid`.
- [ ] Promoting a summary to a memory card preserves provenance and creates a stable `card_uid`.
- [ ] `list_memory_cards` supports pagination, tag filtering, and text search.

### Operational
- [ ] Generating a summary for a 10k-turn session completes in under 30 seconds (local model) or under 120 seconds (remote API) with chunked strategy.
- [ ] Summary tables do not block ingest or search queries (independent partitions, no locks).
- [ ] Reindexing (`moraine reindex`) can rebuild summary search documents without regenerating LLM summaries.

### Safety
- [ ] Summary retrieval includes the full `_safety` envelope with `content_classification = "memory_content"`.
- [ ] `strict` mode on `summarize_session` suppresses raw `provenance_json` and returns only `event_uid` references.
- [ ] Summaries of redacted sessions do not expose redacted content (generation runs on redacted text).

### Compatibility
- [ ] New tables use the same `ReplacingMergeTree` + `event_version` pattern as existing canonical tables.
- [ ] MCP tool schemas declare `additionalProperties: false`.
- [ ] Existing `search` tool behavior is unchanged when no summary-class documents are present.

### Observability
- [ ] `moraine doctor` checks for orphan `summary_provenance` rows (event_uid missing from `events`).
- [ ] Monitor shows summary generation queue depth, stale count, and average confidence.
- [ ] Prometheus-style metric `moraine_summary_generation_duration_seconds` histogram.

## PR Sequencing

1. `schema(summaries): add summaries, memory_cards, summary_provenance tables`  
   - SQL only, no application code. Includes materialized view for search integration.
2. `feat(summaries): add summarize_session MCP tool and backend generation queue`  
   - New crate `moraine-summaries-core` or module in `moraine-conversations`. Async generation with configurable provider.
3. `feat(cli): add summary generate, review, and promote commands`  
   - CLI surface, no monitor UI yet.
4. `feat(monitor): add summary review panel and memory card browser`  
   - Svelte UI, read-only first, then review actions.
5. `feat(mcp): add list_memory_cards and search summary-class documents`  
   - Retrieval integration, `_safety` envelope, strict mode.
6. `test(summaries): add evaluation fixtures for summary quality`  
   - Golden sessions, expected summaries, regression tests for prompt versions.

## Open Questions

1. **Local vs remote generation:** Should Moraine bundle a local summarization model (e.g., via llama.cpp/ort), or should it require an OpenAI/Anthropic API key? Recommendation: start with remote (configurable endpoint), add local as P2 follow-up.
2. **Chunking strategy:** Should chunk boundaries align with turns, file edits, or time windows? Recommendation: turn-based chunks with overlap.
3. **Memory card format:** Markdown vs structured JSON? Recommendation: store both — `content_markdown` for humans, structured fields for MCP.
