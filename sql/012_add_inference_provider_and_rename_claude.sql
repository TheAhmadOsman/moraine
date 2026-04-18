-- Add `inference_provider` column alongside `harness`, and rename the harness
-- value `claude` to `claude-code` so the existing `codex`/`claude-code` split
-- reflects the CLI identifier while `inference_provider` captures the backend
-- that served the LLM call (e.g. `openai`, `anthropic`, `bedrock/anthropic`,
-- `azure/openai`).
--
-- The column grammar is intentionally permissive: a single
-- `LowCardinality(String)` that today holds `openai` or `anthropic`, and in
-- future Hermes work may hold cloud-prefixed values like `bedrock/anthropic`
-- or `azure/openai`. No parsing of vendor/model strings happens in this
-- migration; the normalizer populates it based on harness (Codex -> openai,
-- Claude Code -> anthropic).
--
-- This migration is idempotent: it uses `ADD COLUMN IF NOT EXISTS`, drops and
-- recreates materialized views, and guards backfill `UPDATE` statements with
-- `WHERE` clauses.

-- ---------------------------------------------------------------------------
-- 1) Add `inference_provider` column to every table that carries `harness`.
-- ---------------------------------------------------------------------------

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.event_links
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.ingest_errors
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.search_documents
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.search_postings
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

ALTER TABLE moraine.search_hit_log
  ADD COLUMN IF NOT EXISTS inference_provider LowCardinality(String) DEFAULT '' AFTER harness;

-- ---------------------------------------------------------------------------
-- 2) Backfill `inference_provider` from existing `harness` values. These are
-- async mutations; they are safe to retry because the WHERE clause skips rows
-- already populated.
-- ---------------------------------------------------------------------------

ALTER TABLE moraine.raw_events
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.events
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.event_links
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.tool_io
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.ingest_errors
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.search_documents
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.search_postings
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

ALTER TABLE moraine.search_hit_log
  UPDATE inference_provider = if(harness = 'codex', 'openai', if(harness = 'claude' OR harness = 'claude-code', 'anthropic', ''))
  WHERE inference_provider = '';

-- ---------------------------------------------------------------------------
-- 3) Hard-rename the legacy harness value `claude` to `claude-code` in every
-- table that carries `harness`. No back-compat alias.
-- ---------------------------------------------------------------------------

ALTER TABLE moraine.raw_events
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.events
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.event_links
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.tool_io
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.ingest_errors
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.search_documents
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.search_postings
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

ALTER TABLE moraine.search_hit_log
  UPDATE harness = 'claude-code'
  WHERE harness = 'claude';

-- ---------------------------------------------------------------------------
-- 4) Drop and recreate the search materialized views so their projection
-- includes the new `inference_provider` column. The MVs in sql/004 select by
-- column name from `moraine.events` / `moraine.search_documents`, so we need
-- to refresh the view definitions to pick up the new column.
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS moraine.mv_search_documents_from_events;
DROP VIEW IF EXISTS moraine.mv_search_postings;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_documents_from_events
TO moraine.search_documents
AS
SELECT
  event_version AS doc_version,
  ingested_at,
  event_uid,
  origin_event_id AS compacted_parent_uid,
  session_id,
  session_date,
  source_name,
  harness,
  inference_provider,
  source_file,
  source_generation,
  source_line_no,
  source_offset,
  source_ref,
  record_ts,
  event_kind AS event_class,
  payload_type,
  actor_kind AS actor_role,
  tool_name AS name,
  if(tool_phase != '', tool_phase, op_status) AS phase,
  text_content,
  payload_json,
  token_usage_json
FROM moraine.events
WHERE lengthUTF8(replaceRegexpAll(text_content, '\\s+', '')) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_postings
TO moraine.search_postings
AS
SELECT
  d.doc_version AS post_version,
  d.term,
  d.event_uid AS doc_id,
  d.session_id,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len,
  toUInt16(count()) AS tf
FROM
(
  SELECT
    doc_version,
    event_uid,
    session_id,
    source_name,
    harness,
    inference_provider,
    event_class,
    payload_type,
    actor_role,
    name,
    phase,
    source_ref,
    doc_len,
    arrayJoin(extractAll(lowerUTF8(text_content), '[a-z0-9_]+')) AS term
  FROM moraine.search_documents
  WHERE doc_len > 0
) AS d
WHERE lengthUTF8(d.term) BETWEEN 2 AND 64
GROUP BY
  d.doc_version,
  d.term,
  d.event_uid,
  d.session_id,
  d.source_name,
  d.harness,
  d.inference_provider,
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len;
