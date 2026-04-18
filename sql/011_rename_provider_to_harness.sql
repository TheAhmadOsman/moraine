-- Rename the legacy `provider` column to `harness` across all tables that
-- carry it. The column continues to hold the CLI/agent identifier that wrote
-- the trace (`codex` / `claude`) with no semantic change. A separate follow-up
-- migration will introduce a distinct `inference_provider` column for the
-- backend that served the LLM call.

ALTER TABLE moraine.raw_events
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.events
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.event_links
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.tool_io
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.ingest_errors
  RENAME COLUMN IF EXISTS provider TO harness;

-- Search index tables.
ALTER TABLE moraine.search_documents
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.search_postings
  RENAME COLUMN IF EXISTS provider TO harness;

ALTER TABLE moraine.search_hit_log
  RENAME COLUMN IF EXISTS provider TO harness;

-- The materialized views defined in sql/004_search_index.sql select the
-- renamed column by name. Drop and recreate them so the projection targets
-- `harness` rather than the now-missing `provider` column.
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
  d.event_class,
  d.payload_type,
  d.actor_role,
  d.name,
  d.phase,
  d.source_ref,
  d.doc_len;
