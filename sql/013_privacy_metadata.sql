-- Add ingest-time privacy metadata columns. These columns are additive and
-- default to "not processed" so existing rows and queries keep working.

ALTER TABLE moraine.raw_events
  ADD COLUMN IF NOT EXISTS privacy_policy_version String DEFAULT '' AFTER event_uid,
  ADD COLUMN IF NOT EXISTS privacy_redaction_applied UInt8 DEFAULT 0 AFTER privacy_policy_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_count UInt64 DEFAULT 0 AFTER privacy_redaction_applied,
  ADD COLUMN IF NOT EXISTS privacy_redaction_kinds Array(String) DEFAULT [] AFTER privacy_redaction_count,
  ADD COLUMN IF NOT EXISTS privacy_key_id String DEFAULT '' AFTER privacy_redaction_kinds;

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS privacy_policy_version String DEFAULT '' AFTER token_usage_json,
  ADD COLUMN IF NOT EXISTS privacy_redaction_applied UInt8 DEFAULT 0 AFTER privacy_policy_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_count UInt64 DEFAULT 0 AFTER privacy_redaction_applied,
  ADD COLUMN IF NOT EXISTS privacy_redaction_kinds Array(String) DEFAULT [] AFTER privacy_redaction_count,
  ADD COLUMN IF NOT EXISTS privacy_key_id String DEFAULT '' AFTER privacy_redaction_kinds;

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS privacy_policy_version String DEFAULT '' AFTER event_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_applied UInt8 DEFAULT 0 AFTER privacy_policy_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_count UInt64 DEFAULT 0 AFTER privacy_redaction_applied,
  ADD COLUMN IF NOT EXISTS privacy_redaction_kinds Array(String) DEFAULT [] AFTER privacy_redaction_count,
  ADD COLUMN IF NOT EXISTS privacy_key_id String DEFAULT '' AFTER privacy_redaction_kinds;

ALTER TABLE moraine.ingest_errors
  ADD COLUMN IF NOT EXISTS privacy_policy_version String DEFAULT '' AFTER raw_fragment,
  ADD COLUMN IF NOT EXISTS privacy_redaction_applied UInt8 DEFAULT 0 AFTER privacy_policy_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_count UInt64 DEFAULT 0 AFTER privacy_redaction_applied,
  ADD COLUMN IF NOT EXISTS privacy_redaction_kinds Array(String) DEFAULT [] AFTER privacy_redaction_count,
  ADD COLUMN IF NOT EXISTS privacy_key_id String DEFAULT '' AFTER privacy_redaction_kinds;

ALTER TABLE moraine.search_documents
  ADD COLUMN IF NOT EXISTS privacy_policy_version String DEFAULT '' AFTER token_usage_json,
  ADD COLUMN IF NOT EXISTS privacy_redaction_applied UInt8 DEFAULT 0 AFTER privacy_policy_version,
  ADD COLUMN IF NOT EXISTS privacy_redaction_count UInt64 DEFAULT 0 AFTER privacy_redaction_applied,
  ADD COLUMN IF NOT EXISTS privacy_redaction_kinds Array(String) DEFAULT [] AFTER privacy_redaction_count,
  ADD COLUMN IF NOT EXISTS privacy_key_id String DEFAULT '' AFTER privacy_redaction_kinds;

DROP VIEW IF EXISTS moraine.mv_search_documents_from_events;

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
  token_usage_json,
  privacy_policy_version,
  privacy_redaction_applied,
  privacy_redaction_count,
  privacy_redaction_kinds,
  privacy_key_id
FROM moraine.events
WHERE lengthUTF8(replaceRegexpAll(text_content, '\\s+', '')) > 0;
