# S06 — Multimodal Trace Ingestion

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** P03 (entity/attachment layer), C13 (OTel export)

## Objective

Some future harnesses will store screenshots, images, audio, or rendered artifacts. Plan for them but do not let them destabilize text trace ingest. Binary data is bounded, typed, and never inserted into prompts without clear user control.

## Design Principles

1. **Text ingest remains the fast path.** Multimodal attachments are secondary; they must not slow down or complicate the critical text pipeline. [src: ADR-001]
2. **Binary data is content-addressed and stored outside ClickHouse.** ClickHouse stores metadata and references; blobs live in a local content-addressed store. [src: ADR-004]
3. **Attachments are immutable and deduplicated.** Same content hash = same storage location; no duplicates. [src: ADR-005]
4. **OCR/transcription is optional and offline.** Text extraction runs asynchronously after blob storage. The text index is updated when extraction completes.

## Schema Design

### New Tables

```sql
-- Attachment metadata: what, where, when.
CREATE TABLE IF NOT EXISTS moraine.attachments (
  attachment_uid String,
  session_id String,
  event_uid String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  attachment_kind LowCardinality(String), -- 'screenshot', 'image', 'audio', 'video', 'pdf', 'rendered_html', 'unknown'
  mime_type LowCardinality(String),
  content_hash String,                   -- SHA-256 of blob
  blob_path String,                      -- relative path in blob store
  original_filename String,
  size_bytes UInt64,
  width UInt32,
  height UInt32,
  duration_seconds Float64,
  extracted_text String,                 -- OCR or transcription result
  extraction_status LowCardinality(String), -- 'pending', 'complete', 'failed', 'unsupported'
  extracted_at DateTime64(3),
  privacy_policy_version String,
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (session_id, attachment_uid);

-- Attachment links: connect attachments to events, summaries, notes.
CREATE TABLE IF NOT EXISTS moraine.attachment_links (
  link_uid String,
  attachment_uid String,
  target_kind LowCardinality(String),    -- 'event', 'summary', 'note', 'memory_card'
  target_id String,
  link_role LowCardinality(String),      -- 'primary', 'context', 'reference'
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (attachment_uid, target_id);
```

### Blob Store

Local filesystem-based content-addressed store:

```
~/.moraine/blobs/
  <content_hash[:2]>/
    <content_hash[2:4]>/
      <content_hash>          # raw blob file
      <content_hash>.meta     # JSON metadata (mime, size, original name)
```

- Max blob size: 50MB default (configurable).
- Total store size cap: 10GB default; LRU eviction of unreferenced blobs when cap exceeded.
- Reference counting: blobs referenced by `attachments` table are protected; unreferenced blobs are garbage collected.

## API Sketches

### MCP Tools

#### `get_attachment` (new tool)

**Input schema:**
```json
{
  "attachment_uid": "string",
  "include_extracted_text": "boolean?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "attachment_uid": "string",
  "attachment_kind": "string",
  "mime_type": "string",
  "size_bytes": "number",
  "dimensions": {"width": "number", "height": "number"},
  "extracted_text": "string?",
  "blob_url": "string",              # local file URL or base64 data URL
  "_safety": { ... }
}
```

**Behavior:**
- Returns metadata and local blob path.
- Does NOT return base64-encoded binary by default (too large for MCP). Client reads blob from local path.
- `strict` mode suppresses `extracted_text` if it contains sensitive patterns.

#### `search` extension

Add `include_attachments` boolean. When true, search also matches `attachments.extracted_text`.

### CLI Commands

```bash
moraine attachments list <session_id>
moraine attachments show <attachment_uid>
moraine attachments extract-text <attachment_uid>  # trigger or re-trigger OCR
moraine attachments gc                             # garbage collect unreferenced blobs
moraine attachments stats                          # store size, count, eviction stats
```

### Monitor Endpoints

- `GET /api/attachments?session_id=...` — list attachments.
- `GET /api/attachments/:attachment_uid` — metadata + preview URL.
- `GET /api/attachments/:attachment_uid/blob` — serve blob (range requests supported).
- `POST /api/attachments/:attachment_uid/extract` — queue OCR/transcription.

## Data Flow

1. **Ingest:** Harness provides attachment reference in event payload (e.g., `{"screenshot": "/path/to/img.png"}`).
2. **Normalize:** Normalizer extracts attachment path, computes SHA-256, copies to blob store if not present.
3. **Store:** Writes `attachments` row with `extraction_status = 'pending'`.
4. **Extract:** Async worker runs OCR (tesseract) or transcription (whisper) depending on `attachment_kind`.
5. **Index:** When extraction completes, `extracted_text` is added to `search_documents` via materialized view or explicit backfill.
6. **Retrieve:** MCP `get_attachment` returns metadata; monitor displays preview; search matches extracted text.
7. **GC:** Periodic job removes unreferenced blobs exceeding size cap.

## OCR/Transcription Pipeline

**Screenshot → OCR:**
- Engine: Tesseract or equivalent (local-only, no cloud).
- Languages: English default; configurable additional languages.
- Output: plain text + bounding box JSON (optional).

**Audio → Transcription:**
- Engine: Whisper.cpp or equivalent (local-only).
- Output: plain text + segment timestamps (optional).

**PDF → Text:**
- Engine: pdftotext or pdfium.
- Output: plain text + page mapping.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Prompt injection hidden in images | OCR text is treated as untrusted memory; same `_safety` envelope and redaction pipeline as regular text. |
| Large files and GIF/frame bombs | Max blob size enforced (50MB); GIFs are not exploded into frames; only first frame processed. |
| Copyright and privacy concerns | Attachments are local-only; never uploaded to cloud. Privacy redaction runs on extracted text. |
| OCR can hallucinate or omit text | `extraction_status = 'complete'` does not guarantee accuracy; user can manually correct extracted text. |
| Blob store fills disk | Configurable size cap; LRU eviction of unreferenced blobs; alerts when cap >80%. |
| Very large images crash OCR | Resize to max 4096x4096 before OCR; preserve original in blob store. |

## Acceptance Contract

### Functional
- [ ] Screenshot attachment is stored in blob store and searchable via OCR text within 30 seconds.
- [ ] Same attachment referenced twice stores only one blob (deduplication by hash).
- [ ] `get_attachment` returns local file path, not base64 payload.
- [ ] Search matches extracted text when `include_attachments = true`.

### Operational
- [ ] Attachment ingest does not slow text event ingest by more than 10%.
- [ ] Blob store garbage collection reclaims space within 5 minutes of attachment deletion.
- [ ] OCR worker processes 100 screenshots in under 60 seconds (local CPU dependent).

### Safety
- [ ] Extracted text runs through privacy redaction pipeline before search indexing.
- [ ] Attachments from encrypted sessions are encrypted at rest (blob store uses same key provider as R09).
- [ ] `_safety` envelope on attachment retrieval labels content as untrusted memory.

### Compatibility
- [ ] Attachments table does not alter existing `events` schema (only adds new table).
- [ ] Existing text-only search behavior unchanged when `include_attachments = false`.

### Observability
- [ ] Monitor shows attachment count by kind, blob store size, OCR queue depth, and extraction success rate.
- [ ] `moraine doctor` checks for missing blobs (attachment row exists but blob file missing).

## PR Sequencing

1. `schema(attachments): add attachments and attachment_links tables`  
   - SQL only.
2. `feat(attachments): add content-addressed blob store`  
   - Local filesystem store; hash computation; deduplication.
3. `feat(attachments): add attachment normalization and ingest integration`  
   - Normalizer extracts attachments from harness payloads.
4. `feat(attachments): add OCR pipeline for screenshots and images`  
   - Tesseract integration; async worker.
5. `feat(attachments): add transcription pipeline for audio`  
   - Whisper integration; optional feature flag.
6. `feat(mcp): add get_attachment tool and search integration`  
   - MCP surface.
7. `feat(cli): add attachments list, show, extract-text, gc commands`  
   - CLI surface.
8. `feat(monitor): add attachment gallery and preview`  
   - UI with lazy-loaded thumbnails.
9. `test(attachments): add fixture images and OCR accuracy tests`  
   - Golden images with expected text; measure precision/recall.

## Open Questions

1. **Which OCR engine?** Tesseract is battle-tested and local. Evaluate easyocr or paddleocr if CJK accuracy is insufficient.
2. **Should video be supported?** Not in first slice. Video is complex (frame extraction, scene detection). Defer to P3 follow-up.
3. **Should extracted text include formatting?** Plain text first; markdown or structured output (tables, code blocks) as future enhancement.
