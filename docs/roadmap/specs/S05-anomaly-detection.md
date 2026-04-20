# S05 — Agent QA and Anomaly Detection

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** P01 (summaries), P03 (graph layer), C13 (OTel export), C14 (alerts)

## Objective

Use the corpus to detect risky or low-quality agent behavior. Detect loops, repeated failed commands, high-error sessions, excessive tool calls, abrupt model switches, and long-running unresolved tasks. Build dashboards and optional alerts.

## Design Principles

1. **Findings are explainable and never overwrite source trace data.** Anomaly tables are derived projections; canonical events are immutable. [src: ADR-004]
2. **Detection is statistical and configurable, not magical.** Users can tune thresholds and disable detectors.
3. **False positives are expected and manageable.** Findings are ranked by confidence; low-confidence findings are hidden by default.
4. **Anomaly detection runs asynchronously.** It does not block ingest or retrieval.

## Schema Design

### New Tables

```sql
-- Detectors: configuration for each anomaly type.
CREATE TABLE IF NOT EXISTS moraine.anomaly_detectors (
  detector_id String,
  detector_name String,
  detector_kind LowCardinality(String),  -- 'loop', 'error_spike', 'tool_abuse', 'model_switch', 'unresolved_task'
  enabled UInt8,
  threshold_json String,                 -- kind-specific thresholds
  sensitivity Float64,                   -- 0.0 - 1.0
  created_at DateTime64(3),
  updated_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (detector_id);

-- Anomaly findings: detected issues per session.
CREATE TABLE IF NOT EXISTS moraine.anomaly_findings (
  finding_id String,
  detector_id String,
  session_id String,
  source_name LowCardinality(String),
  harness LowCardinality(String),
  confidence Float64,
  severity LowCardinality(String),       -- 'info', 'warning', 'critical'
  summary String,
  evidence_json String,                  -- [{event_uid, reason}, ...]
  suggested_action String,
  dismissed UInt8,
  dismissed_by String,
  dismissed_at DateTime64(3),
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (session_id, detector_id, created_at);

-- Anomaly metrics: time-series of detection rates.
CREATE TABLE IF NOT EXISTS moraine.anomaly_metrics (
  ts DateTime64(3),
  detector_id String,
  sessions_scanned UInt32,
  findings_count UInt32,
  avg_confidence Float64,
  p95_confidence Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (detector_id, ts);
```

## Detectors

### Loop Detector

**Definition:** Repeating the same tool call or command with identical arguments > N times within M turns.

**Thresholds:**
- `max_identical_repeats`: 3
- `window_turns`: 10

### Error Spike Detector

**Definition:** Session where error rate (tool_error + op_status = 'failure') exceeds threshold.

**Thresholds:**
- `error_rate_threshold`: 0.3 (30%)
- `min_events`: 10

### Tool Abuse Detector

**Definition:** Excessive tool call volume relative to session length.

**Thresholds:**
- `max_tool_calls_per_turn`: 10
- `max_total_tool_calls`: 100

### Model Switch Detector

**Definition:** Abrupt change of `inference_provider` or `model` mid-session.

**Thresholds:**
- `flag_any_switch`: true (any switch is noted)
- `min_events_before_switch`: 5

### Unresolved Task Detector

**Definition:** Session with task start marker but no task complete marker within expected duration.

**Thresholds:**
- `max_duration_minutes`: 60
- `require_task_complete`: true

## API Sketches

### MCP Tools

#### `list_anomalies` (new tool)

**Input schema:**
```json
{
  "session_id": "string?",
  "detector_kind": "string?",
  "severity": "string?",
  "min_confidence": "number?",
  "limit": "number?",
  "cursor": "string?",
  "verbosity": "prose | full",
  "safety_mode": "normal | strict"
}
```

**Output schema (full):**
```json
{
  "findings": [{
    "finding_id": "string",
    "detector_kind": "string",
    "session_id": "string",
    "confidence": "number",
    "severity": "string",
    "summary": "string",
    "evidence": [{"event_uid": "string", "reason": "string"}]
  }],
  "_safety": { ... }
}
```

### CLI Commands

```bash
moraine anomalies list [--session ...] [--severity warning]
moraine anomalies show <finding_id>
moraine anomalies dismiss <finding_id> --reason "..."
moraine anomalies detectors list
moraine anomalies detectors edit <detector_id> --threshold ...
moraine anomalies scan <session_id>          # manual rescan
```

### Monitor Endpoints

- `GET /api/anomalies` — list findings with filters.
- `GET /api/anomalies/:finding_id` — detail with evidence.
- `POST /api/anomalies/:finding_id/dismiss` — dismiss finding.
- `GET /api/anomalies/detectors` — detector config.
- `PUT /api/anomalies/detectors/:id` — update thresholds.
- `GET /api/anomalies/metrics` — time-series chart data.

## Data Flow

1. **Trigger:** Async job runs periodically (every 5 minutes) or on demand.
2. **Scan:** Job reads recent sessions from `events` not yet scanned.
3. **Detect:** Each enabled detector evaluates the session.
4. **Store:** Findings written to `anomaly_findings`.
5. **Alert:** C14 alerts can fire on `severity = 'critical'` findings.
6. **Report:** Monitor dashboard shows findings by detector, severity, and trend.
7. **OTel:** Findings exported as traces/metrics if C13 OTel export is enabled.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Some long loops are legitimate debugging | Confidence score; user can dismiss and provide reason. Dismissed findings are excluded from alerts. |
| Models and harnesses produce different event shapes | Detectors are harness-aware; thresholds can vary by harness. |
| False positives erode trust | Default sensitivity is conservative (0.7+ confidence shown). User tunes per detector. |
| Detection lags behind real-time | Async by design; near-real-time detection is not required for this use case. |
| Very large sessions | Detectors process sessions in turn-based chunks to bound memory. |

## Acceptance Contract

### Functional
- [ ] Loop detector flags sessions with >3 identical tool calls in 10 turns.
- [ ] Error spike detector flags sessions with >30% error rate and ≥10 events.
- [ ] Dismissed findings do not reappear on rescan unless session changes.
- [ ] Evidence links to actual `events.event_uid` rows.

### Operational
- [ ] Detection job processes 1000 sessions in under 60 seconds.
- [ ] Detection does not block ingest or search queries.
- [ ] Findings included in backup/restore (R01).

### Safety
- [ ] Findings do not expose redacted content; evidence uses event UIDs, not raw text.
- [ ] Anomaly retrieval carries `_safety` envelope.
- [ ] `strict` mode suppresses detailed evidence JSON.

### Compatibility
- [ ] Detectors can be added without schema changes (threshold_json is flexible).
- [ ] Existing sessions are backfilled on first run.

### Observability
- [ ] Monitor shows findings count by detector, severity trend, and average confidence.
- [ ] `moraine doctor` checks for detectors disabled or misconfigured.

## PR Sequencing

1. `schema(anomalies): add anomaly_detectors, anomaly_findings, anomaly_metrics tables`  
   - SQL only.
2. `feat(anomalies): add anomaly detection framework and loop detector`  
   - New crate `moraine-anomalies-core`; trait `AnomalyDetector`.
3. `feat(anomalies): add error spike and tool abuse detectors`  
   - Additional detectors.
4. `feat(anomalies): add model switch and unresolved task detectors`  
   - Additional detectors.
5. `feat(mcp): add list_anomalies tool`  
   - MCP surface.
6. `feat(cli): add anomalies list, show, dismiss commands`  
   - CLI surface.
7. `feat(monitor): add anomalies dashboard`  
   - UI with findings list, evidence viewer, detector config.
8. `feat(alerts): wire anomaly findings to alert system`  
   - C14 integration.
9. `test(anomalies): add golden session fixtures for each detector`  
   - Known-bad sessions; verify detection precision/recall.

## Open Questions

1. **ML vs rule-based detection:** Start with rule-based (transparent, tunable). Add ML-based anomaly scoring as opt-in future work.
2. **Real-time detection:** Out of scope for P3. Async batch processing is sufficient.
3. **Cross-session anomaly detection:** e.g., "user keeps making the same mistake." Requires P03 graph layer. Deferred to graph-layer follow-up.
