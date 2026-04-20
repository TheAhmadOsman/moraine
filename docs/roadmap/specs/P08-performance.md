# P08 — Performance Tuning and Query Plan Snapshots

**Priority:** P2  
**Effort:** L  
**Status:** Specification / ready for design review  
**Dependencies:** C07 (relevance loop), C12 (retention policy)

## Objective

Keep query and ingest performance visible as the corpus grows. Store query plan snapshots, evaluate physical design choices against measured workload, and add latency budgets to CI or nightly jobs.

## Design Principles

1. **Performance is measured, not assumed.** Every optimization PR includes before/after numbers from reproducible benchmarks. [src: ADR-001]
2. **Query plans are durable artifacts.** Snapshots let operators compare plans across schema changes, ClickHouse upgrades, and data growth.
3. **Benchmarks are fixture-driven, not production-dependent.** Use synthetic corpora of known size so CI can run them.
4. **Latency budgets are team contracts.** A query that exceeds its budget is a regression, even if it "still feels fast."

## Schema Design

### New Tables

```sql
-- Query plan snapshots: EXPLAIN PLAN output for key queries.
CREATE TABLE IF NOT EXISTS moraine.query_plan_snapshots (
  snapshot_id String,
  query_name LowCardinality(String),     -- 'event_search', 'conversation_search', 'session_list', 'source_health'
  query_hash UInt64,
  query_text String,
  plan_text String,
  plan_json String,                      -- structured plan if available
  corpus_events UInt64,                  -- events.count() at snapshot time
  corpus_docs UInt64,                    -- search_documents.count()
  clickhouse_version String,
  moraine_version String,
  snapshot_at DateTime64(3),
  duration_ms UInt32,
  read_rows UInt64,
  read_bytes UInt64,
  result_rows UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (query_name, snapshot_at);

-- Performance benchmarks: recorded runs against fixture corpora.
CREATE TABLE IF NOT EXISTS moraine.performance_benchmarks (
  benchmark_id String,
  benchmark_name LowCardinality(String),
  corpus_size UInt64,
  query_count UInt32,
  total_duration_ms UInt64,
  p50_ms UInt32,
  p95_ms UInt32,
  p99_ms UInt32,
  max_ms UInt32,
  errors UInt32,
  clickhouse_version String,
  moraine_version String,
  run_at DateTime64(3),
  git_sha String,
  metadata_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_at)
ORDER BY (benchmark_name, run_at);
```

## API Sketches

### CLI Commands

```bash
moraine perf snapshot --query event_search --explain
moraine perf benchmark --corpus 1m --queries queries.jsonl
moraine perf compare <snapshot_id_1> <snapshot_id_2>
moraine perf history --query event_search --last 30
moraine perf budget --query session_list --p95 200ms
```

### Monitor Endpoints

- `GET /api/perf/snapshots?query_name=...` — query plan history.
- `GET /api/perf/benchmarks` — benchmark run history.
- `GET /api/perf/budgets` — current latency budgets and status.
- `POST /api/perf/snapshot` — trigger manual snapshot.

### MCP Tools

No new MCP tools. Performance is operator-facing, not agent-facing.

## Data Flow

1. **Fixture Generation:** Script generates synthetic corpus of N events (1K, 10K, 100K, 1M, 10M).
2. **Benchmark Run:** Load fixture corpus, run standard query set, record latencies.
3. **Plan Capture:** Run `EXPLAIN PLAN` for key queries, store in `query_plan_snapshots`.
4. **Comparison:** `moraine perf compare` diffs two plans highlighting index usage changes.
5. **Alerting:** Monitor shows budget status; C14 (alerts) can warn on regression.

## Key Queries to Snapshot

| Query | Table | Why |
|---|---|---|
| Event search (BM25 scoring) | `search_postings` + `search_documents` | Most frequent MCP query |
| Conversation search | `search_postings` + session grouping | Second most frequent |
| Session list | `events` + `v_session_summary` | Monitor home page |
| Source health snapshot | `ingest_checkpoints` + `raw_events` + `ingest_errors` | Operational visibility |
| Trace reconstruction | `v_conversation_trace` | Forensic debugging |
| Entity search (P03) | `entities` + `entity_occurrences` | Graph traversal |

## Physical Design Evaluation

### Primary Keys

Current `events` order key: `(session_id, event_ts, source_name, source_file, source_generation, source_offset, source_line_no, event_uid)`.

Evaluate alternatives:
- `(session_id, event_ts, event_uid)` — shorter key, faster point lookups, but slower source-scoped queries.
- Keep current key unless benchmark shows >20% improvement.

### Projections / Skipping Indexes

Evaluate ClickHouse projections for:
- `events` filtered by `harness` and `event_kind` (common monitor filters).
- `search_documents` pre-aggregated by `session_id` for conversation search.

Evaluate data skipping indexes for:
- `events.event_ts` range queries.
- `search_postings.term` equality (already well-partitioned).

### Materialized View Costs

Measure write amplification from:
- `mv_search_documents_from_events`
- `mv_search_postings`
- Future MVs for summaries (P01), notes (P02), entities (P03)

If MV fanout exceeds 3x write amplification, consider batching or async insert tuning.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| ClickHouse planner changes across versions | Snapshot includes `clickhouse_version`; compare plans before/after upgrade. |
| Projections slow ingest or increase storage | Measure with benchmarks; projections are optional and can be dropped. |
| `FINAL` is expensive | Benchmark reads with/without `FINAL`; document when `FINAL` is required vs optional. |
| Laptop hardware varies widely | Benchmarks report hardware spec; CI runs on consistent sandbox specs. |
| Fixture corpora do not match real workload | Supplement with query telemetry from `search_query_log` to generate realistic query distributions. |

## Acceptance Contract

### Functional
- [ ] `moraine perf benchmark` runs a standard query set and records p50/p95/p99.
- [ ] `moraine perf snapshot` captures EXPLAIN PLAN for key queries.
- [ ] `moraine perf compare` produces a readable diff of two plans.

### Operational
- [ ] Benchmarks run in CI for every PR that touches `sql/` or `crates/moraine-conversations/`.
- [ ] Nightly benchmark runs against 1M and 10M fixture corpora in sandbox.
- [ ] Query plan snapshots are retained for 90 days (configurable TTL).

### Safety
- [ ] Performance benchmarks do not run against production user data (fixture-only).
- [ ] Query plan snapshots do not include query text that might contain secrets (hash or redact raw queries).

### Compatibility
- [ ] Performance tables do not affect existing schema or application behavior.
- [ ] Latency budgets are configurable, not hardcoded.

### Observability
- [ ] Monitor shows benchmark trend charts (p50/p95 over time).
- [ ] `moraine doctor` warns if recent query plans show full table scans where indexes were previously used.

## PR Sequencing

1. `feat(perf): add query_plan_snapshots and performance_benchmarks tables`  
   - SQL only.
2. `feat(perf): add fixture corpus generator`  
   - Script to generate N events with realistic distributions.
3. `feat(perf): add benchmark runner and snapshot capture`  
   - New crate `moraine-perf-core` or scripts.
4. `feat(cli): add perf snapshot, benchmark, compare commands`  
   - CLI surface.
5. `feat(monitor): add performance dashboard with trend charts`  
   - Svelte charts (e.g., lightweight SVG or uPlot).
6. `ci(perf): add performance regression job to CI`  
   - Runs on PRs touching search/ingest/schema.

## Open Questions

1. **Benchmark tool:** Custom Rust binary or reuse existing load-test frameworks? Recommendation: custom binary for tight integration with Moraine config and schema.
2. **ClickHouse `EXPLAIN` JSON parsing:** ClickHouse EXPLAIN output format varies by version. Store raw text + best-effort JSON.
3. **Should benchmarks run in the dev sandbox?** Yes — sandbox provides consistent Linux + ClickHouse environment.
