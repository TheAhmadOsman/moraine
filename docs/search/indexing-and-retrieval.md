# BM25 Indexing and Retrieval

## Retrieval Objective

Moraine retrieval is designed around one principle: ranking should be cheap at query time because corpus transformation has already been paid incrementally at ingest time. ClickHouse materialized views maintain search documents, postings, and statistics as canonical events land. The MCP process stays thin: it validates requests, asks `moraine-conversations` for bounded reads, formats results, and adds safety metadata. [src: sql/004_search_index.sql, crates/moraine-conversations/src/clickhouse_repo.rs, crates/moraine-mcp-core/src/lib.rs]

The stack has four cooperating stages:

1. Ingestion normalizes source records into canonical `events`.
2. ClickHouse projects searchable text into `search_documents`.
3. ClickHouse explodes documents into sparse `search_postings` and derives term/corpus statistics.
4. MCP tools run event-level or conversation-level queries over those indexed surfaces.

When search quality degrades, debug those stages in order. Formula tuning cannot recover text that was never normalized or indexed.

Ingest-time privacy redaction happens before stage 2. If `text_content_mode` redacts, hashes, or drops a secret-bearing string, the search index sees that transformed representation for future rows. Historical search rows keep prior semantics until reingested or rebuilt.

## Index Construction in ClickHouse

`search_documents` is the document surface. It stores event UID, session metadata, class/type fields, payload JSON, text content, and `doc_len`. `doc_len` is materialized from regex token extraction on lowercased text so average document length and BM25 denominator terms are available cheaply at query time. [src: sql/004_search_index.sql]

`mv_search_documents_from_events` feeds `search_documents` from canonical `events`. It carries compacted lineage, filters whitespace-only text, and keeps document refresh aligned with event replacement semantics.

`search_postings` is the sparse term-doc index. Its materialized view tokenizes each document, filters term lengths, groups by `(term, doc)`, and stores term frequency plus enough context metadata for result hydration. The table is partitioned by hashed term buckets and ordered by `(term, doc_id)`, which makes term-constrained scans the dominant access path instead of full-corpus scans.

`search_term_stats` and `search_corpus_stats` expose document-frequency and corpus-wide totals. The repository can fall back to direct aggregate queries when stats are absent or incomplete, which keeps bootstrap and partial-repair states searchable.

## Event Search

The `search` MCP tool maps to event-level search. It accepts:

- `query`
- optional `limit`
- optional `session_id`
- optional `min_score`
- optional `min_should_match`
- optional `include_tool_events`
- optional `event_kind`
- optional `exclude_codex_mcp`
- optional `include_payload_json`
- optional `safety_mode`
- optional `verbosity`

The repository tokenizes query text, clamps term count, validates safe filters, computes or loads BM25 statistics, builds SQL over `search_postings`, hydrates event hits, applies content policy, and logs telemetry best-effort. [src: crates/moraine-conversations/src/clickhouse_repo.rs, crates/moraine-mcp-core/src/lib.rs]

By default, payload JSON is not exposed and non-user-facing content is redacted in returned hits. `include_payload_json=true` only affects user-facing message events and can be suppressed by `safety_mode="strict"`.

## Conversation Search

The `search_conversations` MCP tool ranks whole sessions rather than individual events. It uses session-level candidate generation and exact fallback paths so each returned hit represents one conversation. It accepts time bounds, conversation `mode`, tool-event inclusion, codex-MCP exclusion, payload opt-in, and BM25 thresholds.

Conversation mode classification is exclusive and precedence-based:

```text
web_search > mcp_internal > tool_calling > chat
```

Mode filtering is useful for high-level questions like “show me sessions involving web search” or “find tool-heavy debugging sessions” without relying on brittle keyword-only queries. [src: crates/moraine-conversations/src/domain.rs, crates/moraine-conversations/src/clickhouse_repo.rs]

## Session Navigation Tools

MCP retrieval includes deterministic navigation tools that do not depend on BM25 ranking:

- `list_sessions` lists session summaries with cursor pagination, optional time bounds, optional mode filter, and asc/desc sort.
- `get_session` returns stable summary metadata for one session ID and uses `found=false` for misses.
- `get_session_events` returns a paginated session timeline in forward or reverse order with optional event-kind filtering.
- `open` reconstructs either an event context window by `event_uid` or a paged session transcript by `session_id`.

These tools all read from the same conversation repository abstraction as search. They are useful when an agent already knows a session or event identity and needs deterministic trace traversal instead of ranked discovery.

## BM25 Formula and Query Cost

IDF is computed with Okapi-style smoothing:

```text
ln(1 + ((N - df + 0.5) / (df + 0.5)))
```

The SQL scoring path embeds query terms, aligned IDF values, `k1`, `b`, and average document length. It constrains postings by query terms, sums BM25 contributions per document, applies `matched_terms` and score thresholds, orders by score, and limits results.

Because scoring runs over postings constrained by query terms, dominant cost is posting fanout and term selectivity, not total corpus size. Long queries with broad terms can still be expensive, so `mcp.max_query_terms`, `mcp.max_results`, and `min_should_match` remain important guardrails. [src: crates/moraine-conversations/src/clickhouse_repo.rs, config/moraine.toml]

## Policy Filters

Retrieval defaults optimize agent answer quality, not maximal low-signal recall.

Event search can exclude tool events unless explicitly requested. It can also exclude codex-MCP self-observation to prevent loops where prior search/open tool traces dominate later retrieval. Event kind filters (`message`, `reasoning`, `tool_call`, `tool_result`) let callers narrow result classes more explicitly than broad text filters.

The MCP layer applies response content policy after repository retrieval:

- Non-user-facing event hits lose `text_content` and `payload_json`.
- Payload JSON is omitted unless explicitly requested.
- Strict safety mode suppresses payload JSON opt-ins and filters low-information system events where applicable.

These policies affect returned payloads, not the underlying ClickHouse corpus.

## Safety Envelope

Every successful MCP retrieval response includes safety framing. In full mode, `_safety` is part of `structuredContent`; in prose mode, a preamble states that retrieved content is untrusted memory and reports mode, source, duration, redaction count, and filter count.

The envelope includes:

- `content_classification = "memory_content"`
- `provenance.source = "moraine-mcp"`
- query timing
- redaction counters
- filter counters
- an untrusted-memory notice

`safety_mode="normal"` preserves existing defaults with metadata. `safety_mode="strict"` only reduces exposure: it suppresses payload JSON requests, system-event expansion requests, low-information system events, and payload JSON fields where the tool can directly modify the response. [src: crates/moraine-mcp-core/src/lib.rs]

## Freshness and Rebuild Behavior

Steady-state freshness is push-driven: ingestor writes canonical rows, materialized views update search tables, and MCP reads the latest committed index state. No periodic full-corpus reindex is required for normal operation.

When tokenization, document projection, or search schema changes, backfill search tables before drawing conclusions from mixed historical and new index semantics. The operational rule is simple: if a change alters what should be in `search_documents` or `search_postings`, rebuild those tables or explicitly accept mixed semantics while testing. [src: sql/004_search_index.sql]

The first supported rebuild path is `moraine reindex --search-only`. It previews or executes a deterministic rebuild of `search_documents`, `search_postings`, and `search_conversation_terms` from the existing canonical `events` table. The CLI does not claim to replay raw sources or regenerate canonical history; broader corpus reindex remains a separate operational concern.

Implementation detail: the CLI rebuild uses the checked-in migration SQL for `mv_search_documents_from_events`, `mv_search_postings`, and the `search_conversation_terms` backfill query, so the command stays aligned with current schema and projection semantics instead of maintaining a second hand-written shell projection. Pending schema migrations block the rebuild until `moraine db migrate` is run.

## Query and Interaction Logging

Event search writes `search_query_log` rows and `search_hit_log` rows when telemetry tables are available. Logging is best-effort and should not fail retrieval. These tables are useful for workload inspection, latency replay, and relevance-evaluation fixtures.

`search_interaction_log` is reserved for external feedback capture. It is not auto-populated by MCP today, but it remains part of the schema for future evaluation or learning loops.

## Performance and Quality Tuning

High-impact knobs:

- `mcp.max_results`
- `mcp.default_include_tool_events`
- `mcp.default_exclude_codex_mcp`
- `bm25.k1`
- `bm25.b`
- `bm25.default_min_score`
- `bm25.default_min_should_match`
- `bm25.max_query_terms`

If ranking quality looks noisy, inspect `text_content` extraction, operational-event inclusion, codex-MCP self-observation, and stale search tables before tuning constants. If latency regresses, inspect query term shape and posting fanout before raising limits.

## Known Limits

Moraine currently uses simple regex tokenization with no stemming, lemmatization, phrase scoring, semantic embeddings, or field weighting. This keeps indexing fast and predictable for code-like operational text but limits semantic recall for natural-language paraphrases.

BM25 scores are relative within a query. Do not compare raw scores across unrelated queries without calibration. Agents should prefer rank, snippet quality, and opened context over absolute score thresholds unless the query distribution is controlled.
