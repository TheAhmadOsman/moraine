# Search Quality Evaluation Harness

## Purpose

Use `scripts/bench/search_quality_eval.py` to measure the retrieval quality of Moraine's BM25 search implementation against labeled query sets (qrels).

The harness computes standard information-retrieval metrics:

- **Precision@K** (P@K): fraction of top-K results that are relevant.
- **Recall@K** (R@K): fraction of all relevant documents found in top-K.
- **F1@K**: harmonic mean of precision and recall at K.
- **NDCG@K**: normalized discounted cumulative gain, rewarding highly-relevant items at top ranks.
- **MRR** (mean reciprocal rank): average of `1 / rank_of_first_relevant`.
- **MAP** (mean average precision): average precision across all recall levels.

It supports two modes:

1. **Live evaluation**: runs queries through the local `moraine_conversations` Python binding against a live ClickHouse database.
2. **Offline evaluation**: loads pre-computed results from JSON fixtures and computes metrics without any database or binding build. This is useful for CI, quick regression checks, and sharing reproducible evaluation snapshots.

## Qrels Format

A qrels file is JSON with this shape:

```json
{
  "queries": [
    {
      "query_id": "q-auth-error",
      "query_text": "authentication error login failed",
      "relevant": [
        {"event_uid": "evt-auth-001", "relevance": 2},
        {"event_uid": "evt-auth-002", "relevance": 1}
      ]
    }
  ]
}
```

- `query_id`: stable identifier for the query.
- `query_text`: the raw query string passed to search.
- `relevant`: list of judged events. `relevance` is an integer grade (0 = non-relevant, 1 = relevant, 2 = highly relevant, 3 = perfectly relevant). The harness treats any grade > 0 as relevant for binary metrics and uses the full grade for NDCG.

## Offline Results Format

For offline evaluation, provide a JSON file mapping query IDs to ranked hit lists:

```json
{
  "strategy": "optimized",
  "results": {
    "q-auth-error": [
      {"rank": 1, "event_uid": "evt-auth-001", "score": 14.52, "session_id": "sess-a1"},
      {"rank": 2, "event_uid": "evt-auth-002", "score": 8.31, "session_id": "sess-a1"}
    ]
  }
}
```

## Example Usage

### Offline self-check (no database, no build)

Run the harness against the built-in fixtures to verify the metric pipeline:

```bash
uv run --script scripts/bench/search_quality_eval.py --self-check
```

To write the full offline report:

```bash
uv run --script scripts/bench/search_quality_eval.py \
    --qrels fixtures/search_eval/sample_qrels.json \
    --offline-results fixtures/search_eval/sample_results.json \
    --output-json /tmp/search-quality-offline.json
```

### Live evaluation against a local stack

```bash
uv run --script scripts/bench/search_quality_eval.py \
    --config config/moraine.toml \
    --qrels fixtures/search_eval/sample_qrels.json \
    --output-json /tmp/search-quality-live.json
```

### Compare two search strategies

```bash
uv run --script scripts/bench/search_quality_eval.py \
    --config config/moraine.toml \
    --qrels fixtures/search_eval/sample_qrels.json \
    --compare-strategies optimized,oracle_exact \
    --output-json /tmp/search-quality-compare.json
```

### Generate synthetic qrels from telemetry

If you have search query logs in ClickHouse, bootstrap a labeled dataset:

```bash
uv run --script scripts/bench/search_quality_eval.py \
    --config config/moraine.toml \
    --generate-qrels \
    --window 7d \
    --top-n 50 \
    --output-qrels /tmp/synthetic_qrels.json
```

This treats the top-ranked hit from each logged query as highly relevant (grade 2) and additional top-3 hits as partially relevant (grade 1). Review and curate the output before using it as a ground-truth benchmark.

### Dry-run to inspect a qrels file

```bash
uv run --script scripts/bench/search_quality_eval.py \
    --qrels fixtures/search_eval/sample_qrels.json \
    --dry-run
```

## CLI Flags

- `--config <path>`: Moraine config file for ClickHouse connectivity (required for live evaluation and qrels generation).
- `--qrels <path>`: labeled query set JSON.
- `--offline-results <path>`: pre-computed results JSON for offline evaluation (no DB needed).
- `--generate-qrels`: generate synthetic qrels from `search_query_log` / `search_hit_log`.
- `--output-qrels <path>`: write generated qrels to this file.
- `--window <duration>`: telemetry lookback for generation (`s`, `m`, `h`, `d`, `w`).
- `--top-n <int>`: number of queries to sample from the log.
- `--min-results <int>`: minimum `result_count` to include a logged query.
- `--exclude-source <name>`: exclude sources from generation (repeatable).
- `--limit <int>`: search result limit for live evaluation.
- `--k-values <list>`: comma-separated cutoffs for @K metrics (default `1,3,5,10,25`).
- `--compare-strategies <list>`: comma-separated strategies for live evaluation (default `optimized`).
- `--output-json <path>`: write machine-readable report.
- `--skip-maturin-develop`: skip local binding rebuild before live evaluation.
- `--dry-run`: load qrels and print stats, but skip search execution.
- `--self-check`: run the built-in offline fixtures and assert basic metric sanity.

## Output

Console output includes:

- per-strategy summary with all @K metrics, MRR, and MAP,
- comparison table when multiple strategies are evaluated.

JSON output includes:

- `meta` (timestamp, git SHA, paths, parameters),
- `summaries` (aggregate metrics per strategy),
- `per_query` (metrics for each individual query).

## Exit Codes

- `0`: evaluation completed successfully.
- non-zero: fatal setup error, empty qrels, or binding build failure.

## Troubleshooting

- **ImportError for moraine_conversations**: ensure you run via `uv run --script` or have built the binding with `maturin develop`.
- **No queries found**: verify the qrels JSON structure matches the expected schema.
- **Offline results missing a query_id**: the harness silently returns an empty hit list for missing queries; check that your offline results cover all query IDs in the qrels.
