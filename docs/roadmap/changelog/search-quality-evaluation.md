# Search Quality Evaluation Harness

## Summary

Added a search quality evaluation harness for measuring Moraine's BM25 retrieval
against labeled query sets. This gives search changes a measurable contract
before ranking, weighting, phrase, proximity, or hybrid retrieval work changes
user-facing results.

## Scope

Roadmap area: C07 search relevance evaluation loop.

## What Changed

- Added `scripts/bench/search_quality_eval.py`.
- Added sample qrels and offline-result fixtures under `fixtures/search_eval/`.
- Added `docs/operations/search-quality-evaluation.md`.
- Supported offline evaluation without ClickHouse or Python binding builds.
- Supported live evaluation against a local Moraine/ClickHouse corpus.
- Supported strategy comparison, synthetic qrel generation from telemetry, and
  machine-readable JSON reports.

The harness reports common information-retrieval metrics:

- Precision@K
- Recall@K
- F1@K
- NDCG@K
- MRR
- MAP

## Operational Impact

No runtime service behavior changes. The harness is a developer/operator tool
for creating baselines and regression gates before search relevance work.

## Validation

- Offline fixture self-check.
- Standard docs build coverage through MkDocs.

## Follow-Up

- Curate local qrels from real Moraine use cases.
- Add CI thresholds once the qrels are stable.
- Compare field weighting, phrase/proximity, and future semantic/hybrid
  retrieval strategies against the same qrels.
