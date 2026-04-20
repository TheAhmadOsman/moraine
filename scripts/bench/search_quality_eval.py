#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "maturin>=1.6,<2",
# ]
# ///
"""Search quality evaluation harness for Moraine BM25 retrieval.

Loads a labeled query set (qrels), runs each query through the
moraine-conversations Python binding, and reports standard IR metrics:
Precision@K, Recall@K, F1@K, NDCG@K, MRR, and MAP.

Can also generate a synthetic evaluation dataset from the ClickHouse
search_query_log / search_hit_log tables for bootstrap evaluation, or
run in offline mode against pre-computed result fixtures.

Example usage:

    # Evaluate with an existing qrels file (live search)
    uv run --script scripts/bench/search_quality_eval.py \
        --config config/moraine.toml \
        --qrels fixtures/search_eval/sample_qrels.json \
        --output-json /tmp/search-quality.json

    # Offline evaluation against fixture results (no DB, no binding build)
    uv run --script scripts/bench/search_quality_eval.py \
        --qrels fixtures/search_eval/sample_qrels.json \
        --offline-results fixtures/search_eval/sample_results.json \
        --output-json /tmp/search-quality-offline.json

    # Generate synthetic qrels from recent search telemetry
    uv run --script scripts/bench/search_quality_eval.py \
        --config config/moraine.toml \
        --generate-qrels --window 7d --top-n 50 \
        --output-qrels /tmp/synthetic_qrels.json

    # Compare two search strategies
    uv run --script scripts/bench/search_quality_eval.py \
        --config config/moraine.toml \
        --qrels fixtures/search_eval/sample_qrels.json \
        --compare-strategies optimized,oracle_exact
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ClickHouseSettings:
    url: str
    database: str
    username: str
    password: str
    timeout_seconds: float


@dataclass
class QrelRecord:
    """Single relevance judgment: an event is relevant to a query at a grade."""

    event_uid: str
    relevance: int = 1  # 0..3 scale, 0 = non-relevant


@dataclass
class QueryQrels:
    """All relevance judgments for one query."""

    query_id: str
    query_text: str
    records: list[QrelRecord]


@dataclass
class SearchHit:
    """One hit returned by the search system."""

    rank: int
    event_uid: str
    score: float
    session_id: str = ""


@dataclass
class QueryResult:
    """Result of running one query through the search system."""

    query_id: str
    query_text: str
    strategy: str
    hits: list[SearchHit]
    elapsed_ms: float = 0.0


@dataclass
class MetricsAtK:
    """Metrics computed at a specific K."""

    k: int
    precision: float
    recall: float
    f1: float
    ndcg: float


@dataclass
class QueryMetrics:
    """All metrics for a single query."""

    query_id: str
    query_text: str
    strategy: str
    result_count: int
    at_k: list[MetricsAtK]
    reciprocal_rank: float
    average_precision: float


@dataclass
class EvalSummary:
    """Aggregate metrics across the full query set."""

    strategy: str
    query_count: int
    total_result_count: int
    mean_result_count: float
    mean_reciprocal_rank: float
    mean_average_precision: float
    at_k: list[MetricsAtK]


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def parse_window_interval(value: str) -> str:
    match = WINDOW_RE.match(value)
    if not match:
        raise ValueError("invalid --window, expected format like 24h, 7d, 30m")
    amount = int(match.group(1))
    if amount <= 0:
        raise ValueError("window amount must be > 0")
    unit = match.group(2).lower()
    unit_map = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY", "w": "WEEK"}
    return f"INTERVAL {amount} {unit_map[unit]}"


def parse_toml_scalar(value: str) -> Any:
    stripped = value.split("#", 1)[0].strip()
    if stripped == "":
        raise ValueError("empty value")
    if stripped.startswith('"') and stripped.endswith('"'):
        return json.loads(stripped)
    lowered = stripped.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"[+-]?\d+", stripped):
        return int(stripped)
    if re.fullmatch(r"[+-]?(\d+\.\d*|\d*\.\d+)([eE][+-]?\d+)?", stripped):
        return float(stripped)
    return stripped


def read_config(path: Path) -> ClickHouseSettings:
    if not path.exists():
        raise RuntimeError(f"config not found: {path}")
    section = ""
    clickhouse: dict[str, Any] = {}
    for line_no, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section != "clickhouse":
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            clickhouse[key] = parse_toml_scalar(value)
        except Exception as exc:
            raise RuntimeError(
                f"failed parsing [clickhouse] value for {key!r} at line {line_no}: {exc}"
            ) from exc

    database = str(clickhouse.get("database", "moraine"))
    if not SAFE_IDENTIFIER_RE.match(database):
        raise RuntimeError(f"unsupported database identifier: {database!r}")

    return ClickHouseSettings(
        url=str(clickhouse.get("url", "http://127.0.0.1:8123")),
        database=database,
        username=str(clickhouse.get("username", "default")),
        password=str(clickhouse.get("password", "")),
        timeout_seconds=float(clickhouse.get("timeout_seconds", 30.0)),
    )


def git_sha() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout.strip()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------


def ch_query(ch: ClickHouseSettings, sql: str) -> list[dict[str, Any]]:
    """Run a read-only ClickHouse query and return JSONEachRow rows."""
    url = f"{ch.url.rstrip('/')}/?database={ch.database}&default_format=JSONEachRow"
    req = Request(url, data=sql.encode("utf-8"), method="POST")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    if ch.username:
        import base64

        creds = base64.b64encode(f"{ch.username}:{ch.password}".encode()).decode()
        req.add_header("Authorization", f"Basic {creds}")
    try:
        with urlopen(req, timeout=ch.timeout_seconds) as resp:
            rows: list[dict[str, Any]] = []
            for line in resp.read().decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            return rows
    except Exception as exc:
        raise RuntimeError(f"ClickHouse query failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Qrels I/O
# ---------------------------------------------------------------------------


def load_qrels(path: Path) -> list[QueryQrels]:
    """Load qrels from JSON.

    Expected shape:
    {
      "queries": [
        {
          "query_id": "q1",
          "query_text": "authentication error",
          "relevant": [
            {"event_uid": "evt-123", "relevance": 2},
            {"event_uid": "evt-456", "relevance": 1}
          ]
        }
      ]
    }
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    queries: list[QueryQrels] = []
    for item in data.get("queries", []):
        records = []
        for rel in item.get("relevant", []):
            records.append(
                QrelRecord(
                    event_uid=rel["event_uid"],
                    relevance=int(rel.get("relevance", 1)),
                )
            )
        queries.append(
            QueryQrels(
                query_id=item["query_id"],
                query_text=item["query_text"],
                records=records,
            )
        )
    return queries


def save_qrels(path: Path, queries: list[QueryQrels]) -> None:
    """Write qrels to JSON."""
    payload = {
        "queries": [
            {
                "query_id": q.query_id,
                "query_text": q.query_text,
                "relevant": [
                    {"event_uid": r.event_uid, "relevance": r.relevance}
                    for r in q.records
                ],
            }
            for q in queries
        ]
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Offline results I/O
# ---------------------------------------------------------------------------


def load_offline_results(path: Path) -> dict[str, list[SearchHit]]:
    """Load pre-computed search results from JSON.

    Expected shape:
    {
      "strategy": "optimized",
      "results": {
        "q1": [
          {"rank": 1, "event_uid": "evt-abc", "score": 12.34, "session_id": "s1"},
          ...
        ]
      }
    }
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    strategy = data.get("strategy", "offline")
    results: dict[str, list[SearchHit]] = {}
    for qid, hits in data.get("results", {}).items():
        results[qid] = [
            SearchHit(
                rank=int(h["rank"]),
                event_uid=h["event_uid"],
                score=float(h.get("score", 0.0)),
                session_id=h.get("session_id", ""),
            )
            for h in hits
        ]
    return strategy, results


def save_offline_results(
    path: Path, strategy: str, results: dict[str, list[SearchHit]]
) -> None:
    """Write pre-computed search results to JSON."""
    payload = {
        "strategy": strategy,
        "results": {
            qid: [
                {
                    "rank": h.rank,
                    "event_uid": h.event_uid,
                    "score": h.score,
                    "session_id": h.session_id,
                }
                for h in hits
            ]
            for qid, hits in results.items()
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Synthetic qrels generation
# ---------------------------------------------------------------------------


def generate_synthetic_qrels(
    ch: ClickHouseSettings,
    window: str,
    top_n: int,
    min_results: int,
    exclude_sources: list[str],
) -> list[QueryQrels]:
    """Generate qrels from recent search_query_log + search_hit_log.

    Treats the top-ranked hit from each logged query as relevant (relevance=2)
    and any additional hits in the top-3 as partially relevant (relevance=1).
    """
    exclude_clause = ""
    if exclude_sources:
        quoted = ", ".join(f"'{s}'" for s in exclude_sources)
        exclude_clause = f"AND source NOT IN ({quoted})"

    sql = f"""
SELECT
    query_id,
    raw_query AS query_text,
    groupArray((rank, event_uid)) AS hits
FROM (
    SELECT
        q.query_id,
        q.raw_query,
        h.rank,
        h.event_uid
    FROM {ch.database}.search_query_log AS q
    INNER JOIN {ch.database}.search_hit_log AS h ON h.query_id = q.query_id
    WHERE q.ts >= now() - {window}
      {exclude_clause}
      AND q.result_count >= {min_results}
    ORDER BY q.response_ms DESC
    LIMIT {top_n} BY q.query_id
)
GROUP BY query_id, query_text
ORDER BY query_id
FORMAT JSONEachRow
"""
    rows = ch_query(ch, sql)
    queries: list[QueryQrels] = []
    seen_ids: set[str] = set()
    for row in rows:
        qid = row["query_id"]
        if qid in seen_ids:
            continue
        seen_ids.add(qid)
        records = []
        for rank, event_uid in row["hits"]:
            rel = 2 if int(rank) == 1 else 1
            records.append(QrelRecord(event_uid=event_uid, relevance=rel))
        queries.append(
            QueryQrels(
                query_id=qid,
                query_text=row["query_text"],
                records=records,
            )
        )
    return queries


# ---------------------------------------------------------------------------
# Search execution
# ---------------------------------------------------------------------------


def run_searches_live(
    queries: list[QueryQrels],
    strategy: str,
    limit: int,
    ch: ClickHouseSettings,
) -> list[QueryResult]:
    """Run all queries through the moraine_conversations binding."""
    try:
        from moraine_conversations import ConversationClient
    except ImportError as exc:
        raise RuntimeError(
            "moraine_conversations is not installed; run via `uv run --script` "
            "or build bindings with maturin develop."
        ) from exc

    client = ConversationClient(
        url=ch.url,
        database=ch.database,
        username=ch.username,
        password=ch.password,
        timeout_seconds=ch.timeout_seconds,
        max_results=min(limit, 100),
    )

    results: list[QueryResult] = []
    for q in queries:
        import time

        t0 = time.perf_counter()
        raw = client.search_events_json(
            query=q.query_text,
            limit=limit,
            search_strategy=strategy,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        parsed = json.loads(raw)
        hits = []
        for h in parsed.get("hits", []):
            hits.append(
                SearchHit(
                    rank=int(h["rank"]),
                    event_uid=h["event_uid"],
                    score=float(h["score"]),
                    session_id=h.get("session_id", ""),
                )
            )
        results.append(
            QueryResult(
                query_id=q.query_id,
                query_text=q.query_text,
                strategy=strategy,
                hits=hits,
                elapsed_ms=elapsed_ms,
            )
        )
    return results


def run_searches_offline(
    queries: list[QueryQrels],
    offline_strategy: str,
    offline_results: dict[str, list[SearchHit]],
) -> list[QueryResult]:
    """Return pre-computed results for offline evaluation."""
    results: list[QueryResult] = []
    for q in queries:
        hits = offline_results.get(q.query_id, [])
        results.append(
            QueryResult(
                query_id=q.query_id,
                query_text=q.query_text,
                strategy=offline_strategy,
                hits=hits,
                elapsed_ms=0.0,
            )
        )
    return results


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------


def compute_metrics(
    qrels: QueryQrels,
    result: QueryResult,
    k_values: list[int],
) -> QueryMetrics:
    """Compute per-query IR metrics."""
    rel_map: dict[str, int] = {r.event_uid: r.relevance for r in qrels.records}
    # Build relevance vector for returned hits
    relevances: list[int] = []
    for hit in result.hits:
        relevances.append(rel_map.get(hit.event_uid, 0))

    def precision_at_k(k: int) -> float:
        if k <= 0:
            return 0.0
        rel_k = relevances[:k]
        if not rel_k:
            return 0.0
        return sum(1 for r in rel_k if r > 0) / k

    def recall_at_k(k: int) -> float:
        total_rel = sum(1 for r in rel_map.values() if r > 0)
        if total_rel == 0:
            return 0.0
        rel_k = relevances[:k]
        return sum(1 for r in rel_k if r > 0) / total_rel

    def f1_at_k(k: int) -> float:
        p = precision_at_k(k)
        r = recall_at_k(k)
        if p + r == 0:
            return 0.0
        return 2 * p * r / (p + r)

    def dcg_at_k(k: int) -> float:
        dcg = 0.0
        for i, rel in enumerate(relevances[:k], start=1):
            if rel > 0:
                dcg += (2**rel - 1) / math.log2(i + 1)
        return dcg

    def ideal_dcg_at_k(k: int) -> float:
        ideal_rels = sorted(rel_map.values(), reverse=True)
        idcg = 0.0
        for i, rel in enumerate(ideal_rels[:k], start=1):
            if rel > 0:
                idcg += (2**rel - 1) / math.log2(i + 1)
        return idcg

    def ndcg_at_k(k: int) -> float:
        idcg = ideal_dcg_at_k(k)
        if idcg == 0:
            return 0.0
        return dcg_at_k(k) / idcg

    at_k = []
    for k in k_values:
        at_k.append(
            MetricsAtK(
                k=k,
                precision=precision_at_k(k),
                recall=recall_at_k(k),
                f1=f1_at_k(k),
                ndcg=ndcg_at_k(k),
            )
        )

    # MRR
    rr = 0.0
    for i, rel in enumerate(relevances, start=1):
        if rel > 0:
            rr = 1.0 / i
            break

    # MAP
    ap = 0.0
    num_relevant = 0
    for i, rel in enumerate(relevances, start=1):
        if rel > 0:
            num_relevant += 1
            ap += num_relevant / i
    total_rel = sum(1 for r in rel_map.values() if r > 0)
    if total_rel > 0:
        ap /= total_rel

    return QueryMetrics(
        query_id=qrels.query_id,
        query_text=qrels.query_text,
        strategy=result.strategy,
        result_count=len(result.hits),
        at_k=at_k,
        reciprocal_rank=rr,
        average_precision=ap,
    )


def aggregate_metrics(
    query_metrics: list[QueryMetrics],
    strategy: str,
    k_values: list[int],
) -> EvalSummary:
    """Aggregate metrics across all queries."""
    n = len(query_metrics)
    if n == 0:
        return EvalSummary(
            strategy=strategy,
            query_count=0,
            total_result_count=0,
            mean_result_count=0.0,
            mean_reciprocal_rank=0.0,
            mean_average_precision=0.0,
            at_k=[
                MetricsAtK(k=k, precision=0.0, recall=0.0, f1=0.0, ndcg=0.0)
                for k in k_values
            ],
        )

    total_results = sum(q.result_count for q in query_metrics)
    mean_mrr = sum(q.reciprocal_rank for q in query_metrics) / n
    mean_map = sum(q.average_precision for q in query_metrics) / n

    at_k = []
    for k in k_values:
        idx = k_values.index(k)
        mean_p = sum(q.at_k[idx].precision for q in query_metrics) / n
        mean_r = sum(q.at_k[idx].recall for q in query_metrics) / n
        mean_f1 = sum(q.at_k[idx].f1 for q in query_metrics) / n
        mean_ndcg = sum(q.at_k[idx].ndcg for q in query_metrics) / n
        at_k.append(
            MetricsAtK(k=k, precision=mean_p, recall=mean_r, f1=mean_f1, ndcg=mean_ndcg)
        )

    return EvalSummary(
        strategy=strategy,
        query_count=n,
        total_result_count=total_results,
        mean_result_count=total_results / n,
        mean_reciprocal_rank=mean_mrr,
        mean_average_precision=mean_map,
        at_k=at_k,
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_summary(summary: EvalSummary) -> None:
    print(f"\n{'=' * 60}")
    print(f"Strategy: {summary.strategy}")
    print(f"Queries evaluated: {summary.query_count}")
    print(f"Mean results/query: {summary.mean_result_count:.2f}")
    print(f"{'-' * 60}")
    print(f"{'Metric':<20} {'Value':>10}")
    print(f"{'-' * 60}")
    print(f"{'MRR':<20} {summary.mean_reciprocal_rank:>10.4f}")
    print(f"{'MAP':<20} {summary.mean_average_precision:>10.4f}")
    for m in summary.at_k:
        print(f"{'P@' + str(m.k):<20} {m.precision:>10.4f}")
        print(f"{'R@' + str(m.k):<20} {m.recall:>10.4f}")
        print(f"{'F1@' + str(m.k):<19} {m.f1:>10.4f}")
        print(f"{'NDCG@' + str(m.k):<18} {m.ndcg:>10.4f}")
    print(f"{'=' * 60}\n")


def print_comparison(summaries: list[EvalSummary], k_values: list[int]) -> None:
    if len(summaries) < 2:
        return
    print(f"\n{'=' * 70}")
    print("Comparison")
    print(f"{'=' * 70}")
    for m in summaries[0].at_k:
        print(f"\n@K={m.k}")
        print(f"{'Metric':<12} " + " ".join(f"{s.strategy:>12}" for s in summaries))
        for metric_name in ["precision", "recall", "f1", "ndcg"]:
            row = [
                f"{getattr(s.at_k[k_values.index(m.k)], metric_name):>12.4f}"
                for s in summaries
            ]
            print(f"{metric_name:<12} " + " ".join(row))
    print(f"\n{'Metric':<12} " + " ".join(f"{s.strategy:>12}" for s in summaries))
    print(
        f"{'MRR':<12} "
        + " ".join(f"{s.mean_reciprocal_rank:>12.4f}" for s in summaries)
    )
    print(
        f"{'MAP':<12} "
        + " ".join(f"{s.mean_average_precision:>12.4f}" for s in summaries)
    )
    print(f"{'=' * 70}\n")


def build_json_report(
    summaries: list[EvalSummary],
    query_metrics: dict[str, list[QueryMetrics]],
    k_values: list[int],
    args: argparse.Namespace,
) -> dict[str, Any]:
    return {
        "meta": {
            "git_sha": git_sha(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config_path": str(args.config) if args.config else None,
            "qrels_path": str(args.qrels) if args.qrels else None,
            "offline_results_path": str(args.offline_results)
            if args.offline_results
            else None,
            "strategies": [s.strategy for s in summaries],
            "k_values": k_values,
            "limit": args.limit,
        },
        "summaries": [asdict(s) for s in summaries],
        "per_query": {
            strategy: [asdict(qm) for qm in qms]
            for strategy, qms in query_metrics.items()
        },
    }


# ---------------------------------------------------------------------------
# Build step
# ---------------------------------------------------------------------------


def ensure_binding(config_path: Path) -> None:
    """Build moraine_conversations Python binding via maturin develop."""
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = (
        repo_root / "bindings" / "python" / "moraine_conversations" / "Cargo.toml"
    )
    if not manifest_path.exists():
        raise RuntimeError(
            f"moraine_conversations Cargo.toml not found at {manifest_path}"
        )
    maturin_bin = shutil.which("maturin")
    if not maturin_bin:
        raise RuntimeError(
            "maturin was not found in PATH; run this script via `uv run --script` "
            "so maturin is available from the script metadata."
        )
    print("Building local moraine_conversations binding via maturin develop...")
    proc = subprocess.run(
        [maturin_bin, "develop", "--manifest-path", str(manifest_path)],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = proc.stderr[-800:] if proc.stderr else proc.stdout[-800:]
        raise RuntimeError(f"maturin develop failed: {detail}")
    print("Binding built successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Moraine search quality evaluation harness"
    )
    parser.add_argument("--config", type=Path, help="Path to moraine.toml")
    parser.add_argument("--qrels", type=Path, help="Path to qrels JSON file")
    parser.add_argument(
        "--generate-qrels",
        action="store_true",
        help="Generate synthetic qrels from search_query_log",
    )
    parser.add_argument(
        "--output-qrels", type=Path, help="Write generated qrels to this path"
    )
    parser.add_argument(
        "--window", default="24h", help="Telemetry lookback for generation"
    )
    parser.add_argument(
        "--top-n", type=int, default=20, help="Queries to sample from log"
    )
    parser.add_argument(
        "--min-results",
        type=int,
        default=1,
        help="Minimum result_count to include a logged query",
    )
    parser.add_argument(
        "--exclude-source",
        action="append",
        default=[],
        help="Exclude sources from generation (can repeat)",
    )
    parser.add_argument(
        "--limit", type=int, default=25, help="Search result limit for evaluation"
    )
    parser.add_argument(
        "--k-values",
        type=lambda s: [int(x) for x in s.split(",")],
        default=[1, 3, 5, 10, 25],
        help="Comma-separated cutoffs for @K metrics",
    )
    parser.add_argument(
        "--compare-strategies",
        type=lambda s: [x.strip() for x in s.split(",")],
        default=["optimized"],
        help="Comma-separated strategies to evaluate",
    )
    parser.add_argument("--output-json", type=Path, help="Write JSON report")
    parser.add_argument(
        "--offline-results",
        type=Path,
        help="Path to pre-computed results JSON for offline evaluation",
    )
    parser.add_argument(
        "--skip-maturin-develop",
        action="store_true",
        help="Skip binding rebuild",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load qrels and print stats but skip search execution",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Run offline self-check against built-in fixtures and exit",
    )

    args = parser.parse_args()

    if args.self_check:
        return run_self_check()

    is_offline = args.offline_results is not None
    is_generate = args.generate_qrels

    if not is_offline and not is_generate and not args.config:
        parser.error("--config is required for live evaluation or qrels generation")

    config_path = None
    ch = None
    if args.config:
        config_path = args.config.expanduser().resolve()
        ch = read_config(config_path)

    # ------------------------------------------------------------------
    # Generate qrels mode
    # ------------------------------------------------------------------
    if is_generate:
        if ch is None:
            parser.error("--config is required for --generate-qrels")
        window = parse_window_interval(args.window)
        queries = generate_synthetic_qrels(
            ch,
            window=window,
            top_n=args.top_n,
            min_results=args.min_results,
            exclude_sources=args.exclude_source,
        )
        print(f"Generated {len(queries)} synthetic queries from telemetry.")
        if args.output_qrels:
            save_qrels(args.output_qrels, queries)
            print(f"Wrote synthetic qrels to {args.output_qrels}")
        if not args.qrels and not args.output_qrels:
            for q in queries[:3]:
                print(f"  {q.query_id}: {q.query_text!r} ({len(q.records)} judgments)")
        if not args.qrels:
            return 0
        if args.output_qrels and not args.qrels:
            args.qrels = args.output_qrels

    if not args.qrels:
        parser.error(
            "--qrels is required unless --generate-qrels is used (with --output-qrels)"
        )

    # ------------------------------------------------------------------
    # Load qrels
    # ------------------------------------------------------------------
    qrels_path = args.qrels.expanduser().resolve()
    queries = load_qrels(qrels_path)
    if not queries:
        print("No queries found in qrels file.")
        return 1

    total_judgments = sum(len(q.records) for q in queries)
    print(
        f"Loaded {len(queries)} queries with {total_judgments} judgments from {qrels_path}"
    )

    if args.dry_run:
        for q in queries:
            print(f"  {q.query_id}: {q.query_text!r} ({len(q.records)} judgments)")
        return 0

    # ------------------------------------------------------------------
    # Offline or live execution
    # ------------------------------------------------------------------
    all_summaries: list[EvalSummary] = []
    all_query_metrics: dict[str, list[QueryMetrics]] = {}

    if is_offline:
        offline_path = args.offline_results.expanduser().resolve()
        offline_strategy, offline_results = load_offline_results(offline_path)
        print(
            f"Loaded offline results for {len(offline_results)} queries from {offline_path}"
        )

        results = run_searches_offline(queries, offline_strategy, offline_results)
        query_metrics = []
        for qrel, result in zip(queries, results):
            qm = compute_metrics(qrel, result, args.k_values)
            query_metrics.append(qm)
        summary = aggregate_metrics(query_metrics, offline_strategy, args.k_values)
        all_summaries.append(summary)
        all_query_metrics[offline_strategy] = query_metrics
        print_summary(summary)
    else:
        if config_path is None:
            parser.error("--config is required for live evaluation")
        if not args.skip_maturin_develop:
            ensure_binding(config_path)
        else:
            print("Skipping maturin develop (per --skip-maturin-develop)")

        for strategy in args.compare_strategies:
            print(f"\nEvaluating strategy: {strategy}")
            results = run_searches_live(queries, strategy, args.limit, ch)
            query_metrics = []
            for qrel, result in zip(queries, results):
                qm = compute_metrics(qrel, result, args.k_values)
                query_metrics.append(qm)
            summary = aggregate_metrics(query_metrics, strategy, args.k_values)
            all_summaries.append(summary)
            all_query_metrics[strategy] = query_metrics
            print_summary(summary)

    if len(all_summaries) > 1:
        print_comparison(all_summaries, args.k_values)

    # ------------------------------------------------------------------
    # JSON output
    # ------------------------------------------------------------------
    if args.output_json:
        report = build_json_report(
            all_summaries, all_query_metrics, args.k_values, args
        )
        args.output_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Wrote JSON report to {args.output_json}")

    return 0


# ---------------------------------------------------------------------------
# Self-check
# ---------------------------------------------------------------------------


def run_self_check() -> int:
    """Run offline evaluation against built-in fixtures and assert sanity."""
    repo_root = Path(__file__).resolve().parents[2]
    qrels_path = repo_root / "fixtures" / "search_eval" / "sample_qrels.json"
    results_path = repo_root / "fixtures" / "search_eval" / "sample_results.json"

    if not qrels_path.exists():
        print(f"SELF-CHECK FAIL: missing fixture {qrels_path}")
        return 1
    if not results_path.exists():
        print(f"SELF-CHECK FAIL: missing fixture {results_path}")
        return 1

    queries = load_qrels(qrels_path)
    offline_strategy, offline_results = load_offline_results(results_path)
    results = run_searches_offline(queries, offline_strategy, offline_results)

    query_metrics = []
    for qrel, result in zip(queries, results):
        qm = compute_metrics(qrel, result, [1, 3, 5])
        query_metrics.append(qm)
    summary = aggregate_metrics(query_metrics, offline_strategy, [1, 3, 5])

    # Sanity assertions
    errors = []
    if summary.query_count != 5:
        errors.append(f"expected 5 queries, got {summary.query_count}")
    if summary.mean_reciprocal_rank <= 0:
        errors.append(f"MRR should be > 0, got {summary.mean_reciprocal_rank}")
    if summary.mean_average_precision <= 0:
        errors.append(f"MAP should be > 0, got {summary.mean_average_precision}")

    p_at_1 = summary.at_k[0].precision
    if p_at_1 < 0.5:
        errors.append(f"P@1 should be >= 0.5 for sample data, got {p_at_1}")

    if errors:
        print("SELF-CHECK FAIL:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("SELF-CHECK PASS")
    print(f"  queries: {summary.query_count}")
    print(f"  MRR: {summary.mean_reciprocal_rank:.4f}")
    print(f"  MAP: {summary.mean_average_precision:.4f}")
    print(f"  P@1: {p_at_1:.4f}")
    print(f"  NDCG@5: {summary.at_k[2].ndcg:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
