#!/usr/bin/env python3
"""Smoke-test Moraine monitor HTTP APIs against a live isolated stack."""

import argparse
import json
import urllib.error
import urllib.request
from typing import Any, Dict


JsonObject = Dict[str, Any]


def fetch_json(base_url: str, path: str) -> JsonObject:
    url = f"{base_url.rstrip('/')}{path}"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{path} returned HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise AssertionError(f"{path} request failed: {exc}") from exc

    if status < 200 or status >= 300:
        raise AssertionError(f"{path} returned HTTP {status}: {body}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"{path} did not return JSON: {body!r}") from exc
    if not isinstance(data, dict):
        raise AssertionError(f"{path} JSON body is not an object: {data!r}")
    return data


def assert_ok(data: JsonObject, path: str) -> None:
    if data.get("ok") is not True:
        raise AssertionError(f"{path} expected ok=true: {data}")


def require_array(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise AssertionError(f"{label} must be an array: {value!r}")
    return value


def require_object(value: Any, label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise AssertionError(f"{label} must be an object: {value!r}")
    return value


def parse_expected_source(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise argparse.ArgumentTypeError(
            "--expect-source must use name:harness, for example ci-codex:codex"
        )
    name, harness = value.split(":", 1)
    if not name or not harness:
        raise argparse.ArgumentTypeError("--expect-source name and harness must be non-empty")
    return name, harness


def assert_monitor_basics(base_url: str) -> None:
    for path in ["/api/health", "/api/status", "/api/analytics", "/api/web-searches"]:
        data = fetch_json(base_url, path)
        assert_ok(data, path)


def assert_sources(base_url: str, expected_sources: list[tuple[str, str]]) -> None:
    data = fetch_json(base_url, "/api/sources")
    assert_ok(data, "/api/sources")
    if data.get("query_error") not in (None, ""):
        raise AssertionError(f"/api/sources returned query_error: {data['query_error']}")

    sources = require_array(data.get("sources"), "/api/sources.sources")
    by_name: dict[str, JsonObject] = {}
    for source in sources:
        source_obj = require_object(source, "/api/sources source")
        name = source_obj.get("name")
        if isinstance(name, str):
            by_name[name] = source_obj

    for name, expected_harness in expected_sources:
        if name not in by_name:
            raise AssertionError(f"/api/sources missing configured source {name!r}: {sources}")
        source = by_name[name]
        if source.get("harness") != expected_harness:
            raise AssertionError(f"{name} harness mismatch: {source}")
        if source.get("enabled") is not True:
            raise AssertionError(f"{name} should be enabled: {source}")
        if source.get("status") != "ok":
            raise AssertionError(f"{name} should be ok after fixture ingest: {source}")
        for field in ["glob", "watch_root", "format"]:
            if not isinstance(source.get(field), str) or not source[field]:
                raise AssertionError(f"{name} missing {field}: {source}")
        for field in ["checkpoint_count", "raw_event_count", "ingest_error_count"]:
            if not isinstance(source.get(field), int) or source[field] < 0:
                raise AssertionError(f"{name} invalid {field}: {source}")
        if source["checkpoint_count"] < 1:
            raise AssertionError(f"{name} should have at least one checkpoint: {source}")
        if source["raw_event_count"] < 1:
            raise AssertionError(f"{name} should have indexed raw events: {source}")
        if source["ingest_error_count"] != 0:
            raise AssertionError(f"{name} should have no ingest errors: {source}")
        if not isinstance(source.get("latest_checkpoint_at"), str) or not source["latest_checkpoint_at"]:
            raise AssertionError(f"{name} missing latest checkpoint timestamp: {source}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run monitor HTTP smoke tests against a Moraine monitor base URL."
    )
    parser.add_argument("--base-url", required=True)
    parser.add_argument(
        "--expect-source",
        action="append",
        type=parse_expected_source,
        default=[],
        help="Expected source as name:harness. May be passed multiple times.",
    )
    args = parser.parse_args()

    assert_monitor_basics(args.base_url)
    assert_sources(args.base_url, args.expect_source)
    print("monitor smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
