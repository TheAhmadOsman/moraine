# Factory Droid Ingest And MCP Wiring

This slice adds first-class Factory Droid session ingestion so Moraine MCP can retrieve Droid conversations through the existing `search`, `search_conversations`, `list_sessions`, `get_session`, `get_session_events`, and `open` tools.

## What Landed

- Added the `factory-droid` harness and `factory_droid_jsonl` source format.
- Added default source coverage for `~/.factory/sessions/**/*.jsonl`.
- Paired each Droid JSONL session with its sibling `<session>.settings.json` sidecar when the JSONL advances.
- Normalized `session_start`, `message`, `compaction_state`, and sidecar settings into canonical rows.
- Preserved exact JSONL and sidecar source JSON in `raw_events.raw_json` while pruning encrypted/noisy blobs from canonical event payload JSON.
- Added Factory Droid source coverage to sandbox host-session mounts and fixture config generation.
- Documented source behavior, timestamp fallback, sidecar handling, and MCP install guidance.

## Operational Notes

Factory Droid sidecars are read opportunistically. If the sidecar is absent, messages still ingest. If the sidecar is malformed, Moraine records a sidecar-scoped ingest error without blocking the JSONL session. The watcher tracks JSONL paths, so settings-only changes are picked up on the next JSONL append or explicit reindex.
