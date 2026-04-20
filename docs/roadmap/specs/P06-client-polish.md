# P06 — Python, CLI, and HTTP Client Polish

**Priority:** P2  
**Effort:** M  
**Status:** Specification / ready for design review  
**Dependencies:** C04 (source drilldown), C06 (query workbench)

## Objective

Make Moraine a better library as well as a service. Provide typed, async Python clients for conversations, source health, search, and exports. Achieve CLI parity for all monitor APIs. Stabilize JSON schemas for machine output.

## Design Principles

1. **Clients are thin wrappers over HTTP APIs.** They do not reimplement business logic. [src: ADR-001]
2. **Async first, sync optional.** Python async is the modern default; sync wrappers can be generated or provided as a separate module.
3. **Schemas are code-generated from Rust types.** Use `schemars` + `typeshare` or similar to keep Rust, TypeScript, and Python types in sync.
4. **Errors are typed and actionable.** Clients surface HTTP status, Moraine error codes, and retry guidance.

## API Sketches

### Python Client (`bindings/python/` or `moraine-client` PyPI package)

```python
import asyncio
from moraine import MoraineClient

async def main():
    client = MoraineClient(base_url="http://127.0.0.1:8080")
    
    # Search
    results = await client.search(
        query="auth refactor",
        limit=10,
        project="backend",
        scope="auto"
    )
    for hit in results.hits:
        print(hit.rank, hit.session_id, hit.preview)
    
    # Session navigation
    session = await client.get_session("sess_abc123")
    events = await client.get_session_events("sess_abc123", direction="forward", limit=50)
    
    # Source health
    sources = await client.list_sources()
    for src in sources:
        print(src.name, src.status, src.last_checkpoint_at)
    
    # Notes (P02)
    note = await client.create_note(
        title="Auth pattern",
        body="Use JWT with rotation...",
        tags=["backend", "auth"],
        links=[{"target_kind": "session", "target_id": "sess_abc123"}]
    )
    
    # Export
    replay = await client.export_replay("sess_abc123", include_files=True)
    print(replay.manifest.replay_path)

asyncio.run(main())
```

**Sync wrapper:**
```python
from moraine.sync import MoraineClient
client = MoraineClient()
results = client.search("auth refactor")
```

### CLI Parity

Current CLI (`apps/moraine`) covers stack lifecycle and `sources status`. Extend to cover all monitor APIs:

```bash
# Search (new)
moraine search "auth refactor" [--limit 10] [--project backend] [--json]
moraine search-conversations "debugging" [--mode tool_calling]

# Sessions (new)
moraine sessions list [--from 2026-04-01] [--to 2026-04-20] [--limit 25]
moraine sessions show <session_id> [--events] [--json]
moraine sessions events <session_id> [--direction forward] [--limit 50]

# Source drilldown (extends existing)
moraine sources files <source_name> [--limit 50]
moraine sources errors <source_name> [--limit 50]
moraine sources checkpoints <source_name>

# Query workbench (new)
moraine query save "backend-auth" --query "auth refactor" --project backend
moraine query list
moraine query run <query_name> [--json]

# Export (new, links to C11 / P04)
moraine export sessions [--from ...] [--to ...] [--project ...] [--format jsonl|parquet]
moraine export replay <session_id> [--out ./replay.tar.gz]

# JSON output stabilization
moraine --output json <subcommand>   # machine-readable output for all commands
```

### HTTP API Stabilization

Stabilize monitor HTTP API paths and response schemas:

| Endpoint | Method | Response Schema |
|---|---|---|
| `/api/health` | GET | `{status, version, uptime_ms}` |
| `/api/sources` | GET | `[{name, status, lag_seconds, row_count, error_count, checkpoint_at}]` |
| `/api/sources/:name/files` | GET | `[{path, size, modified_at, checkpoint_offset}]` |
| `/api/sources/:name/errors` | GET | `[{at, kind, text, raw_fragment}]` |
| `/api/sessions` | GET | `[{session_id, start_at, end_at, event_count, harness, mode}]` |
| `/api/sessions/:id` | GET | `{session_id, metadata, event_count, source_names}` |
| `/api/sessions/:id/events` | GET | `[{event_uid, event_time, event_kind, actor_role, text_preview}]` |
| `/api/search` | POST | `{query_id, terms, hits, stats}` |
| `/api/search/conversations` | POST | `{query_id, hits, stats}` |
| `/api/notes` | CRUD | P02 schemas |
| `/api/summaries` | CRUD | P01 schemas |
| `/api/profiles` | CRUD | P05 schemas |

All responses use a unified envelope:
```json
{
  "data": { ... },
  "meta": {"request_id": "...", "duration_ms": 12},
  "error": null
}
```

## Schema Design

No new ClickHouse tables. This feature is about API surfaces and client bindings.

### JSON Schema Generation

Introduce `moraine-api-types` crate with `schemars::JsonSchema` derives on all request/response structs. Generate:
- OpenAPI 3.1 spec (for docs and client generation).
- Python pydantic models via `datamodel-codegen`.
- TypeScript interfaces for monitor frontend.

## Data Flow

1. **Rust types:** Defined in `crates/moraine-api-types`.
2. **Schema gen:** CI step generates `openapi.json`, `models.py`, `types.ts`.
3. **Python package:** `bindings/python/` wraps HTTP calls with aiohttp/httpx.
4. **CLI:** `apps/moraine` uses the same HTTP APIs (or shared crate logic) for new subcommands.
5. **Monitor:** Frontend uses generated TypeScript types.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| API versioning | URL path versioning (`/api/v1/...`); v1 is current stable. New breaking changes go to v2 with deprecation period. |
| Cursor compatibility | Cursors are opaque strings; clients must not parse them. |
| Optional fields across versions | JSON schemas use `required` arrays carefully; new fields are additive only within a major version. |
| Local server not running | Python client raises `MoraineConnectionError` with retry count and suggestion to run `moraine up`. |
| Large result sets | Streaming JSONL for exports; paginated arrays for list endpoints. |

## Acceptance Contract

### Functional
- [ ] Python async client supports search, sessions, sources, notes, summaries, and export APIs.
- [ ] Python sync wrapper is auto-generated or thin adapter around async client.
- [ ] CLI `moraine search` returns same results as monitor search API.
- [ ] `--output json` produces valid JSON for every CLI subcommand.

### Operational
- [ ] Python client handles 10k result pages without memory issues (streaming).
- [ ] HTTP API latency P99 < 500ms for search and session list under 1M events.

### Safety
- [ ] Python client respects `safety_mode` and `_safety` envelope.
- [ ] Exported JSON schemas do not expose internal server paths or credentials.

### Compatibility
- [ ] OpenAPI spec is versioned and published in releases.
- [ ] Breaking API changes require major version bump and migration guide.

### Observability
- [ ] Python client emits request/response logs at DEBUG level.
- [ ] HTTP API returns `X-Request-ID` header for tracing.

## PR Sequencing

1. `refactor(api): introduce moraine-api-types crate with schemars derives`  
   - Extract types from monitor and MCP crates; no behavior change.
2. `feat(api): stabilize monitor HTTP API paths and unified response envelope`  
   - Add `/api/v1/` prefix; standardize error shape.
3. `feat(python): add async client and sync wrapper`  
   - `bindings/python/moraine/` with pytest suite against sandbox.
4. `feat(cli): add search, sessions, and export subcommands`  
   - CLI parity with monitor APIs.
5. `feat(docs): generate OpenAPI spec and publish Python client docs`  
   - MkDocs update; PyPI release automation.
6. `test(api): add HTTP API conformance tests`  
   - Schema validation against all endpoints; fixture-driven.

## Open Questions

1. **HTTP client library:** `httpx` (recommended) vs `aiohttp`. `httpx` supports both sync and async with one codebase.
2. **TypeScript generation:** Use `openapi-typescript` from generated OpenAPI spec. Monitor frontend migrates gradually.
3. **Should MCP tools also use the HTTP API internally?** No — MCP runs locally and can use the shared Rust crate directly. HTTP API is for external clients.
