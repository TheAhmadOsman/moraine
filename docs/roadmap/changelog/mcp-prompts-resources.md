# Changelog: MCP Prompts And Static Resources

Date: 2026-04-20
Scope: C10 MCP resources, prompts, and safe retrieval workflow guidance

## What Changed

### `crates/moraine-mcp-core`
- Added MCP prompt methods:
  - `prompts/list`
  - `prompts/get`
- Added a concrete prompt catalog:
  - `search_session_triage`
  - `open_session_context`
  - `prepare_session_handoff`
- Added static guide resources to `resources/list` and `resources/read`:
  - `moraine://guides/capabilities`
  - `moraine://guides/safety`
  - `moraine://guides/uri-templates`
- Preserved the existing dynamic resource templates for:
  - `moraine://sessions/{session_id}`
  - `moraine://events/{event_uid}`
- Added focused tests for:
  - prompt method dispatch
  - prompt validation
  - static resource list/read behavior
  - initialize capabilities including prompt support

### Docs
- Updated `docs/mcp/agent-interface.md` with the new prompt and resource methods, example payloads, and usage guidance.

## Validation

```bash
cargo fmt --all                 # pass
cargo test -p moraine-mcp-core --locked  # pass
```

## Notes

- The prompt catalog is intentionally text-first. It gives hosts safe retrieval workflows without introducing a second action-execution protocol.
- Static resources stay narrow and always-safe; session and event lookups remain on the existing dynamic resource templates.
